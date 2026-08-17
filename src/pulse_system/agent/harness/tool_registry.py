"""Auditable discovery and capability pruning for Harness tool ecosystems.

The registry is intentionally a *descriptive* boundary.  It may read an
allowlisted local manifest or an explicitly supplied in-memory manifest, but
it never installs, imports, starts, connects to, or invokes a tool.  A later
execution adapter must take the registry's decision and apply its own runtime
policy, approval, and epoch checks.

This separation is important for Pulse: an extension, MCP server, skill,
plugin, or hook is not an Engram and cannot acquire Engram identity from a
manifest.  The registry therefore stores only bounded metadata and a
capability intersection.  Manifest payloads, tool arguments, credentials,
entrypoint command lines, and raw handshake values are deliberately absent
from the public descriptor.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tomllib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import Enum
from pathlib import Path
from threading import RLock
from typing import Any, TypeAlias

from .capabilities import CapabilityContext, CapabilityError, CapabilitySet, EvidenceClass

__all__ = [
    "CapabilityDecision",
    "DescriptorKind",
    "ExtensionDescriptor",
    "HookDescriptor",
    "MCPDescriptor",
    "PluginDescriptor",
    "PluginProvenance",
    "RegistryFinding",
    "RegistryStatus",
    "SignatureState",
    "SkillDescriptor",
    "ToolDescriptor",
    "ToolRegistry",
]


_MAX_REASON = 160
_MAX_DESCRIPTION = 512
_MAX_HANDSHAKE_FIELDS = 16
_MAX_FINDINGS = 512
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,127}$")
_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.+_-]{0,127}$")
_DIGEST_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_KEY_ID_RE = re.compile(r"^[A-Za-z0-9_.:/@+-]{1,128}$")
_SAFE_REASON_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,79}$")
_MANIFEST_NAMES = frozenset(
    {
        "manifest.json",
        "manifest.toml",
        "tool.json",
        "skill.json",
        "plugin.json",
        "extension.json",
        "hook.json",
        "mcp.json",
    }
)
_MANIFEST_SUFFIXES = frozenset({".json", ".toml"})
_KIND_ALIASES = {
    "tool": "tool",
    "mcp": "mcp",
    "mcp_server": "mcp",
    "mcp-server": "mcp",
    "skill": "skill",
    "plugin": "plugin",
    "extension": "extension",
    "hook": "hook",
}
_TRANSPORT_ALIASES = {
    "streamable-http": "streamable_http",
    "streamable_http": "streamable_http",
    "http": "streamable_http",
    "sse": "sse",
    "stdio": "stdio",
    "websocket": "websocket",
    "ws": "websocket",
    "loopback": "loopback",
}
_KNOWN_LIFECYCLES = frozenset(
    {"startup", "shutdown", "session", "turn", "request", "event", "static"}
)
_KNOWN_LOCATIONS = frozenset(
    {"pi_rpc", "pulse_loopback", "local_process", "workspace", "server", "client"}
)
_SAFE_HANDSHAKE_KEYS = frozenset(
    {"protocol_version", "server_name", "server_version", "capabilities_digest", "transport"}
)


def _roots_overlap(left: Iterable[str], right: Iterable[str]) -> bool:
    for left_root in left:
        for right_root in right:
            if (
                left_root == "."
                or right_root == "."
                or left_root == right_root
                or left_root.startswith(right_root + "/")
                or right_root.startswith(left_root + "/")
            ):
                return True
    return False


def _domain_in_scope(child: str, parent: str) -> bool:
    child = child.removeprefix("*.")
    parent = parent.removeprefix("*.")
    return child == parent or child.endswith("." + parent)


def _domains_overlap(left: Iterable[str], right: Iterable[str]) -> bool:
    return any(
        _domain_in_scope(left_domain, right_domain)
        or _domain_in_scope(right_domain, left_domain)
        for left_domain in left
        for right_domain in right
    )


class DescriptorKind(str, Enum):
    TOOL = "tool"
    MCP = "mcp"
    SKILL = "skill"
    PLUGIN = "plugin"
    EXTENSION = "extension"
    HOOK = "hook"


class RegistryStatus(str, Enum):
    DISABLED = "disabled"
    ENABLED = "enabled"
    QUARANTINED = "quarantined"
    INCOMPATIBLE = "incompatible"
    UNSUPPORTED = "unsupported"


class SignatureState(str, Enum):
    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    INVALID = "invalid"
    MISSING = "missing"
    UNSUPPORTED = "unsupported"


def _safe_identifier(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    candidate = value.strip()
    if _ID_RE.fullmatch(candidate) is None:
        raise ValueError(f"{field_name} has an invalid shape")
    return candidate


def _safe_version(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("version must be a string")
    candidate = value.strip()
    if _VERSION_RE.fullmatch(candidate) is None:
        raise ValueError("version has an invalid shape")
    return candidate


def _safe_reason(value: Any, *, fallback: str) -> str:
    """Keep operator/manifest reasons bounded without echoing free text."""

    if isinstance(value, str):
        candidate = value.strip().lower()
        if _SAFE_REASON_RE.fullmatch(candidate) is not None:
            return candidate
        if candidate:
            digest = hashlib.sha256(candidate.encode("utf-8", "replace")).hexdigest()[:12]
            return f"reason_digest_{digest}"
    return fallback


def _safe_text(value: Any, *, limit: int = _MAX_DESCRIPTION) -> str:
    if not isinstance(value, str):
        return ""
    text = " ".join(value.replace("\x00", "").split())
    # Descriptions are metadata, but they can still be a credential exfiltration
    # channel.  Preserve a safe marker instead of the suspicious value.
    text = re.sub(r"(?i)(bearer\s+)[^\s,;]+", r"\1[REDACTED]", text)
    text = re.sub(r"(?i)(password|passwd|secret|token|api[_-]?key)\s*[:=]\s*[^\s,;]+", r"\1=[REDACTED]", text)
    return text[:limit]


def _safe_optional_key(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    return candidate if _KEY_ID_RE.fullmatch(candidate) else None


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    """Hash a bounded manifest representation without retaining it."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def _default_lifecycle(kind: DescriptorKind) -> str:
    return {
        DescriptorKind.TOOL: "turn",
        DescriptorKind.MCP: "session",
        DescriptorKind.SKILL: "turn",
        DescriptorKind.PLUGIN: "session",
        DescriptorKind.EXTENSION: "session",
        DescriptorKind.HOOK: "event",
    }[kind]


def _default_location(kind: DescriptorKind) -> str:
    return {
        DescriptorKind.TOOL: "pi_rpc",
        DescriptorKind.MCP: "local_process",
        DescriptorKind.SKILL: "workspace",
        DescriptorKind.PLUGIN: "local_process",
        DescriptorKind.EXTENSION: "pi_rpc",
        DescriptorKind.HOOK: "server",
    }[kind]


def _default_approval(kind: DescriptorKind) -> bool:
    return kind in {
        DescriptorKind.MCP,
        DescriptorKind.PLUGIN,
        DescriptorKind.EXTENSION,
        DescriptorKind.HOOK,
    }


@dataclass(frozen=True, slots=True)
class PluginProvenance:
    """Safe source evidence for one descriptor.

    ``relative_path`` is always workspace-relative or ``None``.  The digest
    is the manifest/artifact digest observed by the registry; it is not a
    claim that an external package was fetched or executed.
    """

    source_id: str
    source_kind: str
    relative_path: str | None
    version: str
    digest: str
    signature_state: SignatureState
    signature_key_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", _safe_identifier(self.source_id, field_name="source_id"))
        object.__setattr__(self, "source_kind", _safe_identifier(self.source_kind, field_name="source_kind"))
        object.__setattr__(self, "version", _safe_version(self.version))
        digest = self.digest.lower()
        if _DIGEST_RE.fullmatch(digest) is None:
            raise ValueError("provenance digest must be a sha256 hex digest")
        object.__setattr__(self, "digest", digest)
        if not isinstance(self.signature_state, SignatureState):
            object.__setattr__(self, "signature_state", SignatureState(str(self.signature_state)))
        if self.relative_path is not None:
            relative = self.relative_path.replace("\\", "/")
            if relative.startswith("/") or ".." in relative.split("/"):
                raise ValueError("provenance path must be workspace-relative")
            object.__setattr__(self, "relative_path", relative or ".")
        object.__setattr__(self, "signature_key_id", _safe_optional_key(self.signature_key_id))

    @property
    def verified(self) -> bool:
        return self.signature_state is SignatureState.VERIFIED

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "source_id": self.source_id,
            "source_kind": self.source_kind,
            "relative_path": self.relative_path,
            "version": self.version,
            "digest": self.digest,
            "signature_state": self.signature_state.value,
        }
        if self.signature_key_id is not None:
            data["signature_key_id"] = self.signature_key_id
        return data


@dataclass(frozen=True, slots=True)
class ToolDescriptor:
    """A safe descriptor for any registry-managed ecosystem item."""

    descriptor_id: str
    name: str
    version: str
    kind: DescriptorKind
    provenance: PluginProvenance
    declared_capabilities: CapabilitySet
    lifecycle: str
    execution_location: str
    status: RegistryStatus = RegistryStatus.DISABLED
    reason: str = "discovered_not_enabled"
    enabled: bool = False
    version_pin: str | None = None
    transport: str | None = None
    handshake: tuple[tuple[str, str], ...] = ()
    configured_scope: CapabilitySet | None = None
    approval_required: bool = True
    description: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "descriptor_id", _safe_identifier(self.descriptor_id, field_name="descriptor_id"))
        object.__setattr__(self, "name", _safe_text(self.name, limit=128) or self.descriptor_id)
        object.__setattr__(self, "version", _safe_version(self.version))
        if not isinstance(self.kind, DescriptorKind):
            object.__setattr__(self, "kind", DescriptorKind(str(self.kind)))
        if not isinstance(self.provenance, PluginProvenance):
            raise TypeError("provenance must be PluginProvenance")
        if not isinstance(self.declared_capabilities, CapabilitySet):
            raise TypeError("declared_capabilities must be CapabilitySet")
        if not isinstance(self.status, RegistryStatus):
            object.__setattr__(self, "status", RegistryStatus(str(self.status)))
        object.__setattr__(self, "reason", _safe_reason(self.reason, fallback="registry_state"))
        if self.version_pin is not None:
            object.__setattr__(self, "version_pin", _safe_version(self.version_pin))
        if self.transport is not None:
            object.__setattr__(self, "transport", _safe_identifier(self.transport, field_name="transport"))
        if not isinstance(self.handshake, tuple):
            object.__setattr__(self, "handshake", tuple(self.handshake))
        object.__setattr__(self, "description", _safe_text(self.description))

    @property
    def id(self) -> str:
        return self.descriptor_id

    @property
    def stable_id(self) -> str:
        return self.descriptor_id

    @property
    def source(self) -> str:
        return self.provenance.source_id

    @property
    def digest(self) -> str:
        return self.provenance.digest

    @property
    def signature_state(self) -> SignatureState:
        return self.provenance.signature_state

    def qualified_id(self) -> str:
        return f"{self.descriptor_id}@{self.version}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.descriptor_id,
            "name": self.name,
            "version": self.version,
            "kind": self.kind.value,
            "provenance": self.provenance.to_dict(),
            "declared_capabilities": self.declared_capabilities.to_dict(),
            "lifecycle": self.lifecycle,
            "execution_location": self.execution_location,
            "status": self.status.value,
            "reason": self.reason,
            "enabled": self.enabled,
            "version_pin": self.version_pin,
            "transport": self.transport,
            "handshake": dict(self.handshake),
            "configured_scope": (
                None if self.configured_scope is None else self.configured_scope.to_dict()
            ),
            "approval_required": self.approval_required,
            "description": self.description,
        }

    as_dict = to_dict


@dataclass(frozen=True, slots=True)
class SkillDescriptor(ToolDescriptor):
    pass


@dataclass(frozen=True, slots=True)
class PluginDescriptor(ToolDescriptor):
    pass


@dataclass(frozen=True, slots=True)
class ExtensionDescriptor(ToolDescriptor):
    pass


@dataclass(frozen=True, slots=True)
class HookDescriptor(ToolDescriptor):
    pass


@dataclass(frozen=True, slots=True)
class MCPDescriptor(ToolDescriptor):
    pass


Descriptor: TypeAlias = (
    ToolDescriptor
    | SkillDescriptor
    | PluginDescriptor
    | ExtensionDescriptor
    | HookDescriptor
    | MCPDescriptor
)


@dataclass(frozen=True, slots=True)
class RegistryFinding:
    """A safe result for discovery/configuration/quarantine operations."""

    action: str
    descriptor_id: str
    status: RegistryStatus
    reason: str
    changed: bool
    evidence_class: EvidenceClass = EvidenceClass.CONTRACT_ONLY
    descriptor: Descriptor | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "descriptor_id": self.descriptor_id,
            "status": self.status.value,
            "reason": self.reason,
            "changed": self.changed,
            "evidence_class": self.evidence_class.value,
            "descriptor": None if self.descriptor is None else self.descriptor.to_dict(),
        }

    as_dict = to_dict


@dataclass(frozen=True, slots=True)
class CapabilityDecision:
    """The registry's bounded answer before an execution adapter is called."""

    descriptor_id: str
    allowed: bool
    reason: str
    effective_capabilities: CapabilitySet
    evidence_class: EvidenceClass = EvidenceClass.CONTRACT_ONLY
    live_gate: EvidenceClass = EvidenceClass.LIVE_GATE_UNVERIFIED
    status: RegistryStatus | None = None
    requires_approval: bool = False
    transport_status: str | None = None
    descriptor: Descriptor | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "descriptor_id": self.descriptor_id,
            "allowed": self.allowed,
            "reason": self.reason,
            "effective_capabilities": self.effective_capabilities.to_dict(),
            "evidence_class": self.evidence_class.value,
            "live_gate": self.live_gate.value,
            "status": None if self.status is None else self.status.value,
            "requires_approval": self.requires_approval,
            "transport_status": self.transport_status,
            "descriptor": None if self.descriptor is None else self.descriptor.to_dict(),
        }

    as_dict = to_dict


class ToolRegistry:
    """Discover and resolve bounded ecosystem descriptors without execution."""

    def __init__(
        self,
        *,
        workspace_root: str | Path | None = None,
        allowed_source_roots: Iterable[str | Path] | None = None,
        allowed_registry_ids: Iterable[str] | None = None,
        supported_mcp_transports: Iterable[str] | None = None,
        trusted_digests: Iterable[str] | None = None,
        signature_verifier: Callable[[bytes, Mapping[str, str]], bool] | None = None,
        max_descriptors: int = 256,
        max_manifest_bytes: int = 64 * 1024,
        max_scan_files: int = 1024,
    ) -> None:
        if type(max_descriptors) is not int or not 1 <= max_descriptors <= 4096:
            raise ValueError("max_descriptors must be between 1 and 4096")
        if type(max_manifest_bytes) is not int or not 1024 <= max_manifest_bytes <= 1024 * 1024:
            raise ValueError("max_manifest_bytes must be between 1024 and 1 MiB")
        if type(max_scan_files) is not int or not 1 <= max_scan_files <= 16_384:
            raise ValueError("max_scan_files must be between 1 and 16384")

        self._workspace_root = Path(workspace_root or Path.cwd()).resolve()
        # Source-root validation is used while constructing the allowlist;
        # initialize the field first so that an empty allowlist means
        # "explicit paths inside workspace_root", never "all host paths".
        self._allowed_source_roots: tuple[Path, ...] = ()
        self._allowed_source_roots = tuple(
            self._resolve_configured_root(path) for path in (allowed_source_roots or ())
        )
        self._allowed_registry_ids = frozenset(
            _safe_identifier(value, field_name="registry_id")
            for value in (allowed_registry_ids or ())
        )
        self._supported_mcp_transports = frozenset(
            self._normalize_transport(value) for value in (supported_mcp_transports or ())
        )
        self._trusted_digests = frozenset(self._validate_digest(value) for value in (trusted_digests or ()))
        self._signature_verifier = signature_verifier
        self._max_descriptors = max_descriptors
        self._max_manifest_bytes = max_manifest_bytes
        self._max_scan_files = max_scan_files
        self._descriptors: dict[tuple[str, str], Descriptor] = {}
        self._findings: list[RegistryFinding] = []
        self._lock = RLock()

    @property
    def workspace_root(self) -> Path:
        return self._workspace_root

    @property
    def supported_mcp_transports(self) -> tuple[str, ...]:
        return tuple(sorted(self._supported_mcp_transports))

    def descriptors(self) -> list[Descriptor]:
        with self._lock:
            return sorted(
                self._descriptors.values(),
                key=lambda descriptor: (descriptor.descriptor_id, descriptor.version),
            )

    def findings(self) -> list[RegistryFinding]:
        with self._lock:
            return list(self._findings)

    def discover(self, source_config: Any = None) -> list[Descriptor]:
        """Discover bounded manifests from configured local sources.

        ``source_config`` may be a path, a sequence of paths, a mapping with
        ``roots``/``manifests``/``sources``, or a single in-memory manifest
        mapping.  A registry URL or package name is never fetched here.
        """

        sources = self._collect_sources(source_config)
        discovered: list[Descriptor] = []
        with self._lock:
            for source_kind, source in sources:
                if len(self._descriptors) >= self._max_descriptors:
                    self._record_finding(
                        "discover_limit",
                        "registry",
                        RegistryStatus.UNSUPPORTED,
                        "descriptor_limit_reached",
                    )
                    break
                if source_kind == "inline":
                    descriptor = self._discover_inline(source)
                    if descriptor is not None:
                        discovered.append(descriptor)
                    continue
                for path in self._manifest_paths(source):
                    if len(self._descriptors) >= self._max_descriptors:
                        self._record_finding(
                            "discover_limit",
                            "registry",
                            RegistryStatus.UNSUPPORTED,
                            "descriptor_limit_reached",
                        )
                        break
                    descriptor = self._discover_file(path)
                    if descriptor is not None:
                        discovered.append(descriptor)

        return sorted(
            discovered,
            key=lambda descriptor: (descriptor.descriptor_id, descriptor.version),
        )

    def configure(
        self,
        descriptor_id: str,
        enabled: bool,
        version_pin: str | None = None,
        scope: CapabilitySet | Mapping[str, Any] | None = None,
    ) -> RegistryFinding:
        """Set registry state after validation; never installs or starts anything."""

        with self._lock:
            descriptor = self._lookup(descriptor_id, version_pin=version_pin)
            if descriptor is None:
                if version_pin is None:
                    matches = [
                        item
                        for (stable_id, _version), item in self._descriptors.items()
                        if stable_id == descriptor_id
                    ]
                    if len(matches) > 1:
                        return self._record_finding(
                            "configure_rejected",
                            descriptor_id,
                            RegistryStatus.INCOMPATIBLE,
                            "version_pin_required",
                        )
                return self._record_finding(
                    "configure_rejected",
                    descriptor_id,
                    RegistryStatus.INCOMPATIBLE,
                    "descriptor_not_found",
                )

            qualified_input = "@" in descriptor_id and descriptor_id != descriptor.descriptor_id
            pin = version_pin or descriptor.version_pin or (
                descriptor.version if qualified_input else None
            )
            if pin is not None and pin != descriptor.version:
                updated = replace(
                    descriptor,
                    status=RegistryStatus.INCOMPATIBLE,
                    enabled=False,
                    reason="version_pin_mismatch",
                    version_pin=pin,
                )
                self._replace(updated)
                return self._record_finding(
                    "configure_rejected",
                    descriptor.descriptor_id,
                    updated.status,
                    updated.reason,
                    changed=updated != descriptor,
                    descriptor=updated,
                )

            matches = [item for item in self._descriptors.values() if item.descriptor_id == descriptor.descriptor_id]
            if enabled and pin is None and len(matches) > 1:
                return self._record_finding(
                    "configure_rejected",
                    descriptor.descriptor_id,
                    RegistryStatus.INCOMPATIBLE,
                    "version_pin_required",
                    descriptor=descriptor,
                )

            if enabled:
                if descriptor.status is RegistryStatus.QUARANTINED:
                    return self._record_finding(
                        "configure_rejected",
                        descriptor.descriptor_id,
                        descriptor.status,
                        descriptor.reason,
                        descriptor=descriptor,
                    )
                if descriptor.status is RegistryStatus.UNSUPPORTED:
                    return self._record_finding(
                        "configure_rejected",
                        descriptor.descriptor_id,
                        descriptor.status,
                        descriptor.reason,
                        descriptor=descriptor,
                    )
                if not descriptor.provenance.verified:
                    updated = replace(
                        descriptor,
                        status=RegistryStatus.QUARANTINED,
                        enabled=False,
                        reason="signature_unverified",
                    )
                    self._replace(updated)
                    return self._record_finding(
                        "configure_rejected",
                        descriptor.descriptor_id,
                        updated.status,
                        updated.reason,
                        changed=updated != descriptor,
                        descriptor=updated,
                    )

                configured_scope: CapabilitySet | None = None
                if scope is not None:
                    configured_scope = self._coerce_capabilities(
                        scope,
                        base=descriptor.declared_capabilities,
                    )
                    if configured_scope.is_empty():
                        return self._record_finding(
                            "configure_rejected",
                            descriptor.descriptor_id,
                            RegistryStatus.INCOMPATIBLE,
                            "scope_empty",
                            descriptor=descriptor,
                        )
                updated = replace(
                    descriptor,
                    status=RegistryStatus.ENABLED,
                    enabled=True,
                    reason="enabled_by_operator",
                    version_pin=descriptor.version,
                    configured_scope=configured_scope,
                )
            else:
                preserved_status = descriptor.status in {
                    RegistryStatus.QUARANTINED,
                    RegistryStatus.UNSUPPORTED,
                    RegistryStatus.INCOMPATIBLE,
                }
                updated = replace(
                    descriptor,
                    status=(
                        descriptor.status
                        if preserved_status
                        else RegistryStatus.DISABLED
                    ),
                    enabled=False,
                    reason=(
                        descriptor.reason
                        if preserved_status
                        else "disabled_by_operator"
                    ),
                )

            changed = updated != descriptor
            self._replace(updated)
            return self._record_finding(
                "configured",
                updated.descriptor_id,
                updated.status,
                updated.reason,
                changed=changed,
                descriptor=updated,
            )

    def quarantine(self, descriptor_id: str, reason: str) -> RegistryFinding:
        """Disable a descriptor with a bounded, non-secret reason code."""

        with self._lock:
            descriptor = self._lookup(descriptor_id)
            if descriptor is None:
                return self._record_finding(
                    "quarantine_rejected",
                    descriptor_id,
                    RegistryStatus.INCOMPATIBLE,
                    "descriptor_not_found",
                )
            safe_reason = _safe_reason(reason, fallback="operator_quarantine")
            updated = replace(
                descriptor,
                status=RegistryStatus.QUARANTINED,
                enabled=False,
                reason=safe_reason,
            )
            self._replace(updated)
            return self._record_finding(
                "quarantined",
                descriptor.descriptor_id,
                updated.status,
                updated.reason,
                changed=updated != descriptor,
                descriptor=updated,
            )

    def resolve(
        self,
        descriptor_id: str,
        context: CapabilityContext | Mapping[str, Any],
    ) -> CapabilityDecision:
        """Return a side-effect-free policy decision for one descriptor."""

        with self._lock:
            descriptor = self._lookup(descriptor_id)
            if descriptor is None:
                return self._decision(
                    descriptor_id,
                    allowed=False,
                    reason="descriptor_not_found",
                )
            if isinstance(context, CapabilityContext):
                capability_context = context
            elif isinstance(context, Mapping):
                try:
                    capability_context = CapabilityContext.from_mapping(context)
                except (CapabilityError, TypeError, ValueError):
                    return self._decision(
                        descriptor.descriptor_id,
                        allowed=False,
                        reason="context_invalid",
                        descriptor=descriptor,
                        status=descriptor.status,
                    )
            else:
                return self._decision(
                    descriptor.descriptor_id,
                    allowed=False,
                    reason="context_invalid",
                    descriptor=descriptor,
                    status=descriptor.status,
                )

            if descriptor.kind is DescriptorKind.MCP:
                if descriptor.transport not in self._supported_mcp_transports:
                    return self._decision(
                        descriptor.descriptor_id,
                        allowed=False,
                        reason="mcp_transport_unsupported",
                        descriptor=descriptor,
                        status=RegistryStatus.UNSUPPORTED,
                        transport_status="unsupported",
                        requires_approval=True,
                    )

            if descriptor.status is not RegistryStatus.ENABLED or not descriptor.enabled:
                return self._decision(
                    descriptor.descriptor_id,
                    allowed=False,
                    reason=(
                        "descriptor_quarantined"
                        if descriptor.status is RegistryStatus.QUARANTINED
                        else "descriptor_not_enabled"
                    ),
                    descriptor=descriptor,
                    status=descriptor.status,
                    requires_approval=descriptor.approval_required,
                )

            effective = descriptor.declared_capabilities
            if descriptor.configured_scope is not None:
                effective = effective.intersect(descriptor.configured_scope)
            effective = effective.intersect(capability_context.policy)

            if effective.is_empty():
                return self._decision(
                    descriptor.descriptor_id,
                    allowed=False,
                    reason="capability_intersection_empty",
                    effective=effective,
                    descriptor=descriptor,
                    status=descriptor.status,
                    requires_approval=descriptor.approval_required,
                )
            if self._dimension_conflict(
                descriptor.declared_capabilities,
                capability_context.policy,
                effective,
            ):
                return self._decision(
                    descriptor.descriptor_id,
                    allowed=False,
                    reason="capability_intersection_empty",
                    effective=effective,
                    descriptor=descriptor,
                    status=descriptor.status,
                    requires_approval=descriptor.approval_required,
                )
            if not effective.allows_scope(
                world_id=capability_context.world_id,
                engram_id=capability_context.engram_id,
                turn_id=capability_context.turn_id,
            ):
                return self._decision(
                    descriptor.descriptor_id,
                    allowed=False,
                    reason="scope_denied",
                    effective=effective,
                    descriptor=descriptor,
                    status=descriptor.status,
                    requires_approval=descriptor.approval_required,
                )
            if not effective.contains(capability_context.required):
                return self._decision(
                    descriptor.descriptor_id,
                    allowed=False,
                    reason="required_capability_not_granted",
                    effective=effective,
                    descriptor=descriptor,
                    status=descriptor.status,
                    requires_approval=descriptor.approval_required,
                )
            if descriptor.approval_required and not bool(
                getattr(capability_context, "approval_granted", False)
            ):
                return self._decision(
                    descriptor.descriptor_id,
                    allowed=False,
                    reason="approval_required",
                    effective=effective,
                    descriptor=descriptor,
                    status=descriptor.status,
                    requires_approval=True,
                )

            return self._decision(
                descriptor.descriptor_id,
                allowed=True,
                reason="capability_allowed_contract_only",
                effective=effective,
                descriptor=descriptor,
                status=descriptor.status,
                requires_approval=descriptor.approval_required,
                transport_status=("contract_only" if descriptor.kind is DescriptorKind.MCP else None),
            )

    def _decision(
        self,
        descriptor_id: str,
        *,
        allowed: bool,
        reason: str,
        effective: CapabilitySet | None = None,
        descriptor: Descriptor | None = None,
        status: RegistryStatus | None = None,
        requires_approval: bool = False,
        transport_status: str | None = None,
    ) -> CapabilityDecision:
        return CapabilityDecision(
            descriptor_id=descriptor_id,
            allowed=allowed,
            reason=_safe_reason(reason, fallback="policy_denied"),
            effective_capabilities=effective or CapabilitySet.empty(),
            status=status,
            requires_approval=requires_approval,
            transport_status=transport_status,
            descriptor=descriptor,
        )

    def _collect_sources(self, source_config: Any) -> list[tuple[str, Any]]:
        if source_config is None:
            return [("path", root) for root in self._allowed_source_roots]
        if isinstance(source_config, Mapping):
            # A manifest itself is accepted as an explicit, in-memory source.
            if "id" in source_config or "name" in source_config:
                return [("inline", dict(source_config))]
            items: list[Any] = []
            for key in ("roots", "manifests", "paths", "sources"):
                value = source_config.get(key, ())
                if isinstance(value, (str, Path, Mapping)):
                    items.append(value)
                elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
                    items.extend(value)
                elif value:
                    self._record_finding(
                        "source_rejected",
                        "registry",
                        RegistryStatus.INCOMPATIBLE,
                        "source_config_invalid",
                    )
            return self._normalize_source_items(items)
        if isinstance(source_config, (str, Path)):
            return [("path", source_config)]
        if isinstance(source_config, Sequence):
            return self._normalize_source_items(list(source_config))
        self._record_finding(
            "source_rejected",
            "registry",
            RegistryStatus.INCOMPATIBLE,
            "source_config_invalid",
        )
        return []

    def _normalize_source_items(self, items: Sequence[Any]) -> list[tuple[str, Any]]:
        normalized: list[tuple[str, Any]] = []
        for item in items:
            if isinstance(item, Mapping):
                if "id" in item or "name" in item:
                    normalized.append(("inline", dict(item)))
                    continue
                if "manifest" in item and isinstance(item["manifest"], Mapping):
                    normalized.append(("inline", dict(item["manifest"])))
                    continue
                registry_id = item.get("registry_id")
                if registry_id is not None:
                    try:
                        registry_id = _safe_identifier(registry_id, field_name="registry_id")
                    except ValueError:
                        registry_id = None
                    if registry_id not in self._allowed_registry_ids:
                        self._record_finding(
                            "source_rejected",
                            "registry",
                            RegistryStatus.QUARANTINED,
                            "registry_source_not_allowlisted",
                        )
                    else:
                        self._record_finding(
                            "source_rejected",
                            registry_id,
                            RegistryStatus.UNSUPPORTED,
                            "remote_registry_fetch_not_supported",
                        )
                    continue
                path = item.get("path", item.get("root"))
                if path is not None:
                    normalized.append(("path", path))
                    continue
                self._record_finding(
                    "source_rejected",
                    "registry",
                    RegistryStatus.INCOMPATIBLE,
                    "source_entry_invalid",
                )
                continue
            if isinstance(item, (str, Path)):
                normalized.append(("path", item))
            else:
                self._record_finding(
                    "source_rejected",
                    "registry",
                    RegistryStatus.INCOMPATIBLE,
                    "source_entry_invalid",
                )
        return normalized

    @staticmethod
    def _dimension_conflict(
        declared: CapabilitySet,
        policy: CapabilitySet,
        effective: CapabilitySet,
    ) -> bool:
        """Reject a policy that explicitly removes a declared dimension.

        A policy may omit dimensions it does not request, but when it names a
        tool, root, domain, process class, secret level, or transport, an
        empty intersection means that the requested operation is not allowed.
        """

        return (
            (bool(declared.tool_names) and bool(policy.tool_names) and not effective.tool_names)
            or (
                bool(declared.filesystem_roots)
                and bool(policy.filesystem_roots)
                and not effective.filesystem_roots
            )
            or (
                bool(declared.network_domains)
                and bool(policy.network_domains)
                and not effective.network_domains
            )
            or (
                bool(declared.process_classes)
                and bool(policy.process_classes)
                and not effective.process_classes
            )
            or (
                bool(declared.secrets_access)
                and bool(policy.secrets_access)
                and not effective.secrets_access
            )
            or (
                bool(declared.mcp_transports)
                and bool(policy.mcp_transports)
                and not effective.mcp_transports
            )
        )

    def _resolve_configured_root(self, raw: str | Path) -> Path:
        resolved = self._resolve_source_path(raw, explicit=True)
        if resolved is None:
            raise ValueError("allowed source root must be inside workspace_root")
        return resolved

    def _resolve_source_path(self, raw: Any, *, explicit: bool) -> Path | None:
        if not isinstance(raw, (str, Path)):
            return None
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = self._workspace_root / candidate
        try:
            resolved = candidate.resolve()
        except OSError:
            return None
        try:
            resolved.relative_to(self._workspace_root)
        except ValueError:
            return None
        if self._allowed_source_roots and not any(
            resolved == root or root in resolved.parents
            for root in self._allowed_source_roots
        ):
            return None
        return resolved

    def _manifest_paths(self, raw: Any) -> list[Path]:
        path = self._resolve_source_path(raw, explicit=True)
        if path is None:
            self._record_finding(
                "source_rejected",
                "registry",
                RegistryStatus.QUARANTINED,
                "source_path_not_allowlisted",
            )
            return []
        if path.is_file():
            if path.suffix.lower() not in _MANIFEST_SUFFIXES:
                self._record_finding(
                    "manifest_rejected",
                    "registry",
                    RegistryStatus.INCOMPATIBLE,
                    "manifest_suffix_unsupported",
                )
                return []
            return [path]
        if not path.is_dir():
            self._record_finding(
                "source_rejected",
                "registry",
                RegistryStatus.INCOMPATIBLE,
                "source_path_missing",
            )
            return []

        paths: list[Path] = []
        for current, dirs, files in os.walk(path, topdown=True, followlinks=False):
            current_path = Path(current)
            dirs[:] = sorted(
                name
                for name in dirs
                if not (current_path / name).is_symlink()
            )
            for name in sorted(files):
                candidate = current_path / name
                if candidate.name not in _MANIFEST_NAMES:
                    continue
                if candidate.is_symlink():
                    continue
                resolved = self._resolve_source_path(candidate, explicit=False)
                if resolved is None:
                    continue
                paths.append(resolved)
                if len(paths) >= self._max_scan_files:
                    self._record_finding(
                        "discover_limit",
                        "registry",
                        RegistryStatus.UNSUPPORTED,
                        "scan_file_limit_reached",
                    )
                    return paths
        return paths

    def _discover_inline(self, manifest: Mapping[str, Any]) -> Descriptor | None:
        try:
            raw_bytes = _canonical_bytes(manifest)
            if len(raw_bytes) > self._max_manifest_bytes:
                self._record_finding(
                    "manifest_rejected",
                    "registry",
                    RegistryStatus.QUARANTINED,
                    "manifest_too_large",
                )
                return None
            descriptor = self._build_descriptor(
                manifest,
                raw_bytes=raw_bytes,
                source_id=self._inline_source_id(manifest),
                source_kind="inline",
                relative_path=None,
            )
        except (CapabilityError, TypeError, ValueError, KeyError):
            self._record_finding(
                "manifest_rejected",
                "registry",
                RegistryStatus.QUARANTINED,
                "manifest_invalid",
            )
            return None
        return self._add_descriptor(descriptor)

    def _discover_file(self, path: Path) -> Descriptor | None:
        try:
            raw_bytes = path.read_bytes()
            if len(raw_bytes) > self._max_manifest_bytes:
                self._record_finding(
                    "manifest_rejected",
                    "registry",
                    RegistryStatus.QUARANTINED,
                    "manifest_too_large",
                )
                return None
            if path.suffix.lower() == ".toml":
                manifest = tomllib.loads(raw_bytes.decode("utf-8"))
            else:
                manifest = json.loads(raw_bytes.decode("utf-8"))
            if not isinstance(manifest, Mapping):
                raise ValueError("manifest must be an object")
            relative = path.relative_to(self._workspace_root).as_posix()
            descriptor = self._build_descriptor(
                manifest,
                raw_bytes=raw_bytes,
                source_id=self._local_source_id(relative),
                source_kind="local",
                relative_path=relative,
            )
        except (OSError, UnicodeError, json.JSONDecodeError, tomllib.TOMLDecodeError, CapabilityError, TypeError, ValueError, KeyError):
            self._record_finding(
                "manifest_rejected",
                "registry",
                RegistryStatus.QUARANTINED,
                "manifest_invalid",
            )
            return None
        return self._add_descriptor(descriptor)

    def _build_descriptor(
        self,
        manifest: Mapping[str, Any],
        *,
        raw_bytes: bytes,
        source_id: str,
        source_kind: str,
        relative_path: str | None,
    ) -> Descriptor:
        raw_id = manifest.get("id", manifest.get("name"))
        descriptor_id = _safe_identifier(raw_id, field_name="id")
        name = _safe_text(manifest.get("name", descriptor_id), limit=128) or descriptor_id
        version = _safe_version(manifest.get("version"))
        kind_value = str(manifest.get("kind", manifest.get("type", ""))).strip().lower()
        kind_value = _KIND_ALIASES.get(kind_value, kind_value)
        kind = DescriptorKind(kind_value)

        digest = hashlib.sha256(raw_bytes).hexdigest()
        declared_digest = manifest.get("digest")
        digest_invalid = False
        if declared_digest is not None:
            if not isinstance(declared_digest, str) or _DIGEST_RE.fullmatch(declared_digest.strip()) is None:
                digest_invalid = True
            elif declared_digest.strip().lower() != digest:
                digest_invalid = True

        signature_state, signature_key_id = self._signature_state(
            manifest.get("signature"), raw_bytes=raw_bytes, digest=digest
        )
        provenance = PluginProvenance(
            source_id=source_id,
            source_kind=source_kind,
            relative_path=relative_path,
            version=version,
            digest=digest,
            signature_state=signature_state,
            signature_key_id=signature_key_id,
        )

        capabilities_value = manifest.get(
            "declared_capabilities", manifest.get("capabilities", {})
        )
        declared_capabilities = CapabilitySet.from_mapping(capabilities_value)
        transport = self._manifest_transport(manifest.get("transport"))
        handshake = self._safe_handshake(manifest.get("handshake"))
        lifecycle = self._safe_lifecycle(manifest.get("lifecycle"), kind)
        location = self._safe_location(manifest.get("execution_location"), kind)
        approval_required = manifest.get("approval_required")
        if not isinstance(approval_required, bool):
            approval_required = _default_approval(kind)

        if digest_invalid:
            status = RegistryStatus.QUARANTINED
            reason = "digest_mismatch"
        elif signature_state is SignatureState.INVALID:
            status = RegistryStatus.QUARANTINED
            reason = "signature_invalid"
        elif signature_state is not SignatureState.VERIFIED:
            status = RegistryStatus.QUARANTINED
            reason = "signature_unverified"
        elif kind is DescriptorKind.MCP and transport not in self._supported_mcp_transports:
            status = RegistryStatus.UNSUPPORTED
            reason = "mcp_transport_unsupported"
        else:
            status = RegistryStatus.DISABLED
            reason = "discovered_not_enabled"

        descriptor_type: type[ToolDescriptor] = {
            DescriptorKind.TOOL: ToolDescriptor,
            DescriptorKind.MCP: MCPDescriptor,
            DescriptorKind.SKILL: SkillDescriptor,
            DescriptorKind.PLUGIN: PluginDescriptor,
            DescriptorKind.EXTENSION: ExtensionDescriptor,
            DescriptorKind.HOOK: HookDescriptor,
        }[kind]
        return descriptor_type(
            descriptor_id=descriptor_id,
            name=name,
            version=version,
            kind=kind,
            provenance=provenance,
            declared_capabilities=declared_capabilities,
            lifecycle=lifecycle,
            execution_location=location,
            status=status,
            reason=reason,
            enabled=False,
            transport=transport,
            handshake=handshake,
            approval_required=approval_required,
            description=manifest.get("description", ""),
        )

    def _add_descriptor(self, descriptor: Descriptor) -> Descriptor | None:
        key = (descriptor.descriptor_id, descriptor.version)
        with self._lock:
            existing = self._descriptors.get(key)
            if existing is not None:
                if existing.provenance.digest != descriptor.provenance.digest:
                    self._record_finding(
                        "duplicate_rejected",
                        descriptor.descriptor_id,
                        RegistryStatus.QUARANTINED,
                        "duplicate_id_version",
                        descriptor=existing,
                    )
                return existing
            self._descriptors[key] = descriptor
            return descriptor

    def _replace(self, descriptor: Descriptor) -> None:
        self._descriptors[(descriptor.descriptor_id, descriptor.version)] = descriptor

    def _lookup(self, descriptor_id: str, *, version_pin: str | None = None) -> Descriptor | None:
        if not isinstance(descriptor_id, str):
            return None
        candidate = descriptor_id.strip()
        if version_pin is not None:
            try:
                pin = _safe_version(version_pin)
            except ValueError:
                return None
            return self._descriptors.get((candidate, pin))
        if "@" in candidate:
            stable_id, pin = candidate.rsplit("@", 1)
            if (stable_id, pin) in self._descriptors:
                return self._descriptors[(stable_id, pin)]
        exact = [
            descriptor
            for (stable_id, _version), descriptor in self._descriptors.items()
            if stable_id == candidate
        ]
        if len(exact) == 1:
            return exact[0]
        if len(exact) > 1 and "@" in candidate:
            stable_id, pin = candidate.rsplit("@", 1)
            return self._descriptors.get((stable_id, pin))
        return None

    def _record_finding(
        self,
        action: str,
        descriptor_id: str,
        status: RegistryStatus,
        reason: str,
        *,
        changed: bool = False,
        descriptor: Descriptor | None = None,
    ) -> RegistryFinding:
        try:
            safe_id = _safe_identifier(descriptor_id, field_name="descriptor_id")
        except ValueError:
            safe_id = "registry"
        finding = RegistryFinding(
            action=_safe_reason(action, fallback="registry_action"),
            descriptor_id=safe_id,
            status=status,
            reason=_safe_reason(reason, fallback="registry_reason"),
            changed=changed,
            descriptor=descriptor,
        )
        if len(self._findings) >= _MAX_FINDINGS:
            del self._findings[: len(self._findings) - _MAX_FINDINGS + 1]
        self._findings.append(finding)
        return finding

    def _coerce_capabilities(
        self,
        value: CapabilitySet | Mapping[str, Any],
        *,
        base: CapabilitySet | None = None,
    ) -> CapabilitySet:
        if isinstance(value, CapabilitySet):
            return value
        parsed = CapabilitySet.from_mapping(value)
        if base is None:
            return parsed
        if not isinstance(value, Mapping):
            return parsed

        # A configuration scope is a partial limiter: omitted dimensions do
        # not erase the descriptor's declared permission.  An explicitly
        # empty list still means deny that dimension.
        aliases = {
            "tool_names": "tool_names",
            "tools": "tool_names",
            "filesystem_roots": "filesystem_roots",
            "filesystem": "filesystem_roots",
            "network_domains": "network_domains",
            "network": "network_domains",
            "process_classes": "process_classes",
            "process": "process_classes",
            "secrets_access": "secrets_access",
            "secrets": "secrets_access",
            "world_ids": "world_ids",
            "worlds": "world_ids",
            "engram_ids": "engram_ids",
            "engrams": "engram_ids",
            "turn_ids": "turn_ids",
            "turns": "turn_ids",
            "mcp_transports": "mcp_transports",
            "transports": "mcp_transports",
        }
        provided = {aliases[key] for key in value if key in aliases}
        return CapabilitySet(
            tool_names=(parsed.tool_names if "tool_names" in provided else base.tool_names),
            filesystem_roots=(
                parsed.filesystem_roots
                if "filesystem_roots" in provided
                else base.filesystem_roots
            ),
            network_domains=(
                parsed.network_domains
                if "network_domains" in provided
                else base.network_domains
            ),
            process_classes=(
                parsed.process_classes
                if "process_classes" in provided
                else base.process_classes
            ),
            secrets_access=(
                parsed.secrets_access
                if "secrets_access" in provided
                else base.secrets_access
            ),
            world_ids=(parsed.world_ids if "world_ids" in provided else base.world_ids),
            engram_ids=(
                parsed.engram_ids
                if "engram_ids" in provided
                else base.engram_ids
            ),
            turn_ids=(parsed.turn_ids if "turn_ids" in provided else base.turn_ids),
            mcp_transports=(
                parsed.mcp_transports
                if "mcp_transports" in provided
                else base.mcp_transports
            ),
        )

    def _signature_state(
        self,
        signature: Any,
        *,
        raw_bytes: bytes,
        digest: str,
    ) -> tuple[SignatureState, str | None]:
        state = SignatureState.MISSING
        key_id: str | None = None
        metadata: dict[str, str] = {}
        if isinstance(signature, Mapping):
            raw_state = signature.get("state", signature.get("status", ""))
            if isinstance(raw_state, str):
                try:
                    state = SignatureState(raw_state.strip().lower())
                except ValueError:
                    state = SignatureState.UNSUPPORTED
            key_id = _safe_optional_key(signature.get("key_id"))
            metadata = {
                key: _safe_text(signature.get(key), limit=128)
                for key in ("state", "status", "algorithm", "key_id")
                if isinstance(signature.get(key), str)
            }
        elif isinstance(signature, str) and signature.strip():
            try:
                state = SignatureState(signature.strip().lower())
            except ValueError:
                state = SignatureState.UNSUPPORTED

        if digest in self._trusted_digests:
            return SignatureState.VERIFIED, key_id
        if self._signature_verifier is not None:
            try:
                if self._signature_verifier(raw_bytes, metadata):
                    return SignatureState.VERIFIED, key_id
                if state is SignatureState.VERIFIED:
                    return SignatureState.INVALID, key_id
            except Exception:  # verifier failure is fail-closed
                return SignatureState.INVALID, key_id
        # A manifest cannot authenticate itself by declaring ``verified``.
        if state is SignatureState.VERIFIED:
            return SignatureState.UNVERIFIED, key_id
        return state, key_id

    @staticmethod
    def _validate_digest(value: Any) -> str:
        if not isinstance(value, str) or _DIGEST_RE.fullmatch(value.strip()) is None:
            raise ValueError("trusted digest must be a sha256 hex digest")
        return value.strip().lower()

    @staticmethod
    def _normalize_transport(value: Any) -> str:
        if isinstance(value, Mapping):
            value = value.get("type", value.get("name"))
        if not isinstance(value, str):
            return "unsupported"
        return _TRANSPORT_ALIASES.get(value.strip().lower(), "unsupported")

    def _manifest_transport(self, value: Any) -> str | None:
        if value is None:
            return None
        return self._normalize_transport(value)

    @staticmethod
    def _safe_lifecycle(value: Any, kind: DescriptorKind) -> str:
        if not isinstance(value, str):
            return _default_lifecycle(kind)
        candidate = value.strip().lower()
        return candidate if candidate in _KNOWN_LIFECYCLES else "static"

    @staticmethod
    def _safe_location(value: Any, kind: DescriptorKind) -> str:
        if not isinstance(value, str):
            return _default_location(kind)
        candidate = value.strip().lower()
        return candidate if candidate in _KNOWN_LOCATIONS else "server"

    @staticmethod
    def _safe_handshake(value: Any) -> tuple[tuple[str, str], ...]:
        if not isinstance(value, Mapping):
            return ()
        fields: list[tuple[str, str]] = []
        for key in sorted(value):
            if len(fields) >= _MAX_HANDSHAKE_FIELDS:
                break
            if key not in _SAFE_HANDSHAKE_KEYS:
                continue
            raw = value[key]
            if not isinstance(raw, (str, int, bool)):
                continue
            text = _safe_text(str(raw), limit=128)
            if text:
                fields.append((key, text))
        return tuple(fields)

    def _inline_source_id(self, manifest: Mapping[str, Any]) -> str:
        value = manifest.get("source_id", manifest.get("registry_id", "inline"))
        if not isinstance(value, str):
            return "inline"
        try:
            candidate = _safe_identifier(value, field_name="source_id")
        except ValueError:
            return "inline"
        # Inline manifests are untrusted input.  Only the registry's own
        # neutral marker is exposed verbatim; arbitrary source labels are
        # represented by a stable digest so a copied credential cannot become
        # provenance output.
        if candidate == "inline" or candidate.startswith(("local_", "registry_")):
            return candidate
        return f"inline_{hashlib.sha256(candidate.encode('utf-8')).hexdigest()[:16]}"

    @staticmethod
    def _local_source_id(relative_path: str) -> str:
        digest = hashlib.sha256(relative_path.encode("utf-8")).hexdigest()[:16]
        return f"local_{digest}"

"""Narrow MCP runtime service boundary.

This module owns the runtime-side registry around :mod:`mcp_transport`.
It deliberately does not load configuration, discover servers, implement an
approval broker, or claim an operating-system sandbox.  A caller must supply
an explicit descriptor, an explicit allowlist, a stable operation id and the
approval grant that was produced by an upper-layer policy service.

The transport reports a real MCP handshake as ``LIVE_MCP_TRANSPORT``.  This
service keeps the stronger runtime evidence at ``LIVE_GATE_UNVERIFIED``:
transport liveness is not approval evidence, and an ordinary subprocess is
not an OS sandbox.  ``LIVE`` is a reserved label for a future externally
bound live gate and is never inferred by this service.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
import time
from collections import OrderedDict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, TypedDict

from pulse_system.core.runtime.publication import RuntimePublicationPermit

from .mcp_transport import (
    EXECUTION_SAFETY_UNVERIFIED,
    MCPError,
    MCPRemoteError,
    MCPStdioTransport,
    MCPTransportCloseSummary,
    MCPCancelledError,
    MCPCapabilitySnapshot,
)
from .capabilities import CapabilityContext, CapabilitySet
from .tool_registry import RegistryStatus, ToolRegistry

__all__ = [
    "CONTRACT",
    "CONTRACT_ONLY",
    "EXECUTION_SAFETY_UNVERIFIED",
    "LIVE",
    "LIVE_GATE_UNVERIFIED",
    "MCPApprovalGrant",
    "MCPCallRequest",
    "MCPCallResult",
    "MCPCallScope",
    "MCPRuntimeConfig",
    "MCPRuntimeCloseSummary",
    "MCPRuntimeError",
    "MCPRuntimeEvidence",
    "MCPPhysicalOwnerKey",
    "MCPRegistryGate",
    "MCPRuntimeService",
    "MCPServerCapability",
    "MCPServerDescriptor",
    "MCPServerHandle",
    "MCPServerIdentity",
    "MCPServerNotAllowedError",
    "MCPServerState",
    "MCPApprovalRequiredError",
    "MCPApprovalMismatchError",
    "MCPActionBackend",
    "MCPCallExecutionError",
    "MCPOperationCollisionError",
]


CONTRACT = "CONTRACT"
# Alias for callers that use the older harness vocabulary.  The runtime
# service deliberately emits the shorter CONTRACT label in its summaries.
CONTRACT_ONLY = CONTRACT
LIVE_GATE_UNVERIFIED = "LIVE_GATE_UNVERIFIED"
LIVE = "LIVE"

_STDIO_TRANSPORT = "stdio"
_DEFAULT_PROTOCOL_VERSION = "2025-06-18"
_MAX_SERVER_ID_BYTES = 128
_MAX_DISPLAY_NAME_BYTES = 256
_MAX_IDENTIFIER_BYTES = 192
_MAX_ARGUMENT_BYTES = 64 * 1024
_MAX_ENV_VALUE_BYTES = 8 * 1024
_MAX_ENV_KEYS = 64
_MAX_SERVERS = 256
_MAX_RETAINED_CALLS = 4096
_MAX_ACTIVE_CALLS = 256
_MAX_TOOLS_IN_SUMMARY = 256
_MAX_TIMEOUT_SECONDS = 300.0
_MAX_ARTIFACTS = 64
_MAX_ARTIFACT_BYTES = 256 * 1024 * 1024
_MAX_CONNECT_WAIT_SECONDS = 300.0
_DEFAULT_CLOSE_TIMEOUT_SECONDS = 2.0
_MAX_TOOL_NAME_BYTES = 128
_TRANSPORT_CLOSE_SUMMARY_KEYS = frozenset({
    "active_before",
    "unresolved",
    "transport_owners_unresolved",
    "reader_owners_unresolved",
    "process_roots_observed",
    "process_root_owners_unresolved",
    "owner_joined",
    "process_tree_state",
})
_DIGEST_RE = re.compile(r"\A[a-f0-9]{64}\Z")
_OWNER_TOKEN_RE = re.compile(r"\A[a-f0-9]{32}\Z")
_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_.:/-]{0,191}\Z")
_SERVER_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_ENV_NAME_RE = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_TOOL_NAME_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_PROTOCOL_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_EVIDENCE_VALUES = frozenset({CONTRACT, LIVE_GATE_UNVERIFIED, LIVE})


class MCPRuntimeEvidence(StrEnum):
    """Evidence labels owned by this service boundary."""

    CONTRACT = CONTRACT
    CONTRACT_ONLY = CONTRACT
    LIVE_GATE_UNVERIFIED = LIVE_GATE_UNVERIFIED
    LIVE = LIVE


class MCPRuntimeCloseSummary(TypedDict):
    """Payload-free aggregate evidence for the MCP execution domain."""

    active_before: int
    unresolved: int
    active_calls_before: int
    active_calls_unresolved: int
    connect_owners_unresolved: int
    transports_observed: int
    transport_owners_unresolved: int
    reader_owners_unresolved: int
    process_roots_observed: int
    process_root_owners_unresolved: int
    owner_joined: bool
    process_tree_state: Literal[
        "not_applicable",
        "empty_verified",
        "root_exit_only",
        "unknown",
    ]


class MCPRegistryGate:
    """Bind explicit runtime descriptors to the Harness capability registry.

    ``MCPRuntimeService`` deliberately accepts only explicit descriptors, but
    explicit configuration alone is not ecosystem governance.  This gate
    derives credential-free manifests from those descriptors, pins their
    identity digests in :class:`ToolRegistry`, requires an operator-enabled
    stdio capability, and re-resolves that capability immediately before the
    approved transport boundary is crossed.

    Discovery remains side-effect free.  In particular, constructing or
    previewing this gate never starts an MCP process.
    """

    _VERSION = "1.0.0"

    def __init__(
        self,
        descriptors: Iterable["MCPServerDescriptor"],
        *,
        workspace_root: str | Path,
    ) -> None:
        descriptor_list = tuple(descriptors)
        manifests: list[dict[str, Any]] = []
        trusted_digests: list[str] = []
        self._descriptor_ids: dict[str, str] = {}
        self._runtime_digests: dict[str, str] = {}
        for descriptor in descriptor_list:
            if not isinstance(descriptor, MCPServerDescriptor):
                raise TypeError("registry descriptors must be MCPServerDescriptor values")
            descriptor_id = f"mcp.{descriptor.server_id}"
            identity_digest = descriptor.identity.descriptor_digest
            if descriptor.server_id in self._descriptor_ids:
                raise ValueError("duplicate MCP registry server id")
            manifest = {
                "id": descriptor_id,
                "name": f"MCP server {descriptor.server_id}",
                "kind": "mcp",
                "version": self._VERSION,
                "description": "Explicit local MCP stdio descriptor",
                "declared_capabilities": {"mcp_transports": ["stdio"]},
                "lifecycle": "session",
                "execution_location": "local_process",
                "transport": "stdio",
                "handshake": {
                    "protocol_version": descriptor.protocol_version,
                    "capabilities_digest": identity_digest,
                    "transport": "stdio",
                },
                "approval_required": True,
            }
            raw = json.dumps(
                manifest,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
            manifests.append(manifest)
            trusted_digests.append(hashlib.sha256(raw).hexdigest())
            self._descriptor_ids[descriptor.server_id] = descriptor_id
            self._runtime_digests[descriptor.server_id] = identity_digest

        self._registry = ToolRegistry(
            workspace_root=workspace_root,
            supported_mcp_transports=("stdio",),
            trusted_digests=trusted_digests,
            max_descriptors=max(1, len(manifests)),
        )
        discovered = self._registry.discover({"manifests": manifests})
        if len(discovered) != len(manifests):
            raise ValueError("MCP registry discovery did not preserve every descriptor")
        for descriptor_id in self._descriptor_ids.values():
            finding = self._registry.configure(
                descriptor_id,
                True,
                version_pin=self._VERSION,
                scope={"mcp_transports": ["stdio"]},
            )
            if finding.status is not RegistryStatus.ENABLED:
                raise ValueError(f"MCP registry refused {descriptor_id}: {finding.reason}")

    @property
    def registry(self) -> ToolRegistry:
        return self._registry

    def authorize(
        self,
        *,
        server_id: str,
        descriptor_digest: str,
        world_id: str,
        engram_id: str,
        turn_id: str,
        approval_granted: bool,
    ) -> Mapping[str, Any]:
        descriptor_id = self._descriptor_ids.get(server_id)
        if descriptor_id is None:
            raise MCPRuntimeError("registry_descriptor_missing", status=403)
        if self._runtime_digests.get(server_id) != descriptor_digest:
            raise MCPRuntimeError("registry_descriptor_mismatch", status=409)
        transport = CapabilitySet(mcp_transports={"stdio"})
        decision = self._registry.resolve(
            descriptor_id,
            CapabilityContext(
                world_id=world_id,
                engram_id=engram_id,
                turn_id=turn_id,
                policy=transport,
                required=transport,
                approval_granted=approval_granted,
            ),
        )
        preview_allowed = (
            not approval_granted
            and decision.reason == "approval_required"
            and decision.status is RegistryStatus.ENABLED
        )
        if not decision.allowed and not preview_allowed:
            reason = re.sub(r"[^a-z0-9_.-]", "_", decision.reason.lower())[:80]
            raise MCPRuntimeError(f"registry_{reason or 'denied'}", status=403)
        descriptor = decision.descriptor
        if descriptor is None:
            raise MCPRuntimeError("registry_descriptor_missing", status=403)
        return {
            "registry_descriptor_id": descriptor_id,
            "registry_provenance_digest": descriptor.provenance.digest,
            "registry_status": descriptor.status.value,
            "registry_reason": decision.reason,
            "registry_evidence_class": decision.evidence_class.value,
        }

    def summary(self) -> Mapping[str, Any]:
        descriptors = self._registry.descriptors()
        return {
            "attached": True,
            "descriptors": len(descriptors),
            "enabled": sum(item.status is RegistryStatus.ENABLED for item in descriptors),
            "supported_transports": list(self._registry.supported_mcp_transports),
            "evidence_class": CONTRACT,
            "live_gate": LIVE_GATE_UNVERIFIED,
        }


class MCPServerState(StrEnum):
    DECLARED = "DECLARED"
    CONNECTING = "CONNECTING"
    READY = "READY"
    FAILED = "FAILED"
    BROKEN = "BROKEN"
    CLOSED = "CLOSED"


class MCPRuntimeError(RuntimeError):
    """A bounded, non-secret error from the MCP runtime service."""

    def __init__(self, code: str, *, status: int = 409) -> None:
        if not isinstance(code, str) or not _ID_RE.fullmatch(code):
            raise ValueError("MCP runtime error code is invalid")
        self.code = code
        self.status = status
        super().__init__(code)

    def to_safe_dict(self) -> dict[str, Any]:
        return {"error_code": self.code, "status": self.status}


class MCPServerNotAllowedError(MCPRuntimeError):
    def __init__(self, server_id: str) -> None:
        self.server_id = server_id
        super().__init__("server_not_allowlisted", status=403)


class MCPApprovalRequiredError(MCPRuntimeError):
    def __init__(self) -> None:
        super().__init__("approval_required", status=403)


class MCPApprovalMismatchError(MCPRuntimeError):
    def __init__(self) -> None:
        super().__init__("approval_mismatch", status=409)


class MCPOperationCollisionError(MCPRuntimeError):
    def __init__(self) -> None:
        super().__init__("operation_scope_collision", status=409)


class MCPCallExecutionError(MCPRuntimeError):
    """A transport failure projected without returning remote payload data."""

    def __init__(self, code: str, *, operation_id: str, server_id: str) -> None:
        self.operation_id = operation_id
        self.server_id = server_id
        super().__init__(code, status=502)

    def to_safe_dict(self) -> dict[str, Any]:
        result = super().to_safe_dict()
        result.update(
            {
                "operation_id": self.operation_id,
                "server_id": self.server_id,
            }
        )
        return result


def _bounded_text(value: Any, *, field_name: str, maximum: int) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"{field_name} must be a non-empty string")
    if len(value.encode("utf-8", errors="strict")) > maximum:
        raise ValueError(f"{field_name} is too large")
    return value


def _bounded_env_value(value: Any) -> str:
    if not isinstance(value, str) or "\x00" in value:
        raise ValueError("environment value must be a string without NUL")
    if len(value.encode("utf-8", errors="strict")) > _MAX_ENV_VALUE_BYTES:
        raise ValueError("environment value is too large")
    return value


def _bounded_identifier(value: Any, *, field_name: str, server: bool = False) -> str:
    maximum = _MAX_SERVER_ID_BYTES if server else _MAX_IDENTIFIER_BYTES
    result = _bounded_text(value, field_name=field_name, maximum=maximum)
    pattern = _SERVER_ID_RE if server else _ID_RE
    if pattern.fullmatch(result) is None:
        raise ValueError(f"{field_name} has an invalid shape")
    return result


def _bounded_tool_name(value: Any) -> str:
    result = _bounded_text(value, field_name="tool_name", maximum=_MAX_TOOL_NAME_BYTES)
    if _TOOL_NAME_RE.fullmatch(result) is None:
        raise ValueError("tool_name has an invalid shape")
    return result


def _bounded_timeout(value: Any, *, field_name: str, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be numeric")
    result = float(value)
    if not 0.01 <= result <= maximum:
        raise ValueError(f"{field_name} is outside the bounded range")
    return result


def _bounded_positive_int(value: Any, *, field_name: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{field_name} is outside the bounded range")
    return value


def _canonical_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonical_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_json(item) for item in value]
    return value


def _digest(value: Any) -> str:
    encoded = json.dumps(
        _canonical_json(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_digest(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a sha256 digest")
    return value


def _bound_json(value: Any, *, field_name: str, depth: int = 0) -> Any:
    if depth > 8:
        raise ValueError(f"{field_name} exceeds maximum nesting")
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        if "\x00" in value or len(value.encode("utf-8")) > _MAX_ARGUMENT_BYTES:
            raise ValueError(f"{field_name} contains oversized text")
        return value
    if isinstance(value, Mapping):
        if len(value) > 256:
            raise ValueError(f"{field_name} contains too many keys")
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key or len(key) > 128 or "\x00" in key:
                raise ValueError(f"{field_name} contains an invalid key")
            result[key] = _bound_json(item, field_name=field_name, depth=depth + 1)
        return result
    if isinstance(value, (list, tuple)):
        if len(value) > 256:
            raise ValueError(f"{field_name} contains too many items")
        return [_bound_json(item, field_name=field_name, depth=depth + 1) for item in value]
    raise ValueError(f"{field_name} contains an unsupported value")


def _is_cancelled(value: Any) -> bool:
    if value is None:
        return False
    method = getattr(value, "is_set", None)
    if callable(method):
        try:
            return bool(method())
        except Exception:
            return True
    return bool(getattr(value, "cancelled", False) or getattr(value, "aborted", False))


def _absolute_deadline(value: float | None) -> float:
    if value is None:
        return time.monotonic() + _DEFAULT_CLOSE_TIMEOUT_SECONDS
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("deadline must be a finite monotonic timestamp")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("deadline must be a finite monotonic timestamp")
    return result


class _CancellationRelay:
    """Combine caller cancellation and service-owned close/cancel signals."""

    def __init__(self, *signals: Any) -> None:
        self._signals = tuple(signal for signal in signals if signal is not None)

    def is_set(self) -> bool:
        return any(_is_cancelled(signal) for signal in self._signals)


@dataclass(frozen=True, slots=True)
class MCPRuntimeConfig:
    """Limits for the explicit in-memory registry and call projection."""

    max_servers: int = 64
    max_active_calls: int = 64
    max_retained_calls: int = 1024
    max_connect_wait_sec: float = 30.0
    max_call_wait_sec: float = 300.0
    max_summary_tools: int = 256

    def __post_init__(self) -> None:
        _bounded_positive_int(self.max_servers, field_name="max_servers", maximum=_MAX_SERVERS)
        _bounded_positive_int(
            self.max_active_calls,
            field_name="max_active_calls",
            maximum=_MAX_ACTIVE_CALLS,
        )
        _bounded_positive_int(
            self.max_retained_calls,
            field_name="max_retained_calls",
            maximum=_MAX_RETAINED_CALLS,
        )
        if self.max_active_calls > self.max_retained_calls:
            raise ValueError("max_active_calls cannot exceed max_retained_calls")
        _bounded_timeout(
            self.max_connect_wait_sec,
            field_name="max_connect_wait_sec",
            maximum=_MAX_CONNECT_WAIT_SECONDS,
        )
        _bounded_timeout(
            self.max_call_wait_sec,
            field_name="max_call_wait_sec",
            maximum=_MAX_TIMEOUT_SECONDS,
        )
        _bounded_positive_int(
            self.max_summary_tools,
            field_name="max_summary_tools",
            maximum=_MAX_TOOLS_IN_SUMMARY,
        )


@dataclass(frozen=True, slots=True)
class MCPServerIdentity:
    """Credential-free identity of one explicit server descriptor."""

    server_id: str
    transport: str
    descriptor_digest: str
    argv_digest: str
    cwd_digest: str
    env_digest: str
    artifact_digest: str

    def __post_init__(self) -> None:
        _bounded_identifier(self.server_id, field_name="server_id", server=True)
        if self.transport != _STDIO_TRANSPORT:
            raise ValueError("unsupported MCP transport")
        for name in (
            "descriptor_digest",
            "argv_digest",
            "cwd_digest",
            "env_digest",
            "artifact_digest",
        ):
            _validate_digest(getattr(self, name), field_name=name)

    def to_safe_dict(self) -> dict[str, str]:
        return {
            "server_id": self.server_id,
            "transport": self.transport,
            "descriptor_digest": self.descriptor_digest,
            "argv_digest": self.argv_digest,
            "cwd_digest": self.cwd_digest,
            "env_digest": self.env_digest,
            "artifact_digest": self.artifact_digest,
        }


@dataclass(frozen=True, slots=True)
class MCPServerDescriptor:
    """An explicit stdio server descriptor; never loaded from ambient config."""

    server_id: str
    argv: tuple[str, ...]
    cwd: Path | str
    env: Mapping[str, str] = field(default_factory=dict, repr=False)
    env_allowlist: tuple[str, ...] = ()
    reserved_tool_names: tuple[str, ...] = ()
    protocol_version: str = _DEFAULT_PROTOCOL_VERSION
    request_timeout: float = 10.0
    max_timeout: float = 60.0
    max_tools: int = 256
    artifact_paths: tuple[Path | str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        server_id = _bounded_identifier(self.server_id, field_name="server_id", server=True)
        object.__setattr__(self, "server_id", server_id)

        if isinstance(self.argv, (str, bytes, bytearray)) or not isinstance(self.argv, Sequence):
            raise ValueError("argv must be an explicit sequence")
        if not 1 <= len(self.argv) <= 64:
            raise ValueError("argv must contain between 1 and 64 entries")
        argv: list[str] = []
        for item in self.argv:
            value = _bounded_text(item, field_name="argv entry", maximum=8192)
            argv.append(value)
        object.__setattr__(self, "argv", tuple(argv))

        candidate_cwd = Path(self.cwd)
        if not candidate_cwd.is_absolute():
            raise ValueError("cwd must be absolute")
        try:
            resolved_cwd = candidate_cwd.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise ValueError("cwd must resolve to an existing directory") from exc
        if not resolved_cwd.is_dir():
            raise ValueError("cwd must be a directory")
        object.__setattr__(self, "cwd", resolved_cwd)

        if isinstance(self.artifact_paths, (str, bytes, bytearray)):
            raise ValueError("artifact_paths must be a sequence")
        artifacts: list[Path] = []
        explicit = list(self.artifact_paths)
        inferred: list[Path | str] = [self.argv[0]]
        inferred.extend(
            item for item in self.argv[1:]
            if not item.startswith("-")
        )
        for raw_path in [*explicit, *inferred]:
            candidate = Path(raw_path)
            if not candidate.is_absolute():
                candidate = resolved_cwd / candidate
            try:
                resolved = candidate.resolve(strict=True)
            except (OSError, RuntimeError):
                if raw_path in explicit or raw_path == self.argv[0]:
                    raise ValueError("declared MCP artifact must exist") from None
                continue
            if not resolved.is_file():
                if raw_path in explicit or raw_path == self.argv[0]:
                    raise ValueError("declared MCP artifact must be a file")
                continue
            if resolved not in artifacts:
                artifacts.append(resolved)
        if not artifacts or len(artifacts) > _MAX_ARTIFACTS:
            raise ValueError("MCP artifact set is empty or exceeds its bound")
        object.__setattr__(self, "artifact_paths", tuple(artifacts))

        allowlist: dict[str, str] = {}
        if isinstance(self.env_allowlist, (str, bytes, bytearray)):
            raise ValueError("env_allowlist must be a sequence")
        for item in self.env_allowlist:
            name = _bounded_text(item, field_name="environment key", maximum=128)
            if _ENV_NAME_RE.fullmatch(name) is None:
                raise ValueError("environment key has an invalid shape")
            key = name.upper() if os.name == "nt" else name
            if key in allowlist and allowlist[key] != name:
                raise ValueError("environment allowlist contains a case collision")
            allowlist[key] = name
        if len(allowlist) > _MAX_ENV_KEYS:
            raise ValueError("environment allowlist is too large")
        if not isinstance(self.env, Mapping):
            raise ValueError("env must be a mapping")
        env: dict[str, str] = {}
        for raw_name, raw_value in self.env.items():
            name = _bounded_text(raw_name, field_name="environment key", maximum=128)
            if _ENV_NAME_RE.fullmatch(name) is None:
                raise ValueError("environment key has an invalid shape")
            key = name.upper() if os.name == "nt" else name
            if key not in allowlist:
                raise ValueError("environment key is not in the explicit allowlist")
            value = _bounded_env_value(raw_value)
            env[allowlist[key]] = value
        object.__setattr__(self, "env_allowlist", tuple(sorted(allowlist.values())))
        object.__setattr__(self, "env", MappingProxyType(dict(sorted(env.items()))))

        if isinstance(self.reserved_tool_names, (str, bytes, bytearray)):
            raise ValueError("reserved_tool_names must be a sequence")
        reserved: set[str] = set()
        for item in self.reserved_tool_names:
            name = _bounded_tool_name(item)
            reserved.add(name)
        object.__setattr__(self, "reserved_tool_names", tuple(sorted(reserved)))

        protocol = _bounded_text(
            self.protocol_version,
            field_name="protocol_version",
            maximum=64,
        )
        if _PROTOCOL_RE.fullmatch(protocol) is None:
            raise ValueError("protocol_version has an invalid shape")
        object.__setattr__(self, "protocol_version", protocol)
        object.__setattr__(
            self,
            "request_timeout",
            _bounded_timeout(
                self.request_timeout,
                field_name="request_timeout",
                maximum=_MAX_TIMEOUT_SECONDS,
            ),
        )
        object.__setattr__(
            self,
            "max_timeout",
            _bounded_timeout(
                self.max_timeout,
                field_name="max_timeout",
                maximum=_MAX_TIMEOUT_SECONDS,
            ),
        )
        if self.request_timeout > self.max_timeout:
            raise ValueError("request_timeout cannot exceed max_timeout")
        object.__setattr__(
            self,
            "max_tools",
            _bounded_positive_int(self.max_tools, field_name="max_tools", maximum=256),
        )

    @property
    def identity(self) -> MCPServerIdentity:
        argv_digest = _digest(self.argv)
        cwd_digest = _digest(str(self.cwd))
        artifact_facts: list[dict[str, Any]] = []
        for index, path in enumerate(self.artifact_paths):
            try:
                stat = path.stat()
                if not path.is_file() or stat.st_size > _MAX_ARTIFACT_BYTES:
                    raise ValueError("MCP artifact is unavailable or exceeds its bound")
                hasher = hashlib.sha256()
                with path.open("rb") as handle:
                    while chunk := handle.read(1024 * 1024):
                        hasher.update(chunk)
            except (OSError, RuntimeError) as exc:
                raise ValueError("MCP artifact fingerprint is unavailable") from exc
            artifact_facts.append(
                {"index": index, "size": stat.st_size, "sha256": hasher.hexdigest()}
            )
        artifact_digest = _digest(artifact_facts)
        env_digest = _digest(
            {
                "allowlist": self.env_allowlist,
                "values": {name: _digest(value) for name, value in self.env.items()},
            }
        )
        descriptor_digest = _digest(
            {
                "server_id": self.server_id,
                "transport": _STDIO_TRANSPORT,
                "argv_digest": argv_digest,
                "cwd_digest": cwd_digest,
                "env_digest": env_digest,
                "artifact_digest": artifact_digest,
                "reserved_tool_names": self.reserved_tool_names,
                "protocol_version": self.protocol_version,
                "request_timeout": self.request_timeout,
                "max_timeout": self.max_timeout,
                "max_tools": self.max_tools,
            }
        )
        return MCPServerIdentity(
            server_id=self.server_id,
            transport=_STDIO_TRANSPORT,
            descriptor_digest=descriptor_digest,
            argv_digest=argv_digest,
            cwd_digest=cwd_digest,
            env_digest=env_digest,
            artifact_digest=artifact_digest,
        )

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "server_id": self.server_id,
            "transport": _STDIO_TRANSPORT,
            "identity": self.identity.to_safe_dict(),
            "protocol_version": self.protocol_version,
            "env_keys": list(self.env_allowlist),
            "reserved_tool_names": list(self.reserved_tool_names),
            "approval_required": True,
            "evidence_class": CONTRACT,
            "execution_safety": EXECUTION_SAFETY_UNVERIFIED,
        }

    def build_transport(
        self,
        *,
        publication_permit: RuntimePublicationPermit | None = None,
    ) -> MCPStdioTransport:
        """Build the only transport permitted by this descriptor."""

        return MCPStdioTransport(
            argv=self.argv,
            cwd=self.cwd,
            env=dict(self.env),
            env_allowlist=self.env_allowlist,
            reserved_tool_names=self.reserved_tool_names,
            protocol_version=self.protocol_version,
            request_timeout=self.request_timeout,
            max_timeout=self.max_timeout,
            max_tools=self.max_tools,
            publication_permit=publication_permit,
        )


@dataclass(frozen=True, slots=True)
class MCPServerCapability:
    """Safe live capability view; schemas and descriptions remain private."""

    server_id: str
    protocol_version: str
    server_name: str
    server_version: str
    tool_names: tuple[str, ...]
    tool_schema_digests: tuple[tuple[str, str], ...]
    server_capability_digest: str
    capability_digest: str
    evidence_class: str = LIVE_GATE_UNVERIFIED
    execution_safety: str = EXECUTION_SAFETY_UNVERIFIED

    def __post_init__(self) -> None:
        _bounded_identifier(self.server_id, field_name="server_id", server=True)
        _bounded_text(self.protocol_version, field_name="protocol_version", maximum=64)
        _bounded_text(self.server_name, field_name="server_name", maximum=256)
        _bounded_text(self.server_version, field_name="server_version", maximum=256)
        if len(self.tool_names) > _MAX_TOOLS_IN_SUMMARY:
            raise ValueError("tool_names exceeds summary bound")
        for name in self.tool_names:
            _bounded_tool_name(name)
        if tuple(sorted(self.tool_names)) != self.tool_names:
            raise ValueError("tool_names must be sorted")
        if len(self.tool_schema_digests) != len(self.tool_names):
            raise ValueError("tool schema digest count does not match tools")
        for name, digest in self.tool_schema_digests:
            if name not in self.tool_names:
                raise ValueError("tool schema digest has an unknown tool")
            _validate_digest(digest, field_name="tool_schema_digest")
        _validate_digest(self.server_capability_digest, field_name="server_capability_digest")
        _validate_digest(self.capability_digest, field_name="capability_digest")
        if self.evidence_class not in _EVIDENCE_VALUES:
            raise ValueError("unsupported MCP evidence class")
        if self.execution_safety != EXECUTION_SAFETY_UNVERIFIED:
            raise ValueError("MCP runtime cannot claim execution safety")

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "server_id": self.server_id,
            "protocol_version": self.protocol_version,
            "server_name": self.server_name,
            "server_version": self.server_version,
            "tool_names": list(self.tool_names),
            "tool_schema_digests": {
                name: digest for name, digest in self.tool_schema_digests
            },
            "server_capability_digest": self.server_capability_digest,
            "capability_digest": self.capability_digest,
            "evidence_class": self.evidence_class,
            "execution_safety": self.execution_safety,
            "approval_required": True,
        }


@dataclass(frozen=True, slots=True)
class MCPServerHandle:
    """Opaque-to-execution handle returned after explicit connection."""

    identity: MCPServerIdentity
    capability: MCPServerCapability
    state: MCPServerState = MCPServerState.READY

    @property
    def server_id(self) -> str:
        return self.identity.server_id

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "identity": self.identity.to_safe_dict(),
            "capability": self.capability.to_safe_dict(),
        }


@dataclass(frozen=True, slots=True)
class MCPCallScope:
    """Stable parent scope required for every tool invocation."""

    world_id: str
    engram_id: str
    turn_id: str
    epoch: int

    def __post_init__(self) -> None:
        for name in ("world_id", "engram_id", "turn_id"):
            _bounded_identifier(getattr(self, name), field_name=name)
        if type(self.epoch) is not int or self.epoch < 1:
            raise ValueError("epoch must be an integer >= 1")

    @property
    def digest(self) -> str:
        return _digest(
            {
                "world_id": self.world_id,
                "engram_id": self.engram_id,
                "turn_id": self.turn_id,
                "epoch": self.epoch,
            }
        )

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "scope_digest": self.digest,
            "world_id_digest": _digest(self.world_id),
            "engram_id_digest": _digest(self.engram_id),
            "turn_id_digest": _digest(self.turn_id),
            "epoch": self.epoch,
        }


@dataclass(frozen=True, slots=True)
class MCPCallRequest:
    """Request value used by the future Runtime wiring seam."""

    server_id: str
    tool_name: str
    operation_id: str
    scope: MCPCallScope
    arguments: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        _bounded_identifier(self.server_id, field_name="server_id", server=True)
        _bounded_tool_name(self.tool_name)
        _bounded_identifier(self.operation_id, field_name="operation_id")
        if not isinstance(self.scope, MCPCallScope):
            raise ValueError("scope must be MCPCallScope")
        if not isinstance(self.arguments, Mapping):
            raise ValueError("arguments must be a mapping")
        bounded = _bound_json(self.arguments, field_name="tool_arguments")
        if not isinstance(bounded, dict):
            raise ValueError("arguments must be a JSON object")
        encoded = json.dumps(bounded, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > _MAX_ARGUMENT_BYTES:
            raise ValueError("arguments are too large")
        object.__setattr__(self, "arguments", MappingProxyType(bounded))

    @property
    def arguments_digest(self) -> str:
        return _digest(self.arguments)

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "server_id": self.server_id,
            "tool_name": self.tool_name,
            "operation_id": self.operation_id,
            "scope": self.scope.to_safe_dict(),
            "arguments_digest": self.arguments_digest,
        }


@dataclass(frozen=True, slots=True)
class MCPApprovalGrant:
    """Approval assertion consumed by this service, not created by it."""

    server_id: str
    tool_name: str
    operation_id: str
    scope_digest: str
    arguments_digest: str
    capability_digest: str
    decision: str = "allow_once"
    evidence_class: str = CONTRACT

    def __post_init__(self) -> None:
        _bounded_identifier(self.server_id, field_name="server_id", server=True)
        _bounded_tool_name(self.tool_name)
        _bounded_identifier(self.operation_id, field_name="operation_id")
        for name in ("scope_digest", "arguments_digest", "capability_digest"):
            _validate_digest(getattr(self, name), field_name=name)
        if self.decision != "allow_once":
            raise ValueError("only allow_once approval is supported by this seam")
        if self.evidence_class not in _EVIDENCE_VALUES:
            raise ValueError("unsupported approval evidence class")

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "server_id": self.server_id,
            "tool_name": self.tool_name,
            "operation_id": self.operation_id,
            "scope_digest": self.scope_digest,
            "arguments_digest": self.arguments_digest,
            "capability_digest": self.capability_digest,
            "decision": self.decision,
            "evidence_class": self.evidence_class,
        }


@dataclass(frozen=True, slots=True)
class MCPCallResult:
    """Successful call result with payload omitted and digested."""

    server_id: str
    tool_name: str
    operation_id: str
    scope_digest: str
    arguments_digest: str
    capability_digest: str
    result_digest: str
    status: str
    safe_summary: Mapping[str, Any]
    evidence_class: str = LIVE_GATE_UNVERIFIED
    execution_safety: str = EXECUTION_SAFETY_UNVERIFIED

    def __post_init__(self) -> None:
        _bounded_identifier(self.server_id, field_name="server_id", server=True)
        _bounded_tool_name(self.tool_name)
        _bounded_identifier(self.operation_id, field_name="operation_id")
        for name in (
            "scope_digest",
            "arguments_digest",
            "capability_digest",
            "result_digest",
        ):
            _validate_digest(getattr(self, name), field_name=name)
        if self.status != "COMPLETED":
            raise ValueError("MCPCallResult only represents a completed call")
        if not isinstance(self.safe_summary, Mapping):
            raise ValueError("safe_summary must be a mapping")
        if self.evidence_class not in _EVIDENCE_VALUES:
            raise ValueError("unsupported MCP evidence class")
        if self.execution_safety != EXECUTION_SAFETY_UNVERIFIED:
            raise ValueError("MCP runtime cannot claim execution safety")

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "server_id": self.server_id,
            "tool_name": self.tool_name,
            "operation_id": self.operation_id,
            "scope_digest": self.scope_digest,
            "arguments_digest": self.arguments_digest,
            "capability_digest": self.capability_digest,
            "result_digest": self.result_digest,
            "status": self.status,
            "safe_summary": _canonical_json(self.safe_summary),
            "evidence_class": self.evidence_class,
            "execution_safety": self.execution_safety,
        }


@dataclass(frozen=True, slots=True)
class MCPPhysicalOwnerKey:
    """Exact physical identity for one spawned MCP owner generation."""

    server_id: str
    descriptor_digest: str
    connect_generation: int
    owner_token: str

    def __post_init__(self) -> None:
        if type(self.server_id) is not str:
            raise ValueError("server_id must be an exact string")
        if type(self.descriptor_digest) is not str:
            raise ValueError("descriptor_digest must be an exact string")
        _bounded_identifier(self.server_id, field_name="server_id", server=True)
        _validate_digest(self.descriptor_digest, field_name="descriptor_digest")
        if (
            type(self.connect_generation) is not int
            or self.connect_generation <= 0
        ):
            raise ValueError("connect_generation must be a positive int")
        if (
            type(self.owner_token) is not str
            or _OWNER_TOKEN_RE.fullmatch(self.owner_token) is None
        ):
            raise ValueError("owner_token must be a 32-character lowercase hex id")


@dataclass(frozen=True, slots=True)
class _ProvisionalTransportOwnerKey:
    """Pre-spawn identity; it must be promoted if a process owner publishes."""

    server_id: str
    descriptor_digest: str
    connect_generation: int
    transport_token: str


_RetainedTransportKey = MCPPhysicalOwnerKey | _ProvisionalTransportOwnerKey


@dataclass(slots=True)
class _Session:
    descriptor: MCPServerDescriptor
    transport: MCPStdioTransport | None = None
    capability: MCPServerCapability | None = None
    state: MCPServerState = MCPServerState.DECLARED
    connect_generation: int = 0
    closed_explicitly: bool = False
    connected_identity: MCPServerIdentity | None = None
    connect_owner: threading.Thread | None = None
    connect_completed: threading.Event | None = None
    active_owner_key: _RetainedTransportKey | None = None


@dataclass(slots=True)
class _CallRecord:
    fingerprint: str
    cancel_event: threading.Event
    completed: threading.Event = field(default_factory=threading.Event)
    result: MCPCallResult | None = None
    # The requesting model needs the MCP result, while durable control-plane
    # projections must not retain it.  Keep the bounded payload only in this
    # in-process idempotency record; restart recovery never replays it.
    raw_result: Mapping[str, Any] | None = field(default=None, repr=False)
    error_code: str | None = None
    started: bool = False


@dataclass(frozen=True, slots=True)
class _TransportCloseObservation:
    """Runtime-owned, validated evidence for one transport observation."""

    active_before: int
    unresolved: int
    transport_owners_unresolved: int
    reader_owners_unresolved: int
    process_roots_observed: int
    process_root_owners_unresolved: int
    owner_joined: bool
    process_tree_state: Literal[
        "not_applicable",
        "empty_verified",
        "root_exit_only",
        "unknown",
    ]

    @property
    def retirable(self) -> bool:
        # Spawned owners require both final tree proof and zero unresolved
        # owners.  When the shared witness reports EMPTY_VERIFIED before its
        # resource is released, the transport keeps one physical owner
        # unresolved, so this remains false until a later exact observation.
        return (
            self.owner_joined
            and self.unresolved == 0
            and (
                (
                    self.process_roots_observed == 0
                    and self.process_tree_state == "not_applicable"
                )
                or (
                    self.process_roots_observed == 1
                    and self.process_tree_state == "empty_verified"
                )
            )
        )


@dataclass(slots=True)
class _ClosingTransportRecord:
    """First-winner owner record retained independently from logical sessions."""

    server_id: str
    transport: MCPStdioTransport
    owner_key: _RetainedTransportKey
    first_reason: str
    observation_lock: threading.Lock = field(
        default_factory=threading.Lock,
        repr=False,
    )
    last_observation: _TransportCloseObservation | None = None


def _unknown_transport_close_observation() -> _TransportCloseObservation:
    return _TransportCloseObservation(
        active_before=1,
        unresolved=1,
        transport_owners_unresolved=1,
        reader_owners_unresolved=0,
        process_roots_observed=0,
        process_root_owners_unresolved=0,
        owner_joined=False,
        process_tree_state="unknown",
    )


def _validated_transport_close_observation(
    raw: MCPTransportCloseSummary,
) -> _TransportCloseObservation:
    """Convert only the exact transport TypedDict shape into owned evidence.

    A generic Mapping is deliberately rejected.  Retained-census callers own
    the transport object and invoke ``close`` themselves; they cannot inject a
    mapping-shaped summary as a substitute for that physical observation.
    """

    if type(raw) is not dict or frozenset(raw) != _TRANSPORT_CLOSE_SUMMARY_KEYS:
        raise TypeError("transport close must return the exact typed summary")
    integer_fields = (
        "active_before",
        "unresolved",
        "transport_owners_unresolved",
        "reader_owners_unresolved",
        "process_roots_observed",
        "process_root_owners_unresolved",
    )
    values: dict[str, int] = {}
    for name in integer_fields:
        value = raw[name]
        if type(value) is not int or value < 0:
            raise TypeError("transport close summary counters must be non-negative ints")
        values[name] = value
    if values["process_roots_observed"] not in {0, 1}:
        raise ValueError("one stdio transport can observe at most one process root")
    if values["process_root_owners_unresolved"] not in {0, 1}:
        raise ValueError("one stdio transport can own at most one process root")
    if (
        values["process_root_owners_unresolved"]
        > values["process_roots_observed"]
    ):
        raise ValueError("an unresolved process root must have been observed")
    unresolved = (
        values["transport_owners_unresolved"]
        + values["reader_owners_unresolved"]
        + values["process_root_owners_unresolved"]
    )
    if values["unresolved"] != unresolved:
        raise ValueError("transport close summary unresolved count is inconsistent")
    owner_joined = raw["owner_joined"]
    if type(owner_joined) is not bool or owner_joined is not (unresolved == 0):
        raise TypeError("transport close summary owner_joined is inconsistent")
    process_tree = raw["process_tree_state"]
    if process_tree not in {
        "not_applicable",
        "empty_verified",
        "root_exit_only",
        "unknown",
    }:
        raise ValueError("transport close summary process tree is invalid")
    if values["process_root_owners_unresolved"]:
        if process_tree not in {"unknown", "empty_verified"}:
            raise ValueError(
                "an unresolved process owner requires unknown or empty tree evidence"
            )
    elif values["process_roots_observed"]:
        if process_tree not in {
            "unknown",
            "root_exit_only",
            "empty_verified",
        }:
            raise ValueError("spawned process evidence cannot be not_applicable")
    elif process_tree in {"root_exit_only", "empty_verified"}:
        raise ValueError("process-tree exit evidence requires an observed root")
    return _TransportCloseObservation(
        active_before=values["active_before"],
        unresolved=unresolved,
        transport_owners_unresolved=values["transport_owners_unresolved"],
        reader_owners_unresolved=values["reader_owners_unresolved"],
        process_roots_observed=values["process_roots_observed"],
        process_root_owners_unresolved=values[
            "process_root_owners_unresolved"
        ],
        owner_joined=owner_joined,
        process_tree_state=process_tree,
    )


def _merge_transport_close_observations(
    previous: _TransportCloseObservation | None,
    current: _TransportCloseObservation,
) -> _TransportCloseObservation:
    """Allow owner counts to settle without erasing first-seen root evidence."""

    if previous is None:
        return current
    process_roots_observed = max(
        previous.process_roots_observed,
        current.process_roots_observed,
    )
    if previous.process_tree_state == "empty_verified":
        process_tree: Literal[
            "not_applicable",
            "empty_verified",
            "root_exit_only",
            "unknown",
        ] = "empty_verified"
    elif current.process_tree_state == "empty_verified":
        process_tree = "empty_verified"
    elif process_roots_observed and (
        previous.process_tree_state == "root_exit_only"
        or current.process_tree_state == "root_exit_only"
    ):
        process_tree = "root_exit_only"
    elif process_roots_observed:
        process_tree = "unknown"
    elif current.process_tree_state == "not_applicable":
        process_tree = "not_applicable"
    else:
        process_tree = "unknown"
    return _TransportCloseObservation(
        active_before=max(previous.active_before, current.active_before),
        unresolved=current.unresolved,
        transport_owners_unresolved=current.transport_owners_unresolved,
        reader_owners_unresolved=current.reader_owners_unresolved,
        process_roots_observed=process_roots_observed,
        process_root_owners_unresolved=current.process_root_owners_unresolved,
        owner_joined=current.owner_joined,
        process_tree_state=process_tree,
    )


class MCPRuntimeService:
    """Bounded MCP registry and approval-bound call service.

    The constructor is intentionally explicit.  It accepts descriptors and
    allowlisted ids only; it never reads environment configuration, config
    files, project metadata, or a global server registry.
    """

    def __init__(
        self,
        descriptors: Iterable[MCPServerDescriptor],
        *,
        allowlisted_server_ids: Iterable[str],
        config: MCPRuntimeConfig | None = None,
        publication_permit: RuntimePublicationPermit | None = None,
    ) -> None:
        if (
            publication_permit is not None
            and type(publication_permit) is not RuntimePublicationPermit
        ):
            raise TypeError(
                "publication_permit must be a RuntimePublicationPermit or None"
            )
        self._config = config or MCPRuntimeConfig()
        self._publication_permit = publication_permit
        if isinstance(descriptors, (str, bytes, bytearray)):
            raise ValueError("descriptors must be an iterable of descriptors")
        descriptor_list = list(descriptors)
        if len(descriptor_list) > self._config.max_servers:
            raise ValueError("descriptor registry exceeds configured bound")
        sessions: dict[str, _Session] = {}
        for descriptor in descriptor_list:
            if not isinstance(descriptor, MCPServerDescriptor):
                raise ValueError("descriptors must contain MCPServerDescriptor values")
            if descriptor.server_id in sessions:
                raise ValueError("duplicate MCP server id")
            sessions[descriptor.server_id] = _Session(descriptor=descriptor)

        if isinstance(allowlisted_server_ids, (str, bytes, bytearray)):
            raise ValueError("allowlisted_server_ids must be an iterable")
        allowlist: set[str] = set()
        for raw_server_id in allowlisted_server_ids:
            server_id = _bounded_identifier(
                raw_server_id,
                field_name="allowlisted server id",
                server=True,
            )
            if server_id not in sessions:
                raise ValueError("allowlisted server id has no explicit descriptor")
            allowlist.add(server_id)

        self._sessions = sessions
        self._allowlisted_server_ids = frozenset(allowlist)
        self._lock = threading.RLock()
        self._connect_waiters: dict[str, threading.Event] = {}
        self._calls: OrderedDict[tuple[str, str], _CallRecord] = OrderedDict()
        self._closing_transports: OrderedDict[
            _RetainedTransportKey,
            _ClosingTransportRecord,
        ] = OrderedDict()
        self._closed = False

    @property
    def config(self) -> MCPRuntimeConfig:
        return self._config

    @property
    def allowlisted_server_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._allowlisted_server_ids))

    def list_server_summaries(self) -> tuple[dict[str, Any], ...]:
        """Return bounded summaries without argv, cwd, env values or payloads."""

        with self._lock:
            return tuple(
                self._server_summary_locked(self._sessions[server_id])
                for server_id in sorted(self._allowlisted_server_ids)
            )

    def list_servers(self) -> tuple[dict[str, Any], ...]:
        """Alias for future API adapters."""

        return self.list_server_summaries()

    def approval_preview(self, server_id: str, tool_name: str) -> dict[str, Any]:
        """Return a process-free approval binding for one declared tool.

        A model-originated preview must never start an MCP server before the
        human decision.  Descriptor identity is static and therefore safe to
        bind before approval.  A capability digest is included only when an
        operator or an earlier approved call already negotiated that server;
        this method never connects implicitly.
        """

        server_id = _bounded_identifier(server_id, field_name="server_id", server=True)
        tool_name = _bounded_tool_name(tool_name)
        session = self._allowed_session(server_id)
        with self._lock:
            self._ensure_open_locked()
            identity = session.descriptor.identity
            if (
                session.state is MCPServerState.READY
                and session.connected_identity is not None
                and session.connected_identity.descriptor_digest
                != identity.descriptor_digest
            ):
                raise MCPRuntimeError("server_artifact_changed", status=409)
            capability = (
                session.capability
                if session.state is MCPServerState.READY
                else None
            )
            if capability is not None and tool_name not in capability.tool_names:
                raise MCPRuntimeError("tool_not_advertised", status=404)
            return {
                "server_id": server_id,
                "tool_name": tool_name,
                "descriptor_digest": identity.descriptor_digest,
                "capability_digest": (
                    None if capability is None else capability.capability_digest
                ),
                "capability_state": (
                    "declared" if capability is None else "negotiated"
                ),
                "process_started": session.transport is not None,
            }

    def safe_summary(self) -> dict[str, Any]:
        """Return the service projection safe for control-plane display."""

        with self._lock:
            return {
                "service": "mcp_runtime",
                "state": "CLOSED" if self._closed else "OPEN",
                "allowlisted_server_ids": list(self.allowlisted_server_ids),
                "servers": list(self.list_server_summaries()),
                "registry": {
                    "max_servers": self._config.max_servers,
                    "max_active_calls": self._config.max_active_calls,
                    "max_retained_calls": self._config.max_retained_calls,
                    "retained_calls": len(self._calls),
                    "retained_closing_transports": len(self._closing_transports),
                },
                "approval_required": True,
                "auto_config": False,
                "execution_safety": EXECUTION_SAFETY_UNVERIFIED,
                "evidence_class": (
                    LIVE_GATE_UNVERIFIED if any(
                        session.capability is not None for session in self._sessions.values()
                    ) else CONTRACT
                ),
                "live_label_reserved": LIVE,
            }

    def connect(
        self,
        server_id: str,
        *,
        cancel_event: Any = None,
    ) -> MCPServerHandle:
        """Connect one explicitly allowlisted server and negotiate capabilities."""

        server_id = _bounded_identifier(server_id, field_name="server_id", server=True)
        session = self._allowed_session(server_id)
        connect_deadline = time.monotonic() + self._config.max_connect_wait_sec
        while True:
            reconcile_required = False
            with self._lock:
                self._ensure_open_locked()
                if _is_cancelled(cancel_event):
                    raise MCPRuntimeError("connect_cancelled", status=409)
                if session.closed_explicitly:
                    raise MCPRuntimeError("server_closed", status=409)
                if (
                    session.state is MCPServerState.READY
                    and session.transport is not None
                    and session.transport.phase != "READY"
                ):
                    owner_key = (
                        session.active_owner_key
                        or self._registry_key_for_transport(
                            server_id=server_id,
                            descriptor_digest=(
                                session.connected_identity.descriptor_digest
                                if session.connected_identity is not None
                                else session.descriptor.identity.descriptor_digest
                            ),
                            connect_generation=session.connect_generation,
                            transport=session.transport,
                        )
                    )
                    self.detach_to_retained_locked(
                        session,
                        session.transport,
                        owner_key,
                        "connect_observed_terminal_transport",
                        terminal_state=MCPServerState.BROKEN,
                    )
                reconcile_required = any(
                    record.server_id == server_id
                    for record in self._closing_transports.values()
                )
                if reconcile_required:
                    waiter = None
                elif (
                    session.state is MCPServerState.READY
                    and session.capability is not None
                ):
                    current_identity = session.descriptor.identity
                    if (
                        session.connected_identity is None
                        or session.connected_identity.descriptor_digest
                        != current_identity.descriptor_digest
                    ):
                        raise MCPRuntimeError("server_artifact_changed", status=409)
                    return self._handle_locked(session)
                elif session.state is MCPServerState.CONNECTING:
                    waiter = self._connect_waiters[server_id]
                else:
                    session.connect_generation += 1
                    generation = session.connect_generation
                    session.state = MCPServerState.CONNECTING
                    session.capability = None
                    session.connected_identity = None
                    approved_identity = session.descriptor.identity
                    waiter = threading.Event()
                    self._connect_waiters[server_id] = waiter
                    session.connect_owner = threading.current_thread()
                    session.connect_completed = threading.Event()
                    session.active_owner_key = None
                    break
            if reconcile_required:
                reconcile_deadline = min(
                    connect_deadline,
                    time.monotonic() + _DEFAULT_CLOSE_TIMEOUT_SECONDS,
                )
                self.reconcile_server_close(
                    server_id,
                    deadline=reconcile_deadline,
                )
                with self._lock:
                    if any(
                        record.server_id == server_id
                        for record in self._closing_transports.values()
                    ):
                        raise MCPRuntimeError(
                            "server_transport_close_unresolved",
                            status=409,
                        )
                continue
            assert waiter is not None
            while True:
                if _is_cancelled(cancel_event):
                    raise MCPRuntimeError("connect_cancelled", status=409)
                remaining = connect_deadline - time.monotonic()
                if remaining <= 0:
                    raise MCPRuntimeError("connect_wait_timeout", status=504)
                if waiter.wait(timeout=min(0.05, remaining)):
                    break

        transport: MCPStdioTransport | None = None
        try:
            if _is_cancelled(cancel_event):
                raise MCPRuntimeError("connect_cancelled", status=409)
            candidate = session.descriptor.build_transport(
                publication_permit=self._publication_permit,
            )
            if not isinstance(candidate, MCPStdioTransport):
                raise TypeError(
                    "descriptor build_transport must return MCPStdioTransport"
                )
            transport = candidate
            transport.bind_runtime_owner_callbacks(
                owner_published=(
                    lambda observed_transport, _owner: (
                        self._transport_owner_published(
                            session=session,
                            transport=observed_transport,
                            server_id=server_id,
                            descriptor_digest=approved_identity.descriptor_digest,
                            connect_generation=generation,
                        )
                    )
                ),
                terminal=(
                    lambda terminal_transport, reason, deadline: (
                        self._transport_terminal(
                            session=session,
                            transport=terminal_transport,
                            server_id=server_id,
                            descriptor_digest=approved_identity.descriptor_digest,
                            connect_generation=generation,
                            reason=reason,
                            deadline=deadline,
                        )
                    )
                ),
            )
            with self._lock:
                if (
                    self._closed
                    or _is_cancelled(cancel_event)
                    or session.closed_explicitly
                    or session.connect_generation != generation
                    or session.state is not MCPServerState.CONNECTING
                ):
                    raise MCPRuntimeError("connect_cancelled", status=409)
                session.transport = transport
                session.active_owner_key = self._registry_key_for_transport(
                    server_id=server_id,
                    descriptor_digest=approved_identity.descriptor_digest,
                    connect_generation=generation,
                    transport=transport,
                )
            transport.start(
                cancel_event=cancel_event,
                deadline=connect_deadline,
            )
            if _is_cancelled(cancel_event):
                raise MCPRuntimeError("connect_cancelled", status=409)
            snapshot = transport.list_tools(deadline=connect_deadline)
            capability = self._build_capability(server_id, snapshot)
            connected_identity = session.descriptor.identity
            with self._lock:
                if (
                    self._closed
                    or session.closed_explicitly
                    or session.connect_generation != generation
                    or session.state is not MCPServerState.CONNECTING
                    or session.transport is not transport
                    or transport.phase != "READY"
                ):
                    raise MCPRuntimeError("connect_cancelled", status=409)
                if connected_identity.descriptor_digest != approved_identity.descriptor_digest:
                    raise MCPRuntimeError("server_artifact_changed", status=409)
                session.transport = transport
                session.capability = capability
                session.connected_identity = connected_identity
                session.state = MCPServerState.READY
                session.active_owner_key = self._registry_key_for_transport(
                    server_id=server_id,
                    descriptor_digest=connected_identity.descriptor_digest,
                    connect_generation=generation,
                    transport=transport,
                )
                if not isinstance(session.active_owner_key, MCPPhysicalOwnerKey):
                    raise MCPRuntimeError("physical_owner_unavailable", status=502)
                return self._handle_locked(session)
        except MCPRuntimeError:
            retained = None
            if transport is not None:
                retained = self._detach_terminal_transport(
                    session=session,
                    transport=transport,
                    server_id=server_id,
                    descriptor_digest=approved_identity.descriptor_digest,
                    connect_generation=generation,
                    reason="connect_runtime_error",
                    deadline=connect_deadline,
                    terminal_state=MCPServerState.FAILED,
                )
            with self._lock:
                if (
                    retained is None
                    and session.connect_generation == generation
                    and not session.closed_explicitly
                ):
                    session.connected_identity = None
                    session.state = MCPServerState.CLOSED if self._closed else MCPServerState.FAILED
            raise
        except MCPError as exc:
            retained = None
            if transport is not None:
                retained = self._detach_terminal_transport(
                    session=session,
                    transport=transport,
                    server_id=server_id,
                    descriptor_digest=approved_identity.descriptor_digest,
                    connect_generation=generation,
                    reason="connect_transport_error",
                    deadline=connect_deadline,
                    terminal_state=MCPServerState.FAILED,
                )
            with self._lock:
                if (
                    retained is None
                    and session.connect_generation == generation
                    and not session.closed_explicitly
                ):
                    session.connected_identity = None
                    session.state = MCPServerState.CLOSED if self._closed else MCPServerState.FAILED
            code = "connect_cancelled" if isinstance(exc, MCPCancelledError) else exc.code
            raise MCPRuntimeError(code, status=502) from None
        except (OSError, ValueError, TypeError, RuntimeError) as exc:
            retained = None
            if transport is not None:
                retained = self._detach_terminal_transport(
                    session=session,
                    transport=transport,
                    server_id=server_id,
                    descriptor_digest=approved_identity.descriptor_digest,
                    connect_generation=generation,
                    reason="connect_adapter_error",
                    deadline=connect_deadline,
                    terminal_state=MCPServerState.FAILED,
                )
            with self._lock:
                if (
                    retained is None
                    and session.connect_generation == generation
                    and not session.closed_explicitly
                ):
                    session.connected_identity = None
                    session.state = MCPServerState.CLOSED if self._closed else MCPServerState.FAILED
            raise MCPRuntimeError("connect_failed", status=502) from exc
        finally:
            with self._lock:
                registered = self._connect_waiters.get(server_id)
                if registered is waiter:
                    self._connect_waiters.pop(server_id, None)
                    waiter.set()
                completed = session.connect_completed
                if completed is not None:
                    completed.set()
                if session.connect_completed is completed:
                    session.connect_owner = None
                    session.connect_completed = None

    def close_server(self, server_id: str) -> None:
        """Cancel calls for one server and close its transport."""

        server_id = _bounded_identifier(server_id, field_name="server_id", server=True)
        session = self._allowed_session(server_id)
        absolute = time.monotonic() + _DEFAULT_CLOSE_TIMEOUT_SECONDS
        with self._lock:
            active = [
                record
                for (record_server_id, _operation_id), record in self._calls.items()
                if record_server_id == server_id and not record.completed.is_set()
            ]
            for record in active:
                record.cancel_event.set()
            transport = session.transport
            if transport is not None:
                owner_key = (
                    session.active_owner_key
                    or self._registry_key_for_transport(
                        server_id=server_id,
                        descriptor_digest=(
                            session.connected_identity.descriptor_digest
                            if session.connected_identity is not None
                            else session.descriptor.identity.descriptor_digest
                        ),
                        connect_generation=max(1, session.connect_generation),
                        transport=transport,
                    )
                )
                self.detach_to_retained_locked(
                    session,
                    transport,
                    owner_key,
                    "close_server",
                    terminal_state=MCPServerState.CLOSED,
                )
            session.connect_generation += 1
            session.closed_explicitly = True
            session.state = MCPServerState.CLOSED
            session.capability = None
            session.connected_identity = None
            waiter = self._connect_waiters.get(server_id)
            if waiter is not None:
                waiter.set()
            closing = tuple(
                record
                for record in self._closing_transports.values()
                if record.server_id == server_id
            )
        self._broadcast_closing_transports(closing)
        for record in closing:
            self._observe_closing_transport(record, deadline=absolute)

    def cancel(self, *, server_id: str, operation_id: str) -> bool:
        """Request cancellation of one in-flight stable operation."""

        server_id = _bounded_identifier(server_id, field_name="server_id", server=True)
        operation_id = _bounded_identifier(operation_id, field_name="operation_id")
        self._allowed_session(server_id)
        with self._lock:
            record = self._calls.get((server_id, operation_id))
            if record is None or record.completed.is_set():
                return False
            record.cancel_event.set()
            return True

    def call_tool(
        self,
        *,
        server_id: str,
        tool_name: str,
        arguments: Mapping[str, Any] | None = None,
        operation_id: str,
        scope: MCPCallScope,
        approval: MCPApprovalGrant | None = None,
        timeout: float | None = None,
        cancel_event: Any = None,
    ) -> MCPCallResult:
        """Execute one allowlisted, approval-bound MCP tool call.

        ``operation_id`` and ``scope`` are required inputs.  The service never
        manufactures either value, because their parent Runtime owns the
        identity and exactly-once semantics of the larger turn.
        """

        request = MCPCallRequest(
            server_id=server_id,
            tool_name=tool_name,
            operation_id=operation_id,
            scope=scope,
            arguments={} if arguments is None else arguments,
        )
        session = self._allowed_session(request.server_id)
        with self._lock:
            self._ensure_open_locked()
            if session.state is not MCPServerState.READY or session.capability is None:
                raise MCPRuntimeError("server_not_connected", status=409)
            capability = session.capability
            if request.tool_name not in capability.tool_names:
                raise MCPRuntimeError("tool_not_advertised", status=404)
            if approval is None:
                raise MCPApprovalRequiredError()
            if not isinstance(approval, MCPApprovalGrant):
                raise MCPApprovalMismatchError()
            if not self._approval_matches(request, approval, capability):
                raise MCPApprovalMismatchError()
            if _is_cancelled(cancel_event):
                raise MCPCallExecutionError(
                    "call_cancelled",
                    operation_id=request.operation_id,
                    server_id=request.server_id,
                )
            effective_timeout = self._effective_timeout(session.descriptor, timeout)
            call_deadline = time.monotonic() + effective_timeout
            fingerprint = _digest(
                {
                    "server_id": request.server_id,
                    "operation_id": request.operation_id,
                    "tool_name": request.tool_name,
                    "scope_digest": request.scope.digest,
                    "arguments_digest": request.arguments_digest,
                    "capability_digest": capability.capability_digest,
                }
            )
            key = (request.server_id, request.operation_id)
            record = self._calls.get(key)
            if record is not None:
                if record.fingerprint != fingerprint:
                    raise MCPOperationCollisionError()
                if record.completed.is_set():
                    return self._return_record(record, request)
                owner = False
            else:
                self._evict_completed_locked()
                active_count = sum(
                    1 for item in self._calls.values() if not item.completed.is_set()
                )
                if active_count >= self._config.max_active_calls:
                    raise MCPRuntimeError("call_capacity_exceeded", status=429)
                record = _CallRecord(
                    fingerprint=fingerprint,
                    cancel_event=threading.Event(),
                )
                self._calls[key] = record
                owner = True
            transport = session.transport
            transport_generation = session.connect_generation
            transport_descriptor_digest = (
                session.connected_identity.descriptor_digest
                if session.connected_identity is not None
                else session.descriptor.identity.descriptor_digest
            )

        if not owner:
            return self._wait_for_record(record, request, cancel_event)
        if transport is None:
            self._finish_error(record, request, "server_not_connected")
            raise MCPCallExecutionError(
                "server_not_connected",
                operation_id=request.operation_id,
                server_id=request.server_id,
            )
        with self._lock:
            record.started = True
        relay = _CancellationRelay(cancel_event, record.cancel_event)
        try:
            raw_result = transport.call_tool(
                request.tool_name,
                request.arguments,
                deadline=call_deadline,
                cancel_event=relay,
            )
            result, bounded_result = self._build_call_result(
                request, capability, raw_result
            )
        except MCPError as exc:
            error_code = "call_cancelled" if isinstance(exc, MCPCancelledError) else exc.code
            if record.cancel_event.is_set() and error_code == "transport_closed":
                error_code = "call_cancelled"
            if not isinstance(exc, MCPRemoteError):
                self._detach_terminal_transport(
                    session=session,
                    transport=transport,
                    server_id=request.server_id,
                    descriptor_digest=transport_descriptor_digest,
                    connect_generation=transport_generation,
                    reason=f"call_{error_code}"[:96],
                    deadline=call_deadline,
                    terminal_state=MCPServerState.BROKEN,
                )
            self._finish_error(record, request, error_code)
            raise MCPCallExecutionError(
                error_code,
                operation_id=request.operation_id,
                server_id=request.server_id,
            ) from None
        except Exception:
            self._detach_terminal_transport(
                session=session,
                transport=transport,
                server_id=request.server_id,
                descriptor_digest=transport_descriptor_digest,
                connect_generation=transport_generation,
                reason="call_adapter_failed",
                deadline=call_deadline,
                terminal_state=MCPServerState.BROKEN,
            )
            self._finish_error(record, request, "call_adapter_failed")
            raise MCPCallExecutionError(
                "call_adapter_failed",
                operation_id=request.operation_id,
                server_id=request.server_id,
            ) from None
        else:
            with self._lock:
                record.result = result
                record.raw_result = MappingProxyType(bounded_result)
                record.completed.set()
            if transport.phase != "READY":
                self._detach_terminal_transport(
                    session=session,
                    transport=transport,
                    server_id=request.server_id,
                    descriptor_digest=transport_descriptor_digest,
                    connect_generation=transport_generation,
                    reason="call_terminal_after_result",
                    deadline=call_deadline,
                    terminal_state=MCPServerState.BROKEN,
                )
            return result

    def delivery_payload(
        self,
        *,
        server_id: str,
        operation_id: str,
    ) -> dict[str, Any]:
        """Return one completed payload to its requesting runtime only.

        This method is intentionally separate from ``to_safe_dict`` and
        ``safe_summary``.  It is an ephemeral model tool-result channel, not
        evidence for Workbench, logs, the operation ledger, or recovery.
        """

        server_id = _bounded_identifier(server_id, field_name="server_id", server=True)
        operation_id = _bounded_identifier(operation_id, field_name="operation_id")
        self._allowed_session(server_id)
        with self._lock:
            record = self._calls.get((server_id, operation_id))
            if (
                record is None
                or not record.completed.is_set()
                or record.result is None
                or record.raw_result is None
            ):
                raise MCPRuntimeError("delivery_payload_unavailable", status=409)
            # Round-trip gives the caller a detached JSON value and prevents
            # mutation of the idempotency record.
            value = json.loads(
                json.dumps(
                    dict(record.raw_result),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
        if not isinstance(value, dict):
            raise MCPRuntimeError("delivery_payload_invalid", status=502)
        return value

    def call(
        self,
        request: MCPCallRequest,
        *,
        approval: MCPApprovalGrant | None,
        timeout: float | None = None,
        cancel_event: Any = None,
    ) -> MCPCallResult:
        """Narrow request-object alias for future Runtime wiring."""

        if not isinstance(request, MCPCallRequest):
            raise ValueError("request must be MCPCallRequest")
        return self.call_tool(
            server_id=request.server_id,
            tool_name=request.tool_name,
            arguments=request.arguments,
            operation_id=request.operation_id,
            scope=request.scope,
            approval=approval,
            timeout=timeout,
            cancel_event=cancel_event,
        )

    def close(self, *, deadline: float | None = None) -> MCPRuntimeCloseSummary:
        """Broadcast all cancellation, then observe one shared close deadline."""

        absolute = _absolute_deadline(deadline)
        with self._lock:
            self._closed = True
            connect_events: list[threading.Event] = []
            active_calls_before = 0
            for session in self._sessions.values():
                transport = session.transport
                if transport is not None:
                    owner_key = (
                        session.active_owner_key
                        or self._registry_key_for_transport(
                            server_id=session.descriptor.server_id,
                            descriptor_digest=(
                                session.connected_identity.descriptor_digest
                                if session.connected_identity is not None
                                else session.descriptor.identity.descriptor_digest
                            ),
                            connect_generation=max(1, session.connect_generation),
                            transport=transport,
                        )
                    )
                    self.detach_to_retained_locked(
                        session,
                        transport,
                        owner_key,
                        "runtime_close",
                        terminal_state=MCPServerState.CLOSED,
                    )
                session.connect_generation += 1
                session.closed_explicitly = True
                session.state = MCPServerState.CLOSED
                session.capability = None
                session.connected_identity = None
                if (
                    session.connect_completed is not None
                    and not session.connect_completed.is_set()
                ):
                    connect_events.append(session.connect_completed)
            for record in self._calls.values():
                if not record.completed.is_set():
                    active_calls_before += 1
                    record.cancel_event.set()
            for waiter in self._connect_waiters.values():
                waiter.set()
            closing = tuple(self._closing_transports.values())

        # This is the hard-shutdown broadcast boundary: every call and every
        # transport sees cancellation before any one owner receives wait time.
        self._broadcast_closing_transports(closing)

        transport_observations: list[
            tuple[_ClosingTransportRecord, _TransportCloseObservation]
        ] = []
        for record in closing:
            transport_observations.append(
                (
                    record,
                    self._observe_closing_transport(record, deadline=absolute),
                )
            )

        for event in connect_events:
            remaining = absolute - time.monotonic()
            if remaining <= 0:
                break
            event.wait(timeout=remaining)

        with self._lock:
            call_events = tuple(
                record.completed
                for record in self._calls.values()
                if not record.completed.is_set()
            )
        for event in call_events:
            remaining = absolute - time.monotonic()
            if remaining <= 0:
                break
            event.wait(timeout=remaining)

        # A terminal call owner can finish after the transport observer took
        # its first reader/process snapshot.  Re-observe the same retained
        # records after owner completion so the returned close evidence is
        # current rather than a stale, pessimistic sample.  With an exhausted
        # deadline this is still a non-waiting census.
        if connect_events or call_events:
            transport_observations = [
                (
                    record,
                    self._observe_closing_transport(record, deadline=absolute),
                )
                for record, _observation in transport_observations
            ]

        # A connect owner can lose the close race after the first transport
        # snapshot, detach its local transport, and then complete.  Re-census
        # the retained registry after connect-owner observation so that a
        # completed connect owner cannot make its unresolved transport vanish.
        with self._lock:
            late_closing = tuple(
                record
                for record in self._closing_transports.values()
                if not any(
                    observed_record is record
                    for observed_record, _observation in transport_observations
                )
            )
        self._broadcast_closing_transports(late_closing)
        for record in late_closing:
            transport_observations.append(
                (
                    record,
                    self._observe_closing_transport(record, deadline=absolute),
                )
            )

        with self._lock:
            active_calls_unresolved = sum(
                not record.completed.is_set()
                for record in self._calls.values()
            )
            connect_owners_unresolved = sum(
                session.connect_completed is not None
                and not session.connect_completed.is_set()
                for session in self._sessions.values()
            )
            retained = tuple(self._closing_transports.values())

        # If a connect owner claimed a transport after the second snapshot,
        # its own close observation is already stored before its completion
        # event is cleared.  Include that typed observation, or fail closed if
        # the record is currently being observed.
        for record in retained:
            if not any(
                observed_record is record
                for observed_record, _observation in transport_observations
            ):
                transport_observations.append(
                    (
                        record,
                        self._snapshot_closing_transport(record),
                    )
                )

        transport_summaries = tuple(
            observation for _record, observation in transport_observations
        )

        transport_owners_unresolved = sum(
            item.transport_owners_unresolved for item in transport_summaries
        )
        reader_owners_unresolved = sum(
            item.reader_owners_unresolved for item in transport_summaries
        )
        process_roots_observed = sum(
            item.process_roots_observed for item in transport_summaries
        )
        process_root_owners_unresolved = sum(
            item.process_root_owners_unresolved for item in transport_summaries
        )
        trees = {item.process_tree_state for item in transport_summaries}
        if "unknown" in trees:
            process_tree: Literal[
                "not_applicable",
                "empty_verified",
                "root_exit_only",
                "unknown",
            ] = "unknown"
        elif "root_exit_only" in trees:
            process_tree = "root_exit_only"
        elif "empty_verified" in trees:
            process_tree = "empty_verified"
        else:
            process_tree = "not_applicable"

        unresolved = (
            active_calls_unresolved
            + connect_owners_unresolved
            + transport_owners_unresolved
            + reader_owners_unresolved
            + process_root_owners_unresolved
        )
        return {
            "active_before": (
                active_calls_before
                + len(connect_events)
                + sum(item.active_before for item in transport_summaries)
            ),
            "unresolved": unresolved,
            "active_calls_before": active_calls_before,
            "active_calls_unresolved": active_calls_unresolved,
            "connect_owners_unresolved": connect_owners_unresolved,
            "transports_observed": len(transport_summaries),
            "transport_owners_unresolved": transport_owners_unresolved,
            "reader_owners_unresolved": reader_owners_unresolved,
            "process_roots_observed": process_roots_observed,
            "process_root_owners_unresolved": process_root_owners_unresolved,
            "owner_joined": unresolved == 0,
            "process_tree_state": process_tree,
        }

    def __enter__(self) -> "MCPRuntimeService":
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> None:
        self.close()

    @staticmethod
    def _registry_key_for_transport(
        *,
        server_id: str,
        descriptor_digest: str,
        connect_generation: int,
        transport: MCPStdioTransport,
    ) -> _RetainedTransportKey:
        if not isinstance(transport, MCPStdioTransport):
            raise TypeError("closing transport must be MCPStdioTransport")
        owner = transport.physical_owner
        if owner is not None:
            return MCPPhysicalOwnerKey(
                server_id=server_id,
                descriptor_digest=descriptor_digest,
                connect_generation=connect_generation,
                owner_token=owner.owner_token,
            )
        return _ProvisionalTransportOwnerKey(
            server_id=server_id,
            descriptor_digest=descriptor_digest,
            connect_generation=connect_generation,
            transport_token=transport.provisional_owner_token,
        )

    def _record_for_transport_locked(
        self,
        transport: MCPStdioTransport,
    ) -> _ClosingTransportRecord | None:
        return next(
            (
                record
                for record in self._closing_transports.values()
                if record.transport is transport
            ),
            None,
        )

    def _promote_record_key_locked(
        self,
        record: _ClosingTransportRecord,
        owner_key: MCPPhysicalOwnerKey,
    ) -> None:
        current = self._closing_transports.get(owner_key)
        if current is not None and current is not record:
            raise RuntimeError("closing transport owner key collision")
        old_key = record.owner_key
        if old_key == owner_key:
            return
        if not isinstance(old_key, _ProvisionalTransportOwnerKey):
            raise RuntimeError("physical owner key cannot be replaced")
        if self._closing_transports.get(old_key) is record:
            self._closing_transports.pop(old_key)
        record.owner_key = owner_key
        self._closing_transports[owner_key] = record

    def detach_to_retained_locked(
        self,
        session: _Session,
        transport: MCPStdioTransport,
        owner_key: _RetainedTransportKey,
        reason: str,
        *,
        terminal_state: MCPServerState,
    ) -> _ClosingTransportRecord:
        """The only seam allowed to sever an attached transport reference.

        Callers hold ``self._lock``.  The record is published first; logical
        state and reachability change only after the exact first-winner claim.
        """

        if not isinstance(transport, MCPStdioTransport):
            raise TypeError("closing transport must be MCPStdioTransport")
        if type(reason) is not str or not reason or len(reason) > 96:
            raise ValueError("closing reason must be a bounded token")
        if type(terminal_state) is not MCPServerState:
            raise TypeError("terminal_state must be MCPServerState")

        existing = self._record_for_transport_locked(transport)
        if session.transport is not transport:
            if existing is None:
                raise RuntimeError("detached transport has no retained owner record")
            return existing

        exact_key = self._registry_key_for_transport(
            server_id=owner_key.server_id,
            descriptor_digest=owner_key.descriptor_digest,
            connect_generation=owner_key.connect_generation,
            transport=transport,
        )
        if isinstance(owner_key, MCPPhysicalOwnerKey) and exact_key != owner_key:
            raise RuntimeError("stale physical owner key")
        owner_key = exact_key

        if existing is not None:
            if existing.server_id != owner_key.server_id:
                raise RuntimeError("closing transport server identity collision")
            if isinstance(owner_key, MCPPhysicalOwnerKey):
                self._promote_record_key_locked(existing, owner_key)
            record = existing
        else:
            for current in self._closing_transports.values():
                if current.server_id == owner_key.server_id:
                    raise RuntimeError("server already has a retained physical owner")
            collision = self._closing_transports.get(owner_key)
            if collision is not None:
                raise RuntimeError("closing transport owner key collision")
            record = _ClosingTransportRecord(
                server_id=owner_key.server_id,
                transport=transport,
                owner_key=owner_key,
                first_reason=reason,
            )
            self._closing_transports[owner_key] = record

        # This assignment is intentionally unique in the service: every
        # logical detach has an exact retained record before reachability ends.
        session.transport = None
        session.active_owner_key = None
        session.capability = None
        session.connected_identity = None
        session.state = (
            MCPServerState.CLOSED
            if self._closed or session.closed_explicitly
            else terminal_state
        )
        return record

    def _transport_owner_published(
        self,
        *,
        session: _Session,
        transport: MCPStdioTransport,
        server_id: str,
        descriptor_digest: str,
        connect_generation: int,
    ) -> None:
        owner_key = self._registry_key_for_transport(
            server_id=server_id,
            descriptor_digest=descriptor_digest,
            connect_generation=connect_generation,
            transport=transport,
        )
        if not isinstance(owner_key, MCPPhysicalOwnerKey):
            raise RuntimeError("published process lacks a physical owner key")
        with self._lock:
            if (
                session.transport is transport
                and session.connect_generation == connect_generation
            ):
                session.active_owner_key = owner_key
            record = self._record_for_transport_locked(transport)
            if record is not None:
                self._promote_record_key_locked(record, owner_key)

    def _detach_terminal_transport(
        self,
        *,
        session: _Session,
        transport: MCPStdioTransport,
        server_id: str,
        descriptor_digest: str,
        connect_generation: int,
        reason: str,
        deadline: float | None,
        terminal_state: MCPServerState,
    ) -> _TransportCloseObservation | None:
        absolute = _absolute_deadline(deadline)
        with self._lock:
            if session.transport is transport:
                owner_key = (
                    session.active_owner_key
                    or self._registry_key_for_transport(
                        server_id=server_id,
                        descriptor_digest=descriptor_digest,
                        connect_generation=connect_generation,
                        transport=transport,
                    )
                )
                record = self.detach_to_retained_locked(
                    session,
                    transport,
                    owner_key,
                    reason,
                    terminal_state=terminal_state,
                )
            else:
                record = self._record_for_transport_locked(transport)
                if record is None:
                    # A stale callback from a retired generation is fenced and
                    # cannot claim, close, or detach a successor transport.
                    return None
        self._broadcast_closing_transports((record,))
        return self._observe_closing_transport(
            record,
            deadline=absolute,
            wait_for_observer=False,
        )

    def _transport_terminal(
        self,
        *,
        session: _Session,
        transport: MCPStdioTransport,
        server_id: str,
        descriptor_digest: str,
        connect_generation: int,
        reason: str,
        deadline: float | None,
    ) -> None:
        with self._lock:
            terminal_state = (
                MCPServerState.FAILED
                if session.state is MCPServerState.CONNECTING
                else MCPServerState.BROKEN
            )
        self._detach_terminal_transport(
            session=session,
            transport=transport,
            server_id=server_id,
            descriptor_digest=descriptor_digest,
            connect_generation=connect_generation,
            reason=reason,
            deadline=deadline,
            terminal_state=terminal_state,
        )

    def reconcile_server_close(
        self,
        server_id: str,
        *,
        deadline: float,
    ) -> MCPTransportCloseSummary | None:
        """Boundedly reobserve one retained server owner without spawning."""

        server_id = _bounded_identifier(server_id, field_name="server_id", server=True)
        self._allowed_session(server_id)
        absolute = _absolute_deadline(deadline)
        with self._lock:
            records = tuple(
                record
                for record in self._closing_transports.values()
                if record.server_id == server_id
            )
        if not records:
            return None
        self._broadcast_closing_transports(records)
        observation = _unknown_transport_close_observation()
        for record in records:
            observation = self._observe_closing_transport(
                record,
                deadline=absolute,
            )
        return self._transport_observation_summary(observation)

    @staticmethod
    def _broadcast_closing_transports(
        records: Iterable[_ClosingTransportRecord],
    ) -> None:
        for record in records:
            try:
                record.transport.signal_close()
            except Exception:
                # Observation below converts any close failure to explicit
                # UNKNOWN owner evidence; broadcast failure never permits drop.
                pass

    def _observe_closing_transport(
        self,
        record: _ClosingTransportRecord,
        *,
        deadline: float,
        wait_for_observer: bool = True,
    ) -> _TransportCloseObservation:
        if wait_for_observer:
            remaining = max(0.0, deadline - time.monotonic())
            acquired = record.observation_lock.acquire(timeout=remaining)
        else:
            # A transport/call owner must never wait on a close observer that
            # may itself be joining that owner.  Claim/reuse is already done;
            # Runtime close or reconcile owns the bounded observation.
            acquired = record.observation_lock.acquire(blocking=False)
        if not acquired:
            return _unknown_transport_close_observation()
        try:
            previous = record.last_observation
            if previous is not None and previous.retirable:
                observation = previous
            else:
                try:
                    raw = record.transport.close(deadline=deadline)
                    current = _validated_transport_close_observation(raw)
                except Exception:
                    current = _unknown_transport_close_observation()
                observation = _merge_transport_close_observations(
                    previous,
                    current,
                )
                record.last_observation = observation
        finally:
            record.observation_lock.release()

        if observation.retirable:
            with self._lock:
                owner_key = record.owner_key
                if isinstance(owner_key, _ProvisionalTransportOwnerKey):
                    exact_key = self._registry_key_for_transport(
                        server_id=owner_key.server_id,
                        descriptor_digest=owner_key.descriptor_digest,
                        connect_generation=owner_key.connect_generation,
                        transport=record.transport,
                    )
                    if isinstance(exact_key, MCPPhysicalOwnerKey):
                        self._promote_record_key_locked(record, exact_key)
                        owner_key = exact_key
                if isinstance(owner_key, MCPPhysicalOwnerKey) and (
                    record.transport.physical_owner is None
                    or record.transport.physical_owner.owner_token
                    != owner_key.owner_token
                ):
                    return observation
                current_record = self._closing_transports.get(record.owner_key)
                if current_record is record:
                    self._closing_transports.pop(record.owner_key, None)
        return observation

    @staticmethod
    def _snapshot_closing_transport(
        record: _ClosingTransportRecord,
    ) -> _TransportCloseObservation:
        if not record.observation_lock.acquire(blocking=False):
            return _unknown_transport_close_observation()
        try:
            return (
                record.last_observation
                if record.last_observation is not None
                else _unknown_transport_close_observation()
            )
        finally:
            record.observation_lock.release()

    @staticmethod
    def _transport_observation_summary(
        observation: _TransportCloseObservation,
    ) -> MCPTransportCloseSummary:
        return {
            "active_before": observation.active_before,
            "unresolved": observation.unresolved,
            "transport_owners_unresolved": (
                observation.transport_owners_unresolved
            ),
            "reader_owners_unresolved": observation.reader_owners_unresolved,
            "process_roots_observed": observation.process_roots_observed,
            "process_root_owners_unresolved": (
                observation.process_root_owners_unresolved
            ),
            "owner_joined": observation.owner_joined,
            "process_tree_state": observation.process_tree_state,
        }

    def _allowed_session(self, server_id: str) -> _Session:
        with self._lock:
            if server_id not in self._allowlisted_server_ids:
                raise MCPServerNotAllowedError(server_id)
            session = self._sessions.get(server_id)
            if session is None:
                raise MCPServerNotAllowedError(server_id)
            return session

    def _ensure_open_locked(self) -> None:
        if self._closed:
            raise MCPRuntimeError("runtime_closed", status=503)

    def _handle_locked(self, session: _Session) -> MCPServerHandle:
        if session.capability is None or session.connected_identity is None:
            raise MCPRuntimeError("server_capability_unavailable", status=503)
        return MCPServerHandle(
            identity=session.connected_identity,
            capability=session.capability,
            state=session.state,
        )

    def _server_summary_locked(self, session: _Session) -> dict[str, Any]:
        capability = session.capability
        identity = session.connected_identity or session.descriptor.identity
        summary: dict[str, Any] = {
            "server_id": session.descriptor.server_id,
            "state": session.state.value,
            "identity": identity.to_safe_dict(),
            "approval_required": True,
            "execution_safety": EXECUTION_SAFETY_UNVERIFIED,
            "evidence_class": CONTRACT,
        }
        if capability is not None:
            summary.update(
                {
                    "server_name": capability.server_name,
                    "server_version": capability.server_version,
                    "protocol_version": capability.protocol_version,
                    "tool_names": list(capability.tool_names),
                    "tool_schema_digests": {
                        name: digest for name, digest in capability.tool_schema_digests
                    },
                    "server_capability_digest": capability.server_capability_digest,
                    "capability_digest": capability.capability_digest,
                    "evidence_class": capability.evidence_class,
                }
            )
        return summary

    def _build_capability(
        self,
        server_id: str,
        snapshot: MCPCapabilitySnapshot,
    ) -> MCPServerCapability:
        tool_pairs = tuple(sorted((tool.name, tool.input_schema_digest) for tool in snapshot.tools))
        tool_names = tuple(name for name, _digest_value in tool_pairs)
        server_capability_digest = _digest(snapshot.server_capabilities)
        capability_digest = _digest(
            {
                "server_id": server_id,
                "protocol_version": snapshot.protocol_version,
                "server_name": snapshot.server_name,
                "server_version": snapshot.server_version,
                "server_capability_digest": server_capability_digest,
                "tools": tool_pairs,
            }
        )
        return MCPServerCapability(
            server_id=server_id,
            protocol_version=snapshot.protocol_version,
            server_name=snapshot.server_name,
            server_version=snapshot.server_version,
            tool_names=tool_names,
            tool_schema_digests=tool_pairs,
            server_capability_digest=server_capability_digest,
            capability_digest=capability_digest,
        )

    def _effective_timeout(
        self,
        descriptor: MCPServerDescriptor,
        timeout: float | None,
    ) -> float:
        if timeout is None:
            candidate = descriptor.request_timeout
        else:
            candidate = _bounded_timeout(
                timeout,
                field_name="timeout",
                maximum=min(descriptor.max_timeout, self._config.max_call_wait_sec),
            )
        return min(candidate, descriptor.max_timeout, self._config.max_call_wait_sec)

    @staticmethod
    def _approval_matches(
        request: MCPCallRequest,
        approval: MCPApprovalGrant,
        capability: MCPServerCapability,
    ) -> bool:
        return (
            approval.server_id == request.server_id
            and approval.tool_name == request.tool_name
            and approval.operation_id == request.operation_id
            and approval.scope_digest == request.scope.digest
            and approval.arguments_digest == request.arguments_digest
            and approval.capability_digest == capability.capability_digest
            and approval.decision == "allow_once"
        )

    def _build_call_result(
        self,
        request: MCPCallRequest,
        capability: MCPServerCapability,
        raw_result: Mapping[str, Any],
    ) -> tuple[MCPCallResult, dict[str, Any]]:
        bounded_result = _bound_json(raw_result, field_name="tool_result")
        if not isinstance(bounded_result, dict):
            raise MCPRuntimeError("tool_result_invalid", status=502)
        safe_summary = _safe_result_summary(bounded_result)
        result = MCPCallResult(
            server_id=request.server_id,
            tool_name=request.tool_name,
            operation_id=request.operation_id,
            scope_digest=request.scope.digest,
            arguments_digest=request.arguments_digest,
            capability_digest=capability.capability_digest,
            result_digest=_digest(bounded_result),
            status="COMPLETED",
            safe_summary=safe_summary,
        )
        return result, bounded_result

    def _wait_for_record(
        self,
        record: _CallRecord,
        request: MCPCallRequest,
        cancel_event: Any,
    ) -> MCPCallResult:
        deadline = time.monotonic() + self._config.max_call_wait_sec
        while not record.completed.wait(timeout=min(0.05, max(0.0, deadline - time.monotonic()))):
            if _is_cancelled(cancel_event):
                raise MCPCallExecutionError(
                    "call_wait_cancelled",
                    operation_id=request.operation_id,
                    server_id=request.server_id,
                )
            if time.monotonic() >= deadline:
                raise MCPCallExecutionError(
                    "call_wait_timeout",
                    operation_id=request.operation_id,
                    server_id=request.server_id,
                )
        return self._return_record(record, request)

    @staticmethod
    def _return_record(record: _CallRecord, request: MCPCallRequest) -> MCPCallResult:
        if record.result is not None:
            return record.result
        raise MCPCallExecutionError(
            record.error_code or "call_failed",
            operation_id=request.operation_id,
            server_id=request.server_id,
        )

    def _finish_error(
        self,
        record: _CallRecord,
        request: MCPCallRequest,
        error_code: str,
    ) -> None:
        with self._lock:
            record.error_code = error_code
            record.completed.set()

    def _evict_completed_locked(self) -> None:
        while len(self._calls) >= self._config.max_retained_calls:
            completed_key = next(
                (
                    key
                    for key, record in self._calls.items()
                    if record.completed.is_set()
                ),
                None,
            )
            if completed_key is None:
                raise MCPRuntimeError("call_registry_capacity_exceeded", status=429)
            self._calls.pop(completed_key)


class MCPActionBackend:
    """Bind ``pulse_mcp_call`` to the ordinary Harness approval/action seam.

    The surrounding ``HarnessActionBroker`` owns human approval, epoch
    fencing, the E0 operation boundary and terminal events.  This adapter
    owns only explicit MCP descriptor/capability validation and one call.
    MCP payload is returned ephemerally to the requesting Pi tool call; only
    digests and counts are placed in adapter summaries.
    """

    evidence_class = LIVE_GATE_UNVERIFIED
    evidence_binding: Mapping[str, str] = MappingProxyType({})

    def __init__(
        self,
        service: MCPRuntimeService,
        *,
        world_id: str,
        registry_gate: MCPRegistryGate | None = None,
    ) -> None:
        if not isinstance(service, MCPRuntimeService):
            raise TypeError("service must be MCPRuntimeService")
        if registry_gate is not None and not isinstance(registry_gate, MCPRegistryGate):
            raise TypeError("registry_gate must be MCPRegistryGate")
        self._service = service
        self._world_id = _bounded_identifier(world_id, field_name="world_id")
        self._registry_gate = registry_gate

    @property
    def service(self) -> MCPRuntimeService:
        return self._service

    def preview_for(
        self,
        tool_name: str,
        input_data: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        if tool_name != "pulse_mcp_call":
            raise ValueError("unsupported MCP proxy tool")
        server_id, remote_tool, arguments, timeout = self._request(input_data)
        static_preview = self._service.approval_preview(server_id, remote_tool)
        registry_preview: Mapping[str, Any] = {}
        if self._registry_gate is not None:
            registry_preview = self._registry_gate.authorize(
                server_id=server_id,
                descriptor_digest=str(static_preview["descriptor_digest"]),
                world_id=self._world_id,
                engram_id="preview-engram",
                turn_id="preview-turn",
                approval_granted=False,
            )
        request = MCPCallRequest(
            server_id=server_id,
            tool_name=remote_tool,
            operation_id="preview-operation",
            scope=MCPCallScope(
                world_id="preview-world",
                engram_id="preview-engram",
                turn_id="preview-turn",
                epoch=1,
            ),
            arguments=arguments,
        )
        return {
            "operation": "mcp_call",
            "server_id": server_id,
            "tool_name": remote_tool,
            "arguments_digest": request.arguments_digest,
            "descriptor_digest": static_preview["descriptor_digest"],
            "capability_digest": static_preview["capability_digest"],
            "capability_state": static_preview["capability_state"],
            "timeout_seconds": timeout,
            "execution_safety": EXECUTION_SAFETY_UNVERIFIED,
            **dict(registry_preview),
        }

    def execute(
        self,
        *,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        tool_name: str,
        input_data: Mapping[str, Any],
        policy_preview: Mapping[str, Any],
        signal: Any = None,
        **_: Any,
    ) -> Mapping[str, Any]:
        if _is_cancelled(signal):
            return self._cancelled_before_execution()
        try:
            server_id, remote_tool, arguments, timeout = self._request(input_data)
            static_preview = self._service.approval_preview(server_id, remote_tool)
            if policy_preview.get("descriptor_digest") != static_preview.get(
                "descriptor_digest"
            ):
                raise MCPRuntimeError("descriptor_changed_after_approval", status=409)
            if self._registry_gate is not None:
                registry_decision = self._registry_gate.authorize(
                    server_id=server_id,
                    descriptor_digest=str(static_preview["descriptor_digest"]),
                    world_id=self._world_id,
                    engram_id=engram_id,
                    turn_id=turn_id,
                    approval_granted=True,
                )
                if policy_preview.get("registry_provenance_digest") != (
                    registry_decision.get("registry_provenance_digest")
                ):
                    raise MCPRuntimeError("registry_changed_after_approval", status=409)
            if _is_cancelled(signal):
                return self._cancelled_before_execution()
            handle = self._service.connect(server_id, cancel_event=signal)
            if remote_tool not in handle.capability.tool_names:
                raise MCPRuntimeError("tool_not_advertised", status=404)
            approved_capability = policy_preview.get("capability_digest")
            if (
                isinstance(approved_capability, str)
                and approved_capability != handle.capability.capability_digest
            ):
                raise MCPRuntimeError("capability_changed_after_approval", status=409)
            scope = MCPCallScope(
                world_id=self._world_id,
                engram_id=engram_id,
                turn_id=turn_id,
                epoch=epoch,
            )
            request = MCPCallRequest(
                server_id=server_id,
                tool_name=remote_tool,
                operation_id=action_request_id,
                scope=scope,
                arguments=arguments,
            )
            grant = MCPApprovalGrant(
                server_id=server_id,
                tool_name=remote_tool,
                operation_id=action_request_id,
                scope_digest=scope.digest,
                arguments_digest=request.arguments_digest,
                capability_digest=handle.capability.capability_digest,
            )
            result = self._service.call(
                request,
                approval=grant,
                timeout=timeout,
                cancel_event=signal,
            )
            payload = self._service.delivery_payload(
                server_id=server_id,
                operation_id=action_request_id,
            )
            return {
                "ok": True,
                "status": "completed",
                "execution_status": "completed",
                "evidence_class": LIVE_GATE_UNVERIFIED,
                "mcp_server_id": server_id,
                "mcp_tool_name": remote_tool,
                "mcp_server_identity_digest": handle.identity.descriptor_digest,
                "mcp_capability_digest": handle.capability.capability_digest,
                "mcp_arguments_digest": request.arguments_digest,
                "mcp_result_digest": result.result_digest,
                "mcp_content_items": result.safe_summary.get("content_items", 0),
                "mcp_is_error": result.safe_summary.get("is_error"),
                "ephemeral_content": self._delivery_text(payload),
                "recovery_state": "none",
            }
        except MCPCallExecutionError as exc:
            return {
                "ok": False,
                "status": "uncertain",
                "error": exc.code,
                "recovery_state": "uncertain",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        except MCPRuntimeError as exc:
            if exc.code == "connect_cancelled":
                return self._cancelled_before_execution()
            return {
                "ok": False,
                "status": "failed",
                "error": exc.code,
                "recovery_state": "none",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        except (TypeError, ValueError):
            return {
                "ok": False,
                "status": "failed",
                "error": "mcp_request_invalid",
                "recovery_state": "none",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }

    @staticmethod
    def _cancelled_before_execution() -> Mapping[str, Any]:
        return {
            "ok": False,
            "status": "cancelled",
            "execution_status": "cancelled",
            "error": "call_cancelled",
            "recovery_state": "none",
            "evidence_class": LIVE_GATE_UNVERIFIED,
        }

    def cancel(self, action_request_id: str, input_data: Mapping[str, Any]) -> bool:
        try:
            server_id, _tool, _arguments, _timeout = self._request(input_data)
            return self._service.cancel(
                server_id=server_id,
                operation_id=action_request_id,
            )
        except Exception:
            return False

    @staticmethod
    def _request(
        input_data: Mapping[str, Any],
    ) -> tuple[str, str, Mapping[str, Any], float | None]:
        if not isinstance(input_data, Mapping):
            raise ValueError("MCP input must be an object")
        allowed = {"server_id", "tool_name", "arguments", "timeout"}
        if set(input_data).difference(allowed):
            raise ValueError("MCP input contains unknown fields")
        server_id = _bounded_identifier(
            input_data.get("server_id"), field_name="server_id", server=True
        )
        remote_tool = _bounded_tool_name(input_data.get("tool_name"))
        arguments = input_data.get("arguments", {})
        if not isinstance(arguments, Mapping):
            raise ValueError("MCP arguments must be an object")
        bounded_arguments = _bound_json(arguments, field_name="tool_arguments")
        if not isinstance(bounded_arguments, dict):
            raise ValueError("MCP arguments must be an object")
        raw_timeout = input_data.get("timeout")
        timeout = (
            None
            if raw_timeout is None
            else _bounded_timeout(
                raw_timeout,
                field_name="timeout",
                maximum=_MAX_TIMEOUT_SECONDS,
            )
        )
        return server_id, remote_tool, bounded_arguments, timeout

    @staticmethod
    def _delivery_text(payload: Mapping[str, Any]) -> str:
        content = payload.get("content")
        texts: list[str] = []
        if isinstance(content, list):
            for item in content[:256]:
                if isinstance(item, Mapping):
                    value = item.get("text")
                    if isinstance(value, str):
                        texts.append(value)
        if texts:
            value = "\n".join(texts)
        else:
            value = json.dumps(
                dict(payload),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        if len(value) > 1_000_000:
            return value[:1_000_000] + "\n[TRUNCATED]"
        return value


def _safe_result_summary(result: Mapping[str, Any]) -> dict[str, Any]:
    """Project an MCP result without retaining text, arguments, or secrets."""

    content = result.get("content")
    content_items = content if isinstance(content, list) else []
    kinds: list[str] = []
    text_chars = 0
    for item in content_items[:256]:
        if isinstance(item, Mapping):
            raw_type = item.get("type")
            kind = raw_type if isinstance(raw_type, str) and raw_type else "object"
            kinds.append(kind[:64])
            raw_text = item.get("text")
            if isinstance(raw_text, str):
                text_chars += len(raw_text)
        else:
            kinds.append(type(item).__name__[:64])
    raw_is_error = result.get("isError")
    is_error = raw_is_error if isinstance(raw_is_error, bool) else None
    return {
        "payload": "omitted",
        "result_keys": sorted(str(key) for key in result.keys())[:128],
        "content_items": len(content_items),
        "content_kinds": kinds,
        "text_chars": text_chars,
        "is_error": is_error,
        "structured_content_present": "structuredContent" in result,
        "result_digest": _digest(result),
    }

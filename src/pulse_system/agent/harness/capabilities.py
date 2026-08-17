"""Capability contracts for the Harness tool ecosystem.

This module is deliberately a policy value object, not an execution adapter.
It describes the *intersection* between a descriptor and an upper-layer
policy.  It never contains credentials, tool arguments, or a callback that
could execute a process.  A registry or a real adapter may use these values
before it asks another layer to perform an action.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from pathlib import PurePosixPath
from typing import Any

__all__ = [
    "CapabilityContext",
    "CapabilityError",
    "CapabilitySet",
    "EvidenceClass",
]


_MAX_ITEMS = 256
_MAX_VALUE_LENGTH = 256
_MAX_SCOPE_LENGTH = 160
_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
_DOMAIN_RE = re.compile(
    r"^(?:\*\.)?(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
)
_SECRET_ACCESS = frozenset({"metadata", "read", "write"})
_CAPABILITY_KEYS = frozenset(
    {
        "tool_names",
        "tools",
        "filesystem_roots",
        "filesystem",
        "network_domains",
        "network",
        "process_classes",
        "process",
        "secrets_access",
        "secrets",
        "world_ids",
        "worlds",
        "engram_ids",
        "engrams",
        "turn_ids",
        "turns",
        "mcp_transports",
        "transports",
    }
)


class CapabilityError(ValueError):
    """A capability value is malformed or attempts to widen a scope."""


class EvidenceClass(str, Enum):
    """Evidence labels carried by registry and execution-plane contracts."""

    CONTRACT_ONLY = "CONTRACT_ONLY"
    LIVE_GATE_UNVERIFIED = "LIVE_GATE_UNVERIFIED"
    LIVE_VERIFIED = "LIVE_VERIFIED"


def _bounded_values(value: Any, *, field_name: str) -> tuple[str, ...]:
    """Return bounded, unique strings without coercing arbitrary payloads."""

    if value is None:
        return ()
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(value, Iterable):
        raise CapabilityError(f"{field_name} must be a list of strings")

    values: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise CapabilityError(f"{field_name} contains a non-string value")
        item = item.strip()
        if not item or len(item) > _MAX_VALUE_LENGTH:
            raise CapabilityError(f"{field_name} contains an invalid value")
        if item not in seen:
            seen.add(item)
            values.append(item)
        if len(values) > _MAX_ITEMS:
            raise CapabilityError(f"{field_name} exceeds {_MAX_ITEMS} values")
    return tuple(values)


def _safe_token_values(value: Any, *, field_name: str) -> frozenset[str]:
    values = _bounded_values(value, field_name=field_name)
    invalid = [item for item in values if _TOKEN_RE.fullmatch(item) is None]
    if invalid:
        raise CapabilityError(f"{field_name} contains an invalid token")
    return frozenset(values)


def _normalize_root(value: str) -> str:
    """Normalize a workspace-relative POSIX root without resolving a host path."""

    candidate = value.replace("\\", "/").strip()
    if not candidate or len(candidate) > _MAX_VALUE_LENGTH:
        raise CapabilityError("filesystem_roots contains an invalid root")
    if candidate.startswith("/") or re.match(r"^[A-Za-z]:", candidate):
        raise CapabilityError("filesystem_roots must be workspace-relative")
    parts = [part for part in candidate.split("/") if part not in ("", ".")]
    if any(part == ".." for part in parts):
        raise CapabilityError("filesystem_roots may not contain '..'")
    normalized = str(PurePosixPath(*parts)) if parts else "."
    if normalized == ".." or normalized.startswith("../"):
        raise CapabilityError("filesystem_roots must be workspace-relative")
    return normalized


def _safe_roots(value: Any) -> frozenset[str]:
    values = _bounded_values(value, field_name="filesystem_roots")
    return frozenset(_normalize_root(item) for item in values)


def _normalize_domain(value: str) -> str:
    candidate = value.strip().lower().rstrip(".")
    if not candidate or len(candidate) > _MAX_VALUE_LENGTH:
        raise CapabilityError("network_domains contains an invalid domain")
    if "://" in candidate or "/" in candidate or ":" in candidate:
        raise CapabilityError("network_domains must contain host names, not URLs")
    if _DOMAIN_RE.fullmatch(candidate) is None:
        raise CapabilityError("network_domains contains an invalid domain")
    return candidate


def _safe_domains(value: Any) -> frozenset[str]:
    values = _bounded_values(value, field_name="network_domains")
    return frozenset(_normalize_domain(item) for item in values)


def _safe_scope_values(value: Any, *, field_name: str) -> frozenset[str]:
    values = _bounded_values(value, field_name=field_name)
    for item in values:
        if len(item) > _MAX_SCOPE_LENGTH or any(
            ord(char) < 0x20 or ord(char) == 0x7F for char in item
        ):
            raise CapabilityError(f"{field_name} contains an invalid scope id")
    return frozenset(values)


def _intersection_roots(left: frozenset[str], right: frozenset[str]) -> frozenset[str]:
    """Return the narrower roots where the two root sets overlap."""

    result: set[str] = set()
    for left_root in left:
        for right_root in right:
            if left_root == ".":
                result.add(right_root)
            elif right_root == ".":
                result.add(left_root)
            elif left_root == right_root:
                result.add(left_root)
            elif left_root.startswith(right_root + "/"):
                result.add(left_root)
            elif right_root.startswith(left_root + "/"):
                result.add(right_root)
    return frozenset(result)


def _domain_in_scope(child: str, parent: str) -> bool:
    child = child.removeprefix("*.")
    parent = parent.removeprefix("*.")
    return child == parent or child.endswith("." + parent)


def _intersection_domains(
    left: frozenset[str], right: frozenset[str]
) -> frozenset[str]:
    """Return the narrower host names where two domain scopes overlap."""

    result: set[str] = set()
    for left_domain in left:
        for right_domain in right:
            if _domain_in_scope(left_domain, right_domain):
                result.add(left_domain)
            elif _domain_in_scope(right_domain, left_domain):
                result.add(right_domain)
    return frozenset(result)


def _contains_roots(container: frozenset[str], requested: frozenset[str]) -> bool:
    return all(
        any(
            allowed == "."
            or root == allowed
            or root.startswith(allowed + "/")
            for allowed in container
        )
        for root in requested
    )


def _contains_domains(container: frozenset[str], requested: frozenset[str]) -> bool:
    return all(
        any(_domain_in_scope(domain, allowed) for allowed in container)
        for domain in requested
    )


@dataclass(frozen=True, slots=True)
class CapabilitySet:
    """A bounded, JSON-safe set of permissions.

    Empty fields mean *no permission in that dimension*.  There is no
    implicit wildcard and no ``None means all`` behavior.  This is important
    for descriptor declarations: a missing field cannot accidentally widen a
    policy during intersection.
    """

    tool_names: frozenset[str] = field(default_factory=frozenset)
    filesystem_roots: frozenset[str] = field(default_factory=frozenset)
    network_domains: frozenset[str] = field(default_factory=frozenset)
    process_classes: frozenset[str] = field(default_factory=frozenset)
    secrets_access: frozenset[str] = field(default_factory=frozenset)
    world_ids: frozenset[str] = field(default_factory=frozenset)
    engram_ids: frozenset[str] = field(default_factory=frozenset)
    turn_ids: frozenset[str] = field(default_factory=frozenset)
    mcp_transports: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        # Direct construction is part of the public contract, so normalize
        # and validate it just as ``from_mapping`` does.  Frozen dataclasses
        # still allow this one-time canonicalization.
        object.__setattr__(self, "tool_names", _safe_token_values(self.tool_names, field_name="tool_names"))
        object.__setattr__(self, "filesystem_roots", _safe_roots(self.filesystem_roots))
        object.__setattr__(self, "network_domains", _safe_domains(self.network_domains))
        object.__setattr__(self, "process_classes", _safe_token_values(self.process_classes, field_name="process_classes"))
        secrets = _safe_token_values(self.secrets_access, field_name="secrets_access")
        if not secrets.issubset(_SECRET_ACCESS):
            raise CapabilityError("secrets_access contains an unsupported access level")
        object.__setattr__(self, "secrets_access", secrets)
        object.__setattr__(self, "world_ids", _safe_scope_values(self.world_ids, field_name="world_ids"))
        object.__setattr__(self, "engram_ids", _safe_scope_values(self.engram_ids, field_name="engram_ids"))
        object.__setattr__(self, "turn_ids", _safe_scope_values(self.turn_ids, field_name="turn_ids"))
        object.__setattr__(self, "mcp_transports", _safe_token_values(self.mcp_transports, field_name="mcp_transports"))

    @classmethod
    def empty(cls) -> "CapabilitySet":
        """Return a deny-all capability value."""

        return cls()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "CapabilitySet":
        """Parse only known capability fields; unknown payload is ignored.

        Ignoring unknown keys is intentional: a manifest may contain vendor
        metadata, but vendor metadata must not become an executable capability
        merely because it was copied into this contract.
        """

        if value is None:
            return cls.empty()
        if not isinstance(value, Mapping):
            raise CapabilityError("capabilities must be an object")

        def pick(*names: str) -> Any:
            for name in names:
                if name in value:
                    return value[name]
            return None

        return cls(
            tool_names=_safe_token_values(
                pick("tool_names", "tools"), field_name="tool_names"
            ),
            filesystem_roots=_safe_roots(
                pick("filesystem_roots", "filesystem")
            ),
            network_domains=_safe_domains(
                pick("network_domains", "network")
            ),
            process_classes=_safe_token_values(
                pick("process_classes", "process"), field_name="process_classes"
            ),
            secrets_access=_safe_token_values(
                pick("secrets_access", "secrets"), field_name="secrets_access"
            ),
            world_ids=_safe_scope_values(
                pick("world_ids", "worlds"), field_name="world_ids"
            ),
            engram_ids=_safe_scope_values(
                pick("engram_ids", "engrams"), field_name="engram_ids"
            ),
            turn_ids=_safe_scope_values(
                pick("turn_ids", "turns"), field_name="turn_ids"
            ),
            mcp_transports=_safe_token_values(
                pick("mcp_transports", "transports"), field_name="mcp_transports"
            ),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any] | None) -> "CapabilitySet":
        """Compatibility alias for callers that use dict-oriented naming."""

        return cls.from_mapping(value)

    def to_dict(self) -> dict[str, list[str]]:
        """Return a deterministic, credential-free representation."""

        return {
            "tool_names": sorted(self.tool_names),
            "filesystem_roots": sorted(self.filesystem_roots),
            "network_domains": sorted(self.network_domains),
            "process_classes": sorted(self.process_classes),
            "secrets_access": sorted(self.secrets_access),
            "world_ids": sorted(self.world_ids),
            "engram_ids": sorted(self.engram_ids),
            "turn_ids": sorted(self.turn_ids),
            "mcp_transports": sorted(self.mcp_transports),
        }

    as_dict = to_dict

    def intersect(self, other: "CapabilitySet") -> "CapabilitySet":
        """Return the capability intersection; no field can be widened."""

        if not isinstance(other, CapabilitySet):
            raise TypeError("capability intersection requires CapabilitySet")
        return CapabilitySet(
            tool_names=self.tool_names & other.tool_names,
            filesystem_roots=_intersection_roots(
                self.filesystem_roots, other.filesystem_roots
            ),
            network_domains=_intersection_domains(
                self.network_domains, other.network_domains
            ),
            process_classes=self.process_classes & other.process_classes,
            secrets_access=self.secrets_access & other.secrets_access,
            world_ids=self.world_ids & other.world_ids,
            engram_ids=self.engram_ids & other.engram_ids,
            turn_ids=self.turn_ids & other.turn_ids,
            mcp_transports=self.mcp_transports & other.mcp_transports,
        )

    intersection = intersect

    def is_empty(self) -> bool:
        return not any(
            (
                self.tool_names,
                self.filesystem_roots,
                self.network_domains,
                self.process_classes,
                self.secrets_access,
                self.world_ids,
                self.engram_ids,
                self.turn_ids,
                self.mcp_transports,
            )
        )

    def contains(self, requested: "CapabilitySet") -> bool:
        """Return whether this set can satisfy ``requested`` without widening."""

        if not isinstance(requested, CapabilitySet):
            raise TypeError("capability containment requires CapabilitySet")
        return (
            requested.tool_names.issubset(self.tool_names)
            and _contains_roots(self.filesystem_roots, requested.filesystem_roots)
            and _contains_domains(self.network_domains, requested.network_domains)
            and requested.process_classes.issubset(self.process_classes)
            and requested.secrets_access.issubset(self.secrets_access)
            and requested.world_ids.issubset(self.world_ids)
            and requested.engram_ids.issubset(self.engram_ids)
            and requested.turn_ids.issubset(self.turn_ids)
            and requested.mcp_transports.issubset(self.mcp_transports)
        )

    def allows_scope(
        self,
        *,
        world_id: str | None = None,
        engram_id: str | None = None,
        turn_id: str | None = None,
    ) -> bool:
        """Check exact current world/Engram/turn identity when constrained."""

        return (
            not self.is_empty()
            and
            (not self.world_ids or world_id in self.world_ids)
            and (not self.engram_ids or engram_id in self.engram_ids)
            and (not self.turn_ids or turn_id in self.turn_ids)
        )


@dataclass(frozen=True, slots=True)
class CapabilityContext:
    """Runtime identity plus an upper policy for a registry resolution."""

    world_id: str
    engram_id: str
    turn_id: str | None
    policy: CapabilitySet
    required: CapabilitySet = field(default_factory=CapabilitySet.empty)
    approval_granted: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.world_id, str) or not self.world_id.strip():
            raise CapabilityError("world_id must be non-empty")
        if not isinstance(self.engram_id, str) or not self.engram_id.strip():
            raise CapabilityError("engram_id must be non-empty")
        if self.turn_id is not None and (
            not isinstance(self.turn_id, str) or not self.turn_id.strip()
        ):
            raise CapabilityError("turn_id must be non-empty when provided")
        if not isinstance(self.policy, CapabilitySet):
            raise TypeError("policy must be CapabilitySet")
        if not isinstance(self.required, CapabilitySet):
            raise TypeError("required must be CapabilitySet")
        if type(self.approval_granted) is not bool:
            raise CapabilityError("approval_granted must be a boolean")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CapabilityContext":
        if not isinstance(value, Mapping):
            raise CapabilityError("capability context must be an object")
        world_id = value.get("world_id")
        engram_id = value.get("engram_id")
        if not isinstance(world_id, str) or not isinstance(engram_id, str):
            raise CapabilityError("capability context needs world_id and engram_id")
        policy_value = value.get("policy", value.get("allowed", value.get("capabilities")))
        required_value = value.get("required")
        return cls(
            world_id=world_id,
            engram_id=engram_id,
            turn_id=value.get("turn_id"),
            policy=(
                policy_value
                if isinstance(policy_value, CapabilitySet)
                else CapabilitySet.from_mapping(policy_value)
            ),
            required=(
                required_value
                if isinstance(required_value, CapabilitySet)
                else CapabilitySet.from_mapping(required_value)
            ),
            approval_granted=(
                value.get("approval_granted", False)
                if type(value.get("approval_granted", False)) is bool
                else False
            ),
        )

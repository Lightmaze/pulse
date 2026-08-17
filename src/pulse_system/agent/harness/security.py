"""Pure security contracts for the Harness execution plane.

This module deliberately stops before an operating-system or Pi adapter.  It
answers one question only: *would a request be permitted by the configured
policy, and what safe preview may be shown to a caller?*  An executor must
still perform the action and must keep the returned ``CONTRACT_ONLY`` evidence
class honest until a real adapter has been verified.

The defaults are intentionally restrictive:

* filesystem reads are confined to the configured workspace;
* writes, commands, network access and third-party capabilities are denied
  until explicitly configured;
* full access is represented as a policy shape, but requires an explicit
  opt-in and never claims to be a live sandbox;
* previews are relative, bounded and redacted before they leave this module.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import shlex
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable
from urllib.parse import SplitResult, urlsplit, urlunsplit

__all__ = [
    "ApprovalMode",
    "CommandPolicy",
    "CommandScope",
    "CONTRACT_ONLY",
    "ExecutionPolicy",
    "FilesystemAccess",
    "FilesystemMode",
    "FilesystemPolicy",
    "LIVE_GATE_UNVERIFIED",
    "LIVE_OS_RESTRICTED",
    "LIVE_WORKSPACE_CHECKPOINTED",
    "NetworkAccess",
    "NetworkMode",
    "NetworkPolicy",
    "PolicyContext",
    "PolicyDecision",
    "PolicyError",
    "PolicyRequest",
    "PolicyDeniedError",
    "RedactionReport",
    "Redactor",
    "resolve_policy_decision",
]


CONTRACT_ONLY = "CONTRACT_ONLY"
LIVE_GATE_UNVERIFIED = "LIVE_GATE_UNVERIFIED"
LIVE_OS_RESTRICTED = "LIVE_OS_RESTRICTED"
LIVE_WORKSPACE_CHECKPOINTED = "LIVE_WORKSPACE_CHECKPOINTED"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$")
_REASON_CODE_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,79}$")
_CAPABILITY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/*@-]{0,127}$")
_SHELL_META_RE = re.compile(r"[;&|<>`$()]|\x00")

_SECRET_KEY_RE = re.compile(
    r"(?:^|[_\-.])(?:api[_\-.]?key|access[_\-.]?key|auth(?:orization)?|"
    r"bearer|client[_\-.]?secret|cookie|credential|password|private[_\-.]?key|"
    r"refresh[_\-.]?token|secret|session[_\-.]?token|token)(?:$|[_\-.])",
    re.IGNORECASE,
)
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?P<label>\b(?:api[_-]?key|access[_-]?key|authorization|bearer|"
    r"cookie|credential|password|private[_-]?key|refresh[_-]?token|secret|"
    r"session[_-]?token|token)\b\s*[:=]\s*)(?P<value>[^\s,;\]}]+)",
    re.IGNORECASE,
)
_BEARER_RE = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{8,}", re.IGNORECASE)
_COMMON_KEY_RE = re.compile(r"\b(?:sk|rk|gh[pousr]|xox[baprs])-[-A-Za-z0-9_]{12,}\b")
_AWS_ACCESS_KEY_RE = re.compile(r"\bAKIA[0-9A-Z]{16}\b")
_WINDOWS_PATH_RE = re.compile(
    r"(?<![A-Za-z0-9_])(?:[A-Za-z]:[\\/]|\\\\)[^\r\n\"'<>|]+"
)
_POSIX_PATH_RE = re.compile(r"(?<![A-Za-z0-9_:/])/(?:[^\s\"'<>|]+/)*[^\s\"'<>|]+")
_URL_SECRET_QUERY_RE = re.compile(
    r"([?&](?:api[_-]?key|access[_-]?token|auth|authorization|cookie|"
    r"password|secret|token|key)=[^&#\s]*)",
    re.IGNORECASE,
)

_READ_OPERATIONS = frozenset(
    {
        "file_read",
        "file_list",
        "read",
        "read_file",
        "list_files",
        "workspace_read",
    }
)
_WRITE_OPERATIONS = frozenset(
    {
        "file_change",
        "file_delete",
        "file_edit",
        "file_write",
        "delete",
        "edit",
        "patch",
        "write",
        "workspace_write",
    }
)
_COMMAND_OPERATIONS = frozenset(
    {"command", "command_exec", "exec", "run_command", "shell", "terminal"}
)
_NETWORK_OPERATIONS = frozenset(
    {"http", "http_request", "mcp_network", "network", "network_request"}
)
_CAPABILITY_OPERATIONS = frozenset(
    {
        "capability",
        "extension_load",
        "hook_run",
        "mcp_call",
        "plugin_load",
        "skill_load",
        "tool_call",
    }
)


class FilesystemAccess(StrEnum):
    """Filesystem boundary named by the policy, not an OS sandbox."""

    READ_ONLY = "read_only"
    WORKSPACE_WRITE = "workspace_write"
    FULL_ACCESS = "full_access"


FilesystemMode = FilesystemAccess
FilesystemPolicy = FilesystemAccess


class NetworkAccess(StrEnum):
    """Network boundary for future adapters."""

    DENY = "deny"
    ALLOWLIST = "allowlist"
    FULL_ACCESS = "full_access"


NetworkMode = NetworkAccess
NetworkPolicy = NetworkAccess


class CommandScope(StrEnum):
    """Command scope.  ``NONE`` is the safe default."""

    NONE = "none"
    SAFE = "safe"
    WORKSPACE = "workspace"
    FULL_ACCESS = "full_access"


CommandPolicy = CommandScope


class ApprovalMode(StrEnum):
    """How an otherwise permitted request obtains a server-side approval."""

    NEVER = "never"
    ON_RISK = "on_risk"
    ALWAYS = "always"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime, *, field_name: str = "timestamp") -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _coerce_enum(value: Any, enum_type: type[StrEnum], aliases: Mapping[str, str]) -> StrEnum:
    if isinstance(value, enum_type):
        return value
    if not isinstance(value, str):
        raise ValueError(f"{enum_type.__name__} must be a string")
    normalized = value.strip().casefold().replace("-", "_")
    normalized = aliases.get(normalized, normalized)
    try:
        return enum_type(normalized)
    except ValueError as exc:
        choices = ", ".join(member.value for member in enum_type)
        raise ValueError(f"unknown {enum_type.__name__} {value!r}; expected one of {choices}") from exc


def _require_identifier(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or _IDENTIFIER_RE.fullmatch(value.strip()) is None:
        raise ValueError(f"{field_name} must be a bounded identifier")
    return value.strip()


def _normalise_reason(value: str) -> str:
    normalized = value.strip().casefold().replace("-", "_")
    if _REASON_CODE_RE.fullmatch(normalized) is None:
        return "policy_denied"
    return normalized


def _json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=lambda item: {"type": type(item).__name__},
        ).encode("utf-8")
    except Exception:
        return f"<{type(value).__name__}>".encode("utf-8", errors="replace")


def _bounded_text(value: str, max_bytes: int) -> tuple[str, bool]:
    encoded = value.encode("utf-8", errors="replace")
    if len(encoded) <= max_bytes:
        return value, False
    if max_bytes <= 3:
        return encoded[:max_bytes].decode("utf-8", errors="ignore"), True
    prefix = encoded[: max_bytes - 3].decode("utf-8", errors="ignore")
    return prefix + "...", True


def _digest(value: Any) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class PolicyContext:
    """Identity and fencing values supplied by the Pulse control plane."""

    world_id: str
    engram_id: str
    epoch: int
    session_id: str | None = None
    subject_kind: str = "engram"

    def __post_init__(self) -> None:
        object.__setattr__(self, "world_id", _require_identifier(self.world_id, "world_id"))
        object.__setattr__(self, "engram_id", _require_identifier(self.engram_id, "engram_id"))
        if type(self.epoch) is not int or self.epoch < 0:
            raise ValueError("epoch must be a non-negative integer")
        if self.session_id is not None:
            object.__setattr__(self, "session_id", _require_identifier(self.session_id, "session_id"))
        if not isinstance(self.subject_kind, str) or not self.subject_kind.strip():
            raise ValueError("subject_kind must be a non-empty string")
        object.__setattr__(self, "subject_kind", self.subject_kind.strip().casefold())

    @classmethod
    def from_value(cls, value: "PolicyContext | Mapping[str, Any]") -> "PolicyContext":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ValueError("policy context must be a PolicyContext or object")
        return cls(
            world_id=value.get("world_id"),
            engram_id=value.get("engram_id"),
            epoch=value.get("epoch"),
            session_id=value.get("session_id"),
            subject_kind=value.get("subject_kind", "engram"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "world_id": self.world_id,
            "engram_id": self.engram_id,
            "epoch": self.epoch,
            "session_id": self.session_id,
            "subject_kind": self.subject_kind,
        }


@dataclass(frozen=True, slots=True)
class PolicyRequest:
    """Normalized request accepted by :class:`ExecutionPolicy`."""

    operation: str
    path: str | None = None
    command: str | tuple[str, ...] | None = None
    cwd: str | None = None
    network_target: str | None = None
    capability: str | None = None
    tool_name: str | None = None
    source: str | None = None
    shell: bool = False
    requires_approval: bool | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.operation, str) or not self.operation.strip():
            raise ValueError("operation must be a non-empty string")
        normalized = self.operation.strip().casefold().replace("-", "_")
        object.__setattr__(self, "operation", normalized)
        if self.path is not None and not isinstance(self.path, str):
            raise ValueError("path must be a string when supplied")
        if self.cwd is not None and not isinstance(self.cwd, str):
            raise ValueError("cwd must be a string when supplied")
        if self.network_target is not None and not isinstance(self.network_target, str):
            raise ValueError("network_target must be a string when supplied")
        if self.capability is not None:
            object.__setattr__(self, "capability", _require_identifier(self.capability, "capability"))
        if self.tool_name is not None:
            object.__setattr__(self, "tool_name", _require_identifier(self.tool_name, "tool_name"))
        if self.source is not None and not isinstance(self.source, str):
            raise ValueError("source must be a string when supplied")
        if type(self.shell) is not bool:
            raise ValueError("shell must be a bool")
        if self.requires_approval not in (None, True, False):
            raise ValueError("requires_approval must be true, false, or null")
        if not isinstance(self.metadata, Mapping):
            raise ValueError("metadata must be an object")
        if isinstance(self.command, list):
            object.__setattr__(self, "command", tuple(self.command))
        if self.command is not None and not isinstance(self.command, (str, tuple)):
            raise ValueError("command must be a string or sequence")
        if isinstance(self.command, tuple) and any(not isinstance(item, str) for item in self.command):
            raise ValueError("command argv must contain only strings")

    @classmethod
    def from_value(cls, value: "PolicyRequest | Mapping[str, Any]") -> "PolicyRequest":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ValueError("policy request must be a PolicyRequest or object")
        operation = value.get("operation", value.get("kind", value.get("action")))
        command = value.get("command", value.get("argv"))
        capability = value.get("capability", value.get("capability_name"))
        tool_name = value.get("tool_name", value.get("tool"))
        network_target = value.get("network_target", value.get("url", value.get("target")))
        return cls(
            operation=operation,
            path=value.get("path"),
            command=command,
            cwd=value.get("cwd"),
            network_target=network_target,
            capability=capability,
            tool_name=tool_name,
            source=value.get("source"),
            shell=value.get("shell", False),
            requires_approval=value.get("requires_approval"),
            metadata=value.get("metadata", {}),
        )


@dataclass(frozen=True, slots=True)
class PolicyError:
    """Safe, serializable denial reason with no request payload."""

    code: str
    detail: str
    field: str | None = None
    retryable: bool = False
    status: int = 403

    def __post_init__(self) -> None:
        normalized = _normalise_reason(self.code)
        object.__setattr__(self, "code", normalized)
        if not isinstance(self.detail, str) or not self.detail.strip():
            raise ValueError("PolicyError detail must be non-empty")
        if self.field is not None:
            object.__setattr__(self, "field", _normalise_reason(self.field))
        if type(self.retryable) is not bool:
            raise ValueError("retryable must be a bool")
        if type(self.status) is not int or self.status < 400 or self.status > 599:
            raise ValueError("status must be an HTTP error status")

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "detail": self.detail,
            "field": self.field,
            "retryable": self.retryable,
            "status": self.status,
            "evidence_class": CONTRACT_ONLY,
        }


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    """The result of a policy check; it never means that an action ran."""

    allow: bool
    requires_approval: bool
    reason_code: str
    safe_preview: Mapping[str, Any] = field(default_factory=dict)
    expires_at: datetime | None = None
    capability_scope: tuple[str, ...] = ()
    error: PolicyError | None = None
    policy_id: str = "harness.execution.v1"
    evidence_class: str = CONTRACT_ONLY

    def __post_init__(self) -> None:
        if type(self.allow) is not bool or type(self.requires_approval) is not bool:
            raise ValueError("allow and requires_approval must be bools")
        object.__setattr__(self, "reason_code", _normalise_reason(self.reason_code))
        if not isinstance(self.safe_preview, Mapping):
            raise ValueError("safe_preview must be an object")
        object.__setattr__(self, "safe_preview", dict(self.safe_preview))
        if self.expires_at is not None:
            object.__setattr__(self, "expires_at", _ensure_utc(self.expires_at, field_name="expires_at"))
        object.__setattr__(self, "capability_scope", tuple(self.capability_scope))
        if self.error is not None and not isinstance(self.error, PolicyError):
            raise ValueError("error must be a PolicyError")
        if not isinstance(self.policy_id, str) or not self.policy_id.strip():
            raise ValueError("policy_id must be non-empty")
        if self.evidence_class not in {CONTRACT_ONLY, LIVE_GATE_UNVERIFIED}:
            raise ValueError("unknown evidence class")

    @property
    def denied(self) -> bool:
        return not self.allow

    def to_dict(self) -> dict[str, Any]:
        return {
            "allow": self.allow,
            "requires_approval": self.requires_approval,
            "reason_code": self.reason_code,
            "safe_preview": dict(self.safe_preview),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "capability_scope": list(self.capability_scope),
            "error": self.error.to_dict() if self.error else None,
            "policy_id": self.policy_id,
            "evidence_class": self.evidence_class,
        }


def _coerce_policy_decision(value: Any) -> PolicyDecision:
    """Normalize an adapter result into the canonical policy decision type."""

    if isinstance(value, PolicyDecision):
        return value
    if isinstance(value, Mapping):
        get = value.get
    else:
        get = lambda key, default=None: getattr(value, key, default)

    raw_allow = get("allow", get("allowed", False))
    allow = raw_allow if type(raw_allow) is bool else False
    raw_requires_approval = get("requires_approval", False)
    requires_approval = (
        raw_requires_approval if type(raw_requires_approval) is bool else False
    )
    safe_preview = get("safe_preview", {})
    if not isinstance(safe_preview, Mapping):
        safe_preview = {}
    raw_expires_at = get("expires_at")
    expires_at = raw_expires_at if isinstance(raw_expires_at, datetime) else None
    raw_scope = get("capability_scope", ())
    capability_scope = tuple(raw_scope) if isinstance(raw_scope, (list, tuple)) else ()
    raw_error = get("error")
    error = raw_error if isinstance(raw_error, PolicyError) else None
    evidence_class = str(get("evidence_class", CONTRACT_ONLY))
    if evidence_class not in {CONTRACT_ONLY, LIVE_GATE_UNVERIFIED}:
        evidence_class = CONTRACT_ONLY
    return PolicyDecision(
        allow=allow,
        requires_approval=requires_approval,
        reason_code=str(
            get("reason_code", get("reason", "external_policy_decision"))
        ),
        safe_preview=dict(safe_preview),
        expires_at=expires_at,
        capability_scope=capability_scope,
        error=error,
        policy_id=str(get("policy_id", "external-policy")),
        evidence_class=evidence_class,
    )


def resolve_policy_decision(
    policy_context: Any,
    request: Any,
    *,
    action: str,
) -> PolicyDecision:
    """Resolve the canonical execution-policy boundary, failing closed.

    Terminal, file-change and checkpoint adapters all consume this function;
    keeping the resolver here prevents each execution surface from inventing
    a subtly different policy result type.
    """

    if not isinstance(action, str) or not action.strip():
        raise ValueError("action must be a non-empty string")
    if policy_context is None:
        return PolicyDecision(
            allow=False,
            requires_approval=False,
            reason_code="policy_required",
            error=PolicyError("policy_required", "a policy context is required"),
        )
    try:
        if isinstance(policy_context, (PolicyDecision, Mapping)):
            value = policy_context
        elif hasattr(policy_context, "authorize"):
            try:
                value = policy_context.authorize(request, action=action)
            except TypeError:
                value = policy_context.authorize(request)
        elif hasattr(policy_context, "evaluate"):
            try:
                value = policy_context.evaluate(request, action=action)
            except TypeError:
                value = policy_context.evaluate(request)
        elif callable(policy_context):
            value = policy_context(request)
        else:
            return PolicyDecision(
                allow=False,
                requires_approval=False,
                reason_code="policy_context_unsupported",
                error=PolicyError(
                    "policy_context_unsupported",
                    "the policy context does not expose an evaluator",
                ),
            )
        return _coerce_policy_decision(value)
    except Exception as exc:
        return PolicyDecision(
            allow=False,
            requires_approval=False,
            reason_code="policy_evaluation_failed",
            error=PolicyError(
                "policy_evaluation_failed",
                f"policy evaluation failed: {type(exc).__name__}",
            ),
        )


class PolicyDeniedError(PermissionError):
    """Raised by ``ExecutionPolicy.enforce`` with a structured safe error."""

    def __init__(self, error: PolicyError, *, decision: PolicyDecision | None = None) -> None:
        self.error = error
        self.decision = decision
        super().__init__(f"{error.code}: {error.detail}")

    def to_dict(self) -> dict[str, Any]:
        payload = self.error.to_dict()
        if self.decision is not None:
            payload["decision"] = self.decision.to_dict()
        return payload


@dataclass(frozen=True, slots=True)
class RedactionReport:
    """Evidence about a safe projection, never the removed value itself."""

    redacted: bool
    truncated: bool
    reasons: tuple[str, ...]
    original_type: str
    original_bytes: int
    safe_bytes: int
    digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "reasons", tuple(dict.fromkeys(self.reasons)))
        if self.original_bytes < 0 or self.safe_bytes < 0:
            raise ValueError("redaction byte counts must be non-negative")

    def to_dict(self) -> dict[str, Any]:
        return {
            "redacted": self.redacted,
            "truncated": self.truncated,
            "reasons": list(self.reasons),
            "original_type": self.original_type,
            "original_bytes": self.original_bytes,
            "safe_bytes": self.safe_bytes,
            "digest": self.digest,
        }


class _RedactionState:
    def __init__(self) -> None:
        self.redacted = False
        self.truncated = False
        self.reasons: list[str] = []

    def add(self, reason: str, *, redacted: bool = True, truncated: bool = False) -> None:
        if reason not in self.reasons:
            self.reasons.append(reason)
        self.redacted = self.redacted or redacted
        self.truncated = self.truncated or truncated


class Redactor:
    """Bounded recursive redaction for event, approval and error payloads."""

    def __init__(
        self,
        *,
        workspace_root: str | Path | None = None,
        max_text_bytes: int = 16 * 1024,
        max_payload_bytes: int = 64 * 1024,
    ) -> None:
        if type(max_text_bytes) is not int or max_text_bytes < 64:
            raise ValueError("max_text_bytes must be an integer >= 64")
        if type(max_payload_bytes) is not int or max_payload_bytes < max_text_bytes:
            raise ValueError("max_payload_bytes must be >= max_text_bytes")
        self._workspace_root = (
            Path(workspace_root).expanduser().resolve()
            if workspace_root is not None
            else None
        )
        self.max_text_bytes = max_text_bytes
        self.max_payload_bytes = max_payload_bytes

    @property
    def workspace_root(self) -> Path | None:
        return self._workspace_root

    def redact(self, value: Any, context: Mapping[str, Any] | None = None) -> tuple[Any, RedactionReport]:
        """Return a new safe value and bounded redaction evidence.

        ``context`` is intentionally descriptive only; callers cannot disable
        redaction through it.  ``Path`` and ``bytes`` values are converted to
        safe JSON-compatible forms.  Exceptions are represented by type and a
        redacted message, never by traceback or environment details.
        """

        del context
        state = _RedactionState()
        original_bytes = self._original_size(value)
        digest = _digest(value)
        safe = self._redact_value(value, state, key=None)
        encoded = _json_bytes(safe)
        if len(encoded) > self.max_payload_bytes:
            state.add("payload_cap", redacted=False, truncated=True)
            safe = {
                "type": self._type_name(value),
                "bytes": original_bytes,
                "digest": digest,
                "truncated": True,
            }
            encoded = _json_bytes(safe)
        report = RedactionReport(
            redacted=state.redacted,
            truncated=state.truncated,
            reasons=tuple(state.reasons),
            original_type=self._type_name(value),
            original_bytes=original_bytes,
            safe_bytes=len(encoded),
            digest=digest,
        )
        return safe, report

    def safe_preview(self, value: Any) -> Any:
        """Return only the safe part of :meth:`redact` for UI previews."""

        return self.redact(value)[0]

    def _type_name(self, value: Any) -> str:
        if isinstance(value, BaseException):
            return "exception"
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, (int, float)):
            return "number"
        if isinstance(value, str):
            return "string"
        if isinstance(value, bytes):
            return "bytes"
        if isinstance(value, Mapping):
            return "object"
        if isinstance(value, (list, tuple, set, frozenset)):
            return "array"
        if isinstance(value, Path):
            return "path"
        return type(value).__name__

    def _original_size(self, value: Any) -> int:
        if isinstance(value, str):
            return len(value.encode("utf-8", errors="replace"))
        if isinstance(value, bytes):
            return len(value)
        return len(_json_bytes(value))

    def _redact_value(self, value: Any, state: _RedactionState, *, key: str | None) -> Any:
        if key is not None and _SECRET_KEY_RE.search(key.replace(" ", "_")):
            state.add("secret_key")
            return "[REDACTED]"
        if isinstance(value, BaseException):
            state.add("exception_message")
            message, _ = self._redact_string(str(value), state)
            return {"type": type(value).__name__, "message": message}
        if isinstance(value, Path):
            state.add("path_value")
            return self._safe_path(value, state)
        if isinstance(value, bytes):
            try:
                text = value.decode("utf-8", errors="replace")
            except Exception:
                state.add("bytes_value")
                return {"type": "bytes", "length": len(value)}
            safe, _ = self._redact_string(text, state)
            return safe
        if isinstance(value, str):
            safe, _ = self._redact_string(value, state)
            return safe
        if isinstance(value, Mapping):
            output: dict[str, Any] = {}
            for raw_key, child in value.items():
                if not isinstance(raw_key, str):
                    safe_key = "[NON_STRING_KEY]"
                    state.add("non_string_key")
                else:
                    safe_key = self._safe_key(raw_key, state)
                output[safe_key] = self._redact_value(
                    child,
                    state,
                    key=raw_key if isinstance(raw_key, str) else None,
                )
            return output
        if isinstance(value, (list, tuple, set, frozenset)):
            return [self._redact_value(child, state, key=None) for child in value]
        if value is None or isinstance(value, (bool, int)):
            return value
        if isinstance(value, float):
            if value != value or value in {float("inf"), float("-inf")}:
                state.add("non_finite_number")
                return None
            return value
        state.add("unsupported_type")
        return {"type": self._type_name(value), "value": "[REDACTED]"}

    def _safe_key(self, key: str, state: _RedactionState) -> str:
        bounded, truncated = _bounded_text(key, 128)
        if truncated:
            state.add("key_cap", redacted=False, truncated=True)
        # A field name is useful for an audit preview, but a key that itself
        # contains a path or credential-looking value must not pass through.
        if _SECRET_KEY_RE.search(bounded.replace(" ", "_")):
            return bounded
        safe, _ = self._redact_string(bounded, state, redact_secrets=False)
        return safe

    def _redact_string(
        self,
        value: str,
        state: _RedactionState,
        *,
        redact_secrets: bool = True,
    ) -> tuple[str, bool]:
        safe = value
        if redact_secrets:
            before = safe
            safe = _SECRET_ASSIGNMENT_RE.sub(
                lambda match: match.group("label") + "[REDACTED]",
                safe,
            )
            safe = _BEARER_RE.sub("Bearer [REDACTED]", safe)
            safe = _COMMON_KEY_RE.sub("[REDACTED]", safe)
            safe = _AWS_ACCESS_KEY_RE.sub("[REDACTED]", safe)
            safe = _URL_SECRET_QUERY_RE.sub(lambda match: match.group(1).split("=", 1)[0] + "=[REDACTED]", safe)
            if safe != before:
                state.add("secret_value")

        def replace_windows(match: re.Match[str]) -> str:
            state.add("absolute_path")
            return self._safe_path(match.group(0), state)

        def replace_posix(match: re.Match[str]) -> str:
            candidate = match.group(0)
            # Do not mistake a bare slash in punctuation for a path.
            if len(candidate) == 1:
                return candidate
            state.add("absolute_path")
            return self._safe_path(candidate, state)

        safe = _WINDOWS_PATH_RE.sub(replace_windows, safe)
        safe = _POSIX_PATH_RE.sub(replace_posix, safe)
        bounded, truncated = _bounded_text(safe, self.max_text_bytes)
        if truncated:
            state.add("text_cap", redacted=False, truncated=True)
        return bounded, truncated

    def _safe_path(self, value: str | Path, state: _RedactionState) -> str:
        raw = str(value)
        try:
            candidate_path = Path(raw).expanduser()
            if not candidate_path.is_absolute() and self._workspace_root is not None:
                candidate_path = self._workspace_root / candidate_path
            candidate = candidate_path.resolve()
        except (OSError, RuntimeError, ValueError):
            return "[ABSOLUTE_PATH_REDACTED]"
        if self._workspace_root is not None:
            try:
                relative = candidate.relative_to(self._workspace_root)
            except ValueError:
                return "[ABSOLUTE_PATH_REDACTED]"
            relative_text = relative.as_posix() or "."
            return f"./{relative_text}" if relative_text != "." else "."
        return "[ABSOLUTE_PATH_REDACTED]"


@dataclass(frozen=True, slots=True)
class _PathResult:
    path: Path | None
    safe_relative: str
    error: PolicyError | None = None


class ExecutionPolicy:
    """Evaluate Harness actions without executing them.

    This object is safe to use from an adapter, but it is not a sandbox and it
    cannot prove DNS, OS, process, or Pi behaviour.  A returned allow decision
    therefore carries ``evidence_class=CONTRACT_ONLY`` until a Lead-owned live
    adapter supplies independent evidence.
    """

    def __init__(
        self,
        *,
        workspace_root: str | Path,
        filesystem: FilesystemAccess | str = FilesystemAccess.READ_ONLY,
        network: NetworkAccess | str = NetworkAccess.DENY,
        command: CommandScope | str = CommandScope.NONE,
        command_scope: CommandScope | str | None = None,
        command_allowlist: Iterable[str] = (),
        tool_allowlist: Iterable[str] = (),
        capability_allowlist: Iterable[str] | None = None,
        network_allowlist: Iterable[str] = (),
        approval_mode: ApprovalMode | str = ApprovalMode.ON_RISK,
        approval_ttl_seconds: float = 300.0,
        protected_roots: Iterable[str | Path] | None = None,
        allow_full_access: bool = False,
        policy_id: str = "harness.execution.v1",
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        root = Path(workspace_root).expanduser().resolve()
        if not root.is_dir():
            raise ValueError("workspace_root must be an existing directory")
        if command_scope is not None:
            command = command_scope
        self._workspace_root = root
        self.filesystem = _coerce_enum(
            filesystem,
            FilesystemAccess,
            {"read": "read_only", "workspace": "workspace_write", "full": "full_access"},
        )
        self.network = _coerce_enum(
            network,
            NetworkAccess,
            {"none": "deny", "allow": "allowlist", "full": "full_access"},
        )
        self.command = _coerce_enum(
            command,
            CommandScope,
            {"disabled": "none", "workspace_write": "workspace", "full": "full_access"},
        )
        self.approval_mode = _coerce_enum(
            approval_mode,
            ApprovalMode,
            {"required": "always", "require": "always", "onrisk": "on_risk"},
        )
        if isinstance(approval_ttl_seconds, bool) or not isinstance(approval_ttl_seconds, (int, float)):
            raise ValueError("approval_ttl_seconds must be a positive number")
        if approval_ttl_seconds <= 0 or approval_ttl_seconds > 24 * 60 * 60:
            raise ValueError("approval_ttl_seconds must be between 0 and 86400")
        if type(allow_full_access) is not bool:
            raise ValueError("allow_full_access must be a bool")
        self.approval_ttl_seconds = float(approval_ttl_seconds)
        self.allow_full_access = allow_full_access
        self.policy_id = _require_identifier(policy_id, "policy_id")
        self._clock = clock or _utc_now
        self.command_allowlist = frozenset(self._normalise_command(item) for item in command_allowlist)
        raw_capabilities = capability_allowlist if capability_allowlist is not None else tool_allowlist
        self.capability_allowlist = frozenset(self._normalise_capability(item) for item in raw_capabilities)
        self.network_allowlist = frozenset(self._normalise_host(item) for item in network_allowlist)
        protected = tuple(protected_roots) if protected_roots is not None else (root / ".pulse",)
        normalized_protected: list[Path] = []
        for item in protected:
            candidate = Path(item).expanduser().resolve()
            if not candidate.is_relative_to(root):
                raise ValueError("protected_roots must stay inside workspace_root")
            normalized_protected.append(candidate)
        self.protected_roots = tuple(normalized_protected)
        self.redactor = Redactor(workspace_root=root)

    @property
    def workspace_root(self) -> Path:
        return self._workspace_root

    @property
    def evidence_class(self) -> str:
        return CONTRACT_ONLY

    def evaluate(
        self,
        request: PolicyRequest | Mapping[str, Any],
        context: PolicyContext | Mapping[str, Any] | None = None,
    ) -> PolicyDecision:
        """Return an explicit safe decision; never execute or silently allow."""

        try:
            normalized = PolicyRequest.from_value(request)
            policy_context = self._coerce_context(context)
        except (TypeError, ValueError):
            return self._deny(
                "invalid_request",
                "the policy request or identity context is invalid",
            )
        del policy_context  # identity is passed to the approval layer; policy is pure.

        operation = normalized.operation
        if operation in _READ_OPERATIONS:
            return self._evaluate_file(normalized, write=False)
        if operation in _WRITE_OPERATIONS:
            return self._evaluate_file(normalized, write=True)
        if operation in _COMMAND_OPERATIONS:
            return self._evaluate_command(normalized)
        if operation in _NETWORK_OPERATIONS:
            return self._evaluate_network(normalized)
        if operation in _CAPABILITY_OPERATIONS:
            return self._evaluate_capability(normalized)
        return self._deny("unsupported_operation", "the requested operation is not in the policy vocabulary")

    def enforce(
        self,
        request: PolicyRequest | Mapping[str, Any],
        context: PolicyContext | Mapping[str, Any] | None = None,
    ) -> PolicyDecision:
        """Raise a structured error unless the policy permits the request."""

        decision = self.evaluate(request, context)
        if not decision.allow:
            error = decision.error or PolicyError(
                decision.reason_code,
                "the policy did not permit this request",
            )
            raise PolicyDeniedError(error, decision=decision)
        return decision

    def resolve_path(self, path: str, *, write: bool = False) -> Path:
        """Resolve an already-authorized path for an adapter.

        The absolute path is intentionally available only to the adapter that
        calls this method after policy enforcement; previews use
        ``safe_relative_path`` instead.
        """

        result = self._path_result(path, write=write)
        if result.error is not None or result.path is None:
            raise PolicyDeniedError(result.error or PolicyError("path_invalid", "path is invalid"))
        return result.path

    def safe_relative_path(self, path: str, *, write: bool = False) -> str:
        result = self._path_result(path, write=write)
        if result.error is not None:
            return "[PATH_REDACTED]"
        return result.safe_relative

    def _coerce_context(self, value: PolicyContext | Mapping[str, Any] | None) -> PolicyContext:
        if value is None:
            # A policy decision can be inspected before a live turn is bound,
            # but integrations should always provide the real context before
            # creating an approval.
            return PolicyContext(world_id="unbound", engram_id="unbound", epoch=0)
        return PolicyContext.from_value(value)

    def _evaluate_file(self, request: PolicyRequest, *, write: bool) -> PolicyDecision:
        if request.path is None or not request.path.strip():
            return self._deny("path_required", "a workspace-relative path is required", field="path")
        result = self._path_result(request.path, write=write)
        if result.error is not None:
            return self._decision_from_error(result.error, {"operation": request.operation})
        assert result.path is not None
        preview = {
            "operation": request.operation,
            "path": result.safe_relative,
            "filesystem": self.filesystem.value,
        }
        if not write:
            return self._allow(preview, scope=("filesystem:read",))
        if self.filesystem is FilesystemAccess.READ_ONLY:
            return self._deny("filesystem_write_denied", "the filesystem policy is read_only")
        if self.filesystem is FilesystemAccess.FULL_ACCESS and not self.allow_full_access:
            return self._deny("full_access_unsupported", "full_access requires an explicitly verified adapter")
        for protected in self.protected_roots:
            if result.path == protected or result.path.is_relative_to(protected):
                return self._deny("path_protected", "the target is inside a protected runtime root")
        return self._allow_or_approve(preview, scope=("filesystem:write",))

    def _evaluate_command(self, request: PolicyRequest) -> PolicyDecision:
        if self.command is CommandScope.NONE:
            return self._deny("command_denied", "command execution is disabled by policy")
        if self.command is CommandScope.FULL_ACCESS and not self.allow_full_access:
            return self._deny("full_access_unsupported", "full_access requires an explicitly verified adapter")
        argv = self._command_argv(request.command)
        if not argv:
            return self._deny("command_required", "a non-empty command is required", field="command")
        if request.shell or (isinstance(request.command, str) and _SHELL_META_RE.search(request.command)):
            return self._deny("shell_syntax_denied", "shell metacharacters require an explicit shell adapter")
        executable = self._normalise_command(argv[0])
        if executable not in self.command_allowlist:
            return self._deny("command_not_allowlisted", "the command executable is not allowlisted")
        cwd = request.cwd or "."
        cwd_result = self._path_result(cwd, write=False)
        if cwd_result.error is not None:
            return self._decision_from_error(cwd_result.error, {"operation": request.operation})
        preview = {
            "operation": request.operation,
            "executable": Path(executable).name,
            "argument_count": max(0, len(argv) - 1),
            "cwd": cwd_result.safe_relative,
            "command_scope": self.command.value,
        }
        return self._allow_or_approve(preview, scope=(f"command:{Path(executable).name}",))

    def _evaluate_network(self, request: PolicyRequest) -> PolicyDecision:
        if self.network is NetworkAccess.DENY:
            return self._deny("network_denied", "network access is disabled by policy")
        if self.network is NetworkAccess.FULL_ACCESS and not self.allow_full_access:
            return self._deny("network_full_access_unsupported", "full network access requires an explicitly verified adapter")
        target = request.network_target
        if target is None or not target.strip():
            return self._deny("network_target_required", "a network target is required", field="network_target")
        parsed, error = self._parse_network_target(target)
        if error is not None:
            return self._decision_from_error(error, {"operation": request.operation})
        assert parsed is not None
        host = parsed.hostname or ""
        if not self._host_is_allowlisted(host):
            return self._deny("network_host_not_allowlisted", "the network host is not in the policy allowlist")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        preview = {
            "operation": request.operation,
            "scheme": parsed.scheme,
            "host": host.casefold(),
            "port": port,
            "network": self.network.value,
        }
        return self._allow_or_approve(preview, scope=(f"network:{host.casefold()}",))

    def _evaluate_capability(self, request: PolicyRequest) -> PolicyDecision:
        raw_name = request.capability or request.tool_name
        if raw_name is None or not raw_name.strip() or _CAPABILITY_RE.fullmatch(raw_name.strip()) is None:
            return self._deny("capability_required", "a bounded capability name is required", field="capability")
        name = raw_name.strip()
        if not self._capability_is_allowlisted(name):
            return self._deny("capability_denied", "the capability is not allowlisted")
        preview = {
            "operation": request.operation,
            "capability": name,
            "source": self._safe_source(request.source),
        }
        return self._allow_or_approve(preview, scope=(f"capability:{name}",))

    def _allow(self, preview: Mapping[str, Any], *, scope: tuple[str, ...]) -> PolicyDecision:
        return PolicyDecision(
            allow=True,
            requires_approval=False,
            reason_code="allowed",
            safe_preview=preview,
            capability_scope=scope,
            policy_id=self.policy_id,
        )

    def _allow_or_approve(self, preview: Mapping[str, Any], *, scope: tuple[str, ...]) -> PolicyDecision:
        risk = preview.get("operation") not in _READ_OPERATIONS
        needs_approval = self.approval_mode is ApprovalMode.ALWAYS or (
            self.approval_mode is ApprovalMode.ON_RISK and risk
        )
        if needs_approval:
            expires_at = _ensure_utc(self._clock()) + timedelta(seconds=self.approval_ttl_seconds)
            return PolicyDecision(
                allow=False,
                requires_approval=True,
                reason_code="approval_required",
                safe_preview=preview,
                expires_at=expires_at,
                capability_scope=scope,
                error=PolicyError(
                    "approval_required",
                    "server-side approval is required before this action",
                    status=403,
                ),
                policy_id=self.policy_id,
            )
        return self._allow(preview, scope=scope)

    def _deny(self, code: str, detail: str, *, field: str | None = None) -> PolicyDecision:
        error = PolicyError(code, detail, field=field)
        return PolicyDecision(
            allow=False,
            requires_approval=False,
            reason_code=error.code,
            safe_preview={},
            error=error,
            policy_id=self.policy_id,
        )

    def _decision_from_error(self, error: PolicyError, preview: Mapping[str, Any]) -> PolicyDecision:
        return PolicyDecision(
            allow=False,
            requires_approval=False,
            reason_code=error.code,
            safe_preview=preview,
            error=error,
            policy_id=self.policy_id,
        )

    def _path_result(self, raw_path: str, *, write: bool) -> _PathResult:
        if not isinstance(raw_path, str) or not raw_path.strip():
            return _PathResult(None, "[PATH_REDACTED]", PolicyError("path_required", "a path is required", field="path"))
        if "\u0000" in raw_path:
            return _PathResult(None, "[PATH_REDACTED]", PolicyError("path_invalid", "path contains a forbidden character", field="path"))
        try:
            candidate = Path(raw_path).expanduser()
            resolved = candidate.resolve() if candidate.is_absolute() else (self._workspace_root / candidate).resolve()
        except (OSError, RuntimeError, ValueError):
            return _PathResult(None, "[PATH_REDACTED]", PolicyError("path_invalid", "path cannot be resolved", field="path"))
        inside = resolved.is_relative_to(self._workspace_root)
        if not inside and not (self.filesystem is FilesystemAccess.FULL_ACCESS and self.allow_full_access):
            return _PathResult(None, "[PATH_REDACTED]", PolicyError("path_outside_workspace", "path must remain inside the workspace", field="path"))
        try:
            relative = resolved.relative_to(self._workspace_root).as_posix() or "."
            safe_relative = f"./{relative}" if relative != "." else "."
        except ValueError:
            safe_relative = "[ABSOLUTE_PATH_REDACTED]"
        if write:
            for protected in self.protected_roots:
                if resolved == protected or resolved.is_relative_to(protected):
                    return _PathResult(None, safe_relative, PolicyError("path_protected", "the target is inside a protected runtime root", field="path"))
        return _PathResult(resolved, safe_relative, None)

    @staticmethod
    def _command_argv(command: str | tuple[str, ...] | None) -> tuple[str, ...]:
        if command is None:
            return ()
        if isinstance(command, tuple):
            return tuple(item for item in command if item.strip())
        try:
            return tuple(item for item in shlex.split(command, posix=False) if item.strip())
        except ValueError:
            return ()

    @staticmethod
    def _normalise_command(value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("command allowlist entries must be non-empty strings")
        return Path(value.strip().replace("\\", "/")).name.casefold()

    @staticmethod
    def _normalise_capability(value: Any) -> str:
        if not isinstance(value, str) or not value.strip() or _CAPABILITY_RE.fullmatch(value.strip()) is None:
            raise ValueError("capability allowlist entries must have a bounded name")
        return value.strip()

    @staticmethod
    def _normalise_host(value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("network allowlist entries must be non-empty strings")
        candidate = value.strip().casefold().rstrip(".")
        if "/" in candidate:
            try:
                return str(ipaddress.ip_network(candidate, strict=False))
            except ValueError as exc:
                raise ValueError("network allowlist CIDR is invalid") from exc
        if any(character.isspace() for character in candidate) or len(candidate) > 253:
            raise ValueError("network allowlist host is invalid")
        return candidate

    def _host_is_allowlisted(self, host: str) -> bool:
        normalized = host.casefold().rstrip(".")
        if not normalized:
            return False
        try:
            address = ipaddress.ip_address(normalized)
        except ValueError:
            address = None
        if address is not None and (
            address.is_private
            or address.is_loopback
            or address.is_link_local
            or address.is_multicast
            or address.is_reserved
            or address.is_unspecified
        ):
            # Private/local targets remain denied unless explicitly named.  A
            # future local adapter can use a dedicated loopback policy entry.
            if normalized not in self.network_allowlist:
                return False
        if self.network is NetworkAccess.FULL_ACCESS:
            return self.allow_full_access
        for allowed in self.network_allowlist:
            if allowed == normalized:
                return True
            if allowed.startswith("*."):
                suffix = allowed[1:]
                if normalized.endswith(suffix) and normalized != suffix.lstrip("."):
                    return True
            try:
                if address is not None and address in ipaddress.ip_network(allowed, strict=False):
                    return True
            except ValueError:
                continue
        return False

    @staticmethod
    def _parse_network_target(raw: str) -> tuple[SplitResult | None, PolicyError | None]:
        try:
            parsed = urlsplit(raw.strip())
            if parsed.scheme.casefold() not in {"http", "https"} or not parsed.hostname:
                return None, PolicyError("network_target_invalid", "network target must be an http or https URL")
            if parsed.username is not None or parsed.password is not None:
                return None, PolicyError("network_credentials_in_url", "credentials in network URLs are not accepted")
            if parsed.fragment:
                return None, PolicyError("network_target_invalid", "URL fragments are not part of an execution target")
            # Accessing port validates malformed numeric ports.
            _ = parsed.port
            safe = urlunsplit((parsed.scheme.casefold(), parsed.netloc, parsed.path, "", ""))
            return urlsplit(safe), None
        except ValueError:
            return None, PolicyError("network_target_invalid", "network target is malformed")

    def _capability_is_allowlisted(self, name: str) -> bool:
        if name in self.capability_allowlist:
            return True
        for allowed in self.capability_allowlist:
            if allowed.endswith("/*") and name.startswith(allowed[:-1]):
                return True
        return False

    @staticmethod
    def _safe_source(source: str | None) -> str:
        if source is None or not source.strip():
            return "unspecified"
        bounded, _ = _bounded_text(source.strip(), 128)
        return bounded if re.fullmatch(r"[A-Za-z0-9_.:/@-]+", bounded) else "untrusted"

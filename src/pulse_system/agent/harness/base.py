"""Shared contracts for persistent agent Harness sessions.

The delegation ``AgentBackend`` contract intentionally remains narrow and
one-shot.  These types describe the separate lifetime contract used when one
Engram owns a durable harness session across many pulses.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Mapping, Protocol, runtime_checkable

__all__ = [
    "BINDING_COMPONENT",
    "BINDING_VERSION",
    "BindingState",
    "HarnessError",
    "HarnessRuntime",
    "HarnessEventCallback",
    "HarnessSession",
    "HarnessState",
    "HarnessTurnResult",
    "SessionBinding",
    "binding_snapshot",
    "load_binding_state",
    "normalize_session_file",
]

BINDING_COMPONENT = "harness.pi.sessions.v1"
BINDING_VERSION = 1

# The callback receives a copy of one Pi/Pulse event.  It is an observation
# seam, not a permission seam: implementations must redact and bound the
# payload before persistence, and callback failures must never change the
# accepted-turn outcome.
HarnessEventCallback = Callable[[str, str | None, Mapping[str, Any]], None]


class HarnessState(StrEnum):
    """Observable lifecycle states for one durable Harness session."""

    UNBOUND = "UNBOUND"
    STARTING = "STARTING"
    READY = "READY"
    ADMITTING = "ADMITTING"
    RUNNING = "RUNNING"
    SETTLING = "SETTLING"
    ROTATING = "ROTATING"
    BROKEN = "BROKEN"
    CLOSED = "CLOSED"


class BindingState(StrEnum):
    """Durability state of one persisted Pi session binding."""

    PENDING_LINEAGE = "pending_lineage"
    MATERIALIZED = "materialized"


class HarnessError(Exception):
    """A classified Harness failure with retry and acceptance semantics."""

    def __init__(
        self,
        code: str,
        detail: str,
        remedy: str,
        *,
        phase: str,
        retryable: bool = False,
        prompt_accepted: bool | None = False,
        partial_output: str = "",
        trace: list[dict[str, Any]] | None = None,
    ) -> None:
        if not all(isinstance(value, str) and value for value in (
            code,
            detail,
            remedy,
            phase,
        )):
            raise ValueError("HarnessError code/detail/remedy/phase must be non-empty")
        self.code = code
        self.detail = detail
        self.remedy = remedy
        self.phase = phase
        self.retryable = bool(retryable)
        if prompt_accepted not in (True, False, None):
            raise ValueError("prompt_accepted must be true, false, or None")
        self.prompt_accepted = prompt_accepted
        self.partial_output = partial_output if isinstance(partial_output, str) else ""
        self.trace = list(trace or [])
        super().__init__(f"{code}: {detail}\n  remedy: {remedy}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "detail": self.detail,
            "remedy": self.remedy,
            "phase": self.phase,
            "retryable": self.retryable,
            "prompt_accepted": self.prompt_accepted,
            "partial_output": self.partial_output,
            "trace": self.trace,
        }


def normalize_session_file(value: str) -> str:
    """Return the canonical absolute spelling used by binding snapshots."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError("session_file must be a non-empty string")
    return os.path.abspath(os.path.normpath(os.path.expanduser(value)))


def _binding_key(value: str) -> str:
    return os.path.normcase(normalize_session_file(value))


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _validate_utc_timestamp(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("updated_at must be an RFC3339 UTC string")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError("updated_at must be an RFC3339 UTC string") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("updated_at must use UTC")
    return value


@dataclass(frozen=True, slots=True)
class SessionBinding:
    """One Engram's durable pointer to a Pi JSONL session file."""

    engram_id: str
    state: BindingState
    session_file: str | None = None
    session_id: str | None = None
    parent_session_file: str | None = None
    bootstrapped: bool = False
    updated_at: str = field(default_factory=_utc_now)

    def __post_init__(self) -> None:
        if not isinstance(self.engram_id, str) or not self.engram_id.strip():
            raise ValueError("engram_id must be a non-empty string")
        try:
            state = BindingState(self.state)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unknown binding state: {self.state!r}") from exc
        object.__setattr__(self, "state", state)
        if self.parent_session_file is not None:
            object.__setattr__(
                self,
                "parent_session_file",
                normalize_session_file(self.parent_session_file),
            )
        if type(self.bootstrapped) is not bool:
            raise ValueError("bootstrapped must be a bool")
        _validate_utc_timestamp(self.updated_at)

        if state is BindingState.PENDING_LINEAGE:
            if self.session_id is not None or self.session_file is not None:
                raise ValueError("pending_lineage requires null session_id/session_file")
            if self.parent_session_file is None:
                raise ValueError("pending_lineage requires parent_session_file")
            if self.bootstrapped:
                raise ValueError("pending_lineage requires bootstrapped=false")
        else:
            if not isinstance(self.session_id, str) or not self.session_id:
                raise ValueError("materialized binding requires a session_id")
            if not isinstance(self.session_file, str) or not self.session_file.strip():
                raise ValueError("materialized binding requires a session_file")
            object.__setattr__(
                self,
                "session_file",
                normalize_session_file(self.session_file),
            )

    @classmethod
    def from_wire(cls, engram_id: str, value: Mapping[str, Any]) -> "SessionBinding":
        if not isinstance(value, Mapping):
            raise ValueError("binding must be an object")
        required = {
            "state",
            "session_id",
            "session_file",
            "parent_session_file",
            "bootstrapped",
            "updated_at",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise ValueError(f"binding is missing fields: {', '.join(missing)}")
        return cls(
            engram_id=engram_id,
            state=value["state"],
            session_id=value["session_id"],
            session_file=value["session_file"],
            parent_session_file=value["parent_session_file"],
            bootstrapped=value["bootstrapped"],
            updated_at=value["updated_at"],
        )

    def to_wire(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "session_id": self.session_id,
            "session_file": self.session_file,
            "parent_session_file": self.parent_session_file,
            "bootstrapped": self.bootstrapped,
            "updated_at": self.updated_at,
        }

def load_binding_state(value: Mapping[str, Any] | None) -> dict[str, SessionBinding]:
    """Validate and normalize the complete ``harness.pi.sessions.v1`` value."""

    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise HarnessError(
            "pi_binding_invalid",
            "the persisted Pi binding component is not an object",
            f"replace {BINDING_COMPONENT} with a valid v1 binding snapshot",
            phase="binding",
        )
    version = value.get("version")
    if type(version) is not int or version != BINDING_VERSION:
        raise HarnessError(
            "pi_binding_version_unsupported",
            f"the persisted Pi binding version is {version!r}, not {BINDING_VERSION}",
            "migrate the binding component explicitly before starting the Harness",
            phase="binding",
        )
    sessions = value.get("sessions")
    if not isinstance(sessions, Mapping):
        raise HarnessError(
            "pi_binding_invalid",
            "the persisted Pi binding component has no sessions object",
            f"repair {BINDING_COMPONENT} before starting the Harness",
            phase="binding",
        )

    loaded: dict[str, SessionBinding] = {}
    owners: dict[str, str] = {}
    try:
        for engram_id, raw_binding in sessions.items():
            if not isinstance(engram_id, str) or not engram_id.strip():
                raise ValueError("every binding key must be a non-empty Engram ID")
            binding = SessionBinding.from_wire(engram_id, raw_binding)
            if binding.state is BindingState.MATERIALIZED:
                assert binding.session_file is not None
                key = _binding_key(binding.session_file)
                previous = owners.get(key)
                if previous is not None:
                    raise HarnessError(
                        "pi_binding_conflict",
                        f"Engrams {previous!r} and {engram_id!r} share Pi session file {binding.session_file!r}",
                        "assign each Engram a distinct Pi session file before startup",
                        phase="binding",
                    )
                owners[key] = engram_id
            loaded[engram_id] = binding
    except HarnessError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise HarnessError(
            "pi_binding_invalid",
            f"the persisted Pi binding component is invalid: {exc}",
            f"repair {BINDING_COMPONENT} before starting the Harness",
            phase="binding",
        ) from exc
    return loaded


def binding_snapshot(bindings: Mapping[str, SessionBinding]) -> dict[str, Any]:
    """Build a complete deterministic v1 snapshot for persistence callbacks."""

    return {
        "version": BINDING_VERSION,
        "sessions": {
            engram_id: bindings[engram_id].to_wire()
            for engram_id in sorted(bindings)
        },
    }


@dataclass(frozen=True, slots=True)
class HarnessTurnResult:
    """The settled natural-language projection of one complete Harness turn."""

    engram_id: str
    session_file: str
    content: str
    stop_reason: str
    session_id: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0
    cache_write_tokens: int = 0
    tool_calls: int = 0
    provider_requests: int = 0
    trace: list[dict[str, Any]] = field(default_factory=list)
    evidence_class: str = "UNKNOWN"

    def __post_init__(self) -> None:
        if not isinstance(self.engram_id, str) or not self.engram_id.strip():
            raise ValueError("engram_id must be a non-empty string")
        object.__setattr__(self, "session_file", normalize_session_file(self.session_file))
        if not isinstance(self.content, str) or not self.content.strip():
            raise ValueError("successful Harness content must be non-empty")
        if self.stop_reason != "stop":
            raise ValueError("successful Harness turns must have stop_reason='stop'")
        for name in (
            "input_tokens",
            "output_tokens",
            "cached_tokens",
            "cache_write_tokens",
            "tool_calls",
            "provider_requests",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative int")
        if not isinstance(self.evidence_class, str) or not self.evidence_class.strip():
            raise ValueError("evidence_class must be a non-empty string")
        object.__setattr__(self, "trace", list(self.trace))


@runtime_checkable
class HarnessSession(Protocol):
    """Persistent single-Engram lifecycle, separate from ``AgentBackend``."""

    @property
    def state(self) -> HarnessState:
        ...

    def run_turn(
        self,
        prompt: str,
        *,
        timeout_sec: float | None = None,
        bootstrap_text: str | None = None,
        turn_id: str | None = None,
    ) -> HarnessTurnResult:
        ...

    def snapshot(self) -> dict[str, Any]:
        ...

    def abort(self) -> None:
        ...

    def steer(self, content: str) -> None:
        ...

    def close(self) -> None:
        ...


@runtime_checkable
class HarnessRuntime(Protocol):
    """World-level owner of persistent per-Engram Harness sessions."""

    def preflight(self) -> None:
        ...

    def run_turn(
        self,
        engram_id: str,
        prompt: str,
        *,
        timeout_sec: float | None = None,
        bootstrap_text: str | None = None,
        turn_id: str | None = None,
    ) -> HarnessTurnResult:
        ...

    def snapshot(self, engram_id: str) -> dict[str, Any]:
        ...

    def abort(self, engram_id: str) -> None:
        ...

    def steer(self, engram_id: str, content: str) -> None:
        ...

    def succeed(
        self,
        old_engram_id: str,
        new_engram_id: str,
        *,
        capacity_timeout_sec: float | None = None,
    ) -> None:
        ...

    def close_session(self, engram_id: str) -> None:
        ...

    def close(self) -> None:
        ...

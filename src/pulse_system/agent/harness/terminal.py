"""Policy-aware bounded terminal process contracts.

This module deliberately does not ship a subprocess implementation.  The
default backend is an explicit unsupported adapter so a caller cannot mistake
an absent OS execution surface for a successful command.  A future adapter
must implement ProcessBackend, receive an explicit policy decision, and
provide its own evidence class.
"""

from __future__ import annotations

import hashlib
import math
import os
import re
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from .security import (
    CONTRACT_ONLY,
    PolicyDecision,
    resolve_policy_decision,
)

__all__ = [
    "BackendProcessState",
    "BackendSnapshot",
    "CleanupResult",
    "CommandProgress",
    "CONTRACT_ONLY",
    "InteractionResult",
    "PolicyDecision",
    "ProcessBackend",
    "ProcessHandle",
    "ProcessResult",
    "ProcessSpec",
    "TerminalManager",
    "TerminalState",
    "TerminalValidationError",
    "UnsupportedProcessBackend",
    "resolve_policy_decision",
]


_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:")
_HANDLE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_DEFAULT_OUTPUT_CAP = 64 * 1024
_DEFAULT_STDIN_CAP = 16 * 1024
_DEFAULT_MAX_OUTPUT_CAP = 1024 * 1024
_DEFAULT_MAX_PROCESSES = 8
_DEFAULT_MAX_RETIRED_HANDLES = 4096
_DEFAULT_CONTROL_GRACE_SEC = 0.25
_MAX_OUTPUT_CHUNK = 16 * 1024
_POLL_INTERVAL_SEC = 0.01


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _digest_argv(argv: Sequence[str]) -> str:
    joined = "\x00".join(argv).encode("utf-8", errors="strict")
    return hashlib.sha256(joined).hexdigest()


def _non_empty(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TerminalValidationError(f"{field_name} must be a non-empty string")
    if "\x00" in value:
        raise TerminalValidationError(f"{field_name} must not contain NUL")
    return value


def _bounded_handle_id(value: Any) -> str:
    value = _non_empty(value, "handle_id")
    if not _HANDLE_ID.fullmatch(value):
        raise TerminalValidationError(
            "handle_id must be a bounded opaque identifier"
        )
    return value


class TerminalValidationError(ValueError):
    """An input cannot be safely represented as a process request."""


class TerminalState(StrEnum):
    """Observable process lifecycle states.

    EXITED is an observed OS/backend exit, not a successful exit.  The exit
    code must be inspected separately.  UNSUPPORTED and UNCERTAIN are
    intentionally terminal states without a fabricated exit code.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    EXITED = "EXITED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    KILL_REQUESTED = "KILL_REQUESTED"
    KILLED = "KILLED"
    UNCERTAIN = "UNCERTAIN"
    DENIED = "DENIED"
    UNSUPPORTED = "UNSUPPORTED"

    @property
    def is_terminal(self) -> bool:
        return self in {
            TerminalState.EXITED,
            TerminalState.FAILED,
            TerminalState.TIMED_OUT,
            TerminalState.CANCELLED,
            TerminalState.KILLED,
            TerminalState.UNCERTAIN,
            TerminalState.DENIED,
            TerminalState.UNSUPPORTED,
        }


class BackendProcessState(StrEnum):
    """Small adapter-facing state vocabulary."""

    RUNNING = "RUNNING"
    EXITED = "EXITED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class BackendSnapshot:
    """A poll result returned by a process adapter."""

    state: BackendProcessState
    exit_code: int | None = None
    error_code: str | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "state", BackendProcessState(self.state))
        if self.exit_code is not None and type(self.exit_code) is not int:
            raise TerminalValidationError("backend exit_code must be an int or null")
        if self.error_code is not None:
            _non_empty(self.error_code, "backend error_code")
        if self.detail is not None:
            _non_empty(self.detail, "backend detail")


@dataclass(frozen=True, slots=True)
class ProcessSpec:
    """A shell-free, bounded process request.

    argv is always an argument vector.  There is no shell option, no
    inherited environment, and no implicit workspace escape.
    """

    turn_id: str
    argv: Sequence[str]
    cwd: str = "."
    foreground: bool = True
    timeout_sec: float | None = None
    output_cap_bytes: int = _DEFAULT_OUTPUT_CAP
    stdin_cap_bytes: int = _DEFAULT_STDIN_CAP
    allow_stdin: bool = False
    env: Mapping[str, str] = field(default_factory=dict)
    request_id: str | None = None

    def __post_init__(self) -> None:
        _non_empty(self.turn_id, "turn_id")
        if isinstance(self.argv, (str, bytes)) or not isinstance(self.argv, Sequence):
            raise TerminalValidationError("argv must be a non-empty argument sequence")
        argv = tuple(self.argv)
        if not argv:
            raise TerminalValidationError("argv must be a non-empty argument sequence")
        for index, argument in enumerate(argv):
            _non_empty(argument, f"argv[{index}]")
        object.__setattr__(self, "argv", argv)

        cwd = os.fspath(self.cwd)
        _non_empty(cwd, "cwd")
        object.__setattr__(self, "cwd", cwd)

        if type(self.foreground) is not bool:
            raise TerminalValidationError("foreground must be a bool")
        if self.timeout_sec is not None and (
            type(self.timeout_sec) not in (int, float)
            or not math.isfinite(float(self.timeout_sec))
            or self.timeout_sec <= 0
        ):
            raise TerminalValidationError("timeout_sec must be a finite positive number")
        if type(self.output_cap_bytes) is not int or self.output_cap_bytes <= 0:
            raise TerminalValidationError("output_cap_bytes must be a positive int")
        if type(self.stdin_cap_bytes) is not int or self.stdin_cap_bytes <= 0:
            raise TerminalValidationError("stdin_cap_bytes must be a positive int")
        if type(self.allow_stdin) is not bool:
            raise TerminalValidationError("allow_stdin must be a bool")

        if not isinstance(self.env, Mapping):
            raise TerminalValidationError("env must be a mapping")
        normalized_env: dict[str, str] = {}
        for key, value in self.env.items():
            _non_empty(key, "environment key")
            _non_empty(value, f"environment value for {key!r}")
            normalized_env[key] = value
        object.__setattr__(self, "env", normalized_env)

        if self.request_id is not None:
            _non_empty(self.request_id, "request_id")


@dataclass(frozen=True, slots=True)
class CommandProgress:
    """A bounded, redaction-ready output item."""

    handle_id: str
    request_id: str
    turn_id: str
    seq: int
    stream: str
    text: str
    byte_count: int
    truncated: bool
    redacted: bool
    at: str
    evidence_class: str

    def to_wire(self) -> dict[str, Any]:
        return {
            "handle_id": self.handle_id,
            "request_id": self.request_id,
            "turn_id": self.turn_id,
            "seq": self.seq,
            "stream": self.stream,
            "text": self.text,
            "byte_count": self.byte_count,
            "truncated": self.truncated,
            "redacted": self.redacted,
            "at": self.at,
            "evidence_class": self.evidence_class,
        }


@dataclass(frozen=True, slots=True)
class ProcessHandle:
    """A safe process snapshot suitable for a control-plane response."""

    id: str
    request_id: str
    turn_id: str
    cwd_relative: str
    foreground: bool
    state: TerminalState
    started_at: str
    ended_at: str | None
    evidence_class: str
    output_bytes: int
    output_cap_bytes: int
    output_truncated: bool
    timeout_sec: float | None
    cleanup_attempted: bool
    cleanup_succeeded: bool | None
    command_digest: str
    error_code: str | None = None
    error_detail: str | None = None

    @property
    def handle_id(self) -> str:
        return self.id

    def __post_init__(self) -> None:
        _non_empty(self.id, "handle id")
        _non_empty(self.request_id, "request_id")
        _non_empty(self.turn_id, "turn_id")
        _non_empty(self.cwd_relative, "cwd_relative")
        object.__setattr__(self, "state", TerminalState(self.state))
        if type(self.foreground) is not bool:
            raise TerminalValidationError("handle foreground must be a bool")
        if type(self.output_bytes) is not int or self.output_bytes < 0:
            raise TerminalValidationError("handle output_bytes must be non-negative")
        if type(self.output_cap_bytes) is not int or self.output_cap_bytes <= 0:
            raise TerminalValidationError("handle output_cap_bytes must be positive")
        if self.output_bytes > self.output_cap_bytes:
            raise TerminalValidationError("handle output_bytes exceeds output cap")
        if type(self.output_truncated) is not bool:
            raise TerminalValidationError("handle output_truncated must be a bool")
        _non_empty(self.command_digest, "command_digest")
        _non_empty(self.started_at, "started_at")
        if self.ended_at is not None:
            _non_empty(self.ended_at, "ended_at")
        _non_empty(self.evidence_class, "evidence_class")
        if self.error_code is not None:
            _non_empty(self.error_code, "error_code")
        if self.error_detail is not None:
            _non_empty(self.error_detail, "error_detail")

    def to_wire(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "request_id": self.request_id,
            "turn_id": self.turn_id,
            "cwd_relative": self.cwd_relative,
            "foreground": self.foreground,
            "state": self.state.value,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "evidence_class": self.evidence_class,
            "output_bytes": self.output_bytes,
            "output_cap_bytes": self.output_cap_bytes,
            "output_truncated": self.output_truncated,
            "timeout_sec": self.timeout_sec,
            "cleanup_attempted": self.cleanup_attempted,
            "cleanup_succeeded": self.cleanup_succeeded,
            "command_digest": self.command_digest,
            "error_code": self.error_code,
            "error_detail": self.error_detail,
        }


@dataclass(frozen=True, slots=True)
class ProcessResult:
    """Terminal result plus bounded output and explicit evidence."""

    handle: ProcessHandle
    exit_code: int | None
    output: tuple[CommandProgress, ...] = ()
    timed_out: bool = False
    cancel_requested: bool = False
    uncertain_reason: str | None = None

    @property
    def state(self) -> TerminalState:
        return self.handle.state

    @property
    def evidence_class(self) -> str:
        return self.handle.evidence_class

    def to_wire(self) -> dict[str, Any]:
        return {
            "handle": self.handle.to_wire(),
            "exit_code": self.exit_code,
            "output": [item.to_wire() for item in self.output],
            "timed_out": self.timed_out,
            "cancel_requested": self.cancel_requested,
            "uncertain_reason": self.uncertain_reason,
        }


@dataclass(frozen=True, slots=True)
class InteractionResult:
    """Result of a bounded stdin write or other process interaction."""

    handle_id: str
    request_id: str
    accepted: bool
    state: TerminalState
    evidence_class: str
    error_code: str | None = None
    detail: str | None = None

    def to_wire(self) -> dict[str, Any]:
        return {
            "handle_id": self.handle_id,
            "request_id": self.request_id,
            "accepted": self.accepted,
            "state": self.state.value,
            "evidence_class": self.evidence_class,
            "error_code": self.error_code,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class CleanupResult:
    """Explicit cleanup evidence; it never implies process termination."""

    handle_id: str
    attempted: bool
    succeeded: bool
    state: TerminalState
    evidence_class: str
    error_code: str | None = None
    detail: str | None = None

    def to_wire(self) -> dict[str, Any]:
        return {
            "handle_id": self.handle_id,
            "attempted": self.attempted,
            "succeeded": self.succeeded,
            "state": self.state.value,
            "evidence_class": self.evidence_class,
            "error_code": self.error_code,
            "detail": self.detail,
        }


class ProcessBackend(Protocol):
    """Adapter boundary for a real OS or an explicit contract test backend."""

    supports_execution: bool
    evidence_class: str

    def spawn(
        self,
        spec: ProcessSpec,
        *,
        handle_id: str,
        on_output: Callable[[bytes, str], None],
    ) -> str:
        """Start one already-authorized process and return an adapter handle."""

    def poll(self, backend_id: str) -> BackendSnapshot:
        ...

    def write_stdin(self, backend_id: str, data: bytes) -> bool:
        ...

    def interrupt(self, backend_id: str) -> bool:
        ...

    def kill(self, backend_id: str) -> bool:
        ...

    def cleanup(self, backend_id: str) -> bool:
        ...


class UnsupportedProcessBackend:
    """The safe default: no OS process is ever created."""

    supports_execution = False
    evidence_class = CONTRACT_ONLY

    def spawn(
        self,
        spec: ProcessSpec,
        *,
        handle_id: str,
        on_output: Callable[[bytes, str], None],
    ) -> str:
        raise RuntimeError("unsupported_execution")

    def poll(self, backend_id: str) -> BackendSnapshot:
        return BackendSnapshot(
            BackendProcessState.UNKNOWN,
            error_code="unsupported_execution",
            detail="no OS process backend is connected",
        )

    def write_stdin(self, backend_id: str, data: bytes) -> bool:
        return False

    def interrupt(self, backend_id: str) -> bool:
        return False

    def kill(self, backend_id: str) -> bool:
        return False

    def cleanup(self, backend_id: str) -> bool:
        return False


@dataclass
class _ProcessRecord:
    spec: ProcessSpec
    handle_id: str
    request_id: str
    cwd_absolute: Path | None
    cwd_relative: str
    command_digest: str
    evidence_class: str
    state: TerminalState
    started_at: str
    started_mono: float
    backend_id: str | None = None
    ended_at: str | None = None
    exit_code: int | None = None
    error_code: str | None = None
    error_detail: str | None = None
    uncertain_reason: str | None = None
    action: str | None = None
    output: list[CommandProgress] = field(default_factory=list)
    output_bytes: int = 0
    output_truncated: bool = False
    cleanup_attempted: bool = False
    cleanup_succeeded: bool | None = None
    redaction_failed: bool = False


class TerminalManager:
    """Own bounded process records and fail-closed lifecycle transitions."""

    def __init__(
        self,
        workspace_root: str | os.PathLike[str],
        *,
        backend: ProcessBackend | None = None,
        policy_evaluator: Any = None,
        output_redactor: Callable[[bytes, str], bytes | str] | None = None,
        allowed_env_keys: Sequence[str] = (),
        max_processes: int = _DEFAULT_MAX_PROCESSES,
        max_retired_handle_ids: int = _DEFAULT_MAX_RETIRED_HANDLES,
        max_output_cap_bytes: int = _DEFAULT_MAX_OUTPUT_CAP,
        max_control_grace_sec: float = _DEFAULT_CONTROL_GRACE_SEC,
    ) -> None:
        root = Path(os.fspath(workspace_root)).expanduser()
        if not root.is_absolute():
            raise TerminalValidationError("workspace_root must be absolute")
        try:
            root = root.resolve(strict=True)
        except OSError as exc:
            raise TerminalValidationError("workspace_root must resolve") from exc
        if not root.is_dir():
            raise TerminalValidationError("workspace_root must be a directory")
        if type(max_processes) is not int or max_processes <= 0:
            raise TerminalValidationError("max_processes must be a positive int")
        if type(max_retired_handle_ids) is not int or max_retired_handle_ids <= 0:
            raise TerminalValidationError(
                "max_retired_handle_ids must be a positive int"
            )
        if type(max_output_cap_bytes) is not int or max_output_cap_bytes <= 0:
            raise TerminalValidationError(
                "max_output_cap_bytes must be a positive int"
            )
        if (
            type(max_control_grace_sec) not in (int, float)
            or not math.isfinite(float(max_control_grace_sec))
            or max_control_grace_sec < 0
        ):
            raise TerminalValidationError(
                "max_control_grace_sec must be a finite non-negative number"
            )

        self._workspace_root = root
        self._backend = backend or UnsupportedProcessBackend()
        self._policy_evaluator = policy_evaluator
        self._output_redactor = output_redactor
        self._allowed_env_keys = frozenset(allowed_env_keys)
        self._max_processes = max_processes
        self._max_retired_handle_ids = max_retired_handle_ids
        self._max_output_cap_bytes = max_output_cap_bytes
        self._max_control_grace_sec = float(max_control_grace_sec)
        self._records: dict[str, _ProcessRecord] = {}
        self._retired_handle_ids: set[str] = set()
        self._retired_handle_order: deque[str] = deque()
        self._lock = threading.RLock()

    @property
    def workspace_root(self) -> Path:
        """Return the resolved root for adapter integration, never for wire data."""

        return self._workspace_root

    def backend_metadata(self) -> dict[str, Any]:
        """Return bounded capability evidence without exposing adapter handles.

        The terminal-session service uses these independent axes to avoid
        treating an ordinary pipe, a sandbox gate, and process-tree ownership
        as interchangeable evidence.  Missing adapter declarations stay
        conservative.
        """

        with self._lock:
            transport = str(getattr(self._backend, "transport", "pipe"))
            evidence_class = str(
                getattr(self._backend, "evidence_class", CONTRACT_ONLY)
            )
            sandbox_evidence = str(
                getattr(self._backend, "sandbox_evidence", evidence_class)
            )
            tree_containment = str(
                getattr(self._backend, "tree_containment", "UNVERIFIED")
            )
            return {
                "supports_execution": bool(self._backend.supports_execution),
                "transport": transport[:32],
                "evidence_class": evidence_class[:64],
                "sandbox_evidence": sandbox_evidence[:64],
                "tree_containment": tree_containment[:64],
                "kill_on_owner_death_verified": bool(
                    getattr(
                        self._backend,
                        "kill_on_owner_death_verified",
                        False,
                    )
                ),
            }

    def spawn(
        self,
        spec: ProcessSpec,
        policy_context: Any = None,
        *,
        handle_id: str | None = None,
    ) -> ProcessHandle:
        """Create a bounded handle or a safe denial/unsupported record."""

        if not isinstance(spec, ProcessSpec):
            raise TypeError("spawn requires ProcessSpec")
        handle_id = (
            uuid.uuid4().hex
            if handle_id is None
            else _bounded_handle_id(handle_id)
        )
        request_id = spec.request_id or uuid.uuid4().hex
        command_digest = _digest_argv(spec.argv)

        with self._lock:
            if handle_id in self._records or handle_id in self._retired_handle_ids:
                raise TerminalValidationError("handle_id is already retained")

        try:
            cwd_absolute, cwd_relative = self._resolve_cwd(spec.cwd)
        except TerminalValidationError as exc:
            return self._record_rejection(
                spec,
                handle_id=handle_id,
                request_id=request_id,
                state=TerminalState.DENIED,
                error_code="unsafe_cwd",
                detail=str(exc),
                cwd_relative="[REJECTED]",
                command_digest=command_digest,
            )

        if spec.output_cap_bytes > self._max_output_cap_bytes:
            return self._record_rejection(
                spec,
                handle_id=handle_id,
                request_id=request_id,
                state=TerminalState.DENIED,
                error_code="output_cap_exceeded",
                detail="requested output cap exceeds manager limit",
                cwd_relative=cwd_relative,
                command_digest=command_digest,
            )
        if any(key not in self._allowed_env_keys for key in spec.env):
            return self._record_rejection(
                spec,
                handle_id=handle_id,
                request_id=request_id,
                state=TerminalState.DENIED,
                error_code="env_not_allowlisted",
                detail="every environment key must be explicitly allowlisted",
                cwd_relative=cwd_relative,
                command_digest=command_digest,
            )

        request = {
            "action": "terminal.spawn",
            "operation": "terminal",
            "turn_id": spec.turn_id,
            "request_id": request_id,
            "cwd_relative": cwd_relative,
            "cwd": cwd_relative,
            "argv_digest": command_digest,
            "argv0": spec.argv[0],
            "command": spec.argv,
            "shell": False,
            "foreground": spec.foreground,
            "timeout_sec": spec.timeout_sec,
            "output_cap_bytes": spec.output_cap_bytes,
            "allow_stdin": spec.allow_stdin,
            "env_keys": sorted(spec.env),
        }
        decision = resolve_policy_decision(
            policy_context if policy_context is not None else self._policy_evaluator,
            request,
            action="terminal.spawn",
        )

        with self._lock:
            active = sum(
                self._holds_process_risk(record)
                for record in self._records.values()
            )
            if active >= self._max_processes:
                return self._record_rejection(
                    spec,
                    handle_id=handle_id,
                    request_id=request_id,
                    state=TerminalState.DENIED,
                    error_code="capacity_exhausted",
                    detail="terminal process capacity is exhausted",
                    cwd_relative=cwd_relative,
                    command_digest=command_digest,
                )
            if not decision.allow:
                return self._record_rejection(
                    spec,
                    handle_id=handle_id,
                    request_id=request_id,
                    state=TerminalState.DENIED,
                    error_code="policy_denied",
                    detail=decision.reason_code,
                    cwd_relative=cwd_relative,
                    command_digest=command_digest,
                    evidence_class=decision.evidence_class,
                )
            if not self._backend.supports_execution:
                return self._record_rejection(
                    spec,
                    handle_id=handle_id,
                    request_id=request_id,
                    state=TerminalState.UNSUPPORTED,
                    error_code="unsupported_execution",
                    detail="no OS process backend is connected",
                    cwd_relative=cwd_relative,
                    command_digest=command_digest,
                    evidence_class=CONTRACT_ONLY,
                )
            if self._output_redactor is None:
                return self._record_rejection(
                    spec,
                    handle_id=handle_id,
                    request_id=request_id,
                    state=TerminalState.DENIED,
                    error_code="output_redaction_required",
                    detail="supported execution requires an output redactor",
                    cwd_relative=cwd_relative,
                    command_digest=command_digest,
                    evidence_class=decision.evidence_class,
                )

            record = _ProcessRecord(
                spec=spec,
                handle_id=handle_id,
                request_id=request_id,
                cwd_absolute=cwd_absolute,
                cwd_relative=cwd_relative,
                command_digest=command_digest,
                evidence_class=str(
                    getattr(self._backend, "evidence_class", decision.evidence_class)
                ),
                state=TerminalState.PENDING,
                started_at=_utc_now(),
                started_mono=time.monotonic(),
            )
            self._records[handle_id] = record
            try:
                backend_id = self._backend.spawn(
                    spec,
                    handle_id=handle_id,
                    on_output=lambda data, stream="stdout": self._on_output(
                        handle_id, data, stream
                    ),
                )
                _non_empty(backend_id, "backend process id")
                record.backend_id = backend_id
                record.state = TerminalState.RUNNING
            except Exception as exc:
                record.state = TerminalState.UNCERTAIN
                record.ended_at = _utc_now()
                record.error_code = "spawn_uncertain"
                record.error_detail = (
                    f"{type(exc).__name__}: process start evidence is incomplete"
                )
                record.uncertain_reason = "adapter_spawn_failed_after_request"
                self._attempt_cleanup(record)
            return self._snapshot(record)

    def snapshot(self, handle_id: str) -> ProcessHandle:
        with self._lock:
            record = self._get_record(handle_id)
            self._poll_record(record)
            if self._timeout_expired(record):
                self._control_locked(
                    record,
                    operation="timeout",
                    reason="process_timeout",
                    deadline=self._max_control_grace_sec,
                )
            return self._snapshot(record)

    def inspect(self, handle_id: str) -> ProcessResult:
        """Poll one handle and return only bounded output plus safe metadata."""

        with self._lock:
            record = self._get_record(handle_id)
            self._poll_record(record)
            if self._timeout_expired(record):
                self._control_locked(
                    record,
                    operation="timeout",
                    reason="process_timeout",
                    deadline=self._max_control_grace_sec,
                )
            return self._result(record)

    def read_output(
        self,
        handle_id: str,
        *,
        after_seq: int = 0,
        limit: int = 100,
    ) -> tuple[CommandProgress, ...]:
        if type(after_seq) is not int or after_seq < 0:
            raise TerminalValidationError("after_seq must be a non-negative int")
        if type(limit) is not int or not 0 < limit <= 500:
            raise TerminalValidationError("limit must be between 1 and 500")
        with self._lock:
            record = self._get_record(handle_id)
            return tuple(
                item
                for item in record.output
                if item.seq > after_seq
            )[:limit]

    def write_stdin(self, handle_id: str, data: bytes) -> InteractionResult:
        if not isinstance(data, bytes):
            raise TerminalValidationError("stdin data must be bytes")
        with self._lock:
            record = self._get_record(handle_id)
            if record.state.is_terminal:
                return InteractionResult(
                    handle_id=handle_id,
                    request_id=record.request_id,
                    accepted=False,
                    state=record.state,
                    evidence_class=record.evidence_class,
                    error_code="process_terminal",
                    detail="stdin cannot be written after terminal state",
                )
            if not record.spec.allow_stdin:
                return InteractionResult(
                    handle_id=handle_id,
                    request_id=record.request_id,
                    accepted=False,
                    state=record.state,
                    evidence_class=record.evidence_class,
                    error_code="stdin_not_allowed",
                    detail="stdin was not enabled by the process spec",
                )
            if len(data) > record.spec.stdin_cap_bytes:
                return InteractionResult(
                    handle_id=handle_id,
                    request_id=record.request_id,
                    accepted=False,
                    state=record.state,
                    evidence_class=record.evidence_class,
                    error_code="stdin_cap_exceeded",
                    detail="stdin payload exceeds the per-write cap",
                )
            if record.backend_id is None:
                return InteractionResult(
                    handle_id=handle_id,
                    request_id=record.request_id,
                    accepted=False,
                    state=TerminalState.UNSUPPORTED,
                    evidence_class=record.evidence_class,
                    error_code="unsupported_execution",
                    detail="no backend handle exists",
                )
            try:
                accepted = bool(self._backend.write_stdin(record.backend_id, data))
            except Exception as exc:
                record.state = TerminalState.UNCERTAIN
                record.ended_at = _utc_now()
                record.error_code = "stdin_uncertain"
                record.error_detail = f"{type(exc).__name__}: stdin result is unknown"
                record.uncertain_reason = "adapter_stdin_failure"
                self._attempt_cleanup(record)
                accepted = False
            return InteractionResult(
                handle_id=handle_id,
                request_id=record.request_id,
                accepted=accepted,
                state=record.state,
                evidence_class=record.evidence_class,
                error_code=None if accepted else "stdin_rejected",
                detail=None if accepted else "backend rejected stdin",
            )

    def wait(
        self,
        handle_id: str,
        *,
        timeout_sec: float | None = None,
    ) -> ProcessResult:
        if timeout_sec is not None and (
            type(timeout_sec) not in (int, float)
            or not math.isfinite(float(timeout_sec))
            or timeout_sec < 0
        ):
            raise TerminalValidationError(
                "wait timeout_sec must be a finite non-negative number"
            )
        with self._lock:
            record = self._get_record(handle_id)
            if record.state.is_terminal:
                return self._result(record)
            if timeout_sec is None:
                timeout_sec = record.spec.timeout_sec
            deadline = (
                time.monotonic() + float(timeout_sec)
                if timeout_sec is not None
                else None
            )

        while True:
            with self._lock:
                record = self._get_record(handle_id)
                self._poll_record(record)
                if record.state.is_terminal:
                    return self._result(record)
                if deadline is not None and time.monotonic() >= deadline:
                    return self._control_locked(
                        record,
                        operation="timeout",
                        reason="wait_timeout",
                        deadline=self._max_control_grace_sec,
                    )
            time.sleep(_POLL_INTERVAL_SEC)

    def cancel(
        self,
        handle_id: str,
        *,
        reason: str = "user_cancel",
        deadline: float | None = None,
    ) -> ProcessResult:
        _non_empty(reason, "cancel reason")
        return self._control(
            handle_id,
            operation="cancel",
            reason=reason,
            deadline=deadline,
        )

    def kill(
        self,
        handle_id: str,
        *,
        reason: str = "user_kill",
        deadline: float | None = None,
    ) -> ProcessResult:
        _non_empty(reason, "kill reason")
        return self._control(
            handle_id,
            operation="kill",
            reason=reason,
            deadline=deadline,
        )

    def cleanup(self, handle_id: str) -> CleanupResult:
        with self._lock:
            record = self._get_record(handle_id)
            if record.backend_id is None:
                return CleanupResult(
                    handle_id=handle_id,
                    attempted=False,
                    succeeded=False,
                    state=record.state,
                    evidence_class=record.evidence_class,
                    error_code="unsupported_execution",
                    detail="no backend handle exists",
                )
            result = self._attempt_cleanup(record)
            if (
                result
                and not record.state.is_terminal
                and record.cleanup_succeeded
            ):
                record.state = TerminalState.UNCERTAIN
                record.ended_at = _utc_now()
                record.error_code = "cleanup_without_termination_evidence"
                record.error_detail = (
                    "backend cleanup succeeded without process termination evidence"
                )
                record.uncertain_reason = "cleanup_did_not_prove_exit"
            return CleanupResult(
                handle_id=handle_id,
                attempted=record.cleanup_attempted,
                succeeded=bool(record.cleanup_succeeded),
                state=record.state,
                evidence_class=record.evidence_class,
                error_code=None
                if record.cleanup_succeeded
                else "cleanup_failed",
                detail=None
                if record.cleanup_succeeded
                else record.error_detail,
            )

    def forget(self, handle_id: str) -> bool:
        """Release one terminal in-memory record after a durable owner settles it.

        Active records are never forgotten: losing their backend handle would
        turn bounded retention into an uncontrolled process.  Durable session
        storage calls this only after its terminal row and event commit.
        """

        with self._lock:
            record = self._get_record(handle_id)
            self._poll_record(record)
            if not record.state.is_terminal or record.state is TerminalState.UNCERTAIN:
                return False
            self._attempt_cleanup(record)
            if record.backend_id is not None and record.cleanup_succeeded is not True:
                return False
            del self._records[handle_id]
            self._retired_handle_ids.add(handle_id)
            self._retired_handle_order.append(handle_id)
            while len(self._retired_handle_order) > self._max_retired_handle_ids:
                retired = self._retired_handle_order.popleft()
                self._retired_handle_ids.discard(retired)
            return True

    def capacity_snapshot(self) -> dict[str, int]:
        with self._lock:
            active = sum(
                self._holds_process_risk(record)
                for record in self._records.values()
            )
            return {
                "max_processes": self._max_processes,
                "active_processes": active,
                "retained_records": len(self._records),
            }

    def close(self) -> tuple[ProcessResult, ...]:
        results: list[ProcessResult] = []
        with self._lock:
            handles = [
                record.handle_id
                for record in self._records.values()
                if self._holds_process_risk(record)
            ]
        for handle_id in handles:
            results.append(
                self.cancel(
                    handle_id,
                    reason="manager_close",
                    deadline=self._max_control_grace_sec,
                )
            )
        return tuple(results)

    @staticmethod
    def _holds_process_risk(record: _ProcessRecord) -> bool:
        return (
            not record.state.is_terminal
            or (
                record.state is TerminalState.UNCERTAIN
                and record.backend_id is not None
            )
        )

    def _resolve_cwd(self, cwd: str) -> tuple[Path, str]:
        normalized = cwd.replace("\\", "/")
        if (
            normalized.startswith("/")
            or normalized.startswith("//")
            or _WINDOWS_DRIVE.match(normalized)
            or os.path.isabs(cwd)
        ):
            raise TerminalValidationError("cwd must be workspace-relative")
        parts = normalized.split("/")
        if any(not part or part == ".." for part in parts):
            raise TerminalValidationError("cwd contains an empty or parent segment")
        if any(":" in part for part in parts):
            raise TerminalValidationError("cwd contains a forbidden colon")
        clean_parts = tuple(part for part in parts if part != ".")
        relative = "/".join(clean_parts) if clean_parts else "."
        candidate = self._workspace_root.joinpath(*clean_parts)
        try:
            resolved = candidate.resolve(strict=True)
        except OSError as exc:
            raise TerminalValidationError("cwd does not exist") from exc
        try:
            common = os.path.commonpath((str(self._workspace_root), str(resolved)))
        except ValueError as exc:
            raise TerminalValidationError("cwd is on a different volume") from exc
        if os.path.normcase(common) != os.path.normcase(str(self._workspace_root)):
            raise TerminalValidationError("cwd escapes workspace root")
        if not resolved.is_dir():
            raise TerminalValidationError("cwd must be a directory")
        return resolved, relative

    def _record_rejection(
        self,
        spec: ProcessSpec,
        *,
        handle_id: str,
        request_id: str,
        state: TerminalState,
        error_code: str,
        detail: str,
        cwd_relative: str,
        command_digest: str,
        evidence_class: str = CONTRACT_ONLY,
    ) -> ProcessHandle:
        now = _utc_now()
        record = _ProcessRecord(
            spec=spec,
            handle_id=handle_id,
            request_id=request_id,
            cwd_absolute=None,
            cwd_relative=cwd_relative,
            command_digest=command_digest,
            evidence_class=evidence_class,
            state=state,
            started_at=now,
            started_mono=time.monotonic(),
            ended_at=now,
            error_code=error_code,
            error_detail=detail,
        )
        with self._lock:
            self._records[handle_id] = record
            return self._snapshot(record)

    def _get_record(self, handle_id: str) -> _ProcessRecord:
        _non_empty(handle_id, "handle_id")
        try:
            return self._records[handle_id]
        except KeyError as exc:
            raise KeyError(f"unknown terminal handle: {handle_id}") from exc

    def _snapshot(self, record: _ProcessRecord) -> ProcessHandle:
        return ProcessHandle(
            id=record.handle_id,
            request_id=record.request_id,
            turn_id=record.spec.turn_id,
            cwd_relative=record.cwd_relative,
            foreground=record.spec.foreground,
            state=record.state,
            started_at=record.started_at,
            ended_at=record.ended_at,
            evidence_class=record.evidence_class,
            output_bytes=record.output_bytes,
            output_cap_bytes=record.spec.output_cap_bytes,
            output_truncated=record.output_truncated,
            timeout_sec=record.spec.timeout_sec,
            cleanup_attempted=record.cleanup_attempted,
            cleanup_succeeded=record.cleanup_succeeded,
            command_digest=record.command_digest,
            error_code=record.error_code,
            error_detail=record.error_detail,
        )

    def _result(self, record: _ProcessRecord) -> ProcessResult:
        return ProcessResult(
            handle=self._snapshot(record),
            exit_code=record.exit_code,
            output=tuple(record.output),
            timed_out=record.action == "timeout"
            or record.state is TerminalState.TIMED_OUT,
            cancel_requested=record.action in {"cancel", "timeout"},
            uncertain_reason=record.uncertain_reason,
        )

    def _on_output(
        self,
        handle_id: str,
        data: bytes,
        stream: str = "stdout",
    ) -> None:
        if not isinstance(data, bytes):
            data = str(data).encode("utf-8", errors="replace")
        stream = stream if stream in {"stdout", "stderr", "combined"} else "other"
        with self._lock:
            record = self._records.get(handle_id)
            if record is None:
                return
            if not data:
                return
            redacted = False
            safe_data = data
            if self._output_redactor is None:
                safe_data = b"[REDACTION_REQUIRED]"
                redacted = True
            else:
                try:
                    converted = self._output_redactor(data, stream)
                    safe_data = (
                        converted
                        if isinstance(converted, bytes)
                        else str(converted).encode("utf-8", errors="replace")
                    )
                    redacted = safe_data != data
                except Exception:
                    safe_data = b"[REDACTION_FAILED]"
                    redacted = True
                    record.redaction_failed = True

            remaining = record.spec.output_cap_bytes - record.output_bytes
            if remaining <= 0:
                record.output_truncated = True
                return
            if len(safe_data) > remaining:
                safe_data = safe_data[:remaining]
                record.output_truncated = True
            offset = 0
            while offset < len(safe_data):
                chunk = safe_data[offset : offset + _MAX_OUTPUT_CHUNK]
                record.output_bytes += len(chunk)
                record.output.append(
                    CommandProgress(
                        handle_id=record.handle_id,
                        request_id=record.request_id,
                        turn_id=record.spec.turn_id,
                        seq=len(record.output) + 1,
                        stream=stream,
                        text=chunk.decode("utf-8", errors="replace"),
                        byte_count=len(chunk),
                        truncated=record.output_truncated
                        and offset + len(chunk) >= len(safe_data),
                        redacted=redacted,
                        at=_utc_now(),
                        evidence_class=record.evidence_class,
                    )
                )
                offset += len(chunk)

    def _poll_record(self, record: _ProcessRecord) -> None:
        if record.state.is_terminal or record.backend_id is None:
            return
        try:
            snapshot = self._backend.poll(record.backend_id)
            snapshot = (
                snapshot
                if isinstance(snapshot, BackendSnapshot)
                else BackendSnapshot(**snapshot)
            )
        except Exception as exc:
            self._mark_uncertain(
                record,
                "poll_uncertain",
                f"{type(exc).__name__}: process state could not be confirmed",
            )
            return
        if snapshot.state is BackendProcessState.RUNNING:
            return
        if snapshot.state is BackendProcessState.UNKNOWN:
            self._mark_uncertain(
                record,
                snapshot.error_code or "process_state_unknown",
                snapshot.detail or "backend returned unknown process state",
            )
            return
        if snapshot.state is BackendProcessState.FAILED:
            record.state = TerminalState.FAILED
            record.exit_code = snapshot.exit_code
            record.error_code = snapshot.error_code or "process_failed"
            record.error_detail = snapshot.detail or "backend reported process failure"
            self._finalize(record)
            return
        record.exit_code = snapshot.exit_code
        if record.action == "timeout":
            record.state = TerminalState.TIMED_OUT
        elif record.action == "cancel":
            record.state = TerminalState.CANCELLED
        elif record.action == "kill":
            record.state = TerminalState.KILLED
        else:
            record.state = TerminalState.EXITED
        self._finalize(record)

    def _timeout_expired(self, record: _ProcessRecord) -> bool:
        return (
            not record.state.is_terminal
            and record.spec.timeout_sec is not None
            and time.monotonic() - record.started_mono >= record.spec.timeout_sec
        )

    def _control(
        self,
        handle_id: str,
        *,
        operation: str,
        reason: str,
        deadline: float | None,
    ) -> ProcessResult:
        with self._lock:
            record = self._get_record(handle_id)
            return self._control_locked(
                record,
                operation=operation,
                reason=reason,
                deadline=deadline,
            )

    def _control_locked(
        self,
        record: _ProcessRecord,
        *,
        operation: str,
        reason: str,
        deadline: float | None,
    ) -> ProcessResult:
        if record.state.is_terminal:
            return self._result(record)
        if deadline is None:
            deadline = self._max_control_grace_sec
        if (
            type(deadline) not in (int, float)
            or not math.isfinite(float(deadline))
            or deadline < 0
        ):
            raise TerminalValidationError(
                "control deadline must be a finite non-negative number"
            )
        if record.backend_id is None:
            self._mark_uncertain(
                record,
                "unsupported_execution",
                "control request has no backend process handle",
            )
            return self._result(record)

        record.action = "timeout" if operation == "timeout" else operation
        record.state = (
            TerminalState.CANCEL_REQUESTED
            if operation in {"cancel", "timeout"}
            else TerminalState.KILL_REQUESTED
        )
        signal_sent = False
        try:
            signal_sent = (
                bool(self._backend.interrupt(record.backend_id))
                if operation in {"cancel", "timeout"}
                else bool(self._backend.kill(record.backend_id))
            )
        except Exception as exc:
            record.error_code = "control_signal_failed"
            record.error_detail = f"{type(exc).__name__}: {reason}"

        if operation in {"cancel", "timeout"} and not signal_sent:
            record.state = TerminalState.KILL_REQUESTED
            try:
                signal_sent = bool(self._backend.kill(record.backend_id))
            except Exception as exc:
                record.error_code = "kill_signal_failed"
                record.error_detail = f"{type(exc).__name__}: {reason}"

        deadline_mono = time.monotonic() + float(deadline)
        while True:
            self._poll_record(record)
            if record.state.is_terminal:
                return self._result(record)
            if time.monotonic() >= deadline_mono:
                break
            time.sleep(_POLL_INTERVAL_SEC)

        if record.state.is_terminal:
            return self._result(record)
        if operation in {"cancel", "timeout"}:
            record.state = TerminalState.KILL_REQUESTED
            hard_signal_sent = False
            try:
                hard_signal_sent = bool(self._backend.kill(record.backend_id))
            except Exception as exc:
                record.error_code = "kill_signal_failed"
                record.error_detail = f"{type(exc).__name__}: {reason}"
            hard_deadline = time.monotonic() + float(deadline)
            while hard_signal_sent:
                self._poll_record(record)
                if record.state.is_terminal:
                    return self._result(record)
                if time.monotonic() >= hard_deadline:
                    break
                time.sleep(_POLL_INTERVAL_SEC)
        self._mark_uncertain(
            record,
            "termination_barrier_missing",
            "soft interrupt/kill did not produce terminal process evidence",
        )
        return self._result(record)

    def _mark_uncertain(
        self,
        record: _ProcessRecord,
        error_code: str,
        detail: str,
    ) -> None:
        if record.state.is_terminal:
            return
        record.state = TerminalState.UNCERTAIN
        record.ended_at = _utc_now()
        record.error_code = error_code
        record.error_detail = detail
        record.uncertain_reason = error_code
        self._attempt_cleanup(record)

    def _finalize(self, record: _ProcessRecord) -> None:
        if record.ended_at is None:
            record.ended_at = _utc_now()
        self._attempt_cleanup(record)

    def _attempt_cleanup(self, record: _ProcessRecord) -> bool:
        if record.cleanup_attempted:
            return bool(record.cleanup_succeeded)
        if record.backend_id is None:
            return False
        record.cleanup_attempted = True
        try:
            record.cleanup_succeeded = bool(
                self._backend.cleanup(record.backend_id)
            )
        except Exception as exc:
            record.cleanup_succeeded = False
            if record.error_detail is None:
                record.error_code = "cleanup_failed"
                record.error_detail = (
                    f"{type(exc).__name__}: backend cleanup failed"
                )
        return bool(record.cleanup_succeeded)

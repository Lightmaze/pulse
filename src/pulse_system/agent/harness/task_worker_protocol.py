"""Contracts for bounded, temporary Harness task workers.

This module deliberately contains no world, Engram, or persistence code.  A
task worker is a short-lived child of one Harness turn.  The parent context is
an authorization boundary, not a new subject identity, and the worker never
gets a durable lineage of its own.

The default backend supplied by :mod:`task_subagents` is unavailable.  The
``ContractOnlyTaskWorkerBackend`` below is an explicit protocol fixture: it
can exercise lifecycle and control wiring without pretending to run a model,
provider, Pi process, or operating-system sandbox.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping as MappingABC
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from math import isfinite
from typing import Any, Callable, Mapping, Protocol, runtime_checkable

__all__ = [
    "ActivityEmitter",
    "ContractOnlyTaskWorkerBackend",
    "TaskSubagentActivity",
    "TaskSubagentHandle",
    "TaskSubagentParentContext",
    "TaskSubagentSnapshot",
    "TaskSubagentSpec",
    "TaskSubagentState",
    "TaskSubagentWaitResult",
    "TaskWorkerBackend",
    "TaskWorkerCloseObservation",
    "TaskWorkerCloseSummary",
    "TaskWorkerControlResult",
    "TaskWorkerEvidence",
    "TaskWorkerProcessTreeState",
    "TaskWorkerStartResult",
    "TaskSubagentControlResult",
    "UnavailableTaskWorkerBackend",
    "utc_now",
    "value_digest",
]


def utc_now() -> datetime:
    """Return an aware UTC timestamp suitable for worker contracts."""

    return datetime.now(timezone.utc)


def value_digest(value: str) -> str:
    """Return a short non-reversible identifier for an untrusted value."""

    if not isinstance(value, str):
        raise TypeError("value_digest expects a string")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


class TaskSubagentState(StrEnum):
    """Observable lifecycle of a temporary worker.

    ``NOT_FOUND`` is a read/control response state, not a stored worker.  It
    exists so callers do not need to special-case a second response shape.
    """

    PENDING_INIT = "PENDING_INIT"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    INTERRUPTED = "INTERRUPTED"
    COMPLETED = "COMPLETED"
    ERRORED = "ERRORED"
    SHUTDOWN = "SHUTDOWN"
    UNCERTAIN = "UNCERTAIN"
    NOT_FOUND = "NOT_FOUND"


class TaskWorkerEvidence(StrEnum):
    """Evidence classes understood by the control plane."""

    CONTRACT_ONLY = "CONTRACT_ONLY"
    LIVE_GATE_UNVERIFIED = "LIVE_GATE_UNVERIFIED"
    LIVE_PI_PROVIDER = "LIVE_PI_PROVIDER"


class TaskWorkerProcessTreeState(StrEnum):
    """Per-close process-tree evidence, independent from worker state."""

    NOT_APPLICABLE = "not_applicable"
    EMPTY_VERIFIED = "empty_verified"
    ROOT_EXIT_ONLY = "root_exit_only"
    UNKNOWN = "unknown"


TERMINAL_TASK_STATES = frozenset(
    {
        TaskSubagentState.INTERRUPTED,
        TaskSubagentState.COMPLETED,
        TaskSubagentState.ERRORED,
        TaskSubagentState.SHUTDOWN,
        TaskSubagentState.UNCERTAIN,
    }
)


@dataclass(frozen=True, slots=True)
class TaskSubagentParentContext:
    """The scope a caller must prove before it can control a worker."""

    world_id: str
    engram_id: str
    turn_id: str
    epoch: int
    capabilities: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        for name in ("world_id", "engram_id", "turn_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
            object.__setattr__(self, name, value.strip())
        if type(self.epoch) is not int or self.epoch < 0:
            raise ValueError("epoch must be a non-negative int")
        capabilities = frozenset(self.capabilities)
        if any(not isinstance(value, str) or not value.strip() for value in capabilities):
            raise ValueError("capabilities must contain non-empty strings")
        object.__setattr__(self, "capabilities", frozenset(value.strip() for value in capabilities))


@dataclass(frozen=True, slots=True)
class TaskSubagentSpec:
    """Bounded input for one temporary worker.

    ``task`` is passed to an explicitly selected backend during ``spawn`` but
    is never copied into a handle, snapshot, or activity record.  The service
    only retains its digest and size for audit-safe inspection.
    """

    task: str
    capabilities: frozenset[str] = field(default_factory=frozenset)
    timeout_sec: float | None = None
    idle_timeout_sec: float | None = None
    deadline_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.task, str) or not self.task.strip():
            raise ValueError("task must be a non-empty string")
        object.__setattr__(self, "task", self.task)
        capabilities = frozenset(self.capabilities)
        if any(not isinstance(value, str) or not value.strip() for value in capabilities):
            raise ValueError("capabilities must contain non-empty strings")
        object.__setattr__(self, "capabilities", frozenset(value.strip() for value in capabilities))
        for name in ("timeout_sec", "idle_timeout_sec"):
            value = getattr(self, name)
            if value is not None and (
                type(value) not in (int, float) or not isfinite(float(value)) or value <= 0
            ):
                raise ValueError(f"{name} must be a finite positive number")
        if self.deadline_at is not None:
            if not isinstance(self.deadline_at, datetime) or self.deadline_at.tzinfo is None:
                raise ValueError("deadline_at must be an aware datetime")
            object.__setattr__(
                self,
                "deadline_at",
                self.deadline_at.astimezone(timezone.utc),
            )


@dataclass(frozen=True, slots=True)
class TaskSubagentHandle:
    """Stable, non-Engram identity returned by ``spawn``."""

    task_id: str
    parent_turn_id: str
    world_id: str
    engram_id: str
    epoch: int
    state: TaskSubagentState
    capability_scope: frozenset[str]
    evidence_class: TaskWorkerEvidence
    created_at: datetime
    deadline_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "parent_turn_id": self.parent_turn_id,
            "world_id": self.world_id,
            "engram_id": self.engram_id,
            "epoch": self.epoch,
            "state": self.state.value,
            "capability_scope": sorted(self.capability_scope),
            "evidence_class": self.evidence_class.value,
            "created_at": self.created_at.isoformat(),
            "deadline_at": self.deadline_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class TaskSubagentActivity:
    """A bounded, safe activity projection for one worker."""

    task_id: str
    seq: int
    kind: str
    state: TaskSubagentState
    occurred_at: datetime
    summary: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    evidence_class: TaskWorkerEvidence = TaskWorkerEvidence.CONTRACT_ONLY
    redacted: bool = False
    truncated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "subagent_activity",
            "task_id": self.task_id,
            "seq": self.seq,
            "activity_kind": self.kind,
            "state": self.state.value,
            "occurred_at": self.occurred_at.isoformat(),
            "summary": self.summary,
            "payload": dict(self.payload),
            "evidence_class": self.evidence_class.value,
            "redacted": self.redacted,
            "truncated": self.truncated,
        }


@dataclass(frozen=True, slots=True)
class TaskSubagentWaitResult:
    """Finite observation window result; timeout never means worker success."""

    task_id: str
    state: TaskSubagentState
    activities: tuple[TaskSubagentActivity, ...]
    next_seq: int
    terminal: bool
    timed_out: bool = False
    gap: bool = False
    evidence_class: TaskWorkerEvidence = TaskWorkerEvidence.CONTRACT_ONLY

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "state": self.state.value,
            "activities": [activity.to_dict() for activity in self.activities],
            "next_seq": self.next_seq,
            "terminal": self.terminal,
            "timed_out": self.timed_out,
            "gap": self.gap,
            "evidence_class": self.evidence_class.value,
        }


@dataclass(frozen=True, slots=True)
class TaskSubagentSnapshot:
    """Safe inspection result with no task prompt or raw worker output."""

    task_id: str
    parent_turn_id: str | None
    world_id: str | None
    engram_id: str | None
    epoch: int | None
    state: TaskSubagentState
    capability_scope: frozenset[str] = field(default_factory=frozenset)
    evidence_class: TaskWorkerEvidence = TaskWorkerEvidence.CONTRACT_ONLY
    created_at: datetime | None = None
    deadline_at: datetime | None = None
    last_activity_at: datetime | None = None
    activity_seq: int = 0
    first_available_seq: int = 1
    task_digest: str | None = None
    task_chars: int | None = None
    result_digest: str | None = None
    error_code: str | None = None
    gap: bool = False

    def to_dict(self) -> dict[str, Any]:
        def iso(value: datetime | None) -> str | None:
            return value.isoformat() if value is not None else None

        return {
            "task_id": self.task_id,
            "parent_turn_id": self.parent_turn_id,
            "world_id": self.world_id,
            "engram_id": self.engram_id,
            "epoch": self.epoch,
            "state": self.state.value,
            "capability_scope": sorted(self.capability_scope),
            "evidence_class": self.evidence_class.value,
            "created_at": iso(self.created_at),
            "deadline_at": iso(self.deadline_at),
            "last_activity_at": iso(self.last_activity_at),
            "activity_seq": self.activity_seq,
            "first_available_seq": self.first_available_seq,
            "task_digest": self.task_digest,
            "task_chars": self.task_chars,
            "result_digest": self.result_digest,
            "error_code": self.error_code,
            "gap": self.gap,
        }


@dataclass(frozen=True, slots=True)
class TaskWorkerStartResult:
    """Backend result for one synchronous worker start handshake."""

    state: TaskSubagentState = TaskSubagentState.PENDING_INIT
    backend_handle: Any = None
    evidence_class: TaskWorkerEvidence = TaskWorkerEvidence.CONTRACT_ONLY
    summary: str = "worker start accepted"
    error_code: str | None = None

    def __post_init__(self) -> None:
        state = TaskSubagentState(self.state)
        if state is TaskSubagentState.NOT_FOUND:
            raise ValueError("a backend cannot start a NOT_FOUND worker")
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "evidence_class", TaskWorkerEvidence(self.evidence_class))
        if not isinstance(self.summary, str):
            raise ValueError("summary must be a string")


@dataclass(frozen=True, slots=True)
class TaskWorkerControlResult:
    """Backend-side result for a steer or stop sideband operation."""

    accepted: bool
    terminal: bool = False
    state: TaskSubagentState = TaskSubagentState.RUNNING
    detail: str = ""
    error_code: str | None = None

    def __post_init__(self) -> None:
        state = TaskSubagentState(self.state)
        if state is TaskSubagentState.NOT_FOUND:
            raise ValueError("backend control cannot return NOT_FOUND")
        object.__setattr__(self, "state", state)
        if type(self.accepted) is not bool or type(self.terminal) is not bool:
            raise ValueError("accepted and terminal must be bools")


@dataclass(frozen=True, slots=True)
class TaskWorkerCloseObservation:
    """One backend owner's terminal observation under an absolute deadline."""

    task_id: str
    terminal: bool
    owner_joined: bool
    process_tree_state: TaskWorkerProcessTreeState
    state: TaskSubagentState
    error_code: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, str) or not self.task_id.strip():
            raise ValueError("task_id must be a non-empty string")
        if type(self.terminal) is not bool or type(self.owner_joined) is not bool:
            raise ValueError("terminal and owner_joined must be bools")
        object.__setattr__(
            self,
            "process_tree_state",
            TaskWorkerProcessTreeState(self.process_tree_state),
        )
        object.__setattr__(self, "state", TaskSubagentState(self.state))


@dataclass(frozen=True, slots=True)
class TaskWorkerCloseSummary(MappingABC[str, Any]):
    """Typed fleet evidence returned by TaskSubagent and Runtime bridges.

    The mapping surface preserves the existing Runtime adapter seam while the
    concrete type prevents a wrapper return from being mistaken for joined
    worker/process ownership.
    """

    active_before: int
    unresolved: int
    owner_joined: bool
    process_tree_state: TaskWorkerProcessTreeState
    cancellation_requested: int = 0
    terminal_observed: int = 0
    spawn_operations_settled_uncertain: int = 0
    reason: str = "runtime_shutdown"

    def __post_init__(self) -> None:
        for name in (
            "active_before",
            "unresolved",
            "cancellation_requested",
            "terminal_observed",
            "spawn_operations_settled_uncertain",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative int")
        if type(self.owner_joined) is not bool:
            raise ValueError("owner_joined must be a bool")
        if self.owner_joined and self.unresolved:
            raise ValueError("joined owners cannot have unresolved work")
        object.__setattr__(
            self,
            "process_tree_state",
            TaskWorkerProcessTreeState(self.process_tree_state),
        )
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("reason must be a non-empty string")

    @property
    def workers_observed(self) -> int:
        return self.active_before

    def to_dict(self) -> dict[str, Any]:
        return {
            "active_before": self.active_before,
            "workers_observed": self.active_before,
            "unresolved": self.unresolved,
            "owner_joined": self.owner_joined,
            "process_tree_state": self.process_tree_state.value,
            "cancellation_requested": self.cancellation_requested,
            "terminal_observed": self.terminal_observed,
            "spawn_operations_settled_uncertain": (
                self.spawn_operations_settled_uncertain
            ),
            "reason": self.reason,
        }

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())


@dataclass(frozen=True, slots=True)
class TaskSubagentControlResult:
    """Service-level, idempotent result for one control request."""

    task_id: str
    request_id: str
    operation: str
    accepted: bool
    state: TaskSubagentState
    idempotent: bool = False
    uncertain: bool = False
    detail: str = ""
    error_code: str | None = None
    evidence_class: TaskWorkerEvidence = TaskWorkerEvidence.CONTRACT_ONLY

    def __post_init__(self) -> None:
        for name in ("task_id", "request_id", "operation"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        object.__setattr__(self, "state", TaskSubagentState(self.state))
        object.__setattr__(self, "evidence_class", TaskWorkerEvidence(self.evidence_class))
        if type(self.accepted) is not bool or type(self.idempotent) is not bool:
            raise ValueError("accepted and idempotent must be bools")
        if type(self.uncertain) is not bool:
            raise ValueError("uncertain must be a bool")

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "request_id": self.request_id,
            "operation": self.operation,
            "accepted": self.accepted,
            "state": self.state.value,
            "idempotent": self.idempotent,
            "uncertain": self.uncertain,
            "detail": self.detail,
            "error_code": self.error_code,
            "evidence_class": self.evidence_class.value,
        }


ActivityEmitter = Callable[..., None]


@runtime_checkable
class TaskWorkerBackend(Protocol):
    """Minimal adapter needed by ``TaskSubagentService``.

    A backend must provide its own evidence label.  It may emit activities
    asynchronously through ``emit``; the service still caps and sanitizes
    every emitted item before exposing it.
    """

    evidence_class: TaskWorkerEvidence

    def start(
        self,
        task_id: str,
        spec: TaskSubagentSpec,
        parent_context: TaskSubagentParentContext,
        emit: ActivityEmitter,
    ) -> TaskWorkerStartResult:
        ...

    def steer(
        self,
        backend_handle: Any,
        message: str,
        emit: ActivityEmitter,
    ) -> TaskWorkerControlResult:
        ...

    def stop(
        self,
        backend_handle: Any,
        reason: str,
        emit: ActivityEmitter,
    ) -> TaskWorkerControlResult:
        ...

    def request_stop(
        self,
        backend_handle: Any,
        reason: str,
        emit: ActivityEmitter,
    ) -> TaskWorkerControlResult:
        """Broadcast cancellation without waiting for terminal ownership."""

        ...

    def observe_stop(
        self,
        backend_handle: Any,
        *,
        deadline: float,
    ) -> TaskWorkerCloseObservation:
        """Observe owner/process settlement against one absolute deadline."""

        ...

    def close(self, backend_handle: Any) -> None:
        ...


class UnavailableTaskWorkerBackend:
    """Safe default: explicitly unavailable, never a hidden local mock."""

    evidence_class = TaskWorkerEvidence.CONTRACT_ONLY

    def start(
        self,
        task_id: str,
        spec: TaskSubagentSpec,
        parent_context: TaskSubagentParentContext,
        emit: ActivityEmitter,
    ) -> TaskWorkerStartResult:
        del task_id, spec, parent_context, emit
        return TaskWorkerStartResult(
            state=TaskSubagentState.ERRORED,
            evidence_class=TaskWorkerEvidence.CONTRACT_ONLY,
            summary="no task worker backend is configured",
            error_code="worker_backend_unavailable",
        )

    def steer(
        self,
        backend_handle: Any,
        message: str,
        emit: ActivityEmitter,
    ) -> TaskWorkerControlResult:
        del backend_handle, message, emit
        return TaskWorkerControlResult(
            accepted=False,
            state=TaskSubagentState.ERRORED,
            detail="no task worker backend is configured",
            error_code="worker_backend_unavailable",
        )

    def stop(
        self,
        backend_handle: Any,
        reason: str,
        emit: ActivityEmitter,
    ) -> TaskWorkerControlResult:
        del backend_handle, reason, emit
        return TaskWorkerControlResult(
            accepted=False,
            state=TaskSubagentState.ERRORED,
            detail="no task worker backend is configured",
            error_code="worker_backend_unavailable",
        )

    def close(self, backend_handle: Any) -> None:
        del backend_handle

    def request_stop(
        self,
        backend_handle: Any,
        reason: str,
        emit: ActivityEmitter,
    ) -> TaskWorkerControlResult:
        return self.stop(backend_handle, reason, emit)

    def observe_stop(
        self,
        backend_handle: Any,
        *,
        deadline: float,
    ) -> TaskWorkerCloseObservation:
        del backend_handle, deadline
        return TaskWorkerCloseObservation(
            task_id="task_unavailable",
            terminal=True,
            owner_joined=True,
            process_tree_state=TaskWorkerProcessTreeState.NOT_APPLICABLE,
            state=TaskSubagentState.ERRORED,
            error_code="worker_backend_unavailable",
        )


class ContractOnlyTaskWorkerBackend:
    """Explicit lifecycle fixture; it produces no model/provider output.

    This adapter is intentionally useful for contract tests and local wiring
    checks only.  ``complete`` is a test/control action with a digest-only
    summary, not a claim that a real subagent ran.
    """

    evidence_class = TaskWorkerEvidence.CONTRACT_ONLY

    def __init__(self) -> None:
        self._sessions: dict[str, tuple[Any, ActivityEmitter]] = {}

    def start(
        self,
        task_id: str,
        spec: TaskSubagentSpec,
        parent_context: TaskSubagentParentContext,
        emit: ActivityEmitter,
    ) -> TaskWorkerStartResult:
        del spec, parent_context
        handle = object()
        self._sessions[task_id] = (handle, emit)
        return TaskWorkerStartResult(
            state=TaskSubagentState.RUNNING,
            backend_handle=handle,
            evidence_class=self.evidence_class,
            summary="contract-only worker started; no model/provider is attached",
        )

    def steer(
        self,
        backend_handle: Any,
        message: str,
        emit: ActivityEmitter,
    ) -> TaskWorkerControlResult:
        del backend_handle
        emit(
            "steer_accepted",
            "contract-only sideband steer accepted",
            {"message": message},
        )
        return TaskWorkerControlResult(
            accepted=True,
            state=TaskSubagentState.RUNNING,
            detail="contract-only sideband steer accepted",
        )

    def stop(
        self,
        backend_handle: Any,
        reason: str,
        emit: ActivityEmitter,
    ) -> TaskWorkerControlResult:
        del backend_handle
        emit(
            "stop_barrier",
            "contract-only stop barrier observed",
            {"reason": reason},
            state=TaskSubagentState.INTERRUPTED,
            terminal=True,
        )
        return TaskWorkerControlResult(
            accepted=True,
            terminal=True,
            state=TaskSubagentState.INTERRUPTED,
            detail="contract-only stop barrier observed",
        )

    def request_stop(
        self,
        backend_handle: Any,
        reason: str,
        emit: ActivityEmitter,
    ) -> TaskWorkerControlResult:
        return self.stop(backend_handle, reason, emit)

    def observe_stop(
        self,
        backend_handle: Any,
        *,
        deadline: float,
    ) -> TaskWorkerCloseObservation:
        del deadline
        task_id = next(
            (
                task_id
                for task_id, (handle, _emit) in self._sessions.items()
                if handle is backend_handle
            ),
            "task_contract_only",
        )
        return TaskWorkerCloseObservation(
            task_id=task_id,
            terminal=True,
            owner_joined=True,
            process_tree_state=TaskWorkerProcessTreeState.NOT_APPLICABLE,
            state=TaskSubagentState.INTERRUPTED,
        )

    def complete(self, task_id: str, *, summary: str = "contract-only completion") -> None:
        """Emit an explicit contract completion for tests or adapter checks."""

        session = self._sessions.get(task_id)
        if session is None:
            raise KeyError(task_id)
        _handle, emit = session
        emit(
            "worker_completed",
            summary,
            {"result_digest": value_digest(summary)},
            state=TaskSubagentState.COMPLETED,
            terminal=True,
        )

    def emit(
        self,
        task_id: str,
        kind: str,
        *,
        summary: str = "contract-only activity",
        payload: Mapping[str, Any] | None = None,
        state: TaskSubagentState | None = None,
        terminal: bool = False,
    ) -> None:
        """Emit a bounded-lifecycle fixture activity; never model output."""

        session = self._sessions.get(task_id)
        if session is None:
            raise KeyError(task_id)
        _handle, emit = session
        emit(
            kind,
            summary,
            payload or {},
            state=state,
            terminal=terminal,
        )

    def fail(self, task_id: str, *, error_code: str = "contract_failure") -> None:
        """Emit an explicit contract failure without fabricating output."""

        session = self._sessions.get(task_id)
        if session is None:
            raise KeyError(task_id)
        _handle, emit = session
        emit(
            "worker_failed",
            "contract-only worker failed",
            {"error_code": error_code},
            state=TaskSubagentState.ERRORED,
            terminal=True,
        )

    def close(self, backend_handle: Any) -> None:
        for task_id, (handle, _emit) in list(self._sessions.items()):
            if handle is backend_handle:
                self._sessions.pop(task_id, None)
                break

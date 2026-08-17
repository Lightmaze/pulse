"""Bounded temporary TaskSubagent control plane.

This module intentionally stops at the task-worker boundary.  The service owns
short-lived worker identity, authorization checks, quotas, deadlines and a
bounded in-memory activity projection.  It does not create Engrams, touch the
PulseWorld, write the causal ledger, or settle a Harness turn.

The service is useful before a real Pi/Codex-style worker adapter is wired in:
the default backend returns an explicit ``CONTRACT_ONLY`` failure, while the
separately named contract backend can exercise lifecycle transitions in tests.
Neither path manufactures model/provider output.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from math import isfinite
from typing import Any, Callable, Iterable, Mapping

from .task_worker_protocol import (
    TERMINAL_TASK_STATES,
    ActivityEmitter,
    TaskSubagentActivity,
    TaskSubagentControlResult,
    TaskSubagentHandle,
    TaskSubagentParentContext,
    TaskSubagentSnapshot,
    TaskSubagentSpec,
    TaskSubagentState,
    TaskSubagentWaitResult,
    TaskWorkerBackend,
    TaskWorkerCloseObservation,
    TaskWorkerCloseSummary,
    TaskWorkerControlResult,
    TaskWorkerEvidence,
    TaskWorkerProcessTreeState,
    TaskWorkerStartResult,
    UnavailableTaskWorkerBackend,
    utc_now,
    value_digest,
)

__all__ = [
    "TASK_INSPECT_CAPABILITY",
    "TASK_SPAWN_CAPABILITY",
    "TASK_STEER_CAPABILITY",
    "TASK_STOP_CAPABILITY",
    "TaskSubagentConfig",
    "TaskSubagentError",
    "TaskSubagentService",
]


TASK_INSPECT_CAPABILITY = "task:inspect"
TASK_SPAWN_CAPABILITY = "task:spawn"
TASK_STEER_CAPABILITY = "task:steer"
TASK_STOP_CAPABILITY = "task:stop"

_CONTROL_CAPABILITIES = {
    "steer": TASK_STEER_CAPABILITY,
    "stop": TASK_STOP_CAPABILITY,
}
_SENSITIVE_KEYS = {
    "api_key",
    "apikey",
    "authorization",
    "content",
    "credential",
    "input",
    "message",
    "password",
    "prompt",
    "reason",
    "result",
    "secret",
    "token",
}
_SAFE_PAYLOAD_KEYS = {
    "capability",
    "capabilities",
    "command_digest",
    "duration_ms",
    "error_code",
    "evidence_class",
    "exit_code",
    "idle_timeout_at",
    "message_chars",
    "message_digest",
    "output_chars",
    "output_digest",
    "path_digest",
    "payload_bytes",
    "result_digest",
    "state",
    "status",
    "task_chars",
    "task_digest",
    "timeout_kind",
    "worker_count",
}
_ABSOLUTE_PATH_RE = re.compile(
    r"(?i)(?:[a-z]:[\\/][^\s'\";]+|(?<![\w])/"
    r"(?:[^/\s'\";]+/)+[^/\s'\";]+)"
)
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(?:api[_-]?key|authorization|credential|password|secret|token)\b"
    r"\s*[:=]\s*[^\s,;]+"
)
_BEARER_RE = re.compile(r"(?i)\bbearer\s+[a-z0-9._~+/=-]+")
_OPENAI_KEY_RE = re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b")
_DIGEST_RE = re.compile(r"\A[a-fA-F0-9]{8,64}\Z")


def _safe_text(value: str, *, limit: int) -> tuple[str, bool]:
    """Scrub common credential/path forms and cap untrusted display text."""

    original = value if isinstance(value, str) else str(value)
    cleaned = _SECRET_ASSIGNMENT_RE.sub("[REDACTED]", original)
    cleaned = _BEARER_RE.sub("[REDACTED]", cleaned)
    cleaned = _OPENAI_KEY_RE.sub("[REDACTED]", cleaned)
    cleaned = _ABSOLUTE_PATH_RE.sub("[PATH]", cleaned)
    truncated = len(cleaned) > limit
    return cleaned[:limit], truncated


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _request_digest(spec: "TaskSubagentSpec") -> str:
    """Bind an explicit task id to the complete spawn request."""

    return _digest(
        json.dumps(
            {
                "task_digest": _digest(spec.task),
                "capabilities": sorted(spec.capabilities),
                "timeout_sec": spec.timeout_sec,
                "idle_timeout_sec": spec.idle_timeout_sec,
                "deadline_at": (
                    spec.deadline_at.astimezone(timezone.utc).isoformat()
                    if spec.deadline_at is not None
                    else None
                ),
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    )


@dataclass(frozen=True, slots=True)
class TaskSubagentConfig:
    """Hard limits for one in-memory worker service."""

    max_workers: int = 4
    max_workers_per_turn: int = 4
    max_retained_tasks: int = 256
    max_input_chars: int = 8192
    max_output_chars: int = 32768
    max_activity_events: int = 128
    max_activity_payload_bytes: int = 8192
    max_summary_chars: int = 256
    default_timeout_sec: float = 300.0
    max_timeout_sec: float = 900.0
    default_idle_timeout_sec: float = 60.0
    max_idle_timeout_sec: float = 300.0
    max_wait_sec: float = 30.0

    def __post_init__(self) -> None:
        for name in (
            "max_workers",
            "max_workers_per_turn",
            "max_retained_tasks",
            "max_input_chars",
            "max_output_chars",
            "max_activity_events",
            "max_activity_payload_bytes",
            "max_summary_chars",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive int")
        for name in (
            "default_timeout_sec",
            "max_timeout_sec",
            "default_idle_timeout_sec",
            "max_idle_timeout_sec",
            "max_wait_sec",
        ):
            value = getattr(self, name)
            if type(value) not in (int, float) or not isfinite(float(value)) or value <= 0:
                raise ValueError(f"{name} must be a finite positive number")
        if self.default_timeout_sec > self.max_timeout_sec:
            raise ValueError("default_timeout_sec cannot exceed max_timeout_sec")
        if self.default_idle_timeout_sec > self.max_idle_timeout_sec:
            raise ValueError("default_idle_timeout_sec cannot exceed max_idle_timeout_sec")


class TaskSubagentError(RuntimeError):
    """Classified, non-secret service error for spawn/observation callers."""

    def __init__(self, code: str, detail: str, *, status: int = 409) -> None:
        if not code or not detail:
            raise ValueError("TaskSubagentError requires code and detail")
        self.code = code
        self.detail = detail
        self.status = status
        super().__init__(f"{code}: {detail}")

    def to_dict(self) -> dict[str, Any]:
        return {"error": self.code, "detail": self.detail, "status": self.status}


@dataclass(slots=True)
class _WorkerRecord:
    task_id: str
    parent_context: TaskSubagentParentContext
    capability_scope: frozenset[str]
    task_digest: str
    request_digest: str
    task_chars: int
    state: TaskSubagentState
    evidence_class: TaskWorkerEvidence
    created_at: datetime
    deadline_at: datetime
    idle_timeout_sec: float
    last_activity_at: datetime
    backend_handle: Any = None
    activities: list[TaskSubagentActivity] = field(default_factory=list)
    next_seq: int = 1
    first_available_seq: int = 1
    gap: bool = False
    output_chars: int = 0
    result_digest: str | None = None
    error_code: str | None = None
    ended_at: datetime | None = None
    delivery_content: str | None = field(default=None, repr=False)
    io_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    termination_started: bool = field(default=False, repr=False)
    start_owner: threading.Thread | None = field(default=None, repr=False)
    shutdown_owner: threading.Thread | None = field(default=None, repr=False)
    shutdown_requested: bool = field(default=False, repr=False)
    close_activity_recorded: bool = field(default=False, repr=False)


class TaskSubagentService:
    """Thread-safe bounded registry and control surface for task workers."""

    def __init__(
        self,
        backend: TaskWorkerBackend | None = None,
        *,
        config: TaskSubagentConfig | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._config = config or TaskSubagentConfig()
        self._clock = clock
        self._backend: TaskWorkerBackend = backend or UnavailableTaskWorkerBackend()
        try:
            self._backend_evidence = TaskWorkerEvidence(self._backend.evidence_class)
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("backend must declare a supported evidence_class") from exc
        self._condition = threading.Condition(threading.RLock())
        self._records: dict[str, _WorkerRecord] = {}
        self._turn_spawn_counts: dict[tuple[str, str, str], int] = {}
        self._control_results: dict[str, TaskSubagentControlResult] = {}
        self._control_inflight: dict[str, tuple[str, str]] = {}
        self._closed = False
        self._close_done = threading.Event()
        self._close_broadcast_done = threading.Event()
        self._close_summary: TaskWorkerCloseSummary | None = None
        self._close_deadline_mono: float | None = None

    @property
    def config(self) -> TaskSubagentConfig:
        return self._config

    def spawn(
        self,
        spec: TaskSubagentSpec,
        parent_context: TaskSubagentParentContext,
        *,
        task_id: str | None = None,
    ) -> TaskSubagentHandle:
        """Reserve one bounded worker and perform its backend start handshake."""

        if not isinstance(spec, TaskSubagentSpec):
            raise TaskSubagentError("invalid_spec", "spawn requires TaskSubagentSpec", status=400)
        self._validate_parent_context(parent_context)
        now = self._now()
        self.sweep()
        explicit_task_id = task_id is not None
        if explicit_task_id and (
            not isinstance(task_id, str)
            or not task_id.startswith("task_")
            or len(task_id) > 96
            or not re.fullmatch(r"task_[A-Za-z0-9_.:-]+", task_id)
        ):
            raise TaskSubagentError(
                "invalid_task_id",
                "explicit task_id must be a bounded task_ identifier",
                status=400,
            )
        request_digest = _request_digest(spec)
        with self._condition:
            if self._closed:
                raise TaskSubagentError("service_closed", "task worker service is closed", status=503)
            self._prune_locked()
            # An exact retry must not consume capacity or turn quota and must
            # continue to work while unrelated workers occupy every slot.
            # A reused id with any request change is a collision, including a
            # changed deadline/timeout that used to be ignored here.
            if explicit_task_id and task_id in self._records:
                record = self._records[task_id]
                if (
                    record.parent_context == parent_context
                    and record.request_digest == request_digest
                ):
                    return self._handle_locked(record)
                raise TaskSubagentError(
                    "task_id_collision",
                    "explicit task_id is bound to another worker scope or request",
                    status=409,
                )
            if len(spec.task) > self._config.max_input_chars:
                raise TaskSubagentError(
                    "task_input_too_large",
                    "task input exceeds the configured worker cap",
                    status=413,
                )
            if TASK_SPAWN_CAPABILITY not in parent_context.capabilities:
                raise TaskSubagentError(
                    "task_capability_denied",
                    f"missing capability {TASK_SPAWN_CAPABILITY}",
                    status=403,
                )
            if not spec.capabilities.issubset(parent_context.capabilities):
                raise TaskSubagentError(
                    "task_capability_denied",
                    "requested worker capabilities exceed the parent scope",
                    status=403,
                )
            active = sum(
                record.state not in TERMINAL_TASK_STATES
                for record in self._records.values()
            )
            if active >= self._config.max_workers:
                raise TaskSubagentError(
                    "task_capacity_exhausted",
                    "temporary task worker capacity is exhausted",
                    status=429,
                )
            turn_key = (
                parent_context.world_id,
                parent_context.engram_id,
                parent_context.turn_id,
            )
            spawned = self._turn_spawn_counts.get(turn_key, 0)
            if spawned >= self._config.max_workers_per_turn:
                raise TaskSubagentError(
                    "task_turn_quota_exceeded",
                    "the parent Harness turn has exhausted its task worker quota",
                    status=429,
                )
            deadline_at = self._resolve_deadline(spec, now)
            idle_timeout_sec = self._resolve_idle_timeout(spec)
            if task_id is None:
                task_id = f"task_{uuid.uuid4().hex}"
            record = _WorkerRecord(
                task_id=task_id,
                parent_context=parent_context,
                capability_scope=spec.capabilities,
                task_digest=_digest(spec.task),
                request_digest=request_digest,
                task_chars=len(spec.task),
                state=TaskSubagentState.PENDING_INIT,
                evidence_class=self._backend_evidence,
                created_at=now,
                deadline_at=deadline_at,
                idle_timeout_sec=idle_timeout_sec,
                last_activity_at=now,
                start_owner=threading.current_thread(),
            )
            self._records[task_id] = record
            self._turn_spawn_counts[turn_key] = spawned + 1
            self._append_activity_locked(
                record,
                "spawn_requested",
                "temporary task worker reserved",
                {
                    "task_digest": record.task_digest,
                    "task_chars": record.task_chars,
                    "capabilities": sorted(record.capability_scope),
                },
            )
            emitter = self._emitter(task_id)

        # Backend startup is an execution owner.  It must never hold the
        # fleet condition, otherwise close cannot freeze admission or signal
        # workers already admitted behind a blocked starter.
        try:
            start_result = self._backend.start(
                task_id,
                spec,
                parent_context,
                emitter,
            )
            if not isinstance(start_result, TaskWorkerStartResult):
                raise TypeError("backend.start must return TaskWorkerStartResult")
            if start_result.evidence_class is not self._backend_evidence:
                raise ValueError(
                    "backend start evidence does not match backend evidence_class"
                )
        except Exception as exc:
            with self._condition:
                current = self._records.get(task_id)
                if current is record:
                    record.start_owner = None
                    if record.state not in TERMINAL_TASK_STATES:
                        record.state = TaskSubagentState.ERRORED
                        record.error_code = "worker_backend_start_failed"
                        record.ended_at = self._now()
                        self._append_activity_locked(
                            record,
                            "worker_failed",
                            "task worker backend start failed",
                            {"error_code": record.error_code},
                        )
                    self._condition.notify_all()
            raise TaskSubagentError(
                "worker_backend_start_failed",
                "task worker backend could not start the child",
                status=503,
            ) from exc

        late_shutdown = False
        close_terminal_handle = False
        with self._condition:
            current = self._records.get(task_id)
            if current is not record:
                record.start_owner = None
                late_shutdown = True
            else:
                record.start_owner = None
                record.backend_handle = start_result.backend_handle
                late_shutdown = self._closed or record.shutdown_requested
                if late_shutdown:
                    if record.state not in TERMINAL_TASK_STATES:
                        self._apply_backend_state_locked(
                            record,
                            TaskSubagentState.UNCERTAIN,
                        )
                        record.error_code = "worker_started_after_shutdown_fence"
                        record.ended_at = self._now()
                    if not record.close_activity_recorded:
                        self._append_activity_locked(
                            record,
                            "service_closed",
                            "task worker start crossed the shutdown fence",
                            {
                                "status": "uncertain",
                                "error_code": record.error_code,
                            },
                        )
                        record.close_activity_recorded = True
                else:
                    if record.state is TaskSubagentState.PENDING_INIT:
                        self._apply_backend_state_locked(record, start_result.state)
                    if start_result.error_code:
                        record.error_code = start_result.error_code
                    if record.state in TERMINAL_TASK_STATES:
                        record.ended_at = self._now()
                    self._append_activity_locked(
                        record,
                        (
                            "worker_started"
                            if record.state not in TERMINAL_TASK_STATES
                            else "worker_terminal"
                        ),
                        start_result.summary,
                        {
                            "status": (
                                "started"
                                if record.state not in TERMINAL_TASK_STATES
                                else "terminal"
                            ),
                            "error_code": start_result.error_code,
                            "evidence_class": record.evidence_class.value,
                        },
                    )
                    close_terminal_handle = record.state in TERMINAL_TASK_STATES
                if late_shutdown:
                    self._append_activity_locked(
                        record,
                        "shutdown_stop_broadcast",
                        "late worker handle received shutdown cancellation",
                        {"status": "requested"},
                    )
            handle = self._handle_locked(record)
            if current is record:
                self._condition.notify_all()
        if late_shutdown and start_result.backend_handle is not None:
            self._launch_late_start_stop(
                record,
                start_result.backend_handle,
            )
        elif close_terminal_handle and start_result.backend_handle is not None:
            if record.state is TaskSubagentState.COMPLETED:
                delivery = getattr(self._backend, "delivery_content", None)
                if callable(delivery):
                    try:
                        value = delivery(start_result.backend_handle)
                    except Exception:
                        value = None
                    if isinstance(value, str):
                        with self._condition:
                            record.delivery_content = value[
                                : self._config.max_output_chars
                            ]
            try:
                self._backend.close(start_result.backend_handle)
            except Exception:
                pass
            with self._condition:
                if (
                    self._records.get(task_id) is record
                    and record.backend_handle is start_result.backend_handle
                ):
                    record.backend_handle = None
        return handle

    def wait(
        self,
        task_id: str,
        after_seq: int = 0,
        timeout: float = 0.0,
        *,
        parent_context: TaskSubagentParentContext | None = None,
    ) -> TaskSubagentWaitResult:
        """Observe a finite activity window; timeout never completes a worker."""

        if not isinstance(task_id, str) or not task_id.strip():
            return self._not_found_wait(task_id)
        if type(after_seq) is not int or after_seq < 0:
            raise TaskSubagentError("invalid_cursor", "after_seq must be a non-negative int", status=400)
        timeout_value = self._validate_wait_timeout(timeout)
        deadline = self._now() + timedelta(seconds=timeout_value)
        with self._condition:
            record = self._records.get(task_id)
            if record is None or not self._scope_visible(record, parent_context):
                return self._not_found_wait(task_id)
        while True:
            self._expire_if_due(record, self._now())
            with self._condition:
                if self._records.get(task_id) is not record:
                    return self._not_found_wait(task_id)
                activities = tuple(
                    activity for activity in record.activities if activity.seq > after_seq
                )
                if activities or record.state in TERMINAL_TASK_STATES or timeout_value <= 0:
                    return self._wait_result_locked(record, activities, after_seq, timed_out=False)
                remaining = (deadline - self._now()).total_seconds()
                if remaining <= 0:
                    return self._wait_result_locked(record, (), after_seq, timed_out=True)
                self._condition.wait(timeout=remaining)

    def steer(
        self,
        task_id: str,
        message: str,
        request_id: str,
        expected_epoch: int,
        *,
        parent_context: TaskSubagentParentContext | None = None,
    ) -> TaskSubagentControlResult:
        """Send natural-language sideband input to a running worker."""

        if not isinstance(message, str) or not message.strip():
            return self._rejected_control(
                task_id,
                request_id,
                "steer",
                TaskSubagentState.NOT_FOUND,
                "invalid_message",
                "steer message must be non-empty",
            )
        if len(message) > self._config.max_input_chars:
            return self._rejected_control(
                task_id,
                request_id,
                "steer",
                TaskSubagentState.NOT_FOUND,
                "steer_input_too_large",
                "steer message exceeds the configured worker input cap",
            )
        return self._control(
            task_id,
            request_id,
            "steer",
            expected_epoch,
            parent_context,
            message=message,
        )

    def stop(
        self,
        task_id: str,
        reason: str,
        request_id: str,
        expected_epoch: int,
        *,
        parent_context: TaskSubagentParentContext | None = None,
    ) -> TaskSubagentControlResult:
        """Stop a worker and report uncertain state when no terminal barrier exists."""

        if not isinstance(reason, str) or not reason.strip():
            return self._rejected_control(
                task_id,
                request_id,
                "stop",
                TaskSubagentState.NOT_FOUND,
                "invalid_reason",
                "stop reason must be non-empty",
            )
        if len(reason) > self._config.max_input_chars:
            return self._rejected_control(
                task_id,
                request_id,
                "stop",
                TaskSubagentState.NOT_FOUND,
                "stop_reason_too_large",
                "stop reason exceeds the configured worker input cap",
            )
        return self._control(
            task_id,
            request_id,
            "stop",
            expected_epoch,
            parent_context,
            reason=reason,
        )

    def inspect(
        self,
        task_id: str,
        *,
        parent_context: TaskSubagentParentContext | None = None,
    ) -> TaskSubagentSnapshot:
        """Return a safe summary; unknown or out-of-scope workers stay opaque."""

        with self._condition:
            record = self._records.get(task_id)
            if record is None or not self._scope_visible(record, parent_context):
                return self._not_found_snapshot(task_id)
        self._expire_if_due(record, self._now())
        with self._condition:
            if self._records.get(task_id) is not record:
                return self._not_found_snapshot(task_id)
            return self._snapshot_locked(record)

    def delivery_content(
        self,
        task_id: str,
        *,
        parent_context: TaskSubagentParentContext,
    ) -> str | None:
        """Return ephemeral worker output to the exact parent scope."""

        with self._condition:
            record = self._records.get(task_id)
            if record is None or not self._scope_visible(record, parent_context):
                return None
            if record.state is not TaskSubagentState.COMPLETED:
                return None
            if isinstance(record.delivery_content, str):
                return record.delivery_content[: self._config.max_output_chars]
            delivery = getattr(self._backend, "delivery_content", None)
            if not callable(delivery):
                return None
            try:
                value = delivery(record.backend_handle)
            except Exception:
                return None
            if not isinstance(value, str):
                return None
            return value[: self._config.max_output_chars]

    def sweep(self) -> int:
        """Apply deadline/idle bounds to all live workers and return transitions."""

        with self._condition:
            records = tuple(self._records.values())
            before = sum(record.state not in TERMINAL_TASK_STATES for record in records)
        now = self._now()
        for record in records:
            self._expire_if_due(record, now)
        with self._condition:
            after = sum(
                record.state not in TERMINAL_TASK_STATES
                for record in self._records.values()
            )
        return before - after

    def capacity_snapshot(self) -> dict[str, Any]:
        """Return bounded fleet and activity counters for observation only."""

        self.sweep()
        with self._condition:
            active = sum(
                record.state not in TERMINAL_TASK_STATES
                for record in self._records.values()
            )
            terminal = len(self._records) - active
            activity_count = sum(len(record.activities) for record in self._records.values())
            return {
                "max_workers": self._config.max_workers,
                "active_workers": active,
                "available_workers": max(0, self._config.max_workers - active),
                "retained_workers": len(self._records),
                "terminal_workers": terminal,
                "activity_events": activity_count,
                "activity_event_cap": self._config.max_activity_events,
                "max_workers_per_turn": self._config.max_workers_per_turn,
                "evidence_class": self._backend_evidence.value,
                "live_gate": (
                    "LIVE_GATE_UNVERIFIED"
                    if self._backend_evidence is TaskWorkerEvidence.CONTRACT_ONLY
                    else "REQUIRED"
                ),
            }

    def close(
        self,
        *,
        deadline: float | None = None,
        reason: str = "runtime_shutdown",
    ) -> TaskWorkerCloseSummary:
        """Broadcast cancellation fleet-wide, then observe one shared deadline."""

        close_deadline = self._resolve_close_deadline(deadline)
        with self._condition:
            if self._close_summary is not None:
                return self._close_summary
            if self._closed:
                close_owner = False
                active_snapshot = tuple(
                    record
                    for record in self._records.values()
                    if record.shutdown_requested
                )
            else:
                close_owner = True
                self._closed = True
                self._close_deadline_mono = close_deadline
                active_snapshot = tuple(
                    record
                    for record in self._records.values()
                    if (
                        record.state not in TERMINAL_TASK_STATES
                        or (
                            record.start_owner is not None
                            and record.start_owner.is_alive()
                        )
                    )
                )
                for record in active_snapshot:
                    record.shutdown_requested = True
                    record.termination_started = True
                self._condition.notify_all()

        if not close_owner:
            self._close_done.wait(
                timeout=max(0.0, close_deadline - time.monotonic())
            )
            with self._condition:
                if self._close_summary is not None:
                    return self._close_summary
                unresolved = sum(
                    self._record_owner_alive(record)
                    for record in active_snapshot
                )
            return TaskWorkerCloseSummary(
                active_before=len(active_snapshot),
                unresolved=max(1, unresolved),
                owner_joined=False,
                process_tree_state=(
                    TaskWorkerProcessTreeState.UNKNOWN
                    if active_snapshot
                    else TaskWorkerProcessTreeState.NOT_APPLICABLE
                ),
                reason=reason,
            )

        request_threads: dict[str, threading.Thread] = {}
        request_started: dict[str, threading.Event] = {}
        request_results: dict[str, TaskWorkerControlResult] = {}
        observations: dict[str, TaskWorkerCloseObservation] = {}
        result_lock = threading.Lock()

        # Phase one: every admitted handle gets its own cancellation owner.
        # No request is joined before every owner has been started.
        for record in active_snapshot:
            backend_handle = record.backend_handle
            if backend_handle is None:
                continue
            started = threading.Event()
            thread = threading.Thread(
                target=self._run_close_request,
                args=(
                    record,
                    backend_handle,
                    reason,
                    started,
                    request_results,
                    result_lock,
                ),
                name=f"pulse-task-stop-{record.task_id[-12:]}",
                daemon=True,
            )
            request_started[record.task_id] = started
            request_threads[record.task_id] = thread
            thread.start()

        for started in request_started.values():
            started.wait(timeout=max(0.0, close_deadline - time.monotonic()))
        self._close_broadcast_done.set()

        # Phase two: observations run concurrently and consume only remaining
        # time from the same absolute deadline.
        observation_threads: dict[str, threading.Thread] = {}
        for record in active_snapshot:
            backend_handle = record.backend_handle
            request_thread = request_threads.get(record.task_id)
            if backend_handle is None or request_thread is None:
                continue
            observer = threading.Thread(
                target=self._run_close_observation,
                args=(
                    record,
                    backend_handle,
                    request_thread,
                    close_deadline,
                    observations,
                    result_lock,
                ),
                name=f"pulse-task-observe-{record.task_id[-12:]}",
                daemon=True,
            )
            with self._condition:
                record.shutdown_owner = observer
            observation_threads[record.task_id] = observer
            observer.start()

        for thread in observation_threads.values():
            if thread is threading.current_thread():
                continue
            thread.join(timeout=max(0.0, close_deadline - time.monotonic()))
        for thread in request_threads.values():
            if thread is threading.current_thread():
                continue
            thread.join(timeout=max(0.0, close_deadline - time.monotonic()))

        # A starter can publish a handle only through the shutdown fence.  Its
        # late-stop owner is tracked on the record and shares this deadline.
        for record in active_snapshot:
            with self._condition:
                start_owner = record.start_owner
                shutdown_owner = record.shutdown_owner
            for owner in (start_owner, shutdown_owner):
                if owner is None or owner is threading.current_thread():
                    continue
                owner.join(timeout=max(0.0, close_deadline - time.monotonic()))

        unresolved = 0
        terminal_observed = 0
        trees: list[TaskWorkerProcessTreeState] = []
        with self._condition:
            for record in active_snapshot:
                observation = observations.get(record.task_id)
                owners_alive = self._record_owner_alive(record)
                request_alive = (
                    request_threads.get(record.task_id) is not None
                    and request_threads[record.task_id].is_alive()
                )
                observe_alive = (
                    observation_threads.get(record.task_id) is not None
                    and observation_threads[record.task_id].is_alive()
                )
                owner_joined = (
                    not owners_alive
                    and not request_alive
                    and not observe_alive
                    and (
                        observation is not None
                        and observation.owner_joined
                        or record.backend_handle is None
                        and record.start_owner is None
                    )
                )
                if record.state not in TERMINAL_TASK_STATES:
                    terminal_state = (
                        observation.state
                        if owner_joined
                        and observation is not None
                        and observation.terminal
                        and observation.state in TERMINAL_TASK_STATES
                        else (
                            TaskSubagentState.SHUTDOWN
                            if owner_joined
                            else TaskSubagentState.UNCERTAIN
                        )
                    )
                    self._apply_backend_state_locked(
                        record,
                        terminal_state,
                    )
                    record.error_code = (
                        None if owner_joined else "worker_owner_exit_unproven"
                    )
                if not record.close_activity_recorded:
                    self._append_activity_locked(
                        record,
                        "service_closed",
                        "task worker service closed",
                        {
                            "status": "shutdown" if owner_joined else "uncertain",
                            "error_code": record.error_code,
                        },
                    )
                    record.close_activity_recorded = True
                record.termination_started = False
                if owner_joined:
                    record.backend_handle = None
                    terminal_observed += 1
                else:
                    unresolved += 1
                trees.append(
                    observation.process_tree_state
                    if observation is not None
                    else (
                        TaskWorkerProcessTreeState.NOT_APPLICABLE
                        if owner_joined
                        and record.evidence_class
                        is TaskWorkerEvidence.CONTRACT_ONLY
                        else TaskWorkerProcessTreeState.UNKNOWN
                    )
                )
            self._condition.notify_all()

        tree = self._aggregate_process_tree(active_snapshot, trees)
        summary = TaskWorkerCloseSummary(
            active_before=len(active_snapshot),
            unresolved=unresolved,
            owner_joined=unresolved == 0,
            process_tree_state=tree,
            cancellation_requested=sum(
                event.is_set() for event in request_started.values()
            ),
            terminal_observed=terminal_observed,
            reason=reason,
        )
        with self._condition:
            self._close_summary = summary
            self._close_done.set()
            self._condition.notify_all()
        return summary

    def wait_for_close_broadcast(self, *, deadline: float) -> bool:
        """Wait only for fleet-wide cancellation dispatch, never settlement."""

        close_deadline = self._resolve_close_deadline(deadline)
        return self._close_broadcast_done.wait(
            timeout=max(0.0, close_deadline - time.monotonic())
        )

    def _resolve_close_deadline(self, deadline: float | None) -> float:
        if deadline is None:
            return time.monotonic() + float(self._config.max_wait_sec)
        if (
            type(deadline) not in (int, float)
            or not isfinite(float(deadline))
        ):
            raise ValueError("deadline must be a finite monotonic timestamp")
        return float(deadline)

    @staticmethod
    def _record_owner_alive(record: _WorkerRecord) -> bool:
        return any(
            owner is not None and owner.is_alive()
            for owner in (record.start_owner, record.shutdown_owner)
        )

    def _run_close_request(
        self,
        record: _WorkerRecord,
        backend_handle: Any,
        reason: str,
        started: threading.Event,
        sink: dict[str, TaskWorkerControlResult],
        sink_lock: threading.Lock,
    ) -> None:
        started.set()
        try:
            request_stop = getattr(self._backend, "request_stop", None)
            result = (
                request_stop(backend_handle, reason, self._emitter(record.task_id))
                if callable(request_stop)
                else self._backend.stop(
                    backend_handle,
                    reason,
                    self._emitter(record.task_id),
                )
            )
            if not isinstance(result, TaskWorkerControlResult):
                raise TypeError("backend stop request must return TaskWorkerControlResult")
        except Exception:
            result = TaskWorkerControlResult(
                accepted=False,
                state=TaskSubagentState.UNCERTAIN,
                error_code="worker_stop_broadcast_failed",
            )
        with sink_lock:
            sink[record.task_id] = result

    def _run_close_observation(
        self,
        record: _WorkerRecord,
        backend_handle: Any,
        request_thread: threading.Thread,
        deadline: float,
        sink: dict[str, TaskWorkerCloseObservation],
        sink_lock: threading.Lock,
    ) -> None:
        try:
            observe_stop = getattr(self._backend, "observe_stop", None)
            if callable(observe_stop):
                observation = observe_stop(
                    backend_handle,
                    deadline=deadline,
                )
                if not isinstance(observation, TaskWorkerCloseObservation):
                    raise TypeError(
                        "backend observe_stop must return TaskWorkerCloseObservation"
                    )
            else:
                if request_thread is not threading.current_thread():
                    request_thread.join(
                        timeout=max(0.0, deadline - time.monotonic())
                    )
                with self._condition:
                    terminal = record.state in TERMINAL_TASK_STATES
                joined = terminal and not request_thread.is_alive()
                observation = TaskWorkerCloseObservation(
                    task_id=record.task_id,
                    terminal=terminal,
                    owner_joined=joined,
                    process_tree_state=(
                        TaskWorkerProcessTreeState.NOT_APPLICABLE
                        if joined
                        and record.evidence_class
                        is TaskWorkerEvidence.CONTRACT_ONLY
                        else TaskWorkerProcessTreeState.UNKNOWN
                    ),
                    state=(
                        record.state if terminal else TaskSubagentState.UNCERTAIN
                    ),
                    error_code=(
                        None if joined else "worker_owner_exit_unproven"
                    ),
                )
            if observation.owner_joined:
                self._backend.close(backend_handle)
        except Exception:
            observation = TaskWorkerCloseObservation(
                task_id=record.task_id,
                terminal=False,
                owner_joined=False,
                process_tree_state=TaskWorkerProcessTreeState.UNKNOWN,
                state=TaskSubagentState.UNCERTAIN,
                error_code="worker_close_observation_failed",
            )
        with sink_lock:
            sink[record.task_id] = observation

    def _launch_late_start_stop(
        self,
        record: _WorkerRecord,
        backend_handle: Any,
    ) -> None:
        with self._condition:
            deadline = self._close_deadline_mono or time.monotonic()
            existing = record.shutdown_owner
            if existing is not None and existing.is_alive():
                return

        def settle() -> None:
            started = threading.Event()
            results: dict[str, TaskWorkerControlResult] = {}
            observations: dict[str, TaskWorkerCloseObservation] = {}
            result_lock = threading.Lock()
            self._run_close_request(
                record,
                backend_handle,
                "runtime_shutdown",
                started,
                results,
                result_lock,
            )
            request_owner = threading.current_thread()
            self._run_close_observation(
                record,
                backend_handle,
                request_owner,
                deadline,
                observations,
                result_lock,
            )

        owner = threading.Thread(
            target=settle,
            name=f"pulse-task-late-stop-{record.task_id[-12:]}",
            daemon=True,
        )
        with self._condition:
            record.shutdown_owner = owner
        owner.start()

    @staticmethod
    def _aggregate_process_tree(
        records: tuple[_WorkerRecord, ...],
        trees: Iterable[TaskWorkerProcessTreeState],
    ) -> TaskWorkerProcessTreeState:
        values = tuple(trees)
        if not records:
            return TaskWorkerProcessTreeState.NOT_APPLICABLE
        if not values or TaskWorkerProcessTreeState.UNKNOWN in values:
            return TaskWorkerProcessTreeState.UNKNOWN
        if TaskWorkerProcessTreeState.ROOT_EXIT_ONLY in values:
            return TaskWorkerProcessTreeState.ROOT_EXIT_ONLY
        if TaskWorkerProcessTreeState.EMPTY_VERIFIED in values:
            return TaskWorkerProcessTreeState.EMPTY_VERIFIED
        return TaskWorkerProcessTreeState.NOT_APPLICABLE

    # ── Scope and lifecycle internals ────────────────────────────────

    def _now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise ValueError("task worker clock must return an aware datetime")
        return value.astimezone(timezone.utc)

    @staticmethod
    def _validate_parent_context(context: TaskSubagentParentContext) -> None:
        if not isinstance(context, TaskSubagentParentContext):
            raise TaskSubagentError(
                "invalid_parent_context",
                "parent_context must be TaskSubagentParentContext",
                status=400,
            )

    def _resolve_deadline(self, spec: TaskSubagentSpec, now: datetime) -> datetime:
        if spec.deadline_at is not None:
            deadline = spec.deadline_at.astimezone(timezone.utc)
            if deadline <= now:
                raise TaskSubagentError(
                    "task_deadline_expired",
                    "task deadline is already expired",
                    status=422,
                )
            if (deadline - now).total_seconds() > self._config.max_timeout_sec:
                raise TaskSubagentError(
                    "task_deadline_exceeds_cap",
                    "task deadline exceeds the configured worker cap",
                    status=422,
                )
            return deadline
        timeout_sec = spec.timeout_sec or self._config.default_timeout_sec
        if timeout_sec > self._config.max_timeout_sec:
            raise TaskSubagentError(
                "task_timeout_exceeds_cap",
                "task timeout exceeds the configured worker cap",
                status=422,
            )
        return now + timedelta(seconds=timeout_sec)

    def _resolve_idle_timeout(self, spec: TaskSubagentSpec) -> float:
        timeout_sec = spec.idle_timeout_sec or self._config.default_idle_timeout_sec
        if timeout_sec > self._config.max_idle_timeout_sec:
            raise TaskSubagentError(
                "task_idle_timeout_exceeds_cap",
                "task idle timeout exceeds the configured worker cap",
                status=422,
            )
        return float(timeout_sec)

    def _validate_wait_timeout(self, timeout: float) -> float:
        if type(timeout) not in (int, float) or not isfinite(float(timeout)) or timeout < 0:
            raise TaskSubagentError("invalid_wait_timeout", "timeout must be non-negative", status=400)
        if timeout > self._config.max_wait_sec:
            raise TaskSubagentError(
                "wait_timeout_exceeds_cap",
                "wait timeout exceeds the configured observation window",
                status=422,
            )
        return float(timeout)

    def _scope_visible(
        self,
        record: _WorkerRecord,
        parent_context: TaskSubagentParentContext | None,
    ) -> bool:
        if parent_context is None:
            return True
        if not isinstance(parent_context, TaskSubagentParentContext):
            return False
        return (
            parent_context.world_id == record.parent_context.world_id
            and parent_context.engram_id == record.parent_context.engram_id
            and parent_context.turn_id == record.parent_context.turn_id
            and parent_context.epoch == record.parent_context.epoch
        )

    def _scope_error(
        self,
        record: _WorkerRecord,
        parent_context: TaskSubagentParentContext | None,
        expected_epoch: int,
        operation: str,
    ) -> tuple[str, str] | None:
        if parent_context is None:
            return "scope_required", "parent scope is required for worker control"
        if not isinstance(parent_context, TaskSubagentParentContext):
            return "invalid_parent_context", "parent scope is invalid"
        if type(expected_epoch) is not int:
            return "invalid_expected_epoch", "expected_epoch must be an int"
        if expected_epoch != record.parent_context.epoch or parent_context.epoch != expected_epoch:
            return "epoch_stale", "worker control epoch is stale"
        if (
            parent_context.world_id != record.parent_context.world_id
            or parent_context.engram_id != record.parent_context.engram_id
            or parent_context.turn_id != record.parent_context.turn_id
        ):
            return "scope_mismatch", "worker control parent scope does not match"
        required = _CONTROL_CAPABILITIES[operation]
        if required not in parent_context.capabilities:
            return "task_capability_denied", f"missing capability {required}"
        return None

    def _control(
        self,
        task_id: str,
        request_id: str,
        operation: str,
        expected_epoch: int,
        parent_context: TaskSubagentParentContext | None,
        *,
        message: str | None = None,
        reason: str | None = None,
    ) -> TaskSubagentControlResult:
        if not isinstance(request_id, str) or not request_id.strip():
            return self._rejected_control(
                task_id,
                request_id,
                operation,
                TaskSubagentState.NOT_FOUND,
                "invalid_request_id",
                "request_id must be non-empty",
            )
        self._expire_task_if_due(task_id)
        wait_deadline = time.monotonic() + self._config.max_wait_sec
        with self._condition:
            while request_id in self._control_inflight:
                bound = self._control_inflight[request_id]
                if bound != (task_id, operation):
                    return TaskSubagentControlResult(
                        task_id=task_id or "unknown",
                        request_id=request_id,
                        operation=operation,
                        accepted=False,
                        state=TaskSubagentState.NOT_FOUND,
                        error_code="request_id_reuse",
                        detail="request_id is already bound to another worker operation",
                    )
                remaining = wait_deadline - time.monotonic()
                if remaining <= 0:
                    record = self._records.get(task_id)
                    return TaskSubagentControlResult(
                        task_id=task_id,
                        request_id=request_id,
                        operation=operation,
                        accepted=False,
                        state=(
                            TaskSubagentState.NOT_FOUND
                            if record is None
                            else record.state
                        ),
                        uncertain=True,
                        error_code="control_in_progress_timeout",
                        detail="the original worker control is still in progress",
                        evidence_class=(
                            TaskWorkerEvidence.CONTRACT_ONLY
                            if record is None
                            else record.evidence_class
                        ),
                    )
                self._condition.wait(timeout=remaining)

            previous = self._control_results.get(request_id)
            if previous is not None:
                if previous.task_id != task_id or previous.operation != operation:
                    return TaskSubagentControlResult(
                        task_id=task_id or "unknown",
                        request_id=request_id,
                        operation=operation,
                        accepted=False,
                        state=TaskSubagentState.NOT_FOUND,
                        error_code="request_id_reuse",
                        detail="request_id is already bound to another worker operation",
                    )
                return TaskSubagentControlResult(
                    task_id=previous.task_id,
                    request_id=previous.request_id,
                    operation=previous.operation,
                    accepted=previous.accepted,
                    state=previous.state,
                    idempotent=True,
                    uncertain=previous.uncertain,
                    detail=previous.detail,
                    error_code=previous.error_code,
                    evidence_class=previous.evidence_class,
                )
            record = self._records.get(task_id)
            if record is None:
                return self._remember_control(
                    self._rejected_control(
                        task_id,
                        request_id,
                        operation,
                        TaskSubagentState.NOT_FOUND,
                        "task_not_found",
                        "task worker was not found",
                    )
                )
            scope_error = self._scope_error(record, parent_context, expected_epoch, operation)
            if scope_error is not None:
                code, detail = scope_error
                return self._remember_control(
                    self._rejected_control(
                        task_id,
                        request_id,
                        operation,
                        TaskSubagentState.NOT_FOUND if code != "epoch_stale" else record.state,
                        code,
                        detail,
                        evidence_class=record.evidence_class,
                    )
                )
            if record.state in TERMINAL_TASK_STATES:
                return self._remember_control(
                    self._rejected_control(
                        task_id,
                        request_id,
                        operation,
                        record.state,
                        "task_terminal",
                        "task worker is already terminal",
                        evidence_class=record.evidence_class,
                    )
                )
            self._append_activity_locked(
                record,
                "control_requested",
                f"task {operation} requested",
                {
                    "status": "requested",
                    "message_digest": _digest(message) if message is not None else None,
                    "message_chars": len(message) if message is not None else None,
                    "timeout_kind": "sideband",
                },
            )
            self._control_inflight[request_id] = (task_id, operation)
            backend_handle = record.backend_handle
            initial_state = record.state

        # The worker adapter may block on an RPC/settlement barrier.  Serialize
        # controls for this worker only; unrelated worker inspection, waits,
        # events, and controls remain responsive on the service condition.
        try:
            with record.io_lock:
                with self._condition:
                    callable_now = (
                        not self._closed
                        and self._records.get(task_id) is record
                        and record.state not in TERMINAL_TASK_STATES
                        and record.backend_handle is backend_handle
                    )
                if not callable_now:
                    backend_result = TaskWorkerControlResult(
                        accepted=False,
                        state=record.state,
                        detail="task worker became terminal before control dispatch",
                        error_code="task_terminal",
                    )
                elif operation == "steer":
                    backend_result = self._backend.steer(
                        backend_handle,
                        message or "",
                        self._emitter(task_id),
                    )
                else:
                    backend_result = self._backend.stop(
                        backend_handle,
                        reason or "",
                        self._emitter(task_id),
                    )
                if not isinstance(backend_result, TaskWorkerControlResult):
                    raise TypeError("backend control must return TaskWorkerControlResult")
        except Exception:
            backend_result = TaskWorkerControlResult(
                accepted=False,
                state=initial_state,
                detail="task worker backend control failed",
                error_code="worker_backend_control_failed",
            )

        with self._condition:
            try:
                current = self._records.get(task_id)
                if current is not record:
                    result = TaskSubagentControlResult(
                        task_id=task_id,
                        request_id=request_id,
                        operation=operation,
                        accepted=False,
                        state=TaskSubagentState.NOT_FOUND,
                        uncertain=True,
                        detail="task worker record disappeared during control",
                        error_code="task_record_lost",
                        evidence_class=record.evidence_class,
                    )
                else:
                    if backend_result.accepted:
                        if record.shutdown_requested:
                            target_state = TaskSubagentState.UNCERTAIN
                        elif operation == "stop":
                            if backend_result.terminal:
                                target_state = (
                                    backend_result.state
                                    if backend_result.state in TERMINAL_TASK_STATES
                                    else TaskSubagentState.INTERRUPTED
                                )
                            else:
                                target_state = TaskSubagentState.UNCERTAIN
                        else:
                            target_state = backend_result.state
                        self._apply_backend_state_locked(record, target_state)
                        if target_state in TERMINAL_TASK_STATES:
                            record.ended_at = self._now()
                            self._close_backend_locked(record)
                    result = TaskSubagentControlResult(
                        task_id=task_id,
                        request_id=request_id,
                        operation=operation,
                        accepted=backend_result.accepted,
                        state=record.state,
                        uncertain=record.state is TaskSubagentState.UNCERTAIN,
                        detail=_safe_text(
                            backend_result.detail or "",
                            limit=self._config.max_summary_chars,
                        )[0],
                        error_code=backend_result.error_code,
                        evidence_class=record.evidence_class,
                    )
                    self._append_activity_locked(
                        record,
                        "control_resolved",
                        "task control resolved",
                        {
                            "status": "accepted" if result.accepted else "rejected",
                            "error_code": result.error_code,
                        },
                    )
                self._control_results[request_id] = result
                return result
            finally:
                self._control_inflight.pop(request_id, None)
                self._condition.notify_all()

    def _remember_control(self, result: TaskSubagentControlResult) -> TaskSubagentControlResult:
        self._control_results[result.request_id] = result
        return result

    def _rejected_control(
        self,
        task_id: str,
        request_id: str,
        operation: str,
        state: TaskSubagentState,
        error_code: str,
        detail: str,
        *,
        evidence_class: TaskWorkerEvidence = TaskWorkerEvidence.CONTRACT_ONLY,
    ) -> TaskSubagentControlResult:
        return TaskSubagentControlResult(
            task_id=task_id if isinstance(task_id, str) and task_id else "unknown",
            request_id=request_id if isinstance(request_id, str) and request_id else "invalid",
            operation=operation,
            accepted=False,
            state=state,
            error_code=error_code,
            detail=detail,
            evidence_class=evidence_class,
        )

    # ── Backend events and bounded projection ───────────────────────

    def _emitter(self, task_id: str) -> ActivityEmitter:
        def emit(
            kind: str,
            summary: str = "",
            payload: Mapping[str, Any] | None = None,
            *,
            state: TaskSubagentState | None = None,
            terminal: bool = False,
        ) -> None:
            with self._condition:
                record = self._records.get(task_id)
                if record is None:
                    return
                if record.state in TERMINAL_TASK_STATES:
                    return
                if record.shutdown_requested:
                    # close owns the terminal claim once cancellation has
                    # been broadcast.  A late worker completion may supply
                    # owner evidence, but it cannot replace that winner.
                    return
                # A configured real adapter is not provider-live merely by
                # existing.  It may upgrade the record only on a successful
                # terminal event whose settled Harness result carries the
                # live provider evidence.  Contract fixtures can never enter
                # this branch because they start as CONTRACT_ONLY.
                if (
                    terminal
                    and state is TaskSubagentState.COMPLETED
                    and record.evidence_class is TaskWorkerEvidence.LIVE_GATE_UNVERIFIED
                    and isinstance(payload, Mapping)
                    and payload.get("evidence_class")
                    == TaskWorkerEvidence.LIVE_PI_PROVIDER.value
                ):
                    record.evidence_class = TaskWorkerEvidence.LIVE_PI_PROVIDER
                if state is not None:
                    self._apply_backend_state_locked(record, state)
                elif terminal:
                    self._apply_backend_state_locked(record, TaskSubagentState.COMPLETED)
                if terminal and record.state not in TERMINAL_TASK_STATES:
                    self._apply_backend_state_locked(record, TaskSubagentState.UNCERTAIN)
                if isinstance(payload, Mapping) and payload:
                    result_digest = payload.get("result_digest")
                    if isinstance(result_digest, str) and result_digest:
                        record.result_digest = (
                            result_digest[:64]
                            if _DIGEST_RE.fullmatch(result_digest)
                            else _digest(result_digest)
                        )
                self._append_activity_locked(record, kind, summary, payload or {})
                if terminal or record.state in TERMINAL_TASK_STATES:
                    record.ended_at = self._now()
                    if (
                        record.state in TERMINAL_TASK_STATES
                        and not record.termination_started
                    ):
                        self._close_backend_locked(record)
                self._condition.notify_all()

        return emit

    def _apply_backend_state_locked(
        self,
        record: _WorkerRecord,
        state: TaskSubagentState,
    ) -> None:
        state = TaskSubagentState(state)
        if state is TaskSubagentState.NOT_FOUND:
            raise ValueError("backend cannot move a worker to NOT_FOUND")
        if record.state in TERMINAL_TASK_STATES:
            return
        allowed = {
            TaskSubagentState.PENDING_INIT: {
                TaskSubagentState.RUNNING,
                TaskSubagentState.WAITING,
                *TERMINAL_TASK_STATES,
            },
            TaskSubagentState.RUNNING: {
                TaskSubagentState.WAITING,
                *TERMINAL_TASK_STATES,
            },
            TaskSubagentState.WAITING: {
                TaskSubagentState.RUNNING,
                *TERMINAL_TASK_STATES,
            },
        }
        if state is record.state:
            return
        if state not in allowed.get(record.state, set()):
            raise ValueError(
                f"invalid task worker state transition {record.state.value}->{state.value}"
            )
        record.state = state
        if state in TERMINAL_TASK_STATES:
            record.ended_at = self._now()

    def _append_activity_locked(
        self,
        record: _WorkerRecord,
        kind: str,
        summary: str,
        payload: Mapping[str, Any],
    ) -> TaskSubagentActivity:
        safe_summary, summary_truncated = _safe_text(
            summary or "task worker activity",
            limit=self._config.max_summary_chars,
        )
        payload_for_safe = dict(payload) if isinstance(payload, Mapping) else {}
        raw_output_chars = sum(
            len(value)
            for key, value in payload_for_safe.items()
            if str(key).casefold() in {"content", "output", "result"}
            and isinstance(value, str)
        )
        output_cap_truncated = False
        if raw_output_chars:
            available = max(0, self._config.max_output_chars - record.output_chars)
            record.output_chars += min(raw_output_chars, available)
            if raw_output_chars > available:
                output_cap_truncated = True
                payload_for_safe = {
                    key: value
                    for key, value in payload_for_safe.items()
                    if str(key).casefold() not in {"content", "output", "result"}
                }
                payload_for_safe["output_chars"] = record.output_chars
                payload_for_safe["status"] = "output_cap_reached"
        safe_payload, payload_redacted, payload_truncated = self._safe_payload(payload_for_safe)
        activity = TaskSubagentActivity(
            task_id=record.task_id,
            seq=record.next_seq,
            kind=str(kind)[:64] or "activity",
            state=record.state,
            occurred_at=self._now(),
            summary=safe_summary,
            payload=safe_payload,
            evidence_class=record.evidence_class,
            redacted=payload_redacted,
            truncated=summary_truncated or payload_truncated or output_cap_truncated,
        )
        record.next_seq += 1
        record.last_activity_at = activity.occurred_at
        record.activities.append(activity)
        if len(record.activities) > self._config.max_activity_events:
            record.activities.pop(0)
            record.gap = True
            if record.activities:
                record.first_available_seq = record.activities[0].seq
            else:
                record.first_available_seq = record.next_seq
        elif record.activities:
            record.first_available_seq = record.activities[0].seq
        return activity

    def _safe_payload(
        self,
        payload: Mapping[str, Any],
    ) -> tuple[dict[str, Any], bool, bool]:
        safe: dict[str, Any] = {}
        redacted = False
        truncated = False
        if not isinstance(payload, Mapping):
            return {}, True, True
        for raw_key, raw_value in payload.items():
            key = str(raw_key)
            normalized = key.casefold().replace("-", "_")
            if raw_value is None:
                continue
            if normalized in _SENSITIVE_KEYS or normalized in {
                "output",
                "path",
                "args",
                "command",
            }:
                if isinstance(raw_value, str):
                    suffix = "path" if normalized == "path" else normalized
                    safe[f"{suffix}_digest"] = _digest(raw_value)
                    safe[f"{suffix}_chars"] = len(raw_value)
                redacted = True
                continue
            if normalized not in _SAFE_PAYLOAD_KEYS:
                continue
            if isinstance(raw_value, str):
                if normalized.endswith("_digest"):
                    safe[normalized] = (
                        raw_value[:64]
                        if _DIGEST_RE.fullmatch(raw_value)
                        else _digest(raw_value)
                    )
                    continue
                value, was_truncated = _safe_text(raw_value, limit=128)
                safe[normalized] = value
                truncated = truncated or was_truncated
            elif isinstance(raw_value, (int, float, bool)):
                safe[normalized] = raw_value
            elif isinstance(raw_value, (list, tuple, set, frozenset)):
                values = [str(value)[:64] for value in raw_value]
                safe[normalized] = sorted(values)[:32]
                truncated = truncated or len(values) > 32
            elif isinstance(raw_value, Mapping):
                numeric = {
                    str(key): value
                    for key, value in raw_value.items()
                    if isinstance(value, (int, float)) and not isinstance(value, bool)
                }
                safe[normalized] = dict(list(numeric.items())[:16])
                truncated = truncated or len(numeric) > 16
            else:
                redacted = True
        try:
            encoded = json.dumps(safe, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        except (TypeError, ValueError):
            return {"payload_digest": _digest(repr(payload))}, True, True
        encoded_bytes = encoded.encode("utf-8")
        if len(encoded_bytes) > self._config.max_activity_payload_bytes:
            return {
                "payload_digest": _digest(encoded),
                "payload_bytes": len(encoded_bytes),
            }, True, True
        return safe, redacted, truncated

    # ── Expiry, replay and capacity internals ───────────────────────

    def _expire_task_if_due(self, task_id: str) -> None:
        with self._condition:
            record = self._records.get(task_id)
        if record is not None:
            self._expire_if_due(record, self._now())

    def _expire_if_due(self, record: _WorkerRecord, now: datetime) -> None:
        with self._condition:
            if (
                self._records.get(record.task_id) is not record
                or record.state in TERMINAL_TASK_STATES
                or record.termination_started
            ):
                return
            if now >= record.deadline_at:
                timeout_kind = "deadline"
            elif (now - record.last_activity_at).total_seconds() >= record.idle_timeout_sec:
                timeout_kind = "idle_timeout"
            else:
                return
            record.termination_started = True
            backend_handle = record.backend_handle
        self._terminate_record(
            record,
            backend_handle,
            reason=f"worker {timeout_kind}",
            mode=timeout_kind,
        )

    def _terminate_record(
        self,
        record: _WorkerRecord,
        backend_handle: Any,
        *,
        reason: str,
        mode: str,
    ) -> None:
        """Run adapter stop/close outside the service-wide condition lock."""

        backend_result = None
        with record.io_lock:
            with self._condition:
                callable_now = (
                    self._records.get(record.task_id) is record
                    and record.state not in TERMINAL_TASK_STATES
                    and record.backend_handle is backend_handle
                )
            if callable_now:
                try:
                    backend_result = self._backend.stop(
                        backend_handle,
                        reason,
                        self._emitter(record.task_id),
                    )
                except Exception:
                    backend_result = None

        detached_handle = None
        with self._condition:
            if self._records.get(record.task_id) is not record:
                return
            completed_before_stop = (
                not callable_now and record.state in TERMINAL_TASK_STATES
            )
            if completed_before_stop:
                record.termination_started = False
                if record.backend_handle is backend_handle:
                    detached_handle = record.backend_handle
                    record.backend_handle = None
                self._condition.notify_all()
            else:
                if record.state not in TERMINAL_TASK_STATES:
                    target_state = (
                        TaskSubagentState.SHUTDOWN
                        if mode == "service_close"
                        and backend_result is not None
                        and backend_result.terminal
                        else (
                            TaskSubagentState.ERRORED
                            if mode != "service_close"
                            and backend_result is not None
                            and backend_result.terminal
                            else TaskSubagentState.UNCERTAIN
                        )
                    )
                    self._apply_backend_state_locked(record, target_state)
                if mode != "service_close":
                    record.error_code = (
                        "task_deadline_exceeded"
                        if mode == "deadline"
                        else "task_idle_timeout"
                    )
                    activity_kind = "worker_expired"
                    summary = "task worker expired at a bounded timeout"
                    payload = {"timeout_kind": mode, "error_code": record.error_code}
                else:
                    activity_kind = "service_closed"
                    summary = "task worker service closed"
                    payload = {
                        "status": (
                            "shutdown"
                            if backend_result is not None and backend_result.terminal
                            else "uncertain"
                        )
                    }
                record.ended_at = self._now()
                self._append_activity_locked(record, activity_kind, summary, payload)
                record.termination_started = False
                if record.backend_handle is backend_handle:
                    detached_handle = record.backend_handle
                    record.backend_handle = None
                self._condition.notify_all()
        if detached_handle is not None:
            try:
                self._backend.close(detached_handle)
            except Exception:
                pass

    def _prune_locked(self) -> None:
        overflow = len(self._records) - self._config.max_retained_tasks + 1
        if overflow <= 0:
            return
        terminal = sorted(
            (
                record
                for record in self._records.values()
                if record.state in TERMINAL_TASK_STATES
            ),
            key=lambda record: record.ended_at or record.last_activity_at,
        )
        for record in terminal[:overflow]:
            self._records.pop(record.task_id, None)
            key = (
                record.parent_context.world_id,
                record.parent_context.engram_id,
                record.parent_context.turn_id,
            )
            count = self._turn_spawn_counts.get(key, 0)
            if count <= 1:
                self._turn_spawn_counts.pop(key, None)
            else:
                self._turn_spawn_counts[key] = count - 1

    def _close_backend_locked(self, record: _WorkerRecord) -> None:
        if record.backend_handle is None:
            return
        if (
            record.state is TaskSubagentState.COMPLETED
            and record.delivery_content is None
        ):
            delivery = getattr(self._backend, "delivery_content", None)
            if callable(delivery):
                try:
                    value = delivery(record.backend_handle)
                except Exception:
                    value = None
                if isinstance(value, str):
                    record.delivery_content = value[: self._config.max_output_chars]
        try:
            self._backend.close(record.backend_handle)
        except Exception:
            pass
        record.backend_handle = None

    def _handle_locked(self, record: _WorkerRecord) -> TaskSubagentHandle:
        return TaskSubagentHandle(
            task_id=record.task_id,
            parent_turn_id=record.parent_context.turn_id,
            world_id=record.parent_context.world_id,
            engram_id=record.parent_context.engram_id,
            epoch=record.parent_context.epoch,
            state=record.state,
            capability_scope=record.capability_scope,
            evidence_class=record.evidence_class,
            created_at=record.created_at,
            deadline_at=record.deadline_at,
        )

    def _snapshot_locked(self, record: _WorkerRecord) -> TaskSubagentSnapshot:
        return TaskSubagentSnapshot(
            task_id=record.task_id,
            parent_turn_id=record.parent_context.turn_id,
            world_id=record.parent_context.world_id,
            engram_id=record.parent_context.engram_id,
            epoch=record.parent_context.epoch,
            state=record.state,
            capability_scope=record.capability_scope,
            evidence_class=record.evidence_class,
            created_at=record.created_at,
            deadline_at=record.deadline_at,
            last_activity_at=record.last_activity_at,
            activity_seq=record.next_seq - 1,
            first_available_seq=record.first_available_seq,
            task_digest=record.task_digest,
            task_chars=record.task_chars,
            result_digest=record.result_digest,
            error_code=record.error_code,
            gap=record.gap,
        )

    def _wait_result_locked(
        self,
        record: _WorkerRecord,
        activities: tuple[TaskSubagentActivity, ...],
        after_seq: int,
        *,
        timed_out: bool,
    ) -> TaskSubagentWaitResult:
        gap = record.gap and after_seq < record.first_available_seq - 1
        next_seq = activities[-1].seq if activities else max(after_seq, record.next_seq - 1)
        return TaskSubagentWaitResult(
            task_id=record.task_id,
            state=record.state,
            activities=activities[: self._config.max_activity_events],
            next_seq=next_seq,
            terminal=record.state in TERMINAL_TASK_STATES,
            timed_out=timed_out,
            gap=gap,
            evidence_class=record.evidence_class,
        )

    @staticmethod
    def _not_found_snapshot(task_id: str) -> TaskSubagentSnapshot:
        return TaskSubagentSnapshot(
            task_id=task_id if isinstance(task_id, str) else "unknown",
            parent_turn_id=None,
            world_id=None,
            engram_id=None,
            epoch=None,
            state=TaskSubagentState.NOT_FOUND,
            evidence_class=TaskWorkerEvidence.CONTRACT_ONLY,
        )

    @staticmethod
    def _not_found_wait(task_id: str) -> TaskSubagentWaitResult:
        return TaskSubagentWaitResult(
            task_id=task_id if isinstance(task_id, str) else "unknown",
            state=TaskSubagentState.NOT_FOUND,
            activities=(),
            next_seq=0,
            terminal=True,
            evidence_class=TaskWorkerEvidence.CONTRACT_ONLY,
        )

"""Temporary Pi task-worker adapter.

This module is the real adapter seam for a short-lived delegated Pi process.
It deliberately sits below :mod:`task_subagents`: the service owns the public
worker identity and scope checks, while this adapter owns the child process,
its isolated roots, cancellation barrier, and cleanup outcome.

The adapter never creates an Engram, writes PulseWorld or the causal ledger,
and never routes a task through the persistent Harness runtime.  A worker has
an internal Pi session only for the duration of one task.  Its public result
is a digest-and-counter projection; task text, model output, credentials and
filesystem paths do not enter activity payloads.

``session_factory`` and a ``PiBackend`` with ``transport_factory`` are explicit
test seams.  Either seam downgrades the evidence class to ``CONTRACT_ONLY``;
unit and process-contract tests therefore cannot be mistaken for a provider
live gate.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from math import isfinite
from pathlib import Path
from typing import Any

from pulse_system.agent.backends.pi import (
    PiBackend,
    _PI_PROCESS_BASE_ENV_KEYS,
    _PI_PROVIDER_ENV_KEYS,
    _minimal_pi_environment,
)
from pulse_system.agent.harness.base import HarnessError, HarnessSession
from pulse_system.agent.harness.pi import (
    PiProcessContext,
    PiSession,
    PiSessionCloseSummary,
)
from pulse_system.agent.harness.task_worker_protocol import (
    ActivityEmitter,
    TaskSubagentParentContext,
    TaskSubagentSpec,
    TaskSubagentState,
    TaskWorkerCloseObservation,
    TaskWorkerCloseSummary,
    TaskWorkerControlResult,
    TaskWorkerEvidence,
    TaskWorkerProcessTreeState,
    TaskWorkerStartResult,
    value_digest,
    utc_now,
)
from pulse_system.core.runtime.publication import (
    RuntimePublicationError,
    RuntimePublicationPermit,
)

__all__ = [
    "PiTaskWorkerBackend",
    "PiTaskWorkerHandle",
]


_WORKER_SCOPE_PREFIX = "pulse-task-worker-"
_WORKER_EXTRA_ARGS_BLOCKLIST = frozenset(
    {
        "--extension",
        "-e",
        "--tools",
        "-t",
        "--exclude-tools",
        "-xt",
    }
)


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("worker clock values must be aware datetimes")
    return value.astimezone(timezone.utc)


def _scope_fingerprint(
    task_id: str,
    spec: TaskSubagentSpec,
    parent_context: TaskSubagentParentContext,
) -> tuple[Any, ...]:
    """Return a collision key without retaining task text."""

    deadline = (
        spec.deadline_at.astimezone(timezone.utc).isoformat()
        if spec.deadline_at is not None
        else None
    )
    return (
        task_id,
        parent_context.world_id,
        parent_context.engram_id,
        parent_context.turn_id,
        parent_context.epoch,
        tuple(sorted(spec.capabilities)),
        value_digest(spec.task),
        len(spec.task),
        spec.timeout_sec,
        spec.idle_timeout_sec,
        deadline,
    )


def _safe_backend_error(exc: BaseException) -> str:
    """Classify an exception without copying its message into the projection."""

    if isinstance(exc, HarnessError):
        return exc.code
    return f"{type(exc).__name__}"[:80] or "WorkerError"


def _merge_process_tree_states(
    *states: TaskWorkerProcessTreeState,
) -> TaskWorkerProcessTreeState:
    if TaskWorkerProcessTreeState.UNKNOWN in states:
        return TaskWorkerProcessTreeState.UNKNOWN
    if TaskWorkerProcessTreeState.ROOT_EXIT_ONLY in states:
        return TaskWorkerProcessTreeState.ROOT_EXIT_ONLY
    return TaskWorkerProcessTreeState.NOT_APPLICABLE


def _remove_worker_tree(path: Path) -> None:
    """Remove one owned worker root, including long Windows descendants."""

    raw = os.path.abspath(os.fspath(path))
    if os.name == "nt" and not raw.startswith("\\\\?\\"):
        raw = (
            "\\\\?\\UNC\\" + raw[2:]
            if raw.startswith("\\\\")
            else "\\\\?\\" + raw
        )
    shutil.rmtree(raw)


@dataclass(slots=True)
class PiTaskWorkerHandle:
    """Opaque backend handle with inspectable, non-secret recovery facts."""

    task_id: str
    parent_context: TaskSubagentParentContext
    capability_scope: frozenset[str]
    task_digest: str
    task_chars: int
    deadline_at: datetime
    root: Path
    workspace_root: Path
    agent_root: Path
    session_root: Path
    evidence_class: TaskWorkerEvidence
    emit: ActivityEmitter = field(repr=False)
    cancel_event: threading.Event = field(default_factory=threading.Event, repr=False)
    done_event: threading.Event = field(default_factory=threading.Event, repr=False)
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    session: HarnessSession | None = field(default=None, repr=False)
    thread: Future[Any] | None = field(default=None, repr=False)
    cancel_owner: threading.Thread | None = field(default=None, repr=False)
    cleanup_owner: threading.Thread | None = field(default=None, repr=False)
    sideband_owners: set[threading.Thread] = field(
        default_factory=set,
        repr=False,
    )
    cancel_requested: bool = False
    cancel_confirmed: bool = False
    closed: bool = False
    session_closed: bool = False
    session_owner_joined: bool = True
    session_process_tree_state: TaskWorkerProcessTreeState = (
        TaskWorkerProcessTreeState.NOT_APPLICABLE
    )
    session_close_error_code: str | None = None
    session_close_summary: PiSessionCloseSummary | None = field(
        default=None,
        repr=False,
    )
    session_closing: bool = False
    cleanup_state: str = "PENDING"
    cleanup_error: str | None = None
    outcome_state: TaskSubagentState = TaskSubagentState.PENDING_INIT
    error_code: str | None = None
    result_digest: str | None = None
    result_content: str | None = field(default=None, repr=False)
    slot_released: bool = False

    @property
    def scope(self) -> TaskSubagentParentContext:
        return self.parent_context

    def recovery_snapshot(self) -> dict[str, Any]:
        """Return bounded recovery metadata; never include prompt/output text."""

        with self.lock:
            return {
                "task_id": self.task_id,
                "world_id": self.parent_context.world_id,
                "engram_id": self.parent_context.engram_id,
                "turn_id": self.parent_context.turn_id,
                "epoch": self.parent_context.epoch,
                "capability_scope": sorted(self.capability_scope),
                "state": self.outcome_state.value,
                "evidence_class": self.evidence_class.value,
                "task_digest": self.task_digest,
                "task_chars": self.task_chars,
                "deadline_at": self.deadline_at.isoformat(),
                "done": self.done_event.is_set(),
                "cancel_requested": self.cancel_requested,
                "cancel_confirmed": self.cancel_confirmed,
                "session_closed": self.session_closed,
                "session_owner_joined": self.session_owner_joined,
                "session_process_tree_state": self.session_process_tree_state.value,
                "cleanup_state": self.cleanup_state,
                "cleanup_error": self.cleanup_error,
                "result_digest": self.result_digest,
                "root_exists": self.root.exists(),
            }


@dataclass(slots=True)
class _RequestRecord:
    fingerprint: tuple[Any, ...]
    start_result: TaskWorkerStartResult
    handle: PiTaskWorkerHandle | None


class PiTaskWorkerBackend:
    """Bounded backend for one-shot, temporary Pi task workers.

    The class implements the existing ``TaskWorkerBackend`` protocol.  It is
    intentionally not a replacement for ``PiHarnessRuntime``: each accepted
    request receives a fresh workspace and fresh Pi agent/session roots, and
    the binding sink is left unset so the temporary session cannot become a
    durable Engram lineage.
    """

    evidence_class: TaskWorkerEvidence

    def __init__(
        self,
        pi_backend: PiBackend | None = None,
        *,
        worker_root: str | os.PathLike[str] | None = None,
        max_workers: int = 2,
        max_pending: int = 0,
        max_request_records: int = 1024,
        default_timeout_sec: float = 300.0,
        max_timeout_sec: float = 900.0,
        cancel_timeout_sec: float = 2.0,
        handshake_timeout_sec: float = 30.0,
        sideband_timeout_sec: float = 2.0,
        abort_timeout_sec: float = 1.0,
        publication_permit: RuntimePublicationPermit | None = None,
        session_factory: Callable[..., HarnessSession] | None = None,
        cleanup_fn: Callable[[Path], None] | None = None,
    ) -> None:
        if not isinstance(pi_backend, (PiBackend, type(None))):
            raise TypeError("pi_backend must be PiBackend or None")
        if (
            publication_permit is not None
            and type(publication_permit) is not RuntimePublicationPermit
        ):
            raise TypeError(
                "publication_permit must be a RuntimePublicationPermit or None"
            )
        for name, value in (
            ("max_workers", max_workers),
            ("max_pending", max_pending),
            ("max_request_records", max_request_records),
        ):
            if type(value) is not int or value < (1 if name != "max_pending" else 0):
                raise ValueError(f"{name} must be a valid integer")
        for name, value in (
            ("default_timeout_sec", default_timeout_sec),
            ("max_timeout_sec", max_timeout_sec),
            ("cancel_timeout_sec", cancel_timeout_sec),
            ("handshake_timeout_sec", handshake_timeout_sec),
            ("sideband_timeout_sec", sideband_timeout_sec),
            ("abort_timeout_sec", abort_timeout_sec),
        ):
            if (
                type(value) not in (int, float)
                or not isinstance(value, (int, float))
                or value <= 0
                or not isfinite(float(value))
            ):
                raise ValueError(f"{name} must be a finite positive number")
        if default_timeout_sec > max_timeout_sec:
            raise ValueError("default_timeout_sec cannot exceed max_timeout_sec")

        self._template = pi_backend or PiBackend()
        self._worker_root = (
            Path(worker_root).expanduser().resolve()
            if worker_root is not None
            else None
        )
        if self._worker_root is not None:
            if self._worker_root.exists() and not self._worker_root.is_dir():
                raise ValueError("worker_root must be a directory")
        self._max_workers = max_workers
        self._max_pending = max_pending
        self._max_request_records = max_request_records
        self._default_timeout_sec = float(default_timeout_sec)
        self._max_timeout_sec = float(max_timeout_sec)
        self._cancel_timeout_sec = float(cancel_timeout_sec)
        self._handshake_timeout_sec = float(handshake_timeout_sec)
        self._sideband_timeout_sec = float(sideband_timeout_sec)
        self._abort_timeout_sec = float(abort_timeout_sec)
        self._publication_permit = publication_permit
        self._session_factory = session_factory
        self._cleanup_fn = cleanup_fn or _remove_worker_tree
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="pulse-pi-task-worker",
        )
        self._capacity = threading.BoundedSemaphore(max_workers + max_pending)
        self._lock = threading.RLock()
        self._requests: dict[str, _RequestRecord] = {}
        self._active: dict[str, PiTaskWorkerHandle] = {}
        # Completed exact Pi workers may have joined every direct owner while
        # still lacking a descendant-tree census.  Retiring their handle must
        # not erase that orthogonal process evidence from fleet shutdown.
        self._retired_process_tree_state = (
            TaskWorkerProcessTreeState.NOT_APPLICABLE
        )
        self._closed = False
        self.evidence_class = (
            TaskWorkerEvidence.CONTRACT_ONLY
            if session_factory is not None
            or getattr(self._template, "_transport_factory", None) is not None
            else TaskWorkerEvidence.LIVE_GATE_UNVERIFIED
        )
        self._validate_template_args()

    def _validate_template_args(self) -> None:
        extra_args = tuple(getattr(self._template, "_extra_args", ()))
        for raw in extra_args:
            name = raw.split("=", 1)[0]
            if name in _WORKER_EXTRA_ARGS_BLOCKLIST or any(
                raw.startswith(f"{blocked}=")
                for blocked in _WORKER_EXTRA_ARGS_BLOCKLIST
            ):
                raise ValueError(
                    "Pi task workers do not accept extension or global tool-filter flags"
                )

    def _resolve_deadline(
        self,
        spec: TaskSubagentSpec,
        now: datetime,
    ) -> datetime | None:
        if spec.deadline_at is not None:
            deadline = _utc(spec.deadline_at)
        else:
            timeout = (
                self._default_timeout_sec
                if spec.timeout_sec is None
                else float(spec.timeout_sec)
            )
            deadline = now + timedelta(seconds=timeout)
        if deadline <= now:
            return None
        if (deadline - now).total_seconds() > self._max_timeout_sec:
            return None
        return deadline

    def _error_start(
        self,
        code: str,
        summary: str,
    ) -> TaskWorkerStartResult:
        return TaskWorkerStartResult(
            state=TaskSubagentState.ERRORED,
            evidence_class=self.evidence_class,
            summary=summary,
            error_code=code,
        )

    def start(
        self,
        task_id: str,
        spec: TaskSubagentSpec,
        parent_context: TaskSubagentParentContext,
        emit: ActivityEmitter,
    ) -> TaskWorkerStartResult:
        """Reserve one worker and schedule its isolated Pi lifecycle.

        ``task_id`` is the backend request key.  An exact retry returns the
        same start result and handle; reuse with a different scope or task
        digest is rejected before a second process or root can be created.
        """

        if not isinstance(task_id, str) or not task_id.strip():
            return self._error_start(
                "worker_invalid_task_id",
                "temporary Pi worker request id is invalid",
            )
        task_id = task_id.strip()
        if not isinstance(spec, TaskSubagentSpec):
            return self._error_start(
                "worker_invalid_spec",
                "temporary Pi worker spec is invalid",
            )
        if not isinstance(parent_context, TaskSubagentParentContext):
            return self._error_start(
                "worker_invalid_scope",
                "temporary Pi worker parent scope is invalid",
            )
        if (
            self._publication_permit is not None
            and parent_context.epoch != self._publication_permit.epoch
        ):
            return self._error_start(
                "worker_publication_epoch_mismatch",
                "temporary Pi worker publication permit belongs to another epoch",
            )
        if not callable(emit):
            return self._error_start(
                "worker_invalid_emitter",
                "temporary Pi worker activity emitter is invalid",
            )
        now = _utc(utc_now())
        deadline = self._resolve_deadline(spec, now)
        if deadline is None:
            return self._error_start(
                "worker_deadline_invalid",
                "temporary Pi worker deadline is expired or exceeds the adapter cap",
            )
        fingerprint = _scope_fingerprint(task_id, spec, parent_context)

        # Replaying a previously committed admission has no new physical or
        # process side effect, so it remains available after revoke.  A new
        # admission is checked again under the transaction guard below.
        with self._lock:
            prior = self._requests.get(task_id)
            if prior is not None:
                if prior.fingerprint == fingerprint:
                    return prior.start_result
                return self._error_start(
                    "worker_request_collision",
                    "worker request id is already bound to another parent scope or task",
                )

        publication_permit = self._publication_permit
        if (
            publication_permit is None
            and self.evidence_class is not TaskWorkerEvidence.CONTRACT_ONLY
        ):
            return self._error_start(
                "worker_publication_permit_required",
                "live temporary Pi worker admission requires Runtime publication authority",
            )
        admission_guard = (
            nullcontext()
            if publication_permit is None
            else publication_permit.transaction_guard()
        )
        cleanup_root: Path | None = None
        cleanup_handle: PiTaskWorkerHandle | None = None
        admission_result: TaskWorkerStartResult | None = None

        try:
            # This is the physical admission commit.  Revoke either wins
            # before capacity/root/process scheduling (zero side effects), or
            # immediately records this accepted starter in the Runtime's
            # publication-owner census while physical admission completes.
            # Revoke itself never waits for mkdir or executor submission.
            with admission_guard:
                with self._lock:
                    prior = self._requests.get(task_id)
                    if prior is not None:
                        if prior.fingerprint == fingerprint:
                            return prior.start_result
                        return self._error_start(
                            "worker_request_collision",
                            "worker request id is already bound to another parent scope or task",
                        )
                    if self._closed:
                        return self._error_start(
                            "worker_backend_closed",
                            "temporary Pi worker backend is closed",
                        )
                    if len(self._requests) >= self._max_request_records:
                        return self._error_start(
                            "worker_request_registry_exhausted",
                            "temporary Pi worker request registry is bounded and full",
                        )
                    if not self._capacity.acquire(blocking=False):
                        # Capacity refusal is retryable.  Do not tombstone a
                        # request that never acquired a child-process slot.
                        return self._error_start(
                            "worker_capacity_exhausted",
                            "temporary Pi worker capacity is exhausted",
                        )
                    # Reserve the request under the registry lock, then leave
                    # that lock before touching the filesystem.  A blocked
                    # mkdir must not freeze revoke, close, or unrelated starts.
                    provisional = TaskWorkerStartResult(
                        state=TaskSubagentState.PENDING_INIT,
                        evidence_class=self.evidence_class,
                        summary="temporary Pi worker physical admission is in progress",
                    )
                    self._requests[task_id] = _RequestRecord(
                        fingerprint=fingerprint,
                        start_result=provisional,
                        handle=None,
                    )

                root: Path | None = None
                try:
                    if self._worker_root is not None:
                        self._worker_root.mkdir(parents=True, exist_ok=True)
                    root = Path(
                        tempfile.mkdtemp(
                            prefix=_WORKER_SCOPE_PREFIX,
                            dir=(
                                str(self._worker_root)
                                if self._worker_root is not None
                                else None
                            ),
                        )
                    ).resolve()
                    workspace = root / "workspace"
                    agent_root = root / "agent"
                    session_root = root / "sessions"
                    for directory in (workspace, agent_root, session_root):
                        directory.mkdir(parents=True, exist_ok=False)
                except Exception:
                    cleanup_root = root
                    self._capacity.release()
                    admission_result = self._error_start(
                        "worker_root_creation_failed",
                        "temporary Pi worker roots could not be created",
                    )
                    with self._lock:
                        self._requests[task_id].start_result = admission_result

                if admission_result is None:
                    assert root is not None
                    handle = PiTaskWorkerHandle(
                        task_id=task_id,
                        parent_context=parent_context,
                        capability_scope=spec.capabilities,
                        task_digest=value_digest(spec.task),
                        task_chars=len(spec.task),
                        deadline_at=deadline,
                        root=root,
                        workspace_root=workspace,
                        agent_root=agent_root,
                        session_root=session_root,
                        evidence_class=self.evidence_class,
                        emit=emit,
                    )
                    start_result = TaskWorkerStartResult(
                        state=TaskSubagentState.PENDING_INIT,
                        backend_handle=handle,
                        evidence_class=self.evidence_class,
                        summary=(
                            "temporary Pi worker reserved; isolated session is starting"
                        ),
                    )
                    with self._lock:
                        if self._closed:
                            admission_result = self._error_start(
                                "worker_backend_closed",
                                "temporary Pi worker backend closed during admission",
                            )
                            self._requests[task_id] = _RequestRecord(
                                fingerprint=fingerprint,
                                start_result=admission_result,
                                handle=None,
                            )
                            self._capacity.release()
                            with handle.lock:
                                handle.slot_released = True
                            cleanup_handle = handle
                        else:
                            self._requests[task_id] = _RequestRecord(
                                fingerprint=fingerprint,
                                start_result=start_result,
                                handle=handle,
                            )
                            self._active[task_id] = handle

                    if admission_result is None:
                        try:
                            thread = self._executor.submit(self._run, handle, spec.task)
                        except Exception:
                            admission_result = self._error_start(
                                "worker_capacity_submit_failed",
                                "temporary Pi worker could not enter the bounded executor",
                            )
                            with self._lock:
                                self._requests[task_id] = _RequestRecord(
                                    fingerprint=fingerprint,
                                    start_result=admission_result,
                                    handle=None,
                                )
                                self._active.pop(task_id, None)
                            self._capacity.release()
                            with handle.lock:
                                handle.slot_released = True
                            cleanup_handle = handle
                        else:
                            with handle.lock:
                                handle.thread = thread
                            thread.add_done_callback(
                                lambda completed, owned=handle: self._on_future_done(
                                    owned,
                                    completed,
                                )
                            )
                            return start_result
        except RuntimePublicationError:
            return self._error_start(
                "worker_publication_revoked",
                "temporary Pi worker admission lost Runtime publication authority",
            )

        # Rollback deletion intentionally happens after the publication guard:
        # it removes an uncommitted external effect and must remain available
        # after revoke.  Cleanup is convergence, never ordinary publication.
        cleanup_ok = True
        if cleanup_handle is not None:
            cleanup_ok = self._cleanup_root(cleanup_handle)
        elif cleanup_root is not None:
            cleanup_ok = self._cleanup_uncommitted_root(cleanup_root)
        if not cleanup_ok:
            admission_result = self._error_start(
                "worker_admission_cleanup_uncertain",
                "temporary Pi worker admission failed with uncertain root cleanup",
            )
            with self._lock:
                prior = self._requests.get(task_id)
                if prior is not None:
                    prior.start_result = admission_result
        assert admission_result is not None
        return admission_result

    def steer(
        self,
        backend_handle: Any,
        message: str,
        emit: ActivityEmitter,
    ) -> TaskWorkerControlResult:
        """Forward a bounded sideband steer without exposing its content."""

        handle = self._owned_handle(backend_handle)
        if handle is None:
            return TaskWorkerControlResult(
                accepted=False,
                state=TaskSubagentState.NOT_FOUND,
                detail="temporary Pi worker handle is not owned by this backend",
                error_code="worker_handle_invalid",
            )
        if not isinstance(message, str) or not message.strip():
            return TaskWorkerControlResult(
                accepted=False,
                state=handle.outcome_state,
                detail="worker steer message is empty",
                error_code="worker_invalid_message",
            )
        with handle.lock:
            if handle.done_event.is_set():
                return TaskWorkerControlResult(
                    accepted=False,
                    terminal=True,
                    state=handle.outcome_state,
                    detail="temporary Pi worker is already terminal",
                    error_code="worker_terminal",
                )
            if handle.cancel_requested or handle.cancel_event.is_set():
                return TaskWorkerControlResult(
                    accepted=False,
                    state=TaskSubagentState.UNCERTAIN,
                    detail="temporary Pi worker is stopping",
                    error_code="worker_stopping",
                )
            session = handle.session
        if session is None:
            return TaskWorkerControlResult(
                accepted=False,
                state=TaskSubagentState.PENDING_INIT,
                detail="temporary Pi worker session is not ready",
                error_code="worker_not_running",
            )
        ok, _value, error = self._bounded_call(
            handle,
            session.steer,
            message,
            timeout=self._sideband_timeout_sec,
            expected_session=session,
        )
        if not ok:
            return TaskWorkerControlResult(
                accepted=False,
                state=TaskSubagentState.UNCERTAIN,
                detail="temporary Pi worker steer was not confirmed",
                error_code="worker_steer_unconfirmed" if error is None else _safe_backend_error(error),
            )
        try:
            emit(
                "steer_accepted",
                "temporary Pi worker steer accepted",
                {
                    "message_digest": value_digest(message),
                    "message_chars": len(message),
                    "status": "accepted",
                },
                state=TaskSubagentState.RUNNING,
            )
        except Exception:
            pass
        return TaskWorkerControlResult(
            accepted=True,
            state=TaskSubagentState.RUNNING,
            detail="temporary Pi worker steer accepted",
        )

    def stop(
        self,
        backend_handle: Any,
        reason: str,
        emit: ActivityEmitter,
    ) -> TaskWorkerControlResult:
        """Request cancellation, then observe against the adapter's bound."""

        requested = self.request_stop(backend_handle, reason, emit)
        if not requested.accepted or requested.terminal:
            return requested
        observation = self.observe_stop(
            backend_handle,
            deadline=time.monotonic() + self._cancel_timeout_sec,
        )
        if observation.terminal and observation.owner_joined:
            return TaskWorkerControlResult(
                accepted=True,
                terminal=True,
                state=observation.state,
                detail="temporary Pi worker cancellation settled",
                error_code=observation.error_code,
            )
        return TaskWorkerControlResult(
            accepted=True,
            terminal=False,
            state=TaskSubagentState.UNCERTAIN,
            detail="temporary Pi worker cancellation was not confirmed before the bound",
            error_code=observation.error_code or "worker_cancel_unconfirmed",
        )

    def request_stop(
        self,
        backend_handle: Any,
        reason: str,
        emit: ActivityEmitter,
    ) -> TaskWorkerControlResult:
        """Signal one worker without waiting for abort or cleanup owners."""

        del emit
        handle = self._owned_handle(backend_handle)
        if handle is None:
            return TaskWorkerControlResult(
                accepted=False,
                state=TaskSubagentState.ERRORED,
                detail="temporary Pi worker handle is not owned by this backend",
                error_code="worker_handle_invalid",
            )
        if not isinstance(reason, str) or not reason.strip():
            return TaskWorkerControlResult(
                accepted=False,
                state=handle.outcome_state,
                detail="worker stop reason is empty",
                error_code="worker_invalid_reason",
            )
        with handle.lock:
            if handle.done_event.is_set():
                return TaskWorkerControlResult(
                    accepted=False,
                    terminal=True,
                    state=handle.outcome_state,
                    detail="temporary Pi worker is already terminal",
                    error_code="worker_terminal",
                )
            handle.cancel_requested = True
            handle.cancel_event.set()
            session = handle.session
            cancel_owner = handle.cancel_owner
            if session is not None and (
                cancel_owner is None or not cancel_owner.is_alive()
            ):
                cancel_owner = threading.Thread(
                    target=self._abort_session,
                    args=(session,),
                    name=f"pulse-pi-task-worker-abort-{value_digest(handle.task_id)}",
                    daemon=True,
                )
                handle.cancel_owner = cancel_owner
                cancel_owner.start()
        return TaskWorkerControlResult(
            accepted=True,
            terminal=False,
            state=handle.outcome_state,
            detail="temporary Pi worker cancellation was broadcast",
        )

    def observe_stop(
        self,
        backend_handle: Any,
        *,
        deadline: float,
    ) -> TaskWorkerCloseObservation:
        """Observe the worker, abort owner and cleanup barrier independently."""

        if (
            type(deadline) not in (int, float)
            or not isfinite(float(deadline))
        ):
            raise ValueError("deadline must be a finite monotonic timestamp")
        handle = self._owned_handle(backend_handle)
        if handle is None:
            return TaskWorkerCloseObservation(
                task_id="task_invalid",
                terminal=False,
                owner_joined=False,
                process_tree_state=TaskWorkerProcessTreeState.UNKNOWN,
                state=TaskSubagentState.UNCERTAIN,
                error_code="worker_handle_invalid",
            )
        remaining = max(0.0, float(deadline) - time.monotonic())
        handle.done_event.wait(timeout=remaining)
        with handle.lock:
            future = handle.thread
            cancel_owner = handle.cancel_owner
            cleanup_owner = handle.cleanup_owner
            sideband_owners = tuple(handle.sideband_owners)
        if future is not None and not future.done():
            remaining = max(0.0, float(deadline) - time.monotonic())
            if remaining > 0:
                try:
                    future.result(timeout=remaining)
                except FutureTimeoutError:
                    pass
                except Exception:
                    pass
        if cancel_owner is not None and cancel_owner is not threading.current_thread():
            cancel_owner.join(
                timeout=max(0.0, float(deadline) - time.monotonic())
            )
        if cleanup_owner is not None and cleanup_owner is not threading.current_thread():
            cleanup_owner.join(
                timeout=max(0.0, float(deadline) - time.monotonic())
            )
        for sideband_owner in sideband_owners:
            if sideband_owner is threading.current_thread():
                continue
            sideband_owner.join(
                timeout=max(0.0, float(deadline) - time.monotonic())
            )
        with handle.lock:
            handle.sideband_owners.difference_update(
                owner for owner in sideband_owners if not owner.is_alive()
            )
            terminal = handle.done_event.is_set()
            future_joined = future is None or future.done()
            cancel_joined = cancel_owner is None or not cancel_owner.is_alive()
            cleanup_joined = cleanup_owner is None or not cleanup_owner.is_alive()
            sidebands_joined = not any(
                owner.is_alive() for owner in handle.sideband_owners
            )
            session_joined = handle.session_owner_joined
            session_tree = handle.session_process_tree_state
            owner_joined = (
                terminal
                and future_joined
                and cancel_joined
                and cleanup_joined
                and sidebands_joined
                and session_joined
            )
            state = handle.outcome_state if terminal else TaskSubagentState.UNCERTAIN
            cleanup_state = handle.cleanup_state
            handle.cancel_confirmed = (
                owner_joined and state is TaskSubagentState.INTERRUPTED
            )
            error_code = handle.session_close_error_code or handle.error_code
        if handle.session_close_summary is not None:
            # Exact PiSession close evidence is authoritative for its own
            # process-tree axis even when every direct owner joined.
            tree = session_tree
        elif not owner_joined:
            tree = (
                TaskWorkerProcessTreeState.UNKNOWN
                if session_tree is TaskWorkerProcessTreeState.NOT_APPLICABLE
                else session_tree
            )
            error_code = error_code or "worker_owner_exit_unproven"
        elif handle.evidence_class is TaskWorkerEvidence.CONTRACT_ONLY:
            tree = TaskWorkerProcessTreeState.NOT_APPLICABLE
        else:
            # Pi/RPC root settlement is not a descendant-tree census.
            tree = TaskWorkerProcessTreeState.ROOT_EXIT_ONLY
        if owner_joined:
            self._retire_if_quiescent(handle)
        if cleanup_state != "CLEAN" and owner_joined:
            state = TaskSubagentState.UNCERTAIN
            error_code = "worker_cleanup_uncertain"
        return TaskWorkerCloseObservation(
            task_id=handle.task_id,
            terminal=terminal,
            owner_joined=owner_joined,
            process_tree_state=tree,
            state=state,
            error_code=error_code,
        )

    def _abort_session(self, session: HarnessSession) -> None:
        try:
            session.abort()
        except Exception:
            pass

    def close(self, backend_handle: Any) -> None:
        """Detach a settled handle; cancellation itself is a separate phase."""

        handle = self._owned_handle(backend_handle)
        if handle is None:
            return
        with handle.lock:
            handle.closed = True
            handle.cancel_requested = True
            handle.cancel_event.set()
            done = handle.done_event.is_set()
        if not done:
            self.request_stop(handle, "backend handle closed", handle.emit)

    def shutdown(
        self,
        *,
        deadline: float | None = None,
    ) -> TaskWorkerCloseSummary:
        """Broadcast to every handle before observing any worker owner."""

        if deadline is not None and (
            type(deadline) not in (int, float)
            or not isfinite(float(deadline))
        ):
            raise ValueError("deadline must be a finite monotonic timestamp")
        with self._lock:
            if self._closed:
                active = tuple(self._active.values())
            else:
                self._closed = True
                active = tuple(self._active.values())
        close_deadline = (
            time.monotonic() + self._cancel_timeout_sec
            if deadline is None
            else float(deadline)
        )
        requested = 0
        for handle in active:
            result = self.request_stop(handle, "backend shutdown", handle.emit)
            requested += int(result.accepted)
        # A queued Future never enters _run and therefore has no finally block.
        # Cancel it before observation so its tracked completion callback can
        # own root cleanup, capacity release and terminalization.
        self._executor.shutdown(wait=False, cancel_futures=True)
        observations = tuple(
            self.observe_stop(handle, deadline=close_deadline)
            for handle in active
        )
        unresolved = sum(not item.owner_joined for item in observations)
        with self._lock:
            retired_tree = self._retired_process_tree_state
        tree = _merge_process_tree_states(
            retired_tree,
            *(item.process_tree_state for item in observations),
        )
        return TaskWorkerCloseSummary(
            active_before=len(active),
            unresolved=unresolved,
            owner_joined=unresolved == 0,
            process_tree_state=tree,
            cancellation_requested=requested,
            terminal_observed=sum(item.terminal for item in observations),
        )

    def wait(self, backend_handle: Any, timeout: float | None = None) -> bool:
        """Wait for the worker's own terminal-and-cleanup barrier."""

        handle = self._owned_handle(backend_handle)
        if handle is None:
            return False
        if timeout is not None and (
            type(timeout) not in (int, float) or timeout < 0
        ):
            raise ValueError("timeout must be a non-negative number or None")
        return handle.done_event.wait(timeout=timeout)

    def delivery_content(self, backend_handle: Any) -> str | None:
        """Return completed model output only to the scoped parent service.

        The value is never copied into activity/recovery snapshots.  It is an
        in-process delivery channel analogous to an ordinary Pi tool result;
        after restart only ``result_digest`` remains and automatic replay is
        forbidden.
        """

        handle = self._owned_handle(backend_handle)
        if handle is None:
            return None
        with handle.lock:
            if (
                not handle.done_event.is_set()
                or handle.outcome_state is not TaskSubagentState.COMPLETED
                or not isinstance(handle.result_content, str)
            ):
                return None
            return handle.result_content

    def _owned_handle(self, value: Any) -> PiTaskWorkerHandle | None:
        if not isinstance(value, PiTaskWorkerHandle):
            return None
        with self._lock:
            record = self._requests.get(value.task_id)
            return value if record is not None and record.handle is value else None

    def _session_backend(self, handle: PiTaskWorkerHandle) -> PiBackend:
        """Clone the template with a clean provider-scoped child environment."""

        provider = getattr(self._template, "_provider", None)
        source_env = getattr(self._template, "_env", {})
        allowed = set(_PI_PROCESS_BASE_ENV_KEYS)
        normalized = provider.casefold() if isinstance(provider, str) else ""
        allowed.update(_PI_PROVIDER_ENV_KEYS.get(normalized, ()))
        minimal = _minimal_pi_environment(provider)
        if isinstance(source_env, Mapping):
            minimal.update(
                {
                    key: value
                    for key, value in source_env.items()
                    if key in allowed and isinstance(value, str) and value
                }
            )
        extra_args = tuple(getattr(self._template, "_extra_args", ()))
        if "--no-builtin-tools" not in extra_args and "-nbt" not in extra_args:
            extra_args += ("--no-builtin-tools",)
        if "--no-extensions" not in extra_args and "-ne" not in extra_args:
            extra_args += ("--no-extensions",)
        return PiBackend(
            executable=getattr(self._template, "_executable", "pi"),
            workdir=handle.workspace_root,
            provider=provider,
            model=getattr(self._template, "_model", None),
            env=minimal,
            handshake_timeout_sec=self._handshake_timeout_sec,
            max_trace_events=getattr(self._template, "_max_trace", 500),
            include_session_leaf=False,
            transport_factory=getattr(self._template, "_transport_factory", None),
            extra_args=extra_args,
            launcher_args=tuple(getattr(self._template, "_launcher_args", ())),
        )

    def _new_session(
        self,
        handle: PiTaskWorkerHandle,
        backend: PiBackend,
        *,
        handshake_timeout_sec: float,
    ) -> HarnessSession:
        context = PiProcessContext(
            env={
                "PI_CODING_AGENT_DIR": str(handle.agent_root),
                "PI_CODING_AGENT_SESSION_DIR": str(handle.session_root),
            }
        )
        factory = self._session_factory
        if factory is not None:
            session = factory(
                engram_id=f"worker-{value_digest(handle.task_id)}",
                workspace=handle.workspace_root,
                backend=backend,
                process_context=context,
                handshake_timeout_sec=handshake_timeout_sec,
                sideband_timeout_sec=self._sideband_timeout_sec,
                abort_timeout_sec=self._abort_timeout_sec,
                publication_permit=self._publication_permit,
            )
        else:
            session = PiSession(
                f"worker-{value_digest(handle.task_id)}",
                handle.workspace_root,
                backend=backend,
                process_context=context,
                handshake_timeout_sec=handshake_timeout_sec,
                sideband_timeout_sec=self._sideband_timeout_sec,
                abort_timeout_sec=self._abort_timeout_sec,
                publication_permit=self._publication_permit,
            )
        if not isinstance(session, HarnessSession):
            raise TypeError("session_factory must return a HarnessSession")
        return session

    def _run(self, handle: PiTaskWorkerHandle, task: str) -> None:
        session: HarnessSession | None = None
        outcome = TaskSubagentState.ERRORED
        error_code: str | None = "worker_not_started"
        summary = "temporary Pi worker failed before completion"
        payload: dict[str, Any] = {"status": "failed"}
        try:
            self._emit(
                handle,
                "worker_starting",
                "temporary Pi worker is starting an isolated Pi session",
                {"status": "starting"},
                state=TaskSubagentState.WAITING,
            )
            if handle.cancel_event.is_set():
                outcome = TaskSubagentState.INTERRUPTED
                error_code = "worker_cancelled_before_start"
                summary = "temporary Pi worker cancelled before Pi startup"
                payload = {"status": "cancelled", "error_code": error_code}
                return

            if (handle.deadline_at - _utc(utc_now())).total_seconds() <= 0:
                outcome = TaskSubagentState.ERRORED
                error_code = "worker_deadline_exceeded"
                summary = "temporary Pi worker deadline elapsed before Pi startup"
                payload = {"status": "failed", "error_code": error_code}
                return

            startup_budget = (
                handle.deadline_at - _utc(utc_now())
            ).total_seconds()
            if startup_budget <= 0:
                outcome = TaskSubagentState.ERRORED
                error_code = "worker_deadline_exceeded"
                summary = "temporary Pi worker deadline elapsed before Pi startup"
                payload = {"status": "failed", "error_code": error_code}
                return
            backend = self._session_backend(handle)
            session = self._new_session(
                handle,
                backend,
                handshake_timeout_sec=min(self._handshake_timeout_sec, startup_budget),
            )
            with handle.lock:
                handle.session = session
                # From this point until trusted close evidence says otherwise,
                # the session owns a possible Pi process/reader set.
                handle.session_owner_joined = False
                handle.session_process_tree_state = (
                    TaskWorkerProcessTreeState.UNKNOWN
                )
            if handle.cancel_event.is_set():
                outcome = TaskSubagentState.INTERRUPTED
                error_code = "worker_cancelled_before_start"
                summary = "temporary Pi worker cancelled before Pi startup"
                payload = {"status": "cancelled", "error_code": error_code}
                return

            if (handle.deadline_at - _utc(utc_now())).total_seconds() <= 0:
                outcome = TaskSubagentState.ERRORED
                error_code = "worker_deadline_exceeded"
                summary = "temporary Pi worker deadline elapsed before the task turn"
                payload = {"status": "failed", "error_code": error_code}
                return

            session.start()
            self._emit(
                handle,
                "worker_ready",
                "temporary Pi worker session is ready",
                {"status": "ready"},
                state=TaskSubagentState.RUNNING,
            )
            if handle.cancel_event.is_set():
                outcome = TaskSubagentState.INTERRUPTED
                error_code = "worker_cancelled_before_turn"
                summary = "temporary Pi worker cancelled before the task turn"
                payload = {"status": "cancelled", "error_code": error_code}
                return

            remaining = (handle.deadline_at - _utc(utc_now())).total_seconds()
            if remaining <= 0:
                outcome = TaskSubagentState.ERRORED
                error_code = "worker_deadline_exceeded"
                summary = "temporary Pi worker deadline elapsed before the task turn"
                payload = {"status": "failed", "error_code": error_code}
                return

            result = session.run_turn(
                task,
                timeout_sec=remaining,
                turn_id=handle.task_id,
            )
            if handle.cancel_event.is_set():
                outcome = TaskSubagentState.INTERRUPTED
                error_code = "worker_cancelled"
                summary = "temporary Pi worker cancelled after the Pi turn"
                payload = {"status": "cancelled", "error_code": error_code}
                return
            content = getattr(result, "content", None)
            if not isinstance(content, str) or not content.strip():
                outcome = TaskSubagentState.UNCERTAIN
                error_code = "worker_result_invalid"
                summary = "temporary Pi worker returned no usable structured result"
                payload = {"status": "uncertain", "error_code": error_code}
                return
            if self.evidence_class is TaskWorkerEvidence.LIVE_GATE_UNVERIFIED:
                if getattr(result, "evidence_class", None) != "LIVE_PI_PROVIDER":
                    outcome = TaskSubagentState.UNCERTAIN
                    error_code = "worker_provider_evidence_unverified"
                    summary = "temporary Pi worker settled without live provider evidence"
                    payload = {"status": "uncertain", "error_code": error_code}
                    return
                with handle.lock:
                    handle.evidence_class = TaskWorkerEvidence.LIVE_PI_PROVIDER
            handle.result_digest = value_digest(content)
            with handle.lock:
                handle.result_content = content[:1_000_000]
            outcome = TaskSubagentState.COMPLETED
            error_code = None
            summary = "temporary Pi worker completed with a structured summary"
            payload = {
                "status": "completed",
                "result_digest": handle.result_digest,
                "output_chars": len(content),
                "tool_calls": getattr(result, "tool_calls", 0),
            }
        except HarnessError as exc:
            with handle.lock:
                cancelled = handle.cancel_requested or handle.cancel_event.is_set()
            if cancelled:
                outcome = TaskSubagentState.INTERRUPTED
                error_code = "worker_cancelled"
                summary = "temporary Pi worker cancellation settled through Pi"
                payload = {"status": "cancelled", "error_code": error_code}
            elif exc.prompt_accepted is not False:
                outcome = TaskSubagentState.UNCERTAIN
                error_code = exc.code
                summary = "temporary Pi worker stopped without a confirmed terminal result"
                payload = {"status": "uncertain", "error_code": error_code}
            else:
                outcome = TaskSubagentState.ERRORED
                error_code = exc.code
                summary = "temporary Pi worker failed before accepting the task"
                payload = {"status": "failed", "error_code": error_code}
        except Exception as exc:
            outcome = TaskSubagentState.ERRORED
            error_code = "worker_backend_exception"
            summary = "temporary Pi worker backend raised an isolated adapter error"
            payload = {
                "status": "failed",
                "error_code": error_code,
                "exception_type": _safe_backend_error(exc),
            }
        finally:
            close_returned = True
            session_owner_joined = True
            if session is not None:
                with handle.lock:
                    handle.session_closing = True
                try:
                    close_summary = session.close()
                except Exception as exc:
                    close_returned = False
                    session_owner_joined = False
                    with handle.lock:
                        handle.cleanup_error = _safe_backend_error(exc)
                        handle.session_close_error_code = _safe_backend_error(exc)
                        handle.session_process_tree_state = (
                            TaskWorkerProcessTreeState.UNKNOWN
                        )
                else:
                    session_owner_joined = self._record_session_close(
                        handle,
                        session,
                        close_summary,
                    )
            with handle.lock:
                handle.session_closed = close_returned
                handle.session_owner_joined = session_owner_joined
                handle.session_closing = False
                if not session_owner_joined:
                    handle.cleanup_state = "UNCERTAIN"
                    handle.cleanup_error = (
                        handle.cleanup_error
                        or handle.session_close_error_code
                        or "worker_session_owner_unresolved"
                    )
            cleanup_ok = (
                self._cleanup_root(handle)
                if session_owner_joined
                else False
            )
            if not close_returned or not session_owner_joined or not cleanup_ok:
                outcome = TaskSubagentState.UNCERTAIN
                error_code = "worker_cleanup_uncertain"
                summary = "temporary Pi worker finished with uncertain cleanup"
                payload = {
                    "status": "uncertain",
                    "error_code": error_code,
                    "recovery_state": "uncertain",
                }
            elif outcome is TaskSubagentState.COMPLETED:
                payload["evidence_class"] = handle.evidence_class.value
            self._finish(handle, outcome, error_code, summary, payload)

    def _finish(
        self,
        handle: PiTaskWorkerHandle,
        state: TaskSubagentState,
        error_code: str | None,
        summary: str,
        payload: Mapping[str, Any],
    ) -> None:
        with handle.lock:
            if handle.done_event.is_set():
                return
            handle.outcome_state = state
            handle.error_code = error_code
            handle.done_event.set()
        with handle.lock:
            if not handle.slot_released:
                handle.slot_released = True
                self._capacity.release()
        self._emit(
            handle,
            "worker_completed" if state is TaskSubagentState.COMPLETED else "worker_terminal",
            summary,
            dict(payload),
            state=state,
            terminal=True,
        )

    def _on_future_done(
        self,
        handle: PiTaskWorkerHandle,
        future: Future[Any],
    ) -> None:
        """Retire a worker Future or own cleanup if it never entered _run."""

        if future.cancelled():
            with handle.lock:
                owner = handle.cleanup_owner
                if owner is None:
                    owner = threading.Thread(
                        target=self._finish_cancelled_before_start,
                        args=(handle,),
                        name=(
                            "pulse-pi-task-worker-queued-cleanup-"
                            f"{value_digest(handle.task_id)}"
                        ),
                        daemon=True,
                    )
                    handle.cleanup_owner = owner
                    owner.start()
            return
        self._retire_if_quiescent(handle)

    def _finish_cancelled_before_start(
        self,
        handle: PiTaskWorkerHandle,
    ) -> None:
        cleanup_ok = self._cleanup_root(handle)
        with handle.lock:
            handle.session_closed = True
        if cleanup_ok:
            self._finish(
                handle,
                TaskSubagentState.INTERRUPTED,
                "worker_cancelled_before_start",
                "queued temporary Pi worker was cancelled before startup",
                {
                    "status": "cancelled",
                    "error_code": "worker_cancelled_before_start",
                },
            )
        else:
            self._finish(
                handle,
                TaskSubagentState.UNCERTAIN,
                "worker_cleanup_uncertain",
                "queued temporary Pi worker root cleanup is uncertain",
                {
                    "status": "uncertain",
                    "error_code": "worker_cleanup_uncertain",
                    "recovery_state": "uncertain",
                },
            )

    def _retire_if_quiescent(self, handle: PiTaskWorkerHandle) -> None:
        """Drop only handles whose complete physical owner set has exited."""

        # Keep the registry membership check and the complete owner census in
        # one backend→handle lock order.  A sideband reservation uses the same
        # order, so retirement cannot slip between its terminal recheck and
        # owner registration.
        with self._lock:
            if self._active.get(handle.task_id) is not handle:
                return
            with handle.lock:
                future = handle.thread
                cancel_owner = handle.cancel_owner
                cleanup_owner = handle.cleanup_owner
                dead_sidebands = tuple(
                    owner
                    for owner in handle.sideband_owners
                    if not owner.is_alive()
                )
                handle.sideband_owners.difference_update(dead_sidebands)
                quiescent = (
                    handle.done_event.is_set()
                    and handle.session_owner_joined
                    and (future is None or future.done())
                    and (cancel_owner is None or not cancel_owner.is_alive())
                    and (cleanup_owner is None or not cleanup_owner.is_alive())
                    and not handle.sideband_owners
                )
                if quiescent:
                    self._retired_process_tree_state = _merge_process_tree_states(
                        self._retired_process_tree_state,
                        handle.session_process_tree_state,
                    )
                    self._active.pop(handle.task_id, None)

    def _record_session_close(
        self,
        handle: PiTaskWorkerHandle,
        session: HarnessSession,
        raw_summary: Any,
    ) -> bool:
        """Retain exact Pi owner evidence; opaque test seams stay contract-only."""

        if type(session) is not PiSession:
            with handle.lock:
                handle.session_process_tree_state = (
                    TaskWorkerProcessTreeState.NOT_APPLICABLE
                )
                handle.session_close_error_code = None
            return True
        if type(raw_summary) is not dict:
            with handle.lock:
                handle.session_process_tree_state = TaskWorkerProcessTreeState.UNKNOWN
                handle.session_close_error_code = "worker_pi_close_summary_invalid"
            return False
        owner_value = raw_summary.get("owner_joined")
        unresolved_value = raw_summary.get("unresolved")
        tree_value = raw_summary.get("process_tree_state")
        if (
            type(owner_value) is not bool
            or type(unresolved_value) is not int
            or unresolved_value < 0
            or tree_value
            not in {
                TaskWorkerProcessTreeState.NOT_APPLICABLE.value,
                TaskWorkerProcessTreeState.ROOT_EXIT_ONLY.value,
                TaskWorkerProcessTreeState.UNKNOWN.value,
            }
        ):
            with handle.lock:
                handle.session_process_tree_state = TaskWorkerProcessTreeState.UNKNOWN
                handle.session_close_error_code = "worker_pi_close_summary_invalid"
            return False
        normalized = dict(raw_summary)
        joined = owner_value and unresolved_value == 0
        error_value = raw_summary.get("error_code")
        with handle.lock:
            handle.session_close_summary = normalized  # type: ignore[assignment]
            handle.session_process_tree_state = TaskWorkerProcessTreeState(tree_value)
            handle.session_close_error_code = (
                error_value
                if isinstance(error_value, str) and error_value
                else (None if joined else "worker_pi_session_owner_unresolved")
            )
        return joined

    def _emit(
        self,
        handle: PiTaskWorkerHandle,
        kind: str,
        summary: str,
        payload: Mapping[str, Any],
        *,
        state: TaskSubagentState | None = None,
        terminal: bool = False,
    ) -> None:
        try:
            handle.emit(
                kind,
                summary,
                dict(payload),
                state=state,
                terminal=terminal,
            )
        except Exception:
            # An observer cannot change cleanup or terminal state.
            pass

    def _cleanup_root(self, handle: PiTaskWorkerHandle) -> bool:
        """Remove an admitted root as an independent convergence operation.

        This method deliberately accepts no publication permit and enters no
        publication transaction: deletion after cancellation/revoke is
        containment, not a new Runtime publication.
        """

        with handle.lock:
            if handle.cleanup_state == "CLEAN":
                return True
            if handle.cleanup_state == "UNCERTAIN":
                return False
        try:
            if handle.root.exists():
                self._cleanup_fn(handle.root)
            clean = not handle.root.exists()
        except Exception as exc:
            with handle.lock:
                handle.cleanup_state = "UNCERTAIN"
                handle.cleanup_error = _safe_backend_error(exc)
            return False
        with handle.lock:
            handle.cleanup_state = "CLEAN" if clean else "UNCERTAIN"
            if not clean:
                handle.cleanup_error = "root_still_exists"
        return clean

    def _cleanup_uncommitted_root(self, root: Path) -> bool:
        """Remove a partially admitted root without publication authority."""

        try:
            if root.exists():
                self._cleanup_fn(root)
            return not root.exists()
        except Exception:
            return False

    @staticmethod
    def _close_session(session: HarnessSession) -> None:
        try:
            session.close()
        except Exception:
            pass

    def _bounded_call(
        self,
        handle: PiTaskWorkerHandle,
        function: Callable[..., Any],
        *args: Any,
        timeout: float,
        expected_session: HarnessSession | None = None,
    ) -> tuple[bool, Any, BaseException | None]:
        completed = threading.Event()
        result: list[Any] = []
        error: list[BaseException] = []

        def invoke() -> None:
            try:
                result.append(function(*args))
            except BaseException as exc:  # preserve timeout/KeyboardInterrupt in the worker
                error.append(exc)
            finally:
                completed.set()
                current = threading.current_thread()
                with handle.lock:
                    handle.sideband_owners.discard(current)
                self._retire_if_quiescent(handle)

        owner = threading.Thread(
            target=invoke,
            name="pulse-pi-task-worker-sideband",
            daemon=True,
        )
        with self._lock:
            with handle.lock:
                handle.sideband_owners.difference_update(
                    owner
                    for owner in tuple(handle.sideband_owners)
                    if not owner.is_alive()
                )
                if (
                    self._active.get(handle.task_id) is not handle
                    or handle.done_event.is_set()
                    or handle.session_closing
                    or handle.session_closed
                    or handle.cancel_requested
                    or handle.cancel_event.is_set()
                    # One worker may own at most one unsettled sideband.  A
                    # timed-out RPC thread remains a real shutdown owner; a
                    # retry must not multiply hidden threads behind it.
                    or bool(handle.sideband_owners)
                    or (
                        expected_session is not None
                        and handle.session is not expected_session
                    )
                ):
                    return False, None, None
                handle.sideband_owners.add(owner)
                try:
                    owner.start()
                except BaseException as exc:
                    handle.sideband_owners.discard(owner)
                    return False, None, exc
        if not completed.wait(timeout=max(0.0, timeout)):
            return False, None, None
        owner.join(timeout=0.0)
        if error:
            return False, None, error[0]
        return True, result[0] if result else None, None

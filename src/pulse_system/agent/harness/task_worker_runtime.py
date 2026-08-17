"""Runtime/Pi tool bridge for bounded temporary task workers.

The bridge keeps a task worker below the Engram/PulseWorld identity plane.  A
spawn is a durable E0 operation whose id is the parent Pi ``toolCallId``; the
worker receives a stable ``task_`` id, and its terminal state is joined to one
canonical Harness event.  Restart never respawns an orphaned worker.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from math import isfinite
from typing import Any

from pulse_system.agent.tools.gateway import ToolInvocationContext
from pulse_system.core.runtime.publication import RuntimeRecoveryPermit

from .events import (
    HarnessEventDraft,
    HarnessEventKind,
    HarnessEventPhase,
    HarnessEventSource,
    HarnessEventStatus,
)
from .operations import (
    HarnessOperation,
    HarnessOperationLedger,
    OperationPhase,
    OperationRecoveryState,
    OperationTerminalState,
    deterministic_terminal_event_id,
)
from .role_leases import (
    HolderKind,
    RoleClass,
    RoleLease,
    RoleLeaseError,
    RoleLeaseStatus,
    RoleLeaseStore,
    RoleScope,
    RuntimeLeaseProof,
)
from .task_subagents import (
    TASK_INSPECT_CAPABILITY,
    TASK_SPAWN_CAPABILITY,
    TASK_STEER_CAPABILITY,
    TASK_STOP_CAPABILITY,
    TaskSubagentError,
    TaskSubagentService,
)
from .task_worker_protocol import (
    TERMINAL_TASK_STATES,
    TaskSubagentParentContext,
    TaskSubagentSpec,
    TaskSubagentState,
    TaskWorkerCloseSummary,
    TaskWorkerProcessTreeState,
)

__all__ = ["TASK_WORKER_TOOL_NAMES", "TaskWorkerToolBridge"]


TASK_WORKER_TOOL_NAMES = frozenset(
    {
        "pulse_task_spawn",
        "pulse_task_wait",
        "pulse_task_steer",
        "pulse_task_stop",
    }
)
_CAPABILITIES = frozenset(
    {
        TASK_SPAWN_CAPABILITY,
        TASK_INSPECT_CAPABILITY,
        TASK_STEER_CAPABILITY,
        TASK_STOP_CAPABILITY,
    }
)
_MAX_FENCED_TURNS = 2_048


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda item: {"type": type(item).__name__},
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _model_control_content(data: Mapping[str, Any]) -> str:
    """Expose only the bounded control fields a parent model must act on."""

    fields = (
        "task_id",
        "state",
        "execution_status",
        "next_seq",
        "terminal",
        "timed_out",
        "gap",
        "output_delivered",
    )
    safe = {
        field: data[field]
        for field in fields
        if field in data
        and (
            isinstance(data[field], (str, int, bool))
            and not isinstance(data[field], float)
        )
    }
    return json.dumps(safe, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


class TaskWorkerToolBridge:
    """Connect Pi task tools to a real ``TaskSubagentService`` and E0."""

    def __init__(
        self,
        service: TaskSubagentService,
        *,
        operation_ledger: HarnessOperationLedger,
        event_store: Any,
        world_id: str,
        owner_id: str,
        epoch_provider: Callable[[], int],
        role_store: RoleLeaseStore,
    ) -> None:
        if not isinstance(service, TaskSubagentService):
            raise TypeError("service must be TaskSubagentService")
        if not isinstance(operation_ledger, HarnessOperationLedger):
            raise TypeError("operation_ledger must be HarnessOperationLedger")
        if not isinstance(world_id, str) or not world_id.strip():
            raise ValueError("world_id must be non-empty")
        if not isinstance(owner_id, str) or not owner_id.strip():
            raise ValueError("owner_id must be non-empty")
        if not callable(epoch_provider):
            raise TypeError("epoch_provider must be callable")
        if not isinstance(role_store, RoleLeaseStore):
            raise TypeError("role_store must be RoleLeaseStore")
        self._service = service
        self._ledger = operation_ledger
        self._event_store = event_store
        self._world_id = world_id.strip()
        self._owner_id = owner_id.strip()
        self._epoch_provider = epoch_provider
        self._role_store = role_store
        self._lock = threading.RLock()
        self._closed = False
        self._watching: set[str] = set()
        self._watcher_threads: dict[str, threading.Thread] = {}
        self._active_spawns: dict[
            str,
            tuple[HarnessOperation, TaskSubagentParentContext],
        ] = {}
        self._starting_spawns: set[str] = set()
        self._starting_threads: dict[str, threading.Thread] = {}
        self._close_owner_threads: dict[str, threading.Thread] = {}
        self._close_done = threading.Event()
        self._fenced_turns: OrderedDict[tuple[str, str], None] = OrderedDict()
        self._close_summary: TaskWorkerCloseSummary | None = None
        self._close_active_scopes: tuple[
            tuple[str, tuple[HarnessOperation, TaskSubagentParentContext]], ...
        ] = ()
        self._close_physical_unresolved = 0
        self._close_physical_owner_joined = True
        self._close_recovery_lock = threading.Lock()

    @property
    def service(self) -> TaskSubagentService:
        return self._service

    def dispatch(
        self,
        engram_id: str,
        tool_name: str,
        args: Mapping[str, Any],
        invocation: ToolInvocationContext,
        turn_id: str,
    ) -> dict[str, Any]:
        if (
            tool_name not in TASK_WORKER_TOOL_NAMES
            or not isinstance(invocation, ToolInvocationContext)
            or not isinstance(args, Mapping)
            or not isinstance(engram_id, str)
            or not engram_id.strip()
            or not isinstance(turn_id, str)
            or not turn_id.strip()
        ):
            return self._failure("task_request_invalid", invocation, tool_name)
        with self._lock:
            if self._closed:
                return self._failure("task_service_closed", invocation, tool_name)
            if (engram_id.strip(), turn_id.strip()) in self._fenced_turns:
                return self._failure(
                    "turn_authority_revoked",
                    invocation,
                    tool_name,
                )
        try:
            epoch = self._epoch_provider()
        except Exception:
            return self._failure("runtime_lease_lost", invocation, tool_name)
        if type(epoch) is not int or epoch < 1:
            return self._failure("runtime_lease_lost", invocation, tool_name)
        parent = TaskSubagentParentContext(
            world_id=self._world_id,
            engram_id=engram_id,
            turn_id=turn_id,
            epoch=epoch,
            capabilities=_CAPABILITIES,
        )
        try:
            if tool_name == "pulse_task_spawn":
                return self._spawn(args, invocation, parent)
            if tool_name == "pulse_task_wait":
                return self._wait(args, invocation, parent)
            if tool_name == "pulse_task_steer":
                return self._steer(args, invocation, parent)
            return self._stop(args, invocation, parent)
        except TaskSubagentError as exc:
            return self._failure(exc.code, invocation, tool_name)
        except (TypeError, ValueError):
            return self._failure("task_request_invalid", invocation, tool_name)
        except Exception:
            return self._failure("task_bridge_failed", invocation, tool_name)

    def _spawn(
        self,
        args: Mapping[str, Any],
        invocation: ToolInvocationContext,
        parent: TaskSubagentParentContext,
    ) -> dict[str, Any]:
        if set(args).difference({"task", "timeout", "idle_timeout"}):
            raise ValueError("unknown spawn field")
        task = args.get("task")
        if not isinstance(task, str) or not task.strip() or len(task) > 8192:
            raise ValueError("invalid task")
        timeout = args.get("timeout")
        idle = args.get("idle_timeout")
        spec = TaskSubagentSpec(
            task=task,
            timeout_sec=timeout,
            idle_timeout_sec=idle,
        )
        operation_id = invocation.tool_call_id
        operation = self._admit(
            "worker.spawn",
            operation_id,
            parent,
            effect={
                "task_digest": _digest(task),
                "timeout": timeout,
                "idle_timeout": idle,
            },
        )
        if isinstance(operation, dict):
            return operation
        task_id = "task_" + _digest(
            {
                "world_id": parent.world_id,
                "engram_id": parent.engram_id,
                "turn_id": parent.turn_id,
                "epoch": parent.epoch,
                "operation_id": operation_id,
            }
        )[:32]
        role = self._grant_task_role(
            task_id,
            parent,
            ttl_seconds=float(
                timeout
                if timeout is not None
                else self._service.config.default_timeout_sec
            )
            + 30.0,
        )
        if role is None:
            return self._fail_not_started(
                operation,
                parent,
                "task_role_grant_failed",
            )
        if not self._transition(operation, OperationPhase.STARTING):
            self._release_task_role(task_id, parent)
            return self._fail_not_started(operation, parent, "worker_ledger_start_failed")
        started = self._append(
            parent,
            HarnessEventStatus.RUNNING,
            {
                "action_request_id": operation_id,
                "task_id": task_id,
                "state": "starting",
                "execution_status": "starting",
                "task_digest": _digest(task),
                "role_lease_id": role.role_lease_id,
                "role_epoch": role.role_epoch,
                "evidence_class": "LIVE_GATE_UNVERIFIED",
            },
        )
        if started is None:
            self._release_task_role(task_id, parent)
            return self._fail_not_started(operation, parent, "worker_event_store_unavailable")
        if self._authorize_task_role(task_id, parent) is None:
            self._release_task_role(task_id, parent)
            return self._fail_not_started(operation, parent, "task_role_authorization_failed")
        with self._lock:
            if self._closed:
                self._release_task_role(task_id, parent)
                return self._settle_spawn_exception(
                    operation,
                    parent,
                    task_id,
                    "task_service_closed",
                )
            if (parent.engram_id, parent.turn_id) in self._fenced_turns:
                self._release_task_role(task_id, parent)
                return self._fail_not_started(
                    operation,
                    parent,
                    "turn_authority_revoked",
                )
            # Boundary persistence and active registration share the bridge
            # lock with stop_turn's fence+scan.  Therefore either the worker
            # is registered before the scan and is stopped, or it observes
            # the fence and can never call the backend.
            if not self._mark_boundary(operation):
                self._release_task_role(task_id, parent)
                return self._fail_not_started(
                    operation,
                    parent,
                    "worker_boundary_persistence_failed",
                )
            self._active_spawns[task_id] = (operation, parent)
            self._starting_spawns.add(task_id)
            self._starting_threads[task_id] = threading.current_thread()
        try:
            handle = self._service.spawn(spec, parent, task_id=task_id)
        except TaskSubagentError as exc:
            self._release_task_role(task_id, parent)
            return self._settle_spawn_exception(
                operation,
                parent,
                task_id,
                exc.code,
            )
        except Exception:
            self._release_task_role(task_id, parent)
            return self._settle_spawn_exception(
                operation,
                parent,
                task_id,
                "worker_backend_start_failed",
            )
        with self._lock:
            self._starting_spawns.discard(task_id)
            self._starting_threads.pop(task_id, None)
            turn_fenced = (parent.engram_id, parent.turn_id) in self._fenced_turns
            bridge_closed = self._closed
        if turn_fenced or bridge_closed:
            # stop_turn may fence while the backend start handshake is in
            # progress.  It deliberately does not issue a premature NOT_FOUND
            # stop for that reservation; the spawning thread owns this
            # post-start reconciliation and cannot return a live child.
            self._release_task_role(task_id, parent)
            if handle.state in TERMINAL_TASK_STATES:
                terminal = self._service.wait(task_id, parent_context=parent)
                terminal_event = self._settle_worker(
                    operation,
                    parent,
                    task_id,
                    terminal,
                )
                result = self._failure_code(
                    operation_id,
                    (
                        "task_service_closed"
                        if bridge_closed
                        else "turn_authority_revoked"
                    ),
                    "worker settled after its parent authority was withdrawn",
                )
                result["event_id"] = (
                    started.event_id
                    if terminal_event is None
                    else terminal_event.event_id
                )
                return result
            self._start_watcher(operation, parent, task_id)
            stopped = self._revoke_worker_after_role_loss(task_id, parent)
            result = self._failure_code(
                operation_id,
                (
                    "task_service_closed"
                    if bridge_closed
                    else "turn_authority_revoked"
                ),
                "worker start was reconciled against withdrawn parent authority",
            )
            result["event_id"] = stopped.get("event_id")
            result["data"]["stop_accepted"] = stopped.get("ok") is True
            result["data"]["uncertain"] = stopped.get("ok") is not True
            return result
        if handle.state in TERMINAL_TASK_STATES:
            terminal = self._service.wait(task_id, parent_context=parent)
            terminal_event = self._settle_worker(
                operation,
                parent,
                task_id,
                terminal,
            )
            content = "Temporary Pi worker settled during startup."
            data = {
                **handle.to_dict(),
                "action_request_id": operation_id,
                "execution_status": handle.state.value.casefold(),
                "role_lease_id": role.role_lease_id,
                "role_epoch": role.role_epoch,
            }
            if handle.state is TaskSubagentState.COMPLETED:
                delivered = self._service.delivery_content(
                    task_id,
                    parent_context=parent,
                )
                if delivered is not None:
                    content = delivered
                    data["output_delivered"] = True
            if data.get("output_delivered") is not True:
                content = _model_control_content(data)
            return {
                "ok": handle.state is TaskSubagentState.COMPLETED,
                "content": content,
                "data": data,
                "event_id": (
                    started.event_id
                    if terminal_event is None
                    else terminal_event.event_id
                ),
                **(
                    {}
                    if handle.state is TaskSubagentState.COMPLETED
                    else {"error": "worker_start_terminal"}
                ),
            }
        self._start_watcher(operation, parent, task_id)
        data = {
            **handle.to_dict(),
            "action_request_id": operation_id,
            "execution_status": "running",
            "role_lease_id": role.role_lease_id,
            "role_epoch": role.role_epoch,
        }
        return {
            "ok": True,
            "content": _model_control_content(data),
            "data": data,
            "event_id": started.event_id,
        }

    def _wait(
        self,
        args: Mapping[str, Any],
        invocation: ToolInvocationContext,
        parent: TaskSubagentParentContext,
    ) -> dict[str, Any]:
        if set(args).difference({"task_id", "after_seq", "timeout"}):
            raise ValueError("unknown wait field")
        task_id = args.get("task_id")
        if not isinstance(task_id, str) or not task_id.startswith("task_"):
            raise ValueError("invalid task id")
        after_seq = args.get("after_seq", 0)
        timeout = args.get("timeout", 0.0)
        result = self._service.wait(
            task_id,
            after_seq=after_seq,
            timeout=timeout,
            parent_context=parent,
        )
        data = result.to_dict()
        content = "Temporary Pi worker activity observed."
        if result.state is TaskSubagentState.COMPLETED:
            delivered = self._service.delivery_content(
                task_id,
                parent_context=parent,
            )
            if delivered is not None:
                content = delivered
                data["output_delivered"] = True
        if data.get("output_delivered") is not True:
            content = _model_control_content(data)
        return {
            "ok": result.state is not TaskSubagentState.NOT_FOUND,
            "content": content,
            "data": data,
            "event_id": None,
            **(
                {}
                if result.state is not TaskSubagentState.NOT_FOUND
                else {"error": "task_not_found"}
            ),
        }

    def _steer(
        self,
        args: Mapping[str, Any],
        invocation: ToolInvocationContext,
        parent: TaskSubagentParentContext,
    ) -> dict[str, Any]:
        if set(args) != {"task_id", "message"}:
            raise ValueError("invalid steer schema")
        task_id = args.get("task_id")
        message = args.get("message")
        if not isinstance(task_id, str) or not isinstance(message, str):
            raise ValueError("invalid steer values")
        effect = self._control_effect(task_id, "message", message)
        if self._ledger.get("worker.steer", invocation.tool_call_id) is not None:
            replay = self._admit(
                "worker.steer",
                invocation.tool_call_id,
                parent,
                effect=effect,
            )
            assert isinstance(replay, dict)
            return replay
        authority = self._authorize_task_role(task_id, parent)
        if authority is None:
            return self._failure_code(
                invocation.tool_call_id,
                "task_role_authorization_failed",
                "temporary worker role is no longer authoritative",
            )
        return self._execute_control(
            operation_kind="worker.steer",
            operation_name="steer",
            task_id=task_id,
            value=message,
            invocation=invocation,
            parent=parent,
            authority=authority,
            effect=effect,
        )

    def _stop(
        self,
        args: Mapping[str, Any],
        invocation: ToolInvocationContext,
        parent: TaskSubagentParentContext,
    ) -> dict[str, Any]:
        if set(args).difference({"task_id", "reason"}) or "task_id" not in args:
            raise ValueError("invalid stop schema")
        task_id = args.get("task_id")
        reason = args.get("reason", "parent requested stop")
        if not isinstance(task_id, str) or not isinstance(reason, str):
            raise ValueError("invalid stop values")
        effect = self._control_effect(task_id, "reason", reason)
        if self._ledger.get("worker.stop", invocation.tool_call_id) is not None:
            replay = self._admit(
                "worker.stop",
                invocation.tool_call_id,
                parent,
                effect=effect,
            )
            assert isinstance(replay, dict)
            return replay
        authority = self._authorize_task_role(task_id, parent)
        if authority is None:
            return self._failure_code(
                invocation.tool_call_id,
                "task_role_authorization_failed",
                "temporary worker role is no longer authoritative",
            )
        return self._execute_control(
            operation_kind="worker.stop",
            operation_name="stop",
            task_id=task_id,
            value=reason,
            invocation=invocation,
            parent=parent,
            authority=authority,
            effect=effect,
        )

    def _execute_control(
        self,
        *,
        operation_kind: str,
        operation_name: str,
        task_id: str,
        value: str,
        invocation: ToolInvocationContext,
        parent: TaskSubagentParentContext,
        authority: RoleLease | None,
        effect: Mapping[str, Any],
        revocation: bool = False,
    ) -> dict[str, Any]:
        operation = self._admit(
            operation_kind,
            invocation.tool_call_id,
            parent,
            effect=effect,
        )
        if isinstance(operation, dict):
            return operation
        if not self._transition(operation, OperationPhase.STARTING):
            return self._fail_not_started(
                operation,
                parent,
                "worker_control_ledger_start_failed",
            )
        started = self._append(
            parent,
            HarnessEventStatus.RUNNING,
            {
                "action_request_id": operation.operation_id,
                "task_id": task_id,
                "operation": operation_name,
                "state": "starting",
                "role_lease_id": (
                    self._task_role_id(task_id)
                    if authority is None
                    else authority.role_lease_id
                ),
                "role_epoch": None if authority is None else authority.role_epoch,
                "role_authority": "lost" if authority is None else "validated",
                "evidence_class": "LIVE_GATE_UNVERIFIED",
            },
        )
        if started is None:
            return self._fail_not_started(
                operation,
                parent,
                "worker_control_start_event_failed",
            )
        if revocation:
            try:
                runtime_authoritative = self._epoch_provider() == parent.epoch
            except Exception:
                runtime_authoritative = False
            if not runtime_authoritative or authority is not None:
                return self._fail_not_started(
                    operation,
                    parent,
                    "runtime_lease_lost",
                )
        else:
            refreshed_authority = self._authorize_task_role(task_id, parent)
            if (
                refreshed_authority is None
                or authority is None
                or refreshed_authority.role_epoch != authority.role_epoch
            ):
                return self._fail_not_started(
                    operation,
                    parent,
                    "task_role_authorization_failed",
                )
        with self._lock:
            if (
                not revocation
                and (parent.engram_id, parent.turn_id) in self._fenced_turns
            ):
                return self._fail_not_started(
                    operation,
                    parent,
                    "turn_authority_revoked",
                )
            # This is the linearization point against stop_turn's fence.  A
            # normal control either crosses its durable boundary first, after
            # which stop_turn revokes the worker, or it observes the fence and
            # never calls the backend.
            if not self._mark_boundary(operation):
                return self._fail_not_started(
                    operation,
                    parent,
                    "worker_control_boundary_failed",
                )
        try:
            if operation_name == "steer":
                result = self._service.steer(
                    task_id,
                    value,
                    invocation.tool_call_id,
                    parent.epoch,
                    parent_context=parent,
                )
            else:
                result = self._service.stop(
                    task_id,
                    value,
                    invocation.tool_call_id,
                    parent.epoch,
                    parent_context=parent,
                )
            if not self._transition(operation, OperationPhase.ADAPTER_RETURNED):
                raise RuntimeError("worker control ledger return transition failed")
            data = result.to_dict()
        except Exception:
            event = self._append_operation_terminal(
                operation,
                parent,
                HarnessEventStatus.UNCERTAIN,
                {
                    "action_request_id": operation.operation_id,
                    "task_id": task_id,
                    "operation": operation_name,
                    "state": "UNCERTAIN",
                    "accepted": False,
                    "terminal": True,
                    "error_code": "worker_control_uncertain",
                    "evidence_class": "LIVE_GATE_UNVERIFIED",
                },
                terminal_state=OperationTerminalState.UNCERTAIN,
            )
            return {
                "ok": False,
                "content": f"Temporary worker {operation_name} is uncertain.",
                "data": {
                    "action_request_id": operation.operation_id,
                    "task_id": task_id,
                    "state": "uncertain",
                    "execution_status": "uncertain",
                    "recovery_state": "uncertain",
                },
                "event_id": None if event is None else event.event_id,
                "error": "worker_control_uncertain",
            }
        accepted = data.get("accepted") is True
        uncertain = data.get("uncertain") is True
        event = self._append_operation_terminal(
            operation,
            parent,
            (
                HarnessEventStatus.UNCERTAIN
                if uncertain
                else HarnessEventStatus.COMPLETED
                if accepted
                else HarnessEventStatus.FAILED
            ),
            {
                "action_request_id": operation.operation_id,
                "task_id": task_id,
                "operation": operation_name,
                "state": data.get("state", "unknown"),
                "accepted": accepted,
                "uncertain": uncertain,
                "terminal": True,
                "error_code": data.get("error_code"),
                "role_lease_id": (
                    self._task_role_id(task_id)
                    if authority is None
                    else authority.role_lease_id
                ),
                "role_epoch": None if authority is None else authority.role_epoch,
                "role_authority": "lost" if authority is None else "validated",
                "evidence_class": data.get(
                    "evidence_class", "LIVE_GATE_UNVERIFIED"
                ),
            },
            terminal_state=(
                OperationTerminalState.UNCERTAIN
                if uncertain
                else OperationTerminalState.COMPLETED
            ),
        )
        if event is None:
            return {
                "ok": False,
                "content": f"Temporary worker {operation_name} lost its durable terminal.",
                "data": {
                    "action_request_id": operation.operation_id,
                    "task_id": task_id,
                    "state": "uncertain",
                    "execution_status": "uncertain",
                    "recovery_state": "uncertain",
                },
                "event_id": None,
                "error": "worker_control_terminal_persistence_failed",
            }
        data["action_request_id"] = operation.operation_id
        return self._control_result(
            data,
            operation_name,
            event_id=event.event_id,
        )

    def _control_effect(
        self,
        task_id: str,
        value_name: str,
        value: str,
    ) -> dict[str, Any]:
        role = self._role_store.get(self._task_role_id(task_id))
        return {
            "task_id": task_id,
            f"{value_name}_digest": _digest(value),
            "role_lease_id": self._task_role_id(task_id),
            "role_epoch": None if role is None else role.role_epoch,
        }

    @staticmethod
    def _control_result(
        data: dict[str, Any],
        operation: str,
        *,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        accepted = data.get("accepted") is True
        return {
            "ok": accepted,
            "content": f"Temporary worker {operation} {'accepted' if accepted else 'rejected'}.",
            "data": data,
            "event_id": event_id,
            **({} if accepted else {"error": data.get("error_code", "task_control_rejected")}),
        }

    @staticmethod
    def _task_role_id(task_id: str) -> str:
        return "role_" + _digest({"kind": "temporary-worker", "task_id": task_id})[:32]

    @staticmethod
    def _task_role_scope(task_id: str) -> RoleScope:
        return RoleScope(action_scope=f"task:{task_id}")

    def _runtime_proof(self, parent: TaskSubagentParentContext) -> RuntimeLeaseProof:
        return RuntimeLeaseProof(
            world_id=parent.world_id,
            owner_id=self._owner_id,
            epoch=parent.epoch,
        )

    def _grant_task_role(
        self,
        task_id: str,
        parent: TaskSubagentParentContext,
        *,
        ttl_seconds: float,
    ) -> RoleLease | None:
        role_id = self._task_role_id(task_id)
        scope = self._task_role_scope(task_id)
        try:
            existing = self._role_store.get(role_id)
            if existing is not None:
                if (
                    existing.world_id == parent.world_id
                    and existing.holder_kind is HolderKind.WORKER
                    and existing.holder_id == task_id
                    and existing.role_class is RoleClass.TASK_ROLE
                    and existing.scope.matches(scope)
                    and existing.runtime_owner_id == self._owner_id
                    and existing.runtime_epoch == parent.epoch
                    and existing.status is RoleLeaseStatus.ACTIVE
                ):
                    return existing
                return None
            return self._role_store.grant_new(
                world_id=parent.world_id,
                lineage_id=None,
                holder_kind=HolderKind.WORKER,
                holder_id=task_id,
                role_class=RoleClass.TASK_ROLE,
                role_label="temporary Pi task worker",
                scope=scope,
                issuer_kind=HolderKind.ENGRAM.value,
                issuer_id=parent.engram_id,
                runtime=self._runtime_proof(parent),
                ttl_seconds=min(24.0 * 60.0 * 60.0, max(1.0, ttl_seconds)),
                role_lease_id=role_id,
            )
        except RoleLeaseError:
            return None

    def _authorize_task_role(
        self,
        task_id: str,
        parent: TaskSubagentParentContext,
    ) -> Any | None:
        try:
            role = self._role_store.get(self._task_role_id(task_id))
            if role is None:
                return None
            return self._role_store.authorize(
                role.role_lease_id,
                holder_kind=HolderKind.WORKER,
                holder_id=task_id,
                expected_role_epoch=role.role_epoch,
                runtime=self._runtime_proof(parent),
                scope=self._task_role_scope(task_id),
            )
        except RoleLeaseError:
            return None

    def _release_task_role(
        self,
        task_id: str,
        parent: TaskSubagentParentContext,
    ) -> None:
        try:
            role = self._role_store.get(self._task_role_id(task_id))
            if role is None or role.status not in {
                RoleLeaseStatus.REQUESTED,
                RoleLeaseStatus.ACTIVE,
                RoleLeaseStatus.SUSPENDED,
            }:
                return
            self._role_store.release(
                role.role_lease_id,
                expected_role_epoch=role.role_epoch,
                runtime=self._runtime_proof(parent),
            )
        except RoleLeaseError:
            # A lost Runtime epoch already makes the role unusable.  Recovery
            # will revoke the stale task role without respawning its worker.
            return

    def _admit(
        self,
        kind: str,
        operation_id: str,
        parent: TaskSubagentParentContext,
        *,
        effect: Mapping[str, Any],
    ) -> HarnessOperation | dict[str, Any]:
        scope_digest = _digest(
            {
                "kind": kind,
                "operation_id": operation_id,
                "world_id": parent.world_id,
                "engram_id": parent.engram_id,
                "turn_id": parent.turn_id,
                "epoch": parent.epoch,
            }
        )
        effect_key = _digest(effect)
        existing = self._ledger.get(kind, operation_id)
        if existing is not None:
            if (
                existing.world_id != parent.world_id
                or existing.engram_id != parent.engram_id
                or existing.turn_id != parent.turn_id
                or existing.requested_epoch != parent.epoch
                or existing.scope_digest != scope_digest
                or existing.effect_key != effect_key
            ):
                return self._failure_code(
                    operation_id,
                    "worker_operation_scope_conflict",
                    "worker operation id is bound to another scope or task",
                )
            if existing.is_terminal:
                return self._replay(existing)
            return self._failure_code(
                operation_id,
                "operation_recovery_required",
                "worker operation is nonterminal and cannot be replayed",
            )
        try:
            return self._ledger.admit(
                kind,
                operation_id,
                world_id=parent.world_id,
                engram_id=parent.engram_id,
                turn_id=parent.turn_id,
                requested_epoch=parent.epoch,
                owner_id=self._owner_id,
                scope_digest=scope_digest,
                effect_key=effect_key,
            )
        except Exception:
            return self._failure_code(
                operation_id,
                "operation_ledger_unavailable",
                "worker operation admission failed closed",
            )

    def _transition(self, operation: HarnessOperation, phase: OperationPhase) -> bool:
        try:
            value = self._ledger.transition(
                operation.operation_kind,
                operation.operation_id,
                phase=phase,
                expected_epoch=operation.requested_epoch,
                owner_id=operation.owner_id,
            )
            return not value.is_terminal
        except Exception:
            return False

    def _mark_boundary(self, operation: HarnessOperation) -> bool:
        try:
            value = self._ledger.mark_boundary(
                operation.operation_kind,
                operation.operation_id,
                expected_epoch=operation.requested_epoch,
                owner_id=operation.owner_id,
            )
            return not value.is_terminal
        except Exception:
            return False

    def _start_watcher(
        self,
        operation: HarnessOperation,
        parent: TaskSubagentParentContext,
        task_id: str,
    ) -> None:
        with self._lock:
            if self._closed:
                return
            if task_id in self._watching:
                return
            self._watching.add(task_id)
        thread = threading.Thread(
            target=self._watch,
            args=(operation, parent, task_id),
            name=f"pulse-task-watch-{task_id[-8:]}",
            daemon=True,
        )
        with self._lock:
            if self._closed:
                self._watching.discard(task_id)
                return
            self._watcher_threads[task_id] = thread
            thread.start()

    def _watch(
        self,
        operation: HarnessOperation,
        parent: TaskSubagentParentContext,
        task_id: str,
    ) -> None:
        cursor = 0
        try:
            while True:
                with self._lock:
                    if self._closed:
                        return
                result = self._service.wait(
                    task_id,
                    after_seq=cursor,
                    timeout=min(0.25, self._service.config.max_wait_sec),
                    parent_context=parent,
                )
                cursor = max(cursor, result.next_seq)
                if result.terminal:
                    self._settle_worker(operation, parent, task_id, result)
                    return
                try:
                    runtime_epoch = self._epoch_provider()
                except Exception:
                    self.close(reason="runtime_lease_lost")
                    return
                if runtime_epoch != parent.epoch:
                    self.close(reason="runtime_lease_lost")
                    return
                if self._authorize_task_role(task_id, parent) is None:
                    self._revoke_worker_after_role_loss(task_id, parent)
                    terminal = self._service.wait(
                        task_id,
                        after_seq=cursor,
                        timeout=0.0,
                        parent_context=parent,
                    )
                    if terminal.terminal:
                        self._settle_worker(
                            operation,
                            parent,
                            task_id,
                            terminal,
                        )
                    return
        except Exception:
            # Leave the durable nonterminal row for successor recovery.
            return
        finally:
            with self._lock:
                self._watching.discard(task_id)
                self._watcher_threads.pop(task_id, None)

    def _revoke_worker_after_role_loss(
        self,
        task_id: str,
        parent: TaskSubagentParentContext,
    ) -> dict[str, Any]:
        """Stop one child under Runtime authority after its role is lost.

        A revoked/expired role cannot authorize its own stop.  This separate
        path is therefore fenced by the current Runtime epoch and gets its own
        durable ``worker.stop`` E0 operation.  It cannot steer or spawn.
        """

        reason = "temporary worker role authority was lost"
        operation_id = "role-loss-stop-" + _digest(
            {
                "task_id": task_id,
                "world_id": parent.world_id,
                "turn_id": parent.turn_id,
                "epoch": parent.epoch,
            }
        )[:32]
        return self._execute_control(
            operation_kind="worker.stop",
            operation_name="stop",
            task_id=task_id,
            value=reason,
            invocation=ToolInvocationContext(operation_id),
            parent=parent,
            authority=None,
            effect=self._control_effect(task_id, "reason", reason),
            revocation=True,
        )

    def stop_turn(
        self,
        engram_id: str,
        turn_id: str,
        *,
        reason: str = "turn authority revoked",
    ) -> dict[str, Any]:
        """Revoke and stop every temporary worker spawned by one Pi turn."""

        if not isinstance(engram_id, str) or not engram_id.strip():
            raise ValueError("engram_id must be non-empty")
        if not isinstance(turn_id, str) or not turn_id.strip():
            raise ValueError("turn_id must be non-empty")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be non-empty")
        turn_key = (engram_id.strip(), turn_id.strip())
        with self._lock:
            self._fenced_turns[turn_key] = None
            self._fenced_turns.move_to_end(turn_key)
            while len(self._fenced_turns) > _MAX_FENCED_TURNS:
                self._fenced_turns.popitem(last=False)
            active = tuple(
                (
                    task_id,
                    operation,
                    parent,
                    task_id in self._starting_spawns,
                )
                for task_id, (operation, parent) in self._active_spawns.items()
                if parent.engram_id == engram_id and parent.turn_id == turn_id
            )
        accepted = 0
        uncertain = 0
        task_ids: list[str] = []
        for task_id, operation, parent, starting in active:
            task_ids.append(task_id)
            self._release_task_role(task_id, parent)
            if starting:
                # The spawn thread owns the post-start stop.  Calling stop
                # before TaskSubagentService has installed its record could
                # cache NOT_FOUND and let the later backend escape.
                uncertain += 1
                continue
            result = self._revoke_worker_after_role_loss(
                task_id,
                parent,
            )
            terminal = self._service.wait(
                task_id,
                after_seq=0,
                timeout=0.0,
                parent_context=parent,
            )
            if terminal.terminal:
                self._settle_worker(
                    operation,
                    parent,
                    task_id,
                    terminal,
                )
            if result.get("ok") is True:
                accepted += 1
            else:
                uncertain += 1
        return {
            "configured": True,
            "workers_observed": len(active),
            "stop_accepted": accepted,
            "uncertain": uncertain > 0,
            "task_ids": task_ids,
        }

    def _settle_worker(self, operation, parent, task_id, result) -> Any | None:
        try:
            if self._epoch_provider() != parent.epoch:
                return None
        except Exception:
            return None
        authority = self._authorize_task_role(task_id, parent)
        uncertain = (
            result.state is TaskSubagentState.UNCERTAIN
            or result.gap
            or authority is None
        )
        event = self._append_operation_terminal(
            operation,
            parent,
            HarnessEventStatus.UNCERTAIN
            if uncertain
            else (
                HarnessEventStatus.COMPLETED
                if result.state is TaskSubagentState.COMPLETED
                else HarnessEventStatus.FAILED
            ),
            {
                "action_request_id": operation.operation_id,
                "task_id": task_id,
                "state": result.state.value,
                "execution_status": result.state.value.casefold(),
                "terminal": True,
                "gap": result.gap,
                "next_seq": result.next_seq,
                "role_lease_id": (
                    self._task_role_id(task_id)
                    if authority is None
                    else authority.role_lease_id
                ),
                "role_epoch": None if authority is None else authority.role_epoch,
                "role_authority": "lost" if authority is None else "validated",
                "evidence_class": result.evidence_class.value,
            },
            terminal_state=(
                OperationTerminalState.UNCERTAIN
                if uncertain
                else OperationTerminalState.COMPLETED
            ),
        )
        self._release_task_role(task_id, parent)
        current = self._ledger.get(operation.operation_kind, operation.operation_id)
        if current is not None and current.is_terminal:
            with self._lock:
                self._starting_spawns.discard(task_id)
                self._starting_threads.pop(task_id, None)
                self._active_spawns.pop(task_id, None)
        return event

    def _settle_spawn_exception(
        self,
        operation: HarnessOperation,
        parent: TaskSubagentParentContext,
        task_id: str,
        code: str,
    ) -> dict[str, Any]:
        """Record uncertainty when spawn raises after the durable boundary."""

        event = self._append_operation_terminal(
            operation,
            parent,
            HarnessEventStatus.UNCERTAIN,
            {
                "action_request_id": operation.operation_id,
                "task_id": task_id,
                "state": "UNCERTAIN",
                "execution_status": "uncertain",
                "terminal": True,
                "error_code": code,
                "evidence_class": "LIVE_GATE_UNVERIFIED",
            },
            terminal_state=OperationTerminalState.UNCERTAIN,
        )
        with self._lock:
            self._starting_spawns.discard(task_id)
            self._starting_threads.pop(task_id, None)
            self._active_spawns.pop(task_id, None)
        return {
            "ok": False,
            "content": "Temporary worker startup crossed its boundary but did not settle safely.",
            "data": {
                "action_request_id": operation.operation_id,
                "task_id": task_id,
                "state": "uncertain",
                "execution_status": "uncertain",
                "evidence_class": "LIVE_GATE_UNVERIFIED",
            },
            "event_id": None if event is None else event.event_id,
            "error": code,
        }

    def _append(
        self,
        parent: TaskSubagentParentContext,
        status: HarnessEventStatus,
        payload: Mapping[str, Any],
    ) -> Any | None:
        append = getattr(self._event_store, "append", None)
        if not callable(append):
            return None
        try:
            return append(
                HarnessEventDraft(
                    turn_id=parent.turn_id,
                    world_id=parent.world_id,
                    engram_id=parent.engram_id,
                    kind=HarnessEventKind.SUBAGENT_ACTIVITY,
                    phase=(
                        HarnessEventPhase.TERMINAL
                        if payload.get("terminal") is True
                        else HarnessEventPhase.STREAM
                    ),
                    source=HarnessEventSource.PULSE_CONTROL,
                    status=status,
                    payload=dict(payload),
                )
            )
        except Exception:
            return None

    def _append_operation_terminal(
        self,
        operation: HarnessOperation,
        parent: TaskSubagentParentContext,
        status: HarnessEventStatus,
        payload: Mapping[str, Any],
        *,
        terminal_state: OperationTerminalState,
        recovery_permit: RuntimeRecoveryPermit | None = None,
    ) -> Any | None:
        terminal_append = getattr(
            self._event_store,
            (
                "recover_terminal_operation"
                if recovery_permit is not None
                else "append_terminal_operation"
            ),
            None,
        )
        if not callable(terminal_append):
            return None
        draft = HarnessEventDraft(
            turn_id=parent.turn_id,
            world_id=parent.world_id,
            engram_id=parent.engram_id,
            kind=HarnessEventKind.SUBAGENT_ACTIVITY,
            phase=(
                HarnessEventPhase.RECOVERY
                if recovery_permit is not None
                else HarnessEventPhase.TERMINAL
            ),
            source=(
                HarnessEventSource.RECOVERY
                if recovery_permit is not None
                else HarnessEventSource.PULSE_CONTROL
            ),
            status=status,
            payload=dict(payload),
            event_id=deterministic_terminal_event_id(
                operation.operation_kind,
                operation.operation_id,
            ),
        )
        try:
            kwargs = {
                "ledger": self._ledger,
                "operation_kind": operation.operation_kind,
                "operation_id": operation.operation_id,
                "expected_epoch": operation.requested_epoch,
                "owner_id": operation.owner_id,
                "terminal_state": terminal_state,
            }
            if recovery_permit is not None:
                kwargs["recovery_permit"] = recovery_permit
            event, winner = terminal_append(draft, **kwargs)
            return event if winner.terminal_event_id == event.event_id else None
        except Exception:
            if recovery_permit is not None:
                # Recovery authority is deliberately accepted only by the
                # atomic recovery entry point above.  Never fall through to
                # an ordinary publication write after revocation.
                return None
            try:
                current = self._ledger.get(
                    operation.operation_kind,
                    operation.operation_id,
                )
                post_boundary = current is not None and current.phase in {
                    OperationPhase.BOUNDARY_ENTERED,
                    OperationPhase.ADAPTER_RETURNED,
                    OperationPhase.TERMINALIZING,
                }
                self._ledger.claim_terminal(
                    operation.operation_kind,
                    operation.operation_id,
                    expected_epoch=operation.requested_epoch,
                    owner_id=operation.owner_id,
                    terminal_state=(
                        OperationTerminalState.UNCERTAIN
                        if post_boundary
                        else terminal_state
                    ),
                    terminal_event_id=None,
                )
            except Exception:
                pass
            return None

    def _fail_not_started(
        self,
        operation: HarnessOperation,
        parent: TaskSubagentParentContext,
        code: str,
    ) -> dict[str, Any]:
        event = self._append_operation_terminal(
            operation,
            parent,
            HarnessEventStatus.FAILED,
            {
                "action_request_id": operation.operation_id,
                "state": "FAILED_NOT_STARTED",
                "execution_status": "not_started",
                "terminal": True,
                "error_code": code,
                "evidence_class": "LIVE_GATE_UNVERIFIED",
            },
            terminal_state=OperationTerminalState.FAILED_NOT_STARTED,
        )
        result = self._failure_code(operation.operation_id, code, "worker was not started")
        result["event_id"] = None if event is None else event.event_id
        return result

    def _replay(self, operation: HarnessOperation) -> dict[str, Any]:
        event = None
        if operation.terminal_event_id:
            get = getattr(self._event_store, "get", None)
            if callable(get):
                try:
                    event = get(operation.terminal_event_id)
                except Exception:
                    event = None
        if event is None or operation.recovery_state is not OperationRecoveryState.CLEARED:
            return self._failure_code(
                operation.operation_id,
                "operation_recovery_required",
                "worker terminal evidence requires reconciliation",
            )
        payload = event.payload if isinstance(event.payload, Mapping) else {}
        if operation.operation_kind in {"worker.steer", "worker.stop"}:
            operation_name = operation.operation_kind.removeprefix("worker.")
            data = {
                "action_request_id": operation.operation_id,
                "task_id": payload.get("task_id"),
                "request_id": operation.operation_id,
                "operation": operation_name,
                "accepted": payload.get("accepted") is True,
                "state": payload.get("state", "terminal"),
                "uncertain": payload.get("uncertain") is True,
                "error_code": payload.get("error_code"),
                "evidence_class": payload.get(
                    "evidence_class", "LIVE_GATE_UNVERIFIED"
                ),
                "idempotent": True,
            }
            replay = self._control_result(
                data,
                operation_name,
                event_id=event.event_id,
            )
            replay["idempotent"] = True
            return replay
        task_id = payload.get("task_id")
        successful = payload.get("state") == TaskSubagentState.COMPLETED.value
        return {
            "ok": successful,
            "content": "Replayed the durable worker terminal.",
            "data": {
                "action_request_id": operation.operation_id,
                "task_id": task_id,
                "state": payload.get("state", "terminal"),
                "execution_status": payload.get("execution_status", "terminal"),
                "idempotent": True,
            },
            "event_id": operation.terminal_event_id,
            "idempotent": True,
            **(
                {}
                if successful
                else {"error": "worker_terminal_replay"}
            ),
        }

    @staticmethod
    def _failure(code: str, invocation: Any, tool_name: str) -> dict[str, Any]:
        operation_id = (
            invocation.tool_call_id
            if isinstance(invocation, ToolInvocationContext)
            else "unknown"
        )
        return TaskWorkerToolBridge._failure_code(
            operation_id,
            code,
            f"{tool_name or 'task tool'} failed closed",
        )

    @staticmethod
    def _failure_code(operation_id: str, code: str, content: str) -> dict[str, Any]:
        return {
            "ok": False,
            "content": content,
            "data": {
                "action_request_id": operation_id,
                "state": "failed",
                "execution_status": "not_started",
                "evidence_class": "LIVE_GATE_UNVERIFIED",
            },
            "event_id": None,
            "error": code,
        }

    def close(
        self,
        *,
        reason: str = "runtime_shutdown",
        deadline: float | None = None,
        recovery_permit: RuntimeRecoveryPermit | None = None,
    ) -> TaskWorkerCloseSummary:
        """Stop the fleet and terminalize every admitted spawn exactly once.

        Lease-loss shutdown is a revocation-only path: it never starts or
        steers a child.  Because every tracked spawn has already crossed E0's
        durable adapter boundary, an unprovable shutdown is projected as
        ``UNCERTAIN`` under that operation's original immutable scope.
        """

        error_code = (
            reason
            if reason in {"runtime_lease_lost", "runtime_shutdown"}
            else "runtime_shutdown"
        )
        if deadline is not None and (
            type(deadline) not in (int, float)
            or not isfinite(float(deadline))
        ):
            raise ValueError("deadline must be a finite monotonic timestamp")
        if recovery_permit is not None:
            if not isinstance(recovery_permit, RuntimeRecoveryPermit):
                raise TypeError(
                    "recovery_permit must be a RuntimeRecoveryPermit or null"
                )
            if recovery_permit.owner_id != self._owner_id:
                raise ValueError(
                    "task worker recovery permit belongs to another Runtime owner"
                )
            recovery_permit.assert_recovery()
        close_deadline = (
            time.monotonic() + float(self._service.config.max_wait_sec)
            if deadline is None
            else float(deadline)
        )
        with self._lock:
            if self._close_summary is not None:
                cached_summary = self._close_summary
                close_owner = None
                active = self._close_active_scopes
                starting_threads = ()
                watcher_threads = ()
            else:
                cached_summary = None
                active = tuple(self._active_spawns.items())
                starting_threads = tuple(self._starting_threads.values())
                watcher_threads = tuple(self._watcher_threads.values())
                if recovery_permit is not None and any(
                    operation.owner_id != recovery_permit.owner_id
                    or operation.requested_epoch != recovery_permit.epoch
                    for _task_id, (operation, _parent) in active
                ):
                    raise ValueError(
                        "task worker recovery permit belongs to another operation epoch"
                    )
                if self._closed:
                    close_owner = False
                else:
                    close_owner = True
                    self._closed = True
                    self._close_active_scopes = active

        if cached_summary is not None:
            if recovery_permit is None:
                return cached_summary
            return self._retry_close_recovery(
                recovery_permit,
                deadline=close_deadline,
            )

        if not close_owner:
            self._close_done.wait(
                timeout=max(0.0, close_deadline - time.monotonic())
            )
            with self._lock:
                if self._close_summary is not None:
                    cached_summary = self._close_summary
                else:
                    cached_summary = None
            if cached_summary is not None:
                if recovery_permit is None:
                    return cached_summary
                return self._retry_close_recovery(
                    recovery_permit,
                    deadline=close_deadline,
                )
            return TaskWorkerCloseSummary(
                active_before=len(active),
                unresolved=max(1, len(active)),
                owner_joined=False,
                process_tree_state=TaskWorkerProcessTreeState.UNKNOWN,
                reason=error_code,
            )

        # TaskSubagentService performs the fleet-wide broadcast in its own
        # owner.  Durable terminalization starts as soon as that broadcast
        # barrier opens, in parallel with physical owner observation.
        service_result: dict[str, TaskWorkerCloseSummary] = {}

        def close_service() -> None:
            service_result["summary"] = self._service.close(
                deadline=close_deadline,
                reason=error_code,
            )

        service_thread = threading.Thread(
            target=close_service,
            name="pulse-task-service-close",
            daemon=True,
        )
        service_thread.start()
        self._service.wait_for_close_broadcast(deadline=close_deadline)
        settled_ids: set[str] = set()
        settled_lock = threading.Lock()
        settlement_threads: dict[str, threading.Thread] = {}
        for task_id, (operation, parent) in active:
            thread = threading.Thread(
                target=self._settle_close_operation,
                args=(
                    task_id,
                    operation,
                    parent,
                    error_code,
                    settled_ids,
                    settled_lock,
                    recovery_permit,
                ),
                name=f"pulse-task-durable-close-{task_id[-10:]}",
                daemon=True,
            )
            settlement_threads[task_id] = thread
            with self._lock:
                self._close_owner_threads[task_id] = thread
            thread.start()

        observed_owners = (
            (service_thread,)
            + tuple(settlement_threads.values())
            + watcher_threads
            + starting_threads
        )
        for owner in observed_owners:
            if owner is threading.current_thread():
                continue
            try:
                owner.join(timeout=max(0.0, close_deadline - time.monotonic()))
            except RuntimeError:
                # A start owner registered before Thread.start is itself
                # unresolved; never upgrade that wrapper to joined.
                pass

        service_summary = service_result.get("summary")
        if service_summary is None:
            service_summary = TaskWorkerCloseSummary(
                active_before=len(active),
                unresolved=max(1, len(active)),
                owner_joined=False,
                process_tree_state=TaskWorkerProcessTreeState.UNKNOWN,
                reason=error_code,
            )
        alive_owners = sum(
            owner is threading.current_thread() or owner.is_alive()
            for owner in observed_owners
        )
        with settled_lock:
            settled = len(settled_ids)
        durable_unresolved = max(0, len(active) - settled)
        physical_unresolved = service_summary.unresolved + alive_owners
        unresolved = physical_unresolved + durable_unresolved
        physical_owner_joined = (
            service_summary.owner_joined and physical_unresolved == 0
        )
        owner_joined = physical_owner_joined and unresolved == 0
        summary = TaskWorkerCloseSummary(
            active_before=max(len(active), service_summary.active_before),
            unresolved=unresolved,
            owner_joined=owner_joined,
            process_tree_state=service_summary.process_tree_state,
            cancellation_requested=service_summary.cancellation_requested,
            terminal_observed=service_summary.terminal_observed,
            spawn_operations_settled_uncertain=settled,
            reason=error_code,
        )
        with self._lock:
            self._close_physical_unresolved = physical_unresolved
            self._close_physical_owner_joined = physical_owner_joined
            self._close_summary = summary
            self._close_done.set()
        return summary

    def _retry_close_recovery(
        self,
        recovery_permit: RuntimeRecoveryPermit,
        *,
        deadline: float,
    ) -> TaskWorkerCloseSummary:
        """Monotonically fill E0 winners without touching worker owners."""

        with self._close_recovery_lock:
            with self._lock:
                cached = self._close_summary
                active = self._close_active_scopes
                physical_unresolved = self._close_physical_unresolved
                physical_owner_joined = self._close_physical_owner_joined
            if cached is None:
                raise RuntimeError("task worker close has not produced a summary")
            if any(
                operation.owner_id != recovery_permit.owner_id
                or operation.requested_epoch != recovery_permit.epoch
                for _task_id, (operation, _parent) in active
            ):
                raise ValueError(
                    "task worker recovery permit belongs to another operation epoch"
                )

            durable_unresolved = 0
            settled_uncertain = 0
            for index, (task_id, (operation, parent)) in enumerate(active):
                if time.monotonic() >= deadline:
                    durable_unresolved += len(active) - index
                    break
                settled, uncertain = self._recover_close_operation(
                    task_id,
                    operation,
                    parent,
                    recovery_permit,
                )
                if settled:
                    settled_uncertain += int(uncertain)
                else:
                    durable_unresolved += 1

            unresolved = physical_unresolved + durable_unresolved
            final_unresolved = min(cached.unresolved, unresolved)
            updated = TaskWorkerCloseSummary(
                active_before=cached.active_before,
                unresolved=final_unresolved,
                owner_joined=(
                    cached.owner_joined
                    or physical_owner_joined and final_unresolved == 0
                ),
                process_tree_state=cached.process_tree_state,
                cancellation_requested=cached.cancellation_requested,
                terminal_observed=max(
                    cached.terminal_observed,
                    len(active) - durable_unresolved,
                ),
                spawn_operations_settled_uncertain=max(
                    cached.spawn_operations_settled_uncertain,
                    settled_uncertain,
                ),
                reason=cached.reason,
            )
            with self._lock:
                current = self._close_summary
                if current is not None and (
                    current.unresolved < updated.unresolved
                    or (
                        current.unresolved == updated.unresolved
                        and current.spawn_operations_settled_uncertain
                        > updated.spawn_operations_settled_uncertain
                    )
                ):
                    return current
                self._close_summary = updated
                return updated

    def _recover_close_operation(
        self,
        task_id: str,
        operation: HarnessOperation,
        parent: TaskSubagentParentContext,
        recovery_permit: RuntimeRecoveryPermit,
    ) -> tuple[bool, bool]:
        """Recover one immutable operation scope; never call the worker."""

        current = self._ledger.get(
            operation.operation_kind,
            operation.operation_id,
        )
        if current is None:
            return False, False
        terminal_state = (
            current.terminal_state
            if current.terminal_state is not None
            else OperationTerminalState.UNCERTAIN
        )
        if (
            current.is_terminal
            and current.terminal_event_id is not None
            and current.recovery_state is OperationRecoveryState.CLEARED
        ):
            return True, terminal_state is OperationTerminalState.UNCERTAIN
        status = {
            OperationTerminalState.COMPLETED: HarnessEventStatus.COMPLETED,
            OperationTerminalState.FAILED_NOT_STARTED: HarnessEventStatus.FAILED,
            OperationTerminalState.CANCELLED_NOT_STARTED: (
                HarnessEventStatus.CANCELLED
            ),
            OperationTerminalState.UNCERTAIN: HarnessEventStatus.UNCERTAIN,
        }[terminal_state]
        self._append_operation_terminal(
            operation,
            parent,
            status,
            {
                "action_request_id": operation.operation_id,
                "task_id": task_id,
                "state": terminal_state.value,
                "execution_status": (
                    "uncertain"
                    if terminal_state is OperationTerminalState.UNCERTAIN
                    else terminal_state.value.casefold()
                ),
                "terminal": True,
                "owner_joined": False,
                "recovery_state": "recovered",
                "error_code": "runtime_shutdown_recovery",
                "evidence_class": "LIVE_GATE_UNVERIFIED",
            },
            terminal_state=terminal_state,
            recovery_permit=recovery_permit,
        )
        winner = self._ledger.get(
            operation.operation_kind,
            operation.operation_id,
        )
        settled = (
            winner is not None
            and winner.is_terminal
            and winner.terminal_event_id is not None
            and winner.recovery_state is OperationRecoveryState.CLEARED
        )
        return (
            settled,
            bool(
                settled
                and winner is not None
                and winner.terminal_state is OperationTerminalState.UNCERTAIN
            ),
        )

    def _settle_close_operation(
        self,
        task_id: str,
        operation: HarnessOperation,
        parent: TaskSubagentParentContext,
        error_code: str,
        settled_ids: set[str],
        settled_lock: threading.Lock,
        recovery_permit: RuntimeRecoveryPermit | None,
    ) -> None:
        settled = False
        try:
            current = self._ledger.get(
                operation.operation_kind,
                operation.operation_id,
            )
            if current is not None and not current.is_terminal:
                result = self._service.wait(
                    task_id,
                    after_seq=0,
                    timeout=0.0,
                    parent_context=parent,
                )
                event = self._append_operation_terminal(
                    operation,
                    parent,
                    HarnessEventStatus.UNCERTAIN,
                    {
                        "action_request_id": operation.operation_id,
                        "task_id": task_id,
                        "state": TaskSubagentState.UNCERTAIN.value,
                        "observed_worker_state": (
                            result.state.value
                            if result.terminal
                            else TaskSubagentState.UNCERTAIN.value
                        ),
                        "owner_joined": False,
                        "execution_status": "uncertain",
                        "terminal": True,
                        "gap": result.gap,
                        "error_code": error_code,
                        "role_lease_id": self._task_role_id(task_id),
                        "role_authority": "revoked",
                        "evidence_class": result.evidence_class.value,
                    },
                    terminal_state=OperationTerminalState.UNCERTAIN,
                    recovery_permit=recovery_permit,
                )
                winner = self._ledger.get(
                    operation.operation_kind,
                    operation.operation_id,
                )
                settled = event is not None or (
                    winner is not None
                    and winner.terminal_state
                    is OperationTerminalState.UNCERTAIN
                )
            elif current is not None and current.is_terminal:
                settled = True
            self._release_task_role(task_id, parent)
        finally:
            if settled:
                with settled_lock:
                    settled_ids.add(task_id)
            with self._lock:
                self._starting_spawns.discard(task_id)
                self._starting_threads.pop(task_id, None)
                self._active_spawns.pop(task_id, None)
                self._close_owner_threads.pop(task_id, None)

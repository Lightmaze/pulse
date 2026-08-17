"""The single server-side action seam for mutable Pi tools.

Pi remains the only model/tool/settle loop.  This module is deliberately a
small L3 boundary: it receives a tool call that has already crossed the
per-process bearer Gateway, evaluates the Pulse policy, creates an approval
request when required, and records a redacted Harness event.  It does not
spawn a process and it never falls back to an unrestricted local executor.

A deployment may supply a real ``ProcessSandboxBackend``.  Until that happens an
otherwise approved action returns ``sandbox_backend_unavailable`` and the
Workbench can distinguish that fact from an approval grant.
"""

from __future__ import annotations

import hashlib
import json
import shlex
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import Executor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .approvals import ApprovalRegistry, ApprovalResolution, ApprovalState
from .events import (
    HarnessEventDraft,
    HarnessEventKind,
    HarnessEventPhase,
    HarnessEventSource,
    HarnessEventStatus,
)
from .security import (
    ApprovalMode,
    CommandScope,
    CONTRACT_ONLY,
    ExecutionPolicy,
    FilesystemAccess,
    LIVE_GATE_UNVERIFIED,
    LIVE_OS_RESTRICTED,
    LIVE_WORKSPACE_CHECKPOINTED,
    NetworkAccess,
    PolicyContext,
    PolicyDecision,
    PolicyRequest,
)
from .operations import (
    HarnessOperation,
    HarnessOperationLedger,
    OperationPhase,
    OperationRecoveryState,
    OperationScopeCollisionError,
    OperationTerminalState,
    deterministic_terminal_event_id,
)
from .terminal import ProcessSpec, TerminalState
from .terminal_sessions import (
    TerminalSessionError,
    TerminalSessionService,
    TerminalSessionStartError,
    TerminalSessionSummary,
)

__all__ = [
    "ActionCancellationToken",
    "HarnessActionBroker",
    "HarnessActionError",
    "MUTABLE_PI_TOOL_NAMES",
    "RoutedActionBackend",
    "TerminalSessionActionBackend",
]


MUTABLE_PI_TOOL_NAMES = frozenset(
    {"bash", "edit", "write", "pulse_mcp_call"}
)
_MAX_ACTIONS = 1024
_DEFAULT_ACTION_TIMEOUT_SECONDS = 60.0
_MAX_ACTION_TIMEOUT_SECONDS = 300.0
_ACTION_TERMINAL_WAIT_SECONDS = 5.0
_ACTIVE_ACTION_STATES = frozenset({"starting", "running"})
_TERMINALIZING_ACTION_STATES = frozenset(
    {"terminalizing", "terminalizing_uncertain"}
)
_PIPE_BACKGROUND_BACKEND_IMPLEMENTATION = (
    "codex_cli_pipe_process.windows_job.v1"
)
_PIPE_BACKGROUND_BINDING_REQUIRED = {
    "transport": "pipe",
    "session_scope": "runtime_connection",
    "sandbox_evidence": LIVE_OS_RESTRICTED,
    "tree_containment": "JOB_OBJECT_VERIFIED",
    "workspace_write_denied": "DENIED_VERIFIED",
    "environment_sentinel": "NOT_LEAKED_VERIFIED",
    "background_lifecycle": "VERIFIED",
    "backend_implementation": _PIPE_BACKGROUND_BACKEND_IMPLEMENTATION,
}


def _background_evidence_binding_is_complete(value: Any) -> bool:
    """Require both sandbox and lifecycle identities for live PIPE evidence."""

    if not isinstance(value, Mapping):
        return False
    if any(value.get(key) != expected for key, expected in _PIPE_BACKGROUND_BINDING_REQUIRED.items()):
        return False
    return all(
        isinstance(value.get(key), str) and bool(value[key].strip())
        for key in ("sandbox_gate_id", "lifecycle_gate_id")
    )


@dataclass(frozen=True, slots=True)
class _ActionScope:
    """The complete namespace for one Pi mutable tool request.

    ``tool_call_id`` is only unique inside one Pi turn in practice.  Keeping
    the world, Engram, turn and lease epoch in the in-memory key prevents a
    stale or colliding request from borrowing another subject's approval,
    input, result or running cancellation token.
    """

    world_id: str
    engram_id: str
    turn_id: str
    epoch: int
    action_request_id: str
    tool_name: str


class ActionCancellationToken:
    """Small thread-safe cancellation signal passed to a sandbox adapter."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._reason: str | None = None

    def cancel(self, reason: str = "cancelled") -> bool:
        with self._lock:
            if self._event.is_set():
                return False
            self._reason = reason[:128] if isinstance(reason, str) else "cancelled"
            self._event.set()
            return True

    def is_set(self) -> bool:
        return self._event.is_set()

    @property
    def reason(self) -> str | None:
        with self._lock:
            return self._reason


class ProcessSandboxBackend(Protocol):
    """The narrow execution seam for a sandboxed process backend.

    The protocol is intentionally not implemented here.  An adapter must be
    selected and preflighted by the owning Runtime before it is callable.
    """

    @property
    def evidence_class(self) -> str: ...

    @property
    def evidence_binding(self) -> Mapping[str, str]: ...

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
    ) -> Mapping[str, Any]: ...


class RoutedActionBackend:
    """Route each mutable tool to one explicitly selected live adapter.

    A read-only command sandbox and a checkpointed file adapter prove
    different things.  This router keeps those evidence bindings per tool
    instead of flattening them into one misleading aggregate capability.
    """

    def __init__(self, routes: Mapping[str, ProcessSandboxBackend]) -> None:
        selected = dict(routes)
        if not selected or any(
            name not in MUTABLE_PI_TOOL_NAMES
            or backend is None
            or not callable(getattr(backend, "execute", None))
            for name, backend in selected.items()
        ):
            raise ValueError("routes must bind mutable tool names to execution adapters")
        self._routes = selected

    @property
    def evidence_class(self) -> str:
        # A single bound adapter preserves its old aggregate evidence.  Mixed
        # adapters prove different capabilities, so aggregate inspection must
        # remain unverified and execution asks evidence_for(tool_name).
        values = {self.evidence_for(name) for name in self._routes}
        return values.pop() if len(values) == 1 else LIVE_GATE_UNVERIFIED

    @property
    def evidence_binding(self) -> Mapping[str, str]:
        bindings = [dict(self.evidence_binding_for(name)) for name in self._routes]
        if bindings and all(binding == bindings[0] for binding in bindings[1:]):
            return bindings[0]
        return {}

    def evidence_for(
        self,
        tool_name: str,
        execution_context: Mapping[str, Any] | None = None,
    ) -> str:
        backend = self._routes.get(tool_name)
        if backend is None:
            return LIVE_GATE_UNVERIFIED
        try:
            contextual = getattr(backend, "evidence_for", None)
            value = (
                contextual(tool_name, execution_context)
                if callable(contextual)
                else backend.evidence_class
            )
        except Exception:
            return LIVE_GATE_UNVERIFIED
        return value if isinstance(value, str) else LIVE_GATE_UNVERIFIED

    def evidence_binding_for(
        self,
        tool_name: str,
        execution_context: Mapping[str, Any] | None = None,
    ) -> Mapping[str, str]:
        backend = self._routes.get(tool_name)
        if backend is None:
            return {}
        try:
            contextual = getattr(backend, "evidence_binding_for", None)
            value = (
                contextual(tool_name, execution_context)
                if callable(contextual)
                else backend.evidence_binding
            )
        except Exception:
            return {}
        return value if isinstance(value, Mapping) else {}

    def execute(self, *, tool_name: str, **kwargs: Any) -> Mapping[str, Any]:
        backend = self._routes.get(tool_name)
        if backend is None:
            return {
                "ok": False,
                "error": "sandbox_tool_not_supported",
                "status": "failed",
                "execution_status": "not_started",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        return backend.execute(tool_name=tool_name, **kwargs)

    def supports_progress_for(self, tool_name: str) -> bool:
        backend = self._routes.get(tool_name)
        return bool(getattr(backend, "supports_progress", False))

    def preview_for(
        self,
        tool_name: str,
        input_data: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        backend = self._routes.get(tool_name)
        preview = None if backend is None else getattr(backend, "preview_for", None)
        if not callable(preview):
            return {}
        value = preview(tool_name, input_data)
        return value if isinstance(value, Mapping) else {}


class TerminalSessionActionBackend:
    """Route approved bash calls to foreground or durable PIPE execution.

    This adapter does not make an authorization decision.  It is reachable
    only behind :class:`HarnessActionBroker`, and the manager receives an
    explicit one-shot allow decision after that broker has resolved policy
    and approval.  ``background`` never falls through to the foreground
    adapter when the durable session service is absent.
    """

    def __init__(
        self,
        foreground_backend: ProcessSandboxBackend,
        session_service: TerminalSessionService,
        *,
        background_backend: Any | None = None,
    ) -> None:
        if not callable(getattr(foreground_backend, "execute", None)):
            raise TypeError("foreground_backend must implement execute")
        if not isinstance(session_service, TerminalSessionService):
            raise TypeError("session_service must be a TerminalSessionService")
        self._foreground = foreground_backend
        self._sessions = session_service
        self._background = background_backend

    @property
    def evidence_class(self) -> str:
        value = getattr(self._foreground, "evidence_class", LIVE_GATE_UNVERIFIED)
        return value if isinstance(value, str) else LIVE_GATE_UNVERIFIED

    @property
    def evidence_binding(self) -> Mapping[str, str]:
        value = getattr(self._foreground, "evidence_binding", {})
        return value if isinstance(value, Mapping) else {}

    @staticmethod
    def _background_requested(
        execution_context: Mapping[str, Any] | None,
    ) -> bool:
        if not isinstance(execution_context, Mapping):
            return False
        return (
            execution_context.get("background") is True
            or execution_context.get("execution_mode")
            == "background_pipe_session"
        )

    def evidence_for(
        self,
        tool_name: str,
        execution_context: Mapping[str, Any] | None = None,
    ) -> str:
        if tool_name != "bash" or not self._background_requested(
            execution_context
        ):
            return self.evidence_class
        backend = self._background
        try:
            evidence = getattr(backend, "evidence_class")
            binding = getattr(backend, "evidence_binding")
        except Exception:
            return LIVE_GATE_UNVERIFIED
        if (
            evidence != LIVE_OS_RESTRICTED
            or not _background_evidence_binding_is_complete(binding)
        ):
            return LIVE_GATE_UNVERIFIED
        return LIVE_OS_RESTRICTED

    def evidence_binding_for(
        self,
        tool_name: str,
        execution_context: Mapping[str, Any] | None = None,
    ) -> Mapping[str, str]:
        if tool_name != "bash" or not self._background_requested(
            execution_context
        ):
            return self.evidence_binding
        if self.evidence_for(tool_name, execution_context) != LIVE_OS_RESTRICTED:
            return {}
        try:
            binding = getattr(self._background, "evidence_binding")
        except Exception:
            return {}
        return dict(binding) if isinstance(binding, Mapping) else {}

    @property
    def supports_progress(self) -> bool:
        return bool(getattr(self._foreground, "supports_progress", False))

    def preview_for(
        self,
        tool_name: str,
        input_data: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        if tool_name == "bash" and input_data.get("background") is True:
            return {
                "execution_mode": "background_pipe_session",
                "transport": "pipe",
                "session_scope": "runtime_connection",
            }
        preview = getattr(self._foreground, "preview_for", None)
        if not callable(preview):
            return {}
        value = preview(tool_name, input_data)
        return value if isinstance(value, Mapping) else {}

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
        progress_callback: Callable[[Mapping[str, Any]], Any] | None = None,
    ) -> Mapping[str, Any]:
        if tool_name != "bash" or input_data.get("background") is not True:
            return self._foreground.execute(
                action_request_id=action_request_id,
                engram_id=engram_id,
                turn_id=turn_id,
                epoch=epoch,
                tool_name=tool_name,
                input_data=input_data,
                policy_preview=policy_preview,
                signal=signal,
                progress_callback=progress_callback,
            )
        if self._signal_is_set(signal):
            return self._failure(
                "sandbox_cancelled",
                execution_status="cancelled",
            )
        command = input_data.get("command")
        if not isinstance(command, str):
            return self._failure("sandbox_command_invalid")
        try:
            argv = tuple(
                part for part in shlex.split(command, posix=False) if part.strip()
            )
        except ValueError:
            argv = ()
        if not argv:
            return self._failure("sandbox_command_invalid")
        timeout = input_data.get("timeout", _DEFAULT_ACTION_TIMEOUT_SECONDS)
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 0 < float(timeout) <= _MAX_ACTION_TIMEOUT_SECONDS
        ):
            return self._failure("sandbox_timeout_invalid")
        launch_digest = hashlib.sha256(
            (
                f"{self._sessions.world_id}\0{engram_id}\0{turn_id}\0"
                f"{epoch}\0{action_request_id}"
            ).encode("utf-8")
        ).hexdigest()
        spec = ProcessSpec(
            turn_id=turn_id,
            argv=argv,
            cwd=".",
            foreground=False,
            timeout_sec=float(timeout),
            allow_stdin=False,
            env={},
            request_id=action_request_id,
        )
        try:
            summary = self._sessions.start_background(
                spec,
                engram_id=engram_id,
                policy_context={
                    "allow": True,
                    "requires_approval": False,
                    "reason_code": "harness_approval_resolved",
                    "safe_preview": dict(policy_preview),
                    "evidence_class": LIVE_GATE_UNVERIFIED,
                },
                launch_action_digest=launch_digest,
            )
        except TerminalSessionStartError as exc:
            return self._summary_failure(exc.summary, exc.error_code)
        except TerminalSessionError as exc:
            return self._failure(
                "terminal_session_unavailable",
                recovery_state=(
                    "uncertain"
                    if "lease" in type(exc).__name__.casefold()
                    else "none"
                ),
            )
        if self._signal_is_set(signal):
            request_id = hashlib.sha256(
                f"cancel\0{launch_digest}".encode("utf-8")
            ).hexdigest()
            try:
                stopped = self._sessions.stop(
                    summary.terminal_session_id,
                    request_id=request_id,
                    expected_epoch=summary.epoch,
                    expected_engram_id=summary.engram_id,
                    expected_turn_id=summary.turn_id,
                    reason="action_cancelled_after_start",
                )
                return self._summary_failure(
                    stopped.summary,
                    stopped.error_code or "sandbox_cancelled",
                )
            except TerminalSessionError:
                return self._summary_failure(
                    summary,
                    "sandbox_cancellation_uncertain",
                    recovery_state="uncertain",
                )
        if summary.state is not TerminalState.RUNNING:
            return self._summary_failure(
                summary,
                summary.error_code or "background_process_not_running",
            )
        safe = self._summary_fields(summary)
        evidence = self.evidence_for(tool_name, input_data)
        binding = self.evidence_binding_for(tool_name, input_data)
        session_binding_matches = (
            evidence == LIVE_OS_RESTRICTED
            and summary.evidence_class == evidence
            and summary.sandbox_evidence == binding.get("sandbox_evidence")
            and summary.tree_containment.value == binding.get("tree_containment")
            and summary.mode.value == "PIPE_SESSION"
            and summary.transport.value == binding.get("transport")
            and summary.session_scope.value == binding.get("session_scope")
        )
        return {
            "ok": True,
            "status": "completed",
            "execution_status": "completed",
            "evidence_class": (
                evidence
                if session_binding_matches
                else LIVE_GATE_UNVERIFIED
            ),
            **(
                {"evidence_binding": dict(binding)}
                if session_binding_matches
                else {}
            ),
            **safe,
            "ephemeral_content": (
                "Started non-interactive PIPE session "
                f"{summary.terminal_session_id}."
            ),
        }

    @staticmethod
    def _signal_is_set(signal: Any) -> bool:
        check = getattr(signal, "is_set", None)
        if not callable(check):
            return False
        try:
            return check() is True
        except Exception:
            return True

    @staticmethod
    def _summary_fields(summary: TerminalSessionSummary) -> dict[str, Any]:
        return {
            "linked_terminal_session_id": summary.terminal_session_id,
            "mode": summary.mode.value,
            "transport": summary.transport.value,
            "session_scope": summary.session_scope.value,
            "session_state": summary.state.value,
            "command_digest": summary.command_digest,
            "sandbox_evidence": summary.sandbox_evidence,
            "tree_containment": summary.tree_containment.value,
        }

    def _summary_failure(
        self,
        summary: TerminalSessionSummary,
        error_code: str,
        *,
        recovery_state: str | None = None,
    ) -> Mapping[str, Any]:
        uncertain = summary.state is TerminalState.UNCERTAIN or (
            recovery_state == "uncertain"
        )
        return {
            "ok": False,
            "error": error_code,
            "status": "uncertain" if uncertain else "failed",
            "execution_status": "uncertain" if uncertain else "not_started",
            "recovery_state": "uncertain" if uncertain else "none",
            "evidence_class": LIVE_GATE_UNVERIFIED,
            **self._summary_fields(summary),
        }

    @staticmethod
    def _failure(
        error_code: str,
        *,
        execution_status: str = "not_started",
        recovery_state: str = "none",
    ) -> Mapping[str, Any]:
        return {
            "ok": False,
            "error": error_code,
            "status": (
                "uncertain" if recovery_state == "uncertain" else "failed"
            ),
            "execution_status": execution_status,
            "recovery_state": recovery_state,
            "evidence_class": LIVE_GATE_UNVERIFIED,
        }

class HarnessActionError(RuntimeError):
    """Safe structured refusal used by the API control adapter."""

    def __init__(self, code: str, detail: str, remedy: str, *, status: int = 503):
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail
        self.remedy = remedy
        self.status = status


class HarnessActionBroker:
    """Fence-aware policy and approval broker for ``bash/edit/write``.

    The registry is bounded and in-process; its durable observation is the
    ``harness_events`` projection.  Raw command text and file content are not
    retained in the action result or event payload.
    """

    def __init__(
        self,
        *,
        workspace_root: str | Path,
        world_id: str,
        event_store: Any,
        epoch_provider: Callable[[], int],
        approval_registry: ApprovalRegistry | None = None,
        policy: ExecutionPolicy | None = None,
        command_allowlist: tuple[str, ...] = (),
        backend: ProcessSandboxBackend | None = None,
        execution_executor: Executor | None = None,
        operation_ledger: HarnessOperationLedger | None = None,
        owner_id: str | None = None,
        max_actions: int = _MAX_ACTIONS,
    ) -> None:
        if type(max_actions) is not int or max_actions < 16:
            raise ValueError("max_actions must be an integer >= 16")
        self._workspace = Path(workspace_root).expanduser().resolve()
        if not self._workspace.is_dir():
            raise ValueError("workspace_root must be an existing directory")
        if not isinstance(world_id, str) or not world_id.strip():
            raise ValueError("world_id must be a non-empty string")
        if not callable(epoch_provider):
            raise TypeError("epoch_provider must be callable")
        self._world_id = world_id.strip()
        self._event_store = event_store
        self._epoch_provider = epoch_provider
        self._approvals = approval_registry or ApprovalRegistry()
        self._policy = policy or ExecutionPolicy(
            workspace_root=self._workspace,
            filesystem=FilesystemAccess.WORKSPACE_WRITE,
            network=NetworkAccess.DENY,
            command=CommandScope.WORKSPACE,
            command_allowlist=command_allowlist,
            approval_mode=ApprovalMode.ALWAYS,
            protected_roots=(self._workspace / ".pulse",),
        )
        self._backend = backend
        if (operation_ledger is None) != (owner_id is None):
            raise ValueError(
                "operation_ledger and owner_id must be configured together"
            )
        if owner_id is not None and (
            not isinstance(owner_id, str)
            or not owner_id.strip()
            or len(owner_id.strip()) > 128
        ):
            raise ValueError("owner_id must be a bounded non-empty identifier")
        self._operation_ledger = operation_ledger
        self._operation_owner_id = None if owner_id is None else owner_id.strip()
        # Runtime owns this bounded executor.  Keeping it outside the broker's
        # request stack means an approval HTTP call only commits the decision
        # and schedules work; the original Pi waiter observes the final action
        # result through the condition below.
        self._execution_executor = execution_executor
        self._max_actions = max_actions
        self._lock = threading.RLock()
        self._action_results: dict[_ActionScope, dict[str, Any]] = {}
        self._action_scopes: dict[str, _ActionScope] = {}
        self._action_inflight: set[_ActionScope] = set()
        # Raw tool arguments are retained only until the matching approval is
        # resolved.  They never enter the durable event payload or response;
        # the bounded map exists so an approved request can reach the adapter
        # without asking Pi to replay the side effect.
        self._action_inputs: dict[_ActionScope, dict[str, Any]] = {}
        self._resolution_results: dict[tuple[str, str], dict[str, Any]] = {}
        self._running_actions: dict[_ActionScope, ActionCancellationToken] = {}
        self._action_state: dict[_ActionScope, str] = {}
        self._action_deadlines: dict[_ActionScope, float] = {}
        # Interrupt installs this admission fence before enumerating actions.
        # It closes both sides of the race: a just-reserved worker observes it
        # at adapter entry, while a later tool call is rejected at reservation.
        self._turn_fences: dict[tuple[str, str, int], str] = {}
        self._condition = threading.Condition(self._lock)

    @property
    def approvals(self) -> ApprovalRegistry:
        return self._approvals

    @property
    def evidence_class(self) -> str:
        if self._backend is None:
            return CONTRACT_ONLY
        try:
            candidate = self._backend.evidence_class
        except Exception:
            return LIVE_GATE_UNVERIFIED
        return LIVE_OS_RESTRICTED if candidate == LIVE_OS_RESTRICTED else LIVE_GATE_UNVERIFIED

    def dispatch(
        self,
        engram_id: str,
        tool_name: str,
        input_data: Mapping[str, Any],
        invocation: Any,
        turn_id: str,
    ) -> dict[str, Any]:
        """Evaluate one mutable Pi proxy call before any side effect."""

        action_request_id = getattr(invocation, "tool_call_id", None)
        if (
            not isinstance(action_request_id, str)
            or not action_request_id.strip()
            or tool_name not in MUTABLE_PI_TOOL_NAMES
            or not isinstance(input_data, Mapping)
            or not _valid_identity(engram_id)
            or not _valid_identity(turn_id)
        ):
            return self._failure(
                action_request_id if isinstance(action_request_id, str) else None,
                tool_name,
                "action_request_invalid",
                "the mutable tool request did not satisfy the action contract",
                turn_id=turn_id,
            )
        action_request_id = action_request_id.strip()
        payload = dict(input_data)
        try:
            epoch = self._current_epoch()
        except HarnessActionError as exc:
            result = self._failure(
                action_request_id,
                tool_name,
                exc.code,
                exc.detail,
                turn_id=turn_id,
                status=exc.status,
            )
            return result

        scope = _ActionScope(
            world_id=self._world_id,
            engram_id=engram_id.strip(),
            turn_id=turn_id.strip(),
            epoch=epoch,
            action_request_id=action_request_id,
            tool_name=tool_name,
        )
        reservation = self._reserve_scope(scope, payload)
        if reservation is not None:
            return reservation

        try:
            policy_request = self._policy_request(tool_name, payload)
            context = PolicyContext(
                world_id=self._world_id,
                engram_id=engram_id,
                epoch=epoch,
                subject_kind="engram",
            )
            decision = self._policy.evaluate(policy_request, context)
            preview = dict(decision.safe_preview)
            if tool_name == "bash":
                preview["execution_mode"] = (
                    "background_pipe_session"
                    if payload.get("background") is True
                    else "foreground"
                )
            preview_for = (
                None
                if self._backend is None
                else getattr(self._backend, "preview_for", None)
            )
            if callable(preview_for):
                preview.update(
                    _safe_external_policy_preview(
                        preview_for(tool_name, payload)
                    )
                )
        except Exception:
            result = self._terminal_failure_action(
                scope,
                {},
                error_code="policy_backend_unavailable",
                content=(
                    "The Harness policy could not be evaluated; no action "
                    "was started."
                ),
                execution_status="not_started",
                approval_id=None,
                epoch=epoch,
            )
            return self._remember_action(scope, result)

        # Policy evaluation can call an external decision seam and therefore
        # cannot run while holding the broker lock.  Re-enter one short
        # admission critical section before projecting TOOL_STARTED.  An
        # interrupt that fenced/settled the reserved action while policy was
        # evaluating wins this race; dispatch then replays that terminal
        # winner and cannot append a late start or approval.
        with self._condition:
            post_policy_admitted = (
                self._action_state.get(scope) == "starting"
                and not self._turn_is_fenced_locked(scope)
            )
            started = (
                self._append(
                    turn_id=turn_id,
                    engram_id=engram_id,
                    kind=HarnessEventKind.TOOL_STARTED,
                    phase=HarnessEventPhase.STREAM,
                    source=HarnessEventSource.PI_RPC,
                    status=HarnessEventStatus.RUNNING,
                    payload={
                        "action_request_id": action_request_id,
                        "tool_name": tool_name,
                        "policy_id": decision.policy_id,
                        "epoch": epoch,
                    },
                )
                if post_policy_admitted
                else None
            )
        if not post_policy_admitted:
            result = self._terminal_failure_action(
                scope,
                preview,
                error_code="turn_interrupt_fenced",
                content=(
                    "The Harness turn was interrupted before policy "
                    "admission completed; no action was started."
                ),
                execution_status="not_started",
                approval_id=None,
                epoch=epoch,
            )
            return self._remember_action(scope, result)
        if started is None:
            result = self._terminal_failure_action(
                scope,
                preview,
                error_code="harness_event_store_unavailable",
                content=(
                    "The durable Harness event projection is unavailable; "
                    "no action was started."
                ),
                execution_status="not_started",
                approval_id=None,
                epoch=epoch,
            )
            return self._remember_action(scope, result)

        if not decision.allow and decision.requires_approval:
            return self._request_approval_after_policy(
                scope,
                payload,
                preview,
                decision,
            )

        if not decision.allow:
            result = self._terminal_failure_action(
                scope,
                preview,
                error_code=decision.reason_code,
                content="The action was denied by the Harness policy.",
                execution_status="not_started",
                approval_id=None,
                epoch=epoch,
            )
            return self._remember_action(scope, result)

        # A configured backend may be called only after the policy and lease
        # checks above.  This release deliberately has no unrestricted fallback.
        if self._backend is None:
            result = self._terminal_failure_action(
                scope,
                preview,
                error_code="sandbox_backend_unavailable",
                content="No verified workspace sandbox backend is configured.",
                execution_status="unsupported",
                approval_id=None,
                epoch=epoch,
            )
            return self._remember_action(scope, result)

        result = self._execute_backend(
            scope,
            payload,
            preview,
            approval_id=None,
        )
        return self._remember_action(scope, result)

    def _request_approval_after_policy(
        self,
        scope: _ActionScope,
        payload: Mapping[str, Any],
        preview: Mapping[str, Any],
        decision: PolicyDecision,
    ) -> dict[str, Any]:
        """Publish one approval only while the post-policy action is live.

        The broker condition covers the registry request, durable projection,
        operation transition and pending-result publication as one admission
        unit.  Interrupt may win before this section or observe a complete
        pending approval afterward; it can never settle the action between
        those steps and leave a late REQUESTED approval behind.
        """

        with self._condition:
            if (
                self._action_state.get(scope) != "starting"
                or self._turn_is_fenced_locked(scope)
            ):
                result = self._terminal_failure_action(
                    scope,
                    preview,
                    error_code="turn_interrupt_fenced",
                    content=(
                        "The Harness turn was interrupted before approval "
                        "admission completed; no action was started."
                    ),
                    execution_status="not_started",
                    approval_id=None,
                    epoch=scope.epoch,
                )
                return self._remember_action(scope, result)
            try:
                approval = self._approvals.request(
                    request_id=scope.action_request_id,
                    world_id=self._world_id,
                    engram_id=scope.engram_id,
                    turn_id=scope.turn_id,
                    epoch=scope.epoch,
                    target_kind=scope.tool_name,
                    safe_preview=preview,
                    policy_id=decision.policy_id,
                    ttl_seconds=self._policy.approval_ttl_seconds,
                    capability_scope=decision.capability_scope,
                )
            except Exception:
                result = self._terminal_failure_action(
                    scope,
                    preview,
                    error_code="approval_backend_unavailable",
                    content=(
                        "The server-side approval broker did not accept the "
                        "request; no action was started."
                    ),
                    execution_status="not_started",
                    approval_id=None,
                    epoch=scope.epoch,
                )
                return self._remember_action(scope, result)
            approval_event = self._append(
                turn_id=scope.turn_id,
                engram_id=scope.engram_id,
                kind=HarnessEventKind.APPROVAL_REQUESTED,
                phase=HarnessEventPhase.APPROVAL,
                source=HarnessEventSource.POLICY,
                status=HarnessEventStatus.RUNNING,
                payload={
                    "action_request_id": scope.action_request_id,
                    "request_id": scope.action_request_id,
                    "approval_id": approval.approval_id,
                    "target_kind": scope.tool_name,
                    "epoch": scope.epoch,
                    "state": approval.state.value,
                    "safe_preview": dict(preview),
                    "evidence_class": CONTRACT_ONLY,
                },
            )
            if approval_event is None:
                # An in-memory registry entry is not an observable approval
                # surface.  Revoke it and terminate the action immediately;
                # otherwise the original Pi tool call can wait for the full
                # TTL on a request that no Workbench/replay consumer can see.
                try:
                    self._approvals.revoke(
                        approval.approval_id,
                        expected_epoch=scope.epoch,
                        request_id=f"projection-failed-{scope.action_request_id}",
                        expected_state=ApprovalState.REQUESTED,
                        world_id=self._world_id,
                        engram_id=scope.engram_id,
                        turn_id=scope.turn_id,
                    )
                except Exception:
                    # The action's own terminal state remains the final
                    # execution fence even if registry cleanup also fails.
                    pass
                result = self._terminal_failure_action(
                    scope,
                    preview,
                    error_code="approval_request_persistence_failed",
                    content=(
                        "The approval request could not be persisted; no "
                        "sandbox action was started."
                    ),
                    execution_status="not_started",
                    approval_id=approval.approval_id,
                    epoch=scope.epoch,
                )
                return self._remember_action(scope, result)
            if not self._transition_operation(scope, OperationPhase.APPROVAL_PENDING):
                try:
                    self._approvals.revoke(
                        approval.approval_id,
                        expected_epoch=scope.epoch,
                        request_id=f"ledger-failed-{scope.action_request_id}",
                        expected_state=ApprovalState.REQUESTED,
                        world_id=self._world_id,
                        engram_id=scope.engram_id,
                        turn_id=scope.turn_id,
                    )
                except Exception:
                    pass
                result = self._terminal_failure_action(
                    scope,
                    preview,
                    error_code="operation_ledger_unavailable",
                    content=(
                        "The durable operation ledger could not record the "
                        "approval boundary; no action was started."
                    ),
                    execution_status="not_started",
                    approval_id=approval.approval_id,
                    epoch=scope.epoch,
                )
                return self._remember_action(scope, result)
            result = {
                "ok": False,
                "content": "Approval required before this action can be considered.",
                "data": {
                    "action_request_id": scope.action_request_id,
                    "approval_id": approval.approval_id,
                    "state": approval.state.value,
                    "expires_at": approval.expires_at.isoformat(),
                    "execution_status": "pending_approval",
                    "evidence_class": CONTRACT_ONLY,
                },
                "event_id": approval_event.event_id,
                "error": "approval_required",
            }
            self._remember_input(scope, payload)
            return self._remember_action(scope, result)

    def resolve_approval(self, approval_id: str, request: Mapping[str, Any]) -> dict[str, Any]:
        """Resolve approval and hand execution back to the original Pi call.

        The approval transition is deliberately short.  Once ``allow_once``
        is accepted, a cancellation token is installed before the registry
        transition becomes visible, then adapter work is submitted to the
        Runtime-owned bounded executor.  The HTTP caller receives ``starting``
        while ``wait_for_action`` remains attached to the same action scope.
        """

        resolution_request_id = request.get("request_id")
        expected_turn_id = request.get("expected_turn_id")
        expected_epoch = request.get("expected_epoch")
        decision = request.get("decision")
        if not all(
            isinstance(value, str) and value.strip()
            for value in (approval_id, resolution_request_id, expected_turn_id)
        ):
            raise HarnessActionError(
                "approval_request_invalid",
                "approval resolution is missing a bounded identity field",
                "send approval_id, request_id and expected_turn_id",
                status=400,
            )
        if (
            isinstance(expected_epoch, bool)
            or not isinstance(expected_epoch, int)
            or expected_epoch < 1
        ):
            raise HarnessActionError(
                "invalid_epoch",
                "expected_epoch must be a positive integer",
                "refresh the Workbench turn summary",
                status=400,
            )
        if decision not in {"allow_once", "deny", "cancel"}:
            raise HarnessActionError(
                "invalid_decision",
                "approval decision is outside the supported vocabulary",
                "choose allow_once, deny or cancel",
                status=400,
            )
        approval_id = approval_id.strip()
        resolution_request_id = resolution_request_id.strip()
        expected_turn_id = expected_turn_id.strip()
        key = (approval_id, resolution_request_id)
        with self._lock:
            prior = self._resolution_results.get(key)
            if prior is not None:
                return dict(prior, idempotent=True)

        approval = self._approvals.get(approval_id)
        if approval is None:
            # Unknown approvals still require a live lease proof.  A known
            # action is the only exception: cancellation of that action must
            # remain possible after the lease callback has fenced new work.
            current_epoch = self._current_epoch()
            if current_epoch != expected_epoch:
                raise HarnessActionError(
                    "stale_epoch",
                    "the approval was created under an old Runtime lease epoch",
                    "refresh the turn summary and retry with its current epoch",
                    status=409,
                )
            return self._remember_resolution(
                key,
                {
                    "accepted": False,
                    "state": "rejected",
                    "error_code": "approval_unknown",
                    "evidence_class": CONTRACT_ONLY,
                },
            )
        if approval.turn_id != expected_turn_id:
            raise HarnessActionError(
                "approval_scope_mismatch",
                "the approval does not belong to the expected Harness turn",
                "use the turn_id shown on the approval event",
                status=409,
            )
        if decision == "cancel":
            if expected_epoch != approval.epoch:
                raise HarnessActionError(
                    "stale_epoch",
                    "cancellation does not match the action's creation epoch",
                    "use the epoch shown on the approval event",
                    status=409,
                )
            # Cancellation is a stop operation, not a new mutation.  It is
            # allowed to proceed when the old lease has just been fenced.
            try:
                self._current_epoch()
            except HarnessActionError:
                pass
        else:
            current_epoch = self._current_epoch()
            if current_epoch != expected_epoch:
                raise HarnessActionError(
                    "stale_epoch",
                    "the approval was created under an old Runtime lease epoch",
                    "refresh the turn summary and retry with its current epoch",
                    status=409,
                )

        scope = _ActionScope(
            world_id=self._world_id,
            engram_id=approval.engram_id,
            turn_id=approval.turn_id,
            epoch=approval.epoch,
            action_request_id=approval.request_id,
            tool_name=approval.target_kind,
        )
        with self._condition:
            known_scope = self._action_scopes.get(approval.request_id)
            if known_scope is not None and known_scope != scope:
                raise HarnessActionError(
                    "action_request_scope_conflict",
                    "the approval request is bound to a different Harness action scope",
                    "use the action_request_id and approval shown for this turn",
                    status=409,
                )
            if self._action_state.get(scope) == "terminal":
                prior = self._action_results.get(scope)
                data = prior.get("data") if isinstance(prior, Mapping) else None
                execution_status = (
                    data.get("execution_status")
                    if isinstance(data, Mapping)
                    else "not_started"
                )
                return self._remember_resolution(
                    key,
                    {
                        "accepted": False,
                        "state": "rejected",
                        "error_code": "action_already_terminal",
                        "approval_id": approval.approval_id,
                        "action_request_id": approval.request_id,
                        "approval_accepted": False,
                        "execution_status": execution_status,
                        "evidence_class": LIVE_GATE_UNVERIFIED,
                    },
                )
            if (
                decision == "allow_once"
                and self._action_state.get(scope) in {"starting", "running"}
                and scope in self._running_actions
            ):
                return {
                    "accepted": True,
                    "state": "starting",
                    "error_code": None,
                    "approval_id": approval.approval_id,
                    "action_request_id": approval.request_id,
                    "approval_accepted": True,
                    "execution_status": self._action_state.get(scope, "starting"),
                    "idempotent": True,
                    "evidence_class": LIVE_GATE_UNVERIFIED,
                }
            transition_token: ActionCancellationToken | None = None
            if decision == "allow_once" and self._action_state.get(scope) == "pending":
                # This latch is visible to cancel_action before ApprovalRegistry
                # changes REQUESTED -> ALLOWED_ONCE.  It closes the approval to
                # backend-start race without holding the broker lock over I/O.
                transition_token = ActionCancellationToken()
                self._running_actions[scope] = transition_token
                self._action_state[scope] = "starting"
                self._condition.notify_all()

        try:
            resolution = self._approvals.resolve(
                approval.approval_id,
                decision,
                expected_epoch=expected_epoch,
                request_id=resolution_request_id,
                expected_state=ApprovalState.REQUESTED,
                world_id=self._world_id,
                engram_id=approval.engram_id,
                turn_id=approval.turn_id,
            )
        except Exception:
            if transition_token is not None:
                self._restore_pending_after_transition(scope, transition_token)
            raise
        result = self._resolution_result(approval, resolution)
        action_result: dict[str, Any] | None = None
        resolution_event = self._append(
            turn_id=approval.turn_id,
            engram_id=approval.engram_id,
            kind=HarnessEventKind.APPROVAL_RESOLVED,
            phase=HarnessEventPhase.APPROVAL,
            source=HarnessEventSource.PULSE_CONTROL,
            status=(
                HarnessEventStatus.COMPLETED
                if resolution.accepted
                else HarnessEventStatus.FAILED
            ),
            payload={
                "action_request_id": approval.request_id,
                "request_id": approval.request_id,
                "approval_id": approval.approval_id,
                "resolution_request_id": resolution_request_id,
                "decision": resolution.decision,
                "state": resolution.state.value if resolution.state else None,
                "epoch": expected_epoch,
                "reason_code": resolution.reason_code,
                "evidence_class": resolution.evidence_class,
            },
        )

        if resolution_event is None:
            # The approval registry is intentionally in-process; the event
            # projection is the durable audit/recovery boundary.  Never cross
            # into an adapter when the accepted human decision could not be
            # recorded for replay.
            if transition_token is not None:
                transition_token.cancel("approval_resolution_persistence_failed")
            action_result = self._terminal_failure_action(
                scope,
                approval.safe_preview,
                error_code="approval_resolution_persistence_failed",
                content=(
                    "The approval decision could not be persisted; no sandbox "
                    "action was started."
                ),
                execution_status="not_started",
                approval_id=approval.approval_id,
                epoch=expected_epoch,
            )
            result.update(
                accepted=False,
                state="failed",
                error_code="approval_resolution_persistence_failed",
                approval_accepted=resolution.accepted,
                execution_status="not_started",
                evidence_class=LIVE_GATE_UNVERIFIED,
            )
            if transition_token is not None:
                self._drop_action_token(scope, transition_token)
            self._forget_input(scope)
            self._remember_action(scope, action_result)
            return self._remember_resolution(key, result)

        if not resolution.accepted:
            if transition_token is not None:
                self._restore_pending_after_transition(scope, transition_token)
            return self._remember_resolution(key, result)

        if resolution.state is ApprovalState.ALLOWED_ONCE:
            if transition_token is None:
                # A recovery path may have a durable approval but no in-memory
                # pending result after a process restart.  Recreate the latch
                # before touching the adapter; missing input will still fail
                # closed below.
                transition_token = ActionCancellationToken()
                with self._condition:
                    self._running_actions[scope] = transition_token
                    self._action_state[scope] = "starting"
            if transition_token.is_set():
                action_result = self._cancelled_action_result(
                    scope,
                    approval.safe_preview,
                    approval_id=approval.approval_id,
                    reason=transition_token.reason or "cancelled",
                )
                result.update(
                    accepted=False,
                    state="cancelled",
                    error_code="cancelled",
                    approval_accepted=True,
                    execution_status="cancelled",
                    evidence_class=LIVE_GATE_UNVERIFIED,
                )
                self._drop_action_token(scope, transition_token)
                self._forget_input(scope)
            else:
                # Re-fence immediately before crossing into the process
                # adapter.  The approval transition alone never authorizes
                # work under a lease that has since been lost.
                epoch_error: HarnessActionError | None = None
                try:
                    execution_epoch = self._current_epoch()
                except HarnessActionError as exc:
                    execution_epoch = None
                    epoch_error = exc
                if epoch_error is not None or execution_epoch != expected_epoch:
                    code = epoch_error.code if epoch_error is not None else "stale_epoch"
                    detail = (
                        "the Runtime lease was lost before the approved action could execute"
                        if epoch_error is not None
                        else "the Runtime lease changed before the approved action could execute"
                    )
                    action_result = self._terminal_failure_action(
                        scope,
                        approval.safe_preview,
                        error_code=code,
                        content=detail,
                        execution_status="not_started",
                        approval_id=approval.approval_id,
                        epoch=expected_epoch if execution_epoch is None else execution_epoch,
                    )
                    result.update(
                        accepted=False,
                        state="failed",
                        error_code=code,
                        approval_accepted=True,
                        execution_status="not_started",
                        evidence_class=LIVE_GATE_UNVERIFIED,
                    )
                    transition_token.cancel(code)
                    self._drop_action_token(scope, transition_token)
                    self._forget_input(scope)
                elif self._backend is None:
                    action_result = self._terminal_failure_action(
                        scope,
                        approval.safe_preview,
                        error_code="sandbox_backend_unavailable",
                        content="No verified workspace sandbox backend is configured.",
                        execution_status="unsupported",
                        approval_id=approval.approval_id,
                        epoch=expected_epoch,
                    )
                    result.update(
                        accepted=False,
                        state="unsupported_execution",
                        error_code="sandbox_backend_unavailable",
                        approval_accepted=True,
                        execution_status="unsupported",
                        evidence_class=LIVE_GATE_UNVERIFIED,
                    )
                    transition_token.cancel("sandbox_backend_unavailable")
                    self._drop_action_token(scope, transition_token)
                    self._forget_input(scope)
                else:
                    input_data = self._get_action_input(scope)
                    if input_data is None:
                        action_result = self._terminal_failure_action(
                            scope,
                            approval.safe_preview,
                            error_code="sandbox_input_unavailable",
                            content="The bounded approved action input is no longer available.",
                            execution_status="not_started",
                            approval_id=approval.approval_id,
                            epoch=expected_epoch,
                        )
                        result.update(
                            accepted=False,
                            state="failed",
                            error_code="sandbox_input_unavailable",
                            approval_accepted=True,
                            execution_status="not_started",
                            evidence_class=LIVE_GATE_UNVERIFIED,
                        )
                        transition_token.cancel("sandbox_input_unavailable")
                        self._drop_action_token(scope, transition_token)
                    elif self._execution_executor is None:
                        self._set_execution_deadline(scope, input_data)
                        execution = self._execute_backend(
                            scope,
                            input_data,
                            approval.safe_preview,
                            approval_id=approval.approval_id,
                            token=transition_token,
                        )
                        execution_data = execution.get("data")
                        if not isinstance(execution_data, Mapping):
                            execution_data = {}
                        succeeded = execution.get("ok") is True
                        result = {
                            "accepted": succeeded,
                            "state": "completed" if succeeded else "failed",
                            "error_code": None
                            if succeeded
                            else execution.get("error", "sandbox_execution_failed"),
                            "approval_id": approval.approval_id,
                            "action_request_id": approval.request_id,
                            "approval_accepted": True,
                            "execution_status": execution_data.get(
                                "execution_status",
                                "completed" if succeeded else "failed",
                            ),
                            "idempotent": False,
                            "evidence_class": execution_data.get(
                                "evidence_class", LIVE_GATE_UNVERIFIED
                            ),
                        }
                        evidence_binding = execution_data.get("evidence_binding")
                        if isinstance(evidence_binding, Mapping):
                            result["evidence_binding"] = dict(evidence_binding)
                        action_result = execution
                        self._forget_input(scope)
                    else:
                        self._set_execution_deadline(scope, input_data)
                        starting_action = self._publish_starting_action(
                            scope,
                            approval_id=approval.approval_id,
                        )
                        starting_data = starting_action.get("data")
                        if (
                            not isinstance(starting_data, Mapping)
                            or starting_data.get("execution_status") != "starting"
                        ):
                            action_result = starting_action
                            result.update(
                                accepted=False,
                                state=starting_data.get("state", "failed")
                                if isinstance(starting_data, Mapping)
                                else "failed",
                                error_code=starting_action.get(
                                    "error", "sandbox_cancellation_uncertain"
                                ),
                                approval_accepted=True,
                                execution_status=starting_data.get(
                                    "execution_status", "uncertain"
                                )
                                if isinstance(starting_data, Mapping)
                                else "uncertain",
                                evidence_class=starting_data.get(
                                    "evidence_class", LIVE_GATE_UNVERIFIED
                                )
                                if isinstance(starting_data, Mapping)
                                else LIVE_GATE_UNVERIFIED,
                            )
                            self._forget_input(scope)
                        else:
                            self._append_progress(
                                scope,
                                state="starting",
                                approval_id=approval.approval_id,
                            )
                            try:
                                self._execution_executor.submit(
                                    self._run_approved_backend,
                                    scope,
                                    input_data,
                                    approval.safe_preview,
                                    approval.approval_id,
                                    transition_token,
                                )
                            except Exception:
                                transition_token.cancel("execution_capacity_unavailable")
                                action_result = self._terminal_failure_action(
                                    scope,
                                    approval.safe_preview,
                                    error_code="execution_capacity_unavailable",
                                    content="The bounded Harness execution capacity is unavailable.",
                                    execution_status="not_started",
                                    approval_id=approval.approval_id,
                                    epoch=expected_epoch,
                                )
                                result.update(
                                    accepted=False,
                                    state="failed",
                                    error_code="execution_capacity_unavailable",
                                    approval_accepted=True,
                                    execution_status="not_started",
                                    evidence_class=LIVE_GATE_UNVERIFIED,
                                )
                                self._drop_action_token(scope, transition_token)
                                self._forget_input(scope)
                            else:
                                result.update(
                                    accepted=True,
                                    state="starting",
                                    error_code=None,
                                    approval_accepted=True,
                                    execution_status="starting",
                                    evidence_class=LIVE_GATE_UNVERIFIED,
                                )
        elif resolution.state in {ApprovalState.DENIED, ApprovalState.CANCELLED}:
            # Deny/cancel are terminal approval outcomes.  Emit the matching
            # tool terminal so replay and Workbench do not leave a phantom
            # running action behind.
            terminal = self._append_terminal(
                approval.turn_id,
                approval.engram_id,
                approval.request_id,
                approval.target_kind,
                status=(
                    HarnessEventStatus.CANCELLED
                    if resolution.state is ApprovalState.CANCELLED
                    else HarnessEventStatus.FAILED
                ),
                error_code=resolution.reason_code,
                epoch=expected_epoch,
                preview=approval.safe_preview,
                approval_id=approval.approval_id,
            )
            if terminal is not None:
                result["event_id"] = terminal.event_id
            action_result = {
                "ok": False,
                "content": "The action was not executed after the approval was closed.",
                "data": {
                    "action_request_id": approval.request_id,
                    "state": result["state"],
                    "execution_status": "not_started",
                    "evidence_class": CONTRACT_ONLY,
                },
                "event_id": None if terminal is None else terminal.event_id,
                "error": result["error_code"],
            }
            self._forget_input(scope)
        if action_result is not None:
            self._remember_action(scope, action_result)
        return self._remember_resolution(key, result)

    def _restore_pending_after_transition(
        self,
        scope: _ActionScope,
        token: ActionCancellationToken,
    ) -> None:
        with self._condition:
            if self._running_actions.get(scope) is token:
                self._running_actions.pop(scope, None)
            if self._action_state.get(scope) == "starting":
                self._action_state[scope] = "pending"
            self._condition.notify_all()

    def _drop_action_token(
        self,
        scope: _ActionScope,
        token: ActionCancellationToken,
    ) -> None:
        with self._condition:
            if self._running_actions.get(scope) is token:
                self._running_actions.pop(scope, None)
            self._condition.notify_all()

    def _run_approved_backend(
        self,
        scope: _ActionScope,
        input_data: Mapping[str, Any],
        preview: Mapping[str, Any],
        approval_id: str,
        token: ActionCancellationToken,
    ) -> None:
        try:
            execution = self._execute_backend(
                scope,
                input_data,
                preview,
                approval_id=approval_id,
                token=token,
            )
        except Exception:
            execution = self._terminal_failure_action(
                scope,
                preview,
                error_code="sandbox_execution_failed",
                content="The restricted sandbox backend failed before returning a terminal result.",
                execution_status="failed",
                approval_id=approval_id,
                epoch=scope.epoch,
            )
        finally:
            self._forget_input(scope)
        self._remember_action(scope, execution)

    def _terminal_failure_action(
        self,
        scope: _ActionScope,
        preview: Mapping[str, Any],
        *,
        error_code: str,
        content: str,
        execution_status: str,
        approval_id: str | None,
        epoch: int,
        already_claimed: bool = False,
    ) -> dict[str, Any]:
        if not already_claimed and not self._claim_terminal(
            scope,
            uncertain=execution_status == "uncertain",
        ):
            prior = self._await_terminal_result(scope)
            if prior is not None:
                return prior
            # A terminal owner that disappeared is itself a recovery failure;
            # do not append a second event from this non-owner.
            return {
                "ok": False,
                "content": content,
                "data": {
                    "action_request_id": scope.action_request_id,
                    "approval_id": approval_id,
                    "state": "uncertain",
                    "execution_status": "uncertain",
                    "recovery_state": "uncertain",
                    "evidence_class": LIVE_GATE_UNVERIFIED,
                },
                "event_id": None,
                "error": "sandbox_terminal_owner_lost",
            }
        terminal = self._append_terminal(
            scope.turn_id,
            scope.engram_id,
            scope.action_request_id,
            scope.tool_name,
            status=(
                HarnessEventStatus.CANCELLED
                if execution_status == "cancelled"
                else (
                    HarnessEventStatus.UNCERTAIN
                    if execution_status == "uncertain"
                    else HarnessEventStatus.FAILED
                )
            ),
            error_code=error_code,
            epoch=epoch,
            preview=preview,
            approval_id=approval_id,
            adapter_summary=(
                {"recovery_state": "uncertain"}
                if execution_status == "uncertain"
                else None
            ),
            execution_status=execution_status,
        )
        result = {
            "ok": False,
            "content": content,
            "data": {
                "action_request_id": scope.action_request_id,
                "approval_id": approval_id,
                "state": execution_status,
                "execution_status": execution_status,
                "evidence_class": LIVE_GATE_UNVERIFIED,
                **(
                    {"recovery_state": "uncertain"}
                    if execution_status == "uncertain"
                    else {}
                ),
            },
            "event_id": None if terminal is None else terminal.event_id,
            "error": error_code,
        }
        published = self._publish_terminal(scope, result)
        return result if published is None else published

    def _cancelled_action_result(
        self,
        scope: _ActionScope,
        preview: Mapping[str, Any],
        *,
        approval_id: str | None,
        reason: str,
    ) -> dict[str, Any]:
        self._append_progress(
            scope,
            state="cancelled_before_start",
            approval_id=approval_id,
            reason=reason,
        )
        return self._terminal_failure_action(
            scope,
            preview,
            error_code="sandbox_cancelled",
            content="The sandbox action was cancelled before execution started.",
            execution_status="cancelled",
            approval_id=approval_id,
            epoch=scope.epoch,
        )

    def _current_epoch(self) -> int:
        try:
            epoch = self._epoch_provider()
        except HarnessActionError:
            raise
        except Exception as exc:
            raise HarnessActionError(
                "runtime_lease_lost",
                "the Runtime could not prove ownership of the PulseWorld",
                "reconnect after a new Runtime has acquired the durable lease",
                status=409,
            ) from exc
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 1:
            raise HarnessActionError(
                "runtime_lease_lost",
                "the Runtime returned no valid ownership epoch",
                "reconnect after the Runtime lease is healthy",
                status=409,
            )
        return epoch

    def _policy_request(self, tool_name: str, input_data: Mapping[str, Any]) -> PolicyRequest:
        if tool_name == "bash":
            return PolicyRequest(
                operation="command",
                command=input_data.get("command"),
                cwd=".",
                shell=False,
                tool_name=tool_name,
                source="pi",
            )
        if tool_name == "pulse_mcp_call":
            return PolicyRequest(
                operation="capability",
                capability="mcp.call",
                tool_name=tool_name,
                source="pi",
                requires_approval=True,
            )
        return PolicyRequest(
            operation="file_edit" if tool_name == "edit" else "file_write",
            path=input_data.get("path"),
            tool_name=tool_name,
            source="pi",
        )

    def _append_terminal(
        self,
        turn_id: str,
        engram_id: str,
        action_request_id: str,
        tool_name: str,
        *,
        status: HarnessEventStatus,
        error_code: str,
        epoch: int,
        preview: Mapping[str, Any],
        approval_id: str | None = None,
        adapter_summary: Mapping[str, Any] | None = None,
        execution_status: str | None = None,
        evidence_class: str | None = None,
    ) -> Any | None:
        ledger = self._operation_ledger
        owner_id = self._operation_owner_id
        scope = _ActionScope(
            world_id=self._world_id,
            engram_id=engram_id,
            turn_id=turn_id,
            epoch=epoch,
            action_request_id=action_request_id,
            tool_name=tool_name,
        )
        terminal_state: OperationTerminalState | None = None
        if ledger is not None and owner_id is not None:
            try:
                prior_operation = ledger.get(
                    self._operation_kind(scope), scope.action_request_id
                )
            except Exception:
                return None
            if prior_operation is None:
                return None
            if prior_operation.is_terminal:
                if prior_operation.terminal_event_id is None:
                    return None
                get_event = getattr(self._event_store, "get", None)
                if not callable(get_event):
                    return None
                try:
                    return get_event(prior_operation.terminal_event_id)
                except Exception:
                    return None
            post_boundary = prior_operation.phase in {
                OperationPhase.BOUNDARY_ENTERED,
                OperationPhase.ADAPTER_RETURNED,
                OperationPhase.TERMINALIZING,
            }
            if status is HarnessEventStatus.UNCERTAIN:
                terminal_state = OperationTerminalState.UNCERTAIN
            elif post_boundary:
                terminal_state = OperationTerminalState.COMPLETED
            elif status is HarnessEventStatus.CANCELLED:
                terminal_state = OperationTerminalState.CANCELLED_NOT_STARTED
            else:
                terminal_state = OperationTerminalState.FAILED_NOT_STARTED
        payload: dict[str, Any] = {
            "action_request_id": action_request_id,
            "tool_name": tool_name,
            "epoch": epoch,
            "error_code": error_code,
            "execution_status": execution_status
            or (
                "unsupported"
                if error_code == "sandbox_backend_unavailable"
                else ("completed" if error_code == "none" else "not_started")
            ),
            "safe_preview": dict(preview),
            "evidence_class": evidence_class
            or (LIVE_GATE_UNVERIFIED if error_code in {"sandbox_backend_unavailable", "none"} else CONTRACT_ONLY),
        }
        if approval_id is not None:
            payload["approval_id"] = approval_id
        if adapter_summary is not None:
            payload["adapter_result"] = dict(adapter_summary)
        draft = HarnessEventDraft(
            turn_id=turn_id,
            world_id=self._world_id,
            engram_id=engram_id,
            kind=HarnessEventKind.TOOL_COMPLETED,
            phase=HarnessEventPhase.TERMINAL,
            source=HarnessEventSource.POLICY,
            status=status,
            payload=payload,
            event_id=(
                None
                if ledger is None
                else deterministic_terminal_event_id(
                    self._operation_kind(scope),
                    scope.action_request_id,
                )
            ),
        )
        if ledger is None:
            return self._append(
                turn_id=turn_id,
                engram_id=engram_id,
                kind=HarnessEventKind.TOOL_COMPLETED,
                phase=HarnessEventPhase.TERMINAL,
                source=HarnessEventSource.POLICY,
                status=status,
                payload=payload,
            )
        if owner_id is None or terminal_state is None:
            return None
        terminal_append = getattr(
            self._event_store,
            "append_terminal_operation",
            None,
        )
        if not callable(terminal_append):
            return None
        try:
            event, winner = terminal_append(
                draft,
                ledger=ledger,
                operation_kind=self._operation_kind(scope),
                operation_id=scope.action_request_id,
                expected_epoch=scope.epoch,
                owner_id=owner_id,
                terminal_state=terminal_state,
            )
            if winner.terminal_event_id != event.event_id:
                return None
            return event
        except Exception:
            # The combined transaction rolled back both rows.  Preserve one
            # durable fail-closed winner in a second transaction: after the
            # adapter boundary the only honest projection-less outcome is
            # UNCERTAIN; before it, the original not-started state is safe.
            try:
                ledger.claim_terminal(
                    self._operation_kind(scope),
                    scope.action_request_id,
                    expected_epoch=scope.epoch,
                    owner_id=owner_id,
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

    def _append(
        self,
        *,
        turn_id: str,
        engram_id: str,
        kind: HarnessEventKind,
        phase: HarnessEventPhase,
        source: HarnessEventSource,
        status: HarnessEventStatus,
        payload: Mapping[str, Any],
        event_id: str | None = None,
    ) -> Any | None:
        store = self._event_store
        if store is None or not callable(getattr(store, "append", None)):
            return None
        try:
            return store.append(
                HarnessEventDraft(
                    turn_id=turn_id,
                    world_id=self._world_id,
                    engram_id=engram_id,
                    kind=kind,
                    phase=phase,
                    source=source,
                    status=status,
                    payload=dict(payload),
                    event_id=event_id,
                )
            )
        except Exception:
            return None

    @staticmethod
    def _failure(
        action_request_id: str | None,
        tool_name: str,
        code: str,
        detail: str,
        *,
        turn_id: str,
        status: int = 503,
    ) -> dict[str, Any]:
        del status
        return {
            "ok": False,
            "content": detail,
            "data": {
                "action_request_id": action_request_id,
                "tool_name": tool_name,
                "state": "rejected",
                "execution_status": "not_started",
                "evidence_class": LIVE_GATE_UNVERIFIED,
                "turn_id": turn_id,
            },
            "event_id": None,
            "error": code,
        }

    def _reserve_scope(
        self,
        scope: _ActionScope,
        input_data: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Reserve a complete action scope before policy or adapter work."""

        deadline = time.monotonic() + 30.0
        with self._condition:
            known = self._action_scopes.get(scope.action_request_id)
            if known is not None and known != scope:
                return self._failure(
                    scope.action_request_id,
                    scope.tool_name,
                    "action_request_scope_conflict",
                    "action_request_id is already bound to another World, Engram, turn or epoch",
                    turn_id=scope.turn_id,
                    status=409,
                )
            if known is None:
                if self._turn_is_fenced_locked(scope):
                    return self._failure(
                        scope.action_request_id,
                        scope.tool_name,
                        "turn_interrupt_fenced",
                        "the Harness turn is already fenced against new mutable actions",
                        turn_id=scope.turn_id,
                        status=409,
                    )
                durable = self._admit_operation(scope, input_data)
                if isinstance(durable, Mapping):
                    return dict(durable)
                while len(self._action_scopes) >= self._max_actions:
                    if not self._evict_completed_scope_locked():
                        return self._failure(
                            scope.action_request_id,
                            scope.tool_name,
                            "action_registry_full",
                            "the bounded action registry has no terminal entry that can be evicted",
                            turn_id=scope.turn_id,
                            status=503,
                        )
                self._action_scopes[scope.action_request_id] = scope
                self._action_state[scope] = "starting"
            while scope in self._action_inflight and scope not in self._action_results:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return self._failure(
                        scope.action_request_id,
                        scope.tool_name,
                        "action_request_in_flight",
                        "the same action request is still being resolved",
                        turn_id=scope.turn_id,
                        status=409,
                    )
                self._condition.wait(timeout=min(remaining, 0.25))
            prior = self._action_results.get(scope)
            if prior is not None:
                return dict(prior, idempotent=True)
            self._action_inflight.add(scope)
        return None

    @staticmethod
    def _turn_fence_key(scope: _ActionScope) -> tuple[str, str, int]:
        return (scope.engram_id, scope.turn_id, scope.epoch)

    def _turn_is_fenced_locked(self, scope: _ActionScope) -> bool:
        return self._turn_fence_key(scope) in self._turn_fences

    @staticmethod
    def _operation_kind(scope: _ActionScope) -> str:
        return f"tool.{scope.tool_name}"

    @staticmethod
    def _digest(value: Any) -> str:
        """Hash canonical operation identity without storing raw arguments."""

        def bounded(candidate: Any) -> Any:
            if candidate is None or isinstance(candidate, (bool, int, float, str)):
                return candidate
            if isinstance(candidate, Mapping):
                return {
                    str(key)[:128]: bounded(item)
                    for key, item in sorted(
                        candidate.items(), key=lambda pair: str(pair[0])
                    )[:256]
                }
            if isinstance(candidate, (list, tuple)):
                return [bounded(item) for item in candidate[:256]]
            return {"type": type(candidate).__name__[:128]}

        encoded = json.dumps(
            bounded(value),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        return "sha256:" + hashlib.sha256(encoded).hexdigest()

    def _admit_operation(
        self,
        scope: _ActionScope,
        input_data: Mapping[str, Any],
    ) -> HarnessOperation | dict[str, Any] | None:
        ledger = self._operation_ledger
        owner_id = self._operation_owner_id
        if ledger is None or owner_id is None:
            return None
        kind = self._operation_kind(scope)
        scope_digest = self._digest(
            {
                "world_id": scope.world_id,
                "engram_id": scope.engram_id,
                "turn_id": scope.turn_id,
                "epoch": scope.epoch,
                "action_request_id": scope.action_request_id,
                "tool_name": scope.tool_name,
            }
        )
        effect_key = self._digest(
            {"tool_name": scope.tool_name, "input": dict(input_data)}
        )
        try:
            existing = ledger.get(kind, scope.action_request_id)
            if existing is not None:
                if (
                    existing.world_id != scope.world_id
                    or existing.engram_id != scope.engram_id
                    or existing.turn_id != scope.turn_id
                    or existing.requested_epoch != scope.epoch
                    or existing.scope_digest != scope_digest
                    or existing.effect_key != effect_key
                ):
                    return self._failure(
                        scope.action_request_id,
                        scope.tool_name,
                        "action_request_scope_conflict",
                        "the durable operation id is bound to another subject, turn, epoch or effect",
                        turn_id=scope.turn_id,
                        status=409,
                    )
                if existing.is_terminal:
                    return self._replay_operation(existing, scope)
                return self._failure(
                    scope.action_request_id,
                    scope.tool_name,
                    "operation_recovery_required",
                    "the durable operation exists without a terminal winner; automatic replay is fenced",
                    turn_id=scope.turn_id,
                    status=409,
                )
            return ledger.admit(
                kind,
                scope.action_request_id,
                world_id=scope.world_id,
                engram_id=scope.engram_id,
                turn_id=scope.turn_id,
                requested_epoch=scope.epoch,
                owner_id=owner_id,
                scope_digest=scope_digest,
                effect_key=effect_key,
            )
        except OperationScopeCollisionError:
            return self._failure(
                scope.action_request_id,
                scope.tool_name,
                "operation_scope_conflict",
                "the durable operation id is bound to a different immutable scope",
                turn_id=scope.turn_id,
                status=409,
            )
        except Exception:
            return self._failure(
                scope.action_request_id,
                scope.tool_name,
                "operation_ledger_unavailable",
                "the durable operation ledger rejected admission; no action was started",
                turn_id=scope.turn_id,
                status=503,
            )

    def _replay_operation(
        self,
        operation: HarnessOperation,
        scope: _ActionScope,
    ) -> dict[str, Any]:
        event = None
        if operation.terminal_event_id is not None:
            get_event = getattr(self._event_store, "get", None)
            if callable(get_event):
                try:
                    event = get_event(operation.terminal_event_id)
                except Exception:
                    event = None
        if (
            operation.recovery_state is not OperationRecoveryState.CLEARED
            or event is None
        ):
            return {
                "ok": False,
                "content": "The prior durable operation requires recovery reconciliation.",
                "data": {
                    "action_request_id": scope.action_request_id,
                    "state": "uncertain",
                    "execution_status": "uncertain",
                    "recovery_state": "required",
                    "evidence_class": LIVE_GATE_UNVERIFIED,
                },
                "event_id": operation.terminal_event_id,
                "error": "operation_recovery_required",
                "idempotent": True,
            }
        payload = event.payload if isinstance(getattr(event, "payload", None), Mapping) else {}
        adapter = payload.get("adapter_result") if isinstance(payload, Mapping) else None
        adapter_summary = _safe_adapter_summary(adapter)
        succeeded = (
            operation.terminal_state is OperationTerminalState.COMPLETED
            and getattr(event.status, "value", None) == "completed"
        )
        execution_status = payload.get("execution_status") if isinstance(payload, Mapping) else None
        if not isinstance(execution_status, str):
            execution_status = "completed" if succeeded else "not_started"
        result: dict[str, Any] = {
            "ok": succeeded,
            "content": "Replayed the prior durable Harness action result.",
            "data": {
                "action_request_id": scope.action_request_id,
                "state": "completed" if succeeded else execution_status,
                "execution_status": execution_status,
                "evidence_class": (
                    payload.get("evidence_class", LIVE_GATE_UNVERIFIED)
                    if isinstance(payload, Mapping)
                    else LIVE_GATE_UNVERIFIED
                ),
                **adapter_summary,
            },
            "event_id": operation.terminal_event_id,
            "idempotent": True,
        }
        if not succeeded:
            result["error"] = (
                payload.get("error_code", "operation_terminal_replay")
                if isinstance(payload, Mapping)
                else "operation_terminal_replay"
            )
        return result

    def _transition_operation(
        self,
        scope: _ActionScope,
        phase: OperationPhase,
    ) -> bool:
        ledger = self._operation_ledger
        owner_id = self._operation_owner_id
        if ledger is None or owner_id is None:
            return True
        try:
            operation = ledger.transition(
                self._operation_kind(scope),
                scope.action_request_id,
                phase=phase,
                expected_epoch=scope.epoch,
                owner_id=owner_id,
            )
            return not operation.is_terminal
        except Exception:
            return False

    def _mark_operation_boundary(self, scope: _ActionScope) -> bool:
        ledger = self._operation_ledger
        owner_id = self._operation_owner_id
        if ledger is None or owner_id is None:
            return True
        try:
            operation = ledger.mark_boundary(
                self._operation_kind(scope),
                scope.action_request_id,
                expected_epoch=scope.epoch,
                owner_id=owner_id,
            )
            return not operation.is_terminal
        except Exception:
            return False

    def _remember_action(self, scope: _ActionScope, result: dict[str, Any]) -> dict[str, Any]:
        with self._condition:
            prior = self._action_results.get(scope)
            if self._action_state.get(scope) == "terminal" and isinstance(prior, Mapping):
                # Terminal ownership is decided before the durable terminal
                # event is appended.  Any late caller is therefore a replay
                # of the winner, never a second terminal writer.
                self._condition.notify_all()
                return dict(prior)
            if self._action_state.get(scope) in _TERMINALIZING_ACTION_STATES:
                # The owner is still constructing the one durable terminal.
                # Do not replace its starting record or publish a competing
                # result from a late adapter thread.
                self._condition.notify_all()
                return dict(prior) if isinstance(prior, Mapping) else dict(result)
            self._action_scopes.setdefault(scope.action_request_id, scope)
            self._action_results[scope] = dict(result)
            self._action_inflight.discard(scope)
            data = result.get("data")
            execution_status = data.get("execution_status") if isinstance(data, Mapping) else None
            self._action_state[scope] = (
                "pending"
                if execution_status == "pending_approval"
                else "terminal"
            )
            if self._action_state[scope] == "terminal":
                self._action_deadlines.pop(scope, None)
            self._condition.notify_all()
        return result

    def _claim_terminal(self, scope: _ActionScope, *, uncertain: bool) -> bool:
        """Atomically claim the right to append the sole terminal event."""

        with self._condition:
            state = self._action_state.get(scope)
            if state not in {"pending", "starting", "running"}:
                return False
            self._action_state[scope] = (
                "terminalizing_uncertain" if uncertain else "terminalizing"
            )
            self._condition.notify_all()
            return True

    def _publish_terminal(
        self,
        scope: _ActionScope,
        result: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Publish a terminal result only for the current terminal owner."""

        with self._condition:
            state = self._action_state.get(scope)
            if state == "terminal":
                prior = self._action_results.get(scope)
                return dict(prior) if isinstance(prior, Mapping) else None
            if state not in _TERMINALIZING_ACTION_STATES:
                return None
            published = dict(result)
            self._action_results[scope] = published
            self._action_state[scope] = "terminal"
            self._action_inflight.discard(scope)
            self._running_actions.pop(scope, None)
            self._action_deadlines.pop(scope, None)
            self._condition.notify_all()
            return dict(published)

    def _await_terminal_result(self, scope: _ActionScope) -> dict[str, Any] | None:
        """Wait for the terminal owner to publish its result."""

        deadline = time.monotonic() + _ACTION_TERMINAL_WAIT_SECONDS
        with self._condition:
            while self._action_state.get(scope) in _TERMINALIZING_ACTION_STATES:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(timeout=min(remaining, 0.05))
            current = self._action_results.get(scope)
            return dict(current) if isinstance(current, Mapping) else None

    def settle_cancellations(
        self,
        *,
        timeout_seconds: float = 0.5,
        action_request_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        """Wait briefly for stop signals and classify survivors as UNCERTAIN."""

        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        with self._condition:
            while True:
                active = [
                    scope
                    for scope in self._action_scopes.values()
                    if self._action_state.get(scope) in _ACTIVE_ACTION_STATES
                    and (
                        action_request_ids is None
                        or scope.action_request_id in action_request_ids
                    )
                ]
                if not active:
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(timeout=min(remaining, 0.05))
            survivors = [
                scope
                for scope in self._action_scopes.values()
                if self._action_state.get(scope) in _ACTIVE_ACTION_STATES
                and (
                    action_request_ids is None
                    or scope.action_request_id in action_request_ids
                )
            ]
        uncertain_ids: list[str] = []
        for scope in survivors:
            with self._condition:
                if self._action_state.get(scope) not in _ACTIVE_ACTION_STATES:
                    continue
                # Claim before appending.  A worker that is just past its
                # backend return barrier must observe this state and lose the
                # durable terminal race rather than append success afterward.
                self._action_state[scope] = "terminalizing_uncertain"
                token = self._running_actions.get(scope)
                self._condition.notify_all()
            if token is not None:
                token.cancel("cancellation_unconfirmed")
            uncertain = self._terminal_failure_action(
                scope,
                {},
                error_code="sandbox_cancellation_uncertain",
                content="The sandbox cancellation was requested but process termination was not confirmed.",
                execution_status="uncertain",
                approval_id=None,
                epoch=scope.epoch,
                already_claimed=True,
            )
            if uncertain.get("data", {}).get("execution_status") == "uncertain":
                uncertain_ids.append(scope.action_request_id)
        return {
            "uncertain": len(uncertain_ids),
            "uncertain_action_request_ids": uncertain_ids,
        }

    def _publish_starting_action(
        self,
        scope: _ActionScope,
        *,
        approval_id: str,
    ) -> dict[str, Any]:
        """Publish a resumable starting result before queueing adapter work."""

        result = {
            "ok": False,
            "content": "Approval accepted; the sandbox action is starting.",
            "data": {
                "action_request_id": scope.action_request_id,
                "approval_id": approval_id,
                "state": "starting",
                "execution_status": "starting",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            },
            "event_id": None,
        }
        wait_for_terminal = False
        with self._condition:
            state = self._action_state.get(scope)
            if state == "terminal":
                current = self._action_results.get(scope)
                if isinstance(current, Mapping):
                    return dict(current)
                return result
            if state in _TERMINALIZING_ACTION_STATES:
                wait_for_terminal = True
            else:
                self._action_results[scope] = dict(result)
                self._action_state[scope] = "starting"
                self._action_inflight.discard(scope)
                self._condition.notify_all()
        if wait_for_terminal:
            current = self._await_terminal_result(scope)
            return result if current is None else current
        return result

    def _evict_completed_scope_locked(self) -> bool:
        """Evict only terminal entries; pending/running actions stay resumable."""

        for action_request_id, scope in tuple(self._action_scopes.items()):
            if (
                scope in self._action_inflight
                or scope in self._running_actions
                or self._action_state.get(scope) != "terminal"
            ):
                continue
            result = self._action_results.get(scope)
            data = result.get("data") if isinstance(result, Mapping) else None
            if isinstance(data, Mapping) and data.get("execution_status") == "pending_approval":
                continue
            self._action_scopes.pop(action_request_id, None)
            self._action_results.pop(scope, None)
            self._action_inputs.pop(scope, None)
            self._action_state.pop(scope, None)
            self._action_deadlines.pop(scope, None)
            return True
        return False

    def _remember_input(self, scope: _ActionScope, input_data: Mapping[str, Any]) -> None:
        with self._lock:
            self._action_inputs[scope] = dict(input_data)

    def _set_execution_deadline(
        self,
        scope: _ActionScope,
        input_data: Mapping[str, Any],
    ) -> None:
        requested = input_data.get("timeout", _DEFAULT_ACTION_TIMEOUT_SECONDS)
        if (
            isinstance(requested, bool)
            or not isinstance(requested, (int, float))
            or requested <= 0
        ):
            requested = _DEFAULT_ACTION_TIMEOUT_SECONDS
        timeout = min(float(requested), _MAX_ACTION_TIMEOUT_SECONDS)
        with self._condition:
            self._action_deadlines[scope] = time.monotonic() + timeout
            self._condition.notify_all()

    def _get_action_input(self, scope: _ActionScope) -> dict[str, Any] | None:
        with self._lock:
            value = self._action_inputs.get(scope)
            return None if value is None else dict(value)

    def _forget_input(self, scope: _ActionScope) -> None:
        with self._condition:
            self._action_inputs.pop(scope, None)
            self._action_deadlines.pop(scope, None)
            self._condition.notify_all()

    def wait_for_action(
        self,
        engram_id: str,
        action_request_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Hold the original Pi tool invocation until its approval settles.

        The HTTP/Gateway caller may therefore return the final result to the
        same Pi ``toolCallId``.  Approval waiting and adapter execution have
        separate deadlines.  If the execution deadline wins, the continuation
        cancels the action and waits for a cancelled/uncertain terminal result;
        it never returns a normal wait timeout while an adapter can still run.
        """

        with self._condition:
            scope = self._action_scopes.get(action_request_id)
            if scope is None:
                return self._failure(
                    action_request_id,
                    "unknown",
                    "action_request_unknown",
                    "the Harness action is no longer present in the bounded continuation registry",
                    turn_id="unknown",
                    status=404,
                )
            if scope.engram_id != engram_id:
                return self._failure(
                    action_request_id,
                    scope.tool_name,
                    "action_request_scope_conflict",
                    "the action belongs to another Engram",
                    turn_id=scope.turn_id,
                    status=409,
                )

        approval_id: str | None = None
        with self._condition:
            current = self._action_results.get(scope)
            if isinstance(current, Mapping):
                data = current.get("data")
                if isinstance(data, Mapping) and data.get("execution_status") == "pending_approval":
                    candidate = data.get("approval_id")
                    approval_id = candidate if isinstance(candidate, str) else None
                elif self._action_state.get(scope) != "terminal":
                    # Approval has transitioned to starting/running.  Do not
                    # return the progress record to the original Pi call;
                    # wait for the same action's terminal result.
                    approval_id = None
                else:
                    return dict(current)

        deadline = (
            None
            if timeout_seconds is None
            else time.monotonic() + max(0.0, float(timeout_seconds))
        )
        deadline_kind = "caller" if deadline is not None else None
        while True:
            if approval_id is not None:
                approval = self._approvals.get(approval_id)
                if approval is not None and approval.state is ApprovalState.EXPIRED:
                    return self._expire_action(scope, approval)
                if approval is not None and deadline is None:
                    expiry_remaining = approval.expires_at.timestamp() - time.time()
                    deadline = time.monotonic() + max(0.0, expiry_remaining) + 1.0
                    deadline_kind = "approval"

            expired_state: str | None = None
            with self._condition:
                current = self._action_results.get(scope)
                state = self._action_state.get(scope)
                if state == "terminal" and isinstance(current, Mapping):
                    return dict(current)
                execution_deadline = self._action_deadlines.get(scope)
                if state in _ACTIVE_ACTION_STATES:
                    if execution_deadline is not None:
                        if deadline_kind in {None, "approval"}:
                            deadline = execution_deadline
                        elif deadline is None:
                            deadline = execution_deadline
                        else:
                            deadline = min(deadline, execution_deadline)
                        deadline_kind = "execution"
                    # The approval has already handed the action to the
                    # adapter.  Its TTL must not continue to govern this
                    # wait, and expiry must never be reported as approval
                    # timeout after allow_once.
                    approval_id = None
                if state in _TERMINALIZING_ACTION_STATES:
                    self._condition.wait(timeout=0.05)
                    continue
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        expired_state = state
                    else:
                        self._condition.wait(timeout=min(remaining, 0.5))
                else:
                    self._condition.wait(timeout=0.5)
            if expired_state in _ACTIVE_ACTION_STATES:
                return self._cancel_continuation_deadline(scope)
            if expired_state == "pending" and approval_id is not None:
                approval = self._approvals.get(approval_id)
                if approval is not None and approval.state is ApprovalState.EXPIRED:
                    return self._expire_action(scope, approval)
                # A caller-side continuation deadline must also close a
                # still-pending approval; otherwise the original Pi call
                # could return while a later human allow starts the action.
                return self._cancel_continuation_deadline(scope)
            if expired_state is not None:
                return self._failure(
                    scope.action_request_id,
                    scope.tool_name,
                    "approval_wait_timeout",
                    "the approval continuation reached its bounded wait deadline",
                    turn_id=scope.turn_id,
                    status=409,
                )

    def _cancel_continuation_deadline(self, scope: _ActionScope) -> dict[str, Any]:
        """Cancel an action whose original Pi continuation deadline expired."""

        self.cancel_action(
            scope.engram_id,
            scope.action_request_id,
            reason="continuation_timeout",
        )
        # Give a cooperative adapter a bounded opportunity to publish its
        # cancelled terminal.  Survivors are durably classified UNCERTAIN so
        # the Pi call cannot outlive the process-control decision.
        self.settle_cancellations(
            timeout_seconds=0.5,
            action_request_ids={scope.action_request_id},
        )
        with self._condition:
            current = self._action_results.get(scope)
            if self._action_state.get(scope) == "terminal" and isinstance(current, Mapping):
                return dict(current)
        self.settle_cancellations(
            timeout_seconds=0.0,
            action_request_ids={scope.action_request_id},
        )
        with self._condition:
            current = self._action_results.get(scope)
            if self._action_state.get(scope) == "terminal" and isinstance(current, Mapping):
                return dict(current)
        return self._terminal_failure_action(
            scope,
            {},
            error_code="sandbox_cancellation_uncertain",
            content="The action continuation expired before sandbox termination was confirmed.",
            execution_status="uncertain",
            approval_id=None,
            epoch=scope.epoch,
        )

    def cancel_action(
        self,
        engram_id: str,
        action_request_id: str,
        *,
        reason: str = "cancelled",
    ) -> dict[str, Any]:
        """Cancel a pending approval or signal an executing sandbox action."""
        with self._condition:
            scope = self._action_scopes.get(action_request_id)
            if scope is None:
                return self._failure(
                    action_request_id,
                    "unknown",
                    "action_request_unknown",
                    "the Harness action is not present in the bounded cancellation registry",
                    turn_id="unknown",
                    status=404,
                )
            if scope.engram_id != engram_id:
                return self._failure(
                    action_request_id,
                    scope.tool_name,
                    "action_request_scope_conflict",
                    "cancellation does not match the action's Engram scope",
                    turn_id=scope.turn_id,
                    status=409,
                )
            token = self._running_actions.get(scope)
            current = self._action_results.get(scope)
            state = self._action_state.get(scope)
        # A known action is already fenced by its immutable scope.  Stopping
        # it must remain possible after lease loss; the lease check belongs to
        # dispatch/start, not to this stop path.
        if token is not None:
            token.cancel(reason)
            self._append_progress(scope, state="cancellation_requested", reason=reason)
            return {
                "ok": True,
                "content": "Cancellation was delivered to the sandbox adapter.",
                "data": {
                    "action_request_id": action_request_id,
                    "state": "cancelling",
                    "execution_status": "cancelling",
                    "reason": reason[:128],
                },
                "event_id": None,
            }
        if isinstance(current, Mapping):
            data = current.get("data")
            approval_id = data.get("approval_id") if isinstance(data, Mapping) else None
            pending = state == "pending" or (
                isinstance(data, Mapping)
                and data.get("execution_status") == "pending_approval"
            )
            if not pending:
                return dict(current, idempotent=True)
        else:
            approval_id = None
            pending = False
        if pending and isinstance(approval_id, str):
            try:
                self.resolve_approval(
                    approval_id,
                    {
                        "request_id": f"system-cancel-{action_request_id}",
                        "expected_turn_id": scope.turn_id,
                        "expected_epoch": scope.epoch,
                        "decision": "cancel",
                    },
                )
            except HarnessActionError:
                return self._failure(
                    action_request_id,
                    scope.tool_name,
                    "action_cancel_failed",
                    "the pending approval could not be cancelled",
                    turn_id=scope.turn_id,
                    status=409,
                )
            with self._lock:
                final = self._action_results.get(scope)
            if final is not None:
                return dict(final)
        return self._failure(
            action_request_id,
            scope.tool_name,
            "action_not_cancellable",
            "the Harness action is not pending or running",
            turn_id=scope.turn_id,
            status=409,
        )

    def cancel_all(self, *, reason: str = "runtime_lease_lost") -> dict[str, Any]:
        """Signal every known non-terminal action during Runtime teardown.

        This intentionally does not ask the current lease for an epoch.  The
        caller invokes it precisely when ownership is no longer provable, and
        already-started adapters still need their stop signal.
        """

        with self._condition:
            actions = [
                scope
                for scope in self._action_scopes.values()
                if self._action_state.get(scope) != "terminal"
            ]
        cancelled = 0
        for scope in actions:
            result = self.cancel_action(
                scope.engram_id,
                scope.action_request_id,
                reason=reason,
            )
            if result.get("ok") is True or result.get("error") in {
                "cancelled",
                "sandbox_cancelled",
            }:
                cancelled += 1
        return {
            "cancelled": cancelled,
            "action_request_ids": [scope.action_request_id for scope in actions],
        }

    def cancel_for_turn(
        self,
        engram_id: str,
        turn_id: str,
        *,
        epoch: int | None = None,
        reason: str = "turn_interrupt",
    ) -> dict[str, Any]:
        """Atomically fence admission, then cancel actions in one Pi turn."""

        if epoch is not None and (
            isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 1
        ):
            raise ValueError("epoch must be a positive integer when provided")
        with self._condition:
            matching = [
                scope
                for scope in self._action_scopes.values()
                if scope.engram_id == engram_id
                and scope.turn_id == turn_id
                and (epoch is None or scope.epoch == epoch)
            ]
            epochs = {scope.epoch for scope in matching}
            if epoch is not None:
                epochs.add(epoch)
            elif not epochs:
                try:
                    epochs.add(self._current_epoch())
                except HarnessActionError:
                    pass
            bounded_reason = (
                reason[:128] if isinstance(reason, str) else "turn_interrupt"
            )
            for fenced_epoch in sorted(epochs):
                self._turn_fences[(engram_id, turn_id, fenced_epoch)] = (
                    bounded_reason
                )
            while len(self._turn_fences) > self._max_actions:
                self._turn_fences.pop(next(iter(self._turn_fences)))
            active = [
                scope
                for scope in matching
                if self._action_state.get(scope)
                in {"pending", "starting", "running"}
            ]
            for scope in active:
                token = self._running_actions.get(scope)
                if token is not None:
                    token.cancel(bounded_reason)
            action_ids = [scope.action_request_id for scope in active]
            self._condition.notify_all()
        cancelled = 0
        for action_id in action_ids:
            result = self.cancel_action(engram_id, action_id, reason=reason)
            if result.get("ok") is True or result.get("error") is None:
                cancelled += 1
        return {
            "fenced": bool(epochs),
            "fenced_epochs": sorted(epochs),
            "cancelled": cancelled,
            "action_request_ids": action_ids[: self._max_actions],
        }

    def _expire_action(self, scope: _ActionScope, approval: Any) -> dict[str, Any]:
        with self._condition:
            current = self._action_results.get(scope)
            if isinstance(current, Mapping):
                data = current.get("data")
                if not (
                    isinstance(data, Mapping)
                    and data.get("execution_status") == "pending_approval"
                ):
                    return dict(current)
        self._append(
            turn_id=scope.turn_id,
            engram_id=scope.engram_id,
            kind=HarnessEventKind.APPROVAL_RESOLVED,
            phase=HarnessEventPhase.APPROVAL,
            source=HarnessEventSource.PULSE_CONTROL,
            status=HarnessEventStatus.FAILED,
            payload={
                "action_request_id": scope.action_request_id,
                "request_id": scope.action_request_id,
                "approval_id": approval.approval_id,
                "decision": "expire",
                "state": ApprovalState.EXPIRED.value,
                "epoch": scope.epoch,
                "reason_code": "approval_expired",
                "evidence_class": CONTRACT_ONLY,
            },
        )
        terminal = self._append_terminal(
            scope.turn_id,
            scope.engram_id,
            scope.action_request_id,
            scope.tool_name,
            status=HarnessEventStatus.FAILED,
            error_code="approval_expired",
            epoch=scope.epoch,
            preview=approval.safe_preview,
            approval_id=approval.approval_id,
        )
        result = {
            "ok": False,
            "content": "The approval expired before the action was executed.",
            "data": {
                "action_request_id": scope.action_request_id,
                "approval_id": approval.approval_id,
                "state": "expired",
                "execution_status": "not_started",
                "evidence_class": CONTRACT_ONLY,
            },
            "event_id": None if terminal is None else terminal.event_id,
            "error": "approval_expired",
        }
        self._forget_input(scope)
        return self._remember_action(scope, result)

    def _execute_backend(
        self,
        scope: _ActionScope,
        input_data: Mapping[str, Any],
        preview: Mapping[str, Any],
        *,
        approval_id: str | None,
        token: ActionCancellationToken | None = None,
    ) -> dict[str, Any]:
        backend = self._backend
        if backend is None:
            return self._failure(
                scope.action_request_id,
                scope.tool_name,
                "sandbox_backend_unavailable",
                "no verified workspace sandbox backend is configured",
                turn_id=scope.turn_id,
            )
        token_was_supplied = token is not None
        token = token or ActionCancellationToken()
        try:
            execution_epoch = self._current_epoch()
        except HarnessActionError:
            execution_epoch = None
        if execution_epoch != scope.epoch:
            # Approval and executor submission may be separated by queueing.
            # Re-fence at worker entry so a job approved under an old Runtime
            # lease never reaches the adapter after another owner takes over.
            token.cancel("runtime_lease_lost")
            return self._terminal_failure_action(
                scope,
                preview,
                error_code="runtime_lease_lost",
                content=(
                    "The Runtime lease changed while the sandbox action was "
                    "queued; no action was started."
                ),
                execution_status="not_started",
                approval_id=approval_id,
                epoch=scope.epoch,
            )
        with self._condition:
            current_token = self._running_actions.get(scope)
            current_state = self._action_state.get(scope)
            turn_fenced = self._turn_is_fenced_locked(scope)
            can_start = not turn_fenced and current_state == "starting" and (
                current_token is token
                or (not token_was_supplied and current_token is None)
            )
            if can_start:
                # The transition token is the queued job's ownership proof.
                # A settled/closed action may still be waiting in the bounded
                # executor; it must never turn terminal back into running.
                self._running_actions[scope] = token
                self._action_state[scope] = "running"
                self._condition.notify_all()
        if turn_fenced:
            token.cancel("turn_interrupt")
            return self._terminal_failure_action(
                scope,
                preview,
                error_code="turn_interrupt_fenced",
                content=(
                    "The Harness turn was interrupted before the adapter "
                    "boundary; no action was started."
                ),
                execution_status="not_started",
                approval_id=approval_id,
                epoch=scope.epoch,
            )
        if not can_start:
            # A queued worker can arrive after cancellation or settlement.
            # Replay the terminal owner and leave the durable ledger, token,
            # progress stream, and adapter untouched.  The fallback is only a
            # local fail-closed result if the owner has not published within
            # the bounded recovery wait; _remember_action will not replace a
            # terminalizing/terminal winner.
            winner = self._await_terminal_result(scope)
            if winner is not None:
                return winner
            return {
                "ok": False,
                "content": "The queued sandbox action was settled before execution started.",
                "data": {
                    "action_request_id": scope.action_request_id,
                    "approval_id": approval_id,
                    "state": "uncertain",
                    "execution_status": "uncertain",
                    "recovery_state": "uncertain",
                    "evidence_class": LIVE_GATE_UNVERIFIED,
                },
                "event_id": None,
                "error": "sandbox_queued_after_settlement",
            }
        if not self._transition_operation(scope, OperationPhase.STARTING):
            token.cancel("operation_ledger_unavailable")
            return self._terminal_failure_action(
                scope,
                preview,
                error_code="operation_ledger_unavailable",
                content=(
                    "The durable operation ledger could not fence adapter "
                    "startup; no action was started."
                ),
                execution_status="not_started",
                approval_id=approval_id,
                epoch=scope.epoch,
            )
        try:
            if token.is_set():
                self._append_progress(
                    scope,
                    state="cancelled_before_start",
                    approval_id=approval_id,
                    reason=token.reason or "cancelled",
                )
                raw = {
                    "ok": False,
                    "error": "sandbox_cancelled",
                    "status": "cancelled",
                    "recovery_state": "not_started",
                }
            else:
                self._append_progress(scope, state="running", approval_id=approval_id)
                try:
                    execution_epoch = self._current_epoch()
                except HarnessActionError:
                    execution_epoch = None
                if execution_epoch != scope.epoch:
                    # Keep the check adjacent to the adapter boundary as well
                    # as worker entry.  Runtime lease-loss callbacks still
                    # carry the cancellation token across the remaining race.
                    token.cancel("runtime_lease_lost")
                    raw = {
                        "ok": False,
                        "error": "runtime_lease_lost",
                        "status": "failed",
                        "recovery_state": "not_started",
                    }
                else:
                    if not self._mark_operation_boundary(scope):
                        return self._terminal_failure_action(
                            scope,
                            preview,
                            error_code="operation_boundary_persistence_failed",
                            content=(
                                "The durable adapter boundary could not be "
                                "recorded; no action was started."
                            ),
                            execution_status="not_started",
                            approval_id=approval_id,
                            epoch=scope.epoch,
                        )
                    try:
                        execute_kwargs: dict[str, Any] = {
                            "action_request_id": scope.action_request_id,
                            "engram_id": scope.engram_id,
                            "turn_id": scope.turn_id,
                            "epoch": scope.epoch,
                            "tool_name": scope.tool_name,
                            "input_data": dict(input_data),
                            "policy_preview": dict(preview),
                            "signal": token,
                        }
                        supports_progress_for = getattr(
                            backend, "supports_progress_for", None
                        )
                        supports_progress = (
                            bool(supports_progress_for(scope.tool_name))
                            if callable(supports_progress_for)
                            else bool(getattr(backend, "supports_progress", False))
                        )
                        if supports_progress:
                            execute_kwargs["progress_callback"] = (
                                lambda update: self._append_command_output(
                                    scope,
                                    update,
                                    approval_id=approval_id,
                                )
                            )
                        raw = backend.execute(**execute_kwargs)
                    except Exception:
                        raw = {
                            "ok": False,
                            "error": "sandbox_execution_failed",
                            "status": "uncertain",
                            "recovery_state": "uncertain",
                        }
            if token.is_set() and isinstance(raw, Mapping) and raw.get("ok") is True:
                raw = {
                    **dict(raw),
                    "ok": False,
                    "error": "sandbox_cancel_race",
                    "status": "uncertain",
                    "recovery_state": "uncertain",
                }
            return self._normalise_backend_result(
                scope,
                preview,
                raw,
            )
        finally:
            with self._condition:
                if self._running_actions.get(scope) is token:
                    self._running_actions.pop(scope, None)
                self._condition.notify_all()

    def _append_progress(
        self,
        scope: _ActionScope,
        *,
        state: str,
        approval_id: str | None = None,
        reason: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "action_request_id": scope.action_request_id,
            "tool_name": scope.tool_name,
            "epoch": scope.epoch,
            "state": state,
            "execution_status": state,
            "evidence_class": LIVE_GATE_UNVERIFIED,
        }
        if approval_id is not None:
            payload["approval_id"] = approval_id
        if reason is not None:
            payload["reason"] = reason[:128]
        self._append(
            turn_id=scope.turn_id,
            engram_id=scope.engram_id,
            kind=HarnessEventKind.TOOL_PROGRESS,
            phase=HarnessEventPhase.STREAM,
            source=HarnessEventSource.POLICY,
            status=HarnessEventStatus.RUNNING,
            payload=payload,
        )

    def _append_command_output(
        self,
        scope: _ActionScope,
        update: Any,
        *,
        approval_id: str | None,
    ) -> bool:
        """Project one already-redacted terminal chunk into durable replay."""

        if not isinstance(update, Mapping):
            return False
        stream = update.get("stream")
        chunk = update.get("chunk")
        output_seq = update.get("output_seq")
        byte_count = update.get("bytes")
        if (
            stream not in {"stdout", "stderr"}
            or not isinstance(chunk, str)
            or isinstance(output_seq, bool)
            or not isinstance(output_seq, int)
            or output_seq < 1
            or isinstance(byte_count, bool)
            or not isinstance(byte_count, int)
            or byte_count < 0
        ):
            return False
        payload: dict[str, Any] = {
            "action_request_id": scope.action_request_id,
            "tool_name": scope.tool_name,
            "epoch": scope.epoch,
            "stream": stream,
            "output_seq": output_seq,
            "chunk": chunk,
            "bytes": min(byte_count, 4 * 1024 * 1024),
            "truncated": update.get("truncated") is True,
            "evidence_class": LIVE_GATE_UNVERIFIED,
        }
        if approval_id is not None:
            payload["approval_id"] = approval_id
        return self._append(
            turn_id=scope.turn_id,
            engram_id=scope.engram_id,
            kind=HarnessEventKind.COMMAND_OUTPUT,
            phase=HarnessEventPhase.STREAM,
            source=HarnessEventSource.TERMINAL,
            status=HarnessEventStatus.RUNNING,
            payload=payload,
        ) is not None

    def _remember_resolution(
        self,
        key: tuple[str, str],
        result: dict[str, Any],
    ) -> dict[str, Any]:
        with self._lock:
            self._resolution_results[key] = dict(result)
            self._prune_locked(self._resolution_results)
        return result

    def _prune_locked(self, values: dict[Any, Any]) -> None:
        while len(values) > self._max_actions:
            values.pop(next(iter(values)))

    def _normalise_backend_result(
        self,
        scope: _ActionScope,
        preview: Mapping[str, Any],
        raw: Mapping[str, Any],
    ) -> dict[str, Any]:
        mapping = raw if isinstance(raw, Mapping) else {}
        if not self._transition_operation(scope, OperationPhase.ADAPTER_RETURNED):
            mapping = {
                **dict(mapping),
                "ok": False,
                "error": "operation_ledger_unavailable",
                "status": "uncertain",
                "recovery_state": "uncertain",
            }
        nested_data = mapping.get("data")
        nested_mapping = nested_data if isinstance(nested_data, Mapping) else {}
        raw_status = mapping.get("status")
        status_value = raw_status if isinstance(raw_status, str) else "failed"
        adapter_summary = _safe_adapter_summary(mapping)
        backend_binding: dict[str, Any] = {}
        live_evidence_classes = {
            LIVE_OS_RESTRICTED,
            LIVE_WORKSPACE_CHECKPOINTED,
        }
        try:
            evidence_for = (
                None
                if self._backend is None
                else getattr(self._backend, "evidence_for", None)
            )
            backend_evidence = (
                evidence_for(scope.tool_name, preview)
                if callable(evidence_for)
                else (
                    self._backend.evidence_class
                    if self._backend is not None
                    else LIVE_GATE_UNVERIFIED
                )
            )
            if backend_evidence in live_evidence_classes and self._backend is not None:
                binding_for = getattr(self._backend, "evidence_binding_for", None)
                binding = (
                    binding_for(scope.tool_name, preview)
                    if callable(binding_for)
                    else self._backend.evidence_binding
                )
                safe_backend = _safe_adapter_summary(
                    {"evidence_binding": binding}
                ).get("evidence_binding")
                if isinstance(safe_backend, Mapping) and safe_backend:
                    backend_binding = dict(safe_backend)
                else:
                    backend_evidence = LIVE_GATE_UNVERIFIED
                if (
                    preview.get("execution_mode")
                    == "background_pipe_session"
                    and not _background_evidence_binding_is_complete(
                        backend_binding
                    )
                ):
                    backend_binding = {}
                    backend_evidence = LIVE_GATE_UNVERIFIED
        except Exception:
            backend_evidence = LIVE_GATE_UNVERIFIED
        raw_evidence = mapping.get("evidence_class", nested_mapping.get("evidence_class"))
        raw_binding = adapter_summary.get("evidence_binding")
        binding_mismatch = (
            mapping.get("ok") is True
            and raw_evidence in live_evidence_classes
            and backend_evidence in live_evidence_classes
            and (
                raw_evidence != backend_evidence
                or
                not isinstance(raw_binding, Mapping)
                or dict(raw_binding) != backend_binding
            )
        )
        evidence_mismatch = mapping.get("ok") is True and (
            (raw_evidence in live_evidence_classes)
            != (backend_evidence in live_evidence_classes)
        ) or binding_mismatch
        uncertain = (
            status_value == "uncertain"
            or adapter_summary.get("recovery_state") == "uncertain"
            or adapter_summary.get("stream_observation_failed") is True
            or mapping.get("error") == "sandbox_cleanup_uncertain"
            or evidence_mismatch
        )
        succeeded = mapping.get("ok") is True and not uncertain
        error = mapping.get("error")
        error_code = (
            "none"
            if succeeded
            else (
                "sandbox_evidence_mismatch"
                if evidence_mismatch
                else (
                    "command_output_projection_failed"
                    if adapter_summary.get("stream_observation_failed") is True
                    else (error if isinstance(error, str) and error else "sandbox_execution_failed")
                )
            )
        )
        evidence_class = (
            str(backend_evidence)
            if succeeded
            and raw_evidence == backend_evidence
            and backend_evidence in live_evidence_classes
            else LIVE_GATE_UNVERIFIED
        )
        if evidence_class not in live_evidence_classes:
            adapter_summary.pop("evidence_binding", None)
        execution_status = "completed" if succeeded else (
            "uncertain" if uncertain else status_value
        )
        event_status = (
            HarnessEventStatus.COMPLETED
            if succeeded
            else (
                HarnessEventStatus.UNCERTAIN
                if uncertain
                else (
                    HarnessEventStatus.CANCELLED
                    if status_value == "cancelled"
                    else HarnessEventStatus.FAILED
                )
            )
        )
        if not self._claim_terminal(scope, uncertain=uncertain):
            prior = self._await_terminal_result(scope)
            if prior is not None:
                return prior
            return self._terminal_failure_action(
                scope,
                preview,
                error_code="sandbox_terminal_owner_lost",
                content="The sandbox terminal owner did not publish a durable result.",
                execution_status="uncertain",
                approval_id=None,
                epoch=scope.epoch,
            )
        event = self._append_terminal(
            scope.turn_id,
            scope.engram_id,
            scope.action_request_id,
            scope.tool_name,
            status=event_status,
            error_code=error_code,
            epoch=scope.epoch,
            preview=preview,
            adapter_summary=adapter_summary,
            execution_status=execution_status,
            evidence_class=evidence_class,
        )
        if event is None:
            # The adapter boundary has already been crossed.  Without the
            # canonical terminal append, neither replay nor a successor
            # Runtime can prove whether the external effect completed.  A
            # successful process result must therefore never escape as
            # success when its durable settlement is missing.
            adapter_summary.pop("evidence_binding", None)
            adapter_summary["recovery_state"] = "uncertain"
            result = {
                "ok": False,
                "content": (
                    "The sandbox action returned, but its durable terminal "
                    "record could not be persisted."
                ),
                "data": {
                    "action_request_id": scope.action_request_id,
                    "state": "uncertain",
                    "execution_status": "uncertain",
                    "evidence_class": LIVE_GATE_UNVERIFIED,
                    **adapter_summary,
                },
                "event_id": None,
                "error": "sandbox_terminal_persistence_failed",
            }
        else:
            ephemeral_content = mapping.get("ephemeral_content")
            result = {
                "ok": succeeded,
                "content": (
                    ephemeral_content[:1_000_000]
                    if succeeded and isinstance(ephemeral_content, str)
                    else "The restricted sandbox backend completed the action."
                    if succeeded
                    else "The restricted sandbox backend did not complete the action."
                ),
                "data": {
                    "action_request_id": scope.action_request_id,
                    "state": "completed" if succeeded else execution_status,
                    "execution_status": execution_status,
                    "evidence_class": evidence_class,
                    **adapter_summary,
                },
                "event_id": event.event_id,
                **({} if succeeded else {"error": error_code}),
            }
        published = self._publish_terminal(scope, result)
        return result if published is None else published

    @staticmethod
    def _resolution_result(
        approval: Any,
        resolution: ApprovalResolution,
    ) -> dict[str, Any]:
        terminal_decision = resolution.state in {
            ApprovalState.DENIED,
            ApprovalState.CANCELLED,
            ApprovalState.EXPIRED,
            ApprovalState.REVOKED,
        }
        return {
            "accepted": resolution.accepted,
            "state": resolution.state.value if resolution.state else "rejected",
            # ``accepted`` means the human decision was accepted by the
            # registry; deny/cancel are still terminal tool failures and must
            # return an explicit error to the original Pi tool call.
            "error_code": resolution.reason_code if terminal_decision else None,
            "approval_id": approval.approval_id,
            "action_request_id": approval.request_id,
            "approval_accepted": resolution.accepted,
            "execution_status": "awaiting_adapter" if resolution.accepted else "not_started",
            "idempotent": resolution.idempotent,
            "evidence_class": resolution.evidence_class,
        }


def _safe_adapter_summary(value: Mapping[str, Any] | Any) -> dict[str, Any]:
    """Keep adapter output bounded and free of raw command/file arguments."""

    if not isinstance(value, Mapping):
        return {}
    nested = value.get("data")
    source = dict(value)
    if isinstance(nested, Mapping):
        for nested_key, nested_value in nested.items():
            source.setdefault(nested_key, nested_value)
    result: dict[str, Any] = {}
    for key in (
        "exit_code",
        "duration_ms",
        "truncated",
        "stream_observation_failed",
        "recovery_state",
        "stdout",
        "stderr",
        "adapter_state",
        "evidence_binding",
        "checkpoint_id",
        "manifest_digest",
        "diff_digest",
        "before_digest",
        "after_digest",
        "post_digest",
        "change_kind",
        "diff_preview",
        "changed_paths",
        "applied_paths",
        "failed_paths",
        "mcp_server_id",
        "mcp_tool_name",
        "mcp_server_identity_digest",
        "mcp_capability_digest",
        "mcp_arguments_digest",
        "mcp_result_digest",
        "mcp_content_items",
        "mcp_is_error",
        "linked_terminal_session_id",
        "mode",
        "transport",
        "session_scope",
        "session_state",
        "command_digest",
        "sandbox_evidence",
        "tree_containment",
        "process_tree_state",
    ):
        candidate = source.get(key)
        if key in {"exit_code", "duration_ms"}:
            if isinstance(candidate, int) and not isinstance(candidate, bool):
                result[key] = max(-1, min(3_600_000, candidate))
        elif key in {"truncated", "stream_observation_failed"}:
            if isinstance(candidate, bool):
                result[key] = candidate
        elif key == "mcp_is_error":
            if isinstance(candidate, bool) or candidate is None:
                result[key] = candidate
        elif key in {
            "stdout",
            "stderr",
            "recovery_state",
            "adapter_state",
            "diff_preview",
        }:
            if isinstance(candidate, str):
                result[key] = candidate[:16_384]
        elif key in {
            "checkpoint_id",
            "manifest_digest",
            "diff_digest",
            "before_digest",
            "after_digest",
            "post_digest",
            "change_kind",
            "mcp_server_id",
            "mcp_tool_name",
            "mcp_server_identity_digest",
            "mcp_capability_digest",
            "mcp_arguments_digest",
            "mcp_result_digest",
            "linked_terminal_session_id",
            "mode",
            "transport",
            "session_scope",
            "session_state",
            "command_digest",
            "sandbox_evidence",
            "tree_containment",
            "process_tree_state",
        }:
            if isinstance(candidate, str) and candidate:
                result[key] = candidate[:256]
        elif key == "mcp_content_items":
            if isinstance(candidate, int) and not isinstance(candidate, bool):
                result[key] = max(0, min(candidate, 256))
        elif key in {"changed_paths", "applied_paths", "failed_paths"}:
            if isinstance(candidate, (list, tuple)):
                paths = [
                    safe
                    for item in candidate[:128]
                    if (safe := _safe_adapter_path(item)) is not None
                ]
                result[key] = paths
        elif key == "evidence_binding" and isinstance(candidate, Mapping):
            allowed = {
                "gate_id",
                "artifact_version",
                "permission_profile",
                "executable_version",
                "executable_sha256",
                "executable_path_digest",
                "workspace_boundary_digest",
                "codex_config_path_digest",
                "codex_config_sha256",
                "sandbox_implementation",
                "sandbox_implementation_source",
                "adapter_version",
                "adapter",
                "filesystem_mode",
                "manifest_version",
                "manifest_digest",
                "checkpoint_root_digest",
                "checkpoint_boundary_digest",
                "expires_at",
                "sandbox_gate_id",
                "lifecycle_gate_id",
                "transport",
                "session_scope",
                "sandbox_evidence",
                "tree_containment",
                "workspace_write_denied",
                "environment_sentinel",
                "confidential_read_isolation",
                "background_lifecycle",
                "backend_implementation",
            }
            bounded = {
                str(binding_key): binding_value[:256]
                for binding_key, binding_value in candidate.items()
                if binding_key in allowed and isinstance(binding_value, str)
            }
            if bounded:
                result[key] = bounded
    return result


def _safe_adapter_path(value: Any) -> str | None:
    """Bound a workspace-relative adapter path without resolving the host FS."""

    if not isinstance(value, str) or not value or len(value) > 512 or "\x00" in value:
        return None
    normalized = value.replace("\\", "/")
    if normalized.startswith(("/", "//")) or (
        len(normalized) >= 2 and normalized[0].isalpha() and normalized[1] == ":"
    ):
        return None
    parts = normalized.split("/")
    if any(not part or part in {".", ".."} or ":" in part for part in parts):
        return None
    return "/".join(parts)


def _safe_external_policy_preview(value: Any) -> dict[str, Any]:
    """Keep MCP/other capability previews useful without raw arguments."""

    if not isinstance(value, Mapping):
        return {}
    output: dict[str, Any] = {}
    for key in (
        "operation",
        "server_id",
        "tool_name",
        "arguments_digest",
        "descriptor_digest",
        "capability_digest",
        "capability_state",
        "execution_safety",
        "registry_descriptor_id",
        "registry_provenance_digest",
        "registry_status",
        "registry_reason",
        "registry_evidence_class",
    ):
        candidate = value.get(key)
        if isinstance(candidate, str) and candidate:
            output[key] = candidate[:256]
    timeout = value.get("timeout_seconds")
    if isinstance(timeout, (int, float)) and not isinstance(timeout, bool):
        output["timeout_seconds"] = max(0.0, min(float(timeout), 300.0))
    return output


def _valid_identity(value: Any) -> bool:
    return isinstance(value, str) and 0 < len(value.strip()) <= 256

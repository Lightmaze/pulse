"""The first-class tools of one Autonomous Life Runtime.

The service is deliberately below the HTTP layer and above the frozen
``CausalLedger``/``WorldRegistry`` contracts.  Its only identity input is the
Engram owner supplied by the loopback capability Gateway plus the Pi
``ToolInvocationContext``.  No tool argument can select a different subject.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from pulse_system.agent.harness.actions import MUTABLE_PI_TOOL_NAMES
from pulse_system.agent.harness.purpose_governance import (
    PurposeAmendmentKind,
    PurposeAmendmentProposal,
    PurposeAmendmentProposalState,
    PurposeGovernance,
    PurposeGovernanceError,
    PurposeProposalConflictError,
    PurposeReflectionRequiredError,
    PurposeRevision,
    PurposeRevisionConflictError,
)
from pulse_system.agent.harness.role_leases import (
    HolderKind,
    RoleClass,
    RoleContributionEvidence,
    RoleContributionKind,
    RoleLease,
    RoleLeaseError,
    RoleLeaseStatus,
    RoleLeaseStore,
    RoleObligation,
    RoleRenewalEvidence,
    RoleReceiptVerifier,
    RoleScope,
    RuntimeLeaseProof,
)
from pulse_system.agent.harness.task_worker_runtime import TASK_WORKER_TOOL_NAMES
from pulse_system.agent.tools.gateway import ToolInvocationContext
from pulse_system.core.causality import (
    CausalLedger,
    CausalTransitionError,
    RuntimeFence,
)
from pulse_system.core.habitat import Action, ManagedHabitat, Reply
from pulse_system.core.types import (
    ActivityCenterStatus,
    ActivityKind,
    ActivityOrigin,
    CausalEventDomain,
    CausalEventFlow,
    CausalEventKind,
    CausalEventSource,
    CausalEventStatus,
    EngramStatus,
    LivingConcernDisposition,
    LivingOrientationState,
    MembershipRelation,
    TaskRelationshipStatus,
)
from pulse_system.core.world import WorldRegistry
from pulse_system.service.living_portfolio import (
    LivingPortfolioProjector,
    LivingPortfolioRecoveryError,
    LivingPortfolioValidationError,
)
from pulse_system.substrate.storage import Storage

TOOL_NAMES = (
    "pulse_task_offer_respond",
    "pulse_task_relationship_respond",
    "pulse_life_list",
    "pulse_life_portfolio",
    "pulse_life_concerns",
    "pulse_life_hold",
    "pulse_life_orientations",
    "pulse_life_orient",
    "pulse_life_create",
    "pulse_life_update",
    "pulse_life_purpose",
    "pulse_life_amend_purpose",
    "pulse_life_roles",
    "pulse_life_accept_role",
    "pulse_life_renew_role",
    "pulse_life_release_role",
    "pulse_habitat_observe",
    "pulse_habitat_act",
    "pulse_habitat_subscribe",
    "pulse_delegate",
    "pulse_mcp_call",
    "pulse_task_spawn",
    "pulse_task_wait",
    "pulse_task_steer",
    "pulse_task_stop",
)

_TOOL_SET = frozenset(TOOL_NAMES)
_READ_PROXY_NAMES = frozenset({"read"})
_UNATTENDED_DENY = frozenset({"bash", "write", "edit"})
_READ_ONLY = frozenset({"read", "grep", "find", "ls"})
_TASK_CONTEXT_BLOCKED_LIFE_MUTATIONS = frozenset({
    "pulse_life_hold",
    "pulse_life_orient",
    "pulse_life_create",
    "pulse_life_update",
    "pulse_life_accept_role",
    "pulse_life_renew_role",
    "pulse_life_release_role",
    "pulse_habitat_act",
    "pulse_habitat_subscribe",
})
_TASK_OFFER_RESPONSE_TOOL = "pulse_task_offer_respond"
_TASK_RELATIONSHIP_RESPONSE_TOOL = "pulse_task_relationship_respond"
_TASK_OFFER_ALLOWED_TOOLS = frozenset({
    _TASK_OFFER_RESPONSE_TOOL,
    "pulse_life_list",
    "pulse_life_portfolio",
    "pulse_life_concerns",
    "pulse_life_orientations",
    "pulse_life_purpose",
    "pulse_life_roles",
    "pulse_habitat_observe",
})
_TASK_OFFER_DECISION_STATUS = {
    "accept": "accepted",
    "refuse": "refused",
    "request_changes": "changes_requested",
}
_TASK_OFFER_PUBLIC_ERRORS = frozenset({
    "task_offer_already_resolved",
    "task_offer_context_required",
    "task_offer_response_required",
    "task_offer_revision_conflict",
    "task_offer_service_unavailable",
    "task_offer_subject_mismatch",
})
_TASK_OFFER_SERVICE_ERROR_MAP = {
    "invalid_task_offer": "task_offer_context_required",
    "task_offer_inconsistent": "task_offer_service_unavailable",
    "task_offer_subject_inactive": "task_offer_subject_mismatch",
    "unknown_task_offer": "task_offer_context_required",
    "unknown_task_offer_subject": "task_offer_subject_mismatch",
}
_TASK_RELATIONSHIP_ALLOWED_TOOLS = frozenset({
    _TASK_RELATIONSHIP_RESPONSE_TOOL,
    "pulse_life_list",
    "pulse_life_portfolio",
    "pulse_life_concerns",
    "pulse_life_orientations",
    "pulse_life_purpose",
    "pulse_life_roles",
})
_TASK_RELATIONSHIP_ACTION_STATUS = {
    "pause": "paused",
    "request_changes": "renegotiation_requested",
    "resume": "active",
    "exit": "exited",
}
_TASK_RELATIONSHIP_PUBLIC_ERRORS = frozenset({
    "task_relationship_already_resolved",
    "task_relationship_context_required",
    "task_relationship_exited",
    "task_relationship_response_required",
    "task_relationship_revision_conflict",
    "task_relationship_service_unavailable",
    "task_relationship_subject_mismatch",
    "task_relationship_transition_invalid",
})
_TASK_RELATIONSHIP_SERVICE_ERROR_MAP = {
    "invalid_task_relationship": "task_relationship_context_required",
    "task_relationship_center_unavailable": (
        "task_relationship_service_unavailable"
    ),
    "task_relationship_effect_collision": (
        "task_relationship_already_resolved"
    ),
    "task_relationship_inconsistent": "task_relationship_service_unavailable",
    "task_relationship_subject_inactive": "task_relationship_subject_mismatch",
    "unknown_task_relationship": "task_relationship_context_required",
    "unknown_task_relationship_subject": "task_relationship_subject_mismatch",
}
_ALLOWED_LIFE_KINDS = frozenset(
    {kind.value for kind in ActivityKind if kind is not ActivityKind.TASK}
)
_PRIVATE_MARKERS = (
    ".pulse",
    "pulse_tool_capability",
    "pulse_tool_gateway",
    "pulse-system-state",
)
_PATH_INPUT_KEYS = frozenset(
    {"path", "paths", "file", "file_path", "directory", "dir", "cwd", "root"}
)


class TaskOfferServiceProtocol(Protocol):
    """Frozen subject-facing seam owned by ``TaskOfferService``.

    The service owns durable state and exactly-once decision semantics.  This
    caller supplies only identities proven by the running causal root; none
    of them are accepted from model-controlled tool arguments.
    """

    def respond(
        self,
        offer_id: str,
        expected_revision: int,
        subject_engram_id: str,
        decision: str,
        response: str | None = None,
        decision_event_id: str | None = None,
    ) -> Any: ...


class TaskRelationshipServiceProtocol(Protocol):
    """Minimal seam for one subject-authored lifecycle decision.

    The durable service owns the aggregate, CAS transition, current-successor
    ownership and root-to-relationship checks.  ``source_event_id`` points
    at the causal TOOL_CALL whose parent is the non-model-controlled root, so
    no parallel context model is needed here.
    """

    def respond(
        self,
        *,
        relationship_id: str,
        expected_revision: int,
        subject_engram_id: str,
        action: str,
        response: str | None = None,
        source_event_id: str,
    ) -> Any: ...

    def get_for_center(self, center_id: str) -> Any: ...


class LifeToolService:
    """Dispatch Pulse tools and persist their causal facts."""

    def __init__(
        self,
        storage: Storage,
        ledger: CausalLedger,
        world: WorldRegistry,
        habitat: ManagedHabitat,
        *,
        world_id: str,
        workspace: str | os.PathLike[str],
        delegation_tunnel: Any = None,
        delegator: Any = None,
        metrics: Any = None,
        action_gateway: Callable[..., Mapping[str, Any]] | None = None,
        action_waiter: Callable[..., Mapping[str, Any]] | None = None,
        task_worker_gateway: Callable[..., Mapping[str, Any]] | None = None,
        purpose_governance: PurposeGovernance | None = None,
        lineage_resolver: Callable[[str], str] | None = None,
        role_store: RoleLeaseStore | None = None,
        runtime_lease_provider: Callable[[], RuntimeLeaseProof] | None = None,
        workspace_receipt_resolver: (
            Callable[..., Mapping[str, Any] | None] | None
        ) = None,
        task_offer_service: TaskOfferServiceProtocol | None = None,
        task_relationship_service: TaskRelationshipServiceProtocol | None = None,
        task_relationship_revoker: Callable[..., Mapping[str, Any]] | None = None,
        runtime_fence_provider: Callable[[], RuntimeFence] | None = None,
        max_workers: int = 2,
    ) -> None:
        self._storage = storage
        self._ledger = ledger
        self._world = world
        self._habitat = habitat
        self._world_id = world_id
        self._workspace = Path(workspace).resolve()
        self._delegation_tunnel = delegation_tunnel
        self._delegator = delegator
        self._metrics = metrics
        self._action_gateway = action_gateway
        self._action_waiter = action_waiter
        self._task_worker_gateway = task_worker_gateway
        self._purpose_governance = purpose_governance
        self._purpose_settlement_health = "healthy"
        self._purpose_settlement_last_error: str | None = None
        self._lineage_resolver = lineage_resolver
        self._role_store = role_store
        self._runtime_lease_provider = runtime_lease_provider
        self._role_receipt_verifier = RoleReceiptVerifier(
            workspace_resolver=workspace_receipt_resolver,
            habitat_resolver=self._habitat.verify_effect_receipt,
        )
        self._task_offer_service = task_offer_service
        self._task_relationship_service = task_relationship_service
        self._task_relationship_revoker = task_relationship_revoker
        if runtime_fence_provider is not None and not callable(
            runtime_fence_provider
        ):
            raise ValueError("runtime_fence_provider must be callable or null")
        self._runtime_fence_provider = runtime_fence_provider
        self._jobs_lock = threading.RLock()
        self._jobs: dict[str, Future] = {}
        self._tool_result_data: dict[str, dict[str, Any]] = {}
        # Stable child IDs make retries idempotent.  Synchronization is scoped
        # to one Engram: a pending approval or a long adapter call may hold
        # that Engram's exactly-once lock, but it must never freeze another
        # subject's read/life dispatch on this shared LifeToolService.
        self._dispatch_locks_guard = threading.Lock()
        self._dispatch_locks: dict[str, threading.RLock] = {}
        self._closed = False
        self._close_summary: dict[str, Any] | None = None
        self._close_done = threading.Event()
        self._owner_drained = threading.Event()
        # A delegation result is first recorded as a terminal child, then
        # delivered as a queued turn root.  Rebuild a missing delivery after a
        # crash from that durable fact before accepting new tool calls.
        self._recover_delegation_deliveries()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="pulse-life-delegate",
        )

    @property
    def habitat(self) -> ManagedHabitat:
        return self._habitat

    def _runtime_fence(self) -> RuntimeFence | None:
        provider = self._runtime_fence_provider
        return None if provider is None else provider()

    def dispatch(
        self,
        engram_id: str,
        tool_name: str,
        args: Mapping[str, Any] | None = None,
        invocation: ToolInvocationContext | None = None,
    ) -> dict[str, Any]:
        """Dispatch one tool for a capability owner.

        Direct callers without a ``ToolInvocationContext`` fail closed.  The
        Runtime's Gateway always supplies one from the non-model-controlled
        Pi header.
        """

        if invocation is None:
            return self._reject("tool_invocation_context_required")
        if not isinstance(engram_id, str) or not engram_id.strip():
            return self._reject("engram_identity_invalid")
        if tool_name not in _TOOL_SET and tool_name not in MUTABLE_PI_TOOL_NAMES and tool_name not in _READ_PROXY_NAMES:
            return self._reject("unknown_tool")
        if self._closed:
            return self._reject("runtime_quiescing")
        if args is not None and not isinstance(args, Mapping):
            return self._reject("tool_args_object_required")
        payload = dict(args or {})
        root = self._current_root(engram_id)
        if root is None:
            return self._reject("running_turn_required")
        tool_call = None
        try:
            handler = None if (
                tool_name in MUTABLE_PI_TOOL_NAMES
                or tool_name in TASK_WORKER_TOOL_NAMES
            ) else getattr(self, f"_{tool_name}", None)
            task_offer_request = (
                self._task_offer_request_binding(payload)
                if tool_name == _TASK_OFFER_RESPONSE_TOOL
                else None
            )
            task_relationship_request = (
                self._task_relationship_request_binding(payload)
                if tool_name == _TASK_RELATIONSHIP_RESPONSE_TOOL
                else None
            )
            portfolio_history_limit = (
                self._portfolio_history_limit(payload)
                if tool_name == "pulse_life_portfolio"
                else None
            )
            with self._dispatch_lock_for(engram_id):
                tool_call = self._tool_call(
                    root,
                    engram_id,
                    tool_name,
                    invocation,
                    portfolio_history_limit=portfolio_history_limit,
                    task_offer_request=task_offer_request,
                    task_relationship_request=task_relationship_request,
                )
                existing = self._find_tool_result(tool_call.id, invocation.tool_call_id)
                if existing is not None:
                    # Portfolio owns no durable snapshot.  A successful retry,
                    # including one after Runtime restart when the in-memory
                    # result cache is empty, must rebuild the same read model
                    # from its canonical durable sources.  The existing
                    # TOOL_CALL/TOOL_RESULT pair remains the only Harness trace.
                    if (
                        tool_name == "pulse_life_portfolio"
                        and existing.metadata.get("ok") is True
                    ):
                        if handler is None:
                            raise ValueError("unknown_tool")
                        content, data, ok, _extra_event_id = handler(
                            engram_id,
                            payload,
                            root,
                            tool_call,
                            invocation,
                        )
                        self._tool_result_data[existing.id] = json.loads(
                            json.dumps(data, ensure_ascii=False)
                        )
                    else:
                        content = existing.content or ""
                        cached_data = self._tool_result_data.get(existing.id)
                        if cached_data is not None:
                            data = json.loads(
                                json.dumps(cached_data, ensure_ascii=False)
                            )
                        else:
                            stored_data = existing.metadata.get("result_refs", {})
                            data = (
                                dict(stored_data)
                                if isinstance(stored_data, dict)
                                else {}
                            )
                        if tool_name == "pulse_life_hold":
                            concern_id = data.get("concern_id")
                            if isinstance(concern_id, str):
                                concern = self._world.get_living_concern(concern_id)
                                if concern is not None:
                                    data = {
                                        "concern": self._concern_view(concern),
                                        "concern_id": concern.id,
                                    }
                        elif tool_name == "pulse_life_orient":
                            orientation_id = data.get("orientation_id")
                            if isinstance(orientation_id, str):
                                orientation = self._world.get_living_orientation(
                                    orientation_id
                                )
                                if orientation is not None:
                                    data = {
                                        "orientation": self._orientation_view(
                                            orientation
                                        ),
                                        "orientation_id": orientation.id,
                                    }
                        elif tool_name == "pulse_life_orientations":
                            data = {
                                "orientations": self._orientation_rows(
                                    engram_id,
                                    payload,
                                )
                            }
                        elif tool_name == "pulse_life_amend_purpose":
                            proposal_id = data.get("proposal_id")
                            if not isinstance(proposal_id, str):
                                proposal_id = data.get("purpose_revision_id")
                            if (
                                isinstance(proposal_id, str)
                                and self._purpose_governance is not None
                            ):
                                proposal = self._purpose_governance.get_proposal(
                                    proposal_id
                                )
                                if proposal is not None:
                                    data = {
                                        "proposal_id": proposal_id,
                                        "purpose_revision_id": proposal_id,
                                        "status": proposal.state.value,
                                        "purpose_proposal": self._purpose_proposal_view(
                                            proposal
                                        ),
                                    }
                        elif tool_name in {
                            "pulse_life_accept_role",
                            "pulse_life_renew_role",
                            "pulse_life_release_role",
                        }:
                            role_id = data.get("role_lease_id")
                            if isinstance(role_id, str) and self._role_store is not None:
                                role = self._role_store.get(role_id)
                                if role is not None:
                                    data = {
                                        "role_lease_id": role_id,
                                        "role": self._role_lease_view(role),
                                    }
                        ok = bool(existing.metadata.get("ok"))
                    result = existing
                else:
                    if tool_name == _TASK_RELATIONSHIP_RESPONSE_TOOL:
                        self._require_task_relationship_response_context(
                            root,
                            engram_id,
                            task_relationship_request,
                        )
                    else:
                        self._require_task_offer_capability(root, tool_name)
                        self._require_task_relationship_capability(
                            root,
                            tool_name,
                        )
                    self._require_active_task_relationship(
                        root,
                        engram_id,
                        tool_name,
                    )
                    error_code = None
                    if tool_name in MUTABLE_PI_TOOL_NAMES:
                        running_turn = self._ledger.get_running_turn(engram_id)
                        if running_turn is None:
                            raise CausalTransitionError("running_turn_required")
                        action_result = self._dispatch_mutable_action(
                            engram_id,
                            tool_name,
                            payload,
                            invocation,
                            running_turn.id,
                        )
                        content = action_result.get("content", "")
                        data = action_result.get("data", {})
                        ok = action_result.get("ok") is True
                        error_code = action_result.get("error")
                        if not isinstance(content, str):
                            content = "The Harness action returned an invalid result."
                        if not isinstance(data, Mapping):
                            data = {}
                        if not isinstance(error_code, str):
                            error_code = None
                        extra_event_id = None
                    elif tool_name in TASK_WORKER_TOOL_NAMES:
                        running_turn = self._ledger.get_running_turn(engram_id)
                        if running_turn is None:
                            raise CausalTransitionError("running_turn_required")
                        task_result = self._dispatch_task_worker(
                            engram_id,
                            tool_name,
                            payload,
                            invocation,
                            running_turn.id,
                        )
                        content = task_result.get("content", "")
                        data = task_result.get("data", {})
                        ok = task_result.get("ok") is True
                        error_code = task_result.get("error")
                        if not isinstance(content, str):
                            content = "The temporary worker returned an invalid result."
                        if not isinstance(data, Mapping):
                            data = {}
                        if not isinstance(error_code, str):
                            error_code = None
                        extra_event_id = task_result.get("event_id")
                        if not isinstance(extra_event_id, str):
                            extra_event_id = None
                    else:
                        if handler is None:
                            raise ValueError("unknown_tool")
                        self._require_life_mutation_context(root, tool_name)
                        content, data, ok, extra_event_id = handler(
                            engram_id,
                            payload,
                            root,
                            tool_call,
                            invocation,
                        )
                    result_parent = extra_event_id or tool_call.id
                    result = self._tool_result(
                        result_parent,
                        engram_id,
                        tool_name,
                        invocation,
                        content,
                        ok=ok,
                        data=data,
                        error_code=error_code,
                    )
                    self._tool_result_data[result.id] = json.loads(
                        json.dumps(data, ensure_ascii=False)
                    )
            self._record_role_contribution(
                engram_id=engram_id,
                tool_name=tool_name,
                root=root,
                result=result,
                data=data,
                ok=ok,
            )
            self._record_metric("pulse_tool_finished", tool=tool_name, ok=ok)
            response = {
                "ok": ok,
                "content": content,
                "data": data,
                "event_id": result.id,
            }
            error_code = result.metadata.get("error_code")
            if not ok and isinstance(error_code, str):
                response["error"] = error_code
            return response
        except (KeyError, ValueError, PermissionError, CausalTransitionError) as exc:
            code = self._exception_code(exc)
            event_id = self._record_failed_tool_result(
                tool_call,
                engram_id,
                tool_name,
                invocation,
                code,
            )
            self._record_metric("pulse_tool_finished", tool=tool_name, ok=False)
            return self._reject(code, event_id=event_id)
        except Exception:
            event_id = self._record_failed_tool_result(
                tool_call,
                engram_id,
                tool_name,
                invocation,
                "tool_failed",
            )
            self._record_metric("pulse_tool_finished", tool=tool_name, ok=False)
            return self._reject("tool_failed", event_id=event_id)

    def authorize(
        self,
        engram_id: str,
        tool_name: str,
        input_data: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Authorize Pi built-ins from the current running root event."""

        root = self._current_root(engram_id)
        if root is None or self._closed:
            return {"allow": False, "reason_code": "running_turn_required"}
        input_data = dict(input_data or {})
        if tool_name == _TASK_RELATIONSHIP_RESPONSE_TOOL:
            try:
                request = self._task_relationship_request_binding(input_data)
                self._require_task_relationship_response_context(
                    root,
                    engram_id,
                    request,
                )
            except (KeyError, ValueError, PermissionError) as exc:
                return self._authorization_decision(
                    root,
                    engram_id,
                    tool_name,
                    False,
                    self._exception_code(exc),
                )
            return self._authorization_decision(
                root,
                engram_id,
                tool_name,
                True,
                "task_relationship_response_allowed",
            )
        if self._is_task_relationship_root(root):
            try:
                self._task_relationship_negotiation_context(root)
            except ValueError:
                allowed = False
                reason_code = "task_relationship_context_required"
            else:
                allowed = tool_name in _TASK_RELATIONSHIP_ALLOWED_TOOLS
                reason_code = (
                    "task_relationship_negotiation_allowed"
                    if allowed
                    else "task_relationship_negotiation_only"
                )
            return self._authorization_decision(
                root,
                engram_id,
                tool_name,
                allowed,
                reason_code,
            )
        if self._is_task_offer_root(root):
            allowed = tool_name in _TASK_OFFER_ALLOWED_TOOLS
            reason_code = (
                "task_offer_deliberation_allowed"
                if allowed
                else "task_offer_deliberation_only"
            )
            if tool_name == _TASK_OFFER_RESPONSE_TOOL:
                try:
                    self._task_offer_context(root)
                except ValueError:
                    allowed = False
                    reason_code = "task_offer_context_required"
            return self._authorization_decision(
                root,
                engram_id,
                tool_name,
                allowed,
                reason_code,
            )
        try:
            self._require_active_task_relationship(root, engram_id, tool_name)
        except (KeyError, ValueError, PermissionError) as exc:
            return self._authorization_decision(
                root,
                engram_id,
                tool_name,
                False,
                self._exception_code(exc),
            )
        if tool_name == _TASK_OFFER_RESPONSE_TOOL:
            return self._authorization_decision(
                root,
                engram_id,
                tool_name,
                False,
                "task_offer_context_required",
            )
        if tool_name in MUTABLE_PI_TOOL_NAMES:
            # These names are deliberately unavailable as Pi native tools.
            # Their same-name Pulse proxy is the only path and reaches the
            # policy/approval/action seam through the bearer Gateway.
            return self._authorization_decision(
                root,
                engram_id,
                tool_name,
                False,
                "mutable_native_tool_disabled",
            )
        if tool_name in _READ_PROXY_NAMES:
            if not self._safe_workspace_input(tool_name, input_data):
                return self._authorization_decision(
                    root,
                    engram_id,
                    tool_name,
                    False,
                    "workspace_path_unproven",
                )
            return self._authorization_decision(
                root, engram_id, tool_name, True, "workspace_read_allowed"
            )
        if tool_name in _TOOL_SET:
            return self._authorization_decision(
                root, engram_id, tool_name, True, "pulse_tool_allowed"
            )

        source = root.source.value
        unattended = source != "user"
        if unattended and tool_name in _UNATTENDED_DENY:
            return self._authorization_decision(
                root,
                engram_id,
                tool_name,
                False,
                "unattended_host_write_denied",
            )
        if tool_name in _READ_ONLY:
            if not self._safe_workspace_input(tool_name, input_data):
                return self._authorization_decision(
                    root,
                    engram_id,
                    tool_name,
                    False,
                    "workspace_path_unproven",
                )
            return self._authorization_decision(
                root, engram_id, tool_name, True, "workspace_read_allowed"
            )
        if unattended:
            return self._authorization_decision(
                root, engram_id, tool_name, False, "unattended_tool_denied"
            )
        if self._contains_private_material(input_data):
            return self._authorization_decision(
                root,
                engram_id,
                tool_name,
                False,
                "runtime_private_root_denied",
            )
        return self._authorization_decision(
            root, engram_id, tool_name, True, "user_harness_allowed"
        )

    def _dispatch_mutable_action(
        self,
        engram_id: str,
        tool_name: str,
        payload: Mapping[str, Any],
        invocation: ToolInvocationContext,
        turn_id: str,
    ) -> dict[str, Any]:
        """Send mutable Pi proxies through the single L3 action seam."""

        gateway = self._action_gateway
        if gateway is None:
            return {
                "ok": False,
                "content": "The Pulse action gateway is unavailable; no native fallback was used.",
                "data": {
                    "action_request_id": invocation.tool_call_id,
                    "state": "rejected",
                    "execution_status": "not_started",
                },
                "error": "action_gateway_unavailable",
            }
        try:
            result = gateway(
                engram_id,
                tool_name,
                dict(payload),
                invocation,
                turn_id,
            )
        except Exception:
            return {
                "ok": False,
                "content": "The Pulse action gateway rejected this request; no native fallback was used.",
                "data": {
                    "action_request_id": invocation.tool_call_id,
                    "state": "rejected",
                    "execution_status": "not_started",
                },
                "error": "action_gateway_unavailable",
            }
        if not isinstance(result, Mapping) or type(result.get("ok")) is not bool:
            return {
                "ok": False,
                "content": "The Pulse action gateway returned no valid decision.",
                "data": {
                    "action_request_id": invocation.tool_call_id,
                    "state": "rejected",
                    "execution_status": "not_started",
                },
                "error": "action_gateway_contract_invalid",
            }
        if (
            result.get("error") == "approval_required"
            and self._action_waiter is not None
        ):
            try:
                continued = self._action_waiter(
                    engram_id,
                    invocation.tool_call_id,
                )
            except Exception:
                return {
                    "ok": False,
                    "content": "The approval continuation failed closed; no native fallback was used.",
                    "data": {
                        "action_request_id": invocation.tool_call_id,
                        "state": "uncertain",
                        "execution_status": "uncertain",
                    },
                    "error": "action_continuation_unavailable",
                }
            if isinstance(continued, Mapping) and type(continued.get("ok")) is bool:
                return dict(continued)
        return dict(result)

    def _dispatch_task_worker(
        self,
        engram_id: str,
        tool_name: str,
        payload: Mapping[str, Any],
        invocation: ToolInvocationContext,
        turn_id: str,
    ) -> dict[str, Any]:
        """Dispatch a temporary worker operation within the parent turn."""

        gateway = self._task_worker_gateway
        if gateway is None:
            return {
                "ok": False,
                "content": "The temporary task-worker adapter is not enabled.",
                "data": {
                    "action_request_id": invocation.tool_call_id,
                    "state": "failed",
                    "execution_status": "not_started",
                },
                "event_id": None,
                "error": "task_worker_unavailable",
            }
        try:
            result = gateway(
                engram_id,
                tool_name,
                dict(payload),
                invocation,
                turn_id,
            )
        except Exception:
            return {
                "ok": False,
                "content": "The temporary task-worker adapter failed closed.",
                "data": {
                    "action_request_id": invocation.tool_call_id,
                    "state": "failed",
                    "execution_status": "not_started",
                },
                "event_id": None,
                "error": "task_worker_failed",
            }
        if not isinstance(result, Mapping):
            return {
                "ok": False,
                "content": "The temporary task-worker adapter returned an invalid result.",
                "data": {},
                "event_id": None,
                "error": "task_worker_contract_invalid",
            }
        return dict(result)

    def close(self, timeout: float | None = None) -> dict[str, Any]:
        """Freeze dispatch and bound ownership proof for delegate workers.

        CPython cannot safely kill an arbitrary worker thread.  The executor
        join therefore belongs to a daemon finalizer while this method waits
        only for its caller-supplied local budget and reports unresolved work
        explicitly.
        """

        if timeout is None:
            timeout_seconds = 30.0
        elif (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(float(timeout))
            or float(timeout) < 0.0
            or float(timeout) > 300.0
        ):
            raise ValueError("timeout must be a finite number in [0, 300]")
        else:
            timeout_seconds = float(timeout)
        with self._jobs_lock:
            if self._closed:
                first_caller = False
                executor = None
                futures = ()
            else:
                first_caller = True
                self._closed = True
                executor = self._executor
                futures = tuple(self._jobs.values())

        if not first_caller:
            self._close_done.wait(timeout=timeout_seconds)
            with self._jobs_lock:
                if self._close_summary is not None:
                    return dict(self._close_summary)
                return {
                    "active_before": len(self._jobs),
                    "cancelled": 0,
                    "unresolved": max(1, len(self._jobs)),
                    "owner_joined": False,
                }

        assert executor is not None

        cancelled = sum(future.cancel() for future in futures)
        executor.shutdown(wait=False, cancel_futures=True)
        joined = threading.Event()

        def finalize() -> None:
            try:
                executor.shutdown(wait=True, cancel_futures=True)
            finally:
                joined.set()
                self._owner_drained.set()

        threading.Thread(
            target=finalize,
            name="pulse-life-delegate-finalizer",
            daemon=True,
        ).start()
        joined.wait(timeout=timeout_seconds)
        unresolved = sum(not future.done() for future in futures)
        if not joined.is_set():
            unresolved = max(1, unresolved)
        with self._dispatch_locks_guard:
            self._dispatch_locks.clear()
        summary = {
            "active_before": len(futures),
            "cancelled": cancelled,
            "unresolved": unresolved,
            "owner_joined": joined.is_set(),
        }
        with self._jobs_lock:
            self._close_summary = dict(summary)
            self._close_done.set()
        return summary

    def wait_for_shutdown_drain(self, timeout: float | None = None) -> bool:
        """Wait for the delegate executor owner without granting publication.

        Runtime calls this only from a daemon shutdown adapter/finalizer.  A
        late worker can finish, but its RuntimeFence still prevents it from
        publishing after the generation has been revoked.
        """

        if timeout is not None and (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(float(timeout))
            or float(timeout) < 0.0
        ):
            raise ValueError("timeout must be a finite non-negative number or null")
        return self._owner_drained.wait(
            timeout=None if timeout is None else float(timeout)
        )

    def _dispatch_lock_for(self, engram_id: str) -> threading.RLock:
        """Return the exactly-once lock for one Engram only.

        The lock deliberately spans the durable check, adapter/approval wait,
        and durable result commit for one subject.  That is the smallest
        scope that prevents two concurrent retries from both creating a
        causal tool call while allowing every other Engram to continue.
        """

        key = engram_id.strip()
        with self._dispatch_locks_guard:
            lock = self._dispatch_locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self._dispatch_locks[key] = lock
            return lock

    # ── Tool implementations ───────────────────────────────────

    def _pulse_task_offer_respond(
        self,
        engram_id,
        args,
        root,
        tool_call,
        invocation,
    ):
        """Commit the current subject's decision for the current offer root.

        ``offer_id`` and subject identity never cross the model-controlled
        argument boundary.  The durable service rechecks the root-derived
        scope and owns the decision CAS plus any accepted task bundle.
        """

        del invocation
        request = self._task_offer_request_binding(args)
        decision = request["decision"]
        expected_revision = request["expected_revision"]
        response = request["response"]

        offer_id, root_revision = self._task_offer_context(root)
        if expected_revision != root_revision:
            raise ValueError("task_offer_revision_conflict")
        service = self._task_offer_service
        if service is None:
            raise ValueError("task_offer_service_unavailable")
        try:
            raw = service.respond(
                offer_id=offer_id,
                expected_revision=expected_revision,
                subject_engram_id=engram_id,
                decision=decision,
                response=response,
                decision_event_id=tool_call.id,
            )
        except Exception as exc:
            code = getattr(exc, "code", None)
            if isinstance(code, str) and code in _TASK_OFFER_PUBLIC_ERRORS:
                raise ValueError(code) from exc
            if isinstance(code, str) and code in _TASK_OFFER_SERVICE_ERROR_MAP:
                raise ValueError(_TASK_OFFER_SERVICE_ERROR_MAP[code]) from exc
            raise
        snapshot = self._task_offer_result_member(raw, "snapshot", None)
        offer = self._task_offer_result_member(snapshot, "offer", None)
        if offer is None:
            offer = self._task_offer_result_member(raw, "task_offer", None)
        if offer is None:
            offer = self._task_offer_result_member(raw, "offer", None)
        returned_offer_id = self._task_offer_result_member(
            offer,
            "id",
            self._task_offer_result_member(raw, "task_offer_id", None),
        )
        returned_revision = self._task_offer_result_member(
            offer,
            "current_revision",
            self._task_offer_result_member(raw, "task_offer_revision", None),
        )
        returned_status = self._task_offer_result_member(
            offer,
            "status",
            self._task_offer_result_member(raw, "status", None),
        )
        if hasattr(returned_status, "value"):
            returned_status = returned_status.value
        expected_status = _TASK_OFFER_DECISION_STATUS[decision]
        if (
            returned_offer_id != offer_id
            or returned_revision != expected_revision
            or returned_status != expected_status
        ):
            raise ValueError("task_offer_service_unavailable")

        task_front_id = self._task_offer_result_member(
            raw, "task_front_id", None
        )
        if task_front_id is None:
            task_front_id = self._task_offer_result_member(
                offer, "task_front_id", None
            )
        task_event_id = self._task_offer_result_member(
            raw,
            "task_event_id",
            self._task_offer_result_member(raw, "event_id", None),
        )
        if decision == "accept":
            if (
                not isinstance(task_front_id, str)
                or not task_front_id
                or not isinstance(task_event_id, str)
                or not task_event_id
            ):
                raise ValueError("task_offer_service_unavailable")
        elif task_front_id is not None or task_event_id is not None:
            raise ValueError("task_offer_service_unavailable")

        data: dict[str, Any] = {
            "task_offer_id": offer_id,
            "task_offer_revision": expected_revision,
            "decision": decision,
            "status": expected_status,
            "decision_event_id": tool_call.id,
        }
        if decision == "accept":
            data["task_front_id"] = task_front_id
            data["task_event_id"] = task_event_id
        content = {
            "accept": (
                "The current task offer was accepted. Task work is queued "
                "for a separate task turn."
            ),
            "refuse": (
                "The current task offer was refused. No task was created."
            ),
            "request_changes": (
                "Changes were requested for the current task offer. No task "
                "was created."
            ),
        }[decision]
        return content, data, True, None

    @staticmethod
    def _task_offer_request_binding(args: Mapping[str, Any]) -> dict[str, Any]:
        if set(args).difference({"decision", "expected_revision", "response"}):
            raise ValueError("tool_schema_unknown_field")
        if not {"decision", "expected_revision"}.issubset(args):
            raise ValueError("tool_schema_required_field")
        decision = args.get("decision")
        expected_revision = args.get("expected_revision")
        response = args.get("response")
        if decision not in _TASK_OFFER_DECISION_STATUS:
            raise ValueError("tool_schema_decision_invalid")
        if type(expected_revision) is not int or expected_revision < 1:
            raise ValueError("tool_schema_expected_revision_invalid")
        if response is not None and (
            not isinstance(response, str) or len(response) > 4000
        ):
            raise ValueError("tool_schema_response_invalid")
        if decision == "request_changes" and (
            not isinstance(response, str) or not response.strip()
        ):
            raise ValueError("task_offer_response_required")
        if (
            decision != "request_changes"
            and isinstance(response, str)
            and not response.strip()
        ):
            response = None
        response_binding = "none" if response is None else "text:" + response
        return {
            "decision": decision,
            "expected_revision": expected_revision,
            "response": response,
            "response_digest": hashlib.sha256(
                response_binding.encode("utf-8")
            ).hexdigest(),
        }

    def _pulse_task_relationship_respond(
        self,
        engram_id,
        args,
        root,
        tool_call,
        invocation,
    ):
        """Commit one subject-owned task relationship transition.

        The model may select only a relationship ID, expected revision,
        action and bounded note.  Capability identity and causal provenance
        come from the running Pi process and its durable TOOL_CALL.
        """

        del invocation
        request = self._task_relationship_request_binding(args)
        self._require_task_relationship_response_context(
            root,
            engram_id,
            request,
        )
        relationship_id = request["relationship_id"]
        expected_revision = request["expected_revision"]
        action = request["action"]
        response = request["response"]

        service = self._task_relationship_service
        if service is None:
            raise ValueError("task_relationship_service_unavailable")
        try:
            raw = service.respond(
                relationship_id=relationship_id,
                expected_revision=expected_revision,
                subject_engram_id=engram_id,
                action=action,
                response=response,
                source_event_id=tool_call.id,
            )
        except Exception as exc:
            code = getattr(exc, "code", None)
            if isinstance(code, str) and code in _TASK_RELATIONSHIP_PUBLIC_ERRORS:
                raise ValueError(code) from exc
            if (
                isinstance(code, str)
                and code in _TASK_RELATIONSHIP_SERVICE_ERROR_MAP
            ):
                raise ValueError(
                    _TASK_RELATIONSHIP_SERVICE_ERROR_MAP[code]
                ) from exc
            raise

        snapshot = self._task_offer_result_member(raw, "snapshot", None)
        relationship = self._task_offer_result_member(
            snapshot,
            "relationship",
            None,
        )
        if relationship is None:
            relationship = self._task_offer_result_member(
                raw,
                "task_relationship",
                None,
            )
        if relationship is None:
            relationship = self._task_offer_result_member(
                raw,
                "relationship",
                None,
            )

        returned_id = self._task_offer_result_member(
            relationship,
            "id",
            self._task_offer_result_member(raw, "task_relationship_id", None),
        )
        returned_revision = self._task_offer_result_member(
            relationship,
            "revision",
            self._task_offer_result_member(
                raw,
                "task_relationship_revision",
                None,
            ),
        )
        returned_status = self._task_offer_result_member(
            relationship,
            "status",
            self._task_offer_result_member(raw, "status", None),
        )
        if hasattr(returned_status, "value"):
            returned_status = returned_status.value
        returned_subject = self._task_offer_result_member(
            relationship,
            "current_subject_engram_id",
            None,
        )
        returned_center = self._task_offer_result_member(
            relationship,
            "center_id",
            None,
        )
        effect_revision = self._task_offer_result_member(
            raw,
            "effect_revision",
            returned_revision,
        )
        effect_status = self._task_offer_result_member(
            raw,
            "effect_status",
            returned_status,
        )
        if hasattr(effect_status, "value"):
            effect_status = effect_status.value
        effect_subject = self._task_offer_result_member(
            raw,
            "effect_subject_engram_id",
            returned_subject,
        )
        effect_center = self._task_offer_result_member(
            raw,
            "effect_center_id",
            returned_center,
        )
        expected_status = _TASK_RELATIONSHIP_ACTION_STATUS[action]
        if (
            returned_id != relationship_id
            or effect_revision != expected_revision + 1
            or effect_status != expected_status
            or effect_subject != engram_id
            or not isinstance(effect_center, str)
            or not effect_center
        ):
            raise ValueError("task_relationship_service_unavailable")
        if root.center_id is not None:
            center = self._world.get_activity_center(root.center_id)
            if (
                center is not None
                and center.kind is ActivityKind.TASK
                and effect_center != root.center_id
            ):
                raise ValueError("task_relationship_service_unavailable")

        data = {
            "task_relationship_id": relationship_id,
            "task_relationship_revision": effect_revision,
            "action": action,
            "status": expected_status,
            "source_event_id": tool_call.id,
        }
        if action != "resume" and self._task_relationship_revoker is not None:
            try:
                revocation = self._task_relationship_revoker(
                    engram_id=engram_id,
                    relationship_id=relationship_id,
                    relationship_revision=effect_revision,
                    action=action,
                    source_event_id=tool_call.id,
                )
                if not isinstance(revocation, Mapping):
                    raise TypeError("task relationship revoker returned no evidence")
                data["execution_revocation"] = json.loads(
                    json.dumps(dict(revocation), ensure_ascii=False)
                )
            except Exception:
                # The subject decision is already durable.  Preserve it while
                # truthfully marking teardown uncertainty; the non-active
                # relationship still fences every later tool admission.
                data["execution_revocation"] = {
                    "state": "uncertain",
                    "uncertain": True,
                    "error_code": "task_relationship_revocation_unavailable",
                }
        content = {
            "pause": "Participation in this task relationship is now paused.",
            "request_changes": (
                "Changed terms were requested. Task participation remains paused."
            ),
            "resume": "The subject voluntarily resumed this task relationship.",
            "exit": "The subject exited this task relationship.",
        }[action]
        return content, data, True, None

    @staticmethod
    def _task_relationship_request_binding(
        args: Mapping[str, Any],
    ) -> dict[str, Any]:
        allowed = {"relationship_id", "expected_revision", "action", "response"}
        if set(args).difference(allowed):
            raise ValueError("tool_schema_unknown_field")
        if not {"relationship_id", "expected_revision", "action"}.issubset(args):
            raise ValueError("tool_schema_required_field")
        relationship_id = args.get("relationship_id")
        expected_revision = args.get("expected_revision")
        action = args.get("action")
        response = args.get("response")
        if (
            not isinstance(relationship_id, str)
            or not relationship_id.strip()
            or relationship_id.strip() != relationship_id
            or len(relationship_id) > 128
            or "\x00" in relationship_id
        ):
            raise ValueError("tool_schema_relationship_id_invalid")
        if type(expected_revision) is not int or expected_revision < 1:
            raise ValueError("tool_schema_expected_revision_invalid")
        if action not in _TASK_RELATIONSHIP_ACTION_STATUS:
            raise ValueError("tool_schema_action_invalid")
        if response is not None and (
            not isinstance(response, str) or len(response) > 4000
        ):
            raise ValueError("tool_schema_response_invalid")
        if action == "request_changes" and (
            not isinstance(response, str) or not response.strip()
        ):
            raise ValueError("task_relationship_response_required")
        response_binding = "none" if response is None else "text:" + response
        return {
            "relationship_id": relationship_id,
            "expected_revision": expected_revision,
            "action": action,
            "response": response,
            "response_digest": hashlib.sha256(
                response_binding.encode("utf-8")
            ).hexdigest(),
        }

    @staticmethod
    def _task_offer_result_member(
        value: Any,
        name: str,
        fallback: Any,
    ) -> Any:
        if isinstance(value, Mapping):
            return value.get(name, fallback)
        return getattr(value, name, fallback)

    def _purpose_context(self, engram_id: str) -> tuple[PurposeGovernance, str]:
        governance = self._purpose_governance
        resolver = self._lineage_resolver
        if governance is None or resolver is None:
            raise ValueError("purpose_governance_unavailable")
        lineage_id = resolver(engram_id)
        lineage = governance.require_lineage(lineage_id)
        if lineage.current_engram_id != engram_id:
            raise ValueError("purpose_lineage_holder_mismatch")
        return governance, lineage_id

    def on_harness_turn_terminal(
        self,
        harness_turn_id: str,
    ) -> PurposeAmendmentProposal | None:
        """Resolve a staged purpose proposal without changing turn truth.

        The Harness turn is already terminal when this hook runs.  A purpose
        recovery fault therefore cannot make that turn retryable or rewrite
        its causal status; the proposal remains pending for startup recovery
        and the degraded state is surfaced separately.
        """

        governance = self._purpose_governance
        if governance is None:
            return None
        try:
            proposal = governance.resolve_turn_proposal(harness_turn_id)
        except Exception as exc:  # noqa: BLE001 - preserve settled turn truth
            self._purpose_settlement_health = "degraded"
            self._purpose_settlement_last_error = type(exc).__name__
            self._record_metric(
                "purpose_amendment_settlement_failed",
                turn=harness_turn_id,
                error_type=type(exc).__name__,
            )
            return None
        if proposal is None:
            return None
        self._purpose_settlement_health = "healthy"
        self._purpose_settlement_last_error = None
        self._record_metric(
            "purpose_amendment_settled",
            turn=harness_turn_id,
            proposal=proposal.proposal_id,
            state=proposal.state.value,
        )
        return proposal

    def purpose_settlement_status(self) -> dict[str, Any]:
        return {
            "health": self._purpose_settlement_health,
            "last_error_type": self._purpose_settlement_last_error,
        }

    def _pulse_life_purpose(self, engram_id, args, root, tool_call, invocation):
        del root, tool_call, invocation
        governance, lineage_id = self._purpose_context(engram_id)
        history = args.get("history", False)
        limit = args.get("limit", 100)
        if type(history) is not bool or type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("purpose_query_invalid")
        lineage = governance.require_lineage(lineage_id)
        current = governance.current_revision(lineage_id)
        revisions = governance.list_revisions(lineage_id, limit=limit) if history else []
        proposals = governance.list_proposals(
            lineage_id,
            limit=limit,
            state=None if history else PurposeAmendmentProposalState.PENDING,
        )
        data = {
            "lineage": {
                "lineage_id": lineage.lineage_id,
                "root_engram_id": lineage.root_engram_id,
                "current_engram_id": lineage.current_engram_id,
                "generation": lineage.generation,
            },
            "current": None if current is None else self._purpose_revision_view(current),
            "history": [self._purpose_revision_view(item) for item in revisions],
            "amendment_attempts": [
                self._purpose_proposal_view(item) for item in proposals
            ],
            "evidence_class": "LIVE_GATE_UNVERIFIED",
        }
        base_content = (
            "No current subject purpose has been adopted."
            if current is None
            else f"Current subject purpose (revision {current.revision}): {current.content}"
        )
        pending_count = sum(
            item.state is PurposeAmendmentProposalState.PENDING
            for item in proposals
        )
        content = (
            base_content
            if pending_count == 0
            else f"{base_content} {pending_count} amendment proposal(s) await turn settlement."
        )
        return content, data, True, None

    def _pulse_life_amend_purpose(
        self,
        engram_id,
        args,
        root,
        tool_call,
        invocation,
    ):
        governance, lineage_id = self._purpose_context(engram_id)
        kind = args.get("amendment_kind")
        expected_revision = args.get("expected_revision")
        content = args.get("content")
        if kind not in {item.value for item in PurposeAmendmentKind}:
            raise ValueError("purpose_amendment_kind_invalid")
        if expected_revision is not None and (
            type(expected_revision) is not int or expected_revision < 1
        ):
            raise ValueError("purpose_expected_revision_invalid")
        if kind == PurposeAmendmentKind.WITHDRAW.value:
            if "content" in args:
                raise ValueError("purpose_withdraw_content_forbidden")
            content = None
        elif not isinstance(content, str) or not content.strip():
            raise ValueError("purpose_content_required")
        revision_id = "purpose_" + hashlib.sha256(
            tool_call.id.encode("utf-8")
        ).hexdigest()[:32]
        turn = self._ledger.get_running_turn(engram_id)
        if turn is None or turn.event_id != root.id:
            raise ValueError("purpose_subject_reflection_required")
        try:
            proposal = governance.stage_amendment(
                lineage_id,
                proposal_id=revision_id,
                author_engram_id=engram_id,
                harness_turn_id=turn.id,
                tool_call_event_id=tool_call.id,
                tool_call_id=invocation.tool_call_id,
                expected_revision=expected_revision,
                content=content,
                amendment_kind=kind,
                source_event_id=root.id,
            )
        except PurposeRevisionConflictError as exc:
            raise ValueError("purpose_revision_conflict") from exc
        except PurposeProposalConflictError as exc:
            raise ValueError("purpose_amendment_already_staged") from exc
        except PurposeReflectionRequiredError as exc:
            raise ValueError("purpose_subject_reflection_required") from exc
        except PurposeGovernanceError as exc:
            raise ValueError("purpose_amendment_rejected") from exc
        data = {
            "lineage_id": lineage_id,
            "proposal_id": proposal.proposal_id,
            "purpose_revision_id": proposal.proposal_id,
            "status": proposal.state.value,
            "purpose_proposal": self._purpose_proposal_view(proposal),
            "evidence_class": "LIVE_GATE_UNVERIFIED",
        }
        return (
            "The subject-authored purpose amendment is durably staged. It will "
            "become current only if this same Harness turn settles successfully.",
            data,
            True,
            None,
        )

    def _role_context(self, engram_id: str) -> tuple[RoleLeaseStore, RuntimeLeaseProof, str]:
        store = self._role_store
        provider = self._runtime_lease_provider
        _governance, lineage_id = self._purpose_context(engram_id)
        if store is None or provider is None:
            raise ValueError("role_lease_store_unavailable")
        proof = provider()
        if not isinstance(proof, RuntimeLeaseProof) or proof.world_id != self._world_id:
            raise ValueError("runtime_role_proof_unavailable")
        return store, proof, lineage_id

    def _pulse_life_roles(self, engram_id, args, root, tool_call, invocation):
        del root, tool_call, invocation
        store, _proof, lineage_id = self._role_context(engram_id)
        active_only = args.get("active_only", False)
        if type(active_only) is not bool:
            raise ValueError("role_query_invalid")
        status = RoleLeaseStatus.ACTIVE if active_only else None
        roles = store.list(
            world_id=self._world_id,
            lineage_id=lineage_id,
            status=status,
            limit=100,
        )
        data = {
            "lineage_id": lineage_id,
            "roles": [self._role_lease_view(role) for role in roles],
            "evidence_class": "LIVE_GATE_UNVERIFIED",
        }
        return f"Observed {len(roles)} bounded subject role lease(s).", data, True, None

    def _pulse_life_accept_role(self, engram_id, args, root, tool_call, invocation):
        del root, invocation
        if set(args).difference(
            {
                "role_label",
                "center_ids",
                "ttl_seconds",
                "purpose_revision_id",
                "obligation",
            }
        ):
            raise ValueError("tool_schema_unknown_field")
        store, proof, lineage_id = self._role_context(engram_id)
        role_label = args.get("role_label")
        center_ids = args.get("center_ids")
        ttl_seconds = args.get("ttl_seconds")
        purpose_revision_id = args.get("purpose_revision_id")
        obligation = None
        if "obligation" in args:
            raw_obligation = args.get("obligation")
            if not isinstance(raw_obligation, Mapping):
                raise ValueError("role_obligation_invalid")
            try:
                obligation = RoleObligation.from_dict(dict(raw_obligation))
            except RoleLeaseError as exc:
                raise ValueError("role_obligation_invalid") from exc
        if not isinstance(role_label, str) or not role_label.strip():
            raise ValueError("role_label_invalid")
        if (
            not isinstance(center_ids, list)
            or not 1 <= len(center_ids) <= 16
            or any(not isinstance(item, str) or not item.strip() for item in center_ids)
        ):
            raise ValueError("role_center_scope_invalid")
        centers = tuple(sorted(set(center_ids)))
        if len(centers) != len(center_ids):
            raise ValueError("role_center_scope_invalid")
        for center_id in centers:
            self._require_non_task_membership(engram_id, center_id)
        if ttl_seconds is not None and (
            isinstance(ttl_seconds, bool)
            or not isinstance(ttl_seconds, (int, float))
            or not math.isfinite(float(ttl_seconds))
            or not 0 < float(ttl_seconds) <= 90 * 24 * 60 * 60
        ):
            raise ValueError("role_ttl_invalid")
        if purpose_revision_id is not None:
            if not isinstance(purpose_revision_id, str) or not purpose_revision_id.strip():
                raise ValueError("purpose_revision_reference_invalid")
            current = self._purpose_governance.current_revision(lineage_id)
            if current is None or current.purpose_revision_id != purpose_revision_id:
                raise ValueError("purpose_revision_reference_stale")
        scope = RoleScope(center_ids=centers, lineage_id=lineage_id)
        role_id = "role_" + hashlib.sha256(tool_call.id.encode("utf-8")).hexdigest()[:32]
        try:
            existing = store.get(role_id)
            if existing is not None:
                if not (
                    existing.world_id == self._world_id
                    and existing.lineage_id == lineage_id
                    and existing.holder_kind is HolderKind.ENGRAM
                    and existing.holder_id == engram_id
                    and existing.role_class is RoleClass.SUBJECT_ROLE
                    and existing.role_label == role_label.strip()
                    and existing.scope.matches(scope)
                    and existing.obligation == obligation
                    and existing.purpose_revision_id == purpose_revision_id
                    and existing.status is RoleLeaseStatus.ACTIVE
                ):
                    raise ValueError("role_lease_id_collision")
                lease = existing
            else:
                lease = store.grant_new(
                    world_id=self._world_id,
                    lineage_id=lineage_id,
                    holder_kind=HolderKind.ENGRAM,
                    holder_id=engram_id,
                    role_class=RoleClass.SUBJECT_ROLE,
                    role_label=role_label,
                    scope=scope,
                    issuer_kind="runtime",
                    issuer_id=proof.owner_id,
                    runtime=proof,
                    obligation=obligation,
                    purpose_revision_id=purpose_revision_id,
                    ttl_seconds=ttl_seconds,
                    role_lease_id=role_id,
                )
        except RoleLeaseError as exc:
            raise ValueError("role_lease_grant_rejected") from exc
        data = {
            "role": self._role_lease_view(lease),
            "role_lease_id": lease.role_lease_id,
            "role_epoch": lease.role_epoch,
        }
        return "The bounded subject role was explicitly accepted.", data, True, None

    def _pulse_life_renew_role(self, engram_id, args, root, tool_call, invocation):
        del root, tool_call, invocation
        if set(args).difference(
            {"role_lease_id", "expected_role_epoch", "ttl_seconds"}
        ):
            raise ValueError("tool_schema_unknown_field")
        store, proof, lineage_id = self._role_context(engram_id)
        role_id = args.get("role_lease_id")
        expected_epoch = args.get("expected_role_epoch")
        ttl_seconds = args.get("ttl_seconds")
        if not isinstance(role_id, str) or not role_id.strip():
            raise ValueError("role_lease_id_invalid")
        if type(expected_epoch) is not int or expected_epoch < 1:
            raise ValueError("role_epoch_invalid")
        if ttl_seconds is not None and (
            isinstance(ttl_seconds, bool)
            or not isinstance(ttl_seconds, (int, float))
            or not math.isfinite(float(ttl_seconds))
            or not 0 < float(ttl_seconds) <= 90 * 24 * 60 * 60
        ):
            raise ValueError("role_ttl_invalid")
        try:
            current = store.get(role_id)
            if (
                current is None
                or current.role_class is not RoleClass.SUBJECT_ROLE
                or current.holder_kind is not HolderKind.ENGRAM
                or current.holder_id != engram_id
                or current.lineage_id != lineage_id
            ):
                raise ValueError("role_holder_mismatch")
            if current.obligation is None:
                raise ValueError("role_direct_output_obligation_required")
            contribution = store.contribution_summary(role_id)
            if (
                not contribution.renewal_eligible
                or contribution.last_direct_output_event_id is None
            ):
                raise ValueError(contribution.reason_code)
            renewed = store.renew(
                role_id,
                expected_role_epoch=expected_epoch,
                runtime=proof,
                evidence_event_id=contribution.last_direct_output_event_id,
                evidence_class=RoleRenewalEvidence.LIVE_EXTERNAL_RESULT,
                ttl_seconds=ttl_seconds,
            )
        except RoleLeaseError as exc:
            raise ValueError("role_renewal_rejected") from exc
        data = {
            "role": self._role_lease_view(renewed),
            "role_lease_id": renewed.role_lease_id,
            "role_epoch": renewed.role_epoch,
            "predecessor_lease_id": role_id,
        }
        return "The direct-output role was renewed from its production receipt.", data, True, None

    def _pulse_life_release_role(self, engram_id, args, root, tool_call, invocation):
        del root, tool_call, invocation
        store, proof, lineage_id = self._role_context(engram_id)
        role_id = args.get("role_lease_id")
        expected_epoch = args.get("expected_role_epoch")
        if not isinstance(role_id, str) or not role_id.strip():
            raise ValueError("role_lease_id_invalid")
        if type(expected_epoch) is not int or expected_epoch < 1:
            raise ValueError("role_epoch_invalid")
        try:
            current = store.get(role_id)
            if (
                current is None
                or current.role_class is not RoleClass.SUBJECT_ROLE
                or current.holder_kind is not HolderKind.ENGRAM
                or current.holder_id != engram_id
                or current.lineage_id != lineage_id
            ):
                raise ValueError("role_holder_mismatch")
            released = store.release(
                role_id,
                expected_role_epoch=expected_epoch,
                runtime=proof,
            )
        except RoleLeaseError as exc:
            raise ValueError("role_release_rejected") from exc
        data = {"role": self._role_lease_view(released), "role_lease_id": role_id}
        return "The subject role lease was released.", data, True, None

    def _read(self, engram_id, args, root, tool_call, invocation):
        del engram_id, root, tool_call, invocation
        if set(args).difference({"path", "offset", "limit"}):
            raise ValueError("tool_schema_unknown_field")
        path = args.get("path")
        if not isinstance(path, str) or not path.strip():
            raise ValueError("tool_schema_path_invalid")
        if not self._safe_workspace_input("read", args):
            raise PermissionError("workspace_path_unproven")
        offset = args.get("offset", 1)
        limit = args.get("limit", 2_000)
        if (
            isinstance(offset, bool)
            or not isinstance(offset, int)
            or offset < 1
            or offset > 10_000_000
        ):
            raise ValueError("tool_schema_offset_invalid")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit < 1
            or limit > 10_000
        ):
            raise ValueError("tool_schema_limit_invalid")
        absolute = (self._workspace / path).resolve() if not Path(path).is_absolute() else Path(path).resolve()
        if not absolute.is_file():
            raise ValueError("read_file_not_found")
        raw = absolute.read_bytes()
        truncated_bytes = len(raw) > 1_048_576
        text = raw[:1_048_576].decode("utf-8", errors="replace")
        lines = text.splitlines()
        start = offset - 1
        selected = lines[start : start + limit]
        truncated = truncated_bytes or start + limit < len(lines)
        content = "\n".join(selected)
        return (
            content,
            {
                "path": absolute.relative_to(self._workspace).as_posix(),
                "offset": offset,
                "limit": limit,
                "truncated": truncated,
            },
            True,
            None,
        )

    def _pulse_life_list(self, engram_id, args, root, tool_call, invocation):
        del args, root, tool_call, invocation
        rows = []
        for membership in self._world.list_memberships(engram_id=engram_id):
            center = self._world.get_activity_center(membership.center_id)
            if center is None:
                continue
            rows.append(self._center_view(center, membership.relation.value))
        return (json.dumps(rows, ensure_ascii=False), {"centers": rows}, True, None)

    def _pulse_life_portfolio(
        self,
        engram_id,
        args,
        root,
        tool_call,
        invocation,
    ):
        del root, tool_call, invocation
        history_limit = self._portfolio_history_limit(args)
        governance = self._purpose_governance
        if governance is None:
            raise ValueError("living_portfolio_unavailable")
        try:
            portfolio = LivingPortfolioProjector(
                self._world,
                governance,
                self._world_id,
            ).project(engram_id, history_limit=history_limit)
        except LivingPortfolioValidationError as exc:
            raise ValueError("tool_schema_history_limit_invalid") from exc
        except LivingPortfolioRecoveryError as exc:
            raise ValueError("living_portfolio_unavailable") from exc
        return (
            f"Living portfolio contains {portfolio['item_count']} life centers.",
            {"portfolio": portfolio},
            True,
            None,
        )

    @staticmethod
    def _portfolio_history_limit(args: Mapping[str, Any]) -> int:
        """Return the one safe request value bound to Portfolio tool identity."""

        if set(args).difference({"history_limit"}):
            raise ValueError("tool_schema_unknown_field")
        history_limit = args.get("history_limit", 20)
        if (
            type(history_limit) is not int
            or not 1 <= history_limit <= 100
        ):
            raise ValueError("tool_schema_history_limit_invalid")
        return history_limit

    def _pulse_life_concerns(self, engram_id, args, root, tool_call, invocation):
        del root, tool_call, invocation
        if set(args).difference({"center_id"}):
            raise ValueError("tool_schema_unknown_field")
        center_id = args.get("center_id")
        if center_id is not None:
            self._require_non_task_membership(engram_id, center_id)
            centers = [self._world.get_activity_center(center_id)]
        else:
            centers = [
                self._world.get_activity_center(membership.center_id)
                for membership in self._world.list_memberships(engram_id=engram_id)
            ]

        concerns = []
        seen: set[str] = set()
        for center in centers:
            if center is None or center.kind is ActivityKind.TASK:
                continue
            for concern in self._world.list_living_concerns(
                center_id=center.id,
                owner_engram_id=engram_id,
            ):
                if concern.id in seen:
                    continue
                seen.add(concern.id)
                concerns.append(self._concern_view(concern))
        return (
            f"已找到 {len(concerns)} 条生活关切。",
            {"concerns": concerns},
            True,
            None,
        )

    def _pulse_life_orientations(
        self,
        engram_id,
        args,
        root,
        tool_call,
        invocation,
    ):
        del root, tool_call, invocation
        if set(args).difference({"center_id", "current_only"}):
            raise ValueError("tool_schema_unknown_field")
        current_only = args.get("current_only", True)
        if type(current_only) is not bool:
            raise ValueError("tool_schema_current_only_invalid")
        center_id = args.get("center_id")
        if center_id is not None:
            self._require_orientation_center(engram_id, center_id)
        rows = self._orientation_rows(
            engram_id,
            {"center_id": center_id, "current_only": current_only},
        )
        return (
            f"已找到 {len(rows)} 条生活取向。",
            {"orientations": rows},
            True,
            None,
        )

    def _pulse_life_orient(
        self,
        engram_id,
        args,
        root,
        tool_call,
        invocation,
    ):
        del invocation
        allowed = {"center_id", "content", "state", "orientation_id"}
        if set(args).difference(allowed):
            raise ValueError("tool_schema_unknown_field")
        if not {"center_id", "content", "state"}.issubset(args):
            raise ValueError("tool_schema_required_field")

        center_id = args.get("center_id")
        content = args.get("content")
        state = args.get("state")
        if not isinstance(center_id, str) or not center_id.strip():
            raise ValueError("tool_schema_string_field")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("tool_schema_string_field")
        if len(content) > 4000:
            raise ValueError("tool_schema_content_invalid")
        if not isinstance(state, str) or not state.strip():
            raise ValueError("tool_schema_string_field")
        try:
            normalized_state = LivingOrientationState(state)
        except ValueError as exc:
            raise ValueError("tool_schema_orientation_state_invalid") from exc

        orientation_id = args.get("orientation_id")
        if orientation_id is not None and (
            not isinstance(orientation_id, str) or not orientation_id.strip()
        ):
            raise ValueError("tool_schema_string_field")

        self._require_orientation_center(engram_id, center_id)
        if tool_call.center_id != center_id:
            raise ValueError("center_attribution_required")

        if orientation_id is None:
            if normalized_state is LivingOrientationState.CLOSED:
                raise ValueError("living_orientation_create_closed_forbidden")
            stable_id = self._stable_id(tool_call.id, "living-orientation")
            existing = self._world.get_living_orientation(stable_id)
            if existing is not None:
                if self._orientation_matches_tool_call(
                    existing,
                    engram_id=engram_id,
                    center_id=center_id,
                    causal_id=root.causal_id,
                    source_event_id=tool_call.id,
                ):
                    orientation = existing
                    changed = False
                else:
                    raise ValueError("living_orientation_id_conflict")
            else:
                current = self._world.list_living_orientations(
                    center_id=center_id,
                    owner_engram_id=engram_id,
                    current_only=True,
                )
                if current:
                    raise ValueError("living_orientation_current_conflict")
                try:
                    orientation = self._world.create_living_orientation(
                        center_id,
                        engram_id,
                        content,
                        root.causal_id,
                        tool_call.id,
                        state=normalized_state,
                        orientation_id=stable_id,
                    )
                except ValueError as exc:
                    if "only one current" in str(exc):
                        raise ValueError(
                            "living_orientation_current_conflict"
                        ) from exc
                    raise
                changed = True
        else:
            current = self._world.get_living_orientation(orientation_id)
            if current is None:
                raise KeyError("living_orientation_not_found")
            if current.owner_engram_id != engram_id:
                raise PermissionError("living_orientation_owner_required")
            if current.center_id != center_id:
                raise PermissionError("living_orientation_center_required")
            if self._orientation_matches_tool_call(
                current,
                engram_id=engram_id,
                center_id=center_id,
                causal_id=root.causal_id,
                source_event_id=tool_call.id,
            ):
                orientation = current
                changed = False
            else:
                if current.state is LivingOrientationState.CLOSED:
                    raise ValueError("living_orientation_closed_terminal")
                orientation = self._world.update_living_orientation(
                    orientation_id,
                    expected_owner_engram_id=engram_id,
                    expected_revision=current.revision,
                    content=content,
                    state=normalized_state,
                    causal_id=root.causal_id,
                    source_event_id=tool_call.id,
                )
                changed = True

        if changed:
            self._record_metric(
                "living_orientation_changed",
                world=self._world_id,
                center=orientation.center_id,
                engram=orientation.owner_engram_id,
                orientation=orientation.id,
                state=orientation.state.value,
                revision=orientation.revision,
            )

        view = self._orientation_view(orientation)
        return (
            "生活取向已保存。",
            {"orientation": view, "orientation_id": orientation.id},
            True,
            None,
        )

    def _pulse_life_hold(self, engram_id, args, root, tool_call, invocation):
        del invocation
        allowed = {
            "center_id",
            "content",
            "disposition",
            "concern_id",
            "revisit_after_seconds",
        }
        if set(args).difference(allowed):
            raise ValueError("tool_schema_unknown_field")
        if not {"center_id", "content", "disposition"}.issubset(args):
            raise ValueError("tool_schema_required_field")

        center_id = args.get("center_id")
        content = args.get("content")
        disposition = args.get("disposition")
        if not isinstance(center_id, str) or not center_id.strip():
            raise ValueError("tool_schema_string_field")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("tool_schema_string_field")
        if len(content) > 4000:
            raise ValueError("tool_schema_content_invalid")
        if not isinstance(disposition, str) or not disposition.strip():
            raise ValueError("tool_schema_string_field")
        try:
            normalized_disposition = LivingConcernDisposition(disposition)
        except ValueError as exc:
            raise ValueError("tool_schema_disposition_invalid") from exc

        revisit_at = self._revisit_at_from_args(args, normalized_disposition)
        self._require_non_task_membership(engram_id, center_id)
        if tool_call.center_id != center_id:
            raise ValueError("center_attribution_required")

        concern_id = args.get("concern_id")
        if concern_id is not None and (
            not isinstance(concern_id, str) or not concern_id.strip()
        ):
            raise ValueError("tool_schema_string_field")
        if (
            normalized_disposition is LivingConcernDisposition.RESOLVED
            and concern_id is None
        ):
            raise ValueError("tool_schema_concern_required")

        if concern_id is None:
            stable_id = self._stable_id(tool_call.id, "living-concern")
            concern = self._world.create_living_concern(
                center_id,
                engram_id,
                content,
                root.causal_id,
                tool_call.id,
                disposition=normalized_disposition,
                revisit_at=revisit_at,
                concern_id=stable_id,
            )
        else:
            concern = self._world.get_living_concern(concern_id)
            if concern is None:
                raise KeyError("living_concern_not_found")
            if concern.owner_engram_id != engram_id:
                raise PermissionError("living_concern_owner_required")
            if concern.center_id != center_id:
                raise PermissionError("living_concern_center_required")
            if concern.disposition is LivingConcernDisposition.RESOLVED:
                raise ValueError("living_concern_resolved_terminal")
            concern = self._world.update_living_concern(
                concern.id,
                expected_owner_engram_id=engram_id,
                expected_revision=concern.revision,
                content=content,
                disposition=normalized_disposition,
                revisit_at=revisit_at,
                causal_id=root.causal_id,
                source_event_id=tool_call.id,
            )

        view = self._concern_view(concern)
        return (
            "生活关切已保存。",
            {"concern": view, "concern_id": concern.id},
            True,
            None,
        )

    def _pulse_life_create(self, engram_id, args, root, tool_call, invocation):
        del root, invocation
        kind = args.get("kind")
        title = args.get("title")
        if kind not in _ALLOWED_LIFE_KINDS:
            raise ValueError("life_center_kind_invalid")
        if not isinstance(title, str) or not title.strip():
            raise ValueError("life_center_title_required")
        self._require_active(engram_id)
        center_id = self._stable_id(tool_call.id, "life-center")
        center = self._world.get_activity_center(center_id)
        if center is None:
            bundle = self._world.create_center_for_existing_engram(
                kind,
                title,
                engram_id,
                str(args.get("description") or ""),
                origin=ActivityOrigin.SELF,
                autonomy=args.get("autonomy", 1.0),
                center_id=center_id,
            )
            center = bundle.center
        elif (
            center.focal_engram_id != engram_id
            or center.origin is not ActivityOrigin.SELF
        ):
            raise CausalTransitionError(
                "stable life center identity is bound to another subject"
            )
        view = self._center_view(center, MembershipRelation.FOCAL.value)
        return (
            f"已建立生活中心：{center.title}",
            {"center": view, "center_id": center.id},
            True,
            None,
        )

    def _pulse_life_update(self, engram_id, args, root, tool_call, invocation):
        del root, tool_call, invocation
        center_id = args.get("center_id")
        if not isinstance(center_id, str) or not center_id.strip():
            raise ValueError("center_id_required")
        self._require_membership(engram_id, center_id)
        current = self._world.get_activity_center(center_id)
        if current is None:
            raise KeyError(center_id)
        status = args.get("status")
        relationship_service = self._task_relationship_service
        if status is not None and relationship_service is not None:
            relationship = relationship_service.get_for_center(center_id)
            if relationship is not None:
                raise PermissionError("task_relationship_controls_center_status")
        if current.status is ActivityCenterStatus.ARCHIVED and status not in {
            None,
            ActivityCenterStatus.ARCHIVED.value,
        }:
            raise ValueError("archived_center_cannot_reactivate")
        updated = self._world.update_activity_center(
            center_id,
            title=args.get("title"),
            description=args.get("description"),
            status=status,
            autonomy=args.get("autonomy"),
        )
        if updated is None:
            raise KeyError(center_id)
        return (
            f"生活中心已更新：{updated.title}",
            {"center": self._center_view(updated, None), "center_id": center_id},
            True,
            None,
        )

    def _pulse_habitat_observe(self, engram_id, args, root, tool_call, invocation):
        del invocation
        organ = args.get("organ")
        target = args.get("target", "")
        if not isinstance(organ, str) or not organ.strip():
            raise ValueError("organ_required")
        content = self._habitat.perceive(organ, target if isinstance(target, str) else "")
        observation = self._child(
            tool_call,
            engram_id,
            kind=CausalEventKind.HABITAT_OBSERVATION,
            domain=CausalEventDomain.HABITAT,
            source=CausalEventSource.HABITAT,
            content=content,
            metadata={"organ": organ.strip()},
        )
        return content, {"observation_event_id": observation.id}, True, observation.id

    def _pulse_habitat_act(self, engram_id, args, root, tool_call, invocation):
        del root
        verb = args.get("verb")
        target = args.get("target", "")
        if not isinstance(verb, str) or not verb.strip():
            raise ValueError("verb_required")
        if not isinstance(target, str):
            raise ValueError("target_invalid")
        payload = args.get("payload", {})
        if not isinstance(payload, dict):
            raise ValueError("payload_object_required")
        payload_text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        idempotency_key = self._effect_key(root_event_id=tool_call.parent_event_id, invocation=invocation)
        effect = self._ledger.begin_effect(
            tool_call.id,
            kind=CausalEventKind.HABITAT_ACTION,
            domain=CausalEventDomain.HABITAT,
            source=CausalEventSource.SELF,
            idempotency_key=idempotency_key,
            engram_id=engram_id,
            metadata={"tool_name": "pulse_habitat_act", "verb": verb.strip()},
            runtime_fence=self._runtime_fence(),
        )
        if effect.status is not CausalEventStatus.RUNNING:
            consequence = self._first_child(effect.id, CausalEventKind.HABITAT_CONSEQUENCE)
            if effect.status is CausalEventStatus.SETTLED and consequence is not None:
                data = {"effect_event_id": effect.id}
                receipt = consequence.metadata.get("habitat_effect_receipt")
                if isinstance(receipt, dict):
                    data["habitat_effect_receipt"] = dict(receipt)
                return consequence.content or "环境行动已完成", data, True, consequence.id
            return "该环境行动尚未可安全重试，请显式对账。", {"effect_event_id": effect.id}, False, effect.id

        try:
            responses = self._habitat.act(
                Action(
                    verb.strip(),
                    target,
                    payload_text,
                    correlation_id=effect.id,
                )
            )
            response = responses[0] if responses else None
            if response is None:
                settled = self._ledger.settle_effect(
                    effect.id,
                    consequence="环境没有返回新的回应。",
                    consequence_metadata={"reply": "yield"},
                    runtime_fence=self._runtime_fence(),
                )
                consequence = self._first_child(
                    settled.id, CausalEventKind.HABITAT_CONSEQUENCE
                )
                parent_id = consequence.id if consequence is not None else settled.id
                return "环境行动已完成。", {"effect_event_id": settled.id}, True, parent_id
            consequence_text = response.detail or ("环境接受了行动。" if response.yielded else "环境拒绝了行动。")
            receipt = (
                None
                if response.effect_receipt is None
                else response.effect_receipt.to_dict()
            )
            if response.reply is Reply.REFUSE:
                failed = self._ledger.fail_effect(
                    effect.id,
                    prompt_accepted=False,
                    error_code="habitat_refused",
                    consequence=consequence_text,
                    consequence_metadata={"reply": "refuse"},
                    runtime_fence=self._runtime_fence(),
                )
                consequence = self._first_child(
                    failed.id, CausalEventKind.HABITAT_CONSEQUENCE
                )
                parent_id = consequence.id if consequence is not None else failed.id
                return consequence_text, {"effect_event_id": failed.id, "reply": "refuse"}, False, parent_id
            settled = self._ledger.settle_effect(
                effect.id,
                consequence=consequence_text,
                consequence_metadata={
                    "reply": "yield",
                    "habitat_effect_receipt": receipt,
                },
                runtime_fence=self._runtime_fence(),
            )
            consequence = self._first_child(
                settled.id, CausalEventKind.HABITAT_CONSEQUENCE
            )
            parent_id = consequence.id if consequence is not None else settled.id
            return consequence_text, {
                "effect_event_id": settled.id,
                "reply": "yield",
                "habitat_effect_receipt": receipt,
            }, True, parent_id
        except Exception:
            uncertain = self._ledger.fail_effect(
                effect.id,
                prompt_accepted=None,
                error_code="habitat_action_unknown",
                consequence="环境行动结果未知，已标记为 uncertain，未自动重试。",
                consequence_metadata={"reply": "uncertain"},
                runtime_fence=self._runtime_fence(),
            )
            consequence = self._first_child(
                uncertain.id, CausalEventKind.HABITAT_CONSEQUENCE
            )
            parent_id = consequence.id if consequence is not None else uncertain.id
            return "环境行动结果未知，已标记为 uncertain，未自动重试。", {"effect_event_id": uncertain.id}, False, parent_id

    def _pulse_habitat_subscribe(self, engram_id, args, root, tool_call, invocation):
        del tool_call, invocation
        self._require_active(engram_id)
        center_id = args.get("center_id")
        if center_id is None:
            center_id = root.center_id
        elif not isinstance(center_id, str) or not center_id.strip():
            raise ValueError("center_id_invalid")
        else:
            self._require_membership(engram_id, center_id)
        channel = args.get("channel")
        if channel is not None and (
            not isinstance(channel, str) or not channel.strip()
        ):
            raise ValueError("channel_invalid")
        normalized_channel = channel.strip() if isinstance(channel, str) else "all"
        existing = self._storage.list_habitat_subscriptions(
            world_id=self._world_id,
            engram_id=engram_id,
            channel=normalized_channel,
            status="active",
        )
        if existing and existing[0].center_id != center_id:
            raise ValueError("habitat_subscription_center_conflict")
        subscription = self._storage.subscribe_habitat(
            self._world_id,
            engram_id,
            normalized_channel,
            center_id=center_id,
        )
        view = self._subscription_view(subscription)
        return (
            "已订阅 Habitat 环境变化。",
            {
                "subscription_id": subscription.id,
                "center_id": subscription.center_id,
                "subscription": view,
            },
            True,
            None,
        )

    def _pulse_delegate(self, engram_id, args, root, tool_call, invocation):
        task = args.get("task")
        target = args.get("to")
        if not isinstance(task, str) or not task.strip():
            raise ValueError("delegation_task_required")
        if target == engram_id:
            raise ValueError("delegation_cannot_target_caller")
        if target is not None:
            self._require_active(target)
        if self._delegation_tunnel is not None:
            if root.center_id is None:
                raise ValueError("delegation_center_required")
            admission = self._delegation_tunnel.enqueue(
                caller_id=engram_id,
                center_id=root.center_id,
                task=task.strip(),
                target_id=target,
                idempotency_key=(
                    f"pi-tool:{root.id}:{invocation.tool_call_id}"
                ),
            )
            self._record_metric(
                "delegation_request_queued",
                event_id=admission.request_event.id,
                target=admission.target_id,
                recovered=admission.recovered,
            )
            return (
                "委派请求已进入原生 tunnel；目标主体会在自己的持久 Pi 会话中回应。",
                {
                    "request_event_id": admission.request_event.id,
                    "record_id": admission.record_id,
                    "target_id": admission.target_id,
                    "status": admission.request_event.status.value,
                },
                True,
                admission.request_event.id,
            )

        # Explicit mock compatibility path. Production Runtime always mounts
        # DurableDelegationTunnel above and never reaches this FrontAgent loop.
        request = self._ledger.begin_effect(
            tool_call.id,
            kind=CausalEventKind.DELEGATION_REQUEST,
            domain=CausalEventDomain.SYSTEM,
            source=CausalEventSource.DELEGATION,
            flow=CausalEventFlow.TUNNEL,
            content=task.strip(),
            metadata={"has_target": target is not None},
            engram_id=engram_id,
            idempotency_key=(
                f"delegation-effect:{root.id}:{invocation.tool_call_id}"
            ),
            runtime_fence=self._runtime_fence(),
        )
        if request.status is CausalEventStatus.RUNNING:
            with self._jobs_lock:
                future = self._jobs.get(request.id)
                if future is None:
                    if self._closed:
                        raise RuntimeError("runtime_quiescing")
                    future = self._executor.submit(
                        self._run_delegation,
                        request.id,
                        engram_id,
                        task.strip(),
                        target,
                    )
                    self._jobs[request.id] = future
                    future.add_done_callback(
                        lambda _future, event_id=request.id: self._forget_job(
                            event_id
                        )
                    )
            ok = True
            status = "queued"
            content = "委派请求已进入 tunnel，结果会沿同一因果链返回。"
        elif request.status is CausalEventStatus.SETTLED:
            self._ensure_delegation_delivery(request)
            ok = True
            status = "settled"
            content = "委派请求已经完成，结果已沿同一因果链返回。"
        else:
            ok = False
            status = request.status.value
            content = "委派请求的结果不确定，未自动重放。"
        return (
            content,
            {"request_event_id": request.id, "status": status},
            ok,
            request.id,
        )

    # ── Durable delegation worker ───────────────────────────────

    def _forget_job(self, request_event_id: str) -> None:
        with self._jobs_lock:
            self._jobs.pop(request_event_id, None)

    def _run_delegation(
        self,
        request_event_id: str,
        caller_id: str,
        task: str,
        target_id: str | None,
    ) -> None:
        content = ""
        error_code: str | None = None
        prompt_accepted: bool | None = False
        try:
            if self._delegator is None:
                raise RuntimeError("delegator_unavailable")
            # Once control enters the Delegator it may have created a target,
            # appended a prompt, or started a Harness turn before raising.  An
            # exception from that boundary is therefore unknown acceptance,
            # never a safe automatic retry.
            prompt_accepted = None
            if target_id is None:
                results = self._delegator.delegate_routed(caller_id, task)
            else:
                results = [self._delegator.delegate(caller_id, task, target_id=target_id)]
            content = results[0].content if results else "委派没有返回内容。"
        except Exception:
            error_code = "delegation_failed"
            content = "委派执行失败，调用者可沿 causal history 显式重试。"
        try:
            if error_code is None:
                request = self._ledger.settle_effect(
                    request_event_id,
                    consequence=content,
                    consequence_kind=CausalEventKind.DELEGATION_RESULT,
                    consequence_domain=CausalEventDomain.SYSTEM,
                    consequence_source=CausalEventSource.DELEGATION,
                    consequence_flow=CausalEventFlow.TUNNEL,
                    consequence_metadata={
                        "status": "settled",
                        "delivery_role": "fact",
                    },
                    runtime_fence=self._runtime_fence(),
                )
            else:
                request = self._ledger.fail_effect(
                    request_event_id,
                    prompt_accepted=prompt_accepted,
                    error_code=error_code,
                    consequence=content,
                    consequence_kind=CausalEventKind.DELEGATION_RESULT,
                    consequence_domain=CausalEventDomain.SYSTEM,
                    consequence_source=CausalEventSource.DELEGATION,
                    consequence_flow=CausalEventFlow.TUNNEL,
                    consequence_metadata={
                        "status": "failed",
                        "error_code": error_code,
                        "delivery_role": "fact",
                    },
                    runtime_fence=self._runtime_fence(),
                )
            event = self._ensure_delegation_delivery(request)
            self._record_metric(
                "delegation_result_queued",
                event_id=event.id if event is not None else None,
                failed=error_code is not None,
            )
        except Exception:
            self._record_metric("delegation_result_queue_failed", failed=True)

    # ── Causal helpers ──────────────────────────────────────────

    def _recover_delegation_deliveries(self) -> None:
        """Rebuild queued delivery roots from already-durable result facts."""

        after_seq = 0
        while True:
            events = self._ledger.list_events(
                after_seq=after_seq,
                limit=500,
                world_id=self._world_id,
                kind=CausalEventKind.DELEGATION_RESULT,
                status=CausalEventStatus.SETTLED,
            )
            if not events:
                return
            for event in events:
                if event.metadata.get("delivery_role") != "fact":
                    continue
                request = (
                    self._ledger.get_event(event.parent_event_id)
                    if event.parent_event_id is not None
                    else None
                )
                if request is not None and request.kind is CausalEventKind.DELEGATION_REQUEST:
                    self._ensure_delegation_delivery(request)
            last_seq = events[-1].seq
            if last_seq is None or len(events) < 500:
                return
            after_seq = last_seq

    def _ensure_delegation_delivery(self, request):
        """Idempotently turn a terminal delegation fact into caller input."""

        fact = next(
            (
                child
                for child in self._ledger.get_children(request.id)
                if child.kind is CausalEventKind.DELEGATION_RESULT
                and child.status is CausalEventStatus.SETTLED
                and child.metadata.get("delivery_role") == "fact"
            ),
            None,
        )
        if fact is None or request.engram_id is None:
            return None
        return self._ledger.enqueue(
            self._world_id,
            flow=CausalEventFlow.TUNNEL,
            domain=CausalEventDomain.SYSTEM,
            kind=CausalEventKind.DELEGATION_RESULT,
            source=CausalEventSource.DELEGATION,
            content=fact.content,
            causal_id=request.causal_id,
            parent_event_id=fact.id,
            engram_id=request.engram_id,
            metadata={
                "delivery_role": "turn",
                "request_event_id": request.id,
                "result_event_id": fact.id,
                "status": fact.metadata.get("status", "settled"),
            },
            idempotency_key=f"delegation-delivery:{fact.id}",
            runtime_fence=self._runtime_fence(),
        )

    def _current_root(self, engram_id: str):
        try:
            turn = self._ledger.get_running_turn(engram_id)
        except CausalTransitionError:
            return None
        if turn is None:
            return None
        event = self._ledger.get_event(turn.event_id)
        if event is None or event.status is not CausalEventStatus.RUNNING:
            return None
        if event.engram_id not in {None, engram_id}:
            return None
        return event

    def _tool_call(
        self,
        root,
        engram_id: str,
        tool_name: str,
        invocation,
        *,
        portfolio_history_limit: int | None = None,
        task_offer_request: Mapping[str, Any] | None = None,
        task_relationship_request: Mapping[str, Any] | None = None,
    ):
        metadata = {
            "tool_name": tool_name,
            "tool_call_id": invocation.tool_call_id,
        }
        if tool_name == "pulse_life_portfolio":
            if portfolio_history_limit is None:
                raise CausalTransitionError(
                    "living_portfolio_history_limit_unbound"
                )
            metadata["portfolio_history_limit"] = portfolio_history_limit
        if tool_name == _TASK_OFFER_RESPONSE_TOOL:
            if task_offer_request is None:
                raise CausalTransitionError(
                    "task_offer_request_binding_required"
                )
            metadata.update({
                "task_offer_decision": task_offer_request["decision"],
                "task_offer_expected_revision": task_offer_request[
                    "expected_revision"
                ],
                "task_offer_response_digest": task_offer_request[
                    "response_digest"
                ],
            })
        if tool_name == _TASK_RELATIONSHIP_RESPONSE_TOOL:
            if task_relationship_request is None:
                raise CausalTransitionError(
                    "task_relationship_request_binding_required"
                )
            metadata.update({
                "task_relationship_id": task_relationship_request[
                    "relationship_id"
                ],
                "task_relationship_action": task_relationship_request["action"],
                "task_relationship_expected_revision": (
                    task_relationship_request["expected_revision"]
                ),
                "task_relationship_response_digest": (
                    task_relationship_request["response_digest"]
                ),
            })
        event = self._child(
            root,
            engram_id,
            kind=CausalEventKind.TOOL_CALL,
            domain=CausalEventDomain.HARNESS,
            source=CausalEventSource.SELF,
            metadata=metadata,
            suffix="tool-call:" + invocation.tool_call_id,
        )
        if (
            event.metadata.get("tool_name") != tool_name
            or event.metadata.get("tool_call_id") != invocation.tool_call_id
        ):
            raise CausalTransitionError(
                "tool_call_id is already bound to another tool identity"
            )
        if tool_name == "pulse_life_portfolio" and (
            type(event.metadata.get("portfolio_history_limit")) is not int
            or event.metadata.get("portfolio_history_limit")
            != portfolio_history_limit
        ):
            raise CausalTransitionError(
                "living_portfolio_history_limit_conflict"
            )
        if tool_name == _TASK_OFFER_RESPONSE_TOOL and any(
            event.metadata.get(key) != value
            for key, value in (
                ("task_offer_decision", metadata["task_offer_decision"]),
                (
                    "task_offer_expected_revision",
                    metadata["task_offer_expected_revision"],
                ),
                (
                    "task_offer_response_digest",
                    metadata["task_offer_response_digest"],
                ),
            )
        ):
            raise CausalTransitionError("task_offer_already_resolved")
        if tool_name == _TASK_RELATIONSHIP_RESPONSE_TOOL and any(
            event.metadata.get(key) != value
            for key, value in (
                (
                    "task_relationship_id",
                    metadata["task_relationship_id"],
                ),
                (
                    "task_relationship_action",
                    metadata["task_relationship_action"],
                ),
                (
                    "task_relationship_expected_revision",
                    metadata["task_relationship_expected_revision"],
                ),
                (
                    "task_relationship_response_digest",
                    metadata["task_relationship_response_digest"],
                ),
            )
        ):
            raise CausalTransitionError("task_relationship_already_resolved")
        return event

    def _record_role_contribution(
        self,
        *,
        engram_id: str,
        tool_name: str,
        root: Any,
        result: Any,
        data: Mapping[str, Any],
        ok: bool,
    ) -> None:
        """Project trusted receipts into one opt-in role without life stimulus.

        This projection is deliberately fail-closed for renewal and fail-open
        for an already completed world effect: a bookkeeping failure cannot
        undo an external mutation, but it also cannot manufacture role credit.
        """

        if not ok or self._role_store is None:
            return
        center_id = getattr(root, "center_id", None)
        if not isinstance(center_id, str) or not center_id:
            return
        try:
            governance = self._purpose_governance
            provider = self._runtime_lease_provider
            store = self._role_store
            if governance is None or provider is None or store is None:
                return
            lineage = governance.find_lineage_for_engram(engram_id)
            if lineage is None or lineage.current_engram_id != engram_id:
                return
            proof = provider()
            if (
                not isinstance(proof, RuntimeLeaseProof)
                or proof.world_id != self._world_id
            ):
                return
            turn = self._ledger.get_running_turn(engram_id)
            if turn is None or turn.event_id != getattr(root, "id", None):
                return
            roles = store.list(
                world_id=self._world_id,
                lineage_id=lineage.lineage_id,
                status=RoleLeaseStatus.ACTIVE,
                limit=100,
            )
            matching = [
                role
                for role in roles
                if role.obligation is not None
                and role.holder_kind is HolderKind.ENGRAM
                and role.holder_id == engram_id
                and center_id in role.scope.center_ids
            ]
            if len(matching) != 1:
                if matching:
                    self._record_metric(
                        "pulse_role_contribution_ambiguous",
                        tool=tool_name,
                        matching_roles=len(matching),
                    )
                return
            role = matching[0]
            result_metadata = getattr(result, "metadata", None)
            result_tool_call_id = (
                result_metadata.get("tool_call_id")
                if isinstance(result_metadata, Mapping)
                else None
            )
            if not isinstance(result_tool_call_id, str) or not result_tool_call_id:
                return
            common = {
                "expected_role_epoch": role.role_epoch,
                "holder_kind": HolderKind.ENGRAM,
                "holder_id": engram_id,
                "scope": role.scope,
                "runtime": proof,
            }
            if tool_name in {"write", "edit"}:
                if data.get("action_request_id") != result_tool_call_id:
                    return
                verified = self._role_receipt_verifier.verify_workspace(
                    data,
                    world_id=self._world_id,
                    holder_kind=HolderKind.ENGRAM,
                    holder_id=engram_id,
                    source_turn_id=turn.id,
                )
                contribution = store.record_contribution(
                    role.role_lease_id,
                    contribution_kind=RoleContributionKind.DIRECT_OUTPUT,
                    verified_receipt=verified,
                    **common,
                )
            elif tool_name == "pulse_habitat_act":
                receipt = data.get("habitat_effect_receipt")
                effect_event_id = data.get("effect_event_id")
                effect_event = (
                    None
                    if not isinstance(effect_event_id, str)
                    else self._ledger.get_event(effect_event_id)
                )
                effect_parent = (
                    None
                    if effect_event is None or effect_event.parent_event_id is None
                    else self._ledger.get_event(effect_event.parent_event_id)
                )
                if (
                    not isinstance(receipt, Mapping)
                    or effect_event is None
                    or effect_event.kind is not CausalEventKind.HABITAT_ACTION
                    or effect_event.status is not CausalEventStatus.SETTLED
                    or effect_event.engram_id != engram_id
                    or effect_parent is None
                    or effect_parent.kind is not CausalEventKind.TOOL_CALL
                    or effect_parent.parent_event_id != root.id
                    or effect_parent.metadata.get("tool_call_id")
                    != result_tool_call_id
                ):
                    return
                verified = self._role_receipt_verifier.verify_habitat(
                    receipt,
                    world_id=self._world_id,
                    holder_kind=HolderKind.ENGRAM,
                    holder_id=engram_id,
                    source_turn_id=turn.id,
                    expected_correlation_id=effect_event_id,
                )
                contribution = store.record_contribution(
                    role.role_lease_id,
                    contribution_kind=RoleContributionKind.DIRECT_OUTPUT,
                    verified_receipt=verified,
                    **common,
                )
            elif tool_name == "pulse_delegate" or tool_name in TASK_WORKER_TOOL_NAMES:
                result_id = getattr(result, "id", None)
                if not isinstance(result_id, str) or not result_id:
                    return
                contribution = store.record_contribution(
                    role.role_lease_id,
                    contribution_kind=RoleContributionKind.COORDINATION,
                    evidence_event_id=result_id,
                    evidence_class=RoleContributionEvidence.CONTROL_ONLY,
                    source_turn_id=turn.id,
                    **common,
                )
            else:
                return
            self._record_metric(
                "pulse_role_contribution_recorded",
                contribution_kind=contribution.contribution_kind.value,
                output_kind=(
                    "none"
                    if contribution.output_kind is None
                    else contribution.output_kind.value
                ),
            )
        except RoleLeaseError as exc:
            self._record_metric(
                "pulse_role_contribution_failed",
                reason_code=exc.code,
            )
        except Exception:
            self._record_metric(
                "pulse_role_contribution_failed",
                reason_code="role_contribution_projection_failed",
            )

    def _tool_result(
        self,
        parent_id: str,
        engram_id: str,
        tool_name: str,
        invocation,
        content: str,
        *,
        ok: bool,
        data: Mapping[str, Any] | None = None,
        error_code: str | None = None,
    ):
        parent = self._ledger.get_event(parent_id)
        if parent is None:
            raise KeyError(parent_id)
        event_id = self._stable_id(
            parent.id, f"tool-result:{tool_name}:{invocation.tool_call_id}"
        )
        existing = self._ledger.get_event(event_id)
        if existing is not None:
            self._assert_existing_child(
                existing,
                parent,
                kind=CausalEventKind.TOOL_RESULT,
                domain=CausalEventDomain.HARNESS,
                source=CausalEventSource.SELF,
                engram_id=engram_id,
            )
            return existing
        metadata: dict[str, Any] = {
            "tool_name": tool_name,
            "tool_call_id": invocation.tool_call_id,
            "ok": ok,
            # Retries need stable object IDs, not a second copy of the tool
            # result.  Natural/structured results stay in event content and
            # domain stores; causal metadata remains the safe ID/enum sideband.
            "result_refs": self._safe_result_refs(data or {}),
        }
        if error_code is not None:
            metadata["error_code"] = error_code
        return self._ledger.record_child(
            parent_id,
            event_id=event_id,
            engram_id=engram_id,
            kind=CausalEventKind.TOOL_RESULT,
            domain=CausalEventDomain.HARNESS,
            source=CausalEventSource.SELF,
            status=CausalEventStatus.SETTLED if ok else CausalEventStatus.FAILED,
            content=content,
            metadata=metadata,
            runtime_fence=self._runtime_fence(),
        )

    def _record_failed_tool_result(
        self,
        tool_call,
        engram_id: str,
        tool_name: str,
        invocation: ToolInvocationContext,
        error_code: str,
    ) -> str | None:
        if tool_call is None:
            return None
        try:
            result = self._tool_result(
                tool_call.id,
                engram_id,
                tool_name,
                invocation,
                "",
                ok=False,
                error_code=error_code,
            )
        except Exception:
            return None
        return result.id

    @staticmethod
    def _safe_result_refs(data: Mapping[str, Any]) -> dict[str, Any]:
        refs: dict[str, Any] = {}
        for key, value in data.items():
            if key.endswith("_id") and isinstance(value, str):
                refs[key] = value
            elif key in {
                "action",
                "decision",
                "reply",
                "status",
                "evidence_class",
                "before_digest",
                "after_digest",
                "post_digest",
                "task_offer_revision",
                "task_relationship_revision",
            } and isinstance(value, (str, bool, int)):
                refs[key] = value
            elif key == "changed_paths" and isinstance(value, (list, tuple)):
                paths = [
                    item[:512]
                    for item in value[:128]
                    if isinstance(item, str) and item
                ]
                if paths:
                    refs[key] = paths
            elif key == "habitat_effect_receipt" and isinstance(value, Mapping):
                receipt: dict[str, Any] = {}
                for nested in (
                    "journal_effect_id",
                    "correlation_id",
                    "kind",
                    "before_digest",
                    "after_digest",
                    "terminal_state",
                ):
                    candidate = value.get(nested)
                    if candidate is None and nested in {
                        "correlation_id",
                        "before_digest",
                    }:
                        receipt[nested] = None
                    elif isinstance(candidate, str) and candidate:
                        receipt[nested] = candidate[:512]
                if receipt:
                    refs[key] = receipt
            elif key == "subscription" and isinstance(value, Mapping):
                for nested in (
                    "id",
                    "world_id",
                    "engram_id",
                    "center_id",
                    "channel",
                    "status",
                ):
                    candidate = value.get(nested)
                    if isinstance(candidate, str):
                        refs[f"subscription_{nested}"] = candidate
            elif key == "orientation" and isinstance(value, Mapping):
                for nested in (
                    "id",
                    "center_id",
                    "owner_engram_id",
                    "state",
                    "revision",
                    "engagement_count",
                ):
                    candidate = value.get(nested)
                    if isinstance(candidate, (str, int)) and not isinstance(
                        candidate, bool
                    ):
                        refs[f"orientation_{nested}"] = candidate
            elif key == "orientations" and isinstance(value, list):
                ids = [
                    item.get("id")
                    for item in value
                    if isinstance(item, Mapping) and isinstance(item.get("id"), str)
                ]
                if ids:
                    refs["orientation_ids"] = ids
        return refs

    def _find_tool_result(self, tool_call_id: str, invocation_id: str):
        """Find a previous result below a tool call, including effect chains."""

        pending = [tool_call_id]
        seen: set[str] = set()
        while pending:
            parent_id = pending.pop()
            if parent_id in seen:
                continue
            seen.add(parent_id)
            for child in self._ledger.get_children(parent_id):
                if (
                    child.kind == CausalEventKind.TOOL_RESULT
                    and child.metadata.get("tool_call_id") == invocation_id
                ):
                    return child
                pending.append(child.id)
        return None

    def _child(self, parent, engram_id: str, *, kind, domain, source, content=None, metadata=None, flow=None, suffix=None):
        suffix = suffix or kind.value
        event_id = self._stable_id(parent.id, suffix)
        existing = self._ledger.get_event(event_id)
        if existing is not None:
            self._assert_existing_child(
                existing,
                parent,
                kind=kind,
                domain=domain,
                source=source,
                engram_id=engram_id,
                flow=flow,
            )
            return existing
        return self._ledger.record_child(
            parent.id,
            event_id=event_id,
            engram_id=engram_id,
            kind=kind,
            domain=domain,
            source=source,
            flow=flow,
            content=content,
            metadata=metadata or {},
            runtime_fence=self._runtime_fence(),
        )

    @staticmethod
    def _assert_existing_child(
        existing,
        parent,
        *,
        kind,
        domain,
        source,
        engram_id: str,
        flow=None,
    ) -> None:
        expected_flow = flow.value if isinstance(flow, CausalEventFlow) else flow
        actual_flow = existing.flow.value if existing.flow is not None else None
        expected_kind = kind.value if isinstance(kind, CausalEventKind) else kind
        expected_domain = domain.value if isinstance(domain, CausalEventDomain) else domain
        expected_source = source.value if isinstance(source, CausalEventSource) else source
        if not (
            existing.parent_event_id == parent.id
            and existing.causal_id == parent.causal_id
            and existing.engram_id == engram_id
            and existing.kind.value == expected_kind
            and existing.domain.value == expected_domain
            and existing.source.value == expected_source
            and actual_flow == expected_flow
        ):
            raise CausalTransitionError(
                "stable child id is already bound to another causal identity"
            )

    def _first_child(self, parent_id: str, kind):
        return next((child for child in self._ledger.get_children(parent_id) if child.kind == kind), None)

    @staticmethod
    def _stable_id(parent_identity: str, suffix: str) -> str:
        return hashlib.sha256(
            f"{parent_identity}:{suffix}".encode("utf-8")
        ).hexdigest()[:32]

    @staticmethod
    def _effect_key(root_event_id: str | None, invocation: ToolInvocationContext) -> str:
        if not root_event_id:
            raise ValueError("running_root_required")
        return f"habitat-effect:{root_event_id}:{invocation.tool_call_id}"

    def _require_active(self, engram_id: str) -> None:
        engram = self._storage.get_engram(engram_id)
        if engram is None:
            raise KeyError(engram_id)
        if engram.status is not EngramStatus.ACTIVE:
            raise ValueError("archived_engram")

    @staticmethod
    def _is_task_offer_root(root: Any) -> bool:
        # Accepted task roots retain offer provenance.  Only the original
        # null-Center user stimulus is deliberation authority; metadata alone
        # must not disable the formal task turn that acceptance created.
        metadata = getattr(root, "metadata", None)
        return (
            getattr(root, "center_id", None) is None
            and isinstance(metadata, Mapping)
            and (
                "task_offer_id" in metadata
                or "task_offer_revision" in metadata
            )
        )

    @staticmethod
    def _task_offer_context(root: Any) -> tuple[str, int]:
        metadata = getattr(root, "metadata", None)
        offer_id = (
            metadata.get("task_offer_id")
            if isinstance(metadata, Mapping)
            else None
        )
        revision = (
            metadata.get("task_offer_revision")
            if isinstance(metadata, Mapping)
            else None
        )
        if (
            not isinstance(offer_id, str)
            or not offer_id.strip()
            or type(revision) is not int
            or revision < 1
            or root.flow is not CausalEventFlow.CONTENT
            or root.domain is not CausalEventDomain.WORLD
            or root.kind is not CausalEventKind.STIMULUS
            or root.source is not CausalEventSource.USER
            or root.center_id is not None
            or root.parent_event_id is not None
        ):
            raise ValueError("task_offer_context_required")
        return offer_id, revision

    def _require_task_offer_capability(self, root: Any, tool_name: str) -> None:
        """Fail closed before any adapter or life mutation can execute."""

        if (
            self._is_task_offer_root(root)
            and tool_name not in _TASK_OFFER_ALLOWED_TOOLS
        ):
            raise PermissionError("task_offer_deliberation_only")

    @staticmethod
    def _is_task_relationship_root(root: Any) -> bool:
        """Return whether a null-Center root claims negotiation authority."""

        metadata = getattr(root, "metadata", None)
        return (
            root.center_id is None
            and isinstance(metadata, Mapping)
            and (
                "task_relationship_id" in metadata
                or "task_relationship_revision" in metadata
            )
        )

    @staticmethod
    def _task_relationship_negotiation_context(root: Any) -> tuple[str, int]:
        metadata = getattr(root, "metadata", None)
        relationship_id = (
            metadata.get("task_relationship_id")
            if isinstance(metadata, Mapping)
            else None
        )
        revision = (
            metadata.get("task_relationship_revision")
            if isinstance(metadata, Mapping)
            else None
        )
        if (
            not isinstance(relationship_id, str)
            or not relationship_id.strip()
            or type(revision) is not int
            or revision < 1
            or root.flow is not CausalEventFlow.CONTENT
            or root.domain is not CausalEventDomain.WORLD
            or root.kind is not CausalEventKind.STIMULUS
            or root.source is not CausalEventSource.USER
            or root.center_id is not None
            or root.parent_event_id is not None
        ):
            raise ValueError("task_relationship_context_required")
        return relationship_id, revision

    def _require_task_relationship_capability(
        self,
        root: Any,
        tool_name: str,
    ) -> None:
        """Keep a user terms root bounded to observation and subject reply."""

        if not self._is_task_relationship_root(root):
            return
        self._task_relationship_negotiation_context(root)
        if tool_name not in _TASK_RELATIONSHIP_ALLOWED_TOOLS:
            raise PermissionError("task_relationship_negotiation_only")

    def _require_active_task_relationship(
        self,
        root: Any,
        engram_id: str,
        tool_name: str,
    ) -> None:
        """Re-check subject consent before every tool in a managed task root."""

        center_id = getattr(root, "center_id", None)
        if center_id is None:
            return
        center = self._world.get_activity_center(center_id)
        if center is None:
            raise KeyError("activity_center_not_found")
        if center.kind is not ActivityKind.TASK:
            return
        service = self._task_relationship_service
        if service is None:
            return
        snapshot = service.get_for_center(center_id)
        if snapshot is None:
            return
        relationship = getattr(snapshot, "relationship", None)
        if relationship is None:
            raise PermissionError("task_relationship_service_unavailable")
        if relationship.current_subject_engram_id != engram_id:
            raise PermissionError("task_relationship_subject_mismatch")
        if relationship.status is TaskRelationshipStatus.ACTIVE:
            return
        if tool_name in _TASK_RELATIONSHIP_ALLOWED_TOOLS:
            return
        raise PermissionError("task_relationship_not_active")

    def _require_task_relationship_response_context(
        self,
        root: Any,
        engram_id: str,
        request: Mapping[str, Any] | None,
    ) -> None:
        """Prove that this running content root may carry a subject decision."""

        if request is None:
            raise ValueError("task_relationship_context_required")
        if (
            root.engram_id != engram_id
            or root.flow is not CausalEventFlow.CONTENT
            or root.domain
            not in {
                CausalEventDomain.PULSE,
                CausalEventDomain.WORLD,
                CausalEventDomain.HABITAT,
            }
            or root.kind
            not in {
                CausalEventKind.STIMULUS,
                CausalEventKind.SPONTANEOUS,
                CausalEventKind.PULSE,
                CausalEventKind.PROPAGATION,
            }
            or root.source
            not in {
                CausalEventSource.USER,
                CausalEventSource.SELF,
                CausalEventSource.HABITAT,
                CausalEventSource.SENSORY,
                CausalEventSource.PROPAGATION,
            }
        ):
            raise PermissionError("task_relationship_context_required")
        if self._is_task_offer_root(root) and root.center_id is None:
            raise PermissionError("task_relationship_context_required")
        if self._is_task_relationship_root(root):
            relationship_id, revision = (
                self._task_relationship_negotiation_context(root)
            )
            if request.get("relationship_id") != relationship_id:
                raise PermissionError("task_relationship_context_required")
            if request.get("expected_revision") != revision:
                raise ValueError("task_relationship_revision_conflict")
            return
        if root.center_id is None:
            return
        center = self._world.get_activity_center(root.center_id)
        if center is None:
            raise PermissionError("task_relationship_context_required")
        self._require_membership(engram_id, center.id)

    def _require_life_mutation_context(self, root, tool_name: str) -> None:
        """Keep externally bounded work from silently rewriting a life.

        Read-only life tools remain available everywhere.  Mutable life,
        subject-role and Habitat tools are refused when the running causal
        root belongs to a Task Center or to a control/delegation source.  A
        direct Engram turn and a non-task Center retain their existing
        behavior, so this is a context boundary rather than a second identity.
        """

        if tool_name not in _TASK_CONTEXT_BLOCKED_LIFE_MUTATIONS:
            return
        if (
            root.flow is CausalEventFlow.TUNNEL
            or root.source in {
                CausalEventSource.DELEGATION,
                CausalEventSource.SYSTEM,
            }
        ):
            raise PermissionError("task_context_life_mutation_forbidden")
        if root.center_id is None:
            return
        center = self._world.get_activity_center(root.center_id)
        if center is None:
            raise KeyError("activity_center_not_found")
        if center.kind is ActivityKind.TASK:
            raise PermissionError("task_context_life_mutation_forbidden")

    def _require_membership(self, engram_id: str, center_id: str) -> None:
        self._require_active(engram_id)
        if not any(
            membership.center_id == center_id
            for membership in self._world.list_memberships(
                center_id=center_id,
                engram_id=engram_id,
            )
        ):
            raise PermissionError("center_membership_required")

    def _require_non_task_membership(self, engram_id: str, center_id: str) -> None:
        if not isinstance(center_id, str) or not center_id.strip():
            raise ValueError("center_id_required")
        self._require_membership(engram_id, center_id)
        center = self._world.get_activity_center(center_id)
        if center is None:
            raise KeyError("activity_center_not_found")
        if center.kind is ActivityKind.TASK:
            raise ValueError("living_concern_task_center_forbidden")

    def _require_orientation_center(self, engram_id: str, center_id: str) -> None:
        if not isinstance(center_id, str) or not center_id.strip():
            raise ValueError("center_id_required")
        self._require_membership(engram_id, center_id)
        center = self._world.get_activity_center(center_id)
        if center is None:
            raise KeyError("activity_center_not_found")
        if center.kind is ActivityKind.TASK:
            raise ValueError("living_orientation_task_center_forbidden")

    def _orientation_rows(
        self,
        engram_id: str,
        args: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        center_id = args.get("center_id")
        current_only = args.get("current_only", True)
        orientations = self._world.list_living_orientations(
            center_id=center_id,
            owner_engram_id=engram_id,
            current_only=current_only,
        )
        return [self._orientation_view(orientation) for orientation in orientations]

    @staticmethod
    def _orientation_matches_tool_call(
        orientation,
        *,
        engram_id: str,
        center_id: str,
        causal_id: str,
        source_event_id: str,
    ) -> bool:
        return (
            orientation.owner_engram_id == engram_id
            and orientation.center_id == center_id
            and orientation.causal_id == causal_id
            and orientation.source_event_id == source_event_id
        )

    @staticmethod
    def _revisit_at_from_args(
        args: Mapping[str, Any], disposition: LivingConcernDisposition
    ) -> datetime | None:
        present = "revisit_after_seconds" in args
        if disposition is LivingConcernDisposition.REVISIT:
            if not present:
                raise ValueError("tool_schema_revisit_required")
            seconds = args.get("revisit_after_seconds")
            if (
                isinstance(seconds, bool)
                or not isinstance(seconds, (int, float))
                or not math.isfinite(float(seconds))
                or not 0 <= float(seconds) <= 31_536_000
            ):
                raise ValueError("tool_schema_revisit_invalid")
            return datetime.now(timezone.utc) + timedelta(seconds=float(seconds))
        if present:
            raise ValueError("tool_schema_revisit_not_allowed")
        return None

    @staticmethod
    def _center_view(center, relation: str | None) -> dict[str, Any]:
        view = {
            "id": center.id,
            "kind": center.kind.value,
            "title": center.title,
            "description": center.description,
            "status": center.status.value,
            "origin": center.origin.value,
            "autonomy": center.autonomy,
            "focal_engram_id": center.focal_engram_id,
        }
        if relation is not None:
            view["relation"] = relation
        return view

    @staticmethod
    def _concern_view(concern) -> dict[str, Any]:
        def stamp(value: datetime | None) -> str | None:
            return value.isoformat() if value is not None else None

        return {
            "id": concern.id,
            "center_id": concern.center_id,
            "owner_engram_id": concern.owner_engram_id,
            "content": concern.content,
            "disposition": concern.disposition.value,
            "revisit_at": stamp(concern.revisit_at),
            "causal_id": concern.causal_id,
            "source_event_id": concern.source_event_id,
            "revision": concern.revision,
            "last_reentry_event_id": concern.last_reentry_event_id,
            "created_at": stamp(concern.created_at),
            "updated_at": stamp(concern.updated_at),
            "resolved_at": stamp(concern.resolved_at),
        }

    @staticmethod
    def _orientation_view(orientation) -> dict[str, Any]:
        def stamp(value: datetime | None) -> str | None:
            return value.isoformat() if value is not None else None

        return {
            "id": orientation.id,
            "center_id": orientation.center_id,
            "owner_engram_id": orientation.owner_engram_id,
            "content": orientation.content,
            "state": orientation.state.value,
            "causal_id": orientation.causal_id,
            "source_event_id": orientation.source_event_id,
            "revision": orientation.revision,
            "engagement_count": orientation.engagement_count,
            "next_eligible_at": stamp(orientation.next_eligible_at),
            "last_engagement_event_id": orientation.last_engagement_event_id,
            "last_engaged_at": stamp(orientation.last_engaged_at),
            "created_at": stamp(orientation.created_at),
            "updated_at": stamp(orientation.updated_at),
            "closed_at": stamp(orientation.closed_at),
        }

    @staticmethod
    def _purpose_revision_view(revision: PurposeRevision) -> dict[str, Any]:
        return {
            "purpose_revision_id": revision.purpose_revision_id,
            "lineage_id": revision.lineage_id,
            "author_engram_id": revision.author_engram_id,
            "revision": revision.revision,
            "predecessor_revision_id": revision.predecessor_revision_id,
            "amendment_kind": revision.amendment_kind.value,
            "content": revision.content,
            "content_digest": revision.content_digest,
            "state": revision.state.value,
            "source_event_id": revision.source_event_id,
            "reflection_event_id": revision.reflection_event_id,
            "created_at": revision.created_at.isoformat(),
            "superseded_at": (
                None
                if revision.superseded_at is None
                else revision.superseded_at.isoformat()
            ),
        }

    @staticmethod
    def _purpose_proposal_view(
        proposal: PurposeAmendmentProposal,
    ) -> dict[str, Any]:
        return {
            "proposal_id": proposal.proposal_id,
            "lineage_id": proposal.lineage_id,
            "author_engram_id": proposal.author_engram_id,
            "harness_turn_id": proposal.harness_turn_id,
            "tool_call_event_id": proposal.tool_call_event_id,
            "expected_revision": proposal.expected_revision,
            "amendment_kind": proposal.amendment_kind.value,
            "content": proposal.content,
            "content_digest": proposal.content_digest,
            "source_event_id": proposal.source_event_id,
            "source_causal_id": proposal.source_causal_id,
            "source_kind": proposal.source_kind,
            "source_domain": proposal.source_domain,
            "source_flow": proposal.source_flow,
            "source_center_id": proposal.source_center_id,
            "source_provenance_digest": proposal.source_provenance_digest,
            "state": proposal.state.value,
            "committed_revision_id": proposal.committed_revision_id,
            "result_event_id": proposal.result_event_id,
            "resolution_code": proposal.resolution_code,
            "created_at": proposal.created_at.isoformat(),
            "resolved_at": (
                None
                if proposal.resolved_at is None
                else proposal.resolved_at.isoformat()
            ),
        }

    def _role_lease_view(self, role: RoleLease) -> dict[str, Any]:
        # RoleLease.to_dict is intentionally content-free and never includes
        # purpose text or a prompt fragment.
        view = role.to_dict()
        if role.obligation is not None and self._role_store is not None:
            view["contribution_summary"] = self._role_store.contribution_summary(
                role.role_lease_id
            ).to_dict()
        return view

    @staticmethod
    def _subscription_view(subscription) -> dict[str, Any]:
        return {
            "id": subscription.id,
            "world_id": subscription.world_id,
            "engram_id": subscription.engram_id,
            "center_id": subscription.center_id,
            "channel": subscription.channel,
            "status": subscription.status.value,
            "last_fingerprint": subscription.last_fingerprint,
        }

    def _authorization_decision(
        self,
        root,
        engram_id: str,
        tool_name: str,
        allow: bool,
        reason_code: str,
    ) -> dict[str, Any]:
        """Persist only the safe decision, never the ephemeral tool input."""

        self._ledger.record_child(
            root.id,
            event_id=uuid.uuid4().hex,
            engram_id=engram_id,
            kind=CausalEventKind.SYSTEM,
            domain=CausalEventDomain.HARNESS,
            source=CausalEventSource.SYSTEM,
            status=(
                CausalEventStatus.SETTLED if allow else CausalEventStatus.FAILED
            ),
            metadata={
                "phase": "tool_authorization",
                "tool_name": tool_name,
                "allow": allow,
                "reason_code": reason_code,
            },
            runtime_fence=self._runtime_fence(),
        )
        return {"allow": allow, "reason_code": reason_code}

    def _safe_workspace_input(self, tool_name: str, value: Any) -> bool:
        """Prove every built-in path stays in the public workspace.

        The former heuristic accepted a request when *any* string looked like a
        safe path, so a harmless ``'.'`` could mask a second ``../../escape``.
        Pi's read-only tools have explicit path-shaped fields; every supplied
        value in those fields must be proven safe.  grep/find/ls may omit a path
        to mean the workspace root, while read may not.
        """

        if not isinstance(value, Mapping) or self._contains_private_material(value):
            return False
        path_values: list[Any] = []
        for key, child in value.items():
            normalized_key = key.casefold() if isinstance(key, str) else ""
            if normalized_key in _PATH_INPUT_KEYS or normalized_key.endswith(
                ("_path", "_paths", "_directory", "_dir", "_root")
            ):
                if isinstance(child, list):
                    path_values.extend(child)
                else:
                    path_values.append(child)
        if not path_values:
            return tool_name in {"grep", "find", "ls"}
        for raw in path_values:
            if not isinstance(raw, str) or not raw.strip():
                return False
            if not self._inside_workspace(raw) or self._inside_private(raw):
                return False
        return True

    def _inside_workspace(self, raw: str) -> bool:
        path = Path(raw)
        if not path.is_absolute():
            path = self._workspace / path
        try:
            path.resolve().relative_to(self._workspace)
            return True
        except ValueError:
            return False

    def _inside_private(self, raw: str) -> bool:
        path = Path(raw)
        if not path.is_absolute():
            path = self._workspace / path
        try:
            path.resolve().relative_to(self._workspace / ".pulse")
            return True
        except ValueError:
            return self._contains_private_material(raw)

    @staticmethod
    def _strings(value: Any) -> list[str]:
        if isinstance(value, str):
            return [value]
        if isinstance(value, Mapping):
            out: list[str] = []
            for key, child in value.items():
                if isinstance(key, str):
                    out.append(key)
                out.extend(LifeToolService._strings(child))
            return out
        if isinstance(value, list):
            out: list[str] = []
            for child in value:
                out.extend(LifeToolService._strings(child))
            return out
        return []

    @staticmethod
    def _contains_private_material(value: Any) -> bool:
        return any(
            marker in text.casefold()
            for text in LifeToolService._strings(value)
            for marker in _PRIVATE_MARKERS
        )

    @staticmethod
    def _reject(code: str, *, event_id: str | None = None) -> dict[str, Any]:
        return {
            "ok": False,
            "content": "",
            "data": {},
            "event_id": event_id,
            "error": code,
        }

    @staticmethod
    def _exception_code(exc: BaseException) -> str:
        """Preserve only an internal, schema-safe reason code."""

        candidate = exc.args[0] if exc.args else ""
        if isinstance(candidate, str) and candidate.isidentifier():
            return candidate
        return type(exc).__name__.replace("Error", "").casefold()

    def _record_metric(self, event_type: str, **payload: Any) -> None:
        if self._metrics is not None:
            try:
                self._metrics.record(event_type, **payload)
            except Exception:
                pass

"""Durable, Center-scoped delegation over native Pi Harness turns.

This module owns admission, durable identity, result reconciliation, and the
single outcome-learning entry point. It deliberately does not execute a
model, create an Engram, create a Harness session, or run a Python tool loop.
The ordinary Center scheduler and ``EngramManager`` claim the queued target
event and execute it through that Engram's persistent Pi session.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Callable, Protocol

from pulse_system.core.causality import CausalLedger, RuntimeFence
from pulse_system.core.types import (
    CausalEvent,
    CausalEventDomain,
    CausalEventFlow,
    CausalEventKind,
    CausalEventSource,
    CausalEventStatus,
    EngramStatus,
    GenerationTransitionState,
)
from pulse_system.substrate.storage import Storage


DURABLE_DELEGATION_MODE = "durable_pi"
DELEGATION_OUTCOMES = frozenset({"adopted", "revised", "discarded"})
UNSUPPORTED_DELEGATION_MODES = frozenset({"snapshot", "canary"})
_EXECUTION_OWNER = "scheduler_engram_manager_persistent_pi_session"


class DelegationTunnelError(RuntimeError):
    """Base class for durable delegation domain failures."""


class DelegationRejectedError(DelegationTunnelError):
    """The request was rejected before any durable acceptance."""


class DelegationAuthorizationError(DelegationRejectedError):
    """Caller or target is not authorized in the source Center."""


class DelegationConflictError(DelegationTunnelError):
    """A stable identity is already bound to different durable facts."""


class DelegationInvariantError(DelegationTunnelError):
    """Persisted delegation state violates the tunnel contract."""


class UnsupportedDelegationModeError(DelegationRejectedError):
    """A router requested snapshot/canary behavior that this tunnel forbids."""


class RouteDecisionPort(Protocol):
    target_id: str | None
    canary_id: str | None


class DelegationRouterPort(Protocol):
    def choose(
        self,
        caller_id: str,
        task_embedding,
        candidate_ids: list[str],
    ) -> RouteDecisionPort: ...

    def learn_from_history(self) -> int: ...


@dataclass(frozen=True)
class DelegationIdentity:
    request_event_id: str
    record_id: str
    idempotency_key: str


@dataclass(frozen=True)
class DelegationAdmission:
    request_event: CausalEvent
    record_id: str
    caller_id: str
    target_id: str
    center_id: str
    recovered: bool


@dataclass(frozen=True)
class DelegationReconciliation:
    request_event_id: str
    record_id: str
    state: str
    delivery_event: CausalEvent | None = None
    record_completed: bool = False


@dataclass(frozen=True)
class DelegationOutcomeUpdate:
    record_id: str
    outcome: str
    changed: bool
    learning_updates: int


class DurableDelegationTunnel:
    """Domain core for one durable, native-Pi delegation tunnel."""

    def __init__(
        self,
        storage: Storage,
        ledger: CausalLedger,
        *,
        world_id: str,
        router: DelegationRouterPort | None = None,
        runtime_fence_provider: Callable[[], RuntimeFence] | None = None,
    ) -> None:
        self._storage = storage
        self._ledger = ledger
        self._world_id = self._require_id(world_id, "world_id")
        self._router = router
        if runtime_fence_provider is not None and not callable(
            runtime_fence_provider
        ):
            raise ValueError("runtime_fence_provider must be callable or null")
        self._runtime_fence_provider = runtime_fence_provider

    def _runtime_fence(self) -> RuntimeFence | None:
        provider = self._runtime_fence_provider
        return None if provider is None else provider()

    @staticmethod
    def identity_for(
        world_id: str,
        caller_id: str,
        center_id: str,
        idempotency_key: str,
    ) -> DelegationIdentity:
        """Derive stable event, record, and ledger idempotency identities."""

        parts = [
            DurableDelegationTunnel._require_id(world_id, "world_id"),
            DurableDelegationTunnel._require_id(caller_id, "caller_id"),
            DurableDelegationTunnel._require_id(center_id, "center_id"),
            DurableDelegationTunnel._require_id(
                idempotency_key, "idempotency_key"
            ),
        ]
        canonical = json.dumps(
            parts, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        request_digest = hashlib.sha256(
            b"delegation-request\0" + canonical
        ).hexdigest()
        record_digest = hashlib.sha256(
            b"delegation-record\0" + canonical
        ).hexdigest()
        return DelegationIdentity(
            request_event_id=f"delegation-request-{request_digest}",
            record_id=f"delegation-record-{record_digest}",
            idempotency_key=f"delegation-request:{request_digest}",
        )

    def enqueue(
        self,
        *,
        caller_id: str,
        center_id: str,
        task: str,
        idempotency_key: str,
        target_id: str | None = None,
    ) -> DelegationAdmission:
        """Authorize and durably enqueue one target-owned request.

        No acceptance is returned until ``CausalLedger.enqueue`` has committed.
        If the process dies after that commit but before the delegation record
        projection, the same call or :meth:`reconcile_ready` repairs the record
        without creating another request.
        """

        caller_id = self._require_id(caller_id, "caller_id")
        center_id = self._require_id(center_id, "center_id")
        task = self._require_text(task, "task")
        requested_target = (
            None if target_id is None else self._require_id(target_id, "target_id")
        )
        if requested_target == caller_id:
            raise DelegationRejectedError("target must differ from caller")

        identity = self.identity_for(
            self._world_id, caller_id, center_id, idempotency_key
        )
        existing = self._ledger.find_causal_event_by_idempotency(
            self._world_id, identity.idempotency_key
        )
        if existing is not None:
            self._assert_existing_request(
                existing,
                identity=identity,
                caller_id=caller_id,
                center_id=center_id,
                task=task,
                requested_target=requested_target,
            )
            self._ensure_record(existing)
            assert existing.engram_id is not None
            return DelegationAdmission(
                request_event=existing,
                record_id=identity.record_id,
                caller_id=caller_id,
                target_id=existing.engram_id,
                center_id=center_id,
                recovered=True,
            )

        members = self._active_center_members(center_id)
        if caller_id not in members:
            raise DelegationAuthorizationError(
                "caller must be an active member of the source Center"
            )
        candidates = sorted(member for member in members if member != caller_id)
        if requested_target is not None:
            if requested_target not in candidates:
                raise DelegationAuthorizationError(
                    "target must be active and belong to the source Center"
                )
            selected_target = requested_target
            route = "explicit"
        else:
            if not candidates:
                raise DelegationRejectedError(
                    "source Center has no other active delegation participant"
                )
            selected_target, route = self._select_target(caller_id, candidates)

        metadata = {
            "delegation_mode": DURABLE_DELEGATION_MODE,
            "record_id": identity.record_id,
            "caller_id": caller_id,
            "admitted_target_id": selected_target,
            "requested_target_id": requested_target,
            "route": route,
            "execution_owner": _EXECUTION_OWNER,
            "snapshot_supported": False,
            "canary_supported": False,
        }
        request = self._ledger.enqueue(
            world_id=self._world_id,
            flow=CausalEventFlow.TUNNEL,
            domain=CausalEventDomain.SYSTEM,
            kind=CausalEventKind.DELEGATION_REQUEST,
            source=CausalEventSource.DELEGATION,
            content=task,
            causal_id=identity.request_event_id,
            engram_id=selected_target,
            center_id=center_id,
            metadata=metadata,
            idempotency_key=identity.idempotency_key,
            event_id=identity.request_event_id,
            admission_guard=self._delegation_admission_guard(
                caller_id,
                selected_target,
                center_id,
            ),
            runtime_fence=self._runtime_fence(),
        )
        self._assert_existing_request(
            request,
            identity=identity,
            caller_id=caller_id,
            center_id=center_id,
            task=task,
            requested_target=requested_target,
        )
        self._ensure_record(request)
        return DelegationAdmission(
            request_event=request,
            record_id=identity.record_id,
            caller_id=caller_id,
            target_id=selected_target,
            center_id=center_id,
            recovered=False,
        )

    def reconcile(self, request_event_id: str) -> DelegationReconciliation:
        """Repair projections and deliver one terminal request exactly once."""

        request_event_id = self._require_id(request_event_id, "request_event_id")
        request = self._ledger.get_event(request_event_id)
        if request is None:
            raise KeyError(f"unknown delegation request: {request_event_id}")
        caller_id, record_id = self._request_owners(request)
        self._ensure_record(request)

        if request.status is CausalEventStatus.SETTLED:
            result = self._assistant_result(request)
            delivery_identity = self._derived_event_identity(
                "delegation-result", request.id
            )
            admitted_target_id = self._request_admitted_target_id(request)
            delivery_owner_id = self._delivery_owner_for_event(
                caller_id,
                delivery_identity[0],
            )
            delivery = self._ledger.enqueue(
                world_id=request.world_id,
                flow=CausalEventFlow.TUNNEL,
                domain=CausalEventDomain.SYSTEM,
                kind=CausalEventKind.DELEGATION_RESULT,
                source=CausalEventSource.DELEGATION,
                content=result.content,
                causal_id=request.causal_id,
                parent_event_id=result.id,
                engram_id=delivery_owner_id,
                center_id=request.center_id,
                metadata={
                    "delegation_mode": DURABLE_DELEGATION_MODE,
                    "request_event_id": request.id,
                    "assistant_result_event_id": result.id,
                    "record_id": record_id,
                    "caller_id": caller_id,
                    "delivery_engram_id": delivery_owner_id,
                    "admitted_target_id": admitted_target_id,
                    "execution_target_id": request.engram_id,
                    "target_id": request.engram_id,
                    "has_result": True,
                },
                idempotency_key=delivery_identity[1],
                event_id=delivery_identity[0],
                admission_guard=self._active_delivery_admission_guard(
                    delivery_owner_id
                ),
                runtime_fence=self._runtime_fence(),
            )
            self._assert_result_delivery(
                delivery,
                request,
                result,
                caller_id,
                admitted_target_id,
            )
            try:
                completed = self._storage.complete_delegation_once(
                    record_id, result.content or ""
                )
            except ValueError as exc:
                raise DelegationConflictError(str(exc)) from exc
            return DelegationReconciliation(
                request_event_id=request.id,
                record_id=record_id,
                state="delivered",
                delivery_event=delivery,
                record_completed=completed,
            )

        if request.status in {
            CausalEventStatus.FAILED,
            CausalEventStatus.UNCERTAIN,
            CausalEventStatus.CANCELLED,
            CausalEventStatus.RECONCILED,
        }:
            fact = self._terminal_status_fact(request, caller_id, record_id)
            return DelegationReconciliation(
                request_event_id=request.id,
                record_id=record_id,
                state=request.status.value,
                delivery_event=fact,
                record_completed=False,
            )

        return DelegationReconciliation(
            request_event_id=request.id,
            record_id=record_id,
            state="pending",
        )

    def reconcile_ready(
        self,
        *,
        after_seq: int = 0,
        limit: int = 100,
    ) -> list[DelegationReconciliation]:
        """Scan durable native-Pi requests for restart repair.

        Legacy delegation effects are ignored: only events carrying this
        module's explicit ``durable_pi`` marker belong to this reconciler.
        """

        events = self._ledger.list_events(
            after_seq=after_seq,
            limit=limit,
            world_id=self._world_id,
            kind=CausalEventKind.DELEGATION_REQUEST,
        )
        return [
            self.reconcile(event.id)
            for event in events
            if event.metadata.get("delegation_mode") == DURABLE_DELEGATION_MODE
        ]

    def record_outcome(
        self,
        record_id: str,
        outcome: str,
    ) -> DelegationOutcomeUpdate:
        """Persist an outcome, then invoke the sole delegation MLP learner."""

        record_id = self._require_id(record_id, "record_id")
        if outcome not in DELEGATION_OUTCOMES:
            allowed = ", ".join(sorted(DELEGATION_OUTCOMES))
            raise ValueError(f"outcome must be one of: {allowed}")
        record = self._storage.get_delegation(record_id)
        if record is None:
            raise KeyError(f"unknown delegation: {record_id}")
        if record["mode"] != DURABLE_DELEGATION_MODE:
            raise UnsupportedDelegationModeError(
                "outcomes for legacy snapshot/canary records are unsupported"
            )
        if record["result_summary"] is None:
            raise DelegationRejectedError(
                "a delegation without a settled result cannot receive an outcome"
            )
        try:
            changed = self._storage.set_delegation_outcome_once(record_id, outcome)
        except ValueError as exc:
            raise DelegationConflictError(str(exc)) from exc

        # Re-run on an idempotent replay as crash repair. DelegationRouter's
        # durable learned-pair set makes learn_from_history idempotent while
        # allowing a crash after the record write to be repaired.
        updates = (
            self._router.learn_from_history() if self._router is not None else 0
        )
        return DelegationOutcomeUpdate(
            record_id=record_id,
            outcome=outcome,
            changed=changed,
            learning_updates=updates,
        )

    def _select_target(
        self,
        caller_id: str,
        candidates: list[str],
    ) -> tuple[str, str]:
        if self._router is None:
            return candidates[0], "center_order"
        decision = self._router.choose(caller_id, None, list(candidates))
        if decision.canary_id is not None:
            raise UnsupportedDelegationModeError(
                "canary routing is unsupported by the durable Pi tunnel"
            )
        if decision.target_id is None:
            raise DelegationRejectedError(
                "router did not select a same-Center active participant"
            )
        if decision.target_id not in candidates:
            raise DelegationAuthorizationError(
                "router selected a target outside the authorized Center candidates"
            )
        return decision.target_id, "router"

    def _active_center_members(self, center_id: str) -> set[str]:
        active: set[str] = set()
        for membership in self._storage.list_center_memberships(center_id=center_id):
            engram = self._storage.get_engram(membership.engram_id)
            if engram is not None and engram.status is EngramStatus.ACTIVE:
                active.add(engram.id)
        return active

    def _delegation_admission_guard(
        self,
        caller_id: str,
        target_id: str,
        center_id: str,
    ) -> Callable[[Any], None]:
        """Recheck Center authorization inside the event transaction."""

        def guard(connection: Any) -> None:
            rows = connection.execute(
                "SELECT memberships.engram_id FROM center_memberships memberships "
                "JOIN engrams ON engrams.id = memberships.engram_id "
                "WHERE memberships.center_id = ? "
                "AND memberships.engram_id IN (?, ?) AND engrams.status = ?",
                (
                    center_id,
                    caller_id,
                    target_id,
                    EngramStatus.ACTIVE.value,
                ),
            ).fetchall()
            members = {str(row[0]) for row in rows}
            if caller_id not in members:
                raise DelegationAuthorizationError(
                    "caller changed before durable delegation admission"
                )
            if target_id not in members:
                raise DelegationAuthorizationError(
                    "target changed before durable delegation admission"
                )

        return guard

    def _active_delivery_admission_guard(
        self,
        engram_id: str,
    ) -> Callable[[Any], None]:
        """Prevent a resolved caller holder from becoming stale before enqueue."""

        def guard(connection: Any) -> None:
            row = connection.execute(
                "SELECT status FROM engrams WHERE id = ?",
                (engram_id,),
            ).fetchone()
            if row is None or row[0] != EngramStatus.ACTIVE.value:
                raise DelegationInvariantError(
                    "delegation delivery holder changed during admission"
                )

        return guard

    def _ensure_record(self, request: CausalEvent) -> str:
        caller_id, record_id = self._request_owners(request)
        assert request.engram_id is not None
        assert request.content is not None
        admitted_target_id = self._request_admitted_target_id(request)
        self._assert_request_execution_lineage(request, admitted_target_id)
        try:
            return self._storage.create_delegation(
                caller_id,
                admitted_target_id,
                request.content,
                DURABLE_DELEGATION_MODE,
                task_embedding=None,
                group_id=None,
                delegation_id=record_id,
            )
        except ValueError as exc:
            raise DelegationConflictError(str(exc)) from exc

    def _request_admitted_target_id(self, request: CausalEvent) -> str:
        """Return the immutable target identity chosen at admission.

        ``request.engram_id`` is deliberately not that identity: while queued,
        the request follows a committed target succession.  New requests carry
        the immutable value in metadata.  Existing records and explicit legacy
        requests provide bounded compatibility for pre-migration rows.
        """

        raw_target = request.metadata.get("admitted_target_id")
        if isinstance(raw_target, str) and raw_target.strip():
            return raw_target.strip()

        record_id = request.metadata.get("record_id")
        if isinstance(record_id, str) and record_id:
            record = self._storage.get_delegation(record_id)
            if record is not None:
                persisted_target = record.get("target_id")
                if isinstance(persisted_target, str) and persisted_target:
                    return persisted_target

        requested_target = request.metadata.get("requested_target_id")
        if isinstance(requested_target, str) and requested_target.strip():
            return requested_target.strip()

        assert request.engram_id is not None
        predecessors = self._ledger.list_generations(
            successor_id=request.engram_id,
            state=GenerationTransitionState.COMMITTED,
        )
        if predecessors:
            raise DelegationInvariantError(
                "legacy routed delegation cannot prove its admitted target after "
                "succession"
            )
        return request.engram_id

    def _assert_request_execution_lineage(
        self,
        request: CausalEvent,
        admitted_target_id: str,
    ) -> None:
        assert request.engram_id is not None
        path = self._committed_lineage_path(admitted_target_id)
        if request.engram_id not in path:
            raise DelegationConflictError(
                "delegation request owner is outside its admitted target lineage"
            )
        if request.status is CausalEventStatus.QUEUED:
            current = self._current_active_lineage_holder(admitted_target_id)
            if request.engram_id != current:
                raise DelegationInvariantError(
                    "queued delegation request is not owned by the current target "
                    "lineage holder"
                )

    def _request_owners(self, request: CausalEvent) -> tuple[str, str]:
        self._assert_request_shape(request)
        caller_id = request.metadata.get("caller_id")
        record_id = request.metadata.get("record_id")
        assert isinstance(caller_id, str)
        assert isinstance(record_id, str)
        return caller_id, record_id

    def _committed_lineage_path(self, root_engram_id: str) -> tuple[str, ...]:
        """Follow the unique committed generation chain without rewriting it."""

        current = self._require_id(root_engram_id, "root_engram_id")
        path: list[str] = []
        seen: set[str] = set()
        while True:
            if current in seen:
                raise DelegationInvariantError(
                    "delegation lineage contains a generation cycle"
                )
            seen.add(current)
            if self._storage.get_engram(current) is None:
                raise DelegationInvariantError(
                    f"delegation lineage holder {current} is missing"
                )
            path.append(current)
            generations = self._ledger.list_generations(
                predecessor_id=current,
                state=GenerationTransitionState.COMMITTED,
            )
            if not generations:
                return tuple(path)
            successors = {
                generation.successor_id
                for generation in generations
                if generation.successor_id is not None
            }
            if len(generations) != 1 or len(successors) != 1:
                raise DelegationInvariantError(
                    "delegation lineage has ambiguous committed successors"
                )
            current = next(iter(successors))

    def _current_active_lineage_holder(self, root_engram_id: str) -> str:
        path = self._committed_lineage_path(root_engram_id)
        active = [
            engram_id
            for engram_id in path
            if (
                (engram := self._storage.get_engram(engram_id)) is not None
                and engram.status is EngramStatus.ACTIVE
            )
        ]
        if len(active) != 1 or active[0] != path[-1]:
            raise DelegationInvariantError(
                "delegation lineage does not have one committed active holder"
            )
        return active[0]

    def _delivery_owner_for_event(
        self,
        caller_id: str,
        event_id: str,
    ) -> str:
        """Resolve a new delivery, or preserve an existing historical owner."""

        existing = self._ledger.get_event(event_id)
        if existing is None:
            return self._current_active_lineage_holder(caller_id)
        if existing.engram_id is None:
            raise DelegationConflictError(
                "delegation delivery identity has no subject owner"
            )
        path = self._committed_lineage_path(caller_id)
        if existing.engram_id not in path:
            raise DelegationConflictError(
                "delegation delivery identity belongs to another caller lineage"
            )
        if existing.status is CausalEventStatus.QUEUED:
            current = self._current_active_lineage_holder(caller_id)
            if existing.engram_id != current:
                raise DelegationInvariantError(
                    "queued delegation delivery is stranded on an inactive caller"
                )
        return existing.engram_id

    def _assert_request_shape(self, request: CausalEvent) -> None:
        if (
            request.world_id != self._world_id
            or request.flow is not CausalEventFlow.TUNNEL
            or request.domain is not CausalEventDomain.SYSTEM
            or request.kind is not CausalEventKind.DELEGATION_REQUEST
            or request.source is not CausalEventSource.DELEGATION
            or request.parent_event_id is not None
            or request.causal_id != request.id
            or request.engram_id is None
            or request.center_id is None
            or not isinstance(request.content, str)
            or not request.content
            or request.metadata.get("delegation_mode") != DURABLE_DELEGATION_MODE
            or request.metadata.get("execution_owner") != _EXECUTION_OWNER
            or request.metadata.get("snapshot_supported") is not False
            or request.metadata.get("canary_supported") is not False
            or request.idempotency_key is None
        ):
            raise DelegationInvariantError(
                "event is not a canonical durable delegation request"
            )
        caller_id = request.metadata.get("caller_id")
        record_id = request.metadata.get("record_id")
        admitted_target_id = request.metadata.get("admitted_target_id")
        if (
            not isinstance(caller_id, str)
            or not caller_id
            or caller_id == request.engram_id
            or not isinstance(record_id, str)
            or not record_id
            or (
                admitted_target_id is not None
                and (
                    not isinstance(admitted_target_id, str)
                    or not admitted_target_id.strip()
                    or admitted_target_id == caller_id
                )
            )
        ):
            raise DelegationInvariantError(
                "delegation request has invalid caller or record identity"
            )

    def _assert_existing_request(
        self,
        event: CausalEvent,
        *,
        identity: DelegationIdentity,
        caller_id: str,
        center_id: str,
        task: str,
        requested_target: str | None,
    ) -> None:
        self._assert_request_shape(event)
        expected = (
            event.id == identity.request_event_id
            and event.idempotency_key == identity.idempotency_key
            and event.metadata.get("record_id") == identity.record_id
            and event.metadata.get("caller_id") == caller_id
            and event.metadata.get("requested_target_id") == requested_target
            and event.center_id == center_id
            and event.content == task
        )
        if requested_target is not None:
            expected = expected and (
                self._request_admitted_target_id(event) == requested_target
            )
        if not expected:
            raise DelegationConflictError(
                "idempotency identity is already bound to another request"
            )

    def _assistant_result(self, request: CausalEvent) -> CausalEvent:
        children = [
            child
            for child in self._ledger.get_children(request.id)
            if child.kind is CausalEventKind.ASSISTANT_RESULT
            and child.status is CausalEventStatus.SETTLED
        ]
        if len(children) != 1 or children[0].content is None:
            raise DelegationInvariantError(
                "settled delegation request must have exactly one assistant result"
            )
        return children[0]

    def _assert_result_delivery(
        self,
        delivery: CausalEvent,
        request: CausalEvent,
        result: CausalEvent,
        caller_id: str,
        admitted_target_id: str,
    ) -> None:
        delivery_origin = delivery.metadata.get("delivery_engram_id")
        caller_path = self._committed_lineage_path(caller_id)
        if (
            delivery.kind is not CausalEventKind.DELEGATION_RESULT
            or delivery.flow is not CausalEventFlow.TUNNEL
            or delivery.domain is not CausalEventDomain.SYSTEM
            or delivery.source is not CausalEventSource.DELEGATION
            or delivery.parent_event_id != result.id
            or delivery.causal_id != request.causal_id
            or delivery.center_id != request.center_id
            or delivery.engram_id not in caller_path
            or delivery.content != result.content
            or delivery.metadata.get("has_result") is not True
            or delivery.metadata.get("request_event_id") != request.id
            or delivery.metadata.get("assistant_result_event_id") != result.id
            or delivery.metadata.get("target_id") != request.engram_id
            or (
                "caller_id" in delivery.metadata
                and delivery.metadata.get("caller_id") != caller_id
            )
            or (
                "admitted_target_id" in delivery.metadata
                and delivery.metadata.get("admitted_target_id")
                != admitted_target_id
            )
            or (
                "execution_target_id" in delivery.metadata
                and delivery.metadata.get("execution_target_id")
                != request.engram_id
            )
            or (
                delivery_origin is not None
                and delivery_origin not in caller_path
            )
        ):
            raise DelegationConflictError(
                "stable delivery identity is bound to another event"
            )

    def _terminal_status_fact(
        self,
        request: CausalEvent,
        caller_id: str,
        record_id: str,
    ) -> CausalEvent:
        content = {
            CausalEventStatus.FAILED: (
                "Delegation failed without an assistant result; it was not replayed."
            ),
            CausalEventStatus.UNCERTAIN: (
                "Delegation outcome is uncertain; it was not replayed automatically."
            ),
            CausalEventStatus.CANCELLED: (
                "Delegation was cancelled without an assistant result."
            ),
            CausalEventStatus.RECONCILED: (
                "Delegation was reconciled without an assistant result."
            ),
        }[request.status]
        event_id, key = self._derived_event_identity(
            f"delegation-{request.status.value}-fact", request.id
        )
        admitted_target_id = self._request_admitted_target_id(request)
        delivery_owner_id = self._delivery_owner_for_event(caller_id, event_id)
        fact = self._ledger.enqueue(
            world_id=request.world_id,
            flow=CausalEventFlow.TUNNEL,
            domain=CausalEventDomain.SYSTEM,
            kind=CausalEventKind.SYSTEM,
            source=CausalEventSource.DELEGATION,
            content=content,
            causal_id=request.causal_id,
            parent_event_id=request.id,
            engram_id=delivery_owner_id,
            center_id=request.center_id,
            metadata={
                "delegation_mode": DURABLE_DELEGATION_MODE,
                "request_event_id": request.id,
                "record_id": record_id,
                "caller_id": caller_id,
                "delivery_engram_id": delivery_owner_id,
                "admitted_target_id": admitted_target_id,
                "execution_target_id": request.engram_id,
                "target_id": request.engram_id,
                "delegation_status": request.status.value,
                "has_result": False,
                "automatic_replay": False,
            },
            idempotency_key=key,
            event_id=event_id,
            admission_guard=self._active_delivery_admission_guard(
                delivery_owner_id
            ),
            runtime_fence=self._runtime_fence(),
        )
        caller_path = self._committed_lineage_path(caller_id)
        delivery_origin = fact.metadata.get("delivery_engram_id")
        if (
            fact.kind is not CausalEventKind.SYSTEM
            or fact.flow is not CausalEventFlow.TUNNEL
            or fact.domain is not CausalEventDomain.SYSTEM
            or fact.source is not CausalEventSource.DELEGATION
            or fact.parent_event_id != request.id
            or fact.causal_id != request.causal_id
            or fact.engram_id not in caller_path
            or fact.center_id != request.center_id
            or fact.content != content
            or fact.metadata.get("has_result") is not False
            or fact.metadata.get("delegation_status") != request.status.value
            or fact.metadata.get("target_id") != request.engram_id
            or (
                "caller_id" in fact.metadata
                and fact.metadata.get("caller_id") != caller_id
            )
            or (
                "admitted_target_id" in fact.metadata
                and fact.metadata.get("admitted_target_id")
                != admitted_target_id
            )
            or (
                "execution_target_id" in fact.metadata
                and fact.metadata.get("execution_target_id")
                != request.engram_id
            )
            or (
                delivery_origin is not None
                and delivery_origin not in caller_path
            )
        ):
            raise DelegationConflictError(
                "stable terminal fact identity is bound to another event"
            )
        return fact

    @staticmethod
    def _derived_event_identity(
        prefix: str,
        request_event_id: str,
    ) -> tuple[str, str]:
        digest = hashlib.sha256(
            f"{prefix}\0{request_event_id}".encode("utf-8")
        ).hexdigest()
        return f"{prefix}-{digest}", f"{prefix}:{digest}"

    @staticmethod
    def _require_id(value: str, name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a non-empty string")
        return value.strip()

    @staticmethod
    def _require_text(value: str, name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must contain text")
        return value.strip()


__all__ = [
    "DELEGATION_OUTCOMES",
    "DURABLE_DELEGATION_MODE",
    "UNSUPPORTED_DELEGATION_MODES",
    "DelegationAdmission",
    "DelegationAuthorizationError",
    "DelegationConflictError",
    "DelegationIdentity",
    "DelegationInvariantError",
    "DelegationOutcomeUpdate",
    "DelegationReconciliation",
    "DelegationRejectedError",
    "DelegationRouterPort",
    "DelegationTunnelError",
    "DurableDelegationTunnel",
    "UnsupportedDelegationModeError",
]

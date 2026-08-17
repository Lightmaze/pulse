"""Deterministic, durable admission for ActivityCenter pulse work.

This module intentionally knows nothing about Pi, prompts, or Harness turns.
It chooses among candidates the caller has already established as dendrite-ready,
then commits every Center decision and held execution slot in one Storage
transaction before a caller can attempt execution.
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable

from pulse_system.core.types import (
    ActivityCenter,
    ActivityCenterStatus,
    CausalEventStatus,
    CenterLane,
    CenterReservation,
    CenterReservationOutcome,
    CenterReservationState,
    CenterScheduleDecision,
    CenterScheduleReason,
    CenterScheduleState,
    TaskRelationshipStatus,
    center_lane_for_activity_kind,
)
from pulse_system.substrate.storage import Storage


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _nonempty(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{field_name} must be a timezone-aware UTC datetime")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be a UTC datetime")
    return value.astimezone(timezone.utc)


def _finite(value: float, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite number")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field_name} must be a finite number")
    return normalized


def _nonnegative_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be an integer >= 0")
    return value


@dataclass(frozen=True)
class CenterSchedulingConfig:
    """The frozen v1 admission policy parameters."""

    lane_reservation_per_tick: int = 1
    starvation_boost: float = 0.05
    starvation_debt_cap: int = 20
    reservation_history_limit: int = 20

    def __post_init__(self) -> None:
        lane_reservation = self.lane_reservation_per_tick
        if (
            isinstance(lane_reservation, bool)
            or not isinstance(lane_reservation, int)
            or not 0 <= lane_reservation <= 32
        ):
            raise ValueError(
                "lane_reservation_per_tick must be an integer between 0 and 32"
            )
        boost = _finite(self.starvation_boost, "starvation_boost")
        if not 0.0 <= boost <= 1.0:
            raise ValueError("starvation_boost must be between 0 and 1")
        debt_cap = self.starvation_debt_cap
        if (
            isinstance(debt_cap, bool)
            or not isinstance(debt_cap, int)
            or not 1 <= debt_cap <= 10_000
        ):
            raise ValueError(
                "starvation_debt_cap must be an integer between 1 and 10000"
            )
        history_limit = self.reservation_history_limit
        if (
            isinstance(history_limit, bool)
            or not isinstance(history_limit, int)
            or not 1 <= history_limit <= 200
        ):
            raise ValueError(
                "reservation_history_limit must be an integer between 1 and 200"
            )
        object.__setattr__(self, "starvation_boost", boost)


@dataclass(frozen=True)
class CenterAdmissionCandidate:
    """The non-sensitive projection required to decide one ready event."""

    event_id: str
    engram_id: str
    center_id: str | None
    priority: float
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _nonempty(self.event_id, "event_id"))
        object.__setattr__(self, "engram_id", _nonempty(self.engram_id, "engram_id"))
        if self.center_id is not None:
            object.__setattr__(
                self,
                "center_id",
                _nonempty(self.center_id, "center_id"),
            )
        object.__setattr__(self, "priority", _finite(self.priority, "priority"))
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))


@dataclass(frozen=True)
class CenterAdmission:
    """One selected candidate and its durable execution slot."""

    candidate: CenterAdmissionCandidate
    reservation: CenterReservation

    def __post_init__(self) -> None:
        if not isinstance(self.candidate, CenterAdmissionCandidate):
            raise ValueError("candidate must be a CenterAdmissionCandidate")
        if not isinstance(self.reservation, CenterReservation):
            raise ValueError("reservation must be a CenterReservation")
        if self.reservation.event_id != self.candidate.event_id:
            raise ValueError("admission reservation must match candidate event_id")
        if self.reservation.engram_id != self.candidate.engram_id:
            raise ValueError("admission reservation must match candidate engram_id")
        if self.reservation.center_id != self.candidate.center_id:
            raise ValueError("admission reservation must match candidate center_id")
        if self.reservation.base_priority != self.candidate.priority:
            raise ValueError("admission reservation must preserve candidate priority")


@dataclass(frozen=True)
class CenterAdmissionPlan:
    """One fully validated, already-persisted scheduling decision batch."""

    tick: int
    budget: int
    eligible_count: int
    deferred_count: int
    admissions: tuple[CenterAdmission, ...]
    decisions: tuple[CenterScheduleState, ...]

    def __post_init__(self) -> None:
        _nonnegative_int(self.tick, "tick")
        _nonnegative_int(self.budget, "budget")
        _nonnegative_int(self.eligible_count, "eligible_count")
        _nonnegative_int(self.deferred_count, "deferred_count")
        admissions = tuple(self.admissions)
        decisions = tuple(self.decisions)
        if not all(isinstance(item, CenterAdmission) for item in admissions):
            raise ValueError("admissions must contain CenterAdmission values")
        if not all(isinstance(item, CenterScheduleState) for item in decisions):
            raise ValueError("decisions must contain CenterScheduleState values")
        if len(admissions) > self.budget:
            raise ValueError("admissions must not exceed budget")
        if self.eligible_count < len(admissions):
            raise ValueError("eligible_count must cover every admission")
        if self.deferred_count != self.eligible_count - len(admissions):
            raise ValueError("deferred_count must equal eligible minus admitted")
        if len({item.candidate.event_id for item in admissions}) != len(admissions):
            raise ValueError("admissions must not repeat an event")
        if len({item.candidate.engram_id for item in admissions}) != len(admissions):
            raise ValueError("admissions must not repeat an Engram")
        if len({item.reservation.id for item in admissions}) != len(admissions):
            raise ValueError("admissions must not repeat a reservation")
        if len({item.center_id for item in decisions}) != len(decisions):
            raise ValueError("decisions must contain at most one state per Center")
        object.__setattr__(self, "admissions", admissions)
        object.__setattr__(self, "decisions", decisions)

    @property
    def admitted_event_ids(self) -> tuple[str, ...]:
        return tuple(item.candidate.event_id for item in self.admissions)

    @property
    def reservations(self) -> tuple[CenterReservation, ...]:
        return tuple(item.reservation for item in self.admissions)


@dataclass(frozen=True)
class _ScoredCandidate:
    candidate: CenterAdmissionCandidate
    lane: CenterLane
    debt: int
    effective_score: float

    @property
    def sort_key(self) -> tuple[float, datetime, str]:
        return (-self.effective_score, self.candidate.created_at, self.candidate.event_id)


class DurableCenterScheduler:
    """Plan and durably reserve Center work owned by one Runtime epoch."""

    def __init__(
        self,
        storage: Storage,
        world_id: str,
        owner_id: str,
        lease_epoch: int,
        config: CenterSchedulingConfig | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if not isinstance(storage, Storage):
            raise ValueError("storage must be a Storage instance")
        self._storage = storage
        self._world_id = _nonempty(world_id, "world_id")
        self._owner_id = _nonempty(owner_id, "owner_id")
        if (
            isinstance(lease_epoch, bool)
            or not isinstance(lease_epoch, int)
            or lease_epoch < 1
        ):
            raise ValueError("lease_epoch must be an integer >= 1")
        if config is not None and not isinstance(config, CenterSchedulingConfig):
            raise ValueError("config must be a CenterSchedulingConfig or null")
        if not callable(clock):
            raise ValueError("clock must be callable")
        self._lease_epoch = lease_epoch
        self._config = config or CenterSchedulingConfig()
        self._clock = clock

    @property
    def config(self) -> CenterSchedulingConfig:
        return self._config

    def plan(
        self,
        candidates: Iterable[CenterAdmissionCandidate],
        *,
        budget: int,
        tick: int,
    ) -> CenterAdmissionPlan:
        """Choose and atomically reserve at most ``budget`` ready events."""

        _nonnegative_int(budget, "budget")
        _nonnegative_int(tick, "tick")
        candidate_rows = self._validate_candidates(candidates)
        now = _utc(self._clock(), "clock result")
        prior_states = {
            state.center_id: state
            for state in self._storage.list_center_schedule_states()
        }
        centers = self._load_relevant_centers(candidate_rows, prior_states)
        scored, active_ready, blocked_centers = self._score_candidates(
            candidate_rows,
            centers,
            prior_states,
        )
        deduplicated = self._deduplicate_engrams(scored)
        selected = self._select(deduplicated, budget)
        admissions = tuple(
            CenterAdmission(
                candidate=row.candidate,
                reservation=CenterReservation(
                    id=uuid.uuid4().hex,
                    world_id=self._world_id,
                    event_id=row.candidate.event_id,
                    engram_id=row.candidate.engram_id,
                    center_id=row.candidate.center_id,
                    lane=row.lane,
                    owner_id=self._owner_id,
                    lease_epoch=self._lease_epoch,
                    reason=reason,
                    base_priority=row.candidate.priority,
                    effective_score=row.effective_score,
                    created_at=now,
                ),
            )
            for row, reason in selected
        )
        decisions = self._decisions(
            centers=centers,
            prior_states=prior_states,
            active_ready=active_ready,
            blocked_centers=blocked_centers,
            admissions=admissions,
            now=now,
        )
        plan = CenterAdmissionPlan(
            tick=tick,
            budget=budget,
            eligible_count=len(scored),
            deferred_count=len(scored) - len(admissions),
            admissions=admissions,
            decisions=decisions,
        )
        committed = self._storage.commit_center_schedule(
            self._owner_id,
            self._lease_epoch,
            plan.decisions,
            plan.reservations,
            now=now,
        )
        if tuple(committed) != plan.reservations:
            raise RuntimeError("Storage returned a reservation set different from the plan")
        return plan

    def settle(
        self,
        reservation_id: str,
        outcome: CenterReservationOutcome | str,
    ) -> CenterReservation:
        """Settle one held reservation after the caller finishes its attempt."""

        return self._storage.settle_center_reservation(
            reservation_id,
            self._owner_id,
            self._lease_epoch,
            outcome,
            now=_utc(self._clock(), "clock result"),
        )

    def recover_old_reservations(self) -> list[CenterReservation]:
        """Abandon held slots from older owners without touching causal state."""

        return self._storage.recover_held_center_reservations(
            self._owner_id,
            self._lease_epoch,
            now=_utc(self._clock(), "clock result"),
        )

    def scheduling_snapshot(self, history_limit: int | None = None) -> dict:
        """Return canonical, non-content scheduling facts for an API adapter."""

        if history_limit is None:
            history_limit = self._config.reservation_history_limit
        if (
            isinstance(history_limit, bool)
            or not isinstance(history_limit, int)
            or not 1 <= history_limit <= 200
        ):
            raise ValueError("history_limit must be an integer between 1 and 200")
        states = {
            state.center_id: state
            for state in self._storage.list_center_schedule_states()
        }
        centers = self._storage.list_activity_centers()
        reservations = self._storage.list_center_reservations(
            world_id=self._world_id,
            limit=history_limit,
        )
        # Capacity is an exact current fact, not a count truncated by the
        # presentation history limit.
        held_count = len(self._storage.list_center_reservations(
            world_id=self._world_id,
            state=CenterReservationState.HELD,
        ))
        center_rows = tuple(
            self._snapshot_center(center, states.get(center.id))
            for center in sorted(centers, key=lambda item: item.id)
        )
        return {
            "policy_version": "durable-center-scheduling/v1",
            "owner": {
                "owner_id": self._owner_id,
                "lease_epoch": self._lease_epoch,
            },
            "capacity": {
                "lane_reservation_per_tick": self._config.lane_reservation_per_tick,
                "starvation_boost": self._config.starvation_boost,
                "starvation_debt_cap": self._config.starvation_debt_cap,
                "held": held_count,
            },
            "lanes": tuple(
                self._snapshot_lane(lane, states.values())
                for lane in (CenterLane.WORK, CenterLane.LIFE)
            ),
            "centers": center_rows,
            "reservations": tuple(
                {
                    "id": reservation.id,
                    "world_id": reservation.world_id,
                    "event_id": reservation.event_id,
                    "engram_id": reservation.engram_id,
                    "center_id": reservation.center_id,
                    "lane": reservation.lane.value,
                    "owner_id": reservation.owner_id,
                    "lease_epoch": reservation.lease_epoch,
                    "state": reservation.state.value,
                    "outcome": (
                        None
                        if reservation.outcome is None
                        else reservation.outcome.value
                    ),
                    "reason": reservation.reason.value,
                    "base_priority": reservation.base_priority,
                    "effective_score": reservation.effective_score,
                    "created_at": reservation.created_at.isoformat(),
                    "settled_at": (
                        None
                        if reservation.settled_at is None
                        else reservation.settled_at.isoformat()
                    ),
                }
                for reservation in reservations
            ),
        }

    def _validate_candidates(
        self,
        candidates: Iterable[CenterAdmissionCandidate],
    ) -> tuple[CenterAdmissionCandidate, ...]:
        if isinstance(candidates, (str, bytes)):
            raise ValueError("candidates must be an iterable of CenterAdmissionCandidate")
        try:
            rows = tuple(candidates)
        except TypeError as exc:
            raise ValueError(
                "candidates must be an iterable of CenterAdmissionCandidate"
            ) from exc
        if not all(isinstance(row, CenterAdmissionCandidate) for row in rows):
            raise ValueError("candidates must contain CenterAdmissionCandidate values")
        event_ids = [row.event_id for row in rows]
        if len(set(event_ids)) != len(event_ids):
            raise ValueError("candidates must not repeat an event_id")
        held_events = {
            reservation.event_id
            for reservation in self._storage.list_center_reservations(
                world_id=self._world_id,
                state=CenterReservationState.HELD,
            )
        }
        # Local import avoids a package-initialization cycle: causal Runtime
        # fences consume the lifecycle publication gate, while this diagnostic
        # path only needs the ledger when the snapshot is actually requested.
        from pulse_system.core.causality import CausalLedger

        ledger = CausalLedger(self._storage)
        for row in rows:
            if row.event_id in held_events:
                raise ValueError(
                    f"candidate event {row.event_id!r} already has a held reservation"
                )
            event = ledger.get_event(row.event_id)
            if event is None:
                raise KeyError(f"unknown causal event: {row.event_id}")
            if (
                event.world_id != self._world_id
                or event.engram_id != row.engram_id
                or event.center_id != row.center_id
            ):
                raise ValueError(
                    "candidate world, Engram, and Center must match its causal event"
                )
            if event.status is not CausalEventStatus.QUEUED:
                raise ValueError("only queued causal events may be scheduled")
        return rows

    def _load_relevant_centers(
        self,
        candidates: tuple[CenterAdmissionCandidate, ...],
        prior_states: dict[str, CenterScheduleState],
    ) -> dict[str, ActivityCenter]:
        ids = set(prior_states)
        ids.update(row.center_id for row in candidates if row.center_id is not None)
        centers: dict[str, ActivityCenter] = {}
        for center_id in ids:
            center = self._storage.get_activity_center(center_id)
            if center is None:
                raise KeyError(f"unknown ActivityCenter: {center_id}")
            centers[center_id] = center
        return centers

    def _score_candidates(
        self,
        candidates: tuple[CenterAdmissionCandidate, ...],
        centers: dict[str, ActivityCenter],
        prior_states: dict[str, CenterScheduleState],
    ) -> tuple[
        tuple[_ScoredCandidate, ...],
        dict[str, CenterScheduleReason],
        set[str],
    ]:
        scored: list[_ScoredCandidate] = []
        active_ready: set[str] = set()
        blocked_centers: dict[str, CenterScheduleReason] = {}
        for candidate in candidates:
            if candidate.center_id is None:
                scored.append(
                    _ScoredCandidate(
                        candidate=candidate,
                        lane=CenterLane.UNBOUND,
                        debt=0,
                        effective_score=candidate.priority,
                    )
                )
                continue
            center = centers[candidate.center_id]
            lane = center_lane_for_activity_kind(center.kind)
            relationship = self._storage.get_task_relationship_for_center(
                center.id,
                world_id=self._world_id,
            )
            if (
                relationship is not None
                and relationship.status is not TaskRelationshipStatus.ACTIVE
            ):
                blocked_centers[center.id] = CenterScheduleReason.CENTER_INACTIVE
                continue
            if center.status is not ActivityCenterStatus.ACTIVE:
                blocked_centers[center.id] = CenterScheduleReason.CENTER_INACTIVE
                continue
            active_ready.add(center.id)
            old_debt = prior_states.get(center.id, CenterScheduleState(
                center_id=center.id,
                lane=lane,
            )).starvation_debt
            scored.append(
                _ScoredCandidate(
                    candidate=candidate,
                    lane=lane,
                    debt=old_debt,
                    effective_score=(
                        candidate.priority
                        + min(old_debt, self._config.starvation_debt_cap)
                        * self._config.starvation_boost
                    ),
                )
            )
        return tuple(scored), active_ready, blocked_centers

    @staticmethod
    def _deduplicate_engrams(
        candidates: tuple[_ScoredCandidate, ...],
    ) -> tuple[_ScoredCandidate, ...]:
        chosen: list[_ScoredCandidate] = []
        seen_engrams: set[str] = set()
        for row in sorted(candidates, key=lambda item: item.sort_key):
            if row.candidate.engram_id in seen_engrams:
                continue
            seen_engrams.add(row.candidate.engram_id)
            chosen.append(row)
        return tuple(chosen)

    def _select(
        self,
        candidates: tuple[_ScoredCandidate, ...],
        budget: int,
    ) -> tuple[tuple[_ScoredCandidate, CenterScheduleReason], ...]:
        selected: list[tuple[_ScoredCandidate, CenterScheduleReason]] = []
        selected_events: set[str] = set()
        selected_centers: set[str] = set()

        def choose(
            row: _ScoredCandidate,
            reason: CenterScheduleReason,
        ) -> bool:
            if len(selected) >= budget or row.candidate.event_id in selected_events:
                return False
            selected.append((row, reason))
            selected_events.add(row.candidate.event_id)
            if row.candidate.center_id is not None:
                selected_centers.add(row.candidate.center_id)
            return True

        by_lane = {
            lane: tuple(
                row for row in sorted(candidates, key=lambda item: item.sort_key)
                if row.lane is lane
            )
            for lane in (CenterLane.WORK, CenterLane.LIFE)
        }
        if budget >= 2 and by_lane[CenterLane.WORK] and by_lane[CenterLane.LIFE]:
            quota = min(self._config.lane_reservation_per_tick, budget // 2)
            for index in range(quota):
                for lane in (CenterLane.WORK, CenterLane.LIFE):
                    if index < len(by_lane[lane]):
                        choose(
                            by_lane[lane][index],
                            CenterScheduleReason.LANE_RESERVATION,
                        )

        remaining = [
            row for row in candidates if row.candidate.event_id not in selected_events
        ]
        fair_share: list[_ScoredCandidate] = []
        for center_id in sorted({
            row.candidate.center_id
            for row in remaining
            if row.candidate.center_id is not None
            and row.candidate.center_id not in selected_centers
        }):
            center_rows = [
                row for row in remaining if row.candidate.center_id == center_id
            ]
            fair_share.append(min(center_rows, key=lambda item: item.sort_key))
        for row in sorted(fair_share, key=lambda item: item.sort_key):
            choose(row, CenterScheduleReason.FAIR_SHARE)

        for row in sorted(candidates, key=lambda item: item.sort_key):
            choose(row, CenterScheduleReason.EFFECTIVE_SCORE)
        return tuple(selected)

    def _decisions(
        self,
        *,
        centers: dict[str, ActivityCenter],
        prior_states: dict[str, CenterScheduleState],
        active_ready: set[str],
        blocked_centers: dict[str, CenterScheduleReason],
        admissions: tuple[CenterAdmission, ...],
        now: datetime,
    ) -> tuple[CenterScheduleState, ...]:
        admissions_by_center: dict[str, CenterAdmission] = {}
        for admission in admissions:
            center_id = admission.candidate.center_id
            if center_id is not None and center_id not in admissions_by_center:
                admissions_by_center[center_id] = admission
        decisions: list[CenterScheduleState] = []

        def append_if_changed(state: CenterScheduleState) -> None:
            prior = prior_states.get(state.center_id)
            if prior is not None and (
                prior.lane,
                prior.decision,
                prior.reason,
                prior.starvation_debt,
                prior.waiting_since,
                prior.last_admitted_at,
            ) == (
                state.lane,
                state.decision,
                state.reason,
                state.starvation_debt,
                state.waiting_since,
                state.last_admitted_at,
            ):
                return
            decisions.append(state)

        for center_id in sorted(centers):
            center = centers[center_id]
            lane = center_lane_for_activity_kind(center.kind)
            prior = prior_states.get(center_id)
            last_admitted_at = (
                None if prior is None else prior.last_admitted_at
            )
            if center_id in blocked_centers:
                append_if_changed(
                    CenterScheduleState(
                        center_id=center_id,
                        lane=lane,
                        decision=CenterScheduleDecision.BLOCKED,
                        reason=blocked_centers[center_id],
                        last_admitted_at=last_admitted_at,
                        last_decision_at=now,
                        updated_at=now,
                    )
                )
            elif center_id in admissions_by_center:
                # A centered reservation and its ADMITTED decision are one
                # atomic contract.  Even when a frozen/sub-microsecond clock
                # makes the state byte-for-byte identical to the prior row,
                # the current reservation still needs its matching decision
                # in this commit batch.
                decisions.append(
                    CenterScheduleState(
                        center_id=center_id,
                        lane=lane,
                        decision=CenterScheduleDecision.ADMITTED,
                        reason=admissions_by_center[center_id].reservation.reason,
                        last_admitted_at=now,
                        last_decision_at=now,
                        updated_at=now,
                    )
                )
            elif center_id in active_ready:
                waiting_since = (
                    prior.waiting_since
                    if prior is not None
                    and prior.decision is CenterScheduleDecision.WAITING
                    else now
                )
                debt = 1 if prior is None else prior.starvation_debt + 1
                append_if_changed(
                    CenterScheduleState(
                        center_id=center_id,
                        lane=lane,
                        decision=CenterScheduleDecision.WAITING,
                        reason=CenterScheduleReason.BUDGET_DEFERRED,
                        starvation_debt=debt,
                        waiting_since=waiting_since,
                        last_admitted_at=last_admitted_at,
                        last_decision_at=now,
                        updated_at=now,
                    )
                )
            else:
                append_if_changed(
                    CenterScheduleState(
                        center_id=center_id,
                        lane=lane,
                        decision=CenterScheduleDecision.IDLE,
                        reason=CenterScheduleReason.NO_READY_EVENT,
                        last_admitted_at=last_admitted_at,
                        last_decision_at=now,
                        updated_at=now,
                    )
                )
        return tuple(decisions)

    @staticmethod
    def _snapshot_center(
        center: ActivityCenter,
        state: CenterScheduleState | None,
    ) -> dict:
        lane = center_lane_for_activity_kind(center.kind)
        return {
            "center_id": center.id,
            "lane": lane.value,
            "status": center.status.value,
            "decision": (
                CenterScheduleDecision.IDLE.value
                if state is None
                else state.decision.value
            ),
            "reason": (
                CenterScheduleReason.NO_READY_EVENT.value
                if state is None
                else state.reason.value
            ),
            "starvation_debt": 0 if state is None else state.starvation_debt,
            "waiting_since": (
                None
                if state is None or state.waiting_since is None
                else state.waiting_since.isoformat()
            ),
            "last_admitted_at": (
                None
                if state is None or state.last_admitted_at is None
                else state.last_admitted_at.isoformat()
            ),
            "last_decision_at": (
                center.updated_at.isoformat()
                if state is None
                else state.last_decision_at.isoformat()
            ),
            "updated_at": (
                center.updated_at.isoformat()
                if state is None
                else state.updated_at.isoformat()
            ),
        }

    @staticmethod
    def _snapshot_lane(
        lane: CenterLane,
        states: Iterable[CenterScheduleState],
    ) -> dict:
        lane_states = [state for state in states if state.lane is lane]
        waiting = [
            state
            for state in lane_states
            if state.decision is CenterScheduleDecision.WAITING
        ]
        last_admitted = [
            state.last_admitted_at
            for state in lane_states
            if state.last_admitted_at is not None
        ]
        return {
            "lane": lane.value,
            "waiting_centers": len(waiting),
            "max_debt": max((state.starvation_debt for state in waiting), default=0),
            "last_admitted_at": (
                None if not last_admitted else max(last_admitted).isoformat()
            ),
        }

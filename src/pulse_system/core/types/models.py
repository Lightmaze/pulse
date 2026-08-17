"""Core data models for the Pulse system."""

from __future__ import annotations

import math
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from numbers import Real
from typing import Any


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _uuid() -> str:
    return uuid.uuid4().hex[:16]


class EngramStatus(str, Enum):
    ACTIVE = "active"
    PROVISIONAL = "provisional"
    ARCHIVED = "archived"


class MessageRole(str, Enum):
    USER = "user"
    ASSISTANT = "assistant"
    INJECTION = "injection"


class ConnectionType(str, Enum):
    EXCITATORY = "excitatory"
    INHIBITORY = "inhibitory"


class ActivityKind(str, Enum):
    TASK = "task"
    HOBBY = "hobby"
    LIFE_PROJECT = "life_project"
    RELATIONSHIP = "relationship"
    EXPLORATION = "exploration"
    PRACTICE = "practice"
    EXPRESSION = "expression"
    REST = "rest"
    OTHER = "other"


class ActivityCenterStatus(str, Enum):
    ACTIVE = "active"
    DORMANT = "dormant"
    PAUSED = "paused"
    COMPLETED = "completed"
    ARCHIVED = "archived"


class ActivityOrigin(str, Enum):
    USER = "user"
    SELF = "self"
    SHARED = "shared"
    SYSTEM = "system"


class MembershipRelation(str, Enum):
    FOCAL = "focal"
    PARTICIPANT = "participant"
    SHARED = "shared"


class TaskFrontStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    ARCHIVED = "archived"


class TaskOfferStatus(str, Enum):
    PENDING = "pending"
    CHANGES_REQUESTED = "changes_requested"
    ACCEPTED = "accepted"
    REFUSED = "refused"
    WITHDRAWN = "withdrawn"


class TaskOfferDecision(str, Enum):
    ACCEPT = "accept"
    REFUSE = "refuse"
    REQUEST_CHANGES = "request_changes"


class TaskRelationshipStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    RENEGOTIATION_REQUESTED = "renegotiation_requested"
    EXITED = "exited"


class TaskRelationshipAction(str, Enum):
    ACCEPTED = "accepted"
    PAUSED = "paused"
    RENEGOTIATION_REQUESTED = "renegotiation_requested"
    TERMS_PROPOSED = "terms_proposed"
    RESUMED = "resumed"
    EXITED = "exited"
    SUCCESSION = "succession"


class TaskRelationshipActorKind(str, Enum):
    SUBJECT = "subject"
    USER = "user"
    SYSTEM = "system"


class CausalEventFlow(str, Enum):
    """The three information flows visible in the organism model."""

    CONTENT = "content"
    SPECTRUM = "spectrum"
    TUNNEL = "tunnel"


class CausalEventDomain(str, Enum):
    PULSE = "pulse"
    HARNESS = "harness"
    WORLD = "world"
    HABITAT = "habitat"
    GENERATION = "generation"
    SYSTEM = "system"


class CausalEventKind(str, Enum):
    STIMULUS = "stimulus"
    SPONTANEOUS = "spontaneous"
    PULSE = "pulse"
    PROPAGATION = "propagation"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    HABITAT_OBSERVATION = "habitat_observation"
    HABITAT_ACTION = "habitat_action"
    HABITAT_CONSEQUENCE = "habitat_consequence"
    DELEGATION_REQUEST = "delegation_request"
    DELEGATION_RESULT = "delegation_result"
    GENERATION_TRANSITION = "generation_transition"
    ASSISTANT_RESULT = "assistant_result"
    SYSTEM = "system"


class CausalEventSource(str, Enum):
    USER = "user"
    SELF = "self"
    HABITAT = "habitat"
    SENSORY = "sensory"
    PROPAGATION = "propagation"
    DELEGATION = "delegation"
    SYSTEM = "system"


class CausalEventStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SETTLED = "settled"
    FAILED = "failed"
    UNCERTAIN = "uncertain"
    RECONCILED = "reconciled"
    CANCELLED = "cancelled"


class CausalEventResolution(str, Enum):
    """The only durable outcomes for reconciling an uncertain event."""

    ACKNOWLEDGED = "acknowledged"
    CANCELLED = "cancelled"
    SUPERSEDED = "superseded"


class HarnessTurnState(str, Enum):
    RUNNING = "running"
    SETTLED = "settled"
    FAILED = "failed"
    UNCERTAIN = "uncertain"


class GenerationTransitionState(str, Enum):
    PREPARED = "prepared"
    SUMMARIZING = "summarizing"
    ROTATING = "rotating"
    COMMITTED = "committed"
    FAILED = "failed"
    UNCERTAIN = "uncertain"


class HabitatSubscriptionStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"


class LivingConcernDisposition(str, Enum):
    """How a subject chooses to carry one concern forward."""

    REVISIT = "revisit"
    QUIET = "quiet"
    RESOLVED = "resolved"


class LivingOrientationState(str, Enum):
    """How a subject chooses to keep one living direction available."""

    OPEN = "open"
    RESTING = "resting"
    CLOSED = "closed"


class RuntimeLeaseState(str, Enum):
    ACTIVE = "active"
    RELEASED = "released"


class CenterLane(str, Enum):
    WORK = "work"
    LIFE = "life"
    UNBOUND = "unbound"


class CenterScheduleDecision(str, Enum):
    IDLE = "idle"
    ADMITTED = "admitted"
    WAITING = "waiting"
    BLOCKED = "blocked"


class CenterScheduleReason(str, Enum):
    LANE_RESERVATION = "lane_reservation"
    FAIR_SHARE = "fair_share"
    EFFECTIVE_SCORE = "effective_score"
    BUDGET_DEFERRED = "budget_deferred"
    CENTER_INACTIVE = "center_inactive"
    NO_READY_EVENT = "no_ready_event"


class CenterReservationState(str, Enum):
    HELD = "held"
    SETTLED = "settled"
    ABANDONED = "abandoned"


class CenterReservationOutcome(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    UNCERTAIN = "uncertain"
    OWNER_REPLACED = "owner_replaced"


# Readable aliases used by callers that prefer the shorter domain vocabulary.
CausalFlow = CausalEventFlow
CausalDomain = CausalEventDomain
CausalKind = CausalEventKind
CausalSource = CausalEventSource
CausalStatus = CausalEventStatus
CausalResolution = CausalEventResolution
GenerationState = GenerationTransitionState


def _enum_value(value: Any, enum_type: type[Enum], field_name: str) -> Enum:
    if isinstance(value, enum_type):
        return value
    if isinstance(value, str):
        try:
            return enum_type(value)
        except ValueError:
            pass
    allowed = ", ".join(member.value for member in enum_type)
    raise ValueError(f"{field_name} must be one of: {allowed}")


def _nonempty_id(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _optional_id(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _nonempty_id(value, field_name)


def _title(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("title must be a string")
    normalized = value.strip()
    if not 1 <= len(normalized) <= 120:
        raise ValueError("title must contain 1..120 characters after trimming")
    return normalized


def _autonomy(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError("autonomy must be a finite number in [0, 1]")
    normalized = float(value)
    if not math.isfinite(normalized) or not 0.0 <= normalized <= 1.0:
        raise ValueError("autonomy must be a finite number in [0, 1]")
    return normalized


def _utc(value: Any, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _finite_score(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{field_name} must be a finite number")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field_name} must be a finite number")
    return normalized


def center_lane_for_activity_kind(
    value: ActivityKind | str,
) -> CenterLane:
    """Map the frozen ActivityCenter kind partition onto scheduler lanes."""

    kind = _enum_value(value, ActivityKind, "kind")
    return CenterLane.WORK if kind is ActivityKind.TASK else CenterLane.LIFE


_FORBIDDEN_METADATA_KEYS = {
    "prompt",
    "prompt_text",
    "output",
    "output_text",
    "tool_payload",
    "tool_arguments",
    "arguments",
    "payload",
    "capability",
    "gateway_url",
    "session_file",
    "pi_trace",
    "secret",
    "token",
    "api_key",
}


def _metadata(value: Any) -> dict[str, Any]:
    """Validate and copy the safe causal metadata envelope.

    Metadata is deliberately narrower than event content.  It is suitable for
    IDs, enums, booleans, counts, and tool names, but never for prompts,
    outputs, credentials, or arbitrary tool payloads.
    """

    if not isinstance(value, dict):
        raise ValueError("metadata must be a JSON object")

    def visit(item: Any, path: str) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str) or not key:
                    raise ValueError("metadata object keys must be non-empty strings")
                normalized = key.casefold().replace("-", "_")
                if normalized in _FORBIDDEN_METADATA_KEYS:
                    raise ValueError(f"metadata key {key!r} is not allowed")
                if "token" in normalized:
                    raise ValueError(f"metadata key {key!r} is not allowed")
                if normalized.endswith("_payload") or normalized.endswith("_secret"):
                    raise ValueError(f"metadata key {key!r} is not allowed")
                visit(child, f"{path}.{key}")
            return
        if isinstance(item, list):
            for index, child in enumerate(item):
                visit(child, f"{path}[{index}]")
            return
        if item is None or isinstance(item, (str, bool, int)):
            return
        if isinstance(item, float) and math.isfinite(item):
            return
        raise ValueError(f"metadata value at {path} is not JSON-safe")

    visit(value, "metadata")
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise ValueError("metadata must be JSON-serializable") from exc


@dataclass(frozen=True)
class RuntimeLease:
    """The database-scoped fencing lease for one Runtime owner."""

    scope: str
    owner_id: str
    epoch: int
    state: RuntimeLeaseState
    acquired_at: datetime
    renewed_at: datetime
    expires_at: datetime
    released_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.scope != "pulse_world":
            raise ValueError("scope must be 'pulse_world'")
        object.__setattr__(self, "owner_id", _nonempty_id(self.owner_id, "owner_id"))
        if isinstance(self.epoch, bool) or not isinstance(self.epoch, int):
            raise ValueError("epoch must be an integer >= 1")
        if self.epoch < 1:
            raise ValueError("epoch must be an integer >= 1")
        object.__setattr__(
            self,
            "state",
            _enum_value(self.state, RuntimeLeaseState, "state"),
        )
        acquired_at = _utc(self.acquired_at, "acquired_at")
        renewed_at = _utc(self.renewed_at, "renewed_at")
        expires_at = _utc(self.expires_at, "expires_at")
        if renewed_at < acquired_at:
            raise ValueError("renewed_at must not precede acquired_at")
        if expires_at <= renewed_at:
            raise ValueError("expires_at must be after renewed_at")
        object.__setattr__(self, "acquired_at", acquired_at)
        object.__setattr__(self, "renewed_at", renewed_at)
        object.__setattr__(self, "expires_at", expires_at)
        released_at = (
            None
            if self.released_at is None
            else _utc(self.released_at, "released_at")
        )
        if released_at is not None and released_at < acquired_at:
            raise ValueError("released_at must not precede acquired_at")
        if self.state is RuntimeLeaseState.ACTIVE and released_at is not None:
            raise ValueError("active leases must not have released_at")
        if self.state is RuntimeLeaseState.RELEASED and released_at is None:
            raise ValueError("released leases require released_at")
        object.__setattr__(self, "released_at", released_at)


class RuntimeLeaseError(RuntimeError):
    """Base error carrying the lease snapshot that fenced an operation."""

    def __init__(
        self,
        *,
        owner_id: str,
        epoch: int | None,
        reason: str,
        lease: RuntimeLease | None,
    ) -> None:
        self.owner_id = owner_id
        self.epoch = epoch
        self.reason = reason
        self.lease = lease
        detail = "no current lease" if lease is None else (
            f"owner={lease.owner_id!r}, epoch={lease.epoch}, "
            f"state={lease.state.value!r}"
        )
        super().__init__(f"runtime lease {reason}: {detail}")


class RuntimeLeaseConflictError(RuntimeLeaseError):
    """A different active, unexpired owner already holds the lease."""


class RuntimeLeaseLostError(RuntimeLeaseError):
    """The caller no longer owns the active fencing epoch."""


@dataclass(frozen=True)
class CenterScheduleState:
    """The durable admission outcome and starvation fact for one Center."""

    center_id: str
    lane: CenterLane
    decision: CenterScheduleDecision = CenterScheduleDecision.IDLE
    reason: CenterScheduleReason = CenterScheduleReason.NO_READY_EVENT
    starvation_debt: int = 0
    waiting_since: datetime | None = None
    last_admitted_at: datetime | None = None
    last_decision_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)

    def __post_init__(self) -> None:
        object.__setattr__(self, "center_id", _nonempty_id(self.center_id, "center_id"))
        lane = _enum_value(self.lane, CenterLane, "lane")
        if lane is CenterLane.UNBOUND:
            raise ValueError("CenterScheduleState lane must be work or life")
        object.__setattr__(self, "lane", lane)
        decision = _enum_value(
            self.decision, CenterScheduleDecision, "decision"
        )
        object.__setattr__(self, "decision", decision)
        object.__setattr__(
            self,
            "reason",
            _enum_value(self.reason, CenterScheduleReason, "reason"),
        )
        reason = self.reason
        if (
            isinstance(self.starvation_debt, bool)
            or not isinstance(self.starvation_debt, int)
            or self.starvation_debt < 0
        ):
            raise ValueError("starvation_debt must be an integer >= 0")
        waiting_since = (
            None
            if self.waiting_since is None
            else _utc(self.waiting_since, "waiting_since")
        )
        last_admitted_at = (
            None
            if self.last_admitted_at is None
            else _utc(self.last_admitted_at, "last_admitted_at")
        )
        last_decision_at = _utc(self.last_decision_at, "last_decision_at")
        updated_at = _utc(self.updated_at, "updated_at")
        if updated_at < last_decision_at:
            raise ValueError("updated_at must not precede last_decision_at")
        if decision is CenterScheduleDecision.WAITING:
            if self.starvation_debt < 1 or waiting_since is None:
                raise ValueError(
                    "waiting decisions require debt >= 1 and waiting_since"
                )
        elif decision is CenterScheduleDecision.ADMITTED:
            if self.starvation_debt != 0 or waiting_since is not None:
                raise ValueError(
                    "admitted decisions require zero debt and no waiting_since"
                )
            if last_admitted_at is None:
                raise ValueError("admitted decisions require last_admitted_at")
        elif self.starvation_debt != 0 or waiting_since is not None:
            raise ValueError(
                "idle and blocked decisions require zero debt and no waiting_since"
            )
        allowed_reasons = {
            CenterScheduleDecision.IDLE: {CenterScheduleReason.NO_READY_EVENT},
            CenterScheduleDecision.ADMITTED: {
                CenterScheduleReason.LANE_RESERVATION,
                CenterScheduleReason.FAIR_SHARE,
                CenterScheduleReason.EFFECTIVE_SCORE,
            },
            CenterScheduleDecision.WAITING: {
                CenterScheduleReason.BUDGET_DEFERRED,
            },
            CenterScheduleDecision.BLOCKED: {
                CenterScheduleReason.CENTER_INACTIVE,
            },
        }
        if reason not in allowed_reasons[decision]:
            raise ValueError(
                f"reason {reason.value!r} is invalid for decision "
                f"{decision.value!r}"
            )
        object.__setattr__(self, "waiting_since", waiting_since)
        object.__setattr__(self, "last_admitted_at", last_admitted_at)
        object.__setattr__(self, "last_decision_at", last_decision_at)
        object.__setattr__(self, "updated_at", updated_at)


@dataclass(frozen=True)
class CenterReservation:
    """A durable execution-slot reservation created before a Harness call."""

    world_id: str
    event_id: str
    engram_id: str
    center_id: str | None
    lane: CenterLane
    owner_id: str
    lease_epoch: int
    reason: CenterScheduleReason
    base_priority: float
    effective_score: float
    id: str = field(default_factory=_uuid)
    state: CenterReservationState = CenterReservationState.HELD
    outcome: CenterReservationOutcome | None = None
    created_at: datetime = field(default_factory=_now)
    settled_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "world_id", _nonempty_id(self.world_id, "world_id"))
        object.__setattr__(self, "event_id", _nonempty_id(self.event_id, "event_id"))
        object.__setattr__(self, "engram_id", _nonempty_id(self.engram_id, "engram_id"))
        object.__setattr__(self, "center_id", _optional_id(self.center_id, "center_id"))
        object.__setattr__(self, "id", _nonempty_id(self.id, "id"))
        lane = _enum_value(self.lane, CenterLane, "lane")
        if self.center_id is None and lane is not CenterLane.UNBOUND:
            raise ValueError("unbound reservations require lane='unbound'")
        if self.center_id is not None and lane is CenterLane.UNBOUND:
            raise ValueError("center reservations require a work or life lane")
        object.__setattr__(self, "lane", lane)
        object.__setattr__(self, "owner_id", _nonempty_id(self.owner_id, "owner_id"))
        if isinstance(self.lease_epoch, bool) or not isinstance(self.lease_epoch, int):
            raise ValueError("lease_epoch must be an integer >= 1")
        if self.lease_epoch < 1:
            raise ValueError("lease_epoch must be an integer >= 1")
        object.__setattr__(
            self,
            "reason",
            _enum_value(self.reason, CenterScheduleReason, "reason"),
        )
        if self.reason not in {
            CenterScheduleReason.LANE_RESERVATION,
            CenterScheduleReason.FAIR_SHARE,
            CenterScheduleReason.EFFECTIVE_SCORE,
        }:
            raise ValueError("reservation reason must describe an admission")
        object.__setattr__(
            self,
            "base_priority",
            _finite_score(self.base_priority, "base_priority"),
        )
        object.__setattr__(
            self,
            "effective_score",
            _finite_score(self.effective_score, "effective_score"),
        )
        state = _enum_value(self.state, CenterReservationState, "state")
        object.__setattr__(self, "state", state)
        outcome = (
            None
            if self.outcome is None
            else _enum_value(
                self.outcome,
                CenterReservationOutcome,
                "outcome",
            )
        )
        object.__setattr__(self, "outcome", outcome)
        created_at = _utc(self.created_at, "created_at")
        settled_at = (
            None
            if self.settled_at is None
            else _utc(self.settled_at, "settled_at")
        )
        if settled_at is not None and settled_at < created_at:
            raise ValueError("settled_at must not precede created_at")
        if state is CenterReservationState.HELD:
            if outcome is not None or settled_at is not None:
                raise ValueError("held reservations forbid outcome and settled_at")
        elif state is CenterReservationState.SETTLED:
            if outcome not in {
                CenterReservationOutcome.SUCCEEDED,
                CenterReservationOutcome.FAILED,
                CenterReservationOutcome.SKIPPED,
                CenterReservationOutcome.UNCERTAIN,
            } or settled_at is None:
                raise ValueError(
                    "settled reservations require a settled outcome and settled_at"
                )
        elif outcome not in {
            CenterReservationOutcome.UNCERTAIN,
            CenterReservationOutcome.OWNER_REPLACED,
        } or settled_at is None:
            raise ValueError(
                "abandoned reservations require uncertain or owner_replaced outcome"
            )
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "settled_at", settled_at)


@dataclass
class Message:
    role: MessageRole
    content: str
    timestamp: datetime = field(default_factory=_now)
    source_engram_id: str | None = None


@dataclass
class CausalEvent:
    world_id: str
    flow: CausalEventFlow | None = None
    domain: CausalEventDomain = CausalEventDomain.SYSTEM
    kind: CausalEventKind = CausalEventKind.SYSTEM
    source: CausalEventSource = CausalEventSource.SYSTEM
    id: str = field(default_factory=_uuid)
    causal_id: str | None = None
    parent_event_id: str | None = None
    engram_id: str | None = None
    center_id: str | None = None
    status: CausalEventStatus = CausalEventStatus.QUEUED
    content: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    idempotency_key: str | None = None
    attempts: int = 0
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)
    started_at: datetime | None = None
    settled_at: datetime | None = None
    resolution: CausalEventResolution | None = None
    resolution_note: str | None = None

    def __post_init__(self) -> None:
        self.id = _nonempty_id(self.id, "id")
        self.world_id = _nonempty_id(self.world_id, "world_id")
        self.causal_id = self.causal_id or self.id
        self.causal_id = _nonempty_id(self.causal_id, "causal_id")
        self.parent_event_id = _optional_id(
            self.parent_event_id, "parent_event_id"
        )
        self.engram_id = _optional_id(self.engram_id, "engram_id")
        self.center_id = _optional_id(self.center_id, "center_id")
        self.flow = (
            None
            if self.flow is None
            else _enum_value(self.flow, CausalEventFlow, "flow")
        )
        self.domain = _enum_value(  # type: ignore[assignment]
            self.domain, CausalEventDomain, "domain"
        )
        self.kind = _enum_value(  # type: ignore[assignment]
            self.kind, CausalEventKind, "kind"
        )
        self.source = _enum_value(  # type: ignore[assignment]
            self.source, CausalEventSource, "source"
        )
        self.status = _enum_value(  # type: ignore[assignment]
            self.status, CausalEventStatus, "status"
        )
        if self.content is not None and not isinstance(self.content, str):
            raise ValueError("content must be a string or null")
        self.metadata = _metadata(self.metadata)
        if self.resolution is not None:
            self.resolution = _enum_value(  # type: ignore[assignment]
                self.resolution, CausalEventResolution, "resolution"
            )
            if self.status is not CausalEventStatus.RECONCILED:
                raise ValueError("resolution is only valid on reconciled events")
        if self.resolution_note is not None:
            if not isinstance(self.resolution_note, str):
                raise ValueError("resolution_note must be a string or null")
            if len(self.resolution_note) > 2048:
                raise ValueError("resolution_note must contain at most 2048 characters")
            if self.status is not CausalEventStatus.RECONCILED:
                raise ValueError("resolution_note is only valid on reconciled events")
        self.idempotency_key = _optional_id(
            self.idempotency_key, "idempotency_key"
        )
        if isinstance(self.attempts, bool) or not isinstance(self.attempts, int):
            raise ValueError("attempts must be a non-negative integer")
        if self.attempts < 0:
            raise ValueError("attempts must be a non-negative integer")
        if self.seq is not None:
            if isinstance(self.seq, bool) or not isinstance(self.seq, int):
                raise ValueError("seq must be a non-negative integer or null")
            if self.seq < 0:
                raise ValueError("seq must be a non-negative integer or null")
        self.created_at = _utc(self.created_at, "created_at")
        self.updated_at = _utc(self.updated_at, "updated_at")
        if self.started_at is not None:
            self.started_at = _utc(self.started_at, "started_at")
        if self.settled_at is not None:
            self.settled_at = _utc(self.settled_at, "settled_at")

    # SQLite supplies the monotonic observation sequence after insertion.
    seq: int | None = None


@dataclass
class HarnessTurn:
    event_id: str
    engram_id: str
    id: str = field(default_factory=_uuid)
    state: HarnessTurnState = HarnessTurnState.RUNNING
    cursor_before: int = 0
    cursor_after: int = 0
    input_message_id: int | None = None
    prompt_accepted: bool | None = None
    session_id: str | None = None
    session_file: str | None = None
    result_event_id: str | None = None
    error_code: str | None = None
    error_phase: str | None = None
    started_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)
    settled_at: datetime | None = None

    def __post_init__(self) -> None:
        self.id = _nonempty_id(self.id, "id")
        self.event_id = _nonempty_id(self.event_id, "event_id")
        self.engram_id = _nonempty_id(self.engram_id, "engram_id")
        self.state = _enum_value(  # type: ignore[assignment]
            self.state, HarnessTurnState, "state"
        )
        for name in ("cursor_before", "cursor_after"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.input_message_id is not None:
            if (
                isinstance(self.input_message_id, bool)
                or not isinstance(self.input_message_id, int)
                or self.input_message_id < 1
            ):
                raise ValueError("input_message_id must be a positive integer or null")
        if self.prompt_accepted is not None and not isinstance(
            self.prompt_accepted, bool
        ):
            raise ValueError("prompt_accepted must be true, false, or null")
        self.session_id = _optional_id(self.session_id, "session_id")
        self.session_file = _optional_id(self.session_file, "session_file")
        self.result_event_id = _optional_id(self.result_event_id, "result_event_id")
        self.started_at = _utc(self.started_at, "started_at")
        self.updated_at = _utc(self.updated_at, "updated_at")
        if self.settled_at is not None:
            self.settled_at = _utc(self.settled_at, "settled_at")

    @property
    def turn_id(self) -> str:
        return self.id


@dataclass
class GenerationTransition:
    predecessor_id: str
    id: str = field(default_factory=_uuid)
    causal_id: str | None = None
    successor_id: str | None = None
    state: GenerationTransitionState = GenerationTransitionState.PREPARED
    summary_turn_id: str | None = None
    error_code: str | None = None
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)
    settled_at: datetime | None = None
    event_id: str | None = None

    def __post_init__(self) -> None:
        self.id = _nonempty_id(self.id, "id")
        self.causal_id = self.causal_id or self.id
        self.causal_id = _nonempty_id(self.causal_id, "causal_id")
        self.event_id = _optional_id(self.event_id, "event_id")
        self.predecessor_id = _nonempty_id(self.predecessor_id, "predecessor_id")
        self.successor_id = _optional_id(self.successor_id, "successor_id")
        self.state = _enum_value(  # type: ignore[assignment]
            self.state, GenerationTransitionState, "state"
        )
        self.summary_turn_id = _optional_id(
            self.summary_turn_id, "summary_turn_id"
        )
        self.created_at = _utc(self.created_at, "created_at")
        self.updated_at = _utc(self.updated_at, "updated_at")
        if self.settled_at is not None:
            self.settled_at = _utc(self.settled_at, "settled_at")

    @property
    def generation_id(self) -> str:
        return self.id


@dataclass(frozen=True)
class RecoveryReport:
    turn_ids: tuple[str, ...] = ()
    event_ids: tuple[str, ...] = ()
    generation_ids: tuple[str, ...] = ()
    effect_ids: tuple[str, ...] = ()

    @property
    def recovered_turns(self) -> int:
        return len(self.turn_ids)

    @property
    def recovered_events(self) -> int:
        return len(self.event_ids)

    @property
    def recovered_generations(self) -> int:
        return len(self.generation_ids)

    @property
    def recovered_effects(self) -> int:
        return len(self.effect_ids)


@dataclass
class LivingConcern:
    center_id: str
    owner_engram_id: str
    content: str
    causal_id: str
    source_event_id: str
    id: str = field(default_factory=_uuid)
    disposition: LivingConcernDisposition = LivingConcernDisposition.QUIET
    revisit_at: datetime | None = None
    revision: int = 1
    last_reentry_event_id: str | None = None
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)
    resolved_at: datetime | None = None

    def __post_init__(self) -> None:
        self.id = _nonempty_id(self.id, "id")
        self.center_id = _nonempty_id(self.center_id, "center_id")
        self.owner_engram_id = _nonempty_id(
            self.owner_engram_id, "owner_engram_id"
        )
        if not isinstance(self.content, str):
            raise ValueError("content must be a string")
        if not self.content.strip() or len(self.content) > 4000:
            raise ValueError(
                "content must contain 1..4000 characters and not be blank"
            )
        self.disposition = _enum_value(  # type: ignore[assignment]
            self.disposition,
            LivingConcernDisposition,
            "disposition",
        )
        self.causal_id = _nonempty_id(self.causal_id, "causal_id")
        self.source_event_id = _nonempty_id(
            self.source_event_id, "source_event_id"
        )
        self.last_reentry_event_id = _optional_id(
            self.last_reentry_event_id, "last_reentry_event_id"
        )
        if (
            isinstance(self.revision, bool)
            or not isinstance(self.revision, int)
            or self.revision < 1
        ):
            raise ValueError("revision must be a positive integer")
        self.created_at = _utc(self.created_at, "created_at")
        self.updated_at = _utc(self.updated_at, "updated_at")
        if self.revisit_at is not None:
            self.revisit_at = _utc(self.revisit_at, "revisit_at")
        if self.resolved_at is not None:
            self.resolved_at = _utc(self.resolved_at, "resolved_at")
        if self.disposition is LivingConcernDisposition.REVISIT:
            if self.revisit_at is None or self.resolved_at is not None:
                raise ValueError(
                    "revisit concerns require revisit_at and forbid resolved_at"
                )
        elif self.disposition is LivingConcernDisposition.QUIET:
            if self.revisit_at is not None or self.resolved_at is not None:
                raise ValueError(
                    "quiet concerns forbid revisit_at and resolved_at"
                )
        elif self.revisit_at is not None or self.resolved_at is None:
            raise ValueError(
                "resolved concerns require resolved_at and forbid revisit_at"
            )


@dataclass
class LivingOrientation:
    """A subject-authored, durable direction for one non-task Center.

    The orientation is deliberately separate from ``LivingConcern``.  A
    concern is a one-shot or quiet piece of carried content; an orientation is
    the current direction that the organism may return to through its own
    rhythm.  Runtime selection must consume only the state and accounting
    fields below, never interpret ``content``.
    """

    center_id: str
    owner_engram_id: str
    content: str
    causal_id: str
    source_event_id: str
    id: str = field(default_factory=_uuid)
    state: LivingOrientationState = LivingOrientationState.OPEN
    revision: int = 1
    engagement_count: int = 0
    next_eligible_at: datetime | None = None
    last_engagement_event_id: str | None = None
    last_engaged_at: datetime | None = None
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)
    closed_at: datetime | None = None

    def __post_init__(self) -> None:
        self.id = _nonempty_id(self.id, "id")
        self.center_id = _nonempty_id(self.center_id, "center_id")
        self.owner_engram_id = _nonempty_id(
            self.owner_engram_id, "owner_engram_id"
        )
        if not isinstance(self.content, str):
            raise ValueError("content must be a string")
        if not self.content.strip() or len(self.content) > 4000:
            raise ValueError(
                "content must contain 1..4000 characters and not be blank"
            )
        self.state = _enum_value(  # type: ignore[assignment]
            self.state,
            LivingOrientationState,
            "state",
        )
        self.causal_id = _nonempty_id(self.causal_id, "causal_id")
        self.source_event_id = _nonempty_id(
            self.source_event_id, "source_event_id"
        )
        if (
            isinstance(self.revision, bool)
            or not isinstance(self.revision, int)
            or self.revision < 1
        ):
            raise ValueError("revision must be a positive integer")
        if (
            isinstance(self.engagement_count, bool)
            or not isinstance(self.engagement_count, int)
            or self.engagement_count < 0
        ):
            raise ValueError("engagement_count must be a non-negative integer")
        self.last_engagement_event_id = _optional_id(
            self.last_engagement_event_id,
            "last_engagement_event_id",
        )
        self.created_at = _utc(self.created_at, "created_at")
        self.updated_at = _utc(self.updated_at, "updated_at")
        if self.next_eligible_at is not None:
            self.next_eligible_at = _utc(
                self.next_eligible_at,
                "next_eligible_at",
            )
        if self.last_engaged_at is not None:
            self.last_engaged_at = _utc(
                self.last_engaged_at,
                "last_engaged_at",
            )
        if self.closed_at is not None:
            self.closed_at = _utc(self.closed_at, "closed_at")

        if self.engagement_count == 0:
            if (
                self.last_engagement_event_id is not None
                or self.last_engaged_at is not None
            ):
                raise ValueError(
                    "zero-engagement orientations require empty last engagement fields"
                )
        elif (
            self.last_engagement_event_id is None
            or self.last_engaged_at is None
        ):
            raise ValueError(
                "engaged orientations require last event and timestamp"
            )

        if self.state is LivingOrientationState.OPEN:
            if self.closed_at is not None:
                raise ValueError("open orientations forbid closed_at")
        elif self.state is LivingOrientationState.RESTING:
            if self.next_eligible_at is not None or self.closed_at is not None:
                raise ValueError(
                    "resting orientations forbid next_eligible_at and closed_at"
                )
        elif (
            self.next_eligible_at is not None or self.closed_at is None
        ):
            raise ValueError(
                "closed orientations require closed_at and forbid next_eligible_at"
            )


@dataclass
class HabitatSubscription:
    world_id: str
    engram_id: str
    channel: str = "all"
    center_id: str | None = None
    id: str = field(default_factory=_uuid)
    status: HabitatSubscriptionStatus = HabitatSubscriptionStatus.ACTIVE
    last_fingerprint: str | None = None
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)

    def __post_init__(self) -> None:
        self.id = _nonempty_id(self.id, "id")
        self.world_id = _nonempty_id(self.world_id, "world_id")
        self.engram_id = _nonempty_id(self.engram_id, "engram_id")
        self.center_id = _optional_id(self.center_id, "center_id")
        if not isinstance(self.channel, str):
            raise ValueError("channel must be a string")
        self.channel = self.channel.strip() or "all"
        if len(self.channel) > 200:
            raise ValueError("channel must contain at most 200 characters")
        self.status = _enum_value(
            self.status, HabitatSubscriptionStatus, "status"
        )  # type: ignore[assignment]
        if self.last_fingerprint is not None:
            if not isinstance(self.last_fingerprint, str):
                raise ValueError("last_fingerprint must be a string or null")
            if len(self.last_fingerprint) > 512:
                raise ValueError(
                    "last_fingerprint must contain at most 512 characters"
                )
        self.created_at = _utc(self.created_at, "created_at")
        self.updated_at = _utc(self.updated_at, "updated_at")


@dataclass
class EngramMetadata:
    recent_activity: float = 0.0
    self_excitability: float = 0.1
    token_count: int = 0


@dataclass
class Engram:
    id: str = field(default_factory=_uuid)
    project_id: str | None = None
    status: EngramStatus = EngramStatus.ACTIVE
    created_at: datetime = field(default_factory=_now)
    last_pulse_at: datetime | None = None
    total_pulses: int = 0
    metadata: EngramMetadata = field(default_factory=EngramMetadata)
    # substrate registry substrate binding name (SubstrateRegistry); None = default
    substrate_binding: str | None = None
    # Public identity is layered: id/signature joins machines, name is a
    # session-style title, nickname is user-authored only.
    name: str | None = None
    name_origin: str = "auto"
    nickname: str | None = None


@dataclass
class Connection:
    from_id: str
    to_id: str
    weight: float
    conn_type: ConnectionType = ConnectionType.EXCITATORY
    created_at: datetime = field(default_factory=_now)
    last_activated_at: datetime = field(default_factory=_now)
    # The effective weight above is what the pulse engine consumes. The two
    # fields below make its provenance explicit: factory is the reset point;
    # learned is the field override (None means the factory value is active).
    factory_weight: float = 0.0
    learned_weight: float | None = None


@dataclass
class Project:
    id: str = field(default_factory=_uuid)
    name: str = ""
    description: str = ""
    workspace_path: str | None = None
    created_at: datetime = field(default_factory=_now)
    index_engram_id: str | None = None


@dataclass
class ActivityCenter:
    kind: ActivityKind
    title: str
    id: str = field(default_factory=_uuid)
    description: str = ""
    status: ActivityCenterStatus = ActivityCenterStatus.ACTIVE
    origin: ActivityOrigin = ActivityOrigin.USER
    autonomy: float = 1.0
    project_id: str | None = None
    focal_engram_id: str | None = None
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)
    last_active_at: datetime | None = None

    def __post_init__(self) -> None:
        self.id = _nonempty_id(self.id, "id")
        self.kind = _enum_value(self.kind, ActivityKind, "kind")  # type: ignore[assignment]
        self.title = _title(self.title)
        if not isinstance(self.description, str):
            raise ValueError("description must be a string")
        self.status = _enum_value(  # type: ignore[assignment]
            self.status, ActivityCenterStatus, "status"
        )
        self.origin = _enum_value(  # type: ignore[assignment]
            self.origin, ActivityOrigin, "origin"
        )
        self.autonomy = _autonomy(self.autonomy)
        self.project_id = _optional_id(self.project_id, "project_id")
        self.focal_engram_id = _optional_id(
            self.focal_engram_id, "focal_engram_id"
        )
        self.created_at = _utc(self.created_at, "created_at")
        self.updated_at = _utc(self.updated_at, "updated_at")
        if self.last_active_at is not None:
            self.last_active_at = _utc(self.last_active_at, "last_active_at")


@dataclass
class CenterMembership:
    center_id: str
    engram_id: str
    relation: MembershipRelation = MembershipRelation.PARTICIPANT
    created_at: datetime = field(default_factory=_now)

    def __post_init__(self) -> None:
        self.center_id = _nonempty_id(self.center_id, "center_id")
        self.engram_id = _nonempty_id(self.engram_id, "engram_id")
        self.relation = _enum_value(  # type: ignore[assignment]
            self.relation, MembershipRelation, "relation"
        )
        self.created_at = _utc(self.created_at, "created_at")


@dataclass
class TaskFront:
    center_id: str
    focal_engram_id: str
    title: str
    id: str = field(default_factory=_uuid)
    status: TaskFrontStatus = TaskFrontStatus.OPEN
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)
    last_opened_at: datetime = field(default_factory=_now)

    def __post_init__(self) -> None:
        self.id = _nonempty_id(self.id, "id")
        self.center_id = _nonempty_id(self.center_id, "center_id")
        self.focal_engram_id = _nonempty_id(
            self.focal_engram_id, "focal_engram_id"
        )
        self.title = _title(self.title)
        self.status = _enum_value(  # type: ignore[assignment]
            self.status, TaskFrontStatus, "status"
        )
        self.created_at = _utc(self.created_at, "created_at")
        self.updated_at = _utc(self.updated_at, "updated_at")
        self.last_opened_at = _utc(self.last_opened_at, "last_opened_at")


@dataclass(frozen=True)
class TaskFrontBundle:
    front: TaskFront
    center: ActivityCenter
    membership: CenterMembership
    focal_engram: Engram


@dataclass(frozen=True)
class TaskOffer:
    """One durable invitation from a user to a continuing subject."""

    world_id: str
    subject_engram_id: str
    id: str = field(default_factory=_uuid)
    status: TaskOfferStatus = TaskOfferStatus.PENDING
    current_revision: int = 1
    task_front_id: str | None = None
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)
    decided_at: datetime | None = None
    withdrawn_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _nonempty_id(self.id, "id"))
        object.__setattr__(
            self, "world_id", _nonempty_id(self.world_id, "world_id")
        )
        object.__setattr__(
            self,
            "subject_engram_id",
            _nonempty_id(self.subject_engram_id, "subject_engram_id"),
        )
        status = _enum_value(self.status, TaskOfferStatus, "status")
        object.__setattr__(self, "status", status)
        if (
            isinstance(self.current_revision, bool)
            or not isinstance(self.current_revision, int)
            or self.current_revision < 1
        ):
            raise ValueError("current_revision must be an integer >= 1")
        object.__setattr__(
            self,
            "task_front_id",
            _optional_id(self.task_front_id, "task_front_id"),
        )
        created_at = _utc(self.created_at, "created_at")
        updated_at = _utc(self.updated_at, "updated_at")
        decided_at = (
            None
            if self.decided_at is None
            else _utc(self.decided_at, "decided_at")
        )
        withdrawn_at = (
            None
            if self.withdrawn_at is None
            else _utc(self.withdrawn_at, "withdrawn_at")
        )
        if updated_at < created_at:
            raise ValueError("updated_at must not precede created_at")
        if decided_at is not None and decided_at < created_at:
            raise ValueError("decided_at must not precede created_at")
        if withdrawn_at is not None and withdrawn_at < created_at:
            raise ValueError("withdrawn_at must not precede created_at")
        if status is TaskOfferStatus.ACCEPTED:
            if self.task_front_id is None or decided_at is None:
                raise ValueError(
                    "accepted TaskOffer requires task_front_id and decided_at"
                )
            if withdrawn_at is not None:
                raise ValueError("accepted TaskOffer forbids withdrawn_at")
        elif status is TaskOfferStatus.REFUSED:
            if self.task_front_id is not None or decided_at is None:
                raise ValueError(
                    "refused TaskOffer requires decided_at and forbids task_front_id"
                )
            if withdrawn_at is not None:
                raise ValueError("refused TaskOffer forbids withdrawn_at")
        elif status is TaskOfferStatus.WITHDRAWN:
            if (
                self.task_front_id is not None
                or decided_at is not None
                or withdrawn_at is None
            ):
                raise ValueError(
                    "withdrawn TaskOffer requires withdrawn_at and forbids "
                    "task_front_id/decided_at"
                )
        elif (
            self.task_front_id is not None
            or decided_at is not None
            or withdrawn_at is not None
        ):
            raise ValueError(
                "nonterminal TaskOffer forbids task_front_id and terminal timestamps"
            )
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "decided_at", decided_at)
        object.__setattr__(self, "withdrawn_at", withdrawn_at)


@dataclass(frozen=True)
class TaskOfferRevision:
    """Immutable terms and the subject decision for one negotiation round."""

    offer_id: str
    revision: int
    content: str
    title: str
    latest_offer_event_id: str
    project_id: str | None = None
    decision: TaskOfferDecision | None = None
    subject_response: str | None = None
    decision_event_id: str | None = None
    created_at: datetime = field(default_factory=_now)
    decided_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "offer_id", _nonempty_id(self.offer_id, "offer_id")
        )
        if (
            isinstance(self.revision, bool)
            or not isinstance(self.revision, int)
            or self.revision < 1
        ):
            raise ValueError("revision must be an integer >= 1")
        if not isinstance(self.content, str):
            raise ValueError("content must be a string")
        if not self.content.strip() or len(self.content) > 12_000:
            raise ValueError(
                "content must contain 1..12000 characters with non-whitespace text"
            )
        object.__setattr__(self, "title", _title(self.title))
        object.__setattr__(
            self,
            "latest_offer_event_id",
            _nonempty_id(
                self.latest_offer_event_id,
                "latest_offer_event_id",
            ),
        )
        object.__setattr__(
            self, "project_id", _optional_id(self.project_id, "project_id")
        )
        decision = (
            None
            if self.decision is None
            else _enum_value(self.decision, TaskOfferDecision, "decision")
        )
        object.__setattr__(self, "decision", decision)
        if self.subject_response is not None:
            if not isinstance(self.subject_response, str):
                raise ValueError("subject_response must be a string or null")
            if len(self.subject_response) > 4_000:
                raise ValueError(
                    "subject_response must contain at most 4000 characters"
                )
        object.__setattr__(
            self,
            "decision_event_id",
            _optional_id(self.decision_event_id, "decision_event_id"),
        )
        created_at = _utc(self.created_at, "created_at")
        decided_at = (
            None
            if self.decided_at is None
            else _utc(self.decided_at, "decided_at")
        )
        if decided_at is not None and decided_at < created_at:
            raise ValueError("decided_at must not precede created_at")
        if decision is None:
            if (
                self.subject_response is not None
                or self.decision_event_id is not None
                or decided_at is not None
            ):
                raise ValueError(
                    "undecided TaskOfferRevision forbids response and decision evidence"
                )
        else:
            if self.decision_event_id is None or decided_at is None:
                raise ValueError(
                    "decided TaskOfferRevision requires decision_event_id and decided_at"
                )
            if (
                decision is TaskOfferDecision.REQUEST_CHANGES
                and (
                    self.subject_response is None
                    or not self.subject_response.strip()
                )
            ):
                raise ValueError(
                    "request_changes requires a non-empty subject_response"
                )
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "decided_at", decided_at)


@dataclass(frozen=True)
class TaskOfferSnapshot:
    """Consistent offer aggregate returned to service consumers."""

    offer: TaskOffer
    current_revision: TaskOfferRevision
    revisions: tuple[TaskOfferRevision, ...]

    def __post_init__(self) -> None:
        revisions = tuple(self.revisions)
        if not revisions:
            raise ValueError("TaskOfferSnapshot requires at least one revision")
        if any(revision.offer_id != self.offer.id for revision in revisions):
            raise ValueError("all revisions must belong to the snapshot offer")
        revision_numbers = tuple(item.revision for item in revisions)
        if revision_numbers != tuple(range(1, self.offer.current_revision + 1)):
            raise ValueError(
                "TaskOfferSnapshot revisions must be complete and ordered"
            )
        if self.current_revision.offer_id != self.offer.id or (
            self.current_revision.revision != self.offer.current_revision
        ):
            raise ValueError("current revision must match the TaskOffer fence")
        if not any(
            revision == self.current_revision for revision in revisions
        ):
            raise ValueError("current revision must be present in revisions")
        expected_decision = {
            TaskOfferStatus.PENDING: None,
            TaskOfferStatus.CHANGES_REQUESTED: (
                TaskOfferDecision.REQUEST_CHANGES
            ),
            TaskOfferStatus.ACCEPTED: TaskOfferDecision.ACCEPT,
            TaskOfferStatus.REFUSED: TaskOfferDecision.REFUSE,
        }.get(self.offer.status)
        if self.offer.status is not TaskOfferStatus.WITHDRAWN and (
            self.current_revision.decision is not expected_decision
        ):
            raise ValueError(
                "current revision decision must match the TaskOffer status"
            )
        if self.offer.status is TaskOfferStatus.WITHDRAWN and (
            self.current_revision.decision
            not in {None, TaskOfferDecision.REQUEST_CHANGES}
        ):
            raise ValueError(
                "withdrawn TaskOffer may preserve only a change request"
            )
        object.__setattr__(self, "revisions", revisions)


@dataclass(frozen=True)
class TaskRelationship:
    """The subject-owned lifecycle of one accepted task relationship."""

    world_id: str
    accepted_offer_id: str
    task_front_id: str
    center_id: str
    original_subject_engram_id: str
    current_subject_engram_id: str
    id: str = field(default_factory=_uuid)
    status: TaskRelationshipStatus = TaskRelationshipStatus.ACTIVE
    revision: int = 1
    latest_terms_event_id: str | None = None
    latest_subject_note: str | None = None
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)
    exited_at: datetime | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "id",
            "world_id",
            "accepted_offer_id",
            "task_front_id",
            "center_id",
            "original_subject_engram_id",
            "current_subject_engram_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _nonempty_id(getattr(self, field_name), field_name),
            )
        status = _enum_value(self.status, TaskRelationshipStatus, "status")
        object.__setattr__(self, "status", status)
        if (
            isinstance(self.revision, bool)
            or not isinstance(self.revision, int)
            or self.revision < 1
        ):
            raise ValueError("revision must be an integer >= 1")
        object.__setattr__(
            self,
            "latest_terms_event_id",
            _optional_id(self.latest_terms_event_id, "latest_terms_event_id"),
        )
        if self.latest_subject_note is not None:
            if not isinstance(self.latest_subject_note, str):
                raise ValueError("latest_subject_note must be a string or null")
            if len(self.latest_subject_note) > 4_000:
                raise ValueError(
                    "latest_subject_note must contain at most 4000 characters"
                )
        created_at = _utc(self.created_at, "created_at")
        updated_at = _utc(self.updated_at, "updated_at")
        exited_at = (
            None if self.exited_at is None else _utc(self.exited_at, "exited_at")
        )
        if updated_at < created_at:
            raise ValueError("updated_at must not precede created_at")
        if status is TaskRelationshipStatus.EXITED:
            if exited_at is None:
                raise ValueError("exited TaskRelationship requires exited_at")
        elif exited_at is not None:
            raise ValueError("non-exited TaskRelationship forbids exited_at")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "exited_at", exited_at)


@dataclass(frozen=True)
class TaskRelationshipEvent:
    """One immutable actor-attributed relationship transition or proposal."""

    relationship_id: str
    seq: int
    action: TaskRelationshipAction
    actor_kind: TaskRelationshipActorKind
    actor_id: str
    after_status: TaskRelationshipStatus
    before_status: TaskRelationshipStatus | None = None
    content: str | None = None
    source_event_id: str | None = None
    created_at: datetime = field(default_factory=_now)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "relationship_id",
            _nonempty_id(self.relationship_id, "relationship_id"),
        )
        if isinstance(self.seq, bool) or not isinstance(self.seq, int) or self.seq < 1:
            raise ValueError("seq must be an integer >= 1")
        object.__setattr__(
            self,
            "action",
            _enum_value(self.action, TaskRelationshipAction, "action"),
        )
        object.__setattr__(
            self,
            "actor_kind",
            _enum_value(self.actor_kind, TaskRelationshipActorKind, "actor_kind"),
        )
        object.__setattr__(self, "actor_id", _nonempty_id(self.actor_id, "actor_id"))
        object.__setattr__(
            self,
            "after_status",
            _enum_value(self.after_status, TaskRelationshipStatus, "after_status"),
        )
        if self.before_status is not None:
            object.__setattr__(
                self,
                "before_status",
                _enum_value(
                    self.before_status,
                    TaskRelationshipStatus,
                    "before_status",
                ),
            )
        if self.content is not None:
            if not isinstance(self.content, str):
                raise ValueError("content must be a string or null")
            if len(self.content) > 12_000:
                raise ValueError("content must contain at most 12000 characters")
        object.__setattr__(
            self,
            "source_event_id",
            _optional_id(self.source_event_id, "source_event_id"),
        )
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))


@dataclass(frozen=True)
class TaskRelationshipSnapshot:
    relationship: TaskRelationship
    events: tuple[TaskRelationshipEvent, ...]

    def __post_init__(self) -> None:
        events = tuple(self.events)
        if not events or events[0].action is not TaskRelationshipAction.ACCEPTED:
            raise ValueError("TaskRelationshipSnapshot must begin with accepted evidence")
        if any(event.relationship_id != self.relationship.id for event in events):
            raise ValueError("relationship events must belong to the snapshot")
        if tuple(event.seq for event in events) != tuple(range(1, len(events) + 1)):
            raise ValueError("relationship event sequence must be complete and ordered")
        if len(events) != self.relationship.revision:
            raise ValueError("relationship revision must equal its durable event count")
        if events[-1].after_status is not self.relationship.status:
            raise ValueError("latest relationship event must match current status")
        object.__setattr__(self, "events", events)


@dataclass(frozen=True)
class ActivityCenterBundle:
    center: ActivityCenter
    membership: CenterMembership
    focal_engram: Engram


# Readable compatibility aliases for callers that name enums after the
# aggregate rather than its field. They point to the same frozen value sets.
ActivityCenterKind = ActivityKind
ActivityCenterOrigin = ActivityOrigin
CenterMembershipRelation = MembershipRelation

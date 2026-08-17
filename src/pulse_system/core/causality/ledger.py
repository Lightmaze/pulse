"""Durable causal truth for the single PulseWorld.

This module deliberately owns the multi-row transactions.  ``Storage`` keeps
the SQLite connection and its re-entrant lock; ``CausalLedger`` acquires that
lock and only calls uncommitted/private primitives while a transaction is
open.  No public Storage method is called from a ledger transaction, so one
operation has one transaction owner and one commit boundary.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Iterator, Mapping

from pulse_system.core.types import (
    CausalEvent,
    CausalEventDomain,
    CausalEventFlow,
    CausalEventKind,
    CausalEventResolution,
    CausalEventSource,
    CausalEventStatus,
    EngramStatus,
    GenerationTransition,
    GenerationTransitionState,
    HarnessTurn,
    HarnessTurnState,
    Message,
    MessageRole,
    RecoveryReport,
)
from pulse_system.core.types.models import _metadata, _uuid
from pulse_system.core.dendrite.processor import (
    DENDRITIC_WINDOW_POLICY_VERSION,
    DendriticReadyWindow,
    DendriticWindowPolicySnapshot,
)
from pulse_system.substrate.storage.store import Storage
from pulse_system.core.runtime.publication import (
    RuntimeBootstrapPermit,
    RuntimePublicationPermit,
    RuntimeRecoveryPermit,
)
from pulse_system.core.causality.flow_contract import (
    CausalFlowInvariantError,
    NON_TURN_ROOT_KIND_VALUES,
    assert_causal_flow,
    assert_causal_turn,
    causal_turn_violation_codes,
)
from pulse_system.core.causality.amplification import (
    CausalAmplificationSnapshot,
    read_causal_amplification,
)


class CausalLedgerError(RuntimeError):
    """Base error for ledger-specific failures."""


class CausalTransitionError(ValueError):
    """Raised when a compare-and-set state transition cannot be applied."""


class CausalAdmissionConflictError(CausalTransitionError):
    """A valid request lost an admission race against newer causal state."""


class DendriticWindowConflictError(CausalAdmissionConflictError):
    """A ready-window snapshot changed before its atomic materialization."""


@dataclass(frozen=True)
class RuntimeFence:
    """One lease epoch plus an optional same-owner lifecycle capability."""

    owner_id: str
    epoch: int
    permit: (
        RuntimeBootstrapPermit
        | RuntimePublicationPermit
        | RuntimeRecoveryPermit
        | None
    ) = None

    def __post_init__(self) -> None:
        if not isinstance(self.owner_id, str) or not self.owner_id.strip():
            raise ValueError("runtime fence owner_id must be a non-empty string")
        if (
            isinstance(self.epoch, bool)
            or not isinstance(self.epoch, int)
            or self.epoch < 1
        ):
            raise ValueError("runtime fence epoch must be an integer >= 1")
        if self.permit is not None:
            if not isinstance(
                self.permit,
                (
                    RuntimeBootstrapPermit,
                    RuntimePublicationPermit,
                    RuntimeRecoveryPermit,
                ),
            ):
                raise ValueError("runtime fence permit has an invalid type")
            if (
                self.permit.owner_id != self.owner_id
                or self.permit.epoch != self.epoch
            ):
                raise ValueError("runtime fence permit belongs to another owner epoch")

    def assert_publication(self) -> None:
        if self.permit is None:
            raise ValueError("a permitless RuntimeFence cannot authorize publication")
        if not isinstance(self.permit, RuntimePublicationPermit):
            raise ValueError("a recovery permit or bootstrap permit cannot authorize publication")
        self.permit.assert_publication()

    def assert_recovery(self) -> None:
        if self.permit is None:
            raise ValueError("a permitless RuntimeFence cannot authorize recovery")
        if isinstance(self.permit, RuntimeBootstrapPermit):
            self.permit.assert_bootstrap()
            return
        if isinstance(self.permit, RuntimeRecoveryPermit):
            self.permit.assert_recovery()
            return
        raise ValueError("an ordinary publication permit cannot authorize recovery")


@dataclass(frozen=True)
class EngramPulseActivity:
    """Engram metadata committed atomically with one settled Harness turn."""

    last_pulse_at: datetime
    token_count: int
    recent_activity_delta: float = 0.2

    def __post_init__(self) -> None:
        if (
            not isinstance(self.last_pulse_at, datetime)
            or self.last_pulse_at.tzinfo is None
            or self.last_pulse_at.utcoffset() is None
        ):
            raise ValueError("last_pulse_at must be a timezone-aware datetime")
        if (
            isinstance(self.token_count, bool)
            or not isinstance(self.token_count, int)
            or self.token_count < 0
        ):
            raise ValueError("token_count must be an integer >= 0")
        if isinstance(self.recent_activity_delta, bool) or not isinstance(
            self.recent_activity_delta, (int, float)
        ):
            raise ValueError("recent_activity_delta must be a finite number")
        delta = float(self.recent_activity_delta)
        if not math.isfinite(delta) or delta < 0.0:
            raise ValueError("recent_activity_delta must be a finite number >= 0")


@dataclass(frozen=True)
class DendriticIntegrationMember:
    """One immutable input edge into a durable convergence nexus."""

    ordinal: int
    event_id: str
    event_seq: int
    causal_id: str
    source_identity: str
    content_sha256: str
    arrived_at: datetime


@dataclass(frozen=True)
class DendriticWindowMember:
    """One raw queued input observed in a closed durable timing cohort."""

    ordinal: int
    event_id: str
    event_seq: int
    arrived_at: datetime


@dataclass(frozen=True)
class DendriticWindow:
    """Immutable policy snapshot and membership of one closed input window."""

    id: str
    world_id: str
    formation_engram_id: str
    policy_version: str
    event_set_sha256: str
    event_count: int
    base_silence_threshold_seconds: float
    base_max_wait_seconds: float
    wait_modifier: float
    silence_threshold_seconds: float
    max_wait_seconds: float
    window_opened_at: datetime
    last_input_at: datetime
    window_closed_at: datetime
    observed_at: datetime
    observed_event_seq: int
    created_at: datetime
    members: tuple[DendriticWindowMember, ...]


@dataclass(frozen=True)
class DendriticIntegration:
    """A deterministic many-to-one CONTENT integration fact."""

    id: str
    world_id: str
    formation_engram_id: str
    center_id: str | None
    aggregate_event_id: str
    delivery_class: str
    member_set_sha256: str
    content_sha256: str
    member_count: int
    window_opened_at: datetime
    window_closed_at: datetime
    created_at: datetime
    members: tuple[DendriticIntegrationMember, ...]
    window_evidence_class: str
    window: DendriticWindow | None


_EVENT_COLUMNS = """
    seq, id, causal_id, parent_event_id, world_id, engram_id, center_id,
    flow, domain, kind, source, status, content, metadata, idempotency_key,
    attempts, created_at, updated_at, started_at, settled_at, resolution,
    resolution_note
"""
_TURN_COLUMNS = """
    id, event_id, engram_id, state, cursor_before, cursor_after,
    input_message_id, prompt_accepted, session_id, session_file, result_event_id,
    error_code, error_phase, started_at, updated_at, settled_at
"""
_GENERATION_COLUMNS = """
    id, causal_id, event_id, predecessor_id, successor_id, state,
    summary_turn_id, error_code, created_at, updated_at, settled_at
"""
_DENDRITIC_INTEGRATION_COLUMNS = """
    id, world_id, formation_engram_id, center_id, aggregate_event_id, delivery_class,
    member_set_sha256, content_sha256, member_count, window_opened_at,
    window_closed_at, created_at
"""
_DENDRITIC_WINDOW_COLUMNS = """
    id, world_id, formation_engram_id, policy_version, event_set_sha256,
    event_count, base_silence_threshold_seconds, base_max_wait_seconds,
    wait_modifier, silence_threshold_seconds, max_wait_seconds,
    window_opened_at, last_input_at, window_closed_at, observed_at,
    observed_event_seq, created_at
"""
MAX_DENDRITIC_INTEGRATION_MEMBERS = 64
MAX_DENDRITIC_WINDOW_EVENTS = 500
DENDRITIC_WINDOW_EVIDENCE_DURABLE_V6 = "DURABLE_V6"
DENDRITIC_WINDOW_EVIDENCE_LEGACY_V5 = "LEGACY_V5_NO_WINDOW"

_TERMINAL_EVENT_STATES = {
    CausalEventStatus.SETTLED.value,
    CausalEventStatus.FAILED.value,
    CausalEventStatus.UNCERTAIN.value,
    CausalEventStatus.RECONCILED.value,
    CausalEventStatus.CANCELLED.value,
}
_TERMINAL_GENERATION_STATES = {
    GenerationTransitionState.COMMITTED.value,
    GenerationTransitionState.FAILED.value,
    GenerationTransitionState.UNCERTAIN.value,
}
_GENERATION_TRANSITIONS = {
    GenerationTransitionState.PREPARED.value: {
        GenerationTransitionState.SUMMARIZING.value,
        GenerationTransitionState.FAILED.value,
        GenerationTransitionState.UNCERTAIN.value,
    },
    GenerationTransitionState.SUMMARIZING.value: {
        GenerationTransitionState.ROTATING.value,
        GenerationTransitionState.FAILED.value,
        GenerationTransitionState.UNCERTAIN.value,
    },
    GenerationTransitionState.ROTATING.value: {
        GenerationTransitionState.COMMITTED.value,
        GenerationTransitionState.FAILED.value,
        GenerationTransitionState.UNCERTAIN.value,
    },
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ts(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _enum_value(value: Any, enum_type: type, field_name: str) -> str:
    if isinstance(value, enum_type):
        return value.value
    try:
        return enum_type(value).value
    except (TypeError, ValueError) as exc:
        allowed = ", ".join(member.value for member in enum_type)
        raise ValueError(f"{field_name} must be one of: {allowed}") from exc


def _optional_id(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string or null")
    return value


def _require_id(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _safe_code(value: str | None, default: str) -> str:
    if value is None:
        return default
    if not isinstance(value, str) or not value.strip():
        raise ValueError("error codes and phases must be non-empty strings")
    return value


_SAFE_EFFECT_ERROR_CODE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z"
)


def _safe_effect_error_code(value: str | None) -> str:
    """Return a bounded, non-natural-language effect error code."""

    if value is None:
        return "effect_error"
    if not isinstance(value, str):
        raise ValueError("effect error_code must be a string or null")
    normalized = value.strip()
    if _SAFE_EFFECT_ERROR_CODE.fullmatch(normalized) is None:
        raise ValueError(
            "effect error_code must be 1..128 ASCII code characters "
            "(letters, digits, '.', '_', ':', or '-')"
        )
    return normalized


def _safe_note(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("resolution note must be a string or null")
    if len(value) > 2048:
        raise ValueError("resolution note must contain at most 2048 characters")
    return value


_TERMINAL_CHILD_STATES = {
    CausalEventStatus.SETTLED.value,
    CausalEventStatus.FAILED.value,
    CausalEventStatus.CANCELLED.value,
}
_EFFECT_ACCEPTANCE_STATES = {
    False: CausalEventStatus.FAILED.value,
    True: CausalEventStatus.UNCERTAIN.value,
    None: CausalEventStatus.UNCERTAIN.value,
}
_NON_CLAIMABLE_ROOT_KINDS = NON_TURN_ROOT_KIND_VALUES


class CausalLedger:
    """SQLite-backed causal event, turn, and generation state machine."""

    def __init__(
        self,
        storage: Storage,
        *,
        default_runtime_fence: RuntimeFence | None = None,
    ):
        if default_runtime_fence is not None:
            if not isinstance(default_runtime_fence, RuntimeFence):
                raise ValueError("default_runtime_fence must be a RuntimeFence or null")
            if not isinstance(
                default_runtime_fence.permit,
                RuntimePublicationPermit,
            ):
                raise ValueError(
                    "a runtime-bound CausalLedger requires a publication permit"
                )
        self._storage = storage
        self._default_runtime_fence = default_runtime_fence

    @property
    def storage(self) -> Storage:
        return self._storage

    @contextmanager
    def _transaction(
        self,
        *,
        runtime_fence: RuntimeFence | None = None,
        allow_recovery: bool = False,
    ) -> Iterator[sqlite3.Connection]:
        """Run one write operation with one owner and one commit boundary."""

        conn = self._storage._conn
        authority = (
            self._recovery_authority(runtime_fence)
            if allow_recovery
            else nullcontext()
        )
        with authority:
            with self._storage._lock:
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    yield conn
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise

    def _assert_runtime_fence_uncommitted(
        self,
        conn: sqlite3.Connection,
        runtime_fence: RuntimeFence | None,
        *,
        allow_recovery: bool = False,
    ) -> None:
        """Linearize an optional Runtime fence with the caller's writes."""

        runtime_fence = self._resolve_runtime_fence(runtime_fence)
        if runtime_fence is None:
            return
        if not isinstance(runtime_fence, RuntimeFence):
            raise ValueError("runtime_fence must be a RuntimeFence or null")
        self._storage._assert_runtime_lease_uncommitted(
            runtime_fence.owner_id,
            runtime_fence.epoch,
            _now(),
            conn,
        )
        if runtime_fence.permit is None:
            # Direct/offline ledgers may use a lease-only fence.  Runtime-bound
            # ledgers reject it in _resolve_runtime_fence before this point.
            return
        if allow_recovery:
            runtime_fence.assert_recovery()
        else:
            runtime_fence.assert_publication()

    def _resolve_runtime_fence(
        self,
        runtime_fence: RuntimeFence | None,
    ) -> RuntimeFence | None:
        """Apply the live Runtime's default fence without breaking offline use."""

        if runtime_fence is not None and not isinstance(runtime_fence, RuntimeFence):
            raise ValueError("runtime_fence must be a RuntimeFence or null")
        default = self._default_runtime_fence
        if default is None:
            return runtime_fence
        if runtime_fence is None:
            return default
        if (
            runtime_fence.owner_id != default.owner_id
            or runtime_fence.epoch != default.epoch
        ):
            raise ValueError("runtime fence belongs to another Runtime owner epoch")
        if runtime_fence.permit is None:
            raise ValueError(
                "a runtime-bound CausalLedger does not accept a permitless fence"
            )
        return runtime_fence

    def _recovery_authority(self, runtime_fence: RuntimeFence | None):
        resolved = self._resolve_runtime_fence(runtime_fence)
        if resolved is None or resolved.permit is None:
            return nullcontext()
        if not isinstance(
            resolved.permit,
            (RuntimeBootstrapPermit, RuntimeRecoveryPermit),
        ):
            raise ValueError(
                "an ordinary publication permit cannot authorize causal recovery"
            )
        return self._storage._runtime_authority_scope(resolved.permit)

    def _assert_generation_runtime_fence_uncommitted(
        self,
        conn: sqlite3.Connection,
        generation: GenerationTransition,
        runtime_fence: RuntimeFence | None,
    ) -> None:
        """Keep one generation bound to the epoch that began it."""

        runtime_fence = self._resolve_runtime_fence(runtime_fence)
        if generation.event_id is None:
            return
        event = self._get_event_uncommitted(conn, generation.event_id)
        if event is None:
            raise CausalTransitionError(
                f"generation {generation.id} references a missing event"
            )
        owner_id = event.metadata.get("runtime_owner_id")
        lease_epoch = event.metadata.get("runtime_lease_epoch")
        if owner_id is None and lease_epoch is None:
            return
        if (
            not isinstance(owner_id, str)
            or not owner_id
            or type(lease_epoch) is not int
            or lease_epoch < 1
        ):
            raise CausalTransitionError(
                f"generation {generation.id} has an invalid Runtime fence"
            )
        if (
            runtime_fence is None
            or runtime_fence.owner_id != owner_id
            or runtime_fence.epoch != lease_epoch
        ):
            raise CausalTransitionError(
                f"generation {generation.id} belongs to another Runtime epoch"
            )

    @staticmethod
    def _update_engram_pulse_activity_uncommitted(
        conn: sqlite3.Connection,
        engram_id: str,
        activity: EngramPulseActivity | None,
    ) -> None:
        if activity is None:
            return
        if not isinstance(activity, EngramPulseActivity):
            raise ValueError("engram_activity must be an EngramPulseActivity or null")
        updated = conn.execute(
            "UPDATE engrams SET last_pulse_at = ?, "
            "total_pulses = total_pulses + 1, "
            "recent_activity = MIN(1.0, MAX(0.0, recent_activity + ?)), "
            "token_count = ? WHERE id = ?",
            (
                _ts(activity.last_pulse_at),
                float(activity.recent_activity_delta),
                activity.token_count,
                engram_id,
            ),
        )
        if updated.rowcount != 1:
            raise CausalTransitionError(
                f"Engram {engram_id} changed before pulse activity settlement"
            )

    # ── Causal event CRUD ───────────────────────────────────────

    def enqueue(
        self,
        world_id: str,
        flow: CausalEventFlow | str | None = None,
        domain: CausalEventDomain | str = CausalEventDomain.SYSTEM,
        kind: CausalEventKind | str = CausalEventKind.SYSTEM,
        source: CausalEventSource | str = CausalEventSource.SYSTEM,
        content: str | None = None,
        *,
        causal_id: str | None = None,
        parent_event_id: str | None = None,
        engram_id: str | None = None,
        center_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        event_id: str | None = None,
        id: str | None = None,
        admission_guard: Callable[[sqlite3.Connection], None] | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> CausalEvent:
        """Persist a queued event, returning the existing idempotent event."""

        requested_id = event_id or id or _uuid()
        if event_id is not None and id is not None and event_id != id:
            raise ValueError("event_id and id must identify the same event")
        requested_id = _require_id(requested_id, "event_id")
        world_id = _require_id(world_id, "world_id")
        parent_event_id = _optional_id(parent_event_id, "parent_event_id")
        engram_id = _optional_id(engram_id, "engram_id")
        center_id = _optional_id(center_id, "center_id")
        idempotency_key = _optional_id(idempotency_key, "idempotency_key")
        normalized_kind = _enum_value(kind, CausalEventKind, "kind")
        if normalized_kind in _NON_CLAIMABLE_ROOT_KINDS:
            raise CausalTransitionError(
                f"{normalized_kind} must be recorded as a child or effect"
            )

        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)
            if idempotency_key is not None:
                existing = conn.execute(
                    f"SELECT {_EVENT_COLUMNS} FROM causal_events "
                    "WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing is not None:
                    event = self._row_to_event(existing)
                    if event.world_id != world_id:
                        raise CausalTransitionError(
                            "idempotency_key is already used in another world"
                        )
                    if event_id is not None and event.id != event_id:
                        raise CausalTransitionError(
                            "idempotency_key is already bound to another event"
                        )
                    return event

            if admission_guard is not None:
                if not callable(admission_guard):
                    raise TypeError("admission_guard must be callable or null")
                admission_guard(conn)

            parent = None
            if parent_event_id is not None:
                parent_row = conn.execute(
                    f"SELECT {_EVENT_COLUMNS} FROM causal_events WHERE id = ?",
                    (parent_event_id,),
                ).fetchone()
                if parent_row is None:
                    raise KeyError(f"unknown parent event: {parent_event_id}")
                parent = self._row_to_event(parent_row)
                if parent.world_id != world_id:
                    raise ValueError("parent event must belong to the same world")
                if causal_id is not None and causal_id != parent.causal_id:
                    raise ValueError("child event must preserve parent causal_id")
                causal_id = parent.causal_id
                center_id = self._inherit_parent_center(parent, center_id)

            self._ensure_references(
                conn,
                engram_id,
                center_id,
                require_membership=parent is None,
            )
            event = CausalEvent(
                id=requested_id,
                world_id=world_id,
                causal_id=causal_id or requested_id,
                parent_event_id=parent_event_id,
                engram_id=engram_id,
                center_id=center_id,
                flow=flow,
                domain=domain,
                kind=normalized_kind,
                source=source,
                status=CausalEventStatus.QUEUED,
                content=content,
                metadata=metadata if metadata is not None else {},
                idempotency_key=idempotency_key,
                attempts=0,
            )
            self._insert_event_uncommitted(conn, event)
            return self._get_event_uncommitted(conn, event.id)

    enqueue_causal_event = enqueue

    def get_event(self, event_id: str) -> CausalEvent | None:
        with self._storage._lock:
            return self._get_event_uncommitted(self._storage._conn, event_id)

    def find_causal_event_by_idempotency(
        self,
        world_id: str,
        idempotency_key: str,
    ) -> CausalEvent | None:
        """Look up one canonical event without creating or mutating it.

        The world predicate is intentional even though SQLite also keeps the
        key unique: crash-repair callers must never use a key from another
        world as evidence for the current PulseWorld.
        """

        world_id = _require_id(world_id, "world_id")
        idempotency_key = _require_id(idempotency_key, "idempotency_key")
        with self._storage._lock:
            row = self._storage._conn.execute(
                f"SELECT {_EVENT_COLUMNS} FROM causal_events "
                "WHERE world_id = ? AND idempotency_key = ?",
                (world_id, idempotency_key),
            ).fetchone()
            return self._row_to_event(row) if row is not None else None

    def get(self, event_id: str) -> CausalEvent | None:
        return self.get_event(event_id)

    def list_events(
        self,
        after_seq: int = 0,
        limit: int = 100,
        *,
        world_id: str | None = None,
        engram_id: str | None = None,
        center_id: str | None = None,
        causal_id: str | None = None,
        parent_event_id: str | None = None,
        status: CausalEventStatus | str | Iterable[CausalEventStatus | str] | None = None,
        flow: CausalEventFlow | str | None = None,
        domain: CausalEventDomain | str | None = None,
        kind: CausalEventKind | str | None = None,
        source: CausalEventSource | str | None = None,
    ) -> list[CausalEvent]:
        if isinstance(after_seq, bool) or not isinstance(after_seq, int) or after_seq < 0:
            raise ValueError("after_seq must be a non-negative integer")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")

        clauses = ["seq > ?"]
        params: list[Any] = [after_seq]
        if world_id is not None:
            clauses.append("world_id = ?")
            params.append(_require_id(world_id, "world_id"))
        if engram_id is not None:
            clauses.append("engram_id = ?")
            params.append(_require_id(engram_id, "engram_id"))
        if center_id is not None:
            clauses.append("center_id = ?")
            params.append(_require_id(center_id, "center_id"))
        if causal_id is not None:
            clauses.append("causal_id = ?")
            params.append(_require_id(causal_id, "causal_id"))
        if parent_event_id is not None:
            clauses.append("parent_event_id = ?")
            params.append(_require_id(parent_event_id, "parent_event_id"))
        if status is not None:
            statuses = self._enum_values(status, CausalEventStatus, "status")
            clauses.append(
                "status IN (" + ",".join("?" for _ in statuses) + ")"
            )
            params.extend(statuses)
        for value, enum_type, column in (
            (flow, CausalEventFlow, "flow"),
            (domain, CausalEventDomain, "domain"),
            (kind, CausalEventKind, "kind"),
            (source, CausalEventSource, "source"),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(_enum_value(value, enum_type, column))

        query = (
            f"SELECT {_EVENT_COLUMNS} FROM causal_events WHERE "
            + " AND ".join(clauses)
            + " ORDER BY seq ASC LIMIT ?"
        )
        params.append(limit)
        with self._storage._lock:
            rows = self._storage._conn.execute(query, params).fetchall()
            return [self._row_to_event(row) for row in rows]

    def list_causal_events(self, *args: Any, **kwargs: Any) -> list[CausalEvent]:
        return self.list_events(*args, **kwargs)

    def get_children(self, event_id: str) -> list[CausalEvent]:
        event_id = _require_id(event_id, "event_id")
        with self._storage._lock:
            rows = self._storage._conn.execute(
                f"SELECT {_EVENT_COLUMNS} FROM causal_events "
                "WHERE parent_event_id = ? ORDER BY seq ASC",
                (event_id,),
            ).fetchall()
            return [self._row_to_event(row) for row in rows]

    children = get_children

    @staticmethod
    def _dendritic_source_signature(
        event: CausalEvent,
        *,
        require_queued: bool,
    ) -> tuple[str, str] | None:
        if (
            not isinstance(event, CausalEvent)
            or (
                require_queued
                and event.status is not CausalEventStatus.QUEUED
            )
            or event.flow is not CausalEventFlow.CONTENT
            or event.engram_id is None
            or not isinstance(event.content, str)
            or not event.content.strip()
            or event.metadata.get("dendritic_integration_version") is not None
        ):
            return None
        if (
            event.kind is CausalEventKind.PROPAGATION
            or event.source is CausalEventSource.PROPAGATION
        ):
            if not (
                event.kind is CausalEventKind.PROPAGATION
                and event.source is CausalEventSource.PROPAGATION
            ):
                return None
            source_id = event.metadata.get("source_engram_id")
            if not isinstance(source_id, str) or not source_id.strip():
                return None
            return "propagation", f"engram:{source_id}"
        # External adapters do not acquire Engram authority by writing an
        # arbitrary metadata string.  Their durable identity is the typed
        # CausalEventSource.  Engram identity is reserved for propagation,
        # where parent/result/settled-turn provenance is mechanically proven.
        return "external", f"source:{event.source.value}"

    @staticmethod
    def _dendritic_legacy_v5_source_signature(
        event: CausalEvent,
    ) -> tuple[str, str] | None:
        """Recompute only the provenance semantics that v5 actually stored.

        This path is read-only and never participates in new convergence.
        v5 allowed an external adapter's non-empty source_engram_id metadata
        to identify a source; retaining that historical interpretation is
        necessary to validate old immutable rows without upgrading their
        evidence claim to the safer v6 source contract.
        """

        if (
            event.flow is not CausalEventFlow.CONTENT
            or event.engram_id is None
            or not isinstance(event.content, str)
            or not event.content.strip()
            or event.metadata.get("dendritic_integration_version") is not None
        ):
            return None
        if (
            event.kind is CausalEventKind.PROPAGATION
            or event.source is CausalEventSource.PROPAGATION
        ):
            if not (
                event.kind is CausalEventKind.PROPAGATION
                and event.source is CausalEventSource.PROPAGATION
            ):
                return None
            source_id = event.metadata.get("source_engram_id")
            if not isinstance(source_id, str) or not source_id.strip():
                return None
            return "propagation", f"engram:{source_id}"
        source_id = event.metadata.get("source_engram_id")
        return (
            "external",
            f"engram:{source_id}"
            if isinstance(source_id, str) and source_id.strip()
            else f"source:{event.source.value}",
        )

    @staticmethod
    def _dendritic_candidate_signature_uncommitted(
        conn: sqlite3.Connection,
        event: CausalEvent,
    ) -> tuple[str, str] | None:
        signature = CausalLedger._dendritic_source_signature(
            event,
            require_queued=True,
        )
        if signature is None:
            return None
        # A synthetic convergence root has no parent from which to inherit a
        # Center. Preserve the established root contract: when a child
        # propagation legitimately carries a Center into a non-member target,
        # it remains an independent turn instead of becoming an invalid root.
        if event.center_id is not None and event.engram_id is not None:
            membership = conn.execute(
                "SELECT 1 FROM center_memberships "
                "WHERE center_id = ? AND engram_id = ?",
                (event.center_id, event.engram_id),
            ).fetchone()
            if membership is None:
                return None
        return signature

    def dendritic_candidate_signature(
        self,
        event: CausalEvent,
    ) -> tuple[str, str] | None:
        """Return the safe grouping class and source identity for one input.

        This is deliberately narrower than ordinary turn claimability.  It
        keeps addressed TUNNEL traffic, internal/null-flow thought and a
        previously materialized convergence root out of the batching layer.
        The transaction revalidates every field; callers may use this only to
        build conservative candidates.
        """

        with self._storage._lock:
            return CausalLedger._dendritic_candidate_signature_uncommitted(
                self._storage._conn,
                event,
            )

    @staticmethod
    def _assert_dendritic_propagation_provenance_uncommitted(
        conn: sqlite3.Connection,
        event: CausalEvent,
        source_identity: str,
    ) -> None:
        """Anchor a propagation label to its settled source Harness turn."""

        source_engram_id = event.metadata.get("source_engram_id")
        depth = event.metadata.get("depth")
        if (
            event.domain is not CausalEventDomain.PULSE
            or not isinstance(source_engram_id, str)
            or not source_engram_id.strip()
            or source_identity != f"engram:{source_engram_id}"
            or type(depth) is not int
            or depth < 1
            or event.parent_event_id is None
        ):
            raise CausalTransitionError(
                "dendritic propagation source metadata is not canonical"
            )
        parent = CausalLedger._get_event_uncommitted(
            conn,
            event.parent_event_id,
        )
        if parent is None or parent.parent_event_id is None:
            raise CausalTransitionError(
                "dendritic propagation lacks a settled source result"
            )
        source_root = CausalLedger._get_event_uncommitted(
            conn,
            parent.parent_event_id,
        )
        if (
            source_root is None
            or parent.world_id != event.world_id
            or parent.causal_id != event.causal_id
            or parent.center_id != event.center_id
            or parent.engram_id != source_engram_id
            or parent.kind is not CausalEventKind.ASSISTANT_RESULT
            or parent.domain is not CausalEventDomain.HARNESS
            or parent.source is not CausalEventSource.SELF
            or parent.status is not CausalEventStatus.SETTLED
            or parent.settled_at is None
            or parent.created_at > event.created_at
            or source_root.world_id != event.world_id
            or source_root.causal_id != event.causal_id
            or source_root.center_id != event.center_id
            or source_root.engram_id != source_engram_id
            or source_root.status is not CausalEventStatus.SETTLED
            or source_root.settled_at is None
        ):
            raise CausalTransitionError(
                "dendritic propagation source lineage is not settled"
            )
        turns = conn.execute(
            "SELECT event_id, engram_id, state, prompt_accepted "
            "FROM harness_turns WHERE result_event_id = ?",
            (parent.id,),
        ).fetchall()
        if (
            len(turns) != 1
            or turns[0][0] != source_root.id
            or turns[0][1] != source_engram_id
            or turns[0][2] != HarnessTurnState.SETTLED.value
            or turns[0][3] != 1
        ):
            raise CausalTransitionError(
                "dendritic propagation lacks one accepted source Harness turn"
            )

    @staticmethod
    def _dendritic_event_set_sha256(event_ids: Iterable[str]) -> str:
        return hashlib.sha256(
            json.dumps(
                tuple(event_ids),
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _dendritic_window_id(
        *,
        world_id: str,
        ready: DendriticReadyWindow,
    ) -> str:
        projection = {
            "schema_version": ready.policy_version,
            "world_id": world_id,
            "formation_engram_id": ready.engram_id,
            "event_ids": list(ready.event_ids),
            "event_seqs": list(ready.event_seqs),
            "base_silence_threshold_seconds": float(
                ready.base_silence_threshold_seconds
            ),
            "base_max_wait_seconds": float(ready.base_max_wait_seconds),
            "wait_modifier": float(ready.wait_modifier),
            "silence_threshold_seconds": float(
                ready.silence_threshold_seconds
            ),
            "max_wait_seconds": float(ready.max_wait_seconds),
            "window_opened_at": _ts(ready.opened_at),
            "last_input_at": _ts(ready.last_input_at),
            "window_closed_at": _ts(ready.closed_at),
        }
        return hashlib.sha256(
            json.dumps(
                projection,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _is_dendritic_window_shape(
        event: CausalEvent,
    ) -> bool:
        return bool(
            event.engram_id is not None
            and not causal_turn_violation_codes(event)
            and event.metadata.get("dendritic_integration_version") != 1
            and not (
                isinstance(event.metadata.get("generation_id"), str)
                and event.metadata.get("generation_stage") == "summary"
            )
        )

    @staticmethod
    def _is_dendritic_window_input_uncommitted(
        event: CausalEvent,
    ) -> bool:
        return bool(
            event.status is CausalEventStatus.QUEUED
            and CausalLedger._is_dendritic_window_shape(event)
        )

    @staticmethod
    def _partition_closed_dendritic_cohorts(
        events: Iterable[CausalEvent],
        *,
        policies: Mapping[str, DendriticWindowPolicySnapshot],
        observed_at: datetime,
    ) -> tuple[tuple[CausalEvent, ...], ...]:
        ordered = sorted(
            events,
            key=lambda event: (
                event.created_at,
                event.seq if event.seq is not None else 2**63,
                event.id,
            ),
        )
        closed: list[tuple[CausalEvent, ...]] = []
        cohort: list[CausalEvent] = []
        opened_at: datetime | None = None
        last_input_at: datetime | None = None
        deadline: datetime | None = None
        policy: DendriticWindowPolicySnapshot | None = None

        def finish() -> None:
            if cohort and deadline is not None and deadline <= observed_at:
                closed.append(tuple(cohort))

        for event in ordered:
            if not cohort:
                cohort = [event]
                opened_at = event.created_at
                last_input_at = event.created_at
                policy = policies.get(event.id)
            else:
                assert deadline is not None
                if event.created_at > deadline:
                    finish()
                    cohort = [event]
                    opened_at = event.created_at
                    last_input_at = event.created_at
                    policy = policies.get(event.id)
                else:
                    cohort.append(event)
                    last_input_at = event.created_at
            if policy is None:
                raise DendriticWindowConflictError(
                    "dendritic input lacks a durable opening policy"
                )
            assert opened_at is not None and last_input_at is not None
            deadline = min(
                opened_at + timedelta(seconds=policy.max_wait_seconds),
                last_input_at
                + timedelta(seconds=policy.silence_threshold_seconds),
            )
        finish()
        return tuple(closed)

    @staticmethod
    def _dendritic_input_policy_uncommitted(
        conn: sqlite3.Connection,
        event_id: str,
    ) -> DendriticWindowPolicySnapshot | None:
        row = conn.execute(
            "SELECT policy_version, base_silence_threshold_seconds, "
            "base_max_wait_seconds, wait_modifier, "
            "silence_threshold_seconds, max_wait_seconds "
            "FROM dendritic_input_policy_snapshots WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            return DendriticWindowPolicySnapshot(
                policy_version=row[0],
                base_silence_threshold_seconds=float(row[1]),
                base_max_wait_seconds=float(row[2]),
                wait_modifier=float(row[3]),
                silence_threshold_seconds=float(row[4]),
                max_wait_seconds=float(row[5]),
            )
        except (TypeError, ValueError) as exc:
            raise CausalTransitionError(
                "dendritic input policy snapshot drifted"
            ) from exc

    def ensure_dendritic_input_policy(
        self,
        event_id: str,
        policy: DendriticWindowPolicySnapshot,
        *,
        runtime_fence: RuntimeFence | None = None,
    ) -> DendriticWindowPolicySnapshot:
        """Freeze the opening policy before a durable input enters Dendrite."""

        event_id = _require_id(event_id, "event_id")
        if not isinstance(policy, DendriticWindowPolicySnapshot):
            raise TypeError("policy must be a DendriticWindowPolicySnapshot")
        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)
            event = self._get_event_uncommitted(conn, event_id)
            existing = self._dendritic_input_policy_uncommitted(conn, event_id)
            if existing is not None:
                identity = conn.execute(
                    "SELECT world_id, engram_id FROM "
                    "dendritic_input_policy_snapshots WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                if (
                    event is None
                    or identity != (event.world_id, event.engram_id)
                ):
                    raise CausalTransitionError(
                        "dendritic input policy identity drifted"
                    )
                return existing
            if (
                event is None
                or event.status is not CausalEventStatus.QUEUED
                or event.engram_id is None
                or not self._is_dendritic_window_shape(event)
            ):
                raise DendriticWindowConflictError(
                    "only a queued durable turn input can freeze window policy"
                )
            now = max(_now(), event.created_at)
            conn.execute(
                "INSERT INTO dendritic_input_policy_snapshots ("
                "event_id, world_id, engram_id, policy_version, "
                "base_silence_threshold_seconds, base_max_wait_seconds, "
                "wait_modifier, silence_threshold_seconds, max_wait_seconds, "
                "recorded_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    event.id,
                    event.world_id,
                    event.engram_id,
                    policy.policy_version,
                    float(policy.base_silence_threshold_seconds),
                    float(policy.base_max_wait_seconds),
                    float(policy.wait_modifier),
                    float(policy.silence_threshold_seconds),
                    float(policy.max_wait_seconds),
                    _ts(now),
                ),
            )
            return policy

    def _validate_ready_dendritic_window_uncommitted(
        self,
        conn: sqlite3.Connection,
        ready: DendriticReadyWindow,
    ) -> tuple[CausalEvent, ...]:
        if not isinstance(ready, DendriticReadyWindow):
            raise TypeError("window must be a DendriticReadyWindow")
        if not 1 <= len(ready.event_ids) <= MAX_DENDRITIC_WINDOW_EVENTS:
            raise ValueError(
                "a dendritic window must contain between 1 and "
                f"{MAX_DENDRITIC_WINDOW_EVENTS} events"
            )
        engram_row = conn.execute(
            "SELECT status FROM engrams WHERE id = ?",
            (ready.engram_id,),
        ).fetchone()
        if engram_row is None or engram_row[0] != EngramStatus.ACTIVE.value:
            raise DendriticWindowConflictError(
                "dendritic window target is no longer an active Engram"
            )
        placeholders = ",".join("?" for _ in ready.event_ids)
        proposed_rows = conn.execute(
            f"SELECT {_EVENT_COLUMNS} FROM causal_events "
            f"WHERE id IN ({placeholders})",
            ready.event_ids,
        ).fetchall()
        proposed = tuple(self._row_to_event(row) for row in proposed_rows)
        if (
            len(proposed) != len(ready.event_ids)
            or {event.engram_id for event in proposed} != {ready.engram_id}
            or len({event.world_id for event in proposed}) != 1
        ):
            raise DendriticWindowConflictError(
                "dendritic window raw identity changed before materialization"
            )
        world_id = proposed[0].world_id
        rows = conn.execute(
            f"SELECT {_EVENT_COLUMNS} FROM causal_events e "
            "WHERE e.world_id = ? AND e.engram_id = ? AND e.status = 'queued' "
            "AND e.created_at <= ? AND NOT EXISTS ("
            "SELECT 1 FROM dendritic_window_members member "
            "WHERE member.event_id = e.id"
            ") ORDER BY e.created_at ASC, e.seq ASC",
            (world_id, ready.engram_id, _ts(ready.observed_at)),
        ).fetchall()
        eligible = tuple(
            event
            for event in (self._row_to_event(row) for row in rows)
            if self._is_dendritic_window_input_uncommitted(event)
        )
        policies: dict[str, DendriticWindowPolicySnapshot] = {}
        for event in eligible:
            policy = self._dendritic_input_policy_uncommitted(conn, event.id)
            if policy is None:
                raise DendriticWindowConflictError(
                    "dendritic queue contains an input without frozen policy"
                )
            policies[event.id] = policy
        cohorts = self._partition_closed_dendritic_cohorts(
            eligible,
            policies=policies,
            observed_at=ready.observed_at,
        )
        matching = tuple(
            cohort
            for cohort in cohorts
            if tuple(event.id for event in cohort) == ready.event_ids
        )
        if len(matching) != 1:
            raise DendriticWindowConflictError(
                "dendritic window is not one exact closed queue cohort"
            )
        events = matching[0]
        opening_policy = policies[events[0].id]
        if (
            tuple(event.seq for event in events) != ready.event_seqs
            or events[0].created_at != ready.opened_at
            or events[-1].created_at != ready.last_input_at
            or opening_policy.policy_version != ready.policy_version
            or opening_policy.base_silence_threshold_seconds
            != float(ready.base_silence_threshold_seconds)
            or opening_policy.base_max_wait_seconds
            != float(ready.base_max_wait_seconds)
            or opening_policy.wait_modifier != float(ready.wait_modifier)
            or opening_policy.silence_threshold_seconds
            != float(ready.silence_threshold_seconds)
            or opening_policy.max_wait_seconds
            != float(ready.max_wait_seconds)
        ):
            raise DendriticWindowConflictError(
                "dendritic window identity changed before materialization"
            )
        expected_closed_at = min(
            ready.opened_at
            + timedelta(seconds=float(ready.max_wait_seconds)),
            ready.last_input_at
            + timedelta(seconds=float(ready.silence_threshold_seconds)),
        )
        if expected_closed_at != ready.closed_at:
            raise DendriticWindowConflictError(
                "dendritic window boundary does not match its policy snapshot"
            )
        return events

    def dendritic_window_event_ids(
        self,
        event_ids: Iterable[str],
    ) -> set[str]:
        """Return inputs already sealed into an immutable timing cohort."""

        if isinstance(event_ids, (str, bytes, bytearray)):
            raise ValueError("event_ids must be an iterable of event ids")
        requested = tuple(_require_id(value, "event_id") for value in event_ids)
        if not requested:
            return set()
        placeholders = ",".join("?" for _ in requested)
        with self._storage._lock:
            rows = self._storage._conn.execute(
                "SELECT event_id FROM dendritic_window_members "
                f"WHERE event_id IN ({placeholders})",
                requested,
            ).fetchall()
        return {str(row[0]) for row in rows}

    def _materialize_dendritic_group_uncommitted(
        self,
        conn: sqlite3.Connection,
        *,
        window_id: str,
        window_opened_at: datetime,
        window_closed_at: datetime,
        events: tuple[CausalEvent, ...],
        now: datetime,
    ) -> tuple[DendriticIntegration, CausalEvent]:
        ordered = tuple(
            sorted(
                events,
                key=lambda event: (
                    event.seq if event.seq is not None else 2**63,
                    event.id,
                ),
            )
        )
        ordered_ids = tuple(event.id for event in ordered)
        member_set_sha256 = self._dendritic_event_set_sha256(ordered_ids)
        if conn.execute(
            "SELECT 1 FROM dendritic_integrations "
            "WHERE member_set_sha256 = ?",
            (member_set_sha256,),
        ).fetchone() is not None:
            raise DendriticWindowConflictError(
                "dendritic member set was already materialized elsewhere"
            )

        signatures: list[tuple[str, str]] = []
        for event in ordered:
            signature = self._dendritic_candidate_signature_uncommitted(
                conn,
                event,
            )
            if signature is None or causal_turn_violation_codes(event):
                raise CausalTransitionError(
                    f"event {event.id} is not a dendritic CONTENT candidate"
                )
            if event.attempts != 0:
                raise CausalTransitionError(
                    "an already-attempted event cannot enter a new integration"
                )
            if conn.execute(
                "SELECT 1 FROM harness_turns WHERE event_id = ? LIMIT 1",
                (event.id,),
            ).fetchone() is not None:
                raise CausalTransitionError(
                    "an event with Harness history cannot enter an integration"
                )
            if signature[0] == "propagation":
                self._assert_dendritic_propagation_provenance_uncommitted(
                    conn,
                    event,
                    signature[1],
                )
            signatures.append(signature)

        world_ids = {event.world_id for event in ordered}
        engram_ids = {event.engram_id for event in ordered}
        center_ids = {event.center_id for event in ordered}
        delivery_classes = {signature[0] for signature in signatures}
        source_identities = {signature[1] for signature in signatures}
        if len(world_ids) != 1 or len(engram_ids) != 1 or len(center_ids) != 1:
            raise CausalTransitionError(
                "dendritic inputs must share world, Engram and ActivityCenter"
            )
        if len(delivery_classes) != 1:
            raise CausalTransitionError(
                "propagation and external delivery classes cannot converge"
            )
        if len(source_identities) < 2:
            raise CausalTransitionError(
                "dendritic convergence requires at least two source identities"
            )

        world_id = ordered[0].world_id
        engram_id = ordered[0].engram_id
        assert engram_id is not None
        center_id = ordered[0].center_id
        delivery_class = signatures[0][0]
        self._ensure_references(
            conn,
            engram_id,
            center_id,
            require_membership=True,
        )
        integrated_content = "\n\n".join(event.content or "" for event in ordered)
        content_sha256 = hashlib.sha256(
            integrated_content.encode("utf-8")
        ).hexdigest()
        priorities: list[float] = []
        depths: list[int] = []
        for event in ordered:
            raw_priority = event.metadata.get(
                "priority",
                0.8 if delivery_class == "propagation" else 1.0,
            )
            priorities.append(
                float(raw_priority)
                if isinstance(raw_priority, (int, float))
                and not isinstance(raw_priority, bool)
                and math.isfinite(float(raw_priority))
                else (0.8 if delivery_class == "propagation" else 1.0)
            )
            raw_depth = event.metadata.get("depth", 0)
            depths.append(
                raw_depth if type(raw_depth) is int and raw_depth >= 0 else 0
            )

        integration_id = _uuid()
        aggregate_id = _uuid()
        aggregate = CausalEvent(
            id=aggregate_id,
            causal_id=aggregate_id,
            parent_event_id=None,
            world_id=world_id,
            engram_id=engram_id,
            center_id=center_id,
            flow=CausalEventFlow.CONTENT,
            domain=CausalEventDomain.PULSE,
            kind=CausalEventKind.PULSE,
            source=CausalEventSource.SELF,
            status=CausalEventStatus.QUEUED,
            content=integrated_content,
            metadata={
                "dendritic_delivery_class": delivery_class,
                "dendritic_integration_id": integration_id,
                "dendritic_integration_version": 1,
                "dendritic_member_count": len(ordered),
                "dendritic_member_set_sha256": member_set_sha256,
                "dendritic_window_id": window_id,
                "depth": max(depths),
                "priority": max(priorities),
            },
            idempotency_key=f"dendritic:{member_set_sha256}",
            attempts=0,
            created_at=now,
            updated_at=now,
        )
        self._insert_event_uncommitted(conn, aggregate)
        conn.execute(
            "INSERT INTO dendritic_integrations ("
            "id, world_id, formation_engram_id, center_id, aggregate_event_id, "
            "delivery_class, member_set_sha256, content_sha256, member_count, "
            "window_opened_at, window_closed_at, created_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                integration_id,
                world_id,
                engram_id,
                center_id,
                aggregate.id,
                delivery_class,
                member_set_sha256,
                content_sha256,
                len(ordered),
                _ts(window_opened_at),
                _ts(window_closed_at),
                _ts(now),
            ),
        )
        conn.execute(
            "INSERT INTO dendritic_integration_windows "
            "(integration_id, window_id) VALUES (?, ?)",
            (integration_id, window_id),
        )
        for ordinal, (event, signature) in enumerate(zip(ordered, signatures)):
            conn.execute(
                "INSERT INTO dendritic_integration_members ("
                "integration_id, ordinal, event_id, event_seq, causal_id, "
                "source_identity, content_sha256, arrived_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    integration_id,
                    ordinal,
                    event.id,
                    event.seq,
                    event.causal_id,
                    signature[1],
                    hashlib.sha256(
                        (event.content or "").encode("utf-8")
                    ).hexdigest(),
                    _ts(event.created_at),
                ),
            )
        placeholders = ",".join("?" for _ in ordered_ids)
        resolution_note = f"dendritic_integration:{integration_id}"
        updated = conn.execute(
            "UPDATE causal_events SET status = 'reconciled', "
            "resolution = 'superseded', resolution_note = ?, "
            "updated_at = ?, settled_at = ? "
            f"WHERE id IN ({placeholders}) AND status = 'queued' "
            "AND attempts = 0",
            (resolution_note, _ts(now), _ts(now), *ordered_ids),
        )
        if updated.rowcount != len(ordered):
            raise DendriticWindowConflictError(
                "dendritic inputs changed before integration commit"
            )
        row = conn.execute(
            f"SELECT {_DENDRITIC_INTEGRATION_COLUMNS} "
            "FROM dendritic_integrations WHERE id = ?",
            (integration_id,),
        ).fetchone()
        assert row is not None
        integration = self._row_to_dendritic_integration(conn, row)
        aggregate = self._get_event_uncommitted(conn, aggregate.id)
        assert aggregate is not None
        return integration, aggregate

    def materialize_dendritic_window(
        self,
        window: DendriticReadyWindow,
        integration_event_groups: Iterable[Iterable[str]],
        *,
        runtime_fence: RuntimeFence | None = None,
    ) -> tuple[
        DendriticWindow,
        tuple[tuple[DendriticIntegration, CausalEvent], ...],
    ]:
        """Atomically seal one closed cohort and all many-to-one nexuses."""

        if not isinstance(window, DendriticReadyWindow):
            raise TypeError("window must be a DendriticReadyWindow")
        if isinstance(integration_event_groups, (str, bytes, bytearray)):
            raise ValueError("integration_event_groups must be an iterable")
        groups = tuple(
            tuple(_require_id(value, "event_id") for value in group)
            for group in integration_event_groups
        )
        consumed: set[str] = set()
        for group in groups:
            if not 2 <= len(group) <= MAX_DENDRITIC_INTEGRATION_MEMBERS:
                raise ValueError(
                    "dendritic integration requires between 2 and "
                    f"{MAX_DENDRITIC_INTEGRATION_MEMBERS} events"
                )
            if len(set(group)) != len(group):
                raise ValueError("dendritic integration event ids must be unique")
            if not set(group) <= set(window.event_ids):
                raise ValueError("dendritic integration must be inside its window")
            if consumed.intersection(group):
                raise ValueError("dendritic integration groups cannot overlap")
            consumed.update(group)

        event_set_sha256 = self._dendritic_event_set_sha256(window.event_ids)
        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)
            existing_row = conn.execute(
                f"SELECT {_DENDRITIC_WINDOW_COLUMNS} FROM dendritic_windows "
                "WHERE event_set_sha256 = ?",
                (event_set_sha256,),
            ).fetchone()
            if existing_row is not None:
                durable_window = self._row_to_dendritic_window(conn, existing_row)
                expected_id = self._dendritic_window_id(
                    world_id=durable_window.world_id,
                    ready=window,
                )
                if (
                    durable_window.id != expected_id
                    or durable_window.formation_engram_id != window.engram_id
                    or durable_window.policy_version != window.policy_version
                    or durable_window.event_set_sha256 != event_set_sha256
                    or durable_window.event_count != len(window.event_ids)
                    or durable_window.base_silence_threshold_seconds
                    != float(window.base_silence_threshold_seconds)
                    or durable_window.base_max_wait_seconds
                    != float(window.base_max_wait_seconds)
                    or durable_window.wait_modifier
                    != float(window.wait_modifier)
                    or durable_window.silence_threshold_seconds
                    != float(window.silence_threshold_seconds)
                    or durable_window.max_wait_seconds
                    != float(window.max_wait_seconds)
                    or durable_window.window_opened_at != window.opened_at
                    or durable_window.last_input_at != window.last_input_at
                    or durable_window.window_closed_at != window.closed_at
                    or durable_window.observed_at > window.observed_at
                    or tuple(member.event_id for member in durable_window.members)
                    != window.event_ids
                    or tuple(member.event_seq for member in durable_window.members)
                    != window.event_seqs
                ):
                    raise CausalTransitionError(
                        "dendritic window digest collision or retry drift"
                    )
                rows = conn.execute(
                    f"SELECT {_DENDRITIC_INTEGRATION_COLUMNS} "
                    "FROM dendritic_integrations integration JOIN "
                    "dendritic_integration_windows binding "
                    "ON binding.integration_id = integration.id "
                    "WHERE binding.window_id = ? ORDER BY integration.created_at, "
                    "integration.id",
                    (durable_window.id,),
                ).fetchall()
                existing_results: dict[
                    frozenset[str], tuple[DendriticIntegration, CausalEvent]
                ] = {}
                for row in rows:
                    integration = self._row_to_dendritic_integration(conn, row)
                    aggregate = self._get_event_uncommitted(
                        conn,
                        integration.aggregate_event_id,
                    )
                    if aggregate is None:
                        raise CausalTransitionError(
                            "dendritic integration aggregate is missing"
                        )
                    existing_results[
                        frozenset(member.event_id for member in integration.members)
                    ] = (integration, aggregate)
                requested_sets = tuple(frozenset(group) for group in groups)
                if set(existing_results) != set(requested_sets):
                    raise CausalTransitionError(
                        "dendritic window retry changed integration groups"
                    )
                return durable_window, tuple(
                    existing_results[group] for group in requested_sets
                )

            events = self._validate_ready_dendritic_window_uncommitted(
                conn,
                window,
            )
            by_id = {event.id: event for event in events}
            world_id = events[0].world_id
            window_id = self._dendritic_window_id(
                world_id=world_id,
                ready=window,
            )
            observation_row = conn.execute(
                "SELECT MAX(seq) FROM causal_events WHERE world_id = ?",
                (world_id,),
            ).fetchone()
            observed_event_seq = (
                observation_row[0] if observation_row is not None else None
            )
            if (
                type(observed_event_seq) is not int
                or observed_event_seq < max(window.event_seqs)
            ):
                raise DendriticWindowConflictError(
                    "dendritic window lacks a valid causal observation watermark"
                )
            now = max(_now(), window.observed_at)
            conn.execute(
                "INSERT INTO dendritic_windows ("
                "id, world_id, formation_engram_id, policy_version, "
                "event_set_sha256, event_count, "
                "base_silence_threshold_seconds, base_max_wait_seconds, "
                "wait_modifier, silence_threshold_seconds, max_wait_seconds, "
                "window_opened_at, last_input_at, window_closed_at, "
                "observed_at, observed_event_seq, created_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    window_id,
                    world_id,
                    window.engram_id,
                    window.policy_version,
                    event_set_sha256,
                    len(events),
                    float(window.base_silence_threshold_seconds),
                    float(window.base_max_wait_seconds),
                    float(window.wait_modifier),
                    float(window.silence_threshold_seconds),
                    float(window.max_wait_seconds),
                    _ts(window.opened_at),
                    _ts(window.last_input_at),
                    _ts(window.closed_at),
                    _ts(window.observed_at),
                    observed_event_seq,
                    _ts(now),
                ),
            )
            for ordinal, event in enumerate(events):
                conn.execute(
                    "INSERT INTO dendritic_window_members ("
                    "window_id, ordinal, event_id, event_seq, arrived_at"
                    ") VALUES (?, ?, ?, ?, ?)",
                    (
                        window_id,
                        ordinal,
                        event.id,
                        event.seq,
                        _ts(event.created_at),
                    ),
                )
            results = tuple(
                self._materialize_dendritic_group_uncommitted(
                    conn,
                    window_id=window_id,
                    window_opened_at=window.opened_at,
                    window_closed_at=window.closed_at,
                    events=tuple(by_id[event_id] for event_id in group),
                    now=now,
                )
                for group in groups
            )
            durable_row = conn.execute(
                f"SELECT {_DENDRITIC_WINDOW_COLUMNS} FROM dendritic_windows "
                "WHERE id = ?",
                (window_id,),
            ).fetchone()
            assert durable_row is not None
            return self._row_to_dendritic_window(conn, durable_row), results

    def materialize_dendritic_integration(
        self,
        event_ids: Iterable[str],
        *,
        window: DendriticReadyWindow,
        runtime_fence: RuntimeFence | None = None,
    ) -> tuple[DendriticIntegration, CausalEvent]:
        """Compatibility wrapper for one nexus inside an explicit window."""

        _window, results = self.materialize_dendritic_window(
            window,
            (event_ids,),
            runtime_fence=runtime_fence,
        )
        return results[0]

    def get_dendritic_integration(
        self,
        integration_id: str,
    ) -> DendriticIntegration | None:
        integration_id = _require_id(integration_id, "integration_id")
        with self._storage._lock:
            row = self._storage._conn.execute(
                f"SELECT {_DENDRITIC_INTEGRATION_COLUMNS} "
                "FROM dendritic_integrations WHERE id = ?",
                (integration_id,),
            ).fetchone()
            return (
                self._row_to_dendritic_integration(self._storage._conn, row)
                if row is not None
                else None
            )

    def get_dendritic_integration_for_event(
        self,
        event_id: str,
    ) -> DendriticIntegration | None:
        """Resolve either a member input or the aggregate root."""

        event_id = _require_id(event_id, "event_id")
        with self._storage._lock:
            row = self._storage._conn.execute(
                f"SELECT {_DENDRITIC_INTEGRATION_COLUMNS} "
                "FROM dendritic_integrations i WHERE i.aggregate_event_id = ? "
                "OR EXISTS (SELECT 1 FROM dendritic_integration_members m "
                "WHERE m.integration_id = i.id AND m.event_id = ?) "
                "OR EXISTS (SELECT 1 FROM causal_events e WHERE e.id = ? "
                "AND e.causal_id = i.aggregate_event_id) "
                "LIMIT 1",
                (event_id, event_id, event_id),
            ).fetchone()
            return (
                self._row_to_dendritic_integration(self._storage._conn, row)
                if row is not None
                else None
            )

    def causal_amplification(
        self,
        causal_id: str,
        *,
        world_id: str | None = None,
        observed_at: datetime | None = None,
    ) -> CausalAmplificationSnapshot | None:
        """Return one complete read-only causal-chain projection."""

        with self._storage._lock:
            return read_causal_amplification(
                self._storage._conn,
                causal_id,
                world_id=world_id,
                observed_at=observed_at,
            )

    def claim_next_event(self, engram_id: str | None = None) -> CausalEvent | None:
        """Return the oldest claimable event; begin_turn performs the CAS."""

        clauses = [
            "e.status = 'queued'",
            "e.kind NOT IN (" + ",".join(
                f"'{kind}'" for kind in sorted(_NON_CLAIMABLE_ROOT_KINDS)
            ) + ")",
            "NOT EXISTS (SELECT 1 FROM harness_turns t "
            "WHERE t.engram_id = e.engram_id AND t.state = 'running')",
        ]
        params: list[Any] = []
        if engram_id is not None:
            clauses.append("e.engram_id = ?")
            params.append(_require_id(engram_id, "engram_id"))
        with self._storage._lock:
            after_seq = 0
            while True:
                rows = self._storage._conn.execute(
                    f"SELECT {_EVENT_COLUMNS} FROM causal_events e WHERE "
                    + " AND ".join([*clauses, "e.seq > ?"])
                    + " ORDER BY e.seq ASC LIMIT 100",
                    [*params, after_seq],
                ).fetchall()
                if not rows:
                    return None
                for row in rows:
                    event = self._row_to_event(row)
                    if not causal_turn_violation_codes(event):
                        return event
                last_seq = rows[-1][0]
                if not isinstance(last_seq, int) or len(rows) < 100:
                    return None
                after_seq = last_seq

    # ── Non-turn child and external-effect lifecycle ────────────

    def record_child(
        self,
        parent_event_id: str,
        *,
        kind: CausalEventKind | str,
        domain: CausalEventDomain | str,
        source: CausalEventSource | str,
        status: CausalEventStatus | str = CausalEventStatus.SETTLED,
        content: str | None = None,
        metadata: dict[str, Any] | None = None,
        engram_id: str | None = None,
        center_id: str | None = None,
        flow: CausalEventFlow | str | None = None,
        event_id: str | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> CausalEvent:
        """Record a terminal fact without adding a claimable turn root."""

        parent_event_id = _require_id(parent_event_id, "parent_event_id")
        child_status = _enum_value(status, CausalEventStatus, "status")
        if child_status not in _TERMINAL_CHILD_STATES:
            raise CausalTransitionError(
                "record_child only accepts settled, failed, or cancelled"
            )
        engram_id = _optional_id(engram_id, "engram_id")
        center_id = _optional_id(center_id, "center_id")
        event_id = _require_id(event_id or _uuid(), "event_id")
        now = _now()
        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)
            parent = self._get_event_uncommitted(conn, parent_event_id)
            if parent is None:
                raise KeyError(f"unknown parent event: {parent_event_id}")
            engram_id = engram_id or parent.engram_id
            center_id = self._inherit_parent_center(parent, center_id)
            self._ensure_references(conn, engram_id, center_id)
            child = CausalEvent(
                id=event_id,
                world_id=parent.world_id,
                causal_id=parent.causal_id,
                parent_event_id=parent.id,
                engram_id=engram_id,
                center_id=center_id,
                flow=flow,
                domain=domain,
                kind=kind,
                source=source,
                status=CausalEventStatus(child_status),
                content=content,
                metadata=metadata if metadata is not None else {},
                created_at=now,
                updated_at=now,
                started_at=now,
                settled_at=now,
            )
            self._insert_event_uncommitted(conn, child)
            return self._get_event_uncommitted(conn, child.id)  # type: ignore[return-value]

    def begin_effect(
        self,
        parent_event_id: str,
        *,
        kind: CausalEventKind | str = CausalEventKind.HABITAT_ACTION,
        domain: CausalEventDomain | str = CausalEventDomain.HABITAT,
        source: CausalEventSource | str = CausalEventSource.SELF,
        idempotency_key: str,
        content: str | None = None,
        metadata: dict[str, Any] | None = None,
        engram_id: str | None = None,
        center_id: str | None = None,
        flow: CausalEventFlow | str | None = None,
        event_id: str | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> CausalEvent:
        """Durably mark an external side effect running before executing it."""

        parent_event_id = _require_id(parent_event_id, "parent_event_id")
        idempotency_key = _require_id(idempotency_key, "idempotency_key")
        explicit_event_id = event_id is not None
        event_id = (
            _uuid() if event_id is None else _require_id(event_id, "event_id")
        )
        engram_id = _optional_id(engram_id, "engram_id")
        center_id = _optional_id(center_id, "center_id")
        normalized_kind = _enum_value(kind, CausalEventKind, "kind")
        normalized_domain = _enum_value(domain, CausalEventDomain, "domain")
        normalized_source = _enum_value(source, CausalEventSource, "source")
        normalized_flow = (
            None
            if flow is None
            else _enum_value(flow, CausalEventFlow, "flow")
        )
        now = _now()
        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)
            parent = self._get_event_uncommitted(conn, parent_event_id)
            if parent is None:
                raise KeyError(f"unknown parent event: {parent_event_id}")
            effective_engram_id = engram_id or parent.engram_id
            effective_center_id = self._inherit_parent_center(parent, center_id)
            existing_row = conn.execute(
                f"SELECT {_EVENT_COLUMNS} FROM causal_events "
                "WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing_row is not None:
                existing = self._row_to_event(existing_row)
                identity_matches = (
                    existing.parent_event_id == parent.id
                    and existing.world_id == parent.world_id
                    and existing.causal_id == parent.causal_id
                    and existing.kind.value == normalized_kind
                    and existing.domain.value == normalized_domain
                    and existing.source.value == normalized_source
                    and existing.engram_id == effective_engram_id
                    and existing.center_id == effective_center_id
                    and (
                        existing.flow.value if existing.flow is not None else None
                    )
                    == normalized_flow
                    and (not explicit_event_id or existing.id == event_id)
                )
                if not identity_matches:
                    raise CausalTransitionError(
                        "idempotency_key is already bound to a different effect identity"
                    )
                if existing.status is CausalEventStatus.QUEUED:
                    raise CausalTransitionError(
                        "idempotency_key is already bound to a queued turn root"
                    )
                return existing
            engram_id = effective_engram_id
            center_id = effective_center_id
            self._ensure_references(conn, engram_id, center_id)
            effect = CausalEvent(
                id=event_id,
                world_id=parent.world_id,
                causal_id=parent.causal_id,
                parent_event_id=parent.id,
                engram_id=engram_id,
                center_id=center_id,
                flow=normalized_flow,
                domain=normalized_domain,
                kind=normalized_kind,
                source=normalized_source,
                status=CausalEventStatus.RUNNING,
                content=content,
                metadata=metadata if metadata is not None else {},
                idempotency_key=idempotency_key,
                attempts=1,
                created_at=now,
                updated_at=now,
                started_at=now,
            )
            self._insert_event_uncommitted(conn, effect)
            return self._get_event_uncommitted(conn, effect.id)  # type: ignore[return-value]

    def settle_effect(
        self,
        effect_id: str,
        *,
        consequence: str | None = None,
        consequence_kind: CausalEventKind | str = CausalEventKind.HABITAT_CONSEQUENCE,
        consequence_domain: CausalEventDomain | str = CausalEventDomain.HABITAT,
        consequence_source: CausalEventSource | str = CausalEventSource.HABITAT,
        consequence_flow: CausalEventFlow | str | None = None,
        consequence_metadata: dict[str, Any] | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> CausalEvent:
        """Atomically settle an effect and record its direct consequence."""

        return self._finish_effect(
            effect_id,
            status=CausalEventStatus.SETTLED,
            error_code=None,
            consequence=consequence,
            consequence_kind=consequence_kind,
            consequence_domain=consequence_domain,
            consequence_source=consequence_source,
            consequence_flow=consequence_flow,
            consequence_metadata=consequence_metadata,
            runtime_fence=runtime_fence,
        )

    def fail_effect(
        self,
        effect_id: str,
        *,
        prompt_accepted: bool | None,
        error_code: str | None = None,
        consequence: str | None = None,
        consequence_kind: CausalEventKind | str = CausalEventKind.HABITAT_CONSEQUENCE,
        consequence_domain: CausalEventDomain | str = CausalEventDomain.HABITAT,
        consequence_source: CausalEventSource | str = CausalEventSource.HABITAT,
        consequence_flow: CausalEventFlow | str | None = None,
        consequence_metadata: dict[str, Any] | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> CausalEvent:
        """Fail an effect using true/false/null acceptance semantics."""

        if prompt_accepted is not None and not isinstance(prompt_accepted, bool):
            raise ValueError("prompt_accepted must be true, false, or null")
        return self._finish_effect(
            effect_id,
            status=CausalEventStatus(_EFFECT_ACCEPTANCE_STATES[prompt_accepted]),
            error_code=_safe_effect_error_code(error_code),
            consequence=consequence,
            consequence_kind=consequence_kind,
            consequence_domain=consequence_domain,
            consequence_source=consequence_source,
            consequence_flow=consequence_flow,
            consequence_metadata=consequence_metadata,
            runtime_fence=runtime_fence,
        )

    def _finish_effect(
        self,
        effect_id: str,
        *,
        status: CausalEventStatus,
        error_code: str | None,
        consequence: str | None,
        consequence_kind: CausalEventKind | str,
        consequence_domain: CausalEventDomain | str,
        consequence_source: CausalEventSource | str,
        consequence_flow: CausalEventFlow | str | None,
        consequence_metadata: dict[str, Any] | None,
        runtime_fence: RuntimeFence | None,
    ) -> CausalEvent:
        effect_id = _require_id(effect_id, "effect_id")
        now = _now()
        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)
            row = conn.execute(
                f"SELECT {_EVENT_COLUMNS} FROM causal_events WHERE id = ?",
                (effect_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown effect: {effect_id}")
            effect = self._row_to_event(row)
            if effect.status is not CausalEventStatus.RUNNING:
                if effect.status is status:
                    return effect
                raise CausalTransitionError(
                    f"effect {effect_id} is {effect.status.value}, expected running"
                )
            terminal_metadata = dict(effect.metadata)
            if error_code is not None:
                existing_error_code = terminal_metadata.get("error_code")
                if (
                    existing_error_code is not None
                    and existing_error_code != error_code
                ):
                    raise CausalTransitionError(
                        "effect metadata already contains a different error_code"
                    )
                terminal_metadata["error_code"] = error_code
            terminal_metadata = _metadata(terminal_metadata)
            child: CausalEvent | None = None
            if consequence is not None:
                child = CausalEvent(
                    id=_uuid(),
                    world_id=effect.world_id,
                    causal_id=effect.causal_id,
                    parent_event_id=effect.id,
                    engram_id=effect.engram_id,
                    center_id=effect.center_id,
                    flow=consequence_flow,
                    domain=consequence_domain,
                    kind=consequence_kind,
                    source=consequence_source,
                    status=CausalEventStatus.SETTLED,
                    content=consequence,
                    metadata=consequence_metadata or {},
                    created_at=now,
                    updated_at=now,
                    started_at=now,
                    settled_at=now,
                )
                self._insert_event_uncommitted(conn, child)
            updated = conn.execute(
                "UPDATE causal_events SET status = ?, metadata = ?, "
                "updated_at = ?, settled_at = ?, resolution = NULL "
                "WHERE id = ? AND status = 'running'",
                (
                    status.value,
                    json.dumps(terminal_metadata, ensure_ascii=False, sort_keys=True),
                    _ts(now),
                    _ts(now),
                    effect_id,
                ),
            )
            if updated.rowcount != 1:
                raise CausalTransitionError(
                    f"effect {effect_id} changed before terminalization"
                )
            return self._get_event_uncommitted(conn, effect_id)  # type: ignore[return-value]

    # ── Harness turn lifecycle ──────────────────────────────────

    def begin_turn(
        self,
        event_id: str,
        engram_id: str,
        message: Message | str | None = None,
        *,
        session_id: str | None = None,
        session_file: str | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> HarnessTurn:
        event_id = _require_id(event_id, "event_id")
        engram_id = _require_id(engram_id, "engram_id")
        session_id = _optional_id(session_id, "session_id")
        session_file = _optional_id(session_file, "session_file")
        input_message = self._coerce_input_message(message)
        turn_id = _uuid()
        now = _now()

        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)
            event = self._get_event_uncommitted(conn, event_id)
            if event is None:
                raise KeyError(f"unknown event: {event_id}")
            if event.status is not CausalEventStatus.QUEUED:
                raise CausalTransitionError(
                    f"event {event_id} is {event.status.value}, expected queued"
                )
            try:
                assert_causal_turn(event)
            except CausalFlowInvariantError as exc:
                raise CausalTransitionError(str(exc)) from exc
            if event.engram_id is not None and event.engram_id != engram_id:
                raise CausalTransitionError(
                    "event is already assigned to another Engram"
                )
            generation_row = conn.execute(
                "SELECT id FROM generation_transitions "
                "WHERE predecessor_id = ? AND state IN "
                "('prepared', 'summarizing', 'rotating', 'uncertain') "
                "ORDER BY created_at DESC, id DESC LIMIT 1",
                (engram_id,),
            ).fetchone()
            if generation_row is not None:
                generation_id = str(generation_row[0])
                is_own_summary = (
                    event.metadata.get("generation_id") == generation_id
                    and event.metadata.get("generation_stage") == "summary"
                )
                if not is_own_summary:
                    raise CausalTransitionError(
                        f"Engram {engram_id} is held by generation "
                        f"{generation_id}"
                    )
            self._assert_task_relationship_turn_admission_uncommitted(
                conn,
                event,
                engram_id,
            )
            self._ensure_references(conn, engram_id, None)
            running = conn.execute(
                "SELECT 1 FROM harness_turns "
                "WHERE engram_id = ? AND state = 'running' LIMIT 1",
                (engram_id,),
            ).fetchone()
            if running is not None:
                raise CausalTransitionError(
                    f"Engram {engram_id} already has a running turn"
                )

            persisted_cursor = self._load_cursor_uncommitted(conn, engram_id)
            message_count = self._message_count_uncommitted(conn, engram_id)
            if persisted_cursor > message_count:
                raise CausalTransitionError(
                    "harness input cursor exceeds the indexed message count"
                )

            # An explicit-refusal retry reuses the exact message projection
            # created by the first attempt.  The global messages.id is kept in
            # the turn row because get_session intentionally exposes only the
            # per-Engram ordered projection, not storage IDs.
            retry_row = conn.execute(
                f"SELECT {_TURN_COLUMNS} FROM harness_turns "
                "WHERE event_id = ? AND engram_id = ? AND state = 'failed' "
                "AND prompt_accepted = 0 AND input_message_id IS NOT NULL "
                "ORDER BY started_at DESC, id DESC LIMIT 1",
                (event_id, engram_id),
            ).fetchone()
            retry_projection = (
                self._row_to_turn(retry_row) if retry_row is not None else None
            )
            if retry_projection is not None:
                if retry_projection.cursor_before != persisted_cursor:
                    raise CausalTransitionError(
                        "retry projection would skip or rewind persisted messages"
                    )
                if retry_projection.cursor_after > message_count:
                    raise CausalTransitionError(
                        "retry projection exceeds the indexed message count"
                    )
                if input_message is not None:
                    stored_input = conn.execute(
                        "SELECT role, content, source_engram_id FROM messages "
                        "WHERE id = ?",
                        (retry_projection.input_message_id,),
                    ).fetchone()
                    if stored_input is None or (
                        stored_input[0] != input_message.role.value
                        or stored_input[1] != input_message.content
                        or stored_input[2] != input_message.source_engram_id
                    ):
                        raise CausalTransitionError(
                            "retry message differs from the persisted event input"
                        )
                cursor_before = persisted_cursor
                cursor_after = retry_projection.cursor_after
                input_message_id = retry_projection.input_message_id
            else:
                cursor_before = persisted_cursor
                input_message_id = None
                if input_message is not None:
                    input_message_id = self._storage._insert_message(
                        engram_id, input_message
                    )
                cursor_after = self._message_count_uncommitted(conn, engram_id)

            updated = conn.execute(
                "UPDATE causal_events SET status = 'running', engram_id = ?, "
                "attempts = attempts + 1, started_at = ?, updated_at = ? "
                "WHERE id = ? AND status = 'queued'",
                (engram_id, _ts(now), _ts(now), event_id),
            )
            if updated.rowcount != 1:
                raise CausalTransitionError(
                    f"event {event_id} changed before it could be claimed"
                )
            turn = HarnessTurn(
                id=turn_id,
                event_id=event_id,
                engram_id=engram_id,
                state=HarnessTurnState.RUNNING,
                cursor_before=cursor_before,
                cursor_after=cursor_after,
                input_message_id=input_message_id,
                prompt_accepted=None,
                session_id=session_id,
                session_file=session_file,
                started_at=now,
                updated_at=now,
            )
            self._insert_turn_uncommitted(conn, turn)

        return self.get_turn(turn_id)  # type: ignore[return-value]

    begin_harness_turn = begin_turn

    def settle_turn(
        self,
        turn_id: str,
        assistant: Any = None,
        *,
        result: Any = None,
        usage: Mapping[str, Any] | None = None,
        safe_trace: Mapping[str, Any] | None = None,
        trace: Mapping[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        runtime_fence: RuntimeFence | None = None,
        engram_activity: EngramPulseActivity | None = None,
    ) -> tuple[HarnessTurn, CausalEvent]:
        turn_id = _require_id(turn_id, "turn_id")
        if assistant is not None and result is not None:
            raise ValueError("provide assistant or result, not both")
        assistant = assistant if assistant is not None else result
        result_session_id = _optional_id(
            getattr(assistant, "session_id", None),
            "session_id",
        )
        result_session_file = _optional_id(
            getattr(assistant, "session_file", None),
            "session_file",
        )
        assistant_message = self._coerce_assistant_message(assistant)
        safe_metadata = self._merge_safe_metadata(
            metadata=metadata, usage=usage, safe_trace=safe_trace or trace
        )
        now = _now()

        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)
            row = conn.execute(
                f"SELECT {_TURN_COLUMNS} FROM harness_turns WHERE id = ?",
                (turn_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown turn: {turn_id}")
            turn = self._row_to_turn(row)
            if turn.state is not HarnessTurnState.RUNNING:
                raise CausalTransitionError(
                    f"turn {turn_id} is {turn.state.value}, expected running"
                )
            event = self._get_event_uncommitted(conn, turn.event_id)
            if event is None:
                raise CausalTransitionError(
                    f"turn {turn_id} references a missing event"
                )
            if event.status is not CausalEventStatus.RUNNING:
                raise CausalTransitionError(
                    f"event {event.id} is {event.status.value}, expected running"
                )
            if (
                turn.session_id is not None
                and result_session_id is not None
                and turn.session_id != result_session_id
            ):
                raise CausalTransitionError(
                    "settled Harness session id differs from the claimed turn"
                )
            if (
                turn.session_file is not None
                and result_session_file is not None
                and turn.session_file != result_session_file
            ):
                raise CausalTransitionError(
                    "settled Harness session file differs from the claimed turn"
                )
            settled_session_id = result_session_id or turn.session_id
            settled_session_file = result_session_file or turn.session_file

            # The following four writes intentionally remain in this one
            # transaction.  A fault in any helper rolls all of them back.
            self._storage._insert_message(turn.engram_id, assistant_message)
            cursor_after = self._message_count_uncommitted(conn, turn.engram_id)
            self._save_cursor_uncommitted(conn, turn.engram_id, cursor_after)

            result_event = CausalEvent(
                world_id=event.world_id,
                causal_id=event.causal_id,
                parent_event_id=event.id,
                engram_id=turn.engram_id,
                center_id=event.center_id,
                flow=None,
                domain=CausalEventDomain.HARNESS,
                kind=CausalEventKind.ASSISTANT_RESULT,
                source=CausalEventSource.SELF,
                status=CausalEventStatus.SETTLED,
                content=assistant_message.content,
                metadata=safe_metadata,
                attempts=0,
                created_at=now,
                updated_at=now,
                started_at=now,
                settled_at=now,
            )
            self._insert_event_uncommitted(conn, result_event)
            result_event = self._get_event_uncommitted(conn, result_event.id)
            assert result_event is not None

            updated_turn = conn.execute(
                "UPDATE harness_turns SET state = 'settled', cursor_after = ?, "
                "prompt_accepted = 1, session_id = ?, session_file = ?, "
                "result_event_id = ?, updated_at = ?, "
                "settled_at = ? WHERE id = ? AND state = 'running'",
                (
                    cursor_after,
                    settled_session_id,
                    settled_session_file,
                    result_event.id,
                    _ts(now),
                    _ts(now),
                    turn_id,
                ),
            )
            if updated_turn.rowcount != 1:
                raise CausalTransitionError(
                    f"turn {turn_id} changed before settlement"
                )
            updated_event = conn.execute(
                "UPDATE causal_events SET status = 'settled', updated_at = ?, "
                "settled_at = ? WHERE id = ? AND status = 'running'",
                (_ts(now), _ts(now), event.id),
            )
            if updated_event.rowcount != 1:
                raise CausalTransitionError(
                    f"event {event.id} changed before settlement"
                )
            self._update_engram_pulse_activity_uncommitted(
                conn,
                turn.engram_id,
                engram_activity,
            )

        settled_turn = self.get_turn(turn_id)
        assert settled_turn is not None
        return settled_turn, result_event

    settle_harness_turn = settle_turn

    def fail_turn(
        self,
        turn_id: str,
        acceptance: bool | None,
        code: str | None = None,
        phase: str | None = None,
        retry_allowed: bool = False,
        runtime_fence: RuntimeFence | None = None,
    ) -> CausalEvent:
        turn_id = _require_id(turn_id, "turn_id")
        if acceptance is not None and not isinstance(acceptance, bool):
            raise ValueError("acceptance must be true, false, or null")
        if not isinstance(retry_allowed, bool):
            raise ValueError("retry_allowed must be a boolean")
        code = _safe_code(code, "harness_error")
        phase = _safe_code(phase, "unknown")
        now = _now()

        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)
            row = conn.execute(
                f"SELECT {_TURN_COLUMNS} FROM harness_turns WHERE id = ?",
                (turn_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown turn: {turn_id}")
            turn = self._row_to_turn(row)
            if turn.state is not HarnessTurnState.RUNNING:
                raise CausalTransitionError(
                    f"turn {turn_id} is {turn.state.value}, expected running"
                )
            event = self._get_event_uncommitted(conn, turn.event_id)
            if event is None:
                raise CausalTransitionError(
                    f"turn {turn_id} references a missing event"
                )
            if event.status is not CausalEventStatus.RUNNING:
                raise CausalTransitionError(
                    f"event {event.id} is {event.status.value}, expected running"
                )

            if acceptance is False:
                event_status = (
                    CausalEventStatus.QUEUED
                    if retry_allowed
                    else CausalEventStatus.FAILED
                )
                event_settled_at = None if retry_allowed else _ts(now)
                # The candidate submitted boundary remains attached to this
                # failed turn so a retry can reuse its input projection.  The
                # persisted component cursor is intentionally not advanced.
                cursor_after = turn.cursor_after
            else:
                event_status = CausalEventStatus.UNCERTAIN
                event_settled_at = _ts(now)
                cursor_after = turn.cursor_after
                self._save_cursor_uncommitted(conn, turn.engram_id, cursor_after)

            updated_turn = conn.execute(
                "UPDATE harness_turns SET state = ?, cursor_after = ?, "
                "prompt_accepted = ?, error_code = ?, error_phase = ?, "
                "updated_at = ?, settled_at = ? "
                "WHERE id = ? AND state = 'running'",
                (
                    HarnessTurnState.FAILED.value
                    if acceptance is False
                    else HarnessTurnState.UNCERTAIN.value,
                    cursor_after,
                    None if acceptance is None else int(acceptance),
                    code,
                    phase,
                    _ts(now),
                    _ts(now),
                    turn_id,
                ),
            )
            if updated_turn.rowcount != 1:
                raise CausalTransitionError(
                    f"turn {turn_id} changed before failure handling"
                )
            updated_event = conn.execute(
                "UPDATE causal_events SET status = ?, updated_at = ?, "
                "settled_at = ? WHERE id = ? AND status = 'running'",
                (event_status.value, _ts(now), event_settled_at, event.id),
            )
            if updated_event.rowcount != 1:
                raise CausalTransitionError(
                    f"event {event.id} changed before failure handling"
                )
            return self._get_event_uncommitted(conn, event.id)  # type: ignore[return-value]

    def fail_harness_turn(
        self,
        turn_id: str,
        acceptance_or_error: bool | None | Any = None,
        code: str | None = None,
        phase: str | None = None,
        retry_allowed: bool = False,
        runtime_fence: RuntimeFence | None = None,
    ) -> CausalEvent:
        """Compatibility adapter for the Feature Spec HarnessError shape."""

        if acceptance_or_error is not None and not isinstance(
            acceptance_or_error, bool
        ):
            error = acceptance_or_error
            acceptance = getattr(error, "prompt_accepted", None)
            code = code or getattr(error, "code", None)
            phase = phase or getattr(error, "phase", None)
            retry_allowed = bool(
                getattr(error, "retryable", getattr(error, "retry_allowed", False))
            )
        else:
            acceptance = acceptance_or_error
        return self.fail_turn(
            turn_id,
            acceptance=acceptance,
            code=code,
            phase=phase,
            retry_allowed=retry_allowed,
            runtime_fence=runtime_fence,
        )

    def get_turn(self, turn_id: str) -> HarnessTurn | None:
        turn_id = _require_id(turn_id, "turn_id")
        with self._storage._lock:
            row = self._storage._conn.execute(
                f"SELECT {_TURN_COLUMNS} FROM harness_turns WHERE id = ?",
                (turn_id,),
            ).fetchone()
            return self._row_to_turn(row) if row is not None else None

    @staticmethod
    def _assert_task_relationship_turn_admission_uncommitted(
        conn: sqlite3.Connection,
        event: CausalEvent,
        engram_id: str,
    ) -> None:
        """Linearize managed task consent with the queued→running claim."""

        if event.center_id is None:
            return
        relationship = conn.execute(
            "SELECT relationship.id, relationship.status, "
            "relationship.current_subject_engram_id, center.kind, "
            "center.status, center.focal_engram_id "
            "FROM task_relationships relationship "
            "JOIN activity_centers center ON center.id = relationship.center_id "
            "WHERE relationship.world_id = ? AND relationship.center_id = ?",
            (event.world_id, event.center_id),
        ).fetchone()
        if relationship is None:
            return
        if (
            relationship[1] != "active"
            or relationship[2] != engram_id
            or relationship[3] != "task"
            or relationship[4] != "active"
            or relationship[5] != engram_id
        ):
            raise CausalTransitionError(
                "task_relationship_not_active: managed task authority changed "
                "before Harness turn admission"
            )

    def get_turn_for_world(
        self,
        turn_id: str,
        world_id: str,
    ) -> HarnessTurn | None:
        """Return a turn only when its owning causal event belongs to ``world_id``.

        ``harness_turns`` intentionally does not duplicate ``world_id``.  The
        causal event is therefore the authority for World ownership.  Keep the
        turn lookup, event existence check, World predicate, and Engram
        consistency check in one SQLite statement so callers cannot assemble a
        security scope from separately observed rows.
        """

        turn_id = _require_id(turn_id, "turn_id")
        world_id = _require_id(world_id, "world_id")
        with self._storage._lock:
            row = self._storage._conn.execute(
                f"SELECT {_TURN_COLUMNS} FROM harness_turns "
                "WHERE id = ? AND EXISTS ("
                "SELECT 1 FROM causal_events "
                "WHERE causal_events.id = harness_turns.event_id "
                "AND causal_events.world_id = ? "
                "AND causal_events.engram_id = harness_turns.engram_id)",
                (turn_id, world_id),
            ).fetchone()
            return self._row_to_turn(row) if row is not None else None

    def list_turns(
        self,
        *,
        engram_id: str | None = None,
        state: HarnessTurnState | str | None = None,
    ) -> list[HarnessTurn]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        if engram_id is not None:
            clauses.append("engram_id = ?")
            params.append(_require_id(engram_id, "engram_id"))
        if state is not None:
            clauses.append("state = ?")
            params.append(_enum_value(state, HarnessTurnState, "state"))
        with self._storage._lock:
            rows = self._storage._conn.execute(
                f"SELECT {_TURN_COLUMNS} FROM harness_turns WHERE "
                + " AND ".join(clauses)
                # Windows clocks can return the same microsecond for adjacent
                # inserts.  rowid is the durable insertion tiebreaker; random
                # turn ids are identity, not chronology.
                + " ORDER BY started_at ASC, rowid ASC",
                params,
            ).fetchall()
            return [self._row_to_turn(row) for row in rows]

    def get_running_turn(self, engram_id: str) -> HarnessTurn | None:
        """Return the sole running turn, failing closed on corruption."""

        engram_id = _require_id(engram_id, "engram_id")
        with self._storage._lock:
            rows = self._storage._conn.execute(
                f"SELECT {_TURN_COLUMNS} FROM harness_turns "
                "WHERE engram_id = ? AND state = 'running' "
                "ORDER BY started_at ASC, id ASC LIMIT 2",
                (engram_id,),
            ).fetchall()
            if len(rows) > 1:
                raise CausalTransitionError(
                    f"Engram {engram_id} has multiple running turns"
                )
            return self._row_to_turn(rows[0]) if rows else None

    def reconcile_event(
        self,
        event_id: str,
        *,
        action: str,
        note: str | None = None,
    ) -> tuple[CausalEvent, CausalEvent | None]:
        """Atomically resolve an uncertain event, optionally with a new child."""

        event_id = _require_id(event_id, "event_id")
        if not isinstance(action, str):
            raise ValueError("action must be acknowledge, cancel, or requeue")
        action = action.strip().casefold()
        if action not in {"acknowledge", "cancel", "requeue"}:
            raise ValueError("action must be acknowledge, cancel, or requeue")
        note = _safe_note(note)
        now = _now()
        with self._transaction() as conn:
            event = self._get_event_uncommitted(conn, event_id)
            if event is None:
                raise KeyError(f"unknown event: {event_id}")
            if event.status is not CausalEventStatus.UNCERTAIN:
                raise CausalTransitionError(
                    f"event {event_id} is {event.status.value}, expected uncertain"
                )
            resolution = {
                "acknowledge": CausalEventResolution.ACKNOWLEDGED,
                "cancel": CausalEventResolution.CANCELLED,
                "requeue": CausalEventResolution.SUPERSEDED,
            }[action]
            child: CausalEvent | None = None
            if action == "requeue":
                child = CausalEvent(
                    id=_uuid(),
                    world_id=event.world_id,
                    causal_id=event.causal_id,
                    parent_event_id=event.id,
                    engram_id=event.engram_id,
                    center_id=event.center_id,
                    flow=event.flow,
                    domain=event.domain,
                    kind=event.kind,
                    source=event.source,
                    status=CausalEventStatus.QUEUED,
                    content=event.content,
                    metadata=event.metadata,
                    attempts=0,
                    created_at=now,
                    updated_at=now,
                )
                self._insert_event_uncommitted(conn, child)
            updated = conn.execute(
                "UPDATE causal_events SET status = 'reconciled', "
                "resolution = ?, resolution_note = ?, updated_at = ?, "
                "settled_at = ? WHERE id = ? AND status = 'uncertain'",
                (resolution.value, note, _ts(now), _ts(now), event_id),
            )
            if updated.rowcount != 1:
                raise CausalTransitionError(
                    f"event {event_id} changed before reconciliation"
                )
            resolved = self._get_event_uncommitted(conn, event_id)
            assert resolved is not None
            return resolved, child

    # ── Recovery ─────────────────────────────────────────────────

    def recover_inflight(
        self,
        *,
        runtime_fence: RuntimeFence | None,
    ) -> RecoveryReport:
        now = _now()
        turn_ids: list[str] = []
        event_ids: list[str] = []
        generation_ids: list[str] = []
        effect_ids: list[str] = []
        with self._transaction(
            runtime_fence=runtime_fence,
            allow_recovery=True,
        ) as conn:
            # Runtime callers must prove the lease they are recovering under
            # in the same transaction as every terminalization below.  This
            # prevents a quiescing old process from classifying a successor
            # Runtime's live work after lease takeover.  Direct offline tools
            # may intentionally omit the optional fence.
            self._assert_runtime_fence_uncommitted(
                conn,
                runtime_fence,
                allow_recovery=True,
            )
            # Snapshot orphan running events before recovering turns.  A
            # running turn's event becomes an orphan only after the turn row is
            # terminalized; it must not be misclassified as an effect.
            orphan_effect_rows = conn.execute(
                f"SELECT {_EVENT_COLUMNS} FROM causal_events e "
                "WHERE e.status = 'running' AND NOT EXISTS ("
                "SELECT 1 FROM harness_turns t "
                "WHERE t.event_id = e.id AND t.state = 'running') "
                "AND NOT EXISTS (SELECT 1 FROM generation_transitions g "
                "WHERE g.event_id = e.id AND g.state NOT IN "
                "('committed', 'failed', 'uncertain')) "
                "ORDER BY e.seq ASC"
            ).fetchall()
            orphan_effects = [self._row_to_event(row) for row in orphan_effect_rows]
            running_rows = conn.execute(
                f"SELECT {_TURN_COLUMNS} FROM harness_turns "
                "WHERE state = 'running' ORDER BY started_at ASC, id ASC"
            ).fetchall()
            for row in running_rows:
                turn = self._row_to_turn(row)
                event = self._get_event_uncommitted(conn, turn.event_id)
                if event is None or event.status is not CausalEventStatus.RUNNING:
                    raise CausalTransitionError(
                        f"running turn {turn.id} is not paired with a running event"
                    )
                self._save_cursor_uncommitted(
                    conn, turn.engram_id, turn.cursor_after
                )
                updated_turn = conn.execute(
                    "UPDATE harness_turns SET state = 'uncertain', "
                    "prompt_accepted = NULL, error_code = ?, error_phase = ?, "
                    "updated_at = ?, settled_at = ? "
                    "WHERE id = ? AND state = 'running'",
                    (
                        "process_recovered",
                        "recovery",
                        _ts(now),
                        _ts(now),
                        turn.id,
                    ),
                )
                if updated_turn.rowcount != 1:
                    raise CausalTransitionError(
                        f"turn {turn.id} changed during recovery"
                    )
                updated_event = conn.execute(
                    "UPDATE causal_events SET status = 'uncertain', "
                    "updated_at = ?, settled_at = ? "
                    "WHERE id = ? AND status = 'running'",
                    (_ts(now), _ts(now), event.id),
                )
                if updated_event.rowcount != 1:
                    raise CausalTransitionError(
                        f"event {event.id} changed during recovery"
                    )
                turn_ids.append(turn.id)
                event_ids.append(event.id)

            for effect in orphan_effects:
                updated = conn.execute(
                    "UPDATE causal_events SET status = 'uncertain', "
                    "updated_at = ?, settled_at = ? WHERE id = ? "
                    "AND status = 'running'",
                    (_ts(now), _ts(now), effect.id),
                )
                if updated.rowcount != 1:
                    raise CausalTransitionError(
                        f"effect {effect.id} changed during recovery"
                    )
                effect_ids.append(effect.id)
                event_ids.append(effect.id)

            generation_rows = conn.execute(
                f"SELECT {_GENERATION_COLUMNS} FROM generation_transitions "
                "WHERE state NOT IN ('committed', 'failed', 'uncertain') "
                "ORDER BY created_at ASC, id ASC"
            ).fetchall()
            for row in generation_rows:
                generation = self._row_to_generation(row)
                updated = conn.execute(
                    "UPDATE generation_transitions SET state = 'uncertain', "
                    "error_code = ?, updated_at = ?, settled_at = ? "
                    "WHERE id = ? AND state NOT IN "
                    "('committed', 'failed', 'uncertain')",
                    (
                        "process_recovered",
                        _ts(now),
                        _ts(now),
                        generation.id,
                    ),
                )
                if updated.rowcount != 1:
                    raise CausalTransitionError(
                        f"generation {generation.id} changed during recovery"
                    )
                if generation.event_id is not None:
                    event_updated = conn.execute(
                        "UPDATE causal_events SET status = 'uncertain', "
                        "updated_at = ?, settled_at = ? WHERE id = ? "
                        "AND status = 'running'",
                        (_ts(now), _ts(now), generation.event_id),
                    )
                    if event_updated.rowcount != 1:
                        event = self._get_event_uncommitted(
                            conn, generation.event_id
                        )
                        if event is None or event.status is not CausalEventStatus.UNCERTAIN:
                            raise CausalTransitionError(
                                f"generation event {generation.event_id} changed during recovery"
                            )
                generation_ids.append(generation.id)

        return RecoveryReport(
            turn_ids=tuple(turn_ids),
            event_ids=tuple(event_ids),
            generation_ids=tuple(generation_ids),
            effect_ids=tuple(effect_ids),
        )

    recover = recover_inflight

    # ── Generation lifecycle ────────────────────────────────────

    def begin_generation(
        self,
        predecessor_id: str,
        *,
        generation_id: str | None = None,
        causal_id: str | None = None,
        summary_turn_id: str | None = None,
        parent_event_id: str | None = None,
        world_id: str | None = None,
        event_id: str | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> GenerationTransition:
        predecessor_id = _require_id(predecessor_id, "predecessor_id")
        generation_id = _require_id(generation_id or _uuid(), "generation_id")
        parent_event_id = _optional_id(parent_event_id, "parent_event_id")
        world_id = _require_id(world_id, "world_id") if world_id is not None else None
        event_id = _require_id(event_id or _uuid(), "event_id")
        summary_turn_id = _optional_id(summary_turn_id, "summary_turn_id")
        now = _now()
        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)
            self._ensure_references(conn, predecessor_id, None)
            parent = None
            if parent_event_id is not None:
                parent = self._get_event_uncommitted(conn, parent_event_id)
                if parent is None:
                    raise KeyError(f"unknown parent event: {parent_event_id}")
                if world_id is not None and parent.world_id != world_id:
                    raise ValueError("generation parent must belong to the same world")
                if causal_id is not None and causal_id != parent.causal_id:
                    raise ValueError("generation must preserve parent causal_id")
                world_id = parent.world_id
                causal_id = parent.causal_id
            world_id = _require_id(world_id or "default", "world_id")
            causal_id = _require_id(causal_id or event_id, "causal_id")
            if summary_turn_id is not None and conn.execute(
                "SELECT 1 FROM harness_turns WHERE id = ?", (summary_turn_id,)
            ).fetchone() is None:
                raise KeyError(f"unknown summary turn: {summary_turn_id}")
            if conn.execute(
                "SELECT 1 FROM generation_transitions WHERE predecessor_id = ? "
                "AND state IN ('prepared', 'summarizing', 'rotating', 'uncertain') "
                "LIMIT 1",
                (predecessor_id,),
            ).fetchone() is not None:
                raise CausalTransitionError(
                    f"predecessor {predecessor_id} already has a nonterminal or uncertain generation"
                )
            generation_event = CausalEvent(
                id=event_id,
                world_id=world_id,
                causal_id=causal_id,
                parent_event_id=parent_event_id,
                engram_id=predecessor_id,
                center_id=parent.center_id if parent is not None else None,
                flow=None,
                domain=CausalEventDomain.GENERATION,
                kind=CausalEventKind.GENERATION_TRANSITION,
                source=CausalEventSource.SELF,
                status=CausalEventStatus.RUNNING,
                metadata={
                    "generation_id": generation_id,
                    **(
                        {
                            "runtime_owner_id": runtime_fence.owner_id,
                            "runtime_lease_epoch": runtime_fence.epoch,
                        }
                        if runtime_fence is not None
                        else {}
                    ),
                },
                attempts=1,
                created_at=now,
                updated_at=now,
                started_at=now,
            )
            generation = GenerationTransition(
                id=generation_id,
                causal_id=causal_id,
                event_id=event_id,
                predecessor_id=predecessor_id,
                summary_turn_id=summary_turn_id,
                state=GenerationTransitionState.PREPARED,
                created_at=now,
                updated_at=now,
            )
            self._insert_event_uncommitted(conn, generation_event)
            self._insert_generation_uncommitted(conn, generation)
            return self._get_generation_uncommitted(conn, generation.id)  # type: ignore[return-value]

    def transition_generation(
        self,
        generation_id: str,
        state: GenerationTransitionState | str,
        *,
        successor_id: str | None = None,
        summary_turn_id: str | None = None,
        error_code: str | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> GenerationTransition:
        generation_id = _require_id(generation_id, "generation_id")
        next_state = _enum_value(state, GenerationTransitionState, "state")
        successor_id = _optional_id(successor_id, "successor_id")
        summary_turn_id = _optional_id(summary_turn_id, "summary_turn_id")
        error_code = _safe_code(error_code, "generation_error") if error_code else None
        now = _now()
        with self._transaction() as conn:
            return self._transition_generation_uncommitted(
                conn,
                generation_id,
                next_state,
                successor_id=successor_id,
                summary_turn_id=summary_turn_id,
                error_code=error_code,
                runtime_fence=runtime_fence,
                now=now,
            )

    def _transition_generation_uncommitted(
        self,
        conn: sqlite3.Connection,
        generation_id: str,
        next_state: str,
        *,
        successor_id: str | None,
        summary_turn_id: str | None,
        error_code: str | None,
        runtime_fence: RuntimeFence | None,
        now: datetime,
    ) -> GenerationTransition:
        """Apply one generation CAS inside the caller's transaction."""

        self._assert_runtime_fence_uncommitted(conn, runtime_fence)
        current = self._get_generation_uncommitted(conn, generation_id)
        if current is None:
            raise KeyError(f"unknown generation: {generation_id}")
        self._assert_generation_runtime_fence_uncommitted(
            conn,
            current,
            runtime_fence,
        )
        allowed = _GENERATION_TRANSITIONS.get(current.state.value, set())
        if next_state != current.state.value and next_state not in allowed:
            raise CausalTransitionError(
                f"generation {generation_id}: {current.state.value} -> "
                f"{next_state} is not allowed"
            )
        successor_id = successor_id or current.successor_id
        summary_turn_id = summary_turn_id or current.summary_turn_id
        if successor_id is not None:
            self._ensure_references(conn, successor_id, None)
        if summary_turn_id is not None and conn.execute(
            "SELECT 1 FROM harness_turns WHERE id = ?", (summary_turn_id,)
        ).fetchone() is None:
            raise KeyError(f"unknown summary turn: {summary_turn_id}")
        if (
            next_state == GenerationTransitionState.COMMITTED.value
            and successor_id is None
        ):
            raise ValueError("committed generation requires successor_id")
        settled_at = _ts(now) if next_state in _TERMINAL_GENERATION_STATES else None
        updated = conn.execute(
            "UPDATE generation_transitions SET state = ?, successor_id = ?, "
            "summary_turn_id = ?, error_code = ?, updated_at = ?, "
            "settled_at = ? WHERE id = ? AND state = ?",
            (
                next_state,
                successor_id,
                summary_turn_id,
                error_code,
                _ts(now),
                settled_at,
                generation_id,
                current.state.value,
            ),
        )
        if updated.rowcount != 1:
            raise CausalTransitionError(
                f"generation {generation_id} changed before transition"
            )
        if current.event_id is not None:
            event_status = {
                GenerationTransitionState.COMMITTED.value: (
                    CausalEventStatus.SETTLED.value
                ),
                GenerationTransitionState.FAILED.value: (
                    CausalEventStatus.FAILED.value
                ),
                GenerationTransitionState.UNCERTAIN.value: (
                    CausalEventStatus.UNCERTAIN.value
                ),
            }.get(next_state, CausalEventStatus.RUNNING.value)
            event_settled_at = (
                _ts(now)
                if event_status
                in {
                    CausalEventStatus.SETTLED.value,
                    CausalEventStatus.FAILED.value,
                    CausalEventStatus.UNCERTAIN.value,
                }
                else None
            )
            event_update = conn.execute(
                "UPDATE causal_events SET status = ?, updated_at = ?, "
                "settled_at = ? WHERE id = ? AND status = 'running'",
                (
                    event_status,
                    _ts(now),
                    event_settled_at,
                    current.event_id,
                ),
            )
            if event_update.rowcount != 1:
                existing_event = self._get_event_uncommitted(
                    conn, current.event_id
                )
                if (
                    existing_event is None
                    or existing_event.status.value != event_status
                ):
                    raise CausalTransitionError(
                        f"generation event {current.event_id} changed before transition"
                    )
        return self._get_generation_uncommitted(conn, generation_id)  # type: ignore[return-value]

    def commit_succession_publication(
        self,
        generation_id: str,
        predecessor_id: str,
        successor_id: str,
        *,
        summary_turn_id: str | None,
        runtime_fence: RuntimeFence | None,
    ) -> tuple[GenerationTransition, tuple[str, ...]]:
        """Publish the core SQLite succession truth in one fenced commit.

        Lease validity, immutable generation ownership, ACTIVE/PROVISIONAL
        visibility, queued future-event handoff and the COMMITTED terminal
        winner share one ``BEGIN IMMEDIATE`` boundary.  A failure rolls all
        five facts back together.
        """

        generation_id = _require_id(generation_id, "generation_id")
        predecessor_id = _require_id(predecessor_id, "predecessor_id")
        successor_id = _require_id(successor_id, "successor_id")
        summary_turn_id = _optional_id(summary_turn_id, "summary_turn_id")
        if predecessor_id == successor_id:
            raise ValueError("predecessor_id and successor_id must differ")
        now = _now()
        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)
            current = self._get_generation_uncommitted(conn, generation_id)
            if current is None:
                raise KeyError(f"unknown generation: {generation_id}")
            self._assert_generation_runtime_fence_uncommitted(
                conn,
                current,
                runtime_fence,
            )
            if (
                current.predecessor_id != predecessor_id
                or current.successor_id != successor_id
                or current.summary_turn_id != summary_turn_id
            ):
                raise CausalTransitionError(
                    "succession publication no longer matches its generation"
                )
            if current.state is GenerationTransitionState.COMMITTED:
                predecessor = conn.execute(
                    "SELECT status FROM engrams WHERE id = ?",
                    (predecessor_id,),
                ).fetchone()
                successor = conn.execute(
                    "SELECT status FROM engrams WHERE id = ?",
                    (successor_id,),
                ).fetchone()
                if (
                    predecessor is None
                    or predecessor[0] != EngramStatus.ARCHIVED.value
                    or successor is None
                    or successor[0] != EngramStatus.ACTIVE.value
                ):
                    raise CausalTransitionError(
                        "committed generation disagrees with Engram visibility"
                    )
                return current, ()
            if current.state is not GenerationTransitionState.ROTATING:
                raise CausalTransitionError(
                    f"generation {generation_id} is {current.state.value}, "
                    "expected rotating"
                )

            predecessor = conn.execute(
                "UPDATE engrams SET status = ? WHERE id = ? AND status = ?",
                (
                    EngramStatus.ARCHIVED.value,
                    predecessor_id,
                    EngramStatus.ACTIVE.value,
                ),
            )
            if predecessor.rowcount != 1:
                raise CausalTransitionError(
                    "succession predecessor is missing or no longer active"
                )
            successor = conn.execute(
                "UPDATE engrams SET status = ? WHERE id = ? AND status = ?",
                (
                    EngramStatus.ACTIVE.value,
                    successor_id,
                    EngramStatus.PROVISIONAL.value,
                ),
            )
            if successor.rowcount != 1:
                raise CausalTransitionError(
                    "succession candidate is missing or no longer provisional"
                )
            handed_off = self._reassign_queued_events_uncommitted(
                conn,
                predecessor_id,
                successor_id,
            )
            committed = self._transition_generation_uncommitted(
                conn,
                generation_id,
                GenerationTransitionState.COMMITTED.value,
                successor_id=successor_id,
                summary_turn_id=summary_turn_id,
                error_code=None,
                runtime_fence=runtime_fence,
                now=now,
            )
            return committed, handed_off

    def assert_runtime_fence(self, runtime_fence: RuntimeFence | None) -> None:
        """Linearize one lease check without creating another durable fact."""

        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)

    def assert_generation_runtime_fence(
        self,
        generation_id: str,
        runtime_fence: RuntimeFence | None,
    ) -> GenerationTransition:
        """Verify both the live lease and one generation's immutable epoch."""

        generation_id = _require_id(generation_id, "generation_id")
        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)
            generation = self._get_generation_uncommitted(conn, generation_id)
            if generation is None:
                raise KeyError(f"unknown generation: {generation_id}")
            self._assert_generation_runtime_fence_uncommitted(
                conn,
                generation,
                runtime_fence,
            )
            return generation

    def reassign_queued_events(
        self,
        predecessor_id: str,
        successor_id: str,
        *,
        runtime_fence: RuntimeFence | None = None,
    ) -> tuple[str, ...]:
        """Hand unstarted future stimuli to a committed successor.

        Running and terminal rows are historical facts about the predecessor
        and are deliberately immutable.  The returned IDs are selected before
        the compare-and-set update so callers can audit the exact handoff.
        """

        predecessor_id = _require_id(predecessor_id, "predecessor_id")
        successor_id = _require_id(successor_id, "successor_id")
        if predecessor_id == successor_id:
            raise ValueError("predecessor_id and successor_id must differ")
        with self._transaction() as conn:
            self._assert_runtime_fence_uncommitted(conn, runtime_fence)
            return self._reassign_queued_events_uncommitted(
                conn,
                predecessor_id,
                successor_id,
            )

    def _reassign_queued_events_uncommitted(
        self,
        conn: sqlite3.Connection,
        predecessor_id: str,
        successor_id: str,
    ) -> tuple[str, ...]:
        """Move an exact queued-event set inside the caller's transaction."""

        self._ensure_references(conn, predecessor_id, None)
        self._ensure_references(conn, successor_id, None)
        rows = conn.execute(
            "SELECT id FROM causal_events WHERE engram_id = ? "
            "AND status = 'queued' ORDER BY seq ASC",
            (predecessor_id,),
        ).fetchall()
        event_ids = tuple(str(row[0]) for row in rows)
        if not event_ids:
            return ()
        updated = conn.execute(
            "UPDATE causal_events SET engram_id = ?, updated_at = ? "
            "WHERE engram_id = ? AND status = 'queued'",
            (
                successor_id,
                _ts(_now()),
                predecessor_id,
            ),
        )
        if updated.rowcount != len(event_ids):
            raise CausalTransitionError(
                "queued succession events changed before handoff"
            )
        return event_ids

    def generation_blocked_predecessors(self) -> set[str]:
        """Return subjects whose lineage cannot admit another ordinary turn."""

        with self._storage._lock:
            rows = self._storage._conn.execute(
                "SELECT DISTINCT predecessor_id FROM generation_transitions "
                "WHERE state IN ('prepared', 'summarizing', 'rotating', 'uncertain')"
            ).fetchall()
            return {str(row[0]) for row in rows}

    update_generation = transition_generation

    begin_generation_transition = begin_generation
    transition_generation_state = transition_generation

    def recover_generations(
        self,
        *,
        runtime_fence: RuntimeFence | None = None,
    ) -> tuple[GenerationTransition, ...]:
        now = _now()
        with self._transaction(
            runtime_fence=runtime_fence,
            allow_recovery=True,
        ) as conn:
            self._assert_runtime_fence_uncommitted(
                conn,
                runtime_fence,
                allow_recovery=True,
            )
            rows = conn.execute(
                f"SELECT {_GENERATION_COLUMNS} FROM generation_transitions "
                "WHERE state NOT IN ('committed', 'failed', 'uncertain') "
                "ORDER BY created_at ASC, id ASC"
            ).fetchall()
            ids: list[str] = []
            for row in rows:
                generation = self._row_to_generation(row)
                updated = conn.execute(
                    "UPDATE generation_transitions SET state = 'uncertain', "
                    "error_code = ?, updated_at = ?, settled_at = ? "
                    "WHERE id = ? AND state NOT IN "
                    "('committed', 'failed', 'uncertain')",
                    (
                        "process_recovered",
                        _ts(now),
                        _ts(now),
                        generation.id,
                    ),
                )
                if updated.rowcount != 1:
                    raise CausalTransitionError(
                        f"generation {generation.id} changed during recovery"
                    )
                if generation.event_id is not None:
                    event_updated = conn.execute(
                        "UPDATE causal_events SET status = 'uncertain', "
                        "updated_at = ?, settled_at = ? WHERE id = ? "
                        "AND status = 'running'",
                        (_ts(now), _ts(now), generation.event_id),
                    )
                    if event_updated.rowcount != 1:
                        event = self._get_event_uncommitted(
                            conn, generation.event_id
                        )
                        if event is None or event.status is not CausalEventStatus.UNCERTAIN:
                            raise CausalTransitionError(
                                f"generation event {generation.event_id} changed during recovery"
                            )
                ids.append(generation.id)
            return tuple(
                self._get_generation_uncommitted(conn, generation_id)
                for generation_id in ids
            )  # type: ignore[misc]

    def get_generation(self, generation_id: str) -> GenerationTransition | None:
        generation_id = _require_id(generation_id, "generation_id")
        with self._storage._lock:
            return self._get_generation_uncommitted(
                self._storage._conn, generation_id
            )

    def list_generations(
        self,
        *,
        predecessor_id: str | None = None,
        successor_id: str | None = None,
        event_id: str | None = None,
        causal_id: str | None = None,
        state: GenerationTransitionState | str | None = None,
    ) -> list[GenerationTransition]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        if predecessor_id is not None:
            clauses.append("predecessor_id = ?")
            params.append(_require_id(predecessor_id, "predecessor_id"))
        if successor_id is not None:
            clauses.append("successor_id = ?")
            params.append(_require_id(successor_id, "successor_id"))
        if event_id is not None:
            clauses.append("event_id = ?")
            params.append(_require_id(event_id, "event_id"))
        if causal_id is not None:
            clauses.append("causal_id = ?")
            params.append(_require_id(causal_id, "causal_id"))
        if state is not None:
            clauses.append("state = ?")
            params.append(_enum_value(state, GenerationTransitionState, "state"))
        with self._storage._lock:
            rows = self._storage._conn.execute(
                f"SELECT {_GENERATION_COLUMNS} FROM generation_transitions WHERE "
                + " AND ".join(clauses)
                + " ORDER BY created_at ASC, id ASC",
                params,
            ).fetchall()
            return [self._row_to_generation(row) for row in rows]

    get_generation_transition = get_generation
    list_generation_transitions = list_generations

    # ── Uncommitted primitives ──────────────────────────────────

    @staticmethod
    def _ensure_references(
        conn: sqlite3.Connection,
        engram_id: str | None,
        center_id: str | None,
        *,
        require_membership: bool = False,
    ) -> None:
        if engram_id is not None and conn.execute(
            "SELECT 1 FROM engrams WHERE id = ?", (engram_id,)
        ).fetchone() is None:
            raise KeyError(f"unknown Engram: {engram_id}")
        if center_id is not None and conn.execute(
            "SELECT 1 FROM activity_centers WHERE id = ?", (center_id,)
        ).fetchone() is None:
            raise KeyError(f"unknown ActivityCenter: {center_id}")
        if (
            require_membership
            and engram_id is not None
            and center_id is not None
            and conn.execute(
                "SELECT 1 FROM center_memberships "
                "WHERE center_id = ? AND engram_id = ?",
                (center_id, engram_id),
            ).fetchone()
            is None
        ):
            raise CausalTransitionError(
                "root event Engram must be a member of its ActivityCenter"
            )

    @staticmethod
    def _inherit_parent_center(
        parent: CausalEvent,
        requested_center_id: str | None,
    ) -> str | None:
        if (
            requested_center_id is not None
            and requested_center_id != parent.center_id
        ):
            raise CausalTransitionError(
                "child event must inherit parent center_id without switching"
            )
        return parent.center_id

    @staticmethod
    def _insert_event_uncommitted(
        conn: sqlite3.Connection, event: CausalEvent
    ) -> None:
        assert_causal_flow(event)
        conn.execute(
            "INSERT INTO causal_events ("
            "id, causal_id, parent_event_id, world_id, engram_id, center_id, "
            "flow, domain, kind, source, status, content, metadata, "
            "idempotency_key, attempts, created_at, updated_at, started_at, "
            "settled_at, resolution, resolution_note) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event.id,
                event.causal_id,
                event.parent_event_id,
                event.world_id,
                event.engram_id,
                event.center_id,
                event.flow.value if event.flow is not None else None,
                event.domain.value,
                event.kind.value,
                event.source.value,
                event.status.value,
                event.content,
                json.dumps(event.metadata, ensure_ascii=False, sort_keys=True),
                event.idempotency_key,
                event.attempts,
                _ts(event.created_at),
                _ts(event.updated_at),
                _ts(event.started_at) if event.started_at else None,
                _ts(event.settled_at) if event.settled_at else None,
                event.resolution.value if event.resolution is not None else None,
                event.resolution_note,
            ),
        )

    @staticmethod
    def _insert_turn_uncommitted(
        conn: sqlite3.Connection, turn: HarnessTurn
    ) -> None:
        conn.execute(
            "INSERT INTO harness_turns ("
            "id, event_id, engram_id, state, cursor_before, cursor_after, "
            "input_message_id, prompt_accepted, session_id, session_file, result_event_id, "
            "error_code, error_phase, started_at, updated_at, settled_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                turn.id,
                turn.event_id,
                turn.engram_id,
                turn.state.value,
                turn.cursor_before,
                turn.cursor_after,
                turn.input_message_id,
                None if turn.prompt_accepted is None else int(turn.prompt_accepted),
                turn.session_id,
                turn.session_file,
                turn.result_event_id,
                turn.error_code,
                turn.error_phase,
                _ts(turn.started_at),
                _ts(turn.updated_at),
                _ts(turn.settled_at) if turn.settled_at else None,
            ),
        )

    @staticmethod
    def _insert_generation_uncommitted(
        conn: sqlite3.Connection, generation: GenerationTransition
    ) -> None:
        conn.execute(
            "INSERT INTO generation_transitions ("
            "id, causal_id, event_id, predecessor_id, successor_id, state, "
            "summary_turn_id, error_code, created_at, updated_at, settled_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                generation.id,
                generation.causal_id,
                generation.event_id,
                generation.predecessor_id,
                generation.successor_id,
                generation.state.value,
                generation.summary_turn_id,
                generation.error_code,
                _ts(generation.created_at),
                _ts(generation.updated_at),
                _ts(generation.settled_at) if generation.settled_at else None,
            ),
        )

    @staticmethod
    def _get_event_uncommitted(
        conn: sqlite3.Connection, event_id: str
    ) -> CausalEvent | None:
        row = conn.execute(
            f"SELECT {_EVENT_COLUMNS} FROM causal_events WHERE id = ?",
            (event_id,),
        ).fetchone()
        return CausalLedger._row_to_event(row) if row is not None else None

    @staticmethod
    def _get_generation_uncommitted(
        conn: sqlite3.Connection, generation_id: str
    ) -> GenerationTransition | None:
        row = conn.execute(
            f"SELECT {_GENERATION_COLUMNS} FROM generation_transitions "
            "WHERE id = ?",
            (generation_id,),
        ).fetchone()
        return (
            CausalLedger._row_to_generation(row) if row is not None else None
        )

    @staticmethod
    def _message_count_uncommitted(
        conn: sqlite3.Connection, engram_id: str
    ) -> int:
        row = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE engram_id = ?", (engram_id,)
        ).fetchone()
        return int(row[0]) if row is not None else 0

    @staticmethod
    def _load_cursor_uncommitted(
        conn: sqlite3.Connection, engram_id: str
    ) -> int:
        row = conn.execute(
            "SELECT state FROM component_state WHERE component = ?",
            ("harness.pulse.inputs.v1",),
        ).fetchone()
        if row is None:
            return 0
        state = json.loads(row[0])
        if not isinstance(state, dict):
            raise ValueError("harness input component state must be an object")
        cursors = state.get("cursors", {})
        if not isinstance(cursors, dict):
            raise ValueError("harness input component cursors must be an object")
        value = cursors.get(engram_id, 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("harness input cursor must be a non-negative integer")
        return value

    @staticmethod
    def _load_component_state_uncommitted(
        conn: sqlite3.Connection,
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT state FROM component_state WHERE component = ?",
            ("harness.pulse.inputs.v1",),
        ).fetchone()
        if row is None:
            return {"version": 1, "cursors": {}}
        state = json.loads(row[0])
        if not isinstance(state, dict):
            raise ValueError("harness input component state must be an object")
        cursors = state.get("cursors", {})
        if not isinstance(cursors, dict):
            raise ValueError("harness input component cursors must be an object")
        normalized: dict[str, int] = {}
        for key, value in cursors.items():
            if not isinstance(key, str) or isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("harness input cursors must map IDs to counts")
            normalized[key] = value
        state["version"] = 1
        state["cursors"] = normalized
        return state

    @staticmethod
    def _save_cursor_uncommitted(
        conn: sqlite3.Connection, engram_id: str, cursor: int
    ) -> None:
        if isinstance(cursor, bool) or not isinstance(cursor, int) or cursor < 0:
            raise ValueError("cursor must be a non-negative integer")
        state = CausalLedger._load_component_state_uncommitted(conn)
        existing = state["cursors"].get(engram_id, 0)
        if existing > cursor:
            raise CausalTransitionError(
                "harness input cursor cannot move backwards"
            )
        state["cursors"][engram_id] = cursor
        conn.execute(
            "INSERT INTO component_state (component, state, updated_at) "
            "VALUES (?, ?, ?) ON CONFLICT(component) DO UPDATE SET "
            "state = excluded.state, updated_at = excluded.updated_at",
            (
                "harness.pulse.inputs.v1",
                json.dumps(state, ensure_ascii=False, sort_keys=True),
                _ts(_now()),
            ),
        )

    @staticmethod
    def _row_to_event(row: tuple[Any, ...]) -> CausalEvent:
        return CausalEvent(
            seq=row[0],
            id=row[1],
            causal_id=row[2],
            parent_event_id=row[3],
            world_id=row[4],
            engram_id=row[5],
            center_id=row[6],
            flow=CausalEventFlow(row[7]) if row[7] is not None else None,
            domain=CausalEventDomain(row[8]),
            kind=CausalEventKind(row[9]),
            source=CausalEventSource(row[10]),
            status=CausalEventStatus(row[11]),
            content=row[12],
            metadata=json.loads(row[13]),
            idempotency_key=row[14],
            attempts=row[15],
            created_at=_parse_ts(row[16]),
            updated_at=_parse_ts(row[17]),
            started_at=_parse_ts(row[18]) if row[18] else None,
            settled_at=_parse_ts(row[19]) if row[19] else None,
            resolution=(
                CausalEventResolution(row[20]) if row[20] is not None else None
            ),
            resolution_note=row[21],
        )

    @staticmethod
    def _is_same_or_committed_successor_uncommitted(
        conn: sqlite3.Connection,
        original_engram_id: str,
        current_engram_id: str | None,
    ) -> bool:
        """Prove queued-root ownership through the committed generation DAG."""

        if current_engram_id is None:
            return False
        return conn.execute(
            "WITH RECURSIVE lineage(id) AS ("
            "SELECT ? UNION SELECT generation.successor_id "
            "FROM generation_transitions generation JOIN lineage current "
            "ON generation.predecessor_id = current.id "
            "WHERE generation.state = 'committed' "
            "AND generation.successor_id IS NOT NULL"
            ") SELECT 1 FROM lineage WHERE id = ? LIMIT 1",
            (original_engram_id, current_engram_id),
        ).fetchone() is not None

    @staticmethod
    def _row_to_dendritic_window(
        conn: sqlite3.Connection,
        row: tuple[Any, ...],
    ) -> DendriticWindow:
        member_rows = conn.execute(
            "SELECT ordinal, event_id, event_seq, arrived_at "
            "FROM dendritic_window_members WHERE window_id = ? "
            "ORDER BY ordinal ASC",
            (row[0],),
        ).fetchall()
        members = tuple(
            DendriticWindowMember(
                ordinal=member[0],
                event_id=member[1],
                event_seq=member[2],
                arrived_at=_parse_ts(member[3]),
            )
            for member in member_rows
        )
        window = DendriticWindow(
            id=row[0],
            world_id=row[1],
            formation_engram_id=row[2],
            policy_version=row[3],
            event_set_sha256=row[4],
            event_count=row[5],
            base_silence_threshold_seconds=float(row[6]),
            base_max_wait_seconds=float(row[7]),
            wait_modifier=float(row[8]),
            silence_threshold_seconds=float(row[9]),
            max_wait_seconds=float(row[10]),
            window_opened_at=_parse_ts(row[11]),
            last_input_at=_parse_ts(row[12]),
            window_closed_at=_parse_ts(row[13]),
            observed_at=_parse_ts(row[14]),
            observed_event_seq=row[15],
            created_at=_parse_ts(row[16]),
            members=members,
        )
        if (
            re.fullmatch(r"[0-9a-f]{64}", window.id) is None
            or not window.world_id
            or not window.formation_engram_id
            or window.policy_version != DENDRITIC_WINDOW_POLICY_VERSION
            or re.fullmatch(r"[0-9a-f]{64}", window.event_set_sha256) is None
            or type(window.event_count) is not int
            or not 1 <= window.event_count <= MAX_DENDRITIC_WINDOW_EVENTS
            or len(members) != window.event_count
            or type(window.observed_event_seq) is not int
            or window.observed_event_seq < max(
                (member.event_seq for member in members),
                default=1,
            )
            or not (
                window.window_opened_at
                <= window.last_input_at
                <= window.window_closed_at
                <= window.observed_at
                <= window.created_at
            )
            or tuple(member.ordinal for member in members)
            != tuple(range(len(members)))
            or any(
                not member.event_id
                or type(member.event_seq) is not int
                or member.event_seq < 1
                for member in members
            )
        ):
            raise CausalTransitionError(
                "dendritic window header or member evidence drifted"
            )
        try:
            window_policy = DendriticWindowPolicySnapshot(
                policy_version=window.policy_version,
                base_silence_threshold_seconds=(
                    window.base_silence_threshold_seconds
                ),
                base_max_wait_seconds=window.base_max_wait_seconds,
                wait_modifier=window.wait_modifier,
                silence_threshold_seconds=window.silence_threshold_seconds,
                max_wait_seconds=window.max_wait_seconds,
            )
        except ValueError as exc:
            raise CausalTransitionError(
                "dendritic window policy snapshot drifted"
            ) from exc
        if (
            CausalLedger._dendritic_event_set_sha256(
                member.event_id for member in members
            )
            != window.event_set_sha256
        ):
            raise CausalTransitionError(
                "dendritic window event-set digest drifted"
            )

        events: list[CausalEvent] = []
        for member in members:
            event = CausalLedger._get_event_uncommitted(conn, member.event_id)
            input_policy = CausalLedger._dendritic_input_policy_uncommitted(
                conn,
                member.event_id,
            )
            policy_identity = conn.execute(
                "SELECT world_id, engram_id, recorded_at FROM "
                "dendritic_input_policy_snapshots WHERE event_id = ?",
                (member.event_id,),
            ).fetchone()
            if (
                event is None
                or input_policy is None
                or policy_identity is None
                or policy_identity[0] != window.world_id
                or policy_identity[1] != window.formation_engram_id
                or not (
                    event.created_at
                    <= _parse_ts(policy_identity[2])
                    <= window.created_at
                )
                or event.seq != member.event_seq
                or event.created_at != member.arrived_at
                or event.world_id != window.world_id
                or event.engram_id != window.formation_engram_id
                or causal_turn_violation_codes(event)
                or event.metadata.get("dendritic_integration_version") == 1
                or (
                    isinstance(event.metadata.get("generation_id"), str)
                    and event.metadata.get("generation_stage") == "summary"
                )
            ):
                raise CausalTransitionError(
                    "dendritic window raw event provenance drifted"
                )
            events.append(event)
        ordered = sorted(
            events,
            key=lambda event: (
                event.created_at,
                event.seq if event.seq is not None else 2**63,
                event.id,
            ),
        )
        if tuple(event.id for event in ordered) != tuple(
            member.event_id for member in members
        ):
            raise CausalTransitionError(
                "dendritic window member ordering drifted"
            )
        member_ids = {member.event_id for member in members}
        interval_rows = conn.execute(
            f"SELECT {_EVENT_COLUMNS} FROM causal_events "
            "WHERE world_id = ? AND engram_id = ? "
            "AND created_at >= ? AND created_at <= ? "
            "AND seq <= ? AND NOT EXISTS ("
            "SELECT 1 FROM dendritic_window_members member "
            "WHERE member.event_id = causal_events.id"
            ") AND EXISTS ("
            "SELECT 1 FROM dendritic_input_policy_snapshots policy "
            "WHERE policy.event_id = causal_events.id"
            ") ORDER BY seq ASC",
            (
                window.world_id,
                window.formation_engram_id,
                _ts(window.window_opened_at),
                _ts(window.window_closed_at),
                window.observed_event_seq,
            ),
        ).fetchall()
        omitted = [
            event.id
            for event in (
                CausalLedger._row_to_event(row) for row in interval_rows
            )
            if event.id not in member_ids
            and CausalLedger._is_dendritic_window_shape(event)
        ]
        if omitted:
            raise CausalTransitionError(
                "dendritic window omitted raw inputs inside its closed boundary: "
                + ", ".join(omitted)
            )
        if (
            ordered[0].created_at != window.window_opened_at
            or ordered[-1].created_at != window.last_input_at
            or CausalLedger._dendritic_input_policy_uncommitted(
                conn,
                ordered[0].id,
            )
            != window_policy
        ):
            raise CausalTransitionError(
                "dendritic window arrival boundary drifted"
            )
        opened_at = ordered[0].created_at
        last_input_at = opened_at
        deadline = min(
            opened_at + timedelta(seconds=window.max_wait_seconds),
            last_input_at
            + timedelta(seconds=window.silence_threshold_seconds),
        )
        for event in ordered[1:]:
            if event.created_at > deadline:
                raise CausalTransitionError(
                    "dendritic window crosses a closed timing boundary"
                )
            last_input_at = event.created_at
            deadline = min(
                opened_at + timedelta(seconds=window.max_wait_seconds),
                last_input_at
                + timedelta(seconds=window.silence_threshold_seconds),
            )
        if deadline != window.window_closed_at:
            raise CausalTransitionError(
                "dendritic window policy boundary drifted"
            )
        ready = DendriticReadyWindow(
            engram_id=window.formation_engram_id,
            event_ids=tuple(member.event_id for member in members),
            event_seqs=tuple(member.event_seq for member in members),
            policy_version=window.policy_version,
            base_silence_threshold_seconds=(
                window.base_silence_threshold_seconds
            ),
            base_max_wait_seconds=window.base_max_wait_seconds,
            wait_modifier=window.wait_modifier,
            silence_threshold_seconds=window.silence_threshold_seconds,
            max_wait_seconds=window.max_wait_seconds,
            opened_at=window.window_opened_at,
            last_input_at=window.last_input_at,
            closed_at=window.window_closed_at,
            observed_at=window.observed_at,
        )
        if CausalLedger._dendritic_window_id(
            world_id=window.world_id,
            ready=ready,
        ) != window.id:
            raise CausalTransitionError("dendritic window identity drifted")
        return window

    @staticmethod
    def _row_to_dendritic_integration(
        conn: sqlite3.Connection,
        row: tuple[Any, ...],
    ) -> DendriticIntegration:
        window_rows = conn.execute(
            f"SELECT {_DENDRITIC_WINDOW_COLUMNS} FROM dendritic_windows w "
            "JOIN dendritic_integration_windows binding "
            "ON binding.window_id = w.id "
            "WHERE binding.integration_id = ?",
            (row[0],),
        ).fetchall()
        legacy_rows = conn.execute(
            "SELECT legacy.source_schema_version, legacy.evidence_class, "
            "legacy.integration_created_at, migration.name "
            "FROM dendritic_legacy_integrations legacy "
            "JOIN schema_migrations migration ON migration.version = 6 "
            "WHERE legacy.integration_id = ?",
            (row[0],),
        ).fetchall()
        if len(window_rows) == 1 and not legacy_rows:
            window = CausalLedger._row_to_dendritic_window(conn, window_rows[0])
            window_evidence_class = DENDRITIC_WINDOW_EVIDENCE_DURABLE_V6
        elif not window_rows and len(legacy_rows) == 1:
            legacy = legacy_rows[0]
            trigger_row = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
                "AND name = 'dendritic_legacy_integrations_closed_insert'",
            ).fetchone()
            trigger_sql = (
                trigger_row[0].casefold()
                if trigger_row is not None and isinstance(trigger_row[0], str)
                else ""
            )
            if (
                legacy[0] != 5
                or legacy[1] != DENDRITIC_WINDOW_EVIDENCE_LEGACY_V5
                or legacy[2] != row[11]
                or legacy[3] != "dendritic_window_evidence"
                or "before insert on dendritic_legacy_integrations"
                not in " ".join(trigger_sql.split())
                or conn.execute("PRAGMA user_version").fetchone() != (6,)
            ):
                raise CausalTransitionError(
                    "legacy dendritic integration migration seal drifted"
                )
            window = None
            window_evidence_class = DENDRITIC_WINDOW_EVIDENCE_LEGACY_V5
        else:
            raise CausalTransitionError(
                "dendritic integration has neither one durable window nor "
                "one sealed v5 legacy marker"
            )
        member_rows = conn.execute(
            "SELECT ordinal, event_id, event_seq, causal_id, source_identity, "
            "content_sha256, arrived_at FROM dendritic_integration_members "
            "WHERE integration_id = ? ORDER BY ordinal ASC",
            (row[0],),
        ).fetchall()
        members = tuple(
            DendriticIntegrationMember(
                ordinal=member[0],
                event_id=member[1],
                event_seq=member[2],
                causal_id=member[3],
                source_identity=member[4],
                content_sha256=member[5],
                arrived_at=_parse_ts(member[6]),
            )
            for member in member_rows
        )
        if (
            type(row[8]) is not int
            or not 2 <= row[8] <= MAX_DENDRITIC_INTEGRATION_MEMBERS
            or len(members) != row[8]
        ):
            raise CausalTransitionError(
                "dendritic integration member count does not match its ledger row"
            )
        if (
            tuple(member.ordinal for member in members) != tuple(range(len(members)))
            or any(
                type(member.event_seq) is not int
                or member.event_seq < 1
                or not member.event_id
                or not member.causal_id
                or re.fullmatch(
                    r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}",
                    member.source_identity,
                )
                is None
                or re.fullmatch(r"[0-9a-f]{64}", member.content_sha256) is None
                for member in members
            )
        ):
            raise CausalTransitionError(
                "dendritic integration member evidence is not canonical"
            )
        integration = DendriticIntegration(
            id=row[0],
            world_id=row[1],
            formation_engram_id=row[2],
            center_id=row[3],
            aggregate_event_id=row[4],
            delivery_class=row[5],
            member_set_sha256=row[6],
            content_sha256=row[7],
            member_count=row[8],
            window_opened_at=_parse_ts(row[9]),
            window_closed_at=_parse_ts(row[10]),
            created_at=_parse_ts(row[11]),
            members=members,
            window_evidence_class=window_evidence_class,
            window=window,
        )
        if (
            integration.delivery_class not in {"external", "propagation"}
            or not integration.id
            or not integration.world_id
            or not integration.formation_engram_id
            or not integration.aggregate_event_id
            or not re.fullmatch(r"[0-9a-f]{64}", integration.member_set_sha256)
            or not re.fullmatch(r"[0-9a-f]{64}", integration.content_sha256)
            or not (
                2
                <= integration.member_count
                <= MAX_DENDRITIC_INTEGRATION_MEMBERS
            )
            or integration.window_opened_at > integration.window_closed_at
            or (
                window_evidence_class == DENDRITIC_WINDOW_EVIDENCE_DURABLE_V6
                and (
                    window is None
                    or integration.window_opened_at != window.window_opened_at
                    or integration.window_closed_at != window.window_closed_at
                    or integration.created_at != window.created_at
                    or integration.world_id != window.world_id
                    or integration.formation_engram_id
                    != window.formation_engram_id
                )
            )
            or (
                window_evidence_class == DENDRITIC_WINDOW_EVIDENCE_LEGACY_V5
                and (
                    window is not None
                    or integration.window_closed_at != integration.created_at
                )
            )
        ):
            raise CausalTransitionError(
                "dendritic integration header evidence drifted"
            )
        expected_set_sha256 = hashlib.sha256(
            json.dumps(
                tuple(member.event_id for member in members),
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        if expected_set_sha256 != integration.member_set_sha256:
            raise CausalTransitionError(
                "dendritic integration member-set digest drifted"
            )

        source_events: list[CausalEvent] = []
        signatures: list[tuple[str, str]] = []
        for member in members:
            event = CausalLedger._get_event_uncommitted(conn, member.event_id)
            if event is None:
                raise CausalTransitionError(
                    "dendritic integration references a missing member event"
                )
            signature = (
                CausalLedger._dendritic_legacy_v5_source_signature(event)
                if window_evidence_class
                == DENDRITIC_WINDOW_EVIDENCE_LEGACY_V5
                else CausalLedger._dendritic_source_signature(
                    event,
                    require_queued=False,
                )
            )
            if (
                signature is None
                or event.status is not CausalEventStatus.RECONCILED
                or event.resolution is not CausalEventResolution.SUPERSEDED
                or event.resolution_note
                != f"dendritic_integration:{integration.id}"
                or event.seq != member.event_seq
                or event.causal_id != member.causal_id
                or signature[1] != member.source_identity
                or event.created_at != member.arrived_at
                or hashlib.sha256((event.content or "").encode("utf-8")).hexdigest()
                != member.content_sha256
                or event.world_id != integration.world_id
                or event.engram_id != integration.formation_engram_id
                or event.center_id != integration.center_id
                or event.attempts != 0
                or event.started_at is not None
                or event.updated_at != integration.created_at
                or event.settled_at != integration.created_at
                or conn.execute(
                    "SELECT 1 FROM harness_turns WHERE event_id = ? LIMIT 1",
                    (event.id,),
                ).fetchone()
                is not None
            ):
                raise CausalTransitionError(
                    "dendritic integration member provenance drifted"
                )
            if signature[0] == "propagation":
                CausalLedger._assert_dendritic_propagation_provenance_uncommitted(
                    conn,
                    event,
                    signature[1],
                )
            source_events.append(event)
            signatures.append(signature)
        if (
            {signature[0] for signature in signatures}
            != {integration.delivery_class}
            or len({signature[1] for signature in signatures}) < 2
            or (
                window is not None
                and not {member.event_id for member in members}.issubset(
                    {member.event_id for member in window.members}
                )
            )
        ):
            raise CausalTransitionError(
                "dendritic integration source window drifted"
            )

        integrated_content = "\n\n".join(event.content or "" for event in source_events)
        if (
            hashlib.sha256(integrated_content.encode("utf-8")).hexdigest()
            != integration.content_sha256
        ):
            raise CausalTransitionError(
                "dendritic integration content digest drifted"
            )
        aggregate = CausalLedger._get_event_uncommitted(
            conn,
            integration.aggregate_event_id,
        )
        if aggregate is None:
            raise CausalTransitionError(
                "dendritic integration references a missing aggregate event"
            )
        default_priority = (
            0.8 if integration.delivery_class == "propagation" else 1.0
        )
        priorities: list[float] = []
        depths: list[int] = []
        for event in source_events:
            raw_priority = event.metadata.get("priority", default_priority)
            priorities.append(
                float(raw_priority)
                if isinstance(raw_priority, (int, float))
                and not isinstance(raw_priority, bool)
                and math.isfinite(float(raw_priority))
                else default_priority
            )
            raw_depth = event.metadata.get("depth", 0)
            depths.append(
                raw_depth if type(raw_depth) is int and raw_depth >= 0 else 0
            )
        expected_metadata = {
            "dendritic_delivery_class": integration.delivery_class,
            "dendritic_integration_id": integration.id,
            "dendritic_integration_version": 1,
            "dendritic_member_count": integration.member_count,
            "dendritic_member_set_sha256": integration.member_set_sha256,
            "depth": max(depths),
            "priority": max(priorities),
        }
        if window is not None:
            expected_metadata["dendritic_window_id"] = window.id
        if (
            aggregate.id != integration.aggregate_event_id
            or aggregate.causal_id != aggregate.id
            or aggregate.parent_event_id is not None
            or aggregate.world_id != integration.world_id
            or not CausalLedger._is_same_or_committed_successor_uncommitted(
                conn,
                integration.formation_engram_id,
                aggregate.engram_id,
            )
            or aggregate.center_id != integration.center_id
            or aggregate.flow is not CausalEventFlow.CONTENT
            or aggregate.domain is not CausalEventDomain.PULSE
            or aggregate.kind is not CausalEventKind.PULSE
            or aggregate.source is not CausalEventSource.SELF
            or aggregate.content != integrated_content
            or aggregate.metadata != expected_metadata
            or aggregate.idempotency_key
            != f"dendritic:{integration.member_set_sha256}"
            or aggregate.created_at != integration.created_at
        ):
            raise CausalTransitionError(
                "dendritic integration aggregate provenance drifted"
            )
        return integration

    @staticmethod
    def _row_to_turn(row: tuple[Any, ...]) -> HarnessTurn:
        return HarnessTurn(
            id=row[0],
            event_id=row[1],
            engram_id=row[2],
            state=HarnessTurnState(row[3]),
            cursor_before=row[4],
            cursor_after=row[5],
            input_message_id=row[6],
            prompt_accepted=(
                None if row[7] is None else bool(row[7])
            ),
            session_id=row[8],
            session_file=row[9],
            result_event_id=row[10],
            error_code=row[11],
            error_phase=row[12],
            started_at=_parse_ts(row[13]),
            updated_at=_parse_ts(row[14]),
            settled_at=_parse_ts(row[15]) if row[15] else None,
        )

    @staticmethod
    def _row_to_generation(row: tuple[Any, ...]) -> GenerationTransition:
        return GenerationTransition(
            id=row[0],
            causal_id=row[1],
            event_id=row[2],
            predecessor_id=row[3],
            successor_id=row[4],
            state=GenerationTransitionState(row[5]),
            summary_turn_id=row[6],
            error_code=row[7],
            created_at=_parse_ts(row[8]),
            updated_at=_parse_ts(row[9]),
            settled_at=_parse_ts(row[10]) if row[10] else None,
        )

    @staticmethod
    def _enum_values(
        value: Any, enum_type: type, field_name: str
    ) -> list[str]:
        if isinstance(value, (str, enum_type)):
            return [_enum_value(value, enum_type, field_name)]
        try:
            values = list(value)
        except TypeError as exc:
            raise ValueError(f"{field_name} must be an enum or iterable") from exc
        if not values:
            raise ValueError(f"{field_name} cannot be empty")
        return [_enum_value(item, enum_type, field_name) for item in values]

    @staticmethod
    def _coerce_input_message(message: Message | str | None) -> Message | None:
        if message is None:
            return None
        if isinstance(message, Message):
            return message
        if isinstance(message, str):
            return Message(role=MessageRole.INJECTION, content=message)
        raise TypeError("message must be Message, string, or null")

    @staticmethod
    def _coerce_assistant_message(assistant: Any) -> Message:
        if isinstance(assistant, Message):
            if assistant.role is not MessageRole.ASSISTANT:
                raise ValueError("settle_turn requires an assistant message")
            return assistant
        if isinstance(assistant, str):
            return Message(role=MessageRole.ASSISTANT, content=assistant)
        if isinstance(assistant, Mapping):
            content = assistant.get("content")
        else:
            content = getattr(assistant, "content", None)
        if not isinstance(content, str):
            raise TypeError("assistant must provide string content")
        return Message(role=MessageRole.ASSISTANT, content=content)

    @staticmethod
    def _merge_safe_metadata(
        *,
        metadata: dict[str, Any] | None,
        usage: Mapping[str, Any] | None,
        safe_trace: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        if metadata is not None:
            merged.update(metadata)
        if usage is not None:
            merged["usage"] = dict(usage)
        if safe_trace is not None:
            merged["trace"] = dict(safe_trace)
        return _metadata(merged)

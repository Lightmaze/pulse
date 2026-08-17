"""Durable, read-only causal-chain amplification projection."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from pulse_system.core.causality.flow_contract import (
    causal_flow_violation_codes,
    causal_turn_violation_codes,
)
from pulse_system.core.types import (
    CausalEventDomain,
    CausalEventFlow,
    CausalEventKind,
    CausalEventSource,
    CausalEventStatus,
)


_SCHEMA = "causal-amplification.v1"
_EVIDENCE_CLASS = "durable_causal_ledger_projection"
_TERMINAL_TURN_STATES = frozenset({"settled", "failed", "uncertain"})
_USAGE_KEYS = (
    "input_count",
    "output_count",
    "cached_count",
    "cache_write_count",
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _require_id(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _elapsed_ms(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round(max(0.0, (end - start).total_seconds() * 1000.0), 3)


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 6)


@dataclass(frozen=True)
class _FlowProjection:
    flow: CausalEventFlow | None
    domain: CausalEventDomain
    kind: CausalEventKind
    source: CausalEventSource
    status: CausalEventStatus
    content: str | None
    metadata: Mapping[str, Any]
    parent_event_id: str | None
    engram_id: str | None


@dataclass(frozen=True)
class CausalAmplificationSnapshot:
    """JSON-safe facts derived from one complete durable causal chain."""

    causal_id: str
    world_id: str
    observed_at: datetime
    first_seq: int
    last_seq: int
    first_event_at: datetime
    last_event_at: datetime
    event_count: int
    root_event_count: int
    child_event_count: int
    turn_root_count: int
    claimed_turn_root_count: int
    propagation_event_count: int
    distinct_engram_count: int
    revisit_count: int
    revisited_engram_count: int
    max_propagation_depth: int
    max_children_per_parent: int
    queued_event_count: int
    oldest_queued_age_ms: float | None
    max_observed_queue_wait_ms: float | None
    turn_attempt_count: int
    settled_turn_count: int
    terminal_turn_count: int
    active_ms_total: float
    active_ms_max: float
    input_tokens: int
    output_tokens: int
    cached_tokens: int
    cache_write_tokens: int
    usage_complete_turn_count: int
    status_counts: tuple[tuple[str, int], ...]
    flow_counts: tuple[tuple[str, int], ...]
    flow_contract_violation_count: int
    violation_counts: tuple[tuple[str, int], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": _SCHEMA,
            "evidence_class": _EVIDENCE_CLASS,
            "causal_id": self.causal_id,
            "world_id": self.world_id,
            "observed_at": self.observed_at.isoformat(),
            "scope": {
                "first_seq": self.first_seq,
                "last_seq": self.last_seq,
                "first_event_at": self.first_event_at.isoformat(),
                "last_event_at": self.last_event_at.isoformat(),
            },
            "amplification": {
                "event_count": self.event_count,
                "root_event_count": self.root_event_count,
                "child_event_count": self.child_event_count,
                "turn_root_count": self.turn_root_count,
                "claimed_turn_root_count": self.claimed_turn_root_count,
                "propagation_event_count": self.propagation_event_count,
                "distinct_engram_count": self.distinct_engram_count,
                "revisit_count": self.revisit_count,
                "revisited_engram_count": self.revisited_engram_count,
                "max_propagation_depth": self.max_propagation_depth,
                "max_children_per_parent": self.max_children_per_parent,
                "events_per_settled_turn": _ratio(
                    self.event_count, self.settled_turn_count
                ),
                "propagations_per_settled_turn": _ratio(
                    self.propagation_event_count,
                    self.settled_turn_count,
                ),
            },
            "queue": {
                "queued_event_count": self.queued_event_count,
                "oldest_queued_age_ms": self.oldest_queued_age_ms,
                "max_observed_queue_wait_ms": self.max_observed_queue_wait_ms,
            },
            "settle_cost": {
                "turn_attempt_count": self.turn_attempt_count,
                "settled_turn_count": self.settled_turn_count,
                "terminal_turn_count": self.terminal_turn_count,
                "active_ms_total": self.active_ms_total,
                "active_ms_max": self.active_ms_max,
                "input_tokens": self.input_tokens,
                "output_tokens": self.output_tokens,
                "cached_tokens": self.cached_tokens,
                "cache_write_tokens": self.cache_write_tokens,
                "usage_complete_turn_count": self.usage_complete_turn_count,
            },
            "status_counts": dict(self.status_counts),
            "flow_counts": dict(self.flow_counts),
            "flow_contract": {
                "violation_event_count": self.flow_contract_violation_count,
                "violation_counts": dict(self.violation_counts),
            },
        }


def read_causal_amplification(
    conn: sqlite3.Connection,
    causal_id: str,
    *,
    world_id: str | None = None,
    observed_at: datetime | None = None,
) -> CausalAmplificationSnapshot | None:
    """Project one complete chain without writing or materializing counters."""

    causal_id = _require_id(causal_id, "causal_id")
    if world_id is not None:
        world_id = _require_id(world_id, "world_id")
    observed_at = _utc(observed_at or _utc_now(), "observed_at")

    clauses = ["causal_id = ?"]
    params: list[Any] = [causal_id]
    if world_id is not None:
        clauses.append("world_id = ?")
        params.append(world_id)
    rows = conn.execute(
        "SELECT seq, id, parent_event_id, world_id, engram_id, flow, domain, "
        "kind, source, status, content, metadata, attempts, created_at, "
        "updated_at FROM causal_events WHERE "
        + " AND ".join(clauses)
        + " ORDER BY seq ASC",
        params,
    )

    event_count = 0
    root_event_count = 0
    child_event_count = 0
    propagation_event_count = 0
    first_seq: int | None = None
    last_seq: int | None = None
    first_event_at: datetime | None = None
    last_event_at: datetime | None = None
    worlds: set[str] = set()
    status_counts: Counter[str] = Counter()
    flow_counts: Counter[str] = Counter()
    violation_counts: Counter[str] = Counter()
    violation_event_count = 0
    child_counts: Counter[str] = Counter()
    propagation_depths: dict[str, int] = {}
    max_propagation_depth = 0
    eligible_roots: dict[str, tuple[str, datetime, CausalEventStatus]] = {}
    event_created_at: dict[str, datetime] = {}
    usage_totals = Counter({key: 0 for key in _USAGE_KEYS})
    usage_complete_turn_count = 0

    for row in rows:
        (
            seq,
            event_id,
            parent_event_id,
            event_world_id,
            engram_id,
            raw_flow,
            raw_domain,
            raw_kind,
            raw_source,
            raw_status,
            content,
            raw_metadata,
            _attempts,
            raw_created_at,
            raw_updated_at,
        ) = row
        event_count += 1
        worlds.add(event_world_id)
        first_seq = seq if first_seq is None else first_seq
        last_seq = seq
        created_at = _parse_time(raw_created_at)
        updated_at = _parse_time(raw_updated_at) or created_at
        if created_at is None:
            raise ValueError(f"causal event {event_id} has no valid created_at")
        event_created_at[event_id] = created_at
        first_event_at = created_at if first_event_at is None else min(
            first_event_at, created_at
        )
        last_event_at = updated_at if last_event_at is None else max(
            last_event_at, updated_at
        )

        flow = CausalEventFlow(raw_flow) if raw_flow is not None else None
        domain = CausalEventDomain(raw_domain)
        kind = CausalEventKind(raw_kind)
        source = CausalEventSource(raw_source)
        status = CausalEventStatus(raw_status)
        status_counts[status.value] += 1
        flow_counts[flow.value if flow is not None else "internal"] += 1
        if parent_event_id is None:
            root_event_count += 1
        else:
            child_event_count += 1
            child_counts[parent_event_id] += 1

        metadata_invalid = False
        try:
            metadata = json.loads(raw_metadata or "{}")
        except (TypeError, json.JSONDecodeError):
            metadata = {}
            metadata_invalid = True
        if not isinstance(metadata, dict):
            metadata = {}
            metadata_invalid = True
        projection = _FlowProjection(
            flow=flow,
            domain=domain,
            kind=kind,
            source=source,
            status=status,
            content=content,
            metadata=metadata,
            parent_event_id=parent_event_id,
            engram_id=engram_id,
        )
        violations = list(causal_flow_violation_codes(projection))
        if metadata_invalid:
            violations.append("metadata_not_json_object")
        if violations:
            violation_event_count += 1
            violation_counts.update(dict.fromkeys(violations, 1))

        parent_depth = propagation_depths.get(parent_event_id or "", 0)
        depth = parent_depth + int(kind is CausalEventKind.PROPAGATION)
        propagation_depths[event_id] = depth
        if kind is CausalEventKind.PROPAGATION:
            propagation_event_count += 1
            max_propagation_depth = max(max_propagation_depth, depth)

        if engram_id is not None and not causal_turn_violation_codes(projection):
            eligible_roots[event_id] = (engram_id, created_at, status)

        if kind is CausalEventKind.ASSISTANT_RESULT and status is CausalEventStatus.SETTLED:
            usage = metadata.get("usage")
            if isinstance(usage, Mapping) and all(
                isinstance(usage.get(key), int)
                and not isinstance(usage.get(key), bool)
                and usage[key] >= 0
                for key in _USAGE_KEYS
            ):
                usage_complete_turn_count += 1
                for key in _USAGE_KEYS:
                    usage_totals[key] += int(usage[key])

    if event_count == 0:
        return None
    if len(worlds) != 1:
        raise ValueError("causal_id spans more than one world")
    resolved_world_id = next(iter(worlds))
    assert first_seq is not None and last_seq is not None
    assert first_event_at is not None and last_event_at is not None

    turn_clauses = ["e.causal_id = ?"]
    turn_params: list[Any] = [causal_id]
    if world_id is not None:
        turn_clauses.append("e.world_id = ?")
        turn_params.append(world_id)
    turn_rows = conn.execute(
        "SELECT t.event_id, t.state, t.started_at, t.settled_at "
        "FROM harness_turns t JOIN causal_events e ON e.id = t.event_id WHERE "
        + " AND ".join(turn_clauses)
        + " ORDER BY t.started_at ASC, t.id ASC",
        turn_params,
    )
    claimed_root_ids: set[str] = set()
    turn_attempt_count = 0
    settled_turn_count = 0
    terminal_turn_count = 0
    active_durations: list[float] = []
    observed_queue_waits: list[float] = []
    for event_id, state, raw_started_at, raw_settled_at in turn_rows:
        turn_attempt_count += 1
        if event_id in eligible_roots:
            claimed_root_ids.add(event_id)
        if state == "settled":
            settled_turn_count += 1
        if state in _TERMINAL_TURN_STATES:
            terminal_turn_count += 1
        started_at = _parse_time(raw_started_at)
        settled_at = _parse_time(raw_settled_at)
        duration = _elapsed_ms(
            started_at,
            observed_at if state == "running" else settled_at,
        )
        if duration is not None:
            active_durations.append(duration)
        queue_wait = _elapsed_ms(event_created_at.get(event_id), started_at)
        if queue_wait is not None:
            observed_queue_waits.append(queue_wait)

    visits: Counter[str] = Counter(
        eligible_roots[event_id][0]
        for event_id in claimed_root_ids
    )
    revisit_count = sum(max(0, count - 1) for count in visits.values())
    revisited_engram_count = sum(count > 1 for count in visits.values())

    queued_ages = [
        _elapsed_ms(created_at, observed_at) or 0.0
        for _event_id, (_engram_id, created_at, status) in eligible_roots.items()
        if status is CausalEventStatus.QUEUED
    ]
    observed_queue_waits.extend(queued_ages)

    return CausalAmplificationSnapshot(
        causal_id=causal_id,
        world_id=resolved_world_id,
        observed_at=observed_at,
        first_seq=first_seq,
        last_seq=last_seq,
        first_event_at=first_event_at,
        last_event_at=last_event_at,
        event_count=event_count,
        root_event_count=root_event_count,
        child_event_count=child_event_count,
        turn_root_count=len(eligible_roots),
        claimed_turn_root_count=len(claimed_root_ids),
        propagation_event_count=propagation_event_count,
        distinct_engram_count=len(visits),
        revisit_count=revisit_count,
        revisited_engram_count=revisited_engram_count,
        max_propagation_depth=max_propagation_depth,
        max_children_per_parent=max(child_counts.values(), default=0),
        queued_event_count=len(queued_ages),
        oldest_queued_age_ms=max(queued_ages, default=None),
        max_observed_queue_wait_ms=max(observed_queue_waits, default=None),
        turn_attempt_count=turn_attempt_count,
        settled_turn_count=settled_turn_count,
        terminal_turn_count=terminal_turn_count,
        active_ms_total=round(sum(active_durations), 3),
        active_ms_max=max(active_durations, default=0.0),
        input_tokens=usage_totals["input_count"],
        output_tokens=usage_totals["output_count"],
        cached_tokens=usage_totals["cached_count"],
        cache_write_tokens=usage_totals["cache_write_count"],
        usage_complete_turn_count=usage_complete_turn_count,
        status_counts=tuple(
            (status.value, status_counts[status.value])
            for status in CausalEventStatus
        ),
        flow_counts=tuple(
            (key, flow_counts[key])
            for key in ("content", "spectrum", "tunnel", "internal")
        ),
        flow_contract_violation_count=violation_event_count,
        violation_counts=tuple(sorted(violation_counts.items())),
    )


__all__ = [
    "CausalAmplificationSnapshot",
    "read_causal_amplification",
]

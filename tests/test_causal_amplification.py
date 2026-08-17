"""Durable causal-chain amplification projection contracts."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pulse_system.core.causality import CausalLedger
from pulse_system.core.types import (
    CausalEventDomain,
    CausalEventFlow,
    CausalEventKind,
    CausalEventSource,
)
from pulse_system.substrate.storage import Storage


UTC = timezone.utc


def _settle(
    ledger: CausalLedger,
    event_id: str,
    engram_id: str,
    content: str,
    *,
    input_count: int,
    output_count: int,
):
    turn = ledger.begin_turn(event_id, engram_id, content)
    settled_turn, result = ledger.settle_turn(
        turn.id,
        f"answer for {content}",
        usage={
            "input_count": input_count,
            "output_count": output_count,
            "cached_count": 1,
            "cache_write_count": 0,
        },
    )
    return settled_turn, result


def _stamp(value: datetime) -> str:
    return value.isoformat()


def _set_event_times(
    storage: Storage,
    event_id: str,
    created_at: datetime,
    updated_at: datetime,
) -> None:
    storage._conn.execute(
        "UPDATE causal_events SET created_at = ?, updated_at = ? WHERE id = ?",
        (_stamp(created_at), _stamp(updated_at), event_id),
    )


def _set_turn_times(
    storage: Storage,
    turn_id: str,
    started_at: datetime,
    settled_at: datetime,
) -> None:
    storage._conn.execute(
        "UPDATE harness_turns SET started_at = ?, updated_at = ?, settled_at = ? "
        "WHERE id = ?",
        (
            _stamp(started_at),
            _stamp(settled_at),
            _stamp(settled_at),
            turn_id,
        ),
    )


def _chain(storage: Storage):
    for engram_id in ("e1", "e2", "e3"):
        storage.create_engram(engram_id=engram_id)
    ledger = CausalLedger(storage)
    root = ledger.enqueue(
        world_id="world-amplification",
        flow=CausalEventFlow.CONTENT,
        domain=CausalEventDomain.PULSE,
        kind=CausalEventKind.STIMULUS,
        source=CausalEventSource.USER,
        engram_id="e1",
        content="root",
    )
    turn1, result1 = _settle(
        ledger,
        root.id,
        "e1",
        "root",
        input_count=10,
        output_count=1,
    )
    propagation1 = ledger.enqueue(
        world_id=root.world_id,
        flow=CausalEventFlow.CONTENT,
        domain=CausalEventDomain.PULSE,
        kind=CausalEventKind.PROPAGATION,
        source=CausalEventSource.PROPAGATION,
        engram_id="e2",
        content="hop one",
        parent_event_id=result1.id,
        causal_id=root.causal_id,
    )
    turn2, result2 = _settle(
        ledger,
        propagation1.id,
        "e2",
        "hop one",
        input_count=20,
        output_count=2,
    )
    branch = ledger.enqueue(
        world_id=root.world_id,
        flow=CausalEventFlow.CONTENT,
        domain=CausalEventDomain.PULSE,
        kind=CausalEventKind.PROPAGATION,
        source=CausalEventSource.PROPAGATION,
        engram_id="e3",
        content="branch",
        parent_event_id=result1.id,
        causal_id=root.causal_id,
    )
    propagation2 = ledger.enqueue(
        world_id=root.world_id,
        flow=CausalEventFlow.CONTENT,
        domain=CausalEventDomain.PULSE,
        kind=CausalEventKind.PROPAGATION,
        source=CausalEventSource.PROPAGATION,
        engram_id="e1",
        content="return visit",
        parent_event_id=result2.id,
        causal_id=root.causal_id,
    )
    turn3, result3 = _settle(
        ledger,
        propagation2.id,
        "e1",
        "return visit",
        input_count=30,
        output_count=3,
    )
    queued = ledger.enqueue(
        world_id=root.world_id,
        flow=CausalEventFlow.CONTENT,
        domain=CausalEventDomain.PULSE,
        kind=CausalEventKind.PROPAGATION,
        source=CausalEventSource.PROPAGATION,
        engram_id="e3",
        content="deep queued hop",
        parent_event_id=result3.id,
        causal_id=root.causal_id,
    )

    base = datetime(2026, 8, 5, 12, 0, tzinfo=UTC)
    event_times = {
        root.id: (0, 3),
        result1.id: (3, 3),
        propagation1.id: (4, 10),
        result2.id: (10, 10),
        propagation2.id: (11, 17),
        branch.id: (15, 15),
        result3.id: (17, 17),
        queued.id: (20, 20),
    }
    for event_id, (created, updated) in event_times.items():
        _set_event_times(
            storage,
            event_id,
            base + timedelta(seconds=created),
            base + timedelta(seconds=updated),
        )
    _set_turn_times(
        storage,
        turn1.id,
        base + timedelta(seconds=1),
        base + timedelta(seconds=3),
    )
    _set_turn_times(
        storage,
        turn2.id,
        base + timedelta(seconds=6),
        base + timedelta(seconds=10),
    )
    _set_turn_times(
        storage,
        turn3.id,
        base + timedelta(seconds=12),
        base + timedelta(seconds=17),
    )
    storage._conn.commit()
    return ledger, root, branch, base


def test_complete_chain_projection_counts_sweep_queue_and_settle_cost():
    storage = Storage(":memory:")
    try:
        ledger, root, branch, base = _chain(storage)
        observed_at = base + timedelta(seconds=30)
        before = storage._conn.execute(
            "SELECT id, status, updated_at FROM causal_events ORDER BY seq"
        ).fetchall()

        snapshot = ledger.causal_amplification(
            root.causal_id,
            world_id=root.world_id,
            observed_at=observed_at,
        )

        assert snapshot is not None
        payload = snapshot.to_dict()
        assert payload["schema"] == "causal-amplification.v1"
        assert payload["evidence_class"] == "durable_causal_ledger_projection"
        assert payload["amplification"] == {
            "event_count": 8,
            "root_event_count": 1,
            "child_event_count": 7,
            "turn_root_count": 5,
            "claimed_turn_root_count": 3,
            "propagation_event_count": 4,
            "distinct_engram_count": 2,
            "revisit_count": 1,
            "revisited_engram_count": 1,
            "max_propagation_depth": 3,
            "max_children_per_parent": 2,
            "events_per_settled_turn": 2.666667,
            "propagations_per_settled_turn": 1.333333,
        }
        assert payload["queue"] == {
            "queued_event_count": 2,
            "oldest_queued_age_ms": 15_000.0,
            "max_observed_queue_wait_ms": 15_000.0,
        }
        assert payload["settle_cost"] == {
            "turn_attempt_count": 3,
            "settled_turn_count": 3,
            "terminal_turn_count": 3,
            "active_ms_total": 11_000.0,
            "active_ms_max": 5_000.0,
            "input_tokens": 60,
            "output_tokens": 6,
            "cached_tokens": 3,
            "cache_write_tokens": 0,
            "usage_complete_turn_count": 3,
        }
        assert payload["status_counts"]["settled"] == 6
        assert payload["status_counts"]["queued"] == 2
        assert payload["flow_counts"] == {
            "content": 5,
            "spectrum": 0,
            "tunnel": 0,
            "internal": 3,
        }
        assert payload["flow_contract"] == {
            "violation_event_count": 0,
            "violation_counts": {},
        }
        assert storage._conn.execute(
            "SELECT id, status, updated_at FROM causal_events ORDER BY seq"
        ).fetchall() == before

        # A pre-contract bad row remains visible as evidence, but is removed
        # from executable visit/queue counts rather than counted as a sweep.
        storage._conn.execute(
            "UPDATE causal_events SET flow = 'spectrum', domain = 'system', "
            "kind = 'system', source = 'system', content = ? WHERE id = ?",
            ("legacy prose", branch.id),
        )
        storage._conn.commit()
        violated = ledger.causal_amplification(
            root.causal_id,
            observed_at=observed_at,
        )
        assert violated is not None
        violation_payload = violated.to_dict()
        assert violation_payload["flow_contract"]["violation_event_count"] == 1
        assert violation_payload["flow_contract"]["violation_counts"] == {
            "spectrum_cannot_execute": 1,
            "spectrum_content_forbidden": 1,
        }
        assert violation_payload["amplification"]["turn_root_count"] == 4
        assert violation_payload["queue"]["queued_event_count"] == 1
    finally:
        storage.close()


def test_projection_survives_restart_and_unknown_chain_is_none(tmp_path):
    path = tmp_path / "causal-amplification.sqlite"
    first = Storage(path)
    ledger, root, _branch, base = _chain(first)
    expected = ledger.causal_amplification(
        root.causal_id,
        observed_at=base + timedelta(seconds=30),
    )
    assert expected is not None
    first.close()

    second = Storage(path)
    try:
        recovered = CausalLedger(second).causal_amplification(
            root.causal_id,
            observed_at=base + timedelta(seconds=30),
        )
        assert recovered is not None
        assert recovered.to_dict() == expected.to_dict()
        assert CausalLedger(second).causal_amplification("missing-chain") is None
    finally:
        second.close()

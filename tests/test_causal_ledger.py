"""Durable causal-ledger contracts and transaction boundaries."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pulse_system.agent.harness.base import HarnessTurnResult
from pulse_system.core.causality import (
    CausalFlowInvariantError,
    CausalLedger,
    CausalTransitionError,
)
from pulse_system.core.causality.ledger import EngramPulseActivity, RuntimeFence
from pulse_system.core.types import (
    CausalEvent,
    CausalEventDomain,
    CausalEventFlow,
    CausalEventKind,
    CausalEventSource,
    CausalEventStatus,
    GenerationTransitionState,
    HarnessTurnState,
    Message,
    MessageRole,
    RuntimeLeaseLostError,
)
from pulse_system.substrate.storage import Storage


@pytest.fixture
def store() -> Storage:
    storage = Storage(":memory:")
    try:
        yield storage
    finally:
        storage.close()


@pytest.fixture
def ledger(store: Storage) -> CausalLedger:
    store.create_engram(engram_id="e1")
    store.create_engram(engram_id="e2")
    return CausalLedger(store)


def _event(ledger: CausalLedger, **overrides):
    values = {
        "world_id": "world-1",
        "flow": None,
        "domain": CausalEventDomain.PULSE,
        "kind": CausalEventKind.STIMULUS,
        "source": CausalEventSource.SELF,
        "engram_id": "e1",
        "content": "wake",
    }
    values.update(overrides)
    return ledger.enqueue(**values)


def test_frozen_event_enums_and_metadata_are_strict(ledger: CausalLedger):
    assert [member.value for member in CausalEventKind] == [
        "stimulus",
        "spontaneous",
        "pulse",
        "propagation",
        "tool_call",
        "tool_result",
        "habitat_observation",
        "habitat_action",
        "habitat_consequence",
        "delegation_request",
        "delegation_result",
        "generation_transition",
        "assistant_result",
        "system",
    ]
    event = _event(
        ledger,
        flow=CausalEventFlow.CONTENT,
        domain="habitat",
        kind="habitat_observation",
        source="habitat",
        metadata={"tool_name": "observe", "count": 1, "ok": True},
    )
    assert event.flow is CausalEventFlow.CONTENT
    assert event.domain is CausalEventDomain.HABITAT
    assert event.kind is CausalEventKind.HABITAT_OBSERVATION
    assert event.source is CausalEventSource.HABITAT

    with pytest.raises(ValueError):
        _event(ledger, flow="habitat")
    with pytest.raises(ValueError):
        _event(ledger, kind="not-a-frozen-kind")
    with pytest.raises(ValueError):
        _event(ledger, metadata={"tool_payload": {"path": "secret"}})


def test_flow_contract_guards_private_insert_and_historical_claim(
    ledger: CausalLedger,
    store: Storage,
):
    before = store._conn.execute("SELECT COUNT(*) FROM causal_events").fetchone()[0]
    malformed = CausalEvent(
        world_id="world-1",
        flow=CausalEventFlow.SPECTRUM,
        domain=CausalEventDomain.SYSTEM,
        kind=CausalEventKind.STIMULUS,
        source=CausalEventSource.SYSTEM,
        status=CausalEventStatus.QUEUED,
        engram_id="e1",
        content="natural language must not ride the spectrum",
    )
    with pytest.raises(CausalFlowInvariantError, match="spectrum_content_forbidden"):
        with ledger._transaction() as conn:
            ledger._insert_event_uncommitted(conn, malformed)
    assert store._conn.execute("SELECT COUNT(*) FROM causal_events").fetchone()[0] == before

    root = _event(ledger, content="settle a parent")
    turn = ledger.begin_turn(root.id, "e1", root.content)
    ledger.settle_turn(turn.id, "parent settled")
    spectrum = ledger.record_child(
        root.id,
        kind=CausalEventKind.SYSTEM,
        domain=CausalEventDomain.SYSTEM,
        source=CausalEventSource.SYSTEM,
        flow=CausalEventFlow.SPECTRUM,
        metadata={"gain": 0.75, "state": "steady"},
    )
    # Simulate a pre-contract database row. Historical violations remain
    # readable evidence, but claim/begin_turn must fail closed.
    with store._lock:
        store._conn.execute(
            "UPDATE causal_events SET status = 'queued', kind = 'stimulus', "
            "content = ? WHERE id = ?",
            ("legacy spectrum prose", spectrum.id),
        )
        store._conn.commit()
    valid = _event(ledger, content="valid content after legacy corruption")

    assert ledger.claim_next_event("e1") == valid
    with pytest.raises(CausalTransitionError, match="spectrum_cannot"):
        ledger.begin_turn(spectrum.id, "e1")


@pytest.mark.parametrize(
    "overrides, code",
    (
        (
            {
                "flow": CausalEventFlow.CONTENT,
                "content": None,
            },
            "content_flow_requires_natural_content",
        ),
        (
            {
                "flow": CausalEventFlow.TUNNEL,
                "domain": CausalEventDomain.SYSTEM,
                "kind": CausalEventKind.STIMULUS,
                "source": CausalEventSource.DELEGATION,
            },
            "tunnel_kind_invalid",
        ),
        (
            {
                "flow": None,
                "kind": CausalEventKind.PROPAGATION,
                "source": CausalEventSource.PROPAGATION,
            },
            "propagation_requires_content_flow",
        ),
    ),
)
def test_invalid_cross_subject_flow_combinations_fail_before_enqueue(
    ledger: CausalLedger,
    overrides,
    code: str,
):
    with pytest.raises(CausalFlowInvariantError, match=code):
        _event(ledger, **overrides)


def test_schema_has_frozen_tables_indexes_and_nullable_unique_key(
    store: Storage,
):
    tables = {
        row[0]
        for row in store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    assert {"causal_events", "harness_turns", "generation_transitions"} <= tables

    event_columns = {
        row[1]: row for row in store._conn.execute("PRAGMA table_info(causal_events)")
    }
    assert event_columns["flow"][3] == 0
    assert event_columns["idempotency_key"][3] == 0
    assert event_columns["idempotency_key"][5] == 0

    turn_indexes = {
        row[1]
        for row in store._conn.execute("PRAGMA index_list(harness_turns)")
    }
    assert "idx_harness_turns_one_running" in turn_indexes
    assert "idx_harness_turns_one_running_engram" in turn_indexes
    assert "idx_causal_events_status_seq" in {
        row[1]
        for row in store._conn.execute("PRAGMA index_list(causal_events)")
    }


def test_idempotency_is_explicit_and_seq_is_monotonic(ledger: CausalLedger):
    first = _event(ledger, idempotency_key="external-1")
    same = _event(ledger, idempotency_key="external-1", content="ignored")
    second = _event(ledger, idempotency_key="external-2")
    assert same.id == first.id
    assert same.content == first.content
    assert first.seq is not None and second.seq == first.seq + 1
    assert ledger.list_events(after_seq=first.seq, limit=10)[0].id == second.id

    with pytest.raises(CausalTransitionError):
        _event(ledger, idempotency_key="external-1", event_id="another-id")


def test_parent_preserves_causal_chain_and_rejects_cross_world_or_chain(
    ledger: CausalLedger,
):
    root = _event(ledger)
    child = _event(
        ledger,
        parent_event_id=root.id,
        causal_id=root.causal_id,
        kind=CausalEventKind.PULSE,
    )
    assert child.causal_id == root.causal_id
    assert ledger.get_children(root.id) == [child]

    with pytest.raises(ValueError):
        _event(
            ledger,
            parent_event_id=root.id,
            causal_id="other-causal",
        )
    with pytest.raises(ValueError):
        _event(ledger, parent_event_id=root.id, world_id="other-world")


def test_begin_persists_running_event_and_injection_before_harness_call(
    ledger: CausalLedger,
    store: Storage,
):
    event = _event(ledger)
    turn = ledger.begin_turn(event.id, "e1", "input")
    assert turn.state is HarnessTurnState.RUNNING
    assert turn.cursor_before == 0
    assert turn.cursor_after == 1
    assert ledger.get_event(event.id).status is CausalEventStatus.RUNNING
    assert store.get_session("e1")[0].role is MessageRole.INJECTION


def test_turn_world_scope_is_derived_atomically_from_owning_causal_event(
    ledger: CausalLedger,
):
    current_event = _event(
        ledger,
        world_id="world-current",
        engram_id="e1",
        idempotency_key="turn-world-current",
    )
    foreign_event = _event(
        ledger,
        world_id="world-foreign",
        engram_id="e2",
        idempotency_key="turn-world-foreign",
    )
    current_turn = ledger.begin_turn(current_event.id, "e1", "current")
    foreign_turn = ledger.begin_turn(foreign_event.id, "e2", "foreign")

    assert ledger.get_turn_for_world(current_turn.id, "world-current") == current_turn
    assert ledger.get_turn_for_world(foreign_turn.id, "world-foreign") == foreign_turn
    assert ledger.get_turn_for_world(current_turn.id, "world-foreign") is None
    assert ledger.get_turn_for_world(foreign_turn.id, "world-current") is None


def test_database_rejects_two_running_turns_for_one_engram(
    ledger: CausalLedger,
    store: Storage,
):
    first_event = _event(ledger, idempotency_key="running-db-1")
    second_event = _event(ledger, idempotency_key="running-db-2")
    first = ledger.begin_turn(first_event.id, "e1", "first")
    now = first.started_at.isoformat()
    with pytest.raises(sqlite3.IntegrityError):
        store._conn.execute(
            "INSERT INTO harness_turns ("
            "id, event_id, engram_id, state, cursor_before, cursor_after, "
            "input_message_id, prompt_accepted, started_at, updated_at) "
            "VALUES (?, ?, ?, 'running', 0, 0, NULL, NULL, ?, ?)",
            ("duplicate-running", second_event.id, "e1", now, now),
        )
    store._conn.rollback()
    assert ledger.get_turn(first.id).state is HarnessTurnState.RUNNING


def test_settle_is_one_transaction_and_updates_cursor_result_and_terminals(
    ledger: CausalLedger,
    store: Storage,
):
    event = _event(ledger)
    turn = ledger.begin_turn(event.id, "e1", "input")
    settled, result = ledger.settle_turn(
        turn.id,
        Message(role=MessageRole.ASSISTANT, content="answer"),
        usage={"input_count": 1, "output_count": 1},
        safe_trace={"tool_name": "none", "ok": True},
    )
    assert settled.state is HarnessTurnState.SETTLED
    assert result.parent_event_id == event.id
    assert result.causal_id == event.causal_id
    assert result.kind is CausalEventKind.ASSISTANT_RESULT
    assert result.flow is None
    assert ledger.get_event(event.id).status is CausalEventStatus.SETTLED
    assert [m.content for m in store.get_session("e1")] == ["input", "answer"]
    assert store.load_component_state("harness.pulse.inputs.v1") == {
        "cursors": {"e1": 2},
        "version": 1,
    }


def test_settle_atomically_binds_the_returned_harness_session(
    ledger: CausalLedger,
) -> None:
    event = _event(ledger, idempotency_key="settled-session-binding")
    turn = ledger.begin_turn(event.id, "e1", "input")
    session_file = "C:/pulse/session-e1.jsonl"

    settled, result = ledger.settle_turn(
        turn.id,
        HarnessTurnResult(
            engram_id="e1",
            session_id="pi-session-e1",
            session_file=session_file,
            content="answer",
            stop_reason="stop",
            provider_requests=1,
        ),
    )

    assert settled.session_id == "pi-session-e1"
    assert settled.session_file == str(Path(session_file).resolve())
    assert settled.result_event_id == result.id


def test_settle_rejects_a_session_that_conflicts_with_the_claimed_turn(
    ledger: CausalLedger,
    store: Storage,
) -> None:
    event = _event(ledger, idempotency_key="settled-session-conflict")
    turn = ledger.begin_turn(
        event.id,
        "e1",
        "input",
        session_id="claimed-session",
        session_file="C:/pulse/claimed.jsonl",
    )

    with pytest.raises(CausalTransitionError, match="session id differs"):
        ledger.settle_turn(
            turn.id,
            HarnessTurnResult(
                engram_id="e1",
                session_id="other-session",
                session_file="C:/pulse/other.jsonl",
                content="answer",
                stop_reason="stop",
                provider_requests=1,
            ),
        )

    assert ledger.get_turn(turn.id).state is HarnessTurnState.RUNNING
    assert ledger.get_event(event.id).status is CausalEventStatus.RUNNING
    assert [message.content for message in store.get_session("e1")] == ["input"]


def test_settle_fault_rolls_back_message_cursor_child_and_terminal_state(
    ledger: CausalLedger,
    store: Storage,
    monkeypatch: pytest.MonkeyPatch,
):
    event = _event(ledger)
    turn = ledger.begin_turn(event.id, "e1", "input")

    original = ledger._insert_event_uncommitted

    def insert_then_fail(conn, child):
        original(conn, child)
        raise RuntimeError("fault after result child")

    monkeypatch.setattr(ledger, "_insert_event_uncommitted", insert_then_fail)
    with pytest.raises(RuntimeError, match="fault after result child"):
        ledger.settle_turn(turn.id, "answer")

    assert [m.content for m in store.get_session("e1")] == ["input"]
    assert store.load_component_state("harness.pulse.inputs.v1") is None
    assert ledger.get_children(event.id) == []
    assert ledger.get_turn(turn.id).state is HarnessTurnState.RUNNING
    assert ledger.get_event(event.id).status is CausalEventStatus.RUNNING


def test_fenced_settle_commits_engram_activity_in_the_causal_transaction(
    ledger: CausalLedger,
    store: Storage,
):
    lease_now = datetime.now(timezone.utc)
    lease = store.acquire_runtime_lease(
        "runtime-a",
        now=lease_now,
        ttl_sec=30,
    )
    fence = RuntimeFence(owner_id=lease.owner_id, epoch=lease.epoch)
    event = _event(ledger, idempotency_key="fenced-activity")
    turn = ledger.begin_turn(event.id, "e1", "input", runtime_fence=fence)
    activity_at = datetime.now(timezone.utc)

    settled, result = ledger.settle_turn(
        turn.id,
        "answer",
        runtime_fence=fence,
        engram_activity=EngramPulseActivity(
            last_pulse_at=activity_at,
            token_count=7,
        ),
    )

    assert settled.state is HarnessTurnState.SETTLED
    assert result.status is CausalEventStatus.SETTLED
    engram = store.get_engram("e1")
    assert engram is not None
    assert engram.last_pulse_at == activity_at
    assert engram.total_pulses == 1
    assert engram.metadata.recent_activity == pytest.approx(0.2)
    assert engram.metadata.token_count == 7


def test_engram_activity_fault_rolls_back_fenced_settlement(
    ledger: CausalLedger,
    store: Storage,
):
    lease_now = datetime.now(timezone.utc)
    lease = store.acquire_runtime_lease(
        "runtime-a",
        now=lease_now,
        ttl_sec=30,
    )
    fence = RuntimeFence(owner_id=lease.owner_id, epoch=lease.epoch)
    event = _event(ledger, idempotency_key="fenced-activity-rollback")
    turn = ledger.begin_turn(event.id, "e1", "input", runtime_fence=fence)
    store._conn.execute(
        """CREATE TRIGGER fail_engram_pulse_activity
           BEFORE UPDATE OF total_pulses ON engrams
           WHEN NEW.id = 'e1'
           BEGIN SELECT RAISE(ABORT, 'fault during engram activity'); END"""
    )
    try:
        with pytest.raises(sqlite3.IntegrityError, match="engram activity"):
            ledger.settle_turn(
                turn.id,
                "answer",
                runtime_fence=fence,
                engram_activity=EngramPulseActivity(
                    last_pulse_at=datetime.now(timezone.utc),
                    token_count=7,
                ),
            )
    finally:
        store._conn.execute("DROP TRIGGER fail_engram_pulse_activity")
        store._conn.commit()

    assert [message.content for message in store.get_session("e1")] == ["input"]
    assert ledger.get_children(event.id) == []
    assert ledger.get_turn(turn.id).state is HarnessTurnState.RUNNING
    assert ledger.get_event(event.id).status is CausalEventStatus.RUNNING
    engram = store.get_engram("e1")
    assert engram is not None
    assert engram.last_pulse_at is None
    assert engram.total_pulses == 0
    assert engram.metadata.recent_activity == 0.0
    assert engram.metadata.token_count == 0


def test_begin_fault_rolls_back_injection_and_event_claim(
    ledger: CausalLedger,
    store: Storage,
    monkeypatch: pytest.MonkeyPatch,
):
    event = _event(ledger)

    def fail_insert_turn(conn, turn):
        raise RuntimeError("fault before begin commit")

    monkeypatch.setattr(ledger, "_insert_turn_uncommitted", fail_insert_turn)
    with pytest.raises(RuntimeError, match="fault before begin commit"):
        ledger.begin_turn(event.id, "e1", "input")
    assert store.get_session("e1") == []
    assert ledger.get_event(event.id).status is CausalEventStatus.QUEUED
    assert ledger.list_turns() == []


def test_false_acceptance_retries_same_event_and_keeps_failed_history(
    ledger: CausalLedger,
):
    event = _event(ledger)
    first = ledger.begin_turn(event.id, "e1", "first")
    returned = ledger.fail_turn(
        first.id,
        acceptance=False,
        code="rejected",
        phase="prompt",
        retry_allowed=True,
    )
    assert returned.status is CausalEventStatus.QUEUED
    second = ledger.begin_turn(event.id, "e1", "first")
    assert second.id != first.id
    returned = ledger.fail_turn(
        second.id,
        acceptance=False,
        code="rejected_again",
        phase="prompt",
        retry_allowed=False,
    )
    assert returned.status is CausalEventStatus.FAILED
    assert [turn.state for turn in ledger.list_turns()] == [
        HarnessTurnState.FAILED,
        HarnessTurnState.FAILED,
    ]


def test_refusal_retry_reuses_one_input_projection_and_settles_cursor(
    ledger: CausalLedger,
    store: Storage,
):
    event = _event(ledger, idempotency_key="projection-retry")
    first = ledger.begin_turn(event.id, "e1", "same input")
    ledger.fail_turn(
        first.id,
        acceptance=False,
        code="refused",
        phase="prompt",
        retry_allowed=True,
    )
    second = ledger.begin_turn(event.id, "e1", "same input")
    assert second.input_message_id == first.input_message_id
    assert second.cursor_before == first.cursor_before == 0
    assert second.cursor_after == first.cursor_after == 1
    assert [message.content for message in store.get_session("e1")] == [
        "same input"
    ]

    settled, _child = ledger.settle_turn(second.id, "answer")
    assert settled.cursor_after == 2
    assert store.load_component_state("harness.pulse.inputs.v1")["cursors"] == {
        "e1": 2
    }
    assert [message.content for message in store.get_session("e1")] == [
        "same input",
        "answer",
    ]


def test_cursor_before_uses_persisted_boundary_with_consumed_prefix_and_retry(
    ledger: CausalLedger,
    store: Storage,
):
    store.append_messages(
        "e1",
        [
            Message(role=MessageRole.USER, content="old-1"),
            Message(role=MessageRole.ASSISTANT, content="old-2"),
        ],
    )
    store.save_component_state(
        "harness.pulse.inputs.v1",
        {"version": 1, "cursors": {"e1": 2}},
    )
    event = _event(ledger, idempotency_key="prefix-retry")
    first = ledger.begin_turn(event.id, "e1", "new input")
    assert first.cursor_before == 2
    assert first.cursor_after == 3
    ledger.fail_turn(first.id, acceptance=False, retry_allowed=True)

    second = ledger.begin_turn(event.id, "e1", "new input")
    assert second.cursor_before == 2
    assert second.cursor_after == 3
    assert second.input_message_id == first.input_message_id
    assert len(store.get_session("e1")) == 3
    ledger.settle_turn(second.id, "new answer")
    assert store.load_component_state("harness.pulse.inputs.v1")["cursors"]["e1"] == 4


def test_accepted_unknown_failure_and_recovery_advance_cursor_without_replay(
    ledger: CausalLedger,
    store: Storage,
):
    event = _event(ledger)
    turn = ledger.begin_turn(event.id, "e1", "input")
    uncertain = ledger.fail_turn(
        turn.id,
        acceptance=None,
        code="disconnect",
        phase="rpc",
        retry_allowed=True,
    )
    assert uncertain.status is CausalEventStatus.UNCERTAIN
    assert store.load_component_state("harness.pulse.inputs.v1")["cursors"]["e1"] == 1
    with pytest.raises(CausalTransitionError):
        ledger.begin_turn(event.id, "e1")

    event2 = _event(ledger, idempotency_key="second")
    turn2 = ledger.begin_turn(event2.id, "e1", "another")
    report = ledger.recover_inflight(runtime_fence=None)
    assert report.turn_ids == (turn2.id,)
    assert ledger.get_event(event2.id).status is CausalEventStatus.UNCERTAIN
    cursor = store.load_component_state("harness.pulse.inputs.v1")["cursors"]["e1"]
    assert cursor == turn2.cursor_after
    assert ledger.recover_inflight(runtime_fence=None).turn_ids == ()


def test_generation_transitions_and_recovery(ledger: CausalLedger, store: Storage):
    generation = ledger.begin_generation("e1", generation_id="gen-1")
    assert generation.state is GenerationTransitionState.PREPARED
    generation = ledger.transition_generation(
        generation.id, GenerationTransitionState.SUMMARIZING
    )
    assert generation.state is GenerationTransitionState.SUMMARIZING
    report = ledger.recover_inflight(runtime_fence=None)
    assert report.generation_ids == (generation.id,)
    assert ledger.get_generation(generation.id).state is GenerationTransitionState.UNCERTAIN
    assert ledger.recover_generations() == ()

    # An uncertain predecessor remains occupied until explicit governance
    # resolves it; exercise the normal path with a different predecessor.
    second = ledger.begin_generation("e2", generation_id="gen-2")
    ledger.transition_generation(second.id, GenerationTransitionState.SUMMARIZING)
    ledger.transition_generation(second.id, GenerationTransitionState.ROTATING)
    committed = ledger.transition_generation(
        second.id,
        GenerationTransitionState.COMMITTED,
        successor_id="e2",
    )
    assert committed.state is GenerationTransitionState.COMMITTED
    assert committed.successor_id == "e2"


def test_generation_and_queue_handoff_are_fenced_by_one_runtime_epoch(
    ledger: CausalLedger,
    store: Storage,
):
    now = datetime.now(timezone.utc)
    lease = store.acquire_runtime_lease("runtime-a", now=now, ttl_sec=30)
    fence = RuntimeFence(owner_id=lease.owner_id, epoch=lease.epoch)
    generation = ledger.begin_generation(
        "e1",
        generation_id="fenced-generation",
        runtime_fence=fence,
    )
    ledger.transition_generation(
        generation.id,
        GenerationTransitionState.SUMMARIZING,
        runtime_fence=fence,
    )

    store.release_runtime_lease(
        lease.owner_id,
        lease.epoch,
        now=now + timedelta(milliseconds=1),
    )
    takeover = store.acquire_runtime_lease(
        "runtime-b",
        now=now + timedelta(milliseconds=2),
        ttl_sec=30,
    )
    takeover_fence = RuntimeFence(
        owner_id=takeover.owner_id,
        epoch=takeover.epoch,
    )

    with pytest.raises(RuntimeLeaseLostError):
        ledger.transition_generation(
            generation.id,
            GenerationTransitionState.ROTATING,
            runtime_fence=fence,
        )
    with pytest.raises(RuntimeLeaseLostError):
        ledger.enqueue(
            world_id="world-1",
            kind=CausalEventKind.STIMULUS,
            source=CausalEventSource.SELF,
            engram_id="e1",
            content="late",
            runtime_fence=fence,
        )
    with pytest.raises(RuntimeLeaseLostError):
        ledger.reassign_queued_events("e1", "e2", runtime_fence=fence)

    successor_event = ledger.enqueue(
        world_id="world-1",
        kind=CausalEventKind.STIMULUS,
        source=CausalEventSource.SELF,
        engram_id="e2",
        content="new owner is working",
        runtime_fence=takeover_fence,
    )
    successor_turn = ledger.begin_turn(
        successor_event.id,
        "e2",
        successor_event.content,
        runtime_fence=takeover_fence,
    )
    with pytest.raises(RuntimeLeaseLostError):
        ledger.recover_inflight(runtime_fence=fence)
    assert ledger.get_event(successor_event.id).status is CausalEventStatus.RUNNING
    assert ledger.get_turn(successor_turn.id).state is HarnessTurnState.RUNNING
    with pytest.raises(
        CausalTransitionError,
        match="belongs to another Runtime epoch",
    ):
        ledger.transition_generation(
            generation.id,
            GenerationTransitionState.ROTATING,
            runtime_fence=takeover_fence,
        )
    assert ledger.get_generation(generation.id).state is GenerationTransitionState.SUMMARIZING


def test_succession_handoff_moves_only_queued_future_events(
    ledger: CausalLedger,
):
    settled_event = _event(ledger, idempotency_key="settled-before-succession")
    settled_turn = ledger.begin_turn(settled_event.id, "e1", "settled input")
    ledger.settle_turn(settled_turn.id, "settled answer")

    running_event = _event(ledger, idempotency_key="running-before-succession")
    ledger.begin_turn(running_event.id, "e1", "running input")
    first = _event(ledger, idempotency_key="queued-before-succession-1")
    second = _event(ledger, idempotency_key="queued-before-succession-2")

    handed_off = ledger.reassign_queued_events("e1", "e2")

    assert handed_off == (first.id, second.id)
    assert ledger.get_event(first.id).engram_id == "e2"
    assert ledger.get_event(second.id).engram_id == "e2"
    assert ledger.get_event(running_event.id).engram_id == "e1"
    assert ledger.get_event(settled_event.id).engram_id == "e1"
    assert ledger.reassign_queued_events("e1", "e2") == ()


def test_succession_handoff_is_not_limited_by_sqlite_parameter_count(
    ledger: CausalLedger,
):
    queued = [
        _event(ledger, idempotency_key=f"large-handoff-{index}")
        for index in range(1_001)
    ]

    handed_off = ledger.reassign_queued_events("e1", "e2")

    assert handed_off == tuple(event.id for event in queued)
    assert ledger.get_event(queued[0].id).engram_id == "e2"
    assert ledger.get_event(queued[-1].id).engram_id == "e2"


def test_provisional_candidate_status_swap_is_atomic_and_compare_and_set(
    ledger: CausalLedger,
    store: Storage,
):
    store.mark_engram_provisional("e2")

    store.commit_engram_succession_status("e1", "e2")

    assert store.get_engram("e1").status.value == "archived"
    assert store.get_engram("e2").status.value == "active"
    with pytest.raises(ValueError, match="no longer provisional"):
        store.commit_engram_succession_status("e1", "e2")
    assert store.get_engram("e1").status.value == "archived"
    assert store.get_engram("e2").status.value == "active"


def test_atomic_succession_publication_rolls_back_every_core_fact_on_fault(
    ledger: CausalLedger,
    store: Storage,
):
    store.mark_engram_provisional("e2")
    generation = ledger.begin_generation("e1", generation_id="gen-publication")
    ledger.transition_generation(
        generation.id,
        GenerationTransitionState.SUMMARIZING,
    )
    ledger.transition_generation(
        generation.id,
        GenerationTransitionState.ROTATING,
        successor_id="e2",
    )
    queued = _event(ledger, idempotency_key="queued-before-publication")
    store._conn.execute(
        """CREATE TRIGGER fail_succession_publication
           BEFORE UPDATE OF state ON generation_transitions
           WHEN NEW.id = 'gen-publication' AND NEW.state = 'committed'
           BEGIN SELECT RAISE(ABORT, 'fault during succession publication'); END"""
    )
    store._conn.commit()

    try:
        with pytest.raises(sqlite3.IntegrityError, match="succession publication"):
            ledger.commit_succession_publication(
                generation.id,
                "e1",
                "e2",
                summary_turn_id=None,
                runtime_fence=None,
            )
    finally:
        store._conn.execute("DROP TRIGGER fail_succession_publication")
        store._conn.commit()

    assert store.get_engram("e1").status.value == "active"
    assert store.get_engram("e2").status.value == "provisional"
    assert ledger.get_event(queued.id).engram_id == "e1"
    assert (
        ledger.get_generation(generation.id).state
        is GenerationTransitionState.ROTATING
    )

    committed, handed_off = ledger.commit_succession_publication(
        generation.id,
        "e1",
        "e2",
        summary_turn_id=None,
        runtime_fence=None,
    )
    assert committed.state is GenerationTransitionState.COMMITTED
    assert handed_off == (queued.id,)
    assert store.get_engram("e1").status.value == "archived"
    assert store.get_engram("e2").status.value == "active"
    assert ledger.get_event(queued.id).engram_id == "e2"


def test_provisional_creation_rolls_back_identity_when_seed_insert_fails(
    store: Storage,
    monkeypatch,
):
    def fail_seed_insert(_engram_id, _message):
        raise RuntimeError("seed projection failed")

    monkeypatch.setattr(store, "_insert_message", fail_seed_insert)
    with pytest.raises(RuntimeError, match="seed projection failed"):
        store.create_provisional_engram(
            "candidate-rollback",
            None,
            [Message(role=MessageRole.ASSISTANT, content="lineage seed")],
            token_count=3,
        )

    assert store.get_engram("candidate-rollback") is None


def test_generation_transition_fault_rolls_back_and_recovery_is_explicit(
    ledger: CausalLedger,
    store: Storage,
):
    generation = ledger.begin_generation("e1", generation_id="gen-fault")
    store._conn.execute(
        """CREATE TRIGGER fail_generation_transition
           BEFORE UPDATE OF state ON generation_transitions
           WHEN NEW.id = OLD.id AND NEW.state = 'summarizing'
           BEGIN SELECT RAISE(ABORT, 'fault during generation transition'); END"""
    )
    try:
        with pytest.raises(sqlite3.IntegrityError, match="fault during generation"):
            ledger.transition_generation(
                generation.id, GenerationTransitionState.SUMMARIZING
            )
    finally:
        store._conn.execute("DROP TRIGGER fail_generation_transition")
        store._conn.commit()
    assert ledger.get_generation(generation.id).state is GenerationTransitionState.PREPARED

    transitioned = ledger.transition_generation(
        generation.id, GenerationTransitionState.SUMMARIZING
    )
    assert transitioned.state is GenerationTransitionState.SUMMARIZING
    recovered = ledger.recover_generations()
    assert [item.id for item in recovered] == [generation.id]
    assert ledger.get_generation(generation.id).state is GenerationTransitionState.UNCERTAIN


def test_write_failure_rolls_back_uncertain_failure_cursor(
    ledger: CausalLedger,
    store: Storage,
    monkeypatch: pytest.MonkeyPatch,
):
    event = _event(ledger)
    turn = ledger.begin_turn(event.id, "e1", "input")

    def fail_cursor(conn, engram_id, cursor):
        raise RuntimeError("fault while persisting recovery cursor")

    monkeypatch.setattr(ledger, "_save_cursor_uncommitted", fail_cursor)
    with pytest.raises(RuntimeError, match="fault while persisting recovery cursor"):
        ledger.fail_turn(turn.id, acceptance=None, code="unknown", phase="rpc")
    assert ledger.get_turn(turn.id).state is HarnessTurnState.RUNNING
    assert ledger.get_event(event.id).status is CausalEventStatus.RUNNING
    assert store.load_component_state("harness.pulse.inputs.v1") is None


def test_write_failure_rolls_back_explicit_refusal_failure(
    ledger: CausalLedger,
    store: Storage,
):
    event = _event(ledger, idempotency_key="refusal-rollback")
    turn = ledger.begin_turn(event.id, "e1", "input")
    store._conn.execute(
        """CREATE TRIGGER fail_refusal_event_update
           BEFORE UPDATE OF status ON causal_events
           WHEN NEW.id = OLD.id AND NEW.status = 'queued'
           BEGIN SELECT RAISE(ABORT, 'fault during refusal event update'); END"""
    )
    try:
        with pytest.raises(sqlite3.IntegrityError, match="fault during refusal"):
            ledger.fail_turn(
                turn.id,
                acceptance=False,
                code="refused",
                phase="prompt",
                retry_allowed=True,
            )
    finally:
        store._conn.execute("DROP TRIGGER fail_refusal_event_update")
        store._conn.commit()
    assert ledger.get_turn(turn.id).state is HarnessTurnState.RUNNING
    assert ledger.get_event(event.id).status is CausalEventStatus.RUNNING
    assert [message.content for message in store.get_session("e1")] == ["input"]


def test_storage_reopen_preserves_causal_rows_and_migration_is_idempotent(tmp_path):
    db = tmp_path / "causal.db"
    first = Storage(db)
    first.create_engram(engram_id="e1")
    ledger = CausalLedger(first)
    event = _event(ledger)
    first.close()

    second = Storage(db)
    try:
        restored = CausalLedger(second).get_event(event.id)
        assert restored is not None
        assert restored.seq == event.seq
        assert second.causal_ledger().get_event(event.id) == restored
    finally:
        second.close()


def test_foreign_key_violation_is_not_swallowed(store: Storage):
    with pytest.raises(sqlite3.IntegrityError):
        store._conn.execute(
            "INSERT INTO causal_events ("
            "id, causal_id, world_id, engram_id, domain, kind, source, "
            "metadata, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "bad",
                "bad",
                "world",
                "missing",
                "pulse",
                "stimulus",
                "self",
                "{}",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )

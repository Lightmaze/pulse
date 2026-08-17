"""Durable generation stages, crash isolation, and no-replay evidence."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from datetime import datetime, timezone

import pytest

from pulse_system.agent.harness.base import HarnessError, HarnessTurnResult
from pulse_system.core.causality import CausalLedger
from pulse_system.core.engram import EngramManager
from pulse_system.core.types import (
    CausalEventDomain,
    CausalEventKind,
    CausalEventSource,
    CausalEventStatus,
    GenerationTransitionState,
)
from pulse_system.service.runtime import (
    RuntimeService,
    RuntimeServiceConfig,
    ServiceError,
)
from pulse_system.substrate.storage import Storage


def _close_processless_harness_fixture(service: RuntimeService) -> None:
    """Test-only acknowledgement for an opaque but ownerless fake Harness."""

    if getattr(service, "_processless_fixture_closed", False):
        return
    report = service.close()
    assert report.physical_exit_proven is False
    keeper = service._lease_keeper
    assert keeper is not None
    lease = keeper.health().lease
    service.storage.release_runtime_lease(
        lease.owner_id,
        lease.epoch,
        now=datetime.now(timezone.utc),
    )
    service._close_shared_state()
    service._processless_fixture_closed = True


def _config(tmp_path):
    return RuntimeServiceConfig(
        db_path=tmp_path / "generation.db",
        metrics_path=tmp_path / "generation.metrics.jsonl",
        workspace=tmp_path,
        mock=True,
        tick_interval=0.01,
        default_max_wait=0.0,
        silence_threshold=0.0,
        base_spontaneous_rate=0.0,
    )


def _seed_world(tmp_path):
    service = RuntimeService(_config(tmp_path))
    front_id = service.front_engram_id
    world_id = service.world_id
    service.close()
    return front_id, world_id


def _seed_parent(ledger: CausalLedger, world_id: str, engram_id: str):
    return ledger.enqueue(
        world_id=world_id,
        domain=CausalEventDomain.PULSE,
        kind=CausalEventKind.STIMULUS,
        source=CausalEventSource.SELF,
        content="settled predecessor fact",
        engram_id=engram_id,
    )


def test_startup_explicitly_terminates_queued_summary_without_replay(tmp_path):
    front_id, world_id = _seed_world(tmp_path)

    store = Storage(tmp_path / "generation.db")
    ledger = CausalLedger(store)
    parent = _seed_parent(ledger, world_id, front_id)
    generation = ledger.begin_generation(
        front_id,
        parent_event_id=parent.id,
    )
    ledger.transition_generation(
        generation.id,
        GenerationTransitionState.SUMMARIZING,
    )
    summary = ledger.enqueue(
        world_id=world_id,
        domain=CausalEventDomain.GENERATION,
        kind=CausalEventKind.SPONTANEOUS,
        source=CausalEventSource.SELF,
        content="summary prompt must never be replayed after this crash",
        parent_event_id=generation.event_id,
        engram_id=front_id,
        metadata={
            "generation_id": generation.id,
            "generation_stage": "summary",
        },
        idempotency_key=f"generation-summary:{generation.id}",
    )

    # Simulate a hard process exit after enqueue and before begin_turn.  No
    # Runtime.close() is allowed to tidy the rows before the next process.
    store.close()

    reborn = RuntimeService(_config(tmp_path))
    try:
        recovered_generation = reborn.causal_ledger.get_generation(generation.id)
        recovered_summary = reborn.causal_ledger.get_event(summary.id)
        assert recovered_generation is not None
        assert recovered_generation.state is GenerationTransitionState.UNCERTAIN
        assert recovered_summary is not None
        assert recovered_summary.status is CausalEventStatus.FAILED

        [failed_turn] = reborn.causal_ledger.list_turns(
            engram_id=front_id,
        )
        assert failed_turn.event_id == summary.id
        assert failed_turn.state.value == "failed"
        assert failed_turn.error_code == "generation_recovered"
        assert reborn.snapshot()["recovery"]["isolated_generation_summaries"] == 1

        # The normal engine sees no claimable summary root and therefore
        # cannot call the Harness or create an assistant result for it.
        async def drive_without_summary_replay():
            await reborn.start()
            await asyncio.sleep(0.03)
            await reborn.stop()

        asyncio.run(drive_without_summary_replay())
        # The unrelated predecessor event remains queued behind the uncertain
        # lineage gate; only the recovered summary is terminalized. A later
        # explicit generation reconciliation decides which identity owns this
        # still-unstarted future stimulus.
        assert reborn.causal_ledger.claim_next_event(front_id).id == parent.id
        assert reborn.causal_ledger.get_children(summary.id) == []
    finally:
        if not reborn._closed:
            reborn.close()


def test_startup_archives_successor_created_before_generation_link(tmp_path):
    front_id, world_id = _seed_world(tmp_path)

    store = Storage(tmp_path / "generation.db")
    ledger = CausalLedger(store)
    generation = ledger.begin_generation(front_id, world_id=world_id)
    ledger.transition_generation(
        generation.id,
        GenerationTransitionState.SUMMARIZING,
    )
    ledger.transition_generation(
        generation.id,
        GenerationTransitionState.ROTATING,
    )
    candidate_id = EngramManager.generation_candidate_id(generation.id)
    # The candidate exists, but a crash lands before transition_generation can
    # persist successor_id.  It is therefore an orphan, not a successor.
    store.create_engram(engram_id=candidate_id)
    store.mark_engram_provisional(candidate_id)
    store.close()

    reborn = RuntimeService(_config(tmp_path))
    try:
        candidate = reborn.storage.get_engram(candidate_id)
        recovered_generation = reborn.causal_ledger.get_generation(generation.id)
        assert candidate is not None
        assert candidate.status.value == "archived"
        assert recovered_generation is not None
        assert recovered_generation.state is GenerationTransitionState.UNCERTAIN
        assert recovered_generation.successor_id is None
        assert reborn.snapshot()["recovery"]["archived_generation_orphans"] == 1
    finally:
        if not reborn._closed:
            reborn.close()


def test_direct_runtime_succession_uses_the_one_world_without_parent(tmp_path):
    service = RuntimeService(_config(tmp_path))
    old_id = service.front_engram_id
    world_id = service.world_id
    try:
        result = service.engrams.succession(old_id)
        generations = service.causal_ledger.list_generations(
            predecessor_id=old_id,
        )
        assert len(generations) == 1
        generation = generations[0]
        assert generation.state is GenerationTransitionState.COMMITTED
        assert generation.successor_id == result.new_id
        assert service.causal_ledger.get_event(generation.event_id).world_id == world_id
        assert generation.causal_id == result.causal_id
        assert generation.summary_turn_id == result.summary_turn_id
        assert service.world_id != "default"
        assert service.front_engram_id == result.new_id

        generation_event = service.causal_ledger.get_event(generation.event_id)
        assert generation_event is not None
        [summary_event] = [
            event
            for event in service.causal_ledger.get_children(generation.event_id)
            if event.metadata.get("generation_stage") == "summary"
        ]
        assert summary_event.world_id == service.world_id
        assert summary_event.causal_id == generation.causal_id
        summary_turn = service.causal_ledger.get_turn(result.summary_turn_id)
        assert summary_turn is not None
        assert summary_turn.event_id == summary_event.id
        assert summary_turn.state.value == "settled"
        [assistant_event] = service.causal_ledger.get_children(summary_event.id)
        assert assistant_event.kind is CausalEventKind.ASSISTANT_RESULT
        assert assistant_event.causal_id == generation.causal_id
    finally:
        if not service._closed:
            service.close()


def test_concurrent_inject_completes_its_local_commit_before_close(
    tmp_path,
    monkeypatch,
):
    """close cannot tear down SQLite midway through an accepted injection."""

    service = RuntimeService(_config(tmp_path))
    front_id = service.front_engram_id
    entered_owner_read = threading.Event()
    release_owner_read = threading.Event()
    original_get_engram = service.storage.get_engram
    call_lock = threading.Lock()
    call_count = 0

    def blocking_first_get(engram_id):
        nonlocal call_count
        with call_lock:
            call_count += 1
            should_block = call_count == 1
        if should_block:
            entered_owner_read.set()
            assert release_owner_read.wait(2.0)
        return original_get_engram(engram_id)

    monkeypatch.setattr(service.storage, "get_engram", blocking_first_get)
    injected: list[str] = []
    failures: list[BaseException] = []

    def inject() -> None:
        try:
            injected.append(service.inject(front_id, "accepted at close boundary"))
        except BaseException as exc:  # noqa: BLE001 - cross-thread assertion
            failures.append(exc)

    def close() -> None:
        try:
            service.close()
        except BaseException as exc:  # noqa: BLE001 - cross-thread assertion
            failures.append(exc)

    inject_thread = threading.Thread(target=inject)
    close_thread = threading.Thread(target=close)
    try:
        inject_thread.start()
        assert entered_owner_read.wait(2.0)
        close_thread.start()
        time.sleep(0.02)
        assert close_thread.is_alive(), "close crossed the in-flight input boundary"

        release_owner_read.set()
        inject_thread.join(2.0)
        close_thread.join(2.0)
        assert not inject_thread.is_alive()
        assert not close_thread.is_alive()
        assert failures == []
        assert len(injected) == 1
        assert service._closed is True

        reopened = Storage(tmp_path / "generation.db")
        try:
            event = CausalLedger(reopened).get_event(injected[0])
            assert event is not None
            assert event.status is CausalEventStatus.QUEUED
        finally:
            reopened.close()
    finally:
        release_owner_read.set()
        inject_thread.join(2.0)
        close_thread.join(2.0)
        if not service._closed:
            service.close()


def test_running_generation_recovery_is_idempotent_and_never_claimable(tmp_path):
    store = Storage(":memory:")
    store.create_engram(engram_id="e1")
    ledger = CausalLedger(store)
    generation = ledger.begin_generation("e1", world_id="world-recovery")
    ledger.transition_generation(generation.id, GenerationTransitionState.SUMMARIZING)

    first = ledger.recover_inflight(runtime_fence=None)
    second = ledger.recover_inflight(runtime_fence=None)
    assert first.generation_ids == (generation.id,)
    assert second.generation_ids == ()
    recovered = ledger.get_generation(generation.id)
    assert recovered is not None
    assert recovered.state is GenerationTransitionState.UNCERTAIN
    assert ledger.claim_next_event("e1") is None
    store.close()


def test_settle_transaction_fault_does_not_replay_successful_turn():
    store = Storage(":memory:")
    store.create_engram(engram_id="e1")
    ledger = CausalLedger(store)
    event = ledger.enqueue(
        world_id="world-recovery",
        domain=CausalEventDomain.PULSE,
        kind=CausalEventKind.STIMULUS,
        source=CausalEventSource.USER,
        content="fault before settle",
        engram_id="e1",
    )
    turn = ledger.begin_turn(event.id, "e1", "fault before settle")
    store._conn.execute(
        """
        CREATE TRIGGER recovery_fail_settle
        BEFORE UPDATE OF status ON causal_events
        WHEN NEW.id = OLD.id AND NEW.status = 'settled'
        BEGIN SELECT RAISE(ABORT, 'recovery settle fault'); END
        """
    )
    with pytest.raises(sqlite3.IntegrityError, match="recovery settle fault"):
        ledger.settle_turn(turn.id, "successful but uncommitted")
    store._conn.execute("DROP TRIGGER recovery_fail_settle")

    # The transaction rolled back, so the turn is still running until the
    # explicit recovery boundary classifies it.  Recovery never guesses that
    # the returned text was or was not observed by the outside Harness.
    report = ledger.recover_inflight(runtime_fence=None)
    assert report.turn_ids == (turn.id,)
    assert ledger.get_event(event.id).status is CausalEventStatus.UNCERTAIN
    assert ledger.claim_next_event("e1") is None


class _AcceptedThenFailsHarness:
    """Deterministic fake Pi: summary input was accepted, result is unknown."""

    def __init__(self):
        self.calls = 0

    def snapshot(self, engram_id):
        raise HarnessError(
            "pi_session_unknown",
            f"no session for {engram_id}",
            "run a turn first",
            phase="snapshot",
        )

    def run_turn(self, engram_id, prompt, *, timeout_sec=None, bootstrap_text=None):
        del engram_id, prompt, timeout_sec, bootstrap_text
        self.calls += 1
        raise HarnessError(
            "pi_connection_lost",
            "the fake transport accepted the prompt before the process died",
            "reconcile the uncertain generation",
            phase="prompt",
            retryable=False,
            prompt_accepted=True,
        )


def test_accepted_summary_failure_becomes_uncertain_and_blocks_new_generation(
    tmp_path,
):
    # This test exercises Manager's production ledger branch without a direct
    # LLM fallback.  It stops at the accepted/unknown summary boundary.
    from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
    from pulse_system.substrate.llm import LLMAdapter

    store = Storage(":memory:")
    engram = store.create_engram(engram_id="e1")
    harness = _AcceptedThenFailsHarness()
    manager = EngramManager(
        store,
        LLMAdapter(mock=True),
        ConnectionNetwork(store, ConnectionConfig()),
        harness=harness,
        causal_ledger=CausalLedger(store),
        causal_world_id="world-recovery",
    )
    with pytest.raises(HarnessError):
        manager.succession(engram.id)
    [generation] = manager.causal_ledger.list_generations(
        predecessor_id=engram.id,
    )
    assert generation.state is GenerationTransitionState.UNCERTAIN
    assert manager.causal_ledger.get_event(generation.event_id).status is CausalEventStatus.UNCERTAIN
    with pytest.raises(HarnessError, match="generation_blocked"):
        manager.succession(engram.id)
    store.close()


class _RotationOutcomeHarness:
    """Return a durable summary, then make Pi's rotation outcome explicit."""

    def __init__(self, tmp_path, rotation_error):
        self._tmp_path = tmp_path
        self._rotation_error = rotation_error
        self._bootstrapped: set[str] = set()

    def snapshot(self, engram_id):
        if engram_id not in self._bootstrapped:
            raise HarnessError(
                "pi_session_unknown",
                "no binding",
                "run a turn before snapshot",
                phase="snapshot",
            )
        return {"engram_id": engram_id, "state": "READY", "bootstrapped": True}

    def run_turn(
        self,
        engram_id,
        prompt,
        *,
        timeout_sec=None,
        bootstrap_text=None,
    ):
        del prompt, timeout_sec, bootstrap_text
        self._bootstrapped.add(engram_id)
        return HarnessTurnResult(
            engram_id=engram_id,
            session_id=f"rotation-test:{engram_id}",
            session_file=str(self._tmp_path / f"{engram_id}.jsonl"),
            content="durable succession summary",
            stop_reason="stop",
            input_tokens=7,
            output_tokens=3,
        )

    def succeed(self, old_engram_id, new_engram_id):
        del old_engram_id, new_engram_id
        raise self._rotation_error


@pytest.mark.parametrize(
    ("prompt_accepted", "expected_state", "candidate_status"),
    [
        (False, GenerationTransitionState.FAILED, "archived"),
        (True, GenerationTransitionState.UNCERTAIN, "provisional"),
        (None, GenerationTransitionState.UNCERTAIN, "provisional"),
    ],
)
def test_durable_rotation_succeed_outcome_is_not_guessed(
    tmp_path,
    prompt_accepted,
    expected_state,
    candidate_status,
):
    from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
    from pulse_system.substrate.llm import LLMAdapter

    store = Storage(":memory:")
    engram = store.create_engram(engram_id=f"rotate-{prompt_accepted}")
    error = HarnessError(
        "pi_rotation_outcome",
        "Pi reported the requested rotation outcome",
        "reconcile the generation before retrying",
        phase="succession",
        retryable=False,
        prompt_accepted=prompt_accepted,
    )
    ledger = CausalLedger(store)
    manager = EngramManager(
        store,
        LLMAdapter(mock=True),
        ConnectionNetwork(store, ConnectionConfig()),
        harness=_RotationOutcomeHarness(tmp_path, error),
        causal_ledger=ledger,
        causal_world_id="world-recovery-rotation",
    )

    try:
        with pytest.raises(HarnessError):
            manager.succession(engram.id)

        [generation] = ledger.list_generations(predecessor_id=engram.id)
        assert generation.state is expected_state
        assert generation.successor_id is not None
        assert ledger.get_event(generation.event_id).status is {
            GenerationTransitionState.FAILED: CausalEventStatus.FAILED,
            GenerationTransitionState.UNCERTAIN: CausalEventStatus.UNCERTAIN,
        }[expected_state]
        candidate = store.get_engram(generation.successor_id)
        assert candidate is not None
        assert candidate.status.value == candidate_status
        assert store.get_engram(engram.id).status.value == "active"
    finally:
        store.close()


class _BlockingRuntimeHarness:
    """Hold one real Runtime turn open across stop(timeout)."""

    def __init__(self, workspace, **_kwargs):
        self.workspace = workspace
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = 0
        self.closed = False

    def preflight(self):
        return None

    def snapshot(self, engram_id):
        raise HarnessError(
            "pi_session_unknown",
            f"no session for {engram_id}",
            "run the first turn",
            phase="snapshot",
        )

    def run_turn(
        self,
        engram_id,
        prompt,
        *,
        timeout_sec=None,
        bootstrap_text=None,
    ):
        del prompt, timeout_sec, bootstrap_text
        self.calls += 1
        self.entered.set()
        if not self.release.wait(5.0):
            raise HarnessError(
                "blocking_test_timeout",
                "the test Harness was not released",
                "release the test Harness before closing",
                phase="turn",
                retryable=False,
                prompt_accepted=None,
            )
        return HarnessTurnResult(
            engram_id=engram_id,
            session_id=f"blocking:{engram_id}",
            session_file=str(self.workspace / f"{engram_id}.jsonl"),
            content="late result must not settle after recovery",
            stop_reason="stop",
            input_tokens=4,
            output_tokens=2,
        )

    def succeed(self, old_engram_id, new_engram_id):
        del old_engram_id, new_engram_id

    def close_session(self, engram_id):
        del engram_id

    def abort(self, engram_id):
        raise HarnessError(
            "blocking_test_not_async",
            f"no asynchronous turn for {engram_id}",
            "wait for the blocking turn",
            phase="abort",
        )

    def steer(self, engram_id, content):
        del content
        raise HarnessError(
            "blocking_test_not_async",
            f"no asynchronous turn for {engram_id}",
            "wait for the blocking turn",
            phase="steer",
        )

    def binding_snapshot(self):
        return {"version": 1, "sessions": {}}

    def close(self):
        self.closed = True
        return {
            "active_before": 0,
            "sessions_observed": 0,
            "continuity_writers_sealed": 0,
            "unresolved": 0,
            "owner_joined": True,
            "cancel_signalled": False,
            "process_tree_state": "not_applicable",
        }


def test_runtime_quiesce_rejects_concurrent_inject_and_closes_after_timeout(
    tmp_path,
    production_harness_test_provider_credential,
):
    harnesses = []

    def factory(workspace, **kwargs):
        del kwargs
        harness = _BlockingRuntimeHarness(workspace)
        harnesses.append(harness)
        return harness

    service = RuntimeService(
        RuntimeServiceConfig(
            db_path=tmp_path / "quiesce.db",
            metrics_path=tmp_path / "quiesce.metrics.jsonl",
            workspace=tmp_path,
            mock=False,
            tick_interval=0.01,
            default_max_wait=0.0,
            silence_threshold=0.0,
            base_spontaneous_rate=0.0,
        ),
        harness_factory=factory,
    )
    front_id = service.front_engram_id
    first_event_id = service.inject(front_id, "turn held during quiesce")
    tick_blocked = threading.Event()
    tick_release = threading.Event()
    original_tick_once = service.tick_once

    def blocking_tick_once():
        result = original_tick_once()
        assert harnesses[0].entered.wait(2.0), "the Runtime never entered Pi"
        tick_blocked.set()
        if not tick_release.wait(5.0):
            raise RuntimeError("the test tick boundary was not released")
        return result

    service.tick_once = blocking_tick_once

    async def stop_while_turn_is_running():
        await service.start()
        deadline = time.monotonic() + 5.0
        while not tick_blocked.is_set():
            assert time.monotonic() < deadline, "the tick boundary never blocked"
            await asyncio.sleep(0.005)

        stop_awaitable = service.stop(timeout=0.01)
        assert service._quiescing, "stop() must synchronously fence new work"
        stop_task = asyncio.create_task(stop_awaitable)

        with pytest.raises(ServiceError) as rejected:
            service.inject(front_id, "arrived at the close boundary")
        assert rejected.value.status == 409
        assert rejected.value.error == "runtime_quiescing"

        # Recovery is a later logical epoch and must not overtake an owner
        # admitted before revocation.  Observe the timeout/revocation first,
        # then let that owner drain; its late success cannot settle afterward.
        while service._recovery_permit is None:
            assert time.monotonic() < deadline, "stop(timeout) did not revoke"
            await asyncio.sleep(0.005)
        harnesses[0].release.set()
        tick_release.set()
        await stop_task
        [turn] = service.causal_ledger.list_turns(engram_id=front_id)
        assert turn.state.value == "uncertain"

    try:
        asyncio.run(stop_while_turn_is_running())
        assert service.causal_ledger.get_event(first_event_id).status is CausalEventStatus.UNCERTAIN
        assert harnesses[0].calls == 1

        # The pre-revoke worker has left the tick lock. close() must join the
        # resource boundary, and its late result remains unreplayable.
        _close_processless_harness_fixture(service)
        assert harnesses[0].closed is True

        # Runtime correctly refuses to upgrade an opaque Harness summary.
        # This deterministic fixture has no hidden owner, so the test helper
        # explicitly releases its lease after observing the real worker exit.

        reborn = RuntimeService(
            RuntimeServiceConfig(
                db_path=tmp_path / "quiesce.db",
                metrics_path=tmp_path / "quiesce-reborn.metrics.jsonl",
                workspace=tmp_path,
                mock=False,
                tick_interval=0.01,
                default_max_wait=0.0,
                silence_threshold=0.0,
                base_spontaneous_rate=0.0,
            ),
            harness_factory=factory,
        )
        try:
            assert reborn.causal_ledger.get_event(first_event_id).status is CausalEventStatus.UNCERTAIN
            assert reborn.causal_ledger.get_children(first_event_id) == []
            assert len(harnesses) == 2
            assert harnesses[1].calls == 0
        finally:
            _close_processless_harness_fixture(reborn)
    finally:
        harnesses[0].release.set()
        tick_release.set()
        _close_processless_harness_fixture(service)

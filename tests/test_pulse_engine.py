"""Tests for the Pulse engine.

Verifies: chain propagation (A→B→C), circular reverberation (A→B→A),
resource competition truncation, spontaneous activation distribution.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import pulse_system.core.pulse.engine as pulse_engine_module
from pulse_system.agent.harness.base import HarnessError
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.dendrite import DendriteConfig, DendriteProcessor
from pulse_system.core.engram import EngramManager
from pulse_system.core.pulse import PulseEngine, PulseEngineConfig, PulseReason
from pulse_system.core.runtime import RuntimeConfig, RuntimeManager
from pulse_system.core.types import Message, MessageRole
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


@pytest.fixture
def store():
    s = Storage(":memory:")
    yield s
    s.close()


@pytest.fixture
def mock_llm():
    return LLMAdapter(mock=True)


@pytest.fixture
def conn_net(store):
    return ConnectionNetwork(store, ConnectionConfig(
        stdp_strength=0.1,
        decay_rate=0.003,
        prune_threshold=0.01,
    ))


@pytest.fixture
def mgr(store, mock_llm, conn_net):
    return EngramManager(store, mock_llm, conn_net)


@pytest.fixture
def dendrite(mgr):
    return DendriteProcessor(mgr, DendriteConfig(
        silence_threshold=0.0,   # instant dispatch for tests
        default_max_wait=0.0,    # instant dispatch for tests
    ))


@pytest.fixture
def runtime():
    return RuntimeManager(RuntimeConfig(
        budget_per_tick=10,
        hourly_token_budget=1_000_000,
        daily_token_budget=10_000_000,
    ))


@pytest.fixture
def engine(store, mgr, conn_net, dendrite, runtime):
    return PulseEngine(
        storage=store,
        engram_manager=mgr,
        connection_network=conn_net,
        dendrite=dendrite,
        runtime=runtime,
        config=PulseEngineConfig(
            propagation_threshold=0.3,
            budget_per_tick=10,
            spontaneous_check_interval=1000.0,  # disable spontaneous in most tests
            tick_interval=0.01,
            decay_interval=1000.0,  # disable decay in most tests
            base_spontaneous_rate=0.02,
        ),
    )


def _make_engram(mgr, content="hello"):
    return mgr.create(initial_messages=[
        Message(role=MessageRole.USER, content=content),
    ])


# ── External event injection ─────────────────────────────────────


class TestExternalEvent:
    def test_inject_and_tick(self, engine: PulseEngine, mgr: EngramManager):
        e = _make_engram(mgr)
        engine.inject_external_event(e.id, "external stimulus")
        results = engine.tick()
        # Engram should have pulsed
        assert len(results) == 1
        assert results[0][0] == e.id
        assert len(results[0][1]) > 0

    def test_external_appends_to_session(self, engine: PulseEngine, mgr: EngramManager):
        e = _make_engram(mgr)
        engine.inject_external_event(e.id, "new info")
        engine.tick()
        session = mgr.get_session(e.id)
        # initial msg + injection + LLM response
        assert len(session) >= 3


# ── Chain propagation (A→B→C) ────────────────────────────────────


class TestChainPropagation:
    def test_a_to_b_to_c(self, engine: PulseEngine, mgr: EngramManager, store: Storage):
        a = _make_engram(mgr, "I am engram A")
        b = _make_engram(mgr, "I am engram B")
        c = _make_engram(mgr, "I am engram C")

        # Create chain: A → B → C
        store.create_connection(a.id, b.id, 0.5)
        store.create_connection(b.id, c.id, 0.5)

        # Tick 1: inject into A, A pulses, output propagates to B's dendrite
        engine.inject_external_event(a.id, "start the chain")
        results1 = engine.tick()
        assert any(r[0] == a.id for r in results1)

        # Tick 2: B's dendrite dispatches, B pulses, output propagates to C's dendrite
        results2 = engine.tick()
        assert any(r[0] == b.id for r in results2)

        # Tick 3: C's dendrite dispatches, C pulses
        results3 = engine.tick()
        assert any(r[0] == c.id for r in results3)

    def test_weak_connection_no_propagation(self, engine: PulseEngine, mgr: EngramManager, store: Storage):
        a = _make_engram(mgr, "I am A")
        b = _make_engram(mgr, "I am B")

        # Connection below propagation threshold (0.3)
        store.create_connection(a.id, b.id, 0.1)

        engine.inject_external_event(a.id, "start")
        engine.tick()

        # B should NOT have received anything
        results2 = engine.tick()
        assert not any(r[0] == b.id for r in results2)

    def test_hard_depth_fence_stops_second_hop_even_with_maximal_edges(
        self,
        engine: PulseEngine,
        mgr: EngramManager,
        store: Storage,
    ) -> None:
        a = _make_engram(mgr, "I am source A")
        b = _make_engram(mgr, "I am relay B")
        c = _make_engram(mgr, "I am forbidden second-hop C")
        store.create_connection(a.id, b.id, 1.0)
        store.create_connection(b.id, c.id, 1.0)

        engine._config.propagation_threshold = 0.0
        engine._config.max_content_propagation_depth = 1
        engine._config.propagation_content_prefix = "[[RELAY_ONLY]]\n"
        # Make modulation maximally permissive as an adversarial condition;
        # the depth fence, not a soft threshold, must stop B -> C.
        engine._propagation_mods[a.id] = 0.0
        engine._propagation_mods[b.id] = 0.0

        engine.inject_external_event(a.id, "start the bounded relay")
        assert any(engram_id == a.id for engram_id, _ in engine.tick())
        assert any(engram_id == b.id for engram_id, _ in engine.tick())
        assert any(
            message.content.startswith("[[RELAY_ONLY]]\n")
            for message in mgr.get_session(b.id)
            if message.role is MessageRole.INJECTION
        )

        for _ in range(3):
            assert not any(engram_id == c.id for engram_id, _ in engine.tick())


# ── Circular reverberation (A→B→A) ──────────────────────────────


class TestCircularReverberation:
    def test_a_b_loop(self, engine: PulseEngine, mgr: EngramManager, store: Storage):
        a = _make_engram(mgr, "I am A in a loop")
        b = _make_engram(mgr, "I am B in a loop")

        store.create_connection(a.id, b.id, 0.5)
        store.create_connection(b.id, a.id, 0.5)

        # Kick off: inject into A
        engine.inject_external_event(a.id, "loop start")

        # Track which engrams pulse over multiple ticks
        pulsed = []
        for _ in range(6):
            results = engine.tick()
            for eid, _ in results:
                pulsed.append(eid)

        # Both A and B should have pulsed multiple times
        a_count = pulsed.count(a.id)
        b_count = pulsed.count(b.id)
        assert a_count >= 2, f"A pulsed {a_count} times, expected >= 2"
        assert b_count >= 1, f"B pulsed {b_count} times, expected >= 1"

    def test_three_node_cycle(self, engine: PulseEngine, mgr: EngramManager, store: Storage):
        a = _make_engram(mgr, "cycle node A")
        b = _make_engram(mgr, "cycle node B")
        c = _make_engram(mgr, "cycle node C")

        store.create_connection(a.id, b.id, 0.5)
        store.create_connection(b.id, c.id, 0.5)
        store.create_connection(c.id, a.id, 0.5)

        engine.inject_external_event(a.id, "cycle start")

        pulsed_ids = set()
        for _ in range(6):
            results = engine.tick()
            for eid, _ in results:
                pulsed_ids.add(eid)

        assert a.id in pulsed_ids
        assert b.id in pulsed_ids
        assert c.id in pulsed_ids


# ── Resource competition (Rule 7) ────────────────────────────────


class TestResourceCompetition:
    def test_budget_truncation(self, store, mgr, conn_net, dendrite, runtime):
        """When more events than budget, only top-priority fire."""
        engine = PulseEngine(
            storage=store,
            engram_manager=mgr,
            connection_network=conn_net,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                budget_per_tick=2,  # only 2 per tick
                propagation_threshold=0.3,
                spontaneous_check_interval=1000.0,
                decay_interval=1000.0,
            ),
        )

        engrams = [_make_engram(mgr, f"engram {i}") for i in range(5)]
        for e in engrams:
            engine.inject_external_event(e.id, "stimulus", priority=0.5)

        results = engine.tick()
        assert len(results) <= 2

    def test_priority_ordering(self, store, mgr, conn_net, dendrite, runtime):
        engine = PulseEngine(
            storage=store,
            engram_manager=mgr,
            connection_network=conn_net,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                budget_per_tick=1,
                propagation_threshold=0.3,
                spontaneous_check_interval=1000.0,
                decay_interval=1000.0,
            ),
        )

        low = _make_engram(mgr, "low priority")
        high = _make_engram(mgr, "high priority")

        engine.inject_external_event(low.id, "low", priority=0.1)
        engine.inject_external_event(high.id, "high", priority=0.9)

        results = engine.tick()
        assert len(results) == 1
        assert results[0][0] == high.id

    def test_remaining_events_carry_over(self, store, mgr, conn_net, dendrite, runtime):
        engine = PulseEngine(
            storage=store,
            engram_manager=mgr,
            connection_network=conn_net,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                budget_per_tick=1,
                propagation_threshold=0.3,
                spontaneous_check_interval=1000.0,
                decay_interval=1000.0,
            ),
        )

        e1 = _make_engram(mgr, "first")
        e2 = _make_engram(mgr, "second")

        engine.inject_external_event(e1.id, "go", priority=0.9)
        engine.inject_external_event(e2.id, "go", priority=0.5)

        # Tick 1: only e1 fires (budget=1)
        r1 = engine.tick()
        assert len(r1) == 1
        assert r1[0][0] == e1.id

        # Tick 2: e2 should fire from carryover
        r2 = engine.tick()
        assert any(rid == e2.id for rid, _ in r2)


# ── Spontaneous activation (Rule 6) ─────────────────────────────


class TestSpontaneousActivation:
    def test_probability_proportional_to_state(self, engine: PulseEngine, store: Storage, mgr: EngramManager):
        """Engrams with higher activity/connections should have higher spontaneous probability."""
        isolated = _make_engram(mgr, "isolated engram")
        connected = _make_engram(mgr, "connected engram")
        other = _make_engram(mgr, "other")

        # Give 'connected' more connections and activity
        store.create_connection(connected.id, other.id, 0.5)
        store.create_connection(other.id, connected.id, 0.5)
        store.update_engram_metadata(connected.id, recent_activity=0.8)
        store.update_engram_metadata(isolated.id, recent_activity=0.0)

        from pulse_system.core.types import EngramStatus
        engram_connected = store.get_engram(connected.id)
        engram_isolated = store.get_engram(isolated.id)

        p_connected = engine._compute_spontaneous_probability(connected.id, engram_connected)
        p_isolated = engine._compute_spontaneous_probability(isolated.id, engram_isolated)

        assert p_connected > p_isolated

    def test_spontaneous_events_generated(
        self,
        store,
        mgr,
        conn_net,
        dendrite,
        runtime,
        monkeypatch,
    ):
        """With high spontaneous rate, events should be generated."""
        engine = PulseEngine(
            storage=store,
            engram_manager=mgr,
            connection_network=conn_net,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                propagation_threshold=0.3,
                budget_per_tick=100,
                spontaneous_check_interval=0.0,  # check every tick
                base_spontaneous_rate=1.0,
                decay_interval=1000.0,
            ),
        )

        # Create engrams with high excitability
        for i in range(5):
            e = _make_engram(mgr, f"spontaneous {i}")
            store.update_engram_metadata(e.id, self_excitability=1.0, recent_activity=1.0)

        # A disconnected Engram still receives the connection-factor floor, so
        # base_spontaneous_rate=1.0 produces p=0.5 rather than a guaranteed draw.
        # Pin only the random draw; the probability calculation remains real.
        monkeypatch.setattr(pulse_engine_module.random, "random", lambda: 0.0)
        results = engine.tick()
        assert len(results) > 0


# ── STDP learning during tick ────────────────────────────────────


class TestSTDPDuringTick:
    def test_co_active_creates_connections(self, engine: PulseEngine, mgr: EngramManager, store: Storage):
        a = _make_engram(mgr, "engram A")
        b = _make_engram(mgr, "engram B")

        # Both get external events → both pulse in same tick → STDP fires
        engine.inject_external_event(a.id, "stimulus A")
        engine.inject_external_event(b.id, "stimulus B")
        engine.tick()

        # STDP should have created or attempted connections
        conn_ab = store.get_connection(a.id, b.id)
        conn_ba = store.get_connection(b.id, a.id)
        # At least one direction should exist
        assert conn_ab is not None or conn_ba is not None


# ── Propagation ──────────────────────────────────────────────────


class TestPropagation:
    def test_propagate_sends_to_dendrite(self, engine: PulseEngine, mgr: EngramManager, store: Storage, dendrite: DendriteProcessor):
        a = _make_engram(mgr, "source")
        b = _make_engram(mgr, "target")
        store.create_connection(a.id, b.id, 0.5)

        engine.propagate(a.id, "pulse output from A")
        assert dendrite.has_pending(b.id)
        assert dendrite.get_queue_size(b.id) == 1

    def test_propagate_skips_weak_connections(self, engine: PulseEngine, mgr: EngramManager, store: Storage, dendrite: DendriteProcessor):
        a = _make_engram(mgr, "source")
        b = _make_engram(mgr, "weak target")
        store.create_connection(a.id, b.id, 0.1)

        engine.propagate(a.id, "output")
        assert not dendrite.has_pending(b.id)


# ── Async run ────────────────────────────────────────────────────


class TestAsyncRun:
    @pytest.mark.asyncio
    async def test_run_stops_on_event(self, engine: PulseEngine):
        stop = asyncio.Event()

        async def stop_after():
            await asyncio.sleep(0.05)
            stop.set()

        await asyncio.gather(engine.run(stop), stop_after())
        assert engine.tick_count > 0

    @pytest.mark.asyncio
    async def test_run_processes_events(self, engine: PulseEngine, mgr: EngramManager):
        e = _make_engram(mgr)
        engine.inject_external_event(e.id, "async test")
        stop = asyncio.Event()

        async def stop_after():
            await asyncio.sleep(0.05)
            stop.set()

        await asyncio.gather(engine.run(stop), stop_after())
        session = mgr.get_session(e.id)
        assert len(session) >= 2  # initial + at least injection


# ── Automatic succession ─────────────────────────────────────────


class TestAutoSuccession:
    def _engine_with_threshold(self, store, mgr, conn_net, dendrite, runtime, threshold):
        return PulseEngine(
            storage=store,
            engram_manager=mgr,
            connection_network=conn_net,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                propagation_threshold=0.3,
                budget_per_tick=10,
                spontaneous_check_interval=1000.0,
                decay_interval=1000.0,
                succession_token_threshold=threshold,
            ),
        )

    def test_succession_triggered_over_threshold(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        from pulse_system.core.types import EngramStatus

        engine = self._engine_with_threshold(store, mgr, conn_net, dendrite, runtime, 10)
        e = _make_engram(mgr, "long-lived engram " * 20)
        engine.inject_external_event(e.id, "stimulus")
        engine.tick()

        old = store.get_engram(e.id)
        assert old.status == EngramStatus.ARCHIVED
        active = store.list_engrams(status=EngramStatus.ACTIVE)
        assert len(active) == 1
        successor = active[0]
        # successor seeded with the summary
        session = mgr.get_session(successor.id)
        assert len(session) == 1

    def test_succession_transfers_connections_and_queue(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        from pulse_system.core.types import EngramStatus

        engine = self._engine_with_threshold(store, mgr, conn_net, dendrite, runtime, 10)
        e = _make_engram(mgr, "source " * 20)
        other = _make_engram(mgr, "downstream")
        store.create_connection(other.id, e.id, 0.5)

        # queue a pending input for the old engram, then trigger succession
        engine.inject_external_event(e.id, "stimulus")
        engine.tick()

        active = [
            x for x in store.list_engrams(status=EngramStatus.ACTIVE)
            if x.id != other.id
        ]
        assert len(active) == 1
        successor = active[0]
        # incoming connection re-pointed at the successor
        assert store.get_connection(other.id, e.id) is None
        assert store.get_connection(other.id, successor.id) is not None

    def test_no_succession_below_threshold(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        from pulse_system.core.types import EngramStatus

        engine = self._engine_with_threshold(
            store, mgr, conn_net, dendrite, runtime, 1_000_000
        )
        e = _make_engram(mgr)
        engine.inject_external_event(e.id, "stimulus")
        engine.tick()
        assert store.get_engram(e.id).status == EngramStatus.ACTIVE

    def test_none_disables_succession(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        from pulse_system.core.types import EngramStatus

        engine = self._engine_with_threshold(store, mgr, conn_net, dendrite, runtime, None)
        e = _make_engram(mgr, "x" * 2000)
        engine.inject_external_event(e.id, "stimulus")
        engine.tick()
        assert store.get_engram(e.id).status == EngramStatus.ACTIVE

    def test_succession_cost_recorded(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        engine = self._engine_with_threshold(store, mgr, conn_net, dendrite, runtime, 10)
        e = _make_engram(mgr, "budget tracked " * 10)
        engine.inject_external_event(e.id, "stimulus")
        engine.tick()
        stats = runtime.get_stats()
        # pulse + succession summary call both recorded
        assert stats.total_pulses >= 2


# ── Sticky external priority ──────────────────────────────────────


class TestStickyExternalPriority:
    def test_priority_survives_dendrite_window(self, store, mgr, conn_net, runtime):
        """External priority must not degrade to the 0.8 default when the
        dendrite window spans multiple ticks."""
        from datetime import datetime, timedelta, timezone

        dendrite = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=100.0,   # window never closes on its own
            default_max_wait=1000.0,
        ))
        engine = PulseEngine(
            storage=store,
            engram_manager=mgr,
            connection_network=conn_net,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                propagation_threshold=0.3,
                budget_per_tick=1,          # forces priority competition
                spontaneous_check_interval=1000.0,
                decay_interval=1000.0,
            ),
        )

        a = _make_engram(mgr, "external target")
        b = _make_engram(mgr, "propagation target")

        # tick 1: external event routed into a's dendrite queue, not ready yet
        engine.inject_external_event(a.id, "user message", priority=1.0)
        assert engine.tick() == []

        # a propagation-style input lands in b's queue
        dendrite.receive(b.id, "propagated content", "some_source", 0.5)

        # force both windows to be ready
        past = datetime.now(timezone.utc) - timedelta(seconds=500)
        for eid in (a.id, b.id):
            q = dendrite._queues[eid]
            q.last_input_at = past
            q.window_opened_at = past

        # tick 2: both dispatch, but budget=1 — external (1.0) must win
        r2 = engine.tick()
        assert len(r2) == 1
        assert r2[0][0] == a.id

        # tick 3: the propagation event fires from carryover
        r3 = engine.tick()
        assert any(eid == b.id for eid, _ in r3)

    def test_external_marker_consumed_once(self, store, mgr, conn_net, runtime):
        from datetime import datetime, timedelta, timezone

        dendrite = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=100.0,
            default_max_wait=1000.0,
        ))
        engine = PulseEngine(
            storage=store,
            engram_manager=mgr,
            connection_network=conn_net,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                propagation_threshold=0.3,
                budget_per_tick=10,
                spontaneous_check_interval=1000.0,
                decay_interval=1000.0,
            ),
        )
        a = _make_engram(mgr, "target")
        engine.inject_external_event(a.id, "hello", priority=1.0)
        engine.tick()

        past = datetime.now(timezone.utc) - timedelta(seconds=500)
        q = dendrite._queues[a.id]
        q.last_input_at = past
        q.window_opened_at = past
        engine.tick()

        # sticky state fully consumed after dispatch
        assert a.id not in engine._sticky_priority
        assert a.id not in engine._external_marked


# ── Event loop responsiveness ─────────────────────────────────────


class TestEventLoopResponsiveness:
    @pytest.mark.asyncio
    async def test_slow_llm_does_not_block_loop(self, store, mgr, conn_net, dendrite, runtime):
        """tick() runs in a worker thread, so a blocking LLM call must not
        freeze the event loop."""
        import time

        engine = PulseEngine(
            storage=store,
            engram_manager=mgr,
            connection_network=conn_net,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                propagation_threshold=0.3,
                budget_per_tick=10,
                spontaneous_check_interval=1000.0,
                tick_interval=0.01,
                decay_interval=1000.0,
            ),
        )
        e = _make_engram(mgr, "slow engram")
        engine.inject_external_event(e.id, "trigger slow call")

        llm = mgr.llm
        orig = llm.complete

        def slow_complete(messages, **kwargs):
            time.sleep(0.25)  # blocking, like a real API call
            return orig(messages, **kwargs)

        llm.complete = slow_complete

        stop = asyncio.Event()
        side_iterations = 0

        async def side_work():
            nonlocal side_iterations
            while not stop.is_set():
                await asyncio.sleep(0.01)
                side_iterations += 1

        async def stopper():
            await asyncio.sleep(0.35)
            stop.set()

        await asyncio.gather(engine.run(stop), side_work(), stopper())

        # With a blocked loop the side coroutine would advance only in the
        # ~0.1s outside the LLM call; in a worker-thread model it keeps
        # running throughout the 0.35s window.
        assert side_iterations >= 10, f"event loop starved: {side_iterations}"
        assert engine.tick_count >= 1

    @pytest.mark.asyncio
    async def test_storage_usable_from_loop_while_engine_runs(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        """Storage is shared between the worker thread (tick) and the event
        loop thread (front agent / clone) — must not raise."""
        engine = PulseEngine(
            storage=store,
            engram_manager=mgr,
            connection_network=conn_net,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                propagation_threshold=0.3,
                budget_per_tick=10,
                spontaneous_check_interval=1000.0,
                tick_interval=0.001,
                decay_interval=1000.0,
            ),
        )
        e = _make_engram(mgr, "shared storage")
        engine.inject_external_event(e.id, "go")

        stop = asyncio.Event()

        async def reader():
            for _ in range(20):
                store.list_engrams()  # event-loop thread access
                await asyncio.sleep(0.005)
            stop.set()

        await asyncio.gather(engine.run(stop), reader())
        assert engine.tick_count >= 1


# ── LLM failure handling (v0.3 / 3.2) ────────────────────────────


class TestLLMFailureHandling:
    def _failing_engine(self, store, mgr, conn_net, dendrite, runtime, **cfg):
        return PulseEngine(
            storage=store,
            engram_manager=mgr,
            connection_network=conn_net,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                propagation_threshold=0.3,
                budget_per_tick=10,
                spontaneous_check_interval=1000.0,
                decay_interval=1000.0,
                **cfg,
            ),
        )

    def test_failed_event_requeued_with_lower_priority(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        from pulse_system.substrate.llm import LLMCallError

        engine = self._failing_engine(
            store, mgr, conn_net, dendrite, runtime,
            failure_backoff_threshold=100,  # don't hit backoff here
        )
        e = _make_engram(mgr)
        engine.inject_external_event(e.id, "will fail", priority=1.0)

        llm = mgr.llm
        orig = llm.complete
        calls = {"n": 0}

        def flaky(messages, **kw):
            calls["n"] += 1
            if calls["n"] == 1:
                raise LLMCallError("simulated 429")
            return orig(messages, **kw)

        llm.complete = flaky

        r1 = engine.tick()   # fails, requeued
        assert r1 == []
        assert len(engine._pending_events) == 1
        assert engine._pending_events[0].attempts == 1
        assert engine._pending_events[0].priority < 1.0

        r2 = engine.tick()   # retry succeeds
        assert len(r2) == 1
        assert engine.failure_domain_snapshot()["total"] == 0

    def test_event_dropped_after_max_retries(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        from pulse_system.substrate.llm import LLMCallError

        engine = self._failing_engine(
            store, mgr, conn_net, dendrite, runtime,
            max_pulse_retries=2,
            failure_backoff_threshold=100,
        )
        e = _make_engram(mgr)
        engine.inject_external_event(e.id, "always fails")

        def always_fail(messages, **kw):
            raise LLMCallError("permanent failure")

        mgr.llm.complete = always_fail

        for _ in range(5):
            engine.tick()
        # initial attempt + 2 retries consumed, then dropped
        assert engine._pending_events == []

    def test_backoff_window_is_scoped_to_failed_engram(
        self, store, mgr, conn_net, dendrite, runtime, monkeypatch
    ):
        from pulse_system.substrate.llm import LLMCallError

        clock = {"now": datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)}
        monkeypatch.setattr(
            pulse_engine_module,
            "_now",
            lambda: clock["now"],
        )
        engine = self._failing_engine(
            store, mgr, conn_net, dendrite, runtime,
            max_pulse_retries=10,
            failure_backoff_threshold=2,
            failure_backoff_seconds=60.0,
        )
        e1 = _make_engram(mgr, "one")
        e2 = _make_engram(mgr, "two")
        engine.inject_external_event(e1.id, "x")
        engine.inject_external_event(e2.id, "y")

        original_pulse = mgr.pulse
        calls = {e1.id: 0, e2.id: 0}
        fail_first = {"active": True}

        def one_bad_session(engram_id, *args, **kwargs):
            calls[engram_id] = calls.get(engram_id, 0) + 1
            if engram_id == e1.id and fail_first["active"]:
                raise LLMCallError("one local session is unavailable")
            return original_pulse(engram_id, *args, **kwargs)

        mgr.pulse = one_bad_session

        first = engine.tick()  # A fails once; B succeeds in the same batch.
        assert [engram_id for engram_id, _content in first] == [e2.id]
        assert engine.failure_domain_snapshot()["items"][0]["state"] == "degraded"

        engine.tick()  # A fails again and enters its own cooldown.
        failure = engine.failure_domain_snapshot()
        assert failure["total"] == 1
        assert failure["cooling"] == 1
        assert failure["items"][0]["engram_id"] == e1.id
        assert failure["items"][0]["consecutive_failures"] == 2

        engine.inject_external_event(e2.id, "still alive")
        third = engine.tick()
        assert [engram_id for engram_id, _content in third] == [e2.id]
        assert calls[e1.id] == 2
        assert calls[e2.id] == 2
        assert [event.engram_id for event in engine._pending_events] == [e1.id]
        after_peer_success = engine.failure_domain_snapshot()
        assert after_peer_success["cooling"] == 1
        assert after_peer_success["items"][0]["engram_id"] == e1.id

        # Expiry itself manufactures no event. The existing retry becomes a
        # normal probe; only A's own success clears A's failure domain.
        clock["now"] += timedelta(seconds=61)
        fail_first["active"] = False
        recovered = engine.tick()
        assert [engram_id for engram_id, _content in recovered] == [e1.id]
        assert engine.failure_domain_snapshot()["total"] == 0

    def test_failure_domain_snapshot_is_bounded_and_stably_sorted(
        self, store, mgr, conn_net, dendrite, runtime, monkeypatch
    ):
        from pulse_system.substrate.llm import LLMCallError

        fixed = datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(pulse_engine_module, "_now", lambda: fixed)
        engine = self._failing_engine(
            store,
            mgr,
            conn_net,
            dendrite,
            runtime,
            failure_backoff_threshold=100,
        )
        for index in range(7):
            engine._record_failure_domain(
                f"engram-{index}",
                LLMCallError("private provider detail must not be projected"),
            )

        snapshot = engine.failure_domain_snapshot(limit=5)
        assert snapshot["policy_version"] == "engram-failure-domain.v1"
        assert snapshot["evidence_class"] == "runtime_memory_projection"
        assert snapshot["total"] == 7
        assert snapshot["degraded"] == 7
        assert snapshot["truncated"] is True
        assert [item["engram_id"] for item in snapshot["items"]] == [
            "engram-0",
            "engram-1",
            "engram-2",
            "engram-3",
            "engram-4",
        ]
        assert "private provider detail" not in str(snapshot)
        with pytest.raises(ValueError, match="from 1 to 64"):
            engine.failure_domain_snapshot(limit=65)

    def test_failure_projection_rejects_natural_language_error_symbols(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        engine = self._failing_engine(
            store,
            mgr,
            conn_net,
            dendrite,
            runtime,
            failure_backoff_threshold=100,
        )
        engram = _make_engram(mgr)
        engine._record_failure_domain(
            engram.id,
            HarnessError(
                "供应商错误 private prose",
                "private detail",
                "private remedy",
                phase="模型 阶段",
                retryable=True,
                prompt_accepted=False,
            ),
        )

        item = engine.failure_domain_snapshot()["items"][0]
        assert item["last_error_code"] == "harness_error"
        assert item["last_error_phase"] == "unknown"
        assert "供应商" not in str(item)
        assert "private" not in str(item)

    def test_probe_ready_does_not_self_stimulate_and_failure_recools(
        self, store, mgr, conn_net, dendrite, runtime, monkeypatch
    ):
        from pulse_system.substrate.llm import LLMCallError

        clock = {"now": datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)}
        monkeypatch.setattr(pulse_engine_module, "_now", lambda: clock["now"])
        engine = self._failing_engine(
            store,
            mgr,
            conn_net,
            dendrite,
            runtime,
            failure_backoff_threshold=1,
            failure_backoff_seconds=30.0,
        )
        engram = _make_engram(mgr)
        engine._record_failure_domain(engram.id, LLMCallError("private"))
        assert engine.failure_domain_snapshot()["cooling"] == 1

        clock["now"] += timedelta(seconds=31)
        pending_before = list(engine._pending_events)
        assert engine.tick() == []
        assert engine._pending_events == pending_before
        assert engine.failure_domain_snapshot()["probe_ready"] == 1

        engine._record_failure_domain(engram.id, LLMCallError("private again"))
        snapshot = engine.failure_domain_snapshot()
        assert snapshot["cooling"] == 1
        assert snapshot["items"][0]["consecutive_failures"] == 2

        mgr.archive(engram.id)
        assert engine.tick() == []
        assert engine.failure_domain_snapshot()["total"] == 0

    def test_retryable_harness_error_is_requeued(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        engine = self._failing_engine(
            store,
            mgr,
            conn_net,
            dendrite,
            runtime,
            failure_backoff_threshold=100,
        )
        engram = _make_engram(mgr)
        engine.inject_external_event(engram.id, "retry this", priority=1.0)

        def retryable_failure(*args, **kwargs):
            raise HarnessError(
                "pi_prompt_refused",
                "provider rejected the prompt",
                "repair credentials",
                phase="prompt",
                retryable=True,
                prompt_accepted=False,
            )

        mgr.pulse = retryable_failure

        assert engine.tick() == []
        assert len(engine._pending_events) == 1
        retried = engine._pending_events[0]
        assert retried.engram_id == engram.id
        assert retried.attempts == 1
        assert retried.priority == pytest.approx(0.8)

    def test_unknown_harness_acceptance_is_never_requeued(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        engine = self._failing_engine(
            store,
            mgr,
            conn_net,
            dendrite,
            runtime,
            failure_backoff_threshold=100,
        )
        engram = _make_engram(mgr)
        engine.inject_external_event(engram.id, "possibly accepted")
        calls = {"count": 0}

        def uncertain_failure(*args, **kwargs):
            calls["count"] += 1
            raise HarnessError(
                "pi_prompt_ack_timeout",
                "acknowledgement was lost",
                "reconcile the Pi session",
                phase="prompt",
                retryable=False,
                prompt_accepted=None,
            )

        mgr.pulse = uncertain_failure

        assert engine.tick() == []
        assert engine._pending_events == []
        assert calls["count"] == 1
        assert engine.tick() == []
        assert calls["count"] == 1


# ── Runtime snapshot (v0.3 / 3.3) ────────────────────────────────


class TestRuntimeSnapshot:
    def test_snapshot_fields(self, runtime):
        runtime.consume_budget(100, 50, cached_input_tokens=40)
        snap = runtime.snapshot()
        assert snap["total_pulses"] == 1
        assert snap["total_input_tokens"] == 100
        assert snap["total_cached_input_tokens"] == 40
        assert snap["hourly_budget_remaining"] > 0
        assert snap["billable_tokens_this_hour"] == snap["billable_tokens_today"]


# ── Parallel pulses (v0.3 / 3.6) ─────────────────────────────────


class TestParallelPulses:
    def _parallel_engine(self, store, mgr, conn_net, dendrite, runtime, workers):
        return PulseEngine(
            storage=store,
            engram_manager=mgr,
            connection_network=conn_net,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                propagation_threshold=0.3,
                budget_per_tick=10,
                spontaneous_check_interval=1000.0,
                decay_interval=1000.0,
                max_parallel_pulses=workers,
            ),
        )

    def test_parallel_faster_than_serial(self, store, mgr, conn_net, dendrite, runtime):
        import time

        engine = self._parallel_engine(store, mgr, conn_net, dendrite, runtime, 4)
        engrams = [_make_engram(mgr, f"engram {i}") for i in range(4)]
        for e in engrams:
            engine.inject_external_event(e.id, "go")

        llm = mgr.llm
        orig = llm.complete

        def slow(messages, **kw):
            time.sleep(0.2)
            return orig(messages, **kw)

        llm.complete = slow

        t0 = time.monotonic()
        results = engine.tick()
        elapsed = time.monotonic() - t0

        assert len(results) == 4
        # serial would be >= 0.8s; parallel with 4 workers ≈ 0.2s
        assert elapsed < 0.6, f"parallel tick took {elapsed:.2f}s"

    def test_parallel_results_and_accounting_complete(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        engine = self._parallel_engine(store, mgr, conn_net, dendrite, runtime, 4)
        engrams = [_make_engram(mgr, f"engram {i}") for i in range(4)]
        for e in engrams:
            engine.inject_external_event(e.id, "go")

        results = engine.tick()
        assert {eid for eid, _ in results} == {e.id for e in engrams}
        stats = runtime.get_stats()
        assert stats.total_pulses == 4
        llm_stats = mgr.llm.get_stats()
        assert llm_stats.total_calls == 4

    def test_parallel_failure_isolated(self, store, mgr, conn_net, dendrite, runtime):
        from pulse_system.substrate.llm import LLMCallError

        engine = self._parallel_engine(store, mgr, conn_net, dendrite, runtime, 4)
        good = [_make_engram(mgr, f"good {i}") for i in range(3)]
        bad = _make_engram(mgr, "FAIL_MARKER engram")
        for e in [*good, bad]:
            engine.inject_external_event(e.id, "go")

        llm = mgr.llm
        orig = llm.complete

        def selective(messages, **kw):
            if messages and "FAIL_MARKER" in messages[0].get("content", ""):
                raise LLMCallError("simulated outage for one engram")
            return orig(messages, **kw)

        llm.complete = selective

        results = engine.tick()
        assert len(results) == 3
        assert {eid for eid, _ in results} == {e.id for e in good}
        # failed event requeued for retry
        assert any(ev.engram_id == bad.id for ev in engine._pending_events)

    def test_background_dispatch_keeps_world_ticking_while_two_turns_block(
        self,
        store,
        mgr,
        conn_net,
        dendrite,
        runtime,
    ):
        import threading
        import time

        engine = PulseEngine(
            storage=store,
            engram_manager=mgr,
            connection_network=conn_net,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                budget_per_tick=10,
                spontaneous_check_interval=1000.0,
                decay_interval=1000.0,
                max_parallel_pulses=2,
                background_dispatch=True,
            ),
        )
        first = _make_engram(mgr, "BLOCK_A")
        second = _make_engram(mgr, "BLOCK_B")
        entered = {first.id: threading.Event(), second.id: threading.Event()}
        release = threading.Event()
        original = mgr.llm.complete

        def blocking_complete(messages, **kwargs):
            rendered = "\n".join(str(message.get("content", "")) for message in messages)
            engram_id = first.id if "BLOCK_A" in rendered else second.id
            entered[engram_id].set()
            assert release.wait(timeout=5.0)
            return original(messages, **kwargs)

        mgr.llm.complete = blocking_complete
        try:
            engine.inject_external_event(first.id, "start A")
            started = time.monotonic()
            assert engine.tick() == []
            assert time.monotonic() - started < 0.2
            assert entered[first.id].wait(timeout=1.0)

            engine.inject_external_event(second.id, "start B")
            tick_before = engine.tick_count
            started = time.monotonic()
            assert engine.tick() == []
            assert time.monotonic() - started < 0.2
            assert engine.tick_count == tick_before + 1
            assert entered[second.id].wait(timeout=1.0)
            assert engine.capacity_snapshot()["worker_running"] == 2

            release.set()
            outputs = []
            deadline = time.monotonic() + 2.0
            while len(outputs) < 2 and time.monotonic() < deadline:
                outputs.extend(engine.tick())
                time.sleep(0.01)
            assert {engram_id for engram_id, _content in outputs} == {
                first.id,
                second.id,
            }
            assert engine.capacity_snapshot()["worker_running"] == 0
        finally:
            release.set()
            engine.close()

    def test_background_succession_summary_isolated_from_other_subjects(
        self,
        tmp_path,
    ):
        import threading
        import time

        from pulse_system.agent.harness.base import HarnessTurnResult
        from pulse_system.core.types import EngramStatus

        summary_entered = threading.Event()
        release_summary = threading.Event()
        listener_threads: list[str] = []

        class GateHarness:
            def __init__(self):
                self.calls: list[tuple[str, str]] = []
                self.rotations: list[tuple[str, str]] = []

            def snapshot(self, engram_id):
                raise HarnessError(
                    "pi_session_unknown",
                    "not bootstrapped",
                    "run a turn",
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
                del timeout_sec, bootstrap_text
                self.calls.append((engram_id, prompt))
                is_summary = "comprehensive summary" in prompt
                if is_summary:
                    summary_entered.set()
                    assert release_summary.wait(timeout=5.0)
                is_first_subject = engram_id == first.id
                return HarnessTurnResult(
                    engram_id=engram_id,
                    session_id=f"session-{engram_id}",
                    session_file=str(tmp_path / f"{engram_id}.jsonl"),
                    content="lineage summary" if is_summary else "ordinary result",
                    stop_reason="stop",
                    input_tokens=12 if is_first_subject else 1,
                    output_tokens=3 if is_first_subject else 1,
                )

            def succeed(self, old_engram_id, new_engram_id):
                self.rotations.append((old_engram_id, new_engram_id))

        store = Storage(":memory:")
        llm = LLMAdapter(mock=True)
        connections = ConnectionNetwork(store, ConnectionConfig())
        harness = GateHarness()
        manager = EngramManager(store, llm, connections, harness=harness)
        dendrite = DendriteProcessor(
            manager,
            DendriteConfig(silence_threshold=0.0, default_max_wait=0.0),
        )
        runtime = RuntimeManager(
            RuntimeConfig(
                budget_per_tick=10,
                hourly_token_budget=1_000_000,
                daily_token_budget=10_000_000,
            )
        )
        engine = PulseEngine(
            storage=store,
            engram_manager=manager,
            connection_network=connections,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                budget_per_tick=10,
                spontaneous_check_interval=1000.0,
                decay_interval=1000.0,
                succession_token_threshold=10,
                max_parallel_pulses=2,
                max_parallel_successions=1,
                background_dispatch=True,
            ),
        )
        first = manager.create(
            initial_messages=[Message(role=MessageRole.USER, content="first")]
        )
        second = manager.create(
            initial_messages=[Message(role=MessageRole.USER, content="second")]
        )
        manager.add_succession_listener(
            lambda _old, _new: listener_threads.append(
                threading.current_thread().name
            )
        )

        try:
            engine.inject_external_event(first.id, "start first")
            assert engine.tick() == []
            deadline = time.monotonic() + 2.0
            while not summary_entered.is_set() and time.monotonic() < deadline:
                engine.tick()
                time.sleep(0.01)
            assert summary_entered.is_set()
            assert engine.capacity_snapshot()["succession_workers_running"] == 1

            engine.inject_external_event(second.id, "second remains alive")
            tick_before = engine.tick_count
            assert engine.tick() == []
            assert engine.tick_count == tick_before + 1
            outputs: list[tuple[str, str]] = []
            deadline = time.monotonic() + 2.0
            while not outputs and time.monotonic() < deadline:
                outputs.extend(engine.tick())
                time.sleep(0.01)
            assert (second.id, "ordinary result") in outputs
            assert store.get_engram(first.id).status is EngramStatus.ACTIVE
            assert listener_threads == []

            release_summary.set()
            deadline = time.monotonic() + 2.0
            while (
                store.get_engram(first.id).status is EngramStatus.ACTIVE
                and time.monotonic() < deadline
            ):
                engine.tick()
                time.sleep(0.01)
            assert store.get_engram(first.id).status is EngramStatus.ARCHIVED
            active_ids = {
                engram.id
                for engram in store.list_engrams(status=EngramStatus.ACTIVE)
            }
            assert second.id in active_ids
            assert first.id not in active_ids
            assert len(active_ids) == 2
            assert listener_threads == [threading.current_thread().name]
        finally:
            release_summary.set()
            engine.close()
            store.close()

    def test_saturated_succession_pool_keeps_pending_subject_productive(
        self,
        tmp_path,
    ):
        import threading
        import time

        from pulse_system.agent.harness.base import HarnessTurnResult
        from pulse_system.core.types import EngramStatus

        first_summary_entered = threading.Event()
        release_first_summary = threading.Event()

        class GateHarness:
            def __init__(self):
                self.calls: list[tuple[str, bool]] = []
                self.rotations: list[tuple[str, str]] = []

            def snapshot(self, engram_id):
                raise HarnessError(
                    "pi_session_unknown",
                    "not bootstrapped",
                    "run a turn",
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
                del timeout_sec, bootstrap_text
                is_summary = "comprehensive summary" in prompt
                self.calls.append((engram_id, is_summary))
                if is_summary and engram_id == first.id:
                    first_summary_entered.set()
                    assert release_first_summary.wait(timeout=5.0)
                return HarnessTurnResult(
                    engram_id=engram_id,
                    session_id=f"session-{engram_id}",
                    session_file=str(tmp_path / f"{engram_id}.jsonl"),
                    content="lineage summary" if is_summary else "ordinary result",
                    stop_reason="stop",
                    input_tokens=1,
                    output_tokens=1,
                )

            def succeed(self, old_engram_id, new_engram_id):
                self.rotations.append((old_engram_id, new_engram_id))

        store = Storage(":memory:")
        connections = ConnectionNetwork(store, ConnectionConfig())
        harness = GateHarness()
        manager = EngramManager(
            store,
            LLMAdapter(mock=True),
            connections,
            harness=harness,
        )
        dendrite = DendriteProcessor(
            manager,
            DendriteConfig(silence_threshold=0.0, default_max_wait=0.0),
        )
        runtime = RuntimeManager(RuntimeConfig(
            budget_per_tick=10,
            hourly_token_budget=1_000_000,
            daily_token_budget=10_000_000,
        ))
        engine = PulseEngine(
            storage=store,
            engram_manager=manager,
            connection_network=connections,
            dendrite=dendrite,
            runtime=runtime,
            config=PulseEngineConfig(
                budget_per_tick=10,
                spontaneous_check_interval=1000.0,
                decay_interval=1000.0,
                max_parallel_pulses=2,
                max_parallel_successions=1,
                background_dispatch=True,
            ),
        )
        first = manager.create(initial_messages=[
            Message(role=MessageRole.USER, content="first"),
        ])
        second = manager.create(initial_messages=[
            Message(role=MessageRole.USER, content="second"),
        ])

        try:
            engine._request_succession(
                first.id,
                parent_event_id=None,
                runtime_fence=None,
            )
            engine._request_succession(
                second.id,
                parent_event_id=None,
                runtime_fence=None,
            )
            engine._dispatch_pending_successions()
            assert first_summary_entered.wait(timeout=2.0)
            capacity = engine.capacity_snapshot()
            assert capacity["succession_workers_running"] == 1
            assert capacity["succession_subjects_pending"] == 1

            # Pending is backpressure, not a per-subject hold: B may keep
            # advancing while A owns the only succession worker.
            engine.inject_external_event(second.id, "work while waiting")
            outputs: list[tuple[str, str]] = []
            deadline = time.monotonic() + 2.0
            while not outputs and time.monotonic() < deadline:
                outputs.extend(engine.tick())
                time.sleep(0.01)
            assert (second.id, "ordinary result") in outputs
            assert (second.id, False) in harness.calls
            assert (second.id, True) not in harness.calls
            assert store.get_engram(second.id).status is EngramStatus.ACTIVE

            release_first_summary.set()
            deadline = time.monotonic() + 3.0
            while (
                (
                    store.get_engram(first.id).status is EngramStatus.ACTIVE
                    or store.get_engram(second.id).status is EngramStatus.ACTIVE
                )
                and time.monotonic() < deadline
            ):
                engine.tick()
                time.sleep(0.01)

            assert store.get_engram(first.id).status is EngramStatus.ARCHIVED
            assert store.get_engram(second.id).status is EngramStatus.ARCHIVED
            assert [old for old, _new in harness.rotations] == [first.id, second.id]
            assert harness.calls.count((first.id, True)) == 1
            assert harness.calls.count((second.id, True)) == 1
            assert engine.capacity_snapshot()["succession_subjects_pending"] == 0
        finally:
            release_first_summary.set()
            engine.close()
            store.close()

    def test_stdp_window_spans_separate_ticks(
        self,
        engine: PulseEngine,
        mgr: EngramManager,
        store: Storage,
    ):
        first = _make_engram(mgr, "first activation")
        second = _make_engram(mgr, "second activation")

        engine.inject_external_event(first.id, "one")
        engine.tick()
        assert store.get_connection(first.id, second.id) is None

        engine.inject_external_event(second.id, "two")
        engine.tick()
        assert store.get_connection(first.id, second.id) is not None


# ── Inhibitory connections (v0.4 / 4.1) ──────────────────────────


class TestInhibition:
    def test_inhibitory_edge_delivers_no_content(
        self, engine: PulseEngine, mgr: EngramManager, store: Storage, dendrite: DendriteProcessor
    ):
        from pulse_system.core.types import ConnectionType

        a = _make_engram(mgr, "inhibitor")
        b = _make_engram(mgr, "target")
        store.create_connection(a.id, b.id, 0.6, conn_type=ConnectionType.INHIBITORY)

        engine.propagate(a.id, "suppressive output")

        assert not dendrite.has_pending(b.id)
        assert engine._inhibition_level(b.id, __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc)) > 0

    def test_inhibition_lowers_spontaneous_probability(
        self, engine: PulseEngine, mgr: EngramManager, store: Storage
    ):
        from datetime import datetime, timezone
        from pulse_system.core.types import ConnectionType

        a = _make_engram(mgr, "inhibitor")
        b = _make_engram(mgr, "suppressed")
        store.update_engram_metadata(b.id, self_excitability=1.0, recent_activity=1.0)
        engram_b = store.get_engram(b.id)

        p_before = engine._compute_spontaneous_probability(b.id, engram_b)

        store.create_connection(a.id, b.id, 1.0, conn_type=ConnectionType.INHIBITORY)
        engine.propagate(a.id, "quiet down")

        p_after = engine._compute_spontaneous_probability(b.id, engram_b)
        assert p_after < p_before

    def test_inhibition_lowers_propagation_priority_not_external(
        self, store, mgr, conn_net, runtime
    ):
        from datetime import datetime, timedelta, timezone
        from pulse_system.core.types import ConnectionType

        dendrite = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=0.0, default_max_wait=0.0,
        ))
        engine = PulseEngine(
            storage=store, engram_manager=mgr, connection_network=conn_net,
            dendrite=dendrite, runtime=runtime,
            config=PulseEngineConfig(
                propagation_threshold=0.3, budget_per_tick=0,  # collect only
                spontaneous_check_interval=1000.0, decay_interval=1000.0,
            ),
        )
        inhibitor = _make_engram(mgr, "inhibitor")
        target = _make_engram(mgr, "target")
        store.create_connection(inhibitor.id, target.id, 1.0,
                                conn_type=ConnectionType.INHIBITORY)

        # raise target's inhibition, then feed it normal propagation content
        engine.propagate(inhibitor.id, "hush")
        dendrite.receive(target.id, "regular content", "someone", 0.5)
        engine.tick()  # budget 0: events queued but not executed

        [ev] = engine._pending_events
        assert ev.engram_id == target.id
        assert ev.priority < 0.8  # dampened below the default

        # external events are never dampened
        engine._pending_events.clear()
        engine.inject_external_event(target.id, "user speaks", priority=1.0)
        engine.tick()
        [ev2] = engine._pending_events
        assert ev2.priority == 1.0

    def test_inhibition_decays_over_time(self, engine: PulseEngine, mgr: EngramManager):
        from datetime import datetime, timedelta, timezone

        b = _make_engram(mgr, "target")
        now = datetime.now(timezone.utc)
        engine._add_inhibition(b.id, 1.0, now)

        level_now = engine._inhibition_level(b.id, now)
        level_later = engine._inhibition_level(
            b.id, now + timedelta(seconds=engine.config.inhibition_tau * 3)
        )
        assert level_now == pytest.approx(1.0)
        assert level_later < 0.06  # e^-3 ≈ 0.05


# ── Resonance and lateral inhibition: propagation gate ───────────


class TestInhibitionPropagationGate:
    def _engine(self, store, mgr, conn_net, dendrite, runtime, gate):
        return PulseEngine(
            storage=store, engram_manager=mgr, connection_network=conn_net,
            dendrite=dendrite, runtime=runtime,
            config=PulseEngineConfig(
                propagation_threshold=0.3, budget_per_tick=10,
                spontaneous_check_interval=1000.0, decay_interval=1000.0,
                inhibition_propagation_gate=gate),
        )

    def _now(self):
        from datetime import datetime, timezone
        return datetime.now(timezone.utc)

    def test_gate_on_drops_propagation_when_inhibited(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        from pulse_system.core.pulse.engine import PulseEvent
        eng = self._engine(store, mgr, conn_net, dendrite, runtime, gate=1.0)
        e = _make_engram(mgr)
        eng._add_inhibition(e.id, 1e6, self._now())  # effectively total
        eng._pending_events.append(PulseEvent(
            engram_id=e.id, reason=PulseReason.PROPAGATION, priority=0.8))
        results = eng.tick()
        assert results == []  # gated out
        assert store.get_engram(e.id).total_pulses == 0

    def test_gate_off_propagation_fires_despite_inhibition(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        from pulse_system.core.pulse.engine import PulseEvent
        eng = self._engine(store, mgr, conn_net, dendrite, runtime, gate=0.0)
        e = _make_engram(mgr)
        eng._add_inhibition(e.id, 1e6, self._now())
        eng._pending_events.append(PulseEvent(
            engram_id=e.id, reason=PulseReason.PROPAGATION, priority=0.8))
        results = eng.tick()
        assert len(results) == 1  # baseline: inhibition never gated propagation

    def test_gate_on_leaves_spontaneous_path_to_its_own_gate(
        self, store, mgr, conn_net, dendrite, runtime
    ):
        # the propagation gate must not touch a non-inhibited propagation pulse
        from pulse_system.core.pulse.engine import PulseEvent
        eng = self._engine(store, mgr, conn_net, dendrite, runtime, gate=1.0)
        e = _make_engram(mgr)
        eng._pending_events.append(PulseEvent(
            engram_id=e.id, reason=PulseReason.PROPAGATION, priority=0.8))
        assert len(eng.tick()) == 1  # no inhibition → fires normally


# ── Single-tick same-engram deduplication ─────────────────────────


class TestSameEngramDedup:
    def test_two_events_same_engram_pulse_once(
        self, engine: PulseEngine, mgr: EngramManager, store: Storage
    ):
        """A PROPAGATION and a SPONTANEOUS event on the same engram in one
        tick must execute a single pulse; the loser waits for a later tick."""
        from pulse_system.core.pulse.engine import PulseEvent

        e = _make_engram(mgr, "double-scheduled")
        engine._pending_events.append(PulseEvent(
            engram_id=e.id, reason=PulseReason.PROPAGATION, priority=0.8,
        ))
        engine._pending_events.append(PulseEvent(
            engram_id=e.id, reason=PulseReason.SPONTANEOUS, priority=0.3,
        ))

        results = engine.tick()

        # exactly one pulse for the engram this tick
        assert [r for r in results if r[0] == e.id] == results
        assert len(results) == 1
        assert store.get_engram(e.id).total_pulses == 1
        # the lower-priority duplicate is retained, not dropped
        leftover = [ev for ev in engine._pending_events if ev.engram_id == e.id]
        assert len(leftover) == 1
        assert leftover[0].reason == PulseReason.SPONTANEOUS

    def test_distinct_engrams_not_deduped(
        self, engine: PulseEngine, mgr: EngramManager
    ):
        from pulse_system.core.pulse.engine import PulseEvent

        a = _make_engram(mgr, "A")
        b = _make_engram(mgr, "B")
        engine._pending_events.append(PulseEvent(
            engram_id=a.id, reason=PulseReason.SPONTANEOUS, priority=0.5,
        ))
        engine._pending_events.append(PulseEvent(
            engram_id=b.id, reason=PulseReason.SPONTANEOUS, priority=0.5,
        ))
        results = engine.tick()
        assert {r[0] for r in results} == {a.id, b.id}


# ── Recent-activity decay ─────────────────────────────────────────


class TestRecentActivityDecay:
    def test_pulse_raises_then_decay_lowers_activity(
        self, store, mgr, conn_net, runtime
    ):
        from datetime import timedelta

        from pulse_system.core.pulse.engine import _now

        dend = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=0.0, default_max_wait=0.0,
        ))
        engine = PulseEngine(
            storage=store, engram_manager=mgr, connection_network=conn_net,
            dendrite=dend, runtime=runtime,
            config=PulseEngineConfig(
                spontaneous_check_interval=1000.0,
                decay_interval=0.0,            # decay step runs every tick
                activity_halflife_seconds=1.0,
            ),
        )
        e = _make_engram(mgr, "gets active")

        # a pulse lifts recent_activity toward 1.0 (+0.2/pulse)
        mgr.pulse(e.id)
        assert store.get_engram(e.id).metadata.recent_activity == pytest.approx(0.2)

        # one half-life of elapsed decay halves it
        engine._last_decay_at = _now() - timedelta(seconds=1.0)
        engine.tick()
        decayed = store.get_engram(e.id).metadata.recent_activity
        assert decayed < 0.2
        assert decayed == pytest.approx(0.1, abs=0.02)


# ── Succession accounting from the single summary call ───────────


class _ConcurrentStatsAdapter:
    """Mock-like adapter whose summary call also inflates shared stats, as if
    a concurrent front/clone pulse landed on the same adapter mid-succession.
    Succession accounting must charge only its own call, not the stats delta."""

    def __init__(self):
        from pulse_system.substrate.llm.adapter import LLMStats

        self.stats = LLMStats()
        self.summary_usage = (140, 30, 10)  # (input, output, cached)

    def complete(self, messages, **kw):
        from pulse_system.substrate.llm.adapter import CompletionResult

        i, o, c = self.summary_usage
        self.stats.total_input_tokens += i
        self.stats.total_output_tokens += o
        self.stats.cached_input_tokens += c
        # concurrent, unrelated traffic on the shared stats object
        self.stats.total_input_tokens += 5000
        self.stats.total_output_tokens += 700
        return CompletionResult(
            content="SUMMARY", input_tokens=i, output_tokens=o,
            cached_tokens=c, model="fake",
        )

    def get_stats(self):
        return self.stats


class TestSuccessionAccounting:
    def test_succession_result_reports_single_call_usage(self, store, conn_net):
        adapter = _ConcurrentStatsAdapter()
        m = EngramManager(store, adapter, conn_net)
        e = m.create(initial_messages=[
            Message(role=MessageRole.USER, content="live long"),
        ])
        result = m.succession(e.id)
        assert (result.input_tokens, result.output_tokens, result.cached_tokens) \
            == (140, 30, 10)

    def test_engine_charges_only_the_succession_call(
        self, store, conn_net, runtime
    ):
        adapter = _ConcurrentStatsAdapter()
        m = EngramManager(store, adapter, conn_net)
        dend = DendriteProcessor(m, DendriteConfig(
            silence_threshold=0.0, default_max_wait=0.0,
        ))
        engine = PulseEngine(
            storage=store, engram_manager=m, connection_network=conn_net,
            dendrite=dend, runtime=runtime, config=PulseEngineConfig(),
        )
        e = m.create(initial_messages=[
            Message(role=MessageRole.USER, content="hand over"),
        ])
        engine._run_succession(e.id)

        s = runtime.get_stats()
        # exactly the summary call — the +5000/+700 concurrent inflation is
        # not attributed to succession (old stats-delta bug would have).
        assert s.total_input_tokens == 140
        assert s.total_output_tokens == 30
        assert s.total_cached_input_tokens == 10

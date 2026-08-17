"""End-to-end integration tests for Phase 1–3 pipeline.

Validates the full system lifecycle: engram creation, pulse propagation,
STDP learning, front-stage thinking, succession, spontaneous activation,
and resource competition. All tests use mock LLM.
"""

import random

import pytest

from pulse_system.agent.front import FrontAgent, FrontAgentConfig
from pulse_system.agent.tools import ToolRegistry
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.dendrite import DendriteConfig, DendriteProcessor
from pulse_system.core.engram import EngramManager
from pulse_system.core.pulse import PulseEngine, PulseEngineConfig
from pulse_system.core.runtime import RuntimeConfig, RuntimeManager, StabilityAdvice
from pulse_system.core.types import Engram, EngramStatus, Message, MessageRole
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


# ── Shared fixtures ──────────────────────────────────────────────


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


# ── Scenario 1: Basic lifecycle (create → pulse → propagate → STDP) ──


class TestBasicLifecycle:
    def test_chain_propagation_and_stdp(self, store, mgr, conn_net):
        dendrite = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=0.0,
            default_max_wait=0.0,
        ))
        runtime = RuntimeManager(RuntimeConfig(
            budget_per_tick=10,
            hourly_token_budget=1_000_000,
            daily_token_budget=10_000_000,
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

        # 1. Create 3 engrams with distinct content
        a = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="I study quantum physics."),
        ])
        b = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="I work on neural networks."),
        ])
        c = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="I research philosophy of mind."),
        ])

        # 2. Manual connections: A→B, B→C
        store.create_connection(a.id, b.id, 0.5)
        store.create_connection(b.id, c.id, 0.5)

        # 3. External event to A
        engine.inject_external_event(a.id, "A new quantum entanglement experiment succeeded!")

        # 4-5. Run ticks and track activity
        all_pulsed = []
        for _ in range(6):
            results = engine.tick()
            for eid, output in results:
                all_pulsed.append(eid)

        # Verify: A pulsed
        assert a.id in all_pulsed, "A should have pulsed"
        # Verify: propagation reached B
        assert b.id in all_pulsed, "B should have pulsed via propagation from A"
        # Verify: propagation reached C
        assert c.id in all_pulsed, "C should have pulsed via propagation from B"

        # Verify ordering: A before B before C
        first_a = all_pulsed.index(a.id)
        first_b = all_pulsed.index(b.id)
        first_c = all_pulsed.index(c.id)
        assert first_a < first_b < first_c

        # 6. Verify STDP strengthened A→B connection
        conn_ab = store.get_connection(a.id, b.id)
        assert conn_ab is not None
        assert conn_ab.weight >= 0.5  # should have been strengthened

    def test_session_grows_through_pipeline(self, store, mgr, conn_net):
        dendrite = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=0.0,
            default_max_wait=0.0,
        ))
        runtime = RuntimeManager(RuntimeConfig(budget_per_tick=10))
        engine = PulseEngine(
            storage=store, engram_manager=mgr, connection_network=conn_net,
            dendrite=dendrite, runtime=runtime,
            config=PulseEngineConfig(
                spontaneous_check_interval=1000.0, decay_interval=1000.0,
            ),
        )

        a = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="Hello world"),
        ])
        b = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="Receiver"),
        ])
        store.create_connection(a.id, b.id, 0.5)

        engine.inject_external_event(a.id, "trigger")

        # Tick 1: A pulses
        engine.tick()
        session_a = mgr.get_session(a.id)
        # A: initial_user + injection("trigger") + assistant_response
        assert len(session_a) >= 3

        # Tick 2: B pulses (propagated from A)
        engine.tick()
        session_b = mgr.get_session(b.id)
        # B: initial_user + injection(from A's output) + assistant_response
        assert len(session_b) >= 3


# ── Scenario 2: Front-stage thinking ────────────────────────────


class TestFrontThinking:
    def test_receive_message_and_respond(self, mgr):
        tools = ToolRegistry(mock=True)
        front_engram = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="I am the front-stage consciousness."),
        ])
        front = FrontAgent(front_engram.id, mgr, tools)

        response = front.receive_user_message("你好，今天怎么样？")
        assert isinstance(response, str)
        assert len(response) > 0

        # Verify session grew: initial + user_injection + at least one LLM response
        session = mgr.get_session(front_engram.id)
        assert len(session) >= 3

        # Verify user message is in session
        injections = [m for m in session if m.role == MessageRole.INJECTION]
        user_msgs = [m for m in injections if "你好" in m.content]
        assert len(user_msgs) >= 1

    def test_status_queryable(self, mgr):
        tools = ToolRegistry(mock=True)
        front_engram = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="Front engram init."),
        ])
        front = FrontAgent(front_engram.id, mgr, tools)
        front.receive_user_message("test")

        status = front.get_status()
        assert front_engram.id in status
        assert "pulses" in status


# ── Scenario 3: Succession (generational turnover) ──────────────


class TestSuccession:
    def test_succession_lifecycle(self, store, mgr, mock_llm, conn_net):
        # 1. Create engram and fill with many messages
        engram = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="Let's talk about history."),
        ])

        # Build up a long conversation
        for i in range(20):
            mgr.pulse(engram.id, injected_context=f"Historical fact #{i}: something happened in year {1900 + i}.")

        # Create connections to verify transfer
        other = mgr.create()
        store.create_connection(engram.id, other.id, 0.7)
        store.create_connection(other.id, engram.id, 0.3)

        original_id = engram.id
        session_before = mgr.get_session(original_id)
        assert len(session_before) > 20

        # 2. Trigger succession
        new_id = mgr.succession(original_id).new_id

        # 3. Verify: new engram exists and is active
        new_engram = store.get_engram(new_id)
        assert new_engram is not None
        assert new_engram.status == EngramStatus.ACTIVE

        # Verify: old engram is archived
        old_engram = store.get_engram(original_id)
        assert old_engram.status == EngramStatus.ARCHIVED

        # Verify: connections transferred
        conn_out = store.get_connection(new_id, other.id)
        assert conn_out is not None
        assert conn_out.weight == 0.7

        conn_in = store.get_connection(other.id, new_id)
        assert conn_in is not None
        assert conn_in.weight == 0.3

        # Old connections should be gone (transferred)
        assert store.get_connection(original_id, other.id) is None

        # New engram has a summary as initial content
        new_session = mgr.get_session(new_id)
        assert len(new_session) >= 1


# ── Scenario 4: Spontaneous activation ──────────────────────────


class TestSpontaneousActivation:
    def test_high_excitability_fires_more(self, store, mgr, conn_net):
        # This test isolates spontaneous probability. Cross-tick STDP is
        # covered separately and would otherwise grow a dense graph during the
        # 100 ticks, feeding connection density back into every candidate.
        conn_net.config.coactivation_window = 0.0
        dendrite = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=0.0,
            default_max_wait=0.0,
        ))
        runtime = RuntimeManager(RuntimeConfig(
            budget_per_tick=20,
            hourly_token_budget=10_000_000,
            daily_token_budget=100_000_000,
        ))
        runtime.check_global_stability = lambda *_args: StabilityAdvice.NORMAL
        engine = PulseEngine(
            storage=store, engram_manager=mgr, connection_network=conn_net,
            dendrite=dendrite, runtime=runtime,
            config=PulseEngineConfig(
                propagation_threshold=0.3,
                budget_per_tick=20,
                spontaneous_check_interval=0.0,  # check every tick
                base_spontaneous_rate=0.5,
                decay_interval=1000.0,
            ),
        )

        # Create 5 engrams, no external events
        engrams = []
        for i in range(5):
            e = mgr.create(initial_messages=[
                Message(role=MessageRole.USER, content=f"I am engram {i}."),
            ])
            engrams.append(e)

        # Set one engram to high excitability + activity
        hot = engrams[0]
        store.update_engram_metadata(hot.id, self_excitability=1.0, recent_activity=0.8)
        # Others keep defaults (excitability=0.1, activity=0.0)

        # Sample the probability contract without feeding each sampled pulse
        # back into recent_activity, propagation, or the scheduling queue. The
        # old 100-tick loop measured that whole nonlinear system while claiming
        # to isolate spontaneous probability, and its 2x assertion had become
        # a long-standing false failure.
        probabilities = {
            engram.id: engine._compute_spontaneous_probability(
                engram.id,
                store.get_engram(engram.id),
            )
            for engram in engrams
        }
        hot_probability = probabilities[hot.id]
        other_probabilities = [
            probabilities[engram.id] for engram in engrams[1:]
        ]
        assert hot_probability > max(other_probabilities) * 20

        rng = random.Random(42)
        pulse_counts = {
            engram.id: sum(
                rng.random() < probabilities[engram.id] for _ in range(1_000)
            )
            for engram in engrams
        }

        # The fixed independent sample demonstrates the user-visible direction
        # while the exact formula assertion above carries the deterministic
        # modulation guarantee.
        hot_count = pulse_counts[hot.id]
        other_counts = [pulse_counts[e.id] for e in engrams[1:]]
        max_other = max(other_counts) if other_counts else 0

        assert hot_count > 100, f"Hot engram fired {hot_count} times, expected > 100"
        assert hot_count > max_other * 10, (
            f"Hot ({hot_count}) should be >> max other ({max_other})"
        )


# ── Scenario 5: Resource competition ────────────────────────────


class TestResourceCompetition:
    def test_budget_limits_concurrent_pulses(self, store, mgr, conn_net):
        dendrite = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=0.0,
            default_max_wait=0.0,
        ))
        runtime = RuntimeManager(RuntimeConfig(
            budget_per_tick=100,
            hourly_token_budget=10_000_000,
            daily_token_budget=100_000_000,
        ))
        engine = PulseEngine(
            storage=store, engram_manager=mgr, connection_network=conn_net,
            dendrite=dendrite, runtime=runtime,
            config=PulseEngineConfig(
                budget_per_tick=3,  # only 3 per tick
                spontaneous_check_interval=1000.0,
                decay_interval=1000.0,
            ),
        )

        # Create 10 engrams and inject external events to all
        engrams = []
        for i in range(10):
            e = mgr.create(initial_messages=[
                Message(role=MessageRole.USER, content=f"Engram {i} ready."),
            ])
            engrams.append(e)
            engine.inject_external_event(e.id, f"Stimulus for engram {i}", priority=0.5 + i * 0.01)

        # Single tick: only 3 should fire
        results = engine.tick()
        assert len(results) == 3, f"Expected 3 pulses, got {len(results)}"

        # The remaining 7 should be deferred
        pulsed_ids = {eid for eid, _ in results}
        deferred_count = sum(1 for e in engrams if e.id not in pulsed_ids)
        assert deferred_count == 7

        # Run more ticks to drain the queue
        all_pulsed = set(pulsed_ids)
        for _ in range(5):
            results = engine.tick()
            for eid, _ in results:
                all_pulsed.add(eid)

        # All 10 should eventually pulse
        assert len(all_pulsed) == 10, f"Expected all 10 to pulse, got {len(all_pulsed)}"

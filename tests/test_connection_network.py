"""Tests for the connection network."""

from datetime import datetime, timedelta, timezone

import pytest

from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.types import (
    ConnectionType,
    Message,
    MessageRole,
)
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


def _ts(offset_s: float = 0.0) -> datetime:
    return datetime(2026, 6, 28, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=offset_s)


@pytest.fixture
def store():
    s = Storage(":memory:")
    yield s
    s.close()


@pytest.fixture
def mock_llm():
    return LLMAdapter(mock=True)


@pytest.fixture
def config():
    return ConnectionConfig(
        stdp_strength=0.1,
        coactivation_window=30.0,
        stdp_tau=10.0,
        decay_rate=0.1,
        prune_threshold=0.01,
        embedding_threshold=0.3,
    )


@pytest.fixture
def net(store, config):
    return ConnectionNetwork(store, config)


def _make_engrams(store: Storage, *ids: str):
    for eid in ids:
        store.create_engram(engram_id=eid)


# ── STDP update ──────────────────────────────────────────────────


class TestSTDP:
    def test_two_engrams_creates_connection(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "a", "b")
        activations = [("a", _ts(0)), ("b", _ts(5))]

        changed = net.stdp_update(activations)
        assert len(changed) == 1

        conn = store.get_connection("a", "b")
        assert conn is not None
        assert conn.weight > 0

    def test_stdp_direction(self, net: ConnectionNetwork, store: Storage):
        """A fires before B → A→B connection created, not B→A."""
        _make_engrams(store, "a", "b")
        net.stdp_update([("a", _ts(0)), ("b", _ts(2))])

        assert store.get_connection("a", "b") is not None
        assert store.get_connection("b", "a") is None

    def test_stdp_strengthens_existing(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "a", "b")
        store.create_connection("a", "b", 0.3)

        net.stdp_update([("a", _ts(0)), ("b", _ts(2))])
        conn = store.get_connection("a", "b")
        assert conn.weight > 0.3

    def test_stdp_strength_decays_with_time(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "a", "b", "c")
        # Pre-create connections so STDP strengthens (not creates) them
        store.create_connection("a", "b", 0.1)
        store.create_connection("a", "c", 0.1)

        # a→b at dt=1s (strong), a→c at dt=20s (weak due to exponential decay)
        net.stdp_update([("a", _ts(0)), ("b", _ts(1)), ("c", _ts(20))])

        ab = store.get_connection("a", "b")
        ac = store.get_connection("a", "c")
        assert ab is not None
        assert ac is not None
        assert ab.weight > ac.weight

    def test_outside_window_no_connection(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "a", "b")
        # 31 seconds apart, window is 30
        net.stdp_update([("a", _ts(0)), ("b", _ts(31))])
        assert store.get_connection("a", "b") is None

    def test_self_loop_ignored(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "a")
        net.stdp_update([("a", _ts(0)), ("a", _ts(5))])
        assert store.get_connection("a", "a") is None

    def test_single_activation_noop(self, net: ConnectionNetwork):
        changed = net.stdp_update([("a", _ts(0))])
        assert changed == []

    def test_empty_activations(self, net: ConnectionNetwork):
        changed = net.stdp_update([])
        assert changed == []

    def test_multiple_pairs(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "a", "b", "c")
        # a(0) → b(2), a(0) → c(3), b(2) → c(3)
        activations = [("a", _ts(0)), ("b", _ts(2)), ("c", _ts(3))]
        changed = net.stdp_update(activations)
        assert len(changed) == 3

        assert store.get_connection("a", "b") is not None
        assert store.get_connection("a", "c") is not None
        assert store.get_connection("b", "c") is not None

    def test_weight_capped_at_1(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "a", "b")
        store.create_connection("a", "b", 0.99)

        for _ in range(10):
            net.stdp_update([("a", _ts(0)), ("b", _ts(0.1))])

        conn = store.get_connection("a", "b")
        assert conn.weight <= 1.0


# ── decay_and_prune ──────────────────────────────────────────────


class TestDecayAndPrune:
    def test_decay_reduces_weights(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "a", "b")
        store.create_connection("a", "b", 0.5)

        net.decay_and_prune()
        conn = store.get_connection("a", "b")
        assert conn.weight < 0.5

    def test_prune_removes_weak(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "a", "b", "c")
        store.create_connection("a", "b", 0.005)  # below threshold
        store.create_connection("a", "c", 0.5)

        net.decay_and_prune()
        assert store.get_connection("a", "b") is None
        assert store.get_connection("a", "c") is not None

    def test_decay_and_prune_returns_counts(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "a", "b", "c")
        store.create_connection("a", "b", 0.005)
        store.create_connection("a", "c", 0.5)

        decayed, pruned = net.decay_and_prune()
        assert decayed == 2
        assert pruned == 1


# ── transfer_connections ─────────────────────────────────────────


class TestTransferConnections:
    def test_transfer(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "old", "new", "x", "y")
        store.create_connection("old", "x", 0.5)
        store.create_connection("y", "old", 0.3)

        net.transfer_connections("old", "new")

        assert store.get_connection("old", "x") is None
        assert store.get_connection("new", "x") is not None
        assert store.get_connection("new", "x").weight == pytest.approx(0.5)

        assert store.get_connection("y", "old") is None
        assert store.get_connection("y", "new") is not None


# ── initialize_from_embeddings ───────────────────────────────────


class TestEmbeddingInit:
    def test_similar_texts_get_connected(self, net: ConnectionNetwork, store: Storage, mock_llm: LLMAdapter):
        _make_engrams(store, "e1", "e2")
        store.append_message("e1", Message(role=MessageRole.USER, content="machine learning neural networks deep learning"))
        store.append_message("e2", Message(role=MessageRole.USER, content="machine learning neural networks deep learning"))

        created = net.initialize_from_embeddings(["e1", "e2"], mock_llm)
        # Same text → identical embedding → cosine sim = 1.0 → should connect
        assert len(created) >= 1

    def test_creates_bidirectional(self, net: ConnectionNetwork, store: Storage, mock_llm: LLMAdapter):
        _make_engrams(store, "e1", "e2")
        store.append_message("e1", Message(role=MessageRole.USER, content="identical content"))
        store.append_message("e2", Message(role=MessageRole.USER, content="identical content"))

        net.initialize_from_embeddings(["e1", "e2"], mock_llm)
        assert store.get_connection("e1", "e2") is not None
        assert store.get_connection("e2", "e1") is not None

    def test_single_engram_noop(self, net: ConnectionNetwork, store: Storage, mock_llm: LLMAdapter):
        _make_engrams(store, "e1")
        store.append_message("e1", Message(role=MessageRole.USER, content="hello"))

        created = net.initialize_from_embeddings(["e1"], mock_llm)
        assert created == []

    def test_empty_session_skipped(self, net: ConnectionNetwork, store: Storage, mock_llm: LLMAdapter):
        _make_engrams(store, "e1", "e2")
        # e1 has content, e2 is empty
        store.append_message("e1", Message(role=MessageRole.USER, content="hello"))

        created = net.initialize_from_embeddings(["e1", "e2"], mock_llm)
        assert created == []

    def test_does_not_duplicate(self, net: ConnectionNetwork, store: Storage, mock_llm: LLMAdapter):
        _make_engrams(store, "e1", "e2")
        store.append_message("e1", Message(role=MessageRole.USER, content="same"))
        store.append_message("e2", Message(role=MessageRole.USER, content="same"))

        # Pre-create a connection
        store.create_connection("e1", "e2", 0.3)

        created = net.initialize_from_embeddings(["e1", "e2"], mock_llm)
        # Should only create the reverse, not duplicate e1→e2
        forward_conns = [c for c in created if c.from_id == "e1" and c.to_id == "e2"]
        assert len(forward_conns) == 0

    def test_weight_proportional_to_similarity(self, net: ConnectionNetwork, store: Storage, mock_llm: LLMAdapter):
        _make_engrams(store, "e1", "e2")
        store.append_message("e1", Message(role=MessageRole.USER, content="same text"))
        store.append_message("e2", Message(role=MessageRole.USER, content="same text"))

        net.initialize_from_embeddings(["e1", "e2"], mock_llm)
        conn = store.get_connection("e1", "e2")
        assert conn is not None
        # weight = similarity * 0.5; identical text → sim ≈ 1.0 → weight ≈ 0.5
        assert conn.weight > 0.3


# ── get_propagation_targets ──────────────────────────────────────


class TestPropagationTargets:
    def test_returns_above_threshold(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "a", "b", "c")
        store.create_connection("a", "b", 0.5)
        store.create_connection("a", "c", 0.1)

        targets = net.get_propagation_targets("a", threshold=0.3)
        assert len(targets) == 1
        assert targets[0].to_id == "b"

    def test_no_targets(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "a")
        targets = net.get_propagation_targets("a", threshold=0.3)
        assert targets == []

    def test_all_above(self, net: ConnectionNetwork, store: Storage):
        _make_engrams(store, "a", "b", "c")
        store.create_connection("a", "b", 0.5)
        store.create_connection("a", "c", 0.8)

        targets = net.get_propagation_targets("a", threshold=0.3)
        assert len(targets) == 2


# ── LTD (v0.4 / 4.2) ─────────────────────────────────────────────


class TestLTD:
    def _net(self, store, ltd=0.04):
        return ConnectionNetwork(store, ConnectionConfig(
            stdp_strength=0.1, ltd_strength=ltd,
        ))

    def _activations(self, a, b, gap=1.0):
        from datetime import datetime, timedelta, timezone

        t0 = datetime.now(timezone.utc)
        return [(a, t0), (b, t0 + timedelta(seconds=gap))]

    def test_reverse_edge_weakened(self, store):
        net = self._net(store)
        a = store.create_engram()
        b = store.create_engram()
        store.create_connection(b.id, a.id, 0.5)  # anti-causal edge

        net.stdp_update(self._activations(a.id, b.id))

        weakened = store.get_connection(b.id, a.id)
        assert weakened.weight < 0.5

    def test_no_reverse_edge_created(self, store):
        net = self._net(store)
        a = store.create_engram()
        b = store.create_engram()

        net.stdp_update(self._activations(a.id, b.id))

        # LTP creates a->b; LTD must NOT create b->a
        assert store.get_connection(a.id, b.id) is not None
        assert store.get_connection(b.id, a.id) is None

    def test_ltd_decays_with_gap(self, store):
        net = self._net(store)
        a = store.create_engram()
        b = store.create_engram()
        c = store.create_engram()
        store.create_connection(b.id, a.id, 0.5)
        store.create_connection(c.id, a.id, 0.5)

        net.stdp_update(self._activations(a.id, b.id, gap=1.0))
        net.stdp_update(self._activations(a.id, c.id, gap=9.0))

        w_close = store.get_connection(b.id, a.id).weight
        w_far = store.get_connection(c.id, a.id).weight
        # closer-in-time anti-causal firing is punished harder
        assert w_close < w_far

    def test_ltd_disabled_with_zero_strength(self, store):
        net = self._net(store, ltd=0.0)
        a = store.create_engram()
        b = store.create_engram()
        store.create_connection(b.id, a.id, 0.5)

        net.stdp_update(self._activations(a.id, b.id))

        assert store.get_connection(b.id, a.id).weight == pytest.approx(0.5)

    def test_reciprocal_loop_develops_asymmetry(self, store):
        """A consistently firing before B should tilt the loop toward A->B."""
        net = self._net(store)
        a = store.create_engram()
        b = store.create_engram()
        store.create_connection(a.id, b.id, 0.5)
        store.create_connection(b.id, a.id, 0.5)

        for _ in range(5):
            net.stdp_update(self._activations(a.id, b.id))

        w_ab = store.get_connection(a.id, b.id).weight
        w_ba = store.get_connection(b.id, a.id).weight
        assert w_ab > w_ba

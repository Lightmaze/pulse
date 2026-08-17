"""Tests for substrate registry: substrate adapter registry and per-engram binding."""

import pytest

from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.engram import EngramManager
from pulse_system.core.types import Message, MessageRole
from pulse_system.substrate.llm import LLMAdapter, SubstrateRegistry
from pulse_system.substrate.storage import Storage


class _CountingAdapter(LLMAdapter):
    """Mock adapter that counts its own completions."""

    def __init__(self, tag):
        super().__init__(mock=True)
        self.tag = tag
        self.completions = 0

    def complete(self, messages, **kwargs):
        self.completions += 1
        return super().complete(messages, **kwargs)


@pytest.fixture
def stack():
    store = Storage(":memory:")
    default = _CountingAdapter("default")
    deep = _CountingAdapter("deep")
    registry = SubstrateRegistry(default)
    registry.register("deep-reasoner", deep)
    conn_net = ConnectionNetwork(store, ConnectionConfig())
    mgr = EngramManager(store, default, conn_net, substrates=registry)
    yield store, mgr, registry, default, deep
    store.close()


def _make(mgr, content="engram"):
    return mgr.create(initial_messages=[
        Message(role=MessageRole.USER, content=content),
    ])


class TestRegistry:
    def test_default_fallback(self, stack):
        _, _, registry, default, _ = stack
        assert registry.get() is default
        assert registry.get("nonexistent") is default

    def test_reserved_name(self, stack):
        _, _, registry, _, _ = stack
        with pytest.raises(ValueError):
            registry.register("default", object())

    def test_combined_stats_sums_distinct_adapters(self, stack):
        _, _, registry, default, deep = stack
        default.complete([{"role": "user", "content": "a"}])
        deep.complete([{"role": "user", "content": "b"}])
        total = registry.combined_stats()
        assert total.total_calls == 2
        assert total.total_input_tokens > 0


class TestBinding:
    def test_pulse_uses_bound_substrate(self, stack):
        store, mgr, _, default, deep = stack
        e = _make(mgr)
        mgr.bind_substrate(e.id, "deep-reasoner")

        mgr.pulse(e.id)

        assert deep.completions == 1
        assert default.completions == 0

    def test_unbound_uses_default(self, stack):
        store, mgr, _, default, deep = stack
        e = _make(mgr)
        mgr.pulse(e.id)
        assert default.completions == 1
        assert deep.completions == 0

    def test_binding_persisted_on_engram(self, stack):
        store, mgr, _, _, _ = stack
        e = _make(mgr)
        mgr.bind_substrate(e.id, "deep-reasoner")
        fetched = store.get_engram(e.id)
        assert fetched.substrate_binding == "deep-reasoner"

    def test_unknown_binding_rejected(self, stack):
        _, mgr, _, _, _ = stack
        e = _make(mgr)
        with pytest.raises(ValueError, match="unknown substrate"):
            mgr.bind_substrate(e.id, "no-such-substrate")

    def test_unbind_reverts_to_default(self, stack):
        store, mgr, _, default, deep = stack
        e = _make(mgr)
        mgr.bind_substrate(e.id, "deep-reasoner")
        mgr.bind_substrate(e.id, None)
        mgr.pulse(e.id)
        assert default.completions == 1 and deep.completions == 0

    def test_succession_summary_on_bound_substrate(self, stack):
        store, mgr, _, default, deep = stack
        e = _make(mgr, "long life on the deep substrate")
        mgr.bind_substrate(e.id, "deep-reasoner")

        mgr.succession(e.id)

        assert deep.completions == 1  # the summary call
        assert default.completions == 0

    def test_combined_stats_via_manager(self, stack):
        store, mgr, _, _, deep = stack
        e = _make(mgr)
        mgr.bind_substrate(e.id, "deep-reasoner")
        mgr.pulse(e.id)
        total = mgr.combined_llm_stats()
        assert total.total_calls == 1

    def test_manager_without_registry_unchanged(self):
        store = Storage(":memory:")
        llm = LLMAdapter(mock=True)
        conn_net = ConnectionNetwork(store, ConnectionConfig())
        mgr = EngramManager(store, llm, conn_net)
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="plain"),
        ])
        result = mgr.pulse(e.id)
        assert result.content
        assert mgr.combined_llm_stats().total_calls == 1
        store.close()

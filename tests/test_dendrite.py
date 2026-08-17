"""Tests for the dendritic processor."""

from datetime import datetime, timedelta, timezone

import pytest

from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.dendrite import DendriteConfig, DendriteProcessor
from pulse_system.core.engram import EngramManager
from pulse_system.core.types import Message, MessageRole
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
def mgr(store):
    llm = LLMAdapter(mock=True)
    conn_net = ConnectionNetwork(store, ConnectionConfig())
    return EngramManager(store, llm, conn_net)


@pytest.fixture
def dendrite(mgr):
    config = DendriteConfig(
        silence_threshold=5.0,
        default_max_wait=30.0,
    )
    return DendriteProcessor(mgr, config)


# ── receive ──────────────────────────────────────────────────────


class TestReceive:
    def test_receive_creates_queue(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        dendrite.receive(e.id, "hello", "src1", 0.5)
        assert dendrite.get_queue_size(e.id) == 1

    def test_receive_opens_window(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        assert not dendrite.has_pending(e.id)
        dendrite.receive(e.id, "hello", "src1", 0.5)
        assert dendrite.has_pending(e.id)

    def test_receive_multiple(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        dendrite.receive(e.id, "msg1", "src1", 0.5)
        dendrite.receive(e.id, "msg2", "src2", 0.3)
        dendrite.receive(e.id, "msg3", "src1", 0.5)
        assert dendrite.get_queue_size(e.id) == 3


# ── check_ready ──────────────────────────────────────────────────


class TestCheckReady:
    def test_empty_queue_not_ready(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        assert not dendrite.check_ready(e.id)

    def test_silence_threshold(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        t0 = _ts(0)
        dendrite.receive(e.id, "hello", "src1", 0.5)
        # Override timestamps for deterministic testing
        queue = dendrite._queues[e.id]
        queue.window_opened_at = t0
        queue.last_input_at = t0

        # Not ready immediately
        assert not dendrite.check_ready(e.id, now=_ts(3))
        # Ready after silence threshold (5s)
        assert dendrite.check_ready(e.id, now=_ts(6))

    def test_max_wait_forced_dispatch(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        t0 = _ts(0)
        dendrite.receive(e.id, "hello", "src1", 0.5)
        queue = dendrite._queues[e.id]
        queue.window_opened_at = t0
        queue.last_input_at = _ts(29)  # input just arrived, silence not met

        # Not ready at 29s (last input just arrived, silence not met)
        assert not dendrite.check_ready(e.id, now=_ts(29))
        # Ready at 31s (max wait 30s exceeded)
        assert dendrite.check_ready(e.id, now=_ts(31))

    def test_custom_wait_time(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        dendrite.set_wait_time(e.id, 10.0)

        t0 = _ts(0)
        dendrite.receive(e.id, "hello", "src1", 0.5)
        queue = dendrite._queues[e.id]
        queue.window_opened_at = t0
        queue.last_input_at = _ts(9)

        assert not dendrite.check_ready(e.id, now=_ts(9))
        assert dendrite.check_ready(e.id, now=_ts(11))

    def test_wait_modifier_scales_silence_threshold(
        self, dendrite: DendriteProcessor, mgr: EngramManager
    ):
        """The claustrum wait factor governs both dispatch paths, not just
        max-wait — otherwise silence-driven dispatch (the common path)
        escapes modulation entirely."""
        e = mgr.create()
        t0 = _ts(0)
        dendrite.receive(e.id, "hello", "src1", 0.5)
        queue = dendrite._queues[e.id]
        queue.window_opened_at = t0
        queue.last_input_at = t0

        # base silence threshold is 5s; at 3s nothing dispatches
        assert not dendrite.check_ready(e.id, now=_ts(3))
        # factor 0.5 → effective silence threshold 2.5s → ready at 3s
        dendrite.set_wait_modifiers({e.id: 0.5})
        assert dendrite.check_ready(e.id, now=_ts(3))
        # factor 1.5 → effective threshold 7.5s → not ready at 6s
        dendrite.set_wait_modifiers({e.id: 1.5})
        assert not dendrite.check_ready(e.id, now=_ts(6))
        assert dendrite.check_ready(e.id, now=_ts(8))
        # neutral factor restores base behavior
        dendrite.set_wait_modifiers({})
        assert dendrite.check_ready(e.id, now=_ts(6))


# ── integrate ────────────────────────────────────────────────────


class TestIntegrate:
    def test_single_input(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        dendrite.receive(e.id, "injected thought", "src1", 0.5)

        result = dendrite.integrate(e.id)
        assert result == "injected thought"

        # Queue should be cleared
        assert dendrite.get_queue_size(e.id) == 0
        assert not dendrite.has_pending(e.id)

        # Session should have the injection
        session = mgr.get_session(e.id)
        assert len(session) == 1
        assert session[0].role == MessageRole.INJECTION
        assert session[0].content == "injected thought"

    def test_multiple_inputs_merged(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        dendrite.receive(e.id, "from A", "a", 0.5)
        dendrite.receive(e.id, "from B", "b", 0.3)

        result = dendrite.integrate(e.id)
        assert "from A" in result
        assert "from B" in result

    def test_same_source_grouped(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        dendrite.receive(e.id, "first", "a", 0.5)
        dendrite.receive(e.id, "second", "a", 0.5)
        dendrite.receive(e.id, "other", "b", 0.3)

        result = dendrite.integrate(e.id)
        assert "first" in result
        assert "second" in result
        assert "other" in result

    def test_empty_queue_returns_none(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        assert dendrite.integrate(e.id) is None

    def test_integrate_clears_window(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        dendrite.receive(e.id, "hello", "src1", 0.5)
        dendrite.integrate(e.id)

        # Window should be reset
        queue = dendrite._queues[e.id]
        assert queue.window_opened_at is None
        assert queue.last_input_at is None


# ── get_all_ready ────────────────────────────────────────────────


class TestGetAllReady:
    def test_returns_ready_engrams(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e1 = mgr.create()
        e2 = mgr.create()
        e3 = mgr.create()

        t0 = _ts(0)
        dendrite.receive(e1.id, "msg", "src", 0.5)
        dendrite.receive(e2.id, "msg", "src", 0.5)
        # e3 has no input

        # Set windows to be open since t0
        for eid in [e1.id, e2.id]:
            dendrite._queues[eid].window_opened_at = t0
            dendrite._queues[eid].last_input_at = t0

        ready = dendrite.get_all_ready(now=_ts(6))
        assert set(ready) == {e1.id, e2.id}

    def test_excludes_not_ready(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e1 = mgr.create()
        e2 = mgr.create()

        t0 = _ts(0)
        dendrite.receive(e1.id, "msg", "src", 0.5)
        dendrite.receive(e2.id, "msg", "src", 0.5)

        dendrite._queues[e1.id].window_opened_at = t0
        dendrite._queues[e1.id].last_input_at = t0
        dendrite._queues[e2.id].window_opened_at = _ts(4)
        dendrite._queues[e2.id].last_input_at = _ts(4)

        # At t=6, e1 is ready (silence 6s > 5s), e2 not ready (silence 2s < 5s)
        ready = dendrite.get_all_ready(now=_ts(6))
        assert ready == [e1.id]


# ── wait time management ────────────────────────────────────────


class TestWaitTime:
    def test_default_wait_time(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        assert dendrite.get_wait_time(e.id) == 30.0

    def test_custom_wait_time(self, dendrite: DendriteProcessor, mgr: EngramManager):
        e = mgr.create()
        dendrite.set_wait_time(e.id, 2.0)
        assert dendrite.get_wait_time(e.id) == 2.0


# ── queue transfer during succession ─────────────────────────────


class TestTransferQueue:
    def test_moves_pending_items(self, dendrite: DendriteProcessor, mgr: EngramManager):
        old = mgr.create()
        new = mgr.create()
        dendrite.receive(old.id, "queued input", "src", 0.5)

        dendrite.transfer_queue(old.id, new.id)

        assert not dendrite.has_pending(old.id)
        assert dendrite.has_pending(new.id)
        assert dendrite.get_queue_size(new.id) == 1

    def test_merges_into_existing_queue(self, dendrite: DendriteProcessor, mgr: EngramManager):
        old = mgr.create()
        new = mgr.create()
        dendrite.receive(old.id, "from old", "src", 0.5)
        dendrite.receive(new.id, "already here", "src", 0.5)

        dendrite.transfer_queue(old.id, new.id)

        assert dendrite.get_queue_size(new.id) == 2
        queue = dendrite._queues[new.id]
        assert queue.window_opened_at is not None
        assert queue.last_input_at is not None

    def test_moves_wait_time_override(self, dendrite: DendriteProcessor, mgr: EngramManager):
        old = mgr.create()
        new = mgr.create()
        dendrite.set_wait_time(old.id, 2.0)

        dendrite.transfer_queue(old.id, new.id)

        assert dendrite.get_wait_time(new.id) == 2.0

    def test_noop_when_nothing_queued(self, dendrite: DendriteProcessor, mgr: EngramManager):
        old = mgr.create()
        new = mgr.create()
        dendrite.transfer_queue(old.id, new.id)
        assert not dendrite.has_pending(new.id)

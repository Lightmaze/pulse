"""Tests for sensory cortex channels."""

import pytest

from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.dendrite import DendriteConfig, DendriteProcessor
from pulse_system.core.engram import EngramManager
from pulse_system.core.pulse import PulseEngine, PulseEngineConfig
from pulse_system.core.runtime import RuntimeConfig, RuntimeManager
from pulse_system.core.sensory import (
    CallableChannel,
    FileWatchChannel,
    InteroceptionChannel,
    SensoryCortex,
)
from pulse_system.core.types import Message, MessageRole
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


class TestFileWatchChannel:
    def test_new_file_detected_once(self, tmp_path):
        ch = FileWatchChannel(tmp_path, "*.md")
        assert ch.poll() == []
        (tmp_path / "note.md").write_text("fresh content", encoding="utf-8")
        [item] = ch.poll()
        assert "fresh content" in item and "note.md" in item
        assert ch.poll() == []  # no re-emission

    def test_modification_detected(self, tmp_path):
        f = tmp_path / "doc.md"
        f.write_text("v1", encoding="utf-8")
        ch = FileWatchChannel(tmp_path, "*.md")  # existing file pre-seen
        assert ch.poll() == []
        import os
        f.write_text("v2 changed", encoding="utf-8")
        os.utime(f, (f.stat().st_atime, f.stat().st_mtime + 10))
        [item] = ch.poll()
        assert "v2 changed" in item

    def test_pattern_filter(self, tmp_path):
        ch = FileWatchChannel(tmp_path, "*.md")
        (tmp_path / "ignored.txt").write_text("x", encoding="utf-8")
        assert ch.poll() == []

    def test_truncation(self, tmp_path):
        ch = FileWatchChannel(tmp_path, "*.md")
        (tmp_path / "big.md").write_text("y" * 10_000, encoding="utf-8")
        [item] = ch.poll()
        assert "truncated" in item


class TestOtherChannels:
    def test_callable_channel(self):
        items = [["a", "b"], []]
        ch = CallableChannel(lambda: items.pop(0))
        assert ch.poll() == ["a", "b"]
        assert ch.poll() == []

    def test_callable_channel_error_isolated(self):
        def boom():
            raise RuntimeError("feed down")
        ch = CallableChannel(boom)
        assert ch.poll() == []

    def test_interoception_renders_and_dedupes(self):
        snap = {"heartbeat": {"active": 2, "total": 5, "ratio": 0.4},
                "total_pulses": 42, "billable_tokens_today": 1000,
                "daily_budget_remaining": 9000}
        ch = InteroceptionChannel(lambda: snap, interval_seconds=0.0)
        [text] = ch.poll()
        assert "内感受" in text and "40%" in text and "42" in text
        assert ch.poll() == []  # unchanged state → silent
        snap["total_pulses"] = 43
        [text2] = ch.poll()
        assert "43" in text2

    def test_interoception_interval(self):
        n = {"v": 0}
        def snap():
            n["v"] += 1
            return {"total_pulses": n["v"]}
        ch = InteroceptionChannel(snap, interval_seconds=3600.0)
        assert len(ch.poll()) == 1
        assert ch.poll() == []  # inside interval, even though state changed


class TestSensoryCortex:
    @pytest.fixture
    def stack(self):
        store = Storage(":memory:")
        llm = LLMAdapter(mock=True)
        conn_net = ConnectionNetwork(store, ConnectionConfig())
        mgr = EngramManager(store, llm, conn_net)
        dendrite = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=0.0, default_max_wait=30.0,
        ))
        sensory = SensoryCortex(dendrite, default_wait=1.0)
        runtime = RuntimeManager(RuntimeConfig(
            hourly_token_budget=1_000_000, daily_token_budget=10_000_000,
        ))
        engine = PulseEngine(
            storage=store, engram_manager=mgr, connection_network=conn_net,
            dendrite=dendrite, runtime=runtime, sensory=sensory,
            config=PulseEngineConfig(
                spontaneous_check_interval=1000.0, decay_interval=1000.0,
            ),
        )
        yield store, mgr, dendrite, sensory, engine
        store.close()

    def _make(self, mgr, content="sensory engram"):
        return mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content=content),
        ])

    def test_bind_sets_sensory_rhythm(self, stack):
        _, mgr, dendrite, sensory, _ = stack
        e = self._make(mgr)
        sensory.bind(e.id, CallableChannel(lambda: []))
        assert dendrite.get_wait_time(e.id) == 1.0

    def test_unbind_restores_rhythm(self, stack):
        _, mgr, dendrite, sensory, _ = stack
        e = self._make(mgr)
        sensory.bind(e.id, CallableChannel(lambda: []))
        sensory.unbind(e.id)
        assert dendrite.get_wait_time(e.id) == 30.0
        assert sensory.bound_engrams() == []

    def test_channel_items_pulse_bound_engram(self, stack):
        store, mgr, dendrite, sensory, engine = stack
        e = self._make(mgr)
        queue = [["观察到窗外下雨了"]]
        sensory.bind(e.id, CallableChannel(lambda: queue.pop(0) if queue else []))

        engine.tick()   # intake → dendrite (silence 0 → dispatch same tick chain)
        engine.tick()

        session = mgr.get_session(e.id)
        injections = [m for m in session if m.role == MessageRole.INJECTION]
        assert any("下雨" in m.content for m in injections)
        assert store.get_engram(e.id).total_pulses >= 1

    def test_succession_reassign_keeps_binding(self, stack):
        _, mgr, dendrite, sensory, _ = stack
        e = self._make(mgr)
        sensory.bind(e.id, CallableChannel(lambda: []))
        sensory.reassign_engram(e.id, "successor")
        assert sensory.bound_engrams() == ["successor"]

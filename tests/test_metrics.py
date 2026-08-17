"""Tests for dynamics metrics recording."""

import json
import threading
from datetime import datetime, timezone

import pytest

from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.dendrite import DendriteConfig, DendriteProcessor
from pulse_system.core.engram import EngramManager
from pulse_system.core.pulse import PulseEngine, PulseEngineConfig
from pulse_system.core.runtime import RuntimeConfig, RuntimeManager
from pulse_system.core.runtime.publication import RuntimePublicationGate
from pulse_system.core.types import ConnectionType, Message, MessageRole
from pulse_system.interaction.metrics import MetricsRecorder
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


# ── Recorder unit tests ──────────────────────────────────────────


class TestRecorder:
    def test_bootstrap_permit_cannot_authorize_metrics_persistence(
        self,
        tmp_path,
    ) -> None:
        gate = RuntimePublicationGate("metrics-bootstrap-denied", 1)
        path = tmp_path / "bootstrap.metrics.jsonl"

        with pytest.raises(TypeError, match="RuntimePublicationPermit"):
            MetricsRecorder(
                path,
                publication_permit=gate.bootstrap_permit,  # type: ignore[arg-type]
            )
        assert not path.exists()

    def test_record_and_read(self):
        rec = MetricsRecorder()
        rec.record("pulse", engram="e1", depth=0)
        rec.record("pulse", engram="e2", depth=1)
        rec.record("heartbeat", active=1, total=3, ratio=0.33)

        assert len(rec.events()) == 3
        assert len(rec.events("pulse")) == 2
        series = rec.series("pulse", "depth")
        assert [v for _, v in series] == [0, 1]

    def test_summary(self):
        rec = MetricsRecorder()
        rec.record("heartbeat", active=2, total=4, ratio=0.5)
        rec.record("pulse", engram="e1")
        s = rec.summary()
        assert s["event_counts"] == {"heartbeat": 1, "pulse": 1}
        assert s["heartbeat"]["ratio"] == 0.5

    def test_jsonl_persistence(self, tmp_path):
        path = tmp_path / "metrics.jsonl"
        rec = MetricsRecorder(path)
        for i in range(5):
            rec.record("pulse", engram=f"e{i}")
        rec.flush()

        lines = path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 5
        parsed = json.loads(lines[0])
        assert parsed["type"] == "pulse"
        assert "t" in parsed

    def test_flush_every_one_writes_immediately(self, tmp_path):
        """Live observation needs per-event durability: with flush_every=1
        each record hits disk without waiting for the 200-event batch."""
        path = tmp_path / "m.jsonl"
        rec = MetricsRecorder(path, flush_every=1)
        rec.record("pulse", engram="e1")
        assert len(path.read_text(encoding="utf-8").strip().splitlines()) == 1
        rec.record("heartbeat", ratio=0.5)
        assert len(path.read_text(encoding="utf-8").strip().splitlines()) == 2

    def test_runtime_revocation_keeps_late_metrics_memory_only(self, tmp_path):
        path = tmp_path / "runtime.metrics.jsonl"
        gate = RuntimePublicationGate("metrics-runtime", 1)
        rec = MetricsRecorder(
            path,
            flush_every=1,
            publication_permit=gate.publication_permit,
        )
        rec.record("before_revoke", sequence=1)
        committed = path.read_bytes()

        gate.revoke(reason="test_shutdown")
        rec.record("after_revoke", sequence=2)
        rec.flush()

        assert path.read_bytes() == committed
        assert [event["type"] for event in rec.events()] == [
            "before_revoke",
            "after_revoke",
        ]

    def test_metrics_flush_and_runtime_revoke_have_one_ordering_point(
        self,
        tmp_path,
        monkeypatch,
    ):
        path = tmp_path / "ordered.metrics.jsonl"
        gate = RuntimePublicationGate("metrics-order", 1)
        rec = MetricsRecorder(
            path,
            flush_every=1,
            publication_permit=gate.publication_permit,
        )
        entered = threading.Event()
        release = threading.Event()
        revoked = threading.Event()
        original_file_size = rec._file_size

        def blocking_file_size(candidate):
            entered.set()
            assert release.wait(timeout=2.0)
            return original_file_size(candidate)

        monkeypatch.setattr(rec, "_file_size", blocking_file_size)
        writer = threading.Thread(target=lambda: rec.record("ordered", sequence=1))
        writer.start()
        assert entered.wait(timeout=1.0)

        revoker = threading.Thread(
            target=lambda: (
                gate.revoke(reason="test_shutdown"),
                revoked.set(),
            )
        )
        revoker.start()
        # The flush already owns an admitted transaction, but it cannot make
        # the hard Runtime revoke wait for filesystem progress.
        assert revoked.wait(timeout=0.2)

        release.set()
        writer.join(timeout=2.0)
        revoker.join(timeout=2.0)

        assert not writer.is_alive()
        assert not revoker.is_alive()
        assert revoked.is_set()
        assert json.loads(path.read_text(encoding="utf-8"))["type"] == "ordered"

    def test_memory_cap(self):
        rec = MetricsRecorder(memory_cap=10)
        for i in range(50):
            rec.record("pulse", i=i)
        assert len(rec.events()) == 10
        # counts still reflect everything recorded
        assert rec.summary()["event_counts"]["pulse"] == 50

    def test_rotation_caps_active_file_and_finite_archives(self, tmp_path):
        path = tmp_path / "metrics.jsonl"
        max_bytes = 320
        rec = MetricsRecorder(
            path,
            flush_every=1,
            max_bytes=max_bytes,
            archive_count=2,
        )

        for i in range(40):
            rec.record("pulse", engram=f"engram-{i:03d}", sequence=i)

        managed = [path, tmp_path / "metrics.jsonl.1", tmp_path / "metrics.jsonl.2"]
        existing = [candidate for candidate in managed if candidate.exists()]
        assert existing
        assert all(candidate.stat().st_size <= max_bytes for candidate in existing)
        assert sum(candidate.stat().st_size for candidate in existing) <= 3 * max_bytes
        assert not (tmp_path / "metrics.jsonl.3").exists()

        active = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        assert active[-1]["sequence"] == 39

    def test_oversized_legacy_file_is_preserved_on_first_rotation(self, tmp_path):
        path = tmp_path / "metrics.jsonl"
        path.write_bytes(b"x" * 1024)
        rec = MetricsRecorder(
            path,
            flush_every=1,
            max_bytes=256,
            archive_count=1,
        )

        rec.record("heartbeat", active=1, total=1, ratio=1.0)

        assert path.stat().st_size <= 256
        assert not (tmp_path / "metrics.jsonl.1").exists()
        assert (tmp_path / "metrics.jsonl.legacy").read_bytes() == b"x" * 1024
        assert json.loads(path.read_text(encoding="utf-8"))["type"] == "heartbeat"

    def test_flush_recovers_when_rotation_left_no_active_file(self, tmp_path):
        path = tmp_path / "metrics.jsonl"
        archive = tmp_path / "metrics.jsonl.1"
        archive.write_text('{"t":"old","type":"heartbeat"}\n', encoding="utf-8")
        rec = MetricsRecorder(
            path,
            flush_every=1,
            max_bytes=256,
            archive_count=2,
        )

        rec.record("pulse", engram="new")

        assert archive.exists()
        assert json.loads(path.read_text(encoding="utf-8"))["engram"] == "new"

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"memory_cap": 0}, "memory_cap"),
            ({"max_bytes": 0}, "max_bytes"),
            ({"archive_count": -1}, "archive_count"),
        ],
    )
    def test_bounded_configuration_is_validated(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            MetricsRecorder(**kwargs)


# ── Engine integration ───────────────────────────────────────────


@pytest.fixture
def stack():
    store = Storage(":memory:")
    llm = LLMAdapter(mock=True)
    conn_net = ConnectionNetwork(store, ConnectionConfig(stdp_strength=0.1))
    mgr = EngramManager(store, llm, conn_net)
    dendrite = DendriteProcessor(mgr, DendriteConfig(
        silence_threshold=0.0, default_max_wait=0.0,
    ))
    runtime = RuntimeManager(RuntimeConfig(
        budget_per_tick=10,
        hourly_token_budget=1_000_000,
        daily_token_budget=10_000_000,
    ))
    metrics = MetricsRecorder()
    engine = PulseEngine(
        storage=store, engram_manager=mgr, connection_network=conn_net,
        dendrite=dendrite, runtime=runtime, metrics=metrics,
        config=PulseEngineConfig(
            propagation_threshold=0.3, budget_per_tick=10,
            spontaneous_check_interval=1000.0, decay_interval=1000.0,
        ),
    )
    yield store, mgr, engine, metrics
    store.close()


def _make(mgr, content="hello"):
    return mgr.create(initial_messages=[
        Message(role=MessageRole.USER, content=content),
    ])


class TestEngineMetrics:
    def test_heartbeat_recorded_every_tick(self, stack):
        _, mgr, engine, metrics = stack
        _make(mgr)
        engine.tick()
        engine.tick()
        beats = metrics.events("heartbeat")
        assert len(beats) == 2
        assert beats[0]["total"] == 1

    def test_pulse_event_carries_tokens_and_reason(self, stack):
        _, mgr, engine, metrics = stack
        e = _make(mgr)
        engine.inject_external_event(e.id, "stimulus")
        engine.tick()
        [pulse] = metrics.events("pulse")
        assert pulse["engram"] == e.id
        assert pulse["reason"] == "propagation" or pulse["reason"] == "external"
        assert pulse["input_tokens"] > 0

    def test_chain_depth_increases_along_propagation(self, stack):
        store, mgr, engine, metrics = stack
        a = _make(mgr, "A")
        b = _make(mgr, "B")
        c = _make(mgr, "C")
        store.create_connection(a.id, b.id, 0.5)
        store.create_connection(b.id, c.id, 0.5)

        engine.inject_external_event(a.id, "start")
        for _ in range(3):
            engine.tick()

        depths = {e["engram"]: e["depth"] for e in metrics.events("pulse")}
        assert depths[a.id] == 0
        assert depths[b.id] == 1
        assert depths[c.id] == 2

    def test_propagate_event_lists_targets(self, stack):
        store, mgr, engine, metrics = stack
        a = _make(mgr, "A")
        b = _make(mgr, "B")
        store.create_connection(a.id, b.id, 0.5)
        engine.propagate(a.id, "content")
        [prop] = metrics.events("propagate")
        assert prop["source"] == a.id
        assert b.id in prop["targets"]

    def test_resonance_pairs_by_project(self, stack):
        store, mgr, engine, metrics = stack
        p = store.create_project("proj")
        a = mgr.create(project_id=p.id, initial_messages=[
            Message(role=MessageRole.USER, content="A"),
        ])
        b = mgr.create(project_id=p.id, initial_messages=[
            Message(role=MessageRole.USER, content="B"),
        ])
        c = _make(mgr, "C")  # no project
        for e in (a, b, c):
            engine.inject_external_event(e.id, "go")
        engine.tick()

        [res] = metrics.events("resonance")
        assert res["same_project_pairs"] == 1   # (a,b)
        assert res["cross_pairs"] == 2          # (a,c), (b,c)

    def test_no_metrics_is_noop(self, stack):
        """Engine without a recorder must behave identically."""
        store, mgr, _, _ = stack
        conn_net = ConnectionNetwork(store, ConnectionConfig())
        dendrite = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=0.0, default_max_wait=0.0,
        ))
        engine = PulseEngine(
            storage=store, engram_manager=mgr, connection_network=conn_net,
            dendrite=dendrite,
            runtime=RuntimeManager(RuntimeConfig(
                hourly_token_budget=1_000_000, daily_token_budget=1_000_000,
            )),
            config=PulseEngineConfig(
                spontaneous_check_interval=1000.0, decay_interval=1000.0,
            ),
        )
        e = _make(mgr)
        engine.inject_external_event(e.id, "x")
        assert len(engine.tick()) >= 0  # no crash, no recorder


class TestTopologySnapshot:
    """metrics topology event: the resting-state graph the activity stream omits.

    Pulse/propagate events carry activation edges only; a network view needs
    the standing connection weights too.
    """

    def _engine_with_topology(self, store, mgr, interval):
        conn_net = ConnectionNetwork(store, ConnectionConfig(stdp_strength=0.1))
        dendrite = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=0.0, default_max_wait=0.0,
        ))
        metrics = MetricsRecorder()
        engine = PulseEngine(
            storage=store, engram_manager=mgr, connection_network=conn_net,
            dendrite=dendrite,
            runtime=RuntimeManager(RuntimeConfig(
                budget_per_tick=10, hourly_token_budget=1_000_000,
                daily_token_budget=10_000_000,
            )),
            metrics=metrics,
            config=PulseEngineConfig(
                budget_per_tick=10, spontaneous_check_interval=1000.0,
                decay_interval=1000.0, topology_interval_ticks=interval,
            ),
        )
        return engine, metrics

    def test_off_by_default(self, stack):
        """Baseline runs stay byte-identical — opt-in only."""
        _, mgr, engine, metrics = stack
        _make(mgr)
        for _ in range(5):
            engine.tick()
        assert metrics.events("topology") == []

    def test_snapshot_carries_nodes_and_indexed_edges(self, stack):
        store, mgr, _, _ = stack
        a = _make(mgr, "A")
        b = _make(mgr, "B")
        store.create_connection(a.id, b.id, 0.42)
        engine, metrics = self._engine_with_topology(store, mgr, 1)

        engine.tick()

        [snap] = metrics.events("topology")
        ids = [n["id"] for n in snap["engrams"]]
        assert set(ids) == {a.id, b.id}
        # Edges reference node positions, not repeated 36-char uuids: a full
        # dump every minute must stay cheap enough to keep in the JSONL.
        [edge] = snap["edges"]
        src, dst, weight, kind = edge
        assert ids[src] == a.id
        assert ids[dst] == b.id
        assert weight == pytest.approx(0.42)
        assert kind == "e"

    def test_snapshot_respects_interval(self, stack):
        store, mgr, _, _ = stack
        _make(mgr, "A")
        engine, metrics = self._engine_with_topology(store, mgr, 3)

        for _ in range(7):
            engine.tick()

        # ticks 3 and 6 — first tick does not snapshot
        assert len(metrics.events("topology")) == 2

    def test_inhibitory_edges_are_marked(self, stack):
        store, mgr, _, _ = stack
        a = _make(mgr, "A")
        b = _make(mgr, "B")
        store.create_connection(a.id, b.id, 0.3, conn_type=ConnectionType.INHIBITORY)
        engine, metrics = self._engine_with_topology(store, mgr, 1)

        engine.tick()

        [snap] = metrics.events("topology")
        assert snap["edges"][0][3] == "i"

    def test_node_carries_project_and_activity(self, stack):
        store, mgr, _, _ = stack
        p = store.create_project("proj")
        e = mgr.create(project_id=p.id, initial_messages=[
            Message(role=MessageRole.USER, content="A"),
        ])
        engine, metrics = self._engine_with_topology(store, mgr, 1)
        engine.inject_external_event(e.id, "go")

        engine.tick()

        [snap] = metrics.events("topology")
        [node] = snap["engrams"]
        assert node["project"] == p.id
        assert node["pulses"] >= 1
        assert node["activity"] > 0.0


class TestConnectivitySnapshot:
    """Production cadence for the threshold-eligible content graph."""

    def _engine_with_connectivity(
        self,
        store,
        mgr,
        interval,
        *,
        gate=0.0,
        inhibition_tau=30.0,
    ):
        conn_net = ConnectionNetwork(store, ConnectionConfig(stdp_strength=0.1))
        dendrite = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=0.0, default_max_wait=0.0,
        ))
        metrics = MetricsRecorder()
        engine = PulseEngine(
            storage=store,
            engram_manager=mgr,
            connection_network=conn_net,
            dendrite=dendrite,
            runtime=RuntimeManager(RuntimeConfig(
                budget_per_tick=10,
                hourly_token_budget=1_000_000,
                daily_token_budget=10_000_000,
            )),
            metrics=metrics,
            config=PulseEngineConfig(
                budget_per_tick=10,
                base_spontaneous_rate=0.0,
                spontaneous_check_interval=1000.0,
                decay_interval=1000.0,
                propagation_threshold=0.3,
                inhibition_propagation_gate=gate,
                inhibition_tau=inhibition_tau,
                connectivity_interval_ticks=interval,
            ),
        )
        return engine, metrics

    def test_direct_engine_is_off_by_default(self, stack):
        _, mgr, engine, metrics = stack
        _make(mgr)
        for _ in range(5):
            engine.tick()
        assert metrics.events("connectivity") == []

    def test_snapshot_respects_cadence_and_is_content_free(self, stack):
        store, mgr, _, _ = stack
        private_text = "private session content must never enter metrics"
        _make(mgr, private_text)
        engine, metrics = self._engine_with_connectivity(store, mgr, 2)

        for _ in range(5):
            engine.tick()

        snapshots = metrics.events("connectivity")
        assert [event["tick"] for event in snapshots] == [2, 4]
        serialized = json.dumps(snapshots, ensure_ascii=False)
        assert private_text not in serialized
        assert all(event["schema_version"] == "pulse-connectivity.v1" for event in snapshots)
        assert all(
            event["evidence_class"] == "runtime_effective_threshold_projection"
            for event in snapshots
        )

    def test_source_threshold_modulation_is_observed(self, stack):
        store, mgr, _, _ = stack
        a = _make(mgr, "A")
        b = _make(mgr, "B")
        store.create_connection(a.id, b.id, 0.5)
        engine, metrics = self._engine_with_connectivity(store, mgr, 1)
        engine._propagation_mods = {a.id: 2.0, b.id: 0.5}

        engine.tick()

        [snapshot] = metrics.events("connectivity")
        assert snapshot["raw_edge_count"] == 1
        assert snapshot["effective_excitatory_edge_count"] == 0
        assert snapshot["source_threshold_min"] == 0.15
        assert snapshot["source_threshold_max"] == 0.6

    def test_target_gate_acceptance_is_descriptive_not_an_edge_cutoff(self, stack):
        store, mgr, _, _ = stack
        a = _make(mgr, "A")
        b = _make(mgr, "B")
        store.create_connection(a.id, b.id, 0.5)
        engine, metrics = self._engine_with_connectivity(
            store,
            mgr,
            1,
            gate=1.0,
            inhibition_tau=1_000_000_000.0,
        )
        engine._gate_mods = {b.id: 2.0}
        engine._inhibition[b.id] = (1.0, datetime.now(timezone.utc))

        engine.tick()

        [snapshot] = metrics.events("connectivity")
        assert snapshot["effective_excitatory_edge_count"] == 1
        assert snapshot["minimum_gate_acceptance"] == pytest.approx(1 / 3, abs=1e-5)
        assert snapshot["mean_gate_acceptance"] == pytest.approx(1 / 3, abs=1e-5)

    @pytest.mark.parametrize("value", [0, -1, True, 1.5])
    def test_direct_engine_rejects_invalid_cadence(self, stack, value):
        store, mgr, _, _ = stack
        conn_net = ConnectionNetwork(store, ConnectionConfig())
        dendrite = DendriteProcessor(mgr, DendriteConfig())
        with pytest.raises(ValueError, match="connectivity_interval_ticks"):
            PulseEngine(
                storage=store,
                engram_manager=mgr,
                connection_network=conn_net,
                dendrite=dendrite,
                runtime=RuntimeManager(),
                config=PulseEngineConfig(connectivity_interval_ticks=value),
            )

    def test_runtime_service_defaults_to_bounded_observation(self):
        from pulse_system.service.runtime import RuntimeServiceConfig

        assert RuntimeServiceConfig().connectivity_interval_ticks == 100
        assert RuntimeServiceConfig(
            connectivity_interval_ticks=None
        ).connectivity_interval_ticks is None
        for value in (0, -1, True, 1.5):
            with pytest.raises(ValueError, match="connectivity_interval_ticks"):
                RuntimeServiceConfig(connectivity_interval_ticks=value)

    def test_runtime_service_passes_cadence_to_engine(self, tmp_path):
        from pulse_system.service.runtime import RuntimeService, RuntimeServiceConfig

        service = RuntimeService(RuntimeServiceConfig(
            db_path=tmp_path / "connectivity-runtime.db",
            metrics_path=tmp_path / "connectivity-runtime.jsonl",
            workspace=tmp_path,
            mock=True,
            base_spontaneous_rate=0.0,
            connectivity_interval_ticks=7,
        ))
        try:
            assert service._engine.config.connectivity_interval_ticks == 7
        finally:
            service.close()


class TestWeightSummary:
    def test_summary_fields(self, stack):
        store, mgr, _, _ = stack
        a = _make(mgr, "A")
        b = _make(mgr, "B")
        c = _make(mgr, "C")
        store.create_connection(a.id, b.id, 0.2)
        store.create_connection(b.id, c.id, 0.6)
        s = store.weight_summary()
        assert s["count"] == 2
        assert s["min"] == pytest.approx(0.2)
        assert s["max"] == pytest.approx(0.6)

    def test_empty(self, stack):
        store, _, _, _ = stack
        assert store.weight_summary()["count"] == 0

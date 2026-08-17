"""Tests for the right-hand rail's three reads.

Two properties carry most of the weight here:

- An empty network is a *state*, not a failure. No engrams, no edges, no
  firings, a database the run has not created yet — every one of those
  answers 200 with an empty array. Only "no database was configured at all"
  is an error, because that is the server saying it cannot see, which is a
  different sentence from "there is nothing there".
- /pulse/history is a firing sequence, not a curve. Order is the payload:
  it is the same input STDP consumes, so nothing may be bucketed, merged or
  dropped, including two firings inside one second.
"""

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pulse_system.core.types import (
    ConnectionType,
    Message,
    MessageRole,
)
from pulse_system.core.connection.viability import (
    ConnectivityEdge,
    analyze_connectivity,
)
from pulse_system.interaction.api.app import create_app
from pulse_system.interaction.api.routes_pulse import create_pulse_router
from pulse_system.substrate.storage import Storage


def _client(tmp_path, *, metrics=None, db=None, **kwargs) -> TestClient:
    app = FastAPI()
    app.include_router(
        create_pulse_router(metrics or tmp_path / "m.jsonl", db_path=db, **kwargs)
    )
    return TestClient(app)


def _write(path, events) -> None:
    with path.open("a", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _graph_db(tmp_path, node_ids, edges) -> object:
    db = tmp_path / "connectivity.db"
    store = Storage(db)
    for node_id in node_ids:
        store.create_engram(engram_id=node_id)
    for edge in edges:
        store.create_connection(
            edge.from_id,
            edge.to_id,
            edge.weight,
            conn_type=edge.conn_type,
        )
    store.close()
    return db


def _runtime_connectivity_event(
    node_ids,
    edges,
    *,
    at=None,
    base_threshold=0.3,
    threshold_factors=None,
    target_gate_acceptance=None,
):
    if target_gate_acceptance is None:
        target_gate_acceptance = {node_id: 1.0 for node_id in node_ids}
    projection = analyze_connectivity(
        node_ids,
        edges,
        base_threshold=base_threshold,
        threshold_factors=threshold_factors,
        target_gate_acceptance=target_gate_acceptance,
        evidence_class="runtime_effective_threshold_projection",
    )
    return {
        "t": _iso(at or datetime.now(timezone.utc)),
        "type": "connectivity",
        "tick": 100,
        **projection,
    }


class TestEmptyIsAState:
    """No data is not an error, and "I cannot see" is not "nothing is there"."""

    def test_empty_store_returns_empty_arrays(self, tmp_path):
        db = tmp_path / "run.db"
        Storage(db).close()  # a real, schema'd, empty run
        c = _client(tmp_path, db=db)

        assert c.get("/pulse/active").json() == []
        assert c.get("/pulse/history").json() == []
        assert c.get("/pulse/topology").json() == {"nodes": [], "edges": []}
        connectivity = c.get("/pulse/connectivity")
        assert connectivity.status_code == 200
        assert connectivity.json()["structural_regime"] == "empty"
        for p in (
            "/pulse/active",
            "/pulse/history",
            "/pulse/topology",
            "/pulse/connectivity",
        ):
            assert c.get(p).status_code == 200

    def test_database_not_written_yet_is_empty_not_broken(self, tmp_path):
        """Opening the rail before the run starts must not be an error."""
        c = _client(tmp_path, db=tmp_path / "not-yet.db")
        assert c.get("/pulse/active").json() == []
        assert c.get("/pulse/topology").json() == {"nodes": [], "edges": []}
        assert c.get("/pulse/connectivity").json()["structural_regime"] == "empty"

    def test_file_without_schema_is_empty_not_500(self, tmp_path):
        db = tmp_path / "blank.db"
        sqlite3.connect(db).close()  # a file, but no tables
        c = _client(tmp_path, db=db)
        assert c.get("/pulse/active").status_code == 200
        assert c.get("/pulse/active").json() == []
        assert c.get("/pulse/topology").json() == {"nodes": [], "edges": []}
        assert c.get("/pulse/connectivity").json()["structural_regime"] == "empty"

    def test_no_database_configured_says_how_to_fix_it(self, tmp_path):
        """Contract §6: a refusal without a remedy is half a refusal.

        Flat keys, not FastAPI's nested {"detail": {...}} — the rail reads
        error/detail/remedy off the top level (web/src/pulse.ts readFault),
        so a nested remedy would never reach the screen.
        """
        c = _client(tmp_path)
        for path in ("/pulse/active", "/pulse/topology", "/pulse/connectivity"):
            r = c.get(path)
            assert r.status_code == 404
            body = r.json()
            assert body["error"] == "no_db"
            assert isinstance(body["detail"], str)
            assert "--db" in body["remedy"]

    def test_history_needs_no_database_at_all(self, tmp_path):
        """The firing sequence lives in the JSONL, so it answers regardless."""
        assert _client(tmp_path).get("/pulse/history").json() == []

    def test_no_provider_key_is_ever_required(self, tmp_path, monkeypatch):
        for var in ("DEEPSEEK_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
            monkeypatch.delenv(var, raising=False)
        db = tmp_path / "run.db"
        Storage(db).close()
        c = _client(tmp_path, db=db)
        assert c.get("/pulse/active").status_code == 200
        assert c.get("/pulse/topology").status_code == 200
        assert c.get("/pulse/connectivity").status_code == 200

    def test_writer_holding_the_db_never_500s(self, tmp_path):
        db = tmp_path / "run.db"
        store = Storage(db)
        store.create_engram(engram_id="e1")
        writer = Storage(db)  # simulates the engine's open connection
        try:
            assert _client(tmp_path, db=db).get(
                "/pulse/active"
            ).status_code in (200, 503)
        finally:
            writer.close()
            store.close()


@pytest.fixture
def populated(tmp_path):
    """Three engrams: one that just fired, one long quiet, one archived."""
    db = tmp_path / "run.db"
    store = Storage(db)
    now = datetime.now(timezone.utc)
    store.create_engram(engram_id="e-alpha", initial_messages=[
        Message(role=MessageRole.USER, content="  什么是心潮？\n  n/N 的比例  "),
        Message(role=MessageRole.ASSISTANT, content="活跃比例。"),
    ])
    store.create_engram(engram_id="e-beta", initial_messages=[
        Message(role=MessageRole.USER, content="另一个线程"),
    ])
    store.create_engram(engram_id="e-gone", initial_messages=[
        Message(role=MessageRole.USER, content="已归档"),
    ])
    store.update_engram_metadata(
        "e-alpha", last_pulse_at=now, total_pulses=3, recent_activity=0.61234,
    )
    store.update_engram_metadata(
        "e-beta", last_pulse_at=now - timedelta(seconds=600), total_pulses=1,
    )
    store.archive_engram("e-gone")
    store.create_connection("e-alpha", "e-beta", 0.4212345)
    store.create_connection("e-beta", "e-alpha", 0.12,
                            conn_type=ConnectionType.INHIBITORY)
    store.create_connection("e-alpha", "e-gone", 0.9)  # endpoint archived
    store.close()
    return db


class TestActive:
    def test_reports_each_live_engram_with_its_standing(self, tmp_path, populated):
        response = _client(tmp_path, db=populated).get("/pulse/active")
        body = response.json()

        assert [e["engram_id"] for e in body] == ["e-alpha", "e-beta"]
        alpha, beta = body
        assert set(alpha) == {
            "engram_id", "name", "name_origin", "nickname", "firing",
            "inhibition", "gate", "last_fired_at",
        }
        assert alpha["name"] == "什么是心潮？ n/N 的比例"  # first message, one line
        assert alpha["name_origin"] == "auto"
        assert alpha["nickname"] is None
        assert alpha["firing"] is True
        assert beta["firing"] is False  # fired 10 minutes ago
        assert alpha["gate"] == 0.0
        assert alpha["inhibition"] == 0.0
        assert response.headers["X-Pulse-Replay-Complete"] == "true"

    def test_archived_engram_is_not_drawn_as_live(self, tmp_path, populated):
        body = _client(tmp_path, db=populated).get("/pulse/active").json()
        assert "e-gone" not in {e["engram_id"] for e in body}

    def test_never_fired_engram_has_a_null_time_not_a_fake_one(self, tmp_path):
        db = tmp_path / "run.db"
        store = Storage(db)
        store.create_engram(engram_id="e-new", initial_messages=[
            Message(role=MessageRole.USER, content="just born"),
        ])
        store.close()
        [e] = _client(tmp_path, db=db).get("/pulse/active").json()
        assert e["last_fired_at"] is None
        assert e["firing"] is False

    def test_unnamed_engram_falls_back_to_its_signature(self, tmp_path):
        db = tmp_path / "run.db"
        store = Storage(db)
        store.create_engram(engram_id="e-silent")
        store.close()
        [e] = _client(tmp_path, db=db).get("/pulse/active").json()
        assert e["name"] == "e-silent"

    def test_gate_reflects_the_run_it_was_told_about(self, tmp_path, populated):
        c = _client(tmp_path, db=populated, gate=0.5)
        assert {e["gate"] for e in c.get("/pulse/active").json()} == {0.5}

    def test_inhibition_is_rebuilt_from_the_propagate_stream(
        self, tmp_path, populated
    ):
        """No event carries the level, so the sideband re-runs the engine's
        own decay over what is recorded: source, targets, edge weight, gap."""
        m = tmp_path / "m.jsonl"
        now = datetime.now(timezone.utc)
        _write(m, [
            {"t": _iso(now), "type": "propagate", "source": "e-beta",
             "targets": [], "inhibited": ["e-alpha"]},
        ])
        [alpha, beta] = _client(tmp_path, metrics=m, db=populated).get(
            "/pulse/active"
        ).json()
        assert alpha["inhibition"] == pytest.approx(0.12, abs=0.01)  # the edge weight
        assert beta["inhibition"] == 0.0

    def test_inhibition_decays_with_the_gap(self, tmp_path, populated):
        m = tmp_path / "m.jsonl"
        old = datetime.now(timezone.utc) - timedelta(seconds=120)  # 4 tau
        _write(m, [
            {"t": _iso(old), "type": "propagate", "source": "e-beta",
             "inhibited": ["e-alpha"]},
        ])
        [alpha, _] = _client(tmp_path, metrics=m, db=populated).get(
            "/pulse/active"
        ).json()
        assert alpha["inhibition"] < 0.12 * 0.05


class TestHistoryIsASequence:
    @pytest.fixture
    def stream(self, tmp_path):
        m = tmp_path / "m.jsonl"
        base = datetime.now(timezone.utc)
        _write(m, [
            {"t": _iso(base - timedelta(seconds=900)), "type": "pulse",
             "engram": "e-old", "reason": "spontaneous"},
            {"t": _iso(base - timedelta(seconds=30)), "type": "pulse",
             "engram": "e-alpha", "reason": "spontaneous"},
            {"t": _iso(base - timedelta(seconds=29)), "type": "heartbeat",
             "active": 1, "total": 2},
            {"t": _iso(base - timedelta(seconds=28)), "type": "pulse",
             "engram": "e-beta", "reason": "propagation"},
            {"t": _iso(base - timedelta(seconds=27, milliseconds=500)),
             "type": "pulse", "engram": "e-beta", "reason": "propagation"},
            {"t": _iso(base), "type": "pulse",
             "engram": "e-alpha", "reason": "external"},
        ])
        return m

    def test_only_firings_in_the_contract_vocabulary(self, tmp_path, stream):
        body = _client(tmp_path, metrics=stream).get("/pulse/history").json()
        assert [e["kind"] for e in body] == [
            "spontaneous", "propagated", "propagated", "injected",
        ]
        assert set(body[0]) == {"engram_id", "t", "kind"}

    def test_order_ascending_and_gaps_preserved(self, tmp_path, stream):
        body = _client(tmp_path, metrics=stream).get("/pulse/history").json()
        times = [e["t"] for e in body]
        assert times == sorted(times)
        assert [e["engram_id"] for e in body] == [
            "e-alpha", "e-beta", "e-beta", "e-alpha",
        ]

    def test_repeat_firings_are_not_aggregated(self, tmp_path, stream):
        """Two firings 1.5s apart are two items — that gap is the STDP term."""
        body = _client(tmp_path, metrics=stream).get("/pulse/history").json()
        beta = [e for e in body if e["engram_id"] == "e-beta"]
        assert len(beta) == 2
        gap = (
            datetime.fromisoformat(beta[1]["t"])
            - datetime.fromisoformat(beta[0]["t"])
        ).total_seconds()
        assert gap == pytest.approx(0.5, abs=0.01)

    def test_window_trims_from_the_streams_own_clock(self, tmp_path, stream):
        c = _client(tmp_path, metrics=stream)
        assert "e-old" not in {e["engram_id"] for e in c.get(
            "/pulse/history?window=300").json()}
        assert "e-old" in {e["engram_id"] for e in c.get(
            "/pulse/history?window=1200").json()}
        assert "e-old" in {e["engram_id"] for e in c.get(
            "/pulse/history?window=0").json()}

    def test_events_arriving_out_of_order_are_still_sorted(self, tmp_path):
        m = tmp_path / "m.jsonl"
        base = datetime.now(timezone.utc)
        _write(m, [
            {"t": _iso(base), "type": "pulse", "engram": "b",
             "reason": "spontaneous"},
            {"t": _iso(base - timedelta(seconds=5)), "type": "pulse",
             "engram": "a", "reason": "spontaneous"},
        ])
        body = _client(tmp_path, metrics=m).get("/pulse/history").json()
        assert [e["engram_id"] for e in body] == ["a", "b"]

    def test_unknown_reason_survives_instead_of_vanishing(self, tmp_path):
        """A hole in an order is a false order; pass the name through."""
        m = tmp_path / "m.jsonl"
        _write(m, [{"t": _iso(datetime.now(timezone.utc)), "type": "pulse",
                    "engram": "e1", "reason": "something_new"}])
        [e] = _client(tmp_path, metrics=m).get("/pulse/history").json()
        assert e["kind"] == "something_new"

    def test_torn_trailing_line_never_becomes_a_firing(self, tmp_path):
        m = tmp_path / "m.jsonl"
        _write(m, [{"t": _iso(datetime.now(timezone.utc)), "type": "pulse",
                    "engram": "e1", "reason": "spontaneous"}])
        with m.open("a", encoding="utf-8") as f:
            f.write('{"t": "2026-07-25T00:00:00+00:00", "type": "pul')
        body = _client(tmp_path, metrics=m).get("/pulse/history").json()
        assert [e["engram_id"] for e in body] == ["e1"]

    def test_missing_metrics_file_is_an_empty_sequence(self, tmp_path):
        c = _client(tmp_path, metrics=tmp_path / "never-written.jsonl")
        assert c.get("/pulse/history").json() == []

    def test_large_history_is_a_bounded_explicit_replay_projection(self, tmp_path):
        m = tmp_path / "large.jsonl"
        base = datetime.now(timezone.utc)
        with m.open("w", encoding="utf-8") as f:
            for i in range(200):
                f.write(json.dumps({
                    "t": _iso(base + timedelta(seconds=i)),
                    "type": "pulse",
                    "engram": f"e-{i}",
                    "reason": "spontaneous",
                }) + "\n")
        response = _client(
            tmp_path,
            metrics=m,
            replay_bytes=512,
        ).get("/pulse/history?window=0")

        body = response.json()
        assert body
        assert len(body) < 200
        assert response.headers["X-Pulse-Replay-Truncated"] == "true"
        assert response.headers["X-Pulse-Replay-Window-Bytes"] == "512"
        assert response.headers["X-Pulse-Replay-Cursor"].startswith("v1-")


class TestTopology:
    def test_nodes_and_weighted_edges(self, tmp_path, populated):
        body = _client(tmp_path, db=populated).get("/pulse/topology").json()

        assert [n["engram_id"] for n in body["nodes"]] == ["e-alpha", "e-beta"]
        alpha = body["nodes"][0]
        assert alpha["name"] == "什么是心潮？ n/N 的比例"
        assert alpha["activity"] == 0.6123
        assert alpha["total_pulses"] == 3

        by_pair = {(e["from"], e["to"]): e for e in body["edges"]}
        assert by_pair[("e-alpha", "e-beta")]["weight"] == 0.4212
        assert by_pair[("e-alpha", "e-beta")]["type"] == "excitatory"
        assert by_pair[("e-beta", "e-alpha")]["type"] == "inhibitory"

    def test_edge_into_an_archived_engram_is_dropped(self, tmp_path, populated):
        """An edge must resolve to a node, as in the engine's own dump."""
        body = _client(tmp_path, db=populated).get("/pulse/topology").json()
        assert all("e-gone" not in (e["from"], e["to"]) for e in body["edges"])

    def test_nodes_join_active_on_the_same_key(self, tmp_path, populated):
        c = _client(tmp_path, db=populated)
        nodes = {n["engram_id"] for n in c.get("/pulse/topology").json()["nodes"]}
        assert nodes == {e["engram_id"] for e in c.get("/pulse/active").json()}


class TestConnectivity:
    def test_sideband_chain_is_exact_and_gate_unknown(self, tmp_path):
        edges = [
            ConnectivityEdge("a", "b", 0.8),
            ConnectivityEdge("b", "c", 0.7),
        ]
        db = _graph_db(tmp_path, ["a", "b", "c"], edges)

        response = _client(tmp_path, db=db).get("/pulse/connectivity")
        body = response.json()
        expected = analyze_connectivity(
            ["a", "b", "c"],
            edges,
            base_threshold=0.3,
            evidence_class="sideband_base_threshold_projection",
        )

        assert {key: body[key] for key in expected} == expected
        assert set(body) == set(expected) | {
            "projection_source", "observed_at", "age_seconds", "replay_complete",
        }
        assert body["projection_source"] == "sideband_fallback"
        assert body["minimum_gate_acceptance"] is None
        assert body["mean_gate_acceptance"] is None
        assert body["structural_regime"] == "connected_acyclic"
        assert body["weak_cut_vertex_count"] == 1
        assert body["age_seconds"] == 0.0
        assert body["replay_complete"] is True
        assert response.headers["X-Pulse-Replay-Complete"] == "true"
        assert response.headers["X-Pulse-Replay-Truncated"] == "false"
        assert response.headers["X-Pulse-Replay-Window-Bytes"] == str(1024 * 1024)
        assert response.headers["X-Pulse-Replay-Start-Offset"] == "0"
        assert response.headers["X-Pulse-Replay-End-Offset"] == "0"
        assert response.headers["X-Pulse-Replay-Reset"] == "missing"

    def test_matching_runtime_scc_projection_wins(self, tmp_path):
        edges = [
            ConnectivityEdge("a", "b", 0.8),
            ConnectivityEdge("b", "a", 0.8),
        ]
        db = _graph_db(tmp_path, ["a", "b", "c"], edges)
        metrics = tmp_path / "runtime.jsonl"
        observed = datetime.now(timezone.utc) - timedelta(seconds=2)
        event = _runtime_connectivity_event(
            ["a", "b", "c"],
            edges,
            at=observed,
            base_threshold=0.4,
            threshold_factors={"a": 0.5, "b": 1.5, "c": 1.0},
            target_gate_acceptance={"a": 0.8, "b": 0.4, "c": 1.0},
        )
        _write(metrics, [event])

        response = _client(
            tmp_path,
            metrics=metrics,
            db=db,
            propagation_threshold=0.4,
        ).get(
            "/pulse/connectivity"
        )
        body = response.json()

        assert {key: body[key] for key in event if key not in {"t", "type", "tick"}} == {
            key: value for key, value in event.items() if key not in {"t", "type", "tick"}
        }
        assert body["projection_source"] == "runtime_event"
        assert body["evidence_class"] == "runtime_effective_threshold_projection"
        assert body["structural_regime"] == "fragmented_reverberant"
        assert body["minimum_gate_acceptance"] == 0.4
        assert body["mean_gate_acceptance"] == 0.6
        assert body["observed_at"] == observed.isoformat()
        assert body["age_seconds"] >= 0.0
        assert response.headers["X-Pulse-Replay-Cursor"].startswith("v1-")

    @pytest.mark.parametrize(
        "fingerprint_field",
        ["node_fingerprint", "raw_topology_fingerprint"],
    )
    def test_wrong_run_fingerprint_falls_back(self, tmp_path, fingerprint_field):
        edges = [ConnectivityEdge("a", "b", 0.8)]
        db = _graph_db(tmp_path, ["a", "b"], edges)
        metrics = tmp_path / "wrong-run.jsonl"
        event = _runtime_connectivity_event(["a", "b"], edges)
        event[fingerprint_field] = "0" * 64
        _write(metrics, [event])

        body = _client(tmp_path, metrics=metrics, db=db).get(
            "/pulse/connectivity"
        ).json()
        assert body["projection_source"] == "sideband_fallback"
        assert body["evidence_class"] == "sideband_base_threshold_projection"
        assert body["minimum_gate_acceptance"] is None

    def test_runtime_event_with_stale_base_threshold_falls_back(self, tmp_path):
        edges = [ConnectivityEdge("a", "b", 0.5)]
        db = _graph_db(tmp_path, ["a", "b"], edges)
        metrics = tmp_path / "stale-threshold.jsonl"
        _write(metrics, [
            _runtime_connectivity_event(
                ["a", "b"],
                edges,
                base_threshold=0.3,
            )
        ])

        body = _client(
            tmp_path,
            metrics=metrics,
            db=db,
            propagation_threshold=0.7,
        ).get("/pulse/connectivity").json()

        assert body["projection_source"] == "sideband_fallback"
        assert body["evidence_class"] == "sideband_base_threshold_projection"
        assert body["base_threshold"] == 0.7
        assert body["effective_excitatory_edge_count"] == 0

    def test_topology_changed_after_event_falls_back(self, tmp_path):
        edges = [ConnectivityEdge("a", "b", 0.8)]
        db = _graph_db(tmp_path, ["a", "b"], edges)
        metrics = tmp_path / "stale.jsonl"
        _write(metrics, [_runtime_connectivity_event(["a", "b"], edges)])
        store = Storage(db)
        store.create_engram(engram_id="c")
        store.close()

        body = _client(tmp_path, metrics=metrics, db=db).get(
            "/pulse/connectivity"
        ).json()
        assert body["projection_source"] == "sideband_fallback"
        assert body["node_count"] == 3
        assert body["isolated_node_ids"] == ["c"]

    @pytest.mark.parametrize(
        ("field", "invalid"),
        [
            ("node_count", True),
            ("largest_weak_fraction", "1.0"),
            ("minimum_gate_acceptance", 2.0),
            ("schema_version", "pulse-connectivity.v0"),
            ("t", "not-a-time"),
        ],
    )
    def test_corrupt_runtime_event_is_ignored(self, tmp_path, field, invalid):
        edges = [ConnectivityEdge("a", "b", 0.8)]
        db = _graph_db(tmp_path, ["a", "b"], edges)
        metrics = tmp_path / f"corrupt-{field}.jsonl"
        event = _runtime_connectivity_event(["a", "b"], edges)
        event[field] = invalid
        _write(metrics, [event])

        response = _client(tmp_path, metrics=metrics, db=db).get(
            "/pulse/connectivity"
        )
        assert response.status_code == 200
        assert response.json()["projection_source"] == "sideband_fallback"

    def test_missing_canonical_field_is_ignored(self, tmp_path):
        edges = [ConnectivityEdge("a", "b", 0.8)]
        db = _graph_db(tmp_path, ["a", "b"], edges)
        metrics = tmp_path / "missing-field.jsonl"
        event = _runtime_connectivity_event(["a", "b"], edges)
        event.pop("largest_out_reach_size")
        _write(metrics, [event])

        body = _client(tmp_path, metrics=metrics, db=db).get(
            "/pulse/connectivity"
        ).json()
        assert body["projection_source"] == "sideband_fallback"

    def test_truncated_replay_without_visible_event_falls_back(self, tmp_path):
        edges = [ConnectivityEdge("a", "b", 0.8)]
        db = _graph_db(tmp_path, ["a", "b"], edges)
        metrics = tmp_path / "truncated.jsonl"
        events = [_runtime_connectivity_event(["a", "b"], edges)]
        events.extend(
            {"t": _iso(datetime.now(timezone.utc)), "type": "heartbeat", "pad": "x" * 120}
            for _ in range(20)
        )
        _write(metrics, events)

        response = _client(
            tmp_path, metrics=metrics, db=db, replay_bytes=512
        ).get("/pulse/connectivity")
        body = response.json()
        assert body["projection_source"] == "sideband_fallback"
        assert body["replay_complete"] is False
        assert response.headers["X-Pulse-Replay-Truncated"] == "true"
        assert response.headers["X-Pulse-Replay-Window-Bytes"] == "512"

    def test_mount_threshold_controls_sideband_projection(self, tmp_path):
        edges = [ConnectivityEdge("a", "b", 0.5)]
        db = _graph_db(tmp_path, ["a", "b"], edges)

        body = _client(
            tmp_path, db=db, propagation_threshold=0.7
        ).get("/pulse/connectivity").json()
        assert body["base_threshold"] == 0.7
        assert body["source_threshold_min"] == 0.7
        assert body["effective_excitatory_edge_count"] == 0
        assert body["structural_regime"] == "fragmented_acyclic"

    def test_create_app_uses_runtime_config_or_standalone_default(self, tmp_path):
        edges = [ConnectivityEdge("a", "b", 0.5)]
        db = _graph_db(tmp_path, ["a", "b"], edges)
        metrics = tmp_path / "app.jsonl"
        runtime = SimpleNamespace(
            config=SimpleNamespace(propagation_threshold=0.7),
            world_id="test-world",
        )

        live = TestClient(create_app(metrics, db_path=db, runtime=runtime))
        standalone = TestClient(create_app(metrics, db_path=db))
        assert live.get("/pulse/connectivity").json()["base_threshold"] == 0.7
        assert standalone.get("/pulse/connectivity").json()["base_threshold"] == 0.3

    def test_invalid_sqlite_edges_are_skipped_not_500(self, tmp_path):
        edges = [ConnectivityEdge("a", "b", 0.8)]
        db = _graph_db(tmp_path, ["a", "b"], edges)
        conn = sqlite3.connect(db)
        conn.execute(
            "UPDATE connections SET weight = 'not-finite', conn_type = 'unknown'"
        )
        conn.commit()
        conn.close()

        response = _client(tmp_path, db=db).get("/pulse/connectivity")
        assert response.status_code == 200
        assert response.json()["raw_edge_count"] == 0

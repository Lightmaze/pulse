"""Canonical structural facts for the threshold-eligible content graph."""

from __future__ import annotations

import math

import pytest

from pulse_system.core.connection import ConnectivityEdge, analyze_connectivity
from pulse_system.core.connection import viability
from pulse_system.core.types import ConnectionType


def _edge(
    source: str,
    target: str,
    weight: float = 0.5,
    conn_type: ConnectionType = ConnectionType.EXCITATORY,
) -> ConnectivityEdge:
    return ConnectivityEdge(source, target, weight, conn_type)


def _analyze(nodes, edges=(), **kwargs):
    return analyze_connectivity(
        nodes,
        edges,
        base_threshold=kwargs.pop("base_threshold", 0.3),
        evidence_class=kwargs.pop(
            "evidence_class", "runtime_effective_threshold_projection"
        ),
        **kwargs,
    )


def test_empty_and_singleton_are_defined_states() -> None:
    empty = _analyze([])
    assert empty["structural_regime"] == "empty"
    assert empty["weak_component_count"] == 0
    assert empty["largest_out_reach_fraction"] == 0.0
    assert empty["excitatory_edge_occupancy"] is None

    single = _analyze(["a"])
    assert single["structural_regime"] == "singleton"
    assert single["weak_component_count"] == 1
    assert single["strong_component_count"] == 1
    assert single["largest_out_reach_fraction"] == 1.0
    assert single["isolated_node_ids"] == ["a"]


def test_directed_chain_reports_reach_and_weak_cut_without_cycles() -> None:
    result = _analyze(
        ["c", "a", "b"],
        [_edge("a", "b"), _edge("b", "c")],
    )

    assert result["structural_regime"] == "connected_acyclic"
    assert result["weak_component_count"] == 1
    assert result["largest_weak_fraction"] == 1.0
    assert result["strong_component_count"] == 3
    assert result["largest_strong_fraction"] == pytest.approx(1 / 3, abs=1e-6)
    assert result["largest_out_reach_fraction"] == 1.0
    assert result["mean_out_reach_fraction"] == pytest.approx(2 / 3, abs=1e-6)
    assert result["source_only_node_count"] == 1
    assert result["sink_only_node_count"] == 1
    assert result["weak_cut_vertex_count"] == 1
    assert result["cycle_capable_node_count"] == 0
    assert result["excitatory_edge_occupancy"] == pytest.approx(2 / 6, abs=1e-6)


def test_local_scc_and_isolate_are_fragmented_reverberation_capacity() -> None:
    result = _analyze(
        ["a", "b", "c"],
        [_edge("a", "b"), _edge("b", "a")],
    )

    assert result["structural_regime"] == "fragmented_reverberant"
    assert result["weak_component_count"] == 2
    assert result["largest_weak_component_size"] == 2
    assert result["largest_strong_component_size"] == 2
    assert result["cycle_capable_node_count"] == 2
    assert result["isolated_node_ids"] == ["c"]
    assert result["observations"] == [
        "content_fragmented",
        "isolate_present",
        "cycle_capacity_present",
    ]


def test_full_directed_cycle_is_strongly_connected() -> None:
    result = _analyze(
        ["a", "b", "c"],
        [_edge("a", "b"), _edge("b", "c"), _edge("c", "a")],
    )
    assert result["structural_regime"] == "strongly_connected"
    assert result["largest_strong_fraction"] == 1.0
    assert result["cycle_capable_fraction"] == 1.0


def test_self_loop_is_cycle_capable_but_not_an_isolate() -> None:
    result = _analyze(["a"], [_edge("a", "a")])
    assert result["structural_regime"] == "singleton"
    assert result["effective_self_loop_count"] == 1
    assert result["cycle_capable_node_count"] == 1
    assert result["isolated_node_count"] == 0
    assert result["source_only_node_count"] == 0
    assert result["sink_only_node_count"] == 0


def test_inhibitory_edges_never_create_content_connectivity() -> None:
    result = _analyze(
        ["a", "b"],
        [_edge("a", "b", conn_type=ConnectionType.INHIBITORY)],
    )
    assert result["raw_edge_count"] == 1
    assert result["effective_inhibitory_edge_count"] == 1
    assert result["effective_excitatory_edge_count"] == 0
    assert result["weak_component_count"] == 2
    assert result["isolated_node_count"] == 2


def test_source_specific_threshold_changes_only_eligible_edges() -> None:
    result = _analyze(
        ["a", "b"],
        [_edge("a", "b", 0.5), _edge("b", "a", 0.5)],
        threshold_factors={"a": 2.0, "b": 0.5},
    )
    assert result["raw_edge_count"] == 2
    assert result["effective_excitatory_edge_count"] == 1
    assert result["source_threshold_min"] == 0.15
    assert result["source_threshold_max"] == 0.6
    assert result["source_threshold_mean"] == 0.375
    assert result["source_only_node_count"] == 1  # b -> a
    assert result["sink_only_node_count"] == 1


def test_gate_acceptance_is_summarized_but_does_not_delete_edges() -> None:
    result = _analyze(
        ["a", "b", "c"],
        [_edge("a", "b"), _edge("a", "c")],
        target_gate_acceptance={"a": 1.0, "b": 0.25, "c": 0.75},
    )
    assert result["effective_excitatory_edge_count"] == 2
    assert result["minimum_gate_acceptance"] == 0.25
    assert result["mean_gate_acceptance"] == 0.5
    assert result["largest_out_reach_fraction"] == 1.0

    fallback = _analyze(["a", "b"], [_edge("a", "b")])
    assert fallback["minimum_gate_acceptance"] is None
    assert fallback["mean_gate_acceptance"] is None


def test_unknown_endpoints_are_outside_the_active_induced_graph() -> None:
    result = _analyze(
        ["a", "b"],
        [_edge("a", "b"), _edge("a", "archived"), _edge("archived", "a")],
    )
    assert result["raw_edge_count"] == 1
    assert result["effective_excitatory_edge_count"] == 1


def test_isolate_ids_are_bounded_and_stably_sorted() -> None:
    nodes = [f"node-{index:02d}" for index in reversed(range(25))]
    result = _analyze(nodes)
    assert result["isolated_node_count"] == 25
    assert result["isolated_node_ids"] == sorted(nodes)[:20]
    assert result["isolated_node_ids_truncated"] is True


def test_fingerprints_and_metrics_are_input_order_independent() -> None:
    edges = [_edge("a", "b", 0.4), _edge("b", "a", 0.7)]
    left = _analyze(["b", "a"], edges)
    right = _analyze(["a", "b"], list(reversed(edges)))
    assert left == right
    assert len(left["node_fingerprint"]) == 64
    assert len(left["raw_topology_fingerprint"]) == 64


@pytest.mark.parametrize(
    ("nodes", "edges", "kwargs", "message"),
    [
        (["a", "a"], [], {}, "unique"),
        ([""], [], {}, "non-empty"),
        (["a"], [], {"base_threshold": math.nan}, "base_threshold"),
        (["a"], [], {"threshold_factors": {"a": 0.0}}, "factors"),
        (["a"], [], {"target_gate_acceptance": {"a": 1.1}}, "acceptance"),
        (["a"], [_edge("a", "a", math.inf)], {}, "weights"),
    ],
)
def test_invalid_graph_evidence_fails_fast(nodes, edges, kwargs, message) -> None:
    with pytest.raises(ValueError, match=message):
        _analyze(nodes, edges, **kwargs)


def test_square_lattice_reference_is_explicitly_non_applicable_and_non_causal(
    monkeypatch,
) -> None:
    before = _analyze(["a", "b"], [_edge("a", "b")])
    reference = before["percolation_reference"]
    assert reference == {
        "model": "iid_site_percolation_on_infinite_square_lattice",
        "critical_occupation_probability": 0.59274621,
        "applicable": False,
        "reason_code": "finite_directed_weighted_dynamic_graph",
    }

    monkeypatch.setattr(
        viability,
        "SQUARE_LATTICE_SITE_PERCOLATION_REFERENCE",
        0.01,
    )
    after = _analyze(["a", "b"], [_edge("a", "b")])
    assert after["structural_regime"] == before["structural_regime"]
    assert after["observations"] == before["observations"]
    assert after["largest_out_reach_fraction"] == before["largest_out_reach_fraction"]
    assert after["percolation_reference"]["critical_occupation_probability"] == 0.01

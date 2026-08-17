"""Read-only structural analysis of the Pulse content-propagation graph.

This module deliberately does not decide whether a graph is healthy.  It
projects the *threshold-eligible* excitatory graph and reports connectivity,
directed reach, recurrence capacity, and weak articulation points.  Actual
delivery can still be suppressed by target-side inhibition/gating, admission,
budgets, or Harness failure.

The often-cited 0.5927 value is the site-percolation threshold of an infinite
IID square lattice.  A Pulse graph is finite, directed, weighted, learned,
and dynamically thresholded, so that number is retained only as an explicit
non-applicable reference.  It is never read by a classification expression.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol

from pulse_system.core.types import ConnectionType


CONNECTIVITY_SCHEMA_VERSION = "pulse-connectivity.v1"
SQUARE_LATTICE_SITE_PERCOLATION_REFERENCE = 0.59274621
_MAX_EXPOSED_ISOLATE_IDS = 20


class ConnectionLike(Protocol):
    """Minimum edge surface shared by stored and sideband connections."""

    from_id: str
    to_id: str
    weight: float
    conn_type: ConnectionType


@dataclass(frozen=True, slots=True)
class ConnectivityEdge:
    """Content-free edge value used by read-only projections."""

    from_id: str
    to_id: str
    weight: float
    conn_type: ConnectionType = ConnectionType.EXCITATORY


def _ratio(numerator: int | float, denominator: int | float) -> float:
    if denominator == 0:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _fingerprint(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _weight_token(weight: float) -> str:
    """Stable full-precision token shared by Runtime and SQLite projections."""

    return format(weight, ".17g")


def _weak_components(
    nodes: tuple[str, ...],
    adjacency: Mapping[str, set[str]],
) -> list[set[str]]:
    unseen = set(nodes)
    components: list[set[str]] = []
    while unseen:
        start = min(unseen)
        unseen.remove(start)
        component = {start}
        stack = [start]
        while stack:
            current = stack.pop()
            for target in sorted(adjacency[current], reverse=True):
                if target in unseen:
                    unseen.remove(target)
                    component.add(target)
                    stack.append(target)
        components.append(component)
    return components


def _strong_components(
    nodes: tuple[str, ...],
    outgoing: Mapping[str, set[str]],
    incoming: Mapping[str, set[str]],
) -> list[set[str]]:
    """Iterative Kosaraju decomposition (safe beyond recursion limits)."""

    visited: set[str] = set()
    order: list[str] = []
    for start in nodes:
        if start in visited:
            continue
        stack: list[tuple[str, bool]] = [(start, False)]
        while stack:
            current, expanded = stack.pop()
            if expanded:
                order.append(current)
                continue
            if current in visited:
                continue
            visited.add(current)
            stack.append((current, True))
            for target in sorted(outgoing[current], reverse=True):
                if target not in visited:
                    stack.append((target, False))

    assigned: set[str] = set()
    components: list[set[str]] = []
    for start in reversed(order):
        if start in assigned:
            continue
        component: set[str] = set()
        stack = [start]
        assigned.add(start)
        while stack:
            current = stack.pop()
            component.add(current)
            for source in sorted(incoming[current], reverse=True):
                if source not in assigned:
                    assigned.add(source)
                    stack.append(source)
        components.append(component)
    return components


def _reachable(start: str, outgoing: Mapping[str, set[str]]) -> set[str]:
    reached = {start}
    stack = [start]
    while stack:
        current = stack.pop()
        for target in outgoing[current]:
            if target not in reached:
                reached.add(target)
                stack.append(target)
    return reached


def _weak_cut_vertices(
    nodes: tuple[str, ...],
    adjacency: Mapping[str, set[str]],
) -> set[str]:
    """Iterative Tarjan articulation points on the weak projection."""

    discovered: dict[str, int] = {}
    low: dict[str, int] = {}
    parent: dict[str, str | None] = {}
    children: dict[str, int] = {}
    cuts: set[str] = set()
    clock = 0

    for root in nodes:
        if root in discovered:
            continue
        parent[root] = None
        children[root] = 0
        discovered[root] = low[root] = clock
        clock += 1
        stack: list[tuple[str, object]] = [
            (root, iter(sorted(adjacency[root] - {root})))
        ]

        while stack:
            current, raw_iterator = stack[-1]
            iterator = raw_iterator  # retained as object for a compact stack type
            try:
                target = next(iterator)  # type: ignore[arg-type]
            except StopIteration:
                stack.pop()
                ancestor = parent[current]
                if ancestor is None:
                    if children[current] > 1:
                        cuts.add(current)
                    continue
                low[ancestor] = min(low[ancestor], low[current])
                if parent[ancestor] is not None and low[current] >= discovered[ancestor]:
                    cuts.add(ancestor)
                continue

            if target not in discovered:
                parent[target] = current
                children[target] = 0
                children[current] += 1
                discovered[target] = low[target] = clock
                clock += 1
                stack.append((target, iter(sorted(adjacency[target] - {target}))))
            elif target != parent[current]:
                low[current] = min(low[current], discovered[target])

    return cuts


def analyze_connectivity(
    node_ids: Iterable[str],
    connections: Iterable[ConnectionLike],
    *,
    base_threshold: float,
    threshold_factors: Mapping[str, float] | None = None,
    target_gate_acceptance: Mapping[str, float] | None = None,
    evidence_class: str,
) -> dict[str, object]:
    """Return the canonical ``pulse-connectivity.v1`` structural projection.

    ``target_gate_acceptance`` is descriptive evidence only.  It is summarized
    across threshold-eligible excitatory edges but never used to delete an edge
    or classify the graph; choosing a second arbitrary probability threshold
    would merely recreate the scalar-target error this projection avoids.
    """

    nodes = tuple(sorted(node_ids))
    if any(not isinstance(node_id, str) or not node_id for node_id in nodes):
        raise ValueError("node ids must be non-empty strings")
    if len(set(nodes)) != len(nodes):
        raise ValueError("node ids must be unique")
    if (
        isinstance(base_threshold, bool)
        or not isinstance(base_threshold, (int, float))
        or not math.isfinite(float(base_threshold))
        or float(base_threshold) < 0
    ):
        raise ValueError("base_threshold must be finite and non-negative")
    if not isinstance(evidence_class, str) or not evidence_class:
        raise ValueError("evidence_class must be a non-empty string")

    known = set(nodes)
    factors: dict[str, float] = {}
    thresholds: dict[str, float] = {}
    for node_id in nodes:
        factor = 1.0 if threshold_factors is None else threshold_factors.get(node_id, 1.0)
        if (
            isinstance(factor, bool)
            or not isinstance(factor, (int, float))
            or not math.isfinite(float(factor))
            or float(factor) <= 0
        ):
            raise ValueError("threshold factors must be finite and positive")
        factors[node_id] = float(factor)
        thresholds[node_id] = float(base_threshold) * float(factor)

    gate_acceptance: dict[str, float] | None = None
    if target_gate_acceptance is not None:
        gate_acceptance = {}
        for node_id in nodes:
            value = target_gate_acceptance.get(node_id, 1.0)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or not 0.0 <= float(value) <= 1.0
            ):
                raise ValueError("target gate acceptance must be within [0, 1]")
            gate_acceptance[node_id] = float(value)

    normalized_edges: list[tuple[str, str, float, ConnectionType]] = []
    for edge in connections:
        if edge.from_id not in known or edge.to_id not in known:
            continue
        weight = edge.weight
        if (
            isinstance(weight, bool)
            or not isinstance(weight, (int, float))
            or not math.isfinite(float(weight))
            or float(weight) < 0
        ):
            raise ValueError("connection weights must be finite and non-negative")
        try:
            conn_type = (
                edge.conn_type
                if isinstance(edge.conn_type, ConnectionType)
                else ConnectionType(edge.conn_type)
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("connection type must be excitatory or inhibitory") from exc
        normalized_edges.append(
            (edge.from_id, edge.to_id, float(weight), conn_type)
        )

    normalized_edges.sort(key=lambda item: (item[0], item[1], item[3].value, item[2]))
    outgoing = {node_id: set() for node_id in nodes}
    incoming = {node_id: set() for node_id in nodes}
    weak = {node_id: set() for node_id in nodes}
    effective_excitatory: list[tuple[str, str, float]] = []
    effective_inhibitory = 0
    for source, target, weight, conn_type in normalized_edges:
        if weight < thresholds[source]:
            continue
        if conn_type is ConnectionType.INHIBITORY:
            effective_inhibitory += 1
            continue
        effective_excitatory.append((source, target, weight))
        outgoing[source].add(target)
        incoming[target].add(source)
        weak[source].add(target)
        weak[target].add(source)

    weak_components = _weak_components(nodes, weak) if nodes else []
    strong_components = _strong_components(nodes, outgoing, incoming) if nodes else []
    largest_weak = max((len(component) for component in weak_components), default=0)
    largest_strong = max((len(component) for component in strong_components), default=0)

    self_loop_nodes = {
        source for source, target, _weight in effective_excitatory if source == target
    }
    cycle_nodes = set(self_loop_nodes)
    for component in strong_components:
        if len(component) > 1:
            cycle_nodes.update(component)

    reaches = [len(_reachable(node_id, outgoing)) for node_id in nodes]
    largest_reach = max(reaches, default=0)
    mean_reach_fraction = (
        round(sum(size / len(nodes) for size in reaches) / len(nodes), 6)
        if nodes
        else 0.0
    )

    isolated = sorted(
        node_id
        for node_id in nodes
        if not outgoing[node_id] and not incoming[node_id]
    )
    nonself_outgoing = {
        node_id: outgoing[node_id] - {node_id} for node_id in nodes
    }
    nonself_incoming = {
        node_id: incoming[node_id] - {node_id} for node_id in nodes
    }
    source_only = sum(
        bool(nonself_outgoing[node_id]) and not nonself_incoming[node_id]
        for node_id in nodes
    )
    sink_only = sum(
        bool(nonself_incoming[node_id]) and not nonself_outgoing[node_id]
        for node_id in nodes
    )
    cuts = _weak_cut_vertices(nodes, weak) if nodes else set()

    if not nodes:
        regime = "empty"
    elif len(nodes) == 1:
        regime = "singleton"
    elif len(weak_components) > 1:
        regime = (
            "fragmented_reverberant" if cycle_nodes else "fragmented_acyclic"
        )
    elif largest_strong == len(nodes):
        regime = "strongly_connected"
    elif cycle_nodes:
        regime = "connected_reverberant"
    else:
        regime = "connected_acyclic"

    observations: list[str] = []
    if len(weak_components) > 1:
        observations.append("content_fragmented")
    if isolated:
        observations.append("isolate_present")
    if cycle_nodes:
        observations.append("cycle_capacity_present")
    if cuts:
        observations.append("weak_cut_present")

    eligible_gate_values = (
        [gate_acceptance[target] for _source, target, _weight in effective_excitatory]
        if gate_acceptance is not None
        else []
    )
    minimum_gate_acceptance = (
        round(min(eligible_gate_values), 6) if eligible_gate_values else None
    )
    mean_gate_acceptance = (
        round(sum(eligible_gate_values) / len(eligible_gate_values), 6)
        if eligible_gate_values
        else None
    )

    raw_topology = [
        [source, target, conn_type.value, _weight_token(weight)]
        for source, target, weight, conn_type in normalized_edges
    ]
    threshold_values = list(thresholds.values())
    nonself_excitatory = sum(
        source != target for source, target, _weight in effective_excitatory
    )
    possible_nonself = len(nodes) * (len(nodes) - 1)

    return {
        "schema_version": CONNECTIVITY_SCHEMA_VERSION,
        "evidence_class": evidence_class,
        "node_count": len(nodes),
        "raw_edge_count": len(normalized_edges),
        "effective_excitatory_edge_count": len(effective_excitatory),
        "effective_inhibitory_edge_count": effective_inhibitory,
        "effective_self_loop_count": len(self_loop_nodes),
        "excitatory_edge_occupancy": (
            _ratio(nonself_excitatory, possible_nonself)
            if possible_nonself
            else None
        ),
        "base_threshold": round(float(base_threshold), 6),
        "source_threshold_min": (
            round(min(threshold_values), 6) if threshold_values else None
        ),
        "source_threshold_max": (
            round(max(threshold_values), 6) if threshold_values else None
        ),
        "source_threshold_mean": (
            round(sum(threshold_values) / len(threshold_values), 6)
            if threshold_values
            else None
        ),
        "weak_component_count": len(weak_components),
        "largest_weak_component_size": largest_weak,
        "largest_weak_fraction": _ratio(largest_weak, len(nodes)),
        "strong_component_count": len(strong_components),
        "largest_strong_component_size": largest_strong,
        "largest_strong_fraction": _ratio(largest_strong, len(nodes)),
        "largest_out_reach_size": largest_reach,
        "largest_out_reach_fraction": _ratio(largest_reach, len(nodes)),
        "mean_out_reach_fraction": mean_reach_fraction,
        "isolated_node_count": len(isolated),
        "isolated_node_ids": isolated[:_MAX_EXPOSED_ISOLATE_IDS],
        "isolated_node_ids_truncated": len(isolated) > _MAX_EXPOSED_ISOLATE_IDS,
        "source_only_node_count": source_only,
        "sink_only_node_count": sink_only,
        "cycle_capable_node_count": len(cycle_nodes),
        "cycle_capable_fraction": _ratio(len(cycle_nodes), len(nodes)),
        "weak_cut_vertex_count": len(cuts),
        "minimum_gate_acceptance": minimum_gate_acceptance,
        "mean_gate_acceptance": mean_gate_acceptance,
        "structural_regime": regime,
        "observations": observations,
        "node_fingerprint": _fingerprint(list(nodes)),
        "raw_topology_fingerprint": _fingerprint(raw_topology),
        "percolation_reference": {
            "model": "iid_site_percolation_on_infinite_square_lattice",
            "critical_occupation_probability": (
                SQUARE_LATTICE_SITE_PERCOLATION_REFERENCE
            ),
            "applicable": False,
            "reason_code": "finite_directed_weighted_dynamic_graph",
        },
    }

// @ts-nocheck

import assert from "node:assert/strict";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";

// pulse.ts shares browser runtime helpers; install only the inert location
// surface needed during module initialization, then import dynamically.
globalThis.window = {
  location: {
    origin: "http://pulse.test",
    href: "http://pulse.test/",
    hash: "",
    search: "",
  },
};
globalThis.document = { documentElement: { lang: "en" }, hidden: false };
const vite = await createServer({
  root: fileURLToPath(new URL("../", import.meta.url)),
  logLevel: "silent",
  server: { middlewareMode: true },
  appType: "custom",
});
const { parseConnectivity, parseScheduling } = await vite.ssrLoadModule("/src/pulse.ts");
await vite.close();

function runtimeConnectivityBody(overrides: Record<string, unknown> = {}) {
  return {
    schema_version: "pulse-connectivity.v1",
    evidence_class: "runtime_effective_threshold_projection",
    node_count: 4,
    raw_edge_count: 5,
    effective_excitatory_edge_count: 3,
    effective_inhibitory_edge_count: 2,
    effective_self_loop_count: 0,
    excitatory_edge_occupancy: 0.25,
    base_threshold: 0.3,
    source_threshold_min: 0.24,
    source_threshold_max: 0.39,
    source_threshold_mean: 0.31,
    weak_component_count: 2,
    largest_weak_component_size: 3,
    largest_weak_fraction: 0.75,
    strong_component_count: 2,
    largest_strong_component_size: 3,
    largest_strong_fraction: 0.75,
    largest_out_reach_size: 3,
    largest_out_reach_fraction: 0.75,
    mean_out_reach_fraction: 0.625,
    isolated_node_count: 1,
    isolated_node_ids: ["engram-4"],
    isolated_node_ids_truncated: false,
    source_only_node_count: 0,
    sink_only_node_count: 0,
    cycle_capable_node_count: 3,
    cycle_capable_fraction: 0.75,
    weak_cut_vertex_count: 0,
    minimum_gate_acceptance: 0.62,
    mean_gate_acceptance: 0.78,
    structural_regime: "fragmented_reverberant",
    observations: [
      "content_fragmented",
      "isolate_present",
      "cycle_capacity_present",
    ],
    node_fingerprint: "a".repeat(64),
    raw_topology_fingerprint: "b".repeat(64),
    percolation_reference: {
      model: "iid_site_percolation_on_infinite_square_lattice",
      critical_occupation_probability: 0.59274621,
      applicable: false,
      reason_code: "finite_directed_weighted_dynamic_graph",
    },
    projection_source: "runtime_event",
    observed_at: "2026-08-05T12:00:00Z",
    age_seconds: 2.5,
    replay_complete: true,
    ...overrides,
  };
}

function runtimeSchedulingBody(capacityOverrides: Record<string, unknown> = {}) {
  return {
    policy_version: "pulse-admission.v1",
    lease: {
      scope: "world:world-1",
      owner_id: "runtime-owner-1",
      epoch: 3,
      state: "active",
      healthy: true,
      acquired_at: "2026-08-06T12:00:00Z",
      renewed_at: "2026-08-06T12:00:05Z",
      expires_at: "2026-08-06T12:00:35Z",
      released_at: null,
      lost_reason: null,
    },
    capacity: {
      budget_per_tick: 8,
      lane_reservation_per_tick: 1,
      starvation_boost: 0.2,
      starvation_debt_cap: 8,
      held: 2,
      background_dispatch: true,
      worker_limit: 8,
      worker_running: 2,
      worker_available: 6,
      resident_limit: 12,
      resident_sessions: 4,
      starting_sessions: 1,
      busy_sessions: 2,
      ...capacityOverrides,
    },
    failure_domains: {
      policy_version: "engram-failure-domain.v1",
      evidence_class: "runtime_memory_projection",
      limit: 64,
      total: 0,
      cooling: 0,
      degraded: 0,
      probe_ready: 0,
      truncated: false,
      items: [],
    },
    lanes: [],
    centers: [],
    reservations: [],
  };
}

test("parseConnectivity accepts runtime-effective evidence", () => {
  const parsed = parseConnectivity(runtimeConnectivityBody());

  assert.equal(parsed.evidence_class, "runtime_effective_threshold_projection");
  assert.equal(parsed.projection_source, "runtime_event");
  assert.equal(parsed.structural_regime, "fragmented_reverberant");
  assert.equal(parsed.minimum_gate_acceptance, 0.62);
  assert.deepEqual(parsed.isolated_node_ids, ["engram-4"]);
});

test("parseConnectivity accepts an explicit base-threshold fallback", () => {
  const parsed = parseConnectivity(runtimeConnectivityBody({
    evidence_class: "sideband_base_threshold_projection",
    source_threshold_min: 0.3,
    source_threshold_max: 0.3,
    source_threshold_mean: 0.3,
    minimum_gate_acceptance: null,
    mean_gate_acceptance: null,
    projection_source: "sideband_fallback",
    observed_at: "2026-08-05T12:00:03+00:00",
    age_seconds: 0,
  }));

  assert.equal(parsed.evidence_class, "sideband_base_threshold_projection");
  assert.equal(parsed.projection_source, "sideband_fallback");
  assert.equal(parsed.minimum_gate_acceptance, null);
  assert.equal(parsed.source_threshold_mean, parsed.base_threshold);
});

test("parseConnectivity rejects the wrong schema version", () => {
  assert.throws(
    () => parseConnectivity(runtimeConnectivityBody({ schema_version: "pulse-connectivity.v2" })),
    /schema_version/,
  );
});

test("parseConnectivity rejects invalid field types instead of drawing them", () => {
  assert.throws(
    () => parseConnectivity(runtimeConnectivityBody({ largest_out_reach_fraction: "75%" })),
    /largest_out_reach_fraction/,
  );
});

test("parseScheduling accepts succession execution-domain capacity", () => {
  const parsed = parseScheduling(runtimeSchedulingBody({
    succession_worker_limit: 4,
    succession_workers_running: 2,
    succession_subjects_pending: 3,
    succession_subjects_blocked: 1,
  }));

  assert.equal(parsed.capacity.succession_worker_limit, 4);
  assert.equal(parsed.capacity.succession_workers_running, 2);
  assert.equal(parsed.capacity.succession_subjects_pending, 3);
  assert.equal(parsed.capacity.succession_subjects_blocked, 1);
});

test("parseScheduling defaults absent succession capacity to zero", () => {
  const parsed = parseScheduling(runtimeSchedulingBody());

  assert.equal(parsed.capacity.succession_worker_limit, 0);
  assert.equal(parsed.capacity.succession_workers_running, 0);
  assert.equal(parsed.capacity.succession_subjects_pending, 0);
  assert.equal(parsed.capacity.succession_subjects_blocked, 0);
});

test("parseScheduling rejects invalid succession capacity when supplied", () => {
  for (const key of [
    "succession_worker_limit",
    "succession_workers_running",
    "succession_subjects_pending",
    "succession_subjects_blocked",
  ]) {
    assert.throws(
      () => parseScheduling(runtimeSchedulingBody({ [key]: -1 })),
      new RegExp(key),
    );
  }
  assert.throws(
    () => parseScheduling(runtimeSchedulingBody({ succession_workers_running: 1.5 })),
    /succession_workers_running/,
  );
});

// Right-rail runtime client — docs/api-contract-v0.1.md §2.2, §2.3, §3, §4.
//
// Scope is fixed by §0: everything in this module is 调律 (rhythm) or 委派
// (routing). Nothing here writes text into an engram's context. That path is
// POST /engrams/{id}/inject and it belongs to the centre pane, on purpose.
//
// Field tolerance follows the house convention in types.ts: beyond the couple
// of keys a record cannot exist without, every field is optional at parse time,
// because the contract is being implemented on the other side right now.

import { useCallback, useEffect, useRef, useState } from "react";
import { apiFetch } from "./apiSecurity";
import { currentLocale, translate } from "./i18n";
import { subscribeLiveStream, type LiveFrame } from "./liveStream";
import { apiBase, useViewer } from "./store";

// ── shapes ───────────────────────────────────────────────────────────────

export interface ActiveEngram {
  engram_id: string;
  name: string | null;
  name_origin: "auto" | "user" | string;
  nickname: string | null;
  firing: boolean;
  inhibition: number | null;
  gate: number | null;
  last_fired_at: string | null;
}

/** Contract §3 kinds; anything else falls through to the neutral colour. */
export type FiringKind = "spontaneous" | "propagated" | "injected" | string;

export interface FiringMark {
  engramId: string;
  tMs: number;
  kind: FiringKind;
}

export type KnobName = "activity" | "wait" | "propagation_threshold" | "gate";

export const KNOB_NAMES: KnobName[] = [
  "activity",
  "wait",
  "propagation_threshold",
  "gate",
];

/** null on a knob = 交还屏状体 — autonomy is per knob, not one global switch. */
export type KnobValues = Record<KnobName, number | null>;

export interface TuningSnapshot {
  commanded: KnobValues;
  observed: KnobValues;
  applied_at_tick: number | null;
}

export interface RoutingCandidate {
  engram_id: string;
  score: number | null;
}

export interface DelegationRow {
  id: string;
  task: string;
  to: string | null;
  backend: string | null;
  status: string | null;
  created_at: string | null;
  result: string | null;
  chosen: string | null;
  temperature: number | null;
  candidates: RoutingCandidate[];
}

export interface EngramIdentity {
  signature: string;
  name: string | null;
  name_origin: "auto" | "user" | string;
  nickname: string | null;
}

export interface SchedulingLease {
  scope: string;
  owner_id: string;
  epoch: number;
  state: string;
  healthy: boolean;
  acquired_at: string;
  renewed_at: string;
  expires_at: string;
  released_at: string | null;
  lost_reason: string | null;
}

export interface SchedulingCapacity {
  budget_per_tick: number;
  lane_reservation_per_tick: number;
  starvation_boost: number;
  starvation_debt_cap: number;
  held: number;
  background_dispatch: boolean;
  worker_limit: number;
  worker_running: number;
  worker_available: number;
  resident_limit: number;
  resident_sessions: number;
  starting_sessions: number;
  busy_sessions: number;
  succession_worker_limit: number;
  succession_workers_running: number;
  succession_subjects_pending: number;
  succession_subjects_blocked: number;
}

export interface SchedulingLane {
  lane: string;
  waiting_centers: number;
  max_debt: number;
  last_admitted_at: string | null;
}

export interface SchedulingCenter {
  center_id: string;
  lane: string;
  status: string;
  decision: string;
  reason: string;
  starvation_debt: number;
  waiting_since: string | null;
  last_admitted_at: string | null;
  last_decision_at: string;
  updated_at: string;
}

export interface SchedulingReservation {
  id: string;
  world_id: string;
  event_id: string;
  engram_id: string;
  center_id: string | null;
  lane: string;
  owner_id: string;
  lease_epoch: number;
  state: string;
  outcome: string | null;
  reason: string;
  base_priority: number;
  effective_score: number;
  created_at: string;
  settled_at: string | null;
}

export const ENGRAM_FAILURE_DOMAIN_POLICY_VERSION = "engram-failure-domain.v1" as const;
export const ENGRAM_FAILURE_DOMAIN_EVIDENCE_CLASS = "runtime_memory_projection" as const;
export const ENGRAM_FAILURE_DOMAIN_ITEM_LIMIT = 64 as const;

export type EngramFailureDomainState = "cooling" | "degraded" | "probe_ready";

export interface EngramFailureDomainItem {
  engram_id: string;
  state: EngramFailureDomainState;
  consecutive_failures: number;
  last_failure_at: string;
  retry_at: string | null;
  last_error_code: string;
  last_error_phase: string | null;
  error_retryable: boolean;
  prompt_accepted: boolean | null;
}

export interface EngramFailureDomainSnapshot {
  policy_version: typeof ENGRAM_FAILURE_DOMAIN_POLICY_VERSION;
  evidence_class: typeof ENGRAM_FAILURE_DOMAIN_EVIDENCE_CLASS;
  limit: typeof ENGRAM_FAILURE_DOMAIN_ITEM_LIMIT;
  total: number;
  cooling: number;
  degraded: number;
  probe_ready: number;
  truncated: boolean;
  items: EngramFailureDomainItem[];
}

export interface SchedulingSnapshot {
  policy_version: string;
  lease: SchedulingLease;
  capacity: SchedulingCapacity;
  failure_domains: EngramFailureDomainSnapshot;
  lanes: SchedulingLane[];
  centers: SchedulingCenter[];
  reservations: SchedulingReservation[];
}

export const CONNECTIVITY_SCHEMA_VERSION = "pulse-connectivity.v1" as const;
export const SQUARE_LATTICE_SITE_CRITICAL_PROBABILITY = 0.59274621;

export type ConnectivityEvidenceClass =
  | "runtime_effective_threshold_projection"
  | "sideband_base_threshold_projection";

export type ConnectivityProjectionSource = "runtime_event" | "sideband_fallback";

export type ConnectivityStructuralRegime =
  | "empty"
  | "singleton"
  | "fragmented_acyclic"
  | "fragmented_reverberant"
  | "strongly_connected"
  | "connected_acyclic"
  | "connected_reverberant";

export type ConnectivityObservation =
  | "content_fragmented"
  | "isolate_present"
  | "cycle_capacity_present"
  | "weak_cut_present";

export interface PercolationReference {
  model: string;
  critical_occupation_probability: number;
  applicable: false;
  reason_code: string;
}

export interface ConnectivitySnapshot {
  schema_version: typeof CONNECTIVITY_SCHEMA_VERSION;
  evidence_class: ConnectivityEvidenceClass;
  node_count: number;
  raw_edge_count: number;
  effective_excitatory_edge_count: number;
  effective_inhibitory_edge_count: number;
  effective_self_loop_count: number;
  excitatory_edge_occupancy: number | null;
  base_threshold: number;
  source_threshold_min: number | null;
  source_threshold_max: number | null;
  source_threshold_mean: number | null;
  weak_component_count: number;
  largest_weak_component_size: number;
  largest_weak_fraction: number;
  strong_component_count: number;
  largest_strong_component_size: number;
  largest_strong_fraction: number;
  largest_out_reach_size: number;
  largest_out_reach_fraction: number;
  mean_out_reach_fraction: number;
  isolated_node_count: number;
  isolated_node_ids: string[];
  isolated_node_ids_truncated: boolean;
  source_only_node_count: number;
  sink_only_node_count: number;
  cycle_capable_node_count: number;
  cycle_capable_fraction: number;
  weak_cut_vertex_count: number;
  minimum_gate_acceptance: number | null;
  mean_gate_acceptance: number | null;
  structural_regime: ConnectivityStructuralRegime;
  observations: ConnectivityObservation[];
  node_fingerprint: string;
  raw_topology_fingerprint: string;
  percolation_reference: PercolationReference;
  projection_source: ConnectivityProjectionSource;
  observed_at: string | null;
  age_seconds: number | null;
  replay_complete: boolean;
}

// ── faults ───────────────────────────────────────────────────────────────

/**
 * `absent` separates "the runtime is up but has no such route yet" (404/501)
 * from "I cannot reach the runtime at all". The rail renders those two
 * differently because they are different problems with different remedies.
 */
export class RuntimeFault extends Error {
  readonly remedy: string | null;
  readonly absent: boolean;

  constructor(message: string, remedy: string | null, absent: boolean) {
    super(message);
    this.name = "RuntimeFault";
    this.remedy = remedy;
    this.absent = absent;
  }
}

async function readFault(r: Response): Promise<RuntimeFault> {
  // Contract §6: a refusal without a remedy is only half a refusal.
  const body = await r.json().catch(() => null);
  const rec = typeof body === "object" && body !== null ? (body as Record<string, unknown>) : {};
  const detail =
    str(rec.detail) ?? str(rec.error) ?? `HTTP ${r.status} ${r.statusText}`.trim();
  const absent = r.status === 404 || r.status === 501;
  return new RuntimeFault(
    detail,
    str(rec.remedy) ??
      (absent ? translate(currentLocale(), "runtime.endpointAbsent") : null),
    absent,
  );
}

function unreachableFault(): RuntimeFault {
  const locale = currentLocale();
  return new RuntimeFault(
    translate(locale, "runtime.unreachable"),
    translate(locale, "runtime.startRemedy"),
    false,
  );
}

export async function getJson(url: string, signal: AbortSignal): Promise<unknown> {
  let r: Response;
  try {
    r = await apiFetch(url, { signal });
  } catch (e) {
    if (e instanceof Error && e.name === "AbortError") throw e;
    throw unreachableFault();
  }
  if (!r.ok) throw await readFault(r);
  return r.json();
}

async function writeJson(url: string, method: "POST" | "PATCH", body: unknown): Promise<unknown> {
  let r: Response;
  try {
    r = await apiFetch(url, {
      method,
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch {
    throw unreachableFault();
  }
  if (!r.ok) throw await readFault(r);
  return r.json().catch(() => ({}));
}

export async function postJson(url: string, body: unknown): Promise<unknown> {
  return writeJson(url, "POST", body);
}

export async function patchJson(url: string, body: unknown): Promise<unknown> {
  return writeJson(url, "PATCH", body);
}

// ── tolerant parsing ─────────────────────────────────────────────────────

export function num(v: unknown): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

export function str(v: unknown): string | null {
  return typeof v === "string" && v !== "" ? v : null;
}

export function rec(v: unknown): Record<string, unknown> {
  return typeof v === "object" && v !== null ? (v as Record<string, unknown>) : {};
}

/** The contract shows bare arrays; the existing server wraps. Accept both. */
function listOf(body: unknown, ...keys: string[]): Record<string, unknown>[] {
  const raw = Array.isArray(body)
    ? body
    : keys.map((k) => rec(body)[k]).find((v) => Array.isArray(v));
  return Array.isArray(raw) ? raw.map(rec) : [];
}

export function parseActive(body: unknown): ActiveEngram[] {
  return listOf(body, "active", "engrams", "pulse")
    .map((r) => ({
      engram_id: str(r.engram_id) ?? str(r.id) ?? "",
      name: str(r.name) ?? str(r.title),
      name_origin: str(r.name_origin) ?? "auto",
      nickname: str(r.nickname),
      firing: r.firing === true,
      inhibition: num(r.inhibition),
      gate: num(r.gate),
      last_fired_at: str(r.last_fired_at),
    }))
    .filter((e) => e.engram_id !== "");
}

export function parseHistory(body: unknown): FiringMark[] {
  return listOf(body, "history", "pulses", "events")
    .map((r) => ({
      engramId: str(r.engram_id) ?? str(r.id) ?? "",
      tMs: Date.parse(str(r.t) ?? str(r.timestamp) ?? ""),
      kind: str(r.kind) ?? str(r.reason) ?? "unknown",
    }))
    .filter((m) => m.engramId !== "" && Number.isFinite(m.tMs))
    .sort((a, b) => a.tMs - b.tMs);
}

function parseKnobs(v: unknown): KnobValues {
  const r = rec(v);
  return {
    activity: num(r.activity),
    wait: num(r.wait),
    propagation_threshold: num(r.propagation_threshold),
    gate: num(r.gate),
  };
}

export function parseTuning(body: unknown): TuningSnapshot {
  const r = rec(body);
  return {
    commanded: parseKnobs(r.commanded),
    observed: parseKnobs(r.observed),
    applied_at_tick: num(r.applied_at_tick),
  };
}

export function parseDelegations(body: unknown): DelegationRow[] {
  return listOf(body, "delegations", "records")
    .map((r) => {
      // The public runtime returns `route` with a score map; pre-contract
      // builds used `routing|decision` with a candidates array. Keep the
      // tolerant reader, but prefer the shipped shape.
      const routing = rec(r.route ?? r.routing ?? r.decision);
      const scoreMap = rec(routing.scores);
      const scoreCandidates = Object.entries(scoreMap)
        .filter((entry): entry is [string, number] => typeof entry[1] === "number")
        .map(([engram_id, score]) => ({ engram_id, score }));
      const listedCandidates = (Array.isArray(routing.candidates) ? routing.candidates : [])
        .map(rec)
        .map((c) => ({
          engram_id: str(c.engram_id) ?? str(c.id) ?? "",
          score: num(c.score),
        }))
        .filter((c) => c.engram_id !== "");
      return {
        id: str(r.id) ?? str(r.delegation_id) ?? "",
        task: str(r.task) ?? "",
        to: str(r.to) ?? str(r.target_id) ?? str(r.target) ?? null,
        backend: str(r.backend),
        status: str(r.status),
        created_at: str(r.created_at) ?? str(r.t),
        result: str(r.result),
        chosen:
          str(routing.chosen) ??
          str(r.chosen) ??
          str(r.target_id) ??
          str(r.to) ??
          null,
        temperature: num(routing.temperature) ?? num(r.temperature),
        candidates:
          scoreCandidates.length > 0 ? scoreCandidates : listedCandidates,
      };
    })
    .filter((d) => d.id !== "");
}

export function parseHealth(body: unknown): { ok: boolean; version: string | null } {
  const r = rec(body);
  return { ok: r.ok !== false, version: str(r.version) };
}

// Connectivity validates the frozen wire contract; graph analysis stays in the backend.
const CONNECTIVITY_EVIDENCE_CLASSES = [
  "runtime_effective_threshold_projection",
  "sideband_base_threshold_projection",
] as const;
const CONNECTIVITY_PROJECTION_SOURCES = ["runtime_event", "sideband_fallback"] as const;
const CONNECTIVITY_REGIMES = [
  "empty", "singleton", "fragmented_acyclic", "fragmented_reverberant",
  "strongly_connected", "connected_acyclic", "connected_reverberant",
] as const;
const CONNECTIVITY_OBSERVATIONS = [
  "content_fragmented", "isolate_present", "cycle_capacity_present", "weak_cut_present",
] as const;

function connectivityError(path: string, expected: string): never {
  throw new Error(`Invalid /pulse/connectivity payload: ${path} must be ${expected}`);
}

function connectivityRecord(value: unknown, path: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return connectivityError(path, "an object");
  }
  return value as Record<string, unknown>;
}

function connectivityField(row: Record<string, unknown>, key: string, path = "$"): unknown {
  if (!Object.hasOwn(row, key)) return connectivityError(`${path}.${key}`, "present");
  return row[key];
}

function connectivityText(row: Record<string, unknown>, key: string, path = "$"): string {
  const value = connectivityField(row, key, path);
  if (typeof value !== "string" || value === "") {
    return connectivityError(`${path}.${key}`, "a non-empty string");
  }
  return value;
}

function connectivityFinite(
  row: Record<string, unknown>,
  key: string,
  rule: { integer?: boolean; min?: number; max?: number } = {},
  path = "$",
): number {
  const value = connectivityField(row, key, path);
  if (
    typeof value !== "number" || !Number.isFinite(value) ||
    (rule.integer === true && !Number.isInteger(value)) ||
    (rule.min !== undefined && value < rule.min) ||
    (rule.max !== undefined && value > rule.max)
  ) return connectivityError(`${path}.${key}`, "a finite number in range");
  return value;
}

function connectivityNullableFinite(
  row: Record<string, unknown>,
  key: string,
  rule: { min?: number; max?: number } = {},
): number | null {
  return connectivityField(row, key) === null ? null : connectivityFinite(row, key, rule);
}

function connectivityBoolean(row: Record<string, unknown>, key: string, path = "$"): boolean {
  const value = connectivityField(row, key, path);
  if (typeof value !== "boolean") return connectivityError(`${path}.${key}`, "a boolean");
  return value;
}

function connectivityChoice<T extends string>(
  row: Record<string, unknown>,
  key: string,
  allowed: readonly T[],
): T {
  const value = connectivityField(row, key);
  if (typeof value !== "string" || !allowed.includes(value as T)) {
    return connectivityError(`$.${key}`, `one of ${allowed.join(", ")}`);
  }
  return value as T;
}

function connectivityStringList(row: Record<string, unknown>, key: string): string[] {
  const value = connectivityField(row, key);
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string" || item === "")) {
    return connectivityError(`$.${key}`, "an array of non-empty strings");
  }
  return value as string[];
}

export function parseConnectivity(body: unknown): ConnectivitySnapshot {
  const root = connectivityRecord(body, "$");
  const count = (key: string) => connectivityFinite(root, key, { integer: true, min: 0 });
  const fraction = (key: string) => connectivityFinite(root, key, { min: 0, max: 1 });
  const nullableFraction = (key: string) =>
    connectivityNullableFinite(root, key, { min: 0, max: 1 });
  const reference = connectivityRecord(
    connectivityField(root, "percolation_reference"),
    "$.percolation_reference",
  );
  const referenceProbability = connectivityFinite(
    reference, "critical_occupation_probability", { min: 0, max: 1 },
    "$.percolation_reference",
  );
  if (
    referenceProbability !== SQUARE_LATTICE_SITE_CRITICAL_PROBABILITY ||
    connectivityBoolean(reference, "applicable", "$.percolation_reference")
  ) return connectivityError("$.percolation_reference", "the fixed non-applicable reference");

  const observations = connectivityStringList(root, "observations").map((value, index) => {
    if (!CONNECTIVITY_OBSERVATIONS.includes(value as ConnectivityObservation)) {
      return connectivityError(`$.observations[${index}]`, "a canonical observation");
    }
    return value as ConnectivityObservation;
  });
  const observedAt = connectivityField(root, "observed_at");
  if (
    observedAt !== null &&
    (typeof observedAt !== "string" || !Number.isFinite(Date.parse(observedAt)) ||
      !/(?:Z|[+-]00:00)$/i.test(observedAt))
  ) return connectivityError("$.observed_at", "an ISO-8601 UTC timestamp or null");
  const nodeFingerprint = connectivityText(root, "node_fingerprint");
  const topologyFingerprint = connectivityText(root, "raw_topology_fingerprint");
  if (!/^[0-9a-f]{64}$/i.test(nodeFingerprint) || !/^[0-9a-f]{64}$/i.test(topologyFingerprint)) {
    return connectivityError("$", "64-character hexadecimal fingerprints");
  }

  const snapshot: ConnectivitySnapshot = {
    schema_version: connectivityChoice(root, "schema_version", [CONNECTIVITY_SCHEMA_VERSION]),
    evidence_class: connectivityChoice(root, "evidence_class", CONNECTIVITY_EVIDENCE_CLASSES),
    node_count: count("node_count"),
    raw_edge_count: count("raw_edge_count"),
    effective_excitatory_edge_count: count("effective_excitatory_edge_count"),
    effective_inhibitory_edge_count: count("effective_inhibitory_edge_count"),
    effective_self_loop_count: count("effective_self_loop_count"),
    excitatory_edge_occupancy: nullableFraction("excitatory_edge_occupancy"),
    base_threshold: connectivityFinite(root, "base_threshold", { min: 0 }),
    source_threshold_min: connectivityNullableFinite(root, "source_threshold_min"),
    source_threshold_max: connectivityNullableFinite(root, "source_threshold_max"),
    source_threshold_mean: connectivityNullableFinite(root, "source_threshold_mean"),
    weak_component_count: count("weak_component_count"),
    largest_weak_component_size: count("largest_weak_component_size"),
    largest_weak_fraction: fraction("largest_weak_fraction"),
    strong_component_count: count("strong_component_count"),
    largest_strong_component_size: count("largest_strong_component_size"),
    largest_strong_fraction: fraction("largest_strong_fraction"),
    largest_out_reach_size: count("largest_out_reach_size"),
    largest_out_reach_fraction: fraction("largest_out_reach_fraction"),
    mean_out_reach_fraction: fraction("mean_out_reach_fraction"),
    isolated_node_count: count("isolated_node_count"),
    isolated_node_ids: connectivityStringList(root, "isolated_node_ids"),
    isolated_node_ids_truncated: connectivityBoolean(root, "isolated_node_ids_truncated"),
    source_only_node_count: count("source_only_node_count"),
    sink_only_node_count: count("sink_only_node_count"),
    cycle_capable_node_count: count("cycle_capable_node_count"),
    cycle_capable_fraction: fraction("cycle_capable_fraction"),
    weak_cut_vertex_count: count("weak_cut_vertex_count"),
    minimum_gate_acceptance: nullableFraction("minimum_gate_acceptance"),
    mean_gate_acceptance: nullableFraction("mean_gate_acceptance"),
    structural_regime: connectivityChoice(root, "structural_regime", CONNECTIVITY_REGIMES),
    observations,
    node_fingerprint: nodeFingerprint,
    raw_topology_fingerprint: topologyFingerprint,
    percolation_reference: {
      model: connectivityText(reference, "model", "$.percolation_reference"),
      critical_occupation_probability: referenceProbability,
      applicable: false,
      reason_code: connectivityText(reference, "reason_code", "$.percolation_reference"),
    },
    projection_source: connectivityChoice(
      root, "projection_source", CONNECTIVITY_PROJECTION_SOURCES,
    ),
    observed_at: observedAt as string | null,
    age_seconds: connectivityNullableFinite(root, "age_seconds", { min: 0 }),
    replay_complete: connectivityBoolean(root, "replay_complete"),
  };

  const thresholds = [
    snapshot.source_threshold_min, snapshot.source_threshold_max, snapshot.source_threshold_mean,
  ];
  if (
    (snapshot.minimum_gate_acceptance === null) !==
      (snapshot.mean_gate_acceptance === null) ||
    (snapshot.minimum_gate_acceptance !== null &&
      snapshot.minimum_gate_acceptance > (snapshot.mean_gate_acceptance as number)) ||
    (snapshot.node_count < 2) !== (snapshot.excitatory_edge_occupancy === null) ||
    (snapshot.node_count === 0
      ? !thresholds.every((value) => value === null)
      : !thresholds.every((value) => value !== null) ||
        (snapshot.source_threshold_min as number) > (snapshot.source_threshold_mean as number) ||
        (snapshot.source_threshold_mean as number) > (snapshot.source_threshold_max as number))
  ) return connectivityError("$", "internally consistent nullable evidence fields");

  const runtimeEvidence =
    snapshot.evidence_class === "runtime_effective_threshold_projection";
  if (
    (runtimeEvidence && snapshot.projection_source !== "runtime_event") ||
    (!runtimeEvidence &&
      (snapshot.projection_source !== "sideband_fallback" ||
        snapshot.minimum_gate_acceptance !== null ||
        snapshot.mean_gate_acceptance !== null ||
        (snapshot.node_count > 0 &&
          !thresholds.every((value) => value === snapshot.base_threshold))))
  ) return connectivityError("$", "projection metadata consistent with evidence_class");

  return snapshot;
}


// Scheduling is an operational truth surface. Unlike legacy observational
// readers, it must fail closed: a missing field cannot be rendered as zero
// capacity, no waiting, or a healthy owner.
function schedulingPayloadError(path: string, expected: string): never {
  throw new Error(`Invalid /scheduling payload: ${path} must be ${expected}`);
}

function schedulingRecord(value: unknown, path: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return schedulingPayloadError(path, "an object");
  }
  return value as Record<string, unknown>;
}

function schedulingField(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): unknown {
  if (!Object.hasOwn(parent, key)) {
    return schedulingPayloadError(`${path}.${key}`, "present");
  }
  return parent[key];
}

function schedulingString(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): string {
  const value = schedulingField(parent, key, path);
  if (typeof value !== "string" || value === "") {
    return schedulingPayloadError(`${path}.${key}`, "a non-empty string");
  }
  return value;
}

function schedulingNullableString(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): string | null {
  const value = schedulingField(parent, key, path);
  if (value === null) return null;
  if (typeof value !== "string" || value === "") {
    return schedulingPayloadError(`${path}.${key}`, "a non-empty string or null");
  }
  return value;
}

function schedulingOptionalNullableString(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): string | null {
  if (!Object.hasOwn(parent, key) || parent[key] === null) return null;
  if (typeof parent[key] !== "string" || parent[key] === "") {
    return schedulingPayloadError(`${path}.${key}`, "a non-empty string or null");
  }
  return parent[key] as string;
}

function schedulingNumber(
  parent: Record<string, unknown>,
  key: string,
  path: string,
  integer = false,
  minimum: number | null = null,
): number {
  const value = schedulingField(parent, key, path);
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    (integer && !Number.isInteger(value)) ||
    (minimum !== null && value < minimum)
  ) {
    return schedulingPayloadError(
      `${path}.${key}`,
      `${integer ? "a finite integer" : "a finite number"}${
        minimum === null ? "" : ` >= ${minimum}`
      }`,
    );
  }
  return value;
}

function schedulingOptionalNumber(
  parent: Record<string, unknown>,
  key: string,
  path: string,
  fallback: number,
  integer = false,
  minimum: number | null = null,
): number {
  if (!Object.hasOwn(parent, key)) return fallback;
  return schedulingNumber(parent, key, path, integer, minimum);
}

function schedulingBoolean(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): boolean {
  const value = schedulingField(parent, key, path);
  if (typeof value !== "boolean") {
    return schedulingPayloadError(`${path}.${key}`, "a boolean");
  }
  return value;
}

function schedulingArray(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): unknown[] {
  const value = schedulingField(parent, key, path);
  if (!Array.isArray(value)) {
    return schedulingPayloadError(`${path}.${key}`, "an array");
  }
  return value;
}

function schedulingChoice<const Choices extends readonly string[]>(
  parent: Record<string, unknown>,
  key: string,
  path: string,
  choices: Choices,
): Choices[number] {
  const value = schedulingString(parent, key, path);
  if (!choices.includes(value)) {
    return schedulingPayloadError(`${path}.${key}`, `one of ${choices.join(", ")}`);
  }
  return value as Choices[number];
}

function schedulingNullableBoolean(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): boolean | null {
  const value = schedulingField(parent, key, path);
  if (value === null) return null;
  if (typeof value !== "boolean") {
    return schedulingPayloadError(`${path}.${key}`, "a boolean or null");
  }
  return value;
}

function schedulingUtcTimestamp(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): string {
  const value = schedulingString(parent, key, path);
  if (!Number.isFinite(Date.parse(value)) || !/(?:Z|[+-]00:00)$/i.test(value)) {
    return schedulingPayloadError(`${path}.${key}`, "an ISO-8601 UTC timestamp");
  }
  return value;
}

function schedulingNullableUtcTimestamp(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): string | null {
  const value = schedulingField(parent, key, path);
  if (value === null) return null;
  if (
    typeof value !== "string" ||
    !Number.isFinite(Date.parse(value)) ||
    !/(?:Z|[+-]00:00)$/i.test(value)
  ) {
    return schedulingPayloadError(`${path}.${key}`, "an ISO-8601 UTC timestamp or null");
  }
  return value;
}

function schedulingExactKeys(
  value: Record<string, unknown>,
  keys: readonly string[],
  path: string,
): void {
  const actual = Object.keys(value);
  if (actual.length !== keys.length || keys.some((key) => !Object.hasOwn(value, key))) {
    schedulingPayloadError(path, `an object with exactly: ${keys.join(", ")}`);
  }
}

const FAILURE_DOMAIN_STATES = ["cooling", "degraded", "probe_ready"] as const;
const FAILURE_DOMAIN_SYMBOL = /^[A-Za-z0-9._:-]{1,128}$/;

function parseFailureDomains(value: unknown): EngramFailureDomainSnapshot {
  const path = "$.failure_domains";
  const root = schedulingRecord(value, path);
  schedulingExactKeys(
    root,
    [
      "policy_version",
      "evidence_class",
      "limit",
      "total",
      "cooling",
      "degraded",
      "probe_ready",
      "truncated",
      "items",
    ],
    path,
  );

  const limit = schedulingNumber(root, "limit", path, true, 1);
  if (limit !== ENGRAM_FAILURE_DOMAIN_ITEM_LIMIT) {
    schedulingPayloadError(
      `${path}.limit`,
      String(ENGRAM_FAILURE_DOMAIN_ITEM_LIMIT),
    );
  }
  const total = schedulingNumber(root, "total", path, true, 0);
  const cooling = schedulingNumber(root, "cooling", path, true, 0);
  const degraded = schedulingNumber(root, "degraded", path, true, 0);
  const probeReady = schedulingNumber(root, "probe_ready", path, true, 0);
  const truncated = schedulingBoolean(root, "truncated", path);
  const rows = schedulingArray(root, "items", path);

  const seen = new Set<string>();
  const items = rows.map((entry, index): EngramFailureDomainItem => {
    const itemPath = `${path}.items[${index}]`;
    const row = schedulingRecord(entry, itemPath);
    schedulingExactKeys(
      row,
      [
        "engram_id",
        "state",
        "consecutive_failures",
        "last_failure_at",
        "retry_at",
        "last_error_code",
        "last_error_phase",
        "error_retryable",
        "prompt_accepted",
      ],
      itemPath,
    );
    const engramId = schedulingString(row, "engram_id", itemPath);
    if (seen.has(engramId)) {
      schedulingPayloadError(`${itemPath}.engram_id`, "unique within items");
    }
    seen.add(engramId);
    const state = schedulingChoice(row, "state", itemPath, FAILURE_DOMAIN_STATES);
    const retryAt = schedulingNullableUtcTimestamp(row, "retry_at", itemPath);
    if ((state === "cooling") !== (retryAt !== null)) {
      schedulingPayloadError(
        `${itemPath}.retry_at`,
        state === "cooling" ? "a UTC timestamp while cooling" : "null unless cooling",
      );
    }
    const lastErrorCode = schedulingString(row, "last_error_code", itemPath);
    const lastErrorPhase = schedulingNullableString(row, "last_error_phase", itemPath);
    if (!FAILURE_DOMAIN_SYMBOL.test(lastErrorCode)) {
      schedulingPayloadError(`${itemPath}.last_error_code`, "a bounded ASCII symbol");
    }
    if (lastErrorPhase !== null && !FAILURE_DOMAIN_SYMBOL.test(lastErrorPhase)) {
      schedulingPayloadError(`${itemPath}.last_error_phase`, "a bounded ASCII symbol or null");
    }
    return {
      engram_id: engramId,
      state,
      consecutive_failures: schedulingNumber(
        row,
        "consecutive_failures",
        itemPath,
        true,
        1,
      ),
      last_failure_at: schedulingUtcTimestamp(row, "last_failure_at", itemPath),
      retry_at: retryAt,
      last_error_code: lastErrorCode,
      last_error_phase: lastErrorPhase,
      error_retryable: schedulingBoolean(row, "error_retryable", itemPath),
      prompt_accepted: schedulingNullableBoolean(row, "prompt_accepted", itemPath),
    };
  });

  if (cooling + degraded + probeReady !== total) {
    schedulingPayloadError(path, "state counts that sum to total");
  }
  if (truncated !== (total > limit) || items.length !== Math.min(total, limit)) {
    schedulingPayloadError(path, "items/truncated consistent with total and limit");
  }
  const visibleCounts = { cooling: 0, degraded: 0, probe_ready: 0 };
  for (const item of items) visibleCounts[item.state] += 1;
  if (
    visibleCounts.cooling > cooling ||
    visibleCounts.degraded > degraded ||
    visibleCounts.probe_ready > probeReady
  ) {
    schedulingPayloadError(path, "visible item states covered by aggregate counts");
  }
  const stateOrder: Record<EngramFailureDomainState, number> = {
    cooling: 0,
    degraded: 1,
    probe_ready: 2,
  };
  for (let index = 1; index < items.length; index += 1) {
    const previous = items[index - 1];
    const current = items[index];
    const outOfOrder =
      stateOrder[previous.state] > stateOrder[current.state] ||
      (previous.state === current.state &&
        (Date.parse(previous.last_failure_at) < Date.parse(current.last_failure_at) ||
          (previous.last_failure_at === current.last_failure_at &&
            previous.engram_id > current.engram_id)));
    if (outOfOrder) {
      schedulingPayloadError(`${path}.items`, "canonical failure-domain order");
    }
  }

  return {
    policy_version: schedulingChoice(
      root,
      "policy_version",
      path,
      [ENGRAM_FAILURE_DOMAIN_POLICY_VERSION],
    ),
    evidence_class: schedulingChoice(
      root,
      "evidence_class",
      path,
      [ENGRAM_FAILURE_DOMAIN_EVIDENCE_CLASS],
    ),
    limit: ENGRAM_FAILURE_DOMAIN_ITEM_LIMIT,
    total,
    cooling,
    degraded,
    probe_ready: probeReady,
    truncated,
    items,
  };
}

export function parseScheduling(body: unknown): SchedulingSnapshot {
  const root = schedulingRecord(body, "$");
  const lease = schedulingRecord(
    schedulingField(root, "lease", "$"),
    "$.lease",
  );
  const capacity = schedulingRecord(
    schedulingField(root, "capacity", "$"),
    "$.capacity",
  );
  const failureDomains = parseFailureDomains(
    schedulingField(root, "failure_domains", "$"),
  );
  const laneRows = schedulingArray(root, "lanes", "$");
  const centerRows = schedulingArray(root, "centers", "$");
  const reservationRows = schedulingArray(root, "reservations", "$");

  return {
    policy_version: schedulingString(root, "policy_version", "$"),
    lease: {
      scope: schedulingString(lease, "scope", "$.lease"),
      owner_id: schedulingString(lease, "owner_id", "$.lease"),
      epoch: schedulingNumber(lease, "epoch", "$.lease", true, 1),
      state: schedulingString(lease, "state", "$.lease"),
      healthy: schedulingBoolean(lease, "healthy", "$.lease"),
      acquired_at: schedulingString(lease, "acquired_at", "$.lease"),
      renewed_at: schedulingString(lease, "renewed_at", "$.lease"),
      expires_at: schedulingString(lease, "expires_at", "$.lease"),
      released_at: schedulingNullableString(lease, "released_at", "$.lease"),
      lost_reason: schedulingOptionalNullableString(lease, "lost_reason", "$.lease"),
    },
    capacity: {
      budget_per_tick: schedulingNumber(capacity, "budget_per_tick", "$.capacity", true, 0),
      lane_reservation_per_tick: schedulingNumber(
        capacity,
        "lane_reservation_per_tick",
        "$.capacity",
        true,
        0,
      ),
      starvation_boost: schedulingNumber(capacity, "starvation_boost", "$.capacity", false, 0),
      starvation_debt_cap: schedulingNumber(
        capacity,
        "starvation_debt_cap",
        "$.capacity",
        true,
        1,
      ),
      held: schedulingNumber(capacity, "held", "$.capacity", true, 0),
      background_dispatch: schedulingBoolean(
        capacity,
        "background_dispatch",
        "$.capacity",
      ),
      worker_limit: schedulingNumber(capacity, "worker_limit", "$.capacity", true, 0),
      worker_running: schedulingNumber(capacity, "worker_running", "$.capacity", true, 0),
      worker_available: schedulingNumber(
        capacity,
        "worker_available",
        "$.capacity",
        true,
        0,
      ),
      resident_limit: schedulingNumber(capacity, "resident_limit", "$.capacity", true, 0),
      resident_sessions: schedulingNumber(
        capacity,
        "resident_sessions",
        "$.capacity",
        true,
        0,
      ),
      starting_sessions: schedulingNumber(
        capacity,
        "starting_sessions",
        "$.capacity",
        true,
        0,
      ),
      busy_sessions: schedulingNumber(capacity, "busy_sessions", "$.capacity", true, 0),
      succession_worker_limit: schedulingOptionalNumber(
        capacity,
        "succession_worker_limit",
        "$.capacity",
        0,
        true,
        0,
      ),
      succession_workers_running: schedulingOptionalNumber(
        capacity,
        "succession_workers_running",
        "$.capacity",
        0,
        true,
        0,
      ),
      succession_subjects_pending: schedulingOptionalNumber(
        capacity,
        "succession_subjects_pending",
        "$.capacity",
        0,
        true,
        0,
      ),
      succession_subjects_blocked: schedulingOptionalNumber(
        capacity,
        "succession_subjects_blocked",
        "$.capacity",
        0,
        true,
        0,
      ),
    },
    failure_domains: failureDomains,
    lanes: laneRows.map((value, index) => {
      const row = schedulingRecord(value, `$.lanes[${index}]`);
      return {
        lane: schedulingString(row, "lane", `$.lanes[${index}]`),
        waiting_centers: schedulingNumber(
          row,
          "waiting_centers",
          `$.lanes[${index}]`,
          true,
          0,
        ),
        max_debt: schedulingNumber(row, "max_debt", `$.lanes[${index}]`, true, 0),
        last_admitted_at: schedulingNullableString(
          row,
          "last_admitted_at",
          `$.lanes[${index}]`,
        ),
      };
    }),
    centers: centerRows.map((value, index) => {
      const path = `$.centers[${index}]`;
      const row = schedulingRecord(value, path);
      return {
        center_id: schedulingString(row, "center_id", path),
        lane: schedulingString(row, "lane", path),
        status: schedulingString(row, "status", path),
        decision: schedulingString(row, "decision", path),
        reason: schedulingString(row, "reason", path),
        starvation_debt: schedulingNumber(row, "starvation_debt", path, true, 0),
        waiting_since: schedulingNullableString(row, "waiting_since", path),
        last_admitted_at: schedulingNullableString(row, "last_admitted_at", path),
        last_decision_at: schedulingString(row, "last_decision_at", path),
        updated_at: schedulingString(row, "updated_at", path),
      };
    }),
    reservations: reservationRows.map((value, index) => {
      const path = `$.reservations[${index}]`;
      const row = schedulingRecord(value, path);
      return {
        id: schedulingString(row, "id", path),
        world_id: schedulingString(row, "world_id", path),
        event_id: schedulingString(row, "event_id", path),
        engram_id: schedulingString(row, "engram_id", path),
        center_id: schedulingNullableString(row, "center_id", path),
        lane: schedulingString(row, "lane", path),
        owner_id: schedulingString(row, "owner_id", path),
        lease_epoch: schedulingNumber(row, "lease_epoch", path, true, 1),
        state: schedulingString(row, "state", path),
        outcome: schedulingNullableString(row, "outcome", path),
        reason: schedulingString(row, "reason", path),
        base_priority: schedulingNumber(row, "base_priority", path),
        effective_score: schedulingNumber(row, "effective_score", path),
        created_at: schedulingString(row, "created_at", path),
        settled_at: schedulingNullableString(row, "settled_at", path),
      };
    }),
  };
}

// ── where the runtime is ─────────────────────────────────────────────────

/**
 * Same-origin when the API serves the built bundle; the explicit host is for
 * `npm run dev`, where the viewer and the runtime sit on different ports —
 * the same fallback App.tsx already uses for the event stream.
 */
export function useRuntimeBase(): string {
  const mode = useViewer((s) => s.mode);
  const liveUrl = useViewer((s) => s.liveUrl);
  const base = apiBase({ mode, liveUrl });
  // Vite proxies the public runtime routes during development; production is
  // served by the runtime itself. Both paths are intentionally same-origin so
  // PATCH identity, POST commands, and SSE exercise one browser contract.
  return base;
}

// ── polled endpoint ──────────────────────────────────────────────────────

export type FetchState = "idle" | "loading" | "ok" | "absent" | "failed";

export interface Fetched<T> {
  data: T | null;
  state: FetchState;
  detail: string | null;
  remedy: string | null;
  reload: () => void;
}

/**
 * One endpoint, polled. `base === null` disables it (the rail is collapsed);
 * a failure clears `data` rather than keeping the last good values on screen —
 * stale numbers rendered as live readings are the exact lie this rail exists
 * to avoid.
 */
export function useEndpoint<T>(
  base: string | null,
  path: string,
  parse: (body: unknown) => T,
  intervalMs: number,
): Fetched<T> {
  const [data, setData] = useState<T | null>(null);
  const [state, setState] = useState<FetchState>("idle");
  const [fault, setFault] = useState<{ detail: string; remedy: string | null } | null>(
    null,
  );
  const [nonce, setNonce] = useState(0);
  const parseRef = useRef(parse);
  parseRef.current = parse;

  const reload = useCallback(() => setNonce((n) => n + 1), []);

  useEffect(() => {
    if (base === null) {
      setState("idle");
      setData(null);
      setFault(null);
      return;
    }
    let cancelled = false;
    let controller = new AbortController();

    const run = () => {
      if (document.hidden) return;
      controller.abort();
      controller = new AbortController();
      setState((s) => (s === "ok" ? "ok" : "loading"));
      getJson(`${base}${path}`, controller.signal)
        .then((body) => {
          if (cancelled) return;
          setData(parseRef.current(body));
          setState("ok");
          setFault(null);
        })
        .catch((e: unknown) => {
          if (cancelled || (e instanceof Error && e.name === "AbortError")) return;
          const f =
            e instanceof RuntimeFault
              ? e
              : new RuntimeFault(e instanceof Error ? e.message : String(e), null, false);
          setData(null);
          setState(f.absent ? "absent" : "failed");
          setFault({ detail: f.message, remedy: f.remedy });
        });
    };

    run();
    const timer = window.setInterval(run, intervalMs);
    return () => {
      cancelled = true;
      controller.abort();
      window.clearInterval(timer);
    };
  }, [base, path, intervalMs, nonce]);

  return {
    data,
    state,
    detail: fault?.detail ?? null,
    remedy: fault?.remedy ?? null,
    reload,
  };
}

// ── event stream (contract §4) ───────────────────────────────────────────

export type StreamState = "off" | "connecting" | "open" | "error";

export interface StreamEvent {
  type: string;
  payload: Record<string, unknown>;
}

export function useRailStream(
  base: string | null,
  onEvent: (e: StreamEvent) => void,
): StreamState {
  const [state, setState] = useState<StreamState>("off");
  const onRef = useRef(onEvent);
  onRef.current = onEvent;

  useEffect(() => {
    if (base === null) {
      setState("off");
      return;
    }
    setState("connecting");

    const emit = (fallbackType: string, item: unknown) => {
      const r = rec(item);
      const type = str(r.type) ?? fallbackType;
      if (type === "") return;
      onRef.current({ type, payload: r });
    };
    const handle = (raw: LiveFrame) => {
      if (raw.event === "replay") return;
      setState("open");
      let body: unknown;
      try {
        body = JSON.parse(raw.data);
      } catch {
        return;
      }
      const fallbackType =
        raw.event === "snapshot" || raw.event === "append" || raw.event === "message"
          ? ""
          : raw.event;
      const wrapped =
        typeof body === "object" && body !== null ? rec(body)["events"] : null;
      const items = Array.isArray(body)
        ? body
        : Array.isArray(wrapped)
          ? wrapped
          : [body];
      for (const item of items) emit(fallbackType, item);
    };

    const release = subscribeLiveStream(`${base}/events`, {
      onFrame: handle,
      onStatus: (status) => setState(status),
    });

    return () => {
      release();
      setState("off");
    };
  }, [base]);

  return state;
}

// ── firing log: polled history + live overlay ────────────────────────────

const MAX_LIVE_MARKS = 3000;

export interface FiringLog extends Omit<Fetched<FiringMark[]>, "data"> {
  marks: FiringMark[];
  push: (mark: FiringMark) => void;
}

/**
 * GET /pulse/history is the floor; SSE `pulse` frames land on top so the strip
 * moves at pulse latency instead of poll latency. Both are firing *events*,
 * never averages — the gaps between marks are the signal (contract §3).
 */
export function useFiringLog(base: string | null, windowSec: number): FiringLog {
  const fetched = useEndpoint<FiringMark[]>(
    base,
    `/pulse/history?window=${windowSec}`,
    parseHistory,
    15_000,
  );
  const [live, setLive] = useState<FiringMark[]>([]);
  const buffer = useRef<FiringMark[]>([]);
  const flushTimer = useRef(0);

  useEffect(() => {
    if (base === null) setLive([]);
  }, [base]);

  const push = useCallback((mark: FiringMark) => {
    // A snapshot frame can carry thousands of historical pulses; coalesce them
    // into one state write instead of one render per pulse.
    buffer.current.push(mark);
    if (flushTimer.current !== 0) return;
    flushTimer.current = window.setTimeout(() => {
      flushTimer.current = 0;
      const batch = buffer.current;
      buffer.current = [];
      setLive((prev) => [...prev, ...batch].slice(-MAX_LIVE_MARKS));
    }, 120);
  }, []);

  useEffect(
    () => () => {
      window.clearTimeout(flushTimer.current);
      flushTimer.current = 0;
    },
    [],
  );

  // Dedupe on (engram, ms, kind): a poll and a stream frame report the same
  // firing, and drawing it twice would fake a burst.
  const cutoff = Date.now() - windowSec * 1000;
  const seen = new Set<string>();
  const marks: FiringMark[] = [];
  for (const m of [...(fetched.data ?? []), ...live]) {
    if (m.tMs < cutoff) continue;
    const key = `${m.engramId}|${m.tMs}|${m.kind}`;
    if (seen.has(key)) continue;
    seen.add(key);
    marks.push(m);
  }
  marks.sort((a, b) => a.tMs - b.tMs);

  return {
    marks,
    state: fetched.state,
    detail: fetched.detail,
    remedy: fetched.remedy,
    reload: fetched.reload,
    push,
  };
}

// ── writes ───────────────────────────────────────────────────────────────

/**
 * A POST always carries all four keys. Omitting one is not "leave it alone" —
 * under §2.2 a null hands that knob back to the claustrum, so a partial body
 * would silently release every knob it forgot to mention.
 */
export function mergeKnobs(current: KnobValues, patch: Partial<KnobValues>): KnobValues {
  const out = {} as KnobValues;
  for (const k of KNOB_NAMES) out[k] = k in patch ? (patch[k] ?? null) : current[k];
  return out;
}

export interface TuningAck {
  commanded: KnobValues;
  will_apply_from_tick: number | null;
}

export async function postTuning(base: string, body: KnobValues): Promise<TuningAck> {
  const ack = rec(await postJson(`${base}/tuning`, body));
  return {
    commanded: parseKnobs(ack.commanded),
    will_apply_from_tick: num(ack.will_apply_from_tick),
  };
}

export async function postDelegate(
  base: string,
  body: { task: string; to: string | null; backend: string | null },
): Promise<string | null> {
  const ack = rec(await postJson(`${base}/delegate`, body));
  return str(ack.delegation_id);
}

export async function postInject(
  base: string,
  engramId: string,
  content: string,
): Promise<string | null> {
  const ack = rec(
    await postJson(
      `${base}/engrams/${encodeURIComponent(engramId)}/inject`,
      { content, source: "user" },
    ),
  );
  return str(ack.event_id);
}

export async function patchIdentity(
  base: string,
  engramId: string,
  updates: { name?: string; nickname?: string | null },
): Promise<EngramIdentity> {
  const body = rec(
    await patchJson(
      `${base}/engrams/${encodeURIComponent(engramId)}/identity`,
      updates,
    ),
  );
  return {
    signature: str(body.signature) ?? engramId,
    name: str(body.name),
    name_origin: str(body.name_origin) ?? "user",
    nickname: str(body.nickname),
  };
}

export function faultText(e: unknown): string {
  if (e instanceof RuntimeFault) return e.remedy === null ? e.message : `${e.message} — ${e.remedy}`;
  return e instanceof Error ? e.message : String(e);
}

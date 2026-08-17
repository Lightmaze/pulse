import { apiFetch } from "./apiSecurity.ts";

export const MAX_ROLE_ACCOUNTABILITY_ROWS = 64;

export type RoleAccountabilityStatus =
  | "requested"
  | "active"
  | "suspended"
  | "released"
  | "expired"
  | "revoked";

export type RoleAccountabilityClass = "subject_role" | "task_role";
export type RoleOutputKind = "workspace_checkpoint" | "habitat_effect";
export type RoleEvidenceClass =
  | "CONTRACT_ONLY"
  | "LIVE_GATE_UNVERIFIED"
  | "LIVE";
export type RoleContributionEvidence =
  | "CONTROL_ONLY"
  | "LIVE_WORKSPACE_CHECKPOINTED"
  | "LIVE_HABITAT_EFFECT";

export interface RoleAccountabilityScope {
  center_ids: string[];
  lineage_id: string | null;
  task_front_id: string | null;
  action_scope: string | null;
}

export interface RoleAccountabilityObligation {
  kind: "direct_output";
  minimum_direct_outputs: number;
  max_consecutive_coordination: number;
  accepted_output_kinds: RoleOutputKind[];
}

export interface RoleContributionSummary {
  role_lease_id: string;
  accountability_cycle_id: string | null;
  role_epoch: number;
  direct_output_count: number;
  coordination_count: number;
  consecutive_coordination: number;
  last_direct_output_event_id: string | null;
  last_contribution_at: string | null;
  renewal_eligible: boolean;
  reason_code: string;
}

export interface RoleRenewalGate {
  contribution_gate_satisfied: boolean;
  eligible_now: boolean;
  reason_code: string;
  authorization_still_required: true;
}

export interface RoleAccountabilityEvidence {
  role: RoleEvidenceClass;
  contributions: RoleContributionEvidence[];
  payload_disclosed: false;
}

export interface RoleAccountabilityRole {
  role_lease_id: string;
  role_epoch: number;
  role_class: RoleAccountabilityClass;
  role_label: string;
  status: RoleAccountabilityStatus;
  lineage_id: string | null;
  scope: RoleAccountabilityScope;
  obligation: RoleAccountabilityObligation | null;
  accountability_cycle_id: string | null;
  valid_from: string;
  renew_after: string;
  expires_at: string;
  renewal_count: number;
  predecessor_lease_id: string | null;
  contribution_summary: RoleContributionSummary;
  renewal_gate: RoleRenewalGate;
  evidence: RoleAccountabilityEvidence;
}

export interface RoleAccountabilitySnapshot {
  schema_version: "role-accountability.v1";
  world_id: string;
  engram_id: string;
  projected_at: string;
  observer_effect: "READ_ONLY_NO_STIMULUS";
  payload_disclosed: false;
  roles: RoleAccountabilityRole[];
  role_count: number;
  roles_truncated: boolean;
}

export class RoleAccountabilityFault extends Error {
  readonly status: number;
  readonly code: string;
  readonly remedy: string | null;

  constructor(
    message: string,
    status: number,
    code: string,
    remedy: string | null,
  ) {
    super(message);
    this.name = "RoleAccountabilityFault";
    this.status = status;
    this.code = code;
    this.remedy = remedy;
  }
}

function fail(path: string, expectation: string): never {
  throw new RoleAccountabilityFault(
    `Invalid role-accountability.v1 payload: ${path} must be ${expectation}`,
    502,
    "role_accountability_projection_invalid",
    "reload after the durable role projection is healthy",
  );
}

function object(value: unknown, path: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return fail(path, "an object");
  }
  return value as Record<string, unknown>;
}

function exact(
  value: Record<string, unknown>,
  keys: readonly string[],
  path: string,
): void {
  const expected = new Set(keys);
  const actual = Object.keys(value);
  if (
    actual.length !== expected.size ||
    actual.some((key) => !expected.has(key))
  ) {
    fail(path, `exactly the fields ${keys.join(", ")}`);
  }
}

function text(value: unknown, path: string): string {
  if (typeof value !== "string" || value.trim() === "" || value !== value.trim()) {
    return fail(path, "non-empty text without surrounding whitespace");
  }
  return value;
}

function identifier(value: unknown, path: string): string {
  const result = text(value, path);
  if (!/^[A-Za-z0-9][A-Za-z0-9_.:/@-]{0,127}$/.test(result)) {
    return fail(path, "a bounded opaque identifier");
  }
  return result;
}

function nullableIdentifier(value: unknown, path: string): string | null {
  return value === null ? null : identifier(value, path);
}

function integer(value: unknown, path: string, minimum = 0): number {
  if (!Number.isInteger(value) || (value as number) < minimum) {
    return fail(path, `an integer >= ${minimum}`);
  }
  return value as number;
}

function boolean(value: unknown, path: string): boolean {
  if (typeof value !== "boolean") return fail(path, "a boolean");
  return value;
}

function timestamp(value: unknown, path: string): string {
  const result = text(value, path);
  if (!Number.isFinite(Date.parse(result))) return fail(path, "an ISO timestamp");
  return result;
}

function nullableTimestamp(value: unknown, path: string): string | null {
  return value === null ? null : timestamp(value, path);
}

function oneOf<T extends string>(
  value: unknown,
  allowed: readonly T[],
  path: string,
): T {
  if (typeof value !== "string" || !allowed.includes(value as T)) {
    return fail(path, `one of ${allowed.join(", ")}`);
  }
  return value as T;
}

function uniqueIdentifiers(value: unknown, path: string): string[] {
  if (!Array.isArray(value) || value.length > 64) {
    return fail(path, "an array of at most 64 identifiers");
  }
  const result = value.map((item, index) => identifier(item, `${path}[${index}]`));
  if (new Set(result).size !== result.length) return fail(path, "unique identifiers");
  return result;
}

function parseScope(value: unknown, path: string): RoleAccountabilityScope {
  const row = object(value, path);
  exact(row, ["center_ids", "lineage_id", "task_front_id", "action_scope"], path);
  const scope = {
    center_ids: uniqueIdentifiers(row.center_ids, `${path}.center_ids`),
    lineage_id: nullableIdentifier(row.lineage_id, `${path}.lineage_id`),
    task_front_id: nullableIdentifier(row.task_front_id, `${path}.task_front_id`),
    action_scope: nullableIdentifier(row.action_scope, `${path}.action_scope`),
  };
  if (
    scope.center_ids.length === 0 &&
    scope.lineage_id === null &&
    scope.task_front_id === null &&
    scope.action_scope === null
  ) {
    fail(path, "a non-empty bounded role scope");
  }
  return scope;
}

function parseObligation(
  value: unknown,
  path: string,
): RoleAccountabilityObligation | null {
  if (value === null) return null;
  const row = object(value, path);
  exact(
    row,
    [
      "kind",
      "minimum_direct_outputs",
      "max_consecutive_coordination",
      "accepted_output_kinds",
    ],
    path,
  );
  if (row.kind !== "direct_output") return fail(`${path}.kind`, 'literal "direct_output"');
  const outputsRaw = row.accepted_output_kinds;
  if (!Array.isArray(outputsRaw) || outputsRaw.length === 0 || outputsRaw.length > 2) {
    return fail(`${path}.accepted_output_kinds`, "one or two output kinds");
  }
  const outputs = outputsRaw.map((item, index) => oneOf(
    item,
    ["workspace_checkpoint", "habitat_effect"] as const,
    `${path}.accepted_output_kinds[${index}]`,
  ));
  if (new Set(outputs).size !== outputs.length) {
    return fail(`${path}.accepted_output_kinds`, "unique output kinds");
  }
  return {
    kind: "direct_output",
    minimum_direct_outputs: integer(row.minimum_direct_outputs, `${path}.minimum_direct_outputs`, 1),
    max_consecutive_coordination: integer(row.max_consecutive_coordination, `${path}.max_consecutive_coordination`),
    accepted_output_kinds: outputs,
  };
}

function parseSummary(
  value: unknown,
  roleId: string,
  roleEpoch: number,
  cycleId: string | null,
  obligation: RoleAccountabilityObligation | null,
  path: string,
): RoleContributionSummary {
  const row = object(value, path);
  exact(
    row,
    [
      "role_lease_id",
      "accountability_cycle_id",
      "role_epoch",
      "direct_output_count",
      "coordination_count",
      "consecutive_coordination",
      "last_direct_output_event_id",
      "last_contribution_at",
      "renewal_eligible",
      "reason_code",
    ],
    path,
  );
  const direct = integer(row.direct_output_count, `${path}.direct_output_count`);
  const coordination = integer(row.coordination_count, `${path}.coordination_count`);
  const consecutive = integer(row.consecutive_coordination, `${path}.consecutive_coordination`);
  const lastDirect = nullableIdentifier(row.last_direct_output_event_id, `${path}.last_direct_output_event_id`);
  const lastAt = nullableTimestamp(row.last_contribution_at, `${path}.last_contribution_at`);
  const eligible = boolean(row.renewal_eligible, `${path}.renewal_eligible`);
  const reason = text(row.reason_code, `${path}.reason_code`);
  if (
    identifier(row.role_lease_id, `${path}.role_lease_id`) !== roleId ||
    nullableIdentifier(row.accountability_cycle_id, `${path}.accountability_cycle_id`) !== cycleId ||
    integer(row.role_epoch, `${path}.role_epoch`, 1) !== roleEpoch ||
    consecutive > coordination ||
    (direct === 0) !== (lastDirect === null) ||
    (direct + coordination === 0) !== (lastAt === null)
  ) {
    return fail(path, "counts and identity consistent with the containing role");
  }
  let expectedEligible: boolean;
  let expectedReason: string;
  if (obligation === null) {
    if (direct !== 0 || coordination !== 0) {
      return fail(path, "zero contributions when no accountability cycle exists");
    }
    expectedEligible = true;
    expectedReason = "role_has_no_direct_output_obligation";
  } else if (direct < obligation.minimum_direct_outputs) {
    expectedEligible = false;
    expectedReason = "role_direct_output_required";
  } else if (consecutive > obligation.max_consecutive_coordination) {
    expectedEligible = false;
    expectedReason = "role_coordination_streak_exceeded";
  } else {
    expectedEligible = true;
    expectedReason = "role_direct_output_obligation_satisfied";
  }
  if (eligible !== expectedEligible || reason !== expectedReason) {
    return fail(path, "the canonical contribution-gate conclusion");
  }
  return {
    role_lease_id: roleId,
    accountability_cycle_id: cycleId,
    role_epoch: roleEpoch,
    direct_output_count: direct,
    coordination_count: coordination,
    consecutive_coordination: consecutive,
    last_direct_output_event_id: lastDirect,
    last_contribution_at: lastAt,
    renewal_eligible: eligible,
    reason_code: reason,
  };
}

function parseRenewalGate(
  value: unknown,
  status: RoleAccountabilityStatus,
  projectedAt: number,
  renewAfter: number,
  expiresAt: number,
  summary: RoleContributionSummary,
  path: string,
): RoleRenewalGate {
  const row = object(value, path);
  exact(
    row,
    [
      "contribution_gate_satisfied",
      "eligible_now",
      "reason_code",
      "authorization_still_required",
    ],
    path,
  );
  const contribution = boolean(row.contribution_gate_satisfied, `${path}.contribution_gate_satisfied`);
  const eligible = boolean(row.eligible_now, `${path}.eligible_now`);
  const reason = text(row.reason_code, `${path}.reason_code`);
  if (row.authorization_still_required !== true) {
    return fail(`${path}.authorization_still_required`, "literal true");
  }
  let expectedEligible = false;
  let expectedReason: string;
  if (projectedAt >= expiresAt) {
    expectedReason = "role_expired";
  } else if (status !== "active") {
    expectedReason = "role_not_active";
  } else if (projectedAt < renewAfter) {
    expectedReason = "role_renewal_window_not_open";
  } else if (!summary.renewal_eligible) {
    expectedReason = summary.reason_code;
  } else {
    expectedEligible = true;
    expectedReason = "role_renewal_window_and_contribution_gate_satisfied";
  }
  if (
    contribution !== summary.renewal_eligible ||
    eligible !== expectedEligible ||
    reason !== expectedReason
  ) {
    return fail(path, "the canonical timing and contribution-gate conclusion");
  }
  return {
    contribution_gate_satisfied: contribution,
    eligible_now: eligible,
    reason_code: reason,
    authorization_still_required: true,
  };
}

function parseEvidence(
  value: unknown,
  summary: RoleContributionSummary,
  path: string,
): RoleAccountabilityEvidence {
  const row = object(value, path);
  exact(row, ["role", "contributions", "payload_disclosed"], path);
  const role = oneOf(
    row.role,
    ["CONTRACT_ONLY", "LIVE_GATE_UNVERIFIED", "LIVE"] as const,
    `${path}.role`,
  );
  if (!Array.isArray(row.contributions) || row.contributions.length > 3) {
    return fail(`${path}.contributions`, "a bounded evidence-class array");
  }
  const contributions = row.contributions.map((item, index) => oneOf(
    item,
    ["CONTROL_ONLY", "LIVE_WORKSPACE_CHECKPOINTED", "LIVE_HABITAT_EFFECT"] as const,
    `${path}.contributions[${index}]`,
  ));
  if (new Set(contributions).size !== contributions.length) {
    return fail(`${path}.contributions`, "unique evidence classes");
  }
  const hasDirectEvidence = contributions.some((item) => item !== "CONTROL_ONLY");
  if (
    row.payload_disclosed !== false ||
    (summary.coordination_count > 0) !== contributions.includes("CONTROL_ONLY") ||
    (summary.direct_output_count > 0) !== hasDirectEvidence
  ) {
    return fail(path, "payload-free evidence consistent with contribution counts");
  }
  return { role, contributions, payload_disclosed: false };
}

function parseRole(
  value: unknown,
  projectedAt: number,
  path: string,
): RoleAccountabilityRole {
  const row = object(value, path);
  exact(
    row,
    [
      "role_lease_id",
      "role_epoch",
      "role_class",
      "role_label",
      "status",
      "lineage_id",
      "scope",
      "obligation",
      "accountability_cycle_id",
      "valid_from",
      "renew_after",
      "expires_at",
      "renewal_count",
      "predecessor_lease_id",
      "contribution_summary",
      "renewal_gate",
      "evidence",
    ],
    path,
  );
  const roleId = identifier(row.role_lease_id, `${path}.role_lease_id`);
  const roleEpoch = integer(row.role_epoch, `${path}.role_epoch`, 1);
  const roleClass = oneOf(
    row.role_class,
    ["subject_role", "task_role"] as const,
    `${path}.role_class`,
  );
  const status = oneOf(
    row.status,
    ["requested", "active", "suspended", "released", "expired", "revoked"] as const,
    `${path}.status`,
  );
  const lineageId = nullableIdentifier(row.lineage_id, `${path}.lineage_id`);
  const scope = parseScope(row.scope, `${path}.scope`);
  const obligation = parseObligation(row.obligation, `${path}.obligation`);
  const cycleId = nullableIdentifier(row.accountability_cycle_id, `${path}.accountability_cycle_id`);
  if ((obligation === null) !== (cycleId === null)) {
    return fail(path, "matching obligation and accountability-cycle presence");
  }
  if (roleClass === "subject_role" && (lineageId === null || scope.center_ids.length === 0)) {
    return fail(path, "a lineage- and Center-bound subject role");
  }
  const validFrom = timestamp(row.valid_from, `${path}.valid_from`);
  const renewAfter = timestamp(row.renew_after, `${path}.renew_after`);
  const expiresAt = timestamp(row.expires_at, `${path}.expires_at`);
  const validMs = Date.parse(validFrom);
  const renewMs = Date.parse(renewAfter);
  const expiresMs = Date.parse(expiresAt);
  if (!(validMs < renewMs && renewMs < expiresMs) || projectedAt < validMs) {
    return fail(path, "an ordered role lifetime containing the projection time");
  }
  if (
    ["requested", "active", "suspended"].includes(status) &&
    projectedAt >= expiresMs
  ) {
    return fail(`${path}.status`, "effective expiry at the projection time");
  }
  const summary = parseSummary(
    row.contribution_summary,
    roleId,
    roleEpoch,
    cycleId,
    obligation,
    `${path}.contribution_summary`,
  );
  const renewalGate = parseRenewalGate(
    row.renewal_gate,
    status,
    projectedAt,
    renewMs,
    expiresMs,
    summary,
    `${path}.renewal_gate`,
  );
  return {
    role_lease_id: roleId,
    role_epoch: roleEpoch,
    role_class: roleClass,
    role_label: text(row.role_label, `${path}.role_label`),
    status,
    lineage_id: lineageId,
    scope,
    obligation,
    accountability_cycle_id: cycleId,
    valid_from: validFrom,
    renew_after: renewAfter,
    expires_at: expiresAt,
    renewal_count: integer(row.renewal_count, `${path}.renewal_count`),
    predecessor_lease_id: nullableIdentifier(row.predecessor_lease_id, `${path}.predecessor_lease_id`),
    contribution_summary: summary,
    renewal_gate: renewalGate,
    evidence: parseEvidence(row.evidence, summary, `${path}.evidence`),
  };
}

export function parseRoleAccountability(
  value: unknown,
  expectedEngramId?: string,
): RoleAccountabilitySnapshot {
  const root = object(value, "$");
  exact(
    root,
    [
      "schema_version",
      "world_id",
      "engram_id",
      "projected_at",
      "observer_effect",
      "payload_disclosed",
      "roles",
      "role_count",
      "roles_truncated",
    ],
    "$",
  );
  if (root.schema_version !== "role-accountability.v1") {
    return fail("$.schema_version", 'literal "role-accountability.v1"');
  }
  if (root.observer_effect !== "READ_ONLY_NO_STIMULUS") {
    return fail("$.observer_effect", 'literal "READ_ONLY_NO_STIMULUS"');
  }
  if (root.payload_disclosed !== false) {
    return fail("$.payload_disclosed", "literal false");
  }
  const worldId = identifier(root.world_id, "$.world_id");
  const engramId = identifier(root.engram_id, "$.engram_id");
  if (expectedEngramId !== undefined && engramId !== expectedEngramId) {
    return fail("$.engram_id", "the requested Engram id");
  }
  const projectedAtText = timestamp(root.projected_at, "$.projected_at");
  const projectedAt = Date.parse(projectedAtText);
  if (!Array.isArray(root.roles) || root.roles.length > MAX_ROLE_ACCOUNTABILITY_ROWS) {
    return fail("$.roles", `an array of at most ${MAX_ROLE_ACCOUNTABILITY_ROWS} roles`);
  }
  const roles = root.roles.map((item, index) => parseRole(item, projectedAt, `$.roles[${index}]`));
  if (new Set(roles.map((role) => role.role_lease_id)).size !== roles.length) {
    return fail("$.roles", "unique role identities");
  }
  const roleCount = integer(root.role_count, "$.role_count");
  if (roleCount !== roles.length) return fail("$.role_count", "the returned role count");
  return {
    schema_version: "role-accountability.v1",
    world_id: worldId,
    engram_id: engramId,
    projected_at: projectedAtText,
    observer_effect: "READ_ONLY_NO_STIMULUS",
    payload_disclosed: false,
    roles,
    role_count: roleCount,
    roles_truncated: boolean(root.roles_truncated, "$.roles_truncated"),
  };
}

function basePath(base: string): string {
  return base.replace(/\/+$/, "");
}

export async function fetchRoleAccountability(
  base: string,
  engramId: string,
  signal?: AbortSignal,
  limit = 32,
): Promise<RoleAccountabilitySnapshot> {
  const boundedLimit = Math.max(
    1,
    Math.min(MAX_ROLE_ACCOUNTABILITY_ROWS, Math.floor(limit)),
  );
  let response: Response;
  try {
    response = await apiFetch(
      `${basePath(base)}/engrams/${encodeURIComponent(engramId)}` +
        `/role-accountability?limit=${boundedLimit}`,
      { signal },
    );
  } catch (cause) {
    if (cause instanceof Error && cause.name === "AbortError") throw cause;
    throw new RoleAccountabilityFault(
      "the role accountability observatory is unreachable",
      503,
      "role_accountability_unavailable",
      "start the Runtime API and retry",
    );
  }
  if (!response.ok) {
    const body = object(await response.json().catch(() => ({})), "fault");
    throw new RoleAccountabilityFault(
      typeof body.detail === "string" ? body.detail : `HTTP ${response.status}`,
      response.status,
      typeof body.error === "string" ? body.error : "role_accountability_request_failed",
      typeof body.remedy === "string" ? body.remedy : null,
    );
  }
  return parseRoleAccountability(await response.json(), engramId);
}

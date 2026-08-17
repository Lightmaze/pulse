import { getJson } from "./pulse";

export const RUNTIME_SHUTDOWN_PROTOCOL_VERSION = "runtime-shutdown.v1" as const;
export const RUNTIME_SHUTDOWN_PATH = "/runtime/shutdown" as const;

export type RuntimeShutdownPhase =
  | "open"
  | "freezing"
  | "settling"
  | "fencing"
  | "fenced"
  | "cleaning"
  | "closed";

export type RuntimeShutdownLifecycle = "open" | "closing" | "closed";
export type ShutdownEffectState = "not_started" | "settled" | "uncertain";
export type ShutdownOwnerState = "joined" | "escaped" | "unknown";
export type ShutdownProcessTreeState =
  | "not_applicable"
  | "empty_verified"
  | "root_exit_only"
  | "unknown"
  | "escaped";
export type ShutdownCancelState = "not_needed" | "signalled" | "failed" | "unknown";
export type ShutdownDurableRecoveryState =
  | "not_attempted"
  | "not_needed"
  | "completed"
  | "failed"
  | "timed_out";
export type ShutdownPublicationFenceState =
  | "not_attempted"
  | "active"
  | "revoked"
  | "failed";
export type ShutdownOwnerLeaseState =
  | "not_attempted"
  | "released"
  | "lost"
  | "release_pending"
  | "failed";
export type ShutdownStorageState =
  | "open"
  | "closed"
  | "retained_for_escaped_workers"
  | "close_pending"
  | "failed";

export interface RuntimeShutdownComponent {
  component: string;
  effect: ShutdownEffectState;
  owner: ShutdownOwnerState;
  process_tree: ShutdownProcessTreeState;
  cancel: ShutdownCancelState;
  started_at: string;
  finished_at: string;
  elapsed_seconds: number;
  active_before: number;
  unresolved: number;
  escaped: boolean;
  physical_exit_proven: boolean;
  clean: boolean;
  error_code: string | null;
}

export interface RuntimeShutdownSnapshot {
  protocol_version: typeof RUNTIME_SHUTDOWN_PROTOCOL_VERSION;
  shutdown_id: string | null;
  phase: RuntimeShutdownPhase;
  started_at: string | null;
  finished_at: string | null;
  timeout_seconds: number | null;
  elapsed_seconds: number;
  deadline_exhausted: boolean;
  admission_frozen: boolean;
  durable_recovery: ShutdownDurableRecoveryState;
  publication_fence: ShutdownPublicationFenceState;
  owner_lease: ShutdownOwnerLeaseState;
  control_plane_closed: boolean;
  contract_satisfied: boolean;
  clean: boolean;
  physical_exit_proven: boolean;
  escaped_count: number;
  storage_state: ShutdownStorageState;
  components: RuntimeShutdownComponent[];
}

const PHASES = [
  "open",
  "freezing",
  "settling",
  "fencing",
  "fenced",
  "cleaning",
  "closed",
] as const;
const EFFECT_STATES = ["not_started", "settled", "uncertain"] as const;
const OWNER_STATES = ["joined", "escaped", "unknown"] as const;
const PROCESS_TREE_STATES = [
  "not_applicable",
  "empty_verified",
  "root_exit_only",
  "unknown",
  "escaped",
] as const;
const CANCEL_STATES = ["not_needed", "signalled", "failed", "unknown"] as const;
const RECOVERY_STATES = [
  "not_attempted",
  "not_needed",
  "completed",
  "failed",
  "timed_out",
] as const;
const PUBLICATION_STATES = ["not_attempted", "active", "revoked", "failed"] as const;
const OWNER_LEASE_STATES = [
  "not_attempted",
  "released",
  "lost",
  "release_pending",
  "failed",
] as const;
const STORAGE_STATES = [
  "open",
  "closed",
  "retained_for_escaped_workers",
  "close_pending",
  "failed",
] as const;
const TOKEN = /^[a-z][a-z0-9._-]{0,63}$/;
const SHUTDOWN_ID = /^[0-9a-f]{32}$/;
const TIMEZONE_SUFFIX = /(?:z|[+-]\d{2}:\d{2})$/i;
const MAX_COMPONENTS = 64;
const MAX_ELAPSED_SECONDS = 86_400;

function invalid(path: string, expected: string): never {
  throw new Error(
    "Invalid " + RUNTIME_SHUTDOWN_PATH + " payload: " + path + " must be " + expected,
  );
}

function objectAt(value: unknown, path: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return invalid(path, "an object");
  }
  return value as Record<string, unknown>;
}

function field(row: Record<string, unknown>, key: string, path = "$"): unknown {
  if (!Object.hasOwn(row, key)) return invalid(path + "." + key, "present");
  return row[key];
}

function choice<T extends string>(
  row: Record<string, unknown>,
  key: string,
  allowed: readonly T[],
  path = "$",
): T {
  const value = field(row, key, path);
  if (typeof value !== "string" || !allowed.includes(value as T)) {
    return invalid(path + "." + key, "one of " + allowed.join(", "));
  }
  return value as T;
}

function bool(row: Record<string, unknown>, key: string, path = "$"): boolean {
  const value = field(row, key, path);
  if (typeof value !== "boolean") return invalid(path + "." + key, "a boolean");
  return value;
}

function finite(
  row: Record<string, unknown>,
  key: string,
  path = "$",
  options: { integer?: boolean; minimum?: number; maximum?: number } = {},
): number {
  const value = field(row, key, path);
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    (options.integer === true && !Number.isInteger(value)) ||
    (options.minimum !== undefined && value < options.minimum) ||
    (options.maximum !== undefined && value > options.maximum)
  ) {
    return invalid(path + "." + key, "a finite number in range");
  }
  return value;
}

function nullableFinite(
  row: Record<string, unknown>,
  key: string,
  path: string,
  options: { minimum?: number; maximum?: number },
): number | null {
  return field(row, key, path) === null ? null : finite(row, key, path, options);
}

function timestamp(row: Record<string, unknown>, key: string, path = "$"): string {
  const value = field(row, key, path);
  if (
    typeof value !== "string" ||
    !TIMEZONE_SUFFIX.test(value) ||
    !Number.isFinite(Date.parse(value))
  ) {
    return invalid(path + "." + key, "an ISO-8601 timestamp with timezone");
  }
  return value;
}

function nullableTimestamp(
  row: Record<string, unknown>,
  key: string,
  path = "$",
): string | null {
  return field(row, key, path) === null ? null : timestamp(row, key, path);
}

function nullableToken(row: Record<string, unknown>, key: string, path: string): string | null {
  const value = field(row, key, path);
  if (value === null) return null;
  if (typeof value !== "string" || !TOKEN.test(value)) {
    return invalid(path + "." + key, "null or a bounded canonical token");
  }
  return value;
}

function validateDiscardedDetail(row: Record<string, unknown>, path: string): void {
  const value = field(row, "detail", path);
  if (
    value !== null &&
    (typeof value !== "string" ||
      value.length < 1 ||
      value.length > 256 ||
      value.includes("\n") ||
      value.includes("\r"))
  ) {
    invalid(path + ".detail", "null or a bounded single-line string");
  }
}

function parseComponent(value: unknown, index: number): RuntimeShutdownComponent {
  const path = "$.components[" + index + "]";
  const row = objectAt(value, path);
  const componentValue = field(row, "component", path);
  if (typeof componentValue !== "string" || !TOKEN.test(componentValue)) {
    return invalid(path + ".component", "a bounded canonical token");
  }
  const effect = choice(row, "effect", EFFECT_STATES, path);
  const owner = choice(row, "owner", OWNER_STATES, path);
  const processTree = choice(row, "process_tree", PROCESS_TREE_STATES, path);
  const cancel = choice(row, "cancel", CANCEL_STATES, path);
  const unresolved = finite(row, "unresolved", path, { integer: true, minimum: 0 });
  const escaped = bool(row, "escaped", path);
  const physicalExitProven = bool(row, "physical_exit_proven", path);
  const clean = bool(row, "clean", path);
  const expectedEscaped = owner === "escaped" || processTree === "escaped";
  const expectedPhysicalExit =
    owner === "joined" &&
    (processTree === "not_applicable" || processTree === "empty_verified");
  const expectedClean = effect !== "uncertain" && expectedPhysicalExit && unresolved === 0;

  if (escaped !== expectedEscaped) {
    invalid(path + ".escaped", "consistent with owner/process_tree");
  }
  if (expectedEscaped && unresolved < 1) {
    invalid(path + ".unresolved", "at least 1 for escaped evidence");
  }
  if (physicalExitProven !== expectedPhysicalExit) {
    invalid(path + ".physical_exit_proven", "consistent with owner/process_tree");
  }
  if (clean !== expectedClean) {
    invalid(path + ".clean", "consistent with effect, owner, process_tree, and unresolved");
  }
  validateDiscardedDetail(row, path);
  const startedAt = timestamp(row, "started_at", path);
  const finishedAt = timestamp(row, "finished_at", path);
  if (Date.parse(finishedAt) < Date.parse(startedAt)) {
    invalid(path + ".finished_at", "at or after started_at");
  }

  return {
    component: componentValue,
    effect,
    owner,
    process_tree: processTree,
    cancel,
    started_at: startedAt,
    finished_at: finishedAt,
    elapsed_seconds: finite(row, "elapsed_seconds", path, {
      minimum: 0,
      maximum: MAX_ELAPSED_SECONDS,
    }),
    active_before: finite(row, "active_before", path, { integer: true, minimum: 0 }),
    unresolved,
    escaped,
    physical_exit_proven: physicalExitProven,
    clean,
    error_code: nullableToken(row, "error_code", path),
  };
}

export function runtimeShutdownLifecycle(
  snapshot: Pick<RuntimeShutdownSnapshot, "phase">,
): RuntimeShutdownLifecycle {
  if (snapshot.phase === "open") return "open";
  if (snapshot.phase === "closed") return "closed";
  return "closing";
}

export function parseRuntimeShutdown(body: unknown): RuntimeShutdownSnapshot {
  const root = objectAt(body, "$");
  if (field(root, "protocol_version") !== RUNTIME_SHUTDOWN_PROTOCOL_VERSION) {
    invalid("$.protocol_version", RUNTIME_SHUTDOWN_PROTOCOL_VERSION);
  }
  const phase = choice(root, "phase", PHASES);
  const rawShutdownId = field(root, "shutdown_id");
  const shutdownId = rawShutdownId === null ? null : rawShutdownId;
  if (shutdownId !== null && (typeof shutdownId !== "string" || !SHUTDOWN_ID.test(shutdownId))) {
    invalid("$.shutdown_id", "null or a 32-character lowercase hex id");
  }
  const componentValues = field(root, "components");
  if (!Array.isArray(componentValues) || componentValues.length > MAX_COMPONENTS) {
    invalid("$.components", "an array of at most " + MAX_COMPONENTS + " items");
  }
  const components = componentValues.map(parseComponent);
  if (new Set(components.map((item) => item.component)).size !== components.length) {
    invalid("$.components", "unique component names");
  }

  const startedAt = nullableTimestamp(root, "started_at");
  const finishedAt = nullableTimestamp(root, "finished_at");
  if (
    startedAt !== null &&
    finishedAt !== null &&
    Date.parse(finishedAt) < Date.parse(startedAt)
  ) {
    invalid("$.finished_at", "at or after started_at");
  }
  const timeoutSeconds = nullableFinite(root, "timeout_seconds", "$", {
    minimum: 0.05,
    maximum: 300,
  });
  const deadlineExhausted = bool(root, "deadline_exhausted");
  const admissionFrozen = bool(root, "admission_frozen");
  const durableRecovery = choice(root, "durable_recovery", RECOVERY_STATES);
  const publicationFence = choice(root, "publication_fence", PUBLICATION_STATES);
  const ownerLease = choice(root, "owner_lease", OWNER_LEASE_STATES);
  const controlPlaneClosed = bool(root, "control_plane_closed");
  const contractSatisfied = bool(root, "contract_satisfied");
  const clean = bool(root, "clean");
  const physicalExitProven = bool(root, "physical_exit_proven");
  const escapedCount = finite(root, "escaped_count", "$", {
    integer: true,
    minimum: 0,
    maximum: MAX_COMPONENTS,
  });
  const storageState = choice(root, "storage_state", STORAGE_STATES);
  const elapsedSeconds = finite(root, "elapsed_seconds", "$", {
    minimum: 0,
    maximum: MAX_ELAPSED_SECONDS,
  });
  const expectedEscapedCount = components.filter((item) => item.escaped).length;
  const expectedContract =
    admissionFrozen && controlPlaneClosed && publicationFence === "revoked";
  const expectedPhysicalExit =
    components.length > 0 && components.every((item) => item.physical_exit_proven);
  const expectedClean =
    expectedContract &&
    (durableRecovery === "completed" || durableRecovery === "not_needed") &&
    (ownerLease === "released" || ownerLease === "lost") &&
    storageState === "closed" &&
    expectedPhysicalExit &&
    components.every((item) => item.clean);

  if (escapedCount !== expectedEscapedCount) {
    invalid("$.escaped_count", "the number of escaped components");
  }
  if (escapedCount > 0 && !deadlineExhausted) {
    invalid("$.deadline_exhausted", "true when escaped work is reported");
  }
  if (contractSatisfied !== expectedContract) {
    invalid("$.contract_satisfied", "consistent with admission/control/publication evidence");
  }
  if (phase === "open") {
    if (
      shutdownId !== null ||
      startedAt !== null ||
      finishedAt !== null ||
      timeoutSeconds !== null ||
      elapsedSeconds !== 0 ||
      deadlineExhausted ||
      admissionFrozen ||
      durableRecovery !== "not_attempted" ||
      publicationFence !== "active" ||
      ownerLease !== "not_attempted" ||
      controlPlaneClosed ||
      contractSatisfied ||
      storageState !== "open" ||
      components.length !== 0 ||
      physicalExitProven ||
      clean
    ) {
      invalid("$", "an internally consistent open snapshot");
    }
  } else {
    if (
      shutdownId === null ||
      startedAt === null ||
      timeoutSeconds === null ||
      !admissionFrozen
    ) {
      invalid("$", "shutdown identity, start, and timeout after open");
    }
    if (
      phase === "closed"
        ? finishedAt === null || !controlPlaneClosed
        : finishedAt !== null || controlPlaneClosed
    ) {
      invalid("$", "finished/control evidence consistent with phase");
    }
    if (physicalExitProven !== expectedPhysicalExit) {
      invalid("$.physical_exit_proven", "consistent with all component evidence");
    }
    if (clean !== (phase === "closed" && expectedClean)) {
      invalid("$.clean", "consistent with all independent shutdown evidence");
    }
  }

  return {
    protocol_version: RUNTIME_SHUTDOWN_PROTOCOL_VERSION,
    shutdown_id: shutdownId,
    phase,
    started_at: startedAt,
    finished_at: finishedAt,
    timeout_seconds: timeoutSeconds,
    elapsed_seconds: elapsedSeconds,
    deadline_exhausted: deadlineExhausted,
    admission_frozen: admissionFrozen,
    durable_recovery: durableRecovery,
    publication_fence: publicationFence,
    owner_lease: ownerLease,
    control_plane_closed: controlPlaneClosed,
    contract_satisfied: contractSatisfied,
    clean,
    physical_exit_proven: physicalExitProven,
    escaped_count: escapedCount,
    storage_state: storageState,
    components,
  };
}

export async function fetchRuntimeShutdown(
  base: string,
  signal: AbortSignal,
): Promise<RuntimeShutdownSnapshot> {
  const normalizedBase = base.endsWith("/") ? base.slice(0, -1) : base;
  const body = await getJson(normalizedBase + RUNTIME_SHUTDOWN_PATH, signal);
  return parseRuntimeShutdown(body);
}

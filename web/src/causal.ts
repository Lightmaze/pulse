import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { apiFetch } from "./apiSecurity";

export type CausalFlow = "content" | "spectrum" | "tunnel";
export type CausalDomain =
  | "pulse"
  | "harness"
  | "world"
  | "habitat"
  | "generation"
  | "system"
  | string;
export type CausalStatus =
  | "queued"
  | "running"
  | "settled"
  | "failed"
  | "uncertain"
  | "reconciled"
  | "cancelled"
  | string;

export interface CausalEventView {
  seq: number;
  id: string;
  causal_id: string;
  parent_event_id: string | null;
  world_id: string;
  engram_id: string | null;
  center_id: string | null;
  flow: CausalFlow | null;
  domain: CausalDomain;
  kind: string;
  source: string;
  status: CausalStatus;
  metadata: Record<string, unknown>;
  attempts: number;
  created_at: string | null;
  updated_at: string | null;
  started_at: string | null;
  settled_at: string | null;
  resolution: string | null;
}

export interface CausalEventDetail extends CausalEventView {
  content: string | null;
  resolution_note: string | null;
}

export interface HarnessTurnView {
  id: string;
  event_id: string;
  engram_id: string;
  state: string;
  cursor_before: number;
  cursor_after: number;
  prompt_accepted: boolean | null;
  result_event_id: string | null;
  error_code: string | null;
  error_phase: string | null;
  started_at: string | null;
  updated_at: string | null;
  settled_at: string | null;
}

export interface GenerationView {
  id: string;
  causal_id: string;
  event_id: string | null;
  predecessor_id: string;
  successor_id: string | null;
  state: string;
  summary_turn_id: string | null;
  error_code: string | null;
  created_at: string | null;
  updated_at: string | null;
  settled_at: string | null;
}

export interface CausalDetailResponse {
  event: CausalEventDetail;
  children: CausalEventView[];
  turn: HarnessTurnView | null;
  generation: GenerationView | null;
  amplification: CausalAmplificationView;
}

export interface CausalAmplificationView {
  schema: "causal-amplification.v1";
  evidence_class: "durable_causal_ledger_projection";
  causal_id: string;
  world_id: string;
  observed_at: string;
  scope: {
    first_seq: number;
    last_seq: number;
    first_event_at: string;
    last_event_at: string;
  };
  amplification: {
    event_count: number;
    root_event_count: number;
    child_event_count: number;
    turn_root_count: number;
    claimed_turn_root_count: number;
    propagation_event_count: number;
    distinct_engram_count: number;
    revisit_count: number;
    revisited_engram_count: number;
    max_propagation_depth: number;
    max_children_per_parent: number;
    events_per_settled_turn: number | null;
    propagations_per_settled_turn: number | null;
  };
  queue: {
    queued_event_count: number;
    oldest_queued_age_ms: number | null;
    max_observed_queue_wait_ms: number | null;
  };
  settle_cost: {
    turn_attempt_count: number;
    settled_turn_count: number;
    terminal_turn_count: number;
    active_ms_total: number;
    active_ms_max: number;
    input_tokens: number;
    output_tokens: number;
    cached_tokens: number;
    cache_write_tokens: number;
    usage_complete_turn_count: number;
  };
  status_counts: Record<string, number>;
  flow_counts: Record<string, number>;
  flow_contract: {
    violation_event_count: number;
    violation_counts: Record<string, number>;
  };
}

export type ReconcileAction = "acknowledge" | "cancel" | "requeue";

export interface ReconcileResponse {
  event: CausalEventView;
  child: CausalEventView | null;
}

export type CausalStreamState = "idle" | "connecting" | "open" | "error";

export type CausalScope =
  | { kind: "world"; worldId: string | null }
  | { kind: "center"; worldId: string | null; centerId: string }
  | { kind: "engram"; worldId: string | null; engramId: string };

export interface CausalStreamSnapshot {
  events: CausalEventView[];
  state: CausalStreamState;
  error: string | null;
  lastSeq: number;
  hasEarlier: boolean;
  loadingEarlier: boolean;
  atCapacity: boolean;
  reconnect: () => void;
  loadEarlier: () => void;
  merge: (events: CausalEventView | CausalEventView[]) => void;
}

function record(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : {};
}

function stringValue(value: unknown): string | null {
  return typeof value === "string" && value !== "" ? value : null;
}

function finiteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function flowValue(value: unknown): CausalFlow | null {
  return value === "content" || value === "spectrum" || value === "tunnel"
    ? value
    : null;
}

function metadataValue(value: unknown): Record<string, unknown> {
  const source = record(value);
  const result: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(source)) {
    if (
      child === null ||
      typeof child === "string" ||
      typeof child === "boolean" ||
      (typeof child === "number" && Number.isFinite(child))
    ) {
      result[key] = child;
    }
  }
  return result;
}

function nonnegativeInteger(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) && value >= 0
    ? value
    : null;
}

function nonnegativeNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? value
    : null;
}

function nullableNonnegativeNumber(value: unknown): number | null | undefined {
  if (value === null) return null;
  const parsed = nonnegativeNumber(value);
  return parsed === null ? undefined : parsed;
}

function countRecord(value: unknown): Record<string, number> | null {
  const source = record(value);
  const result: Record<string, number> = {};
  for (const [key, child] of Object.entries(source)) {
    const parsed = nonnegativeInteger(child);
    if (parsed === null) return null;
    result[key] = parsed;
  }
  return result;
}

export function parseCausalAmplification(
  value: unknown,
): CausalAmplificationView | null {
  const root = record(value);
  const scope = record(root.scope);
  const amplification = record(root.amplification);
  const queue = record(root.queue);
  const settle = record(root.settle_cost);
  const flowContract = record(root.flow_contract);
  const causalId = stringValue(root.causal_id);
  const worldId = stringValue(root.world_id);
  const observedAt = stringValue(root.observed_at);
  const firstEventAt = stringValue(scope.first_event_at);
  const lastEventAt = stringValue(scope.last_event_at);
  const statusCounts = countRecord(root.status_counts);
  const flowCounts = countRecord(root.flow_counts);
  const violationCounts = countRecord(flowContract.violation_counts);
  if (
    root.schema !== "causal-amplification.v1" ||
    root.evidence_class !== "durable_causal_ledger_projection" ||
    causalId === null ||
    worldId === null ||
    observedAt === null ||
    firstEventAt === null ||
    lastEventAt === null ||
    statusCounts === null ||
    flowCounts === null ||
    violationCounts === null
  ) return null;

  const firstSeq = nonnegativeInteger(scope.first_seq);
  const lastSeq = nonnegativeInteger(scope.last_seq);
  const eventCount = nonnegativeInteger(amplification.event_count);
  const rootEventCount = nonnegativeInteger(amplification.root_event_count);
  const childEventCount = nonnegativeInteger(amplification.child_event_count);
  const turnRootCount = nonnegativeInteger(amplification.turn_root_count);
  const claimedTurnRootCount = nonnegativeInteger(amplification.claimed_turn_root_count);
  const propagationEventCount = nonnegativeInteger(amplification.propagation_event_count);
  const distinctEngramCount = nonnegativeInteger(amplification.distinct_engram_count);
  const revisitCount = nonnegativeInteger(amplification.revisit_count);
  const revisitedEngramCount = nonnegativeInteger(amplification.revisited_engram_count);
  const maxPropagationDepth = nonnegativeInteger(amplification.max_propagation_depth);
  const maxChildrenPerParent = nonnegativeInteger(amplification.max_children_per_parent);
  const eventsPerSettledTurn = nullableNonnegativeNumber(amplification.events_per_settled_turn);
  const propagationsPerSettledTurn = nullableNonnegativeNumber(amplification.propagations_per_settled_turn);
  const queuedEventCount = nonnegativeInteger(queue.queued_event_count);
  const oldestQueuedAge = nullableNonnegativeNumber(queue.oldest_queued_age_ms);
  const maxQueueWait = nullableNonnegativeNumber(queue.max_observed_queue_wait_ms);
  const turnAttemptCount = nonnegativeInteger(settle.turn_attempt_count);
  const settledTurnCount = nonnegativeInteger(settle.settled_turn_count);
  const terminalTurnCount = nonnegativeInteger(settle.terminal_turn_count);
  const activeMsTotal = nonnegativeNumber(settle.active_ms_total);
  const activeMsMax = nonnegativeNumber(settle.active_ms_max);
  const inputTokens = nonnegativeInteger(settle.input_tokens);
  const outputTokens = nonnegativeInteger(settle.output_tokens);
  const cachedTokens = nonnegativeInteger(settle.cached_tokens);
  const cacheWriteTokens = nonnegativeInteger(settle.cache_write_tokens);
  const usageCompleteTurnCount = nonnegativeInteger(settle.usage_complete_turn_count);
  const violationEventCount = nonnegativeInteger(flowContract.violation_event_count);
  if (
    firstSeq === null || lastSeq === null || eventCount === null ||
    rootEventCount === null || childEventCount === null || turnRootCount === null ||
    claimedTurnRootCount === null || propagationEventCount === null ||
    distinctEngramCount === null || revisitCount === null ||
    revisitedEngramCount === null || maxPropagationDepth === null ||
    maxChildrenPerParent === null || eventsPerSettledTurn === undefined ||
    propagationsPerSettledTurn === undefined || queuedEventCount === null ||
    oldestQueuedAge === undefined || maxQueueWait === undefined ||
    turnAttemptCount === null || settledTurnCount === null ||
    terminalTurnCount === null || activeMsTotal === null || activeMsMax === null ||
    inputTokens === null || outputTokens === null || cachedTokens === null ||
    cacheWriteTokens === null || usageCompleteTurnCount === null ||
    violationEventCount === null
  ) return null;

  return {
    schema: "causal-amplification.v1",
    evidence_class: "durable_causal_ledger_projection",
    causal_id: causalId,
    world_id: worldId,
    observed_at: observedAt,
    scope: {
      first_seq: firstSeq,
      last_seq: lastSeq,
      first_event_at: firstEventAt,
      last_event_at: lastEventAt,
    },
    amplification: {
      event_count: eventCount,
      root_event_count: rootEventCount,
      child_event_count: childEventCount,
      turn_root_count: turnRootCount,
      claimed_turn_root_count: claimedTurnRootCount,
      propagation_event_count: propagationEventCount,
      distinct_engram_count: distinctEngramCount,
      revisit_count: revisitCount,
      revisited_engram_count: revisitedEngramCount,
      max_propagation_depth: maxPropagationDepth,
      max_children_per_parent: maxChildrenPerParent,
      events_per_settled_turn: eventsPerSettledTurn,
      propagations_per_settled_turn: propagationsPerSettledTurn,
    },
    queue: {
      queued_event_count: queuedEventCount,
      oldest_queued_age_ms: oldestQueuedAge,
      max_observed_queue_wait_ms: maxQueueWait,
    },
    settle_cost: {
      turn_attempt_count: turnAttemptCount,
      settled_turn_count: settledTurnCount,
      terminal_turn_count: terminalTurnCount,
      active_ms_total: activeMsTotal,
      active_ms_max: activeMsMax,
      input_tokens: inputTokens,
      output_tokens: outputTokens,
      cached_tokens: cachedTokens,
      cache_write_tokens: cacheWriteTokens,
      usage_complete_turn_count: usageCompleteTurnCount,
    },
    status_counts: statusCounts,
    flow_counts: flowCounts,
    flow_contract: {
      violation_event_count: violationEventCount,
      violation_counts: violationCounts,
    },
  };
}

export function parseCausalEvent(value: unknown): CausalEventView | null {
  const row = record(value);
  const seq = finiteNumber(row.seq);
  const id = stringValue(row.id);
  const causalId = stringValue(row.causal_id);
  const worldId = stringValue(row.world_id);
  if (seq === null || id === null || causalId === null || worldId === null) return null;
  return {
    seq,
    id,
    causal_id: causalId,
    parent_event_id: stringValue(row.parent_event_id),
    world_id: worldId,
    engram_id: stringValue(row.engram_id),
    center_id: stringValue(row.center_id),
    flow: flowValue(row.flow),
    domain: stringValue(row.domain) ?? "system",
    kind: stringValue(row.kind) ?? "system",
    source: stringValue(row.source) ?? "system",
    status: stringValue(row.status) ?? "queued",
    metadata: metadataValue(row.metadata),
    attempts: finiteNumber(row.attempts) ?? 0,
    created_at: stringValue(row.created_at),
    updated_at: stringValue(row.updated_at),
    started_at: stringValue(row.started_at),
    settled_at: stringValue(row.settled_at),
    resolution: stringValue(row.resolution),
  };
}

function parseDetail(value: unknown): CausalDetailResponse | null {
  const root = record(value);
  const eventRow = record(root.event);
  const event = parseCausalEvent(eventRow);
  const amplification = parseCausalAmplification(root.amplification);
  if (event === null || amplification === null || amplification.causal_id !== event.causal_id) return null;
  return {
    event: {
      ...event,
      content: typeof eventRow.content === "string" ? eventRow.content : null,
      resolution_note:
        typeof eventRow.resolution_note === "string" ? eventRow.resolution_note : null,
    },
    children: Array.isArray(root.children)
      ? root.children.map(parseCausalEvent).filter((item): item is CausalEventView => item !== null)
      : [],
    turn: parseTurn(root.turn),
    generation: parseGeneration(root.generation),
    amplification,
  };
}

function parseTurn(value: unknown): HarnessTurnView | null {
  const row = record(value);
  const id = stringValue(row.id);
  const eventId = stringValue(row.event_id);
  const engramId = stringValue(row.engram_id);
  if (id === null || eventId === null || engramId === null) return null;
  return {
    id,
    event_id: eventId,
    engram_id: engramId,
    state: stringValue(row.state) ?? "running",
    cursor_before: finiteNumber(row.cursor_before) ?? 0,
    cursor_after: finiteNumber(row.cursor_after) ?? 0,
    prompt_accepted:
      typeof row.prompt_accepted === "boolean" ? row.prompt_accepted : null,
    result_event_id: stringValue(row.result_event_id),
    error_code: stringValue(row.error_code),
    error_phase: stringValue(row.error_phase),
    started_at: stringValue(row.started_at),
    updated_at: stringValue(row.updated_at),
    settled_at: stringValue(row.settled_at),
  };
}

function parseGeneration(value: unknown): GenerationView | null {
  const row = record(value);
  const id = stringValue(row.id);
  const causalId = stringValue(row.causal_id);
  const predecessorId = stringValue(row.predecessor_id);
  if (id === null || causalId === null || predecessorId === null) return null;
  return {
    id,
    causal_id: causalId,
    event_id: stringValue(row.event_id),
    predecessor_id: predecessorId,
    successor_id: stringValue(row.successor_id),
    state: stringValue(row.state) ?? "prepared",
    summary_turn_id: stringValue(row.summary_turn_id),
    error_code: stringValue(row.error_code),
    created_at: stringValue(row.created_at),
    updated_at: stringValue(row.updated_at),
    settled_at: stringValue(row.settled_at),
  };
}

function joinUrl(base: string, path: string): string {
  return `${base.replace(/\/+$/, "")}${path}`;
}

async function readJson(response: Response): Promise<unknown> {
  const body = await response.json().catch(() => null);
  if (!response.ok) {
    const row = record(body);
    const detail = stringValue(row.detail) ?? `HTTP ${response.status}`;
    const remedy = stringValue(row.remedy);
    throw new Error(remedy === null ? detail : `${detail} · ${remedy}`);
  }
  return body;
}

export async function fetchCausalDetail(
  base: string,
  eventId: string,
  signal?: AbortSignal,
): Promise<CausalDetailResponse> {
  const response = await apiFetch(
    `${joinUrl(base, "/causal-events/")}${encodeURIComponent(eventId)}`,
    { signal },
  );
  const parsed = parseDetail(await readJson(response));
  if (parsed === null) throw new Error("The runtime returned an incomplete causal detail.");
  return parsed;
}

export async function reconcileCausalEvent(
  base: string,
  eventId: string,
  action: ReconcileAction,
  note?: string,
): Promise<ReconcileResponse> {
  const response = await apiFetch(
    `${joinUrl(base, "/causal-events/")}${encodeURIComponent(eventId)}/reconcile`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, ...(note === undefined ? {} : { note }) }),
    },
  );
  const root = record(await readJson(response));
  const event = parseCausalEvent(root.event);
  const child = root.child === null ? null : parseCausalEvent(root.child);
  if (event === null || (root.child !== null && child === null)) {
    throw new Error("The runtime returned an incomplete reconciliation.");
  }
  return { event, child };
}

function scopeParams(scope: CausalScope): URLSearchParams {
  const params = new URLSearchParams();
  if (scope.worldId !== null) params.set("world_id", scope.worldId);
  if (scope.kind === "center") params.set("center_id", scope.centerId);
  if (scope.kind === "engram") params.set("engram_id", scope.engramId);
  return params;
}

function scopeKey(scope: CausalScope): string {
  return scope.kind === "center"
    ? `center\u0000${scope.worldId ?? ""}\u0000${scope.centerId}`
    : scope.kind === "engram"
      ? `engram\u0000${scope.worldId ?? ""}\u0000${scope.engramId}`
      : `world\u0000${scope.worldId ?? ""}`;
}

function eventMatchesScope(event: CausalEventView, scope: CausalScope): boolean {
  if (scope.worldId !== null && event.world_id !== scope.worldId) return false;
  if (scope.kind === "center") return event.center_id === scope.centerId;
  if (scope.kind === "engram") return event.engram_id === scope.engramId;
  return true;
}

function streamUrl(base: string, scope: CausalScope, afterSeq: number): string {
  const params = scopeParams(scope);
  params.set("after_seq", String(afterSeq));
  return `${joinUrl(base, "/causal-stream")}?${params.toString()}`;
}

const RETRY_DELAYS = [500, 1_000, 2_000, 4_000, 8_000];
const INITIAL_WINDOW = 180;
const EARLIER_WINDOW = 120;
export const MAX_CAUSAL_EVENTS = 1_000;

interface CausalWindow {
  events: CausalEventView[];
  hasEarlier: boolean;
  earliestSeq: number | null;
  nextSeq: number;
}

async function fetchCausalWindow(
  base: string,
  scope: CausalScope,
  limit: number,
  beforeSeq: number | null,
  signal: AbortSignal,
): Promise<CausalWindow> {
  const params = scopeParams(scope);
  params.set("direction", "backward");
  params.set("limit", String(limit));
  if (beforeSeq !== null) params.set("before_seq", String(beforeSeq));
  const response = await apiFetch(
    `${joinUrl(base, "/causal-events")}?${params.toString()}`,
    { signal },
  );
  const root = record(await readJson(response));
  if (!Array.isArray(root.events)) {
    throw new Error("The runtime returned an incomplete causal window.");
  }
  const events = root.events
    .map(parseCausalEvent)
    .filter((event): event is CausalEventView => event !== null)
    .sort((left, right) => left.seq - right.seq || left.id.localeCompare(right.id));
  if (
    events.length !== root.events.length ||
    events.some((event) => !eventMatchesScope(event, scope))
  ) {
    throw new Error("The runtime returned events outside the selected causal scope.");
  }
  return {
    events,
    hasEarlier:
      typeof root.has_earlier === "boolean"
        ? root.has_earlier
        : events.length === limit,
    earliestSeq: finiteNumber(root.earliest_seq) ?? events[0]?.seq ?? null,
    nextSeq:
      finiteNumber(root.next_seq) ??
      events.at(-1)?.seq ??
      0,
  };
}

export function mergeBoundedCausalEvents(
  current: Map<string, CausalEventView>,
  incoming: CausalEventView[],
): Map<string, CausalEventView> {
  const byId = new Map(current);
  for (const event of incoming) byId.set(event.id, event);
  if (byId.size <= MAX_CAUSAL_EVENTS) return byId;
  const newest = Array.from(byId.values())
    .sort((left, right) => left.seq - right.seq || left.id.localeCompare(right.id))
    .slice(-MAX_CAUSAL_EVENTS);
  return new Map(newest.map((event) => [event.id, event]));
}

export function useCausalStream(
  base: string | null,
  scope: CausalScope,
): CausalStreamSnapshot {
  const [eventsById, setEventsById] = useState<Map<string, CausalEventView>>(
    () => new Map(),
  );
  const [state, setState] = useState<CausalStreamState>("idle");
  const [error, setError] = useState<string | null>(null);
  const [hasEarlier, setHasEarlier] = useState(false);
  const [loadingEarlier, setLoadingEarlier] = useState(false);
  const [nonce, setNonce] = useState(0);
  const lastSeqRef = useRef(0);
  const earliestSeqRef = useRef<number | null>(null);
  const scopeRef = useRef<string | null>(null);
  const scopeIdentity = scopeKey(scope);

  const merge = useCallback((incoming: CausalEventView | CausalEventView[]) => {
    const rows = Array.isArray(incoming) ? incoming : [incoming];
    setEventsById((current) => {
      for (const row of rows) {
        lastSeqRef.current = Math.max(lastSeqRef.current, row.seq);
        earliestSeqRef.current = earliestSeqRef.current === null
          ? row.seq
          : Math.min(earliestSeqRef.current, row.seq);
      }
      return mergeBoundedCausalEvents(current, rows);
    });
  }, []);

  const reconnect = useCallback(() => {
    setNonce((value) => value + 1);
  }, []);

  const loadEarlier = useCallback(() => {
    const beforeSeq = earliestSeqRef.current;
    if (
      base === null ||
      beforeSeq === null ||
      loadingEarlier ||
      !hasEarlier ||
      eventsById.size >= MAX_CAUSAL_EVENTS
    ) {
      return;
    }
    const requestedScope = `${base}\u0000${scopeIdentity}`;
    const controller = new AbortController();
    setLoadingEarlier(true);
    void fetchCausalWindow(base, scope, EARLIER_WINDOW, beforeSeq, controller.signal)
      .then((window) => {
        if (scopeRef.current !== requestedScope) return;
        earliestSeqRef.current = window.earliestSeq ?? earliestSeqRef.current;
        setHasEarlier(window.hasEarlier);
        setEventsById((current) => mergeBoundedCausalEvents(current, window.events));
      })
      .catch((cause: unknown) => {
        if (scopeRef.current !== requestedScope) return;
        setError(cause instanceof Error ? cause.message : String(cause));
      })
      .finally(() => {
        if (scopeRef.current === requestedScope) setLoadingEarlier(false);
      });
  }, [base, eventsById.size, hasEarlier, loadingEarlier, scope, scopeIdentity]);

  useEffect(() => {
    const activeScope = `${base ?? ""}\u0000${scopeIdentity}`;
    if (scopeRef.current !== activeScope) {
      scopeRef.current = activeScope;
      setEventsById(new Map());
      lastSeqRef.current = 0;
      earliestSeqRef.current = null;
      setHasEarlier(false);
      setLoadingEarlier(false);
    }
    if (base === null) {
      setState("idle");
      setError(null);
      return;
    }

    let disposed = false;
    let eventSource: EventSource | null = null;
    let retryTimer: number | null = null;
    let stableTimer: number | null = null;
    let retryIndex = 0;
    const bootstrapController = new AbortController();

    const clearTimers = () => {
      if (retryTimer !== null) window.clearTimeout(retryTimer);
      if (stableTimer !== null) window.clearTimeout(stableTimer);
      retryTimer = null;
      stableTimer = null;
    };

    const scheduleRetry = (connect: () => void) => {
      if (disposed) return;
      const delay = RETRY_DELAYS[Math.min(retryIndex, RETRY_DELAYS.length - 1)];
      retryIndex = Math.min(retryIndex + 1, RETRY_DELAYS.length - 1);
      retryTimer = window.setTimeout(() => {
        retryTimer = null;
        connect();
      }, delay);
    };

    const open = () => {
      if (disposed) return;
      setState("connecting");
      eventSource = new EventSource(streamUrl(base, scope, lastSeqRef.current));
      eventSource.onopen = () => {
        if (disposed) return;
        setState("open");
        setError(null);
        stableTimer = window.setTimeout(() => {
          retryIndex = 0;
        }, 5_000);
      };
      eventSource.addEventListener("causal_event", (rawEvent: Event) => {
        const message = rawEvent as MessageEvent<string>;
        try {
          const event = parseCausalEvent(JSON.parse(message.data) as unknown);
          if (event !== null && eventMatchesScope(event, scope)) {
            merge(event);
          } else if (event !== null) {
            eventSource?.close();
            eventSource = null;
            setState("error");
            setError("The causal stream crossed the selected scope boundary.");
          }
        } catch {
          // A malformed frame is not a reason to tear down a healthy stream;
          // the next durable frame still carries its own canonical identity.
        }
      });
      eventSource.onerror = () => {
        if (disposed) return;
        eventSource?.close();
        eventSource = null;
        clearTimers();
        setState("error");
        setError("Causal stream disconnected.");
        scheduleRetry(open);
      };
    };

    const bootstrap = () => {
      if (disposed) return;
      setState("connecting");
      void fetchCausalWindow(
        base,
        scope,
        INITIAL_WINDOW,
        null,
        bootstrapController.signal,
      )
        .then((window) => {
          if (disposed) return;
          setEventsById(new Map(window.events.map((event) => [event.id, event])));
          lastSeqRef.current = window.nextSeq;
          earliestSeqRef.current = window.earliestSeq;
          setHasEarlier(window.hasEarlier);
          open();
        })
        .catch((cause: unknown) => {
          if (disposed || (cause instanceof Error && cause.name === "AbortError")) return;
          setState("error");
          setError(cause instanceof Error ? cause.message : String(cause));
          scheduleRetry(bootstrap);
        });
    };

    setError(null);
    bootstrap();
    return () => {
      disposed = true;
      bootstrapController.abort();
      clearTimers();
      eventSource?.close();
      eventSource = null;
    };
  }, [base, merge, nonce, scopeIdentity]);

  const events = useMemo(
    () =>
      Array.from(eventsById.values())
        .filter((event) => eventMatchesScope(event, scope))
        .sort(
          (left, right) => left.seq - right.seq || left.id.localeCompare(right.id),
        ),
    [eventsById, scope],
  );

  return {
    events,
    state,
    error,
    lastSeq: lastSeqRef.current,
    hasEarlier: hasEarlier && eventsById.size < MAX_CAUSAL_EVENTS,
    loadingEarlier,
    atCapacity: eventsById.size >= MAX_CAUSAL_EVENTS,
    reconnect,
    loadEarlier,
    merge,
  };
}

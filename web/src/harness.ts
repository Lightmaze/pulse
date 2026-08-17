/**
 * Browser-side contract for the bounded Harness observation plane.
 *
 * This module owns DTO parsing and HTTP/SSE transport only.  It does not
 * import RuntimeService, Pi internals, or the global Workbench store.  A
 * Workbench component can use the default clients below, or inject callbacks
 * when the host app has a different authenticated transport.
 */

import { apiFetch } from "./apiSecurity.ts";

export const MAX_HARNESS_EVENTS = 500;
export const MAX_HARNESS_TERMINAL_SESSIONS = 64;
export const MAX_HARNESS_TERMINAL_OUTPUT = 500;

export type HarnessEvidenceClass =
  | "FAKE_RPC_CONTRACT"
  | "LIVE_PI_PROVIDER"
  | "LIVE_OS_RESTRICTED"
  | "LIVE_WORKSPACE_CHECKPOINTED"
  | "CONTRACT_ONLY"
  | "LIVE_GATE_UNVERIFIED";

const HARNESS_EVIDENCE_CLASSES = new Set<HarnessEvidenceClass>([
  "FAKE_RPC_CONTRACT",
  "LIVE_PI_PROVIDER",
  "LIVE_OS_RESTRICTED",
  "LIVE_WORKSPACE_CHECKPOINTED",
  "CONTRACT_ONLY",
  "LIVE_GATE_UNVERIFIED",
]);

export interface HarnessEventPayload {
  [key: string]: unknown;
}

export interface HarnessEvent {
  event_id: string;
  turn_id: string;
  world_id: string | null;
  engram_id: string | null;
  seq: number;
  parent_event_id: string | null;
  kind: string;
  phase: string;
  source: string;
  status: string;
  occurred_at: string | null;
  payload: HarnessEventPayload;
  payload_bytes: number;
  payload_digest: string | null;
  redacted: boolean;
  truncated: boolean;
}

export interface HarnessGap {
  kind: "event_gap";
  turn_id?: string;
  event_id?: string;
  seq?: number;
  missing_from: number;
  missing_to: number;
  earliest_seq: number | null;
  next_seq: number | null;
  reason: string | null;
  evidence_class?: HarnessEvidenceClass;
}

export interface HarnessEventPage {
  events: HarnessEvent[];
  next_seq: number;
  has_more: boolean;
  gap: HarnessGap | null;
  earliest_seq: number | null;
  evidence_class: HarnessEvidenceClass;
}

export interface HarnessTurnItemHistory {
  revision: number;
  event_id: string | null;
  seq: number | null;
  kind: string;
  phase: string;
  status: string;
  occurred_at: string | null;
  evidence_level: string;
  terminal: boolean;
  late: boolean;
}

export interface HarnessTurnItem {
  item_id: string;
  turn_id: string;
  item_type: string;
  tool_call_id: string | null;
  tool_name: string | null;
  state: string;
  terminal: boolean;
  phase: string;
  phase_history: string[];
  history: HarnessTurnItemHistory[];
  history_total: number;
  history_truncated: boolean;
  late_event_count: number;
  evidence_class: HarnessEvidenceClass;
  evidence_level: string;
  has_gap: boolean;
  first_seq: number | null;
  last_seq: number | null;
  revision: number;
  conflict_count: number;
}

export interface HarnessTurnItemPage {
  protocol_version: string;
  turn_id: string;
  items: HarnessTurnItem[];
  next_revision: number;
  has_more: boolean;
  turn_known: boolean;
  bounded: boolean;
  truncated: boolean;
  source_evidence_class: HarnessEvidenceClass;
  event_next_seq: number;
  event_has_more: boolean;
}

export interface HarnessEventCursor {
  first_seq?: number;
  last_seq?: number;
  next_seq?: number;
  after_seq?: number;
  event_rows?: number;
  has_gap?: boolean;
}

export interface HarnessTurnSummary {
  turn_id: string;
  world_id: string | null;
  engram_id: string | null;
  state: string;
  terminal: boolean;
  terminal_reason?: string | null;
  stop_reason?: string | null;
  epoch: number | null;
  evidence_class: HarnessEvidenceClass;
  live_available: boolean;
  recovery: boolean | Record<string, unknown>;
  usage: Record<string, number>;
  event_cursor: HarnessEventCursor;
  capacity: Record<string, number | string>;
  error_code?: string | null;
}

export interface HarnessTurnCatalogEntry {
  id: string;
  engram_id: string | null;
  state: string;
  prompt_accepted: boolean | null;
  error_code: string | null;
  error_phase: string | null;
  started_at: string | null;
  updated_at: string | null;
  settled_at: string | null;
}

export interface HarnessTurnCatalogPage {
  turns: HarnessTurnCatalogEntry[];
  order: "desc";
  has_more: boolean;
  next_cursor: string | null;
}

export interface HarnessTurnCatalogRequest {
  limit?: number;
  before_turn_id?: string | null;
  state?: string | null;
}

export interface HarnessControlRequest {
  request_id: string;
  expected_epoch: number;
  expected_state?: string;
}

export interface HarnessInterruptRequest extends HarnessControlRequest {
  reason?: string;
}

export interface HarnessSteerRequest extends HarnessControlRequest {
  message: string;
}

export type HarnessApprovalDecision =
  | "allow_once"
  | "deny"
  | "cancel";

export interface HarnessApprovalRequest extends HarnessControlRequest {
  decision: HarnessApprovalDecision;
  expected_turn_id: string;
}

export interface HarnessControlResponse {
  request_id: string;
  turn_id: string | null;
  state: string;
  accepted: boolean;
  error_code: string | null;
  event_seq: number | null;
  evidence_class: HarnessEvidenceClass;
  uncertain: boolean;
  idempotent: boolean;
  approval_id?: string | null;
}

export interface HarnessCheckpoint {
  checkpoint_id: string;
  turn_id: string | null;
  world_id: string | null;
  engram_id: string | null;
  epoch: number | null;
  state: string;
  changed_paths: string[];
  applied_paths: string[];
  restored_paths: string[];
  manifest_digest: string | null;
  diff_digest: string | null;
  diff_preview: string | null;
  created_at: string | null;
  updated_at: string | null;
  evidence_class: HarnessEvidenceClass;
  uncertain: boolean;
  idempotent: boolean;
  reconciled_from_uncertain: boolean;
  error_code: string | null;
}

export interface HarnessCheckpointPage {
  checkpoints: HarnessCheckpoint[];
  count: number;
  evidence_class: HarnessEvidenceClass;
}

export interface HarnessRestoreRequest {
  request_id: string;
  expected_epoch: number;
  changed_paths?: string[];
}

export type HarnessTerminalSessionMode = "PIPE_SESSION" | "UNKNOWN";
export type HarnessTerminalTransport = "pipe" | "unknown";
export type HarnessTerminalSessionScope = "runtime_connection" | "unknown";

export interface HarnessTerminalCapabilities {
  stdin: false;
  resize: false;
  reconnect: boolean;
  stop: boolean;
}

export interface HarnessTerminalSession {
  terminal_session_id: string;
  turn_id: string | null;
  world_id: string | null;
  engram_id: string | null;
  epoch: number | null;
  mode: HarnessTerminalSessionMode;
  transport: HarnessTerminalTransport;
  session_scope: HarnessTerminalSessionScope;
  state: string;
  cwd_relative: string;
  command_digest: string;
  started_at: string | null;
  ended_at: string | null;
  exit_code: number | null;
  output_bytes: number;
  output_truncated: boolean;
  last_output_seq: number;
  launch_action_digest: string | null;
  evidence_class: HarnessEvidenceClass;
  sandbox_evidence: HarnessEvidenceClass;
  tree_containment: string;
  error_code: string | null;
  uncertain_reason: string | null;
  capabilities: HarnessTerminalCapabilities;
}

export interface HarnessTerminalSessionPage {
  sessions: HarnessTerminalSession[];
  count: number;
  evidence_class: HarnessEvidenceClass;
}

export interface HarnessTerminalOutputChunk {
  terminal_session_id: string;
  seq: number;
  stream: "stdout" | "stderr";
  text: string;
  byte_count: number;
  truncated: boolean;
  redacted: boolean;
  at: string | null;
}

export interface HarnessTerminalOutputGap {
  missing_from: number;
  missing_to: number;
  reason: string;
}

export interface HarnessTerminalOutputPage {
  terminal_session_id: string;
  output: HarnessTerminalOutputChunk[];
  earliest_seq: number | null;
  next_seq: number;
  has_more: boolean;
  gap: HarnessTerminalOutputGap | null;
  truncated: boolean;
  evidence_class: HarnessEvidenceClass;
}

export interface HarnessTerminalStopRequest {
  request_id: string;
  expected_epoch: number;
  expected_turn_id: string;
  expected_state?: string;
  reason?: string;
}

export interface HarnessTerminalStopResult extends HarnessTerminalSession {
  request_id: string;
  accepted: boolean;
  idempotent: boolean;
}

export class HarnessFault extends Error {
  readonly status: number;
  readonly code: string;
  readonly remedy: string | null;
  readonly absent: boolean;

  constructor(
    message: string,
    status: number,
    code: string,
    remedy: string | null,
  ) {
    super(message);
    this.name = "HarnessFault";
    this.status = status;
    this.code = code;
    this.remedy = remedy;
    this.absent = status === 404 || status === 501;
  }
}

export interface HarnessStreamHandlers {
  onEvent: (event: HarnessEvent) => void;
  onGap?: (gap: HarnessGap) => void;
  onOpen?: () => void;
  onError?: (error: HarnessFault) => void;
  onTransportFault?: (error: HarnessFault | null) => void;
}

export interface HarnessSubscription {
  close: () => void;
}

function record(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : {};
}

function text(value: unknown): string | null {
  return typeof value === "string" && value !== "" ? value : null;
}

function evidenceClass(value: unknown): HarnessEvidenceClass {
  const candidate = text(value);
  return candidate !== null && HARNESS_EVIDENCE_CLASSES.has(candidate as HarnessEvidenceClass)
    ? (candidate as HarnessEvidenceClass)
    : "LIVE_GATE_UNVERIFIED";
}

function integer(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) && value >= 0
    ? value
    : null;
}

function signedInteger(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) ? value : null;
}

function number(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function boolean(value: unknown, fallback: boolean): boolean {
  return typeof value === "boolean" ? value : fallback;
}

function workspaceRelative(value: unknown): string {
  const candidate = text(value);
  if (candidate === null) return ".";
  const normalized = candidate.replaceAll("\\", "/");
  if (
    /^[A-Za-z]:\//.test(normalized) ||
    normalized.startsWith("/") ||
    normalized === ".." ||
    normalized.startsWith("../") ||
    normalized.includes("/../")
  ) {
    return "[REDACTED]";
  }
  return normalized;
}

function payload(value: unknown): HarnessEventPayload {
  return record(value);
}

function event(value: unknown, fallbackTurnId: string): HarnessEvent | null {
  const row = record(value);
  const seq = integer(row.seq);
  if (seq === null || seq < 1) return null;
  return {
    event_id: text(row.event_id) ?? text(row.id) ?? `${fallbackTurnId}:${seq}`,
    turn_id: text(row.turn_id) ?? fallbackTurnId,
    world_id: text(row.world_id),
    engram_id: text(row.engram_id),
    seq,
    parent_event_id: text(row.parent_event_id),
    kind: text(row.kind) ?? "warning",
    phase: text(row.phase) ?? "observe",
    source: text(row.source) ?? "pi_rpc",
    status: text(row.status) ?? "completed",
    occurred_at: text(row.occurred_at),
    payload: payload(row.payload),
    payload_bytes: integer(row.payload_bytes) ?? 0,
    payload_digest: text(row.payload_digest),
    redacted: boolean(row.redacted, false),
    truncated: boolean(row.truncated, false),
  };
}

function gap(value: unknown): HarnessGap | null {
  const row = record(value);
  const missingFrom = integer(row.missing_from);
  const missingTo = integer(row.missing_to);
  if (missingFrom === null || missingTo === null) return null;
  return {
    kind: "event_gap",
    turn_id: text(row.turn_id) ?? undefined,
    event_id: text(row.event_id) ?? undefined,
    seq: integer(row.seq) ?? undefined,
    missing_from: missingFrom,
    missing_to: Math.max(missingFrom, missingTo),
    earliest_seq: integer(row.earliest_seq),
    next_seq: integer(row.next_seq),
    reason: text(row.reason),
    evidence_class: evidenceClass(row.evidence_class),
  };
}

function parsePage(value: unknown, turnId: string): HarnessEventPage {
  const root = record(value);
  const source = Array.isArray(root.events) ? root.events : [];
  const events = source
    .map((item) => event(item, turnId))
    .filter((item): item is HarnessEvent => item !== null)
    .sort((a, b) => a.seq - b.seq);
  return {
    events,
    next_seq: integer(root.next_seq) ?? events.at(-1)?.seq ?? 0,
    has_more: boolean(root.has_more, false),
    gap: gap(root.gap),
    earliest_seq: integer(root.earliest_seq) ?? events.at(0)?.seq ?? null,
    evidence_class: evidenceClass(root.evidence_class),
  };
}

function turnItemHistory(value: unknown): HarnessTurnItemHistory | null {
  const row = record(value);
  const revision = integer(row.revision);
  if (revision === null) return null;
  return {
    revision,
    event_id: text(row.event_id),
    seq: integer(row.seq),
    kind: text(row.kind) ?? "unknown",
    phase: text(row.phase) ?? "observe",
    status: text(row.status) ?? "unknown",
    occurred_at: text(row.occurred_at),
    evidence_level: text(row.evidence_level) ?? "LIVE_GATE_UNVERIFIED",
    terminal: boolean(row.terminal, false),
    late: boolean(row.late, false),
  };
}

function turnItem(value: unknown, turnId: string): HarnessTurnItem | null {
  const row = record(value);
  const itemId = text(row.item_id);
  if (itemId === null) return null;
  const rawHistory = Array.isArray(row.history) ? row.history : [];
  const history = rawHistory
    .map(turnItemHistory)
    .filter((item): item is HarnessTurnItemHistory => item !== null)
    .slice(-128);
  const conflicts = Array.isArray(row.conflicts) ? row.conflicts.length : 0;
  return {
    item_id: itemId,
    turn_id: text(row.turn_id) ?? turnId,
    item_type: text(row.item_type) ?? "event",
    tool_call_id: text(row.tool_call_id),
    tool_name: text(row.tool_name),
    state: text(row.state) ?? "unknown",
    terminal: boolean(row.terminal, false),
    phase: text(row.phase) ?? "observe",
    phase_history: stringList(row.phase_history),
    history,
    history_total: integer(row.history_total) ?? history.length,
    history_truncated: boolean(row.history_truncated, false),
    late_event_count: integer(row.late_event_count) ?? 0,
    evidence_class: evidenceClass(row.evidence_class),
    evidence_level: text(row.evidence_level) ?? "LIVE_GATE_UNVERIFIED",
    has_gap: boolean(row.has_gap, false),
    first_seq: integer(row.first_seq),
    last_seq: integer(row.last_seq),
    revision: integer(row.revision) ?? 0,
    conflict_count: conflicts,
  };
}

function parseTurnItems(value: unknown, turnId: string): HarnessTurnItemPage {
  const root = record(value);
  const source = Array.isArray(root.items) ? root.items : [];
  const items = source
    .map((item) => turnItem(item, turnId))
    .filter((item): item is HarnessTurnItem => item !== null);
  return {
    protocol_version: text(root.protocol_version) ?? "harness.turn-items.v1",
    turn_id: text(root.turn_id) ?? turnId,
    items,
    next_revision: integer(root.next_revision) ?? 0,
    has_more: boolean(root.has_more, false),
    turn_known: boolean(root.turn_known, items.length > 0),
    bounded: boolean(root.bounded, true),
    truncated: boolean(root.truncated, false),
    source_evidence_class: evidenceClass(root.source_evidence_class),
    event_next_seq: integer(root.event_next_seq) ?? 0,
    event_has_more: boolean(root.event_has_more, false),
  };
}

function parseSummary(value: unknown, turnId: string): HarnessTurnSummary {
  const root = record(value);
  const rawCursor = record(root.event_cursor ?? root.cursor);
  const cursor: HarnessEventCursor = {};
  for (const key of ["first_seq", "last_seq", "next_seq", "after_seq", "event_rows"] as const) {
    const parsed = integer(rawCursor[key]);
    if (parsed !== null) cursor[key] = parsed;
  }
  if (typeof rawCursor.has_gap === "boolean") cursor.has_gap = rawCursor.has_gap;
  const rawUsage = record(root.usage);
  const usage: Record<string, number> = {};
  for (const [key, valueAtKey] of Object.entries(rawUsage)) {
    const parsed = number(valueAtKey);
    if (parsed !== null) usage[key] = parsed;
  }
  const rawCapacity = record(root.capacity);
  const capacity: Record<string, number | string> = {};
  for (const [key, valueAtKey] of Object.entries(rawCapacity)) {
    const parsedNumber = number(valueAtKey);
    if (parsedNumber !== null) capacity[key] = parsedNumber;
    else if (typeof valueAtKey === "string") capacity[key] = valueAtKey;
  }
  const recoveryValue = root.recovery ?? root.recovery_required ?? false;
  return {
    turn_id: text(root.turn_id) ?? turnId,
    world_id: text(root.world_id),
    engram_id: text(root.engram_id),
    state: text(root.state) ?? text(root.status) ?? "unknown",
    terminal: boolean(root.terminal, false),
    terminal_reason: text(root.terminal_reason),
    stop_reason: text(root.stop_reason),
    epoch: integer(root.epoch),
    evidence_class: evidenceClass(root.evidence_class),
    live_available: boolean(root.live_available, false),
    recovery:
      typeof recoveryValue === "object" && recoveryValue !== null
        ? record(recoveryValue)
        : Boolean(recoveryValue),
    usage,
    event_cursor: cursor,
    capacity,
    error_code: text(root.error_code),
  };
}

function catalogEntry(value: unknown): HarnessTurnCatalogEntry | null {
  const row = record(value);
  const id = text(row.id);
  if (id === null) return null;
  return {
    id,
    engram_id: text(row.engram_id),
    state: text(row.state) ?? "unknown",
    prompt_accepted:
      typeof row.prompt_accepted === "boolean" ? row.prompt_accepted : null,
    error_code: text(row.error_code),
    error_phase: text(row.error_phase),
    started_at: text(row.started_at),
    updated_at: text(row.updated_at),
    settled_at: text(row.settled_at),
  };
}

function parseCatalog(value: unknown): HarnessTurnCatalogPage {
  const root = record(value);
  const source = Array.isArray(root.turns) ? root.turns : [];
  const turns = source
    .map(catalogEntry)
    .filter((item): item is HarnessTurnCatalogEntry => item !== null);
  return {
    turns,
    order: "desc",
    has_more: boolean(root.has_more, false),
    next_cursor: text(root.next_cursor),
  };
}

function parseControl(value: unknown, fallbackRequestId: string): HarnessControlResponse {
  const root = record(value);
  return {
    request_id: text(root.request_id) ?? fallbackRequestId,
    turn_id: text(root.turn_id),
    state: text(root.state) ?? "rejected",
    accepted: boolean(root.accepted, false),
    error_code: text(root.error_code),
    event_seq: integer(root.event_seq),
    evidence_class: evidenceClass(root.evidence_class),
    uncertain: boolean(root.uncertain, false),
    idempotent: boolean(root.idempotent, false),
    approval_id: text(root.approval_id),
  };
}

function stringList(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string").slice(0, 128)
    : [];
}

function checkpoint(value: unknown): HarnessCheckpoint | null {
  const root = record(value);
  const checkpointId = text(root.checkpoint_id) ?? text(root.id);
  if (checkpointId === null) return null;
  return {
    checkpoint_id: checkpointId,
    turn_id: text(root.turn_id),
    world_id: text(root.world_id),
    engram_id: text(root.engram_id),
    epoch: integer(root.epoch),
    state: text(root.state) ?? text(root.status) ?? "unknown",
    changed_paths: stringList(root.changed_paths),
    applied_paths: stringList(root.applied_paths),
    restored_paths: stringList(root.restored_paths),
    manifest_digest: text(root.manifest_digest),
    diff_digest: text(root.diff_digest),
    diff_preview: text(root.diff_preview),
    created_at: text(root.created_at),
    updated_at: text(root.updated_at),
    evidence_class: evidenceClass(root.evidence_class),
    uncertain: boolean(root.uncertain, false),
    idempotent: boolean(root.idempotent, false),
    reconciled_from_uncertain: boolean(root.reconciled_from_uncertain, false),
    error_code: text(root.error_code),
  };
}

function terminalSession(
  value: unknown,
  expectedSessionId?: string,
): HarnessTerminalSession | null {
  const root = record(value);
  const terminalSessionId = text(root.terminal_session_id);
  if (terminalSessionId === null) return null;
  if (expectedSessionId !== undefined && terminalSessionId !== expectedSessionId) {
    return null;
  }
  const rawCapabilities = record(root.capabilities);
  const rawMode = text(root.mode);
  const rawTransport = text(root.transport);
  const rawScope = text(root.session_scope);
  return {
    terminal_session_id: terminalSessionId,
    turn_id: text(root.turn_id),
    world_id: text(root.world_id),
    engram_id: text(root.engram_id),
    epoch: integer(root.epoch),
    mode: rawMode === "PIPE_SESSION" ? "PIPE_SESSION" : "UNKNOWN",
    transport: rawTransport === "pipe" ? "pipe" : "unknown",
    session_scope:
      rawScope === "runtime_connection" ? "runtime_connection" : "unknown",
    state: text(root.state) ?? "UNKNOWN",
    cwd_relative: workspaceRelative(root.cwd_relative),
    command_digest: text(root.command_digest) ?? "[REDACTED]",
    started_at: text(root.started_at),
    ended_at: text(root.ended_at),
    exit_code: signedInteger(root.exit_code),
    output_bytes: integer(root.output_bytes) ?? 0,
    output_truncated: boolean(root.output_truncated, false),
    last_output_seq: integer(root.last_output_seq) ?? 0,
    launch_action_digest: text(root.launch_action_digest),
    evidence_class: evidenceClass(root.evidence_class),
    sandbox_evidence: evidenceClass(root.sandbox_evidence),
    tree_containment: text(root.tree_containment) ?? "UNVERIFIED",
    error_code: text(root.error_code),
    uncertain_reason: text(root.uncertain_reason),
    capabilities: {
      // v1 is deliberately non-interactive.  A malformed/private response
      // cannot unlock stdin or resize in the browser.
      stdin: false,
      resize: false,
      reconnect: boolean(rawCapabilities.reconnect, false),
      stop: boolean(rawCapabilities.stop, false),
    },
  };
}

function parseTerminalSessions(
  value: unknown,
  turnId: string,
): HarnessTerminalSessionPage | null {
  const root = record(value);
  if (!Array.isArray(root.sessions)) return null;
  const sessions: HarnessTerminalSession[] = [];
  const seenSessionIds = new Set<string>();
  for (const valueAtIndex of root.sessions) {
    const item = terminalSession(valueAtIndex);
    if (
      item === null ||
      item.turn_id !== turnId ||
      seenSessionIds.has(item.terminal_session_id)
    ) {
      return null;
    }
    seenSessionIds.add(item.terminal_session_id);
    sessions.push(item);
    if (sessions.length >= MAX_HARNESS_TERMINAL_SESSIONS) break;
  }
  return {
    sessions,
    count: sessions.length,
    evidence_class: evidenceClass(root.evidence_class),
  };
}

function terminalOutputChunk(
  value: unknown,
  terminalSessionId: string,
): HarnessTerminalOutputChunk | null {
  const root = record(value);
  const rowSessionId = text(root.terminal_session_id);
  if (rowSessionId !== terminalSessionId) return null;
  const seq = integer(root.seq);
  if (seq === null || seq < 1) return null;
  const stream = text(root.stream);
  if (stream !== "stdout" && stream !== "stderr") return null;
  if (typeof root.text !== "string") return null;
  return {
    terminal_session_id: terminalSessionId,
    seq,
    stream,
    text: root.text.slice(0, 16_384),
    byte_count: integer(root.byte_count) ?? 0,
    truncated: boolean(root.truncated, false),
    redacted: boolean(root.redacted, false),
    at: text(root.at),
  };
}

function terminalOutputGap(
  value: unknown,
  afterSeq: number,
  earliestSeq: number | null,
): HarnessTerminalOutputGap | null {
  const root = record(value);
  const missingFrom = integer(root.missing_from) ?? afterSeq + 1;
  const missingTo =
    integer(root.missing_to) ??
    (earliestSeq === null ? missingFrom : earliestSeq - 1);
  return {
    missing_from: missingFrom,
    missing_to: Math.max(missingFrom, missingTo),
    reason: text(root.reason) ?? "retention_window",
  };
}

function gapCovers(
  gap: HarnessTerminalOutputGap | null,
  missingFrom: number,
  missingTo: number,
): boolean {
  return (
    gap !== null &&
    gap.missing_from <= missingFrom &&
    gap.missing_to >= missingTo
  );
}

function parseTerminalOutput(
  value: unknown,
  terminalSessionId: string,
  afterSeq: number,
): HarnessTerminalOutputPage | null {
  const root = record(value);
  if (text(root.terminal_session_id) !== terminalSessionId) return null;
  if (!("output" in root)) return null;
  const sourceValue = root.output;
  if (!Array.isArray(sourceValue)) return null;
  const source = sourceValue;
  const allChunks: HarnessTerminalOutputChunk[] = [];
  let previousSeq: number | null = null;
  for (const item of source) {
    if (text(record(item).terminal_session_id) !== terminalSessionId) {
      return null;
    }
    const chunk = terminalOutputChunk(item, terminalSessionId);
    if (chunk === null || chunk.seq <= afterSeq) return null;
    if (previousSeq !== null && chunk.seq <= previousSeq) return null;
    previousSeq = chunk.seq;
    allChunks.push(chunk);
  }
  const output = allChunks
    .filter((item) => item.seq > afterSeq)
    .slice(0, MAX_HARNESS_TERMINAL_OUTPUT);
  const projected = allChunks.filter((item) => item.seq > afterSeq);
  const earliestSeq = integer(root.earliest_seq) ?? allChunks.at(0)?.seq ?? null;
  if (
    earliestSeq !== null &&
    (allChunks.some((item) => item.seq < earliestSeq) ||
      (projected.length > 0 && earliestSeq > projected[0].seq))
  ) {
    return null;
  }
  const rawGap = root.gap;
  const gap =
    rawGap === undefined || rawGap === null || rawGap === false
      ? null
      : terminalOutputGap(rawGap, afterSeq, earliestSeq);
  if (gap !== null) {
    if (
      gap.missing_from < afterSeq + 1 ||
      allChunks.some(
        (item) =>
          gap.missing_from <= item.seq && item.seq <= gap.missing_to,
      )
    ) {
      return null;
    }
  }
  if (projected.length > 0) {
    if (
      projected[0].seq > afterSeq + 1 &&
      !gapCovers(gap, afterSeq + 1, projected[0].seq - 1)
    ) {
      return null;
    }
    for (let index = 1; index < projected.length; index += 1) {
      const previous = projected[index - 1];
      const current = projected[index];
      if (
        current.seq > previous.seq + 1 &&
        !gapCovers(gap, previous.seq + 1, current.seq - 1)
      ) {
        return null;
      }
    }
  } else if (
    earliestSeq !== null &&
    earliestSeq > afterSeq + 1 &&
    !gapCovers(gap, afterSeq + 1, earliestSeq - 1)
  ) {
    return null;
  }
  const expectedNextSeq =
    output.at(-1)?.seq ?? Math.max(0, Math.floor(afterSeq));
  const advertisedNextSeq = integer(root.next_seq);
  if (
    advertisedNextSeq === null ||
    advertisedNextSeq < 0 ||
    advertisedNextSeq !== expectedNextSeq
  ) {
    return null;
  }
  return {
    terminal_session_id: terminalSessionId,
    output,
    earliest_seq: earliestSeq,
    next_seq: advertisedNextSeq,
    has_more: boolean(root.has_more, false),
    gap,
    truncated: boolean(root.truncated, false) || output.some((item) => item.truncated),
    evidence_class: evidenceClass(root.evidence_class),
  };
}

function parseTerminalStop(
  value: unknown,
  terminalSessionId: string,
  requestId: string,
): HarnessTerminalStopResult | null {
  const session = terminalSession(value, terminalSessionId);
  if (session === null) return null;
  const root = record(value);
  if (typeof root.accepted !== "boolean") return null;
  return {
    ...session,
    request_id: text(root.request_id) ?? requestId,
    accepted: root.accepted,
    idempotent: boolean(root.idempotent, false),
  };
}

function basePath(base: string): string {
  return base.replace(/\/+$/, "");
}

function endpoint(base: string, path: string): string {
  return `${basePath(base)}${path}`;
}

async function responseFault(response: Response): Promise<HarnessFault> {
  const body = await response.json().catch(() => null);
  const root = record(body);
  return new HarnessFault(
    text(root.detail) ?? text(root.error) ?? `HTTP ${response.status}`,
    response.status,
    text(root.error) ?? text(root.error_code) ?? "harness_request_failed",
    text(root.remedy),
  );
}

async function fetchJson(url: string, signal?: AbortSignal): Promise<unknown> {
  let response: Response;
  try {
    response = await apiFetch(url, { signal });
  } catch (cause) {
    if (cause instanceof Error && cause.name === "AbortError") throw cause;
    throw new HarnessFault(
      "the Harness observatory is unreachable",
      503,
      "harness_unavailable",
      "start the sideband API and retry",
    );
  }
  if (!response.ok) throw await responseFault(response);
  return response.json();
}

async function postJson(
  url: string,
  body: unknown,
  signal?: AbortSignal,
): Promise<unknown> {
  let response: Response;
  try {
    response = await apiFetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
      signal,
    });
  } catch (cause) {
    if (cause instanceof Error && cause.name === "AbortError") throw cause;
    throw new HarnessFault(
      "the Harness control gateway is unreachable",
      503,
      "harness_unavailable",
      "attach the live policy-aware gateway and retry",
    );
  }
  if (!response.ok) throw await responseFault(response);
  return response.json().catch(() => ({}));
}

export function makeHarnessRequestId(prefix = "harness"): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${prefix}-${crypto.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export async function fetchHarnessSummary(
  base: string,
  turnId: string,
  signal?: AbortSignal,
): Promise<HarnessTurnSummary> {
  const body = await fetchJson(
    endpoint(base, `/harness/turns/${encodeURIComponent(turnId)}`),
    signal,
  );
  return parseSummary(body, turnId);
}

export async function fetchLatestHarnessTurnId(
  base: string,
  engramId: string,
  signal?: AbortSignal,
): Promise<string | null> {
  const page = await fetchHarnessTurns(base, engramId, { limit: 1 }, signal);
  return page.turns.at(0)?.id ?? null;
}

export async function fetchHarnessTurns(
  base: string,
  engramId: string,
  request: HarnessTurnCatalogRequest = {},
  signal?: AbortSignal,
): Promise<HarnessTurnCatalogPage> {
  const limit = Math.max(1, Math.min(500, Math.floor(request.limit ?? 12)));
  const query = new URLSearchParams({
    engram_id: engramId,
    limit: String(limit),
    order: "desc",
  });
  if (request.before_turn_id) query.set("before_turn_id", request.before_turn_id);
  if (request.state) query.set("state", request.state);
  const body = await fetchJson(
    endpoint(base, "/harness-turns?" + query.toString()),
    signal,
  );
  return parseCatalog(body);
}

export async function fetchHarnessEvents(
  base: string,
  turnId: string,
  afterSeq = 0,
  limit = 250,
  signal?: AbortSignal,
): Promise<HarnessEventPage> {
  const boundedLimit = Math.max(1, Math.min(MAX_HARNESS_EVENTS, Math.floor(limit)));
  const query = new URLSearchParams({
    after_seq: String(Math.max(0, Math.floor(afterSeq))),
    limit: String(boundedLimit),
  });
  const body = await fetchJson(
    endpoint(base, `/harness/turns/${encodeURIComponent(turnId)}/events?${query.toString()}`),
    signal,
  );
  return parsePage(body, turnId);
}

export async function fetchHarnessTurnItems(
  base: string,
  turnId: string,
  signal?: AbortSignal,
): Promise<HarnessTurnItemPage> {
  const body = await fetchJson(
    endpoint(base, `/harness/turns/${encodeURIComponent(turnId)}/items`),
    signal,
  );
  return parseTurnItems(body, turnId);
}

export async function interruptHarnessTurn(
  base: string,
  turnId: string,
  request: HarnessInterruptRequest,
  signal?: AbortSignal,
): Promise<HarnessControlResponse> {
  const body = await postJson(
    endpoint(base, `/harness/turns/${encodeURIComponent(turnId)}/interrupt`),
    request,
    signal,
  );
  return parseControl(body, request.request_id);
}

export async function steerHarnessTurn(
  base: string,
  turnId: string,
  request: HarnessSteerRequest,
  signal?: AbortSignal,
): Promise<HarnessControlResponse> {
  const body = await postJson(
    endpoint(base, `/harness/turns/${encodeURIComponent(turnId)}/steer`),
    request,
    signal,
  );
  return parseControl(body, request.request_id);
}

export async function resolveHarnessApproval(
  base: string,
  approvalId: string,
  request: HarnessApprovalRequest,
  signal?: AbortSignal,
): Promise<HarnessControlResponse> {
  const body = await postJson(
    endpoint(base, `/harness/approvals/${encodeURIComponent(approvalId)}/resolve`),
    request,
    signal,
  );
  return parseControl(body, request.request_id);
}

export async function fetchHarnessTerminalSessions(
  base: string,
  turnId: string,
  limit = 16,
  signal?: AbortSignal,
): Promise<HarnessTerminalSessionPage> {
  const boundedLimit = Math.max(
    1,
    Math.min(MAX_HARNESS_TERMINAL_SESSIONS, Math.floor(limit)),
  );
  const query = new URLSearchParams({ limit: String(boundedLimit) });
  const body = await fetchJson(
    endpoint(
      base,
      `/harness/turns/${encodeURIComponent(turnId)}/terminal-sessions?${query.toString()}`,
    ),
    signal,
  );
  const parsed = parseTerminalSessions(body, turnId);
  if (parsed === null) {
    throw new HarnessFault(
      "the terminal session list response is invalid",
      502,
      "terminal_session_projection_invalid",
      "reload after the durable terminal session gateway is healthy",
    );
  }
  return parsed;
}

export async function fetchHarnessTerminalSession(
  base: string,
  terminalSessionId: string,
  turnId: string,
  signal?: AbortSignal,
): Promise<HarnessTerminalSession> {
  const query = new URLSearchParams({ turn_id: turnId });
  const body = await fetchJson(
    endpoint(
      base,
      `/harness/terminal-sessions/${encodeURIComponent(terminalSessionId)}?${query.toString()}`,
    ),
    signal,
  );
  const parsed = terminalSession(body, terminalSessionId);
  if (parsed === null) {
    throw new HarnessFault(
      "the terminal session response is invalid",
      502,
      "terminal_session_projection_invalid",
      "reload the bounded terminal session list",
    );
  }
  return parsed;
}

export async function fetchHarnessTerminalOutput(
  base: string,
  terminalSessionId: string,
  turnId: string,
  afterSeq = 0,
  limit = 200,
  signal?: AbortSignal,
): Promise<HarnessTerminalOutputPage> {
  const boundedAfter = Math.max(0, Math.floor(afterSeq));
  const boundedLimit = Math.max(
    1,
    Math.min(MAX_HARNESS_TERMINAL_OUTPUT, Math.floor(limit)),
  );
  const query = new URLSearchParams({
    turn_id: turnId,
    after_seq: String(boundedAfter),
    limit: String(boundedLimit),
  });
  const body = await fetchJson(
    endpoint(
      base,
      `/harness/terminal-sessions/${encodeURIComponent(terminalSessionId)}/output?${query.toString()}`,
    ),
    signal,
  );
  const parsed = parseTerminalOutput(body, terminalSessionId, boundedAfter);
  if (parsed === null) {
    throw new HarnessFault(
      "the terminal output response is invalid",
      502,
      "terminal_output_projection_invalid",
      "reload the bounded output page for this terminal session",
    );
  }
  return parsed;
}

export async function stopHarnessTerminalSession(
  base: string,
  terminalSessionId: string,
  request: HarnessTerminalStopRequest,
  signal?: AbortSignal,
): Promise<HarnessTerminalStopResult> {
  const body = await postJson(
    endpoint(
      base,
      `/harness/terminal-sessions/${encodeURIComponent(terminalSessionId)}/stop`,
    ),
    request,
    signal,
  );
  const parsed = parseTerminalStop(
    body,
    terminalSessionId,
    request.request_id,
  );
  if (parsed === null) {
    throw new HarnessFault(
      "the terminal stop response is invalid",
      502,
      "terminal_stop_result_invalid",
      "inspect the session and reuse the same request_id only after reconciliation",
    );
  }
  return parsed;
}

export async function fetchHarnessCheckpoints(
  base: string,
  turnId?: string,
  signal?: AbortSignal,
): Promise<HarnessCheckpointPage> {
  const query = new URLSearchParams();
  if (turnId) query.set("turn_id", turnId);
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  const body = await fetchJson(endpoint(base, `/harness/checkpoints${suffix}`), signal);
  const root = record(body);
  const source = Array.isArray(root.checkpoints) ? root.checkpoints : [];
  const checkpoints = source
    .map(checkpoint)
    .filter((item): item is HarnessCheckpoint => item !== null);
  return {
    checkpoints,
    count: integer(root.count) ?? checkpoints.length,
    evidence_class: evidenceClass(root.evidence_class),
  };
}

export async function restoreHarnessCheckpoint(
  base: string,
  checkpointId: string,
  request: HarnessRestoreRequest,
  signal?: AbortSignal,
): Promise<HarnessCheckpoint> {
  const body = await postJson(
    endpoint(base, `/harness/checkpoints/${encodeURIComponent(checkpointId)}/restore`),
    request,
    signal,
  );
  const parsed = checkpoint(body);
  if (parsed === null) {
    throw new HarnessFault(
      "the checkpoint restore response is invalid",
      502,
      "checkpoint_result_invalid",
      "reload checkpoint state before retrying",
    );
  }
  return parsed;
}

export function subscribeHarnessTurn(
  base: string,
  turnId: string,
  afterSeq: number,
  handlers: HarnessStreamHandlers,
): HarnessSubscription {
  const query = new URLSearchParams({
    after_seq: String(Math.max(0, Math.floor(afterSeq))),
    limit: String(MAX_HARNESS_EVENTS),
  });
  const source = new EventSource(
    endpoint(base, `/harness/turns/${encodeURIComponent(turnId)}/stream?${query.toString()}`),
  );
  let closed = false;

  const close = () => {
    if (closed) return;
    closed = true;
    source.removeEventListener("harness_event", eventHandler);
    source.removeEventListener("event_gap", gapHandler);
    source.removeEventListener("harness_error", streamErrorHandler);
    source.close();
  };

  const eventHandler = (message: Event) => {
    if (closed) return;
    const payloadText = (message as MessageEvent<string>).data;
    try {
      const parsed = event(JSON.parse(payloadText), turnId);
      if (parsed !== null) {
        handlers.onEvent(parsed);
        if (parsed.kind === "turn_terminal") close();
      }
    } catch {
      handlers.onError?.(
        new HarnessFault(
          "the Harness stream emitted an invalid event",
          502,
          "invalid_harness_event",
          "reconnect from the last confirmed sequence",
        ),
      );
    }
  };
  const gapHandler = (message: Event) => {
    if (closed) return;
    try {
      const parsed = gap(JSON.parse((message as MessageEvent<string>).data));
      if (parsed !== null) {
        handlers.onGap?.(parsed);
        close();
      }
    } catch {
      handlers.onError?.(
        new HarnessFault(
          "the Harness stream emitted an invalid gap",
          502,
          "invalid_harness_gap",
          "reconnect from the last confirmed sequence",
        ),
      );
    }
  };
  const streamErrorHandler = (message: Event) => {
    if (closed) return;
    let code = "harness_stream_unavailable";
    let remedy = "reload from the last confirmed sequence";
    try {
      const payload = record(JSON.parse((message as MessageEvent<string>).data));
      code = text(payload.error) ?? code;
      remedy = text(payload.remedy) ?? remedy;
    } catch {
      // The fault remains explicit even if the bounded error frame is malformed.
    }
    const fault = new HarnessFault(
      "the Harness event stream became unavailable",
      503,
      code,
      remedy,
    );
    handlers.onTransportFault?.(fault);
    handlers.onError?.(fault);
    close();
  };
  source.addEventListener("harness_event", eventHandler);
  source.addEventListener("event_gap", gapHandler);
  source.addEventListener("harness_error", streamErrorHandler);
  source.onopen = () => {
    // A bounded server window intentionally closes and EventSource reopens
    // from its last event id.  Clear transient transport state on every
    // successful open instead of leaving the UI permanently offline.
    handlers.onTransportFault?.(null);
    handlers.onOpen?.();
  };
  source.onerror = () => {
    if (!closed) {
      handlers.onTransportFault?.(
        new HarnessFault(
          "the Harness event stream is reconnecting",
          503,
          "harness_stream_disconnected",
          "the browser will retry from the last confirmed event id",
        ),
      );
    }
  };

  return {
    close,
  };
}

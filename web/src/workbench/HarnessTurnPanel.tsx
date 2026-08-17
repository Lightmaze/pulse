import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useI18n } from "../i18n";
import {
  fetchHarnessCheckpoints,
  fetchHarnessEvents,
  fetchHarnessSummary,
  fetchHarnessTerminalOutput,
  fetchHarnessTerminalSessions,
  fetchHarnessTurnItems,
  HarnessFault,
  interruptHarnessTurn,
  makeHarnessRequestId,
  resolveHarnessApproval,
  restoreHarnessCheckpoint,
  steerHarnessTurn,
  stopHarnessTerminalSession,
  subscribeHarnessTurn,
  type HarnessApprovalRequest,
  type HarnessControlResponse,
  type HarnessCheckpoint,
  type HarnessCheckpointPage,
  type HarnessEvent,
  type HarnessEventPage,
  type HarnessGap,
  type HarnessInterruptRequest,
  type HarnessSteerRequest,
  type HarnessRestoreRequest,
  type HarnessStreamHandlers,
  type HarnessSubscription,
  type HarnessTerminalOutputChunk,
  type HarnessTerminalOutputGap,
  type HarnessTerminalOutputPage,
  type HarnessTerminalSession,
  type HarnessTerminalSessionPage,
  type HarnessTerminalStopRequest,
  type HarnessTerminalStopResult,
  type HarnessTurnSummary,
  type HarnessTurnItem,
  type HarnessTurnItemPage,
} from "../harness";
import { Icon } from "./Icons";
import "./HarnessTurnPanel.css";
import { zhText } from "../locales/zh-ui.ts";

export interface HarnessTurnPanelProps {
  /** Null keeps the panel mounted while the Workbench has no selected turn. */
  turnId: string | null;
  base: string;
  fetchSummary?: (
    base: string,
    turnId: string,
    signal?: AbortSignal,
  ) => Promise<HarnessTurnSummary>;
  replay?: (
    base: string,
    turnId: string,
    afterSeq?: number,
    limit?: number,
    signal?: AbortSignal,
  ) => Promise<HarnessEventPage>;
  fetchItems?: (
    base: string,
    turnId: string,
    signal?: AbortSignal,
  ) => Promise<HarnessTurnItemPage>;
  subscribe?: (
    base: string,
    turnId: string,
    afterSeq: number,
    handlers: HarnessStreamHandlers,
  ) => HarnessSubscription;
  interrupt?: (
    base: string,
    turnId: string,
    request: HarnessInterruptRequest,
    signal?: AbortSignal,
  ) => Promise<HarnessControlResponse>;
  steer?: (
    base: string,
    turnId: string,
    request: HarnessSteerRequest,
    signal?: AbortSignal,
  ) => Promise<HarnessControlResponse>;
  resolveApproval?: (
    base: string,
    approvalId: string,
    request: HarnessApprovalRequest,
    signal?: AbortSignal,
  ) => Promise<HarnessControlResponse>;
  fetchCheckpoints?: (
    base: string,
    turnId?: string,
    signal?: AbortSignal,
  ) => Promise<HarnessCheckpointPage>;
  restoreCheckpoint?: (
    base: string,
    checkpointId: string,
    request: HarnessRestoreRequest,
    signal?: AbortSignal,
  ) => Promise<HarnessCheckpoint>;
  fetchTerminalSessions?: (
    base: string,
    turnId: string,
    limit?: number,
    signal?: AbortSignal,
  ) => Promise<HarnessTerminalSessionPage>;
  fetchTerminalOutput?: (
    base: string,
    terminalSessionId: string,
    turnId: string,
    afterSeq?: number,
    limit?: number,
    signal?: AbortSignal,
  ) => Promise<HarnessTerminalOutputPage>;
  stopTerminalSession?: (
    base: string,
    terminalSessionId: string,
    request: HarnessTerminalStopRequest,
    signal?: AbortSignal,
  ) => Promise<HarnessTerminalStopResult>;
}

type ConnectionState = "idle" | "loading" | "replaying" | "live" | "offline" | "gap";

type CheckpointOutcomeKind = "restored" | "partial" | "conflict" | "failed" | "uncertain";

interface RestoreConfirmation {
  turnId: string;
  checkpointId: string;
  expectedEpoch: number;
  paths: string[];
}

interface CheckpointOutcome {
  turnId: string;
  checkpointId: string;
  kind: CheckpointOutcomeKind;
  pathCount: number;
  state: string | null;
  code: string | null;
  detail: string | null;
}

type TerminalStopOutcomeKind =
  | "submitting"
  | "pending"
  | "terminal"
  | "conflict"
  | "unavailable"
  | "uncertain"
  | "failed";

interface TerminalOutputView {
  chunks: HarnessTerminalOutputChunk[];
  gap: HarnessTerminalOutputGap | null;
  earliestSeq: number | null;
  nextSeq: number;
  truncated: boolean;
  loading: boolean;
  fault: string | null;
}

interface TerminalStopOutcome {
  turnId: string;
  terminalSessionId: string;
  requestId: string;
  kind: TerminalStopOutcomeKind;
  code: string | null;
  detail: string | null;
}

const TERMINAL_STATES = new Set([
  "settled",
  "completed",
  "complete",
  "interrupted",
  "cancelled",
  "canceled",
  "failed",
  "uncertain",
  "reconciled",
]);

const RESTORABLE_CHECKPOINT_STATES = new Set(["sealed", "partially_restored"]);

const ACTIVE_TERMINAL_STATES = new Set([
  "pending",
  "running",
  "cancel_requested",
  "kill_requested",
]);

const TERMINAL_SESSION_STATES = new Set([
  "exited",
  "failed",
  "timed_out",
  "cancelled",
  "killed",
  "uncertain",
  "denied",
  "unsupported",
]);

const MAX_VISIBLE_TERMINAL_OUTPUT = 500;
const TERMINAL_POLL_INTERVAL_MS = 1_500;

function mergeEvents(current: HarnessEvent[], incoming: HarnessEvent[]): HarnessEvent[] {
  const bySeq = new Map(current.map((item) => [item.seq, item]));
  for (const item of incoming) bySeq.set(item.seq, item);
  return [...bySeq.values()].sort((a, b) => a.seq - b.seq).slice(-500);
}

function eventClass(value: string): string {
  return value.replace(/[^a-zA-Z0-9_-]/g, "-").slice(0, 48);
}

function shortId(value: string | null): string {
  if (value === null || value.length <= 18) return value ?? "—";
  return `${value.slice(0, 8)}…${value.slice(-6)}`;
}

function uniqueCheckpointPaths(paths: string[]): string[] {
  return [...new Set(paths.filter((path) => path !== ""))];
}

function selectableCheckpointPaths(checkpoint: HarnessCheckpoint): string[] {
  const restored = new Set(checkpoint.restored_paths);
  return uniqueCheckpointPaths(checkpoint.changed_paths).filter((path) => !restored.has(path));
}

function selectedCheckpointPaths(
  checkpoint: HarnessCheckpoint,
  selection: string[] | undefined,
): string[] {
  const available = selectableCheckpointPaths(checkpoint);
  if (selection === undefined) return available;
  const selected = new Set(selection);
  return available.filter((path) => selected.has(path));
}

function defaultCheckpointSelections(
  checkpoints: HarnessCheckpoint[],
): Map<string, string[]> {
  return new Map(
    checkpoints.map((checkpoint) => [
      checkpoint.checkpoint_id,
      selectableCheckpointPaths(checkpoint),
    ]),
  );
}

function checkpointStateKind(checkpoint: HarnessCheckpoint): string {
  if (checkpoint.uncertain) return "uncertain";
  return eventClass(checkpoint.state.toLowerCase());
}

function failureOutcomeKind(error: unknown): CheckpointOutcomeKind {
  if (error instanceof HarnessFault) {
    const code = error.code.toLowerCase();
    if (error.status === 503 || code.includes("uncertain")) return "uncertain";
    if (error.status === 409 || code.includes("conflict") || code.includes("stale")) return "conflict";
  }
  return "failed";
}

function resultOutcomeKind(
  checkpoint: HarnessCheckpoint | null,
  requestedPaths: string[],
  error: unknown | null,
): CheckpointOutcomeKind {
  if (checkpoint !== null) {
    const state = checkpoint.state.toLowerCase();
    if (checkpoint.uncertain || state === "uncertain") return "uncertain";
    if (state === "conflict") return "conflict";
    if (state === "failed" || state === "dropped") return "failed";
    const restored = new Set(checkpoint.restored_paths);
    const selectedPathsRestored = requestedPaths.every((path) => restored.has(path));
    if (error === null || selectedPathsRestored) {
      if (state === "partially_restored") return "partial";
      if (state === "restored" || state === "completed" || selectedPathsRestored) return "restored";
    }
  }
  return error === null ? "failed" : failureOutcomeKind(error);
}

function checkpointTime(
  value: string | null,
  formatter: Intl.DateTimeFormat,
  unavailable: string,
): string {
  if (value === null) return unavailable;
  const timestamp = Date.parse(value);
  return Number.isNaN(timestamp) ? value : formatter.format(timestamp);
}

function payloadString(event: HarnessEvent): string | null {
  const payload = event.payload;
  const adapter = payload.adapter_result;
  if (adapter !== null && typeof adapter === "object" && !Array.isArray(adapter)) {
    const preview = (adapter as Record<string, unknown>).diff_preview;
    if (typeof preview === "string") return preview;
  }
  for (const key of [
    "text",
    "reasoning",
    "delta",
    "output_preview",
    "command_preview",
    "diff",
    "reason",
  ]) {
    if (typeof payload[key] === "string") return payload[key] as string;
  }
  const serialized = JSON.stringify(payload, null, 2);
  return serialized === "{}" ? null : serialized.slice(0, 4_000);
}

function approvalId(event: HarnessEvent): string | null {
  const value = event.payload.approval_id;
  return typeof value === "string" && value !== "" ? value : null;
}

function eventTitle(event: HarnessEvent, zh: boolean): string {
  if (!zh) return event.kind.replaceAll("_", " ");
  const labels: Record<string, string> = {
    turn_started: zhText("workbench.HarnessTurnPanel.line325"),
    text_delta: zhText("workbench.HarnessTurnPanel.line326"),
    reasoning_delta: zhText("workbench.HarnessTurnPanel.line327"),
    tool_started: zhText("workbench.HarnessTurnPanel.line328"),
    tool_progress: zhText("workbench.HarnessTurnPanel.line329"),
    tool_completed: zhText("workbench.HarnessTurnPanel.line330"),
    command_started: zhText("workbench.HarnessTurnPanel.line331"),
    command_output: zhText("workbench.HarnessTurnPanel.line332"),
    command_completed: zhText("workbench.HarnessTurnPanel.line333"),
    file_change: zhText("workbench.HarnessTurnPanel.line334"),
    usage: zhText("workbench.HarnessTurnPanel.line335"),
    approval_requested: zhText("workbench.HarnessTurnPanel.line336"),
    approval_resolved: zhText("workbench.HarnessTurnPanel.line337"),
    control_requested: zhText("workbench.HarnessTurnPanel.line338"),
    control_resolved: zhText("workbench.HarnessTurnPanel.line339"),
    subagent_activity: zhText("workbench.HarnessTurnPanel.line340"),
    turn_terminal: zhText("workbench.HarnessTurnPanel.line341"),
    warning: zhText("workbench.HarnessTurnPanel.line342"),
    event_gap: zhText("workbench.HarnessTurnPanel.line343"),
  };
  return labels[event.kind] ?? event.kind;
}

function turnItemTitle(item: HarnessTurnItem, zh: boolean): string {
  if (item.tool_name !== null) return item.tool_name;
  const labels: Record<string, string> = zh
    ? {
        assistant: zhText("workbench.HarnessTurnPanel.line352"),
        command: zhText("workbench.HarnessTurnPanel.line353"),
        file_change: zhText("workbench.HarnessTurnPanel.line354"),
        tool_call: zhText("workbench.HarnessTurnPanel.line355"),
        control: zhText("workbench.HarnessTurnPanel.line356"),
        subagent: zhText("workbench.HarnessTurnPanel.line357"),
        event: zhText("workbench.HarnessTurnPanel.line358"),
      }
    : {
        assistant: "Assistant output",
        command: "Terminal command",
        file_change: "File change",
        tool_call: "Tool call",
        control: "Control action",
        subagent: "Temporary worker",
        event: "Runtime event",
      };
  return labels[item.item_type] ?? item.item_type.replaceAll("_", " ");
}

function errorText(error: unknown): string {
  if (error !== null && typeof error === "object" && "message" in error) {
    const message = (error as { message: unknown }).message;
    if (typeof message === "string") return message;
  }
  return String(error);
}

function terminalStateIsActive(state: string): boolean {
  return ACTIVE_TERMINAL_STATES.has(state.toLowerCase());
}

function terminalStateIsTerminal(state: string): boolean {
  return TERMINAL_SESSION_STATES.has(state.toLowerCase());
}

function terminalStateIsUncertain(session: HarnessTerminalSession): boolean {
  return session.state.toLowerCase() === "uncertain" || session.uncertain_reason !== null;
}

function mergeTerminalSessions(
  current: HarnessTerminalSession[],
  incoming: HarnessTerminalSession[],
): HarnessTerminalSession[] {
  const currentById = new Map(current.map((session) => [session.terminal_session_id, session]));
  return incoming.map((session) => {
    const prior = currentById.get(session.terminal_session_id);
    if (
      prior !== undefined
      && terminalStateIsTerminal(prior.state)
      && terminalStateIsActive(session.state)
    ) return prior;
    return session;
  });
}

function emptyTerminalOutput(): TerminalOutputView {
  return {
    chunks: [],
    gap: null,
    earliestSeq: null,
    nextSeq: 0,
    truncated: false,
    loading: true,
    fault: null,
  };
}

function mergeTerminalOutput(
  current: TerminalOutputView | undefined,
  page: HarnessTerminalOutputPage,
): TerminalOutputView {
  const bySeq = new Map<number, HarnessTerminalOutputChunk>();
  for (const chunk of current?.chunks ?? []) bySeq.set(chunk.seq, chunk);
  for (const chunk of page.output) bySeq.set(chunk.seq, chunk);
  const ordered = [...bySeq.values()].sort((left, right) => left.seq - right.seq);
  const chunks = ordered.slice(-MAX_VISIBLE_TERMINAL_OUTPUT);
  const clientPruned = ordered.length > chunks.length;
  const firstVisibleSeq = chunks.at(0)?.seq ?? null;
  const priorEarliestSeq = current?.earliestSeq ?? page.earliest_seq ?? ordered.at(0)?.seq ?? null;
  const clientGap = clientPruned && priorEarliestSeq !== null && firstVisibleSeq !== null
    ? {
        missing_from: priorEarliestSeq,
        missing_to: Math.max(priorEarliestSeq, firstVisibleSeq - 1),
        reason: "client_bounded_window",
      }
    : null;
  return {
    chunks,
    gap: page.gap ?? clientGap ?? current?.gap ?? null,
    earliestSeq: firstVisibleSeq ?? page.earliest_seq ?? current?.earliestSeq ?? null,
    nextSeq: Math.max(page.next_seq, current?.nextSeq ?? 0, chunks.at(-1)?.seq ?? 0),
    truncated:
      page.truncated
      || (current?.truncated ?? false)
      || clientPruned,
    loading: false,
    fault: null,
  };
}

function terminalStopOutcomeLabel(kind: TerminalStopOutcomeKind, zh: boolean): string {
  const labels: Record<TerminalStopOutcomeKind, string> = zh
    ? {
        submitting: zhText("workbench.HarnessTurnPanel.line456"),
        pending: zhText("workbench.HarnessTurnPanel.line457"),
        terminal: zhText("workbench.HarnessTurnPanel.line458"),
        conflict: zhText("workbench.HarnessTurnPanel.line459"),
        unavailable: zhText("workbench.HarnessTurnPanel.line460"),
        uncertain: zhText("workbench.HarnessTurnPanel.line461"),
        failed: zhText("workbench.HarnessTurnPanel.line462"),
      }
    : {
        submitting: "Requesting stop",
        pending: "Stop requested; awaiting exit evidence",
        terminal: "Stop outcome confirmed",
        conflict: "Session state changed",
        unavailable: "Stop backend unavailable",
        uncertain: "Stop outcome uncertain",
        failed: "Stop request failed",
      };
  return labels[kind];
}

function terminalStopOutcomeMessage(outcome: TerminalStopOutcome, zh: boolean): string {
  if (outcome.kind === "pending") {
    return zh
      ? zhText("workbench.HarnessTurnPanel.line479")
      : "The server durably accepted the stop intent. The session is not yet reported as stopped; inspection continues.";
  }
  if (outcome.kind === "terminal") {
    return zh
      ? zhText("workbench.HarnessTurnPanel.line484")
      : "Durable state is terminal; final exit and evidence remain visible.";
  }
  if (outcome.kind === "conflict") {
    return zh
      ? zhText("workbench.HarnessTurnPanel.line489")
      : "The turn, epoch, or session state drifted. State was refreshed without automatically retrying the side effect.";
  }
  if (outcome.kind === "unavailable") {
    return zh
      ? zhText("workbench.HarnessTurnPanel.line494")
      : "Stop capability cannot currently be proven. The session is not shown as stopped and the request is not retried automatically.";
  }
  if (outcome.kind === "uncertain") {
    return zh
      ? zhText("workbench.HarnessTurnPanel.line499")
      : "Process-tree exit could not be proven. Treat UNCERTAIN and durable recovery evidence as authoritative.";
  }
  if (outcome.kind === "failed") {
    return zh
      ? zhText("workbench.HarnessTurnPanel.line504")
      : "The request produced no confirmable outcome. Its error code is retained and it will not be retried automatically.";
  }
  return zh ? zhText("workbench.HarnessTurnPanel.line507") : "Sending the scoped stop request.";
}

function statusIsTerminal(summary: HarnessTurnSummary | null): boolean {
  if (summary === null) return false;
  return summary.terminal || TERMINAL_STATES.has(summary.state.toLowerCase());
}

function connectionLabel(state: ConnectionState, zh: boolean): string {
  const labels: Record<ConnectionState, string> = zh
    ? {
        idle: zhText("workbench.HarnessTurnPanel.line518"),
        loading: zhText("workbench.HarnessTurnPanel.line519"),
        replaying: zhText("workbench.HarnessTurnPanel.line520"),
        live: zhText("workbench.HarnessTurnPanel.line521"),
        offline: zhText("workbench.HarnessTurnPanel.line522"),
        gap: zhText("workbench.HarnessTurnPanel.line523"),
      }
    : {
        idle: "No turn selected",
        loading: "Loading",
        replaying: "Replaying",
        live: "Following live",
        offline: "Live unavailable",
        gap: "Replay gap",
      };
  return labels[state];
}

function controlStateLabel(
  result: HarnessControlResponse,
  zh: boolean,
): string {
  if (result.uncertain) return zh ? zhText("workbench.HarnessTurnPanel.line540") : "Outcome uncertain";
  if (result.accepted) return result.idempotent ? (zh ? zhText("workbench.HarnessTurnPanel.line541") : "Idempotent") : (zh ? zhText("workbench.HarnessTurnPanel.line541.2") : "Accepted");
  return result.error_code ?? (zh ? zhText("workbench.HarnessTurnPanel.line542") : "Not executed");
}

function checkpointOutcomeLabel(kind: CheckpointOutcomeKind, zh: boolean): string {
  const labels: Record<CheckpointOutcomeKind, string> = zh
    ? {
        restored: zhText("workbench.HarnessTurnPanel.line548"),
        partial: zhText("workbench.HarnessTurnPanel.line549"),
        conflict: zhText("workbench.HarnessTurnPanel.line550"),
        failed: zhText("workbench.HarnessTurnPanel.line551"),
        uncertain: zhText("workbench.HarnessTurnPanel.line552"),
      }
    : {
        restored: "Restore confirmed",
        partial: "Partial restore confirmed",
        conflict: "Restore conflict",
        failed: "Restore failed",
        uncertain: "Restore outcome uncertain",
      };
  return labels[kind];
}

function checkpointOutcomeMessage(outcome: CheckpointOutcome, zh: boolean): string {
  if (outcome.kind === "restored") {
    return zh
      ? (zhText("workbench.HarnessTurnPanel.line567.head") + String(outcome.pathCount) + zhText("workbench.HarnessTurnPanel.line567.tail1"))
      : `${outcome.pathCount} path(s) verified; refreshing turn replay.`;
  }
  if (outcome.kind === "partial") {
    return zh
      ? (zhText("workbench.HarnessTurnPanel.line572.head") + String(outcome.pathCount) + zhText("workbench.HarnessTurnPanel.line572.tail1"))
      : `Restored ${outcome.pathCount} selected path(s); remaining paths stay checkpointed.`;
  }
  if (outcome.code === "restore_scope_stale") {
    return zh
      ? zhText("workbench.HarnessTurnPanel.line577")
      : "The turn, epoch, checkpoint state, or path scope changed. Review again; nothing was sent automatically.";
  }
  if (outcome.kind === "uncertain") {
    return zh
      ? zhText("workbench.HarnessTurnPanel.line582")
      : "The side effect could not be confirmed. Durable state was refreshed and the request will not be retried automatically.";
  }
  if (outcome.kind === "conflict") {
    return zh
      ? zhText("workbench.HarnessTurnPanel.line587")
      : "The checkpoint post-image no longer matches the workspace. Inspect changes before a new operation.";
  }
  return zh
    ? zhText("workbench.HarnessTurnPanel.line591")
    : "Restore did not complete. Checkpoint state and turn replay were refreshed without an automatic retry.";
}

export function HarnessTurnPanel({
  turnId,
  base,
  fetchSummary: fetchSummaryOverride,
  replay: replayOverride,
  fetchItems: fetchItemsOverride,
  subscribe: subscribeOverride,
  interrupt: interruptOverride,
  steer: steerOverride,
  resolveApproval: resolveApprovalOverride,
  fetchCheckpoints: fetchCheckpointsOverride,
  restoreCheckpoint: restoreCheckpointOverride,
  fetchTerminalSessions: fetchTerminalSessionsOverride,
  fetchTerminalOutput: fetchTerminalOutputOverride,
  stopTerminalSession: stopTerminalSessionOverride,
}: HarnessTurnPanelProps) {
  const { locale } = useI18n();
  const zh = locale === "zh-CN";
  const [summary, setSummary] = useState<HarnessTurnSummary | null>(null);
  const [events, setEvents] = useState<HarnessEvent[]>([]);
  const [turnItems, setTurnItems] = useState<HarnessTurnItem[]>([]);
  const [itemsTruncated, setItemsTruncated] = useState(false);
  const [gap, setGap] = useState<HarnessGap | null>(null);
  const [connection, setConnection] = useState<ConnectionState>("idle");
  const [fault, setFault] = useState<string | null>(null);
  const [steerDraft, setSteerDraft] = useState("");
  const [controlBusy, setControlBusy] = useState(false);
  const [controlResult, setControlResult] = useState<HarnessControlResponse | null>(null);
  const [submittedApprovals, setSubmittedApprovals] = useState<Set<string>>(() => new Set());
  const [checkpoints, setCheckpoints] = useState<HarnessCheckpoint[]>([]);
  const [checkpointViewTurnId, setCheckpointViewTurnId] = useState<string | null>(null);
  const [checkpointPathSelections, setCheckpointPathSelections] = useState<Map<string, string[]>>(
    () => new Map(),
  );
  const [restoreConfirmation, setRestoreConfirmation] = useState<RestoreConfirmation | null>(null);
  const [checkpointOutcome, setCheckpointOutcome] = useState<CheckpointOutcome | null>(null);
  const [checkpointFault, setCheckpointFault] = useState<string | null>(null);
  const [checkpointBusy, setCheckpointBusy] = useState<string | null>(null);
  const [terminalSessions, setTerminalSessions] = useState<HarnessTerminalSession[]>([]);
  const [terminalOutputs, setTerminalOutputs] = useState<Record<string, TerminalOutputView>>({});
  const [terminalFault, setTerminalFault] = useState<string | null>(null);
  const [terminalLoading, setTerminalLoading] = useState(false);
  const [terminalStopOutcomes, setTerminalStopOutcomes] = useState<Record<string, TerminalStopOutcome>>({});
  const [revision, setRevision] = useState(0);
  const restoreContextRef = useRef(0);
  const restoreAttemptRef = useRef<symbol | null>(null);
  const terminalRefreshRef = useRef<(() => void) | null>(null);
  const terminalSessionsRef = useRef<HarnessTerminalSession[]>([]);
  const terminalStopOutcomesRef = useRef<Record<string, TerminalStopOutcome>>({});
  const terminalStopControllersRef = useRef<Map<string, AbortController>>(new Map());
  const terminalScopeKey = turnId === null || turnId === "" ? null : `${base}\u0000${turnId}`;
  const terminalScopeKeyRef = useRef<string | null>(terminalScopeKey);
  terminalScopeKeyRef.current = terminalScopeKey;
  terminalSessionsRef.current = terminalSessions;
  terminalStopOutcomesRef.current = terminalStopOutcomes;

  const checkpointDateFormatter = useMemo(
    () => new Intl.DateTimeFormat(locale, {
      year: "numeric",
      month: "short",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    }),
    [locale],
  );

  const terminalNumberFormatter = useMemo(
    () => new Intl.NumberFormat(locale),
    [locale],
  );

  const loadSummary = useMemo(
    () => fetchSummaryOverride ?? fetchHarnessSummary,
    [fetchSummaryOverride],
  );
  const loadReplay = useMemo(
    () => replayOverride ?? fetchHarnessEvents,
    [replayOverride],
  );
  const loadTurnItems = useMemo(
    () => fetchItemsOverride ?? fetchHarnessTurnItems,
    [fetchItemsOverride],
  );
  const openStream = useMemo(
    () => subscribeOverride ?? subscribeHarnessTurn,
    [subscribeOverride],
  );
  const sendInterrupt = useMemo(
    () => interruptOverride ?? interruptHarnessTurn,
    [interruptOverride],
  );
  const sendSteer = useMemo(
    () => steerOverride ?? steerHarnessTurn,
    [steerOverride],
  );
  const sendApproval = useMemo(
    () => resolveApprovalOverride ?? resolveHarnessApproval,
    [resolveApprovalOverride],
  );
  const loadCheckpoints = useMemo(
    () => fetchCheckpointsOverride ?? fetchHarnessCheckpoints,
    [fetchCheckpointsOverride],
  );
  const sendRestore = useMemo(
    () => restoreCheckpointOverride ?? restoreHarnessCheckpoint,
    [restoreCheckpointOverride],
  );
  const loadTerminalSessions = useMemo(
    () => fetchTerminalSessionsOverride ?? fetchHarnessTerminalSessions,
    [fetchTerminalSessionsOverride],
  );
  const loadTerminalOutput = useMemo(
    () => fetchTerminalOutputOverride ?? fetchHarnessTerminalOutput,
    [fetchTerminalOutputOverride],
  );
  const sendTerminalStop = useMemo(
    () => stopTerminalSessionOverride ?? stopHarnessTerminalSession,
    [stopTerminalSessionOverride],
  );

  useEffect(() => () => {
    for (const controller of terminalStopControllersRef.current.values()) controller.abort();
    terminalStopControllersRef.current.clear();
  }, [terminalScopeKey]);

  useEffect(() => {
    if (turnId === null || turnId === "") {
      terminalRefreshRef.current = null;
      setTerminalSessions([]);
      setTerminalOutputs({});
      setTerminalFault(null);
      setTerminalLoading(false);
      setTerminalStopOutcomes({});
      return;
    }

    const scopedTurnId = turnId;
    const scopedKey = `${base}\u0000${scopedTurnId}`;
    const controller = new AbortController();
    const cursors = new Map<string, number>();
    let active = true;
    let refreshing = false;
    let refreshAgain = false;
    let refreshFailed = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let latestSessions: HarnessTerminalSession[] = [];

    setTerminalSessions([]);
    setTerminalOutputs({});
    setTerminalFault(null);
    setTerminalLoading(true);
    setTerminalStopOutcomes((current) => Object.fromEntries(
      Object.entries(current).filter(([, outcome]) => outcome.turnId === scopedTurnId),
    ));

    const schedule = (delay: number) => {
      if (!active || controller.signal.aborted) return;
      if (timer !== null) clearTimeout(timer);
      timer = setTimeout(() => {
        timer = null;
        void refresh();
      }, delay);
    };

    const readOutput = async (session: HarnessTerminalSession) => {
      const sessionId = session.terminal_session_id;
      let afterSeq = cursors.get(sessionId) ?? 0;
      let pageCount = 0;
      setTerminalOutputs((current) => ({
        ...current,
        [sessionId]: current[sessionId] ?? emptyTerminalOutput(),
      }));
      try {
        while (active && !controller.signal.aborted && pageCount < 3) {
          const page = await loadTerminalOutput(
            base,
            sessionId,
            scopedTurnId,
            afterSeq,
            200,
            controller.signal,
          );
          if (!active || terminalScopeKeyRef.current !== scopedKey) return;
          setTerminalOutputs((current) => ({
            ...current,
            [sessionId]: mergeTerminalOutput(current[sessionId], page),
          }));
          const nextSeq = Math.max(afterSeq, page.next_seq);
          cursors.set(sessionId, nextSeq);
          pageCount += 1;
          if (!page.has_more || nextSeq <= afterSeq) break;
          afterSeq = nextSeq;
        }
      } catch (cause) {
        if (!active || controller.signal.aborted || terminalScopeKeyRef.current !== scopedKey) return;
        setTerminalOutputs((current) => ({
          ...current,
          [sessionId]: {
            ...(current[sessionId] ?? emptyTerminalOutput()),
            loading: false,
            fault: errorText(cause),
          },
        }));
      }
    };

    const refresh = async () => {
      if (!active || controller.signal.aborted) return;
      if (refreshing) {
        refreshAgain = true;
        return;
      }
      refreshing = true;
      refreshAgain = false;
      try {
        const page = await loadTerminalSessions(base, scopedTurnId, 64, controller.signal);
        if (!active || terminalScopeKeyRef.current !== scopedKey) return;
        latestSessions = page.sessions;
        refreshFailed = false;
        const retainedIds = new Set(page.sessions.map((session) => session.terminal_session_id));
        setTerminalSessions((current) => mergeTerminalSessions(current, page.sessions));
        setTerminalFault(null);
        setTerminalOutputs((current) => Object.fromEntries(
          Object.entries(current).filter(([sessionId]) => retainedIds.has(sessionId)),
        ));
        await Promise.all(page.sessions.map((session) => readOutput(session)));
        if (!active || terminalScopeKeyRef.current !== scopedKey) return;
        setTerminalStopOutcomes((current) => {
          let changed = false;
          const next = { ...current };
          for (const session of page.sessions) {
            const outcome = next[session.terminal_session_id];
            if (
              outcome === undefined
              || outcome.turnId !== scopedTurnId
              || (outcome.kind !== "pending" && outcome.kind !== "submitting")
              || !terminalStateIsTerminal(session.state)
            ) continue;
            next[session.terminal_session_id] = {
              ...outcome,
              kind: terminalStateIsUncertain(session) ? "uncertain" : "terminal",
              code: session.error_code,
              detail: session.uncertain_reason,
            };
            changed = true;
          }
          return changed ? next : current;
        });
      } catch (cause) {
        if (!active || controller.signal.aborted || terminalScopeKeyRef.current !== scopedKey) return;
        refreshFailed = true;
        setTerminalFault(errorText(cause));
      } finally {
        refreshing = false;
        if (!active || controller.signal.aborted) return;
        setTerminalLoading(false);
        if (refreshAgain) schedule(80);
        else if (refreshFailed) schedule(TERMINAL_POLL_INTERVAL_MS);
        else if (latestSessions.some((session) => terminalStateIsActive(session.state))) {
          schedule(TERMINAL_POLL_INTERVAL_MS);
        }
      }
    };

    terminalRefreshRef.current = () => schedule(80);
    void refresh();

    return () => {
      active = false;
      controller.abort();
      if (timer !== null) clearTimeout(timer);
      if (terminalRefreshRef.current !== null) terminalRefreshRef.current = null;
    };
  }, [base, loadTerminalOutput, loadTerminalSessions, revision, turnId]);

  useEffect(() => {
    restoreContextRef.current += 1;
    setRestoreConfirmation(null);
    setCheckpointBusy(null);
    setCheckpointViewTurnId(null);
    setCheckpointPathSelections(new Map());
    setCheckpointOutcome((current) => (
      current !== null && current.turnId !== turnId ? null : current
    ));
    if (turnId === null || turnId === "") {
      setSummary(null);
      setEvents([]);
      setTurnItems([]);
      setItemsTruncated(false);
      setGap(null);
      setFault(null);
      setSubmittedApprovals(new Set());
      setCheckpoints([]);
      setCheckpointFault(null);
      setConnection("idle");
      return;
    }
    const controller = new AbortController();
    let active = true;
    let subscription: HarnessSubscription | null = null;
    let itemRefreshTimer: ReturnType<typeof setTimeout> | null = null;
    setSummary(null);
    setEvents([]);
    setTurnItems([]);
    setItemsTruncated(false);
    setGap(null);
    setFault(null);
    setSubmittedApprovals(new Set());
    setCheckpoints([]);
    setCheckpointFault(null);
    setConnection("loading");

    void (async () => {
      try {
        const nextSummary = await loadSummary(base, turnId, controller.signal);
        if (!active) return;
        setSummary(nextSummary);
        setConnection("replaying");
        const page = await loadReplay(base, turnId, 0, 500, controller.signal);
        if (!active) return;
        setEvents(page.events);
        setGap(page.gap);
        if (page.gap !== null) setConnection("gap");
        const cursor = page.events.at(-1)?.seq ?? page.next_seq;
        const initialItems = await loadTurnItems(base, turnId, controller.signal);
        if (!active) return;
        setTurnItems(initialItems.items);
        setItemsTruncated(initialItems.truncated || initialItems.has_more);
        const scheduleItemRefresh = () => {
          if (itemRefreshTimer !== null) clearTimeout(itemRefreshTimer);
          itemRefreshTimer = setTimeout(() => {
            itemRefreshTimer = null;
            void loadTurnItems(base, turnId, controller.signal)
              .then((nextItems) => {
                if (!active) return;
                setTurnItems(nextItems.items);
                setItemsTruncated(nextItems.truncated || nextItems.has_more);
              })
              .catch((cause) => {
                if (active && !controller.signal.aborted) setFault(errorText(cause));
              });
          }, 180);
        };
        subscription = openStream(base, turnId, cursor, {
          onEvent: (nextEvent) => {
            if (!active) return;
            setEvents((current) => mergeEvents(current, [nextEvent]));
            setSummary((current) => {
              if (current === null) return current;
              return {
                ...current,
                state: nextEvent.kind === "turn_terminal" ? nextEvent.status : current.state,
                terminal: nextEvent.kind === "turn_terminal" || current.terminal,
                event_cursor: {
                  ...current.event_cursor,
                  last_seq: nextEvent.seq,
                  next_seq: nextEvent.seq + 1,
                  has_gap: current.event_cursor.has_gap ?? false,
                },
              };
            });
            scheduleItemRefresh();
            if (nextEvent.kind.startsWith("command_")) terminalRefreshRef.current?.();
            setConnection("live");
          },
          onGap: (nextGap) => {
            if (!active) return;
            setGap(nextGap);
            setConnection("gap");
          },
          onOpen: () => {
            if (active) {
              setConnection("live");
              terminalRefreshRef.current?.();
            }
          },
          onError: (cause: HarnessFault) => {
            if (active) {
              setFault(errorText(cause));
              setConnection("offline");
            }
          },
          onTransportFault: (cause) => {
            if (!active) return;
            if (cause === null) {
              setConnection("live");
              terminalRefreshRef.current?.();
            } else {
              setConnection("offline");
            }
          },
        });
        try {
          const checkpointPage = await loadCheckpoints(base, turnId, controller.signal);
          if (active) {
            setCheckpoints(checkpointPage.checkpoints);
            setCheckpointPathSelections(defaultCheckpointSelections(checkpointPage.checkpoints));
            setCheckpointViewTurnId(turnId);
          }
        } catch (cause) {
          if (active && !controller.signal.aborted) {
            const code = cause instanceof HarnessFault ? cause.code : "checkpoint_unavailable";
            if (code !== "checkpoint_unavailable") setCheckpointFault(errorText(cause));
          }
        }
      } catch (cause) {
        if (!active || controller.signal.aborted) return;
        setFault(errorText(cause));
        setConnection("offline");
      }
    })();

    return () => {
      active = false;
      restoreContextRef.current += 1;
      controller.abort();
      if (itemRefreshTimer !== null) clearTimeout(itemRefreshTimer);
      subscription?.close();
    };
  }, [base, loadCheckpoints, loadReplay, loadSummary, loadTurnItems, openStream, revision, turnId]);

  const canControl = summary !== null && summary.live_available && !statusIsTerminal(summary) && summary.epoch !== null;

  const runControl = useCallback(
    async (operation: "interrupt" | "steer", message?: string) => {
      if (turnId === null || summary === null || summary.epoch === null || controlBusy) return;
      setControlBusy(true);
      setControlResult(null);
      setFault(null);
      const requestId = makeHarnessRequestId(operation);
      try {
        const result = operation === "interrupt"
          ? await sendInterrupt(base, turnId, {
              request_id: requestId,
              expected_epoch: summary.epoch,
              expected_state: summary.state,
              reason: message,
            })
          : await sendSteer(base, turnId, {
              request_id: requestId,
              expected_epoch: summary.epoch,
              expected_state: summary.state,
              message: message ?? "",
            });
        setControlResult(result);
        if (result.uncertain) setConnection("offline");
        if (operation === "steer") setSteerDraft("");
      } catch (cause) {
        setFault(errorText(cause));
      } finally {
        setControlBusy(false);
      }
    },
    [base, controlBusy, sendInterrupt, sendSteer, summary, turnId],
  );

  const resolveApproval = useCallback(
    async (id: string, decision: HarnessApprovalRequest["decision"]) => {
      if (turnId === null || summary === null || summary.epoch === null || controlBusy || submittedApprovals.has(id)) return;
      setControlBusy(true);
      setSubmittedApprovals((current) => new Set(current).add(id));
      setControlResult(null);
      setFault(null);
      const requestId = makeHarnessRequestId("approval");
      try {
        const result = await sendApproval(base, id, {
          request_id: requestId,
          expected_epoch: summary.epoch,
          expected_state: summary.state,
          expected_turn_id: turnId,
          decision,
        });
        setControlResult(result);
        if (!result.accepted && !result.idempotent) {
          setSubmittedApprovals((current) => {
            const next = new Set(current);
            next.delete(id);
            return next;
          });
        }
      } catch (cause) {
        setFault(errorText(cause));
        setSubmittedApprovals((current) => {
          const next = new Set(current);
          next.delete(id);
          return next;
        });
      } finally {
        setControlBusy(false);
      }
    },
    [base, controlBusy, sendApproval, submittedApprovals, summary, turnId],
  );

  const stopTerminal = useCallback(
    async (session: HarnessTerminalSession) => {
      if (
        turnId === null
        || turnId === ""
        || session.turn_id !== turnId
        || session.epoch === null
        || !session.capabilities.stop
        || !terminalStateIsActive(session.state)
      ) return;
      const scopeKey = `${base}\u0000${turnId}`;
      const existing = terminalStopOutcomesRef.current[session.terminal_session_id];
      const requestId = existing !== undefined
        && existing.turnId === turnId
        && existing.terminalSessionId === session.terminal_session_id
        && (existing.kind === "pending" || existing.kind === "submitting")
        ? existing.requestId
        : makeHarnessRequestId("terminal-stop");
      const requestKey = `${turnId}\u0000${session.terminal_session_id}\u0000${requestId}`;
      if (terminalStopControllersRef.current.has(requestKey)) return;

      const controller = new AbortController();
      terminalStopControllersRef.current.set(requestKey, controller);
      setTerminalStopOutcomes((current) => ({
        ...current,
        [session.terminal_session_id]: {
          turnId,
          terminalSessionId: session.terminal_session_id,
          requestId,
          kind: "submitting",
          code: null,
          detail: null,
        },
      }));
      try {
        const result = await sendTerminalStop(
          base,
          session.terminal_session_id,
          {
            request_id: requestId,
            expected_epoch: session.epoch,
            expected_turn_id: turnId,
            expected_state: session.state,
            reason: zh ? zhText("workbench.HarnessTurnPanel.line1133") : "User requested background pipe-session stop",
          },
          controller.signal,
        );
        if (terminalScopeKeyRef.current !== scopeKey || controller.signal.aborted) return;
        const prior = terminalSessionsRef.current.find(
          (item) => item.terminal_session_id === result.terminal_session_id,
        );
        const effectiveResult = prior !== undefined
          && terminalStateIsTerminal(prior.state)
          && terminalStateIsActive(result.state)
          ? prior
          : result;
        setTerminalSessions((current) => current.map((item) => (
          item.terminal_session_id === result.terminal_session_id
          && !(terminalStateIsTerminal(item.state) && terminalStateIsActive(effectiveResult.state))
            ? effectiveResult
            : item
        )));
        const resultKind: TerminalStopOutcomeKind = terminalStateIsUncertain(effectiveResult)
          ? "uncertain"
          : terminalStateIsTerminal(effectiveResult.state)
            ? "terminal"
            : result.accepted
              ? "pending"
              : "failed";
        setTerminalStopOutcomes((current) => ({
          ...current,
          [session.terminal_session_id]: {
            turnId,
            terminalSessionId: session.terminal_session_id,
            requestId,
            kind: resultKind,
            code: effectiveResult.error_code,
            detail: effectiveResult.uncertain_reason,
          },
        }));
        terminalRefreshRef.current?.();
      } catch (cause) {
        if (
          controller.signal.aborted
          || terminalScopeKeyRef.current !== scopeKey
          || (cause instanceof Error && cause.name === "AbortError")
        ) return;
        const kind: TerminalStopOutcomeKind = cause instanceof HarnessFault && cause.status === 409
          ? "conflict"
          : cause instanceof HarnessFault && cause.status === 503
            ? "unavailable"
            : cause instanceof HarnessFault && cause.code.toLowerCase().includes("uncertain")
              ? "uncertain"
              : "failed";
        setTerminalStopOutcomes((current) => ({
          ...current,
          [session.terminal_session_id]: {
            turnId,
            terminalSessionId: session.terminal_session_id,
            requestId,
            kind,
            code: cause instanceof HarnessFault ? cause.code : null,
            detail: errorText(cause),
          },
        }));
        terminalRefreshRef.current?.();
      } finally {
        if (terminalStopControllersRef.current.get(requestKey) === controller) {
          terminalStopControllersRef.current.delete(requestKey);
        }
      }
    },
    [base, sendTerminalStop, turnId, zh],
  );

  const toggleCheckpointPath = useCallback(
    (checkpoint: HarnessCheckpoint, path: string, checked: boolean) => {
      const available = selectableCheckpointPaths(checkpoint);
      if (!available.includes(path)) return;
      setCheckpointPathSelections((current) => {
        const next = new Map(current);
        const selected = new Set(selectedCheckpointPaths(checkpoint, current.get(checkpoint.checkpoint_id)));
        if (checked) selected.add(path);
        else selected.delete(path);
        next.set(
          checkpoint.checkpoint_id,
          available.filter((candidate) => selected.has(candidate)),
        );
        return next;
      });
      setRestoreConfirmation((current) => (
        current?.checkpointId === checkpoint.checkpoint_id ? null : current
      ));
      setCheckpointOutcome((current) => (
        current?.checkpointId === checkpoint.checkpoint_id ? null : current
      ));
    },
    [],
  );

  const beginCheckpointRestore = useCallback(
    (checkpoint: HarnessCheckpoint) => {
      if (
        turnId === null
        || checkpointViewTurnId !== turnId
        || summary === null
        || summary.epoch === null
        || !summary.live_available
        || checkpointBusy !== null
        || checkpoint.uncertain
        || !RESTORABLE_CHECKPOINT_STATES.has(checkpoint.state.toLowerCase())
      ) return;
      const paths = selectedCheckpointPaths(
        checkpoint,
        checkpointPathSelections.get(checkpoint.checkpoint_id),
      );
      if (paths.length === 0) return;
      setCheckpointFault(null);
      setCheckpointOutcome(null);
      setRestoreConfirmation({
        turnId,
        checkpointId: checkpoint.checkpoint_id,
        expectedEpoch: summary.epoch,
        paths,
      });
    },
    [
      checkpointBusy,
      checkpointPathSelections,
      checkpointViewTurnId,
      summary,
      turnId,
    ],
  );

  const submitCheckpointRestore = useCallback(async () => {
    const confirmation = restoreConfirmation;
    if (
      restoreAttemptRef.current !== null
      ||
      confirmation === null
      || turnId === null
      || confirmation.turnId !== turnId
      || checkpointViewTurnId !== turnId
      || summary === null
      || summary.epoch === null
      || checkpointBusy !== null
    ) return;

    const checkpoint = checkpoints.find(
      (item) => item.checkpoint_id === confirmation.checkpointId,
    );
    const available = checkpoint === undefined ? [] : selectableCheckpointPaths(checkpoint);
    const availableSet = new Set(available);
    const requestedPaths = [...new Set(confirmation.paths)];
    const scopeIsValid = checkpoint !== undefined
      && requestedPaths.length > 0
      && requestedPaths.length === confirmation.paths.length
      && requestedPaths.every((path) => availableSet.has(path));
    const contextIsValid = scopeIsValid
      && summary.live_available
      && summary.epoch === confirmation.expectedEpoch
      && checkpoint !== undefined
      && !checkpoint.uncertain
      && RESTORABLE_CHECKPOINT_STATES.has(checkpoint.state.toLowerCase());

    if (!contextIsValid) {
      setRestoreConfirmation(null);
      setCheckpointOutcome({
        turnId,
        checkpointId: confirmation.checkpointId,
        kind: "conflict",
        pathCount: requestedPaths.length,
        state: checkpoint?.state ?? null,
        code: "restore_scope_stale",
        detail: null,
      });
      return;
    }

    const contextVersion = restoreContextRef.current;
    const attempt = Symbol("checkpoint-restore");
    restoreAttemptRef.current = attempt;
    const requestTurnId = turnId;
    setCheckpointBusy(confirmation.checkpointId);
    setCheckpointFault(null);
    let restoredCheckpoint: HarnessCheckpoint | null = null;
    let requestError: unknown | null = null;
    let refreshError: unknown | null = null;
    try {
      try {
        restoredCheckpoint = await sendRestore(base, confirmation.checkpointId, {
          request_id: makeHarnessRequestId("restore"),
          expected_epoch: confirmation.expectedEpoch,
          changed_paths: requestedPaths,
        });
      } catch (cause) {
        requestError = cause;
      }
      if (restoreContextRef.current !== contextVersion) return;

      if (restoredCheckpoint !== null) {
        const returnedCheckpoint = restoredCheckpoint;
        setCheckpoints((current) => current.map((item) => (
          item.checkpoint_id === returnedCheckpoint.checkpoint_id ? returnedCheckpoint : item
        )));
      }

      try {
        const checkpointPage = await loadCheckpoints(base, requestTurnId);
        if (restoreContextRef.current !== contextVersion) return;
        setCheckpoints(checkpointPage.checkpoints);
        setCheckpointPathSelections(defaultCheckpointSelections(checkpointPage.checkpoints));
        setCheckpointViewTurnId(requestTurnId);
        restoredCheckpoint = checkpointPage.checkpoints.find(
          (item) => item.checkpoint_id === confirmation.checkpointId,
        ) ?? restoredCheckpoint;
      } catch (cause) {
        refreshError = cause;
        if (requestError === null) setCheckpointFault(errorText(cause));
      }
      if (restoreContextRef.current !== contextVersion) return;

      const kind = resultOutcomeKind(restoredCheckpoint, requestedPaths, requestError);
      const failedOutcome = kind === "conflict" || kind === "failed" || kind === "uncertain";
      setCheckpointOutcome({
        turnId: requestTurnId,
        checkpointId: confirmation.checkpointId,
        kind,
        pathCount: requestedPaths.length,
        state: restoredCheckpoint?.state ?? null,
        code: failedOutcome && requestError instanceof HarnessFault ? requestError.code : null,
        detail: failedOutcome && requestError !== null ? errorText(requestError) : null,
      });
      if (refreshError !== null && requestError !== null) {
        setCheckpointFault(errorText(refreshError));
      }
      setRestoreConfirmation(null);
      setRevision((value) => value + 1);
    } finally {
      if (restoreAttemptRef.current === attempt) restoreAttemptRef.current = null;
      if (restoreContextRef.current === contextVersion) setCheckpointBusy(null);
    }
  }, [
    base,
    checkpointBusy,
    checkpointViewTurnId,
    checkpoints,
    loadCheckpoints,
    restoreConfirmation,
    sendRestore,
    summary,
    turnId,
  ]);

  const reloadPanel = useCallback(() => {
    restoreContextRef.current += 1;
    setRestoreConfirmation(null);
    setCheckpointBusy(null);
    setCheckpointViewTurnId(null);
    setCheckpointPathSelections(new Map());
    setRevision((value) => value + 1);
  }, []);

  if (turnId === null || turnId === "") {
    return (
      <section className="pw-harness-panel pw-harness-panel-empty" aria-label={zh ? zhText("workbench.HarnessTurnPanel.line1396") : "Harness observation"}>
        <div className="pw-harness-empty-icon"><Icon name="radio" size={20} /></div>
        <div>
          <strong>{zh ? zhText("workbench.HarnessTurnPanel.line1399") : "Select a Harness turn"}</strong>
          <span>{zh ? zhText("workbench.HarnessTurnPanel.line1400") : "Bounded events, controls, and recovery evidence appear here."}</span>
        </div>
      </section>
    );
  }

  const latestSeq = events.at(-1)?.seq ?? summary?.event_cursor.last_seq ?? 0;
  const resolvedApprovalIds = new Set(
    events
      .filter((event) => event.kind === "approval_resolved")
      .map(approvalId)
      .filter((id): id is string => id !== null),
  );
  const stateClass = eventClass(summary?.state ?? "unknown");
  const visibleCheckpointOutcome = checkpointOutcome?.turnId === turnId
    ? checkpointOutcome
    : null;

  return (
    <section className="pw-harness-panel" aria-label={zh ? zhText("workbench.HarnessTurnPanel.line1419") : "Harness turn workbench"}>
      <header className="pw-harness-head">
        <div className="pw-harness-heading">
          <Icon name="radio" size={16} />
          <div>
            <strong>{zh ? "Harness turn" : "Harness turn"}</strong>
            <code title={turnId}>{shortId(turnId)}</code>
          </div>
        </div>
        <div className="pw-harness-head-actions">
          <span className={`pw-harness-connection is-${eventClass(connection)}`}>
            {connectionLabel(connection, zh)}
          </span>
          <button
            className="pw-harness-icon-button"
            type="button"
            title={zh ? zhText("workbench.HarnessTurnPanel.line1435") : "Reload and replay"}
            aria-label={zh ? zhText("workbench.HarnessTurnPanel.line1436") : "Reload and replay"}
            onClick={reloadPanel}
          >
            <Icon name="refresh" size={15} />
          </button>
        </div>
      </header>

      {summary !== null && (
        <div className="pw-harness-summary">
          <span className={`pw-harness-state is-${stateClass}`}>{summary.state}</span>
          <span>{zh ? zhText("workbench.HarnessTurnPanel.line1447") : "observed evidence"} {summary.evidence_class}</span>
          <span>{zh ? zhText("workbench.HarnessTurnPanel.line1448") : "events"} {latestSeq}/500</span>
          <span>{summary.usage.total_tokens ?? summary.usage.output_tokens ?? 0} {zh ? zhText("workbench.HarnessTurnPanel.tokens") : "tokens"}</span>
          {summary.recovery !== false && <span className="pw-harness-warning">{zh ? zhText("workbench.HarnessTurnPanel.line1450") : "recovery required"}</span>}
        </div>
      )}

      {gap !== null && (
        <div className="pw-harness-gap" role="status">
          <Icon name="info" size={15} />
          <span>
            {zh ? zhText("workbench.HarnessTurnPanel.line1458") : "Replay has an event gap"} · {gap.missing_from}–{gap.missing_to}
          </span>
          <small>{gap.reason ?? (zh ? zhText("workbench.HarnessTurnPanel.line1460") : "retention window")}</small>
        </div>
      )}

      {fault !== null && (
        <div className="pw-harness-fault" role="alert">
          <Icon name="info" size={15} />
          <span>{fault}</span>
        </div>
      )}

      {controlResult !== null && (
        <div className={`pw-harness-control-result${controlResult.uncertain ? " is-uncertain" : ""}`} role="status">
          <span>{controlStateLabel(controlResult, zh)}</span>
          {controlResult.event_seq !== null && <small>seq {controlResult.event_seq}</small>}
        </div>
      )}

      <div className="pw-harness-controls">
        <button
          className="pw-harness-danger-button"
          type="button"
          disabled={!canControl || controlBusy}
          onClick={() => void runControl("interrupt", zh ? zhText("workbench.HarnessTurnPanel.line1483") : "User requested interrupt")}
        >
          {controlBusy ? (zh ? zhText("workbench.HarnessTurnPanel.line1485") : "Working…") : (zh ? zhText("workbench.HarnessTurnPanel.line1485.2") : "Interrupt")}
        </button>
        <input
          className="pw-harness-steer-input"
          value={steerDraft}
          disabled={!canControl || controlBusy}
          placeholder={zh ? zhText("workbench.HarnessTurnPanel.line1491") : "Send user-language steer…"}
          onChange={(event) => setSteerDraft(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !event.nativeEvent.isComposing && steerDraft.trim() !== "") {
              event.preventDefault();
              void runControl("steer", steerDraft.trim());
            }
          }}
        />
        <button
          className="pw-harness-steer-button"
          type="button"
          disabled={!canControl || controlBusy || steerDraft.trim() === ""}
          onClick={() => void runControl("steer", steerDraft.trim())}
        >
          <Icon name="send" size={14} />
          {zh ? zhText("workbench.HarnessTurnPanel.steerAction") : "Steer"}
        </button>
      </div>

      {(terminalSessions.length > 0 || terminalFault !== null) && (
        <section
          className="pw-harness-terminal-sessions"
          aria-label={zh ? zhText("workbench.HarnessTurnPanel.line1514") : "Background command sessions"}
        >
          <div className="pw-harness-terminal-heading">
            <div>
              <strong>{zh ? zhText("workbench.HarnessTurnPanel.line1518") : "Background command sessions"}</strong>
              {terminalSessions.length > 0 && (
                <small>
                  {zh
                    ? zhText("workbench.HarnessTurnPanel.line1522")
                    : "Durable non-interactive pipes, loaded independently of items and checkpoints"}
                </small>
              )}
            </div>
            <span>{terminalLoading ? "…" : terminalSessions.length}</span>
          </div>

          {terminalFault !== null && (
            <div className="pw-harness-terminal-fault" role="alert">
              <Icon name="info" size={14} />
              <span>{terminalFault}</span>
            </div>
          )}

          <div className="pw-harness-terminal-list">
            {terminalSessions.map((session) => {
              const sessionId = session.terminal_session_id;
              const output = terminalOutputs[sessionId];
              const activeSession = terminalStateIsActive(session.state);
              const outcome = terminalStopOutcomes[sessionId]?.turnId === turnId
                ? terminalStopOutcomes[sessionId]
                : null;
              const stopSubmitting = outcome?.kind === "submitting";
              const stopPending = outcome?.kind === "pending";
              const stateKind = eventClass(session.state.toLowerCase());
              return (
                <article
                  className={`pw-harness-terminal-session is-${stateKind}${terminalStateIsUncertain(session) ? " is-uncertain" : ""}`}
                  key={sessionId}
                >
                  <header className="pw-harness-terminal-session-header">
                    <div>
                      <span className="pw-harness-terminal-mode">{session.mode}</span>
                      <code title={sessionId}>{shortId(sessionId)}</code>
                    </div>
                    <span className={`pw-harness-terminal-state is-${stateKind}`}>
                      {session.state}
                    </span>
                  </header>

                  <div className="pw-harness-terminal-facts">
                    <span>{session.transport} · {session.session_scope}</span>
                    <span title={session.cwd_relative}>cwd {session.cwd_relative}</span>
                    <code title={session.command_digest}>cmd {shortId(session.command_digest)}</code>
                    <span title={session.evidence_class}>{session.evidence_class}</span>
                    <span title={session.sandbox_evidence}>sandbox {session.sandbox_evidence}</span>
                    <span>tree {session.tree_containment}</span>
                    {session.epoch !== null && <span>epoch {session.epoch}</span>}
                    <time
                      dateTime={session.started_at ?? undefined}
                      title={session.started_at ?? (zh ? zhText("workbench.HarnessTurnPanel.line1573") : "Start time unavailable")}
                    >
                      {zh ? zhText("workbench.HarnessTurnPanel.line1575") : "started"} {checkpointTime(
                        session.started_at,
                        checkpointDateFormatter,
                        zh ? zhText("workbench.HarnessTurnPanel.line1578") : "unavailable",
                      )}
                    </time>
                    {session.ended_at !== null && (
                      <time dateTime={session.ended_at} title={session.ended_at}>
                        {zh ? zhText("workbench.HarnessTurnPanel.line1583") : "ended"} {checkpointTime(
                          session.ended_at,
                          checkpointDateFormatter,
                          zh ? zhText("workbench.HarnessTurnPanel.line1586") : "unavailable",
                        )}
                      </time>
                    )}
                    {session.exit_code !== null && <span>exit {session.exit_code}</span>}
                    <span>{terminalNumberFormatter.format(session.output_bytes)} bytes</span>
                    {(session.output_truncated || output?.truncated === true) && (
                      <span className="is-warning">{zh ? zhText("workbench.HarnessTurnPanel.line1593") : "output truncated"}</span>
                    )}
                    {session.error_code !== null && <code className="is-error">{session.error_code}</code>}
                  </div>

                  <div className="pw-harness-terminal-capabilities" role="note">
                    <strong>{zh ? zhText("workbench.HarnessTurnPanel.line1599") : "Non-interactive PIPE_SESSION; not a PTY."}</strong>
                    <span>
                      {zh
                        ? (zhText("workbench.HarnessTurnPanel.line1602.head") + (session.capabilities.reconnect
                          ? zhText("workbench.HarnessTurnPanel.reconnectAvailable")
                          : zhText("workbench.HarnessTurnPanel.reconnectUnavailable")))
                        : `stdin unavailable · resize unavailable · reconnect ${session.capabilities.reconnect ? "available" : "unavailable"}`}
                    </span>
                  </div>

                  <div
                    className="pw-harness-terminal-output"
                    role="log"
                    aria-live={activeSession ? "polite" : "off"}
                    aria-label={zh ? (zhText("workbench.HarnessTurnPanel.line1611.head") + String(shortId(sessionId)) + zhText("workbench.HarnessTurnPanel.line1611.tail1")) : `Bounded output for session ${shortId(sessionId)}`}
                  >
                    {output?.gap !== null && output?.gap !== undefined && (
                      <div className="pw-harness-terminal-gap" role="status">
                        <strong>{zh ? zhText("workbench.HarnessTurnPanel.line1615") : "Output retention gap"}</strong>
                        <span>
                          #{output.gap.missing_from}–{output.gap.missing_to} · {output.gap.reason}
                        </span>
                      </div>
                    )}
                    {output !== undefined && output.earliestSeq !== null && (
                      <small className="pw-harness-terminal-output-range">
                        {zh ? zhText("workbench.HarnessTurnPanel.line1623") : "retained from"} #{output.earliestSeq}
                        {output.nextSeq > 0 && ` · ${zh ? zhText("workbench.HarnessTurnPanel.line1624") : "cursor"} #${output.nextSeq}`}
                      </small>
                    )}
                    <pre tabIndex={0}>
                      {(output?.chunks ?? []).map((chunk) => (
                        <span
                          className={`is-${chunk.stream}${chunk.redacted ? " is-redacted" : ""}`}
                          key={`${sessionId}:${chunk.seq}`}
                          data-seq={chunk.seq}
                        >
                          <b>#{chunk.seq} {chunk.stream}</b>{" │ "}{chunk.text}{chunk.text.endsWith("\n") ? "" : "\n"}
                        </span>
                      ))}
                      {(output?.chunks.length ?? 0) === 0 && (
                        <span className="is-empty">
                          {output?.loading === true || terminalLoading
                            ? (zh ? zhText("workbench.HarnessTurnPanel.line1640") : "Loading bounded output…")
                            : (zh ? zhText("workbench.HarnessTurnPanel.line1641") : "No retained output.")}
                        </span>
                      )}
                    </pre>
                    {output?.fault !== null && output?.fault !== undefined && (
                      <div className="pw-harness-terminal-output-fault" role="alert">
                        {output.fault}
                      </div>
                    )}
                  </div>

                  {(session.uncertain_reason !== null || session.error_code !== null) && (
                    <div className="pw-harness-terminal-evidence-warning" role="status">
                      <strong>{terminalStateIsUncertain(session) ? "UNCERTAIN" : (zh ? zhText("workbench.HarnessTurnPanel.line1654") : "Session error")}</strong>
                      <span>{session.uncertain_reason ?? session.error_code}</span>
                    </div>
                  )}

                  {(activeSession || outcome !== null) && (
                    <div className="pw-harness-terminal-control">
                      {activeSession && session.capabilities.stop && session.epoch !== null && (
                        <button
                          type="button"
                          disabled={stopSubmitting}
                          onClick={() => void stopTerminal(session)}
                        >
                          {stopSubmitting
                            ? (zh ? zhText("workbench.HarnessTurnPanel.line1668") : "Requesting…")
                            : stopPending
                              ? (zh ? zhText("workbench.HarnessTurnPanel.line1670") : "Recheck with same request")
                              : (zh ? zhText("workbench.HarnessTurnPanel.line1671") : "Stop session")}
                        </button>
                      )}
                      {activeSession && !session.capabilities.stop && (
                        <span className="pw-harness-terminal-stop-unavailable">
                          {zh ? zhText("workbench.HarnessTurnPanel.line1676") : "Stop capability is not proven for this session"}
                        </span>
                      )}
                      {outcome !== null && (
                        <div
                          className={`pw-harness-terminal-stop-outcome is-${outcome.kind}`}
                          role={outcome.kind === "pending" || outcome.kind === "terminal" ? "status" : "alert"}
                        >
                          <strong>{terminalStopOutcomeLabel(outcome.kind, zh)}</strong>
                          <span>{terminalStopOutcomeMessage(outcome, zh)}</span>
                          <small>
                            <code title={outcome.requestId}>{shortId(outcome.requestId)}</code>
                            {outcome.code !== null && ` · ${outcome.code}`}
                          </small>
                          {outcome.detail !== null && <small>{outcome.detail}</small>}
                        </div>
                      )}
                    </div>
                  )}
                </article>
              );
            })}
          </div>
        </section>
      )}

      {(checkpoints.length > 0 || checkpointFault !== null || visibleCheckpointOutcome !== null) && (
        <section className="pw-harness-checkpoints" aria-label={zh ? zhText("workbench.HarnessTurnPanel.line1703") : "File checkpoints"}>
          <div className="pw-harness-checkpoint-heading">
            <div>
              <strong>{zh ? zhText("workbench.HarnessTurnPanel.line1706") : "File checkpoints"}</strong>
              <small>{zh ? zhText("workbench.HarnessTurnPanel.line1707") : "Review scope before explicitly confirming restore"}</small>
            </div>
            <span>{checkpoints.length}</span>
          </div>
          {checkpoints.length === 0 && checkpointFault === null && (
            <div className="pw-harness-checkpoint-empty">
              {zh ? zhText("workbench.HarnessTurnPanel.line1713") : "No checkpoint for this turn after refresh."}
            </div>
          )}
          {checkpoints.map((checkpoint) => {
            const stateKind = checkpointStateKind(checkpoint);
            const allPaths = uniqueCheckpointPaths(checkpoint.changed_paths);
            const restoredPathSet = new Set(checkpoint.restored_paths);
            const restoredCount = allPaths.filter((path) => restoredPathSet.has(path)).length;
            const availablePaths = selectableCheckpointPaths(checkpoint);
            const selectedPaths = selectedCheckpointPaths(
              checkpoint,
              checkpointPathSelections.get(checkpoint.checkpoint_id),
            );
            const selectedPathSet = new Set(selectedPaths);
            const stateRestorable = !checkpoint.uncertain
              && RESTORABLE_CHECKPOINT_STATES.has(checkpoint.state.toLowerCase());
            const restorable = checkpointViewTurnId === turnId
              && summary?.live_available === true
              && summary.epoch !== null
              && stateRestorable
              && availablePaths.length > 0;
            const isConfirming = restoreConfirmation?.turnId === turnId
              && restoreConfirmation.checkpointId === checkpoint.checkpoint_id;
            const isBusy = checkpointBusy === checkpoint.checkpoint_id;
            return (
              <article
                className={`pw-harness-checkpoint is-${stateKind}${isConfirming ? " is-confirming" : ""}`}
                key={checkpoint.checkpoint_id}
              >
                <header className="pw-harness-checkpoint-header">
                  <div>
                    <strong>{zh ? zhText("workbench.HarnessTurnPanel.checkpointTitle") : "Checkpoint"}</strong>
                    <code title={checkpoint.checkpoint_id}>{shortId(checkpoint.checkpoint_id)}</code>
                  </div>
                  <span className={`pw-harness-checkpoint-state is-${stateKind}`}>
                    {checkpoint.uncertain ? "uncertain" : checkpoint.state}
                  </span>
                </header>

                <div className="pw-harness-checkpoint-facts">
                  <span title={checkpoint.evidence_class}>{checkpoint.evidence_class}</span>
                  <span>{allPaths.length} {zh ? zhText("workbench.HarnessTurnPanel.line1754") : "changed"}</span>
                  <span>{restoredCount}/{allPaths.length} {zh ? zhText("workbench.HarnessTurnPanel.line1755") : "restored"}</span>
                  <time
                    dateTime={checkpoint.created_at ?? undefined}
                    title={checkpoint.created_at ?? (zh ? zhText("workbench.HarnessTurnPanel.line1758") : "Created time unavailable")}
                  >
                    {zh ? zhText("workbench.HarnessTurnPanel.line1760") : "created"} {checkpointTime(
                      checkpoint.created_at,
                      checkpointDateFormatter,
                      zh ? zhText("workbench.HarnessTurnPanel.line1763") : "unavailable",
                    )}
                  </time>
                  {checkpoint.reconciled_from_uncertain && (
                    <span className="is-reconciled">
                      {zh ? zhText("workbench.HarnessTurnPanel.line1768") : "reconciled from uncertain"}
                    </span>
                  )}
                  {checkpoint.error_code !== null && (
                    <code className="is-error">{checkpoint.error_code}</code>
                  )}
                </div>

                <fieldset className="pw-harness-checkpoint-paths">
                  <legend>
                    <span>{zh ? zhText("workbench.HarnessTurnPanel.line1778") : "Restore paths"}</span>
                    <small>
                      {selectedPaths.length}/{availablePaths.length} {zh ? zhText("workbench.HarnessTurnPanel.line1780") : "available selected"}
                    </small>
                  </legend>
                  <div className="pw-harness-checkpoint-path-list">
                    {allPaths.length === 0 && (
                      <span className="pw-harness-checkpoint-path-empty">
                        {zh ? zhText("workbench.HarnessTurnPanel.line1786") : "No safe paths are available for review."}
                      </span>
                    )}
                    {allPaths.map((path) => {
                      const alreadyRestored = restoredPathSet.has(path);
                      return (
                        <label
                          className={`pw-harness-checkpoint-path${alreadyRestored ? " is-restored" : ""}`}
                          key={path}
                        >
                          <input
                            type="checkbox"
                            checked={alreadyRestored || selectedPathSet.has(path)}
                            disabled={
                              alreadyRestored
                              || !stateRestorable
                              || checkpointViewTurnId !== turnId
                              || checkpointBusy !== null
                              || isConfirming
                            }
                            onChange={(event) => toggleCheckpointPath(
                              checkpoint,
                              path,
                              event.target.checked,
                            )}
                          />
                          <code title={path}>{path}</code>
                          {alreadyRestored && <span>{zh ? zhText("workbench.HarnessTurnPanel.line1813") : "restored"}</span>}
                        </label>
                      );
                    })}
                  </div>
                </fieldset>

                <details className="pw-harness-checkpoint-diff">
                  <summary>
                    <span>{zh ? zhText("workbench.HarnessTurnPanel.line1822") : "Bounded diff preview"}</span>
                    <small>
                      {checkpoint.diff_preview === null
                        ? (zh ? zhText("workbench.HarnessTurnPanel.line1825") : "unavailable")
                        : (zh ? zhText("workbench.HarnessTurnPanel.line1826") : "expand to review")}
                    </small>
                  </summary>
                  <div className="pw-harness-checkpoint-diff-body">
                    {checkpoint.diff_preview === null ? (
                      <p>
                        {zh
                          ? zhText("workbench.HarnessTurnPanel.line1833")
                          : "The safe diff summary is unavailable; raw files will not be read as a fallback."}
                      </p>
                    ) : (
                      <pre tabIndex={0}>{checkpoint.diff_preview}</pre>
                    )}
                  </div>
                </details>

                <div className="pw-harness-checkpoint-actions">
                  <span>
                    {selectedPaths.length === 0
                      ? (zh ? zhText("workbench.HarnessTurnPanel.line1845") : "Select at least one unrestored path")
                      : (zh ? (zhText("workbench.HarnessTurnPanel.line1846.head") + String(selectedPaths.length) + zhText("workbench.HarnessTurnPanel.line1846.tail1")) : `${selectedPaths.length} path(s) ready for review`)}
                  </span>
                  {!isConfirming && (
                    <button
                      type="button"
                      disabled={
                        !restorable
                        || selectedPaths.length === 0
                        || checkpointBusy !== null
                        || restoreConfirmation !== null
                      }
                      onClick={() => beginCheckpointRestore(checkpoint)}
                    >
                      {zh ? zhText("workbench.HarnessTurnPanel.line1859") : "Review restore"}
                    </button>
                  )}
                </div>

                {isConfirming && (
                  <div
                    className="pw-harness-checkpoint-confirmation"
                    role="group"
                    aria-label={zh ? zhText("workbench.HarnessTurnPanel.line1868") : "Confirm checkpoint restore scope"}
                  >
                    <div>
                      <strong>{zh ? zhText("workbench.HarnessTurnPanel.line1871") : "Confirm restore scope"}</strong>
                      <small>
                        {zh
                          ? zhText("workbench.HarnessTurnPanel.line1874")
                          : "The second action sends only these paths after revalidating the turn, epoch, and path subset."}
                      </small>
                    </div>
                    <ul>
                      {restoreConfirmation.paths.map((path) => (
                        <li key={path}><code>{path}</code></li>
                      ))}
                    </ul>
                    <div className="pw-harness-checkpoint-confirm-actions">
                      <button
                        type="button"
                        disabled={isBusy}
                        onClick={() => setRestoreConfirmation(null)}
                      >
                        {zh ? zhText("workbench.HarnessTurnPanel.line1889") : "Cancel"}
                      </button>
                      <button
                        className="pw-harness-checkpoint-confirm-button"
                        type="button"
                        disabled={isBusy}
                        onClick={() => void submitCheckpointRestore()}
                      >
                        {isBusy
                          ? (zh ? zhText("workbench.HarnessTurnPanel.line1898") : "Restoring…")
                          : (zh ? zhText("workbench.HarnessTurnPanel.line1899") : "Confirm selected paths")}
                      </button>
                    </div>
                  </div>
                )}
              </article>
            );
          })}
          {visibleCheckpointOutcome !== null && (
            <div
              className={`pw-harness-checkpoint-outcome is-${visibleCheckpointOutcome.kind}`}
              role={visibleCheckpointOutcome.kind === "restored" || visibleCheckpointOutcome.kind === "partial" ? "status" : "alert"}
            >
              <span className="pw-harness-checkpoint-outcome-mark" aria-hidden="true" />
              <div>
                <strong>{checkpointOutcomeLabel(visibleCheckpointOutcome.kind, zh)}</strong>
                <span>{checkpointOutcomeMessage(visibleCheckpointOutcome, zh)}</span>
                <small>
                  <code title={visibleCheckpointOutcome.checkpointId}>
                    {shortId(visibleCheckpointOutcome.checkpointId)}
                  </code>
                  {visibleCheckpointOutcome.state !== null && ` · ${visibleCheckpointOutcome.state}`}
                  {visibleCheckpointOutcome.code !== null && ` · ${visibleCheckpointOutcome.code}`}
                </small>
                {visibleCheckpointOutcome.detail !== null && (
                  <small className="pw-harness-checkpoint-outcome-detail">
                    {visibleCheckpointOutcome.detail}
                  </small>
                )}
              </div>
            </div>
          )}
          {checkpointFault !== null && (
            <div className="pw-harness-checkpoint-fault" role="alert">
              <Icon name="info" size={14} />
              <span>{checkpointFault}</span>
            </div>
          )}
        </section>
      )}

      <section className="pw-harness-items" aria-label={zh ? zhText("workbench.HarnessTurnPanel.line1940") : "Turn items"}>
        <div className="pw-harness-items-heading">
          <div>
            <strong>{zh ? zhText("workbench.HarnessTurnPanel.line1943") : "Turn items"}</strong>
            <small>{zh ? zhText("workbench.HarnessTurnPanel.line1944") : "Projected from durable events; not another execution loop"}</small>
          </div>
          <span>{turnItems.length}</span>
        </div>
        {itemsTruncated && (
          <div className="pw-harness-items-warning">
            {zh ? zhText("workbench.HarnessTurnPanel.line1950") : "Item history is bounded by retention"}
          </div>
        )}
        {turnItems.length === 0 && connection !== "loading" && (
          <div className="pw-harness-no-events">{zh ? zhText("workbench.HarnessTurnPanel.line1954") : "No projected items yet."}</div>
        )}
        <div className="pw-harness-item-list">
          {turnItems.map((item) => (
            <details
              className={`pw-harness-item is-${eventClass(item.item_type)} is-${eventClass(item.state)}`}
              key={item.item_id}
            >
              <summary>
                <span className={`pw-harness-item-dot is-${eventClass(item.state)}`} />
                <span className="pw-harness-item-main">
                  <strong>{turnItemTitle(item, zh)}</strong>
                  <small>
                    {item.item_type} · {item.phase}
                    {item.first_seq !== null && ` · #${item.first_seq}${item.last_seq !== item.first_seq && item.last_seq !== null ? `–${item.last_seq}` : ""}`}
                  </small>
                </span>
                <span className="pw-harness-item-state">{item.state}</span>
                <span className="pw-harness-item-expand">⌄</span>
              </summary>
              <div className="pw-harness-item-body">
                <div className="pw-harness-item-facts">
                  <span>{item.evidence_class}</span>
                  <span>{item.history_total} {zh ? zhText("workbench.HarnessTurnPanel.line1977") : "updates"}</span>
                  {item.tool_call_id !== null && <code title={item.tool_call_id}>{shortId(item.tool_call_id)}</code>}
                  {item.has_gap && <span className="pw-harness-warning">gap</span>}
                  {item.late_event_count > 0 && <span className="pw-harness-warning">late {item.late_event_count}</span>}
                  {item.conflict_count > 0 && <span className="pw-harness-warning">conflict {item.conflict_count}</span>}
                </div>
                <ol className="pw-harness-item-history">
                  {item.history.map((entry) => (
                    <li key={`${item.item_id}:${entry.revision}`}>
                      <span>r{entry.revision}</span>
                      <strong>{entry.kind.replaceAll("_", " ")}</strong>
                      <small>{entry.status} · {entry.phase}{entry.seq !== null ? ` · #${entry.seq}` : ""}</small>
                      {entry.late && <em>late</em>}
                    </li>
                  ))}
                </ol>
                {item.history_truncated && <small className="pw-harness-warning">{zh ? zhText("workbench.HarnessTurnPanel.line1993") : "Earlier history truncated"}</small>}
              </div>
            </details>
          ))}
        </div>
      </section>

      <details className="pw-harness-event-ledger">
        <summary>
          <span>{zh ? zhText("workbench.HarnessTurnPanel.line2002") : "Raw event ledger"}</span>
          <small>{events.length}</small>
        </summary>
      <div className="pw-harness-events" aria-live="polite">
        {events.length === 0 && connection !== "loading" && (
          <div className="pw-harness-no-events">{zh ? zhText("workbench.HarnessTurnPanel.line2007") : "No replayable events for this turn."}</div>
        )}
        {events.map((item) => {
          const id = approvalId(item);
          const text = payloadString(item);
          const isApproval = item.kind === "approval_requested"
            && id !== null
            && !resolvedApprovalIds.has(id)
            && !submittedApprovals.has(id);
          return (
            <details className={`pw-harness-event is-${eventClass(item.kind)}${item.truncated ? " is-truncated" : ""}`} key={`${item.turn_id}:${item.seq}`}>
              <summary>
                <span className="pw-harness-seq">{item.seq}</span>
                <span className="pw-harness-kind">{eventTitle(item, zh)}</span>
                <span className="pw-harness-status">{item.status}</span>
                {item.redacted && <span className="pw-harness-badge">{zh ? zhText("workbench.HarnessTurnPanel.line2022") : "redacted"}</span>}
                {item.truncated && <span className="pw-harness-badge">{zh ? zhText("workbench.HarnessTurnPanel.line2023") : "truncated"}</span>}
                <span className="pw-harness-time">{item.occurred_at?.slice(11, 19) ?? ""}</span>
              </summary>
              <div className="pw-harness-event-body">
                {item.kind === "reasoning_delta" && (
                  <span className="pw-harness-reasoning-label">{zh ? zhText("workbench.HarnessTurnPanel.line2028") : "Provider-declared reasoning fragment"}</span>
                )}
                {text !== null && <pre>{text}</pre>}
                <div className="pw-harness-event-meta">
                  <span>{item.source}</span>
                  <span>{item.phase}</span>
                  {item.payload_digest !== null && <span>digest {item.payload_digest.slice(0, 16)}</span>}
                </div>
                {isApproval && canControl && (
                  <div className="pw-harness-approval-actions" role="group" aria-label={zh ? zhText("workbench.HarnessTurnPanel.line2037") : "Approval decision"}>
                    {(["allow_once", "deny", "cancel"] as const).map((decision) => (
                      <button
                        key={decision}
                        type="button"
                        disabled={controlBusy}
                        onClick={() => void resolveApproval(id, decision)}
                      >
                        {decision}
                      </button>
                    ))}
                  </div>
                )}
              </div>
            </details>
          );
        })}
      </div>
      </details>
    </section>
  );
}

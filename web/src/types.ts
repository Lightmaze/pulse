// Event shapes mirror MetricsRecorder output (interaction/metrics/recorder.py).
// The vocabulary evolves (e.g. heartbeat gained coherent/breadth on 2026-07-16),
// so every field beyond {t, type} is treated as optional at parse time.

export interface RawEvent {
  t: string;
  type: string;
  [key: string]: unknown;
}

export interface IndexedEvent extends RawEvent {
  tMs: number;
}

export interface HeartbeatSeries {
  tSec: number[]; // uPlot x axis (unix seconds)
  tMs: number[];
  ratio: (number | null)[];
  coherent: (number | null)[];
  breadth: (number | null)[];
  active: (number | null)[];
  total: (number | null)[];
  pending: (number | null)[];
}

export interface PulseMark {
  tMs: number;
  row: number; // index into LoadedRun.engrams
  reason: string; // spontaneous | propagation | external | ...
  inputTokens: number;
  outputTokens: number;
  cachedTokens: number;
}

export interface SuccessionMark {
  tMs: number;
  oldId: string;
  newId: string;
}

export interface TokenCum {
  tMs: number[]; // one entry per pulse event
  input: number[]; // prefix sums
  output: number[];
  cached: number[];
}

export interface TopologyNode {
  id: string;
  project: string | null;
  activity: number;
  pulses: number;
}

/** [sourceIndex, targetIndex, weight, "e" | "i"] — indices into `nodes`. */
export type TopologyEdge = [number, number, number, string];

export interface TopologySnapshot {
  tMs: number;
  tick: number;
  nodes: TopologyNode[];
  edges: TopologyEdge[];
}

export interface PropagationMark {
  tMs: number;
  source: string;
  targets: string[];
}

export interface LoadedRun {
  name: string;
  events: IndexedEvent[]; // time-sorted
  heartbeats: HeartbeatSeries;
  pulses: PulseMark[]; // time-sorted (same order as source)
  engrams: string[]; // row order = first appearance anywhere in the stream
  successions: SuccessionMark[];
  topology: TopologySnapshot[]; // time-sorted; empty unless the run opted in
  propagations: PropagationMark[]; // time-sorted
  tokenCum: TokenCum;
  counts: Record<string, number>;
  skipped: number; // unparseable lines
  tMinMs: number;
  tMaxMs: number;
}

export const REASON_COLORS: Record<string, string> = {
  spontaneous: "#f2a541",
  propagation: "#5b9dd9",
  external: "#6fc276",
};
export const REASON_FALLBACK = "#8a8f98";

export function reasonColor(reason: string): string {
  return REASON_COLORS[reason] ?? REASON_FALLBACK;
}

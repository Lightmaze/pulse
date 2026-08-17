import type {
  HeartbeatSeries,
  IndexedEvent,
  LoadedRun,
  PropagationMark,
  PulseMark,
  SuccessionMark,
  TokenCum,
  TopologyEdge,
  TopologyNode,
  TopologySnapshot,
} from "./types";

function num(v: unknown): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function str(v: unknown): string | null {
  return typeof v === "string" && v.length > 0 ? v : null;
}

/** Timestamp-index one raw event object; null if it is not a usable metrics event. */
export function indexEvent(raw: Record<string, unknown>): IndexedEvent | null {
  const t = str(raw.t);
  const type = str(raw.type);
  if (t === null || type === null) return null;
  const tMs = Date.parse(t);
  if (Number.isNaN(tMs)) return null;
  return { ...raw, t, type, tMs };
}

/** Parse a Pulse metrics JSONL dump into the indexed structures every view reads. */
export function parseJsonl(text: string, name: string): LoadedRun {
  const events: IndexedEvent[] = [];
  let skipped = 0;

  for (const line of text.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    let indexed: IndexedEvent | null = null;
    try {
      indexed = indexEvent(JSON.parse(trimmed) as Record<string, unknown>);
    } catch {
      indexed = null;
    }
    if (indexed === null) skipped += 1;
    else events.push(indexed);
  }

  if (events.length === 0) {
    throw new Error(`${name}: no parseable Pulse metrics events (${skipped} lines skipped)`);
  }
  return buildRun(events, name, skipped);
}

/**
 * The reducer: indexed events in, view-ready structures out.
 *
 * The single point where replay and live converge (frontend-design.md §3) —
 * replay feeds it a whole file, live feeds it the accumulated stream after
 * each SSE frame. Every view reads only what this returns, so neither mode
 * can drift from the other.
 */
export function buildRun(
  events: IndexedEvent[],
  name: string,
  skipped = 0,
): LoadedRun {
  // Recorder appends in order, but merged/concatenated files may not be sorted.
  events.sort((a, b) => a.tMs - b.tMs);

  const heartbeats: HeartbeatSeries = {
    tSec: [], tMs: [], ratio: [], coherent: [], breadth: [],
    active: [], total: [], pending: [],
  };
  const pulses: PulseMark[] = [];
  const successions: SuccessionMark[] = [];
  const topology: TopologySnapshot[] = [];
  const propagations: PropagationMark[] = [];
  const tokenCum: TokenCum = { tMs: [], input: [], output: [], cached: [] };
  const counts: Record<string, number> = {};
  const engrams: string[] = [];
  const rowOf = new Map<string, number>();

  const row = (id: string): number => {
    let r = rowOf.get(id);
    if (r === undefined) {
      r = engrams.length;
      rowOf.set(id, r);
      engrams.push(id);
    }
    return r;
  };

  let cumIn = 0;
  let cumOut = 0;
  let cumCached = 0;

  for (const e of events) {
    counts[e.type] = (counts[e.type] ?? 0) + 1;

    switch (e.type) {
      case "heartbeat": {
        heartbeats.tSec.push(e.tMs / 1000);
        heartbeats.tMs.push(e.tMs);
        heartbeats.ratio.push(num(e.ratio));
        heartbeats.coherent.push(num(e.coherent));
        heartbeats.breadth.push(num(e.breadth));
        heartbeats.active.push(num(e.active));
        heartbeats.total.push(num(e.total));
        heartbeats.pending.push(num(e.pending));
        break;
      }
      case "pulse": {
        const id = str(e.engram);
        if (id === null) break;
        pulses.push({
          tMs: e.tMs,
          row: row(id),
          reason: str(e.reason) ?? "unknown",
          inputTokens: num(e.input_tokens) ?? 0,
          outputTokens: num(e.output_tokens) ?? 0,
          cachedTokens: num(e.cached_tokens) ?? 0,
        });
        cumIn += num(e.input_tokens) ?? 0;
        cumOut += num(e.output_tokens) ?? 0;
        cumCached += num(e.cached_tokens) ?? 0;
        tokenCum.tMs.push(e.tMs);
        tokenCum.input.push(cumIn);
        tokenCum.output.push(cumOut);
        tokenCum.cached.push(cumCached);
        break;
      }
      case "propagate": {
        const source = str(e.source);
        if (source === null) break;
        row(source);
        const targets: string[] = [];
        if (Array.isArray(e.targets)) {
          for (const target of e.targets) {
            const id = str(target);
            if (id !== null) {
              row(id);
              targets.push(id);
            }
          }
        }
        propagations.push({ tMs: e.tMs, source, targets });
        break;
      }
      case "topology": {
        if (!Array.isArray(e.engrams) || !Array.isArray(e.edges)) break;
        const nodes: TopologyNode[] = [];
        for (const raw of e.engrams) {
          if (typeof raw !== "object" || raw === null) continue;
          const n = raw as Record<string, unknown>;
          const id = str(n.id);
          if (id === null) continue;
          row(id);
          nodes.push({
            id,
            project: str(n.project),
            activity: num(n.activity) ?? 0,
            pulses: num(n.pulses) ?? 0,
          });
        }
        const edges: TopologyEdge[] = [];
        for (const raw of e.edges) {
          if (!Array.isArray(raw) || raw.length < 4) continue;
          const [i, j, w, kind] = raw as unknown[];
          if (typeof i !== "number" || typeof j !== "number") continue;
          if (i < 0 || i >= nodes.length || j < 0 || j >= nodes.length) continue;
          edges.push([i, j, num(w) ?? 0, str(kind) ?? "e"]);
        }
        topology.push({ tMs: e.tMs, tick: num(e.tick) ?? 0, nodes, edges });
        break;
      }
      case "sensory": {
        const id = str(e.engram);
        if (id !== null) row(id);
        break;
      }
      case "succession": {
        const oldId = str(e.old);
        const newId = str(e.new);
        if (oldId !== null && newId !== null) {
          row(oldId);
          row(newId);
          successions.push({ tMs: e.tMs, oldId, newId });
        }
        break;
      }
    }
  }

  return {
    name,
    events,
    heartbeats,
    pulses,
    engrams,
    successions,
    topology,
    propagations,
    tokenCum,
    counts,
    skipped,
    // Live mode can build a run before the first event arrives.
    tMinMs: events.length > 0 ? events[0].tMs : 0,
    tMaxMs: events.length > 0 ? events[events.length - 1].tMs : 0,
  };
}

/** Index of the last element in sorted `times` that is <= tMs, or -1. */
export function lastIndexAtOrBefore(times: number[], tMs: number): number {
  let lo = 0;
  let hi = times.length - 1;
  let ans = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (times[mid] <= tMs) {
      ans = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return ans;
}

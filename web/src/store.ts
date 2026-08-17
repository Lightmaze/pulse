// Event-sourced viewer state (frontend-design.md §3): the run is an indexed
// event log; playback is a cursor moving through its time range. Replay feeds
// the log from a file, live feeds it from SSE — both land in buildRun, so the
// views cannot tell the two apart.

import { create } from "zustand";
import { subscribeWithSelector } from "zustand/middleware";
import { subscribeLiveStream, type LiveFrame } from "./liveStream";
import { buildRun, indexEvent } from "./parse";
import type { IndexedEvent, LoadedRun } from "./types";

export type Mode = "replay" | "live";
export type LiveStatus = "off" | "connecting" | "open" | "error";

export interface ReplayWindow {
  complete: boolean;
  truncated: boolean;
  window_bytes: number;
  bytes_read: number;
  start_offset: number;
  end_offset: number;
  file_size: number;
  cursor: string | null;
  reset: string | null;
}

/** Workspace pages (frontend-design.md §9: 工作台化). */
export type Page = "sessions" | "trace" | "models" | "cost" | "settings";

const PAGE_HASH: Record<Page, string> = {
  sessions: "#/sessions",
  trace: "#/trace",
  models: "#/models",
  cost: "#/cost",
  settings: "#/settings",
};

export function pageFromHash(hash: string): Page | null {
  const entry = Object.entries(PAGE_HASH).find(([, h]) => h === hash);
  return entry !== undefined ? (entry[0] as Page) : null;
}

export interface ViewerState {
  run: LoadedRun | null;
  error: string | null;
  cursorMs: number;
  playing: boolean;
  speed: number; // wall-clock multiplier

  mode: Mode;
  liveStatus: LiveStatus;
  liveUrl: string;
  replayWindow: ReplayWindow | null;
  following: boolean; // live: cursor pinned to the newest event
  inspected: string | null; // engram id open in the session inspector
  page: Page;

  loadRun(run: LoadedRun): void;
  setError(message: string | null): void;
  seek(tMs: number): void;
  setPlaying(playing: boolean): void;
  setSpeed(speed: number): void;
  /** Advance playback by dtRealMs of wall-clock time. */
  tick(dtRealMs: number): void;

  connectLive(url: string): void;
  disconnectLive(): void;
  setFollowing(following: boolean): void;
  inspect(engramId: string | null): void;
  setPage(page: Page): void;
}

/** Where /engrams lives: the live server when connected, else same-origin
 *  (which works when the built viewer is served by the API itself). */
export function apiBase(state: Pick<ViewerState, "mode" | "liveUrl">): string {
  if (state.mode === "live") return state.liveUrl.replace(/\/events.*$/, "");
  return "";
}

// Non-reactive connection state: the shared subscription and accumulated log
// are not render inputs, and re-creating them on every state write would
// thrash.  Both are bounded for a world that lives indefinitely.
const MAX_LIVE_EVENTS = 5_000;
let releaseSource: (() => void) | null = null;
let liveGeneration = 0;
let liveEvents: IndexedEvent[] = [];
let rebuildTimer = 0;

function replayWindow(data: string): ReplayWindow | null {
  let value: unknown;
  try {
    value = JSON.parse(data);
  } catch {
    return null;
  }
  if (typeof value !== "object" || value === null) return null;
  const row = value as Record<string, unknown>;
  const number = (key: string): number => {
    const candidate = row[key];
    return typeof candidate === "number" && Number.isFinite(candidate)
      ? candidate
      : 0;
  };
  return {
    complete: row.complete === true,
    truncated: row.truncated === true,
    window_bytes: number("window_bytes"),
    bytes_read: number("bytes_read"),
    start_offset: number("start_offset"),
    end_offset: number("end_offset"),
    file_size: number("file_size"),
    cursor: typeof row.cursor === "string" ? row.cursor : null,
    reset: typeof row.reset === "string" ? row.reset : null,
  };
}

export const useViewer = create<ViewerState>()(
  subscribeWithSelector((set, get) => ({
    run: null,
    error: null,
    cursorMs: 0,
    playing: false,
    speed: 60,

    mode: "replay",
    liveStatus: "off",
    // Same-origin when the API serves this bundle; the explicit host is only
    // for `npm run dev`, where viewer and server sit on different ports. A
    // hardcoded :8000 here sent the live path at port 8000 no matter which
    // port the runtime was actually on -- App.tsx computed this correctly and
    // never wrote it back, so the default won.
    liveUrl: `${window.location.origin}/events`,
    replayWindow: null,
    following: true,
    inspected: null,
    page: pageFromHash(window.location.hash) ?? "sessions",

    loadRun: (run) =>
      set({
        run,
        error: null,
        cursorMs: run.tMinMs,
        playing: false,
        mode: "replay",
      }),
    setError: (error) => set({ error }),
    seek: (tMs) => {
      const { run, mode } = get();
      if (run === null) return;
      set({
        cursorMs: Math.min(Math.max(tMs, run.tMinMs), run.tMaxMs),
        // Scrubbing back through a live run means "hold here" — the user is
        // reading history, so stop yanking the cursor to the newest event.
        ...(mode === "live" ? { following: false } : null),
      });
    },
    setPlaying: (playing) => {
      const { run, cursorMs } = get();
      if (run === null) return;
      // Restart from the beginning when playing again from the end.
      if (playing && cursorMs >= run.tMaxMs) {
        set({ playing, cursorMs: run.tMinMs });
      } else {
        set({ playing });
      }
    },
    setSpeed: (speed) => set({ speed }),
    tick: (dtRealMs) => {
      const { run, playing, cursorMs, speed } = get();
      if (run === null || !playing) return;
      const next = cursorMs + dtRealMs * speed;
      if (next >= run.tMaxMs) {
        set({ cursorMs: run.tMaxMs, playing: false });
      } else {
        set({ cursorMs: next });
      }
    },

    connectLive: (url) => {
      get().disconnectLive();
      liveEvents = [];
      const generation = ++liveGeneration;

      // Rebuilding the whole index per frame is O(n) in the run so far. At the
      // engine's event rate (tens to hundreds per minute, batched per flush)
      // that is microseconds, and it buys one reducer for both modes instead
      // of a second, drift-prone incremental path.
      const rebuild = () => {
        const run = buildRun([...liveEvents], url, 0);
        set((s) => ({
          run,
          cursorMs: s.following ? run.tMaxMs : s.cursorMs,
        }));
      };
      const scheduleRebuild = () => {
        if (rebuildTimer !== 0) return;
        rebuildTimer = window.setTimeout(() => {
          rebuildTimer = 0;
          rebuild();
        }, 200);
      };

      const ingest = (raw: LiveFrame) => {
        let batch: unknown;
        try {
          batch = JSON.parse(raw.data);
        } catch {
          return;
        }
        const wrapped =
          typeof batch === "object" && batch !== null
            ? (batch as Record<string, unknown>).events
            : null;
        const items = Array.isArray(batch)
          ? batch
          : Array.isArray(wrapped)
            ? wrapped
            : [batch];
        for (const item of items) {
          if (typeof item !== "object" || item === null) continue;
          const indexed = indexEvent(item as Record<string, unknown>);
          if (indexed !== null) liveEvents.push(indexed);
        }
        if (liveEvents.length > MAX_LIVE_EVENTS) {
          liveEvents.splice(0, liveEvents.length - MAX_LIVE_EVENTS);
        }
        scheduleRebuild();
      };

      set({
        mode: "live",
        liveStatus: "connecting",
        liveUrl: url,
        replayWindow: null,
        error: null,
        playing: false,
        following: true,
      });

      releaseSource = subscribeLiveStream(url, {
        onStatus: (status) => {
          if (generation === liveGeneration) set({ liveStatus: status });
        },
        onFrame: (frame) => {
          if (generation !== liveGeneration) return;
          if (frame.event === "replay") {
            const replay = replayWindow(frame.data);
            if (replay === null) return;
            if (
              replay.reset === "initial" ||
              replay.reset === "replaced" ||
              replay.reset === "truncated" ||
              replay.reset === "overrun"
            ) {
              liveEvents = [];
              window.clearTimeout(rebuildTimer);
              rebuildTimer = 0;
            }
            set({ replayWindow: replay });
            return;
          }
          if (frame.event !== "snapshot" && frame.event !== "append") return;
          set({ liveStatus: "open" });
          ingest(frame);
          if (frame.event === "snapshot") {
            // Show the snapshot immediately; only later frames are debounced.
            window.clearTimeout(rebuildTimer);
            rebuildTimer = 0;
            rebuild();
          }
        },
      });
    },

    disconnectLive: () => {
      liveGeneration += 1;
      if (releaseSource !== null) {
        releaseSource();
        releaseSource = null;
      }
      window.clearTimeout(rebuildTimer);
      rebuildTimer = 0;
      liveEvents = [];
      set({ liveStatus: "off", mode: "replay", replayWindow: null });
    },

    setFollowing: (following) => {
      const { run } = get();
      set({
        following,
        ...(following && run !== null ? { cursorMs: run.tMaxMs } : null),
      });
    },

    inspect: (engramId) => set({ inspected: engramId }),

    setPage: (page) => {
      window.history.replaceState(null, "", PAGE_HASH[page]);
      set({ page });
    },
  })),
);

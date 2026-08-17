import { translate, useI18n, type Locale } from "../i18n";
import { useViewer } from "../store";
import type { IndexedEvent, LoadedRun } from "../types";
import { reasonColor } from "../types";

const WINDOW = 30;

const TYPE_COLORS: Record<string, string> = {
  heartbeat: "#3d4450",
  propagate: "#7aa2f7",
  sensory: "#6fc276",
  succession: "#e8e8e8",
  decay: "#8a8f98",
  budget_exhausted: "#e95454",
  resonance: "#c678dd",
  topology: "#56b6c2",
  // 调律流（Claustrum）
  claustrum_learn: "#d19a66",
  // 委派流（路由判定 + 委派记录）
  route: "#e5c07b",
  delegation: "#e5c07b",
  router_learn: "#d19a66",
};

function eventIndexAt(run: LoadedRun, tMs: number): number {
  let lo = 0;
  let hi = run.events.length - 1;
  let ans = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (run.events[mid].tMs <= tMs) {
      ans = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return ans;
}

function summarize(e: IndexedEvent, locale: Locale): string {
  switch (e.type) {
    case "heartbeat": {
      const extra =
        typeof e.coherent === "number"
          ? translate(locale, "timeline.coherentPart", { coherent: e.coherent })
          : "";
      return translate(locale, "timeline.heartbeatSummary", {
        active: String(e.active),
        total: String(e.total),
        ratio: String(e.ratio),
        pending: String(e.pending),
      }) + extra;
    }
    case "pulse":
      return translate(locale, "timeline.pulseSummary", {
        engram: String(e.engram),
        reason: String(e.reason),
        depth: String(e.depth),
        input: String(e.input_tokens),
        output: String(e.output_tokens),
      });
    case "propagate": {
      const n = Array.isArray(e.targets) ? e.targets.length : 0;
      const inhibited =
        typeof e.inhibited === "number" && e.inhibited > 0
          ? translate(locale, "timeline.inhibitedPart", { count: e.inhibited })
          : "";
      return translate(locale, "timeline.propagateSummary", {
        source: String(e.source),
        count: n,
      }) + inhibited;
    }
    case "succession":
      return `${e.old} → ${e.new}`;
    case "resonance":
      return translate(locale, "timeline.resonanceSummary", {
        same: String(e.same_project_pairs),
        cross: String(e.cross_pairs),
      });
    case "decay":
      return translate(locale, "timeline.decaySummary", {
        decayed: String(e.decayed),
        pruned: String(e.pruned),
      });
    case "budget_exhausted":
      return translate(locale, "timeline.pendingSummary", {
        pending: String(e.pending),
      });
    case "topology": {
      const n = Array.isArray(e.engrams) ? e.engrams.length : 0;
      const m = Array.isArray(e.edges) ? e.edges.length : 0;
      return translate(locale, "timeline.snapshot", {
        tick: String(e.tick),
        nodes: n,
        edges: m,
      });
    }
    case "route": {
      const canary =
        e.canary != null
          ? ` (${translate(locale, "timeline.canary", { value: String(e.canary) })})`
          : "";
      return `${e.caller} ⇒ ${e.target}${canary} · T=${e.temperature}`;
    }
    case "delegation":
      return `${e.caller} ⇒ ${e.target} · ${e.mode}`;
    case "router_learn":
      return translate(locale, "timeline.routingLearn", {
        count: String(e.pairwise_updates),
      });
    case "claustrum_learn":
      return (
        translate(locale, "timeline.tuningLearn", {
          reward: String(e.mean_reward),
          noise: String(e.noise),
        }) + (e.cloned === true ? ` · ${translate(locale, "timeline.cloned")}` : "")
      );
    default: {
      const { t: _t, type: _type, tMs: _tMs, ...payload } = e;
      const s = JSON.stringify(payload);
      return s.length > 64 ? `${s.slice(0, 63)}…` : s;
    }
  }
}

function typeColor(e: IndexedEvent): string {
  if (e.type === "pulse") return reasonColor(String(e.reason));
  return TYPE_COLORS[e.type] ?? "#8a8f98";
}

/** 光标附近的事件窗口，当前事件高亮；点击任意行跳转。 */
export function Timeline() {
  const { locale, t } = useI18n();
  const run = useViewer((s) => s.run);
  const idx = useViewer((s) =>
    s.run === null ? -1 : eventIndexAt(s.run, s.cursorMs),
  );

  if (run === null) return null;

  const lo = Math.max(0, idx - WINDOW);
  const hi = Math.min(run.events.length, idx + WINDOW + 1);
  const slice = run.events.slice(lo, hi);

  return (
    <div className="timeline">
      <div className="timeline-head">
        {t("timeline.title")} {idx + 1} / {run.events.length}
      </div>
      <div className="timeline-rows">
        {slice.map((e, i) => {
          const globalIndex = lo + i;
          return (
            <div
              key={globalIndex}
              className={
                globalIndex === idx ? "timeline-row current" : "timeline-row"
              }
              onClick={() => useViewer.getState().seek(e.tMs)}
            >
              <span className="dot" style={{ background: typeColor(e) }} />
              <span className="ts">
                {new Date(e.tMs).toISOString().slice(11, 23)}
              </span>
              <span className="etype">{e.type}</span>
              <span className="summary">{summarize(e, locale)}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

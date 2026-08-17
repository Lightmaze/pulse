import { useEffect, useRef, useState } from "react";
import { useI18n } from "../i18n";
import {
  parseActive,
  parseDelegations,
  parseHealth,
  parseTuning,
  useEndpoint,
  useFiringLog,
  useRailStream,
  useRuntimeBase,
  type StreamEvent,
} from "../pulse";
import { RailActive } from "./RailActive";
import { RailStrip } from "./RailStrip";
import { RailTuning } from "./RailTuning";
import { RailSteering } from "./RailSteering";
import { RailTrace } from "./RailTrace";
import { RailDelegate } from "./RailDelegate";

const WINDOW_SEC = 300;
const COLLAPSE_KEY = "pulse.rail.collapsed:v1";
const VIEW_KEY = "pulse.rail.view:v1";

type RailView = "trace" | "field";

function readStored(key: string): string | null {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function store(key: string, value: string): void {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // View state remains valid for this session.
  }
}

/**
 * The right rail — 调律流 + 委派流.
 *
 * Three panes, three streams: 左 = 工作区导航, 中 = 内容流 (the conversation),
 * 右 = the two streams that are *not* content. That is why this is a rail and
 * not a dashboard: 轨迹观测 already exists for metrics, and the rail carries
 * only what a person would act on — who is firing, in what order, and the knobs
 * that change when (never what).
 *
 * Collapsing it stops every poll and closes the stream. That is deliberate: a
 * collapsed rail is not watching, and it must not pretend to hold live numbers.
 */
export function PulseRail() {
  const { t } = useI18n();
  const [collapsed, setCollapsed] = useState(
    () => readStored(COLLAPSE_KEY) === "1",
  );
  const [view, setView] = useState<RailView>(
    () => (readStored(VIEW_KEY) === "field" ? "field" : "trace"),
  );
  const [nowMs, setNowMs] = useState(() => Date.now());
  const [selected, setSelected] = useState<string | null>(null);
  const lastFire = useRef(new Map<string, number>());

  const runtimeBase = useRuntimeBase();
  const base = collapsed ? null : runtimeBase;

  const health = useEndpoint(base, "/health", parseHealth, 10_000);
  const active = useEndpoint(base, "/pulse/active", parseActive, 2_000);
  const tuning = useEndpoint(base, "/tuning", parseTuning, 5_000);
  const delegations = useEndpoint(base, "/delegations?limit=20", parseDelegations, 15_000);
  const log = useFiringLog(base, WINDOW_SEC);

  const onEvent = (e: StreamEvent) => {
    const p = e.payload;
    switch (e.type) {
      case "pulse": {
        const id =
          typeof p.engram_id === "string"
            ? p.engram_id
            : typeof p.engram === "string"
              ? p.engram
              : null;
        if (id === null) return;
        const parsed = Date.parse(typeof p.t === "string" ? p.t : "");
        const tMs = Number.isFinite(parsed) ? parsed : Date.now();
        const kind =
          typeof p.kind === "string"
            ? p.kind
            : typeof p.reason === "string"
              ? p.reason
              : "unknown";
        log.push({ engramId: id, tMs, kind });
        // Only recent firings drive the flash; a snapshot replay of an old run
        // must not light up rows that have been silent for an hour.
        if (Date.now() - tMs < 10_000) lastFire.current.set(id, tMs);
        break;
      }
      case "tuning_applied":
        // Contract §4: this frame is what turns `commanded` into `observed`.
        tuning.reload();
        break;
      case "delegation":
        delegations.reload();
        break;
      default:
        break;
    }
  };

  const stream = useRailStream(base, onEvent);

  useEffect(() => {
    const timer = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  const toggle = () => {
    const next = !collapsed;
    setCollapsed(next);
    store(COLLAPSE_KEY, next ? "1" : "0");
  };

  if (collapsed) {
    return (
      <aside className="rail collapsed">
        <button className="rail-toggle" onClick={toggle} title={t("rail.expand")}>
          ‹
        </button>
        <div className="rail-spine">{t("rail.title")}</div>
      </aside>
    );
  }

  const hostUp = health.state === "ok";
  const link = ((): { cls: string; text: string } => {
    if (health.state === "loading" || health.state === "idle")
      return { cls: "probing", text: t("rail.probing") };
    if (health.state !== "ok")
      return { cls: "offline", text: t("rail.offline") };
    if (stream === "open") return { cls: "online", text: t("rail.streamOnline") };
    if (stream === "connecting")
      return { cls: "probing", text: t("rail.streamConnecting") };
    // Reachable but not streaming: the numbers are still true, just up to one
    // poll old. Saying "online" flat out would overstate it.
    return { cls: "degraded", text: t("rail.polling") };
  })();

  const roster = active.data;
  const labels = new Map((roster ?? []).map((e) => [e.engram_id, e.name ?? e.engram_id]));

  const activeBand = (
    <RailActive
      active={active}
      hostUp={hostUp}
      lastFireMs={lastFire.current}
      nowMs={nowMs}
      selected={selected}
      onSelect={(id) => setSelected(id === selected ? null : id)}
      base={base}
      onIdentitySaved={active.reload}
    />
  );

  return (
    <aside className="rail">
      <div className="rail-head">
        <span className="rail-title">{t("rail.title")}</span>
        <span className="rail-live-mark">
          <span className="rail-live-dot" />
          {t("rail.live")}
        </span>
        <span className={`rail-link ${link.cls}`} title={health.detail ?? undefined}>
          {link.text}
        </span>
        <button className="rail-toggle" onClick={toggle} title={t("rail.collapse")}>
          ›
        </button>
      </div>
      {hostUp && health.data !== null && health.data.version !== null && (
        <div className="rail-version">runtime {health.data.version}</div>
      )}

      <div className="rail-tabs" role="tablist">
        <button
          role="tab"
          aria-selected={view === "trace"}
          className={view === "trace" ? "active" : ""}
          title={t("rail.traceTabHint")}
          onClick={() => {
            setView("trace");
            store(VIEW_KEY, "trace");
          }}
        >
          {t("rail.traceTab")}
        </button>
        <button
          role="tab"
          aria-selected={view === "field"}
          className={view === "field" ? "active" : ""}
          title={t("rail.fieldTabHint")}
          onClick={() => {
            setView("field");
            store(VIEW_KEY, "field");
          }}
        >
          {t("rail.fieldTab")}
        </button>
      </div>

      <div className={`rail-body rail-view-${view}`}>
        {activeBand}
        {view === "trace" ? (
          <RailTrace
            log={log}
            labels={labels}
            hostUp={hostUp}
            onSelect={(id) => setSelected(id === selected ? null : id)}
          />
        ) : (
          <>
            <RailStrip
              log={log}
              roster={(roster ?? []).map((e) => e.engram_id)}
              labels={labels}
              windowSec={WINDOW_SEC}
              hostUp={hostUp}
              onSelect={(id) => setSelected(id === selected ? null : id)}
            />
            <RailTuning
              tuning={tuning}
              base={base}
              hostUp={hostUp}
              onApplied={tuning.reload}
            />
            <RailSteering />
            <RailDelegate
              delegations={delegations}
              roster={roster}
              base={base}
              hostUp={hostUp}
              selected={selected}
              onSent={delegations.reload}
            />
          </>
        )}
      </div>
    </aside>
  );
}

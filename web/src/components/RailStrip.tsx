import { useEffect, useRef, useState } from "react";
import { useI18n } from "../i18n";
import type { FiringLog, FiringMark } from "../pulse";
import { useElementSize } from "../useElementSize";
import { reasonColor } from "../types";
import { RailBand, RailFault, RailIdle, RailWaiting } from "./RailParts";

const LABEL_W = 70;
const AXIS_H = 15;
const MAX_STRIP_H = 190;

/** Contract §3 kind names mapped onto the observatory's existing pulse palette,
 *  so the rail and 轨迹观测 never disagree about what a colour means. */
const KIND_REASON: Record<string, string> = {
  spontaneous: "spontaneous",
  propagated: "propagation",
  propagation: "propagation",
  injected: "external",
  external: "external",
};

function kindColor(kind: string): string {
  return reasonColor(KIND_REASON[kind] ?? kind);
}

function shortSpan(sec: number): string {
  return sec >= 120 ? `${Math.round(sec / 60)}m` : `${Math.round(sec)}s`;
}

interface Rows {
  ids: string[];
  silent: Set<string>;
}

/** Row order = first firing inside the window, which makes a propagation
 *  cascade read as a diagonal. Roster members that never fired sit below,
 *  dimmed — present, but silent, and the difference is visible. */
function rowsFor(marks: FiringMark[], roster: string[]): Rows {
  const ids: string[] = [];
  const seen = new Set<string>();
  for (const m of marks) {
    if (seen.has(m.engramId)) continue;
    seen.add(m.engramId);
    ids.push(m.engramId);
  }
  const silent = new Set<string>();
  for (const id of roster) {
    if (seen.has(id)) continue;
    seen.add(id);
    silent.add(id);
    ids.push(id);
  }
  return { ids, silent };
}

function draw(
  canvas: HTMLCanvasElement,
  width: number,
  marks: FiringMark[],
  rows: Rows,
  windowSec: number,
  rowH: number,
  labels: Map<string, string>,
  nowLabel: string,
) {
  const plotW = Math.max(width - LABEL_W - 4, 30);
  const bodyH = rows.ids.length * rowH;
  const height = bodyH + AXIS_H;
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.floor(width * dpr);
  canvas.height = Math.floor(height * dpr);
  canvas.style.width = `${width}px`;
  canvas.style.height = `${height}px`;
  const ctx = canvas.getContext("2d");
  if (ctx === null) return;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, width, height);

  const now = Date.now();
  const spanMs = windowSec * 1000;
  const xOf = (tMs: number) => LABEL_W + ((tMs - (now - spanMs)) / spanMs) * plotW;

  // Rows + names.
  ctx.font = "9px ui-monospace, monospace";
  ctx.textBaseline = "middle";
  rows.ids.forEach((id, r) => {
    const y = r * rowH;
    ctx.fillStyle = r % 2 === 0 ? "rgba(255,255,255,0.025)" : "transparent";
    ctx.fillRect(LABEL_W, y, plotW, rowH);
    ctx.fillStyle = rows.silent.has(id) ? "#4b515a" : "#8a919a";
    const name = labels.get(id) ?? id;
    ctx.fillText(name.length > 11 ? `${name.slice(0, 10)}…` : name, 2, y + rowH / 2, LABEL_W - 6);
  });

  // Minute gridlines — orientation only, deliberately faint.
  ctx.strokeStyle = "rgba(255,255,255,0.05)";
  ctx.lineWidth = 1;
  for (let s = 60; s < windowSec; s += 60) {
    const x = Math.round(xOf(now - s * 1000)) + 0.5;
    ctx.beginPath();
    ctx.moveTo(x, 0);
    ctx.lineTo(x, bodyH);
    ctx.stroke();
  }

  // Firing marks. One rectangle per firing, never a smoothed rate: the gaps
  // are the information, because firing order is what STDP consumes.
  const rowOf = new Map(rows.ids.map((id, i) => [id, i]));
  const markH = Math.max(rowH - 3, 2);
  for (const m of marks) {
    const r = rowOf.get(m.engramId);
    if (r === undefined) continue;
    ctx.fillStyle = kindColor(m.kind);
    ctx.fillRect(Math.round(xOf(m.tMs)) - 1, r * rowH + 1.5, 2, markH);
  }

  // The now edge.
  ctx.strokeStyle = "rgba(233, 84, 84, 0.85)";
  ctx.beginPath();
  ctx.moveTo(LABEL_W + plotW - 0.5, 0);
  ctx.lineTo(LABEL_W + plotW - 0.5, bodyH);
  ctx.stroke();

  // Axis.
  ctx.fillStyle = "#4b515a";
  ctx.fillRect(LABEL_W, bodyH, plotW, 1);
  ctx.font = "9px ui-monospace, monospace";
  ctx.textAlign = "left";
  ctx.fillText(`-${shortSpan(windowSec)}`, LABEL_W, bodyH + 8);
  ctx.textAlign = "center";
  ctx.fillText(`-${shortSpan(windowSec / 2)}`, LABEL_W + plotW / 2, bodyH + 8);
  ctx.textAlign = "right";
  ctx.fillText(nowLabel, LABEL_W + plotW, bodyH + 8);
  ctx.textAlign = "left";
}

/**
 * 激活历史 — the firing-order strip (contract §3, GET /pulse/history).
 *
 * A piano roll, not a curve. Reuses the observatory Raster's drawing idiom
 * (canvas, one row per engram, 2px marks, colour = kind) and its palette, but
 * not the component: Raster is bound to the replay `LoadedRun` in the store and
 * to the playback cursor, and this strip is a rolling wall-clock window over a
 * live endpoint with neither.
 */
export function RailStrip({
  log,
  roster,
  labels,
  windowSec,
  hostUp,
  onSelect,
}: {
  log: FiringLog;
  roster: string[];
  labels: Map<string, string>;
  windowSec: number;
  hostUp: boolean;
  onSelect: (id: string) => void;
}) {
  const { t } = useI18n();
  const boxRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const size = useElementSize(boxRef);
  const hoverId = useRef("");
  const [hover, setHover] = useState("");
  const [, setTick] = useState(0);

  const { marks } = log;
  const rows = rowsFor(marks, roster);
  const rowH = rows.ids.length > 14 ? 8 : 12;
  const drawable = log.state === "ok" && rows.ids.length > 0 && size.w > 0;

  // The window slides even when nothing arrives; marks must age off screen.
  useEffect(() => {
    const timer = window.setInterval(() => setTick((t) => t + 1), 500);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (canvas === null || !drawable) return;
    draw(canvas, size.w, marks, rows, windowSec, rowH, labels, t("history.now"));
  });

  const onMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const r = Math.floor((e.clientY - rect.top) / rowH);
    const id = rows.ids[r];
    if (id === undefined) {
      hoverId.current = "";
      setHover("");
      return;
    }
    hoverId.current = id;
    const plotW = Math.max(size.w - LABEL_W - 4, 30);
    const spanMs = windowSec * 1000;
    const tMs = Date.now() - spanMs + ((e.clientX - rect.left - LABEL_W) / plotW) * spanMs;
    let best: FiringMark | null = null;
    for (const m of marks) {
      if (m.engramId !== id) continue;
      if (best === null || Math.abs(m.tMs - tMs) < Math.abs(best.tMs - tMs)) best = m;
    }
    const near = best !== null && Math.abs(best.tMs - tMs) < (spanMs / plotW) * 4;
    setHover(
      near && best !== null
        ? `${id} · ${best.kind} · ${t("common.secondsAgo", {
            count: Math.max(Math.round((Date.now() - best.tMs) / 1000), 0),
          })}`
        : `${id}${rows.silent.has(id) ? ` · ${t("history.silent")}` : ""}`,
    );
  };

  const body = (() => {
    if (log.state === "failed") {
      return (
        <RailFault
          kind="offline"
          title={hostUp ? t("history.readFailed") : t("rail.offline")}
          detail={log.detail}
          remedy={log.remedy}
          onRetry={log.reload}
        />
      );
    }
    if (log.state === "absent") {
      return (
        <RailFault
          kind="absent"
          title={t("history.routeAbsent")}
          detail={log.detail}
          remedy={log.remedy}
          onRetry={log.reload}
        />
      );
    }
    if (log.state !== "ok") return <RailWaiting>{t("history.loading")}</RailWaiting>;
    if (rows.ids.length === 0) return <RailIdle>{t("history.none")}</RailIdle>;

    return (
      <>
        <div className="rail-strip-box" style={{ maxHeight: MAX_STRIP_H }}>
          <canvas
            ref={canvasRef}
            onMouseMove={onMove}
            onMouseLeave={() => {
              hoverId.current = "";
              setHover("");
            }}
            onClick={() => {
              if (hoverId.current !== "") onSelect(hoverId.current);
            }}
          />
          {marks.length === 0 && (
            <div className="rail-strip-quiet">{t("history.quiet")}</div>
          )}
        </div>
        <div className="rail-strip-legend">
          {(["spontaneous", "propagated", "injected"] as const).map((k) => (
            <span key={k}>
              <span className="dot" style={{ background: kindColor(k) }} />
              {t(
                k === "spontaneous"
                  ? "activity.spontaneous"
                  : k === "propagated"
                    ? "activity.propagated"
                    : "activity.injected",
              )}
            </span>
          ))}
        </div>
        <div className="rail-strip-hover">{hover}</div>
      </>
    );
  })();

  return (
    <RailBand
      title={t("history.title")}
      subtitle={t("history.subtitle")}
      note={
        log.state === "ok" ? (
          <span className={marks.length === 0 ? "" : "hot"}>
            {t("history.count", { count: marks.length, span: shortSpan(windowSec) })}
          </span>
        ) : (
          <span className="unknown">—</span>
        )
      }
    >
      {/* The measured box is mounted unconditionally: ResizeObserver only
          attaches once, so a ref that appears later (when the endpoint finally
          answers) would never get its first measurement and the canvas would
          stay at the 300x150 default. */}
      <div className="rail-strip-wrap" ref={boxRef}>
        {body}
      </div>
    </RailBand>
  );
}

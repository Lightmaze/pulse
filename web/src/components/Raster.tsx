import { useEffect, useRef, useState } from "react";
import { useViewer } from "../store";
import { useElementSize } from "../useElementSize";
import type { LoadedRun } from "../types";
import { reasonColor } from "../types";

const LABEL_W = 128;
const MIN_ROW_H = 3;
const MAX_ROW_H = 16;

interface Layout {
  rowH: number;
  canvasH: number;
  plotW: number;
}

function layoutFor(run: LoadedRun, width: number, height: number): Layout {
  const rows = Math.max(run.engrams.length, 1);
  const rowH = Math.min(Math.max(Math.floor(height / rows), MIN_ROW_H), MAX_ROW_H);
  return {
    rowH,
    canvasH: Math.max(rows * rowH, height),
    plotW: Math.max(width - LABEL_W, 50),
  };
}

function xOf(run: LoadedRun, layout: Layout, tMs: number): number {
  const span = Math.max(run.tMaxMs - run.tMinMs, 1);
  return LABEL_W + ((tMs - run.tMinMs) / span) * layout.plotW;
}

function drawStatic(
  canvas: HTMLCanvasElement,
  run: LoadedRun,
  layout: Layout,
  width: number,
) {
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.floor(width * dpr);
  canvas.height = Math.floor(layout.canvasH * dpr);
  canvas.style.width = `${width}px`;
  canvas.style.height = `${layout.canvasH}px`;
  const ctx = canvas.getContext("2d");
  if (ctx === null) return;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, width, layout.canvasH);

  const { rowH } = layout;

  // Row separators + labels. Skip labels when rows are too dense to read.
  ctx.font = "10px ui-monospace, monospace";
  ctx.textBaseline = "middle";
  const labelEvery = rowH >= 10 ? 1 : Math.ceil(10 / rowH);
  for (let r = 0; r < run.engrams.length; r++) {
    const y = r * rowH;
    ctx.fillStyle = r % 2 === 0 ? "rgba(255,255,255,0.02)" : "transparent";
    ctx.fillRect(LABEL_W, y, layout.plotW, rowH);
    if (r % labelEvery === 0) {
      ctx.fillStyle = "#8a919a";
      const label = run.engrams[r];
      ctx.fillText(
        label.length > 18 ? `${label.slice(0, 17)}…` : label,
        4,
        y + rowH / 2,
        LABEL_W - 8,
      );
    }
  }

  // Pulse marks.
  const markH = Math.max(rowH - 2, 2);
  for (const p of run.pulses) {
    ctx.fillStyle = reasonColor(p.reason);
    ctx.fillRect(xOf(run, layout, p.tMs) - 1, p.row * rowH + 1, 2, markH);
  }

  // Succession: hollow tick on the old row, solid on the new row.
  ctx.strokeStyle = "#e8e8e8";
  ctx.fillStyle = "#e8e8e8";
  for (const s of run.successions) {
    const oldRow = run.engrams.indexOf(s.oldId);
    const newRow = run.engrams.indexOf(s.newId);
    const x = xOf(run, layout, s.tMs);
    if (oldRow >= 0) ctx.strokeRect(x - 2, oldRow * rowH + 1, 4, markH);
    if (newRow >= 0) ctx.fillRect(x - 2, newRow * rowH + 1, 4, markH);
  }
}

/** 脉冲栅格：行 = Engram，横轴 = 时间，颜色 = PulseReason。 */
export function Raster() {
  const run = useViewer((s) => s.run);
  const scrollRef = useRef<HTMLDivElement>(null);
  const staticRef = useRef<HTMLCanvasElement>(null);
  const overlayRef = useRef<HTMLCanvasElement>(null);
  const size = useElementSize(scrollRef);
  const [hover, setHover] = useState<string>("");

  useEffect(() => {
    const staticCanvas = staticRef.current;
    const overlay = overlayRef.current;
    if (staticCanvas === null || overlay === null) return;
    if (run === null || size.w === 0 || size.h === 0) return;

    const layout = layoutFor(run, size.w, size.h);
    drawStatic(staticCanvas, run, layout, size.w);

    const dpr = window.devicePixelRatio || 1;
    overlay.width = Math.floor(size.w * dpr);
    overlay.height = Math.floor(layout.canvasH * dpr);
    overlay.style.width = `${size.w}px`;
    overlay.style.height = `${layout.canvasH}px`;
    const ctx = overlay.getContext("2d");
    if (ctx === null) return;
    ctx.scale(dpr, dpr);

    const drawCursor = (cursorMs: number) => {
      ctx.clearRect(0, 0, size.w, layout.canvasH);
      const x = xOf(run, layout, cursorMs);
      ctx.strokeStyle = "rgba(233, 84, 84, 0.9)";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(x, 0);
      ctx.lineTo(x, layout.canvasH);
      ctx.stroke();
    };
    drawCursor(useViewer.getState().cursorMs);
    const unsubscribe = useViewer.subscribe((s) => s.cursorMs, drawCursor);
    return unsubscribe;
  }, [run, size]);

  const onMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    if (run === null || size.w === 0) return;
    const layout = layoutFor(run, size.w, size.h);
    const rect = e.currentTarget.getBoundingClientRect();
    const y = e.clientY - rect.top;
    const rowIndex = Math.floor(y / layout.rowH);
    setHover(run.engrams[rowIndex] ?? "");
  };

  const onClick = (e: React.MouseEvent<HTMLCanvasElement>) => {
    if (run === null || size.w === 0) return;
    const layout = layoutFor(run, size.w, size.h);
    const rect = e.currentTarget.getBoundingClientRect();
    const x = e.clientX - rect.left;
    if (x < LABEL_W) {
      // The label gutter answers "who", the plot area answers "when":
      // clicking a name opens that engram's session.
      const row = Math.floor((e.clientY - rect.top) / layout.rowH);
      const id = run.engrams[row];
      if (id !== undefined) useViewer.getState().inspect(id);
      return;
    }
    const span = Math.max(run.tMaxMs - run.tMinMs, 1);
    useViewer.getState().seek(run.tMinMs + ((x - LABEL_W) / layout.plotW) * span);
  };

  return (
    <div className="raster">
      <div className="raster-scroll" ref={scrollRef}>
        <div className="raster-stack">
          <canvas ref={staticRef} />
          <canvas
            ref={overlayRef}
            onMouseMove={onMouseMove}
            onMouseLeave={() => setHover("")}
            onClick={onClick}
          />
        </div>
      </div>
      <div className="raster-hover">{hover}</div>
    </div>
  );
}

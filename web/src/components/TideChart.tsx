import { useEffect, useRef } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { useViewer } from "../store";
import type { LoadedRun } from "../types";

const AXIS_STYLE: Partial<uPlot.Axis> = {
  stroke: "#8a919a",
  grid: { stroke: "#1f242c", width: 1 },
  ticks: { stroke: "#2a303a", width: 1 },
};

function buildOptions(width: number, height: number): uPlot.Options {
  return {
    width,
    height,
    padding: [8, 8, 0, 0],
    cursor: { drag: { x: false, y: false } },
    legend: { live: true },
    scales: {
      x: { time: true },
      u: { range: [0, 1.05] },
      n: { range: (_u, _min, max) => [0, Math.max(max, 1)] },
    },
    axes: [
      { ...AXIS_STYLE },
      { ...AXIS_STYLE, scale: "u", label: "0..1" },
      { ...AXIS_STYLE, scale: "n", side: 1, label: "pending" },
    ],
    series: [
      {},
      { label: "ratio", scale: "u", stroke: "#5b9dd9", width: 1.5 },
      { label: "coherent", scale: "u", stroke: "#f2a541", width: 1.5 },
      { label: "breadth", scale: "u", stroke: "#6fc276", width: 1.5 },
      { label: "pending", scale: "n", stroke: "#8a8f98", width: 1, dash: [4, 4] },
    ],
  };
}

function buildData(run: LoadedRun): uPlot.AlignedData {
  const hb = run.heartbeats;
  return [hb.tSec, hb.ratio, hb.coherent, hb.breadth, hb.pending] as uPlot.AlignedData;
}

/** heartbeat 序列：ratio / coherent / breadth（左轴 0..1），pending（右轴）。 */
export function TideChart() {
  const run = useViewer((s) => s.run);
  const wrapRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);

  useEffect(() => {
    const wrap = wrapRef.current;
    if (wrap === null || run === null) return;

    const chart = new uPlot(
      buildOptions(wrap.clientWidth, wrap.clientHeight),
      buildData(run),
      wrap,
    );
    chartRef.current = chart;

    // Playback cursor: a positioned line inside the plot area, no redraw needed.
    const line = document.createElement("div");
    line.className = "playback-line";
    chart.over.appendChild(line);
    const positionLine = (cursorMs: number) => {
      const px = chart.valToPos(cursorMs / 1000, "x");
      const visible = px >= 0 && px <= chart.over.clientWidth;
      line.style.display = visible ? "block" : "none";
      if (visible) line.style.left = `${px}px`;
    };
    positionLine(useViewer.getState().cursorMs);
    const unsubscribe = useViewer.subscribe((s) => s.cursorMs, positionLine);

    const ro = new ResizeObserver(() => {
      chart.setSize({ width: wrap.clientWidth, height: wrap.clientHeight });
      positionLine(useViewer.getState().cursorMs);
    });
    ro.observe(wrap);

    return () => {
      ro.disconnect();
      unsubscribe();
      chartRef.current = null;
      chart.destroy();
    };
  }, [run]);

  // Seek on click. Capture phase on the wrapper, NOT a listener on chart.over:
  // uPlot registers its own click handler there first and calls
  // stopImmediatePropagation, which silently eats any later-attached listener.
  const onClickCapture = (e: React.MouseEvent) => {
    const chart = chartRef.current;
    if (chart === null) return;
    const rect = chart.over.getBoundingClientRect();
    const left = e.clientX - rect.left;
    if (left < 0 || left > rect.width) return; // axes / legend clicks
    useViewer.getState().seek(chart.posToVal(left, "x") * 1000);
  };

  return <div className="tide-chart" ref={wrapRef} onClickCapture={onClickCapture} />;
}

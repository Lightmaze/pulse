import { useEffect, useMemo, useRef } from "react";
import {
  forceCenter,
  forceCollide,
  forceLink,
  forceManyBody,
  forceSimulation,
  type SimulationLinkDatum,
  type SimulationNodeDatum,
} from "d3-force";
import { useViewer } from "../store";
import { lastIndexAtOrBefore } from "../parse";
import { useElementSize } from "../useElementSize";
import type { LoadedRun, TopologySnapshot } from "../types";
import { useI18n } from "../i18n";

const PROJECT_COLORS = [
  "#5b9dd9", "#f2a541", "#6fc276", "#c678dd",
  "#e06c75", "#56b6c2", "#d19a66", "#98c379",
];
const NO_PROJECT = "#6b7280";

interface LayoutNode extends SimulationNodeDatum {
  id: string;
}

interface Placed {
  positions: Map<string, { x: number; y: number }>;
  colors: Map<string, string>;
}

/** Lay out the final snapshot once: a stable frame of reference for the whole
 *  replay. Scrubbing then changes weights and activity, never node positions. */
function layout(snapshot: TopologySnapshot, w: number, h: number): Placed {
  const nodes: LayoutNode[] = snapshot.nodes.map((n) => ({ id: n.id }));
  const links: SimulationLinkDatum<LayoutNode>[] = snapshot.edges.map(([i, j]) => ({
    source: i,
    target: j,
  }));

  forceSimulation(nodes)
    .force("link", forceLink(links).distance(46).strength(0.35))
    .force("charge", forceManyBody().strength(-160))
    .force("center", forceCenter(w / 2, h / 2))
    .force("collide", forceCollide(13))
    .stop()
    .tick(320); // run to rest synchronously — no animation loop to babysit

  const positions = new Map<string, { x: number; y: number }>();
  for (const n of nodes) {
    positions.set(n.id, { x: n.x ?? w / 2, y: n.y ?? h / 2 });
  }

  const projects = [...new Set(snapshot.nodes.map((n) => n.project ?? ""))].sort();
  const colors = new Map<string, string>();
  for (const n of snapshot.nodes) {
    const key = n.project ?? "";
    colors.set(
      n.id,
      key === "" ? NO_PROJECT : PROJECT_COLORS[projects.indexOf(key) % PROJECT_COLORS.length],
    );
  }
  return { positions, colors };
}

/** Recency window scaled to the run's own tick rate — a fixed wall-clock
 *  window would read as "everything lit" on a compressed mock run and
 *  "nothing lit" on a real hour-scale one. */
function recencyWindowMs(run: LoadedRun): number {
  const t = run.heartbeats.tMs;
  if (t.length < 3) return 5_000;
  const gaps: number[] = [];
  for (let i = 1; i < t.length; i++) gaps.push(t[i] - t[i - 1]);
  gaps.sort((a, b) => a - b);
  const median = gaps[gaps.length >> 1];
  return Math.min(Math.max(median * 12, 500), 120_000);
}

export function NetworkGraph() {
  const { t } = useI18n();
  const run = useViewer((s) => s.run);
  const wrapRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const size = useElementSize(wrapRef);

  const placed = useMemo(() => {
    if (run === null || run.topology.length === 0 || size.w === 0) return null;
    return layout(run.topology[run.topology.length - 1], size.w, size.h);
  }, [run, size]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (canvas === null || run === null || placed === null) return;
    if (size.w === 0 || size.h === 0) return;

    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.floor(size.w * dpr);
    canvas.height = Math.floor(size.h * dpr);
    canvas.style.width = `${size.w}px`;
    canvas.style.height = `${size.h}px`;
    const ctx = canvas.getContext("2d");
    if (ctx === null) return;
    ctx.scale(dpr, dpr);

    const window_ = recencyWindowMs(run);
    const topoTimes = run.topology.map((s) => s.tMs);
    const pulseTimes = run.pulses.map((p) => p.tMs);
    const propTimes = run.propagations.map((p) => p.tMs);

    const draw = (cursorMs: number) => {
      ctx.clearRect(0, 0, size.w, size.h);

      // Snapshot at or before the cursor; before the first one, show the first
      // (the graph existed then too — we just had not sampled it yet).
      const si = lastIndexAtOrBefore(topoTimes, cursorMs);
      const snap = run.topology[si >= 0 ? si : 0];
      const pos = placed.positions;

      // Which engrams pulsed recently, and how recently (drives the glow).
      const recent = new Map<string, number>();
      for (let i = lastIndexAtOrBefore(pulseTimes, cursorMs); i >= 0; i--) {
        const age = cursorMs - run.pulses[i].tMs;
        if (age > window_) break;
        const id = run.engrams[run.pulses[i].row];
        const strength = 1 - age / window_;
        if ((recent.get(id) ?? 0) < strength) recent.set(id, strength);
      }

      // Edges that actually carried content recently.
      const firing = new Set<string>();
      for (let i = lastIndexAtOrBefore(propTimes, cursorMs); i >= 0; i--) {
        const p = run.propagations[i];
        if (cursorMs - p.tMs > window_) break;
        for (const target of p.targets) firing.add(`${p.source}\u0000${target}`);
      }

      // 1. Standing edges: weight -> alpha/width; inhibitory in warm red.
      for (const [i, j, weight, kind] of snap.edges) {
        const a = pos.get(snap.nodes[i]?.id ?? "");
        const b = pos.get(snap.nodes[j]?.id ?? "");
        if (a === undefined || b === undefined) continue;
        const lit = firing.has(`${snap.nodes[i].id}\u0000${snap.nodes[j].id}`);
        const inhibitory = kind === "i";
        ctx.strokeStyle = inhibitory
          ? `rgba(224, 108, 117, ${0.12 + weight * 0.35})`
          : `rgba(140, 170, 210, ${0.1 + weight * 0.4})`;
        ctx.lineWidth = lit ? 2.2 : 0.4 + weight * 1.6;
        if (lit) ctx.strokeStyle = inhibitory ? "#e06c75" : "#a8d8ff";
        ctx.setLineDash(inhibitory ? [3, 3] : []);
        ctx.beginPath();
        ctx.moveTo(a.x, a.y);
        ctx.lineTo(b.x, b.y);
        ctx.stroke();
      }
      ctx.setLineDash([]);

      // 2. Nodes: radius by cumulative pulses, glow by recency.
      for (const n of snap.nodes) {
        const p = pos.get(n.id);
        if (p === undefined) continue;
        const r = 3.5 + Math.sqrt(n.pulses) * 0.9;
        const strength = recent.get(n.id) ?? 0;
        const color = placed.colors.get(n.id) ?? NO_PROJECT;
        if (strength > 0) {
          ctx.fillStyle = color;
          ctx.globalAlpha = 0.18 * strength;
          ctx.beginPath();
          ctx.arc(p.x, p.y, r + 9 * strength, 0, Math.PI * 2);
          ctx.fill();
          ctx.globalAlpha = 1;
        }
        ctx.fillStyle = color;
        ctx.globalAlpha = strength > 0 ? 1 : 0.45;
        ctx.beginPath();
        ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
        ctx.fill();
        ctx.globalAlpha = 1;
        if (strength > 0.5) {
          ctx.strokeStyle = "#ffffff";
          ctx.lineWidth = 1;
          ctx.stroke();
        }
      }

      // 3. Caption: which snapshot is on screen.
      ctx.fillStyle = "#6b7280";
      ctx.font = "11px ui-monospace, monospace";
      ctx.fillText(
        `tick ${snap.tick} · ${snap.nodes.length} engrams · ${snap.edges.length} edges`,
        8,
        14,
      );
    };

    draw(useViewer.getState().cursorMs);
    return useViewer.subscribe((s) => s.cursorMs, draw);
  }, [run, placed, size]);

  const onClick = (e: React.MouseEvent<HTMLCanvasElement>) => {
    if (placed === null) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    let best: string | null = null;
    let bestD = 14 * 14; // click tolerance in px²
    for (const [id, p] of placed.positions) {
      const d = (p.x - x) ** 2 + (p.y - y) ** 2;
      if (d < bestD) {
        bestD = d;
        best = id;
      }
    }
    if (best !== null) useViewer.getState().inspect(best);
  };

  const empty = run !== null && run.topology.length === 0;

  return (
    <div className="network" ref={wrapRef}>
      {empty ? (
        <div className="network-empty">
          {t("network.empty")}
          <br />
          <code>topology_interval_ticks</code> {t("network.intervalDisabled")}
        </div>
      ) : (
        <canvas ref={canvasRef} onClick={onClick} />
      )}
    </div>
  );
}

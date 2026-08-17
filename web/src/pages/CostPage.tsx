import { useEffect, useMemo, useRef } from "react";
import { useI18n } from "../i18n";
import uPlot from "uplot";
import { useViewer } from "../store";
import { useElementSize } from "../useElementSize";

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

interface EngramCost {
  id: string;
  pulses: number;
  input: number;
  output: number;
  cached: number;
}

/** 成本中心：Pulse 指标事件的词元记账与缓存观测。 */
export function CostPage() {
  const { t } = useI18n();
  const run = useViewer((s) => s.run);
  const chartRef = useRef<HTMLDivElement>(null);
  const size = useElementSize(chartRef);

  const perEngram = useMemo(() => {
    if (run === null) return [];
    const acc = new Map<number, EngramCost>();
    for (const p of run.pulses) {
      let row = acc.get(p.row);
      if (row === undefined) {
        row = { id: run.engrams[p.row], pulses: 0, input: 0, output: 0, cached: 0 };
        acc.set(p.row, row);
      }
      row.pulses += 1;
      row.input += p.inputTokens;
      row.output += p.outputTokens;
      row.cached += p.cachedTokens;
    }
    return [...acc.values()].sort((a, b) => b.input - a.input);
  }, [run]);

  useEffect(() => {
    const wrap = chartRef.current;
    if (wrap === null || run === null || size.w === 0) return;
    if (run.tokenCum.tMs.length === 0) return;

    const chart = new uPlot(
      {
        width: size.w,
        height: 220,
        padding: [8, 8, 0, 0],
        cursor: { drag: { x: false, y: false } },
        scales: { x: { time: true } },
        axes: [
          { stroke: "#8a919a", grid: { stroke: "#1f242c", width: 1 } },
          {
            stroke: "#8a919a",
            grid: { stroke: "#1f242c", width: 1 },
            values: (_u, ticks) => ticks.map(fmt),
          },
        ],
        series: [
          {},
          { label: t("cost.seriesInput"), stroke: "#5b9dd9", width: 1.5 },
          { label: t("cost.seriesCached"), stroke: "#6fc276", width: 1.5 },
          { label: t("cost.seriesOutput"), stroke: "#f2a541", width: 1.5 },
        ],
      },
      [
        run.tokenCum.tMs.map((t) => t / 1000),
        run.tokenCum.input,
        run.tokenCum.cached,
        run.tokenCum.output,
      ] as uPlot.AlignedData,
      wrap,
    );
    return () => chart.destroy();
  }, [run, size, t]);

  if (run === null) {
    return (
      <div className="cost-page">
        <div className="page-title">{t("cost.title")}</div>
        <div className="inspector-note">
          {t("cost.empty")}
        </div>
      </div>
    );
  }

  const total = perEngram.reduce(
    (a, r) => ({
      input: a.input + r.input,
      output: a.output + r.output,
      cached: a.cached + r.cached,
    }),
    { input: 0, output: 0, cached: 0 },
  );
  const hitRate = total.input > 0 ? total.cached / total.input : 0;

  return (
    <div className="cost-page">
      <div className="page-title">
        {t("cost.title")}
        <span className="page-sub">
          {t("cost.subtitle")}
        </span>
      </div>
      <div className="cost-tiles">
        <div className="cost-tile">
          <div className="cost-tile-label">{t("cost.inputTokens")}</div>
          <div className="cost-tile-value">{fmt(total.input)}</div>
        </div>
        <div className="cost-tile">
          <div className="cost-tile-label">{t("cost.cachedTokens")}</div>
          <div className="cost-tile-value">{fmt(total.cached)}</div>
        </div>
        <div className="cost-tile">
          <div className="cost-tile-label">{t("cost.outputTokens")}</div>
          <div className="cost-tile-value">{fmt(total.output)}</div>
        </div>
        <div className="cost-tile">
          <div className="cost-tile-label">{t("cost.cacheHit")}</div>
          <div className="cost-tile-value">{(hitRate * 100).toFixed(1)}%</div>
        </div>
      </div>
      <div className="cost-chart" ref={chartRef} />
      <table className="cost-table">
        <thead>
          <tr>
            <th>{t("cost.engram")}</th>
            <th>{t("cost.pulses")}</th>
            <th>{t("cost.input")}</th>
            <th>{t("cost.cached")}</th>
            <th>{t("cost.output")}</th>
            <th>{t("cost.cacheHit")}</th>
          </tr>
        </thead>
        <tbody>
          {perEngram.slice(0, 60).map((r) => (
            <tr key={r.id}>
              <td className="mono">{r.id}</td>
              <td>{r.pulses}</td>
              <td>{fmt(r.input)}</td>
              <td>{fmt(r.cached)}</td>
              <td>{fmt(r.output)}</td>
              <td>{r.input > 0 ? `${((r.cached / r.input) * 100).toFixed(0)}%` : "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {perEngram.length > 60 && (
        <div className="inspector-note">
          {t("cost.topSixty", { count: perEngram.length })}
        </div>
      )}
    </div>
  );
}

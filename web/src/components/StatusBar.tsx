import { useI18n } from "../i18n";
import { useViewer } from "../store";
import { lastIndexAtOrBefore } from "../parse";

function fmtTokens(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

/** 光标处的系统快照：最近一次 heartbeat + 累计脉冲与 token。 */
export function StatusBar() {
  const { t } = useI18n();
  const run = useViewer((s) => s.run);
  const hbIdx = useViewer((s) =>
    s.run === null ? -1 : lastIndexAtOrBefore(s.run.heartbeats.tMs, s.cursorMs),
  );
  const pulseIdx = useViewer((s) =>
    s.run === null ? -1 : lastIndexAtOrBefore(s.run.tokenCum.tMs, s.cursorMs),
  );

  if (run === null) return null;

  const hb = hbIdx >= 0 ? hbIdx : null;
  const fmt = (v: number | null | undefined) => (v === null || v === undefined ? "—" : v);
  const cumIn = pulseIdx >= 0 ? run.tokenCum.input[pulseIdx] : 0;
  const cumOut = pulseIdx >= 0 ? run.tokenCum.output[pulseIdx] : 0;
  const cumCached = pulseIdx >= 0 ? run.tokenCum.cached[pulseIdx] : 0;

  return (
    <div className="status-bar">
      <span className="run-name" title={run.name}>{run.name}</span>
      <span>
        n/N{" "}
        <b>
          {hb === null ? "—" : `${fmt(run.heartbeats.active[hb])}/${fmt(run.heartbeats.total[hb])}`}
        </b>
      </span>
      <span>{t("status.ratio")} <b>{hb === null ? "—" : fmt(run.heartbeats.ratio[hb])}</b></span>
      <span>{t("status.coherent")} <b>{hb === null ? "—" : fmt(run.heartbeats.coherent[hb])}</b></span>
      <span>{t("status.breadth")} <b>{hb === null ? "—" : fmt(run.heartbeats.breadth[hb])}</b></span>
      <span>{t("status.pending")} <b>{hb === null ? "—" : fmt(run.heartbeats.pending[hb])}</b></span>
      <span className="sep" />
      <span>
        {t("status.pulses")} <b>{pulseIdx + 1}</b>/{run.pulses.length}
      </span>
      <span>
        {t("status.tokenInput")} <b>{fmtTokens(cumIn)}</b> · {t("status.tokenOutput")} <b>{fmtTokens(cumOut)}</b> · {t("status.cached")}{" "}
        <b>{fmtTokens(cumCached)}</b>
      </span>
      <span className="sep" />
      <span>
        {t("status.engrams")} <b>{run.engrams.length}</b> · {t("status.events")} <b>{run.events.length}</b>
        {run.skipped > 0 ? ` · ${t("status.skipped", { count: run.skipped })}` : ""}
      </span>
    </div>
  );
}

import { useEffect, useState } from "react";
import { useI18n } from "../i18n";
import {
  faultText,
  postDelegate,
  type ActiveEngram,
  type DelegationRow,
  type Fetched,
} from "../pulse";
import { RailBand, RailFault, RailIdle, RailWaiting, relTime } from "./RailParts";

const BACKENDS = ["pi", "local"];

/**
 * 委派 — the tunnel stream (contract §2.3).
 *
 * There is no task box here, and that is the point. Routing is about *objects*
 * — who does the work, on which backend — so this band only ever changes `to`
 * and `backend`. 重投 re-issues an existing delegation's task **verbatim** to a
 * different addressee; the rail never composes a task string, because text that
 * lands in an engram's context is content, and content is the centre pane's
 * business (§0, the free-context rule).
 *
 * `to: null` hands the choice to the delegation router, so the ledger has to show what
 * the router picked and why — otherwise the tunnel stream is a black box.
 */
export function RailDelegate({
  delegations,
  roster,
  base,
  hostUp,
  selected,
  onSent,
}: {
  delegations: Fetched<DelegationRow[]>;
  roster: ActiveEngram[] | null;
  base: string | null;
  hostUp: boolean;
  selected: string | null;
  onSent: () => void;
}) {
  const { locale, t } = useI18n();
  const [target, setTarget] = useState("");
  const [backend, setBackend] = useState("");
  const [sending, setSending] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [ack, setAck] = useState<string | null>(null);
  const now = Date.now();

  // Picking an engram in 当前活跃 designates it as the delegation target — the
  // two bands are the same stream seen from two ends.
  useEffect(() => {
    if (selected !== null) setTarget(selected);
  }, [selected]);

  const reroute = (row: DelegationRow) => {
    if (base === null) return;
    setSending(row.id);
    setError(null);
    setAck(null);
    postDelegate(base, {
      task: row.task, // verbatim — the rail re-addresses, it does not author
      to: target === "" ? null : target,
      backend: backend === "" ? null : backend,
    })
      .then((id) => setAck(id ?? ""))
      .catch((e: unknown) => setError(faultText(e)))
      .finally(() => {
        setSending(null);
        onSent();
      });
  };

  const rows = delegations.data;

  const body = (() => {
    if (delegations.state === "failed") {
      return (
        <RailFault
          kind="offline"
          title={hostUp ? t("delegation.readFailed") : t("rail.offline")}
          detail={delegations.detail}
          remedy={delegations.remedy}
          onRetry={delegations.reload}
        />
      );
    }
    if (delegations.state === "absent") {
      return (
        <RailFault
          kind="absent"
          title={t("delegation.routeAbsent")}
          detail={delegations.detail}
          remedy={delegations.remedy}
          onRetry={delegations.reload}
        />
      );
    }
    if (rows === null) return <RailWaiting>{t("delegation.loading")}</RailWaiting>;

    return (
      <>
        <div className="route-picker">
          <label>
            <span>{t("delegation.to")}</span>
            <select value={target} onChange={(e) => setTarget(e.target.value)}>
              <option value="">{t("delegation.routerDecides")}</option>
              {(roster ?? []).map((e) => (
                <option key={e.engram_id} value={e.engram_id}>
                  {e.nickname ?? e.name ?? e.engram_id}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>{t("delegation.substrate")}</span>
            <select value={backend} onChange={(e) => setBackend(e.target.value)}>
              <option value="">{t("delegation.routerDecides")}</option>
              {BACKENDS.map((b) => (
                <option key={b} value={b}>
                  {b}
                </option>
              ))}
            </select>
          </label>
        </div>
        <div className="rail-note">
          {t("delegation.boundary")}
        </div>

        {rows.length === 0 ? (
          <RailIdle>{t("delegation.none")}</RailIdle>
        ) : (
          rows.map((d) => (
            <div className="deleg" key={d.id}>
              <div className="deleg-top">
                <span className={`deleg-status ${d.status ?? "unknown"}`}>
                  {d.status ?? "—"}
                </span>
                <span className="deleg-when">{relTime(d.created_at, now, locale)}</span>
                <button
                  className="deleg-reroute"
                  disabled={base === null || sending !== null}
                  title={t("delegation.rerouteHint")}
                  onClick={() => reroute(d)}
                >
                  {sending === d.id ? t("delegation.rerouting") : t("delegation.reroute")}
                </button>
              </div>
              <div className="deleg-task" title={d.task}>
                {d.task === "" ? t("delegation.emptyTask") : d.task}
              </div>
              <div className="deleg-route">
                <span className="deleg-chosen">→ {d.chosen ?? t("delegation.undecided")}</span>
                {d.backend !== null && <span className="deleg-backend">{d.backend}</span>}
                {d.temperature !== null && <span>T={d.temperature.toFixed(2)}</span>}
              </div>
              {d.candidates.length > 0 && (
                <div className="deleg-cands">
                  {d.candidates.slice(0, 4).map((c) => (
                    <span
                      key={c.engram_id}
                      className={c.engram_id === d.chosen ? "cand won" : "cand"}
                    >
                      {c.engram_id}
                      {c.score !== null ? ` ${c.score.toFixed(2)}` : ""}
                    </span>
                  ))}
                </div>
              )}
              {d.result !== null && <div className="deleg-result">{d.result}</div>}
            </div>
          ))
        )}
        {ack !== null && (
          <div className="rail-note ok">
            {ack === ""
              ? t("delegation.queued")
              : t("delegation.queuedWithId", { id: ack })}
          </div>
        )}
        {error !== null && <div className="rail-error">{error}</div>}
      </>
    );
  })();

  return (
    <RailBand
      title={t("delegation.title")}
      subtitle={t("delegation.subtitle")}
      note={
        delegations.state === "ok" && rows !== null ? (
          <span>{t("delegation.count", { count: rows.length })}</span>
        ) : (
          <span className="unknown">—</span>
        )
      }
    >
      {body}
    </RailBand>
  );
}

import { useI18n } from "../i18n";
import type { FiringLog } from "../pulse";
import { RailBand, RailFault, RailIdle, RailWaiting } from "./RailParts";

const MAX_VISIBLE = 16;

function timeLabel(tMs: number): string {
  return new Date(tMs).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

/**
 * The Trace projection is intentionally chronological. A firing mark does not
 * carry a causal parent, so this component never draws an arrow or invents one.
 * Explicit propagation edges can be added when the read contract exposes them.
 */
export function RailTrace({
  log,
  labels,
  hostUp,
  onSelect,
}: {
  log: FiringLog;
  labels: Map<string, string>;
  hostUp: boolean;
  onSelect: (id: string) => void;
}) {
  const { t } = useI18n();

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
    if (log.marks.length === 0) return <RailIdle>{t("activity.quiet")}</RailIdle>;

    return (
      <div className="activity-trace">
        {log.marks.slice(-MAX_VISIBLE).map((mark, index, visible) => {
          const kindKey =
            mark.kind === "spontaneous"
              ? "activity.spontaneous"
              : mark.kind === "propagated" || mark.kind === "propagation"
                ? "activity.propagated"
                : mark.kind === "injected" || mark.kind === "external"
                  ? "activity.injected"
                  : null;
          return (
            <button
              className={`activity-event kind-${mark.kind}`}
              key={`${mark.engramId}-${mark.tMs}-${mark.kind}`}
              onClick={() => onSelect(mark.engramId)}
            >
              <span className="activity-time">{timeLabel(mark.tMs)}</span>
              <span className="activity-axis" aria-hidden="true">
                <span className="activity-node" />
                {index < visible.length - 1 && <span className="activity-line" />}
              </span>
              <span className="activity-copy">
                <span className="activity-name">{labels.get(mark.engramId) ?? mark.engramId}</span>
                <span className="activity-kind">{kindKey === null ? mark.kind : t(kindKey)}</span>
              </span>
            </button>
          );
        })}
      </div>
    );
  })();

  return (
    <RailBand
      title={t("activity.title")}
      subtitle={t("activity.subtitle")}
      note={
        log.state === "ok" ? (
          <span className={log.marks.length === 0 ? "" : "hot"}>
            {t("activity.latest", { count: log.marks.length })}
          </span>
        ) : (
          <span className="unknown">—</span>
        )
      }
    >
      {body}
    </RailBand>
  );
}

import type { ReactNode } from "react";
import { translate, useI18n, type Locale } from "../i18n";

/** One band of the rail. */
export function RailBand({
  title,
  subtitle,
  note,
  children,
}: {
  title: string;
  subtitle: string;
  note?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="rail-band">
      <div className="rail-band-head">
        <span className="rail-band-title">{title}</span>
        <span className="rail-band-sub">{subtitle}</span>
        <span className="rail-band-note">{note}</span>
      </div>
      <div className="rail-band-body">{children}</div>
    </section>
  );
}

/**
 * The rail cannot see. Two flavours, and they are genuinely different problems:
 *
 *   offline — nothing answers at this origin.
 *   absent  — the runtime answers, this route does not exist yet (404/501).
 *
 * Both refuse to draw any chart furniture, axis, roster or zero. An empty grid
 * with a clean axis says "the network is quiet", which would be a lie: the
 * truthful statement is "I cannot see the network". Contract §6 also requires
 * a remedy, so one is always rendered when the server supplied it.
 */
export function RailFault({
  kind,
  title,
  detail,
  remedy,
  onRetry,
}: {
  kind: "offline" | "absent";
  title: string;
  detail: string | null;
  remedy: string | null;
  onRetry?: () => void;
}) {
  const { t } = useI18n();
  return (
    <div className={`rail-fault ${kind}`}>
      <div className="rail-fault-title">
        <span className="rail-fault-pip" />
        {title}
      </div>
      {detail !== null && <div className="rail-fault-detail">{detail}</div>}
      {remedy !== null && <div className="rail-fault-remedy">{remedy}</div>}
      {onRetry !== undefined && (
        <button className="rail-retry" onClick={onRetry}>
          {t("common.retry")}
        </button>
      )}
    </div>
  );
}

/** Connected, and there is genuinely nothing happening. Normal chrome, no alarm. */
export function RailIdle({ children }: { children: ReactNode }) {
  return (
    <div className="rail-idle">
      <span className="rail-idle-pip" />
      {children}
    </div>
  );
}

export function RailWaiting({ children }: { children: ReactNode }) {
  return <div className="rail-waiting">{children}</div>;
}

/** A 0..1 bar. `value === null` renders an explicit dash, never a zero-length bar. */
export function Meter({
  value,
  label,
  tone,
}: {
  value: number | null;
  label: string;
  tone: "inhibition" | "gate";
}) {
  const { t } = useI18n();
  return (
    <span
      className="meter"
      title={`${label} ${value === null ? t("common.notReported") : value.toFixed(3)}`}
    >
      <span className="meter-label">{label}</span>
      {value === null ? (
        <span className="meter-null">—</span>
      ) : (
        <>
          <span className="meter-track">
            <span
              className={`meter-fill ${tone}`}
              style={{ width: `${Math.min(Math.max(value, 0), 1) * 100}%` }}
            />
          </span>
          <span className="meter-value">{value.toFixed(2)}</span>
        </>
      )}
    </span>
  );
}

export function relTime(iso: string | null, now: number, locale: Locale): string {
  if (iso === null) return translate(locale, "common.never");
  const ms = now - Date.parse(iso);
  if (!Number.isFinite(ms)) return "—";
  if (ms < 1000) return translate(locale, "common.justNow");
  if (ms < 60_000)
    return translate(locale, "common.secondsAgo", { count: Math.floor(ms / 1000) });
  if (ms < 3_600_000)
    return translate(locale, "common.minutesAgo", { count: Math.floor(ms / 60_000) });
  return translate(locale, "common.hoursAgo", { count: Math.floor(ms / 3_600_000) });
}

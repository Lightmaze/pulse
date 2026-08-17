import { useState } from "react";
import { useI18n } from "../i18n";
import {
  faultText,
  mergeKnobs,
  postTuning,
  type Fetched,
  type KnobName,
  type KnobValues,
  type TuningSnapshot,
} from "../pulse";
import { RailBand, RailFault, RailWaiting } from "./RailParts";

interface KnobSpec {
  key: KnobName;
  label: string;
  hint: string;
  min: number;
  max: number;
  step: number;
  unit: string;
}

/**
 * Slider domains are a *display* choice — the contract fixes no ranges, and the
 * numeric readout below each track always shows the true value, so an observed
 * reading outside the domain is still legible even though its marker clamps.
 */
function pct(v: number, spec: KnobSpec): number {
  return Math.min(Math.max((v - spec.min) / (spec.max - spec.min), 0), 1) * 100;
}

function fmt(v: number | null, spec: KnobSpec): string {
  if (v === null) return "—";
  return `${spec.step >= 0.5 ? v.toFixed(1) : v.toFixed(2)}${spec.unit}`;
}

function settled(a: number | null, b: number | null, spec: KnobSpec): boolean {
  if (a === null || b === null) return false;
  return Math.abs(a - b) <= spec.step / 2;
}

/**
 * 调律 — the rhythm knobs (contract §2.2).
 *
 * Two things here are architecture, not decoration:
 *
 *  1. commanded and observed are drawn side by side on the same track. A pulse
 *     knob takes effect some ticks after it is turned, and a control that snaps
 *     to its new value on drag would be reporting "done" for the whole interval
 *     in which the truth is "not yet". Dragging produces a *draft*; 下令 turns
 *     it into `commanded`; only a `tuning_applied` frame moves `observed`.
 *  2. null is per knob, not global. 交还 releases one knob back to the
 *     claustrum without touching the other three — and because a null in the
 *     POST body *means* release, every POST carries all four keys (mergeKnobs).
 */
export function RailTuning({
  tuning,
  base,
  hostUp,
  onApplied,
}: {
  tuning: Fetched<TuningSnapshot>;
  base: string | null;
  hostUp: boolean;
  onApplied: () => void;
}) {
  const { t } = useI18n();
  const specs: KnobSpec[] = [
    {
      key: "activity",
      label: t("tuning.activity"),
      hint: t("tuning.activityHint"),
      min: 0,
      max: 1,
      step: 0.01,
      unit: "",
    },
    {
      key: "wait",
      label: t("tuning.wait"),
      hint: t("tuning.waitHint"),
      min: 0,
      max: 60,
      step: 0.5,
      unit: "s",
    },
    {
      key: "propagation_threshold",
      label: t("tuning.threshold"),
      hint: t("tuning.thresholdHint"),
      min: 0,
      max: 1,
      step: 0.01,
      unit: "",
    },
    {
      key: "gate",
      label: "gate",
      hint: t("tuning.gateHint"),
      min: 0,
      max: 1,
      step: 0.01,
      unit: "",
    },
  ];
  const [drafts, setDrafts] = useState<Partial<Record<KnobName, number>>>({});
  const [sending, setSending] = useState(false);
  const [willApply, setWillApply] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  const snap = tuning.data;
  const writable = base !== null && tuning.state === "ok" && snap !== null && !sending;

  const send = (next: KnobValues, clearDrafts: KnobName[]) => {
    if (base === null) return;
    setSending(true);
    setError(null);
    postTuning(base, next)
      .then((ack) => {
        setWillApply(ack.will_apply_from_tick);
        setDrafts((d) => {
          const copy = { ...d };
          for (const k of clearDrafts) delete copy[k];
          return copy;
        });
        onApplied();
      })
      .catch((e: unknown) => setError(faultText(e)))
      .finally(() => setSending(false));
  };

  const drafted = specs.filter(
    (s) => drafts[s.key] !== undefined && !settled(drafts[s.key] ?? null, snap?.commanded[s.key] ?? null, s),
  );

  const body = (() => {
    if (tuning.state === "failed") {
      return (
        <RailFault
          kind="offline"
          title={hostUp ? t("tuning.readFailed") : t("rail.offline")}
          detail={tuning.detail}
          remedy={tuning.remedy}
          onRetry={tuning.reload}
        />
      );
    }
    if (tuning.state === "absent") {
      return (
        <RailFault
          kind="absent"
          title={t("tuning.routeAbsent")}
          detail={tuning.detail}
          remedy={tuning.remedy}
          onRetry={tuning.reload}
        />
      );
    }
    if (snap === null) return <RailWaiting>{t("tuning.loading")}</RailWaiting>;

    return (
      <>
        {specs.map((spec) => {
          const observed = snap.observed[spec.key];
          const commanded = snap.commanded[spec.key];
          const draft = drafts[spec.key];
          const autonomous = commanded === null;
          const isDraft = draft !== undefined && !settled(draft, commanded, spec);
          const pending = !autonomous && !settled(commanded, observed, spec);

          return (
            <div className={`knob${autonomous ? " autonomous" : ""}`} key={spec.key}>
              <div className="knob-head">
                <span className="knob-label" title={spec.hint}>
                  {spec.label}
                </span>
                <span className="knob-key">{spec.key}</span>
                <button
                  className="knob-mode"
                  disabled={!writable}
                  title={
                    autonomous
                      ? t("tuning.takeOverHint")
                      : t("tuning.releaseHint")
                  }
                  onClick={() =>
                    send(
                      mergeKnobs(snap.commanded, {
                        [spec.key]: autonomous
                          ? (observed ?? (spec.min + spec.max) / 2)
                          : null,
                      }),
                      [spec.key],
                    )
                  }
                >
                  {autonomous ? t("tuning.takeOver") : t("tuning.release")}
                </button>
              </div>

              {/* commanded and observed on one track — never collapsed into one number */}
              <div className="knob-track">
                {observed !== null && (
                  <span className="knob-observed-fill" style={{ width: `${pct(observed, spec)}%` }} />
                )}
                {observed !== null && (
                  <span className="knob-observed" style={{ left: `${pct(observed, spec)}%` }} />
                )}
                {commanded !== null && (
                  <span
                    className={`knob-commanded${pending ? " pending" : ""}`}
                    style={{ left: `${pct(commanded, spec)}%` }}
                  />
                )}
                {isDraft && draft !== undefined && (
                  <span className="knob-draft" style={{ left: `${pct(draft, spec)}%` }} />
                )}
              </div>

              <input
                type="range"
                className="knob-range"
                min={spec.min}
                max={spec.max}
                step={spec.step}
                disabled={!writable || autonomous}
                value={draft ?? commanded ?? observed ?? spec.min}
                onChange={(e) =>
                  setDrafts((d) => ({ ...d, [spec.key]: Number(e.target.value) }))
                }
              />

              <div className="knob-read">
                <span className="knob-observed-read">
                  {t("tuning.observed")} {fmt(observed, spec)}
                </span>
                {autonomous ? (
                  <span className="knob-auto">{t("tuning.autonomous")}</span>
                ) : (
                  <>
                    <span className="knob-commanded-read">
                      {t("tuning.commanded")} {fmt(commanded, spec)}
                    </span>
                    {pending ? (
                      <span className="knob-pending">
                        {t("tuning.pending")}
                        {willApply !== null ? ` ≥tick ${willApply}` : ""}
                      </span>
                    ) : (
                      <span className="knob-settled">
                        {t("tuning.applied")}
                        {snap.applied_at_tick !== null ? ` @tick ${snap.applied_at_tick}` : ""}
                      </span>
                    )}
                  </>
                )}
                {isDraft && (
                  <span className="knob-draft-read">
                    {t("tuning.draft", { value: fmt(draft ?? null, spec) })}
                  </span>
                )}
              </div>
            </div>
          );
        })}

        <div className="knob-actions">
          <button
            className="primary"
            disabled={!writable || drafted.length === 0}
            onClick={() => {
              const patch: Partial<KnobValues> = {};
              for (const s of drafted) patch[s.key] = drafts[s.key] ?? null;
              send(mergeKnobs(snap.commanded, patch), drafted.map((s) => s.key));
            }}
          >
            {sending
              ? t("tuning.commanding")
              : t("tuning.command", { count: drafted.length })}
          </button>
          <button disabled={drafted.length === 0 || sending} onClick={() => setDrafts({})}>
            {t("tuning.discard")}
          </button>
        </div>
        {!writable && base !== null && tuning.state === "ok" && (
          <div className="rail-note">{t("tuning.locked")}</div>
        )}
        {error !== null && <div className="rail-error">{error}</div>}
      </>
    );
  })();

  return (
    <RailBand
      title={t("tuning.title")}
      subtitle={t("tuning.subtitle")}
      note={
        tuning.state === "ok" && snap !== null ? (
          <span>
            {t("tuning.controlled", {
              count: specs.filter((s) => snap.commanded[s.key] !== null).length,
              total: specs.length,
            })}
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

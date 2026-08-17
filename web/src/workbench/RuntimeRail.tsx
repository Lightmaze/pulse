import { useEffect, useMemo, useRef, useState } from "react";
import { localizedRuntimeFault, useI18n, type Locale } from "../i18n";
import {
  KNOB_NAMES,
  faultText,
  mergeKnobs,
  parseActive,
  parseConnectivity,
  parseDelegations,
  parseHealth,
  parseScheduling,
  parseTuning,
  postTuning,
  useEndpoint,
  useFiringLog,
  useRailStream,
  useRuntimeBase,
  type ActiveEngram,
  type ConnectivitySnapshot,
  type ConnectivityStructuralRegime,
  type DelegationRow,
  type EngramFailureDomainState,
  type Fetched,
  type FiringLog,
  type KnobName,
  type KnobValues,
  type SchedulingSnapshot,
  type StreamEvent,
  type StreamState,
  type TuningSnapshot,
} from "../pulse";
import { useViewer } from "../store";
import {
  RUNTIME_SHUTDOWN_PATH,
  parseRuntimeShutdown,
  runtimeShutdownLifecycle,
  type RuntimeShutdownComponent,
  type RuntimeShutdownSnapshot,
} from "../runtimeShutdown";
import { HexMark, Icon, type IconName } from "./Icons";
import { CausalTimeline } from "./CausalTimeline";
import {
  displayIdentity,
  relativeTime,
  shortSignature,
  shutdownEvidenceStateLabel,
  successionCapacityPresentation,
  wcopy,
  type EngramSummary,
} from "./model";
import { zhText } from "../locales/zh-ui.ts";

const VIEW_KEY = "pulse.rail.projection:v2";
const MAX_TRACE_EVENTS = 500;
const SCHEDULING_EVENTS = new Set([
  "center_admission_planned",
  "center_reservation_settled",
  "center_reservation_recovered",
  "runtime_lease_acquired",
  "runtime_lease_lost",
  "runtime_lease_released",
]);

type Projection = "trace" | "field";
type Tone = "pulse" | "amber" | "blue" | "violet" | "neutral";

interface EvidenceEvent {
  id: string;
  tMs: number;
  type: string;
  stream: "pulse" | "tuning" | "delegation" | "advisory" | "system";
  label: string;
  detail: string;
  actorId: string | null;
  tone: Tone;
  explicitRelation: string | null;
}

interface KnobSpec {
  key: KnobName;
  labelKey: "activity" | "wait" | "propagation" | "gate";
  min: number;
  max: number;
  step: number;
  format: (value: number) => string;
}

const KNOBS: KnobSpec[] = [
  {
    key: "activity",
    labelKey: "activity",
    min: 0,
    max: 2,
    step: 0.01,
    format: (value) => value.toFixed(2),
  },
  {
    key: "wait",
    labelKey: "wait",
    min: 0,
    max: 60,
    step: 0.5,
    format: (value) => `${value.toFixed(value < 10 ? 1 : 0)}s`,
  },
  {
    key: "propagation_threshold",
    labelKey: "propagation",
    min: 0,
    max: 1,
    step: 0.01,
    format: (value) => value.toFixed(2),
  },
  {
    key: "gate",
    labelKey: "gate",
    min: 0,
    max: 1,
    step: 0.01,
    format: (value) => value.toFixed(2),
  },
];

function rec(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : {};
}

function str(value: unknown): string | null {
  return typeof value === "string" && value !== "" ? value : null;
}

function eventTime(payload: Record<string, unknown>): number {
  const parsed = Date.parse(str(payload.t) ?? str(payload.timestamp) ?? "");
  return Number.isFinite(parsed) ? parsed : Date.now();
}

function traceType(type: string): boolean {
  return [
    "runtime_start",
    "runtime_stop",
    "inject",
    "pulse",
    "propagate",
    "succession",
    "identity_updated",
    "tuning_commanded",
    "tuning_applied",
    "delegation_requested",
    "route",
    "delegation",
    "delegation_started",
    "delegation_done",
    "delegation_failed",
    "advisory_offered",
    "advisory_injected",
    "advisory_used",
    "advisory_ignored",
  ].includes(type);
}

function normalizeEvidence(event: StreamEvent): EvidenceEvent | null {
  const payload = event.payload;
  const type = event.type;
  if (!traceType(type)) return null;

  const tMs = eventTime(payload);
  const actorId =
    str(payload.engram_id) ??
    str(payload.engram) ??
    str(payload.caller_id) ??
    str(payload.source) ??
    null;
  const canonicalId =
    str(payload.event_id) ??
    str(payload.delegation_id) ??
    str(payload.advisory_id) ??
    (typeof payload.seq === "number" ? String(payload.seq) : null);
  const id = canonicalId ?? `${type}:${tMs}:${actorId ?? "runtime"}`;
  const cause =
    str(payload.causation_id) ??
    str(payload.source_event_id) ??
    str(payload.previous_event_id);

  if (type === "pulse") {
    const reason = str(payload.reason) ?? str(payload.kind) ?? "unknown";
    return {
      id,
      tMs,
      type,
      stream: "pulse",
      label: "Engram pulse",
      detail: `${actorId === null ? "unknown" : shortSignature(actorId)} · ${reason}`,
      actorId,
      tone: reason === "propagation" || reason === "propagated" ? "blue" : "pulse",
      explicitRelation: cause,
    };
  }
  if (type === "propagate") {
    const source = str(payload.source) ?? actorId;
    const targets = Array.isArray(payload.targets)
      ? payload.targets.filter((value): value is string => typeof value === "string")
      : [];
    return {
      id,
      tMs,
      type,
      stream: "pulse",
      label: targets.length === 1 ? "Propagation" : `Propagation · ${targets.length} targets`,
      detail: `${source === null ? "unknown" : shortSignature(source)} → ${
        targets.length === 0 ? "—" : targets.map(shortSignature).join(", ")
      }`,
      actorId: source,
      tone: "blue",
      explicitRelation: cause ?? (source === null ? null : `source ${shortSignature(source)}`),
    };
  }
  if (type === "inject") {
    const source = str(payload.source) ?? "external";
    return {
      id,
      tMs,
      type,
      stream: "pulse",
      label: source === "user" || source === "external" ? "External input" : "Input injected",
      detail: `${source} → ${actorId === null ? "unknown" : shortSignature(actorId)}`,
      actorId,
      tone: "neutral",
      explicitRelation: cause,
    };
  }
  if (type.startsWith("tuning_")) {
    return {
      id,
      tMs,
      type,
      stream: "tuning",
      label: type === "tuning_commanded" ? "Tuning commanded" : "Tuning applied",
      detail:
        type === "tuning_applied" && typeof payload.tick === "number"
          ? `tick ${payload.tick}`
          : "independent claustrum stream",
      actorId: null,
      tone: "violet",
      explicitRelation: null,
    };
  }
  if (type === "succession") {
    const oldId = str(payload.old) ?? str(payload.old_id);
    const newId = str(payload.new) ?? str(payload.new_id);
    return {
      id,
      tMs,
      type,
      stream: "system",
      label: "Engram succession",
      detail: `${oldId === null ? "—" : shortSignature(oldId)} → ${
        newId === null ? "—" : shortSignature(newId)
      }`,
      actorId: newId,
      tone: "amber",
      explicitRelation: oldId === null ? cause : `generation ${shortSignature(oldId)}`,
    };
  }
  if (type.startsWith("advisory_")) {
    const state = type.slice("advisory_".length);
    const advisoryId = str(payload.advisory_id);
    return {
      id,
      tMs,
      type,
      stream: "advisory",
      label: `Advisory ${state}`,
      detail: advisoryId ?? "",
      actorId,
      tone: state === "injected" || state === "used" ? "amber" : "neutral",
      explicitRelation: cause,
    };
  }
  if (
    type.startsWith("delegation_") ||
    type === "delegation" ||
    type === "route"
  ) {
    const target =
      str(payload.target_id) ??
      str(payload.target) ??
      str(rec(payload.route).chosen);
    const backend =
      str(payload.backend) ??
      str(rec(payload.route).backend) ??
      str(payload.executor);
    const stage = type === "route" ? "routed" : type.replace("delegation_", "");
    return {
      id,
      tMs,
      type,
      stream: "delegation",
      label: `Delegation ${stage}`,
      detail: `${target === null ? "router deciding" : shortSignature(target)}${
        backend === null ? "" : ` · ${backend}`
      }`,
      actorId: target,
      tone: "blue",
      explicitRelation: cause ?? str(payload.delegation_id),
    };
  }
  if (type === "identity_updated") {
    return {
      id,
      tMs,
      type,
      stream: "system",
      label: "Identity updated",
      detail: actorId === null ? "Engram" : shortSignature(actorId),
      actorId,
      tone: "neutral",
      explicitRelation: cause,
    };
  }
  return {
    id,
    tMs,
    type,
    stream: "system",
    label: type.replaceAll("_", " "),
    detail: actorId === null ? "runtime" : shortSignature(actorId),
    actorId,
    tone: "neutral",
    explicitRelation: cause,
  };
}

function evidenceLabel(event: EvidenceEvent, locale: Locale): string {
  if (event.type === "pulse") return wcopy(locale, "eventPulse");
  if (event.type === "propagate") return wcopy(locale, "eventPropagation");
  if (event.type === "inject") {
    return event.label === "External input"
      ? wcopy(locale, "eventExternalInput")
      : wcopy(locale, "eventInjectedInput");
  }
  if (event.type === "tuning_commanded") return wcopy(locale, "eventTuningCommanded");
  if (event.type === "tuning_applied") return wcopy(locale, "eventTuningApplied");
  if (event.type === "identity_updated") return wcopy(locale, "eventIdentityUpdated");
  if (event.type === "runtime_start") return wcopy(locale, "eventRuntimeStart");
  if (event.type === "runtime_stop") return wcopy(locale, "eventRuntimeStop");
  if (event.type === "succession") return wcopy(locale, "eventSuccession");

  if (
    event.type === "route" ||
    event.type === "delegation" ||
    event.type.startsWith("delegation_")
  ) {
    const stage =
      event.type === "route"
        ? wcopy(locale, "eventRouted")
        : event.type === "delegation"
          ? wcopy(locale, "queued")
        : event.type === "delegation_started"
          ? wcopy(locale, "eventStarted")
          : event.type === "delegation_done"
            ? wcopy(locale, "eventDone")
            : event.type === "delegation_failed"
              ? wcopy(locale, "failed")
              : event.type.replace("delegation_", "");
    return `${wcopy(locale, "delegation")} · ${stage}`;
  }

  if (event.type.startsWith("advisory_")) {
    const stage =
      event.type === "advisory_offered"
        ? wcopy(locale, "eventOffered")
        : event.type === "advisory_injected"
          ? wcopy(locale, "eventInjected")
          : event.type === "advisory_used"
            ? wcopy(locale, "eventUsed")
            : wcopy(locale, "eventIgnored");
    return `${wcopy(locale, "advisory")} · ${stage}`;
  }

  return event.label;
}

function evidenceDetail(event: EvidenceEvent, locale: Locale): string {
  if (event.type.startsWith("advisory_") && event.detail === "") {
    return wcopy(locale, "companionIntervention");
  }
  const detail = event.detail;
  if (locale === "en") return detail;
  return detail
    .replaceAll("independent claustrum stream", zhText("workbench.RuntimeRail.line385"))
    .replaceAll("router deciding", zhText("workbench.RuntimeRail.line386"))
    .replaceAll("unknown", zhText("workbench.RuntimeRail.line387"))
    .replaceAll("runtime", zhText("workbench.RuntimeRail.line388"))
    .replaceAll("external", zhText("workbench.RuntimeRail.line389"))
    .replaceAll("user", zhText("workbench.RuntimeRail.line390"));
}

function liveStatus(
  health: Fetched<{ ok: boolean; version: string | null }>,
  stream: StreamState,
): { tone: string; key: "live" | "polling" | "offline" | "connecting" } {
  if (health.state === "loading" || health.state === "idle" || stream === "connecting") {
    return { tone: "connecting", key: "connecting" };
  }
  if (health.state !== "ok") return { tone: "offline", key: "offline" };
  if (stream === "open") return { tone: "live", key: "live" };
  return { tone: "polling", key: "polling" };
}

function Presence({
  active,
  selectedId,
  now,
  onSelect,
}: {
  active: Fetched<ActiveEngram[]>;
  selectedId: string | null;
  now: number;
  onSelect: (id: string) => void;
}) {
  const { locale } = useI18n();
  if (active.state === "loading" || active.state === "idle") {
    return (
      <div className="pw-presence-loading">
        <span /><span /><span />
      </div>
    );
  }
  if (active.state !== "ok" || active.data === null) {
    return (
      <div className="pw-rail-fault">
        <Icon name="radio" size={17} />
        <span>{wcopy(locale, "runtimeUnavailable")}</span>
        <button onClick={active.reload}>{wcopy(locale, "retry")}</button>
      </div>
    );
  }
  if (active.data.length === 0) {
    return <div className="pw-rail-empty">{wcopy(locale, "noSessions")}</div>;
  }

  const rows = [...active.data].sort((a, b) => {
    if (a.firing !== b.firing) return a.firing ? -1 : 1;
    return (b.last_fired_at ?? "").localeCompare(a.last_fired_at ?? "");
  });
  return (
    <div className="pw-presence-list">
      {rows.map((row) => {
        const identity = displayIdentity({
          id: row.engram_id,
          name: row.name,
          nickname: row.nickname,
        });
        const tone: Tone = row.firing ? "pulse" : "neutral";
        return (
          <button
            key={row.engram_id}
            className={`pw-presence-row${selectedId === row.engram_id ? " is-selected" : ""}`}
            onClick={() => onSelect(row.engram_id)}
          >
            <HexMark tone={tone} size={20} />
            <span className="pw-presence-copy">
              <strong>{identity.primary}</strong>
              <small>{identity.secondary}</small>
            </span>
            <span className={`pw-presence-state${row.firing ? " is-hot" : ""}`}>
              {row.firing ? (
                <>
                  <Icon name="pulse" size={14} />
                  {wcopy(locale, "justPulsed")}
                </>
              ) : (
                <>
                  <Icon name="clock" size={14} />
                  {row.last_fired_at === null
                    ? wcopy(locale, "neverPulsed")
                    : relativeTime(row.last_fired_at, locale, now)}
                </>
              )}
            </span>
          </button>
        );
      })}
    </div>
  );
}

function EvidenceTrace({
  evidence,
  firingLog,
  labels,
  selectedActor,
  windowSec,
  onSelectActor,
}: {
  evidence: EvidenceEvent[];
  firingLog: FiringLog;
  labels: Map<string, string>;
  selectedActor: string | null;
  windowSec: number;
  onSelectActor: (id: string) => void;
}) {
  const { locale } = useI18n();
  const selectedTime = useViewer((state) =>
    state.run === null ? null : state.cursorMs,
  );
  const anchor = selectedTime ?? Date.now();
  const cutoff = anchor - windowSec * 1000;
  const clockFormatter = useMemo(
    () =>
      new Intl.DateTimeFormat(locale, {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }),
    [locale],
  );

  const rows = useMemo(() => {
    const byKey = new Map<string, EvidenceEvent>();
    const pulseKeys = new Set<string>();
    for (const item of evidence) {
      if (item.tMs > anchor || item.tMs < cutoff) continue;
      byKey.set(item.id, item);
      if (item.type === "pulse" && item.actorId !== null) {
        pulseKeys.add(`${item.actorId}:${Math.round(item.tMs)}`);
      }
    }
    for (const mark of firingLog.marks) {
      if (mark.tMs > anchor || mark.tMs < cutoff) continue;
      const key = `pulse:${mark.engramId}:${mark.tMs}:${mark.kind}`;
      const rounded = Math.round(mark.tMs);
      if (
        pulseKeys.has(`${mark.engramId}:${rounded - 1}`) ||
        pulseKeys.has(`${mark.engramId}:${rounded}`) ||
        pulseKeys.has(`${mark.engramId}:${rounded + 1}`)
      ) {
        continue;
      }
      byKey.set(key, {
        id: key,
        tMs: mark.tMs,
        type: "pulse",
        stream: "pulse",
        label: "Engram pulse",
        detail: `${labels.get(mark.engramId) ?? shortSignature(mark.engramId)} · ${mark.kind}`,
        actorId: mark.engramId,
        tone: mark.kind === "propagated" || mark.kind === "propagation" ? "blue" : "pulse",
        explicitRelation: null,
      });
      pulseKeys.add(`${mark.engramId}:${rounded}`);
    }
    return [...byKey.values()]
      .sort((a, b) => a.tMs - b.tMs)
      .slice(-12);
  }, [anchor, cutoff, evidence, firingLog.marks, labels]);

  if (rows.length === 0) {
    return (
      <div className="pw-trace-empty">
        <div className="pw-trace-empty-axis"><span /><span /><span /></div>
        <strong>{wcopy(locale, "noTrace")}</strong>
        <span>{wcopy(locale, "traceHint")}</span>
      </div>
    );
  }

  return (
    <div className="pw-trace">
      <div className="pw-trace-minimap" aria-hidden="true">
        {rows.map((row) => <span key={row.id} className={`tone-${row.tone}`} />)}
      </div>
      <div className="pw-trace-events">
        {rows.map((row, index) => {
          const selected = row.actorId !== null && row.actorId === selectedActor;
          const clock = clockFormatter.format(row.tMs);
          return (
            <button
              className={`pw-trace-event tone-${row.tone}${selected ? " is-selected" : ""}`}
              aria-disabled={row.actorId === null}
              key={row.id}
              tabIndex={row.actorId === null ? -1 : 0}
              onClick={() => {
                if (row.actorId !== null) onSelectActor(row.actorId);
              }}
            >
              <time>{clock}</time>
              <span className="pw-trace-axis">
                <span className="pw-trace-node">
                  {row.type === "pulse" ? (
                    <Icon name="pulse" size={14} />
                  ) : row.stream === "delegation" ? (
                    <Icon name="route" size={14} />
                  ) : row.stream === "tuning" ? (
                    <Icon name="spark" size={14} />
                  ) : row.type === "propagate" ? (
                    <Icon name="network" size={14} />
                  ) : (
                    <Icon name="external" size={14} />
                  )}
                </span>
                {index < rows.length - 1 && <span className="pw-trace-spine" />}
                {row.explicitRelation !== null && <span className="pw-trace-relation" />}
              </span>
              <span className="pw-trace-copy">
                <strong>{evidenceLabel(row, locale)}</strong>
                <small>{evidenceDetail(row, locale)}</small>
              </span>
              <span className="pw-trace-kind">
                {row.explicitRelation === null
                  ? wcopy(locale, "chronologyOnly")
                  : wcopy(locale, "explicitRelation")}
              </span>
            </button>
          );
        })}
      </div>
      <button
        className="pw-trace-inspect"
        onClick={() => useViewer.getState().setPage("trace")}
      >
        <Icon name="external" size={15} />
        <span>{wcopy(locale, "inspectTrajectory")}</span>
        <Icon name="chevronRight" size={14} />
      </button>
    </div>
  );
}

function ActivationHistory({
  log,
  roster,
  labels,
  windowSec,
  selectedId,
  onSelect,
  onWindowChange,
}: {
  log: FiringLog;
  roster: string[];
  labels: Map<string, string>;
  windowSec: number;
  selectedId: string | null;
  onSelect: (id: string) => void;
  onWindowChange: (seconds: number) => void;
}) {
  const { locale } = useI18n();
  const bucketCount = 11;
  const now = Date.now();
  const cutoff = now - windowSec * 1000;
  const ids = roster.length > 0
    ? roster.slice(0, 5)
    : [...new Set(log.marks.map((mark) => mark.engramId))].slice(0, 5);
  const offset = (ratio: number) => {
    const seconds = Math.round(windowSec * ratio);
    return seconds < 60 ? `-${seconds}s` : `-${Math.round(seconds / 60)}m`;
  };

  return (
    <section className="pw-field-section pw-history-section">
      <div className="pw-field-section-head">
        <div>
          <span>{wcopy(locale, "activationHistory")}</span>
          <small>{wcopy(locale, "fieldHint")}</small>
        </div>
        <label className="pw-window-select">
          <select
            value={windowSec}
            aria-label={wcopy(locale, "historyWindow")}
            onChange={(event) => onWindowChange(Number(event.target.value))}
          >
            <option value={60}>1 min</option>
            <option value={300}>5 min</option>
            <option value={900}>15 min</option>
          </select>
          <Icon name="chevronDown" size={12} />
        </label>
      </div>
      {log.state !== "ok" ? (
        <div className="pw-field-loading">
          <span /><span /><span />
        </div>
      ) : ids.length === 0 ? (
        <div className="pw-rail-empty">{wcopy(locale, "noTrace")}</div>
      ) : (
        <div className="pw-history-grid">
          <div className="pw-history-axis">
            <span />
            <span>{offset(1)}</span>
            <span>{offset(0.6)}</span>
            <span>{offset(0.2)}</span>
            <span>{wcopy(locale, "now")}</span>
          </div>
          {ids.map((id) => {
            const buckets = Array.from({ length: bucketCount }, () => ({
              count: 0,
              kinds: new Set<string>(),
            }));
            for (const mark of log.marks) {
              if (mark.engramId !== id || mark.tMs < cutoff || mark.tMs > now) continue;
              const ratio = (mark.tMs - cutoff) / (windowSec * 1000);
              const bucket = Math.min(bucketCount - 1, Math.max(0, Math.floor(ratio * bucketCount)));
              buckets[bucket].count += 1;
              buckets[bucket].kinds.add(mark.kind);
            }
            return (
              <button
                className={`pw-history-row${selectedId === id ? " is-selected" : ""}`}
                key={id}
                onClick={() => onSelect(id)}
              >
                <span className="pw-history-label">
                  <HexMark
                    tone={selectedId === id ? "pulse" : "neutral"}
                    size={14}
                  />
                  <span>{labels.get(id) ?? shortSignature(id)}</span>
                </span>
                <span className="pw-history-dots">
                  {buckets.map((bucket, index) => {
                    const propagated =
                      bucket.kinds.has("propagated") || bucket.kinds.has("propagation");
                    return (
                      <span
                        key={index}
                        className={`${bucket.count > 0 ? "is-on" : ""}${propagated ? " is-propagated" : ""}`}
                        style={{ "--density": String(Math.min(bucket.count, 3)) } as React.CSSProperties}
                      />
                    );
                  })}
                  <i />
                </span>
              </button>
            );
          })}
        </div>
      )}
    </section>
  );
}

function admissionDecisionLabel(value: string, locale: Locale): string {
  if (value === "idle") return wcopy(locale, "admissionIdle");
  if (value === "admitted") return wcopy(locale, "admissionAdmitted");
  if (value === "waiting") return wcopy(locale, "admissionWaiting");
  if (value === "blocked") return wcopy(locale, "admissionBlocked");
  return value;
}

function admissionReasonLabel(value: string, locale: Locale): string {
  if (value === "lane_reservation") return wcopy(locale, "reasonLaneReservation");
  if (value === "fair_share") return wcopy(locale, "reasonFairShare");
  if (value === "effective_score") return wcopy(locale, "reasonEffectiveScore");
  if (value === "budget_deferred") return wcopy(locale, "reasonBudgetDeferred");
  if (value === "center_inactive") return wcopy(locale, "reasonCenterInactive");
  if (value === "no_ready_event") return wcopy(locale, "reasonNoReadyEvent");
  return value;
}

function reservationLabel(value: string, locale: Locale): string {
  if (value === "held") return wcopy(locale, "reservationHeld");
  if (value === "settled") return wcopy(locale, "reservationSettled");
  if (value === "abandoned") return wcopy(locale, "reservationAbandoned");
  if (value === "succeeded") return wcopy(locale, "outcomeSucceeded");
  if (value === "failed") return wcopy(locale, "outcomeFailed");
  if (value === "skipped") return wcopy(locale, "outcomeSkipped");
  if (value === "uncertain") return wcopy(locale, "outcomeUncertain");
  if (value === "owner_replaced") return wcopy(locale, "outcomeOwnerReplaced");
  return value;
}

function laneLabel(value: string, locale: Locale): string {
  if (value === "work") return wcopy(locale, "workLane");
  if (value === "life") return wcopy(locale, "lifeLane");
  return value;
}

function failureDomainStateLabel(value: EngramFailureDomainState, locale: Locale): string {
  if (value === "cooling") return wcopy(locale, "failureDomainCooling");
  if (value === "degraded") return wcopy(locale, "failureDomainDegraded");
  return wcopy(locale, "failureDomainProbeReady");
}

function failureDomainHint(value: EngramFailureDomainState, locale: Locale): string {
  if (value === "cooling") return wcopy(locale, "failureDomainCoolingHint");
  if (value === "degraded") return wcopy(locale, "failureDomainDegradedHint");
  return wcopy(locale, "failureDomainProbeReadyHint");
}

function timestampLabel(value: string, locale: Locale): string {
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) return value;
  return new Intl.DateTimeFormat(locale, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    timeZoneName: "short",
  }).format(parsed);
}

function schedulingIcon(value: string): IconName {
  if (["active", "admitted", "settled", "succeeded", "probe_ready"].includes(value)) return "check";
  if (["waiting", "held", "cooling"].includes(value)) return "clock";
  if (["blocked", "abandoned", "failed", "owner_replaced"].includes(value)) return "x";
  if (["uncertain", "skipped", "degraded"].includes(value)) return "info";
  return "radio";
}

function AdmissionState({
  value,
  label,
  title,
}: {
  value: string;
  label: string;
  title: string;
}) {
  return (
    <span
      className={`pw-admission-state state-${value.toLowerCase().replace(/[^a-z0-9-]+/g, "-")}`}
      aria-label={label}
      title={title}
    >
      <Icon name={schedulingIcon(value)} size={12} />
      <span>{label}</span>
    </span>
  );
}

function connectivityPercent(value: number, locale: Locale): string {
  return value.toLocaleString(locale, { style: "percent", maximumFractionDigits: 1 });
}

function connectivityAge(age: number | null, locale: Locale): string {
  if (age === null) return wcopy(locale, "connectivityAgeUnknown");
  const value = Math.max(0, Math.floor(age));
  if (value < 2) return locale === "zh-CN" ? zhText("workbench.RuntimeRail.line834") : "observed now";
  if (value < 60) return locale === "zh-CN" ? ("" + String(value) + zhText("workbench.RuntimeRail.line835.tail1")) : `${value}s ago`;
  const minutes = Math.floor(value / 60);
  return locale === "zh-CN" ? ("" + String(minutes) + zhText("workbench.RuntimeRail.line837.tail1")) : `${minutes}m ago`;
}

function connectivityRegimeLabel(regime: ConnectivityStructuralRegime, locale: Locale): string {
  if (regime === "empty") return wcopy(locale, "connectivityRegimeEmpty");
  if (regime === "singleton") return wcopy(locale, "connectivityRegimeSingleton");
  if (regime === "fragmented_acyclic") return wcopy(locale, "connectivityRegimeFragmentedAcyclic");
  if (regime === "fragmented_reverberant") {
    return wcopy(locale, "connectivityRegimeFragmentedReverberant");
  }
  if (regime === "strongly_connected") return wcopy(locale, "connectivityRegimeStronglyConnected");
  return wcopy(
    locale,
    regime === "connected_acyclic"
      ? "connectivityRegimeConnectedAcyclic"
      : "connectivityRegimeConnectedReverberant",
  );
}

type ShutdownEvidenceTone = "good" | "notice" | "danger" | "neutral";

function shutdownEvidenceTone(state: string): ShutdownEvidenceTone {
  if (
    [
      "active",
      "revoked",
      "settled",
      "joined",
      "not_applicable",
      "empty_verified",
      "not_needed",
      "completed",
      "released",
      "lost",
    ].includes(state)
  ) {
    return "good";
  }
  if (["failed", "escaped"].includes(state)) return "danger";
  if (
    [
      "closing",
      "freezing",
      "settling",
      "fencing",
      "fenced",
      "cleaning",
      "uncertain",
      "unknown",
      "root_exit_only",
      "signalled",
      "timed_out",
      "release_pending",
      "retained_for_escaped_workers",
      "close_pending",
    ].includes(state)
  ) {
    return "notice";
  }
  return "neutral";
}

function ShutdownState({
  state,
  label,
}: {
  state: string;
  label?: string;
}) {
  const { locale } = useI18n();
  return (
    <span
      className={"pw-shutdown-state tone-" + shutdownEvidenceTone(state)}
      data-state={state}
    >
      {label ?? shutdownEvidenceStateLabel(locale, state)}
    </span>
  );
}

function ShutdownIndicator({
  shutdown,
}: {
  shutdown: Fetched<RuntimeShutdownSnapshot>;
}) {
  const { locale } = useI18n();
  const snapshot = shutdown.data;
  if (shutdown.state !== "ok" || snapshot === null) {
    return (
      <span
        className="pw-shutdown-indicator is-loading"
        aria-label={wcopy(locale, "shutdownLoading")}
        title={wcopy(locale, "shutdownLoading")}
      >
        <Icon name="clock" size={10} />
      </span>
    );
  }
  const lifecycle = runtimeShutdownLifecycle(snapshot);
  return (
    <span
      className={"pw-shutdown-indicator lifecycle-" + lifecycle}
      aria-label={
        wcopy(locale, "shutdownLifecycle") +
        ": " +
        shutdownEvidenceStateLabel(locale, lifecycle)
      }
      title={
        wcopy(locale, "publicationFence") +
        ": " +
        shutdownEvidenceStateLabel(locale, snapshot.publication_fence)
      }
    >
      <i />
      {shutdownEvidenceStateLabel(locale, lifecycle)}
    </span>
  );
}

function ShutdownVerdict({
  label,
  value,
  lifecycle,
}: {
  label: string;
  value: boolean;
  lifecycle: "open" | "closing" | "closed";
}) {
  const { locale } = useI18n();
  const tone =
    lifecycle === "open"
      ? "neutral"
      : value
        ? "good"
        : lifecycle === "closing"
          ? "notice"
          : "danger";
  return (
    <div className={"pw-shutdown-verdict tone-" + tone}>
      <span>{label}</span>
      <strong>{shutdownEvidenceStateLabel(locale, String(value))}</strong>
    </div>
  );
}

function ShutdownAxis({
  label,
  state,
}: {
  label: string;
  state: string;
}) {
  const { locale } = useI18n();
  return (
    <div className={"pw-shutdown-axis tone-" + shutdownEvidenceTone(state)}>
      <dt>{label}</dt>
      <dd>{shutdownEvidenceStateLabel(locale, state)}</dd>
    </div>
  );
}

function ShutdownComponentEvidence({
  component,
}: {
  component: RuntimeShutdownComponent;
}) {
  const { locale } = useI18n();
  return (
    <article
      className={
        "pw-shutdown-component" + (component.clean ? " is-clean" : " is-unresolved")
      }
    >
      <header>
        <strong>{component.component.replaceAll("_", " ")}</strong>
        <span>
          {component.active_before} {wcopy(locale, "activeAtFreeze")}
          {" · "}
          {component.unresolved} {wcopy(locale, "unresolved")}
        </span>
      </header>
      <dl className="pw-shutdown-axes">
        <ShutdownAxis label={wcopy(locale, "shutdownEffectAxis")} state={component.effect} />
        <ShutdownAxis label={wcopy(locale, "shutdownOwnerAxis")} state={component.owner} />
        <ShutdownAxis
          label={wcopy(locale, "shutdownProcessTreeAxis")}
          state={component.process_tree}
        />
        <ShutdownAxis label={wcopy(locale, "shutdownCancelAxis")} state={component.cancel} />
      </dl>
    </article>
  );
}

function ShutdownPanel({
  shutdown,
}: {
  shutdown: Fetched<RuntimeShutdownSnapshot>;
}) {
  const { locale } = useI18n();
  const snapshot = shutdown.data;

  if (shutdown.state !== "ok" || snapshot === null) {
    const loading = shutdown.state === "idle" || shutdown.state === "loading";
    return (
      <section className="pw-field-section pw-shutdown-panel" aria-labelledby="pw-shutdown-title">
        <div className="pw-field-section-head">
          <div>
            <span id="pw-shutdown-title">{wcopy(locale, "shutdownEvidence")}</span>
            <small>{wcopy(locale, "shutdownEvidenceHint")}</small>
          </div>
        </div>
        <div className="pw-compact-fault pw-shutdown-fault" role="status">
          <Icon name={loading ? "clock" : "x"} size={14} />
          <span>
            {wcopy(locale, loading ? "shutdownLoading" : "shutdownUnavailable")}
          </span>
          {!loading && <button onClick={shutdown.reload}>{wcopy(locale, "retry")}</button>}
        </div>
      </section>
    );
  }

  const lifecycle = runtimeShutdownLifecycle(snapshot);
  return (
    <section className="pw-field-section pw-shutdown-panel" aria-labelledby="pw-shutdown-title">
      <div className="pw-field-section-head">
        <div>
          <span id="pw-shutdown-title">{wcopy(locale, "shutdownEvidence")}</span>
          <small>{wcopy(locale, "shutdownEvidenceHint")}</small>
        </div>
        <ShutdownState state={lifecycle} />
      </div>

      <div className="pw-shutdown-body" aria-live="polite">
        <div className="pw-shutdown-boundary">
          <div>
            <span>{wcopy(locale, "shutdownLifecycle")}</span>
            <strong>{shutdownEvidenceStateLabel(locale, lifecycle)}</strong>
            <small>{shutdownEvidenceStateLabel(locale, snapshot.phase)}</small>
          </div>
          <div>
            <span>{wcopy(locale, "publicationFence")}</span>
            <ShutdownState state={snapshot.publication_fence} />
          </div>
        </div>

        <div>
          <h4>{wcopy(locale, "shutdownVerdicts")}</h4>
          <div className="pw-shutdown-verdicts">
            <ShutdownVerdict
              label={wcopy(locale, "shutdownContract")}
              value={snapshot.contract_satisfied}
              lifecycle={lifecycle}
            />
            <ShutdownVerdict
              label={wcopy(locale, "shutdownClean")}
              value={snapshot.clean}
              lifecycle={lifecycle}
            />
            <ShutdownVerdict
              label={wcopy(locale, "shutdownPhysicalExit")}
              value={snapshot.physical_exit_proven}
              lifecycle={lifecycle}
            />
            <div
              className={
                "pw-shutdown-verdict" +
                (snapshot.escaped_count > 0 ? " tone-danger" : " tone-neutral")
              }
            >
              <span>{wcopy(locale, "shutdownEscaped")}</span>
              <strong>{snapshot.escaped_count}</strong>
            </div>
          </div>
        </div>

        <div>
          <h4>{wcopy(locale, "shutdownBoundaryFacts")}</h4>
          <dl className="pw-shutdown-facts">
            <div>
              <dt>{wcopy(locale, "durableRecovery")}</dt>
              <dd>{shutdownEvidenceStateLabel(locale, snapshot.durable_recovery)}</dd>
            </div>
            <div>
              <dt>{wcopy(locale, "ownerLease")}</dt>
              <dd>{shutdownEvidenceStateLabel(locale, snapshot.owner_lease)}</dd>
            </div>
            <div>
              <dt>{wcopy(locale, "storageState")}</dt>
              <dd>{shutdownEvidenceStateLabel(locale, snapshot.storage_state)}</dd>
            </div>
            <div>
              <dt>{wcopy(locale, "deadlineState")}</dt>
              <dd>
                {wcopy(
                  locale,
                  snapshot.deadline_exhausted ? "deadlineExhausted" : "deadlineWithinBudget",
                )}
              </dd>
            </div>
          </dl>
        </div>

        <aside className="pw-shutdown-caveat">
          <Icon name="info" size={14} />
          <span>{wcopy(locale, "shutdownEvidenceCaveat")}</span>
        </aside>

        <div className="pw-shutdown-components">
          <header>
            <div>
              <h4>{wcopy(locale, "componentEvidence")}</h4>
              <small>{wcopy(locale, "componentEvidenceHint")}</small>
            </div>
            <strong>{snapshot.components.length}</strong>
          </header>
          {snapshot.components.length === 0 ? (
            <p>{wcopy(locale, "noComponentEvidence")}</p>
          ) : (
            <div className="pw-shutdown-component-list">
              {snapshot.components.map((component) => (
                <ShutdownComponentEvidence component={component} key={component.component} />
              ))}
            </div>
          )}
        </div>
      </div>
    </section>
  );
}

function ConnectivityPanel({ connectivity }: { connectivity: Fetched<ConnectivitySnapshot> }) {
  const { locale } = useI18n();
  const [expanded, setExpanded] = useState(false);
  const snapshot = connectivity.data;

  if (connectivity.state !== "ok" || snapshot === null) {
    const loading = connectivity.state === "loading" || connectivity.state === "idle";
    const absent = connectivity.state === "absent";
    return (
      <section className="pw-field-section pw-connectivity" aria-labelledby="pw-connectivity-title">
        <div className="pw-field-section-head">
          <div>
            <span id="pw-connectivity-title">{wcopy(locale, "connectivity")}</span>
            <small>{wcopy(locale, "connectivityHint")}</small>
          </div>
        </div>
        <div className={`pw-compact-fault pw-connectivity-state${loading ? " is-loading" : " is-fault"}`} role="status">
          <Icon name={loading ? "clock" : absent ? "info" : "x"} size={14} />
          <span title={connectivity.detail ?? undefined}>
            {loading
              ? wcopy(locale, "connectivityLoading")
              : wcopy(locale, absent ? "connectivityAbsent" : "connectivityFault")}
          </span>
          {!loading && <button onClick={connectivity.reload}>{wcopy(locale, "retry")}</button>}
        </div>
      </section>
    );
  }

  const runtimeEvidence =
    snapshot.evidence_class === "runtime_effective_threshold_projection";
  const evidenceLabel = wcopy(
    locale,
    runtimeEvidence ? "connectivityRuntimeEvidence" : "connectivityFallbackEvidence",
  );
  const reaches = [
    [wcopy(locale, "connectivityWeakCoverage"), snapshot.largest_weak_fraction,
      snapshot.largest_weak_component_size],
    [wcopy(locale, "connectivityStrongCore"), snapshot.largest_strong_fraction,
      snapshot.largest_strong_component_size],
    [wcopy(locale, "connectivityOutReach"), snapshot.largest_out_reach_fraction,
      snapshot.largest_out_reach_size],
  ] as const;
  const counts = [
    [wcopy(locale, "connectivityIsolates"), snapshot.isolated_node_count, true],
    [wcopy(locale, "connectivityCycleCapable"), snapshot.cycle_capable_node_count, false],
    [wcopy(locale, "connectivityWeakCuts"), snapshot.weak_cut_vertex_count, true],
  ] as const;

  return (
    <section className="pw-field-section pw-connectivity" aria-labelledby="pw-connectivity-title">
      <button
        className="pw-field-section-head pw-disclosure-head"
        aria-controls="pw-connectivity-detail"
        aria-expanded={expanded}
        onClick={() => setExpanded((value) => !value)}
      >
        <div>
          <span id="pw-connectivity-title">{wcopy(locale, "connectivity")}</span>
          <small>{wcopy(locale, "connectivityHint")}</small>
        </div>
        <span className={`pw-connectivity-badge${runtimeEvidence ? "" : " is-fallback"}`}>
          {evidenceLabel}
        </span>
        <Icon name={expanded ? "chevronDown" : "chevronRight"} size={14} />
      </button>

      <div className="pw-connectivity-summary">
        <div className={`pw-connectivity-regime${snapshot.weak_component_count > 1 ? " is-notice" : ""}`}>
          <Icon name="network" size={14} />
          <span>{connectivityRegimeLabel(snapshot.structural_regime, locale)}</span>
          <small>{snapshot.node_count} {wcopy(locale, "connectivityNodes")}</small>
        </div>
        {(snapshot.structural_regime === "empty" ||
          snapshot.structural_regime === "singleton") && (
          <p>
            {wcopy(
              locale,
              snapshot.structural_regime === "empty"
                ? "connectivityEmptyHint"
                : "connectivitySingletonHint",
            )}
          </p>
        )}
        <div className="pw-connectivity-reaches">
          {reaches.map(([label, fraction, size]) => (
            <div key={label}>
              <span>{label}</span>
              <strong>{connectivityPercent(fraction, locale)}</strong>
              <small>{size}/{snapshot.node_count}</small>
            </div>
          ))}
        </div>
        <div className="pw-connectivity-counts">
          {counts.map(([label, count, caution]) => (
            <div className={caution && count > 0 ? "is-notice" : ""} key={label}>
              <span>{label}</span>
              <strong>{count}</strong>
            </div>
          ))}
        </div>
      </div>

      {expanded && (
        <div className="pw-connectivity-detail" id="pw-connectivity-detail">
          <div className={`pw-connectivity-evidence${runtimeEvidence ? "" : " is-fallback"}`}>
            <strong>{evidenceLabel}</strong>
            <span title={snapshot.observed_at ?? undefined}>
              {connectivityAge(snapshot.age_seconds, locale)}
              {" · "}
              {wcopy(
                locale,
                snapshot.replay_complete
                  ? "connectivityReplayComplete"
                  : "connectivityReplayIncomplete",
              )}
            </span>
            <p>
              {wcopy(
                locale,
                runtimeEvidence
                  ? "connectivityRuntimeEvidenceHint"
                  : "connectivityFallbackEvidenceHint",
              )}
            </p>
          </div>
          <p className="pw-connectivity-caveat">
            <Icon name="route" size={14} />
            <span>{wcopy(locale, "connectivityThresholdCaveat")}</span>
          </p>
          <dl>
            <div>
              <dt>{wcopy(locale, "connectivityEffectiveExcitatory")}</dt>
              <dd>{snapshot.effective_excitatory_edge_count}</dd>
            </div>
            <div>
              <dt>{wcopy(locale, "connectivitySourceThresholds")}</dt>
              <dd>
                {snapshot.source_threshold_min === null
                  ? "—"
                  : `${snapshot.source_threshold_min.toFixed(3)}–${(
                      snapshot.source_threshold_max ?? snapshot.source_threshold_min
                    ).toFixed(3)}`}
              </dd>
            </div>
            <div>
              <dt>{wcopy(locale, "connectivityGateAcceptance")}</dt>
              <dd>
                {snapshot.minimum_gate_acceptance === null
                  ? wcopy(locale, "connectivityNotObserved")
                  : `${connectivityPercent(snapshot.minimum_gate_acceptance, locale)}–${connectivityPercent(
                      snapshot.mean_gate_acceptance ?? snapshot.minimum_gate_acceptance,
                      locale,
                    )}`}
              </dd>
            </div>
          </dl>
          <aside>
            <Icon name="info" size={14} />
            <div>
              <strong>{wcopy(locale, "connectivityReferenceTitle")}</strong>
              <p>{wcopy(locale, "connectivityReferenceBody")}</p>
            </div>
          </aside>
        </div>
      )}
    </section>
  );
}

function AdmissionPanel({
  scheduling,
  selectedCenterId,
  labels,
  now,
}: {
  scheduling: Fetched<SchedulingSnapshot>;
  selectedCenterId: string | null;
  labels: Map<string, string>;
  now: number;
}) {
  const { locale } = useI18n();
  const snapshot = scheduling.data;

  if (scheduling.state !== "ok" || snapshot === null) {
    const unavailable = scheduling.state === "absent" || scheduling.state === "failed";
    return (
      <section className="pw-field-section pw-admission-section" aria-labelledby="pw-admission-title">
        <div className="pw-field-section-head">
          <div>
            <span id="pw-admission-title">{wcopy(locale, "admission")}</span>
            <small>{wcopy(locale, "admissionHint")}</small>
          </div>
        </div>
        <div className="pw-compact-fault pw-admission-fault" role="status" aria-live="polite">
          <Icon name={unavailable ? "x" : "clock"} size={14} />
          <span title={scheduling.detail ?? undefined}>
            {unavailable
              ? scheduling.detail === null
                ? wcopy(locale, "unavailable")
                : localizedRuntimeFault(locale, scheduling.detail)
              : wcopy(locale, "connecting")}
          </span>
          <button onClick={scheduling.reload}>{wcopy(locale, "retry")}</button>
        </div>
      </section>
    );
  }

  const leaseHealthy = snapshot.lease.healthy && snapshot.lease.state === "active";
  const successionCapacity = successionCapacityPresentation(locale, snapshot.capacity);
  const selected = selectedCenterId === null
    ? null
    : snapshot.centers.find((center) => center.center_id === selectedCenterId) ?? null;
  const selectedDecision = selected?.decision ?? "idle";
  const selectedReason = selected?.reason ?? "no_ready_event";
  const recent =
    (selectedCenterId === null
      ? null
      : snapshot.reservations.find((reservation) => reservation.center_id === selectedCenterId)) ??
    snapshot.reservations[0] ??
    null;
  return (
    <section className="pw-field-section pw-admission-section" aria-labelledby="pw-admission-title">
      <div className="pw-field-section-head">
        <div>
          <span id="pw-admission-title">{wcopy(locale, "admission")}</span>
          <small>{wcopy(locale, "admissionHint")}</small>
        </div>
        <AdmissionState
          value={leaseHealthy ? "active" : "blocked"}
          label={wcopy(locale, leaseHealthy ? "leaseHealthy" : "leaseUnhealthy")}
          title={`${snapshot.lease.state} · ${snapshot.lease.lost_reason ?? snapshot.lease.owner_id}`}
        />
      </div>

      <div className="pw-admission-body" aria-live="polite">
        <div className="pw-admission-overview">
          <div className="pw-admission-fact">
            <span>{wcopy(locale, "ownerLease")}</span>
            <strong
              aria-label={`${wcopy(locale, "ownerLease")}: ${snapshot.lease.owner_id}`}
              title={snapshot.lease.owner_id}
            >
              {shortSignature(snapshot.lease.owner_id)}
            </strong>
            <small title={`${wcopy(locale, "renewed")}: ${snapshot.lease.renewed_at}; ${wcopy(locale, "expires")}: ${snapshot.lease.expires_at}`}>
              {wcopy(locale, "epoch")} {snapshot.lease.epoch} · {wcopy(locale, "renewed")} {relativeTime(snapshot.lease.renewed_at, locale, now)}
            </small>
          </div>
          <div className="pw-admission-fact">
            <span>{wcopy(locale, "tickAdmission")}</span>
            <strong>{snapshot.capacity.held} / {snapshot.capacity.budget_per_tick}</strong>
            <small>{wcopy(locale, "heldSlots")} / {wcopy(locale, "perTick")}</small>
          </div>
          <div className="pw-admission-fact">
            <span>{wcopy(locale, "pulseWorkers")}</span>
            <strong>{snapshot.capacity.worker_running} / {snapshot.capacity.worker_limit}</strong>
            <small>
              {wcopy(locale, snapshot.capacity.background_dispatch ? "boundedAsync" : "inlineHarness")}
              {" · "}{snapshot.capacity.worker_available} {wcopy(locale, "workerAvailable")}
            </small>
          </div>
          <div className="pw-admission-fact" aria-label={successionCapacity.ariaLabel}>
            <span>{wcopy(locale, "successionExecutionDomain")}</span>
            <strong>{successionCapacity.primary}</strong>
            <small>{successionCapacity.secondary}</small>
          </div>
          <div className="pw-admission-fact">
            <span>{wcopy(locale, "piResidency")}</span>
            <strong>{snapshot.capacity.resident_sessions} / {snapshot.capacity.resident_limit}</strong>
            <small>
              {snapshot.capacity.busy_sessions} {wcopy(locale, "busyProcesses")}
              {" · "}{snapshot.capacity.starting_sessions} {wcopy(locale, "startingProcesses")}
              {" · "}{snapshot.capacity.resident_sessions} {wcopy(locale, "residentProcesses")}
            </small>
          </div>
        </div>

        <div
          className={`pw-failure-domains${snapshot.failure_domains.total > 0 ? " has-local-failures" : ""}`}
          aria-labelledby="pw-failure-domains-title"
        >
          <div className="pw-failure-domains-head">
            <div>
              <span id="pw-failure-domains-title">{wcopy(locale, "engramFailureDomains")}</span>
              <small>{wcopy(locale, "engramFailureDomainsHint")}</small>
            </div>
            <strong aria-label={`${wcopy(locale, "failureDomainTotal")}: ${snapshot.failure_domains.total}`}>
              {snapshot.failure_domains.total}
            </strong>
          </div>
          {snapshot.failure_domains.total > 0 && (
            <p className={`pw-failure-domain-boundary${leaseHealthy ? " is-world-healthy" : ""}`}>
              <Icon name={leaseHealthy ? "check" : "info"} size={12} />
              <span>
                {wcopy(
                  locale,
                  leaseHealthy ? "failureDomainBoundaryHealthy" : "failureDomainBoundarySeparate",
                )}
              </span>
            </p>
          )}
          <dl className="pw-failure-domain-counts">
            <div>
              <dt>{wcopy(locale, "failureDomainTotal")}</dt>
              <dd>{snapshot.failure_domains.total}</dd>
            </div>
            <div className="state-cooling">
              <dt>{wcopy(locale, "failureDomainCooling")}</dt>
              <dd>{snapshot.failure_domains.cooling}</dd>
            </div>
            <div className="state-degraded">
              <dt>{wcopy(locale, "failureDomainDegraded")}</dt>
              <dd>{snapshot.failure_domains.degraded}</dd>
            </div>
            <div className="state-probe-ready">
              <dt>{wcopy(locale, "failureDomainProbeReady")}</dt>
              <dd>{snapshot.failure_domains.probe_ready}</dd>
            </div>
          </dl>
          {snapshot.failure_domains.items.length === 0 ? (
            <p className="pw-failure-domain-empty">{wcopy(locale, "failureDomainNone")}</p>
          ) : (
            <div className="pw-failure-domain-list">
              {snapshot.failure_domains.items.map((item) => {
                const label = labels.get(item.engram_id) ?? shortSignature(item.engram_id);
                return (
                  <article className={`pw-failure-domain-row state-${item.state.replace("_", "-")}`} key={item.engram_id}>
                    <header>
                      <div>
                        <strong title={item.engram_id}>{label}</strong>
                        {label !== shortSignature(item.engram_id) && (
                          <small title={item.engram_id}>{shortSignature(item.engram_id)}</small>
                        )}
                      </div>
                      <AdmissionState
                        value={item.state}
                        label={failureDomainStateLabel(item.state, locale)}
                        title={`state=${item.state}`}
                      />
                    </header>
                    <div className="pw-failure-domain-meta">
                      <span>
                        {item.consecutive_failures} {wcopy(locale, "failureDomainFailures")}
                      </span>
                      <code title={`phase=${item.last_error_phase ?? "null"}; code=${item.last_error_code}`}>
                        {item.last_error_phase ?? "—"} / {item.last_error_code}
                      </code>
                      <time dateTime={item.last_failure_at} title={item.last_failure_at}>
                        {wcopy(locale, "failureDomainLastFailure")} {relativeTime(item.last_failure_at, locale, now)}
                      </time>
                    </div>
                    <p>
                      {failureDomainHint(item.state, locale)}
                      {item.state === "cooling" && item.retry_at !== null && (
                        <span>
                          {" "}{wcopy(locale, "failureDomainRetryAt")}{" "}
                          <time dateTime={item.retry_at} title={item.retry_at}>
                            {timestampLabel(item.retry_at, locale)}
                          </time>
                        </span>
                      )}
                    </p>
                  </article>
                );
              })}
            </div>
          )}
          {snapshot.failure_domains.truncated && (
            <small className="pw-failure-domain-truncated">
              {wcopy(locale, "failureDomainTruncated")} · {snapshot.failure_domains.items.length}/{snapshot.failure_domains.total}
            </small>
          )}
        </div>

        <div className="pw-admission-lanes" aria-label={wcopy(locale, "capacity")}>
          {snapshot.lanes.map((lane) => (
            <div
              className={`pw-admission-lane${
                lane.lane === "work" || lane.lane === "life" ? ` lane-${lane.lane}` : ""
              }`}
              key={lane.lane}
            >
              <span className="pw-lane-shape" aria-hidden="true">
                {lane.lane === "work" ? "W" : lane.lane === "life" ? "L" : "·"}
              </span>
              <div>
                <strong title={lane.lane}>{laneLabel(lane.lane, locale)}</strong>
                <span>
                  {lane.waiting_centers} {wcopy(locale, "waitingCenters")} · {wcopy(locale, "maxDebt")} {lane.max_debt}
                </span>
                <small>{wcopy(locale, "lastAdmitted")} {relativeTime(lane.last_admitted_at, locale, now)}</small>
              </div>
            </div>
          ))}
        </div>

        <div className="pw-admission-detail">
          <div className="pw-admission-detail-head">
            <span>{wcopy(locale, "selectedCenter")}</span>
            {selectedCenterId !== null && (
              <strong
                aria-label={`${wcopy(locale, "selectedCenter")}: ${selectedCenterId}`}
                title={selectedCenterId}
              >
                {shortSignature(selectedCenterId)}
              </strong>
            )}
          </div>
          {selectedCenterId === null ? (
            <p>{wcopy(locale, "selectCenterForAdmission")}</p>
          ) : (
            <div className="pw-admission-decision">
              <AdmissionState
                value={selectedDecision}
                label={admissionDecisionLabel(selectedDecision, locale)}
                title={`${wcopy(locale, "decision")}: ${selectedDecision}`}
              />
              <span className="pw-admission-reason" title={`${wcopy(locale, "reason")}: ${selectedReason}`}>
                {admissionReasonLabel(selectedReason, locale)}
              </span>
              <span>{wcopy(locale, "debt")} {selected?.starvation_debt ?? 0}</span>
              {selected !== null && (
                <time title={selected.last_decision_at} dateTime={selected.last_decision_at}>
                  {relativeTime(selected.last_decision_at, locale, now)}
                </time>
              )}
            </div>
          )}
          {selected?.lane === "life" && selected.reason === "no_ready_event" && (
            <p className="pw-admission-human-summary">{wcopy(locale, "lifeQuietSummary")}</p>
          )}
        </div>

        <div className="pw-admission-detail pw-reservation-detail">
          <div className="pw-admission-detail-head">
            <span>{wcopy(locale, "recentReservation")}</span>
            {recent !== null && (
              <strong
                aria-label={`${wcopy(locale, "recentReservation")}: ${recent.id}`}
                title={recent.id}
              >
                {shortSignature(recent.id)}
              </strong>
            )}
          </div>
          {recent === null ? (
            <p>{wcopy(locale, "noReservations")}</p>
          ) : (
            <div className="pw-reservation-row">
              <AdmissionState
                value={recent.state}
                label={reservationLabel(recent.state, locale)}
                title={`state=${recent.state}; outcome=${recent.outcome ?? "null"}`}
              />
              {recent.outcome !== null && (
                <AdmissionState
                  value={recent.outcome}
                  label={reservationLabel(recent.outcome, locale)}
                  title={`outcome=${recent.outcome}`}
                />
              )}
              <span title={recent.reason}>{laneLabel(recent.lane, locale)} · {admissionReasonLabel(recent.reason, locale)}</span>
              <time
                dateTime={recent.settled_at ?? recent.created_at}
                title={recent.settled_at ?? recent.created_at}
              >
                {relativeTime(recent.settled_at ?? recent.created_at, locale, now)}
              </time>
            </div>
          )}
        </div>
      </div>
    </section>
  );
}

function TuningPanel({
  tuning,
  base,
}: {
  tuning: Fetched<TuningSnapshot>;
  base: string | null;
}) {
  const { locale } = useI18n();
  const [expanded, setExpanded] = useState(false);
  const [drafts, setDrafts] = useState<Partial<KnobValues>>({});
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const snapshot = tuning.data;
  const hasDrafts = KNOB_NAMES.some((key) => Object.hasOwn(drafts, key));

  const apply = async () => {
    if (base === null || snapshot === null || !hasDrafts || sending) return;
    setSending(true);
    setError(null);
    try {
      await postTuning(base, mergeKnobs(snapshot.commanded, drafts));
      setDrafts({});
      tuning.reload();
    } catch (cause) {
      setError(faultText(cause));
    } finally {
      setSending(false);
    }
  };

  return (
    <section className={`pw-field-section pw-tuning-section${expanded ? " is-expanded" : ""}`}>
      <button className="pw-field-section-head pw-disclosure-head" onClick={() => setExpanded((value) => !value)}>
        <div>
          <span>{wcopy(locale, "tuning")}</span>
          <small>{wcopy(locale, "tuningStream")}</small>
        </div>
        <span className="pw-independent">
          {wcopy(locale, "independent")}
        </span>
        <Icon name={expanded ? "chevronDown" : "chevronRight"} size={14} />
      </button>
      {tuning.state !== "ok" || snapshot === null ? (
        <div className="pw-compact-fault">
          <span>{tuning.state === "absent" ? wcopy(locale, "unavailable") : wcopy(locale, "connecting")}</span>
          <button onClick={tuning.reload}>{wcopy(locale, "retry")}</button>
        </div>
      ) : (
        <>
          <div className="pw-tuning-summary">
            {KNOBS.map((spec) => {
              const observed = snapshot.observed[spec.key];
              const commanded = snapshot.commanded[spec.key];
              const draft = Object.hasOwn(drafts, spec.key) ? drafts[spec.key] : undefined;
              const value = draft ?? commanded ?? observed;
              const pct =
                value === null || value === undefined
                  ? 0
                  : Math.min(1, Math.max(0, (value - spec.min) / (spec.max - spec.min)));
              return (
                <div className="pw-tuning-cell" key={spec.key}>
                  <span>{wcopy(locale, spec.labelKey)}</span>
                  <div className="pw-tuning-spark" aria-hidden="true">
                    <i style={{ width: `${Math.max(4, pct * 100)}%` }} />
                    {commanded !== null && (
                      <b style={{ left: `${Math.min(100, Math.max(0, ((commanded - spec.min) / (spec.max - spec.min)) * 100))}%` }} />
                    )}
                  </div>
                  <strong>
                    {value === null || value === undefined
                      ? "—"
                      : spec.format(value)}
                  </strong>
                  {commanded === null && <small>{wcopy(locale, "uncommanded")}</small>}
                </div>
              );
            })}
          </div>
          {expanded && (
            <div className="pw-tuning-editor">
              {KNOBS.map((spec) => {
                const observed = snapshot.observed[spec.key];
                const commanded = snapshot.commanded[spec.key];
                const hasDraft = Object.hasOwn(drafts, spec.key);
                const draft = hasDraft ? drafts[spec.key] : commanded;
                const value = draft ?? observed ?? spec.min;
                return (
                  <div className="pw-knob-editor" key={spec.key}>
                    <div className="pw-knob-editor-head">
                      <span>{wcopy(locale, spec.labelKey)}</span>
                      <span>
                        {wcopy(locale, "observed")}{" "}
                        {observed === null ? "—" : spec.format(observed)}
                      </span>
                      <button
                        onClick={() =>
                          setDrafts((current) => ({
                            ...current,
                            [spec.key]: commanded === null ? (observed ?? spec.min) : null,
                          }))
                        }
                      >
                        {commanded === null ? wcopy(locale, "takeOver") : wcopy(locale, "release")}
                      </button>
                    </div>
                    <input
                      type="range"
                      min={spec.min}
                      max={spec.max}
                      step={spec.step}
                      disabled={draft === null}
                      value={value}
                      onChange={(event) =>
                        setDrafts((current) => ({
                          ...current,
                          [spec.key]: Number(event.target.value),
                        }))
                      }
                    />
                  </div>
                );
              })}
              {error !== null && <div className="pw-inline-error">{error}</div>}
              <div className="pw-tuning-actions">
                <button disabled={!hasDrafts || sending} onClick={() => setDrafts({})}>
                  {wcopy(locale, "discard")}
                </button>
                <button className="is-primary" disabled={!hasDrafts || sending} onClick={() => void apply()}>
                  {wcopy(locale, "apply")}
                </button>
              </div>
            </div>
          )}
        </>
      )}
    </section>
  );
}

function AdvisoryPanel() {
  const { locale } = useI18n();
  const [expanded, setExpanded] = useState(false);
  return (
    <section className="pw-field-section pw-advisory-section">
      <button className="pw-field-section-head pw-disclosure-head" onClick={() => setExpanded((value) => !value)}>
        <div>
          <span>{wcopy(locale, "advisory")}</span>
          <small>{wcopy(locale, "advisorySubtitle")}</small>
        </div>
        <span className="pw-muted-state">{wcopy(locale, "unavailable")}</span>
        <Icon name={expanded ? "chevronDown" : "chevronRight"} size={14} />
      </button>
      {expanded && (
        <div className="pw-advisory-empty">
          <Icon name="info" size={15} />
          <span>
            {locale === "zh-CN"
              ? zhText("workbench.RuntimeRail.line1804")
              : "The runtime does not expose a persistent Advisory lifecycle yet; offered, injected, and used states are never fabricated."}
          </span>
        </div>
      )}
    </section>
  );
}

function statusLabel(status: string | null, locale: "en" | "zh-CN"): string {
  const value = status?.toLowerCase() ?? "queued";
  if (value.includes("fail") || value.includes("error")) return wcopy(locale, "failed");
  if (value.includes("done") || value.includes("return") || value.includes("complete")) {
    return wcopy(locale, "returned");
  }
  if (value.includes("run") || value.includes("progress") || value.includes("start")) {
    return wcopy(locale, "inProgress");
  }
  return wcopy(locale, "queued");
}

function DelegationPanel({
  delegations,
}: {
  delegations: Fetched<DelegationRow[]>;
}) {
  const { locale } = useI18n();
  const [expanded, setExpanded] = useState(false);
  const rows = delegations.data ?? [];
  const visible = expanded ? rows.slice(0, 6) : rows.slice(0, 1);
  return (
    <section className="pw-field-section pw-delegation-section">
      <button className="pw-field-section-head pw-disclosure-head" onClick={() => setExpanded((value) => !value)}>
        <div>
          <span>{wcopy(locale, "delegation")}</span>
          <small>{wcopy(locale, "runtimeRoute")}</small>
        </div>
        {rows.length > 0 && <span className="pw-count-badge">{rows.length}</span>}
        <Icon name={expanded ? "chevronDown" : "chevronRight"} size={14} />
      </button>
      {delegations.state !== "ok" ? (
        <div className="pw-compact-fault">
          <span>{wcopy(locale, delegations.state === "absent" ? "unavailable" : "connecting")}</span>
          <button onClick={delegations.reload}>{wcopy(locale, "retry")}</button>
        </div>
      ) : rows.length === 0 ? (
        <div className="pw-delegation-empty">
          <HexMark label="PI" tone="blue" size={38} />
          <div>
            <strong>{wcopy(locale, "agentBackend")}</strong>
            <span>{wcopy(locale, "noDelegations")}</span>
          </div>
        </div>
      ) : (
        <div className="pw-delegation-list">
          {visible.map((row) => (
            <div className="pw-delegation-row" key={row.id}>
              <HexMark label="D" tone="blue" size={38} />
              <div className="pw-delegation-task">
                <strong>{row.task || row.id}</strong>
                <span>
                  {row.chosen === null ? wcopy(locale, "routerDecides") : shortSignature(row.chosen)}
                  {row.backend === null ? "" : ` · ${row.backend}`}
                </span>
              </div>
              <span className={`pw-delegation-status status-${row.status ?? "queued"}`}>
                {statusLabel(row.status, locale)}
              </span>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

export function RuntimeRail({
  collapsed,
  harnessKind,
  selectedEngram,
  selectedCenter,
  worldId,
  onToggle,
  onSelectEngram,
}: {
  collapsed: boolean;
  harnessKind: string | null;
  selectedEngram: string | null;
  selectedCenter: string | null;
  worldId: string | null;
  onToggle: () => void;
  onSelectEngram: (id: string) => void;
}) {
  const { locale } = useI18n();
  const runtimeBase = useRuntimeBase();
  const base = collapsed ? null : runtimeBase;
  const [projection, setProjection] = useState<Projection>(() => {
    try {
      return window.localStorage.getItem(VIEW_KEY) === "field" ? "field" : "trace";
    } catch {
      return "trace";
    }
  });
  const [now, setNow] = useState(() => Date.now());
  const [windowSec, setWindowSec] = useState(300);
  const [selectedOnly, setSelectedOnly] = useState(false);
  const [observedEngram, setObservedEngram] = useState<string | null>(selectedEngram);
  const [evidence, setEvidence] = useState<EvidenceEvent[]>([]);
  const evidenceKeys = useRef(new Set<string>());
  const lastFire = useRef(new Map<string, number>());
  const closeRailRef = useRef<HTMLButtonElement>(null);
  const previouslyCollapsed = useRef(collapsed);

  useEffect(() => {
    setObservedEngram(selectedEngram);
  }, [selectedCenter, selectedEngram]);

  useEffect(() => {
    const wasCollapsed = previouslyCollapsed.current;
    previouslyCollapsed.current = collapsed;
    if (wasCollapsed && !collapsed) {
      const frame = window.requestAnimationFrame(() => closeRailRef.current?.focus());
      return () => window.cancelAnimationFrame(frame);
    }
  }, [collapsed]);

  const health = useEndpoint(base, "/health", parseHealth, 10_000);
  const active = useEndpoint(base, "/pulse/active", parseActive, 2_000);
  const shutdown = useEndpoint(
    base,
    RUNTIME_SHUTDOWN_PATH,
    parseRuntimeShutdown,
    1_000,
  );
  const scheduling = useEndpoint(base, "/scheduling", parseScheduling, 3_000);
  const connectivity = useEndpoint(
    projection === "field" ? base : null,
    "/pulse/connectivity",
    parseConnectivity,
    5_000,
  );
  const tuning = useEndpoint(base, "/tuning", parseTuning, 5_000);
  const delegations = useEndpoint(base, "/delegations?limit=20", parseDelegations, 10_000);
  const firingLog = useFiringLog(base, windowSec);

  const onEvent = (event: StreamEvent) => {
    const normalized = normalizeEvidence(event);
    if (normalized !== null && !evidenceKeys.current.has(normalized.id)) {
      evidenceKeys.current.add(normalized.id);
      setEvidence((current) => {
        const next = [...current, normalized]
          .sort((a, b) => a.tMs - b.tMs)
          .slice(-MAX_TRACE_EVENTS);
        const liveKeys = new Set(next.map((item) => item.id));
        for (const key of evidenceKeys.current) {
          if (!liveKeys.has(key)) evidenceKeys.current.delete(key);
        }
        return next;
      });
    }

    if (event.type === "pulse") {
      const id =
        str(event.payload.engram_id) ??
        str(event.payload.engram);
      if (id !== null) {
        const tMs = eventTime(event.payload);
        const kind = str(event.payload.kind) ?? str(event.payload.reason) ?? "unknown";
        firingLog.push({ engramId: id, tMs, kind });
        if (Date.now() - tMs < 10_000) lastFire.current.set(id, tMs);
      }
    } else if (event.type === "tuning_applied") {
      tuning.reload();
    } else if (
      event.type === "delegation" ||
      event.type.startsWith("delegation_") ||
      event.type === "route"
    ) {
      delegations.reload();
    }
    if (event.type === "runtime_stop") shutdown.reload();
    if (SCHEDULING_EVENTS.has(event.type)) scheduling.reload();
  };
  const stream = useRailStream(base, onEvent);

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, []);

  const roster = active.data ?? [];
  const labels = useMemo(
    () =>
      new Map(
        roster.map((engram) => [
          engram.engram_id,
          engram.nickname ?? engram.name ?? shortSignature(engram.engram_id),
        ]),
      ),
    [roster],
  );
  const link = liveStatus(health, stream);
  const focusedEngram = observedEngram ?? selectedEngram;
  const focusedEvidence =
    selectedOnly && focusedEngram !== null
      ? evidence.filter((event) => event.actorId === focusedEngram)
      : evidence;
  const focusedFiringLog =
    selectedOnly && focusedEngram !== null
      ? {
          ...firingLog,
          marks: firingLog.marks.filter((mark) => mark.engramId === focusedEngram),
        }
      : firingLog;
  const focusedRoster =
    selectedOnly && focusedEngram !== null
      ? [focusedEngram]
      : roster.map((engram) => engram.engram_id);

  if (collapsed) {
    return (
      <aside className="pw-runtime-rail pw-runtime-rail-collapsed" aria-label={wcopy(locale, "pulseRail")}>
        <button
          aria-label={wcopy(locale, "expandRail")}
          onClick={onToggle}
          title={wcopy(locale, "expandRail")}
        >
          <Icon name="panelRight" />
          <span>{wcopy(locale, "pulse")}</span>
          <i className={`pw-link-dot tone-${link.tone}`} />
        </button>
      </aside>
    );
  }

  return (
    <aside className="pw-runtime-rail" aria-label={wcopy(locale, "pulseRail")}>
      <header className="pw-rail-head">
        <div className="pw-rail-brand">
          <span>{wcopy(locale, "pulse")}</span>
          <span className={`pw-live-state state-${link.tone}`}>
            <i />
            {wcopy(locale, link.key)}
          </span>
          {harnessKind !== null && (
            <span
              className={`pw-harness-mode mode-${harnessKind.toLowerCase().replace(/[^a-z0-9-]+/g, "-")}`}
              title={harnessKind}
            >
              {harnessKind === "mock" ? wcopy(locale, "simulatedHarness") : harnessKind}
            </span>
          )}
          <ShutdownIndicator shutdown={shutdown} />
        </div>
        <span
          className="pw-scope-control"
          title={wcopy(locale, "runtimeScopeOnly")}
          aria-label={`${wcopy(locale, "scope")}: ${wcopy(locale, "runtime")}`}
        >
          {wcopy(locale, "runtime")}
        </span>
        <button
          className={`pw-icon-button${selectedOnly ? " is-active" : ""}`}
          aria-label={
            selectedOnly
              ? wcopy(locale, "showAllEngrams")
              : wcopy(locale, "focusSelected")
          }
          aria-pressed={selectedOnly}
          disabled={focusedEngram === null}
          title={
            selectedOnly
              ? wcopy(locale, "showAllEngrams")
              : wcopy(locale, "focusSelected")
          }
          onClick={() => setSelectedOnly((value) => !value)}
        >
          <Icon name="filter" size={16} />
        </button>
        <button
          ref={closeRailRef}
          className="pw-icon-button"
          aria-label={wcopy(locale, "collapseRail")}
          onClick={onToggle}
          title={wcopy(locale, "collapseRail")}
        >
          <Icon name="panelRight" size={17} />
        </button>
      </header>

      <section className="pw-presence">
        <div className="pw-rail-section-label">
          <div>
            <span>{wcopy(locale, "currentPresence")}</span>
            <small>{wcopy(locale, "currentPresenceHint")}</small>
          </div>
          {active.state === "ok" && active.data !== null && (
            <span>{active.data.length}</span>
          )}
        </div>
        <Presence
          active={active}
          selectedId={focusedEngram}
          now={now}
          onSelect={(id) => {
            setObservedEngram(id);
            onSelectEngram(id);
          }}
        />
      </section>

      <div className="pw-projection-tabs" role="tablist">
        <button
          role="tab"
          id="pw-trace-tab"
          aria-controls="pw-trace-panel"
          aria-selected={projection === "trace"}
          className={projection === "trace" ? "is-active" : ""}
          title={wcopy(locale, "traceHint")}
          onClick={() => {
            setProjection("trace");
            try {
              window.localStorage.setItem(VIEW_KEY, "trace");
            } catch {
              // Local persistence is optional.
            }
          }}
        >
          {wcopy(locale, "trace")}
        </button>
        <button
          role="tab"
          id="pw-field-tab"
          aria-controls="pw-field-panel"
          aria-selected={projection === "field"}
          className={projection === "field" ? "is-active" : ""}
          title={wcopy(locale, "fieldHint")}
          onClick={() => {
            setProjection("field");
            try {
              window.localStorage.setItem(VIEW_KEY, "field");
            } catch {
              // Local persistence is optional.
            }
          }}
        >
          {wcopy(locale, "field")}
        </button>
      </div>

      <div className="pw-projection-stage">
        <div
          className={`pw-projection-panel pw-trace-panel${projection === "trace" ? " is-active" : ""}`}
          id="pw-trace-panel"
          role="tabpanel"
          aria-labelledby="pw-trace-tab"
          hidden={projection !== "trace"}
        >
          <div className="pw-projection-intro">
            <span>{wcopy(locale, "trace")}</span>
            <small>{wcopy(locale, "traceHint")}</small>
          </div>
          <EvidenceTrace
            evidence={focusedEvidence}
            firingLog={focusedFiringLog}
            labels={labels}
            selectedActor={focusedEngram}
            windowSec={windowSec}
            onSelectActor={setObservedEngram}
          />
        </div>
        <div
          className={`pw-projection-panel pw-field-panel${projection === "field" ? " is-active" : ""}`}
          id="pw-field-panel"
          role="tabpanel"
          aria-labelledby="pw-field-tab"
          hidden={projection !== "field"}
        >
          <ShutdownPanel shutdown={shutdown} />
          <AdmissionPanel
            scheduling={scheduling}
            selectedCenterId={selectedCenter}
            labels={labels}
            now={now}
          />
          <ConnectivityPanel connectivity={connectivity} />
          <ActivationHistory
            log={focusedFiringLog}
            roster={focusedRoster}
            labels={labels}
            windowSec={windowSec}
            selectedId={focusedEngram}
            onSelect={setObservedEngram}
            onWindowChange={setWindowSec}
          />
          <TuningPanel tuning={tuning} base={base} />
          <AdvisoryPanel />
          <DelegationPanel delegations={delegations} />
        </div>
      </div>

      <CausalTimeline
        base={base}
        worldId={worldId}
        engramId={focusedEngram}
        centerId={selectedCenter}
      />
    </aside>
  );
}

export function toEngramSummary(engram: ActiveEngram): EngramSummary {
  return {
    id: engram.engram_id,
    name: engram.name,
    name_origin: engram.name_origin,
    nickname: engram.nickname,
    project_id: null,
    status: "active",
    created_at: null,
    last_pulse_at: engram.last_fired_at,
    total_pulses: 0,
    message_count: 0,
  };
}

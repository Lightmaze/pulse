import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import { localizedRuntimeFault, useI18n } from "../i18n";
import {
  fetchCausalDetail,
  reconcileCausalEvent,
  useCausalStream,
  type CausalDetailResponse,
  type CausalAmplificationView,
  type CausalEventView,
  type CausalScope,
  type ReconcileAction,
} from "../causal";
import { Icon } from "./Icons";
import { zhText } from "../locales/zh-ui.ts";

interface CausalTimelineProps {
  base: string | null;
  worldId: string | null;
  engramId: string | null;
  centerId: string | null;
}

interface CausalGroup {
  id: string;
  events: CausalEventView[];
  depths: Map<string, number>;
}

function shortId(value: string): string {
  return value.length <= 12 ? value : `${value.slice(0, 6)}…${value.slice(-4)}`;
}

function cssToken(value: string): string {
  return value.replace(/[^a-zA-Z0-9_-]/g, "-");
}

function eventLabel(event: CausalEventView, zh: boolean): string {
  if (event.metadata.reason_code === "living_orientation_engagement") {
    return zh ? zhText("workbench.CausalTimeline.line38") : "living orientation engagement";
  }
  if (
    event.metadata.reason === "living_concern_reentry" ||
    event.metadata.reason_code === "living_concern_reentry"
  ) {
    return zh ? zhText("workbench.CausalTimeline.line44") : "living concern re-entry";
  }
  const toolName = typeof event.metadata.tool_name === "string"
    ? event.metadata.tool_name
    : null;
  if ((event.kind === "tool_call" || event.kind === "tool_result") && toolName !== null) {
    return `${zh ? (event.kind === "tool_call" ? zhText("workbench.CausalTimeline.line50") : zhText("workbench.CausalTimeline.line50.2")) : event.kind.replaceAll("_", " ")} · ${toolName}`;
  }
  const labels: Record<string, string> = zh
    ? {
        stimulus: zhText("workbench.CausalTimeline.line54"),
        spontaneous: zhText("workbench.CausalTimeline.line55"),
        pulse: zhText("workbench.CausalTimeline.line56"),
        propagation: zhText("workbench.CausalTimeline.line57"),
        tool_call: zhText("workbench.CausalTimeline.line58"),
        tool_result: zhText("workbench.CausalTimeline.line59"),
        habitat_observation: zhText("workbench.CausalTimeline.line60"),
        habitat_action: zhText("workbench.CausalTimeline.line61"),
        habitat_consequence: zhText("workbench.CausalTimeline.line62"),
        delegation_request: zhText("workbench.CausalTimeline.line63"),
        delegation_result: zhText("workbench.CausalTimeline.line64"),
        generation_transition: zhText("workbench.CausalTimeline.line65"),
        assistant_result: zhText("workbench.CausalTimeline.line66"),
        system: zhText("workbench.CausalTimeline.line67"),
      }
    : {
        stimulus: "stimulus",
        spontaneous: "spontaneous pulse",
        pulse: "pulse",
        propagation: "propagation",
        tool_call: "tool",
        tool_result: "tool result",
        habitat_observation: "observation",
        habitat_action: "action",
        habitat_consequence: "consequence",
        delegation_request: "delegation request",
        delegation_result: "delegation result",
        generation_transition: "generation",
        assistant_result: "assistant result",
        system: "system",
      };
  return labels[event.kind] ?? event.kind.replaceAll("_", " ");
}

function statusLabel(status: string, zh: boolean): string {
  if (!zh) return status;
  return {
    queued: zhText("workbench.CausalTimeline.line91"),
    running: zhText("workbench.CausalTimeline.line92"),
    settled: zhText("workbench.CausalTimeline.line93"),
    failed: zhText("workbench.CausalTimeline.line94"),
    uncertain: zhText("workbench.CausalTimeline.line95"),
    reconciled: zhText("workbench.CausalTimeline.line96"),
    cancelled: zhText("workbench.CausalTimeline.line97"),
  }[status] ?? status;
}

function eventTime(value: string | null, zh: boolean): string {
  if (value === null) return "—";
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) return value;
  return new Intl.DateTimeFormat(zh ? "zh-CN" : "en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(parsed);
}

function flowLabel(flow: CausalEventView["flow"], zh: boolean): string | null {
  if (flow === null) return null;
  return zh
    ? { content: zhText("workbench.CausalTimeline.line115"), spectrum: zhText("workbench.CausalTimeline.line115.2"), tunnel: zhText("workbench.CausalTimeline.line115.3") }[flow]
    : flow;
}

function compactDuration(value: number | null): string {
  if (value === null) return "—";
  if (value < 1_000) return `${Math.round(value)} ms`;
  if (value < 60_000) return `${(value / 1_000).toFixed(1)} s`;
  return `${(value / 60_000).toFixed(1)} min`;
}

function ChainAmplification({
  value,
  zh,
}: {
  value: CausalAmplificationView;
  zh: boolean;
}) {
  const amplification = value.amplification;
  const settle = value.settle_cost;
  const violations = value.flow_contract.violation_event_count;
  const tokenTotal = settle.input_tokens + settle.output_tokens;
  const items = [
    {
      label: zh ? zhText("workbench.CausalTimeline.line139") : "Claimed visits",
      value: `${amplification.claimed_turn_root_count} / ${amplification.turn_root_count}`,
    },
    {
      label: "Engrams",
      value: amplification.distinct_engram_count.toLocaleString(),
    },
    {
      label: zh ? zhText("workbench.CausalTimeline.line147") : "Revisits",
      value: amplification.revisit_count.toLocaleString(),
    },
    {
      label: zh ? zhText("workbench.CausalTimeline.line151") : "Propagation depth",
      value: amplification.max_propagation_depth.toLocaleString(),
    },
    {
      label: zh ? zhText("workbench.CausalTimeline.line155") : "Propagation / events",
      value: `${amplification.propagation_event_count} / ${amplification.event_count}`,
    },
    {
      label: zh ? zhText("workbench.CausalTimeline.line159") : "Queued / oldest",
      value: `${value.queue.queued_event_count} · ${compactDuration(value.queue.oldest_queued_age_ms)}`,
    },
    {
      label: zh ? zhText("workbench.CausalTimeline.line163") : "Settled / active time",
      value: `${settle.settled_turn_count} · ${compactDuration(settle.active_ms_total)}`,
    },
    {
      label: zh ? zhText("workbench.CausalTimeline.line167") : "Input + output tokens",
      value: tokenTotal.toLocaleString(),
    },
    {
      label: zh ? zhText("workbench.CausalTimeline.line171") : "Flow violations",
      value: violations.toLocaleString(),
      danger: violations > 0,
    },
  ];
  return (
    <div className="pw-causal-amplification">
      <div className="pw-causal-amplification-head">
        <strong>{zh ? zhText("workbench.CausalTimeline.line179") : "Complete causal chain"}</strong>
        <span>durable · #{value.scope.first_seq}–#{value.scope.last_seq}</span>
      </div>
      <div className="pw-causal-amplification-grid">
        {items.map((item) => (
          <span key={item.label} className={item.danger ? "is-danger" : ""}>
            <small>{item.label}</small>
            <b>{item.value}</b>
          </span>
        ))}
      </div>
      <small className="pw-causal-amplification-evidence">
        {zh ? zhText("workbench.CausalTimeline.line191") : "Durable ledger projection"}
        {` · usage ${settle.usage_complete_turn_count}/${settle.settled_turn_count}`}
      </small>
    </div>
  );
}

function groupEvents(events: CausalEventView[]): CausalGroup[] {
  const groups = new Map<string, CausalEventView[]>();
  for (const event of events) {
    const group = groups.get(event.causal_id) ?? [];
    group.push(event);
    groups.set(event.causal_id, group);
  }
  return Array.from(groups, ([id, rawEvents]) => {
    const eventsInGroup = rawEvents.sort((left, right) => left.seq - right.seq);
    const byId = new Map(eventsInGroup.map((event) => [event.id, event]));
    const depths = new Map<string, number>();
    const depthFor = (event: CausalEventView, trail: Set<string>): number => {
      const cached = depths.get(event.id);
      if (cached !== undefined) return cached;
      if (trail.has(event.id) || event.parent_event_id === null) {
        depths.set(event.id, 0);
        return 0;
      }
      const parent = byId.get(event.parent_event_id);
      if (parent === undefined) {
        depths.set(event.id, 0);
        return 0;
      }
      const nextTrail = new Set(trail);
      nextTrail.add(event.id);
      const depth = Math.min(depthFor(parent, nextTrail) + 1, 3);
      depths.set(event.id, depth);
      return depth;
    };
    for (const event of eventsInGroup) depthFor(event, new Set());
    return { id, events: eventsInGroup, depths };
  }).sort((left, right) => right.events[0].seq - left.events[0].seq);
}

export function CausalTimeline({
  base,
  worldId,
  engramId,
  centerId,
}: CausalTimelineProps) {
  const { locale } = useI18n();
  const zh = locale === "zh-CN";
  const [scopeKind, setScopeKind] = useState<"world" | "center" | "engram">(
    centerId !== null ? "center" : engramId !== null ? "engram" : "world",
  );
  useEffect(() => {
    setScopeKind(centerId !== null ? "center" : engramId !== null ? "engram" : "world");
  }, [centerId, engramId]);
  const scope = useMemo<CausalScope>(() => {
    if (scopeKind === "center" && centerId !== null) {
      return { kind: "center", worldId, centerId };
    }
    if (scopeKind === "engram" && engramId !== null) {
      return { kind: "engram", worldId, engramId };
    }
    return { kind: "world", worldId };
  }, [centerId, engramId, scopeKind, worldId]);
  const stream = useCausalStream(base, scope);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(() => new Set());
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<CausalDetailResponse | null>(null);
  const [detailState, setDetailState] = useState<"idle" | "loading" | "error">("idle");
  const [detailError, setDetailError] = useState<string | null>(null);
  const [detailAttempt, setDetailAttempt] = useState(0);
  const detailRequest = useRef(0);
  const [pendingAction, setPendingAction] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  const groups = useMemo(() => groupEvents(stream.events), [stream.events]);

  useEffect(() => {
    if (
      selectedId !== null &&
      !groups.some((group) => group.events.some((event) => event.id === selectedId))
    ) {
      setSelectedId(null);
      setDetail(null);
      setDetailState("idle");
    }
  }, [groups, selectedId]);

  useEffect(() => {
    if (selectedId === null || base === null) {
      detailRequest.current += 1;
      setDetail(null);
      setDetailState("idle");
      return;
    }
    const requestId = detailRequest.current + 1;
    detailRequest.current = requestId;
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 5_000);
    setDetailState("loading");
    setDetailError(null);
    void fetchCausalDetail(base, selectedId, controller.signal)
      .then((next) => {
        if (requestId !== detailRequest.current) return;
        window.clearTimeout(timeout);
        setDetail(next);
        setDetailState("idle");
      })
      .catch((cause: unknown) => {
        if (requestId !== detailRequest.current) return;
        window.clearTimeout(timeout);
        setDetailState("error");
        setDetailError(
          controller.signal.aborted
            ? zh ? zhText("workbench.CausalTimeline.line305") : "The detail request timed out or was interrupted."
            : cause instanceof Error ? cause.message : String(cause),
        );
      });
    return () => {
      window.clearTimeout(timeout);
      if (detailRequest.current === requestId) detailRequest.current += 1;
      controller.abort();
    };
  }, [base, detailAttempt, selectedId, zh]);

  const toggleGroup = (causalId: string) => {
    setCollapsedGroups((current) => {
      const next = new Set(current);
      if (next.has(causalId)) next.delete(causalId);
      else next.add(causalId);
      return next;
    });
  };

  const reconcile = async (event: CausalEventView, action: ReconcileAction) => {
    if (base === null || event.status !== "uncertain") return;
    setPendingAction(`${event.id}:${action}`);
    setActionError(null);
    try {
      const response = await reconcileCausalEvent(base, event.id, action);
      stream.merge(response.child === null ? response.event : [response.event, response.child]);
    } catch (cause) {
      setActionError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setPendingAction(null);
    }
  };

  const status = stream.state === "error" ? (zh ? zhText("workbench.CausalTimeline.line339") : "offline") : stream.state;
  const title = zh ? zhText("workbench.CausalTimeline.line340") : "Causal timeline";
  const hint = zh ? zhText("workbench.CausalTimeline.line341") : "Bounded window · server-attributed scope";

  return (
    <section className="pw-causal-dock" aria-label={title}>
      <header className="pw-causal-head">
        <div className="pw-causal-heading">
          <span>{title}</span>
          <small>{hint}</small>
        </div>
        <div className="pw-causal-scope" role="group" aria-label={zh ? zhText("workbench.CausalTimeline.line350") : "Causal scope"}>
          <button
            className={scope.kind === "world" ? "is-active" : ""}
            onClick={() => setScopeKind("world")}
          >{zh ? zhText("workbench.CausalTimeline.line354") : "World"}</button>
          <button
            className={scope.kind === "center" ? "is-active" : ""}
            disabled={centerId === null}
            onClick={() => setScopeKind("center")}
          >Center</button>
          <button
            className={scope.kind === "engram" ? "is-active" : ""}
            disabled={engramId === null}
            onClick={() => setScopeKind("engram")}
          >Engram</button>
        </div>
        <span className={`pw-causal-connection state-${cssToken(stream.state)}`}>
          <i />{status}
        </span>
        <button
          className="pw-icon-button"
          aria-label={zh ? zhText("workbench.CausalTimeline.line371") : "Reconnect causal stream"}
          title={zh ? zhText("workbench.CausalTimeline.line372") : "Reconnect causal stream"}
          onClick={stream.reconnect}
        >
          <Icon name="refresh" size={14} />
        </button>
      </header>

      <div className="pw-causal-body">
        {stream.error !== null && (
          <div className="pw-causal-fault">
            <span>{localizedRuntimeFault(locale, stream.error)}</span>
            <button onClick={stream.reconnect}>{zh ? zhText("workbench.CausalTimeline.line383") : "Retry"}</button>
          </div>
        )}
        {(stream.hasEarlier || stream.atCapacity) && (
          <div className="pw-causal-history-window">
            <button
              disabled={stream.loadingEarlier || stream.atCapacity}
              onClick={stream.loadEarlier}
            >
              {stream.atCapacity
                ? zh ? zhText("workbench.CausalTimeline.line393") : "1,000-event window reached"
                : stream.loadingEarlier
                  ? zh ? zhText("workbench.CausalTimeline.line395") : "Loading earlier history…"
                  : zh ? zhText("workbench.CausalTimeline.line396") : "Load earlier history"}
            </button>
            <span>{scope.kind === "center" ? `Center · ${shortId(scope.centerId)}` : scope.kind === "engram" ? `Engram · ${shortId(scope.engramId)}` : zh ? zhText("workbench.CausalTimeline.line398") : "World scope"}</span>
          </div>
        )}
        {groups.length === 0 ? (
          <div className="pw-causal-empty">
            <span className="pw-causal-empty-node" />
            <div>
              <strong>{zh ? zhText("workbench.CausalTimeline.line405") : "No durable causal events yet"}</strong>
              <span>{zh ? zhText("workbench.CausalTimeline.line406") : "Stimuli, tools, and habitat consequences appear here."}</span>
            </div>
          </div>
        ) : (
          <div className="pw-causal-groups">
            {groups.map((group) => {
              const isOpen = !collapsedGroups.has(group.id);
              const latest = group.events[group.events.length - 1];
              return (
                <div className="pw-causal-group" key={group.id}>
                  <button
                    className="pw-causal-group-head"
                    aria-expanded={isOpen}
                    onClick={() => toggleGroup(group.id)}
                  >
                    <Icon name={isOpen ? "chevronDown" : "chevronRight"} size={12} />
                    <span className="pw-causal-group-id">{shortId(group.id)}</span>
                    <span className={`pw-causal-status status-${cssToken(latest.status)}`}>
                      {statusLabel(latest.status, zh)}
                    </span>
                    <small>{group.events.length} · #{latest.seq}</small>
                  </button>
                  {isOpen && (
                    <div className="pw-causal-events">
                      {group.events.map((event) => {
                        const flow = flowLabel(event.flow, zh);
                        const selected = event.id === selectedId;
                        const uncertain = event.status === "uncertain";
                        const orientationEngagement = event.metadata.reason_code === "living_orientation_engagement";
                        return (
                          <div
                            className={`pw-causal-event kind-${cssToken(event.kind)} status-${cssToken(event.status)}${selected ? " is-selected" : ""}${orientationEngagement ? " is-orientation-engagement" : ""}`}
                            key={event.id}
                            style={{ "--causal-depth": group.depths.get(event.id) ?? 0 } as CSSProperties}
                          >
                            <button
                              className="pw-causal-event-main"
                              onClick={() => setSelectedId(event.id)}
                              aria-pressed={selected}
                            >
                              <span className="pw-causal-event-axis">
                                <i />
                              </span>
                              <span className="pw-causal-event-copy">
                                <strong>{eventLabel(event, zh)}</strong>
                                <small>
                                  #{event.seq} · {eventTime(event.updated_at ?? event.created_at, zh)}
                                  {flow === null ? "" : ` · ${flow}`}
                                  {event.center_id === null ? "" : ` · Center ${shortId(event.center_id)}`}
                                  {event.domain === "habitat" || event.domain === "generation"
                                    ? ` · ${event.domain}`
                                    : ""}
                                </small>
                              </span>
                              <span className="pw-causal-event-status">
                                {statusLabel(event.status, zh)}
                              </span>
                            </button>
                            {uncertain && (
                              <div className="pw-causal-reconcile" role="group" aria-label={zh ? zhText("workbench.CausalTimeline.line465") : "Reconcile uncertain event"}>
                                <span>{zh ? zhText("workbench.CausalTimeline.line466") : "External result is unknown; no automatic replay."}</span>
                                {(["acknowledge", "cancel", "requeue"] as ReconcileAction[]).map((action) => (
                                  <button
                                    key={action}
                                    disabled={pendingAction !== null}
                                    onClick={() => void reconcile(event, action)}
                                  >
                                    {action === "acknowledge"
                                      ? zh ? zhText("workbench.CausalTimeline.line474") : "Acknowledge"
                                      : action === "cancel"
                                        ? zh ? zhText("workbench.CausalTimeline.line476") : "Cancel"
                                        : zh ? zhText("workbench.CausalTimeline.line477") : "Requeue"}
                                  </button>
                                ))}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
        {actionError !== null && <div className="pw-causal-action-error">{actionError}</div>}
        {selectedId !== null && (
          <div className="pw-causal-detail">
            <div className="pw-causal-detail-head">
              <span>{zh ? zhText("workbench.CausalTimeline.line496") : "Event detail"}</span>
              <button className="pw-icon-button" onClick={() => setSelectedId(null)} aria-label={zh ? zhText("workbench.CausalTimeline.line497") : "Close detail"}>
                <Icon name="x" size={13} />
              </button>
            </div>
            {detailState === "loading" && <span className="pw-causal-detail-muted">{zh ? zhText("workbench.CausalTimeline.line501") : "Reading…"}</span>}
            {detailState === "error" && (
              <div className="pw-causal-detail-fault">
                <span className="pw-causal-detail-error">
                  {detailError === null ? null : localizedRuntimeFault(locale, detailError)}
                </span>
                <button onClick={() => setDetailAttempt((attempt) => attempt + 1)}>
                  {zh ? zhText("workbench.CausalTimeline.line508") : "Retry"}
                </button>
              </div>
            )}
            {detailState === "idle" && detail !== null && (
              <>
                {detail.event.content !== null && <p>{detail.event.content}</p>}
                {detail.event.resolution_note !== null && <small>{detail.event.resolution_note}</small>}
                <ChainAmplification value={detail.amplification} zh={zh} />
                <code title={detail.event.id}>{shortId(detail.event.id)} · {detail.event.kind} · {statusLabel(detail.event.status, zh)}</code>
                {detail.event.center_id !== null && <code title={detail.event.center_id}>Center · {shortId(detail.event.center_id)}</code>}
                {typeof detail.event.metadata.reason === "string" && <code>{zh ? zhText("workbench.CausalTimeline.line519") : "reason"} · {detail.event.metadata.reason}</code>}
                {typeof detail.event.metadata.reason_code === "string" && <code>{zh ? zhText("workbench.CausalTimeline.line520") : "reason code"} · {detail.event.metadata.reason_code}</code>}
                {typeof detail.event.metadata.tool_name === "string" && <code>tool · {detail.event.metadata.tool_name}</code>}
                {detail.turn !== null && <code>turn · {shortId(detail.turn.id)} · {detail.turn.state}</code>}
                {detail.turn?.error_code !== null && detail.turn?.error_code !== undefined && <code>{zh ? zhText("workbench.CausalTimeline.line523") : "error code"} · {detail.turn.error_code}</code>}
                {detail.turn?.error_phase !== null && detail.turn?.error_phase !== undefined && <code>{zh ? zhText("workbench.CausalTimeline.line524") : "phase"} · {detail.turn.error_phase}</code>}
                {detail.generation !== null && <code>generation · {shortId(detail.generation.id)} · {detail.generation.state}</code>}
              </>
            )}
          </div>
        )}
      </div>
    </section>
  );
}

export default CausalTimeline;

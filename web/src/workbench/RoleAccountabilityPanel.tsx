import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useI18n, type Locale } from "../i18n";
import {
  fetchRoleAccountability,
  type RoleAccountabilityRole,
  type RoleAccountabilitySnapshot,
} from "../roleAccountability";
import { Icon } from "./Icons";
import { shortSignature } from "./model";
import "./RoleAccountabilityPanel.css";
import { zhText } from "../locales/zh-ui.ts";

const REFRESH_MS = 5_000;

type FetchSnapshot = (
  base: string,
  engramId: string,
  signal?: AbortSignal,
  limit?: number,
) => Promise<RoleAccountabilitySnapshot>;

export interface RoleAccountabilityPanelProps {
  base: string;
  engramId: string | null;
  fetchSnapshot?: FetchSnapshot;
}

function formatMoment(value: string, locale: Locale): string {
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) return value;
  return new Intl.DateTimeFormat(locale, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(parsed);
}

function statusLabel(status: RoleAccountabilityRole["status"], locale: Locale): string {
  if (locale === "en") return status;
  return {
    requested: zhText("workbench.RoleAccountabilityPanel.line41"),
    active: zhText("workbench.RoleAccountabilityPanel.line42"),
    suspended: zhText("workbench.RoleAccountabilityPanel.line43"),
    released: zhText("workbench.RoleAccountabilityPanel.line44"),
    expired: zhText("workbench.RoleAccountabilityPanel.line45"),
    revoked: zhText("workbench.RoleAccountabilityPanel.line46"),
  }[status];
}

function roleClassLabel(role: RoleAccountabilityRole, locale: Locale): string {
  if (role.role_class === "subject_role") {
    return locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line52") : "subject role";
  }
  return locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line54") : "task role";
}

function renewalReason(role: RoleAccountabilityRole, locale: Locale): string {
  const reason = role.renewal_gate.reason_code;
  const zh: Record<string, string> = {
    role_not_active: zhText("workbench.RoleAccountabilityPanel.line60"),
    role_renewal_window_not_open: zhText("workbench.RoleAccountabilityPanel.line61"),
    role_expired: zhText("workbench.RoleAccountabilityPanel.line62"),
    role_direct_output_required: zhText("workbench.RoleAccountabilityPanel.line63"),
    role_coordination_streak_exceeded: zhText("workbench.RoleAccountabilityPanel.line64"),
    role_renewal_window_and_contribution_gate_satisfied: zhText("workbench.RoleAccountabilityPanel.line65"),
  };
  const en: Record<string, string> = {
    role_not_active: "this role version no longer holds authority",
    role_renewal_window_not_open: "the renewal window has not opened",
    role_expired: "the role has expired",
    role_direct_output_required: "a verified direct output is still required",
    role_coordination_streak_exceeded: "coordination continued too long without returning to practice",
    role_renewal_window_and_contribution_gate_satisfied: "the timing and direct-output gates are satisfied",
  };
  return (locale === "zh-CN" ? zh : en)[reason] ?? reason;
}

function outputKindLabel(kind: string, locale: Locale): string {
  if (kind === "workspace_checkpoint") {
    return locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line80") : "workspace checkpoint";
  }
  return locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line82") : "Habitat effect";
}

function FaultView({
  message,
  stale,
  onRetry,
  locale,
}: {
  message: string;
  stale: boolean;
  onRetry: () => void;
  locale: Locale;
}) {
  return (
    <div className={`pw-role-accountability-fault${stale ? " is-stale" : ""}`} role="alert">
      <Icon name="info" size={15} />
      <div>
        <strong>
          {stale
            ? locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line102") : "Refresh failed; showing the last snapshot"
            : locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line103") : "Role accountability is unavailable"}
        </strong>
        <span>{message}</span>
      </div>
      <button type="button" onClick={onRetry}>
        {locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line108") : "Retry"}
      </button>
    </div>
  );
}

function RoleSelector({
  role,
  selected,
  onSelect,
  locale,
}: {
  role: RoleAccountabilityRole;
  selected: boolean;
  onSelect: () => void;
  locale: Locale;
}) {
  const direct = role.contribution_summary.direct_output_count;
  const minimum = role.obligation?.minimum_direct_outputs ?? 0;
  return (
    <button
      type="button"
      className={`pw-role-accountability-role${selected ? " is-selected" : ""}`}
      aria-pressed={selected}
      onClick={onSelect}
    >
      <span className="pw-role-accountability-role-topline">
        <span className={`is-${role.status}`}><i />{statusLabel(role.status, locale)}</span>
        <small>{roleClassLabel(role, locale)}</small>
      </span>
      <strong>{role.role_label}</strong>
      <span className="pw-role-accountability-role-progress">
        <span>
          {locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line141") : "direct output"}
          <b>{direct}{role.obligation !== null ? ` / ${minimum}` : ""}</b>
        </span>
        <span>
          {locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line145") : "coordination streak"}
          <b>{role.contribution_summary.consecutive_coordination}</b>
        </span>
      </span>
      <span className={`pw-role-accountability-gate${role.renewal_gate.eligible_now ? " is-ready" : ""}`}>
        <Icon name={role.renewal_gate.eligible_now ? "check" : "clock"} size={12} />
        {renewalReason(role, locale)}
      </span>
    </button>
  );
}

function RoleDetail({ role, locale }: { role: RoleAccountabilityRole; locale: Locale }) {
  const direct = role.contribution_summary.direct_output_count;
  const minimum = role.obligation?.minimum_direct_outputs ?? 0;
  const directRatio = role.obligation === null
    ? 1
    : Math.min(1, direct / Math.max(1, minimum));
  const centerScope = role.scope.center_ids;
  return (
    <article className="pw-role-accountability-detail">
      <header>
        <div>
          <span>{roleClassLabel(role, locale)} · epoch {role.role_epoch}</span>
          <h3>{role.role_label}</h3>
        </div>
        <span className={`pw-role-accountability-status is-${role.status}`}>
          <i />{statusLabel(role.status, locale)}
        </span>
      </header>

      {role.obligation === null ? (
        <div className="pw-role-accountability-unobligated">
          <Icon name="spark" size={16} />
          <span>
            {locale === "zh-CN"
              ? zhText("workbench.RoleAccountabilityPanel.line181")
              : "This role carries no direct-output obligation; ordinary life is not converted into output accounting."}
          </span>
        </div>
      ) : (
        <div className="pw-role-accountability-obligation">
          <div className="pw-role-accountability-obligation-head">
            <span>{locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line188") : "Obligation this cycle"}</span>
            <strong>{direct} / {minimum}</strong>
          </div>
          <div className="pw-role-accountability-meter" aria-hidden="true">
            <span style={{ width: `${directRatio * 100}%` }} />
          </div>
          <div className="pw-role-accountability-output-kinds">
            {role.obligation.accepted_output_kinds.map((kind) => (
              <span key={kind}><Icon name="check" size={11} />{outputKindLabel(kind, locale)}</span>
            ))}
          </div>
        </div>
      )}

      <div className="pw-role-accountability-facts">
        <div>
          <span>{locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line204") : "Direct outputs"}</span>
          <strong>{direct}</strong>
          <small>
            {role.contribution_summary.last_direct_output_event_id === null
              ? locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line208") : "no owner-verified receipt yet"
              : `${locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line209") : "latest evidence"} · ${shortSignature(role.contribution_summary.last_direct_output_event_id)}`}
          </small>
        </div>
        <div>
          <span>{locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line213") : "Coordination"}</span>
          <strong>{role.contribution_summary.coordination_count}</strong>
          <small>
            {locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line216") : "current streak"} · {role.contribution_summary.consecutive_coordination}
            {role.obligation !== null ? ` / ${role.obligation.max_consecutive_coordination}` : ""}
          </small>
        </div>
        <div className={role.renewal_gate.eligible_now ? "is-ready" : ""}>
          <span>{locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line221") : "Renewal interpretation"}</span>
          <strong>{role.renewal_gate.eligible_now
            ? locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line223") : "ready"
            : locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line224") : "not ready"}</strong>
          <small>{renewalReason(role, locale)}</small>
        </div>
      </div>

      <div className="pw-role-accountability-meta">
        <span>
          <Icon name="clock" size={12} />
          {locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line232") : "renewal window"} · {formatMoment(role.renew_after, locale)} → {formatMoment(role.expires_at, locale)}
        </span>
        <span>
          <Icon name="route" size={12} />
          {locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line236") : "cycle"} · {role.accountability_cycle_id === null ? "—" : shortSignature(role.accountability_cycle_id)}
        </span>
        {centerScope.length > 0 && (
          <span title={centerScope.join(", ")}>
            <Icon name="globe" size={12} />
            {centerScope.length} {locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line241") : centerScope.length === 1 ? "Center" : "Centers"}
          </span>
        )}
      </div>

      <footer>
        <div>
          <Icon name="database" size={14} />
          <span>
            {locale === "zh-CN"
              ? zhText("workbench.RoleAccountabilityPanel.line251")
              : "Only receipt classes and counts are visible; output bodies, prompts, paths, and processes remain private."}
          </span>
        </div>
        <code>{role.evidence.role}</code>
        {role.evidence.contributions.map((evidence) => <code key={evidence}>{evidence}</code>)}
      </footer>
    </article>
  );
}

export function RoleAccountabilityPanel({
  base,
  engramId,
  fetchSnapshot = fetchRoleAccountability,
}: RoleAccountabilityPanelProps) {
  const { locale } = useI18n();
  const [snapshot, setSnapshot] = useState<RoleAccountabilitySnapshot | null>(null);
  const [phase, setPhase] = useState<"idle" | "loading" | "ready" | "refreshing" | "error">("idle");
  const [fault, setFault] = useState<string | null>(null);
  const [selectedRoleId, setSelectedRoleId] = useState<string | null>(null);
  const [collapsed, setCollapsed] = useState(false);
  const controllerRef = useRef<AbortController | null>(null);

  const load = useCallback(async () => {
    if (engramId === null) return;
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    setPhase((current) => current === "idle" || current === "loading" ? "loading" : "refreshing");
    setFault(null);
    try {
      const next = await fetchSnapshot(base, engramId, controller.signal, 32);
      if (controller.signal.aborted) return;
      setSnapshot(next);
      setSelectedRoleId((current) => {
        if (current !== null && next.roles.some((role) => role.role_lease_id === current)) {
          return current;
        }
        return next.roles.find((role) => role.status === "active")?.role_lease_id
          ?? next.roles[0]?.role_lease_id
          ?? null;
      });
      setPhase("ready");
    } catch (cause) {
      if (controller.signal.aborted) return;
      setFault(cause instanceof Error ? cause.message : String(cause));
      setPhase("error");
    }
  }, [base, engramId, fetchSnapshot]);

  useEffect(() => {
    controllerRef.current?.abort();
    setSnapshot(null);
    setSelectedRoleId(null);
    setFault(null);
    if (engramId === null) {
      setPhase("idle");
      return undefined;
    }
    setPhase("loading");
    void load();
    const timer = window.setInterval(() => void load(), REFRESH_MS);
    return () => {
      window.clearInterval(timer);
      controllerRef.current?.abort();
    };
  }, [engramId, load]);

  const selectedRole = snapshot?.roles.find((role) => role.role_lease_id === selectedRoleId)
    ?? null;
  const totals = useMemo(() => {
    const roles = snapshot?.roles ?? [];
    return {
      direct: roles.reduce((sum, role) => sum + role.contribution_summary.direct_output_count, 0),
      attention: roles.filter(
        (role) => role.status === "active" && !role.contribution_summary.renewal_eligible,
      ).length,
    };
  }, [snapshot?.roles]);

  return (
    <section
      className={`pw-role-accountability${collapsed ? " is-collapsed" : ""}`}
      aria-label={locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line335") : "Role accountability observatory"}
      data-observer-effect="READ_ONLY_NO_STIMULUS"
    >
      <header className="pw-role-accountability-head">
        <div className="pw-role-accountability-title">
          <span className="pw-role-accountability-mark"><Icon name="route" size={16} /></span>
          <div>
            <strong>{locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line342") : "Role accountability"}</strong>
            <span>
              {engramId === null
                ? locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line345") : "Choose an Engram"
                : `${shortSignature(engramId)} · ${locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line346") : "read-only, zero-stimulus"}`}
            </span>
          </div>
        </div>
        <div className="pw-role-accountability-head-stats" aria-label={locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line350") : "Role summary"}>
          <span><b>{snapshot?.role_count ?? 0}</b>{locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line351") : "roles"}</span>
          <span><b>{totals.direct}</b>{locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line352") : "outputs"}</span>
          {totals.attention > 0 && <span className="is-attention"><b>{totals.attention}</b>{locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line353") : "need practice"}</span>}
        </div>
        <div className="pw-role-accountability-actions">
          {phase === "refreshing" && <span className="pw-role-accountability-sync" aria-label={locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line356") : "Refreshing"} />}
          <button
            type="button"
            aria-label={locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line359") : "Refresh role accountability"}
            disabled={engramId === null || phase === "loading" || phase === "refreshing"}
            onClick={() => void load()}
          >
            <Icon name="refresh" size={14} />
          </button>
          <button
            type="button"
            aria-expanded={!collapsed}
            aria-label={collapsed
              ? locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line369") : "Expand role accountability"
              : locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line370") : "Collapse role accountability"}
            onClick={() => setCollapsed((value) => !value)}
          >
            <Icon name={collapsed ? "chevronRight" : "chevronDown"} size={14} />
          </button>
        </div>
      </header>

      {!collapsed && (
        <div className="pw-role-accountability-body">
          {engramId === null ? (
            <div className="pw-role-accountability-empty">
              <Icon name="route" size={20} />
              <span>{locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line383") : "Choose a Task Front or Life Center to inspect its roles."}</span>
            </div>
          ) : phase === "loading" && snapshot === null ? (
            <div className="pw-role-accountability-loading"><span /><span /><span /></div>
          ) : phase === "error" && snapshot === null ? (
            <FaultView message={fault ?? "unknown read fault"} stale={false} onRetry={() => void load()} locale={locale} />
          ) : (
            <>
              {phase === "error" && snapshot !== null && (
                <FaultView message={fault ?? "unknown refresh fault"} stale onRetry={() => void load()} locale={locale} />
              )}
              {snapshot !== null && snapshot.roles.length === 0 ? (
                <div className="pw-role-accountability-empty is-life">
                  <Icon name="spark" size={20} />
                  <div>
                    <strong>{locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line398") : "No bounded role is held now"}</strong>
                    <span>
                      {locale === "zh-CN"
                        ? zhText("workbench.RoleAccountabilityPanel.line401")
                        : "Roles are not required for life. No role obligation does not mean no life, interests, or personal projects."}
                    </span>
                  </div>
                </div>
              ) : snapshot !== null ? (
                <div className="pw-role-accountability-content">
                  <nav aria-label={locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line408") : "Select a role"}>
                    {snapshot.roles.map((role) => (
                      <RoleSelector
                        key={role.role_lease_id}
                        role={role}
                        selected={role.role_lease_id === selectedRoleId}
                        onSelect={() => setSelectedRoleId(role.role_lease_id)}
                        locale={locale}
                      />
                    ))}
                  </nav>
                  {selectedRole !== null && <RoleDetail role={selectedRole} locale={locale} />}
                </div>
              ) : null}
              {snapshot?.roles_truncated && (
                <div className="pw-role-accountability-truncated">
                  <Icon name="info" size={13} />
                  {locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line425") : "Only the 32 most recent role versions are shown."}
                </div>
              )}
              {snapshot !== null && (
                <div className="pw-role-accountability-boundary">
                  <Icon name="radio" size={13} />
                  <span>{locale === "zh-CN" ? zhText("workbench.RoleAccountabilityPanel.line431") : "Observation cannot grant a role, write a contribution, amend Purpose, or enqueue a pulse."}</span>
                  <code>{snapshot.observer_effect}</code>
                </div>
              )}
            </>
          )}
        </div>
      )}
    </section>
  );
}

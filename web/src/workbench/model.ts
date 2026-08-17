import type { Locale } from "../i18n";
import {
  relativeTimeEn,
  shutdownEvidenceStateEn,
  statusLabelsEn,
  workbenchEn,
  type WorkbenchCopyKey,
} from "./locales/en.ts";
import {
  relativeTimeZhCN,
  shutdownEvidenceStateZhCN,
  statusLabelsZhCN,
  workbenchZhCN,
} from "./locales/zh-CN.ts";

export interface EngramSummary {
  id: string;
  name: string | null;
  name_origin: string;
  nickname: string | null;
  project_id: string | null;
  status: string;
  created_at: string | null;
  last_pulse_at: string | null;
  total_pulses: number;
  message_count: number;
}

export interface ProjectSummary {
  id: string;
  name: string;
  description?: string | null;
  engram_count?: number;
}

export type WorkspacePage = "sessions" | "trace" | "models" | "cost" | "settings";

export type WorkspaceSelection =
  | { kind: "task"; id: string }
  | { kind: "life"; id: string; subjectEngramId?: string }
  | { kind: "engram"; id: string };

const copy = { en: workbenchEn, "zh-CN": workbenchZhCN } as const;

export type { WorkbenchCopyKey } from "./locales/en.ts";

export function wcopy(locale: Locale, key: WorkbenchCopyKey): string {
  return copy[locale][key];
}

const shutdownEvidenceStateCopy = { en: shutdownEvidenceStateEn, "zh-CN": shutdownEvidenceStateZhCN } as const;

export function shutdownEvidenceStateLabel(locale: Locale, state: string): string {
  const labels = shutdownEvidenceStateCopy[locale] as Record<string, string>;
  return labels[state] ?? state.replaceAll("_", " ");
}

export interface SuccessionCapacityPresentation {
  primary: string;
  secondary: string;
  ariaLabel: string;
}

export function successionCapacityPresentation(
  locale: Locale,
  capacity: {
    succession_worker_limit: number;
    succession_workers_running: number;
    succession_subjects_pending: number;
    succession_subjects_blocked: number;
  },
): SuccessionCapacityPresentation {
  const label = wcopy(locale, "successionExecutionDomain");
  const running = wcopy(locale, "successionRunning");
  const limit = wcopy(locale, "successionLimit");
  const pending = wcopy(locale, "successionPending");
  const blocked = wcopy(locale, "successionBlocked");
  const runningCount = capacity.succession_workers_running;
  const limitCount = capacity.succession_worker_limit;
  const pendingCount = capacity.succession_subjects_pending;
  const blockedCount = capacity.succession_subjects_blocked;

  return {
    primary: `${runningCount} / ${limitCount}`,
    secondary: `${running} / ${limit} · ${pendingCount} ${pending} · ${blockedCount} ${blocked}`,
    ariaLabel: `${label}: ${runningCount} ${running}, ${limitCount} ${limit}, ${pendingCount} ${pending}, ${blockedCount} ${blocked}`,
  };
}

export function shortSignature(signature: string): string {
  if (signature.length <= 12) return signature;
  return `${signature.slice(0, 6)}…${signature.slice(-4)}`;
}

export function displayName(engram: Pick<EngramSummary, "name" | "id">): string {
  const candidate = engram.name?.trim();
  return candidate && candidate !== engram.id
    ? candidate
    : shortSignature(engram.id);
}

export function displayIdentity(
  engram: Pick<EngramSummary, "nickname" | "name" | "id">,
): { primary: string; secondary: string } {
  const name = displayName(engram);
  const signature = shortSignature(engram.id);
  const hasNamedIdentity = name !== signature;
  return engram.nickname === null || engram.nickname.trim() === ""
    ? {
        primary: name,
        secondary: hasNamedIdentity ? signature : "",
      }
    : {
        primary: engram.nickname,
        secondary: hasNamedIdentity ? `${name} · ${signature}` : signature,
      };
}

export function statusLabel(locale: Locale, status: string): string {
  const labels = locale === "zh-CN" ? statusLabelsZhCN : statusLabelsEn;
  return (labels as Record<string, string>)[status] ?? status;
}

export function relativeTime(
  timestamp: string | null,
  locale: Locale,
  now = Date.now(),
): string {
  const format = locale === "zh-CN" ? relativeTimeZhCN : relativeTimeEn;
  if (timestamp === null) return format.never;
  const parsed = Date.parse(timestamp);
  if (!Number.isFinite(parsed)) return timestamp;
  const delta = Math.max(0, now - parsed);
  if (delta < 60_000) return format.now;
  if (delta < 3_600_000) {
    const value = Math.floor(delta / 60_000);
    return format.minutes(value);
  }
  if (delta < 86_400_000) {
    const value = Math.floor(delta / 3_600_000);
    return format.hours(value);
  }
  const value = Math.floor(delta / 86_400_000);
  return format.days(value);
}

export function timeGroup(
  timestamp: string | null,
  now = Date.now(),
): "today" | "yesterday" | "earlier" {
  if (timestamp === null) return "earlier";
  const parsed = Date.parse(timestamp);
  if (!Number.isFinite(parsed)) return "earlier";
  const today = new Date(now);
  today.setHours(0, 0, 0, 0);
  const yesterday = today.getTime() - 86_400_000;
  if (parsed >= today.getTime()) return "today";
  if (parsed >= yesterday) return "yesterday";
  return "earlier";
}

import { useCallback, useEffect, useState } from "react";
import { useI18n } from "../i18n";
import { apiBase, useViewer } from "../store";
import {
  fetchTaskFrontDetail,
  type TaskFrontDetail,
} from "../world";

export interface SessionMessage {
  role: string;
  content: string;
  timestamp: string;
  source_engram_id: string | null;
}

export interface SessionHead {
  id: string;
  name: string | null;
  name_origin: string;
  nickname: string | null;
  project_id: string | null;
  status: string;
  total_pulses: number;
}

export interface Session {
  engram: SessionHead;
  messages: SessionMessage[];
}

export function useSession(engramId: string | null): {
  session: Session | null;
  error: string | null;
  loading: boolean;
  reload: () => void;
} {
  const { t } = useI18n();
  const mode = useViewer((s) => s.mode);
  const liveUrl = useViewer((s) => s.liveUrl);
  const [session, setSession] = useState<Session | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [revision, setRevision] = useState(0);
  const reload = useCallback(() => setRevision((value) => value + 1), []);

  useEffect(() => {
    setSession(null);
    setError(null);
    if (engramId === null) return;

    const base = apiBase({ mode, liveUrl });
    let cancelled = false;
    setLoading(true);
    fetch(`${base}/engrams/${encodeURIComponent(engramId)}`)
      .then(async (r) => {
        if (!r.ok) {
          const detail = await r
            .json()
            .then((b) => String(b.detail ?? r.status))
            .catch(() => String(r.status));
          throw new Error(detail);
        }
        return r.json() as Promise<Session>;
      })
      .then((s) => {
        if (!cancelled) setSession(s);
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setError(
            e instanceof TypeError
              ? t("sessions.apiOffline")
              : e instanceof Error
                ? e.message
                : String(e),
          );
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [engramId, mode, liveUrl, revision, t]);

  return { session, error, loading, reload };
}

export function useTaskFront(frontId: string | null): {
  detail: TaskFrontDetail | null;
  error: string | null;
  loading: boolean;
  reload: () => void;
} {
  const { t } = useI18n();
  const mode = useViewer((s) => s.mode);
  const liveUrl = useViewer((s) => s.liveUrl);
  const [detail, setDetail] = useState<TaskFrontDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [revision, setRevision] = useState(0);
  const reload = useCallback(() => setRevision((value) => value + 1), []);

  useEffect(() => {
    setDetail((current) =>
      current?.taskFront.id === frontId ? current : null);
    setError(null);
    if (frontId === null) {
      setLoading(false);
      return;
    }

    const controller = new AbortController();
    const base = apiBase({ mode, liveUrl });
    setLoading(true);
    void fetchTaskFrontDetail(base, frontId, controller.signal)
      .then((next) => setDetail(next))
      .catch((cause: unknown) => {
        if (controller.signal.aborted) return;
        setError(
          cause instanceof TypeError
            ? t("sessions.apiOffline")
            : cause instanceof Error
              ? cause.message
              : String(cause),
        );
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [frontId, liveUrl, mode, revision, t]);

  return { detail, error, loading, reload };
}

/** One message. Injections collapse to a single line by default — the pulse
 *  analog of the codex-style folded tool-call step: the thought's own words
 *  stay open, the traffic that fed it folds away until asked for. */
function MessageRow({
  m,
  onOpenEngram,
}: {
  m: SessionMessage;
  onOpenEngram: (id: string) => void;
}) {
  const { t } = useI18n();
  const [open, setOpen] = useState(m.role !== "injection");
  const foldable = m.role === "injection";
  const roleLabel = m.role === "injection" ? t("sessions.injection") : m.role;

  return (
    <div className={`msg ${m.role}${foldable && !open ? " folded" : ""}`}>
      <div
        className="msg-head"
        onClick={foldable ? () => setOpen(!open) : undefined}
        style={foldable ? { cursor: "pointer" } : undefined}
      >
        {foldable && <span className="msg-fold">{open ? "▾" : "▸"}</span>}
        <span className="msg-role">{roleLabel}</span>
        {m.source_engram_id !== null && (
          <button
            className="msg-source"
            title={t("sessions.from", { id: m.source_engram_id })}
            onClick={(e) => {
              e.stopPropagation();
              onOpenEngram(m.source_engram_id!);
            }}
          >
            {m.source_engram_id}
          </button>
        )}
        {foldable && !open && (
          <span className="msg-preview">{m.content.slice(0, 60)}</span>
        )}
        <span className="msg-ts">{m.timestamp.slice(11, 19)}</span>
      </div>
      {open && <div className="msg-content">{m.content}</div>}
    </div>
  );
}

/** Pure renderer over one useSession result — the hook runs once in the
 *  caller, so drawer and page forms never double-fetch. */
export function SessionMessages({
  session,
  error,
  loading,
  onOpenEngram,
}: {
  session: Session | null;
  error: string | null;
  loading: boolean;
  onOpenEngram: (id: string) => void;
}) {
  const { t } = useI18n();
  return (
    <>
      {loading && <div className="inspector-note">{t("common.loading")}</div>}
      {error !== null && <div className="inspector-note error">{error}</div>}
      {session !== null &&
        session.messages.map((m, i) => (
          <MessageRow key={i} m={m} onOpenEngram={onOpenEngram} />
        ))}
      {session !== null && session.messages.length === 0 && (
        <div className="inspector-note">{t("common.emptySession")}</div>
      )}
    </>
  );
}

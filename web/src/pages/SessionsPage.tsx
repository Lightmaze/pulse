import { useCallback, useEffect, useState } from "react";
import { translate, useI18n, type Locale } from "../i18n";
import { apiBase, useViewer } from "../store";
import { SessionMessages, useSession } from "../components/SessionView";
import { faultText, postInject } from "../pulse";

interface EngramRow {
  id: string;
  name: string | null;
  name_origin: string;
  nickname: string | null;
  project_id: string | null;
  status: string;
  last_pulse_at: string | null;
  total_pulses: number;
  message_count: number;
}

interface ProjectRow {
  id: string;
  name: string;
}

function relative(ts: string | null, locale: Locale): string {
  if (ts === null) return translate(locale, "common.never");
  const ms = Date.now() - Date.parse(ts);
  if (ms < 60_000)
    return translate(locale, "common.secondsAgo", {
      count: Math.max(Math.floor(ms / 1000), 0),
    });
  if (ms < 3_600_000)
    return translate(locale, "common.minutesAgo", { count: Math.floor(ms / 60_000) });
  if (ms < 86_400_000)
    return translate(locale, "common.hoursAgo", { count: Math.floor(ms / 3_600_000) });
  return translate(locale, "common.daysAgo", { count: Math.floor(ms / 86_400_000) });
}

/**
 * The chat-list page: left session list and right conversation pane.
 * User text enters only through the content-stream endpoint; the composer
 * never writes directly to storage or crosses into the tuning sideband.
 */
export function SessionsPage() {
  const { locale, t } = useI18n();
  const mode = useViewer((s) => s.mode);
  const liveUrl = useViewer((s) => s.liveUrl);
  const [engrams, setEngrams] = useState<EngramRow[] | null>(null);
  const [projects, setProjects] = useState<Map<string, string>>(new Map());
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [draft, setDraft] = useState("");
  const [sending, setSending] = useState(false);
  const [sendError, setSendError] = useState<string | null>(null);
  const sessionState = useSession(selected);

  const refresh = useCallback(() => {
    const base = apiBase({ mode, liveUrl });
    setError(null);
    fetch(`${base}/engrams`)
      .then(async (r) => {
        if (!r.ok) {
          const detail = await r
            .json()
            .then((b) => String(b.detail ?? r.status))
            .catch(() => String(r.status));
          throw new Error(detail);
        }
        return r.json() as Promise<{ engrams: EngramRow[] }>;
      })
      .then((body) => {
        const rows = [...body.engrams].sort((a, b) =>
          (b.last_pulse_at ?? "").localeCompare(a.last_pulse_at ?? ""),
        );
        setEngrams(rows);
        setSelected((current) => {
          if (current !== null && rows.some((row) => row.id === current)) {
            return current;
          }
          return rows.find((row) => row.status === "active")?.id ?? rows[0]?.id ?? null;
        });
      })
      .catch((e: unknown) =>
        setError(
          e instanceof TypeError
            ? t("sessions.apiOffline")
            : e instanceof Error
              ? e.message
              : String(e),
        ),
      );
    fetch(`${base}/projects`)
      .then((r) => (r.ok ? r.json() : { projects: [] }))
      .then((body: { projects: ProjectRow[] }) =>
        setProjects(new Map(body.projects.map((p) => [p.id, p.name]))),
      )
      .catch(() => undefined); // grouping degrades to raw ids
  }, [mode, liveUrl, t]);

  useEffect(refresh, [refresh]);

  useEffect(() => {
    if (selected === null) return;
    const timer = window.setInterval(sessionState.reload, 2_000);
    return () => window.clearInterval(timer);
  }, [selected, sessionState.reload]);

  const send = useCallback(async () => {
    const content = draft.trim();
    if (selected === null || content === "" || sending) return;
    setSending(true);
    setSendError(null);
    try {
      const base = apiBase({ mode, liveUrl });
      await postInject(base, selected, content);
      setDraft("");
      sessionState.reload();
      refresh();
    } catch (cause) {
      setSendError(`${t("composer.failed")}: ${faultText(cause)}`);
    } finally {
      setSending(false);
    }
  }, [
    draft,
    liveUrl,
    mode,
    refresh,
    selected,
    sending,
    sessionState.reload,
    t,
  ]);

  return (
    <div className="sessions-page">
      <div className="sessions-list">
        <div className="sessions-list-head">
          <span>{engrams?.length ?? "—"} engrams</span>
          <button onClick={refresh}>{t("sessions.refresh")}</button>
        </div>
        {error !== null && <div className="inspector-note error">{error}</div>}
        {engrams?.map((e) => (
          <div
            key={e.id}
            className={selected === e.id ? "session-row selected" : "session-row"}
            onClick={() => setSelected(e.id)}
          >
            <div className="session-row-top">
              <span className="session-id" title={e.id}>{e.name ?? e.id}</span>
              <span className="session-time">{relative(e.last_pulse_at, locale)}</span>
            </div>
            <div className="session-row-sub">
              {e.project_id !== null && (
                <span className="session-project">
                  {projects.get(e.project_id) ?? e.project_id.slice(0, 8)}
                </span>
              )}
              {e.nickname !== null && <span className="session-project">{e.nickname}</span>}
              <span>
                {t("sessions.pulses", { count: e.total_pulses })} ·{" "}
                {t("sessions.messages", { count: e.message_count })}
                {e.status !== "active" ? ` · ${e.status}` : ""}
              </span>
            </div>
          </div>
        ))}
      </div>
      <div className="sessions-reader">
        {selected !== null && (
          <div className="sessions-reader-head">
            <span className="inspector-title">
              {engrams?.find((engram) => engram.id === selected)?.name ?? selected}
            </span>
            {sessionState.session !== null && (
              <span className="inspector-meta">
                {sessionState.session.engram.status} ·{" "}
                {t("sessions.pulses", {
                  count: sessionState.session.engram.total_pulses,
                })}{" "}
                ·{" "}
                {t("sessions.messages", {
                  count: sessionState.session.messages.length,
                })}
              </span>
            )}
          </div>
        )}
        <div className="sessions-reader-body">
          {selected === null ? (
            <div className="inspector-note">
              {t("sessions.selectHelp")}
            </div>
          ) : (
            <SessionMessages
              session={sessionState.session}
              error={sessionState.error}
              loading={sessionState.loading}
              onOpenEngram={setSelected}
            />
          )}
        </div>
        {selected !== null && (
          <div className="session-composer-wrap">
            <div className="session-composer">
              <textarea
                aria-label={t("composer.placeholder")}
                value={draft}
                rows={3}
                placeholder={t("composer.placeholder")}
                disabled={sending}
                onChange={(event) => setDraft(event.target.value)}
                onKeyDown={(event) => {
                  if (
                    event.key === "Enter" &&
                    !event.shiftKey &&
                    !event.nativeEvent.isComposing
                  ) {
                    event.preventDefault();
                    void send();
                  }
                }}
              />
              <div className="session-composer-foot">
                <span>{sendError ?? t("composer.hint")}</span>
                <button
                  type="button"
                  disabled={sending || draft.trim() === ""}
                  onClick={() => void send()}
                >
                  {sending ? t("composer.sending") : t("composer.send")}
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

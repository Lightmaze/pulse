import { useI18n } from "../i18n";
import { useViewer } from "../store";
import { SessionMessages, useSession } from "./SessionView";

/** The drawer form of the session reader, used from the trace page. */
export function Inspector() {
  const { t } = useI18n();
  const inspected = useViewer((s) => s.inspected);
  const { session, error, loading } = useSession(inspected);

  if (inspected === null) return null;

  return (
    <div className="inspector">
      <div className="inspector-head">
        <span className="inspector-title" title={inspected}>
          {inspected}
        </span>
        {session !== null && (
          <span className="inspector-meta">
            {session.engram.status} · {t("inspector.pulseCount", { count: session.engram.total_pulses })} ·{" "}
            {t("inspector.messageCount", { count: session.messages.length })}
          </span>
        )}
        <button onClick={() => useViewer.getState().inspect(null)}>✕</button>
      </div>
      <div className="inspector-body">
        <SessionMessages
          session={session}
          error={error}
          loading={loading}
          onOpenEngram={(id) => useViewer.getState().inspect(id)}
        />
      </div>
    </div>
  );
}

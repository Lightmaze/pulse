import { useEffect, useMemo, useState, type FormEvent } from "react";
import { useI18n, type Locale } from "../i18n";
import { faultText } from "../pulse";
import {
  MAX_TASK_RELATIONSHIP_CONTENT,
  proposeTaskRelationshipTerms,
  type TaskRelationshipAction,
  type TaskRelationshipActorKind,
  type TaskRelationshipEventView,
  type TaskRelationshipMode,
  type TaskRelationshipStatus,
  type TaskRelationshipView,
} from "../world";
import { Icon } from "./Icons";
import { shortSignature, wcopy, type WorkbenchCopyKey } from "./model";

const STATUS_COPY: Record<
  TaskRelationshipStatus,
  { label: WorkbenchCopyKey; help: WorkbenchCopyKey }
> = {
  active: {
    label: "taskRelationshipActive",
    help: "taskRelationshipActiveHelp",
  },
  paused: {
    label: "taskRelationshipPaused",
    help: "taskRelationshipPausedHelp",
  },
  renegotiation_requested: {
    label: "taskRelationshipRenegotiating",
    help: "taskRelationshipRenegotiatingHelp",
  },
  exited: {
    label: "taskRelationshipExited",
    help: "taskRelationshipExitedHelp",
  },
};

const ACTION_COPY: Record<TaskRelationshipAction, WorkbenchCopyKey> = {
  accepted: "taskRelationshipActionAccepted",
  paused: "taskRelationshipActionPaused",
  renegotiation_requested: "taskRelationshipActionRenegotiationRequested",
  terms_proposed: "taskRelationshipActionTermsProposed",
  resumed: "taskRelationshipActionResumed",
  exited: "taskRelationshipActionExited",
  succession: "taskRelationshipActionSuccession",
};

const ACTOR_COPY: Record<TaskRelationshipActorKind, WorkbenchCopyKey> = {
  subject: "taskRelationshipActorSubject",
  user: "taskRelationshipActorUser",
  system: "taskRelationshipActorSystem",
};

function formatRelationshipTime(timestamp: string, locale: Locale): string {
  const parsed = Date.parse(timestamp);
  if (!Number.isFinite(parsed)) return timestamp;
  return new Intl.DateTimeFormat(locale, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(parsed);
}

function statusText(locale: Locale, status: TaskRelationshipStatus): string {
  return wcopy(locale, STATUS_COPY[status].label);
}

export function TaskRelationshipPanel({
  base,
  frontStatus,
  mode,
  relationship,
  events,
  subjectLabel,
  onReload,
}: {
  base: string;
  frontStatus: string;
  mode: TaskRelationshipMode;
  relationship: TaskRelationshipView | null;
  events: TaskRelationshipEventView[];
  subjectLabel: string;
  onReload: () => void;
}) {
  const { locale } = useI18n();
  const [terms, setTerms] = useState("");
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sent, setSent] = useState(false);

  useEffect(() => {
    setTerms("");
    setSending(false);
    setError(null);
    setSent(false);
  }, [relationship?.id]);

  const latestTerms = useMemo(() => {
    for (let index = events.length - 1; index >= 0; index -= 1) {
      const event = events[index];
      if (event.action === "terms_proposed" && event.content !== null) {
        return event.content;
      }
    }
    return null;
  }, [events]);

  if (mode === "unmanaged_compatibility") {
    return (
      <section
        className="pw-task-relationship is-unmanaged"
        data-task-relationship-mode="unmanaged_compatibility"
      >
        <header className="pw-task-relationship-head">
          <span className="pw-task-relationship-mark"><Icon name="gitBranch" size={16} /></span>
          <div>
            <strong>{wcopy(locale, "taskRelationship")}</strong>
            <span>{wcopy(locale, "taskRelationshipHint")}</span>
          </div>
          <span className="pw-task-relationship-status is-unmanaged">
            {wcopy(locale, "taskRelationshipUnmanaged")}
          </span>
        </header>
        <div className="pw-task-relationship-unmanaged">
          <Icon name="info" size={16} />
          <div>
            <strong>{wcopy(locale, "taskRelationshipUnmanaged")}</strong>
            <span>{wcopy(locale, "taskRelationshipUnmanagedHelp")}</span>
          </div>
        </div>
      </section>
    );
  }

  if (relationship === null) return null;

  const statusCopy = STATUS_COPY[relationship.status];
  const latestSubjectNote = relationship.latest_subject_note?.trim() || null;
  const canProposeTerms = relationship.status !== "exited" && frontStatus !== "archived";
  const changedTerms = terms.trim();

  const submitTerms = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (
      !canProposeTerms ||
      sending ||
      changedTerms === "" ||
      changedTerms.length > MAX_TASK_RELATIONSHIP_CONTENT
    ) return;
    setSending(true);
    setError(null);
    setSent(false);
    try {
      await proposeTaskRelationshipTerms(
        base,
        relationship.id,
        relationship.revision,
        changedTerms,
      );
      setTerms("");
      setSent(true);
      onReload();
    } catch (cause) {
      const detail = faultText(cause);
      setError(detail);
      if (detail.toLowerCase().includes("task_relationship_revision_conflict")) {
        onReload();
      }
    } finally {
      setSending(false);
    }
  };

  return (
    <section
      className={`pw-task-relationship is-${relationship.status}`}
      data-task-relationship-mode={mode}
      data-task-relationship-status={relationship.status}
      data-task-relationship-revision={relationship.revision}
    >
      <header className="pw-task-relationship-head">
        <span className="pw-task-relationship-mark"><Icon name="gitBranch" size={16} /></span>
        <div>
          <strong>{wcopy(locale, "taskRelationship")}</strong>
          <span>{wcopy(locale, "taskRelationshipHint")}</span>
        </div>
        <span className={`pw-task-relationship-status is-${relationship.status}`}>
          {wcopy(locale, statusCopy.label)}
        </span>
      </header>

      <div className="pw-task-relationship-state">
        <div>
          <span>{wcopy(locale, "taskRelationshipRevision")}</span>
          <strong>#{relationship.revision}</strong>
        </div>
        <div>
          <span>{wcopy(locale, "taskRelationshipCurrentSubject")}</span>
          <strong>{subjectLabel}</strong>
          <small>{shortSignature(relationship.current_subject_engram_id)}</small>
        </div>
        {relationship.original_subject_engram_id !== relationship.current_subject_engram_id && (
          <div>
            <span>{wcopy(locale, "taskRelationshipOriginalSubject")}</span>
            <strong>{shortSignature(relationship.original_subject_engram_id)}</strong>
          </div>
        )}
      </div>

      <div className={`pw-task-relationship-meaning is-${relationship.status}`}>
        <Icon name={relationship.status === "active" ? "check" : "info"} size={16} />
        <div>
          <strong>{wcopy(locale, statusCopy.label)}</strong>
          <span>{wcopy(locale, statusCopy.help)}</span>
        </div>
      </div>

      <div className="pw-task-relationship-authority">
        <Icon name="pulse" size={15} />
        <div>
          <strong>{wcopy(locale, "taskRelationshipSubjectAuthority")}</strong>
          <span>{wcopy(locale, "taskRelationshipSubjectAuthorityHelp")}</span>
        </div>
      </div>

      {(latestSubjectNote !== null || latestTerms !== null) && (
        <div className="pw-task-relationship-latest">
          {latestSubjectNote !== null && (
            <article>
              <span>{wcopy(locale, "taskRelationshipLatestSubjectNote")}</span>
              <p>{latestSubjectNote}</p>
            </article>
          )}
          {latestTerms !== null && (
            <article>
              <span>{wcopy(locale, "taskRelationshipLatestTerms")}</span>
              <p>{latestTerms}</p>
            </article>
          )}
        </div>
      )}

      <section className="pw-task-relationship-history">
        <header>
          <div>
            <strong>{wcopy(locale, "taskRelationshipHistory")}</strong>
            <span>{wcopy(locale, "taskRelationshipHistoryHint")}</span>
          </div>
          <small>{events.length}</small>
        </header>
        <ol>
          {events.map((item) => (
            <li key={`${item.relationship_id}:${item.seq}`} data-relationship-action={item.action}>
              <span className="pw-task-relationship-event-axis"><i /></span>
              <div>
                <div className="pw-task-relationship-event-head">
                  <strong>{wcopy(locale, ACTION_COPY[item.action])}</strong>
                  <time>{formatRelationshipTime(item.created_at, locale)}</time>
                </div>
                <div className="pw-task-relationship-event-meta">
                  <span>{wcopy(locale, ACTOR_COPY[item.actor_kind])}</span>
                  <code>{shortSignature(item.actor_id)}</code>
                  <span>
                    {item.before_status === null
                      ? "—"
                      : statusText(locale, item.before_status)}
                    {" → "}{statusText(locale, item.after_status)}
                  </span>
                </div>
                {item.content !== null && item.content.trim() !== "" && (
                  <p>{item.content}</p>
                )}
              </div>
            </li>
          ))}
        </ol>
      </section>

      {canProposeTerms ? (
        <form className="pw-task-relationship-terms" onSubmit={(event) => void submitTerms(event)}>
          <label htmlFor={`task-relationship-terms-${relationship.id}`}>
            <strong>{wcopy(locale, "taskRelationshipTermsTitle")}</strong>
            <span>{wcopy(locale, "taskRelationshipTermsHelp")}</span>
          </label>
          <textarea
            id={`task-relationship-terms-${relationship.id}`}
            rows={3}
            maxLength={MAX_TASK_RELATIONSHIP_CONTENT}
            value={terms}
            disabled={sending}
            placeholder={wcopy(locale, "taskRelationshipTermsPlaceholder")}
            onChange={(event) => {
              setTerms(event.target.value);
              setError(null);
              setSent(false);
            }}
          />
          <div className="pw-task-relationship-terms-foot">
            <span className={error !== null ? "is-error" : sent ? "is-success" : ""}>
              {error ?? (sent
                ? wcopy(locale, "taskRelationshipTermsSent")
                : `${changedTerms.length}/${MAX_TASK_RELATIONSHIP_CONTENT}`)}
            </span>
            <button
              type="submit"
              disabled={sending || changedTerms === ""}
            >
              <Icon name="send" size={14} />
              {wcopy(
                locale,
                sending
                  ? "taskRelationshipTermsSending"
                  : "taskRelationshipTermsSubmit",
              )}
            </button>
          </div>
        </form>
      ) : frontStatus === "archived" && relationship.status !== "exited" ? (
        <div className="pw-task-relationship-terms-unavailable">
          <Icon name="database" size={15} />
          <span>{wcopy(locale, "taskRelationshipTermsArchived")}</span>
        </div>
      ) : null}
    </section>
  );
}

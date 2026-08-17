import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
} from "react";
import { useI18n, type Locale } from "../i18n";
import {
  faultText,
  patchIdentity,
  postInject,
} from "../pulse";
import {
  createTaskOffer,
  createTaskFront,
  fetchLivingPortfolio,
  sendActivityCenterMessage,
  sendTaskFrontMessage,
  updateTaskFrontStatus,
  type TaskFrontSummary,
} from "../world";
import {
  useSession,
  useTaskFront,
  type SessionMessage,
} from "../components/SessionView";
import { HexMark, Icon } from "./Icons";
import { TaskRelationshipPanel } from "./TaskRelationshipPanel";
import {
  displayIdentity,
  displayName,
  shortSignature,
  statusLabel,
  wcopy,
  type EngramSummary,
} from "./model";
import { zhText } from "../locales/zh-ui.ts";

function formatClock(timestamp: string, locale: Locale): string {
  const parsed = Date.parse(timestamp);
  if (!Number.isFinite(parsed)) return timestamp.slice(11, 16);
  return new Intl.DateTimeFormat(locale, {
    hour: "2-digit",
    minute: "2-digit",
  }).format(parsed);
}

export function MessageContent({ content }: { content: string }) {
  const { locale } = useI18n();
  const blocks = useMemo(() => {
    const parts = content.split(/```/);
    return parts.map((part, index) => ({
      kind: index % 2 === 1 ? "code" : "text",
      value: part,
    }));
  }, [content]);

  if (/^\[mock response to:\s*]$/.test(content)) {
    return (
      <div className="pw-message-content pw-message-simulated">
        <p>
          {locale === "zh-CN"
            ? zhText("workbench.ConversationPane.line64")
            : "The mock Harness completed an empty-input pulse; no real model content exists here."}
        </p>
      </div>
    );
  }

  return (
    <div className="pw-message-content">
      {blocks.map((block, index) =>
        block.kind === "code" ? (
          <pre key={index}><code>{block.value.replace(/^[^\n]*\n/, "")}</code></pre>
        ) : (
          <div className="pw-message-prose" key={index}>
            {block.value.split("\n").map((line, lineIndex) => {
              const checklist = line.match(/^\s*[-*]\s+\[([ xX])]\s+(.*)$/);
              if (checklist !== null) {
                return (
                  <div className="pw-check-line" key={lineIndex}>
                    <span className={checklist[1].toLowerCase() === "x" ? "is-done" : ""}>
                      {checklist[1].toLowerCase() === "x" && <Icon name="check" size={12} />}
                    </span>
                    <span>{checklist[2]}</span>
                  </div>
                );
              }
              const bullet = line.match(/^\s*[-*]\s+(.*)$/);
              if (bullet !== null) {
                return (
                  <div className="pw-bullet-line" key={lineIndex}>
                    <span>•</span><span>{bullet[1]}</span>
                  </div>
                );
              }
              return line === "" ? <br key={lineIndex} /> : <p key={lineIndex}>{line}</p>;
            })}
          </div>
        ),
      )}
    </div>
  );
}

function InjectionRow({
  message,
  onOpenEngram,
}: {
  message: SessionMessage;
  onOpenEngram: (id: string) => void;
}) {
  const { locale } = useI18n();
  const [open, setOpen] = useState(false);
  const source = message.source_engram_id;
  return (
    <div className={`pw-injection${open ? " is-open" : ""}`}>
      <button
        className="pw-injection-head"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
      >
        <Icon name="route" size={15} />
        <span>{wcopy(locale, "advisory")}</span>
        {source !== null && (
          <span
            className="pw-injection-source"
            onClick={(event) => {
              event.stopPropagation();
              if (source !== "external") onOpenEngram(source);
            }}
          >
            {wcopy(locale, "injectedFrom")}{" "}
            {source === "external" ? wcopy(locale, "external") : shortSignature(source)}
          </span>
        )}
        <span className="pw-injection-preview">{message.content.slice(0, 84)}</span>
        <Icon name={open ? "chevronDown" : "chevronRight"} size={14} />
      </button>
      {open && <MessageContent content={message.content} />}
    </div>
  );
}

function MessageRow({
  message,
  engram,
  onOpenEngram,
}: {
  message: SessionMessage;
  engram: Pick<EngramSummary, "id" | "name">;
  onOpenEngram: (id: string) => void;
}) {
  const { locale } = useI18n();
  if (message.role === "injection" && message.source_engram_id !== "external") {
    return <InjectionRow message={message} onOpenEngram={onOpenEngram} />;
  }

  const fromUser = message.role === "user" || message.role === "injection";
  const label = fromUser ? wcopy(locale, "you") : displayName(engram);
  return (
    <article className={`pw-message pw-message-${fromUser ? "user" : "engram"}`}>
      <div className="pw-message-avatar">
        {fromUser ? <span>{locale === "zh-CN" ? zhText("workbench.ConversationPane.line165") : "Y"}</span> : <HexMark tone="pulse" size={31} />}
      </div>
      <div className="pw-message-body">
        <div className="pw-message-meta">
          <strong>{label}</strong>
          <time>{formatClock(message.timestamp, locale)}</time>
          {!fromUser && message.role !== "assistant" && (
            <span className="pw-message-role">{message.role}</span>
          )}
        </div>
        <MessageContent content={message.content} />
      </div>
    </article>
  );
}

function IdentityPopover({
  base,
  engram,
  onClose,
  onSaved,
}: {
  base: string;
  engram: EngramSummary;
  onClose: () => void;
  onSaved: () => void;
}) {
  const { locale } = useI18n();
  const [name, setName] = useState(engram.name ?? "");
  const [nickname, setNickname] = useState(engram.nickname ?? "");
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const changed =
    name.trim() !== (engram.name ?? "") || nickname.trim() !== (engram.nickname ?? "");

  const save = async () => {
    if (!changed || name.trim() === "" || sending) return;
    const updates: { name?: string; nickname?: string | null } = {};
    if (name.trim() !== (engram.name ?? "")) updates.name = name.trim();
    if (nickname.trim() !== (engram.nickname ?? "")) {
      updates.nickname = nickname.trim() === "" ? null : nickname.trim();
    }
    setSending(true);
    setError(null);
    try {
      await patchIdentity(base, engram.id, updates);
      onSaved();
      onClose();
    } catch (cause) {
      setError(faultText(cause));
    } finally {
      setSending(false);
    }
  };

  return (
    <div className="pw-identity-popover" role="dialog" aria-label={wcopy(locale, "rename")}>
      <div className="pw-popover-title">
        <span>{wcopy(locale, "rename")}</span>
        <button
          className="pw-icon-button"
          aria-label={wcopy(locale, "close")}
          onClick={onClose}
        >
          <Icon name="x" size={15} />
        </button>
      </div>
      <label>
        <span>{wcopy(locale, "name")}</span>
        <input value={name} maxLength={80} onChange={(event) => setName(event.target.value)} />
      </label>
      <label>
        <span>{wcopy(locale, "nickname")}</span>
        <input
          value={nickname}
          maxLength={80}
          onChange={(event) => setNickname(event.target.value)}
        />
      </label>
      <div className="pw-signature-row">
        <span>{wcopy(locale, "signature")}</span>
        <code>{engram.id}</code>
      </div>
      {error !== null && <div className="pw-inline-error">{error}</div>}
      <div className="pw-popover-actions">
        <button onClick={onClose}>{wcopy(locale, "cancel")}</button>
        <button className="is-primary" disabled={!changed || sending || name.trim() === ""} onClick={() => void save()}>
          {wcopy(locale, "save")}
        </button>
      </div>
    </div>
  );
}

export function ConversationPane({
  base,
  engram,
  subject,
  newTask,
  newTaskSubjectEngramId,
  directoryError,
  onOpenSidebar,
  onOpenRail,
  onSelectEngram,
  onSelectLife,
  onDirectoryRefresh,
  onTaskCreated,
  onTaskOffered,
}: {
  base: string;
  engram: EngramSummary | null;
  subject: {
    kind: "task" | "life" | "engram";
    id: string;
    title: string;
    status: string;
    focalEngramId: string;
  } | null;
  newTask: boolean;
  newTaskSubjectEngramId: string | null;
  directoryError: string | null;
  onOpenSidebar: () => void;
  onOpenRail: () => void;
  onSelectEngram: (id: string) => void;
  onSelectLife: (centerId: string, subjectEngramId?: string) => void;
  onDirectoryRefresh: () => void;
  onTaskCreated: (front: TaskFrontSummary) => void;
  onTaskOffered: () => void;
}) {
  const { locale } = useI18n();
  const [draft, setDraft] = useState("");
  const [sending, setSending] = useState(false);
  const [sendError, setSendError] = useState<string | null>(null);
  const [editingIdentity, setEditingIdentity] = useState(false);
  const [actionsOpen, setActionsOpen] = useState(false);
  const [taskStatusOverride, setTaskStatusOverride] = useState<"open" | "closed" | null>(null);
  const [taskMutation, setTaskMutation] = useState<"open" | "closed" | null>(null);
  const [taskMutationError, setTaskMutationError] = useState<string | null>(null);
  const [returnState, setReturnState] = useState<"idle" | "loading" | "error">("idle");
  const [returnError, setReturnError] = useState<string | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const returnControllerRef = useRef<AbortController | null>(null);
  const taskFrontId = !newTask && subject?.kind === "task" ? subject.id : null;
  const directEngramId = !newTask && subject !== null && subject.kind !== "task"
    ? subject.focalEngramId
    : null;
  const sessionState = useSession(directEngramId);
  const taskState = useTaskFront(taskFrontId);
  const taskDetail = taskState.detail;
  const taskStatus = subject?.kind === "task"
    ? taskStatusOverride ?? taskDetail?.taskFront.status ?? subject.status
    : null;
  const taskRelationshipStatus = taskDetail?.taskRelationship?.status ?? null;
  const visibleEngram = subject?.kind === "task"
    ? taskDetail?.focalEngram ?? engram
    : engram;
  const selectedMessages: SessionMessage[] = subject?.kind === "task"
    ? taskDetail?.messages ?? []
    : sessionState.session?.messages ?? [];
  const selectedContentLoaded = subject?.kind === "task"
    ? taskDetail !== null
    : sessionState.session !== null;
  const selectedLoading = subject?.kind === "task"
    ? taskState.loading
    : sessionState.loading;
  const selectedError = subject?.kind === "task"
    ? taskState.error
    : sessionState.error;
  const selectedReload = subject?.kind === "task"
    ? taskState.reload
    : sessionState.reload;
  const subjectIdentity = visibleEngram === null
    ? null
    : displayIdentity(visibleEngram);
  const isSubjectBoundNewTask = newTask && newTaskSubjectEngramId !== null;

  useEffect(() => {
    if (subject === null || newTask) return;
    const timer = window.setInterval(selectedReload, 2_000);
    return () => window.clearInterval(timer);
  }, [newTask, selectedReload, subject?.id, subject?.kind]);

  useEffect(() => {
    const node = scrollRef.current;
    if (node === null || !selectedContentLoaded) return;
    const nearBottom = node.scrollHeight - node.scrollTop - node.clientHeight < 180;
    if (nearBottom) node.scrollTo({ top: node.scrollHeight, behavior: "smooth" });
  }, [selectedContentLoaded, selectedMessages.length]);

  useEffect(() => {
    setEditingIdentity(false);
    setActionsOpen(false);
    setSendError(null);
    setTaskStatusOverride(null);
    setTaskMutation(null);
    setTaskMutationError(null);
    setReturnState("idle");
    setReturnError(null);
    returnControllerRef.current?.abort();
  }, [newTask, subject?.id, subject?.kind]);

  useEffect(() => {
    if (
      taskStatusOverride !== null &&
      taskDetail?.taskFront.status === taskStatusOverride
    ) {
      setTaskStatusOverride(null);
    }
  }, [taskDetail?.taskFront.status, taskStatusOverride]);

  useEffect(() => () => returnControllerRef.current?.abort(), []);

  const relationshipAllowsTaskMessages = taskDetail !== null && (
    taskDetail.taskRelationshipMode === "unmanaged_compatibility" ||
    taskDetail.taskRelationship?.status === "active"
  );
  const writable = subject !== null && (
    subject.kind === "task"
      ? taskStatus === "open" && taskMutation === null && relationshipAllowsTaskMessages
      : subject.kind === "life"
        ? subject.status === "active" || subject.status === "dormant"
        : subject.status === "active"
  );

  const mutateTaskStatus = useCallback(async (status: "open" | "closed") => {
    if (
      subject?.kind !== "task" ||
      taskDetail === null ||
      taskMutation !== null
    ) return;
    setTaskMutation(status);
    setTaskMutationError(null);
    try {
      const updated = await updateTaskFrontStatus(base, subject.id, status);
      setTaskStatusOverride(updated.status as "open" | "closed");
      taskState.reload();
      onDirectoryRefresh();
    } catch (cause) {
      setTaskMutationError(faultText(cause));
    } finally {
      setTaskMutation(null);
    }
  }, [base, onDirectoryRefresh, subject, taskDetail, taskMutation, taskState.reload]);

  const returnToLife = useCallback(async () => {
    if (taskDetail === null || returnState === "loading") return;
    returnControllerRef.current?.abort();
    const controller = new AbortController();
    returnControllerRef.current = controller;
    setReturnState("loading");
    setReturnError(null);
    try {
      const portfolio = await fetchLivingPortfolio(
        base,
        taskDetail.focalEngram.id,
        controller.signal,
      );
      if (controller.signal.aborted) return;
      const destination = portfolio.items.find((item) =>
        item.portfolio_state === "active" || item.portfolio_state === "quiet");
      if (destination === undefined) {
        setReturnState("error");
        setReturnError(wcopy(locale, "returnToLifeNoCenter"));
        return;
      }
      setReturnState("idle");
      onSelectLife(
        destination.center.id,
        portfolio.subject.current_engram_id,
      );
    } catch (cause) {
      if (controller.signal.aborted) return;
      setReturnState("error");
      setReturnError(faultText(cause));
    }
  }, [base, locale, onSelectLife, returnState, taskDetail]);

  const send = useCallback(async () => {
    const content = draft.trim();
    if (content === "" || sending || (!newTask && !writable)) return;
    const offerSubjectEngramId = newTaskSubjectEngramId ?? undefined;
    setSending(true);
    setSendError(null);
    try {
      if (newTask && offerSubjectEngramId !== undefined) {
        await createTaskOffer(base, offerSubjectEngramId, content);
        onTaskOffered();
      } else if (newTask) {
        const created = await createTaskFront(base, content);
        onTaskCreated(created.taskFront);
      } else if (subject?.kind === "task") {
        await sendTaskFrontMessage(base, subject.id, content);
        taskState.reload();
        onDirectoryRefresh();
      } else if (subject?.kind === "life") {
        await sendActivityCenterMessage(base, subject.id, content);
        sessionState.reload();
        onDirectoryRefresh();
      } else if (subject?.kind === "engram") {
        await postInject(base, subject.focalEngramId, content);
        sessionState.reload();
        onDirectoryRefresh();
      }
      setDraft("");
    } catch (cause) {
      setSendError(faultText(cause));
    } finally {
      setSending(false);
    }
  }, [
    base,
    draft,
    newTask,
    newTaskSubjectEngramId,
    onDirectoryRefresh,
    onTaskCreated,
    onTaskOffered,
    sending,
    sessionState.reload,
    subject,
    taskState.reload,
    writable,
  ]);

  const onComposerKey = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
      event.preventDefault();
      void send();
    }
  };

  const title = newTask
    ? wcopy(locale, isSubjectBoundNewTask ? "subjectBoundTaskTitle" : "newTaskTitle")
    : subject === null
      ? wcopy(locale, "selectSession")
      : subject.kind === "task"
        ? taskDetail?.taskFront.title ?? subject.title
        : subject.title;
  const subjectIcon = newTask || subject?.kind === "task"
    ? "message"
    : subject?.kind === "life"
      ? "spark"
      : "activity";
  const effectiveStatus = subject?.kind === "task"
    ? taskStatus ?? subject.status
    : subject?.status ?? null;
  const showTaskIdentity = subjectIdentity !== null && (
    isSubjectBoundNewTask || (!newTask && subject?.kind === "task")
  );
  const taskRelationshipComposerGate = !newTask && subject?.kind === "task"
    ? taskDetail === null
      ? wcopy(locale, "taskRelationshipLoadingComposer")
      : taskRelationshipStatus === "paused"
        ? wcopy(locale, "taskRelationshipPausedComposer")
        : taskRelationshipStatus === "renegotiation_requested"
          ? wcopy(locale, "taskRelationshipRenegotiatingComposer")
          : taskRelationshipStatus === "exited"
            ? wcopy(locale, "taskRelationshipExitedComposer")
            : null
    : null;
  const composerPlaceholder = !newTask && subject?.kind === "task"
    ? taskStatus === "archived"
      ? wcopy(locale, "taskArchivedPlaceholder")
      : taskRelationshipStatus === "paused"
        ? wcopy(locale, "taskRelationshipPausedPlaceholder")
        : taskRelationshipStatus === "renegotiation_requested"
          ? wcopy(locale, "taskRelationshipRenegotiatingPlaceholder")
          : taskRelationshipStatus === "exited"
            ? wcopy(locale, "taskRelationshipExitedPlaceholder")
            : taskStatus === "closed"
              ? wcopy(locale, "taskClosedPlaceholder")
              : taskDetail === null
                ? wcopy(locale, "taskRelationshipLoadingPlaceholder")
                : wcopy(locale, "sendPlaceholder")
    : newTask
      ? wcopy(
        locale,
        isSubjectBoundNewTask ? "taskOfferPlaceholder" : "newTaskPlaceholder",
      )
      : wcopy(locale, "sendPlaceholder");

  return (
    <section className="pw-conversation">
      <header className="pw-conversation-head">
        <button
          className="pw-icon-button pw-mobile-toggle"
          aria-label={wcopy(locale, "expandSidebar")}
          onClick={onOpenSidebar}
        >
          <Icon name="panelLeft" />
        </button>
        <div className="pw-session-heading">
          <Icon name={subjectIcon} size={17} />
          <div className={`pw-session-heading-copy${showTaskIdentity ? " has-task-subject" : ""}`}>
            <span className="pw-session-title">{title}</span>
            {showTaskIdentity && subjectIdentity !== null && (
              <span className="pw-task-subject-line">
                <span>{wcopy(locale, isSubjectBoundNewTask ? "offerFor" : "taskFor")}</span>
                <strong>{subjectIdentity.primary}</strong>
                {subjectIdentity.secondary !== "" && (
                  <small>{subjectIdentity.secondary}</small>
                )}
              </span>
            )}
            {!newTask && subject?.kind === "engram" && engram?.nickname !== null && engram !== null && (
              <span className="pw-session-nickname">{engram.nickname}</span>
            )}
          </div>
          {engram !== null && !newTask && subject?.kind === "engram" && (
            <button
              className="pw-icon-button pw-rename-trigger"
              aria-label={wcopy(locale, "rename")}
              title={wcopy(locale, "rename")}
              onClick={() => setEditingIdentity((value) => !value)}
            >
              <Icon name="edit" size={15} />
            </button>
          )}
          {editingIdentity && engram !== null && subject?.kind === "engram" && (
            <IdentityPopover
              base={base}
              engram={engram}
              onClose={() => setEditingIdentity(false)}
              onSaved={onDirectoryRefresh}
            />
          )}
        </div>

        <div className="pw-environment">
          <span title={base === "" ? window.location.origin : base}>
            <Icon name="monitor" size={16} />
            {wcopy(locale, "localRuntime")}
          </span>
          {visibleEngram !== null && subject !== null && !newTask && effectiveStatus !== null && (
            <>
              <span>
                <Icon name="activity" size={15} />
                {visibleEngram.total_pulses} {wcopy(locale, "pulses")}
              </span>
              <span className={`pw-session-status pw-status-${effectiveStatus}`}>
                {statusLabel(locale, effectiveStatus)}
              </span>
            </>
          )}
          {engram !== null && !newTask && subject?.kind === "engram" && (
            <div
              className="pw-session-actions-wrap"
              onBlur={(event) => {
                if (!event.currentTarget.contains(event.relatedTarget)) setActionsOpen(false);
              }}
            >
              <button
                className="pw-icon-button"
                aria-expanded={actionsOpen}
                aria-haspopup="menu"
                aria-label={wcopy(locale, "moreActions")}
                onClick={() => setActionsOpen((value) => !value)}
              >
                <Icon name="more" />
              </button>
              {actionsOpen && (
                <div className="pw-session-actions" role="menu">
                  <button
                    role="menuitem"
                    onClick={() => {
                      sessionState.reload();
                      setActionsOpen(false);
                    }}
                  >
                    <Icon name="refresh" size={14} />
                    {wcopy(locale, "refreshSession")}
                  </button>
                  <button
                    role="menuitem"
                    onClick={() => {
                      setEditingIdentity(true);
                      setActionsOpen(false);
                    }}
                  >
                    <Icon name="edit" size={14} />
                    {wcopy(locale, "rename")}
                  </button>
                </div>
              )}
            </div>
          )}
        </div>
        {!newTask && subject?.kind === "task" && (
          <div className="pw-task-front-controls">
            {taskStatus === "open" ? (
              <button
                type="button"
                aria-label={wcopy(locale, "closeTaskFront")}
                title={wcopy(locale, "closeTaskFront")}
                disabled={taskDetail === null || taskMutation !== null}
                onClick={() => void mutateTaskStatus("closed")}
              >
                <Icon name="x" size={13} />
                {taskMutation === "closed"
                  ? wcopy(locale, "closingTaskFront")
                  : wcopy(locale, "closeTaskFront")}
              </button>
            ) : taskStatus === "closed" ? (
              <button
                type="button"
                className="is-primary"
                aria-label={wcopy(locale, "reopenTaskFront")}
                title={wcopy(locale, "reopenTaskFront")}
                disabled={taskDetail === null || taskMutation !== null}
                onClick={() => void mutateTaskStatus("open")}
              >
                <Icon name="refresh" size={13} />
                {taskMutation === "open"
                  ? wcopy(locale, "reopeningTaskFront")
                  : wcopy(locale, "reopenTaskFront")}
              </button>
            ) : (
              <span className="pw-task-archived-label">
                {wcopy(locale, "taskFrontArchived")}
              </span>
            )}
          </div>
        )}
        <button
          className="pw-icon-button pw-mobile-toggle"
          aria-label={wcopy(locale, "expandRail")}
          onClick={onOpenRail}
        >
          <Icon name="panelRight" />
        </button>
      </header>

      <div className="pw-conversation-scroll" ref={scrollRef}>
        <div className="pw-conversation-inner">
          {newTask ? (
            <div className="pw-new-task-state">
              <HexMark tone="blue" size={48} label="+" />
              <h1>{title}</h1>
              <p>
                {wcopy(
                  locale,
                  isSubjectBoundNewTask ? "subjectBoundTaskHelp" : "newTaskHelp",
                )}
              </p>
              {isSubjectBoundNewTask && subjectIdentity !== null && (
                <div className="pw-new-task-subject" data-subject-engram-id={newTaskSubjectEngramId ?? undefined}>
                  <HexMark tone="pulse" size={31} />
                  <div>
                    <span>{wcopy(locale, "continuingSubject")}</span>
                    <strong>{subjectIdentity.primary}</strong>
                    {subjectIdentity.secondary !== "" && (
                      <small>{subjectIdentity.secondary}</small>
                    )}
                  </div>
                  <span>{wcopy(locale, "sameIdentitySession")}</span>
                </div>
              )}
            </div>
          ) : subject === null || (subject.kind !== "task" && visibleEngram === null) ? (
            <div className="pw-empty-conversation">
              <HexMark size={48} />
              <h1>{wcopy(locale, "selectSession")}</h1>
              <p>
                {directoryError ?? wcopy(locale, "selectWorldHint")}
              </p>
            </div>
          ) : (
            <>
              {selectedLoading && !selectedContentLoaded && (
                <div className="pw-conversation-loading">
                  <span /><span /><span />
                </div>
              )}
              {selectedError !== null && (
                <div className="pw-conversation-error">
                  <Icon name="info" />
                  <div>
                    <strong>{wcopy(locale, "runtimeUnavailable")}</strong>
                    <span>{selectedError}</span>
                  </div>
                  <button onClick={selectedReload}>{wcopy(locale, "retry")}</button>
                </div>
              )}
              {subject.kind === "task" && taskDetail !== null && subjectIdentity !== null && (
                <section
                  className="pw-task-continuity"
                  data-message-scope={taskDetail.messageScope}
                >
                  <div className="pw-task-continuity-subject">
                    <HexMark tone="pulse" size={34} />
                    <div>
                      <span>{wcopy(locale, "continuingSubject")}</span>
                      <strong>{subjectIdentity.primary}</strong>
                      {subjectIdentity.secondary !== "" && (
                        <small>{subjectIdentity.secondary}</small>
                      )}
                    </div>
                    <span className="pw-task-session-badge">
                      {wcopy(locale, "sameIdentitySession")}
                    </span>
                  </div>
                  <div className="pw-task-scope-note">
                    <Icon name="globe" size={15} />
                    <div>
                      <strong>{wcopy(locale, "centerScopedTaskHistory")}</strong>
                      <span>{wcopy(locale, "centerScopedTaskHistoryHint")}</span>
                    </div>
                  </div>
                  {(taskStatus === "closed" || taskStatus === "archived") && (
                    <div className={`pw-task-closed-state is-${taskStatus}`}>
                      <Icon name={taskStatus === "closed" ? "check" : "database"} size={17} />
                      <div>
                        <strong>
                          {wcopy(
                            locale,
                            taskStatus === "closed" ? "taskFrontClosed" : "taskFrontArchived",
                          )}
                        </strong>
                        <span>
                          {wcopy(
                            locale,
                            taskStatus === "closed"
                              ? "taskFrontClosedHelp"
                              : "taskFrontArchivedHelp",
                          )}
                        </span>
                      </div>
                      <button
                        type="button"
                        disabled={returnState === "loading"}
                        onClick={() => void returnToLife()}
                      >
                        <Icon name="spark" size={14} />
                        {wcopy(
                          locale,
                          returnState === "loading" ? "returningToLife" : "returnToLife",
                        )}
                      </button>
                    </div>
                  )}
                  {taskMutationError !== null && (
                    <div className="pw-task-control-error" role="alert">
                      <Icon name="info" size={14} />
                      <span>{taskMutationError}</span>
                    </div>
                  )}
                  {returnError !== null && (
                    <div className="pw-task-control-error" role="alert">
                      <Icon name="info" size={14} />
                      <span>{returnError}</span>
                    </div>
                  )}
                </section>
              )}
              {subject.kind === "task" && taskDetail !== null && subjectIdentity !== null && (
                <TaskRelationshipPanel
                  base={base}
                  frontStatus={taskStatus ?? taskDetail.taskFront.status}
                  mode={taskDetail.taskRelationshipMode}
                  relationship={taskDetail.taskRelationship}
                  events={taskDetail.relationshipEvents}
                  subjectLabel={subjectIdentity.primary}
                  onReload={taskState.reload}
                />
              )}
              {visibleEngram !== null && selectedMessages.map((message, index) => (
                <MessageRow
                  key={`${message.timestamp}-${index}`}
                  message={message}
                  engram={visibleEngram}
                  onOpenEngram={onSelectEngram}
                />
              ))}
              {selectedContentLoaded && selectedMessages.length === 0 && (
                <div className="pw-empty-conversation">
                  <HexMark size={42} />
                  <h1>{title}</h1>
                  <p>{wcopy(locale, "noMessages")}</p>
                </div>
              )}
              {subject.kind === "task" &&
                taskDetail !== null &&
                visibleEngram !== null &&
                taskDetail.unattributedHistory.length > 0 && (
                  <details className="pw-task-unattributed">
                    <summary>
                      <span>{wcopy(locale, "unattributedTaskHistory")}</span>
                      <small>{taskDetail.unattributedHistory.length}</small>
                    </summary>
                    <p>{wcopy(locale, "unattributedTaskHistoryHint")}</p>
                    <div className="pw-task-unattributed-messages">
                      {taskDetail.unattributedHistory.map((message) => (
                        <MessageRow
                          key={message.event_id}
                          message={message}
                          engram={visibleEngram}
                          onOpenEngram={onSelectEngram}
                        />
                      ))}
                    </div>
                  </details>
                )}
            </>
          )}
        </div>
      </div>

      <div className="pw-composer-zone">
        {taskRelationshipComposerGate !== null && (
          <div
            className={`pw-task-composer-gate is-${taskRelationshipStatus ?? "loading"}`}
            role="status"
            data-task-composer-disabled-reason={taskRelationshipStatus ?? "loading"}
          >
            <Icon name="info" size={16} />
            <div>
              <strong>{wcopy(locale, "taskRelationship")}</strong>
              <span>{taskRelationshipComposerGate}</span>
            </div>
          </div>
        )}
        <div className={`pw-composer${sendError !== null ? " has-error" : ""}`}>
          <textarea
            rows={2}
            value={draft}
            disabled={sending || (!newTask && !writable)}
            placeholder={composerPlaceholder}
            onChange={(event) => setDraft(event.target.value)}
            onKeyDown={onComposerKey}
          />
          <div className="pw-composer-foot">
            <div className="pw-composer-tools">
              <span>
                {isSubjectBoundNewTask ? (
                  <>
                    <Icon name="pulse" size={15} />
                    {wcopy(locale, "taskOffer")}
                  </>
                ) : newTask ? (
                  <>
                    <Icon name="message" size={15} />
                    {wcopy(locale, "taskFront")}
                  </>
                ) : subject?.kind === "task" ? (
                  <>
                    <Icon name="message" size={15} />
                    {wcopy(locale, "taskFront")}
                  </>
                ) : subject?.kind === "life" ? (
                  <>
                    <Icon name="spark" size={15} />
                    {wcopy(locale, "explicitStimulus")}
                  </>
                ) : (
                  <>
                    <Icon name="pulse" size={15} />
                    {wcopy(locale, "inject")}
                  </>
                )}
              </span>
              <span className="pw-composer-hint">
                {sendError ?? taskRelationshipComposerGate ?? wcopy(locale, "composerHint")}
              </span>
            </div>
            <button
              className="pw-send-button"
              aria-label={sending ? wcopy(locale, "sending") : wcopy(locale, "send")}
              disabled={draft.trim() === "" || sending || (!newTask && !writable)}
              onClick={() => void send()}
            >
              {sending ? <span className="pw-send-spinner" /> : <Icon name="send" size={17} />}
            </button>
          </div>
        </div>
      </div>
    </section>
  );
}

import { useEffect, useMemo, useRef, useState } from "react";
import { useI18n, type Locale } from "../i18n";
import type { ActivityCenterSummary, ActivityKind, TaskFrontSummary } from "../world";
import { Icon, type IconName } from "./Icons";
import {
  relativeTime,
  shortSignature,
  statusLabel,
  wcopy,
  type WorkbenchCopyKey,
  type WorkspacePage,
  type WorkspaceSelection,
} from "./model";

const PAGE_ITEMS: Array<{
  page: WorkspacePage;
  icon: IconName;
  label: WorkbenchCopyKey;
}> = [
  { page: "sessions", icon: "message", label: "world" },
  { page: "trace", icon: "activity", label: "engramObservatory" },
  { page: "models", icon: "database", label: "substrates" },
  { page: "cost", icon: "coins", label: "cost" },
];

const KIND_COPY: Record<Exclude<ActivityKind, "task">, WorkbenchCopyKey> = {
  hobby: "hobby",
  life_project: "lifeProject",
  relationship: "relationship",
  exploration: "exploration",
  practice: "practice",
  expression: "expression",
  rest: "rest",
  other: "other",
};

function kindLabel(locale: Locale, kind: ActivityKind): string {
  return kind === "task" ? wcopy(locale, "taskFront") : wcopy(locale, KIND_COPY[kind]);
}

function FrontRows({
  rows,
  selected,
  locale,
  onSelect,
}: {
  rows: TaskFrontSummary[];
  selected: WorkspaceSelection | null;
  locale: Locale;
  onSelect: (id: string) => void;
}) {
  if (rows.length === 0) {
    return <div className="pw-section-empty">{wcopy(locale, "noTasks")}</div>;
  }
  return (
    <div className="pw-session-group-rows">
      {rows.map((front) => (
        <button
          className={`pw-session-row${
            selected?.kind === "task" && selected.id === front.id ? " is-selected" : ""
          }`}
          key={front.id}
          onClick={() => onSelect(front.id)}
          title={`${front.title} · ${front.id}`}
        >
          <Icon name="message" size={15} />
          <span className="pw-session-row-copy">
            <span className="pw-session-row-title">{front.title}</span>
            {front.status !== "open" && (
              <span className="pw-session-row-nickname">
                {statusLabel(locale, front.status)}
              </span>
            )}
          </span>
          <span className="pw-session-row-time">
            {relativeTime(front.last_opened_at, locale)}
          </span>
        </button>
      ))}
    </div>
  );
}

function LifeRows({
  rows,
  selected,
  locale,
  onSelect,
}: {
  rows: ActivityCenterSummary[];
  selected: WorkspaceSelection | null;
  locale: Locale;
  onSelect: (id: string) => void;
}) {
  if (rows.length === 0) {
    return <div className="pw-section-empty">{wcopy(locale, "noLife")}</div>;
  }
  return (
    <div className="pw-session-group-rows">
      {rows.map((center) => (
        <button
          className={`pw-session-row pw-life-row${
            selected?.kind === "life" && selected.id === center.id ? " is-selected" : ""
          }`}
          key={center.id}
          onClick={() => onSelect(center.id)}
          title={`${center.title} · ${kindLabel(locale, center.kind)}`}
        >
          <Icon name={center.kind === "rest" ? "clock" : "spark"} size={15} />
          <span className="pw-session-row-copy">
            <span className="pw-session-row-title">{center.title}</span>
            <span className="pw-session-row-nickname">
              {kindLabel(locale, center.kind)}
            </span>
          </span>
          <span className={`pw-center-state pw-status-${center.status}`}>
            {statusLabel(locale, center.status)}
          </span>
        </button>
      ))}
    </div>
  );
}

export function WorkspaceSidebar({
  collapsed,
  obscured,
  page,
  worldId,
  taskFronts,
  activityCenters,
  selected,
  directoryState,
  directoryError,
  onToggle,
  onPage,
  onSelectTask,
  onSelectLife,
  onNewTask,
  onNewLife,
  onRefresh,
}: {
  collapsed: boolean;
  obscured: boolean;
  page: WorkspacePage;
  worldId: string | null;
  taskFronts: TaskFrontSummary[];
  activityCenters: ActivityCenterSummary[];
  selected: WorkspaceSelection | null;
  directoryState: "loading" | "ready" | "failed";
  directoryError: string | null;
  onToggle: () => void;
  onPage: (page: WorkspacePage) => void;
  onSelectTask: (id: string) => void;
  onSelectLife: (id: string) => void;
  onNewTask: () => void;
  onNewLife: () => void;
  onRefresh: () => void;
}) {
  const { locale } = useI18n();
  const [query, setQuery] = useState("");
  const searchRef = useRef<HTMLInputElement>(null);
  const navRef = useRef<HTMLElement>(null);
  const shortcutModifier = useMemo(
    () => (/Mac|iPhone|iPad|iPod/.test(navigator.platform) ? "⌘" : "Ctrl"),
    [],
  );

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        onNewTask();
      } else if ((event.metaKey || event.ctrlKey) && event.key === "/") {
        event.preventDefault();
        searchRef.current?.focus();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onNewTask]);

  useEffect(() => {
    navRef.current?.toggleAttribute("inert", obscured);
  }, [obscured]);

  const needle = query.trim().toLocaleLowerCase(locale);
  const visibleFronts = useMemo(
    () => needle === ""
      ? taskFronts
      : taskFronts.filter((front) =>
          [front.title, front.id].some((part) =>
            part.toLocaleLowerCase(locale).includes(needle),
          ),
        ),
    [locale, needle, taskFronts],
  );
  const visibleLife = useMemo(
    () => needle === ""
      ? activityCenters
      : activityCenters.filter((center) =>
          [center.title, center.description, center.kind, center.id].some((part) =>
            part.toLocaleLowerCase(locale).includes(needle),
          ),
        ),
    [activityCenters, locale, needle],
  );

  if (collapsed) {
    return (
      <nav
        ref={navRef}
        className="pw-sidebar pw-sidebar-collapsed"
        aria-label={wcopy(locale, "workspaceNavigation")}
        aria-hidden={obscured || undefined}
      >
        <button
          className="pw-brand-mark"
          aria-label={wcopy(locale, "expandSidebar")}
          onClick={onToggle}
          title={wcopy(locale, "expandSidebar")}
        >
          <span />
        </button>
        <button
          className="pw-icon-button pw-sidebar-new-mini"
          aria-label={wcopy(locale, "newTask")}
          onClick={onNewTask}
          title={wcopy(locale, "newTask")}
        >
          <Icon name="plus" />
        </button>
        <button
          className="pw-icon-button pw-sidebar-life-mini"
          aria-label={wcopy(locale, "newLife")}
          onClick={onNewLife}
          title={wcopy(locale, "newLife")}
        >
          <Icon name="spark" />
        </button>
        <div className="pw-sidebar-mini-pages">
          {PAGE_ITEMS.map((item) => (
            <button
              className={page === item.page ? "is-active" : ""}
              aria-current={page === item.page ? "page" : undefined}
              aria-label={wcopy(locale, item.label)}
              key={item.page}
              onClick={() => onPage(item.page)}
              title={wcopy(locale, item.label)}
            >
              <Icon name={item.icon} />
            </button>
          ))}
        </div>
        <button
          className="pw-icon-button pw-sidebar-expand"
          aria-label={wcopy(locale, "expandSidebar")}
          onClick={onToggle}
        >
          <Icon name="panelLeft" />
        </button>
        <button
          className={`pw-icon-button pw-sidebar-settings-mini${page === "settings" ? " is-active" : ""}`}
          aria-label={wcopy(locale, "settings")}
          aria-current={page === "settings" ? "page" : undefined}
          onClick={() => onPage("settings")}
          title={wcopy(locale, "settings")}
        >
          <Icon name="settings" />
        </button>
      </nav>
    );
  }

  return (
    <nav
      ref={navRef}
      className="pw-sidebar"
      aria-label={wcopy(locale, "workspaceNavigation")}
      aria-hidden={obscured || undefined}
    >
      <div className="pw-sidebar-brand">
        <div className="pw-brand-mark" aria-hidden="true"><span /></div>
        <span className="pw-brand-word">Pulse</span>
        <button
          className="pw-icon-button pw-brand-collapse"
          aria-label={wcopy(locale, "collapseSidebar")}
          onClick={onToggle}
        >
          <Icon name="panelLeft" size={17} />
        </button>
      </div>

      <div className="pw-create-actions">
        <button className="pw-new-session" onClick={onNewTask}>
          <Icon name="plus" size={17} />
          <span>{wcopy(locale, "newTask")}</span>
          <kbd>{shortcutModifier} K</kbd>
        </button>
        <button className="pw-new-life" onClick={onNewLife}>
          <Icon name="spark" size={15} />
          <span>{wcopy(locale, "newLife")}</span>
        </button>
      </div>

      <label className="pw-session-search">
        <Icon name="search" size={16} />
        <input
          ref={searchRef}
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder={wcopy(locale, "searchWorld")}
        />
        {query !== "" ? (
          <button
            type="button"
            aria-label={wcopy(locale, "clearSearch")}
            onClick={() => setQuery("")}
          >
            <Icon name="x" size={14} />
          </button>
        ) : (
          <kbd>{shortcutModifier} /</kbd>
        )}
      </label>

      <div className="pw-project-head" title={worldId ?? undefined}>
        <Icon name="radio" size={17} />
        <span>PulseWorld</span>
        {worldId !== null && <code>{shortSignature(worldId)}</code>}
      </div>

      <div className="pw-session-scroll">
        {directoryState === "loading" && (
          <div className="pw-sidebar-state">{wcopy(locale, "loadingSessions")}</div>
        )}
        {directoryState === "failed" && (
          <div className="pw-sidebar-state is-error">
            <span>{wcopy(locale, "runtimeUnavailable")}</span>
            {directoryError !== null && <small>{directoryError}</small>}
            <button onClick={onRefresh}>
              <Icon name="refresh" size={13} />
              {wcopy(locale, "retry")}
            </button>
          </div>
        )}
        {directoryState === "ready" && (
          <>
            <section className="pw-session-group pw-world-section">
              <div className="pw-session-group-label">
                <span>{wcopy(locale, "tasks")}</span>
                <b>{visibleFronts.length}</b>
              </div>
              <FrontRows
                rows={visibleFronts}
                selected={selected}
                locale={locale}
                onSelect={onSelectTask}
              />
            </section>
            <section className="pw-session-group pw-world-section">
              <div className="pw-session-group-label">
                <span>{wcopy(locale, "life")}</span>
                <b>{visibleLife.length}</b>
              </div>
              <LifeRows
                rows={visibleLife}
                selected={selected}
                locale={locale}
                onSelect={onSelectLife}
              />
            </section>
          </>
        )}
      </div>

      <div className="pw-sidebar-pages">
        {PAGE_ITEMS.map((item) => (
          <button
            className={page === item.page ? "is-active" : ""}
            aria-current={page === item.page ? "page" : undefined}
            key={item.page}
            onClick={() => onPage(item.page)}
          >
            <Icon name={item.icon} size={16} />
            <span>{wcopy(locale, item.label)}</span>
          </button>
        ))}
      </div>

      <div className="pw-sidebar-footer">
        <button
          className={`pw-settings-entry${page === "settings" ? " is-active" : ""}`}
          type="button"
          aria-current={page === "settings" ? "page" : undefined}
          onClick={() => onPage("settings")}
        >
          <Icon name="settings" size={16} />
          <span>{wcopy(locale, "settings")}</span>
          <kbd>{shortcutModifier}+,</kbd>
        </button>
      </div>
    </nav>
  );
}

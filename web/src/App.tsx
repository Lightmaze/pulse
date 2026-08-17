import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { parseJsonl } from "./parse";
import { useViewer } from "./store";
import { useI18n } from "./i18n";
import { Controls } from "./components/Controls";
import { StatusBar } from "./components/StatusBar";
import { TideChart } from "./components/TideChart";
import { Raster } from "./components/Raster";
import { NetworkGraph } from "./components/NetworkGraph";
import { Inspector } from "./components/Inspector";
import { Timeline } from "./components/Timeline";
import { ModelsPage } from "./pages/ModelsPage";
import { CostPage } from "./pages/CostPage";
import { SettingsPage } from "./pages/SettingsPage";
import { REASON_COLORS } from "./types";
import { ConversationPane } from "./workbench/ConversationPane";
import { RuntimeRail } from "./workbench/RuntimeRail";
import { WorkspaceSidebar } from "./workbench/WorkspaceSidebar";
import { LifeCenterDialog } from "./workbench/LifeCenterDialog";
import { LivingCenterPane } from "./workbench/LivingCenterPane";
import { useWorldDirectory } from "./workbench/useEngramDirectory";
import { Icon } from "./workbench/Icons";
import { HarnessSessionPanel } from "./workbench/HarnessSessionPanel";
import { RoleAccountabilityPanel } from "./workbench/RoleAccountabilityPanel";
import { SecurityProfileBadge } from "./components/SecurityProfileBadge";
import {
  wcopy,
  type EngramSummary,
  type WorkspacePage,
  type WorkspaceSelection,
} from "./workbench/model";
import type { ActivityCenterSummary, TaskFrontSummary } from "./world";

type ObservatoryView = "raster" | "network";

const DEFAULT_LIVE = "/events";
const SIDEBAR_KEY = "pulse.workspace.sidebar-collapsed:v2";
const RAIL_KEY = "pulse.workspace.rail-collapsed:v2";
const SELECTED_KEY = "pulse.workspace.selection:v3";

function storedBoolean(key: string): boolean {
  try {
    return window.localStorage.getItem(key) === "1";
  } catch {
    return false;
  }
}

function storedString(key: string): string | null {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function persist(key: string, value: string): void {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // Workspace state remains valid for this tab.
  }
}

function storedSelection(): WorkspaceSelection | null {
  const stored = storedString(SELECTED_KEY);
  if (stored === null) return null;
  if (stored.startsWith("{")) {
    try {
      const parsed: unknown = JSON.parse(stored);
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        return null;
      }
      const row = parsed as Record<string, unknown>;
      if (typeof row.id !== "string" || row.id === "") return null;
      if (row.kind === "task" || row.kind === "engram") {
        return { kind: row.kind, id: row.id };
      }
      if (row.kind !== "life") return null;
      if (row.subjectEngramId === undefined) {
        return { kind: "life", id: row.id };
      }
      if (typeof row.subjectEngramId !== "string" || row.subjectEngramId === "") {
        return null;
      }
      return {
        kind: "life",
        id: row.id,
        subjectEngramId: row.subjectEngramId,
      };
    } catch {
      return null;
    }
  }
  const separator = stored.indexOf(":");
  if (separator < 1) return { kind: "engram", id: stored };
  const kind = stored.slice(0, separator);
  const id = stored.slice(separator + 1);
  if ((kind === "task" || kind === "life" || kind === "engram") && id !== "") {
    return { kind, id };
  }
  return null;
}

function persistSelection(selection: WorkspaceSelection): void {
  persist(SELECTED_KEY, JSON.stringify(selection));
}

function placeholderEngram(id: string, title: string): EngramSummary {
  return {
    id,
    name: title,
    name_origin: "auto",
    nickname: null,
    project_id: null,
    status: "active",
    created_at: null,
    last_pulse_at: null,
    total_pulses: 0,
    message_count: 0,
  };
}

function loadText(text: string, name: string) {
  const state = useViewer.getState();
  try {
    state.loadRun(parseJsonl(text, name));
    state.setPage("trace");
  } catch (cause) {
    state.setError(cause instanceof Error ? cause.message : String(cause));
  }
}

function TracePage() {
  const { t } = useI18n();
  const run = useViewer((state) => state.run);
  const [view, setView] = useState<ObservatoryView>("raster");

  if (run === null) {
    return (
      <div className="empty pw-observatory-empty">
        <Icon name="activity" size={28} />
        <h2>{t("trace.emptyTitle")}</h2>
        <p>{t("trace.replayHelp")}</p>
        <p>
          {t("trace.liveHelp")}{" "}
          <code>pulse --with-claustrum --with-router</code>
        </p>
        <p className="hint">{t("trace.keyHelp")}</p>
      </div>
    );
  }

  return (
    <div className="main">
      <div className="left">
        <TideChart />
        <div className="legend">
          <span className="tabs">
            <button
              className={view === "raster" ? "active" : ""}
              onClick={() => setView("raster")}
            >
              {t("trace.raster")}
            </button>
            <button
              className={view === "network" ? "active" : ""}
              onClick={() => setView("network")}
            >
              {t("trace.network")}
              {run.topology.length > 0 ? ` (${run.topology.length})` : ""}
            </button>
          </span>
          {view === "raster" ? (
            <>
              {Object.entries(REASON_COLORS).map(([reason, color]) => (
                <span key={reason}>
                  <span className="dot" style={{ background: color }} />
                  {reason}
                </span>
              ))}
              <span>
                <span className="dot succession" />
                {t("trace.successionLegend")}
              </span>
            </>
          ) : (
            <>
              <span>{t("trace.nodeLegend")}</span>
              <span>{t("trace.edgeLegend")}</span>
            </>
          )}
        </div>
        {view === "raster" ? <Raster /> : <NetworkGraph />}
      </div>
      <Timeline />
      <Inspector />
    </div>
  );
}

function SecondaryWorkspace({
  page,
  onFile,
}: {
  page: Exclude<WorkspacePage, "sessions" | "settings">;
  onFile: (file: File) => void;
}) {
  const { locale } = useI18n();
  const labels = {
    trace: wcopy(locale, "observatory"),
    models: wcopy(locale, "substrates"),
    cost: wcopy(locale, "cost"),
  };
  return (
    <section className="pw-secondary-workspace">
      <header className="pw-secondary-head">
        <div>
          <Icon
            name={page === "trace" ? "activity" : page === "models" ? "database" : "coins"}
            size={18}
          />
          <span>{labels[page]}</span>
        </div>
        {page === "trace" && <Controls onFile={onFile} />}
      </header>
      {(page === "trace" || page === "cost") && <StatusBar />}
      <div className="pw-secondary-body">
        {page === "trace" && <TracePage />}
        {page === "models" && <ModelsPage />}
        {page === "cost" && <CostPage />}
      </div>
    </section>
  );
}

export default function App() {
  const { locale, t } = useI18n();
  const page = useViewer((state) => state.page) as WorkspacePage;
  const error = useViewer((state) => state.error);
  const directory = useWorldDirectory();
  const [selection, setSelection] = useState<WorkspaceSelection | null>(storedSelection);
  const [newTask, setNewTask] = useState(false);
  const [newTaskSubjectEngramId, setNewTaskSubjectEngramId] = useState<string | null>(null);
  const [newLife, setNewLife] = useState(false);
  const [optimisticTask, setOptimisticTask] = useState<TaskFrontSummary | null>(null);
  const [optimisticCenter, setOptimisticCenter] = useState<ActivityCenterSummary | null>(null);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(
    () =>
      storedBoolean(SIDEBAR_KEY) ||
      window.matchMedia("(max-width: 1040px)").matches,
  );
  const [railCollapsed, setRailCollapsed] = useState(
    () =>
      storedBoolean(RAIL_KEY) ||
      window.matchMedia("(max-width: 790px)").matches,
  );
  const [railOverlaysWorkspace, setRailOverlaysWorkspace] = useState(
    () => window.matchMedia("(max-width: 790px)").matches,
  );
  const mainRef = useRef<HTMLElement>(null);
  const lastWorkspacePage = useRef<Exclude<WorkspacePage, "settings">>(
    page === "settings" ? "sessions" : page,
  );

  useEffect(() => {
    const query = window.matchMedia("(max-width: 790px)");
    const sync = () => setRailOverlaysWorkspace(query.matches);
    sync();
    query.addEventListener("change", sync);
    return () => query.removeEventListener("change", sync);
  }, []);

  const taskFronts = useMemo(() => {
    if (
      optimisticTask === null ||
      directory.taskFronts.some((front) => front.id === optimisticTask.id)
    ) {
      return directory.taskFronts;
    }
    return [optimisticTask, ...directory.taskFronts];
  }, [directory.taskFronts, optimisticTask]);
  const activityCenters = useMemo(() => {
    const life = directory.activityCenters.filter((center) => center.kind !== "task");
    if (
      optimisticCenter === null ||
      life.some((center) => center.id === optimisticCenter.id)
    ) {
      return life;
    }
    return [optimisticCenter, ...life];
  }, [directory.activityCenters, optimisticCenter]);

  const selectedTask = useMemo(
    () => selection?.kind === "task"
      ? taskFronts.find((front) => front.id === selection.id) ?? null
      : null,
    [selection, taskFronts],
  );
  const selectedCenter = useMemo(
    () => selection?.kind === "life"
      ? activityCenters.find((center) => center.id === selection.id) ?? null
      : null,
    [activityCenters, selection],
  );
  const selectedEngramId = selection?.kind === "engram"
    ? selection.id
    : selection?.kind === "life"
      ? selection.subjectEngramId ?? selectedCenter?.focal_engram_id ?? null
      : selectedTask?.focal_engram_id ?? null;
  const selectedCenterId = selectedTask?.center_id ?? selectedCenter?.id ?? null;
  const selectedEngram = useMemo(() => {
    if (selectedEngramId === null) return null;
    return directory.engrams.find((engram) => engram.id === selectedEngramId) ??
      placeholderEngram(
        selectedEngramId,
        selectedEngramId,
      );
  }, [directory.engrams, selectedEngramId]);
  const newTaskEngram = useMemo(() => {
    if (newTaskSubjectEngramId === null) return null;
    return directory.engrams.find(
      (engram) => engram.id === newTaskSubjectEngramId,
    ) ?? placeholderEngram(newTaskSubjectEngramId, newTaskSubjectEngramId);
  }, [directory.engrams, newTaskSubjectEngramId]);
  const conversationEngram = newTask ? newTaskEngram : selectedEngram;
  const workspaceEngramId = newTask ? newTaskSubjectEngramId : selectedEngramId;
  const workspaceCenterId = newTask ? null : selectedCenterId;
  const conversationSubject = useMemo(() => {
    if (selection?.kind === "task" && selectedTask !== null) {
      return {
        kind: "task" as const,
        id: selectedTask.id,
        title: selectedTask.title,
        status: selectedTask.status,
        focalEngramId: selectedTask.focal_engram_id,
      };
    }
    if (selection?.kind === "life" && selectedCenter !== null) {
      return {
        kind: "life" as const,
        id: selectedCenter.id,
        title: selectedCenter.title,
        status: selectedCenter.status,
        focalEngramId: selectedEngramId ?? "",
      };
    }
    if (selection?.kind === "engram" && selectedEngram !== null) {
      return {
        kind: "engram" as const,
        id: selectedEngram.id,
        title: selectedEngram.name ?? selectedEngram.id,
        status: selectedEngram.status,
        focalEngramId: selectedEngram.id,
      };
    }
    return null;
  }, [selectedCenter, selectedEngram, selectedEngramId, selectedTask, selection?.kind]);

  useEffect(() => {
    if (page !== "settings") lastWorkspacePage.current = page;
  }, [page]);

  useEffect(() => {
    const onSettingsKey = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key === ",") {
        event.preventDefault();
        useViewer.getState().setPage("settings");
      } else if (page === "settings" && event.key === "Escape") {
        event.preventDefault();
        useViewer.getState().setPage(lastWorkspacePage.current);
      }
    };
    window.addEventListener("keydown", onSettingsKey);
    return () => window.removeEventListener("keydown", onSettingsKey);
  }, [page]);

  useEffect(() => {
    document.documentElement.lang = locale;
    document.title = page === "settings"
      ? t("document.settingsTitle")
      : t("document.appTitle");
  }, [locale, page, t]);

  useEffect(() => {
    if (newTask || directory.state !== "ready") return;
    const valid = selection === null
      ? false
      : selection.kind === "task"
        ? taskFronts.some((front) => front.id === selection.id)
        : selection.kind === "life"
          ? activityCenters.some((center) => center.id === selection.id)
          : directory.engrams.some((engram) => engram.id === selection.id);
    if (valid) return;
    const front = taskFronts.find((item) => item.status === "open") ?? taskFronts[0];
    const center = activityCenters.find((item) => item.status === "active") ?? activityCenters[0];
    const next: WorkspaceSelection | null = front !== undefined
      ? { kind: "task", id: front.id }
      : center !== undefined
        ? { kind: "life", id: center.id }
        : null;
    setSelection(next);
    if (next !== null) persistSelection(next);
  }, [activityCenters, directory.engrams, directory.state, newTask, selection, taskFronts]);

  useEffect(() => {
    if (
      optimisticTask !== null &&
      directory.taskFronts.some((front) => front.id === optimisticTask.id)
    ) {
      setOptimisticTask(null);
    }
    if (
      optimisticCenter !== null &&
      directory.activityCenters.some((center) => center.id === optimisticCenter.id)
    ) {
      setOptimisticCenter(null);
    }
  }, [directory.activityCenters, directory.taskFronts, optimisticCenter, optimisticTask]);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const live = params.get("live");
    if (live !== null) {
      useViewer.getState().connectLive(live === "" ? DEFAULT_LIVE : live);
      return;
    }
    const src = params.get("src");
    if (src === null) return;
    void fetch(src)
      .then((response) => {
        if (!response.ok) throw new Error(`fetch ${src}: HTTP ${response.status}`);
        return response.text();
      })
      .then((text) => loadText(text, src.split("/").pop() ?? src))
      .catch((cause: unknown) =>
        useViewer
          .getState()
          .setError(cause instanceof Error ? cause.message : String(cause)),
      );
  }, []);

  useEffect(() => {
    const onDragOver = (event: DragEvent) => event.preventDefault();
    const onDrop = (event: DragEvent) => {
      event.preventDefault();
      const file = event.dataTransfer?.files?.[0];
      if (file === undefined) return;
      void file.text().then((text) => loadText(text, file.name));
    };
    window.addEventListener("dragover", onDragOver);
    window.addEventListener("drop", onDrop);
    return () => {
      window.removeEventListener("dragover", onDragOver);
      window.removeEventListener("drop", onDrop);
    };
  }, []);

  useEffect(() => {
    let frame = 0;
    let previous = performance.now();
    const advance = () => {
      const next = performance.now();
      useViewer.getState().tick(next - previous);
      previous = next;
    };
    const loop = () => {
      advance();
      frame = requestAnimationFrame(loop);
    };
    frame = requestAnimationFrame(loop);
    const interval = window.setInterval(advance, 250);
    const onKey = (event: globalThis.KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (target !== null && ["INPUT", "SELECT", "TEXTAREA"].includes(target.tagName)) return;
      const state = useViewer.getState();
      if (event.code === "Space") {
        event.preventDefault();
        state.setPlaying(!state.playing);
      } else if (event.code === "Home" && state.run !== null) {
        state.seek(state.run.tMinMs);
      } else if (event.code === "End" && state.run !== null) {
        state.seek(state.run.tMaxMs);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => {
      cancelAnimationFrame(frame);
      window.clearInterval(interval);
      window.removeEventListener("keydown", onKey);
    };
  }, []);

  const selectEngram = useCallback((id: string) => {
    const next: WorkspaceSelection = { kind: "engram", id };
    setSelection(next);
    setNewTask(false);
    setNewTaskSubjectEngramId(null);
    persistSelection(next);
    useViewer.getState().setPage("sessions");
  }, []);

  const selectTask = useCallback((id: string) => {
    const next: WorkspaceSelection = { kind: "task", id };
    setSelection(next);
    setNewTask(false);
    setNewTaskSubjectEngramId(null);
    persistSelection(next);
    useViewer.getState().setPage("sessions");
  }, []);

  const selectLife = useCallback((id: string, subjectEngramId?: string) => {
    const next: WorkspaceSelection = subjectEngramId === undefined
      ? { kind: "life", id }
      : { kind: "life", id, subjectEngramId };
    setSelection(next);
    setNewTask(false);
    setNewTaskSubjectEngramId(null);
    persistSelection(next);
    useViewer.getState().setPage("sessions");
  }, []);

  const startNewSession = useCallback(() => {
    setNewTaskSubjectEngramId(null);
    setNewTask(true);
    setNewLife(false);
    useViewer.getState().setPage("sessions");
  }, []);

  const startTaskForSubject = useCallback((subjectEngramId: string) => {
    setNewTaskSubjectEngramId(subjectEngramId);
    setNewTask(true);
    setNewLife(false);
    useViewer.getState().setPage("sessions");
  }, []);

  const toggleSidebar = () => {
    setSidebarCollapsed((current) => {
      const next = !current;
      persist(SIDEBAR_KEY, next ? "1" : "0");
      return next;
    });
  };

  const toggleRail = () => {
    setRailCollapsed((current) => {
      const next = !current;
      persist(RAIL_KEY, next ? "1" : "0");
      return next;
    });
  };

  const railOwnsMobileFocus = page !== "settings" && railOverlaysWorkspace && !railCollapsed;

  useEffect(() => {
    mainRef.current?.toggleAttribute("inert", railOwnsMobileFocus);
  }, [railOwnsMobileFocus]);

  return (
    <div
      className={`pulse-workbench${sidebarCollapsed ? " is-sidebar-collapsed" : ""}${
        railCollapsed ? " is-rail-collapsed" : ""
      }${page === "settings" ? " is-settings-page" : ""}`}
    >
      {page !== "settings" && <SecurityProfileBadge base={directory.base} />}
      <WorkspaceSidebar
        collapsed={sidebarCollapsed}
        obscured={railOwnsMobileFocus}
        page={page}
        worldId={directory.worldId}
        taskFronts={taskFronts}
        activityCenters={activityCenters}
        selected={selection}
        directoryState={directory.state}
        directoryError={directory.error}
        onToggle={toggleSidebar}
        onPage={(next) => useViewer.getState().setPage(next)}
        onSelectTask={selectTask}
        onSelectLife={selectLife}
        onNewTask={startNewSession}
        onNewLife={() => setNewLife(true)}
        onRefresh={directory.refresh}
      />

      <main
        ref={mainRef}
        className="pw-main"
        aria-hidden={railOwnsMobileFocus || undefined}
      >
        {page !== "settings" && error !== null && (
          <div className="pw-global-error">
            <Icon name="info" size={15} />
            <span>{error}</span>
            <button
              aria-label={wcopy(locale, "close")}
              onClick={() => useViewer.getState().setError(null)}
            >
              <Icon name="x" size={14} />
            </button>
          </div>
        )}
        {page === "settings" ? (
          <SettingsPage
            onClose={() => useViewer.getState().setPage(lastWorkspacePage.current)}
          />
        ) : page === "sessions" ? (
          <div className="pw-session-stack">
            {!newTask && selection?.kind === "life" && selectedCenter !== null ? (
              <LivingCenterPane
                base={directory.base}
                center={selectedCenter}
                subjectEngramId={selectedEngramId}
                onOpenSidebar={() => setSidebarCollapsed(false)}
                onOpenRail={() => setRailCollapsed(false)}
                onDirectoryRefresh={directory.refresh}
                onSelectLife={selectLife}
                onNewTaskForSubject={startTaskForSubject}
                onSelectTask={selectTask}
              />
            ) : (
              <ConversationPane
                base={directory.base}
                engram={conversationEngram}
                subject={conversationSubject}
                newTask={newTask}
                newTaskSubjectEngramId={newTaskSubjectEngramId}
                directoryError={directory.error}
                onOpenSidebar={() => setSidebarCollapsed(false)}
                onOpenRail={() => setRailCollapsed(false)}
                onSelectEngram={selectEngram}
                onSelectLife={selectLife}
                onDirectoryRefresh={directory.refresh}
                onTaskCreated={(front) => {
                  setOptimisticTask(front);
                  const next: WorkspaceSelection = { kind: "task", id: front.id };
                  setSelection(next);
                  persistSelection(next);
                  setNewTask(false);
                  setNewTaskSubjectEngramId(null);
                  directory.refresh();
                }}
                onTaskOffered={() => {
                  setNewTask(false);
                  setNewTaskSubjectEngramId(null);
                  directory.refresh();
                }}
              />
            )}
            <RoleAccountabilityPanel
              base={directory.base}
              engramId={workspaceEngramId}
            />
            <HarnessSessionPanel
              base={directory.base}
              engramId={workspaceEngramId}
              harnessKind={directory.harnessKind}
            />
          </div>
        ) : (
          <SecondaryWorkspace
            page={page}
            onFile={(file) => void file.text().then((text) => loadText(text, file.name))}
          />
        )}
      </main>

      {page !== "settings" && (
        <RuntimeRail
          collapsed={railCollapsed}
          harnessKind={directory.harnessKind}
          selectedEngram={workspaceEngramId}
          selectedCenter={workspaceCenterId}
          worldId={directory.worldId}
          onToggle={toggleRail}
          onSelectEngram={selectEngram}
        />
      )}

      {newLife && (
        <LifeCenterDialog
          base={directory.base}
          onClose={() => setNewLife(false)}
          onCreated={(center) => {
            setOptimisticCenter(center);
            const next: WorkspaceSelection = { kind: "life", id: center.id };
            setSelection(next);
            persistSelection(next);
            setNewLife(false);
            setNewTask(false);
            setNewTaskSubjectEngramId(null);
            useViewer.getState().setPage("sessions");
            directory.refresh();
          }}
        />
      )}
    </div>
  );
}

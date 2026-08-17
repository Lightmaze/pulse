import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRuntimeBase } from "../pulse";
import {
  fetchPulseWorld,
  type ActivityCenterSummary,
  type TaskFrontSummary,
} from "../world";
import type { EngramSummary, ProjectSummary } from "./model";

type DirectoryState = "loading" | "ready" | "failed";

function record(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : {};
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim() !== "" ? value : null;
}

function numeric(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function parseEngrams(body: unknown): EngramSummary[] {
  const root = record(body);
  const source = Array.isArray(body)
    ? body
    : Array.isArray(root.engrams)
      ? root.engrams
      : [];
  const rows: EngramSummary[] = [];
  for (const item of source) {
    const row = record(item);
    const id = text(row.id) ?? text(row.engram_id);
    if (id === null) continue;
    rows.push({
      id,
      name: text(row.name) ?? text(row.title),
      name_origin: text(row.name_origin) ?? "auto",
      nickname: text(row.nickname),
      project_id: text(row.project_id),
      status: text(row.status) ?? "active",
      created_at: text(row.created_at),
      last_pulse_at: text(row.last_pulse_at) ?? text(row.last_fired_at),
      total_pulses: numeric(row.total_pulses ?? row.pulse_count),
      message_count: numeric(row.message_count),
    });
  }
  return rows.sort((a, b) => {
    const byRecent = (b.last_pulse_at ?? "").localeCompare(a.last_pulse_at ?? "");
    if (byRecent !== 0) return byRecent;
    return (b.created_at ?? "").localeCompare(a.created_at ?? "");
  });
}

function parseProjects(body: unknown): ProjectSummary[] {
  const root = record(body);
  const source = Array.isArray(body)
    ? body
    : Array.isArray(root.projects)
      ? root.projects
      : [];
  const rows: ProjectSummary[] = [];
  for (const item of source) {
    const row = record(item);
    const id = text(row.id) ?? text(row.project_id);
    if (id === null) continue;
    rows.push({
      id,
      name: text(row.name) ?? id,
      description: text(row.description),
      engram_count: numeric(row.engram_count),
    });
  }
  return rows;
}

async function fetchJson(url: string, signal: AbortSignal): Promise<unknown> {
  const response = await fetch(url, { signal });
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    const detail = record(body).detail;
    throw new Error(typeof detail === "string" ? detail : `HTTP ${response.status}`);
  }
  return response.json();
}

export function useWorldDirectory(): {
  base: string;
  worldId: string | null;
  continuityEngramId: string | null;
  harnessKind: string | null;
  mock: boolean;
  taskFronts: TaskFrontSummary[];
  activityCenters: ActivityCenterSummary[];
  engrams: EngramSummary[];
  projects: ProjectSummary[];
  projectNames: Map<string, string>;
  state: DirectoryState;
  error: string | null;
  refresh: () => void;
} {
  const base = useRuntimeBase();
  const [worldId, setWorldId] = useState<string | null>(null);
  const [continuityEngramId, setContinuityEngramId] = useState<string | null>(null);
  const [harnessKind, setHarnessKind] = useState<string | null>(null);
  const [mock, setMock] = useState(false);
  const [taskFronts, setTaskFronts] = useState<TaskFrontSummary[]>([]);
  const [activityCenters, setActivityCenters] = useState<ActivityCenterSummary[]>([]);
  const [engrams, setEngrams] = useState<EngramSummary[]>([]);
  const [projects, setProjects] = useState<ProjectSummary[]>([]);
  const [state, setState] = useState<DirectoryState>("loading");
  const [error, setError] = useState<string | null>(null);
  const [revision, setRevision] = useState(0);
  const hasLoaded = useRef(false);

  const refresh = useCallback(() => setRevision((value) => value + 1), []);

  useEffect(() => {
    const controller = new AbortController();
    if (!hasLoaded.current) setState("loading");
    setError(null);

    void Promise.all([
      fetchPulseWorld(base, controller.signal),
      fetchJson(`${base}/engrams`, controller.signal),
      fetchJson(`${base}/projects`, controller.signal).catch(() => ({ projects: [] })),
    ])
      .then(([world, engramBody, projectBody]) => {
        setWorldId(world.worldId);
        setContinuityEngramId(world.continuityEngramId);
        setHarnessKind(world.harnessKind);
        setMock(world.mock);
        setTaskFronts(world.taskFronts);
        setActivityCenters(world.activityCenters);
        setEngrams(parseEngrams(engramBody));
        setProjects(parseProjects(projectBody));
        setState("ready");
        hasLoaded.current = true;
      })
      .catch((cause: unknown) => {
        if (controller.signal.aborted) return;
        setState("failed");
        setError(cause instanceof Error ? cause.message : String(cause));
      });

    return () => controller.abort();
  }, [base, revision]);

  useEffect(() => {
    const timer = window.setInterval(refresh, 5_000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  const projectNames = useMemo(
    () => new Map(projects.map((project) => [project.id, project.name])),
    [projects],
  );

  return {
    base,
    worldId,
    continuityEngramId,
    harnessKind,
    mock,
    taskFronts,
    activityCenters,
    engrams,
    projects,
    projectNames,
    state,
    error,
    refresh,
  };
}

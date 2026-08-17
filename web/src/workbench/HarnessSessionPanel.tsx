import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useI18n } from "../i18n";
import {
  fetchHarnessTurns,
  HarnessFault,
  type HarnessTurnCatalogEntry,
  type HarnessTurnCatalogPage,
  type HarnessTurnCatalogRequest,
} from "../harness";
import { Icon } from "./Icons";
import { HarnessTurnPanel } from "./HarnessTurnPanel";
import "./HarnessSessionPanel.css";
import { zhText } from "../locales/zh-ui.ts";

const CATALOG_PAGE_SIZE = 12;
const CATALOG_REFRESH_MS = 2_000;

const TERMINAL_STATES = new Set([
  "settled",
  "completed",
  "complete",
  "interrupted",
  "cancelled",
  "canceled",
  "reconciled",
  "stopped",
]);

const ERROR_STATES = new Set([
  "failed",
  "error",
  "uncertain",
  "rejected",
]);

type FetchTurns = (
  base: string,
  engramId: string,
  request?: HarnessTurnCatalogRequest,
  signal?: AbortSignal,
) => Promise<HarnessTurnCatalogPage>;

export interface HarnessSessionPanelProps {
  base: string;
  engramId: string | null;
  harnessKind?: string | null;
  /** Optional transport seam for a host-side contract test. */
  fetchTurns?: FetchTurns;
}

interface CatalogFaultView {
  code: string | null;
  message: string;
  remedy: string | null;
}

interface CatalogState {
  ownerBase: string;
  ownerEngramId: string | null;
  generation: number;
  turns: HarnessTurnCatalogEntry[];
  selectedTurnId: string | null;
  followLatest: boolean;
  pinLatestId: string | null;
  unseenTurnIds: string[];
  unseenOverflow: boolean;
  hasMore: boolean;
  nextCursor: string | null;
  olderPagesLoaded: boolean;
  initialized: boolean;
  refreshing: boolean;
  loadingOlder: boolean;
  catalogFault: CatalogFaultView | null;
  olderFault: CatalogFaultView | null;
  notice: "cursor_reset" | null;
}

type TurnTone = "running" | "terminal" | "error" | "unknown";

const COPY = {
  en: {
    ariaLabel: "Harness turn session",
    catalogLabel: "Harness turn navigator",
    title: "Harness turns",
    noEngram: "Choose a Task Front or Engram to inspect its execution turns.",
    loading: "Loading turn catalog…",
    retrying: "Checking the catalog again…",
    emptyTitle: "No Harness turns yet",
    emptyBody: "New durable turns will appear here when this Engram starts executing.",
    unavailableTitle: "Turn catalog unavailable",
    unavailableBody: "No catalog data is available yet. Automatic polling will keep trying.",
    following: "Following latest",
    followingShort: "following",
    followingBody: "The selected turn advances when a newer turn appears.",
    pinned: "History pinned",
    pinnedShort: "pinned",
    pinnedBody: "This historical turn stays selected while the catalog continues to refresh.",
    degraded: "Degraded",
    degradedBody: "Showing the last loaded catalog while refresh is unavailable.",
    empty: "Empty",
    noSelection: "No turn selected",
    refresh: "Refresh turn catalog",
    followLatest: "Follow latest",
    latest: "Latest",
    history: "History",
    selectTurn: "Select Harness turn",
    timestampMissing: "Time unavailable",
    loadOlder: "Load earlier turns",
    loadingOlder: "Loading earlier…",
    noMore: "All earlier turns loaded",
    cursorUnavailable: "The server reported earlier turns without a usable cursor.",
    cursorReset: "The earlier-turn cursor expired. The latest page was reloaded; the selected turn remained pinned.",
    faultTitle: "Catalog request failed",
    retry: "Retry now",
    running: "running",
    terminal: "terminal",
    error: "error",
    unknown: "unknown",
    simulatedTitle: "Simulated Harness evidence",
    simulatedEvidence: "CONTRACT_ONLY · SIMULATED",
    simulatedBody:
      "This durable causal turn came from the test Harness. It does not emit canonical execution-plane replay, so no live tool trace or control claim is made here.",
    count: (value: number) => `${value} turn${value === 1 ? "" : "s"} loaded`,
    newTurns: (value: number, atLeast: boolean) =>
      `${value}${atLeast ? "+" : ""} new turn${value === 1 && !atLeast ? "" : "s"} arrived while history was pinned.`,
  },
  zh: {
    ariaLabel: zhText("workbench.HarnessSessionPanel.line127"),
    catalogLabel: zhText("workbench.HarnessSessionPanel.line128"),
    title: "Harness turns",
    noEngram: zhText("workbench.HarnessSessionPanel.line130"),
    loading: zhText("workbench.HarnessSessionPanel.line131"),
    retrying: zhText("workbench.HarnessSessionPanel.line132"),
    emptyTitle: zhText("workbench.HarnessSessionPanel.line133"),
    emptyBody: zhText("workbench.HarnessSessionPanel.line134"),
    unavailableTitle: zhText("workbench.HarnessSessionPanel.line135"),
    unavailableBody: zhText("workbench.HarnessSessionPanel.line136"),
    following: zhText("workbench.HarnessSessionPanel.line137"),
    followingShort: zhText("workbench.HarnessSessionPanel.line138"),
    followingBody: zhText("workbench.HarnessSessionPanel.line139"),
    pinned: zhText("workbench.HarnessSessionPanel.line140"),
    pinnedShort: zhText("workbench.HarnessSessionPanel.line141"),
    pinnedBody: zhText("workbench.HarnessSessionPanel.line142"),
    degraded: zhText("workbench.HarnessSessionPanel.line143"),
    degradedBody: zhText("workbench.HarnessSessionPanel.line144"),
    empty: zhText("workbench.HarnessSessionPanel.line145"),
    noSelection: zhText("workbench.HarnessSessionPanel.line146"),
    refresh: zhText("workbench.HarnessSessionPanel.line147"),
    followLatest: zhText("workbench.HarnessSessionPanel.line148"),
    latest: zhText("workbench.HarnessSessionPanel.line149"),
    history: zhText("workbench.HarnessSessionPanel.line150"),
    selectTurn: zhText("workbench.HarnessSessionPanel.line151"),
    timestampMissing: zhText("workbench.HarnessSessionPanel.line152"),
    loadOlder: zhText("workbench.HarnessSessionPanel.line153"),
    loadingOlder: zhText("workbench.HarnessSessionPanel.line154"),
    noMore: zhText("workbench.HarnessSessionPanel.line155"),
    cursorUnavailable: zhText("workbench.HarnessSessionPanel.line156"),
    cursorReset: zhText("workbench.HarnessSessionPanel.line157"),
    faultTitle: zhText("workbench.HarnessSessionPanel.line158"),
    retry: zhText("workbench.HarnessSessionPanel.line159"),
    running: zhText("workbench.HarnessSessionPanel.line160"),
    terminal: zhText("workbench.HarnessSessionPanel.line161"),
    error: zhText("workbench.HarnessSessionPanel.line162"),
    unknown: zhText("workbench.HarnessSessionPanel.line163"),
    simulatedTitle: zhText("workbench.HarnessSessionPanel.line164"),
    simulatedEvidence: "CONTRACT_ONLY · SIMULATED",
    simulatedBody:
      zhText("workbench.HarnessSessionPanel.line167"),
    count: (value: number) => (zhText("workbench.HarnessSessionPanel.line168.head") + String(value) + zhText("workbench.HarnessSessionPanel.line168.tail1")),
    newTurns: (value: number, atLeast: boolean) =>
      (zhText("workbench.HarnessSessionPanel.line170.head") + String(value) + "" + String(atLeast ? "+" : "") + zhText("workbench.HarnessSessionPanel.line170.tail2")),
  },
} as const;

function createCatalogState(
  base: string,
  engramId: string | null,
  generation: number,
): CatalogState {
  const hasEngram = engramId !== null;
  return {
    ownerBase: base,
    ownerEngramId: engramId,
    generation,
    turns: [],
    selectedTurnId: null,
    followLatest: true,
    pinLatestId: null,
    unseenTurnIds: [],
    unseenOverflow: false,
    hasMore: false,
    nextCursor: null,
    olderPagesLoaded: false,
    initialized: !hasEngram,
    refreshing: hasEngram,
    loadingOlder: false,
    catalogFault: null,
    olderFault: null,
    notice: null,
  };
}

function ownsRequest(
  state: CatalogState,
  base: string,
  engramId: string,
  generation: number,
): boolean {
  return state.ownerBase === base
    && state.ownerEngramId === engramId
    && state.generation === generation;
}

function uniqueTurns(turns: HarnessTurnCatalogEntry[]): HarnessTurnCatalogEntry[] {
  const seen = new Set<string>();
  const result: HarnessTurnCatalogEntry[] = [];
  for (const turn of turns) {
    if (seen.has(turn.id)) continue;
    seen.add(turn.id);
    result.push(turn);
  }
  return result;
}

function mergeCatalogHead(
  head: HarnessTurnCatalogEntry[],
  current: HarnessTurnCatalogEntry[],
): HarnessTurnCatalogEntry[] {
  const headIds = new Set(head.map((turn) => turn.id));
  return [...head, ...current.filter((turn) => !headIds.has(turn.id))];
}

function appendOlderTurns(
  current: HarnessTurnCatalogEntry[],
  older: HarnessTurnCatalogEntry[],
): HarnessTurnCatalogEntry[] {
  const seen = new Set(current.map((turn) => turn.id));
  const result = [...current];
  for (const turn of older) {
    if (seen.has(turn.id)) continue;
    seen.add(turn.id);
    result.push(turn);
  }
  return result;
}

function unseenAfterHead(
  current: CatalogState,
  head: HarnessTurnCatalogEntry[],
): Pick<CatalogState, "unseenTurnIds" | "unseenOverflow"> {
  if (current.followLatest) {
    return { unseenTurnIds: [], unseenOverflow: false };
  }
  const knownIds = new Set(current.turns.map((turn) => turn.id));
  const unseenIds = new Set(current.unseenTurnIds);
  for (const turn of head) {
    if (!knownIds.has(turn.id)) unseenIds.add(turn.id);
  }
  const baselineMissing = current.pinLatestId !== null
    && head.length > 0
    && !head.some((turn) => turn.id === current.pinLatestId);
  return {
    unseenTurnIds: [...unseenIds],
    unseenOverflow: current.unseenOverflow || baselineMissing,
  };
}

function applyHeadPage(
  current: CatalogState,
  page: HarnessTurnCatalogPage,
): CatalogState {
  const head = uniqueTurns(page.turns);
  const turns = mergeCatalogHead(head, current.turns);
  const latestId = head.at(0)?.id ?? turns.at(0)?.id ?? null;
  const unseen = unseenAfterHead(current, head);
  const adoptHeadCursor = !current.initialized || !current.olderPagesLoaded;
  return {
    ...current,
    turns,
    selectedTurnId: current.followLatest
      ? latestId
      : current.selectedTurnId ?? latestId,
    pinLatestId: current.followLatest ? null : current.pinLatestId,
    unseenTurnIds: unseen.unseenTurnIds,
    unseenOverflow: unseen.unseenOverflow,
    hasMore: adoptHeadCursor ? page.has_more : current.hasMore,
    nextCursor: adoptHeadCursor ? page.next_cursor : current.nextCursor,
    initialized: true,
    refreshing: false,
    catalogFault: null,
  };
}

function applyOlderPage(
  current: CatalogState,
  page: HarnessTurnCatalogPage,
): CatalogState {
  return {
    ...current,
    turns: appendOlderTurns(current.turns, uniqueTurns(page.turns)),
    hasMore: page.has_more,
    nextCursor: page.next_cursor,
    olderPagesLoaded: true,
    loadingOlder: false,
    olderFault: null,
    notice: null,
  };
}

function applyCursorReset(
  current: CatalogState,
  page: HarnessTurnCatalogPage,
): CatalogState {
  const head = uniqueTurns(page.turns);
  const headIds = new Set(head.map((turn) => turn.id));
  const selectedEntry = current.selectedTurnId === null
    ? null
    : current.turns.find((turn) => turn.id === current.selectedTurnId) ?? null;
  const turns = selectedEntry !== null && !headIds.has(selectedEntry.id)
    ? [...head, selectedEntry]
    : head;
  const latestId = head.at(0)?.id ?? current.selectedTurnId;
  const unseen = unseenAfterHead(current, head);
  return {
    ...current,
    turns,
    selectedTurnId: current.followLatest
      ? latestId
      : current.selectedTurnId ?? latestId,
    pinLatestId: current.followLatest ? null : current.pinLatestId,
    unseenTurnIds: unseen.unseenTurnIds,
    unseenOverflow: unseen.unseenOverflow,
    hasMore: page.has_more,
    nextCursor: page.next_cursor,
    olderPagesLoaded: false,
    initialized: true,
    refreshing: false,
    loadingOlder: false,
    catalogFault: null,
    olderFault: null,
    notice: "cursor_reset",
  };
}

function faultView(cause: unknown): CatalogFaultView {
  if (cause instanceof HarnessFault) {
    return {
      code: cause.code.slice(0, 96),
      message: cause.message.slice(0, 320),
      remedy: cause.remedy?.slice(0, 320) ?? null,
    };
  }
  if (cause instanceof Error) {
    return {
      code: null,
      message: cause.message.slice(0, 320),
      remedy: null,
    };
  }
  return {
    code: null,
    message: typeof cause === "string"
      ? cause.slice(0, 320)
      : "Harness catalog request failed.",
    remedy: null,
  };
}

function isAbort(cause: unknown): boolean {
  return cause instanceof Error && cause.name === "AbortError";
}

function shortId(value: string): string {
  if (value.length <= 22) return value;
  return `${value.slice(0, 10)}…${value.slice(-7)}`;
}

function turnTimestamp(turn: HarnessTurnCatalogEntry): string | null {
  return turn.settled_at ?? turn.updated_at ?? turn.started_at;
}

function turnTone(turn: HarnessTurnCatalogEntry): TurnTone {
  const state = turn.state.toLowerCase();
  if (turn.error_code !== null || ERROR_STATES.has(state)) return "error";
  if (TERMINAL_STATES.has(state)) return "terminal";
  if (state === "unknown" || state === "") return "unknown";
  return "running";
}

export function HarnessSessionPanel({
  base,
  engramId,
  harnessKind = null,
  fetchTurns: fetchTurnsOverride,
}: HarnessSessionPanelProps) {
  const { locale } = useI18n();
  const zh = locale === "zh-CN";
  const copy = zh ? COPY.zh : COPY.en;
  const activeEngramId = engramId === "" ? null : engramId;
  const loadTurns = fetchTurnsOverride ?? fetchHarnessTurns;
  const generationRef = useRef(0);
  const refreshRef = useRef<(() => void) | null>(null);
  const olderRequestRef = useRef<AbortController | null>(null);
  const [catalog, setCatalog] = useState<CatalogState>(() =>
    createCatalogState(base, activeEngramId, 0),
  );

  const timeFormatter = useMemo(
    () => new Intl.DateTimeFormat(locale, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    }),
    [locale],
  );

  useEffect(() => {
    const generation = generationRef.current + 1;
    generationRef.current = generation;
    olderRequestRef.current?.abort();
    olderRequestRef.current = null;
    setCatalog(createCatalogState(base, activeEngramId, generation));

    if (activeEngramId === null) {
      refreshRef.current = null;
      return;
    }

    const controller = new AbortController();
    let refreshInFlight = false;

    const refresh = async () => {
      if (refreshInFlight || controller.signal.aborted) return;
      refreshInFlight = true;
      setCatalog((current) => (
        ownsRequest(current, base, activeEngramId, generation)
          ? { ...current, refreshing: true }
          : current
      ));
      try {
        const page = await loadTurns(
          base,
          activeEngramId,
          { limit: CATALOG_PAGE_SIZE },
          controller.signal,
        );
        if (controller.signal.aborted || generationRef.current !== generation) return;
        setCatalog((current) => (
          ownsRequest(current, base, activeEngramId, generation)
            ? applyHeadPage(current, page)
            : current
        ));
      } catch (cause) {
        if (
          controller.signal.aborted
          || generationRef.current !== generation
          || isAbort(cause)
        ) return;
        const fault = faultView(cause);
        setCatalog((current) => (
          ownsRequest(current, base, activeEngramId, generation)
            ? {
                ...current,
                initialized: true,
                refreshing: false,
                catalogFault: fault,
              }
            : current
        ));
      } finally {
        refreshInFlight = false;
      }
    };

    const requestRefresh = () => void refresh();
    refreshRef.current = requestRefresh;
    requestRefresh();
    const interval = window.setInterval(requestRefresh, CATALOG_REFRESH_MS);

    return () => {
      controller.abort();
      window.clearInterval(interval);
      if (refreshRef.current === requestRefresh) refreshRef.current = null;
      const olderController = olderRequestRef.current;
      olderController?.abort();
      if (olderRequestRef.current === olderController) olderRequestRef.current = null;
    };
  }, [activeEngramId, base, loadTurns]);

  const sourceMatches = catalog.ownerBase === base
    && catalog.ownerEngramId === activeEngramId;
  const turns = sourceMatches ? catalog.turns : [];
  const selectedTurnId = sourceMatches ? catalog.selectedTurnId : null;
  const followLatest = sourceMatches ? catalog.followLatest : true;
  const catalogFault = sourceMatches ? catalog.catalogFault : null;
  const olderFault = sourceMatches ? catalog.olderFault : null;
  const refreshing = sourceMatches ? catalog.refreshing : activeEngramId !== null;
  const loadingOlder = sourceMatches && catalog.loadingOlder;
  const initialized = sourceMatches && catalog.initialized;
  const hasMore = sourceMatches && catalog.hasMore;
  const nextCursor = sourceMatches ? catalog.nextCursor : null;
  const unseenCount = sourceMatches ? catalog.unseenTurnIds.length : 0;
  const unseenOverflow = sourceMatches && catalog.unseenOverflow;
  const cursorWasReset = sourceMatches && catalog.notice === "cursor_reset";
  const initialLoading = activeEngramId !== null && turns.length === 0
    && (!initialized || refreshing);

  const selectTurn = useCallback((turnId: string) => {
    if (activeEngramId === null) return;
    setCatalog((current) => {
      if (
        current.ownerBase !== base
        || current.ownerEngramId !== activeEngramId
        || !current.turns.some((turn) => turn.id === turnId)
        || current.selectedTurnId === turnId
      ) return current;
      const enteringPinned = current.followLatest;
      return {
        ...current,
        selectedTurnId: turnId,
        followLatest: false,
        pinLatestId: enteringPinned
          ? current.turns.at(0)?.id ?? null
          : current.pinLatestId,
        unseenTurnIds: enteringPinned ? [] : current.unseenTurnIds,
        unseenOverflow: enteringPinned ? false : current.unseenOverflow,
        notice: null,
      };
    });
  }, [activeEngramId, base]);

  const followNewestTurn = useCallback(() => {
    if (activeEngramId === null) return;
    setCatalog((current) => {
      if (
        current.ownerBase !== base
        || current.ownerEngramId !== activeEngramId
      ) return current;
      return {
        ...current,
        selectedTurnId: current.turns.at(0)?.id ?? null,
        followLatest: true,
        pinLatestId: null,
        unseenTurnIds: [],
        unseenOverflow: false,
        notice: null,
      };
    });
    refreshRef.current?.();
  }, [activeEngramId, base]);

  const loadOlder = useCallback(async () => {
    if (
      activeEngramId === null
      || !sourceMatches
      || !catalog.hasMore
      || catalog.nextCursor === null
      || catalog.loadingOlder
      || olderRequestRef.current !== null
    ) return;

    const generation = catalog.generation;
    const cursor = catalog.nextCursor;
    const controller = new AbortController();
    olderRequestRef.current = controller;
    setCatalog((current) => (
      ownsRequest(current, base, activeEngramId, generation)
        ? {
            ...current,
            loadingOlder: true,
            olderFault: null,
            notice: null,
          }
        : current
    ));

    try {
      const page = await loadTurns(
        base,
        activeEngramId,
        { limit: CATALOG_PAGE_SIZE, before_turn_id: cursor },
        controller.signal,
      );
      if (controller.signal.aborted || generationRef.current !== generation) return;
      setCatalog((current) => (
        ownsRequest(current, base, activeEngramId, generation)
          ? applyOlderPage(current, page)
          : current
      ));
    } catch (cause) {
      if (
        controller.signal.aborted
        || generationRef.current !== generation
        || isAbort(cause)
      ) return;

      if (cause instanceof HarnessFault && cause.status === 404) {
        try {
          const page = await loadTurns(
            base,
            activeEngramId,
            { limit: CATALOG_PAGE_SIZE },
            controller.signal,
          );
          if (controller.signal.aborted || generationRef.current !== generation) return;
          setCatalog((current) => (
            ownsRequest(current, base, activeEngramId, generation)
              ? applyCursorReset(current, page)
              : current
          ));
        } catch (reloadCause) {
          if (
            controller.signal.aborted
            || generationRef.current !== generation
            || isAbort(reloadCause)
          ) return;
          const fault = faultView(reloadCause);
          setCatalog((current) => (
            ownsRequest(current, base, activeEngramId, generation)
              ? { ...current, loadingOlder: false, olderFault: fault }
              : current
          ));
        }
      } else {
        const fault = faultView(cause);
        setCatalog((current) => (
          ownsRequest(current, base, activeEngramId, generation)
            ? { ...current, loadingOlder: false, olderFault: fault }
            : current
        ));
      }
    } finally {
      if (olderRequestRef.current === controller) olderRequestRef.current = null;
      if (!controller.signal.aborted && generationRef.current === generation) {
        setCatalog((current) => (
          ownsRequest(current, base, activeEngramId, generation)
            ? { ...current, loadingOlder: false }
            : current
        ));
      }
    }
  }, [activeEngramId, base, catalog, loadTurns, sourceMatches]);

  const formatTime = (value: string | null): string => {
    if (value === null) return copy.timestampMissing;
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return copy.timestampMissing;
    return timeFormatter.format(parsed);
  };

  let modeLabel: string = copy.noSelection;
  let modeClass: "idle" | "degraded" | "loading" | "empty" | "following" | "pinned" = "idle";
  let modeBody: string = copy.noEngram;
  if (activeEngramId !== null) {
    if (catalogFault !== null) {
      modeLabel = copy.degraded;
      modeClass = "degraded";
      modeBody = turns.length > 0 ? copy.degradedBody : copy.unavailableBody;
    } else if (initialLoading) {
      modeLabel = copy.loading;
      modeClass = "loading";
      modeBody = copy.loading;
    } else if (turns.length === 0) {
      modeLabel = copy.empty;
      modeClass = "empty";
      modeBody = copy.emptyBody;
    } else if (followLatest) {
      modeLabel = copy.following;
      modeClass = "following";
      modeBody = copy.followingBody;
    } else {
      modeLabel = copy.pinned;
      modeClass = "pinned";
      modeBody = copy.pinnedBody;
    }
  }

  return (
    <section
      className={`pw-harness-session-panel is-${modeClass}`}
      aria-label={copy.ariaLabel}
    >
      <section
        className="pw-harness-session-catalog"
        aria-label={copy.catalogLabel}
        aria-busy={initialLoading || loadingOlder || undefined}
      >
        <header className="pw-harness-session-head">
          <div className="pw-harness-session-heading">
            <span className="pw-harness-session-heading-icon">
              <Icon name="history" size={16} />
            </span>
            <div>
              <strong>{copy.title}</strong>
              <span>{modeBody}</span>
            </div>
          </div>
          <div className="pw-harness-session-actions">
            <span
              className={`pw-harness-session-mode is-${modeClass}`}
              role="status"
            >
              {refreshing && turns.length > 0 && (
                <span className="pw-harness-session-sync-dot" aria-hidden="true" />
              )}
              {modeLabel}
            </span>
            {!followLatest && turns.length > 0 && (
              <button
                className="pw-harness-session-follow"
                type="button"
                onClick={followNewestTurn}
              >
                <Icon name="radio" size={14} />
                {copy.followLatest}
              </button>
            )}
            {activeEngramId !== null && (
              <button
                className="pw-harness-session-refresh"
                type="button"
                title={copy.refresh}
                aria-label={copy.refresh}
                disabled={refreshing}
                onClick={() => refreshRef.current?.()}
              >
                <Icon name="refresh" size={14} />
              </button>
            )}
          </div>
        </header>

        {catalogFault !== null && (
          <div
            className="pw-harness-session-fault"
            role={turns.length === 0 ? "alert" : "status"}
          >
            <Icon name="info" size={15} />
            <div>
              <strong>{copy.faultTitle}</strong>
              <span>
                {catalogFault.code !== null && <code>{catalogFault.code}</code>}
                {catalogFault.message}
              </span>
              {catalogFault.remedy !== null && <small>{catalogFault.remedy}</small>}
            </div>
            <button type="button" onClick={() => refreshRef.current?.()}>
              {copy.retry}
            </button>
          </div>
        )}

        {activeEngramId === null ? (
          <div className="pw-harness-session-empty">
            <Icon name="radio" size={21} />
            <div>
              <strong>{copy.noSelection}</strong>
              <span>{copy.noEngram}</span>
            </div>
          </div>
        ) : initialLoading ? (
          <div className="pw-harness-session-empty is-loading" role="status">
            <Icon name="refresh" size={20} />
            <div>
              <strong>{copy.loading}</strong>
              <span>{catalogFault === null ? copy.followingBody : copy.retrying}</span>
            </div>
          </div>
        ) : turns.length === 0 ? (
          <div className="pw-harness-session-empty">
            <Icon name={catalogFault === null ? "history" : "info"} size={21} />
            <div>
              <strong>
                {catalogFault === null ? copy.emptyTitle : copy.unavailableTitle}
              </strong>
              <span>
                {catalogFault === null ? copy.emptyBody : copy.unavailableBody}
              </span>
            </div>
          </div>
        ) : (
          <>
            {!followLatest && unseenCount > 0 && (
              <div className="pw-harness-session-new-turns" role="status">
                <Icon name="spark" size={15} />
                <span>{copy.newTurns(unseenCount, unseenOverflow)}</span>
              </div>
            )}

            {cursorWasReset && (
              <div className="pw-harness-session-notice" role="status">
                <Icon name="refresh" size={15} />
                <span>{copy.cursorReset}</span>
              </div>
            )}

            <div
              className="pw-harness-session-turn-viewport"
              tabIndex={0}
              aria-label={copy.catalogLabel}
            >
              <ol className="pw-harness-session-turn-list">
                {turns.map((turn, index) => {
                  const selected = turn.id === selectedTurnId;
                  const tone = turnTone(turn);
                  const timestamp = turnTimestamp(turn);
                  const errorDetail = [turn.error_phase, turn.error_code]
                    .filter((value): value is string => value !== null)
                    .join(" · ");
                  return (
                    <li key={turn.id}>
                      <button
                        className={`pw-harness-session-turn is-${tone}${
                          selected ? " is-selected" : ""
                        }${selected && !followLatest ? " is-pinned" : ""}`}
                        type="button"
                        aria-pressed={selected}
                        aria-current={selected ? "true" : undefined}
                        aria-label={`${copy.selectTurn} ${turn.id}, ${turn.state}`}
                        onClick={() => selectTurn(turn.id)}
                      >
                        <span className="pw-harness-session-turn-topline">
                          <span className="pw-harness-session-turn-badges">
                            <span>{index === 0 ? copy.latest : copy.history}</span>
                            {selected && (
                              <em>
                                {followLatest
                                  ? copy.followingShort
                                  : copy.pinnedShort}
                              </em>
                            )}
                          </span>
                          <span className={`pw-harness-session-turn-state is-${tone}`}>
                            <i aria-hidden="true" />
                            {turn.state}
                            <span className="pw-harness-session-sr-only">
                              {copy[tone]}
                            </span>
                          </span>
                        </span>
                        <code title={turn.id}>{shortId(turn.id)}</code>
                        <span className="pw-harness-session-turn-time">
                          <Icon name="clock" size={12} />
                          <time dateTime={timestamp ?? undefined} title={timestamp ?? undefined}>
                            {formatTime(timestamp)}
                          </time>
                        </span>
                        {errorDetail !== "" && (
                          <small title={errorDetail}>{errorDetail.slice(0, 120)}</small>
                        )}
                      </button>
                    </li>
                  );
                })}
              </ol>
            </div>

            {olderFault !== null && (
              <div className="pw-harness-session-page-fault" role="alert">
                <Icon name="info" size={14} />
                <span>
                  {olderFault.code !== null && <code>{olderFault.code}</code>}
                  {olderFault.message}
                </span>
              </div>
            )}

            {hasMore && nextCursor === null && (
              <div className="pw-harness-session-page-fault" role="alert">
                <Icon name="info" size={14} />
                <span>{copy.cursorUnavailable}</span>
              </div>
            )}

            <footer className="pw-harness-session-footer">
              <span>{copy.count(turns.length)}</span>
              {hasMore ? (
                <button
                  type="button"
                  disabled={loadingOlder || nextCursor === null}
                  onClick={() => void loadOlder()}
                >
                  <Icon name="chevronDown" size={14} />
                  {loadingOlder ? copy.loadingOlder : copy.loadOlder}
                </button>
              ) : (
                <span className="pw-harness-session-exhausted">
                  <Icon name="check" size={13} />
                  {copy.noMore}
                </span>
              )}
            </footer>
          </>
        )}
      </section>

      {activeEngramId !== null && (
        harnessKind === "mock" && selectedTurnId !== null ? (
          <section className="pw-harness-session-contract-note" role="note">
            <div className="pw-harness-session-contract-icon" aria-hidden="true">
              <Icon name="info" size={17} />
            </div>
            <div>
              <span>{copy.simulatedEvidence}</span>
              <strong>{copy.simulatedTitle}</strong>
              <p>{copy.simulatedBody}</p>
            </div>
          </section>
        ) : (
          <HarnessTurnPanel base={base} turnId={selectedTurnId} />
        )
      )}
    </section>
  );
}

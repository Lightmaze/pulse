"""Read-only Pulse projections used by the right-hand rail.

The rail draws the network *as it bears on the current conversation* — who is
firing, in what order, over which edges. Same sideband posture as app.py, for
the same reason: this module opens the run's **files** (the Pulse metrics JSONL, and the
SQLite in `mode=ro`), never the engine object. It has no write path at all, so
the free-context rule holds trivially — nothing here can reach an LLM context, and nothing
here needs a provider key.

Mounted by the caller, not by this file::

    from pulse_system.interaction.api.routes_pulse import create_pulse_router
    app.include_router(create_pulse_router(metrics_path, db_path=db))

The prefix is fixed at ``/pulse`` because these paths form the stable
frontend/runtime boundary.

Two states this module refuses to conflate, per the standing house rule:

- **there is nothing there** — no engrams, no edges, no firings yet. That is a
  legitimate state of a young network and answers with an empty array.
- **I cannot see** — no SQLite file was configured at all. That answers 404
  with a remedy (contract §6), because returning ``[]`` for it would report a
  blindfold as an empty room.

A configured database that does not exist yet, or exists without a schema, is
the *first* case: the run has not started writing. LineTailer already takes
that stance for the JSONL ("connecting before the run starts must not be an
error") and the two halves of one run should not disagree about it.
"""

from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.responses import JSONResponse

from pulse_system.core.connection.viability import CONNECTIVITY_SCHEMA_VERSION
from pulse_system.core.connection.viability import ConnectivityEdge, analyze_connectivity
from pulse_system.core.types import ConnectionType, session_name
from pulse_system.interaction.api.tailer import (
    DEFAULT_REPLAY_BYTES,
    LineTailer,
    TailBatch,
)

# Seconds of look-back for /pulse/history when the caller names none (§3).
DEFAULT_HISTORY_WINDOW = 300.0

# An engram reads as `firing` for this long after its last pulse. A pulse is
# an LLM round trip, so the honest unit here is seconds, not ticks — the rail
# wants a flash that outlives one frame and dies well inside one conversation
# turn. Tunable at mount; deliberately not a query parameter, so every client
# of one server agrees on what "firing" means.
DEFAULT_FIRING_WINDOW = 10.0

# Mirrors of PulseEngineConfig defaults. Duplicated rather than imported: the
# observatory must not pull the engine (and its LLM adapter) into its own
# process just to read two floats — that is the whole point of being sideband.
# Pass the run's real values at mount if it overrides them.
DEFAULT_INHIBITION_TAU = 30.0            # PulseEngineConfig.inhibition_tau
DEFAULT_GATE = 0.0                       # .inhibition_propagation_gate

DEFAULT_PROPAGATION_THRESHOLD = 0.3

_RUNTIME_CONNECTIVITY_EVIDENCE = "runtime_effective_threshold_projection"
_SIDEBAND_CONNECTIVITY_EVIDENCE = "sideband_base_threshold_projection"
_MAX_CONNECTIVITY_ISOLATE_IDS = 20
_CONNECTIVITY_COUNT_FIELDS = (
    "node_count", "raw_edge_count", "effective_excitatory_edge_count",
    "effective_inhibitory_edge_count", "effective_self_loop_count", "weak_component_count",
    "largest_weak_component_size", "strong_component_count",
    "largest_strong_component_size", "largest_out_reach_size", "isolated_node_count",
    "source_only_node_count", "sink_only_node_count", "cycle_capable_node_count",
    "weak_cut_vertex_count",
)
_CONNECTIVITY_RATIO_FIELDS = (
    "largest_weak_fraction", "largest_strong_fraction", "largest_out_reach_fraction",
    "mean_out_reach_fraction", "cycle_capable_fraction",
)
_CONNECTIVITY_NULLABLE_RATIO_FIELDS = (
    "excitatory_edge_occupancy", "minimum_gate_acceptance", "mean_gate_acceptance",
)
_CONNECTIVITY_SOURCE_THRESHOLD_FIELDS = (
    "source_threshold_min", "source_threshold_max", "source_threshold_mean",
)
_STRUCTURAL_REGIMES = {
    "empty", "singleton", "fragmented_acyclic", "fragmented_reverberant",
    "strongly_connected", "connected_acyclic", "connected_reverberant",
}
_OBSERVATION_ORDER = (
    "content_fragmented", "isolate_present", "cycle_capacity_present", "weak_cut_present",
)
# Below this the engine forgets an inhibition level entirely (_inhibition_level).
_INHIBITION_FLOOR = 1e-4

# PulseReason.value → the contract's `kind` vocabulary. The engine's names and
# the rail's names differ; this is the only place that translation lives.
_KIND = {
    "spontaneous": "spontaneous",
    "propagation": "propagated",
    "external": "injected",
}

# Contract §6, and flat on purpose: HTTPException would nest this under
# "detail", where the rail's reader (web/src/pulse.ts readFault) finds a
# string-typed field and drops the remedy on the floor. A refusal whose
# remedy never reaches the screen is the half-refusal the rule forbids.
_NO_DB = {
    "error": "no_db",
    "detail": "the network view reads the run's SQLite file, and this server "
              "was started without one",
    "remedy": "start the server with --db <run.db>, or pass db_path= to "
              "create_pulse_router",
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: object, *, require_timezone: bool = False) -> datetime | None:
    """Tolerant ISO-8601 read; None for anything unparseable."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo:
        return parsed.astimezone(timezone.utc)
    return None if require_timezone else parsed.replace(tzinfo=timezone.utc)


def _is_number(
    value: object, *, minimum: float | None = None, maximum: float | None = None,
) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        number = float(value)
        return (
            math.isfinite(number)
            and (minimum is None or number >= minimum)
            and (maximum is None or number <= maximum)
        )
    except (OverflowError, TypeError, ValueError):
        return False


def _is_fingerprint(value: object) -> bool:
    return (
        isinstance(value, str) and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _validated_runtime_connectivity(
    event: object,
    fallback: dict[str, object],
) -> tuple[dict[str, object], datetime] | None:
    """Sanitize one transport record; graph semantics stay in the analyzer."""
    metadata_fields = {"t", "type", "tick"}
    if type(event) is not dict or set(event) != set(fallback) | metadata_fields:
        return None
    observed_at = _parse_iso(event["t"], require_timezone=True)
    if (
        event["type"] != "connectivity"
        or type(event["tick"]) is not int
        or event["tick"] < 0
        or observed_at is None
        or event["schema_version"] != CONNECTIVITY_SCHEMA_VERSION
        or event["evidence_class"] != _RUNTIME_CONNECTIVITY_EVIDENCE
    ):
        return None
    if (
        type(event["structural_regime"]) is not str
        or event["structural_regime"] not in _STRUCTURAL_REGIMES
    ):
        return None
    if (
        not _is_fingerprint(event["node_fingerprint"])
        or not _is_fingerprint(event["raw_topology_fingerprint"])
        or event["node_fingerprint"] != fallback["node_fingerprint"]
        or event["raw_topology_fingerprint"]
        != fallback["raw_topology_fingerprint"]
    ):
        return None
    if any(
        type(event[field]) is not int or event[field] < 0
        for field in _CONNECTIVITY_COUNT_FIELDS
    ):
        return None
    if any(
        not _is_number(event[field], minimum=0.0, maximum=1.0)
        for field in _CONNECTIVITY_RATIO_FIELDS
    ):
        return None
    if any(
        event[field] is not None
        and not _is_number(event[field], minimum=0.0, maximum=1.0)
        for field in _CONNECTIVITY_NULLABLE_RATIO_FIELDS
    ):
        return None
    if not _is_number(event["base_threshold"], minimum=0.0):
        return None
    if any(
        event[field] is not None
        and not _is_number(event[field], minimum=0.0)
        for field in _CONNECTIVITY_SOURCE_THRESHOLD_FIELDS
    ):
        return None

    isolate_ids = event["isolated_node_ids"]
    if (
        type(isolate_ids) is not list
        or len(isolate_ids) > _MAX_CONNECTIVITY_ISOLATE_IDS
        or any(type(node_id) is not str or not node_id for node_id in isolate_ids)
        or isolate_ids != sorted(set(isolate_ids))
        or type(event["isolated_node_ids_truncated"]) is not bool
    ):
        return None
    observations = event["observations"]
    if (
        type(observations) is not list
        or any(
            type(observation) is not str
            or observation not in _OBSERVATION_ORDER
            for observation in observations
        )
        or observations
        != [item for item in _OBSERVATION_ORDER if item in observations]
    ):
        return None
    reference = event["percolation_reference"]
    expected_reference = fallback["percolation_reference"]
    if (
        type(reference) is not dict
        or set(reference) != set(expected_reference)
        or reference != expected_reference
        or not _is_number(
            reference["critical_occupation_probability"], minimum=0.0, maximum=1.0
        )
        or reference["applicable"] is not False
    ):
        return None
    if (
        event["node_count"] != fallback["node_count"]
        or event["raw_edge_count"] != fallback["raw_edge_count"]
        or float(event["base_threshold"]) != float(fallback["base_threshold"])
    ):
        return None

    source_thresholds = [event[field] for field in _CONNECTIVITY_SOURCE_THRESHOLD_FIELDS]
    if (event["node_count"] == 0) != all(
        value is None for value in source_thresholds
    ):
        return None
    if (event["excitatory_edge_occupancy"] is None) != (event["node_count"] < 2):
        return None
    if (event["minimum_gate_acceptance"] is None) != (
        event["mean_gate_acceptance"] is None
    ):
        return None

    return ({field: event[field] for field in fallback}, observed_at)


def _identity(row: sqlite3.Row, first_content: str | None) -> dict[str, object]:
    """Read new identity columns while remaining tolerant of pre-v0.1 DBs."""
    keys = set(row.keys())
    # A new-schema row with a null name is intentionally unnamed. In
    # particular, the runtime's private FRONT_SEED is not effective session
    # content and must never leak out as a public title. Only legacy databases
    # without the column derive a display name from their first message.
    name = (
        row["name"] or row["id"]
        if "name" in keys
        else session_name(first_content or "") or row["id"]
    )
    return {
        "name": name,
        "name_origin": row["name_origin"] if "name_origin" in keys else "auto",
        "nickname": row["nickname"] if "nickname" in keys else None,
    }


def _read_events(
    path: Path,
    replay_bytes: int,
) -> tuple[list[dict], TailBatch]:
    """Parse one bounded recent window and preserve its replay facts."""
    events: list[dict] = []
    batch = LineTailer(path, replay_bytes=replay_bytes).read_batch()
    for line in batch.lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return events, batch


def _set_replay_headers(response: Response, batch: TailBatch) -> None:
    """Keep list response bodies compatible while stating projection scope."""
    response.headers["X-Pulse-Replay-Complete"] = str(
        not batch.replay_truncated
    ).lower()
    response.headers["X-Pulse-Replay-Truncated"] = str(
        batch.replay_truncated
    ).lower()
    response.headers["X-Pulse-Replay-Window-Bytes"] = str(batch.window_bytes)
    response.headers["X-Pulse-Replay-Start-Offset"] = str(batch.start_offset)
    response.headers["X-Pulse-Replay-End-Offset"] = str(batch.end_offset)
    if batch.cursor is not None:
        response.headers["X-Pulse-Replay-Cursor"] = batch.cursor
    if batch.reset_reason is not None:
        response.headers["X-Pulse-Replay-Reset"] = batch.reset_reason


def create_pulse_router(
    metrics_path: str | Path,
    *,
    db_path: str | Path | None = None,
    firing_window: float = DEFAULT_FIRING_WINDOW,
    inhibition_tau: float = DEFAULT_INHIBITION_TAU,
    gate: float = DEFAULT_GATE,
    propagation_threshold: float = DEFAULT_PROPAGATION_THRESHOLD,
    replay_bytes: int = DEFAULT_REPLAY_BYTES,
) -> APIRouter:
    """Build the /pulse router. Read-only in every direction."""
    if replay_bytes <= 0:
        raise ValueError("replay_bytes must be positive")
    if not _is_number(propagation_threshold, minimum=0.0):
        raise ValueError("propagation_threshold must be finite and non-negative")
    base_propagation_threshold = float(propagation_threshold)
    path = Path(metrics_path)
    db = Path(db_path) if db_path is not None else None
    router = APIRouter(prefix="/pulse", tags=["pulse"])

    @contextmanager
    def _reader() -> Iterator[sqlite3.Connection | None]:
        """A read-only connection, or None when there is nothing yet to read.

        mode=ro means this can neither block nor corrupt the engine's writer;
        a database the engine happens to be holding surfaces as 503, never as
        a crash and never as a wrong answer.
        """
        assert db is not None  # routes check first and answer 404 with a remedy
        if not db.exists():
            yield None  # the run has not written yet — empty, not broken
            return
        try:
            conn = sqlite3.connect(
                f"file:{db.as_posix()}?mode=ro", uri=True, timeout=2.0,
            )
        except sqlite3.OperationalError as exc:
            raise HTTPException(503, detail=f"database busy: {exc}") from exc
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _fetch(
        conn: sqlite3.Connection | None, sql: str, params: tuple = (),
    ) -> list[sqlite3.Row]:
        if conn is None:
            return []
        try:
            return conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return []  # schema not created yet: an empty run, not a fault
            raise HTTPException(503, detail=f"database busy: {exc}") from exc

    def _first_messages(conn: sqlite3.Connection | None) -> dict[str, str]:
        rows = _fetch(conn, """
            SELECT engram_id, content FROM messages
            WHERE id IN (SELECT MIN(id) FROM messages GROUP BY engram_id)
        """)
        return {r["engram_id"]: r["content"] for r in rows}

    def _inhibition_levels(
        conn: sqlite3.Connection | None, events: list[dict], now: datetime,
    ) -> dict[str, float]:
        """Rebuild each engram's inhibition level from the observed stream.

        The engine holds this in memory (PulseEngine._inhibition) and no event
        carries the level itself, so the sideband re-runs the engine's own
        arithmetic over what *is* recorded: a `propagate` event names the
        targets that received inhibition instead of content, the amount is
        that edge's weight, and each contribution decays exp(-dt/tau)
        (_add_inhibition / _inhibition_level).

        Two limits worth knowing before drawing a bar from this number: edge
        weights are read as they stand *now*, not as they stood at fire time,
        and a level raised before the visible span of the JSONL is invisible.
        So 0.0 means "nothing in the observed stream raised it" — which, since
        STDP only ever creates excitatory edges, is also the honest answer for
        every run that has no hand-made inhibitory edge in it.
        """
        fired = [
            (e.get("source"), e.get("inhibited") or [], _parse_iso(e.get("t")))
            for e in events
            if e.get("type") == "propagate" and e.get("inhibited")
        ]
        if not fired:
            return {}
        weights = {
            (r["from_id"], r["to_id"]): r["weight"]
            for r in _fetch(conn, """
                SELECT from_id, to_id, weight FROM connections
                WHERE conn_type = 'inhibitory'
            """)
        }
        levels: dict[str, float] = {}
        for source, targets, t in fired:
            if t is None:
                continue
            decay = math.exp(-max(0.0, (now - t).total_seconds()) / inhibition_tau)
            for target in targets:
                weight = weights.get((source, target), 0.0)
                if weight:
                    levels[target] = levels.get(target, 0.0) + weight * decay
        return {
            eid: round(level, 4)
            for eid, level in levels.items()
            if level >= _INHIBITION_FLOOR
        }

    @router.get("/active", response_model=None)
    def active(response: Response) -> list[dict] | JSONResponse:
        """Live engrams and how each one currently stands (§3).

        Active only: a succeeded (archived) engram no longer bears on the
        conversation, and drawing it would make the rail claim a wider live
        network than there is.

        `gate` is the inhibition→propagation gate this server was told the run
        uses. The claustrum's per-engram gate factor (claustrum fourth head) emits no
        event today, so the sideband cannot see it — with `modulate_gate` off
        (the default) this value is exact for every engram; with it on, this
        is the base and the per-engram factor is missing from the wire.
        """
        if db is None:
            return JSONResponse(status_code=404, content=_NO_DB)
        now = _now()
        events, batch = _read_events(path, replay_bytes)
        _set_replay_headers(response, batch)
        with _reader() as conn:
            rows = _fetch(conn, """
                SELECT * FROM engrams
                WHERE status = 'active' ORDER BY created_at
            """)
            if not rows:
                return []
            titles = _first_messages(conn)
            inhibition = _inhibition_levels(conn, events, now)

        out = []
        for row in rows:
            last = _parse_iso(row["last_pulse_at"])
            elapsed = (now - last).total_seconds() if last else None
            out.append({
                "engram_id": row["id"],
                **_identity(row, titles.get(row["id"])),
                "firing": elapsed is not None and elapsed <= firing_window,
                "inhibition": inhibition.get(row["id"], 0.0),
                "gate": gate,
                # null, not a fabricated timestamp: an engram that has never
                # pulsed has no last firing, and 1970 is not a better answer.
                "last_fired_at": row["last_pulse_at"],
            })
        return out

    @router.get("/history")
    def history(
        response: Response,
        window: float = Query(DEFAULT_HISTORY_WINDOW, ge=0),
    ) -> list[dict]:
        """The firing sequence — who fired, in what order, with what gaps.

        Not a metric series. The rail draws this because it is precisely what
        STDP consumes: pass 4b of demo.py shows the offline weight table this
        ordering produces, and this endpoint is its live counterpart. So: one
        item per firing, sorted by time ascending, never bucketed, never
        deduplicated. Two firings inside the same second are two items,
        because to STDP they are two events with a gap between them.

        `window` is seconds, measured back from the newest event in the stream
        rather than from wall-clock now. A finished or replayed run would
        otherwise answer "nothing fired" when what is true is "nothing fired
        *recently*" — and the sequence it does hold is exactly what the rail
        was opened to read. window=0 means no time filter.

        Kinds are the contract's three. An unrecognised engine reason passes
        through under its own name instead of being dropped: a firing that
        happened must not vanish from a firing sequence, and a hole in an
        order is a false order.
        """
        pulses = []
        latest: datetime | None = None
        events, batch = _read_events(path, replay_bytes)
        _set_replay_headers(response, batch)
        for event in events:
            t = _parse_iso(event.get("t"))
            if t is None:
                continue
            if latest is None or t > latest:
                latest = t
            if event.get("type") != "pulse":
                continue
            engram_id = event.get("engram")
            if not engram_id:
                continue
            reason = event.get("reason")
            pulses.append((t, {
                "engram_id": engram_id,
                "t": event["t"],
                "kind": _KIND.get(reason, reason or "unknown"),
            }))

        if window and latest is not None:
            cutoff = latest.timestamp() - window
            pulses = [p for p in pulses if p[0].timestamp() >= cutoff]

        pulses.sort(key=lambda p: p[0])  # stable: co-timed firings keep file order
        return [item for _, item in pulses]

    @router.get("/topology", response_model=None)
    def topology() -> dict | JSONResponse:
        """The standing graph: nodes the rail can lay out, edges with weights.

        Nodes are keyed `engram_id`, the same key /pulse/active uses, so the
        rail can join the two without a lookup table. Edges carry `type`
        alongside {from, to, weight} — an inhibitory edge drawn as if it were
        excitatory reverses the meaning of the picture.

        Edges whose endpoint is archived are dropped, mirroring the engine's
        own metrics topology dump: an edge must resolve to a node.
        """
        if db is None:
            return JSONResponse(status_code=404, content=_NO_DB)
        with _reader() as conn:
            rows = _fetch(conn, """
                SELECT * FROM engrams
                WHERE status = 'active' ORDER BY created_at
            """)
            if not rows:
                return {"nodes": [], "edges": []}
            titles = _first_messages(conn)
            edge_rows = _fetch(conn, """
                SELECT from_id, to_id, weight, conn_type FROM connections
            """)

        nodes = [
            {
                "engram_id": r["id"],
                **_identity(r, titles.get(r["id"])),
                "project_id": r["project_id"],
                "activity": round(r["recent_activity"], 4),
                "total_pulses": r["total_pulses"],
            }
            for r in rows
        ]
        known = {n["engram_id"] for n in nodes}
        edges = [
            {
                "from": e["from_id"],
                "to": e["to_id"],
                "weight": round(e["weight"], 4),
                "type": e["conn_type"],
            }
            for e in edge_rows
            if e["from_id"] in known and e["to_id"] in known
        ]
        return {"nodes": nodes, "edges": edges}

    @router.get("/connectivity", response_model=None)
    def connectivity(response: Response) -> dict | JSONResponse:
        """Current threshold-eligible content graph, with explicit evidence."""
        if db is None:
            return JSONResponse(status_code=404, content=_NO_DB)

        now = _now()
        events, batch = _read_events(path, replay_bytes)
        _set_replay_headers(response, batch)
        with _reader() as conn:
            node_rows = _fetch(conn, """
                SELECT id FROM engrams
                WHERE status = 'active' ORDER BY id
            """)
            node_ids = sorted({
                row["id"]
                for row in node_rows
                if isinstance(row["id"], str) and row["id"]
            })
            edge_rows = _fetch(conn, """
                SELECT from_id, to_id, weight, conn_type FROM connections
            """)

        active_ids = set(node_ids)
        edges: list[ConnectivityEdge] = []
        for row in edge_rows:
            source = row["from_id"]
            target = row["to_id"]
            weight = row["weight"]
            if source not in active_ids or target not in active_ids:
                continue
            if not _is_number(weight, minimum=0.0):
                continue
            try:
                conn_type = ConnectionType(row["conn_type"])
            except (TypeError, ValueError):
                continue
            edges.append(ConnectivityEdge(
                source, target, float(weight), conn_type
            ))

        fallback = analyze_connectivity(
            node_ids,
            edges,
            base_threshold=base_propagation_threshold,
            evidence_class=_SIDEBAND_CONNECTIVITY_EVIDENCE,
        )
        replay_complete = not batch.replay_truncated
        for event in reversed(events):
            validated = _validated_runtime_connectivity(event, fallback)
            if validated is None:
                continue
            runtime_projection, observed_at = validated
            return {
                **runtime_projection,
                "projection_source": "runtime_event",
                "observed_at": observed_at.isoformat(),
                "age_seconds": round(max(
                    0.0, (now - observed_at).total_seconds()
                ), 6),
                "replay_complete": replay_complete,
            }

        return {
            **fallback,
            "projection_source": "sideband_fallback",
            "observed_at": now.isoformat(),
            "age_seconds": 0.0,
            "replay_complete": replay_complete,
        }

    return router

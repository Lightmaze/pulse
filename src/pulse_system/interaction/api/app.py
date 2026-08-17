"""The sideband observatory API.

Serves the event stream over SSE so a viewer can run live against the same
reducer it uses for replay.

Deliberately *not* in-process with the engine: this reads the metrics file
only.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
from collections import Counter
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.routing import Match

from pulse_system.interaction.api.tailer import DEFAULT_REPLAY_BYTES, LineTailer
from pulse_system.interaction.api.security import (
    ApiSecurityMiddleware,
    LocalApiSecurity,
)
from pulse_system.version import PUBLIC_VERSION

_logger = logging.getLogger("pulse_system.observatory")

POLL_INTERVAL = 0.5


def _sse(event: str, data: str, event_id: str | None = None) -> str:
    identity = "" if event_id is None else f"id: {event_id}\n"
    return f"{identity}event: {event}\ndata: {data}\n\n"


def _frame(lines: list[str]) -> str:
    """Batch raw JSONL lines into one JSON array without re-parsing them."""
    return "[" + ",".join(lines) + "]"


def create_app(
    metrics_path: str | Path,
    *,
    db_path: str | Path | None = None,
    static_dir: str | Path | None = None,
    poll_interval: float = POLL_INTERVAL,
    runtime: object | None = None,
    propagation_threshold: float | None = None,
    replay_bytes: int = DEFAULT_REPLAY_BYTES,
    api_security: LocalApiSecurity | None = None,
) -> FastAPI:
    if replay_bytes <= 0:
        raise ValueError("replay_bytes must be positive")
    path = Path(metrics_path)
    db = Path(db_path) if db_path is not None else None
    security = api_security if api_security is not None else LocalApiSecurity()
    app = FastAPI(title="Pulse Observatory", version=PUBLIC_VERSION)

    def _db_conn() -> sqlite3.Connection:
        """A fresh read-only connection per request.

        Same sideband stance as the tailer: we open the *file*, never the
        engine's Storage object. mode=ro means we cannot block or corrupt a
        writer; a busy database surfaces as 503, never a crash.
        """
        if db is None:
            raise HTTPException(
                404,
                detail="session inspection needs the run's SQLite file — "
                       "start the server with --db <run.db>",
            )
        if not db.exists():
            raise HTTPException(404, detail=f"database not found: {db}")
        try:
            conn = sqlite3.connect(
                f"file:{db.as_posix()}?mode=ro", uri=True, timeout=2.0,
            )
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.OperationalError as exc:
            raise HTTPException(503, detail=f"database busy: {exc}") from exc

    class _ReadOnlyHarnessEventReader:
        """File-backed replay reader for an observatory without Runtime.

        The live service exposes the locked ``HarnessEventStore`` directly.
        A separately launched observatory must not open a writable Storage or
        acquire the PulseWorld lease, so it uses one short-lived SQLite
        ``mode=ro`` connection per bounded request instead.
        """

        _COLUMNS = (
            "event_id, turn_id, world_id, engram_id, seq, parent_event_id, "
            "kind, phase, source, status, occurred_at, payload_json, "
            "payload_bytes, payload_digest, redacted, truncated"
        )

        def replay(self, turn_id: str, *, after_seq: int = 0, limit: int = 100):
            from pulse_system.agent.harness.events import HarnessEvent

            conn = _db_conn()
            try:
                rows = conn.execute(
                    f"SELECT {self._COLUMNS} FROM harness_events "
                    "WHERE turn_id = ? AND seq > ? ORDER BY seq LIMIT ?",
                    (turn_id, after_seq, min(max(int(limit), 1), 500) + 1),
                ).fetchall()
                page_rows = rows[: min(max(int(limit), 1), 500)]
                events = [
                    HarnessEvent.from_storage_row(tuple(row)).to_dict()
                    for row in page_rows
                ]
                oldest_row = conn.execute(
                    "SELECT MIN(seq), MAX(seq) FROM harness_events WHERE turn_id = ?",
                    (turn_id,),
                ).fetchone()
                oldest = oldest_row[0] if oldest_row else None
                latest = oldest_row[1] if oldest_row else None
                gap = None
                if oldest is not None and after_seq < int(oldest) - 1:
                    gap = {
                        "missing_from": after_seq + 1,
                        "missing_to": int(oldest) - 1,
                        "reason": "pruned_or_missing",
                    }
                return {
                    "turn_id": turn_id,
                    "events": events,
                    "next_seq": events[-1]["seq"] if events else after_seq,
                    "has_more": len(rows) > len(page_rows),
                    "gap": gap,
                    "gaps": [] if gap is None else [gap],
                    "earliest_seq": oldest,
                    "oldest_seq": oldest,
                    "latest_seq": latest,
                    "turn_known": oldest is not None,
                    "evidence_class": "LIVE_GATE_UNVERIFIED",
                }
            finally:
                conn.close()

        def capacity_snapshot(self, **_: object) -> dict[str, int | None]:
            conn = _db_conn()
            try:
                row = conn.execute(
                    "SELECT COUNT(*), COALESCE(SUM(payload_bytes), 0), "
                    "COUNT(DISTINCT turn_id) FROM harness_events"
                ).fetchone()
                return {
                    "event_rows": int(row[0]),
                    "event_bytes": int(row[1]),
                    "retained_turns": int(row[2]),
                }
            finally:
                conn.close()

    harness_event_store = (
        getattr(runtime, "harness_event_store", None)
        if runtime is not None
        else None
    )
    if harness_event_store is None and db is not None:
        harness_event_store = _ReadOnlyHarnessEventReader()
    harness_control_gateway = (
        getattr(runtime, "harness_control_gateway", None)
        if runtime is not None
        else None
    )

    def _route_template(scope: dict) -> str:
        partial: str | None = None
        pending = list(app.router.routes)
        while pending:
            route = pending.pop(0)
            included = getattr(route, "original_router", None)
            if included is not None:
                pending[0:0] = list(getattr(included, "routes", ()))
                continue
            match, _ = route.matches(scope)
            path_template = getattr(route, "path", None)
            if not isinstance(path_template, str):
                continue
            if match is Match.FULL:
                return path_template
            if match is Match.PARTIAL:
                partial = path_template
        return partial or "unmatched"

    # Authentication is the inner boundary; CORS is outermost so browser
    # clients receive exact-origin headers even on fixed 401/403 responses.
    app.add_middleware(
        ApiSecurityMiddleware,
        security=security,
        route_template=_route_template,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(security.allowed_origins),
        allow_methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "Last-Event-ID"],
    )
    app.state.api_security = security
    _logger.info(
        "api_security_ready profile=%s loopback=%s origin_count=%d",
        security.profile.value,
        security.loopback_only,
        len(security.allowed_origins),
    )

    @app.get("/health")
    def health() -> dict:
        return {
            "ok": True,
            "metrics_available": path.exists(),
            "replay_window_bytes": replay_bytes,
        }

    @app.get("/runtime-profile")
    def runtime_profile() -> dict[str, object]:
        """Public safety projection; never includes the startup token."""
        return security.public_projection()

    @app.get("/status")
    def status() -> dict:
        """Counts + heartbeat from the bounded replay projection."""
        counts: Counter = Counter()
        heartbeat: dict | None = None
        batch = LineTailer(path, replay_bytes=replay_bytes).read_batch()
        for line in batch.lines:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            counts[event.get("type", "?")] += 1
            if event.get("type") == "heartbeat":
                heartbeat = {
                    "active": event.get("active"),
                    "total": event.get("total"),
                    "ratio": event.get("ratio"),
                    "coherent": event.get("coherent"),
                    "breadth": event.get("breadth"),
                }
        return {
            "event_counts": dict(counts),
            "event_counts_scope": "replay_window",
            "heartbeat": heartbeat,
            "replay": batch.replay,
        }

    @app.get("/projects")
    def projects() -> dict:
        """Projects (spec-level purpose clusters) with engram counts."""
        conn = _db_conn()
        try:
            rows = conn.execute(
                """SELECT p.id, p.name, p.description, p.created_at,
                          COUNT(e.id) AS engram_count
                   FROM projects p LEFT JOIN engrams e ON e.project_id = p.id
                   GROUP BY p.id ORDER BY p.created_at""",
            ).fetchall()
        except sqlite3.OperationalError as exc:
            raise HTTPException(503, detail=f"database busy: {exc}") from exc
        finally:
            conn.close()
        return {"projects": [dict(r) for r in rows]}

    @app.get("/substrates")
    def substrates() -> dict:
        """The provider table for the model page (per-Engram substrate binding's adapter registry).

        key_configured is a boolean env check — the key value itself must
        never cross the wire.
        """
        from pulse_system.substrate.llm.adapter import _PROVIDER_PROFILES

        providers = [
            {
                "provider": name,
                "base_url": profile["base_url"],
                "model": profile["model"],
                "api_key_env": profile["api_key_env"],
                "key_configured": bool(os.environ.get(profile["api_key_env"])),
                "cache_read_discount": profile["cache_read_discount"],
            }
            for name, profile in _PROVIDER_PROFILES.items()
        ]
        return {"providers": providers}

    @app.get("/engrams")
    def engrams() -> dict:
        """Every engram with enough metadata to pick one to read."""
        conn = _db_conn()
        try:
            rows = conn.execute(
                """SELECT e.*, COUNT(m.id) AS message_count
                   FROM engrams e LEFT JOIN messages m ON m.engram_id = e.id
                   GROUP BY e.id ORDER BY e.created_at""",
            ).fetchall()
        except sqlite3.OperationalError as exc:
            raise HTTPException(503, detail=f"database busy: {exc}") from exc
        finally:
            conn.close()
        return {"engrams": [dict(r) for r in rows]}

    @app.get("/engrams/{engram_id}")
    def engram_session(engram_id: str) -> dict:
        """The session itself — natural language, transparency mode 1."""
        conn = _db_conn()
        try:
            head = conn.execute(
                "SELECT * FROM engrams WHERE id = ?", (engram_id,),
            ).fetchone()
            if head is None:
                raise HTTPException(404, detail=f"no engram {engram_id}")
            rows = conn.execute(
                """SELECT role, content, timestamp, source_engram_id
                   FROM messages WHERE engram_id = ? ORDER BY id""",
                (engram_id,),
            ).fetchall()
        except sqlite3.OperationalError as exc:
            raise HTTPException(503, detail=f"database busy: {exc}") from exc
        finally:
            conn.close()
        return {"engram": dict(head), "messages": [dict(r) for r in rows]}

    @app.get("/events")
    async def events(request: Request, once: bool = False) -> StreamingResponse:
        """A bounded replay `snapshot`, then incremental `append` frames.

        One frame per batch, not per event: replaying a 15k-event run must
        not become 15k SSE dispatches. Because a single tailer spans both
        phases there is no offset handshake between them, so no gap and no
        duplicate at the seam.

        `?once=1` sends the snapshot and closes — a one-shot fetch of the
        current state for scripts, and the bounded form the tests can drive
        (an open-ended stream never returns).
        """

        async def stream():
            tailer = LineTailer(
                path,
                replay_bytes=replay_bytes,
                cursor=request.headers.get("last-event-id"),
            )
            first = True
            last_replay: tuple[object, ...] | None = None
            while True:
                if not once and await request.is_disconnected():
                    break
                batch = await asyncio.to_thread(tailer.read_batch)
                replay_key = (
                    batch.cursor,
                    batch.reset_reason,
                    batch.file_size,
                    batch.replay_truncated,
                )
                if (first or batch.reset_reason is not None) and replay_key != last_replay:
                    yield _sse(
                        "replay",
                        json.dumps(batch.replay, separators=(",", ":")),
                        batch.cursor,
                    )
                    last_replay = replay_key

                if batch.lines:
                    yield _sse(
                        "snapshot" if first else "append",
                        _frame(batch.lines),
                        batch.cursor,
                    )
                    first = False
                elif first:
                    # Empty/absent file: connected, with replay facts already
                    # stated in the preceding frame.
                    yield _sse("snapshot", "[]", batch.cursor)
                    first = False
                else:
                    yield ": keepalive\n\n"
                if once:
                    return
                await asyncio.sleep(poll_interval)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # The rail's read endpoints do not retain the runtime object: like the
    # tailer, they read the run's files. So they mount whether or not a live
    # service is attached, and a replayed run gets the same rail as a live one.
    from .routes_pulse import (
        DEFAULT_PROPAGATION_THRESHOLD,
        create_pulse_router,
    )
    from .routes_causal import create_causal_router
    from .routes_harness import create_harness_router

    mounted_propagation_threshold = propagation_threshold
    if mounted_propagation_threshold is None:
        runtime_config = getattr(runtime, "config", None)
        mounted_propagation_threshold = getattr(
            runtime_config,
            "propagation_threshold",
            DEFAULT_PROPAGATION_THRESHOLD,
        )
    app.include_router(create_pulse_router(
        path,
        db_path=db,
        propagation_threshold=mounted_propagation_threshold,
        replay_bytes=replay_bytes,
    ))
    app.include_router(
        create_causal_router(
            db_path=db,
            runtime=runtime,
            poll_interval=poll_interval,
        )
    )
    app.state.harness_event_store = harness_event_store
    app.state.harness_control_gateway = harness_control_gateway
    app.include_router(
        create_harness_router(
            event_store=harness_event_store,
            control_gateway=harness_control_gateway,
            runtime=runtime,
            world_id=getattr(runtime, "world_id", None),
            poll_interval=poll_interval,
        )
    )

    # The write half mounts only when a live runtime is attached. Without one
    # there is nothing to inject into and no tick for a tuning value to land
    # on, and a POST that accepts work nobody will do is worse than a 404.
    if runtime is not None:
        from .routes_write import create_write_router

        app.state.runtime = runtime
        app.include_router(create_write_router(runtime))

    # Mounted last: StaticFiles at "/" swallows every unmatched path, so any
    # router added after this point would be shadowed by the SPA.
    if static_dir is not None:
        d = Path(static_dir)
        if d.is_dir():
            app.mount("/", StaticFiles(directory=d, html=True), name="web")
        else:
            _logger.warning("static dir %s not found — API only", d)

    return app

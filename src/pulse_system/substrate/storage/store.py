"""Persistent storage backed by SQLite.

Responsibilities:
- Engram CRUD (create, read, append messages, archive)
- Connection CRUD (create, query edges, update weight, batch decay, prune)
- Session history is append-only (append-only storage / E1)
"""

from __future__ import annotations

import functools
import json
import math
import re
import sqlite3
import threading
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from numbers import Real
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from pulse_system.substrate.storage.migrator import (
    SchemaMigrationResult,
    SchemaMigrator,
    TARGET_SCHEMA_VERSION,
    execute_sql_script,
)

_STORAGE_BOOTSTRAP_TABLES = (
    "activity_centers",
    "causal_events",
    "center_memberships",
    "center_reservations",
    "center_schedule_state",
    "component_slots",
    "component_state",
    "connections",
    "delegations",
    "dendritic_integration_members",
    "dendritic_integrations",
    "dendritic_integration_windows",
    "dendritic_legacy_integrations",
    "dendritic_input_policy_snapshots",
    "dendritic_window_members",
    "dendritic_windows",
    "engrams",
    "factory_connections",
    "generation_transitions",
    "habitat_subscriptions",
    "harness_control_observations",
    "harness_events",
    "harness_operations",
    "harness_turns",
    "living_concerns",
    "living_orientations",
    "messages",
    "projects",
    "runtime_leases",
    "task_fronts",
    "task_offer_revisions",
    "task_offers",
    "task_relationship_events",
    "task_relationships",
    "weight_checkpoints",
    "weight_state",
)

if TYPE_CHECKING:
    from pulse_system.core.runtime.publication import (
        RuntimeBootstrapPermit,
        RuntimePublicationPermit,
        RuntimeRecoveryPermit,
    )

from pulse_system.core.types import (
    ActivityCenter,
    ActivityCenterBundle,
    ActivityCenterStatus,
    ActivityKind,
    ActivityOrigin,
    CausalEventDomain,
    CausalEventKind,
    CausalEventSource,
    CausalEventStatus,
    CenterLane,
    CenterMembership,
    CenterReservation,
    CenterReservationOutcome,
    CenterReservationState,
    CenterScheduleDecision,
    CenterScheduleState,
    Connection,
    ConnectionType,
    Engram,
    EngramMetadata,
    EngramStatus,
    HabitatSubscription,
    HabitatSubscriptionStatus,
    LivingConcern,
    LivingConcernDisposition,
    LivingOrientation,
    LivingOrientationState,
    Message,
    MessageRole,
    MembershipRelation,
    Project,
    RuntimeLease,
    RuntimeLeaseConflictError,
    RuntimeLeaseLostError,
    RuntimeLeaseState,
    TaskFront,
    TaskFrontBundle,
    TaskFrontStatus,
    TaskOffer,
    TaskOfferDecision,
    TaskOfferRevision,
    TaskOfferStatus,
    TaskRelationship,
    TaskRelationshipAction,
    TaskRelationshipActorKind,
    TaskRelationshipEvent,
    TaskRelationshipStatus,
    center_lane_for_activity_kind,
    session_name,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ts(dt: datetime) -> str:
    return dt.isoformat()


def _parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _require_utc(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _require_lease_ttl(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError("ttl_sec must be a finite number in [5, 3600]")
    ttl_sec = float(value)
    if not math.isfinite(ttl_sec) or not 5.0 <= ttl_sec <= 3600.0:
        raise ValueError("ttl_sec must be a finite number in [5, 3600]")
    return ttl_sec


def _require_owner_id(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("owner_id must be a non-empty string")
    return value


def _require_epoch(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("epoch must be an integer >= 1")
    return value


def _enum_value(value, enum_type, field_name: str):
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        allowed = ", ".join(item.value for item in enum_type)
        raise ValueError(f"{field_name} must be one of: {allowed}") from exc


_F = TypeVar("_F", bound=Callable)

_MUTATING_SQL_PREFIXES = frozenset(
    {
        "ALTER",
        "ANALYZE",
        "ATTACH",
        "CREATE",
        "DELETE",
        "DETACH",
        "DROP",
        "INSERT",
        "REINDEX",
        "REPLACE",
        "UPDATE",
        "VACUUM",
    }
)
_MUTATING_WITH_TOKEN = re.compile(
    r"\b(?:INSERT|UPDATE|DELETE|REPLACE)\b",
    re.IGNORECASE,
)
_SQL_LEADING_COMMENT = re.compile(
    r"\A(?:\s+|--[^\r\n]*(?:\r?\n|\Z)|/\*.*?\*/)*",
    re.DOTALL,
)
_RUNTIME_LEASE_CONTROL = object()
_MISSING_RUNTIME_AUTHORITY = object()


def _sql_mutates(sql: object) -> bool:
    """Classify SQLite statements whose execution can change durable state."""

    if not isinstance(sql, str):
        return True
    normalized = _SQL_LEADING_COMMENT.sub("", sql, count=1).lstrip()
    if not normalized:
        return False
    first = normalized.split(None, 1)[0].rstrip(";").upper()
    if first in _MUTATING_SQL_PREFIXES:
        return True
    if first == "WITH":
        return _MUTATING_WITH_TOKEN.search(normalized) is not None
    if first == "PRAGMA":
        # Read-only PRAGMAs have no assignment and no function-style argument.
        return "=" in normalized or "(" in normalized
    return False


class _RuntimeGuardedConnection(sqlite3.Connection):
    """Fence every SQLite mutation through its physical commit or rollback.

    A method-level precheck leaves a revoke race between the check and SQLite.
    The first mutating statement therefore opens a counted transaction owner
    that remains visible until commit/rollback.  Commit performs the final
    admission check: revoke in the statement→commit gap rolls the transaction
    back, while a commit admitted first may finish as explicit pre-revoke work.
    Reads and transaction-control statements remain available while quiescing
    so recovery can inspect state.
    """

    _runtime_write_guard_factory: Callable[[], Any] | None = None
    _runtime_transaction_dirty: bool = False
    _runtime_transaction_guard: Any | None = None

    def bind_runtime_write_guard_factory(
        self,
        factory: Callable[[], Any],
    ) -> None:
        if not callable(factory):
            raise ValueError("runtime write guard factory must be callable")
        if self._runtime_write_guard_factory is not None:
            raise RuntimeError("runtime write guard factory is already bound")
        self._runtime_write_guard_factory = factory

    def _guard_for(self, sql: object):
        if not _sql_mutates(sql):
            return nullcontext()
        return self._mutation_guard()

    def _mutation_guard(self):
        factory = self._runtime_write_guard_factory
        if factory is None:
            return nullcontext()
        return factory()

    def _execute_mutation(self, operation: Callable[[], Any]):
        transaction_guard = self._runtime_transaction_guard
        if transaction_guard is not None:
            return operation()

        transaction_guard = self._mutation_guard()
        transaction_guard.__enter__()
        try:
            result = operation()
        except BaseException as exc:
            transaction_guard.__exit__(type(exc), exc, exc.__traceback__)
            raise
        if self.in_transaction:
            self._runtime_transaction_guard = transaction_guard
            self._runtime_transaction_dirty = True
        else:
            transaction_guard.__exit__(None, None, None)
        return result

    def _finish_runtime_transaction_guard(self) -> None:
        transaction_guard = self._runtime_transaction_guard
        self._runtime_transaction_guard = None
        self._runtime_transaction_dirty = False
        if transaction_guard is not None:
            transaction_guard.__exit__(None, None, None)

    def execute(self, sql, parameters=(), /):  # type: ignore[override]
        if not _sql_mutates(sql):
            return super().execute(sql, parameters)
        return self._execute_mutation(
            lambda: super(_RuntimeGuardedConnection, self).execute(sql, parameters)
        )

    def executemany(self, sql, seq_of_parameters, /):  # type: ignore[override]
        if not _sql_mutates(sql):
            return super().executemany(sql, seq_of_parameters)
        return self._execute_mutation(
            lambda: super(_RuntimeGuardedConnection, self).executemany(
                sql,
                seq_of_parameters,
            )
        )

    def executescript(self, sql_script, /):  # type: ignore[override]
        if not _sql_mutates(sql_script):
            return super().executescript(sql_script)
        # sqlite3.executescript commits a pending transaction implicitly.
        # Route that boundary through our explicit commit admission first.
        if self._runtime_transaction_guard is not None:
            self.commit()
        return self._execute_mutation(
            lambda: super(_RuntimeGuardedConnection, self).executescript(sql_script)
        )

    def commit(self):  # type: ignore[override]
        """Fence the physical durability boundary, not only its SQL statements.

        A mutating statement can return before its transaction is committed.
        If revoke wins in that gap, the pending transaction is rolled back;
        if commit is admitted first, its potentially blocking physical work is
        counted as a pre-revoke publication owner until it returns.
        """

        if self._runtime_transaction_guard is None:
            return super().commit()
        try:
            with self._mutation_guard():
                result = super().commit()
        except BaseException:
            try:
                super().rollback()
            finally:
                self._finish_runtime_transaction_guard()
            raise
        self._finish_runtime_transaction_guard()
        return result

    def rollback(self):  # type: ignore[override]
        try:
            return super().rollback()
        finally:
            self._finish_runtime_transaction_guard()

    def close(self):  # type: ignore[override]
        try:
            if self._runtime_transaction_guard is not None:
                super().rollback()
        finally:
            self._finish_runtime_transaction_guard()
        return super().close()


def _locked(fn: _F) -> _F:
    """Serialize a Storage method across threads.

    The pulse engine runs ticks in a worker thread (asyncio.to_thread) while
    the front agent and clone sessions run on the event-loop thread; each
    method is one atomic unit of work (statements + commit).
    """

    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return fn(self, *args, **kwargs)

    return wrapper  # type: ignore[return-value]


class Storage:
    """SQLite-backed storage for engrams and connections.

    Thread-safe: a re-entrant lock serializes all public methods, and the
    sqlite connection is created with check_same_thread=False so it can be
    used from the engine's worker thread.
    """

    def __init__(self, db_path: str | Path = ":memory:"):
        self._lock = threading.RLock()
        self._harness_last_prune_at: datetime | None = None
        self._runtime_publication_permit: RuntimePublicationPermit | None = None
        self._runtime_write_context = threading.local()
        migrator = SchemaMigrator(
            db_path,
            required_tables=_STORAGE_BOOTSTRAP_TABLES,
        )
        self._conn = sqlite3.connect(
            str(db_path),
            check_same_thread=False,
            factory=_RuntimeGuardedConnection,
        )
        try:
            if not isinstance(self._conn, _RuntimeGuardedConnection):
                raise RuntimeError("runtime-guarded SQLite connection was not created")
            self._conn.bind_runtime_write_guard_factory(self._runtime_write_guard)
            # Inspection and any legacy backup deliberately precede WAL mode
            # changes and every schema statement.
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.execute("PRAGMA foreign_keys=ON")
            plan = migrator.inspect(self._conn)
            self._validate_schema_preconditions()
            backup_path = migrator.backup_if_needed(self._conn, plan)
            self._conn.execute("PRAGMA journal_mode=WAL")
            if plan.needs_work:
                try:
                    self._conn.execute("BEGIN IMMEDIATE")
                    self._init_schema(commit=False)
                    if plan.needs_migration:
                        applied = migrator.apply_in_transaction(self._conn, plan)
                        repaired = ()
                    else:
                        applied = ()
                        repaired = migrator.repair_in_transaction(self._conn, plan)
                    self._conn.commit()
                except BaseException:
                    if self._conn.in_transaction:
                        self._conn.rollback()
                    raise
            else:
                applied = ()
                repaired = ()
            self.schema_migration_result = SchemaMigrationResult(
                from_version=plan.current_version,
                to_version=TARGET_SCHEMA_VERSION,
                applied_versions=applied,
                backup_path=backup_path,
                repaired_tables=repaired,
            )
        except BaseException:
            self._conn.close()
            raise

    def bind_runtime_publication_permit(
        self,
        permit: RuntimePublicationPermit,
    ) -> None:
        """Bind all ordinary durable writes to one Runtime generation.

        Storage remains usable as an offline substrate when no permit is
        bound.  Runtime binds exactly once, immediately after acquiring its
        owner lease and before constructing any Harness ledger.
        """

        # Import lazily: importing the runtime package while Storage itself is
        # being imported would traverse DurableCenterScheduler back to here.
        from pulse_system.core.runtime.publication import RuntimePublicationPermit

        if not isinstance(permit, RuntimePublicationPermit):
            raise ValueError("permit must be a RuntimePublicationPermit")
        with self._lock:
            if self._runtime_publication_permit is not None:
                raise RuntimeError("Runtime publication permit is already bound")
            permit.assert_publication()
            self._runtime_publication_permit = permit

    def _runtime_write_guard(self):
        """Return the typed authority for the next mutating SQL statement."""

        from pulse_system.core.runtime.publication import (
            RuntimeBootstrapPermit,
            RuntimeRecoveryPermit,
        )

        authority = getattr(
            self._runtime_write_context,
            "authority",
            _MISSING_RUNTIME_AUTHORITY,
        )
        if authority is _RUNTIME_LEASE_CONTROL:
            return nullcontext()
        if isinstance(authority, (RuntimeBootstrapPermit, RuntimeRecoveryPermit)):
            return authority.transaction_guard()
        if authority is not _MISSING_RUNTIME_AUTHORITY:
            raise RuntimeError("invalid Runtime write authority scope")
        permit = self._runtime_publication_permit
        if permit is None:
            return nullcontext()
        return permit.transaction_guard()

    @contextmanager
    def _runtime_authority_scope(
        self,
        authority: RuntimeBootstrapPermit | RuntimeRecoveryPermit | object,
    ) -> Iterator[None]:
        """Install one non-ordinary authority on the current worker only."""

        from pulse_system.core.runtime.publication import (
            RuntimeBootstrapPermit,
            RuntimeRecoveryPermit,
        )

        if authority is not _RUNTIME_LEASE_CONTROL and not isinstance(
            authority,
            (RuntimeBootstrapPermit, RuntimeRecoveryPermit),
        ):
            raise ValueError("invalid Runtime write authority")
        previous = getattr(
            self._runtime_write_context,
            "authority",
            _MISSING_RUNTIME_AUTHORITY,
        )
        if previous is not _MISSING_RUNTIME_AUTHORITY and previous is not authority:
            raise RuntimeError("cannot replace a nested Runtime write authority")
        self._runtime_write_context.authority = authority
        try:
            yield
        finally:
            if previous is _MISSING_RUNTIME_AUTHORITY:
                try:
                    del self._runtime_write_context.authority
                except AttributeError:
                    pass
            else:
                self._runtime_write_context.authority = previous

    def _assert_runtime_publication(
        self,
    ) -> None:
        """Fail before expensive preparation; SQL still enforces the boundary."""

        guard = self._runtime_write_guard()
        with guard:
            return

    def _validate_schema_preconditions(self) -> None:
        """Read-only checks that must run even when the target schema is complete."""

        # The partial uniqueness invariant is stronger than a legacy database
        # containing duplicate in-flight generations.  Do not rewrite either
        # row: fail before backup/schema mutation and make the operator decision
        # explicit instead of allowing SQLite to report an opaque index error.
        has_generation_table = self._conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'generation_transitions'"
        ).fetchone() is not None
        if has_generation_table:
            duplicate_generation = self._conn.execute(
                "SELECT predecessor_id, COUNT(*), GROUP_CONCAT(id) "
                "FROM generation_transitions "
                "WHERE state IN ('prepared', 'summarizing', 'rotating', 'uncertain') "
                "GROUP BY predecessor_id HAVING COUNT(*) > 1 LIMIT 1"
            ).fetchone()
            if duplicate_generation is not None:
                predecessor_id, count, generation_ids = duplicate_generation
                raise sqlite3.IntegrityError(
                    "causal migration blocked: predecessor "
                    f"{predecessor_id!r} has {count} nonterminal/uncertain "
                    "generation transitions "
                    f"({generation_ids}); no historical rows were rewritten"
                )
        return None

    def _init_schema(self, *, commit: bool = True) -> None:
        self._validate_schema_preconditions()
        had_factory_connections = self._conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'factory_connections'"
        ).fetchone() is not None
        execute_sql_script(self._conn, """
            CREATE TABLE IF NOT EXISTS engrams (
                id TEXT PRIMARY KEY,
                project_id TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                last_pulse_at TEXT,
                total_pulses INTEGER NOT NULL DEFAULT 0,
                recent_activity REAL NOT NULL DEFAULT 0.0,
                self_excitability REAL NOT NULL DEFAULT 0.1,
                token_count INTEGER NOT NULL DEFAULT 0,
                substrate_binding TEXT,
                name TEXT,
                name_origin TEXT NOT NULL DEFAULT 'auto',
                nickname TEXT
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                engram_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                source_engram_id TEXT,
                FOREIGN KEY (engram_id) REFERENCES engrams(id)
            );

            CREATE INDEX IF NOT EXISTS idx_messages_engram
                ON messages(engram_id);

            CREATE TABLE IF NOT EXISTS connections (
                from_id TEXT NOT NULL,
                to_id TEXT NOT NULL,
                weight REAL NOT NULL,
                conn_type TEXT NOT NULL DEFAULT 'excitatory',
                created_at TEXT NOT NULL,
                last_activated_at TEXT NOT NULL,
                learned_weight REAL,
                PRIMARY KEY (from_id, to_id),
                FOREIGN KEY (from_id) REFERENCES engrams(id),
                FOREIGN KEY (to_id) REFERENCES engrams(id)
            );

            CREATE INDEX IF NOT EXISTS idx_conn_from ON connections(from_id);
            CREATE INDEX IF NOT EXISTS idx_conn_to ON connections(to_id);

            CREATE TABLE IF NOT EXISTS factory_connections (
                from_id TEXT NOT NULL,
                to_id TEXT NOT NULL,
                weight REAL NOT NULL,
                conn_type TEXT NOT NULL DEFAULT 'excitatory',
                created_at TEXT NOT NULL,
                last_activated_at TEXT NOT NULL,
                PRIMARY KEY (from_id, to_id),
                FOREIGN KEY (from_id) REFERENCES engrams(id),
                FOREIGN KEY (to_id) REFERENCES engrams(id)
            );

            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                workspace_path TEXT,
                created_at TEXT NOT NULL,
                index_engram_id TEXT
            );

            CREATE TABLE IF NOT EXISTS delegations (
                id TEXT PRIMARY KEY,
                caller_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                task TEXT NOT NULL,
                mode TEXT NOT NULL,
                contract TEXT,
                task_embedding TEXT,
                result_summary TEXT,
                outcome TEXT,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                group_id TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_deleg_caller
                ON delegations(caller_id);

            CREATE TABLE IF NOT EXISTS component_slots (
                component TEXT NOT NULL,
                engram_id TEXT NOT NULL,
                slot INTEGER NOT NULL,
                PRIMARY KEY (component, slot),
                UNIQUE (component, engram_id)
            );

            CREATE TABLE IF NOT EXISTS component_state (
                component TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS weight_state (
                component TEXT NOT NULL,
                layer TEXT NOT NULL CHECK(layer IN ('factory', 'field')),
                state TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (component, layer)
            );

            CREATE TABLE IF NOT EXISTS weight_checkpoints (
                id TEXT PRIMARY KEY,
                label TEXT,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS activity_centers (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL CHECK(kind IN (
                    'task', 'hobby', 'life_project', 'relationship',
                    'exploration', 'practice', 'expression', 'rest', 'other'
                )),
                title TEXT NOT NULL
                    CHECK(length(trim(title)) BETWEEN 1 AND 120),
                description TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL CHECK(status IN (
                    'active', 'dormant', 'paused', 'completed', 'archived'
                )),
                origin TEXT NOT NULL CHECK(origin IN (
                    'user', 'self', 'shared', 'system'
                )),
                autonomy REAL NOT NULL CHECK(
                    typeof(autonomy) IN ('real', 'integer')
                    AND autonomy >= 0.0 AND autonomy <= 1.0
                ),
                project_id TEXT,
                focal_engram_id TEXT,
                focal_relation TEXT NOT NULL DEFAULT 'focal'
                    CHECK(focal_relation = 'focal'),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_active_at TEXT,
                UNIQUE(id, focal_engram_id),
                FOREIGN KEY (project_id) REFERENCES projects(id),
                FOREIGN KEY (focal_engram_id) REFERENCES engrams(id),
                FOREIGN KEY (id, focal_engram_id, focal_relation)
                    REFERENCES center_memberships(
                        center_id, engram_id, relation
                    ) DEFERRABLE INITIALLY DEFERRED
            );

            CREATE INDEX IF NOT EXISTS idx_activity_centers_kind_status
                ON activity_centers(kind, status);
            CREATE INDEX IF NOT EXISTS idx_activity_centers_project
                ON activity_centers(project_id);
            CREATE INDEX IF NOT EXISTS idx_activity_centers_focal
                ON activity_centers(focal_engram_id);

            CREATE TABLE IF NOT EXISTS center_memberships (
                center_id TEXT NOT NULL,
                engram_id TEXT NOT NULL,
                relation TEXT NOT NULL CHECK(relation IN (
                    'focal', 'participant', 'shared'
                )),
                created_at TEXT NOT NULL,
                PRIMARY KEY (center_id, engram_id),
                UNIQUE(center_id, engram_id, relation),
                FOREIGN KEY (center_id) REFERENCES activity_centers(id)
                    ON DELETE CASCADE,
                FOREIGN KEY (engram_id) REFERENCES engrams(id)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_center_one_focal
                ON center_memberships(center_id) WHERE relation = 'focal';
            CREATE INDEX IF NOT EXISTS idx_center_memberships_engram
                ON center_memberships(engram_id);

            CREATE TABLE IF NOT EXISTS task_fronts (
                id TEXT PRIMARY KEY,
                center_id TEXT NOT NULL UNIQUE,
                focal_engram_id TEXT NOT NULL,
                focal_relation TEXT NOT NULL DEFAULT 'focal'
                    CHECK(focal_relation = 'focal'),
                title TEXT NOT NULL
                    CHECK(length(trim(title)) BETWEEN 1 AND 120),
                status TEXT NOT NULL CHECK(status IN (
                    'open', 'closed', 'archived'
                )),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_opened_at TEXT NOT NULL,
                FOREIGN KEY (center_id, focal_engram_id)
                    REFERENCES activity_centers(id, focal_engram_id)
                    DEFERRABLE INITIALLY DEFERRED,
                FOREIGN KEY (center_id, focal_engram_id, focal_relation)
                    REFERENCES center_memberships(
                        center_id, engram_id, relation
                    ) DEFERRABLE INITIALLY DEFERRED
            );

            CREATE INDEX IF NOT EXISTS idx_task_fronts_status
                ON task_fronts(status);
            CREATE INDEX IF NOT EXISTS idx_task_fronts_focal
                ON task_fronts(focal_engram_id);

            CREATE TRIGGER IF NOT EXISTS task_front_requires_task_center_insert
            BEFORE INSERT ON task_fronts
            WHEN COALESCE(
                (SELECT kind FROM activity_centers WHERE id = NEW.center_id),
                ''
            ) <> 'task'
            BEGIN
                SELECT RAISE(
                    ABORT, 'TaskFront must reference a task ActivityCenter'
                );
            END;

            CREATE TRIGGER IF NOT EXISTS task_front_requires_task_center_update
            BEFORE UPDATE OF center_id ON task_fronts
            WHEN COALESCE(
                (SELECT kind FROM activity_centers WHERE id = NEW.center_id),
                ''
            ) <> 'task'
            BEGIN
                SELECT RAISE(
                    ABORT, 'TaskFront must reference a task ActivityCenter'
                );
            END;
        """)
        execute_sql_script(self._conn, """
            CREATE TABLE IF NOT EXISTS causal_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                causal_id TEXT NOT NULL,
                parent_event_id TEXT,
                world_id TEXT NOT NULL,
                engram_id TEXT,
                center_id TEXT,
                flow TEXT CHECK(flow IS NULL OR flow IN (
                    'content', 'spectrum', 'tunnel'
                )),
                domain TEXT NOT NULL CHECK(domain IN (
                    'pulse', 'harness', 'world', 'habitat',
                    'generation', 'system'
                )),
                kind TEXT NOT NULL CHECK(kind IN (
                    'stimulus', 'spontaneous', 'pulse', 'propagation',
                    'tool_call', 'tool_result', 'habitat_observation',
                    'habitat_action', 'habitat_consequence',
                    'delegation_request', 'delegation_result',
                    'generation_transition', 'assistant_result', 'system'
                )),
                source TEXT NOT NULL CHECK(source IN (
                    'user', 'self', 'habitat', 'sensory', 'propagation',
                    'delegation', 'system'
                )),
                status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN (
                    'queued', 'running', 'settled', 'failed', 'uncertain',
                    'reconciled', 'cancelled'
                )),
                content TEXT,
                metadata TEXT NOT NULL DEFAULT '{}',
                idempotency_key TEXT UNIQUE,
                attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                settled_at TEXT,
                resolution TEXT CHECK(resolution IS NULL OR resolution IN (
                    'acknowledged', 'cancelled', 'superseded'
                )),
                resolution_note TEXT,
                FOREIGN KEY (parent_event_id) REFERENCES causal_events(id),
                FOREIGN KEY (engram_id) REFERENCES engrams(id),
                FOREIGN KEY (center_id) REFERENCES activity_centers(id)
            );

            CREATE INDEX IF NOT EXISTS idx_causal_events_world_idempotency
                ON causal_events(world_id, idempotency_key);
            CREATE INDEX IF NOT EXISTS idx_causal_events_status_seq
                ON causal_events(status, seq);
            CREATE INDEX IF NOT EXISTS idx_causal_events_engram_status_seq
                ON causal_events(engram_id, status, seq);
            CREATE INDEX IF NOT EXISTS idx_causal_events_causal
                ON causal_events(causal_id);
            CREATE INDEX IF NOT EXISTS idx_causal_events_parent
                ON causal_events(parent_event_id);

            CREATE TABLE IF NOT EXISTS dendritic_input_policy_snapshots (
                event_id TEXT PRIMARY KEY,
                world_id TEXT NOT NULL,
                engram_id TEXT NOT NULL,
                policy_version TEXT NOT NULL CHECK(
                    policy_version = 'dendritic-window.v1'
                ),
                base_silence_threshold_seconds REAL NOT NULL CHECK(
                    typeof(base_silence_threshold_seconds) IN ('real', 'integer')
                    AND base_silence_threshold_seconds >= 0
                ),
                base_max_wait_seconds REAL NOT NULL CHECK(
                    typeof(base_max_wait_seconds) IN ('real', 'integer')
                    AND base_max_wait_seconds >= 0
                ),
                wait_modifier REAL NOT NULL CHECK(
                    typeof(wait_modifier) IN ('real', 'integer')
                    AND wait_modifier > 0
                ),
                silence_threshold_seconds REAL NOT NULL CHECK(
                    typeof(silence_threshold_seconds) IN ('real', 'integer')
                    AND silence_threshold_seconds >= 0
                ),
                max_wait_seconds REAL NOT NULL CHECK(
                    typeof(max_wait_seconds) IN ('real', 'integer')
                    AND max_wait_seconds >= 0
                ),
                recorded_at TEXT NOT NULL,
                FOREIGN KEY (event_id) REFERENCES causal_events(id),
                FOREIGN KEY (engram_id) REFERENCES engrams(id)
            );

            CREATE INDEX IF NOT EXISTS idx_dendritic_input_policy_engram
                ON dendritic_input_policy_snapshots(engram_id, recorded_at);

            CREATE TABLE IF NOT EXISTS dendritic_windows (
                id TEXT PRIMARY KEY CHECK(length(id) = 64),
                world_id TEXT NOT NULL,
                formation_engram_id TEXT NOT NULL,
                policy_version TEXT NOT NULL CHECK(
                    policy_version = 'dendritic-window.v1'
                ),
                event_set_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(event_set_sha256) = 64),
                event_count INTEGER NOT NULL CHECK(event_count BETWEEN 1 AND 500),
                base_silence_threshold_seconds REAL NOT NULL CHECK(
                    typeof(base_silence_threshold_seconds) IN ('real', 'integer')
                    AND base_silence_threshold_seconds >= 0
                ),
                base_max_wait_seconds REAL NOT NULL CHECK(
                    typeof(base_max_wait_seconds) IN ('real', 'integer')
                    AND base_max_wait_seconds >= 0
                ),
                wait_modifier REAL NOT NULL CHECK(
                    typeof(wait_modifier) IN ('real', 'integer')
                    AND wait_modifier > 0
                ),
                silence_threshold_seconds REAL NOT NULL CHECK(
                    typeof(silence_threshold_seconds) IN ('real', 'integer')
                    AND silence_threshold_seconds >= 0
                ),
                max_wait_seconds REAL NOT NULL CHECK(
                    typeof(max_wait_seconds) IN ('real', 'integer')
                    AND max_wait_seconds >= 0
                ),
                window_opened_at TEXT NOT NULL,
                last_input_at TEXT NOT NULL,
                window_closed_at TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                observed_event_seq INTEGER NOT NULL CHECK(
                    observed_event_seq >= 1
                ),
                created_at TEXT NOT NULL,
                FOREIGN KEY (formation_engram_id) REFERENCES engrams(id),
                CHECK(window_opened_at <= last_input_at),
                CHECK(last_input_at <= window_closed_at),
                CHECK(window_closed_at <= observed_at),
                CHECK(observed_at <= created_at)
            );

            CREATE INDEX IF NOT EXISTS idx_dendritic_windows_formation_closed
                ON dendritic_windows(formation_engram_id, window_closed_at);

            CREATE TABLE IF NOT EXISTS dendritic_window_members (
                window_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 499),
                event_id TEXT NOT NULL UNIQUE,
                event_seq INTEGER NOT NULL CHECK(event_seq >= 1),
                arrived_at TEXT NOT NULL,
                PRIMARY KEY (window_id, ordinal),
                FOREIGN KEY (window_id) REFERENCES dendritic_windows(id),
                FOREIGN KEY (event_id) REFERENCES causal_events(id)
            );

            CREATE INDEX IF NOT EXISTS idx_dendritic_window_members_seq
                ON dendritic_window_members(window_id, event_seq);

            CREATE TABLE IF NOT EXISTS dendritic_integrations (
                id TEXT PRIMARY KEY,
                world_id TEXT NOT NULL,
                formation_engram_id TEXT NOT NULL,
                center_id TEXT,
                aggregate_event_id TEXT NOT NULL UNIQUE,
                delivery_class TEXT NOT NULL CHECK(delivery_class IN (
                    'external', 'propagation'
                )),
                member_set_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(member_set_sha256) = 64),
                content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
                member_count INTEGER NOT NULL CHECK(
                    member_count BETWEEN 2 AND 64
                ),
                window_opened_at TEXT NOT NULL,
                window_closed_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (formation_engram_id) REFERENCES engrams(id),
                FOREIGN KEY (center_id) REFERENCES activity_centers(id),
                FOREIGN KEY (aggregate_event_id) REFERENCES causal_events(id)
            );

            CREATE INDEX IF NOT EXISTS idx_dendritic_integrations_formation_created
                ON dendritic_integrations(formation_engram_id, created_at);

            CREATE TABLE IF NOT EXISTS dendritic_integration_members (
                integration_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 63),
                event_id TEXT NOT NULL UNIQUE,
                event_seq INTEGER NOT NULL CHECK(event_seq >= 1),
                causal_id TEXT NOT NULL,
                source_identity TEXT NOT NULL CHECK(
                    length(source_identity) BETWEEN 1 AND 256
                ),
                content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
                arrived_at TEXT NOT NULL,
                PRIMARY KEY (integration_id, ordinal),
                FOREIGN KEY (integration_id) REFERENCES dendritic_integrations(id),
                FOREIGN KEY (event_id) REFERENCES causal_events(id)
            );

            CREATE INDEX IF NOT EXISTS idx_dendritic_members_integration_event
                ON dendritic_integration_members(integration_id, event_seq);

            CREATE TABLE IF NOT EXISTS dendritic_integration_windows (
                integration_id TEXT PRIMARY KEY,
                window_id TEXT NOT NULL,
                FOREIGN KEY (integration_id) REFERENCES dendritic_integrations(id),
                FOREIGN KEY (window_id) REFERENCES dendritic_windows(id)
            );

            CREATE INDEX IF NOT EXISTS idx_dendritic_integration_windows_window
                ON dendritic_integration_windows(window_id, integration_id);

            CREATE TABLE IF NOT EXISTS dendritic_legacy_integrations (
                integration_id TEXT PRIMARY KEY,
                source_schema_version INTEGER NOT NULL CHECK(
                    source_schema_version = 5
                ),
                evidence_class TEXT NOT NULL CHECK(
                    evidence_class = 'LEGACY_V5_NO_WINDOW'
                ),
                integration_created_at TEXT NOT NULL,
                FOREIGN KEY (integration_id) REFERENCES dendritic_integrations(id)
            );

            CREATE TRIGGER IF NOT EXISTS dendritic_input_policy_immutable_update
            BEFORE UPDATE ON dendritic_input_policy_snapshots
            BEGIN
                SELECT RAISE(ABORT, 'dendritic input policies are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_input_policy_immutable_delete
            BEFORE DELETE ON dendritic_input_policy_snapshots
            BEGIN
                SELECT RAISE(ABORT, 'dendritic input policies are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_integrations_immutable_update
            BEFORE UPDATE ON dendritic_integrations
            BEGIN
                SELECT RAISE(ABORT, 'dendritic integrations are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_integrations_immutable_delete
            BEFORE DELETE ON dendritic_integrations
            BEGIN
                SELECT RAISE(ABORT, 'dendritic integrations are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_members_immutable_update
            BEFORE UPDATE ON dendritic_integration_members
            BEGIN
                SELECT RAISE(ABORT, 'dendritic integration members are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_members_immutable_delete
            BEFORE DELETE ON dendritic_integration_members
            BEGIN
                SELECT RAISE(ABORT, 'dendritic integration members are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_windows_immutable_update
            BEFORE UPDATE ON dendritic_windows
            BEGIN
                SELECT RAISE(ABORT, 'dendritic windows are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_windows_immutable_delete
            BEFORE DELETE ON dendritic_windows
            BEGIN
                SELECT RAISE(ABORT, 'dendritic windows are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_window_members_immutable_update
            BEFORE UPDATE ON dendritic_window_members
            BEGIN
                SELECT RAISE(ABORT, 'dendritic window members are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_window_members_immutable_delete
            BEFORE DELETE ON dendritic_window_members
            BEGIN
                SELECT RAISE(ABORT, 'dendritic window members are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_integration_windows_immutable_update
            BEFORE UPDATE ON dendritic_integration_windows
            BEGIN
                SELECT RAISE(ABORT, 'dendritic integration-window bindings are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_integration_windows_immutable_delete
            BEFORE DELETE ON dendritic_integration_windows
            BEGIN
                SELECT RAISE(ABORT, 'dendritic integration-window bindings are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_legacy_integrations_closed_insert
            BEFORE INSERT ON dendritic_legacy_integrations
            BEGIN
                SELECT RAISE(ABORT, 'legacy dendritic integration set is migration-sealed');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_legacy_integrations_immutable_update
            BEFORE UPDATE ON dendritic_legacy_integrations
            BEGIN
                SELECT RAISE(ABORT, 'legacy dendritic integration evidence is immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS dendritic_legacy_integrations_immutable_delete
            BEFORE DELETE ON dendritic_legacy_integrations
            BEGIN
                SELECT RAISE(ABORT, 'legacy dendritic integration evidence is immutable');
            END;

            CREATE TABLE IF NOT EXISTS task_offers (
                id TEXT PRIMARY KEY,
                world_id TEXT NOT NULL,
                subject_engram_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN (
                    'pending', 'changes_requested', 'accepted', 'refused',
                    'withdrawn'
                )),
                current_revision INTEGER NOT NULL DEFAULT 1 CHECK(
                    typeof(current_revision) = 'integer'
                    AND current_revision >= 1
                ),
                task_front_id TEXT UNIQUE,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                decided_at TEXT,
                withdrawn_at TEXT,
                FOREIGN KEY (subject_engram_id) REFERENCES engrams(id),
                FOREIGN KEY (task_front_id) REFERENCES task_fronts(id)
                    DEFERRABLE INITIALLY DEFERRED,
                CHECK(
                    (status = 'accepted'
                        AND task_front_id IS NOT NULL
                        AND decided_at IS NOT NULL
                        AND withdrawn_at IS NULL)
                    OR (status = 'refused'
                        AND task_front_id IS NULL
                        AND decided_at IS NOT NULL
                        AND withdrawn_at IS NULL)
                    OR (status = 'withdrawn'
                        AND task_front_id IS NULL
                        AND decided_at IS NULL
                        AND withdrawn_at IS NOT NULL)
                    OR (status IN ('pending', 'changes_requested')
                        AND task_front_id IS NULL
                        AND decided_at IS NULL
                        AND withdrawn_at IS NULL)
                )
            );

            CREATE INDEX IF NOT EXISTS idx_task_offers_world_status
                ON task_offers(world_id, status, updated_at, id);
            CREATE INDEX IF NOT EXISTS idx_task_offers_subject_status
                ON task_offers(
                    world_id, subject_engram_id, status, updated_at, id
                );

            CREATE TABLE IF NOT EXISTS task_offer_revisions (
                offer_id TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK(
                    typeof(revision) = 'integer' AND revision >= 1
                ),
                content TEXT NOT NULL CHECK(
                    length(trim(content)) >= 1 AND length(content) <= 12000
                ),
                title TEXT NOT NULL CHECK(
                    length(trim(title)) BETWEEN 1 AND 120
                ),
                project_id TEXT,
                latest_offer_event_id TEXT NOT NULL UNIQUE,
                decision TEXT CHECK(decision IS NULL OR decision IN (
                    'accept', 'refuse', 'request_changes'
                )),
                subject_response TEXT CHECK(
                    subject_response IS NULL
                    OR length(subject_response) <= 4000
                ),
                decision_event_id TEXT UNIQUE,
                created_at TEXT NOT NULL,
                decided_at TEXT,
                PRIMARY KEY (offer_id, revision),
                FOREIGN KEY (offer_id) REFERENCES task_offers(id)
                    ON DELETE RESTRICT,
                FOREIGN KEY (project_id) REFERENCES projects(id),
                FOREIGN KEY (latest_offer_event_id) REFERENCES causal_events(id)
                    DEFERRABLE INITIALLY DEFERRED,
                FOREIGN KEY (decision_event_id) REFERENCES causal_events(id)
                    DEFERRABLE INITIALLY DEFERRED,
                CHECK(
                    (decision IS NULL
                        AND subject_response IS NULL
                        AND decision_event_id IS NULL
                        AND decided_at IS NULL)
                    OR (decision IS NOT NULL
                        AND decision_event_id IS NOT NULL
                        AND decided_at IS NOT NULL)
                ),
                CHECK(
                    decision IS NULL
                    OR decision <> 'request_changes'
                    OR (
                        subject_response IS NOT NULL
                        AND length(trim(subject_response)) >= 1
                    )
                )
            );

            CREATE INDEX IF NOT EXISTS idx_task_offer_revisions_offer
                ON task_offer_revisions(offer_id, revision);
            CREATE INDEX IF NOT EXISTS idx_task_offer_revisions_project
                ON task_offer_revisions(project_id, offer_id, revision);

            CREATE TABLE IF NOT EXISTS task_relationships (
                id TEXT PRIMARY KEY,
                world_id TEXT NOT NULL,
                accepted_offer_id TEXT NOT NULL UNIQUE,
                task_front_id TEXT NOT NULL UNIQUE,
                center_id TEXT NOT NULL UNIQUE,
                original_subject_engram_id TEXT NOT NULL,
                current_subject_engram_id TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN (
                    'active', 'paused', 'renegotiation_requested', 'exited'
                )),
                revision INTEGER NOT NULL CHECK(
                    typeof(revision) = 'integer' AND revision >= 1
                ),
                latest_terms_event_id TEXT,
                latest_subject_note TEXT CHECK(
                    latest_subject_note IS NULL
                    OR length(latest_subject_note) <= 4000
                ),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                exited_at TEXT,
                FOREIGN KEY (accepted_offer_id) REFERENCES task_offers(id)
                    ON DELETE RESTRICT,
                FOREIGN KEY (task_front_id) REFERENCES task_fronts(id)
                    ON DELETE RESTRICT,
                FOREIGN KEY (center_id) REFERENCES activity_centers(id)
                    ON DELETE RESTRICT,
                FOREIGN KEY (original_subject_engram_id) REFERENCES engrams(id),
                FOREIGN KEY (current_subject_engram_id) REFERENCES engrams(id),
                FOREIGN KEY (latest_terms_event_id) REFERENCES causal_events(id)
                    DEFERRABLE INITIALLY DEFERRED,
                CHECK(
                    (status = 'exited' AND exited_at IS NOT NULL)
                    OR (status <> 'exited' AND exited_at IS NULL)
                )
            );

            CREATE INDEX IF NOT EXISTS idx_task_relationships_world_status
                ON task_relationships(world_id, status, updated_at, id);
            CREATE INDEX IF NOT EXISTS idx_task_relationships_subject_status
                ON task_relationships(
                    world_id, current_subject_engram_id, status, updated_at, id
                );

            CREATE TABLE IF NOT EXISTS task_relationship_events (
                relationship_id TEXT NOT NULL,
                seq INTEGER NOT NULL CHECK(
                    typeof(seq) = 'integer' AND seq >= 1
                ),
                action TEXT NOT NULL CHECK(action IN (
                    'accepted', 'paused', 'renegotiation_requested',
                    'terms_proposed', 'resumed', 'exited', 'succession'
                )),
                actor_kind TEXT NOT NULL CHECK(actor_kind IN (
                    'subject', 'user', 'system'
                )),
                actor_id TEXT NOT NULL,
                before_status TEXT CHECK(
                    before_status IS NULL OR before_status IN (
                        'active', 'paused', 'renegotiation_requested', 'exited'
                    )
                ),
                after_status TEXT NOT NULL CHECK(after_status IN (
                    'active', 'paused', 'renegotiation_requested', 'exited'
                )),
                content TEXT CHECK(content IS NULL OR length(content) <= 12000),
                source_event_id TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (relationship_id, seq),
                UNIQUE (relationship_id, source_event_id),
                FOREIGN KEY (relationship_id) REFERENCES task_relationships(id)
                    ON DELETE RESTRICT,
                FOREIGN KEY (source_event_id) REFERENCES causal_events(id)
                    DEFERRABLE INITIALLY DEFERRED
            );

            CREATE INDEX IF NOT EXISTS idx_task_relationship_events_source
                ON task_relationship_events(source_event_id);

            CREATE TABLE IF NOT EXISTS harness_turns (
                id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                engram_id TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN (
                    'running', 'settled', 'failed', 'uncertain'
                )),
                cursor_before INTEGER NOT NULL CHECK(cursor_before >= 0),
                cursor_after INTEGER NOT NULL CHECK(cursor_after >= 0),
                input_message_id INTEGER,
                prompt_accepted INTEGER CHECK(
                    prompt_accepted IS NULL OR prompt_accepted IN (0, 1)
                ),
                session_id TEXT,
                session_file TEXT,
                result_event_id TEXT,
                error_code TEXT,
                error_phase TEXT,
                started_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                settled_at TEXT,
                FOREIGN KEY (event_id) REFERENCES causal_events(id),
                FOREIGN KEY (engram_id) REFERENCES engrams(id),
                FOREIGN KEY (result_event_id) REFERENCES causal_events(id),
                FOREIGN KEY (input_message_id) REFERENCES messages(id)
            );

            CREATE INDEX IF NOT EXISTS idx_harness_turns_event
                ON harness_turns(event_id);
            CREATE INDEX IF NOT EXISTS idx_harness_turns_engram_state
                ON harness_turns(engram_id, state);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_harness_turns_one_running
                ON harness_turns(event_id)
                WHERE state = 'running';
            CREATE UNIQUE INDEX IF NOT EXISTS idx_harness_turns_one_running_engram
                ON harness_turns(engram_id)
                WHERE state = 'running';

            CREATE TABLE IF NOT EXISTS harness_events (
                event_id TEXT PRIMARY KEY,
                turn_id TEXT NOT NULL,
                world_id TEXT NOT NULL,
                engram_id TEXT NOT NULL,
                seq INTEGER NOT NULL CHECK(seq >= 1),
                parent_event_id TEXT,
                kind TEXT NOT NULL CHECK(kind IN (
                    'turn_started', 'text_delta', 'reasoning_delta',
                    'tool_started', 'tool_progress', 'tool_completed',
                    'command_started', 'command_output', 'command_completed',
                    'file_change', 'usage', 'approval_requested',
                    'approval_resolved', 'control_requested',
                    'control_resolved', 'subagent_activity', 'turn_terminal',
                    'warning', 'event_gap'
                )),
                phase TEXT NOT NULL CHECK(phase IN (
                    'observe', 'start', 'stream', 'approval', 'control',
                    'terminal', 'recovery'
                )),
                source TEXT NOT NULL CHECK(source IN (
                    'pi_rpc', 'pulse_control', 'terminal', 'policy',
                    'task_subagent', 'recovery'
                )),
                status TEXT NOT NULL CHECK(status IN (
                    'running', 'completed', 'failed', 'cancelled',
                    'uncertain', 'redacted'
                )),
                occurred_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                payload_bytes INTEGER NOT NULL CHECK(payload_bytes >= 0),
                payload_digest TEXT NOT NULL,
                redacted INTEGER NOT NULL CHECK(redacted IN (0, 1)),
                truncated INTEGER NOT NULL CHECK(truncated IN (0, 1)),
                UNIQUE(turn_id, seq)
            );

            CREATE INDEX IF NOT EXISTS idx_harness_events_turn_seq
                ON harness_events(turn_id, seq);
            CREATE INDEX IF NOT EXISTS idx_harness_events_world_time
                ON harness_events(world_id, occurred_at, event_id);
            CREATE INDEX IF NOT EXISTS idx_harness_events_engram_time
                ON harness_events(engram_id, occurred_at, event_id);
            CREATE INDEX IF NOT EXISTS idx_harness_events_kind_status
                ON harness_events(kind, status, occurred_at);

            -- Payload-free audit trail for the life/control firewall.  This
            -- table is intentionally separate from both causal_events and
            -- harness_events: replaying either source must never recreate a
            -- life stimulus.  Retention is bounded by the append method.
            CREATE TABLE IF NOT EXISTS harness_control_observations (
                world_id TEXT NOT NULL,
                record_id TEXT NOT NULL,
                sequence INTEGER NOT NULL CHECK(sequence >= 1),
                stimulus_id TEXT NOT NULL,
                stimulus_class TEXT NOT NULL,
                declared_class TEXT NOT NULL,
                evidence_class TEXT NOT NULL,
                route TEXT NOT NULL CHECK(route = 'control_ledger'),
                reason_code TEXT NOT NULL,
                provenance_digest TEXT NOT NULL,
                external_effect_id TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY(world_id, record_id),
                UNIQUE(world_id, sequence)
            );

            CREATE INDEX IF NOT EXISTS idx_harness_control_world_sequence
                ON harness_control_observations(world_id, sequence);

            CREATE TABLE IF NOT EXISTS generation_transitions (
                id TEXT PRIMARY KEY,
                causal_id TEXT NOT NULL,
                event_id TEXT,
                predecessor_id TEXT NOT NULL,
                successor_id TEXT,
                state TEXT NOT NULL CHECK(state IN (
                    'prepared', 'summarizing', 'rotating', 'committed',
                    'failed', 'uncertain'
                )),
                summary_turn_id TEXT,
                error_code TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                settled_at TEXT,
                FOREIGN KEY (predecessor_id) REFERENCES engrams(id),
                FOREIGN KEY (successor_id) REFERENCES engrams(id),
                FOREIGN KEY (summary_turn_id) REFERENCES harness_turns(id),
                FOREIGN KEY (event_id) REFERENCES causal_events(id)
            );

            CREATE INDEX IF NOT EXISTS idx_generation_transitions_causal
                ON generation_transitions(causal_id);
            CREATE INDEX IF NOT EXISTS idx_generation_transitions_predecessor
                ON generation_transitions(predecessor_id);
            CREATE INDEX IF NOT EXISTS idx_generation_transitions_state
                ON generation_transitions(state);
            CREATE TABLE IF NOT EXISTS habitat_subscriptions (
                id TEXT PRIMARY KEY,
                world_id TEXT NOT NULL,
                engram_id TEXT NOT NULL,
                center_id TEXT,
                channel TEXT NOT NULL DEFAULT 'all',
                status TEXT NOT NULL DEFAULT 'active'
                    CHECK(status IN ('active', 'inactive')),
                last_fingerprint TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (engram_id) REFERENCES engrams(id),
                FOREIGN KEY (center_id) REFERENCES activity_centers(id)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_habitat_subscriptions_active_unique
                ON habitat_subscriptions(world_id, engram_id, channel)
                WHERE status = 'active';
            CREATE INDEX IF NOT EXISTS idx_habitat_subscriptions_engram
                ON habitat_subscriptions(engram_id, status);
            CREATE INDEX IF NOT EXISTS idx_habitat_subscriptions_world
                ON habitat_subscriptions(world_id, status);
            CREATE TABLE IF NOT EXISTS living_concerns (
                id TEXT PRIMARY KEY,
                center_id TEXT NOT NULL,
                owner_engram_id TEXT NOT NULL,
                content TEXT NOT NULL CHECK(
                    length(trim(content)) >= 1 AND length(content) <= 4000
                ),
                disposition TEXT NOT NULL CHECK(disposition IN (
                    'revisit', 'quiet', 'resolved'
                )),
                revisit_at TEXT,
                causal_id TEXT NOT NULL,
                source_event_id TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK(revision >= 1),
                last_reentry_event_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                resolved_at TEXT,
                FOREIGN KEY (center_id) REFERENCES activity_centers(id),
                FOREIGN KEY (owner_engram_id) REFERENCES engrams(id),
                FOREIGN KEY (source_event_id) REFERENCES causal_events(id),
                FOREIGN KEY (last_reentry_event_id) REFERENCES causal_events(id),
                CHECK (
                    (disposition = 'revisit' AND revisit_at IS NOT NULL
                        AND resolved_at IS NULL)
                    OR (disposition = 'quiet' AND revisit_at IS NULL
                        AND resolved_at IS NULL)
                    OR (disposition = 'resolved' AND revisit_at IS NULL
                        AND resolved_at IS NOT NULL)
                )
            );

            CREATE INDEX IF NOT EXISTS idx_living_concerns_due
                ON living_concerns(disposition, revisit_at, id);
            CREATE INDEX IF NOT EXISTS idx_living_concerns_center
                ON living_concerns(center_id, disposition, updated_at);
            CREATE INDEX IF NOT EXISTS idx_living_concerns_owner
                ON living_concerns(owner_engram_id, disposition, updated_at);

            CREATE TABLE IF NOT EXISTS living_orientations (
                id TEXT PRIMARY KEY,
                center_id TEXT NOT NULL,
                owner_engram_id TEXT NOT NULL,
                content TEXT NOT NULL CHECK(
                    length(trim(content)) >= 1 AND length(content) <= 4000
                ),
                state TEXT NOT NULL CHECK(state IN (
                    'open', 'resting', 'closed'
                )),
                causal_id TEXT NOT NULL,
                source_event_id TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK(
                    typeof(revision) = 'integer' AND revision >= 1
                ),
                engagement_count INTEGER NOT NULL DEFAULT 0 CHECK(
                    typeof(engagement_count) = 'integer'
                    AND engagement_count >= 0
                ),
                next_eligible_at TEXT,
                last_engagement_event_id TEXT,
                last_engaged_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                closed_at TEXT,
                FOREIGN KEY (center_id) REFERENCES activity_centers(id),
                FOREIGN KEY (owner_engram_id) REFERENCES engrams(id),
                FOREIGN KEY (source_event_id) REFERENCES causal_events(id),
                FOREIGN KEY (last_engagement_event_id)
                    REFERENCES causal_events(id),
                CHECK(
                    (state = 'open' AND closed_at IS NULL)
                    OR (state = 'resting'
                        AND next_eligible_at IS NULL
                        AND closed_at IS NULL)
                    OR (state = 'closed'
                        AND next_eligible_at IS NULL
                        AND closed_at IS NOT NULL)
                ),
                CHECK(
                    (engagement_count = 0
                        AND last_engagement_event_id IS NULL
                        AND last_engaged_at IS NULL)
                    OR (engagement_count >= 1
                        AND last_engagement_event_id IS NOT NULL
                        AND last_engaged_at IS NOT NULL)
                )
            );

            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_living_orientations_current
                ON living_orientations(center_id, owner_engram_id)
                WHERE state IN ('open', 'resting');
            CREATE INDEX IF NOT EXISTS idx_living_orientations_center
                ON living_orientations(center_id, state, updated_at, id);
            CREATE INDEX IF NOT EXISTS idx_living_orientations_owner
                ON living_orientations(owner_engram_id, state, updated_at, id);
            CREATE INDEX IF NOT EXISTS idx_living_orientations_eligibility
                ON living_orientations(
                    owner_engram_id, state, next_eligible_at,
                    engagement_count, id
                );
        """)
        # Defensive migrations for databases created before newer columns
        cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(delegations)"
        ).fetchall()]
        if "group_id" not in cols:
            self._conn.execute(
                "ALTER TABLE delegations ADD COLUMN group_id TEXT"
            )
        engram_cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(engrams)"
        ).fetchall()]
        if "substrate_binding" not in engram_cols:
            self._conn.execute(
                "ALTER TABLE engrams ADD COLUMN substrate_binding TEXT"
            )
        if "name" not in engram_cols:
            self._conn.execute(
                "ALTER TABLE engrams ADD COLUMN name TEXT"
            )
        if "name_origin" not in engram_cols:
            self._conn.execute(
                "ALTER TABLE engrams ADD COLUMN name_origin TEXT "
                "NOT NULL DEFAULT 'auto'"
            )
        if "nickname" not in engram_cols:
            self._conn.execute(
                "ALTER TABLE engrams ADD COLUMN nickname TEXT"
            )
        turn_cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(harness_turns)"
        ).fetchall()]
        if "input_message_id" not in turn_cols:
            self._conn.execute(
                "ALTER TABLE harness_turns ADD COLUMN input_message_id INTEGER"
            )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_harness_turns_input_message "
            "ON harness_turns(input_message_id)"
        )
        self._conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "idx_harness_turns_one_running_engram "
            "ON harness_turns(engram_id) WHERE state = 'running'"
        )
        event_cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(causal_events)"
        ).fetchall()]
        if "resolution_note" not in event_cols:
            self._conn.execute(
                "ALTER TABLE causal_events ADD COLUMN resolution_note TEXT"
            )
        # SQLite cannot add a CHECK constraint to an existing table.  These
        # additive guards give migrated databases the same durable enum/state
        # boundary as newly-created databases without rewriting history.
        execute_sql_script(self._conn, """
            CREATE TRIGGER IF NOT EXISTS causal_event_resolution_insert_guard
            BEFORE INSERT ON causal_events
            WHEN (
                NEW.resolution IS NOT NULL AND (
                    NEW.resolution NOT IN (
                        'acknowledged', 'cancelled', 'superseded'
                    ) OR NEW.status <> 'reconciled'
                )
            ) OR (
                NEW.resolution_note IS NOT NULL
                AND NEW.status <> 'reconciled'
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid causal event resolution');
            END;

            CREATE TRIGGER IF NOT EXISTS causal_event_resolution_update_guard
            BEFORE UPDATE OF status, resolution, resolution_note ON causal_events
            WHEN (
                NEW.resolution IS NOT NULL AND (
                    NEW.resolution NOT IN (
                        'acknowledged', 'cancelled', 'superseded'
                    ) OR NEW.status <> 'reconciled'
                )
            ) OR (
                NEW.resolution_note IS NOT NULL
                AND NEW.status <> 'reconciled'
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid causal event resolution');
            END;
        """)
        generation_cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(generation_transitions)"
        ).fetchall()]
        if "event_id" not in generation_cols:
            self._conn.execute(
                "ALTER TABLE generation_transitions ADD COLUMN event_id TEXT "
                "REFERENCES causal_events(id)"
            )
        self._conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "idx_generation_predecessor_nonterminal "
            "ON generation_transitions(predecessor_id) "
            "WHERE state IN ('prepared', 'summarizing', 'rotating', 'uncertain')"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS habitat_subscriptions ("
            "id TEXT PRIMARY KEY, world_id TEXT NOT NULL, engram_id TEXT NOT NULL, "
            "channel TEXT NOT NULL DEFAULT 'all', status TEXT NOT NULL DEFAULT 'active' "
            "CHECK(status IN ('active', 'inactive')), last_fingerprint TEXT, "
            "created_at TEXT NOT NULL, updated_at TEXT NOT NULL, "
            "FOREIGN KEY (engram_id) REFERENCES engrams(id))"
        )
        subscription_cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(habitat_subscriptions)"
        ).fetchall()]
        if "center_id" not in subscription_cols:
            self._conn.execute(
                "ALTER TABLE habitat_subscriptions ADD COLUMN center_id TEXT "
                "REFERENCES activity_centers(id)"
            )
        self._conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "idx_habitat_subscriptions_active_unique "
            "ON habitat_subscriptions(world_id, engram_id, channel) "
            "WHERE status = 'active'"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_habitat_subscriptions_engram "
            "ON habitat_subscriptions(engram_id, status)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_habitat_subscriptions_world "
            "ON habitat_subscriptions(world_id, status)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_habitat_subscriptions_center "
            "ON habitat_subscriptions(center_id, status)"
        )
        execute_sql_script(self._conn, """
            CREATE TABLE IF NOT EXISTS living_concerns (
                id TEXT PRIMARY KEY,
                center_id TEXT NOT NULL,
                owner_engram_id TEXT NOT NULL,
                content TEXT NOT NULL CHECK(
                    length(trim(content)) >= 1 AND length(content) <= 4000
                ),
                disposition TEXT NOT NULL CHECK(disposition IN (
                    'revisit', 'quiet', 'resolved'
                )),
                revisit_at TEXT,
                causal_id TEXT NOT NULL,
                source_event_id TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK(revision >= 1),
                last_reentry_event_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                resolved_at TEXT,
                FOREIGN KEY (center_id) REFERENCES activity_centers(id),
                FOREIGN KEY (owner_engram_id) REFERENCES engrams(id),
                FOREIGN KEY (source_event_id) REFERENCES causal_events(id),
                FOREIGN KEY (last_reentry_event_id) REFERENCES causal_events(id),
                CHECK (
                    (disposition = 'revisit' AND revisit_at IS NOT NULL
                        AND resolved_at IS NULL)
                    OR (disposition = 'quiet' AND revisit_at IS NULL
                        AND resolved_at IS NULL)
                    OR (disposition = 'resolved' AND revisit_at IS NULL
                        AND resolved_at IS NOT NULL)
                )
            );
            CREATE INDEX IF NOT EXISTS idx_living_concerns_due
                ON living_concerns(disposition, revisit_at, id);
            CREATE INDEX IF NOT EXISTS idx_living_concerns_center
                ON living_concerns(center_id, disposition, updated_at);
            CREATE INDEX IF NOT EXISTS idx_living_concerns_owner
                ON living_concerns(owner_engram_id, disposition, updated_at);
        """)
        connection_cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(connections)"
        ).fetchall()]
        if "learned_weight" not in connection_cols:
            self._conn.execute(
                "ALTER TABLE connections ADD COLUMN learned_weight REAL"
            )
        # Legacy databases persisted only one effective value. Preserve it as
        # the immutable reset point; provenance cannot be reconstructed after
        # the fact, and this migration is the only lossless choice.
        if not had_factory_connections:
            self._conn.execute(
                """INSERT OR IGNORE INTO factory_connections
                   (from_id, to_id, weight, conn_type, created_at,
                    last_activated_at)
                   SELECT from_id, to_id, weight, conn_type, created_at,
                          last_activated_at
                   FROM connections"""
            )
        # Durable scheduling tables are additive: an
        # existing world retains every historical causal and Harness row.
        execute_sql_script(self._conn, """
            CREATE TABLE IF NOT EXISTS runtime_leases (
                scope TEXT PRIMARY KEY CHECK(scope = 'pulse_world'),
                owner_id TEXT NOT NULL CHECK(length(trim(owner_id)) >= 1),
                epoch INTEGER NOT NULL CHECK(
                    typeof(epoch) = 'integer' AND epoch >= 1
                ),
                state TEXT NOT NULL CHECK(state IN ('active', 'released')),
                acquired_at TEXT NOT NULL,
                renewed_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                released_at TEXT,
                CHECK(
                    (state = 'active' AND released_at IS NULL)
                    OR (state = 'released' AND released_at IS NOT NULL)
                )
            );

            CREATE TABLE IF NOT EXISTS center_schedule_state (
                center_id TEXT PRIMARY KEY,
                lane TEXT NOT NULL CHECK(lane IN ('work', 'life')),
                decision TEXT NOT NULL CHECK(decision IN (
                    'idle', 'admitted', 'waiting', 'blocked'
                )),
                reason TEXT NOT NULL CHECK(reason IN (
                    'lane_reservation', 'fair_share', 'effective_score',
                    'budget_deferred', 'center_inactive', 'no_ready_event'
                )),
                starvation_debt INTEGER NOT NULL DEFAULT 0 CHECK(
                    typeof(starvation_debt) = 'integer'
                    AND starvation_debt >= 0
                ),
                waiting_since TEXT,
                last_admitted_at TEXT,
                last_decision_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (center_id) REFERENCES activity_centers(id),
                CHECK(
                    (decision = 'waiting' AND starvation_debt >= 1
                        AND waiting_since IS NOT NULL
                        AND reason = 'budget_deferred')
                    OR (decision = 'admitted' AND starvation_debt = 0
                        AND waiting_since IS NULL AND last_admitted_at IS NOT NULL
                        AND reason IN (
                            'lane_reservation', 'fair_share', 'effective_score'
                        ))
                    OR (decision = 'idle' AND starvation_debt = 0
                        AND waiting_since IS NULL AND reason = 'no_ready_event')
                    OR (decision = 'blocked' AND starvation_debt = 0
                        AND waiting_since IS NULL AND reason = 'center_inactive')
                )
            );

            CREATE INDEX IF NOT EXISTS idx_center_schedule_state_lane_decision
                ON center_schedule_state(lane, decision, updated_at, center_id);

            CREATE TABLE IF NOT EXISTS center_reservations (
                id TEXT PRIMARY KEY,
                world_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                engram_id TEXT NOT NULL,
                center_id TEXT,
                lane TEXT NOT NULL CHECK(lane IN ('work', 'life', 'unbound')),
                owner_id TEXT NOT NULL CHECK(length(trim(owner_id)) >= 1),
                lease_epoch INTEGER NOT NULL CHECK(
                    typeof(lease_epoch) = 'integer' AND lease_epoch >= 1
                ),
                state TEXT NOT NULL CHECK(state IN (
                    'held', 'settled', 'abandoned'
                )),
                outcome TEXT CHECK(outcome IS NULL OR outcome IN (
                    'succeeded', 'failed', 'skipped', 'uncertain',
                    'owner_replaced'
                )),
                reason TEXT NOT NULL CHECK(reason IN (
                    'lane_reservation', 'fair_share', 'effective_score'
                )),
                base_priority REAL NOT NULL CHECK(
                    typeof(base_priority) IN ('real', 'integer')
                ),
                effective_score REAL NOT NULL CHECK(
                    typeof(effective_score) IN ('real', 'integer')
                ),
                created_at TEXT NOT NULL,
                settled_at TEXT,
                FOREIGN KEY (event_id) REFERENCES causal_events(id),
                FOREIGN KEY (engram_id) REFERENCES engrams(id),
                FOREIGN KEY (center_id) REFERENCES activity_centers(id),
                CHECK(
                    (center_id IS NULL AND lane = 'unbound')
                    OR (center_id IS NOT NULL AND lane IN ('work', 'life'))
                ),
                CHECK(
                    (state = 'held' AND outcome IS NULL AND settled_at IS NULL)
                    OR (state = 'settled' AND outcome IN (
                        'succeeded', 'failed', 'skipped', 'uncertain'
                    ) AND settled_at IS NOT NULL)
                    OR (state = 'abandoned' AND outcome IN (
                        'uncertain', 'owner_replaced'
                    ) AND settled_at IS NOT NULL)
                )
            );

            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_center_reservations_one_held_event
                ON center_reservations(event_id) WHERE state = 'held';
            CREATE INDEX IF NOT EXISTS idx_center_reservations_history
                ON center_reservations(world_id, created_at DESC, id);
            CREATE INDEX IF NOT EXISTS idx_center_reservations_center_history
                ON center_reservations(center_id, created_at DESC, id);
            CREATE INDEX IF NOT EXISTS idx_center_reservations_held_owner
                ON center_reservations(owner_id, lease_epoch, state, created_at, id);

            -- The operation ledger is deliberately separate from
            -- harness_events.  It is the durable recovery fact source for
            -- actions whose adapter boundary may have crossed.
            CREATE TABLE IF NOT EXISTS harness_operations (
                operation_kind TEXT NOT NULL
                    CHECK(length(trim(operation_kind)) BETWEEN 1 AND 64),
                operation_id TEXT NOT NULL
                    CHECK(length(trim(operation_id)) BETWEEN 1 AND 128),
                world_id TEXT NOT NULL
                    CHECK(length(trim(world_id)) BETWEEN 1 AND 128),
                engram_id TEXT NOT NULL
                    CHECK(length(trim(engram_id)) BETWEEN 1 AND 128),
                turn_id TEXT NOT NULL
                    CHECK(length(trim(turn_id)) BETWEEN 1 AND 128),
                requested_epoch INTEGER NOT NULL
                    CHECK(typeof(requested_epoch) = 'integer'
                          AND requested_epoch >= 1),
                owner_id TEXT NOT NULL
                    CHECK(length(trim(owner_id)) BETWEEN 1 AND 128),
                scope_digest TEXT NOT NULL
                    CHECK(length(trim(scope_digest)) BETWEEN 1 AND 128),
                effect_key TEXT NOT NULL
                    CHECK(length(trim(effect_key)) BETWEEN 1 AND 128),
                phase TEXT NOT NULL CHECK(phase IN (
                    'intent', 'admitted', 'approval_pending', 'starting',
                    'boundary_entered', 'adapter_returned', 'terminalizing',
                    'terminal'
                )),
                terminal_state TEXT CHECK(terminal_state IS NULL OR
                    terminal_state IN (
                        'FAILED_NOT_STARTED', 'CANCELLED_NOT_STARTED',
                        'COMPLETED', 'UNCERTAIN'
                    )),
                terminal_event_id TEXT,
                recovery_owner_id TEXT,
                recovery_epoch INTEGER,
                recovery_state TEXT NOT NULL DEFAULT 'none'
                    CHECK(recovery_state IN ('none', 'required', 'cleared')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(operation_kind, operation_id),
                CHECK((phase = 'terminal') = (terminal_state IS NOT NULL)),
                CHECK(
                    (recovery_owner_id IS NULL AND recovery_epoch IS NULL)
                    OR (length(trim(recovery_owner_id)) BETWEEN 1 AND 128
                        AND typeof(recovery_epoch) = 'integer'
                        AND recovery_epoch >= 1)
                ),
                CHECK(
                    (terminal_state IS NULL
                        AND terminal_event_id IS NULL
                        AND recovery_state = 'none')
                    OR (terminal_state IS NOT NULL
                        AND (
                            (terminal_event_id IS NULL
                                AND recovery_state = 'required')
                            OR (terminal_event_id IS NOT NULL
                                AND recovery_state = 'cleared')
                        ))
                )
            );

            CREATE INDEX IF NOT EXISTS idx_harness_operations_recovery
                ON harness_operations(phase, recovery_state, updated_at,
                                      operation_kind, operation_id);
            CREATE INDEX IF NOT EXISTS idx_harness_operations_scope
                ON harness_operations(world_id, engram_id, turn_id,
                                      requested_epoch, owner_id);

            CREATE TRIGGER IF NOT EXISTS harness_operations_immutable_update
            BEFORE UPDATE OF operation_kind, operation_id, world_id,
                engram_id, turn_id, requested_epoch, owner_id,
                scope_digest, effect_key ON harness_operations
            WHEN OLD.operation_kind IS NOT NEW.operation_kind
                OR OLD.operation_id IS NOT NEW.operation_id
                OR OLD.world_id IS NOT NEW.world_id
                OR OLD.engram_id IS NOT NEW.engram_id
                OR OLD.turn_id IS NOT NEW.turn_id
                OR OLD.requested_epoch IS NOT NEW.requested_epoch
                OR OLD.owner_id IS NOT NEW.owner_id
                OR OLD.scope_digest IS NOT NEW.scope_digest
                OR OLD.effect_key IS NOT NEW.effect_key
            BEGIN
                SELECT RAISE(ABORT, 'harness operation scope is immutable');
            END;
        """)
        # ``harness_operations`` first appeared on the Phase-B development
        # branch before successor recovery identity was added.  Keep local
        # development databases forward-readable without rebuilding or
        # copying user data.  Fresh databases receive the stricter CHECK
        # above; legacy rows start with no recovery claimant.
        operation_columns = {
            str(row[1])
            for row in self._conn.execute("PRAGMA table_info(harness_operations)")
        }
        if "recovery_owner_id" not in operation_columns:
            self._conn.execute(
                "ALTER TABLE harness_operations ADD COLUMN recovery_owner_id TEXT"
            )
        if "recovery_epoch" not in operation_columns:
            self._conn.execute(
                "ALTER TABLE harness_operations ADD COLUMN recovery_epoch INTEGER"
            )
        if commit:
            self._conn.commit()

    # ── Harness event projection ─────────────────────────────────

    @staticmethod
    def _harness_event_limit(value: object, field_name: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{field_name} must be an integer >= 1")
        return value

    @staticmethod
    def _harness_event_select() -> str:
        return (
            "SELECT event_id, turn_id, world_id, engram_id, seq, "
            "parent_event_id, kind, phase, source, status, occurred_at, "
            "payload_json, payload_bytes, payload_digest, redacted, truncated "
            "FROM harness_events"
        )

    @staticmethod
    def _row_to_harness_event(row: tuple):
        from pulse_system.agent.harness.events import HarnessEvent

        return HarnessEvent.from_storage_row(row)

    @staticmethod
    def _harness_event_identity_matches(existing, incoming) -> bool:
        return (
            existing.turn_id == incoming.turn_id
            and existing.world_id == incoming.world_id
            and existing.engram_id == incoming.engram_id
            and existing.parent_event_id == incoming.parent_event_id
            and existing.kind.value == incoming.kind.value
            and existing.phase.value == incoming.phase.value
            and existing.source.value == incoming.source.value
            and existing.status.value == incoming.status.value
            and existing.payload_digest == incoming.payload_digest
            and existing.redacted == incoming.redacted
            and existing.truncated == incoming.truncated
        )

    @staticmethod
    def _prepared_from_harness_event(event):
        from pulse_system.agent.harness.events import _PreparedHarnessEvent

        return _PreparedHarnessEvent(
            event_id=event.event_id,
            turn_id=event.turn_id,
            world_id=event.world_id,
            engram_id=event.engram_id,
            seq=event.seq,
            parent_event_id=event.parent_event_id,
            kind=event.kind,
            phase=event.phase,
            source=event.source,
            status=event.status,
            occurred_at=event.occurred_at,
            payload_json=event.payload_json,
            payload_json_text=event.payload_json_text,
            payload_bytes=event.payload_bytes,
            payload_digest=event.payload_digest,
            redacted=event.redacted,
            truncated=event.truncated,
        )

    @_locked
    def append_harness_event(
        self,
        event: object,
        *,
        max_events_per_turn: int = 512,
        max_payload_bytes: int = 64 * 1024,
        max_total_rows: int = 10_000,
        max_total_bytes: int = 64 * 1024 * 1024,
    ):
        """Append one redacted event in the same SQLite transaction as pruning.

        ``HarnessEventStore`` normally passes a prepared private value here,
        while accepting a ``HarnessEventDraft`` keeps the storage adapter
        useful in isolated contract tests.  No raw mapping is accepted by
        this method; the draft path is sanitized in ``events.py`` first.
        """

        from pulse_system.agent.harness.events import (
            HarnessEvent,
            HarnessEventCapacityError,
            HarnessEventConflictError,
            HarnessEventDraft,
            _PreparedHarnessEvent,
            _prepare_draft,
        )

        max_events_per_turn = self._harness_event_limit(
            max_events_per_turn,
            "max_events_per_turn",
        )
        max_payload_bytes = self._harness_event_limit(
            max_payload_bytes,
            "max_payload_bytes",
        )
        max_total_rows = self._harness_event_limit(max_total_rows, "max_total_rows")
        max_total_bytes = self._harness_event_limit(
            max_total_bytes,
            "max_total_bytes",
        )

        if isinstance(event, HarnessEventDraft):
            prepared = _prepare_draft(
                event,
                max_payload_bytes=max_payload_bytes,
            )
        elif isinstance(event, _PreparedHarnessEvent):
            prepared = event
        elif isinstance(event, HarnessEvent):
            # Re-run the public event through the redactor rather than
            # allowing a manually-created HarnessEvent to bypass the boundary.
            prepared = _prepare_draft(
                HarnessEventDraft(
                    turn_id=event.turn_id,
                    world_id=event.world_id,
                    engram_id=event.engram_id,
                    kind=event.kind,
                    phase=event.phase,
                    source=event.source,
                    status=event.status,
                    occurred_at=event.occurred_at,
                    payload=event.payload_json,
                    parent_event_id=event.parent_event_id,
                    seq=event.seq,
                    event_id=event.event_id,
                ),
                max_payload_bytes=max_payload_bytes,
            )
        else:
            raise TypeError(
                "event must be HarnessEventDraft, HarnessEvent, or prepared event"
            )

        if prepared.payload_bytes > max_payload_bytes:
            raise HarnessEventCapacityError(
                "prepared event payload exceeds max_payload_bytes"
            )

        removed_rows = 0
        transaction = (
            nullcontext(self._conn)
            if self._conn.in_transaction
            else self._immediate_transaction()
        )
        with transaction as conn:
            self._assert_runtime_publication()
            existing_by_id = None
            if prepared.event_id:
                existing_by_id = conn.execute(
                    self._harness_event_select()
                    + " WHERE event_id = ?",
                    (prepared.event_id,),
                ).fetchone()
            if existing_by_id is not None:
                existing = self._row_to_harness_event(existing_by_id)
                if (
                    existing.turn_id == prepared.turn_id
                    and (prepared.seq is None or existing.seq == prepared.seq)
                    and self._harness_event_identity_matches(existing, prepared)
                ):
                    return existing
                raise HarnessEventConflictError(
                    prepared.turn_id,
                    prepared.seq or existing.seq,
                    "event_id is already bound to a different event",
                )

            seq = prepared.seq
            if seq is None:
                row = conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) FROM harness_events "
                    "WHERE turn_id = ?",
                    (prepared.turn_id,),
                ).fetchone()
                seq = int(row[0]) + 1
            if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
                raise ValueError("seq must be an integer >= 1")
            prepared = replace(prepared, seq=seq)

            existing_row = conn.execute(
                self._harness_event_select()
                + " WHERE turn_id = ? AND seq = ?",
                (prepared.turn_id, prepared.seq),
            ).fetchone()
            if existing_row is not None:
                existing = self._row_to_harness_event(existing_row)
                if self._harness_event_identity_matches(existing, prepared):
                    return existing
                raise HarnessEventConflictError(
                    prepared.turn_id,
                    prepared.seq,
                )

            conn.execute(
                """INSERT INTO harness_events (
                    event_id, turn_id, world_id, engram_id, seq,
                    parent_event_id, kind, phase, source, status,
                    occurred_at, payload_json, payload_bytes, payload_digest,
                    redacted, truncated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    prepared.event_id,
                    prepared.turn_id,
                    prepared.world_id,
                    prepared.engram_id,
                    prepared.seq,
                    prepared.parent_event_id,
                    prepared.kind.value,
                    prepared.phase.value,
                    prepared.source.value,
                    prepared.status.value,
                    prepared.occurred_at.isoformat(),
                    prepared.payload_json_text,
                    prepared.payload_bytes,
                    prepared.payload_digest,
                    int(prepared.redacted),
                    int(prepared.truncated),
                ),
            )

            removed_rows, _ = self._prune_harness_events_uncommitted(
                conn,
                max_events_per_turn=max_events_per_turn,
                max_total_rows=max_total_rows,
                max_total_bytes=max_total_bytes,
                exclude_event_id=prepared.event_id,
            )
            row = conn.execute(
                self._harness_event_select()
                + " WHERE event_id = ?",
                (prepared.event_id,),
            ).fetchone()
            if row is None:
                raise HarnessEventCapacityError("appended event is not durable")

        if removed_rows:
            self._harness_last_prune_at = _now()
        return self._row_to_harness_event(row)

    @_locked
    def append_harness_terminal_event(
        self,
        event: object,
        *,
        operation_kind: str,
        operation_id: str,
        expected_epoch: int,
        owner_id: str,
        terminal_state: object,
        max_events_per_turn: int = 512,
        max_payload_bytes: int = 64 * 1024,
        max_total_rows: int = 10_000,
        max_total_bytes: int = 64 * 1024 * 1024,
    ):
        """Atomically append one terminal projection and claim its E0 winner."""

        from pulse_system.agent.harness.events import (
            HarnessEvent,
            HarnessEventDraft,
            _PreparedHarnessEvent,
            _prepare_draft,
        )
        from pulse_system.agent.harness.operations import (
            HarnessOperationLedger,
            OperationRecoveryState,
            OperationScopeCollisionError,
            OperationTerminalState,
        )

        if isinstance(event, HarnessEventDraft):
            prepared = _prepare_draft(event, max_payload_bytes=max_payload_bytes)
        elif isinstance(event, _PreparedHarnessEvent):
            prepared = event
        elif isinstance(event, HarnessEvent):
            prepared = _prepare_draft(
                HarnessEventDraft(
                    turn_id=event.turn_id,
                    world_id=event.world_id,
                    engram_id=event.engram_id,
                    kind=event.kind,
                    phase=event.phase,
                    source=event.source,
                    status=event.status,
                    occurred_at=event.occurred_at,
                    payload=event.payload_json,
                    parent_event_id=event.parent_event_id,
                    seq=event.seq,
                    event_id=event.event_id,
                ),
                max_payload_bytes=max_payload_bytes,
            )
        else:
            raise TypeError("event must be a Harness terminal event")
        if not prepared.event_id:
            raise ValueError("an E0 terminal event requires a deterministic event_id")

        state = (
            terminal_state
            if isinstance(terminal_state, OperationTerminalState)
            else OperationTerminalState(str(terminal_state))
        )
        ledger = HarnessOperationLedger(self)
        key = ledger._key(operation_kind, operation_id)
        with self._immediate_transaction() as conn:
            self._assert_runtime_publication()
            current = ledger._fetch(conn, key)
            if current is None:
                raise ValueError("operation does not exist")
            if current.is_terminal:
                if current.terminal_state is not state:
                    raise OperationScopeCollisionError(
                        "terminal operation has another durable outcome"
                    )
                if current.terminal_event_id is not None:
                    if current.terminal_event_id != prepared.event_id:
                        raise OperationScopeCollisionError(
                            "terminal operation is bound to another projection"
                        )
                    row = conn.execute(
                        self._harness_event_select() + " WHERE event_id = ?",
                        (prepared.event_id,),
                    ).fetchone()
                    if row is None:
                        raise ValueError("terminal operation projection is missing")
                    return self._row_to_harness_event(row), current
                if current.recovery_state is not OperationRecoveryState.REQUIRED:
                    raise ValueError("terminal operation has no recoverable projection")
                appended = self.append_harness_event(
                    prepared,
                    max_events_per_turn=max_events_per_turn,
                    max_payload_bytes=max_payload_bytes,
                    max_total_rows=max_total_rows,
                    max_total_bytes=max_total_bytes,
                )
                conn.execute(
                    "UPDATE harness_operations SET terminal_event_id = ?, "
                    "recovery_state = 'cleared', updated_at = ? "
                    "WHERE operation_kind = ? AND operation_id = ? "
                    "AND terminal_event_id IS NULL",
                    (
                        appended.event_id,
                        _ts(_now()),
                        current.operation_kind,
                        current.operation_id,
                    ),
                )
                return appended, ledger._updated_row(conn, key)
            ledger._require_cas(
                current,
                expected_epoch=expected_epoch,
                owner_id=owner_id,
            )
            appended = self.append_harness_event(
                prepared,
                max_events_per_turn=max_events_per_turn,
                max_payload_bytes=max_payload_bytes,
                max_total_rows=max_total_rows,
                max_total_bytes=max_total_bytes,
            )
            winner = ledger._claim_terminal_row(
                conn,
                current,
                terminal_state=state,
                terminal_event_id=appended.event_id,
            )
            if (
                winner.terminal_event_id != appended.event_id
                or winner.recovery_state is not OperationRecoveryState.CLEARED
            ):
                raise ValueError("terminal event and operation winner did not commit together")
            return appended, winner

    def recover_harness_terminal_event(
        self,
        event: object,
        *,
        recovery_permit: RuntimeRecoveryPermit,
        operation_kind: str,
        operation_id: str,
        expected_epoch: int,
        owner_id: str,
        terminal_state: object,
        max_events_per_turn: int = 512,
        max_payload_bytes: int = 64 * 1024,
        max_total_rows: int = 10_000,
        max_total_bytes: int = 64 * 1024 * 1024,
    ):
        """Append one shutdown-recovery projection under recovery authority.

        This is intentionally separate from the ordinary append API.  A
        recovery permit cannot be supplied to arbitrary Harness publication,
        and callers cannot substitute an untyped callback.
        """

        from pulse_system.core.runtime.publication import RuntimeRecoveryPermit

        if not isinstance(recovery_permit, RuntimeRecoveryPermit):
            raise ValueError("recovery_permit must be a RuntimeRecoveryPermit")
        recovery_permit.assert_recovery()
        with self._runtime_authority_scope(recovery_permit):
            return self.append_harness_terminal_event(
                event,
                operation_kind=operation_kind,
                operation_id=operation_id,
                expected_epoch=expected_epoch,
                owner_id=owner_id,
                terminal_state=terminal_state,
                max_events_per_turn=max_events_per_turn,
                max_payload_bytes=max_payload_bytes,
                max_total_rows=max_total_rows,
                max_total_bytes=max_total_bytes,
            )

    @_locked
    def get_harness_event(self, event_id: str):
        if not isinstance(event_id, str) or not event_id.strip():
            raise ValueError("event_id must be a non-empty string")
        row = self._conn.execute(
            self._harness_event_select() + " WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        return None if row is None else self._row_to_harness_event(row)

    # ── Harness control-only audit ──────────────────────────────

    @staticmethod
    def _harness_control_text(value: object, field_name: str, *, maximum: int = 256) -> str:
        if not isinstance(value, str) or not value.strip() or "\x00" in value:
            raise ValueError(f"{field_name} must be a non-empty string")
        result = value.strip()
        if len(result.encode("utf-8")) > maximum:
            raise ValueError(f"{field_name} is too large")
        return result

    @staticmethod
    def _harness_control_snapshot_uncommitted(conn, world_id: str) -> dict:
        row = conn.execute(
            "SELECT MIN(sequence), MAX(sequence), COUNT(*) "
            "FROM harness_control_observations WHERE world_id = ?",
            (world_id,),
        ).fetchone()
        oldest = None if row is None or row[0] is None else int(row[0])
        latest = None if row is None or row[1] is None else int(row[1])
        retained = 0 if row is None else int(row[2])
        return {
            "world_id": world_id,
            "retained_records": retained,
            "total_seen": 0 if latest is None else latest,
            "oldest_sequence": oldest,
            "latest_sequence": latest,
            "truncated": oldest is not None and oldest > 1,
            "payload_free": True,
            "replay_can_enqueue": False,
        }

    @_locked
    def append_harness_control_observation(
        self,
        *,
        world_id: str,
        record_id: str,
        stimulus_id: str,
        stimulus_class: str,
        declared_class: str,
        evidence_class: str,
        route: str,
        reason_code: str,
        provenance_digest: str,
        external_effect_id: str | None,
        max_records: int = 4096,
    ) -> dict:
        """Persist one payload-free firewall decision with bounded retention.

        Idempotent retries preserve the original sequence.  A record-id
        collision with different immutable fields fails closed.  When the
        retention bound is crossed only the oldest audit rows are pruned;
        the monotonic sequence retains explicit truncation evidence.
        """

        maximums = {
            "world_id": 128,
            "record_id": 256,
            "stimulus_id": 256,
            "stimulus_class": 64,
            "declared_class": 64,
            "evidence_class": 64,
            "route": 64,
            "reason_code": 128,
            "provenance_digest": 256,
        }
        values = {
            "world_id": world_id,
            "record_id": record_id,
            "stimulus_id": stimulus_id,
            "stimulus_class": stimulus_class,
            "declared_class": declared_class,
            "evidence_class": evidence_class,
            "route": route,
            "reason_code": reason_code,
            "provenance_digest": provenance_digest,
        }
        clean = {
            key: self._harness_control_text(value, key, maximum=maximums[key])
            for key, value in values.items()
        }
        if clean["route"] != "control_ledger":
            raise ValueError("a durable control observation must use control_ledger route")
        clean_effect = None
        if external_effect_id is not None:
            clean_effect = self._harness_control_text(
                external_effect_id,
                "external_effect_id",
                maximum=256,
            )
        max_records = self._harness_event_limit(max_records, "max_records")

        immutable = (
            clean["stimulus_id"],
            clean["stimulus_class"],
            clean["declared_class"],
            clean["evidence_class"],
            clean["route"],
            clean["reason_code"],
            clean["provenance_digest"],
            clean_effect,
        )
        with self._immediate_transaction() as conn:
            existing = conn.execute(
                "SELECT stimulus_id, stimulus_class, declared_class, "
                "evidence_class, route, reason_code, provenance_digest, "
                "external_effect_id FROM harness_control_observations "
                "WHERE world_id = ? AND record_id = ?",
                (clean["world_id"], clean["record_id"]),
            ).fetchone()
            if existing is not None:
                if tuple(existing) != immutable:
                    raise ValueError("control observation record_id collision")
                return self._harness_control_snapshot_uncommitted(
                    conn,
                    clean["world_id"],
                )

            row = conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) "
                "FROM harness_control_observations WHERE world_id = ?",
                (clean["world_id"],),
            ).fetchone()
            sequence = int(row[0]) + 1
            conn.execute(
                """INSERT INTO harness_control_observations (
                    world_id, record_id, sequence, stimulus_id,
                    stimulus_class, declared_class, evidence_class, route,
                    reason_code, provenance_digest, external_effect_id,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    clean["world_id"],
                    clean["record_id"],
                    sequence,
                    clean["stimulus_id"],
                    clean["stimulus_class"],
                    clean["declared_class"],
                    clean["evidence_class"],
                    clean["route"],
                    clean["reason_code"],
                    clean["provenance_digest"],
                    clean_effect,
                    _ts(_now()),
                ),
            )
            conn.execute(
                "DELETE FROM harness_control_observations WHERE world_id = ? "
                "AND sequence IN (SELECT sequence FROM "
                "harness_control_observations WHERE world_id = ? "
                "ORDER BY sequence ASC LIMIT MAX(0, "
                "(SELECT COUNT(*) FROM harness_control_observations "
                "WHERE world_id = ?) - ?))",
                (
                    clean["world_id"],
                    clean["world_id"],
                    clean["world_id"],
                    max_records,
                ),
            )
            return self._harness_control_snapshot_uncommitted(
                conn,
                clean["world_id"],
            )

    @_locked
    def harness_control_audit_snapshot(self, world_id: str) -> dict:
        clean_world_id = self._harness_control_text(
            world_id,
            "world_id",
            maximum=128,
        )
        return self._harness_control_snapshot_uncommitted(
            self._conn,
            clean_world_id,
        )

    @_locked
    def list_harness_control_observations(
        self,
        world_id: str,
        *,
        after_sequence: int = 0,
        limit: int = 500,
    ) -> list[dict]:
        """Read payload-free audit rows; this method has no life ingress."""

        clean_world_id = self._harness_control_text(
            world_id,
            "world_id",
            maximum=128,
        )
        if type(after_sequence) is not int or after_sequence < 0:
            raise ValueError("after_sequence must be an integer >= 0")
        limit = min(self._harness_event_limit(limit, "limit"), 1000)
        rows = self._conn.execute(
            "SELECT record_id, sequence, stimulus_id, stimulus_class, "
            "declared_class, evidence_class, route, reason_code, "
            "provenance_digest, external_effect_id, created_at "
            "FROM harness_control_observations WHERE world_id = ? "
            "AND sequence > ? ORDER BY sequence ASC LIMIT ?",
            (clean_world_id, after_sequence, limit),
        ).fetchall()
        return [
            {
                "world_id": clean_world_id,
                "record_id": str(row[0]),
                "sequence": int(row[1]),
                "stimulus_id": str(row[2]),
                "stimulus_class": str(row[3]),
                "declared_class": str(row[4]),
                "evidence_class": str(row[5]),
                "route": str(row[6]),
                "reason_code": str(row[7]),
                "provenance_digest": str(row[8]),
                "external_effect_id": row[9],
                "created_at": str(row[10]),
            }
            for row in rows
        ]

    @_locked
    def replay_harness_events(
        self,
        turn_id: str,
        *,
        after_seq: int = 0,
        limit: int = 500,
    ):
        from pulse_system.agent.harness.events import (
            HarnessEventGap,
            HarnessEventPage,
        )

        if not isinstance(turn_id, str) or not turn_id.strip():
            raise ValueError("turn_id must be a non-empty string")
        if isinstance(after_seq, bool) or not isinstance(after_seq, int) or after_seq < 0:
            raise ValueError("after_seq must be an integer >= 0")
        limit = min(self._harness_event_limit(limit, "limit"), 500)
        stats = self._conn.execute(
            "SELECT MIN(seq), MAX(seq), COUNT(*) FROM harness_events "
            "WHERE turn_id = ?",
            (turn_id,),
        ).fetchone()
        oldest_seq = None if stats[0] is None else int(stats[0])
        latest_seq = None if stats[1] is None else int(stats[1])
        turn_known = bool(stats[2])
        rows = self._conn.execute(
            self._harness_event_select()
            + " WHERE turn_id = ? AND seq > ? ORDER BY seq LIMIT ?",
            (turn_id, after_seq, limit + 1),
        ).fetchall()
        has_more = len(rows) > limit
        events = tuple(
            self._row_to_harness_event(row) for row in rows[:limit]
        )

        gaps: list[HarnessEventGap] = []
        cursor = after_seq
        if oldest_seq is not None and oldest_seq > cursor + 1:
            gaps.append(
                HarnessEventGap(
                    turn_id=turn_id,
                    from_seq=cursor + 1,
                    to_seq=oldest_seq - 1,
                    reason="pruned_or_missing",
                )
            )
            cursor = oldest_seq - 1
        for row in rows:
            seq = int(row[4])
            if seq > cursor + 1:
                gaps.append(
                    HarnessEventGap(
                        turn_id=turn_id,
                        from_seq=cursor + 1,
                        to_seq=seq - 1,
                        reason="pruned_or_missing",
                    )
                )
            cursor = seq

        next_seq = events[-1].seq if events else after_seq
        return HarnessEventPage(
            turn_id=turn_id,
            events=events,
            next_seq=next_seq,
            has_more=has_more,
            gaps=tuple(gaps),
            oldest_seq=oldest_seq,
            latest_seq=latest_seq,
            turn_known=turn_known,
        )

    def list_harness_events(
        self,
        turn_id: str,
        *,
        after_seq: int = 0,
        limit: int = 500,
    ):
        """Compatibility spelling for callers that prefer list semantics."""

        return self.replay_harness_events(
            turn_id,
            after_seq=after_seq,
            limit=limit,
        )

    def _delete_harness_event_candidate(
        self,
        conn: sqlite3.Connection,
        *,
        where_sql: str,
        params: tuple,
        order_sql: str,
    ) -> tuple[int, int] | None:
        row = conn.execute(
            "SELECT event_id, payload_bytes FROM harness_events WHERE "
            + where_sql
            + " ORDER BY "
            + order_sql
            + " LIMIT 1",
            params,
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            "DELETE FROM harness_events WHERE event_id = ?",
            (row[0],),
        )
        return 1, int(row[1])

    def _prune_harness_events_uncommitted(
        self,
        conn: sqlite3.Connection,
        *,
        max_events_per_turn: int,
        max_total_rows: int,
        max_total_bytes: int,
        exclude_event_id: str | None,
    ) -> tuple[int, int]:
        from pulse_system.agent.harness.events import (
            HARD_PROTECTED_CONTROL_PHASES,
            HARD_PROTECTED_CONTROL_SOURCES,
            HarnessEventCapacityError,
            PROTECTED_EVENT_KINDS,
        )

        protected = tuple(sorted(PROTECTED_EVENT_KINDS))
        protected_marks = ", ".join("?" for _ in protected)
        control_sources = tuple(sorted(HARD_PROTECTED_CONTROL_SOURCES))
        control_source_marks = ", ".join("?" for _ in control_sources)
        control_phases = tuple(sorted(HARD_PROTECTED_CONTROL_PHASES))
        control_phase_marks = ", ".join("?" for _ in control_phases)
        terminal_kind = "turn_terminal"
        hard_protected_sql = (
            "(kind = ? OR (source IN ("
            + control_source_marks
            + ") AND phase IN ("
            + control_phase_marks
            + ")))"
        )
        hard_protected_params = [terminal_kind, *control_sources, *control_phases]
        removed_rows = 0
        removed_bytes = 0

        turn_rows = conn.execute(
            "SELECT turn_id FROM harness_events GROUP BY turn_id "
            "HAVING COUNT(*) > ?",
            (max_events_per_turn,),
        ).fetchall()
        for (turn_id,) in turn_rows:
            while True:
                count = conn.execute(
                    "SELECT COUNT(*) FROM harness_events WHERE turn_id = ?",
                    (turn_id,),
                ).fetchone()[0]
                if count <= max_events_per_turn:
                    break
                where = (
                    f"turn_id = ? AND NOT {hard_protected_sql} "
                    f"AND kind NOT IN ({protected_marks})"
                )
                params: list[object] = [
                    turn_id,
                    *hard_protected_params,
                    *protected,
                ]
                if exclude_event_id is not None:
                    where += " AND event_id <> ?"
                    params.append(exclude_event_id)
                deleted = self._delete_harness_event_candidate(
                    conn,
                    where_sql=where,
                    params=tuple(params),
                    order_sql="seq, event_id",
                )
                if deleted is None:
                    where = (
                        f"turn_id = ? AND NOT {hard_protected_sql} "
                        f"AND kind IN ({protected_marks})"
                    )
                    params = [turn_id, *hard_protected_params, *protected]
                    if exclude_event_id is not None:
                        where += " AND event_id <> ?"
                        params.append(exclude_event_id)
                    deleted = self._delete_harness_event_candidate(
                        conn,
                        where_sql=where,
                        params=tuple(params),
                        order_sql="seq, event_id",
                    )
                if deleted is None:
                    raise HarnessEventCapacityError(
                        "per-turn event quota would delete protected terminal evidence"
                    )
                removed_rows += deleted[0]
                removed_bytes += deleted[1]

        while True:
            row_count, byte_count = conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(payload_bytes), 0) "
                "FROM harness_events"
            ).fetchone()
            if row_count <= max_total_rows and byte_count <= max_total_bytes:
                break
            where = (
                f"NOT {hard_protected_sql} "
                f"AND kind NOT IN ({protected_marks})"
            )
            params = [*hard_protected_params, *protected]
            if exclude_event_id is not None:
                where += " AND event_id <> ?"
                params.append(exclude_event_id)
            # Keep one row per turn so a replay cursor cannot silently restart
            # at sequence one after global retention.
            where += (
                " AND (SELECT COUNT(*) FROM harness_events AS peer "
                "WHERE peer.turn_id = harness_events.turn_id) > 1"
            )
            deleted = self._delete_harness_event_candidate(
                conn,
                where_sql=where,
                params=tuple(params),
                # occurred_at is intentionally bounded to milliseconds; use
                # SQLite insertion order as the stable tie-breaker so a burst
                # of events does not make retention depend on UUID ordering.
                order_sql="occurred_at, rowid, seq, event_id",
            )
            if deleted is None:
                where = (
                    f"NOT {hard_protected_sql} "
                    f"AND kind IN ({protected_marks})"
                )
                params = [*hard_protected_params, *protected]
                if exclude_event_id is not None:
                    where += " AND event_id <> ?"
                    params.append(exclude_event_id)
                where += (
                    " AND (SELECT COUNT(*) FROM harness_events AS peer "
                    "WHERE peer.turn_id = harness_events.turn_id) > 1"
                )
                deleted = self._delete_harness_event_candidate(
                    conn,
                    where_sql=where,
                    params=tuple(params),
                    order_sql="occurred_at, rowid, seq, event_id",
                )
            if deleted is None:
                raise HarnessEventCapacityError(
                    "global event quota would delete protected terminal evidence"
                )
            removed_rows += deleted[0]
            removed_bytes += deleted[1]

        return removed_rows, removed_bytes

    def _harness_event_capacity_uncommitted(
        self,
        conn: sqlite3.Connection,
        *,
        max_events_per_turn: int,
        max_event_payload_bytes: int,
        max_total_rows: int,
        max_total_bytes: int,
        last_prune_at: datetime | None = None,
    ):
        from pulse_system.agent.harness.events import HarnessCapacitySnapshot

        event_rows, event_bytes, retained_turns = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(payload_bytes), 0), "
            "COUNT(DISTINCT turn_id) FROM harness_events"
        ).fetchone()
        return HarnessCapacitySnapshot(
            event_rows=int(event_rows),
            event_bytes=int(event_bytes),
            retained_turns=int(retained_turns),
            last_prune_at=(
                self._harness_last_prune_at
                if last_prune_at is None
                else last_prune_at
            ),
            max_events_per_turn=max_events_per_turn,
            max_event_payload_bytes=max_event_payload_bytes,
            max_event_rows=max_total_rows,
            max_event_bytes=max_total_bytes,
        )

    @_locked
    def harness_event_capacity(
        self,
        *,
        max_events_per_turn: int = 512,
        max_event_payload_bytes: int = 64 * 1024,
        max_total_rows: int = 10_000,
        max_total_bytes: int = 64 * 1024 * 1024,
        max_payload_bytes: int | None = None,
    ):
        if max_payload_bytes is not None:
            max_event_payload_bytes = max_payload_bytes
        max_events_per_turn = self._harness_event_limit(
            max_events_per_turn,
            "max_events_per_turn",
        )
        max_event_payload_bytes = self._harness_event_limit(
            max_event_payload_bytes,
            "max_event_payload_bytes",
        )
        max_total_rows = self._harness_event_limit(max_total_rows, "max_total_rows")
        max_total_bytes = self._harness_event_limit(
            max_total_bytes,
            "max_total_bytes",
        )
        return self._harness_event_capacity_uncommitted(
            self._conn,
            max_events_per_turn=max_events_per_turn,
            max_event_payload_bytes=max_event_payload_bytes,
            max_total_rows=max_total_rows,
            max_total_bytes=max_total_bytes,
        )

    @_locked
    def prune_harness_events(
        self,
        *,
        max_events_per_turn: int = 512,
        max_total_rows: int = 10_000,
        max_total_bytes: int = 64 * 1024 * 1024,
        max_event_payload_bytes: int = 64 * 1024,
        now: datetime | None = None,
    ):
        from pulse_system.agent.harness.events import HarnessPruneResult

        max_events_per_turn = self._harness_event_limit(
            max_events_per_turn,
            "max_events_per_turn",
        )
        max_event_payload_bytes = self._harness_event_limit(
            max_event_payload_bytes,
            "max_event_payload_bytes",
        )
        max_total_rows = self._harness_event_limit(max_total_rows, "max_total_rows")
        max_total_bytes = self._harness_event_limit(
            max_total_bytes,
            "max_total_bytes",
        )
        prune_time = _require_utc(now or _now(), "now")
        with self._immediate_transaction() as conn:
            removed_rows, removed_bytes = self._prune_harness_events_uncommitted(
                conn,
                max_events_per_turn=max_events_per_turn,
                max_total_rows=max_total_rows,
                max_total_bytes=max_total_bytes,
                exclude_event_id=None,
            )
            retained_terminal_count = conn.execute(
                "SELECT COUNT(*) FROM harness_events WHERE kind = 'turn_terminal'"
            ).fetchone()[0]
            capacity = self._harness_event_capacity_uncommitted(
                conn,
                max_events_per_turn=max_events_per_turn,
                max_event_payload_bytes=max_event_payload_bytes,
                max_total_rows=max_total_rows,
                max_total_bytes=max_total_bytes,
                last_prune_at=prune_time,
            )
        self._harness_last_prune_at = prune_time
        return HarnessPruneResult(
            removed_rows=int(removed_rows),
            removed_bytes=int(removed_bytes),
            retained_terminal_count=int(retained_terminal_count),
            last_prune_at=prune_time,
            capacity=capacity,
        )

    # ── Durable center scheduling ───────────────────────────────
    # ── Durable center scheduling ───────────────────────────────

    @contextmanager
    def _immediate_transaction(self) -> Iterator[sqlite3.Connection]:
        """Serialize a durable scheduling mutation across SQLite connections."""

        try:
            self._conn.execute("BEGIN IMMEDIATE")
            yield self._conn
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise

    def _harness_operation_read(self, callback):
        """Run a bounded operation-ledger read under Storage's lock.

        The callback receives the transaction-local SQLite handle so the
        ledger module never reaches through Storage to ``_conn`` directly.
        """

        with self._lock:
            return callback(self._conn)

    def _harness_operation_write(
        self,
        callback,
    ):
        """Run one operation-ledger mutation in ``BEGIN IMMEDIATE``."""

        with self._lock:
            with self._immediate_transaction() as conn:
                self._assert_runtime_publication()
                return callback(conn)

    @_locked
    def get_runtime_lease(self) -> RuntimeLease | None:
        """Return the current lease row without claiming that it is healthy."""

        return self._get_runtime_lease_uncommitted()

    @_locked
    def acquire_runtime_lease(
        self,
        owner_id: str,
        *,
        now: datetime,
        ttl_sec: float,
    ) -> RuntimeLease:
        """Atomically acquire the fixed database-scoped owner lease.

        A live lease is never stolen, including by a caller that reuses the
        same owner ID. Callers renew an epoch they already own instead.
        """

        owner_id = _require_owner_id(owner_id)
        now = _require_utc(now, "now")
        ttl_sec = _require_lease_ttl(ttl_sec)
        with self._runtime_authority_scope(_RUNTIME_LEASE_CONTROL):
            with self._immediate_transaction() as conn:
                current = self._get_runtime_lease_uncommitted(conn)
                if (
                    current is not None
                    and current.state is RuntimeLeaseState.ACTIVE
                    and current.expires_at > now
                ):
                    raise RuntimeLeaseConflictError(
                        owner_id=owner_id,
                        epoch=None,
                        reason="active_conflict",
                        lease=current,
                    )
                epoch = 1 if current is None else current.epoch + 1
                lease = RuntimeLease(
                    scope="pulse_world",
                    owner_id=owner_id,
                    epoch=epoch,
                    state=RuntimeLeaseState.ACTIVE,
                    acquired_at=now,
                    renewed_at=now,
                    expires_at=now + timedelta(seconds=ttl_sec),
                )
                if current is None:
                    conn.execute(
                        "INSERT INTO runtime_leases "
                        "(scope, owner_id, epoch, state, acquired_at, renewed_at, "
                        "expires_at, released_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        self._runtime_lease_values(lease),
                    )
                else:
                    conn.execute(
                        "UPDATE runtime_leases SET owner_id = ?, epoch = ?, "
                        "state = ?, acquired_at = ?, renewed_at = ?, expires_at = ?, "
                        "released_at = NULL WHERE scope = 'pulse_world'",
                        (
                            lease.owner_id,
                            lease.epoch,
                            lease.state.value,
                            _ts(lease.acquired_at),
                            _ts(lease.renewed_at),
                            _ts(lease.expires_at),
                        ),
                    )
                return lease

    @_locked
    def renew_runtime_lease(
        self,
        owner_id: str,
        epoch: int,
        *,
        now: datetime,
        ttl_sec: float,
    ) -> RuntimeLease:
        """Renew one active epoch without changing its fencing token."""

        owner_id = _require_owner_id(owner_id)
        epoch = _require_epoch(epoch)
        now = _require_utc(now, "now")
        ttl_sec = _require_lease_ttl(ttl_sec)
        with self._runtime_authority_scope(_RUNTIME_LEASE_CONTROL):
            with self._immediate_transaction() as conn:
                current = self._assert_runtime_lease_uncommitted(
                    owner_id,
                    epoch,
                    now,
                    conn,
                )
                renewed = RuntimeLease(
                    scope=current.scope,
                    owner_id=current.owner_id,
                    epoch=current.epoch,
                    state=RuntimeLeaseState.ACTIVE,
                    acquired_at=current.acquired_at,
                    renewed_at=now,
                    expires_at=now + timedelta(seconds=ttl_sec),
                )
                conn.execute(
                    "UPDATE runtime_leases SET renewed_at = ?, expires_at = ? "
                    "WHERE scope = 'pulse_world'",
                    (_ts(renewed.renewed_at), _ts(renewed.expires_at)),
                )
                return renewed

    @_locked
    def assert_runtime_lease(
        self,
        owner_id: str,
        epoch: int,
        *,
        now: datetime,
    ) -> RuntimeLease:
        """Fence a caller before it starts a tick or commits scheduling facts."""

        owner_id = _require_owner_id(owner_id)
        epoch = _require_epoch(epoch)
        now = _require_utc(now, "now")
        with self._immediate_transaction() as conn:
            return self._assert_runtime_lease_uncommitted(
                owner_id,
                epoch,
                now,
                conn,
            )

    @_locked
    def release_runtime_lease(
        self,
        owner_id: str,
        epoch: int,
        *,
        now: datetime,
    ) -> RuntimeLease:
        """Release an owned, live epoch so the next Runtime may take over."""

        owner_id = _require_owner_id(owner_id)
        epoch = _require_epoch(epoch)
        now = _require_utc(now, "now")
        with self._runtime_authority_scope(_RUNTIME_LEASE_CONTROL):
            with self._immediate_transaction() as conn:
                current = self._assert_runtime_lease_uncommitted(
                    owner_id,
                    epoch,
                    now,
                    conn,
                )
                released = RuntimeLease(
                    scope=current.scope,
                    owner_id=current.owner_id,
                    epoch=current.epoch,
                    state=RuntimeLeaseState.RELEASED,
                    acquired_at=current.acquired_at,
                    renewed_at=current.renewed_at,
                    expires_at=current.expires_at,
                    released_at=now,
                )
                conn.execute(
                    "UPDATE runtime_leases SET state = ?, released_at = ? "
                    "WHERE scope = 'pulse_world'",
                    (released.state.value, _ts(released.released_at)),
                )
                return released

    @_locked
    def commit_center_schedule(
        self,
        owner_id: str,
        epoch: int,
        decisions: Sequence[CenterScheduleState],
        reservations: Sequence[CenterReservation],
        *,
        now: datetime,
    ) -> list[CenterReservation]:
        """Persist one complete admission decision batch and its held slots.

        The lease check, schedule-state replacement, and every held
        reservation share one `BEGIN IMMEDIATE` boundary. An error leaves no
        debt change or partial reservation behind.
        """

        owner_id = _require_owner_id(owner_id)
        epoch = _require_epoch(epoch)
        now = _require_utc(now, "now")
        decision_rows = tuple(decisions)
        reservation_rows = tuple(reservations)
        if not all(isinstance(row, CenterScheduleState) for row in decision_rows):
            raise ValueError("decisions must contain CenterScheduleState rows")
        if not all(isinstance(row, CenterReservation) for row in reservation_rows):
            raise ValueError("reservations must contain CenterReservation rows")
        if len({row.center_id for row in decision_rows}) != len(decision_rows):
            raise ValueError("decisions must contain at most one row per center")
        if len({row.id for row in reservation_rows}) != len(reservation_rows):
            raise ValueError("reservations must have unique IDs")
        if len({row.event_id for row in reservation_rows}) != len(reservation_rows):
            raise ValueError("reservations must have unique event IDs")
        decision_by_center = {row.center_id: row for row in decision_rows}
        reserved_centers = {
            row.center_id for row in reservation_rows if row.center_id is not None
        }
        for decision in decision_rows:
            admitted = decision.decision is CenterScheduleDecision.ADMITTED
            if admitted != (decision.center_id in reserved_centers):
                raise ValueError(
                    "admitted Center decisions and centered reservations must agree"
                )
        for reservation in reservation_rows:
            if reservation.state is not CenterReservationState.HELD:
                raise ValueError("schedule commits may only create held reservations")
            if (
                reservation.owner_id != owner_id
                or reservation.lease_epoch != epoch
            ):
                raise ValueError(
                    "held reservations must identify the committing owner epoch"
                )
            if reservation.center_id is not None:
                decision = decision_by_center.get(reservation.center_id)
                if (
                    decision is None
                    or decision.decision is not CenterScheduleDecision.ADMITTED
                ):
                    raise ValueError(
                        "every centered reservation requires an admitted decision"
                    )
        with self._immediate_transaction() as conn:
            self._assert_runtime_lease_uncommitted(owner_id, epoch, now, conn)
            self._assert_runtime_publication()
            for decision in decision_rows:
                self._assert_center_lane_uncommitted(
                    conn,
                    decision.center_id,
                    decision.lane,
                )
                conn.execute(
                    "INSERT INTO center_schedule_state "
                    "(center_id, lane, decision, reason, starvation_debt, "
                    "waiting_since, last_admitted_at, last_decision_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(center_id) DO UPDATE SET "
                    "lane = excluded.lane, decision = excluded.decision, "
                    "reason = excluded.reason, "
                    "starvation_debt = excluded.starvation_debt, "
                    "waiting_since = excluded.waiting_since, "
                    "last_admitted_at = excluded.last_admitted_at, "
                    "last_decision_at = excluded.last_decision_at, "
                    "updated_at = excluded.updated_at",
                    self._center_schedule_state_values(decision),
                )
            for reservation in reservation_rows:
                self._assert_reservation_event_uncommitted(conn, reservation)
                if reservation.center_id is not None:
                    self._assert_center_lane_uncommitted(
                        conn,
                        reservation.center_id,
                        reservation.lane,
                    )
                conn.execute(
                    "INSERT INTO center_reservations "
                    "(id, world_id, event_id, engram_id, center_id, lane, "
                    "owner_id, lease_epoch, state, outcome, reason, base_priority, "
                    "effective_score, created_at, settled_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    self._center_reservation_values(reservation),
                )
            return list(reservation_rows)

    @_locked
    def settle_center_reservation(
        self,
        reservation_id: str,
        owner_id: str,
        epoch: int,
        outcome: CenterReservationOutcome | str,
        *,
        now: datetime,
    ) -> CenterReservation:
        """Settle a held slot once, without allowing a stale epoch to alter it."""

        if not isinstance(reservation_id, str) or not reservation_id.strip():
            raise ValueError("reservation_id must be a non-empty string")
        owner_id = _require_owner_id(owner_id)
        epoch = _require_epoch(epoch)
        now = _require_utc(now, "now")
        normalized_outcome = _enum_value(
            outcome,
            CenterReservationOutcome,
            "outcome",
        )
        if normalized_outcome not in {
            CenterReservationOutcome.SUCCEEDED,
            CenterReservationOutcome.FAILED,
            CenterReservationOutcome.SKIPPED,
            CenterReservationOutcome.UNCERTAIN,
        }:
            raise ValueError("settlement outcome must be a Harness outcome")
        with self._immediate_transaction() as conn:
            current_lease = self._assert_runtime_lease_uncommitted(
                owner_id,
                epoch,
                now,
                conn,
            )
            self._assert_runtime_publication()
            reservation = self._get_center_reservation_uncommitted(
                conn,
                reservation_id,
            )
            if reservation is None:
                raise KeyError(f"unknown center reservation: {reservation_id}")
            if (
                reservation.owner_id != owner_id
                or reservation.lease_epoch != epoch
            ):
                raise RuntimeLeaseLostError(
                    owner_id=owner_id,
                    epoch=epoch,
                    reason="reservation_owner_mismatch",
                    lease=current_lease,
                )
            if reservation.state is CenterReservationState.SETTLED:
                if reservation.outcome is normalized_outcome:
                    return reservation
                raise ValueError("reservation is already settled with another outcome")
            if reservation.state is CenterReservationState.ABANDONED:
                raise ValueError("abandoned reservations cannot be settled")
            settled = replace(
                reservation,
                state=CenterReservationState.SETTLED,
                outcome=normalized_outcome,
                settled_at=now,
            )
            conn.execute(
                "UPDATE center_reservations SET state = ?, outcome = ?, "
                "settled_at = ? WHERE id = ? AND state = 'held' "
                "AND owner_id = ? AND lease_epoch = ?",
                (
                    settled.state.value,
                    settled.outcome.value,
                    _ts(settled.settled_at),
                    reservation.id,
                    owner_id,
                    epoch,
                ),
            )
            return settled

    @_locked
    def recover_held_center_reservations(
        self,
        owner_id: str,
        epoch: int,
        *,
        now: datetime,
        bootstrap_permit: RuntimeBootstrapPermit | None = None,
    ) -> list[CenterReservation]:
        """Abandon old held slots without changing causal replay semantics.

        Causal recovery runs before this method. A still-queued event was never
        accepted and may be admitted again, so its abandoned slot records
        ``owner_replaced``. Every other durable state is conservatively
        ``uncertain``; the scheduler never turns it back into queued work.
        """

        owner_id = _require_owner_id(owner_id)
        epoch = _require_epoch(epoch)
        now = _require_utc(now, "now")
        if self._runtime_publication_permit is not None and bootstrap_permit is None:
            raise ValueError(
                "runtime-bound reservation recovery requires a bootstrap permit"
            )
        authority = (
            nullcontext()
            if bootstrap_permit is None
            else self._runtime_authority_scope(bootstrap_permit)
        )
        with authority:
            with self._immediate_transaction() as conn:
                self._assert_runtime_lease_uncommitted(owner_id, epoch, now, conn)
                self._assert_runtime_publication()
                rows = conn.execute(
                "SELECT center_reservations.id, center_reservations.world_id, "
                "center_reservations.event_id, center_reservations.engram_id, "
                "center_reservations.center_id, center_reservations.lane, "
                "center_reservations.owner_id, center_reservations.lease_epoch, "
                "center_reservations.state, center_reservations.outcome, "
                "center_reservations.reason, center_reservations.base_priority, "
                "center_reservations.effective_score, "
                "center_reservations.created_at, center_reservations.settled_at, "
                "causal_events.status "
                "FROM center_reservations JOIN causal_events "
                "ON causal_events.id = center_reservations.event_id "
                "WHERE center_reservations.state = 'held' "
                "AND (center_reservations.owner_id <> ? "
                "OR center_reservations.lease_epoch <> ?) "
                "ORDER BY center_reservations.created_at, center_reservations.id",
                (owner_id, epoch),
                ).fetchall()
                recovered: list[CenterReservation] = []
                for row in rows:
                    held = self._row_to_center_reservation(row)
                    recovery_outcome = (
                        CenterReservationOutcome.OWNER_REPLACED
                        if row[15] == "queued"
                        else CenterReservationOutcome.UNCERTAIN
                    )
                    abandoned = replace(
                        held,
                        state=CenterReservationState.ABANDONED,
                        outcome=recovery_outcome,
                        settled_at=now,
                    )
                    conn.execute(
                        "UPDATE center_reservations SET state = ?, outcome = ?, "
                        "settled_at = ? WHERE id = ? AND state = 'held'",
                        (
                            abandoned.state.value,
                            abandoned.outcome.value,
                            _ts(abandoned.settled_at),
                            abandoned.id,
                        ),
                    )
                    recovered.append(abandoned)
                return recovered

    @_locked
    def recover_owned_center_reservations_for_shutdown(
        self,
        owner_id: str,
        epoch: int,
        *,
        now: datetime,
        recovery_permit: RuntimeRecoveryPermit,
    ) -> list[CenterReservation]:
        """Abandon this Runtime's held slots under shutdown-only authority."""

        from pulse_system.core.runtime.publication import RuntimeRecoveryPermit

        owner_id = _require_owner_id(owner_id)
        epoch = _require_epoch(epoch)
        now = _require_utc(now, "now")
        if not isinstance(recovery_permit, RuntimeRecoveryPermit):
            raise ValueError("recovery_permit must be a RuntimeRecoveryPermit")
        recovery_permit.assert_recovery()
        with self._runtime_authority_scope(recovery_permit):
            with self._immediate_transaction() as conn:
                self._assert_runtime_lease_uncommitted(owner_id, epoch, now, conn)
                rows = conn.execute(
                    "SELECT id, world_id, event_id, engram_id, center_id, lane, "
                    "owner_id, lease_epoch, state, outcome, reason, base_priority, "
                    "effective_score, created_at, settled_at "
                    "FROM center_reservations WHERE state = 'held' "
                    "AND owner_id = ? AND lease_epoch = ? "
                    "ORDER BY created_at, id",
                    (owner_id, epoch),
                ).fetchall()
                recovered: list[CenterReservation] = []
                for row in rows:
                    held = self._row_to_center_reservation(row)
                    abandoned = replace(
                        held,
                        state=CenterReservationState.ABANDONED,
                        outcome=CenterReservationOutcome.UNCERTAIN,
                        settled_at=now,
                    )
                    updated = conn.execute(
                        "UPDATE center_reservations SET state = ?, outcome = ?, "
                        "settled_at = ? WHERE id = ? AND state = 'held' "
                        "AND owner_id = ? AND lease_epoch = ?",
                        (
                            abandoned.state.value,
                            abandoned.outcome.value,
                            _ts(abandoned.settled_at),
                            abandoned.id,
                            owner_id,
                            epoch,
                        ),
                    )
                    if updated.rowcount != 1:
                        raise RuntimeLeaseLostError(
                            owner_id=owner_id,
                            epoch=epoch,
                            reason="reservation_shutdown_race",
                            lease=self._get_runtime_lease_uncommitted(conn),
                        )
                    recovered.append(abandoned)
                return recovered

    @_locked
    def get_center_schedule_state(
        self,
        center_id: str,
    ) -> CenterScheduleState | None:
        if not isinstance(center_id, str) or not center_id.strip():
            raise ValueError("center_id must be a non-empty string")
        row = self._conn.execute(
            "SELECT center_id, lane, decision, reason, starvation_debt, "
            "waiting_since, last_admitted_at, last_decision_at, updated_at "
            "FROM center_schedule_state WHERE center_id = ?",
            (center_id,),
        ).fetchone()
        return None if row is None else self._row_to_center_schedule_state(row)

    @_locked
    def list_center_schedule_states(self) -> list[CenterScheduleState]:
        """List durable Center decisions in a stable newest-decision order."""

        rows = self._conn.execute(
            "SELECT center_id, lane, decision, reason, starvation_debt, "
            "waiting_since, last_admitted_at, last_decision_at, updated_at "
            "FROM center_schedule_state "
            "ORDER BY last_decision_at DESC, center_id"
        ).fetchall()
        return [self._row_to_center_schedule_state(row) for row in rows]

    @_locked
    def get_center_reservation(
        self,
        reservation_id: str,
    ) -> CenterReservation | None:
        if not isinstance(reservation_id, str) or not reservation_id.strip():
            raise ValueError("reservation_id must be a non-empty string")
        return self._get_center_reservation_uncommitted(
            self._conn,
            reservation_id,
        )

    @_locked
    def list_center_reservations(
        self,
        *,
        world_id: str | None = None,
        center_id: str | None = None,
        state: CenterReservationState | str | None = None,
        limit: int | None = None,
    ) -> list[CenterReservation]:
        """List reservation history without joining or exposing event content."""

        clauses: list[str] = []
        params: list[object] = []
        if world_id is not None:
            if not isinstance(world_id, str) or not world_id.strip():
                raise ValueError("world_id must be a non-empty string")
            clauses.append("world_id = ?")
            params.append(world_id)
        if center_id is not None:
            if not isinstance(center_id, str) or not center_id.strip():
                raise ValueError("center_id must be a non-empty string")
            clauses.append("center_id = ?")
            params.append(center_id)
        if state is not None:
            clauses.append("state = ?")
            params.append(
                _enum_value(state, CenterReservationState, "state").value
            )
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
                raise ValueError("limit must be a positive integer")
        query = (
            "SELECT id, world_id, event_id, engram_id, center_id, lane, "
            "owner_id, lease_epoch, state, outcome, reason, base_priority, "
            "effective_score, created_at, settled_at FROM center_reservations"
        )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at DESC, id"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_center_reservation(row) for row in rows]

    def _get_runtime_lease_uncommitted(
        self,
        conn: sqlite3.Connection | None = None,
    ) -> RuntimeLease | None:
        connection = conn or self._conn
        row = connection.execute(
            "SELECT scope, owner_id, epoch, state, acquired_at, renewed_at, "
            "expires_at, released_at FROM runtime_leases "
            "WHERE scope = 'pulse_world'"
        ).fetchone()
        return None if row is None else self._row_to_runtime_lease(row)

    def _get_center_reservation_uncommitted(
        self,
        conn: sqlite3.Connection,
        reservation_id: str,
    ) -> CenterReservation | None:
        row = conn.execute(
            "SELECT id, world_id, event_id, engram_id, center_id, lane, "
            "owner_id, lease_epoch, state, outcome, reason, base_priority, "
            "effective_score, created_at, settled_at "
            "FROM center_reservations WHERE id = ?",
            (reservation_id,),
        ).fetchone()
        return None if row is None else self._row_to_center_reservation(row)

    def _assert_runtime_lease_uncommitted(
        self,
        owner_id: str,
        epoch: int,
        now: datetime,
        conn: sqlite3.Connection,
    ) -> RuntimeLease:
        current = self._get_runtime_lease_uncommitted(conn)
        if current is None:
            raise RuntimeLeaseLostError(
                owner_id=owner_id,
                epoch=epoch,
                reason="lease_absent",
                lease=None,
            )
        if current.state is not RuntimeLeaseState.ACTIVE:
            reason = "lease_released"
        elif current.owner_id != owner_id:
            reason = "owner_mismatch"
        elif current.epoch != epoch:
            reason = "epoch_mismatch"
        elif current.expires_at <= now:
            reason = "lease_expired"
        else:
            return current
        raise RuntimeLeaseLostError(
            owner_id=owner_id,
            epoch=epoch,
            reason=reason,
            lease=current,
        )

    @staticmethod
    def _assert_center_lane_uncommitted(
        conn: sqlite3.Connection,
        center_id: str,
        lane: CenterLane,
    ) -> None:
        row = conn.execute(
            "SELECT kind FROM activity_centers WHERE id = ?",
            (center_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"ActivityCenter {center_id!r} not found")
        expected_lane = center_lane_for_activity_kind(row[0])
        if lane is not expected_lane:
            raise ValueError(
                f"center {center_id!r} maps to {expected_lane.value!r}, "
                f"not {lane.value!r}"
            )

    @staticmethod
    def _assert_reservation_event_uncommitted(
        conn: sqlite3.Connection,
        reservation: CenterReservation,
    ) -> None:
        row = conn.execute(
            "SELECT world_id, engram_id, center_id, status "
            "FROM causal_events WHERE id = ?",
            (reservation.event_id,),
        ).fetchone()
        if row is None:
            raise ValueError(
                f"causal event {reservation.event_id!r} not found"
            )
        expected = (
            reservation.world_id,
            reservation.engram_id,
            reservation.center_id,
        )
        if tuple(row[:3]) != expected:
            raise ValueError(
                "reservation world/engram/center must match its causal event"
            )
        if row[3] != "queued":
            raise ValueError("only queued causal events may be reserved")

    @staticmethod
    def _runtime_lease_values(lease: RuntimeLease) -> tuple[object, ...]:
        return (
            lease.scope,
            lease.owner_id,
            lease.epoch,
            lease.state.value,
            _ts(lease.acquired_at),
            _ts(lease.renewed_at),
            _ts(lease.expires_at),
            None if lease.released_at is None else _ts(lease.released_at),
        )

    @staticmethod
    def _center_schedule_state_values(
        state: CenterScheduleState,
    ) -> tuple[object, ...]:
        return (
            state.center_id,
            state.lane.value,
            state.decision.value,
            state.reason.value,
            state.starvation_debt,
            None if state.waiting_since is None else _ts(state.waiting_since),
            (
                None
                if state.last_admitted_at is None
                else _ts(state.last_admitted_at)
            ),
            _ts(state.last_decision_at),
            _ts(state.updated_at),
        )

    @staticmethod
    def _center_reservation_values(
        reservation: CenterReservation,
    ) -> tuple[object, ...]:
        return (
            reservation.id,
            reservation.world_id,
            reservation.event_id,
            reservation.engram_id,
            reservation.center_id,
            reservation.lane.value,
            reservation.owner_id,
            reservation.lease_epoch,
            reservation.state.value,
            (
                None
                if reservation.outcome is None
                else reservation.outcome.value
            ),
            reservation.reason.value,
            reservation.base_priority,
            reservation.effective_score,
            _ts(reservation.created_at),
            None if reservation.settled_at is None else _ts(reservation.settled_at),
        )

    @staticmethod
    def _row_to_runtime_lease(row: tuple) -> RuntimeLease:
        return RuntimeLease(
            scope=row[0],
            owner_id=row[1],
            epoch=row[2],
            state=RuntimeLeaseState(row[3]),
            acquired_at=_parse_ts(row[4]),
            renewed_at=_parse_ts(row[5]),
            expires_at=_parse_ts(row[6]),
            released_at=_parse_ts(row[7]) if row[7] is not None else None,
        )

    @staticmethod
    def _row_to_center_schedule_state(row: tuple) -> CenterScheduleState:
        return CenterScheduleState(
            center_id=row[0],
            lane=CenterLane(row[1]),
            decision=row[2],
            reason=row[3],
            starvation_debt=row[4],
            waiting_since=_parse_ts(row[5]) if row[5] is not None else None,
            last_admitted_at=(
                _parse_ts(row[6]) if row[6] is not None else None
            ),
            last_decision_at=_parse_ts(row[7]),
            updated_at=_parse_ts(row[8]),
        )

    @staticmethod
    def _row_to_center_reservation(row: tuple) -> CenterReservation:
        return CenterReservation(
            id=row[0],
            world_id=row[1],
            event_id=row[2],
            engram_id=row[3],
            center_id=row[4],
            lane=CenterLane(row[5]),
            owner_id=row[6],
            lease_epoch=row[7],
            state=CenterReservationState(row[8]),
            outcome=(
                CenterReservationOutcome(row[9])
                if row[9] is not None
                else None
            ),
            reason=row[10],
            base_priority=row[11],
            effective_score=row[12],
            created_at=_parse_ts(row[13]),
            settled_at=_parse_ts(row[14]) if row[14] is not None else None,
        )

    # ── Engram CRUD ──────────────────────────────────────────────

    @_locked
    def create_engram(
        self,
        engram_id: str | None = None,
        project_id: str | None = None,
        initial_messages: list[Message] | None = None,
        *,
        auto_name: bool = True,
        name: str | None = None,
        name_origin: str = "auto",
        nickname: str | None = None,
    ) -> Engram:
        self._assert_runtime_publication()
        engram = self._create_engram_uncommitted(
            engram_id=engram_id,
            project_id=project_id,
            initial_messages=initial_messages,
            auto_name=auto_name,
            name=name,
            name_origin=name_origin,
            nickname=nickname,
        )
        self._conn.commit()
        return engram

    @_locked
    def create_provisional_engram(
        self,
        engram_id: str | None,
        project_id: str | None,
        initial_messages: list[Message],
        *,
        token_count: int,
        name: str | None = None,
        name_origin: str = "auto",
        nickname: str | None = None,
        runtime_owner_id: str | None = None,
        runtime_lease_epoch: int | None = None,
        runtime_now: datetime | None = None,
    ) -> Engram:
        """Create one non-admissible succession candidate atomically.

        The identity row, summary seed, metadata and ``provisional`` status
        share one transaction.  A failed message/metadata write therefore
        cannot leave a second ACTIVE Engram behind.  Runtime callers may also
        bind the insertion to their current lease epoch.
        """

        if (runtime_owner_id is None) != (runtime_lease_epoch is None):
            raise ValueError(
                "runtime_owner_id and runtime_lease_epoch must be provided together"
            )
        if (
            isinstance(token_count, bool)
            or not isinstance(token_count, int)
            or token_count < 0
        ):
            raise ValueError("token_count must be an integer >= 0")
        with self._immediate_transaction() as conn:
            self._assert_runtime_publication()
            if runtime_owner_id is not None:
                assert runtime_lease_epoch is not None
                self._assert_runtime_lease_uncommitted(
                    runtime_owner_id,
                    runtime_lease_epoch,
                    _now() if runtime_now is None else runtime_now,
                    conn,
                )
            return self._create_engram_uncommitted(
                engram_id=engram_id,
                project_id=project_id,
                initial_messages=initial_messages,
                auto_name=False,
                name=name,
                name_origin=name_origin,
                nickname=nickname,
                status=EngramStatus.PROVISIONAL,
                token_count=token_count,
            )

    def _create_engram_uncommitted(
        self,
        engram_id: str | None = None,
        project_id: str | None = None,
        initial_messages: list[Message] | None = None,
        *,
        auto_name: bool = True,
        name: str | None = None,
        name_origin: str = "auto",
        nickname: str | None = None,
        status: EngramStatus = EngramStatus.ACTIVE,
        token_count: int = 0,
    ) -> Engram:
        """Insert an Engram without committing, for aggregate transactions."""
        from pulse_system.core.types.models import _uuid

        eid = engram_id or _uuid()
        now = _now()
        generated_name = name
        if generated_name is None and auto_name and initial_messages:
            generated_name = next(
                (
                    candidate
                    for message in initial_messages
                    if (candidate := session_name(message.content)) is not None
                ),
                None,
            )
        if not isinstance(status, EngramStatus):
            raise ValueError("status must be an EngramStatus")
        if (
            isinstance(token_count, bool)
            or not isinstance(token_count, int)
            or token_count < 0
        ):
            raise ValueError("token_count must be an integer >= 0")
        engram = Engram(
            id=eid,
            project_id=project_id,
            status=status,
            created_at=now,
            name=generated_name,
            name_origin=name_origin,
            nickname=nickname,
        )
        engram.metadata.token_count = token_count
        self._conn.execute(
            """INSERT INTO engrams
               (id, project_id, status, created_at, total_pulses,
                recent_activity, self_excitability, token_count,
                name, name_origin, nickname)
               VALUES (?, ?, ?, ?, 0, 0.0, 0.1, ?, ?, ?, ?)""",
            (
                eid,
                project_id,
                engram.status.value,
                _ts(now),
                token_count,
                generated_name,
                name_origin,
                nickname,
            ),
        )
        if initial_messages:
            for msg in initial_messages:
                self._insert_message(eid, msg)
        return engram

    @_locked
    def get_engram(self, engram_id: str) -> Engram | None:
        row = self._conn.execute(
            "SELECT * FROM engrams WHERE id = ?", (engram_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_engram(row)

    @_locked
    def list_engrams(
        self, status: EngramStatus | None = None, project_id: str | None = None
    ) -> list[Engram]:
        query = "SELECT * FROM engrams WHERE 1=1"
        params: list = []
        if status is not None:
            query += " AND status = ?"
            params.append(status.value)
        if project_id is not None:
            query += " AND project_id = ?"
            params.append(project_id)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_engram(r) for r in rows]

    @_locked
    def archive_engram(self, engram_id: str) -> bool:
        self._assert_runtime_publication()
        cur = self._conn.execute(
            "UPDATE engrams SET status = ? WHERE id = ?",
            (EngramStatus.ARCHIVED.value, engram_id),
        )
        self._conn.commit()
        return cur.rowcount > 0

    @_locked
    def mark_engram_provisional(self, engram_id: str) -> None:
        """Hide a newly-created succession candidate from live admission."""

        self._assert_runtime_publication()
        cur = self._conn.execute(
            "UPDATE engrams SET status = ? WHERE id = ? AND status = ?",
            (
                EngramStatus.PROVISIONAL.value,
                engram_id,
                EngramStatus.ACTIVE.value,
            ),
        )
        if cur.rowcount != 1:
            self._conn.rollback()
            raise ValueError(
                "succession candidate is missing or no longer newly active"
            )
        self._conn.commit()

    @_locked
    def commit_engram_succession_status(
        self,
        predecessor_id: str,
        successor_id: str,
    ) -> None:
        """Atomically make one provisional candidate live and archive its predecessor.

        Succession candidates are stored as ``provisional`` while Pi prepares the
        lineage so normal admission cannot mistake a provisional identity for
        a second living subject.  This compare-and-set is the visibility
        boundary; it never revives an arbitrary archived Engram.
        """

        if predecessor_id == successor_id:
            raise ValueError("predecessor_id and successor_id must differ")
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            self._assert_runtime_publication()
            successor = self._conn.execute(
                "UPDATE engrams SET status = ? WHERE id = ? AND status = ?",
                (
                    EngramStatus.ACTIVE.value,
                    successor_id,
                    EngramStatus.PROVISIONAL.value,
                ),
            )
            if successor.rowcount != 1:
                raise ValueError(
                    "succession candidate is missing or no longer provisional"
                )
            predecessor = self._conn.execute(
                "UPDATE engrams SET status = ? WHERE id = ? AND status = ?",
                (
                    EngramStatus.ARCHIVED.value,
                    predecessor_id,
                    EngramStatus.ACTIVE.value,
                ),
            )
            if predecessor.rowcount != 1:
                raise ValueError(
                    "succession predecessor is missing or no longer active"
                )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    @_locked
    def ensure_auto_name(self, engram_id: str, content: str) -> bool:
        """Name an unnamed session once; never overwrite a user rename."""
        generated = session_name(content)
        if generated is None:
            return False
        self._assert_runtime_publication()
        cur = self._conn.execute(
            """UPDATE engrams SET name = ?
               WHERE id = ? AND name IS NULL AND name_origin = 'auto'""",
            (generated, engram_id),
        )
        self._conn.commit()
        return cur.rowcount > 0

    @_locked
    def update_engram_identity(
        self,
        engram_id: str,
        updates: dict[str, str | None],
    ) -> Engram | None:
        """Apply user-authored name/nickname changes to one engram."""
        sets: list[str] = []
        params: list[str | None] = []
        if "name" in updates:
            sets.extend(["name = ?", "name_origin = 'user'"])
            params.append(updates["name"])
        if "nickname" in updates:
            sets.append("nickname = ?")
            params.append(updates["nickname"])
        if not sets:
            return self.get_engram(engram_id)
        self._assert_runtime_publication()
        params.append(engram_id)
        cur = self._conn.execute(
            f"UPDATE engrams SET {', '.join(sets)} WHERE id = ?",
            params,
        )
        self._conn.commit()
        if cur.rowcount == 0:
            return None
        row = self._conn.execute(
            "SELECT * FROM engrams WHERE id = ?", (engram_id,)
        ).fetchone()
        return self._row_to_engram(row) if row is not None else None

    @_locked
    def update_engram_metadata(
        self,
        engram_id: str,
        last_pulse_at: datetime | None = None,
        total_pulses: int | None = None,
        recent_activity: float | None = None,
        self_excitability: float | None = None,
        token_count: int | None = None,
    ) -> bool:
        sets: list[str] = []
        params: list = []
        if last_pulse_at is not None:
            sets.append("last_pulse_at = ?")
            params.append(_ts(last_pulse_at))
        if total_pulses is not None:
            sets.append("total_pulses = ?")
            params.append(total_pulses)
        if recent_activity is not None:
            sets.append("recent_activity = ?")
            params.append(recent_activity)
        if self_excitability is not None:
            sets.append("self_excitability = ?")
            params.append(self_excitability)
        if token_count is not None:
            sets.append("token_count = ?")
            params.append(token_count)
        if not sets:
            return False
        self._assert_runtime_publication()
        params.append(engram_id)
        cur = self._conn.execute(
            f"UPDATE engrams SET {', '.join(sets)} WHERE id = ?", params
        )
        self._conn.commit()
        return cur.rowcount > 0

    # ── Message / Session (append-only) ──────────────────────────

    @_locked
    def append_message(self, engram_id: str, message: Message) -> None:
        self._assert_runtime_publication()
        self._insert_message(engram_id, message)
        self._conn.commit()

    @_locked
    def append_messages(self, engram_id: str, messages: list[Message]) -> None:
        self._assert_runtime_publication()
        for msg in messages:
            self._insert_message(engram_id, msg)
        self._conn.commit()

    @_locked
    def get_session(
        self, engram_id: str, limit: int | None = None
    ) -> list[Message]:
        """Return the session history in chronological order.

        With `limit`, returns the most recent N messages (still oldest-first)
        — callers limiting a session want recent context, not its opening.
        """
        if limit is not None:
            query = (
                "SELECT role, content, timestamp, source_engram_id FROM ("
                "  SELECT id, role, content, timestamp, source_engram_id"
                "  FROM messages WHERE engram_id = ? ORDER BY id DESC LIMIT ?"
                ") ORDER BY id ASC"
            )
            params: list = [engram_id, limit]
        else:
            query = (
                "SELECT role, content, timestamp, source_engram_id "
                "FROM messages WHERE engram_id = ? ORDER BY id ASC"
            )
            params = [engram_id]
        rows = self._conn.execute(query, params).fetchall()
        return [
            Message(
                role=MessageRole(r[0]),
                content=r[1],
                timestamp=_parse_ts(r[2]),
                source_engram_id=r[3],
            )
            for r in rows
        ]

    @_locked
    def get_message_count(self, engram_id: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM messages WHERE engram_id = ?", (engram_id,)
        ).fetchone()
        return row[0] if row else 0

    # ── Connection CRUD ──────────────────────────────────────────

    @_locked
    def create_connection(
        self,
        from_id: str,
        to_id: str,
        weight: float,
        conn_type: ConnectionType = ConnectionType.EXCITATORY,
        *,
        layer: str = "factory",
    ) -> Connection:
        if layer not in {"factory", "field"}:
            raise ValueError("connection layer must be 'factory' or 'field'")
        self._assert_runtime_publication()
        now = _now()
        clamped = max(0.0, min(1.0, weight))
        restored_factory = None
        if layer == "factory":
            restored_factory = self._conn.execute(
                """SELECT weight, conn_type, created_at, last_activated_at
                   FROM factory_connections
                   WHERE from_id = ? AND to_id = ?""",
                (from_id, to_id),
            ).fetchone()
            if restored_factory is not None:
                # A pruned factory edge remains in the reset table. Recreating
                # it restores that immutable baseline instead of silently
                # replacing its provenance with a new initializer value.
                clamped = float(restored_factory[0])
                conn_type = ConnectionType(restored_factory[1])
        factory_weight = clamped if layer == "factory" else 0.0
        learned_weight = clamped if layer == "field" else None
        conn = Connection(
            from_id=from_id,
            to_id=to_id,
            weight=clamped,
            conn_type=conn_type,
            created_at=now,
            last_activated_at=now,
            factory_weight=factory_weight,
            learned_weight=learned_weight,
        )
        self._conn.execute(
            """INSERT INTO connections
               (from_id, to_id, weight, conn_type, created_at,
                last_activated_at, learned_weight)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                from_id, to_id, clamped, conn_type.value, _ts(now), _ts(now),
                learned_weight,
            ),
        )
        if layer == "factory" and restored_factory is None:
            self._conn.execute(
                """INSERT INTO factory_connections
                   (from_id, to_id, weight, conn_type, created_at,
                    last_activated_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    from_id, to_id, clamped, conn_type.value, _ts(now),
                    _ts(now),
                ),
            )
        self._conn.commit()
        return conn

    @_locked
    def get_connection(self, from_id: str, to_id: str) -> Connection | None:
        row = self._conn.execute(
            """SELECT c.*, COALESCE(f.weight, 0.0)
               FROM connections AS c
               LEFT JOIN factory_connections AS f
                 ON f.from_id = c.from_id AND f.to_id = c.to_id
               WHERE c.from_id = ? AND c.to_id = ?""",
            (from_id, to_id),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_connection(row)

    @_locked
    def get_outgoing(
        self, engram_id: str, min_weight: float | None = None
    ) -> list[Connection]:
        query = (
            "SELECT c.*, COALESCE(f.weight, 0.0) "
            "FROM connections AS c "
            "LEFT JOIN factory_connections AS f "
            "ON f.from_id = c.from_id AND f.to_id = c.to_id "
            "WHERE c.from_id = ?"
        )
        params: list = [engram_id]
        if min_weight is not None:
            query += " AND c.weight >= ?"
            params.append(min_weight)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_connection(r) for r in rows]

    @_locked
    def get_incoming(
        self, engram_id: str, min_weight: float | None = None
    ) -> list[Connection]:
        query = (
            "SELECT c.*, COALESCE(f.weight, 0.0) "
            "FROM connections AS c "
            "LEFT JOIN factory_connections AS f "
            "ON f.from_id = c.from_id AND f.to_id = c.to_id "
            "WHERE c.to_id = ?"
        )
        params: list = [engram_id]
        if min_weight is not None:
            query += " AND c.weight >= ?"
            params.append(min_weight)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_connection(r) for r in rows]

    @_locked
    def update_weight(
        self,
        from_id: str,
        to_id: str,
        new_weight: float,
        *,
        layer: str = "field",
    ) -> bool:
        if layer not in {"factory", "field"}:
            raise ValueError("connection layer must be 'factory' or 'field'")
        self._assert_runtime_publication()
        clamped = max(0.0, min(1.0, new_weight))
        now = _now()
        if layer == "field":
            cur = self._conn.execute(
                """UPDATE connections
                   SET weight = ?, learned_weight = ?, last_activated_at = ?
                   WHERE from_id = ? AND to_id = ?""",
                (clamped, clamped, _ts(now), from_id, to_id),
            )
        else:
            cur = self._conn.execute(
                """UPDATE factory_connections
                   SET weight = ?, last_activated_at = ?
                   WHERE from_id = ? AND to_id = ?""",
                (clamped, _ts(now), from_id, to_id),
            )
            self._conn.execute(
                """UPDATE connections
                   SET weight = ?, last_activated_at = ?
                   WHERE from_id = ? AND to_id = ?
                     AND learned_weight IS NULL""",
                (clamped, _ts(now), from_id, to_id),
            )
        self._conn.commit()
        return cur.rowcount > 0

    @_locked
    def decay_all(self, rate: float) -> int:
        """Multiply all connection weights by (1 - rate). Returns count of updated rows."""
        self._assert_runtime_publication()
        factor = 1.0 - rate
        cur = self._conn.execute(
            """UPDATE connections
               SET weight = weight * ?, learned_weight = weight * ?""",
            (factor, factor),
        )
        self._conn.commit()
        return cur.rowcount

    @_locked
    def decay_recent_activity(self, factor: float) -> int:
        """Multiply recent_activity of all ACTIVE engrams by factor.

        One bulk UPDATE (same shape as decay_all) so the per-tick decay step
        stays cheap regardless of population size. Returns rows updated.
        """
        self._assert_runtime_publication()
        cur = self._conn.execute(
            "UPDATE engrams SET recent_activity = recent_activity * ? "
            "WHERE status = ?",
            (factor, EngramStatus.ACTIVE.value),
        )
        self._conn.commit()
        return cur.rowcount

    @_locked
    def prune(self, threshold: float) -> int:
        """Delete connections with weight below threshold. Returns count of deleted rows."""
        self._assert_runtime_publication()
        cur = self._conn.execute(
            "DELETE FROM connections WHERE weight < ?", (threshold,)
        )
        self._conn.commit()
        return cur.rowcount

    @_locked
    def weight_summary(self) -> dict:
        """Distribution summary of connection weights (runtime metrics)."""
        row = self._conn.execute(
            "SELECT COUNT(*), AVG(weight), MIN(weight), MAX(weight) FROM connections"
        ).fetchone()
        count = row[0] or 0
        if count == 0:
            return {"count": 0, "avg": 0.0, "min": 0.0, "max": 0.0, "median": 0.0}
        median_row = self._conn.execute(
            "SELECT weight FROM connections ORDER BY weight LIMIT 1 OFFSET ?",
            (count // 2,),
        ).fetchone()
        return {
            "count": count,
            "avg": row[1],
            "min": row[2],
            "max": row[3],
            "median": median_row[0] if median_row else 0.0,
        }

    @_locked
    def list_all_connections(self) -> list[Connection]:
        """Every edge in the network (metrics topology snapshot).

        Whole-graph read for the sideband observer only — the tick path uses
        get_outgoing/get_incoming, which are indexed per engram.
        """
        rows = self._conn.execute(
            """SELECT c.*, COALESCE(f.weight, 0.0)
               FROM connections AS c
               LEFT JOIN factory_connections AS f
                 ON f.from_id = c.from_id AND f.to_id = c.to_id"""
        ).fetchall()
        return [self._row_to_connection(r) for r in rows]

    @_locked
    def count_connections(self, engram_id: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM connections WHERE from_id = ? OR to_id = ?",
            (engram_id, engram_id),
        ).fetchone()
        return row[0] if row else 0

    @_locked
    def transfer_connections(self, old_id: str, new_id: str) -> None:
        """Transfer all connections from old engram to new engram (for succession).

        Safe when both engrams share a counterpart: conflicting edges are
        merged keeping the larger weight in each layer. Factory provenance
        travels independently from the effective/field layer, so a later
        field reset still returns the successor to the correct baseline.
        Edges between old and new (and any self-loops) are dropped — they
        would collapse into new→new.
        """
        self._assert_runtime_publication()
        self._transfer_connection_table(
            "connections", old_id, new_id, has_learned=True
        )
        self._transfer_connection_table(
            "factory_connections", old_id, new_id, has_learned=False
        )
        self._conn.commit()

    def _transfer_connection_table(
        self,
        table: str,
        old_id: str,
        new_id: str,
        *,
        has_learned: bool,
    ) -> None:
        """Re-key and max-merge one trusted connection table."""
        if table not in {"connections", "factory_connections"}:
            raise ValueError("unexpected connection table")
        columns = (
            "from_id, to_id, weight, conn_type, created_at, "
            "last_activated_at"
        )
        if has_learned:
            columns += ", learned_weight"
        rows = self._conn.execute(
            f"SELECT {columns} FROM {table} "
            "WHERE from_id IN (?, ?) OR to_id IN (?, ?)",
            (old_id, new_id, old_id, new_id),
        ).fetchall()
        self._conn.execute(
            f"DELETE FROM {table} "
            "WHERE from_id IN (?, ?) OR to_id IN (?, ?)",
            (old_id, new_id, old_id, new_id),
        )

        merged: dict[tuple[str, str], tuple] = {}
        for row in rows:
            from_id = new_id if row[0] == old_id else row[0]
            to_id = new_id if row[1] == old_id else row[1]
            if from_id == to_id:
                continue
            candidate = (from_id, to_id, *row[2:])
            key = (from_id, to_id)
            previous = merged.get(key)
            if previous is None or candidate[2] > previous[2]:
                merged[key] = candidate

        placeholders = ", ".join("?" for _ in columns.split(","))
        self._conn.executemany(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
            list(merged.values()),
        )

    # ── Project CRUD ────────────────────────────────────────────

    @_locked
    def create_project(
        self,
        name: str,
        description: str = "",
        workspace_path: str | None = None,
        project_id: str | None = None,
    ) -> Project:
        from pulse_system.core.types.models import _uuid

        pid = project_id or _uuid()
        now = _now()
        project = Project(
            id=pid, name=name, description=description,
            workspace_path=workspace_path, created_at=now,
        )
        self._conn.execute(
            """INSERT INTO projects (id, name, description, workspace_path, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (pid, name, description, workspace_path, _ts(now)),
        )
        self._conn.commit()
        return project

    @_locked
    def get_project(self, project_id: str) -> Project | None:
        row = self._conn.execute(
            "SELECT * FROM projects WHERE id = ?", (project_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_project(row)

    @_locked
    def list_projects(self) -> list[Project]:
        rows = self._conn.execute("SELECT * FROM projects").fetchall()
        return [self._row_to_project(r) for r in rows]

    @_locked
    def update_project(self, project_id: str, **kwargs) -> bool:
        allowed = {"name", "description", "workspace_path", "index_engram_id"}
        sets: list[str] = []
        params: list = []
        for k, v in kwargs.items():
            if k in allowed:
                sets.append(f"{k} = ?")
                params.append(v)
        if not sets:
            return False
        params.append(project_id)
        cur = self._conn.execute(
            f"UPDATE projects SET {', '.join(sets)} WHERE id = ?", params
        )
        self._conn.commit()
        return cur.rowcount > 0

    @_locked
    def set_substrate_binding(self, engram_id: str, binding: str | None) -> bool:
        self._assert_runtime_publication()
        cur = self._conn.execute(
            "UPDATE engrams SET substrate_binding = ? WHERE id = ?",
            (binding, engram_id),
        )
        self._conn.commit()
        return cur.rowcount > 0

    @_locked
    def update_engram_project(self, engram_id: str, project_id: str | None) -> bool:
        self._assert_runtime_publication()
        cur = self._conn.execute(
            "UPDATE engrams SET project_id = ? WHERE id = ?",
            (project_id, engram_id),
        )
        self._conn.commit()
        return cur.rowcount > 0

    # ── PulseWorld fronts and life centers ─────────────────────

    @_locked
    def create_task_bundle(
        self,
        title: str,
        description: str = "",
        project_id: str | None = None,
        *,
        origin: ActivityOrigin | str = ActivityOrigin.USER,
        autonomy: float = 1.0,
        center_id: str | None = None,
        front_id: str | None = None,
        engram_id: str | None = None,
        focal_engram_id: str | None = None,
    ) -> TaskFrontBundle:
        """Atomically create a task Center, focal Engram and TaskFront."""

        from pulse_system.core.types.models import _uuid

        focal = self._resolve_focal_id(engram_id, focal_engram_id)
        center = ActivityCenter(
            id=center_id or _uuid(),
            kind=ActivityKind.TASK,
            title=title,
            description=description,
            origin=origin,
            autonomy=autonomy,
            project_id=project_id,
            focal_engram_id=focal,
        )
        membership = CenterMembership(
            center_id=center.id,
            engram_id=focal,
            relation=MembershipRelation.FOCAL,
            created_at=center.created_at,
        )
        front = TaskFront(
            id=front_id or _uuid(),
            center_id=center.id,
            focal_engram_id=focal,
            title=center.title,
            created_at=center.created_at,
            updated_at=center.updated_at,
            last_opened_at=center.created_at,
        )

        try:
            self._conn.execute("BEGIN")
            focal_engram = self._create_engram_uncommitted(
                engram_id=focal,
                project_id=project_id,
                auto_name=False,
                name=center.title,
            )
            self._insert_activity_center(center)
            self._insert_center_membership(membership)
            self._insert_task_front(front)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return TaskFrontBundle(front, center, membership, focal_engram)

    @_locked
    def create_task_bundle_for_existing_engram(
        self,
        title: str,
        focal_engram_id: str,
        description: str = "",
        project_id: str | None = None,
        *,
        origin: ActivityOrigin | str = ActivityOrigin.USER,
        autonomy: float = 1.0,
        center_id: str | None = None,
        front_id: str | None = None,
    ) -> TaskFrontBundle:
        """Atomically create a task bundle for one existing active Engram."""

        from pulse_system.core.types.models import _uuid

        if not isinstance(focal_engram_id, str) or not focal_engram_id.strip():
            raise ValueError("focal_engram_id must be a non-empty string")
        center = ActivityCenter(
            id=center_id or _uuid(),
            kind=ActivityKind.TASK,
            title=title,
            description=description,
            origin=origin,
            autonomy=autonomy,
            project_id=project_id,
            focal_engram_id=focal_engram_id,
        )
        membership = CenterMembership(
            center_id=center.id,
            engram_id=focal_engram_id,
            relation=MembershipRelation.FOCAL,
            created_at=center.created_at,
        )
        front = TaskFront(
            id=front_id or _uuid(),
            center_id=center.id,
            focal_engram_id=focal_engram_id,
            title=center.title,
            created_at=center.created_at,
            updated_at=center.updated_at,
            last_opened_at=center.created_at,
        )

        with self._immediate_transaction() as conn:
            row = conn.execute(
                "SELECT * FROM engrams WHERE id = ?",
                (focal_engram_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Engram {focal_engram_id} not found")
            focal_engram = self._row_to_engram(row)
            if focal_engram.status is not EngramStatus.ACTIVE:
                raise ValueError(f"Engram {focal_engram_id} is not active")
            self._insert_activity_center(center)
            self._insert_center_membership(membership)
            self._insert_task_front(front)

        return TaskFrontBundle(front, center, membership, focal_engram)

    @_locked
    def create_non_task_center_bundle(
        self,
        kind: ActivityKind | str,
        title: str,
        description: str = "",
        project_id: str | None = None,
        *,
        origin: ActivityOrigin | str = ActivityOrigin.USER,
        autonomy: float = 1.0,
        center_id: str | None = None,
        engram_id: str | None = None,
        focal_engram_id: str | None = None,
    ) -> ActivityCenterBundle:
        """Atomically create a non-task life Center and its focal Engram."""

        from pulse_system.core.types.models import _uuid

        normalized_kind = _enum_value(kind, ActivityKind, "kind")
        if normalized_kind is ActivityKind.TASK:
            raise ValueError(
                "task ActivityCenters must be created with create_task_bundle"
            )
        focal = self._resolve_focal_id(engram_id, focal_engram_id)
        center = ActivityCenter(
            id=center_id or _uuid(),
            kind=normalized_kind,
            title=title,
            description=description,
            origin=origin,
            autonomy=autonomy,
            project_id=project_id,
            focal_engram_id=focal,
        )
        membership = CenterMembership(
            center_id=center.id,
            engram_id=focal,
            relation=MembershipRelation.FOCAL,
            created_at=center.created_at,
        )
        try:
            self._conn.execute("BEGIN")
            focal_engram = self._create_engram_uncommitted(
                engram_id=focal,
                project_id=project_id,
                auto_name=False,
                name=center.title,
            )
            self._insert_activity_center(center)
            self._insert_center_membership(membership)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return ActivityCenterBundle(center, membership, focal_engram)

    @_locked
    def create_center_for_existing_engram(
        self,
        kind: ActivityKind | str,
        title: str,
        focal_engram_id: str,
        description: str = "",
        project_id: str | None = None,
        *,
        origin: ActivityOrigin | str = ActivityOrigin.SYSTEM,
        autonomy: float = 1.0,
        center_id: str | None = None,
    ) -> ActivityCenterBundle:
        """Atomically wrap an existing Engram in a new Center for migration."""

        from pulse_system.core.types.models import _uuid

        focal_engram = self.get_engram(focal_engram_id)
        if focal_engram is None:
            raise ValueError(f"Engram {focal_engram_id} not found")
        center = ActivityCenter(
            id=center_id or _uuid(),
            kind=kind,
            title=title,
            description=description,
            origin=origin,
            autonomy=autonomy,
            project_id=project_id,
            focal_engram_id=focal_engram_id,
        )
        membership = CenterMembership(
            center.id,
            focal_engram_id,
            MembershipRelation.FOCAL,
            center.created_at,
        )
        try:
            self._conn.execute("BEGIN")
            self._insert_activity_center(center)
            self._insert_center_membership(membership)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return ActivityCenterBundle(center, membership, focal_engram)

    @_locked
    def create_task_front_for_center(
        self,
        center_id: str,
        focal_engram_id: str,
        title: str,
        *,
        status: TaskFrontStatus | str = TaskFrontStatus.OPEN,
        front_id: str | None = None,
    ) -> TaskFront:
        """Attach a Front to an existing task Center (legacy migration path)."""

        from pulse_system.core.types.models import _uuid

        center = self._get_activity_center_unlocked(center_id)
        if center is None:
            raise ValueError(f"ActivityCenter {center_id} not found")
        if center.kind is not ActivityKind.TASK:
            raise ValueError("TaskFront must reference a task ActivityCenter")
        if center.focal_engram_id != focal_engram_id:
            raise ValueError("TaskFront focal Engram must match its ActivityCenter")
        membership = self._conn.execute(
            "SELECT relation FROM center_memberships "
            "WHERE center_id = ? AND engram_id = ?",
            (center_id, focal_engram_id),
        ).fetchone()
        if membership is None or membership[0] != MembershipRelation.FOCAL.value:
            raise ValueError("TaskFront focal Engram has no focal membership")
        now = _now()
        front = TaskFront(
            id=front_id or _uuid(),
            center_id=center_id,
            focal_engram_id=focal_engram_id,
            title=title,
            status=status,
            created_at=now,
            updated_at=now,
            last_opened_at=now,
        )
        self._insert_task_front(front)
        self._conn.commit()
        return front

    @_locked
    def get_activity_center(self, center_id: str) -> ActivityCenter | None:
        return self._get_activity_center_unlocked(center_id)

    @_locked
    def list_activity_centers(
        self,
        *,
        kind: ActivityKind | str | None = None,
        status: ActivityCenterStatus | str | None = None,
        origin: ActivityOrigin | str | None = None,
        project_id: str | None = None,
        engram_id: str | None = None,
    ) -> list[ActivityCenter]:
        query = (
            "SELECT DISTINCT c.id, c.kind, c.title, c.description, c.status, "
            "c.origin, c.autonomy, c.project_id, c.focal_engram_id, "
            "c.created_at, c.updated_at, c.last_active_at "
            "FROM activity_centers c"
        )
        params: list = []
        clauses: list[str] = []
        if engram_id is not None:
            query += " JOIN center_memberships m ON m.center_id = c.id"
            clauses.append("m.engram_id = ?")
            params.append(engram_id)
        if kind is not None:
            clauses.append("c.kind = ?")
            params.append(_enum_value(kind, ActivityKind, "kind").value)
        if status is not None:
            clauses.append("c.status = ?")
            params.append(
                _enum_value(status, ActivityCenterStatus, "status").value
            )
        if origin is not None:
            clauses.append("c.origin = ?")
            params.append(_enum_value(origin, ActivityOrigin, "origin").value)
        if project_id is not None:
            clauses.append("c.project_id = ?")
            params.append(project_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY c.updated_at DESC, c.id"
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_activity_center(row) for row in rows]

    @_locked
    def update_activity_center(
        self,
        center_id: str,
        *,
        title: str | None = None,
        description: str | None = None,
        status: ActivityCenterStatus | str | None = None,
        autonomy: float | None = None,
    ) -> ActivityCenter | None:
        current = self._get_activity_center_unlocked(center_id)
        if current is None:
            return None
        updates = {}
        if title is not None:
            updates["title"] = title
        if description is not None:
            updates["description"] = description
        if status is not None:
            updates["status"] = status
        if autonomy is not None:
            updates["autonomy"] = autonomy
        if not updates:
            return current
        candidate = replace(current, **updates, updated_at=_now())
        self._conn.execute(
            "UPDATE activity_centers SET title = ?, description = ?, "
            "status = ?, autonomy = ?, updated_at = ? WHERE id = ?",
            (
                candidate.title,
                candidate.description,
                candidate.status.value,
                candidate.autonomy,
                _ts(candidate.updated_at),
                center_id,
            ),
        )
        self._conn.commit()
        return candidate

    @_locked
    def touch_activity_center(self, center_id: str) -> ActivityCenter | None:
        current = self._get_activity_center_unlocked(center_id)
        if current is None:
            return None
        now = _now()
        status = (
            ActivityCenterStatus.ACTIVE
            if current.status is ActivityCenterStatus.DORMANT
            else current.status
        )
        self._conn.execute(
            "UPDATE activity_centers SET status = ?, updated_at = ?, "
            "last_active_at = ? WHERE id = ?",
            (status.value, _ts(now), _ts(now), center_id),
        )
        self._conn.commit()
        return replace(
            current,
            status=status,
            updated_at=now,
            last_active_at=now,
        )

    @_locked
    def get_task_front(self, front_id: str) -> TaskFront | None:
        row = self._conn.execute(
            "SELECT id, center_id, focal_engram_id, title, status, "
            "created_at, updated_at, last_opened_at "
            "FROM task_fronts WHERE id = ?",
            (front_id,),
        ).fetchone()
        return self._row_to_task_front(row) if row is not None else None

    @_locked
    def list_task_fronts(
        self,
        *,
        status: TaskFrontStatus | str | None = None,
        center_id: str | None = None,
        focal_engram_id: str | None = None,
    ) -> list[TaskFront]:
        query = (
            "SELECT id, center_id, focal_engram_id, title, status, "
            "created_at, updated_at, last_opened_at FROM task_fronts"
        )
        clauses: list[str] = []
        params: list = []
        if status is not None:
            clauses.append("status = ?")
            params.append(_enum_value(status, TaskFrontStatus, "status").value)
        if center_id is not None:
            clauses.append("center_id = ?")
            params.append(center_id)
        if focal_engram_id is not None:
            clauses.append("focal_engram_id = ?")
            params.append(focal_engram_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY last_opened_at DESC, id"
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_task_front(row) for row in rows]

    @_locked
    def update_task_front(
        self,
        front_id: str,
        *,
        title: str | None = None,
        status: TaskFrontStatus | str | None = None,
    ) -> TaskFront | None:
        row = self._conn.execute(
            "SELECT id, center_id, focal_engram_id, title, status, "
            "created_at, updated_at, last_opened_at "
            "FROM task_fronts WHERE id = ?",
            (front_id,),
        ).fetchone()
        if row is None:
            return None
        current = self._row_to_task_front(row)
        updates = {}
        if title is not None:
            updates["title"] = title
        if status is not None:
            updates["status"] = status
        if not updates:
            return current
        now = _now()
        candidate = replace(current, **updates, updated_at=now)
        self._conn.execute(
            "UPDATE task_fronts SET title = ?, status = ?, updated_at = ? "
            "WHERE id = ?",
            (candidate.title, candidate.status.value, _ts(now), front_id),
        )
        self._conn.commit()
        return candidate

    @_locked
    def touch_task_front(self, front_id: str) -> TaskFront | None:
        row = self._conn.execute(
            "SELECT id, center_id, focal_engram_id, title, status, "
            "created_at, updated_at, last_opened_at "
            "FROM task_fronts WHERE id = ?",
            (front_id,),
        ).fetchone()
        if row is None:
            return None
        current = self._row_to_task_front(row)
        now = _now()
        self._conn.execute(
            "UPDATE task_fronts SET updated_at = ?, last_opened_at = ? "
            "WHERE id = ?",
            (_ts(now), _ts(now), front_id),
        )
        self._conn.commit()
        return replace(current, updated_at=now, last_opened_at=now)

    @_locked
    def add_center_membership(
        self,
        center_id: str,
        engram_id: str,
        relation: MembershipRelation | str = MembershipRelation.PARTICIPANT,
    ) -> CenterMembership:
        center = self._get_activity_center_unlocked(center_id)
        if center is None:
            raise ValueError(f"ActivityCenter {center_id} not found")
        if self._conn.execute(
            "SELECT 1 FROM engrams WHERE id = ?", (engram_id,)
        ).fetchone() is None:
            raise ValueError(f"Engram {engram_id} not found")
        normalized = _enum_value(relation, MembershipRelation, "relation")
        if normalized is MembershipRelation.FOCAL:
            if center.focal_engram_id not in (None, engram_id):
                raise ValueError("ActivityCenter already has a different focal Engram")
        membership = CenterMembership(center_id, engram_id, normalized)
        try:
            self._conn.execute("BEGIN")
            self._insert_center_membership(membership)
            if normalized is MembershipRelation.FOCAL:
                self._conn.execute(
                    "UPDATE activity_centers SET focal_engram_id = ?, "
                    "updated_at = ? WHERE id = ?",
                    (engram_id, _ts(_now()), center_id),
                )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return membership

    @_locked
    def list_center_memberships(
        self,
        *,
        center_id: str | None = None,
        engram_id: str | None = None,
        relation: MembershipRelation | str | None = None,
    ) -> list[CenterMembership]:
        query = (
            "SELECT center_id, engram_id, relation, created_at "
            "FROM center_memberships"
        )
        clauses: list[str] = []
        params: list = []
        if center_id is not None:
            clauses.append("center_id = ?")
            params.append(center_id)
        if engram_id is not None:
            clauses.append("engram_id = ?")
            params.append(engram_id)
        if relation is not None:
            clauses.append("relation = ?")
            params.append(
                _enum_value(relation, MembershipRelation, "relation").value
            )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at, center_id, engram_id"
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_center_membership(row) for row in rows]

    @_locked
    def activity_autonomy_for_engram(self, engram_id: str) -> float:
        """Sideband spontaneous multiplier; uncentered Engrams stay legacy-live."""

        rows = self._conn.execute(
            "SELECT c.status, c.autonomy FROM activity_centers c "
            "JOIN center_memberships m ON m.center_id = c.id "
            "WHERE m.engram_id = ?",
            (engram_id,),
        ).fetchall()
        if not rows:
            return 1.0
        active = [float(row[1]) for row in rows if row[0] == "active"]
        return max(active) if active else 0.0

    # ── Task offer persistence ─────────────────────────────────

    @staticmethod
    def _task_offer_columns() -> str:
        return (
            "id, world_id, subject_engram_id, status, current_revision, "
            "task_front_id, created_at, updated_at, decided_at, withdrawn_at"
        )

    @staticmethod
    def _task_offer_revision_columns() -> str:
        return (
            "offer_id, revision, content, title, project_id, "
            "latest_offer_event_id, decision, subject_response, "
            "decision_event_id, created_at, decided_at"
        )

    @_locked
    def get_task_offer(
        self,
        offer_id: str,
        *,
        world_id: str | None = None,
    ) -> TaskOffer | None:
        offer_id = self._require_storage_id(offer_id, "offer_id")
        if world_id is not None:
            world_id = self._require_storage_id(world_id, "world_id")
        return self._get_task_offer_uncommitted(
            self._conn,
            offer_id,
            world_id=world_id,
        )

    @_locked
    def list_task_offers(
        self,
        *,
        world_id: str | None = None,
        subject_engram_id: str | None = None,
        include_committed_predecessors: bool = False,
        status: TaskOfferStatus | str | None = None,
        limit: int = 100,
    ) -> list[TaskOffer]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
            raise ValueError("limit must be an integer between 1 and 500")
        clauses: list[str] = []
        params: list = []
        if world_id is not None:
            clauses.append("world_id = ?")
            params.append(self._require_storage_id(world_id, "world_id"))
        if subject_engram_id is not None:
            subject_engram_id = self._require_storage_id(
                subject_engram_id,
                "subject_engram_id",
            )
            if include_committed_predecessors:
                clauses.append(
                    "subject_engram_id IN ("
                    "WITH RECURSIVE lineage_engram(id) AS ("
                    "SELECT ? UNION "
                    "SELECT generation.predecessor_id "
                    "FROM generation_transitions generation "
                    "JOIN lineage_engram current "
                    "ON generation.successor_id = current.id "
                    "WHERE generation.state = 'committed'"
                    ") SELECT id FROM lineage_engram)"
                )
            else:
                clauses.append("subject_engram_id = ?")
            params.append(subject_engram_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(_enum_value(status, TaskOfferStatus, "status").value)
        query = "SELECT " + self._task_offer_columns() + " FROM task_offers"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC, id LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_task_offer(row) for row in rows]

    @_locked
    def get_task_offer_revision(
        self,
        offer_id: str,
        revision: int,
    ) -> TaskOfferRevision | None:
        offer_id = self._require_storage_id(offer_id, "offer_id")
        if (
            isinstance(revision, bool)
            or not isinstance(revision, int)
            or revision < 1
        ):
            raise ValueError("revision must be an integer >= 1")
        return self._get_task_offer_revision_uncommitted(
            self._conn,
            offer_id,
            revision,
        )

    @_locked
    def list_task_offer_revisions(
        self,
        offer_id: str,
    ) -> list[TaskOfferRevision]:
        offer_id = self._require_storage_id(offer_id, "offer_id")
        return self._list_task_offer_revisions_uncommitted(
            self._conn,
            offer_id,
        )

    # ── Accepted task relationship persistence ────────────────

    @staticmethod
    def _task_relationship_columns() -> str:
        return (
            "id, world_id, accepted_offer_id, task_front_id, center_id, "
            "original_subject_engram_id, current_subject_engram_id, status, "
            "revision, latest_terms_event_id, latest_subject_note, created_at, "
            "updated_at, exited_at"
        )

    @staticmethod
    def _task_relationship_event_columns() -> str:
        return (
            "relationship_id, seq, action, actor_kind, actor_id, before_status, "
            "after_status, content, source_event_id, created_at"
        )

    @_locked
    def get_task_relationship(
        self,
        relationship_id: str,
        *,
        world_id: str | None = None,
    ) -> TaskRelationship | None:
        relationship_id = self._require_storage_id(
            relationship_id,
            "relationship_id",
        )
        if world_id is not None:
            world_id = self._require_storage_id(world_id, "world_id")
        return self._get_task_relationship_uncommitted(
            self._conn,
            relationship_id,
            world_id=world_id,
        )

    @_locked
    def get_task_relationship_for_front(
        self,
        task_front_id: str,
        *,
        world_id: str | None = None,
    ) -> TaskRelationship | None:
        return self._get_task_relationship_by_unique_uncommitted(
            self._conn,
            "task_front_id",
            self._require_storage_id(task_front_id, "task_front_id"),
            world_id=world_id,
        )

    @_locked
    def get_task_relationship_for_center(
        self,
        center_id: str,
        *,
        world_id: str | None = None,
    ) -> TaskRelationship | None:
        return self._get_task_relationship_by_unique_uncommitted(
            self._conn,
            "center_id",
            self._require_storage_id(center_id, "center_id"),
            world_id=world_id,
        )

    @_locked
    def get_task_relationship_for_offer(
        self,
        accepted_offer_id: str,
        *,
        world_id: str | None = None,
    ) -> TaskRelationship | None:
        return self._get_task_relationship_by_unique_uncommitted(
            self._conn,
            "accepted_offer_id",
            self._require_storage_id(accepted_offer_id, "accepted_offer_id"),
            world_id=world_id,
        )

    @_locked
    def list_task_relationships(
        self,
        *,
        world_id: str | None = None,
        current_subject_engram_id: str | None = None,
        status: TaskRelationshipStatus | str | None = None,
        limit: int = 100,
    ) -> list[TaskRelationship]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
            raise ValueError("limit must be an integer between 1 and 500")
        clauses: list[str] = []
        params: list = []
        if world_id is not None:
            clauses.append("world_id = ?")
            params.append(self._require_storage_id(world_id, "world_id"))
        if current_subject_engram_id is not None:
            clauses.append("current_subject_engram_id = ?")
            params.append(self._require_storage_id(
                current_subject_engram_id,
                "current_subject_engram_id",
            ))
        if status is not None:
            clauses.append("status = ?")
            params.append(
                _enum_value(status, TaskRelationshipStatus, "status").value
            )
        query = "SELECT " + self._task_relationship_columns() + " FROM task_relationships"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC, id LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_task_relationship(row) for row in rows]

    @_locked
    def list_task_relationship_events(
        self,
        relationship_id: str,
    ) -> list[TaskRelationshipEvent]:
        relationship_id = self._require_storage_id(
            relationship_id,
            "relationship_id",
        )
        return self._list_task_relationship_events_uncommitted(
            self._conn,
            relationship_id,
        )

    # ── Living concern persistence ─────────────────────────────

    @_locked
    def create_living_concern(
        self,
        center_id: str,
        owner_engram_id: str,
        content: str,
        causal_id: str,
        source_event_id: str,
        *,
        disposition: LivingConcernDisposition | str = (
            LivingConcernDisposition.QUIET
        ),
        revisit_at: datetime | None = None,
        concern_id: str | None = None,
    ) -> LivingConcern:
        """Persist one subject-authored concern for a non-task Center."""

        from pulse_system.core.types.models import _uuid

        candidate = LivingConcern(
            id=concern_id or _uuid(),
            center_id=center_id,
            owner_engram_id=owner_engram_id,
            content=content,
            disposition=disposition,
            revisit_at=revisit_at,
            causal_id=causal_id,
            source_event_id=source_event_id,
        )
        if candidate.disposition is LivingConcernDisposition.RESOLVED:
            raise ValueError("a new LivingConcern cannot start resolved")
        self._validate_living_concern_context_unlocked(
            center_id=candidate.center_id,
            owner_engram_id=candidate.owner_engram_id,
            causal_id=candidate.causal_id,
            source_event_id=candidate.source_event_id,
        )
        if self._get_living_concern_unlocked(candidate.id) is not None:
            raise ValueError(f"LivingConcern {candidate.id} already exists")
        try:
            self._insert_living_concern(candidate)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return candidate

    @_locked
    def get_living_concern(self, concern_id: str) -> LivingConcern | None:
        concern_id = self._require_storage_id(concern_id, "concern_id")
        return self._get_living_concern_unlocked(concern_id)

    @_locked
    def update_living_concern(
        self,
        concern_id: str,
        *,
        expected_owner_engram_id: str,
        expected_revision: int,
        content: str,
        disposition: LivingConcernDisposition | str,
        revisit_at: datetime | None,
        causal_id: str,
        source_event_id: str,
    ) -> LivingConcern:
        """Apply one authored revision without allowing owner or Center theft."""

        concern_id = self._require_storage_id(concern_id, "concern_id")
        expected_owner_engram_id = self._require_storage_id(
            expected_owner_engram_id, "expected_owner_engram_id"
        )
        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or expected_revision < 1
        ):
            raise ValueError("expected_revision must be a positive integer")
        current = self._get_living_concern_unlocked(concern_id)
        if current is None:
            raise KeyError(f"unknown LivingConcern: {concern_id}")
        if current.owner_engram_id != expected_owner_engram_id:
            raise PermissionError("LivingConcern owner does not match")
        if current.disposition is LivingConcernDisposition.RESOLVED:
            raise ValueError("resolved LivingConcern is terminal")
        if current.revision != expected_revision:
            raise ValueError(
                f"LivingConcern revision changed: expected {expected_revision}, "
                f"found {current.revision}"
            )
        now = _now()
        normalized_disposition = _enum_value(
            disposition, LivingConcernDisposition, "disposition"
        )
        candidate = LivingConcern(
            id=current.id,
            center_id=current.center_id,
            owner_engram_id=current.owner_engram_id,
            content=content,
            disposition=normalized_disposition,
            revisit_at=revisit_at,
            causal_id=causal_id,
            source_event_id=source_event_id,
            revision=current.revision + 1,
            last_reentry_event_id=current.last_reentry_event_id,
            created_at=current.created_at,
            updated_at=now,
            resolved_at=(
                now
                if normalized_disposition is LivingConcernDisposition.RESOLVED
                else None
            ),
        )
        self._validate_living_concern_context_unlocked(
            center_id=candidate.center_id,
            owner_engram_id=candidate.owner_engram_id,
            causal_id=candidate.causal_id,
            source_event_id=candidate.source_event_id,
        )
        try:
            updated = self._conn.execute(
                "UPDATE living_concerns SET content = ?, disposition = ?, "
                "revisit_at = ?, causal_id = ?, source_event_id = ?, "
                "revision = ?, updated_at = ?, resolved_at = ? "
                "WHERE id = ? AND owner_engram_id = ? AND revision = ?",
                (
                    candidate.content,
                    candidate.disposition.value,
                    _ts(candidate.revisit_at) if candidate.revisit_at else None,
                    candidate.causal_id,
                    candidate.source_event_id,
                    candidate.revision,
                    _ts(candidate.updated_at),
                    _ts(candidate.resolved_at) if candidate.resolved_at else None,
                    candidate.id,
                    current.owner_engram_id,
                    expected_revision,
                ),
            )
            if updated.rowcount != 1:
                raise ValueError("LivingConcern changed before revision update")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return self._get_living_concern_unlocked(candidate.id)  # type: ignore[return-value]

    @_locked
    def list_living_concerns(
        self,
        *,
        center_id: str | None = None,
        owner_engram_id: str | None = None,
        disposition: LivingConcernDisposition | str | None = None,
    ) -> list[LivingConcern]:
        clauses = ["1 = 1"]
        params: list[str] = []
        if center_id is not None:
            clauses.append("center_id = ?")
            params.append(self._require_storage_id(center_id, "center_id"))
        if owner_engram_id is not None:
            clauses.append("owner_engram_id = ?")
            params.append(
                self._require_storage_id(owner_engram_id, "owner_engram_id")
            )
        if disposition is not None:
            clauses.append("disposition = ?")
            params.append(
                _enum_value(
                    disposition, LivingConcernDisposition, "disposition"
                ).value
            )
        rows = self._conn.execute(
            "SELECT " + self._living_concern_columns() + " FROM living_concerns "
            "WHERE " + " AND ".join(clauses)
            + " ORDER BY updated_at DESC, id",
            params,
        ).fetchall()
        return [self._row_to_living_concern(row) for row in rows]

    @_locked
    def list_due_living_concerns(
        self,
        now: datetime,
        limit: int,
    ) -> list[LivingConcern]:
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be a timezone-aware datetime")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        rows = self._conn.execute(
            "SELECT " + self._living_concern_columns("lc")
            + " FROM living_concerns lc "
            "JOIN activity_centers c ON c.id = lc.center_id "
            "JOIN engrams e ON e.id = lc.owner_engram_id "
            "WHERE lc.disposition = 'revisit' AND lc.revisit_at <= ? "
            "AND c.status = 'active' AND c.kind <> 'task' "
            "AND e.status = 'active' "
            "ORDER BY lc.revisit_at ASC, lc.updated_at ASC, lc.id ASC LIMIT ?",
            (_ts(now.astimezone(timezone.utc)), limit),
        ).fetchall()
        return [self._row_to_living_concern(row) for row in rows]

    @_locked
    def mark_living_concern_reentered(
        self,
        concern_id: str,
        expected_revision: int,
        event_id: str,
    ) -> LivingConcern:
        """CAS-consume one revisit only after its durable event exists."""

        concern_id = self._require_storage_id(concern_id, "concern_id")
        event_id = self._require_storage_id(event_id, "event_id")
        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or expected_revision < 1
        ):
            raise ValueError("expected_revision must be a positive integer")
        current = self._get_living_concern_unlocked(concern_id)
        if current is None:
            raise KeyError(f"unknown LivingConcern: {concern_id}")
        if (
            current.revision == expected_revision
            and current.disposition is LivingConcernDisposition.QUIET
            and current.last_reentry_event_id == event_id
        ):
            return current
        if current.revision != expected_revision:
            raise ValueError(
                f"LivingConcern revision changed: expected {expected_revision}, "
                f"found {current.revision}"
            )
        if current.disposition is not LivingConcernDisposition.REVISIT:
            raise ValueError("LivingConcern is not awaiting revisit")
        event = self._conn.execute(
            "SELECT causal_id, parent_event_id, engram_id, center_id, kind, source "
            "FROM causal_events WHERE id = ?",
            (event_id,),
        ).fetchone()
        if event is None:
            raise KeyError(f"unknown CausalEvent: {event_id}")
        expected_event = (
            current.causal_id,
            current.source_event_id,
            current.owner_engram_id,
            current.center_id,
            "spontaneous",
            "self",
        )
        if tuple(event) != expected_event:
            raise ValueError(
                "re-entry event does not preserve the LivingConcern causal identity"
            )
        now = _now()
        try:
            updated = self._conn.execute(
                "UPDATE living_concerns SET disposition = 'quiet', "
                "revisit_at = NULL, last_reentry_event_id = ?, updated_at = ? "
                "WHERE id = ? AND revision = ? AND disposition = 'revisit'",
                (event_id, _ts(now), concern_id, expected_revision),
            )
            if updated.rowcount != 1:
                raise ValueError("LivingConcern changed before re-entry mark")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return self._get_living_concern_unlocked(concern_id)  # type: ignore[return-value]

    # ── Living orientation persistence ─────────────────────────

    @_locked
    def create_living_orientation(
        self,
        center_id: str,
        owner_engram_id: str,
        content: str,
        causal_id: str,
        source_event_id: str,
        *,
        state: LivingOrientationState | str = LivingOrientationState.OPEN,
        orientation_id: str | None = None,
    ) -> LivingOrientation:
        """Persist one current, subject-authored direction for a Life Center."""

        from pulse_system.core.types.models import _uuid

        normalized_state = _enum_value(
            state,
            LivingOrientationState,
            "state",
        )
        if normalized_state is LivingOrientationState.CLOSED:
            raise ValueError("a new LivingOrientation cannot start closed")
        now = _now()
        candidate = LivingOrientation(
            id=orientation_id or _uuid(),
            center_id=center_id,
            owner_engram_id=owner_engram_id,
            content=content,
            state=normalized_state,
            causal_id=causal_id,
            source_event_id=source_event_id,
            created_at=now,
            updated_at=now,
        )
        self._validate_living_orientation_context_unlocked(
            center_id=candidate.center_id,
            owner_engram_id=candidate.owner_engram_id,
            causal_id=candidate.causal_id,
            source_event_id=candidate.source_event_id,
        )
        if self._get_living_orientation_unlocked(candidate.id) is not None:
            raise ValueError(f"LivingOrientation {candidate.id} already exists")
        current = self._conn.execute(
            "SELECT 1 FROM living_orientations "
            "WHERE center_id = ? AND owner_engram_id = ? "
            "AND state IN ('open', 'resting') LIMIT 1",
            (candidate.center_id, candidate.owner_engram_id),
        ).fetchone()
        if current is not None:
            raise ValueError(
                "an owner may have only one current LivingOrientation per Center"
            )
        try:
            self._insert_living_orientation(candidate)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return candidate

    @_locked
    def get_living_orientation(
        self,
        orientation_id: str,
    ) -> LivingOrientation | None:
        orientation_id = self._require_storage_id(
            orientation_id,
            "orientation_id",
        )
        return self._get_living_orientation_unlocked(orientation_id)

    @_locked
    def update_living_orientation(
        self,
        orientation_id: str,
        *,
        expected_owner_engram_id: str,
        expected_revision: int,
        content: str,
        state: LivingOrientationState | str,
        causal_id: str,
        source_event_id: str,
    ) -> LivingOrientation:
        """Apply one authored revision with owner and revision CAS."""

        orientation_id = self._require_storage_id(
            orientation_id,
            "orientation_id",
        )
        expected_owner_engram_id = self._require_storage_id(
            expected_owner_engram_id,
            "expected_owner_engram_id",
        )
        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or expected_revision < 1
        ):
            raise ValueError("expected_revision must be a positive integer")
        current = self._get_living_orientation_unlocked(orientation_id)
        if current is None:
            raise KeyError(f"unknown LivingOrientation: {orientation_id}")
        if current.owner_engram_id != expected_owner_engram_id:
            raise PermissionError("LivingOrientation owner does not match")
        if current.state is LivingOrientationState.CLOSED:
            raise ValueError("closed LivingOrientation is terminal")
        if current.revision != expected_revision:
            raise ValueError(
                f"LivingOrientation revision changed: expected {expected_revision}, "
                f"found {current.revision}"
            )
        normalized_state = _enum_value(
            state,
            LivingOrientationState,
            "state",
        )
        now = _now()
        preserves_refractory = (
            current.state is LivingOrientationState.OPEN
            and normalized_state is LivingOrientationState.OPEN
        )
        candidate = LivingOrientation(
            id=current.id,
            center_id=current.center_id,
            owner_engram_id=current.owner_engram_id,
            content=content,
            state=normalized_state,
            causal_id=causal_id,
            source_event_id=source_event_id,
            revision=current.revision + 1,
            engagement_count=current.engagement_count,
            next_eligible_at=(
                current.next_eligible_at if preserves_refractory else None
            ),
            last_engagement_event_id=current.last_engagement_event_id,
            last_engaged_at=current.last_engaged_at,
            created_at=current.created_at,
            updated_at=now,
            closed_at=(
                now
                if normalized_state is LivingOrientationState.CLOSED
                else None
            ),
        )
        self._validate_living_orientation_context_unlocked(
            center_id=candidate.center_id,
            owner_engram_id=candidate.owner_engram_id,
            causal_id=candidate.causal_id,
            source_event_id=candidate.source_event_id,
        )
        try:
            updated = self._conn.execute(
                "UPDATE living_orientations SET content = ?, state = ?, "
                "causal_id = ?, source_event_id = ?, revision = ?, "
                "next_eligible_at = ?, updated_at = ?, closed_at = ? "
                "WHERE id = ? AND owner_engram_id = ? AND revision = ?",
                (
                    candidate.content,
                    candidate.state.value,
                    candidate.causal_id,
                    candidate.source_event_id,
                    candidate.revision,
                    (
                        _ts(candidate.next_eligible_at)
                        if candidate.next_eligible_at is not None
                        else None
                    ),
                    _ts(candidate.updated_at),
                    _ts(candidate.closed_at) if candidate.closed_at else None,
                    candidate.id,
                    current.owner_engram_id,
                    expected_revision,
                ),
            )
            if updated.rowcount != 1:
                raise ValueError(
                    "LivingOrientation changed before revision update"
                )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return self._get_living_orientation_unlocked(candidate.id)  # type: ignore[return-value]

    @_locked
    def list_living_orientations(
        self,
        *,
        center_id: str | None = None,
        owner_engram_id: str | None = None,
        state: LivingOrientationState | str | None = None,
        current_only: bool = False,
    ) -> list[LivingOrientation]:
        """List orientations with current rows first and stable tie-breaks."""

        if not isinstance(current_only, bool):
            raise ValueError("current_only must be a boolean")
        clauses = ["1 = 1"]
        params: list[object] = []
        if center_id is not None:
            clauses.append("center_id = ?")
            params.append(self._require_storage_id(center_id, "center_id"))
        if owner_engram_id is not None:
            clauses.append("owner_engram_id = ?")
            params.append(
                self._require_storage_id(owner_engram_id, "owner_engram_id")
            )
        if state is not None:
            clauses.append("state = ?")
            params.append(
                _enum_value(state, LivingOrientationState, "state").value
            )
        if current_only:
            clauses.append("state IN ('open', 'resting')")
        rows = self._conn.execute(
            "SELECT "
            + self._living_orientation_columns()
            + " FROM living_orientations WHERE "
            + " AND ".join(clauses)
            + " ORDER BY CASE WHEN state IN ('open', 'resting') "
            "THEN 0 ELSE 1 END ASC, updated_at DESC, id ASC",
            params,
        ).fetchall()
        return [self._row_to_living_orientation(row) for row in rows]

    @_locked
    def select_living_orientation(
        self,
        engram_id: str,
        now: datetime,
    ) -> LivingOrientation | None:
        """Select one eligible orientation without claiming or scoring content."""

        engram_id = self._require_storage_id(engram_id, "engram_id")
        now = _require_utc(now, "now")
        rows = self._conn.execute(
            "SELECT "
            + self._living_orientation_columns("o")
            + " FROM living_orientations o "
            "JOIN activity_centers c ON c.id = o.center_id "
            "JOIN engrams e ON e.id = o.owner_engram_id "
            "JOIN center_memberships m ON m.center_id = o.center_id "
            "AND m.engram_id = o.owner_engram_id "
            "LEFT JOIN causal_events last_event "
            "ON last_event.id = o.last_engagement_event_id "
            "WHERE o.owner_engram_id = ? "
            "AND c.status = 'active' AND c.kind <> 'task' "
            "AND e.status = 'active' AND c.autonomy > 0.0 "
            "AND o.state = 'open' "
            "AND (o.next_eligible_at IS NULL OR o.next_eligible_at <= ?) "
            "AND (o.last_engagement_event_id IS NULL OR "
            "last_event.status NOT IN ('queued', 'running', 'uncertain')) "
            "ORDER BY ((o.engagement_count + 1.0) / c.autonomy) ASC, "
            "COALESCE(o.last_engaged_at, o.created_at) ASC, "
            "o.created_at ASC, o.id ASC LIMIT 1",
            (engram_id, _ts(now)),
        ).fetchall()
        if not rows:
            return None
        return self._row_to_living_orientation(rows[0])

    @_locked
    def mark_living_orientation_engaged(
        self,
        orientation_id: str,
        expected_revision: int,
        expected_engagement_count: int,
        event_id: str,
        next_eligible_at: datetime | None,
    ) -> LivingOrientation:
        """Mark one durable orientation event with identity and accounting CAS."""

        orientation_id = self._require_storage_id(
            orientation_id,
            "orientation_id",
        )
        event_id = self._require_storage_id(event_id, "event_id")
        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or expected_revision < 1
        ):
            raise ValueError("expected_revision must be a positive integer")
        if (
            isinstance(expected_engagement_count, bool)
            or not isinstance(expected_engagement_count, int)
            or expected_engagement_count < 0
        ):
            raise ValueError(
                "expected_engagement_count must be a non-negative integer"
            )
        if next_eligible_at is not None:
            next_eligible_at = _require_utc(
                next_eligible_at,
                "next_eligible_at",
            )

        with self._immediate_transaction() as conn:
            current = self._get_living_orientation_uncommitted(
                conn,
                orientation_id,
            )
            if current is None:
                raise KeyError(f"unknown LivingOrientation: {orientation_id}")
            if current.revision != expected_revision:
                raise ValueError(
                    f"LivingOrientation revision changed: expected {expected_revision}, "
                    f"found {current.revision}"
                )
            self._validate_living_orientation_engagement_event_uncommitted(
                conn,
                current,
                event_id,
                expected_engagement_count + 1,
            )
            if (
                current.last_engagement_event_id == event_id
                and current.engagement_count == expected_engagement_count + 1
            ):
                return current
            if current.engagement_count != expected_engagement_count:
                raise ValueError(
                    "LivingOrientation engagement count changed before mark"
                )
            if current.state is not LivingOrientationState.OPEN:
                raise ValueError(
                    "only an open LivingOrientation can be engaged"
                )
            now = _now()
            updated = conn.execute(
                "UPDATE living_orientations SET engagement_count = ?, "
                "next_eligible_at = ?, last_engagement_event_id = ?, "
                "last_engaged_at = ?, updated_at = ? "
                "WHERE id = ? AND revision = ? AND engagement_count = ? "
                "AND state = 'open'",
                (
                    expected_engagement_count + 1,
                    (
                        _ts(next_eligible_at)
                        if next_eligible_at is not None
                        else None
                    ),
                    event_id,
                    _ts(now),
                    _ts(now),
                    orientation_id,
                    expected_revision,
                    expected_engagement_count,
                ),
            )
            if updated.rowcount != 1:
                raise ValueError(
                    "LivingOrientation changed before engagement mark"
                )
            return self._get_living_orientation_uncommitted(  # type: ignore[return-value]
                conn,
                orientation_id,
            )

    @_locked
    def update_focal_succession(
        self,
        old_engram_id: str,
        new_engram_id: str,
    ) -> int:
        """Move all memberships and focal references to one successor atomically."""

        if self._conn.execute(
            "SELECT 1 FROM engrams WHERE id = ?", (old_engram_id,)
        ).fetchone() is None:
            raise ValueError(f"Engram {old_engram_id} not found")
        if self._conn.execute(
            "SELECT 1 FROM engrams WHERE id = ?", (new_engram_id,)
        ).fetchone() is None:
            raise ValueError(f"Engram {new_engram_id} not found")
        memberships = self._conn.execute(
            "SELECT center_id, relation, created_at FROM center_memberships "
            "WHERE engram_id = ? ORDER BY center_id",
            (old_engram_id,),
        ).fetchall()
        focal_count = sum(row[1] == MembershipRelation.FOCAL.value for row in memberships)
        try:
            self._conn.execute("BEGIN")
            for center_id, old_relation, created_at in memberships:
                existing = self._conn.execute(
                    "SELECT relation, created_at FROM center_memberships "
                    "WHERE center_id = ? AND engram_id = ?",
                    (center_id, new_engram_id),
                ).fetchone()
                if existing is None:
                    self._conn.execute(
                        "UPDATE center_memberships SET engram_id = ? "
                        "WHERE center_id = ? AND engram_id = ?",
                        (new_engram_id, center_id, old_engram_id),
                    )
                else:
                    self._conn.execute(
                        "DELETE FROM center_memberships "
                        "WHERE center_id = ? AND engram_id = ?",
                        (center_id, old_engram_id),
                    )
                    relation = self._stronger_membership(
                        old_relation, existing[0]
                    )
                    earliest = min(created_at, existing[1])
                    self._conn.execute(
                        "UPDATE center_memberships SET relation = ?, "
                        "created_at = ? WHERE center_id = ? AND engram_id = ?",
                        (relation, earliest, center_id, new_engram_id),
                    )
            succession_at = _ts(_now())
            self._conn.execute(
                "UPDATE living_concerns SET owner_engram_id = ?, "
                "updated_at = ? WHERE owner_engram_id = ?",
                (new_engram_id, succession_at, old_engram_id),
            )
            self._conn.execute(
                "UPDATE living_orientations SET owner_engram_id = ?, "
                "updated_at = ? WHERE owner_engram_id = ?",
                (new_engram_id, succession_at, old_engram_id),
            )
            relationship_rows = self._conn.execute(
                "SELECT id, status, revision FROM task_relationships "
                "WHERE current_subject_engram_id = ? ORDER BY id",
                (old_engram_id,),
            ).fetchall()
            for relationship_id, relationship_status, relationship_revision in (
                relationship_rows
            ):
                next_revision = int(relationship_revision) + 1
                updated = self._conn.execute(
                    "UPDATE task_relationships SET current_subject_engram_id = ?, "
                    "revision = ?, updated_at = ? WHERE id = ? AND revision = ? "
                    "AND current_subject_engram_id = ?",
                    (
                        new_engram_id,
                        next_revision,
                        succession_at,
                        relationship_id,
                        relationship_revision,
                        old_engram_id,
                    ),
                )
                if updated.rowcount != 1:
                    raise ValueError(
                        "task relationship changed during focal succession"
                    )
                self._conn.execute(
                    "INSERT INTO task_relationship_events ("
                    "relationship_id, seq, action, actor_kind, actor_id, "
                    "before_status, after_status, content, source_event_id, "
                    "created_at) VALUES (?, ?, 'succession', 'system', ?, ?, ?, "
                    "NULL, NULL, ?)",
                    (
                        relationship_id,
                        next_revision,
                        "world-registry",
                        relationship_status,
                        relationship_status,
                        succession_at,
                    ),
                )
            # Only unresolved offers follow the living subject.  Accepted and
            # refused offers remain historical evidence of who decided them;
            # their TaskFront, if any, follows the ordinary focal succession
            # update below.
            self._conn.execute(
                "UPDATE task_offers SET subject_engram_id = ?, "
                "updated_at = ? WHERE subject_engram_id = ? "
                "AND status IN ('pending', 'changes_requested')",
                (new_engram_id, succession_at, old_engram_id),
            )
            # Habitat subscriptions are part of the same focal succession
            # boundary.  A subscription is identity-bearing state: move it
            # when the successor has no matching active channel, otherwise
            # merge the latest durable fingerprint and retire the duplicate.
            subscription_rows = self._conn.execute(
                "SELECT id, world_id, channel, center_id, last_fingerprint, "
                "created_at, updated_at FROM habitat_subscriptions "
                "WHERE engram_id = ? AND status = 'active' "
                "ORDER BY world_id, channel, id",
                (old_engram_id,),
            ).fetchall()
            for (
                subscription_id,
                world_id,
                channel,
                source_center_id,
                source_fingerprint,
                _source_created_at,
                source_updated_at,
            ) in subscription_rows:
                target = self._conn.execute(
                    "SELECT id, center_id, last_fingerprint, updated_at "
                    "FROM habitat_subscriptions "
                    "WHERE world_id = ? AND engram_id = ? AND channel = ? "
                    "AND status = 'active'",
                    (world_id, new_engram_id, channel),
                ).fetchone()
                if target is None:
                    self._conn.execute(
                        "UPDATE habitat_subscriptions SET engram_id = ?, "
                        "updated_at = ? WHERE id = ? AND status = 'active'",
                        (new_engram_id, _ts(_now()), subscription_id),
                    )
                    continue
                (
                    target_id,
                    target_center_id,
                    target_fingerprint,
                    target_updated_at,
                ) = target
                if target_center_id != source_center_id:
                    raise ValueError(
                        "succession would merge Habitat subscriptions from "
                        "different Centers"
                    )
                # ISO timestamps are normally lexicographically ordered, but
                # parse them so offsets from older databases cannot select an
                # older fingerprint as the winner.  The ID tie-break keeps a
                # same-timestamp merge deterministic.
                source_key = (_parse_ts(source_updated_at), subscription_id)
                target_key = (_parse_ts(target_updated_at), target_id)
                source_is_latest = source_key > target_key
                merged_fingerprint = (
                    source_fingerprint if source_is_latest else target_fingerprint
                )
                merged_updated_at = (
                    source_updated_at if source_is_latest else target_updated_at
                )
                self._conn.execute(
                    "UPDATE habitat_subscriptions SET last_fingerprint = ?, "
                    "updated_at = ? WHERE id = ? AND status = 'active'",
                    (merged_fingerprint, merged_updated_at, target_id),
                )
                self._conn.execute(
                    "UPDATE habitat_subscriptions SET status = 'inactive', "
                    "updated_at = ? WHERE id = ? AND status = 'active'",
                    (_ts(_now()), subscription_id),
                )
            now = _ts(_now())
            self._conn.execute(
                "UPDATE activity_centers SET focal_engram_id = ?, "
                "updated_at = ? WHERE focal_engram_id = ?",
                (new_engram_id, now, old_engram_id),
            )
            self._conn.execute(
                "UPDATE task_fronts SET focal_engram_id = ?, updated_at = ? "
                "WHERE focal_engram_id = ?",
                (new_engram_id, now, old_engram_id),
            )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return focal_count

    # ── Habitat subscription persistence ───────────────────────

    @staticmethod
    def _normalize_subscription_channel(channel: str | None) -> str:
        if channel is None:
            return "all"
        if not isinstance(channel, str):
            raise ValueError("channel must be a string or null")
        normalized = channel.strip() or "all"
        if len(normalized) > 200:
            raise ValueError("channel must contain at most 200 characters")
        return normalized

    @staticmethod
    def _normalize_fingerprint(fingerprint: str | None) -> str | None:
        if fingerprint is not None and not isinstance(fingerprint, str):
            raise ValueError("last_fingerprint must be a string or null")
        if fingerprint is not None and len(fingerprint) > 512:
            raise ValueError(
                "last_fingerprint must contain at most 512 characters"
            )
        return fingerprint

    @staticmethod
    def _require_storage_id(value: str, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field_name} must be a non-empty string")
        return value

    @_locked
    def subscribe_habitat(
        self,
        world_id: str,
        engram_id: str,
        channel: str | None = None,
        *,
        center_id: str | None = None,
        subscription_id: str | None = None,
        last_fingerprint: str | None = None,
    ) -> HabitatSubscription:
        """Create or reactivate one durable subscription idempotently."""

        world_id = self._require_storage_id(world_id, "world_id")
        engram_id = self._require_storage_id(engram_id, "engram_id")
        channel = self._normalize_subscription_channel(channel)
        last_fingerprint = self._normalize_fingerprint(last_fingerprint)
        if center_id is not None:
            center_id = self._require_storage_id(center_id, "center_id")
        if subscription_id is not None:
            subscription_id = self._require_storage_id(
                subscription_id, "subscription_id"
            )
        if self._conn.execute(
            "SELECT 1 FROM engrams WHERE id = ?", (engram_id,)
        ).fetchone() is None:
            raise ValueError(f"Engram {engram_id} not found")
        if center_id is not None:
            center = self._conn.execute(
                "SELECT 1 FROM activity_centers WHERE id = ?", (center_id,)
            ).fetchone()
            if center is None:
                raise ValueError(f"ActivityCenter {center_id} not found")
            membership = self._conn.execute(
                "SELECT 1 FROM center_memberships "
                "WHERE center_id = ? AND engram_id = ?",
                (center_id, engram_id),
            ).fetchone()
            if membership is None:
                raise PermissionError(
                    "Habitat subscription owner must be a Center member"
                )

        # Never mutate the attribution of an active binding.  Null is a real
        # diffuse attribution, not a request to infer a Center retroactively.
        existing = self._conn.execute(
            "SELECT id, world_id, engram_id, center_id, channel, status, "
            "last_fingerprint, created_at, updated_at "
            "FROM habitat_subscriptions WHERE world_id = ? AND engram_id = ? "
            "AND channel = ? AND status = 'active' LIMIT 1",
            (world_id, engram_id, channel),
        ).fetchone()
        if existing is not None:
            if subscription_id is not None and existing[0] != subscription_id:
                raise ValueError(
                    "subscription_id does not match the existing channel binding"
                )
            if existing[3] != center_id:
                raise ValueError(
                    "active Habitat subscription cannot be rebound to another Center"
                )
            now = _ts(_now())
            fingerprint = (
                last_fingerprint
                if last_fingerprint is not None
                else existing[6]
            )
            try:
                self._conn.execute(
                    "UPDATE habitat_subscriptions SET status = 'active', "
                    "last_fingerprint = ?, updated_at = ? WHERE id = ?",
                    (fingerprint, now, existing[0]),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
            row = self._conn.execute(
                "SELECT id, world_id, engram_id, center_id, channel, status, "
                "last_fingerprint, created_at, updated_at "
                "FROM habitat_subscriptions WHERE id = ?",
                (existing[0],),
            ).fetchone()
            assert row is not None
            return self._row_to_habitat_subscription(row)

        # An explicitly addressed inactive row may be reactivated only with
        # the exact same identity and attribution.  Without an ID, reuse an
        # exact-attribution row; otherwise create a new historical binding.
        if subscription_id is not None:
            inactive = self._conn.execute(
                "SELECT id, world_id, engram_id, center_id, channel, status, "
                "last_fingerprint, created_at, updated_at "
                "FROM habitat_subscriptions WHERE id = ?",
                (subscription_id,),
            ).fetchone()
            if inactive is not None and (
                inactive[1], inactive[2], inactive[3], inactive[4]
            ) != (world_id, engram_id, center_id, channel):
                raise ValueError(
                    "subscription_id is bound to a different Habitat identity"
                )
        else:
            inactive = self._conn.execute(
                "SELECT id, world_id, engram_id, center_id, channel, status, "
                "last_fingerprint, created_at, updated_at "
                "FROM habitat_subscriptions WHERE world_id = ? AND engram_id = ? "
                "AND channel = ? AND status = 'inactive' "
                "AND center_id IS ? ORDER BY updated_at DESC, id LIMIT 1",
                (world_id, engram_id, channel, center_id),
            ).fetchone()
        if inactive is not None:
            now = _ts(_now())
            fingerprint = (
                last_fingerprint
                if last_fingerprint is not None
                else inactive[6]
            )
            try:
                self._conn.execute(
                    "UPDATE habitat_subscriptions SET status = 'active', "
                    "last_fingerprint = ?, updated_at = ? WHERE id = ?",
                    (fingerprint, now, inactive[0]),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
            row = self._conn.execute(
                "SELECT id, world_id, engram_id, center_id, channel, status, "
                "last_fingerprint, created_at, updated_at "
                "FROM habitat_subscriptions WHERE id = ?",
                (inactive[0],),
            ).fetchone()
            assert row is not None
            return self._row_to_habitat_subscription(row)

        from pulse_system.core.types.models import _uuid

        created = _now()
        subscription = HabitatSubscription(
            id=subscription_id or _uuid(),
            world_id=world_id,
            engram_id=engram_id,
            center_id=center_id,
            channel=channel,
            last_fingerprint=last_fingerprint,
            created_at=created,
            updated_at=created,
        )
        try:
            self._conn.execute(
                "INSERT INTO habitat_subscriptions "
                "(id, world_id, engram_id, center_id, channel, status, "
                "last_fingerprint, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    subscription.id,
                    subscription.world_id,
                    subscription.engram_id,
                    subscription.center_id,
                    subscription.channel,
                    subscription.status.value,
                    subscription.last_fingerprint,
                    _ts(subscription.created_at),
                    _ts(subscription.updated_at),
                ),
            )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return subscription

    create_habitat_subscription = subscribe_habitat

    @_locked
    def get_habitat_subscription(
        self, subscription_id: str
    ) -> HabitatSubscription | None:
        subscription_id = self._require_storage_id(subscription_id, "subscription_id")
        row = self._conn.execute(
            "SELECT id, world_id, engram_id, center_id, channel, status, "
            "last_fingerprint, "
            "created_at, updated_at FROM habitat_subscriptions WHERE id = ?",
            (subscription_id,),
        ).fetchone()
        return self._row_to_habitat_subscription(row) if row is not None else None

    @_locked
    def list_habitat_subscriptions(
        self,
        *,
        world_id: str | None = None,
        engram_id: str | None = None,
        center_id: str | None = None,
        channel: str | None = None,
        status: HabitatSubscriptionStatus | str | None = None,
    ) -> list[HabitatSubscription]:
        clauses = ["1 = 1"]
        params: list[str] = []
        if world_id is not None:
            clauses.append("world_id = ?")
            params.append(self._require_storage_id(world_id, "world_id"))
        if engram_id is not None:
            clauses.append("engram_id = ?")
            params.append(self._require_storage_id(engram_id, "engram_id"))
        if center_id is not None:
            clauses.append("center_id = ?")
            params.append(self._require_storage_id(center_id, "center_id"))
        if channel is not None:
            clauses.append("channel = ?")
            params.append(self._normalize_subscription_channel(channel))
        if status is not None:
            normalized = _enum_value(
                status, HabitatSubscriptionStatus, "status"
            ).value
            clauses.append("status = ?")
            params.append(normalized)
        rows = self._conn.execute(
            "SELECT id, world_id, engram_id, center_id, channel, status, "
            "last_fingerprint, "
            "created_at, updated_at FROM habitat_subscriptions WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at, id",
            params,
        ).fetchall()
        return [self._row_to_habitat_subscription(row) for row in rows]

    list_subscriptions = list_habitat_subscriptions

    @_locked
    def update_habitat_fingerprint(
        self, subscription_id: str, fingerprint: str | None
    ) -> HabitatSubscription | None:
        subscription_id = self._require_storage_id(subscription_id, "subscription_id")
        fingerprint = self._normalize_fingerprint(fingerprint)
        now = _ts(_now())
        try:
            updated = self._conn.execute(
                "UPDATE habitat_subscriptions SET last_fingerprint = ?, "
                "updated_at = ? WHERE id = ?",
                (fingerprint, now, subscription_id),
            )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        if updated.rowcount != 1:
            return None
        return self.get_habitat_subscription(subscription_id)

    update_habitat_subscription_fingerprint = update_habitat_fingerprint
    update_subscription_fingerprint = update_habitat_fingerprint
    update_fingerprint = update_habitat_fingerprint

    @_locked
    def deactivate_habitat_subscription(
        self, subscription_id: str
    ) -> HabitatSubscription | None:
        subscription_id = self._require_storage_id(subscription_id, "subscription_id")
        now = _ts(_now())
        try:
            updated = self._conn.execute(
                "UPDATE habitat_subscriptions SET status = 'inactive', "
                "updated_at = ? WHERE id = ?",
                (now, subscription_id),
            )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        if updated.rowcount != 1:
            return None
        return self.get_habitat_subscription(subscription_id)

    deactivate_subscription = deactivate_habitat_subscription

    # ── Delegation records (delegation tunnel; learning data for delegation router) ─────────

    @_locked
    def create_delegation(
        self,
        caller_id: str,
        target_id: str,
        task: str,
        mode: str,
        contract: str | None = None,
        task_embedding: str | None = None,
        group_id: str | None = None,
        *,
        delegation_id: str | None = None,
    ) -> str:
        from pulse_system.core.types.models import _uuid

        did = _uuid()
        if delegation_id is not None:
            if not isinstance(delegation_id, str) or not delegation_id.strip():
                raise ValueError("delegation_id must be a non-empty string")
            did = delegation_id.strip()
            existing = self._conn.execute(
                "SELECT * FROM delegations WHERE id = ?", (did,)
            ).fetchone()
            if existing is not None:
                record = self._row_to_delegation(existing)
                expected = {
                    "caller_id": caller_id,
                    "target_id": target_id,
                    "task": task,
                    "mode": mode,
                    "contract": contract,
                    "task_embedding": task_embedding,
                    "group_id": group_id,
                }
                if any(record[key] != value for key, value in expected.items()):
                    raise ValueError(
                        "delegation_id is already bound to another delegation"
                    )
                return did
        self._assert_runtime_publication()
        self._conn.execute(
            """INSERT INTO delegations
               (id, caller_id, target_id, task, mode, contract,
                task_embedding, created_at, group_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (did, caller_id, target_id, task, mode, contract,
             task_embedding, _ts(_now()), group_id),
        )
        self._conn.commit()
        return did

    @_locked
    def complete_delegation(self, delegation_id: str, result_summary: str) -> bool:
        self._assert_runtime_publication()
        cur = self._conn.execute(
            "UPDATE delegations SET result_summary = ?, completed_at = ? WHERE id = ?",
            (result_summary, _ts(_now()), delegation_id),
        )
        self._conn.commit()
        return cur.rowcount > 0

    @_locked
    def complete_delegation_once(
        self,
        delegation_id: str,
        result_summary: str,
    ) -> bool:
        """Complete a delegation exactly once.

        Returns ``True`` for the first write and ``False`` for an idempotent
        replay of the same result. A different result for the same durable
        record is an invariant violation rather than an overwrite.
        """

        row = self._conn.execute(
            "SELECT result_summary FROM delegations WHERE id = ?",
            (delegation_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown delegation: {delegation_id}")
        if row[0] is not None:
            if row[0] != result_summary:
                raise ValueError(
                    "delegation is already complete with another result"
                )
            return False
        self._assert_runtime_publication()
        cur = self._conn.execute(
            "UPDATE delegations SET result_summary = ?, completed_at = ? "
            "WHERE id = ? AND result_summary IS NULL",
            (result_summary, _ts(_now()), delegation_id),
        )
        self._conn.commit()
        if cur.rowcount == 1:
            return True
        row = self._conn.execute(
            "SELECT result_summary FROM delegations WHERE id = ?",
            (delegation_id,),
        ).fetchone()
        if row is not None and row[0] == result_summary:
            return False
        raise ValueError("delegation completion changed concurrently")

    @_locked
    def set_delegation_outcome(self, delegation_id: str, outcome: str) -> bool:
        self._assert_runtime_publication()
        cur = self._conn.execute(
            "UPDATE delegations SET outcome = ? WHERE id = ?",
            (outcome, delegation_id),
        )
        self._conn.commit()
        return cur.rowcount > 0

    @_locked
    def set_delegation_outcome_once(
        self,
        delegation_id: str,
        outcome: str,
    ) -> bool:
        """Persist one immutable outcome for the durable tunnel learner."""

        row = self._conn.execute(
            "SELECT outcome FROM delegations WHERE id = ?", (delegation_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown delegation: {delegation_id}")
        if row[0] is not None:
            if row[0] != outcome:
                raise ValueError(
                    "delegation outcome is already set to another value"
                )
            return False
        self._assert_runtime_publication()
        cur = self._conn.execute(
            "UPDATE delegations SET outcome = ? "
            "WHERE id = ? AND outcome IS NULL",
            (outcome, delegation_id),
        )
        self._conn.commit()
        if cur.rowcount == 1:
            return True
        row = self._conn.execute(
            "SELECT outcome FROM delegations WHERE id = ?", (delegation_id,)
        ).fetchone()
        if row is not None and row[0] == outcome:
            return False
        raise ValueError("delegation outcome changed concurrently")

    @_locked
    def get_delegation(self, delegation_id: str) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM delegations WHERE id = ?", (delegation_id,)
        ).fetchone()
        return self._row_to_delegation(row) if row else None

    @_locked
    def list_delegations(self, caller_id: str | None = None) -> list[dict]:
        if caller_id is not None:
            rows = self._conn.execute(
                "SELECT * FROM delegations WHERE caller_id = ? ORDER BY created_at",
                (caller_id,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM delegations ORDER BY created_at"
            ).fetchall()
        return [self._row_to_delegation(r) for r in rows]

    @staticmethod
    def _row_to_delegation(row: tuple) -> dict:
        return {
            "id": row[0], "caller_id": row[1], "target_id": row[2],
            "task": row[3], "mode": row[4], "contract": row[5],
            "task_embedding": row[6], "result_summary": row[7],
            "outcome": row[8], "created_at": row[9], "completed_at": row[10],
            "group_id": row[11] if len(row) > 11 else None,
        }

    # ── Component slot maps (delegation router/Claustrum modulator mask strategy) ──────────────

    @_locked
    def get_slot_map(self, component: str) -> dict[str, int]:
        rows = self._conn.execute(
            "SELECT engram_id, slot FROM component_slots WHERE component = ?",
            (component,),
        ).fetchall()
        return {r[0]: r[1] for r in rows}

    @_locked
    def assign_slot(self, component: str, engram_id: str) -> int:
        row = self._conn.execute(
            "SELECT slot FROM component_slots WHERE component = ? AND engram_id = ?",
            (component, engram_id),
        ).fetchone()
        if row is not None:
            return row[0]
        self._assert_runtime_publication()
        # smallest free slot (freed slots are reused; weights there start over)
        taken = {r[0] for r in self._conn.execute(
            "SELECT slot FROM component_slots WHERE component = ?",
            (component,),
        ).fetchall()}
        slot = 0
        while slot in taken:
            slot += 1
        self._conn.execute(
            "INSERT INTO component_slots (component, engram_id, slot) VALUES (?, ?, ?)",
            (component, engram_id, slot),
        )
        self._conn.commit()
        return slot

    @_locked
    def release_slot(self, component: str, engram_id: str) -> None:
        self._assert_runtime_publication()
        self._conn.execute(
            "DELETE FROM component_slots WHERE component = ? AND engram_id = ?",
            (component, engram_id),
        )
        self._conn.commit()

    @_locked
    def reassign_slot(self, component: str, old_id: str, new_id: str) -> None:
        """Succession: the successor inherits the predecessor's slot (and
        therefore its learned weights)."""
        self._assert_runtime_publication()
        self._conn.execute(
            "DELETE FROM component_slots WHERE component = ? AND engram_id = ?",
            (component, new_id),
        )
        self._conn.execute(
            "UPDATE component_slots SET engram_id = ? WHERE component = ? AND engram_id = ?",
            (new_id, component, old_id),
        )
        self._conn.commit()

    # ── Component state (delegation router/Claustrum modulator weight persistence) ─────────────

    @_locked
    def save_component_state(self, component: str, state: dict) -> None:
        """Upsert a sideband component's learned state (JSON-serializable)."""
        self._assert_runtime_publication()
        self._conn.execute(
            "INSERT INTO component_state (component, state, updated_at) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT(component) DO UPDATE SET "
            "state = excluded.state, updated_at = excluded.updated_at",
            (component, json.dumps(state), _ts(_now())),
        )
        self._conn.commit()

    @_locked
    def load_component_state(self, component: str) -> dict | None:
        row = self._conn.execute(
            "SELECT state FROM component_state WHERE component = ?",
            (component,),
        ).fetchone()
        return json.loads(row[0]) if row else None

    @_locked
    def delete_component_state(self, component: str) -> bool:
        """Remove one legacy/non-weight component record."""
        self._assert_runtime_publication()
        cur = self._conn.execute(
            "DELETE FROM component_state WHERE component = ?", (component,)
        )
        self._conn.commit()
        return cur.rowcount > 0

    # ── Explicit factory / field weight layers ──────────────────

    @_locked
    def save_weight_state(
        self,
        component: str,
        layer: str,
        state: dict,
    ) -> None:
        """Persist one numeric component layer without touching the other."""
        if layer not in {"factory", "field"}:
            raise ValueError("weight layer must be 'factory' or 'field'")
        self._assert_runtime_publication()
        self._conn.execute(
            """INSERT INTO weight_state (component, layer, state, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(component, layer) DO UPDATE SET
                 state = excluded.state,
                 updated_at = excluded.updated_at""",
            (component, layer, json.dumps(state), _ts(_now())),
        )
        self._conn.commit()

    @_locked
    def save_component_state_fenced(
        self,
        component: str,
        state: dict,
        *,
        runtime_owner_id: str,
        runtime_lease_epoch: int,
        now: datetime,
    ) -> None:
        """Replace component state in the same transaction as its lease check."""

        payload = json.dumps(state)
        with self._immediate_transaction() as conn:
            self._assert_runtime_publication()
            self._assert_runtime_lease_uncommitted(
                runtime_owner_id,
                runtime_lease_epoch,
                now,
                conn,
            )
            conn.execute(
                "INSERT INTO component_state (component, state, updated_at) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(component) DO UPDATE SET "
                "state = excluded.state, updated_at = excluded.updated_at",
                (component, payload, _ts(now)),
            )

    @_locked
    def save_weight_state_and_release_slot(
        self,
        component: str,
        layer: str,
        state: dict,
        engram_id: str,
    ) -> None:
        """Atomically neutralize a weight layer and release one slot mapping.

        Claustrum archive handling must not expose a persisted free slot while
        the field layer still contains its previous occupant's learned rows.
        Serializing before ``BEGIN`` also makes invalid state fail without
        changing either side of the boundary.
        """
        if layer not in {"factory", "field"}:
            raise ValueError("weight layer must be 'factory' or 'field'")
        encoded = json.dumps(state)
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            self._assert_runtime_publication()
            self._conn.execute(
                """INSERT INTO weight_state (component, layer, state, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(component, layer) DO UPDATE SET
                     state = excluded.state,
                     updated_at = excluded.updated_at""",
                (component, layer, encoded, _ts(_now())),
            )
            self._conn.execute(
                """DELETE FROM component_slots
                   WHERE component = ? AND engram_id = ?""",
                (component, engram_id),
            )
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise

    @_locked
    def load_weight_state(self, component: str, layer: str) -> dict | None:
        if layer not in {"factory", "field"}:
            raise ValueError("weight layer must be 'factory' or 'field'")
        row = self._conn.execute(
            """SELECT state FROM weight_state
               WHERE component = ? AND layer = ?""",
            (component, layer),
        ).fetchone()
        return json.loads(row[0]) if row else None

    @_locked
    def export_field_weights(self) -> dict:
        """Serialize only in-field learning, never factory baselines."""
        overrides = self._conn.execute(
            """SELECT from_id, to_id, weight, conn_type, created_at,
                      last_activated_at
               FROM connections
               WHERE learned_weight IS NOT NULL
               ORDER BY from_id, to_id"""
        ).fetchall()
        tombstones = self._conn.execute(
            """SELECT f.from_id, f.to_id
               FROM factory_connections AS f
               LEFT JOIN connections AS c
                 ON c.from_id = f.from_id AND c.to_id = f.to_id
               WHERE c.from_id IS NULL
               ORDER BY f.from_id, f.to_id"""
        ).fetchall()
        components = {
            component: json.loads(state)
            for component, state in self._conn.execute(
                """SELECT component, state FROM weight_state
                   WHERE layer = 'field' ORDER BY component"""
            ).fetchall()
        }
        connections = [
            {
                "from_id": row[0],
                "to_id": row[1],
                "weight": row[2],
                "conn_type": row[3],
                "created_at": row[4],
                "last_activated_at": row[5],
                "tombstone": False,
            }
            for row in overrides
        ]
        connections.extend(
            {
                "from_id": from_id,
                "to_id": to_id,
                "tombstone": True,
            }
            for from_id, to_id in tombstones
        )
        connections.sort(key=lambda item: (item["from_id"], item["to_id"]))
        return {
            "format": "pc01.field-weights.v1",
            "connections": connections,
            "components": components,
        }

    @_locked
    def reset_field_weights(self) -> dict[str, int]:
        """Clear field learning and restore the immutable factory layer."""
        before = self._field_counts()
        self._reset_field_weights_uncommitted()
        self._conn.commit()
        return before

    @_locked
    def import_field_weights(self, payload: dict) -> dict[str, int]:
        """Replace the current field layer from an exported payload."""
        self._validate_field_payload(payload)
        self._reset_field_weights_uncommitted()
        counts = self._apply_field_weights_uncommitted(payload)
        self._conn.commit()
        return counts

    @_locked
    def checkpoint_field_weights(self, label: str | None = None) -> str:
        """Persist a rollback point inside the same database."""
        checkpoint_id = uuid.uuid4().hex[:16]
        payload = self.export_field_weights()
        self._conn.execute(
            """INSERT INTO weight_checkpoints (id, label, payload, created_at)
               VALUES (?, ?, ?, ?)""",
            (
                checkpoint_id,
                label,
                json.dumps(payload),
                _ts(_now()),
            ),
        )
        self._conn.commit()
        return checkpoint_id

    @_locked
    def list_weight_checkpoints(self) -> list[dict]:
        return [
            {
                "id": row[0],
                "label": row[1],
                "created_at": row[2],
            }
            for row in self._conn.execute(
                """SELECT id, label, created_at FROM weight_checkpoints
                   ORDER BY created_at, id"""
            ).fetchall()
        ]

    @_locked
    def rollback_field_weights(self, checkpoint_id: str) -> dict[str, int]:
        row = self._conn.execute(
            "SELECT payload FROM weight_checkpoints WHERE id = ?",
            (checkpoint_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown weight checkpoint: {checkpoint_id}")
        payload = json.loads(row[0])
        self._validate_field_payload(payload)
        self._reset_field_weights_uncommitted()
        counts = self._apply_field_weights_uncommitted(payload)
        self._conn.commit()
        return counts

    def _field_counts(self) -> dict[str, int]:
        connection_count = self._conn.execute(
            "SELECT COUNT(*) FROM connections WHERE learned_weight IS NOT NULL"
        ).fetchone()[0]
        tombstone_count = self._conn.execute(
            """SELECT COUNT(*)
               FROM factory_connections AS f
               LEFT JOIN connections AS c
                 ON c.from_id = f.from_id AND c.to_id = f.to_id
               WHERE c.from_id IS NULL"""
        ).fetchone()[0]
        component_count = self._conn.execute(
            "SELECT COUNT(*) FROM weight_state WHERE layer = 'field'"
        ).fetchone()[0]
        return {
            "connections": connection_count,
            "tombstones": tombstone_count,
            "components": component_count,
        }

    def _reset_field_weights_uncommitted(self) -> None:
        self._conn.execute("DELETE FROM connections")
        self._conn.execute(
            """INSERT INTO connections
               (from_id, to_id, weight, conn_type, created_at,
                last_activated_at, learned_weight)
               SELECT from_id, to_id, weight, conn_type, created_at,
                      last_activated_at, NULL
               FROM factory_connections"""
        )
        self._conn.execute("DELETE FROM weight_state WHERE layer = 'field'")

    def _apply_field_weights_uncommitted(
        self,
        payload: dict,
    ) -> dict[str, int]:
        applied = 0
        tombstones = 0
        for item in payload["connections"]:
            from_id = item["from_id"]
            to_id = item["to_id"]
            if item.get("tombstone", False):
                self._conn.execute(
                    """DELETE FROM connections
                       WHERE from_id = ? AND to_id = ?""",
                    (from_id, to_id),
                )
                tombstones += 1
                continue
            weight = max(0.0, min(1.0, float(item["weight"])))
            self._conn.execute(
                """INSERT INTO connections
                   (from_id, to_id, weight, conn_type, created_at,
                    last_activated_at, learned_weight)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(from_id, to_id) DO UPDATE SET
                     weight = excluded.weight,
                     conn_type = excluded.conn_type,
                     last_activated_at = excluded.last_activated_at,
                     learned_weight = excluded.learned_weight""",
                (
                    from_id,
                    to_id,
                    weight,
                    item["conn_type"],
                    item["created_at"],
                    item["last_activated_at"],
                    weight,
                ),
            )
            applied += 1
        for component, state in payload["components"].items():
            self._conn.execute(
                """INSERT INTO weight_state
                   (component, layer, state, updated_at)
                   VALUES (?, 'field', ?, ?)
                   ON CONFLICT(component, layer) DO UPDATE SET
                     state = excluded.state,
                     updated_at = excluded.updated_at""",
                (component, json.dumps(state), _ts(_now())),
            )
        return {
            "connections": applied,
            "tombstones": tombstones,
            "components": len(payload["components"]),
        }

    @staticmethod
    def _validate_field_payload(payload: dict) -> None:
        if not isinstance(payload, dict):
            raise ValueError("field-weight payload must be an object")
        if payload.get("format") != "pc01.field-weights.v1":
            raise ValueError("unsupported field-weight payload format")
        if not isinstance(payload.get("connections"), list):
            raise ValueError("field-weight connections must be a list")
        if not isinstance(payload.get("components"), dict):
            raise ValueError("field-weight components must be an object")
        for item in payload["connections"]:
            if not isinstance(item, dict):
                raise ValueError("each field connection must be an object")
            if not item.get("from_id") or not item.get("to_id"):
                raise ValueError("field connection endpoints are required")
            if not item.get("tombstone", False):
                required = {
                    "weight",
                    "conn_type",
                    "created_at",
                    "last_activated_at",
                }
                missing = required - set(item)
                if missing:
                    raise ValueError(
                        f"field connection lacks {sorted(missing)}"
                    )

    # ── Internal helpers ─────────────────────────────────────────

    @staticmethod
    def _living_concern_columns(prefix: str | None = None) -> str:
        columns = (
            "id", "center_id", "owner_engram_id", "content", "disposition",
            "revisit_at", "causal_id", "source_event_id", "revision",
            "last_reentry_event_id", "created_at", "updated_at", "resolved_at",
        )
        if prefix is None:
            return ", ".join(columns)
        return ", ".join(f"{prefix}.{column}" for column in columns)

    @staticmethod
    def _living_orientation_columns(prefix: str | None = None) -> str:
        columns = (
            "id", "center_id", "owner_engram_id", "content", "state",
            "causal_id", "source_event_id", "revision", "engagement_count",
            "next_eligible_at", "last_engagement_event_id", "last_engaged_at",
            "created_at", "updated_at", "closed_at",
        )
        if prefix is None:
            return ", ".join(columns)
        return ", ".join(f"{prefix}.{column}" for column in columns)

    def _validate_living_concern_context_unlocked(
        self,
        *,
        center_id: str,
        owner_engram_id: str,
        causal_id: str,
        source_event_id: str,
    ) -> None:
        center = self._conn.execute(
            "SELECT kind FROM activity_centers WHERE id = ?", (center_id,)
        ).fetchone()
        if center is None:
            raise ValueError(f"ActivityCenter {center_id} not found")
        if center[0] == ActivityKind.TASK.value:
            raise ValueError("LivingConcern requires a non-task ActivityCenter")
        membership = self._conn.execute(
            "SELECT 1 FROM center_memberships "
            "WHERE center_id = ? AND engram_id = ?",
            (center_id, owner_engram_id),
        ).fetchone()
        if membership is None:
            raise PermissionError(
                "LivingConcern owner must be a member of its ActivityCenter"
            )
        source = self._conn.execute(
            "SELECT causal_id, engram_id, center_id FROM causal_events WHERE id = ?",
            (source_event_id,),
        ).fetchone()
        if source is None:
            raise ValueError(f"CausalEvent {source_event_id} not found")
        if tuple(source) != (causal_id, owner_engram_id, center_id):
            raise ValueError(
                "source event must share the concern causal_id, owner, and Center"
            )

    def _validate_living_orientation_context_unlocked(
        self,
        *,
        center_id: str,
        owner_engram_id: str,
        causal_id: str,
        source_event_id: str,
    ) -> None:
        center = self._conn.execute(
            "SELECT kind FROM activity_centers WHERE id = ?", (center_id,)
        ).fetchone()
        if center is None:
            raise ValueError(f"ActivityCenter {center_id} not found")
        if center[0] == ActivityKind.TASK.value:
            raise ValueError(
                "LivingOrientation requires a non-task ActivityCenter"
            )
        membership = self._conn.execute(
            "SELECT 1 FROM center_memberships "
            "WHERE center_id = ? AND engram_id = ?",
            (center_id, owner_engram_id),
        ).fetchone()
        if membership is None:
            raise PermissionError(
                "LivingOrientation owner must be a member of its ActivityCenter"
            )
        source = self._conn.execute(
            "SELECT causal_id, engram_id, center_id FROM causal_events WHERE id = ?",
            (source_event_id,),
        ).fetchone()
        if source is None:
            raise ValueError(f"CausalEvent {source_event_id} not found")
        if tuple(source) != (causal_id, owner_engram_id, center_id):
            raise ValueError(
                "source event must share the orientation causal_id, owner, and Center"
            )

    @staticmethod
    def _living_orientation_idempotency_key(
        orientation_id: str,
        revision: int,
        engagement_sequence: int,
    ) -> str:
        return (
            f"living-orientation:{orientation_id}:revision:{revision}:"
            f"engagement:{engagement_sequence}"
        )

    @staticmethod
    def _get_living_orientation_uncommitted(
        conn: sqlite3.Connection,
        orientation_id: str,
    ) -> LivingOrientation | None:
        row = conn.execute(
            "SELECT "
            + Storage._living_orientation_columns()
            + " FROM living_orientations WHERE id = ?",
            (orientation_id,),
        ).fetchone()
        return (
            Storage._row_to_living_orientation(row)
            if row is not None
            else None
        )

    def _get_living_orientation_unlocked(
        self,
        orientation_id: str,
    ) -> LivingOrientation | None:
        return self._get_living_orientation_uncommitted(
            self._conn,
            orientation_id,
        )

    @staticmethod
    def _validate_living_orientation_engagement_event_uncommitted(
        conn: sqlite3.Connection,
        orientation: LivingOrientation,
        event_id: str,
        engagement_sequence: int,
    ) -> None:
        row = conn.execute(
            "SELECT world_id, causal_id, parent_event_id, engram_id, "
            "center_id, flow, domain, kind, source, status, metadata, "
            "idempotency_key FROM causal_events WHERE id = ?",
            (event_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown CausalEvent: {event_id}")
        source_world = conn.execute(
            "SELECT world_id FROM causal_events WHERE id = ?",
            (orientation.source_event_id,),
        ).fetchone()
        if source_world is None or row[0] != source_world[0]:
            raise ValueError(
                "engagement event must share the orientation source event world"
            )
        expected_identity = (
            orientation.causal_id,
            orientation.source_event_id,
            orientation.owner_engram_id,
            orientation.center_id,
            None,
            CausalEventDomain.PULSE.value,
            CausalEventKind.SPONTANEOUS.value,
            CausalEventSource.SELF.value,
        )
        if tuple(row[1:9]) != expected_identity:
            raise ValueError(
                "engagement event does not preserve LivingOrientation identity"
            )
        if row[9] != CausalEventStatus.QUEUED.value:
            raise ValueError("engagement event must be queued")
        try:
            metadata = json.loads(row[10])
        except (TypeError, ValueError) as exc:
            raise ValueError("engagement event metadata is not valid JSON") from exc
        if not isinstance(metadata, dict):
            raise ValueError("engagement event metadata must be an object")
        expected_metadata_keys = {
            "reason_code",
            "orientation_id",
            "orientation_revision",
            "engagement_sequence",
            "priority",
        }
        if set(metadata) != expected_metadata_keys:
            raise ValueError(
                "engagement event metadata has an unexpected shape"
            )
        if metadata["reason_code"] != "living_orientation_engagement":
            raise ValueError("engagement event reason_code is invalid")
        if metadata["orientation_id"] != orientation.id:
            raise ValueError("engagement event orientation_id is invalid")
        if metadata["orientation_revision"] != orientation.revision:
            raise ValueError("engagement event revision is invalid")
        if metadata["engagement_sequence"] != engagement_sequence:
            raise ValueError("engagement event sequence is invalid")
        priority = metadata["priority"]
        if (
            isinstance(priority, bool)
            or not isinstance(priority, (int, float))
            or not math.isfinite(float(priority))
            or not 0.0 <= float(priority) <= 1.0
        ):
            raise ValueError("engagement event priority is invalid")
        expected_key = Storage._living_orientation_idempotency_key(
            orientation.id,
            orientation.revision,
            engagement_sequence,
        )
        if row[11] != expected_key:
            raise ValueError("engagement event idempotency key is invalid")

    def _insert_living_orientation(self, orientation: LivingOrientation) -> None:
        self._conn.execute(
            "INSERT INTO living_orientations ("
            + self._living_orientation_columns()
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                orientation.id,
                orientation.center_id,
                orientation.owner_engram_id,
                orientation.content,
                orientation.state.value,
                orientation.causal_id,
                orientation.source_event_id,
                orientation.revision,
                orientation.engagement_count,
                (
                    _ts(orientation.next_eligible_at)
                    if orientation.next_eligible_at is not None
                    else None
                ),
                orientation.last_engagement_event_id,
                (
                    _ts(orientation.last_engaged_at)
                    if orientation.last_engaged_at is not None
                    else None
                ),
                _ts(orientation.created_at),
                _ts(orientation.updated_at),
                _ts(orientation.closed_at) if orientation.closed_at else None,
            ),
        )

    def _insert_living_concern(self, concern: LivingConcern) -> None:
        self._conn.execute(
            "INSERT INTO living_concerns ("
            + self._living_concern_columns()
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                concern.id,
                concern.center_id,
                concern.owner_engram_id,
                concern.content,
                concern.disposition.value,
                _ts(concern.revisit_at) if concern.revisit_at else None,
                concern.causal_id,
                concern.source_event_id,
                concern.revision,
                concern.last_reentry_event_id,
                _ts(concern.created_at),
                _ts(concern.updated_at),
                _ts(concern.resolved_at) if concern.resolved_at else None,
            ),
        )

    def _get_living_concern_unlocked(
        self, concern_id: str
    ) -> LivingConcern | None:
        row = self._conn.execute(
            "SELECT " + self._living_concern_columns()
            + " FROM living_concerns WHERE id = ?",
            (concern_id,),
        ).fetchone()
        return self._row_to_living_concern(row) if row is not None else None

    @staticmethod
    def _resolve_focal_id(
        engram_id: str | None,
        focal_engram_id: str | None,
    ) -> str:
        from pulse_system.core.types.models import _uuid

        if (
            engram_id is not None
            and focal_engram_id is not None
            and engram_id != focal_engram_id
        ):
            raise ValueError(
                "engram_id and focal_engram_id must name the same Engram"
            )
        value = focal_engram_id or engram_id or _uuid()
        if not isinstance(value, str) or not value.strip():
            raise ValueError("focal_engram_id must be a non-empty string")
        return value

    def _insert_activity_center(self, center: ActivityCenter) -> None:
        self._conn.execute(
            """INSERT INTO activity_centers
               (id, kind, title, description, status, origin, autonomy,
                project_id, focal_engram_id, created_at, updated_at,
                last_active_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                center.id,
                center.kind.value,
                center.title,
                center.description,
                center.status.value,
                center.origin.value,
                center.autonomy,
                center.project_id,
                center.focal_engram_id,
                _ts(center.created_at),
                _ts(center.updated_at),
                _ts(center.last_active_at) if center.last_active_at else None,
            ),
        )

    def _insert_center_membership(self, membership: CenterMembership) -> None:
        self._conn.execute(
            """INSERT INTO center_memberships
               (center_id, engram_id, relation, created_at)
               VALUES (?, ?, ?, ?)""",
            (
                membership.center_id,
                membership.engram_id,
                membership.relation.value,
                _ts(membership.created_at),
            ),
        )

    def _insert_task_front(self, front: TaskFront) -> None:
        self._conn.execute(
            """INSERT INTO task_fronts
               (id, center_id, focal_engram_id, title, status, created_at,
                updated_at, last_opened_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                front.id,
                front.center_id,
                front.focal_engram_id,
                front.title,
                front.status.value,
                _ts(front.created_at),
                _ts(front.updated_at),
                _ts(front.last_opened_at),
            ),
        )

    def _insert_task_offer(self, offer: TaskOffer) -> None:
        self._conn.execute(
            "INSERT INTO task_offers ("
            + self._task_offer_columns()
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                offer.id,
                offer.world_id,
                offer.subject_engram_id,
                offer.status.value,
                offer.current_revision,
                offer.task_front_id,
                _ts(offer.created_at),
                _ts(offer.updated_at),
                _ts(offer.decided_at) if offer.decided_at else None,
                _ts(offer.withdrawn_at) if offer.withdrawn_at else None,
            ),
        )

    def _insert_task_offer_revision(
        self,
        revision: TaskOfferRevision,
    ) -> None:
        self._conn.execute(
            "INSERT INTO task_offer_revisions ("
            + self._task_offer_revision_columns()
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                revision.offer_id,
                revision.revision,
                revision.content,
                revision.title,
                revision.project_id,
                revision.latest_offer_event_id,
                (
                    revision.decision.value
                    if revision.decision is not None
                    else None
                ),
                revision.subject_response,
                revision.decision_event_id,
                _ts(revision.created_at),
                _ts(revision.decided_at) if revision.decided_at else None,
            ),
        )

    @classmethod
    def _get_task_offer_uncommitted(
        cls,
        conn: sqlite3.Connection,
        offer_id: str,
        *,
        world_id: str | None = None,
    ) -> TaskOffer | None:
        query = "SELECT " + cls._task_offer_columns() + " FROM task_offers WHERE id = ?"
        params: list = [offer_id]
        if world_id is not None:
            query += " AND world_id = ?"
            params.append(world_id)
        row = conn.execute(query, params).fetchone()
        return cls._row_to_task_offer(row) if row is not None else None

    @classmethod
    def _get_task_offer_revision_uncommitted(
        cls,
        conn: sqlite3.Connection,
        offer_id: str,
        revision: int,
    ) -> TaskOfferRevision | None:
        row = conn.execute(
            "SELECT "
            + cls._task_offer_revision_columns()
            + " FROM task_offer_revisions WHERE offer_id = ? AND revision = ?",
            (offer_id, revision),
        ).fetchone()
        return cls._row_to_task_offer_revision(row) if row is not None else None

    @classmethod
    def _list_task_offer_revisions_uncommitted(
        cls,
        conn: sqlite3.Connection,
        offer_id: str,
    ) -> list[TaskOfferRevision]:
        rows = conn.execute(
            "SELECT "
            + cls._task_offer_revision_columns()
            + " FROM task_offer_revisions WHERE offer_id = ? "
            "ORDER BY revision ASC",
            (offer_id,),
        ).fetchall()
        return [cls._row_to_task_offer_revision(row) for row in rows]

    def _insert_task_relationship(self, relationship: TaskRelationship) -> None:
        self._conn.execute(
            "INSERT INTO task_relationships ("
            + self._task_relationship_columns()
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                relationship.id,
                relationship.world_id,
                relationship.accepted_offer_id,
                relationship.task_front_id,
                relationship.center_id,
                relationship.original_subject_engram_id,
                relationship.current_subject_engram_id,
                relationship.status.value,
                relationship.revision,
                relationship.latest_terms_event_id,
                relationship.latest_subject_note,
                _ts(relationship.created_at),
                _ts(relationship.updated_at),
                _ts(relationship.exited_at) if relationship.exited_at else None,
            ),
        )

    def _insert_task_relationship_event(
        self,
        event: TaskRelationshipEvent,
    ) -> None:
        self._conn.execute(
            "INSERT INTO task_relationship_events ("
            + self._task_relationship_event_columns()
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event.relationship_id,
                event.seq,
                event.action.value,
                event.actor_kind.value,
                event.actor_id,
                event.before_status.value if event.before_status else None,
                event.after_status.value,
                event.content,
                event.source_event_id,
                _ts(event.created_at),
            ),
        )

    @classmethod
    def _get_task_relationship_uncommitted(
        cls,
        conn: sqlite3.Connection,
        relationship_id: str,
        *,
        world_id: str | None = None,
    ) -> TaskRelationship | None:
        query = (
            "SELECT "
            + cls._task_relationship_columns()
            + " FROM task_relationships WHERE id = ?"
        )
        params: list = [relationship_id]
        if world_id is not None:
            query += " AND world_id = ?"
            params.append(world_id)
        row = conn.execute(query, params).fetchone()
        return cls._row_to_task_relationship(row) if row is not None else None

    @classmethod
    def _get_task_relationship_by_unique_uncommitted(
        cls,
        conn: sqlite3.Connection,
        field_name: str,
        value: str,
        *,
        world_id: str | None = None,
    ) -> TaskRelationship | None:
        if field_name not in {"accepted_offer_id", "task_front_id", "center_id"}:
            raise ValueError("unsupported TaskRelationship lookup")
        query = (
            "SELECT "
            + cls._task_relationship_columns()
            + f" FROM task_relationships WHERE {field_name} = ?"
        )
        params: list = [value]
        if world_id is not None:
            query += " AND world_id = ?"
            params.append(world_id)
        row = conn.execute(query, params).fetchone()
        return cls._row_to_task_relationship(row) if row is not None else None

    @classmethod
    def _list_task_relationship_events_uncommitted(
        cls,
        conn: sqlite3.Connection,
        relationship_id: str,
    ) -> list[TaskRelationshipEvent]:
        rows = conn.execute(
            "SELECT "
            + cls._task_relationship_event_columns()
            + " FROM task_relationship_events WHERE relationship_id = ? "
            "ORDER BY seq ASC",
            (relationship_id,),
        ).fetchall()
        return [cls._row_to_task_relationship_event(row) for row in rows]

    def _get_activity_center_unlocked(
        self,
        center_id: str,
    ) -> ActivityCenter | None:
        row = self._conn.execute(
            "SELECT id, kind, title, description, status, origin, autonomy, "
            "project_id, focal_engram_id, created_at, updated_at, "
            "last_active_at FROM activity_centers WHERE id = ?",
            (center_id,),
        ).fetchone()
        return self._row_to_activity_center(row) if row is not None else None

    @staticmethod
    def _stronger_membership(left: str, right: str) -> str:
        priority = {
            MembershipRelation.PARTICIPANT.value: 0,
            MembershipRelation.SHARED.value: 1,
            MembershipRelation.FOCAL.value: 2,
        }
        return left if priority[left] >= priority[right] else right

    @staticmethod
    def _row_to_activity_center(row: tuple) -> ActivityCenter:
        return ActivityCenter(
            id=row[0],
            kind=ActivityKind(row[1]),
            title=row[2],
            description=row[3],
            status=ActivityCenterStatus(row[4]),
            origin=ActivityOrigin(row[5]),
            autonomy=row[6],
            project_id=row[7],
            focal_engram_id=row[8],
            created_at=_parse_ts(row[9]),
            updated_at=_parse_ts(row[10]),
            last_active_at=_parse_ts(row[11]) if row[11] else None,
        )

    @staticmethod
    def _row_to_center_membership(row: tuple) -> CenterMembership:
        return CenterMembership(
            center_id=row[0],
            engram_id=row[1],
            relation=MembershipRelation(row[2]),
            created_at=_parse_ts(row[3]),
        )

    @staticmethod
    def _row_to_task_front(row: tuple) -> TaskFront:
        return TaskFront(
            id=row[0],
            center_id=row[1],
            focal_engram_id=row[2],
            title=row[3],
            status=TaskFrontStatus(row[4]),
            created_at=_parse_ts(row[5]),
            updated_at=_parse_ts(row[6]),
            last_opened_at=_parse_ts(row[7]),
        )

    @staticmethod
    def _row_to_task_offer(row: tuple) -> TaskOffer:
        return TaskOffer(
            id=row[0],
            world_id=row[1],
            subject_engram_id=row[2],
            status=TaskOfferStatus(row[3]),
            current_revision=row[4],
            task_front_id=row[5],
            created_at=_parse_ts(row[6]),
            updated_at=_parse_ts(row[7]),
            decided_at=_parse_ts(row[8]) if row[8] else None,
            withdrawn_at=_parse_ts(row[9]) if row[9] else None,
        )

    @staticmethod
    def _row_to_task_offer_revision(row: tuple) -> TaskOfferRevision:
        return TaskOfferRevision(
            offer_id=row[0],
            revision=row[1],
            content=row[2],
            title=row[3],
            project_id=row[4],
            latest_offer_event_id=row[5],
            decision=(TaskOfferDecision(row[6]) if row[6] else None),
            subject_response=row[7],
            decision_event_id=row[8],
            created_at=_parse_ts(row[9]),
            decided_at=_parse_ts(row[10]) if row[10] else None,
        )

    @staticmethod
    def _row_to_task_relationship(row: tuple) -> TaskRelationship:
        return TaskRelationship(
            id=row[0],
            world_id=row[1],
            accepted_offer_id=row[2],
            task_front_id=row[3],
            center_id=row[4],
            original_subject_engram_id=row[5],
            current_subject_engram_id=row[6],
            status=TaskRelationshipStatus(row[7]),
            revision=row[8],
            latest_terms_event_id=row[9],
            latest_subject_note=row[10],
            created_at=_parse_ts(row[11]),
            updated_at=_parse_ts(row[12]),
            exited_at=_parse_ts(row[13]) if row[13] else None,
        )

    @staticmethod
    def _row_to_task_relationship_event(row: tuple) -> TaskRelationshipEvent:
        return TaskRelationshipEvent(
            relationship_id=row[0],
            seq=row[1],
            action=TaskRelationshipAction(row[2]),
            actor_kind=TaskRelationshipActorKind(row[3]),
            actor_id=row[4],
            before_status=(TaskRelationshipStatus(row[5]) if row[5] else None),
            after_status=TaskRelationshipStatus(row[6]),
            content=row[7],
            source_event_id=row[8],
            created_at=_parse_ts(row[9]),
        )

    @staticmethod
    def _row_to_habitat_subscription(row: tuple) -> HabitatSubscription:
        return HabitatSubscription(
            id=row[0],
            world_id=row[1],
            engram_id=row[2],
            center_id=row[3],
            channel=row[4],
            status=HabitatSubscriptionStatus(row[5]),
            last_fingerprint=row[6],
            created_at=_parse_ts(row[7]),
            updated_at=_parse_ts(row[8]),
        )

    @staticmethod
    def _row_to_living_concern(row: tuple) -> LivingConcern:
        return LivingConcern(
            id=row[0],
            center_id=row[1],
            owner_engram_id=row[2],
            content=row[3],
            disposition=LivingConcernDisposition(row[4]),
            revisit_at=_parse_ts(row[5]) if row[5] else None,
            causal_id=row[6],
            source_event_id=row[7],
            revision=row[8],
            last_reentry_event_id=row[9],
            created_at=_parse_ts(row[10]),
            updated_at=_parse_ts(row[11]),
            resolved_at=_parse_ts(row[12]) if row[12] else None,
        )

    @staticmethod
    def _row_to_living_orientation(row: tuple) -> LivingOrientation:
        return LivingOrientation(
            id=row[0],
            center_id=row[1],
            owner_engram_id=row[2],
            content=row[3],
            state=LivingOrientationState(row[4]),
            causal_id=row[5],
            source_event_id=row[6],
            revision=row[7],
            engagement_count=row[8],
            next_eligible_at=_parse_ts(row[9]) if row[9] else None,
            last_engagement_event_id=row[10],
            last_engaged_at=_parse_ts(row[11]) if row[11] else None,
            created_at=_parse_ts(row[12]),
            updated_at=_parse_ts(row[13]),
            closed_at=_parse_ts(row[14]) if row[14] else None,
        )

    def _insert_message(self, engram_id: str, msg: Message) -> int:
        cursor = self._conn.execute(
            """INSERT INTO messages (engram_id, role, content, timestamp, source_engram_id)
               VALUES (?, ?, ?, ?, ?)""",
            (engram_id, msg.role.value, msg.content, _ts(msg.timestamp), msg.source_engram_id),
        )
        return int(cursor.lastrowid)

    def _row_to_engram(self, row: tuple) -> Engram:
        return Engram(
            id=row[0],
            project_id=row[1],
            status=EngramStatus(row[2]),
            created_at=_parse_ts(row[3]),
            last_pulse_at=_parse_ts(row[4]) if row[4] else None,
            total_pulses=row[5],
            metadata=EngramMetadata(
                recent_activity=row[6],
                self_excitability=row[7],
                token_count=row[8],
            ),
            substrate_binding=row[9] if len(row) > 9 else None,
            name=row[10] if len(row) > 10 else None,
            name_origin=row[11] if len(row) > 11 else "auto",
            nickname=row[12] if len(row) > 12 else None,
        )

    def _row_to_project(self, row: tuple) -> Project:
        return Project(
            id=row[0],
            name=row[1],
            description=row[2],
            workspace_path=row[3],
            created_at=_parse_ts(row[4]),
            index_engram_id=row[5],
        )

    def _row_to_connection(self, row: tuple) -> Connection:
        return Connection(
            from_id=row[0],
            to_id=row[1],
            weight=row[2],
            conn_type=ConnectionType(row[3]),
            created_at=_parse_ts(row[4]),
            last_activated_at=_parse_ts(row[5]),
            learned_weight=row[6] if len(row) > 6 else None,
            factory_weight=row[7] if len(row) > 7 else row[2],
        )

    @_locked
    def close(self) -> None:
        self._conn.close()

    @_locked
    def causal_ledger(self):
        """Return the causal facade bound to this Storage connection."""
        from pulse_system.core.causality import CausalLedger

        return CausalLedger(self)

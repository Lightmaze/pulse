"""Durable, lease-fenced lifecycle for non-interactive pipe sessions.

The public values in this module are deliberately safe projections.  Raw
commands, argument vectors, absolute paths, environments, PIDs, and backend
handles exist only at the :class:`TerminalManager` process boundary and are
never written to the session tables or Harness events.

Every durable start, stop, output, terminal, and recovery mutation verifies
the current ``pulse_world`` Runtime lease in the *same* SQLite
``BEGIN IMMEDIATE`` transaction as its row-version CAS.  This is stronger
than an advisory callback and prevents a stale Runtime owner from committing
control-plane facts after lease transfer.
"""

from __future__ import annotations

import hashlib
import math
import re
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from pulse_system.core.runtime.publication import RuntimeRecoveryPermit
from pulse_system.substrate.storage.store import Storage

from .events import (
    HarnessEvent,
    HarnessEventDraft,
    HarnessEventKind,
    HarnessEventPhase,
    HarnessEventSource,
    HarnessEventStatus,
    HarnessEventStore,
)
from .security import CONTRACT_ONLY, LIVE_GATE_UNVERIFIED, LIVE_OS_RESTRICTED
from .terminal import (
    CommandProgress,
    ProcessHandle,
    ProcessResult,
    ProcessSpec,
    TerminalManager,
    TerminalState,
    TerminalValidationError,
)

__all__ = [
    "RUNTIME_LEASE_SCOPE",
    "ReconciliationState",
    "SessionProjectionState",
    "TerminalOutputChunk",
    "TerminalOutputGap",
    "TerminalOutputPage",
    "TerminalSessionCapabilities",
    "TerminalSessionCloseSummary",
    "TerminalSessionProcessTreeState",
    "TerminalSessionConflictError",
    "TerminalSessionError",
    "TerminalSessionLeaseError",
    "TerminalSessionList",
    "TerminalSessionMode",
    "TerminalSessionNotFoundError",
    "TerminalSessionScope",
    "TerminalSessionService",
    "TerminalSessionStartError",
    "TerminalSessionStore",
    "TerminalSessionSummary",
    "TerminalSessionTransport",
    "TerminalStopResult",
    "TreeContainment",
    "deterministic_recovery_event_id",
    "deterministic_start_event_id",
    "deterministic_terminal_event_id",
    "deterministic_output_event_id",
]


# This is the fixed scope used by Storage.runtime_leases.  Session writes use
# the same connection and transaction as this row; it is not a second lease.
RUNTIME_LEASE_SCOPE = "pulse_world"

_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:")
_MAX_ID_BYTES = 256
_MAX_EVIDENCE_BYTES = 64
_MAX_ERROR_BYTES = 128
_MAX_OUTPUT_TEXT_BYTES = 16 * 1024
_MAX_STOP_REQUESTS_PER_SESSION = 32
_MAX_MONITOR_ERRORS = 128
_MAX_RECONCILIATION_BATCH = 32
_DEFAULT_CLOSE_TIMEOUT_SEC = 2.0
_ACTIVE_STATES = frozenset(
    {
        TerminalState.PENDING,
        TerminalState.RUNNING,
        TerminalState.CANCEL_REQUESTED,
        TerminalState.KILL_REQUESTED,
    }
)


class TerminalSessionError(RuntimeError):
    """Base error for durable terminal-session control."""


class TerminalSessionLeaseError(TerminalSessionError):
    """The caller no longer owns the current durable Runtime lease."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"runtime_lease_lost:{reason}")


class TerminalSessionConflictError(TerminalSessionError):
    """A scope, epoch, state, or row-version CAS no longer matches."""


class TerminalSessionNotFoundError(TerminalSessionError, KeyError):
    """The bounded session store no longer retains the requested session."""


class TerminalSessionStartError(TerminalSessionError):
    """A background launch did not reach reliable durable control."""

    def __init__(self, summary: "TerminalSessionSummary", error_code: str):
        self.summary = summary
        self.error_code = error_code
        super().__init__(error_code)


class TerminalSessionMode(StrEnum):
    PIPE_SESSION = "PIPE_SESSION"


class TerminalSessionTransport(StrEnum):
    PIPE = "pipe"


class TerminalSessionScope(StrEnum):
    RUNTIME_CONNECTION = "runtime_connection"


class TreeContainment(StrEnum):
    UNVERIFIED = "UNVERIFIED"
    OBSERVED_CLEANUP = "OBSERVED_CLEANUP"
    JOB_OBJECT_VERIFIED = "JOB_OBJECT_VERIFIED"


class TerminalSessionProcessTreeState(StrEnum):
    """Per-close process evidence; static containment is not an exit census."""

    NOT_APPLICABLE = "not_applicable"
    EMPTY_VERIFIED = "empty_verified"
    ROOT_EXIT_ONLY = "root_exit_only"
    UNKNOWN = "unknown"


class SessionProjectionState(StrEnum):
    START_RESERVED = "start_reserved"
    START_PROJECTED = "start_projected"
    TERMINAL_PENDING = "terminal_pending"
    TERMINAL_PROJECTED = "terminal_projected"


class ReconciliationState(StrEnum):
    OWNED = "owned"
    STOP_REQUESTED = "stop_requested"
    PROJECTION_REQUIRED = "projection_required"
    SETTLED = "settled"
    RECOVERED = "recovered"


class _SessionControlState(StrEnum):
    """Private process-boundary state; never projected to events or DTOs."""

    PREPARED = "PREPARED"
    SPAWN_REQUESTED = "SPAWN_REQUESTED"
    ATTACHED = "ATTACHED"
    STOP_REQUESTED = "STOP_REQUESTED"
    TERMINAL = "TERMINAL"


def _bounded_text(value: Any, field_name: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise TerminalSessionError(f"{field_name} must be a non-empty string")
    result = value.strip()
    if len(result.encode("utf-8")) > maximum:
        raise TerminalSessionError(f"{field_name} is too large")
    return result


def _identifier(value: Any, field_name: str) -> str:
    return _bounded_text(value, field_name, maximum=_MAX_ID_BYTES)


def _token(value: Any, field_name: str) -> str:
    result = _identifier(value, field_name)
    if not _TOKEN.fullmatch(result):
        raise TerminalSessionError(f"{field_name} is not a safe opaque token")
    return result


def _digest(value: Any, field_name: str) -> str:
    result = _bounded_text(value, field_name, maximum=64)
    if not _SHA256.fullmatch(result):
        raise TerminalSessionError(f"{field_name} must be a lowercase sha256")
    return result


def _epoch(value: Any, field_name: str = "epoch") -> int:
    if type(value) is not int or value < 1:
        raise TerminalSessionError(f"{field_name} must be an integer >= 1")
    return value


def _positive_limit(value: Any, field_name: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise TerminalSessionError(
            f"{field_name} must be an integer between 1 and {maximum}"
        )
    return value


def _utc(value: datetime | None = None) -> datetime:
    result = datetime.now(timezone.utc) if value is None else value
    if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
        raise TerminalSessionError("clock must return a timezone-aware datetime")
    return result.astimezone(timezone.utc)


def _parse_time(value: str) -> datetime:
    try:
        return _utc(datetime.fromisoformat(value))
    except (TypeError, ValueError) as exc:
        raise TerminalSessionError("stored session timestamp is invalid") from exc


def _iso(value: datetime | None = None) -> str:
    return _utc(value).isoformat()


def _safe_cwd(value: Any) -> str:
    result = _identifier(value, "cwd_relative").replace("\\", "/")
    if (
        result.startswith("/")
        or result.startswith("//")
        or _WINDOWS_DRIVE.match(result)
        or any(part in {"", ".."} or ":" in part for part in result.split("/"))
    ):
        if result != ".":
            raise TerminalSessionError("cwd_relative must stay workspace-relative")
    return result


def _clip_utf8_text(value: Any, *, maximum: int) -> tuple[str, bool]:
    text = str(value)
    encoded = text.encode("utf-8", errors="replace")
    if len(encoded) <= maximum:
        return text, False
    clipped = encoded[:maximum]
    while clipped:
        try:
            return clipped.decode("utf-8"), True
        except UnicodeDecodeError:
            clipped = clipped[:-1]
    return "", True


def _event_id(kind: str, *parts: object) -> str:
    digest = hashlib.sha256(
        "\x00".join(("harness.terminal-sessions.v1", kind, *(str(part) for part in parts))).encode(
            "utf-8"
        )
    ).hexdigest()
    return f"terminal_session_{kind}_{digest}"


def deterministic_start_event_id(terminal_session_id: str) -> str:
    return _event_id("start", _token(terminal_session_id, "terminal_session_id"))


def deterministic_output_event_id(terminal_session_id: str, seq: int) -> str:
    if type(seq) is not int or seq < 1:
        raise TerminalSessionError("output seq must be an integer >= 1")
    return _event_id("output", _token(terminal_session_id, "terminal_session_id"), seq)


def deterministic_terminal_event_id(terminal_session_id: str) -> str:
    return _event_id("terminal", _token(terminal_session_id, "terminal_session_id"))


def deterministic_recovery_event_id(
    terminal_session_id: str,
    prior_owner_id: str,
    prior_epoch: int,
) -> str:
    return _event_id(
        "recovery",
        _token(terminal_session_id, "terminal_session_id"),
        _token(prior_owner_id, "prior_owner_id"),
        _epoch(prior_epoch, "prior_epoch"),
    )


@dataclass(frozen=True, slots=True)
class TerminalSessionCapabilities:
    stdin: bool = False
    resize: bool = False
    reconnect: bool = True
    stop: bool = True

    def to_wire(self) -> dict[str, bool]:
        return {
            "stdin": False,
            "resize": False,
            "reconnect": True,
            "stop": True,
        }


@dataclass(frozen=True, slots=True)
class TerminalSessionSummary:
    terminal_session_id: str
    turn_id: str
    world_id: str
    engram_id: str
    epoch: int
    state: TerminalState
    cwd_relative: str
    command_digest: str
    started_at: str
    ended_at: str | None
    exit_code: int | None
    output_bytes: int
    output_truncated: bool
    last_output_seq: int
    evidence_class: str
    sandbox_evidence: str
    tree_containment: TreeContainment
    launch_action_digest: str | None = None
    error_code: str | None = None
    uncertain_reason: str | None = None
    mode: TerminalSessionMode = TerminalSessionMode.PIPE_SESSION
    transport: TerminalSessionTransport = TerminalSessionTransport.PIPE
    session_scope: TerminalSessionScope = TerminalSessionScope.RUNTIME_CONNECTION
    capabilities: TerminalSessionCapabilities = TerminalSessionCapabilities()

    def to_wire(self) -> dict[str, Any]:
        return {
            "terminal_session_id": self.terminal_session_id,
            "turn_id": self.turn_id,
            "world_id": self.world_id,
            "engram_id": self.engram_id,
            "epoch": self.epoch,
            "mode": self.mode.value,
            "transport": self.transport.value,
            "session_scope": self.session_scope.value,
            "state": self.state.value,
            "cwd_relative": self.cwd_relative,
            "command_digest": self.command_digest,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "exit_code": self.exit_code,
            "output_bytes": self.output_bytes,
            "output_truncated": self.output_truncated,
            "last_output_seq": self.last_output_seq,
            "launch_action_digest": self.launch_action_digest,
            "evidence_class": self.evidence_class,
            "sandbox_evidence": self.sandbox_evidence,
            "tree_containment": self.tree_containment.value,
            "error_code": self.error_code,
            "uncertain_reason": self.uncertain_reason,
            "capabilities": self.capabilities.to_wire(),
        }


@dataclass(frozen=True, slots=True)
class TerminalOutputChunk:
    terminal_session_id: str
    seq: int
    stream: str
    text: str
    byte_count: int
    truncated: bool
    redacted: bool
    at: str

    def to_wire(self) -> dict[str, Any]:
        return {
            "terminal_session_id": self.terminal_session_id,
            "seq": self.seq,
            "stream": self.stream,
            "text": self.text,
            "byte_count": self.byte_count,
            "truncated": self.truncated,
            "redacted": self.redacted,
            "at": self.at,
        }


@dataclass(frozen=True, slots=True)
class TerminalOutputGap:
    missing_from: int
    missing_to: int
    reason: str = "retention_pruned"

    def to_wire(self) -> dict[str, Any]:
        return {
            "missing_from": self.missing_from,
            "missing_to": self.missing_to,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class TerminalOutputPage:
    terminal_session_id: str
    output: tuple[TerminalOutputChunk, ...]
    earliest_seq: int
    next_seq: int
    has_more: bool
    gap: TerminalOutputGap | None
    truncated: bool
    evidence_class: str

    def to_wire(self) -> dict[str, Any]:
        return {
            "terminal_session_id": self.terminal_session_id,
            "output": [item.to_wire() for item in self.output],
            "earliest_seq": self.earliest_seq,
            "next_seq": self.next_seq,
            "has_more": self.has_more,
            "gap": None if self.gap is None else self.gap.to_wire(),
            "truncated": self.truncated,
            "evidence_class": self.evidence_class,
        }


@dataclass(frozen=True, slots=True)
class TerminalSessionList:
    sessions: tuple[TerminalSessionSummary, ...]
    count: int
    evidence_class: str

    def to_wire(self) -> dict[str, Any]:
        return {
            "sessions": [session.to_wire() for session in self.sessions],
            "count": self.count,
            "evidence_class": self.evidence_class,
        }


@dataclass(frozen=True, slots=True)
class TerminalStopResult:
    summary: TerminalSessionSummary
    accepted: bool
    idempotent: bool
    uncertain: bool
    error_code: str | None = None

    @property
    def pending(self) -> bool:
        return self.accepted and self.summary.state in _ACTIVE_STATES

    def to_wire(self) -> dict[str, Any]:
        return {
            **self.summary.to_wire(),
            "accepted": self.accepted,
            "idempotent": self.idempotent,
            "uncertain": self.uncertain,
            "error_code": self.error_code or self.summary.error_code,
        }


@dataclass(frozen=True, slots=True)
class TerminalSessionCloseSummary:
    """Typed evidence for all owners captured by one Terminal close."""

    active_before: int
    unresolved: int
    owner_joined: bool
    process_tree_state: TerminalSessionProcessTreeState
    cancellation_requested: int = 0
    terminal_observed: int = 0
    starters_before: int = 0
    monitors_before: int = 0
    results: tuple[TerminalStopResult, ...] = ()

    def __post_init__(self) -> None:
        for name in (
            "active_before",
            "unresolved",
            "cancellation_requested",
            "terminal_observed",
            "starters_before",
            "monitors_before",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative int")
        if type(self.owner_joined) is not bool:
            raise ValueError("owner_joined must be a bool")
        if self.owner_joined and self.unresolved:
            raise ValueError("joined owners cannot have unresolved work")
        object.__setattr__(
            self,
            "process_tree_state",
            TerminalSessionProcessTreeState(self.process_tree_state),
        )
        object.__setattr__(self, "results", tuple(self.results))

    def to_wire(self) -> dict[str, Any]:
        return {
            "active_before": self.active_before,
            "unresolved": self.unresolved,
            "owner_joined": self.owner_joined,
            "process_tree_state": self.process_tree_state.value,
            "cancellation_requested": self.cancellation_requested,
            "terminal_observed": self.terminal_observed,
            "starters_before": self.starters_before,
            "monitors_before": self.monitors_before,
            "results": [item.to_wire() for item in self.results],
        }

    def __iter__(self) -> Iterator[TerminalStopResult]:
        """Preserve legacy outcome iteration while exposing typed evidence."""

        return iter(self.results)

    def __len__(self) -> int:
        return len(self.results)


@dataclass(frozen=True, slots=True)
class _SessionRow:
    summary: TerminalSessionSummary
    owner_id: str
    row_version: int
    projection_state: SessionProjectionState
    reconciliation_state: ReconciliationState
    start_event_id: str
    terminal_event_id: str | None
    reconciled_by_owner_id: str | None
    reconciled_by_epoch: int | None


@dataclass(frozen=True, slots=True)
class _SessionControlRow:
    terminal_session_id: str
    owner_id: str
    epoch: int
    state: _SessionControlState
    prepared_at: str
    spawn_boundary_at: str | None
    attached_at: str | None
    terminal_at: str | None
    spawn_boundary_id: str | None
    opaque_backend_handle: str | None
    containment_id: str | None
    host_boot_id: str | None
    process_fingerprint: str | None
    row_version: int


@dataclass(frozen=True, slots=True)
class _AttachmentFacts:
    spawn_boundary_id: str
    opaque_backend_handle: str
    containment_id: str
    host_boot_id: str
    process_fingerprint: str


@dataclass(slots=True)
class _SessionLockEntry:
    lock: threading.RLock
    users: int = 0


@dataclass(frozen=True, slots=True)
class _StopClaim:
    row: _SessionRow
    idempotent: bool


_SESSION_SELECT = """
    SELECT terminal_session_id, turn_id, world_id, engram_id, epoch,
           state, cwd_relative, command_digest, started_at, ended_at,
           exit_code, output_bytes, output_truncated, last_output_seq,
           evidence_class, sandbox_evidence, tree_containment,
           launch_action_digest, error_code, uncertain_reason,
           owner_id, row_version, projection_state, reconciliation_state,
           start_event_id, terminal_event_id,
           reconciled_by_owner_id, reconciled_by_epoch
    FROM terminal_sessions
"""


class TerminalSessionStore:
    """Additive SQLite store sharing one transaction domain with Harness events."""

    def __init__(
        self,
        storage: Storage,
        event_store: HarnessEventStore,
        *,
        max_sessions: int = 512,
        max_output_chunks_per_session: int = 1_000,
        max_output_bytes_per_session: int = 1024 * 1024,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(storage, Storage):
            raise TypeError("storage must be a Storage")
        if not isinstance(event_store, HarnessEventStore):
            raise TypeError("event_store must be a HarnessEventStore")
        if event_store.storage is not storage:
            raise TerminalSessionError(
                "terminal sessions and Harness events must share one Storage"
            )
        self._storage = storage
        self._events = event_store
        self._max_sessions = _positive_limit(max_sessions, "max_sessions", 10_000)
        self._max_output_chunks = _positive_limit(
            max_output_chunks_per_session,
            "max_output_chunks_per_session",
            100_000,
        )
        self._max_output_bytes = _positive_limit(
            max_output_bytes_per_session,
            "max_output_bytes_per_session",
            64 * 1024 * 1024,
        )
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._initialize_schema()

    @property
    def storage(self) -> Storage:
        return self._storage

    def _initialize_schema(self) -> None:
        def write(conn) -> None:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS terminal_sessions (
                    terminal_session_id TEXT PRIMARY KEY,
                    turn_id TEXT NOT NULL,
                    world_id TEXT NOT NULL,
                    engram_id TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    epoch INTEGER NOT NULL CHECK(epoch >= 1),
                    mode TEXT NOT NULL CHECK(mode = 'PIPE_SESSION'),
                    transport TEXT NOT NULL CHECK(transport = 'pipe'),
                    session_scope TEXT NOT NULL
                        CHECK(session_scope = 'runtime_connection'),
                    state TEXT NOT NULL CHECK(state IN (
                        'PENDING', 'RUNNING', 'EXITED', 'FAILED', 'TIMED_OUT',
                        'CANCEL_REQUESTED', 'CANCELLED', 'KILL_REQUESTED',
                        'KILLED', 'UNCERTAIN', 'DENIED', 'UNSUPPORTED'
                    )),
                    cwd_relative TEXT NOT NULL,
                    command_digest TEXT NOT NULL,
                    launch_action_digest TEXT,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    exit_code INTEGER,
                    output_bytes INTEGER NOT NULL DEFAULT 0
                        CHECK(output_bytes >= 0),
                    output_truncated INTEGER NOT NULL DEFAULT 0
                        CHECK(output_truncated IN (0, 1)),
                    last_output_seq INTEGER NOT NULL DEFAULT 0
                        CHECK(last_output_seq >= 0),
                    evidence_class TEXT NOT NULL,
                    sandbox_evidence TEXT NOT NULL,
                    tree_containment TEXT NOT NULL CHECK(tree_containment IN (
                        'UNVERIFIED', 'OBSERVED_CLEANUP', 'JOB_OBJECT_VERIFIED'
                    )),
                    error_code TEXT,
                    uncertain_reason TEXT,
                    row_version INTEGER NOT NULL DEFAULT 1
                        CHECK(row_version >= 1),
                    projection_state TEXT NOT NULL CHECK(projection_state IN (
                        'start_reserved', 'start_projected',
                        'terminal_pending', 'terminal_projected'
                    )),
                    reconciliation_state TEXT NOT NULL
                        CHECK(reconciliation_state IN (
                            'owned', 'stop_requested', 'projection_required',
                            'settled', 'recovered'
                        )),
                    start_event_id TEXT NOT NULL,
                    terminal_event_id TEXT,
                    reconciled_by_owner_id TEXT,
                    reconciled_by_epoch INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    CHECK(
                        (reconciled_by_owner_id IS NULL
                            AND reconciled_by_epoch IS NULL)
                        OR (reconciled_by_owner_id IS NOT NULL
                            AND reconciled_by_epoch >= 1)
                    )
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS terminal_session_controls (
                    terminal_session_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    epoch INTEGER NOT NULL CHECK(epoch >= 1),
                    control_state TEXT NOT NULL CHECK(control_state IN (
                        'PREPARED', 'SPAWN_REQUESTED', 'ATTACHED',
                        'STOP_REQUESTED', 'TERMINAL'
                    )),
                    prepared_at TEXT NOT NULL,
                    spawn_boundary_at TEXT,
                    attached_at TEXT,
                    terminal_at TEXT,
                    spawn_boundary_id TEXT,
                    opaque_backend_handle TEXT,
                    containment_id TEXT,
                    host_boot_id TEXT,
                    process_fingerprint TEXT,
                    row_version INTEGER NOT NULL DEFAULT 1
                        CHECK(row_version >= 1),
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(terminal_session_id)
                        REFERENCES terminal_sessions(terminal_session_id)
                        ON DELETE CASCADE,
                    CHECK(
                        control_state != 'ATTACHED'
                        OR (
                            spawn_boundary_at IS NOT NULL
                            AND attached_at IS NOT NULL
                            AND spawn_boundary_id IS NOT NULL
                            AND length(spawn_boundary_id) = 64
                            AND opaque_backend_handle IS NOT NULL
                            AND length(opaque_backend_handle) = 64
                            AND containment_id IS NOT NULL
                            AND length(containment_id) = 64
                            AND host_boot_id IS NOT NULL
                            AND length(host_boot_id) = 64
                            AND process_fingerprint IS NOT NULL
                            AND length(process_fingerprint) = 64
                        )
                    )
                )"""
            )
            # Additive migration for an early worktree database.  An
            # old active row has no attachment proof, so it is deliberately
            # backfilled no further than SPAWN_REQUESTED and will become
            # UNCERTAIN during startup recovery rather than being reattached.
            conn.execute(
                """INSERT OR IGNORE INTO terminal_session_controls (
                    terminal_session_id, owner_id, epoch, control_state,
                    prepared_at, spawn_boundary_at, attached_at, terminal_at,
                    spawn_boundary_id, opaque_backend_handle, containment_id,
                    host_boot_id, process_fingerprint, row_version, updated_at
                )
                SELECT terminal_session_id, owner_id, epoch,
                       CASE
                           WHEN state IN ('PENDING', 'RUNNING',
                                          'CANCEL_REQUESTED', 'KILL_REQUESTED')
                           THEN 'SPAWN_REQUESTED'
                           ELSE 'TERMINAL'
                       END,
                       created_at,
                       CASE
                           WHEN state IN ('PENDING', 'RUNNING',
                                          'CANCEL_REQUESTED', 'KILL_REQUESTED')
                           THEN started_at ELSE NULL
                       END,
                       NULL,
                       CASE
                           WHEN state IN ('PENDING', 'RUNNING',
                                          'CANCEL_REQUESTED', 'KILL_REQUESTED')
                           THEN NULL ELSE COALESCE(ended_at, updated_at)
                       END,
                       NULL, NULL, NULL, NULL, NULL, 1, updated_at
                FROM terminal_sessions"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS terminal_session_output (
                    terminal_session_id TEXT NOT NULL,
                    seq INTEGER NOT NULL CHECK(seq >= 1),
                    stream TEXT NOT NULL CHECK(stream IN ('stdout', 'stderr')),
                    text TEXT NOT NULL,
                    byte_count INTEGER NOT NULL CHECK(byte_count >= 0),
                    truncated INTEGER NOT NULL CHECK(truncated IN (0, 1)),
                    redacted INTEGER NOT NULL CHECK(redacted IN (0, 1)),
                    at TEXT NOT NULL,
                    event_id TEXT NOT NULL UNIQUE,
                    projection_state TEXT NOT NULL
                        CHECK(projection_state = 'projected'),
                    PRIMARY KEY(terminal_session_id, seq),
                    FOREIGN KEY(terminal_session_id)
                        REFERENCES terminal_sessions(terminal_session_id)
                        ON DELETE CASCADE
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS terminal_session_stop_requests (
                    terminal_session_id TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    epoch INTEGER NOT NULL CHECK(epoch >= 1),
                    expected_turn_id TEXT NOT NULL,
                    expected_state TEXT,
                    scope_digest TEXT NOT NULL,
                    outcome TEXT NOT NULL CHECK(outcome IN (
                        'accepted', 'terminal', 'uncertain'
                    )),
                    row_version INTEGER NOT NULL DEFAULT 1
                        CHECK(row_version >= 1),
                    error_code TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(terminal_session_id, request_id),
                    FOREIGN KEY(terminal_session_id)
                        REFERENCES terminal_sessions(terminal_session_id)
                        ON DELETE CASCADE
                )"""
            )
            conn.execute(
                """CREATE INDEX IF NOT EXISTS
                    idx_terminal_sessions_turn
                    ON terminal_sessions(turn_id, started_at,
                                         terminal_session_id)"""
            )
            conn.execute(
                """CREATE INDEX IF NOT EXISTS
                    idx_terminal_sessions_subject_turn
                    ON terminal_sessions(world_id, engram_id, turn_id,
                                         started_at, terminal_session_id)"""
            )
            conn.execute(
                """CREATE INDEX IF NOT EXISTS
                    idx_terminal_sessions_recovery
                    ON terminal_sessions(state, projection_state,
                                         reconciliation_state, updated_at)"""
            )
            conn.execute(
                """CREATE INDEX IF NOT EXISTS
                    idx_terminal_session_controls_state
                    ON terminal_session_controls(control_state, updated_at,
                                                 terminal_session_id)"""
            )
            conn.execute(
                """CREATE TRIGGER IF NOT EXISTS
                    terminal_sessions_immutable_scope
                    BEFORE UPDATE OF terminal_session_id, turn_id, world_id,
                        engram_id, owner_id, epoch, mode, transport,
                        session_scope, cwd_relative, command_digest,
                        launch_action_digest, start_event_id
                    ON terminal_sessions
                    WHEN OLD.terminal_session_id IS NOT NEW.terminal_session_id
                      OR OLD.turn_id IS NOT NEW.turn_id
                      OR OLD.world_id IS NOT NEW.world_id
                      OR OLD.engram_id IS NOT NEW.engram_id
                      OR OLD.owner_id IS NOT NEW.owner_id
                      OR OLD.epoch IS NOT NEW.epoch
                      OR OLD.mode IS NOT NEW.mode
                      OR OLD.transport IS NOT NEW.transport
                      OR OLD.session_scope IS NOT NEW.session_scope
                      OR OLD.cwd_relative IS NOT NEW.cwd_relative
                      OR OLD.command_digest IS NOT NEW.command_digest
                      OR OLD.launch_action_digest IS NOT NEW.launch_action_digest
                      OR OLD.start_event_id IS NOT NEW.start_event_id
                    BEGIN
                        SELECT RAISE(ABORT,
                            'terminal session immutable scope changed');
                    END"""
            )
            conn.execute(
                """CREATE TRIGGER IF NOT EXISTS
                    terminal_session_controls_immutable_scope
                    BEFORE UPDATE OF terminal_session_id, owner_id, epoch,
                                     prepared_at
                    ON terminal_session_controls
                    WHEN OLD.terminal_session_id IS NOT NEW.terminal_session_id
                      OR OLD.owner_id IS NOT NEW.owner_id
                      OR OLD.epoch IS NOT NEW.epoch
                      OR OLD.prepared_at IS NOT NEW.prepared_at
                    BEGIN
                        SELECT RAISE(ABORT,
                            'terminal session control scope changed');
                    END"""
            )

        self._storage._harness_operation_write(write)

    def _now(self) -> datetime:
        return _utc(self._clock())

    @staticmethod
    def _assert_current_lease(
        conn,
        *,
        owner_id: str,
        epoch: int,
        now: datetime,
    ) -> None:
        row = conn.execute(
            "SELECT owner_id, epoch, state, expires_at FROM runtime_leases "
            "WHERE scope = ?",
            (RUNTIME_LEASE_SCOPE,),
        ).fetchone()
        if row is None:
            raise TerminalSessionLeaseError("lease_absent")
        if str(row[2]) != "active":
            reason = "lease_released"
        elif str(row[0]) != owner_id:
            reason = "owner_mismatch"
        elif int(row[1]) != epoch:
            reason = "epoch_mismatch"
        elif _parse_time(str(row[3])) <= now:
            reason = "lease_expired"
        else:
            return
        raise TerminalSessionLeaseError(reason)

    @staticmethod
    def _row(row: tuple[Any, ...]) -> _SessionRow:
        summary = TerminalSessionSummary(
            terminal_session_id=str(row[0]),
            turn_id=str(row[1]),
            world_id=str(row[2]),
            engram_id=str(row[3]),
            epoch=int(row[4]),
            state=TerminalState(str(row[5])),
            cwd_relative=str(row[6]),
            command_digest=str(row[7]),
            started_at=str(row[8]),
            ended_at=None if row[9] is None else str(row[9]),
            exit_code=None if row[10] is None else int(row[10]),
            output_bytes=int(row[11]),
            output_truncated=bool(row[12]),
            last_output_seq=int(row[13]),
            evidence_class=str(row[14]),
            sandbox_evidence=str(row[15]),
            tree_containment=TreeContainment(str(row[16])),
            launch_action_digest=None if row[17] is None else str(row[17]),
            error_code=None if row[18] is None else str(row[18]),
            uncertain_reason=None if row[19] is None else str(row[19]),
        )
        return _SessionRow(
            summary=summary,
            owner_id=str(row[20]),
            row_version=int(row[21]),
            projection_state=SessionProjectionState(str(row[22])),
            reconciliation_state=ReconciliationState(str(row[23])),
            start_event_id=str(row[24]),
            terminal_event_id=None if row[25] is None else str(row[25]),
            reconciled_by_owner_id=None if row[26] is None else str(row[26]),
            reconciled_by_epoch=None if row[27] is None else int(row[27]),
        )

    @staticmethod
    def _fetch(conn, terminal_session_id: str) -> _SessionRow | None:
        row = conn.execute(
            _SESSION_SELECT + " WHERE terminal_session_id = ?",
            (terminal_session_id,),
        ).fetchone()
        return None if row is None else TerminalSessionStore._row(row)

    @staticmethod
    def _require_row(conn, terminal_session_id: str) -> _SessionRow:
        row = TerminalSessionStore._fetch(conn, terminal_session_id)
        if row is None:
            raise TerminalSessionNotFoundError(terminal_session_id)
        return row

    @staticmethod
    def _control_row(row: tuple[Any, ...]) -> _SessionControlRow:
        return _SessionControlRow(
            terminal_session_id=str(row[0]),
            owner_id=str(row[1]),
            epoch=int(row[2]),
            state=_SessionControlState(str(row[3])),
            prepared_at=str(row[4]),
            spawn_boundary_at=None if row[5] is None else str(row[5]),
            attached_at=None if row[6] is None else str(row[6]),
            terminal_at=None if row[7] is None else str(row[7]),
            spawn_boundary_id=None if row[8] is None else str(row[8]),
            opaque_backend_handle=None if row[9] is None else str(row[9]),
            containment_id=None if row[10] is None else str(row[10]),
            host_boot_id=None if row[11] is None else str(row[11]),
            process_fingerprint=None if row[12] is None else str(row[12]),
            row_version=int(row[13]),
        )

    @staticmethod
    def _fetch_control(conn, terminal_session_id: str) -> _SessionControlRow | None:
        row = conn.execute(
            """SELECT terminal_session_id, owner_id, epoch, control_state,
                      prepared_at, spawn_boundary_at, attached_at, terminal_at,
                      spawn_boundary_id, opaque_backend_handle, containment_id,
                      host_boot_id, process_fingerprint, row_version
               FROM terminal_session_controls
               WHERE terminal_session_id = ?""",
            (terminal_session_id,),
        ).fetchone()
        return None if row is None else TerminalSessionStore._control_row(row)

    @staticmethod
    def _require_control(conn, terminal_session_id: str) -> _SessionControlRow:
        row = TerminalSessionStore._fetch_control(conn, terminal_session_id)
        if row is None:
            raise TerminalSessionConflictError(
                "terminal session is missing private control facts"
            )
        return row

    def _append_event(self, draft: HarnessEventDraft) -> HarnessEvent:
        return self._storage.append_harness_event(
            draft,
            max_events_per_turn=self._events.max_events_per_turn,
            max_payload_bytes=self._events.max_payload_bytes,
            max_total_rows=self._events.max_total_rows,
            max_total_bytes=self._events.max_total_bytes,
        )

    def _notify(self, events: tuple[HarnessEvent, ...]) -> None:
        observer = self._events.observer
        if observer is None:
            return
        for event in events:
            try:
                observer(event)
            except Exception:
                # The durable transaction has committed.  Optional SSE/UI
                # observation cannot retroactively replay an OS boundary.
                pass

    @staticmethod
    def _summary_payload(summary: TerminalSessionSummary) -> dict[str, Any]:
        payload = summary.to_wire()
        payload["item_id"] = f"terminal_session:{summary.terminal_session_id}"
        return payload

    @staticmethod
    def _terminal_status(state: TerminalState) -> HarnessEventStatus:
        if state is TerminalState.EXITED:
            return HarnessEventStatus.COMPLETED
        if state in {
            TerminalState.CANCELLED,
            TerminalState.KILLED,
            TerminalState.TIMED_OUT,
        }:
            return HarnessEventStatus.CANCELLED
        if state is TerminalState.UNCERTAIN:
            return HarnessEventStatus.UNCERTAIN
        return HarnessEventStatus.FAILED

    def _started_draft(self, row: _SessionRow) -> HarnessEventDraft:
        return HarnessEventDraft(
            event_id=row.start_event_id,
            turn_id=row.summary.turn_id,
            world_id=row.summary.world_id,
            engram_id=row.summary.engram_id,
            kind=HarnessEventKind.COMMAND_STARTED,
            phase=HarnessEventPhase.START,
            source=HarnessEventSource.TERMINAL,
            status=HarnessEventStatus.RUNNING,
            occurred_at=_parse_time(row.summary.started_at),
            payload=self._summary_payload(row.summary),
        )

    def _terminal_draft(
        self,
        row: _SessionRow,
        *,
        recovery: bool,
    ) -> HarnessEventDraft:
        if row.terminal_event_id is None:
            raise TerminalSessionError("terminal row is missing terminal_event_id")
        return HarnessEventDraft(
            event_id=row.terminal_event_id,
            turn_id=row.summary.turn_id,
            world_id=row.summary.world_id,
            engram_id=row.summary.engram_id,
            kind=HarnessEventKind.COMMAND_COMPLETED,
            phase=(
                HarnessEventPhase.RECOVERY
                if recovery
                else HarnessEventPhase.TERMINAL
            ),
            source=(
                HarnessEventSource.RECOVERY
                if recovery
                else HarnessEventSource.TERMINAL
            ),
            status=self._terminal_status(row.summary.state),
            occurred_at=_parse_time(row.summary.ended_at or row.summary.started_at),
            payload=self._summary_payload(row.summary),
        )

    def _prune_for_capacity(self, conn) -> None:
        count = int(conn.execute("SELECT COUNT(*) FROM terminal_sessions").fetchone()[0])
        if count < self._max_sessions:
            return
        needed = count - self._max_sessions + 1
        ids = [
            str(row[0])
            for row in conn.execute(
                "SELECT terminal_session_id FROM terminal_sessions "
                "WHERE state NOT IN ('PENDING', 'RUNNING', "
                "'CANCEL_REQUESTED', 'KILL_REQUESTED') "
                "ORDER BY COALESCE(ended_at, started_at), terminal_session_id "
                "LIMIT ?",
                (needed,),
            ).fetchall()
        ]
        for terminal_session_id in ids:
            conn.execute(
                "DELETE FROM terminal_session_stop_requests "
                "WHERE terminal_session_id = ?",
                (terminal_session_id,),
            )
            conn.execute(
                "DELETE FROM terminal_session_output WHERE terminal_session_id = ?",
                (terminal_session_id,),
            )
            conn.execute(
                "DELETE FROM terminal_session_controls WHERE terminal_session_id = ?",
                (terminal_session_id,),
            )
            conn.execute(
                "DELETE FROM terminal_sessions WHERE terminal_session_id = ?",
                (terminal_session_id,),
            )
        retained = int(conn.execute("SELECT COUNT(*) FROM terminal_sessions").fetchone()[0])
        if retained >= self._max_sessions:
            raise TerminalSessionConflictError("terminal session capacity exhausted")

    def reserve_start(
        self,
        *,
        terminal_session_id: str,
        turn_id: str,
        world_id: str,
        engram_id: str,
        owner_id: str,
        epoch: int,
        cwd_relative: str,
        command_digest: str,
        launch_action_digest: str | None,
        evidence_class: str,
        sandbox_evidence: str,
        tree_containment: TreeContainment | str,
    ) -> TerminalSessionSummary:
        terminal_session_id = _token(terminal_session_id, "terminal_session_id")
        turn_id = _identifier(turn_id, "turn_id")
        world_id = _identifier(world_id, "world_id")
        engram_id = _identifier(engram_id, "engram_id")
        owner_id = _token(owner_id, "owner_id")
        epoch = _epoch(epoch)
        cwd_relative = _safe_cwd(cwd_relative)
        command_digest = _digest(command_digest, "command_digest")
        if launch_action_digest is not None:
            launch_action_digest = _digest(
                launch_action_digest,
                "launch_action_digest",
            )
        evidence_class = _bounded_text(
            evidence_class,
            "evidence_class",
            maximum=_MAX_EVIDENCE_BYTES,
        )
        sandbox_evidence = _bounded_text(
            sandbox_evidence,
            "sandbox_evidence",
            maximum=_MAX_EVIDENCE_BYTES,
        )
        tree = TreeContainment(tree_containment)
        start_event_id = deterministic_start_event_id(terminal_session_id)

        def write(conn) -> _SessionRow:
            now = self._now()
            self._assert_current_lease(
                conn,
                owner_id=owner_id,
                epoch=epoch,
                now=now,
            )
            existing = self._fetch(conn, terminal_session_id)
            if existing is not None:
                raise TerminalSessionConflictError(
                    "terminal_session_id is already retained"
                )
            self._prune_for_capacity(conn)
            stamp = _iso(now)
            conn.execute(
                """INSERT INTO terminal_sessions (
                    terminal_session_id, turn_id, world_id, engram_id,
                    owner_id, epoch, mode, transport, session_scope, state,
                    cwd_relative, command_digest, launch_action_digest,
                    started_at, ended_at, exit_code, output_bytes,
                    output_truncated, last_output_seq, evidence_class,
                    sandbox_evidence, tree_containment, error_code,
                    uncertain_reason, row_version, projection_state,
                    reconciliation_state, start_event_id, terminal_event_id,
                    reconciled_by_owner_id, reconciled_by_epoch,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'PIPE_SESSION', 'pipe',
                          'runtime_connection', 'PENDING', ?, ?, ?, ?, NULL,
                          NULL, 0, 0, 0, ?, ?, ?, NULL, NULL, 1,
                          'start_reserved', 'owned', ?, NULL, NULL, NULL,
                          ?, ?)""",
                (
                    terminal_session_id,
                    turn_id,
                    world_id,
                    engram_id,
                    owner_id,
                    epoch,
                    cwd_relative,
                    command_digest,
                    launch_action_digest,
                    stamp,
                    evidence_class,
                    sandbox_evidence,
                    tree.value,
                    start_event_id,
                    stamp,
                    stamp,
                ),
            )
            conn.execute(
                """INSERT INTO terminal_session_controls (
                    terminal_session_id, owner_id, epoch, control_state,
                    prepared_at, spawn_boundary_at, attached_at, terminal_at,
                    spawn_boundary_id, opaque_backend_handle, containment_id,
                    host_boot_id, process_fingerprint, row_version, updated_at
                ) VALUES (?, ?, ?, 'PREPARED', ?, NULL, NULL, NULL,
                          NULL, NULL, NULL, NULL, NULL, 1, ?)""",
                (
                    terminal_session_id,
                    owner_id,
                    epoch,
                    stamp,
                    stamp,
                ),
            )
            return self._require_row(conn, terminal_session_id)

        row = self._storage._harness_operation_write(write)
        return row.summary

    def mark_spawn_requested(
        self,
        terminal_session_id: str,
        *,
        owner_id: str,
        epoch: int,
        spawn_boundary_id: str,
    ) -> None:
        """Persist the irreversible-spawn boundary before invoking the adapter."""

        terminal_session_id = _token(terminal_session_id, "terminal_session_id")
        owner_id = _token(owner_id, "owner_id")
        epoch = _epoch(epoch)
        spawn_boundary_id = _digest(spawn_boundary_id, "spawn_boundary_id")

        def write(conn) -> None:
            now = self._now()
            self._assert_current_lease(
                conn,
                owner_id=owner_id,
                epoch=epoch,
                now=now,
            )
            session = self._require_row(conn, terminal_session_id)
            control = self._require_control(conn, terminal_session_id)
            if (
                session.owner_id != owner_id
                or session.summary.epoch != epoch
                or control.owner_id != owner_id
                or control.epoch != epoch
            ):
                raise TerminalSessionConflictError(
                    "spawn boundary owner/epoch CAS failed"
                )
            if control.state is _SessionControlState.SPAWN_REQUESTED:
                if control.spawn_boundary_id != spawn_boundary_id:
                    raise TerminalSessionConflictError(
                        "spawn boundary is already bound to different facts"
                    )
                return
            if (
                session.summary.state is not TerminalState.PENDING
                or control.state is not _SessionControlState.PREPARED
            ):
                raise TerminalSessionConflictError(
                    "spawn request requires PREPARED control state"
                )
            stamp = _iso(now)
            updated = conn.execute(
                """UPDATE terminal_session_controls
                   SET control_state = 'SPAWN_REQUESTED',
                       spawn_boundary_at = ?, spawn_boundary_id = ?,
                       row_version = row_version + 1, updated_at = ?
                   WHERE terminal_session_id = ? AND row_version = ?
                     AND control_state = 'PREPARED'""",
                (
                    stamp,
                    spawn_boundary_id,
                    stamp,
                    terminal_session_id,
                    control.row_version,
                ),
            )
            if updated.rowcount != 1:
                raise TerminalSessionConflictError(
                    "spawn boundary row version changed"
                )

        self._storage._harness_operation_write(write)

    def commit_started(
        self,
        terminal_session_id: str,
        *,
        owner_id: str,
        epoch: int,
        handle: ProcessHandle,
        attachment: _AttachmentFacts,
    ) -> TerminalSessionSummary:
        terminal_session_id = _token(terminal_session_id, "terminal_session_id")
        owner_id = _token(owner_id, "owner_id")
        epoch = _epoch(epoch)
        if not isinstance(handle, ProcessHandle) or handle.id != terminal_session_id:
            raise TerminalSessionConflictError("manager handle does not match session")
        if handle.state is not TerminalState.RUNNING:
            raise TerminalSessionConflictError("start commit requires RUNNING evidence")
        if not isinstance(attachment, _AttachmentFacts):
            raise TypeError("attachment must contain private process-boundary facts")
        attachment = _AttachmentFacts(
            spawn_boundary_id=_digest(
                attachment.spawn_boundary_id,
                "spawn_boundary_id",
            ),
            opaque_backend_handle=_digest(
                attachment.opaque_backend_handle,
                "opaque_backend_handle",
            ),
            containment_id=_digest(
                attachment.containment_id,
                "containment_id",
            ),
            host_boot_id=_digest(attachment.host_boot_id, "host_boot_id"),
            process_fingerprint=_digest(
                attachment.process_fingerprint,
                "process_fingerprint",
            ),
        )

        def write(conn) -> tuple[_SessionRow, HarnessEvent, bool]:
            now = self._now()
            self._assert_current_lease(
                conn,
                owner_id=owner_id,
                epoch=epoch,
                now=now,
            )
            current = self._require_row(conn, terminal_session_id)
            control = self._require_control(conn, terminal_session_id)
            if current.projection_state is SessionProjectionState.START_PROJECTED:
                if control.state not in {
                    _SessionControlState.ATTACHED,
                    _SessionControlState.STOP_REQUESTED,
                    _SessionControlState.TERMINAL,
                }:
                    raise TerminalSessionConflictError(
                        "projected start lacks durable attachment facts"
                    )
                if (
                    control.spawn_boundary_id != attachment.spawn_boundary_id
                    or control.opaque_backend_handle
                    != attachment.opaque_backend_handle
                    or control.containment_id != attachment.containment_id
                    or control.host_boot_id != attachment.host_boot_id
                    or control.process_fingerprint
                    != attachment.process_fingerprint
                ):
                    raise TerminalSessionConflictError(
                        "projected start is bound to different attachment facts"
                    )
                event_row = conn.execute(
                    "SELECT event_id FROM harness_events WHERE event_id = ?",
                    (current.start_event_id,),
                ).fetchone()
                if event_row is None:
                    raise TerminalSessionConflictError(
                        "start projection state has no durable event"
                    )
                event = self._storage.get_harness_event(current.start_event_id)
                assert event is not None
                return current, event, False
            if (
                current.summary.state is not TerminalState.PENDING
                or current.owner_id != owner_id
                or current.summary.epoch != epoch
                or control.owner_id != owner_id
                or control.epoch != epoch
                or control.state is not _SessionControlState.SPAWN_REQUESTED
                or control.spawn_boundary_id != attachment.spawn_boundary_id
            ):
                raise TerminalSessionConflictError("start row CAS failed")
            stamp = _iso(now)
            updated = conn.execute(
                """UPDATE terminal_sessions
                   SET state = 'RUNNING', started_at = ?,
                       projection_state = 'start_projected',
                       reconciliation_state = 'owned',
                       row_version = row_version + 1, updated_at = ?
                   WHERE terminal_session_id = ? AND row_version = ?
                     AND state = 'PENDING'""",
                (
                    handle.started_at,
                    stamp,
                    terminal_session_id,
                    current.row_version,
                ),
            )
            if updated.rowcount != 1:
                raise TerminalSessionConflictError("start row version changed")
            attached = conn.execute(
                """UPDATE terminal_session_controls
                   SET control_state = 'ATTACHED', attached_at = ?,
                       opaque_backend_handle = ?, containment_id = ?,
                       host_boot_id = ?, process_fingerprint = ?,
                       row_version = row_version + 1, updated_at = ?
                   WHERE terminal_session_id = ? AND row_version = ?
                     AND control_state = 'SPAWN_REQUESTED'
                     AND spawn_boundary_id = ?""",
                (
                    handle.started_at,
                    attachment.opaque_backend_handle,
                    attachment.containment_id,
                    attachment.host_boot_id,
                    attachment.process_fingerprint,
                    stamp,
                    terminal_session_id,
                    control.row_version,
                    attachment.spawn_boundary_id,
                ),
            )
            if attached.rowcount != 1:
                raise TerminalSessionConflictError(
                    "attachment control row version changed"
                )
            winner = self._require_row(conn, terminal_session_id)
            event = self._append_event(self._started_draft(winner))
            return winner, event, True

        row, event, created = self._storage._harness_operation_write(write)
        if created:
            self._notify((event,))
        return row.summary

    def inspect(self, terminal_session_id: str) -> TerminalSessionSummary:
        terminal_session_id = _token(terminal_session_id, "terminal_session_id")
        row = self._storage._harness_operation_read(
            lambda conn: self._fetch(conn, terminal_session_id)
        )
        if row is None:
            raise TerminalSessionNotFoundError(terminal_session_id)
        return row.summary

    def list_for_turn(
        self,
        *,
        world_id: str,
        engram_id: str,
        turn_id: str,
        limit: int = 16,
    ) -> TerminalSessionList:
        world_id = _identifier(world_id, "world_id")
        engram_id = _identifier(engram_id, "engram_id")
        turn_id = _identifier(turn_id, "turn_id")
        limit = _positive_limit(limit, "limit", 64)

        def read(conn) -> list[_SessionRow]:
            rows = conn.execute(
                _SESSION_SELECT
                + " WHERE world_id = ? AND engram_id = ? AND turn_id = ? "
                "ORDER BY started_at, terminal_session_id LIMIT ?",
                (world_id, engram_id, turn_id, limit),
            ).fetchall()
            return [self._row(row) for row in rows]

        rows = self._storage._harness_operation_read(read)
        sessions = tuple(row.summary for row in rows)
        return TerminalSessionList(
            sessions=sessions,
            count=len(sessions),
            evidence_class=self._aggregate_evidence(sessions),
        )

    @staticmethod
    def _aggregate_evidence(sessions: tuple[TerminalSessionSummary, ...]) -> str:
        if not sessions:
            return LIVE_GATE_UNVERIFIED
        ranks = {
            CONTRACT_ONLY: 0,
            LIVE_GATE_UNVERIFIED: 1,
            LIVE_OS_RESTRICTED: 2,
        }
        return min(
            (session.evidence_class for session in sessions),
            key=lambda item: ranks.get(item, 0),
        )

    def active_for_scope(
        self,
        *,
        world_id: str,
        engram_id: str | None = None,
        turn_id: str | None = None,
    ) -> tuple[TerminalSessionSummary, ...]:
        world_id = _identifier(world_id, "world_id")
        if engram_id is not None:
            engram_id = _identifier(engram_id, "engram_id")
        if turn_id is not None:
            turn_id = _identifier(turn_id, "turn_id")
            if engram_id is None:
                raise TerminalSessionError(
                    "turn-scoped session lookup requires engram_id"
                )

        def read(conn) -> tuple[TerminalSessionSummary, ...]:
            clauses = (
                "state IN ('PENDING', 'RUNNING', "
                "'CANCEL_REQUESTED', 'KILL_REQUESTED')"
                " AND world_id = ?"
            )
            params: list[Any] = [world_id]
            if engram_id is not None:
                clauses += " AND engram_id = ?"
                params.append(engram_id)
            if turn_id is not None:
                clauses += " AND turn_id = ?"
                params.append(turn_id)
            rows = conn.execute(
                _SESSION_SELECT + " WHERE " + clauses + " ORDER BY started_at",
                tuple(params),
            ).fetchall()
            return tuple(self._row(row).summary for row in rows)

        return self._storage._harness_operation_read(read)

    def read_output(
        self,
        terminal_session_id: str,
        *,
        after_seq: int = 0,
        limit: int = 200,
    ) -> TerminalOutputPage:
        terminal_session_id = _token(terminal_session_id, "terminal_session_id")
        if type(after_seq) is not int or after_seq < 0:
            raise TerminalSessionError("after_seq must be a non-negative integer")
        limit = _positive_limit(limit, "limit", 500)

        def read(conn) -> TerminalOutputPage:
            session = self._require_row(conn, terminal_session_id).summary
            earliest_row = conn.execute(
                "SELECT MIN(seq) FROM terminal_session_output "
                "WHERE terminal_session_id = ?",
                (terminal_session_id,),
            ).fetchone()
            retained_earliest = (
                None
                if earliest_row is None or earliest_row[0] is None
                else int(earliest_row[0])
            )
            earliest_seq = (
                retained_earliest
                if retained_earliest is not None
                else (session.last_output_seq + 1 if session.last_output_seq else 0)
            )
            gap = None
            if earliest_seq > 0 and after_seq + 1 < earliest_seq:
                gap = TerminalOutputGap(
                    missing_from=after_seq + 1,
                    missing_to=earliest_seq - 1,
                )
            rows = conn.execute(
                """SELECT terminal_session_id, seq, stream, text,
                          byte_count, truncated, redacted, at
                   FROM terminal_session_output
                   WHERE terminal_session_id = ? AND seq > ?
                   ORDER BY seq LIMIT ?""",
                (terminal_session_id, after_seq, limit + 1),
            ).fetchall()
            has_more = len(rows) > limit
            rows = rows[:limit]
            output = tuple(
                TerminalOutputChunk(
                    terminal_session_id=str(row[0]),
                    seq=int(row[1]),
                    stream=str(row[2]),
                    text=str(row[3]),
                    byte_count=int(row[4]),
                    truncated=bool(row[5]),
                    redacted=bool(row[6]),
                    at=str(row[7]),
                )
                for row in rows
            )
            next_seq = output[-1].seq if output else after_seq
            return TerminalOutputPage(
                terminal_session_id=terminal_session_id,
                output=output,
                earliest_seq=earliest_seq,
                next_seq=next_seq,
                has_more=has_more,
                gap=gap,
                truncated=session.output_truncated or gap is not None,
                evidence_class=session.evidence_class,
            )

        return self._storage._harness_operation_read(read)

    def append_output(
        self,
        terminal_session_id: str,
        *,
        owner_id: str,
        epoch: int,
        progress: CommandProgress,
    ) -> TerminalOutputChunk:
        terminal_session_id = _token(terminal_session_id, "terminal_session_id")
        owner_id = _token(owner_id, "owner_id")
        epoch = _epoch(epoch)
        if not isinstance(progress, CommandProgress):
            raise TypeError("progress must be CommandProgress")
        if progress.handle_id != terminal_session_id:
            raise TerminalSessionConflictError("output handle does not match session")
        if type(progress.seq) is not int or progress.seq < 1:
            raise TerminalSessionError("output seq must be an integer >= 1")
        stream = progress.stream if progress.stream == "stdout" else "stderr"
        text, clipped = _clip_utf8_text(
            progress.text,
            maximum=_MAX_OUTPUT_TEXT_BYTES,
        )
        if type(progress.byte_count) is not int or progress.byte_count < 0:
            raise TerminalSessionError("output byte_count must be non-negative")
        byte_count = min(progress.byte_count, _MAX_OUTPUT_TEXT_BYTES)
        truncated = bool(progress.truncated or clipped or byte_count < progress.byte_count)
        event_id = deterministic_output_event_id(terminal_session_id, progress.seq)
        chunk = TerminalOutputChunk(
            terminal_session_id=terminal_session_id,
            seq=progress.seq,
            stream=stream,
            text=text,
            byte_count=byte_count,
            truncated=truncated,
            redacted=bool(progress.redacted),
            at=progress.at,
        )

        def write(conn) -> tuple[TerminalOutputChunk, HarnessEvent, bool]:
            now = self._now()
            self._assert_current_lease(
                conn,
                owner_id=owner_id,
                epoch=epoch,
                now=now,
            )
            current = self._require_row(conn, terminal_session_id)
            control = self._require_control(conn, terminal_session_id)
            if current.owner_id != owner_id or current.summary.epoch != epoch:
                raise TerminalSessionConflictError("output owner/epoch CAS failed")
            if control.state not in {
                _SessionControlState.ATTACHED,
                _SessionControlState.STOP_REQUESTED,
            }:
                raise TerminalSessionConflictError(
                    "output has no durable process attachment"
                )
            existing = conn.execute(
                """SELECT terminal_session_id, seq, stream, text,
                          byte_count, truncated, redacted, at, event_id
                   FROM terminal_session_output
                   WHERE terminal_session_id = ? AND seq = ?""",
                (terminal_session_id, progress.seq),
            ).fetchone()
            if existing is not None:
                stored = TerminalOutputChunk(
                    terminal_session_id=str(existing[0]),
                    seq=int(existing[1]),
                    stream=str(existing[2]),
                    text=str(existing[3]),
                    byte_count=int(existing[4]),
                    truncated=bool(existing[5]),
                    redacted=bool(existing[6]),
                    at=str(existing[7]),
                )
                if stored != chunk or str(existing[8]) != event_id:
                    raise TerminalSessionConflictError(
                        "output seq is already bound to different facts"
                    )
                event = self._storage.get_harness_event(event_id)
                if event is None:
                    raise TerminalSessionConflictError(
                        "projected output is missing its Harness event"
                    )
                return stored, event, False
            if current.summary.state not in _ACTIVE_STATES:
                raise TerminalSessionConflictError(
                    "output cannot append after durable terminal state"
                )
            if progress.seq != current.summary.last_output_seq + 1:
                raise TerminalSessionConflictError(
                    "output seq must be monotonic without an invented gap"
                )
            if current.summary.output_bytes + byte_count > self._max_output_bytes:
                raise TerminalSessionConflictError(
                    "terminal session output capacity exhausted"
                )
            conn.execute(
                """INSERT INTO terminal_session_output (
                    terminal_session_id, seq, stream, text, byte_count,
                    truncated, redacted, at, event_id, projection_state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'projected')""",
                (
                    terminal_session_id,
                    chunk.seq,
                    chunk.stream,
                    chunk.text,
                    chunk.byte_count,
                    int(chunk.truncated),
                    int(chunk.redacted),
                    chunk.at,
                    event_id,
                ),
            )
            retained = int(
                conn.execute(
                    "SELECT COUNT(*) FROM terminal_session_output "
                    "WHERE terminal_session_id = ?",
                    (terminal_session_id,),
                ).fetchone()[0]
            )
            pruned = max(0, retained - self._max_output_chunks)
            if pruned:
                conn.execute(
                    """DELETE FROM terminal_session_output
                       WHERE terminal_session_id = ? AND seq IN (
                           SELECT seq FROM terminal_session_output
                           WHERE terminal_session_id = ?
                           ORDER BY seq LIMIT ?
                       )""",
                    (terminal_session_id, terminal_session_id, pruned),
                )
            updated = conn.execute(
                """UPDATE terminal_sessions
                   SET output_bytes = output_bytes + ?,
                       output_truncated = CASE
                           WHEN output_truncated = 1 OR ? = 1 OR ? > 0
                           THEN 1 ELSE 0 END,
                       last_output_seq = ?, row_version = row_version + 1,
                       updated_at = ?
                   WHERE terminal_session_id = ? AND row_version = ?
                     AND state IN ('PENDING', 'RUNNING',
                                   'CANCEL_REQUESTED', 'KILL_REQUESTED')""",
                (
                    byte_count,
                    int(chunk.truncated),
                    pruned,
                    chunk.seq,
                    _iso(now),
                    terminal_session_id,
                    current.row_version,
                ),
            )
            if updated.rowcount != 1:
                raise TerminalSessionConflictError("output row version changed")
            winner = self._require_row(conn, terminal_session_id)
            payload = chunk.to_wire()
            payload.update(
                {
                    "item_id": f"terminal_session:{terminal_session_id}",
                    "evidence_class": winner.summary.evidence_class,
                }
            )
            event = self._append_event(
                HarnessEventDraft(
                    event_id=event_id,
                    turn_id=winner.summary.turn_id,
                    world_id=winner.summary.world_id,
                    engram_id=winner.summary.engram_id,
                    kind=HarnessEventKind.COMMAND_OUTPUT,
                    phase=HarnessEventPhase.STREAM,
                    source=HarnessEventSource.TERMINAL,
                    status=HarnessEventStatus.RUNNING,
                    occurred_at=_parse_time(chunk.at),
                    payload=payload,
                )
            )
            return chunk, event, True

        stored, event, created = self._storage._harness_operation_write(write)
        if created:
            self._notify((event,))
        return stored

    @staticmethod
    def _stop_scope_digest(
        *,
        terminal_session_id: str,
        request_id: str,
        expected_world_id: str,
        expected_engram_id: str,
        expected_turn_id: str,
        expected_epoch: int,
        expected_state: TerminalState | None,
    ) -> str:
        value = "\x00".join(
            (
                terminal_session_id,
                request_id,
                expected_world_id,
                expected_engram_id,
                expected_turn_id,
                str(expected_epoch),
                "" if expected_state is None else expected_state.value,
            )
        )
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def begin_stop(
        self,
        terminal_session_id: str,
        *,
        request_id: str,
        owner_id: str,
        epoch: int,
        expected_world_id: str,
        expected_engram_id: str,
        expected_turn_id: str,
        expected_epoch: int,
        expected_state: TerminalState | str | None = None,
    ) -> _StopClaim:
        terminal_session_id = _token(terminal_session_id, "terminal_session_id")
        request_id = _token(request_id, "request_id")
        owner_id = _token(owner_id, "owner_id")
        epoch = _epoch(epoch)
        expected_world_id = _identifier(expected_world_id, "expected_world_id")
        expected_engram_id = _identifier(expected_engram_id, "expected_engram_id")
        expected_turn_id = _identifier(expected_turn_id, "expected_turn_id")
        expected_epoch = _epoch(expected_epoch, "expected_epoch")
        expected = None if expected_state is None else TerminalState(expected_state)
        scope_digest = self._stop_scope_digest(
            terminal_session_id=terminal_session_id,
            request_id=request_id,
            expected_world_id=expected_world_id,
            expected_engram_id=expected_engram_id,
            expected_turn_id=expected_turn_id,
            expected_epoch=expected_epoch,
            expected_state=expected,
        )

        def write(conn) -> _StopClaim:
            now = self._now()
            self._assert_current_lease(
                conn,
                owner_id=owner_id,
                epoch=epoch,
                now=now,
            )
            current = self._require_row(conn, terminal_session_id)
            control = self._require_control(conn, terminal_session_id)
            existing = conn.execute(
                """SELECT scope_digest FROM terminal_session_stop_requests
                   WHERE terminal_session_id = ? AND request_id = ?""",
                (terminal_session_id, request_id),
            ).fetchone()
            if existing is not None:
                if str(existing[0]) != scope_digest:
                    raise TerminalSessionConflictError(
                        "stop request id was reused with a different scope"
                    )
                return _StopClaim(current, True)
            if current.summary.world_id != expected_world_id:
                raise TerminalSessionConflictError("stop world scope drift")
            if current.summary.engram_id != expected_engram_id:
                raise TerminalSessionConflictError("stop Engram scope drift")
            if current.summary.turn_id != expected_turn_id:
                raise TerminalSessionConflictError("stop turn scope drift")
            if current.summary.epoch != expected_epoch:
                raise TerminalSessionConflictError("stop expected epoch drift")
            if (
                current.owner_id != owner_id
                or current.summary.epoch != epoch
                or control.owner_id != owner_id
                or control.epoch != epoch
            ):
                raise TerminalSessionConflictError("stop owner/epoch CAS failed")
            if expected is not None and current.summary.state is not expected:
                raise TerminalSessionConflictError("stop expected state drift")
            count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM terminal_session_stop_requests "
                    "WHERE terminal_session_id = ?",
                    (terminal_session_id,),
                ).fetchone()[0]
            )
            if count >= _MAX_STOP_REQUESTS_PER_SESSION:
                conn.execute(
                    """DELETE FROM terminal_session_stop_requests
                       WHERE terminal_session_id = ? AND request_id IN (
                           SELECT request_id FROM terminal_session_stop_requests
                           WHERE terminal_session_id = ?
                           ORDER BY created_at LIMIT ?
                       )""",
                    (
                        terminal_session_id,
                        terminal_session_id,
                        count - _MAX_STOP_REQUESTS_PER_SESSION + 1,
                    ),
                )
            outcome = "terminal" if current.summary.state.is_terminal else "accepted"
            stamp = _iso(now)
            conn.execute(
                """INSERT INTO terminal_session_stop_requests (
                    terminal_session_id, request_id, owner_id, epoch,
                    expected_turn_id, expected_state, scope_digest, outcome,
                    row_version, error_code, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, ?, ?)""",
                (
                    terminal_session_id,
                    request_id,
                    owner_id,
                    epoch,
                    expected_turn_id,
                    None if expected is None else expected.value,
                    scope_digest,
                    outcome,
                    stamp,
                    stamp,
                ),
            )
            if current.summary.state.is_terminal:
                return _StopClaim(current, False)
            next_state = (
                TerminalState.CANCEL_REQUESTED
                if current.summary.state
                in {TerminalState.PENDING, TerminalState.RUNNING}
                else current.summary.state
            )
            updated = conn.execute(
                """UPDATE terminal_sessions
                   SET state = ?, reconciliation_state = 'stop_requested',
                       row_version = row_version + 1, updated_at = ?
                   WHERE terminal_session_id = ? AND row_version = ?
                     AND state IN ('PENDING', 'RUNNING',
                                   'CANCEL_REQUESTED', 'KILL_REQUESTED')""",
                (
                    next_state.value,
                    stamp,
                    terminal_session_id,
                    current.row_version,
                ),
            )
            if updated.rowcount != 1:
                raise TerminalSessionConflictError("stop row version changed")
            if control.state is not _SessionControlState.TERMINAL:
                control_updated = conn.execute(
                    """UPDATE terminal_session_controls
                       SET control_state = 'STOP_REQUESTED',
                           row_version = row_version + 1, updated_at = ?
                       WHERE terminal_session_id = ? AND row_version = ?
                         AND control_state IN (
                             'PREPARED', 'SPAWN_REQUESTED', 'ATTACHED',
                             'STOP_REQUESTED'
                         )""",
                    (
                        stamp,
                        terminal_session_id,
                        control.row_version,
                    ),
                )
                if control_updated.rowcount != 1:
                    raise TerminalSessionConflictError(
                        "stop control row version changed"
                    )
            return _StopClaim(
                self._require_row(conn, terminal_session_id),
                False,
            )

        return self._storage._harness_operation_write(write)

    @staticmethod
    def _transition_control_terminal(
        conn,
        control: _SessionControlRow,
        *,
        terminal_at: str,
    ) -> None:
        if control.state is _SessionControlState.TERMINAL:
            return
        updated = conn.execute(
            """UPDATE terminal_session_controls
               SET control_state = 'TERMINAL', terminal_at = ?,
                   row_version = row_version + 1, updated_at = ?
               WHERE terminal_session_id = ? AND row_version = ?
                 AND control_state IN (
                     'PREPARED', 'SPAWN_REQUESTED', 'ATTACHED',
                     'STOP_REQUESTED'
                 )""",
            (
                terminal_at,
                terminal_at,
                control.terminal_session_id,
                control.row_version,
            ),
        )
        if updated.rowcount != 1:
            raise TerminalSessionConflictError(
                "terminal control row version changed"
            )

    def commit_terminal(
        self,
        terminal_session_id: str,
        *,
        owner_id: str,
        epoch: int,
        state: TerminalState | str,
        ended_at: str | None,
        exit_code: int | None,
        error_code: str | None,
        uncertain_reason: str | None,
        output_truncated: bool = False,
        recovery: bool = False,
        recovery_permit: RuntimeRecoveryPermit | None = None,
        terminal_event_id: str | None = None,
    ) -> TerminalSessionSummary:
        terminal_session_id = _token(terminal_session_id, "terminal_session_id")
        owner_id = _token(owner_id, "owner_id")
        epoch = _epoch(epoch)
        if recovery_permit is not None:
            if not isinstance(recovery_permit, RuntimeRecoveryPermit):
                raise TypeError(
                    "recovery_permit must be a RuntimeRecoveryPermit or null"
                )
            if (
                recovery_permit.owner_id != owner_id
                or recovery_permit.epoch != epoch
            ):
                raise TerminalSessionLeaseError(
                    "terminal recovery permit belongs to another owner epoch"
                )
            recovery_permit.assert_recovery()
            recovery = True
        state = TerminalState(state)
        if not state.is_terminal:
            raise TerminalSessionError("terminal commit requires terminal state")
        if exit_code is not None and type(exit_code) is not int:
            raise TerminalSessionError("exit_code must be an integer or null")
        if error_code is not None:
            error_code = _bounded_text(
                error_code,
                "error_code",
                maximum=_MAX_ERROR_BYTES,
            )
        if uncertain_reason is not None:
            uncertain_reason = _bounded_text(
                uncertain_reason,
                "uncertain_reason",
                maximum=_MAX_ERROR_BYTES,
            )
        terminal_event_id = terminal_event_id or deterministic_terminal_event_id(
            terminal_session_id
        )
        terminal_event_id = _token(terminal_event_id, "terminal_event_id")

        def write(conn) -> tuple[_SessionRow, HarnessEvent, bool]:
            now = self._now()
            self._assert_current_lease(
                conn,
                owner_id=owner_id,
                epoch=epoch,
                now=now,
            )
            current = self._require_row(conn, terminal_session_id)
            control = self._require_control(conn, terminal_session_id)
            if current.summary.state.is_terminal:
                self._transition_control_terminal(
                    conn,
                    control,
                    terminal_at=current.summary.ended_at or _iso(now),
                )
                if current.terminal_event_id is None:
                    raise TerminalSessionConflictError(
                        "terminal winner is missing deterministic event id"
                    )
                if current.terminal_event_id != terminal_event_id and not recovery:
                    terminal_id = current.terminal_event_id
                else:
                    terminal_id = current.terminal_event_id
                if current.projection_state is SessionProjectionState.TERMINAL_PENDING:
                    event = self._append_event(
                        self._terminal_draft(current, recovery=True)
                    )
                    updated = conn.execute(
                        """UPDATE terminal_sessions
                           SET projection_state = 'terminal_projected',
                               reconciliation_state = 'settled',
                               row_version = row_version + 1, updated_at = ?
                           WHERE terminal_session_id = ? AND row_version = ?
                             AND terminal_event_id = ?""",
                        (
                            _iso(now),
                            terminal_session_id,
                            current.row_version,
                            terminal_id,
                        ),
                    )
                    if updated.rowcount != 1:
                        raise TerminalSessionConflictError(
                            "terminal projection row version changed"
                        )
                    return self._require_row(conn, terminal_session_id), event, True
                event = self._storage.get_harness_event(terminal_id)
                if event is None:
                    raise TerminalSessionConflictError(
                        "terminal projection state has no durable event"
                    )
                return current, event, False
            if current.owner_id != owner_id or current.summary.epoch != epoch:
                raise TerminalSessionConflictError("terminal owner/epoch CAS failed")
            if control.owner_id != current.owner_id or control.epoch != current.summary.epoch:
                raise TerminalSessionConflictError(
                    "terminal private control scope drift"
                )
            stamp = ended_at or _iso(now)
            _parse_time(stamp)
            updated = conn.execute(
                """UPDATE terminal_sessions
                   SET state = ?, ended_at = ?, exit_code = ?,
                       output_truncated = CASE
                           WHEN output_truncated = 1 OR ? = 1 THEN 1 ELSE 0 END,
                       error_code = ?, uncertain_reason = ?,
                       terminal_event_id = ?,
                       projection_state = 'terminal_projected',
                       reconciliation_state = ?,
                       reconciled_by_owner_id = CASE WHEN ? = 1 THEN ? ELSE NULL END,
                       reconciled_by_epoch = CASE WHEN ? = 1 THEN ? ELSE NULL END,
                       row_version = row_version + 1, updated_at = ?
                   WHERE terminal_session_id = ? AND row_version = ?
                     AND state IN ('PENDING', 'RUNNING',
                                   'CANCEL_REQUESTED', 'KILL_REQUESTED')""",
                (
                    state.value,
                    stamp,
                    exit_code,
                    int(output_truncated),
                    error_code,
                    uncertain_reason,
                    terminal_event_id,
                    (
                        ReconciliationState.RECOVERED.value
                        if recovery
                        else ReconciliationState.SETTLED.value
                    ),
                    int(recovery),
                    owner_id,
                    int(recovery),
                    epoch,
                    _iso(now),
                    terminal_session_id,
                    current.row_version,
                ),
            )
            if updated.rowcount != 1:
                winner = self._require_row(conn, terminal_session_id)
                if winner.summary.state.is_terminal:
                    event = self._storage.get_harness_event(
                        winner.terminal_event_id or ""
                    )
                    if event is None:
                        raise TerminalSessionConflictError(
                            "terminal winner lacks durable projection"
                        )
                    return winner, event, False
                raise TerminalSessionConflictError("terminal row version changed")
            self._transition_control_terminal(
                conn,
                control,
                terminal_at=stamp,
            )
            winner = self._require_row(conn, terminal_session_id)
            event = self._append_event(
                self._terminal_draft(winner, recovery=recovery)
            )
            conn.execute(
                """UPDATE terminal_session_stop_requests
                   SET outcome = ?, error_code = ?,
                       row_version = row_version + 1, updated_at = ?
                   WHERE terminal_session_id = ?""",
                (
                    "uncertain" if state is TerminalState.UNCERTAIN else "terminal",
                    error_code,
                    _iso(now),
                    terminal_session_id,
                ),
            )
            return winner, event, True

        if recovery_permit is None:
            row, event, created = self._storage._harness_operation_write(write)
        else:
            with self._storage._runtime_authority_scope(recovery_permit):
                row, event, created = self._storage._harness_operation_write(write)
        if created and recovery_permit is None:
            self._notify((event,))
        return row.summary

    def mark_unprojected_uncertain(
        self,
        terminal_session_id: str,
        *,
        owner_id: str,
        epoch: int,
        error_code: str,
    ) -> TerminalSessionSummary:
        terminal_session_id = _token(terminal_session_id, "terminal_session_id")
        owner_id = _token(owner_id, "owner_id")
        epoch = _epoch(epoch)
        error_code = _bounded_text(
            error_code,
            "error_code",
            maximum=_MAX_ERROR_BYTES,
        )
        event_id = deterministic_terminal_event_id(terminal_session_id)

        def write(conn) -> _SessionRow:
            now = self._now()
            self._assert_current_lease(
                conn,
                owner_id=owner_id,
                epoch=epoch,
                now=now,
            )
            current = self._require_row(conn, terminal_session_id)
            control = self._require_control(conn, terminal_session_id)
            if current.summary.state.is_terminal:
                return current
            if (
                current.owner_id != owner_id
                or current.summary.epoch != epoch
                or control.owner_id != owner_id
                or control.epoch != epoch
            ):
                raise TerminalSessionConflictError(
                    "uncertain terminal owner/epoch CAS failed"
                )
            stamp = _iso(now)
            updated = conn.execute(
                """UPDATE terminal_sessions
                   SET state = 'UNCERTAIN', ended_at = ?, exit_code = NULL,
                       output_truncated = 1, error_code = ?,
                       uncertain_reason = ?, terminal_event_id = ?,
                       projection_state = 'terminal_pending',
                       reconciliation_state = 'projection_required',
                       row_version = row_version + 1, updated_at = ?
                   WHERE terminal_session_id = ? AND row_version = ?
                     AND state IN ('PENDING', 'RUNNING',
                                   'CANCEL_REQUESTED', 'KILL_REQUESTED')""",
                (
                    stamp,
                    error_code,
                    error_code,
                    event_id,
                    stamp,
                    terminal_session_id,
                    current.row_version,
                ),
            )
            if updated.rowcount != 1:
                raise TerminalSessionConflictError(
                    "uncertain terminal row version changed"
                )
            self._transition_control_terminal(
                conn,
                control,
                terminal_at=stamp,
            )
            conn.execute(
                """UPDATE terminal_session_stop_requests
                   SET outcome = 'uncertain', error_code = ?,
                       row_version = row_version + 1, updated_at = ?
                   WHERE terminal_session_id = ?""",
                (error_code, _iso(now), terminal_session_id),
            )
            return self._require_row(conn, terminal_session_id)

        return self._storage._harness_operation_write(write).summary

    def pending_projection_ids(
        self,
        *,
        world_id: str,
        owner_id: str,
        epoch: int,
        limit: int = _MAX_RECONCILIATION_BATCH,
    ) -> tuple[str, ...]:
        """Return a bounded snapshot of current-owner terminal projections."""

        world_id = _identifier(world_id, "world_id")
        owner_id = _token(owner_id, "owner_id")
        epoch = _epoch(epoch)
        limit = _positive_limit(limit, "limit", _MAX_RECONCILIATION_BATCH)

        def read(conn) -> tuple[str, ...]:
            return tuple(
                str(row[0])
                for row in conn.execute(
                    """SELECT terminal_session_id
                       FROM terminal_sessions
                       WHERE world_id = ? AND owner_id = ? AND epoch = ?
                         AND projection_state = 'terminal_pending'
                         AND state NOT IN (
                             'PENDING', 'RUNNING',
                             'CANCEL_REQUESTED', 'KILL_REQUESTED'
                         )
                       ORDER BY updated_at, terminal_session_id
                       LIMIT ?""",
                    (world_id, owner_id, epoch, limit),
                ).fetchall()
            )

        return self._storage._harness_operation_read(read)

    def reconcile_pending(
        self,
        terminal_session_id: str,
        *,
        owner_id: str,
        epoch: int,
    ) -> TerminalSessionSummary:
        """Project one current-owner terminal winner under the Runtime lease."""

        terminal_session_id = _token(terminal_session_id, "terminal_session_id")
        owner_id = _token(owner_id, "owner_id")
        epoch = _epoch(epoch)

        def write(conn) -> tuple[_SessionRow, HarnessEvent | None, bool]:
            now = self._now()
            self._assert_current_lease(
                conn,
                owner_id=owner_id,
                epoch=epoch,
                now=now,
            )
            current = self._require_row(conn, terminal_session_id)
            control = self._require_control(conn, terminal_session_id)
            if current.owner_id != owner_id or current.summary.epoch != epoch:
                raise TerminalSessionConflictError(
                    "pending projection is not owned by this Runtime"
                )
            if control.owner_id != owner_id or control.epoch != epoch:
                raise TerminalSessionConflictError(
                    "pending projection private scope drift"
                )
            if not current.summary.state.is_terminal:
                raise TerminalSessionConflictError(
                    "pending projection has no terminal winner"
                )
            if current.projection_state is SessionProjectionState.TERMINAL_PROJECTED:
                event = self._storage.get_harness_event(
                    current.terminal_event_id or ""
                )
                if event is None:
                    raise TerminalSessionConflictError(
                        "terminal projection state has no durable event"
                    )
                return current, event, False
            if (
                current.projection_state is not SessionProjectionState.TERMINAL_PENDING
                or current.terminal_event_id is None
            ):
                raise TerminalSessionConflictError(
                    "session is not awaiting terminal projection"
                )
            self._transition_control_terminal(
                conn,
                control,
                terminal_at=current.summary.ended_at or _iso(now),
            )
            event = self._append_event(
                self._terminal_draft(current, recovery=False)
            )
            updated = conn.execute(
                """UPDATE terminal_sessions
                   SET projection_state = 'terminal_projected',
                       reconciliation_state = 'settled',
                       row_version = row_version + 1, updated_at = ?
                   WHERE terminal_session_id = ? AND row_version = ?
                     AND projection_state = 'terminal_pending'
                     AND terminal_event_id = ?""",
                (
                    _iso(now),
                    terminal_session_id,
                    current.row_version,
                    current.terminal_event_id,
                ),
            )
            if updated.rowcount != 1:
                raise TerminalSessionConflictError(
                    "terminal reconciliation row version changed"
                )
            return self._require_row(conn, terminal_session_id), event, True

        row, event, created = self._storage._harness_operation_write(write)
        if created and event is not None:
            self._notify((event,))
        return row.summary

    def recover_orphaned(
        self,
        *,
        owner_id: str,
        epoch: int,
    ) -> tuple[TerminalSessionSummary, ...]:
        owner_id = _token(owner_id, "owner_id")
        epoch = _epoch(epoch)

        def write(conn) -> tuple[tuple[_SessionRow, ...], tuple[HarnessEvent, ...]]:
            now = self._now()
            self._assert_current_lease(
                conn,
                owner_id=owner_id,
                epoch=epoch,
                now=now,
            )
            rows = [
                self._row(row)
                for row in conn.execute(
                    _SESSION_SELECT
                    + " WHERE state IN ('PENDING', 'RUNNING', "
                    "'CANCEL_REQUESTED', 'KILL_REQUESTED') "
                    "OR projection_state = 'terminal_pending' "
                    "ORDER BY started_at, terminal_session_id"
                ).fetchall()
            ]
            recovered: list[_SessionRow] = []
            events: list[HarnessEvent] = []
            for current in rows:
                control = self._require_control(
                    conn,
                    current.summary.terminal_session_id,
                )
                if current.summary.state.is_terminal:
                    self._transition_control_terminal(
                        conn,
                        control,
                        terminal_at=current.summary.ended_at or _iso(now),
                    )
                    if current.terminal_event_id is None:
                        raise TerminalSessionConflictError(
                            "pending terminal projection lacks event id"
                        )
                    event = self._append_event(
                        self._terminal_draft(current, recovery=True)
                    )
                    updated = conn.execute(
                        """UPDATE terminal_sessions
                           SET projection_state = 'terminal_projected',
                               reconciliation_state = 'recovered',
                               reconciled_by_owner_id = ?,
                               reconciled_by_epoch = ?,
                               row_version = row_version + 1, updated_at = ?
                           WHERE terminal_session_id = ? AND row_version = ?
                             AND projection_state = 'terminal_pending'""",
                        (
                            owner_id,
                            epoch,
                            _iso(now),
                            current.summary.terminal_session_id,
                            current.row_version,
                        ),
                    )
                    if updated.rowcount != 1:
                        raise TerminalSessionConflictError(
                            "terminal reconciliation row version changed"
                        )
                    recovered.append(
                        self._require_row(
                            conn,
                            current.summary.terminal_session_id,
                        )
                    )
                    events.append(event)
                    continue
                recovery_event_id = deterministic_recovery_event_id(
                    current.summary.terminal_session_id,
                    current.owner_id,
                    current.summary.epoch,
                )
                updated = conn.execute(
                    """UPDATE terminal_sessions
                       SET state = 'UNCERTAIN', ended_at = ?, exit_code = NULL,
                           output_truncated = 1,
                           error_code = 'runtime_restart_process_unobserved',
                           uncertain_reason = 'runtime_restart_process_unobserved',
                           terminal_event_id = ?,
                           projection_state = 'terminal_projected',
                           reconciliation_state = 'recovered',
                           reconciled_by_owner_id = ?,
                           reconciled_by_epoch = ?,
                           row_version = row_version + 1, updated_at = ?
                       WHERE terminal_session_id = ? AND row_version = ?
                         AND state IN ('PENDING', 'RUNNING',
                                       'CANCEL_REQUESTED', 'KILL_REQUESTED')""",
                    (
                        _iso(now),
                        recovery_event_id,
                        owner_id,
                        epoch,
                        _iso(now),
                        current.summary.terminal_session_id,
                        current.row_version,
                    ),
                )
                if updated.rowcount != 1:
                    raise TerminalSessionConflictError(
                        "recovery row version changed"
                    )
                self._transition_control_terminal(
                    conn,
                    control,
                    terminal_at=_iso(now),
                )
                winner = self._require_row(
                    conn,
                    current.summary.terminal_session_id,
                )
                event = self._append_event(
                    self._terminal_draft(winner, recovery=True)
                )
                conn.execute(
                    """UPDATE terminal_session_stop_requests
                       SET outcome = 'uncertain',
                           error_code = 'runtime_restart_process_unobserved',
                           row_version = row_version + 1, updated_at = ?
                       WHERE terminal_session_id = ?""",
                    (_iso(now), current.summary.terminal_session_id),
                )
                recovered.append(winner)
                events.append(event)
            return tuple(recovered), tuple(events)

        recovered, events = self._storage._harness_operation_write(write)
        self._notify(events)
        return tuple(row.summary for row in recovered)


class TerminalSessionService:
    """Own one Runtime connection's durable PIPE_SESSION monitors.

    A launch action succeeds only after the process is observed RUNNING, its
    safe row and deterministic start event are committed, and a local monitor
    is registered.  Command completion remains a separate session lifecycle.
    """

    def __init__(
        self,
        *,
        manager: TerminalManager,
        store: TerminalSessionStore,
        world_id: str,
        owner_id: str,
        epoch: int,
        poll_interval_sec: float = 0.05,
        auto_recover: bool = True,
    ) -> None:
        if not isinstance(manager, TerminalManager):
            raise TypeError("manager must be a TerminalManager")
        if not isinstance(store, TerminalSessionStore):
            raise TypeError("store must be a TerminalSessionStore")
        if (
            type(poll_interval_sec) not in (int, float)
            or not math.isfinite(float(poll_interval_sec))
            or not 0.001 <= float(poll_interval_sec) <= 5.0
        ):
            raise TerminalSessionError(
                "poll_interval_sec must be a finite number in [0.001, 5]"
            )
        self._manager = manager
        self._store = store
        self._world_id = _identifier(world_id, "world_id")
        self._owner_id = _token(owner_id, "owner_id")
        self._epoch = _epoch(epoch)
        self._poll_interval_sec = float(poll_interval_sec)
        self._state_lock = threading.RLock()
        self._start_condition = threading.Condition(self._state_lock)
        self._session_locks: dict[str, _SessionLockEntry] = {}
        self._monitors: dict[str, threading.Thread] = {}
        self._monitor_errors: OrderedDict[str, str] = OrderedDict()
        self._shutdown = threading.Event()
        self._reconcile_shutdown = threading.Event()
        self._reconcile_wakeup = threading.Event()
        self._reconciler: threading.Thread | None = None
        self._active_starters = 0
        self._starter_threads: set[threading.Thread] = set()
        self._closing = False
        self._closed = False
        self._close_done = threading.Event()
        self._close_summary: TerminalSessionCloseSummary | None = None
        self._close_active_scopes: tuple[TerminalSessionSummary, ...] = ()
        self._close_request_threads: dict[str, threading.Thread] = {}
        self._close_settler_threads: dict[str, threading.Thread] = {}
        self._close_auxiliary_owners: tuple[threading.Thread, ...] = ()
        self._close_active_query_failed = False
        self._close_recovery_lock = threading.Lock()
        self._recovery_complete = False
        boot_epoch = int(time.time() - time.monotonic())
        self._host_boot_id = hashlib.sha256(
            f"harness-host-boot\x00{boot_epoch}".encode("utf-8")
        ).hexdigest()
        if auto_recover:
            self.recover_orphaned()

    @property
    def world_id(self) -> str:
        return self._world_id

    @property
    def owner_id(self) -> str:
        return self._owner_id

    @property
    def epoch(self) -> int:
        return self._epoch

    @contextmanager
    def _session_guard(self, terminal_session_id: str) -> Iterator[None]:
        with self._state_lock:
            entry = self._session_locks.get(terminal_session_id)
            if entry is None:
                entry = _SessionLockEntry(threading.RLock())
                self._session_locks[terminal_session_id] = entry
            entry.users += 1
        entry.lock.acquire()
        try:
            yield
        finally:
            entry.lock.release()
            with self._state_lock:
                entry.users -= 1
                self._release_session_entry_locked(
                    terminal_session_id,
                    entry,
                )

    def _release_session_entry_locked(
        self,
        terminal_session_id: str,
        entry: _SessionLockEntry | None = None,
    ) -> None:
        current = self._session_locks.get(terminal_session_id)
        if current is None or (entry is not None and current is not entry):
            return
        if current.users == 0 and terminal_session_id not in self._monitors:
            self._session_locks.pop(terminal_session_id, None)

    def _record_monitor_error(self, key: str, value: str) -> None:
        safe_value, _ = _clip_utf8_text(
            value or "unknown_monitor_error",
            maximum=_MAX_ERROR_BYTES,
        )
        with self._state_lock:
            self._monitor_errors.pop(key, None)
            self._monitor_errors[key] = safe_value
            while len(self._monitor_errors) > _MAX_MONITOR_ERRORS:
                self._monitor_errors.popitem(last=False)

    def _begin_start(self) -> None:
        with self._start_condition:
            if self._closing or self._closed:
                raise TerminalSessionError("terminal session service is closed")
            if not self._recovery_complete:
                raise TerminalSessionError("terminal session recovery is required")
            self._active_starters += 1
            self._starter_threads.add(threading.current_thread())

    def _end_start(self) -> None:
        with self._start_condition:
            self._active_starters -= 1
            self._starter_threads.discard(threading.current_thread())
            self._start_condition.notify_all()

    def _assert_start_admitted(self) -> None:
        with self._state_lock:
            if self._closing or self._closed:
                raise TerminalSessionError("terminal session service is closing")

    @staticmethod
    def _command_digest(spec: ProcessSpec) -> str:
        return hashlib.sha256(
            "\x00".join(spec.argv).encode("utf-8", errors="strict")
        ).hexdigest()

    def _evidence(self) -> tuple[str, str, TreeContainment]:
        metadata = self._manager.backend_metadata()
        if metadata.get("transport") != TerminalSessionTransport.PIPE.value:
            raise TerminalSessionError("background backend transport must be pipe")
        evidence_class = _bounded_text(
            metadata.get("evidence_class", LIVE_GATE_UNVERIFIED),
            "evidence_class",
            maximum=_MAX_EVIDENCE_BYTES,
        )
        sandbox_evidence = _bounded_text(
            metadata.get("sandbox_evidence", LIVE_GATE_UNVERIFIED),
            "sandbox_evidence",
            maximum=_MAX_EVIDENCE_BYTES,
        )
        try:
            tree = TreeContainment(
                metadata.get("tree_containment", TreeContainment.UNVERIFIED.value)
            )
        except ValueError:
            tree = TreeContainment.UNVERIFIED
        if evidence_class == LIVE_OS_RESTRICTED and not (
            tree is TreeContainment.JOB_OBJECT_VERIFIED
            and metadata.get("kill_on_owner_death_verified") is True
        ):
            # A sandboxed one-shot process is not yet a live durable session
            # if owner-death containment has not been independently proven.
            evidence_class = LIVE_GATE_UNVERIFIED
        return evidence_class, sandbox_evidence, tree

    @staticmethod
    def _private_digest(kind: str, *values: object) -> str:
        return hashlib.sha256(
            "\x00".join(
                ("harness.terminal-session-control.v1", kind, *(str(v) for v in values))
            ).encode("utf-8", errors="strict")
        ).hexdigest()

    def _attachment_facts(
        self,
        terminal_session_id: str,
        *,
        spawn_boundary_id: str,
        handle: ProcessHandle,
        evidence_class: str,
        sandbox_evidence: str,
        tree: TreeContainment,
    ) -> _AttachmentFacts:
        # These digests are private correlation facts, not public process
        # evidence and never a replacement for the independent live gates.
        opaque_backend_handle = self._private_digest(
            "manager_attachment",
            terminal_session_id,
            handle.request_id,
            handle.started_at,
            spawn_boundary_id,
        )
        containment_id = self._private_digest(
            "containment_attachment",
            terminal_session_id,
            spawn_boundary_id,
            tree.value,
            evidence_class,
            sandbox_evidence,
        )
        process_fingerprint = self._private_digest(
            "process_fingerprint",
            opaque_backend_handle,
            handle.command_digest,
            handle.started_at,
            self._host_boot_id,
        )
        return _AttachmentFacts(
            spawn_boundary_id=spawn_boundary_id,
            opaque_backend_handle=opaque_backend_handle,
            containment_id=containment_id,
            host_boot_id=self._host_boot_id,
            process_fingerprint=process_fingerprint,
        )

    def start_background(
        self,
        spec: ProcessSpec,
        *,
        engram_id: str,
        policy_context: Any = None,
        launch_action_digest: str | None = None,
    ) -> TerminalSessionSummary:
        if not isinstance(spec, ProcessSpec):
            raise TypeError("spec must be ProcessSpec")
        if spec.foreground:
            raise TerminalValidationError(
                "start_background requires ProcessSpec.foreground=False"
            )
        if spec.allow_stdin:
            raise TerminalValidationError(
                "PIPE_SESSION does not permit interactive stdin"
            )
        engram_id = _identifier(engram_id, "engram_id")
        if launch_action_digest is not None:
            launch_action_digest = _digest(
                launch_action_digest,
                "launch_action_digest",
            )
        self._begin_start()
        terminal_session_id = uuid.uuid4().hex
        try:
            evidence_class, sandbox_evidence, tree = self._evidence()
            self._store.reserve_start(
                terminal_session_id=terminal_session_id,
                turn_id=spec.turn_id,
                world_id=self._world_id,
                engram_id=engram_id,
                owner_id=self._owner_id,
                epoch=self._epoch,
                cwd_relative=spec.cwd.replace("\\", "/"),
                command_digest=self._command_digest(spec),
                launch_action_digest=launch_action_digest,
                evidence_class=evidence_class,
                sandbox_evidence=sandbox_evidence,
                tree_containment=tree,
            )
            spawn_boundary_id = self._private_digest(
                "spawn_boundary",
                terminal_session_id,
                uuid.uuid4().hex,
                self._owner_id,
                self._epoch,
            )
            try:
                self._assert_start_admitted()
                self._store.mark_spawn_requested(
                    terminal_session_id,
                    owner_id=self._owner_id,
                    epoch=self._epoch,
                    spawn_boundary_id=spawn_boundary_id,
                )
                self._assert_start_admitted()
            except TerminalSessionLeaseError:
                raise
            except Exception as exc:
                summary = self._contain_projection_failure(
                    terminal_session_id,
                    error_code="spawn_admission_closed",
                )
                raise TerminalSessionStartError(
                    summary,
                    "spawn_admission_closed",
                ) from exc

            try:
                handle = self._manager.spawn(
                    spec,
                    policy_context,
                    handle_id=terminal_session_id,
                )
            except Exception as exc:
                self._cancel_spawn_after_admission_loss(terminal_session_id)
                summary = self._contain_projection_failure(
                    terminal_session_id,
                    error_code="spawn_boundary_uncertain",
                )
                raise TerminalSessionStartError(
                    summary,
                    "spawn_boundary_uncertain",
                ) from exc

            try:
                self._assert_start_admitted()
            except Exception as exc:
                self._cancel_spawn_after_admission_loss(terminal_session_id)
                summary = self._contain_projection_failure(
                    terminal_session_id,
                    error_code="spawn_admission_closed",
                )
                raise TerminalSessionStartError(
                    summary,
                    "spawn_admission_closed",
                ) from exc

            if handle.state is not TerminalState.RUNNING:
                try:
                    result = self._manager.inspect(terminal_session_id)
                    summary = self._persist_result(terminal_session_id, result)
                except Exception as exc:
                    summary = self._contain_projection_failure(
                        terminal_session_id,
                        error_code="spawn_result_projection_failed",
                    )
                    raise TerminalSessionStartError(
                        summary,
                        summary.error_code or "spawn_result_projection_failed",
                    ) from exc
                raise TerminalSessionStartError(
                    summary,
                    summary.error_code or "background_process_not_running",
                )

            attachment = self._attachment_facts(
                terminal_session_id,
                spawn_boundary_id=spawn_boundary_id,
                handle=handle,
                evidence_class=evidence_class,
                sandbox_evidence=sandbox_evidence,
                tree=tree,
            )
            try:
                self._assert_start_admitted()
                self._store.commit_started(
                    terminal_session_id,
                    owner_id=self._owner_id,
                    epoch=self._epoch,
                    handle=handle,
                    attachment=attachment,
                )
                monitor = threading.Thread(
                    target=self._monitor,
                    args=(terminal_session_id,),
                    name=f"terminal-session-{terminal_session_id[:12]}",
                    daemon=True,
                )
                # Starting while holding the lifecycle lock ensures close()
                # can observe only a started thread.  The monitor's cleanup
                # also needs this lock, so it cannot finish before publish.
                with self._state_lock:
                    if self._closing or self._closed:
                        raise TerminalSessionError(
                            "terminal session service is closing"
                        )
                    monitor.start()
                    self._monitors[terminal_session_id] = monitor
            except Exception as exc:
                # The backend handle now certainly exists.  Cancel it before
                # any durable projection or lock-entry cleanup so a close that
                # won during spawn/attach cannot leave a late process alive.
                self._cancel_spawn_after_admission_loss(terminal_session_id)
                with self._state_lock:
                    self._monitors.pop(terminal_session_id, None)
                    self._release_session_entry_locked(terminal_session_id)
                summary = self._contain_projection_failure(
                    terminal_session_id,
                    error_code="start_control_projection_failed",
                )
                raise TerminalSessionStartError(
                    summary,
                    "start_control_projection_failed",
                ) from exc
            return self._store.inspect(terminal_session_id)
        finally:
            self._end_start()

    def _monitor(self, terminal_session_id: str) -> None:
        try:
            while not self._shutdown.wait(self._poll_interval_sec):
                if not self._poll_once(terminal_session_id):
                    return
        except TerminalSessionLeaseError as exc:
            self._cancel_local_without_durable_claim(terminal_session_id)
            self._record_monitor_error(terminal_session_id, str(exc))
        except Exception as exc:
            try:
                self._contain_projection_failure(
                    terminal_session_id,
                    error_code="session_monitor_failed",
                )
            except Exception:
                self._cancel_local_without_durable_claim(terminal_session_id)
            self._record_monitor_error(
                terminal_session_id,
                type(exc).__name__,
            )
        finally:
            with self._state_lock:
                self._monitors.pop(terminal_session_id, None)
                self._release_session_entry_locked(terminal_session_id)

    def _poll_once(self, terminal_session_id: str) -> bool:
        with self._session_guard(terminal_session_id):
            summary = self._store.inspect(terminal_session_id)
            if summary.state.is_terminal:
                return False
            try:
                result = self._manager.inspect(terminal_session_id)
            except Exception as exc:
                self._contain_projection_failure(
                    terminal_session_id,
                    error_code="process_observation_failed",
                )
                raise TerminalSessionError("process observation failed") from exc
            persisted = self._persist_result(terminal_session_id, result)
            return not persisted.state.is_terminal

    def _persist_result(
        self,
        terminal_session_id: str,
        result: ProcessResult,
    ) -> TerminalSessionSummary:
        if not isinstance(result, ProcessResult):
            raise TypeError("result must be ProcessResult")
        summary = self._store.inspect(terminal_session_id)
        try:
            for progress in result.output:
                if progress.seq <= summary.last_output_seq:
                    continue
                self._store.append_output(
                    terminal_session_id,
                    owner_id=self._owner_id,
                    epoch=self._epoch,
                    progress=progress,
                )
                summary = self._store.inspect(terminal_session_id)
            if result.state.is_terminal:
                summary = self._store.commit_terminal(
                    terminal_session_id,
                    owner_id=self._owner_id,
                    epoch=self._epoch,
                    state=result.state,
                    ended_at=result.handle.ended_at,
                    exit_code=result.exit_code,
                    error_code=result.handle.error_code,
                    uncertain_reason=result.uncertain_reason,
                    output_truncated=result.handle.output_truncated,
                )
                self._forget_manager_terminal(terminal_session_id)
            return summary
        except TerminalSessionLeaseError:
            self._cancel_local_without_durable_claim(terminal_session_id)
            raise
        except Exception:
            return self._contain_projection_failure(
                terminal_session_id,
                error_code="session_projection_failed",
            )

    def _cancel_local_without_durable_claim(self, terminal_session_id: str) -> None:
        try:
            self._manager.cancel(
                terminal_session_id,
                reason="runtime_lease_lost",
                deadline=0,
            )
        except Exception:
            pass

    def _cancel_spawn_after_admission_loss(
        self,
        terminal_session_id: str,
    ) -> ProcessResult | None:
        """Directly signal a deterministic handle created after close won."""

        try:
            return self._manager.cancel(
                terminal_session_id,
                reason="start_admission_closed",
                deadline=0,
            )
        except Exception:
            return None

    def _forget_manager_terminal(self, terminal_session_id: str) -> None:
        try:
            self._manager.forget(terminal_session_id)
        except (KeyError, TerminalValidationError):
            pass

    def _contain_projection_failure(
        self,
        terminal_session_id: str,
        *,
        error_code: str,
    ) -> TerminalSessionSummary:
        self._cancel_local_without_durable_claim(terminal_session_id)
        try:
            summary = self._store.mark_unprojected_uncertain(
                terminal_session_id,
                owner_id=self._owner_id,
                epoch=self._epoch,
                error_code=error_code,
            )
            self._forget_manager_terminal(terminal_session_id)
            self._reconcile_wakeup.set()
            return summary
        except TerminalSessionLeaseError:
            raise
        except Exception:
            # The original safe reservation remains durable and will be
            # terminalized by successor recovery.  Never synthesize a live
            # summary outside the store.
            return self._store.inspect(terminal_session_id)

    def list_for_turn(
        self,
        turn_id: str,
        *,
        engram_id: str,
        limit: int = 16,
    ) -> TerminalSessionList:
        return self._store.list_for_turn(
            world_id=self._world_id,
            engram_id=engram_id,
            turn_id=turn_id,
            limit=limit,
        )

    def inspect(
        self,
        terminal_session_id: str,
        *,
        expected_engram_id: str | None = None,
        expected_turn_id: str | None = None,
    ) -> TerminalSessionSummary:
        summary = self._store.inspect(terminal_session_id)
        if summary.world_id != self._world_id:
            raise TerminalSessionConflictError(
                "terminal session belongs to another world"
            )
        if (expected_engram_id is None) != (expected_turn_id is None):
            raise TerminalSessionConflictError(
                "terminal session inspection requires complete Engram and turn scope"
            )
        if expected_engram_id is not None and expected_turn_id is not None:
            expected_engram_id = _identifier(
                expected_engram_id, "expected_engram_id"
            )
            expected_turn_id = _identifier(expected_turn_id, "expected_turn_id")
            if (
                summary.engram_id != expected_engram_id
                or summary.turn_id != expected_turn_id
            ):
                raise TerminalSessionConflictError(
                    "terminal session belongs to another Engram or turn"
                )
        return summary

    def read_output(
        self,
        terminal_session_id: str,
        *,
        expected_engram_id: str | None = None,
        expected_turn_id: str | None = None,
        after_seq: int = 0,
        limit: int = 200,
    ) -> TerminalOutputPage:
        self.inspect(
            terminal_session_id,
            expected_engram_id=expected_engram_id,
            expected_turn_id=expected_turn_id,
        )
        return self._store.read_output(
            terminal_session_id,
            after_seq=after_seq,
            limit=limit,
        )

    def stop(
        self,
        terminal_session_id: str,
        *,
        request_id: str,
        expected_epoch: int,
        expected_engram_id: str,
        expected_turn_id: str,
        expected_state: TerminalState | str | None = None,
        reason: str = "user_stop",
        _control_deadline: float | None = None,
    ) -> TerminalStopResult:
        terminal_session_id = _token(terminal_session_id, "terminal_session_id")
        _bounded_text(reason, "reason", maximum=_MAX_ERROR_BYTES)
        with self._session_guard(terminal_session_id):
            try:
                claim = self._store.begin_stop(
                    terminal_session_id,
                    request_id=request_id,
                    owner_id=self._owner_id,
                    epoch=self._epoch,
                    expected_world_id=self._world_id,
                    expected_engram_id=expected_engram_id,
                    expected_turn_id=expected_turn_id,
                    expected_epoch=expected_epoch,
                    expected_state=expected_state,
                )
            except TerminalSessionLeaseError:
                self._cancel_local_without_durable_claim(terminal_session_id)
                raise
            if claim.row.summary.state.is_terminal:
                summary = claim.row.summary
                return TerminalStopResult(
                    summary=summary,
                    accepted=False,
                    idempotent=claim.idempotent,
                    uncertain=summary.state is TerminalState.UNCERTAIN,
                    error_code=summary.error_code,
                )
            try:
                result = self._manager.cancel(
                    terminal_session_id,
                    reason=reason,
                    deadline=_control_deadline,
                )
                summary = self._persist_result(terminal_session_id, result)
            except TerminalSessionLeaseError:
                raise
            except Exception:
                summary = self._contain_projection_failure(
                    terminal_session_id,
                    error_code="stop_control_failed",
                )
            return TerminalStopResult(
                summary=summary,
                accepted=True,
                idempotent=claim.idempotent,
                uncertain=summary.state is TerminalState.UNCERTAIN,
                error_code=summary.error_code,
            )

    @staticmethod
    def _control_request_id(
        kind: str,
        terminal_session_id: str,
        owner_id: str,
        epoch: int,
        reason: str,
    ) -> str:
        return _event_id(kind, terminal_session_id, owner_id, epoch, reason)

    def stop_turn(
        self,
        turn_id: str,
        *,
        engram_id: str,
        reason: str = "turn_interrupt",
    ) -> tuple[TerminalStopResult, ...]:
        turn_id = _identifier(turn_id, "turn_id")
        engram_id = _identifier(engram_id, "engram_id")
        results: list[TerminalStopResult] = []
        for summary in self._store.active_for_scope(
            world_id=self._world_id,
            engram_id=engram_id,
            turn_id=turn_id,
        ):
            results.append(
                self.stop(
                    summary.terminal_session_id,
                    request_id=self._control_request_id(
                        "stop_turn",
                        summary.terminal_session_id,
                        self._owner_id,
                        self._epoch,
                        reason,
                    ),
                    expected_epoch=summary.epoch,
                    expected_engram_id=summary.engram_id,
                    expected_turn_id=summary.turn_id,
                    reason=reason,
                )
            )
        return tuple(results)

    def stop_all(
        self,
        *,
        reason: str = "runtime_close",
    ) -> tuple[TerminalStopResult, ...]:
        results: list[TerminalStopResult] = []
        for summary in self._store.active_for_scope(world_id=self._world_id):
            results.append(
                self.stop(
                    summary.terminal_session_id,
                    request_id=self._control_request_id(
                        "stop_all",
                        summary.terminal_session_id,
                        self._owner_id,
                        self._epoch,
                        reason,
                    ),
                    expected_epoch=summary.epoch,
                    expected_engram_id=summary.engram_id,
                    expected_turn_id=summary.turn_id,
                    reason=reason,
                )
            )
        return tuple(results)

    def _start_reconciler_locked(self) -> None:
        if self._reconciler is not None:
            return
        if self._closing or self._closed:
            raise TerminalSessionError(
                "terminal projection reconciler cannot start while closing"
            )
        reconciler = threading.Thread(
            target=self._reconcile_loop,
            name=f"terminal-reconciler-{self._owner_id[:12]}",
            daemon=True,
        )
        # As with monitors, start before publish while holding _state_lock so
        # close() can never observe a Thread that has not been started.
        reconciler.start()
        self._reconciler = reconciler

    def _reconcile_batch(self) -> int:
        terminal_session_ids = self._store.pending_projection_ids(
            world_id=self._world_id,
            owner_id=self._owner_id,
            epoch=self._epoch,
            limit=_MAX_RECONCILIATION_BATCH,
        )
        reconciled = 0
        for terminal_session_id in terminal_session_ids:
            try:
                self._store.reconcile_pending(
                    terminal_session_id,
                    owner_id=self._owner_id,
                    epoch=self._epoch,
                )
                reconciled += 1
                with self._state_lock:
                    self._monitor_errors.pop(
                        f"reconcile:{terminal_session_id}",
                        None,
                    )
            except TerminalSessionLeaseError:
                raise
            except Exception as exc:
                self._record_monitor_error(
                    f"reconcile:{terminal_session_id}",
                    type(exc).__name__,
                )
        if len(terminal_session_ids) == _MAX_RECONCILIATION_BATCH:
            self._reconcile_wakeup.set()
        return reconciled

    def _reconcile_loop(self) -> None:
        interval = max(0.01, self._poll_interval_sec)
        while not self._reconcile_shutdown.is_set():
            self._reconcile_wakeup.wait(interval)
            self._reconcile_wakeup.clear()
            if self._reconcile_shutdown.is_set():
                return
            try:
                self._reconcile_batch()
            except TerminalSessionLeaseError as exc:
                self._record_monitor_error("reconciler", str(exc))
                return
            except Exception as exc:
                self._record_monitor_error(
                    "reconciler",
                    type(exc).__name__,
                )

    def _drain_pending_reconciliation(self) -> None:
        deadline = time.monotonic() + max(0.25, self._poll_interval_sec * 20)
        while time.monotonic() < deadline:
            pending = self._store.pending_projection_ids(
                world_id=self._world_id,
                owner_id=self._owner_id,
                epoch=self._epoch,
                limit=_MAX_RECONCILIATION_BATCH,
            )
            if not pending:
                return
            try:
                progressed = self._reconcile_batch()
            except TerminalSessionLeaseError:
                return
            if progressed == 0:
                time.sleep(min(0.01, self._poll_interval_sec))

    def recover_orphaned(self) -> tuple[TerminalSessionSummary, ...]:
        with self._state_lock:
            if self._recovery_complete:
                return ()
            if self._monitors or self._active_starters:
                raise TerminalSessionConflictError(
                    "recovery cannot run after local session activity starts"
                )
        recovered = self._store.recover_orphaned(
            owner_id=self._owner_id,
            epoch=self._epoch,
        )
        with self._state_lock:
            self._recovery_complete = True
            self._start_reconciler_locked()
        return recovered

    def close(
        self,
        *,
        deadline: float | None = None,
        recovery_permit: RuntimeRecoveryPermit | None = None,
    ) -> TerminalSessionCloseSummary:
        """Freeze admission, broadcast every stop, then observe one deadline."""

        if recovery_permit is not None:
            if not isinstance(recovery_permit, RuntimeRecoveryPermit):
                raise TypeError(
                    "recovery_permit must be a RuntimeRecoveryPermit or null"
                )
            if (
                recovery_permit.owner_id != self._owner_id
                or recovery_permit.epoch != self._epoch
            ):
                raise TerminalSessionLeaseError(
                    "terminal recovery permit belongs to another owner epoch"
                )
            recovery_permit.assert_recovery()
        close_deadline = self._resolve_close_deadline(deadline)
        with self._start_condition:
            if self._close_summary is not None:
                cached_summary = self._close_summary
                close_owner = None
                starters = ()
                monitors = ()
            else:
                cached_summary = None
                if self._closing or self._closed:
                    close_owner = False
                    starters = tuple(self._starter_threads)
                    monitors = tuple(self._monitors.values())
                else:
                    close_owner = True
                    self._closing = True
                    starters = tuple(self._starter_threads)
                    monitors = tuple(self._monitors.values())
                    self._shutdown.set()
                    self._reconcile_shutdown.set()
                    self._reconcile_wakeup.set()
                    self._start_condition.notify_all()
            reconciler = self._reconciler

        if cached_summary is not None:
            if recovery_permit is None:
                return cached_summary
            return self._retry_close_recovery(
                recovery_permit,
                deadline=close_deadline,
            )

        if not close_owner:
            self._close_done.wait(
                timeout=max(0.0, close_deadline - time.monotonic())
            )
            with self._start_condition:
                if self._close_summary is not None:
                    cached_summary = self._close_summary
                else:
                    cached_summary = None
                unresolved = sum(thread.is_alive() for thread in starters + monitors)
                if reconciler is not None and reconciler.is_alive():
                    unresolved += 1
            if cached_summary is not None:
                if recovery_permit is None:
                    return cached_summary
                return self._retry_close_recovery(
                    recovery_permit,
                    deadline=close_deadline,
                )
            return TerminalSessionCloseSummary(
                active_before=len(starters) + len(monitors),
                unresolved=max(1, unresolved),
                owner_joined=False,
                process_tree_state=TerminalSessionProcessTreeState.UNKNOWN,
                starters_before=len(starters),
                monitors_before=len(monitors),
            )

        try:
            active = self._store.active_for_scope(world_id=self._world_id)
        except Exception:
            active = ()
            active_query_failed = True
        else:
            active_query_failed = False
        with self._start_condition:
            self._close_active_scopes = tuple(active)
            self._close_active_query_failed = active_query_failed

        request_threads: dict[str, threading.Thread] = {}
        request_started: dict[str, threading.Event] = {}
        request_results: dict[str, TerminalStopResult] = {}
        process_results: dict[str, ProcessResult] = {}
        result_lock = threading.Lock()

        # Phase one: launch every cancellation owner before observing any one
        # session.  Each manager control uses a zero wait budget; a blocking
        # adapter remains an escaped owner instead of serializing the fleet.
        for summary in active:
            started = threading.Event()
            thread = threading.Thread(
                target=self._run_close_request,
                args=(
                    summary,
                    started,
                    request_results,
                    process_results,
                    result_lock,
                    recovery_permit,
                ),
                name=(
                    "terminal-stop-"
                    f"{summary.terminal_session_id[-12:]}"
                ),
                daemon=True,
            )
            request_started[summary.terminal_session_id] = started
            request_threads[summary.terminal_session_id] = thread
            thread.start()

        for started in request_started.values():
            started.wait(timeout=max(0.0, close_deadline - time.monotonic()))

        # Phase two: every session gets a concurrent terminal-winner owner.
        # A timeout winner is committed through the same CAS as ordinary
        # completion, so a late process owner can only replay that winner.
        settler_threads: dict[str, threading.Thread] = {}
        for summary in active:
            request_thread = request_threads[summary.terminal_session_id]
            settler = threading.Thread(
                target=self._settle_close_winner,
                args=(
                    summary,
                    request_thread,
                    close_deadline,
                    request_results,
                    process_results,
                    result_lock,
                    recovery_permit,
                ),
                name=(
                    "terminal-settle-"
                    f"{summary.terminal_session_id[-12:]}"
                ),
                daemon=True,
            )
            settler_threads[summary.terminal_session_id] = settler
            settler.start()

        observed_owners = (
            tuple(settler_threads.values())
            + tuple(request_threads.values())
            + monitors
            + starters
            + (() if reconciler is None else (reconciler,))
        )
        for owner in observed_owners:
            if owner is threading.current_thread():
                continue
            owner.join(timeout=max(0.0, close_deadline - time.monotonic()))

        results: list[TerminalStopResult] = []
        unresolved = 0
        for summary in active:
            terminal_session_id = summary.terminal_session_id
            with result_lock:
                result = request_results.get(terminal_session_id)
            if result is None:
                try:
                    winner = self._store.inspect(terminal_session_id)
                except Exception:
                    winner = summary
                if winner.state.is_terminal:
                    result = TerminalStopResult(
                        summary=winner,
                        accepted=True,
                        idempotent=False,
                        uncertain=winner.state is TerminalState.UNCERTAIN,
                        error_code=winner.error_code,
                    )
            if result is not None:
                results.append(result)
            owner_alive = (
                request_threads[terminal_session_id].is_alive()
                or settler_threads[terminal_session_id].is_alive()
            )
            if (
                owner_alive
                or result is None
                or result.uncertain
                or not result.summary.state.is_terminal
            ):
                unresolved += 1

        auxiliary_alive = sum(
            owner is not threading.current_thread() and owner.is_alive()
            for owner in monitors + starters + (() if reconciler is None else (reconciler,))
        )
        unresolved += auxiliary_alive
        if active_query_failed:
            unresolved += 1
        owner_joined = unresolved == 0
        if active_query_failed:
            process_tree = TerminalSessionProcessTreeState.UNKNOWN
        elif not active:
            process_tree = TerminalSessionProcessTreeState.NOT_APPLICABLE
        elif owner_joined:
            # TerminalManager observes root completion/cleanup only.  A
            # static JOB_OBJECT label is not ActiveProcesses==0 evidence.
            process_tree = TerminalSessionProcessTreeState.ROOT_EXIT_ONLY
        else:
            process_tree = TerminalSessionProcessTreeState.UNKNOWN

        close_summary = TerminalSessionCloseSummary(
            active_before=max(
                int(active_query_failed),
                len(active) + len(starters),
            ),
            unresolved=unresolved,
            owner_joined=owner_joined,
            process_tree_state=process_tree,
            cancellation_requested=sum(
                event.is_set() for event in request_started.values()
            ),
            terminal_observed=len(results),
            starters_before=len(starters),
            monitors_before=len(monitors),
            results=tuple(results),
        )
        with self._start_condition:
            self._close_request_threads = dict(request_threads)
            self._close_settler_threads = dict(settler_threads)
            self._close_auxiliary_owners = (
                starters
                + monitors
                + (() if reconciler is None else (reconciler,))
            )
            self._starter_threads = {
                thread for thread in self._starter_threads if thread.is_alive()
            }
            self._monitors = {
                key: thread
                for key, thread in self._monitors.items()
                if thread.is_alive()
            }
            if reconciler is not None and not reconciler.is_alive():
                self._reconciler = None
            if owner_joined:
                self._session_locks.clear()
                self._monitor_errors.clear()
            self._closed = True
            self._closing = False
            self._close_summary = close_summary
            self._close_done.set()
            self._start_condition.notify_all()
        return close_summary

    def _retry_close_recovery(
        self,
        recovery_permit: RuntimeRecoveryPermit,
        *,
        deadline: float,
    ) -> TerminalSessionCloseSummary:
        """Fill durable winners only; never signal or inspect a process."""

        with self._close_recovery_lock:
            with self._start_condition:
                cached = self._close_summary
                active = self._close_active_scopes
                request_threads = dict(self._close_request_threads)
                settler_threads = dict(self._close_settler_threads)
                auxiliary_owners = self._close_auxiliary_owners
                active_query_failed = self._close_active_query_failed
            if cached is None:
                raise RuntimeError("terminal close has not produced a summary")

            recovered: dict[str, TerminalStopResult] = {
                item.summary.terminal_session_id: item
                for item in cached.results
            }
            for original in active:
                if time.monotonic() >= deadline:
                    break
                terminal_session_id = original.terminal_session_id
                try:
                    current = self._store.inspect(terminal_session_id)
                    was_terminal = current.state.is_terminal
                    winner = self._store.commit_terminal(
                        terminal_session_id,
                        owner_id=self._owner_id,
                        epoch=self._epoch,
                        state=(
                            current.state
                            if was_terminal
                            else TerminalState.UNCERTAIN
                        ),
                        ended_at=(
                            current.ended_at if was_terminal else _iso()
                        ),
                        exit_code=(
                            current.exit_code if was_terminal else None
                        ),
                        error_code=(
                            current.error_code
                            if was_terminal
                            else "runtime_close_owner_unresolved"
                        ),
                        uncertain_reason=(
                            current.uncertain_reason
                            if was_terminal
                            else "runtime_close_owner_unresolved"
                        ),
                        output_truncated=(
                            current.output_truncated or not was_terminal
                        ),
                        recovery_permit=recovery_permit,
                    )
                    recovered[terminal_session_id] = TerminalStopResult(
                        summary=winner,
                        accepted=True,
                        idempotent=was_terminal,
                        uncertain=winner.state is TerminalState.UNCERTAIN,
                        error_code=winner.error_code,
                    )
                except Exception:
                    # A later retry with the same typed permit may continue;
                    # never replace durable evidence with an in-memory claim.
                    continue

            ordered_results = tuple(
                recovered[item.terminal_session_id]
                for item in active
                if item.terminal_session_id in recovered
            )
            unresolved = 0
            for original in active:
                terminal_session_id = original.terminal_session_id
                result = recovered.get(terminal_session_id)
                owner_alive = any(
                    owner is not None and owner.is_alive()
                    for owner in (
                        request_threads.get(terminal_session_id),
                        settler_threads.get(terminal_session_id),
                    )
                )
                if (
                    owner_alive
                    or result is None
                    or result.uncertain
                    or not result.summary.state.is_terminal
                ):
                    unresolved += 1
            unresolved += sum(owner.is_alive() for owner in auxiliary_owners)
            unresolved += int(active_query_failed)
            final_unresolved = min(cached.unresolved, unresolved)
            updated = TerminalSessionCloseSummary(
                active_before=cached.active_before,
                unresolved=final_unresolved,
                owner_joined=(
                    cached.owner_joined or final_unresolved == 0
                ),
                process_tree_state=cached.process_tree_state,
                cancellation_requested=cached.cancellation_requested,
                terminal_observed=max(
                    cached.terminal_observed,
                    sum(
                        item.summary.state.is_terminal
                        for item in ordered_results
                    ),
                ),
                starters_before=cached.starters_before,
                monitors_before=cached.monitors_before,
                results=ordered_results,
            )
            with self._start_condition:
                current_summary = self._close_summary
                if current_summary is not None and (
                    current_summary.unresolved < updated.unresolved
                    or (
                        current_summary.unresolved == updated.unresolved
                        and current_summary.terminal_observed
                        > updated.terminal_observed
                    )
                ):
                    return current_summary
                self._close_summary = updated
                return updated

    @staticmethod
    def _resolve_close_deadline(deadline: float | None) -> float:
        if deadline is None:
            return time.monotonic() + _DEFAULT_CLOSE_TIMEOUT_SEC
        if (
            type(deadline) not in (int, float)
            or not math.isfinite(float(deadline))
        ):
            raise TerminalSessionError(
                "deadline must be a finite monotonic timestamp"
            )
        return float(deadline)

    def _run_close_request(
        self,
        summary: TerminalSessionSummary,
        started: threading.Event,
        sink: dict[str, TerminalStopResult],
        process_sink: dict[str, ProcessResult],
        sink_lock: threading.Lock,
        recovery_permit: RuntimeRecoveryPermit | None,
    ) -> None:
        started.set()
        if recovery_permit is not None:
            # Ordinary publication is already revoked.  Signal the process
            # boundary directly; the phase-two owner will project exactly one
            # terminal winner under the typed recovery permit.
            try:
                process_result = self._manager.cancel(
                    summary.terminal_session_id,
                    reason="runtime_close",
                    deadline=0.0,
                )
            except Exception:
                return
            with sink_lock:
                process_sink[summary.terminal_session_id] = process_result
            return
        try:
            result = self.stop(
                summary.terminal_session_id,
                request_id=self._control_request_id(
                    "close",
                    summary.terminal_session_id,
                    self._owner_id,
                    self._epoch,
                    "runtime_close",
                ),
                expected_epoch=summary.epoch,
                expected_engram_id=summary.engram_id,
                expected_turn_id=summary.turn_id,
                reason="runtime_close",
                _control_deadline=0.0,
            )
        except Exception:
            try:
                winner = self._contain_projection_failure(
                    summary.terminal_session_id,
                    error_code="runtime_close_control_failed",
                )
                result = TerminalStopResult(
                    summary=winner,
                    accepted=True,
                    idempotent=False,
                    uncertain=winner.state is TerminalState.UNCERTAIN,
                    error_code=winner.error_code,
                )
            except Exception:
                return
        with sink_lock:
            sink[summary.terminal_session_id] = result

    def _settle_close_winner(
        self,
        summary: TerminalSessionSummary,
        request_thread: threading.Thread,
        deadline: float,
        sink: dict[str, TerminalStopResult],
        process_sink: dict[str, ProcessResult],
        sink_lock: threading.Lock,
        recovery_permit: RuntimeRecoveryPermit | None,
    ) -> None:
        if request_thread is not threading.current_thread():
            request_thread.join(timeout=max(0.0, deadline - time.monotonic()))
        with sink_lock:
            if summary.terminal_session_id in sink:
                return
            process_result = process_sink.get(summary.terminal_session_id)
        try:
            winner = self._store.inspect(summary.terminal_session_id)
            if not winner.state.is_terminal:
                observed_terminal = (
                    process_result is not None
                    and process_result.state.is_terminal
                )
                winner = self._store.commit_terminal(
                    summary.terminal_session_id,
                    owner_id=self._owner_id,
                    epoch=self._epoch,
                    state=(
                        process_result.state
                        if observed_terminal
                        else TerminalState.UNCERTAIN
                    ),
                    ended_at=(
                        process_result.handle.ended_at
                        if observed_terminal
                        else _iso()
                    ),
                    exit_code=(
                        process_result.exit_code
                        if observed_terminal
                        else None
                    ),
                    error_code=(
                        process_result.handle.error_code
                        if observed_terminal
                        else "runtime_close_owner_unresolved"
                    ),
                    uncertain_reason=(
                        process_result.uncertain_reason
                        if observed_terminal
                        else "runtime_close_owner_unresolved"
                    ),
                    output_truncated=(
                        True
                        if process_result is None
                        else (
                            process_result.handle.output_truncated
                            or bool(process_result.output)
                        )
                    ),
                    recovery_permit=recovery_permit,
                )
                self._forget_manager_terminal(summary.terminal_session_id)
            result = TerminalStopResult(
                summary=winner,
                accepted=True,
                idempotent=False,
                uncertain=winner.state is TerminalState.UNCERTAIN,
                error_code=winner.error_code,
            )
        except Exception:
            return
        with sink_lock:
            sink.setdefault(summary.terminal_session_id, result)

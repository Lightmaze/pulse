"""Durable fact source for one Harness adapter operation.

The operation ledger is intentionally smaller than the Harness event stream.
It stores only bounded identifiers, epochs and digests needed to decide what
may be recovered after a process restart.  Prompts, commands, file contents,
credentials and ordinary event payloads do not belong here.
"""

from __future__ import annotations

import re
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from pulse_system.substrate.storage.store import Storage

__all__ = [
    "HarnessOperation",
    "HarnessOperationError",
    "HarnessOperationLedger",
    "OperationCASMismatchError",
    "OperationPhase",
    "OperationRecoveryState",
    "OperationScopeCollisionError",
    "OperationTerminalState",
    "OperationTransitionError",
    "deterministic_terminal_event_id",
]


_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_MAX_RECOVERY_LIMIT = 500


def deterministic_terminal_event_id(operation_kind: str, operation_id: str) -> str:
    """Derive the sole canonical terminal projection id for an E0 operation."""

    kind, identifier = HarnessOperationLedger._key(operation_kind, operation_id)
    digest = hashlib.sha256(
        f"{kind}\x00{identifier}\x00terminal".encode("utf-8")
    ).hexdigest()
    return f"terminal_{digest}"


class OperationPhase(StrEnum):
    """Durable phase of an operation, ordered by adapter risk."""

    INTENT = "intent"
    ADMITTED = "admitted"
    APPROVAL_PENDING = "approval_pending"
    STARTING = "starting"
    BOUNDARY_ENTERED = "boundary_entered"
    ADAPTER_RETURNED = "adapter_returned"
    TERMINALIZING = "terminalizing"
    TERMINAL = "terminal"


class OperationTerminalState(StrEnum):
    """The only terminal outcomes that the recovery ledger can assert."""

    FAILED_NOT_STARTED = "FAILED_NOT_STARTED"
    CANCELLED_NOT_STARTED = "CANCELLED_NOT_STARTED"
    COMPLETED = "COMPLETED"
    UNCERTAIN = "UNCERTAIN"


class OperationRecoveryState(StrEnum):
    """Whether a terminal row still needs an event binding/recovery pass."""

    NONE = "none"
    REQUIRED = "required"
    CLEARED = "cleared"


class HarnessOperationError(ValueError):
    """Base class for fail-closed operation ledger errors."""


class OperationScopeCollisionError(HarnessOperationError):
    """The stable key was reused with a different immutable scope."""


class OperationCASMismatchError(HarnessOperationError):
    """The caller no longer owns the operation's admitted epoch."""


class OperationTransitionError(HarnessOperationError):
    """The requested phase or terminal outcome is not safe."""


def _token(value: Any, field_name: str, *, max_length: int = 128) -> str:
    if not isinstance(value, str):
        raise HarnessOperationError(f"{field_name} must be a bounded identifier")
    if value != value.strip() or not value or len(value) > max_length:
        raise HarnessOperationError(
            f"{field_name} must be a bounded identifier without surrounding whitespace"
        )
    if not _TOKEN_RE.fullmatch(value):
        raise HarnessOperationError(
            f"{field_name} must contain only safe identifier characters"
        )
    return value


def _epoch(value: Any) -> int:
    if type(value) is not int or value < 1:
        raise HarnessOperationError("epoch must be an integer >= 1")
    return value


def _phase(value: OperationPhase | str) -> OperationPhase:
    try:
        return value if isinstance(value, OperationPhase) else OperationPhase(value)
    except (TypeError, ValueError) as exc:
        raise HarnessOperationError("unsupported operation phase") from exc


def _terminal_state(value: OperationTerminalState | str) -> OperationTerminalState:
    try:
        return (
            value
            if isinstance(value, OperationTerminalState)
            else OperationTerminalState(value)
        )
    except (TypeError, ValueError) as exc:
        raise HarnessOperationError("unsupported terminal state") from exc


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise HarnessOperationError("stored operation timestamp is not timezone-aware")
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class HarnessOperation:
    """Safe snapshot of one durable operation row."""

    operation_kind: str
    operation_id: str
    world_id: str
    engram_id: str
    turn_id: str
    requested_epoch: int
    owner_id: str
    scope_digest: str
    effect_key: str
    phase: OperationPhase
    terminal_state: OperationTerminalState | None
    terminal_event_id: str | None
    recovery_owner_id: str | None
    recovery_epoch: int | None
    recovery_state: OperationRecoveryState
    created_at: datetime
    updated_at: datetime

    @property
    def is_terminal(self) -> bool:
        return self.phase is OperationPhase.TERMINAL

    @property
    def is_recoverable(self) -> bool:
        return not self.is_terminal or self.recovery_state is OperationRecoveryState.REQUIRED

    def to_dict(self) -> dict[str, Any]:
        """Return the bounded wire projection; no operation payload exists."""

        return {
            "operation_kind": self.operation_kind,
            "operation_id": self.operation_id,
            "world_id": self.world_id,
            "engram_id": self.engram_id,
            "turn_id": self.turn_id,
            "requested_epoch": self.requested_epoch,
            "owner_id": self.owner_id,
            "scope_digest": self.scope_digest,
            "effect_key": self.effect_key,
            "phase": self.phase.value,
            "terminal_state": self.terminal_state.value if self.terminal_state else None,
            "terminal_event_id": self.terminal_event_id,
            "recovery_owner_id": self.recovery_owner_id,
            "recovery_epoch": self.recovery_epoch,
            "recovery_state": self.recovery_state.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


_SELECT = """
    SELECT operation_kind, operation_id, world_id, engram_id, turn_id,
           requested_epoch, owner_id, scope_digest, effect_key, phase,
           terminal_state, terminal_event_id, recovery_owner_id,
           recovery_epoch, recovery_state,
           created_at, updated_at
    FROM harness_operations
"""

_PHASE_ORDER = {
    OperationPhase.INTENT: 0,
    OperationPhase.ADMITTED: 1,
    OperationPhase.APPROVAL_PENDING: 2,
    OperationPhase.STARTING: 3,
    OperationPhase.BOUNDARY_ENTERED: 4,
    OperationPhase.ADAPTER_RETURNED: 5,
    OperationPhase.TERMINALIZING: 6,
    OperationPhase.TERMINAL: 7,
}
_BOUNDARY_ORDER = _PHASE_ORDER[OperationPhase.BOUNDARY_ENTERED]


class HarnessOperationLedger:
    """SQLite-backed, bounded operation state machine.

    All mutation methods use Storage's lock plus ``BEGIN IMMEDIATE``.  The
    ledger owns no in-memory truth, so a new ledger over the same Storage
    hydrates from the durable row immediately.
    """

    def __init__(self, storage: Storage):
        if not isinstance(storage, Storage):
            raise TypeError("storage must be a Storage")
        self._storage = storage

    @staticmethod
    def _key(operation_kind: str, operation_id: str) -> tuple[str, str]:
        return (
            _token(operation_kind, "operation_kind", max_length=64),
            _token(operation_id, "operation_id"),
        )

    @classmethod
    def _immutable_scope(
        cls,
        *,
        world_id: str,
        engram_id: str,
        turn_id: str,
        requested_epoch: int,
        owner_id: str,
        scope_digest: str,
        effect_key: str,
    ) -> tuple[str, str, str, int, str, str, str]:
        return (
            _token(world_id, "world_id"),
            _token(engram_id, "engram_id"),
            _token(turn_id, "turn_id"),
            _epoch(requested_epoch),
            _token(owner_id, "owner_id"),
            _token(scope_digest, "scope_digest"),
            _token(effect_key, "effect_key"),
        )

    @staticmethod
    def _row_to_operation(row: tuple[Any, ...]) -> HarnessOperation:
        return HarnessOperation(
            operation_kind=str(row[0]),
            operation_id=str(row[1]),
            world_id=str(row[2]),
            engram_id=str(row[3]),
            turn_id=str(row[4]),
            requested_epoch=int(row[5]),
            owner_id=str(row[6]),
            scope_digest=str(row[7]),
            effect_key=str(row[8]),
            phase=OperationPhase(str(row[9])),
            terminal_state=(
                None if row[10] is None else OperationTerminalState(str(row[10]))
            ),
            terminal_event_id=None if row[11] is None else str(row[11]),
            recovery_owner_id=None if row[12] is None else str(row[12]),
            recovery_epoch=None if row[13] is None else int(row[13]),
            recovery_state=OperationRecoveryState(str(row[14])),
            created_at=_timestamp(str(row[15])),
            updated_at=_timestamp(str(row[16])),
        )

    @staticmethod
    def _fetch(conn, key: tuple[str, str]) -> HarnessOperation | None:
        row = conn.execute(
            _SELECT + " WHERE operation_kind = ? AND operation_id = ?",
            key,
        ).fetchone()
        return None if row is None else HarnessOperationLedger._row_to_operation(row)

    @staticmethod
    def _require_cas(
        current: HarnessOperation,
        *,
        expected_epoch: int,
        owner_id: str,
    ) -> None:
        epoch = _epoch(expected_epoch)
        owner = _token(owner_id, "owner_id")
        if current.requested_epoch != epoch or current.owner_id != owner:
            raise OperationCASMismatchError(
                "operation epoch/owner CAS failed; the caller is stale"
            )

    @staticmethod
    def _scope_matches(
        current: HarnessOperation,
        scope: tuple[str, str, str, int, str, str, str],
    ) -> bool:
        return (
            current.world_id,
            current.engram_id,
            current.turn_id,
            current.requested_epoch,
            current.owner_id,
            current.scope_digest,
            current.effect_key,
        ) == scope

    @staticmethod
    def _updated_row(conn, key: tuple[str, str]) -> HarnessOperation:
        row = HarnessOperationLedger._fetch(conn, key)
        if row is None:
            raise OperationTransitionError("operation disappeared during transition")
        return row

    @staticmethod
    def _terminal_update(
        conn,
        current: HarnessOperation,
        *,
        terminal_state: OperationTerminalState,
        terminal_event_id: str | None,
        recovery_owner_id: str | None = None,
        recovery_epoch: int | None = None,
        now: str,
    ) -> HarnessOperation:
        if terminal_event_id is not None:
            terminal_event_id = _token(terminal_event_id, "terminal_event_id")
        recovery_state = (
            OperationRecoveryState.CLEARED.value
            if terminal_event_id is not None
            else OperationRecoveryState.REQUIRED.value
        )
        key = (current.operation_kind, current.operation_id)
        updated = conn.execute(
            """UPDATE harness_operations
               SET phase = 'terminal', terminal_state = ?,
                   terminal_event_id = ?, recovery_owner_id = ?,
                   recovery_epoch = ?, recovery_state = ?, updated_at = ?
               WHERE operation_kind = ? AND operation_id = ?
                 AND phase <> 'terminal'""",
            (
                terminal_state.value,
                terminal_event_id,
                recovery_owner_id,
                recovery_epoch,
                recovery_state,
                now,
                key[0],
                key[1],
            ),
        )
        if updated.rowcount != 1:
            # A concurrent claimant can only win before this transaction.  A
            # missing row is a real invariant failure, never a success.
            winner = HarnessOperationLedger._fetch(conn, key)
            if winner is not None and winner.is_terminal:
                return winner
            raise OperationTransitionError("terminal claim was not durable")
        return HarnessOperationLedger._updated_row(conn, key)

    def admit(
        self,
        operation_kind: str,
        operation_id: str,
        *,
        world_id: str,
        engram_id: str,
        turn_id: str,
        requested_epoch: int,
        owner_id: str,
        scope_digest: str,
        effect_key: str,
    ) -> HarnessOperation:
        """Admit or replay one operation with an immutable scope."""

        key = self._key(operation_kind, operation_id)
        scope = self._immutable_scope(
            world_id=world_id,
            engram_id=engram_id,
            turn_id=turn_id,
            requested_epoch=requested_epoch,
            owner_id=owner_id,
            scope_digest=scope_digest,
            effect_key=effect_key,
        )

        def write(conn) -> HarnessOperation:
            existing = self._fetch(conn, key)
            if existing is not None:
                if self._scope_matches(existing, scope):
                    return existing
                raise OperationScopeCollisionError(
                    "operation key was reused with a different immutable scope"
                )
            now = _utc_now().isoformat()
            conn.execute(
                """INSERT INTO harness_operations (
                    operation_kind, operation_id, world_id, engram_id, turn_id,
                    requested_epoch, owner_id, scope_digest, effect_key, phase,
                    terminal_state, terminal_event_id, recovery_owner_id,
                    recovery_epoch, recovery_state,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'admitted',
                          NULL, NULL, NULL, NULL, 'none', ?, ?)""",
                (*key, *scope, now, now),
            )
            return self._updated_row(conn, key)

        return self._storage._harness_operation_write(write)

    def claim_recovery(
        self,
        operation_kind: str,
        operation_id: str,
        *,
        successor_owner_id: str,
        successor_epoch: int,
        expected_prior_owner_id: str,
        expected_prior_epoch: int,
    ) -> HarnessOperation:
        """Settle one orphan under a strictly newer Runtime lease.

        This is deliberately not a general owner transfer.  A successor may
        only terminalize a row left by the exact prior owner/epoch, and may
        never re-enter the external adapter boundary.  Pre-boundary rows are
        safely cancelled; any row at or beyond the boundary becomes
        ``UNCERTAIN``.  The original scope remains immutable for audit.
        """

        key = self._key(operation_kind, operation_id)
        successor_owner = _token(successor_owner_id, "successor_owner_id")
        successor = _epoch(successor_epoch)
        prior_owner = _token(expected_prior_owner_id, "expected_prior_owner_id")
        prior_epoch = _epoch(expected_prior_epoch)
        if successor_owner == prior_owner or successor <= prior_epoch:
            raise OperationCASMismatchError(
                "successor recovery requires a different owner and newer epoch"
            )

        def write(conn) -> HarnessOperation:
            current = self._fetch(conn, key)
            if current is None:
                raise OperationTransitionError("operation does not exist")
            if current.is_terminal:
                return current
            if (
                current.owner_id != prior_owner
                or current.requested_epoch != prior_epoch
            ):
                raise OperationCASMismatchError(
                    "operation prior owner/epoch CAS failed"
                )
            terminal_state = (
                OperationTerminalState.CANCELLED_NOT_STARTED
                if _PHASE_ORDER[current.phase] < _BOUNDARY_ORDER
                else OperationTerminalState.UNCERTAIN
            )
            return self._terminal_update(
                conn,
                current,
                terminal_state=terminal_state,
                terminal_event_id=None,
                recovery_owner_id=successor_owner,
                recovery_epoch=successor,
                now=_utc_now().isoformat(),
            )

        return self._storage._harness_operation_write(write)

    def get(self, operation_kind: str, operation_id: str) -> HarnessOperation | None:
        key = self._key(operation_kind, operation_id)
        return self._storage._harness_operation_read(lambda conn: self._fetch(conn, key))

    def transition(
        self,
        operation_kind: str,
        operation_id: str,
        *,
        phase: OperationPhase | str,
        expected_epoch: int,
        owner_id: str,
        terminal_state: OperationTerminalState | str | None = None,
        terminal_event_id: str | None = None,
    ) -> HarnessOperation:
        """CAS-transition an operation, or replay the terminal winner."""

        key = self._key(operation_kind, operation_id)
        target = _phase(phase)
        state = None if terminal_state is None else _terminal_state(terminal_state)
        if terminal_event_id is not None:
            terminal_event_id = _token(terminal_event_id, "terminal_event_id")

        def write(conn) -> HarnessOperation:
            current = self._fetch(conn, key)
            if current is None:
                raise OperationTransitionError("operation does not exist")
            if current.is_terminal:
                return current
            self._require_cas(
                current,
                expected_epoch=expected_epoch,
                owner_id=owner_id,
            )
            current_order = _PHASE_ORDER[current.phase]
            target_order = _PHASE_ORDER[target]
            if target_order < current_order:
                raise OperationTransitionError("operation phase cannot move backwards")
            if target is current.phase and target is not OperationPhase.TERMINAL:
                return current
            if (
                target is not OperationPhase.TERMINAL
                and target_order > _BOUNDARY_ORDER
                and current_order < _BOUNDARY_ORDER
            ):
                raise OperationTransitionError(
                    "mark_boundary must durably precede adapter_returned or terminalizing"
                )
            if target is OperationPhase.TERMINAL:
                if state is None:
                    raise OperationTransitionError("terminal phase requires terminal_state")
                return self._claim_terminal_row(
                    conn,
                    current,
                    terminal_state=state,
                    terminal_event_id=terminal_event_id,
                )
            if state is not None or terminal_event_id is not None:
                raise OperationTransitionError(
                    "terminal metadata is only valid for the terminal phase"
                )
            now = _utc_now().isoformat()
            conn.execute(
                """UPDATE harness_operations SET phase = ?, updated_at = ?
                   WHERE operation_kind = ? AND operation_id = ?
                     AND phase <> 'terminal'""",
                (target.value, now, key[0], key[1]),
            )
            return self._updated_row(conn, key)

        return self._storage._harness_operation_write(write)

    def mark_boundary(
        self,
        operation_kind: str,
        operation_id: str,
        *,
        expected_epoch: int,
        owner_id: str,
    ) -> HarnessOperation:
        """Durably record that the external adapter boundary was entered."""

        key = self._key(operation_kind, operation_id)

        def write(conn) -> HarnessOperation:
            current = self._fetch(conn, key)
            if current is None:
                raise OperationTransitionError("operation does not exist")
            if current.is_terminal:
                return current
            self._require_cas(
                current,
                expected_epoch=expected_epoch,
                owner_id=owner_id,
            )
            if _PHASE_ORDER[current.phase] >= _BOUNDARY_ORDER:
                return current
            now = _utc_now().isoformat()
            conn.execute(
                """UPDATE harness_operations SET phase = 'boundary_entered',
                       updated_at = ?
                   WHERE operation_kind = ? AND operation_id = ?
                     AND phase <> 'terminal'""",
                (now, key[0], key[1]),
            )
            return self._updated_row(conn, key)

        return self._storage._harness_operation_write(write)

    def _claim_terminal_row(
        self,
        conn,
        current: HarnessOperation,
        *,
        terminal_state: OperationTerminalState,
        terminal_event_id: str | None,
    ) -> HarnessOperation:
        current_order = _PHASE_ORDER[current.phase]
        if terminal_state in {
            OperationTerminalState.FAILED_NOT_STARTED,
            OperationTerminalState.CANCELLED_NOT_STARTED,
        } and current_order >= _BOUNDARY_ORDER:
            raise OperationTransitionError(
                "a post-boundary failure must be UNCERTAIN"
            )
        if terminal_state is OperationTerminalState.COMPLETED and current_order < _BOUNDARY_ORDER:
            raise OperationTransitionError(
                "COMPLETED requires a durable boundary_entered phase"
            )
        return self._terminal_update(
            conn,
            current,
            terminal_state=terminal_state,
            terminal_event_id=terminal_event_id,
            now=_utc_now().isoformat(),
        )

    def claim_terminal(
        self,
        operation_kind: str,
        operation_id: str,
        *,
        expected_epoch: int,
        owner_id: str,
        terminal_state: OperationTerminalState | str,
        terminal_event_id: str | None = None,
    ) -> HarnessOperation:
        """Claim exactly one terminal outcome; late callers replay its winner."""

        key = self._key(operation_kind, operation_id)
        state = _terminal_state(terminal_state)
        if terminal_event_id is not None:
            terminal_event_id = _token(terminal_event_id, "terminal_event_id")

        def write(conn) -> HarnessOperation:
            current = self._fetch(conn, key)
            if current is None:
                raise OperationTransitionError("operation does not exist")
            if current.is_terminal:
                return current
            self._require_cas(
                current,
                expected_epoch=expected_epoch,
                owner_id=owner_id,
            )
            return self._claim_terminal_row(
                conn,
                current,
                terminal_state=state,
                terminal_event_id=terminal_event_id,
            )

        return self._storage._harness_operation_write(write)

    def bind_terminal_event(
        self,
        operation_kind: str,
        operation_id: str,
        *,
        terminal_event_id: str,
        expected_epoch: int | None = None,
        owner_id: str | None = None,
    ) -> HarnessOperation:
        """Bind the event projection after terminal append has succeeded."""

        key = self._key(operation_kind, operation_id)
        event_id = _token(terminal_event_id, "terminal_event_id")
        if (expected_epoch is None) != (owner_id is None):
            raise HarnessOperationError(
                "expected_epoch and owner_id must be supplied together"
            )

        def write(conn) -> HarnessOperation:
            current = self._fetch(conn, key)
            if current is None:
                raise OperationTransitionError("operation does not exist")
            if not current.is_terminal:
                raise OperationTransitionError(
                    "terminal event cannot bind before terminal claim"
                )
            if current.terminal_event_id is not None:
                if current.terminal_event_id == event_id:
                    return current
                raise OperationScopeCollisionError(
                    "terminal event is already bound to a different event id"
                )
            if expected_epoch is not None and owner_id is not None:
                self._require_cas(
                    current,
                    expected_epoch=expected_epoch,
                    owner_id=owner_id,
                )
            now = _utc_now().isoformat()
            conn.execute(
                """UPDATE harness_operations
                   SET terminal_event_id = ?, recovery_state = 'cleared',
                       updated_at = ?
                   WHERE operation_kind = ? AND operation_id = ?
                     AND terminal_event_id IS NULL""",
                (event_id, now, key[0], key[1]),
            )
            return self._updated_row(conn, key)

        return self._storage._harness_operation_write(write)

    def list_recovery(
        self,
        *,
        operation_kind: str | None = None,
        limit: int = 100,
    ) -> list[HarnessOperation]:
        """List nonterminal rows and terminal rows needing event recovery."""

        if operation_kind is not None:
            operation_kind = _token(operation_kind, "operation_kind", max_length=64)
        if type(limit) is not int or not 1 <= limit <= _MAX_RECOVERY_LIMIT:
            raise HarnessOperationError(
                f"limit must be an integer in [1, {_MAX_RECOVERY_LIMIT}]"
            )

        def read(conn) -> list[HarnessOperation]:
            params: list[Any] = []
            where = "(phase <> 'terminal' OR recovery_state = 'required')"
            if operation_kind is not None:
                where += " AND operation_kind = ?"
                params.append(operation_kind)
            rows = conn.execute(
                _SELECT
                + " WHERE "
                + where
                + " ORDER BY updated_at, operation_kind, operation_id LIMIT ?",
                (*params, limit),
            ).fetchall()
            return [self._row_to_operation(row) for row in rows]

        return self._storage._harness_operation_read(read)

    def list_for_turn(
        self,
        turn_id: str,
        *,
        limit: int = 100,
    ) -> list[HarnessOperation]:
        """Return bounded operation facts for one Harness turn."""

        turn = _token(turn_id, "turn_id")
        if type(limit) is not int or not 1 <= limit <= _MAX_RECOVERY_LIMIT:
            raise HarnessOperationError(
                f"limit must be an integer in [1, {_MAX_RECOVERY_LIMIT}]"
            )

        def read(conn) -> list[HarnessOperation]:
            rows = conn.execute(
                _SELECT
                + " WHERE turn_id = ? ORDER BY created_at, operation_kind, "
                + "operation_id LIMIT ?",
                (turn, limit),
            ).fetchall()
            return [self._row_to_operation(row) for row in rows]

        return self._storage._harness_operation_read(read)

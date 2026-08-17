"""Same-owner publication revocation for one Runtime lifecycle.

The durable owner lease prevents a different process from writing with an old
epoch.  It does not stop a late worker in the *same* process while that lease is
still active.  This gate supplies the missing lifecycle capability: every
Runtime-owned transaction carries a permit that can be revoked immediately,
without waiting for SQLite lease release or for a Python worker to cooperate.

Recovery receives a distinct capability.  It can be accepted only by recovery
entry points and therefore cannot be reused to publish an assistant result,
propagation, succession or ordinary world mutation after revocation.
"""

from __future__ import annotations

import re
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import TypedDict

from .shutdown import ShutdownDeadline

_OWNER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_REASON = re.compile(r"^[a-z][a-z0-9._-]{0,63}$")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RuntimePublicationState(StrEnum):
    ACTIVE = "active"
    REVOKED = "revoked"


class RuntimePublicationError(RuntimeError):
    """A lifecycle permit cannot authorize the requested transaction."""

    def __init__(self, code: str, *, owner_id: str, epoch: int) -> None:
        super().__init__(code)
        self.code = code
        self.owner_id = owner_id
        self.epoch = epoch


@dataclass(frozen=True, slots=True)
class RuntimePublicationSnapshot:
    owner_id: str
    epoch: int
    generation: str
    state: RuntimePublicationState
    revoked_at: datetime | None
    reason: str | None
    active_publication_transactions: int
    active_bootstrap_transactions: int
    active_recovery_transactions: int

    def to_dict(self) -> dict[str, object]:
        return {
            "owner_id": self.owner_id,
            "epoch": self.epoch,
            "generation": self.generation,
            "state": self.state.value,
            "revoked_at": (
                None if self.revoked_at is None else self.revoked_at.isoformat()
            ),
            "reason": self.reason,
            "active_publication_transactions": (
                self.active_publication_transactions
            ),
            "active_bootstrap_transactions": self.active_bootstrap_transactions,
            "active_recovery_transactions": self.active_recovery_transactions,
        }


class RuntimePublicationDrainSummary(TypedDict):
    """Payload-free evidence for pre-revoke transaction owners."""

    active_before: int
    unresolved: int
    owner_joined: bool
    process_tree_state: str
    publication_transactions: int
    bootstrap_transactions: int


class RuntimePublicationWatchdogSummary(TypedDict):
    """Payload-free evidence for the deadline watchdog owner."""

    active_before: int
    unresolved: int
    owner_joined: bool
    process_tree_state: str


@dataclass(frozen=True, slots=True)
class RuntimePublicationPermit:
    owner_id: str
    epoch: int
    generation: str
    _gate: "RuntimePublicationGate"

    def assert_publication(self) -> None:
        self._gate._assert_publication(self)

    @contextmanager
    def transaction_guard(self):
        """Linearize one ordinary durable write with revoke."""

        with self._gate._publication_transaction_guard(self):
            yield


@dataclass(frozen=True, slots=True)
class RuntimeRecoveryPermit:
    owner_id: str
    epoch: int
    generation: str
    _gate: "RuntimePublicationGate"

    def assert_recovery(self) -> None:
        self._gate._assert_recovery(self)

    @contextmanager
    def transaction_guard(self):
        """Linearize one recovery write with the lifecycle gate."""

        with self._gate._recovery_transaction_guard(self):
            yield


@dataclass(frozen=True, slots=True)
class RuntimeBootstrapPermit:
    """Typed authority for takeover/startup recovery while the gate is active.

    It is intentionally not an ordinary publication permit.  Code that only
    accepts :class:`RuntimePublicationPermit` cannot reuse bootstrap recovery
    to enqueue or settle ordinary work.
    """

    owner_id: str
    epoch: int
    generation: str
    _gate: "RuntimePublicationGate"

    def assert_bootstrap(self) -> None:
        self._gate._assert_bootstrap(self)

    @contextmanager
    def transaction_guard(self):
        """Linearize one takeover-recovery write while ownership is active."""

        with self._gate._bootstrap_transaction_guard(self):
            yield


class RuntimePublicationGate:
    """Issue one publication generation and revoke it exactly once."""

    def __init__(self, owner_id: str, epoch: int) -> None:
        if not isinstance(owner_id, str) or _OWNER.fullmatch(owner_id) is None:
            raise ValueError("owner_id must be a bounded Runtime identifier")
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 1:
            raise ValueError("epoch must be an integer >= 1")
        self._owner_id = owner_id
        self._epoch = epoch
        self._generation = uuid.uuid4().hex
        self._lock = threading.RLock()
        self._drained = threading.Condition(self._lock)
        self._state = RuntimePublicationState.ACTIVE
        self._revoked_at: datetime | None = None
        self._reason: str | None = None
        self._recovery_permit: RuntimeRecoveryPermit | None = None
        self._active_publication_transactions = 0
        self._active_bootstrap_transactions = 0
        self._active_recovery_transactions = 0
        self._publication_transactions_at_revoke = 0
        self._bootstrap_transactions_at_revoke = 0
        self._watchdog_stop = threading.Event()
        self._watchdog_armed = threading.Event()
        self._watchdog_done = threading.Event()
        self._watchdog_thread: threading.Thread | None = None
        self._publication_permit = RuntimePublicationPermit(
            owner_id,
            epoch,
            self._generation,
            self,
        )
        self._bootstrap_permit = RuntimeBootstrapPermit(
            owner_id,
            epoch,
            self._generation,
            self,
        )

    @property
    def publication_permit(self) -> RuntimePublicationPermit:
        return self._publication_permit

    @property
    def bootstrap_permit(self) -> RuntimeBootstrapPermit:
        return self._bootstrap_permit

    def snapshot(self) -> RuntimePublicationSnapshot:
        with self._lock:
            return RuntimePublicationSnapshot(
                owner_id=self._owner_id,
                epoch=self._epoch,
                generation=self._generation,
                state=self._state,
                revoked_at=self._revoked_at,
                reason=self._reason,
                active_publication_transactions=(
                    self._active_publication_transactions
                ),
                active_bootstrap_transactions=(
                    self._active_bootstrap_transactions
                ),
                active_recovery_transactions=self._active_recovery_transactions,
            )

    def revoke(self, *, reason: str) -> RuntimeRecoveryPermit:
        if not isinstance(reason, str) or _REASON.fullmatch(reason) is None:
            raise ValueError("reason must be a bounded lowercase code")
        with self._lock:
            # Normal coordinator progress must retire the independently owned
            # deadline watcher instead of leaving it asleep until the original
            # deadline.  The watchdog also takes this path when it wins.
            self._watchdog_stop.set()
            if self._state is RuntimePublicationState.ACTIVE:
                # Preserve the owner census at the actual linearization point.
                # A fast transaction may return before the independently
                # observed drain task starts, but it still owned publication
                # authority when shutdown revoked the generation.
                self._publication_transactions_at_revoke = (
                    self._active_publication_transactions
                )
                self._bootstrap_transactions_at_revoke = (
                    self._active_bootstrap_transactions
                )
                self._state = RuntimePublicationState.REVOKED
                self._revoked_at = _utc_now()
                self._reason = reason
                self._recovery_permit = RuntimeRecoveryPermit(
                    self._owner_id,
                    self._epoch,
                    self._generation,
                    self,
                )
                self._drained.notify_all()
            assert self._recovery_permit is not None
            return self._recovery_permit

    def wait_for_publication_drain(self) -> RuntimePublicationDrainSummary:
        """Wait until every transaction admitted before revoke has returned.

        Revocation itself never waits for these owners.  The caller runs this
        method in an independently observed shutdown owner, so an uncooperative
        filesystem or SQLite call cannot hold the control-plane deadline.  A
        recovery transaction is observed by its owning recovery component and
        is deliberately not included here.
        """

        with self._drained:
            while self._state is RuntimePublicationState.ACTIVE:
                self._drained.wait()
            publication_before = self._publication_transactions_at_revoke
            bootstrap_before = self._bootstrap_transactions_at_revoke
            active_before = publication_before + bootstrap_before
            while (
                self._active_publication_transactions
                or self._active_bootstrap_transactions
            ):
                self._drained.wait()
            return {
                "active_before": active_before,
                "unresolved": 0,
                "owner_joined": True,
                "process_tree_state": "not_applicable",
                "publication_transactions": publication_before,
                "bootstrap_transactions": bootstrap_before,
            }

    def arm_deadline(
        self,
        deadline: ShutdownDeadline,
        *,
        reason: str = "shutdown_deadline",
    ) -> threading.Event:
        """Revoke even if the shutdown coordinator itself becomes blocked."""

        if not isinstance(deadline, ShutdownDeadline):
            raise ValueError("deadline must be a ShutdownDeadline")
        def watchdog() -> None:
            self._watchdog_armed.set()
            try:
                remaining = deadline.remaining_seconds()
                if not self._watchdog_stop.wait(timeout=remaining):
                    self.revoke(reason=reason)
            finally:
                self._watchdog_done.set()

        with self._lock:
            if self._watchdog_thread is None:
                thread = threading.Thread(
                    target=watchdog,
                    name=f"publication-fence-{self._epoch}",
                    daemon=True,
                )
                self._watchdog_thread = thread
                thread.start()
        return self._watchdog_armed

    def wait_for_watchdog_exit(self) -> RuntimePublicationWatchdogSummary:
        """Join the armed watchdog as an explicit shutdown owner."""

        with self._lock:
            thread = self._watchdog_thread
        if thread is None:
            return {
                "active_before": 0,
                "unresolved": 0,
                "owner_joined": True,
                "process_tree_state": "not_applicable",
            }
        self._watchdog_done.wait()
        if thread is not threading.current_thread():
            thread.join()
        unresolved = int(thread.is_alive() or not self._watchdog_done.is_set())
        return {
            "active_before": 1,
            "unresolved": unresolved,
            "owner_joined": unresolved == 0,
            "process_tree_state": "not_applicable",
        }

    def _assert_publication(self, permit: RuntimePublicationPermit) -> None:
        with self._lock:
            if (
                permit._gate is not self
                or permit.owner_id != self._owner_id
                or permit.epoch != self._epoch
                or permit.generation != self._generation
            ):
                raise RuntimePublicationError(
                    "publication_permit_mismatch",
                    owner_id=self._owner_id,
                    epoch=self._epoch,
                )
            if self._state is not RuntimePublicationState.ACTIVE:
                raise RuntimePublicationError(
                    "publication_revoked",
                    owner_id=self._owner_id,
                    epoch=self._epoch,
                )

    @contextmanager
    def _publication_transaction_guard(
        self,
        permit: RuntimePublicationPermit,
    ):
        """Admit one write atomically without letting it block revocation.

        The increment is the linearization point.  Revoke either wins first
        and rejects admission, or observes this transaction as an explicit
        pre-revoke owner.  The potentially blocking physical operation runs
        outside the gate lock and is accounted for by
        :meth:`wait_for_publication_drain`.
        """

        with self._drained:
            self._assert_publication(permit)
            self._active_publication_transactions += 1
        try:
            yield
        finally:
            with self._drained:
                self._active_publication_transactions -= 1
                self._drained.notify_all()

    def _assert_bootstrap(self, permit: RuntimeBootstrapPermit) -> None:
        with self._lock:
            if (
                permit._gate is not self
                or permit is not self._bootstrap_permit
                or permit.owner_id != self._owner_id
                or permit.epoch != self._epoch
                or permit.generation != self._generation
            ):
                raise RuntimePublicationError(
                    "bootstrap_permit_mismatch",
                    owner_id=self._owner_id,
                    epoch=self._epoch,
                )
            if self._state is not RuntimePublicationState.ACTIVE:
                raise RuntimePublicationError(
                    "bootstrap_permit_inactive",
                    owner_id=self._owner_id,
                    epoch=self._epoch,
                )

    @contextmanager
    def _bootstrap_transaction_guard(
        self,
        permit: RuntimeBootstrapPermit,
    ):
        with self._drained:
            self._assert_bootstrap(permit)
            self._active_bootstrap_transactions += 1
        try:
            yield
        finally:
            with self._drained:
                self._active_bootstrap_transactions -= 1
                self._drained.notify_all()

    def _assert_recovery(self, permit: RuntimeRecoveryPermit) -> None:
        with self._lock:
            if (
                permit._gate is not self
                or permit.owner_id != self._owner_id
                or permit.epoch != self._epoch
                or permit.generation != self._generation
            ):
                raise RuntimePublicationError(
                    "recovery_permit_mismatch",
                    owner_id=self._owner_id,
                    epoch=self._epoch,
                )
            if (
                self._state is not RuntimePublicationState.REVOKED
                or self._recovery_permit is not permit
            ):
                raise RuntimePublicationError(
                    "recovery_permit_inactive",
                    owner_id=self._owner_id,
                    epoch=self._epoch,
                )

    @contextmanager
    def _recovery_transaction_guard(
        self,
        permit: RuntimeRecoveryPermit,
    ):
        with self._drained:
            self._assert_recovery(permit)
            # Recovery is a later logical epoch within the same Runtime
            # generation.  It may be authorized immediately so shutdown can
            # signal owners without delay, but its durable effects must not
            # overtake writes that linearized before revoke.  If an admitted
            # physical operation never returns, this recovery owner remains
            # independently visible/unclean and Storage is retained.
            while (
                self._active_publication_transactions
                or self._active_bootstrap_transactions
            ):
                self._drained.wait()
                self._assert_recovery(permit)
            self._active_recovery_transactions += 1
        try:
            yield
        finally:
            with self._drained:
                self._active_recovery_transactions -= 1
                self._drained.notify_all()


__all__ = [
    "RuntimePublicationError",
    "RuntimeBootstrapPermit",
    "RuntimePublicationGate",
    "RuntimePublicationPermit",
    "RuntimePublicationDrainSummary",
    "RuntimePublicationSnapshot",
    "RuntimePublicationState",
    "RuntimeRecoveryPermit",
]

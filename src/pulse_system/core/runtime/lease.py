"""Cross-process ownership heartbeat for one durable PulseWorld.

The SQLite transaction methods live in :mod:`substrate.storage`; this module
owns only the process resource that renews an acquired fencing epoch while the
Runtime may be blocked inside a Harness turn.
"""

from __future__ import annotations

import math
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Protocol

from pulse_system.core.types import (
    RuntimeLease,
    RuntimeLeaseError,
    RuntimeLeaseLostError,
    RuntimeLeaseState,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class LeaseStorage(Protocol):
    def acquire_runtime_lease(
        self,
        owner_id: str,
        *,
        now: datetime,
        ttl_sec: float,
    ) -> RuntimeLease: ...

    def renew_runtime_lease(
        self,
        owner_id: str,
        epoch: int,
        *,
        now: datetime,
        ttl_sec: float,
    ) -> RuntimeLease: ...

    def assert_runtime_lease(
        self,
        owner_id: str,
        epoch: int,
        *,
        now: datetime,
    ) -> RuntimeLease: ...

    def release_runtime_lease(
        self,
        owner_id: str,
        epoch: int,
        *,
        now: datetime,
    ) -> RuntimeLease: ...


@dataclass(frozen=True)
class RuntimeLeaseHealth:
    lease: RuntimeLease
    healthy: bool
    lost_reason: str | None = None


class RuntimeLeaseKeeper:
    """Acquire one lease and renew it independently of the async tick loop."""

    def __init__(
        self,
        storage: LeaseStorage,
        *,
        ttl_sec: float,
        renew_interval_sec: float,
        owner_id: str | None = None,
        on_lost: Callable[[RuntimeLeaseError], None] | None = None,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        if (
            isinstance(ttl_sec, bool)
            or not isinstance(ttl_sec, (int, float))
            or not math.isfinite(float(ttl_sec))
            or not 5.0 <= float(ttl_sec) <= 3600.0
        ):
            raise ValueError("ttl_sec must be a finite number between 5 and 3600")
        if (
            isinstance(renew_interval_sec, bool)
            or not isinstance(renew_interval_sec, (int, float))
            or not math.isfinite(float(renew_interval_sec))
            or not 1.0 <= float(renew_interval_sec) <= float(ttl_sec) / 2.0
        ):
            raise ValueError(
                "renew_interval_sec must be a finite number between 1 and "
                "half the lease TTL"
            )
        self._storage = storage
        self._ttl_sec = float(ttl_sec)
        self._renew_interval_sec = float(renew_interval_sec)
        self._owner_id = owner_id or f"runtime-{uuid.uuid4().hex}"
        self._on_lost = on_lost
        self._clock = clock
        self._lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._stop = threading.Event()
        self._closed = False
        self._lost_error: RuntimeLeaseError | None = None
        self._lease = storage.acquire_runtime_lease(
            self._owner_id,
            now=clock(),
            ttl_sec=self._ttl_sec,
        )
        self._thread = threading.Thread(
            target=self._run,
            name=f"pulse-lease-{self._lease.epoch}",
            daemon=True,
        )
        try:
            self._thread.start()
        except BaseException:
            try:
                storage.release_runtime_lease(
                    self._owner_id,
                    self._lease.epoch,
                    now=clock(),
                )
            except Exception:
                pass
            raise

    @property
    def owner_id(self) -> str:
        return self._owner_id

    @property
    def epoch(self) -> int:
        with self._lock:
            return self._lease.epoch

    def health(self) -> RuntimeLeaseHealth:
        with self._lock:
            return RuntimeLeaseHealth(
                lease=self._lease,
                healthy=self._lost_error is None and not self._closed,
                lost_reason=(
                    None if self._lost_error is None else self._lost_error.reason
                ),
            )

    def assert_owned(self) -> RuntimeLease:
        """Fence a foreground mutation against the current owner epoch."""

        with self._lock:
            lost = self._lost_error
            lease = self._lease
            closed = self._closed
        if lost is not None:
            raise lost
        if closed:
            raise RuntimeLeaseLostError(
                owner_id=self._owner_id,
                epoch=lease.epoch,
                reason="keeper_closed",
                lease=lease,
            )
        try:
            current = self._storage.assert_runtime_lease(
                self._owner_id,
                lease.epoch,
                now=self._clock(),
            )
        except RuntimeLeaseError as exc:
            self._mark_lost(exc)
            raise
        with self._lock:
            self._lease = current
        return current

    def close(self, *, release: bool = True) -> RuntimeLease:
        """Stop the heartbeat and, on a healthy normal close, release ownership.

        Idempotent calls return the last known row. A lost owner is never
        allowed to release the replacement owner's epoch.
        """

        with self._close_lock:
            self._stop.set()
            if threading.current_thread() is not self._thread:
                self._thread.join(
                    timeout=max(2.0, self._renew_interval_sec + 1.0)
                )
            with self._lock:
                self._closed = True
                lost = self._lost_error
                lease = self._lease
            if self._thread.is_alive():
                raise RuntimeError("Runtime lease heartbeat did not stop")
            # A bounded startup cleanup may first stop renewal without
            # releasing while another physical owner is unresolved.  A later
            # single-flight finalizer must still be able to release the same
            # healthy owner/epoch once that owner joins.
            if (
                release
                and lost is None
                and lease.state is not RuntimeLeaseState.RELEASED
            ):
                released = self._storage.release_runtime_lease(
                    self._owner_id,
                    lease.epoch,
                    now=self._clock(),
                )
                with self._lock:
                    self._lease = released
                return released
            return lease

    def _run(self) -> None:
        while not self._stop.wait(self._renew_interval_sec):
            with self._lock:
                if self._closed or self._lost_error is not None:
                    return
                epoch = self._lease.epoch
            try:
                renewed = self._storage.renew_runtime_lease(
                    self._owner_id,
                    epoch,
                    now=self._clock(),
                    ttl_sec=self._ttl_sec,
                )
            except RuntimeLeaseError as exc:
                self._mark_lost(exc)
                return
            except Exception as exc:  # fail closed on an unreadable local DB
                self._mark_lost(RuntimeLeaseLostError(
                    owner_id=self._owner_id,
                    epoch=epoch,
                    reason=f"renew_failed:{type(exc).__name__}",
                    lease=self.health().lease,
                ))
                return
            with self._lock:
                self._lease = renewed

    def _mark_lost(self, error: RuntimeLeaseError) -> None:
        callback: Callable[[RuntimeLeaseError], None] | None = None
        with self._lock:
            if self._lost_error is not None or self._closed:
                return
            self._lost_error = error
            callback = self._on_lost
        self._stop.set()
        if callback is not None:
            try:
                callback(error)
            except Exception:
                # Ownership is already fail-closed. An observability callback
                # must not resurrect the keeper or hide the original loss.
                pass

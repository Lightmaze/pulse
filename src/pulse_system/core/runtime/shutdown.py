"""A non-extendable shutdown budget and orthogonal shutdown evidence.

Shutdown has three independent questions:

* did the effect settle, remain uncertain, or never start;
* did the Python owner join or escape;
* did an external process tree become provably empty.

Collapsing those axes makes ``cancel signalled`` look like ``process stopped``.
This contract keeps them separate and also distinguishes logical publication
revocation from the cross-process Runtime owner lease.
"""

from __future__ import annotations

import math
import re
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

SHUTDOWN_PROTOCOL_VERSION = "runtime-shutdown.v1"
MIN_SHUTDOWN_TIMEOUT_SECONDS = 0.05
MAX_SHUTDOWN_TIMEOUT_SECONDS = 300.0
MAX_REPORTED_ELAPSED_SECONDS = 86_400.0
MAX_SHUTDOWN_COMPONENTS = 64

_TOKEN = re.compile(r"^[a-z][a-z0-9._-]{0,63}$")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _finite_seconds(
    value: object,
    field_name: str,
    *,
    minimum: float = 0.0,
    maximum: float = MAX_REPORTED_ELAPSED_SECONDS,
) -> float:
    if type(value) not in {int, float}:
        raise ValueError(
            f"{field_name} must be a finite number between {minimum} and {maximum}"
        )
    seconds = float(value)
    if not math.isfinite(seconds) or not minimum <= seconds <= maximum:
        raise ValueError(
            f"{field_name} must be a finite number between {minimum} and {maximum}"
        )
    return seconds


def _aware_utc(value: object, field_name: str) -> datetime:
    if (
        type(value) is not datetime
        or value.tzinfo is None
        or type(value.tzinfo) is not timezone
    ):
        raise ValueError(
            f"{field_name} must be an exact datetime with fixed-offset timezone"
        )
    if value.tzinfo is timezone.utc:
        return value
    return value.astimezone(timezone.utc)


def _token(value: object, field_name: str) -> str:
    if type(value) is not str or _TOKEN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must match {_TOKEN.pattern!r}")
    return value


def _optional_text(value: object, field_name: str, *, maximum: int) -> str | None:
    if value is None:
        return None
    if type(value) is not str or not value or len(value) > maximum:
        raise ValueError(f"{field_name} must be null or a string of 1..{maximum} chars")
    if "\n" in value or "\r" in value:
        raise ValueError(f"{field_name} must be a single line")
    return value


class ShutdownPhase(StrEnum):
    OPEN = "open"
    FREEZING = "freezing"
    SETTLING = "settling"
    FENCING = "fencing"
    FENCED = "fenced"
    CLEANING = "cleaning"
    CLOSED = "closed"


_PHASE_ORDER = {phase: index for index, phase in enumerate(ShutdownPhase)}


class RuntimeShutdownTrigger(StrEnum):
    """The bounded reasons that may start one Runtime shutdown flight."""

    CLOSE = "close"
    STARTUP_FAILURE = "startup_failure"
    LEASE_LOST = "lease_lost"


class ShutdownEffectState(StrEnum):
    NOT_STARTED = "not_started"
    SETTLED = "settled"
    UNCERTAIN = "uncertain"


class ShutdownOwnerState(StrEnum):
    JOINED = "joined"
    ESCAPED = "escaped"
    UNKNOWN = "unknown"


class ShutdownProcessTreeState(StrEnum):
    NOT_APPLICABLE = "not_applicable"
    EMPTY_VERIFIED = "empty_verified"
    ROOT_EXIT_ONLY = "root_exit_only"
    UNKNOWN = "unknown"
    ESCAPED = "escaped"


class ShutdownCancelState(StrEnum):
    NOT_NEEDED = "not_needed"
    SIGNALLED = "signalled"
    FAILED = "failed"
    UNKNOWN = "unknown"


class ShutdownDurableRecoveryState(StrEnum):
    NOT_ATTEMPTED = "not_attempted"
    NOT_NEEDED = "not_needed"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMED_OUT = "timed_out"


class ShutdownPublicationFenceState(StrEnum):
    NOT_ATTEMPTED = "not_attempted"
    ACTIVE = "active"
    REVOKED = "revoked"
    FAILED = "failed"


class ShutdownOwnerLeaseState(StrEnum):
    NOT_ATTEMPTED = "not_attempted"
    RELEASED = "released"
    LOST = "lost"
    RELEASE_PENDING = "release_pending"
    FAILED = "failed"


class ShutdownStorageState(StrEnum):
    OPEN = "open"
    CLOSED = "closed"
    RETAINED_FOR_ESCAPED_WORKERS = "retained_for_escaped_workers"
    CLOSE_PENDING = "close_pending"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ShutdownDeadline:
    """One monotonic deadline that child operations cannot extend."""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        del kwargs
        raise TypeError("ShutdownDeadline is a final canonical type")

    timeout_seconds: float
    started_monotonic: float
    deadline_monotonic: float
    _monotonic: Callable[[], float] = field(repr=False, compare=False)

    @classmethod
    def after(
        cls,
        timeout_seconds: float,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> "ShutdownDeadline":
        timeout = _finite_seconds(
            timeout_seconds,
            "timeout_seconds",
            minimum=MIN_SHUTDOWN_TIMEOUT_SECONDS,
            maximum=MAX_SHUTDOWN_TIMEOUT_SECONDS,
        )
        if not callable(monotonic):
            raise ValueError("monotonic must be callable")
        started = float(monotonic())
        if not math.isfinite(started):
            raise ValueError("monotonic clock must return a finite number")
        return cls(timeout, started, started + timeout, monotonic)

    def __post_init__(self) -> None:
        timeout = _finite_seconds(
            self.timeout_seconds,
            "timeout_seconds",
            minimum=MIN_SHUTDOWN_TIMEOUT_SECONDS,
            maximum=MAX_SHUTDOWN_TIMEOUT_SECONDS,
        )
        object.__setattr__(self, "timeout_seconds", timeout)
        if not callable(self._monotonic):
            raise ValueError("monotonic must be callable")
        canonical_clock_values: list[float] = []
        for value, field_name in (
            (self.started_monotonic, "started_monotonic"),
            (self.deadline_monotonic, "deadline_monotonic"),
        ):
            if type(value) not in {int, float} or not math.isfinite(float(value)):
                raise ValueError(f"{field_name} must be finite")
            canonical_clock_values.append(float(value))
        started_monotonic, deadline_monotonic = canonical_clock_values
        object.__setattr__(self, "started_monotonic", started_monotonic)
        object.__setattr__(self, "deadline_monotonic", deadline_monotonic)
        if not math.isclose(
            deadline_monotonic,
            started_monotonic + timeout,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise ValueError("deadline_monotonic must equal start plus timeout")

    def sample(self) -> tuple[float, float, bool]:
        """Read the monotonic clock once for a coherent cutoff decision."""

        now = float(self._monotonic())
        if not math.isfinite(now):
            return self.timeout_seconds, 0.0, True
        elapsed = min(
            max(0.0, now - self.started_monotonic),
            MAX_REPORTED_ELAPSED_SECONDS,
        )
        remaining = max(0.0, self.deadline_monotonic - now)
        return elapsed, remaining, remaining <= 0.0

    def remaining_seconds(self) -> float:
        return self.sample()[1]

    def elapsed_seconds(self) -> float:
        return self.sample()[0]

    @property
    def expired(self) -> bool:
        return self.sample()[2]

    def bounded_timeout(self, requested_seconds: float | None = None) -> float:
        remaining = self.remaining_seconds()
        if requested_seconds is None:
            return remaining
        requested = _finite_seconds(
            requested_seconds,
            "requested_seconds",
            minimum=0.0,
            maximum=MAX_SHUTDOWN_TIMEOUT_SECONDS,
        )
        return min(remaining, requested)


@dataclass(frozen=True, slots=True)
class ShutdownComponentReport:
    """One component's effect, owner and process-tree evidence."""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        del kwargs
        raise TypeError("ShutdownComponentReport is a final canonical type")

    component: str
    effect: ShutdownEffectState
    owner: ShutdownOwnerState
    process_tree: ShutdownProcessTreeState
    cancel: ShutdownCancelState
    started_at: datetime
    finished_at: datetime
    elapsed_seconds: float
    active_before: int = 0
    unresolved: int = 0
    error_code: str | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "component", _token(self.component, "component"))
        for value, enum_type, field_name in (
            (self.effect, ShutdownEffectState, "effect"),
            (self.owner, ShutdownOwnerState, "owner"),
            (self.process_tree, ShutdownProcessTreeState, "process_tree"),
            (self.cancel, ShutdownCancelState, "cancel"),
        ):
            if not isinstance(value, enum_type):
                raise ValueError(f"{field_name} has an invalid state")
        started = _aware_utc(self.started_at, "started_at")
        finished = _aware_utc(self.finished_at, "finished_at")
        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "finished_at", finished)
        if finished < started:
            raise ValueError("finished_at cannot precede started_at")
        object.__setattr__(
            self,
            "elapsed_seconds",
            _finite_seconds(self.elapsed_seconds, "elapsed_seconds"),
        )
        for value, field_name in (
            (self.active_before, "active_before"),
            (self.unresolved, "unresolved"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{field_name} must be a non-negative integer")
        if self.owner is ShutdownOwnerState.ESCAPED and self.unresolved < 1:
            raise ValueError("an escaped owner must report unresolved work")
        if self.process_tree is ShutdownProcessTreeState.ESCAPED and self.unresolved < 1:
            raise ValueError("an escaped process tree must report unresolved work")
        object.__setattr__(
            self,
            "error_code",
            None if self.error_code is None else _token(self.error_code, "error_code"),
        )
        object.__setattr__(
            self,
            "detail",
            _optional_text(self.detail, "detail", maximum=256),
        )

    @property
    def escaped(self) -> bool:
        return (
            self.owner is ShutdownOwnerState.ESCAPED
            or self.process_tree is ShutdownProcessTreeState.ESCAPED
        )

    @property
    def physical_exit_proven(self) -> bool:
        return (
            self.owner is ShutdownOwnerState.JOINED
            and self.process_tree
            in {
                ShutdownProcessTreeState.NOT_APPLICABLE,
                ShutdownProcessTreeState.EMPTY_VERIFIED,
            }
        )

    @property
    def clean(self) -> bool:
        return (
            self.effect is not ShutdownEffectState.UNCERTAIN
            and self.physical_exit_proven
            and self.unresolved == 0
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "component": self.component,
            "effect": self.effect.value,
            "owner": self.owner.value,
            "process_tree": self.process_tree.value,
            "cancel": self.cancel.value,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "elapsed_seconds": self.elapsed_seconds,
            "active_before": self.active_before,
            "unresolved": self.unresolved,
            "escaped": self.escaped,
            "physical_exit_proven": self.physical_exit_proven,
            "clean": self.clean,
            "error_code": self.error_code,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class RuntimeShutdownReport:
    shutdown_id: str
    phase: ShutdownPhase
    started_at: datetime
    finished_at: datetime
    timeout_seconds: float
    elapsed_seconds: float
    deadline_exhausted: bool
    admission_frozen: bool
    durable_recovery: ShutdownDurableRecoveryState
    publication_fence: ShutdownPublicationFenceState
    owner_lease: ShutdownOwnerLeaseState
    control_plane_closed: bool
    contract_satisfied: bool
    clean: bool
    physical_exit_proven: bool
    escaped_count: int
    storage_state: ShutdownStorageState
    components: tuple[ShutdownComponentReport, ...] = ()
    protocol_version: str = SHUTDOWN_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        if (
            type(self.protocol_version) is not str
            or self.protocol_version != SHUTDOWN_PROTOCOL_VERSION
        ):
            raise ValueError(
                f"protocol_version must be {SHUTDOWN_PROTOCOL_VERSION!r}"
            )
        if (
            type(self.shutdown_id) is not str
            or re.fullmatch(r"[0-9a-f]{32}", self.shutdown_id) is None
        ):
            raise ValueError("shutdown_id must be a 32-character lowercase hex id")
        if self.phase is not ShutdownPhase.CLOSED:
            raise ValueError("a terminal RuntimeShutdownReport must be closed")
        started = _aware_utc(self.started_at, "started_at")
        finished = _aware_utc(self.finished_at, "finished_at")
        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "finished_at", finished)
        if finished < started:
            raise ValueError("finished_at cannot precede started_at")
        object.__setattr__(
            self,
            "timeout_seconds",
            _finite_seconds(
                self.timeout_seconds,
                "timeout_seconds",
                minimum=MIN_SHUTDOWN_TIMEOUT_SECONDS,
                maximum=MAX_SHUTDOWN_TIMEOUT_SECONDS,
            ),
        )
        object.__setattr__(
            self,
            "elapsed_seconds",
            _finite_seconds(self.elapsed_seconds, "elapsed_seconds"),
        )
        for value, field_name in (
            (self.deadline_exhausted, "deadline_exhausted"),
            (self.admission_frozen, "admission_frozen"),
            (self.control_plane_closed, "control_plane_closed"),
            (self.contract_satisfied, "contract_satisfied"),
            (self.clean, "clean"),
            (self.physical_exit_proven, "physical_exit_proven"),
        ):
            if type(value) is not bool:
                raise ValueError(f"{field_name} must be a bool")
        for value, enum_type, field_name in (
            (self.durable_recovery, ShutdownDurableRecoveryState, "durable_recovery"),
            (self.publication_fence, ShutdownPublicationFenceState, "publication_fence"),
            (self.owner_lease, ShutdownOwnerLeaseState, "owner_lease"),
            (self.storage_state, ShutdownStorageState, "storage_state"),
        ):
            if not isinstance(value, enum_type):
                raise ValueError(f"{field_name} has an invalid state")
        if type(self.components) is not tuple:
            raise ValueError("components must be a tuple")
        components = self.components
        if len(components) > MAX_SHUTDOWN_COMPONENTS:
            raise ValueError("shutdown report has too many components")
        if any(type(item) is not ShutdownComponentReport for item in components):
            raise ValueError("components must contain ShutdownComponentReport values")
        names = [item.component for item in components]
        if len(names) != len(set(names)):
            raise ValueError("shutdown component names must be unique")
        object.__setattr__(self, "components", components)
        expected_escaped = sum(item.escaped for item in components)
        if type(self.escaped_count) is not int or self.escaped_count < 0:
            raise ValueError("escaped_count must be a non-negative integer")
        if self.escaped_count != expected_escaped:
            raise ValueError("escaped_count must equal escaped component count")
        if expected_escaped and not self.deadline_exhausted:
            raise ValueError("escaped work requires deadline_exhausted")
        expected_contract = (
            self.admission_frozen
            and self.control_plane_closed
            and self.publication_fence is ShutdownPublicationFenceState.REVOKED
        )
        if self.contract_satisfied != expected_contract:
            raise ValueError("contract_satisfied must match publication revocation")
        expected_physical = bool(components) and all(
            item.physical_exit_proven for item in components
        )
        if self.physical_exit_proven != expected_physical:
            raise ValueError("physical_exit_proven must match component evidence")
        expected_clean = (
            expected_contract
            and self.durable_recovery
            in {
                ShutdownDurableRecoveryState.COMPLETED,
                ShutdownDurableRecoveryState.NOT_NEEDED,
            }
            and self.owner_lease
            in {
                ShutdownOwnerLeaseState.RELEASED,
                ShutdownOwnerLeaseState.LOST,
            }
            and self.storage_state is ShutdownStorageState.CLOSED
            and expected_physical
            and all(item.clean for item in components)
        )
        if self.clean != expected_clean:
            raise ValueError("clean must match recovery, lease, storage, and component evidence")

    def to_dict(self) -> dict[str, Any]:
        return {
            "protocol_version": self.protocol_version,
            "shutdown_id": self.shutdown_id,
            "phase": self.phase.value,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "timeout_seconds": self.timeout_seconds,
            "elapsed_seconds": self.elapsed_seconds,
            "deadline_exhausted": self.deadline_exhausted,
            "admission_frozen": self.admission_frozen,
            "durable_recovery": self.durable_recovery.value,
            "publication_fence": self.publication_fence.value,
            "owner_lease": self.owner_lease.value,
            "control_plane_closed": self.control_plane_closed,
            "contract_satisfied": self.contract_satisfied,
            "clean": self.clean,
            "physical_exit_proven": self.physical_exit_proven,
            "escaped_count": self.escaped_count,
            "storage_state": self.storage_state.value,
            "components": [item.to_dict() for item in self.components],
        }


class ShutdownReportBuilder:
    """Thread-safe first-winner aggregation for one Runtime shutdown."""

    def __init__(
        self,
        deadline: ShutdownDeadline,
        *,
        shutdown_id: str | None = None,
        started_at: datetime | None = None,
        utc_clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if type(deadline) is not ShutdownDeadline:
            raise ValueError("deadline must be a ShutdownDeadline")
        if not callable(utc_clock):
            raise ValueError("utc_clock must be callable")
        self.deadline = deadline
        candidate_shutdown_id = (
            uuid.uuid4().hex if shutdown_id is None else shutdown_id
        )
        if (
            type(candidate_shutdown_id) is not str
            or re.fullmatch(r"[0-9a-f]{32}", candidate_shutdown_id) is None
        ):
            raise ValueError("shutdown_id must be a 32-character lowercase hex id")
        self.shutdown_id = candidate_shutdown_id
        candidate_started_at = utc_clock() if started_at is None else started_at
        self.started_at = _aware_utc(candidate_started_at, "started_at")
        self._utc_clock = utc_clock
        self._lock = threading.RLock()
        self._phase = ShutdownPhase.OPEN
        self._admission_frozen = False
        self._durable_recovery = ShutdownDurableRecoveryState.NOT_ATTEMPTED
        self._publication_fence = ShutdownPublicationFenceState.NOT_ATTEMPTED
        self._owner_lease = ShutdownOwnerLeaseState.NOT_ATTEMPTED
        self._storage_state = ShutdownStorageState.OPEN
        self._expected_components: set[str] = set()
        self._components: dict[str, ShutdownComponentReport] = {}
        self._terminal: RuntimeShutdownReport | None = None
        self._deadline_terminalizer: threading.Thread | None = None

    def _bind_deadline_terminalizer(
        self,
        terminalizer: threading.Thread,
    ) -> None:
        """Install the sole writer capability that becomes active at cutoff."""

        if not isinstance(terminalizer, threading.Thread):
            raise ValueError("terminalizer must be a threading.Thread")
        with self._lock:
            if self._deadline_terminalizer is None:
                self._deadline_terminalizer = terminalizer
                return
            if self._deadline_terminalizer is not terminalizer:
                raise RuntimeError("shutdown deadline terminalizer is already bound")

    def _current_thread_owns_deadline(self) -> bool:
        return threading.current_thread() is self._deadline_terminalizer

    @property
    def phase(self) -> ShutdownPhase:
        with self._lock:
            return self._phase

    def advance(self, phase: ShutdownPhase) -> ShutdownPhase:
        if not isinstance(phase, ShutdownPhase):
            raise ValueError("phase must be a ShutdownPhase")
        if phase is ShutdownPhase.CLOSED:
            raise ValueError("use finish() to enter the closed phase")
        with self._lock:
            if self._terminal is not None:
                return self._phase
            if self.deadline.expired and not self._current_thread_owns_deadline():
                return self._phase
            if _PHASE_ORDER[phase] < _PHASE_ORDER[self._phase]:
                raise ValueError("shutdown phase cannot move backwards")
            self._phase = phase
            return phase

    def freeze_admission(self) -> None:
        with self._lock:
            if self._terminal is not None:
                return
            self._admission_frozen = True
            if _PHASE_ORDER[self._phase] < _PHASE_ORDER[ShutdownPhase.FREEZING]:
                self._phase = ShutdownPhase.FREEZING

    def expect_components(self, components: Iterable[str]) -> tuple[str, ...]:
        """Declare the complete shutdown evidence surface before work starts.

        A terminal report must never prove physical exit from an empty or
        partially observed component set.  Any expected component without a
        winner at ``finish()`` is materialised as escaped/uncertain evidence.
        """

        names = tuple(_token(item, "component") for item in components)
        with self._lock:
            if self._terminal is not None:
                return tuple(sorted(self._expected_components))
            if (
                self.deadline.expired
                and self._phase is not ShutdownPhase.OPEN
                and not self._current_thread_owns_deadline()
            ):
                return tuple(sorted(self._expected_components))
            combined = self._expected_components | set(names) | set(self._components)
            if len(combined) > MAX_SHUTDOWN_COMPONENTS:
                raise ValueError("shutdown report has too many components")
            self._expected_components.update(names)
            return tuple(sorted(self._expected_components))

    def record_component(self, report: ShutdownComponentReport) -> ShutdownComponentReport:
        if type(report) is not ShutdownComponentReport:
            raise ValueError("report must be a ShutdownComponentReport")
        with self._lock:
            if self._terminal is not None:
                winner = next(
                    (
                        item
                        for item in self._terminal.components
                        if item.component == report.component
                    ),
                    None,
                )
                return winner or report
            winner = self._components.get(report.component)
            if winner is not None:
                return winner
            if self.deadline.expired:
                # The deadline terminalizer owns the immutable cutoff. Late
                # observations remain available to the private finalizer but
                # cannot race into the public first-winner report.
                return report
            if len(set(self._components) | self._expected_components | {report.component}) > MAX_SHUTDOWN_COMPONENTS:
                raise ValueError("shutdown report has too many components")
            self._components[report.component] = report
            return report

    def set_durable_recovery(self, state: ShutdownDurableRecoveryState) -> None:
        if not isinstance(state, ShutdownDurableRecoveryState):
            raise ValueError("invalid durable recovery state")
        with self._lock:
            if self._terminal is not None:
                return
            if self.deadline.expired and not self._current_thread_owns_deadline():
                return
            self._durable_recovery = state

    def set_publication_fence(self, state: ShutdownPublicationFenceState) -> None:
        if not isinstance(state, ShutdownPublicationFenceState):
            raise ValueError("invalid publication fence state")
        with self._lock:
            if self._terminal is not None:
                return
            if self.deadline.expired and not self._current_thread_owns_deadline():
                return
            self._publication_fence = state

    def set_owner_lease(self, state: ShutdownOwnerLeaseState) -> None:
        if not isinstance(state, ShutdownOwnerLeaseState):
            raise ValueError("invalid owner lease state")
        with self._lock:
            if self._terminal is not None:
                return
            if self.deadline.expired and not self._current_thread_owns_deadline():
                return
            self._owner_lease = state

    def set_storage_state(self, state: ShutdownStorageState) -> None:
        if not isinstance(state, ShutdownStorageState):
            raise ValueError("invalid storage state")
        with self._lock:
            if self._terminal is not None:
                return
            if self.deadline.expired and not self._current_thread_owns_deadline():
                return
            self._storage_state = state

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            if self._terminal is not None:
                return self._terminal.to_dict()
            components = tuple(self._components.values())
            elapsed_seconds, _remaining, deadline_expired = (
                self.deadline.sample()
            )
            publication_revoked = (
                self._publication_fence is ShutdownPublicationFenceState.REVOKED
            )
            return {
                "protocol_version": SHUTDOWN_PROTOCOL_VERSION,
                "shutdown_id": self.shutdown_id,
                "phase": self._phase.value,
                "started_at": self.started_at.isoformat(),
                "finished_at": None,
                "timeout_seconds": self.deadline.timeout_seconds,
                "elapsed_seconds": elapsed_seconds,
                "deadline_exhausted": deadline_expired,
                "admission_frozen": self._admission_frozen,
                "durable_recovery": self._durable_recovery.value,
                "publication_fence": self._publication_fence.value,
                "owner_lease": self._owner_lease.value,
                "control_plane_closed": False,
                "contract_satisfied": False,
                "clean": False,
                "physical_exit_proven": (
                    bool(components)
                    and self._expected_components.issubset(self._components)
                    and all(item.physical_exit_proven for item in components)
                ),
                "escaped_count": sum(item.escaped for item in components),
                "storage_state": self._storage_state.value,
                "components": [item.to_dict() for item in components],
                "publication_revoked": publication_revoked,
            }

    def finish(
        self,
        *,
        finished_at: datetime | None = None,
        deadline_terminalizer: bool = False,
    ) -> RuntimeShutdownReport:
        with self._lock:
            if self._terminal is not None:
                return self._terminal

        # UTC acquisition is deliberately outside the commit lock. If an
        # injected/system wall clock stalls, the deadline terminalizer remains
        # free to enter the builder and publish from the monotonic boundary.
        candidate_finished_at = (
            self._utc_clock() if finished_at is None else finished_at
        )
        observed_at = max(
            _aware_utc(
                candidate_finished_at,
                "finished_at",
            ),
            self.started_at,
        )
        with self._lock:
            if self._terminal is not None:
                return self._terminal
            if deadline_terminalizer:
                if not self._current_thread_owns_deadline():
                    raise RuntimeError(
                        "only the bound shutdown deadline terminalizer may finish"
                    )
            if not self._admission_frozen:
                raise RuntimeError("cannot finish shutdown before admission is frozen")
            missing = sorted(self._expected_components - set(self._components))
            missing_reports = tuple(
                ShutdownComponentReport(
                    component=component,
                    effect=ShutdownEffectState.UNCERTAIN,
                    owner=ShutdownOwnerState.ESCAPED,
                    process_tree=ShutdownProcessTreeState.UNKNOWN,
                    cancel=ShutdownCancelState.UNKNOWN,
                    started_at=observed_at,
                    finished_at=observed_at,
                    elapsed_seconds=0.0,
                    active_before=1,
                    unresolved=1,
                    error_code="shutdown_component_unobserved",
                )
                for component in missing
            )
            components = (*self._components.values(), *missing_reports)
            escaped_count = sum(item.escaped for item in components)
            physical_exit_proven = bool(components) and all(
                item.physical_exit_proven for item in components
            )
            components_clean = all(item.clean for item in components)

            # This is the terminal commit linearization point. Everything
            # that may call an injected clock or construct missing evidence
            # has already completed. The one monotonic sample decides which
            # exact thread owns publication; no later clock read can reverse
            # that decision while the builder lock is held.
            elapsed_seconds, _remaining, deadline_expired = (
                self.deadline.sample()
            )
            if deadline_terminalizer:
                if not deadline_expired:
                    raise RuntimeError("shutdown deadline has not expired")
            elif deadline_expired:
                raise RuntimeError(
                    "expired shutdown must be finished by its deadline terminalizer"
                )

            durable_recovery = self._durable_recovery
            if deadline_terminalizer and durable_recovery in {
                ShutdownDurableRecoveryState.NOT_ATTEMPTED,
                ShutdownDurableRecoveryState.COMPLETED,
            }:
                # An overall lifecycle timeout invalidates an earlier
                # optimistic durable-completion classification: retained work
                # may still publish against local state.
                durable_recovery = ShutdownDurableRecoveryState.TIMED_OUT
            contract_satisfied = (
                self._publication_fence is ShutdownPublicationFenceState.REVOKED
            )
            clean = (
                contract_satisfied
                and durable_recovery
                in {
                    ShutdownDurableRecoveryState.COMPLETED,
                    ShutdownDurableRecoveryState.NOT_NEEDED,
                }
                and self._owner_lease
                in {
                    ShutdownOwnerLeaseState.RELEASED,
                    ShutdownOwnerLeaseState.LOST,
                }
                and self._storage_state is ShutdownStorageState.CLOSED
                and physical_exit_proven
                and components_clean
            )
            report = RuntimeShutdownReport(
                shutdown_id=self.shutdown_id,
                phase=ShutdownPhase.CLOSED,
                started_at=self.started_at,
                finished_at=observed_at,
                timeout_seconds=self.deadline.timeout_seconds,
                elapsed_seconds=elapsed_seconds,
                deadline_exhausted=deadline_expired or escaped_count > 0,
                admission_frozen=True,
                durable_recovery=durable_recovery,
                publication_fence=self._publication_fence,
                owner_lease=self._owner_lease,
                control_plane_closed=True,
                contract_satisfied=contract_satisfied,
                clean=clean,
                physical_exit_proven=physical_exit_proven,
                escaped_count=escaped_count,
                storage_state=self._storage_state,
                components=components,
            )
            for missing_report in missing_reports:
                self._components[missing_report.component] = missing_report
            self._durable_recovery = durable_recovery
            self._phase = ShutdownPhase.CLOSED
            self._terminal = report
            return report


@dataclass(frozen=True, slots=True)
class RuntimeShutdownClaim:
    """One caller's immutable view of a single-flight begin operation."""

    builder: ShutdownReportBuilder | None
    trigger: RuntimeShutdownTrigger
    primary_trigger: RuntimeShutdownTrigger
    seen_triggers: tuple[RuntimeShutdownTrigger, ...]
    is_owner: bool
    claim_id: str

    def __post_init__(self) -> None:
        if self.is_owner and not isinstance(self.builder, ShutdownReportBuilder):
            raise ValueError("an owner claim must carry its builder")
        if not self.is_owner and self.builder is not None:
            raise ValueError("a later claim must not expose the mutable builder")
        for value, field_name in (
            (self.trigger, "trigger"),
            (self.primary_trigger, "primary_trigger"),
        ):
            if not isinstance(value, RuntimeShutdownTrigger):
                raise ValueError(f"{field_name} must be a RuntimeShutdownTrigger")
        triggers = tuple(self.seen_triggers)
        if not triggers or self.primary_trigger not in triggers:
            raise ValueError("seen_triggers must contain the primary trigger")
        if len(triggers) != len(set(triggers)):
            raise ValueError("seen_triggers must be unique")
        if any(not isinstance(item, RuntimeShutdownTrigger) for item in triggers):
            raise ValueError("seen_triggers contains an invalid trigger")
        object.__setattr__(self, "seen_triggers", triggers)
        if not isinstance(self.is_owner, bool):
            raise ValueError("is_owner must be a bool")
        if (
            type(self.claim_id) is not str
            or re.fullmatch(r"[0-9a-f]{32}", self.claim_id) is None
        ):
            raise ValueError("claim_id must be a 32-character lowercase hex id")


class RuntimeRetainedOwnerProbe:
    """Non-executable typed snapshot cell for one retained physical owner.

    Observation never calls adapter code: producers publish an already-built
    canonical report through the composition-root registry. The sole Runtime
    finalizer therefore cannot be blocked or killed by a probe callback.
    """

    __slots__ = ("_component", "_lock", "_report")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        del kwargs
        raise TypeError("RuntimeRetainedOwnerProbe is a final canonical type")

    def __init__(
        self,
        component: str,
        initial_report: ShutdownComponentReport,
    ) -> None:
        self._component = _token(component, "component")
        if type(initial_report) is not ShutdownComponentReport:
            raise ValueError("initial_report must be a ShutdownComponentReport")
        if initial_report.component != self._component:
            raise ValueError("initial_report component must match probe component")
        self._lock = threading.RLock()
        self._report = initial_report

    @property
    def component(self) -> str:
        return self._component

    def snapshot(self) -> ShutdownComponentReport:
        with self._lock:
            return self._report

    def _publish(self, report: ShutdownComponentReport) -> None:
        if type(report) is not ShutdownComponentReport:
            raise ValueError("report must be a ShutdownComponentReport")
        if report.component != self._component:
            raise ValueError("report component must match probe component")
        with self._lock:
            if self._report.physical_exit_proven and not report.physical_exit_proven:
                raise RuntimeError("retained-owner physical proof cannot regress")
            self._report = report


class RuntimeRetainedOwnerProbeRegistry:
    """Single wakeable registry consumed by the Runtime finalizer owner."""

    def __init__(self) -> None:
        self._condition = threading.Condition(threading.RLock())
        self._generation = 0
        self._probes: dict[str, RuntimeRetainedOwnerProbe] = {}
        self._sealed = False

    def register(self, probe: RuntimeRetainedOwnerProbe) -> None:
        if type(probe) is not RuntimeRetainedOwnerProbe:
            raise ValueError("probe must be a RuntimeRetainedOwnerProbe")
        with self._condition:
            if self._sealed:
                raise RuntimeError("retained-owner probe registry is sealed")
            existing = self._probes.get(probe.component)
            if existing is not None and existing is not probe:
                raise RuntimeError(
                    f"retained-owner probe already registered: {probe.component}"
                )
            if existing is probe:
                return
            self._probes[probe.component] = probe
            self._generation += 1
            self._condition.notify_all()

    def publish(
        self,
        probe: RuntimeRetainedOwnerProbe,
        report: ShutdownComponentReport,
    ) -> None:
        if type(probe) is not RuntimeRetainedOwnerProbe:
            raise ValueError("probe must be a RuntimeRetainedOwnerProbe")
        if type(report) is not ShutdownComponentReport:
            raise ValueError("report must be a ShutdownComponentReport")
        with self._condition:
            if self._sealed:
                raise RuntimeError("retained-owner probe registry is sealed")
            if self._probes.get(probe.component) is not probe:
                raise RuntimeError(
                    f"retained-owner probe is not registered: {probe.component}"
                )
            probe._publish(report)
            self._generation += 1
            self._condition.notify_all()

    def snapshot(
        self,
    ) -> tuple[int, tuple[RuntimeRetainedOwnerProbe, ...]]:
        with self._condition:
            return self._generation, tuple(self._probes.values())

    def seal_if_unchanged(self, generation: int) -> bool:
        if type(generation) is not int:
            raise ValueError("generation must be an integer")
        with self._condition:
            if self._generation != generation:
                return False
            self._sealed = True
            return True

    def wait_for_change(
        self,
        generation: int,
        timeout: float | None = None,
    ) -> int:
        if type(generation) is not int:
            raise ValueError("generation must be an integer")
        if generation < 0:
            raise ValueError("generation must be non-negative")
        if timeout is not None:
            timeout = _finite_seconds(
                timeout,
                "timeout",
                minimum=0.0,
                maximum=MAX_REPORTED_ELAPSED_SECONDS,
            )
        with self._condition:
            if self._generation == generation:
                self._condition.wait_for(
                    lambda: self._generation != generation,
                    timeout=timeout,
                )
            return self._generation


class RuntimeShutdownObserver:
    """Read-only access to a shutdown flight before Runtime construction ends."""

    __slots__ = ("_controller",)

    def __init__(self, controller: "RuntimeShutdownController") -> None:
        if not isinstance(controller, RuntimeShutdownController):
            raise ValueError("controller must be a RuntimeShutdownController")
        self._controller = controller

    @property
    def primary_trigger(self) -> RuntimeShutdownTrigger | None:
        return self._controller.primary_trigger

    @property
    def seen_triggers(self) -> tuple[RuntimeShutdownTrigger, ...]:
        return self._controller.seen_triggers

    @property
    def owner_alive(self) -> bool:
        return self._controller.owner_alive

    def snapshot(self) -> dict[str, Any]:
        return self._controller.snapshot()

    def wait_terminal(
        self,
        timeout: float | None = None,
    ) -> RuntimeShutdownReport | None:
        return self._controller.wait_terminal(timeout)


class RuntimeShutdownController:
    """Thread-safe ownership for exactly one Runtime shutdown flight.

    The controller owns only lifecycle coordination state.  It never touches
    Storage or invokes a component closer; the Runtime composition root must
    first obtain the owner claim and then run its bounded coordinator.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._done = threading.Event()
        self._builder: ShutdownReportBuilder | None = None
        self._terminal: RuntimeShutdownReport | None = None
        self._primary_trigger: RuntimeShutdownTrigger | None = None
        self._seen_triggers: list[RuntimeShutdownTrigger] = []
        self._owner_claim_id: str | None = None
        self._owner_claim: RuntimeShutdownClaim | None = None
        self._owner: threading.Thread | None = None
        self._deadline_terminalizer: threading.Thread | None = None
        self._runtime_authority_acquired = False
        self._observer = RuntimeShutdownObserver(self)

    @property
    def observer(self) -> RuntimeShutdownObserver:
        return self._observer

    @property
    def primary_trigger(self) -> RuntimeShutdownTrigger | None:
        with self._lock:
            return self._primary_trigger

    @property
    def seen_triggers(self) -> tuple[RuntimeShutdownTrigger, ...]:
        with self._lock:
            return tuple(self._seen_triggers)

    @property
    def terminal(self) -> RuntimeShutdownReport | None:
        with self._lock:
            return self._terminal

    @property
    def shutdown_id(self) -> str | None:
        with self._lock:
            builder = self._builder
            return None if builder is None else builder.shutdown_id

    @property
    def deadline(self) -> ShutdownDeadline | None:
        with self._lock:
            builder = self._builder
            return None if builder is None else builder.deadline

    @property
    def owner_alive(self) -> bool:
        with self._lock:
            owner = self._owner
        return owner is not None and owner.is_alive()

    @property
    def runtime_authority_acquired(self) -> bool:
        with self._lock:
            return self._runtime_authority_acquired

    def mark_runtime_authority_acquired(self) -> None:
        """Record the point where Storage and lease became Runtime-owned."""

        with self._lock:
            if self._terminal is not None:
                raise RuntimeError(
                    "runtime authority cannot be acquired after shutdown terminal"
                )
            self._runtime_authority_acquired = True

    def begin(
        self,
        trigger: RuntimeShutdownTrigger,
        deadline: ShutdownDeadline,
        expected_components: Iterable[str],
    ) -> RuntimeShutdownClaim:
        """Claim or join the one flight without extending its deadline."""

        if not isinstance(trigger, RuntimeShutdownTrigger):
            raise ValueError("trigger must be a RuntimeShutdownTrigger")
        if type(deadline) is not ShutdownDeadline:
            raise ValueError("deadline must be a ShutdownDeadline")
        expected = tuple(expected_components)
        with self._lock:
            builder = self._builder
            if builder is None:
                # Construct and validate the complete candidate before any
                # controller field is mutated.  A rejected component name must
                # not leave a ghost trigger in observer history.
                candidate = ShutdownReportBuilder(deadline)
                candidate.expect_components(expected)
                candidate.freeze_admission()
                claim_id = uuid.uuid4().hex
                claim = RuntimeShutdownClaim(
                    builder=candidate,
                    trigger=trigger,
                    primary_trigger=trigger,
                    seen_triggers=(trigger,),
                    is_owner=True,
                    claim_id=claim_id,
                )
                self._builder = candidate
                self._primary_trigger = trigger
                self._seen_triggers.append(trigger)
                self._owner_claim_id = claim_id
                self._owner_claim = claim
                return claim
            if trigger not in self._seen_triggers:
                self._seen_triggers.append(trigger)
            primary = self._primary_trigger
            claim_id = self._owner_claim_id
            if primary is None or claim_id is None:  # pragma: no cover - invariant
                raise RuntimeError("shutdown controller is missing its first claim")
            return RuntimeShutdownClaim(
                builder=None,
                trigger=trigger,
                primary_trigger=primary,
                seen_triggers=tuple(self._seen_triggers),
                is_owner=False,
                claim_id=claim_id,
            )

    def bind_owner(
        self,
        claim: RuntimeShutdownClaim,
        owner: threading.Thread | None = None,
    ) -> None:
        """Bind the first claim to one coordinator thread exactly once."""

        if not isinstance(claim, RuntimeShutdownClaim):
            raise ValueError("claim must be a RuntimeShutdownClaim")
        candidate = threading.current_thread() if owner is None else owner
        if not isinstance(candidate, threading.Thread):
            raise ValueError("owner must be a threading.Thread")
        with self._lock:
            if (
                claim is not self._owner_claim
                or claim.builder is not self._builder
            ):
                raise RuntimeError("only the first shutdown claim may bind an owner")
            if self._owner is None:
                self._owner = candidate
                return
            if self._owner is not candidate:
                raise RuntimeError("shutdown coordinator owner is already bound")

    def bind_deadline_terminalizer(
        self,
        claim: RuntimeShutdownClaim,
        terminalizer: threading.Thread,
    ) -> None:
        """Bind one deadline publisher capability without transferring claim."""

        if not isinstance(claim, RuntimeShutdownClaim):
            raise ValueError("claim must be a RuntimeShutdownClaim")
        if not isinstance(terminalizer, threading.Thread):
            raise ValueError("terminalizer must be a threading.Thread")
        with self._lock:
            if claim is not self._owner_claim or claim.builder is not self._builder:
                raise RuntimeError(
                    "only the first shutdown claim may bind a terminalizer"
                )
            if self._deadline_terminalizer is None:
                if self._builder is None:  # pragma: no cover - invariant
                    raise RuntimeError("shutdown controller has no builder")
                self._builder._bind_deadline_terminalizer(terminalizer)
                self._deadline_terminalizer = terminalizer
                return
            if self._deadline_terminalizer is not terminalizer:
                raise RuntimeError("shutdown deadline terminalizer is already bound")

    def finish(self, claim: RuntimeShutdownClaim) -> RuntimeShutdownReport:
        """Finish the canonical builder under the exact first-claim authority."""

        if not isinstance(claim, RuntimeShutdownClaim):
            raise ValueError("claim must be a RuntimeShutdownClaim")
        with self._lock:
            if self._terminal is not None:
                if claim is not self._owner_claim:
                    raise RuntimeError(
                        "only the first shutdown claim may publish terminal state"
                    )
                return self._terminal
            builder = self._builder
            if claim is not self._owner_claim or claim.builder is not builder:
                raise RuntimeError(
                    "only the first shutdown claim may publish terminal state"
                )
            if self._owner is None:
                raise RuntimeError("shutdown coordinator owner is not bound")
            if threading.current_thread() is not self._owner:
                raise RuntimeError(
                    "only the bound shutdown coordinator thread may publish terminal state"
                )
            if builder is None:  # pragma: no cover - invariant
                raise RuntimeError("shutdown controller has no builder")
        # Never hold the controller lock across builder preparation. The
        # builder is the first-winner authority; releasing this outer lock
        # lets the exact terminalizer overtake an owner stalled before the
        # final monotonic commit sample.
        report = builder.finish()
        with self._lock:
            if self._terminal is None:
                self._terminal = report
                self._done.set()
            elif self._terminal is not report:  # pragma: no cover - invariant
                raise RuntimeError("shutdown controller terminal diverged")
            return self._terminal

    def finish_on_deadline(
        self,
        claim: RuntimeShutdownClaim,
    ) -> RuntimeShutdownReport:
        """Publish at the frozen deadline from the pre-bound terminalizer only."""

        if not isinstance(claim, RuntimeShutdownClaim):
            raise ValueError("claim must be a RuntimeShutdownClaim")
        with self._lock:
            if self._terminal is not None:
                if claim is not self._owner_claim:
                    raise RuntimeError(
                        "only the first shutdown claim may publish terminal state"
                    )
                return self._terminal
            builder = self._builder
            if claim is not self._owner_claim or claim.builder is not builder:
                raise RuntimeError(
                    "only the first shutdown claim may publish terminal state"
                )
            if threading.current_thread() is not self._deadline_terminalizer:
                raise RuntimeError(
                    "only the bound shutdown deadline terminalizer may publish"
                )
            if builder is None:  # pragma: no cover - invariant
                raise RuntimeError("shutdown controller has no builder")
        report = builder.finish(deadline_terminalizer=True)
        with self._lock:
            if self._terminal is None:
                self._terminal = report
                self._done.set()
            elif self._terminal is not report:  # pragma: no cover - invariant
                raise RuntimeError("shutdown controller terminal diverged")
            return self._terminal

    def wait_terminal(
        self,
        timeout: float | None = None,
    ) -> RuntimeShutdownReport | None:
        if timeout is not None:
            timeout = _finite_seconds(
                timeout,
                "timeout",
                minimum=0.0,
                maximum=MAX_REPORTED_ELAPSED_SECONDS,
            )
        self._done.wait(timeout=timeout)
        with self._lock:
            return self._terminal

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            terminal = self._terminal
            builder = self._builder
        if terminal is not None:
            return terminal.to_dict()
        if builder is not None:
            return dict(builder.snapshot())
        return dict(shutdown_snapshot_open())


def component_report(
    component: str,
    *,
    effect: ShutdownEffectState,
    owner: ShutdownOwnerState,
    process_tree: ShutdownProcessTreeState,
    cancel: ShutdownCancelState,
    started_at: datetime,
    started_monotonic: float,
    active_before: int = 0,
    unresolved: int = 0,
    error_code: str | None = None,
    detail: str | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    utc_clock: Callable[[], datetime] = _utc_now,
) -> ShutdownComponentReport:
    elapsed = max(0.0, float(monotonic()) - float(started_monotonic))
    return ShutdownComponentReport(
        component=component,
        effect=effect,
        owner=owner,
        process_tree=process_tree,
        cancel=cancel,
        started_at=started_at,
        finished_at=utc_clock(),
        elapsed_seconds=min(elapsed, MAX_REPORTED_ELAPSED_SECONDS),
        active_before=active_before,
        unresolved=unresolved,
        error_code=error_code,
        detail=detail,
    )


def shutdown_snapshot_open() -> Mapping[str, Any]:
    return {
        "protocol_version": SHUTDOWN_PROTOCOL_VERSION,
        "shutdown_id": None,
        "phase": ShutdownPhase.OPEN.value,
        "started_at": None,
        "finished_at": None,
        "timeout_seconds": None,
        "elapsed_seconds": 0.0,
        "deadline_exhausted": False,
        "admission_frozen": False,
        "durable_recovery": ShutdownDurableRecoveryState.NOT_ATTEMPTED.value,
        "publication_fence": ShutdownPublicationFenceState.ACTIVE.value,
        "owner_lease": ShutdownOwnerLeaseState.NOT_ATTEMPTED.value,
        "control_plane_closed": False,
        "contract_satisfied": False,
        "clean": False,
        "physical_exit_proven": False,
        "escaped_count": 0,
        "storage_state": ShutdownStorageState.OPEN.value,
        "components": [],
    }


__all__ = [
    "MAX_SHUTDOWN_COMPONENTS",
    "MAX_SHUTDOWN_TIMEOUT_SECONDS",
    "MIN_SHUTDOWN_TIMEOUT_SECONDS",
    "RuntimeShutdownReport",
    "RuntimeShutdownClaim",
    "RuntimeShutdownController",
    "RuntimeShutdownObserver",
    "RuntimeShutdownTrigger",
    "SHUTDOWN_PROTOCOL_VERSION",
    "ShutdownCancelState",
    "ShutdownComponentReport",
    "ShutdownDeadline",
    "ShutdownDurableRecoveryState",
    "ShutdownEffectState",
    "ShutdownOwnerLeaseState",
    "ShutdownOwnerState",
    "ShutdownPhase",
    "ShutdownProcessTreeState",
    "ShutdownPublicationFenceState",
    "ShutdownReportBuilder",
    "ShutdownStorageState",
    "component_report",
    "shutdown_snapshot_open",
]

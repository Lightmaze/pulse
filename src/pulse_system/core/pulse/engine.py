"""Pulse engine — the system heartbeat.

Implements the seven rules and the event-driven main loop.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import threading
import time
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Callable

from pulse_system.agent.harness.base import HarnessError
from pulse_system.core.connection.network import ConnectionNetwork
from pulse_system.core.connection.viability import analyze_connectivity
from pulse_system.core.causality import CausalLedger, CausalTransitionError
from pulse_system.core.causality.flow_contract import (
    causal_turn_violation_codes,
    may_emit_content_propagation,
)
from pulse_system.core.causality.ledger import (
    DendriticWindowConflictError,
    MAX_DENDRITIC_INTEGRATION_MEMBERS,
    RuntimeFence,
)
from pulse_system.core.dendrite.processor import (
    DendriteProcessor,
    DendriticReadyWindow,
)
from pulse_system.core.engram.manager import (
    SuccessionHarnessError,
    SuccessionPreparation,
    SuccessionPreparationError,
    SuccessionResult,
)
from pulse_system.core.runtime.resource import RuntimeManager, StabilityAdvice
from pulse_system.core.runtime.shutdown import (
    ShutdownCancelState,
    ShutdownComponentReport,
    ShutdownDeadline,
    ShutdownEffectState,
    ShutdownOwnerState,
    ShutdownProcessTreeState,
    component_report,
)
from pulse_system.core.types import (
    CausalEvent,
    CausalEventDomain,
    CausalEventFlow,
    CausalEventKind,
    CausalEventSource,
    CausalEventStatus,
    CenterReservation,
    CenterReservationOutcome,
    ConnectionType,
    EngramStatus,
    RuntimeLeaseError,
)
from pulse_system.substrate.llm.adapter import LLMCallError
from pulse_system.substrate.storage.store import Storage

if TYPE_CHECKING:
    from pulse_system.core.engram.manager import EngramManager
    from pulse_system.core.runtime.scheduling import DurableCenterScheduler
    from pulse_system.interaction.metrics import MetricsRecorder

_logger = logging.getLogger("pulse_system.engine")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class PulseReason(str, Enum):
    EXTERNAL = "external"
    PROPAGATION = "propagation"
    SPONTANEOUS = "spontaneous"


class SpontaneousDispatch(str, Enum):
    """Runtime's typed decision after spontaneous rhythm has fired."""

    EMITTED = "emitted"
    SUPPRESSED = "suppressed"
    FALLBACK = "fallback"


@dataclass
class PulseEvent:
    engram_id: str
    reason: PulseReason
    priority: float
    created_at: datetime = field(default_factory=_now)
    source_event_id: str | None = None
    content: str | None = None  # for external events carrying payload
    attempts: int = 0           # retryable execution attempts consumed
    # Durable causal context.  These fields are appended after the historical
    # constructor fields so old positional/unit-test construction remains
    # valid.  ``source_event_id`` is retained as the compatibility alias for
    # the direct parent event.
    event_id: str | None = None
    causal_id: str | None = None
    parent_event_id: str | None = None
    flow: CausalEventFlow | None = None
    source_engram_id: str | None = None
    depth: int = 0
    center_id: str | None = None

    def __post_init__(self) -> None:
        if self.parent_event_id is None:
            self.parent_event_id = self.source_event_id
        elif self.source_event_id is None:
            self.source_event_id = self.parent_event_id

    @classmethod
    def from_causal_event(cls, event: CausalEvent) -> "PulseEvent":
        """Build the reconstructible scheduling projection of one event."""

        dendritic_delivery = event.metadata.get("dendritic_delivery_class")
        if (
            event.metadata.get("dendritic_integration_version") == 1
            and dendritic_delivery == "propagation"
        ):
            reason = PulseReason.PROPAGATION
            default_priority = 0.8
        elif (
            event.metadata.get("dendritic_integration_version") == 1
            and dendritic_delivery == "external"
        ):
            reason = PulseReason.EXTERNAL
            default_priority = 1.0
        elif event.kind is CausalEventKind.PROPAGATION:
            reason = PulseReason.PROPAGATION
            default_priority = 0.8
        elif event.kind is CausalEventKind.SPONTANEOUS:
            reason = PulseReason.SPONTANEOUS
            default_priority = 0.3
        else:
            reason = PulseReason.EXTERNAL
            default_priority = 1.0

        raw_priority = event.metadata.get("priority", default_priority)
        priority = (
            float(raw_priority)
            if isinstance(raw_priority, (int, float))
            and not isinstance(raw_priority, bool)
            and math.isfinite(float(raw_priority))
            else default_priority
        )
        source_engram_id = event.metadata.get("source_engram_id")
        if not isinstance(source_engram_id, str) or not source_engram_id:
            source_engram_id = None
        raw_depth = event.metadata.get("depth", 0)
        depth = (
            raw_depth
            if type(raw_depth) is int and raw_depth >= 0
            else 0
        )
        return cls(
            engram_id=event.engram_id or "",
            reason=reason,
            priority=priority,
            created_at=event.created_at,
            source_event_id=event.parent_event_id,
            content=event.content,
            attempts=event.attempts,
            event_id=event.id,
            causal_id=event.causal_id,
            parent_event_id=event.parent_event_id,
            flow=event.flow,
            source_engram_id=source_engram_id,
            depth=depth,
            center_id=event.center_id,
        )


@dataclass(frozen=True)
class _EngramFailureDomain:
    """Transient protection state for one persistent Engram identity."""

    consecutive_failures: int
    last_failure_at: datetime
    cooling_until: datetime | None
    last_error_code: str
    last_error_phase: str | None
    error_retryable: bool
    prompt_accepted: bool | None


@dataclass(frozen=True)
class _SuccessionRequest:
    """One threshold crossing waiting for the bounded preparation fleet."""

    predecessor_id: str
    parent_event_id: str | None
    runtime_fence: RuntimeFence | None
    requested_at: datetime = field(default_factory=_now)


@dataclass
class PulseEngineConfig:
    propagation_threshold: float = 0.3
    # Optional hard causal ceiling for CONTENT diffusion.  A value of 1 lets
    # depth-0 sources enqueue direct depth-1 children and forbids those
    # children from emitting a second hop regardless of weight, claustrum
    # modulation, or later threshold changes.  None preserves open diffusion.
    max_content_propagation_depth: int | None = None
    # Optional provider-visible relay wrapper. It is empty in ordinary use;
    # callers may use it to request a no-tools turn profile without adding
    # control metadata to Engram content.
    propagation_content_prefix: str = ""
    budget_per_tick: int = 5
    spontaneous_check_interval: float = 10.0  # seconds
    tick_interval: float = 0.1                 # seconds between ticks
    decay_interval: float = 60.0               # seconds between decay cycles
    base_spontaneous_rate: float = 0.02        # base probability scaling
    # Half-life of an engram's recent_activity, applied in the periodic decay
    # step (same rhythm as connection decay). Each pulse adds +0.2 (capped at
    # 1.0); without decay the value saturates at 1.0 within ~5 pulses and both
    # the spontaneous factor (0.3+0.7·activity) and the claustrum feature lose
    # all resolution. ~10 min balances a steadily-pulsing engram near the top
    # of the range while letting a quiet one fade back down.
    activity_halflife_seconds: float = 600.0
    # Context size (engram.metadata.token_count) above which succession is
    # triggered after a pulse. Must leave headroom below the model's context
    # window for the summary request. None disables automatic succession.
    succession_token_threshold: int | None = 100_000
    # Failure handling: legacy LLMCallError and retryable HarnessError events
    # are requeued at reduced priority up to max_pulse_retries times. A
    # non-retryable HarnessError is recorded and dropped without replay.
    # After one Engram reaches failure_backoff_threshold consecutive failures,
    # only that Engram is excluded for failure_backoff_seconds. Other subjects
    # and the world clock continue; events for the affected Engram stay queued.
    max_pulse_retries: int = 3
    failure_backoff_threshold: int = 3
    failure_backoff_seconds: float = 30.0
    # Number of this tick's pulses to execute concurrently. 1 = serial.
    # Each pulse is an independent LLM session, so wall-clock per tick drops
    # from N x latency to ~1 x latency at the cost of parallel API load.
    max_parallel_pulses: int = 1
    # Runtime-owned worlds dispatch turns onto a persistent bounded worker
    # fleet and return immediately, so one slow Harness turn cannot freeze the
    # world's coordination clock. Direct/unit-test engines retain the legacy
    # synchronous tick contract unless this is explicitly enabled.
    background_dispatch: bool = False
    # Blocking summary/rotation work has its own bounded execution domain.
    # It must never consume the ordinary pulse fleet or mutate world state.
    max_parallel_successions: int = 2
    # An engram counts as "active" for the mind-tide (n/N) when it pulsed
    # within this window. Shorter-timescale runs can shrink it so the tide
    # stays dynamic instead of saturating.
    activity_window_seconds: float = 60.0
    # Inhibitory connections (v0.4): when a source with an INHIBITORY edge
    # fires, the target accumulates an inhibition level (edge weight) that
    # decays exponentially with this time constant. Inhibition suppresses
    # spontaneous activation and lowers propagation-event priority — it is
    # the mechanistic damper for reverberating loops.
    inhibition_tau: float = 30.0
    # Inhibition-to-propagation gate. By default inhibition only
    # gates *spontaneous* activation, so a propagation-sustained excitatory
    # cluster is immune to lateral inhibition and cannot be suppressed by a
    # competing theme. With gate g>0, a
    # propagation-triggered pulse fires only with probability 1/(1+g·inhibition),
    # so a strongly-inhibited engram stops reverberating: resonant clusters and
    # winner-take-all lateral inhibition coexist. g=0 disables the gate, g=1 applies it fully,
    # intermediate = partial coexistence (a smooth switch).
    inhibition_propagation_gate: float = 0.0
    # metrics topology snapshot cadence, in ticks. The activity stream (pulse /
    # propagate) carries only edges that fired; a network view also needs the
    # standing weights, which no other event exposes. None = off to preserve
    # the event stream; observatory runs opt in (~60s of ticks is a sane
    # cadence — the dump is a whole-graph SELECT).
    topology_interval_ticks: int | None = None
    # Compact structural projection of the threshold-eligible content graph.
    # Unlike the full topology dump this is cheap enough for production
    # observability. Direct engines remain opt-in so enabling the projection
    # never changes event volume implicitly. RuntimeService enables a bounded
    # cadence explicitly.
    connectivity_interval_ticks: int | None = None


class PulseEngine:
    """The system heartbeat: event collection, pulse execution, propagation, learning."""

    def __init__(
        self,
        storage: Storage,
        engram_manager: EngramManager,
        connection_network: ConnectionNetwork,
        dendrite: DendriteProcessor,
        runtime: RuntimeManager,
        config: PulseEngineConfig | None = None,
        metrics: "MetricsRecorder | None" = None,
        claustrum=None,
        sensory=None,
        spontaneous_factor: Callable[[str], float] | None = None,
        causal_ledger: "CausalLedger | None" = None,
        world_id: str | None = None,
        spontaneous_center: Callable[[str], str | None] | None = None,
        spontaneous_emitter: Callable[[str], SpontaneousDispatch] | None = None,
        scheduler: "DurableCenterScheduler | None" = None,
        runtime_fence: RuntimeFence | None = None,
    ):
        self._storage = storage
        self._engram_mgr = engram_manager
        self._connections = connection_network
        self._dendrite = dendrite
        self._runtime = runtime
        self._config = config or PulseEngineConfig()
        self._metrics = metrics
        # This ledger is the source of truth.  ``_pending_events`` is
        # only rebuilt as a priority/parallelism cache from QUEUED rows.
        self._causal_ledger = causal_ledger
        self._world_id = world_id
        # Optional ClaustrumModulator (spectrum stream). Detached, the
        # engine keeps neutral factors and its default threshold behavior.
        self._claustrum = claustrum
        self._activity_mods: dict[str, float] = {}
        # Per-engram propagation-threshold factor (claustrum control surface):
        # a source's outgoing propagation uses base_threshold * factor. Empty
        # (no claustrum) → .get default 1.0 → base threshold.
        self._propagation_mods: dict[str, float] = {}
        # Per-engram inhibition→propagation gate factor (claustrum fourth head): the
        # effective gate is config gate × factor, so the claustrum can learn the
        # resonance/inhibition balance per Engram. Empty → factor 1.0 → config gate unchanged.
        self._gate_mods: dict[str, float] = {}
        # Optional SensoryCortex: bound channels feed external events
        # directly into their engrams' local networks each tick.
        self._sensory = sensory
        # ActivityCenter status/autonomy is a world sideband.  It can scale
        # spontaneous life but never contributes text to an Engram prompt.
        self._spontaneous_factor = spontaneous_factor
        # Attribution is deliberately separate from modulation.  A caller may
        # identify one unambiguous active Center without turning Center data
        # into prompt text or guessing among several life domains.
        self._spontaneous_center = spontaneous_center
        # The Runtime-owned emitter is called only after the probability and
        # all existing sideband/inhibition modulation have accepted a random
        # spontaneous discharge.  It may project a subject-authored durable
        # event, suppress the discharge, or request the historical generic
        # path; it never participates in probability calculation.
        self._spontaneous_emitter = spontaneous_emitter
        if scheduler is not None and causal_ledger is None:
            raise ValueError("durable Center scheduler requires a causal ledger")
        if runtime_fence is not None and not isinstance(runtime_fence, RuntimeFence):
            raise ValueError("runtime_fence must be a RuntimeFence or null")
        if runtime_fence is not None and causal_ledger is None:
            raise ValueError("runtime_fence requires a causal ledger")
        self._scheduler = scheduler
        # Runtime-owned engines receive one permit-bearing lifecycle fence.
        # A reservation may confirm owner/epoch, but it must never be used to
        # reconstruct a weaker fence that drops the revocable publication
        # generation.
        self._runtime_fence = runtime_fence

        workers = self._config.max_parallel_pulses
        if type(workers) is not int or not 1 <= workers <= 64:
            raise ValueError("max_parallel_pulses must be an integer between 1 and 64")
        failure_threshold = self._config.failure_backoff_threshold
        if type(failure_threshold) is not int or failure_threshold < 1:
            raise ValueError("failure_backoff_threshold must be a positive integer")
        failure_seconds = self._config.failure_backoff_seconds
        if (
            isinstance(failure_seconds, bool)
            or not isinstance(failure_seconds, (int, float))
            or not math.isfinite(float(failure_seconds))
            or failure_seconds < 0
        ):
            raise ValueError(
                "failure_backoff_seconds must be a finite non-negative number"
            )
        if type(self._config.background_dispatch) is not bool:
            raise ValueError("background_dispatch must be a bool")
        max_content_depth = self._config.max_content_propagation_depth
        if max_content_depth is not None and (
            type(max_content_depth) is not int or max_content_depth < 0
        ):
            raise ValueError(
                "max_content_propagation_depth must be a non-negative integer or None"
            )
        propagation_prefix = self._config.propagation_content_prefix
        if (
            not isinstance(propagation_prefix, str)
            or "\x00" in propagation_prefix
            or len(propagation_prefix) > 4096
        ):
            raise ValueError(
                "propagation_content_prefix must be bounded NUL-free text"
            )
        succession_workers = self._config.max_parallel_successions
        if (
            type(succession_workers) is not int
            or not 1 <= succession_workers <= 64
        ):
            raise ValueError(
                "max_parallel_successions must be an integer between 1 and 64"
            )
        connectivity_interval = self._config.connectivity_interval_ticks
        if (
            connectivity_interval is not None
            and (
                type(connectivity_interval) is not int
                or connectivity_interval <= 0
            )
        ):
            raise ValueError(
                "connectivity_interval_ticks must be a positive integer or None"
            )
        self._pulse_executor: ThreadPoolExecutor | None = None
        if self._config.background_dispatch:
            self._pulse_executor = ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="pulse-turn",
            )
        self._succession_executor: ThreadPoolExecutor | None = None
        if self._config.background_dispatch:
            self._succession_executor = ThreadPoolExecutor(
                max_workers=succession_workers,
                thread_name_prefix="pulse-succession",
            )
        # The shutdown lock owns executor admission and the first-winner close
        # report.  It is never held while waiting for a worker or calling an
        # external Harness abort method.
        self._shutdown_lock = threading.RLock()
        self._shutdown_started = False
        self._shutdown_finished = threading.Event()
        self._shutdown_drained = threading.Event()
        self._shutdown_reports: tuple[ShutdownComponentReport, ...] | None = None
        self._inflight: dict[
            Future,
            tuple[PulseEvent, CenterReservation | None],
        ] = {}
        self._succession_lock = threading.RLock()
        self._succession_pending: dict[str, _SuccessionRequest] = {}
        self._succession_inflight: dict[Future, _SuccessionRequest] = {}
        # Completed activations are retained for the real STDP time window,
        # rather than being forgotten at the end of the Python tick in which
        # they happened. New activations are paired with this history exactly
        # once when their worker result is reaped.
        self._recent_activations: list[tuple[str, datetime]] = []

        self._pending_events: list[PulseEvent] = []
        self._last_decay_at: datetime = _now()
        self._last_spontaneous_at: datetime = _now()
        self._spontaneous_modifier: float = 1.0
        self._tick_count: int = 0
        # External-event metadata that must survive across ticks: the dendrite
        # window usually spans multiple ticks, so priority and origin are kept
        # here until the queued input actually dispatches.
        self._sticky_priority: dict[str, float] = {}
        self._external_marked: set[str] = set()
        # Harness failure protection follows the same identity boundary as
        # PiSession: a bad Engram can cool down without stopping its peers.
        # Durable retry/accepted/unknown truth remains in CausalLedger; this
        # transient map never creates or rewrites a causal event.
        self._failure_domain_lock = threading.RLock()
        self._failure_domains: dict[str, _EngramFailureDomain] = {}
        # Inhibition levels: engram_id -> (level, last_updated)
        self._inhibition: dict[str, tuple[float, datetime]] = {}
        # Trigger-chain depth (runtime metrics): external/spontaneous pulses are depth 0;
        # a pulse dispatched from propagated input is 1 + the deepest source.
        self._pulse_depth: dict[str, int] = {}
        self._incoming_depth: dict[str, int] = {}
        # Historical malformed rows remain auditable but are never offered to
        # Dendrite/Pi. Remember IDs so one persistent row emits one metric,
        # rather than one metric per coordinator tick.
        self._flow_blocked_event_ids: set[str] = set()

    @property
    def config(self) -> PulseEngineConfig:
        return self._config

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def causal_ledger(self) -> "CausalLedger | None":
        """The optional durable ledger used by Runtime composition."""

        return self._causal_ledger

    def capacity_snapshot(self) -> dict[str, int | bool]:
        """Content-free facts about the bounded pulse worker fleet."""

        limit = self._config.max_parallel_pulses
        with self._shutdown_lock:
            running = len(self._inflight)
        with self._succession_lock:
            succession_running = len(self._succession_inflight)
            succession_pending = len(self._succession_pending)
            running_predecessors = {
                request.predecessor_id
                for request in self._succession_inflight.values()
            }
        blocked = (
            set()
            if self._causal_ledger is None
            else self._causal_ledger.generation_blocked_predecessors()
        ) - running_predecessors
        return {
            "background_dispatch": self._config.background_dispatch,
            "worker_limit": limit,
            "worker_running": running,
            "worker_available": max(0, limit - running),
            "succession_worker_limit": self._config.max_parallel_successions,
            "succession_workers_running": succession_running,
            "succession_subjects_pending": succession_pending,
            "succession_subjects_blocked": len(blocked),
        }

    def failure_domain_snapshot(
        self,
        *,
        limit: int = 64,
        observed_at: datetime | None = None,
    ) -> dict[str, object]:
        """Return a bounded, content-free view of local Harness failures."""

        if type(limit) is not int or limit < 1 or limit > 64:
            raise ValueError("failure-domain limit must be an integer from 1 to 64")
        now = observed_at or _now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        now = now.astimezone(timezone.utc)
        with self._failure_domain_lock:
            records = list(self._failure_domains.items())

        items: list[dict[str, object]] = []
        counts = {"cooling": 0, "degraded": 0, "probe_ready": 0}
        state_order = {"cooling": 0, "degraded": 1, "probe_ready": 2}
        for engram_id, record in records:
            if record.cooling_until is not None and now < record.cooling_until:
                state = "cooling"
                retry_at = record.cooling_until.isoformat()
            elif (
                record.consecutive_failures
                >= self._config.failure_backoff_threshold
            ):
                state = "probe_ready"
                retry_at = None
            else:
                state = "degraded"
                retry_at = None
            counts[state] += 1
            items.append({
                "engram_id": engram_id,
                "state": state,
                "consecutive_failures": record.consecutive_failures,
                "last_failure_at": record.last_failure_at.isoformat(),
                "retry_at": retry_at,
                "last_error_code": record.last_error_code,
                "last_error_phase": record.last_error_phase,
                "error_retryable": record.error_retryable,
                "prompt_accepted": record.prompt_accepted,
            })
        items.sort(
            key=lambda item: (
                state_order[str(item["state"])],
                -datetime.fromisoformat(str(item["last_failure_at"])).timestamp(),
                str(item["engram_id"]),
            )
        )
        total = len(items)
        return {
            "policy_version": "engram-failure-domain.v1",
            "evidence_class": "runtime_memory_projection",
            "limit": limit,
            "total": total,
            "cooling": counts["cooling"],
            "degraded": counts["degraded"],
            "probe_ready": counts["probe_ready"],
            "truncated": total > limit,
            "items": items[:limit],
        }

    def close(
        self,
        *,
        deadline: ShutdownDeadline | None = None,
        abort: Callable[[str], None] | None = None,
    ) -> tuple[ShutdownComponentReport, ...]:
        """Freeze dispatch and return before one non-extendable deadline.

        Queued calls are cancelled.  Running calls receive best-effort aborts,
        but an uncooperative Python worker is not relabelled as stopped: it is
        returned as ``escaped`` and Runtime recovery owns its durable rows.
        Successful pulse turns that settled before the deadline keep their
        existing durable winner, but shutdown never creates propagation, STDP,
        or a new succession commit from a late worker result.
        """

        deadline = deadline or ShutdownDeadline.after(30.0)
        if not isinstance(deadline, ShutdownDeadline):
            raise ValueError("deadline must be a ShutdownDeadline")

        with self._shutdown_lock:
            reports = self._shutdown_reports
            if reports is not None:
                return reports
            if self._shutdown_started:
                first_caller = False
                pulse_executor = None
                succession_executor = None
                pulse_entries = []
                succession_entries = []
            else:
                first_caller = True
                self._shutdown_started = True
                pulse_executor = self._pulse_executor
                succession_executor = self._succession_executor
                self._pulse_executor = None
                self._succession_executor = None
                pulse_entries = list(self._inflight.items())
                with self._succession_lock:
                    self._succession_pending.clear()
                    succession_entries = list(self._succession_inflight.items())

        if not first_caller:
            self._shutdown_finished.wait(timeout=deadline.remaining_seconds())
            with self._shutdown_lock:
                if self._shutdown_reports is not None:
                    return self._shutdown_reports
            now = _now()
            return (
                ShutdownComponentReport(
                    component="engine_shutdown",
                    effect=ShutdownEffectState.UNCERTAIN,
                    owner=ShutdownOwnerState.ESCAPED,
                    process_tree=ShutdownProcessTreeState.NOT_APPLICABLE,
                    cancel=ShutdownCancelState.UNKNOWN,
                    started_at=now,
                    finished_at=now,
                    elapsed_seconds=0.0,
                    active_before=1,
                    unresolved=1,
                    error_code="concurrent_shutdown_incomplete",
                ),
            )

        started_at = _now()
        started_monotonic = time.monotonic()
        cancelled_pulses = 0
        cancelled_successions = 0
        running_engrams: list[str] = []

        for future, (event, _reservation) in pulse_entries:
            if future.cancel():
                cancelled_pulses += 1
            elif not future.done():
                running_engrams.append(event.engram_id)
        for future, request in succession_entries:
            if future.cancel():
                cancelled_successions += 1
            elif not future.done():
                running_engrams.append(request.predecessor_id)

        # This is deliberately wait=False.  A daemon finalizer below may prove
        # that the pool threads exited, but the close caller never enters
        # ThreadPoolExecutor's unbounded join path.
        for executor in (pulse_executor, succession_executor):
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)

        abort_threads: list[threading.Thread] = []
        abort_failures: list[str] = []
        abort_failure_lock = threading.Lock()
        if abort is not None:
            for engram_id in dict.fromkeys(running_engrams):
                thread = threading.Thread(
                    target=self._abort_for_shutdown,
                    args=(
                        abort,
                        engram_id,
                        abort_failures,
                        abort_failure_lock,
                    ),
                    name=f"pulse-abort-{engram_id[:12]}",
                    daemon=True,
                )
                abort_threads.append(thread)
                thread.start()

        futures = {
            *(future for future, _scope in pulse_entries),
            *(future for future, _scope in succession_entries),
        }
        if futures and not deadline.expired:
            wait(futures, timeout=deadline.remaining_seconds())

        pulse_claimed, pulse_errors = self._reap_shutdown_pulses(pulse_entries)
        succession_claimed, succession_errors = self._reap_shutdown_successions(
            succession_entries
        )

        finalizers = {
            "pulse_workers": self._start_executor_finalizer(
                pulse_executor,
                name="pulse-pool-finalizer",
            ),
            "succession_workers": self._start_executor_finalizer(
                succession_executor,
                name="succession-pool-finalizer",
            ),
        }
        pending_finalizers = tuple(
            done for done in finalizers.values() if done is not None
        )
        if pending_finalizers:
            def mark_drained() -> None:
                for done in pending_finalizers:
                    done.wait()
                self._shutdown_drained.set()

            threading.Thread(
                target=mark_drained,
                name="pulse-engine-drain",
                daemon=True,
            ).start()
        else:
            self._shutdown_drained.set()
        for done in finalizers.values():
            if done is not None:
                done.wait(timeout=deadline.remaining_seconds())
        for thread in abort_threads:
            thread.join(timeout=deadline.remaining_seconds())

        pulse_unresolved = max(0, len(pulse_entries) - len(pulse_claimed))
        succession_unresolved = max(
            0,
            len(succession_entries) - len(succession_claimed),
        )
        pulse_finalized = (
            finalizers["pulse_workers"] is None
            or finalizers["pulse_workers"].is_set()
        )
        succession_finalized = (
            finalizers["succession_workers"] is None
            or finalizers["succession_workers"].is_set()
        )
        if not pulse_finalized:
            pulse_unresolved = max(1, pulse_unresolved)
        if not succession_finalized:
            succession_unresolved = max(1, succession_unresolved)

        reports = (
            self._worker_shutdown_report(
                component="pulse_workers",
                started_at=started_at,
                started_monotonic=started_monotonic,
                active_before=len(pulse_entries),
                unresolved=pulse_unresolved,
                cancelled=cancelled_pulses,
                errors=pulse_errors,
                finalized=pulse_finalized,
            ),
            self._worker_shutdown_report(
                component="succession_workers",
                started_at=started_at,
                started_monotonic=started_monotonic,
                active_before=len(succession_entries),
                unresolved=succession_unresolved,
                cancelled=cancelled_successions,
                errors=succession_errors,
                finalized=succession_finalized,
            ),
        )
        alive_aborts = sum(thread.is_alive() for thread in abort_threads)
        if abort_threads:
            reports = (
                *reports,
                component_report(
                    "harness_abort",
                    effect=(
                        ShutdownEffectState.UNCERTAIN
                        if alive_aborts or abort_failures
                        else ShutdownEffectState.SETTLED
                    ),
                    owner=(
                        ShutdownOwnerState.ESCAPED
                        if alive_aborts
                        else ShutdownOwnerState.JOINED
                    ),
                    process_tree=ShutdownProcessTreeState.UNKNOWN,
                    cancel=(
                        ShutdownCancelState.UNKNOWN
                        if alive_aborts
                        else (
                            ShutdownCancelState.FAILED
                            if abort_failures
                            else ShutdownCancelState.SIGNALLED
                        )
                    ),
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    active_before=len(abort_threads),
                    unresolved=alive_aborts,
                    error_code=(
                        "abort_callback_incomplete"
                        if alive_aborts
                        else (
                            "abort_callback_failed" if abort_failures else None
                        )
                    ),
                ),
            )

        with self._shutdown_lock:
            self._shutdown_reports = reports
            self._shutdown_finished.set()
        return reports

    def wait_for_shutdown_drain(self, timeout: float | None = None) -> bool:
        """Wait only for executor-owner exit, never for publication rights.

        Runtime uses this from its daemon Storage finalizer.  The publication
        gate remains the authoritative late-write fence even if this returns
        false forever.
        """

        return self._shutdown_drained.wait(timeout=timeout)

    @staticmethod
    def _abort_for_shutdown(
        abort: Callable[[str], None],
        engram_id: str,
        failures: list[str],
        failures_lock: threading.Lock,
    ) -> None:
        try:
            abort(engram_id)
        except Exception:  # noqa: BLE001 - the worker report remains authoritative
            with failures_lock:
                failures.append(engram_id)
            _logger.exception("could not abort in-flight pulse for %s", engram_id)

    @staticmethod
    def _start_executor_finalizer(
        executor: ThreadPoolExecutor | None,
        *,
        name: str,
    ) -> threading.Event | None:
        if executor is None:
            return None
        done = threading.Event()

        def finalize() -> None:
            try:
                # The daemon wrapper, not the close caller, owns this join. If
                # a worker is non-cooperative the parent deadline reports it as
                # escaped while this finalizer remains available for eventual
                # cleanup.
                executor.shutdown(wait=True, cancel_futures=True)
            finally:
                done.set()

        threading.Thread(target=finalize, name=name, daemon=True).start()
        return done

    def _reap_shutdown_pulses(
        self,
        entries: list[tuple[Future, tuple[PulseEvent, CenterReservation | None]]],
    ) -> tuple[set[Future], int]:
        """Settle done reservations without creating new living activity."""

        claimed: set[Future] = set()
        attempts = []
        with self._shutdown_lock:
            for future, (event, reservation) in entries:
                current = self._inflight.get(future)
                if current is None:
                    # A coordinator tick won the claim before shutdown.
                    claimed.add(future)
                    continue
                if not future.done():
                    continue
                self._inflight.pop(future, None)
                claimed.add(future)
                try:
                    attempt = future.result()
                except CancelledError:
                    attempt = (
                        event,
                        _now(),
                        None,
                        ValueError("pulse dispatch cancelled before execution"),
                    )
                attempts.append((attempt, reservation))

        errors = 0
        for (event, _pulse_time, pulse_result, error), reservation in attempts:
            if reservation is not None and self._scheduler is not None:
                if pulse_result is not None:
                    outcome = CenterReservationOutcome.SUCCEEDED
                elif isinstance(error, (LLMCallError, HarnessError)):
                    outcome = CenterReservationOutcome.FAILED
                elif isinstance(error, ValueError):
                    outcome = CenterReservationOutcome.SKIPPED
                else:
                    outcome = CenterReservationOutcome.UNCERTAIN
                try:
                    self._scheduler.settle(reservation.id, outcome)
                except Exception:  # noqa: BLE001 - Runtime recovery owns the row
                    errors += 1
            if pulse_result is not None:
                try:
                    self._runtime.consume_budget(
                        pulse_result.input_tokens,
                        pulse_result.output_tokens,
                        pulse_result.cached_tokens,
                        pulse_result.cache_write_tokens,
                    )
                except Exception:  # noqa: BLE001 - accounting evidence is uncertain
                    errors += 1
                self._record(
                    "pulse_shutdown_settled",
                    engram=event.engram_id,
                    event_id=event.event_id,
                )
            elif error is not None and not isinstance(error, ValueError):
                self._record(
                    "pulse_shutdown_failed",
                    engram=event.engram_id,
                    event_id=event.event_id,
                    code=type(error).__name__.casefold(),
                )
        return claimed, errors

    def _reap_shutdown_successions(
        self,
        entries: list[tuple[Future, _SuccessionRequest]],
    ) -> tuple[set[Future], int]:
        """Charge settled summary usage but never commit a shutdown lineage."""

        claimed: set[Future] = set()
        completed: list[tuple[Future, _SuccessionRequest]] = []
        with self._succession_lock:
            for future, request in entries:
                if future not in self._succession_inflight:
                    claimed.add(future)
                    continue
                if not future.done():
                    continue
                self._succession_inflight.pop(future, None)
                claimed.add(future)
                completed.append((future, request))

        errors = 0
        for future, request in completed:
            try:
                preparation = future.result()
            except CancelledError:
                self._record(
                    "succession_shutdown_cancelled",
                    old=request.predecessor_id,
                )
                continue
            except Exception as error:  # noqa: BLE001 - no commit during shutdown
                if isinstance(
                    error,
                    (SuccessionHarnessError, SuccessionPreparationError),
                ):
                    usage = error.claim_usage()
                    if usage is not None:
                        try:
                            self._runtime.consume_budget(
                                usage.input_tokens,
                                usage.output_tokens,
                                usage.cached_tokens,
                                usage.cache_write_tokens,
                            )
                        except Exception:  # noqa: BLE001
                            errors += 1
                self._record(
                    "succession_shutdown_failed",
                    old=request.predecessor_id,
                    code=type(error).__name__.casefold(),
                )
                continue
            if not isinstance(preparation, SuccessionPreparation):
                errors += 1
                continue
            try:
                self._runtime.consume_budget(
                    preparation.input_tokens,
                    preparation.output_tokens,
                    preparation.cached_tokens,
                    preparation.cache_write_tokens,
                )
            except Exception:  # noqa: BLE001
                errors += 1
            self._record(
                "succession_shutdown_abandoned",
                old=preparation.predecessor_id,
                generation=preparation.generation_id,
            )
        return claimed, errors

    @staticmethod
    def _worker_shutdown_report(
        *,
        component: str,
        started_at: datetime,
        started_monotonic: float,
        active_before: int,
        unresolved: int,
        cancelled: int,
        errors: int,
        finalized: bool,
    ) -> ShutdownComponentReport:
        if unresolved > 0 or not finalized:
            effect = ShutdownEffectState.UNCERTAIN
            owner = ShutdownOwnerState.ESCAPED
            unresolved = max(1, unresolved)
            error_code = "worker_exit_unproven"
            cancel = (
                ShutdownCancelState.SIGNALLED
                if cancelled
                else ShutdownCancelState.UNKNOWN
            )
        elif errors:
            effect = ShutdownEffectState.UNCERTAIN
            owner = ShutdownOwnerState.JOINED
            error_code = "shutdown_bookkeeping_failed"
            cancel = (
                ShutdownCancelState.SIGNALLED
                if cancelled
                else ShutdownCancelState.NOT_NEEDED
            )
        elif active_before == 0:
            effect = ShutdownEffectState.NOT_STARTED
            owner = ShutdownOwnerState.JOINED
            error_code = None
            cancel = ShutdownCancelState.NOT_NEEDED
        elif cancelled == active_before:
            effect = ShutdownEffectState.NOT_STARTED
            owner = ShutdownOwnerState.JOINED
            error_code = None
            cancel = ShutdownCancelState.SIGNALLED
        else:
            effect = ShutdownEffectState.SETTLED
            owner = ShutdownOwnerState.JOINED
            error_code = None
            cancel = (
                ShutdownCancelState.SIGNALLED
                if cancelled
                else ShutdownCancelState.NOT_NEEDED
            )
        return component_report(
            component,
            effect=effect,
            owner=owner,
            process_tree=ShutdownProcessTreeState.NOT_APPLICABLE,
            cancel=cancel,
            started_at=started_at,
            started_monotonic=started_monotonic,
            active_before=active_before,
            unresolved=unresolved,
            error_code=error_code,
        )

    # ── Public API ───────────────────────────────────────────────

    def inject_external_event(
        self, engram_id: str, content: str, priority: float = 1.0
    ) -> CausalEvent | None:
        """Rule 1 entry point: external event arrives."""
        with self._shutdown_lock:
            if self._shutdown_started:
                raise RuntimeError("pulse engine is closed")
        if self._causal_ledger is not None:
            return self.enqueue_causal_event(
                engram_id=engram_id,
                content=content,
                flow=CausalEventFlow.CONTENT,
                domain=CausalEventDomain.PULSE,
                kind=CausalEventKind.STIMULUS,
                source=CausalEventSource.USER,
                metadata={"priority": priority},
            )
        self._pending_events.append(PulseEvent(
            engram_id=engram_id,
            reason=PulseReason.EXTERNAL,
            priority=priority,
            content=content,
        ))
        return None

    def enqueue_causal_event(
        self,
        *,
        engram_id: str | None,
        content: str | None,
        flow: CausalEventFlow | str | None,
        domain: CausalEventDomain | str,
        kind: CausalEventKind | str,
        source: CausalEventSource | str,
        causal_id: str | None = None,
        parent_event_id: str | None = None,
        center_id: str | None = None,
        metadata: dict | None = None,
        world_id: str | None = None,
        idempotency_key: str | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> CausalEvent:
        """Persist a causal input for a future tick.

        This is the narrow Runtime composition seam for durable external/world
        inputs.  It intentionally does not touch Dendrite or the in-memory
        pending list.
        """

        if self._causal_ledger is None:
            raise ValueError("causal ledger is required for durable events")
        resolved_world_id = world_id or self._world_id
        if not isinstance(resolved_world_id, str) or not resolved_world_id:
            raise ValueError(
                "world_id is required to enqueue a durable causal event"
            )
        resolved_runtime_fence = runtime_fence or self._runtime_fence
        return self._causal_ledger.enqueue(
            world_id=resolved_world_id,
            flow=flow,
            domain=domain,
            kind=kind,
            source=source,
            content=content,
            causal_id=causal_id,
            parent_event_id=parent_event_id,
            engram_id=engram_id,
            center_id=center_id,
            metadata=metadata,
            idempotency_key=idempotency_key,
            runtime_fence=resolved_runtime_fence,
        )

    def tick(self) -> list[tuple[str, str]]:
        """Advance one coordinator tick and return turns reaped this tick.

        With background dispatch enabled, newly admitted turns continue after
        this method returns and appear in a later tick's result list.
        """
        with self._shutdown_lock:
            if self._shutdown_started:
                return []
        now = _now()
        self._tick_count += 1
        # A completed preparation is committed before any new admission so
        # the successor, queued-event handoff, and listener projections become
        # visible as one coordinator-owned boundary.
        self._reap_successions()
        results, activations, unexpected_errors = self._reap_completed()
        if unexpected_errors:
            # All other completed attempts in the reap batch have already
            # reached their terminal bookkeeping before one fault is surfaced.
            raise unexpected_errors[0]

        # A threshold crossing reaped above gets first claim on an available
        # succession slot.  If the succession pool is saturated the request
        # remains pending and the subject may continue ordinary turns; once a
        # slot is available, dispatch creates the per-subject hold before this
        # tick admits another turn for that subject.
        self._dispatch_pending_successions()

        # 0. Spectrum modulation (Claustrum modulator): refresh activity/wait factors before
        #    any readiness or spontaneous computation this tick.
        self._apply_claustrum(now)

        # 0.5 Sensory intake (SensoryCortex): channel items become external events on
        #     their bound engrams — local processing, no front routing.
        if self._sensory is not None:
            for eid, content, priority in self._sensory.poll():
                self.inject_external_event(eid, content, priority=priority)
                self._record("sensory", engram=eid, chars=len(content))

        if self._causal_ledger is None:
            # 1. Route external events into dendrite queues. Priority and
            #    origin are recorded in sticky state so they survive until
            #    the dendrite window actually dispatches — possibly several
            #    ticks later.
            remaining: list[PulseEvent] = []
            for ev in self._pending_events:
                if ev.reason == PulseReason.EXTERNAL and ev.content:
                    # The coordinator owns this admission timestamp.  Using
                    # Dendrite's independent wall clock can make an input look
                    # as though it arrived after the readiness observation
                    # when Pulse runs under a frozen/simulated clock.
                    self._dendrite.receive(
                        ev.engram_id,
                        ev.content,
                        "external",
                        1.0,
                        arrived_at=_now(),
                    )
                    self._sticky_priority[ev.engram_id] = max(
                        self._sticky_priority.get(ev.engram_id, 0.0),
                        ev.priority,
                    )
                    self._external_marked.add(ev.engram_id)
                else:
                    remaining.append(ev)
            self._pending_events = remaining

            # 2. Collect all dendrite-ready engrams (from prior propagations +
            #    externals). Readiness is evaluated at the current instant,
            #    not the tick start — inputs routed in step 1 have
            #    last_input_at later than `now`, and a negative elapsed would
            #    wrongly defer zero-threshold windows by one tick.
            ready_ids = self._dendrite.get_all_ready(_now())
            already_pending = {ev.engram_id for ev in self._pending_events}
            for eid in ready_ids:
                self._dendrite.integrate(eid)
                if eid not in already_pending:
                    priority = self._sticky_priority.pop(eid, 0.8)
                    if eid in self._external_marked:
                        reason = PulseReason.EXTERNAL
                        self._external_marked.discard(eid)
                    else:
                        reason = PulseReason.PROPAGATION
                        # Inhibition dampens network-internal traffic, never
                        # user-facing external events.
                        inhibition = self._inhibition_level(eid, now)
                        if inhibition > 0:
                            priority *= 1.0 / (1.0 + inhibition)
                    self._pending_events.append(PulseEvent(
                        engram_id=eid,
                        reason=reason,
                        priority=priority,
                    ))
        else:
            # CausalLedger is the durable queue.  The list below is only a
            # reconstructible priority/parallelism cache and is refreshed from
            # QUEUED rows every tick; no event is claimed by this list.
            self._refresh_durable_event_cache()

        # 3. Spontaneous activation (Rule 6)
        elapsed_spont = (now - self._last_spontaneous_at).total_seconds()
        if elapsed_spont >= self._config.spontaneous_check_interval:
            self._check_spontaneous()
            self._last_spontaneous_at = now
            if self._causal_ledger is not None:
                self._refresh_durable_event_cache()

        # 4. Failure domains: an expired local cooldown becomes probe-ready,
        #    but no timer manufactures a pulse or Harness call.
        self._prune_inactive_failure_domains()
        self._release_expired_failure_domains(now)
        cooling_engrams = self._cooling_engrams(now)
        succession_excluded = self._succession_excluded_engrams()

        # 5. Budget check
        budget = min(self._runtime.get_budget(), self._config.budget_per_tick)
        worker_available = (
            self._config.max_parallel_pulses - len(self._inflight)
            if self._config.background_dispatch
            else budget
        )
        admission_budget = min(budget, max(0, worker_available))

        # 6. Admission. Without a scheduler this remains the historical
        # priority truncation path. Runtime-owned durable worlds first commit
        # Center decisions and held reservations in one SQLite transaction.
        to_execute, reservations_by_event = self._select_events(
            now,
            admission_budget,
            excluded_engrams={
                event.engram_id
                for event, _reservation in self._inflight.values()
            }
            | cooling_engrams
            | succession_excluded,
        )
        if admission_budget <= 0 and to_execute:
            raise RuntimeError("zero-capacity scheduler admitted an event")
        if budget <= 0 and self._pending_events:
            _logger.warning(
                "budget exhausted: %d events waiting", len(self._pending_events)
            )
            self._record("budget_exhausted", pending=len(self._pending_events))
        elif worker_available <= 0 and self._pending_events:
            self._record(
                "pulse_workers_saturated",
                running=len(self._inflight),
                limit=self._config.max_parallel_pulses,
                pending=len(self._pending_events),
            )
        if self._causal_ledger is not None:
            for event in to_execute:
                if event.event_id is not None:
                    self._dendrite.remove_event(event.event_id)

        # 7. Runtime-owned worlds submit and immediately continue. Legacy
        # direct engines preserve synchronous return values for compatibility.
        if self._config.background_dispatch:
            self._dispatch_events(to_execute, reservations_by_event)
        else:
            attempts = self._execute_events(to_execute, reservations_by_event)
            completed, completed_activations, errors = self._process_attempts(
                attempts,
                reservations_by_event,
            )
            results.extend(completed)
            activations.extend(completed_activations)
            unexpected_errors.extend(errors)

        if unexpected_errors:
            # Every attempted slot above is already terminal (unexpected
            # attempts are conservative ``uncertain``). Surface the first
            # programming/infrastructure fault only after the whole batch can
            # be recovered without leaving same-owner held rows behind.
            raise unexpected_errors[0]

        # 8. STDP learning (Rules 2 & 3) spans the configured co-activation
        # window even when the two worker completions land in different ticks.
        self._learn_from_activations(activations)

        # 9. Periodic decay (Rule 4)
        elapsed_decay = (now - self._last_decay_at).total_seconds()
        if elapsed_decay >= self._config.decay_interval:
            decayed, pruned = self._connections.decay_and_prune()
            # recent_activity decays on the same rhythm, in one bulk UPDATE.
            # Factor from the actual elapsed span, so a changed decay_interval
            # still yields the configured half-life.
            factor = 0.5 ** (
                elapsed_decay / self._config.activity_halflife_seconds
            )
            self._storage.decay_recent_activity(factor)
            self._last_decay_at = now
            if self._metrics is not None:
                self._record(
                    "decay",
                    decayed=decayed,
                    pruned=pruned,
                    weights=self._storage.weight_summary(),
                )

        # 10. Global stability check
        all_engrams = self._storage.list_engrams(status=EngramStatus.ACTIVE)
        total = len(all_engrams)
        window = self._config.activity_window_seconds
        active = len([e for e in all_engrams if e.last_pulse_at is not None
                      and (now - e.last_pulse_at).total_seconds() < window])
        ratio = active / total if total else 0.0
        coherent = self._coherent_focus(all_engrams, now)
        breadth = self._breadth(all_engrams, now)
        self._record(
            "heartbeat",
            active=active,
            total=total,
            ratio=round(ratio, 4),
            coherent=round(coherent, 4),
            breadth=round(breadth, 4),
            pending=len(self._pending_events),
        )
        self._record_topology(all_engrams)
        self._record_connectivity(all_engrams, now)
        if self._claustrum is not None and total > 0:
            self._claustrum.observe_mind_tide(
                ratio, coherent=coherent, breadth=breadth)
        advice = self._runtime.check_global_stability(active, total)
        if advice == StabilityAdvice.REDUCE_SPONTANEOUS:
            self._spontaneous_modifier = max(0.1, self._spontaneous_modifier * 0.5)
            _logger.debug(
                "stability: reduce spontaneous (heartbeat %d/%d, modifier %.2f)",
                active, total, self._spontaneous_modifier,
            )
        elif advice == StabilityAdvice.INCREASE_SPONTANEOUS:
            self._spontaneous_modifier = min(5.0, self._spontaneous_modifier * 1.5)
            _logger.debug(
                "stability: increase spontaneous (heartbeat %d/%d, modifier %.2f)",
                active, total, self._spontaneous_modifier,
            )
        else:
            self._spontaneous_modifier = max(1.0, self._spontaneous_modifier * 0.95)

        return results

    def propagate(self, source_id: str, output_content: str) -> None:
        """Public propagation interface for testing."""
        self._propagate(source_id, output_content)

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main event loop.

        tick() performs blocking LLM and SQLite calls, so it runs in a worker
        thread — the event loop stays responsive for the front agent and
        clone sessions while the engine works. Ticks remain strictly serial
        (one at a time); Storage serializes cross-thread access with a lock.
        """
        while not stop_event.is_set():
            await asyncio.to_thread(self.tick)
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self._config.tick_interval,
                )
            except asyncio.TimeoutError:
                pass

    # ── Internal ─────────────────────────────────────────────────

    def _select_events(
        self,
        now: datetime,
        budget: int,
        *,
        excluded_engrams: set[str] | None = None,
    ) -> tuple[list[PulseEvent], dict[str, CenterReservation]]:
        """Select this tick and, when configured, durably reserve every slot."""

        excluded = excluded_engrams or set()
        gate = self._config.inhibition_propagation_gate

        def propagation_is_gated(event: PulseEvent) -> bool:
            if gate <= 0 or event.reason is not PulseReason.PROPAGATION:
                return False
            inhibition = self._inhibition_level(event.engram_id, now)
            factor = gate * self._gate_mods.get(event.engram_id, 1.0)
            return (
                inhibition > 0
                and random.random() >= 1.0 / (1.0 + factor * inhibition)
            )

        if self._scheduler is None:
            # Historical Rule 7 compatibility. Sorting is stable, so equal
            # priorities retain their original queue order.
            self._pending_events.sort(key=lambda event: event.priority, reverse=True)
            to_execute: list[PulseEvent] = []
            deferred: list[PulseEvent] = []
            selected_engrams: set[str] = set()
            for event in self._pending_events:
                if event.engram_id in excluded:
                    deferred.append(event)
                    continue
                # Propagation rejected by the inhibition gate is dropped from
                # this in-memory tick cache.
                if propagation_is_gated(event):
                    continue
                if (
                    len(to_execute) < budget
                    and event.engram_id not in selected_engrams
                ):
                    selected_engrams.add(event.engram_id)
                    to_execute.append(event)
                else:
                    deferred.append(event)
            self._pending_events = deferred
            return to_execute, {}

        from pulse_system.core.runtime.scheduling import CenterAdmissionCandidate

        schedulable: list[PulseEvent] = []
        gated_objects: set[int] = set()
        for event in self._pending_events:
            if event.engram_id in excluded:
                continue
            if propagation_is_gated(event):
                gated_objects.add(id(event))
                continue
            if event.event_id is None:
                raise RuntimeError("durable scheduler candidate has no event_id")
            schedulable.append(event)
        event_by_id = {event.event_id: event for event in schedulable}
        if len(event_by_id) != len(schedulable):
            raise RuntimeError("durable scheduler candidates contain duplicate events")

        plan = self._scheduler.plan(
            [
                CenterAdmissionCandidate(
                    event_id=event.event_id,
                    engram_id=event.engram_id,
                    center_id=event.center_id,
                    priority=event.priority,
                    created_at=event.created_at,
                )
                for event in schedulable
            ],
            budget=budget,
            tick=self._tick_count,
        )
        admitted_ids = tuple(plan.admitted_event_ids)
        if len(admitted_ids) != len(set(admitted_ids)):
            raise RuntimeError("scheduler returned duplicate admitted events")
        unknown = [event_id for event_id in admitted_ids if event_id not in event_by_id]
        if unknown:
            raise RuntimeError(f"scheduler returned unknown event ids: {unknown!r}")
        if len(admitted_ids) > budget:
            raise RuntimeError("scheduler exceeded the tick budget")
        to_execute = [event_by_id[event_id] for event_id in admitted_ids]
        if len({event.engram_id for event in to_execute}) != len(to_execute):
            raise RuntimeError("scheduler admitted more than one event per Engram")

        reservations: dict[str, CenterReservation] = {}
        for admission in plan.admissions:
            event_id = admission.candidate.event_id
            if admission.reservation.event_id != event_id:
                raise RuntimeError("scheduler admission and reservation disagree")
            if event_id in reservations:
                raise RuntimeError("scheduler returned duplicate reservations")
            reservations[event_id] = admission.reservation
        if set(reservations) != set(admitted_ids):
            raise RuntimeError("every admitted event must have exactly one reservation")

        selected = set(admitted_ids)
        self._pending_events = [
            event
            for event in self._pending_events
            if id(event) not in gated_objects and event.event_id not in selected
        ]
        lane_counts = {"work": 0, "life": 0, "unbound": 0}
        for reservation in reservations.values():
            lane_counts[reservation.lane.value] += 1
        self._record(
            "center_admission_planned",
            tick=plan.tick,
            budget=plan.budget,
            eligible=plan.eligible_count,
            admitted=len(admitted_ids),
            deferred=plan.deferred_count,
            work=lane_counts["work"],
            life=lane_counts["life"],
            unbound=lane_counts["unbound"],
        )
        return to_execute, reservations

    def _refresh_durable_event_cache(self) -> None:
        """Rebuild scheduling state from durable QUEUED events."""

        if self._causal_ledger is None:
            return
        observed_at = _now()

        def cache_events(
            events: list[CausalEvent],
            sealed_ids: set[str],
        ) -> tuple[set[str], set[str], set[str]]:
            active_ids: set[str] = set()
            blocked_ids: set[str] = set()
            dendrite_ids: set[str] = set()
            for event in events:
                if event.engram_id is None or event.world_id != self._world_id:
                    continue
                flow_violations = causal_turn_violation_codes(event)
                if flow_violations or type(event.seq) is not int:
                    blocked_ids.add(event.id)
                    if event.id not in self._flow_blocked_event_ids:
                        self._record(
                            "causal_flow_blocked",
                            event_id=event.id,
                            violations=(
                                list(flow_violations)
                                if flow_violations
                                else ["missing_event_sequence"]
                            ),
                        )
                    continue
                if self._is_generation_control_event(event):
                    # A generation summary is consumed synchronously by
                    # EngramManager after its transition reaches SUMMARIZING.
                    # If the process dies after enqueue but before begin_turn,
                    # startup recovery marks the transition UNCERTAIN. Leaving
                    # this root visible but unclaimable is safer than replay.
                    continue
                engram = self._storage.get_engram(event.engram_id)
                if engram is None or engram.status is not EngramStatus.ACTIVE:
                    continue
                active_ids.add(event.id)
                if (
                    event.id in sealed_ids
                    or event.metadata.get("dendritic_integration_version") == 1
                ):
                    continue
                try:
                    window_policy = (
                        self._causal_ledger.ensure_dendritic_input_policy(
                            event.id,
                            self._dendrite.window_policy_snapshot(
                                event.engram_id
                            ),
                            runtime_fence=self._runtime_fence,
                        )
                    )
                except DendriticWindowConflictError as exc:
                    self._record(
                        "dendritic_policy_conflict",
                        event_id=event.id,
                        formation_engram=event.engram_id,
                        reason=str(exc),
                    )
                    continue
                projection = PulseEvent.from_causal_event(event)
                self._dendrite.receive_durable_event(
                    engram_id=event.engram_id,
                    event_id=event.id,
                    content=event.content,
                    causal_id=event.causal_id,
                    parent_event_id=event.parent_event_id,
                    priority=projection.priority,
                    event_seq=event.seq,
                    window_policy=window_policy,
                    arrived_at=event.created_at,
                )
                dendrite_ids.add(event.id)
            return active_ids, blocked_ids, dendrite_ids

        queued = self._causal_ledger.list_events(
            status=CausalEventStatus.QUEUED,
            limit=500,
        )
        sealed_ids = self._causal_ledger.dendritic_window_event_ids(
            event.id for event in queued
        )
        active_queued_ids, currently_blocked, dendrite_ids = cache_events(
            queued,
            sealed_ids,
        )
        self._dendrite.retain_event_ids(dendrite_ids)
        ready_windows = self._dendrite.get_ready_event_windows(observed_at)
        ready_ids = {
            event_id
            for window in ready_windows
            for event_id in window.event_ids
        }
        ready_ids.update(sealed_ids)
        ready_ids.update(
            event.id
            for event in queued
            if event.id in active_queued_ids
            and event.metadata.get("dendritic_integration_version") == 1
        )
        materialized_ids = self._materialize_ready_dendritic_integrations(
            queued,
            ready_windows,
        )
        if materialized_ids:
            # The transaction reconciled member roots and created new queued
            # roots. Rebuild from SQLite immediately so this same tick can
            # schedule the already-closed window without a second wait.
            queued = self._causal_ledger.list_events(
                status=CausalEventStatus.QUEUED,
                limit=500,
            )
            sealed_ids = self._causal_ledger.dendritic_window_event_ids(
                event.id for event in queued
            )
            active_queued_ids, currently_blocked, dendrite_ids = cache_events(
                queued,
                sealed_ids,
            )
            self._dendrite.retain_event_ids(dendrite_ids)
            ready_windows = self._dendrite.get_ready_event_windows(observed_at)
            ready_ids = {
                event_id
                for window in ready_windows
                for event_id in window.event_ids
            }
            ready_ids.update(sealed_ids)
            ready_ids.update(
                event.id
                for event in queued
                if event.id in active_queued_ids
                and event.metadata.get("dendritic_integration_version") == 1
            )
        self._pending_events = [
            PulseEvent.from_causal_event(event)
            for event in queued
            if event.engram_id is not None
            and event.id in ready_ids
            and event.id in active_queued_ids
        ]
        self._flow_blocked_event_ids = currently_blocked

    def _materialize_ready_dendritic_integrations(
        self,
        queued: list[CausalEvent],
        ready_windows: tuple[DendriticReadyWindow, ...],
    ) -> set[str]:
        """Seal each cohort and atomically form every eligible nexus."""

        if self._causal_ledger is None or not ready_windows:
            return set()
        by_id = {event.id: event for event in queued}
        aggregate_ids: set[str] = set()
        for window in ready_windows:
            grouped: dict[
                tuple[str | None, str],
                list[tuple[CausalEvent, str]],
            ] = {}
            for event_id in window.event_ids:
                event = by_id.get(event_id)
                if event is None or event.engram_id is None:
                    continue
                signature = self._causal_ledger.dendritic_candidate_signature(
                    event
                )
                if signature is None:
                    continue
                delivery_class, source_identity = signature
                grouped.setdefault(
                    (event.center_id, delivery_class),
                    [],
                ).append((event, source_identity))

            integration_groups: list[tuple[str, ...]] = []
            for entries in grouped.values():
                entries.sort(
                    key=lambda item: (
                        item[0].seq if item[0].seq is not None else 2**63,
                        item[0].id,
                    )
                )
                if len({source for _event, source in entries}) < 2:
                    continue
                remaining = list(entries)
                while len(remaining) >= 2:
                    chunk = remaining[:MAX_DENDRITIC_INTEGRATION_MEMBERS]
                    if len({source for _event, source in chunk}) < 2:
                        first_source = chunk[0][1]
                        replacement_index = next(
                            (
                                index
                                for index in range(
                                    MAX_DENDRITIC_INTEGRATION_MEMBERS,
                                    len(remaining),
                                )
                                if remaining[index][1] != first_source
                            ),
                            None,
                        )
                        if replacement_index is None:
                            break
                        selected_indices = {
                            *range(MAX_DENDRITIC_INTEGRATION_MEMBERS - 1),
                            replacement_index,
                        }
                        chunk = [
                            entry
                            for index, entry in enumerate(remaining)
                            if index in selected_indices
                        ]
                        remaining = [
                            entry
                            for index, entry in enumerate(remaining)
                            if index not in selected_indices
                        ]
                    else:
                        remaining = remaining[len(chunk):]
                    integration_groups.append(
                        tuple(event.id for event, _source in chunk)
                    )
            try:
                durable_window, results = (
                    self._causal_ledger.materialize_dendritic_window(
                        window,
                        integration_groups,
                        runtime_fence=self._runtime_fence,
                    )
                )
            except DendriticWindowConflictError as exc:
                self._record(
                    "dendritic_window_conflict",
                    formation_engram=window.engram_id,
                    members=len(window.event_ids),
                    reason=str(exc),
                )
                continue
            self._record(
                "dendritic_window_closed",
                window=durable_window.id,
                formation_engram=durable_window.formation_engram_id,
                members=durable_window.event_count,
                policy=durable_window.policy_version,
                opened_at=durable_window.window_opened_at.isoformat(),
                closed_at=durable_window.window_closed_at.isoformat(),
            )
            for integration, aggregate in results:
                aggregate_ids.add(aggregate.id)
                self._record(
                    "dendritic_convergence_materialized",
                    integration=integration.id,
                    aggregate=aggregate.id,
                    formation_engram=integration.formation_engram_id,
                    center=integration.center_id,
                    delivery_class=integration.delivery_class,
                    members=integration.member_count,
                    member_set_sha256=integration.member_set_sha256,
                    content_sha256=integration.content_sha256,
                )
        return aggregate_ids

    def _is_generation_control_event(self, event: CausalEvent) -> bool:
        """Return true for manager-owned generation summary roots."""

        if self._causal_ledger is None:
            return False
        generation_id = event.metadata.get("generation_id")
        stage = event.metadata.get("generation_stage")
        if not isinstance(generation_id, str) or stage != "summary":
            return False
        # Missing generation metadata is a corruption signal.  Fail closed in
        # the same way as an uncertain transition rather than treating the
        # root as an ordinary self stimulus.
        return True

    def _record(self, event_type: str, **payload) -> None:
        if self._metrics is not None:
            self._metrics.record(event_type, **payload)

    def _record_topology(self, engrams: list) -> None:
        """Full graph dump every topology_interval_ticks ticks (runtime metrics).

        Edges reference node positions in the `engrams` array rather than
        repeating 36-char uuids: at a few hundred nodes and a few thousand
        edges that is the difference between a ~60KB and a ~270KB line, and
        the snapshot has to be cheap enough to sit in the same JSONL as the
        per-tick stream. Archived-only edges (endpoint since archived) are
        dropped — a node index must resolve.
        """
        interval = self._config.topology_interval_ticks
        if (
            self._metrics is None
            or interval is None
            or interval <= 0
            or self._tick_count % interval != 0
        ):
            return

        index = {e.id: i for i, e in enumerate(engrams)}
        edges = [
            [
                index[c.from_id],
                index[c.to_id],
                round(c.weight, 4),
                "i" if c.conn_type == ConnectionType.INHIBITORY else "e",
            ]
            for c in self._storage.list_all_connections()
            if c.from_id in index and c.to_id in index
        ]
        self._record(
            "topology",
            tick=self._tick_count,
            engrams=[
                {
                    "id": e.id,
                    "project": e.project_id,
                    "activity": round(e.metadata.recent_activity, 4),
                    "pulses": e.total_pulses,
                }
                for e in engrams
            ],
            edges=edges,
        )

    def _record_connectivity(self, engrams: list, now: datetime) -> None:
        """Record current content-graph structure without feeding it back.

        The Engine is the only layer that can see the claustrum's current
        per-source threshold factors and target-side inhibition/gate state.
        This event is therefore stronger evidence than a SQLite-only replay
        projection, while remaining a content-free Metrics sideband.
        """

        interval = self._config.connectivity_interval_ticks
        if (
            self._metrics is None
            or interval is None
            or self._tick_count % interval != 0
        ):
            return

        gate = self._config.inhibition_propagation_gate
        target_acceptance: dict[str, float] = {}
        for engram in engrams:
            inhibition = self._inhibition_level(engram.id, now)
            gate_factor = gate * self._gate_mods.get(engram.id, 1.0)
            target_acceptance[engram.id] = 1.0 / (
                1.0 + gate_factor * inhibition
            )

        snapshot = analyze_connectivity(
            [engram.id for engram in engrams],
            self._storage.list_all_connections(),
            base_threshold=self._config.propagation_threshold,
            threshold_factors=self._propagation_mods,
            target_gate_acceptance=target_acceptance,
            evidence_class="runtime_effective_threshold_projection",
        )
        self._record("connectivity", tick=self._tick_count, **snapshot)

    def _coherent_focus(self, engrams: list, now: datetime) -> float:
        """Coherent focus: is ONE purpose cluster both assembled and dominant?

        The purpose cluster is the Engram's Project.
        Among engrams active in the activity window, this is the top project's
        share — but 0 unless that project has >=2 members co-active (a cluster
        with fewer than two active members does not form a focused thought). This is
        the signal the claustrum maximizes in reward_mode="coherent" — no
        hardcoded target band.
        """
        window = self._config.activity_window_seconds
        active = [e for e in engrams if e.last_pulse_at is not None
                  and (now - e.last_pulse_at).total_seconds() < window]
        if len(active) < 2:
            return 0.0
        counts: dict[str | None, int] = {}
        for e in active:
            counts[e.project_id] = counts.get(e.project_id, 0) + 1
        top = max(counts.values())
        if top < 2:
            return 0.0  # nothing assembled — no focused thought
        return top / len(active)

    def _breadth(self, engrams: list, now: datetime) -> float:
        """Breadth: how much of the purpose space is currently in play.

        Distinct Projects represented among the active engrams / total
        Projects. This is the retrieval side of the tradeoff — it rises with
        the mind-tide, exactly opposite to _coherent_focus, which is why the
        two together define the tradeoff the claustrum balances on demand.
        """
        window = self._config.activity_window_seconds
        total_projects = {e.project_id for e in engrams}
        if not total_projects:
            return 0.0
        active_projects = {
            e.project_id for e in engrams
            if e.last_pulse_at is not None
            and (now - e.last_pulse_at).total_seconds() < window
        }
        return len(active_projects) / len(total_projects)

    def _apply_claustrum(self, now: datetime) -> None:
        if self._claustrum is None:
            return
        from pulse_system.core.claustrum.modulator import EngramState

        engrams = self._storage.list_engrams(status=EngramStatus.ACTIVE)
        if not engrams:
            return
        cluster_acts: dict[str | None, list[float]] = {}
        for e in engrams:
            cluster_acts.setdefault(e.project_id, []).append(
                e.metadata.recent_activity
            )
        states = []
        for e in engrams:
            acts = cluster_acts.get(e.project_id, [])
            states.append(EngramState(
                engram_id=e.id,
                recent_activity=e.metadata.recent_activity,
                connection_count=self._storage.count_connections(e.id),
                queue_depth=self._dendrite.get_queue_size(e.id),
                seconds_since_pulse=(
                    (now - e.last_pulse_at).total_seconds()
                    if e.last_pulse_at else 1e9
                ),
                cluster_activity=sum(acts) / len(acts) if acts else 0.0,
            ))
        mods = self._claustrum.modulate(states)
        self._activity_mods = {eid: m[0] for eid, m in mods.items()}
        self._dendrite.set_wait_modifiers(
            {eid: m[1] for eid, m in mods.items()}
        )
        self._propagation_mods = {eid: m[2] for eid, m in mods.items()}
        self._gate_mods = {eid: m[3] for eid, m in mods.items()}

    def _record_resonance(self, activations: list[tuple[str, datetime]]) -> None:
        """Same-Project vs cross-Project co-activation pairs this tick (runtime metrics)."""
        if self._metrics is None:
            return
        projects: dict[str, str | None] = {}
        for eid, _ in activations:
            if eid not in projects:
                engram = self._storage.get_engram(eid)
                projects[eid] = engram.project_id if engram else None
        ids = [eid for eid, _ in activations]
        same = cross = 0
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                a, b = projects[ids[i]], projects[ids[j]]
                if a is not None and a == b:
                    same += 1
                else:
                    cross += 1
        self._record("resonance", same_project_pairs=same, cross_pairs=cross)

    def _record_resonance_pairs(
        self,
        pairs: list[
            tuple[tuple[str, datetime], tuple[str, datetime]]
        ],
    ) -> None:
        """Record exactly the new STDP pairs formed by this reap batch."""

        if self._metrics is None or not pairs:
            return
        projects: dict[str, str | None] = {}
        same = cross = 0
        for left, right in pairs:
            for engram_id in (left[0], right[0]):
                if engram_id not in projects:
                    engram = self._storage.get_engram(engram_id)
                    projects[engram_id] = engram.project_id if engram else None
            left_project = projects[left[0]]
            right_project = projects[right[0]]
            if left_project is not None and left_project == right_project:
                same += 1
            else:
                cross += 1
        self._record("resonance", same_project_pairs=same, cross_pairs=cross)

    def _learn_from_activations(
        self,
        activations: list[tuple[str, datetime]],
    ) -> None:
        """Apply each cross-/same-tick co-activation pair exactly once."""

        if not activations:
            return
        current = sorted(activations, key=lambda item: item[1])
        window = max(0.0, self._connections.config.coactivation_window)
        pairs: list[
            tuple[tuple[str, datetime], tuple[str, datetime]]
        ] = []

        for index, activation in enumerate(current):
            candidates = [*self._recent_activations, *current[:index]]
            for prior in candidates:
                if prior[0] == activation[0]:
                    continue
                if abs((activation[1] - prior[1]).total_seconds()) > window:
                    continue
                ordered = (
                    (prior, activation)
                    if prior[1] <= activation[1]
                    else (activation, prior)
                )
                self._connections.stdp_update([ordered[0], ordered[1]])
                pairs.append(ordered)

        all_recent = [*self._recent_activations, *current]
        latest = max(timestamp for _engram_id, timestamp in all_recent)
        self._recent_activations = [
            activation
            for activation in all_recent
            if (latest - activation[1]).total_seconds() <= window
        ]
        self._record_resonance_pairs(pairs)

    def _run_one(
        self,
        event: PulseEvent,
        reservation: CenterReservation | None,
    ):
        """Execute one Harness attempt inside a worker slot."""

        start = _now()
        try:
            if self._causal_ledger is not None:
                runtime_fence = self._fence_for_reservation(reservation)
                return (
                    event,
                    start,
                    self._engram_mgr.pulse(
                        event.engram_id,
                        pulse_event=event,
                        causal_retry_allowed=(
                            event.attempts < self._config.max_pulse_retries
                        ),
                        runtime_fence=runtime_fence,
                    ),
                    None,
                )
            return (
                event,
                start,
                self._engram_mgr.pulse(event.engram_id),
                None,
            )
        except (ValueError, LLMCallError, HarnessError) as error:
            return (event, start, None, error)
        except Exception as error:  # noqa: BLE001 - reservation settles on owner thread
            return (event, start, None, error)

    def _fence_for_reservation(
        self,
        reservation: CenterReservation | None,
    ) -> RuntimeFence | None:
        """Preserve Runtime's revocable publication generation.

        Direct/offline engines retain the historical owner/epoch-only fence
        derived from a Center reservation.  A Runtime-owned engine always
        returns its fixed permit-bearing fence and treats a reservation from
        another owner epoch as corruption rather than silently weakening the
        capability.
        """

        runtime_fence = self._runtime_fence
        if runtime_fence is not None:
            if reservation is not None and (
                reservation.owner_id != runtime_fence.owner_id
                or reservation.lease_epoch != runtime_fence.epoch
            ):
                raise RuntimeLeaseError(
                    owner_id=runtime_fence.owner_id,
                    epoch=runtime_fence.epoch,
                    reason="center_reservation_owner_mismatch",
                    lease=None,
                )
            return runtime_fence
        if reservation is None:
            return None
        return RuntimeFence(
            owner_id=reservation.owner_id,
            epoch=reservation.lease_epoch,
        )

    def _dispatch_events(
        self,
        events: list[PulseEvent],
        reservations_by_event: dict[str, CenterReservation],
    ) -> None:
        """Submit admitted attempts without waiting for Harness completion."""

        for event in events:
            reservation = (
                None
                if event.event_id is None
                else reservations_by_event.get(event.event_id)
            )
            if self._scheduler is not None and reservation is None:
                raise RuntimeError(
                    f"scheduled event {event.event_id!r} has no reservation"
                )
            try:
                with self._shutdown_lock:
                    executor = self._pulse_executor
                    if executor is None:
                        raise RuntimeError("pulse worker fleet is closed")
                    future = executor.submit(self._run_one, event, reservation)
                    self._inflight[future] = (event, reservation)
            except RuntimeError as error:
                # No Harness call was accepted. The durable event remains
                # QUEUED and may be offered again after the reservation slot
                # is released as skipped.
                dispatch_error = ValueError(f"pulse dispatch rejected: {error}")
                self._process_attempts(
                    [(event, _now(), None, dispatch_error)],
                    reservations_by_event,
                )
                self._record(
                    "pulse_dispatch_rejected",
                    engram=event.engram_id,
                    event_id=event.event_id,
                )
                continue
            self._record(
                "pulse_dispatched",
                engram=event.engram_id,
                event_id=event.event_id,
                running=len(self._inflight),
                limit=self._config.max_parallel_pulses,
            )

    def _reap_completed(
        self,
        *,
        require_done: bool = False,
    ) -> tuple[list[tuple[str, str]], list[tuple[str, datetime]], list[Exception]]:
        """Finalize worker attempts on the single coordinator thread."""

        attempts = []
        reservations: dict[str, CenterReservation] = {}
        with self._shutdown_lock:
            for future, (event, reservation) in list(self._inflight.items()):
                if not future.done():
                    if require_done:
                        raise RuntimeError("pulse worker did not stop during shutdown")
                    continue
                self._inflight.pop(future, None)
                try:
                    attempt = future.result()
                except CancelledError:
                    attempt = (
                        event,
                        _now(),
                        None,
                        ValueError("pulse dispatch cancelled before execution"),
                    )
                attempts.append(attempt)
                if event.event_id is not None and reservation is not None:
                    reservations[event.event_id] = reservation
        return self._process_attempts(attempts, reservations)

    def _process_attempts(
        self,
        attempts,
        reservations_by_event: dict[str, CenterReservation],
    ) -> tuple[list[tuple[str, str]], list[tuple[str, datetime]], list[Exception]]:
        """Apply coordinator-owned bookkeeping for completed Harness calls."""

        activations: list[tuple[str, datetime]] = []
        results: list[tuple[str, str]] = []
        unexpected_errors: list[Exception] = []

        for event, pulse_time, pulse_result, error in attempts:
            reservation = (
                None
                if event.event_id is None
                else reservations_by_event.get(event.event_id)
            )
            if self._scheduler is not None and reservation is None:
                raise RuntimeError(
                    f"scheduled event {event.event_id!r} has no reservation"
                )
            if reservation is not None:
                if pulse_result is not None:
                    outcome = CenterReservationOutcome.SUCCEEDED
                elif isinstance(error, (LLMCallError, HarnessError)):
                    outcome = CenterReservationOutcome.FAILED
                elif isinstance(error, ValueError):
                    outcome = CenterReservationOutcome.SKIPPED
                else:
                    outcome = CenterReservationOutcome.UNCERTAIN
                settled = self._scheduler.settle(reservation.id, outcome)
                self._record(
                    "center_reservation_settled",
                    reservation=settled.id,
                    event=settled.event_id,
                    center=settled.center_id,
                    lane=settled.lane.value,
                    outcome=settled.outcome.value,
                )
            if error is not None:
                if isinstance(error, (LLMCallError, HarnessError)):
                    self._handle_pulse_failure(event, error)
                elif not isinstance(error, ValueError):
                    unexpected_errors.append(error)
                continue

            self._recover_failure_domain(event.engram_id)
            _logger.debug(
                "pulse %s reason=%s prio=%.2f in=%d out=%d cached=%d",
                event.engram_id,
                event.reason.value,
                event.priority,
                pulse_result.input_tokens,
                pulse_result.output_tokens,
                pulse_result.cached_tokens,
            )
            if event.reason == PulseReason.PROPAGATION:
                depth = (
                    event.depth
                    if self._causal_ledger is not None
                    else self._incoming_depth.pop(event.engram_id, 0)
                )
            else:
                depth = 0
                self._incoming_depth.pop(event.engram_id, None)
            self._pulse_depth[event.engram_id] = depth
            self._record(
                "pulse",
                engram=event.engram_id,
                reason=event.reason.value,
                priority=round(event.priority, 4),
                depth=depth,
                input_tokens=pulse_result.input_tokens,
                output_tokens=pulse_result.output_tokens,
                cached_tokens=pulse_result.cached_tokens,
            )
            results.append((event.engram_id, pulse_result.content))
            activation_id = event.engram_id

            # Internal (null-flow) thought may create a *new* CONTENT
            # delivery. TUNNEL has an addressed return path and SPECTRUM is a
            # non-executable sideband; neither may diffuse over content edges.
            if may_emit_content_propagation(event):
                self._propagate(
                    event.engram_id,
                    pulse_result.content,
                    source_event=event,
                    parent_event_id=(
                        pulse_result.result_event_id or event.event_id
                    ),
                    causal_id=pulse_result.causal_id or event.causal_id,
                )

            self._runtime.consume_budget(
                pulse_result.input_tokens,
                pulse_result.output_tokens,
                pulse_result.cached_tokens,
                pulse_result.cache_write_tokens,
            )

            threshold = self._config.succession_token_threshold
            if threshold is not None:
                current = self._storage.get_engram(event.engram_id)
                if current is not None and current.metadata.token_count >= threshold:
                    parent_event_id = (
                        pulse_result.result_event_id or event.event_id
                    )
                    runtime_fence = self._fence_for_reservation(reservation)
                    if self._config.background_dispatch:
                        self._request_succession(
                            event.engram_id,
                            parent_event_id=parent_event_id,
                            runtime_fence=runtime_fence,
                        )
                    else:
                        try:
                            if (
                                self._causal_ledger is not None
                                and self._has_queued_successor_conflict(
                                    event.engram_id,
                                    current_event_id=event.event_id,
                                )
                            ):
                                self._record(
                                    "succession_deferred",
                                    old=event.engram_id,
                                    reason="queued_predecessor_event",
                                )
                            else:
                                activation_id = self._run_succession(
                                    event.engram_id,
                                    parent_event_id=parent_event_id,
                                    runtime_fence=runtime_fence,
                                )
                        except HarnessError as succession_error:
                            self._handle_succession_error(
                                event.engram_id,
                                succession_error,
                            )
                        except (
                            CausalTransitionError,
                            SuccessionPreparationError,
                        ) as succession_error:
                            self._handle_succession_error(
                                event.engram_id,
                                succession_error,
                            )
            activations.append((activation_id, pulse_time))

        return results, activations, unexpected_errors

    def _request_succession(
        self,
        predecessor_id: str,
        *,
        parent_event_id: str | None,
        runtime_fence: RuntimeFence | None,
    ) -> None:
        """Record one threshold crossing without submitting unbounded work."""

        durable_blocked = (
            set()
            if self._causal_ledger is None
            else self._causal_ledger.generation_blocked_predecessors()
        )
        with self._succession_lock:
            pending_request = self._succession_pending.get(predecessor_id)
            already_running = any(
                request.predecessor_id == predecessor_id
                for request in self._succession_inflight.values()
            )
            if pending_request is not None:
                # Capacity saturation may let this subject finish more turns
                # before a succession slot opens.  Preserve the original
                # queue age, but advance the causal parent to the latest
                # settled result from the same Runtime epoch.
                if pending_request.runtime_fence == runtime_fence:
                    self._succession_pending[predecessor_id] = (
                        _SuccessionRequest(
                            predecessor_id=predecessor_id,
                            parent_event_id=parent_event_id,
                            runtime_fence=runtime_fence,
                            requested_at=pending_request.requested_at,
                        )
                    )
                self._record(
                    "succession_deferred",
                    old=predecessor_id,
                    reason="already_requested",
                )
                return
            if (
                already_running
                or predecessor_id in durable_blocked
            ):
                self._record(
                    "succession_deferred",
                    old=predecessor_id,
                    reason=(
                        "generation_blocked"
                        if predecessor_id in durable_blocked and not already_running
                        else "already_requested"
                    ),
                )
                return
            self._succession_pending[predecessor_id] = _SuccessionRequest(
                predecessor_id=predecessor_id,
                parent_event_id=parent_event_id,
                runtime_fence=runtime_fence,
            )
            pending = len(self._succession_pending)
        self._record(
            "succession_requested",
            old=predecessor_id,
            parent_event_id=parent_event_id,
            pending=pending,
        )

    def _succession_excluded_engrams(self) -> set[str]:
        with self._succession_lock:
            excluded = {
                request.predecessor_id
                for request in self._succession_inflight.values()
            }
        if self._causal_ledger is not None:
            excluded.update(
                self._causal_ledger.generation_blocked_predecessors()
            )
        return excluded

    def _dispatch_pending_successions(self) -> None:
        with self._shutdown_lock:
            executor = self._succession_executor
            if executor is None:
                return
            pulse_running = {
                event.engram_id for event, _reservation in self._inflight.values()
            }
        dispatched: list[tuple[Future, _SuccessionRequest]] = []
        with self._shutdown_lock, self._succession_lock:
            # Close may have removed the executor between the read above and
            # this admission boundary.
            executor = self._succession_executor
            if executor is None:
                return
            available = max(
                0,
                self._config.max_parallel_successions
                - len(self._succession_inflight),
            )
            for predecessor_id, request in list(
                self._succession_pending.items()
            ):
                if available <= 0:
                    break
                if predecessor_id in pulse_running:
                    continue
                try:
                    future = executor.submit(
                        self._engram_mgr.prepare_succession,
                        predecessor_id,
                        parent_event_id=request.parent_event_id,
                        runtime_fence=request.runtime_fence,
                    )
                except RuntimeError:
                    break
                self._succession_pending.pop(predecessor_id, None)
                self._succession_inflight[future] = request
                dispatched.append((future, request))
                available -= 1
            pending = len(self._succession_pending)
            running = len(self._succession_inflight)
        for _future, request in dispatched:
            self._record(
                "succession_dispatched",
                old=request.predecessor_id,
                running=running,
                limit=self._config.max_parallel_successions,
            )
        if pending and running >= self._config.max_parallel_successions:
            self._record(
                "succession_workers_saturated",
                pending=pending,
                running=running,
                limit=self._config.max_parallel_successions,
            )

    def _reap_successions(self, *, require_done: bool = False) -> None:
        """Commit completed preparations on the one coordinator thread."""

        with self._succession_lock:
            entries = list(self._succession_inflight.items())
        for future, request in entries:
            if not future.done():
                if require_done:
                    raise RuntimeError(
                        "succession worker did not stop during shutdown"
                    )
                continue
            with self._succession_lock:
                self._succession_inflight.pop(future, None)
            try:
                preparation = future.result()
            except CancelledError:
                self._record(
                    "succession_failed",
                    old=request.predecessor_id,
                    code="succession_dispatch_cancelled",
                    phase="dispatch",
                    retryable=True,
                    prompt_accepted=False,
                )
                continue
            except Exception as error:  # isolate this lineage, not process signals
                self._handle_succession_error(request.predecessor_id, error)
                continue
            if not isinstance(preparation, SuccessionPreparation):
                self._handle_succession_error(
                    request.predecessor_id,
                    TypeError("succession worker returned an invalid preparation"),
                    charge_usage=False,
                )
                continue

            self._runtime.consume_budget(
                preparation.input_tokens,
                preparation.output_tokens,
                preparation.cached_tokens,
                preparation.cache_write_tokens,
            )
            self._record(
                "succession_prepared",
                old=preparation.predecessor_id,
                new=preparation.successor_id,
                generation=preparation.generation_id,
            )
            try:
                result = self._engram_mgr.commit_succession(
                    preparation,
                    runtime_fence=request.runtime_fence,
                )
            except Exception as error:  # isolate this lineage, not process signals
                self._handle_succession_error(
                    request.predecessor_id,
                    error,
                    charge_usage=False,
                )
                continue
            self._apply_succession_result(
                preparation.predecessor_id,
                result,
            )

    def _handle_succession_error(
        self,
        predecessor_id: str,
        error: Exception,
        *,
        charge_usage: bool = True,
    ) -> None:
        if charge_usage and isinstance(
            error,
            (SuccessionHarnessError, SuccessionPreparationError),
        ):
            usage = error.claim_usage()
            if usage is not None:
                self._runtime.consume_budget(
                    usage.input_tokens,
                    usage.output_tokens,
                    usage.cached_tokens,
                    usage.cache_write_tokens,
                )
        classified_error = (
            error.cause
            if isinstance(error, SuccessionPreparationError)
            else error
        )
        if isinstance(classified_error, HarnessError):
            code = classified_error.code
            phase = classified_error.phase
            retryable = classified_error.retryable
            prompt_accepted = classified_error.prompt_accepted
        elif isinstance(classified_error, CausalTransitionError):
            code = "generation_transition_error"
            phase = "generation"
            retryable = False
            prompt_accepted = None
        else:
            code = type(classified_error).__name__.casefold()
            phase = "succession"
            retryable = False
            prompt_accepted = None
        _logger.error(
            "succession for %s failed without automatic retry: %s",
            predecessor_id,
            error,
        )
        self._record(
            "succession_failed",
            old=predecessor_id,
            code=code,
            phase=phase,
            retryable=retryable,
            prompt_accepted=prompt_accepted,
        )
        if isinstance(classified_error, RuntimeLeaseError):
            # Usage and the local failure event are now settled, but lease loss
            # is a process-level stop signal rather than an isolated lineage
            # failure. Preserve the original typed error so RuntimeService can
            # quiesce immediately and reject every subsequent ingress.
            raise classified_error

    def _execute_events(
        self,
        events: list[PulseEvent],
        reservations_by_event: dict[str, CenterReservation],
    ):
        """Run this tick's pulses, optionally in parallel.

        Returns [(event, start_time, PulseResult | None, error | None), ...]
        in the original (priority) order. Only the potentially blocking
        pulse() call is parallelized — Storage is lock-serialized and each
        Harness serializes its own Engram; propagation, budget accounting, and
        succession run serially in tick(). Activation timestamps are pulse
        start times, so parallel pulses register as genuine co-activations for
        STDP.
        """
        def run_one(event: PulseEvent):
            reservation = (
                None
                if event.event_id is None
                else reservations_by_event.get(event.event_id)
            )
            return self._run_one(event, reservation)

        workers = self._config.max_parallel_pulses
        if workers <= 1 or len(events) <= 1:
            return [run_one(ev) for ev in events]

        with ThreadPoolExecutor(max_workers=min(workers, len(events))) as pool:
            return list(pool.map(run_one, events))

    def _cooling_engrams(self, now: datetime) -> set[str]:
        with self._failure_domain_lock:
            return {
                engram_id
                for engram_id, state in self._failure_domains.items()
                if state.cooling_until is not None and now < state.cooling_until
            }

    def _prune_inactive_failure_domains(self) -> None:
        with self._failure_domain_lock:
            if not self._failure_domains:
                return
        active_ids = {
            engram.id
            for engram in self._storage.list_engrams(status=EngramStatus.ACTIVE)
        }
        with self._failure_domain_lock:
            inactive = set(self._failure_domains) - active_ids
        if not inactive:
            return
        with self._failure_domain_lock:
            for engram_id in inactive:
                self._failure_domains.pop(engram_id, None)

    def _release_expired_failure_domains(self, now: datetime) -> None:
        probe_ready: list[tuple[str, int]] = []
        with self._failure_domain_lock:
            for engram_id, state in self._failure_domains.items():
                if state.cooling_until is None or now < state.cooling_until:
                    continue
                self._failure_domains[engram_id] = replace(
                    state,
                    cooling_until=None,
                )
                probe_ready.append((engram_id, state.consecutive_failures))
        for engram_id, count in probe_ready:
            self._record(
                "engram_backoff_probe_ready",
                engram=engram_id,
                consecutive_failures=count,
            )

    @staticmethod
    def _safe_failure_symbol(value: object, *, fallback: str) -> str:
        if not isinstance(value, str) or not value or len(value) > 128:
            return fallback
        if not all(
            character.isascii()
            and (character.isalnum() or character in "._:-")
            for character in value
        ):
            return fallback
        return value

    def _record_failure_domain(
        self,
        engram_id: str,
        error: LLMCallError | HarnessError,
    ) -> None:
        now = _now()
        if isinstance(error, HarnessError):
            code = self._safe_failure_symbol(
                error.code,
                fallback="harness_error",
            )
            phase = self._safe_failure_symbol(
                error.phase,
                fallback="unknown",
            )
            retryable = bool(error.retryable)
            prompt_accepted = error.prompt_accepted
        else:
            code = "llm_call_error"
            phase = "provider"
            retryable = True
            prompt_accepted = None

        threshold = self._config.failure_backoff_threshold
        with self._failure_domain_lock:
            previous = self._failure_domains.get(engram_id)
            consecutive = (
                1 if previous is None else previous.consecutive_failures + 1
            )
            cooling_until = (
                now + timedelta(seconds=self._config.failure_backoff_seconds)
                if consecutive >= threshold
                else None
            )
            self._failure_domains[engram_id] = _EngramFailureDomain(
                consecutive_failures=consecutive,
                last_failure_at=now,
                cooling_until=cooling_until,
                last_error_code=code,
                last_error_phase=phase,
                error_retryable=retryable,
                prompt_accepted=prompt_accepted,
            )

        self._record(
            "engram_failure_recorded",
            engram=engram_id,
            consecutive_failures=consecutive,
            code=code,
            phase=phase,
            retryable=retryable,
            prompt_accepted=prompt_accepted,
        )
        if cooling_until is not None:
            _logger.error(
                "Engram %s reached %d consecutive Harness failures; "
                "cooling locally for %.0fs",
                engram_id,
                consecutive,
                self._config.failure_backoff_seconds,
            )
            self._record(
                "engram_backoff_started",
                engram=engram_id,
                consecutive_failures=consecutive,
                seconds=self._config.failure_backoff_seconds,
                retry_at=cooling_until.isoformat(),
            )

    def _recover_failure_domain(self, engram_id: str) -> None:
        with self._failure_domain_lock:
            previous = self._failure_domains.pop(engram_id, None)
        if previous is not None:
            self._record(
                "engram_failure_recovered",
                engram=engram_id,
                prior_consecutive_failures=previous.consecutive_failures,
            )

    def _handle_pulse_failure(
        self,
        event: PulseEvent,
        error: LLMCallError | HarnessError,
    ) -> None:
        """Requeue only retryable failures and back off after repeated faults."""

        self._record_failure_domain(event.engram_id, error)
        if self._causal_ledger is not None and event.event_id is not None:
            durable = self._causal_ledger.get_event(event.event_id)
            if durable is not None:
                event.attempts = durable.attempts
            # The Manager has already called fail_turn.  The cache must never
            # be used to requeue or drop the durable event; the next tick will
            # reconstruct it if and only if the ledger left it QUEUED.
            retryable = durable is not None and durable.status is CausalEventStatus.QUEUED
            self._record(
                "pulse_failed",
                engram=event.engram_id,
                event_id=event.event_id,
                causal_id=event.causal_id,
                attempt=event.attempts,
                retryable=retryable,
                **(
                    {
                        "code": error.code,
                        "phase": error.phase,
                        "prompt_accepted": error.prompt_accepted,
                    }
                    if isinstance(error, HarnessError)
                    else {}
                ),
            )
            return

        event.attempts += 1
        retryable = not isinstance(error, HarnessError) or (
            error.retryable and error.prompt_accepted is False
        )
        if retryable and event.attempts <= self._config.max_pulse_retries:
            event.priority *= 0.8
            self._pending_events.append(event)
            _logger.warning(
                "pulse failed for %s (attempt %d/%d), requeued: %s",
                event.engram_id, event.attempts,
                self._config.max_pulse_retries, error,
            )
        elif retryable:
            _logger.error(
                "pulse for %s dropped after %d attempts: %s",
                event.engram_id, event.attempts, error,
            )
        else:
            _logger.error(
                "pulse for %s failed without retry (accepted=%r): %s",
                event.engram_id,
                error.prompt_accepted,
                error,
            )

        failure_payload = {
            "engram": event.engram_id,
            "attempt": event.attempts,
            "retryable": retryable,
        }
        if isinstance(error, HarnessError):
            failure_payload.update({
                "code": error.code,
                "phase": error.phase,
                "prompt_accepted": error.prompt_accepted,
            })
        self._record("pulse_failed", **failure_payload)

    def _run_succession(
        self,
        engram_id: str,
        *,
        parent_event_id: str | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> str:
        """Execute succession and re-point all runtime state to the successor.

        The summary LLM call is charged to the runtime budget from the
        succession's own per-call usage (not a global stats delta, which would
        absorb concurrent adapter traffic); the dendrite queue and any pending
        events for the old engram move to the new one.
        """
        result = self._engram_mgr.succession(
            engram_id,
            parent_event_id=parent_event_id,
            runtime_fence=runtime_fence,
        )
        self._runtime.consume_budget(
            result.input_tokens,
            result.output_tokens,
            result.cached_tokens,
            result.cache_write_tokens,
        )
        return self._apply_succession_result(engram_id, result)

    def _apply_succession_result(
        self,
        engram_id: str,
        result: SuccessionResult,
    ) -> str:
        """Move transient Engine projections after the durable world commit."""

        new_id = result.new_id

        if self._causal_ledger is None:
            # Legacy in-memory pulses have no durable owner to preserve.
            self._dendrite.transfer_queue(engram_id, new_id)
            for ev in self._pending_events:
                if ev.engram_id == engram_id:
                    ev.engram_id = new_id
        if engram_id in self._sticky_priority:
            self._sticky_priority[new_id] = max(
                self._sticky_priority.pop(engram_id),
                self._sticky_priority.get(new_id, 0.0),
            )
        if engram_id in self._external_marked:
            self._external_marked.discard(engram_id)
            self._external_marked.add(new_id)
        if engram_id in self._inhibition:
            self._inhibition[new_id] = self._inhibition.pop(engram_id)
        if engram_id in self._pulse_depth:
            self._pulse_depth[new_id] = self._pulse_depth.pop(engram_id)
        if engram_id in self._incoming_depth:
            self._incoming_depth[new_id] = self._incoming_depth.pop(engram_id)
        self._recent_activations = [
            (new_id if existing_id == engram_id else existing_id, timestamp)
            for existing_id, timestamp in self._recent_activations
        ]
        _logger.info("succession: %s -> %s", engram_id, new_id)
        self._record(
            "succession",
            old=engram_id,
            new=new_id,
            generation=result.generation_id,
            handed_off=len(result.handed_off_event_ids),
        )
        return new_id

    def _has_queued_successor_conflict(
        self,
        engram_id: str,
        *,
        current_event_id: str | None,
    ) -> bool:
        """Keep durable predecessor roots from becoming orphaned on rotate."""

        if self._causal_ledger is None:
            return False
        queued = self._causal_ledger.list_events(
            status=CausalEventStatus.QUEUED,
            engram_id=engram_id,
            limit=2,
        )
        return any(event.id != current_event_id for event in queued)

    def _propagate(
        self,
        source_id: str,
        output_content: str,
        *,
        source_event: PulseEvent | None = None,
        parent_event_id: str | None = None,
        causal_id: str | None = None,
    ) -> None:
        """Rule 1: propagate pulse output along outgoing connections.

        Excitatory edges deliver content into the target's dendrite queue.
        Inhibitory edges deliver no content — they raise the target's
        inhibition level instead (suppressing spontaneous activation and
        lowering the priority of its future propagation dispatches).

        With a causal ledger, an excitatory delivery is a durable child event
        and is not duplicated into Dendrite.  The in-memory path remains the
        compatibility adapter for old callers/tests.
        """
        source_depth = self._pulse_depth.get(source_id, 0)
        max_depth = self._config.max_content_propagation_depth
        if max_depth is not None and source_depth >= max_depth:
            self._record(
                "propagation_depth_fenced",
                source=source_id,
                depth=source_depth,
                maximum=max_depth,
            )
            return

        # claustrum spectrum stream: the source's propagation threshold is modulated
        # per-engram (factor 1.0 without a claustrum). A factor >1 raises the
        # bar so fewer edges spread — the brake on propagation cascades.
        threshold = (
            self._config.propagation_threshold
            * self._propagation_mods.get(source_id, 1.0)
        )
        targets = self._connections.get_propagation_targets(
            source_id, threshold
        )
        if targets:
            _logger.debug(
                "propagate %s -> %s",
                source_id,
                [(c.to_id, c.conn_type.value[:5], round(c.weight, 3)) for c in targets],
            )
        now = _now()
        delivered_content = self._config.propagation_content_prefix + output_content
        source_causal_event = None
        if self._causal_ledger is not None and source_event is not None:
            if source_event.event_id is None:
                raise ValueError(
                    "durable propagation requires a source event context"
                )
            source_causal_event = self._causal_ledger.get_event(
                source_event.event_id
            )
            if source_causal_event is None:
                raise ValueError(
                    f"causal source event {source_event.event_id} not found"
                )
            parent_event_id = parent_event_id or source_event.event_id
            causal_id = causal_id or source_event.causal_id
        excitatory: list[str] = []
        inhibitory: list[str] = []
        for conn in targets:
            if conn.conn_type == ConnectionType.INHIBITORY:
                self._add_inhibition(conn.to_id, conn.weight, now)
                inhibitory.append(conn.to_id)
                continue
            if self._causal_ledger is not None:
                if (
                    source_event is None
                    or source_event.event_id is None
                    or source_causal_event is None
                ):
                    raise ValueError(
                        "durable propagation requires a source event context"
                    )
                self.enqueue_causal_event(
                    engram_id=conn.to_id,
                    content=delivered_content,
                    flow=CausalEventFlow.CONTENT,
                    domain=CausalEventDomain.PULSE,
                    kind=CausalEventKind.PROPAGATION,
                    source=CausalEventSource.PROPAGATION,
                    causal_id=causal_id,
                    parent_event_id=parent_event_id,
                    metadata={
                        "priority": conn.weight,
                        "source_engram_id": source_id,
                        "depth": source_depth + 1,
                    },
                    world_id=source_causal_event.world_id,
                )
            else:
                self._dendrite.receive(
                    conn.to_id,
                    delivered_content,
                    source_id,
                    conn.weight,
                )
                self._incoming_depth[conn.to_id] = max(
                    self._incoming_depth.get(conn.to_id, 0), source_depth + 1
                )
            excitatory.append(conn.to_id)
        if targets:
            self._record(
                "propagate",
                source=source_id,
                depth=source_depth,
                targets=excitatory,
                inhibited=inhibitory,
            )

    # ── Inhibition (v0.4) ────────────────────────────────────────

    def _inhibition_level(self, engram_id: str, now: datetime) -> float:
        entry = self._inhibition.get(engram_id)
        if entry is None:
            return 0.0
        level, updated_at = entry
        elapsed = max(0.0, (now - updated_at).total_seconds())
        decayed = level * math.exp(-elapsed / self._config.inhibition_tau)
        if decayed < 1e-4:
            del self._inhibition[engram_id]
            return 0.0
        return decayed

    def _add_inhibition(self, engram_id: str, amount: float, now: datetime) -> None:
        level = self._inhibition_level(engram_id, now)
        self._inhibition[engram_id] = (level + amount, now)

    def _check_spontaneous(self) -> None:
        """Rule 6: check each active engram for spontaneous activation."""
        engrams = self._storage.list_engrams(status=EngramStatus.ACTIVE)
        succession_excluded = self._succession_excluded_engrams()
        for engram in engrams:
            if engram.id in succession_excluded:
                continue
            prob = self._compute_spontaneous_probability(engram.id, engram)
            if random.random() < prob:
                if self._spontaneous_emitter is not None:
                    with self._storage._lock:
                        dispatch = self._spontaneous_emitter(engram.id)
                    if not isinstance(dispatch, SpontaneousDispatch):
                        raise TypeError(
                            "spontaneous_emitter must return SpontaneousDispatch"
                        )
                    if dispatch is not SpontaneousDispatch.FALLBACK:
                        continue
                self._enqueue_generic_spontaneous(engram.id)

    def _enqueue_generic_spontaneous(self, engram_id: str) -> None:
        """Preserve the historical generic spontaneous event path."""

        if self._causal_ledger is not None:
            with self._storage._lock:
                center_id = (
                    self._spontaneous_center(engram_id)
                    if self._spontaneous_center is not None
                    else None
                )
                metadata = {"priority": 0.3}
                if center_id is None:
                    metadata["reason_code"] = "diffuse_spontaneous"
                self.enqueue_causal_event(
                    engram_id=engram_id,
                    content=None,
                    flow=None,
                    domain=CausalEventDomain.PULSE,
                    kind=CausalEventKind.SPONTANEOUS,
                    source=CausalEventSource.SELF,
                    center_id=center_id,
                    metadata=metadata,
                )
        else:
            self._pending_events.append(PulseEvent(
                engram_id=engram_id,
                reason=PulseReason.SPONTANEOUS,
                priority=0.3,
            ))

    def _compute_spontaneous_probability(self, engram_id: str, engram) -> float:
        """Compute non-uniform spontaneous activation probability.

        Based on self_excitability, recent_activity, connection density,
        and the engram's current inhibition level.
        """
        conn_count = self._storage.count_connections(engram_id)
        conn_factor = min(1.0, conn_count / 10.0)

        prob = (
            self._config.base_spontaneous_rate
            * engram.metadata.self_excitability
            * (0.3 + 0.7 * engram.metadata.recent_activity)
            * (0.5 + 0.5 * conn_factor)
            * self._spontaneous_modifier
        )
        if self._spontaneous_factor is not None:
            factor = float(self._spontaneous_factor(engram_id))
            if not math.isfinite(factor) or factor < 0.0:
                raise ValueError(
                    "spontaneous_factor must return a finite non-negative number"
                )
            prob *= factor
        inhibition = self._inhibition_level(engram_id, _now())
        if inhibition > 0:
            prob *= 1.0 / (1.0 + inhibition)
        # Spectrum modulation (Claustrum modulator) composes multiplicatively with inhibition
        prob *= self._activity_mods.get(engram_id, 1.0)
        return min(prob, 1.0)

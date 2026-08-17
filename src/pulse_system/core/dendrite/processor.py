"""Dendritic computation and input processing pipeline.

Manages per-engram input queues, decay windows, and dendritic integration.
Legacy merged text remains an in-memory sideband; durable event IDs are
reconstructible scheduling/integration cache entries whose truth lives in the
CausalLedger.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pulse_system.core.engram.manager import EngramManager


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class DendriteConfig:
    silence_threshold: float = 5.0   # seconds without new input → close window
    default_max_wait: float = 30.0   # seconds max wait before forced dispatch
    front_max_wait: float = 2.0      # front-stage engram max wait


DENDRITIC_WINDOW_POLICY_VERSION = "dendritic-window.v1"


@dataclass(frozen=True)
class DendriticWindowPolicySnapshot:
    """The exact opening policy frozen before a durable input is queued."""

    policy_version: str
    base_silence_threshold_seconds: float
    base_max_wait_seconds: float
    wait_modifier: float
    silence_threshold_seconds: float
    max_wait_seconds: float

    def __post_init__(self) -> None:
        if self.policy_version != DENDRITIC_WINDOW_POLICY_VERSION:
            raise ValueError("unsupported dendritic window policy version")
        for name, value in (
            (
                "base_silence_threshold_seconds",
                self.base_silence_threshold_seconds,
            ),
            ("base_max_wait_seconds", self.base_max_wait_seconds),
            ("wait_modifier", self.wait_modifier),
            ("silence_threshold_seconds", self.silence_threshold_seconds),
            ("max_wait_seconds", self.max_wait_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) < 0.0
                or (name == "wait_modifier" and float(value) == 0.0)
            ):
                raise ValueError(f"{name} must be a finite valid number")
        if (
            float(self.base_silence_threshold_seconds)
            * float(self.wait_modifier)
            != float(self.silence_threshold_seconds)
            or float(self.base_max_wait_seconds) * float(self.wait_modifier)
            != float(self.max_wait_seconds)
        ):
            raise ValueError("effective dendritic timing does not match its snapshot")


@dataclass
class InputItem:
    content: str
    source_engram_id: str
    weight: float
    arrived_at: datetime
    # Durable scheduling context.  Legacy callers leave these null and keep
    # the original constructor/merge behavior.
    event_id: str | None = None
    causal_id: str | None = None
    parent_event_id: str | None = None
    priority: float = 0.0
    event_seq: int | None = None
    window_policy: DendriticWindowPolicySnapshot | None = None


@dataclass
class InputQueue:
    items: list[InputItem] = field(default_factory=list)
    window_opened_at: datetime | None = None
    last_input_at: datetime | None = None


@dataclass(frozen=True)
class DendriticReadyWindow:
    """One closed, reconstructible durable-input cohort.

    Effective timing values are captured after per-Engram/claustrum
    modulation.  They are evidence, not knobs: CausalLedger independently
    recomputes the boundary from raw event timestamps before committing a
    convergence nexus.
    """

    engram_id: str
    event_ids: tuple[str, ...]
    event_seqs: tuple[int, ...]
    policy_version: str
    base_silence_threshold_seconds: float
    base_max_wait_seconds: float
    wait_modifier: float
    silence_threshold_seconds: float
    max_wait_seconds: float
    opened_at: datetime
    last_input_at: datetime
    closed_at: datetime
    observed_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.engram_id, str) or not self.engram_id.strip():
            raise ValueError("dendritic window engram_id must be non-empty")
        DendriticWindowPolicySnapshot(
            policy_version=self.policy_version,
            base_silence_threshold_seconds=self.base_silence_threshold_seconds,
            base_max_wait_seconds=self.base_max_wait_seconds,
            wait_modifier=self.wait_modifier,
            silence_threshold_seconds=self.silence_threshold_seconds,
            max_wait_seconds=self.max_wait_seconds,
        )
        if (
            not self.event_ids
            or len(self.event_ids) != len(self.event_seqs)
            or len(set(self.event_ids)) != len(self.event_ids)
            or len(set(self.event_seqs)) != len(self.event_seqs)
            or any(not isinstance(value, str) or not value for value in self.event_ids)
            or any(type(value) is not int or value < 1 for value in self.event_seqs)
        ):
            raise ValueError("dendritic window event identity is not canonical")
        times = (
            self.opened_at,
            self.last_input_at,
            self.closed_at,
            self.observed_at,
        )
        if any(
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
            for value in times
        ):
            raise ValueError("dendritic window timestamps must be timezone-aware")
        if not (
            self.opened_at
            <= self.last_input_at
            <= self.closed_at
            <= self.observed_at
        ):
            raise ValueError("dendritic window timestamps are out of order")


class DendriteProcessor:
    """Per-engram input queue management and dendritic integration."""

    def __init__(
        self,
        engram_manager: EngramManager,
        config: DendriteConfig | None = None,
    ):
        self._engram_mgr = engram_manager
        self._config = config or DendriteConfig()
        self._queues: dict[str, InputQueue] = {}
        self._wait_times: dict[str, float] = {}
        self._wait_modifiers: dict[str, float] = {}

    @property
    def config(self) -> DendriteConfig:
        return self._config

    def window_policy_snapshot(
        self,
        engram_id: str,
    ) -> DendriticWindowPolicySnapshot:
        base_silence = float(self._config.silence_threshold)
        base_max_wait = float(
            self._wait_times.get(engram_id, self._config.default_max_wait)
        )
        modifier = float(self._wait_modifiers.get(engram_id, 1.0))
        return DendriticWindowPolicySnapshot(
            policy_version=DENDRITIC_WINDOW_POLICY_VERSION,
            base_silence_threshold_seconds=base_silence,
            base_max_wait_seconds=base_max_wait,
            wait_modifier=modifier,
            silence_threshold_seconds=base_silence * modifier,
            max_wait_seconds=base_max_wait * modifier,
        )

    def receive(
        self,
        engram_id: str,
        content: str,
        source_id: str,
        weight: float,
        *,
        event_id: str | None = None,
        causal_id: str | None = None,
        parent_event_id: str | None = None,
        priority: float = 0.0,
        event_seq: int | None = None,
        window_policy: DendriticWindowPolicySnapshot | None = None,
        arrived_at: datetime | None = None,
    ) -> None:
        """Receive an injection into the engram's input queue."""
        now = arrived_at or _now()
        queue = self._get_or_create_queue(engram_id)

        # A durable queue is rebuilt from CausalLedger on every tick/restart.
        # Deduplication by event_id makes that reconstruction idempotent and
        # prevents the scheduler cache from looking like multiple receives.
        if event_id is not None and any(
            item.event_id == event_id
            for candidate in self._queues.values()
            for item in candidate.items
        ):
            return

        queue.items.append(InputItem(
            content=content,
            source_engram_id=source_id,
            weight=weight,
            arrived_at=now,
            event_id=event_id,
            causal_id=causal_id,
            parent_event_id=parent_event_id,
            priority=priority,
            event_seq=event_seq,
            window_policy=window_policy,
        ))
        if queue.last_input_at is None or now > queue.last_input_at:
            queue.last_input_at = now

        if (
            queue.window_opened_at is None
            or now < queue.window_opened_at
        ):
            queue.window_opened_at = now

    def remove_event(self, event_id: str) -> bool:
        """Remove one durable scheduling item without touching Storage."""

        removed = False
        for queue in self._queues.values():
            before = len(queue.items)
            queue.items[:] = [item for item in queue.items if item.event_id != event_id]
            if len(queue.items) != before:
                removed = True
                self._reset_window(queue)
        return removed

    def retain_event_ids(self, event_ids: set[str]) -> None:
        """Drop stale durable cache entries; legacy items are preserved."""

        for queue in self._queues.values():
            if not queue.items:
                continue
            queue.items[:] = [
                item
                for item in queue.items
                if item.event_id is None or item.event_id in event_ids
            ]
            self._reset_window(queue)

    def get_ready_event_windows(
        self,
        now: datetime | None = None,
    ) -> tuple[DendriticReadyWindow, ...]:
        """Partition durable history into closed timing cohorts.

        A process can be absent for longer than one dendritic interval.  On
        restart, rebuilding one flat queue must not reopen an already-expired
        window and merge it with a much later arrival.  We therefore replay
        the silence/max-wait boundary between every adjacent durable item.
        """

        observed_at = now or _now()
        windows: list[DendriticReadyWindow] = []
        for engram_id, queue in self._queues.items():
            durable = [
                item
                for item in queue.items
                if item.event_id is not None and item.event_seq is not None
            ]
            if not durable:
                continue
            durable.sort(
                key=lambda item: (
                    item.arrived_at,
                    item.event_seq if item.event_seq is not None else 2**63,
                    item.event_id or "",
                )
            )
            cohort: list[InputItem] = []
            opened_at: datetime | None = None
            last_input_at: datetime | None = None
            closed_at: datetime | None = None
            policy: DendriticWindowPolicySnapshot | None = None

            def finish() -> None:
                if (
                    not cohort
                    or opened_at is None
                    or last_input_at is None
                    or closed_at is None
                    or policy is None
                    or closed_at > observed_at
                ):
                    return
                windows.append(
                    DendriticReadyWindow(
                        engram_id=engram_id,
                        event_ids=tuple(item.event_id or "" for item in cohort),
                        event_seqs=tuple(
                            item.event_seq if item.event_seq is not None else 0
                            for item in cohort
                        ),
                        policy_version=policy.policy_version,
                        base_silence_threshold_seconds=(
                            policy.base_silence_threshold_seconds
                        ),
                        base_max_wait_seconds=policy.base_max_wait_seconds,
                        wait_modifier=policy.wait_modifier,
                        silence_threshold_seconds=(
                            policy.silence_threshold_seconds
                        ),
                        max_wait_seconds=policy.max_wait_seconds,
                        opened_at=opened_at,
                        last_input_at=last_input_at,
                        closed_at=closed_at,
                        observed_at=observed_at,
                    )
                )

            for item in durable:
                if item.window_policy is None:
                    raise ValueError(
                        "durable dendritic input lacks an opening policy snapshot"
                    )
                if not cohort:
                    cohort = [item]
                    opened_at = item.arrived_at
                    last_input_at = item.arrived_at
                    policy = item.window_policy
                    closed_at = min(
                        opened_at
                        + timedelta(seconds=policy.max_wait_seconds),
                        last_input_at
                        + timedelta(seconds=policy.silence_threshold_seconds),
                    )
                    continue
                assert opened_at is not None
                assert last_input_at is not None
                assert closed_at is not None
                if item.arrived_at > closed_at:
                    finish()
                    cohort = [item]
                    opened_at = item.arrived_at
                    last_input_at = item.arrived_at
                    policy = item.window_policy
                else:
                    cohort.append(item)
                    last_input_at = item.arrived_at
                assert policy is not None
                closed_at = min(
                    opened_at + timedelta(seconds=policy.max_wait_seconds),
                    last_input_at
                    + timedelta(seconds=policy.silence_threshold_seconds),
                )
            finish()
        return tuple(windows)

    def get_ready_event_ids(self, now: datetime | None = None) -> list[str]:
        """Return the flattened compatibility view of closed cohorts."""

        return [
            event_id
            for window in self.get_ready_event_windows(now)
            for event_id in window.event_ids
        ]

    def receive_durable_event(
        self,
        *,
        engram_id: str,
        event_id: str,
        content: str | None,
        causal_id: str | None,
        parent_event_id: str | None,
        priority: float,
        event_seq: int,
        window_policy: DendriticWindowPolicySnapshot,
        arrived_at: datetime,
    ) -> None:
        """Add a ledger event to the reconstructible scheduling cache."""

        self.receive(
            engram_id,
            content or "",
            "durable",
            1.0,
            event_id=event_id,
            causal_id=causal_id,
            parent_event_id=parent_event_id,
            priority=priority,
            event_seq=event_seq,
            window_policy=window_policy,
            arrived_at=arrived_at,
        )

    def check_ready(self, engram_id: str, now: datetime | None = None) -> bool:
        """Check whether this engram's input queue should dispatch.

        Two dispatch conditions:
        1. Silence: no new input for silence_threshold seconds
        2. Max wait: window has been open for max_wait_time seconds
        """
        now = now or _now()
        queue = self._queues.get(engram_id)
        if queue is None or queue.window_opened_at is None:
            return False
        if not queue.items:
            return False

        return self.check_ready_for_queue(queue, now, engram_id=engram_id)

    def check_ready_for_queue(
        self,
        queue: InputQueue,
        now: datetime,
        *,
        engram_id: str,
    ) -> bool:
        """Readiness predicate shared by legacy and durable caches."""

        if not queue.items or queue.window_opened_at is None or queue.last_input_at is None:
            return False
        elapsed_since_last = (now - queue.last_input_at).total_seconds()
        elapsed_since_open = (now - queue.window_opened_at).total_seconds()
        max_wait = self.get_wait_time(engram_id)

        # The claustrum wait factor scales both dispatch paths: silence-driven
        # dispatch is the common one, so a tempo lever that only touched
        # max-wait would barely act on real traffic.
        modifier = self._wait_modifiers.get(engram_id, 1.0)
        if elapsed_since_last >= self._config.silence_threshold * modifier:
            return True
        if elapsed_since_open >= max_wait:
            return True
        return False

    def integrate(self, engram_id: str) -> str | None:
        """Execute dendritic integration.

        Takes all queued inputs, merges them into a coherent natural-text
        context (v1: simple concatenation grouped by source), clears the
        queue, appends the result to the engram session via Engram manager, and returns
        the integrated text.

        Returns None if the queue is empty.
        """
        queue = self._queues.get(engram_id)
        if queue is None or not queue.items:
            return None

        items = sorted(queue.items, key=lambda x: x.arrived_at)
        integrated = self._merge_items(items)

        source_ids = list({item.source_engram_id for item in items})
        combined_source = source_ids[0] if len(source_ids) == 1 else ",".join(source_ids)

        self._engram_mgr.append_injection(engram_id, integrated, combined_source)

        queue.items.clear()
        queue.window_opened_at = None
        queue.last_input_at = None

        return integrated

    def get_all_ready(self, now: datetime | None = None) -> list[str]:
        """Return all engram IDs whose queues are ready to dispatch."""
        now = now or _now()
        return [
            eid for eid, queue in self._queues.items()
            if queue.items and self.check_ready(eid, now)
        ]

    def get_wait_time(self, engram_id: str) -> float:
        """Get the current max wait time for an engram.

        The base wait (per-engram override or default) is multiplied by a
        transient modulation factor when a spectrum modulator (Claustrum modulator) is
        attached upstream — temporal alignment without touching content.
        """
        base = self._wait_times.get(engram_id, self._config.default_max_wait)
        return base * self._wait_modifiers.get(engram_id, 1.0)

    def set_wait_modifiers(self, modifiers: dict[str, float]) -> None:
        """Replace the transient wait-time modulation map (engine, per tick)."""
        self._wait_modifiers = modifiers

    def set_wait_time(self, engram_id: str, wait_time: float) -> None:
        """Override the max wait time for a specific engram."""
        self._wait_times[engram_id] = wait_time

    def transfer_queue(self, old_id: str, new_id: str) -> None:
        """Move pending inputs and wait-time override to a successor engram.

        Called during succession so that inputs queued for the archived
        engram are delivered to its successor instead of being dropped.
        """
        old_queue = self._queues.pop(old_id, None)
        if old_queue is not None and old_queue.items:
            target = self._get_or_create_queue(new_id)
            target.items.extend(old_queue.items)
            if target.window_opened_at is None:
                target.window_opened_at = old_queue.window_opened_at
            elif old_queue.window_opened_at is not None:
                target.window_opened_at = min(
                    target.window_opened_at, old_queue.window_opened_at
                )
            if target.last_input_at is None:
                target.last_input_at = old_queue.last_input_at
            elif old_queue.last_input_at is not None:
                target.last_input_at = max(
                    target.last_input_at, old_queue.last_input_at
                )
        if old_id in self._wait_times:
            self._wait_times[new_id] = self._wait_times.pop(old_id)

    def get_queue_size(self, engram_id: str) -> int:
        queue = self._queues.get(engram_id)
        return len(queue.items) if queue else 0

    def has_pending(self, engram_id: str) -> bool:
        queue = self._queues.get(engram_id)
        return bool(queue and queue.items)

    # ── Internal ─────────────────────────────────────────────────

    def _get_or_create_queue(self, engram_id: str) -> InputQueue:
        if engram_id not in self._queues:
            self._queues[engram_id] = InputQueue()
        return self._queues[engram_id]

    @staticmethod
    def _reset_window(queue: InputQueue) -> None:
        if not queue.items:
            queue.window_opened_at = None
            queue.last_input_at = None
            return
        queue.window_opened_at = min(item.arrived_at for item in queue.items)
        queue.last_input_at = max(item.arrived_at for item in queue.items)

    @staticmethod
    def _merge_items(items: list[InputItem]) -> str:
        """V1 integration: group by source, concatenate with separators."""
        if len(items) == 1:
            return items[0].content

        by_source: dict[str, list[str]] = {}
        for item in items:
            by_source.setdefault(item.source_engram_id, []).append(item.content)

        parts: list[str] = []
        for source_id, contents in by_source.items():
            if len(contents) == 1:
                parts.append(contents[0])
            else:
                parts.append(" ".join(contents))

        return "\n\n".join(parts)

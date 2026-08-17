"""Sleep/dream engine (core/sleep/).

Multi-cycle nights use a fixed, irreversible phase order: NREM local
consolidation first, followed by REM cross-domain integration. Front-half
cycles are NREM-heavy and back-half cycles are REM-heavy.

All dream output goes to wikis (Library), never to any main session: an
engram "wakes up" with its session unchanged but its knowledge base
richer. Forks are transient engrams archived before the phase ends.

REM also drives the delegation router's offline exploration budget through
GRPO learning over delegation history and coverage-first virtual delegations.

The engine holds no scheduler: *when* to sleep is the harness's life
arrangement (CLI /sleep, or an outer routine) — the phase structure is
what this module owns.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

from pulse_system.core.engram.manager import EngramManager
from pulse_system.core.types import EngramStatus, Message, MessageRole
from pulse_system.substrate.llm.adapter import LLMCallError
from pulse_system.substrate.storage.store import Storage

_logger = logging.getLogger("pulse_system.sleep")

_NREM_PROMPT = (
    "以下是你最近积累的观察日记(来自被借用执行任务的记录和旁听观察)。"
    "请做一次自我整理:分类归档这些记录,提炼其中可复用的模式和结论。"
    "输出一段结构化的整理结果。\n\n"
)

_REM_READ_INTRO = (
    "你正在做一次深度精读。下面会分段给你一个认知体的完整经历,"
    "请逐段阅读并思考,在每段后简述你的理解如何被这一段加深或修正。"
)

_REM_ABSTRACT_PROMPT = (
    "回顾你刚才精读的全部内容。请提炼:这段经历与其他知识可能共享的"
    "深层结构是什么?用一两句话给出一个抽象骨架(不是内容摘要,"
    "而是可以迁移到其他领域的模式)。"
)


@dataclass
class SleepConfig:
    cycles: int = 3
    # NREM: cap consolidations per cycle (front-half cycles do more)
    nrem_cap_first_cycle: int = 8
    # REM: total deep reads per night (翻牌子 1-3 per spec)
    rem_reads_per_night: int = 2
    read_segment_chars: int = 2_000
    max_read_segments: int = 5
    hub_connection_weight: float = 0.4
    virtual_delegations_per_cycle: int = 1


@dataclass
class NightReport:
    cycles: int = 0
    consolidated: list[str] = field(default_factory=list)   # engram ids
    deep_reads: list[str] = field(default_factory=list)     # engram ids
    hubs_spawned: list[str] = field(default_factory=list)   # new engram ids
    router_updates: int = 0
    virtual_delegations: int = 0
    errors: int = 0


class SleepEngine:
    """NREM→REM night cycles over the engram network."""

    def __init__(
        self,
        storage: Storage,
        engram_manager: EngramManager,
        connection_network,
        library,
        *,
        delegator=None,
        router=None,
        metrics=None,
        config: SleepConfig | None = None,
    ):
        self._storage = storage
        self._mgr = engram_manager
        self._connections = connection_network
        self._library = library
        self._delegator = delegator
        self._router = router
        self._metrics = metrics
        self._config = config or SleepConfig()
        # 翻牌子 state persists across nights (never-read-first coverage)
        self._state_path = library.root / "sleep-state.json"

    # ── Night ────────────────────────────────────────────────────

    def run_night(self, cycles: int | None = None) -> NightReport:
        cycles = cycles or self._config.cycles
        report = NightReport(cycles=cycles)
        reads_left = self._config.rem_reads_per_night

        for cycle in range(cycles):
            # front-half NREM-heavy, back-half REM-heavy
            frac_left = 1.0 - cycle / max(1, cycles)
            nrem_cap = max(1, int(self._config.nrem_cap_first_cycle * frac_left))
            self._nrem_phase(report, cap=nrem_cap)

            rem_reads = 0
            if cycle >= cycles // 2 or cycles == 1:
                rem_reads = min(reads_left, 1 if cycle < cycles - 1 else reads_left)
            reads_left -= self._rem_phase(report, reads=rem_reads)

        self._record("sleep_night", cycles=cycles,
                     consolidated=len(report.consolidated),
                     deep_reads=len(report.deep_reads),
                     hubs=len(report.hubs_spawned),
                     router_updates=report.router_updates,
                     errors=report.errors)
        return report

    # ── NREM: local consolidation ────────────────────────────────

    def _nrem_phase(self, report: NightReport, cap: int) -> None:
        pending = [
            eid for eid in self._library.engrams_with_new_diary()
            if self._is_active(eid)
        ][:cap]
        for eid in pending:
            delta = self._library.diary_delta(eid)
            try:
                summary = self._fork_and_ask(eid, _NREM_PROMPT + delta)
            except LLMCallError as e:
                _logger.warning("NREM consolidation failed for %s: %s", eid, e)
                report.errors += 1
                continue
            self._library.append_wiki(eid, "离线整理", summary)
            self._library.mark_diary_consolidated(eid)
            report.consolidated.append(eid)
            self._record("sleep_nrem", engram=eid)

        # slow-scale homeostasis: global decay + prune once per cycle
        self._connections.decay_and_prune()

    # ── REM: cross-domain integration + exploration ─────────────

    def _rem_phase(self, report: NightReport, reads: int) -> int:
        done = 0
        abstractions: list[tuple[str, str]] = []
        for _ in range(reads):
            eid = self._pick_for_reading(exclude=set(report.deep_reads))
            if eid is None:
                break
            try:
                abstraction = self._deep_read(eid)
            except LLMCallError as e:
                _logger.warning("REM deep read failed for %s: %s", eid, e)
                report.errors += 1
                continue
            self._mark_read(eid)
            report.deep_reads.append(eid)
            abstractions.append((eid, abstraction))
            self._record("sleep_rem_read", engram=eid)
            done += 1

        # abstraction hub: sources are two-hop connected through it
        if abstractions:
            hub_id = self._spawn_hub(abstractions)
            report.hubs_spawned.append(hub_id)

        # delegation router offline learning + coverage-first virtual exploration
        if self._router is not None:
            report.router_updates += self._router.learn_from_history()
        if self._delegator is not None and self._router is not None:
            report.virtual_delegations += self._virtual_explore(
                self._config.virtual_delegations_per_cycle
            )
        return done

    def _deep_read(self, engram_id: str) -> str:
        """Segment-wise precision reading of an engram's session (REM 梦).

        Understanding accumulates in the dream fork's context segment by
        segment — a future emotional-core coupling point.
        """
        session = self._mgr.get_session(engram_id)
        text = "\n\n".join(m.content for m in session)
        segments = [
            text[i:i + self._config.read_segment_chars]
            for i in range(0, len(text), self._config.read_segment_chars)
        ][: self._config.max_read_segments]

        dream = self._storage.create_engram(initial_messages=[
            Message(role=MessageRole.USER, content=_REM_READ_INTRO),
        ])
        try:
            for i, segment in enumerate(segments, 1):
                self._mgr.pulse(
                    dream.id,
                    injected_context=f"(第 {i}/{len(segments)} 段)\n{segment}",
                    source_engram_id=f"remread:{engram_id}",
                )
            result = self._mgr.pulse(
                dream.id, injected_context=_REM_ABSTRACT_PROMPT,
                source_engram_id="remread:abstract",
            )
            return result.content
        finally:
            # Manager path → archive listeners free the fork's slot.
            self._mgr.archive(dream.id)

    def _spawn_hub(self, abstractions: list[tuple[str, str]]) -> str:
        """Create an abstraction-hub engram wired back to its sources."""
        lines = [
            "这是一个抽象枢纽:以下模式从多段具体经历中提炼而来。",
        ]
        for eid, abstraction in abstractions:
            lines.append(f"- (源自 {eid}) {abstraction}")
        hub = self._mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="\n".join(lines)),
        ])
        w = self._config.hub_connection_weight
        for eid, _ in abstractions:
            if self._is_active(eid):
                self._storage.create_connection(hub.id, eid, w)
                self._storage.create_connection(eid, hub.id, w)
        self._record("hub_spawn", hub=hub.id,
                     sources=[eid for eid, _ in abstractions])
        _logger.info("hub engram %s spawned from %s", hub.id,
                     [eid for eid, _ in abstractions])
        return hub.id

    def _virtual_explore(self, budget: int) -> int:
        """Coverage-first virtual delegation.

        Prefers engrams that have never been a delegation target; runs the
        task in snapshot mode with a synthetic caller so the record (with
        its embedding) enters the delegation router corpus.
        """
        targeted = {
            r["target_id"] for r in self._storage.list_delegations()
        }
        candidates = [
            e.id for e in self._storage.list_engrams(status=EngramStatus.ACTIVE)
            if e.id not in targeted
        ]
        done = 0
        for target in candidates[:budget]:
            try:
                self._delegator.delegate(
                    "dream", "整理你最近经历中的可复用要点", target_id=target,
                    mode="snapshot",
                )
                done += 1
            except (LLMCallError, RuntimeError, ValueError) as e:
                _logger.warning("virtual delegation to %s failed: %s", target, e)
        if done:
            self._record("dream_explore", count=done)
        return done

    # ── Internal ─────────────────────────────────────────────────

    def _fork_and_ask(self, engram_id: str, injection: str) -> str:
        """Fork an engram, ask one question on the fork, archive the fork."""
        session = self._mgr.get_session(engram_id)
        fork = self._storage.create_engram(initial_messages=list(session))
        try:
            result = self._mgr.pulse(
                fork.id, injected_context=injection,
                source_engram_id="sleep:nrem",
            )
            return result.content
        finally:
            # Manager path → archive listeners free the fork's slot.
            self._mgr.archive(fork.id)

    def _pick_for_reading(self, exclude: set[str]) -> str | None:
        """翻牌子: coverage first (never read), then today's most active."""
        read_counts = self._read_counts()
        engrams = [
            e for e in self._storage.list_engrams(status=EngramStatus.ACTIVE)
            if e.id not in exclude
        ]
        if not engrams:
            return None
        never_read = [e for e in engrams if e.id not in read_counts]
        pool = never_read or engrams
        pool.sort(key=lambda e: e.metadata.recent_activity, reverse=True)
        return pool[0].id

    def _read_counts(self) -> dict[str, int]:
        if not self._state_path.exists():
            return {}
        try:
            return json.loads(self._state_path.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            return {}

    def _mark_read(self, engram_id: str) -> None:
        counts = self._read_counts()
        counts[engram_id] = counts.get(engram_id, 0) + 1
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        self._state_path.write_text(
            json.dumps(counts, ensure_ascii=False), encoding="utf-8"
        )

    def _is_active(self, engram_id: str) -> bool:
        e = self._storage.get_engram(engram_id)
        return e is not None and e.status == EngramStatus.ACTIVE

    def _record(self, event_type: str, **payload) -> None:
        if self._metrics is not None:
            self._metrics.record(event_type, **payload)

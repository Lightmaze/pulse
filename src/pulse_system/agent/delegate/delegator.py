"""Delegation tunnel (agent/delegate/) — the directed stream.

Diffuse propagation changes the activation landscape without intent;
delegation is point-to-point, contracted work with a result. It uses the same
tool API and registry as search or file I/O.

Two execution modes are available:

- **mainline** (front-agent grade): the task executes on the target
  engram's own session — the work becomes its permanent experience.
- **snapshot** (engram-to-engram grade): the target is forked, the task
  runs on the fork, the cognitive diff is compressed to a short summary
  (stored in the target's library diary when one is configured), and the
  fork is destroyed. The target's mainline never notices.

Every delegation writes a DelegationRecord (storage layer `delegations` table) —
the training data for the delegation-routing MLP. Outcomes are recorded from
the caller's later behavior (adopted / revised / discarded), GRPO-style.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass

from pulse_system.agent.front.agent import FrontAgent, FrontAgentConfig
from pulse_system.agent.tools.registry import ToolRegistry, ToolResult
from pulse_system.core.engram.manager import EngramManager
from pulse_system.core.types import Message, MessageRole
from pulse_system.substrate.llm.adapter import LLMCallError
from pulse_system.substrate.storage.store import Storage

_logger = logging.getLogger("pulse_system.delegate")

_OUTCOMES = ("adopted", "revised", "discarded")


@dataclass
class DelegatorConfig:
    max_think_iterations: int = 5
    # Delegated engrams share the tool registry, so they can delegate too;
    # this caps the recursion (per call chain, thread-local).
    max_depth: int = 2
    # Snapshot diffs are compressed with one light LLM call above this size.
    compress_above_chars: int = 600


@dataclass
class DelegationResult:
    content: str
    target_id: str
    record_id: str
    mode: str


class Delegator:
    """Executes delegations and persists their records."""

    def __init__(
        self,
        storage: Storage,
        engram_manager: EngramManager,
        tools: ToolRegistry,
        *,
        library=None,
        metrics=None,
        router=None,
        config: DelegatorConfig | None = None,
    ):
        self._storage = storage
        self._mgr = engram_manager
        self._tools = tools
        self._library = library
        self._metrics = metrics
        # Optional DelegationRouter: enables delegate_routed(). The
        # tunnel itself (delegation tunnel) stays fully functional without it.
        self._router = router
        self._config = config or DelegatorConfig()
        self._depth = threading.local()

    # ── Public API ───────────────────────────────────────────────

    def delegate(
        self,
        caller_id: str,
        task: str,
        *,
        target_id: str | None = None,
        mode: str = "mainline",
        contract: str | None = None,
    ) -> DelegationResult:
        if mode not in ("mainline", "snapshot"):
            raise ValueError(f"Unknown delegation mode: {mode}")
        if mode == "snapshot" and target_id is None:
            raise ValueError("snapshot mode requires an existing target engram")

        depth = getattr(self._depth, "value", 0)
        if depth >= self._config.max_depth:
            raise RuntimeError(
                f"delegation depth limit reached ({self._config.max_depth})"
            )
        self._depth.value = depth + 1
        try:
            if mode == "mainline":
                return self._delegate_mainline(caller_id, task, target_id, contract)
            return self._delegate_snapshot(caller_id, task, target_id, contract)
        finally:
            self._depth.value = depth

    def record_outcome(self, record_id: str, outcome: str) -> bool:
        """Record the caller's verdict — the delegation router learning signal."""
        if outcome not in _OUTCOMES:
            raise ValueError(f"outcome must be one of {_OUTCOMES}")
        ok = self._storage.set_delegation_outcome(record_id, outcome)
        if ok and self._router is not None:
            self._router.learn_from_history()
        return ok

    def delegate_routed(
        self,
        caller_id: str,
        task: str,
        *,
        contract: str | None = None,
    ) -> list[DelegationResult]:
        """Route a task through the learned first layer of the fallback chain.

        - no router / no live candidates → mainline on a fresh engram
        - router decides → mainline on the chosen target
        - canary armed (top-2 too close) → both candidates run in snapshot
          mode under one group_id; the caller judges both, and the two
          outcomes become the highest-information pairwise signal
        """
        from pulse_system.core.types import EngramStatus

        embedding = self._task_embedding(task)
        candidates = [
            e.id for e in self._storage.list_engrams(status=EngramStatus.ACTIVE)
            if e.id != caller_id
        ]

        if self._router is None or not candidates:
            return [self.delegate(caller_id, task, contract=contract)]

        decision = self._router.choose(caller_id, embedding, candidates)
        if decision.target_id is None:
            return [self.delegate(caller_id, task, contract=contract)]

        if decision.canary_id is None:
            return [self.delegate(
                caller_id, task, target_id=decision.target_id,
                contract=contract,
            )]

        # Canary: run both in snapshot mode under one comparison group
        import uuid

        group_id = uuid.uuid4().hex[:16]
        results = []
        for target in (decision.target_id, decision.canary_id):
            results.append(self._delegate_snapshot(
                caller_id, task, target, contract, group_id=group_id,
            ))
        return results

    def as_tool(self, caller_id: str):
        """Build a `delegate` tool bound to one caller engram.

        Task syntax: plain text creates a new engram; a leading
        `@<engram_id> ` targets an existing one (mainline mode).
        """

        def delegate(task: str) -> ToolResult:
            task = task.strip()
            target = None
            if task.startswith("@"):
                head, _, rest = task.partition(" ")
                target, task = head[1:], rest.strip()
                if not task:
                    return ToolResult(
                        success=False, content="",
                        error="usage: @<engram_id> <task> or just <task>",
                    )
            try:
                result = self.delegate(caller_id, task, target_id=target)
            except (ValueError, RuntimeError, LLMCallError) as e:
                return ToolResult(success=False, content="", error=str(e))
            return ToolResult(
                success=True,
                content=(
                    f"[delegation {result.record_id} -> engram {result.target_id}]\n"
                    f"{result.content}"
                ),
            )

        return delegate

    # ── Modes ────────────────────────────────────────────────────

    def _delegate_mainline(
        self,
        caller_id: str,
        task: str,
        target_id: str | None,
        contract: str | None,
    ) -> DelegationResult:
        framed = self._frame(task, contract)
        if target_id is None:
            target = self._mgr.create(initial_messages=[
                Message(role=MessageRole.USER, content=framed),
            ])
            target_id = target.id
        else:
            if self._mgr.get(target_id) is None:
                raise ValueError(f"target engram {target_id} not found")
            self._mgr.append_injection(target_id, framed, f"delegation:{caller_id}")

        record_id = self._create_record(caller_id, target_id, task, "mainline", contract)
        output = self._run_think(target_id)
        self._storage.complete_delegation(record_id, output)
        self._record_metric(caller_id, target_id, "mainline")
        return DelegationResult(
            content=output, target_id=target_id, record_id=record_id,
            mode="mainline",
        )

    def _delegate_snapshot(
        self,
        caller_id: str,
        task: str,
        target_id: str,
        contract: str | None,
        group_id: str | None = None,
    ) -> DelegationResult:
        original = self._mgr.get_session(target_id)
        if self._mgr.get(target_id) is None:
            raise ValueError(f"target engram {target_id} not found")

        # Fork: an independent working copy — the target mainline is never
        # touched, and the fork's LLM prefix is the frozen session (cache
        # hits carry over).
        snapshot = self._storage.create_engram(initial_messages=list(original))
        record_id = self._create_record(
            caller_id, target_id, task, "snapshot", contract, group_id=group_id
        )
        try:
            self._mgr.append_injection(
                snapshot.id, self._frame(task, contract),
                f"delegation:{caller_id}",
            )
            self._run_think(snapshot.id)

            # Cognitive diff = everything the fork appended past the fork point
            after = self._mgr.get_session(snapshot.id)
            diff_msgs = after[len(original):]
            summary = self._compress_diff(task, diff_msgs)
        finally:
            # Route through the manager so archive listeners release the
            # slot this fork transiently occupied (no leak on max_slots).
            self._mgr.archive(snapshot.id)

        if self._library is not None:
            self._library.append_diary(
                target_id, f"任务:{task}\n\n{summary}",
                source=f"delegation:{caller_id}",
            )
        self._storage.complete_delegation(record_id, summary)
        self._record_metric(caller_id, target_id, "snapshot")
        return DelegationResult(
            content=summary, target_id=target_id, record_id=record_id,
            mode="snapshot",
        )

    # ── Internal ─────────────────────────────────────────────────

    @staticmethod
    def _frame(task: str, contract: str | None) -> str:
        if contract:
            return f"{task}\n\n(交互约定:{contract})"
        return task

    def _run_think(self, engram_id: str) -> str:
        agent = FrontAgent(
            engram_id, self._mgr, self._tools,
            FrontAgentConfig(max_think_iterations=self._config.max_think_iterations),
        )
        return agent.think()

    def _compress_diff(self, task: str, diff_msgs: list[Message]) -> str:
        text = "\n\n".join(m.content for m in diff_msgs).strip()
        if not text:
            return "(快照执行没有产生新内容)"
        if len(text) <= self._config.compress_above_chars:
            return text
        result = self._mgr.llm.complete([{
            "role": "user",
            "content": (
                f"以下是一次快照委派的执行过程(任务:{task})。"
                f"请压缩为一段简短的认知增量总结,只保留结论和可复用的经验:\n\n{text}"
            ),
        }])
        return result.content

    def _task_embedding(self, task: str) -> list[float] | None:
        try:
            return self._mgr.llm.embed(task).vector
        except (NotImplementedError, LLMCallError) as e:
            _logger.debug("task embedding unavailable: %s", e)
            return None

    def _create_record(
        self,
        caller_id: str,
        target_id: str,
        task: str,
        mode: str,
        contract: str | None,
        group_id: str | None = None,
    ) -> str:
        embedding = self._task_embedding(task)
        embedding_json = json.dumps(embedding) if embedding is not None else None
        return self._storage.create_delegation(
            caller_id, target_id, task, mode,
            contract=contract, task_embedding=embedding_json,
            group_id=group_id,
        )

    def _record_metric(self, caller_id: str, target_id: str, mode: str) -> None:
        if self._metrics is not None:
            self._metrics.record(
                "delegation", caller=caller_id, target=target_id, mode=mode
            )

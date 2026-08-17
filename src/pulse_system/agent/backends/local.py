"""LocalBackend — the task runs here, in this process, on the front-stage agent path.

This is the default backend and the reason the system is demonstrable with
no network and no API key: under `LLMAdapter(mock=True)` a delegated task
still creates an engram, still runs the front-agent think loop, still
returns a trace. Nothing about the delegation path is stubbed out for the
offline case — the same code runs, against a mock substrate.

It executes on the existing front-agent path (`agent/front/agent.py`)
rather than reimplementing a loop, so tool detection, iteration budget and
the front-agent timeout wall-clock deadline behave identically to a front-stage think.

`TaskSpec.target` selects the engram: `None` creates a fresh one whose
first message *is* the task; an id appends the task as an injection to that
engram's mainline, which is delegation tunnel mainline-grade delegation — the work
becomes that engram's permanent experience.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pulse_system.agent.backends.base import (
    BackendError,
    BackendResult,
    TaskSpec,
)
from pulse_system.agent.front.agent import FrontAgent, FrontAgentConfig
from pulse_system.core.types import Message, MessageRole
from pulse_system.substrate.llm.adapter import LLMCallError

if TYPE_CHECKING:
    from pulse_system.agent.tools.registry import ToolRegistry
    from pulse_system.core.engram.manager import EngramManager

__all__ = ["LocalBackend"]


class LocalBackend:
    """Runs a delegated task in-process, on a real engram."""

    name = "local"

    def __init__(
        self,
        engram_manager: EngramManager,
        tools: ToolRegistry,
        *,
        max_think_iterations: int = 5,
    ):
        self._mgr = engram_manager
        self._tools = tools
        self._max_iterations = max_think_iterations

    def preflight(self) -> None:
        """Always available — the executor is this process.

        There is no counterpart here to pi's absence: if this code is
        importable, it can run.
        """
        return None

    def submit(self, spec: TaskSpec) -> BackendResult:
        engram_id, fork_point = self._place(spec)

        agent = FrontAgent(
            engram_id,
            self._mgr,
            self._tools,
            FrontAgentConfig(
                max_think_iterations=self._max_iterations,
                deadline_sec=spec.timeout_sec,
            ),
        )
        try:
            output = agent.think()
        except LLMCallError as exc:
            # The substrate refused. Real execution failure, so it returns
            # with whatever the engram accumulated before the refusal.
            return BackendResult(
                backend=self.name,
                ok=False,
                output="",
                trace=self._trace(engram_id, fork_point),
                error=BackendError(
                    "local_llm_error",
                    f"the LLM substrate refused mid-task: {exc}",
                    "set the provider's API key, select a configured "
                    "provider, or construct LLMAdapter(mock=True) to run "
                    "the system offline",
                ),
            )

        trace = self._trace(engram_id, fork_point)
        if not output.strip():
            # Empty output is a failure to report, not a success with
            # nothing in it — the same rule PiBackend applies.
            return BackendResult(
                backend=self.name,
                ok=False,
                output="",
                trace=trace,
                error=BackendError(
                    "local_no_output",
                    f"the think loop on engram {engram_id} finished without "
                    f"producing any text (after {len(trace)} new messages)",
                    "raise max_think_iterations, raise TaskSpec.timeout_sec, "
                    "or inspect the engram's session for a tool loop",
                ),
            )
        return BackendResult(
            backend=self.name, ok=True, output=output, trace=trace
        )

    # ── Internal ─────────────────────────────────────────────────

    def _place(self, spec: TaskSpec) -> tuple[str, int]:
        """Return (engram_id, index of the first message this task adds)."""
        if spec.target is None:
            engram = self._mgr.create(initial_messages=[
                Message(role=MessageRole.USER, content=spec.task),
            ])
            return engram.id, 0

        if self._mgr.get(spec.target) is None:
            # A precondition, not an outcome: nothing ran, so this raises
            # rather than returning a result that looks like a run.
            raise BackendError(
                "engram_not_found",
                f"TaskSpec.target names engram {spec.target!r}, which does "
                f"not exist",
                "pass target=None to run on a fresh engram, or list live "
                "engrams and choose one",
            )
        fork_point = len(self._mgr.get_session(spec.target))
        self._mgr.append_injection(
            spec.target, spec.task, source_id=f"backend:{self.name}"
        )
        return spec.target, fork_point

    def _trace(self, engram_id: str, fork_point: int) -> list[dict[str, Any]]:
        """Everything this task appended to the engram, in order.

        This is the local analogue of pi's session branch: the path from
        the fork point to the leaf.
        """
        session = self._mgr.get_session(engram_id)
        return [
            {
                "kind": "engram.message",
                "engram_id": engram_id,
                "index": fork_point + offset,
                "role": message.role.value,
                "content": message.content,
                "source_engram_id": message.source_engram_id,
            }
            for offset, message in enumerate(session[fork_point:])
        ]

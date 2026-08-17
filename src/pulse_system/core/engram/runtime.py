"""The Engram runtime: acting is part of firing, not a front-stage privilege.

Any Engram can run the same bounded loop: pulse, detect a natural-language tool
request, perform the action, append the natural-language result, and pulse
again. This keeps action authority with the Engram that owns the session.

The free-context boundary is strict. This module adds no system prompt, tool
manifest, or structured instruction; `EngramManager.pulse()` sends only the raw
session history. Tool requests are recognized from ordinary language already
present in the session, so the affordance is not discoverable a priori.

Tool results are appended as natural-language messages, never function-call
blocks or structured envelopes. Diffuse propagation can therefore move text
between Engrams without carrying hidden control structures across sessions.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from pulse_system.agent.tools.registry import ToolRegistry, ToolResult
    from pulse_system.core.engram.manager import EngramManager


@dataclass
class EngramRuntimeConfig:
    """Bounds on one tool-capable firing.

    `max_tool_iterations` counts pulses, not tool calls: a firing that never
    asks to act costs exactly one pulse and is indistinguishable from today's
    `pulse()`. `deadline_sec` is wall-clock, because an iteration count cannot
    express a harness's hard time limit — one iteration's duration is unbounded
    (the same reason `FrontAgentConfig` carries it).
    """

    max_tool_iterations: int = 10
    deadline_sec: float | None = None


@dataclass
class RuntimeResult:
    """Outcome of a tool-capable firing.

    Token counts are summed over every completion the firing made, so a caller
    charging a budget from this is charged for what actually ran. `tool_calls`
    names the tools in the order they fired — the audit trail for a run.
    """

    content: str
    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0
    cache_write_tokens: int = 0
    iterations: int = 0
    tool_calls: list[str] = field(default_factory=list)


# Detection grammar. The table lives in `agent/front/agent.py` today and is
# imported lazily here for two reasons: `core` must not depend on `agent` at
# import time (the layering runs the other way), and there must be exactly ONE
# grammar — a second copy would drift, and the two loops would then disagree
# about what an engram just said. When the front agent is eventually collapsed
# onto this runtime the table moves here and `agent.py` imports it back.
_PATTERNS: list | None = None


def _patterns() -> list:
    global _PATTERNS
    if _PATTERNS is None:
        from pulse_system.agent.front.agent import _TOOL_PATTERNS

        _PATTERNS = _TOOL_PATTERNS
    return _PATTERNS


def detect_tool_call(text: str, tools: ToolRegistry) -> tuple[str, dict] | None:
    """Read an engram's own words for an act it is asking to perform.

    This is the whole answer to "how does an engram with no system prompt know
    what tools it has", and the answer is that it does not know — it is
    recognised, not informed. That keeps the free-context rule intact (nothing is prepended
    to the context) at a real cost, which is recorded here rather than papered
    over:

    - The affordance is invisible a priori. An engram that never happens to
      phrase a match never learns it can act at all. In practice the signal
      arrives from the model's own pretraining ("let me search: …" is ordinary
      assistant language), or from the session already containing a tool result
      — discovery by consequence of one's own act, which is in-context but is
      not scaffolding the runtime injected.
    - `search()` plus a greedy `(.+)` means the pattern fires anywhere in the
      text and swallows the rest of the line as the argument. Mentioning a tool
      and calling it are indistinguishable, and quoted material can act.
    - First match over an ordered list wins, so overlapping intents route by
      table order rather than by what was meant. `ToolRegistry`'s own toolset restriction note
      records this biting in production: `file_write` outranked `code_execute`
      and silently stole the turn, and the fix was to disable builtins, not to
      fix detection.
    - Under diffuse propagation, another engram's text lands in this session verbatim. If
      that text reads as a tool call, this engram may echo it and act on it.
      Widening the loop from one engram to many widens that channel with it —
      which is exactly why this path is opt-in, and why the real boundary is
      the registry's workspace sandbox, not this function.
    - There is no vocabulary for refusal or failure: an unregistered tool is
      guarded by `has_tool` and simply does not fire, which is silence, not an
      answer. At most one act per pulse.

    Returns (tool_name, kwargs), or None when the engram was only thinking.
    """
    for pattern, tool_name, param_names in _patterns():
        match = pattern.search(text)
        if match and tools.has_tool(tool_name):
            kwargs = {}
            for i, name in enumerate(param_names):
                value = match.group(i + 1)
                if value is not None:
                    kwargs[name] = value.strip()
            if kwargs:
                return tool_name, kwargs
    return None


def format_tool_result(tool_name: str, result: ToolResult) -> str:
    """Render a result as the plain text it will be read as (the natural-text result rule)."""
    if result.success:
        return result.content
    return f"Tool {tool_name} failed: {result.error}"


class EngramRuntime:
    """Runs one engram's firing with the ability to act.

    Holds no identity of its own. It is constructed per firing, over whichever
    engram is firing, and knows nothing about which engram that is — the
    property that makes it a capability of engrams rather than the privilege of
    one.
    """

    def __init__(
        self,
        manager: EngramManager,
        tools: ToolRegistry,
        config: EngramRuntimeConfig | None = None,
        detector: Callable[[str, ToolRegistry], tuple[str, dict] | None] | None = None,
    ):
        self._mgr = manager
        self._tools = tools
        self._config = config or EngramRuntimeConfig()
        # Injectable so callers can supply a compatible natural-language
        # detector without forking the full Pulse loop.
        self._detect = detector or detect_tool_call

    def run(
        self,
        engram_id: str,
        injected_context: str | None = None,
        source_engram_id: str | None = None,
    ) -> RuntimeResult:
        """Fire the engram, letting it act until it stops asking to.

        Each iteration is a plain `pulse()` — the tool-free path, so the
        messages sent to the LLM stay the raw session history (the free-context rule). When
        the output reads as a tool call the result is appended with
        `append_injection` as natural text (the natural-text result rule), and the next iteration
        simply pulses again over that session.
        """
        deadline = self._config.deadline_sec
        start = time.monotonic()
        # A firing is at least one pulse: this is `pulse()`'s contract, and a
        # configured 0 must not turn a firing into a no-op that silently
        # produces no output.
        budget = max(1, self._config.max_tool_iterations)

        out = RuntimeResult(content="")
        pending_context = injected_context
        pending_source = source_engram_id

        for _ in range(budget):
            if deadline is not None and time.monotonic() - start >= deadline:
                break

            result = self._mgr.pulse(
                engram_id,
                injected_context=pending_context,
                source_engram_id=pending_source,
            )
            # Context is injected once, at the start of the firing — not
            # re-injected on every internal iteration.
            pending_context = None
            pending_source = None

            out.content = result.content
            out.input_tokens += result.input_tokens
            out.output_tokens += result.output_tokens
            out.cached_tokens += result.cached_tokens
            out.cache_write_tokens += result.cache_write_tokens
            out.iterations += 1

            call = self._detect(result.content, self._tools)
            if call is None:
                break

            tool_name, kwargs = call
            tool_result = self._tools.execute(tool_name, **kwargs)
            self._mgr.append_injection(
                engram_id,
                format_tool_result(tool_name, tool_result),
                source_id="tool:" + tool_name,
            )
            out.tool_calls.append(tool_name)

        return out

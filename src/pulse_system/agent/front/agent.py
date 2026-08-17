"""Front-stage agent — the system's conscious focus.

The front agent is a special engram that can use tools and iteratively
think until it reaches a conclusion or exhausts its iteration budget.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pulse_system.agent.tools.registry import ToolRegistry, ToolResult
from pulse_system.core.types import Message, MessageRole

if TYPE_CHECKING:
    from pulse_system.core.engram.manager import EngramManager


@dataclass
class FrontAgentConfig:
    max_think_iterations: int = 10
    front_self_excitability: float = 0.5
    # front-agent timeout: wall-clock budget for a single think() call. Harnesses impose hard
    # time limits (LHTB: 90 min/task); an iteration count cannot express
    # them because one iteration's duration is unbounded. None = no limit.
    deadline_sec: float | None = None


_TOOL_PATTERNS: list[tuple[re.Pattern, str, list[str]]] = [
    (re.compile(r"(?:让我搜索一下|我来搜搜|搜索一下|web_search)[：:\s]+(.+)", re.IGNORECASE), "web_search", ["query"]),
    (re.compile(r"(?:让我看看|读取网页|打开链接|web_fetch)[：:\s]+(.+)", re.IGNORECASE), "web_fetch", ["url"]),
    (re.compile(r"(?:读取文件|查看文件|file_read)[：:\s]+(.+)", re.IGNORECASE), "file_read", ["path"]),
    (re.compile(r"(?:写入文件|保存文件|file_write)[：:\s]+(.+?)(?:\n|$)([\s\S]*)", re.IGNORECASE), "file_write", ["path", "content"]),
    (re.compile(r"(?:列出目录|查看目录|file_list)[：:\s]+(.+)", re.IGNORECASE), "file_list", ["directory"]),
    (re.compile(r"(?:执行代码|运行代码|code_execute)[：:\s]+```(\w+)?\n([\s\S]*?)```", re.IGNORECASE), "code_execute", ["language", "code"]),
    # Heavy execution is delegated rather than performed inline.
    (re.compile(r"(?:委派|委托|delegate)[：:\s]+([\s\S]+)", re.IGNORECASE), "delegate", ["task"]),
    # Library: skill discovery; the argument is optional and ignored
    (re.compile(r"(?:发现技能|查看技能|discover_skills)[：:\s]*(.*)", re.IGNORECASE), "discover_skills", ["query"]),
]


class FrontAgent:
    """The system's conscious focus — a special engram with tool access."""

    def __init__(
        self,
        engram_id: str,
        engram_manager: EngramManager,
        tool_registry: ToolRegistry,
        config: FrontAgentConfig | None = None,
    ):
        self._engram_id = engram_id
        self._mgr = engram_manager
        self._tools = tool_registry
        self._config = config or FrontAgentConfig()
        self._last_output: str | None = None

    @property
    def engram_id(self) -> str:
        return self._engram_id

    def think(self) -> str:
        """Execute a front-stage think cycle.

        Calls pulse(), checks output for tool-call intent, executes tools,
        feeds results back, and repeats until no more tool calls, max
        iterations, or the wall-clock deadline is reached. Returns the
        final output.
        """
        deadline = self._config.deadline_sec
        start = time.monotonic()
        for _ in range(self._config.max_think_iterations):
            if deadline is not None and time.monotonic() - start >= deadline:
                break
            output = self._mgr.pulse(self._engram_id).content
            self._last_output = output

            tool_call = self._detect_tool_call(output)
            if tool_call is None:
                return output

            tool_name, kwargs = tool_call
            result = self._tools.execute(tool_name, **kwargs)
            result_text = self._format_tool_result(tool_name, result)

            self._mgr.append_injection(
                self._engram_id,
                result_text,
                source_id="tool:" + tool_name,
            )

        return self._last_output or ""

    def receive_user_message(self, message: str) -> str:
        """Receive a human message, append to session, then think."""
        self._mgr.append_injection(
            self._engram_id,
            message,
            source_id="user",
        )
        return self.think()

    def get_status(self) -> str:
        """Return a brief status summary of the front engram."""
        engram = self._mgr.get(self._engram_id)
        if engram is None:
            return "Front engram not found."

        session = self._mgr.get_session(self._engram_id)
        msg_count = len(session)
        last_content = session[-1].content[:100] if session else "(empty)"

        return (
            f"Front engram {self._engram_id}: "
            f"{msg_count} messages, "
            f"{engram.total_pulses} pulses. "
            f"Latest: {last_content}"
        )

    def _detect_tool_call(self, text: str) -> tuple[str, dict] | None:
        """Detect tool-call intent from natural language output.

        Returns (tool_name, kwargs) or None if no tool call detected.
        """
        for pattern, tool_name, param_names in _TOOL_PATTERNS:
            match = pattern.search(text)
            if match and self._tools.has_tool(tool_name):
                kwargs = {}
                for i, name in enumerate(param_names):
                    value = match.group(i + 1)
                    if value is not None:
                        kwargs[name] = value.strip()
                if kwargs:
                    return tool_name, kwargs
        return None

    @staticmethod
    def _format_tool_result(tool_name: str, result: ToolResult) -> str:
        if result.success:
            return result.content
        return f"Tool {tool_name} failed: {result.error}"

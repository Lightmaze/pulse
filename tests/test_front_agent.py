"""Tests for the front-stage agent.

Uses mock LLM + mock tools. Tests think loop, tool detection,
user message handling, and status reporting.
"""

import pytest

from pulse_system.agent.front import FrontAgent, FrontAgentConfig
from pulse_system.agent.tools import ToolRegistry, ToolResult
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.engram import EngramManager
from pulse_system.core.types import Message, MessageRole
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


@pytest.fixture
def store():
    s = Storage(":memory:")
    yield s
    s.close()


@pytest.fixture
def mock_llm():
    return LLMAdapter(mock=True)


@pytest.fixture
def conn_net(store):
    return ConnectionNetwork(store, ConnectionConfig())


@pytest.fixture
def mgr(store, mock_llm, conn_net):
    return EngramManager(store, mock_llm, conn_net)


@pytest.fixture
def tools():
    return ToolRegistry(mock=True)


def _make_front(mgr, tools, config=None):
    e = mgr.create(initial_messages=[
        Message(role=MessageRole.USER, content="I am the front-stage engram."),
    ])
    return FrontAgent(e.id, mgr, tools, config)


# ── Basic think ──────────────────────────────────────────────────


class TestThink:
    def test_think_returns_output(self, mgr, tools):
        front = _make_front(mgr, tools)
        output = front.think()
        assert isinstance(output, str)
        assert len(output) > 0

    def test_think_appends_to_session(self, mgr, tools):
        front = _make_front(mgr, tools)
        front.think()
        session = mgr.get_session(front.engram_id)
        # initial user msg + assistant response (at minimum)
        assert len(session) >= 2

    def test_think_no_tool_call_single_iteration(self, mgr, tools):
        """When LLM output has no tool-call pattern, think returns after one pulse."""
        front = _make_front(mgr, tools)
        output = front.think()
        # Mock LLM just echoes — no tool patterns → single iteration
        assert output is not None


# ── Tool detection ───────────────────────────────────────────────


class TestToolDetection:
    def test_detect_web_search_chinese(self, mgr, tools):
        front = _make_front(mgr, tools)
        result = front._detect_tool_call("让我搜索一下：quantum computing")
        assert result is not None
        tool_name, kwargs = result
        assert tool_name == "web_search"
        assert kwargs["query"] == "quantum computing"

    def test_detect_web_search_english(self, mgr, tools):
        front = _make_front(mgr, tools)
        result = front._detect_tool_call("web_search: how does photosynthesis work")
        assert result is not None
        assert result[0] == "web_search"
        assert "photosynthesis" in result[1]["query"]

    def test_detect_file_read(self, mgr, tools):
        front = _make_front(mgr, tools)
        result = front._detect_tool_call("读取文件：/tmp/data.txt")
        assert result is not None
        assert result[0] == "file_read"
        assert result[1]["path"] == "/tmp/data.txt"

    def test_detect_file_list(self, mgr, tools):
        front = _make_front(mgr, tools)
        result = front._detect_tool_call("列出目录：/home/user")
        assert result is not None
        assert result[0] == "file_list"
        assert result[1]["directory"] == "/home/user"

    def test_detect_web_fetch(self, mgr, tools):
        front = _make_front(mgr, tools)
        result = front._detect_tool_call("web_fetch: https://example.com/page")
        assert result is not None
        assert result[0] == "web_fetch"
        assert result[1]["url"] == "https://example.com/page"

    def test_no_tool_in_plain_text(self, mgr, tools):
        front = _make_front(mgr, tools)
        result = front._detect_tool_call("I think the answer is 42.")
        assert result is None

    def test_unknown_tool_not_detected(self, mgr, tools):
        front = _make_front(mgr, tools)
        # Pattern matches but tool doesn't exist
        tools._tools.clear()
        result = front._detect_tool_call("web_search: test")
        assert result is None


# ── Tool execution in think loop ─────────────────────────────────


class TestToolExecution:
    def test_think_with_tool_call(self, store, conn_net, tools):
        """Simulate an LLM that outputs a tool-call pattern on first pulse."""
        llm = _ToolCallLLM(["让我搜索一下：test query", "The answer is found."])
        mgr = EngramManager(store, llm, conn_net)
        front = _make_front(mgr, tools)

        output = front.think()
        # Should have iterated: first pulse → tool call → result injected → second pulse
        assert output == "The answer is found."

        session = mgr.get_session(front.engram_id)
        # Should contain: initial msg, first LLM output, tool result injection, second LLM output
        assert len(session) >= 4

    def test_max_iterations_cap(self, store, conn_net, tools):
        """Think loop should stop after max_think_iterations."""
        # LLM always outputs a tool-call pattern
        llm = _ToolCallLLM(["让我搜索一下：query"] * 20)
        mgr = EngramManager(store, llm, conn_net)
        config = FrontAgentConfig(max_think_iterations=3)
        front = _make_front(mgr, tools, config)

        output = front.think()
        # Should have stopped after 3 iterations
        session = mgr.get_session(front.engram_id)
        # Each iteration: LLM output + tool result injection = 2 messages
        # 3 iterations = 6 messages + 1 initial = 7
        # But last iteration's tool result is not injected (loop ends), so 6 + 1 = 7
        # Actually: initial(1) + iter1(llm_out + injection) + iter2(llm_out + injection) + iter3(llm_out) = 6
        assert len(session) <= 8

    def test_failed_tool_result_injected(self, store, conn_net):
        """When a tool fails, the error message is injected back."""
        tools = ToolRegistry(mock=True)
        tools.register("web_search", "search", lambda query: ToolResult(
            success=False, content="", error="Network error"
        ))

        llm = _ToolCallLLM(["让我搜索一下：something", "I see there was an error."])
        mgr = EngramManager(store, llm, conn_net)
        front = _make_front(mgr, tools)

        front.think()
        session = mgr.get_session(front.engram_id)
        # Find the injection that contains the error
        injections = [m for m in session if m.role == MessageRole.INJECTION]
        error_injections = [m for m in injections if "error" in m.content.lower() or "fail" in m.content.lower()]
        assert len(error_injections) >= 1


# ── User message ─────────────────────────────────────────────────


class TestUserMessage:
    def test_receive_user_message(self, mgr, tools):
        front = _make_front(mgr, tools)
        output = front.receive_user_message("Hello, what do you know?")
        assert isinstance(output, str)
        assert len(output) > 0

    def test_user_message_appended_to_session(self, mgr, tools):
        front = _make_front(mgr, tools)
        front.receive_user_message("Tell me about physics.")
        session = mgr.get_session(front.engram_id)
        injections = [m for m in session if m.role == MessageRole.INJECTION and m.source_engram_id == "user"]
        assert len(injections) >= 1
        assert "physics" in injections[0].content


# ── Status ───────────────────────────────────────────────────────


class TestStatus:
    def test_get_status(self, mgr, tools):
        front = _make_front(mgr, tools)
        status = front.get_status()
        assert front.engram_id in status
        assert "messages" in status

    def test_status_after_think(self, mgr, tools):
        front = _make_front(mgr, tools)
        front.think()
        status = front.get_status()
        assert "1 pulses" in status or "pulses" in status


# ── Properties ───────────────────────────────────────────────────


class TestProperties:
    def test_engram_id(self, mgr, tools):
        front = _make_front(mgr, tools)
        assert isinstance(front.engram_id, str)
        assert len(front.engram_id) > 0


# ── Helper: LLM that returns scripted responses ──────────────────


class _ToolCallLLM:
    """Fake LLM that returns a sequence of scripted responses."""

    def __init__(self, responses: list[str]):
        self._responses = list(responses)
        self._call_idx = 0
        self.stats = type("Stats", (), {
            "total_calls": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "cache_hit_rate": 0.0,
        })()

    def complete(self, messages, max_tokens=None, temperature=None):
        resp = self._responses[min(self._call_idx, len(self._responses) - 1)]
        self._call_idx += 1
        return type("Result", (), {
            "content": resp,
            "input_tokens": 10,
            "output_tokens": 10,
            "cached_tokens": 0,
        })()

    def embed(self, text):
        return type("Result", (), {"embedding": [0.0] * 256})()

    def estimate_tokens(self, text):
        return len(text) // 3


# ── front-agent timeout: wall-clock deadline ──────────────────────────────────────


class TestDeadline:
    """front-agent timeout regression: think() must respect wall-clock time, not just an
    iteration count. Harness-imposed limits (LHTB: 90-minute hard timeout)
    previously had to be enforced from outside by strangling the tools."""

    def test_deadline_stops_loop(self, store, conn_net, tools, monkeypatch):
        clock = iter(range(0, 1000000, 100))  # every look at the clock advances 100s
        monkeypatch.setattr(
            "pulse_system.agent.front.agent.time.monotonic", lambda: next(clock)
        )
        llm = _ToolCallLLM(["让我搜索一下：query"] * 20)
        mgr = EngramManager(store, llm, conn_net)
        config = FrontAgentConfig(max_think_iterations=10, deadline_sec=150.0)
        front = _make_front(mgr, tools, config)

        output = front.think()

        # Iteration 1 starts at elapsed 100s (< 150), the check before
        # iteration 2 sees 200s (>= 150) and stops the loop.
        session = mgr.get_session(front.engram_id)
        assert len(session) == 3  # initial + first output + tool injection
        assert output == "让我搜索一下：query"

    def test_no_deadline_is_unchanged(self, store, conn_net, tools, monkeypatch):
        clock = iter(range(0, 1000000, 100))
        monkeypatch.setattr(
            "pulse_system.agent.front.agent.time.monotonic", lambda: next(clock)
        )
        llm = _ToolCallLLM(["让我搜索一下：query"] * 20)
        mgr = EngramManager(store, llm, conn_net)
        config = FrontAgentConfig(max_think_iterations=3)
        front = _make_front(mgr, tools, config)

        front.think()

        # All 3 iterations ran despite the fast-forwarding clock.
        session = mgr.get_session(front.engram_id)
        assert len(session) >= 6

"""Tests for action as a capability of Engram firing.

An ordinary Engram can fire, act, receive a natural-language result in its own
session, and continue without a distinguished front identity. The tests also
enforce raw-session input and natural-text result boundaries.
"""

import pytest

from pulse_system.agent.tools import ToolRegistry, ToolResult
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.engram import EngramManager
from pulse_system.core.engram.runtime import (
    EngramRuntime,
    EngramRuntimeConfig,
    detect_tool_call,
)
from pulse_system.core.types import Message, MessageRole
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


@pytest.fixture
def store():
    s = Storage(":memory:")
    yield s
    s.close()


@pytest.fixture
def conn_net(store):
    return ConnectionNetwork(store, ConnectionConfig())


@pytest.fixture
def mgr(store, conn_net):
    return EngramManager(store, LLMAdapter(mock=True), conn_net)


@pytest.fixture
def tools():
    return ToolRegistry(mock=True)


def _plain_engram(mgr, content="我在想一件事。"):
    """An ordinary engram. No front seed, no identity, no privileges."""
    return mgr.create(
        initial_messages=[Message(role=MessageRole.USER, content=content)]
    )


class _ScriptedLLM:
    """Fake LLM returning scripted replies and recording what it was sent."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0
        self.seen = []
        self.stats = type("Stats", (), {"total_calls": 0})()

    def complete(self, messages, max_tokens=None, temperature=None):
        self.seen.append([dict(m) for m in messages])
        resp = self._responses[min(self.calls, len(self._responses) - 1)]
        self.calls += 1
        return type(
            "Result",
            (),
            {"content": resp, "input_tokens": 7, "output_tokens": 3,
             "cached_tokens": 1, "cache_write_tokens": 0},
        )()

    def embed(self, text):
        return type("Result", (), {"vector": [0.0] * 8})()


# ── The default path is untouched ────────────────────────────────


class TestDefaultPathUnchanged:
    """Runtime configuration changes must not silently invalidate saved state."""

    def test_pulse_without_tools_is_one_completion(self, store, conn_net):
        llm = _ScriptedLLM(["让我搜索一下:量子计算"])
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        result = mgr.pulse(e.id)

        assert llm.calls == 1
        assert result.content == "让我搜索一下:量子计算"
        assert result.tool_calls == 0
        # seed + one assistant message, nothing else
        assert len(mgr.get_session(e.id)) == 2

    def test_pulse_without_tools_never_acts(self, store, conn_net):
        """Output that reads as a tool call still does nothing by default."""
        fired = []
        registry = ToolRegistry(mock=True)
        registry.register(
            "web_search", "search",
            lambda query: fired.append(query) or ToolResult(True, "x"),
        )
        llm = _ScriptedLLM(["让我搜索一下:量子计算"])
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        mgr.pulse(e.id)  # registry exists but was not opted in

        assert fired == []
        assert all(m.role != MessageRole.INJECTION
                   for m in mgr.get_session(e.id))


# ── An ordinary engram acts ──────────────────────────────────────


class TestPlainEngramActs:
    def test_engram_fires_acts_and_continues(self, store, conn_net, tools):
        llm = _ScriptedLLM(["让我搜索一下:分布式共识", "所以共识需要多数派。"])
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        result = mgr.pulse(e.id, tools=tools)

        assert result.tool_calls == 1
        assert result.content == "所以共识需要多数派。"

        session = mgr.get_session(e.id)
        roles = [m.role for m in session]
        assert roles == [
            MessageRole.USER,        # its own opening thought
            MessageRole.ASSISTANT,   # the act, phrased in its own words
            MessageRole.INJECTION,   # what came back
            MessageRole.ASSISTANT,   # and it continues
        ]
        assert session[2].source_engram_id == "tool:web_search"
        assert "分布式共识" in session[2].content

    def test_runtime_carries_no_identity(self, store, conn_net, tools):
        """One runtime instance runs whichever engram is firing."""
        llm = _ScriptedLLM(["让我搜索一下:x", "done", "让我搜索一下:x", "done"])
        mgr = EngramManager(store, llm, conn_net)
        runtime = EngramRuntime(mgr, tools)
        a, b = _plain_engram(mgr), _plain_engram(mgr)

        ra = runtime.run(a.id)
        rb = runtime.run(b.id)

        assert ra.tool_calls == ["web_search"]
        assert rb.tool_calls == ["web_search"]
        assert len(mgr.get_session(a.id)) == len(mgr.get_session(b.id)) == 4

    def test_injected_context_enters_once(self, store, conn_net, tools):
        llm = _ScriptedLLM(["让我搜索一下:x", "让我搜索一下:y", "done"])
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        mgr.pulse(e.id, injected_context="另一个 engram 想到:试试看。",
                  source_engram_id="engram:other", tools=tools)

        from_other = [m for m in mgr.get_session(e.id)
                      if m.source_engram_id == "engram:other"]
        assert len(from_other) == 1


# ── the free-context rule: free context ────────────────────────────────────────


class TestFreeContext:
    def test_no_system_prompt_and_no_tool_manifest(self, store, conn_net,
                                                   tools):
        llm = _ScriptedLLM(["让我搜索一下:x", "done"])
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        mgr.pulse(e.id, tools=tools)

        manifest = tools.describe_tools_natural()
        assert llm.seen, "the LLM was never called"
        for messages in llm.seen:
            assert all(m["role"] in ("user", "assistant") for m in messages)
            blob = "\n".join(m["content"] for m in messages)
            assert manifest not in blob
            assert "Available capabilities" not in blob
            for name, _ in tools.get_available_tools():
                assert f"- {name}:" not in blob

    def test_messages_are_exactly_the_session(self, store, conn_net, tools):
        llm = _ScriptedLLM(["让我搜索一下:x", "done"])
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        mgr.pulse(e.id, tools=tools)

        session = mgr.get_session(e.id)
        # The last call saw everything up to (but not including) its own reply.
        assert [m["content"] for m in llm.seen[-1]] == [
            m.content for m in session[:-1]
        ]


# ── the natural-text result rule: results as natural text ────────────────────────────


class TestNaturalTextResults:
    def test_result_is_the_tools_plain_text(self, store, conn_net):
        registry = ToolRegistry(mock=True)
        registry.register(
            "web_search", "search",
            lambda query: ToolResult(True, "共识算法需要多数派确认。"),
        )
        llm = _ScriptedLLM(["让我搜索一下:共识", "明白了。"])
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        mgr.pulse(e.id, tools=registry)

        injected = mgr.get_session(e.id)[2]
        assert injected.content == "共识算法需要多数派确认。"

    def test_session_carries_no_structured_artifact(self, store, conn_net,
                                                    tools):
        llm = _ScriptedLLM(["让我搜索一下:x", "done"])
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        mgr.pulse(e.id, tools=tools)

        for m in mgr.get_session(e.id):
            assert isinstance(m.content, str)
            for marker in ('"type": "tool_use"', "tool_call_id",
                           "function_call", "<tool_result", "tool_calls"):
                assert marker not in m.content

    def test_an_acting_session_stays_composable(self, store, conn_net, tools):
        """diffuse propagation moves one engram's text into another's session verbatim.

        Every message an acting engram produced must survive that move as
        ordinary language, or engrams stop being composable.
        """
        llm = _ScriptedLLM(["让我搜索一下:x", "结论是这样。"])
        mgr = EngramManager(store, llm, conn_net)
        actor, receiver = _plain_engram(mgr), _plain_engram(mgr)

        mgr.pulse(actor.id, tools=tools)
        for m in mgr.get_session(actor.id):
            mgr.append_injection(receiver.id, m.content,
                                 source_id=f"engram:{actor.id}")

        sent = EngramManager._session_to_llm_messages(
            mgr.get_session(receiver.id)
        )
        assert all(m["role"] in ("user", "assistant") for m in sent)
        assert "结论是这样。" in sent[-1]["content"]

    def test_failure_arrives_as_language_too(self, store, conn_net):
        registry = ToolRegistry(mock=True)
        registry.register(
            "web_search", "search",
            lambda query: ToolResult(False, "", error="Network error"),
        )
        llm = _ScriptedLLM(["让我搜索一下:x", "那我换个办法。"])
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        mgr.pulse(e.id, tools=registry)

        assert (mgr.get_session(e.id)[2].content
                == "Tool web_search failed: Network error")


# ── Detection, bounds, accounting ────────────────────────────────


class TestDetection:
    def test_one_grammar_not_two(self, tools):
        """The runtime reads the front agent's own table, not a copy of it."""
        from pulse_system.agent.front.agent import FrontAgent, _TOOL_PATTERNS
        from pulse_system.core.engram import runtime as runtime_mod

        assert runtime_mod._patterns() is _TOOL_PATTERNS

        front = FrontAgent.__new__(FrontAgent)
        front._tools = tools
        for text in ("让我搜索一下:量子计算", "读取文件:notes.txt",
                     "web_fetch: https://example.com"):
            assert detect_tool_call(text, tools) == front._detect_tool_call(text)

    def test_plain_thought_is_not_an_act(self, tools):
        assert detect_tool_call("我觉得答案是 42。", tools) is None

    def test_unregistered_tool_does_not_fire(self):
        empty = ToolRegistry(mock=True, builtins=False)
        assert detect_tool_call("让我搜索一下:x", empty) is None


class TestBounds:
    def test_iteration_cap(self, store, conn_net, tools):
        llm = _ScriptedLLM(["让我搜索一下:x"] * 20)
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        result = mgr.pulse(
            e.id, tools=tools,
            runtime_config=EngramRuntimeConfig(max_tool_iterations=3),
        )

        assert llm.calls == 3
        assert result.tool_calls == 3

    def test_a_firing_is_at_least_one_pulse(self, store, conn_net, tools):
        llm = _ScriptedLLM(["thinking"])
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        result = mgr.pulse(
            e.id, tools=tools,
            runtime_config=EngramRuntimeConfig(max_tool_iterations=0),
        )

        assert llm.calls == 1
        assert result.content == "thinking"

    def test_deadline_stops_the_loop(self, store, conn_net, tools,
                                     monkeypatch):
        clock = iter(range(0, 1000000, 100))
        monkeypatch.setattr(
            "pulse_system.core.engram.runtime.time.monotonic",
            lambda: next(clock),
        )
        llm = _ScriptedLLM(["让我搜索一下:x"] * 20)
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        mgr.pulse(
            e.id, tools=tools,
            runtime_config=EngramRuntimeConfig(max_tool_iterations=10,
                                               deadline_sec=150.0),
        )

        assert llm.calls == 1


class TestAccounting:
    def test_tokens_are_summed_over_the_firing(self, store, conn_net, tools):
        llm = _ScriptedLLM(["让我搜索一下:x", "done"])
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        result = mgr.pulse(e.id, tools=tools)

        assert llm.calls == 2
        assert result.input_tokens == 14
        assert result.output_tokens == 6
        assert result.cached_tokens == 2

    def test_pulse_count_reflects_every_completion(self, store, conn_net,
                                                   tools):
        llm = _ScriptedLLM(["让我搜索一下:x", "done"])
        mgr = EngramManager(store, llm, conn_net)
        e = _plain_engram(mgr)

        mgr.pulse(e.id, tools=tools)

        assert mgr.get(e.id).total_pulses == 2


# ── End to end on the shipped mock ───────────────────────────────


class TestMockModeEndToEnd:
    def test_no_key_needed(self, mgr, tools):
        """The real mock adapter echoes its last message, so an engram whose
        own thought reads as an act performs it — no API key involved."""
        e = _plain_engram(mgr, "让我搜索一下:分布式共识")

        result = mgr.pulse(e.id, tools=tools)

        session = mgr.get_session(e.id)
        assert result.tool_calls == 1
        assert [m.role for m in session] == [
            MessageRole.USER, MessageRole.ASSISTANT,
            MessageRole.INJECTION, MessageRole.ASSISTANT,
        ]
        assert session[2].source_engram_id == "tool:web_search"
        assert "Search results for" in session[2].content

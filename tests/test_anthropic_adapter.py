"""Tests for the Anthropic adapter (v0.3 / 3.7).

Uses an injected fake client — no anthropic package or network needed.
"""

import pytest

from pulse_system.core.runtime import RuntimeConfig, RuntimeManager
from pulse_system.substrate.llm import AnthropicAdapter, LLMCallError
from pulse_system.substrate.llm.anthropic_adapter import (
    result_from_response,
    to_anthropic_messages,
)


class _Ns:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _fake_response(*, text="hello", uncached=100, read=0, write=0, out=20,
                   stop_reason="end_turn"):
    return _Ns(
        content=[_Ns(type="text", text=text)],
        usage=_Ns(
            input_tokens=uncached,
            cache_read_input_tokens=read,
            cache_creation_input_tokens=write,
            output_tokens=out,
        ),
        stop_reason=stop_reason,
    )


class _FakeMessages:
    def __init__(self, response=None, error=None):
        self._response = response
        self._error = error
        self.last_kwargs = None

    def create(self, **kwargs):
        self.last_kwargs = kwargs
        if self._error is not None:
            raise self._error
        return self._response


class _FakeClient:
    def __init__(self, response=None, error=None):
        self.messages = _FakeMessages(response, error)


# ── Message conversion ───────────────────────────────────────────


class TestToAnthropicMessages:
    def test_breakpoint_on_last_message(self):
        msgs = [
            {"role": "user", "content": "one"},
            {"role": "assistant", "content": "two"},
            {"role": "user", "content": "three"},
        ]
        out = to_anthropic_messages(msgs)
        assert out[0]["content"] == "one"
        assert out[1]["content"] == "two"
        last = out[-1]["content"]
        assert isinstance(last, list)
        assert last[0]["text"] == "three"
        assert last[0]["cache_control"] == {"type": "ephemeral"}

    def test_assistant_first_gets_placeholder_user(self):
        # e.g. plain-text imports seed the session with an assistant message
        msgs = [{"role": "assistant", "content": "imported monologue"}]
        out = to_anthropic_messages(msgs)
        assert out[0]["role"] == "user"
        assert out[1]["role"] == "assistant"

    def test_placeholder_is_byte_stable(self):
        msgs = [{"role": "assistant", "content": "x"}]
        a = to_anthropic_messages(msgs)
        b = to_anthropic_messages(msgs)
        assert a[0] == b[0]  # constant prefix → cacheable

    def test_no_breakpoint_when_disabled(self):
        msgs = [{"role": "user", "content": "hi"}]
        out = to_anthropic_messages(msgs, cache_breakpoint=False)
        assert out[0]["content"] == "hi"


# ── Usage parsing ────────────────────────────────────────────────


class TestResultFromResponse:
    def test_input_tokens_is_total_prompt(self):
        """Anthropic's usage.input_tokens is only the uncached remainder."""
        r = result_from_response(
            _fake_response(uncached=50, read=700, write=250, out=30), "m"
        )
        assert r.input_tokens == 1000
        assert r.cached_tokens == 700
        assert r.cache_write_tokens == 250
        assert r.output_tokens == 30
        assert r.cache_hit

    def test_missing_cache_fields_default_zero(self):
        resp = _Ns(
            content=[_Ns(type="text", text="t")],
            usage=_Ns(input_tokens=10, output_tokens=5),
            stop_reason="end_turn",
        )
        r = result_from_response(resp, "m")
        assert r.input_tokens == 10
        assert r.cached_tokens == 0
        assert r.cache_write_tokens == 0

    def test_joins_text_blocks_skips_others(self):
        resp = _Ns(
            content=[
                _Ns(type="thinking", thinking="..."),
                _Ns(type="text", text="a"),
                _Ns(type="text", text="b"),
            ],
            usage=_Ns(input_tokens=1, output_tokens=1),
            stop_reason="end_turn",
        )
        assert result_from_response(resp, "m").content == "ab"


# ── Adapter behavior ─────────────────────────────────────────────


class TestAnthropicAdapter:
    def test_complete_records_stats(self):
        client = _FakeClient(response=_fake_response(uncached=40, read=60, write=20))
        adapter = AnthropicAdapter(client=client)
        result = adapter.complete([{"role": "user", "content": "hi"}])

        assert result.content == "hello"
        assert result.input_tokens == 120
        stats = adapter.get_stats()
        assert stats.total_calls == 1
        assert stats.total_input_tokens == 120
        assert stats.cached_input_tokens == 60
        assert stats.cache_write_input_tokens == 20
        assert stats.cache_hits == 1

    def test_request_carries_breakpoint_and_model(self):
        client = _FakeClient(response=_fake_response())
        adapter = AnthropicAdapter(client=client, model="claude-opus-4-8",
                                   max_tokens=512)
        adapter.complete([{"role": "user", "content": "hi"}])

        kwargs = client.messages.last_kwargs
        assert kwargs["model"] == "claude-opus-4-8"
        assert kwargs["max_tokens"] == 512
        last_content = kwargs["messages"][-1]["content"]
        assert last_content[0]["cache_control"] == {"type": "ephemeral"}
        assert "temperature" not in kwargs  # rejected by current Claude models

    def test_failure_raises_llm_call_error(self):
        client = _FakeClient(error=RuntimeError("api down"))
        adapter = AnthropicAdapter(client=client)
        with pytest.raises(LLMCallError):
            adapter.complete([{"role": "user", "content": "hi"}])

    def test_embed_unsupported(self):
        adapter = AnthropicAdapter(client=_FakeClient(response=_fake_response()))
        with pytest.raises(NotImplementedError):
            adapter.embed("text")

    def test_duck_type_parity(self):
        adapter = AnthropicAdapter(client=_FakeClient(response=_fake_response()))
        assert adapter.mock is False
        assert adapter.cache_read_discount == pytest.approx(0.1)
        assert adapter.estimate_tokens("hello") >= 1


# ── Budget write premium ─────────────────────────────────────────


class TestCacheWritePremium:
    def test_writes_billed_at_premium(self):
        runtime = RuntimeManager(RuntimeConfig(
            hourly_token_budget=100_000,
            daily_token_budget=100_000,
            cache_read_discount=0.1,
            cache_write_premium=0.25,
        ))
        # prompt 1000 = 700 cached reads + 200 writes + 100 plain; output 50
        runtime.consume_budget(1000, 50, cached_input_tokens=700,
                               cache_write_tokens=200)
        snap = runtime.snapshot()
        # billable = 50 + (1000-700) + 700*0.1 + 200*0.25 = 50+300+70+50 = 470
        assert snap["billable_tokens_today"] == 470

    def test_writes_clamped(self):
        runtime = RuntimeManager(RuntimeConfig(
            hourly_token_budget=100_000, daily_token_budget=100_000,
        ))
        # writes cannot exceed the uncached share of the prompt
        runtime.consume_budget(100, 0, cached_input_tokens=80,
                               cache_write_tokens=500)
        snap = runtime.snapshot()
        # cached=80, writes clamped to 20: 20 + 8 + 5 = 33
        assert snap["billable_tokens_today"] == 20 + int(80 * 0.1) + int(20 * 0.25)

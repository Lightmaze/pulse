"""Tests for the LLM adapter."""

import math

import pytest

from pulse_system.substrate.llm import (
    CompletionResult,
    EmbeddingResult,
    LLMAdapter,
    LLMCallError,
    estimate_tokens,
)


@pytest.fixture
def mock_llm():
    return LLMAdapter(mock=True)


# ── Token estimation ─────────────────────────────────────────────


class TestTokenEstimation:
    def test_estimate_short_text(self):
        assert estimate_tokens("hello") >= 1

    def test_estimate_empty(self):
        assert estimate_tokens("") == 1  # minimum 1

    def test_estimate_long_text(self):
        text = "a" * 3000
        tokens = estimate_tokens(text)
        assert 900 <= tokens <= 1100

    def test_estimate_messages(self, mock_llm: LLMAdapter):
        msgs = [
            {"role": "user", "content": "Hello, how are you?"},
            {"role": "assistant", "content": "I'm doing well, thanks!"},
        ]
        tokens = mock_llm.estimate_messages_tokens(msgs)
        assert tokens > 0


# ── Mock completion ──────────────────────────────────────────────


class TestMockCompletion:
    def test_basic_completion(self, mock_llm: LLMAdapter):
        result = mock_llm.complete([
            {"role": "user", "content": "What is the meaning of life?"}
        ])
        assert isinstance(result, CompletionResult)
        assert "[mock response" in result.content
        assert result.input_tokens > 0
        assert result.output_tokens > 0
        assert "mock" in result.model

    def test_completion_with_history(self, mock_llm: LLMAdapter):
        messages = [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there!"},
            {"role": "user", "content": "How are you?"},
        ]
        result = mock_llm.complete(messages)
        assert "How are you?" in result.content

    def test_empty_messages(self, mock_llm: LLMAdapter):
        result = mock_llm.complete([])
        assert isinstance(result, CompletionResult)

    def test_custom_max_tokens(self, mock_llm: LLMAdapter):
        result = mock_llm.complete(
            [{"role": "user", "content": "test"}],
            max_tokens=100,
        )
        assert isinstance(result, CompletionResult)


# ── Mock embedding ───────────────────────────────────────────────


class TestMockEmbedding:
    def test_basic_embedding(self, mock_llm: LLMAdapter):
        result = mock_llm.embed("Hello world")
        assert isinstance(result, EmbeddingResult)
        assert len(result.vector) == 256
        assert result.input_tokens > 0

    def test_embedding_normalized(self, mock_llm: LLMAdapter):
        result = mock_llm.embed("test text")
        norm = math.sqrt(sum(v * v for v in result.vector))
        assert norm == pytest.approx(1.0, abs=0.01)

    def test_different_texts_different_vectors(self, mock_llm: LLMAdapter):
        r1 = mock_llm.embed("apple")
        r2 = mock_llm.embed("banana")
        assert r1.vector != r2.vector

    def test_same_text_same_vector(self, mock_llm: LLMAdapter):
        r1 = mock_llm.embed("hello")
        r2 = mock_llm.embed("hello")
        assert r1.vector == r2.vector


# ── Statistics tracking ──────────────────────────────────────────


class TestStats:
    def test_initial_stats(self, mock_llm: LLMAdapter):
        stats = mock_llm.get_stats()
        assert stats.total_calls == 0
        assert stats.total_input_tokens == 0
        assert stats.total_output_tokens == 0
        assert stats.cache_hits == 0
        assert stats.cache_misses == 0

    def test_stats_after_completion(self, mock_llm: LLMAdapter):
        mock_llm.complete([{"role": "user", "content": "hi"}])
        stats = mock_llm.get_stats()
        assert stats.total_calls == 1
        assert stats.total_input_tokens > 0
        assert stats.total_output_tokens > 0

    def test_stats_accumulate(self, mock_llm: LLMAdapter):
        mock_llm.complete([{"role": "user", "content": "one"}])
        mock_llm.complete([{"role": "user", "content": "two"}])
        stats = mock_llm.get_stats()
        assert stats.total_calls == 2

    def test_reset_stats(self, mock_llm: LLMAdapter):
        mock_llm.complete([{"role": "user", "content": "hi"}])
        mock_llm.reset_stats()
        stats = mock_llm.get_stats()
        assert stats.total_calls == 0

    def test_cache_hit_rate_zero(self, mock_llm: LLMAdapter):
        stats = mock_llm.get_stats()
        assert stats.cache_hit_rate == 0.0


# ── Cache hit detection ──────────────────────────────────────────


class TestCacheDetection:
    def test_first_call_is_miss(self, mock_llm: LLMAdapter):
        result = mock_llm.complete([{"role": "user", "content": "hi"}])
        assert result.cache_hit is False

    def test_prefix_reuse_is_hit(self, mock_llm: LLMAdapter):
        msgs_v1 = [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi"},
        ]
        mock_llm.complete(msgs_v1)

        msgs_v2 = [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi"},
            {"role": "user", "content": "what's up?"},
        ]
        result = mock_llm.complete(msgs_v2)
        assert result.cache_hit is True

    def test_different_prefix_is_miss(self, mock_llm: LLMAdapter):
        mock_llm.complete([{"role": "user", "content": "hello"}])
        result = mock_llm.complete([{"role": "user", "content": "goodbye"}])
        assert result.cache_hit is False

    def test_cache_hit_stats(self, mock_llm: LLMAdapter):
        base = [{"role": "user", "content": "start"}]
        mock_llm.complete(base)

        extended = base + [{"role": "assistant", "content": "ok"}]
        mock_llm.complete(extended)

        stats = mock_llm.get_stats()
        assert stats.cache_hits >= 1
        assert stats.cache_hit_rate > 0.0


# ── Provider cache contract parsing ──────────────────────────────


class _Ns:
    """Tiny attribute namespace for fake usage objects."""

    def __init__(self, **kw):
        self.__dict__.update(kw)


class TestProviderCacheContract:
    def test_deepseek_usage_field(self):
        from pulse_system.substrate.llm.adapter import _extract_cached_tokens

        usage = _Ns(prompt_tokens=1000, completion_tokens=50,
                    prompt_cache_hit_tokens=768, prompt_cache_miss_tokens=232)
        assert _extract_cached_tokens(usage) == 768

    def test_openai_usage_field(self):
        from pulse_system.substrate.llm.adapter import _extract_cached_tokens

        usage = _Ns(prompt_tokens=2048, completion_tokens=50,
                    prompt_tokens_details=_Ns(cached_tokens=1024))
        assert _extract_cached_tokens(usage) == 1024

    def test_unknown_provider_defaults_zero(self):
        from pulse_system.substrate.llm.adapter import _extract_cached_tokens

        assert _extract_cached_tokens(_Ns(prompt_tokens=100)) == 0
        assert _extract_cached_tokens(None) == 0

    def test_deepseek_takes_precedence(self):
        from pulse_system.substrate.llm.adapter import _extract_cached_tokens

        usage = _Ns(prompt_cache_hit_tokens=64,
                    prompt_tokens_details=_Ns(cached_tokens=128))
        assert _extract_cached_tokens(usage) == 64


class TestMockCacheBounds:
    def test_lru_is_bounded(self, mock_llm: LLMAdapter):
        from pulse_system.substrate.llm.adapter import _MOCK_CACHE_MAX_ENTRIES

        for i in range(_MOCK_CACHE_MAX_ENTRIES + 500):
            mock_llm.complete([{"role": "user", "content": f"msg {i}"}])
        assert len(mock_llm._prefix_cache) <= _MOCK_CACHE_MAX_ENTRIES

    def test_cached_tokens_reported_on_hit(self, mock_llm: LLMAdapter):
        base = [
            {"role": "user", "content": "a long shared prefix " * 20},
            {"role": "assistant", "content": "reply"},
        ]
        mock_llm.complete(base)
        result = mock_llm.complete(base + [{"role": "user", "content": "next"}])
        assert result.cached_tokens > 0
        assert result.cached_tokens <= result.input_tokens


# ── Configuration ────────────────────────────────────────────────


class TestConfiguration:
    def test_default_model(self):
        # deepseek-chat was retired 2026-07-24; the endpoint now serves only
        # deepseek-v4-flash and deepseek-v4-pro. Flash is the default because
        # per-Engram substrate binding binds DeepSeek as the cheap substrate for frequent pulses.
        llm = LLMAdapter(mock=True)
        assert llm.model == "deepseek-v4-flash"
        assert llm.provider == "deepseek"
        assert llm.cache_read_discount == pytest.approx(0.1)

    def test_openai_profile(self):
        llm = LLMAdapter(mock=True, provider="openai")
        assert llm.model == "gpt-4o-mini"
        assert llm.cache_read_discount == pytest.approx(0.5)

    def test_unknown_provider_raises(self):
        with pytest.raises(ValueError, match="Unknown provider"):
            LLMAdapter(mock=True, provider="nonexistent")

    def test_custom_model(self):
        llm = LLMAdapter(mock=True, model="deepseek-reasoner")
        assert llm.model == "deepseek-reasoner"
        result = llm.complete([{"role": "user", "content": "test"}])
        assert "deepseek-reasoner" in result.model

    def test_custom_max_tokens(self):
        llm = LLMAdapter(mock=True, max_tokens=512)
        assert llm.max_tokens == 512


class TestEmptyContentIsAFailure:
    """An empty completion is the substrate failing, not the engram thinking
    nothing. Reasoning models return content="" with finish_reason="length"
    once reasoning_content has eaten the budget. Silently returning "" turns a
    substrate failure into a fabricated observation."""

    def _adapter_returning(self, content, finish_reason):
        # An explicit dummy key keeps the test independent of the caller's
        # environment. The client is replaced on the next line, so nothing
        # here can reach the network.
        from types import SimpleNamespace
        llm = LLMAdapter(provider="deepseek", api_key="not-used-client-is-replaced")
        llm._client = SimpleNamespace(chat=SimpleNamespace(completions=(
            SimpleNamespace(create=lambda **kw: SimpleNamespace(
                choices=[SimpleNamespace(
                    message=SimpleNamespace(content=content),
                    finish_reason=finish_reason)],
                usage=SimpleNamespace(prompt_tokens=10, completion_tokens=400,
                                      prompt_cache_hit_tokens=0))))))
        return llm

    def test_empty_content_raises_rather_than_returning_blank(self):
        llm = self._adapter_returning("", "length")
        with pytest.raises(LLMCallError) as exc:
            llm.complete([{"role": "user", "content": "x"}])
        assert "empty content" in str(exc.value)

    def test_the_error_names_the_budget_as_the_cause(self):
        """A refusal must say what would work -- the rule this project has
        broken more than a dozen times."""
        llm = self._adapter_returning("", "length")
        with pytest.raises(LLMCallError) as exc:
            llm.complete([{"role": "user", "content": "x"}])
        assert "max_tokens" in str(exc.value)

    def test_whitespace_only_counts_as_empty(self):
        llm = self._adapter_returning("   \n  ", "stop")
        with pytest.raises(LLMCallError):
            llm.complete([{"role": "user", "content": "x"}])

    def test_real_content_passes_through(self):
        llm = self._adapter_returning("CALC det(J)", "stop")
        assert llm.complete([{"role": "user", "content": "x"}]).content == "CALC det(J)"

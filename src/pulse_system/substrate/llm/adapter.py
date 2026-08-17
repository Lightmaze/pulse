"""LLM call adapter.

Wraps OpenAI-compatible chat APIs (DeepSeek default).
Provides complete() and embed() as core methods.
Supports mock mode for testing without real API calls.

Cache accounting follows each provider's real contract instead of a local
heuristic: the number of cached input tokens is read from the response
`usage` object (DeepSeek: `prompt_cache_hit_tokens`; OpenAI:
`prompt_tokens_details.cached_tokens`). The local prefix heuristic is kept
only for mock mode, bounded by an LRU.
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

_logger = logging.getLogger("pulse_system.llm")

# Provider profiles. Every OpenAI-compatible endpoint someone is likely to
# already hold a key for gets an entry, so that running this project costs a
# stranger one environment variable rather than a new account.
#
# Each profile carries:
#   base_url             — the provider's OpenAI-compatible endpoint
#   model                — a default chat model that endpoint actually serves
#   api_key_env          — the env var holding the key; "" (falsy) means the
#                          provider needs no key at all (a local server). It is
#                          the empty string and not None because the observatory
#                          feeds this value straight to os.environ.get(), which
#                          raises TypeError on None. Falsy is the contract;
#                          never test it with `is None`.
#   cache_read_discount  — price of a cached input token / an uncached one, used
#                          by the runtime budget model (not for billing). 1.0
#                          means NO discount was confirmed from the provider's
#                          own pricing page. That is deliberately the pessimistic
#                          direction: the budget then charges cached tokens at
#                          full price and under-spends rather than over-spends.
#                          Every entry below states where its number came from,
#                          or says it is unverified. Do not fill one in from
#                          memory.
#   embed_model          — an embeddings model on the same base_url, or None
#                          when the provider has no verified OpenAI-compatible
#                          embeddings endpoint. None falls back to
#                          _DEFAULT_EMBED_MODEL, which only works if you also
#                          pass embed_base_url= pointing at something that
#                          serves it.
#
# Model ids move. Each was read from the provider's own documentation on the
# date noted; pass model=... to override without editing this table.
_PROVIDER_PROFILES: dict[str, dict[str, Any]] = {
    "deepseek": {
        "base_url": "https://api.deepseek.com",
        # deepseek-chat was retired: as of 2026-07-24 the endpoint accepts only
        # deepseek-v4-flash and deepseek-v4-pro (confirmed against /models).
        # Flash is the right default here — per-Engram substrate binding binds DeepSeek as the cheap
        # substrate for high-frequency pulses, and cost structure is meant to
        # shape rhythm rather than be fought.
        "model": "deepseek-v4-flash",
        "api_key_env": "DEEPSEEK_API_KEY",
        "cache_read_discount": 0.1,
        # DeepSeek serves no embeddings endpoint.
        "embed_model": None,
    },
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-4o-mini",
        "api_key_env": "OPENAI_API_KEY",
        "cache_read_discount": 0.5,
        "embed_model": "text-embedding-3-small",
    },
    # ── added 2026-07-25, each verified against the provider's own docs ──
    "gemini": {
        # https://ai.google.dev/gemini-api/docs/openai — Google's OpenAI
        # compatibility layer; the trailing slash is as documented.
        "base_url": "https://generativelanguage.googleapis.com/v1beta/openai/",
        "model": "gemini-3.6-flash",
        "api_key_env": "GEMINI_API_KEY",
        # Verified: ai.google.dev/gemini-api/docs/pricing lists gemini-3.6-flash
        # at $1.50/M input vs $0.15/M cached (and 2.5-flash at $0.30 vs $0.03) —
        # a 0.1 ratio on both.
        "cache_read_discount": 0.1,
        "embed_model": "gemini-embedding-001",
    },
    "openrouter": {
        # https://openrouter.ai/docs/quickstart
        "base_url": "https://openrouter.ai/api/v1",
        # Slug from the model-routing docs. Any slug listed on
        # openrouter.ai/models works; "openrouter/auto-beta" routes for you.
        "model": "deepseek/deepseek-v3.2",
        "api_key_env": "OPENROUTER_API_KEY",
        # UNVERIFIED: OpenRouter bills at the upstream model's rate, so there is
        # no single cache ratio to read off a page. 1.0 = assume no discount.
        "cache_read_discount": 1.0,
        "embed_model": None,
    },
    "ollama": {
        # https://docs.ollama.com/api/openai-compatibility — a local server, so
        # this is the one provider that must build with no key at all.
        "base_url": "http://localhost:11434/v1",
        # The doc's own chat example. Whatever you have pulled works:
        # `ollama pull <model>` then pass model=...
        "model": "gpt-oss:20b",
        "api_key_env": "",  # no key: local. See the header note on falsiness.
        # Local inference is not billed at all, so there is no ratio to verify.
        "cache_read_discount": 1.0,
        # Ollama does serve /v1/embeddings, but its OpenAI-compatibility page
        # shows no model id for it — unverified, so left unset.
        "embed_model": None,
    },
    "groq": {
        # https://console.groq.com/docs/openai
        "base_url": "https://api.groq.com/openai/v1",
        # Production model per console.groq.com/docs/models.
        "model": "openai/gpt-oss-120b",
        "api_key_env": "GROQ_API_KEY",
        # Verified: groq.com/pricing lists gpt-oss-120b at $0.15/M input vs
        # $0.075/M cached. Note this is per-model — llama-3.3-70b-versatile
        # shows no cached price at all, so override if you switch models.
        "cache_read_discount": 0.5,
        "embed_model": None,
    },
    "xai": {
        # https://docs.x.ai/docs/overview
        "base_url": "https://api.x.ai/v1",
        "model": "grok-4.5",
        "api_key_env": "XAI_API_KEY",
        # Verified: docs.x.ai/docs/models prices grok-4.5 at $2.00/M input vs
        # $0.30/M cached (and $4.00 vs $0.60 above 200k) — 0.15 either way.
        "cache_read_discount": 0.15,
        "embed_model": None,
    },
    "moonshot": {
        # https://platform.kimi.ai/docs/api/chat (platform.moonshot.ai now
        # 301s to platform.kimi.ai).
        "base_url": "https://api.moonshot.ai/v1",
        "model": "kimi-k2.6",
        "api_key_env": "MOONSHOT_API_KEY",
        # UNVERIFIED: Kimi prices context caching per model behind pages this
        # check could not read. 1.0 = assume no discount.
        "cache_read_discount": 1.0,
        "embed_model": None,
    },
    "zai": {
        # https://docs.z.ai/api-reference/introduction — Zhipu's GLM API under
        # its international brand.
        "base_url": "https://api.z.ai/api/paas/v4",
        "model": "glm-5.2",
        # The OpenAI-compat docs name no env var; this one is our convention.
        "api_key_env": "ZAI_API_KEY",
        # Verified: docs.z.ai pricing lists glm-5.2 at $1.4/M input vs $0.26/M
        # cached input → 0.186, rounded to 0.19.
        "cache_read_discount": 0.19,
        "embed_model": None,
    },
    "dashscope": {
        # https://docs.qwencloud.com/developer-guides/getting-started/introduction
        # Alibaba's international Model Studio endpoint. The Beijing console
        # uses https://dashscope.aliyuncs.com/compatible-mode/v1 instead, and
        # workspace-scoped *.maas.aliyuncs.com hosts also exist — pass
        # base_url=... for those.
        "base_url": "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
        "model": "qwen3.7-flash",
        "api_key_env": "DASHSCOPE_API_KEY",
        # UNVERIFIED: the Qwen pricing page returned 404 for this check.
        "cache_read_discount": 1.0,
        "embed_model": None,
    },
    "siliconflow": {
        # https://docs.siliconflow.com/en/userguide/quickstart
        "base_url": "https://api.siliconflow.com/v1",
        "model": "deepseek-ai/DeepSeek-R1",
        # The quickstart passes the key inline and names no env var; ours.
        "api_key_env": "SILICONFLOW_API_KEY",
        # UNVERIFIED: no cached-input price confirmed.
        "cache_read_discount": 1.0,
        "embed_model": None,
    },
}

# Historical default, kept so providers with no verified embeddings endpoint
# behave exactly as they did before profiles carried an embed_model.
_DEFAULT_EMBED_MODEL = "text-embedding-3-small"

# The OpenAI client refuses to construct without a non-empty api_key, even
# against a server that ignores it. Ollama's own docs pass a placeholder
# ("required but ignored"), so keyless providers get one here.
_LOCAL_PLACEHOLDER_KEY = "not-needed"

# Anthropic is intentionally absent: it is not OpenAI-compatible and needs
# explicit cache_control breakpoints plus read/write dual accounting
# (cache_read_input_tokens / cache_creation_input_tokens), so it has a
# separate AnthropicAdapter.

_MOCK_CACHE_MAX_ENTRIES = 4096


class LLMCallError(RuntimeError):
    """An LLM call failed after exhausting the client's retries.

    Raised for transport errors, timeouts, rate limits, and server errors.
    Callers (the pulse engine) treat this as retryable at the event level.
    """


@dataclass
class LLMStats:
    total_calls: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    cached_input_tokens: int = 0
    # Tokens written to a provider cache at a write premium (Anthropic's
    # cache_creation_input_tokens). Always 0 for auto-caching providers.
    cache_write_input_tokens: int = 0
    cache_hits: int = 0
    cache_misses: int = 0

    @property
    def cache_hit_rate(self) -> float:
        """Token-weighted hit rate: cached input tokens / total input tokens."""
        if self.total_input_tokens <= 0:
            return 0.0
        return self.cached_input_tokens / self.total_input_tokens


@dataclass
class CompletionResult:
    content: str
    input_tokens: int      # total prompt tokens (cached + uncached + writes)
    output_tokens: int
    cached_tokens: int     # served from the provider cache (read)
    model: str
    cache_write_tokens: int = 0  # written to the cache at a premium (Anthropic)

    @property
    def cache_hit(self) -> bool:
        return self.cached_tokens > 0


@dataclass
class EmbeddingResult:
    vector: list[float]
    input_tokens: int
    model: str


# rough estimate: 1 token ≈ 4 chars for English, ≈ 2 chars for CJK
_AVG_CHARS_PER_TOKEN = 3


def estimate_tokens(text: str) -> int:
    """Rough token count estimate without loading a tokenizer."""
    return max(1, len(text) // _AVG_CHARS_PER_TOKEN)


def estimate_messages_tokens(messages: list[dict[str, str]]) -> int:
    total = 0
    for m in messages:
        total += 4  # role/separators overhead
        total += estimate_tokens(m.get("content", ""))
    return total + 2  # start/end overhead


def _extract_cached_tokens(usage: Any) -> int:
    """Read cached-input-token count from a provider usage object.

    DeepSeek reports `prompt_cache_hit_tokens`; OpenAI reports
    `prompt_tokens_details.cached_tokens`. Unknown providers → 0.
    """
    if usage is None:
        return 0
    v = getattr(usage, "prompt_cache_hit_tokens", None)
    if v is not None:
        return int(v)
    details = getattr(usage, "prompt_tokens_details", None)
    if details is not None:
        v = getattr(details, "cached_tokens", None)
        if v is not None:
            return int(v)
    return 0


class LLMAdapter:
    """Unified LLM interface for OpenAI-compatible providers."""

    def __init__(
        self,
        *,
        provider: str = "deepseek",
        api_key: str | None = None,
        base_url: str | None = None,
        model: str | None = None,
        embed_model: str | None = None,
        embed_base_url: str | None = None,
        max_tokens: int = 2048,
        timeout: float = 60.0,
        max_retries: int = 3,
        mock: bool = False,
        mock_cost_cap: int | None = None,
    ):
        if provider not in _PROVIDER_PROFILES:
            raise ValueError(
                f"Unknown provider: {provider}. Use one of {sorted(_PROVIDER_PROFILES)}"
            )
        profile = _PROVIDER_PROFILES[provider]
        self.provider = provider
        self.model = model or profile["model"]
        self.embed_model = (
            embed_model or profile.get("embed_model") or _DEFAULT_EMBED_MODEL
        )
        self.max_tokens = max_tokens
        self.mock = mock
        # Mock-only performance cap: when set, _mock_complete estimates cost
        # over only the last N messages instead of the whole session. The
        # reply already depends only on the last message, so this changes
        # nothing about behavior or dynamics — only the (heuristic) mock token
        # and cache-hit counts. It exists because the mock prefix-cache scan is
        # O(n^2) in session length: a long mock run where many engrams sustain
        # activity (e.g. a breadth-reward claustrum keeping all clusters live)
        # slows to a crawl. Default None = unchanged, uncapped behavior.
        self.mock_cost_cap = mock_cost_cap
        self.cache_read_discount: float = profile["cache_read_discount"]
        self.stats = LLMStats()

        resolved_base_url = base_url or profile["base_url"]

        self._client: Any = None
        self._embed_client: Any = None
        if not mock:
            from openai import OpenAI

            key_env = profile["api_key_env"]
            if not key_env:
                # A local provider (ollama and friends) authenticates nothing.
                # Refusing here would be the mirror of the bug below: telling
                # someone to set a variable that does not exist and is not read.
                resolved_key = api_key or _LOCAL_PLACEHOLDER_KEY
            else:
                resolved_key = api_key or os.environ.get(key_env)
                if not resolved_key:
                    # Without this, the OpenAI client raises its own error naming
                    # OPENAI_API_KEY — the wrong variable for every provider in
                    # the table but one. A newcomer sets OPENAI_API_KEY, fails
                    # identically, and concludes the project is broken. Name the
                    # variable that would actually work, and the way to run with
                    # no key at all.
                    raise LLMCallError(
                        f"No API key for provider {provider!r}. Set "
                        f"{key_env}, pass api_key=..., or use "
                        f"LLMAdapter(mock=True) to run offline."
                    )
            # The OpenAI client retries connection errors, 408/409/429 and
            # 5xx with exponential backoff up to max_retries.
            self._client = OpenAI(
                api_key=resolved_key,
                base_url=resolved_base_url,
                timeout=timeout,
                max_retries=max_retries,
            )
            if embed_base_url and embed_base_url != resolved_base_url:
                self._embed_client = OpenAI(
                    api_key=resolved_key,
                    base_url=embed_base_url,
                    timeout=timeout,
                    max_retries=max_retries,
                )
            else:
                self._embed_client = self._client

        # Mock-mode prefix cache: hash of seen prefixes -> token estimate.
        # Bounded LRU so long-running mock sessions don't leak.
        self._prefix_cache: OrderedDict[str, int] = OrderedDict()
        # Stats and the mock cache are mutated from parallel pulse threads.
        self._stats_lock = threading.Lock()

    def complete(
        self,
        messages: list[dict[str, str]],
        *,
        max_tokens: int | None = None,
        temperature: float = 0.7,
    ) -> CompletionResult:
        with self._stats_lock:
            self.stats.total_calls += 1
        max_tok = max_tokens or self.max_tokens

        if self.mock:
            return self._mock_complete(messages, max_tok)

        try:
            response = self._client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=max_tok,
                temperature=temperature,
            )
        except Exception as e:  # client retries are exhausted at this point
            _logger.warning("LLM call failed (%s): %s", type(e).__name__, e)
            raise LLMCallError(f"{type(e).__name__}: {e}") from e
        choice = response.choices[0]
        usage = response.usage

        input_tokens = usage.prompt_tokens if usage else estimate_messages_tokens(messages)
        output_tokens = usage.completion_tokens if usage else estimate_tokens(choice.message.content or "")
        cached_tokens = _extract_cached_tokens(usage)

        self._record(input_tokens, output_tokens, cached_tokens)

        # Empty content is never a pulse that "thought nothing" — it is the
        # substrate failing to answer, and it must say so. Reasoning models
        # can spend the completion budget on reasoning_content and then return
        # content="" with finish_reason="length". Silently returning "" would
        # turn a substrate failure into a fabricated observation about the
        # engram.
        if not (choice.message.content or "").strip():
            reason = getattr(choice, "finish_reason", None)
            hint = (
                " — the completion budget was exhausted before any content was "
                "emitted (reasoning models spend it on reasoning_content first); "
                "raise max_tokens"
                if reason == "length" else ""
            )
            raise LLMCallError(
                f"{self.model} returned empty content "
                f"(finish_reason={reason!r}){hint}"
            )

        return CompletionResult(
            content=choice.message.content or "",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            model=self.model,
        )

    def embed(self, text: str) -> EmbeddingResult:
        if self.mock:
            return self._mock_embed(text)

        try:
            response = self._embed_client.embeddings.create(
                model=self.embed_model,
                input=text,
            )
        except Exception as e:
            _logger.warning("Embedding call failed (%s): %s", type(e).__name__, e)
            raise LLMCallError(f"{type(e).__name__}: {e}") from e
        data = response.data[0]
        input_tokens = response.usage.prompt_tokens if response.usage else estimate_tokens(text)

        return EmbeddingResult(
            vector=data.embedding,
            input_tokens=input_tokens,
            model=self.embed_model,
        )

    def estimate_tokens(self, text: str) -> int:
        return estimate_tokens(text)

    def estimate_messages_tokens(self, messages: list[dict[str, str]]) -> int:
        return estimate_messages_tokens(messages)

    def get_stats(self) -> LLMStats:
        return self.stats

    def reset_stats(self) -> None:
        self.stats = LLMStats()

    # ── Internal ─────────────────────────────────────────────────

    def _record(self, input_tokens: int, output_tokens: int, cached_tokens: int) -> None:
        with self._stats_lock:
            self.stats.total_input_tokens += input_tokens
            self.stats.total_output_tokens += output_tokens
            self.stats.cached_input_tokens += cached_tokens
            if cached_tokens > 0:
                self.stats.cache_hits += 1
            else:
                self.stats.cache_misses += 1

    # ── Mock implementations ─────────────────────────────────────

    def _mock_complete(
        self, messages: list[dict[str, str]], max_tokens: int
    ) -> CompletionResult:
        last_content = messages[-1].get("content", "") if messages else ""
        reply = f"[mock response to: {last_content[:60]}]"

        # Cost is estimated over at most the last mock_cost_cap messages; the
        # reply above already uses only messages[-1], so behavior is identical.
        cost_msgs = (messages if self.mock_cost_cap is None
                     else messages[-self.mock_cost_cap:])
        input_tokens = estimate_messages_tokens(cost_msgs)
        output_tokens = estimate_tokens(reply)
        cached_tokens = self._mock_cached_tokens(cost_msgs)

        self._record(input_tokens, output_tokens, cached_tokens)

        return CompletionResult(
            content=reply,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            model=f"{self.model}-mock",
        )

    def _mock_embed(self, text: str) -> EmbeddingResult:
        h = hashlib.sha256(text.encode()).digest()
        dim = 256
        vector = [((b % 200) - 100) / 100.0 for b in h * (dim // len(h) + 1)][:dim]
        norm = sum(v * v for v in vector) ** 0.5
        vector = [v / norm for v in vector]

        return EmbeddingResult(
            vector=vector,
            input_tokens=estimate_tokens(text),
            model=f"{self.embed_model}-mock",
        )

    # ── Mock prefix cache (heuristic, mock mode only) ────────────

    def _mock_cached_tokens(self, messages: list[dict[str, str]]) -> int:
        """Estimate cached tokens as the longest previously-seen prefix."""
        with self._stats_lock:
            cached = 0
            for i in range(len(messages) - 1, 0, -1):
                key = self._messages_hash(messages[:i])
                if key in self._prefix_cache:
                    cached = self._prefix_cache[key]
                    self._prefix_cache.move_to_end(key)
                    break

            self._update_prefix_cache(messages)
            return cached

    def _update_prefix_cache(self, messages: list[dict[str, str]]) -> None:
        # caller holds _stats_lock
        for i in range(1, len(messages) + 1):
            key = self._messages_hash(messages[:i])
            self._prefix_cache[key] = estimate_messages_tokens(messages[:i])
            self._prefix_cache.move_to_end(key)
        while len(self._prefix_cache) > _MOCK_CACHE_MAX_ENTRIES:
            self._prefix_cache.popitem(last=False)

    @staticmethod
    def _messages_hash(messages: list[dict[str, str]]) -> str:
        parts = []
        for m in messages:
            parts.append(f"{m.get('role', '')}:{m.get('content', '')}")
        return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]

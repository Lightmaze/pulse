"""Anthropic adapter (v0.3 / 3.7).

Anthropic's cache contract differs from the OpenAI-compatible providers in
three ways this adapter has to own:

- Caching is **explicit**: a `cache_control: {"type": "ephemeral"}` breakpoint
  must be placed in the request. Because engram sessions are append-only, the
  breakpoint goes on the *last* message each call — the cached prefix grows
  with the session and each call's write is incremental.
- Usage reports **reads and writes separately**: `cache_read_input_tokens`
  (billed ~0.1x) and `cache_creation_input_tokens` (billed 1.25x for the
  5-minute TTL). `usage.input_tokens` is only the *uncached remainder* — this
  adapter reports `input_tokens` as the full prompt size (remainder + reads +
  writes) so token_count/succession semantics match the other providers, and
  surfaces writes via `cache_write_tokens` for the budget's write premium.
- The minimum cacheable prefix is model-dependent (1024–4096 tokens); shorter
  prefixes silently don't cache. Young engrams therefore show zero cache
  activity — expected, not a bug.

Notes:
- Requires the `anthropic` package (`uv add anthropic`); imported lazily.
- `temperature` is ignored: current Claude models reject sampling parameters.
- `embed()` is unsupported (Anthropic has no embeddings endpoint) — pair this
  adapter with an embedding-capable provider if the embedding gate is needed.
- High-frequency pulse networks may prefer a cheaper model than the default;
  pass `model=` explicitly to change it.
"""

from __future__ import annotations

import logging
import os
import threading

from .adapter import (
    CompletionResult,
    EmbeddingResult,
    LLMCallError,
    LLMStats,
    estimate_messages_tokens,
    estimate_tokens,
)

_logger = logging.getLogger("pulse_system.llm.anthropic")

_DEFAULT_MODEL = "claude-opus-4-8"

# Anthropic cache economics (5-minute TTL): reads ~0.1x, writes 1.25x
# (a 0.25 premium on top of nominal input price).
_CACHE_READ_DISCOUNT = 0.1
_CACHE_WRITE_PREMIUM = 0.25

_SESSION_START_PLACEHOLDER = "(session start)"


def to_anthropic_messages(
    messages: list[dict[str, str]], *, cache_breakpoint: bool = True
) -> list[dict]:
    """Convert plain role/content dicts to Anthropic message params.

    - Anthropic requires the first message to be a user turn; sessions seeded
      with an assistant message (e.g. plain-text imports) get a constant
      placeholder user turn prepended (constant → byte-stable → cacheable).
    - With cache_breakpoint, the last message's content becomes a content
      block carrying `cache_control` so the whole prefix is cached.
    """
    out: list[dict] = []
    for m in messages:
        out.append({"role": m.get("role", "user"), "content": m.get("content", "")})

    if out and out[0]["role"] != "user":
        out.insert(0, {"role": "user", "content": _SESSION_START_PLACEHOLDER})

    if cache_breakpoint and out:
        last = out[-1]
        last["content"] = [
            {
                "type": "text",
                "text": last["content"],
                "cache_control": {"type": "ephemeral"},
            }
        ]
    return out


def result_from_response(response, model: str) -> CompletionResult:
    """Build a CompletionResult from an Anthropic Messages response.

    `input_tokens` is reported as the *total* prompt size — Anthropic's
    `usage.input_tokens` alone is only the uncached remainder.
    """
    usage = getattr(response, "usage", None)
    uncached = getattr(usage, "input_tokens", 0) or 0
    cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
    cache_write = getattr(usage, "cache_creation_input_tokens", 0) or 0
    output_tokens = getattr(usage, "output_tokens", 0) or 0

    parts: list[str] = []
    for block in getattr(response, "content", []) or []:
        if getattr(block, "type", None) == "text":
            parts.append(getattr(block, "text", "") or "")
    content = "".join(parts)

    stop_reason = getattr(response, "stop_reason", None)
    if stop_reason == "refusal":
        _logger.warning("Anthropic returned stop_reason=refusal; content may be empty")

    return CompletionResult(
        content=content,
        input_tokens=uncached + cache_read + cache_write,
        output_tokens=output_tokens,
        cached_tokens=cache_read,
        cache_write_tokens=cache_write,
        model=model,
    )


class AnthropicAdapter:
    """LLM adapter for Anthropic's Messages API (duck-type of LLMAdapter)."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str = _DEFAULT_MODEL,
        max_tokens: int = 2048,
        timeout: float = 120.0,
        max_retries: int = 3,
        cache_breakpoints: bool = True,
        client=None,
    ):
        self.provider = "anthropic"
        self.model = model
        self.max_tokens = max_tokens
        self.mock = False  # duck-type parity with LLMAdapter
        self.cache_read_discount = _CACHE_READ_DISCOUNT
        self.cache_write_premium = _CACHE_WRITE_PREMIUM
        self.cache_breakpoints = cache_breakpoints
        self.stats = LLMStats()
        self._stats_lock = threading.Lock()

        if client is not None:
            self._client = client  # injected (tests)
        else:
            import anthropic

            self._client = anthropic.Anthropic(
                api_key=api_key or os.environ.get("ANTHROPIC_API_KEY"),
                timeout=timeout,
                max_retries=max_retries,
            )

    def complete(
        self,
        messages: list[dict[str, str]],
        *,
        max_tokens: int | None = None,
        temperature: float = 0.7,  # accepted for interface parity, ignored
    ) -> CompletionResult:
        with self._stats_lock:
            self.stats.total_calls += 1

        params = to_anthropic_messages(
            messages, cache_breakpoint=self.cache_breakpoints
        )
        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=max_tokens or self.max_tokens,
                messages=params,
            )
        except Exception as e:
            _logger.warning("Anthropic call failed (%s): %s", type(e).__name__, e)
            raise LLMCallError(f"{type(e).__name__}: {e}") from e

        result = result_from_response(response, self.model)
        with self._stats_lock:
            self.stats.total_input_tokens += result.input_tokens
            self.stats.total_output_tokens += result.output_tokens
            self.stats.cached_input_tokens += result.cached_tokens
            self.stats.cache_write_input_tokens += result.cache_write_tokens
            if result.cached_tokens > 0:
                self.stats.cache_hits += 1
            else:
                self.stats.cache_misses += 1
        return result

    def embed(self, text: str) -> EmbeddingResult:
        raise NotImplementedError(
            "Anthropic has no embeddings endpoint. Use an embedding-capable "
            "provider (e.g. LLMAdapter with OpenAI) for embedding paths."
        )

    def estimate_tokens(self, text: str) -> int:
        return estimate_tokens(text)

    def estimate_messages_tokens(self, messages: list[dict[str, str]]) -> int:
        return estimate_messages_tokens(messages)

    def get_stats(self) -> LLMStats:
        return self.stats

    def reset_stats(self) -> None:
        self.stats = LLMStats()

"""substrate registry: substrate adapter registry — per-engram compute binding.

per-Engram substrate binding (多基质绑定): different engrams may bind different substrate
instances even within one modality — cheap fast thinkers on DeepSeek,
deep reasoners on Claude, long-context readers elsewhere. Cost-rhythm
coupling needs no extra machinery: each adapter carries its own price
characteristics and the budget model shapes pulse rhythm accordingly.

The registry maps binding names to adapter instances (duck-typed:
complete/embed/get_stats/cache_read_discount). The default adapter
serves unbound engrams and network-level services (embedding gates,
connection initialization) — embeddings are a network service, not a
per-engram choice.
"""

from __future__ import annotations

import logging

from .adapter import LLMStats

_logger = logging.getLogger("pulse_system.substrate")

DEFAULT = "default"


class SubstrateRegistry:
    """Named substrate adapters with a mandatory default."""

    def __init__(self, default_adapter):
        self._adapters: dict[str, object] = {DEFAULT: default_adapter}

    def register(self, name: str, adapter) -> None:
        if name == DEFAULT:
            raise ValueError("'default' is reserved; pass it to the constructor")
        self._adapters[name] = adapter
        _logger.info("substrate registered: %s (%s)", name,
                     getattr(adapter, "model", "?"))

    def get(self, name: str | None = None):
        """Resolve a binding name; unknown/None falls back to the default."""
        if name is None:
            return self._adapters[DEFAULT]
        adapter = self._adapters.get(name)
        if adapter is None:
            _logger.warning("unknown substrate binding %r, using default", name)
            return self._adapters[DEFAULT]
        return adapter

    def names(self) -> list[str]:
        return list(self._adapters.keys())

    def has(self, name: str) -> bool:
        return name in self._adapters

    def combined_stats(self) -> LLMStats:
        """Summed usage across all distinct adapters (budget accounting)."""
        total = LLMStats()
        for adapter in {id(a): a for a in self._adapters.values()}.values():
            s = adapter.get_stats()
            total.total_calls += s.total_calls
            total.total_input_tokens += s.total_input_tokens
            total.total_output_tokens += s.total_output_tokens
            total.cached_input_tokens += s.cached_input_tokens
            total.cache_write_input_tokens += getattr(
                s, "cache_write_input_tokens", 0
            )
            total.cache_hits += s.cache_hits
            total.cache_misses += s.cache_misses
        return total

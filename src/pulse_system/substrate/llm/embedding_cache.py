"""Per-key embedding cache (v0.4 / 4.4).

Embedding calls are one of the dominant real-mode costs: the clone
activation gate re-embeds every candidate engram's session on every user
message even though sessions change slowly. This cache keys vectors by a
caller-chosen identity (typically an engram id) and invalidates on content
hash, so an unchanged session costs zero embedding calls.
"""

from __future__ import annotations

import hashlib
import threading
from collections import OrderedDict


class EmbeddingCache:
    """LRU cache of embedding vectors keyed by (key, content-hash)."""

    def __init__(self, llm, max_entries: int = 1024):
        self._llm = llm
        self._max_entries = max_entries
        self._cache: OrderedDict[str, tuple[str, list[float]]] = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key: str, text: str) -> list[float]:
        """Return the embedding for text, reusing the cached vector when the
        content under this key is unchanged."""
        digest = hashlib.sha256(text.encode()).hexdigest()
        with self._lock:
            entry = self._cache.get(key)
            if entry is not None and entry[0] == digest:
                self._cache.move_to_end(key)
                self.hits += 1
                return entry[1]

        vector = self._llm.embed(text).vector

        with self._lock:
            self.misses += 1
            self._cache[key] = (digest, vector)
            self._cache.move_to_end(key)
            while len(self._cache) > self._max_entries:
                self._cache.popitem(last=False)
        return vector

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._cache.pop(key, None)

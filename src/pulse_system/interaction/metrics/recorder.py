"""Dynamics metrics recording (interaction/metrics/).

The data side of the observability interface. Runtime observations pass
through this recorder so measurement remains independent of presentation.

Design constraints:
- Zero blocking on the tick path: record() appends to an in-memory deque;
  file flushing is batched.
- Optional persistence: pass a path to also append JSONL; None keeps
  everything in memory (tests, casual CLI runs).
- This is runtime-sideband observability data (the free-context rule): it never enters
  any LLM context.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from collections import Counter, deque
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from pulse_system.core.runtime.publication import (
    RuntimePublicationError,
    RuntimePublicationPermit,
)

_logger = logging.getLogger("pulse_system.metrics")

_DEFAULT_MEMORY_CAP = 50_000
_FLUSH_EVERY = 200
DEFAULT_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_ARCHIVE_COUNT = 3


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class MetricsRecorder:
    """Bounded JSONL recorder with in-memory series access.

    The active file is capped at ``max_bytes``.  Completed segments are named
    ``<path>.1`` (newest) through ``<path>.<archive_count>`` (oldest), so the
    default managed footprint is at most 256 MiB. Pre-upgrade oversized files
    are moved to an explicit ``.legacy`` name and preserved outside that
    budget; the recorder never silently erases historical life. Every rename uses
    :func:`os.replace`: interruption can leave a gap in archive numbering, but
    never a half-renamed active file.  A later flush simply fills the active
    path again and the next rotation compacts the finite archive set.

    A single encoded event larger than ``max_bytes`` remains available through
    the in-memory recorder but is refused from persistence.  Letting one
    observability event silently defeat the storage bound would be worse than
    dropping that sideband record and warning.
    """

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        memory_cap: int = _DEFAULT_MEMORY_CAP,
        flush_every: int = _FLUSH_EVERY,
        max_bytes: int = DEFAULT_MAX_BYTES,
        archive_count: int = DEFAULT_ARCHIVE_COUNT,
        publication_permit: RuntimePublicationPermit | None = None,
    ):
        if memory_cap <= 0:
            raise ValueError("memory_cap must be positive")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if archive_count < 0:
            raise ValueError("archive_count must be non-negative")
        if (
            publication_permit is not None
            and type(publication_permit) is not RuntimePublicationPermit
        ):
            raise TypeError(
                "publication_permit must be a RuntimePublicationPermit or None"
            )
        self._path = Path(path) if path is not None else None
        self._publication_permit = publication_permit
        self._events: deque[dict] = deque(maxlen=memory_cap)
        self._unflushed: list[dict] = []
        self._counts: Counter = Counter()
        self._lock = threading.Lock()
        # 1 = per-event durability, for runs meant to be watched live by the
        # observatory tailer; the 200 default keeps batch runs cheap.
        self._flush_every = max(flush_every, 1)
        self._max_bytes = max_bytes
        self._archive_count = archive_count
        if self._path is not None:
            with self._persistence_guard():
                self._path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, event_type: str, **payload) -> None:
        event = {"t": _now_iso(), "type": event_type, **payload}
        with self._lock:
            self._events.append(event)
            self._counts[event_type] += 1
            if self._path is not None:
                self._unflushed.append(event)
                if len(self._unflushed) >= self._flush_every:
                    self._flush_locked()

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    @contextmanager
    def _persistence_guard(self):
        """Linearize Runtime-owned metrics persistence with publication revoke.

        A recorder without a permit remains a standalone/offline utility.
        Runtime composition always supplies its typed lifecycle permit, so a
        late worker may still add in-memory evidence after revocation but can
        no longer rotate, append, or create a metrics file.
        """

        permit = self._publication_permit
        if permit is None:
            yield
            return
        with permit.transaction_guard():
            yield

    def _flush_locked(self) -> None:
        if self._path is None or not self._unflushed:
            return
        encoded = [
            (json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8")
            for event in self._unflushed
        ]
        committed = 0
        try:
            with self._persistence_guard():
                while committed < len(encoded):
                    line = encoded[committed]
                    if len(line) > self._max_bytes:
                        _logger.warning(
                            "metrics event is %d bytes, above the %d-byte file cap; "
                            "persistence skipped",
                            len(line),
                            self._max_bytes,
                        )
                        committed += 1
                        del self._unflushed[:1]
                        continue

                    current_size = self._file_size(self._path)
                    if current_size >= self._max_bytes:
                        self._rotate_locked()
                        current_size = 0

                    capacity = self._max_bytes - current_size
                    end = committed
                    chunk_size = 0
                    while end < len(encoded):
                        candidate = encoded[end]
                        if len(candidate) > self._max_bytes:
                            break
                        if chunk_size + len(candidate) > capacity:
                            break
                        chunk_size += len(candidate)
                        end += 1

                    if end == committed:
                        self._rotate_locked()
                        continue

                    with self._path.open("ab") as f:
                        f.write(b"".join(encoded[committed:end]))
                    written = end - committed
                    committed = end
                    del self._unflushed[:written]
        except (OSError, RuntimePublicationError) as e:
            # Observability must never take the engine down.  In particular,
            # a revoked Runtime retains the event in memory and performs no
            # fallback persistence outside its lifecycle gate.
            _logger.warning("metrics flush failed: %s", e)

    @staticmethod
    def _file_size(path: Path) -> int:
        try:
            return path.stat().st_size
        except FileNotFoundError:
            return 0

    def _archive_path(self, index: int) -> Path:
        assert self._path is not None
        return self._path.with_name(f"{self._path.name}.{index}")

    def _preserve_legacy_locked(self, source: Path) -> Path:
        """Move a pre-upgrade oversized segment aside without deleting it."""

        base = self._path.with_name(f"{self._path.name}.legacy")
        destination = base
        suffix = 1
        while destination.exists():
            suffix += 1
            destination = self._path.with_name(
                f"{self._path.name}.legacy.{suffix}"
            )
        os.replace(source, destination)
        _logger.warning(
            "preserved oversized legacy metrics segment at %s; it is outside "
            "the managed retention budget",
            destination,
        )
        return destination

    def _rotate_locked(self) -> None:
        """Atomically publish the active segment and enforce finite retention."""
        assert self._path is not None
        if not self._path.exists():
            return

        # Existing worlds may predate bounded metrics by years. Preserve that
        # history under an explicit unmanaged legacy name; never silently
        # delete it merely because the new active stream has a size policy.
        if self._file_size(self._path) > self._max_bytes:
            self._preserve_legacy_locked(self._path)
            return

        if self._archive_count == 0:
            self._path.unlink()
            return

        oldest = self._archive_path(self._archive_count)
        if oldest.exists():
            if self._file_size(oldest) > self._max_bytes:
                self._preserve_legacy_locked(oldest)
            else:
                oldest.unlink()
        for index in range(self._archive_count - 1, 0, -1):
            source = self._archive_path(index)
            if source.exists():
                os.replace(source, self._archive_path(index + 1))
        os.replace(self._path, self._archive_path(1))

        # Normal segments are already <= max_bytes.  This also handles a
        # a legacy unbounded archive: it is preserved under an explicit
        # unmanaged name, never kept as a hidden exception to this budget.
        budget = self._max_bytes * self._archive_count
        used = 0
        for index in range(1, self._archive_count + 1):
            archive = self._archive_path(index)
            if not archive.exists():
                continue
            size = self._file_size(archive)
            if size > self._max_bytes or used + size > budget:
                if size > self._max_bytes:
                    self._preserve_legacy_locked(archive)
                else:
                    archive.unlink()
                continue
            used += size

    # ── Read side ────────────────────────────────────────────────

    def events(self, event_type: str | None = None) -> list[dict]:
        with self._lock:
            if event_type is None:
                return list(self._events)
            return [e for e in self._events if e["type"] == event_type]

    def series(self, event_type: str, field: str) -> list[tuple[str, object]]:
        """(timestamp, value) pairs for one field of one event type."""
        return [
            (e["t"], e.get(field))
            for e in self.events(event_type)
            if field in e
        ]

    def summary(self) -> dict:
        """Counts per event type plus latest heartbeat, for /status and observability interface."""
        with self._lock:
            out: dict = {"event_counts": dict(self._counts)}
            for e in reversed(self._events):
                if e["type"] == "heartbeat":
                    out["heartbeat"] = {
                        "active": e.get("active"),
                        "total": e.get("total"),
                        "ratio": e.get("ratio"),
                    }
                    break
            return out

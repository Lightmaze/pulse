"""Sensory channels — continuous input sources for sensory-cortex engrams.

A channel is anything with ``poll() -> list[str]``. Channels own their own
dedup/thresholds so polling every tick is safe. A sensory-cortex engram is
not a special Engram type: it is an ordinary Engram bound
to a channel; the engram itself processes the input by calling its own
substrate. The Engram remains the subject of perception.
"""

from __future__ import annotations

import logging
import hashlib
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Callable, Protocol

_logger = logging.getLogger("pulse_system.sensory")

_MAX_ITEM_CHARS = 4_000


class Channel(Protocol):
    def poll(self) -> list[str]: ...


class FileWatchChannel:
    """Watches a directory (the 书房 harness): new or modified files whose
    names match the pattern are emitted as natural-text items."""

    def __init__(
        self,
        path: str | Path,
        pattern: str = "*",
        *,
        emit_initial: bool = False,
        state_path: str | Path | None = None,
    ):
        self._path = Path(path)
        self._pattern = pattern
        self._state_path = Path(state_path) if state_path is not None else None
        self._seen: dict[str, str] = self._load_state()
        if not emit_initial and self._path.exists() and not self._seen:
            for f in self._path.glob(pattern):
                if f.is_file() and not self._is_state_file(f):
                    self._seen[str(f)] = self._fingerprint(f)
            self._save_state()

    def poll(self) -> list[str]:
        items = self.poll_fingerprinted()
        for path, _fingerprint, _content, _first_time in items:
            self._seen[path] = _fingerprint
        self._save_state()
        return [content for _path, _fingerprint, content, _first_time in items]

    def poll_fingerprinted(self) -> list[tuple[str, str, str, bool]]:
        """Return changes without acknowledging them.

        The Runtime uses this seam to enqueue the item durably first and calls
        :meth:`acknowledge` only after that commit.  ``poll`` keeps the legacy
        consume-on-read behavior for existing callers.
        """

        if not self._path.exists():
            return []
        items: list[tuple[str, str, str, bool]] = []
        for f in sorted(self._path.glob(self._pattern)):
            if not f.is_file() or self._is_state_file(f):
                continue
            key = str(f)
            try:
                fingerprint = self._fingerprint(f)
            except OSError as e:
                _logger.warning("unreadable watched file %s: %s", f, e)
                continue
            if self._seen.get(key) == fingerprint:
                continue
            first_time = key not in self._seen
            try:
                content = f.read_text(encoding="utf-8", errors="replace")
            except OSError as e:
                _logger.warning("unreadable watched file %s: %s", f, e)
                continue
            if len(content) > _MAX_ITEM_CHARS:
                content = content[:_MAX_ITEM_CHARS] + "\n[... truncated]"
            verb = "出现了新文件" if first_time else "文件有更新"
            items.append((key, fingerprint, f"({verb}: {f.name})\n{content}", first_time))
        return items

    def acknowledge(self, path: str, fingerprint: str) -> None:
        self._seen[path] = fingerprint
        self._save_state()

    def _load_state(self) -> dict[str, str]:
        if self._state_path is None:
            return {}
        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError):
            return {}
        values = raw.get("fingerprints") if isinstance(raw, dict) else None
        if not isinstance(values, dict):
            return {}
        return {
            key: value
            for key, value in values.items()
            if isinstance(key, str) and isinstance(value, str)
        }

    def _save_state(self) -> None:
        if self._state_path is None:
            return
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {"version": 1, "fingerprints": dict(sorted(self._seen.items()))},
            ensure_ascii=False,
        )
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                dir=self._state_path.parent,
                prefix=".sensory-state-",
                delete=False,
            ) as stream:
                temporary = stream.name
                stream.write(payload + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self._state_path)
            temporary = None
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass

    @staticmethod
    def _fingerprint(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(64 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _is_state_file(self, path: Path) -> bool:
        return (
            self._state_path is not None
            and path.resolve() == self._state_path.resolve()
        )


class CallableChannel:
    """Wraps any zero-arg callable returning list[str] (feeds, APIs, ...)."""

    def __init__(self, fn: Callable[[], list[str]]):
        self._fn = fn

    def poll(self) -> list[str]:
        try:
            return list(self._fn() or [])
        except Exception as e:  # a broken feed must not stall the tick
            _logger.warning("callable channel failed: %s", e)
            return []


class InteroceptionChannel:
    """Renders runtime state as natural-language self-perception.

    The bound engram turns numbers into lived experience ("我现在很专注",
    "有什么在后台消耗着我") — the emergence path for affect before/without
    future multimodal coupling. Emits at most once per interval, and only when the state changed.
    """

    def __init__(self, snapshot_fn: Callable[[], dict], *,
                 interval_seconds: float = 60.0):
        self._snapshot_fn = snapshot_fn
        self._interval = interval_seconds
        self._last_emit: float | None = None
        self._last_render = ""

    def poll(self) -> list[str]:
        now = time.monotonic()
        if self._last_emit is not None and now - self._last_emit < self._interval:
            return []
        try:
            snap = self._snapshot_fn()
        except Exception as e:
            _logger.warning("interoception snapshot failed: %s", e)
            return []
        render = self._render(snap)
        if render == self._last_render:
            return []
        self._last_emit = now
        self._last_render = render
        return [render]

    @staticmethod
    def _render(snap: dict) -> str:
        parts = ["(内感受:当前系统状态)"]
        hb = snap.get("heartbeat") or {}
        if hb.get("total"):
            parts.append(
                f"心潮 {hb.get('active', '?')}/{hb.get('total', '?')}"
                f" = {hb.get('ratio', 0):.0%}"
            )
        if "total_pulses" in snap:
            parts.append(f"累计脉冲 {snap['total_pulses']} 次")
        if "billable_tokens_today" in snap:
            parts.append(
                f"今日消耗 {snap['billable_tokens_today']} tokens"
                f"(剩余 {snap.get('daily_budget_remaining', '?')})"
            )
        return ",".join(parts) + "。"

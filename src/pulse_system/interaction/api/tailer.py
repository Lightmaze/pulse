"""Bounded incremental reader for the Pulse metrics JSONL.

The observatory must stay cheap when a world has lived for months.  A new
reader therefore starts at a fixed tail window instead of byte zero, and a
slow reader skips forward to that same bounded window rather than trying to
repay an unbounded backlog.  ``TailBatch.replay`` makes every such gap an
explicit protocol fact.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_REPLAY_BYTES = 1024 * 1024
_CHECKPOINT_BYTES = 64


@dataclass(frozen=True)
class TailBatch:
    """One bounded read and the facts needed to interpret its projection."""

    lines: list[str]
    start_offset: int
    end_offset: int
    file_size: int
    bytes_read: int
    replay_truncated: bool
    reset_reason: str | None
    cursor: str | None
    window_bytes: int

    @property
    def replay(self) -> dict[str, object]:
        return {
            "complete": not self.replay_truncated,
            "truncated": self.replay_truncated,
            "window_bytes": self.window_bytes,
            "bytes_read": self.bytes_read,
            "start_offset": self.start_offset,
            "end_offset": self.end_offset,
            "file_size": self.file_size,
            "cursor": self.cursor,
            "reset": self.reset_reason,
        }


class LineTailer:
    """Yield complete UTF-8 JSONL lines from a bounded recent window.

    ``cursor`` is an opaque value previously returned by this class (and used
    as the SSE ``Last-Event-ID``).  A matching cursor resumes at its safe line
    boundary.  A stale cursor, replacement, truncation, recorder rotation, or
    a producer outrunning the reader resets to the newest ``replay_bytes`` and
    is surfaced through ``reset_reason`` and ``replay_truncated``.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        replay_bytes: int = DEFAULT_REPLAY_BYTES,
        cursor: str | None = None,
    ):
        if replay_bytes <= 0:
            raise ValueError("replay_bytes must be positive")
        self._path = Path(path)
        self._replay_bytes = replay_bytes
        self._cursor_hint = self._parse_cursor(cursor)
        self._offset: int | None = None
        self._identity: tuple[int, int] | None = None
        self._partial = b""
        self._checkpoint = b""
        self._history_truncated = False
        self._last_batch = TailBatch(
            lines=[],
            start_offset=0,
            end_offset=0,
            file_size=0,
            bytes_read=0,
            replay_truncated=False,
            reset_reason="missing",
            cursor=None,
            window_bytes=replay_bytes,
        )

    @property
    def path(self) -> Path:
        return self._path

    @property
    def last_batch(self) -> TailBatch:
        return self._last_batch

    def read_available(self) -> list[str]:
        """Compatibility wrapper returning only complete lines."""
        return self.read_batch().lines

    def read_batch(self) -> TailBatch:
        """Read at most one bounded window, plus a tiny continuity check."""
        try:
            file = self._path.open("rb")
        except OSError:  # connecting before the run starts remains valid
            self._last_batch = self._empty_batch("missing" if self._offset is None else None)
            return self._last_batch

        bytes_read = 0
        with file:
            try:
                stat = os.fstat(file.fileno())
            except OSError:
                self._last_batch = self._empty_batch(None)
                return self._last_batch

            identity = (stat.st_dev, stat.st_ino)
            size = stat.st_size
            start = self._offset if self._offset is not None else 0
            reset: str | None = None
            sequential = False

            if self._offset is None:
                hint = self._cursor_hint
                if hint is not None and hint[:2] == identity and hint[2] <= size:
                    start = hint[2]
                    reset = "reconnect"
                else:
                    start = max(0, size - self._replay_bytes)
                    reset = "initial"
                    self._history_truncated = start > 0 or self._has_archives()
                self._partial = b""
            elif identity != self._identity:
                reset = "rotated" if self._was_rotated(self._identity) else "replaced"
                start = max(0, size - self._replay_bytes)
                self._partial = b""
                self._history_truncated = True
            else:
                checkpoint_changed = False
                if self._checkpoint and self._offset >= len(self._checkpoint):
                    file.seek(self._offset - len(self._checkpoint))
                    observed = file.read(len(self._checkpoint))
                    bytes_read += len(observed)
                    checkpoint_changed = observed != self._checkpoint
                if size < self._offset or checkpoint_changed:
                    reset = "truncated"
                    start = max(0, size - self._replay_bytes)
                    self._partial = b""
                    self._history_truncated = True
                elif size - self._offset > self._replay_bytes:
                    reset = "overrun"
                    start = max(0, size - self._replay_bytes)
                    self._partial = b""
                    self._history_truncated = True
                else:
                    start = self._offset
                    sequential = True

            file.seek(start)
            raw = file.read(min(self._replay_bytes, max(0, size - start)))
            bytes_read += len(raw)
            end = start + len(raw)

            # A tail window may begin inside a UTF-8 code point or JSON line.
            # Test the preceding byte; only discard through the next newline
            # when this is not already a line boundary.
            if not sequential and start > 0:
                file.seek(start - 1)
                previous = file.read(1)
                bytes_read += len(previous)
                if previous != b"\n":
                    newline = raw.find(b"\n")
                    raw = b"" if newline < 0 else raw[newline + 1 :]

            buffered = (self._partial + raw) if sequential else raw
            if buffered.endswith(b"\n"):
                complete = buffered[:-1].split(b"\n") if buffered else []
                self._partial = b""
            else:
                parts = buffered.split(b"\n")
                complete = parts[:-1]
                self._partial = parts[-1] if parts else b""

            lines: list[str] = []
            for line in complete:
                # JSONL written in Windows text mode is CRLF.  Splitting the
                # binary stream on LF must not leave CR inside an SSE data
                # frame, where it would turn the closing array bracket into a
                # second, unprefixed protocol line.
                line = line.removesuffix(b"\r")
                if not line.strip():
                    continue
                try:
                    lines.append(line.decode("utf-8"))
                except UnicodeDecodeError:
                    # A malformed sideband line must not poison the stream.
                    continue

            self._offset = end
            self._identity = identity
            self._checkpoint = self._read_checkpoint(file, end)
            bytes_read += len(self._checkpoint)
            safe_offset = end - len(self._partial)
            cursor = self._make_cursor(identity, safe_offset)
            self._last_batch = TailBatch(
                lines=lines,
                start_offset=start,
                end_offset=end,
                file_size=size,
                bytes_read=bytes_read,
                replay_truncated=self._history_truncated,
                reset_reason=reset,
                cursor=cursor,
                window_bytes=self._replay_bytes,
            )
            return self._last_batch

    def _empty_batch(self, reset: str | None) -> TailBatch:
        offset = self._offset or 0
        cursor = (
            self._make_cursor(self._identity, offset - len(self._partial))
            if self._identity is not None
            else None
        )
        return TailBatch(
            lines=[],
            start_offset=offset,
            end_offset=offset,
            file_size=0,
            bytes_read=0,
            replay_truncated=self._history_truncated,
            reset_reason=reset,
            cursor=cursor,
            window_bytes=self._replay_bytes,
        )

    def _has_archives(self) -> bool:
        return (
            self._path.with_name(f"{self._path.name}.1").exists()
            or self._path.with_name(f"{self._path.name}.legacy").exists()
        )

    def _was_rotated(self, previous: tuple[int, int] | None) -> bool:
        if previous is None:
            return False
        archive = self._path.with_name(f"{self._path.name}.1")
        try:
            stat = archive.stat()
        except OSError:
            return False
        return (stat.st_dev, stat.st_ino) == previous

    @staticmethod
    def _read_checkpoint(file, offset: int) -> bytes:
        length = min(_CHECKPOINT_BYTES, offset)
        if length == 0:
            return b""
        file.seek(offset - length)
        return file.read(length)

    @staticmethod
    def _make_cursor(identity: tuple[int, int], offset: int) -> str:
        return f"v1-{identity[0]:x}-{identity[1]:x}-{max(0, offset):x}"

    @staticmethod
    def _parse_cursor(cursor: str | None) -> tuple[int, int, int] | None:
        if not cursor:
            return None
        parts = cursor.split("-")
        if len(parts) != 4 or parts[0] != "v1":
            return None
        try:
            values = tuple(int(value, 16) for value in parts[1:])
        except ValueError:
            return None
        if values[2] < 0:
            return None
        return values

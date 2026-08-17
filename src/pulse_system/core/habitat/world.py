"""Pluggable Habitat base for typed reads, actions, and channel responses."""

from __future__ import annotations

from abc import ABC, abstractmethod

from .ledger import ChannelLedger
from .types import Action, ChannelSpec, Organ, Response


class Habitat(ABC):
    """A place, not a task environment."""

    def __init__(self, extra_channels: list[ChannelSpec] | None = None) -> None:
        # Adapters may add externally owned channels such as a bounded web read.
        self._extra = list(extra_channels or [])
        self._ledger = ChannelLedger(self.channels() + self._extra)

    # ── what the world is ────────────────────────────────────────

    @abstractmethod
    def channels(self) -> list[ChannelSpec]:
        """Every response channel exposed by this Habitat."""

    @abstractmethod
    def organs(self) -> list[Organ]:
        """Declared read surfaces."""

    # ── living in it ─────────────────────────────────────────────

    @abstractmethod
    def perceive(self, organ: str, target: str = "") -> str:
        """Read the world through one organ. Never returns ledger data."""

    @abstractmethod
    def _act(self, action: Action) -> list[Response]:
        """World-specific consequence of a typed action."""

    @abstractmethod
    def poll(self) -> list[Response]:
        """Return responses produced without a matching caller action."""

    # ── response accounting ──────────────────────────────────────

    def act(self, action: Action) -> list[Response]:
        """Run an action and record its attributed responses."""
        self._ledger.begin_step()
        responses = self._act(action)
        for r in responses:
            self._ledger.record(r)
        return responses

    def collect(self) -> list[Response]:
        """Drain unrequested responses and record one observation step."""
        self._ledger.begin_step()
        responses = self.poll()
        for r in responses:
            self._ledger.record(r)
        return responses

    @property
    def ledger(self) -> ChannelLedger:
        """Internal response accounting; it is not exposed through perceive()."""
        return self._ledger


class EmptyWorld(Habitat):
    """A Habitat with no channels, reads, actions, or spontaneous responses."""

    def channels(self) -> list[ChannelSpec]:
        return []

    def organs(self) -> list[Organ]:
        return []

    def perceive(self, organ: str, target: str = "") -> str:
        return ""

    def _act(self, action: Action) -> list[Response]:
        return []

    def poll(self) -> list[Response]:
        return []

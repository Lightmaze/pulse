"""SensoryCortex — channel↔engram bindings and the perception rhythm.

Binding an engram to a channel makes it a sensory-cortex engram: channel
items flow into the engine as external events targeted at that engram —
local processing first, with no front-agent routing. The binding
also sets a short dendrite wait so the engram runs at a high-frequency
rhythm (its "内在节律", not a cron).

Dynamic by design: want to "see" a new source → bind; done → unbind and
the engram reverts to an ordinary thinker. New modalities arrive as new
channels plus (via substrate registry) a substrate binding — no architectural change.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass

from .channels import Channel

_logger = logging.getLogger("pulse_system.sensory")


@dataclass
class SensoryBinding:
    engram_id: str
    channel: Channel
    priority: float


class SensoryCortex:
    """Manages channel bindings; the engine polls it each tick."""

    def __init__(self, dendrite, *, default_wait: float = 1.0):
        self._dendrite = dendrite
        self._default_wait = default_wait
        self._bindings: dict[str, SensoryBinding] = {}
        self._saved_waits: dict[str, float] = {}

    def bind(
        self,
        engram_id: str,
        channel: Channel,
        *,
        wait_time: float | None = None,
        priority: float = 0.9,
    ) -> None:
        """Attach a channel to an engram and switch it to sensory rhythm."""
        self._saved_waits[engram_id] = self._dendrite.get_wait_time(engram_id)
        self._dendrite.set_wait_time(
            engram_id, wait_time if wait_time is not None else self._default_wait
        )
        self._bindings[engram_id] = SensoryBinding(
            engram_id=engram_id, channel=channel, priority=priority,
        )
        _logger.info("sensory bind: %s <- %s", engram_id,
                     type(channel).__name__)

    def unbind(self, engram_id: str) -> None:
        if engram_id in self._bindings:
            del self._bindings[engram_id]
            saved = self._saved_waits.pop(engram_id, None)
            if saved is not None:
                self._dendrite.set_wait_time(engram_id, saved)
            _logger.info("sensory unbind: %s", engram_id)

    def bound_engrams(self) -> list[str]:
        return list(self._bindings.keys())

    def reassign_engram(self, old_id: str, new_id: str) -> None:
        """Succession: the successor keeps perceiving the channel."""
        binding = self._bindings.pop(old_id, None)
        if binding is None:
            return
        binding.engram_id = new_id
        self._bindings[new_id] = binding
        if old_id in self._saved_waits:
            self._saved_waits[new_id] = self._saved_waits.pop(old_id)

    def poll(self) -> list[tuple[str, str, float]]:
        """(engram_id, content, priority) triples for this tick."""
        out: list[tuple[str, str, float]] = []
        for binding in self._bindings.values():
            for item in binding.channel.poll():
                out.append((binding.engram_id, item, binding.priority))
        return out

    def poll_durable(self) -> list[tuple[str, str, float, str, Channel]]:
        """Poll with a source fingerprint for a durable Runtime boundary.

        File channels expose a two-phase ``poll_fingerprinted``/``acknowledge``
        contract.  Simpler legacy channels remain compatible and receive a
        content fingerprint for ledger idempotency, while restart durability
        belongs to a source that can persist its own cursor.  The Cortex does
        not consume a second private dedup state before the ledger commit.
        """

        out: list[tuple[str, str, float, str, Channel]] = []
        for binding in self._bindings.values():
            poll_fingerprinted = getattr(binding.channel, "poll_fingerprinted", None)
            if callable(poll_fingerprinted):
                items = poll_fingerprinted() or []
                for path, fingerprint, content, _first_time in items:
                    out.append((
                        binding.engram_id,
                        content,
                        binding.priority,
                        json.dumps([path, fingerprint], ensure_ascii=False),
                        binding.channel,
                    ))
                continue
            for content in binding.channel.poll():
                fingerprint = hashlib.sha256(content.encode("utf-8")).hexdigest()
                out.append((
                    binding.engram_id,
                    content,
                    binding.priority,
                    fingerprint,
                    binding.channel,
                ))
        return out

    @staticmethod
    def acknowledge(channel: Channel, fingerprint: str) -> None:
        """Acknowledge a source item after its causal event is committed."""

        acknowledge = getattr(channel, "acknowledge", None)
        if not callable(acknowledge):
            return
        try:
            path, digest = json.loads(fingerprint)
        except (TypeError, ValueError, json.JSONDecodeError):
            return
        acknowledge(path, digest)

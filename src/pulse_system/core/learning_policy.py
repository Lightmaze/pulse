"""Explicit control of inference-affecting online learner mutations.

The policy is immutable configuration.  The audit is deliberately separate
mutable process-local evidence: a disabled channel must still expose that a
legal learning signal arrived, while proving that no learner state was
applied.  Components enforce the policy at the mutation owner so alternate
callers cannot bypass it.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class OnlineLearningChannel(StrEnum):
    CONNECTION_STDP = "connection_stdp"
    CONNECTION_DECAY_PRUNE = "connection_decay_prune"
    DELEGATION_MLP = "delegation_mlp"
    CLAUSTRUM_MLP = "claustrum_mlp"


@dataclass(frozen=True, slots=True)
class OnlineLearningPolicy:
    """Allow or refuse online mutation without changing inference assembly."""

    connection_stdp: bool = True
    connection_decay_prune: bool = True
    delegation_mlp: bool = True
    claustrum_mlp: bool = True

    def __post_init__(self) -> None:
        for channel in OnlineLearningChannel:
            if type(getattr(self, channel.value)) is not bool:
                raise ValueError(
                    f"{channel.value} online-learning policy must be a bool"
                )

    @classmethod
    def disabled(cls) -> OnlineLearningPolicy:
        return cls(
            connection_stdp=False,
            connection_decay_prune=False,
            delegation_mlp=False,
            claustrum_mlp=False,
        )

    def allows(self, channel: OnlineLearningChannel | str) -> bool:
        try:
            typed = (
                channel
                if isinstance(channel, OnlineLearningChannel)
                else OnlineLearningChannel(channel)
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unknown online-learning channel {channel!r}") from exc
        return bool(getattr(self, typed.value))

    def as_dict(self) -> dict[str, bool]:
        return {
            channel.value: self.allows(channel)
            for channel in OnlineLearningChannel
        }


class OnlineLearningAudit:
    """Thread-safe, process-local counters for policy enforcement evidence."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counters: dict[OnlineLearningChannel, dict[str, int]] = {
            channel: {"attempts": 0, "applied": 0}
            for channel in OnlineLearningChannel
        }

    def record_attempt(
        self,
        channel: OnlineLearningChannel | str,
        count: int = 1,
    ) -> None:
        self._increment(channel, "attempts", count)

    def record_applied(
        self,
        channel: OnlineLearningChannel | str,
        count: int = 1,
        **details: int,
    ) -> None:
        self._increment(channel, "applied", count)
        for key, value in details.items():
            if not isinstance(key, str) or not key:
                raise ValueError("online-learning audit detail keys must be non-empty")
            self._increment(channel, key, value)

    def snapshot(self, policy: OnlineLearningPolicy) -> dict[str, Any]:
        if not isinstance(policy, OnlineLearningPolicy):
            raise TypeError("policy must be an OnlineLearningPolicy")
        with self._lock:
            return {
                "policy": policy.as_dict(),
                "channels": {
                    channel.value: dict(self._counters[channel])
                    for channel in OnlineLearningChannel
                },
            }

    def _increment(
        self,
        channel: OnlineLearningChannel | str,
        key: str,
        count: int,
    ) -> None:
        try:
            typed = (
                channel
                if isinstance(channel, OnlineLearningChannel)
                else OnlineLearningChannel(channel)
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unknown online-learning channel {channel!r}") from exc
        if type(count) is not int or count < 0:
            raise ValueError("online-learning audit count must be an integer >= 0")
        if count == 0:
            return
        with self._lock:
            counters = self._counters[typed]
            counters[key] = counters.get(key, 0) + count

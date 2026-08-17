"""Typed requests, responses, channels, and external-effect receipts.

These contracts keep workspace actions explicit and make the origin and
outcome of every response inspectable without exposing payloads in receipts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Reply(str, Enum):
    """How a Habitat channel answered an action."""

    REFUSE = "refuse"
    YIELD = "yield"


@dataclass(frozen=True)
class ChannelSpec:
    """A declared response channel and its attribution metadata."""

    name: str
    description: str
    authored_by_world: bool = True
    #: True when this channel may emit a response without a matching action.
    unbidden_capable: bool = False


@dataclass(frozen=True)
class HabitatEffectReceipt:
    """Payload-free evidence for one terminal external Habitat mutation."""

    journal_effect_id: str
    correlation_id: str | None
    kind: str
    path: str
    before_digest: str | None
    after_digest: str
    terminal_state: str

    def to_dict(self) -> dict[str, str | None]:
        return {
            "journal_effect_id": self.journal_effect_id,
            "correlation_id": self.correlation_id,
            "kind": self.kind,
            "path": self.path,
            "before_digest": self.before_digest,
            "after_digest": self.after_digest,
            "terminal_state": self.terminal_state,
        }


@dataclass(frozen=True)
class Response:
    """One channel response and whether it was requested."""

    channel: str
    reply: Reply
    detail: str = ""
    #: Arrived without a matching action from the caller.
    unbidden: bool = False
    effect_receipt: HabitatEffectReceipt | None = None

    @property
    def yielded(self) -> bool:
        return self.reply is Reply.YIELD


@dataclass(frozen=True)
class Action:
    """A typed action. Habitat implementations define supported verbs."""

    verb: str
    target: str = ""
    payload: str = ""
    correlation_id: str | None = None


@dataclass(frozen=True)
class Organ:
    """A declared read surface and the scales it can resolve."""

    name: str
    description: str
    #: Scales this organ can resolve, e.g. ("symbol", "file", "history").
    #: Named resolution levels supported by this read surface.
    scales: tuple[str, ...] = field(default_factory=tuple)

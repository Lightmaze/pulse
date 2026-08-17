"""Bounded accounting for attributed Habitat channel responses."""

from __future__ import annotations

from dataclasses import dataclass, field

from .types import ChannelSpec, Reply, Response

#: Default minimum number of independently observed channels.
MIN_CHANNELS = 3
#: Share above which one channel is reported as dominant.
DEGENERATE_SHARE = 0.6


@dataclass
class ChannelVerdict:
    k: int
    refusals: int
    yieldings: int
    unbidden: int
    per_channel: dict[str, int]
    dominant: tuple[str, float] | None
    aliases: list[tuple[str, str]]
    faults: list[str] = field(default_factory=list)

    @property
    def habitable(self) -> bool:
        return not self.faults


class ChannelLedger:
    """Accounting over channel responses, kept outside read surfaces."""

    def __init__(self, specs: list[ChannelSpec]):
        self._specs = {s.name: s for s in specs}
        self._log: list[Response] = []
        #: per-step channel firing, for the independence check
        self._steps: list[set[str]] = []
        self._open = False

    # ── recording ────────────────────────────────────────────────

    def begin_step(self) -> None:
        self._steps.append(set())
        self._open = True

    def record(self, response: Response) -> None:
        spec = self._specs.get(response.channel)
        if spec is None:
            raise KeyError(
                f"undeclared channel {response.channel!r}; a world must declare "
                f"its channels so K can be computed rather than asserted"
            )
        self._log.append(response)
        if not self._open:
            self.begin_step()
        self._steps[-1].add(response.channel)

    # ── aggregate verdict ────────────────────────────────────────

    def _world_authored(self) -> list[Response]:
        """Return responses attributed to external Habitat channels."""
        return [r for r in self._log
                if self._specs[r.channel].authored_by_world]

    def aliases(self) -> list[tuple[str, str]]:
        """Return channel pairs that have never been observed independently."""
        names = sorted({r.channel for r in self._world_authored()})
        out: list[tuple[str, str]] = []
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                sa = [a in s for s in self._steps if a in s or b in s]
                sb = [b in s for s in self._steps if a in s or b in s]
                if sa and sa == sb:          # co-fired on every step either fired
                    out.append((a, b))
        return out

    def verdict(self, *, min_channels: int = MIN_CHANNELS,
                degenerate_share: float = DEGENERATE_SHARE) -> ChannelVerdict:
        world = self._world_authored()
        per: dict[str, int] = {}
        for r in world:
            per[r.channel] = per.get(r.channel, 0) + 1

        alias = self.aliases()
        merged = set()
        for a, b in alias:
            merged.add(b)                     # collapse aliases into one
        k = len([c for c in per if c not in merged])

        refusals = sum(1 for r in world if r.reply is Reply.REFUSE)
        yieldings = sum(1 for r in world if r.reply is Reply.YIELD)
        unbidden = sum(1 for r in world if r.unbidden)

        dominant = None
        if world:
            top = max(per.items(), key=lambda kv: kv[1])
            share = top[1] / len(world)
            if share > degenerate_share:
                dominant = (top[0], share)

        faults: list[str] = []
        if k < min_channels:
            faults.append(
                f"K={k} < {min_channels}: too few independently observed channels"
            )
        if yieldings == 0 and world:
            faults.append(
                "no yieldings were observed"
            )
        if refusals == 0 and world:
            faults.append(
                "no refusals were observed"
            )
        if unbidden == 0 and world:
            faults.append(
                "no unrequested channel response was observed"
            )
        if dominant is not None:
            faults.append(
                f"channel {dominant[0]!r} holds {dominant[1]:.0%} of all "
                f"responses (> {degenerate_share:.0%})"
            )
        if not world:
            faults.append("the world never spoke at all")

        return ChannelVerdict(
            k=k, refusals=refusals, yieldings=yieldings, unbidden=unbidden,
            per_channel=per, dominant=dominant, aliases=alias, faults=faults,
        )

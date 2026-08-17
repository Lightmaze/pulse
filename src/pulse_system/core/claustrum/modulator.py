"""Claustrum MLP (core/claustrum/) — the spectrum stream.

Touches rhythm, never content: each tick it reads every live engram's
state features and emits three modulation factors per engram —

- activity modulation: multiplies the spontaneous-activation probability
  (composes with the v0.4 inhibition level)
- wait modulation: multiplies the dendrite max-wait time (cross-source
  temporal alignment)
- propagation modulation: multiplies the source engram's propagation
  threshold (spec frequency-stream control surface). A factor >1 raises
  the bar so fewer edges spread — the brake on propagation cascades that
  a spontaneous-only lever cannot apply.

Cold start is exactly neutral: the output head is zero-initialized, so
raw output 0 maps to factor 1.0 and an untrained claustrum is a no-op.

In band-reward mode, reward is +1 when the mind-tide ratio n/N sits inside
the configured target band and decays with distance outside it. Exploration
noise perturbs the emitted modulations; periodically the MLP regresses toward
the modulations that were active during
above-average-reward ticks, weighted by advantage (reward-weighted
behavioral cloning). All substrate comes from core/nn.
"""

from __future__ import annotations

from collections.abc import Iterable
import logging
import math
from dataclasses import dataclass

import numpy as np

from pulse_system.core.learning_policy import (
    OnlineLearningAudit,
    OnlineLearningChannel,
    OnlineLearningPolicy,
)
from pulse_system.core.nn import SlotIndex, TwoLayerMLP
from pulse_system.substrate.storage.store import Storage

_logger = logging.getLogger("pulse_system.claustrum")

_FEATURES_PER_ENGRAM = 5
# activity, wait, propagation-threshold, inhibition→propagation gate
_OUTPUTS_PER_ENGRAM = 4
# The focus-vs-breadth demand is replicated across this many input dims. As a
# single scalar among max_slots*5 inputs it is swamped by the first-layer
# initialization scale (~sqrt(2/N)). Replicating it gives the conditioning
# signal weight comparable to a slot's worth of features.
_DEMAND_DIMS = 16
_LN2 = math.log(2.0)


@dataclass
class ClaustrumConfig:
    max_slots: int = 256
    hidden_dim: int = 128
    lr: float = 0.02
    # Modulation factor range: factor = exp(mod_log_range * tanh(raw)),
    # i.e. [exp(-r), exp(r)]. Default ln2 → [0.5, 2.0]. The actuator must
    # have enough authority to actually reach the target band — if the
    # environment saturates harder than the range allows, learning has a
    # correct gradient but a blocked path.
    mod_log_range: float = _LN2
    # Target mind-tide band used by the band reward mode.
    target_low: float = 0.15
    target_high: float = 0.45
    # exploration noise on raw outputs (pre-squash); anneals with learning
    noise_hot: float = 0.4
    noise_cold: float = 0.05
    anneal_observations: int = 500
    # Reheat: keep a noise floor of share·noise_hot·(1 − in-band EMA) under
    # the anneal. A claustrum that annealed cold while still out of band
    # (e.g. resumed weights past a per-run anneal budget) would otherwise
    # stop exploring and clone its own drift. 0 disables reheating.
    noise_reheat_share: float = 0.0
    # Stability bonus: reward += bonus · in_band_frac · (1 − std/scale) per
    # hold, so dwelling inside the band outranks crossing through it.
    # 0 disables the bonus and keeps mean per-tick reward only.
    stability_bonus: float = 0.0
    stability_scale: float = 0.15
    # Potential-based reward shaping (drive reduction). Adds
    # weight · (D_head − D_tail) to the hold reward, where D is distance to
    # the band. On the saturation plateau the absolute reward is flat, so
    # advantage (r − batch mean) collapses and cloning stalls; the shaping
    # term rewards holds whose tide *trends toward* the band, restoring
    # advantage spread. Policy-invariant in the PBRS sense — the in-band
    # optimum is unchanged (D=0 inside). 0 = off (absolute reward only).
    reward_shaping_weight: float = 0.0
    # Consensus cloning: positive samples are pulled toward
    # raw + (1−share)·own_noise + share·⟨noise⟩_advantage instead of their
    # own noisy output. Exploration noise that disagrees across good
    # samples cancels in the advantage-weighted mean, so reheated (hot)
    # phases stop writing their own variance into the weights. 0 = off.
    clone_consensus_share: float = 0.0
    # When False, the propagation-threshold factor is pinned to 1.0. The MLP
    # retains and trains the output head; only actuation is disabled. True
    # enables the complete propagation control surface.
    modulate_propagation: bool = True
    # When False, the activity factor is pinned to 1.0 while the other factors
    # remain active. This keeps component-level validation deterministic.
    modulate_activity: bool = True
    # Fourth head: per-Engram inhibition-to-propagation gate factor. The
    # effective gate is the engine base gate × factor. False pins it to 1.0.
    modulate_gate: bool = False
    # Reward target:
    # "band"     — in-band mind-tide reward (uses a configured band).
    # "coherent" — maximize the engine's coherent-focus signal (no band).
    # "breadth"  — maximize the engine's breadth signal (how much of the
    #              purpose space is in play). The opposite pull to "coherent",
    #              and like it a stationary single objective. Distinct from
    #              balance+demand=0: this leaves the demand input untouched, so
    #              focus and breadth policies see the same input distribution
    #              and differ only in reward.
    # "balance"  — the tradeoff the measurements actually support: reward =
    #              demand·focus + (1−demand)·breadth, where breadth = how much
    #              of the purpose space is active (retrieval side, rises with
    #              the tide) and focus = one cluster assembled+dominant
    #              (coherence side, falls with the tide). The inspiration point
    #              is this tradeoff, not an intrinsic peak — so the claustrum
    #              must learn a DEMAND-CONDITIONAL policy, not one fixed target.
    #              The demand is fed in as an input feature (set_demand).
    reward_mode: str = "band"
    # learn every N reward observations from the recent buffer
    learn_interval: int = 20
    buffer_cap: int = 400
    # Credit assignment: one modulation sample is held for this many ticks
    # and attributed the *mean* reward over the hold — modulation effects
    # materialize on the spontaneous-check/activity-window timescale, not
    # on a single tick.
    hold_ticks: int = 8
    # Fraction of exploration noise that is common-mode across engrams.
    # The mind-tide is a global aggregate: independent per-slot noise
    # cancels in the mean and never explores the controllable direction.
    common_noise_share: float = 0.7
    # Ignore rewards during the first N ticks of a hold: the tide there
    # still reflects the *previous* modulation (activity-window carryover).
    credit_delay_ticks: int = 0


@dataclass
class EngramState:
    """Per-engram features the engine feeds each tick."""

    engram_id: str
    recent_activity: float      # 0..1
    connection_count: int
    queue_depth: int
    seconds_since_pulse: float  # large when never pulsed
    cluster_activity: float     # 0..1 (mean activity of its Project)


class ClaustrumModulator:
    """Spectrum modulation over live engrams."""

    COMPONENT = "claustrum_mlp"

    def __init__(
        self,
        storage: Storage,
        *,
        config: ClaustrumConfig | None = None,
        metrics=None,
        seed: int = 11,
        learning_policy: OnlineLearningPolicy | None = None,
        learning_audit: OnlineLearningAudit | None = None,
    ):
        self._config = config or ClaustrumConfig()
        self._storage = storage
        self._metrics = metrics
        self._learning_policy = learning_policy or OnlineLearningPolicy()
        self._learning_audit = learning_audit or OnlineLearningAudit()
        self._slots = SlotIndex(storage, self.COMPONENT, self._config.max_slots)
        # +1 input: the current focus-vs-breadth demand, so the learned policy
        # is demand-conditional (the tradeoff is what the data supports, not a
        # fixed target). Default 1.0 = pure focus.
        self._demand: float = 1.0
        self._mlp = TwoLayerMLP(
            self._config.max_slots * _FEATURES_PER_ENGRAM + _DEMAND_DIMS,
            self._config.hidden_dim,
            self._config.max_slots * _OUTPUTS_PER_ENGRAM,
            lr=self._config.lr,
            zero_output_init=True,   # cold start = neutral (factor 1.0)
        )
        self._rng = np.random.default_rng(seed)
        # Samples carry the output mask captured at inference time. Looking at
        # the current SlotIndex during learning would misattribute old rewards
        # after archival or succession.
        self._buffer: list[
            tuple[
                np.ndarray,
                np.ndarray,
                float,
                np.ndarray,
                float,
                np.ndarray,
            ]
        ] = []
        self._observations = 0
        self._pending: tuple[
            np.ndarray, np.ndarray, np.ndarray, np.ndarray
        ] | None = None
        # hold state: the current sample and its accumulated tide ratios
        self._hold_left = 0
        self._held_factors: dict[
            str, tuple[float, float, float, float]
        ] = {}
        self._ratio_acc: list[float] = []
        self._coherent_acc: list[float] = []
        self._breadth_acc: list[float] = []
        self._last_requested = 0
        self._last_overflow = 0
        # recent per-hold in-band fraction (drives the reheat noise floor)
        self._in_band_ema = 0.0
        self._load_state()

    def set_demand(self, demand: float) -> None:
        """Set the current focus-vs-breadth demand in [0,1].

        1.0 = pure focus (one purpose cluster coherent), 0.0 = pure breadth
        (as much of the purpose space in play as possible). It is both an input
        feature (so the policy is conditional on it) and the reward weight in
        reward_mode="balance". This is what "adjust the tradeoff on demand"
        means concretely: the caller (front-stage / current task) says what the
        cognition needs now; the claustrum learns how to deliver it.
        """
        self._demand = float(min(1.0, max(0.0, demand)))

    # ── Slot lifecycle ───────────────────────────────────────────

    def register_engram(self, engram_id: str) -> None:
        """Reserve a deterministic live slot without running modulation.

        Population builders need to establish the complete mask before the
        first pulse.  Treating a synthetic inference call as registration
        would also create a pending reward sample, so registration is kept as
        the same small lifecycle operation already exposed by the delegation
        router.
        """
        self._slots.slot_of(engram_id, create=True)

    def reassign_engram(self, old_id: str, new_id: str) -> None:
        self._slots.reassign(old_id, new_id)
        factors = self._held_factors.pop(old_id, None)
        if factors is not None:
            # Succession preserves the physical slot and its current credit.
            self._held_factors[new_id] = factors

    def mask_engram(self, engram_id: str) -> None:
        slot = self._slots.slot_of(engram_id, create=False)
        if slot is None:
            return

        stale_mask = self._output_mask_for_slots((slot,))
        self._buffer = [
            sample
            for sample in self._buffer
            if not np.any(sample[5] & stale_mask)
        ]
        if (
            self._pending is not None
            and np.any(self._pending[3] & stale_mask)
        ):
            # A held reward is joint credit for its whole active set. Once one
            # identity leaves, keeping the sample would alias its old reward to
            # whoever later reuses the physical slot.
            self._pending = None
            self._held_factors = {}
            self._hold_left = 0
            self._ratio_acc = []
            self._coherent_acc = []
            self._breadth_acc = []
            self._ticks_into_hold = 0

        self._reset_slot_to_factory(slot)
        # Persist the neutral field and release the durable mapping in one
        # SQLite transaction. SlotIndex.release() then only reconciles its
        # in-memory maps plus an idempotent DELETE.
        self._storage.save_weight_state_and_release_slot(
            self.COMPONENT,
            "field",
            self._field_state(),
            engram_id,
        )
        self._slots.release(engram_id)

    def _output_mask_for_slots(self, slots: Iterable[int]) -> np.ndarray:
        mask = np.zeros(self._mlp.output_dim, dtype=bool)
        for slot in slots:
            base = slot * _OUTPUTS_PER_ENGRAM
            mask[base:base + _OUTPUTS_PER_ENGRAM] = True
        return mask

    def _reset_slot_to_factory(self, slot: int) -> None:
        input_base = slot * _FEATURES_PER_ENGRAM
        input_end = input_base + _FEATURES_PER_ENGRAM
        output_base = slot * _OUTPUTS_PER_ENGRAM
        output_end = output_base + _OUTPUTS_PER_ENGRAM
        self._mlp.w1[input_base:input_end] = self._factory_w1[
            input_base:input_end
        ]
        self._mlp.w2[:, output_base:output_end] = self._factory_w2[
            :, output_base:output_end
        ]
        self._mlp.b2[output_base:output_end] = self._factory_b2[
            output_base:output_end
        ]

    def capacity_snapshot(self) -> dict[str, int | float]:
        """Content-free slot capacity facts for the Runtime read model."""
        limit = self._config.max_slots
        used = len(self._slots.live_ids())
        available = max(0, limit - used)
        utilization = min(1.0, used / limit) if limit > 0 else 0.0
        return {
            "claustrum_slot_limit": limit,
            "claustrum_slot_used": used,
            "claustrum_slot_available": available,
            "claustrum_slot_utilization": utilization,
            "claustrum_last_requested": self._last_requested,
            "claustrum_last_overflow": self._last_overflow,
        }

    # ── Inference ────────────────────────────────────────────────

    def modulate(
        self, states: list[EngramState]
    ) -> dict[str, tuple[float, float, float, float]]:
        """Return {engram_id: (activity, wait, prop_threshold, gate) factors}.

        Factors live in [exp(-r), exp(r)] via exp(r · tanh(raw)); raw 0 →
        1.0. One noisy sample is held for hold_ticks and attributed the
        mean reward over the hold (see ClaustrumConfig).
        """
        requested_ids = {state.engram_id for state in states}
        self._last_requested = len(requested_ids)

        # Serve the held sample only while the engram set is unchanged —
        # membership changes (new engram, succession) force a fresh sample.
        if (
            self._hold_left > 0
            and self._held_factors
            and requested_ids == set(self._held_factors)
        ):
            self._last_overflow = 0
            self._hold_left -= 1
            return self._held_factors

        x = np.zeros(self._config.max_slots * _FEATURES_PER_ENGRAM + _DEMAND_DIMS)
        x[-_DEMAND_DIMS:] = self._demand   # demand-conditional policy (replicated)
        slots_used: dict[str, int] = {}
        overflow_ids: set[str] = set()
        for s in states:
            slot = self._slots.slot_of(s.engram_id, create=True)
            if slot is None:
                overflow_ids.add(s.engram_id)
                continue
            base = slot * _FEATURES_PER_ENGRAM
            x[base + 0] = s.recent_activity
            x[base + 1] = min(1.0, s.connection_count / 10.0)
            x[base + 2] = min(1.0, s.queue_depth / 5.0)
            x[base + 3] = math.exp(-s.seconds_since_pulse / 300.0)
            x[base + 4] = s.cluster_activity
            slots_used[s.engram_id] = slot

        self._last_overflow = len(overflow_ids)
        if not slots_used:
            # No actuator existed for this sample. Do not turn an empty world
            # or a fully unassigned request into a reward-bearing experience.
            self._pending = None
            self._held_factors = {}
            self._hold_left = 0
            self._ratio_acc = []
            self._coherent_acc = []
            self._breadth_acc = []
            self._ticks_into_hold = 0
            return {}

        active_output_mask = self._output_mask_for_slots(slots_used.values())

        raw = self._mlp.forward(x)
        noise = self._noise()
        share = self._config.common_noise_share
        individual = self._rng.normal(
            0.0,
            noise * (1.0 - share),
            int(np.count_nonzero(active_output_mask)),
        )
        common = self._rng.normal(0.0, noise * share)
        noisy = raw.copy()
        noisy[active_output_mask] += individual + common
        self._pending = (x, noisy, raw, active_output_mask.copy())
        self._ratio_acc = []
        self._coherent_acc = []
        self._breadth_acc = []
        self._hold_left = max(0, self._config.hold_ticks - 1)

        out: dict[str, tuple[float, float, float, float]] = {}
        r = self._config.mod_log_range
        for eid, slot in slots_used.items():
            base = slot * _OUTPUTS_PER_ENGRAM
            act = (
                math.exp(r * math.tanh(noisy[base]))
                if self._config.modulate_activity else 1.0
            )
            wait = math.exp(r * math.tanh(noisy[base + 1]))
            prop = (
                math.exp(r * math.tanh(noisy[base + 2]))
                if self._config.modulate_propagation else 1.0
            )
            gate = (
                math.exp(r * math.tanh(noisy[base + 3]))
                if self._config.modulate_gate else 1.0
            )
            out[eid] = (act, wait, prop, gate)
        self._held_factors = out
        return out

    # ── Learning ─────────────────────────────────────────────────

    def learning_audit_snapshot(self) -> dict:
        return self._learning_audit.snapshot(self._learning_policy)

    def observe_mind_tide(self, ratio: float, coherent: float | None = None,
                          breadth: float | None = None) -> None:
        """Accumulate this tick's mind-tide toward the held sample; when the
        hold expires, the sample gets the mean reward over its whole hold,
        plus a stability bonus when it dwelled inside the band (see
        ClaustrumConfig.stability_bonus)."""
        if self._pending is None:
            return
        self._ticks_into_hold = getattr(self, "_ticks_into_hold", 0) + 1
        if self._ticks_into_hold > self._config.credit_delay_ticks:
            self._ratio_acc.append(ratio)
            if coherent is not None:
                self._coherent_acc.append(coherent)
            if breadth is not None:
                self._breadth_acc.append(breadth)
        if self._hold_left > 0:
            return
        x, applied, raw, active_output_mask = self._pending
        self._pending = None
        self._ticks_into_hold = 0
        if not self._ratio_acc:
            return
        channel = OnlineLearningChannel.CLAUSTRUM_MLP
        if not self._has_hold_reward_signal():
            self._ratio_acc = []
            self._coherent_acc = []
            self._breadth_acc = []
            return
        self._learning_audit.record_attempt(channel)
        if not self._learning_policy.allows(channel):
            self._ratio_acc = []
            self._coherent_acc = []
            self._breadth_acc = []
            return
        reward = self._hold_target()
        self._ratio_acc = []
        self._coherent_acc = []
        self._breadth_acc = []
        if reward is None:
            # The configured objective's signal never arrived during this hold.
            # Drop the sample rather than score it on a DIFFERENT objective:
            # falling back to the band reward here would train the policy on the
            # wrong target with no error anywhere.
            return
        self._buffer.append((
            x,
            applied,
            reward,
            raw,
            self._demand,
            active_output_mask.copy(),
        ))
        if len(self._buffer) > self._config.buffer_cap:
            self._buffer.pop(0)
        self._observations += 1
        if self._observations % self._config.learn_interval == 0:
            self._learn()

    def _has_hold_reward_signal(self) -> bool:
        mode = self._config.reward_mode
        if mode == "band":
            return bool(self._ratio_acc)
        if mode == "coherent":
            return bool(self._coherent_acc)
        if mode == "breadth":
            return bool(self._breadth_acc)
        if mode == "balance":
            return bool(self._coherent_acc and self._breadth_acc)
        raise ValueError(f"unknown reward_mode {mode!r}")

    def _hold_target(self) -> float | None:
        """This hold's reward under the configured objective, or None when the
        objective's signal is missing (caller drops the sample).

        Every non-band mode returns None rather than falling back, so a mode can
        never be silently trained on someone else's target.
        """
        mode = self._config.reward_mode
        if mode == "band":
            return self._hold_reward(self._ratio_acc)
        if mode == "coherent":
            # Maximize coherent focus directly — no hardcoded target band.
            reward = (sum(self._coherent_acc) / len(self._coherent_acc)
                      if self._coherent_acc else None)
        elif mode == "breadth":
            # The opposite stationary objective to "coherent": keep as much of
            # the purpose space in play as possible. Demand is untouched, so a
            # breadth-mode policy sees the same inputs as a focus-mode one.
            reward = (sum(self._breadth_acc) / len(self._breadth_acc)
                      if self._breadth_acc else None)
        elif mode == "balance":
            # The tradeoff the measurements support: focus and breadth are two
            # opposite monotones in the tide; the demand picks the point.
            if self._coherent_acc and self._breadth_acc:
                f = sum(self._coherent_acc) / len(self._coherent_acc)
                b = sum(self._breadth_acc) / len(self._breadth_acc)
                reward = self._demand * f + (1.0 - self._demand) * b
            else:
                reward = None
        else:
            raise ValueError(f"unknown reward_mode {mode!r}")
        if reward is None:
            _logger.warning(
                "claustrum reward_mode=%r got no signal this hold; dropping "
                "the sample (not falling back to another objective)", mode)
            return None
        self._in_band_ema = 0.9 * self._in_band_ema + 0.1 * reward
        return reward

    def _reward(self, ratio: float) -> float:
        c = self._config
        if c.target_low <= ratio <= c.target_high:
            return 1.0
        dist = (
            c.target_low - ratio if ratio < c.target_low
            else ratio - c.target_high
        )
        return max(-1.0, 1.0 - 4.0 * dist)

    def _distance_to_band(self, ratio: float) -> float:
        """Homeostatic drive: 0 inside the band, else distance to the
        nearest edge (a dead-zone potential around the target range)."""
        c = self._config
        if ratio < c.target_low:
            return c.target_low - ratio
        if ratio > c.target_high:
            return ratio - c.target_high
        return 0.0

    def _hold_reward(self, ratios: list[float]) -> float:
        """Reward for a whole hold: mean per-tick reward, plus the
        stability bonus for low-variance in-band dwelling, plus optional
        potential-based drive-reduction shaping. Also feeds the in-band EMA
        behind the reheat noise floor."""
        c = self._config
        reward = sum(self._reward(r) for r in ratios) / len(ratios)
        in_frac = sum(
            1 for r in ratios if c.target_low <= r <= c.target_high
        ) / len(ratios)
        self._in_band_ema = 0.9 * self._in_band_ema + 0.1 * in_frac
        if c.stability_bonus > 0.0 and in_frac > 0.0:
            spread = float(np.std(ratios))
            stability = max(0.0, 1.0 - spread / max(c.stability_scale, 1e-9))
            reward += c.stability_bonus * in_frac * stability
        if c.reward_shaping_weight > 0.0 and len(ratios) >= 2:
            # Trend of distance-to-band, head-half vs tail-half (means, not
            # single endpoints, to survive per-tick noise). Positive when the
            # tide moved toward the band under this held sample.
            mid = len(ratios) // 2
            d_head = np.mean([self._distance_to_band(r) for r in ratios[:mid]])
            d_tail = np.mean([self._distance_to_band(r) for r in ratios[mid:]])
            reward += c.reward_shaping_weight * float(d_head - d_tail)
        return reward

    def _learn(self) -> None:
        if len(self._buffer) < 4:
            return
        # Per-demand advantage baseline. The demand regimes have different
        # reward SCALES (focus ~0.3-0.4 vs breadth ~0.7-0.9), so a single batch
        # mean makes every breadth-demand sample look "above average" and every
        # focus-demand one "below" — the advantage would encode which demand was
        # active, not whether the action was good FOR that demand, and cloning
        # collapses to one unconditional policy. Conditioning the baseline on
        # the demand fixes that.
        groups: dict[float, list[float]] = {}
        for _, _, r, _, d, _ in self._buffer:
            groups.setdefault(round(d, 3), []).append(r)
        means = {k: sum(v) / len(v) for k, v in groups.items()}
        mean_r = float(np.mean([r for _, _, r, _, _, _ in self._buffer]))
        # clone only behavior above the baseline OF ITS OWN demand regime
        positives = [
            (x, applied, raw, mask, r - means[round(d, 3)])
            for x, applied, r, raw, d, mask in self._buffer
            if r - means[round(d, 3)] > 0
        ]
        share = self._config.clone_consensus_share
        delta = None
        if share > 0.0 and positives:
            delta_sum = np.zeros(self._mlp.output_dim)
            delta_weight = np.zeros(self._mlp.output_dim)
            for _, applied, raw, mask, adv in positives:
                delta_sum += adv * (applied - raw) * mask
                delta_weight += adv * mask
            delta = np.zeros(self._mlp.output_dim)
            np.divide(
                delta_sum,
                delta_weight,
                out=delta,
                where=delta_weight > 0.0,
            )
        updates = 0
        for x, applied, raw, mask, adv in positives:
            if delta is None:
                target = applied
            else:
                target = (
                    raw + (1.0 - share) * (applied - raw) + share * delta
                )
            self._mlp.update_regression(
                x,
                target,
                weight=adv,
                output_mask=mask,
            )
            updates += 1
        self._learning_audit.record_applied(
            OnlineLearningChannel.CLAUSTRUM_MLP,
            updates,
        )
        self._save_state()
        if self._metrics is not None:
            self._metrics.record(
                "claustrum_learn",
                mean_reward=round(mean_r, 4),
                cloned=updates,
                noise=round(self._noise(), 4),
            )

    def _noise(self) -> float:
        c = self._config
        frac = min(1.0, self._observations / max(1, c.anneal_observations))
        annealed = c.noise_hot + frac * (c.noise_cold - c.noise_hot)
        # explore while lost, exploit while home — the anneal alone can't
        # tell the difference (it only counts time)
        floor = c.noise_reheat_share * c.noise_hot * (1.0 - self._in_band_ema)
        return max(annealed, floor)

    # ── Persistence ──────────────────────────────────────────────

    def _save_state(self) -> None:
        """Persist only field learning; the neutral factory layer is stable."""
        self._storage.save_weight_state(
            self.COMPONENT,
            "field",
            self._field_state(),
        )

    def _field_state(self) -> dict:
        return {
            "mlp": self._mlp.state_dict(),
            "observations": self._observations,
            "in_band_ema": self._in_band_ema,
        }

    def _load_state(self) -> None:
        factory = self._storage.load_weight_state(self.COMPONENT, "factory")
        if factory is None:
            factory = {
                "mlp": self._mlp.state_dict(),
                "observations": 0,
                "in_band_ema": 0.0,
            }
            self._storage.save_weight_state(
                self.COMPONENT, "factory", factory
            )
        elif self._state_matches(factory):
            self._mlp.load_state_dict(factory["mlp"])

        # Slot-local factory rows/columns are the neutral reuse boundary. Keep
        # compact numeric copies before the field layer is loaded; resetting a
        # released slot must not reconstruct initialization from assumptions.
        self._factory_w1 = self._mlp.w1.copy()
        self._factory_w2 = self._mlp.w2.copy()
        self._factory_b2 = self._mlp.b2.copy()

        state = self._storage.load_weight_state(self.COMPONENT, "field")
        if state is None:
            state = self._storage.load_component_state(self.COMPONENT)
            if state is not None:
                self._storage.save_weight_state(
                    self.COMPONENT, "field", state
                )
                self._storage.delete_component_state(self.COMPONENT)
        if not state:
            return
        if not self._state_matches(state):
            _logger.warning(
                "saved claustrum MLP shape does not match config; "
                "using the factory layer"
            )
            return
        self._mlp.load_state_dict(state["mlp"])
        self._observations = int(state.get("observations", 0))
        self._in_band_ema = float(state.get("in_band_ema", 0.0))

    def _state_matches(self, state: dict) -> bool:
        mlp_state = state.get("mlp") or {}
        return (
            mlp_state.get("input_dim") == self._mlp.input_dim
            and mlp_state.get("hidden_dim") == self._mlp.hidden_dim
            and mlp_state.get("output_dim") == self._mlp.output_dim
        )

"""Tests for the claustrum MLP (spectrum modulation)."""

import numpy as np
import pytest

from pulse_system.core.claustrum import (
    ClaustrumConfig,
    ClaustrumModulator,
    EngramState,
)
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.dendrite import DendriteConfig, DendriteProcessor
from pulse_system.core.engram import EngramManager
from pulse_system.core.pulse import PulseEngine, PulseEngineConfig
from pulse_system.core.runtime import RuntimeConfig, RuntimeManager
from pulse_system.core.types import Message, MessageRole
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


@pytest.fixture
def store():
    s = Storage(":memory:")
    yield s
    s.close()


def _state(eid, activity=0.5, conns=3, queue=1, since=10.0, cluster=0.4):
    return EngramState(
        engram_id=eid, recent_activity=activity, connection_count=conns,
        queue_depth=queue, seconds_since_pulse=since, cluster_activity=cluster,
    )


class TestModulate:
    def test_cold_start_is_neutral_without_noise(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=8, noise_hot=0.0, noise_cold=0.0,
        ))
        out = mod.modulate([_state("a"), _state("b")])
        for act, wait, prop, gate in out.values():
            assert act == pytest.approx(1.0)
            assert wait == pytest.approx(1.0)
            assert prop == pytest.approx(1.0)
            assert gate == pytest.approx(1.0)

    def test_factors_bounded(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=8, noise_hot=5.0,  # extreme noise
        ))
        for _ in range(20):
            out = mod.modulate([_state("a")])
            act, wait, prop, gate = out["a"]
            assert 0.5 <= act <= 2.0
            assert 0.5 <= wait <= 2.0
            assert 0.5 <= prop <= 2.0
            assert 0.5 <= gate <= 2.0

    def test_slot_reassign_keeps_modulation(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=8, noise_hot=0.0, noise_cold=0.0,
        ))
        mod.modulate([_state("old")])
        mod.reassign_engram("old", "new")
        out = mod.modulate([_state("new")])
        assert "new" in out


class TestActiveSlotMasking:
    def test_exploration_noise_only_touches_current_active_outputs(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=3,
            noise_hot=1.0,
            noise_cold=1.0,
            hold_ticks=1,
        ), seed=19)

        mod.modulate([_state("a")])
        _, applied, raw, mask = mod._pending

        assert mask.dtype == bool
        assert int(mask.sum()) == 4
        assert not np.array_equal(applied[mask], raw[mask])
        assert np.array_equal(applied[~mask], raw[~mask])

    def test_empty_state_set_drops_pending_sample(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=2,
            hold_ticks=4,
        ))
        mod.modulate([_state("a")])
        assert mod._pending is not None

        assert mod.modulate([]) == {}
        assert mod._pending is None
        assert mod._held_factors == {}
        assert mod._hold_left == 0
        mod.observe_mind_tide(0.3)
        assert mod._buffer == []

    def test_full_slot_board_reports_unassigned_without_identity(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(max_slots=1))

        out = mod.modulate([
            _state("engram-alpha"),
            _state("engram-beta"),
        ])
        snapshot = mod.capacity_snapshot()

        assert set(out) == {"engram-alpha"}
        assert snapshot == {
            "claustrum_slot_limit": 1,
            "claustrum_slot_used": 1,
            "claustrum_slot_available": 0,
            "claustrum_slot_utilization": 1.0,
            "claustrum_last_requested": 2,
            "claustrum_last_overflow": 1,
        }
        assert "engram-alpha" not in str(snapshot)
        assert "engram-beta" not in str(snapshot)

    def test_learning_one_slot_keeps_unused_slot_strictly_neutral(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=2,
            noise_hot=0.0,
            noise_cold=0.0,
            hold_ticks=1,
            learn_interval=10_000,
        ))
        mod.modulate([_state("a")])
        x, _, raw, mask = mod._pending
        applied = raw.copy()
        applied[mask] = 1.0
        filler = raw.copy()
        mod._buffer = [
            (x, applied, 1.0, raw, 1.0, mask.copy()),
            (x, applied, 1.0, raw, 1.0, mask.copy()),
            (x, filler, -1.0, raw, 1.0, mask.copy()),
            (x, filler, -1.0, raw, 1.0, mask.copy()),
        ]

        mod._learn()

        assert np.any(mod._mlp.b2[:4] != 0.0)
        assert np.array_equal(mod._mlp.w2[:, 4:], np.zeros_like(mod._mlp.w2[:, 4:]))
        assert np.array_equal(mod._mlp.b2[4:], np.zeros(4))

        out = mod.modulate([_state("a"), _state("b")])
        assert out["b"] == pytest.approx((1.0, 1.0, 1.0, 1.0))

    def test_released_slot_drops_old_credit_and_resets_before_reuse(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=1,
            noise_hot=0.0,
            noise_cold=0.0,
            hold_ticks=2,
            learn_interval=10_000,
        ))
        mod.modulate([_state("old")])
        x, _, raw, mask = mod._pending
        applied = raw.copy()
        applied[mask] = 1.0
        mod._buffer = [
            (x, applied, 1.0, raw, 1.0, mask.copy()),
            (x, applied, 1.0, raw, 1.0, mask.copy()),
            (x, raw, -1.0, raw, 1.0, mask.copy()),
            (x, raw, -1.0, raw, 1.0, mask.copy()),
        ]
        mod._learn()
        assert np.any(mod._mlp.b2 != 0.0)

        mod.mask_engram("old")

        assert mod._pending is None
        assert mod._buffer == []
        assert mod._slots.slot_of("old", create=False) is None
        assert np.array_equal(mod._mlp.w1[:5], mod._factory_w1[:5])
        assert np.array_equal(mod._mlp.w2, mod._factory_w2)
        assert np.array_equal(mod._mlp.b2, mod._factory_b2)
        out = mod.modulate([_state("new")])
        assert out["new"] == pytest.approx((1.0, 1.0, 1.0, 1.0))

    def test_succession_keeps_held_slot_credit(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=1,
            hold_ticks=3,
        ))
        old_factors = mod.modulate([_state("old")])["old"]

        mod.reassign_engram("old", "new")

        assert mod._pending is not None
        assert mod.modulate([_state("new")])["new"] == old_factors
        assert mod._slots.slot_of("new", create=False) == 0

    def test_consensus_uses_a_per_output_mask_denominator(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=2,
            clone_consensus_share=1.0,
        ))
        x = np.ones(mod._mlp.input_dim)
        raw = np.zeros(mod._mlp.output_dim)
        first_mask = np.array([True] * 4 + [False] * 4)
        second_mask = ~first_mask
        first = raw.copy()
        first[0] = 1.0
        second = raw.copy()
        second[4] = 3.0
        mod._buffer = [
            (x, first, 1.0, raw, 1.0, first_mask),
            (x, second, 1.0, raw, 1.0, second_mask),
            (x, raw, -1.0, raw, 1.0, first_mask),
            (x, raw, -1.0, raw, 1.0, second_mask),
        ]
        captured = []

        def capture(_x, target, weight=1.0, *, output_mask=None):
            captured.append((target.copy(), output_mask.copy(), weight))
            return 0.0

        mod._mlp.update_regression = capture
        mod._learn()

        assert len(captured) == 2
        assert captured[0][0][0] == pytest.approx(1.0)
        assert captured[1][0][4] == pytest.approx(3.0)
        assert np.array_equal(captured[0][1], first_mask)
        assert np.array_equal(captured[1][1], second_mask)

    def test_capacity_snapshot_is_read_only(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(max_slots=4))
        mod.modulate([_state("a")])
        before = (
            mod._observations,
            len(mod._buffer),
            mod._pending,
            list(mod._slots.live_ids()),
        )

        first = mod.capacity_snapshot()
        second = mod.capacity_snapshot()

        assert first == second
        assert before == (
            mod._observations,
            len(mod._buffer),
            mod._pending,
            list(mod._slots.live_ids()),
        )


class TestPropagationModulation:
    """The claustrum modulates propagation threshold with neutral zero-init."""

    def test_modulate_returns_four_factors(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=8, noise_hot=0.0, noise_cold=0.0,
        ))
        out = mod.modulate([_state("a")])
        assert len(out["a"]) == 4

    def test_cold_start_all_four_neutral(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=8, noise_hot=0.0, noise_cold=0.0,
        ))
        out = mod.modulate([_state("a"), _state("b")])
        for act, wait, prop, gate in out.values():
            assert act == pytest.approx(1.0)
            assert wait == pytest.approx(1.0)
            assert prop == pytest.approx(1.0)

    def test_propagation_factor_bounded(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=8, noise_hot=5.0,
        ))
        for _ in range(20):
            prop = mod.modulate([_state("a")])["a"][2]
            assert 0.5 <= prop <= 2.0

    def test_disabled_propagation_stays_neutral(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=8, noise_hot=5.0, modulate_propagation=False,
        ))
        acts = []
        for _ in range(20):
            act, wait, prop, gate = mod.modulate([_state("a")])["a"]
            assert prop == pytest.approx(1.0)   # pinned despite heavy noise
            acts.append(act)
        # the other levers stay live (heavy noise → act varies off 1.0)
        assert max(abs(a - 1.0) for a in acts) > 0.1

    def test_disabled_activity_stays_neutral(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=8, noise_hot=5.0, modulate_activity=False,
        ))
        props = []
        for _ in range(20):
            act, wait, prop, gate = mod.modulate([_state("a")])["a"]
            assert act == pytest.approx(1.0)   # pinned despite heavy noise
            props.append(prop)
        assert max(abs(p - 1.0) for p in props) > 0.1   # prop still live

    def test_reassign_keeps_four_factors(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=8, noise_hot=0.0, noise_cold=0.0,
        ))
        mod.modulate([_state("old")])
        mod.reassign_engram("old", "new")
        out = mod.modulate([_state("new")])
        assert len(out["new"]) == 4


class TestGateHeadAndCoherentReward:
    """A fourth head modulates the inhibition gate; coherent reward needs no band."""

    def test_gate_head_pinned_unless_enabled(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=8, noise_hot=5.0))  # modulate_gate defaults False
        for _ in range(20):
            assert mod.modulate([_state("a")])["a"][3] == pytest.approx(1.0)

    def test_gate_head_live_when_enabled(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=8, noise_hot=5.0, modulate_gate=True))
        gates = [mod.modulate([_state("a")])["a"][3] for _ in range(20)]
        assert max(abs(g - 1.0) for g in gates) > 0.1
        assert all(0.5 <= g <= 2.0 for g in gates)

    def test_coherent_reward_uses_coherent_not_band(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=4, hold_ticks=2, learn_interval=10_000,
            noise_hot=0.0, noise_cold=0.0, reward_mode="coherent"))
        states = [_state("a")]
        # tide ratio is far out of band (would score badly under "band"),
        # but coherent focus is high → reward should be the coherent value
        for _ in range(2):
            mod.modulate(states)
            mod.observe_mind_tide(0.95, coherent=0.9)
        assert mod._buffer[0][2] == pytest.approx(0.9)

    def test_band_mode_ignores_coherent(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=4, hold_ticks=2, learn_interval=10_000,
            noise_hot=0.0, noise_cold=0.0,
            target_low=0.15, target_high=0.45))  # reward_mode defaults "band"
        states = [_state("a")]
        for _ in range(2):
            mod.modulate(states)
            mod.observe_mind_tide(0.30, coherent=0.0)
        assert mod._buffer[0][2] == pytest.approx(1.0)  # in-band reward


class TestBreadthReward:
    """The stationary breadth objective complements coherent-focus reward."""

    def _hold(self, mod, **kw):
        for _ in range(2):
            mod.modulate([_state("a")])
            mod.observe_mind_tide(0.5, **kw)

    def test_breadth_reward_uses_breadth_not_band_or_coherent(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=4, hold_ticks=2, learn_interval=10_000,
            noise_hot=0.0, noise_cold=0.0, reward_mode="breadth"))
        # tide in band (band reward 1.0) and coherent high (0.9) — the reward
        # must be neither, it must be the breadth value
        self._hold(mod, coherent=0.9, breadth=0.4)
        assert mod._buffer[0][2] == pytest.approx(0.4)

    def test_breadth_mode_leaves_demand_untouched(self, store):
        """Why this mode exists rather than balance+set_demand(0): the demand is
        also an INPUT feature, so using it to select the objective would change
        the input distribution between phases and let the net partition the
        tasks instead of overwriting — which would understate forgetting."""
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=4, hold_ticks=2, learn_interval=10_000,
            noise_hot=0.0, noise_cold=0.0, reward_mode="breadth"))
        self._hold(mod, coherent=0.9, breadth=0.4)
        assert mod._demand == pytest.approx(1.0)          # never moved
        assert list(mod._buffer[0][0][-16:]) == pytest.approx([1.0] * 16)

    def test_breadth_mode_without_breadth_signal_does_not_silently_fall_back(
            self, store):
        """If the engine ever stops feeding breadth, this must not quietly train
        on the band reward instead — that would be training on the wrong
        objective with no error."""
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=4, hold_ticks=2, learn_interval=10_000,
            noise_hot=0.0, noise_cold=0.0, reward_mode="breadth",
            target_low=0.15, target_high=0.45))
        self._hold(mod, coherent=0.9)                      # breadth=None
        assert mod._buffer == []                           # dropped, not banded


class TestPerDemandBaseline:
    """The advantage baseline must be conditioned on the demand: the regimes
    have different reward scales, so one shared mean would encode "which demand
    was active" instead of "was this action good for that demand"."""

    def test_baseline_is_per_demand(self, store):
        import numpy as np
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=1, hold_ticks=1, learn_interval=10_000,
            reward_mode="balance"))
        x = np.zeros(mod._mlp.input_dim)
        raw = np.zeros(4)
        # breadth regime scores high (0.8/0.9), focus regime low (0.2/0.3).
        # With a shared mean, BOTH breadth rows would be positive and both
        # focus rows negative. With a per-demand baseline, exactly the better
        # row of each regime is positive.
        mod._buffer = [
            (x, np.array([1.0, 0, 0, 0]), 0.9, raw, 0.0,
             np.ones(4, dtype=bool)),                        # breadth, better
            (x, np.array([2.0, 0, 0, 0]), 0.8, raw, 0.0,
             np.ones(4, dtype=bool)),                        # breadth, worse
            (x, np.array([3.0, 0, 0, 0]), 0.3, raw, 1.0,
             np.ones(4, dtype=bool)),                        # focus, better
            (x, np.array([4.0, 0, 0, 0]), 0.2, raw, 1.0,
             np.ones(4, dtype=bool)),                        # focus, worse
        ]
        cloned = []
        mod._mlp.update_regression = (
            lambda x, t, weight=1.0, output_mask=None:
            cloned.append(float(np.asarray(t)[0])) or 0.0
        )
        mod._learn()
        # one winner per regime — the focus-regime action is NOT excluded just
        # for living on a smaller scale
        assert sorted(cloned) == [1.0, 3.0]


class TestBalanceRewardAndDemand:
    """The tradeoff the measurements support: reward = demand·focus +
    (1−demand)·breadth, with the demand fed in as a feature so the learned
    policy is demand-conditional."""

    def _mod(self, store, demand):
        m = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=4, hold_ticks=2, learn_interval=10_000,
            noise_hot=0.0, noise_cold=0.0, reward_mode="balance"))
        m.set_demand(demand)
        return m

    def _hold(self, mod, coherent, breadth):
        for _ in range(2):
            mod.modulate([_state("a")])
            mod.observe_mind_tide(0.5, coherent=coherent, breadth=breadth)

    def test_full_focus_demand_rewards_focus_only(self, store):
        mod = self._mod(store, demand=1.0)
        self._hold(mod, coherent=0.8, breadth=0.2)
        assert mod._buffer[0][2] == pytest.approx(0.8)

    def test_full_breadth_demand_rewards_breadth_only(self, store):
        mod = self._mod(store, demand=0.0)
        self._hold(mod, coherent=0.8, breadth=0.2)
        assert mod._buffer[0][2] == pytest.approx(0.2)

    def test_mixed_demand_blends(self, store):
        mod = self._mod(store, demand=0.5)
        self._hold(mod, coherent=0.8, breadth=0.2)
        assert mod._buffer[0][2] == pytest.approx(0.5)

    def test_demand_is_an_input_feature(self, store):
        mod = self._mod(store, demand=0.0)
        mod.modulate([_state("a")])
        assert mod._pending[0][-1] == pytest.approx(0.0)  # replicated demand dims
        mod.set_demand(1.0)
        mod._hold_left = 0
        mod.modulate([_state("a")])
        assert mod._pending[0][-1] == pytest.approx(1.0)

    def test_demand_clamped(self, store):
        mod = self._mod(store, demand=5.0)
        assert mod._demand == pytest.approx(1.0)
        mod.set_demand(-2.0)
        assert mod._demand == pytest.approx(0.0)


class TestLearning:
    def test_learns_to_suppress_when_overheated(self, store):
        """Simulated environment: high activity factor overheats the tide
        (ratio 0.9, out of band), low factor lands in band (0.3). The
        modulator should learn to emit act factors below neutral."""
        cfg = ClaustrumConfig(
            max_slots=4, lr=0.05, noise_hot=0.5, noise_cold=0.05,
            anneal_observations=150, learn_interval=10, buffer_cap=200,
            target_low=0.15, target_high=0.45,
            hold_ticks=1,  # this simulated environment reacts instantly
        )
        mod = ClaustrumModulator(store, config=cfg, seed=3)
        state = [_state("a", activity=0.9, conns=8, queue=3)]

        for _ in range(200):
            out = mod.modulate(state)
            act = out["a"][0]
            ratio = 0.9 if act > 1.0 else 0.3
            mod.observe_mind_tide(ratio)

        # after annealing, the emitted factor should sit below neutral
        finals = []
        for _ in range(20):
            out = mod.modulate(state)
            finals.append(out["a"][0])
            mod.observe_mind_tide(0.3)
        avg = sum(finals) / len(finals)
        assert avg < 0.95, f"expected suppression, got avg factor {avg:.3f}"

    def test_reward_shape(self, store):
        mod = ClaustrumModulator(store, config=ClaustrumConfig(
            target_low=0.2, target_high=0.4,
        ))
        assert mod._reward(0.3) == 1.0
        assert mod._reward(0.5) < 1.0
        assert mod._reward(0.9) < mod._reward(0.5)
        assert mod._reward(0.0) < 1.0


def _run_hold(mod, states, ratios):
    """Drive exactly one hold: modulate+observe once per tick."""
    for r in ratios:
        mod.modulate(states)
        mod.observe_mind_tide(r)


class TestRewardShaping:
    """Stability bonus: dwelling inside the band beats crossing it."""

    def _mod(self, store, **overrides):
        cfg = dict(
            max_slots=4, hold_ticks=3, learn_interval=10_000,
            noise_hot=0.0, noise_cold=0.0,
            target_low=0.15, target_high=0.45,
            stability_bonus=1.0, stability_scale=0.15,
        )
        cfg.update(overrides)
        return ClaustrumModulator(store, config=ClaustrumConfig(**cfg))

    def test_steady_in_band_hold_beats_crossing_hold(self, store):
        mod = self._mod(store)
        states = [_state("a")]
        _run_hold(mod, states, [0.30, 0.30, 0.30])   # dwell
        _run_hold(mod, states, [0.05, 0.30, 0.60])   # cross through
        dwell, cross = mod._buffer[0][2], mod._buffer[1][2]
        assert dwell > cross
        # zero-variance fully-in-band hold earns the full bonus
        assert dwell == pytest.approx(2.0)

    def test_bonus_scales_with_in_band_fraction(self, store):
        mod = self._mod(store)
        states = [_state("a")]
        _run_hold(mod, states, [0.30, 0.30, 0.30])
        _run_hold(mod, states, [0.30, 0.30, 0.60])
        assert mod._buffer[0][2] > mod._buffer[1][2]

    def test_bonus_off_by_default(self, store):
        """Default configuration uses mean per-tick reward only."""
        mod = self._mod(store, stability_bonus=0.0)
        _run_hold(mod, [_state("a")], [0.30, 0.30, 0.30])
        assert mod._buffer[0][2] == pytest.approx(1.0)

    def test_out_of_band_hold_gets_no_bonus(self, store):
        mod = self._mod(store)
        _run_hold(mod, [_state("a")], [0.90, 0.90, 0.90])
        assert mod._buffer[0][2] == pytest.approx(mod._reward(0.9))


class TestConsensusCloning:
    """clone_consensus_share: positive samples are cloned toward the
    advantage-weighted mean exploration direction, so per-sample noise
    that disagrees across samples cancels instead of being written into
    the weights."""

    def _mod(self, store, share):
        return ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=1, hold_ticks=1, learn_interval=10_000,
            clone_consensus_share=share,
        ))

    def _stuff_buffer(self, mod):
        import numpy as np
        x = np.zeros(mod._mlp.input_dim)
        raw = np.zeros(4)
        mask = np.ones(4, dtype=bool)
        # two equally-good samples: noise agrees on dim 0, disagrees on
        # dim 1; two below-mean fillers so the good ones have advantage
        # 5th field = the demand this sample was collected under (one regime
        # here, so the per-demand baseline reduces to the batch mean)
        mod._buffer = [
            (x, np.array([1.0, 1.0, 0.0, 0.0]), 1.0, raw, 1.0, mask),
            (x, np.array([1.0, -1.0, 0.0, 0.0]), 1.0, raw, 1.0, mask),
            (x, np.zeros(4), -1.0, raw, 1.0, mask),
            (x, np.zeros(4), -1.0, raw, 1.0, mask),
        ]

    def _captured_targets(self, mod):
        import numpy as np
        captured = []
        mod._mlp.update_regression = (
            lambda x, t, weight=1.0, output_mask=None:
            captured.append(np.asarray(t)) or 0.0
        )
        mod._learn()
        return captured

    def test_consensus_cancels_disagreeing_noise(self, store):
        mod = self._mod(store, share=1.0)
        self._stuff_buffer(mod)
        targets = self._captured_targets(mod)
        assert len(targets) == 2
        for t in targets:  # consensus keeps dim 0, cancels dim 1
            assert t == pytest.approx([1.0, 0.0, 0.0, 0.0])

    def test_share_zero_keeps_current_behavior(self, store):
        mod = self._mod(store, share=0.0)
        self._stuff_buffer(mod)
        targets = self._captured_targets(mod)
        assert len(targets) == 2
        assert targets[0] == pytest.approx([1.0, 1.0, 0.0, 0.0])
        assert targets[1] == pytest.approx([1.0, -1.0, 0.0, 0.0])

    def test_consensus_still_learns_suppression(self, store):
        """End-to-end: the overheated environment from TestLearning is
        still solved with pure consensus cloning."""
        cfg = ClaustrumConfig(
            max_slots=4, lr=0.05, noise_hot=0.5, noise_cold=0.05,
            anneal_observations=150, learn_interval=10, buffer_cap=200,
            target_low=0.15, target_high=0.45, hold_ticks=1,
            clone_consensus_share=1.0,
        )
        mod = ClaustrumModulator(store, config=cfg, seed=3)
        state = [_state("a", activity=0.9, conns=8, queue=3)]
        for _ in range(200):
            out = mod.modulate(state)
            act = out["a"][0]
            mod.observe_mind_tide(0.9 if act > 1.0 else 0.3)
        finals = []
        for _ in range(20):
            out = mod.modulate(state)
            finals.append(out["a"][0])
            mod.observe_mind_tide(0.3)
        avg = sum(finals) / len(finals)
        assert avg < 0.95, f"expected suppression, got avg factor {avg:.3f}"


class TestPotentialRewardShaping:
    """Drive-reduction / potential-based shaping breaks the plateau: two
    holds at the same mean level but opposite trend get different reward,
    restoring advantage spread where absolute reward is flat."""

    def _mod(self, store, weight):
        return ClaustrumModulator(store, config=ClaustrumConfig(
            target_low=0.15, target_high=0.45,
            reward_shaping_weight=weight,
        ))

    def test_distance_to_band(self, store):
        mod = self._mod(store, 0.0)
        assert mod._distance_to_band(0.3) == 0.0          # in band
        assert mod._distance_to_band(0.15) == 0.0         # edge
        assert mod._distance_to_band(0.8) == pytest.approx(0.35)   # above
        assert mod._distance_to_band(0.05) == pytest.approx(0.10)  # below

    def test_moving_toward_band_scores_higher(self, store):
        mod = self._mod(store, weight=1.0)
        toward = mod._hold_reward([0.90, 0.70, 0.50])
        away = mod._hold_reward([0.50, 0.70, 0.90])
        assert toward > away

    def test_breaks_plateau_advantage(self, store):
        mod = self._mod(store, weight=1.0)
        down = mod._hold_reward([0.85, 0.80, 0.75])   # same mean, trending down
        up = mod._hold_reward([0.75, 0.80, 0.85])     # same mean, trending up
        assert down > up

    def test_default_off_is_pure_absolute(self, store):
        mod = self._mod(store, 0.0)
        seq = [0.9, 0.7, 0.5]
        expected = sum(mod._reward(r) for r in seq) / len(seq)
        assert mod._hold_reward(seq) == pytest.approx(expected)


class TestReheatNoise:
    """Out-of-band dwelling keeps exploration alive after the anneal."""

    def _mod(self, store, **overrides):
        cfg = dict(
            max_slots=4, hold_ticks=1, learn_interval=10_000,
            noise_hot=0.6, noise_cold=0.05, anneal_observations=10,
            target_low=0.15, target_high=0.45,
        )
        cfg.update(overrides)
        return ClaustrumModulator(store, config=ClaustrumConfig(**cfg))

    def test_annealed_but_lost_reheats(self, store):
        mod = self._mod(store, noise_reheat_share=1.0)
        for _ in range(20):  # past the anneal, always out of band
            _run_hold(mod, [_state("a")], [0.9])
        assert mod._noise() > 0.5

    def test_annealed_and_dwelling_stays_cold(self, store):
        mod = self._mod(store, noise_reheat_share=1.0)
        for _ in range(40):  # past the anneal, always in band
            _run_hold(mod, [_state("a")], [0.3])
        assert mod._noise() < 0.15

    def test_reheat_off_by_default(self, store):
        mod = self._mod(store)
        for _ in range(20):
            _run_hold(mod, [_state("a")], [0.9])
        assert mod._noise() == pytest.approx(0.05)

    def test_in_band_ema_survives_restart(self, store):
        cfg_kwargs = dict(
            max_slots=4, hold_ticks=1, learn_interval=5,
            noise_hot=0.6, noise_cold=0.05, anneal_observations=10,
            noise_reheat_share=1.0,
        )
        mod = self._mod(store, **cfg_kwargs)
        for _ in range(20):  # dwelling; learn (and save) every 5 holds
            _run_hold(mod, [_state("a")], [0.3])
        assert mod._in_band_ema > 0.5

        reborn = self._mod(store, **cfg_kwargs)
        assert reborn._in_band_ema == pytest.approx(mod._in_band_ema, abs=0.2)


class TestPersistence:
    def test_trained_weights_survive_restart(self, store):
        import numpy as np

        cfg = ClaustrumConfig(
            max_slots=4, lr=0.05, noise_hot=0.5, noise_cold=0.05,
            anneal_observations=150, learn_interval=10, buffer_cap=200,
            hold_ticks=1,
        )
        mod = ClaustrumModulator(store, config=cfg, seed=3)
        state = [_state("a", activity=0.9, conns=8, queue=3)]
        for _ in range(100):
            out = mod.modulate(state)
            act = out["a"][0]
            mod.observe_mind_tide(0.9 if act > 1.0 else 0.3)
        assert np.abs(mod._mlp.w2).sum() > 0  # it actually learned

        reborn = ClaustrumModulator(store, config=cfg, seed=3)
        assert np.allclose(reborn._mlp.w1, mod._mlp.w1)
        assert np.allclose(reborn._mlp.w2, mod._mlp.w2)
        assert reborn._observations > 0  # noise anneal continues, not resets

    def test_untrained_reload_stays_neutral(self, store):
        cfg = ClaustrumConfig(max_slots=8, noise_hot=0.0, noise_cold=0.0)
        ClaustrumModulator(store, config=cfg)  # never learned, saved nothing
        reborn = ClaustrumModulator(store, config=cfg)
        out = reborn.modulate([_state("a")])
        assert out["a"][0] == pytest.approx(1.0)
        assert out["a"][1] == pytest.approx(1.0)

    def test_incompatible_state_starts_fresh(self, store):
        cfg = ClaustrumConfig(
            max_slots=4, lr=0.05, noise_hot=0.5, hold_ticks=1,
            learn_interval=10,
        )
        mod = ClaustrumModulator(store, config=cfg, seed=3)
        state = [_state("a", activity=0.9)]
        for _ in range(40):
            out = mod.modulate(state)
            mod.observe_mind_tide(0.9 if out["a"][0] > 1.0 else 0.3)

        # different max_slots → different MLP shape; must not crash or load
        other = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=8, noise_hot=0.0, noise_cold=0.0,
        ))
        out = other.modulate([_state("a")])
        assert out["a"][0] == pytest.approx(1.0)


class TestEngineIntegration:
    @pytest.fixture
    def stack(self, store):
        llm = LLMAdapter(mock=True)
        conn_net = ConnectionNetwork(store, ConnectionConfig())
        mgr = EngramManager(store, llm, conn_net)
        dendrite = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=0.0, default_max_wait=0.0,
        ))
        runtime = RuntimeManager(RuntimeConfig(
            hourly_token_budget=1_000_000, daily_token_budget=10_000_000,
        ))
        claustrum = ClaustrumModulator(store, config=ClaustrumConfig(
            max_slots=8, noise_hot=0.0, noise_cold=0.0, hold_ticks=1,
        ))
        engine = PulseEngine(
            storage=store, engram_manager=mgr, connection_network=conn_net,
            dendrite=dendrite, runtime=runtime, claustrum=claustrum,
            config=PulseEngineConfig(
                spontaneous_check_interval=1000.0, decay_interval=1000.0,
            ),
        )
        return store, mgr, dendrite, engine, claustrum

    def test_tick_runs_and_observes(self, stack):
        store, mgr, _, engine, claustrum = stack
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="hello"),
        ])
        engine.inject_external_event(e.id, "go")
        engine.tick()
        engine.tick()
        assert claustrum._observations >= 1

    def test_activity_modulation_composes_into_probability(self, stack):
        store, mgr, _, engine, _ = stack
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="x"),
        ])
        store.update_engram_metadata(e.id, self_excitability=1.0,
                                     recent_activity=1.0)
        engram = store.get_engram(e.id)
        p_neutral = engine._compute_spontaneous_probability(e.id, engram)
        engine._activity_mods[e.id] = 0.5
        p_damped = engine._compute_spontaneous_probability(e.id, engram)
        assert p_damped == pytest.approx(p_neutral * 0.5)

    def test_wait_modifier_scales_dendrite_wait(self, stack):
        _, mgr, dendrite, _, _ = stack
        e = mgr.create()
        dendrite.set_wait_time(e.id, 10.0)
        dendrite.set_wait_modifiers({e.id: 1.5})
        assert dendrite.get_wait_time(e.id) == pytest.approx(15.0)
        dendrite.set_wait_modifiers({})
        assert dendrite.get_wait_time(e.id) == pytest.approx(10.0)

    def test_propagation_threshold_modulated_by_source(self, stack):
        store, mgr, dendrite, engine, _ = stack
        a = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="a"),
        ])
        b = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="b"),
        ])
        store.create_connection(a.id, b.id, 0.4)  # above base threshold 0.3

        # raise a's propagation threshold above the edge weight → no spread
        engine._propagation_mods = {a.id: 6.0}   # 0.3 * 6 = 1.8 > 0.4
        engine._propagate(a.id, "x")
        assert dendrite.get_queue_size(b.id) == 0

        # neutral factor → 0.4 ≥ 0.3 → propagates
        engine._propagation_mods = {a.id: 1.0}
        engine._propagate(a.id, "x")
        assert dendrite.get_queue_size(b.id) >= 1

    def test_detached_claustrum_is_noop(self, store):
        llm = LLMAdapter(mock=True)
        conn_net = ConnectionNetwork(store, ConnectionConfig())
        mgr = EngramManager(store, llm, conn_net)
        dendrite = DendriteProcessor(mgr, DendriteConfig(
            silence_threshold=0.0, default_max_wait=0.0,
        ))
        engine = PulseEngine(
            storage=store, engram_manager=mgr, connection_network=conn_net,
            dendrite=dendrite,
            runtime=RuntimeManager(RuntimeConfig(
                hourly_token_budget=1_000_000, daily_token_budget=1_000_000,
            )),
            config=PulseEngineConfig(
                spontaneous_check_interval=1000.0, decay_interval=1000.0,
            ),
        )
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="plain"),
        ])
        engine.inject_external_event(e.id, "go")
        results = engine.tick()
        assert len(results) == 1

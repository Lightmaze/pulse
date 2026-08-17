"""Tests for the runtime resource manager."""

import pytest

from pulse_system.core.runtime import RuntimeConfig, RuntimeManager, StabilityAdvice


@pytest.fixture
def runtime():
    config = RuntimeConfig(
        budget_per_tick=5,
        hourly_token_budget=10_000,
        daily_token_budget=100_000,
        heartbeat_high=0.7,
        heartbeat_low=0.05,
    )
    return RuntimeManager(config)


# ── Budget ───────────────────────────────────────────────────────


class TestBudget:
    def test_initial_budget(self, runtime: RuntimeManager):
        assert runtime.get_budget() == 5

    def test_budget_after_consumption(self, runtime: RuntimeManager):
        runtime.consume_budget(100, 50, 0)
        assert runtime.get_budget() == 5  # still within hourly limit

    def test_budget_exhausted(self, runtime: RuntimeManager):
        # Exhaust hourly budget (10000 tokens)
        runtime.consume_budget(5000, 5000, 0)
        assert runtime.get_budget() == 0

    def test_budget_tracks_tokens(self, runtime: RuntimeManager):
        runtime.consume_budget(100, 50, cached_input_tokens=80)
        runtime.consume_budget(200, 100, cached_input_tokens=0)
        stats = runtime.get_stats()
        assert stats.total_pulses == 2
        assert stats.total_input_tokens == 300
        assert stats.total_output_tokens == 150
        assert stats.total_cached_input_tokens == 80
        assert stats.total_cache_hits == 1
        assert stats.total_cache_misses == 1

    def test_consume_is_incremental_not_cumulative(self, runtime: RuntimeManager):
        """N pulses cost the sum of per-call tokens, not O(N^2)."""
        for _ in range(10):
            runtime.consume_budget(100, 50, 0)
        stats = runtime.get_stats()
        assert stats.tokens_this_hour == 10 * 150

    def test_cached_tokens_are_discounted(self):
        runtime = RuntimeManager(RuntimeConfig(
            budget_per_tick=5,
            hourly_token_budget=10_000,
            daily_token_budget=100_000,
            cache_read_discount=0.1,
        ))
        # 1000 input fully cached + 100 output
        runtime.consume_budget(1000, 100, cached_input_tokens=1000)
        stats = runtime.get_stats()
        # billable = 100 + 0 + 1000*0.1 = 200
        assert stats.tokens_this_hour == 200

    def test_cached_tokens_clamped_to_input(self, runtime: RuntimeManager):
        # Defensive: provider reporting cached > input must not go negative
        runtime.consume_budget(100, 10, cached_input_tokens=500)
        stats = runtime.get_stats()
        assert stats.total_cached_input_tokens == 100
        assert stats.tokens_this_hour == 10 + 0 + int(100 * 0.1)

    def test_budget_scales_down_near_exhaustion(self):
        runtime = RuntimeManager(RuntimeConfig(
            budget_per_tick=5,
            hourly_token_budget=10_000,
            daily_token_budget=100_000,
        ))
        # Average pulse ≈ 3000 billable tokens; 9000 spent, 1000 remain.
        for _ in range(3):
            runtime.consume_budget(2000, 1000, 0)
        # Remaining 1000 // avg 3000 → 0 pulses, not a full batch of 5
        assert runtime.get_budget() == 0


# ── Global stability ────────────────────────────────────────────


class TestGlobalStability:
    def test_normal_range(self, runtime: RuntimeManager):
        # 30% active → normal
        advice = runtime.check_global_stability(30, 100)
        assert advice == StabilityAdvice.NORMAL

    def test_too_high(self, runtime: RuntimeManager):
        # 80% active → reduce
        advice = runtime.check_global_stability(80, 100)
        assert advice == StabilityAdvice.REDUCE_SPONTANEOUS

    def test_too_low(self, runtime: RuntimeManager):
        # 2% active → increase
        advice = runtime.check_global_stability(2, 100)
        assert advice == StabilityAdvice.INCREASE_SPONTANEOUS

    def test_boundary_high(self, runtime: RuntimeManager):
        # Exactly at threshold
        advice = runtime.check_global_stability(70, 100)
        assert advice == StabilityAdvice.NORMAL  # <=0.7 is normal

    def test_boundary_low(self, runtime: RuntimeManager):
        # Exactly at threshold
        advice = runtime.check_global_stability(5, 100)
        assert advice == StabilityAdvice.NORMAL  # >=0.05 is normal

    def test_zero_engrams(self, runtime: RuntimeManager):
        advice = runtime.check_global_stability(0, 0)
        assert advice == StabilityAdvice.NORMAL

    def test_all_active(self, runtime: RuntimeManager):
        advice = runtime.check_global_stability(100, 100)
        assert advice == StabilityAdvice.REDUCE_SPONTANEOUS


# ── Stats ────────────────────────────────────────────────────────


class TestStats:
    def test_initial_stats(self, runtime: RuntimeManager):
        stats = runtime.get_stats()
        assert stats.total_pulses == 0
        assert stats.tokens_this_hour == 0
        assert stats.tokens_today == 0

    def test_accumulation(self, runtime: RuntimeManager):
        runtime.consume_budget(100, 50, 0)
        runtime.consume_budget(200, 100, 0)
        stats = runtime.get_stats()
        assert stats.tokens_this_hour == 450
        assert stats.tokens_today == 450

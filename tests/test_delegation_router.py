"""Tests for the delegation MLP router."""

import numpy as np
import pytest

from pulse_system.agent.delegate import Delegator, DelegatorConfig
from pulse_system.agent.tools import ToolRegistry
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.delegation import DelegationRouter, RouterConfig
from pulse_system.core.engram import EngramManager
from pulse_system.core.runtime.publication import (
    RuntimePublicationPermit,
    RuntimePublicationGate,
)
from pulse_system.core.types import Message, MessageRole
from pulse_system.education.library import Library
from pulse_system.interaction.metrics import MetricsRecorder
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


def _publication_permit(owner_id: str) -> RuntimePublicationPermit:
    return RuntimePublicationGate(owner_id, 1).publication_permit


@pytest.fixture
def store():
    s = Storage(":memory:")
    yield s
    s.close()


@pytest.fixture
def router(store):
    return DelegationRouter(store, config=RouterConfig(
        max_slots=16, lr=0.1, anneal_updates=50,
    ))


def _task_vec(seed: int):
    return np.random.default_rng(seed).normal(size=256).tolist()


def _seed_records(store, *, caller="front", good="B", bad="D", n=6, task_seed=42):
    """Synthetic history: `good` is always adopted, `bad` always discarded."""
    import json
    for i in range(n):
        emb = json.dumps(_task_vec(task_seed))
        rid_good = store.create_delegation(
            caller, good, f"task variant {i}", "mainline", task_embedding=emb,
        )
        store.set_delegation_outcome(rid_good, "adopted")
        rid_bad = store.create_delegation(
            caller, bad, f"task variant {i}", "mainline", task_embedding=emb,
        )
        store.set_delegation_outcome(rid_bad, "discarded")


class TestRanking:
    def test_cold_start_returns_scores_for_all(self, router):
        scores = router.rank("front", _task_vec(1), ["a", "b", "c"])
        assert set(scores) == {"a", "b", "c"}

    def test_learning_promotes_adopted_target(self, store, router):
        _seed_records(store)
        updates = router.learn_from_history()
        assert updates > 0

        scores = router.rank("front", _task_vec(42), ["B", "D"])
        assert scores["B"] > scores["D"]

    def test_learning_is_idempotent_per_record(self, store, router):
        _seed_records(store, n=3)
        first = router.learn_from_history()
        second = router.learn_from_history()
        assert first > 0
        assert second == 0

    def test_temperature_anneals(self, store, router):
        hot = router.temperature()
        _seed_records(store, n=20)
        router.learn_from_history()
        assert router.temperature() < hot


class TestChoose:
    def test_choose_returns_live_candidate(self, router):
        rng = np.random.default_rng(0)
        decision = router.choose("front", _task_vec(2), ["a", "b"], rng=rng)
        assert decision.target_id in ("a", "b")

    def test_no_candidates(self, router):
        decision = router.choose("front", _task_vec(2), [])
        assert decision.target_id is None

    def test_cold_start_arms_canary(self, router):
        """Untrained scores are near-identical → top-2 probs close → canary."""
        rng = np.random.default_rng(0)
        decision = router.choose("front", _task_vec(3), ["a", "b"], rng=rng)
        assert decision.canary_id is not None
        assert decision.canary_id != decision.target_id

    def test_trained_router_skips_canary(self, store, router):
        _seed_records(store, n=10)
        router.learn_from_history()
        rng = np.random.default_rng(0)
        decision = router.choose("front", _task_vec(42), ["B", "D"], rng=rng)
        assert decision.target_id == "B"
        assert decision.canary_id is None


class TestSlotLifecycle:
    def test_explicit_feature_identity_makes_isomorphic_callers_equivalent(self):
        stores = [Storage(":memory:"), Storage(":memory:")]
        try:
            routers = [
                DelegationRouter(
                    item,
                    config=RouterConfig(max_slots=16, lr=0.1),
                )
                for item in stores
            ]
            raw_callers = ("random-production-id-a", "random-production-id-b")
            for item, storage, candidate_ids, caller_id, candidate_prefix in zip(
                routers,
                stores,
                (("a-good", "a-bad"), ("b-good", "b-bad")),
                raw_callers,
                ("a", "b"),
            ):
                item.register_engram(
                    caller_id,
                    feature_identity_key="member:r0:c1",
                )
                item.register_engram(f"{candidate_prefix}-good")
                item.register_engram(f"{candidate_prefix}-bad")
                _seed_records(
                    storage,
                    caller=caller_id,
                    good=candidate_ids[0],
                    bad=candidate_ids[1],
                    n=3,
                )
                assert item.learn_from_history() > 0

            assert routers[0]._mlp.state_dict() == routers[1]._mlp.state_dict()
        finally:
            for item in stores:
                item.close()

    def test_feature_identity_follows_succession_slot(self, router):
        router.register_engram("old", feature_identity_key="member:r0:c1")
        router.reassign_engram("old", "new")

        assert router.feature_identity_key_for("new") == "member:r0:c1"
        assert router.feature_identity_key_for("old") == "old"

    def test_reassign_preserves_learned_score(self, store, router):
        _seed_records(store)
        router.learn_from_history()
        before = router.rank("front", _task_vec(42), ["B"])["B"]

        router.reassign_engram("B", "B2")  # succession

        after = router.rank("front", _task_vec(42), ["B2"])["B2"]
        assert after == pytest.approx(before)

    def test_mask_releases_slot(self, store, router):
        router.register_engram("gone")
        router.mask_engram("gone")
        assert router._slots.slot_of("gone", create=False) is None


class TestPersistence:
    """Learned weights survive a process restart (same Storage db)."""

    _CFG = dict(max_slots=16, lr=0.1, anneal_updates=50)

    def test_weights_survive_restart(self, store, router):
        _seed_records(store)
        router.learn_from_history()
        before = router.rank("front", _task_vec(42), ["B", "D"])

        reborn = DelegationRouter(store, config=RouterConfig(**self._CFG))
        after = reborn.rank("front", _task_vec(42), ["B", "D"])
        assert after["B"] == pytest.approx(before["B"])
        assert after["D"] == pytest.approx(before["D"])
        assert after["B"] > after["D"]

    def test_restart_does_not_relearn_consumed_records(self, store, router):
        _seed_records(store, n=3)
        assert router.learn_from_history() > 0

        reborn = DelegationRouter(store, config=RouterConfig(**self._CFG))
        # already-consumed pairs must not be double-applied after reload
        assert reborn.learn_from_history() == 0

    def test_temperature_anneal_survives_restart(self, store, router):
        hot = router.temperature()
        _seed_records(store, n=20)
        router.learn_from_history()

        reborn = DelegationRouter(store, config=RouterConfig(**self._CFG))
        assert reborn.temperature() == pytest.approx(router.temperature())
        assert reborn.temperature() < hot

    def test_incompatible_state_starts_fresh(self, store, router):
        _seed_records(store)
        router.learn_from_history()

        # different max_slots → different MLP shape; must not crash or load
        reborn = DelegationRouter(store, config=RouterConfig(max_slots=8))
        assert reborn._updates_seen == 0


class TestDelegatorIntegration:
    @pytest.fixture
    def stack(self, store, tmp_path):
        llm = LLMAdapter(mock=True)
        conn_net = ConnectionNetwork(store, ConnectionConfig())
        mgr = EngramManager(store, llm, conn_net)
        tools = ToolRegistry(mock=True, workspace_root=tmp_path)
        library = Library(
            tmp_path / "lib",
            publication_authority=_publication_permit(
                "test:delegation-router-library"
            ),
        )
        metrics = MetricsRecorder()
        router = DelegationRouter(store, config=RouterConfig(max_slots=16),
                                  metrics=metrics)
        delegator = Delegator(
            store, mgr, tools, library=library, metrics=metrics,
            router=router, config=DelegatorConfig(max_think_iterations=1),
        )
        return store, mgr, router, delegator, metrics

    def _make(self, mgr, content):
        return mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content=content),
        ])

    def test_routed_without_candidates_creates_new(self, stack):
        store, mgr, router, delegator, _ = stack
        results = delegator.delegate_routed("front", "fresh territory task")
        assert len(results) == 1
        assert results[0].mode == "mainline"

    def test_routed_canary_runs_both_in_one_group(self, stack):
        store, mgr, router, delegator, _ = stack
        a = self._make(mgr, "candidate A history")
        b = self._make(mgr, "candidate B history")

        results = delegator.delegate_routed("front", "ambiguous task")
        # cold router → canary fires → both run in snapshot mode
        assert len(results) == 2
        assert {r.mode for r in results} == {"snapshot"}
        records = store.list_delegations("front")
        groups = {r["group_id"] for r in records}
        assert len(groups) == 1 and None not in groups
        assert {r["target_id"] for r in records} == {a.id, b.id}

    def test_outcome_feeds_router_learning(self, stack):
        store, mgr, router, delegator, _ = stack
        self._make(mgr, "A")
        self._make(mgr, "B")
        results = delegator.delegate_routed("front", "judge me")
        assert len(results) == 2
        delegator.record_outcome(results[0].record_id, "adopted")
        delegator.record_outcome(results[1].record_id, "discarded")
        # canary pair consumed as a pairwise update
        assert router._updates_seen >= 1

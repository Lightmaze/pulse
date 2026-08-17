"""OnlineLearningPolicy freezes mutation, never inference assembly."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from pulse_system.core.claustrum import (
    ClaustrumConfig,
    ClaustrumModulator,
    EngramState,
)
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.delegation import DelegationRouter, RouterConfig
from pulse_system.core.learning_policy import (
    OnlineLearningAudit,
    OnlineLearningChannel,
    OnlineLearningPolicy,
)
from pulse_system.service import RuntimeService, RuntimeServiceConfig
from pulse_system.substrate.storage import Storage


def _canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _state(engram_id: str = "member") -> EngramState:
    return EngramState(
        engram_id=engram_id,
        recent_activity=0.3,
        connection_count=4,
        queue_depth=1,
        seconds_since_pulse=2.0,
        cluster_activity=0.25,
    )


def _seed_router_history(storage: Storage) -> None:
    embedding = json.dumps(np.zeros(256).tolist())
    good = storage.create_delegation(
        "caller",
        "good",
        "same bounded task",
        "mainline",
        task_embedding=embedding,
        group_id="comparison",
    )
    bad = storage.create_delegation(
        "caller",
        "bad",
        "same bounded task",
        "mainline",
        task_embedding=embedding,
        group_id="comparison",
    )
    storage.set_delegation_outcome(good, "adopted")
    storage.set_delegation_outcome(bad, "discarded")


def test_policy_is_immutable_strict_and_complete():
    policy = OnlineLearningPolicy.disabled()

    assert policy.as_dict() == {
        "connection_stdp": False,
        "connection_decay_prune": False,
        "delegation_mlp": False,
        "claustrum_mlp": False,
    }
    with pytest.raises(FrozenInstanceError):
        policy.connection_stdp = True
    with pytest.raises(ValueError, match="must be a bool"):
        OnlineLearningPolicy(connection_stdp=1)
    with pytest.raises(ValueError, match="unknown online-learning channel"):
        policy.allows("future_hidden_learner")


def test_disabled_connection_channels_record_attempts_without_field_mutation():
    storage = Storage(":memory:")
    try:
        storage.create_engram(engram_id="a")
        storage.create_engram(engram_id="b")
        storage.create_connection("a", "b", 0.4, layer="factory")
        storage.create_connection("b", "a", 0.4, layer="factory")
        audit = OnlineLearningAudit()
        network = ConnectionNetwork(
            storage,
            ConnectionConfig(decay_rate=0.1, prune_threshold=0.5),
            learning_policy=OnlineLearningPolicy.disabled(),
            learning_audit=audit,
        )
        before = _canonical(storage.export_field_weights())
        started = datetime(2026, 1, 1, tzinfo=timezone.utc)

        assert network.stdp_update(
            [("a", started), ("b", started + timedelta(seconds=1))]
        ) == []
        assert network.decay_and_prune() == (0, 0)

        after = _canonical(storage.export_field_weights())
        snapshot = network.learning_audit_snapshot()["channels"]
        assert after == before
        assert snapshot["connection_stdp"] == {"attempts": 1, "applied": 0}
        assert snapshot["connection_decay_prune"] == {
            "attempts": 1,
            "applied": 0,
        }
    finally:
        storage.close()


def test_default_connection_policy_preserves_existing_learning_behavior():
    storage = Storage(":memory:")
    try:
        storage.create_engram(engram_id="a")
        storage.create_engram(engram_id="b")
        network = ConnectionNetwork(storage, ConnectionConfig())
        started = datetime(2026, 1, 1, tzinfo=timezone.utc)

        changed = network.stdp_update(
            [("a", started), ("b", started + timedelta(seconds=1))]
        )

        snapshot = network.learning_audit_snapshot()["channels"]
        assert changed
        assert storage.get_connection("a", "b") is not None
        assert snapshot["connection_stdp"]["attempts"] == 1
        assert snapshot["connection_stdp"]["applied"] == len(changed)
    finally:
        storage.close()


def test_disabled_router_keeps_weights_and_learning_eligibility_unchanged():
    storage = Storage(":memory:")
    try:
        _seed_router_history(storage)
        router = DelegationRouter(
            storage,
            config=RouterConfig(max_slots=8),
            learning_policy=OnlineLearningPolicy.disabled(),
        )
        before = storage.load_weight_state("delegation_mlp", "field")

        assert router.learn_from_history() == 0

        assert storage.load_weight_state("delegation_mlp", "field") == before
        assert router._updates_seen == 0
        assert router._learned_pairs == set()
        channel = router.learning_audit_snapshot()["channels"]["delegation_mlp"]
        assert channel["attempts"] == 1
        assert channel["applied"] == 0
    finally:
        storage.close()


def test_disabled_claustrum_keeps_mlp_observations_and_ema_unchanged():
    storage = Storage(":memory:")
    try:
        modulator = ClaustrumModulator(
            storage,
            config=ClaustrumConfig(
                max_slots=8,
                hold_ticks=1,
                learn_interval=1,
                noise_hot=0.2,
                noise_cold=0.1,
            ),
            seed=9,
            learning_policy=OnlineLearningPolicy.disabled(),
        )
        before = _canonical(modulator._field_state())

        for _ in range(4):
            assert modulator.modulate([_state()])
            modulator.observe_mind_tide(0.3)

        after = _canonical(modulator._field_state())
        channel = modulator.learning_audit_snapshot()["channels"]["claustrum_mlp"]
        assert after == before
        assert modulator._buffer == []
        assert modulator._observations == 0
        assert modulator._in_band_ema == 0.0
        assert channel == {"attempts": 4, "applied": 0}
    finally:
        storage.close()


def test_runtime_assembles_all_inference_components_under_disabled_policy(tmp_path):
    service = RuntimeService(
        RuntimeServiceConfig(
            db_path=tmp_path / "organism.db",
            workspace=tmp_path,
            metrics_path=tmp_path / "metrics.jsonl",
            mock=True,
            with_router=True,
            with_claustrum=True,
            online_learning_policy=OnlineLearningPolicy.disabled(),
        )
    )
    try:
        assert service.connections is not None
        assert service.router is not None
        assert service.claustrum is not None
        assert service.online_learning_audit["policy"] == {
            channel.value: False for channel in OnlineLearningChannel
        }
    finally:
        service.close()


def test_runtime_rejects_untyped_learning_policy(tmp_path):
    with pytest.raises(ValueError, match="must be an OnlineLearningPolicy"):
        RuntimeServiceConfig(
            db_path=tmp_path / "bad.db",
            workspace=tmp_path,
            online_learning_policy={"connection_stdp": False},
        )

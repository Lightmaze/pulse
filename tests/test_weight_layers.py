"""Release-gate tests for explicit factory and in-field weight layers."""

from datetime import datetime, timedelta, timezone

import pytest

from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.delegation import DelegationRouter, RouterConfig
from pulse_system.substrate.storage import Storage


@pytest.fixture
def store():
    storage = Storage(":memory:")
    for engram_id in ("a", "b", "c"):
        storage.create_engram(engram_id=engram_id)
    yield storage
    storage.close()


def test_connection_factory_and_field_are_separate(store):
    store.create_connection("a", "b", 0.4)
    untouched = store.get_connection("a", "b")
    assert untouched.factory_weight == pytest.approx(0.4)
    assert untouched.learned_weight is None

    store.update_weight("a", "b", 0.75, layer="field")
    learned = store.get_connection("a", "b")
    assert learned.weight == pytest.approx(0.75)
    assert learned.factory_weight == pytest.approx(0.4)
    assert learned.learned_weight == pytest.approx(0.75)


def test_stdp_creates_a_field_edge_not_a_factory_prior(store):
    network = ConnectionNetwork(
        store,
        ConnectionConfig(stdp_strength=0.2, ltd_strength=0.0),
    )
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    network.stdp_update(
        [("a", start), ("b", start + timedelta(seconds=1))]
    )

    connection = store.get_connection("a", "b")
    assert connection.weight == pytest.approx(0.1)
    assert connection.factory_weight == pytest.approx(0.0)
    assert connection.learned_weight == pytest.approx(0.1)


def test_pruned_factory_edge_can_be_reintroduced_without_replacing_baseline(
    store,
):
    store.create_connection("a", "b", 0.005)
    store.prune(0.01)
    assert store.get_connection("a", "b") is None

    restored = store.create_connection("a", "b", 0.9)
    assert restored.weight == pytest.approx(0.005)
    assert restored.factory_weight == pytest.approx(0.005)
    assert restored.learned_weight is None


def test_export_reset_and_import_round_trip(store):
    store.create_connection("a", "b", 0.4)
    store.create_connection("a", "c", 0.005)
    store.create_connection("b", "c", 0.2, layer="field")
    store.update_weight("a", "b", 0.8)
    assert store.prune(0.01) == 1
    store.save_weight_state("delegation_mlp", "factory", {"v": 0})
    store.save_weight_state("delegation_mlp", "field", {"v": 3})

    payload = store.export_field_weights()
    assert payload["format"] == "pc01.field-weights.v1"
    assert payload["components"] == {"delegation_mlp": {"v": 3}}
    assert any(
        item["from_id"] == "a"
        and item["to_id"] == "c"
        and item["tombstone"]
        for item in payload["connections"]
    )

    cleared = store.reset_field_weights()
    assert cleared == {
        "connections": 2,
        "tombstones": 1,
        "components": 1,
    }
    assert store.get_connection("a", "b").weight == pytest.approx(0.4)
    assert store.get_connection("a", "c").weight == pytest.approx(0.005)
    assert store.get_connection("b", "c") is None
    assert store.load_weight_state("delegation_mlp", "field") is None
    assert store.load_weight_state("delegation_mlp", "factory") == {"v": 0}

    counts = store.import_field_weights(payload)
    assert counts == {
        "connections": 2,
        "tombstones": 1,
        "components": 1,
    }
    assert store.get_connection("a", "b").weight == pytest.approx(0.8)
    assert store.get_connection("a", "c") is None
    assert store.get_connection("b", "c").weight == pytest.approx(0.2)
    assert store.load_weight_state("delegation_mlp", "field") == {"v": 3}


def test_checkpoint_rolls_back_only_the_field_layer(store):
    store.create_connection("a", "b", 0.4)
    store.update_weight("a", "b", 0.7)
    checkpoint = store.checkpoint_field_weights("known-good")

    store.update_weight("a", "b", 0.1)
    store.save_weight_state("claustrum_mlp", "field", {"bad": True})
    store.rollback_field_weights(checkpoint)

    restored = store.get_connection("a", "b")
    assert restored.weight == pytest.approx(0.7)
    assert restored.factory_weight == pytest.approx(0.4)
    assert store.load_weight_state("claustrum_mlp", "field") is None
    assert store.list_weight_checkpoints()[0]["label"] == "known-good"


def test_router_bootstraps_factory_then_writes_field(store):
    router = DelegationRouter(
        store,
        config=RouterConfig(max_slots=8, caller_dim=4, task_dim=4),
    )
    factory = store.load_weight_state("delegation_mlp", "factory")
    assert factory is not None
    assert store.load_weight_state("delegation_mlp", "field") is None

    # A direct save is enough to pin the storage contract; the router's
    # learning tests separately prove that only relative outcome pairs call it.
    router._save_state()
    field = store.load_weight_state("delegation_mlp", "field")
    assert field is not None
    assert field["mlp"]["input_dim"] == factory["mlp"]["input_dim"]


def test_weight_reset_and_slot_release_commit_in_one_boundary(store):
    component = "claustrum_mlp"
    assert store.assign_slot(component, "a") == 0
    store.save_weight_state(component, "field", {"version": "old"})

    store.save_weight_state_and_release_slot(
        component,
        "field",
        {"version": "neutral"},
        "a",
    )

    assert store.load_weight_state(component, "field") == {
        "version": "neutral"
    }
    assert "a" not in store.get_slot_map(component)


def test_invalid_atomic_weight_state_leaves_slot_and_field_unchanged(store):
    component = "claustrum_mlp"
    assert store.assign_slot(component, "a") == 0
    store.save_weight_state(component, "field", {"version": "old"})

    with pytest.raises(TypeError):
        store.save_weight_state_and_release_slot(
            component,
            "field",
            {"not_json": {"a"}},
            "a",
        )

    assert store.load_weight_state(component, "field") == {"version": "old"}
    assert store.get_slot_map(component) == {"a": 0}

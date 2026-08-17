"""Tests for the shared numeric substrate (core/nn)."""

import numpy as np
import pytest

from pulse_system.core.nn import (
    SlotIndex,
    TwoLayerMLP,
    random_project,
    stable_id_embedding,
)
from pulse_system.substrate.storage import Storage


class TestTwoLayerMLP:
    def test_forward_shape(self):
        mlp = TwoLayerMLP(8, 16, 4)
        y = mlp.forward(np.ones(8))
        assert y.shape == (4,)

    def test_zero_output_init_is_neutral(self):
        mlp = TwoLayerMLP(8, 16, 4, zero_output_init=True)
        assert np.allclose(mlp.forward(np.random.default_rng(0).normal(size=8)), 0.0)

    def test_pairwise_update_raises_winner(self):
        mlp = TwoLayerMLP(8, 16, 4, lr=0.1)
        x = np.ones(8)
        for _ in range(50):
            mlp.update_pairwise(x, winner=1, loser=3)
        y = mlp.forward(x)
        assert y[1] > y[3]

    def test_pairwise_loss_decreases(self):
        mlp = TwoLayerMLP(8, 16, 4, lr=0.1)
        x = np.ones(8)
        first = mlp.update_pairwise(x, 0, 2)
        for _ in range(30):
            last = mlp.update_pairwise(x, 0, 2)
        assert last < first

    def test_regression_converges(self):
        mlp = TwoLayerMLP(4, 16, 3, lr=0.05)
        x = np.array([1.0, 0.5, -0.5, 0.2])
        target = np.array([0.3, -0.7, 1.2])
        for _ in range(300):
            mlp.update_regression(x, target)
        assert np.allclose(mlp.forward(x), target, atol=0.1)

    def test_masked_regression_uses_only_selected_outputs(self):
        mlp = TwoLayerMLP(2, 3, 4, lr=0.1, zero_output_init=True)
        x = np.array([1.0, -0.5])
        target = np.array([2.0, 100.0, -1.0, -100.0])
        mask = np.array([True, False, True, False])

        inactive_w2 = mlp.w2[:, ~mask].copy()
        inactive_b2 = mlp.b2[~mask].copy()
        loss = mlp.update_regression(
            x,
            target,
            weight=0.5,
            output_mask=mask,
        )

        assert loss == pytest.approx(1.25)
        assert np.array_equal(mlp.w2[:, ~mask], inactive_w2)
        assert np.array_equal(mlp.b2[~mask], inactive_b2)

    def test_masked_regression_normalizes_by_active_output_count(self):
        mlp = TwoLayerMLP(1, 2, 4, lr=0.2, zero_output_init=True)
        mask = np.array([True, False, True, False])
        mlp.update_regression(
            np.array([1.0]),
            np.array([1.0, 0.0, 1.0, 0.0]),
            output_mask=mask,
        )

        # dy for each selected output is -1: 2 * -1 / selected_count(2).
        assert mlp.b2[[0, 2]] == pytest.approx([0.2, 0.2])
        assert np.array_equal(mlp.b2[[1, 3]], np.zeros(2))

    @pytest.mark.parametrize(
        ("target", "mask", "message"),
        [
            (np.zeros(2), None, "target"),
            (np.zeros(3), np.ones(2, dtype=bool), "output_mask"),
            (np.zeros(3), np.zeros(3, dtype=bool), "select an output"),
        ],
    )
    def test_invalid_regression_mask_fails_before_update(
        self, target, mask, message
    ):
        mlp = TwoLayerMLP(2, 3, 3, lr=0.1)
        before = tuple(
            value.copy() for value in (mlp.w1, mlp.b1, mlp.w2, mlp.b2)
        )

        with pytest.raises(ValueError, match=message):
            mlp.update_regression(
                np.ones(2),
                target,
                output_mask=mask,
            )

        after = (mlp.w1, mlp.b1, mlp.w2, mlp.b2)
        assert all(np.array_equal(a, b) for a, b in zip(before, after))

    def test_state_dict_roundtrip(self):
        a = TwoLayerMLP(4, 8, 2, seed=1)
        b = TwoLayerMLP(4, 8, 2, seed=99)
        x = np.ones(4)
        assert not np.allclose(a.forward(x), b.forward(x))
        b.load_state_dict(a.state_dict())
        assert np.allclose(a.forward(x), b.forward(x))


class TestSlotIndex:
    @pytest.fixture
    def store(self):
        s = Storage(":memory:")
        yield s
        s.close()

    def test_assign_and_lookup(self, store):
        idx = SlotIndex(store, "test", 8)
        s1 = idx.slot_of("e1")
        s2 = idx.slot_of("e2")
        assert s1 != s2
        assert idx.slot_of("e1") == s1  # stable
        assert idx.id_of(s1) == "e1"

    def test_persistence_across_instances(self, store):
        idx = SlotIndex(store, "test", 8)
        s1 = idx.slot_of("e1")
        idx2 = SlotIndex(store, "test", 8)
        assert idx2.slot_of("e1", create=False) == s1

    def test_release_frees_slot_for_reuse(self, store):
        idx = SlotIndex(store, "test", 8)
        s1 = idx.slot_of("e1")
        idx.release("e1")
        assert idx.slot_of("e1", create=False) is None
        assert idx.slot_of("e2") == s1  # smallest free slot reused

    def test_reassign_inherits_slot(self, store):
        idx = SlotIndex(store, "test", 8)
        s1 = idx.slot_of("old")
        idx.reassign("old", "new")
        assert idx.slot_of("new", create=False) == s1
        assert idx.slot_of("old", create=False) is None

    def test_capacity_limit(self, store):
        idx = SlotIndex(store, "test", 2)
        assert idx.slot_of("a") is not None
        assert idx.slot_of("b") is not None
        assert idx.slot_of("c") is None  # full → caller falls back

    def test_mask(self, store):
        idx = SlotIndex(store, "test", 4)
        idx.slot_of("a")
        idx.slot_of("b")
        m = idx.mask()
        assert m.sum() == 2

    def test_components_are_isolated(self, store):
        a = SlotIndex(store, "comp_a", 8)
        b = SlotIndex(store, "comp_b", 8)
        a.slot_of("e1")
        assert b.slot_of("e1", create=False) is None


class TestFeatures:
    def test_stable_embedding_deterministic_unit(self):
        v1 = stable_id_embedding("engram-x", 64)
        v2 = stable_id_embedding("engram-x", 64)
        v3 = stable_id_embedding("engram-y", 64)
        assert np.allclose(v1, v2)
        assert not np.allclose(v1, v3)
        assert np.isclose(np.linalg.norm(v1), 1.0)

    def test_random_project_deterministic(self):
        v = np.arange(256, dtype=float)
        p1 = random_project(v, 128)
        p2 = random_project(v, 128)
        assert p1.shape == (128,)
        assert np.allclose(p1, p2)

    def test_projection_handles_differing_input_dims(self):
        a = random_project(np.ones(256), 128)
        b = random_project(np.ones(1536), 128)
        assert a.shape == b.shape == (128,)

    def test_identity_when_dims_match(self):
        v = np.arange(128, dtype=float)
        assert np.allclose(random_project(v, 128), v)

"""Shared two-layer MLP for the runtime-sideband learning components.

The delegation router and Claustrum modulator share a small masked MLP with
online SGD: two layers, a ReLU hidden layer, fast inference, and stable online
learning on scarce early data.
This module owns that substrate so both components stay thin.

Two loss modes are provided as explicit update methods rather than a
generic autograd:

- ``update_pairwise``: logistic ranking loss on (winner, loser) output
  slots — the GRPO-style relative signal (delegation router).
- ``update_regression``: weighted MSE toward a target output vector —
  reward-weighted behavioral cloning (Claustrum modulator).

Weights are persistable via ``state_dict``/``load_state_dict`` (plain
lists, JSON-friendly). No torch: numpy keeps the dependency footprint
small; any future multimodal component can make its own framework choice.
"""

from __future__ import annotations

import numpy as np


class TwoLayerMLP:
    """input -> ReLU(hidden) -> linear output, online SGD."""

    def __init__(
        self,
        input_dim: int,
        hidden_dim: int,
        output_dim: int,
        *,
        lr: float = 0.01,
        seed: int = 7,
        zero_output_init: bool = False,
    ):
        rng = np.random.default_rng(seed)
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.output_dim = output_dim
        self.lr = lr
        scale1 = np.sqrt(2.0 / input_dim)
        self.w1 = rng.normal(0.0, scale1, (input_dim, hidden_dim))
        self.b1 = np.zeros(hidden_dim)
        if zero_output_init:
            # Cold start emits exactly zero — lets callers define "zero
            # output = neutral behavior" (e.g. modulation factor 1.0).
            self.w2 = np.zeros((hidden_dim, output_dim))
        else:
            scale2 = np.sqrt(2.0 / hidden_dim)
            self.w2 = rng.normal(0.0, scale2, (hidden_dim, output_dim))
        self.b2 = np.zeros(output_dim)

    # ── Inference ────────────────────────────────────────────────

    def forward(self, x: np.ndarray) -> np.ndarray:
        y, _ = self._forward_cached(x)
        return y

    def _forward_cached(self, x: np.ndarray):
        x = np.asarray(x, dtype=float)
        z1 = x @ self.w1 + self.b1
        h = np.maximum(z1, 0.0)
        y = h @ self.w2 + self.b2
        return y, (x, z1, h)

    # ── Learning ─────────────────────────────────────────────────

    def _apply_output_grad(self, cache, dy: np.ndarray) -> None:
        """One SGD step given dLoss/dOutput."""
        x, z1, h = cache
        dw2 = np.outer(h, dy)
        db2 = dy
        dh = self.w2 @ dy
        dz1 = dh * (z1 > 0)
        dw1 = np.outer(x, dz1)
        db1 = dz1
        self.w2 -= self.lr * dw2
        self.b2 -= self.lr * db2
        self.w1 -= self.lr * dw1
        self.b1 -= self.lr * db1

    def update_pairwise(
        self, x: np.ndarray, winner: int, loser: int
    ) -> float:
        """Logistic ranking step: raise score[winner] above score[loser].

        Returns the loss before the update.
        """
        y, cache = self._forward_cached(x)
        margin = y[winner] - y[loser]
        p = 1.0 / (1.0 + np.exp(-margin))
        loss = -np.log(max(p, 1e-12))
        dy = np.zeros(self.output_dim)
        dy[winner] = -(1.0 - p)
        dy[loser] = (1.0 - p)
        self._apply_output_grad(cache, dy)
        return float(loss)

    def update_regression(
        self,
        x: np.ndarray,
        target: np.ndarray,
        weight: float = 1.0,
        *,
        output_mask: np.ndarray | None = None,
    ) -> float:
        """Weighted MSE step toward selected outputs of a target vector.

        ``output_mask=None`` preserves the original full-vector regression.
        When a mask is supplied, both the loss and its normalization cover
        only selected outputs. Unselected output columns and biases therefore
        receive exactly zero direct gradient.
        """
        target = np.asarray(target, dtype=float)
        expected_shape = (self.output_dim,)
        if target.shape != expected_shape:
            raise ValueError(
                "regression target must have shape "
                f"{expected_shape}, got {target.shape}"
            )

        mask: np.ndarray | None = None
        selected_count = self.output_dim
        if output_mask is not None:
            mask = np.asarray(output_mask, dtype=bool)
            if mask.shape != expected_shape:
                raise ValueError(
                    "regression output_mask must have shape "
                    f"{expected_shape}, got {mask.shape}"
                )
            selected_count = int(np.count_nonzero(mask))
            if selected_count == 0:
                raise ValueError("regression output_mask must select an output")

        y, cache = self._forward_cached(x)
        diff = y - target
        if mask is None:
            loss = float(weight * np.mean(diff**2))
            dy = weight * 2.0 * diff / selected_count
        else:
            loss = float(weight * np.mean(diff[mask] ** 2))
            dy = np.zeros(self.output_dim)
            dy[mask] = weight * 2.0 * diff[mask] / selected_count
        self._apply_output_grad(cache, dy)
        return loss

    # ── Persistence ──────────────────────────────────────────────

    def state_dict(self) -> dict:
        return {
            "input_dim": self.input_dim,
            "hidden_dim": self.hidden_dim,
            "output_dim": self.output_dim,
            "w1": self.w1.tolist(), "b1": self.b1.tolist(),
            "w2": self.w2.tolist(), "b2": self.b2.tolist(),
        }

    def load_state_dict(self, state: dict) -> None:
        assert state["input_dim"] == self.input_dim
        assert state["hidden_dim"] == self.hidden_dim
        assert state["output_dim"] == self.output_dim
        self.w1 = np.asarray(state["w1"], dtype=float)
        self.b1 = np.asarray(state["b1"], dtype=float)
        self.w2 = np.asarray(state["w2"], dtype=float)
        self.b2 = np.asarray(state["b2"], dtype=float)

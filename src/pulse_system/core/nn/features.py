"""Deterministic feature helpers shared by the sideband learning components.

- ``stable_id_embedding``: a fixed pseudo-embedding derived from an id's
  hash — gives every engram a caller-identity vector without training
  (delegation router input) and stays stable across restarts.
- ``random_project``: seeded random projection to a fixed dimension —
  task embeddings differ per provider (mock 256-d, OpenAI 1536-d), the
  MLP input must not. The projection matrix is derived deterministically
  from (in_dim, out_dim, seed), so no state needs persisting.
"""

from __future__ import annotations

import hashlib

import numpy as np

_PROJECTION_CACHE: dict[tuple[int, int, int], np.ndarray] = {}


def stable_id_embedding(identifier: str, dim: int) -> np.ndarray:
    """Unit-norm vector derived from the identifier's SHA-256."""
    digest = hashlib.sha256(identifier.encode()).digest()
    seed = int.from_bytes(digest[:8], "big")
    rng = np.random.default_rng(seed)
    v = rng.normal(0.0, 1.0, dim)
    norm = np.linalg.norm(v)
    return v / norm if norm > 0 else v


def random_project(vector, out_dim: int, *, seed: int = 13) -> np.ndarray:
    """Project a vector of any dimension to out_dim deterministically."""
    v = np.asarray(vector, dtype=float)
    in_dim = v.shape[0]
    if in_dim == out_dim:
        return v
    key = (in_dim, out_dim, seed)
    matrix = _PROJECTION_CACHE.get(key)
    if matrix is None:
        rng = np.random.default_rng(seed + in_dim * 31 + out_dim * 7)
        matrix = rng.normal(0.0, 1.0 / np.sqrt(out_dim), (in_dim, out_dim))
        _PROJECTION_CACHE[key] = matrix
    projected = v @ matrix
    norm = np.linalg.norm(projected)
    return projected / norm if norm > 0 else projected

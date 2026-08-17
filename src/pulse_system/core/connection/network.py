"""Connection network.

Higher-level connection logic on top of Storage:
- STDP learning from co-activation events
- Global decay and pruning
- Connection transfer for engram succession
- Embedding-based connection initialization
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone

from pulse_system.core.types import Connection, ConnectionType
from pulse_system.core.learning_policy import (
    OnlineLearningAudit,
    OnlineLearningChannel,
    OnlineLearningPolicy,
)
from pulse_system.substrate.llm.adapter import LLMAdapter
from pulse_system.substrate.storage.store import Storage


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ConnectionConfig:
    stdp_strength: float = 0.08
    coactivation_window: float = 30.0  # seconds
    stdp_tau: float = 10.0  # STDP time constant for exponential decay
    # LTD (v0.4): when A fires before B, the reverse edge B→A is weakened
    # by ltd_strength * exp(-dt/stdp_tau). Only existing edges are weakened
    # (no negative edges are created); pruning removes edges that decay
    # below prune_threshold. Set to 0.0 to disable.
    ltd_strength: float = 0.04
    decay_rate: float = 0.003
    prune_threshold: float = 0.01
    embedding_threshold: float = 0.3  # min cosine similarity to create connection
    weight_cap: float = 1.0


class ConnectionNetwork:
    """Manages connection topology and STDP learning."""

    def __init__(
        self,
        storage: Storage,
        config: ConnectionConfig | None = None,
        *,
        learning_policy: OnlineLearningPolicy | None = None,
        learning_audit: OnlineLearningAudit | None = None,
    ):
        self._storage = storage
        self._config = config or ConnectionConfig()
        self._learning_policy = learning_policy or OnlineLearningPolicy()
        self._learning_audit = learning_audit or OnlineLearningAudit()

    @property
    def config(self) -> ConnectionConfig:
        return self._config

    def learning_audit_snapshot(self) -> dict:
        return self._learning_audit.snapshot(self._learning_policy)

    def stdp_update(
        self, activations: list[tuple[str, datetime]]
    ) -> list[Connection]:
        """Apply STDP to co-active engram pairs within the time window.

        A fired before B → the causal edge A→B is strengthened (LTP) and
        the anti-causal edge B→A, if it exists, is weakened (LTD). Both
        deltas decay exponentially with the firing gap.

        Args:
            activations: list of (engram_id, pulse_timestamp) sorted by time.

        Returns:
            List of connections that were created, strengthened, or weakened.
        """
        if len(activations) < 2:
            return []

        window = self._config.coactivation_window
        changed: list[Connection] = []

        for i, (id_a, t_a) in enumerate(activations):
            for j in range(i + 1, len(activations)):
                id_b, t_b = activations[j]
                dt = (t_b - t_a).total_seconds()

                if dt > window:
                    break
                if id_a == id_b:
                    continue

                self._learning_audit.record_attempt(
                    OnlineLearningChannel.CONNECTION_STDP
                )
                if not self._learning_policy.allows(
                    OnlineLearningChannel.CONNECTION_STDP
                ):
                    continue

                decay = math.exp(-dt / self._config.stdp_tau)

                # LTP: A fired before B → strengthen A→B
                delta = self._config.stdp_strength * decay
                conn = self._strengthen_or_create(id_a, id_b, delta)
                if conn:
                    changed.append(conn)

                # LTD: weaken the anti-causal edge B→A if it exists
                if self._config.ltd_strength > 0:
                    weakened = self._weaken_existing(
                        id_b, id_a, self._config.ltd_strength * decay
                    )
                    if weakened:
                        changed.append(weakened)

        self._learning_audit.record_applied(
            OnlineLearningChannel.CONNECTION_STDP,
            len(changed),
        )
        return changed

    def decay_and_prune(self) -> tuple[int, int]:
        """Run global decay then prune weak connections.

        Returns:
            (decayed_count, pruned_count)
        """
        channel = OnlineLearningChannel.CONNECTION_DECAY_PRUNE
        self._learning_audit.record_attempt(channel)
        if not self._learning_policy.allows(channel):
            return 0, 0
        decayed = self._storage.decay_all(self._config.decay_rate)
        pruned = self._storage.prune(self._config.prune_threshold)
        self._learning_audit.record_applied(
            channel,
            decayed + pruned,
            decayed=decayed,
            pruned=pruned,
        )
        return decayed, pruned

    def transfer_connections(self, old_id: str, new_id: str) -> None:
        """Transfer all connections from old engram to new (for succession)."""
        self._storage.transfer_connections(old_id, new_id)

    def initialize_from_embeddings(
        self,
        engram_ids: list[str],
        llm: LLMAdapter,
    ) -> list[Connection]:
        """Compute embeddings for engrams' sessions and create connections
        between pairs whose cosine similarity exceeds the threshold.

        Weight is proportional to similarity.
        """
        if len(engram_ids) < 2:
            return []

        # Build text representation per engram from their session
        texts: dict[str, str] = {}
        for eid in engram_ids:
            session = self._storage.get_session(eid)
            combined = " ".join(m.content for m in session)
            if combined.strip():
                texts[eid] = combined

        if len(texts) < 2:
            return []

        # Get embeddings
        embeddings: dict[str, list[float]] = {}
        for eid, text in texts.items():
            result = llm.embed(text)
            embeddings[eid] = result.vector

        # Compare all pairs
        ids = list(embeddings.keys())
        created: list[Connection] = []
        threshold = self._config.embedding_threshold

        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                sim = _cosine_similarity(embeddings[ids[i]], embeddings[ids[j]])
                if sim >= threshold:
                    weight = sim * 0.5  # scale similarity to a moderate initial weight
                    existing = self._storage.get_connection(ids[i], ids[j])
                    if existing is None:
                        conn = self._storage.create_connection(
                            ids[i], ids[j], weight
                        )
                        created.append(conn)
                    # Also create reverse connection
                    existing_rev = self._storage.get_connection(ids[j], ids[i])
                    if existing_rev is None:
                        conn = self._storage.create_connection(
                            ids[j], ids[i], weight
                        )
                        created.append(conn)

        return created

    def get_propagation_targets(
        self, engram_id: str, threshold: float
    ) -> list[Connection]:
        """Get outgoing connections with weight above propagation threshold."""
        return self._storage.get_outgoing(engram_id, min_weight=threshold)

    # ── Internal ─────────────────────────────────────────────────

    def _weaken_existing(
        self, from_id: str, to_id: str, delta: float
    ) -> Connection | None:
        """LTD: reduce an existing edge's weight; never creates edges.

        Weights clamp at 0 in storage; global pruning later removes edges
        below prune_threshold.
        """
        existing = self._storage.get_connection(from_id, to_id)
        if existing is None:
            return None
        self._storage.update_weight(
            from_id,
            to_id,
            existing.weight - delta,
            layer="field",
        )
        return self._storage.get_connection(from_id, to_id)

    def _strengthen_or_create(
        self, from_id: str, to_id: str, delta: float
    ) -> Connection | None:
        existing = self._storage.get_connection(from_id, to_id)
        if existing is not None:
            new_weight = min(existing.weight + delta, self._config.weight_cap)
            self._storage.update_weight(
                from_id,
                to_id,
                new_weight,
                layer="field",
            )
            return self._storage.get_connection(from_id, to_id)
        else:
            # Deliberate: a new edge is seeded flat and `delta` is ignored, so
            # the timing term enters only on reinforcement (branch above). A
            # first co-firing records *that* two engrams fired together, not
            # how tightly; seeding by the first gap would let one arbitrary
            # interval fix the topology before there is evidence that the pair
            # recurs. The demo shows the intended behavior: pass 1 is flat and
            # timing differentiates the columns only on later reinforcement.
            # Changing creation-time weighting requires a versioned option.
            initial_weight = min(
                self._config.stdp_strength * 0.5, self._config.weight_cap
            )
            return self._storage.create_connection(
                from_id,
                to_id,
                initial_weight,
                layer="field",
            )


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)

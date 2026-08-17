"""Delegation MLP (core/delegation/) — learned delegation routing.

Answers "who has handled this kind of task well" for a given caller. It is
separate from the thinking-connection network: the learning signal is result
quality rather than temporal co-activation, and its state is stored in MLP
weights rather than the connections table.

Composition (all shared substrate from core/nn):
- TwoLayerMLP: caller-embedding + projected task-embedding -> slot scores
- SlotIndex: persistent engram↔slot mapping with mask semantics
- GRPO-style pairwise learning from DelegationRecord outcomes
  (adopted > revised > discarded), including canary groups

Exploration–exploitation also accepts offline dream updates from the sleep
engine:
- temperature sampling over masked scores; temperature anneals with the
  amount of learning signal consumed
- canary: when the top-2 sampled probabilities are too close, both
  candidates run (snapshot mode) under one group_id
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import numpy as np

from pulse_system.core.learning_policy import (
    OnlineLearningAudit,
    OnlineLearningChannel,
    OnlineLearningPolicy,
)
from pulse_system.core.nn import (
    SlotIndex,
    TwoLayerMLP,
    random_project,
    stable_id_embedding,
)
from pulse_system.substrate.storage.store import Storage

_logger = logging.getLogger("pulse_system.delegation")

_OUTCOME_RANK = {"adopted": 2, "revised": 1, "discarded": 0}


@dataclass
class RouterConfig:
    max_slots: int = 256
    caller_dim: int = 64
    task_dim: int = 128
    hidden_dim: int = 128
    lr: float = 0.05
    # temperature anneals from hot to cold as pairwise updates accumulate
    temperature_hot: float = 2.0
    temperature_cold: float = 0.3
    anneal_updates: int = 200
    # canary triggers when top-2 sampling probabilities differ by less
    # than this; the threshold itself anneals with experience
    canary_threshold_hot: float = 0.15
    canary_threshold_cold: float = 0.03


@dataclass
class RouteDecision:
    target_id: str | None          # None → no live candidates, create new
    canary_id: str | None          # second candidate when canary triggered
    temperature: float
    scores: dict[str, float]       # live-candidate scores (diagnostics)


class DelegationRouter:
    """Learned router over live engrams for delegation targets."""

    COMPONENT = "delegation_mlp"

    def __init__(
        self,
        storage: Storage,
        *,
        config: RouterConfig | None = None,
        metrics=None,
        learning_policy: OnlineLearningPolicy | None = None,
        learning_audit: OnlineLearningAudit | None = None,
    ):
        self._storage = storage
        self._config = config or RouterConfig()
        self._metrics = metrics
        self._learning_policy = learning_policy or OnlineLearningPolicy()
        self._learning_audit = learning_audit or OnlineLearningAudit()
        self._slots = SlotIndex(storage, self.COMPONENT, self._config.max_slots)
        # Optional caller feature identities are keyed by the persistent slot,
        # not by the ephemeral Engram id.  The default remains the raw id; an
        # explicit composition may bind a stable logical identity without
        # changing durable records.
        self._feature_identity_keys: dict[int, str] = {}
        self._mlp = TwoLayerMLP(
            self._config.caller_dim + self._config.task_dim,
            self._config.hidden_dim,
            self._config.max_slots,
            lr=self._config.lr,
        )
        self._updates_seen = 0
        # Dedup is per *pair*, not per record: a record whose partner hasn't
        # received an outcome yet must stay eligible for future pairing.
        self._learned_pairs: set[tuple[str, str]] = set()
        self._load_state()

    # ── Slot lifecycle (engine/managers call through) ────────────

    def register_engram(
        self,
        engram_id: str,
        *,
        feature_identity_key: str | None = None,
    ) -> None:
        """Register a live Engram and, optionally, its logical caller key.

        ``feature_identity_key`` affects only the caller embedding used by the
        router.  Delegation rows and slot ownership continue to use the real
        Engram id.  Callers that need this explicit composition seam must
        re-register it when constructing a new Router process.
        """
        slot = self._slots.slot_of(engram_id, create=True)
        if slot is None or feature_identity_key is None:
            return
        if not isinstance(feature_identity_key, str) or not feature_identity_key:
            raise ValueError("feature_identity_key must be a non-empty string")
        existing = self._feature_identity_keys.get(slot)
        if existing is not None and existing != feature_identity_key:
            raise ValueError(
                "delegation slot already has a different feature identity key"
            )
        self._feature_identity_keys[slot] = feature_identity_key

    def feature_identity_key_for(self, engram_id: str) -> str:
        """Return the effective caller feature key for one Engram."""
        slot = self._slots.slot_of(engram_id, create=False)
        if slot is None:
            return engram_id
        return self._feature_identity_keys.get(slot, engram_id)

    def mask_engram(self, engram_id: str) -> None:
        slot = self._slots.slot_of(engram_id, create=False)
        if slot is not None:
            self._feature_identity_keys.pop(slot, None)
        self._slots.release(engram_id)

    def reassign_engram(self, old_id: str, new_id: str) -> None:
        # Feature identity is slot-scoped, so succession inherits it together
        # with the learned output slot.
        self._slots.reassign(old_id, new_id)

    # ── Inference ────────────────────────────────────────────────

    def temperature(self) -> float:
        c = self._config
        frac = min(1.0, self._updates_seen / max(1, c.anneal_updates))
        return c.temperature_hot + frac * (c.temperature_cold - c.temperature_hot)

    def canary_threshold(self) -> float:
        c = self._config
        frac = min(1.0, self._updates_seen / max(1, c.anneal_updates))
        return (
            c.canary_threshold_hot
            + frac * (c.canary_threshold_cold - c.canary_threshold_hot)
        )

    def rank(
        self, caller_id: str, task_embedding, candidate_ids: list[str]
    ) -> dict[str, float]:
        """Scores for the given candidates (registering them as needed)."""
        x = self._features(caller_id, task_embedding)
        y = self._mlp.forward(x)
        scores: dict[str, float] = {}
        for cid in candidate_ids:
            slot = self._slots.slot_of(cid, create=True)
            if slot is not None:
                scores[cid] = float(y[slot])
        return scores

    def choose(
        self,
        caller_id: str,
        task_embedding,
        candidate_ids: list[str],
        *,
        rng: np.random.Generator | None = None,
    ) -> RouteDecision:
        """Temperature-sample a target; arm a canary when top-2 are close."""
        scores = self.rank(caller_id, task_embedding, candidate_ids)
        if not scores:
            return RouteDecision(None, None, self.temperature(), {})

        rng = rng or np.random.default_rng()
        ids = list(scores.keys())
        t = self.temperature()
        logits = np.array([scores[i] for i in ids]) / max(t, 1e-6)
        logits -= logits.max()
        probs = np.exp(logits)
        probs /= probs.sum()

        order = np.argsort(probs)[::-1]
        target = ids[int(rng.choice(len(ids), p=probs))]

        canary = None
        if len(ids) >= 2:
            p_sorted = probs[order]
            if float(p_sorted[0] - p_sorted[1]) < self.canary_threshold():
                top1, top2 = ids[int(order[0])], ids[int(order[1])]
                target = top1
                canary = top2
        if self._metrics is not None:
            self._metrics.record(
                "route", caller=caller_id, target=target,
                canary=canary, temperature=round(t, 3),
            )
        return RouteDecision(target, canary, t, scores)

    # ── Learning ─────────────────────────────────────────────────

    def learning_audit_snapshot(self) -> dict:
        return self._learning_audit.snapshot(self._learning_policy)

    def learn_from_history(self) -> int:
        """Consume delegation records with outcomes into pairwise updates.

        Pairs are formed (a) within a canary group_id, and (b) across
        records of the same caller whose outcomes differ. Returns the
        number of pairwise updates applied. Idempotent per record pair
        within one router instance.
        """
        records = [
            r for r in self._storage.list_delegations()
            if r["outcome"] in _OUTCOME_RANK
        ]
        if not records:
            return 0

        groups: list[list[dict]] = []
        # (a) canary groups — the highest-information signal
        by_group: dict[str, list[dict]] = {}
        for r in records:
            if r["group_id"]:
                by_group.setdefault(r["group_id"], []).append(r)
        groups.extend(by_group.values())

        # (b) same-caller cross-time comparisons
        by_caller: dict[str, list[dict]] = {}
        for r in records:
            by_caller.setdefault(r["caller_id"], []).append(r)
        groups.extend(by_caller.values())

        eligible = self._eligible_pairs(groups)
        channel = OnlineLearningChannel.DELEGATION_MLP
        self._learning_audit.record_attempt(channel, len(eligible))
        if not self._learning_policy.allows(channel):
            return 0

        updates = self._apply_pairs(eligible)

        self._updates_seen += updates
        self._learning_audit.record_applied(channel, updates)
        if updates:
            self._save_state()
            if self._metrics is not None:
                self._metrics.record("router_learn", pairwise_updates=updates)
        _logger.debug("router learned %d pairwise updates", updates)
        return updates

    def _eligible_pairs(
        self,
        groups: list[list[dict]],
    ) -> list[tuple[tuple[str, str], dict, dict]]:
        eligible: list[tuple[tuple[str, str], dict, dict]] = []
        seen = set(self._learned_pairs)
        for group in groups:
            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    a, b = group[i], group[j]
                    ra = _OUTCOME_RANK[a["outcome"]]
                    rb = _OUTCOME_RANK[b["outcome"]]
                    if ra == rb or a["target_id"] == b["target_id"]:
                        continue
                    pair_key = tuple(sorted((a["id"], b["id"])))
                    if pair_key in seen:
                        continue
                    seen.add(pair_key)
                    winner, loser = (a, b) if ra > rb else (b, a)
                    eligible.append((pair_key, winner, loser))
        return eligible

    def _apply_pairs(
        self,
        pairs: list[tuple[tuple[str, str], dict, dict]],
    ) -> int:
        updates = 0
        for pair_key, winner, loser in pairs:
            # Preserve the existing consumption rule: a legal pair is marked
            # consumed even if slot capacity prevents a numeric update.
            self._learned_pairs.add(pair_key)
            w_slot = self._slots.slot_of(winner["target_id"], create=True)
            l_slot = self._slots.slot_of(loser["target_id"], create=True)
            if w_slot is None or l_slot is None:
                continue
            x = self._features(
                winner["caller_id"], self._embedding_of(winner)
            )
            self._mlp.update_pairwise(x, w_slot, l_slot)
            updates += 1
        return updates

    # ── Persistence ──────────────────────────────────────────────

    def _save_state(self) -> None:
        """Persist only the field layer; the factory snapshot stays intact."""
        self._storage.save_weight_state(self.COMPONENT, "field", {
            "mlp": self._mlp.state_dict(),
            "updates_seen": self._updates_seen,
            "learned_pairs": sorted(self._learned_pairs),
        })

    def _load_state(self) -> None:
        factory = self._storage.load_weight_state(self.COMPONENT, "factory")
        if factory is None:
            factory = {
                "mlp": self._mlp.state_dict(),
                "updates_seen": 0,
                "learned_pairs": [],
            }
            self._storage.save_weight_state(
                self.COMPONENT, "factory", factory
            )
        elif self._state_matches(factory):
            self._mlp.load_state_dict(factory["mlp"])

        state = self._storage.load_weight_state(self.COMPONENT, "field")
        if state is None:
            # One-time migration from pre-layer databases. Remove the legacy
            # record after copying, otherwise a later field reset would reload
            # the very state it was meant to clear.
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
                "saved delegation MLP shape does not match config; "
                "using the factory layer"
            )
            return
        self._mlp.load_state_dict(state["mlp"])
        self._updates_seen = int(state.get("updates_seen", 0))
        # learned_pairs must reload with the weights — the records those
        # pairs came from are still in storage and would be re-applied.
        self._learned_pairs = {
            tuple(p) for p in state.get("learned_pairs", [])
        }

    def _state_matches(self, state: dict) -> bool:
        mlp_state = state.get("mlp") or {}
        return (
            mlp_state.get("input_dim") == self._mlp.input_dim
            and mlp_state.get("hidden_dim") == self._mlp.hidden_dim
            and mlp_state.get("output_dim") == self._mlp.output_dim
        )

    # ── Internal ─────────────────────────────────────────────────

    def _features(self, caller_id: str, task_embedding) -> np.ndarray:
        c = self._config
        caller_vec = stable_id_embedding(
            self.feature_identity_key_for(caller_id), c.caller_dim
        )
        if task_embedding is None:
            task_vec = np.zeros(c.task_dim)
        else:
            task_vec = random_project(task_embedding, c.task_dim)
        return np.concatenate([caller_vec, task_vec])

    @staticmethod
    def _embedding_of(record: dict):
        raw = record.get("task_embedding")
        if not raw:
            return None
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return None

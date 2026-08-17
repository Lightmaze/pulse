"""Engram ↔ MLP-slot mapping with mask semantics.

The MLPs reserve a fixed number of output/input slots; live engrams map to
slots, everything else is masked. New engrams claim the smallest free slot
(weights there start from scratch), archived engrams release theirs, and a
successor inherits its predecessor's slot — so succession does not lose
learned routing/modulation weights.

The mapping is persisted in storage layer (`component_slots`) keyed by component name,
so restarts keep weights aligned with the same engrams.
"""

from __future__ import annotations

import numpy as np

from pulse_system.substrate.storage.store import Storage


class SlotIndex:
    """Persistent id↔slot mapping for one sideband component."""

    def __init__(self, storage: Storage, component: str, max_slots: int):
        self._storage = storage
        self._component = component
        self._max_slots = max_slots
        self._id_to_slot: dict[str, int] = storage.get_slot_map(component)
        self._slot_to_id: dict[int, str] = {
            v: k for k, v in self._id_to_slot.items()
        }

    @property
    def max_slots(self) -> int:
        return self._max_slots

    def slot_of(self, engram_id: str, *, create: bool = True) -> int | None:
        slot = self._id_to_slot.get(engram_id)
        if slot is not None:
            return slot
        if not create:
            return None
        if len(self._id_to_slot) >= self._max_slots:
            return None  # full — caller falls back (e.g. embedding search)
        slot = self._storage.assign_slot(self._component, engram_id)
        if slot >= self._max_slots:
            # persisted assignment exceeded capacity (shouldn't happen while
            # occupancy < max, but stay defensive)
            self._storage.release_slot(self._component, engram_id)
            return None
        self._id_to_slot[engram_id] = slot
        self._slot_to_id[slot] = engram_id
        return slot

    def id_of(self, slot: int) -> str | None:
        return self._slot_to_id.get(slot)

    def release(self, engram_id: str) -> None:
        slot = self._id_to_slot.pop(engram_id, None)
        if slot is not None:
            self._slot_to_id.pop(slot, None)
            self._storage.release_slot(self._component, engram_id)

    def reassign(self, old_id: str, new_id: str) -> None:
        """Succession: successor inherits the predecessor's slot."""
        slot = self._id_to_slot.pop(old_id, None)
        if slot is None:
            return
        self._slot_to_id[slot] = new_id
        self._id_to_slot[new_id] = slot
        self._storage.reassign_slot(self._component, old_id, new_id)

    def mask(self) -> np.ndarray:
        """Boolean mask over slots: True = live engram."""
        m = np.zeros(self._max_slots, dtype=bool)
        for slot in self._slot_to_id:
            if slot < self._max_slots:
                m[slot] = True
        return m

    def live_ids(self) -> list[str]:
        return list(self._id_to_slot.keys())

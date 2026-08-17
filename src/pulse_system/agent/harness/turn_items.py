"""Bounded, independent turn-item projection for a future Workbench.

The durable ``harness_events`` stream is an append-only observation surface.
It intentionally contains more than one row for a single piece of work:
Pi's projection, the Pulse approval broker, and a terminal adapter can all
report the same tool call.  This module is the read-side index that turns
those rows into one stable item without changing the event store, Runtime, or
Workbench.

The module is deliberately evidence-conservative:

* raw evidence labels are retained on every history entry;
* the item-level evidence label is a normalized lower bound;
* absent or unknown evidence is ``LIVE_GATE_UNVERIFIED`` rather than LIVE;
* an explicit contract label prevents a LIVE claim for the whole item.

No payload body is returned by this projection.  Only bounded metadata and
digests are exposed, so a future UI can consume the result without turning
this read-side index into another secret or prompt store.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


PROTOCOL_VERSION = "harness.turn-items.v1"

CONTRACT = "CONTRACT"
LIVE_GATE_UNVERIFIED = "LIVE_GATE_UNVERIFIED"
LIVE = "LIVE"

DEFAULT_MAX_ITEMS_PER_TURN = 512
DEFAULT_MAX_HISTORY_PER_ITEM = 128
DEFAULT_MAX_REPLAY_ITEMS = 100
DEFAULT_MAX_CONFLICTS = 64
DEFAULT_MAX_SEQUENCE_RECORDS = 4096
MAX_ID_LENGTH = 256
MAX_REASON_LENGTH = 256


class TurnItemError(ValueError):
    """Base error for malformed or unsafe turn-item input."""


class TurnItemCapacityError(TurnItemError):
    """The bounded item index cannot accept another distinct item."""


class TurnItemEvidenceLevel(str, Enum):
    CONTRACT = CONTRACT
    LIVE_GATE_UNVERIFIED = LIVE_GATE_UNVERIFIED
    LIVE = LIVE


class TurnItemState(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNCERTAIN = "uncertain"


_TERMINAL_STATUSES = frozenset(
    {
        TurnItemState.COMPLETED.value,
        TurnItemState.FAILED.value,
        TurnItemState.CANCELLED.value,
        TurnItemState.UNCERTAIN.value,
    }
)
_TERMINAL_KINDS = frozenset(
    {
        "tool_completed",
        "command_completed",
        "turn_terminal",
    }
)
_COMMAND_KINDS = frozenset(
    {
        "command_started",
        "command_output",
        "command_completed",
    }
)
_FILE_KINDS = frozenset({"file_change", "file_changed"})
_APPROVAL_KINDS = frozenset({"approval_requested", "approval_resolved"})
_CONTROL_KINDS = frozenset({"control_requested", "control_resolved"})
_COMMAND_TOOL_NAMES = frozenset({"bash", "shell", "exec", "command", "terminal"})
_CONTRACT_EVIDENCE = frozenset(
    {
        "CONTRACT",
        "CONTRACT_ONLY",
        "FAKE_RPC_CONTRACT",
        "EXPLICIT_MOCK",
        "MOCK",
    }
)
_UNVERIFIED_EVIDENCE = frozenset({LIVE_GATE_UNVERIFIED})


def _enum_value(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value


def _text(value: Any, *, field_name: str, max_length: int = MAX_ID_LENGTH) -> str:
    value = _enum_value(value)
    if not isinstance(value, str) or not value.strip():
        raise TurnItemError(f"{field_name} must be a non-empty string")
    value = value.strip()
    if len(value) > max_length:
        raise TurnItemError(f"{field_name} exceeds the bounded length")
    return value


def _optional_text(value: Any, *, max_length: int = MAX_ID_LENGTH) -> str | None:
    value = _enum_value(value)
    if not isinstance(value, str) or not value.strip():
        return None
    value = value.strip()
    return value[:max_length]


def _canonical(value: Any) -> str:
    def default(item: Any) -> Any:
        item = _enum_value(item)
        if isinstance(item, datetime):
            return item.isoformat()
        return repr(item)

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
            default=default,
        )
    except (TypeError, ValueError) as exc:
        raise TurnItemError("event metadata must be JSON-serializable") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _as_mapping(event: Any) -> dict[str, Any]:
    if isinstance(event, Mapping):
        return dict(event)
    output: dict[str, Any] = {}
    for name in (
        "event_id",
        "turn_id",
        "world_id",
        "engram_id",
        "seq",
        "parent_event_id",
        "kind",
        "phase",
        "source",
        "status",
        "occurred_at",
        "payload_json",
        "payload",
    ):
        value = getattr(event, name, None)
        if value is not None:
            output[name] = value
    if not output:
        raise TurnItemError("event must be a mapping or an event-like object")
    return output


def _mapping_value(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _envelopes(raw: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    """Yield the event and bounded broker/adapter envelopes only."""

    queue: list[tuple[Mapping[str, Any], int]] = [(raw, 0)]
    seen: set[int] = set()
    while queue:
        mapping, depth = queue.pop(0)
        marker = id(mapping)
        if marker in seen:
            continue
        seen.add(marker)
        yield mapping
        if depth >= 3:
            continue
        for key in (
            "payload_json",
            "payload",
            "data",
            "details",
            "result",
            "metadata",
            "action",
        ):
            child = mapping.get(key)
            if isinstance(child, Mapping):
                queue.append((child, depth + 1))


def _all_text(raw: Mapping[str, Any], keys: tuple[str, ...]) -> tuple[str, ...]:
    values: list[str] = []
    for mapping in _envelopes(raw):
        for key in keys:
            value = _optional_text(mapping.get(key))
            if value is not None and value not in values:
                values.append(value)
    return tuple(values)


def _first_text(raw: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
    values = _all_text(raw, keys)
    return values[0] if values else None


def _first_value(raw: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for mapping in _envelopes(raw):
        value = _mapping_value(mapping, *keys)
        if value is not None:
            return value
    return None


def _sequence(value: Any) -> int | None:
    value = _enum_value(value)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return None
    return value


def _normalise_status(value: Any) -> str:
    value = _optional_text(value, max_length=64)
    if value is None:
        return TurnItemState.RUNNING.value
    value = value.casefold()
    return {
        "success": TurnItemState.COMPLETED.value,
        "succeeded": TurnItemState.COMPLETED.value,
        "done": TurnItemState.COMPLETED.value,
        "exited": TurnItemState.COMPLETED.value,
        "failure": TurnItemState.FAILED.value,
        "error": TurnItemState.FAILED.value,
        "aborted": TurnItemState.CANCELLED.value,
        "canceled": TurnItemState.CANCELLED.value,
        "cancel": TurnItemState.CANCELLED.value,
        "unknown": TurnItemState.UNCERTAIN.value,
    }.get(value, value)


def _normalise_phase(raw: Mapping[str, Any], kind: str) -> str:
    phase = _optional_text(_first_value(raw, ("phase",)), max_length=64)
    if phase is not None:
        return phase.casefold()
    if kind in _APPROVAL_KINDS:
        return "approval"
    if kind in _CONTROL_KINDS:
        return "control"
    if kind in _TERMINAL_KINDS:
        return "terminal"
    if kind.endswith("_started") or kind in {"turn_started", "turn_start"}:
        return "start"
    return "stream"


def _normalise_kind(raw: Mapping[str, Any]) -> str:
    value = _optional_text(_first_value(raw, ("kind", "type")), max_length=96)
    return (value or "unknown").casefold()


def _normalise_time(value: Any) -> str:
    value = _enum_value(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        return value.strip()[:96]
    return ""


def _normalise_evidence(raw: Mapping[str, Any]) -> str | None:
    value = _first_text(raw, ("evidence_class", "evidenceClass"))
    if value is None:
        candidate = _first_text(raw, ("evidence",))
        if candidate in _CONTRACT_EVIDENCE | _UNVERIFIED_EVIDENCE or (
            candidate is not None and candidate.startswith("LIVE_")
        ):
            value = candidate
    return value


def _evidence_level(raw: str | None) -> str:
    if raw is None:
        return LIVE_GATE_UNVERIFIED
    upper = raw.upper()
    if upper in _CONTRACT_EVIDENCE or upper == CONTRACT:
        return CONTRACT
    if upper in _UNVERIFIED_EVIDENCE:
        return LIVE_GATE_UNVERIFIED
    if upper == LIVE or upper.startswith("LIVE_"):
        return LIVE
    # Unknown labels must not become a stronger claim than the known gate.
    return LIVE_GATE_UNVERIFIED


def _normalise_item_type(
    raw: Mapping[str, Any],
    *,
    kind: str,
    tool_name: str | None,
) -> str:
    explicit = _first_text(raw, ("item_type", "itemType"))
    if explicit is not None:
        return explicit.casefold()
    if kind in _COMMAND_KINDS or (
        tool_name is not None and tool_name.casefold() in _COMMAND_TOOL_NAMES
    ):
        return "command"
    if kind in _FILE_KINDS:
        return "file_change"
    if kind in _APPROVAL_KINDS:
        return "tool_call"
    if kind in _CONTROL_KINDS:
        return "control"
    if kind.startswith("subagent"):
        return "subagent"
    if kind in _TERMINAL_KINDS or kind.startswith("tool_"):
        return "tool_call"
    if kind.startswith("text") or kind.endswith("_delta"):
        return "assistant"
    return "event"


def _is_terminal(raw: Mapping[str, Any], *, kind: str, status: str) -> bool:
    explicit = _first_value(raw, ("item_terminal", "itemTerminal"))
    if isinstance(explicit, bool) and explicit:
        return kind not in _APPROVAL_KINDS | _CONTROL_KINDS
    if kind in _TERMINAL_KINDS:
        return True
    if kind in _FILE_KINDS or kind.startswith("subagent"):
        return status in _TERMINAL_STATUSES
    return False


@dataclass(frozen=True, slots=True)
class _NormalisedEvent:
    turn_id: str
    world_id: str
    engram_id: str
    event_id: str
    event_key: str
    event_digest: str
    seq: int | None
    kind: str
    phase: str
    source: str
    status: str
    occurred_at: str
    tool_call_id: str | None
    explicit_item_id: str | None
    tool_name: str | None
    item_type: str
    evidence_class: str | None
    evidence_level: str
    terminal: bool
    local_conflict_fields: tuple[str, ...] = ()


def _normalise_event(event: Any) -> _NormalisedEvent:
    raw = _as_mapping(event)
    turn_id = _text(
        _mapping_value(raw, "turn_id", "turnId"),
        field_name="turn_id",
    )
    world_id = _text(
        _mapping_value(raw, "world_id", "worldId"),
        field_name="world_id",
    )
    engram_id = _text(
        _mapping_value(raw, "engram_id", "engramId"),
        field_name="engram_id",
    )
    kind = _normalise_kind(raw)
    phase = _normalise_phase(raw, kind)
    source = (
        _optional_text(_first_value(raw, ("source",)), max_length=64) or "unknown"
    ).casefold()
    status = _normalise_status(_first_value(raw, ("status",)))
    occurred_at = _normalise_time(_first_value(raw, ("occurred_at", "occurredAt")))
    seq = _sequence(_mapping_value(raw, "seq", "sequence"))
    tool_values = _all_text(
        raw,
        (
            "toolCallId",
            "tool_call_id",
            "callId",
            "call_id",
            "action_request_id",
            "actionRequestId",
            "request_id",
            "requestId",
        ),
    )
    explicit_values = _all_text(raw, ("item_id", "itemId"))
    tool_call_id = tool_values[0] if tool_values else None
    explicit_item_id = explicit_values[0] if explicit_values else None
    tool_name_values = _all_text(raw, ("tool_name", "toolName", "name"))
    tool_name = tool_name_values[0] if tool_name_values else None
    item_type = _normalise_item_type(raw, kind=kind, tool_name=tool_name)
    evidence_class = _normalise_evidence(raw)
    evidence_level = _evidence_level(evidence_class)
    terminal = _is_terminal(raw, kind=kind, status=status)

    local_conflicts: list[str] = []
    if len(tool_values) > 1:
        local_conflicts.append("tool_call_id_conflict")
    if len(explicit_values) > 1:
        local_conflicts.append("item_id_conflict")
    if len(tool_name_values) > 1:
        local_conflicts.append("tool_name_conflict")

    payload = _mapping_value(raw, "payload_json", "payload", "data")
    event_id = _optional_text(
        _mapping_value(raw, "event_id", "eventId"),
        max_length=MAX_ID_LENGTH,
    )
    material = {
        "turn_id": turn_id,
        "world_id": world_id,
        "engram_id": engram_id,
        "seq": seq,
        "kind": kind,
        "phase": phase,
        "source": source,
        "status": status,
        "occurred_at": occurred_at,
        "tool_call_id": tool_call_id,
        "explicit_item_id": explicit_item_id,
        "tool_name": tool_name,
        "item_type": item_type,
        "evidence_class": evidence_class,
        "terminal": terminal,
        "payload_digest": _digest(payload),
    }
    event_digest = _digest(material)
    if event_id is None:
        event_id = f"fingerprint:{event_digest[:40]}"
    event_key = f"id:{event_id}" if not event_id.startswith("fingerprint:") else event_id
    return _NormalisedEvent(
        turn_id=turn_id,
        world_id=world_id,
        engram_id=engram_id,
        event_id=event_id,
        event_key=event_key,
        event_digest=event_digest,
        seq=seq,
        kind=kind,
        phase=phase,
        source=source,
        status=status,
        occurred_at=occurred_at,
        tool_call_id=tool_call_id,
        explicit_item_id=explicit_item_id,
        tool_name=tool_name,
        item_type=item_type,
        evidence_class=evidence_class,
        evidence_level=evidence_level,
        terminal=terminal,
        local_conflict_fields=tuple(local_conflicts),
    )


def _stable_item_id(turn_id: str, identity_kind: str, identity_value: str) -> str:
    material = f"{PROTOCOL_VERSION}\x00{turn_id}\x00{identity_kind}\x00{identity_value}"
    return f"turn_item_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:32]}"


@dataclass(frozen=True, slots=True)
class TurnItemGap:
    turn_id: str
    from_seq: int
    to_seq: int
    reason: str = "pruned_or_missing"
    item_id: str | None = None

    def __post_init__(self) -> None:
        _text(self.turn_id, field_name="turn_id")
        if self.from_seq < 1 or self.to_seq < self.from_seq:
            raise TurnItemError("turn-item gap sequence range is invalid")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise TurnItemError("turn-item gap reason must be non-empty")

    def to_dict(self) -> dict[str, Any]:
        return {
            "turn_id": self.turn_id,
            "from_seq": self.from_seq,
            "to_seq": self.to_seq,
            "reason": self.reason,
            "item_id": self.item_id,
        }

    to_wire = to_dict


@dataclass(frozen=True, slots=True)
class TurnItemConflict:
    turn_id: str
    code: str
    detail: str
    revision: int
    event_id: str | None = None
    item_id: str | None = None
    seq: int | None = None
    related_event_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "turn_id": self.turn_id,
            "code": self.code,
            "detail": self.detail,
            "revision": self.revision,
            "event_id": self.event_id,
            "item_id": self.item_id,
            "seq": self.seq,
            "related_event_id": self.related_event_id,
        }

    to_wire = to_dict


@dataclass(frozen=True, slots=True)
class TurnItemHistory:
    revision: int
    event_id: str
    seq: int | None
    kind: str
    phase: str
    source: str
    status: str
    occurred_at: str
    evidence_class: str | None
    evidence_level: str
    event_digest: str
    terminal: bool
    late: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "event_id": self.event_id,
            "seq": self.seq,
            "kind": self.kind,
            "phase": self.phase,
            "source": self.source,
            "status": self.status,
            "occurred_at": self.occurred_at,
            "evidence_class": self.evidence_class,
            "evidence_level": self.evidence_level,
            "event_digest": self.event_digest,
            "terminal": self.terminal,
            "late": self.late,
        }

    to_wire = to_dict


@dataclass(frozen=True, slots=True)
class TurnItem:
    item_id: str
    turn_id: str
    world_id: str
    engram_id: str
    item_type: str
    tool_call_id: str | None
    tool_name: str | None
    state: str
    terminal: bool
    terminal_event_id: str | None
    terminal_revision: int | None
    phase: str
    phase_history: tuple[str, ...]
    history: tuple[TurnItemHistory, ...]
    history_total: int
    history_truncated: bool
    late_event_count: int
    evidence_class: str
    evidence_classes: tuple[str, ...]
    evidence_level: str
    conflicts: tuple[TurnItemConflict, ...]
    gaps: tuple[TurnItemGap, ...]
    first_seq: int | None
    last_seq: int | None
    revision: int

    @property
    def has_gap(self) -> bool:
        return bool(self.gaps)

    def to_dict(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "turn_id": self.turn_id,
            "world_id": self.world_id,
            "engram_id": self.engram_id,
            "item_type": self.item_type,
            "tool_call_id": self.tool_call_id,
            "tool_name": self.tool_name,
            "state": self.state,
            "terminal": self.terminal,
            "terminal_event_id": self.terminal_event_id,
            "terminal_revision": self.terminal_revision,
            "phase": self.phase,
            "phase_history": list(self.phase_history),
            "history": [entry.to_dict() for entry in self.history],
            "history_total": self.history_total,
            "history_truncated": self.history_truncated,
            "late_event_count": self.late_event_count,
            "evidence_class": self.evidence_class,
            "evidence_classes": list(self.evidence_classes),
            "evidence_level": self.evidence_level,
            "conflicts": [conflict.to_dict() for conflict in self.conflicts],
            "gaps": [gap.to_dict() for gap in self.gaps],
            "has_gap": self.has_gap,
            "first_seq": self.first_seq,
            "last_seq": self.last_seq,
            "revision": self.revision,
        }

    to_wire = to_dict


@dataclass(frozen=True, slots=True)
class TurnItemUpdate:
    accepted: bool
    duplicate: bool
    item_id: str | None
    revision: int
    late: bool
    conflicts: tuple[TurnItemConflict, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "duplicate": self.duplicate,
            "item_id": self.item_id,
            "revision": self.revision,
            "late": self.late,
            "conflicts": [conflict.to_dict() for conflict in self.conflicts],
        }

    to_wire = to_dict


@dataclass(frozen=True, slots=True)
class TurnItemReplay:
    turn_id: str
    items: tuple[TurnItem, ...]
    next_revision: int
    has_more: bool
    turn_known: bool
    gaps: tuple[TurnItemGap, ...] = ()
    conflicts: tuple[TurnItemConflict, ...] = ()
    bounded: bool = True
    truncated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "protocol_version": PROTOCOL_VERSION,
            "turn_id": self.turn_id,
            "items": [item.to_dict() for item in self.items],
            "next_revision": self.next_revision,
            "has_more": self.has_more,
            "turn_known": self.turn_known,
            "gaps": [gap.to_dict() for gap in self.gaps],
            "conflicts": [conflict.to_dict() for conflict in self.conflicts],
            "bounded": self.bounded,
            "truncated": self.truncated,
        }

    to_wire = to_dict


@dataclass
class _MutableItem:
    item_id: str
    turn_id: str
    world_id: str
    engram_id: str
    item_type: str
    tool_call_id: str | None
    tool_name: str | None
    first_revision: int
    revision: int
    phase: str = "observe"
    state: str = TurnItemState.RUNNING.value
    terminal: bool = False
    terminal_event_id: str | None = None
    terminal_revision: int | None = None
    history: list[TurnItemHistory] = field(default_factory=list)
    history_total: int = 0
    history_truncated: bool = False
    late_event_count: int = 0
    phase_history: list[str] = field(default_factory=list)
    evidence_classes: list[str] = field(default_factory=list)
    evidence_levels: list[str] = field(default_factory=list)
    conflicts: list[TurnItemConflict] = field(default_factory=list)
    first_seq: int | None = None
    last_seq: int | None = None

    def effective_evidence(self) -> str:
        if CONTRACT in self.evidence_levels:
            return CONTRACT
        if LIVE_GATE_UNVERIFIED in self.evidence_levels:
            return LIVE_GATE_UNVERIFIED
        return LIVE

    def add_history(self, entry: TurnItemHistory, *, max_history: int) -> None:
        self.history_total += 1
        self.history.append(entry)
        if len(self.history) > max_history:
            del self.history[: len(self.history) - max_history]
            self.history_truncated = True
        if entry.phase not in self.phase_history:
            self.phase_history.append(entry.phase)
        if entry.evidence_class is not None and entry.evidence_class not in self.evidence_classes:
            self.evidence_classes.append(entry.evidence_class)
        self.evidence_levels.append(entry.evidence_level)
        if len(self.evidence_levels) > max_history:
            del self.evidence_levels[: len(self.evidence_levels) - max_history]
        if entry.seq is not None:
            self.first_seq = entry.seq if self.first_seq is None else min(self.first_seq, entry.seq)
            self.last_seq = entry.seq if self.last_seq is None else max(self.last_seq, entry.seq)

    def snapshot(
        self,
        *,
        gaps: tuple[TurnItemGap, ...],
        max_conflicts: int,
    ) -> TurnItem:
        relevant_gaps = tuple(
            gap
            for gap in gaps
            if gap.item_id == self.item_id
            or (
                self.first_seq is not None
                and self.last_seq is not None
                and self.first_seq < gap.from_seq
                and self.last_seq > gap.to_seq
            )
        )
        return TurnItem(
            item_id=self.item_id,
            turn_id=self.turn_id,
            world_id=self.world_id,
            engram_id=self.engram_id,
            item_type=self.item_type,
            tool_call_id=self.tool_call_id,
            tool_name=self.tool_name,
            state=self.state,
            terminal=self.terminal,
            terminal_event_id=self.terminal_event_id,
            terminal_revision=self.terminal_revision,
            phase=self.phase,
            phase_history=tuple(self.phase_history),
            history=tuple(self.history),
            history_total=self.history_total,
            history_truncated=self.history_truncated,
            late_event_count=self.late_event_count,
            evidence_class=self.effective_evidence(),
            evidence_classes=tuple(self.evidence_classes),
            evidence_level=self.effective_evidence(),
            conflicts=tuple(self.conflicts[-max_conflicts:]),
            gaps=relevant_gaps,
            first_seq=self.first_seq,
            last_seq=self.last_seq,
            revision=self.revision,
        )


@dataclass
class _TurnState:
    turn_id: str
    world_id: str | None = None
    engram_id: str | None = None
    revision: int = 0
    items: dict[str, _MutableItem] = field(default_factory=dict)
    by_tool_call: dict[str, str] = field(default_factory=dict)
    by_explicit_id: dict[str, str] = field(default_factory=dict)
    event_digests: dict[str, str] = field(default_factory=dict)
    conflicted_event_digests: dict[str, str] = field(default_factory=dict)
    sequence_digests: dict[int, str] = field(default_factory=dict)
    highest_seq: int | None = None
    lowest_seq: int | None = None
    sequence_tracking_truncated: bool = False
    gaps: list[TurnItemGap] = field(default_factory=list)
    conflicts: list[TurnItemConflict] = field(default_factory=list)
    conflicts_truncated: bool = False
    replay_input_truncated: bool = False


class TurnItemIndex:
    """Bounded in-memory item index with a Workbench-friendly replay API.

    This is a read-side projection.  It does not claim durability by itself;
    callers should rebuild it from the durable event replay after restart.
    ``HarnessEventPage``-like objects can be passed to :meth:`ingest_page`
    without importing or changing the event module.
    """

    def __init__(
        self,
        *,
        max_items_per_turn: int = DEFAULT_MAX_ITEMS_PER_TURN,
        max_history_per_item: int = DEFAULT_MAX_HISTORY_PER_ITEM,
        max_replay_items: int = DEFAULT_MAX_REPLAY_ITEMS,
        max_conflicts: int = DEFAULT_MAX_CONFLICTS,
        max_sequence_records: int = DEFAULT_MAX_SEQUENCE_RECORDS,
    ) -> None:
        for name, value in (
            ("max_items_per_turn", max_items_per_turn),
            ("max_history_per_item", max_history_per_item),
            ("max_replay_items", max_replay_items),
            ("max_conflicts", max_conflicts),
            ("max_sequence_records", max_sequence_records),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise TurnItemError(f"{name} must be an integer >= 1")
        self.max_items_per_turn = max_items_per_turn
        self.max_history_per_item = max_history_per_item
        self.max_replay_items = max_replay_items
        self.max_conflicts = max_conflicts
        self.max_sequence_records = max_sequence_records
        self._turns: dict[str, _TurnState] = {}

    def _turn(self, turn_id: str) -> _TurnState:
        turn_id = _text(turn_id, field_name="turn_id")
        state = self._turns.get(turn_id)
        if state is None:
            state = _TurnState(turn_id=turn_id)
            self._turns[turn_id] = state
        return state

    def _next_revision(self, state: _TurnState) -> int:
        state.revision += 1
        return state.revision

    def _append_conflict(
        self,
        state: _TurnState,
        event: _NormalisedEvent,
        *,
        code: str,
        detail: str,
        revision: int,
        item_id: str | None = None,
        related_event_id: str | None = None,
    ) -> TurnItemConflict:
        conflict = TurnItemConflict(
            turn_id=state.turn_id,
            code=code,
            detail=detail[:MAX_REASON_LENGTH],
            revision=revision,
            event_id=event.event_id,
            item_id=item_id,
            seq=event.seq,
            related_event_id=related_event_id,
        )
        if len(state.conflicts) >= self.max_conflicts:
            del state.conflicts[0]
            state.conflicts_truncated = True
        state.conflicts.append(conflict)
        if item_id is not None and item_id in state.items:
            item_conflicts = state.items[item_id].conflicts
            if len(item_conflicts) >= self.max_conflicts:
                del item_conflicts[0]
            item_conflicts.append(conflict)
            state.items[item_id].revision = revision
        return conflict

    def _remember_rejected(
        self,
        state: _TurnState,
        event: _NormalisedEvent,
    ) -> None:
        state.conflicted_event_digests[event.event_key] = event.event_digest
        if len(state.conflicted_event_digests) > self.max_sequence_records:
            oldest = next(iter(state.conflicted_event_digests))
            del state.conflicted_event_digests[oldest]

    def _add_gap(
        self,
        state: _TurnState,
        *,
        from_seq: int,
        to_seq: int,
        reason: str,
        item_id: str | None = None,
    ) -> None:
        if from_seq > to_seq:
            return
        candidate = TurnItemGap(
            turn_id=state.turn_id,
            from_seq=from_seq,
            to_seq=to_seq,
            reason=reason[:MAX_REASON_LENGTH],
            item_id=item_id,
        )
        for index, existing in enumerate(state.gaps):
            if existing.reason != candidate.reason or existing.item_id != candidate.item_id:
                continue
            if candidate.to_seq + 1 < existing.from_seq or existing.to_seq + 1 < candidate.from_seq:
                continue
            state.gaps[index] = TurnItemGap(
                turn_id=state.turn_id,
                from_seq=min(existing.from_seq, candidate.from_seq),
                to_seq=max(existing.to_seq, candidate.to_seq),
                reason=existing.reason,
                item_id=existing.item_id,
            )
            return
        state.gaps.append(candidate)
        state.gaps.sort(key=lambda gap: (gap.from_seq, gap.to_seq, gap.reason))

    def _remove_sequence_from_gaps(self, state: _TurnState, seq: int) -> None:
        updated: list[TurnItemGap] = []
        for gap in state.gaps:
            if gap.item_id is not None or not gap.from_seq <= seq <= gap.to_seq:
                updated.append(gap)
                continue
            if gap.from_seq < seq:
                updated.append(
                    TurnItemGap(
                        turn_id=state.turn_id,
                        from_seq=gap.from_seq,
                        to_seq=seq - 1,
                        reason=gap.reason,
                    )
                )
            if seq < gap.to_seq:
                updated.append(
                    TurnItemGap(
                        turn_id=state.turn_id,
                        from_seq=seq + 1,
                        to_seq=gap.to_seq,
                        reason=gap.reason,
                    )
                )
        state.gaps = updated

    def _observe_sequence(self, state: _TurnState, event: _NormalisedEvent) -> None:
        if event.seq is None:
            return
        if event.seq in state.sequence_digests:
            return
        if len(state.sequence_digests) < self.max_sequence_records:
            state.sequence_digests[event.seq] = event.event_digest
        else:
            state.sequence_tracking_truncated = True
        if state.highest_seq is None:
            state.highest_seq = event.seq
            state.lowest_seq = event.seq
            return
        if event.seq > state.highest_seq:
            if not state.sequence_tracking_truncated and event.seq > state.highest_seq + 1:
                self._add_gap(
                    state,
                    from_seq=state.highest_seq + 1,
                    to_seq=event.seq - 1,
                    reason="sequence_missing",
                )
            state.highest_seq = event.seq
        else:
            self._remove_sequence_from_gaps(state, event.seq)
        if state.lowest_seq is None or event.seq < state.lowest_seq:
            state.lowest_seq = event.seq

    def record_gap(
        self,
        turn_id: str,
        from_seq: int,
        to_seq: int,
        *,
        reason: str = "pruned_or_missing",
        item_id: str | None = None,
    ) -> None:
        state = self._turn(turn_id)
        if isinstance(from_seq, bool) or not isinstance(from_seq, int):
            raise TurnItemError("from_seq must be an integer")
        if isinstance(to_seq, bool) or not isinstance(to_seq, int):
            raise TurnItemError("to_seq must be an integer")
        self._add_gap(
            state,
            from_seq=from_seq,
            to_seq=to_seq,
            reason=reason,
            item_id=item_id,
        )

    def _resolve_item(
        self,
        state: _TurnState,
        event: _NormalisedEvent,
    ) -> tuple[str | None, str | None, str | None]:
        tool_item_id = (
            state.by_tool_call.get(event.tool_call_id)
            if event.tool_call_id is not None
            else None
        )
        explicit_item_id = (
            state.by_explicit_id.get(event.explicit_item_id)
            if event.explicit_item_id is not None
            else None
        )
        if tool_item_id is not None and explicit_item_id is not None and tool_item_id != explicit_item_id:
            return None, tool_item_id, "item_identity_conflict"
        if tool_item_id is not None:
            return tool_item_id, tool_item_id, None
        if explicit_item_id is not None:
            existing = state.items.get(explicit_item_id)
            if existing is not None and existing.tool_call_id not in {None, event.tool_call_id}:
                return None, explicit_item_id, "item_identity_conflict"
            return explicit_item_id, explicit_item_id, None
        if event.tool_call_id is not None:
            return (
                _stable_item_id(state.turn_id, "tool_call", event.tool_call_id),
                None,
                None,
            )
        if event.explicit_item_id is not None:
            return (
                _stable_item_id(state.turn_id, "explicit", event.explicit_item_id),
                None,
                None,
            )
        return _stable_item_id(state.turn_id, "event", event.event_id), None, None

    def _ingest_normalised(self, event: _NormalisedEvent) -> TurnItemUpdate:
        state = self._turn(event.turn_id)
        known = state.event_digests.get(event.event_key)
        if known is not None:
            if known == event.event_digest:
                item_id, _, _ = self._resolve_item(state, event)
                return TurnItemUpdate(True, True, item_id, state.revision, False)
            revision = self._next_revision(state)
            conflict = self._append_conflict(
                state,
                event,
                code="event_id_conflict",
                detail="the same event identity was projected with different facts",
                revision=revision,
            )
            return TurnItemUpdate(False, False, None, revision, False, (conflict,))
        conflicted = state.conflicted_event_digests.get(event.event_key)
        if conflicted == event.event_digest:
            return TurnItemUpdate(False, True, None, state.revision, False)

        if state.world_id is None:
            state.world_id = event.world_id
            state.engram_id = event.engram_id
        elif state.world_id != event.world_id or state.engram_id != event.engram_id:
            revision = self._next_revision(state)
            conflict = self._append_conflict(
                state,
                event,
                code="turn_scope_conflict",
                detail="world_id or engram_id changed inside one turn projection",
                revision=revision,
            )
            self._remember_rejected(state, event)
            return TurnItemUpdate(False, False, None, revision, False, (conflict,))

        item_id, related_item_id, identity_error = self._resolve_item(state, event)
        if identity_error is not None:
            revision = self._next_revision(state)
            conflict = self._append_conflict(
                state,
                event,
                code=identity_error,
                detail="one event binds the same turn to two different item identities",
                revision=revision,
                item_id=related_item_id,
            )
            self._remember_rejected(state, event)
            return TurnItemUpdate(False, False, related_item_id, revision, False, (conflict,))
        assert item_id is not None
        item = state.items.get(item_id)
        if item is None and len(state.items) >= self.max_items_per_turn:
            revision = self._next_revision(state)
            conflict = self._append_conflict(
                state,
                event,
                code="item_capacity_exhausted",
                detail="the bounded turn-item index rejected a new item",
                revision=revision,
                item_id=item_id,
            )
            self._remember_rejected(state, event)
            return TurnItemUpdate(False, False, item_id, revision, False, (conflict,))

        sequence_digest = (
            state.sequence_digests.get(event.seq) if event.seq is not None else None
        )
        if sequence_digest is not None:
            revision = self._next_revision(state)
            conflict = self._append_conflict(
                state,
                event,
                code="sequence_conflict",
                detail="the same turn sequence was projected more than once",
                revision=revision,
                item_id=item_id,
            )
            self._remember_rejected(state, event)
            return TurnItemUpdate(False, False, item_id, revision, False, (conflict,))

        revision = self._next_revision(state)
        if item is None:
            item = _MutableItem(
                item_id=item_id,
                turn_id=event.turn_id,
                world_id=event.world_id,
                engram_id=event.engram_id,
                item_type=event.item_type,
                tool_call_id=event.tool_call_id,
                tool_name=event.tool_name,
                first_revision=revision,
                revision=revision,
            )
            state.items[item_id] = item
        else:
            item.revision = revision

        if event.tool_call_id is not None:
            prior = state.by_tool_call.get(event.tool_call_id)
            if prior is None:
                state.by_tool_call[event.tool_call_id] = item_id
        if event.explicit_item_id is not None:
            prior = state.by_explicit_id.get(event.explicit_item_id)
            if prior is None:
                state.by_explicit_id[event.explicit_item_id] = item_id

        local_conflicts: list[TurnItemConflict] = []
        for code in event.local_conflict_fields:
            local_conflicts.append(
                self._append_conflict(
                    state,
                    event,
                    code=code,
                    detail="one event envelope contains conflicting identity metadata",
                    revision=revision,
                    item_id=item_id,
                )
            )
        if item.tool_call_id not in {None, event.tool_call_id}:
            local_conflicts.append(
                self._append_conflict(
                    state,
                    event,
                    code="tool_call_id_conflict",
                    detail="an item received a different tool call identity",
                    revision=revision,
                    item_id=item_id,
                )
            )
        if item.tool_name not in {None, event.tool_name}:
            local_conflicts.append(
                self._append_conflict(
                    state,
                    event,
                    code="tool_name_conflict",
                    detail="one stable tool item received different tool names",
                    revision=revision,
                    item_id=item_id,
                )
            )
        if item.item_type not in {"event", event.item_type} and event.item_type != "event":
            local_conflicts.append(
                self._append_conflict(
                    state,
                    event,
                    code="item_type_conflict",
                    detail="one stable item received different item types",
                    revision=revision,
                    item_id=item_id,
                )
            )

        late = item.terminal
        history = TurnItemHistory(
            revision=revision,
            event_id=event.event_id,
            seq=event.seq,
            kind=event.kind,
            phase=event.phase,
            source=event.source,
            status=event.status,
            occurred_at=event.occurred_at,
            evidence_class=event.evidence_class,
            evidence_level=event.evidence_level,
            event_digest=event.event_digest,
            terminal=event.terminal,
            late=late,
        )
        item.add_history(history, max_history=self.max_history_per_item)
        if not late:
            item.phase = event.phase
            if event.terminal:
                item.terminal = True
                item.terminal_event_id = event.event_id
                item.terminal_revision = revision
                item.state = (
                    event.status
                    if event.status in _TERMINAL_STATUSES
                    else TurnItemState.COMPLETED.value
                )
        else:
            item.late_event_count += 1
            local_conflicts.append(
                self._append_conflict(
                    state,
                    event,
                    code="late_event_after_terminal",
                    detail="a late event was retained in history but could not reopen the terminal item",
                    revision=revision,
                    item_id=item_id,
                    related_event_id=item.terminal_event_id,
                )
            )

        state.event_digests[event.event_key] = event.event_digest
        if event.seq is not None:
            self._observe_sequence(state, event)
        return TurnItemUpdate(True, False, item_id, revision, late, tuple(local_conflicts))

    def ingest(self, event: Any) -> TurnItemUpdate:
        """Ingest one event-like object and return the bounded update result."""

        return self._ingest_normalised(_normalise_event(event))

    def ingest_many(
        self,
        events: Iterable[Any],
        *,
        sort_by_sequence: bool = False,
    ) -> tuple[TurnItemUpdate, ...]:
        normalised = [_normalise_event(event) for event in events]
        if sort_by_sequence:
            normalised.sort(key=_event_sort_key)
        return tuple(self._ingest_normalised(event) for event in normalised)

    def ingest_page(self, page: Any) -> tuple[TurnItemUpdate, ...]:
        """Ingest a bounded ``HarnessEventPage``-like object.

        The page may be the real event page or a plain mapping.  Events are
        ordered by sequence before ingestion, while the returned replay still
        exposes ``truncated`` if the input page exceeded the configured cap.
        """

        if isinstance(page, Mapping):
            turn_id = _text(page.get("turn_id"), field_name="turn_id")
            events = list(page.get("events", ()))
            gaps = list(page.get("gaps", ()))
        else:
            turn_id = _text(getattr(page, "turn_id", None), field_name="turn_id")
            events = list(getattr(page, "events", ()))
            gaps = list(getattr(page, "gaps", ()))
        state = self._turn(turn_id)
        for gap in gaps:
            if isinstance(gap, Mapping):
                from_seq = gap.get("from_seq", gap.get("fromSeq"))
                to_seq = gap.get("to_seq", gap.get("toSeq"))
                reason = gap.get("reason", "pruned_or_missing")
                item_id = gap.get("item_id", gap.get("itemId"))
            else:
                from_seq = getattr(gap, "from_seq", None)
                to_seq = getattr(gap, "to_seq", None)
                reason = getattr(gap, "reason", "pruned_or_missing")
                item_id = getattr(gap, "item_id", None)
            if not isinstance(from_seq, int) or not isinstance(to_seq, int):
                raise TurnItemError("page gap sequence values must be integers")
            self._add_gap(
                state,
                from_seq=from_seq,
                to_seq=to_seq,
                reason=str(reason),
                item_id=_optional_text(item_id),
            )
        normalised = [_normalise_event(event) for event in events]
        normalised.sort(key=_event_sort_key)
        # ``max_replay_items`` bounds the Workbench response, not the number
        # of rows a caller may feed from one already-bounded event page.  A
        # separate sequence-record cap keeps this ingestion path bounded while
        # still allowing several deltas for one item to be folded together.
        if len(normalised) > self.max_sequence_records:
            state.replay_input_truncated = True
            normalised = normalised[: self.max_sequence_records]
        return tuple(self._ingest_normalised(event) for event in normalised)

    def snapshot(self, turn_id: str, item_id: str) -> TurnItem | None:
        state = self._turns.get(_text(turn_id, field_name="turn_id"))
        if state is None:
            return None
        item = state.items.get(_text(item_id, field_name="item_id"))
        if item is None:
            return None
        return item.snapshot(
            gaps=tuple(state.gaps),
            max_conflicts=self.max_conflicts,
        )

    def replay(
        self,
        turn_id: str,
        *,
        after_revision: int = 0,
        limit: int | None = None,
    ) -> TurnItemReplay:
        turn_id = _text(turn_id, field_name="turn_id")
        if isinstance(after_revision, bool) or not isinstance(after_revision, int) or after_revision < 0:
            raise TurnItemError("after_revision must be an integer >= 0")
        if limit is None:
            limit = self.max_replay_items
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise TurnItemError("limit must be an integer >= 1")
        limit = min(limit, self.max_replay_items)
        state = self._turns.get(turn_id)
        if state is None:
            return TurnItemReplay(
                turn_id=turn_id,
                items=(),
                next_revision=after_revision,
                has_more=False,
                turn_known=False,
            )
        candidates = sorted(
            (item for item in state.items.values() if item.revision > after_revision),
            key=lambda item: (item.revision, item.item_id),
        )
        selected = candidates[:limit]
        next_revision = after_revision
        if selected:
            next_revision = selected[-1].revision
        conflicts = tuple(
            conflict
            for conflict in state.conflicts
            if conflict.revision > after_revision
        )
        truncated = (
            len(candidates) > len(selected)
            or len(conflicts) > self.max_conflicts
            or state.sequence_tracking_truncated
            or state.replay_input_truncated
            or any(item.history_truncated for item in state.items.values())
        )
        return TurnItemReplay(
            turn_id=turn_id,
            items=tuple(
                item.snapshot(
                    gaps=tuple(state.gaps),
                    max_conflicts=self.max_conflicts,
                )
                for item in selected
            ),
            next_revision=next_revision,
            has_more=len(candidates) > len(selected),
            turn_known=True,
            gaps=tuple(state.gaps),
            conflicts=conflicts[-self.max_conflicts :],
            bounded=True,
            truncated=truncated,
        )

    def clear(self, turn_id: str | None = None) -> None:
        """Clear the read-side projection; durable event rows are untouched."""

        if turn_id is None:
            self._turns.clear()
            return
        self._turns.pop(_text(turn_id, field_name="turn_id"), None)


def _event_sort_key(event: _NormalisedEvent) -> tuple[int, int, str, str]:
    return (
        0 if event.seq is not None else 1,
        event.seq if event.seq is not None else 0,
        event.occurred_at,
        event.event_id,
    )


class TurnItemProjector(TurnItemIndex):
    """Named facade reserved for future Runtime/Workbench integration."""


def project_turn_items(
    events: Iterable[Any],
    *,
    turn_id: str,
    gaps: Iterable[Any] = (),
    max_items_per_turn: int = DEFAULT_MAX_ITEMS_PER_TURN,
    max_history_per_item: int = DEFAULT_MAX_HISTORY_PER_ITEM,
    max_replay_items: int = DEFAULT_MAX_REPLAY_ITEMS,
) -> TurnItemReplay:
    """Build one bounded projection from a replayable event iterable."""

    projector = TurnItemProjector(
        max_items_per_turn=max_items_per_turn,
        max_history_per_item=max_history_per_item,
        max_replay_items=max_replay_items,
    )
    for gap in gaps:
        if isinstance(gap, Mapping):
            projector.record_gap(
                turn_id,
                gap.get("from_seq", gap.get("fromSeq")),
                gap.get("to_seq", gap.get("toSeq")),
                reason=str(gap.get("reason", "pruned_or_missing")),
                item_id=_optional_text(gap.get("item_id", gap.get("itemId"))),
            )
        else:
            projector.record_gap(
                turn_id,
                getattr(gap, "from_seq"),
                getattr(gap, "to_seq"),
                reason=str(getattr(gap, "reason", "pruned_or_missing")),
                item_id=_optional_text(getattr(gap, "item_id", None)),
            )
    projector.ingest_many(events, sort_by_sequence=True)
    return projector.replay(turn_id)


__all__ = [
    "CONTRACT",
    "DEFAULT_MAX_CONFLICTS",
    "DEFAULT_MAX_HISTORY_PER_ITEM",
    "DEFAULT_MAX_ITEMS_PER_TURN",
    "DEFAULT_MAX_REPLAY_ITEMS",
    "LIVE",
    "LIVE_GATE_UNVERIFIED",
    "PROTOCOL_VERSION",
    "TurnItem",
    "TurnItemCapacityError",
    "TurnItemConflict",
    "TurnItemError",
    "TurnItemEvidenceLevel",
    "TurnItemGap",
    "TurnItemHistory",
    "TurnItemIndex",
    "TurnItemProjector",
    "TurnItemReplay",
    "TurnItemState",
    "TurnItemUpdate",
    "project_turn_items",
]

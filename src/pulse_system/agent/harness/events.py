"""Canonical Harness event projection and durable replay contracts.

This module deliberately sits beside the Pi RPC adapter instead of inside it.
Pi JSONL remains the transcript source of truth and the causal ledger remains
the settlement source of truth.  The objects here are a bounded, redacted
projection for observation and replay.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Mapping, TYPE_CHECKING

from .security import (
    CONTRACT_ONLY,
    LIVE_GATE_UNVERIFIED,
    LIVE_OS_RESTRICTED,
    LIVE_WORKSPACE_CHECKPOINTED,
)

if TYPE_CHECKING:
    from pulse_system.core.runtime.publication import RuntimeRecoveryPermit
    from pulse_system.substrate.storage import Storage


PROTOCOL_VERSION = "harness.v1"
DEFAULT_MAX_EVENTS_PER_TURN = 512
DEFAULT_MAX_PAYLOAD_BYTES = 64 * 1024
DEFAULT_MAX_STREAM_CHUNK_BYTES = 16 * 1024
DEFAULT_MAX_TOTAL_ROWS = 10_000
DEFAULT_MAX_TOTAL_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_PAGE_SIZE = 500
MAX_PAYLOAD_DEPTH = 32
MAX_STRING_BYTES = DEFAULT_MAX_STREAM_CHUNK_BYTES


class HarnessEventKind(str, Enum):
    TURN_STARTED = "turn_started"
    TEXT_DELTA = "text_delta"
    REASONING_DELTA = "reasoning_delta"
    TOOL_STARTED = "tool_started"
    TOOL_PROGRESS = "tool_progress"
    TOOL_COMPLETED = "tool_completed"
    COMMAND_STARTED = "command_started"
    COMMAND_OUTPUT = "command_output"
    COMMAND_COMPLETED = "command_completed"
    FILE_CHANGE = "file_change"
    USAGE = "usage"
    APPROVAL_REQUESTED = "approval_requested"
    APPROVAL_RESOLVED = "approval_resolved"
    CONTROL_REQUESTED = "control_requested"
    CONTROL_RESOLVED = "control_resolved"
    SUBAGENT_ACTIVITY = "subagent_activity"
    TURN_TERMINAL = "turn_terminal"
    WARNING = "warning"
    EVENT_GAP = "event_gap"


class HarnessEventPhase(str, Enum):
    OBSERVE = "observe"
    START = "start"
    STREAM = "stream"
    APPROVAL = "approval"
    CONTROL = "control"
    TERMINAL = "terminal"
    RECOVERY = "recovery"


class HarnessEventSource(str, Enum):
    PI_RPC = "pi_rpc"
    PULSE_CONTROL = "pulse_control"
    TERMINAL = "terminal"
    POLICY = "policy"
    TASK_SUBAGENT = "task_subagent"
    RECOVERY = "recovery"


class HarnessEventStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNCERTAIN = "uncertain"
    REDACTED = "redacted"


PROTECTED_EVENT_KINDS = frozenset(
    {
        HarnessEventKind.TURN_TERMINAL.value,
        HarnessEventKind.CONTROL_RESOLVED.value,
        HarnessEventKind.APPROVAL_RESOLVED.value,
        HarnessEventKind.EVENT_GAP.value,
    }
)

# E0 terminal projections are the durable explanation for an external effect.
# Ordinary Pi stream traffic may be pruned, but it must never evict the
# terminal event that the operation ledger names as its canonical winner.
HARD_PROTECTED_CONTROL_SOURCES = frozenset(
    {
        HarnessEventSource.PULSE_CONTROL.value,
        HarnessEventSource.POLICY.value,
        HarnessEventSource.RECOVERY.value,
    }
)
HARD_PROTECTED_CONTROL_PHASES = frozenset(
    {
        HarnessEventPhase.TERMINAL.value,
        HarnessEventPhase.RECOVERY.value,
    }
)


class HarnessEventError(ValueError):
    """Base error for invalid or unsafe event projection input."""


class HarnessEventConflictError(HarnessEventError):
    """The same turn/sequence was used for a different event."""

    def __init__(self, turn_id: str, seq: int, detail: str = "sequence conflict"):
        self.turn_id = turn_id
        self.seq = seq
        super().__init__(f"{detail}: turn_id={turn_id!r}, seq={seq}")


class HarnessEventCapacityError(HarnessEventError):
    """A hard event quota cannot be met without deleting protected evidence."""


class HarnessEventSequenceError(HarnessEventError):
    """A caller attempted to reuse a pruned or invalid sequence number."""


class _UnknownKind(str):
    pass


def _require_id(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HarnessEventError(f"{field_name} must be a non-empty string")
    return value


def _coerce_enum(value: object, enum_type: type[Enum], field_name: str) -> Enum:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        allowed = ", ".join(member.value for member in enum_type)
        raise HarnessEventError(f"{field_name} must be one of: {allowed}") from exc


def _coerce_kind(value: object) -> tuple[HarnessEventKind, str | None]:
    if isinstance(value, HarnessEventKind):
        return value, None
    try:
        return HarnessEventKind(value), None
    except (TypeError, ValueError):
        if not isinstance(value, str) or not value.strip():
            raise HarnessEventError("kind must be a non-empty string")
        return HarnessEventKind.WARNING, value


def _utc(value: datetime | None, field_name: str) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None or value.utcoffset() is None:
        raise HarnessEventError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _limit(value: object, field_name: str, *, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise HarnessEventError(f"{field_name} must be an integer >= {minimum}")
    return value


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise HarnessEventError("payload must be JSON-serializable") from exc


def _clip_utf8(value: str, max_bytes: int) -> tuple[str, bool]:
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value, False
    marker = "…[TRUNCATED]"
    marker_bytes = len(marker.encode("utf-8"))
    if max_bytes <= marker_bytes:
        return encoded[:max_bytes].decode("utf-8", errors="ignore"), True
    prefix = encoded[: max_bytes - marker_bytes].decode("utf-8", errors="ignore")
    return prefix + marker, True


_SECRET_KEY_RE = re.compile(
    r"(?:api[_-]?key|access[_-]?token|refresh[_-]?token|"
    r"client[_-]?secret|password|passwd|secret|authorization|cookie|"
    r"credential|private[_-]?key|session[_-]?key)",
    re.IGNORECASE,
)
_SECRET_EXACT_KEYS = frozenset({"token", "auth_token", "api_token"})
_PROMPT_KEYS = frozenset(
    {
        "prompt",
        "raw_prompt",
        "full_prompt",
        "system_prompt",
        "developer_prompt",
        "user_prompt",
        "input",
        "input_message",
        "instructions",
        "steer_message",
    }
)
_PATH_KEYS = frozenset(
    {"path", "file", "filename", "cwd", "working_directory", "workspace"}
)
_SECRET_VALUE_RE = re.compile(
    r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]+"
    r"|\b(?:api[_-]?key|access[_-]?token|refresh[_-]?token|password|"
    r"passwd|secret|authorization|credential)\s*[:=]\s*"
    r"[\"']?[^,\s\"']+"
    r"|\b(?:sk|ghp|github_pat)_[A-Za-z0-9_-]{12,}"
)
_WINDOWS_PATH_RE = re.compile(
    r"(?<![A-Za-z0-9_])(?:[A-Za-z]:[\\/]|\\\\)"
    r"[^\s\"'<>;,|()\[\]{}]+"
)
_POSIX_PATH_RE = re.compile(
    r"(?<![A-Za-z0-9_:])/(?:[^/\s\"'<>;,|()\[\]{}]+/)+"
    r"[^\s\"'<>;,|()\[\]{}]*"
)
_PEM_RE = re.compile(
    r"-----BEGIN [^-]+-----.*?-----END [^-]+-----",
    re.IGNORECASE | re.DOTALL,
)


def _redact_string(value: str) -> tuple[str, bool]:
    redacted = value
    redacted = _PEM_RE.sub("[REDACTED_SECRET]", redacted)
    redacted = _SECRET_VALUE_RE.sub("[REDACTED_SECRET]", redacted)
    redacted = _WINDOWS_PATH_RE.sub("[REDACTED_PATH]", redacted)
    redacted = _POSIX_PATH_RE.sub("[REDACTED_PATH]", redacted)
    return redacted, redacted != value


def _sanitize_payload(
    value: Any,
    *,
    depth: int = 0,
    key_name: str | None = None,
) -> tuple[Any, bool, bool]:
    """Return JSON-safe payload, redacted flag, and truncated flag.

    Key-based handling covers prompt/credential fields.  Value-based handling
    catches credentials and absolute paths even when a provider chooses an
    unexpected key.  Unsupported Python objects are rejected rather than
    persisted through ``repr``.
    """

    if depth > MAX_PAYLOAD_DEPTH:
        return "[TRUNCATED_DEPTH]", False, True

    normalized_key = key_name.casefold().replace("-", "_") if key_name else ""
    if (
        normalized_key in _PROMPT_KEYS
        or normalized_key in _SECRET_EXACT_KEYS
        or _SECRET_KEY_RE.search(normalized_key)
    ):
        return "[REDACTED]", True, False

    if normalized_key == "message":
        if isinstance(value, str):
            return "[REDACTED]", True, False
        if isinstance(value, Mapping):
            role = value.get("role")
            if role in {"user", "system", "developer", "tool"}:
                safe_role = str(role)
                return {"role": safe_role, "content": "[REDACTED]"}, True, False

    if normalized_key in _PATH_KEYS and isinstance(value, str):
        safe_path, redacted = _redact_string(value)
        clipped, clipped_flag = _clip_utf8(safe_path, MAX_STRING_BYTES)
        return clipped, redacted, clipped_flag

    if isinstance(value, str):
        redacted_value, redacted = _redact_string(value)
        clipped_value, truncated = _clip_utf8(redacted_value, MAX_STRING_BYTES)
        return clipped_value, redacted, truncated

    if value is None or isinstance(value, (bool, int)):
        return value, False, False
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise HarnessEventError("payload contains a non-finite float")
        return value, False, False
    if isinstance(value, Enum):
        return value.value, False, False
    if isinstance(value, datetime):
        return _utc(value, "payload datetime").isoformat(), False, False

    if isinstance(value, Mapping):
        output: dict[str, Any] = {}
        redacted = False
        truncated = False
        for raw_key, raw_value in value.items():
            if not isinstance(raw_key, str):
                raise HarnessEventError("payload object keys must be strings")
            child, child_redacted, child_truncated = _sanitize_payload(
                raw_value,
                depth=depth + 1,
                key_name=raw_key,
            )
            output[raw_key] = child
            redacted = redacted or child_redacted
            truncated = truncated or child_truncated
        return output, redacted, truncated

    if isinstance(value, (list, tuple)):
        output_list: list[Any] = []
        redacted = False
        truncated = False
        for item in value:
            child, child_redacted, child_truncated = _sanitize_payload(
                item,
                depth=depth + 1,
            )
            output_list.append(child)
            redacted = redacted or child_redacted
            truncated = truncated or child_truncated
        return output_list, redacted, truncated

    raise HarnessEventError(
        f"payload contains unsupported value type {type(value).__name__}"
    )


def _clip_structure(value: Any, max_string_bytes: int) -> tuple[Any, bool]:
    """Make a second, deterministic size pass without changing redaction."""

    if isinstance(value, str):
        return _clip_utf8(value, max_string_bytes)
    if isinstance(value, list):
        clipped_items: list[Any] = []
        truncated = False
        for item in value:
            clipped, item_truncated = _clip_structure(item, max_string_bytes)
            clipped_items.append(clipped)
            truncated = truncated or item_truncated
        return clipped_items, truncated
    if isinstance(value, dict):
        clipped_dict: dict[str, Any] = {}
        truncated = False
        for key, item in value.items():
            clipped, item_truncated = _clip_structure(item, max_string_bytes)
            clipped_dict[key] = clipped
            truncated = truncated or item_truncated
        return clipped_dict, truncated
    return value, False


def _fit_payload(value: Any, max_payload_bytes: int, digest: str) -> tuple[Any, bool]:
    candidate = value
    for string_limit in (
        MAX_STRING_BYTES,
        8 * 1024,
        4 * 1024,
        1024,
        256,
        64,
    ):
        candidate, clipped = _clip_structure(value, string_limit)
        if len(_canonical_json(candidate).encode("utf-8")) <= max_payload_bytes:
            return candidate, clipped

    if isinstance(value, dict):
        preview: dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= 32:
                break
            preview[key], _ = _clip_structure(item, 128)
        candidate = {
            "_truncated": True,
            "_payload_digest": digest,
            "preview": preview,
        }
        if len(_canonical_json(candidate).encode("utf-8")) <= max_payload_bytes:
            return candidate, True

    return _minimal_truncated_payload(max_payload_bytes, digest), True


def _minimal_truncated_payload(max_payload_bytes: int, digest: str) -> Any:
    candidates: tuple[Any, ...] = (
        {"_truncated": True, "_payload_digest": digest},
        {"_truncated": True},
        None,
        0,
    )
    for candidate in candidates:
        if len(_canonical_json(candidate).encode("utf-8")) <= max_payload_bytes:
            return candidate
    return 0


def prepare_payload(
    payload: Any,
    *,
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
) -> tuple[Any, str, int, bool, bool, str]:
    """Redact and bound a payload before it is passed to durable storage.

    The digest is calculated over the fully redacted, pre-size-cap JSON.  It
    is therefore useful for idempotency without retaining the omitted bytes.
    """

    max_payload_bytes = _limit(
        max_payload_bytes,
        "max_payload_bytes",
        minimum=1,
    )
    safe_payload, redacted, truncated = _sanitize_payload(payload)
    full_json = _canonical_json(safe_payload)
    digest = hashlib.sha256(full_json.encode("utf-8")).hexdigest()
    bounded_payload, size_truncated = _fit_payload(
        safe_payload,
        max_payload_bytes,
        digest,
    )
    truncated = truncated or size_truncated
    payload_json = _canonical_json(bounded_payload)
    payload_bytes = len(payload_json.encode("utf-8"))
    if payload_bytes > max_payload_bytes:
        bounded_payload = _minimal_truncated_payload(max_payload_bytes, digest)
        payload_json = _canonical_json(bounded_payload)
        payload_bytes = len(payload_json.encode("utf-8"))
        truncated = True
    return bounded_payload, payload_json, payload_bytes, digest, redacted, truncated


@dataclass(frozen=True, slots=True)
class HarnessEventDraft:
    turn_id: str
    world_id: str
    engram_id: str
    kind: HarnessEventKind | str
    phase: HarnessEventPhase | str = HarnessEventPhase.OBSERVE
    source: HarnessEventSource | str = HarnessEventSource.PI_RPC
    status: HarnessEventStatus | str = HarnessEventStatus.RUNNING
    occurred_at: datetime | None = None
    payload: Any = field(default_factory=dict)
    parent_event_id: str | None = None
    seq: int | None = None
    event_id: str | None = None
    _unknown_kind: str | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "turn_id", _require_id(self.turn_id, "turn_id"))
        object.__setattr__(self, "world_id", _require_id(self.world_id, "world_id"))
        object.__setattr__(self, "engram_id", _require_id(self.engram_id, "engram_id"))
        kind, unknown_kind = _coerce_kind(self.kind)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "_unknown_kind", unknown_kind)
        object.__setattr__(
            self,
            "phase",
            _coerce_enum(self.phase, HarnessEventPhase, "phase"),
        )
        object.__setattr__(
            self,
            "source",
            _coerce_enum(self.source, HarnessEventSource, "source"),
        )
        object.__setattr__(
            self,
            "status",
            _coerce_enum(self.status, HarnessEventStatus, "status"),
        )
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at, "occurred_at"))
        if self.parent_event_id is not None:
            object.__setattr__(
                self,
                "parent_event_id",
                _require_id(self.parent_event_id, "parent_event_id"),
            )
        if self.event_id is not None:
            object.__setattr__(self, "event_id", _require_id(self.event_id, "event_id"))
        if self.seq is not None:
            if isinstance(self.seq, bool) or not isinstance(self.seq, int) or self.seq < 1:
                raise HarnessEventError("seq must be an integer >= 1")

    def for_sequence(self, seq: int) -> "HarnessEventDraft":
        return replace(self, seq=seq)


@dataclass(frozen=True, slots=True)
class _PreparedHarnessEvent:
    event_id: str
    turn_id: str
    world_id: str
    engram_id: str
    seq: int | None
    parent_event_id: str | None
    kind: HarnessEventKind
    phase: HarnessEventPhase
    source: HarnessEventSource
    status: HarnessEventStatus
    occurred_at: datetime
    payload_json: Any
    payload_json_text: str
    payload_bytes: int
    payload_digest: str
    redacted: bool
    truncated: bool


def _prepare_draft(
    draft: HarnessEventDraft,
    *,
    max_payload_bytes: int,
) -> _PreparedHarnessEvent:
    if not isinstance(draft, HarnessEventDraft):
        raise TypeError("event must be a HarnessEventDraft")
    payload = draft.payload
    if draft._unknown_kind is not None:
        if isinstance(payload, Mapping):
            payload = dict(payload)
            payload.setdefault("_unknown_kind", draft._unknown_kind)
        else:
            payload = {"_unknown_kind": draft._unknown_kind, "value": payload}
    (
        safe_payload,
        payload_json_text,
        payload_bytes,
        payload_digest,
        redacted,
        truncated,
    ) = prepare_payload(payload, max_payload_bytes=max_payload_bytes)
    return _PreparedHarnessEvent(
        event_id=draft.event_id or uuid.uuid4().hex,
        turn_id=draft.turn_id,
        world_id=draft.world_id,
        engram_id=draft.engram_id,
        seq=draft.seq,
        parent_event_id=draft.parent_event_id,
        kind=draft.kind,
        phase=draft.phase,
        source=draft.source,
        status=draft.status,
        occurred_at=draft.occurred_at or datetime.now(timezone.utc),
        payload_json=safe_payload,
        payload_json_text=payload_json_text,
        payload_bytes=payload_bytes,
        payload_digest=payload_digest,
        redacted=redacted,
        truncated=truncated,
    )


@dataclass(frozen=True, slots=True)
class HarnessEvent:
    event_id: str
    turn_id: str
    world_id: str
    engram_id: str
    seq: int
    parent_event_id: str | None
    kind: HarnessEventKind
    phase: HarnessEventPhase
    source: HarnessEventSource
    status: HarnessEventStatus
    occurred_at: datetime
    payload_json: Any
    payload_bytes: int
    payload_digest: str
    redacted: bool
    truncated: bool
    _payload_json_text: str = field(default="", repr=False, compare=False)

    @property
    def payload(self) -> Any:
        return self.payload_json

    @property
    def payload_json_text(self) -> str:
        return self._payload_json_text or _canonical_json(self.payload_json)

    @classmethod
    def from_storage_row(cls, row: tuple[Any, ...]) -> "HarnessEvent":
        try:
            payload_json_text = str(row[11])
            payload_json = json.loads(payload_json_text)
            return cls(
                event_id=str(row[0]),
                turn_id=str(row[1]),
                world_id=str(row[2]),
                engram_id=str(row[3]),
                seq=int(row[4]),
                parent_event_id=row[5],
                kind=HarnessEventKind(row[6]),
                phase=HarnessEventPhase(row[7]),
                source=HarnessEventSource(row[8]),
                status=HarnessEventStatus(row[9]),
                occurred_at=_utc(datetime.fromisoformat(row[10]), "occurred_at"),
                payload_json=payload_json,
                payload_bytes=int(row[12]),
                payload_digest=str(row[13]),
                redacted=bool(row[14]),
                truncated=bool(row[15]),
                _payload_json_text=payload_json_text,
            )
        except (IndexError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise HarnessEventError("invalid harness_events storage row") from exc

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "turn_id": self.turn_id,
            "world_id": self.world_id,
            "engram_id": self.engram_id,
            "seq": self.seq,
            "parent_event_id": self.parent_event_id,
            "kind": self.kind.value,
            "phase": self.phase.value,
            "source": self.source.value,
            "status": self.status.value,
            "occurred_at": self.occurred_at.isoformat(),
            "payload_json": self.payload_json,
            "payload_bytes": self.payload_bytes,
            "payload_digest": self.payload_digest,
            "redacted": self.redacted,
            "truncated": self.truncated,
        }


@dataclass(frozen=True, slots=True)
class HarnessEventGap:
    turn_id: str
    from_seq: int
    to_seq: int
    reason: str = "pruned_or_missing"

    def __post_init__(self) -> None:
        _require_id(self.turn_id, "turn_id")
        if self.from_seq < 1 or self.to_seq < self.from_seq:
            raise HarnessEventError("event gap sequence range is invalid")

    def to_dict(self) -> dict[str, Any]:
        return {
            "turn_id": self.turn_id,
            "from_seq": self.from_seq,
            "to_seq": self.to_seq,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class HarnessEventPage:
    turn_id: str
    events: tuple[HarnessEvent, ...]
    next_seq: int
    has_more: bool
    gaps: tuple[HarnessEventGap, ...] = ()
    oldest_seq: int | None = None
    latest_seq: int | None = None
    turn_known: bool = False

    @property
    def gap(self) -> HarnessEventGap | None:
        return self.gaps[0] if self.gaps else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "turn_id": self.turn_id,
            "events": [event.to_dict() for event in self.events],
            "next_seq": self.next_seq,
            "has_more": self.has_more,
            "gaps": [gap.to_dict() for gap in self.gaps],
            "oldest_seq": self.oldest_seq,
            "latest_seq": self.latest_seq,
            "turn_known": self.turn_known,
        }


@dataclass(frozen=True, slots=True)
class HarnessCapacitySnapshot:
    event_rows: int
    event_bytes: int
    retained_turns: int
    last_prune_at: datetime | None
    max_events_per_turn: int = DEFAULT_MAX_EVENTS_PER_TURN
    max_event_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES
    max_event_rows: int = DEFAULT_MAX_TOTAL_ROWS
    max_event_bytes: int = DEFAULT_MAX_TOTAL_BYTES
    max_live_sessions: int | None = None
    active_sessions: int | None = None
    hibernated_sessions: int | None = None
    task_workers: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_rows": self.event_rows,
            "event_bytes": self.event_bytes,
            "retained_turns": self.retained_turns,
            "last_prune_at": (
                self.last_prune_at.isoformat() if self.last_prune_at else None
            ),
            "max_events_per_turn": self.max_events_per_turn,
            "max_event_payload_bytes": self.max_event_payload_bytes,
            "max_event_rows": self.max_event_rows,
            "max_event_bytes": self.max_event_bytes,
            "max_live_sessions": self.max_live_sessions,
            "active_sessions": self.active_sessions,
            "hibernated_sessions": self.hibernated_sessions,
            "task_workers": self.task_workers,
        }


@dataclass(frozen=True, slots=True)
class HarnessPruneResult:
    removed_rows: int
    removed_bytes: int
    retained_terminal_count: int
    last_prune_at: datetime
    capacity: HarnessCapacitySnapshot

    def to_dict(self) -> dict[str, Any]:
        return {
            "removed_rows": self.removed_rows,
            "removed_bytes": self.removed_bytes,
            "retained_terminal_count": self.retained_terminal_count,
            "last_prune_at": self.last_prune_at.isoformat(),
            "capacity": self.capacity.to_dict(),
        }


class HarnessEventStore:
    """Small policy wrapper around :class:`Storage` event CRUD methods."""

    def __init__(
        self,
        storage: "Storage",
        *,
        max_events_per_turn: int = DEFAULT_MAX_EVENTS_PER_TURN,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        max_total_rows: int = DEFAULT_MAX_TOTAL_ROWS,
        max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
        max_page_size: int = DEFAULT_MAX_PAGE_SIZE,
        observer: Callable[[HarnessEvent], None] | None = None,
    ) -> None:
        if observer is not None and not callable(observer):
            raise TypeError("observer must be callable")
        self.storage = storage
        self.observer = observer
        self.max_events_per_turn = _limit(
            max_events_per_turn, "max_events_per_turn"
        )
        self.max_payload_bytes = _limit(
            max_payload_bytes, "max_payload_bytes", minimum=1
        )
        self.max_total_rows = _limit(max_total_rows, "max_total_rows")
        self.max_total_bytes = _limit(max_total_bytes, "max_total_bytes")
        self.max_page_size = _limit(max_page_size, "max_page_size")

    def append(
        self,
        draft: HarnessEventDraft,
        *,
        seq: int | None = None,
    ) -> HarnessEvent:
        if seq is not None:
            if draft.seq is not None and draft.seq != seq:
                raise HarnessEventError("draft seq and append seq disagree")
            draft = draft.for_sequence(seq)
        prepared = _prepare_draft(
            draft,
            max_payload_bytes=self.max_payload_bytes,
        )
        event = self.storage.append_harness_event(
            prepared,
            max_events_per_turn=self.max_events_per_turn,
            max_payload_bytes=self.max_payload_bytes,
            max_total_rows=self.max_total_rows,
            max_total_bytes=self.max_total_bytes,
        )
        if self.observer is not None:
            try:
                self.observer(event)
            except Exception:
                # Observation can never become permission to fail or replay a
                # durable adapter effect.  The event remains control-plane
                # evidence even if the optional observer is unavailable.
                pass
        return event

    def append_terminal_operation(
        self,
        draft: HarnessEventDraft,
        *,
        ledger: Any,
        operation_kind: str,
        operation_id: str,
        expected_epoch: int,
        owner_id: str,
        terminal_state: Any,
    ) -> tuple[HarnessEvent, Any]:
        """Commit the canonical terminal event and E0 winner atomically."""

        if getattr(ledger, "_storage", None) is not self.storage:
            raise HarnessEventError("terminal ledger and event store must share Storage")
        prepared = _prepare_draft(
            draft,
            max_payload_bytes=self.max_payload_bytes,
        )
        event, operation = self.storage.append_harness_terminal_event(
            prepared,
            operation_kind=operation_kind,
            operation_id=operation_id,
            expected_epoch=expected_epoch,
            owner_id=owner_id,
            terminal_state=terminal_state,
            max_events_per_turn=self.max_events_per_turn,
            max_payload_bytes=self.max_payload_bytes,
            max_total_rows=self.max_total_rows,
            max_total_bytes=self.max_total_bytes,
        )
        if self.observer is not None:
            try:
                self.observer(event)
            except Exception:
                pass
        return event, operation

    def recover_terminal_operation(
        self,
        draft: HarnessEventDraft,
        *,
        ledger: Any,
        recovery_permit: RuntimeRecoveryPermit,
        operation_kind: str,
        operation_id: str,
        expected_epoch: int,
        owner_id: str,
        terminal_state: Any,
    ) -> tuple[HarnessEvent, Any]:
        """Commit shutdown recovery without reopening ordinary observation.

        The durable recovery row is evidence for the next Runtime.  It does
        not invoke the live observer: doing so would publish a normal control
        event after this Runtime's ordinary publication generation was
        revoked.
        """

        if getattr(ledger, "_storage", None) is not self.storage:
            raise HarnessEventError("terminal ledger and event store must share Storage")
        prepared = _prepare_draft(
            draft,
            max_payload_bytes=self.max_payload_bytes,
        )
        return self.storage.recover_harness_terminal_event(
            prepared,
            recovery_permit=recovery_permit,
            operation_kind=operation_kind,
            operation_id=operation_id,
            expected_epoch=expected_epoch,
            owner_id=owner_id,
            terminal_state=terminal_state,
            max_events_per_turn=self.max_events_per_turn,
            max_payload_bytes=self.max_payload_bytes,
            max_total_rows=self.max_total_rows,
            max_total_bytes=self.max_total_bytes,
        )

    def replay(
        self,
        turn_id: str,
        *,
        after_seq: int = 0,
        limit: int = DEFAULT_MAX_PAGE_SIZE,
    ) -> HarnessEventPage:
        bounded_limit = min(_limit(limit, "limit"), self.max_page_size)
        return self.storage.replay_harness_events(
            turn_id,
            after_seq=after_seq,
            limit=bounded_limit,
        )

    def summary(self, turn_id: str) -> dict[str, Any] | None:
        """Return a bounded, replay-derived turn summary for the Workbench.

        The event store is the durable source of truth for a turn.  Keeping
        this projection beside ``replay`` gives the control surface a stable
        epoch and cursor without introducing a second turn registry or
        returning any raw Pi payload.  The route still supplies live gateway
        availability; this method only describes retained evidence.
        """

        page = self.replay(turn_id, after_seq=0, limit=self.max_page_size)
        if not page.turn_known or not page.events:
            return None
        first_page = page
        retained_events = list(page.events)
        retained_gaps = list(page.gaps)
        cursor = page.next_seq
        while page.has_more and len(retained_events) < self.max_events_per_turn:
            page = self.replay(
                turn_id,
                after_seq=cursor,
                limit=self.max_page_size,
            )
            if not page.events or page.next_seq <= cursor:
                break
            retained_events.extend(page.events)
            retained_gaps.extend(page.gaps)
            cursor = page.next_seq
        first = retained_events[0]
        epoch: int | None = None
        evidence_class: str | None = None
        usage: dict[str, int | float] = {}
        terminal_event: HarnessEvent | None = None
        for event in retained_events:
            if event.kind is HarnessEventKind.TURN_TERMINAL:
                terminal_event = event
            payload = event.payload
            if not isinstance(payload, Mapping):
                continue
            if epoch is None:
                candidate_epoch = payload.get("epoch")
                if (
                    isinstance(candidate_epoch, int)
                    and not isinstance(candidate_epoch, bool)
                    and candidate_epoch >= 1
                ):
                    epoch = candidate_epoch
            candidate_evidence = payload.get("evidence_class")
            if isinstance(candidate_evidence, str) and candidate_evidence:
                candidate_evidence = candidate_evidence[:64]
                ranks = {
                    "EXPLICIT_MOCK": 1,
                    "FAKE_RPC_CONTRACT": 1,
                    CONTRACT_ONLY: 1,
                    LIVE_GATE_UNVERIFIED: 2,
                    "LIVE_PI_PROVIDER": 3,
                    LIVE_WORKSPACE_CHECKPOINTED: 4,
                    LIVE_OS_RESTRICTED: 5,
                }
                if evidence_class is None or ranks.get(candidate_evidence, 0) > ranks.get(
                    evidence_class, 0
                ):
                    evidence_class = candidate_evidence
            if event.kind is HarnessEventKind.USAGE:
                candidate_usage = payload.get("usage", payload)
                if isinstance(candidate_usage, Mapping):
                    for key in (
                        "input_tokens",
                        "output_tokens",
                        "cache_read_tokens",
                        "cache_write_tokens",
                        "total_tokens",
                    ):
                        value = candidate_usage.get(key)
                        if isinstance(value, (int, float)) and not isinstance(value, bool):
                            usage[key] = value
        terminal = terminal_event is not None
        operation_recovery: list[dict[str, Any]] = []
        try:
            # Import lazily to keep the event schema independent while still
            # preventing retained TURN_TERMINAL rows from hiding an adapter
            # operation that requires restart reconciliation.
            from .operations import (
                HarnessOperationLedger,
                OperationRecoveryState,
                OperationTerminalState,
            )

            operations = HarnessOperationLedger(self.storage).list_for_turn(
                turn_id,
                limit=min(self.max_events_per_turn, 500),
            )
            for operation in operations:
                if (
                    not operation.is_terminal
                    or operation.recovery_state is OperationRecoveryState.REQUIRED
                    or operation.terminal_state is OperationTerminalState.UNCERTAIN
                ):
                    operation_recovery.append(
                        {
                            "operation_kind": operation.operation_kind,
                            "operation_id": operation.operation_id,
                            "phase": operation.phase.value,
                            "terminal_state": (
                                None
                                if operation.terminal_state is None
                                else operation.terminal_state.value
                            ),
                            "recovery_state": operation.recovery_state.value,
                        }
                    )
        except Exception:
            # Summary remains readable if a legacy/external Storage adapter
            # lacks E0, but it must not manufacture positive evidence.
            operation_recovery.append(
                {
                    "operation_kind": "unknown",
                    "operation_id": "unknown",
                    "phase": "unknown",
                    "terminal_state": None,
                    "recovery_state": "required",
                }
            )
        recovery_required = bool(retained_gaps or operation_recovery)
        return {
            "turn_id": turn_id,
            "world_id": first.world_id,
            "engram_id": first.engram_id,
            "state": (
                HarnessEventStatus.UNCERTAIN.value
                if recovery_required
                else (
                    terminal_event.status.value
                    if terminal_event is not None
                    else HarnessEventStatus.RUNNING.value
                )
            ),
            "terminal": terminal,
            "epoch": epoch,
            "evidence_class": evidence_class,
            "usage": usage,
            "event_cursor": {
                "first_seq": first_page.oldest_seq,
                "last_seq": first_page.latest_seq,
                "next_seq": (
                    first_page.latest_seq + 1
                    if first_page.latest_seq is not None
                    else 1
                ),
                "has_gap": bool(retained_gaps),
            },
            "recovery_required": recovery_required,
            "operation_recovery": operation_recovery[:64],
        }

    def get(self, event_id: str) -> HarnessEvent | None:
        return self.storage.get_harness_event(event_id)

    def prune(
        self,
        *,
        max_events_per_turn: int | None = None,
        max_total_rows: int | None = None,
        max_total_bytes: int | None = None,
    ) -> HarnessPruneResult:
        return self.storage.prune_harness_events(
            max_events_per_turn=(
                self.max_events_per_turn
                if max_events_per_turn is None
                else _limit(max_events_per_turn, "max_events_per_turn")
            ),
            max_total_rows=(
                self.max_total_rows
                if max_total_rows is None
                else _limit(max_total_rows, "max_total_rows")
            ),
            max_total_bytes=(
                self.max_total_bytes
                if max_total_bytes is None
                else _limit(max_total_bytes, "max_total_bytes")
            ),
        )

    def capacity_snapshot(self) -> HarnessCapacitySnapshot:
        return self.storage.harness_event_capacity(
            max_events_per_turn=self.max_events_per_turn,
            max_event_payload_bytes=self.max_payload_bytes,
            max_total_rows=self.max_total_rows,
            max_total_bytes=self.max_total_bytes,
        )

    capacity = capacity_snapshot


_PI_EVENT_KIND_MAP: dict[str, HarnessEventKind] = {
    "agent_start": HarnessEventKind.TURN_STARTED,
    "turn_start": HarnessEventKind.TURN_STARTED,
    "turn_started": HarnessEventKind.TURN_STARTED,
    "message_start": HarnessEventKind.TEXT_DELTA,
    "message_update": HarnessEventKind.TEXT_DELTA,
    "message_delta": HarnessEventKind.TEXT_DELTA,
    "message_end": HarnessEventKind.TEXT_DELTA,
    "text_delta": HarnessEventKind.TEXT_DELTA,
    "assistant_message_delta": HarnessEventKind.TEXT_DELTA,
    "thinking_delta": HarnessEventKind.REASONING_DELTA,
    "reasoning_delta": HarnessEventKind.REASONING_DELTA,
    "tool_execution_start": HarnessEventKind.TOOL_STARTED,
    "tool_execution_update": HarnessEventKind.TOOL_PROGRESS,
    "tool_execution_progress": HarnessEventKind.TOOL_PROGRESS,
    "tool_execution_end": HarnessEventKind.TOOL_COMPLETED,
    "tool_call_start": HarnessEventKind.TOOL_STARTED,
    "tool_call_update": HarnessEventKind.TOOL_PROGRESS,
    "tool_call_end": HarnessEventKind.TOOL_COMPLETED,
    "toolcall_start": HarnessEventKind.TOOL_STARTED,
    "toolcall_delta": HarnessEventKind.TOOL_PROGRESS,
    "toolcall_end": HarnessEventKind.TOOL_COMPLETED,
    "command_start": HarnessEventKind.COMMAND_STARTED,
    "command_started": HarnessEventKind.COMMAND_STARTED,
    "command_output": HarnessEventKind.COMMAND_OUTPUT,
    "command_update": HarnessEventKind.COMMAND_OUTPUT,
    "command_end": HarnessEventKind.COMMAND_COMPLETED,
    "command_completed": HarnessEventKind.COMMAND_COMPLETED,
    "file_change": HarnessEventKind.FILE_CHANGE,
    "file_change_start": HarnessEventKind.FILE_CHANGE,
    "file_change_end": HarnessEventKind.FILE_CHANGE,
    "usage": HarnessEventKind.USAGE,
    "approval_requested": HarnessEventKind.APPROVAL_REQUESTED,
    "approval_resolved": HarnessEventKind.APPROVAL_RESOLVED,
    "control_requested": HarnessEventKind.CONTROL_REQUESTED,
    "control_resolved": HarnessEventKind.CONTROL_RESOLVED,
    "subagent_activity": HarnessEventKind.SUBAGENT_ACTIVITY,
    "agent_settled": HarnessEventKind.TURN_TERMINAL,
    "turn_end": HarnessEventKind.TURN_TERMINAL,
    "agent_end": HarnessEventKind.TURN_TERMINAL,
    "turn_terminal": HarnessEventKind.TURN_TERMINAL,
}
_PI_COMMAND_TOOL_NAMES = frozenset({"bash", "shell", "exec", "command", "terminal"})
_PI_SAFE_SCALAR_KEYS = frozenset(
    {
        "type",
        "toolName",
        "tool_name",
        "name",
        "toolCallId",
        "tool_call_id",
        "callId",
        "call_id",
        "status",
        "role",
        "phase",
        "stopReason",
        "stop_reason",
        "exitCode",
        "exit_code",
        "signal",
        "durationMs",
        "duration_ms",
        "requestId",
        "request_id",
        "approvalId",
        "approval_id",
        "subagentId",
        "subagent_id",
        "workerId",
        "worker_id",
        "parentEventId",
        "parent_event_id",
    }
)
_PI_TEXT_KEYS = frozenset({"delta", "text", "content", "output", "error", "reason"})
_PI_TOOL_VALUE_KEYS = frozenset(
    {"args", "arguments", "result", "details", "metadata", "usage"}
)


def _raw_pi_type(event_mapping: Mapping[str, Any]) -> str:
    raw_kind = event_mapping.get("type", event_mapping.get("kind"))
    if not isinstance(raw_kind, str) or not raw_kind.strip():
        return "unknown"
    return raw_kind.strip().casefold()


def _mapping_value(event_mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in event_mapping:
            return event_mapping[key]
    return None


def _safe_pi_payload(
    event_mapping: Mapping[str, Any],
    *,
    raw_kind: str,
    canonical_kind: HarnessEventKind,
) -> dict[str, Any]:
    """Select a bounded projection surface before generic redaction.

    In particular, this does not persist the raw mapping, prompt, session
    paths, or provider-specific envelope.  ``prepare_payload`` still applies
    its value-level credential/path rules to each selected value.
    """

    payload: dict[str, Any] = {"pi_kind": raw_kind}
    if canonical_kind is HarnessEventKind.WARNING:
        payload["evidence"] = "unmapped_pi_event"
        return payload

    for key in _PI_SAFE_SCALAR_KEYS:
        value = event_mapping.get(key)
        if isinstance(value, (str, int, float, bool)) or value is None:
            if value is not None:
                payload[key] = value

    for key in _PI_TEXT_KEYS:
        value = event_mapping.get(key)
        if isinstance(value, str):
            payload["text" if key in {"delta", "text", "content"} else key] = value

    for key in _PI_TOOL_VALUE_KEYS:
        if key not in event_mapping:
            continue
        value = event_mapping[key]
        if key == "usage" and isinstance(value, Mapping):
            usage: dict[str, Any] = {}
            for usage_key, usage_value in value.items():
                if isinstance(usage_value, (int, float)) and not isinstance(
                    usage_value, bool
                ):
                    usage[str(usage_key)] = usage_value
            payload["usage"] = usage
        elif key in {"args", "arguments"}:
            payload["arguments"] = value
        elif key == "result":
            payload["result"] = value
        elif key == "details":
            payload["details"] = value
        elif key == "metadata" and isinstance(value, Mapping):
            metadata: dict[str, Any] = {}
            for metadata_key, metadata_value in value.items():
                if metadata_key in _PI_SAFE_SCALAR_KEYS and isinstance(
                    metadata_value, (str, int, float, bool)
                ):
                    metadata[metadata_key] = metadata_value
            payload["metadata"] = metadata

    nested = event_mapping.get("assistantMessageEvent")
    if isinstance(nested, Mapping):
        nested_type = nested.get("type")
        if isinstance(nested_type, str):
            payload["message_event_type"] = nested_type
        for key in ("delta", "text", "content"):
            value = nested.get(key)
            if isinstance(value, str):
                payload["text"] = value
                break

    if canonical_kind is HarnessEventKind.FILE_CHANGE:
        for key in ("path", "file", "diff", "kind", "change"):
            value = event_mapping.get(key)
            if isinstance(value, (str, int, float, bool)) or isinstance(value, Mapping):
                payload[key] = value

    if canonical_kind is HarnessEventKind.TURN_TERMINAL:
        payload["terminal"] = True
        if raw_kind == "agent_settled":
            payload["barrier"] = "agent_settled"

    return payload


def _pi_kind_and_phase(
    event_mapping: Mapping[str, Any],
) -> tuple[HarnessEventKind, HarnessEventPhase, HarnessEventStatus]:
    raw_kind = _raw_pi_type(event_mapping)
    canonical_kind = _PI_EVENT_KIND_MAP.get(raw_kind, HarnessEventKind.WARNING)
    nested_message_event = event_mapping.get("assistantMessageEvent")
    if raw_kind in {"message_update", "message_delta"} and isinstance(
        nested_message_event,
        Mapping,
    ):
        nested_type = nested_message_event.get("type")
        if isinstance(nested_type, str):
            canonical_kind = _PI_EVENT_KIND_MAP.get(
                nested_type.casefold(),
                canonical_kind,
            )

    tool_name = _mapping_value(event_mapping, "toolName", "tool_name", "name")
    if (
        canonical_kind in {
            HarnessEventKind.TOOL_STARTED,
            HarnessEventKind.TOOL_PROGRESS,
            HarnessEventKind.TOOL_COMPLETED,
        }
        and isinstance(tool_name, str)
        and tool_name.casefold() in _PI_COMMAND_TOOL_NAMES
    ):
        canonical_kind = {
            HarnessEventKind.TOOL_STARTED: HarnessEventKind.COMMAND_STARTED,
            HarnessEventKind.TOOL_PROGRESS: HarnessEventKind.COMMAND_OUTPUT,
            HarnessEventKind.TOOL_COMPLETED: HarnessEventKind.COMMAND_COMPLETED,
        }[canonical_kind]

    if canonical_kind is HarnessEventKind.TURN_STARTED:
        phase = HarnessEventPhase.START
        status = HarnessEventStatus.RUNNING
    elif canonical_kind is HarnessEventKind.TURN_TERMINAL:
        phase = HarnessEventPhase.TERMINAL
        raw_status = str(
            _mapping_value(event_mapping, "status", "stopReason", "stop_reason") or ""
        ).casefold()
        if raw_status in {"error", "failed", "failure"} or "error" in event_mapping:
            status = HarnessEventStatus.FAILED
        elif raw_status in {"abort", "aborted", "cancelled", "canceled", "interrupt"}:
            status = HarnessEventStatus.CANCELLED
        elif raw_status in {"uncertain", "unknown"}:
            status = HarnessEventStatus.UNCERTAIN
        else:
            status = HarnessEventStatus.COMPLETED
    elif canonical_kind in {
        HarnessEventKind.APPROVAL_REQUESTED,
        HarnessEventKind.APPROVAL_RESOLVED,
    }:
        phase = HarnessEventPhase.APPROVAL
        status = (
            HarnessEventStatus.RUNNING
            if canonical_kind is HarnessEventKind.APPROVAL_REQUESTED
            else HarnessEventStatus.COMPLETED
        )
    elif canonical_kind in {
        HarnessEventKind.CONTROL_REQUESTED,
        HarnessEventKind.CONTROL_RESOLVED,
    }:
        phase = HarnessEventPhase.CONTROL
        status = (
            HarnessEventStatus.RUNNING
            if canonical_kind is HarnessEventKind.CONTROL_REQUESTED
            else HarnessEventStatus.COMPLETED
        )
    elif canonical_kind is HarnessEventKind.WARNING:
        phase = HarnessEventPhase.OBSERVE
        status = HarnessEventStatus.UNCERTAIN
    else:
        phase = HarnessEventPhase.STREAM
        status = HarnessEventStatus.RUNNING

    return canonical_kind, phase, status


class HarnessEventProjector:
    """Map one Pi RPC event into the redacted durable event contract.

    Stable integration signature used by the Lead wiring:

    ``append_observation(engram_id, turn_id, event_mapping) -> HarnessEvent``

    The projector is constructed with the host's single ``world_id`` and a
    ``HarnessEventStore``.  It never returns or stores the raw Pi mapping; only
    the allowlisted projection produced by ``_safe_pi_payload`` reaches the
    store's redaction and size-cap pipeline.
    """

    def __init__(
        self,
        event_store: HarnessEventStore | "Storage",
        *,
        world_id: str,
    ) -> None:
        if not isinstance(event_store, HarnessEventStore):
            try:
                event_store = HarnessEventStore(event_store)
            except (AttributeError, TypeError) as exc:
                raise TypeError(
                    "event_store must be a HarnessEventStore or Storage"
                ) from exc
        self.event_store = event_store
        self.world_id = _require_id(world_id, "world_id")

    def append_observation(
        self,
        engram_id: str,
        turn_id: str,
        event_mapping: Mapping[str, Any],
    ) -> HarnessEvent:
        engram_id = _require_id(engram_id, "engram_id")
        turn_id = _require_id(turn_id, "turn_id")
        if not isinstance(event_mapping, Mapping):
            raise TypeError("event_mapping must be a mapping")

        raw_kind = _raw_pi_type(event_mapping)
        canonical_kind, phase, status = _pi_kind_and_phase(event_mapping)
        payload = _safe_pi_payload(
            event_mapping,
            raw_kind=raw_kind,
            canonical_kind=canonical_kind,
        )
        parent_event_id = _mapping_value(
            event_mapping,
            "parentEventId",
            "parent_event_id",
        )
        if not isinstance(parent_event_id, str) or not parent_event_id.strip():
            parent_event_id = None
        raw_seq = event_mapping.get("seq")
        seq = (
            raw_seq
            if isinstance(raw_seq, int)
            and not isinstance(raw_seq, bool)
            and raw_seq >= 1
            else None
        )
        raw_event_id = event_mapping.get("eventId", event_mapping.get("event_id"))
        event_id = raw_event_id if isinstance(raw_event_id, str) and raw_event_id.strip() else None
        draft = HarnessEventDraft(
            turn_id=turn_id,
            world_id=self.world_id,
            engram_id=engram_id,
            seq=seq,
            event_id=event_id,
            kind=canonical_kind,
            phase=phase,
            source=HarnessEventSource.PI_RPC,
            status=status,
            parent_event_id=parent_event_id,
            payload=payload,
        )
        return self.event_store.append(draft)

    def append_turn_started(
        self,
        engram_id: str,
        turn_id: str,
        *,
        evidence: str = "synthetic_pi_acceptance",
    ) -> HarnessEvent:
        return self.event_store.append(
            HarnessEventDraft(
                turn_id=turn_id,
                world_id=self.world_id,
                engram_id=engram_id,
                kind=HarnessEventKind.TURN_STARTED,
                phase=HarnessEventPhase.START,
                source=HarnessEventSource.PI_RPC,
                status=HarnessEventStatus.RUNNING,
                payload={"synthetic": True, "evidence": evidence},
            )
        )

    def append_turn_terminal(
        self,
        engram_id: str,
        turn_id: str,
        *,
        status: HarnessEventStatus | str = HarnessEventStatus.COMPLETED,
        stop_reason: str | None = None,
        evidence: str = "synthetic_terminal_barrier",
    ) -> HarnessEvent:
        status = _coerce_enum(status, HarnessEventStatus, "status")
        payload: dict[str, Any] = {"synthetic": True, "evidence": evidence}
        if stop_reason is not None:
            payload["stop_reason"] = stop_reason
        return self.event_store.append(
            HarnessEventDraft(
                turn_id=turn_id,
                world_id=self.world_id,
                engram_id=engram_id,
                kind=HarnessEventKind.TURN_TERMINAL,
                phase=HarnessEventPhase.TERMINAL,
                source=HarnessEventSource.PI_RPC,
                status=status,
                payload=payload,
            )
        )


__all__ = [
    "DEFAULT_MAX_EVENTS_PER_TURN",
    "DEFAULT_MAX_PAGE_SIZE",
    "DEFAULT_MAX_PAYLOAD_BYTES",
    "DEFAULT_MAX_STREAM_CHUNK_BYTES",
    "DEFAULT_MAX_TOTAL_BYTES",
    "DEFAULT_MAX_TOTAL_ROWS",
    "HarnessCapacitySnapshot",
    "HarnessEvent",
    "HarnessEventConflictError",
    "HarnessEventDraft",
    "HarnessEventError",
    "HarnessEventGap",
    "HarnessEventKind",
    "HarnessEventPage",
    "HarnessEventPhase",
    "HarnessEventSequenceError",
    "HarnessEventSource",
    "HarnessEventStatus",
    "HarnessEventStore",
    "HarnessEventCapacityError",
    "HarnessEventProjector",
    "HarnessPruneResult",
    "HARD_PROTECTED_CONTROL_PHASES",
    "HARD_PROTECTED_CONTROL_SOURCES",
    "PROTECTED_EVENT_KINDS",
    "PROTOCOL_VERSION",
    "prepare_payload",
]

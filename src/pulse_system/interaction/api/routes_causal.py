"""Safe causal read, reconciliation, and durable SSE routes.

The causal ledger is the source of truth for a live runtime.  A replay-only
observatory may use the configured SQLite file through a fresh read-only
connection, but this module never constructs a writer and never reads Pi's
session JSONL.  List views and the stream intentionally share the same safe
event serializer; only the explicit detail route includes natural content or
the reconciliation note.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import re
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

from pulse_system.core.causality import (
    CausalLedger,
    CausalTransitionError,
    DendriticIntegration,
    read_causal_amplification,
)
from pulse_system.core.types import (
    CausalEvent,
    CausalEventDomain,
    CausalEventFlow,
    CausalEventKind,
    CausalEventSource,
    CausalEventStatus,
    GenerationTransition,
    GenerationTransitionState,
    HarnessTurn,
    HarnessTurnState,
)

_logger = logging.getLogger("pulse_system.observatory.causal")

_MAX_LIMIT = 500
_STREAM_BATCH = 250
_DEFAULT_POLL_INTERVAL = 0.25

_EVENT_COLUMNS = """
    seq, id, causal_id, parent_event_id, world_id, engram_id, center_id,
    flow, domain, kind, source, status, content, metadata, idempotency_key,
    attempts, created_at, updated_at, started_at, settled_at, resolution,
    resolution_note
"""
_TURN_COLUMNS = """
    id, event_id, engram_id, state, cursor_before, cursor_after,
    input_message_id, prompt_accepted, session_id, session_file, result_event_id,
    error_code, error_phase, started_at, updated_at, settled_at
"""
_GENERATION_COLUMNS = """
    id, causal_id, event_id, predecessor_id, successor_id, state,
    summary_turn_id, error_code, created_at, updated_at, settled_at
"""
_DENDRITIC_INTEGRATION_COLUMNS = """
    id, world_id, formation_engram_id, center_id, aggregate_event_id, delivery_class,
    member_set_sha256, content_sha256, member_count, window_opened_at,
    window_closed_at, created_at
"""
_DENDRITIC_WINDOW_COLUMNS = """
    id, world_id, formation_engram_id, policy_version, event_set_sha256,
    event_count, base_silence_threshold_seconds, base_max_wait_seconds,
    wait_modifier, silence_threshold_seconds, max_wait_seconds,
    window_opened_at, last_input_at, window_closed_at, observed_at,
    observed_event_seq, created_at
"""
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_SOURCE_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}\Z")
_WINDOW_EVIDENCE_DURABLE_V6 = "DURABLE_V6"
_WINDOW_EVIDENCE_LEGACY_V5 = "LEGACY_V5_NO_WINDOW"

_SAFE_METADATA_KEYS = {
    "id",
    "event_id",
    "causal_id",
    "parent_event_id",
    "child_event_id",
    "source_event_id",
    "turn_id",
    "generation_id",
    "summary_turn_id",
    "result_event_id",
    "engram_id",
    "focal_engram_id",
    "center_id",
    "world_id",
    "delegation_id",
    "effect_id",
    "subscription_id",
    "predecessor_id",
    "successor_id",
    "target_id",
    "kind",
    "domain",
    "flow",
    "source",
    "status",
    "state",
    "tool",
    "tool_name",
    "verb",
    "organ",
    "channel",
    "reason",
    "reason_code",
    "error_code",
    "error_phase",
    "allow",
    "allowed",
    "accepted",
    "changed",
    "yielded",
    "refused",
    "count",
    "attempt",
    "attempts",
    "duration_ms",
    "fingerprint",
    "operation",
    "dendritic_delivery_class",
    "dendritic_integration_id",
    "dendritic_integration_version",
    "dendritic_member_count",
    "dendritic_member_set_sha256",
    "dendritic_window_id",
    "depth",
    "priority",
}
_SAFE_METADATA_KEYS.update({"total_count", "duration_ms"})
_FORBIDDEN_METADATA_PARTS = (
    "path",
    "token",
    "capability",
    "gateway",
    "session",
    "input_message",
    "prompt",
    "output",
    "payload",
    "secret",
    "url",
)


class _NoCausalSource(RuntimeError):
    """Raised when an app was mounted without a causal read source."""


class _NoReverseSource(RuntimeError):
    """Raised when a live source has no safe bounded reverse-read path."""


@dataclass(frozen=True)
class _DbRow:
    """Small mapping-like row used by the replay-only read path."""

    values: Mapping[str, Any]

    def __getitem__(self, key: str) -> Any:
        return self.values[key]


def _value(value: Any) -> Any:
    return value.value if hasattr(value, "value") else value


def _time(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _metadata_key_is_safe(key: str) -> bool:
    normalized = key.casefold().replace("-", "_")
    if any(part in normalized for part in _FORBIDDEN_METADATA_PARTS):
        # ID references such as result_event_id are safe identifiers, not
        # payloads.  Keep them only when the complete key is an ID field.
        if not normalized.endswith("_id"):
            return False
    return normalized in _SAFE_METADATA_KEYS


def _safe_metadata(value: Any) -> dict[str, Any]:
    """Project metadata to IDs, enums, booleans, counts, and tool names."""

    def project(item: Any, key: str | None = None) -> Any:
        if isinstance(item, Mapping):
            output: dict[str, Any] = {}
            for raw_key, child in item.items():
                if not isinstance(raw_key, str) or not _metadata_key_is_safe(raw_key):
                    continue
                projected = project(child, raw_key)
                if projected is not None:
                    output[raw_key] = projected
            return output
        if isinstance(item, list):
            projected_list = [project(child, key) for child in item]
            return [child for child in projected_list if child is not None]
        if item is None or isinstance(item, (str, bool, int)):
            return item
        if isinstance(item, float) and item == item and abs(item) != float("inf"):
            return item
        return None

    projected = project(value)
    return projected if isinstance(projected, dict) else {}


def _row_value(item: CausalEvent | Mapping[str, Any] | _DbRow, key: str) -> Any:
    if isinstance(item, _DbRow):
        return item.values.get(key)
    if isinstance(item, Mapping):
        return item.get(key)
    return getattr(item, key, None)


def _safe_event(item: CausalEvent | Mapping[str, Any] | _DbRow, *, detail: bool = False) -> dict[str, Any]:
    """Serialize one event without the list/SSE sensitive fields."""

    result: dict[str, Any] = {
        "seq": _row_value(item, "seq"),
        "id": _row_value(item, "id"),
        "causal_id": _row_value(item, "causal_id"),
        "parent_event_id": _row_value(item, "parent_event_id"),
        "world_id": _row_value(item, "world_id"),
        "engram_id": _row_value(item, "engram_id"),
        "center_id": _row_value(item, "center_id"),
        "flow": _value(_row_value(item, "flow")),
        "domain": _value(_row_value(item, "domain")),
        "kind": _value(_row_value(item, "kind")),
        "source": _value(_row_value(item, "source")),
        "status": _value(_row_value(item, "status")),
        "metadata": _safe_metadata(_row_value(item, "metadata")),
        "attempts": _row_value(item, "attempts"),
        "created_at": _time(_row_value(item, "created_at")),
        "updated_at": _time(_row_value(item, "updated_at")),
        "started_at": _time(_row_value(item, "started_at")),
        "settled_at": _time(_row_value(item, "settled_at")),
        "resolution": _value(_row_value(item, "resolution")),
    }
    if detail:
        result["content"] = _row_value(item, "content")
        result["resolution_note"] = _row_value(item, "resolution_note")
    return result


def _safe_turn(item: HarnessTurn | Mapping[str, Any] | _DbRow) -> dict[str, Any]:
    """Serialize a turn while omitting prompt/session material."""

    return {
        "id": _row_value(item, "id"),
        "event_id": _row_value(item, "event_id"),
        "engram_id": _row_value(item, "engram_id"),
        "state": _value(_row_value(item, "state")),
        "cursor_before": _row_value(item, "cursor_before"),
        "cursor_after": _row_value(item, "cursor_after"),
        "prompt_accepted": _row_value(item, "prompt_accepted"),
        "result_event_id": _row_value(item, "result_event_id"),
        "error_code": _row_value(item, "error_code"),
        "error_phase": _row_value(item, "error_phase"),
        "started_at": _time(_row_value(item, "started_at")),
        "updated_at": _time(_row_value(item, "updated_at")),
        "settled_at": _time(_row_value(item, "settled_at")),
    }


def _safe_generation(
    item: GenerationTransition | Mapping[str, Any] | _DbRow,
) -> dict[str, Any]:
    return {
        "id": _row_value(item, "id"),
        "causal_id": _row_value(item, "causal_id"),
        "event_id": _row_value(item, "event_id"),
        "predecessor_id": _row_value(item, "predecessor_id"),
        "successor_id": _row_value(item, "successor_id"),
        "state": _value(_row_value(item, "state")),
        "summary_turn_id": _row_value(item, "summary_turn_id"),
        "error_code": _row_value(item, "error_code"),
        "created_at": _time(_row_value(item, "created_at")),
        "updated_at": _time(_row_value(item, "updated_at")),
        "settled_at": _time(_row_value(item, "settled_at")),
    }


def _safe_dendritic_integration(item: Any) -> dict[str, Any]:
    """Project immutable convergence provenance without natural content."""

    window_evidence_class = _row_value(item, "window_evidence_class")
    window = _row_value(item, "window")
    projected_window_members: list[dict[str, Any]] = []
    projected_window: dict[str, Any] | None = None
    if window_evidence_class == _WINDOW_EVIDENCE_LEGACY_V5:
        if window is not None:
            raise RuntimeError("legacy dendritic evidence cannot claim a window")
    elif window_evidence_class == _WINDOW_EVIDENCE_DURABLE_V6:
        window_members = _row_value(window, "members")
        window_count = _row_value(window, "event_count")
        window_id = _row_value(window, "id")
        window_digest = _row_value(window, "event_set_sha256")
        if (
            not isinstance(window_members, (list, tuple))
            or type(window_count) is not int
            or not 1 <= window_count <= 500
            or len(window_members) != window_count
            or not isinstance(window_id, str)
            or _HEX64.fullmatch(window_id) is None
            or not isinstance(window_digest, str)
            or _HEX64.fullmatch(window_digest) is None
        ):
            raise RuntimeError("dendritic window evidence drifted")
        for expected_ordinal, member in enumerate(window_members):
            ordinal = _row_value(member, "ordinal")
            event_id = _row_value(member, "event_id")
            event_seq = _row_value(member, "event_seq")
            if (
                ordinal != expected_ordinal
                or not isinstance(event_id, str)
                or not event_id
                or type(event_seq) is not int
                or event_seq < 1
            ):
                raise RuntimeError("dendritic window member evidence drifted")
            projected_window_members.append(
                {
                    "ordinal": ordinal,
                    "event_id": event_id,
                    "event_seq": event_seq,
                    "arrived_at": _time(_row_value(member, "arrived_at")),
                }
            )
        projected_window = {
            "schema_version": _row_value(window, "policy_version"),
            "id": window_id,
            "world_id": _row_value(window, "world_id"),
            "formation_engram_id": _row_value(
                window,
                "formation_engram_id",
            ),
            "event_set_sha256": window_digest,
            "event_count": window_count,
            "base_silence_threshold_seconds": _row_value(
                window,
                "base_silence_threshold_seconds",
            ),
            "base_max_wait_seconds": _row_value(
                window,
                "base_max_wait_seconds",
            ),
            "wait_modifier": _row_value(window, "wait_modifier"),
            "silence_threshold_seconds": _row_value(
                window,
                "silence_threshold_seconds",
            ),
            "max_wait_seconds": _row_value(window, "max_wait_seconds"),
            "window_opened_at": _time(_row_value(window, "window_opened_at")),
            "last_input_at": _time(_row_value(window, "last_input_at")),
            "window_closed_at": _time(_row_value(window, "window_closed_at")),
            "observed_at": _time(_row_value(window, "observed_at")),
            "observed_event_seq": _row_value(window, "observed_event_seq"),
            "created_at": _time(_row_value(window, "created_at")),
            "members": projected_window_members,
        }
    else:
        raise RuntimeError("dendritic window evidence class is unavailable")
    members = _row_value(item, "members")
    if not isinstance(members, (list, tuple)):
        raise RuntimeError("dendritic integration members are unavailable")
    member_count = _row_value(item, "member_count")
    if (
        type(member_count) is not int
        or not 2 <= member_count <= 64
        or len(members) != member_count
    ):
        raise RuntimeError("dendritic integration member count drifted")
    projected_members: list[dict[str, Any]] = []
    for expected_ordinal, member in enumerate(members):
        ordinal = _row_value(member, "ordinal")
        event_id = _row_value(member, "event_id")
        event_seq = _row_value(member, "event_seq")
        causal_id = _row_value(member, "causal_id")
        source_identity = _row_value(member, "source_identity")
        content_sha256 = _row_value(member, "content_sha256")
        if (
            ordinal != expected_ordinal
            or type(event_seq) is not int
            or event_seq < 1
            or not isinstance(event_id, str)
            or not event_id
            or not isinstance(causal_id, str)
            or not causal_id
            or not isinstance(source_identity, str)
            or _SAFE_SOURCE_IDENTITY.fullmatch(source_identity) is None
            or not isinstance(content_sha256, str)
            or _HEX64.fullmatch(content_sha256) is None
        ):
            raise RuntimeError("dendritic integration member evidence drifted")
        projected_members.append(
            {
                "ordinal": ordinal,
                "event_id": event_id,
                "event_seq": event_seq,
                "causal_id": causal_id,
                "source_identity": source_identity,
                "content_sha256": content_sha256,
                "arrived_at": _time(_row_value(member, "arrived_at")),
            }
        )
    member_set_sha256 = _row_value(item, "member_set_sha256")
    content_sha256 = _row_value(item, "content_sha256")
    if (
        not isinstance(member_set_sha256, str)
        or _HEX64.fullmatch(member_set_sha256) is None
        or not isinstance(content_sha256, str)
        or _HEX64.fullmatch(content_sha256) is None
    ):
        raise RuntimeError("dendritic integration digest drifted")
    return {
        "schema_version": "dendritic-integration.v1",
        "id": _row_value(item, "id"),
        "world_id": _row_value(item, "world_id"),
        "formation_engram_id": _row_value(item, "formation_engram_id"),
        "center_id": _row_value(item, "center_id"),
        "aggregate_event_id": _row_value(item, "aggregate_event_id"),
        "delivery_class": _row_value(item, "delivery_class"),
        "member_set_sha256": member_set_sha256,
        "content_sha256": content_sha256,
        "member_count": member_count,
        "window_opened_at": _time(_row_value(item, "window_opened_at")),
        "window_closed_at": _time(_row_value(item, "window_closed_at")),
        "created_at": _time(_row_value(item, "created_at")),
        "members": projected_members,
        "window_evidence_class": window_evidence_class,
        "window": projected_window,
    }


def _assert_replay_dendritic_propagation(
    conn: sqlite3.Connection,
    event: Mapping[str, Any],
    source_identity: str,
) -> None:
    """Independently bind one propagation to its accepted source turn."""

    metadata = event.get("metadata")
    if not isinstance(metadata, Mapping):
        raise RuntimeError("dendritic propagation metadata drifted")
    source_engram_id = metadata.get("source_engram_id")
    depth = metadata.get("depth")
    if (
        event.get("domain") != "pulse"
        or not isinstance(source_engram_id, str)
        or not source_engram_id.strip()
        or source_identity != f"engram:{source_engram_id}"
        or type(depth) is not int
        or depth < 1
        or not isinstance(event.get("parent_event_id"), str)
    ):
        raise RuntimeError("dendritic propagation source metadata drifted")
    parent_row = conn.execute(
        f"SELECT {_EVENT_COLUMNS} FROM causal_events WHERE id = ?",
        (event["parent_event_id"],),
    ).fetchone()
    if parent_row is None:
        raise RuntimeError("dendritic propagation source result is missing")
    parent = dict(parent_row)
    source_root_id = parent.get("parent_event_id")
    if not isinstance(source_root_id, str):
        raise RuntimeError("dendritic propagation source root is missing")
    source_root_row = conn.execute(
        f"SELECT {_EVENT_COLUMNS} FROM causal_events WHERE id = ?",
        (source_root_id,),
    ).fetchone()
    if source_root_row is None:
        raise RuntimeError("dendritic propagation source root is missing")
    source_root = dict(source_root_row)
    if (
        parent.get("world_id") != event.get("world_id")
        or parent.get("causal_id") != event.get("causal_id")
        or parent.get("center_id") != event.get("center_id")
        or parent.get("engram_id") != source_engram_id
        or parent.get("kind") != "assistant_result"
        or parent.get("domain") != "harness"
        or parent.get("source") != "self"
        or parent.get("status") != "settled"
        or not isinstance(parent.get("settled_at"), str)
        or not isinstance(parent.get("created_at"), str)
        or not isinstance(event.get("created_at"), str)
        or parent["created_at"] > event["created_at"]
        or source_root.get("world_id") != event.get("world_id")
        or source_root.get("causal_id") != event.get("causal_id")
        or source_root.get("center_id") != event.get("center_id")
        or source_root.get("engram_id") != source_engram_id
        or source_root.get("status") != "settled"
        or not isinstance(source_root.get("settled_at"), str)
    ):
        raise RuntimeError("dendritic propagation source lineage drifted")
    turns = conn.execute(
        "SELECT event_id, engram_id, state, prompt_accepted "
        "FROM harness_turns WHERE result_event_id = ?",
        (parent.get("id"),),
    ).fetchall()
    if (
        len(turns) != 1
        or turns[0][0] != source_root_id
        or turns[0][1] != source_engram_id
        or turns[0][2] != "settled"
        or turns[0][3] != 1
    ):
        raise RuntimeError("dendritic propagation source turn drifted")


def _replay_time(value: Any, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise RuntimeError(f"dendritic {field} timestamp drifted")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(f"dendritic {field} timestamp drifted") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RuntimeError(f"dendritic {field} timestamp drifted")
    return parsed


def _replay_dendritic_window_shape(event: Mapping[str, Any]) -> bool:
    metadata = event.get("metadata")
    if not isinstance(metadata, Mapping):
        return False
    kind = event.get("kind")
    source = event.get("source")
    flow = event.get("flow")
    content = event.get("content")
    if (
        not isinstance(event.get("engram_id"), str)
        or not event["engram_id"]
        or metadata.get("dendritic_integration_version") == 1
        or (
            isinstance(metadata.get("generation_id"), str)
            and metadata.get("generation_stage") == "summary"
        )
        or kind
        in {
            "tool_call",
            "tool_result",
            "habitat_action",
            "habitat_consequence",
            "generation_transition",
            "assistant_result",
        }
        or flow == "spectrum"
    ):
        return False
    if flow in {"content", "tunnel"} and not (
        isinstance(content, str) and content.strip()
    ):
        return False
    if kind == "propagation" or source == "propagation":
        return bool(
            kind == "propagation"
            and source == "propagation"
            and flow == "content"
            and isinstance(event.get("parent_event_id"), str)
            and event["parent_event_id"]
        )
    if flow == "tunnel":
        return bool(
            source == "delegation"
            and event.get("domain") == "system"
            and kind in {"delegation_request", "delegation_result", "system"}
        )
    return source != "delegation"


def _validated_replay_dendritic_window(
    conn: sqlite3.Connection,
    integration_id: str,
) -> Mapping[str, Any]:
    """Recompute a shared timing cohort without the producer implementation."""

    rows = conn.execute(
        f"SELECT {_DENDRITIC_WINDOW_COLUMNS} FROM dendritic_windows w "
        "JOIN dendritic_integration_windows binding "
        "ON binding.window_id = w.id WHERE binding.integration_id = ?",
        (integration_id,),
    ).fetchall()
    if len(rows) != 1:
        raise RuntimeError("dendritic integration window binding drifted")
    values = dict(rows[0])
    member_rows = conn.execute(
        "SELECT ordinal, event_id, event_seq, arrived_at "
        "FROM dendritic_window_members WHERE window_id = ? "
        "ORDER BY ordinal ASC",
        (values.get("id"),),
    ).fetchall()
    event_count = values.get("event_count")
    observed_event_seq = values.get("observed_event_seq")
    base_silence = values.get("base_silence_threshold_seconds")
    base_max_wait = values.get("base_max_wait_seconds")
    modifier = values.get("wait_modifier")
    silence = values.get("silence_threshold_seconds")
    max_wait = values.get("max_wait_seconds")
    if (
        not isinstance(values.get("id"), str)
        or _HEX64.fullmatch(values["id"]) is None
        or values.get("policy_version") != "dendritic-window.v1"
        or not isinstance(values.get("event_set_sha256"), str)
        or _HEX64.fullmatch(values["event_set_sha256"]) is None
        or type(event_count) is not int
        or not 1 <= event_count <= 500
        or len(member_rows) != event_count
        or type(observed_event_seq) is not int
        or observed_event_seq < 1
        or isinstance(base_silence, bool)
        or not isinstance(base_silence, (int, float))
        or not math.isfinite(float(base_silence))
        or float(base_silence) < 0.0
        or isinstance(base_max_wait, bool)
        or not isinstance(base_max_wait, (int, float))
        or not math.isfinite(float(base_max_wait))
        or float(base_max_wait) < 0.0
        or isinstance(modifier, bool)
        or not isinstance(modifier, (int, float))
        or not math.isfinite(float(modifier))
        or float(modifier) <= 0.0
        or isinstance(silence, bool)
        or not isinstance(silence, (int, float))
        or not math.isfinite(float(silence))
        or float(silence) < 0.0
        or isinstance(max_wait, bool)
        or not isinstance(max_wait, (int, float))
        or not math.isfinite(float(max_wait))
        or float(max_wait) < 0.0
        or float(base_silence) * float(modifier) != float(silence)
        or float(base_max_wait) * float(modifier) != float(max_wait)
    ):
        raise RuntimeError("dendritic window replay header drifted")
    opened_at = _replay_time(values.get("window_opened_at"), field="opened")
    last_input_at = _replay_time(values.get("last_input_at"), field="last input")
    closed_at = _replay_time(values.get("window_closed_at"), field="closed")
    observed_at = _replay_time(values.get("observed_at"), field="observed")
    created_at = _replay_time(values.get("created_at"), field="created")
    if not opened_at <= last_input_at <= closed_at <= observed_at <= created_at:
        raise RuntimeError("dendritic window replay timestamp order drifted")

    members: list[dict[str, Any]] = []
    raw_events: list[dict[str, Any]] = []
    input_policies: list[dict[str, Any]] = []
    for ordinal, member_row in enumerate(member_rows):
        member = dict(member_row)
        if (
            member.get("ordinal") != ordinal
            or not isinstance(member.get("event_id"), str)
            or not member["event_id"]
            or type(member.get("event_seq")) is not int
            or member["event_seq"] < 1
        ):
            raise RuntimeError("dendritic window replay member drifted")
        event_row = conn.execute(
            f"SELECT {_EVENT_COLUMNS} FROM causal_events WHERE id = ?",
            (member["event_id"],),
        ).fetchone()
        if event_row is None:
            raise RuntimeError("dendritic window replay event is missing")
        event = dict(event_row)
        try:
            metadata = json.loads(event.get("metadata") or "{}")
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("dendritic window event metadata drifted") from exc
        if not isinstance(metadata, dict):
            raise RuntimeError("dendritic window event metadata drifted")
        event["metadata"] = metadata
        arrived_at = _replay_time(member.get("arrived_at"), field="arrival")
        raw_created_at = _replay_time(event.get("created_at"), field="event")
        policy_row = conn.execute(
            "SELECT world_id, engram_id, policy_version, "
            "base_silence_threshold_seconds, base_max_wait_seconds, "
            "wait_modifier, silence_threshold_seconds, max_wait_seconds, "
            "recorded_at FROM dendritic_input_policy_snapshots "
            "WHERE event_id = ?",
            (member["event_id"],),
        ).fetchone()
        if policy_row is None:
            raise RuntimeError("dendritic input policy snapshot is missing")
        input_policy = dict(policy_row)
        recorded_at = _replay_time(
            input_policy.get("recorded_at"),
            field="policy recorded",
        )
        try:
            policy_numbers = tuple(
                float(input_policy[name])
                for name in (
                    "base_silence_threshold_seconds",
                    "base_max_wait_seconds",
                    "wait_modifier",
                    "silence_threshold_seconds",
                    "max_wait_seconds",
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("dendritic input policy snapshot drifted") from exc
        if (
            input_policy.get("world_id") != values.get("world_id")
            or input_policy.get("engram_id")
            != values.get("formation_engram_id")
            or input_policy.get("policy_version") != "dendritic-window.v1"
            or any(not math.isfinite(value) for value in policy_numbers)
            or policy_numbers[0] < 0.0
            or policy_numbers[1] < 0.0
            or policy_numbers[2] <= 0.0
            or policy_numbers[3] < 0.0
            or policy_numbers[4] < 0.0
            or policy_numbers[0] * policy_numbers[2] != policy_numbers[3]
            or policy_numbers[1] * policy_numbers[2] != policy_numbers[4]
            or not raw_created_at <= recorded_at <= created_at
        ):
            raise RuntimeError("dendritic input policy snapshot drifted")
        if (
            event.get("seq") != member["event_seq"]
            or raw_created_at != arrived_at
            or event.get("world_id") != values.get("world_id")
            or event.get("engram_id") != values.get("formation_engram_id")
            or not _replay_dendritic_window_shape(event)
        ):
            raise RuntimeError("dendritic window raw event provenance drifted")
        member["arrived_at"] = event["created_at"]
        members.append(member)
        raw_events.append(event)
        input_policies.append(input_policy)

    ordered = sorted(
        raw_events,
        key=lambda event: (
            _replay_time(event["created_at"], field="event"),
            event["seq"],
            event["id"],
        ),
    )
    ordered_ids = tuple(event["id"] for event in ordered)
    if ordered_ids != tuple(member["event_id"] for member in members):
        raise RuntimeError("dendritic window replay ordering drifted")
    if observed_event_seq < max(member["event_seq"] for member in members):
        raise RuntimeError("dendritic window replay watermark drifted")
    member_ids = set(ordered_ids)
    interval_rows = conn.execute(
        f"SELECT {_EVENT_COLUMNS} FROM causal_events "
        "WHERE world_id = ? AND engram_id = ? "
        "AND created_at >= ? AND created_at <= ? "
        "AND seq <= ? AND NOT EXISTS ("
        "SELECT 1 FROM dendritic_window_members member "
        "WHERE member.event_id = causal_events.id"
        ") ORDER BY seq ASC",
        (
            values.get("world_id"),
            values.get("formation_engram_id"),
            values.get("window_opened_at"),
            values.get("window_closed_at"),
            observed_event_seq,
        ),
    ).fetchall()
    for interval_row in interval_rows:
        event = dict(interval_row)
        if event.get("id") in member_ids:
            continue
        try:
            event["metadata"] = json.loads(event.get("metadata") or "{}")
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("dendritic interval metadata drifted") from exc
        if _replay_dendritic_window_shape(event):
            raise RuntimeError(
                "dendritic window replay omitted an input inside its boundary"
            )
    event_set_sha256 = hashlib.sha256(
        json.dumps(
            ordered_ids,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    arrivals = [
        _replay_time(event["created_at"], field="event") for event in ordered
    ]
    if (
        event_set_sha256 != values["event_set_sha256"]
        or arrivals[0] != opened_at
        or arrivals[-1] != last_input_at
        or input_policies[0].get("policy_version")
        != values.get("policy_version")
        or float(input_policies[0]["base_silence_threshold_seconds"])
        != float(base_silence)
        or float(input_policies[0]["base_max_wait_seconds"])
        != float(base_max_wait)
        or float(input_policies[0]["wait_modifier"]) != float(modifier)
        or float(input_policies[0]["silence_threshold_seconds"])
        != float(silence)
        or float(input_policies[0]["max_wait_seconds"])
        != float(max_wait)
    ):
        raise RuntimeError("dendritic window replay event set drifted")
    deadline = min(
        opened_at + timedelta(seconds=float(max_wait)),
        opened_at + timedelta(seconds=float(silence)),
    )
    for arrival in arrivals[1:]:
        if arrival > deadline:
            raise RuntimeError("dendritic window replay crosses a closed boundary")
        deadline = min(
            opened_at + timedelta(seconds=float(max_wait)),
            arrival + timedelta(seconds=float(silence)),
        )
    if deadline != closed_at:
        raise RuntimeError("dendritic window replay policy boundary drifted")
    identity_projection = {
        "schema_version": values["policy_version"],
        "world_id": values["world_id"],
        "formation_engram_id": values["formation_engram_id"],
        "event_ids": list(ordered_ids),
        "event_seqs": [member["event_seq"] for member in members],
        "base_silence_threshold_seconds": float(base_silence),
        "base_max_wait_seconds": float(base_max_wait),
        "wait_modifier": float(modifier),
        "silence_threshold_seconds": float(silence),
        "max_wait_seconds": float(max_wait),
        "window_opened_at": values["window_opened_at"],
        "last_input_at": values["last_input_at"],
        "window_closed_at": values["window_closed_at"],
    }
    expected_id = hashlib.sha256(
        json.dumps(
            identity_projection,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    if expected_id != values["id"]:
        raise RuntimeError("dendritic window replay identity drifted")
    values["members"] = members
    return values


def _replay_same_or_committed_successor(
    conn: sqlite3.Connection,
    original_engram_id: str,
    current_engram_id: Any,
) -> bool:
    if not isinstance(current_engram_id, str):
        return False
    return conn.execute(
        "WITH RECURSIVE lineage(id) AS ("
        "SELECT ? UNION SELECT generation.successor_id "
        "FROM generation_transitions generation JOIN lineage current "
        "ON generation.predecessor_id = current.id "
        "WHERE generation.state = 'committed' "
        "AND generation.successor_id IS NOT NULL"
        ") SELECT 1 FROM lineage WHERE id = ? LIMIT 1",
        (original_engram_id, current_engram_id),
    ).fetchone() is not None


def _replay_dendritic_window_evidence(
    conn: sqlite3.Connection,
    integration: Mapping[str, Any],
) -> tuple[str, Mapping[str, Any] | None]:
    """Classify a nexus as proven v6 window evidence or sealed v5 legacy."""

    integration_id = integration.get("id")
    binding_rows = conn.execute(
        "SELECT window_id FROM dendritic_integration_windows "
        "WHERE integration_id = ?",
        (integration_id,),
    ).fetchall()
    legacy_rows = conn.execute(
        "SELECT legacy.source_schema_version, legacy.evidence_class, "
        "legacy.integration_created_at, migration.name "
        "FROM dendritic_legacy_integrations legacy "
        "JOIN schema_migrations migration ON migration.version = 6 "
        "WHERE legacy.integration_id = ?",
        (integration_id,),
    ).fetchall()
    if len(binding_rows) == 1 and not legacy_rows:
        return (
            _WINDOW_EVIDENCE_DURABLE_V6,
            _validated_replay_dendritic_window(conn, str(integration_id)),
        )
    if not binding_rows and len(legacy_rows) == 1:
        legacy = legacy_rows[0]
        trigger_row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
            "AND name = 'dendritic_legacy_integrations_closed_insert'",
        ).fetchone()
        trigger_sql = (
            trigger_row[0].casefold()
            if trigger_row is not None and isinstance(trigger_row[0], str)
            else ""
        )
        version_row = conn.execute("PRAGMA user_version").fetchone()
        if (
            legacy[0] != 5
            or legacy[1] != _WINDOW_EVIDENCE_LEGACY_V5
            or legacy[2] != integration.get("created_at")
            or legacy[3] != "dendritic_window_evidence"
            or "before insert on dendritic_legacy_integrations"
            not in " ".join(trigger_sql.split())
            or version_row is None
            or version_row[0] != 6
        ):
            raise RuntimeError("legacy dendritic migration seal drifted")
        return _WINDOW_EVIDENCE_LEGACY_V5, None
    raise RuntimeError(
        "dendritic integration has neither one durable window nor one "
        "sealed v5 legacy marker"
    )


def _validated_replay_dendritic_integration(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
) -> Mapping[str, Any]:
    """Independently rebuild convergence evidence from read-only SQLite."""

    values = dict(row)
    window_evidence_class, window = _replay_dendritic_window_evidence(
        conn,
        values,
    )
    member_rows = conn.execute(
        "SELECT ordinal, event_id, event_seq, causal_id, source_identity, "
        "content_sha256, arrived_at FROM dendritic_integration_members "
        "WHERE integration_id = ? ORDER BY ordinal ASC",
        (values["id"],),
    ).fetchall()
    if (
        type(values.get("member_count")) is not int
        or not 2 <= values["member_count"] <= 64
        or len(member_rows) != values["member_count"]
        or values.get("delivery_class") not in {"external", "propagation"}
        or not isinstance(values.get("window_opened_at"), str)
        or not isinstance(values.get("window_closed_at"), str)
        or values["window_opened_at"] > values["window_closed_at"]
        or (
            window_evidence_class == _WINDOW_EVIDENCE_DURABLE_V6
            and (
                window is None
                or values.get("window_opened_at")
                != window.get("window_opened_at")
                or values.get("window_closed_at")
                != window.get("window_closed_at")
                or values.get("created_at") != window.get("created_at")
                or values.get("world_id") != window.get("world_id")
                or values.get("formation_engram_id")
                != window.get("formation_engram_id")
            )
        )
        or (
            window_evidence_class == _WINDOW_EVIDENCE_LEGACY_V5
            and (
                window is not None
                or values.get("window_closed_at") != values.get("created_at")
            )
        )
    ):
        raise RuntimeError("dendritic integration replay header drifted")

    members: list[dict[str, Any]] = []
    source_events: list[dict[str, Any]] = []
    signatures: list[tuple[str, str]] = []
    for ordinal, member_row in enumerate(member_rows):
        member = dict(member_row)
        if member.get("ordinal") != ordinal:
            raise RuntimeError("dendritic integration replay ordinal drifted")
        event_row = conn.execute(
            f"SELECT {_EVENT_COLUMNS} FROM causal_events WHERE id = ?",
            (member.get("event_id"),),
        ).fetchone()
        if event_row is None:
            raise RuntimeError("dendritic integration replay member is missing")
        event = dict(event_row)
        try:
            metadata = json.loads(event.get("metadata") or "{}")
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                "dendritic integration replay member metadata drifted"
            ) from exc
        if not isinstance(metadata, dict):
            raise RuntimeError("dendritic integration replay member metadata drifted")
        event["metadata"] = metadata
        if (
            event.get("kind") == "propagation"
            or event.get("source") == "propagation"
        ):
            source_id = metadata.get("source_engram_id")
            if (
                event.get("kind") != "propagation"
                or event.get("source") != "propagation"
                or not isinstance(source_id, str)
                or not source_id.strip()
            ):
                raise RuntimeError(
                    "dendritic integration replay propagation source drifted"
                )
            signature = ("propagation", f"engram:{source_id}")
        else:
            source_id = metadata.get("source_engram_id")
            signature = (
                "external",
                (
                    f"engram:{source_id}"
                    if window_evidence_class == _WINDOW_EVIDENCE_LEGACY_V5
                    and isinstance(source_id, str)
                    and source_id.strip()
                    else f"source:{event.get('source')}"
                ),
            )
        content = event.get("content")
        if (
            event.get("status") != "reconciled"
            or event.get("resolution") != "superseded"
            or event.get("resolution_note")
            != f"dendritic_integration:{values['id']}"
            or event.get("flow") != "content"
            or not isinstance(content, str)
            or not content.strip()
            or metadata.get("dendritic_integration_version") is not None
            or event.get("seq") != member.get("event_seq")
            or event.get("causal_id") != member.get("causal_id")
            or event.get("world_id") != values.get("world_id")
            or event.get("engram_id") != values.get("formation_engram_id")
            or event.get("center_id") != values.get("center_id")
            or event.get("created_at") != member.get("arrived_at")
            or event.get("attempts") != 0
            or event.get("started_at") is not None
            or event.get("updated_at") != values.get("created_at")
            or event.get("settled_at") != values.get("created_at")
            or signature[1] != member.get("source_identity")
            or hashlib.sha256(content.encode("utf-8")).hexdigest()
            != member.get("content_sha256")
            or conn.execute(
                "SELECT 1 FROM harness_turns WHERE event_id = ? LIMIT 1",
                (event.get("id"),),
            ).fetchone()
            is not None
        ):
            raise RuntimeError("dendritic integration replay member drifted")
        if signature[0] == "propagation":
            _assert_replay_dendritic_propagation(
                conn,
                event,
                signature[1],
            )
        members.append(member)
        source_events.append(event)
        signatures.append(signature)

    ordered_ids = [member["event_id"] for member in members]
    member_set_sha256 = hashlib.sha256(
        json.dumps(
            ordered_ids,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    integrated_content = "\n\n".join(event["content"] for event in source_events)
    if (
        values.get("member_set_sha256") != member_set_sha256
        or values.get("content_sha256")
        != hashlib.sha256(integrated_content.encode("utf-8")).hexdigest()
        or {signature[0] for signature in signatures}
        != {values["delivery_class"]}
        or len({signature[1] for signature in signatures}) < 2
        or (
            window is not None
            and not set(ordered_ids).issubset(
                {member["event_id"] for member in window["members"]}
            )
        )
    ):
        raise RuntimeError("dendritic integration replay window drifted")

    aggregate_row = conn.execute(
        f"SELECT {_EVENT_COLUMNS} FROM causal_events WHERE id = ?",
        (values.get("aggregate_event_id"),),
    ).fetchone()
    if aggregate_row is None:
        raise RuntimeError("dendritic integration replay aggregate is missing")
    aggregate = dict(aggregate_row)
    try:
        aggregate_metadata = json.loads(aggregate.get("metadata") or "{}")
    except (TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "dendritic integration replay aggregate metadata drifted"
        ) from exc
    default_priority = 0.8 if values["delivery_class"] == "propagation" else 1.0
    priorities: list[float] = []
    depths: list[int] = []
    for event in source_events:
        raw_priority = event["metadata"].get("priority", default_priority)
        priorities.append(
            float(raw_priority)
            if isinstance(raw_priority, (int, float))
            and not isinstance(raw_priority, bool)
            and math.isfinite(float(raw_priority))
            else default_priority
        )
        raw_depth = event["metadata"].get("depth", 0)
        depths.append(raw_depth if type(raw_depth) is int and raw_depth >= 0 else 0)
    expected_metadata = {
        "dendritic_delivery_class": values["delivery_class"],
        "dendritic_integration_id": values["id"],
        "dendritic_integration_version": 1,
        "dendritic_member_count": values["member_count"],
        "dendritic_member_set_sha256": values["member_set_sha256"],
        "depth": max(depths),
        "priority": max(priorities),
    }
    if window is not None:
        expected_metadata["dendritic_window_id"] = window["id"]
    if (
        aggregate.get("id") != values.get("aggregate_event_id")
        or aggregate.get("causal_id") != aggregate.get("id")
        or aggregate.get("parent_event_id") is not None
        or aggregate.get("world_id") != values.get("world_id")
        or not _replay_same_or_committed_successor(
            conn,
            values["formation_engram_id"],
            aggregate.get("engram_id"),
        )
        or aggregate.get("center_id") != values.get("center_id")
        or aggregate.get("flow") != "content"
        or aggregate.get("domain") != "pulse"
        or aggregate.get("kind") != "pulse"
        or aggregate.get("source") != "self"
        or aggregate.get("content") != integrated_content
        or aggregate_metadata != expected_metadata
        or aggregate.get("idempotency_key")
        != f"dendritic:{values['member_set_sha256']}"
        or aggregate.get("created_at") != values.get("created_at")
    ):
        raise RuntimeError("dendritic integration replay aggregate drifted")
    values["members"] = members
    values["window_evidence_class"] = window_evidence_class
    values["window"] = window
    return values


def _fault(status: int, error: str, detail: str, remedy: str) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": error, "detail": detail, "remedy": remedy},
    )


def _parse_int(raw: str | None, *, name: str, default: int, minimum: int = 0) -> int | JSONResponse:
    if raw is None or raw == "":
        return default
    try:
        value = int(raw, 10)
    except (TypeError, ValueError):
        return _fault(400, "invalid_filter", f"{name} must be an integer", f"provide {name} as a non-negative integer")
    if value < minimum:
        return _fault(400, "invalid_filter", f"{name} must be at least {minimum}", f"provide {name} as a valid integer")
    return value


def _parse_limit(raw: str | None, *, name: str = "limit") -> int | JSONResponse:
    parsed = _parse_int(raw, name=name, default=100, minimum=1)
    if isinstance(parsed, JSONResponse):
        return parsed
    if parsed > _MAX_LIMIT:
        return _fault(400, "invalid_filter", f"{name} must be between 1 and {_MAX_LIMIT}", f"provide {name} in the range 1..{_MAX_LIMIT}")
    return parsed


def _parse_bool(raw: str | None, *, name: str) -> bool | JSONResponse:
    if raw is None or raw == "":
        return False
    normalized = raw.casefold()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    return _fault(400, "invalid_parameter", f"{name} must be a boolean", f"provide {name}=1 or {name}=0")


def _parse_direction(raw: str | None) -> str | JSONResponse:
    if raw is None:
        return "forward"
    if raw in {"forward", "backward"}:
        return raw
    return _fault(
        400,
        "invalid_filter",
        "direction must be forward or backward",
        "use direction=forward or direction=backward",
    )


def _parse_id_filter(raw: str | None, *, name: str) -> str | None | JSONResponse:
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        return _fault(
            400,
            "invalid_filter",
            f"{name} cannot be empty",
            f"omit {name} or provide a non-empty identifier",
        )
    return raw


def _unknown_query_filters(
    request: Request,
    allowed: set[str],
) -> JSONResponse | None:
    unknown = sorted(set(request.query_params) - allowed)
    if not unknown:
        return None
    names = ", ".join(unknown)
    return _fault(
        400,
        "unknown_filter",
        f"unknown query filter(s): {names}",
        "remove unknown filters and use the documented causal query parameters",
    )


def _parse_enum(
    raw: str | None,
    enum_type: type,
    *,
    name: str,
    many: bool = False,
) -> Any | JSONResponse:
    if raw is None or raw == "":
        return None
    values = [part.strip() for part in raw.split(",") if part.strip()]
    if not values:
        return _fault(400, "invalid_filter", f"{name} cannot be empty", f"use one of: {', '.join(member.value for member in enum_type)}")
    try:
        parsed = [enum_type(value) for value in values]
    except ValueError:
        allowed = ", ".join(member.value for member in enum_type)
        return _fault(400, "invalid_filter", f"{name} must be one of: {allowed}", f"use one of the documented {name} values")
    if many:
        return tuple(parsed)
    if len(parsed) != 1:
        return _fault(400, "invalid_filter", f"{name} accepts one value", f"provide a single {name}")
    return parsed[0]


def _row_to_event(row: sqlite3.Row) -> _DbRow:
    values = dict(row)
    try:
        values["metadata"] = json.loads(values.get("metadata") or "{}")
    except (TypeError, json.JSONDecodeError):
        values["metadata"] = {}
    return _DbRow(values)


class _CausalReadSource:
    """Ledger-first source with a read-only SQLite replay fallback."""

    def __init__(
        self,
        db_path: str | Path | None,
        runtime: object | None,
        ledger: CausalLedger | None,
    ) -> None:
        self.db_path = Path(db_path) if db_path is not None else None
        self.runtime = runtime
        self.explicit_ledger = ledger

    @property
    def ledger(self) -> CausalLedger | None:
        if self.explicit_ledger is not None:
            return self.explicit_ledger
        candidate = getattr(self.runtime, "causal_ledger", None)
        return candidate if isinstance(candidate, CausalLedger) else candidate

    @property
    def world_id(self) -> str | None:
        candidate = getattr(self.runtime, "world_id", None)
        return candidate if isinstance(candidate, str) and candidate else None

    def ensure_available(self) -> None:
        if self.ledger is None and self.db_path is None:
            raise _NoCausalSource

    def _connect(self) -> sqlite3.Connection | None:
        if self.db_path is None or not self.db_path.exists():
            return None
        try:
            conn = sqlite3.connect(
                f"file:{self.db_path.resolve().as_posix()}?mode=ro",
                uri=True,
                timeout=2.0,
            )
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.OperationalError as exc:
            raise RuntimeError("causal database is busy") from exc

    @staticmethod
    def _where(
        *,
        after_seq: int | None = None,
        before_seq: int | None = None,
        world_id: str | None = None,
        engram_id: str | None = None,
        center_id: str | None = None,
        causal_id: str | None = None,
        parent_event_id: str | None = None,
        status: CausalEventStatus | Iterable[CausalEventStatus] | None = None,
        flow: CausalEventFlow | None = None,
        domain: CausalEventDomain | None = None,
        kind: CausalEventKind | None = None,
        source: CausalEventSource | None = None,
    ) -> tuple[list[str], list[Any]]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        if after_seq is not None:
            clauses.append("seq > ?")
            params.append(after_seq)
        if before_seq is not None:
            clauses.append("seq < ?")
            params.append(before_seq)
        for column, value in (
            ("world_id", world_id),
            ("engram_id", engram_id),
            ("center_id", center_id),
            ("causal_id", causal_id),
            ("parent_event_id", parent_event_id),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if status is not None:
            values = tuple(status) if not isinstance(status, CausalEventStatus) else (status,)
            clauses.append("status IN (" + ",".join("?" for _ in values) + ")")
            params.extend(_value(item) for item in values)
        for column, value in (
            ("flow", flow),
            ("domain", domain),
            ("kind", kind),
            ("source", source),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(_value(value))
        return clauses, params

    def _list_events_sql(
        self,
        *,
        after_seq: int | None,
        before_seq: int | None,
        limit: int,
        descending: bool = False,
        **filters: Any,
    ) -> list[Any]:
        conn = self._connect()
        if conn is None:
            return []
        clauses, params = self._where(
            after_seq=after_seq,
            before_seq=before_seq,
            **filters,
        )
        order = "DESC" if descending else "ASC"
        try:
            rows = conn.execute(
                f"SELECT {_EVENT_COLUMNS} FROM causal_events WHERE "
                + " AND ".join(clauses)
                + f" ORDER BY seq {order} LIMIT ?",
                [*params, limit],
            ).fetchall()
            events = [_row_to_event(row) for row in rows]
            return list(reversed(events)) if descending else events
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return []
            raise RuntimeError("causal database could not be read") from exc
        finally:
            conn.close()

    def list_events(
        self,
        *,
        after_seq: int,
        limit: int,
        direction: str = "forward",
        before_seq: int | None = None,
        **filters: Any,
    ) -> list[Any]:
        world_id = filters.get("world_id") or self.world_id
        filters = {**filters, "world_id": world_id}
        ledger = self.ledger
        if direction == "backward":
            # CausalLedger currently exposes only an ascending cursor.  The
            # configured database file is the public read boundary shared by
            # the live and replay apps, so reverse pages stay bounded in SQL
            # and never copy the whole live ledger into memory.
            if self.db_path is None or (
                ledger is not None and not self.db_path.exists()
            ):
                raise _NoReverseSource
            return self._list_events_sql(
                after_seq=None,
                before_seq=before_seq,
                limit=limit,
                descending=True,
                **filters,
            )
        if ledger is not None:
            return ledger.list_events(after_seq=after_seq, limit=limit, **filters)
        return self._list_events_sql(
            after_seq=after_seq,
            before_seq=None,
            limit=limit,
            descending=False,
            **filters,
        )

    def center_exists(self, center_id: str) -> bool:
        """Validate a Center filter without inferring it from event history."""

        if self.db_path is not None and self.db_path.exists():
            conn = self._connect()
            if conn is None:
                return False
            try:
                row = conn.execute(
                    "SELECT 1 FROM activity_centers WHERE id = ? LIMIT 1",
                    (center_id,),
                ).fetchone()
                return row is not None
            except sqlite3.OperationalError as exc:
                if "no such table" in str(exc):
                    return False
                raise RuntimeError("activity center database could not be read") from exc
            finally:
                conn.close()

        runtime_get = getattr(self.runtime, "get_activity_center", None)
        if callable(runtime_get):
            try:
                return runtime_get(center_id) is not None
            except Exception as exc:  # noqa: BLE001 - preserve source errors
                if getattr(exc, "error", None) == "unknown_activity_center":
                    return False
                raise

        storage = getattr(self.ledger, "storage", None)
        storage_get = getattr(storage, "get_activity_center", None)
        if callable(storage_get):
            return storage_get(center_id) is not None
        return False

    def has_event_before(self, seq: int, **filters: Any) -> bool:
        """Return whether a bounded matching event exists below ``seq``."""

        ledger = self.ledger
        if ledger is not None:
            events = ledger.list_events(
                after_seq=0,
                limit=1,
                **filters,
            )
            return bool(events and (_row_value(events[0], "seq") or 0) < seq)

        conn = self._connect()
        if conn is None:
            return False
        clauses, params = self._where(before_seq=seq, **filters)
        try:
            row = conn.execute(
                f"SELECT 1 FROM causal_events WHERE "
                + " AND ".join(clauses)
                + " ORDER BY seq ASC LIMIT 1",
                params,
            ).fetchone()
            return row is not None
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return False
            raise RuntimeError("causal database could not be read") from exc
        finally:
            conn.close()

    def get_event(self, event_id: str) -> Any | None:
        ledger = self.ledger
        if ledger is not None:
            return ledger.get_event(event_id)
        conn = self._connect()
        if conn is None:
            return None
        try:
            row = conn.execute(
                f"SELECT {_EVENT_COLUMNS} FROM causal_events WHERE id = ?",
                (event_id,),
            ).fetchone()
            return _row_to_event(row) if row is not None else None
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return None
            raise RuntimeError("causal database could not be read") from exc
        finally:
            conn.close()

    def children(self, event_id: str) -> list[Any]:
        ledger = self.ledger
        if ledger is not None:
            return ledger.get_children(event_id)
        conn = self._connect()
        if conn is None:
            return []
        try:
            rows = conn.execute(
                f"SELECT {_EVENT_COLUMNS} FROM causal_events WHERE parent_event_id = ? ORDER BY seq ASC",
                (event_id,),
            ).fetchall()
            return [_row_to_event(row) for row in rows]
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return []
            raise RuntimeError("causal database could not be read") from exc
        finally:
            conn.close()

    def dendritic_integration(
        self,
        event_id: str,
    ) -> DendriticIntegration | Mapping[str, Any] | None:
        ledger = self.ledger
        if ledger is not None:
            return ledger.get_dendritic_integration_for_event(event_id)
        conn = self._connect()
        if conn is None:
            return None
        try:
            row = conn.execute(
                f"SELECT {_DENDRITIC_INTEGRATION_COLUMNS} "
                "FROM dendritic_integrations i "
                "WHERE i.aggregate_event_id = ? OR EXISTS ("
                "SELECT 1 FROM dendritic_integration_members m "
                "WHERE m.integration_id = i.id AND m.event_id = ?"
                ") OR EXISTS (SELECT 1 FROM causal_events e WHERE e.id = ? "
                "AND e.causal_id = i.aggregate_event_id) LIMIT 1",
                (event_id, event_id, event_id),
            ).fetchone()
            if row is None:
                return None
            return _validated_replay_dendritic_integration(conn, row)
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return None
            raise RuntimeError(
                "dendritic integration evidence could not be read"
            ) from exc
        finally:
            conn.close()

    def amplification(self, causal_id: str) -> Any | None:
        """Read one complete chain from the live ledger or a read-only DB."""

        ledger = self.ledger
        if ledger is not None:
            return ledger.causal_amplification(
                causal_id,
                world_id=self.world_id,
            )
        conn = self._connect()
        if conn is None:
            return None
        try:
            conn.execute("BEGIN")
            return read_causal_amplification(
                conn,
                causal_id,
                world_id=self.world_id,
            )
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return None
            raise RuntimeError("causal amplification could not be read") from exc
        finally:
            conn.close()

    def list_turns(self, *, engram_id: str | None = None, state: HarnessTurnState | None = None) -> list[Any]:
        ledger = self.ledger
        if ledger is not None:
            return ledger.list_turns(engram_id=engram_id, state=state)
        conn = self._connect()
        if conn is None:
            return []
        clauses = ["1 = 1"]
        params: list[Any] = []
        if engram_id is not None:
            clauses.append("engram_id = ?")
            params.append(engram_id)
        if state is not None:
            clauses.append("state = ?")
            params.append(_value(state))
        try:
            rows = conn.execute(
                f"SELECT {_TURN_COLUMNS} FROM harness_turns WHERE "
                + " AND ".join(clauses)
                + " ORDER BY started_at ASC, rowid ASC",
                params,
            ).fetchall()
            return [_DbRow(dict(row)) for row in rows]
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return []
            raise RuntimeError("harness turn data could not be read") from exc
        finally:
            conn.close()

    def list_generations(self, **filters: Any) -> list[Any]:
        ledger = self.ledger
        if ledger is not None:
            return ledger.list_generations(**filters)
        conn = self._connect()
        if conn is None:
            return []
        clauses = ["1 = 1"]
        params: list[Any] = []
        for column in ("predecessor_id", "successor_id", "event_id", "causal_id"):
            value = filters.get(column)
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if filters.get("state") is not None:
            clauses.append("state = ?")
            params.append(_value(filters["state"]))
        try:
            rows = conn.execute(
                f"SELECT {_GENERATION_COLUMNS} FROM generation_transitions WHERE "
                + " AND ".join(clauses)
                + " ORDER BY created_at ASC, id ASC",
                params,
            ).fetchall()
            return [_DbRow(dict(row)) for row in rows]
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return []
            raise RuntimeError("generation data could not be read") from exc
        finally:
            conn.close()


def _source_fault(exc: Exception) -> JSONResponse:
    if isinstance(exc, _NoCausalSource):
        return _fault(
            404,
            "no_causal_source",
            "causal routes need a live runtime or a configured SQLite database",
            "start the server with --db <run.db> or attach RuntimeService",
        )
    if isinstance(exc, _NoReverseSource):
        return _fault(
            503,
            "causal_reverse_unavailable",
            "backward causal reads need the configured SQLite database",
            "start the server with --db <run.db> so the bounded reverse cursor can be read",
        )
    _logger.warning("causal read failed: %s", exc)
    return _fault(
        503,
        "causal_unavailable",
        "the durable causal ledger is temporarily unavailable",
        "retry after the runtime or database becomes readable",
    )


def create_causal_router(
    *,
    db_path: str | Path | None = None,
    runtime: object | None = None,
    ledger: CausalLedger | None = None,
    poll_interval: float = _DEFAULT_POLL_INTERVAL,
) -> APIRouter:
    """Create the public causal API router."""

    causal_source = _CausalReadSource(db_path, runtime, ledger)
    router = APIRouter(tags=["causal"])
    sleep_interval = max(float(poll_interval), 0.05)

    def _event_filters(
        *,
        world_id: str | None,
        engram_id: str | None,
        center_id: str | None,
        causal_id: str | None,
        parent_event_id: str | None,
        status: str | None,
        flow: str | None,
        domain: str | None,
        kind: str | None,
        event_source: str | None,
    ) -> dict[str, Any] | JSONResponse:
        parsed_world = _parse_id_filter(world_id, name="world_id")
        if isinstance(parsed_world, JSONResponse):
            return parsed_world
        parsed_engram = _parse_id_filter(engram_id, name="engram_id")
        if isinstance(parsed_engram, JSONResponse):
            return parsed_engram
        parsed_center = _parse_id_filter(center_id, name="center_id")
        if isinstance(parsed_center, JSONResponse):
            return parsed_center
        parsed_causal = _parse_id_filter(causal_id, name="causal_id")
        if isinstance(parsed_causal, JSONResponse):
            return parsed_causal
        parsed_parent = _parse_id_filter(parent_event_id, name="parent_event_id")
        if isinstance(parsed_parent, JSONResponse):
            return parsed_parent
        parsed_status = _parse_enum(status, CausalEventStatus, name="status", many=True)
        if isinstance(parsed_status, JSONResponse):
            return parsed_status
        parsed_flow = _parse_enum(flow, CausalEventFlow, name="flow")
        if isinstance(parsed_flow, JSONResponse):
            return parsed_flow
        parsed_domain = _parse_enum(domain, CausalEventDomain, name="domain")
        if isinstance(parsed_domain, JSONResponse):
            return parsed_domain
        parsed_kind = _parse_enum(kind, CausalEventKind, name="kind")
        if isinstance(parsed_kind, JSONResponse):
            return parsed_kind
        parsed_source = _parse_enum(event_source, CausalEventSource, name="source")
        if isinstance(parsed_source, JSONResponse):
            return parsed_source
        return {
            "world_id": parsed_world if parsed_world is not None else causal_source.world_id,
            "engram_id": parsed_engram,
            "center_id": parsed_center,
            "causal_id": parsed_causal,
            "parent_event_id": parsed_parent,
            "status": parsed_status,
            "flow": parsed_flow,
            "domain": parsed_domain,
            "kind": parsed_kind,
            "source": parsed_source,
        }

    @router.get("/causal-events")
    def causal_events(
        request: Request,
        after_seq: str | None = None,
        before_seq: str | None = None,
        direction: str | None = None,
        limit: str | None = None,
        world_id: str | None = None,
        engram_id: str | None = None,
        center_id: str | None = None,
        causal_id: str | None = None,
        parent_event_id: str | None = None,
        status: str | None = None,
        flow: str | None = None,
        domain: str | None = None,
        kind: str | None = None,
        source: str | None = None,
    ) -> JSONResponse:
        unknown = _unknown_query_filters(
            request,
            {
                "after_seq",
                "before_seq",
                "direction",
                "limit",
                "world_id",
                "engram_id",
                "center_id",
                "causal_id",
                "parent_event_id",
                "status",
                "flow",
                "domain",
                "kind",
                "source",
            },
        )
        if unknown is not None:
            return unknown
        parsed_direction = _parse_direction(direction)
        if isinstance(parsed_direction, JSONResponse):
            return parsed_direction
        has_after = "after_seq" in request.query_params
        has_before = "before_seq" in request.query_params
        if parsed_direction == "backward" and has_after:
            return _fault(
                400,
                "invalid_filter",
                "backward direction does not accept after_seq",
                "omit after_seq and use before_seq as the backward cursor",
            )
        if parsed_direction == "forward" and has_before:
            return _fault(
                400,
                "invalid_filter",
                "forward direction does not accept before_seq",
                "omit before_seq or use direction=backward",
            )
        parsed_after = _parse_int(after_seq, name="after_seq", default=0)
        if isinstance(parsed_after, JSONResponse):
            return parsed_after
        parsed_before = _parse_int(before_seq, name="before_seq", default=0)
        if isinstance(parsed_before, JSONResponse):
            return parsed_before
        if not has_before:
            parsed_before = None
        parsed_limit = _parse_limit(limit)
        if isinstance(parsed_limit, JSONResponse):
            return parsed_limit
        filters = _event_filters(
            world_id=world_id,
            engram_id=engram_id,
            center_id=center_id,
            causal_id=causal_id,
            parent_event_id=parent_event_id,
            status=status,
            flow=flow,
            domain=domain,
            kind=kind,
            event_source=source,
        )
        if isinstance(filters, JSONResponse):
            return filters
        try:
            causal_source.ensure_available()
            if filters["center_id"] is not None and not causal_source.center_exists(filters["center_id"]):
                return _fault(
                    404,
                    "unknown_center_filter",
                    f"ActivityCenter {filters['center_id']} was not found",
                    "list ActivityCenters and use one of their ids",
                )
            events = causal_source.list_events(
                after_seq=parsed_after,
                before_seq=parsed_before if parsed_direction == "backward" else None,
                direction=parsed_direction,
                limit=parsed_limit,
                **filters,
            )
            earliest_seq = _row_value(events[0], "seq") if events else None
            has_earlier = (
                causal_source.has_event_before(earliest_seq, **filters)
                if isinstance(earliest_seq, int)
                else False
            )
        except Exception as exc:  # noqa: BLE001 - route boundary is structured
            return _source_fault(exc)
        next_seq = parsed_after if parsed_direction == "forward" else (parsed_before or 0)
        if events:
            next_seq = _row_value(events[-1], "seq") or parsed_after
        return JSONResponse(
            {
                "events": [_safe_event(event) for event in events],
                "next_seq": next_seq,
                "has_earlier": has_earlier,
                "earliest_seq": earliest_seq,
            }
        )

    @router.get("/causal-events/{event_id}")
    def causal_event_detail(event_id: str) -> JSONResponse:
        try:
            causal_source.ensure_available()
            event = causal_source.get_event(event_id)
            if event is None or (
                causal_source.world_id is not None
                and _row_value(event, "world_id") != causal_source.world_id
            ):
                return _fault(404, "not_found", f"causal event {event_id} was not found", "use an event id from the causal list")
            children = causal_source.children(event_id)
            dendritic_integration = causal_source.dendritic_integration(event_id)
            turn = next(
                (item for item in causal_source.list_turns(engram_id=_row_value(event, "engram_id")) if _row_value(item, "event_id") == event_id),
                None,
            )
            generations = causal_source.list_generations(event_id=event_id)
            amplification = causal_source.amplification(
                _row_value(event, "causal_id")
            )
            if amplification is None:
                raise RuntimeError("causal event lost its amplification chain")
        except Exception as exc:  # noqa: BLE001
            return _source_fault(exc)
        generation = generations[0] if generations else None
        safe_turn = _safe_turn(turn) if turn is not None else None
        safe_generation = _safe_generation(generation) if generation is not None else None
        return JSONResponse(
            {
                "event": _safe_event(event, detail=True),
                "children": [_safe_event(child) for child in children],
                "turn": safe_turn,
                "harness_turn": safe_turn,
                "generation": safe_generation,
                "generation_transition": safe_generation,
                "amplification": amplification.to_dict(),
                "dendritic_integration": (
                    _safe_dendritic_integration(dendritic_integration)
                    if dendritic_integration is not None
                    else None
                ),
            }
        )

    @router.get("/causal-chains/{causal_id}/amplification")
    def causal_amplification(causal_id: str) -> JSONResponse:
        parsed = _parse_id_filter(causal_id, name="causal_id")
        if isinstance(parsed, JSONResponse):
            return parsed
        assert parsed is not None
        try:
            causal_source.ensure_available()
            snapshot = causal_source.amplification(parsed)
        except Exception as exc:  # noqa: BLE001 - structured source boundary
            return _source_fault(exc)
        if snapshot is None:
            return _fault(
                404,
                "causal_chain_not_found",
                f"causal chain {parsed} was not found",
                "use a causal_id returned by the causal event list",
            )
        return JSONResponse(snapshot.to_dict())

    @router.get("/harness-turns")
    def harness_turns(
        engram_id: str | None = None,
        state: str | None = None,
        limit: str | None = None,
        order: str | None = None,
        before_turn_id: str | None = None,
    ) -> JSONResponse:
        parsed_limit = _parse_limit(limit)
        if isinstance(parsed_limit, JSONResponse):
            return parsed_limit
        parsed_order = (order or "asc").strip().casefold()
        if parsed_order not in {"asc", "desc"}:
            return _fault(
                400,
                "invalid_order",
                "order must be 'asc' or 'desc'",
                "use order=desc when selecting the latest Harness turn",
            )
        parsed_before = _parse_id_filter(before_turn_id, name="before_turn_id")
        if isinstance(parsed_before, JSONResponse):
            return parsed_before
        if parsed_before is not None and parsed_order != "desc":
            return _fault(
                400,
                "invalid_cursor_order",
                "before_turn_id is supported only with order=desc",
                "set order=desc when paging toward older Harness turns",
            )
        parsed_state = _parse_enum(state, HarnessTurnState, name="state")
        if isinstance(parsed_state, JSONResponse):
            return parsed_state
        try:
            causal_source.ensure_available()
            rows = causal_source.list_turns(engram_id=engram_id or None, state=parsed_state)
        except Exception as exc:  # noqa: BLE001
            return _source_fault(exc)
        if parsed_order == "desc":
            rows = list(reversed(rows))
        if parsed_before is not None:
            cursor_index = next(
                (
                    index
                    for index, row in enumerate(rows)
                    if _row_value(row, "id") == parsed_before
                ),
                None,
            )
            if cursor_index is None:
                return _fault(
                    404,
                    "turn_cursor_not_found",
                    "before_turn_id was not found in the filtered Harness turn catalog",
                    "reload the first page and use its next_cursor",
                )
            rows = rows[cursor_index + 1 :]
        window = rows[: parsed_limit + 1]
        has_more = len(window) > parsed_limit
        page = window[:parsed_limit]
        next_cursor = (
            _row_value(page[-1], "id") if has_more and page else None
        )
        return JSONResponse(
            {
                "turns": [_safe_turn(row) for row in page],
                "order": parsed_order,
                "has_more": has_more,
                "next_cursor": next_cursor,
            }
        )

    @router.get("/generation-transitions")
    def generation_transitions(
        engram_id: str | None = None,
        predecessor_id: str | None = None,
        successor_id: str | None = None,
        event_id: str | None = None,
        causal_id: str | None = None,
        state: str | None = None,
        limit: str | None = None,
    ) -> JSONResponse:
        parsed_limit = _parse_limit(limit)
        if isinstance(parsed_limit, JSONResponse):
            return parsed_limit
        parsed_state = _parse_enum(state, GenerationTransitionState, name="state")
        if isinstance(parsed_state, JSONResponse):
            return parsed_state
        # engram_id is a convenient public filter even though the transition
        # table stores predecessor/successor identities rather than one owner.
        try:
            causal_source.ensure_available()
            rows = causal_source.list_generations(
                predecessor_id=predecessor_id,
                successor_id=successor_id,
                event_id=event_id,
                causal_id=causal_id,
                state=parsed_state,
            )
            if engram_id and predecessor_id is None:
                rows = [
                    row
                    for row in rows
                    if _row_value(row, "predecessor_id") == engram_id
                    or _row_value(row, "successor_id") == engram_id
                ]
        except Exception as exc:  # noqa: BLE001
            return _source_fault(exc)
        return JSONResponse({"generations": [_safe_generation(row) for row in rows[:parsed_limit]]})

    @router.post("/causal-events/{event_id}/reconcile")
    async def reconcile(event_id: str, request: Request) -> JSONResponse:
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001
            return _fault(400, "invalid_body", "request body must be a JSON object", "send {action: acknowledge|cancel|requeue, note?: string}")
        if not isinstance(body, dict):
            return _fault(400, "invalid_body", "request body must be a JSON object", "send {action: acknowledge|cancel|requeue, note?: string}")
        unknown = set(body) - {"action", "note"}
        if unknown:
            return _fault(400, "invalid_body", "reconcile accepts only action and note", "remove unknown fields from the request body")
        action = body.get("action")
        note = body.get("note")
        if not isinstance(action, str) or action.casefold() not in {"acknowledge", "cancel", "requeue"}:
            return _fault(400, "invalid_action", "action must be acknowledge, cancel, or requeue", "choose one of the three uncertain-event actions")
        if note is not None and not isinstance(note, str):
            return _fault(400, "invalid_body", "note must be a string when provided", "omit note or provide a bounded string")
        ledger = causal_source.ledger
        if ledger is None:
            return _source_fault(_NoCausalSource())
        try:
            # The ledger's atomic operation is intentionally the only write
            # primitive used here.  The read-before-write closes the public
            # route's world boundary: an event id from another world in the
            # same SQLite file must not be reconciled by this Runtime.
            existing = causal_source.get_event(event_id)
            if existing is None or (
                causal_source.world_id is not None
                and _row_value(existing, "world_id") != causal_source.world_id
            ):
                return _fault(404, "not_found", f"causal event {event_id} was not found", "use an event id from the current world")
            resolved, child = ledger.reconcile_event(
                event_id,
                action=action,
                note=note,
            )
            # ``reconcile_event`` constructs a requeue child before SQLite
            # assigns its monotonic ``seq``.  Reload that child from the
            # durable row before crossing the public API boundary; otherwise
            # the browser receives ``seq: null`` and must reject an event it
            # cannot order canonically.
            if child is not None:
                persisted_child = causal_source.get_event(child.id)
                if persisted_child is None:
                    raise RuntimeError(
                        "the reconciled child was not readable after commit"
                    )
                child = persisted_child
        except KeyError:
            return _fault(404, "not_found", f"causal event {event_id} was not found", "use an event id from the causal list")
        except (CausalTransitionError, ValueError) as exc:
            return _fault(409, "reconcile_rejected", str(exc), "only an uncertain event can be reconciled")
        except Exception as exc:  # noqa: BLE001
            return _source_fault(exc)
        return JSONResponse(
            status_code=200,
            content={
                "event": _safe_event(resolved),
                "child": _safe_event(child) if child is not None else None,
            },
        )

    @router.get("/causal-stream", response_model=None)
    async def causal_stream(
        request: Request,
        after_seq: str | None = None,
        world_id: str | None = None,
        engram_id: str | None = None,
        center_id: str | None = None,
        causal_id: str | None = None,
        parent_event_id: str | None = None,
        status: str | None = None,
        flow: str | None = None,
        domain: str | None = None,
        kind: str | None = None,
        source: str | None = None,
        once: str | None = None,
    ) -> JSONResponse | StreamingResponse:
        unknown = _unknown_query_filters(
            request,
            {
                "after_seq",
                "world_id",
                "engram_id",
                "center_id",
                "causal_id",
                "parent_event_id",
                "status",
                "flow",
                "domain",
                "kind",
                "source",
                "once",
            },
        )
        if unknown is not None:
            return unknown
        parsed_after = _parse_int(after_seq, name="after_seq", default=0)
        if isinstance(parsed_after, JSONResponse):
            return parsed_after
        parsed_once = _parse_bool(once, name="once")
        if isinstance(parsed_once, JSONResponse):
            return parsed_once
        last_event_id = request.headers.get("last-event-id")
        parsed_last = _parse_int(last_event_id, name="Last-Event-ID", default=0)
        # Invalid Last-Event-ID is ignored by design; a browser may send a
        # stale/non-numeric value from another endpoint. A valid cursor never
        # moves backwards from the explicit query cursor.
        if isinstance(parsed_last, int):
            parsed_after = max(parsed_after, parsed_last)
        filters = _event_filters(
            world_id=world_id,
            engram_id=engram_id,
            center_id=center_id,
            causal_id=causal_id,
            parent_event_id=parent_event_id,
            status=status,
            flow=flow,
            domain=domain,
            kind=kind,
            event_source=source,
        )
        if isinstance(filters, JSONResponse):
            return filters
        try:
            causal_source.ensure_available()
            if filters["center_id"] is not None and not causal_source.center_exists(filters["center_id"]):
                return _fault(
                    404,
                    "unknown_center_filter",
                    f"ActivityCenter {filters['center_id']} was not found",
                    "list ActivityCenters and use one of their ids",
                )
        except Exception as exc:  # noqa: BLE001
            return _source_fault(exc)

        async def stream():
            cursor = parsed_after
            first_pass = True
            while True:
                if not parsed_once and await request.is_disconnected():
                    return
                try:
                    events = causal_source.list_events(
                        after_seq=cursor,
                        limit=_STREAM_BATCH,
                        **filters,
                    )
                except Exception as exc:  # noqa: BLE001
                    _logger.warning("causal stream read failed: %s", exc)
                    yield ": causal stream temporarily unavailable\n\n"
                    if parsed_once:
                        return
                    await asyncio.sleep(sleep_interval)
                    continue
                emitted = False
                for item in events:
                    seq = _row_value(item, "seq")
                    if not isinstance(seq, int) or seq <= cursor:
                        continue
                    cursor = seq
                    emitted = True
                    payload = json.dumps(
                        _safe_event(item),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    yield f"id: {seq}\nevent: causal_event\ndata: {payload}\n\n"
                if parsed_once:
                    return
                if not emitted and first_pass:
                    # A comment keeps proxies from treating a valid empty
                    # replay as a dead connection, without inventing a causal
                    # event or a cursor.
                    yield ": causal stream ready\n\n"
                first_pass = False
                await asyncio.sleep(sleep_interval)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


__all__ = ["create_causal_router"]

"""Bounded observation and control routes for one Harness turn.

This module is deliberately an adapter, not a second Harness runtime.  The
event reader is expected to be backed by a redacted durable projection;
the control gateway is expected to own policy checks and the runtime
epoch fence.  The router never opens a Pi session file, reads stdout, or
calls an arbitrary runtime method as a fallback.

The factory is dependency-injected so the read side can be mounted for a
replay-only run while the write side remains an explicit 503 until a live,
policy-aware gateway is attached.  The duck-typed adapter is intentional:
The durable projection and policy gateway can evolve without making the HTTP contract
depend on private implementation details.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import PurePosixPath
from typing import Any, Protocol, runtime_checkable

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

from pulse_system.agent.harness.turn_items import project_turn_items

__all__ = [
    "HarnessControlGateway",
    "HarnessEventReader",
    "create_harness_router",
    "router",
]

_logger = logging.getLogger("pulse_system.observatory.harness")

MAX_PAGE = 500
MAX_TURN_ITEM_SOURCE_EVENTS = 4_096
DEFAULT_PAGE = 100
MAX_STREAM_EVENTS = 500
DEFAULT_POLL_INTERVAL = 0.25
DEFAULT_STREAM_SECONDS = 30.0
MAX_ID_LENGTH = 256
MAX_REASON_LENGTH = 1_024
MAX_STEER_LENGTH = 16_384
MAX_PAYLOAD_STRING = 16_384
MAX_DIFF_PREVIEW = 4_096
MAX_PATH_LENGTH = 512
MAX_TERMINAL_SESSIONS = 64
DEFAULT_TERMINAL_SESSIONS = 16
MAX_TERMINAL_OUTPUT = 500
DEFAULT_TERMINAL_OUTPUT = 200
MAX_TERMINAL_OUTPUT_CHUNK = 16_384
MAX_SAFE_WIRE_INTEGER = 9_007_199_254_740_991

_TERMINAL_STATES = {
    "settled",
    "completed",
    "complete",
    "interrupted",
    "cancelled",
    "canceled",
    "failed",
    "uncertain",
    "reconciled",
}

_TERMINAL_SESSION_STATES = {
    "PENDING",
    "RUNNING",
    "EXITED",
    "FAILED",
    "TIMED_OUT",
    "CANCEL_REQUESTED",
    "CANCELLED",
    "KILL_REQUESTED",
    "KILLED",
    "UNCERTAIN",
    "DENIED",
    "UNSUPPORTED",
}

_TERMINAL_SESSION_TERMINAL_STATES = {
    "EXITED",
    "FAILED",
    "TIMED_OUT",
    "CANCELLED",
    "KILLED",
    "UNCERTAIN",
    "DENIED",
    "UNSUPPORTED",
}

_TERMINAL_TREE_CONTAINMENT = {
    "UNVERIFIED",
    "OBSERVED_CLEANUP",
    "JOB_OBJECT_VERIFIED",
}

_HARNESS_EVIDENCE_CLASSES = frozenset(
    {
        "FAKE_RPC_CONTRACT",
        "LIVE_PI_PROVIDER",
        "LIVE_OS_RESTRICTED",
        "LIVE_WORKSPACE_CHECKPOINTED",
        "CONTRACT_ONLY",
        "LIVE_GATE_UNVERIFIED",
    }
)

_SAFE_PAYLOAD_KEYS = {
    "text",
    "reasoning",
    "delta",
    "tool_call_id",
    "tool_name",
    "tool",
    "name",
    "status",
    "command_preview",
    "output",
    "output_preview",
    "exit_code",
    "duration_ms",
    "change_kind",
    "path",
    "move_path",
    "diff",
    "digest",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "total_tokens",
    "usage",
    "approval_id",
    "target_kind",
    "policy_id",
    "decision",
    "expires_at",
    "grant_state",
    "request_id",
    "operation",
    "expected_state",
    "error_code",
    "reason_code",
    "reason",
    "missing_from",
    "missing_to",
    "retained_from",
    "retained_to",
    "subagent_id",
    "worker_state",
    "evidence_class",
    "redacted",
    "truncated",
    "adapter_result",
    "adapter_state",
    "recovery_state",
    "checkpoint_id",
    "manifest_digest",
    "diff_digest",
    "diff_preview",
    "before_digest",
    "after_digest",
    "changed_paths",
    "applied_paths",
    "failed_paths",
}

_SAFE_SUMMARY_KEYS = {
    "turn_id",
    "id",
    "world_id",
    "engram_id",
    "state",
    "status",
    "terminal",
    "terminal_reason",
    "stop_reason",
    "epoch",
    "evidence_class",
    "live_available",
    "recovery",
    "recovery_required",
    "usage",
    "event_cursor",
    "capacity",
    "updated_at",
    "started_at",
    "settled_at",
    "error_code",
}

_SAFE_CAPACITY_KEYS = {
    "max_live_sessions",
    "active_sessions",
    "hibernated_sessions",
    "starting_sessions",
    "busy_sessions",
    "task_workers",
    "event_rows",
    "event_bytes",
    "retained_turns",
    "last_prune_at",
    "worker_limit",
    "worker_running",
    "worker_available",
}

_SECRET_PATTERN = re.compile(
    r"(?i)(bearer\s+|authorization\s*[:=]\s*|api[_-]?key\s*[:=]\s*|"
    r"password\s*[:=]\s*|secret\s*[:=]\s*|token\s*[:=]\s*)([^\s,;]+)"
)
_TOKEN_PATTERN = re.compile(r"(?i)\b(?:sk|pk|ghp|github_pat|xox[baprs])-[A-Za-z0-9_-]{8,}\b")


@runtime_checkable
class HarnessEventReader(Protocol):
    """The small read contract consumed by this router.

    Concrete event-store implementations may expose ``summary``/``replay`` or the
    equivalent ``get_turn_summary``/``list_events`` names.  Returned events
    must already be redacted before they reach this boundary.
    """

    def summary(self, turn_id: str) -> Any: ...

    def replay(self, turn_id: str, after_seq: int = 0, limit: int = MAX_PAGE) -> Any: ...


@runtime_checkable
class HarnessControlGateway(Protocol):
    """Policy-aware sideband control contract consumed by this router."""

    def request_control(self, operation: str, turn_id: str, request: Mapping[str, Any]) -> Any: ...

    def resolve_approval(self, approval_id: str, request: Mapping[str, Any]) -> Any: ...


class _DependencyUnavailable(RuntimeError):
    pass


class _MissingMethod(RuntimeError):
    pass


_MISSING = object()


@dataclass(frozen=True)
class _ReplayPage:
    events: tuple[dict[str, Any], ...]
    next_seq: int
    has_more: bool
    gap: dict[str, Any] | None
    earliest_seq: int | None
    evidence_class: str


def _fault(
    status: int,
    error: str,
    detail: str,
    remedy: str,
    *,
    evidence_class: str = "LIVE_GATE_UNVERIFIED",
) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={
            "error": error,
            "detail": detail,
            "remedy": remedy,
            "evidence_class": _safe_evidence_class(evidence_class),
        },
    )


def _control_fault(
    status: int,
    *,
    request_id: str,
    turn_id: str | None,
    error_code: str,
    detail: str,
    remedy: str,
    evidence_class: str = "LIVE_GATE_UNVERIFIED",
) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={
            "request_id": request_id,
            "turn_id": turn_id,
            "state": "rejected",
            "accepted": False,
            "error_code": error_code,
            "event_seq": None,
            "evidence_class": _safe_evidence_class(evidence_class),
            "error": error_code,
            "detail": detail,
            "remedy": remedy,
        },
    )


def _as_mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if is_dataclass(value):
        return asdict(value)
    for method_name in ("to_public_dict", "as_dict", "to_dict", "model_dump"):
        method = getattr(value, method_name, None)
        if callable(method):
            try:
                candidate = method()
            except Exception:  # noqa: BLE001 - adapter boundary
                continue
            if isinstance(candidate, Mapping):
                return dict(candidate)
    names = (
        "event_id",
        "id",
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
        "payload",
        "payload_json",
        "payload_bytes",
        "payload_digest",
        "redacted",
        "truncated",
        "state",
        "terminal",
        "usage",
        "capacity",
        "event_cursor",
        "evidence_class",
        "gap",
        "has_more",
        "next_seq",
        "earliest_seq",
    )
    return {name: getattr(value, name) for name in names if hasattr(value, name)}


def _field(value: Any, *names: str, default: Any = None) -> Any:
    mapping = value if isinstance(value, Mapping) else None
    for name in names:
        if mapping is not None and name in mapping:
            return mapping[name]
        if mapping is None and hasattr(value, name):
            return getattr(value, name)
    return default


def _enum_text(value: Any, default: str | None = None) -> str | None:
    if value is None:
        return default
    candidate = getattr(value, "value", value)
    if isinstance(candidate, str) and candidate.strip():
        return candidate.strip()[:128]
    return default


def _safe_evidence_class(
    value: Any,
    default: str = "LIVE_GATE_UNVERIFIED",
) -> str:
    """Return only a protocol evidence label; unknown labels never imply LIVE."""

    fallback = (
        default
        if default in _HARNESS_EVIDENCE_CLASSES
        else "LIVE_GATE_UNVERIFIED"
    )
    candidate = _enum_text(value)
    return candidate if candidate in _HARNESS_EVIDENCE_CLASSES else fallback


def _safe_int(value: Any, default: int | None = None) -> int | None:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 10)
        except ValueError:
            return default
    return default


def _safe_float_or_int(value: Any) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value == value and abs(value) != float("inf"):
        return value
    return None


def _safe_text(value: Any, *, limit: int = MAX_PAYLOAD_STRING) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = _SECRET_PATTERN.sub(r"\1[REDACTED]", value)
    value = _TOKEN_PATTERN.sub("[REDACTED]", value)
    if len(value) > limit:
        return value[:limit] + "…"
    return value


def _safe_path(value: Any) -> str | None:
    text = _safe_text(value, limit=MAX_PATH_LENGTH)
    if text is None:
        return None
    normalized = text.replace("\\", "/")
    # The policy layer should normally hand us a workspace-relative path.  This last
    # boundary prevents an accidental absolute path from crossing the API.
    if (
        re.match(r"^[A-Za-z]:/", normalized)
        or normalized.startswith("/")
        or normalized == ".."
        or normalized.startswith("../")
        or "/../" in normalized
    ):
        return "[REDACTED_PATH]"
    try:
        candidate = PurePosixPath(normalized)
    except Exception:  # noqa: BLE001
        return "[REDACTED_PATH]"
    if ".." in candidate.parts:
        return "[REDACTED_PATH]"
    return normalized


def _safe_payload(value: Any, *, depth: int = 0) -> Any:
    """Last-mile allowlist for a payload already redacted by the event and policy layers."""

    if depth > 5:
        return "[TRUNCATED]"
    if isinstance(value, Mapping):
        projected: dict[str, Any] = {}
        for raw_key, child in value.items():
            if not isinstance(raw_key, str):
                continue
            key = raw_key.casefold().replace("-", "_")
            if key in {"prompt", "session", "session_file", "secret", "credential", "authorization"}:
                continue
            if any(part in key for part in ("password", "api_key", "access_token", "refresh_token")):
                continue
            if key not in _SAFE_PAYLOAD_KEYS and not key.endswith("_id"):
                continue
            if key in {"path", "move_path"}:
                safe = _safe_path(child)
            elif key == "evidence_class":
                safe = _safe_evidence_class(child)
            elif key in {"changed_paths", "applied_paths", "failed_paths"} and isinstance(
                child, (list, tuple)
            ):
                safe = [
                    path
                    for item in child[:128]
                    if (path := _safe_path(item)) is not None
                ]
            else:
                safe = _safe_payload(child, depth=depth + 1)
            if safe is not None:
                projected[key] = safe
        return projected
    if isinstance(value, (list, tuple)):
        return [_safe_payload(item, depth=depth + 1) for item in value[:64]]
    if isinstance(value, bool):
        return value
    number = _safe_float_or_int(value)
    if number is not None:
        return number
    if isinstance(value, str):
        return _safe_text(value)
    return None


def _payload_from_event(raw: Mapping[str, Any]) -> Any:
    payload = raw.get("payload", raw.get("payload_json", {}))
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (TypeError, json.JSONDecodeError):
            payload = {"output_preview": payload}
    return payload


def _safe_event(raw: Any, turn_id: str) -> dict[str, Any] | None:
    mapping = _as_mapping(raw)
    seq = _safe_int(mapping.get("seq"))
    if seq is None or seq < 1:
        return None
    event_id = _safe_text(mapping.get("event_id", mapping.get("id")), limit=MAX_ID_LENGTH)
    event = {
        "event_id": event_id or f"{turn_id}:{seq}",
        "turn_id": turn_id,
        "world_id": _safe_text(mapping.get("world_id"), limit=MAX_ID_LENGTH),
        "engram_id": _safe_text(mapping.get("engram_id"), limit=MAX_ID_LENGTH),
        "seq": seq,
        "parent_event_id": _safe_text(mapping.get("parent_event_id"), limit=MAX_ID_LENGTH),
        "kind": _enum_text(mapping.get("kind"), "warning"),
        "phase": _enum_text(mapping.get("phase"), "observe"),
        "source": _enum_text(mapping.get("source"), "pi_rpc"),
        "status": _enum_text(mapping.get("status"), "completed"),
        "occurred_at": _safe_text(mapping.get("occurred_at", mapping.get("created_at")), limit=128),
        "payload": _safe_payload(_payload_from_event(mapping)),
        "payload_bytes": max(_safe_int(mapping.get("payload_bytes"), 0) or 0, 0),
        "payload_digest": _safe_text(mapping.get("payload_digest"), limit=128),
        "redacted": bool(mapping.get("redacted", False)),
        "truncated": bool(mapping.get("truncated", False)),
    }
    return event


def _safe_gap(
    value: Any,
    *,
    after_seq: int,
    earliest_seq: int | None,
    next_seq: int | None,
) -> dict[str, Any] | None:
    if value is None or value is False:
        return None
    raw = {} if value is True else _as_mapping(value)
    missing_from = _safe_int(raw.get("missing_from"), after_seq + 1)
    missing_to = _safe_int(raw.get("missing_to"), (earliest_seq - 1) if earliest_seq else None)
    if missing_from is None:
        missing_from = after_seq + 1
    if missing_to is None or missing_to < missing_from:
        missing_to = missing_from
    return {
        "kind": "event_gap",
        "missing_from": missing_from,
        "missing_to": missing_to,
        "earliest_seq": earliest_seq,
        "next_seq": next_seq,
        "reason": _safe_text(raw.get("reason", "retention_window"), limit=256),
    }


def _normalize_page(value: Any, turn_id: str, after_seq: int) -> _ReplayPage:
    if isinstance(value, (list, tuple)):
        raw: dict[str, Any] = {"events": value}
    else:
        raw = _as_mapping(value)
    raw_events = raw.get("events", raw.get("items", []))
    if not isinstance(raw_events, (list, tuple)):
        raw_events = []
    events = tuple(
        event
        for item in raw_events
        if (event := _safe_event(item, turn_id)) is not None
    )
    first_seq = events[0]["seq"] if events else None
    last_seq = events[-1]["seq"] if events else None
    earliest_seq = _safe_int(raw.get("earliest_seq"))
    if earliest_seq is None:
        earliest_seq = first_seq
    next_seq = _safe_int(raw.get("next_seq"), last_seq or after_seq) or after_seq
    if last_seq is not None:
        next_seq = max(next_seq, last_seq)
    gap = _safe_gap(
        raw.get("gap", raw.get("event_gap")),
        after_seq=after_seq,
        earliest_seq=earliest_seq,
        next_seq=next_seq,
    )
    if gap is None and first_seq is not None and first_seq > after_seq + 1:
        gap = _safe_gap(
            True,
            after_seq=after_seq,
            earliest_seq=first_seq,
            next_seq=next_seq,
        )
    if gap is None:
        previous = after_seq
        for event in events:
            seq = event["seq"]
            if seq > previous + 1:
                gap = _safe_gap(
                    {"missing_from": previous + 1, "missing_to": seq - 1, "reason": "non_contiguous_page"},
                    after_seq=previous,
                    earliest_seq=earliest_seq,
                    next_seq=next_seq,
                )
                break
            previous = seq
    evidence_class = _safe_evidence_class(raw.get("evidence_class"))
    return _ReplayPage(
        events=events,
        next_seq=next_seq,
        has_more=bool(raw.get("has_more", False)),
        gap=gap,
        earliest_seq=earliest_seq,
        evidence_class=evidence_class,
    )


def _safe_usage(value: Any) -> dict[str, int | float]:
    mapping = _as_mapping(value)
    output: dict[str, int | float] = {}
    for key in (
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "total_tokens",
        "input",
        "output",
        "cacheRead",
        "cacheWrite",
    ):
        number = _safe_float_or_int(mapping.get(key))
        if number is not None:
            output[key] = number
    return output


def _safe_capacity(value: Any) -> dict[str, Any]:
    mapping = _as_mapping(value)
    output: dict[str, Any] = {}
    for key in _SAFE_CAPACITY_KEYS:
        child = mapping.get(key)
        if key == "last_prune_at":
            safe = _safe_text(child, limit=128)
        else:
            safe = _safe_float_or_int(child)
        if safe is not None:
            output[key] = safe
    return output


def _safe_cursor(value: Any) -> dict[str, Any]:
    mapping = _as_mapping(value)
    output: dict[str, Any] = {}
    for key in ("first_seq", "last_seq", "next_seq", "after_seq", "event_rows"):
        number = _safe_int(mapping.get(key))
        if number is not None:
            output[key] = max(number, 0)
    if isinstance(mapping.get("has_gap"), bool):
        output["has_gap"] = mapping["has_gap"]
    return output


def _is_terminal(summary: Mapping[str, Any]) -> bool:
    if summary.get("terminal") is True:
        return True
    state = _enum_text(summary.get("state", summary.get("status")), "") or ""
    return state.casefold() in _TERMINAL_STATES


def _safe_summary(raw: Any, turn_id: str, *, evidence_class: str, live_available: bool) -> dict[str, Any]:
    mapping = _as_mapping(raw)
    state = _enum_text(mapping.get("state", mapping.get("status")), "unknown") or "unknown"
    terminal = _is_terminal({"state": state, "terminal": mapping.get("terminal")})
    summary: dict[str, Any] = {
        "turn_id": turn_id,
        "world_id": _safe_text(mapping.get("world_id"), limit=MAX_ID_LENGTH),
        "engram_id": _safe_text(mapping.get("engram_id"), limit=MAX_ID_LENGTH),
        "state": state,
        "terminal": terminal,
        "evidence_class": _safe_evidence_class(
            mapping.get("evidence_class"), evidence_class
        ),
        "live_available": bool(mapping.get("live_available", live_available)),
        "usage": _safe_usage(mapping.get("usage", {})),
        "event_cursor": _safe_cursor(mapping.get("event_cursor", mapping.get("cursor", {}))),
        "capacity": _safe_capacity(mapping.get("capacity", {})),
        "recovery": mapping.get("recovery", mapping.get("recovery_required", False)),
    }
    for key in ("terminal_reason", "stop_reason", "updated_at", "started_at", "settled_at", "error_code"):
        safe = _safe_text(mapping.get(key), limit=512)
        if safe is not None:
            summary[key] = safe
    epoch = _safe_int(mapping.get("epoch"))
    if epoch is not None:
        summary["epoch"] = epoch
    if not summary["event_cursor"]:
        summary["event_cursor"] = {}
    return summary


def _safe_subagent(value: Any, task_id: str) -> dict[str, Any]:
    mapping = _as_mapping(value)
    output: dict[str, Any] = {
        "task_id": task_id,
        "state": _enum_text(mapping.get("state", mapping.get("status")), "unknown"),
        "evidence_class": _safe_evidence_class(mapping.get("evidence_class")),
    }
    for key in ("subagent_id", "worker_state", "started_at", "updated_at", "finished_at", "error_code"):
        safe = _safe_text(mapping.get(key), limit=256)
        if safe is not None:
            output[key] = safe
    return output


def _safe_checkpoint(value: Any) -> dict[str, Any] | None:
    """Project a durable checkpoint without exposing host paths or content."""

    mapping = _as_mapping(value)
    checkpoint_id = _safe_text(
        mapping.get("checkpoint_id", mapping.get("id")), limit=MAX_ID_LENGTH
    )
    if checkpoint_id is None:
        return None
    output: dict[str, Any] = {
        "checkpoint_id": checkpoint_id,
        "state": _enum_text(mapping.get("state", mapping.get("status")), "unknown"),
        "evidence_class": _safe_evidence_class(mapping.get("evidence_class")),
    }
    for key in (
        "turn_id",
        "world_id",
        "engram_id",
        "created_at",
        "updated_at",
        "error_code",
        "reason",
        "manifest_digest",
        "diff_digest",
    ):
        safe = _safe_text(mapping.get(key), limit=512)
        if safe is not None:
            output[key] = safe
    diff_preview = _safe_text(
        mapping.get("diff_preview"), limit=MAX_DIFF_PREVIEW
    )
    if diff_preview is not None:
        output["diff_preview"] = diff_preview
    epoch = _safe_int(mapping.get("epoch"))
    if epoch is not None and epoch >= 0:
        output["epoch"] = epoch
    for key in ("changed_paths", "applied_paths", "failed_paths"):
        candidate = mapping.get(key)
        if isinstance(candidate, (list, tuple)):
            output[key] = [
                path
                for item in candidate[:128]
                if (path := _safe_path(item)) is not None
            ]
    restored = mapping.get("restored_paths")
    if isinstance(restored, (list, tuple)):
        output["restored_paths"] = [
            path
            for item in restored[:128]
            if (path := _safe_path(item)) is not None
        ]
    output["reconciled_from_uncertain"] = bool(
        mapping.get("reconciled_from_uncertain", False)
    )
    output["uncertain"] = bool(
        mapping.get("uncertain", str(output["state"]).casefold() == "uncertain")
    )
    output["idempotent"] = bool(mapping.get("idempotent", False))
    return output


def _safe_identifier(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate or len(candidate) > MAX_ID_LENGTH:
        return None
    if _TOKEN_PATTERN.search(candidate):
        return None
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]*", candidate) is None:
        return None
    return candidate


def _safe_wire_count(value: Any, default: int = 0) -> int:
    parsed = _safe_int(value, default)
    if parsed is None:
        parsed = default
    return min(max(parsed, 0), MAX_SAFE_WIRE_INTEGER)


def _safe_digest(value: Any, *, required: bool) -> str | None:
    candidate = _safe_text(value, limit=128)
    if candidate is None:
        return "[REDACTED]" if required else None
    if re.fullmatch(r"[A-Fa-f0-9]{64}", candidate) is None:
        return "[REDACTED]"
    return candidate.lower()


def _safe_terminal_summary(
    value: Any,
    *,
    expected_session_id: str | None = None,
) -> dict[str, Any] | None:
    """Strictly project one PIPE_SESSION without exposing process internals."""

    mapping = _as_mapping(value)
    nested = mapping.get("session", mapping.get("summary"))
    if nested is not None:
        value = nested
        mapping = _as_mapping(value)

    session_id = _safe_identifier(
        _field(value, "terminal_session_id", "session_id", "id")
    )
    if session_id is None:
        return None
    if expected_session_id is not None and session_id != expected_session_id:
        return None

    raw_mode = _enum_text(_field(value, "mode"), "PIPE_SESSION")
    raw_transport = _enum_text(_field(value, "transport"), "pipe")
    raw_scope = _enum_text(
        _field(value, "session_scope"), "runtime_connection"
    )
    if raw_mode != "PIPE_SESSION" or raw_transport != "pipe":
        return None
    if raw_scope != "runtime_connection":
        return None

    raw_state = (
        _enum_text(_field(value, "state", "status"), "UNCERTAIN") or "UNCERTAIN"
    ).upper()
    state = raw_state if raw_state in _TERMINAL_SESSION_STATES else "UNCERTAIN"

    cwd_relative = _safe_path(_field(value, "cwd_relative", "cwd", default="."))
    if cwd_relative in {None, "", "[REDACTED_PATH]"}:
        cwd_relative = "[REDACTED]"

    epoch = _safe_int(_field(value, "epoch"))
    if epoch is None or epoch < 0:
        epoch = 0

    tree_containment = (
        _enum_text(_field(value, "tree_containment"), "UNVERIFIED")
        or "UNVERIFIED"
    ).upper()
    if tree_containment not in _TERMINAL_TREE_CONTAINMENT:
        tree_containment = "UNVERIFIED"

    summary: dict[str, Any] = {
        "terminal_session_id": session_id,
        "turn_id": _safe_identifier(_field(value, "turn_id")),
        "world_id": _safe_identifier(_field(value, "world_id")),
        "engram_id": _safe_identifier(_field(value, "engram_id")),
        "epoch": epoch,
        "mode": "PIPE_SESSION",
        "transport": "pipe",
        "session_scope": "runtime_connection",
        "state": state,
        "cwd_relative": cwd_relative,
        "command_digest": _safe_digest(
            _field(value, "command_digest"), required=True
        ),
        "started_at": _safe_text(_field(value, "started_at"), limit=128),
        "ended_at": _safe_text(_field(value, "ended_at"), limit=128),
        "exit_code": _safe_int(_field(value, "exit_code")),
        "output_bytes": _safe_wire_count(_field(value, "output_bytes")),
        "output_truncated": bool(_field(value, "output_truncated", default=False)),
        "last_output_seq": _safe_wire_count(_field(value, "last_output_seq")),
        "launch_action_digest": _safe_digest(
            _field(value, "launch_action_digest"), required=False
        ),
        "evidence_class": _safe_evidence_class(
            _field(value, "evidence_class")
        ),
        "sandbox_evidence": _safe_evidence_class(
            _field(value, "sandbox_evidence")
        ),
        "tree_containment": tree_containment,
        "error_code": _safe_text(_field(value, "error_code"), limit=128),
        "uncertain_reason": _safe_text(
            _field(value, "uncertain_reason"), limit=512
        ),
        # v1 is deliberately non-interactive.  Never infer these flags from
        # a backend object or allow a future private capability to leak early.
        "capabilities": {
            "stdin": False,
            "resize": False,
            "reconnect": True,
            "stop": True,
        },
    }
    return summary


def _safe_terminal_output_chunk(
    value: Any,
    *,
    terminal_session_id: str,
) -> dict[str, Any] | None:
    chunk_session_id = _safe_identifier(_field(value, "terminal_session_id"))
    if chunk_session_id != terminal_session_id:
        return None
    seq = _safe_int(_field(value, "seq"))
    if seq is None or seq < 1:
        return None
    stream = (_enum_text(_field(value, "stream"), "") or "").casefold()
    if stream not in {"stdout", "stderr"}:
        return None
    raw_text = _field(value, "text", "chunk")
    if not isinstance(raw_text, str):
        return None
    text = _safe_text(raw_text, limit=MAX_TERMINAL_OUTPUT_CHUNK)
    if text is None:
        text = ""
    return {
        "terminal_session_id": terminal_session_id,
        "seq": seq,
        "stream": stream,
        "text": text,
        "byte_count": _safe_wire_count(_field(value, "byte_count", "bytes")),
        "truncated": bool(_field(value, "truncated", default=False)),
        "redacted": bool(_field(value, "redacted", default=False)),
        "at": _safe_text(_field(value, "at", "occurred_at"), limit=128),
    }


def _safe_terminal_output_gap(
    value: Any,
    *,
    after_seq: int,
    earliest_seq: int | None,
) -> dict[str, Any] | None:
    if value is None or value is False:
        return None
    mapping = {} if value is True else _as_mapping(value)
    missing_from = _safe_int(mapping.get("missing_from"), after_seq + 1)
    missing_to = _safe_int(
        mapping.get("missing_to"),
        (earliest_seq - 1) if earliest_seq is not None else None,
    )
    if missing_from is None:
        missing_from = after_seq + 1
    if missing_to is None or missing_to < missing_from:
        missing_to = missing_from
    return {
        "missing_from": missing_from,
        "missing_to": missing_to,
        "reason": _safe_text(
            mapping.get("reason", "retention_window"), limit=256
        )
        or "retention_window",
    }


def _terminal_output_gap_covers(
    gap: dict[str, Any] | None,
    *,
    missing_from: int,
    missing_to: int,
) -> bool:
    return bool(
        gap is not None
        and gap["missing_from"] <= missing_from
        and gap["missing_to"] >= missing_to
    )


def _safe_terminal_output_page(
    value: Any,
    *,
    terminal_session_id: str,
    after_seq: int,
    limit: int,
) -> dict[str, Any] | None:
    mapping = _as_mapping(value)
    raw_session_id = _safe_identifier(mapping.get("terminal_session_id"))
    if raw_session_id != terminal_session_id:
        return None
    if "output" not in mapping:
        return None
    source = mapping.get("output")
    if not isinstance(source, (list, tuple)):
        return None
    all_chunks: list[dict[str, Any]] = []
    previous_seq: int | None = None
    for item in source:
        if _safe_identifier(
            _field(item, "terminal_session_id")
        ) != terminal_session_id:
            return None
        chunk = _safe_terminal_output_chunk(
            item, terminal_session_id=terminal_session_id
        )
        if chunk is None:
            return None
        if chunk["seq"] <= after_seq:
            return None
        if previous_seq is not None and chunk["seq"] <= previous_seq:
            return None
        previous_seq = chunk["seq"]
        all_chunks.append(chunk)
    projected = [chunk for chunk in all_chunks if chunk["seq"] > after_seq]
    overflow = len(projected) > limit
    output = projected[:limit]
    earliest_seq = _safe_int(mapping.get("earliest_seq"))
    if earliest_seq is None and all_chunks:
        earliest_seq = all_chunks[0]["seq"]
    gap = _safe_terminal_output_gap(
        mapping.get("gap"), after_seq=after_seq, earliest_seq=earliest_seq
    )
    if earliest_seq is not None:
        if any(chunk["seq"] < earliest_seq for chunk in all_chunks):
            return None
        if projected and earliest_seq > projected[0]["seq"]:
            return None
    if gap is not None:
        if gap["missing_from"] < after_seq + 1:
            return None
        if any(
            gap["missing_from"] <= chunk["seq"] <= gap["missing_to"]
            for chunk in all_chunks
        ):
            return None
    if projected:
        first_seq = projected[0]["seq"]
        if first_seq > after_seq + 1 and not _terminal_output_gap_covers(
            gap,
            missing_from=after_seq + 1,
            missing_to=first_seq - 1,
        ):
            return None
        for previous, current in zip(projected, projected[1:]):
            if current["seq"] > previous["seq"] + 1 and not _terminal_output_gap_covers(
                gap,
                missing_from=previous["seq"] + 1,
                missing_to=current["seq"] - 1,
            ):
                return None
    elif (
        earliest_seq is not None
        and earliest_seq > after_seq + 1
        and not _terminal_output_gap_covers(
            gap,
            missing_from=after_seq + 1,
            missing_to=earliest_seq - 1,
        )
    ):
        return None
    next_seq = output[-1]["seq"] if output else after_seq
    advertised_next_seq = _safe_int(mapping.get("next_seq"))
    if advertised_next_seq is None or advertised_next_seq != next_seq:
        return None
    return {
        "terminal_session_id": terminal_session_id,
        "output": output,
        "earliest_seq": earliest_seq,
        "next_seq": advertised_next_seq,
        "has_more": bool(mapping.get("has_more", False)) or overflow,
        "gap": gap,
        "truncated": bool(mapping.get("truncated", False))
        or any(chunk["truncated"] for chunk in output),
        "evidence_class": _safe_evidence_class(mapping.get("evidence_class")),
    }


def _parse_nonnegative(raw: str | None, *, name: str, default: int = 0) -> int | JSONResponse:
    if raw is None or raw == "":
        return default
    try:
        value = int(raw, 10)
    except (TypeError, ValueError):
        return _fault(400, "invalid_cursor", f"{name} must be a non-negative integer", f"provide {name}=0 or a later cursor")
    if value < 0:
        return _fault(400, "invalid_cursor", f"{name} must be a non-negative integer", f"provide {name}=0 or a later cursor")
    return value


def _parse_limit(raw: str | None, *, default: int = DEFAULT_PAGE) -> int | JSONResponse:
    parsed = _parse_nonnegative(raw, name="limit", default=default)
    if isinstance(parsed, JSONResponse):
        return parsed
    if parsed < 1 or parsed > MAX_PAGE:
        return _fault(400, "invalid_limit", f"limit must be between 1 and {MAX_PAGE}", f"provide limit in the range 1..{MAX_PAGE}")
    return parsed


def _parse_terminal_limit(
    raw: str | None,
    *,
    default: int,
    maximum: int,
) -> int | JSONResponse:
    parsed = _parse_nonnegative(raw, name="limit", default=default)
    if isinstance(parsed, JSONResponse):
        return parsed
    if parsed < 1 or parsed > maximum:
        return _fault(
            400,
            "invalid_limit",
            f"limit must be between 1 and {maximum}",
            f"provide limit in the range 1..{maximum}",
        )
    return parsed


def _parse_bool(raw: str | None, *, name: str) -> bool | JSONResponse:
    if raw is None or raw == "":
        return False
    if raw.casefold() in {"1", "true", "yes"}:
        return True
    if raw.casefold() in {"0", "false", "no"}:
        return False
    return _fault(400, "invalid_parameter", f"{name} must be boolean", f"provide {name}=1 or {name}=0")


def _unknown_query(request: Request, allowed: set[str]) -> JSONResponse | None:
    unknown = sorted(set(request.query_params) - allowed)
    if unknown:
        return _fault(
            400,
            "unknown_filter",
            f"unknown Harness query parameter(s): {', '.join(unknown)}",
            "remove unknown parameters and use after_seq, limit, or once",
        )
    return None


def _parse_last_event_id(raw: str | None, turn_id: str) -> int | None | JSONResponse:
    if raw is None or raw == "":
        return None
    value = raw
    if ":" in value:
        prefix, value = value.rsplit(":", 1)
        if prefix != turn_id:
            return _fault(400, "invalid_cursor", "Last-Event-ID belongs to another turn", "reconnect with the cursor for this turn")
    try:
        parsed = int(value, 10)
    except (TypeError, ValueError):
        return _fault(400, "invalid_cursor", "Last-Event-ID must end in a non-negative sequence", "reconnect with turn_id:seq")
    if parsed < 0:
        return _fault(400, "invalid_cursor", "Last-Event-ID must be non-negative", "reconnect with turn_id:seq")
    return parsed


def _select_method(target: Any, names: tuple[str, ...]) -> Any:
    for name in names:
        method = getattr(target, name, None)
        if callable(method):
            return method
    raise _MissingMethod(f"none of {names} is implemented")


def _call_method(method: Any, kwargs: dict[str, Any]) -> Any:
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        return method(**kwargs)
    parameters = signature.parameters
    if any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()):
        return method(**kwargs)
    selected = {key: value for key, value in kwargs.items() if key in parameters}
    return method(**selected)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _call_target(target: Any, names: tuple[str, ...], kwargs: dict[str, Any]) -> Any:
    try:
        method = _select_method(target, names)
        # Control endpoints must never execute a synchronous adapter on the
        # FastAPI event loop.  Runtime's approval broker normally returns
        # immediately after queuing bounded work; the thread hop is still the
        # fail-safe for custom gateways and keeps SSE/summary/cancel responsive
        # if one of them violates that expectation.
        if inspect.iscoroutinefunction(method):
            value = _call_method(method, kwargs)
        else:
            value = await asyncio.to_thread(_call_method, method, kwargs)
        return await _maybe_await(value)
    except _MissingMethod:
        raise
    except KeyError:
        return None


def _dependency(request: Request, explicit: Any, runtime: Any, names: tuple[str, ...]) -> Any | None:
    if explicit is not None:
        return explicit
    candidates = [
        getattr(request.app.state, name, None) for name in names
    ]
    if runtime is not None:
        candidates.extend(getattr(runtime, name, None) for name in names)
    return next((candidate for candidate in candidates if candidate is not None), None)


def _safe_gateway_error(exc: Exception) -> tuple[int, str, str, str]:
    exception_name = type(exc).__name__
    inferred = {
        "TerminalSessionConflictError": (409, "terminal_session_conflict"),
        "TerminalSessionLeaseError": (409, "stale_epoch"),
        "TerminalSessionNotFoundError": (404, "terminal_session_not_found"),
    }.get(exception_name, (503, "control_unavailable"))
    status = _safe_int(
        getattr(exc, "status", getattr(exc, "http_status", None)), inferred[0]
    ) or inferred[0]
    if status not in {400, 403, 404, 409, 410, 422, 503}:
        status = 503
    error_code = _safe_text(
        getattr(exc, "code", getattr(exc, "error_code", None)), limit=128
    ) or inferred[1]
    detail = _safe_text(getattr(exc, "detail", None), limit=512) or "the Harness control gateway did not accept the request"
    remedy = _safe_text(getattr(exc, "remedy", None), limit=512) or "refresh the turn summary and retry only when the live gateway is available"
    return status, error_code, detail, remedy


def _control_outcome(
    value: Any,
    *,
    request_id: str,
    turn_id: str | None,
    evidence_class: str,
) -> tuple[dict[str, Any], int]:
    mapping = _as_mapping(value)
    if isinstance(value, bool):
        mapping = {"accepted": value}
    accepted = mapping.get("accepted", mapping.get("ok", False)) is True
    state = _enum_text(mapping.get("state"), "accepted" if accepted else "rejected") or "rejected"
    error_code = _safe_text(mapping.get("error_code", mapping.get("error")), limit=128)
    event_seq = _safe_int(mapping.get("event_seq"))
    result = {
        "request_id": request_id,
        "turn_id": turn_id,
        "state": state,
        "accepted": accepted,
        "error_code": error_code,
        "event_seq": event_seq,
        "evidence_class": _safe_evidence_class(
            mapping.get("evidence_class"), evidence_class
        ),
        "uncertain": bool(mapping.get("uncertain", state.casefold() == "uncertain")),
        "idempotent": bool(mapping.get("idempotent", False)),
    }
    for key in (
        "approval_id",
        "action_request_id",
        "approval_accepted",
        "execution_status",
    ):
        if key in mapping:
            value = mapping[key]
            if key in {"approval_accepted"}:
                if isinstance(value, bool):
                    result[key] = value
            elif key in {"approval_id", "action_request_id", "execution_status"}:
                safe_value = _safe_text(value, limit=MAX_ID_LENGTH)
                if safe_value is not None:
                    result[key] = safe_value
    if accepted:
        http_status = 200 if result["idempotent"] or state.casefold() in _TERMINAL_STATES else 202
    else:
        http_status = {
            "stale_epoch": 409,
            "stale_turn": 409,
            "terminal_turn": 410,
            "approval_expired": 410,
            "harness_unavailable": 503,
            "control_unavailable": 503,
            "sandbox_backend_unavailable": 503,
            "sandbox_preflight_failed": 503,
            "sandbox_runtime_unavailable": 503,
            "sandbox_spawn_failed": 503,
            "approval_backend_unavailable": 503,
            "policy_denied": 403,
        }.get(error_code, 409)
    return result, http_status


async def _read_page(store: Any, turn_id: str, after_seq: int, limit: int) -> _ReplayPage:
    try:
        raw = await _call_target(
            store,
            ("replay", "list_events", "read_events", "events"),
            {
                "turn_id": turn_id,
                "after_seq": after_seq,
                "cursor": after_seq,
                "limit": limit,
                "page_size": limit,
            },
        )
    except _MissingMethod as exc:
        raise _DependencyUnavailable("the event reader has no replay method") from exc
    return _normalize_page(raw, turn_id, after_seq)


async def _read_summary(store: Any, turn_id: str) -> Any:
    try:
        return await _call_target(
            store,
            ("summary", "get_turn_summary", "get_summary", "get_turn"),
            {"turn_id": turn_id, "id": turn_id},
        )
    except _MissingMethod:
        return _MISSING


async def _summary_for(
    store: Any,
    turn_id: str,
    *,
    evidence_class: str,
    live_available: bool,
) -> dict[str, Any] | None:
    raw = await _read_summary(store, turn_id)
    if raw is _MISSING:
        page = await _read_page(store, turn_id, 0, MAX_PAGE)
        if not page.events and page.gap is None and page.next_seq == 0:
            return None
        first = page.events[0] if page.events else {}
        last = page.events[-1] if page.events else {}
        raw = {
            "turn_id": turn_id,
            "world_id": first.get("world_id"),
            "engram_id": first.get("engram_id"),
            "state": last.get("status", "unknown"),
            "terminal": last.get("kind") == "turn_terminal",
            "event_cursor": {
                "first_seq": first.get("seq"),
                "last_seq": last.get("seq"),
                "next_seq": page.next_seq,
                "has_gap": page.gap is not None,
            },
        }
        evidence_class = page.evidence_class
    if raw is None:
        return None
    return _safe_summary(raw, turn_id, evidence_class=evidence_class, live_available=live_available)


async def _capacity_for(store: Any | None, gateway: Any | None, *, world_id: str | None) -> dict[str, Any]:
    for target in (store, gateway):
        if target is None:
            continue
        try:
            raw = await _call_target(
                target,
                ("capacity_snapshot", "capacity", "snapshot_capacity"),
                {"world_id": world_id},
            )
        except _MissingMethod:
            continue
        if raw is not None:
            return _safe_capacity(raw)
    return {}


def _control_kwargs(
    *,
    operation: str,
    turn_id: str,
    request_payload: Mapping[str, Any],
    world_id: str | None,
) -> dict[str, Any]:
    return {
        "operation": operation,
        "turn_id": turn_id,
        "request": request_payload,
        "body": request_payload,
        "payload": request_payload,
        "control_request": request_payload,
        "world_id": world_id,
        "expected_epoch": request_payload.get("expected_epoch"),
        "expected_state": request_payload.get("expected_state"),
        "request_id": request_payload.get("request_id"),
        "reason": request_payload.get("reason"),
        "message": request_payload.get("message"),
    }


def _validate_common_control(
    body: Any,
    *,
    allowed: set[str],
    request_id_required: bool = True,
) -> tuple[dict[str, Any] | None, JSONResponse | None]:
    if not isinstance(body, dict):
        return None, _fault(400, "invalid_body", "control body must be a JSON object", "send the documented structured control fields")
    unknown = sorted(set(body) - allowed)
    if unknown:
        return None, _fault(400, "unknown_control_field", f"unknown control field(s): {', '.join(unknown)}", "remove fields outside the Harness control contract")
    request_id = body.get("request_id")
    if request_id_required and (not isinstance(request_id, str) or not request_id.strip() or len(request_id) > MAX_ID_LENGTH):
        return None, _fault(400, "invalid_request_id", "request_id is required and must be a bounded string", "send a stable request_id for idempotent retry")
    expected_epoch = body.get("expected_epoch")
    if isinstance(expected_epoch, bool) or not isinstance(expected_epoch, int) or expected_epoch < 0:
        return None, _fault(400, "invalid_epoch", "expected_epoch is required and must be a non-negative integer", "read the current turn summary and resend its epoch")
    expected_state = body.get("expected_state")
    if expected_state is not None and (not isinstance(expected_state, str) or len(expected_state) > 128):
        return None, _fault(400, "invalid_state", "expected_state must be a bounded string", "omit expected_state or use the state from the turn summary")
    payload = dict(body)
    payload["request_id"] = request_id.strip()
    return payload, None


def _control_method_names(operation: str) -> tuple[str, ...]:
    if operation == "interrupt":
        return ("request_control", "interrupt", "abort")
    if operation == "steer":
        return ("request_control", "steer")
    return ("resolve_approval", "approve", "resolve")


def _sse(event_name: str, payload: Mapping[str, Any], event_id: str | None = None) -> str:
    identity = "" if event_id is None else f"id: {event_id}\n"
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return f"{identity}event: {event_name}\ndata: {data}\n\n"


def create_harness_router(
    event_store: Any | None = None,
    control_gateway: Any | None = None,
    *,
    runtime: Any | None = None,
    world_id: str | None = None,
    prefix: str = "/harness",
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    stream_seconds: float = DEFAULT_STREAM_SECONDS,
    max_stream_events: int = MAX_STREAM_EVENTS,
) -> APIRouter:
    """Create the bounded Harness observation/control router.

    ``event_store`` may be replay-only.  ``control_gateway`` is never inferred
    from a generic runtime method: it must be a policy-aware sideband adapter.
    This keeps a read-only replay server from accidentally accepting a button
    click and pretending that a live Pi session changed state.
    """

    if not prefix.startswith("/"):
        prefix = "/" + prefix
    sleep_interval = max(float(poll_interval), 0.05)
    connection_event_cap = max(1, min(int(max_stream_events), MAX_STREAM_EVENTS))
    connection_seconds = max(float(stream_seconds), 0.1)
    router = APIRouter(prefix=prefix, tags=["harness"])

    def resolve_store(request: Request) -> Any | None:
        return _dependency(
            request,
            event_store,
            runtime,
            ("harness_event_store", "event_store", "harness_events"),
        )

    def resolve_gateway(request: Request) -> Any | None:
        return _dependency(
            request,
            control_gateway,
            runtime,
            ("harness_control_gateway", "control_gateway", "harness_control"),
        )

    def resolve_world(request: Request) -> str | None:
        if world_id is not None:
            return world_id
        for target in (runtime,):
            candidate = getattr(target, "world_id", None) if target is not None else None
            if isinstance(candidate, str) and candidate:
                return candidate
        candidate = getattr(request.app.state, "world_id", None)
        return candidate if isinstance(candidate, str) and candidate else None

    @router.get("/turns/{turn_id}")
    async def turn_summary(turn_id: str, request: Request) -> JSONResponse:
        store = resolve_store(request)
        if store is None:
            return _fault(
                503,
                "harness_unavailable",
                "no Harness event reader is attached to this observatory",
                "mount create_harness_router with a redacted event store",
            )
        gateway = resolve_gateway(request)
        try:
            summary = await _summary_for(
                store,
                turn_id,
                evidence_class="LIVE_GATE_UNVERIFIED",
                live_available=gateway is not None,
            )
        except _DependencyUnavailable:
            return _fault(503, "harness_unavailable", "the Harness event reader is not replayable", "attach the durable event reader and retry")
        except Exception as exc:  # noqa: BLE001 - bounded API boundary
            _logger.warning("Harness summary read failed: %s", exc)
            return _fault(503, "harness_unavailable", "the Harness turn summary is temporarily unavailable", "retry after the event reader is readable")
        if summary is None:
            return _fault(404, "turn_not_found", f"Harness turn {turn_id!r} was not found", "use a turn_id returned by the Harness turn API")
        capacity = await _capacity_for(store, gateway, world_id=resolve_world(request))
        if capacity:
            summary["capacity"] = capacity
        return JSONResponse(summary)

    @router.get("/turns/{turn_id}/events")
    async def turn_events(
        turn_id: str,
        request: Request,
        after_seq: str | None = None,
        limit: str | None = None,
    ) -> JSONResponse:
        unknown = _unknown_query(request, {"after_seq", "limit"})
        if unknown is not None:
            return unknown
        parsed_after = _parse_nonnegative(after_seq, name="after_seq")
        parsed_limit = _parse_limit(limit)
        if isinstance(parsed_after, JSONResponse):
            return parsed_after
        if isinstance(parsed_limit, JSONResponse):
            return parsed_limit
        store = resolve_store(request)
        if store is None:
            return _fault(503, "harness_unavailable", "no Harness event reader is attached to this observatory", "mount the router with a redacted event store")
        try:
            page = await _read_page(store, turn_id, parsed_after, parsed_limit)
        except _DependencyUnavailable:
            return _fault(503, "harness_unavailable", "the Harness event reader is not replayable", "attach the durable event reader and retry")
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Harness event replay failed: %s", exc)
            return _fault(503, "harness_unavailable", "the Harness event projection is temporarily unavailable", "retry after the event reader is readable")
        return JSONResponse(
            {
                "events": list(page.events),
                "next_seq": page.next_seq,
                "has_more": page.has_more,
                "gap": page.gap,
                "earliest_seq": page.earliest_seq,
                "evidence_class": page.evidence_class,
            }
        )

    @router.get("/turns/{turn_id}/items")
    async def turn_items(turn_id: str, request: Request) -> JSONResponse:
        """Project the bounded durable event window into Codex-like items."""

        unknown = _unknown_query(request, set())
        if unknown is not None:
            return unknown
        store = resolve_store(request)
        if store is None:
            return _fault(
                503,
                "harness_unavailable",
                "no Harness event reader is attached to this observatory",
                "mount the router with a redacted event store",
            )
        try:
            source_events: list[dict[str, Any]] = []
            gaps: list[dict[str, Any]] = []
            cursor = 0
            page = await _read_page(store, turn_id, cursor, MAX_PAGE)
            while True:
                source_events.extend(page.events)
                if page.gap is not None:
                    gaps.append(
                        {
                            "from_seq": page.gap["missing_from"],
                            "to_seq": page.gap["missing_to"],
                            "reason": page.gap.get("reason") or "retention_window",
                        }
                    )
                if not page.has_more or len(source_events) >= MAX_TURN_ITEM_SOURCE_EVENTS:
                    break
                next_cursor = page.next_seq
                if next_cursor <= cursor or not page.events:
                    break
                cursor = next_cursor
                page = await _read_page(
                    store,
                    turn_id,
                    cursor,
                    min(MAX_PAGE, MAX_TURN_ITEM_SOURCE_EVENTS - len(source_events)),
                )
            projection = project_turn_items(
                source_events,
                turn_id=turn_id,
                gaps=gaps,
                max_replay_items=MAX_TURN_ITEM_SOURCE_EVENTS,
            ).to_dict()
        except _DependencyUnavailable:
            return _fault(
                503,
                "harness_unavailable",
                "the Harness event reader is not replayable",
                "attach the durable event reader and retry",
            )
        except Exception as exc:  # noqa: BLE001 - bounded projection boundary
            _logger.warning("Harness turn-item projection failed: %s", exc)
            return _fault(
                503,
                "turn_items_unavailable",
                "the Harness turn-item projection is temporarily unavailable",
                "read the event replay directly or retry after the projection is readable",
            )
        projection["source_evidence_class"] = page.evidence_class
        projection["event_next_seq"] = page.next_seq
        projection["event_has_more"] = page.has_more
        return JSONResponse(projection)

    @router.get("/turns/{turn_id}/terminal-sessions")
    async def terminal_sessions_for_turn(
        turn_id: str,
        request: Request,
        limit: str | None = None,
    ) -> JSONResponse:
        unknown = _unknown_query(request, {"limit"})
        if unknown is not None:
            return unknown
        safe_turn_id = _safe_identifier(turn_id)
        if safe_turn_id is None:
            return _fault(
                400,
                "invalid_turn_id",
                "turn_id must be a bounded opaque identifier",
                "use a turn_id returned by the Harness turn API",
            )
        parsed_limit = _parse_terminal_limit(
            limit,
            default=DEFAULT_TERMINAL_SESSIONS,
            maximum=MAX_TERMINAL_SESSIONS,
        )
        if isinstance(parsed_limit, JSONResponse):
            return parsed_limit
        gateway = resolve_gateway(request)
        if gateway is None:
            return _fault(
                503,
                "terminal_sessions_unavailable",
                "no live terminal session reader is attached",
                "attach the durable terminal session gateway and retry",
            )
        try:
            raw = await _call_target(
                gateway,
                (
                    "list_terminal_sessions",
                    "terminal_sessions_for_turn",
                    "list_for_turn",
                ),
                {
                    "turn_id": safe_turn_id,
                    "limit": parsed_limit,
                    "world_id": resolve_world(request),
                },
            )
        except _MissingMethod:
            return _fault(
                503,
                "terminal_sessions_unavailable",
                "the live gateway does not implement terminal session listing",
                "attach the harness.terminal-sessions.v1 read gateway",
            )
        except Exception as exc:  # noqa: BLE001 - bounded gateway boundary
            status, error_code, detail, remedy = _safe_gateway_error(exc)
            return _fault(status, error_code, detail, remedy)
        if raw is None:
            return _fault(
                404,
                "turn_not_found",
                f"Harness turn {safe_turn_id!r} was not found",
                "use a turn_id returned by the Harness turn API",
            )
        mapping = _as_mapping(raw)
        if mapping.get("turn_known") is False:
            return _fault(
                404,
                "turn_not_found",
                f"Harness turn {safe_turn_id!r} was not found",
                "use a turn_id returned by the Harness turn API",
            )
        envelope_turn_id = mapping.get("turn_id")
        if envelope_turn_id is not None and _safe_identifier(
            envelope_turn_id
        ) != safe_turn_id:
            return _fault(
                503,
                "terminal_session_projection_invalid",
                "the durable terminal session gateway returned another turn scope",
                "repair the gateway identity projection before retrying",
            )
        source = raw if isinstance(raw, (list, tuple)) else mapping.get(
            "sessions", mapping.get("items", [])
        )
        if not isinstance(source, (list, tuple)):
            return _fault(
                503,
                "terminal_session_projection_invalid",
                "the durable terminal session gateway returned an invalid session list",
                "repair the harness.terminal-sessions.v1 projection before retrying",
            )
        sessions: list[dict[str, Any]] = []
        seen_session_ids: set[str] = set()
        for item in source:
            summary = _safe_terminal_summary(item)
            if summary is None or summary.get("turn_id") != safe_turn_id:
                return _fault(
                    503,
                    "terminal_session_projection_invalid",
                    "the durable terminal session gateway returned an invalid or cross-turn session",
                    "repair the gateway identity projection before retrying",
                )
            session_id = summary["terminal_session_id"]
            if session_id in seen_session_ids:
                return _fault(
                    503,
                    "terminal_session_projection_invalid",
                    "the durable terminal session gateway returned a duplicate session identity",
                    "repair the gateway identity projection before retrying",
                )
            seen_session_ids.add(session_id)
            sessions.append(summary)
        sessions.sort(key=lambda item: item.get("started_at") or "")
        sessions = sessions[:parsed_limit]
        return JSONResponse(
            {
                "sessions": sessions,
                "count": len(sessions),
                "evidence_class": _safe_evidence_class(
                    mapping.get("evidence_class")
                ),
            }
        )

    @router.get("/terminal-sessions/{terminal_session_id}")
    async def inspect_terminal_session(
        terminal_session_id: str,
        request: Request,
        turn_id: str | None = None,
    ) -> JSONResponse:
        unknown = _unknown_query(request, {"turn_id"})
        if unknown is not None:
            return unknown
        safe_session_id = _safe_identifier(terminal_session_id)
        if safe_session_id is None:
            return _fault(
                400,
                "invalid_terminal_session_id",
                "terminal_session_id must be a bounded opaque identifier",
                "use an id returned by the terminal session list",
            )
        safe_turn_id = _safe_identifier(turn_id)
        if safe_turn_id is None:
            return _fault(
                400,
                "invalid_turn_id",
                "terminal session inspection requires its owning turn_id",
                "use the turn_id from the scoped terminal session list",
            )
        gateway = resolve_gateway(request)
        if gateway is None:
            return _fault(
                503,
                "terminal_sessions_unavailable",
                "no live terminal session reader is attached",
                "attach the durable terminal session gateway and retry",
            )
        try:
            raw = await _call_target(
                gateway,
                (
                    "inspect_terminal_session",
                    "get_terminal_session",
                    "inspect",
                ),
                {
                    "terminal_session_id": safe_session_id,
                    "session_id": safe_session_id,
                    "expected_turn_id": safe_turn_id,
                },
            )
        except _MissingMethod:
            return _fault(
                503,
                "terminal_sessions_unavailable",
                "the live gateway does not implement terminal session inspection",
                "attach the harness.terminal-sessions.v1 read gateway",
            )
        except Exception as exc:  # noqa: BLE001
            status, error_code, detail, remedy = _safe_gateway_error(exc)
            return _fault(status, error_code, detail, remedy)
        if raw is None:
            return _fault(
                404,
                "terminal_session_not_found",
                "the terminal session is unknown or outside bounded retention",
                "reload the session list for this turn",
            )
        summary = _safe_terminal_summary(
            raw, expected_session_id=safe_session_id
        )
        if summary is None:
            return _fault(
                503,
                "terminal_session_projection_invalid",
                "the terminal session reader returned an invalid safe summary",
                "repair the durable projection before retrying",
            )
        if summary.get("turn_id") != safe_turn_id:
            return _fault(
                409,
                "terminal_session_scope_conflict",
                "the terminal session does not belong to the requested turn",
                "reload terminal sessions from the selected Harness turn",
            )
        return JSONResponse(summary)

    @router.get("/terminal-sessions/{terminal_session_id}/output")
    async def terminal_session_output(
        terminal_session_id: str,
        request: Request,
        turn_id: str | None = None,
        after_seq: str | None = None,
        limit: str | None = None,
    ) -> JSONResponse:
        unknown = _unknown_query(request, {"turn_id", "after_seq", "limit"})
        if unknown is not None:
            return unknown
        safe_session_id = _safe_identifier(terminal_session_id)
        if safe_session_id is None:
            return _fault(
                400,
                "invalid_terminal_session_id",
                "terminal_session_id must be a bounded opaque identifier",
                "use an id returned by the terminal session list",
            )
        safe_turn_id = _safe_identifier(turn_id)
        if safe_turn_id is None:
            return _fault(
                400,
                "invalid_turn_id",
                "terminal output replay requires its owning turn_id",
                "use the turn_id from the scoped terminal session list",
            )
        parsed_after = _parse_nonnegative(after_seq, name="after_seq")
        parsed_limit = _parse_terminal_limit(
            limit,
            default=DEFAULT_TERMINAL_OUTPUT,
            maximum=MAX_TERMINAL_OUTPUT,
        )
        if isinstance(parsed_after, JSONResponse):
            return parsed_after
        if isinstance(parsed_limit, JSONResponse):
            return parsed_limit
        gateway = resolve_gateway(request)
        if gateway is None:
            return _fault(
                503,
                "terminal_sessions_unavailable",
                "no live terminal output reader is attached",
                "attach the durable terminal session gateway and retry",
            )
        try:
            # Bind the opaque session id to the selected turn at the HTTP
            # edge even for a custom gateway that silently ignores keyword
            # scope.  Runtime repeats this check against the causal ledger
            # and authoritative Engram before touching the output store.
            scoped_raw = await _call_target(
                gateway,
                (
                    "inspect_terminal_session",
                    "get_terminal_session",
                    "inspect",
                ),
                {
                    "terminal_session_id": safe_session_id,
                    "session_id": safe_session_id,
                    "expected_turn_id": safe_turn_id,
                },
            )
            if scoped_raw is None:
                return _fault(
                    404,
                    "terminal_session_not_found",
                    "the terminal session is unknown or outside bounded retention",
                    "reload the session list for this turn",
                )
            scoped_summary = _safe_terminal_summary(
                scoped_raw, expected_session_id=safe_session_id
            )
            if scoped_summary is None:
                return _fault(
                    503,
                    "terminal_session_projection_invalid",
                    "the terminal session reader returned an invalid safe summary",
                    "repair the durable projection before retrying",
                )
            if scoped_summary.get("turn_id") != safe_turn_id:
                return _fault(
                    409,
                    "terminal_session_scope_conflict",
                    "the terminal session does not belong to the requested turn",
                    "reload terminal sessions from the selected Harness turn",
                )
            raw = await _call_target(
                gateway,
                (
                    "read_terminal_session_output",
                    "terminal_session_output",
                    "read_output",
                ),
                {
                    "terminal_session_id": safe_session_id,
                    "session_id": safe_session_id,
                    "expected_turn_id": safe_turn_id,
                    "after_seq": parsed_after,
                    "limit": parsed_limit,
                },
            )
        except _MissingMethod:
            return _fault(
                503,
                "terminal_sessions_unavailable",
                "the live gateway does not implement terminal output replay",
                "attach the harness.terminal-sessions.v1 read gateway",
            )
        except Exception as exc:  # noqa: BLE001
            status, error_code, detail, remedy = _safe_gateway_error(exc)
            return _fault(status, error_code, detail, remedy)
        if raw is None:
            return _fault(
                404,
                "terminal_session_not_found",
                "the terminal session is unknown or outside bounded retention",
                "reload the session list for this turn",
            )
        page = _safe_terminal_output_page(
            raw,
            terminal_session_id=safe_session_id,
            after_seq=parsed_after,
            limit=parsed_limit,
        )
        if page is None:
            return _fault(
                503,
                "terminal_output_projection_invalid",
                "the terminal output reader returned an invalid safe page",
                "repair the durable output projection before retrying",
            )
        return JSONResponse(page)

    @router.post("/terminal-sessions/{terminal_session_id}/stop")
    async def stop_terminal_session(
        terminal_session_id: str,
        request: Request,
    ) -> JSONResponse:
        safe_session_id = _safe_identifier(terminal_session_id)
        if safe_session_id is None:
            return _fault(
                400,
                "invalid_terminal_session_id",
                "terminal_session_id must be a bounded opaque identifier",
                "use an id returned by the terminal session list",
            )
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001 - invalid JSON is a client fault
            return _fault(
                400,
                "invalid_body",
                "stop body must be valid JSON",
                "send request_id, expected_epoch, and expected_turn_id",
            )
        if not isinstance(body, dict):
            return _fault(
                400,
                "invalid_body",
                "stop body must be a JSON object",
                "send the documented terminal stop fields",
            )
        allowed = {
            "request_id",
            "expected_epoch",
            "expected_state",
            "expected_turn_id",
            "reason",
        }
        unknown = sorted(set(body) - allowed)
        if unknown:
            return _fault(
                400,
                "unknown_control_field",
                f"unknown terminal stop field(s): {', '.join(unknown)}",
                "remove fields outside harness.terminal-sessions.v1",
            )
        request_id = _safe_identifier(body.get("request_id"))
        if request_id is None:
            return _fault(
                400,
                "invalid_request_id",
                "request_id is required and must be a bounded opaque identifier",
                "reuse one stable request_id for idempotent stop retries",
            )
        expected_turn_id = _safe_identifier(body.get("expected_turn_id"))
        if expected_turn_id is None:
            return _fault(
                400,
                "invalid_turn_id",
                "expected_turn_id is required and must be bounded",
                "use the turn_id from the terminal session summary",
            )
        expected_epoch = body.get("expected_epoch")
        if (
            isinstance(expected_epoch, bool)
            or not isinstance(expected_epoch, int)
            or expected_epoch < 0
        ):
            return _fault(
                400,
                "invalid_epoch",
                "expected_epoch is required and must be a non-negative integer",
                "use the epoch from the terminal session summary",
            )
        expected_state = body.get("expected_state")
        if expected_state is not None:
            if not isinstance(expected_state, str):
                return _fault(
                    400,
                    "invalid_state",
                    "expected_state must be a terminal session state",
                    "use the state from the terminal session summary",
                )
            expected_state = expected_state.strip().upper()
            if expected_state not in _TERMINAL_SESSION_STATES:
                return _fault(
                    400,
                    "invalid_state",
                    "expected_state is outside the terminal session contract",
                    "use the state from the terminal session summary",
                )
        reason = body.get("reason")
        if reason is None:
            reason = "user_stop"
        else:
            if (
                not isinstance(reason, str)
                or len(reason) > MAX_REASON_LENGTH
                or "\x00" in reason
            ):
                return _fault(
                    400,
                    "invalid_reason",
                    f"reason must be a string of at most {MAX_REASON_LENGTH} characters",
                    "omit reason or send a bounded explanation",
                )
            reason = reason.strip()
            if not reason:
                reason = "user_stop"
            else:
                reason = _safe_text(reason, limit=MAX_REASON_LENGTH)

        gateway = resolve_gateway(request)
        if gateway is None:
            return _control_fault(
                503,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code="terminal_sessions_unavailable",
                detail="no live terminal session control gateway is attached",
                remedy="attach the durable terminal session gateway; no stop was accepted",
            )

        inspect_kwargs = {
            "terminal_session_id": safe_session_id,
            "session_id": safe_session_id,
            "expected_turn_id": expected_turn_id,
        }
        try:
            current_raw = await _call_target(
                gateway,
                (
                    "inspect_terminal_session",
                    "get_terminal_session",
                    "inspect",
                ),
                inspect_kwargs,
            )
        except _MissingMethod:
            return _control_fault(
                503,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code="terminal_sessions_unavailable",
                detail="the live gateway cannot inspect terminal session scope",
                remedy="attach the harness.terminal-sessions.v1 control gateway",
            )
        except Exception as exc:  # noqa: BLE001
            status, error_code, detail, remedy = _safe_gateway_error(exc)
            return _control_fault(
                status,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code=error_code,
                detail=detail,
                remedy=remedy,
            )
        if current_raw is None:
            return _control_fault(
                404,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code="terminal_session_not_found",
                detail="the terminal session is unknown or outside bounded retention",
                remedy="reload the session list before issuing another stop",
            )
        current = _safe_terminal_summary(
            current_raw, expected_session_id=safe_session_id
        )
        if current is None:
            return _control_fault(
                503,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code="terminal_session_projection_invalid",
                detail="the terminal session scope cannot be safely projected",
                remedy="repair the durable projection before retrying",
            )
        if current.get("turn_id") != expected_turn_id:
            return _control_fault(
                409,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code="stale_turn",
                detail="the terminal session belongs to another turn",
                remedy="reload the session summary and do not retry this side effect automatically",
            )
        if current.get("epoch") != expected_epoch:
            return _control_fault(
                409,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code="stale_epoch",
                detail="the terminal session epoch has changed",
                remedy="reload the session summary and do not retry this side effect automatically",
            )
        if expected_state is not None and current.get("state") != expected_state:
            return _control_fault(
                409,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code="stale_state",
                detail="the terminal session state has changed",
                remedy="inspect the current state before deciding whether to stop",
            )
        if current["state"] in _TERMINAL_SESSION_TERMINAL_STATES:
            result = dict(current)
            result.update(
                {
                    "request_id": request_id,
                    "accepted": True,
                    "idempotent": True,
                }
            )
            return JSONResponse(result)

        stop_request = {
            "request_id": request_id,
            "expected_epoch": expected_epoch,
            "expected_state": expected_state,
            "expected_turn_id": expected_turn_id,
            "reason": reason,
        }
        try:
            raw = await _call_target(
                gateway,
                (
                    "stop_terminal_session",
                    "terminal_session_stop",
                    "stop",
                ),
                {
                    "terminal_session_id": safe_session_id,
                    "session_id": safe_session_id,
                    "request": stop_request,
                    "body": stop_request,
                    "payload": stop_request,
                    **stop_request,
                },
            )
        except _MissingMethod:
            return _control_fault(
                503,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code="terminal_stop_unavailable",
                detail="the live gateway does not implement terminal session stop",
                remedy="attach the harness.terminal-sessions.v1 control gateway; no stop was accepted",
            )
        except Exception as exc:  # noqa: BLE001
            status, error_code, detail, remedy = _safe_gateway_error(exc)
            return _control_fault(
                status,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code=error_code,
                detail=detail,
                remedy=remedy,
            )
        if raw is None:
            return _control_fault(
                503,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code="terminal_stop_result_invalid",
                detail="the terminal stop gateway returned no structured result",
                remedy="inspect the session; do not generate a new request_id automatically",
            )
        raw_mapping = _as_mapping(raw)
        accepted_value = raw_mapping.get("accepted", _MISSING)
        if not isinstance(accepted_value, bool):
            return _control_fault(
                503,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code="terminal_stop_result_invalid",
                detail="the terminal stop gateway did not return an explicit boolean accepted field",
                remedy="inspect the session; do not assume the stop was accepted",
            )
        accepted = accepted_value
        nested = raw_mapping.get("session", raw_mapping.get("summary"))
        result_mapping = _as_mapping(nested if nested is not None else raw)
        has_result_summary = nested is not None or any(
            key in raw_mapping
            for key in ("terminal_session_id", "session_id", "mode", "transport")
        )
        result_summary = (
            _safe_terminal_summary(
                result_mapping, expected_session_id=safe_session_id
            )
            if has_result_summary
            else None
        )
        if has_result_summary and (
            result_summary is None
            or result_summary.get("turn_id") != expected_turn_id
            or result_summary.get("epoch") != expected_epoch
        ):
            return _control_fault(
                503,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code="terminal_stop_result_invalid",
                detail="the terminal stop result cannot be safely bound to this session scope",
                remedy="inspect the session; do not generate a new request_id automatically",
            )
        if not accepted:
            if (
                result_summary is not None
                and result_summary["state"]
                in _TERMINAL_SESSION_TERMINAL_STATES
            ):
                result = dict(result_summary)
                result.update(
                    {
                        "request_id": request_id,
                        "accepted": False,
                        "idempotent": True,
                    }
                )
                return JSONResponse(result, status_code=200)
            error_code = _safe_text(
                raw_mapping.get("error_code", raw_mapping.get("error")), limit=128
            ) or "terminal_stop_rejected"
            status = 503 if error_code in {
                "terminal_stop_unavailable",
                "termination_unavailable",
                "backend_unavailable",
                "control_unavailable",
            } else 409
            return _control_fault(
                status,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code=error_code,
                detail="the terminal stop request was not accepted",
                remedy="inspect the session before deciding whether to reuse the same request_id",
                evidence_class=_safe_evidence_class(
                    raw_mapping.get("evidence_class")
                ),
            )
        if result_summary is None:
            return _control_fault(
                503,
                request_id=request_id,
                turn_id=expected_turn_id,
                error_code="terminal_stop_result_invalid",
                detail="the terminal stop result cannot be safely projected",
                remedy="inspect the session; do not generate a new request_id automatically",
            )
        result = dict(result_summary)
        result.update(
            {
                "request_id": request_id,
                "accepted": True,
                "idempotent": bool(raw_mapping.get("idempotent", False)),
            }
        )
        status = (
            200
            if result_summary["state"] in _TERMINAL_SESSION_TERMINAL_STATES
            else 202
        )
        return JSONResponse(result, status_code=status)

    @router.get("/turns/{turn_id}/stream", response_model=None)
    async def turn_stream(
        turn_id: str,
        request: Request,
        after_seq: str | None = None,
        limit: str | None = None,
        once: str | None = None,
    ) -> JSONResponse | StreamingResponse:
        unknown = _unknown_query(request, {"after_seq", "limit", "once"})
        if unknown is not None:
            return unknown
        parsed_after = _parse_nonnegative(after_seq, name="after_seq")
        parsed_limit = _parse_limit(limit)
        parsed_once = _parse_bool(once, name="once")
        last_id = _parse_last_event_id(request.headers.get("last-event-id"), turn_id)
        if isinstance(parsed_after, JSONResponse):
            return parsed_after
        if isinstance(parsed_limit, JSONResponse):
            return parsed_limit
        if isinstance(parsed_once, JSONResponse):
            return parsed_once
        if isinstance(last_id, JSONResponse):
            return last_id
        if isinstance(last_id, int):
            parsed_after = max(parsed_after, last_id)
        store = resolve_store(request)
        if store is None:
            return _fault(503, "harness_unavailable", "no Harness event reader is attached to this observatory", "mount the router with a redacted event store")

        async def stream():
            cursor = parsed_after
            emitted = 0
            first_pass = True
            deadline = time.monotonic() + connection_seconds
            while True:
                if not parsed_once and await request.is_disconnected():
                    return
                if not parsed_once and time.monotonic() >= deadline:
                    yield ": harness stream closed at its bounded connection window\n\n"
                    return
                try:
                    page = await _read_page(store, turn_id, cursor, min(parsed_limit, connection_event_cap - emitted))
                except Exception as exc:  # noqa: BLE001
                    _logger.warning("Harness SSE read failed: %s", exc)
                    yield _sse(
                        "harness_error",
                        {
                            "turn_id": turn_id,
                            "seq": cursor,
                            "error": "harness_stream_unavailable",
                            "remedy": "reload from the last confirmed sequence",
                            "evidence_class": "LIVE_GATE_UNVERIFIED",
                        },
                    )
                    return
                if page.gap is not None:
                    gap_seq = _safe_int(page.gap.get("missing_from"), cursor + 1) or cursor + 1
                    yield _sse(
                        "event_gap",
                        {
                            "turn_id": turn_id,
                            "event_id": f"{turn_id}:gap:{gap_seq}",
                            "seq": cursor,
                            **page.gap,
                            "evidence_class": page.evidence_class,
                        },
                    )
                    return
                sent = False
                terminal_seen = False
                for event in page.events:
                    seq = event["seq"]
                    if seq <= cursor:
                        continue
                    if seq > cursor + 1:
                        yield _sse(
                            "event_gap",
                            {
                                "turn_id": turn_id,
                                "seq": cursor,
                                "missing_from": cursor + 1,
                                "missing_to": seq - 1,
                                "reason": "non_contiguous_page",
                                "evidence_class": page.evidence_class,
                            },
                        )
                        return
                    cursor = seq
                    emitted += 1
                    sent = True
                    # Tool/approval events commonly carry status=completed.
                    # Only the canonical turn terminal event may close the
                    # turn stream; the bounded connection window otherwise
                    # ends normally and EventSource reconnects by cursor.
                    terminal_seen = terminal_seen or event["kind"] == "turn_terminal"
                    yield _sse("harness_event", event, f"{turn_id}:{seq}")
                    if emitted >= connection_event_cap:
                        yield ": harness stream event cap reached; reconnect with the last seq\n\n"
                        return
                if parsed_once:
                    if not sent:
                        yield ": harness stream ready\n\n"
                    return
                if first_pass and not sent:
                    yield ": harness stream ready\n\n"
                first_pass = False
                if terminal_seen:
                    return
                if not sent:
                    yield ": keepalive\n\n"
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

    @router.get("/capacity")
    async def capacity(request: Request) -> JSONResponse:
        store = resolve_store(request)
        gateway = resolve_gateway(request)
        if store is None and gateway is None:
            return _fault(503, "harness_unavailable", "no Harness capacity source is attached", "mount the router with an event store or live control gateway")
        return JSONResponse(
            {
                "capacity": await _capacity_for(store, gateway, world_id=resolve_world(request)),
                "evidence_class": "LIVE_GATE_UNVERIFIED",
            }
        )

    async def _turn_for_control(request: Request, turn_id: str) -> tuple[Any | None, dict[str, Any] | None, JSONResponse | None]:
        store = resolve_store(request)
        if store is None:
            return None, None, _fault(503, "harness_unavailable", "no Harness event reader is attached", "attach the redacted event store before sending control")
        try:
            summary = await _summary_for(store, turn_id, evidence_class="LIVE_GATE_UNVERIFIED", live_available=resolve_gateway(request) is not None)
        except _DependencyUnavailable:
            return store, None, _fault(503, "harness_unavailable", "the Harness turn cannot be scoped safely", "attach a summary-capable event reader")
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Harness control scope read failed: %s", exc)
            return store, None, _fault(503, "harness_unavailable", "the Harness turn scope is temporarily unavailable", "refresh and retry when the event reader is readable")
        if summary is None:
            return store, None, _fault(404, "turn_not_found", f"Harness turn {turn_id!r} was not found", "use a live turn_id from the Harness API")
        return store, summary, None

    async def _submit_control(
        request: Request,
        *,
        operation: str,
        turn_id: str,
        payload: dict[str, Any],
    ) -> JSONResponse:
        store, summary, scope_error = await _turn_for_control(request, turn_id)
        if scope_error is not None:
            return scope_error
        assert store is not None and summary is not None
        request_id = payload["request_id"]
        if _is_terminal(summary):
            return _control_fault(
                410,
                request_id=request_id,
                turn_id=turn_id,
                error_code="terminal_turn",
                detail="the Harness turn is already terminal; no second side effect was sent",
                remedy="read the terminal event and start a new turn if work is still required",
            )
        gateway = resolve_gateway(request)
        if gateway is None:
            return _control_fault(
                503,
                request_id=request_id,
                turn_id=turn_id,
                error_code="harness_unavailable",
                detail="this observatory has history but no live Harness control gateway",
                remedy="attach a policy-aware Pi control gateway; no mock control was created",
            )
        world = resolve_world(request) or summary.get("world_id")
        kwargs = _control_kwargs(operation=operation, turn_id=turn_id, request_payload=payload, world_id=world)
        try:
            raw = await _call_target(gateway, _control_method_names(operation), kwargs)
        except _MissingMethod:
            return _control_fault(
                503,
                request_id=request_id,
                turn_id=turn_id,
                error_code="control_unavailable",
                detail="the attached gateway does not implement this sideband operation",
                remedy="attach the policy-aware interrupt/steer gateway and retry",
            )
        except Exception as exc:  # noqa: BLE001
            status, error_code, detail, remedy = _safe_gateway_error(exc)
            return _control_fault(
                status,
                request_id=request_id,
                turn_id=turn_id,
                error_code=error_code,
                detail=detail,
                remedy=remedy,
            )
        if raw is None:
            return _control_fault(
                503,
                request_id=request_id,
                turn_id=turn_id,
                error_code="control_no_result",
                detail="the gateway returned no structured control result",
                remedy="do not retry until the gateway returns an idempotent result",
            )
        result, status = _control_outcome(
            raw,
            request_id=request_id,
            turn_id=turn_id,
            evidence_class=_safe_evidence_class(summary.get("evidence_class")),
        )
        return JSONResponse(status_code=status, content=result)

    @router.post("/turns/{turn_id}/interrupt")
    async def interrupt(turn_id: str, request: Request) -> JSONResponse:
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001
            return _fault(400, "invalid_body", "interrupt body must be valid JSON", "send request_id, expected_epoch, and optional reason")
        payload, error = _validate_common_control(
            body,
            allowed={"request_id", "reason", "expected_state", "expected_epoch"},
        )
        if error is not None:
            return error
        assert payload is not None
        reason = payload.get("reason")
        if reason is not None and (not isinstance(reason, str) or len(reason) > MAX_REASON_LENGTH):
            return _fault(400, "invalid_reason", "reason must be a bounded string", f"send reason with at most {MAX_REASON_LENGTH} characters")
        return await _submit_control(request, operation="interrupt", turn_id=turn_id, payload=payload)

    @router.post("/turns/{turn_id}/steer")
    async def steer(turn_id: str, request: Request) -> JSONResponse:
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001
            return _fault(400, "invalid_body", "steer body must be valid JSON", "send request_id, expected_epoch, and a natural-language message")
        payload, error = _validate_common_control(
            body,
            allowed={"request_id", "message", "expected_state", "expected_epoch"},
        )
        if error is not None:
            return error
        assert payload is not None
        message = payload.get("message")
        if not isinstance(message, str) or not message.strip() or len(message) > MAX_STEER_LENGTH:
            return _fault(400, "invalid_message", "steer message must be a non-empty bounded string", f"send message with at most {MAX_STEER_LENGTH} characters")
        payload["message"] = message
        return await _submit_control(request, operation="steer", turn_id=turn_id, payload=payload)

    @router.post("/approvals/{approval_id}/resolve")
    async def resolve_approval(approval_id: str, request: Request) -> JSONResponse:
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001
            return _fault(400, "invalid_body", "approval body must be valid JSON", "send request_id, decision, expected_turn_id, and expected_epoch")
        payload, error = _validate_common_control(
            body,
            allowed={
                "request_id",
                "decision",
                "expected_turn_id",
                "expected_epoch",
                "expected_state",
            },
        )
        if error is not None:
            return error
        assert payload is not None
        decision = payload.get("decision")
        if decision not in {"allow_once", "deny", "cancel"}:
            return _fault(400, "invalid_decision", "decision is outside the current approval contract", "choose allow_once, deny, or cancel")
        expected_turn_id = payload.get("expected_turn_id")
        if not isinstance(expected_turn_id, str) or not expected_turn_id.strip() or len(expected_turn_id) > MAX_ID_LENGTH:
            return _fault(400, "invalid_turn_id", "expected_turn_id is required and must be bounded", "use the turn_id shown with the approval request")
        store, summary, scope_error = await _turn_for_control(request, expected_turn_id)
        if scope_error is not None:
            return scope_error
        assert store is not None and summary is not None
        if _is_terminal(summary):
            return _control_fault(
                410,
                request_id=payload["request_id"],
                turn_id=expected_turn_id,
                error_code="approval_expired",
                detail="the approval belongs to a terminal turn",
                remedy="start a new turn and request approval again",
            )
        gateway = resolve_gateway(request)
        if gateway is None:
            return _control_fault(
                503,
                request_id=payload["request_id"],
                turn_id=expected_turn_id,
                error_code="harness_unavailable",
                detail="this observatory has history but no live approval gateway",
                remedy="attach the policy-aware approval gateway; no grant was created",
            )
        approval_request = dict(payload)
        approval_request["approval_id"] = approval_id
        approval_request["world_id"] = resolve_world(request) or summary.get("world_id")
        try:
            raw = await _call_target(
                gateway,
                ("resolve_approval", "approve", "resolve"),
                {
                    "approval_id": approval_id,
                    "request": approval_request,
                    "body": approval_request,
                    "payload": approval_request,
                    "approval_request": approval_request,
                    "request_id": payload["request_id"],
                    "decision": decision,
                    "expected_turn_id": expected_turn_id,
                    "expected_epoch": payload["expected_epoch"],
                    "world_id": approval_request["world_id"],
                },
            )
        except _MissingMethod:
            return _control_fault(
                503,
                request_id=payload["request_id"],
                turn_id=expected_turn_id,
                error_code="control_unavailable",
                detail="the attached gateway does not implement approval resolution",
                remedy="attach the policy-aware approval gateway and retry",
            )
        except Exception as exc:  # noqa: BLE001
            status, error_code, detail, remedy = _safe_gateway_error(exc)
            return _control_fault(
                status,
                request_id=payload["request_id"],
                turn_id=expected_turn_id,
                error_code=error_code,
                detail=detail,
                remedy=remedy,
            )
        if raw is None:
            return _control_fault(
                503,
                request_id=payload["request_id"],
                turn_id=expected_turn_id,
                error_code="control_no_result",
                detail="the approval gateway returned no structured result",
                remedy="do not retry until the gateway returns an idempotent result",
            )
        result, status = _control_outcome(
            raw,
            request_id=payload["request_id"],
            turn_id=expected_turn_id,
            evidence_class=_safe_evidence_class(summary.get("evidence_class")),
        )
        result["approval_id"] = _safe_text(approval_id, limit=MAX_ID_LENGTH)
        return JSONResponse(status_code=status, content=result)

    @router.get("/checkpoints")
    async def checkpoints(
        request: Request,
        turn_id: str | None = None,
    ) -> JSONResponse:
        unknown = _unknown_query(request, {"turn_id"})
        if unknown is not None:
            return unknown
        if turn_id is not None and (
            not turn_id.strip() or len(turn_id) > MAX_ID_LENGTH
        ):
            return _fault(
                400,
                "invalid_turn_id",
                "turn_id must be a bounded non-empty string",
                "omit turn_id or use a Harness turn identifier",
            )
        gateway = resolve_gateway(request)
        if gateway is None:
            return _fault(
                503,
                "checkpoint_unavailable",
                "no live checkpoint reader is attached",
                "enable the checkpoint-first workspace adapter",
            )
        try:
            raw = await _call_target(
                gateway,
                ("list_checkpoints", "checkpoints"),
                {"turn_id": turn_id},
            )
        except _MissingMethod:
            return _fault(
                503,
                "checkpoint_unavailable",
                "the live gateway has no checkpoint reader",
                "attach a durable checkpoint backend",
            )
        except Exception as exc:  # noqa: BLE001 - adapter boundary
            status, error_code, detail, remedy = _safe_gateway_error(exc)
            return _fault(status, error_code, detail, remedy)
        values = (
            raw.get("checkpoints", raw.get("items", []))
            if isinstance(raw, Mapping)
            else raw
        )
        if not isinstance(values, (list, tuple)):
            values = []
        safe = [
            checkpoint
            for item in values[:128]
            if (checkpoint := _safe_checkpoint(item)) is not None
        ]
        return JSONResponse(
            {
                "checkpoints": safe,
                "count": len(safe),
                "evidence_class": (
                    safe[0]["evidence_class"] if safe else "LIVE_GATE_UNVERIFIED"
                ),
            }
        )

    @router.post("/checkpoints/{checkpoint_id}/restore")
    async def restore_checkpoint(checkpoint_id: str, request: Request) -> JSONResponse:
        if not checkpoint_id.strip() or len(checkpoint_id) > MAX_ID_LENGTH:
            return _fault(
                400,
                "invalid_checkpoint_id",
                "checkpoint_id must be a bounded non-empty string",
                "use an id returned by GET /harness/checkpoints",
            )
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001
            return _fault(
                400,
                "invalid_body",
                "restore body must be valid JSON",
                "send request_id, expected_epoch, and optional changed_paths",
            )
        payload, error = _validate_common_control(
            body,
            allowed={"request_id", "expected_epoch", "changed_paths"},
        )
        if error is not None:
            return error
        assert payload is not None
        changed_paths = payload.get("changed_paths", [])
        if not isinstance(changed_paths, list) or len(changed_paths) > 128:
            return _fault(
                400,
                "invalid_changed_paths",
                "changed_paths must be a bounded JSON array",
                "omit it to restore the full checkpoint scope",
            )
        safe_paths: list[str] = []
        for value in changed_paths:
            safe = _safe_path(value)
            if safe is None or safe == "[REDACTED_PATH]" or safe != str(value).replace("\\", "/"):
                return _fault(
                    400,
                    "invalid_changed_path",
                    "restore paths must be normalized workspace-relative paths",
                    "use a path listed by GET /harness/checkpoints",
                )
            safe_paths.append(safe)
        gateway = resolve_gateway(request)
        if gateway is None:
            return _fault(
                503,
                "checkpoint_unavailable",
                "no live checkpoint restore gateway is attached",
                "enable the checkpoint-first workspace adapter",
            )
        try:
            raw = await _call_target(
                gateway,
                ("restore_checkpoint", "checkpoint_restore"),
                {
                    "checkpoint_id": checkpoint_id,
                    "request_id": payload["request_id"],
                    "expected_epoch": payload["expected_epoch"],
                    "changed_paths": tuple(safe_paths),
                    "request": payload,
                },
            )
        except _MissingMethod:
            return _fault(
                503,
                "checkpoint_unavailable",
                "the live gateway has no restore operation",
                "attach a conflict-checked restore adapter",
            )
        except Exception as exc:  # noqa: BLE001 - adapter boundary
            status, error_code, detail, remedy = _safe_gateway_error(exc)
            return _fault(status, error_code, detail, remedy)
        safe = _safe_checkpoint(raw)
        if safe is None:
            return _fault(
                503,
                "checkpoint_result_invalid",
                "the restore adapter returned no bounded result",
                "inspect the checkpoint manifest before retrying",
            )
        safe["request_id"] = payload["request_id"]
        state = str(safe.get("state", "unknown")).casefold()
        status = (
            200
            if state in {"restored", "completed"}
            else (
                503
                if state in {"uncertain", "unsupported_execution"}
                else (403 if state == "declined" else 409)
            )
        )
        return JSONResponse(status_code=status, content=safe)

    @router.get("/subagents/{task_id}")
    async def subagent(task_id: str, request: Request) -> JSONResponse:
        target = resolve_gateway(request)
        store = resolve_store(request)
        for candidate in (target, store):
            if candidate is None:
                continue
            try:
                raw = await _call_target(candidate, ("subagent", "get_subagent", "inspect_subagent"), {"task_id": task_id, "id": task_id})
            except _MissingMethod:
                continue
            except Exception as exc:  # noqa: BLE001
                _logger.warning("Harness subagent read failed: %s", exc)
                return _fault(503, "harness_unavailable", "the temporary worker status is unavailable", "retry after the worker reader is available")
            if raw is not None:
                return JSONResponse(_safe_subagent(raw, task_id))
        return _fault(503, "harness_unavailable", "no temporary worker reader is attached", "mount a bounded task-worker reader")

    return router


router = create_harness_router()

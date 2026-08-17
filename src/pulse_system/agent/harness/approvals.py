"""Server-side, in-memory approval contracts for Harness actions.

``ApprovalRegistry`` is intentionally not an executor.  It records a bounded
approval state and checks the identity/fencing values supplied by Pulse before
returning a decision.  A caller must still pass the decision to an OS, Pi,
MCP or file-change adapter, and those adapters must keep the
``CONTRACT_ONLY``/``LIVE_GATE_UNVERIFIED`` distinction explicit.

The registry has no database dependency.  Runtime integration can
project its safe request/resolution dictionaries into ``HarnessEvent`` later;
this module never stores a secret, full command or absolute path.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Callable

from .security import (
    CONTRACT_ONLY,
    LIVE_GATE_UNVERIFIED,
    PolicyError,
    Redactor,
)

__all__ = [
    "ApprovalCheck",
    "ApprovalDecision",
    "ApprovalError",
    "ApprovalRegistry",
    "ApprovalResolution",
    "ApprovalState",
    "HarnessApprovalRequest",
]


_IDENTIFIER_MAX = 128
_TERMINAL_STATES = frozenset(
    {
        "allowed_once",
        "allowed_session",
        "denied",
        "cancelled",
        "expired",
        "revoked",
    }
)
_ACTIVE_STATES = frozenset({"requested", "allowed_once", "allowed_session"})
_DECISION_ALIASES = {
    "allowed_once": "allow_once",
    "allowed_session": "allow_session",
    "cancelled": "cancel",
}
_DECISIONS = frozenset({"allow_once", "allow_session", "deny", "cancel"})


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _identifier(value: Any, field_name: str, *, allow_none: bool = False) -> str | None:
    if allow_none and value is None:
        return None
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > _IDENTIFIER_MAX:
        raise ValueError(f"{field_name} must be a bounded non-empty identifier")
    value = value.strip()
    if any(character.isspace() for character in value):
        raise ValueError(f"{field_name} must not contain whitespace")
    return value


def _epoch(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("epoch must be a non-negative integer")
    return value


def _decision(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("approval decision must be a string")
    normalized = value.strip().casefold().replace("-", "_")
    normalized = _DECISION_ALIASES.get(normalized, normalized)
    if normalized not in _DECISIONS:
        raise ValueError("approval decision is not supported")
    return normalized


def _state(value: Any) -> ApprovalState:
    if isinstance(value, ApprovalState):
        return value
    if not isinstance(value, str):
        raise ValueError("expected_state must be an approval state")
    try:
        return ApprovalState(value.strip().casefold().replace("-", "_"))
    except ValueError as exc:
        raise ValueError("expected_state is not a supported approval state") from exc


def _request_id(value: str | None) -> str:
    if value is None:
        return f"approval-request-{uuid.uuid4().hex}"
    return _identifier(value, "request_id") or ""


class ApprovalState(StrEnum):
    """Frozen approval lifecycle plus an explicit unknown read state."""

    REQUESTED = "requested"
    ALLOWED_ONCE = "allowed_once"
    ALLOWED_SESSION = "allowed_session"
    DENIED = "denied"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    REVOKED = "revoked"


ApprovalDecision = str


class ApprovalError(RuntimeError):
    """Safe structured error for invalid or stale approval operations."""

    def __init__(
        self,
        code: str,
        detail: str,
        *,
        status: int = 409,
        retryable: bool = False,
    ) -> None:
        self.code = _identifier(code.replace("-", "_"), "code") or "approval_error"
        self.detail = detail
        self.status = status
        self.retryable = retryable
        super().__init__(f"{self.code}: {detail}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "detail": self.detail,
            "status": self.status,
            "retryable": self.retryable,
            "evidence_class": CONTRACT_ONLY,
        }


@dataclass(frozen=True, slots=True)
class HarnessApprovalRequest:
    """Safe approval request and its server-owned scope."""

    approval_id: str
    request_id: str
    world_id: str
    engram_id: str
    turn_id: str
    epoch: int
    target_kind: str
    safe_preview: Mapping[str, Any]
    policy_id: str
    requested_at: datetime
    expires_at: datetime
    session_grant: bool = False
    session_id: str | None = None
    capability_scope: tuple[str, ...] = ()
    state: ApprovalState = ApprovalState.REQUESTED
    used: bool = False
    resolved_at: datetime | None = None
    resolution_request_id: str | None = None
    revoked_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("approval_id", "request_id", "world_id", "engram_id", "turn_id", "target_kind", "policy_id"):
            object.__setattr__(self, name, _identifier(getattr(self, name), name) or "")
        object.__setattr__(self, "epoch", _epoch(self.epoch))
        if not isinstance(self.safe_preview, Mapping):
            raise ValueError("safe_preview must be an object")
        object.__setattr__(self, "safe_preview", dict(self.safe_preview))
        object.__setattr__(self, "requested_at", _ensure_utc(self.requested_at, field_name="requested_at"))
        object.__setattr__(self, "expires_at", _ensure_utc(self.expires_at, field_name="expires_at"))
        if self.expires_at <= self.requested_at:
            raise ValueError("expires_at must be after requested_at")
        if type(self.session_grant) is not bool or type(self.used) is not bool:
            raise ValueError("session_grant and used must be bools")
        if self.session_id is not None:
            object.__setattr__(self, "session_id", _identifier(self.session_id, "session_id"))
        object.__setattr__(self, "capability_scope", tuple(self.capability_scope))
        if not isinstance(self.state, ApprovalState):
            object.__setattr__(self, "state", ApprovalState(self.state))
        if self.resolved_at is not None:
            object.__setattr__(self, "resolved_at", _ensure_utc(self.resolved_at, field_name="resolved_at"))
        if self.revoked_at is not None:
            object.__setattr__(self, "revoked_at", _ensure_utc(self.revoked_at, field_name="revoked_at"))
        if self.resolution_request_id is not None:
            object.__setattr__(self, "resolution_request_id", _identifier(self.resolution_request_id, "resolution_request_id"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "approval_id": self.approval_id,
            "request_id": self.request_id,
            "world_id": self.world_id,
            "engram_id": self.engram_id,
            "turn_id": self.turn_id,
            "epoch": self.epoch,
            "target_kind": self.target_kind,
            "safe_preview": dict(self.safe_preview),
            "policy_id": self.policy_id,
            "requested_at": self.requested_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "session_grant": self.session_grant,
            "session_id": self.session_id,
            "capability_scope": list(self.capability_scope),
            "state": self.state.value,
            "used": self.used,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "resolution_request_id": self.resolution_request_id,
            "revoked_at": self.revoked_at.isoformat() if self.revoked_at else None,
            "evidence_class": CONTRACT_ONLY,
        }


@dataclass(frozen=True, slots=True)
class ApprovalResolution:
    """The result of a resolve/revoke request, safe for event projection."""

    approval_id: str
    request_id: str
    decision: str | None
    state: ApprovalState | None
    accepted: bool
    reason_code: str
    idempotent: bool = False
    expires_at: datetime | None = None
    grant_id: str | None = None
    error: PolicyError | None = None
    evidence_class: str = CONTRACT_ONLY

    def __post_init__(self) -> None:
        object.__setattr__(self, "approval_id", _identifier(self.approval_id, "approval_id") or "")
        object.__setattr__(self, "request_id", _identifier(self.request_id, "request_id") or "")
        if self.decision is not None:
            object.__setattr__(self, "decision", _decision(self.decision))
        if self.state is not None and not isinstance(self.state, ApprovalState):
            object.__setattr__(self, "state", ApprovalState(self.state))
        if type(self.accepted) is not bool or type(self.idempotent) is not bool:
            raise ValueError("accepted and idempotent must be bools")
        if self.expires_at is not None:
            object.__setattr__(self, "expires_at", _ensure_utc(self.expires_at, field_name="expires_at"))
        if self.grant_id is not None:
            object.__setattr__(self, "grant_id", _identifier(self.grant_id, "grant_id"))
        if self.evidence_class not in {CONTRACT_ONLY, LIVE_GATE_UNVERIFIED}:
            raise ValueError("unknown evidence class")

    def to_dict(self) -> dict[str, Any]:
        return {
            "approval_id": self.approval_id,
            "request_id": self.request_id,
            "decision": self.decision,
            "state": self.state.value if self.state else None,
            "accepted": self.accepted,
            "reason_code": self.reason_code,
            "idempotent": self.idempotent,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "grant_id": self.grant_id,
            "error": self.error.to_dict() if self.error else None,
            "evidence_class": self.evidence_class,
        }


@dataclass(frozen=True, slots=True)
class ApprovalCheck:
    """Result of checking whether a stored grant may be consumed."""

    allowed: bool
    approval_id: str | None
    state: ApprovalState | None
    reason_code: str
    consumed: bool = False
    expires_at: datetime | None = None
    capability_scope: tuple[str, ...] = ()
    error: PolicyError | None = None
    evidence_class: str = CONTRACT_ONLY

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "approval_id": self.approval_id,
            "state": self.state.value if self.state else None,
            "reason_code": self.reason_code,
            "consumed": self.consumed,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "capability_scope": list(self.capability_scope),
            "error": self.error.to_dict() if self.error else None,
            "evidence_class": self.evidence_class,
        }


class ApprovalRegistry:
    """Bounded, thread-safe approval registry with no execution side effects."""

    def __init__(
        self,
        *,
        default_ttl_seconds: float = 300.0,
        max_entries: int = 1024,
        redactor: Redactor | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if isinstance(default_ttl_seconds, bool) or not isinstance(default_ttl_seconds, (int, float)):
            raise ValueError("default_ttl_seconds must be a positive number")
        if default_ttl_seconds <= 0 or default_ttl_seconds > 24 * 60 * 60:
            raise ValueError("default_ttl_seconds must be between 0 and 86400")
        if type(max_entries) is not int or max_entries < 16:
            raise ValueError("max_entries must be an integer >= 16")
        self.default_ttl_seconds = float(default_ttl_seconds)
        self.max_entries = max_entries
        self._redactor = redactor or Redactor()
        self._clock = clock or _utc_now
        self._lock = threading.RLock()
        self._requests: dict[str, HarnessApprovalRequest] = {}
        self._request_ids: dict[str, str] = {}
        self._resolutions: dict[tuple[str, str], ApprovalResolution] = {}

    def request(
        self,
        *,
        request_id: str | None = None,
        world_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        target_kind: str,
        safe_preview: Mapping[str, Any] | Any,
        policy_id: str = "harness.execution.v1",
        ttl_seconds: float | None = None,
        session_grant: bool = False,
        session_id: str | None = None,
        capability_scope: Iterable[str] = (),
    ) -> HarnessApprovalRequest:
        """Create or idempotently return an approval request.

        The input preview is redacted again even if it came from
        ``ExecutionPolicy``.  This makes the registry safe at a future API
        boundary where a caller may accidentally pass an untrusted mapping.
        """

        rid = _request_id(request_id)
        world = _identifier(world_id, "world_id") or ""
        engram = _identifier(engram_id, "engram_id") or ""
        turn = _identifier(turn_id, "turn_id") or ""
        target = _identifier(target_kind, "target_kind") or ""
        policy = _identifier(policy_id, "policy_id") or ""
        current_epoch = _epoch(epoch)
        if type(session_grant) is not bool:
            raise ValueError("session_grant must be a bool")
        if session_id is not None:
            session_id = _identifier(session_id, "session_id")
        ttl = self.default_ttl_seconds if ttl_seconds is None else ttl_seconds
        if isinstance(ttl, bool) or not isinstance(ttl, (int, float)) or ttl <= 0 or ttl > 24 * 60 * 60:
            raise ValueError("ttl_seconds must be between 0 and 86400")
        if session_grant and session_id is None:
            raise ValueError("session_grant requires session_id")
        safe_value, _report = self._redactor.redact(safe_preview)
        if isinstance(safe_value, Mapping):
            safe_mapping = dict(safe_value)
        else:
            safe_mapping = {"value": safe_value}
        scope = tuple(self._safe_scope(capability_scope))
        now = self._now()
        with self._lock:
            self._prune_locked(now)
            existing_id = self._request_ids.get(rid)
            if existing_id is not None:
                existing = self._requests[existing_id]
                if not self._same_request(
                    existing,
                    world_id=world,
                    engram_id=engram,
                    turn_id=turn,
                    epoch=current_epoch,
                    target_kind=target,
                    policy_id=policy,
                    session_grant=session_grant,
                    session_id=session_id,
                    capability_scope=scope,
                ):
                    raise ApprovalError(
                        "approval_request_conflict",
                        "request_id is already bound to a different approval scope",
                        status=409,
                    )
                return existing
            self._ensure_capacity_locked()
            approval_id = f"approval-{uuid.uuid4().hex}"
            request = HarnessApprovalRequest(
                approval_id=approval_id,
                request_id=rid,
                world_id=world,
                engram_id=engram,
                turn_id=turn,
                epoch=current_epoch,
                target_kind=target,
                safe_preview=safe_mapping,
                policy_id=policy,
                requested_at=now,
                expires_at=now + timedelta(seconds=float(ttl)),
                session_grant=session_grant,
                session_id=session_id,
                capability_scope=scope,
            )
            self._requests[approval_id] = request
            self._request_ids[rid] = approval_id
            return request

    def get(self, approval_id: str) -> HarnessApprovalRequest | None:
        """Return a current safe request, transitioning expired state first."""

        approval = _identifier(approval_id, "approval_id")
        with self._lock:
            self._refresh_locked(self._now(), approval)
            return self._requests.get(approval)

    def list(self, *, limit: int = 100) -> tuple[HarnessApprovalRequest, ...]:
        if type(limit) is not int or limit <= 0 or limit > self.max_entries:
            raise ValueError("limit must be a positive bounded integer")
        with self._lock:
            self._prune_locked(self._now())
            return tuple(list(self._requests.values())[-limit:])

    def resolve(
        self,
        approval_id: str,
        decision: str,
        *,
        expected_epoch: int,
        request_id: str,
        expected_state: ApprovalState | str | None = None,
        world_id: str | None = None,
        engram_id: str | None = None,
        turn_id: str | None = None,
        session_id: str | None = None,
    ) -> ApprovalResolution:
        """Resolve once, or return the exact prior result for a duplicate key."""

        approval_key = _identifier(approval_id, "approval_id") or ""
        resolution_request_id = _identifier(request_id, "request_id") or ""
        normalized_decision = _decision(decision)
        expected = _epoch(expected_epoch)
        expected_status = _state(expected_state) if expected_state is not None else None
        with self._lock:
            existing_resolution = self._resolutions.get((approval_key, resolution_request_id))
            if existing_resolution is not None:
                return replace(existing_resolution, idempotent=True)
            approval = self._requests.get(approval_key)
            if approval is None:
                return self._remember_resolution_locked(
                    self._reject(
                        approval_key,
                        resolution_request_id,
                        normalized_decision,
                        "approval_unknown",
                        status=404,
                    )
                )
            self._refresh_locked(self._now(), approval_key)
            approval = self._requests[approval_key]
            mismatch = self._scope_mismatch(
                approval,
                expected_epoch=expected,
                world_id=world_id,
                engram_id=engram_id,
                turn_id=turn_id,
                session_id=session_id,
            )
            if mismatch is not None:
                return self._remember_resolution_locked(
                    self._reject(
                        approval_key,
                        resolution_request_id,
                        normalized_decision,
                        mismatch,
                        status=409,
                    )
                )
            if expected_status is not None and approval.state is not expected_status:
                return self._remember_resolution_locked(
                    self._reject(
                        approval_key,
                        resolution_request_id,
                        normalized_decision,
                        "approval_state_mismatch",
                        status=409,
                        state=approval.state,
                        expires_at=approval.expires_at,
                    )
                )
            if approval.state is ApprovalState.EXPIRED:
                return self._remember_resolution_locked(
                    self._reject(
                        approval_key,
                        resolution_request_id,
                        normalized_decision,
                        "approval_expired",
                        status=410,
                        state=approval.state,
                        expires_at=approval.expires_at,
                    )
                )
            if approval.state is ApprovalState.REVOKED:
                return self._remember_resolution_locked(
                    self._reject(
                        approval_key,
                        resolution_request_id,
                        normalized_decision,
                        "approval_revoked",
                        status=409,
                        state=approval.state,
                        expires_at=approval.expires_at,
                    )
                )
            if approval.state is not ApprovalState.REQUESTED:
                return self._remember_resolution_locked(
                    self._reject(
                        approval_key,
                        resolution_request_id,
                        normalized_decision,
                        "approval_terminal",
                        status=409,
                        state=approval.state,
                        expires_at=approval.expires_at,
                    )
                )
            if normalized_decision == "allow_session" and not approval.session_grant:
                return self._remember_resolution_locked(
                    self._reject(
                        approval_key,
                        resolution_request_id,
                        normalized_decision,
                        "session_grant_not_requested",
                        status=409,
                        state=approval.state,
                        expires_at=approval.expires_at,
                    )
                )
            now = self._now()
            state = {
                "allow_once": ApprovalState.ALLOWED_ONCE,
                "allow_session": ApprovalState.ALLOWED_SESSION,
                "deny": ApprovalState.DENIED,
                "cancel": ApprovalState.CANCELLED,
            }[normalized_decision]
            grant_id = f"grant-{uuid.uuid4().hex}" if state in {ApprovalState.ALLOWED_ONCE, ApprovalState.ALLOWED_SESSION} else None
            updated = replace(
                approval,
                state=state,
                resolved_at=now,
                resolution_request_id=resolution_request_id,
            )
            self._requests[approval_key] = updated
            result = ApprovalResolution(
                approval_id=approval_key,
                request_id=resolution_request_id,
                decision=normalized_decision,
                state=state,
                accepted=True,
                reason_code=state.value,
                expires_at=updated.expires_at,
                grant_id=grant_id,
            )
            return self._remember_resolution_locked(result)

    def revoke(
        self,
        approval_id: str,
        *,
        expected_epoch: int,
        request_id: str,
        expected_state: ApprovalState | str | None = None,
        world_id: str | None = None,
        engram_id: str | None = None,
        turn_id: str | None = None,
        session_id: str | None = None,
    ) -> ApprovalResolution:
        """Revoke a pending or granted approval; repeated calls are safe."""

        approval_key = _identifier(approval_id, "approval_id") or ""
        resolution_request_id = _identifier(request_id, "request_id") or ""
        expected = _epoch(expected_epoch)
        expected_status = _state(expected_state) if expected_state is not None else None
        with self._lock:
            existing_resolution = self._resolutions.get((approval_key, resolution_request_id))
            if existing_resolution is not None:
                return replace(existing_resolution, idempotent=True)
            approval = self._requests.get(approval_key)
            if approval is None:
                return self._remember_resolution_locked(
                    self._reject(approval_key, resolution_request_id, "cancel", "approval_unknown", status=404)
                )
            self._refresh_locked(self._now(), approval_key)
            approval = self._requests[approval_key]
            mismatch = self._scope_mismatch(
                approval,
                expected_epoch=expected,
                world_id=world_id,
                engram_id=engram_id,
                turn_id=turn_id,
                session_id=session_id,
            )
            if mismatch is not None:
                return self._remember_resolution_locked(
                    self._reject(approval_key, resolution_request_id, "cancel", mismatch, status=409)
                )
            if expected_status is not None and approval.state is not expected_status:
                return self._remember_resolution_locked(
                    self._reject(
                        approval_key,
                        resolution_request_id,
                        "cancel",
                        "approval_state_mismatch",
                        status=409,
                        state=approval.state,
                        expires_at=approval.expires_at,
                    )
                )
            if approval.state is ApprovalState.REVOKED:
                return self._remember_resolution_locked(
                    self._reject(
                        approval_key,
                        resolution_request_id,
                        "cancel",
                        "approval_revoked",
                        status=409,
                        state=approval.state,
                        expires_at=approval.expires_at,
                    )
                )
            if approval.state is ApprovalState.EXPIRED:
                return self._remember_resolution_locked(
                    self._reject(
                        approval_key,
                        resolution_request_id,
                        "cancel",
                        "approval_expired",
                        status=410,
                        state=approval.state,
                        expires_at=approval.expires_at,
                    )
                )
            now = self._now()
            updated = replace(
                approval,
                state=ApprovalState.REVOKED,
                revoked_at=now,
                resolved_at=now,
                resolution_request_id=resolution_request_id,
            )
            self._requests[approval_key] = updated
            return self._remember_resolution_locked(
                ApprovalResolution(
                    approval_id=approval_key,
                    request_id=resolution_request_id,
                    decision="cancel",
                    state=ApprovalState.REVOKED,
                    accepted=True,
                    reason_code="revoked",
                    expires_at=updated.expires_at,
                )
            )

    def revoke_scope(
        self,
        *,
        world_id: str,
        engram_id: str,
        expected_epoch: int,
        session_id: str | None = None,
    ) -> tuple[ApprovalResolution, ...]:
        """Revoke every active grant in one world/Engram/session scope."""

        world = _identifier(world_id, "world_id") or ""
        engram = _identifier(engram_id, "engram_id") or ""
        expected = _epoch(expected_epoch)
        session = _identifier(session_id, "session_id") if session_id is not None else None
        with self._lock:
            targets = [
                item
                for item in self._requests.values()
                if item.world_id == world
                and item.engram_id == engram
                and item.epoch == expected
                and (session is None or item.session_id == session)
                and item.state in {ApprovalState.REQUESTED, ApprovalState.ALLOWED_ONCE, ApprovalState.ALLOWED_SESSION}
            ]
        return tuple(
            self.revoke(
                item.approval_id,
                expected_epoch=expected,
                request_id=f"revoke-scope-{uuid.uuid4().hex}",
                world_id=world,
                engram_id=engram,
                turn_id=item.turn_id,
                session_id=session,
            )
            for item in targets
        )

    def check(
        self,
        approval_id: str,
        *,
        expected_epoch: int,
        world_id: str,
        engram_id: str,
        session_id: str | None = None,
        capability: str | None = None,
        consume: bool = False,
    ) -> ApprovalCheck:
        """Check a grant and optionally consume its one-time decision."""

        approval_key = _identifier(approval_id, "approval_id") or ""
        world = _identifier(world_id, "world_id") or ""
        engram = _identifier(engram_id, "engram_id") or ""
        expected = _epoch(expected_epoch)
        session = _identifier(session_id, "session_id") if session_id is not None else None
        with self._lock:
            approval = self._requests.get(approval_key)
            if approval is None:
                return self._check_denied(approval_key, "approval_unknown", status=404)
            self._refresh_locked(self._now(), approval_key)
            approval = self._requests[approval_key]
            if approval.epoch != expected:
                return self._check_denied(approval_key, "stale_epoch", state=approval.state)
            if approval.world_id != world or approval.engram_id != engram:
                return self._check_denied(approval_key, "approval_scope_mismatch", state=approval.state)
            if approval.session_grant and approval.session_id != session:
                return self._check_denied(approval_key, "session_scope_mismatch", state=approval.state)
            if capability is not None and capability not in approval.capability_scope and f"capability:{capability}" not in approval.capability_scope:
                return self._check_denied(approval_key, "capability_scope_mismatch", state=approval.state)
            if approval.state is ApprovalState.EXPIRED:
                return self._check_denied(approval_key, "approval_expired", state=approval.state, expires_at=approval.expires_at)
            if approval.state is ApprovalState.REVOKED:
                return self._check_denied(approval_key, "approval_revoked", state=approval.state, expires_at=approval.expires_at)
            if approval.state is ApprovalState.ALLOWED_ONCE:
                if approval.used:
                    return self._check_denied(approval_key, "approval_consumed", state=approval.state, expires_at=approval.expires_at)
                if consume:
                    self._requests[approval_key] = replace(approval, used=True)
                    approval = self._requests[approval_key]
                return ApprovalCheck(
                    allowed=True,
                    approval_id=approval_key,
                    state=approval.state,
                    reason_code="allowed_once",
                    consumed=consume,
                    expires_at=approval.expires_at,
                    capability_scope=approval.capability_scope,
                )
            if approval.state is ApprovalState.ALLOWED_SESSION:
                return ApprovalCheck(
                    allowed=True,
                    approval_id=approval_key,
                    state=approval.state,
                    reason_code="allowed_session",
                    expires_at=approval.expires_at,
                    capability_scope=approval.capability_scope,
                )
            return self._check_denied(approval_key, "approval_not_allowed", state=approval.state, expires_at=approval.expires_at)

    def is_allowed(self, approval_id: str, **kwargs: Any) -> bool:
        """Boolean convenience wrapper that keeps full detail in ``check``."""

        return self.check(approval_id, **kwargs).allowed

    def snapshot(self) -> dict[str, Any]:
        """Return a bounded safe snapshot for diagnostics."""

        with self._lock:
            self._prune_locked(self._now())
            return {
                "count": len(self._requests),
                "max_entries": self.max_entries,
                "requests": [item.to_dict() for item in self._requests.values()],
                "evidence_class": CONTRACT_ONLY,
            }

    def _now(self) -> datetime:
        return _ensure_utc(self._clock(), field_name="clock")

    def _ensure_capacity_locked(self) -> None:
        if len(self._requests) < self.max_entries:
            return
        # Terminal entries are safe to evict from an in-memory registry.  The
        # durable event projection remains the audit source for Runtime.
        for approval_id, item in tuple(self._requests.items()):
            if item.state.value in _TERMINAL_STATES:
                self._requests.pop(approval_id, None)
                self._request_ids.pop(item.request_id, None)
                for key in tuple(self._resolutions):
                    if key[0] == approval_id:
                        self._resolutions.pop(key, None)
                if len(self._requests) < self.max_entries:
                    return
        raise ApprovalError(
            "approval_capacity",
            "the bounded approval registry has no reclaimable entries",
            status=503,
            retryable=True,
        )

    def _prune_locked(self, now: datetime) -> None:
        for approval_id in tuple(self._requests):
            self._refresh_locked(now, approval_id)

    def _refresh_locked(self, now: datetime, approval_id: str | None) -> None:
        if approval_id is None:
            return
        item = self._requests.get(approval_id)
        if item is None or item.state not in {ApprovalState.REQUESTED, ApprovalState.ALLOWED_ONCE, ApprovalState.ALLOWED_SESSION}:
            return
        if now >= item.expires_at:
            self._requests[approval_id] = replace(
                item,
                state=ApprovalState.EXPIRED,
                resolved_at=now,
            )

    def _remember_resolution_locked(self, result: ApprovalResolution) -> ApprovalResolution:
        self._resolutions[(result.approval_id, result.request_id)] = result
        return result

    @staticmethod
    def _same_request(
        item: HarnessApprovalRequest,
        *,
        world_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        target_kind: str,
        policy_id: str,
        session_grant: bool,
        session_id: str | None,
        capability_scope: tuple[str, ...],
    ) -> bool:
        return (
            item.world_id == world_id
            and item.engram_id == engram_id
            and item.turn_id == turn_id
            and item.epoch == epoch
            and item.target_kind == target_kind
            and item.policy_id == policy_id
            and item.session_grant == session_grant
            and item.session_id == session_id
            and item.capability_scope == capability_scope
        )

    @staticmethod
    def _safe_scope(values: Iterable[str]) -> tuple[str, ...]:
        output: list[str] = []
        for value in values:
            if not isinstance(value, str) or not value.strip() or len(value.strip()) > _IDENTIFIER_MAX:
                raise ValueError("capability scope entries must be bounded strings")
            value = value.strip()
            if value not in output:
                output.append(value)
        return tuple(output)

    @staticmethod
    def _scope_mismatch(
        approval: HarnessApprovalRequest,
        *,
        expected_epoch: int,
        world_id: str | None,
        engram_id: str | None,
        turn_id: str | None,
        session_id: str | None,
    ) -> str | None:
        if approval.epoch != expected_epoch:
            return "stale_epoch"
        if world_id is not None and approval.world_id != world_id:
            return "world_scope_mismatch"
        if engram_id is not None and approval.engram_id != engram_id:
            return "engram_scope_mismatch"
        if turn_id is not None and approval.turn_id != turn_id:
            return "turn_scope_mismatch"
        if approval.session_grant and approval.session_id != session_id:
            return "session_scope_mismatch"
        return None

    @staticmethod
    def _reject(
        approval_id: str,
        request_id: str,
        decision: str,
        reason_code: str,
        *,
        status: int,
        state: ApprovalState | None = None,
        expires_at: datetime | None = None,
    ) -> ApprovalResolution:
        error = PolicyError(
            reason_code,
            "the approval request was not accepted",
            status=status,
            retryable=status in {409, 503},
        )
        return ApprovalResolution(
            approval_id=approval_id,
            request_id=request_id,
            decision=decision,
            state=state,
            accepted=False,
            reason_code=reason_code,
            expires_at=expires_at,
            error=error,
        )

    @staticmethod
    def _check_denied(
        approval_id: str,
        reason_code: str,
        *,
        status: int = 409,
        state: ApprovalState | None = None,
        expires_at: datetime | None = None,
    ) -> ApprovalCheck:
        return ApprovalCheck(
            allowed=False,
            approval_id=approval_id,
            state=state,
            reason_code=reason_code,
            expires_at=expires_at,
            error=PolicyError(
                reason_code,
                "the approval grant cannot be used for this request",
                status=status,
                retryable=status in {409, 503},
            ),
        )

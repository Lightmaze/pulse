"""Bounded structured file-change and diff contracts.

Parsing a patch is only a proposal.  It is never reported as an applied file
change.  Applying a ChangeSet requires an explicit policy decision and an
injected adapter; the default adapter returns unsupported_execution without
touching the workspace.
"""

from __future__ import annotations

import hashlib
import os
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Protocol

from .security import (
    CONTRACT_ONLY,
    resolve_policy_decision,
)

__all__ = [
    "ChangeApplier",
    "ChangeApplyResult",
    "ChangeKind",
    "ChangeSet",
    "ChangeStatus",
    "ChangeValidationError",
    "CONTRACT_ONLY",
    "FileChange",
    "UnsupportedChangeApplier",
    "normalize_relative_path",
]


_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:")
_DEFAULT_PATCH_CAP = 512 * 1024
_DEFAULT_PREVIEW_CAP = 64 * 1024
_REDACTION_MARKER = "[REDACTION_REQUIRED]"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _non_empty(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ChangeValidationError(f"{field_name} must be a non-empty string")
    if "\x00" in value:
        raise ChangeValidationError(f"{field_name} must not contain NUL")
    return value


class ChangeValidationError(ValueError):
    """A path or patch cannot be safely represented."""


class ChangeKind(StrEnum):
    ADD = "add"
    DELETE = "delete"
    UPDATE = "update"
    MOVE = "move"


class ChangeStatus(StrEnum):
    PROPOSED = "proposed"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    DECLINED = "declined"
    UNCERTAIN = "uncertain"


class ChangeApplyState(StrEnum):
    APPLIED = "applied"
    FAILED = "failed"
    DECLINED = "declined"
    UNCERTAIN = "uncertain"
    UNSUPPORTED = "unsupported_execution"


def normalize_relative_path(value: str) -> str:
    """Normalize a workspace-relative path and reject traversal."""

    _non_empty(value, "path")
    normalized = value.replace("\\", "/")
    if (
        normalized.startswith("/")
        or normalized.startswith("//")
        or _WINDOWS_DRIVE.match(normalized)
        or os.path.isabs(value)
    ):
        raise ChangeValidationError("absolute paths are not allowed")
    parts = normalized.split("/")
    if any(not part or part in {".", ".."} for part in parts):
        raise ChangeValidationError("path contains an empty, dot, or parent segment")
    if any(":" in part for part in parts):
        raise ChangeValidationError("path contains a forbidden colon")
    return "/".join(parts)


def _patch_path(raw: str) -> str | None:
    value = raw.strip()
    if "\t" in value:
        value = value.split("\t", 1)[0]
    if value == "/dev/null":
        return None
    if value.startswith('"') or value.endswith('"'):
        raise ChangeValidationError("quoted patch paths are not supported")
    if value.startswith("a/") or value.startswith("b/"):
        value = value[2:]
    return normalize_relative_path(value)


def _safe_preview(
    patch: str,
    *,
    max_preview_bytes: int,
    redactor: Callable[[str], str] | None,
) -> tuple[str, bool, bool]:
    if type(max_preview_bytes) is not int or max_preview_bytes <= 0:
        raise ChangeValidationError("max_preview_bytes must be a positive int")
    if redactor is None:
        return _REDACTION_MARKER, True, False
    try:
        safe = redactor(patch)
        if not isinstance(safe, str):
            raise ChangeValidationError("diff redactor must return text")
    except Exception:
        return "[REDACTION_FAILED]", True, False
    encoded = safe.encode("utf-8", errors="replace")
    truncated = len(encoded) > max_preview_bytes
    if truncated:
        encoded = encoded[:max_preview_bytes]
        safe = encoded.decode("utf-8", errors="replace")
    return safe, safe != patch, truncated


def _parse_unified_patch(
    patch: str,
) -> list[tuple[str | None, str | None, bool]]:
    """Parse file headers without interpreting file contents."""

    pairs: list[tuple[str | None, str | None, bool]] = []
    old: str | None = None
    new: str | None = None
    old_seen = False
    new_seen = False
    rename_from: str | None = None
    rename_to: str | None = None

    def flush() -> None:
        nonlocal old, new, old_seen, new_seen, rename_from, rename_to
        if rename_from is not None or rename_to is not None:
            if rename_from is None or rename_to is None:
                raise ChangeValidationError("rename headers must appear as a pair")
            pairs.append((_patch_path(rename_from), _patch_path(rename_to), True))
        elif old_seen or new_seen:
            if not old_seen or not new_seen:
                raise ChangeValidationError("unified patch headers must appear as a pair")
            pairs.append((old, new, False))
        old = None
        new = None
        old_seen = False
        new_seen = False
        rename_from = None
        rename_to = None

    for line in patch.splitlines():
        if line.startswith("diff --git "):
            flush()
        elif line.startswith("--- "):
            if old_seen:
                flush()
            old = _patch_path(line[4:])
            old_seen = True
        elif line.startswith("+++ "):
            new = _patch_path(line[4:])
            new_seen = True
        elif line.startswith("rename from "):
            rename_from = line[len("rename from ") :]
        elif line.startswith("rename to "):
            rename_to = line[len("rename to ") :]
    flush()
    if not pairs:
        raise ChangeValidationError("patch contains no supported file headers")
    return pairs


@dataclass(frozen=True, slots=True)
class FileChange:
    """One safe, workspace-relative change proposal."""

    path: str
    kind: ChangeKind
    status: ChangeStatus = ChangeStatus.PROPOSED
    move_path: str | None = None
    diff_preview: str = _REDACTION_MARKER
    diff_digest: str | None = None
    before_digest: str | None = None
    after_digest: str | None = None
    redacted: bool = True
    truncated: bool = False
    reject_reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", normalize_relative_path(self.path))
        object.__setattr__(self, "kind", ChangeKind(self.kind))
        object.__setattr__(self, "status", ChangeStatus(self.status))
        if self.kind is ChangeKind.MOVE:
            if self.move_path is None:
                raise ChangeValidationError("move change requires move_path")
            object.__setattr__(
                self,
                "move_path",
                normalize_relative_path(self.move_path),
            )
        elif self.move_path is not None:
            object.__setattr__(
                self,
                "move_path",
                normalize_relative_path(self.move_path),
            )
        _non_empty(self.diff_preview, "diff_preview")
        if self.diff_digest is not None:
            _non_empty(self.diff_digest, "diff_digest")
        if type(self.redacted) is not bool or type(self.truncated) is not bool:
            raise ChangeValidationError("redacted and truncated must be bools")
        if self.reject_reason is not None:
            _non_empty(self.reject_reason, "reject_reason")

    @property
    def diff(self) -> str:
        return self.diff_preview

    def to_wire(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind.value,
            "status": self.status.value,
            "move_path": self.move_path,
            "diff_preview": self.diff_preview,
            "diff_digest": self.diff_digest,
            "before_digest": self.before_digest,
            "after_digest": self.after_digest,
            "redacted": self.redacted,
            "truncated": self.truncated,
            "reject_reason": self.reject_reason,
        }


@dataclass(frozen=True, slots=True)
class ChangeApplyResult:
    """Result of an adapter application, separate from the proposal."""

    change_set_id: str
    state: ChangeApplyState
    applied_paths: tuple[str, ...] = ()
    failed_paths: tuple[str, ...] = ()
    evidence_class: str = CONTRACT_ONLY
    error_code: str | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        _non_empty(self.change_set_id, "change_set_id")
        object.__setattr__(self, "state", ChangeApplyState(self.state))
        object.__setattr__(
            self,
            "applied_paths",
            tuple(normalize_relative_path(path) for path in self.applied_paths),
        )
        object.__setattr__(
            self,
            "failed_paths",
            tuple(normalize_relative_path(path) for path in self.failed_paths),
        )
        _non_empty(self.evidence_class, "evidence_class")
        if self.error_code is not None:
            _non_empty(self.error_code, "error_code")
        if self.detail is not None:
            _non_empty(self.detail, "detail")

    def to_wire(self) -> dict[str, Any]:
        return {
            "change_set_id": self.change_set_id,
            "state": self.state.value,
            "applied_paths": list(self.applied_paths),
            "failed_paths": list(self.failed_paths),
            "evidence_class": self.evidence_class,
            "error_code": self.error_code,
            "detail": self.detail,
        }


class ChangeApplier(Protocol):
    """Injected file-change adapter boundary."""

    supports_execution: bool
    evidence_class: str

    def apply(self, change_set: "ChangeSet") -> ChangeApplyResult:
        ...


class UnsupportedChangeApplier:
    """Safe default which cannot mutate a workspace."""

    supports_execution = False
    evidence_class = CONTRACT_ONLY

    def apply(self, change_set: "ChangeSet") -> ChangeApplyResult:
        return ChangeApplyResult(
            change_set_id=change_set.id,
            state=ChangeApplyState.UNSUPPORTED,
            evidence_class=CONTRACT_ONLY,
            error_code="unsupported_execution",
            detail="no file-change adapter is connected",
        )


@dataclass(frozen=True, slots=True)
class ChangeSet:
    """A bounded patch proposal that is not an applied result."""

    id: str
    turn_id: str
    request_id: str
    changes: tuple[FileChange, ...]
    status: ChangeStatus
    patch_digest: str
    diff_preview: str
    created_at: str
    evidence_class: str = CONTRACT_ONLY
    reject_reason: str | None = None
    redacted: bool = True
    truncated: bool = False
    applied: bool = False

    def __post_init__(self) -> None:
        _non_empty(self.id, "change_set id")
        _non_empty(self.turn_id, "turn_id")
        _non_empty(self.request_id, "request_id")
        object.__setattr__(self, "changes", tuple(self.changes))
        object.__setattr__(self, "status", ChangeStatus(self.status))
        _non_empty(self.patch_digest, "patch_digest")
        _non_empty(self.diff_preview, "diff_preview")
        _non_empty(self.created_at, "created_at")
        _non_empty(self.evidence_class, "evidence_class")
        if type(self.redacted) is not bool:
            raise ChangeValidationError("redacted must be a bool")
        if type(self.truncated) is not bool:
            raise ChangeValidationError("truncated must be a bool")
        if type(self.applied) is not bool:
            raise ChangeValidationError("applied must be a bool")
        if self.status is ChangeStatus.PROPOSED and self.applied:
            raise ChangeValidationError("a proposed change set cannot be applied")
        if self.reject_reason is not None:
            _non_empty(self.reject_reason, "reject_reason")

    @classmethod
    def from_patch(
        cls,
        patch: str,
        *,
        turn_id: str,
        request_id: str | None = None,
        max_patch_bytes: int = _DEFAULT_PATCH_CAP,
        max_preview_bytes: int = _DEFAULT_PREVIEW_CAP,
        redactor: Callable[[str], str] | None = None,
    ) -> "ChangeSet":
        _non_empty(turn_id, "turn_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
        _non_empty(request_id, "request_id")
        if not isinstance(patch, str):
            raise ChangeValidationError("patch must be text")
        encoded = patch.encode("utf-8", errors="strict")
        patch_digest = _digest(encoded)
        if type(max_patch_bytes) is not int or max_patch_bytes <= 0:
            raise ChangeValidationError("max_patch_bytes must be a positive int")
        if len(encoded) > max_patch_bytes:
            return cls._declined(
                turn_id=turn_id,
                request_id=request_id,
                patch_digest=patch_digest,
                reason="patch_too_large",
            )
        try:
            pairs = _parse_unified_patch(patch)
            preview, redacted, truncated = _safe_preview(
                patch,
                max_preview_bytes=max_preview_bytes,
                redactor=redactor,
            )
            changes: list[FileChange] = []
            for old, new, explicit_rename in pairs:
                if old is None and new is None:
                    raise ChangeValidationError("a change must have at least one path")
                if old is None:
                    kind = ChangeKind.ADD
                    path = new
                    move_path = None
                elif new is None:
                    kind = ChangeKind.DELETE
                    path = old
                    move_path = None
                elif explicit_rename or old != new:
                    kind = ChangeKind.MOVE
                    path = old
                    move_path = new
                else:
                    kind = ChangeKind.UPDATE
                    path = old
                    move_path = None
                assert path is not None
                changes.append(
                    FileChange(
                        path=path,
                        kind=kind,
                        move_path=move_path,
                        diff_preview=preview,
                        diff_digest=patch_digest,
                        redacted=redacted,
                        truncated=truncated,
                    )
                )
        except ChangeValidationError as exc:
            return cls._declined(
                turn_id=turn_id,
                request_id=request_id,
                patch_digest=patch_digest,
                reason=str(exc),
            )
        return cls(
            id=uuid.uuid4().hex,
            turn_id=turn_id,
            request_id=request_id,
            changes=tuple(changes),
            status=ChangeStatus.PROPOSED,
            patch_digest=patch_digest,
            diff_preview=preview,
            created_at=_utc_now(),
            reject_reason=None,
            redacted=redacted,
            truncated=truncated,
            applied=False,
        )

    @classmethod
    def _declined(
        cls,
        *,
        turn_id: str,
        request_id: str,
        patch_digest: str,
        reason: str,
    ) -> "ChangeSet":
        return cls(
            id=uuid.uuid4().hex,
            turn_id=turn_id,
            request_id=request_id,
            changes=(),
            status=ChangeStatus.DECLINED,
            patch_digest=patch_digest,
            diff_preview=_REDACTION_MARKER,
            created_at=_utc_now(),
            reject_reason=reason,
            redacted=True,
            truncated=False,
            applied=False,
        )

    def safe_summary(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "turn_id": self.turn_id,
            "request_id": self.request_id,
            "status": self.status.value,
            "paths": [
                change.move_path
                if change.kind is ChangeKind.MOVE
                else change.path
                for change in self.changes
            ],
            "kinds": [change.kind.value for change in self.changes],
            "patch_digest": self.patch_digest,
        }

    def apply(
        self,
        *,
        policy_context: Any = None,
        applier: ChangeApplier | None = None,
    ) -> ChangeApplyResult:
        if self.status is ChangeStatus.DECLINED:
            return ChangeApplyResult(
                change_set_id=self.id,
                state=ChangeApplyState.DECLINED,
                evidence_class=CONTRACT_ONLY,
                error_code="change_set_declined",
                detail=self.reject_reason or "change set was declined",
            )
        request = {
            "action": "change.apply",
            "operation": "file_change",
            "path": self.changes[0].path if self.changes else ".",
            "change_set": self.safe_summary(),
        }
        decision = resolve_policy_decision(
            policy_context,
            request,
            action="change.apply",
        )
        if not decision.allow:
            return ChangeApplyResult(
                change_set_id=self.id,
                state=ChangeApplyState.DECLINED,
                evidence_class=decision.evidence_class,
                error_code="policy_denied",
                detail=decision.reason_code,
            )
        adapter = applier or UnsupportedChangeApplier()
        if not getattr(adapter, "supports_execution", False):
            return ChangeApplyResult(
                change_set_id=self.id,
                state=ChangeApplyState.UNSUPPORTED,
                evidence_class=CONTRACT_ONLY,
                error_code="unsupported_execution",
                detail="no file-change adapter is connected",
            )
        try:
            result = adapter.apply(self)
        except Exception as exc:
            return ChangeApplyResult(
                change_set_id=self.id,
                state=ChangeApplyState.UNCERTAIN,
                evidence_class=str(
                    getattr(adapter, "evidence_class", CONTRACT_ONLY)
                ),
                error_code="apply_uncertain",
                detail=f"{type(exc).__name__}: file application evidence is incomplete",
            )
        if not isinstance(result, ChangeApplyResult):
            raise ChangeValidationError(
                "change applier must return ChangeApplyResult"
            )
        return result

    def to_wire(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "turn_id": self.turn_id,
            "request_id": self.request_id,
            "changes": [change.to_wire() for change in self.changes],
            "status": self.status.value,
            "patch_digest": self.patch_digest,
            "diff_preview": self.diff_preview,
            "created_at": self.created_at,
            "evidence_class": self.evidence_class,
            "reject_reason": self.reject_reason,
            "redacted": self.redacted,
            "truncated": self.truncated,
            "applied": self.applied,
        }

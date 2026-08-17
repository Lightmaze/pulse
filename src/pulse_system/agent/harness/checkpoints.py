"""Safe checkpoint and worktree abstraction contracts.

The default backend is deliberately unsupported.  Creating a reference in
this module never creates a git ref, worktree, archive, or workspace file.
Only an explicitly injected backend may perform a real operation after policy,
workspace, epoch, and changed-path checks pass.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from .changes import ChangeValidationError, normalize_relative_path
from .security import (
    CONTRACT_ONLY,
    resolve_policy_decision,
)

__all__ = [
    "BackendCheckpointResult",
    "BackendDropResult",
    "BackendRestoreResult",
    "CheckpointBackend",
    "CheckpointRef",
    "CheckpointScope",
    "CheckpointState",
    "CheckpointStore",
    "CheckpointValidationError",
    "DropResult",
    "CONTRACT_ONLY",
    "RestoreResult",
    "RestoreState",
    "UnsupportedCheckpointBackend",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _non_empty(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CheckpointValidationError(f"{field_name} must be a non-empty string")
    if "\x00" in value:
        raise CheckpointValidationError(f"{field_name} must not contain NUL")
    return value


def _workspace_digest(root: Path) -> str:
    normalized = os.path.normcase(str(root)).encode("utf-8", errors="strict")
    return hashlib.sha256(normalized).hexdigest()


class CheckpointValidationError(ValueError):
    """Checkpoint input failed a workspace or recovery safety check."""


class CheckpointState(StrEnum):
    CREATED = "created"
    RESTORED = "restored"
    DROPPED = "dropped"
    FAILED = "failed"
    UNCERTAIN = "uncertain"
    DECLINED = "declined"
    UNSUPPORTED = "unsupported_execution"
    STALE = "stale"


class RestoreState(StrEnum):
    RESTORED = "restored"
    FAILED = "failed"
    UNCERTAIN = "uncertain"
    DECLINED = "declined"
    UNSUPPORTED = "unsupported_execution"
    STALE = "stale"
    NOT_FOUND = "not_found"


@dataclass(frozen=True, slots=True)
class CheckpointScope:
    """Identity and bounded path set protected by a checkpoint."""

    turn_id: str
    world_id: str
    engram_id: str
    epoch: int
    workspace_root: str | os.PathLike[str]
    changed_paths: Sequence[str] = ()
    label: str = ""

    def __post_init__(self) -> None:
        _non_empty(self.turn_id, "turn_id")
        _non_empty(self.world_id, "world_id")
        _non_empty(self.engram_id, "engram_id")
        if type(self.epoch) is not int or self.epoch < 0:
            raise CheckpointValidationError("epoch must be a non-negative int")
        root = Path(os.fspath(self.workspace_root)).expanduser()
        if not root.is_absolute():
            raise CheckpointValidationError("workspace_root must be absolute")
        try:
            root = root.resolve(strict=True)
        except OSError as exc:
            raise CheckpointValidationError("workspace_root must resolve") from exc
        if not root.is_dir():
            raise CheckpointValidationError("workspace_root must be a directory")
        object.__setattr__(self, "workspace_root", str(root))

        normalized = sorted(
            {normalize_relative_path(path) for path in self.changed_paths}
        )
        object.__setattr__(self, "changed_paths", tuple(normalized))
        if self.label:
            _non_empty(self.label, "label")

    @property
    def root_path(self) -> Path:
        return Path(self.workspace_root)

    @property
    def workspace_digest(self) -> str:
        return _workspace_digest(self.root_path)

    @property
    def scope_digest(self) -> str:
        joined = "\x00".join(
            (
                self.turn_id,
                self.world_id,
                self.engram_id,
                str(self.epoch),
                self.workspace_digest,
                *self.changed_paths,
            )
        )
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    def safe_wire(self) -> dict[str, Any]:
        return {
            "turn_id": self.turn_id,
            "world_id": self.world_id,
            "engram_id": self.engram_id,
            "epoch": self.epoch,
            "workspace_digest": self.workspace_digest,
            "changed_paths": list(self.changed_paths),
            "label": self.label or None,
            "scope_digest": self.scope_digest,
        }


@dataclass(frozen=True, slots=True)
class BackendCheckpointResult:
    state: CheckpointState
    backend_ref: str | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "state", CheckpointState(self.state))
        if self.backend_ref is not None:
            _non_empty(self.backend_ref, "backend_ref")
        if self.reason is not None:
            _non_empty(self.reason, "reason")


@dataclass(frozen=True, slots=True)
class BackendRestoreResult:
    state: RestoreState
    applied_paths: tuple[str, ...] = ()
    reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "state", RestoreState(self.state))
        object.__setattr__(
            self,
            "applied_paths",
            tuple(normalize_relative_path(path) for path in self.applied_paths),
        )
        if self.reason is not None:
            _non_empty(self.reason, "reason")


@dataclass(frozen=True, slots=True)
class BackendDropResult:
    state: CheckpointState
    reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "state", CheckpointState(self.state))
        if self.reason is not None:
            _non_empty(self.reason, "reason")


@dataclass(frozen=True, slots=True)
class CheckpointRef:
    """An opaque, safe reference; it contains no absolute workspace path."""

    id: str
    turn_id: str
    world_id: str
    engram_id: str
    epoch: int
    workspace_digest: str
    scope_digest: str
    changed_paths: tuple[str, ...]
    state: CheckpointState
    backend: str
    backend_ref: str | None
    evidence_class: str
    created_at: str
    updated_at: str
    reason: str | None = None

    def __post_init__(self) -> None:
        for value, name in (
            (self.id, "checkpoint id"),
            (self.turn_id, "turn_id"),
            (self.world_id, "world_id"),
            (self.engram_id, "engram_id"),
            (self.workspace_digest, "workspace_digest"),
            (self.scope_digest, "scope_digest"),
            (self.backend, "backend"),
            (self.evidence_class, "evidence_class"),
            (self.created_at, "created_at"),
            (self.updated_at, "updated_at"),
        ):
            _non_empty(value, name)
        if type(self.epoch) is not int or self.epoch < 0:
            raise CheckpointValidationError("checkpoint epoch must be non-negative")
        object.__setattr__(
            self,
            "changed_paths",
            tuple(sorted({normalize_relative_path(path) for path in self.changed_paths})),
        )
        object.__setattr__(self, "state", CheckpointState(self.state))
        if self.backend_ref is not None:
            _non_empty(self.backend_ref, "backend_ref")
        if self.reason is not None:
            _non_empty(self.reason, "reason")

    @property
    def checkpoint_id(self) -> str:
        return self.id

    def to_wire(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "turn_id": self.turn_id,
            "world_id": self.world_id,
            "engram_id": self.engram_id,
            "epoch": self.epoch,
            "workspace_digest": self.workspace_digest,
            "scope_digest": self.scope_digest,
            "changed_paths": list(self.changed_paths),
            "state": self.state.value,
            "backend": self.backend,
            "backend_ref": self.backend_ref,
            "evidence_class": self.evidence_class,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class RestoreResult:
    checkpoint_id: str
    state: RestoreState
    workspace_digest: str | None
    requested_paths: tuple[str, ...] = ()
    applied_paths: tuple[str, ...] = ()
    evidence_class: str = CONTRACT_ONLY
    error_code: str | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        _non_empty(self.checkpoint_id, "checkpoint_id")
        object.__setattr__(self, "state", RestoreState(self.state))
        object.__setattr__(
            self,
            "requested_paths",
            tuple(normalize_relative_path(path) for path in self.requested_paths),
        )
        object.__setattr__(
            self,
            "applied_paths",
            tuple(normalize_relative_path(path) for path in self.applied_paths),
        )
        _non_empty(self.evidence_class, "evidence_class")
        if self.error_code is not None:
            _non_empty(self.error_code, "error_code")
        if self.detail is not None:
            _non_empty(self.detail, "detail")

    def to_wire(self) -> dict[str, Any]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "state": self.state.value,
            "workspace_digest": self.workspace_digest,
            "requested_paths": list(self.requested_paths),
            "applied_paths": list(self.applied_paths),
            "evidence_class": self.evidence_class,
            "error_code": self.error_code,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class DropResult:
    checkpoint_id: str
    state: CheckpointState
    evidence_class: str = CONTRACT_ONLY
    error_code: str | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        _non_empty(self.checkpoint_id, "checkpoint_id")
        object.__setattr__(self, "state", CheckpointState(self.state))
        _non_empty(self.evidence_class, "evidence_class")
        if self.error_code is not None:
            _non_empty(self.error_code, "error_code")
        if self.detail is not None:
            _non_empty(self.detail, "detail")

    def to_wire(self) -> dict[str, Any]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "state": self.state.value,
            "evidence_class": self.evidence_class,
            "error_code": self.error_code,
            "detail": self.detail,
        }


class CheckpointBackend(Protocol):
    """Injected git/worktree/archive backend boundary."""

    supports_checkpoints: bool
    evidence_class: str

    def create(
        self,
        scope: CheckpointScope,
        *,
        checkpoint_id: str,
    ) -> BackendCheckpointResult:
        ...

    def restore(
        self,
        reference: CheckpointRef,
        scope: CheckpointScope,
        *,
        changed_paths: tuple[str, ...],
    ) -> BackendRestoreResult:
        ...

    def drop(
        self,
        reference: CheckpointRef,
        scope: CheckpointScope,
    ) -> BackendDropResult:
        ...


class UnsupportedCheckpointBackend:
    """Safe default that never invokes git or writes a worktree."""

    supports_checkpoints = False
    evidence_class = CONTRACT_ONLY

    def create(
        self,
        scope: CheckpointScope,
        *,
        checkpoint_id: str,
    ) -> BackendCheckpointResult:
        return BackendCheckpointResult(
            state=CheckpointState.UNSUPPORTED,
            reason="unsupported_checkpoint_backend",
        )

    def restore(
        self,
        reference: CheckpointRef,
        scope: CheckpointScope,
        *,
        changed_paths: tuple[str, ...],
    ) -> BackendRestoreResult:
        return BackendRestoreResult(
            state=RestoreState.UNSUPPORTED,
            reason="unsupported_checkpoint_backend",
        )

    def drop(
        self,
        reference: CheckpointRef,
        scope: CheckpointScope,
    ) -> BackendDropResult:
        return BackendDropResult(
            state=CheckpointState.UNSUPPORTED,
            reason="unsupported_checkpoint_backend",
        )


class CheckpointStore:
    """Bounded in-memory reference registry with fail-closed recovery checks."""

    def __init__(
        self,
        *,
        backend: CheckpointBackend | None = None,
        policy_evaluator: Any = None,
        max_retained: int = 32,
    ) -> None:
        if type(max_retained) is not int or max_retained <= 0:
            raise CheckpointValidationError("max_retained must be a positive int")
        self._backend = backend or UnsupportedCheckpointBackend()
        self._policy_evaluator = policy_evaluator
        self._max_retained = max_retained
        self._references: dict[str, CheckpointRef] = {}

    def create(
        self,
        scope: CheckpointScope,
        *,
        policy_context: Any = None,
    ) -> CheckpointRef:
        if not isinstance(scope, CheckpointScope):
            raise TypeError("create requires CheckpointScope")
        checkpoint_id = uuid.uuid4().hex
        decision = resolve_policy_decision(
            policy_context if policy_context is not None else self._policy_evaluator,
            {
                "action": "checkpoint.create",
                "operation": "file_change",
                "path": scope.changed_paths[0] if scope.changed_paths else ".",
                "checkpoint_id": checkpoint_id,
                "scope": scope.safe_wire(),
            },
            action="checkpoint.create",
        )
        if not decision.allow:
            reference = self._reference(
                scope,
                checkpoint_id=checkpoint_id,
                state=CheckpointState.DECLINED,
                backend="none",
                backend_ref=None,
                evidence_class=decision.evidence_class,
                reason=decision.reason_code,
            )
            self._remember(reference)
            return reference
        if not getattr(self._backend, "supports_checkpoints", False):
            reference = self._reference(
                scope,
                checkpoint_id=checkpoint_id,
                state=CheckpointState.UNSUPPORTED,
                backend="unsupported",
                backend_ref=None,
                evidence_class=CONTRACT_ONLY,
                reason="unsupported_checkpoint_backend",
            )
            self._remember(reference)
            return reference
        try:
            result = self._backend.create(scope, checkpoint_id=checkpoint_id)
            if not isinstance(result, BackendCheckpointResult):
                raise CheckpointValidationError(
                    "checkpoint backend must return BackendCheckpointResult"
                )
            reference = self._reference(
                scope,
                checkpoint_id=checkpoint_id,
                state=result.state,
                backend=type(self._backend).__name__,
                backend_ref=result.backend_ref,
                evidence_class=str(
                    getattr(self._backend, "evidence_class", decision.evidence_class)
                ),
                reason=result.reason,
            )
        except Exception as exc:
            reference = self._reference(
                scope,
                checkpoint_id=checkpoint_id,
                state=CheckpointState.UNCERTAIN,
                backend=type(self._backend).__name__,
                backend_ref=None,
                evidence_class=str(
                    getattr(self._backend, "evidence_class", CONTRACT_ONLY)
                ),
                reason=f"{type(exc).__name__}: checkpoint creation evidence is incomplete",
            )
        self._remember(reference)
        return reference

    def list(
        self,
        *,
        turn_id: str | None = None,
    ) -> tuple[CheckpointRef, ...]:
        references = tuple(self._references.values())
        if turn_id is not None:
            _non_empty(turn_id, "turn_id")
            references = tuple(
                reference for reference in references if reference.turn_id == turn_id
            )
        return tuple(
            sorted(
                references,
                key=lambda reference: reference.created_at,
                reverse=True,
            )
        )

    def restore(
        self,
        checkpoint_id: str,
        *,
        workspace_root: str | os.PathLike[str],
        expected_epoch: int,
        changed_paths: Sequence[str] = (),
        policy_context: Any = None,
    ) -> RestoreResult:
        _non_empty(checkpoint_id, "checkpoint_id")
        reference = self._references.get(checkpoint_id)
        if reference is None:
            return RestoreResult(
                checkpoint_id=checkpoint_id,
                state=RestoreState.NOT_FOUND,
                workspace_digest=None,
                evidence_class=CONTRACT_ONLY,
                error_code="checkpoint_not_found",
                detail="checkpoint reference is not retained",
            )
        if type(expected_epoch) is not int or expected_epoch < 0:
            raise CheckpointValidationError("expected_epoch must be non-negative")
        requested_paths = tuple(
            sorted({normalize_relative_path(path) for path in changed_paths})
        )
        if expected_epoch != reference.epoch:
            return RestoreResult(
                checkpoint_id=checkpoint_id,
                state=RestoreState.STALE,
                workspace_digest=None,
                requested_paths=requested_paths,
                evidence_class=reference.evidence_class,
                error_code="stale_epoch",
                detail="checkpoint epoch does not match the requested epoch",
            )
        try:
            scope = CheckpointScope(
                turn_id=reference.turn_id,
                world_id=reference.world_id,
                engram_id=reference.engram_id,
                epoch=expected_epoch,
                workspace_root=workspace_root,
                changed_paths=requested_paths or reference.changed_paths,
            )
        except (CheckpointValidationError, ChangeValidationError) as exc:
            return RestoreResult(
                checkpoint_id=checkpoint_id,
                state=RestoreState.DECLINED,
                workspace_digest=None,
                requested_paths=requested_paths,
                evidence_class=reference.evidence_class,
                error_code="workspace_invalid",
                detail=str(exc),
            )
        if scope.workspace_digest != reference.workspace_digest:
            return RestoreResult(
                checkpoint_id=checkpoint_id,
                state=RestoreState.DECLINED,
                workspace_digest=scope.workspace_digest,
                requested_paths=requested_paths,
                evidence_class=reference.evidence_class,
                error_code="workspace_mismatch",
                detail="restore workspace does not match checkpoint scope",
            )
        effective_paths = requested_paths or reference.changed_paths
        if any(path not in reference.changed_paths for path in effective_paths):
            return RestoreResult(
                checkpoint_id=checkpoint_id,
                state=RestoreState.DECLINED,
                workspace_digest=scope.workspace_digest,
                requested_paths=effective_paths,
                evidence_class=reference.evidence_class,
                error_code="changed_path_outside_scope",
                detail="restore path is outside checkpoint changed-path scope",
            )
        decision = resolve_policy_decision(
            policy_context if policy_context is not None else self._policy_evaluator,
            {
                "action": "checkpoint.restore",
                "operation": "file_change",
                "path": effective_paths[0] if effective_paths else ".",
                "checkpoint_id": checkpoint_id,
                "expected_epoch": expected_epoch,
                "scope": scope.safe_wire(),
                "changed_paths": list(effective_paths),
            },
            action="checkpoint.restore",
        )
        if not decision.allow:
            return RestoreResult(
                checkpoint_id=checkpoint_id,
                state=RestoreState.DECLINED,
                workspace_digest=scope.workspace_digest,
                requested_paths=effective_paths,
                evidence_class=decision.evidence_class,
                error_code="policy_denied",
                detail=decision.reason_code,
            )
        if reference.state in {
            CheckpointState.UNSUPPORTED,
            CheckpointState.DECLINED,
            CheckpointState.DROPPED,
        } or not getattr(self._backend, "supports_checkpoints", False):
            return RestoreResult(
                checkpoint_id=checkpoint_id,
                state=RestoreState.UNSUPPORTED,
                workspace_digest=scope.workspace_digest,
                requested_paths=effective_paths,
                evidence_class=CONTRACT_ONLY,
                error_code="unsupported_execution",
                detail="checkpoint backend cannot restore this reference",
            )
        try:
            result = self._backend.restore(
                reference,
                scope,
                changed_paths=effective_paths,
            )
            if not isinstance(result, BackendRestoreResult):
                raise CheckpointValidationError(
                    "checkpoint backend must return BackendRestoreResult"
                )
        except Exception as exc:
            result = BackendRestoreResult(
                state=RestoreState.UNCERTAIN,
                reason=f"{type(exc).__name__}: restore evidence is incomplete",
            )
        if result.state is RestoreState.RESTORED:
            self._references[checkpoint_id] = replace(
                reference,
                state=CheckpointState.RESTORED,
                updated_at=_utc_now(),
                reason=result.reason,
            )
        elif result.state in {RestoreState.FAILED, RestoreState.UNCERTAIN}:
            self._references[checkpoint_id] = replace(
                reference,
                state=(
                    CheckpointState.FAILED
                    if result.state is RestoreState.FAILED
                    else CheckpointState.UNCERTAIN
                ),
                updated_at=_utc_now(),
                reason=result.reason,
            )
        return RestoreResult(
            checkpoint_id=checkpoint_id,
            state=result.state,
            workspace_digest=scope.workspace_digest,
            requested_paths=effective_paths,
            applied_paths=result.applied_paths,
            evidence_class=str(
                getattr(self._backend, "evidence_class", reference.evidence_class)
            ),
            error_code=None
            if result.state is RestoreState.RESTORED
            else result.state.value,
            detail=result.reason,
        )

    def drop(
        self,
        checkpoint_id: str,
        *,
        workspace_root: str | os.PathLike[str],
        expected_epoch: int,
        policy_context: Any = None,
    ) -> DropResult:
        _non_empty(checkpoint_id, "checkpoint_id")
        reference = self._references.get(checkpoint_id)
        if reference is None:
            return DropResult(
                checkpoint_id=checkpoint_id,
                state=CheckpointState.FAILED,
                evidence_class=CONTRACT_ONLY,
                error_code="checkpoint_not_found",
                detail="checkpoint reference is not retained",
            )
        if type(expected_epoch) is not int or expected_epoch < 0:
            raise CheckpointValidationError("expected_epoch must be non-negative")
        if expected_epoch != reference.epoch:
            return DropResult(
                checkpoint_id=checkpoint_id,
                state=CheckpointState.STALE,
                evidence_class=reference.evidence_class,
                error_code="stale_epoch",
                detail="checkpoint epoch does not match the requested epoch",
            )
        try:
            scope = CheckpointScope(
                turn_id=reference.turn_id,
                world_id=reference.world_id,
                engram_id=reference.engram_id,
                epoch=expected_epoch,
                workspace_root=workspace_root,
                changed_paths=reference.changed_paths,
            )
        except (CheckpointValidationError, ChangeValidationError) as exc:
            return DropResult(
                checkpoint_id=checkpoint_id,
                state=CheckpointState.DECLINED,
                evidence_class=reference.evidence_class,
                error_code="workspace_invalid",
                detail=str(exc),
            )
        if scope.workspace_digest != reference.workspace_digest:
            return DropResult(
                checkpoint_id=checkpoint_id,
                state=CheckpointState.DECLINED,
                evidence_class=reference.evidence_class,
                error_code="workspace_mismatch",
                detail="drop workspace does not match checkpoint scope",
            )
        decision = resolve_policy_decision(
            policy_context if policy_context is not None else self._policy_evaluator,
            {
                "action": "checkpoint.drop",
                "operation": "file_change",
                "path": scope.changed_paths[0] if scope.changed_paths else ".",
                "checkpoint_id": checkpoint_id,
                "expected_epoch": expected_epoch,
                "scope": scope.safe_wire(),
            },
            action="checkpoint.drop",
        )
        if not decision.allow:
            return DropResult(
                checkpoint_id=checkpoint_id,
                state=CheckpointState.DECLINED,
                evidence_class=decision.evidence_class,
                error_code="policy_denied",
                detail=decision.reason_code,
            )
        if reference.state in {
            CheckpointState.UNSUPPORTED,
            CheckpointState.DECLINED,
        } or not getattr(self._backend, "supports_checkpoints", False):
            return DropResult(
                checkpoint_id=checkpoint_id,
                state=CheckpointState.UNSUPPORTED,
                evidence_class=CONTRACT_ONLY,
                error_code="unsupported_execution",
                detail="checkpoint backend cannot drop this reference",
            )
        try:
            result = self._backend.drop(reference, scope)
            if not isinstance(result, BackendDropResult):
                raise CheckpointValidationError(
                    "checkpoint backend must return BackendDropResult"
                )
        except Exception as exc:
            result = BackendDropResult(
                state=CheckpointState.UNCERTAIN,
                reason=f"{type(exc).__name__}: drop evidence is incomplete",
            )
        self._references[checkpoint_id] = replace(
            reference,
            state=result.state,
            updated_at=_utc_now(),
            reason=result.reason,
        )
        return DropResult(
            checkpoint_id=checkpoint_id,
            state=result.state,
            evidence_class=str(
                getattr(self._backend, "evidence_class", reference.evidence_class)
            ),
            error_code=None
            if result.state is CheckpointState.DROPPED
            else result.state.value,
            detail=result.reason,
        )

    def _reference(
        self,
        scope: CheckpointScope,
        *,
        checkpoint_id: str,
        state: CheckpointState,
        backend: str,
        backend_ref: str | None,
        evidence_class: str,
        reason: str | None,
    ) -> CheckpointRef:
        now = _utc_now()
        return CheckpointRef(
            id=checkpoint_id,
            turn_id=scope.turn_id,
            world_id=scope.world_id,
            engram_id=scope.engram_id,
            epoch=scope.epoch,
            workspace_digest=scope.workspace_digest,
            scope_digest=scope.scope_digest,
            changed_paths=tuple(scope.changed_paths),
            state=state,
            backend=backend,
            backend_ref=backend_ref,
            evidence_class=evidence_class,
            created_at=now,
            updated_at=now,
            reason=reason,
        )

    def _remember(self, reference: CheckpointRef) -> None:
        self._references[reference.id] = reference
        if len(self._references) <= self._max_retained:
            return
        removable = sorted(
            self._references.values(),
            key=lambda item: item.created_at,
        )
        for candidate in removable:
            if len(self._references) <= self._max_retained:
                break
            if candidate.id != reference.id:
                self._references.pop(candidate.id, None)

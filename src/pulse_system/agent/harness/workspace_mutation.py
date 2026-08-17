"""Checkpoint-first, text-only workspace mutation backend.

This module is the real file side-effect adapter for the Harness action seam.
It deliberately does not use git as a recovery mechanism.  A mutation first
stores the exact pre-image outside the workspace, persists a manifest in a
``prepared`` state, applies the bounded file operation with atomic replacement,
verifies the post-image, and only then seals the checkpoint.  A process restart
can therefore replay a safe result or expose an unfinished operation as
uncertain; it never silently applies an old request a second time.

Only UTF-8 regular text files are supported.  Results and the persistent
manifest contain relative paths, digests and bounded diff previews.  The raw
file bytes live only in the explicitly configured checkpoint root and are
never returned through :meth:`CheckpointedWorkspaceBackend.execute`.
"""

from __future__ import annotations

import difflib
import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import threading
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pulse_system.core.habitat.managed import (
    ExternalEffectPublicationError,
    ExternalEffectTransaction,
    OrdinaryExternalEffectAuthority,
    bind_external_effect_authority,
)
from pulse_system.core.runtime.publication import (
    RuntimePublicationPermit,
)

from .security import LIVE_GATE_UNVERIFIED, Redactor

__all__ = [
    "CheckpointedWorkspaceBackend",
    "WorkspaceMutationError",
    "WorkspaceValidationError",
]


_MANIFEST_VERSION = 1
_EVIDENCE_CLASS = LIVE_GATE_UNVERIFIED
_MAX_PREVIEW_BYTES = 16 * 1024
_MAX_ID_BYTES = 128
_REPARSE_POINT = 0x0400
_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$")
_MISSING = "MISSING"


class WorkspaceMutationError(RuntimeError):
    """A fail-closed error from the real workspace adapter."""


class WorkspaceValidationError(ValueError):
    """An invalid root, path, operation or text payload."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest_json(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return _digest_bytes(encoded)


def _bounded_identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WorkspaceValidationError(f"{field}_required")
    normalized = value.strip()
    if len(normalized.encode("utf-8")) > _MAX_ID_BYTES or "\x00" in normalized:
        raise WorkspaceValidationError(f"{field}_invalid")
    if _IDENTIFIER.fullmatch(normalized) is None:
        raise WorkspaceValidationError(f"{field}_invalid")
    return normalized


def _is_reparse_or_symlink(path: Path) -> bool:
    """Detect POSIX symlinks and Windows junction/reparse points."""

    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise WorkspaceMutationError("filesystem_stat_failed") from exc
    if stat.S_ISLNK(info.st_mode):
        return True
    return bool(getattr(info, "st_file_attributes", 0) & _REPARSE_POINT)


def _existing(path: Path) -> bool:
    # ``Path.exists`` is false for a broken symlink.  ``lexists`` is necessary
    # to reject that path rather than treating it as a safe new file.
    return os.path.lexists(path)


def _relative_path(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WorkspaceValidationError("path_required")
    raw = value.strip()
    if "\x00" in raw:
        raise WorkspaceValidationError("path_invalid")
    normalized = raw.replace("\\", "/")
    if (
        normalized.startswith("/")
        or normalized.startswith("//")
        or _WINDOWS_DRIVE.match(normalized)
        or Path(raw).is_absolute()
    ):
        raise WorkspaceValidationError("absolute_path_denied")
    parts = normalized.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise WorkspaceValidationError("path_traversal_denied")
    if any(":" in part for part in parts):
        raise WorkspaceValidationError("path_stream_denied")
    if any(part.casefold() == ".pulse" for part in parts):
        raise WorkspaceValidationError("protected_path_denied")
    return "/".join(parts)


def _safe_relative(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except (OSError, RuntimeError, ValueError) as exc:
        raise WorkspaceMutationError("workspace_boundary_lost") from exc


def _signal_set(signal: Any) -> bool:
    if signal is None:
        return False
    try:
        method = getattr(signal, "is_set", None)
        if callable(method):
            return bool(method())
        for name in ("cancelled", "canceled", "aborted"):
            value = getattr(signal, name, False)
            if callable(value):
                value = value()
            if bool(value):
                return True
        return False
    except Exception as exc:
        raise WorkspaceMutationError("cancellation_state_unreadable") from exc


@dataclass(frozen=True, slots=True)
class _FileState:
    exists: bool
    digest: str | None
    size: int
    data: bytes | None

    def safe(self) -> dict[str, Any]:
        return {
            "exists": self.exists,
            "digest": self.digest,
            "size": self.size,
        }


@dataclass(frozen=True, slots=True)
class _Operation:
    kind: str
    path: str
    move_path: str | None = None
    content: bytes | None = None
    expected_before_digest: str | None = None


@dataclass(frozen=True, slots=True)
class _Plan:
    operation: _Operation
    before: Mapping[str, _FileState]
    after: Mapping[str, _FileState]

    @property
    def paths(self) -> tuple[str, ...]:
        return tuple(sorted(self.before))


class CheckpointedWorkspaceBackend:
    """A production workspace text mutation backend.

    ``checkpoint_root`` must be a disjoint directory outside
    ``workspace_root``.  It stores the raw pre-images and an atomic JSON
    manifest; neither is reachable through a successful result.  The adapter
    uses ``LIVE_GATE_UNVERIFIED`` because it is a live, bounded file adapter,
    but it is not an OS sandbox.  Callers must still pass the Harness policy
    decision before invoking ``execute``.

    ``world_id`` is fixed at construction because a backend is owned by one
    Runtime/world.  It is optional for compatibility with existing backend
    factories; the action scope still records it on every checkpoint.
    """

    def __init__(
        self,
        workspace_root: str | os.PathLike[str],
        checkpoint_root: str | os.PathLike[str],
        *,
        max_file_bytes: int = 4_000_000,
        max_total_bytes: int = 16_000_000,
        max_operations: int = 256,
        max_checkpoints: int = 1024,
        max_actions: int = 4096,
        world_id: str = "workspace",
        publication_authority: (
            OrdinaryExternalEffectAuthority
            | RuntimePublicationPermit
            | None
        ) = None,
    ) -> None:
        if type(max_file_bytes) is not int or max_file_bytes <= 0:
            raise WorkspaceValidationError("max_file_bytes_invalid")
        if type(max_total_bytes) is not int or max_total_bytes <= 0:
            raise WorkspaceValidationError("max_total_bytes_invalid")
        for name, value, upper in (
            ("max_operations", max_operations, 1024),
            ("max_checkpoints", max_checkpoints, 16_384),
            ("max_actions", max_actions, 65_536),
        ):
            if type(value) is not int or not 1 <= value <= upper:
                raise WorkspaceValidationError(f"{name}_invalid")
        self._world_id = _bounded_identifier(world_id, "world_id")
        self._publication = bind_external_effect_authority(
            publication_authority,
            unbound_scope=f"unbound:workspace:{self._world_id}",
        )
        self._workspace_root = self._prepare_workspace_root(workspace_root)
        self._checkpoint_root = self._prepare_checkpoint_root(
            checkpoint_root,
            self._workspace_root,
        )
        self._max_file_bytes = max_file_bytes
        self._max_total_bytes = max_total_bytes
        self._max_operations = max_operations
        self._max_checkpoints = max_checkpoints
        self._max_actions = max_actions
        self._manifest_path = self._checkpoint_root / "manifest.json"
        self._lock = threading.RLock()
        self._manifest = self._load_manifest()
        # A constructor necessarily represents a new process lifetime. A
        # persisted prepared/applying record may already have crossed the
        # filesystem boundary before the former process died, so it cannot
        # remain a benign pre-start state after restart.
        recovered = False
        for record in self._manifest["checkpoints"].values():
            if isinstance(record, dict) and record.get("state") in {
                "prepared",
                "applying",
                "restoring",
                "dropping",
            }:
                record["state"] = "uncertain"
                record["failure_code"] = "workspace_restart_recovery_required"
                record["updated_at"] = _utc_now()
                recovered = True
        if recovered:
            with self._publication.transaction(
                "workspace:restart-recovery"
            ) as transaction:
                self._persist_manifest(transaction=transaction)
        self._redactor = Redactor(
            workspace_root=self._workspace_root,
            max_text_bytes=_MAX_PREVIEW_BYTES,
            max_payload_bytes=_MAX_PREVIEW_BYTES,
        )

    @property
    def evidence_class(self) -> str:
        return _EVIDENCE_CLASS

    @property
    def evidence_binding(self) -> Mapping[str, str]:
        with self._lock:
            return {
                "artifact_version": "pulse.workspace-mutation.v1",
                "adapter": "checkpointed_workspace",
                "filesystem_mode": "utf8_regular_files_atomic_replace",
                "publication_origin": self._publication.origin,
                "workspace_boundary_digest": _digest_bytes(
                    str(self._workspace_root).encode("utf-8")
                ),
                "checkpoint_boundary_digest": _digest_bytes(
                    str(self._checkpoint_root).encode("utf-8")
                ),
                "manifest_digest": _digest_bytes(
                    self._manifest_path.read_bytes()
                    if self._manifest_path.exists()
                    else b""
                ),
            }

    def preflight(self) -> Mapping[str, Any]:
        """Revalidate both roots and return only safe adapter evidence."""

        with self._lock:
            try:
                self._assert_roots()
                self._publication.assert_active()
                manifest_ok = self._manifest.get("version") == _MANIFEST_VERSION
                checkpoints = self._manifest.get("checkpoints", {})
                actions = self._manifest.get("actions", {})
                if not isinstance(checkpoints, dict) or not isinstance(actions, dict):
                    manifest_ok = False
                inflight = sum(
                    1
                    for record in checkpoints.values()
                    if isinstance(record, Mapping)
                    and record.get("state") in {"prepared", "applying", "uncertain"}
                )
                return {
                    "ok": manifest_ok,
                    "available": manifest_ok,
                    "state": "ready" if manifest_ok else "manifest_invalid",
                    "adapter_state": "callable_preflighted" if manifest_ok else "manifest_invalid",
                    "error_code": None if manifest_ok else "workspace_checkpoint_manifest_invalid",
                    "evidence_class": self.evidence_class,
                    "evidence_binding": dict(self.evidence_binding),
                    "manifest_version": _MANIFEST_VERSION,
                    "checkpoint_count": len(checkpoints) if manifest_ok else 0,
                    "inflight_count": inflight if manifest_ok else 0,
                    "action_count": len(actions) if manifest_ok else 0,
                    "checks": {
                        "workspace_root": True,
                        "checkpoint_root_outside_workspace": True,
                        "manifest_atomic": True,
                        "utf8_regular_file_only": True,
                    },
                }
            except (
                WorkspaceMutationError,
                WorkspaceValidationError,
                ExternalEffectPublicationError,
            ):
                return {
                    "ok": False,
                    "available": False,
                    "state": "preflight_failed",
                    "adapter_state": "preflight_failed",
                    "error_code": "workspace_checkpoint_preflight_failed",
                    "evidence_class": self.evidence_class,
                    "evidence_binding": dict(self.evidence_binding),
                    "manifest_version": _MANIFEST_VERSION,
                    "checkpoint_count": 0,
                    "inflight_count": 0,
                    "action_count": 0,
                    "checks": {
                        "workspace_root": False,
                        "checkpoint_root_outside_workspace": False,
                        "manifest_atomic": False,
                        "utf8_regular_file_only": False,
                    },
                }

    def list_checkpoints(self) -> list[Mapping[str, Any]]:
        """Return safe checkpoint summaries; dropped checkpoints are hidden."""

        with self._lock:
            records = []
            for record in self._manifest["checkpoints"].values():
                if not isinstance(record, Mapping) or record.get("state") == "dropped":
                    continue
                records.append(self._safe_checkpoint(record))
            return sorted(
                records,
                key=lambda item: str(item.get("created_at", "")),
                reverse=True,
            )

    def verify_role_receipt(
        self,
        result: Mapping[str, Any],
        *,
        world_id: str,
        engram_id: str,
        turn_id: str,
    ) -> Mapping[str, Any] | None:
        """Resolve a tool result against the canonical sealed manifest.

        This read-only owner check is the trust boundary used by RoleLease;
        a caller-provided evidence label or checkpoint identifier is never
        sufficient on its own.
        """

        if not isinstance(result, Mapping) or world_id != self._world_id:
            return None
        checkpoint_id = result.get("checkpoint_id")
        if not isinstance(checkpoint_id, str) or not checkpoint_id:
            return None
        with self._lock:
            record = self._manifest.get("checkpoints", {}).get(checkpoint_id)
            if not isinstance(record, Mapping) or record.get("state") != "sealed":
                return None
            changed_paths = result.get("changed_paths")
            if not isinstance(changed_paths, (list, tuple)):
                return None
            normalized_paths = tuple(sorted(str(item) for item in changed_paths))
            canonical_paths = tuple(
                sorted(str(item) for item in record.get("changed_paths", ()))
            )
            if (
                not normalized_paths
                or normalized_paths != canonical_paths
                or record.get("world_id") != world_id
                or record.get("engram_id") != engram_id
                or record.get("turn_id") != turn_id
                or result.get("evidence_class") != self.evidence_class
                or result.get("before_digest") != record.get("before_digest")
                or result.get("post_digest") != record.get("post_digest")
                or result.get("action_request_id")
                != record.get("action_request_id")
                or not isinstance(record.get("sealed_at"), str)
            ):
                return None
            return {
                "checkpoint_id": checkpoint_id,
                "before_digest": record.get("before_digest"),
                "post_digest": record.get("post_digest"),
                "changed_paths": list(canonical_paths),
                "action_request_id": record.get("action_request_id"),
                "evidence_class": self.evidence_class,
                "produced_at": record.get("sealed_at"),
            }

    def recovery_snapshot(self) -> Mapping[str, int]:
        """Return payload-free durable recovery counts without writing state."""

        with self._lock:
            manifest_readable = 1
            try:
                durable = self._load_manifest()
            except WorkspaceValidationError:
                durable = {"checkpoints": {}, "actions": {}}
                manifest_readable = 0
            raw_checkpoints = durable.get("checkpoints", {})
            checkpoints = (
                raw_checkpoints
                if isinstance(raw_checkpoints, Mapping)
                else {}
            )
            latest = {
                str(checkpoint_id): str(record.get("state", "unknown"))
                for checkpoint_id, record in checkpoints.items()
                if isinstance(record, Mapping)
            }
            checkpoint_scan_readable = 1
            try:
                checkpoint_dirs = {
                    path.name
                    for path in self._checkpoint_root.iterdir()
                    if path.is_dir() and re.fullmatch(r"[0-9a-f]{32}", path.name)
                }
            except OSError:
                checkpoint_dirs = set()
                checkpoint_scan_readable = 0
            orphaned = sum(checkpoint_id not in latest for checkpoint_id in checkpoint_dirs)
            missing_evidence = (
                sum(
                    checkpoint_id not in checkpoint_dirs and state != "dropped"
                    for checkpoint_id, state in latest.items()
                )
                if checkpoint_scan_readable
                else 0
            )
            prepared = sum(state == "prepared" for state in latest.values())
            uncertain = sum(state == "uncertain" for state in latest.values())
            inflight = sum(
                state in {"applying", "restoring", "dropping"}
                for state in latest.values()
            )
            partial = sum(
                state == "partially_restored" for state in latest.values()
            )
            recovered = sum(
                isinstance(record, Mapping)
                and (
                    record.get("state") == "restored"
                    or record.get("reconciled_from_uncertain") is True
                )
                for record in checkpoints.values()
            )
            committed = sum(
                state in {"sealed", "restored", "dropped"}
                for state in latest.values()
            )
            quarantined = sum(
                isinstance(record, Mapping)
                and record.get("quarantined") is True
                for record in checkpoints.values()
            )
            scan_unresolved = (
                (1 - manifest_readable) + (1 - checkpoint_scan_readable)
            )
            return {
                "attempted": len(latest) + orphaned,
                "committed": committed,
                "recovered": recovered,
                "unresolved": (
                    prepared
                    + uncertain
                    + inflight
                    + partial
                    + orphaned
                    + missing_evidence
                    + scan_unresolved
                ),
                "uncertain": uncertain,
                "prepared": prepared,
                "inflight": inflight,
                "partially_restored": partial,
                "quarantined": quarantined,
                "orphaned_checkpoint_dirs": orphaned,
                "missing_checkpoint_dirs": missing_evidence,
                "manifest_readable": manifest_readable,
                "checkpoint_scan_readable": checkpoint_scan_readable,
                "evidence_scan_unresolved": scan_unresolved,
            }

    def execute(
        self,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        tool_name: str,
        input_data: Mapping[str, Any],
        policy_preview: Mapping[str, Any],
        signal: Any = None,
    ) -> Mapping[str, Any]:
        """Execute one approved ``write`` or ``edit`` action.

        The method intentionally has the same call shape as
        ``ProcessSandboxBackend.execute``.  Unsupported tool names return a
        structured refusal without creating a checkpoint.  The request scope
        is part of the durable idempotency key.
        """

        try:
            action_request_id = _bounded_identifier(action_request_id, "action_request_id")
            engram_id = _bounded_identifier(engram_id, "engram_id")
            turn_id = _bounded_identifier(turn_id, "turn_id")
            if type(epoch) is not int or epoch < 0:
                raise WorkspaceValidationError("epoch_invalid")
            if not isinstance(tool_name, str) or not tool_name.strip():
                raise WorkspaceValidationError("tool_name_invalid")
            tool_name = tool_name.strip().casefold()
            if not isinstance(input_data, Mapping):
                raise WorkspaceValidationError("input_invalid")
            if not isinstance(policy_preview, Mapping):
                raise WorkspaceValidationError("policy_preview_invalid")
        except (WorkspaceValidationError, TypeError) as exc:
            return self._failure(
                "action_request_invalid",
                detail=str(exc),
                action_request_id=action_request_id if isinstance(action_request_id, str) else None,
            )

        if tool_name not in {"write", "edit"}:
            return self._failure(
                "unsupported_tool",
                detail="only write and edit are implemented by the workspace adapter",
                action_request_id=action_request_id,
                tool_name=tool_name,
            )
        if _signal_set(signal):
            return self._failure(
                "cancelled_before_checkpoint",
                detail="the action was cancelled before checkpoint creation",
                action_request_id=action_request_id,
                tool_name=tool_name,
            )

        try:
            request_digest = _digest_json(
                {"tool_name": tool_name, "input": dict(input_data)}
            )
        except (TypeError, ValueError):
            return self._failure(
                "input_invalid",
                detail="the action input is not persistable as a bounded request",
                action_request_id=action_request_id,
                tool_name=tool_name,
            )
        # Exact-once replay must happen before parsing an edit.  After a
        # successful edit oldText is intentionally absent from the file.
        with self._lock:
            early = self._replay_or_fence_request(
                action_request_id,
                engram_id,
                turn_id,
                epoch,
                tool_name,
                request_digest,
            )
            if early is not None:
                return early

        try:
            operation = self._operation_from_tool(tool_name, input_data)
            return self._execute_operation(
                operation,
                action_request_id=action_request_id,
                engram_id=engram_id,
                turn_id=turn_id,
                epoch=epoch,
                policy_preview=policy_preview,
                signal=signal,
                request_digest=request_digest,
            )
        except (WorkspaceValidationError, WorkspaceMutationError) as exc:
            return self._failure(
                str(exc) if str(exc).isidentifier() else "workspace_mutation_failed",
                detail="workspace mutation was rejected before a side effect",
                action_request_id=action_request_id,
                tool_name=tool_name,
            )

    def restore(
        self,
        checkpoint_id: str,
        *,
        expected_epoch: int,
        changed_paths: Sequence[str] = (),
        signal: Any = None,
    ) -> Mapping[str, Any]:
        """Restore a sealed or explicitly reconciled uncertain checkpoint.

        An uncertain checkpoint is never replayed.  On an operator restore we
        classify every selected path as exactly the recorded before-image or
        post-image; mixed before/post state is safe to roll back, while any
        third state is a conflict and is never overwritten.
        """

        try:
            checkpoint_id = _bounded_identifier(checkpoint_id, "checkpoint_id")
            if type(expected_epoch) is not int or expected_epoch < 0:
                raise WorkspaceValidationError("epoch_invalid")
            requested = tuple(sorted({_relative_path(path) for path in changed_paths}))
        except (WorkspaceValidationError, TypeError):
            return self._failure(
                "restore_request_invalid",
                detail="checkpoint or restore scope is invalid",
                checkpoint_id=checkpoint_id if isinstance(checkpoint_id, str) else None,
            )

        with self._lock:
            record = self._manifest["checkpoints"].get(checkpoint_id)
            if not isinstance(record, dict):
                return self._failure(
                    "checkpoint_not_found",
                    detail="checkpoint is not retained",
                    checkpoint_id=checkpoint_id,
                )
            if record.get("state") == "dropped":
                return self._failure(
                    "checkpoint_dropped",
                    detail="dropped checkpoint cannot be restored",
                    checkpoint_id=checkpoint_id,
                )
            if record.get("epoch") != expected_epoch:
                return self._failure(
                    "stale_epoch",
                    detail="checkpoint epoch does not match the current request",
                    checkpoint_id=checkpoint_id,
                    state="stale",
                )
            uncertain_origin = record.get("state") == "uncertain"
            if record.get("state") not in {
                "sealed",
                "partially_restored",
                "restored",
                "uncertain",
            }:
                return self._failure(
                    "checkpoint_not_sealed",
                    detail="only a sealed checkpoint can be restored",
                    checkpoint_id=checkpoint_id,
                    state="failed",
                )
            all_paths = tuple(record.get("changed_paths", ()))
            if any(path not in all_paths for path in requested):
                return self._failure(
                    "changed_path_outside_checkpoint",
                    detail="restore scope is outside the checkpoint boundary",
                    checkpoint_id=checkpoint_id,
                )
            restored_paths = set(record.get("restored_paths", ()))
            requested_or_all = requested or all_paths
            effective = tuple(path for path in requested_or_all if path not in restored_paths)
            if not effective:
                return self._restore_result(record, status="restored", idempotent=True)
            if _signal_set(signal):
                return self._failure(
                    "cancelled_before_restore",
                    detail="restore was cancelled before any file was touched",
                    checkpoint_id=checkpoint_id,
                    state="cancelled",
                )
            try:
                self._assert_roots()
                self._publication.assert_active()
                if uncertain_origin:
                    effective, already_before = self._partition_uncertain_restore(
                        record,
                        effective,
                    )
                    restored_paths.update(already_before)
                else:
                    self._verify_post_image(record, effective)
            except (WorkspaceMutationError, ExternalEffectPublicationError) as exc:
                return self._failure(
                    exc.code
                    if isinstance(exc, ExternalEffectPublicationError)
                    else str(exc),
                    detail="restore conflict or checkpoint evidence could not be verified",
                    checkpoint_id=checkpoint_id,
                    state=(
                        "conflict"
                        if str(exc) == "restore_conflict"
                        else "failed"
                        if isinstance(exc, ExternalEffectPublicationError)
                        and not exc.crossed_boundary
                        else "uncertain"
                    ),
                    changed_paths=effective,
                )
            record_before_commit = dict(record)
            try:
                self._failpoint("before_restore_commit")
                with self._publication.transaction(
                    "workspace:restore-commit"
                ) as transaction:
                    record["state"] = "restoring"
                    record["restore_attempt_paths"] = list(effective)
                    record["updated_at"] = _utc_now()
                    self._persist_manifest(transaction=transaction)
                    for index, path in enumerate(effective):
                        if _signal_set(signal):
                            raise WorkspaceMutationError(
                                "restore_cancelled_after_partial_apply"
                            )
                        self._restore_path(
                            record,
                            path,
                            transaction=transaction,
                        )
                        self._failpoint(f"restore_after_operation:{index}")
                    if effective:
                        self._verify_before_image(record, effective)
                    restored_paths.update(effective)
                    record["restored_paths"] = sorted(restored_paths)
                    record["state"] = (
                        "restored"
                        if restored_paths == set(all_paths)
                        else "partially_restored"
                    )
                    record.pop("restore_attempt_paths", None)
                    if uncertain_origin:
                        record["reconciled_from_uncertain"] = True
                    record["updated_at"] = _utc_now()
                    self._persist_manifest(transaction=transaction)
            except Exception as exc:
                crossed_boundary = (
                    isinstance(exc, ExternalEffectPublicationError)
                    and exc.crossed_boundary
                )
                failure_code = (
                    exc.code
                    if isinstance(exc, ExternalEffectPublicationError)
                    else "restore_uncertain"
                    if isinstance(exc, WorkspaceMutationError)
                    else "restore_failed"
                )
                if crossed_boundary:
                    record["state"] = "uncertain"
                    record["failure_code"] = failure_code
                    record["quarantined"] = True
                    record["updated_at"] = _utc_now()
                else:
                    record.clear()
                    record.update(record_before_commit)
                return self._failure(
                    failure_code,
                    detail="restore did not reach a verified before-image",
                    checkpoint_id=checkpoint_id,
                    state="uncertain" if crossed_boundary else "failed",
                    changed_paths=effective,
                )
            return self._restore_result(record, status=record["state"], idempotent=False)

    def drop(
        self,
        checkpoint_id: str,
        *,
        expected_epoch: int | None = None,
    ) -> Mapping[str, Any]:
        """Drop checkpoint bytes only; never changes workspace files.

        Prepared, applying and uncertain checkpoints cannot be dropped.  They
        retain the evidence needed for operator recovery.  A sealed or
        restored checkpoint may be dropped, and the durable manifest records
        the terminal ``dropped`` boundary before it disappears from listings.
        """

        try:
            checkpoint_id = _bounded_identifier(checkpoint_id, "checkpoint_id")
            if expected_epoch is not None and (
                type(expected_epoch) is not int or expected_epoch < 0
            ):
                raise WorkspaceValidationError("epoch_invalid")
        except WorkspaceValidationError:
            return self._failure(
                "drop_request_invalid",
                detail="checkpoint or epoch is invalid",
                checkpoint_id=checkpoint_id if isinstance(checkpoint_id, str) else None,
            )
        with self._lock:
            record = self._manifest["checkpoints"].get(checkpoint_id)
            if not isinstance(record, dict):
                return self._failure(
                    "checkpoint_not_found",
                    detail="checkpoint is not retained",
                    checkpoint_id=checkpoint_id,
                )
            if expected_epoch is not None and record.get("epoch") != expected_epoch:
                return self._failure(
                    "stale_epoch",
                    detail="checkpoint epoch does not match the drop request",
                    checkpoint_id=checkpoint_id,
                    state="stale",
                )
            if record.get("state") not in {"sealed", "partially_restored", "restored"}:
                return self._failure(
                    "checkpoint_not_drop_safe",
                    detail="unfinished or uncertain checkpoints must be retained",
                    checkpoint_id=checkpoint_id,
                    state="uncertain",
                )
            record_before_commit = dict(record)
            try:
                self._failpoint("before_checkpoint_drop_commit")
                with self._publication.transaction(
                    "workspace:checkpoint-drop"
                ) as transaction:
                    record["state"] = "dropping"
                    record["updated_at"] = _utc_now()
                    self._persist_manifest(transaction=transaction)
                    checkpoint_dir = self._checkpoint_dir(checkpoint_id)
                    if _existing(checkpoint_dir):
                        self._remove_checkpoint_tree(
                            checkpoint_dir,
                            transaction=transaction,
                        )
                    record["state"] = "dropped"
                    record["updated_at"] = _utc_now()
                    self._persist_manifest(transaction=transaction)
            except Exception as exc:
                crossed_boundary = (
                    isinstance(exc, ExternalEffectPublicationError)
                    and exc.crossed_boundary
                )
                if crossed_boundary:
                    record["state"] = "uncertain"
                    record["failure_code"] = "checkpoint_drop_uncertain"
                else:
                    record.clear()
                    record.update(record_before_commit)
                return self._failure(
                    "checkpoint_drop_uncertain",
                    detail="checkpoint cleanup did not reach a durable boundary",
                    checkpoint_id=checkpoint_id,
                    state="uncertain" if crossed_boundary else "failed",
                )
            return {
                "ok": True,
                "status": "dropped",
                "checkpoint_id": checkpoint_id,
                "changed_paths": list(record.get("changed_paths", ())),
                "evidence_class": self.evidence_class,
            }

    def write(
        self,
        path: str,
        content: str,
        *,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        policy_preview: Mapping[str, Any] | None = None,
        signal: Any = None,
    ) -> Mapping[str, Any]:
        """Explicit high-level write seam used by integration callers."""

        return self.execute(
            action_request_id,
            engram_id,
            turn_id,
            epoch,
            "write",
            {"path": path, "content": content},
            policy_preview or {},
            signal,
        )

    def edit(
        self,
        path: str,
        edits: Sequence[Mapping[str, Any]],
        *,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        policy_preview: Mapping[str, Any] | None = None,
        signal: Any = None,
    ) -> Mapping[str, Any]:
        """Explicit high-level exact-once edit seam."""

        return self.execute(
            action_request_id,
            engram_id,
            turn_id,
            epoch,
            "edit",
            {"path": path, "edits": list(edits)},
            policy_preview or {},
            signal,
        )

    def delete(
        self,
        path: str,
        *,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        policy_preview: Mapping[str, Any] | None = None,
        signal: Any = None,
    ) -> Mapping[str, Any]:
        """Apply a real delete through the same checkpoint-first engine."""

        return self._scoped_operation(
            _Operation("delete", _relative_path(path)),
            action_request_id=action_request_id,
            engram_id=engram_id,
            turn_id=turn_id,
            epoch=epoch,
            policy_preview=policy_preview or {},
            signal=signal,
        )

    def move(
        self,
        source: str,
        destination: str,
        *,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        policy_preview: Mapping[str, Any] | None = None,
        signal: Any = None,
    ) -> Mapping[str, Any]:
        """Apply an atomic same-filesystem move with a sealed pre-image."""

        return self._scoped_operation(
            _Operation("move", _relative_path(source), _relative_path(destination)),
            action_request_id=action_request_id,
            engram_id=engram_id,
            turn_id=turn_id,
            epoch=epoch,
            policy_preview=policy_preview or {},
            signal=signal,
        )

    def apply_operations(
        self,
        operations: Sequence[Mapping[str, Any]],
        *,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        policy_preview: Mapping[str, Any] | None = None,
        signal: Any = None,
    ) -> Mapping[str, Any]:
        """Apply bounded add/update/delete/move operations as one checkpoint."""

        if not isinstance(operations, Sequence) or isinstance(operations, (str, bytes)):
            return self._failure("operations_invalid", detail="operations must be a sequence")
        try:
            parsed = tuple(self._operation_from_mapping(item) for item in operations)
            if not parsed:
                raise WorkspaceValidationError("operations_empty")
            return self._scoped_operations(
                parsed,
                action_request_id=action_request_id,
                engram_id=engram_id,
                turn_id=turn_id,
                epoch=epoch,
                policy_preview=policy_preview or {},
                signal=signal,
            )
        except (WorkspaceValidationError, TypeError) as exc:
            return self._failure("operations_invalid", detail=str(exc))

    # ---- operation planning and execution ---------------------------------

    def _operation_from_tool(self, tool_name: str, input_data: Mapping[str, Any]) -> _Operation:
        path = _relative_path(input_data.get("path"))
        if tool_name == "write":
            content = input_data.get("content")
            if not isinstance(content, str):
                raise WorkspaceValidationError("content_invalid")
            return _Operation("write", path, content=self._encode_text(content))
        raw_edits = input_data.get("edits")
        if not isinstance(raw_edits, Sequence) or isinstance(raw_edits, (str, bytes)):
            raise WorkspaceValidationError("edits_invalid")
        content, expected_digest = self._apply_exact_edits(path, raw_edits)
        return _Operation(
            "edit",
            path,
            content=content,
            expected_before_digest=expected_digest,
        )

    def _operation_from_mapping(self, value: Mapping[str, Any]) -> _Operation:
        if not isinstance(value, Mapping):
            raise WorkspaceValidationError("operation_invalid")
        kind = value.get("kind", value.get("operation"))
        if not isinstance(kind, str):
            raise WorkspaceValidationError("operation_kind_invalid")
        kind = kind.strip().casefold()
        path = _relative_path(value.get("path", value.get("source")))
        move_path = value.get("move_path", value.get("destination"))
        if kind in {"write", "add", "update"}:
            content = value.get("content")
            if not isinstance(content, str):
                raise WorkspaceValidationError("content_invalid")
            return _Operation(kind, path, content=self._encode_text(content))
        if kind == "edit":
            edits = value.get("edits")
            if not isinstance(edits, Sequence) or isinstance(edits, (str, bytes)):
                raise WorkspaceValidationError("edits_invalid")
            content, expected_digest = self._apply_exact_edits(path, edits)
            return _Operation(
                kind,
                path,
                content=content,
                expected_before_digest=expected_digest,
            )
        if kind == "delete":
            return _Operation(kind, path)
        if kind == "move":
            return _Operation(kind, path, _relative_path(move_path))
        raise WorkspaceValidationError("unsupported_operation")

    def _apply_exact_edits(self, path: str, edits: Sequence[Any]) -> tuple[bytes, str]:
        state = self._read_state(path)
        if not state.exists or state.data is None:
            raise WorkspaceValidationError("edit_target_missing")
        text = self._decode_text(state.data)
        if not edits:
            raise WorkspaceValidationError("edits_empty")
        for edit in edits:
            if not isinstance(edit, Mapping):
                raise WorkspaceValidationError("edit_invalid")
            old = edit.get("oldText", edit.get("old_text"))
            new = edit.get("newText", edit.get("new_text"))
            if not isinstance(old, str) or not isinstance(new, str) or not old:
                raise WorkspaceValidationError("edit_text_invalid")
            occurrences = text.count(old)
            if occurrences != 1:
                raise WorkspaceValidationError("edit_ambiguous")
            text = text.replace(old, new, 1)
            self._encode_text(text)
        return self._encode_text(text), state.digest or _MISSING

    def _scoped_operation(
        self,
        operation: _Operation,
        *,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        policy_preview: Mapping[str, Any],
        signal: Any,
        request_digest: str | None = None,
    ) -> Mapping[str, Any]:
        return self._scoped_operations(
            (operation,),
            action_request_id=action_request_id,
            engram_id=engram_id,
            turn_id=turn_id,
            epoch=epoch,
            policy_preview=policy_preview,
            signal=signal,
            request_digest=request_digest,
        )

    def _scoped_operations(
        self,
        operations: Sequence[_Operation],
        *,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        policy_preview: Mapping[str, Any],
        signal: Any,
        request_digest: str | None = None,
    ) -> Mapping[str, Any]:
        try:
            action_request_id = _bounded_identifier(action_request_id, "action_request_id")
            engram_id = _bounded_identifier(engram_id, "engram_id")
            turn_id = _bounded_identifier(turn_id, "turn_id")
            if type(epoch) is not int or epoch < 0:
                raise WorkspaceValidationError("epoch_invalid")
            if not isinstance(policy_preview, Mapping):
                raise WorkspaceValidationError("policy_preview_invalid")
        except (WorkspaceValidationError, TypeError) as exc:
            return self._failure("action_request_invalid", detail=str(exc))
        if _signal_set(signal):
            return self._failure(
                "cancelled_before_checkpoint",
                detail="the action was cancelled before checkpoint creation",
                action_request_id=action_request_id,
            )
        with self._lock:
            return self._execute_operation_set(
                tuple(operations),
                action_request_id=action_request_id,
                engram_id=engram_id,
                turn_id=turn_id,
                epoch=epoch,
                policy_preview=policy_preview,
                signal=signal,
                request_digest=request_digest,
            )

    def _execute_operation(self, operation: _Operation, **kwargs: Any) -> Mapping[str, Any]:
        return self._scoped_operation(operation, **kwargs)

    def _execute_operation_set(
        self,
        operations: tuple[_Operation, ...],
        *,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        policy_preview: Mapping[str, Any],
        signal: Any,
        request_digest: str | None = None,
    ) -> Mapping[str, Any]:
        self._assert_roots()
        try:
            self._publication.assert_active()
        except ExternalEffectPublicationError as exc:
            return self._failure(
                exc.code,
                detail="workspace publication authority is no longer active",
                action_request_id=action_request_id,
            )
        if not operations or len(operations) > self._max_operations:
            return self._failure(
                "operation_limit_exceeded",
                detail="workspace mutation exceeds the bounded operation count",
                action_request_id=action_request_id,
            )
        if request_digest is None:
            request_digest = _digest_json(
                {
                    "tool_operations": [self._operation_fingerprint(op) for op in operations],
                }
            )
        action_key = self._action_key(action_request_id, engram_id, turn_id, epoch, operations)
        prior = self._manifest["actions"].get(action_key)
        if isinstance(prior, Mapping):
            if prior.get("request_digest") != request_digest:
                return self._failure(
                    "action_request_conflict",
                    detail="action_request_id was reused with different input",
                    action_request_id=action_request_id,
                )
            result = prior.get("result")
            if isinstance(result, Mapping):
                return {**dict(result), "idempotent": True}
        if len(self._manifest["actions"]) >= self._max_actions:
            return self._failure(
                "action_registry_exhausted",
                detail="workspace action registry is full; no side effect was started",
                action_request_id=action_request_id,
            )
        retained_checkpoints = sum(
            isinstance(value, Mapping) and value.get("state") != "dropped"
            for value in self._manifest["checkpoints"].values()
        )
        if retained_checkpoints >= self._max_checkpoints:
            return self._failure(
                "checkpoint_registry_exhausted",
                detail="checkpoint retention is full; drop a sealed checkpoint before retrying",
                action_request_id=action_request_id,
            )
        prior_checkpoint = self._find_action_checkpoint(
            action_request_id,
            engram_id,
            turn_id,
            epoch,
            request_digest,
        )
        if prior_checkpoint is not None:
            state = prior_checkpoint.get("state")
            if state == "conflict":
                return self._failure(
                    "action_request_conflict",
                    detail="action_request_id was reused with different input",
                    action_request_id=action_request_id,
                    checkpoint_id=prior_checkpoint.get("checkpoint_id"),
                )
            if state in {"sealed", "partially_restored", "restored"}:
                return self._result_from_record(
                    prior_checkpoint,
                    idempotent=True,
                )
            return self._failure(
                "action_recovery_required",
                detail="an unfinished checkpoint exists and will not be applied again automatically",
                action_request_id=action_request_id,
                checkpoint_id=prior_checkpoint.get("checkpoint_id"),
                state="uncertain",
                changed_paths=prior_checkpoint.get("changed_paths", ()),
            )

        try:
            plans = self._plan(operations)
            if _signal_set(signal):
                return self._failure(
                    "cancelled_before_checkpoint",
                    detail="the action was cancelled before checkpoint creation",
                    action_request_id=action_request_id,
                )
            with self._publication.transaction(
                "workspace:checkpoint-prepare"
            ) as transaction:
                checkpoint = self._prepare_checkpoint(
                    plans,
                    action_request_id=action_request_id,
                    engram_id=engram_id,
                    turn_id=turn_id,
                    epoch=epoch,
                    request_digest=request_digest,
                    policy_digest=_digest_json(dict(policy_preview)),
                    transaction=transaction,
                )
        except (
            WorkspaceValidationError,
            WorkspaceMutationError,
            ExternalEffectPublicationError,
        ) as exc:
            error = (
                exc.code
                if isinstance(exc, ExternalEffectPublicationError)
                else str(exc)
                if str(exc).isidentifier()
                else "workspace_mutation_failed"
            )
            return self._failure(
                error,
                detail="checkpoint preparation did not reach a verified boundary",
                action_request_id=action_request_id,
                state=(
                    "uncertain"
                    if isinstance(exc, ExternalEffectPublicationError)
                    and exc.crossed_boundary
                    else "failed"
                ),
            )
        checkpoint_id = str(checkpoint["checkpoint_id"])
        try:
            self._failpoint("after_checkpoint_prepared")
            for plan in plans:
                self._failpoint(
                    f"before_publication_commit:{plan.operation.path}"
                )
            with self._publication.transaction(
                "workspace:mutation-commit"
            ) as transaction:
                precondition_failure: str | None = None
                for plan in plans:
                    try:
                        self._verify_before_image(checkpoint, plan.paths)
                    except WorkspaceMutationError as exc:
                        precondition_failure = str(exc)
                        checkpoint["state"] = "failed"
                        checkpoint["failure_code"] = precondition_failure
                        checkpoint["updated_at"] = _utc_now()
                        self._persist_manifest(transaction=transaction)
                        break
                if precondition_failure is not None:
                    result = self._failure(
                        precondition_failure,
                        detail="workspace changed after checkpoint preparation",
                        action_request_id=action_request_id,
                        checkpoint_id=checkpoint_id,
                        state="failed",
                        changed_paths=checkpoint.get("changed_paths", ()),
                    )
                    return result
                applied_paths: list[str] = []
                for index, plan in enumerate(plans):
                    if _signal_set(signal):
                        raise WorkspaceMutationError(
                            "mutation_cancelled_after_partial_apply"
                        )
                    self._failpoint(f"before_operation:{index}")
                    self._apply_plan(plan, transaction=transaction)
                    applied_paths.extend(plan.paths)
                    checkpoint["state"] = "applying"
                    checkpoint["applied_paths"] = sorted(set(applied_paths))
                    checkpoint["updated_at"] = _utc_now()
                    self._persist_manifest(transaction=transaction)
                    self._failpoint(f"after_operation:{index}")
                if _signal_set(signal):
                    raise WorkspaceMutationError(
                        "mutation_cancelled_after_partial_apply"
                    )
                self._failpoint("before_seal")
                self._verify_post_image(checkpoint, checkpoint["changed_paths"])
                checkpoint["state"] = "sealed"
                checkpoint["sealed_at"] = _utc_now()
                checkpoint["updated_at"] = _utc_now()
                self._persist_manifest(transaction=transaction)
                result = self._result_from_record(checkpoint, idempotent=False)
                self._remember_action(
                    action_key,
                    request_digest,
                    result,
                    transaction=transaction,
                )
            return result
        except Exception as exc:
            # The durable prepared/applying record is the recovery evidence.
            # Never attempt an unlicensed terminal write after revoke.
            checkpoint["state"] = "uncertain"
            checkpoint["failure_code"] = (
                exc.code
                if isinstance(exc, ExternalEffectPublicationError)
                else str(exc)
                if isinstance(exc, WorkspaceMutationError)
                and str(exc).isidentifier()
                else "mutation_uncertain"
            )
            checkpoint["quarantined"] = True
            checkpoint["updated_at"] = _utc_now()
            return self._failure(
                checkpoint["failure_code"],
                detail="workspace mutation did not reach a verified sealed checkpoint",
                action_request_id=action_request_id,
                checkpoint_id=checkpoint_id,
                state="uncertain",
                changed_paths=checkpoint.get("changed_paths", ()),
            )

    def _plan(self, operations: Sequence[_Operation]) -> tuple[_Plan, ...]:
        plans: list[_Plan] = []
        seen: set[str] = set()
        before_total = 0
        after_total = 0
        for raw_operation in operations:
            if not isinstance(raw_operation, _Operation):
                raise WorkspaceValidationError("operation_invalid")
            kind = raw_operation.kind.strip().casefold()
            path = _relative_path(raw_operation.path)
            move_path = (
                _relative_path(raw_operation.move_path)
                if raw_operation.move_path is not None
                else None
            )
            if kind not in {"write", "add", "update", "edit", "delete", "move"}:
                raise WorkspaceValidationError("unsupported_operation")
            if kind == "move" and move_path is None:
                raise WorkspaceValidationError("move_destination_required")
            affected = (path, move_path) if move_path is not None else (path,)
            if len(set(affected)) != len(affected) or any(item in seen for item in affected):
                raise WorkspaceValidationError("operation_path_collision")
            seen.update(affected)
            before: dict[str, _FileState] = {item: self._read_state(item) for item in affected}
            if kind == "move":
                assert move_path is not None
                if not before[path].exists:
                    raise WorkspaceValidationError("move_source_missing")
                if before[move_path].exists:
                    raise WorkspaceValidationError("move_destination_exists")
                if before[path].data is None:
                    raise WorkspaceValidationError("move_source_invalid")
                after = {
                    path: _FileState(False, None, 0, None),
                    move_path: _FileState(
                        True,
                        before[path].digest,
                        before[path].size,
                        before[path].data,
                    ),
                }
            elif kind == "delete":
                if not before[path].exists:
                    raise WorkspaceValidationError("delete_target_missing")
                after = {path: _FileState(False, None, 0, None)}
            else:
                if raw_operation.content is None:
                    raise WorkspaceValidationError("content_invalid")
                if not isinstance(raw_operation.content, bytes):
                    raise WorkspaceValidationError("content_invalid")
                content = self._validate_bytes(raw_operation.content)
                if kind == "add" and before[path].exists:
                    raise WorkspaceValidationError("add_target_exists")
                if kind in {"update", "edit"} and not before[path].exists:
                    raise WorkspaceValidationError("update_target_missing")
                if (
                    raw_operation.expected_before_digest is not None
                    and before[path].digest != raw_operation.expected_before_digest
                ):
                    raise WorkspaceMutationError("edit_conflict")
                if before[path].exists and before[path].digest == _digest_bytes(content):
                    raise WorkspaceValidationError("mutation_no_change")
                effective_kind = "add" if not before[path].exists else (
                    "update" if kind in {"write", "add", "update"} else kind
                )
                operation = _Operation(
                    effective_kind,
                    path,
                    content=content,
                    expected_before_digest=raw_operation.expected_before_digest,
                )
                after = {path: _FileState(True, _digest_bytes(content), len(content), content)}
                raw_operation = operation
            before_total += sum(state.size for state in before.values())
            after_total += sum(state.size for state in after.values())
            plans.append(_Plan(raw_operation, before, after))
        if before_total > self._max_total_bytes or after_total > self._max_total_bytes:
            raise WorkspaceValidationError("total_size_limit_exceeded")
        return tuple(plans)

    def _prepare_checkpoint(
        self,
        plans: Sequence[_Plan],
        *,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        request_digest: str,
        policy_digest: str,
        transaction: ExternalEffectTransaction,
    ) -> dict[str, Any]:
        self._publication.assert_transaction(transaction)
        checkpoint_id = uuid.uuid4().hex
        checkpoint_dir = self._checkpoint_dir(checkpoint_id)
        self._mkdir_safe(checkpoint_dir, transaction=transaction)
        self._mkdir_safe(checkpoint_dir / "before", transaction=transaction)
        operations: list[dict[str, Any]] = []
        snapshot_index = 0
        try:
            for plan in plans:
                before_entries: dict[str, Any] = {}
                for path in plan.paths:
                    state = plan.before[path]
                    snapshot_name = None
                    if state.exists:
                        snapshot_name = f"before/{snapshot_index}.bin"
                        snapshot_index += 1
                        self._atomic_write_bytes(
                            checkpoint_dir / snapshot_name,
                            state.data or b"",
                            transaction=transaction,
                        )
                    before_entries[path] = {**state.safe(), "snapshot": snapshot_name}
                after_entries = {
                    path: plan.after[path].safe()
                    for path in plan.paths
                }
                operations.append(
                    {
                        "kind": plan.operation.kind,
                        "path": plan.operation.path,
                        "move_path": plan.operation.move_path,
                        "before": before_entries,
                        "after": after_entries,
                    }
                )
            diff = self._build_diff(plans)
            diff_path = checkpoint_dir / "diff.patch"
            self._atomic_write_bytes(
                diff_path,
                diff.encode("utf-8"),
                transaction=transaction,
            )
            changed_paths = sorted({path for plan in plans for path in plan.paths})
            before_digest = self._aggregate_digest(
                {path: state for plan in plans for path, state in plan.before.items()}
            )
            post_digest = self._aggregate_digest(
                {path: state for plan in plans for path, state in plan.after.items()}
            )
            preview = self._safe_diff_preview(diff)
            now = _utc_now()
            record: dict[str, Any] = {
                "checkpoint_id": checkpoint_id,
                "version": _MANIFEST_VERSION,
                "state": "prepared",
                "world_id": self._world_id,
                "engram_id": engram_id,
                "turn_id": turn_id,
                "epoch": epoch,
                "action_request_id": action_request_id,
                "request_digest": request_digest,
                "policy_digest": policy_digest,
                "operations": operations,
                "changed_paths": changed_paths,
                "applied_paths": [],
                "restored_paths": [],
                "before_digest": before_digest,
                "post_digest": post_digest,
                "diff_digest": _digest_bytes(diff.encode("utf-8")),
                "diff_preview": preview,
                "created_at": now,
                "updated_at": now,
                "evidence_class": self.evidence_class,
            }
            self._manifest["checkpoints"][checkpoint_id] = record
            self._persist_manifest(transaction=transaction)
            return record
        except Exception:
            # Never perform unlicensed cleanup after a failed guard. Any
            # checkpoint bytes already written remain private recovery
            # evidence and are surfaced by ``recovery_snapshot``.
            raise

    def _apply_plan(
        self,
        plan: _Plan,
        *,
        transaction: ExternalEffectTransaction,
    ) -> None:
        self._publication.assert_transaction(transaction)
        operation = plan.operation
        if operation.kind in {"add", "update", "edit", "write"}:
            state = plan.after[operation.path]
            assert state.data is not None
            self._atomic_write_bytes(
                self._workspace_path(operation.path),
                state.data,
                transaction=transaction,
            )
            return
        if operation.kind == "delete":
            self._delete_file(operation.path, transaction=transaction)
            return
        if operation.kind == "move":
            assert operation.move_path is not None
            self._move_file(
                operation.path,
                operation.move_path,
                transaction=transaction,
            )
            return
        raise WorkspaceMutationError("unsupported_operation")

    def _restore_path(
        self,
        record: Mapping[str, Any],
        path: str,
        *,
        transaction: ExternalEffectTransaction,
    ) -> None:
        self._publication.assert_transaction(transaction)
        for operation in record.get("operations", ()):
            if not isinstance(operation, Mapping):
                continue
            before = operation.get("before", {})
            if path not in before or not isinstance(before[path], Mapping):
                continue
            state = before[path]
            target = self._workspace_path(path)
            if state.get("exists"):
                snapshot = state.get("snapshot")
                if (
                    not isinstance(snapshot, str)
                    or re.fullmatch(r"before/[0-9]+\.bin", snapshot) is None
                ):
                    raise WorkspaceMutationError("snapshot_missing")
                snapshot_path = self._checkpoint_dir(str(record["checkpoint_id"])) / snapshot
                if _is_reparse_or_symlink(snapshot_path) or not snapshot_path.is_file():
                    raise WorkspaceMutationError("snapshot_missing")
                data = snapshot_path.read_bytes()
                if _digest_bytes(data) != state.get("digest"):
                    raise WorkspaceMutationError("snapshot_digest_mismatch")
                self._atomic_write_bytes(
                    target,
                    data,
                    transaction=transaction,
                )
            else:
                if _existing(target):
                    self._delete_file(path, transaction=transaction)
            return
        raise WorkspaceMutationError("restore_path_outside_checkpoint")

    def _verify_post_image(self, record: Mapping[str, Any], paths: Sequence[str]) -> None:
        expected: dict[str, _FileState] = {}
        for operation in record.get("operations", ()):
            if not isinstance(operation, Mapping):
                raise WorkspaceMutationError("manifest_invalid")
            after = operation.get("after", {})
            for path, raw in after.items():
                if path in paths:
                    expected[path] = self._state_from_safe(raw)
        if set(expected) != set(paths):
            raise WorkspaceMutationError("manifest_scope_invalid")
        actual = {path: self._read_state(path) for path in paths}
        if self._aggregate_digest(actual) != self._aggregate_digest(expected):
            raise WorkspaceMutationError("restore_conflict" if record.get("state") in {"sealed", "partially_restored", "restored"} else "post_digest_mismatch")
        if len(paths) == len(record.get("changed_paths", ())):
            if self._aggregate_digest(actual) != record.get("post_digest"):
                raise WorkspaceMutationError("post_digest_mismatch")

    def _partition_uncertain_restore(
        self,
        record: Mapping[str, Any],
        paths: Sequence[str],
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        before: dict[str, _FileState] = {}
        after: dict[str, _FileState] = {}
        for operation in record.get("operations", ()):
            if not isinstance(operation, Mapping):
                raise WorkspaceMutationError("manifest_invalid")
            raw_before = operation.get("before", {})
            raw_after = operation.get("after", {})
            if not isinstance(raw_before, Mapping) or not isinstance(raw_after, Mapping):
                raise WorkspaceMutationError("manifest_invalid")
            for path in paths:
                if path in raw_before:
                    before[path] = self._state_from_safe(raw_before[path])
                if path in raw_after:
                    after[path] = self._state_from_safe(raw_after[path])
        if set(before) != set(paths) or set(after) != set(paths):
            raise WorkspaceMutationError("manifest_scope_invalid")
        after_paths: list[str] = []
        before_paths: list[str] = []
        for path in paths:
            actual = self._read_state(path)
            if self._state_matches(actual, before[path]):
                before_paths.append(path)
            elif self._state_matches(actual, after[path]):
                after_paths.append(path)
            else:
                raise WorkspaceMutationError("restore_conflict")
        return tuple(after_paths), tuple(before_paths)

    @staticmethod
    def _state_matches(actual: _FileState, expected: _FileState) -> bool:
        return (
            actual.exists == expected.exists
            and actual.digest == expected.digest
            and actual.size == expected.size
        )

    def _verify_before_image(self, record: Mapping[str, Any], paths: Sequence[str]) -> None:
        expected: dict[str, _FileState] = {}
        for operation in record.get("operations", ()):
            before = operation.get("before", {})
            if not isinstance(before, Mapping):
                raise WorkspaceMutationError("manifest_invalid")
            for path, raw in before.items():
                if path in paths:
                    expected[path] = self._state_from_safe(raw)
        actual = {path: self._read_state(path) for path in paths}
        if self._aggregate_digest(actual) != self._aggregate_digest(expected):
            raise WorkspaceMutationError("restore_before_digest_mismatch")

    def _state_from_safe(self, value: Any) -> _FileState:
        if not isinstance(value, Mapping):
            raise WorkspaceMutationError("manifest_invalid")
        exists = value.get("exists")
        digest = value.get("digest")
        size = value.get("size")
        if type(exists) is not bool or (digest is not None and not isinstance(digest, str)):
            raise WorkspaceMutationError("manifest_invalid")
        if type(size) is not int or size < 0 or size > self._max_file_bytes:
            raise WorkspaceMutationError("manifest_invalid")
        if not exists:
            return _FileState(False, None, 0, None)
        return _FileState(True, digest, size, None)

    def _read_state(self, path: str) -> _FileState:
        target = self._workspace_path(path)
        if not _existing(target):
            return _FileState(False, None, 0, None)
        if _is_reparse_or_symlink(target):
            raise WorkspaceValidationError("reparse_path_denied")
        try:
            info = os.lstat(target)
        except OSError as exc:
            raise WorkspaceMutationError("filesystem_stat_failed") from exc
        if not stat.S_ISREG(info.st_mode):
            raise WorkspaceValidationError("regular_file_required")
        if info.st_size > self._max_file_bytes:
            raise WorkspaceValidationError("file_size_limit_exceeded")
        try:
            data = target.read_bytes()
        except OSError as exc:
            raise WorkspaceMutationError("file_read_failed") from exc
        if len(data) > self._max_file_bytes:
            raise WorkspaceValidationError("file_size_limit_exceeded")
        self._decode_text(data)
        return _FileState(True, _digest_bytes(data), len(data), data)

    def _workspace_path(self, relative: str) -> Path:
        normalized = _relative_path(relative)
        self._assert_roots()
        candidate = self._workspace_root.joinpath(*normalized.split("/"))
        current = self._workspace_root
        for part in normalized.split("/")[:-1]:
            current = current / part
            if _existing(current) and _is_reparse_or_symlink(current):
                raise WorkspaceValidationError("reparse_path_denied")
            if _existing(current) and not current.is_dir():
                raise WorkspaceValidationError("parent_directory_required")
        if _existing(candidate) and _is_reparse_or_symlink(candidate):
            raise WorkspaceValidationError("reparse_path_denied")
        return candidate

    def _delete_file(
        self,
        relative: str,
        *,
        transaction: ExternalEffectTransaction,
    ) -> None:
        self._publication.assert_transaction(transaction)
        target = self._workspace_path(relative)
        if not _existing(target):
            raise WorkspaceMutationError("target_disappeared")
        if _is_reparse_or_symlink(target) or not target.is_file():
            raise WorkspaceMutationError("regular_file_required")
        self._failpoint(f"inside_publication_commit:{relative}")
        transaction.mark_mutation()
        target.unlink()

    def _move_file(
        self,
        source: str,
        destination: str,
        *,
        transaction: ExternalEffectTransaction,
    ) -> None:
        self._publication.assert_transaction(transaction)
        source_path = self._workspace_path(source)
        destination_path = self._workspace_path(destination)
        if not _existing(source_path) or _is_reparse_or_symlink(source_path):
            raise WorkspaceMutationError("move_source_unavailable")
        if _existing(destination_path) or _is_reparse_or_symlink(destination_path):
            raise WorkspaceMutationError("move_destination_exists")
        self._failpoint(f"inside_publication_commit:{source}")
        transaction.mark_mutation()
        os.replace(source_path, destination_path)

    def _atomic_write_bytes(
        self,
        target: Path,
        data: bytes,
        *,
        transaction: ExternalEffectTransaction,
    ) -> None:
        self._publication.assert_transaction(transaction)
        if len(data) > self._max_file_bytes:
            raise WorkspaceValidationError("file_size_limit_exceeded")
        self._decode_text(data)
        if _existing(target) and _is_reparse_or_symlink(target):
            raise WorkspaceValidationError("reparse_path_denied")
        parent = target.parent
        if not parent.exists() or not parent.is_dir() or _is_reparse_or_symlink(parent):
            raise WorkspaceValidationError("parent_directory_invalid")
        transaction.mark_mutation()
        fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=str(parent))
        temporary_path = Path(temporary)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            if _existing(target) and _is_reparse_or_symlink(target):
                raise WorkspaceValidationError("reparse_path_denied")
            self._failpoint(f"inside_publication_commit:{target.name}")
            os.replace(temporary_path, target)
            try:
                directory_fd = os.open(parent, os.O_RDONLY)
            except OSError:
                directory_fd = None
            if directory_fd is not None:
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        except Exception:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    def _prepare_workspace_root(self, value: str | os.PathLike[str]) -> Path:
        try:
            raw = Path(value).expanduser()
            if not raw.exists() or not raw.is_dir():
                raise WorkspaceValidationError("workspace_root_invalid")
            if _is_reparse_or_symlink(raw):
                raise WorkspaceValidationError("workspace_root_reparse_denied")
            self._assert_no_reparse_chain(raw)
            resolved = raw.resolve(strict=True)
            if _is_reparse_or_symlink(resolved):
                raise WorkspaceValidationError("workspace_root_reparse_denied")
            return resolved
        except (TypeError, OSError, RuntimeError) as exc:
            raise WorkspaceValidationError("workspace_root_invalid") from exc

    def _prepare_checkpoint_root(self, value: str | os.PathLike[str], workspace: Path) -> Path:
        try:
            raw = Path(value).expanduser()
            lexical = Path(os.path.abspath(raw))
            if lexical == workspace or lexical.is_relative_to(workspace) or workspace.is_relative_to(lexical):
                raise WorkspaceValidationError("checkpoint_root_inside_workspace")
            if raw.exists() and _is_reparse_or_symlink(raw):
                raise WorkspaceValidationError("checkpoint_root_reparse_denied")
            if not raw.exists():
                self._publication.publish(
                    "workspace:checkpoint-root-create",
                    lambda: raw.mkdir(parents=True, exist_ok=False),
                )
            self._assert_no_reparse_chain(raw)
            resolved = raw.resolve(strict=True)
            if not resolved.is_dir() or _is_reparse_or_symlink(resolved):
                raise WorkspaceValidationError("checkpoint_root_invalid")
            if resolved == workspace or resolved.is_relative_to(workspace) or workspace.is_relative_to(resolved):
                raise WorkspaceValidationError("checkpoint_root_inside_workspace")
            return resolved
        except WorkspaceValidationError:
            raise
        except ExternalEffectPublicationError:
            raise
        except (TypeError, OSError, RuntimeError) as exc:
            raise WorkspaceValidationError("checkpoint_root_invalid") from exc

    def _assert_no_reparse_chain(self, path: Path) -> None:
        current = Path(path.anchor) if path.anchor else Path()
        for part in path.parts:
            if part == path.anchor:
                continue
            current = current / part
            if _existing(current) and _is_reparse_or_symlink(current):
                raise WorkspaceValidationError("reparse_root_denied")

    def _assert_roots(self) -> None:
        if not self._workspace_root.exists() or not self._workspace_root.is_dir():
            raise WorkspaceMutationError("workspace_root_unavailable")
        if _is_reparse_or_symlink(self._workspace_root):
            raise WorkspaceMutationError("workspace_root_reparse_denied")
        try:
            self._assert_no_reparse_chain(self._workspace_root)
        except WorkspaceValidationError as exc:
            raise WorkspaceMutationError("workspace_root_reparse_denied") from exc
        if not self._checkpoint_root.exists() or not self._checkpoint_root.is_dir():
            raise WorkspaceMutationError("checkpoint_root_unavailable")
        if _is_reparse_or_symlink(self._checkpoint_root):
            raise WorkspaceMutationError("checkpoint_root_reparse_denied")
        try:
            self._assert_no_reparse_chain(self._checkpoint_root)
        except WorkspaceValidationError as exc:
            raise WorkspaceMutationError("checkpoint_root_reparse_denied") from exc
        if self._checkpoint_root == self._workspace_root or self._checkpoint_root.is_relative_to(self._workspace_root) or self._workspace_root.is_relative_to(self._checkpoint_root):
            raise WorkspaceMutationError("checkpoint_root_inside_workspace")

    def _load_manifest(self) -> dict[str, Any]:
        if not self._manifest_path.exists():
            return {"version": _MANIFEST_VERSION, "checkpoints": {}, "actions": {}}
        if _is_reparse_or_symlink(self._manifest_path):
            raise WorkspaceValidationError("manifest_reparse_denied")
        try:
            raw = json.loads(self._manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise WorkspaceValidationError("manifest_invalid") from exc
        if not isinstance(raw, dict) or raw.get("version") != _MANIFEST_VERSION:
            raise WorkspaceValidationError("manifest_invalid")
        if not isinstance(raw.get("checkpoints"), dict) or not isinstance(raw.get("actions"), dict):
            raise WorkspaceValidationError("manifest_invalid")
        return raw

    def _persist_manifest(
        self,
        *,
        transaction: ExternalEffectTransaction,
    ) -> None:
        self._publication.assert_transaction(transaction)
        payload = json.dumps(
            self._manifest,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        ).encode("utf-8")
        self._atomic_write_bytes_unbounded(
            self._manifest_path,
            payload,
            transaction=transaction,
        )

    def _atomic_write_bytes_unbounded(
        self,
        target: Path,
        data: bytes,
        *,
        transaction: ExternalEffectTransaction,
    ) -> None:
        self._publication.assert_transaction(transaction)
        parent = target.parent
        if not parent.exists() or _is_reparse_or_symlink(parent):
            raise WorkspaceMutationError("manifest_parent_invalid")
        transaction.mark_mutation()
        fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=str(parent))
        temporary_path = Path(temporary)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            if _existing(target) and _is_reparse_or_symlink(target):
                raise WorkspaceMutationError("manifest_reparse_denied")
            os.replace(temporary_path, target)
            try:
                directory_fd = os.open(parent, os.O_RDONLY)
            except OSError:
                directory_fd = None
            if directory_fd is not None:
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        except Exception:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    def _checkpoint_dir(self, checkpoint_id: str) -> Path:
        _bounded_identifier(checkpoint_id, "checkpoint_id")
        candidate = self._checkpoint_root / checkpoint_id
        if candidate.parent != self._checkpoint_root:
            raise WorkspaceMutationError("checkpoint_boundary_lost")
        return candidate

    def _mkdir_safe(
        self,
        path: Path,
        *,
        transaction: ExternalEffectTransaction,
    ) -> None:
        self._publication.assert_transaction(transaction)
        if _existing(path) and _is_reparse_or_symlink(path):
            raise WorkspaceMutationError("checkpoint_reparse_denied")
        transaction.mark_mutation()
        path.mkdir(parents=True, exist_ok=True)
        if not path.is_dir() or _is_reparse_or_symlink(path):
            raise WorkspaceMutationError("checkpoint_directory_invalid")

    def _remove_checkpoint_tree(
        self,
        path: Path,
        *,
        transaction: ExternalEffectTransaction,
    ) -> None:
        """Remove checkpoint bytes only under the admitted typed guard."""

        self._publication.assert_transaction(transaction)
        if _is_reparse_or_symlink(path):
            raise WorkspaceMutationError("checkpoint_reparse_denied")
        transaction.mark_mutation()
        shutil.rmtree(path)

    def _encode_text(self, value: str) -> bytes:
        if not isinstance(value, str):
            raise WorkspaceValidationError("content_invalid")
        try:
            data = value.encode("utf-8", errors="strict")
        except UnicodeError as exc:
            raise WorkspaceValidationError("utf8_required") from exc
        return self._validate_bytes(data)

    def _decode_text(self, value: bytes) -> str:
        try:
            return value.decode("utf-8", errors="strict")
        except UnicodeError as exc:
            raise WorkspaceValidationError("utf8_required") from exc

    def _validate_bytes(self, value: bytes) -> bytes:
        if len(value) > self._max_file_bytes:
            raise WorkspaceValidationError("file_size_limit_exceeded")
        self._decode_text(value)
        return value

    def _build_diff(self, plans: Sequence[_Plan]) -> str:
        chunks: list[str] = []
        for plan in plans:
            operation = plan.operation
            if operation.kind == "move":
                assert operation.move_path is not None
                old_path, new_path = operation.path, operation.move_path
                old_state = plan.before[old_path]
                new_state = plan.after[new_path]
            else:
                old_path = new_path = operation.path
                old_state = plan.before[operation.path]
                new_state = plan.after[operation.path]
            old_text = self._decode_text(old_state.data or b"")
            new_text = self._decode_text(new_state.data or b"")
            diff = list(
                difflib.unified_diff(
                    old_text.splitlines(keepends=True),
                    new_text.splitlines(keepends=True),
                    fromfile=f"a/{old_path}",
                    tofile=f"b/{new_path}",
                    lineterm="\n",
                )
            )
            if not diff:
                diff = [f"--- a/{old_path}\n", f"+++ b/{new_path}\n"]
            chunks.extend(diff)
        return "".join(chunks)

    def _safe_diff_preview(self, diff: str) -> str:
        safe = self._redactor.safe_preview(diff)
        if not isinstance(safe, str):
            return "[DIFF_PREVIEW_UNAVAILABLE]"
        encoded = safe.encode("utf-8", errors="replace")
        if len(encoded) <= _MAX_PREVIEW_BYTES:
            return safe
        return encoded[:_MAX_PREVIEW_BYTES].decode("utf-8", errors="ignore") + "\n[DIFF_TRUNCATED]"

    def _aggregate_digest(self, states: Mapping[str, _FileState]) -> str:
        projection = [
            [path, states[path].digest if states[path].exists else _MISSING, states[path].size]
            for path in sorted(states)
        ]
        return _digest_json(projection)

    def _operation_fingerprint(self, operation: _Operation) -> Mapping[str, Any]:
        return {
            "kind": operation.kind,
            "path": operation.path,
            "move_path": operation.move_path,
            "content_digest": _digest_bytes(operation.content) if operation.content is not None else None,
            "content_size": len(operation.content) if operation.content is not None else 0,
            "expected_before_digest": operation.expected_before_digest,
        }

    def _action_key(
        self,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        operations: Sequence[_Operation],
    ) -> str:
        return self._action_key_for_kinds(
            action_request_id,
            engram_id,
            turn_id,
            epoch,
            tuple(operation.kind for operation in operations),
        )

    def _action_key_for_kinds(
        self,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        kinds: Sequence[str],
    ) -> str:
        return _digest_json(
            [self._world_id, engram_id, turn_id, epoch, action_request_id, list(kinds)]
        )

    def _find_action_checkpoint(
        self,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        request_digest: str,
    ) -> dict[str, Any] | None:
        matches = [
            record
            for record in self._manifest["checkpoints"].values()
            if isinstance(record, dict)
            and record.get("action_request_id") == action_request_id
            and record.get("engram_id") == engram_id
            and record.get("turn_id") == turn_id
            and record.get("epoch") == epoch
        ]
        if not matches:
            return None
        record = sorted(matches, key=lambda item: str(item.get("created_at", "")), reverse=True)[0]
        if record.get("request_digest") != request_digest:
            return {
                "checkpoint_id": record.get("checkpoint_id"),
                "state": "conflict",
                "changed_paths": record.get("changed_paths", ()),
            }
        return record

    def _replay_or_fence_request(
        self,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        tool_name: str,
        request_digest: str,
    ) -> Mapping[str, Any] | None:
        action_key = self._action_key_for_kinds(
            action_request_id,
            engram_id,
            turn_id,
            epoch,
            (tool_name,),
        )
        prior = self._manifest["actions"].get(action_key)
        if isinstance(prior, Mapping):
            if prior.get("request_digest") != request_digest:
                return self._failure(
                    "action_request_conflict",
                    detail="action_request_id was reused with different input",
                    action_request_id=action_request_id,
                    tool_name=tool_name,
                )
            result = prior.get("result")
            if isinstance(result, Mapping):
                return {**dict(result), "idempotent": True}
        checkpoint = self._find_action_checkpoint(
            action_request_id,
            engram_id,
            turn_id,
            epoch,
            request_digest,
        )
        if checkpoint is None:
            return None
        if checkpoint.get("state") == "conflict":
            return self._failure(
                "action_request_conflict",
                detail="action_request_id was reused with different input",
                action_request_id=action_request_id,
                checkpoint_id=checkpoint.get("checkpoint_id"),
                tool_name=tool_name,
            )
        if checkpoint.get("state") in {"sealed", "partially_restored", "restored"}:
            return self._result_from_record(checkpoint, idempotent=True)
        return self._failure(
            "action_recovery_required",
            detail="an unfinished checkpoint exists and will not be applied again automatically",
            action_request_id=action_request_id,
            checkpoint_id=checkpoint.get("checkpoint_id"),
            state="uncertain",
            changed_paths=checkpoint.get("changed_paths", ()),
        )

    def _remember_action(
        self,
        action_key: str,
        request_digest: str,
        result: Mapping[str, Any],
        *,
        transaction: ExternalEffectTransaction,
    ) -> None:
        self._publication.assert_transaction(transaction)
        self._manifest["actions"][action_key] = {
            "request_digest": request_digest,
            "checkpoint_id": result.get("checkpoint_id"),
            "result": dict(result),
        }
        self._persist_manifest(transaction=transaction)

    def _result_from_record(self, record: Mapping[str, Any], *, idempotent: bool) -> Mapping[str, Any]:
        return {
            "ok": True,
            "status": "completed",
            "content": "Workspace mutation completed.",
            "data": {
                "action_request_id": record.get("action_request_id"),
                "state": "completed",
                "execution_status": "completed",
                "evidence_class": self.evidence_class,
                "evidence_binding": dict(self.evidence_binding),
                "checkpoint_id": record.get("checkpoint_id"),
                "changed_paths": list(record.get("changed_paths", ())),
                "paths": list(record.get("changed_paths", ())),
                "diff_preview": record.get("diff_preview", "[DIFF_PREVIEW_UNAVAILABLE]"),
                "diff_digest": record.get("diff_digest"),
                "before_digest": record.get("before_digest"),
                "post_digest": record.get("post_digest"),
            },
            "idempotent": idempotent,
        }

    def _restore_result(self, record: Mapping[str, Any], *, status: str, idempotent: bool) -> Mapping[str, Any]:
        return {
            "ok": True,
            "status": status,
            "checkpoint_id": record.get("checkpoint_id"),
            "changed_paths": list(record.get("changed_paths", ())),
            "restored_paths": list(record.get("restored_paths", ())),
            "before_digest": record.get("before_digest"),
            "post_digest": record.get("post_digest"),
            "evidence_class": self.evidence_class,
            "idempotent": idempotent,
            "reconciled_from_uncertain": record.get("reconciled_from_uncertain") is True,
        }

    def _safe_checkpoint(self, record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            "checkpoint_id": record.get("checkpoint_id"),
            "state": record.get("state"),
            "world_id": record.get("world_id"),
            "engram_id": record.get("engram_id"),
            "turn_id": record.get("turn_id"),
            "epoch": record.get("epoch"),
            "action_request_id": record.get("action_request_id"),
            "changed_paths": list(record.get("changed_paths", ())),
            "restored_paths": list(record.get("restored_paths", ())),
            "before_digest": record.get("before_digest"),
            "post_digest": record.get("post_digest"),
            "diff_digest": record.get("diff_digest"),
            "diff_preview": record.get("diff_preview", "[DIFF_PREVIEW_UNAVAILABLE]"),
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
            "evidence_class": self.evidence_class,
            "reconciled_from_uncertain": record.get("reconciled_from_uncertain") is True,
            "failure_code": record.get("failure_code"),
            "quarantined": record.get("quarantined") is True,
        }

    def _failure(
        self,
        error: str,
        *,
        detail: str,
        action_request_id: str | None = None,
        tool_name: str | None = None,
        checkpoint_id: str | None = None,
        state: str = "failed",
        changed_paths: Sequence[str] = (),
    ) -> Mapping[str, Any]:
        data: dict[str, Any] = {
            "state": state,
            "execution_status": "uncertain" if state == "uncertain" else state,
            "evidence_class": self.evidence_class,
            "error_code": error[:128],
            "changed_paths": list(changed_paths),
        }
        if action_request_id is not None:
            data["action_request_id"] = action_request_id
        if tool_name is not None:
            data["tool_name"] = tool_name
        if checkpoint_id is not None:
            data["checkpoint_id"] = checkpoint_id
        return {
            "ok": False,
            "status": state,
            "content": detail,
            "data": data,
            "error": error[:128],
        }

    def _failpoint(self, name: str) -> None:
        """A no-op production hook; tests may inject deterministic faults."""

        del name

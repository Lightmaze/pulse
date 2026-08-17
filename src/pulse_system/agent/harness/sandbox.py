"""Codex standalone sandbox CLI adapter for the Pulse action seam.

This module is deliberately a process adapter, not an Agent runtime.  It
invokes only ``codex sandbox`` with an argv vector; Pi remains the sole
model/tool/settle loop.  A missing or unverified CLI is a normal, explicit
state and never becomes an unrestricted subprocess fallback.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shlex
import shutil
import signal as os_signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .process_containment import (
    WindowsContainedPopen as _WindowsContainedPopen,
    WindowsJobApi as _WindowsJobApi,
    establish_suspended_job_boundary as _establish_suspended_job_boundary,
)
from .security import LIVE_GATE_UNVERIFIED, LIVE_OS_RESTRICTED, Redactor
from .terminal import BackendProcessState, BackendSnapshot, ProcessSpec

__all__ = [
    "CodexCliPipeProcessBackend",
    "CodexCliSandboxBackend",
    "PipeLifecycleGate",
    "SandboxLiveGate",
    "SandboxPreflight",
]


_MAX_PREFLIGHT_OUTPUT = 16 * 1024
_DEFAULT_OUTPUT_BYTES = 64 * 1024
_DEFAULT_TIMEOUT_SECONDS = 60.0
_MAX_TIMEOUT_SECONDS = 300.0
_LIVE_GATE_ARTIFACT_VERSION = "pulse.read-only-live-gate.v2"
_MAX_LIVE_GATE_BYTES = 64 * 1024
_MAX_LIVE_GATE_VALIDITY_SECONDS = 7 * 24 * 60 * 60
_PIPE_LIFECYCLE_ARTIFACT_VERSION = "pulse.pipe-lifecycle-gate.v2"
_PIPE_BACKEND_IMPLEMENTATION = "codex_cli_pipe_process.windows_job.v1"
_PIPE_OWNER_DEATH_IMPLEMENTATION_AVAILABLE = os.name == "nt"
_MAX_PIPE_LIFECYCLE_BYTES = 64 * 1024
_PIPE_TRANSPORT = "pipe"
_PIPE_SESSION_SCOPE = "runtime_connection"
_TREE_CONTAINMENT_UNVERIFIED = "UNVERIFIED"
_TREE_CONTAINMENT_OBSERVED = "OBSERVED_CLEANUP"
_TREE_CONTAINMENT_JOB = "JOB_OBJECT_VERIFIED"
_REQUIRED_PIPE_LIFECYCLE_CHECKS = (
    "owner_death_containment",
    "workspace_write_denied",
    "environment_sentinel_not_leaked",
    "background_output_streaming",
    "stop_terminal_cleanup",
    "registry_thread_cleanup",
)
_REQUIRED_PIPE_PASS_CODES = {
    "owner_death_containment": "kill_on_owner_death_verified",
    "workspace_write_denied": "workspace_write_denied",
    "environment_sentinel_not_leaked": "sentinel_absent",
    "background_output_streaming": "multiple_stream_chunks_observed",
    "stop_terminal_cleanup": "stop_terminal_cleanup_observed",
    "registry_thread_cleanup": "registry_thread_cleanup_observed",
}
_PIPE_LIFECYCLE_TOP_LEVEL_KEYS = frozenset(
    {
        "artifact_version",
        "gate_id",
        "status",
        "evidence_class",
        "generated_at",
        "expires_at",
        "operator_invoked",
        "transport",
        "session_scope",
        "tree_containment",
        "backend_implementation",
        "sandbox_gate_id",
        "executable_sha256",
        "executable_path_digest",
        "workspace_boundary_digest",
        "codex_config_path_digest",
        "codex_config_sha256",
        "checks",
    }
)
_HEX_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REQUIRED_LIVE_GATE_CHECKS = (
    "preflight",
    "implementation_binding",
    "read_argv_cwd",
    "workspace_write_denied",
    "outside_write_denied",
    "network_denied",
    "environment_sentinel_not_leaked",
    "timeout_cleanup",
    "cancel_cleanup",
    "production_backend_binding",
)
_LIVE_GATE_TOP_LEVEL_KEYS = frozenset(
    {
        "artifact_version",
        "gate_id",
        "status",
        "evidence_class",
        "generated_at",
        "expires_at",
        "operator_invoked",
        "permission_profile",
        "executable_version",
        "executable_sha256",
        "executable_path_digest",
        "workspace_boundary_digest",
        "codex_config_path_digest",
        "codex_config_sha256",
        "sandbox_implementation",
        "sandbox_implementation_source",
        "checks",
    }
)
_ALLOWED_ENV_NAMES = frozenset(
    {
        "PATH",
        "PATHEXT",
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "TEMP",
        "TMP",
        "TMPDIR",
        "USERPROFILE",
        "LOCALAPPDATA",
        "APPDATA",
        "LANG",
        "LC_ALL",
    }
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 string") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _path_digest(path: Path) -> str:
    canonical = os.path.normcase(str(path.expanduser().resolve()))
    return _sha256_bytes(canonical.encode("utf-8", errors="strict"))


def _canonical_gate_id(value: Mapping[str, Any]) -> str:
    payload = {key: item for key, item in value.items() if key != "gate_id"}
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return _sha256_bytes(encoded)


def _require_sha256(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or _HEX_SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return value


def _bounded_code(value: Any, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 96
        or re.fullmatch(r"[a-z0-9_.-]+", value) is None
    ):
        raise ValueError(f"{field_name} must be a bounded machine code")
    return value


@dataclass(frozen=True, slots=True)
class SandboxLiveGate:
    """Strict, machine-readable evidence from one explicit host gate run.

    The artifact contains only bounded booleans, codes and digests.  It never
    stores the executable path, workspace path, raw environment or command
    output.  Runtime activation still rebinds every digest to the current
    executable, effective Codex config and workspace before trusting it.
    """

    artifact_version: str
    gate_id: str
    status: str
    evidence_class: str
    generated_at: datetime
    expires_at: datetime
    operator_invoked: bool
    permission_profile: str
    executable_version: str
    executable_sha256: str
    executable_path_digest: str
    workspace_boundary_digest: str
    codex_config_path_digest: str
    codex_config_sha256: str
    sandbox_implementation: str
    sandbox_implementation_source: str
    checks: Mapping[str, Mapping[str, str]]

    def __post_init__(self) -> None:
        if self.artifact_version != _LIVE_GATE_ARTIFACT_VERSION:
            raise ValueError("unsupported sandbox live-gate artifact version")
        _require_sha256(self.gate_id, "gate_id")
        if self.status not in {"PASSED", "PARTIAL", "FAILED"}:
            raise ValueError("live-gate status must be PASSED, PARTIAL or FAILED")
        if self.evidence_class not in {LIVE_GATE_UNVERIFIED, LIVE_OS_RESTRICTED}:
            raise ValueError("unknown live-gate evidence class")
        if type(self.operator_invoked) is not bool:
            raise ValueError("operator_invoked must be a bool")
        if self.permission_profile != ":read-only":
            raise ValueError("only :read-only live-gate artifacts are supported")
        if not isinstance(self.executable_version, str) or not self.executable_version.strip():
            raise ValueError("executable_version must be non-empty")
        if len(self.executable_version.encode("utf-8")) > 256:
            raise ValueError("executable_version is too long")
        for field_name in (
            "executable_sha256",
            "executable_path_digest",
            "workspace_boundary_digest",
            "codex_config_path_digest",
            "codex_config_sha256",
        ):
            _require_sha256(getattr(self, field_name), field_name)
        if self.sandbox_implementation not in {"elevated", "unelevated", "unknown"}:
            raise ValueError("unknown sandbox implementation")
        _bounded_code(
            self.sandbox_implementation_source,
            "sandbox_implementation_source",
        )
        if not isinstance(self.generated_at, datetime) or (
            self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None
        ):
            raise ValueError("generated_at must be a timezone-aware datetime")
        if not isinstance(self.expires_at, datetime) or (
            self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None
        ):
            raise ValueError("expires_at must be a timezone-aware datetime")
        generated = self.generated_at.astimezone(timezone.utc)
        expires = self.expires_at.astimezone(timezone.utc)
        if expires <= generated:
            raise ValueError("live-gate expiry must follow generation")
        if (expires - generated).total_seconds() > _MAX_LIVE_GATE_VALIDITY_SECONDS:
            raise ValueError("live-gate validity exceeds the seven-day maximum")
        normalized_checks: dict[str, dict[str, str]] = {}
        if not isinstance(self.checks, Mapping):
            raise ValueError("live-gate checks must be an object")
        if set(self.checks) != set(_REQUIRED_LIVE_GATE_CHECKS):
            raise ValueError("live-gate checks do not match the required set")
        for name in _REQUIRED_LIVE_GATE_CHECKS:
            raw = self.checks.get(name)
            if not isinstance(raw, Mapping) or set(raw) != {"status", "code"}:
                raise ValueError(f"live-gate check {name} has an invalid shape")
            check_status = raw.get("status")
            if check_status not in {"PASS", "PARTIAL", "FAIL"}:
                raise ValueError(f"live-gate check {name} has an invalid status")
            normalized_checks[name] = {
                "status": str(check_status),
                "code": _bounded_code(raw.get("code"), f"checks.{name}.code"),
            }
        object.__setattr__(self, "generated_at", generated)
        object.__setattr__(self, "expires_at", expires)
        object.__setattr__(self, "executable_version", self.executable_version.strip())
        object.__setattr__(self, "checks", normalized_checks)
        expected_status = (
            "FAILED"
            if any(item["status"] == "FAIL" for item in normalized_checks.values())
            else (
                "PARTIAL"
                if any(item["status"] == "PARTIAL" for item in normalized_checks.values())
                else "PASSED"
            )
        )
        if self.status != expected_status:
            raise ValueError("live-gate status does not match its checks")
        expected_evidence = (
            LIVE_OS_RESTRICTED if expected_status == "PASSED" else LIVE_GATE_UNVERIFIED
        )
        if self.evidence_class != expected_evidence:
            raise ValueError("live-gate evidence class does not match its status")
        if expected_status == "PASSED" and not self.operator_invoked:
            raise ValueError("a passing live gate requires explicit operator invocation")
        if expected_status == "PASSED" and (
            self.sandbox_implementation != "elevated"
            or self.sandbox_implementation_source != "effective_default_config"
            or normalized_checks["implementation_binding"]
            != {"status": "PASS", "code": "elevated_bound"}
        ):
            raise ValueError(
                "a passing Windows live gate requires the effective elevated sandbox"
            )
        if self.gate_id != _canonical_gate_id(self.to_dict()):
            raise ValueError("live-gate gate_id does not match the canonical artifact")

    @property
    def verified(self) -> bool:
        return (
            self.status == "PASSED"
            and self.evidence_class == LIVE_OS_RESTRICTED
            and self.sandbox_implementation == "elevated"
            and self.sandbox_implementation_source == "effective_default_config"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_version": self.artifact_version,
            "gate_id": self.gate_id,
            "status": self.status,
            "evidence_class": self.evidence_class,
            "generated_at": self.generated_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "operator_invoked": self.operator_invoked,
            "permission_profile": self.permission_profile,
            "executable_version": self.executable_version,
            "executable_sha256": self.executable_sha256,
            "executable_path_digest": self.executable_path_digest,
            "workspace_boundary_digest": self.workspace_boundary_digest,
            "codex_config_path_digest": self.codex_config_path_digest,
            "codex_config_sha256": self.codex_config_sha256,
            "sandbox_implementation": self.sandbox_implementation,
            "sandbox_implementation_source": self.sandbox_implementation_source,
            "checks": {name: dict(self.checks[name]) for name in _REQUIRED_LIVE_GATE_CHECKS},
        }

    @classmethod
    def issue(
        cls,
        *,
        executable: str | Path,
        executable_version: str,
        workspace_root: str | Path,
        codex_config: str | Path,
        sandbox_implementation: str,
        sandbox_implementation_source: str,
        checks: Mapping[str, Mapping[str, str]],
        operator_invoked: bool,
        generated_at: datetime | None = None,
        valid_for_seconds: float = 24 * 60 * 60,
    ) -> "SandboxLiveGate":
        if (
            isinstance(valid_for_seconds, bool)
            or not isinstance(valid_for_seconds, (int, float))
            or not 60 <= float(valid_for_seconds) <= _MAX_LIVE_GATE_VALIDITY_SECONDS
        ):
            raise ValueError("valid_for_seconds must be between 60 and 604800")
        executable_path = Path(executable).expanduser().resolve()
        workspace = Path(workspace_root).expanduser().resolve()
        config_path = Path(codex_config).expanduser().resolve()
        if not executable_path.is_file() or not workspace.is_dir() or not config_path.is_file():
            raise ValueError("live-gate binding inputs must exist")
        generated_value = generated_at or _utc_now()
        if not isinstance(generated_value, datetime) or (
            generated_value.tzinfo is None or generated_value.utcoffset() is None
        ):
            raise ValueError("generated_at must be a timezone-aware datetime")
        generated = generated_value.astimezone(timezone.utc)
        normalized_checks = {name: dict(value) for name, value in checks.items()}
        status = (
            "FAILED"
            if any(value.get("status") == "FAIL" for value in normalized_checks.values())
            else (
                "PARTIAL"
                if any(value.get("status") == "PARTIAL" for value in normalized_checks.values())
                else "PASSED"
            )
        )
        payload: dict[str, Any] = {
            "artifact_version": _LIVE_GATE_ARTIFACT_VERSION,
            "gate_id": "0" * 64,
            "status": status,
            "evidence_class": (
                LIVE_OS_RESTRICTED if status == "PASSED" else LIVE_GATE_UNVERIFIED
            ),
            "generated_at": generated.isoformat(),
            "expires_at": (
                generated + timedelta(seconds=float(valid_for_seconds))
            ).isoformat(),
            "operator_invoked": operator_invoked,
            "permission_profile": ":read-only",
            "executable_version": executable_version.strip(),
            "executable_sha256": _sha256_file(executable_path),
            "executable_path_digest": _path_digest(executable_path),
            "workspace_boundary_digest": _path_digest(workspace),
            "codex_config_path_digest": _path_digest(config_path),
            "codex_config_sha256": _sha256_file(config_path),
            "sandbox_implementation": sandbox_implementation,
            "sandbox_implementation_source": sandbox_implementation_source,
            "checks": normalized_checks,
        }
        payload["gate_id"] = _canonical_gate_id(payload)
        return cls.from_mapping(payload)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SandboxLiveGate":
        if not isinstance(value, Mapping) or set(value) != _LIVE_GATE_TOP_LEVEL_KEYS:
            raise ValueError("live-gate artifact has unknown or missing fields")
        return cls(
            artifact_version=value.get("artifact_version"),
            gate_id=value.get("gate_id"),
            status=value.get("status"),
            evidence_class=value.get("evidence_class"),
            generated_at=_parse_utc(value.get("generated_at"), "generated_at"),
            expires_at=_parse_utc(value.get("expires_at"), "expires_at"),
            operator_invoked=value.get("operator_invoked"),
            permission_profile=value.get("permission_profile"),
            executable_version=value.get("executable_version"),
            executable_sha256=value.get("executable_sha256"),
            executable_path_digest=value.get("executable_path_digest"),
            workspace_boundary_digest=value.get("workspace_boundary_digest"),
            codex_config_path_digest=value.get("codex_config_path_digest"),
            codex_config_sha256=value.get("codex_config_sha256"),
            sandbox_implementation=value.get("sandbox_implementation"),
            sandbox_implementation_source=value.get("sandbox_implementation_source"),
            checks=value.get("checks"),
        )

    @classmethod
    def load(cls, path: str | Path) -> "SandboxLiveGate":
        artifact_path = Path(path).expanduser().resolve()
        if not artifact_path.is_file():
            raise ValueError("sandbox live-gate artifact does not exist")
        if artifact_path.stat().st_size > _MAX_LIVE_GATE_BYTES:
            raise ValueError("sandbox live-gate artifact exceeds 64 KiB")
        try:
            value = json.loads(artifact_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("sandbox live-gate artifact is not valid UTF-8 JSON") from exc
        return cls.from_mapping(value)

    def binding_error(
        self,
        *,
        executable: str | Path,
        executable_version: str,
        workspace_root: str | Path,
        codex_config: str | Path,
        permission_profile: str,
        now: datetime | None = None,
    ) -> str | None:
        current_value = now or _utc_now()
        if not isinstance(current_value, datetime) or (
            current_value.tzinfo is None or current_value.utcoffset() is None
        ):
            return "sandbox_live_gate_clock_invalid"
        current = current_value.astimezone(timezone.utc)
        if not self.verified:
            return "sandbox_live_gate_incomplete"
        if current < self.generated_at - timedelta(minutes=5):
            return "sandbox_live_gate_not_yet_valid"
        if current >= self.expires_at:
            return "sandbox_live_gate_expired"
        executable_path = Path(executable).expanduser().resolve()
        workspace = Path(workspace_root).expanduser().resolve()
        config_path = Path(codex_config).expanduser().resolve()
        if not executable_path.is_file() or not workspace.is_dir() or not config_path.is_file():
            return "sandbox_live_gate_binding_missing"
        if permission_profile != self.permission_profile:
            return "sandbox_live_gate_profile_mismatch"
        if executable_version.strip() != self.executable_version:
            return "sandbox_live_gate_version_mismatch"
        if _path_digest(executable_path) != self.executable_path_digest:
            return "sandbox_live_gate_executable_path_mismatch"
        if _sha256_file(executable_path) != self.executable_sha256:
            return "sandbox_live_gate_executable_digest_mismatch"
        if _path_digest(workspace) != self.workspace_boundary_digest:
            return "sandbox_live_gate_workspace_mismatch"
        if _path_digest(config_path) != self.codex_config_path_digest:
            return "sandbox_live_gate_config_path_mismatch"
        if _sha256_file(config_path) != self.codex_config_sha256:
            return "sandbox_live_gate_config_drift"
        return None

    def safe_binding(self) -> dict[str, str]:
        return {
            "gate_id": self.gate_id,
            "artifact_version": self.artifact_version,
            "permission_profile": self.permission_profile,
            "executable_version": self.executable_version,
            "executable_sha256": self.executable_sha256,
            "executable_path_digest": self.executable_path_digest,
            "workspace_boundary_digest": self.workspace_boundary_digest,
            "codex_config_path_digest": self.codex_config_path_digest,
            "codex_config_sha256": self.codex_config_sha256,
            "sandbox_implementation": self.sandbox_implementation,
            "sandbox_implementation_source": self.sandbox_implementation_source,
            "workspace_write_denied": (
                "DENIED_VERIFIED" if self.verified else "UNVERIFIED"
            ),
            "expires_at": self.expires_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class PipeLifecycleGate:
    """Independent background-process lifecycle evidence.

    The foreground read-only sandbox gate does not by itself prove that a
    detached process dies with its owning Pulse Runtime, cannot write within
    the workspace under the global read-only profile, or keeps a background
    environment sentinel out.  This second artifact binds those facts to the
    exact sandbox gate and pipe backend implementation.  The ``.pulse``
    protected-root rule remains a policy/contract concern and is deliberately
    not a host-live evidence axis here.  Workspace-confidential read isolation
    is a separate, currently unverified evidence axis; output redaction is not
    a confidentiality boundary.
    Merely using a process group or observing ``taskkill /T`` is insufficient;
    a passing v2 artifact requires a verified kill-on-close Job Object.
    """

    artifact_version: str
    gate_id: str
    status: str
    evidence_class: str
    generated_at: datetime
    expires_at: datetime
    operator_invoked: bool
    transport: str
    session_scope: str
    tree_containment: str
    backend_implementation: str
    sandbox_gate_id: str
    executable_sha256: str
    executable_path_digest: str
    workspace_boundary_digest: str
    codex_config_path_digest: str
    codex_config_sha256: str
    checks: Mapping[str, Mapping[str, str]]

    def __post_init__(self) -> None:
        if self.artifact_version != _PIPE_LIFECYCLE_ARTIFACT_VERSION:
            raise ValueError("unsupported pipe lifecycle-gate artifact version")
        _require_sha256(self.gate_id, "gate_id")
        if self.status not in {"PASSED", "PARTIAL", "FAILED"}:
            raise ValueError("pipe lifecycle-gate status is invalid")
        if self.evidence_class not in {
            LIVE_GATE_UNVERIFIED,
            LIVE_OS_RESTRICTED,
        }:
            raise ValueError("unknown pipe lifecycle evidence class")
        if type(self.operator_invoked) is not bool:
            raise ValueError("operator_invoked must be a bool")
        if self.transport != _PIPE_TRANSPORT:
            raise ValueError("pipe lifecycle gate must bind transport=pipe")
        if self.session_scope != _PIPE_SESSION_SCOPE:
            raise ValueError(
                "pipe lifecycle gate must bind session_scope=runtime_connection"
            )
        if self.tree_containment not in {
            _TREE_CONTAINMENT_UNVERIFIED,
            _TREE_CONTAINMENT_OBSERVED,
            _TREE_CONTAINMENT_JOB,
        }:
            raise ValueError("unknown tree containment classification")
        if self.backend_implementation != _PIPE_BACKEND_IMPLEMENTATION:
            raise ValueError("pipe lifecycle gate binds an unknown backend")
        for field_name in (
            "sandbox_gate_id",
            "executable_sha256",
            "executable_path_digest",
            "workspace_boundary_digest",
            "codex_config_path_digest",
            "codex_config_sha256",
        ):
            _require_sha256(getattr(self, field_name), field_name)
        if not isinstance(self.generated_at, datetime) or (
            self.generated_at.tzinfo is None
            or self.generated_at.utcoffset() is None
        ):
            raise ValueError("generated_at must be a timezone-aware datetime")
        if not isinstance(self.expires_at, datetime) or (
            self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None
        ):
            raise ValueError("expires_at must be a timezone-aware datetime")
        generated = self.generated_at.astimezone(timezone.utc)
        expires = self.expires_at.astimezone(timezone.utc)
        if expires <= generated:
            raise ValueError("pipe lifecycle-gate expiry must follow generation")
        if (expires - generated).total_seconds() > _MAX_LIVE_GATE_VALIDITY_SECONDS:
            raise ValueError("pipe lifecycle-gate validity exceeds seven days")

        if not isinstance(self.checks, Mapping):
            raise ValueError("pipe lifecycle-gate checks must be an object")
        if set(self.checks) != set(_REQUIRED_PIPE_LIFECYCLE_CHECKS):
            raise ValueError("pipe lifecycle-gate checks do not match the required set")
        normalized_checks: dict[str, dict[str, str]] = {}
        for name in _REQUIRED_PIPE_LIFECYCLE_CHECKS:
            raw = self.checks.get(name)
            if not isinstance(raw, Mapping) or set(raw) != {"status", "code"}:
                raise ValueError(f"pipe lifecycle check {name} has an invalid shape")
            check_status = raw.get("status")
            if check_status not in {"PASS", "PARTIAL", "FAIL"}:
                raise ValueError(f"pipe lifecycle check {name} has an invalid status")
            normalized_checks[name] = {
                "status": str(check_status),
                "code": _bounded_code(raw.get("code"), f"checks.{name}.code"),
            }

        expected_status = (
            "FAILED"
            if any(item["status"] == "FAIL" for item in normalized_checks.values())
            else (
                "PARTIAL"
                if any(
                    item["status"] == "PARTIAL"
                    for item in normalized_checks.values()
                )
                else "PASSED"
            )
        )
        if self.status != expected_status:
            raise ValueError("pipe lifecycle-gate status does not match its checks")
        expected_evidence = (
            LIVE_OS_RESTRICTED
            if expected_status == "PASSED"
            else LIVE_GATE_UNVERIFIED
        )
        if self.evidence_class != expected_evidence:
            raise ValueError(
                "pipe lifecycle-gate evidence does not match its status"
            )
        if expected_status == "PASSED":
            if not self.operator_invoked:
                raise ValueError(
                    "a passing pipe lifecycle gate requires operator invocation"
                )
            if self.tree_containment != _TREE_CONTAINMENT_JOB:
                raise ValueError(
                    "a passing pipe lifecycle gate requires a verified Job Object"
                )
            for name, code in _REQUIRED_PIPE_PASS_CODES.items():
                if normalized_checks[name] != {"status": "PASS", "code": code}:
                    raise ValueError(
                        f"passing pipe lifecycle check {name} lacks required evidence"
                    )

        object.__setattr__(self, "generated_at", generated)
        object.__setattr__(self, "expires_at", expires)
        object.__setattr__(self, "checks", normalized_checks)
        if self.gate_id != _canonical_gate_id(self.to_dict()):
            raise ValueError(
                "pipe lifecycle-gate id does not match the canonical artifact"
            )

    @property
    def verified(self) -> bool:
        return (
            self.status == "PASSED"
            and self.evidence_class == LIVE_OS_RESTRICTED
            and self.operator_invoked
            and self.tree_containment == _TREE_CONTAINMENT_JOB
            and all(
                self.checks[name]
                == {"status": "PASS", "code": _REQUIRED_PIPE_PASS_CODES[name]}
                for name in _REQUIRED_PIPE_LIFECYCLE_CHECKS
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_version": self.artifact_version,
            "gate_id": self.gate_id,
            "status": self.status,
            "evidence_class": self.evidence_class,
            "generated_at": self.generated_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "operator_invoked": self.operator_invoked,
            "transport": self.transport,
            "session_scope": self.session_scope,
            "tree_containment": self.tree_containment,
            "backend_implementation": self.backend_implementation,
            "sandbox_gate_id": self.sandbox_gate_id,
            "executable_sha256": self.executable_sha256,
            "executable_path_digest": self.executable_path_digest,
            "workspace_boundary_digest": self.workspace_boundary_digest,
            "codex_config_path_digest": self.codex_config_path_digest,
            "codex_config_sha256": self.codex_config_sha256,
            "checks": {
                name: dict(self.checks[name])
                for name in _REQUIRED_PIPE_LIFECYCLE_CHECKS
            },
        }

    @classmethod
    def issue(
        cls,
        *,
        sandbox_gate: SandboxLiveGate,
        workspace_root: str | Path,
        checks: Mapping[str, Mapping[str, str]],
        tree_containment: str,
        operator_invoked: bool,
        generated_at: datetime | None = None,
        valid_for_seconds: float = 24 * 60 * 60,
    ) -> "PipeLifecycleGate":
        """Encode externally observed facts; it does not run or prove them."""

        if not isinstance(sandbox_gate, SandboxLiveGate):
            raise TypeError("sandbox_gate must be a SandboxLiveGate")
        if (
            isinstance(valid_for_seconds, bool)
            or not isinstance(valid_for_seconds, (int, float))
            or not 60
            <= float(valid_for_seconds)
            <= _MAX_LIVE_GATE_VALIDITY_SECONDS
        ):
            raise ValueError("valid_for_seconds must be between 60 and 604800")
        workspace = Path(workspace_root).expanduser().resolve()
        if not workspace.is_dir():
            raise ValueError("workspace_root must be an existing directory")
        generated_value = generated_at or _utc_now()
        if not isinstance(generated_value, datetime) or (
            generated_value.tzinfo is None
            or generated_value.utcoffset() is None
        ):
            raise ValueError("generated_at must be a timezone-aware datetime")
        generated = generated_value.astimezone(timezone.utc)
        normalized_checks = {name: dict(value) for name, value in checks.items()}
        status = (
            "FAILED"
            if any(value.get("status") == "FAIL" for value in normalized_checks.values())
            else (
                "PARTIAL"
                if any(
                    value.get("status") == "PARTIAL"
                    for value in normalized_checks.values()
                )
                else "PASSED"
            )
        )
        if status == "PASSED" and not sandbox_gate.verified:
            raise ValueError(
                "a passing pipe lifecycle gate requires a verified sandbox gate"
            )
        binding = sandbox_gate.safe_binding()
        if _path_digest(workspace) != binding["workspace_boundary_digest"]:
            raise ValueError("workspace_root does not match sandbox gate")
        payload: dict[str, Any] = {
            "artifact_version": _PIPE_LIFECYCLE_ARTIFACT_VERSION,
            "gate_id": "0" * 64,
            "status": status,
            "evidence_class": (
                LIVE_OS_RESTRICTED
                if status == "PASSED"
                else LIVE_GATE_UNVERIFIED
            ),
            "generated_at": generated.isoformat(),
            "expires_at": (
                generated + timedelta(seconds=float(valid_for_seconds))
            ).isoformat(),
            "operator_invoked": operator_invoked,
            "transport": _PIPE_TRANSPORT,
            "session_scope": _PIPE_SESSION_SCOPE,
            "tree_containment": tree_containment,
            "backend_implementation": _PIPE_BACKEND_IMPLEMENTATION,
            "sandbox_gate_id": binding["gate_id"],
            "executable_sha256": binding["executable_sha256"],
            "executable_path_digest": binding["executable_path_digest"],
            "workspace_boundary_digest": binding["workspace_boundary_digest"],
            "codex_config_path_digest": binding["codex_config_path_digest"],
            "codex_config_sha256": binding["codex_config_sha256"],
            "checks": normalized_checks,
        }
        payload["gate_id"] = _canonical_gate_id(payload)
        return cls.from_mapping(payload)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "PipeLifecycleGate":
        if not isinstance(value, Mapping) or set(value) != _PIPE_LIFECYCLE_TOP_LEVEL_KEYS:
            raise ValueError(
                "pipe lifecycle-gate artifact has unknown or missing fields"
            )
        return cls(
            artifact_version=value.get("artifact_version"),
            gate_id=value.get("gate_id"),
            status=value.get("status"),
            evidence_class=value.get("evidence_class"),
            generated_at=_parse_utc(value.get("generated_at"), "generated_at"),
            expires_at=_parse_utc(value.get("expires_at"), "expires_at"),
            operator_invoked=value.get("operator_invoked"),
            transport=value.get("transport"),
            session_scope=value.get("session_scope"),
            tree_containment=value.get("tree_containment"),
            backend_implementation=value.get("backend_implementation"),
            sandbox_gate_id=value.get("sandbox_gate_id"),
            executable_sha256=value.get("executable_sha256"),
            executable_path_digest=value.get("executable_path_digest"),
            workspace_boundary_digest=value.get("workspace_boundary_digest"),
            codex_config_path_digest=value.get("codex_config_path_digest"),
            codex_config_sha256=value.get("codex_config_sha256"),
            checks=value.get("checks"),
        )

    @classmethod
    def load(cls, path: str | Path) -> "PipeLifecycleGate":
        artifact_path = Path(path).expanduser().resolve()
        if not artifact_path.is_file():
            raise ValueError("pipe lifecycle-gate artifact does not exist")
        if artifact_path.stat().st_size > _MAX_PIPE_LIFECYCLE_BYTES:
            raise ValueError("pipe lifecycle-gate artifact exceeds 64 KiB")
        try:
            value = json.loads(artifact_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "pipe lifecycle-gate artifact is not valid UTF-8 JSON"
            ) from exc
        return cls.from_mapping(value)

    def binding_error(
        self,
        *,
        sandbox_gate: SandboxLiveGate,
        workspace_root: str | Path,
        now: datetime | None = None,
    ) -> str | None:
        current_value = now or _utc_now()
        if not isinstance(current_value, datetime) or (
            current_value.tzinfo is None or current_value.utcoffset() is None
        ):
            return "pipe_lifecycle_gate_clock_invalid"
        current = current_value.astimezone(timezone.utc)
        if not self.verified:
            return "pipe_lifecycle_gate_incomplete"
        if not isinstance(sandbox_gate, SandboxLiveGate) or not sandbox_gate.verified:
            return "pipe_lifecycle_sandbox_gate_unverified"
        if current < self.generated_at - timedelta(minutes=5):
            return "pipe_lifecycle_gate_not_yet_valid"
        if current >= self.expires_at:
            return "pipe_lifecycle_gate_expired"
        workspace = Path(workspace_root).expanduser().resolve()
        if not workspace.is_dir():
            return "pipe_lifecycle_workspace_missing"
        binding = sandbox_gate.safe_binding()
        if _path_digest(workspace) != binding["workspace_boundary_digest"]:
            return "pipe_lifecycle_workspace_boundary_mismatch"
        expected = {
            "sandbox_gate_id": binding["gate_id"],
            "executable_sha256": binding["executable_sha256"],
            "executable_path_digest": binding["executable_path_digest"],
            "workspace_boundary_digest": binding["workspace_boundary_digest"],
            "codex_config_path_digest": binding["codex_config_path_digest"],
            "codex_config_sha256": binding["codex_config_sha256"],
        }
        for field_name, expected_value in expected.items():
            if getattr(self, field_name) != expected_value:
                return f"pipe_lifecycle_{field_name}_mismatch"
        return None

    def safe_binding(self) -> dict[str, str]:
        verified = self.verified
        return {
            "lifecycle_gate_id": self.gate_id,
            "artifact_version": self.artifact_version,
            "transport": self.transport,
            "session_scope": self.session_scope,
            "tree_containment": self.tree_containment,
            "backend_implementation": self.backend_implementation,
            "sandbox_gate_id": self.sandbox_gate_id,
            "workspace_write_denied": (
                "DENIED_VERIFIED" if verified else "UNVERIFIED"
            ),
            "environment_sentinel": (
                "NOT_LEAKED_VERIFIED" if verified else "UNVERIFIED"
            ),
            "confidential_read_isolation": "UNVERIFIED",
            "expires_at": self.expires_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class SandboxPreflight:
    """Safe discovery evidence for one configured Codex CLI."""

    available: bool
    executable: str | None
    version: str | None
    command_surface: tuple[str, ...]
    error_code: str | None
    detail: str
    evidence_class: str = LIVE_GATE_UNVERIFIED
    live_gate_id: str | None = None
    sandbox_implementation: str = "unknown"

    @property
    def adapter_state(self) -> str:
        if self.available and self.evidence_class == LIVE_OS_RESTRICTED:
            return "live_os_restricted"
        return "callable_preflighted" if self.available else "live_adapter_unimplemented"

    @property
    def live_gate_verified(self) -> bool:
        return self.available and self.evidence_class == LIVE_OS_RESTRICTED

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "executable": self.executable,
            "version": self.version,
            "command_surface": list(self.command_surface),
            "error_code": self.error_code,
            "detail": self.detail,
            "adapter_state": self.adapter_state,
            "evidence_class": self.evidence_class,
            "live_gate_id": self.live_gate_id,
            "sandbox_implementation": self.sandbox_implementation,
        }


class CodexCliSandboxBackend:
    """Run an allowlisted argv inside Codex's host sandbox CLI.

    The command surface intentionally mirrors Codex's public ``sandbox``
    subcommand rather than app-server.  The built-in ``:workspace`` profile
    provides workspace writes and restricted network access; ``--cd`` binds
    policy and process cwd to the Pulse workspace.  The adapter still reports
    ``LIVE_GATE_UNVERIFIED`` until a fresh adversarial live-gate artifact is
    rebound to the exact executable, version, profile, workspace and effective
    Codex config.  Probe mode exists only for the explicit operator runner and
    can never emit ``LIVE_OS_RESTRICTED``.
    """

    supports_progress = True

    def __init__(
        self,
        *,
        workspace_root: str | Path,
        executable: str | Path | None = None,
        permission_profile: str = ":read-only",
        preflight_timeout_seconds: float = 5.0,
        default_timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        max_output_bytes: int = _DEFAULT_OUTPUT_BYTES,
        runner: Callable[..., subprocess.CompletedProcess[bytes]] | None = None,
        live_gate: SandboxLiveGate | None = None,
        codex_config: str | Path | None = None,
        probe_mode: bool = False,
    ) -> None:
        self._workspace = Path(workspace_root).expanduser().resolve()
        if not self._workspace.is_dir():
            raise ValueError("workspace_root must be an existing directory")
        if not isinstance(permission_profile, str) or not permission_profile.strip():
            raise ValueError("permission_profile must be a non-empty string")
        if len(permission_profile.strip()) > 128:
            raise ValueError("permission_profile is too long")
        if permission_profile.strip() not in {":workspace", ":read-only"}:
            raise ValueError(
                "permission_profile must be :workspace or :read-only; full access is unsupported"
            )
        if (
            isinstance(preflight_timeout_seconds, bool)
            or not isinstance(preflight_timeout_seconds, (int, float))
            or not 0.5 <= float(preflight_timeout_seconds) <= 30
        ):
            raise ValueError("preflight_timeout_seconds must be between 0.5 and 30")
        if (
            isinstance(default_timeout_seconds, bool)
            or not isinstance(default_timeout_seconds, (int, float))
            or not 0 < float(default_timeout_seconds) <= _MAX_TIMEOUT_SECONDS
        ):
            raise ValueError(
                f"default_timeout_seconds must be between 0 and {_MAX_TIMEOUT_SECONDS:g}"
            )
        if type(max_output_bytes) is not int or not 1024 <= max_output_bytes <= 4 * 1024 * 1024:
            raise ValueError("max_output_bytes must be between 1024 and 4194304")
        if live_gate is not None and not isinstance(live_gate, SandboxLiveGate):
            raise TypeError("live_gate must be a SandboxLiveGate")
        if type(probe_mode) is not bool:
            raise ValueError("probe_mode must be a bool")
        if live_gate is not None and probe_mode:
            raise ValueError("probe_mode cannot consume a live-gate artifact")
        self._configured_executable = (
            None
            if executable is None
            else str(executable).strip()
        )
        if self._configured_executable == "":
            raise ValueError("executable must not be empty")
        self._permission_profile = permission_profile.strip()
        self._preflight_timeout = float(preflight_timeout_seconds)
        self._default_timeout = float(default_timeout_seconds)
        self._max_output_bytes = max_output_bytes
        self._runner = runner or subprocess.run
        self._live_gate = live_gate
        self._codex_config = (
            None if codex_config is None else Path(codex_config).expanduser().resolve()
        )
        self._probe_mode = probe_mode
        self._redactor = Redactor(workspace_root=self._workspace)
        self._lock = threading.RLock()
        self._process_condition = threading.Condition(self._lock)
        self._active_processes: dict[int, subprocess.Popen[Any]] = {}
        self._observed_processes = 0
        self._root_exit_only_processes = 0
        self._unknown_process_trees = 0
        self._preflight_result: SandboxPreflight | None = None
        self._resolved_executable: str | None = None
        self._profile_flag = "--permissions-profile"

    @property
    def workspace_root(self) -> Path:
        return self._workspace

    @property
    def executable(self) -> str | None:
        return self._resolved_executable

    @property
    def permission_profile(self) -> str:
        return self._permission_profile

    @property
    def live_gate(self) -> SandboxLiveGate | None:
        return self._live_gate

    @property
    def codex_config(self) -> Path | None:
        return self._codex_config

    @property
    def preflight_result(self) -> SandboxPreflight | None:
        with self._lock:
            return self._preflight_result

    @property
    def evidence_class(self) -> str:
        result = self.preflight()
        if not result.live_gate_verified or result.executable is None:
            return LIVE_GATE_UNVERIFIED
        try:
            binding_error = self._live_gate_binding_error(
                executable=result.executable,
                executable_version=result.version or "",
            )
        except (OSError, ValueError):
            return LIVE_GATE_UNVERIFIED
        return LIVE_OS_RESTRICTED if binding_error is None else LIVE_GATE_UNVERIFIED

    @property
    def evidence_binding(self) -> Mapping[str, str]:
        if self.evidence_class != LIVE_OS_RESTRICTED or self._live_gate is None:
            return {}
        return self._live_gate.safe_binding()

    def preflight(self, *, force: bool = False) -> SandboxPreflight:
        """Discover and invoke only the no-model Codex CLI surfaces."""

        with self._lock:
            if self._preflight_result is not None and not force:
                return self._preflight_result

        executable, discovery_error = self._discover_executable()
        if executable is None:
            result = SandboxPreflight(
                available=False,
                executable=None,
                version=None,
                command_surface=(),
                error_code=discovery_error or "codex_cli_not_found",
                detail="a callable Codex CLI was not discovered; no process adapter is enabled",
            )
            return self._remember_preflight(result)

        version_command = (executable, "--version")
        version_result, error = self._run_preflight(version_command)
        if version_result is None:
            result = SandboxPreflight(
                available=False,
                executable=executable,
                version=None,
                command_surface=(),
                error_code=error or "codex_cli_not_callable",
                detail="the configured Codex CLI could not be invoked safely",
            )
            return self._remember_preflight(result)
        if version_result.returncode != 0:
            result = SandboxPreflight(
                available=False,
                executable=executable,
                version=None,
                command_surface=(),
                error_code="codex_cli_version_failed",
                detail="the configured Codex CLI returned a non-zero version result",
            )
            return self._remember_preflight(result)

        version_text = self._safe_output(version_result.stdout, version_result.stderr).strip()
        help_result, error = self._run_preflight(
            (executable, "sandbox", "--help")
        )
        if help_result is None:
            result = SandboxPreflight(
                available=False,
                executable=executable,
                version=version_text,
                command_surface=(),
                error_code=error or "codex_sandbox_surface_unavailable",
                detail="the Codex CLI version is callable but its sandbox surface is unavailable",
            )
            return self._remember_preflight(result)
        help_text = self._safe_output(help_result.stdout, help_result.stderr)
        if help_result.returncode != 0 or "sandbox" not in help_text.casefold():
            result = SandboxPreflight(
                available=False,
                executable=executable,
                version=version_text,
                command_surface=(),
                error_code="codex_sandbox_surface_unavailable",
                detail="codex sandbox --help did not expose the standalone sandbox command",
            )
            return self._remember_preflight(result)
        profile_flag = (
            "--permissions-profile"
            if "--permissions-profile" in help_text
            else ("--permission-profile" if "--permission-profile" in help_text else None)
        )
        if profile_flag is None or "--cd" not in help_text:
            result = SandboxPreflight(
                available=False,
                executable=executable,
                version=version_text,
                command_surface=(),
                error_code="codex_sandbox_surface_unavailable",
                detail="codex sandbox --help lacks the required profile/cd command surface",
            )
            return self._remember_preflight(result)
        self._profile_flag = profile_flag
        gate_error = self._live_gate_binding_error(
            executable=executable,
            executable_version=version_text,
        )
        if gate_error is None and self._live_gate is not None:
            result = SandboxPreflight(
                available=True,
                executable=executable,
                version=version_text,
                command_surface=("sandbox", profile_flag, "--cd"),
                error_code=None,
                detail="Codex standalone sandbox CLI and bound adversarial read-only gate are valid",
                evidence_class=LIVE_OS_RESTRICTED,
                live_gate_id=self._live_gate.gate_id,
                sandbox_implementation=self._live_gate.sandbox_implementation,
            )
            return self._remember_preflight(result)
        result = SandboxPreflight(
            available=True,
            executable=executable,
            version=version_text,
            command_surface=("sandbox", profile_flag, "--cd"),
            error_code=gate_error,
            detail=(
                "Codex standalone sandbox CLI is callable; a bound adversarial OS live gate is not active"
            ),
            sandbox_implementation=(
                "unknown" if self._live_gate is None else self._live_gate.sandbox_implementation
            ),
        )
        return self._remember_preflight(result)

    def execute(
        self,
        *,
        action_request_id: str,
        engram_id: str,
        turn_id: str,
        epoch: int,
        tool_name: str,
        input_data: Mapping[str, Any],
        policy_preview: Mapping[str, Any],
        signal: Any = None,
        progress_callback: Callable[[Mapping[str, Any]], Any] | None = None,
    ) -> Mapping[str, Any]:
        """Execute only ``bash`` in Codex sandbox; file adapters remain separate."""

        del action_request_id, engram_id, turn_id, epoch, policy_preview
        if tool_name != "bash":
            return {
                "ok": False,
                "error": "sandbox_tool_not_supported",
                "status": "failed",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        if not isinstance(input_data, Mapping) or not isinstance(input_data.get("command"), str):
            return {
                "ok": False,
                "error": "sandbox_command_invalid",
                "status": "failed",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        background = input_data.get("background", False)
        if type(background) is not bool:
            return {
                "ok": False,
                "error": "sandbox_background_invalid",
                "status": "failed",
                "execution_status": "not_started",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        if background:
            # This class is the foreground continuation adapter.  A verified
            # TerminalSessionActionBackend must intercept background intent;
            # silently ignoring it here would turn a durable-session request
            # into a synchronous process with different lifetime semantics.
            return {
                "ok": False,
                "error": "pipe_session_unavailable",
                "status": "failed",
                "execution_status": "not_started",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        preflight = self.preflight()
        if not preflight.available or preflight.executable is None:
            return {
                "ok": False,
                "error": "sandbox_preflight_failed",
                "status": "failed",
                "preflight": preflight.to_dict(),
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        if not preflight.live_gate_verified:
            return {
                "ok": False,
                "error": preflight.error_code or "sandbox_live_gate_required",
                "status": "failed",
                "execution_status": "not_started",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        try:
            command = tuple(
                part for part in shlex.split(input_data["command"], posix=False) if part.strip()
            )
        except ValueError:
            command = ()
        if not command:
            return {
                "ok": False,
                "error": "sandbox_command_invalid",
                "status": "failed",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        timeout = input_data.get("timeout", self._default_timeout)
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 0 < float(timeout) <= _MAX_TIMEOUT_SECONDS
        ):
            timeout = self._default_timeout
        return self.execute_argv(
            command,
            timeout_seconds=float(timeout),
            signal=signal,
            progress_callback=progress_callback,
        )

    def execute_argv(
        self,
        command: tuple[str, ...],
        *,
        timeout_seconds: float | None = None,
        signal: Any = None,
        progress_callback: Callable[[Mapping[str, Any]], Any] | None = None,
    ) -> Mapping[str, Any]:
        """Execute an argv for the dedicated live-gate runner or a verified backend.

        Runtime never enables ``probe_mode``.  Probe executions are therefore
        useful for collecting adversarial facts but remain unverified until
        the resulting artifact is loaded and rebound by a new backend.
        """

        if not isinstance(command, tuple) or not command or any(
            not isinstance(part, str) or not part or "\x00" in part for part in command
        ):
            return {
                "ok": False,
                "error": "sandbox_command_invalid",
                "status": "failed",
                "execution_status": "not_started",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        timeout = self._default_timeout if timeout_seconds is None else timeout_seconds
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 0 < float(timeout) <= _MAX_TIMEOUT_SECONDS
        ):
            return {
                "ok": False,
                "error": "sandbox_timeout_invalid",
                "status": "failed",
                "execution_status": "not_started",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        preflight = self.preflight()
        if not preflight.available or preflight.executable is None:
            return {
                "ok": False,
                "error": "sandbox_preflight_failed",
                "status": "failed",
                "execution_status": "not_started",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        if not self._probe_mode and not preflight.live_gate_verified:
            return {
                "ok": False,
                "error": preflight.error_code or "sandbox_live_gate_required",
                "status": "failed",
                "execution_status": "not_started",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        before_error = None if self._probe_mode else self._live_gate_binding_error(
            executable=preflight.executable,
            executable_version=preflight.version or "",
        )
        if before_error is not None:
            return {
                "ok": False,
                "error": before_error,
                "status": "failed",
                "execution_status": "not_started",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        result = self._run_sandboxed(
            self.build_argv(command, executable=preflight.executable),
            timeout_seconds=float(timeout),
            signal=signal,
            progress_callback=progress_callback,
        )
        if self._probe_mode or result.get("ok") is not True:
            return result
        after_error = self._live_gate_binding_error(
            executable=preflight.executable,
            executable_version=preflight.version or "",
        )
        if after_error is not None:
            return {
                **result,
                "ok": False,
                "error": "sandbox_live_gate_drift",
                "binding_error": after_error,
                "status": "uncertain",
                "recovery_state": "uncertain",
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }
        assert self._live_gate is not None
        return {
            **result,
            "evidence_class": LIVE_OS_RESTRICTED,
            "evidence_binding": self._live_gate.safe_binding(),
        }

    def build_argv(
        self,
        command: tuple[str, ...],
        *,
        executable: str | None = None,
        cwd: str | Path | None = None,
    ) -> tuple[str, ...]:
        """Build the auditable argv vector without a shell boundary."""

        selected = executable or self._resolved_executable
        if not selected:
            raise ValueError("Codex CLI has not passed preflight")
        if not command or any("\x00" in item for item in command):
            raise ValueError("command argv is empty or contains NUL")
        selected_cwd = self.resolve_workspace_cwd(cwd)
        return (
            selected,
            "sandbox",
            self._profile_flag,
            self._permission_profile,
            "--cd",
            str(selected_cwd),
            "--",
            *command,
        )

    def resolve_workspace_cwd(self, cwd: str | Path | None = None) -> Path:
        """Resolve a cwd without broadening the bound workspace."""

        if cwd is None:
            return self._workspace
        raw = Path(cwd)
        candidate = raw if raw.is_absolute() else self._workspace / raw
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(self._workspace)
        except (OSError, ValueError) as exc:
            raise ValueError("cwd must resolve inside the bound workspace") from exc
        if not resolved.is_dir():
            raise ValueError("cwd must be an existing workspace directory")
        return resolved

    def minimal_env(self) -> dict[str, str]:
        """Return a fresh copy of the canonical allowlisted environment."""

        return self._minimal_env()

    def redact_pipe_output(self, data: bytes, stream: str) -> bytes:
        """Redact one bounded pipe chunk before it crosses the adapter seam."""

        if not isinstance(data, bytes):
            raise TypeError("pipe output must be bytes")
        if stream not in {"stdout", "stderr"}:
            raise ValueError("pipe stream must be stdout or stderr")
        return self._safe_output(data, b"").encode("utf-8", errors="replace")

    def terminate_process(
        self,
        process: subprocess.Popen[Any],
        *,
        force: bool = False,
    ) -> bool:
        """Reuse foreground cleanup without upgrading it to containment proof."""

        return self._terminate_tree(process, force=force)

    def _discover_executable(self) -> tuple[str | None, str | None]:
        # A bare PATH lookup is not an operator authorization.  RuntimeService
        # supplies an explicit configured executable and a separate live gate;
        # keep this adapter fail-closed even when it is constructed directly.
        configured = self._configured_executable or os.environ.get("PULSE_CODEX_EXECUTABLE")
        if configured is None:
            return None, "codex_cli_executable_required"
        if any(separator in configured for separator in ("\\", "/")) or Path(configured).suffix:
            candidate = configured
        else:
            candidate = shutil.which(configured)
            if candidate is None:
                return None, "codex_cli_not_found"

        path = Path(candidate).expanduser()
        if not path.is_absolute():
            resolved = shutil.which(str(path))
            if resolved is None:
                return None, "codex_cli_not_found"
            path = Path(resolved)
        try:
            path = path.resolve()
        except OSError:
            return None, "codex_cli_path_invalid"
        if not path.is_file():
            return None, "codex_cli_path_invalid"
        self._resolved_executable = str(path)
        return str(path), None

    def _live_gate_binding_error(
        self,
        *,
        executable: str,
        executable_version: str,
    ) -> str | None:
        if self._live_gate is None:
            return "sandbox_live_gate_required"
        if self._codex_config is None:
            return "sandbox_live_gate_config_required"
        try:
            return self._live_gate.binding_error(
                executable=executable,
                executable_version=executable_version,
                workspace_root=self._workspace,
                codex_config=self._codex_config,
                permission_profile=self._permission_profile,
            )
        except (OSError, ValueError):
            return "sandbox_live_gate_binding_unreadable"

    def _run_preflight(
        self,
        argv: tuple[str, ...],
    ) -> tuple[subprocess.CompletedProcess[bytes] | None, str | None]:
        try:
            result = self._runner(
                list(argv),
                cwd=str(self._workspace),
                env=self._minimal_env(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                timeout=self._preflight_timeout,
            )
        except subprocess.TimeoutExpired:
            return None, "codex_cli_preflight_timeout"
        except (OSError, ValueError):
            return None, "codex_cli_not_callable"
        return result, None

    def _run_sandboxed(
        self,
        argv: tuple[str, ...],
        *,
        timeout_seconds: float,
        signal: Any,
        progress_callback: Callable[[Mapping[str, Any]], Any] | None = None,
    ) -> dict[str, Any]:
        started = time.monotonic()
        stdout = bytearray()
        stderr = bytearray()
        output_lock = threading.Lock()
        truncated = {"stdout": False, "stderr": False}
        output_seq = {"value": 0}
        stream_observation_failed = {"value": False}
        try:
            creationflags = (
                subprocess.CREATE_NEW_PROCESS_GROUP
                if os.name == "nt"
                else 0
            )
            process = subprocess.Popen(
                list(argv),
                cwd=str(self._workspace),
                env=self._minimal_env(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                creationflags=creationflags,
                start_new_session=os.name != "nt",
            )
        except (OSError, ValueError):
            return {
                "ok": False,
                "error": "sandbox_spawn_failed",
                "status": "failed",
                "duration_ms": _duration_ms(started),
                "evidence_class": LIVE_GATE_UNVERIFIED,
            }

        with self._process_condition:
            self._active_processes[process.pid] = process
            self._observed_processes += 1

        def emit_piece(name: str, piece: bytes, *, is_truncated: bool) -> None:
            if progress_callback is None or not piece:
                return
            output_seq["value"] += 1
            try:
                accepted = progress_callback(
                    {
                        "stream": name,
                        "output_seq": output_seq["value"],
                        "chunk": self._safe_output(piece, b""),
                        "bytes": len(piece),
                        "truncated": is_truncated,
                    }
                )
                if accepted is False:
                    stream_observation_failed["value"] = True
            except Exception:
                stream_observation_failed["value"] = True

        def read_stream(stream: Any, target: bytearray, name: str) -> None:
            pending = bytearray()
            try:
                while True:
                    chunk = stream.read(4096)
                    if not chunk:
                        with output_lock:
                            emit_piece(
                                name,
                                bytes(pending),
                                is_truncated=truncated[name],
                            )
                        return
                    with output_lock:
                        remaining = self._max_output_bytes - len(target)
                        if remaining <= 0:
                            truncated[name] = True
                        elif len(chunk) > remaining:
                            retained = chunk[:remaining]
                            target.extend(retained)
                            pending.extend(retained)
                            truncated[name] = True
                        else:
                            target.extend(chunk)
                            pending.extend(chunk)
                        while b"\n" in pending:
                            line, _, rest = pending.partition(b"\n")
                            pending[:] = rest
                            emit_piece(
                                name,
                                line + b"\n",
                                is_truncated=truncated[name],
                            )
            except (OSError, ValueError):
                return

        assert process.stdout is not None and process.stderr is not None
        readers = (
            threading.Thread(
                target=read_stream,
                args=(process.stdout, stdout, "stdout"),
                name="pulse-sandbox-stdout",
                daemon=True,
            ),
            threading.Thread(
                target=read_stream,
                args=(process.stderr, stderr, "stderr"),
                name="pulse-sandbox-stderr",
                daemon=True,
            ),
        )
        for reader in readers:
            reader.start()

        status = "completed"
        error: str | None = None
        termination_proven = True
        deadline = time.monotonic() + timeout_seconds
        while process.poll() is None:
            if _is_cancelled(signal):
                status = "cancelled"
                error = "sandbox_cancelled"
                termination_proven = self._terminate_tree(process)
                if not termination_proven:
                    termination_proven = self._terminate_tree(process, force=True)
                break
            if time.monotonic() >= deadline:
                status = "timed_out"
                error = "sandbox_timeout"
                termination_proven = self._terminate_tree(process)
                if not termination_proven:
                    termination_proven = self._terminate_tree(process, force=True)
                break
            time.sleep(0.02)
        try:
            process.wait(timeout=2)
            termination_proven = process.poll() is not None
        except subprocess.TimeoutExpired:
            termination_proven = self._terminate_tree(process, force=True)
        for reader in readers:
            reader.join(timeout=1)

        exit_code = process.returncode
        if status == "completed" and exit_code != 0:
            status = "failed"
            error = "sandbox_command_failed"
            if b"CreateRestrictedToken failed" in bytes(stderr):
                error = "sandbox_runtime_unavailable"
        root_exited = process.poll() is not None
        recovery_state = "none"
        if status in {"cancelled", "timed_out"}:
            # taskkill /T, killpg and Popen.wait can prove only that the
            # wrapper/root exited.  They are not a per-shutdown descendant
            # census, so cancellation remains uncertain without a queried Job.
            recovery_state = "uncertain"
            status = "uncertain"
            error = "sandbox_cleanup_uncertain"
        process_tree_state = "root_exit_only" if root_exited else "unknown"
        with self._process_condition:
            self._active_processes.pop(process.pid, None)
            if root_exited:
                self._root_exit_only_processes += 1
            else:
                self._unknown_process_trees += 1
            self._process_condition.notify_all()
        return {
            "ok": status == "completed" and exit_code == 0,
            "error": error,
            "status": status,
            "exit_code": exit_code,
            "duration_ms": _duration_ms(started),
            "stdout": self._safe_output(bytes(stdout), b""),
            "stderr": self._safe_output(bytes(stderr), b""),
            "truncated": bool(truncated["stdout"] or truncated["stderr"]),
            "stream_observation_failed": stream_observation_failed["value"],
            "recovery_state": recovery_state,
            "process_tree_state": process_tree_state,
            "evidence_class": LIVE_GATE_UNVERIFIED,
        }

    def shutdown_evidence(self, timeout: float = 0.0) -> dict[str, Any]:
        """Signal active roots and return conservative process-tree evidence."""

        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(float(timeout))
            or float(timeout) < 0.0
            or float(timeout) > _MAX_TIMEOUT_SECONDS
        ):
            raise ValueError("timeout must be a finite bounded non-negative number")
        with self._process_condition:
            active = tuple(self._active_processes.values())
            observed = self._observed_processes
        for process in active:
            threading.Thread(
                target=self._terminate_tree,
                args=(process,),
                kwargs={"force": True},
                name=f"pulse-sandbox-shutdown-{process.pid}",
                daemon=True,
            ).start()
        deadline = time.monotonic() + float(timeout)
        with self._process_condition:
            while self._active_processes:
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    break
                self._process_condition.wait(timeout=remaining)
            unresolved = len(self._active_processes)
            unknown = self._unknown_process_trees
            root_only = self._root_exit_only_processes
        if observed == 0:
            process_tree_state = "not_applicable"
        elif unresolved or unknown:
            process_tree_state = "unknown"
        else:
            process_tree_state = "root_exit_only"
        return {
            "active_before": len(active),
            "observed_processes": observed,
            "root_exit_only_processes": root_only,
            "unresolved": unresolved,
            "owner_joined": unresolved == 0,
            "process_tree_state": process_tree_state,
        }

    def _terminate_tree(self, process: subprocess.Popen[Any], *, force: bool = False) -> bool:
        if process.poll() is not None:
            return True
        command_succeeded = False
        try:
            if os.name == "nt":
                result = subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    shell=False,
                    timeout=2,
                )
                command_succeeded = result.returncode == 0
            else:
                os.killpg(process.pid, os_signal.SIGKILL if force else os_signal.SIGTERM)
                command_succeeded = True
        except (OSError, subprocess.TimeoutExpired):
            command_succeeded = False
        if process.poll() is None and force:
            try:
                process.kill()
                command_succeeded = True
            except OSError:
                command_succeeded = False
        if process.poll() is None:
            try:
                process.wait(timeout=2 if not force else 1)
            except (subprocess.TimeoutExpired, OSError):
                return False
        return process.poll() is not None and (command_succeeded or process.poll() is not None)

    def _minimal_env(self) -> dict[str, str]:
        environment: dict[str, str] = {}
        for key, value in os.environ.items():
            if key.upper() in _ALLOWED_ENV_NAMES and isinstance(value, str):
                environment[key] = value
        return environment

    def _safe_output(self, stdout: bytes, stderr: bytes) -> str:
        combined = (stdout + (b"\n" if stdout and stderr else b"") + stderr)
        if len(combined) > _MAX_PREFLIGHT_OUTPUT:
            combined = combined[:_MAX_PREFLIGHT_OUTPUT]
        text = combined.decode("utf-8", errors="replace")
        safe = self._redactor.safe_preview(text)
        return safe if isinstance(safe, str) else str(safe)

    def _remember_preflight(self, result: SandboxPreflight) -> SandboxPreflight:
        with self._lock:
            self._preflight_result = result
        return result



def _launch_windows_contained_process(
    argv: tuple[str, ...],
    *,
    cwd: Path,
    env: Mapping[str, str],
) -> subprocess.Popen[Any]:
    """Cross the only production background spawn boundary."""

    if os.name != "nt":
        raise OSError("windows_job_object_unavailable")
    return _WindowsContainedPopen(
        list(argv),
        cwd=str(cwd),
        env=dict(env),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
        close_fds=True,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
    )


@dataclass(slots=True)
class _PipeProcessRecord:
    backend_id: str
    process: subprocess.Popen[Any]
    on_output: Callable[[bytes, str], Any]
    readers: tuple[threading.Thread, ...] = ()
    observation_error_code: str | None = None
    observation_error_detail: str | None = None
    termination_attempted: bool = False
    termination_proven: bool | None = None
    exit_observed_at: float | None = None
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)


class CodexCliPipeProcessBackend:
    """Bounded ProcessBackend mechanics for non-interactive Codex pipes.

    On Windows the production spawn seam creates ``codex sandbox`` suspended,
    assigns it to a Pulse-owned outer Job with
    ``JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE``, verifies the exact primary thread
    had suspend count one, and only then resumes it.  A process group remains
    useful for cooperative CTRL_BREAK, but neither it nor ``taskkill /T`` is
    used as containment evidence.
    """

    transport = _PIPE_TRANSPORT
    session_scope = _PIPE_SESSION_SCOPE
    supports_stdin = False
    supports_resize = False
    backend_implementation = _PIPE_BACKEND_IMPLEMENTATION

    def __init__(
        self,
        sandbox_backend: CodexCliSandboxBackend,
        *,
        lifecycle_gate: PipeLifecycleGate | None = None,
        max_processes: int = 8,
        reader_settle_timeout_seconds: float = 1.0,
    ) -> None:
        if not isinstance(sandbox_backend, CodexCliSandboxBackend):
            raise TypeError("sandbox_backend must be CodexCliSandboxBackend")
        if sandbox_backend.permission_profile != ":read-only":
            raise ValueError("pipe backend requires the :read-only profile")
        if lifecycle_gate is not None and not isinstance(
            lifecycle_gate, PipeLifecycleGate
        ):
            raise TypeError("lifecycle_gate must be a PipeLifecycleGate")
        if type(max_processes) is not int or not 1 <= max_processes <= 64:
            raise ValueError("max_processes must be between 1 and 64")
        if (
            isinstance(reader_settle_timeout_seconds, bool)
            or not isinstance(reader_settle_timeout_seconds, (int, float))
            or not 0.05 <= float(reader_settle_timeout_seconds) <= 5
        ):
            raise ValueError(
                "reader_settle_timeout_seconds must be between 0.05 and 5"
            )
        self._sandbox = sandbox_backend
        self._lifecycle_gate = lifecycle_gate
        self._max_processes = max_processes
        self._reader_settle_timeout = float(reader_settle_timeout_seconds)
        self._records: dict[str, _PipeProcessRecord] = {}
        self._lock = threading.RLock()
        self._containment_support_error = self._probe_containment_support()

    @property
    def sandbox_evidence(self) -> str:
        return self._sandbox.evidence_class

    @property
    def tree_containment(self) -> str:
        if not self._lifecycle_binding_verified():
            return _TREE_CONTAINMENT_UNVERIFIED
        if self._containment_support_error is not None:
            return _TREE_CONTAINMENT_UNVERIFIED
        assert self._lifecycle_gate is not None
        return self._lifecycle_gate.tree_containment

    @property
    def kill_on_owner_death_verified(self) -> bool:
        return (
            self._containment_support_error is None
            and self._lifecycle_binding_verified()
            and self.tree_containment == _TREE_CONTAINMENT_JOB
        )

    @property
    def availability_error(self) -> str | None:
        return self._execution_gate_error(force_preflight=False)

    @property
    def supports_execution(self) -> bool:
        return self.availability_error is None

    @property
    def evidence_class(self) -> str:
        return (
            LIVE_OS_RESTRICTED
            if self.supports_execution
            else LIVE_GATE_UNVERIFIED
        )

    @property
    def evidence_binding(self) -> Mapping[str, str]:
        axes = self.evidence_axes()
        safe = {
            "transport": axes["transport"],
            "session_scope": axes["session_scope"],
            "sandbox_evidence": axes["sandbox_evidence"],
            "tree_containment": axes["tree_containment"],
            "workspace_write_denied": axes["workspace_write_denied"],
            "environment_sentinel": axes["environment_sentinel"],
            "confidential_read_isolation": axes[
                "confidential_read_isolation"
            ],
            "background_lifecycle": axes["background_lifecycle"],
            "backend_implementation": self.backend_implementation,
        }
        sandbox_binding = self._sandbox.evidence_binding
        if sandbox_binding:
            safe["sandbox_gate_id"] = sandbox_binding["gate_id"]
        if self._lifecycle_gate is not None:
            safe["lifecycle_gate_id"] = self._lifecycle_gate.gate_id
        return safe

    def evidence_axes(self) -> dict[str, str]:
        gate_binding_verified = self._lifecycle_binding_verified()
        lifecycle_verified = self.kill_on_owner_death_verified
        return {
            "transport": self.transport,
            "session_scope": self.session_scope,
            "sandbox_evidence": self.sandbox_evidence,
            "tree_containment": self.tree_containment,
            "workspace_write_denied": (
                "DENIED_VERIFIED" if gate_binding_verified else "UNVERIFIED"
            ),
            "environment_sentinel": (
                "NOT_LEAKED_VERIFIED" if gate_binding_verified else "UNVERIFIED"
            ),
            "confidential_read_isolation": "UNVERIFIED",
            "background_lifecycle": (
                "VERIFIED" if lifecycle_verified else "UNVERIFIED"
            ),
            "evidence_class": self.evidence_class,
            "availability_error": self.availability_error or "none",
        }

    def spawn(
        self,
        spec: ProcessSpec,
        *,
        handle_id: str,
        on_output: Callable[[bytes, str], None],
    ) -> str:
        if not isinstance(spec, ProcessSpec):
            raise TypeError("spawn requires ProcessSpec")
        if not isinstance(handle_id, str) or not handle_id or "\x00" in handle_id:
            raise ValueError("handle_id must be a non-empty bounded string")
        if len(handle_id.encode("utf-8")) > 256:
            raise ValueError("handle_id is too long")
        if not callable(on_output):
            raise TypeError("on_output must be callable")
        if spec.foreground:
            raise RuntimeError("pipe_backend_requires_background")
        if spec.allow_stdin:
            raise RuntimeError("pipe_stdin_unsupported")
        if spec.env:
            raise RuntimeError("pipe_custom_environment_unsupported")
        if (
            Path(spec.cwd).is_absolute()
            or re.match(r"^[A-Za-z]:", spec.cwd) is not None
            or spec.cwd.startswith(("\\\\", "//"))
        ):
            raise RuntimeError("pipe_cwd_must_be_workspace_relative")
        if spec.timeout_sec is not None and spec.timeout_sec > _MAX_TIMEOUT_SECONDS:
            raise RuntimeError("pipe_timeout_exceeds_limit")

        gate_error = self._execution_gate_error(force_preflight=True)
        if gate_error is not None:
            raise RuntimeError(gate_error)
        preflight = self._sandbox.preflight()
        if preflight.executable is None:
            raise RuntimeError("sandbox_preflight_failed")
        cwd = self._sandbox.resolve_workspace_cwd(spec.cwd)
        argv = self._sandbox.build_argv(
            tuple(spec.argv),
            executable=preflight.executable,
            cwd=cwd,
        )

        # Rebind immediately before crossing the OS process boundary.  A
        # drifted sandbox/lifecycle artifact must never produce a child.
        gate_error = self._execution_gate_error(force_preflight=False)
        if gate_error is not None:
            raise RuntimeError(gate_error)

        backend_id = uuid.uuid4().hex
        with self._lock:
            if len(self._records) >= self._max_processes:
                raise RuntimeError("pipe_process_capacity_exhausted")
            try:
                process = _launch_windows_contained_process(
                    argv,
                    cwd=self._sandbox.workspace_root,
                    env=self._sandbox.minimal_env(),
                )
            except (OSError, ValueError) as exc:
                raise RuntimeError("pipe_spawn_failed") from exc
            try:
                record = _PipeProcessRecord(
                    backend_id=backend_id,
                    process=process,
                    on_output=on_output,
                )
                self._records[backend_id] = record
            except BaseException:
                terminate = getattr(process, "terminate_contained_tree", None)
                if callable(terminate):
                    terminate(3.0)
                raise

        if process.stdout is None or process.stderr is None:
            self._mark_observation_failure(
                record,
                "pipe_stream_unavailable",
                "the spawned process did not expose both pipe streams",
            )
            return backend_id

        readers = (
            threading.Thread(
                target=self._read_stream,
                args=(record, process.stdout, "stdout"),
                name="pulse-pipe-stdout",
                daemon=True,
            ),
            threading.Thread(
                target=self._read_stream,
                args=(record, process.stderr, "stderr"),
                name="pulse-pipe-stderr",
                daemon=True,
            ),
        )
        started_readers: list[threading.Thread] = []
        try:
            for reader in readers:
                reader.start()
                started_readers.append(reader)
        except RuntimeError:
            self._mark_observation_failure(
                record,
                "pipe_reader_start_failed",
                "a bounded pipe reader could not start",
            )
        finally:
            record.readers = tuple(started_readers)
        return backend_id

    def poll(self, backend_id: str) -> BackendSnapshot:
        record = self._get_record(backend_id)
        if record is None:
            return BackendSnapshot(
                BackendProcessState.UNKNOWN,
                error_code="pipe_backend_handle_unknown",
                detail="the opaque pipe handle is not retained",
            )
        try:
            exit_code = record.process.poll()
        except (OSError, ValueError):
            return BackendSnapshot(
                BackendProcessState.UNKNOWN,
                error_code="pipe_process_poll_failed",
                detail="process state could not be observed",
            )
        with record.lock:
            observation_error = record.observation_error_code
            observation_detail = record.observation_error_detail
            termination_proven = record.termination_proven
        if observation_error is not None:
            return BackendSnapshot(
                (
                    BackendProcessState.FAILED
                    if exit_code is not None and termination_proven is True
                    else BackendProcessState.UNKNOWN
                ),
                exit_code=exit_code,
                error_code=observation_error,
                detail=observation_detail or "pipe output observation failed",
            )
        if exit_code is None:
            return BackendSnapshot(BackendProcessState.RUNNING)

        containment_state = getattr(record.process, "containment_state", None)
        if callable(containment_state):
            tree_exited = containment_state()
            if tree_exited is False:
                return BackendSnapshot(BackendProcessState.RUNNING)
            if tree_exited is None:
                return BackendSnapshot(
                    BackendProcessState.UNKNOWN,
                    exit_code=exit_code,
                    error_code="pipe_job_state_unobservable",
                    detail="the outer Job state could not be observed",
                )

        now = time.monotonic()
        with record.lock:
            if record.exit_observed_at is None:
                record.exit_observed_at = now
            exit_observed_at = record.exit_observed_at
        if any(reader.is_alive() for reader in record.readers):
            if now - exit_observed_at <= self._reader_settle_timeout:
                return BackendSnapshot(BackendProcessState.RUNNING)
            return BackendSnapshot(
                BackendProcessState.UNKNOWN,
                exit_code=exit_code,
                error_code="pipe_reader_settle_unproven",
                detail="process exited but bounded pipe readers did not settle",
            )
        return BackendSnapshot(BackendProcessState.EXITED, exit_code=exit_code)

    def write_stdin(self, backend_id: str, data: bytes) -> bool:
        del backend_id, data
        return False

    def interrupt(self, backend_id: str) -> bool:
        record = self._get_record(backend_id)
        if record is None:
            return False
        process = record.process
        if process.poll() is not None:
            return True
        try:
            if os.name == "nt":
                ctrl_break = getattr(
                    subprocess,
                    "CTRL_BREAK_EVENT",
                    getattr(os_signal, "CTRL_BREAK_EVENT", None),
                )
                if ctrl_break is None:
                    return False
                process.send_signal(ctrl_break)
            else:
                os.killpg(process.pid, os_signal.SIGTERM)
        except (OSError, ValueError):
            return False
        return True

    def kill(self, backend_id: str) -> bool:
        record = self._get_record(backend_id)
        if record is None:
            return False
        with record.lock:
            record.termination_attempted = True
        result = self._terminate_record(record)
        with record.lock:
            record.termination_proven = bool(result)
        return bool(result and record.process.poll() is not None)

    def cleanup(self, backend_id: str) -> bool:
        record = self._get_record(backend_id)
        if record is None:
            return False
        if record.process.poll() is None:
            return False
        for reader in record.readers:
            reader.join(timeout=self._reader_settle_timeout)
        if any(reader.is_alive() for reader in record.readers):
            return False
        close_containment = getattr(record.process, "close_containment", None)
        if callable(close_containment) and not close_containment():
            return False
        for stream in (record.process.stdout, record.process.stderr):
            if stream is None:
                continue
            try:
                stream.close()
            except (OSError, ValueError):
                return False
        with self._lock:
            if self._records.get(backend_id) is not record:
                return False
            del self._records[backend_id]
        return True

    def close(self) -> bool:
        with self._lock:
            backend_ids = tuple(self._records)
        all_clean = True
        for backend_id in backend_ids:
            record = self._get_record(backend_id)
            if record is None:
                continue
            if self._record_is_active(record) and not self.kill(backend_id):
                all_clean = False
            if not self.cleanup(backend_id):
                all_clean = False
        return all_clean

    def registry_snapshot(self) -> dict[str, Any]:
        with self._lock:
            records = tuple(self._records.values())
        return {
            "max_processes": self._max_processes,
            "retained_processes": len(records),
            "active_processes": sum(
                self._record_is_active(record) for record in records
            ),
            "reader_threads": sum(
                reader.is_alive()
                for record in records
                for reader in record.readers
            ),
            "transport": self.transport,
            "supports_execution": self.supports_execution,
            "availability_error": self.availability_error,
        }

    def _execution_gate_error(self, *, force_preflight: bool) -> str | None:
        preflight = self._sandbox.preflight(force=force_preflight)
        if not preflight.available or preflight.executable is None:
            return preflight.error_code or "sandbox_preflight_failed"
        if not preflight.live_gate_verified:
            return preflight.error_code or "sandbox_live_gate_required"
        if self._sandbox.evidence_class != LIVE_OS_RESTRICTED:
            return "sandbox_live_gate_drift"
        if self._lifecycle_gate is None:
            return "pipe_lifecycle_gate_required"
        sandbox_gate = self._sandbox.live_gate
        if sandbox_gate is None:
            return "pipe_lifecycle_sandbox_gate_unverified"
        lifecycle_error = self._lifecycle_gate.binding_error(
            sandbox_gate=sandbox_gate,
            workspace_root=self._sandbox.workspace_root,
        )
        if lifecycle_error is not None:
            return lifecycle_error
        if (
            self._lifecycle_gate.safe_binding().get("workspace_write_denied")
            != "DENIED_VERIFIED"
        ):
            return "pipe_workspace_write_denial_unverified"
        if self._containment_support_error is not None:
            return self._containment_support_error
        return None

    def _lifecycle_binding_verified(self) -> bool:
        gate = self._lifecycle_gate
        sandbox_gate = self._sandbox.live_gate
        return bool(
            self._sandbox.evidence_class == LIVE_OS_RESTRICTED
            and gate is not None
            and gate.verified
            and sandbox_gate is not None
            and gate.binding_error(
                sandbox_gate=sandbox_gate,
                workspace_root=self._sandbox.workspace_root,
            )
            is None
        )

    def _get_record(self, backend_id: str) -> _PipeProcessRecord | None:
        if not isinstance(backend_id, str) or not backend_id:
            return None
        with self._lock:
            return self._records.get(backend_id)

    def _read_stream(
        self,
        record: _PipeProcessRecord,
        stream: Any,
        stream_name: str,
    ) -> None:
        try:
            read_chunk = getattr(stream, "read1", None)
            if not callable(read_chunk):
                read_chunk = stream.read
            while True:
                # BufferedReader.read(size) may wait to fill ``size`` on a
                # long-lived pipe.  read1 performs one raw read and therefore
                # preserves background progress before process exit.
                chunk = read_chunk(4096)
                if not chunk:
                    return
                safe = self._sandbox.redact_pipe_output(bytes(chunk), stream_name)
                accepted = record.on_output(safe, stream_name)
                if accepted is False:
                    raise RuntimeError("pipe output callback rejected the chunk")
        except Exception:
            self._mark_observation_failure(
                record,
                "pipe_output_observation_failed",
                "a pipe read, redaction, or callback boundary failed",
            )

    def _mark_observation_failure(
        self,
        record: _PipeProcessRecord,
        error_code: str,
        detail: str,
    ) -> None:
        with record.lock:
            if record.observation_error_code is not None:
                return
            record.observation_error_code = error_code
            record.observation_error_detail = detail
            record.termination_attempted = True
        termination_proven = self._terminate_record(record)
        with record.lock:
            record.termination_proven = bool(termination_proven)

    def _terminate_record(self, record: _PipeProcessRecord) -> bool:
        terminate_contained = getattr(
            record.process,
            "terminate_contained_tree",
            None,
        )
        if callable(terminate_contained):
            try:
                return bool(terminate_contained(3.0))
            except (OSError, ValueError):
                return False
        # Only contract doubles reach this branch.  The production launcher
        # always returns _WindowsContainedPopen and therefore never relies on
        # taskkill/process-group cleanup as owner-death evidence.
        return self._sandbox.terminate_process(record.process, force=True)

    @staticmethod
    def _record_is_active(record: _PipeProcessRecord) -> bool:
        try:
            if record.process.poll() is None:
                return True
            containment_state = getattr(
                record.process,
                "containment_state",
                None,
            )
            if callable(containment_state):
                return containment_state() is not True
            return False
        except (OSError, ValueError):
            return True

    @staticmethod
    def _probe_containment_support() -> str | None:
        if not _PIPE_OWNER_DEATH_IMPLEMENTATION_AVAILABLE:
            return "pipe_owner_death_containment_requires_windows"
        if any(
            not hasattr(subprocess, name)
            for name in (
                "_winapi",
                "Handle",
                "STARTUPINFO",
                "CREATE_NEW_PROCESS_GROUP",
            )
        ):
            return "pipe_owner_death_containment_unavailable"
        try:
            api = _WindowsJobApi()
            probe_handle = api.create_kill_on_close_job()
            if not api.close_handle(probe_handle):
                return "pipe_owner_death_containment_unavailable"
        except OSError:
            return "pipe_owner_death_containment_unavailable"
        return None


def _duration_ms(started: float) -> int:
    return max(0, min(3_600_000, int((time.monotonic() - started) * 1000)))


def _is_cancelled(value: Any) -> bool:
    if value is None:
        return False
    method = getattr(value, "is_set", None)
    if callable(method):
        try:
            return bool(method())
        except Exception:
            return True
    return bool(getattr(value, "cancelled", False) or getattr(value, "aborted", False))

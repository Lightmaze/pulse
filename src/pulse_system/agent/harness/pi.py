"""Persistent Pi Harness sessions and the Engram-to-session registry."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, TypedDict
from types import MappingProxyType

from pulse_system.agent.backends.base import BackendError
from pulse_system.agent.backends.pi import (
    PI_INSTALL_HINT,
    PiBackend,
    RpcConnectionLost,
    RpcTimeout,
)
from pulse_system.agent.harness.base import (
    BINDING_COMPONENT,
    BindingState,
    HarnessError,
    HarnessEventCallback,
    HarnessState,
    HarnessTurnResult,
    SessionBinding,
    binding_snapshot,
    load_binding_state,
    normalize_session_file,
)
from pulse_system.agent.harness.rpc import (
    PiRpcChannel,
    PiRpcCloseSummary,
    RpcProtocolError,
)
from pulse_system.agent.tools.gateway import PulseToolGateway
from pulse_system.core.runtime.publication import (
    RuntimeBootstrapPermit,
    RuntimePublicationError,
    RuntimePublicationPermit,
    RuntimeRecoveryPermit,
)

__all__ = [
    "PiHarnessCloseSummary",
    "PiHarnessRuntime",
    "PiProcessContext",
    "PiSession",
    "PiSessionCloseSummary",
    "merge_pi_settings",
    "pulse_extension_asset",
]

BindingCallback = Callable[[dict[str, Any]], None]
MetricsCallback = Callable[[str, dict[str, Any]], None]
_BindingSink = Callable[[SessionBinding], None]
_RotationCommit = Callable[["PiSession", SessionBinding, SessionBinding], None]

_SETTINGS_LOCK = threading.Lock()
_PI_PHYSICAL_FINAL_TREE_STATES = frozenset(
    {"not_applicable", "empty_verified"}
)
_PI_TREE_RANK = {
    "unknown": 0,
    "root_exit_only": 1,
    "not_applicable": 2,
    "empty_verified": 2,
}


class PiSessionCloseSummary(TypedDict):
    """Content-free owner evidence for one resident Engram session."""

    sessions_observed: int
    signal_dispatched: bool
    signal_sent: bool
    process_owners_observed: int
    process_owners_unresolved: int
    channel_reader_owners_observed: int
    channel_reader_owners_unresolved: int
    transport_reader_owners_observed: int
    transport_reader_owners_unresolved: int
    close_worker_owners_observed: int
    close_worker_owners_unresolved: int
    lifecycle_owner_unresolved: int
    internal_owner_unresolved: int
    unresolved: int
    owner_joined: bool
    process_tree_state: Literal[
        "not_applicable",
        "empty_verified",
        "root_exit_only",
        "unknown",
    ]
    continuity_writer_sealed: bool
    error_code: str | None


class PiHarnessCloseSummary(TypedDict):
    """Fleet shutdown result shaped for a lossless Runtime projection."""

    active_before: int
    sessions_observed: int
    signals_dispatched: int
    signals_sent: int
    process_owners_observed: int
    process_owners_unresolved: int
    channel_reader_owners_observed: int
    channel_reader_owners_unresolved: int
    transport_reader_owners_observed: int
    transport_reader_owners_unresolved: int
    close_worker_owners_observed: int
    close_worker_owners_unresolved: int
    lifecycle_owners_unresolved: int
    internal_owner_unresolved: int
    unresolved: int
    owner_joined: bool
    cancel_signalled: bool
    process_tree_state: Literal[
        "not_applicable",
        "empty_verified",
        "root_exit_only",
        "unknown",
    ]
    continuity_writers_sealed: int
    error_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PiProcessContext:
    """Per-live-process argv/env capability context.

    The callback is deliberately the only lifecycle handle retained here.
    Tokens and gateway addresses remain in the process env and Gateway's
    memory; they never become part of a binding snapshot or metric payload.
    """

    extra_args: tuple[str, ...] = ()
    env: Mapping[str, str] = field(default_factory=dict)
    revoke: Callable[[], None] = field(default=lambda: None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.extra_args, tuple):
            object.__setattr__(self, "extra_args", tuple(self.extra_args))
        if any(not isinstance(arg, str) or not arg for arg in self.extra_args):
            raise ValueError("PiProcessContext.extra_args must contain non-empty strings")
        if not isinstance(self.env, Mapping):
            raise TypeError("PiProcessContext.env must be a mapping")
        if any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in self.env.items()
        ):
            raise ValueError("PiProcessContext.env keys and values must be strings")
        if not callable(self.revoke):
            raise TypeError("PiProcessContext.revoke must be callable")
        object.__setattr__(self, "env", MappingProxyType(dict(self.env)))

    def with_extra_args(self, *args: str) -> "PiProcessContext":
        return PiProcessContext(
            extra_args=(*self.extra_args, *args),
            env=self.env,
            revoke=self.revoke,
        )

    def with_env(self, **values: str) -> "PiProcessContext":
        merged = dict(self.env)
        merged.update(values)
        return PiProcessContext(
            extra_args=self.extra_args,
            env=merged,
            revoke=self.revoke,
        )


def pulse_extension_asset() -> Path:
    """Return the installed package's absolute Pi extension asset path."""

    asset = Path(__file__).resolve().parents[1] / "extensions" / "pulse-tools.ts"
    if not asset.is_file():
        raise HarnessError(
            "pi_extension_asset_missing",
            "the installed Pulse Pi extension asset is missing",
            "install a package containing pulse_system/agent/extensions/pulse-tools.ts",
            phase="startup",
        )
    return asset


def _has_no_builtin_tools(extra_args: tuple[str, ...]) -> bool:
    return "--no-builtin-tools" in extra_args or "-nbt" in extra_args


def _has_no_extensions(extra_args: tuple[str, ...]) -> bool:
    return "--no-extensions" in extra_args or "-ne" in extra_args


def _validate_extension_tool_args(extra_args: tuple[str, ...]) -> None:
    """Reject flags that could disable or bypass the proxy seam.

    Pi intentionally applies ``--tools``/``--exclude-tools`` to built-ins,
    extension tools and custom tools alike.  There is no safe way to express
    "exclude only the native implementation" through those flags.  Explicit
    user extensions are also refused: until extension governance is live, an
    auto-discovered or second explicit extension could register a same-name
    mutable tool outside Pulse's Gateway.  The process adds exactly one
    absolute Pulse extension below.
    """

    unsafe = {"--tools", "-t", "--exclude-tools", "-xt", "--no-tools", "-nt"}
    explicit_extension = {"--extension", "-e"}
    if any(
        arg in unsafe
        or arg in explicit_extension
        or arg.startswith("--extension=")
        or arg.startswith("-e=")
        for arg in extra_args
    ):
        raise HarnessError(
            "pi_capability_args_unsafe",
            "Pi tool filtering or user extensions could bypass the Pulse proxy boundary",
            "remove --tools/--exclude-tools/--no-tools and all -e/--extension arguments",
            phase="startup",
        )


def merge_pi_settings(workspace: str | os.PathLike[str]) -> Path:
    """Merge project Pi settings, preserving unknown fields and disabling compaction.

    This runs before every Harness spawn.  It does not approve an untrusted
    workspace; the RPC-level disable-and-verify sequence remains the hard
    guarantee before a prompt is accepted.
    """

    root = Path(workspace).expanduser().resolve()
    if not root.is_dir():
        raise HarnessError(
            "pi_workspace_invalid",
            f"the Pi Harness workspace {str(root)!r} is not an existing directory",
            "create or select the intended workspace before starting the Harness",
            phase="settings",
        )
    settings_dir = root / ".pi"
    settings_path = settings_dir / "settings.json"

    with _SETTINGS_LOCK:
        try:
            settings_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise HarnessError(
                "pi_settings_unwritable",
                f"could not create Pi project settings directory {str(settings_dir)!r}: {exc}",
                "grant write access to the workspace .pi directory and retry",
                phase="settings",
            ) from exc

        had_bom = False
        mode: int | None = None
        if settings_path.exists():
            try:
                raw = settings_path.read_bytes()
                had_bom = raw.startswith(b"\xef\xbb\xbf")
                text = raw.decode("utf-8-sig" if had_bom else "utf-8")
                loaded = json.loads(text)
                mode = settings_path.stat().st_mode
            except (OSError, UnicodeError, ValueError) as exc:
                raise HarnessError(
                    "pi_settings_invalid",
                    f"Pi project settings {str(settings_path)!r} could not be read as UTF-8 JSON: {exc}",
                    "repair the existing JSON; the Harness left it unchanged",
                    phase="settings",
                ) from exc
            if not isinstance(loaded, dict):
                raise HarnessError(
                    "pi_settings_invalid",
                    f"Pi project settings {str(settings_path)!r} must contain a JSON object",
                    "replace the top-level value with an object; the Harness left it unchanged",
                    phase="settings",
                )
            settings: dict[str, Any] = loaded
        else:
            settings = {}

        compaction = settings.get("compaction")
        if compaction is None:
            compaction = {}
            settings["compaction"] = compaction
        elif not isinstance(compaction, dict):
            raise HarnessError(
                "pi_settings_invalid",
                f"Pi project settings {str(settings_path)!r} has a non-object compaction value",
                "repair compaction as an object; the Harness left the file unchanged",
                phase="settings",
            )

        if compaction.get("enabled") is False and settings_path.is_file():
            return settings_path
        compaction["enabled"] = False
        serialized = ("\ufeff" if had_bom else "") + json.dumps(
            settings,
            ensure_ascii=False,
            indent=2,
        ) + "\n"

        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                prefix="settings.",
                suffix=".tmp",
                dir=settings_dir,
                delete=False,
            ) as stream:
                temporary = stream.name
                stream.write(serialized)
                stream.flush()
                os.fsync(stream.fileno())
            if mode is not None:
                os.chmod(temporary, mode)
            os.replace(temporary, settings_path)
            temporary = None
        except OSError as exc:
            raise HarnessError(
                "pi_settings_unwritable",
                f"could not atomically update Pi project settings {str(settings_path)!r}: {exc}",
                "grant write access to the workspace .pi directory and retry",
                phase="settings",
            ) from exc
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass
        return settings_path


class _TraceBuffer:
    def __init__(self, limit: int):
        self._limit = max(0, limit)
        self._entries: list[dict[str, Any]] = []
        self._dropped = 0

    def add(self, event: dict[str, Any]) -> None:
        if len(self._entries) < self._limit:
            self._entries.append(dict(event, kind="pi.event"))
        else:
            self._dropped += 1

    def finish(self) -> list[dict[str, Any]]:
        entries = list(self._entries)
        if self._dropped:
            entries.append({
                "kind": "pi.trace_truncated",
                "dropped_events": self._dropped,
                "kept_events": self._limit,
            })
        return entries


def _assistant_from_event(event: Mapping[str, Any]) -> dict[str, Any] | None:
    message = event.get("message")
    if isinstance(message, dict) and message.get("role") == "assistant":
        return message
    return None


def _message_key(message: Mapping[str, Any]) -> str:
    try:
        return json.dumps(message, sort_keys=True, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return repr(message)


def _token(usage: Mapping[str, Any], name: str) -> int:
    value = usage.get(name)
    return value if type(value) is int and value >= 0 else 0


def _text_of(message: Mapping[str, Any] | None) -> str:
    if message is None:
        return ""
    content = message.get("content")
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts = [
        item.get("text", "")
        for item in content
        if isinstance(item, dict) and item.get("type") == "text"
    ]
    return "\n".join(part for part in parts if isinstance(part, str) and part).strip()


class _TurnObservation:
    """Aggregate only current-turn assistant terminal events."""

    def __init__(self) -> None:
        self.last_assistant: dict[str, Any] | None = None
        self.input_tokens = 0
        self.output_tokens = 0
        self.cached_tokens = 0
        self.cache_write_tokens = 0
        self.tool_calls = 0
        self.provider_requests = 0
        self._counted_message_keys: set[str] = set()

    def absorb(self, event: Mapping[str, Any]) -> None:
        event_type = event.get("type")
        if event_type == "tool_execution_start":
            self.tool_calls += 1
        if event_type not in ("message_end", "turn_end"):
            return
        message = _assistant_from_event(event)
        if message is None:
            return

        key = _message_key(message)
        if key not in self._counted_message_keys:
            self._counted_message_keys.add(key)
            usage = message.get("usage")
            if isinstance(usage, Mapping):
                self.provider_requests += 1
                self.input_tokens += _token(usage, "input")
                self.output_tokens += _token(usage, "output")
                self.cached_tokens += _token(usage, "cacheRead")
                self.cache_write_tokens += _token(usage, "cacheWrite")
        self.last_assistant = message


def _deadline(timeout_sec: float | None) -> float | None:
    if timeout_sec is None:
        return None
    if isinstance(timeout_sec, bool) or not isinstance(timeout_sec, (int, float)):
        raise ValueError("timeout_sec must be a number or None")
    if timeout_sec < 0:
        raise ValueError("timeout_sec must be non-negative")
    return time.monotonic() + float(timeout_sec)


def _remaining(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return deadline - time.monotonic()


class _PiContinuityFailure(Exception):
    def __init__(self, code: str, detail: str, remedy: str) -> None:
        self.code = code
        self.detail = detail
        self.remedy = remedy
        super().__init__(f"{code}: {detail}")


class _PiContinuityGuard:
    """Committed-tail and stale-writer boundary for Pi JSONL continuity.

    SessionBinding v1 remains untouched.  Content-free sidecars, keyed by the
    normalized session path, record the last terminal-projected byte prefix
    and whether the previous Pi writer was proven closed.  A tainted path is
    never truncated in place: its committed prefix is copied to a fresh Pi
    session path and the old path remains quarantined from canonical resume.
    """

    _VERSION = 1

    def __init__(
        self,
        workspace: Path,
        *,
        publication_permit: RuntimePublicationPermit | None,
        bootstrap_permit: RuntimeBootstrapPermit | None,
    ) -> None:
        if (
            publication_permit is not None
            and type(publication_permit) is not RuntimePublicationPermit
        ):
            raise TypeError("publication_permit must be a RuntimePublicationPermit or None")
        if bootstrap_permit is not None and type(bootstrap_permit) is not RuntimeBootstrapPermit:
            raise TypeError("bootstrap_permit must be a RuntimeBootstrapPermit or None")
        if (
            publication_permit is not None
            and bootstrap_permit is not None
            and (
                publication_permit.owner_id,
                publication_permit.epoch,
                publication_permit.generation,
            )
            != (
                bootstrap_permit.owner_id,
                bootstrap_permit.epoch,
                bootstrap_permit.generation,
            )
        ):
            raise ValueError(
                "publication_permit and bootstrap_permit must belong to the same Runtime generation"
            )
        self._workspace = workspace
        self._root = workspace / ".pulse" / "harness" / "pi" / "continuity"
        self._session_root = workspace / ".pulse" / "harness" / "pi" / "sessions"
        self._publication_permit = publication_permit
        self._bootstrap_permit = bootstrap_permit
        self._lock = threading.RLock()
        self._publication_revoked = threading.Event()
        self._last_committed_bytes = 0
        self._last_quarantined_tail_bytes = 0

    @staticmethod
    def _authority_failure(
        exc: RuntimePublicationError,
        *,
        activity: str,
    ) -> _PiContinuityFailure:
        code = (
            "pi_continuity_revoked"
            if exc.code == "publication_revoked"
            else f"pi_continuity_{exc.code}"
        )
        return _PiContinuityFailure(
            code,
            f"the Runtime {activity} permit rejected this Pi continuity transaction ({exc.code})",
            "re-enter through the matching Runtime lifecycle with a current typed permit",
        )

    def _require_session_open(self, *, detail: str, remedy: str) -> None:
        if self._publication_revoked.is_set():
            raise _PiContinuityFailure(
                "pi_continuity_revoked",
                detail,
                remedy,
            )

    @contextmanager
    def _publication_transaction(self):
        self._require_session_open(
            detail="Pi continuity publication was revoked before its transaction",
            remedy="re-enter through a new Runtime publication lifecycle",
        )
        permit = self._publication_permit
        if type(permit) is not RuntimePublicationPermit:
            raise _PiContinuityFailure(
                "pi_continuity_publication_permit_required",
                "Pi continuity publication has no RuntimePublicationPermit",
                "pass the current Runtime publication permit into PiHarnessRuntime",
            )
        try:
            with permit.transaction_guard():
                self._require_session_open(
                    detail="Pi continuity publication was revoked before its transaction",
                    remedy="re-enter through a new Runtime publication lifecycle",
                )
                yield
        except RuntimePublicationError as exc:
            raise self._authority_failure(exc, activity="publication") from exc

    @contextmanager
    def _bootstrap_transaction(self):
        self._require_session_open(
            detail="Pi continuity bootstrap was revoked before its transaction",
            remedy="re-enter through a new Runtime bootstrap lifecycle",
        )
        permit = self._bootstrap_permit
        if type(permit) is not RuntimeBootstrapPermit:
            raise _PiContinuityFailure(
                "pi_continuity_bootstrap_permit_required",
                "Pi resume recovery has no RuntimeBootstrapPermit",
                "pass the current Runtime bootstrap permit into PiHarnessRuntime",
            )
        try:
            with permit.transaction_guard():
                self._require_session_open(
                    detail="Pi continuity bootstrap was revoked before its transaction",
                    remedy="re-enter through a new Runtime bootstrap lifecycle",
                )
                yield
        except RuntimePublicationError as exc:
            raise self._authority_failure(exc, activity="bootstrap") from exc

    @contextmanager
    def _seal_transaction(
        self,
        recovery_permit: RuntimeRecoveryPermit | None,
    ):
        if recovery_permit is not None:
            if type(recovery_permit) is not RuntimeRecoveryPermit:
                raise TypeError(
                    "recovery_permit must be a RuntimeRecoveryPermit or None"
                )
            try:
                with recovery_permit.transaction_guard():
                    yield
            except RuntimePublicationError as exc:
                raise self._authority_failure(exc, activity="recovery") from exc
            return

        permit = self._publication_permit
        if type(permit) is not RuntimePublicationPermit:
            raise _PiContinuityFailure(
                "pi_continuity_publication_permit_required",
                "ordinary Pi writer sealing has no RuntimePublicationPermit",
                "pass publication authority, or a RuntimeRecoveryPermit after revocation",
            )
        try:
            with permit.transaction_guard():
                yield
        except RuntimePublicationError as exc:
            raise self._authority_failure(exc, activity="publication") from exc

    def publish_settings(self) -> Path:
        """Merge Runtime-owned Pi settings under ordinary publication authority."""

        with self._lock:
            self._require_session_open(
                detail="Pi settings could not be published after continuity revocation",
                remedy="create a new PiSession through a new Harness runtime",
            )
            with self._publication_transaction():
                return merge_pi_settings(self._workspace)

    @staticmethod
    def _key(session_file: str) -> str:
        normalized = os.path.normcase(normalize_session_file(session_file))
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def _paths(self, session_file: str) -> tuple[Path, Path, Path]:
        key = self._key(session_file)
        return (
            self._root / "watermarks" / f"{key}.json",
            self._root / "pending" / f"{key}.json",
            self._root / "writers" / f"{key}.json",
        )

    @staticmethod
    def _native_io_path(path: Path | str) -> str:
        """Return an OS-only long-path spelling without changing identity.

        Bindings and sidecar payloads retain ordinary normalized absolute
        paths.  Only filesystem calls receive Win32's extended spelling when
        the historical 260-character boundary is near.
        """

        raw = os.path.abspath(os.fspath(path))
        if os.name != "nt" or len(raw) < 248 or raw.startswith("\\\\?\\"):
            return raw
        if raw.startswith("\\\\"):
            return "\\\\?\\UNC\\" + raw[2:]
        return "\\\\?\\" + raw

    @staticmethod
    def _native_is_file(path: Path | str) -> bool:
        return os.path.isfile(_PiContinuityGuard._native_io_path(path))

    @staticmethod
    def _hash_prefix(path: Path, length: int | None = None) -> tuple[int, str]:
        digest = hashlib.sha256()
        total = 0
        remaining = length
        with open(_PiContinuityGuard._native_io_path(path), "rb") as source:
            while remaining is None or remaining > 0:
                size = 1024 * 1024 if remaining is None else min(1024 * 1024, remaining)
                chunk = source.read(size)
                if not chunk:
                    break
                digest.update(chunk)
                total += len(chunk)
                if remaining is not None:
                    remaining -= len(chunk)
        return total, digest.hexdigest()

    @staticmethod
    def _read_record(path: Path) -> dict[str, Any] | None:
        native_path = _PiContinuityGuard._native_io_path(path)
        if not os.path.isfile(native_path):
            return None
        try:
            with open(native_path, encoding="utf-8") as stream:
                value = json.load(stream)
        except (OSError, UnicodeError, ValueError) as exc:
            raise _PiContinuityFailure(
                "pi_continuity_metadata_invalid",
                f"Pi continuity metadata {str(path)!r} is unreadable: {exc}",
                "preserve the session JSONL and explicitly repair or quarantine the continuity sidecar",
            ) from exc
        if not isinstance(value, dict) or value.get("version") != 1:
            raise _PiContinuityFailure(
                "pi_continuity_metadata_invalid",
                f"Pi continuity metadata {str(path)!r} has an unsupported shape",
                "migrate the continuity sidecar explicitly before resuming this Engram",
            )
        return value

    @staticmethod
    def _write_record(path: Path, value: Mapping[str, Any]) -> None:
        native_parent = _PiContinuityGuard._native_io_path(path.parent)
        os.makedirs(native_parent, exist_ok=True)
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                # The final sidecar already carries the 64-character digest.
                # Repeating it in the temporary leaf can cross Win32's
                # historical MAX_PATH even while the final path is valid.
                prefix="atomic-",
                suffix=".tmp",
                dir=native_parent,
                delete=False,
            ) as stream:
                temporary = stream.name
                json.dump(dict(value), stream, ensure_ascii=True, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(
                temporary,
                _PiContinuityGuard._native_io_path(path),
            )
            temporary = None
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass

    def _watermark_record(
        self,
        *,
        session_file: str,
        session_id: str,
        committed_bytes: int,
        committed_sha256: str,
    ) -> dict[str, Any]:
        return {
            "version": self._VERSION,
            "session_file": normalize_session_file(session_file),
            "session_id": session_id,
            "committed_bytes": committed_bytes,
            "committed_sha256": committed_sha256,
        }

    def _validate_watermark(
        self,
        record: Mapping[str, Any],
        *,
        session_file: str,
        session_id: str,
    ) -> tuple[int, str]:
        normalized = normalize_session_file(session_file)
        committed_bytes = record.get("committed_bytes")
        committed_sha256 = record.get("committed_sha256")
        if (
            record.get("session_file") != normalized
            or record.get("session_id") != session_id
            or type(committed_bytes) is not int
            or committed_bytes < 0
            or not isinstance(committed_sha256, str)
            or len(committed_sha256) != 64
        ):
            raise _PiContinuityFailure(
                "pi_continuity_metadata_invalid",
                "the Pi continuity watermark does not match the persisted session identity",
                "do not resume this path until its binding and watermark are explicitly reconciled",
            )
        return committed_bytes, committed_sha256

    def prepare_resume(self, binding: SessionBinding) -> SessionBinding:
        """Return a safe binding, forking away from any unsealed writer/tail."""

        assert binding.session_file is not None
        assert binding.session_id is not None
        session_file = normalize_session_file(binding.session_file)
        source = Path(session_file)
        with self._lock:
            self._require_session_open(
                detail="this Pi session continuity authority has been revoked",
                remedy="create a new PiSession through a new Harness runtime",
            )
            watermark_path, pending_path, writer_path = self._paths(session_file)
            watermark = self._read_record(watermark_path)
            pending = self._read_record(pending_path)
            writer = self._read_record(writer_path)

            if watermark is None:
                if pending is not None or (
                    writer is not None and writer.get("state") == "open"
                ):
                    raise _PiContinuityFailure(
                        "pi_continuity_uncommitted",
                        "the materialized Pi binding has no terminal-projected committed tail",
                        "reconcile the accepted turn explicitly; do not adopt the current JSONL tail as canonical history",
                    )
                # Additive migration for pre-watermark bindings: adopt exactly
                # the legacy file visible before any new Pi writer is opened.
                committed_bytes, committed_sha256 = self._hash_prefix(source)
                watermark = self._watermark_record(
                    session_file=session_file,
                    session_id=binding.session_id,
                    committed_bytes=committed_bytes,
                    committed_sha256=committed_sha256,
                )
                with self._bootstrap_transaction():
                    self._write_record(watermark_path, watermark)

            committed_bytes, committed_sha256 = self._validate_watermark(
                watermark,
                session_file=session_file,
                session_id=binding.session_id,
            )
            actual_bytes = os.stat(self._native_io_path(source)).st_size
            if actual_bytes < committed_bytes:
                raise _PiContinuityFailure(
                    "pi_continuity_truncated",
                    "the Pi JSONL is shorter than its committed-tail watermark",
                    "restore the committed prefix from backup before resuming this Engram",
                )
            observed_bytes, observed_sha256 = self._hash_prefix(
                source, committed_bytes
            )
            if (
                observed_bytes != committed_bytes
                or observed_sha256 != committed_sha256
            ):
                raise _PiContinuityFailure(
                    "pi_continuity_diverged",
                    "the Pi JSONL committed prefix no longer matches its watermark",
                    "quarantine the divergent file and explicitly restore a verified canonical prefix",
                )

            writer_open = writer is not None and writer.get("state") == "open"
            requires_fork = (
                actual_bytes > committed_bytes or pending is not None or writer_open
            )
            self._last_committed_bytes = committed_bytes
            self._last_quarantined_tail_bytes = max(
                0, actual_bytes - committed_bytes
            )
            if not requires_fork:
                return binding

            return self._fork_committed_prefix(
                binding=binding,
                source=source,
                source_session_file=session_file,
                committed_bytes=committed_bytes,
                committed_sha256=committed_sha256,
                actual_bytes=actual_bytes,
                writer_open=writer_open,
                pending_present=pending is not None,
            )

    def _fork_committed_prefix(
        self,
        *,
        binding: SessionBinding,
        source: Path,
        source_session_file: str,
        committed_bytes: int,
        committed_sha256: str,
        actual_bytes: int,
        writer_open: bool,
        pending_present: bool,
    ) -> SessionBinding:
        """Isolate and publish one bootstrap-authorized canonical prefix."""

        assert binding.session_id is not None
        staged_path = self._copy_canonical_prefix(source, committed_bytes)
        copied_bytes, copied_sha256 = self._hash_prefix(staged_path)
        if copied_bytes != committed_bytes or copied_sha256 != committed_sha256:
            self._discard_bootstrap_staging(staged_path)
            raise _PiContinuityFailure(
                "pi_continuity_copy_diverged",
                "the isolated Pi canonical prefix did not match its committed watermark",
                "retry only after the stale writer and underlying filesystem are stable",
            )

        safe_path = staged_path.with_suffix(".jsonl")
        safe_file = normalize_session_file(str(safe_path))
        safe_watermark, _, _ = self._paths(safe_file)
        quarantine_path = (
            self._root
            / "quarantine"
            / f"{time.time_ns()}-{self._key(source_session_file)}.json"
        )
        try:
            with self._bootstrap_transaction():
                os.replace(
                    self._native_io_path(staged_path),
                    self._native_io_path(safe_path),
                )
                try:
                    self._write_record(
                        safe_watermark,
                        self._watermark_record(
                            session_file=safe_file,
                            session_id=binding.session_id,
                            committed_bytes=committed_bytes,
                            committed_sha256=committed_sha256,
                        ),
                    )
                    self._write_record(
                        quarantine_path,
                        {
                            "version": self._VERSION,
                            "source_session_file": source_session_file,
                            "canonical_session_file": safe_file,
                            "session_id": binding.session_id,
                            "committed_bytes": committed_bytes,
                            "quarantined_tail_bytes": max(
                                0,
                                actual_bytes - committed_bytes,
                            ),
                            "previous_writer_unsealed": writer_open,
                            "pending_candidate_present": pending_present,
                        },
                    )
                except Exception:
                    for path in (safe_path, safe_watermark, quarantine_path):
                        try:
                            os.unlink(self._native_io_path(path))
                        except OSError:
                            pass
                    raise
        except Exception:
            self._discard_bootstrap_staging(staged_path)
            raise
        return SessionBinding(
            engram_id=binding.engram_id,
            state=BindingState.MATERIALIZED,
            session_id=binding.session_id,
            session_file=safe_file,
            parent_session_file=binding.parent_session_file,
            bootstrapped=binding.bootstrapped,
        )

    def prepare_lineage(self, binding: SessionBinding) -> SessionBinding:
        """Fence a pending successor's parent JSONL before ``new_session``."""

        assert binding.parent_session_file is not None
        parent_file = normalize_session_file(binding.parent_session_file)
        with self._lock:
            watermark_path, pending_path, writer_path = self._paths(parent_file)
            watermark = self._read_record(watermark_path)
            if watermark is None:
                pending = self._read_record(pending_path)
                writer = self._read_record(writer_path)
                if pending is not None or (
                    writer is not None and writer.get("state") == "open"
                ):
                    raise _PiContinuityFailure(
                        "pi_lineage_continuity_uncommitted",
                        "the pending successor parent has no committed continuity watermark",
                        "reconcile the predecessor before reconstructing its successor",
                    )
                # Legacy pending bindings predate this sidecar and do not carry
                # the parent's session ID.  Preserve their existing behavior;
                # every newly produced pending binding has a watermark.
                return binding
            parent_session_id = watermark.get("session_id")
            if not isinstance(parent_session_id, str) or not parent_session_id:
                raise _PiContinuityFailure(
                    "pi_continuity_metadata_invalid",
                    "the pending successor parent watermark has no session identity",
                    "repair the predecessor continuity metadata before succession",
                )
            synthetic = SessionBinding(
                engram_id=binding.engram_id,
                state=BindingState.MATERIALIZED,
                session_id=parent_session_id,
                session_file=parent_file,
                bootstrapped=True,
            )
            safe = self.prepare_resume(synthetic)
            if safe.session_file == parent_file:
                return binding
            assert safe.session_file is not None
            return SessionBinding(
                engram_id=binding.engram_id,
                state=BindingState.PENDING_LINEAGE,
                parent_session_file=safe.session_file,
                bootstrapped=False,
            )

    def _copy_canonical_prefix(self, source: Path, length: int) -> Path:
        # A session JSONL may be large because Pi compaction is disabled.  Do
        # not hold Runtime's bootstrap gate across the whole copy: each output
        # mutation is individually guarded so publication revocation can win
        # between chunks and fleet close can continue broadcasting signals.
        with self._bootstrap_transaction():
            os.makedirs(
                self._native_io_path(self._session_root),
                exist_ok=True,
            )
            descriptor, temporary = tempfile.mkstemp(
                prefix="pulse-resume-",
                suffix=".staging",
                dir=self._native_io_path(self._session_root),
            )
        target = Path(temporary)
        remaining = length
        try:
            with os.fdopen(
                descriptor,
                "wb",
                buffering=0,
            ) as destination, open(
                self._native_io_path(source),
                "rb",
            ) as origin:
                while remaining > 0:
                    chunk = origin.read(min(1024 * 1024, remaining))
                    if not chunk:
                        raise _PiContinuityFailure(
                            "pi_continuity_truncated",
                            "the Pi JSONL changed while its committed prefix was isolated",
                            "retry only after the stale Pi writer has been contained",
                        )
                    offset = 0
                    while offset < len(chunk):
                        with self._bootstrap_transaction():
                            written = destination.write(chunk[offset:])
                        if not written:
                            raise OSError(
                                "Pi canonical staging write made no progress"
                            )
                        offset += written
                    remaining -= len(chunk)
                with self._bootstrap_transaction():
                    os.fsync(destination.fileno())
            return target
        except Exception:
            self._discard_bootstrap_staging(target)
            raise

    def _discard_bootstrap_staging(self, target: Path) -> None:
        try:
            with self._bootstrap_transaction():
                try:
                    os.unlink(self._native_io_path(target))
                except FileNotFoundError:
                    pass
        except (OSError, _PiContinuityFailure):
            # After revocation an unbound ``.staging`` file is intentionally
            # left non-canonical for a later Runtime recovery sweep.
            pass

    def mark_writer_open(self, session_file: str, session_id: str) -> None:
        with self._lock:
            self._require_session_open(
                detail="the Pi writer could not be opened after continuity revocation",
                remedy="create a new PiSession through a new Harness runtime",
            )
            _, _, writer_path = self._paths(session_file)
            with self._publication_transaction():
                self._write_record(
                    writer_path,
                    {
                        "version": self._VERSION,
                        "session_file": normalize_session_file(session_file),
                        "session_id": session_id,
                        "state": "open",
                    },
                )

    def mark_uncommitted(self, session_file: str, session_id: str) -> None:
        with self._lock:
            self._require_session_open(
                detail="the first Pi session tail could not be staged after continuity revocation",
                remedy="reconcile the accepted turn in a new Harness runtime",
            )
            _, pending_path, _ = self._paths(session_file)
            with self._publication_transaction():
                self._write_record(
                    pending_path,
                    {
                        "version": self._VERSION,
                        "session_file": normalize_session_file(session_file),
                        "session_id": session_id,
                        "state": "uncommitted",
                    },
                )

    def stage_candidate(self, session_file: str, session_id: str) -> None:
        with self._lock:
            self._require_session_open(
                detail="the settled Pi tail arrived after continuity revocation",
                remedy="leave it quarantined and reconcile from the previous committed watermark",
            )
            path = Path(normalize_session_file(session_file))
            committed_bytes, committed_sha256 = self._hash_prefix(path)
            _, pending_path, _ = self._paths(session_file)
            with self._publication_transaction():
                self._write_record(
                    pending_path,
                    {
                        "version": self._VERSION,
                        "session_file": normalize_session_file(session_file),
                        "session_id": session_id,
                        "state": "candidate",
                        "committed_bytes": committed_bytes,
                        "committed_sha256": committed_sha256,
                    },
                )

    def commit_candidate(self, session_file: str, session_id: str) -> None:
        with self._lock:
            self._require_session_open(
                detail=(
                    "the terminal-projected Pi tail could not be committed "
                    "after continuity revocation"
                ),
                remedy="resume only from the prior watermark and quarantine this late tail",
            )
            watermark_path, pending_path, _ = self._paths(session_file)
            pending = self._read_record(pending_path)
            if pending is None or pending.get("state") != "candidate":
                raise _PiContinuityFailure(
                    "pi_continuity_candidate_missing",
                    "the terminal Pi tail has no prepared continuity candidate",
                    "do not report the session reusable until its committed tail is reconciled",
                )
            committed_bytes, committed_sha256 = self._validate_watermark(
                pending,
                session_file=session_file,
                session_id=session_id,
            )
            observed_bytes, observed_sha256 = self._hash_prefix(
                Path(normalize_session_file(session_file)), committed_bytes
            )
            if (
                observed_bytes != committed_bytes
                or observed_sha256 != committed_sha256
            ):
                raise _PiContinuityFailure(
                    "pi_continuity_candidate_changed",
                    "the Pi JSONL changed before its terminal candidate could be committed",
                    "quarantine the tail and resume from the prior committed watermark",
                )
            with self._publication_transaction():
                self._write_record(
                    watermark_path,
                    self._watermark_record(
                        session_file=session_file,
                        session_id=session_id,
                        committed_bytes=committed_bytes,
                        committed_sha256=committed_sha256,
                    ),
                )
                self._last_committed_bytes = committed_bytes
                try:
                    os.unlink(self._native_io_path(pending_path))
                except FileNotFoundError:
                    pass

    def revoke_publication(self) -> None:
        # Signal dispatch must never queue behind a long JSONL hash/copy.  Each
        # guarded mutation rechecks this event after entering its typed Runtime
        # transaction, so operations already admitted may finish but no later
        # continuity transaction can begin.
        self._publication_revoked.set()

    def seal_writer_if_joined(
        self,
        session_file: str | None,
        session_id: str | None,
        *,
        owner_joined: bool,
        recovery_permit: RuntimeRecoveryPermit | None = None,
        deadline: float | None = None,
    ) -> bool:
        if (
            recovery_permit is not None
            and type(recovery_permit) is not RuntimeRecoveryPermit
        ):
            raise TypeError("recovery_permit must be a RuntimeRecoveryPermit or None")
        if not owner_joined or not session_file or not session_id:
            return False
        if deadline is None:
            acquired = self._lock.acquire()
        else:
            acquired = False
            while not acquired:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                acquired = self._lock.acquire(timeout=remaining)
        if not acquired:
            return False
        try:
            if deadline is not None and time.monotonic() >= deadline:
                return False
            _, _, writer_path = self._paths(session_file)
            with self._seal_transaction(recovery_permit):
                self._write_record(
                    writer_path,
                    {
                        "version": self._VERSION,
                        "session_file": normalize_session_file(session_file),
                        "session_id": session_id,
                        "state": "closed",
                    },
                )
            return True
        except (OSError, _PiContinuityFailure):
            return False
        finally:
            self._lock.release()

    def snapshot(self) -> dict[str, int | bool]:
        with self._lock:
            return {
                "publication_revoked": self._publication_revoked.is_set(),
                "committed_bytes": self._last_committed_bytes,
                "quarantined_tail_bytes": self._last_quarantined_tail_bytes,
            }


class PiSession:
    """One live Pi RPC process owned by exactly one Engram."""

    def __init__(
        self,
        engram_id: str,
        workspace: str | os.PathLike[str],
        *,
        binding: SessionBinding | None = None,
        backend: PiBackend | None = None,
        process_context: PiProcessContext | None = None,
        executable: str = "pi",
        provider: str | None = None,
        model: str | None = None,
        env: dict[str, str] | None = None,
        transport_factory: Any = None,
        binding_sink: _BindingSink | None = None,
        metrics_callback: MetricsCallback | None = None,
        event_callback: HarnessEventCallback | None = None,
        handshake_timeout_sec: float = 30.0,
        sideband_timeout_sec: float = 30.0,
        abort_timeout_sec: float = 5.0,
        max_trace_events: int = 500,
        publication_permit: RuntimePublicationPermit | None = None,
        bootstrap_permit: RuntimeBootstrapPermit | None = None,
    ) -> None:
        if not isinstance(engram_id, str) or not engram_id.strip():
            raise ValueError("engram_id must be a non-empty string")
        if binding is not None and binding.engram_id != engram_id:
            raise ValueError("binding owner does not match PiSession Engram")
        self._engram_id = engram_id
        self._workspace = Path(workspace).expanduser().resolve()
        self._binding = binding
        base_backend = backend or PiBackend(
            executable=executable,
            workdir=self._workspace,
            provider=provider,
            model=model,
            env=env,
            transport_factory=transport_factory,
        )
        self._evidence_class = (
            "FAKE_RPC_CONTRACT"
            if getattr(base_backend, "_transport_factory", None) is not None
            else "LIVE_PI_PROVIDER"
        )
        if process_context is not None and not isinstance(
            process_context, PiProcessContext
        ):
            raise TypeError("process_context must be a PiProcessContext or None")
        self._process_context = process_context
        self._context_revoked = False
        self._backend = (
            base_backend.for_session(
                extra_args=process_context.extra_args,
                env=process_context.env,
            )
            if process_context is not None
            else base_backend
        )
        self._binding_sink = binding_sink
        self._metrics_callback = metrics_callback
        self._event_callback = event_callback
        self._handshake_timeout = handshake_timeout_sec
        self._sideband_timeout = sideband_timeout_sec
        self._abort_timeout = abort_timeout_sec
        self._max_trace_events = max_trace_events
        self._state = HarnessState.STARTING
        self._state_lock = threading.RLock()
        self._sideband_ready_event = threading.Event()
        self._turn_lock = threading.Lock()
        self._recovery_seal_lock = threading.Lock()
        self._channel: PiRpcChannel | None = None
        self._closing_channel: PiRpcChannel | None = None
        self._last_rpc_close_summary: PiRpcCloseSummary | None = None
        self._close_reason: str | None = None
        self._close_summary: PiSessionCloseSummary | None = None
        self._continuity = _PiContinuityGuard(
            self._workspace,
            publication_permit=publication_permit,
            bootstrap_permit=bootstrap_permit,
        )
        self._session_id: str | None = None
        self._session_file: str | None = None
        self._closed_emitted = False
        self._active_turn_id: str | None = None
        self._terminal_event_emitted = False

    @property
    def engram_id(self) -> str:
        with self._state_lock:
            return self._engram_id

    @property
    def state(self) -> HarnessState:
        with self._state_lock:
            return self._state

    @property
    def binding(self) -> SessionBinding | None:
        with self._state_lock:
            return self._binding

    def wait_sideband_ready(self, timeout_sec: float | None = None) -> bool:
        """Wait for the current turn's durable sideband-ready acknowledgement."""

        if timeout_sec is not None and timeout_sec < 0:
            raise ValueError("timeout_sec must be non-negative or None")
        return self._sideband_ready_event.wait(timeout=timeout_sec)

    def start(self) -> None:
        """Spawn, restore or reconstruct lineage, then verify compaction off."""

        with self._turn_lock:
            with self._state_lock:
                if self._state is HarnessState.CLOSED:
                    raise self._error(
                        "harness_closed",
                        "the Pi session has already been closed",
                        "create a new PiSession through PiHarnessRuntime",
                        phase="startup",
                    )
                if self._state in (HarnessState.READY, HarnessState.RUNNING):
                    return
                self._state = HarnessState.STARTING

            binding = self._binding
            if binding is not None and binding.state is BindingState.MATERIALIZED:
                assert binding.session_file is not None
                if not _PiContinuityGuard._native_is_file(binding.session_file):
                    self._set_state(HarnessState.BROKEN)
                    raise self._error(
                        "pi_session_resume_failed",
                        f"materialized Pi session file {binding.session_file!r} does not exist",
                        "restore or explicitly migrate the persisted Pi binding; no new session was created",
                        phase="resume",
                    )
            if binding is not None and binding.state is BindingState.PENDING_LINEAGE:
                assert binding.parent_session_file is not None
                if not _PiContinuityGuard._native_is_file(
                    binding.parent_session_file
                ):
                    self._set_state(HarnessState.BROKEN)
                    raise self._error(
                        "pi_session_resume_failed",
                        f"pending Pi lineage parent {binding.parent_session_file!r} does not exist",
                        "restore the parent session file before reconstructing the successor",
                        phase="resume",
                    )

            try:
                if binding is not None and binding.state is BindingState.MATERIALIZED:
                    binding = self._prepare_materialized_continuity(binding)
                elif binding is not None:
                    binding = self._prepare_lineage_continuity(binding)
                self._publish_pi_settings()
                # Defer production pipe readers until this session has
                # published an exact closing channel.  From the contained
                # process spawn onward, every Thread.start failure therefore
                # has a retained owner cell and can be reobserved by the fleet.
                transport = self._backend.open_transport(
                    defer_reader_start=True,
                )
                channel = PiRpcChannel(
                    transport,
                    id_prefix=f"pulse-{self._engram_id}",
                    autostart=False,
                )
                close_opened_channel = False
                with self._state_lock:
                    if self._state is HarnessState.CLOSED:
                        # Runtime close may win while open_transport is in
                        # flight.  Attach the just-spawned exact channel to the
                        # retained owner cell before closing it; otherwise the
                        # fleet could have reported an empty physical census.
                        if self._closing_channel is None:
                            self._closing_channel = channel
                        close_opened_channel = True
                    else:
                        self._channel = channel
                if close_opened_channel:
                    channel.begin_close()
                    observed = channel.finish_close(timeout_sec=2.25)
                    with self._state_lock:
                        self._last_rpc_close_summary = observed
                        if self._rpc_close_is_final(observed):
                            if self._closing_channel is channel:
                                self._closing_channel = None
                    raise self._error(
                        "harness_closed",
                        "the Pi session was closed while its transport was opening",
                        "create a new PiSession through a new Harness runtime",
                        phase="startup",
                    )
                start_transport_readers = getattr(
                    transport,
                    "start_readers",
                    None,
                )
                if callable(start_transport_readers):
                    start_transport_readers()
                channel.start_reader()
                state = self._get_state(
                    phase="handshake",
                    refusal_code="pi_handshake_refused",
                )

                resumed = binding is not None
                if binding is not None and binding.state is BindingState.MATERIALIZED:
                    assert binding.session_file is not None
                    reply = self._command(
                        {
                            "type": "switch_session",
                            "sessionPath": binding.session_file,
                        },
                        phase="resume",
                        refusal_code="pi_session_resume_failed",
                        timeout=self._handshake_timeout,
                    )
                    self._require_not_cancelled(reply, command="switch_session")
                    state = self._get_state(
                        phase="resume",
                        refusal_code="pi_session_resume_failed",
                    )
                    self._verify_materialized_identity(binding, state)
                elif binding is not None:
                    assert binding.parent_session_file is not None
                    reply = self._command(
                        {
                            "type": "new_session",
                            "parentSession": binding.parent_session_file,
                        },
                        phase="lineage",
                        refusal_code="pi_lineage_resume_failed",
                        timeout=self._handshake_timeout,
                    )
                    self._require_not_cancelled(reply, command="new_session")
                    state = self._get_state(
                        phase="lineage",
                        refusal_code="pi_lineage_resume_failed",
                    )
                    if state["session_file"] == binding.parent_session_file:
                        raise self._error(
                            "pi_lineage_resume_failed",
                            "Pi new_session reused the parent session file instead of creating a successor handle",
                            "upgrade or repair Pi before retrying the pending lineage",
                            phase="lineage",
                        )

                state = self._disable_and_verify_compaction()
                if binding is not None and binding.state is BindingState.MATERIALIZED:
                    self._verify_materialized_identity(binding, state)
                self._session_id = state["session_id"]
                self._session_file = state["session_file"]
                self._mark_continuity_writer_open(
                    self._session_file,
                    self._session_id,
                )
                assert self._channel is not None
                self._channel.drain_events()
                self._set_state(HarnessState.READY)
                self._emit_metric(
                    "harness_session_ready",
                    engram=self._engram_id,
                    resumed=resumed,
                    session_id=self._session_id,
                )
            except BackendError as exc:
                self._break_and_close()
                raise self._error(
                    exc.code,
                    exc.detail,
                    exc.remedy,
                    phase="preflight",
                    retryable=True,
                ) from exc
            except HarnessError:
                self._break_and_close()
                raise
            except RpcTimeout as exc:
                self._break_and_close()
                raise self._error(
                    "pi_startup_timeout",
                    "Pi did not answer a startup or recovery RPC within the handshake timeout",
                    "check the Pi process and provider configuration, then retry startup",
                    phase="handshake",
                    retryable=True,
                ) from exc
            except (RpcConnectionLost, RpcProtocolError) as exc:
                self._break_and_close()
                raise self._error(
                    "pi_startup_failed",
                    f"Pi RPC failed during startup or recovery: {exc}",
                    "run the configured Pi executable with `--mode rpc` and inspect its startup diagnostics; "
                    + PI_INSTALL_HINT,
                    phase="handshake",
                    retryable=True,
                ) from exc

    def _get_state(
        self,
        *,
        phase: str,
        refusal_code: str,
        deadline: float | None = None,
    ) -> dict[str, Any]:
        reply = self._command(
            {"type": "get_state"},
            phase=phase,
            refusal_code=refusal_code,
            timeout=None if deadline is not None else self._handshake_timeout,
            deadline=deadline,
        )
        data = reply.get("data")
        if not isinstance(data, Mapping):
            raise self._error(
                "pi_session_state_invalid",
                "Pi get_state returned no state object",
                "upgrade or repair the installed Pi RPC implementation",
                phase=phase,
            )
        session_id = data.get("sessionId")
        session_file = data.get("sessionFile")
        if not isinstance(session_id, str) or not session_id:
            raise self._error(
                "pi_session_state_invalid",
                "Pi get_state returned no non-empty sessionId",
                "enable Pi session persistence and retry",
                phase=phase,
            )
        if not isinstance(session_file, str) or not session_file.strip():
            raise self._error(
                "pi_session_state_invalid",
                "Pi get_state returned no sessionFile handle",
                "enable Pi session persistence and retry",
                phase=phase,
            )
        normalized = normalize_session_file(session_file)
        return {
            "session_id": session_id,
            "session_file": normalized,
            "auto_compaction_enabled": data.get("autoCompactionEnabled"),
            "is_streaming": data.get("isStreaming"),
            "pending_message_count": data.get("pendingMessageCount"),
        }

    def _disable_and_verify_compaction(self) -> dict[str, Any]:
        self._command(
            {"type": "set_auto_compaction", "enabled": False},
            phase="compaction",
            refusal_code="pi_compaction_control_failed",
            timeout=self._handshake_timeout,
        )
        state = self._get_state(
            phase="compaction",
            refusal_code="pi_compaction_control_failed",
        )
        if state["auto_compaction_enabled"] is not False:
            raise self._error(
                "pi_compaction_enabled",
                "Pi still reports autoCompactionEnabled=true after it was disabled",
                "use a Pi version that supports set_auto_compaction and do not send a prompt until fixed",
                phase="compaction",
            )
        return state

    def _verify_materialized_identity(
        self,
        binding: SessionBinding,
        state: Mapping[str, Any],
    ) -> None:
        assert binding.session_file is not None
        if (
            state.get("session_file") != binding.session_file
            or state.get("session_id") != binding.session_id
        ):
            raise self._error(
                "pi_session_resume_failed",
                "Pi switch_session did not restore the persisted session path and session ID exactly",
                "do not continue with the silently replaced session; explicitly repair or migrate the binding",
                phase="resume",
            )

    def _prepare_materialized_continuity(
        self,
        binding: SessionBinding,
    ) -> SessionBinding:
        try:
            safe = self._continuity.prepare_resume(binding)
            if safe.session_file != binding.session_file:
                self._commit_binding(safe)
            assert safe.session_file is not None
            assert safe.session_id is not None
            self._continuity.mark_writer_open(safe.session_file, safe.session_id)
            return safe
        except HarnessError:
            raise
        except (_PiContinuityFailure, OSError) as exc:
            if isinstance(exc, _PiContinuityFailure):
                code, detail, remedy = exc.code, exc.detail, exc.remedy
            else:
                code = "pi_continuity_unwritable"
                detail = f"Pi continuity metadata could not be updated: {exc}"
                remedy = "grant write access to the runtime-owned .pulse Harness directory"
            raise self._error(
                code,
                detail,
                remedy,
                phase="continuity",
            ) from exc

    def _prepare_lineage_continuity(
        self,
        binding: SessionBinding,
    ) -> SessionBinding:
        try:
            safe = self._continuity.prepare_lineage(binding)
            if safe.parent_session_file != binding.parent_session_file:
                self._commit_binding(safe)
            return safe
        except HarnessError:
            raise
        except (_PiContinuityFailure, OSError) as exc:
            if isinstance(exc, _PiContinuityFailure):
                code, detail, remedy = exc.code, exc.detail, exc.remedy
            else:
                code = "pi_continuity_unwritable"
                detail = f"Pi lineage continuity metadata could not be updated: {exc}"
                remedy = "grant write access to the runtime-owned .pulse Harness directory"
            raise self._error(
                code,
                detail,
                remedy,
                phase="continuity",
            ) from exc

    def _mark_continuity_writer_open(
        self,
        session_file: str,
        session_id: str,
    ) -> None:
        try:
            self._continuity.mark_writer_open(session_file, session_id)
        except (_PiContinuityFailure, OSError) as exc:
            if isinstance(exc, _PiContinuityFailure):
                code, detail, remedy = exc.code, exc.detail, exc.remedy
            else:
                code = "pi_continuity_unwritable"
                detail = f"Pi writer continuity metadata could not be updated: {exc}"
                remedy = "grant write access to the runtime-owned .pulse Harness directory"
            raise self._error(
                code,
                detail,
                remedy,
                phase="continuity",
            ) from exc

    def _publish_pi_settings(self) -> None:
        try:
            self._continuity.publish_settings()
        except _PiContinuityFailure as exc:
            raise self._error(
                exc.code,
                exc.detail,
                exc.remedy,
                phase="continuity",
            ) from exc

    def _require_not_cancelled(self, reply: Mapping[str, Any], *, command: str) -> None:
        data = reply.get("data")
        if not isinstance(data, Mapping) or type(data.get("cancelled")) is not bool:
            raise self._error(
                "pi_rpc_protocol_error",
                f"Pi {command} response did not contain a boolean cancelled field",
                "upgrade or repair the installed Pi RPC implementation",
                phase="rpc",
            )
        if data["cancelled"]:
            raise self._error(
                f"pi_{command}_cancelled",
                f"Pi cancelled {command} before changing the live session",
                "resolve the Pi lifecycle cancellation and retry explicitly",
                phase="rpc",
            )

    def _command(
        self,
        body: dict[str, Any],
        *,
        phase: str,
        refusal_code: str,
        timeout: float | None = None,
        deadline: float | None = None,
        retryable: bool = False,
        prompt_accepted: bool | None = False,
        partial_output: str = "",
        trace: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        channel = self._channel
        if channel is None:
            raise RpcConnectionLost("Pi RPC channel has not been opened")
        reply = channel.request(body, timeout=timeout, deadline=deadline)
        if not reply.get("success"):
            raise self._error(
                refusal_code,
                f"Pi refused {body['type']!r}: {reply.get('error')!r}",
                f"check that the installed Pi supports {body['type']!r} and retry only after correcting the refusal",
                phase=phase,
                retryable=retryable,
                prompt_accepted=prompt_accepted,
                partial_output=partial_output,
                trace=trace,
            )
        return reply

    def run_turn(
        self,
        prompt: str,
        *,
        timeout_sec: float | None = None,
        bootstrap_text: str | None = None,
        turn_id: str | None = None,
    ) -> HarnessTurnResult:
        return self._run_turn_for(
            self.engram_id,
            prompt,
            timeout_sec=timeout_sec,
            bootstrap_text=bootstrap_text,
            turn_id=turn_id,
        )

    def _run_turn_for(
        self,
        expected_engram_id: str,
        prompt: str,
        *,
        timeout_sec: float | None,
        bootstrap_text: str | None,
        turn_id: str | None = None,
    ) -> HarnessTurnResult:
        if not isinstance(prompt, str):
            raise self._error(
                "harness_input_invalid",
                "prompt must be natural text, not a structured object",
                "pass a Python string, including an exact empty string for a spontaneous pulse",
                phase="input",
            )
        if bootstrap_text is not None and not isinstance(bootstrap_text, str):
            raise self._error(
                "harness_input_invalid",
                "bootstrap_text must be natural text or None",
                "pass a Python string or None",
                phase="input",
            )
        try:
            deadline = _deadline(timeout_sec)
        except ValueError as exc:
            raise self._error(
                "harness_input_invalid",
                str(exc),
                "use a non-negative timeout in seconds or None",
                phase="input",
            ) from exc

        if self.state is HarnessState.STARTING and self._channel is None:
            self.start()

        remaining = _remaining(deadline)
        if remaining is None:
            acquired = self._turn_lock.acquire()
        else:
            acquired = remaining > 0 and self._turn_lock.acquire(timeout=remaining)
        if not acquired:
            raise self._error(
                "pi_timeout",
                "the turn deadline elapsed while waiting for the same Engram's active turn",
                "increase timeout_sec or avoid overlapping pulses for the same Engram",
                phase="queue",
                retryable=True,
            )
        try:
            return self._run_locked(
                expected_engram_id,
                prompt,
                bootstrap_text=bootstrap_text,
                deadline=deadline,
                timeout_sec=timeout_sec,
                turn_id=turn_id,
            )
        finally:
            self._turn_lock.release()

    def _run_locked(
        self,
        expected_engram_id: str,
        prompt: str,
        *,
        bootstrap_text: str | None,
        deadline: float | None,
        timeout_sec: float | None,
        turn_id: str | None,
    ) -> HarnessTurnResult:
        self._active_turn_id = turn_id
        self._terminal_event_emitted = False
        with self._state_lock:
            if self._engram_id != expected_engram_id:
                raise self._error(
                    "pi_session_owner_changed",
                    f"the live Pi process now belongs to Engram {self._engram_id!r}, not {expected_engram_id!r}",
                    "refresh the Harness registry after succession",
                    phase="registry",
                )
            state = self._state
        if state is HarnessState.CLOSED:
            raise self._error(
                "harness_closed",
                "the Pi session has been closed",
                "create a new runtime or session before running another turn",
                phase="turn",
            )
        if state is HarnessState.BROKEN:
            raise self._error(
                "pi_session_broken",
                "the Pi session is broken and cannot accept another prompt",
                "let PiHarnessRuntime recover it from the persisted binding on an explicit next call",
                phase="turn",
                retryable=True,
            )
        if state is not HarnessState.READY:
            raise self._error(
                "pi_session_not_ready",
                f"the Pi session is {state.value}, not READY",
                "wait for the current lifecycle operation to finish",
                phase="turn",
            )

        binding = self._binding
        bootstrapped = bool(
            getattr(self, "_bootstrap_accepted", False)
            or (
                binding is not None
                and binding.state is BindingState.MATERIALIZED
                and binding.bootstrapped
            )
        )
        parts = []
        if not bootstrapped:
            if bootstrap_text not in (None, ""):
                parts.append(bootstrap_text)
            if prompt != "":
                parts.append(prompt)
            actual_prompt = "\n\n".join(parts)
        else:
            actual_prompt = prompt

        channel = self._channel
        if channel is None:
            self._break_and_close()
            raise self._error(
                "pi_connection_lost",
                "the READY Pi session has no RPC channel",
                "restart the session from its persisted binding",
                phase="prompt",
                retryable=True,
            )
        channel.drain_events()
        trace = _TraceBuffer(self._max_trace_events)
        observation = _TurnObservation()

        self._set_state(HarnessState.ADMITTING)
        try:
            reply = channel.request(
                {"type": "prompt", "message": actual_prompt},
                deadline=deadline,
            )
        except RpcTimeout as exc:
            self._break_and_close()
            raise self._error(
                "pi_prompt_ack_timeout",
                "Pi did not acknowledge prompt preflight before the turn deadline; the sent prompt may already have been accepted",
                "reconcile the session before another explicit pulse; do not automatically repeat an ambiguously accepted action",
                phase="prompt",
                retryable=False,
                prompt_accepted=None,
                trace=trace.finish(),
            ) from exc
        except (RpcConnectionLost, RpcProtocolError) as exc:
            self._break_and_close()
            raise self._error(
                "pi_connection_lost",
                f"Pi RPC ended before a prompt acknowledgement arrived: {exc}",
                "the prompt was sent and may have been accepted; reconcile before another explicit pulse",
                phase="prompt",
                retryable=False,
                prompt_accepted=None,
                trace=trace.finish(),
            ) from exc

        if not reply.get("success"):
            raise self._error(
                "pi_prompt_refused",
                f"Pi refused prompt preflight: {reply.get('error')!r}",
                "correct the Pi model, credential, or RPC compatibility error before retrying",
                phase="prompt",
                retryable=True,
                trace=trace.finish(),
            )

        correlation_id = reply.get("id")
        self._set_state(HarnessState.RUNNING)
        self._bootstrap_accepted = True
        if (
            binding is not None
            and binding.state is BindingState.MATERIALIZED
            and not binding.bootstrapped
        ):
            assert binding.session_id is not None
            assert binding.session_file is not None
            updated = SessionBinding(
                engram_id=binding.engram_id,
                state=BindingState.MATERIALIZED,
                session_id=binding.session_id,
                session_file=binding.session_file,
                parent_session_file=binding.parent_session_file,
                bootstrapped=True,
            )
            try:
                self._commit_binding(updated)
                binding = updated
            except HarnessError:
                self._break_and_close()
                raise
            except Exception as exc:
                self._break_and_close()
                raise self._error(
                    "pi_binding_persist_failed",
                    f"prompt was accepted but the bootstrapped binding could not be persisted: {exc}",
                    "repair binding persistence and reconcile the uncertain accepted turn before retrying",
                    phase="binding",
                    prompt_accepted=True,
                ) from exc

        self._emit_metric(
            "harness_turn_started",
            engram=self._engram_id,
            correlation_id=correlation_id,
            bootstrap=not bootstrapped,
        )
        sideband_projected = self._emit_event(
            turn_id,
            {
                "type": "turn_started",
                "correlation_id": correlation_id,
                "bootstrap": not bootstrapped,
                # This durable observation is the public sideband-ready
                # acknowledgement.  A causal turn can already be RUNNING
                # while Pi is still waiting to acknowledge the prompt, so
                # callers must not infer steer readiness from transport write
                # visibility or from the causal turn state alone.
                "sideband_ready": True,
            },
        )
        if sideband_projected:
            with self._state_lock:
                if self._state is HarnessState.RUNNING:
                    self._sideband_ready_event.set()

        try:
            while True:
                event = channel.read_event(deadline=deadline)
                trace.add(event)
                self._emit_event(turn_id, event)
                observation.absorb(event)
                if event.get("type") == "agent_settled":
                    break
        except RpcTimeout as exc:
            raise self._timeout_after_acceptance(
                timeout_sec,
                trace,
                observation,
                correlation_id=correlation_id,
                turn_id=turn_id,
            ) from exc
        except (RpcConnectionLost, RpcProtocolError) as exc:
            self._break_and_close()
            raise self._error(
                "pi_connection_lost",
                f"Pi RPC ended after prompt acceptance but before settlement: {exc}",
                "reconcile the uncertain Pi session before any explicit retry",
                phase="turn",
                prompt_accepted=True,
                partial_output=_text_of(observation.last_assistant),
                trace=trace.finish(),
            ) from exc

        self._set_state(HarnessState.SETTLING)

        if observation.last_assistant is None:
            raise self._error(
                "pi_missing_assistant_event",
                "Pi settled without a current-turn assistant message_end or turn_end",
                "inspect the Pi event stream; do not reuse a previous turn's assistant text",
                phase="settle",
                prompt_accepted=True,
                trace=trace.finish(),
            )

        # Settlement consumes the caller's turn budget.  Persistence
        # reconciliation is a distinct lifecycle phase and therefore gets a
        # fresh, bounded deadline; reusing the exhausted turn deadline could
        # report READY before the accepted turn has a durable binding.
        finalize_deadline = time.monotonic() + self._sideband_timeout
        try:
            materialized = self._materialize_after_turn(deadline=finalize_deadline)
            final_reply = self._command(
                {"type": "get_last_assistant_text"},
                phase="finalize",
                refusal_code="pi_final_text_failed",
                deadline=finalize_deadline,
                prompt_accepted=True,
                partial_output=_text_of(observation.last_assistant),
                trace=trace.finish(),
            )
        except RpcTimeout as exc:
            self._break_and_close()
            raise self._error(
                "pi_timeout",
                "the turn settled but durable finalization exceeded its reconciliation deadline",
                "reconcile the accepted Pi session before explicitly continuing; this process is not reusable",
                phase="finalize",
                prompt_accepted=True,
                partial_output=_text_of(observation.last_assistant),
                trace=trace.finish(),
            ) from exc
        except (RpcConnectionLost, RpcProtocolError) as exc:
            self._break_and_close()
            raise self._error(
                "pi_connection_lost",
                f"Pi RPC ended after settlement during finalization: {exc}",
                "recover from the now-materialized binding and inspect the completed turn before continuing",
                phase="finalize",
                prompt_accepted=True,
                partial_output=_text_of(observation.last_assistant),
                trace=trace.finish(),
            ) from exc

        data = final_reply.get("data")
        text = data.get("text") if isinstance(data, Mapping) else None
        content = text if isinstance(text, str) else ""
        event_text = _text_of(observation.last_assistant)

        if event_text and content != event_text:
            raise self._error(
                "pi_final_text_mismatch",
                "Pi get_last_assistant_text does not match the current turn's final assistant event",
                "inspect the Pi session rather than propagating possibly stale prior-turn text",
                phase="finalize",
                prompt_accepted=True,
                partial_output=event_text,
                trace=trace.finish(),
            )

        stop_reason = observation.last_assistant.get("stopReason")
        if stop_reason == "length":
            raise self._error(
                "pi_truncated",
                "Pi's final assistant message stopped at the context limit",
                "shorten the input or perform explicit succession; truncated output is not completion",
                phase="settle",
                prompt_accepted=True,
                partial_output=content or event_text,
                trace=trace.finish(),
            )
        if stop_reason == "aborted":
            raise self._error(
                "pi_aborted",
                "the Pi turn was aborted before successful completion",
                "inspect the partial trace and issue a new explicit pulse if appropriate",
                phase="settle",
                prompt_accepted=True,
                partial_output=content or event_text,
                trace=trace.finish(),
            )
        if stop_reason == "error":
            detail = observation.last_assistant.get("errorMessage") or "provider error"
            raise self._error(
                "pi_agent_error",
                f"Pi's final assistant message reported an error: {detail}",
                "repair the provider/model failure before another explicit pulse",
                phase="settle",
                prompt_accepted=True,
                partial_output=content or event_text,
                trace=trace.finish(),
            )
        if stop_reason != "stop":
            raise self._error(
                "pi_stop_reason_invalid",
                f"Pi settled with final stopReason {stop_reason!r}, not 'stop'",
                "inspect the Pi turn; only a stop result is a completed Harness turn",
                phase="settle",
                prompt_accepted=True,
                partial_output=content or event_text,
                trace=trace.finish(),
            )
        if not content.strip():
            raise self._error(
                "pi_empty_output",
                "Pi settled without non-empty current-turn assistant text",
                "inspect the provider and Pi event trace before another pulse",
                phase="finalize",
                prompt_accepted=True,
                trace=trace.finish(),
            )

        result = HarnessTurnResult(
            engram_id=self._engram_id,
            session_id=materialized.session_id,
            session_file=materialized.session_file or "",
            content=content,
            stop_reason="stop",
            input_tokens=observation.input_tokens,
            output_tokens=observation.output_tokens,
            cached_tokens=observation.cached_tokens,
            cache_write_tokens=observation.cache_write_tokens,
            tool_calls=observation.tool_calls,
            provider_requests=observation.provider_requests,
            trace=trace.finish(),
            evidence_class=self._evidence_class,
        )
        try:
            assert materialized.session_file is not None
            assert materialized.session_id is not None
            self._continuity.stage_candidate(
                materialized.session_file,
                materialized.session_id,
            )
        except (_PiContinuityFailure, OSError) as exc:
            self._break_and_close()
            if isinstance(exc, _PiContinuityFailure):
                code, detail, remedy = exc.code, exc.detail, exc.remedy
            else:
                code = "pi_continuity_unwritable"
                detail = f"the settled Pi tail could not be staged: {exc}"
                remedy = "repair the runtime-owned continuity directory before resuming this Engram"
            raise self._error(
                code,
                detail,
                remedy,
                phase="continuity",
                prompt_accepted=True,
                partial_output=result.content,
                trace=result.trace,
            ) from exc
        terminal_projected = self._emit_event(
            turn_id,
            {
                "type": "turn_terminal",
                "status": "settled",
                "barrier": "agent_settled",
                "stop_reason": result.stop_reason,
                "input_tokens": result.input_tokens,
                "output_tokens": result.output_tokens,
                "tool_calls": result.tool_calls,
                "provider_requests": result.provider_requests,
            },
        )
        if not terminal_projected:
            self._break_and_close()
            raise self._error(
                "harness_terminal_projection_failed",
                "the accepted Pi turn settled but its canonical terminal event was not projected",
                "repair the durable event projection and reconcile this accepted turn before continuing",
                phase="terminal",
                prompt_accepted=True,
                partial_output=result.content,
                trace=result.trace,
            )
        try:
            self._continuity.commit_candidate(
                materialized.session_file,
                materialized.session_id,
            )
        except (_PiContinuityFailure, OSError) as exc:
            self._break_and_close()
            if isinstance(exc, _PiContinuityFailure):
                code, detail, remedy = exc.code, exc.detail, exc.remedy
            else:
                code = "pi_continuity_unwritable"
                detail = f"the terminal-projected Pi tail could not be committed: {exc}"
                remedy = "repair the runtime-owned continuity directory and resume only from the prior watermark"
            raise self._error(
                code,
                detail,
                remedy,
                phase="continuity",
                prompt_accepted=True,
                partial_output=result.content,
                trace=result.trace,
            ) from exc
        self._active_turn_id = None
        self._terminal_event_emitted = True
        self._set_state(HarnessState.READY)
        self._emit_metric(
            "harness_turn_settled",
            engram=self._engram_id,
            correlation_id=correlation_id,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            cached_tokens=result.cached_tokens,
            cache_write_tokens=result.cache_write_tokens,
            tool_calls=result.tool_calls,
            provider_requests=result.provider_requests,
        )
        return result

    def _materialize_after_turn(self, *, deadline: float | None) -> SessionBinding:
        binding = self._binding
        if binding is not None and binding.state is BindingState.MATERIALIZED:
            return binding

        state = self._get_state(
            phase="materialize",
            refusal_code="pi_session_materialization_failed",
            deadline=deadline,
        )
        if (
            state["session_id"] != self._session_id
            or state["session_file"] != self._session_file
        ):
            self._break_and_close()
            raise self._error(
                "pi_session_materialization_failed",
                "Pi changed the live session identity before the first real turn could be materialized",
                "inspect the Pi lifecycle; do not persist a path or ID from a different session",
                phase="materialize",
                prompt_accepted=True,
            )
        session_file = state["session_file"]
        if not _PiContinuityGuard._native_is_file(session_file):
            # A settled first turn without its promised JSONL cannot safely
            # continue: the accepted work is not recoverable after restart.
            # Keep the pending binding, close this process, and require an
            # explicit reconciliation instead of silently advancing.
            self._break_and_close()
            raise self._error(
                "pi_session_materialization_failed",
                f"Pi settled the first real turn but session file {session_file!r} does not exist",
                "inspect Pi session persistence; no materialized binding was recorded",
                phase="materialize",
                prompt_accepted=True,
            )
        materialized = SessionBinding(
            engram_id=self._engram_id,
            state=BindingState.MATERIALIZED,
            session_id=state["session_id"],
            session_file=session_file,
            parent_session_file=(
                binding.parent_session_file if binding is not None else None
            ),
            bootstrapped=True,
        )
        try:
            assert materialized.session_file is not None
            assert materialized.session_id is not None
            self._continuity.mark_uncommitted(
                materialized.session_file,
                materialized.session_id,
            )
            self._commit_binding(materialized)
        except HarnessError:
            self._break_and_close()
            raise
        except _PiContinuityFailure as exc:
            self._break_and_close()
            raise self._error(
                exc.code,
                exc.detail,
                exc.remedy,
                phase="continuity",
                prompt_accepted=True,
            ) from exc
        except Exception as exc:
            self._break_and_close()
            raise self._error(
                "pi_binding_persist_failed",
                f"the first real turn materialized but its binding callback failed: {exc}",
                "repair persistence and reconcile the completed Pi session before continuing",
                phase="binding",
                prompt_accepted=True,
            ) from exc
        return materialized

    def _timeout_after_acceptance(
        self,
        timeout_sec: float | None,
        trace: _TraceBuffer,
        observation: _TurnObservation,
        *,
        correlation_id: Any,
        turn_id: str | None = None,
    ) -> HarnessError:
        channel = self._channel
        aborted = False
        settled = False
        if channel is not None:
            try:
                reply = channel.request(
                    {"type": "abort"},
                    timeout=self._abort_timeout,
                )
                aborted = bool(reply.get("success"))
            except (RpcTimeout, RpcConnectionLost, RpcProtocolError):
                aborted = False
            if aborted:
                # An abort response only acknowledges the command.  The live
                # process is reusable after the old turn's terminal event has
                # crossed the same ordered event stream.
                settle_deadline = time.monotonic() + self._abort_timeout
                try:
                    while True:
                        event = channel.read_event(deadline=settle_deadline)
                        trace.add(event)
                        self._emit_event(turn_id, event)
                        observation.absorb(event)
                        if event.get("type") == "agent_settled":
                            settled = True
                            break
                except (RpcTimeout, RpcConnectionLost, RpcProtocolError):
                    settled = False
            else:
                for event in channel.drain_events():
                    trace.add(event)
                    self._emit_event(turn_id, event)
                    observation.absorb(event)
        if aborted and settled:
            self._set_state(HarnessState.SETTLING)
        else:
            self._break_and_close()
        outcome = (
            "abort settled"
            if aborted and settled
            else "abort was acknowledged but settlement could not be confirmed"
            if aborted
            else "abort could not be confirmed"
        )
        return self._error(
            "pi_timeout",
            f"Pi did not settle within {timeout_sec!r} seconds; {outcome}",
            "inspect the partial trace and issue a new explicit pulse only after reconciling the accepted turn",
            phase="turn",
            prompt_accepted=True,
            partial_output=_text_of(observation.last_assistant),
            trace=trace.finish(),
        )

    def snapshot(self) -> dict[str, Any]:
        with self._state_lock:
            binding = self._binding
            snapshot = {
                "engram_id": self._engram_id,
                "state": self._state.value,
                "session_id": self._session_id,
                "session_file": self._session_file,
                "binding_state": binding.state.value if binding is not None else None,
                "bootstrapped": bool(
                    getattr(self, "_bootstrap_accepted", False)
                    or (binding is not None and binding.bootstrapped)
                ),
                "sideband_ready": self._sideband_ready_event.is_set(),
            }
        snapshot["continuity"] = self._continuity.snapshot()
        return snapshot

    def abort(self) -> None:
        self._sideband_command({"type": "abort"})
        # The RPC response only acknowledges receipt.  The turn-owning thread
        # remains the sole consumer of the ordered event stream, so wait for
        # it to cross ``agent_settled`` and finish bounded finalization rather
        # than racing it by reading events here.
        deadline = time.monotonic() + self._abort_timeout
        while True:
            state = self.state
            if state is HarnessState.READY:
                return
            if state in {HarnessState.BROKEN, HarnessState.CLOSED}:
                raise self._error(
                    "pi_abort_settlement_unconfirmed",
                    "Pi acknowledged abort but the active turn did not reach a reusable settled state",
                    "reconcile the accepted turn from its durable events before continuing",
                    phase="sideband",
                    prompt_accepted=None if state is HarnessState.ADMITTING else True,
                )
            if time.monotonic() >= deadline:
                self._break_and_close()
                raise self._error(
                    "pi_abort_settle_timeout",
                    "Pi acknowledged abort but no settled turn barrier arrived before the abort deadline",
                    "reconcile the accepted turn; this Pi process was closed and is not reusable",
                    phase="sideband",
                    prompt_accepted=None if state is HarnessState.ADMITTING else True,
                )
            time.sleep(0.01)

    def steer(self, content: str) -> None:
        if not isinstance(content, str):
            raise self._error(
                "harness_input_invalid",
                "steer content must be natural text",
                "pass a Python string",
                phase="input",
                prompt_accepted=self.state is HarnessState.RUNNING,
                project_turn_terminal=False,
            )
        self._sideband_command({"type": "steer", "message": content})

    def _sideband_command(self, body: dict[str, Any]) -> None:
        command = body["type"]
        with self._state_lock:
            state = self._state
            channel = self._channel
            sideband_ready = self._sideband_ready_event.is_set()
        if command == "steer" and (
            state is HarnessState.ADMITTING
            or (state is HarnessState.RUNNING and not sideband_ready)
        ):
            raise self._error(
                "pi_steer_admission_pending",
                f"cannot steer Pi session {self._engram_id!r} before prompt admission is durable",
                "wait for the durable turn_started event with sideband_ready=true, then issue a new steer request",
                phase="sideband",
                retryable=True,
                prompt_accepted=None,
                project_turn_terminal=False,
            )
        allowed = (
            state is HarnessState.RUNNING
            or (command == "abort" and state is HarnessState.ADMITTING)
        )
        if not allowed or channel is None:
            raise self._error(
                "pi_session_not_running",
                f"cannot {command} Pi session {self._engram_id!r} while it is {state.value}",
                "start a real turn first; sideband operations never start a session implicitly",
                phase="sideband",
                project_turn_terminal=False,
            )
        try:
            reply = channel.request(body, timeout=self._sideband_timeout)
        except RpcTimeout as exc:
            if command == "abort":
                self._break_and_close()
            raise self._error(
                f"pi_{command}_timeout",
                f"Pi did not acknowledge {command} within the sideband timeout",
                "reconcile the running turn before another lifecycle operation",
                phase="sideband",
                prompt_accepted=None if state is HarnessState.ADMITTING else True,
                project_turn_terminal=command == "abort",
            ) from exc
        except (RpcConnectionLost, RpcProtocolError) as exc:
            self._break_and_close()
            raise self._error(
                "pi_connection_lost",
                f"Pi RPC ended while sending {command}: {exc}",
                "reconcile the accepted turn before restarting the session",
                phase="sideband",
                prompt_accepted=None if state is HarnessState.ADMITTING else True,
            ) from exc
        if not reply.get("success"):
            raise self._error(
                f"pi_{command}_refused",
                f"Pi refused {command}: {reply.get('error')!r}",
                "inspect the current Pi state and RPC compatibility",
                phase="sideband",
                prompt_accepted=None if state is HarnessState.ADMITTING else True,
                project_turn_terminal=False,
            )

    def _rotate_to(
        self,
        expected_old_engram_id: str,
        new_engram_id: str,
        commit: _RotationCommit,
    ) -> None:
        with self._turn_lock:
            with self._state_lock:
                if self._engram_id != expected_old_engram_id:
                    raise self._error(
                        "pi_session_owner_changed",
                        "the requested predecessor no longer owns this Pi process",
                        "refresh the Harness registry before succession",
                        phase="succession",
                    )
                if self._state is not HarnessState.READY:
                    raise self._error(
                        "pi_session_not_ready",
                        f"succession requires READY, not {self._state.value}",
                        "wait for the current turn or lifecycle operation to finish",
                        phase="succession",
                    )
                old_binding = self._binding
                if (
                    old_binding is None
                    or old_binding.state is not BindingState.MATERIALIZED
                ):
                    raise self._error(
                        "pi_succession_requires_materialized",
                        "succession requires a materialized predecessor session",
                        "complete the predecessor's first real turn before succession",
                        phase="succession",
                    )
                self._state = HarnessState.ROTATING

            assert old_binding.session_file is not None
            try:
                pending = SessionBinding(
                    engram_id=new_engram_id,
                    state=BindingState.PENDING_LINEAGE,
                    session_id=None,
                    session_file=None,
                    parent_session_file=old_binding.session_file,
                    bootstrapped=False,
                )
                commit(self, old_binding, pending)
            except Exception as exc:
                with self._state_lock:
                    still_live = self._state not in (
                        HarnessState.BROKEN,
                        HarnessState.CLOSED,
                    )
                if still_live:
                    self._set_state(HarnessState.READY)
                if isinstance(exc, HarnessError):
                    raise
                raise self._error(
                    "pi_binding_persist_failed",
                    f"pending lineage could not be persisted: {exc}",
                    "the predecessor binding was preserved; repair persistence before retrying succession",
                    phase="binding",
                ) from exc

    def _rollback_rotation(self, old_binding: SessionBinding) -> bool:
        assert old_binding.session_file is not None
        try:
            reply = self._command(
                {
                    "type": "switch_session",
                    "sessionPath": old_binding.session_file,
                },
                phase="succession_rollback",
                refusal_code="pi_succession_rollback_failed",
                timeout=self._handshake_timeout,
            )
            self._require_not_cancelled(reply, command="switch_session")
            state = self._get_state(
                phase="succession_rollback",
                refusal_code="pi_succession_rollback_failed",
            )
            self._verify_materialized_identity(old_binding, state)
            state = self._disable_and_verify_compaction()
            self._verify_materialized_identity(old_binding, state)
            with self._state_lock:
                self._session_id = state["session_id"]
                self._session_file = state["session_file"]
            return True
        except (HarnessError, RpcTimeout, RpcConnectionLost, RpcProtocolError):
            return False

    def close(
        self,
        *,
        timeout_sec: float | None = None,
        recovery_permit: RuntimeRecoveryPermit | None = None,
    ) -> PiSessionCloseSummary:
        """Close once, then monotonically refresh the retained owner.

        A call begun before Runtime publication revocation may finish after the
        gate is revoked.  A later call carrying that Runtime generation's
        recovery permit may seal the continuity writer.  Repeated calls may
        wait on the same physical owner, but never signal or spawn it again.
        """

        return self._close(
            reason="explicit",
            timeout_sec=timeout_sec,
            recovery_permit=recovery_permit,
        )

    def _begin_close(self, *, reason: str) -> None:
        with self._state_lock:
            if self._session_close_is_final(self._close_summary):
                return
            self._state = HarnessState.CLOSED
            self._sideband_ready_event.clear()
            if self._close_reason is None:
                self._close_reason = reason
            channel = None
            if self._closing_channel is None:
                channel = self._channel
                self._channel = None
                self._closing_channel = channel
        self._continuity.revoke_publication()
        self._revoke_context()
        if channel is not None:
            try:
                channel.begin_close()
            except Exception as exc:
                self._emit_metric(
                    "harness_session_close_failed",
                    engram=self._engram_id,
                    reason=reason,
                    error_type=type(exc).__name__,
                )

    def _finish_close(
        self,
        *,
        deadline: float,
        recovery_permit: RuntimeRecoveryPermit | None = None,
    ) -> PiSessionCloseSummary:
        """Refresh close evidence for the same retained RPC owner."""

        if (
            recovery_permit is not None
            and type(recovery_permit) is not RuntimeRecoveryPermit
        ):
            raise TypeError("recovery_permit must be a RuntimeRecoveryPermit or None")
        with self._state_lock:
            if self._session_close_is_final(self._close_summary):
                assert self._close_summary is not None
                return dict(self._close_summary)
            channel = self._closing_channel
            rpc_summary = self._last_rpc_close_summary
            reason = self._close_reason or "explicit"

        if channel is not None:
            observed_rpc = self._close_channel(
                channel,
                reason=reason,
                deadline=deadline,
            )
            with self._state_lock:
                current_rpc = self._last_rpc_close_summary
                if current_rpc is None or (
                    observed_rpc["unresolved"] <= current_rpc["unresolved"]
                    and _PI_TREE_RANK[observed_rpc["process_tree_state"]]
                    >= _PI_TREE_RANK[current_rpc["process_tree_state"]]
                ):
                    self._last_rpc_close_summary = observed_rpc
                assert self._last_rpc_close_summary is not None
                rpc_summary = self._last_rpc_close_summary
                if self._rpc_close_is_final(rpc_summary):
                    if self._closing_channel is channel:
                        self._closing_channel = None

        if rpc_summary is None:
            rpc_summary = {
                "signal_dispatched": False,
                "signal_sent": False,
                "process_owners_observed": 0,
                "process_owners_unresolved": 0,
                "channel_reader_owners_observed": 0,
                "channel_reader_owners_unresolved": 0,
                "transport_reader_owners_observed": 0,
                "transport_reader_owners_unresolved": 0,
                "close_worker_owners_observed": 0,
                "close_worker_owners_unresolved": 0,
                "internal_owner_unresolved": 0,
                "unresolved": 0,
                "owner_joined": True,
                "process_tree_state": "not_applicable",
                "error_code": None,
            }

        # The caller-owned startup/turn thread is outside the RPC channel.
        # We do not pretend to join an arbitrary caller thread; after channel
        # shutdown it either released the lifecycle lock or remains explicit.
        lifecycle_unresolved = int(self._turn_lock.locked())
        with self._state_lock:
            session_file = self._session_file
            session_id = self._session_id
        rpc_joined = self._rpc_close_is_final(rpc_summary)
        continuity_sealed = self._continuity.seal_writer_if_joined(
            session_file,
            session_id,
            # The JSONL writer is the Pi transport/root, not the caller-owned
            # lifecycle thread.  A still-unwinding turn remains explicit in
            # owner evidence, while a joined RPC/process may safely seal this
            # path against future writes. Pending/tail markers still prevent
            # an uncommitted turn from becoming canonical.
            owner_joined=rpc_joined,
            recovery_permit=recovery_permit,
            deadline=deadline,
        )
        continuity_required = bool(session_file and session_id)
        continuity_unresolved = int(continuity_required and not continuity_sealed)
        internal_unresolved = (
            max(0, int(rpc_summary.get("internal_owner_unresolved", 0)))
            + lifecycle_unresolved
            + continuity_unresolved
        )
        process_unresolved = max(
            0, int(rpc_summary.get("process_owners_unresolved", 0))
        )
        unresolved = process_unresolved + internal_unresolved
        error_code = rpc_summary.get("error_code")
        if lifecycle_unresolved and not error_code:
            error_code = "pi_session_lifecycle_owner_unresolved"
        if continuity_unresolved and not error_code:
            error_code = "pi_continuity_writer_unsealed"
        if error_code:
            self._emit_metric(
                "harness_session_close_failed",
                engram=self._engram_id,
                reason=reason,
                error_type=error_code,
            )
        tree = rpc_summary.get("process_tree_state")
        if tree not in {
            "not_applicable",
            "empty_verified",
            "root_exit_only",
            "unknown",
        }:
            tree = "unknown"
        summary: PiSessionCloseSummary = {
            "sessions_observed": 1,
            "signal_dispatched": bool(rpc_summary.get("signal_dispatched")),
            "signal_sent": bool(rpc_summary.get("signal_sent")),
            "process_owners_observed": max(
                0, int(rpc_summary.get("process_owners_observed", 0))
            ),
            "process_owners_unresolved": process_unresolved,
            "channel_reader_owners_observed": max(
                0, int(rpc_summary.get("channel_reader_owners_observed", 0))
            ),
            "channel_reader_owners_unresolved": max(
                0, int(rpc_summary.get("channel_reader_owners_unresolved", 0))
            ),
            "transport_reader_owners_observed": max(
                0, int(rpc_summary.get("transport_reader_owners_observed", 0))
            ),
            "transport_reader_owners_unresolved": max(
                0, int(rpc_summary.get("transport_reader_owners_unresolved", 0))
            ),
            "close_worker_owners_observed": max(
                0, int(rpc_summary.get("close_worker_owners_observed", 0))
            ),
            "close_worker_owners_unresolved": max(
                0, int(rpc_summary.get("close_worker_owners_unresolved", 0))
            ),
            "lifecycle_owner_unresolved": lifecycle_unresolved,
            "internal_owner_unresolved": internal_unresolved,
            "unresolved": unresolved,
            "owner_joined": (
                rpc_joined
                and unresolved == 0
                and tree in _PI_PHYSICAL_FINAL_TREE_STATES
            ),
            "process_tree_state": tree,
            "continuity_writer_sealed": continuity_sealed or not continuity_required,
            "error_code": error_code,
        }
        with self._state_lock:
            current = self._close_summary
            if current is None:
                self._close_summary = summary
            elif (
                summary["unresolved"] <= current["unresolved"]
                and _PI_TREE_RANK[summary["process_tree_state"]]
                >= _PI_TREE_RANK[current["process_tree_state"]]
                and int(summary["continuity_writer_sealed"])
                >= int(current["continuity_writer_sealed"])
            ):
                summary["sessions_observed"] = max(
                    current["sessions_observed"],
                    summary["sessions_observed"],
                )
                summary["signal_dispatched"] = (
                    current["signal_dispatched"]
                    or summary["signal_dispatched"]
                )
                summary["signal_sent"] = (
                    current["signal_sent"] or summary["signal_sent"]
                )
                for observed_name in (
                    "process_owners_observed",
                    "channel_reader_owners_observed",
                    "transport_reader_owners_observed",
                    "close_worker_owners_observed",
                ):
                    summary[observed_name] = max(  # type: ignore[literal-required]
                        current[observed_name],  # type: ignore[literal-required]
                        summary[observed_name],  # type: ignore[literal-required]
                    )
                self._close_summary = summary
            assert self._close_summary is not None
            result = dict(self._close_summary)
        if not self._closed_emitted:
            self._closed_emitted = True
            self._emit_metric(
                "harness_session_closed",
                engram=self._engram_id,
                reason=reason,
            )
        return result

    def _recovery_reseal_cached(
        self,
        recovery_permit: RuntimeRecoveryPermit,
    ) -> PiSessionCloseSummary | None:
        """Refresh physical/lifecycle evidence and recovery-seal continuity."""

        if type(recovery_permit) is not RuntimeRecoveryPermit:
            raise TypeError("recovery_permit must be a RuntimeRecoveryPermit")
        with self._recovery_seal_lock:
            return self._finish_close(
                deadline=time.monotonic() + 2.25,
                recovery_permit=recovery_permit,
            )

    @staticmethod
    def _rpc_close_is_final(summary: PiRpcCloseSummary | None) -> bool:
        return bool(
            summary is not None
            and summary["owner_joined"]
            and summary["unresolved"] == 0
            and summary["process_tree_state"]
            in _PI_PHYSICAL_FINAL_TREE_STATES
        )

    @staticmethod
    def _session_close_is_final(summary: PiSessionCloseSummary | None) -> bool:
        return bool(
            summary is not None
            and summary["owner_joined"]
            and summary["unresolved"] == 0
            and summary["process_tree_state"]
            in _PI_PHYSICAL_FINAL_TREE_STATES
            and summary["continuity_writer_sealed"]
        )

    def _cached_close_evidence(self) -> PiSessionCloseSummary | None:
        with self._state_lock:
            return None if self._close_summary is None else dict(self._close_summary)

    def _await_close_signal(self, *, deadline: float) -> bool:
        with self._state_lock:
            channel = self._closing_channel
        if channel is None:
            return True
        return channel.await_close_signal(deadline=deadline)

    def _close(
        self,
        *,
        reason: str,
        timeout_sec: float | None = None,
        recovery_permit: RuntimeRecoveryPermit | None = None,
        deadline: float | None = None,
    ) -> PiSessionCloseSummary:
        if timeout_sec is not None and timeout_sec < 0:
            raise ValueError("timeout_sec must be non-negative or None")
        if timeout_sec is not None and deadline is not None:
            raise ValueError("timeout_sec and deadline are mutually exclusive")
        if (
            recovery_permit is not None
            and type(recovery_permit) is not RuntimeRecoveryPermit
        ):
            raise TypeError("recovery_permit must be a RuntimeRecoveryPermit or None")
        if deadline is None:
            deadline = time.monotonic() + (
                2.25 if timeout_sec is None else timeout_sec
            )
        self._begin_close(reason=reason)
        return self._finish_close(
            deadline=deadline,
            recovery_permit=recovery_permit,
        )

    def _commit_binding(self, binding: SessionBinding) -> None:
        if self._binding_sink is not None:
            self._binding_sink(binding)
        with self._state_lock:
            self._binding = binding

    def _set_state(self, state: HarnessState) -> None:
        with self._state_lock:
            if (
                self._state in (HarnessState.BROKEN, HarnessState.CLOSED)
                and state not in (HarnessState.BROKEN, HarnessState.CLOSED)
            ):
                return
            self._state = state
            if state is not HarnessState.RUNNING:
                self._sideband_ready_event.clear()

    def _break_and_close(self) -> None:
        with self._state_lock:
            if self._state is not HarnessState.CLOSED:
                self._state = HarnessState.BROKEN
            self._sideband_ready_event.clear()
            if self._closing_channel is None:
                self._closing_channel = self._channel
                self._channel = None
            channel = self._closing_channel
        self._continuity.revoke_publication()
        self._revoke_context()
        if channel is not None:
            deadline = time.monotonic() + 2.25
            try:
                channel.begin_close()
            except Exception:
                pass
            summary = self._close_channel(
                channel,
                reason="broken",
                deadline=deadline,
            )
            with self._state_lock:
                current = self._last_rpc_close_summary
                if current is None or (
                    summary["unresolved"] <= current["unresolved"]
                    and _PI_TREE_RANK[summary["process_tree_state"]]
                    >= _PI_TREE_RANK[current["process_tree_state"]]
                ):
                    self._last_rpc_close_summary = summary
                if self._rpc_close_is_final(self._last_rpc_close_summary):
                    if self._closing_channel is channel:
                        self._closing_channel = None
            self._continuity.seal_writer_if_joined(
                self._session_file,
                self._session_id,
                owner_joined=bool(summary.get("owner_joined")),
                deadline=deadline,
            )

    def _revoke_context(self) -> None:
        with self._state_lock:
            if self._context_revoked:
                return
            self._context_revoked = True
            context = self._process_context
        if context is None:
            return
        try:
            context.revoke()
        except Exception as exc:
            # Revocation is fail-closed at the Gateway boundary.  A callback
            # failure must not prevent transport shutdown or sibling cleanup.
            self._emit_metric(
                "harness_capability_revoke_failed",
                engram=self._engram_id,
                error_type=type(exc).__name__,
            )

    def _close_channel(
        self,
        channel: PiRpcChannel,
        *,
        reason: str,
        deadline: float,
    ) -> PiRpcCloseSummary:
        try:
            return channel.finish_close(deadline=deadline)
        except Exception as exc:
            # Closing one process must not prevent the registry from closing
            # its siblings.  PiRpcChannel itself still signals and joins its
            # reader in a finally block.
            self._emit_metric(
                "harness_session_close_failed",
                engram=self._engram_id,
                reason=reason,
                error_type=type(exc).__name__,
            )
            return {
                "signal_dispatched": True,
                "signal_sent": False,
                "process_owners_observed": 0,
                "process_owners_unresolved": 1,
                "channel_reader_owners_observed": 1,
                "channel_reader_owners_unresolved": 1,
                "transport_reader_owners_observed": 0,
                "transport_reader_owners_unresolved": 0,
                "close_worker_owners_observed": 1,
                "close_worker_owners_unresolved": 1,
                "internal_owner_unresolved": 2,
                "unresolved": 3,
                "owner_joined": False,
                "process_tree_state": "unknown",
                "error_code": "pi_rpc_close_failed",
            }

    def _emit_metric(self, event: str, **fields: Any) -> None:
        callback = self._metrics_callback
        if callback is None:
            return
        try:
            callback(event, dict(fields))
        except Exception:
            # Observability cannot alter Harness state or accepted-turn semantics.
            pass

    def _emit_event(
        self,
        turn_id: str | None,
        event: Mapping[str, Any],
    ) -> bool:
        callback = self._event_callback
        if callback is None:
            return True
        # These lifecycle records remain in the bounded HarnessTurnResult
        # trace, but the durable projection has one canonical terminal event:
        # the synthetic event emitted after finalization.  Forwarding every
        # intermediate barrier would make SSE close early and would present
        # several competing terminal outcomes to a replay client.
        if event.get("type") in {"agent_settled", "turn_end", "agent_end"}:
            return True
        try:
            # The callback is deliberately downstream of the single Pi reader
            # and receives a detached mapping.  It may redact, persist, or
            # publish the observation, but it cannot consume stdout or affect
            # the turn state when it fails.
            callback(self._engram_id, turn_id, dict(event))
            if event.get("type") == "turn_terminal":
                self._terminal_event_emitted = True
            return True
        except Exception:
            self._emit_metric(
                "harness_event_callback_failed",
                engram=self._engram_id,
                turn_id=turn_id,
                error_type="callback_error",
            )
            return False

    def _error(
        self,
        code: str,
        detail: str,
        remedy: str,
        *,
        phase: str,
        retryable: bool = False,
        prompt_accepted: bool | None = False,
        partial_output: str = "",
        trace: list[dict[str, Any]] | None = None,
        project_turn_terminal: bool = True,
    ) -> HarnessError:
        error = HarnessError(
            code,
            detail,
            remedy,
            phase=phase,
            retryable=retryable,
            prompt_accepted=prompt_accepted,
            partial_output=partial_output,
            trace=trace,
        )
        if project_turn_terminal:
            active_turn_id = self._active_turn_id
            terminal_projected = True
            if active_turn_id is not None and not self._terminal_event_emitted:
                terminal_projected = self._emit_event(
                    active_turn_id,
                    {
                        "type": "turn_terminal",
                        "status": "failed" if prompt_accepted is False else "uncertain",
                        "code": code,
                        "phase": phase,
                        "prompt_accepted": prompt_accepted,
                    },
                )
                if terminal_projected:
                    self._terminal_event_emitted = True
                    self._active_turn_id = None
            if terminal_projected:
                with self._state_lock:
                    if self._state in {
                        HarnessState.ADMITTING,
                        HarnessState.SETTLING,
                    }:
                        self._state = HarnessState.READY
                        self._sideband_ready_event.clear()
        self._emit_metric(
            "harness_turn_failed" if project_turn_terminal else "harness_sideband_failed",
            engram=self._engram_id,
            code=code,
            phase=phase,
            retryable=retryable,
            prompt_accepted=prompt_accepted,
        )
        return error


class PiHarnessRuntime:
    """Own live Pi processes and durable Engram binding snapshots."""

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        *,
        binding_state: Mapping[str, Any] | None = None,
        binding_callback: BindingCallback | None = None,
        metrics_callback: MetricsCallback | None = None,
        event_callback: HarnessEventCallback | None = None,
        backend: PiBackend | None = None,
        executable: str = "pi",
        provider: str | None = None,
        model: str | None = None,
        env: dict[str, str] | None = None,
        transport_factory: Any = None,
        handshake_timeout_sec: float = 30.0,
        sideband_timeout_sec: float = 30.0,
        abort_timeout_sec: float = 5.0,
        max_trace_events: int = 500,
        max_live_sessions: int | None = None,
        session_context_factory: Callable[[str], PiProcessContext] | None = None,
        tool_gateway: PulseToolGateway | None = None,
        publication_permit: RuntimePublicationPermit | None = None,
        bootstrap_permit: RuntimeBootstrapPermit | None = None,
    ) -> None:
        if (
            max_live_sessions is not None
            and (
                type(max_live_sessions) is not int
                or not 1 <= max_live_sessions <= 256
            )
        ):
            raise ValueError(
                "max_live_sessions must be an integer between 1 and 256, or None"
            )
        if (
            publication_permit is not None
            and type(publication_permit) is not RuntimePublicationPermit
        ):
            raise TypeError("publication_permit must be a RuntimePublicationPermit or None")
        if bootstrap_permit is not None and type(bootstrap_permit) is not RuntimeBootstrapPermit:
            raise TypeError("bootstrap_permit must be a RuntimeBootstrapPermit or None")
        if (
            publication_permit is not None
            and bootstrap_permit is not None
            and (
                publication_permit.owner_id,
                publication_permit.epoch,
                publication_permit.generation,
            )
            != (
                bootstrap_permit.owner_id,
                bootstrap_permit.epoch,
                bootstrap_permit.generation,
            )
        ):
            raise ValueError(
                "publication_permit and bootstrap_permit must belong to the same Runtime generation"
            )
        self._workspace = Path(workspace).expanduser().resolve()
        self._bindings = load_binding_state(binding_state)
        self._binding_callback = binding_callback
        self._metrics_callback = metrics_callback
        self._event_callback = event_callback
        self._backend = backend or PiBackend(
            executable=executable,
            workdir=self._workspace,
            provider=provider,
            model=model,
            env=env,
            transport_factory=transport_factory,
        )
        self._handshake_timeout = handshake_timeout_sec
        self._sideband_timeout = sideband_timeout_sec
        self._abort_timeout = abort_timeout_sec
        self._max_trace_events = max_trace_events
        self._max_live_sessions = max_live_sessions
        self._session_context_factory = session_context_factory
        self._tool_gateway = tool_gateway
        self._publication_permit = publication_permit
        self._bootstrap_permit = bootstrap_permit
        self._runtime_continuity_authority = _PiContinuityGuard(
            self._workspace,
            publication_permit=publication_permit,
            bootstrap_permit=bootstrap_permit,
        )
        self._pi_agent_dir = (
            self._workspace / ".pulse" / "harness" / "pi" / "agent"
        )
        self._pi_session_dir = (
            self._workspace / ".pulse" / "harness" / "pi" / "sessions"
        )
        self._capability_boundary_configured = (
            self._session_context_factory is not None or self._tool_gateway is not None
        )
        self._extension_asset = (
            pulse_extension_asset() if self._capability_boundary_configured else None
        )
        self._lock = threading.RLock()
        self._capacity_changed = threading.Condition(self._lock)
        self._sessions: dict[str, PiSession] = {}
        self._starting: set[str] = set()
        # ``close_session`` keeps a cancelled reservation present until its
        # exact checkout attempt unwinds.  A second checkout therefore cannot
        # reuse the same logical key while the first attempt is between its
        # reservation and its exact PiSession record.
        self._cancelled_starts: set[str] = set()
        self._starting_sessions: dict[str, PiSession] = {}
        # Admission and physical ownership are deliberately different
        # registries.  An entry leaves this exact-identity registry only after
        # its session summary is physically final.
        self._physical_sessions: dict[int, PiSession] = {}
        # Logical IDs can outlive admission during succession.  Each fence
        # points back to the exact retained session; aliases are removed only
        # when that physical owner is final.
        self._physical_session_fences: dict[str, PiSession] = {}
        self._session_users: dict[str, int] = {}
        self._session_recency: dict[str, int] = {}
        self._recency_clock = 0
        self._closed = False
        self._closing = False
        self._close_done = threading.Event()
        self._close_summary: PiHarnessCloseSummary | None = None
        self._closed_sessions: tuple[PiSession, ...] = ()
        self._gateway_close_started = False

    def preflight(self) -> None:
        with self._lock:
            if self._closed:
                raise self._runtime_error(
                    "harness_closed",
                    "the Pi Harness runtime has been closed",
                    "create a new runtime before preflight",
                    phase="preflight",
                )
        try:
            self._runtime_continuity_authority.publish_settings()
        except _PiContinuityFailure as exc:
            raise self._runtime_error(
                exc.code,
                exc.detail,
                exc.remedy,
                phase="preflight",
            ) from exc
        try:
            self._backend.preflight()
        except BackendError as exc:
            raise self._runtime_error(
                exc.code,
                exc.detail,
                exc.remedy,
                phase="preflight",
                retryable=True,
            ) from exc

    def run_turn(
        self,
        engram_id: str,
        prompt: str,
        *,
        timeout_sec: float | None = None,
        bootstrap_text: str | None = None,
        turn_id: str | None = None,
    ) -> HarnessTurnResult:
        self._validate_engram_id(engram_id)
        if not isinstance(prompt, str):
            raise self._runtime_error(
                "harness_input_invalid",
                "prompt must be natural text",
                "pass a Python string",
                phase="input",
            )
        if bootstrap_text is not None and not isinstance(bootstrap_text, str):
            raise self._runtime_error(
                "harness_input_invalid",
                "bootstrap_text must be natural text or None",
                "pass a Python string or None",
                phase="input",
            )
        try:
            deadline = _deadline(timeout_sec)
        except ValueError as exc:
            raise self._runtime_error(
                "harness_input_invalid",
                str(exc),
                "use a non-negative timeout in seconds or None",
                phase="input",
            ) from exc

        session = self._checkout_session(engram_id, deadline=deadline)
        try:
            remaining = _remaining(deadline)
            if remaining is not None and remaining <= 0:
                raise self._capacity_timeout(engram_id)
            return session._run_turn_for(
                engram_id,
                prompt,
                timeout_sec=remaining,
                bootstrap_text=bootstrap_text,
                turn_id=turn_id,
            )
        except HarnessError:
            self._discard_broken_session(engram_id, session)
            raise
        finally:
            self._release_session(engram_id, session)

    def snapshot(self, engram_id: str) -> dict[str, Any]:
        self._validate_engram_id(engram_id)
        with self._lock:
            session = self._sessions.get(engram_id)
            binding = self._bindings.get(engram_id)
        if session is not None:
            return session.snapshot()
        if binding is None:
            raise self._runtime_error(
                "pi_session_unknown",
                f"Engram {engram_id!r} has no live or persisted Pi session",
                "run a turn before requesting its Harness snapshot",
                phase="snapshot",
            )
        return {
            "engram_id": engram_id,
            "state": HarnessState.UNBOUND.value,
            "session_id": binding.session_id,
            "session_file": binding.session_file,
            "binding_state": binding.state.value,
            "bootstrapped": binding.bootstrapped,
        }

    def abort(self, engram_id: str) -> None:
        self._live_session(engram_id).abort()

    def steer(self, engram_id: str, content: str) -> None:
        self._live_session(engram_id).steer(content)

    def succeed(
        self,
        old_engram_id: str,
        new_engram_id: str,
        *,
        capacity_timeout_sec: float | None = None,
    ) -> None:
        self._validate_engram_id(old_engram_id)
        self._validate_engram_id(new_engram_id)
        if old_engram_id == new_engram_id:
            raise self._runtime_error(
                "pi_succession_invalid",
                "predecessor and successor Engram IDs must differ",
                "provide a distinct successor Engram ID",
                phase="succession",
            )
        # This deadline bounds resident-process admission only.  Once the
        # predecessor session is checked out, Pi owns its ordinary bounded
        # RPC lifecycle; this parameter is not a whole-rotation deadline.
        deadline = _deadline(capacity_timeout_sec)
        retained_retry: PiSession | None = None
        retained_retry_reservations: tuple[str, str] | None = None
        with self._capacity_changed:
            if self._closed:
                raise self._runtime_error(
                    "harness_closed",
                    "the Pi Harness runtime has been closed",
                    "create a new runtime before succession",
                    phase="succession",
                )
            old_binding = self._bindings.get(old_engram_id)
            new_binding = self._bindings.get(new_engram_id)
            old_fence = self._physical_session_fences.get(old_engram_id)
            new_fence = self._physical_session_fences.get(new_engram_id)
            pending_retry = bool(
                old_binding is not None
                and old_binding.state is BindingState.MATERIALIZED
                and old_binding.session_file is not None
                and new_binding is not None
                and new_binding.state is BindingState.PENDING_LINEAGE
                and new_binding.parent_session_file == old_binding.session_file
                and new_engram_id not in self._sessions
            )
            if pending_retry and old_fence is not None and old_fence is new_fence:
                if (
                    old_engram_id in self._starting
                    or new_engram_id in self._starting
                ):
                    raise self._physical_owner_unresolved_error(old_engram_id)
                retained_retry = old_fence
                retained_retry_reservations = (
                    old_engram_id,
                    new_engram_id,
                )
                self._starting.update(retained_retry_reservations)
            elif (
                pending_retry
                and old_fence is None
                and new_fence is None
                and old_engram_id not in self._sessions
                and old_engram_id not in self._starting
                and old_engram_id not in self._starting_sessions
            ):
                # The persisted lineage already committed and its predecessor
                # owner has since converged.  This is an idempotent explicit
                # succession retry, not a second rotation.
                return
            elif new_binding is not None or new_engram_id in self._sessions:
                raise self._runtime_error(
                    "pi_succession_target_exists",
                    f"successor Engram {new_engram_id!r} already has a Pi binding",
                    "choose an unbound successor or resolve the existing binding explicitly",
                    phase="succession",
                )
        if retained_retry is not None:
            assert retained_retry_reservations is not None
            try:
                converged = self._reobserve_retained_session_for_demand(
                    retained_retry,
                    requested_engram_id=new_engram_id,
                    deadline=deadline,
                    reason="succession_reobserve",
                )
            finally:
                with self._capacity_changed:
                    cancelled = any(
                        engram_id in self._cancelled_starts
                        for engram_id in retained_retry_reservations
                    )
                    runtime_closed = self._closed
                    for engram_id in retained_retry_reservations:
                        self._starting.discard(engram_id)
                        self._cancelled_starts.discard(engram_id)
                    self._capacity_changed.notify_all()
            if runtime_closed:
                raise self._runtime_error(
                    "harness_closed_during_succession",
                    "the Harness runtime closed during retained succession convergence",
                    "recover the persisted pending lineage in a new runtime",
                    phase="succession",
                )
            if cancelled:
                raise self._runtime_error(
                    "pi_session_owner_changed",
                    "the retained predecessor was removed during succession convergence",
                    "retry only after reconciling the retained physical owner",
                    phase="succession",
                )
            if not converged:
                raise self._physical_owner_unresolved_error(old_engram_id)
            return
        session = self._checkout_session(old_engram_id, deadline=deadline)
        try:
            session._rotate_to(
                old_engram_id,
                new_engram_id,
                self._commit_rotation,
            )
            summary = session._close(reason="succession")
            self._retire_physical_session(session, summary)
            if not PiSession._session_close_is_final(summary):
                raise self._physical_owner_unresolved_error(old_engram_id)
        finally:
            self._release_session(old_engram_id, session)

    def close_session(self, engram_id: str) -> None:
        self._validate_engram_id(engram_id)
        claimed: dict[int, PiSession] = {}
        with self._capacity_changed:
            session = self._sessions.pop(engram_id, None)
            starting_session = self._starting_sessions.pop(engram_id, None)
            if engram_id in self._starting:
                self._cancelled_starts.add(engram_id)
            self._session_users.pop(engram_id, None)
            self._session_recency.pop(engram_id, None)
            retained = self._physical_session_fences.get(engram_id)
            if retained is not None:
                claimed[id(retained)] = retained
            for candidate in (session, starting_session):
                if candidate is not None:
                    self._claim_physical_session_locked(candidate)
                    claimed[id(candidate)] = candidate
            self._capacity_changed.notify_all()
        for candidate in claimed.values():
            summary = candidate._close(reason="close_session")
            self._retire_physical_session(candidate, summary)

    def close(
        self,
        *,
        timeout_sec: float | None = None,
        recovery_permit: RuntimeRecoveryPermit | None = None,
    ) -> PiHarnessCloseSummary:
        """Broadcast once, then monotonically reobserve the frozen fleet.

        Runtime must pass ``recovery_permit`` when close begins after
        publication revocation.  If a no-permit close began before revocation
        but crossed it before writer sealing, Runtime may call this method
        again with the recovery permit.  A repeat may wait on the same physical
        owners, but never dispatches another signal, spawns a process, or
        closes the Gateway twice.
        """

        if timeout_sec is not None and timeout_sec < 0:
            raise ValueError("timeout_sec must be non-negative or None")
        if (
            recovery_permit is not None
            and type(recovery_permit) is not RuntimeRecoveryPermit
        ):
            raise TypeError("recovery_permit must be a RuntimeRecoveryPermit or None")
        deadline = time.monotonic() + (
            max(2.25, self._abort_timeout + 2.25)
            if timeout_sec is None
            else timeout_sec
        )
        wait_for_other = False
        first_close = False
        with self._capacity_changed:
            if self._harness_close_is_final(self._close_summary):
                assert self._close_summary is not None
                return dict(self._close_summary)
            if self._closing:
                wait_for_other = True
                sessions: tuple[PiSession, ...] = ()
            else:
                self._closing = True
                self._close_done.clear()
                if not self._closed:
                    first_close = True
                    self._closed = True
                    for session in (
                        *self._sessions.values(),
                        *self._starting_sessions.values(),
                    ):
                        self._claim_physical_session_locked(session)
                    self._closed_sessions = tuple(
                        self._physical_sessions.values()
                    )
                    self._sessions.clear()
                    self._starting.clear()
                    self._cancelled_starts.clear()
                    self._starting_sessions.clear()
                    self._session_users.clear()
                    self._session_recency.clear()
                    self._capacity_changed.notify_all()
                sessions = self._closed_sessions
        if first_close:
            self._runtime_continuity_authority.revoke_publication()
        if wait_for_other:
            self._close_done.wait(timeout=max(0.0, deadline - time.monotonic()))
            with self._capacity_changed:
                if self._close_summary is not None:
                    cached = dict(self._close_summary)
                else:
                    cached = None
                sessions_observed = len(self._closed_sessions)
            if cached is not None:
                remaining = max(0.0, deadline - time.monotonic())
                if (
                    recovery_permit is not None
                    and not self._harness_close_is_final(cached)
                    and remaining > 0
                ):
                    return self.close(
                        timeout_sec=remaining,
                        recovery_permit=recovery_permit,
                    )
                return cached
            return self._incomplete_close_summary(
                sessions_observed=sessions_observed,
                error_code="pi_harness_concurrent_close_unresolved",
            )

        # Phase one is a broadcast: every resident/starting session loses its
        # capability and dispatches transport close before this method waits
        # for even the first process or reader owner.
        for session in sessions:
            try:
                session._begin_close(reason="runtime_close")
            except Exception as exc:
                callback = self._metrics_callback
                if callback is not None:
                    try:
                        callback(
                            "harness_session_close_failed",
                            {
                                "engram": session.engram_id,
                                "reason": "runtime_close",
                                "error_type": type(exc).__name__,
                            },
                        )
                    except Exception:
                        pass

        # This acknowledgement barrier waits only for each close signal call
        # to return.  No process/root/reader wait begins until every resident
        # transport has received that first-phase signal or the shared
        # deadline has expired.
        for session in sessions:
            session._await_close_signal(deadline=deadline)

        results: list[PiSessionCloseSummary] = []
        for session in sessions:
            try:
                results.append(
                    session._finish_close(
                        deadline=deadline,
                        recovery_permit=recovery_permit,
                    )
                )
            except Exception as exc:
                callback = self._metrics_callback
                if callback is not None:
                    try:
                        callback(
                            "harness_session_close_failed",
                            {
                                "engram": session.engram_id,
                                "reason": "runtime_close_finish",
                                "error_type": type(exc).__name__,
                            },
                        )
                    except Exception:
                        pass
                results.append(self._failed_session_close_summary())
        gateway = None
        with self._capacity_changed:
            if not self._gateway_close_started:
                self._gateway_close_started = True
                gateway = self._tool_gateway
        if gateway is not None:
            try:
                gateway.close()
            except Exception:
                pass
        candidate_summary = self._aggregate_close_summaries(results)
        with self._capacity_changed:
            for session, result in zip(sessions, results, strict=True):
                if PiSession._session_close_is_final(result):
                    self._retire_physical_session_locked(session)
            self._close_summary = self._merge_harness_close_summaries(
                self._close_summary,
                candidate_summary,
            )
            if self._harness_close_is_final(self._close_summary):
                self._closed_sessions = ()
            self._closing = False
            self._close_done.set()
            self._capacity_changed.notify_all()
            return dict(self._close_summary)

    def _recovery_reseal_cached_fleet(
        self,
        sessions: tuple[PiSession, ...],
        recovery_permit: RuntimeRecoveryPermit,
        *,
        deadline: float,
    ) -> PiHarnessCloseSummary:
        """Compatibility helper for a recovery refresh of the frozen fleet."""

        results: list[PiSessionCloseSummary] = []
        for session in sessions:
            result: PiSessionCloseSummary | None
            if time.monotonic() < deadline:
                result = session._finish_close(
                    deadline=deadline,
                    recovery_permit=recovery_permit,
                )
            else:
                result = session._cached_close_evidence()
            if result is None:
                with self._capacity_changed:
                    assert self._close_summary is not None
                    return dict(self._close_summary)
            results.append(result)

        candidate = self._aggregate_close_summaries(results)
        with self._capacity_changed:
            current = self._close_summary
            self._close_summary = self._merge_harness_close_summaries(
                current,
                candidate,
            )
            assert self._close_summary is not None
            return dict(self._close_summary)

    def _claim_physical_session_locked(
        self,
        session: PiSession,
        *,
        fence_engram_ids: tuple[str, ...] = (),
    ) -> PiSession:
        """Retain one exact session before removing any admission reference."""

        key = id(session)
        current = self._physical_sessions.get(key)
        if current is not None and current is not session:
            raise RuntimeError("Pi physical owner identity collision")
        fence_ids = (session.engram_id, *fence_engram_ids)
        for engram_id in fence_ids:
            fenced = self._physical_session_fences.get(engram_id)
            if fenced is not None and fenced is not session:
                raise RuntimeError("Pi logical owner fence collision")
        self._physical_sessions[key] = session
        for engram_id in fence_ids:
            self._physical_session_fences[engram_id] = session
        return session

    def _retire_physical_session_locked(self, session: PiSession) -> bool:
        if self._physical_sessions.get(id(session)) is not session:
            return False
        self._physical_sessions.pop(id(session), None)
        for engram_id, fenced in tuple(self._physical_session_fences.items()):
            if fenced is session:
                self._physical_session_fences.pop(engram_id, None)
        return True

    def _retire_physical_session(
        self,
        session: PiSession,
        summary: PiSessionCloseSummary,
    ) -> None:
        if not PiSession._session_close_is_final(summary):
            return
        with self._capacity_changed:
            if self._retire_physical_session_locked(session):
                self._capacity_changed.notify_all()

    @staticmethod
    def _harness_close_is_final(summary: PiHarnessCloseSummary | None) -> bool:
        return bool(
            summary is not None
            and summary["owner_joined"]
            and summary["unresolved"] == 0
            and summary["process_tree_state"]
            in _PI_PHYSICAL_FINAL_TREE_STATES
            and summary["continuity_writers_sealed"]
            == summary["sessions_observed"]
        )

    @classmethod
    def _merge_harness_close_summaries(
        cls,
        current: PiHarnessCloseSummary | None,
        candidate: PiHarnessCloseSummary,
    ) -> PiHarnessCloseSummary:
        if current is None:
            return candidate
        current_tree = current["process_tree_state"]
        candidate_tree = candidate["process_tree_state"]
        tree = (
            current_tree
            if _PI_TREE_RANK[current_tree] >= _PI_TREE_RANK[candidate_tree]
            else candidate_tree
        )
        process_unresolved = min(
            current["process_owners_unresolved"],
            candidate["process_owners_unresolved"],
        )
        internal_unresolved = min(
            current["internal_owner_unresolved"],
            candidate["internal_owner_unresolved"],
        )
        unresolved = process_unresolved + internal_unresolved
        sessions_observed = max(
            current["sessions_observed"],
            candidate["sessions_observed"],
        )
        continuity_writers_sealed = max(
            current["continuity_writers_sealed"],
            candidate["continuity_writers_sealed"],
        )
        owner_joined = (
            unresolved == 0
            and tree in _PI_PHYSICAL_FINAL_TREE_STATES
            and continuity_writers_sealed == sessions_observed
        )
        return {
            "active_before": max(
                current["active_before"],
                candidate["active_before"],
            ),
            "sessions_observed": sessions_observed,
            "signals_dispatched": max(
                current["signals_dispatched"],
                candidate["signals_dispatched"],
            ),
            "signals_sent": max(
                current["signals_sent"],
                candidate["signals_sent"],
            ),
            "process_owners_observed": max(
                current["process_owners_observed"],
                candidate["process_owners_observed"],
            ),
            "process_owners_unresolved": process_unresolved,
            "channel_reader_owners_observed": max(
                current["channel_reader_owners_observed"],
                candidate["channel_reader_owners_observed"],
            ),
            "channel_reader_owners_unresolved": min(
                current["channel_reader_owners_unresolved"],
                candidate["channel_reader_owners_unresolved"],
            ),
            "transport_reader_owners_observed": max(
                current["transport_reader_owners_observed"],
                candidate["transport_reader_owners_observed"],
            ),
            "transport_reader_owners_unresolved": min(
                current["transport_reader_owners_unresolved"],
                candidate["transport_reader_owners_unresolved"],
            ),
            "close_worker_owners_observed": max(
                current["close_worker_owners_observed"],
                candidate["close_worker_owners_observed"],
            ),
            "close_worker_owners_unresolved": min(
                current["close_worker_owners_unresolved"],
                candidate["close_worker_owners_unresolved"],
            ),
            "lifecycle_owners_unresolved": min(
                current["lifecycle_owners_unresolved"],
                candidate["lifecycle_owners_unresolved"],
            ),
            "internal_owner_unresolved": internal_unresolved,
            "unresolved": unresolved,
            "owner_joined": owner_joined,
            "cancel_signalled": (
                current["cancel_signalled"] or candidate["cancel_signalled"]
            ),
            "process_tree_state": tree,
            "continuity_writers_sealed": continuity_writers_sealed,
            "error_codes": () if owner_joined else candidate["error_codes"],
        }

    @staticmethod
    def _failed_session_close_summary() -> PiSessionCloseSummary:
        return {
            "sessions_observed": 1,
            "signal_dispatched": False,
            "signal_sent": False,
            "process_owners_observed": 1,
            "process_owners_unresolved": 1,
            "channel_reader_owners_observed": 1,
            "channel_reader_owners_unresolved": 1,
            "transport_reader_owners_observed": 0,
            "transport_reader_owners_unresolved": 0,
            "close_worker_owners_observed": 1,
            "close_worker_owners_unresolved": 1,
            "lifecycle_owner_unresolved": 1,
            "internal_owner_unresolved": 3,
            "unresolved": 4,
            "owner_joined": False,
            "process_tree_state": "unknown",
            "continuity_writer_sealed": False,
            "error_code": "pi_session_close_failed",
        }

    @classmethod
    def _aggregate_close_summaries(
        cls,
        results: list[PiSessionCloseSummary],
    ) -> PiHarnessCloseSummary:
        def total(name: str) -> int:
            return sum(max(0, int(result.get(name, 0))) for result in results)

        process_unresolved = total("process_owners_unresolved")
        internal_unresolved = total("internal_owner_unresolved")
        unresolved = process_unresolved + internal_unresolved
        trees = {result.get("process_tree_state") for result in results}
        if "unknown" in trees:
            process_tree: Literal[
                "not_applicable",
                "empty_verified",
                "root_exit_only",
                "unknown",
            ] = "unknown"
        elif "root_exit_only" in trees:
            process_tree = "root_exit_only"
        elif "empty_verified" in trees:
            process_tree = "empty_verified"
        else:
            process_tree = "not_applicable"
        error_codes = tuple(
            sorted(
                {
                    code
                    for result in results
                    if isinstance((code := result.get("error_code")), str)
                    and code
                }
            )
        )
        signals_dispatched = sum(
            bool(result.get("signal_dispatched")) for result in results
        )
        signals_sent = sum(bool(result.get("signal_sent")) for result in results)
        return {
            "active_before": len(results),
            "sessions_observed": len(results),
            "signals_dispatched": signals_dispatched,
            "signals_sent": signals_sent,
            "process_owners_observed": total("process_owners_observed"),
            "process_owners_unresolved": process_unresolved,
            "channel_reader_owners_observed": total(
                "channel_reader_owners_observed"
            ),
            "channel_reader_owners_unresolved": total(
                "channel_reader_owners_unresolved"
            ),
            "transport_reader_owners_observed": total(
                "transport_reader_owners_observed"
            ),
            "transport_reader_owners_unresolved": total(
                "transport_reader_owners_unresolved"
            ),
            "close_worker_owners_observed": total(
                "close_worker_owners_observed"
            ),
            "close_worker_owners_unresolved": total(
                "close_worker_owners_unresolved"
            ),
            "lifecycle_owners_unresolved": total(
                "lifecycle_owner_unresolved"
            ),
            "internal_owner_unresolved": internal_unresolved,
            "unresolved": unresolved,
            "owner_joined": all(
                bool(result.get("owner_joined")) for result in results
            )
            and unresolved == 0
            and process_tree in _PI_PHYSICAL_FINAL_TREE_STATES,
            "cancel_signalled": signals_sent > 0,
            "process_tree_state": process_tree,
            "continuity_writers_sealed": sum(
                bool(result.get("continuity_writer_sealed"))
                for result in results
            ),
            "error_codes": error_codes,
        }

    @classmethod
    def _incomplete_close_summary(
        cls,
        *,
        sessions_observed: int,
        error_code: str,
    ) -> PiHarnessCloseSummary:
        failed = cls._failed_session_close_summary()
        results = [failed for _ in range(max(1, sessions_observed))]
        summary = cls._aggregate_close_summaries(results)
        summary["active_before"] = sessions_observed
        summary["sessions_observed"] = sessions_observed
        summary["error_codes"] = (error_code,)
        return summary

    def binding_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return binding_snapshot(self._bindings)

    def capacity_snapshot(self) -> dict[str, int | None]:
        """Return content-free resident-process capacity facts."""

        with self._lock:
            return {
                "resident_limit": self._max_live_sessions,
                "resident_sessions": len(self._sessions),
                "starting_sessions": len(self._starting),
                "busy_sessions": sum(
                    1 for count in self._session_users.values() if count > 0
                ),
            }

    @property
    def evidence_class(self) -> str:
        """State whether this runtime is backed by Pi or a test transport."""

        return (
            "FAKE_RPC_CONTRACT"
            if getattr(self._backend, "_transport_factory", None) is not None
            else "LIVE_PI_PROVIDER"
        )

    @property
    def backend_template(self) -> PiBackend:
        """Return the configured Pi process template for isolated child fleets.

        The returned backend is configuration, not a live session.  Temporary
        workers clone and further restrict it; they never reuse an Engram's
        process, session roots, Gateway capability, or durable binding.
        """

        return self._backend

    def _checkout_session(
        self,
        engram_id: str,
        *,
        deadline: float | None,
    ) -> PiSession:
        """Lease one live process, hibernating only an idle READY peer.

        A durable Pi binding is the Engram's continuity.  A live process is a
        bounded cache of that continuity.  The lease closes the race between
        returning a READY session and another Engram evicting it before its
        turn acquires the PiSession lock.
        """

        close_before_start: list[tuple[PiSession, str]] = []
        persisted: SessionBinding | None = None
        while True:
            retained_to_reobserve: PiSession | None = None
            with self._capacity_changed:
                if self._closed:
                    raise self._runtime_error(
                        "harness_closed",
                        "the Pi Harness runtime has been closed",
                        "create a new runtime before running another turn",
                        phase="registry",
                    )

                retained = self._physical_session_fences.get(engram_id)
                if retained is not None:
                    retained_to_reobserve = retained
                else:
                    current = self._sessions.get(engram_id)
                    if current is not None and current.state not in (
                        HarnessState.BROKEN,
                        HarnessState.CLOSED,
                    ):
                        self._session_users[engram_id] = (
                            self._session_users.get(engram_id, 0) + 1
                        )
                        self._touch_session_locked(engram_id)
                        return current
                    if current is not None:
                        self._claim_physical_session_locked(current)
                        self._sessions.pop(engram_id, None)
                        self._session_users.pop(engram_id, None)
                        self._session_recency.pop(engram_id, None)
                        close_before_start.append((current, "recover_broken"))
                        # The old exact owner must converge before replacement,
                        # but it cannot consume the capacity wait that only its
                        # own close can release.
                        self._starting.add(engram_id)
                        self._cancelled_starts.discard(engram_id)
                        persisted = self._bindings.get(engram_id)
                        break

                    if engram_id in self._starting:
                        self._wait_for_capacity_locked(engram_id, deadline)
                        continue

                    resident = (
                        len(self._sessions)
                        + len(self._starting)
                        + len(self._physical_sessions)
                    )
                    if (
                        self._max_live_sessions is None
                        or resident < self._max_live_sessions
                    ):
                        self._starting.add(engram_id)
                        self._cancelled_starts.discard(engram_id)
                        persisted = self._bindings.get(engram_id)
                        break

                    victim_id = self._idle_lru_session_locked()
                    if victim_id is None:
                        # A retained owner can be the only object preventing
                        # admission. Select its exact identity under the
                        # registry lock, then observe it outside that lock.
                        retained_to_reobserve = next(
                            iter(self._physical_sessions.values()),
                            None,
                        )
                        if retained_to_reobserve is None:
                            self._wait_for_capacity_locked(engram_id, deadline)
                            continue
                    else:
                        victim = self._sessions.pop(victim_id)
                        self._claim_physical_session_locked(victim)
                        self._session_users.pop(victim_id, None)
                        self._session_recency.pop(victim_id, None)
                        close_before_start.append((victim, "capacity_hibernate"))
                        self._starting.add(engram_id)
                        self._cancelled_starts.discard(engram_id)
                        persisted = self._bindings.get(engram_id)
                        break

            if retained_to_reobserve is not None:
                converged = self._reobserve_retained_session_for_demand(
                    retained_to_reobserve,
                    requested_engram_id=engram_id,
                    deadline=deadline,
                )
                if not converged:
                    raise self._physical_owner_unresolved_error(
                        retained_to_reobserve.engram_id
                    )
                continue

        for old_session, reason in close_before_start:
            old_summary = old_session._close(reason=reason, deadline=deadline)
            self._retire_physical_session(old_session, old_summary)
            if not PiSession._session_close_is_final(old_summary):
                with self._capacity_changed:
                    self._starting.discard(engram_id)
                    self._cancelled_starts.discard(engram_id)
                    self._capacity_changed.notify_all()
                self._require_capacity_budget(engram_id, deadline)
                raise self._physical_owner_unresolved_error(
                    old_session.engram_id
                )

        process_context: PiProcessContext | None = None
        session: PiSession | None = None
        try:
            self._require_capacity_budget(engram_id, deadline)
            process_context = self._new_process_context(engram_id)
            self._require_capacity_budget(engram_id, deadline)
            session = PiSession(
                engram_id,
                self._workspace,
                binding=persisted,
                backend=self._backend,
                process_context=process_context,
                binding_sink=self._accept_binding,
                metrics_callback=self._metrics_callback,
                event_callback=self._event_callback,
                handshake_timeout_sec=self._handshake_timeout,
                sideband_timeout_sec=self._sideband_timeout,
                abort_timeout_sec=self._abort_timeout,
                max_trace_events=self._max_trace_events,
                publication_permit=self._publication_permit,
                bootstrap_permit=self._bootstrap_permit,
            )
            self._require_capacity_budget(engram_id, deadline)
            with self._capacity_changed:
                self._require_capacity_budget(engram_id, deadline)
                if self._closed:
                    raise self._runtime_error(
                        "harness_closed",
                        "the Pi Harness runtime closed before session startup",
                        "create a new runtime",
                        phase="registry",
                    )
                if engram_id in self._cancelled_starts:
                    raise self._runtime_error(
                        "pi_session_closed_during_start",
                        "the Pi session was removed from admission before startup",
                        "retry only after the cancelled startup attempt has unwound",
                        phase="registry",
                    )
                self._starting_sessions[engram_id] = session
            self._require_capacity_budget(engram_id, deadline)
            session.start()
        except Exception:
            if session is not None:
                with self._capacity_changed:
                    self._claim_physical_session_locked(session)
                    self._starting_sessions.pop(engram_id, None)
                    self._starting.discard(engram_id)
                    self._cancelled_starts.discard(engram_id)
                    self._capacity_changed.notify_all()
                startup_summary = session._close(reason="startup_failed")
                self._retire_physical_session(session, startup_summary)
            else:
                if process_context is not None:
                    try:
                        process_context.revoke()
                    except Exception:
                        pass
                with self._capacity_changed:
                    self._starting_sessions.pop(engram_id, None)
                    self._starting.discard(engram_id)
                    self._cancelled_starts.discard(engram_id)
                    self._capacity_changed.notify_all()
            raise

        close_after_start = False
        runtime_closed_after_start = False
        with self._capacity_changed:
            self._starting_sessions.pop(engram_id, None)
            self._starting.discard(engram_id)
            self._cancelled_starts.discard(engram_id)
            if self._closed:
                self._claim_physical_session_locked(session)
                close_after_start = True
                runtime_closed_after_start = True
            elif session.state in (HarnessState.BROKEN, HarnessState.CLOSED):
                self._claim_physical_session_locked(session)
                close_after_start = True
            else:
                self._sessions[engram_id] = session
                self._session_users[engram_id] = 1
                self._touch_session_locked(engram_id)
            self._capacity_changed.notify_all()
        if close_after_start:
            late_summary = session._close(
                reason=(
                    "runtime_closed_during_start"
                    if runtime_closed_after_start
                    else "removed_during_start"
                )
            )
            self._retire_physical_session(session, late_summary)
            raise self._runtime_error(
                (
                    "harness_closed"
                    if runtime_closed_after_start
                    else "pi_session_closed_during_start"
                ),
                (
                    "the Pi Harness runtime was closed during session startup"
                    if runtime_closed_after_start
                    else "the Pi session was removed from admission during startup"
                ),
                (
                    "create a new runtime"
                    if runtime_closed_after_start
                    else "retry only after the retained physical owner converges"
                ),
                phase="registry",
            )
        return session

    def _release_session(self, engram_id: str, session: PiSession) -> None:
        with self._capacity_changed:
            if self._sessions.get(engram_id) is session:
                users = self._session_users.get(engram_id, 0)
                if users <= 1:
                    self._session_users.pop(engram_id, None)
                    if session.state is HarnessState.READY:
                        self._touch_session_locked(engram_id)
                else:
                    self._session_users[engram_id] = users - 1
            self._capacity_changed.notify_all()

    def _touch_session_locked(self, engram_id: str) -> None:
        self._recency_clock += 1
        self._session_recency[engram_id] = self._recency_clock

    def _idle_lru_session_locked(self) -> str | None:
        candidates = [
            engram_id
            for engram_id, session in self._sessions.items()
            if self._session_users.get(engram_id, 0) == 0
            and session.state is HarnessState.READY
        ]
        if not candidates:
            return None
        return min(
            candidates,
            key=lambda engram_id: (
                self._session_recency.get(engram_id, 0),
                engram_id,
            ),
        )

    def _wait_for_capacity_locked(
        self,
        engram_id: str,
        deadline: float | None,
    ) -> None:
        remaining = _remaining(deadline)
        if remaining is not None and remaining <= 0:
            raise self._capacity_timeout(engram_id)
        self._capacity_changed.wait(timeout=remaining)

    def _reobserve_retained_session_for_demand(
        self,
        session: PiSession,
        *,
        requested_engram_id: str,
        deadline: float | None,
        reason: str = "capacity_reobserve",
    ) -> bool:
        """Spend one bounded generation on an exact retained session.

        The caller selected ``session`` while holding ``_capacity_changed``;
        this method must run after that lock is released.  PiSession retains
        the same RPC/process owner, so this refresh cannot spawn or dispatch a
        second close signal.
        """

        self._require_capacity_budget(requested_engram_id, deadline)
        now = time.monotonic()
        observation_deadline = now + 2.25
        if deadline is not None:
            observation_deadline = min(observation_deadline, deadline)
        summary = session._close(
            reason=reason,
            deadline=observation_deadline,
        )
        self._retire_physical_session(session, summary)
        if not PiSession._session_close_is_final(summary):
            self._require_capacity_budget(requested_engram_id, deadline)
        return PiSession._session_close_is_final(summary)

    def _require_capacity_budget(
        self,
        engram_id: str,
        deadline: float | None,
    ) -> None:
        remaining = _remaining(deadline)
        if remaining is not None and remaining <= 0:
            raise self._capacity_timeout(engram_id)

    def _capacity_timeout(self, engram_id: str) -> HarnessError:
        return self._runtime_error(
            "pi_capacity_timeout",
            f"no resident Pi process slot became available for Engram {engram_id!r} before the turn deadline",
            "increase max_live_sessions or the turn timeout, or reduce simultaneous pulses",
            phase="capacity",
            retryable=True,
        )

    def _physical_owner_unresolved_error(self, engram_id: str) -> HarnessError:
        return self._runtime_error(
            "pi_physical_owner_unresolved",
            f"the previous Pi physical owner for Engram {engram_id!r} has not converged",
            "reobserve shutdown with a bounded close before admitting a successor process",
            phase="capacity",
            retryable=True,
        )

    def _new_process_context(self, engram_id: str) -> PiProcessContext | None:
        if not self._capability_boundary_configured:
            # Preserve the base Pi Harness until the caller opts
            # into the Pulse tool boundary.  In particular, do not load the
            # extension with an empty/default Gateway: its authorization hook
            # would otherwise deny Pi's ordinary built-in coding tools.
            return None
        factory = self._session_context_factory
        if factory is not None:
            context = factory(engram_id)
            if not isinstance(context, PiProcessContext):
                raise TypeError(
                    "session_context_factory must return PiProcessContext"
                )
        else:
            gateway = self._tool_gateway
            if gateway is None:
                raise HarnessError(
                    "pi_capability_gateway_missing",
                    "no Tool Gateway is available for a Pi process context",
                    "provide a session_context_factory or Tool Gateway",
                    phase="startup",
                )
            address = gateway.start()
            token = gateway.issue(engram_id)
            context = PiProcessContext(
                env={
                    "PULSE_TOOL_GATEWAY_URL": address.url,
                    "PULSE_TOOL_CAPABILITY": token,
                },
                revoke=lambda token=token, gateway=gateway: gateway.revoke(token),
            )

        assert self._extension_asset is not None
        # Never let a production Pi process discover the operator's personal
        # auth.json, models.json, settings or sessions.  These runtime-owned
        # roots are protected by the same .pulse policy as other substrate
        # state and remain stable across Runtime restarts.
        context = context.with_env(
            PI_CODING_AGENT_DIR=str(self._pi_agent_dir),
            PI_CODING_AGENT_SESSION_DIR=str(self._pi_session_dir),
        )
        _validate_extension_tool_args(context.extra_args)
        # Pi's ``--no-extensions`` disables discovery but intentionally keeps
        # explicit CLI extension paths.  Add only the internal absolute Pulse
        # asset so project/user extensions cannot register a bypass tool.
        if not _has_no_extensions(context.extra_args):
            context = context.with_extra_args("--no-extensions")
        context = context.with_extra_args("--extension", str(self._extension_asset))
        # ``--exclude-tools`` is global in Pi and would also remove the
        # same-name Pulse extension tools.  The upstream ``--no-builtin-tools``
        # mode is the supported distinction: native definitions are not active
        # while extension/custom tools remain available.
        if not _has_no_builtin_tools(context.extra_args):
            context = context.with_extra_args("--no-builtin-tools")
        return context

    def _live_session(self, engram_id: str) -> PiSession:
        self._validate_engram_id(engram_id)
        with self._lock:
            session = self._sessions.get(engram_id)
        if session is None:
            raise self._runtime_error(
                "pi_session_not_running",
                f"Engram {engram_id!r} has no started Pi session",
                "run a real turn first; abort and steer never start Pi implicitly",
                phase="sideband",
            )
        return session

    def _accept_binding(self, updated: SessionBinding) -> None:
        with self._lock:
            if self._closed:
                raise self._runtime_error(
                    "harness_closed",
                    "the Harness runtime closed before a binding could be committed",
                    "recover the accepted Pi session in a new runtime",
                    phase="binding",
                )
            candidate = dict(self._bindings)
            candidate[updated.engram_id] = updated
            self._ensure_unique_materialized(candidate)
            snapshot = binding_snapshot(candidate)
            if self._binding_callback is not None:
                self._binding_callback(snapshot)
            if self._closed:
                raise self._runtime_error(
                    "harness_closed",
                    "the Harness runtime closed while a binding callback was running",
                    "recover the callback's persisted snapshot in a new runtime",
                    phase="binding",
                )
            self._bindings = candidate

    def _commit_rotation(
        self,
        session: PiSession,
        old_binding: SessionBinding,
        pending: SessionBinding,
    ) -> None:
        with self._capacity_changed:
            if self._closed or session.state in (
                HarnessState.BROKEN,
                HarnessState.CLOSED,
            ):
                raise HarnessError(
                    "harness_closed_during_succession",
                    "the Harness runtime or Pi session closed before succession could commit",
                    "recover from the predecessor binding in a new runtime",
                    phase="succession",
                )
            if self._sessions.get(old_binding.engram_id) is not session:
                raise HarnessError(
                    "pi_session_owner_changed",
                    "the predecessor process left the registry during succession",
                    "retry only after reconciling the registry",
                    phase="succession",
                )
            if pending.engram_id in self._bindings or pending.engram_id in self._sessions:
                raise HarnessError(
                    "pi_succession_target_exists",
                    "the successor acquired a binding while succession was in flight",
                    "resolve the concurrent successor before retrying",
                    phase="succession",
                )
            candidate = dict(self._bindings)
            candidate[old_binding.engram_id] = old_binding
            candidate[pending.engram_id] = pending
            self._ensure_unique_materialized(candidate)
            snapshot = binding_snapshot(candidate)
            if self._binding_callback is not None:
                self._binding_callback(snapshot)
            self._bindings = candidate
            # Persisting pending lineage is the recovery point.  The exact
            # predecessor must enter physical ownership before admission is
            # removed, and both logical IDs remain fenced until that owner is
            # physically final.
            self._claim_physical_session_locked(
                session,
                fence_engram_ids=(old_binding.engram_id, pending.engram_id),
            )
            if self._sessions.get(old_binding.engram_id) is session:
                self._sessions.pop(old_binding.engram_id, None)
            self._session_users.pop(old_binding.engram_id, None)
            self._session_recency.pop(old_binding.engram_id, None)
            self._capacity_changed.notify_all()
            if self._closed:
                raise HarnessError(
                    "harness_closed_during_succession",
                    "the Harness runtime closed after the pending succession binding was persisted",
                    "recover the persisted pending lineage in a new runtime",
                    phase="succession",
                    prompt_accepted=True,
                )

    @staticmethod
    def _ensure_unique_materialized(
        bindings: Mapping[str, SessionBinding],
    ) -> None:
        owners: dict[str, str] = {}
        for engram_id, binding in bindings.items():
            if binding.state is not BindingState.MATERIALIZED:
                continue
            assert binding.session_file is not None
            key = os.path.normcase(normalize_session_file(binding.session_file))
            previous = owners.get(key)
            if previous is not None:
                raise HarnessError(
                    "pi_binding_conflict",
                    f"Engrams {previous!r} and {engram_id!r} share Pi session file {binding.session_file!r}",
                    "assign each materialized Engram a distinct Pi session file",
                    phase="binding",
                )
            owners[key] = engram_id

    def _discard_broken_session(self, engram_id: str, session: PiSession) -> None:
        if session.state is not HarnessState.BROKEN:
            return
        claimed = False
        with self._capacity_changed:
            if self._sessions.get(engram_id) is session:
                self._claim_physical_session_locked(session)
                claimed = True
                self._sessions.pop(engram_id, None)
                self._session_users.pop(engram_id, None)
                self._session_recency.pop(engram_id, None)
                self._capacity_changed.notify_all()
        if claimed:
            summary = session._close(reason="discard_broken")
            self._retire_physical_session(session, summary)

    @staticmethod
    def _validate_engram_id(engram_id: str) -> None:
        if not isinstance(engram_id, str) or not engram_id.strip():
            raise HarnessError(
                "harness_input_invalid",
                "engram_id must be a non-empty string",
                "pass the canonical Engram ID",
                phase="input",
            )

    @staticmethod
    def _runtime_error(
        code: str,
        detail: str,
        remedy: str,
        *,
        phase: str,
        retryable: bool = False,
    ) -> HarnessError:
        return HarnessError(
            code,
            detail,
            remedy,
            phase=phase,
            retryable=retryable,
            prompt_accepted=False,
        )

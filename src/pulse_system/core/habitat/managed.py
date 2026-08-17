"""The production, single-workspace Habitat used by the organism.

``ManagedHabitat`` is intentionally small.  It is an adapter around one
directory, not a general host/filesystem tool.  Every path is interpreted as a
relative path below the configured root; shell, network and path traversal do
not exist in this surface.

The adapter does not decide whether an external change should wake an Engram.
It reports changes with a stable fingerprint.  The Runtime commits the
corresponding causal event first and then acknowledges the fingerprint.  That
ordering means a process death cannot silently consume an observation before
the durable ledger has it.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import uuid
from collections.abc import Iterator, Mapping
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePath
from typing import Callable, TypeVar

from pulse_system.core.runtime.publication import (
    RuntimePublicationError,
    RuntimePublicationPermit,
)

from .types import (
    Action,
    ChannelSpec,
    HabitatEffectReceipt,
    Organ,
    Reply,
    Response,
)
from .world import Habitat

__all__ = [
    "ExternalEffectPublicationError",
    "ExternalEffectPublicationFence",
    "ExternalEffectTransaction",
    "ExternalEffectUncertainError",
    "HabitatChange",
    "ManagedHabitat",
    "ManagedHabitatEffectUncertain",
    "OrdinaryExternalEffectAuthority",
    "bind_external_effect_authority",
]

_STATE_FILE = ".managed-habitat-state.json"
_EFFECT_JOURNAL_FILE = ".managed-effect-journal.jsonl"
_PRIVATE_PREFIX = ".managed-"
_MAX_READ_CHARS = 64_000
_MAX_WRITE_CHARS = 1_000_000
_WORLD_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_AUTHORITY_SCOPE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$")
_REVOCATION_REASON = re.compile(r"^[a-z][a-z0-9._-]{0,63}$")
_ORDINARY_TOKEN = object()
_TRANSACTION_TOKEN = object()
_T = TypeVar("_T")


class ExternalEffectPublicationError(RuntimeError):
    """A typed external-effect authority cannot authorize a commit."""

    def __init__(self, code: str, *, effect_name: str = "") -> None:
        super().__init__(code)
        self.code = code
        self.effect_name = effect_name
        self.crossed_boundary = False


class ExternalEffectUncertainError(ExternalEffectPublicationError):
    """An external effect may have crossed its physical commit boundary."""

    def __init__(self, code: str, *, effect_name: str) -> None:
        super().__init__(code, effect_name=effect_name)
        self.crossed_boundary = True


class OrdinaryExternalEffectAuthority:
    """Non-transferable commit capability issued by one external fence."""

    __slots__ = ("_fence", "_token")

    def __init__(
        self,
        fence: "ExternalEffectPublicationFence",
        *,
        _token: object,
    ) -> None:
        if _token is not _ORDINARY_TOKEN:
            raise TypeError("ordinary authority must be issued by a fence")
        self._fence = fence
        self._token = _token

    @property
    def origin(self) -> str:
        return self._fence.origin

    def assert_active(self) -> None:
        self._fence._assert_active(self)

    @contextmanager
    def transaction(
        self,
        effect_name: str,
    ) -> Iterator["ExternalEffectTransaction"]:
        """Enter one typed, revocation-linearized filesystem transaction."""

        with self._fence._transaction(self, effect_name) as transaction:
            yield transaction

    def assert_transaction(self, transaction: "ExternalEffectTransaction") -> None:
        """Reject persistence helpers called outside this authority's guard."""

        self._fence._validate_transaction(self, transaction)

    def publish(self, effect_name: str, operation: Callable[[], _T]) -> _T:
        """Linearize one physical commit with the backing typed permit."""

        with self.transaction(effect_name) as transaction:
            transaction.mark_mutation()
            return operation()


class ExternalEffectTransaction:
    """Unforgeable, short-lived proof that one typed guard is currently held."""

    __slots__ = (
        "_active",
        "_authority",
        "_effect_name",
        "_fence",
        "_mutation_started",
        "_token",
    )

    def __init__(
        self,
        fence: "ExternalEffectPublicationFence",
        authority: OrdinaryExternalEffectAuthority,
        effect_name: str,
        *,
        _token: object,
    ) -> None:
        if _token is not _TRANSACTION_TOKEN:
            raise TypeError("external effect transaction must be issued by a fence")
        self._fence = fence
        self._authority = authority
        self._effect_name = effect_name
        self._token = _token
        self._active = True
        self._mutation_started = False

    @property
    def effect_name(self) -> str:
        return self._effect_name

    @property
    def origin(self) -> str:
        return self._fence.origin

    def mark_mutation(self) -> None:
        """Mark the first physical persistent write in this guarded scope."""

        self._fence._validate_transaction(self._authority, self)
        self._mutation_started = True


class UnboundExternalEffectAuthority:
    """Fail-closed placeholder until a composition root supplies authority."""

    __slots__ = ("scope",)

    def __init__(self, scope: str) -> None:
        if not isinstance(scope, str) or _AUTHORITY_SCOPE.fullmatch(scope) is None:
            raise ValueError("unbound scope must be a bounded identifier")
        self.scope = scope

    @property
    def origin(self) -> str:
        return "unbound"

    def assert_active(self) -> None:
        raise ExternalEffectPublicationError("external_effect_authority_required")

    @contextmanager
    def transaction(self, effect_name: str) -> Iterator[ExternalEffectTransaction]:
        del effect_name
        raise ExternalEffectPublicationError("external_effect_authority_required")
        yield  # pragma: no cover - contextmanager requires a generator

    def assert_transaction(self, transaction: ExternalEffectTransaction) -> None:
        del transaction
        raise ExternalEffectPublicationError("external_effect_authority_required")

    def publish(self, effect_name: str, operation: Callable[[], _T]) -> _T:
        del effect_name, operation
        raise ExternalEffectPublicationError("external_effect_authority_required")


class ExternalEffectPublicationFence:
    """Admit external commits against an explicit lifecycle revocation.

    The backing source is deliberately closed: only the Runtime's ordinary
    publication permit is accepted. Bootstrap/recovery permits, callbacks and
    duck-typed objects cannot enter this ordinary-effect seam.

    The Runtime permit's ``transaction_guard`` is an atomic admission point.
    Revocation either wins first and rejects the transaction, or observes an
    already-admitted owner which may finish outside the gate lock.  In
    particular, a blocked filesystem call never blocks the hard-deadline
    control path.  The Runtime can observe those owners separately through
    ``wait_for_publication_drain``.
    """

    def __init__(
        self,
        source: RuntimePublicationPermit,
    ) -> None:
        if type(source) is not RuntimePublicationPermit:
            raise TypeError(
                "external effect fence requires ordinary Runtime publication authority"
            )
        self._source = source
        self._lock = threading.RLock()
        self._revoked = False
        self._reason: str | None = None
        self._ordinary = OrdinaryExternalEffectAuthority(self, _token=_ORDINARY_TOKEN)

    @property
    def ordinary_authority(self) -> OrdinaryExternalEffectAuthority:
        return self._ordinary

    @property
    def origin(self) -> str:
        return "runtime"

    def revoke(self, *, reason: str) -> None:
        if not isinstance(reason, str) or _REVOCATION_REASON.fullmatch(reason) is None:
            raise ValueError("reason must be a bounded lowercase code")
        with self._lock:
            self._revoked = True
            self._reason = reason

    def _validate(self, authority: OrdinaryExternalEffectAuthority) -> None:
        if (
            type(authority) is not OrdinaryExternalEffectAuthority
            or authority._token is not _ORDINARY_TOKEN
            or authority._fence is not self
        ):
            raise ExternalEffectPublicationError("external_effect_authority_mismatch")

    def _assert_source(self, *, effect_name: str = "") -> None:
        if self._revoked:
            raise ExternalEffectPublicationError(
                "external_effect_fence_revoked",
                effect_name=effect_name,
            )
        try:
            self._source.assert_publication()
        except RuntimePublicationError as exc:
            raise ExternalEffectPublicationError(
                exc.code,
                effect_name=effect_name,
            ) from exc

    def _assert_active(self, authority: OrdinaryExternalEffectAuthority) -> None:
        with self._lock:
            self._validate(authority)
            self._assert_source()

    def _validate_transaction(
        self,
        authority: OrdinaryExternalEffectAuthority,
        transaction: ExternalEffectTransaction,
    ) -> None:
        with self._lock:
            self._validate(authority)
            if (
                type(transaction) is not ExternalEffectTransaction
                or transaction._token is not _TRANSACTION_TOKEN
                or transaction._fence is not self
                or transaction._authority is not authority
                or not transaction._active
            ):
                raise ExternalEffectPublicationError(
                    "external_effect_transaction_required"
                )

    @contextmanager
    def _transaction(
        self,
        authority: OrdinaryExternalEffectAuthority,
        effect_name: str,
    ) -> Iterator[ExternalEffectTransaction]:
        if not isinstance(effect_name, str) or _AUTHORITY_SCOPE.fullmatch(effect_name) is None:
            raise ValueError("effect_name must be a bounded identifier")
        try:
            with ExitStack() as admitted:
                # Hold the local lock only across validation and the Runtime
                # guard's atomic admission increment.  The potentially
                # blocking filesystem body deliberately runs without it, so
                # revoke remains immediate while this transaction is counted
                # as a pre-revoke owner by RuntimePublicationGate.
                with self._lock:
                    self._validate(authority)
                    if self._revoked:
                        raise ExternalEffectPublicationError(
                            "external_effect_fence_revoked",
                            effect_name=effect_name,
                        )
                    admitted.enter_context(self._source.transaction_guard())
                transaction = ExternalEffectTransaction(
                    self,
                    authority,
                    effect_name,
                    _token=_TRANSACTION_TOKEN,
                )
                try:
                    yield transaction
                except ExternalEffectUncertainError:
                    raise
                except Exception as exc:
                    if not transaction._mutation_started:
                        raise
                    raise ExternalEffectUncertainError(
                        "external_effect_commit_uncertain",
                        effect_name=effect_name,
                    ) from exc
                finally:
                    transaction._active = False
        except RuntimePublicationError as exc:
            raise ExternalEffectPublicationError(
                exc.code,
                effect_name=effect_name,
            ) from exc


def bind_external_effect_authority(
    authority: (
        OrdinaryExternalEffectAuthority
        | RuntimePublicationPermit
        | None
    ),
    *,
    unbound_scope: str,
) -> OrdinaryExternalEffectAuthority | UnboundExternalEffectAuthority:
    """Normalize only recognized typed authorities for adapter construction.

    ``None`` deliberately stays unbound: adapters may still be constructed for
    reads and discovery, but every persistent effect fails closed. Runtime and
    effectful test composition must pass an ordinary publication permit;
    bootstrap/recovery authority cannot be upgraded into ordinary effects.
    """

    if authority is None:
        return UnboundExternalEffectAuthority(unbound_scope)
    if type(authority) is OrdinaryExternalEffectAuthority:
        return authority
    if type(authority) is RuntimePublicationPermit:
        return ExternalEffectPublicationFence(authority).ordinary_authority
    raise TypeError(
        "publication_authority must be an ordinary Runtime publication or "
        "external-effect authority"
    )


class ManagedHabitatEffectUncertain(ExternalEffectUncertainError):
    """A Habitat effect is quarantined because success cannot be asserted."""


@dataclass(frozen=True, slots=True)
class HabitatChange:
    """One unacknowledged external change discovered by ``poll``."""

    path: str
    fingerprint: str
    response: Response


class ManagedHabitat(Habitat):
    """A safe, persistent local Habitat for one PulseWorld.

    The default root is ``<workspace>/.pulse/habitat/<world_id>``.  An
    explicitly supplied root is accepted only when it remains inside the
    workspace, so a caller cannot turn a life tool into an arbitrary personal
    directory browser.
    """

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        world_id: str,
        *,
        root: str | os.PathLike[str] | None = None,
        publication_authority: (
            OrdinaryExternalEffectAuthority
            | RuntimePublicationPermit
            | None
        ) = None,
    ) -> None:
        self._workspace = Path(workspace).expanduser().resolve()
        if not self._workspace.is_dir():
            raise ValueError("workspace must be an existing directory")
        if not isinstance(world_id, str) or _WORLD_ID.fullmatch(world_id) is None:
            raise ValueError("world_id must be one safe path segment")
        self._world_id = world_id
        self._publication = bind_external_effect_authority(
            publication_authority,
            unbound_scope=f"unbound:managed-habitat:{world_id}",
        )
        candidate = (
            Path(root).expanduser()
            if root is not None
            else self._workspace / ".pulse" / "habitat" / world_id
        ).resolve()
        try:
            candidate.relative_to(self._workspace)
        except ValueError as exc:
            raise ValueError("ManagedHabitat root must stay inside workspace") from exc
        self._root = candidate
        if not self._root.exists():
            self._publication.publish(
                "managed-habitat:root-create",
                lambda: self._root.mkdir(parents=True, exist_ok=False),
            )
        if not self._root.is_dir():
            raise ValueError("ManagedHabitat root must be a directory")
        self._state_path = self._root / _STATE_FILE
        self._effect_journal_path = self._root / _EFFECT_JOURNAL_FILE
        self._lock = threading.RLock()
        self._seen = self._load_seen()
        super().__init__()

    @property
    def root(self) -> Path:
        """The resolved root for internal adapters and tests."""

        return self._root

    @property
    def world_id(self) -> str:
        return self._world_id

    @property
    def publication_origin(self) -> str:
        """Safe composition evidence for the ordinary Runtime authority."""

        return self._publication.origin

    def recovery_snapshot(self) -> dict[str, int]:
        """Return payload-free durable recovery counts without changing state."""

        with self._lock:
            latest: dict[str, str] = {}
            journal_records = 0
            malformed_records = 0
            journal_readable = 1
            if self._effect_journal_path.is_file():
                try:
                    lines = self._effect_journal_path.read_text(
                        encoding="utf-8"
                    ).splitlines()
                except (OSError, UnicodeError):
                    lines = []
                    journal_readable = 0
                for line in lines:
                    journal_records += 1
                    try:
                        record = json.loads(line)
                    except (TypeError, ValueError):
                        malformed_records += 1
                        continue
                    if not isinstance(record, dict):
                        malformed_records += 1
                        continue
                    effect_id = record.get("effect_id")
                    state = record.get("state")
                    if isinstance(effect_id, str) and isinstance(state, str):
                        latest[effect_id] = state
                    else:
                        malformed_records += 1
            staging_readable = 1
            try:
                staged_ids = {
                    path.name.removeprefix(".managed-").removesuffix(".staged")
                    for path in self._root.glob(".managed-*.staged")
                    if path.is_file()
                }
            except OSError:
                staged_ids = set()
                staging_readable = 0
            orphaned_staged = sum(effect_id not in latest for effect_id in staged_ids)
            prepared = sum(state == "prepared" for state in latest.values())
            uncertain = sum(state == "uncertain" for state in latest.values())
            committed = sum(state == "committed" for state in latest.values())
            recovered = sum(state == "recovered" for state in latest.values())
            aborted = sum(state == "aborted" for state in latest.values())
            scan_unresolved = (
                malformed_records
                + (1 - journal_readable)
                + (1 - staging_readable)
            )
            return {
                "attempted": len(latest) + orphaned_staged,
                "committed": committed,
                "recovered": recovered,
                "unresolved": (
                    prepared + uncertain + orphaned_staged + scan_unresolved
                ),
                "uncertain": uncertain,
                "prepared": prepared,
                "aborted": aborted,
                "staged": len(staged_ids),
                "orphaned_staged": orphaned_staged,
                "journal_records": journal_records,
                "malformed_records": malformed_records,
                "journal_readable": journal_readable,
                "staging_readable": staging_readable,
                "evidence_scan_unresolved": scan_unresolved,
            }

    def verify_effect_receipt(
        self,
        receipt: Mapping[str, object],
    ) -> Mapping[str, object] | None:
        """Resolve a returned effect receipt against the fsynced journal."""

        if not isinstance(receipt, Mapping):
            return None
        effect_id = receipt.get("journal_effect_id")
        if not isinstance(effect_id, str) or not effect_id:
            return None
        with self._lock:
            if not self._effect_journal_path.is_file():
                return None
            try:
                lines = self._effect_journal_path.read_text(
                    encoding="utf-8"
                ).splitlines()
            except (OSError, UnicodeError):
                return None
            canonical: Mapping[str, object] | None = None
            for line in lines:
                try:
                    record = json.loads(line)
                except (TypeError, ValueError):
                    continue
                if isinstance(record, Mapping) and record.get("effect_id") == effect_id:
                    canonical = record
            if canonical is None or canonical.get("state") != "committed":
                return None
            if (
                receipt.get("terminal_state") != "committed"
                or receipt.get("correlation_id") != canonical.get("correlation_id")
                or receipt.get("kind") != canonical.get("kind")
                or receipt.get("path") != canonical.get("path")
                or receipt.get("before_digest") != canonical.get("before_digest")
                or receipt.get("after_digest") != canonical.get("after_digest")
                or canonical.get("digest") != canonical.get("after_digest")
                or canonical.get("error") is not None
                or not isinstance(canonical.get("recorded_at"), str)
            ):
                return None
            return {
                "journal_effect_id": effect_id,
                "correlation_id": canonical.get("correlation_id"),
                "kind": canonical.get("kind"),
                "path": canonical.get("path"),
                "before_digest": canonical.get("before_digest"),
                "after_digest": canonical.get("after_digest"),
                "terminal_state": "committed",
                "produced_at": canonical.get("recorded_at"),
            }

    def channels(self) -> list[ChannelSpec]:
        return [
            ChannelSpec(
                "filesystem",
                "files in the managed Habitat can yield or refuse",
                authored_by_world=True,
                unbidden_capable=True,
            )
        ]

    def organs(self) -> list[Organ]:
        return [
            Organ("directory", "list entries below the managed Habitat", ("directory",)),
            Organ("file", "read UTF-8 text below the managed Habitat", ("file",)),
        ]

    def perceive(self, organ: str, target: str = "") -> str:
        organ = organ.strip().casefold() if isinstance(organ, str) else ""
        if organ in {"directory", "list", "ls"}:
            return self._list_text(target)
        if organ in {"file", "read"}:
            try:
                path = self._safe_path(target, allow_root=False)
                return self._read_text(path)
            except (OSError, ValueError) as exc:
                return f"拒绝读取：{self._safe_error(exc)}"
        return "拒绝感知：未知 organ"

    def _act(self, action: Action) -> list[Response]:
        verb = action.verb.strip().casefold() if isinstance(action.verb, str) else ""
        receipt: HabitatEffectReceipt | None = None
        try:
            if verb in {"list", "ls"}:
                detail = self._list_text(action.target)
            elif verb in {"read", "observe"}:
                path = self._safe_path(action.target, allow_root=False)
                detail = self._read_text(path)
            elif verb == "write":
                path = self._safe_path(action.target, allow_root=False)
                receipt = self._atomic_write(
                    path,
                    action.payload,
                    append=False,
                    correlation_id=action.correlation_id,
                )
                detail = f"环境已写入：{self._relative(path)}"
            elif verb == "append":
                path = self._safe_path(action.target, allow_root=False)
                receipt = self._atomic_write(
                    path,
                    action.payload,
                    append=True,
                    correlation_id=action.correlation_id,
                )
                detail = f"环境已追加：{self._relative(path)}"
            elif verb == "mkdir":
                path = self._safe_path(action.target, allow_root=True)
                self._mkdir(path)
                detail = f"环境目录已建立：{self._relative(path) or '.'}"
            else:
                return [Response("filesystem", Reply.REFUSE, "环境拒绝：未知 action")]
        except ExternalEffectUncertainError as exc:
            return [
                Response(
                    "filesystem",
                    Reply.REFUSE,
                    f"环境作用状态不确定：UNCERTAIN/{exc.code}",
                )
            ]
        except ExternalEffectPublicationError as exc:
            return [
                Response(
                    "filesystem",
                    Reply.REFUSE,
                    f"环境拒绝：{exc.code}",
                )
            ]
        except (OSError, ValueError) as exc:
            return [Response("filesystem", Reply.REFUSE, f"环境拒绝：{self._safe_error(exc)}")]
        return [
            Response(
                "filesystem",
                Reply.YIELD,
                detail,
                effect_receipt=receipt,
            )
        ]

    def poll(self) -> list[Response]:
        """Return unacknowledged changes as plain Habitat responses."""

        return [change.response for change in self.poll_changes()]

    def poll_changes(self) -> list[HabitatChange]:
        """Discover external file changes without consuming them."""

        changes: list[HabitatChange] = []
        with self._lock:
            current: dict[str, str] = {}
            for path in sorted(self._root.rglob("*")):
                if not path.is_file():
                    continue
                try:
                    path.resolve().relative_to(self._root)
                except ValueError:
                    # A symlink placed inside the root must not turn polling
                    # into an outside-workspace read either.
                    continue
                relative = self._relative(path)
                if self._is_private_relative(relative):
                    # Atomic adapter/state writes briefly exist on disk. They
                    # are implementation state, never speech from the world.
                    continue
                try:
                    fingerprint = self._fingerprint(path)
                except OSError:
                    continue
                current[relative] = fingerprint
                if self._seen.get(relative) == fingerprint:
                    continue
                try:
                    text = self._read_text(path)
                except OSError as exc:
                    text = f"文件无法读取：{self._safe_error(exc)}"
                detail = f"环境文件发生变化：{relative}\n{text}"
                changes.append(
                    HabitatChange(
                        path=relative,
                        fingerprint=fingerprint,
                        response=Response(
                            "filesystem",
                            Reply.YIELD,
                            detail,
                            unbidden=True,
                        ),
                    )
                )

            # Deletions are also world speech.  Keep the content short and
            # never attempt to recreate the file.
            for relative in sorted(set(self._seen).difference(current)):
                if self._is_private_relative(relative):
                    continue
                fingerprint = self._deleted_fingerprint(relative)
                if self._seen.get(relative) == fingerprint:
                    continue
                changes.append(
                    HabitatChange(
                        path=relative,
                        fingerprint=fingerprint,
                        response=Response(
                            "filesystem",
                            Reply.REFUSE,
                            f"环境文件已消失：{relative}",
                            unbidden=True,
                        ),
                    )
                )
        return changes

    def acknowledge(self, change: HabitatChange) -> bool:
        """Persist a durable observation only while that version is current.

        Polling and durable event creation are intentionally separated. A
        later adapter-owned write or a newer external version may therefore
        win before the old event is acknowledged. Fingerprints are hashes, not
        ordered versions, so the safe CAS is against the file state current
        under this adapter's publication lock. A stale acknowledge is a no-op;
        the newer state remains visible (or already remembered).
        """

        with self._lock:
            try:
                path = self._safe_path(change.path, allow_root=False)
            except ValueError:
                return False
            try:
                if path.is_file():
                    current_fingerprint = self._fingerprint(path)
                elif not path.exists():
                    current_fingerprint = self._deleted_fingerprint(change.path)
                else:
                    return False
            except OSError:
                return False
            if current_fingerprint != change.fingerprint:
                return False
            if self._seen.get(change.path) == change.fingerprint:
                return True
            effect_id = uuid.uuid4().hex
            previous = self._seen.get(change.path)
            self._seen[change.path] = change.fingerprint
            try:
                with self._publication.transaction(
                    "managed-habitat:observation-ack"
                ) as transaction:
                    self._record_effect(
                        effect_id,
                        state="prepared",
                        kind="habitat_observation_ack",
                        path=change.path,
                        transaction=transaction,
                    )
                    self._save_seen(transaction=transaction)
                    self._record_effect(
                        effect_id,
                        state="committed",
                        kind="habitat_observation_ack",
                        path=change.path,
                        transaction=transaction,
                    )
                return True
            except ExternalEffectPublicationError:
                if previous is None:
                    self._seen.pop(change.path, None)
                else:
                    self._seen[change.path] = previous
                return False

    def _list_text(self, target: str) -> str:
        path = self._safe_path(target, allow_root=True)
        if not path.exists() or not path.is_dir():
            raise ValueError("directory does not exist")
        names = []
        for child in sorted(path.iterdir(), key=lambda item: item.name.casefold()):
            if child.name.startswith(_PRIVATE_PREFIX):
                continue
            names.append(child.name + ("/" if child.is_dir() else ""))
        return "\n".join(names) if names else "（目录为空）"

    def _safe_path(self, relative: str, *, allow_root: bool) -> Path:
        if not isinstance(relative, str):
            raise ValueError("path must be a relative string")
        raw = relative.strip()
        if not raw:
            if allow_root:
                return self._root
            raise ValueError("path must not be empty")
        # Reject both Windows and POSIX absolute/traversal spellings before
        # Path normalization can hide them.
        if Path(raw).is_absolute() or PurePath(raw).is_absolute():
            raise ValueError("absolute paths are not allowed")
        parts = raw.replace("\\", "/").split("/")
        if any(part in {"", ".", ".."} for part in parts):
            raise ValueError("path must be a safe relative path")
        if any(part.startswith(_PRIVATE_PREFIX) for part in parts):
            raise ValueError("ManagedHabitat private paths are reserved")
        candidate = (self._root / Path(*parts)).resolve()
        try:
            candidate.relative_to(self._root)
        except ValueError as exc:
            raise ValueError("path escapes the Habitat root") from exc
        if candidate == self._state_path:
            raise ValueError("Habitat state is private")
        return candidate

    def _read_text(self, path: Path) -> str:
        data = path.read_bytes()
        text = data.decode("utf-8", errors="replace")
        if len(text) > _MAX_READ_CHARS:
            return text[:_MAX_READ_CHARS] + "\n[…内容已截断]"
        return text

    def _atomic_write(
        self,
        path: Path,
        payload: str,
        *,
        append: bool,
        correlation_id: str | None = None,
    ) -> HabitatEffectReceipt:
        if not isinstance(payload, str):
            raise ValueError("payload must be text")
        if correlation_id is not None and (
            not isinstance(correlation_id, str)
            or not correlation_id
            or len(correlation_id) > 128
        ):
            raise ValueError("correlation_id must be a bounded non-empty string")
        if len(payload) > _MAX_WRITE_CHARS:
            raise ValueError("payload is too large")
        # The append read, replacement and adapter-owned fingerprint form one
        # publication transaction with ``poll_changes``.  Releasing the lock
        # between replace and remember would expose our own write as world
        # speech; omitting it around the append read would lose concurrent
        # updates.  ``RLock`` permits ``_remember`` to reuse the same domain.
        with self._lock:
            self._publication.assert_active()
            parent_created = self._mkdir(path.parent)
            before_digest = (
                hashlib.sha256(path.read_bytes()).hexdigest()
                if path.is_file()
                else None
            )
            content = payload
            if append and path.exists():
                # ``_read_text`` is intentionally bounded for perception.  An
                # append must preserve the complete durable file instead of
                # silently truncating it at the perception limit.
                content = (
                    path.read_bytes().decode("utf-8", errors="replace") + payload
                )
                if len(content) > _MAX_WRITE_CHARS:
                    raise ValueError("resulting file is too large")
            effect_id = uuid.uuid4().hex
            kind = "habitat_file_append" if append else "habitat_file_write"
            relative = self._relative(path)
            digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
            staged_path = path.parent / f".managed-{effect_id}.staged"
            prepared = False
            try:
                with self._publication.transaction(
                    "managed-habitat:file-prepare"
                ) as transaction:
                    self._record_effect(
                        effect_id,
                        state="prepared",
                        kind=kind,
                        path=relative,
                        digest=digest,
                        correlation_id=correlation_id,
                        before_digest=before_digest,
                        after_digest=digest,
                        transaction=transaction,
                    )
                    with staged_path.open(
                        "x",
                        encoding="utf-8",
                        newline="",
                    ) as stream:
                        stream.write(content)
                        stream.flush()
                        os.fsync(stream.fileno())
                prepared = True
                self._failpoint("before_external_effect_commit")
                with self._publication.transaction(
                    "managed-habitat:file-commit"
                ) as transaction:
                    transaction.mark_mutation()
                    os.replace(staged_path, path)
                    self._failpoint(
                        "after_business_replace_before_terminal_evidence"
                    )
                    self._seen[relative] = self._fingerprint(path)
                    self._save_seen(transaction=transaction)
                    self._record_effect(
                        effect_id,
                        state="committed",
                        kind=kind,
                        path=relative,
                        digest=digest,
                        correlation_id=correlation_id,
                        before_digest=before_digest,
                        after_digest=digest,
                        transaction=transaction,
                    )
            except ExternalEffectPublicationError as exc:
                crossed_boundary = exc.crossed_boundary or parent_created or prepared
                if crossed_boundary:
                    raise ManagedHabitatEffectUncertain(
                        "habitat_file_publication_uncertain",
                        effect_name=exc.effect_name or "managed-habitat:file-commit",
                    ) from exc
                raise
            actual_digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if actual_digest != digest:
                raise ManagedHabitatEffectUncertain(
                    "habitat_file_digest_mismatch",
                    effect_name="managed-habitat:file-commit",
                )
            return HabitatEffectReceipt(
                journal_effect_id=effect_id,
                correlation_id=correlation_id,
                kind=kind,
                path=relative,
                before_digest=before_digest,
                after_digest=digest,
                terminal_state="committed",
            )

    def _mkdir(self, path: Path) -> bool:
        with self._lock:
            self._publication.assert_active()
            if path.exists():
                return False
            effect_id = uuid.uuid4().hex
            relative = self._relative(path) or "."
            prepared = False
            try:
                with self._publication.transaction(
                    "managed-habitat:mkdir-prepare"
                ) as transaction:
                    self._record_effect(
                        effect_id,
                        state="prepared",
                        kind="habitat_mkdir",
                        path=relative,
                        transaction=transaction,
                    )
                prepared = True
                self._failpoint("before_external_effect_commit")
                with self._publication.transaction(
                    "managed-habitat:mkdir-commit"
                ) as transaction:
                    transaction.mark_mutation()
                    path.mkdir(parents=True, exist_ok=True)
                    self._record_effect(
                        effect_id,
                        state="committed",
                        kind="habitat_mkdir",
                        path=relative,
                        transaction=transaction,
                    )
            except ExternalEffectPublicationError as exc:
                if prepared or exc.crossed_boundary:
                    raise ManagedHabitatEffectUncertain(
                        "habitat_directory_publication_uncertain",
                        effect_name=exc.effect_name or "managed-habitat:mkdir-commit",
                    ) from exc
                raise
            return True

    def _load_seen(self) -> dict[str, str]:
        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError):
            return {}
        values = raw.get("fingerprints") if isinstance(raw, dict) else None
        if not isinstance(values, dict):
            return {}
        return {
            str(path): fingerprint
            for path, fingerprint in values.items()
            if (
                isinstance(path, str)
                and isinstance(fingerprint, str)
                and not self._is_private_relative(path)
            )
        }

    @staticmethod
    def _is_private_relative(relative: str) -> bool:
        return any(
            part.startswith(_PRIVATE_PREFIX)
            for part in relative.replace("\\", "/").split("/")
        )

    def _save_seen(self, *, transaction: ExternalEffectTransaction) -> None:
        self._publication.assert_transaction(transaction)
        transaction.mark_mutation()
        payload = json.dumps(
            {"version": 1, "fingerprints": dict(sorted(self._seen.items()))},
            ensure_ascii=False,
            indent=2,
        )
        temporary_path = self._root / f".managed-state-{uuid.uuid4().hex}.staged"
        try:
            with temporary_path.open(
                "x",
                encoding="utf-8",
                newline="\n",
            ) as stream:
                stream.write(payload + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, self._state_path)
        finally:
            if temporary_path.exists():
                try:
                    temporary_path.unlink()
                except OSError:
                    pass

    def _record_effect(
        self,
        effect_id: str,
        *,
        state: str,
        kind: str,
        path: str,
        digest: str | None = None,
        correlation_id: str | None = None,
        before_digest: str | None = None,
        after_digest: str | None = None,
        error: str | None = None,
        transaction: ExternalEffectTransaction,
    ) -> None:
        """Append payload-free evidence only inside a typed transaction."""

        self._publication.assert_transaction(transaction)
        transaction.mark_mutation()

        record = {
            "version": 2,
            "effect_id": effect_id,
            "correlation_id": correlation_id,
            "state": state,
            "kind": kind,
            "path": path,
            "digest": digest,
            "before_digest": before_digest,
            "after_digest": after_digest,
            "error": error,
            "recorded_at": datetime.now(timezone.utc).isoformat(
                timespec="milliseconds"
            ),
        }
        try:
            with self._effect_journal_path.open(
                "a",
                encoding="utf-8",
                newline="\n",
            ) as stream:
                stream.write(json.dumps(record, ensure_ascii=True, sort_keys=True) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
        except OSError as exc:
            raise ExternalEffectPublicationError(
                "external_effect_journal_unavailable"
            ) from exc

    def _failpoint(self, name: str) -> None:
        """No-op production hook for deterministic commit-boundary tests."""

        del name

    def _fingerprint(self, path: Path) -> str:
        digest = hashlib.sha256()
        # A fingerprint identifies an observation, not merely a byte string.
        # Two distinct files with identical content must still wake a subscriber
        # twice; otherwise Runtime idempotency would silently collapse one of
        # the world's voices.
        digest.update(self._relative(path).encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(64 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _deleted_fingerprint(relative: str) -> str:
        return hashlib.sha256(f"deleted:{relative}".encode("utf-8")).hexdigest()

    def _relative(self, path: Path) -> str:
        return path.relative_to(self._root).as_posix()

    @staticmethod
    def _safe_error(exc: BaseException) -> str:
        return type(exc).__name__

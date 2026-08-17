"""Durable, bounded role authority for the Harness execution plane.

``RoleLease`` is deliberately a separate domain from purpose, subject
identity and the Runtime lease.  It answers only: *which holder may perform
which bounded action, until when, under which fencing epochs?*

The store is intentionally self-contained.  It uses its own SQLite database
and does not import or mutate the PulseWorld storage schema.  A Runtime lease
is represented here only by :class:`RuntimeLeaseProof`; this module never
creates, renews or releases a Runtime lease.  The proof is checked again at
every authorization and mutation boundary.

The module is a durable domain contract, not a production Runtime adapter.
Every public snapshot therefore carries ``LIVE_GATE_UNVERIFIED`` evidence.
No method in this module upgrades that evidence to ``LIVE``.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import secrets
import sqlite3
import threading
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pulse_system.core.runtime.publication import (
        RuntimeBootstrapPermit,
        RuntimePublicationPermit,
        RuntimeRecoveryPermit,
    )

__all__ = [
    "CONTRACT_ONLY",
    "LIVE",
    "LIVE_GATE_UNVERIFIED",
    "HarnessAuthority",
    "HolderKind",
    "PurposeAuthorityError",
    "RoleClass",
    "RoleAccountabilityObservation",
    "RoleAccountabilitySnapshot",
    "RoleEvidenceClass",
    "RoleContribution",
    "RoleContributionEvidence",
    "RoleContributionKind",
    "RoleContributionSummary",
    "RoleLease",
    "RoleLeaseConflictError",
    "RoleLeaseError",
    "RoleLeaseExpiredError",
    "RoleLeaseHolderError",
    "RoleLeaseNotFoundError",
    "RoleLeaseScopeError",
    "RoleLeaseStateError",
    "RoleLeaseStatus",
    "RoleLeaseStorageError",
    "RoleLeaseStore",
    "RoleLeaseValidationError",
    "RoleLeaseLedger",
    "RoleObligation",
    "RoleObligationKind",
    "RoleOutputKind",
    "RoleRenewalEvidence",
    "RoleReceiptVerifier",
    "RoleScope",
    "RuntimeLeaseFenceError",
    "RuntimeLeaseProof",
    "VerifiedRoleReceipt",
]


CONTRACT_ONLY = "CONTRACT_ONLY"
LIVE_GATE_UNVERIFIED = "LIVE_GATE_UNVERIFIED"
LIVE = "LIVE"
_VERIFIED_RECEIPT_SEAL = object()
_VERIFIED_RECEIPT_KEY = secrets.token_bytes(32)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@-]{0,127}$")
_WILDCARD_RE = re.compile(r"[*?]")
_ROLE_LABEL_MAX_BYTES = 256
_MAX_SUBJECT_TTL_SECONDS = 90.0 * 24.0 * 60.0 * 60.0
_MAX_TASK_TTL_SECONDS = 24.0 * 60.0 * 60.0
_DEFAULT_SUBJECT_TTL_SECONDS = 30.0 * 24.0 * 60.0 * 60.0
_DEFAULT_TASK_TTL_SECONDS = 30.0 * 60.0
_MAX_LIST_LIMIT = 500
_MAX_RENEWAL_COUNT = 3
_MAX_DIRECT_OUTPUTS_REQUIRED = 16
_MAX_CONSECUTIVE_COORDINATION = 64
_ROLE_ROW_SELECT = """SELECT role_leases.*,
    (SELECT obligation_json FROM role_obligations
        WHERE role_obligations.role_lease_id = role_leases.role_lease_id)
        AS obligation_json,
    (SELECT obligation_digest FROM role_obligations
        WHERE role_obligations.role_lease_id = role_leases.role_lease_id)
        AS obligation_digest,
    (SELECT accountability_cycle_id FROM role_obligations
        WHERE role_obligations.role_lease_id = role_leases.role_lease_id)
        AS accountability_cycle_id,
    (SELECT role_accountability_cycles.world_id
       FROM role_accountability_cycles JOIN role_obligations
         ON role_obligations.accountability_cycle_id =
            role_accountability_cycles.accountability_cycle_id
      WHERE role_obligations.role_lease_id = role_leases.role_lease_id)
        AS accountability_world_id,
    (SELECT role_accountability_cycles.obligation_digest
       FROM role_accountability_cycles JOIN role_obligations
         ON role_obligations.accountability_cycle_id =
            role_accountability_cycles.accountability_cycle_id
      WHERE role_obligations.role_lease_id = role_leases.role_lease_id)
        AS accountability_obligation_digest
    FROM role_leases"""


class RoleLeaseError(ValueError):
    """Base class for fail-closed role-domain errors."""

    code = "role_lease_error"


class RoleLeaseValidationError(RoleLeaseError):
    """Input does not satisfy the role lease contract."""

    code = "role_lease_invalid"


class RoleLeaseConflictError(RoleLeaseError):
    """A CAS, uniqueness or immutable-scope check failed."""

    code = "role_lease_epoch_conflict"


class RoleLeaseExpiredError(RoleLeaseError):
    """The requested role lease is expired or became expired at the boundary."""

    code = "role_lease_expired"


class RoleLeaseHolderError(RoleLeaseError):
    """The caller is not the holder recorded by the lease."""

    code = "role_lease_holder_mismatch"


class RoleLeaseScopeError(RoleLeaseError):
    """The caller's action scope is not the lease's exact bounded scope."""

    code = "role_lease_scope_mismatch"


class RoleLeaseStateError(RoleLeaseError):
    """The requested lifecycle transition is not valid for the current state."""

    code = "role_lease_state_invalid"


class RoleLeaseStorageError(RoleLeaseError):
    """SQLite could not safely complete a durable operation."""

    code = "role_lease_storage_error"


class RuntimeLeaseFenceError(RoleLeaseError):
    """The supplied Runtime owner/epoch is not the lease's write fence."""

    code = "runtime_lease_lost"


class PurposeAuthorityError(RoleLeaseError):
    """A role lease was asked to act as a purpose authority."""

    code = "purpose_authority_not_role_authority"


class RoleClass(StrEnum):
    """The two intentionally different role time scales."""

    SUBJECT_ROLE = "subject_role"
    TASK_ROLE = "task_role"


class HolderKind(StrEnum):
    """Principal kinds that may hold a bounded role."""

    ENGRAM = "engram"
    WORKER = "worker"
    USER = "user"


class RoleLeaseStatus(StrEnum):
    """Durable role lifecycle states."""

    REQUESTED = "requested"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    RELEASED = "released"
    EXPIRED = "expired"
    REVOKED = "revoked"


class RoleEvidenceClass(StrEnum):
    """Evidence labels; this module remains unverified by construction."""

    CONTRACT_ONLY = CONTRACT_ONLY
    LIVE_GATE_UNVERIFIED = LIVE_GATE_UNVERIFIED
    LIVE = LIVE


class RoleRenewalEvidence(StrEnum):
    """Evidence that may justify creating a successor lease."""

    CONTROL_ONLY = "CONTROL_ONLY"
    LIVE_EXTERNAL_RESULT = "LIVE_EXTERNAL_RESULT"
    SUBJECT_REFLECTION = "SUBJECT_REFLECTION"
    USER_TASK_CONTINUATION = "USER_TASK_CONTINUATION"


class RoleObligationKind(StrEnum):
    """Explicit responsibility carried by a voluntarily accepted role."""

    DIRECT_OUTPUT = "direct_output"


class RoleOutputKind(StrEnum):
    """Production receipts that may satisfy a direct-output obligation."""

    WORKSPACE_CHECKPOINT = "workspace_checkpoint"
    HABITAT_EFFECT = "habitat_effect"


class RoleContributionKind(StrEnum):
    """Typed role activity; only ``DIRECT_OUTPUT`` may satisfy renewal."""

    DIRECT_OUTPUT = "direct_output"
    COORDINATION = "coordination"


class RoleContributionEvidence(StrEnum):
    """Evidence strength is supplied by the production mutation owner."""

    CONTROL_ONLY = "CONTROL_ONLY"
    LIVE_WORKSPACE_CHECKPOINTED = "LIVE_WORKSPACE_CHECKPOINTED"
    LIVE_HABITAT_EFFECT = "LIVE_HABITAT_EFFECT"


def _identifier(value: Any, field_name: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or value != value.strip() or not value:
        raise RoleLeaseValidationError(
            f"{field_name} must be a bounded identifier without surrounding whitespace"
        )
    if _IDENTIFIER_RE.fullmatch(value) is None:
        raise RoleLeaseValidationError(
            f"{field_name} must contain only safe identifier characters"
        )
    return value


def _role_label(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RoleLeaseValidationError("role_label must be non-empty text")
    normalized = value.strip()
    if "\x00" in normalized or any(char in normalized for char in "\r\n"):
        raise RoleLeaseValidationError("role_label contains a forbidden control character")
    if len(normalized.encode("utf-8")) > _ROLE_LABEL_MAX_BYTES:
        raise RoleLeaseValidationError(
            f"role_label must be at most {_ROLE_LABEL_MAX_BYTES} UTF-8 bytes"
        )
    return normalized


def _epoch(value: Any, field_name: str = "epoch") -> int:
    if type(value) is not int or value < 1:
        raise RoleLeaseValidationError(f"{field_name} must be an integer >= 1")
    return value


def _nonnegative_count(value: Any, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise RoleLeaseValidationError(f"{field_name} must be an integer >= 0")
    return value


def _enum(value: Any, enum_type: type[StrEnum], field_name: str) -> StrEnum:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        expected = ", ".join(item.value for item in enum_type)
        raise RoleLeaseValidationError(
            f"{field_name} must be one of: {expected}"
        ) from exc


def _utc(value: Any, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise RoleLeaseValidationError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise RoleLeaseValidationError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value, "timestamp").isoformat()


def _parse_timestamp(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise RoleLeaseStorageError(f"stored {field_name} is not text")
    try:
        return _utc(datetime.fromisoformat(value), field_name)
    except (TypeError, ValueError) as exc:
        raise RoleLeaseStorageError(f"stored {field_name} is not a valid UTC timestamp") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _receipt_attestation(
    *,
    world_id: str,
    holder_kind: HolderKind,
    holder_id: str,
    source_turn_id: str,
    output_kind: RoleOutputKind,
    evidence_event_id: str,
    evidence_class: RoleContributionEvidence,
    before_digest: str | None,
    after_digest: str,
    produced_at: datetime,
) -> str:
    payload = {
        "world_id": world_id,
        "holder_kind": holder_kind.value,
        "holder_id": holder_id,
        "source_turn_id": source_turn_id,
        "output_kind": output_kind.value,
        "evidence_event_id": evidence_event_id,
        "evidence_class": evidence_class.value,
        "before_digest": before_digest,
        "after_digest": after_digest,
        "produced_at": _timestamp(produced_at),
    }
    return hmac.new(
        _VERIFIED_RECEIPT_KEY,
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _bounded_text_token(value: Any, field_name: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or value != value.strip() or not value:
        raise RoleLeaseValidationError(f"{field_name} must be non-empty text")
    if len(value.encode("utf-8")) > 256:
        raise RoleLeaseValidationError(f"{field_name} is too long")
    if "\x00" in value:
        raise RoleLeaseValidationError(f"{field_name} contains a NUL")
    return value


@dataclass(frozen=True, slots=True)
class RoleScope:
    """Explicit scope selectors; no wildcard or world-wide selector exists."""

    center_ids: tuple[str, ...] = ()
    lineage_id: str | None = None
    task_front_id: str | None = None
    action_scope: str | None = None

    def __post_init__(self) -> None:
        raw_centers: Iterable[Any]
        if self.center_ids is None:  # type: ignore[comparison-overlap]
            raw_centers = ()
        elif isinstance(self.center_ids, str):
            raise RoleLeaseValidationError("center_ids must be a sequence of identifiers")
        else:
            raw_centers = self.center_ids
        centers = tuple(
            sorted(
                {
                    _scope_selector(item, "center_id")
                    for item in raw_centers
                }
            )
        )
        lineage = _scope_selector(self.lineage_id, "scope.lineage_id", optional=True)
        task_front = _scope_selector(
            self.task_front_id, "task_front_id", optional=True
        )
        action = _scope_selector(self.action_scope, "action_scope", optional=True)
        if not any((centers, lineage, task_front, action)):
            raise RoleLeaseScopeError("role scope must bind a center, lineage, task front or action")
        object.__setattr__(self, "center_ids", centers)
        object.__setattr__(self, "lineage_id", lineage)
        object.__setattr__(self, "task_front_id", task_front)
        object.__setattr__(self, "action_scope", action)

    def to_dict(self) -> dict[str, Any]:
        """Return a bounded observation projection, never a prompt fragment."""

        return {
            "center_ids": list(self.center_ids),
            "lineage_id": self.lineage_id,
            "task_front_id": self.task_front_id,
            "action_scope": self.action_scope,
        }

    def canonical(self) -> dict[str, Any]:
        return self.to_dict()

    @property
    def digest(self) -> str:
        return _digest(self.canonical())

    @classmethod
    def from_dict(cls, value: Any) -> "RoleScope":
        if not isinstance(value, dict):
            raise RoleLeaseStorageError("stored role scope is not an object")
        unknown = set(value) - {"center_ids", "lineage_id", "task_front_id", "action_scope"}
        if unknown:
            raise RoleLeaseStorageError("stored role scope contains unknown fields")
        centers = value.get("center_ids", ())
        if not isinstance(centers, list):
            raise RoleLeaseStorageError("stored role scope center_ids is not a list")
        return cls(
            center_ids=tuple(centers),
            lineage_id=value.get("lineage_id"),
            task_front_id=value.get("task_front_id"),
            action_scope=value.get("action_scope"),
        )

    def matches(self, other: "RoleScope") -> bool:
        if not isinstance(other, RoleScope):
            return False
        return self.canonical() == other.canonical()


@dataclass(frozen=True, slots=True)
class RoleObligation:
    """A bounded, opt-in execution obligation carried by one role version.

    This is deliberately not a productivity score.  It answers one narrow
    renewal question: did this holder produce at least a declared number of
    independently receipted world changes, and did it return from any long
    coordination-only streak before asking to retain the role?
    """

    kind: RoleObligationKind = RoleObligationKind.DIRECT_OUTPUT
    minimum_direct_outputs: int = 1
    max_consecutive_coordination: int = 3
    accepted_output_kinds: tuple[RoleOutputKind, ...] = (
        RoleOutputKind.WORKSPACE_CHECKPOINT,
        RoleOutputKind.HABITAT_EFFECT,
    )

    def __post_init__(self) -> None:
        kind = _enum(self.kind, RoleObligationKind, "obligation.kind")
        minimum = self.minimum_direct_outputs
        maximum_coordination = self.max_consecutive_coordination
        if (
            type(minimum) is not int
            or not 1 <= minimum <= _MAX_DIRECT_OUTPUTS_REQUIRED
        ):
            raise RoleLeaseValidationError(
                "obligation.minimum_direct_outputs must be in "
                f"[1, {_MAX_DIRECT_OUTPUTS_REQUIRED}]"
            )
        if (
            type(maximum_coordination) is not int
            or not 0 <= maximum_coordination <= _MAX_CONSECUTIVE_COORDINATION
        ):
            raise RoleLeaseValidationError(
                "obligation.max_consecutive_coordination must be in "
                f"[0, {_MAX_CONSECUTIVE_COORDINATION}]"
            )
        raw_outputs = self.accepted_output_kinds
        if isinstance(raw_outputs, (str, bytes)) or not isinstance(
            raw_outputs, Sequence
        ):
            raise RoleLeaseValidationError(
                "obligation.accepted_output_kinds must be a sequence"
            )
        outputs = tuple(
            sorted(
                {
                    _enum(item, RoleOutputKind, "obligation.accepted_output_kind")
                    for item in raw_outputs
                },
                key=lambda item: item.value,
            )
        )
        if not outputs:
            raise RoleLeaseValidationError(
                "obligation.accepted_output_kinds must not be empty"
            )
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "accepted_output_kinds", outputs)

    def canonical(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "minimum_direct_outputs": self.minimum_direct_outputs,
            "max_consecutive_coordination": self.max_consecutive_coordination,
            "accepted_output_kinds": [
                item.value for item in self.accepted_output_kinds
            ],
        }

    def to_dict(self) -> dict[str, Any]:
        return self.canonical()

    @property
    def digest(self) -> str:
        return _digest(self.canonical())

    @classmethod
    def from_dict(cls, value: Any) -> "RoleObligation":
        if not isinstance(value, dict):
            raise RoleLeaseValidationError("obligation must be an object")
        allowed = {
            "kind",
            "minimum_direct_outputs",
            "max_consecutive_coordination",
            "accepted_output_kinds",
        }
        if set(value).difference(allowed):
            raise RoleLeaseValidationError("obligation contains unknown fields")
        return cls(
            kind=value.get("kind", RoleObligationKind.DIRECT_OUTPUT.value),
            minimum_direct_outputs=value.get("minimum_direct_outputs", 1),
            max_consecutive_coordination=value.get(
                "max_consecutive_coordination", 3
            ),
            accepted_output_kinds=tuple(
                value.get(
                    "accepted_output_kinds",
                    (
                        RoleOutputKind.WORKSPACE_CHECKPOINT.value,
                        RoleOutputKind.HABITAT_EFFECT.value,
                    ),
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class RoleContribution:
    """One append-only, payload-free contribution bound to one cycle."""

    contribution_id: str
    accountability_cycle_id: str
    role_lease_id: str
    role_epoch: int
    cycle_sequence: int
    world_id: str
    holder_kind: HolderKind
    holder_id: str
    contribution_kind: RoleContributionKind
    output_kind: RoleOutputKind | None
    evidence_event_id: str
    source_turn_id: str | None
    evidence_class: RoleContributionEvidence
    created_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "contribution_id": self.contribution_id,
            "accountability_cycle_id": self.accountability_cycle_id,
            "role_lease_id": self.role_lease_id,
            "role_epoch": self.role_epoch,
            "cycle_sequence": self.cycle_sequence,
            "world_id": self.world_id,
            "holder_kind": self.holder_kind.value,
            "holder_id": self.holder_id,
            "contribution_kind": self.contribution_kind.value,
            "output_kind": None if self.output_kind is None else self.output_kind.value,
            "evidence_event_id": self.evidence_event_id,
            "source_turn_id": self.source_turn_id,
            "evidence_class": self.evidence_class.value,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class VerifiedRoleReceipt:
    """Production-owner-verified receipt accepted by the role ledger.

    Instances can only be issued by :class:`RoleReceiptVerifier`; the store
    never accepts raw evidence labels or caller-asserted checkpoint IDs for a
    direct-output contribution.
    """

    world_id: str
    holder_kind: HolderKind
    holder_id: str
    source_turn_id: str
    output_kind: RoleOutputKind
    evidence_event_id: str
    evidence_class: RoleContributionEvidence
    before_digest: str | None
    after_digest: str
    produced_at: datetime
    _attestation: str = field(repr=False, compare=False)
    _seal: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        self.assert_valid()

    def assert_valid(self) -> None:
        if self._seal is not _VERIFIED_RECEIPT_SEAL:
            raise RoleLeaseValidationError(
                "verified role receipts must be issued by RoleReceiptVerifier"
            )
        expected = _receipt_attestation(
            world_id=self.world_id,
            holder_kind=self.holder_kind,
            holder_id=self.holder_id,
            source_turn_id=self.source_turn_id,
            output_kind=self.output_kind,
            evidence_event_id=self.evidence_event_id,
            evidence_class=self.evidence_class,
            before_digest=self.before_digest,
            after_digest=self.after_digest,
            produced_at=self.produced_at,
        )
        if not hmac.compare_digest(self._attestation, expected):
            raise RoleLeaseValidationError(
                "verified role receipt attestation does not match"
            )


class RoleReceiptVerifier:
    """Translate canonical production-owner records into sealed receipts."""

    def __init__(
        self,
        *,
        workspace_resolver: Callable[..., Mapping[str, Any] | None] | None = None,
        habitat_resolver: Callable[..., Mapping[str, Any] | None] | None = None,
    ) -> None:
        if workspace_resolver is not None and not callable(workspace_resolver):
            raise RoleLeaseValidationError("workspace_resolver must be callable")
        if habitat_resolver is not None and not callable(habitat_resolver):
            raise RoleLeaseValidationError("habitat_resolver must be callable")
        self._workspace_resolver = workspace_resolver
        self._habitat_resolver = habitat_resolver

    @staticmethod
    def _issue(
        owner_record: Mapping[str, Any],
        *,
        world_id: str,
        holder_kind: HolderKind,
        holder_id: str,
        source_turn_id: str,
        output_kind: RoleOutputKind,
        event_field: str,
        after_field: str,
        produced_field: str,
        evidence_class: RoleContributionEvidence,
    ) -> VerifiedRoleReceipt:
        event_id = _identifier(owner_record.get(event_field), event_field)
        world = _identifier(world_id, "world_id")
        holder = _identifier(holder_id, "holder_id")
        turn = _identifier(source_turn_id, "source_turn_id")
        assert event_id is not None and world is not None
        assert holder is not None and turn is not None
        if "before_digest" not in owner_record or after_field not in owner_record:
            raise RoleLeaseValidationError(
                "production receipt is missing digest evidence"
            )
        before = owner_record.get("before_digest")
        after = owner_record.get(after_field)
        if before is not None:
            before = _bounded_text_token(before, "before_digest")
        after = _bounded_text_token(after, "after_digest")
        assert after is not None
        if before == after:
            raise RoleLeaseValidationError(
                "production receipt does not describe a world change"
            )
        produced_value = owner_record.get(produced_field)
        try:
            produced_at = _parse_timestamp(produced_value, produced_field)
        except RoleLeaseStorageError as exc:
            raise RoleLeaseValidationError(
                "production receipt has no valid owner timestamp"
            ) from exc
        attestation = _receipt_attestation(
            world_id=world,
            holder_kind=holder_kind,
            holder_id=holder,
            source_turn_id=turn,
            output_kind=output_kind,
            evidence_event_id=event_id,
            evidence_class=evidence_class,
            before_digest=before,
            after_digest=after,
            produced_at=produced_at,
        )
        return VerifiedRoleReceipt(
            world_id=world,
            holder_kind=holder_kind,
            holder_id=holder,
            source_turn_id=turn,
            output_kind=output_kind,
            evidence_event_id=event_id,
            evidence_class=evidence_class,
            before_digest=before,
            after_digest=after,
            produced_at=produced_at,
            _attestation=attestation,
            _seal=_VERIFIED_RECEIPT_SEAL,
        )

    def verify_workspace(
        self,
        result: Mapping[str, Any],
        *,
        world_id: str,
        holder_kind: HolderKind,
        holder_id: str,
        source_turn_id: str,
    ) -> VerifiedRoleReceipt:
        resolver = self._workspace_resolver
        if resolver is None:
            raise RoleLeaseValidationError(
                "workspace production receipt resolver is unavailable"
            )
        owner_record = resolver(
            result,
            world_id=world_id,
            engram_id=holder_id,
            turn_id=source_turn_id,
        )
        if not isinstance(owner_record, Mapping):
            raise RoleLeaseValidationError(
                "workspace production owner did not verify the receipt"
            )
        return self._issue(
            owner_record,
            world_id=world_id,
            holder_kind=holder_kind,
            holder_id=holder_id,
            source_turn_id=source_turn_id,
            output_kind=RoleOutputKind.WORKSPACE_CHECKPOINT,
            event_field="checkpoint_id",
            after_field="post_digest",
            produced_field="produced_at",
            evidence_class=RoleContributionEvidence.LIVE_WORKSPACE_CHECKPOINTED,
        )

    def verify_habitat(
        self,
        receipt: Mapping[str, Any],
        *,
        world_id: str,
        holder_kind: HolderKind,
        holder_id: str,
        source_turn_id: str,
        expected_correlation_id: str,
    ) -> VerifiedRoleReceipt:
        resolver = self._habitat_resolver
        if resolver is None:
            raise RoleLeaseValidationError(
                "Habitat production receipt resolver is unavailable"
            )
        owner_record = resolver(receipt)
        if not isinstance(owner_record, Mapping):
            raise RoleLeaseValidationError(
                "Habitat production owner did not verify the receipt"
            )
        correlation = _identifier(
            expected_correlation_id,
            "expected_correlation_id",
        )
        if owner_record.get("correlation_id") != correlation:
            raise RoleLeaseValidationError(
                "Habitat production receipt belongs to another action"
            )
        return self._issue(
            owner_record,
            world_id=world_id,
            holder_kind=holder_kind,
            holder_id=holder_id,
            source_turn_id=source_turn_id,
            output_kind=RoleOutputKind.HABITAT_EFFECT,
            event_field="journal_effect_id",
            after_field="after_digest",
            produced_field="produced_at",
            evidence_class=RoleContributionEvidence.LIVE_HABITAT_EFFECT,
        )


@dataclass(frozen=True, slots=True)
class RoleContributionSummary:
    """Read model used by the holder and the renewal gate."""

    role_lease_id: str
    accountability_cycle_id: str | None
    role_epoch: int
    direct_output_count: int
    coordination_count: int
    consecutive_coordination: int
    last_direct_output_event_id: str | None
    last_contribution_at: datetime | None
    renewal_eligible: bool
    reason_code: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "role_lease_id": self.role_lease_id,
            "accountability_cycle_id": self.accountability_cycle_id,
            "role_epoch": self.role_epoch,
            "direct_output_count": self.direct_output_count,
            "coordination_count": self.coordination_count,
            "consecutive_coordination": self.consecutive_coordination,
            "last_direct_output_event_id": self.last_direct_output_event_id,
            "last_contribution_at": (
                None
                if self.last_contribution_at is None
                else self.last_contribution_at.isoformat()
            ),
            "renewal_eligible": self.renewal_eligible,
            "reason_code": self.reason_code,
        }


def _scope_selector(value: Any, field_name: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    value = _identifier(value, field_name)
    assert value is not None
    if _WILDCARD_RE.search(value) or value.casefold() in {"world=", "world=*", "world/*"}:
        raise RoleLeaseScopeError("role scope cannot contain a wildcard or world-wide selector")
    return value


@dataclass(frozen=True, slots=True)
class RuntimeLeaseProof:
    """A caller-supplied Runtime owner/epoch proof, not a Runtime lease."""

    world_id: str
    owner_id: str
    epoch: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "world_id", _identifier(self.world_id, "world_id"))
        object.__setattr__(self, "owner_id", _identifier(self.owner_id, "runtime_owner_id"))
        object.__setattr__(self, "epoch", _epoch(self.epoch, "runtime_epoch"))

    @property
    def runtime_owner_id(self) -> str:
        return self.owner_id

    @property
    def runtime_epoch(self) -> int:
        return self.epoch

    def to_dict(self) -> dict[str, Any]:
        return {
            "world_id": self.world_id,
            "runtime_owner_id": self.owner_id,
            "runtime_epoch": self.epoch,
        }


def _runtime_proof(
    *,
    world_id: str,
    runtime_owner_id: str | None,
    runtime_epoch: int | None,
    runtime: RuntimeLeaseProof | None,
) -> RuntimeLeaseProof:
    if runtime is not None and not isinstance(runtime, RuntimeLeaseProof):
        raise RoleLeaseValidationError("runtime must be a RuntimeLeaseProof")
    if runtime is not None:
        if runtime.world_id != world_id:
            raise RuntimeLeaseFenceError("Runtime proof belongs to a different world")
        if runtime_owner_id is not None and runtime_owner_id != runtime.owner_id:
            raise RoleLeaseConflictError("runtime owner arguments disagree with the proof")
        if runtime_epoch is not None and runtime_epoch != runtime.epoch:
            raise RoleLeaseConflictError("runtime epoch arguments disagree with the proof")
        return runtime
    if runtime_owner_id is None or runtime_epoch is None:
        raise RoleLeaseValidationError(
            "runtime_owner_id and runtime_epoch are required as a Runtime proof"
        )
    return RuntimeLeaseProof(world_id, runtime_owner_id, runtime_epoch)


def _validate_role_binding(
    role_class: RoleClass,
    holder_kind: HolderKind,
    lineage_id: str | None,
    scope: RoleScope,
) -> None:
    if role_class is RoleClass.SUBJECT_ROLE:
        if holder_kind is not HolderKind.ENGRAM:
            raise RoleLeaseValidationError("subject_role may only be held by an engram")
        if lineage_id is None:
            raise RoleLeaseValidationError("subject_role requires a lineage_id")
        if not scope.center_ids:
            raise RoleLeaseScopeError("subject_role must bind one or more center_ids")
        if scope.task_front_id is not None or scope.action_scope is not None:
            raise RoleLeaseScopeError("subject_role cannot be a task/action scope")
        if scope.lineage_id is not None and scope.lineage_id != lineage_id:
            raise RoleLeaseScopeError("subject role scope lineage does not match holder lineage")
    else:
        if scope.task_front_id is None and scope.action_scope is None:
            raise RoleLeaseScopeError("task_role must bind a task_front_id or action_scope")
        if holder_kind in {HolderKind.WORKER, HolderKind.USER} and lineage_id is not None:
            raise RoleLeaseValidationError(
                "worker/user task roles cannot borrow an Engram lineage"
            )
        if scope.lineage_id is not None and lineage_id != scope.lineage_id:
            raise RoleLeaseScopeError("task role scope lineage does not match holder lineage")


def _issuer(
    issuer_kind: Any,
    issuer_id: Any,
    *,
    holder_kind: HolderKind,
    holder_id: str,
) -> tuple[str, str]:
    kind = _identifier(issuer_kind, "issuer_kind")
    identifier = _identifier(issuer_id, "issuer_id")
    assert kind is not None and identifier is not None
    if kind == holder_kind.value and identifier == holder_id:
        raise PurposeAuthorityError(
            "a holder cannot self-issue role authority; purpose is not a role issuer"
        )
    return kind, identifier


def _role_ttl(role_class: RoleClass, ttl_seconds: Any | None) -> float:
    default = (
        _DEFAULT_SUBJECT_TTL_SECONDS
        if role_class is RoleClass.SUBJECT_ROLE
        else _DEFAULT_TASK_TTL_SECONDS
    )
    maximum = (
        _MAX_SUBJECT_TTL_SECONDS
        if role_class is RoleClass.SUBJECT_ROLE
        else _MAX_TASK_TTL_SECONDS
    )
    value = default if ttl_seconds is None else ttl_seconds
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RoleLeaseValidationError("ttl_seconds must be a finite positive number")
    value = float(value)
    if not math.isfinite(value) or value <= 0 or value > maximum:
        raise RoleLeaseValidationError(
            f"ttl_seconds must be in (0, {maximum:g}] for {role_class.value}"
        )
    return value


def _times(
    role_class: RoleClass,
    *,
    valid_from: datetime,
    ttl_seconds: Any | None,
    expires_at: datetime | None,
    renew_after: datetime | None,
) -> tuple[datetime, datetime, datetime]:
    start = _utc(valid_from, "valid_from")
    if expires_at is None:
        end = start + timedelta(seconds=_role_ttl(role_class, ttl_seconds))
    else:
        if ttl_seconds is not None:
            raise RoleLeaseValidationError("ttl_seconds and expires_at are mutually exclusive")
        end = _utc(expires_at, "expires_at")
        maximum = (
            _MAX_SUBJECT_TTL_SECONDS
            if role_class is RoleClass.SUBJECT_ROLE
            else _MAX_TASK_TTL_SECONDS
        )
        duration = (end - start).total_seconds()
        if duration <= 0 or duration > maximum:
            raise RoleLeaseValidationError(
                f"expires_at must be in (valid_from, valid_from + {maximum:g}s]"
            )
    renewal = (
        start + timedelta(seconds=((end - start).total_seconds() * 2.0 / 3.0))
        if renew_after is None
        else _utc(renew_after, "renew_after")
    )
    if not start < renewal < end:
        raise RoleLeaseValidationError("renew_after must be strictly between valid_from and expires_at")
    return start, end, renewal


def _scope_key(
    *, world_id: str, role_class: RoleClass, lineage_id: str | None, scope: RoleScope
) -> str:
    # Subject lineage is part of subject-role identity even when callers use
    # a center-only RoleScope.  A task role's outer lineage is holder
    # metadata, not a new action scope: a worker -> Engram handoff must still
    # advance the same task-role epoch.
    scope_lineage = lineage_id if role_class is RoleClass.SUBJECT_ROLE else None
    return _digest(
        {
            "world_id": world_id,
            "role_class": role_class.value,
            "lineage_id": scope_lineage,
            "scope_digest": scope.digest,
        }
    )


@dataclass(frozen=True, slots=True)
class HarnessAuthority:
    """The minimum role/runtime proof passed to an adapter.

    Purpose is intentionally absent.  A caller may separately read a
    purpose revision, but role authority can never be converted into purpose
    authority by this module.
    """

    world_id: str
    runtime_owner_id: str
    runtime_epoch: int
    role_lease_id: str
    role_epoch: int
    holder_kind: HolderKind
    holder_id: str
    scope: RoleScope
    evidence_class: RoleEvidenceClass = RoleEvidenceClass.LIVE_GATE_UNVERIFIED

    def to_dict(self) -> dict[str, Any]:
        return {
            "world_id": self.world_id,
            "runtime_owner_id": self.runtime_owner_id,
            "runtime_epoch": self.runtime_epoch,
            "role_lease_id": self.role_lease_id,
            "role_epoch": self.role_epoch,
            "holder_kind": self.holder_kind.value,
            "holder_id": self.holder_id,
            "scope": self.scope.to_dict(),
            "evidence_class": self.evidence_class.value,
        }


@dataclass(frozen=True, slots=True)
class RoleLease:
    """Immutable observation of one role lease version."""

    role_lease_id: str
    world_id: str
    lineage_id: str | None
    holder_kind: HolderKind
    holder_id: str
    role_class: RoleClass
    role_label: str
    scope: RoleScope
    obligation: RoleObligation | None
    accountability_cycle_id: str | None
    purpose_revision_id: str | None
    issuer_kind: str
    issuer_id: str
    role_epoch: int
    runtime_owner_id: str
    runtime_epoch: int
    valid_from: datetime
    expires_at: datetime
    renew_after: datetime
    status: RoleLeaseStatus
    predecessor_lease_id: str | None
    renewal_count: int
    last_evidence_event_id: str | None
    created_at: datetime
    updated_at: datetime
    released_at: datetime | None
    evidence_class: RoleEvidenceClass = RoleEvidenceClass.LIVE_GATE_UNVERIFIED

    @property
    def is_subject_role(self) -> bool:
        return self.role_class is RoleClass.SUBJECT_ROLE

    @property
    def is_task_role(self) -> bool:
        return self.role_class is RoleClass.TASK_ROLE

    @property
    def is_currently_active(self) -> bool:
        return self.status is RoleLeaseStatus.ACTIVE

    def is_valid_at(self, now: datetime) -> bool:
        current = _utc(now, "now")
        return self.status is RoleLeaseStatus.ACTIVE and current < self.expires_at

    def to_dict(self) -> dict[str, Any]:
        """Return an observation projection without purpose content or prompt data."""

        return {
            "role_lease_id": self.role_lease_id,
            "world_id": self.world_id,
            "lineage_id": self.lineage_id,
            "holder_kind": self.holder_kind.value,
            "holder_id": self.holder_id,
            "role_class": self.role_class.value,
            "role_label": self.role_label,
            "scope": self.scope.to_dict(),
            "obligation": (
                None if self.obligation is None else self.obligation.to_dict()
            ),
            "accountability_cycle_id": self.accountability_cycle_id,
            "purpose_revision_id": self.purpose_revision_id,
            "issuer_kind": self.issuer_kind,
            "issuer_id": self.issuer_id,
            "role_epoch": self.role_epoch,
            "runtime_owner_id": self.runtime_owner_id,
            "runtime_epoch": self.runtime_epoch,
            "valid_from": self.valid_from.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "renew_after": self.renew_after.isoformat(),
            "status": self.status.value,
            "predecessor_lease_id": self.predecessor_lease_id,
            "renewal_count": self.renewal_count,
            "last_evidence_event_id": self.last_evidence_event_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "released_at": None if self.released_at is None else self.released_at.isoformat(),
            "evidence_class": self.evidence_class.value,
        }


@dataclass(frozen=True, slots=True)
class RoleAccountabilityObservation:
    """One role and its contribution gate from a single read snapshot.

    ``effective_status`` may report an elapsed lease as expired without
    mutating the durable row.  The observation deliberately carries no
    contribution payload, prompt, path or process detail.
    """

    role: RoleLease
    effective_status: RoleLeaseStatus
    contribution_summary: RoleContributionSummary
    contribution_evidence_classes: tuple[RoleContributionEvidence, ...]


@dataclass(frozen=True, slots=True)
class RoleAccountabilitySnapshot:
    """Holder-bounded, transactionally consistent read model."""

    world_id: str
    holder_kind: HolderKind
    holder_id: str
    observed_at: datetime
    roles: tuple[RoleAccountabilityObservation, ...]


class RoleLeaseStore:
    """Thread-safe and restartable SQLite role lease domain.

    The store keeps one connection per instance and serializes its own
    threads.  SQLite's ``BEGIN IMMEDIATE`` also fences separate store
    instances sharing the same file.  Every mutation that can affect a
    holder or epoch happens in one transaction.
    """

    evidence_class = RoleEvidenceClass.LIVE_GATE_UNVERIFIED

    def __init__(
        self,
        db_path: str | Path = ":memory:",
        *,
        clock: Callable[[], datetime] | None = None,
        publication_permit: RuntimePublicationPermit | None = None,
    ) -> None:
        from pulse_system.core.runtime.publication import RuntimePublicationPermit

        self._lock = threading.RLock()
        self._closed = False
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        if publication_permit is not None and not isinstance(
            publication_permit,
            RuntimePublicationPermit,
        ):
            raise RoleLeaseValidationError(
                "publication_permit must be a RuntimePublicationPermit or null"
            )
        self._publication_permit = publication_permit
        if isinstance(db_path, Path):
            path = str(db_path)
        elif isinstance(db_path, str):
            path = db_path
        else:
            raise RoleLeaseValidationError("db_path must be a path or ':memory:'")
        if path != ":memory:":
            parent = Path(path).expanduser().parent
            if not parent.exists():
                raise RoleLeaseStorageError("role lease database parent directory does not exist")
        self.db_path = path
        self._conn: sqlite3.Connection | None = None
        try:
            self._conn = sqlite3.connect(
                path,
                timeout=5.0,
                isolation_level=None,
                check_same_thread=False,
            )
            self._conn.row_factory = sqlite3.Row
            guard = (
                nullcontext()
                if self._publication_permit is None
                else self._publication_permit.transaction_guard()
            )
            with guard:
                self._conn.execute("PRAGMA journal_mode=WAL")
                self._conn.execute("PRAGMA synchronous=FULL")
                self._conn.execute("PRAGMA foreign_keys=ON")
                self._conn.execute("PRAGMA busy_timeout=5000")
                self._init_schema()
        except (sqlite3.Error, OSError) as exc:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
            raise RoleLeaseStorageError("role lease database could not be opened") from exc

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS role_scope_counters (
                scope_key TEXT PRIMARY KEY,
                last_epoch INTEGER NOT NULL CHECK(last_epoch >= 1)
            );

            CREATE TABLE IF NOT EXISTS role_leases (
                role_lease_id TEXT PRIMARY KEY,
                world_id TEXT NOT NULL,
                lineage_id TEXT,
                holder_kind TEXT NOT NULL CHECK(holder_kind IN ('engram', 'worker', 'user')),
                holder_id TEXT NOT NULL,
                role_class TEXT NOT NULL CHECK(role_class IN ('subject_role', 'task_role')),
                role_label TEXT NOT NULL,
                scope_json TEXT NOT NULL,
                scope_digest TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                purpose_revision_id TEXT,
                issuer_kind TEXT NOT NULL,
                issuer_id TEXT NOT NULL,
                role_epoch INTEGER NOT NULL CHECK(role_epoch >= 1),
                runtime_owner_id TEXT NOT NULL,
                runtime_epoch INTEGER NOT NULL CHECK(runtime_epoch >= 1),
                valid_from TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                renew_after TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN (
                    'requested', 'active', 'suspended', 'released', 'expired', 'revoked'
                )),
                predecessor_lease_id TEXT,
                renewal_count INTEGER NOT NULL CHECK(renewal_count >= 0),
                last_evidence_event_id TEXT,
                evidence_class TEXT NOT NULL CHECK(evidence_class IN (
                    'CONTRACT_ONLY', 'LIVE_GATE_UNVERIFIED', 'LIVE'
                )),
                handoff_suspended INTEGER NOT NULL DEFAULT 0
                    CHECK(handoff_suspended IN (0, 1)),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                released_at TEXT,
                UNIQUE(scope_key, role_epoch),
                CHECK(expires_at > valid_from),
                CHECK(renew_after > valid_from AND renew_after < expires_at),
                CHECK(
                    role_class <> 'subject_role'
                    OR (holder_kind = 'engram' AND lineage_id IS NOT NULL)
                )
            );

            CREATE TABLE IF NOT EXISTS role_accountability_cycles (
                accountability_cycle_id TEXT PRIMARY KEY,
                world_id TEXT NOT NULL,
                opened_role_lease_id TEXT NOT NULL REFERENCES role_leases(role_lease_id),
                obligation_digest TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS role_obligations (
                role_lease_id TEXT PRIMARY KEY REFERENCES role_leases(role_lease_id),
                accountability_cycle_id TEXT NOT NULL
                    REFERENCES role_accountability_cycles(accountability_cycle_id),
                obligation_json TEXT NOT NULL,
                obligation_digest TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS role_contributions (
                contribution_id TEXT PRIMARY KEY,
                accountability_cycle_id TEXT NOT NULL
                    REFERENCES role_accountability_cycles(accountability_cycle_id),
                role_lease_id TEXT NOT NULL REFERENCES role_leases(role_lease_id),
                role_epoch INTEGER NOT NULL CHECK(role_epoch >= 1),
                cycle_sequence INTEGER NOT NULL CHECK(cycle_sequence >= 1),
                world_id TEXT NOT NULL,
                holder_kind TEXT NOT NULL CHECK(holder_kind IN ('engram', 'worker', 'user')),
                holder_id TEXT NOT NULL,
                contribution_kind TEXT NOT NULL CHECK(contribution_kind IN (
                    'direct_output', 'coordination'
                )),
                output_kind TEXT CHECK(output_kind IN (
                    'workspace_checkpoint', 'habitat_effect'
                )),
                evidence_event_id TEXT NOT NULL,
                source_turn_id TEXT,
                evidence_class TEXT NOT NULL CHECK(evidence_class IN (
                    'CONTROL_ONLY', 'LIVE_WORKSPACE_CHECKPOINTED',
                    'LIVE_HABITAT_EFFECT'
                )),
                created_at TEXT NOT NULL,
                UNIQUE(accountability_cycle_id, evidence_event_id),
                UNIQUE(accountability_cycle_id, cycle_sequence),
                CHECK(
                    (contribution_kind = 'direct_output' AND output_kind IS NOT NULL
                        AND evidence_class <> 'CONTROL_ONLY')
                    OR (contribution_kind = 'coordination' AND output_kind IS NULL
                        AND evidence_class = 'CONTROL_ONLY')
                )
            );

            CREATE UNIQUE INDEX IF NOT EXISTS role_leases_one_nonterminal_per_scope
                ON role_leases(scope_key)
                WHERE status IN ('requested', 'active', 'suspended');

            CREATE INDEX IF NOT EXISTS role_leases_world_status
                ON role_leases(world_id, status, updated_at);
            CREATE INDEX IF NOT EXISTS role_leases_lineage
                ON role_leases(world_id, lineage_id, status);
            CREATE INDEX IF NOT EXISTS role_leases_holder
                ON role_leases(world_id, holder_kind, holder_id, status);
            CREATE INDEX IF NOT EXISTS role_leases_predecessor
                ON role_leases(predecessor_lease_id);
            CREATE UNIQUE INDEX IF NOT EXISTS role_direct_output_receipt_claim_once
                ON role_contributions(world_id, output_kind, evidence_event_id)
                WHERE contribution_kind = 'direct_output';
            CREATE INDEX IF NOT EXISTS role_contributions_cycle_created
                ON role_contributions(accountability_cycle_id, cycle_sequence);

            CREATE TRIGGER IF NOT EXISTS role_accountability_cycles_immutable_update
            BEFORE UPDATE ON role_accountability_cycles
            BEGIN
                SELECT RAISE(ABORT, 'role accountability cycles are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS role_accountability_cycles_immutable_delete
            BEFORE DELETE ON role_accountability_cycles
            BEGIN
                SELECT RAISE(ABORT, 'role accountability cycles are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS role_obligations_immutable_update
            BEFORE UPDATE ON role_obligations
            BEGIN
                SELECT RAISE(ABORT, 'role obligations are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS role_obligations_immutable_delete
            BEFORE DELETE ON role_obligations
            BEGIN
                SELECT RAISE(ABORT, 'role obligations are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS role_contributions_immutable_update
            BEFORE UPDATE ON role_contributions
            BEGIN
                SELECT RAISE(ABORT, 'role contributions are append-only');
            END;

            CREATE TRIGGER IF NOT EXISTS role_contributions_immutable_delete
            BEFORE DELETE ON role_contributions
            BEGIN
                SELECT RAISE(ABORT, 'role contributions are append-only');
            END;
            """
        )
        columns = {
            str(row["name"])
            for row in self._conn.execute("PRAGMA table_info(role_leases)")
        }
        if "handoff_suspended" not in columns:
            self._conn.execute(
                "ALTER TABLE role_leases ADD COLUMN handoff_suspended "
                "INTEGER NOT NULL DEFAULT 0 "
                "CHECK(handoff_suspended IN (0, 1))"
            )
        # Older databases may contain an earlier trigger definition.  Keep the
        # role row immutable while the separate obligation row has its own
        # append-only trigger above.
        self._conn.executescript(
            """
            DROP TRIGGER IF EXISTS role_leases_immutable_fields;
            CREATE TRIGGER role_leases_immutable_fields
            BEFORE UPDATE OF role_lease_id, world_id, lineage_id, holder_kind,
                holder_id, role_class, role_label, scope_json, scope_digest,
                scope_key, purpose_revision_id, issuer_kind, issuer_id,
                role_epoch, runtime_owner_id, runtime_epoch, valid_from,
                expires_at, renew_after, predecessor_lease_id, renewal_count,
                created_at, evidence_class
            ON role_leases
            BEGIN
                SELECT RAISE(ABORT, 'role lease immutable fields cannot change');
            END;
            """
        )

    def _ensure_open(self) -> None:
        if self._closed:
            raise RoleLeaseStorageError("role lease store is closed")

    @contextmanager
    def _write_transaction(
        self,
        *,
        bootstrap_permit: RuntimeBootstrapPermit | None = None,
        recovery_permit: RuntimeRecoveryPermit | None = None,
    ) -> Iterator[sqlite3.Connection]:
        from pulse_system.core.runtime.publication import (
            RuntimeBootstrapPermit,
            RuntimeRecoveryPermit,
        )

        if bootstrap_permit is not None and not isinstance(
            bootstrap_permit,
            RuntimeBootstrapPermit,
        ):
            raise RoleLeaseValidationError(
                "bootstrap_permit must be a RuntimeBootstrapPermit or null"
            )
        if recovery_permit is not None and not isinstance(
            recovery_permit,
            RuntimeRecoveryPermit,
        ):
            raise RoleLeaseValidationError(
                "recovery_permit must be a RuntimeRecoveryPermit or null"
            )
        if bootstrap_permit is not None and recovery_permit is not None:
            raise RoleLeaseValidationError(
                "bootstrap and shutdown recovery permits are mutually exclusive"
            )
        permit = bootstrap_permit or recovery_permit or self._publication_permit
        guard = nullcontext() if permit is None else permit.transaction_guard()
        with self._lock:
            self._ensure_open()
            with guard:
                try:
                    self._conn.execute("BEGIN IMMEDIATE")
                except sqlite3.Error as exc:
                    raise RoleLeaseStorageError("could not begin role lease transaction") from exc
                try:
                    yield self._conn
                    self._conn.execute("COMMIT")
                except BaseException:
                    try:
                        self._conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    raise

    @contextmanager
    def _read_transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._ensure_open()
            try:
                self._conn.execute("BEGIN")
            except sqlite3.Error as exc:
                raise RoleLeaseStorageError("could not begin role lease read") from exc
            try:
                yield self._conn
                self._conn.execute("COMMIT")
            except BaseException:
                try:
                    self._conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                try:
                    self._conn.close()
                finally:
                    self._closed = True

    def __enter__(self) -> "RoleLeaseStore":
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def _now(self, value: datetime | None) -> datetime:
        return _utc(self._clock() if value is None else value, "now")

    @staticmethod
    def _normalise_role_class(value: Any) -> RoleClass:
        return _enum(value, RoleClass, "role_class")  # type: ignore[return-value]

    @staticmethod
    def _normalise_holder_kind(value: Any) -> HolderKind:
        return _enum(value, HolderKind, "holder_kind")  # type: ignore[return-value]

    @staticmethod
    def _normalise_scope(value: Any) -> RoleScope:
        if not isinstance(value, RoleScope):
            raise RoleLeaseValidationError("scope must be a RoleScope")
        return value

    @staticmethod
    def _normalise_obligation(value: Any) -> RoleObligation | None:
        if value is None:
            return None
        if not isinstance(value, RoleObligation):
            raise RoleLeaseValidationError(
                "obligation must be a RoleObligation or null"
            )
        return value

    @staticmethod
    def _normalise_purpose_reference(value: Any) -> str | None:
        return _identifier(value, "purpose_revision_id", optional=True)

    @staticmethod
    def _new_id(value: Any, field_name: str) -> str:
        candidate = uuid.uuid4().hex if value is None else value
        return _identifier(candidate, field_name)  # type: ignore[return-value]

    @staticmethod
    def _same_immutable(current: RoleLease, candidate: dict[str, Any]) -> bool:
        return (
            current.world_id == candidate["world_id"]
            and current.lineage_id == candidate["lineage_id"]
            and current.holder_kind is candidate["holder_kind"]
            and current.holder_id == candidate["holder_id"]
            and current.role_class is candidate["role_class"]
            and current.role_label == candidate["role_label"]
            and current.scope.matches(candidate["scope"])
            and current.obligation == candidate["obligation"]
            and current.purpose_revision_id == candidate["purpose_revision_id"]
            and current.issuer_kind == candidate["issuer_kind"]
            and current.issuer_id == candidate["issuer_id"]
            and current.role_epoch == candidate["role_epoch"]
            and current.runtime_owner_id == candidate["runtime"].owner_id
            and current.runtime_epoch == candidate["runtime"].epoch
            and current.valid_from == candidate["valid_from"]
            and current.expires_at == candidate["expires_at"]
            and current.renew_after == candidate["renew_after"]
            and current.predecessor_lease_id == candidate["predecessor_lease_id"]
            and current.renewal_count == candidate["renewal_count"]
        )

    @staticmethod
    def _expire_due_locked(conn: sqlite3.Connection, now: datetime) -> None:
        now_text = _timestamp(now)
        conn.execute(
            """UPDATE role_leases
               SET status = 'expired',
                   released_at = expires_at,
                   updated_at = ?
               WHERE status IN ('requested', 'active', 'suspended')
                 AND expires_at <= ?""",
            (now_text, now_text),
        )

    @staticmethod
    def _row(conn: sqlite3.Connection, role_lease_id: str) -> sqlite3.Row | None:
        return conn.execute(
            _ROLE_ROW_SELECT + " WHERE role_leases.role_lease_id = ?",
            (role_lease_id,),
        ).fetchone()

    @staticmethod
    def _allocate_epoch(conn: sqlite3.Connection, scope_key: str) -> int:
        current = conn.execute(
            "SELECT last_epoch FROM role_scope_counters WHERE scope_key = ?",
            (scope_key,),
        ).fetchone()
        if current is None:
            conn.execute(
                "INSERT INTO role_scope_counters(scope_key, last_epoch) VALUES (?, 1)",
                (scope_key,),
            )
            return 1
        next_epoch = int(current[0]) + 1
        conn.execute(
            "UPDATE role_scope_counters SET last_epoch = ? WHERE scope_key = ?",
            (next_epoch, scope_key),
        )
        return next_epoch

    @classmethod
    def _from_row(cls, row: sqlite3.Row) -> RoleLease:
        try:
            scope_data = json.loads(str(row["scope_json"]))
            scope = RoleScope.from_dict(scope_data)
            if scope.digest != row["scope_digest"]:
                raise RoleLeaseStorageError("stored role scope digest does not match")
            expected_scope_key = _scope_key(
                world_id=str(row["world_id"]),
                role_class=RoleClass(str(row["role_class"])),
                lineage_id=None if row["lineage_id"] is None else str(row["lineage_id"]),
                scope=scope,
            )
            if expected_scope_key != row["scope_key"]:
                raise RoleLeaseStorageError("stored role scope key does not match")
            obligation: RoleObligation | None = None
            obligation_json = row["obligation_json"]
            obligation_digest = row["obligation_digest"]
            accountability_cycle = row["accountability_cycle_id"]
            accountability_world = row["accountability_world_id"]
            accountability_digest = row["accountability_obligation_digest"]
            obligation_fields = (
                obligation_json,
                obligation_digest,
                accountability_cycle,
                accountability_world,
                accountability_digest,
            )
            if any(value is None for value in obligation_fields) and not all(
                value is None for value in obligation_fields
            ):
                raise RoleLeaseStorageError("stored role obligation is incomplete")
            if obligation_json is not None:
                obligation = RoleObligation.from_dict(
                    json.loads(str(obligation_json))
                )
                if obligation.digest != str(obligation_digest):
                    raise RoleLeaseStorageError(
                        "stored role obligation digest does not match"
                    )
                if (
                    str(accountability_world) != str(row["world_id"])
                    or str(accountability_digest) != obligation.digest
                ):
                    raise RoleLeaseStorageError(
                        "stored accountability cycle binding does not match"
                    )
            return RoleLease(
                role_lease_id=str(row["role_lease_id"]),
                world_id=str(row["world_id"]),
                lineage_id=None if row["lineage_id"] is None else str(row["lineage_id"]),
                holder_kind=HolderKind(str(row["holder_kind"])),
                holder_id=str(row["holder_id"]),
                role_class=RoleClass(str(row["role_class"])),
                role_label=str(row["role_label"]),
                scope=scope,
                obligation=obligation,
                accountability_cycle_id=(
                    None
                    if accountability_cycle is None
                    else str(accountability_cycle)
                ),
                purpose_revision_id=(
                    None
                    if row["purpose_revision_id"] is None
                    else str(row["purpose_revision_id"])
                ),
                issuer_kind=str(row["issuer_kind"]),
                issuer_id=str(row["issuer_id"]),
                role_epoch=int(row["role_epoch"]),
                runtime_owner_id=str(row["runtime_owner_id"]),
                runtime_epoch=int(row["runtime_epoch"]),
                valid_from=_parse_timestamp(row["valid_from"], "valid_from"),
                expires_at=_parse_timestamp(row["expires_at"], "expires_at"),
                renew_after=_parse_timestamp(row["renew_after"], "renew_after"),
                status=RoleLeaseStatus(str(row["status"])),
                predecessor_lease_id=(
                    None
                    if row["predecessor_lease_id"] is None
                    else str(row["predecessor_lease_id"])
                ),
                renewal_count=int(row["renewal_count"]),
                last_evidence_event_id=(
                    None
                    if row["last_evidence_event_id"] is None
                    else str(row["last_evidence_event_id"])
                ),
                created_at=_parse_timestamp(row["created_at"], "created_at"),
                updated_at=_parse_timestamp(row["updated_at"], "updated_at"),
                released_at=(
                    None
                    if row["released_at"] is None
                    else _parse_timestamp(row["released_at"], "released_at")
                ),
                evidence_class=RoleEvidenceClass(str(row["evidence_class"])),
            )
        except RoleLeaseError:
            raise
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RoleLeaseStorageError("stored role lease row is corrupt") from exc

    @classmethod
    def _insert(
        cls,
        conn: sqlite3.Connection,
        *,
        role_lease_id: str,
        world_id: str,
        lineage_id: str | None,
        holder_kind: HolderKind,
        holder_id: str,
        role_class: RoleClass,
        role_label: str,
        scope: RoleScope,
        obligation: RoleObligation | None,
        purpose_revision_id: str | None,
        issuer_kind: str,
        issuer_id: str,
        role_epoch: int,
        runtime: RuntimeLeaseProof,
        valid_from: datetime,
        expires_at: datetime,
        renew_after: datetime,
        status: RoleLeaseStatus,
        predecessor_lease_id: str | None,
        renewal_count: int,
        last_evidence_event_id: str | None,
        now: datetime,
        accountability_cycle_id: str | None = None,
    ) -> RoleLease:
        scope_key = _scope_key(
            world_id=world_id,
            role_class=role_class,
            lineage_id=lineage_id,
            scope=scope,
        )
        now_text = _timestamp(now)
        try:
            conn.execute(
                """INSERT INTO role_leases (
                    role_lease_id, world_id, lineage_id, holder_kind, holder_id,
                    role_class, role_label, scope_json, scope_digest, scope_key,
                    purpose_revision_id, issuer_kind, issuer_id, role_epoch,
                    runtime_owner_id, runtime_epoch, valid_from, expires_at,
                    renew_after, status, predecessor_lease_id, renewal_count,
                    last_evidence_event_id, evidence_class, created_at,
                    updated_at, released_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                          ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
                (
                    role_lease_id,
                    world_id,
                    lineage_id,
                    holder_kind.value,
                    holder_id,
                    role_class.value,
                    role_label,
                    json.dumps(scope.canonical(), ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                    scope.digest,
                    scope_key,
                    purpose_revision_id,
                    issuer_kind,
                    issuer_id,
                    role_epoch,
                    runtime.owner_id,
                    runtime.epoch,
                    _timestamp(valid_from),
                    _timestamp(expires_at),
                    _timestamp(renew_after),
                    status.value,
                    predecessor_lease_id,
                    renewal_count,
                    last_evidence_event_id,
                    LIVE_GATE_UNVERIFIED,
                    now_text,
                    now_text,
                ),
            )
            if obligation is not None:
                cycle_id = (
                    "accountability_" + uuid.uuid4().hex
                    if accountability_cycle_id is None
                    else _identifier(
                        accountability_cycle_id,
                        "accountability_cycle_id",
                    )
                )
                assert cycle_id is not None
                if accountability_cycle_id is None:
                    conn.execute(
                        """INSERT INTO role_accountability_cycles (
                            accountability_cycle_id, world_id,
                            opened_role_lease_id, obligation_digest, created_at
                        ) VALUES (?, ?, ?, ?, ?)""",
                        (
                            cycle_id,
                            world_id,
                            role_lease_id,
                            obligation.digest,
                            now_text,
                        ),
                    )
                else:
                    cycle = conn.execute(
                        "SELECT world_id, obligation_digest "
                        "FROM role_accountability_cycles "
                        "WHERE accountability_cycle_id = ?",
                        (cycle_id,),
                    ).fetchone()
                    if (
                        cycle is None
                        or str(cycle["world_id"]) != world_id
                        or str(cycle["obligation_digest"]) != obligation.digest
                    ):
                        raise RoleLeaseStorageError(
                            "accountability cycle cannot be rebound to this role"
                        )
                conn.execute(
                    """INSERT INTO role_obligations (
                        role_lease_id, accountability_cycle_id, obligation_json,
                        obligation_digest, created_at
                    ) VALUES (?, ?, ?, ?, ?)""",
                    (
                        role_lease_id,
                        cycle_id,
                        json.dumps(
                            obligation.canonical(),
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        obligation.digest,
                        now_text,
                    ),
                )
            elif accountability_cycle_id is not None:
                raise RoleLeaseStorageError(
                    "an unobligated role cannot bind an accountability cycle"
                )
        except sqlite3.IntegrityError as exc:
            raise RoleLeaseConflictError(
                "role scope already has a nonterminal lease or lease identity collides"
            ) from exc
        row = cls._row(conn, role_lease_id)
        if row is None:
            raise RoleLeaseStorageError("inserted role lease could not be read back")
        return cls._from_row(row)

    @classmethod
    def _candidate(
        cls,
        *,
        world_id: Any,
        lineage_id: Any,
        holder_kind: Any,
        holder_id: Any,
        role_class: Any,
        role_label: Any,
        scope: Any,
        obligation: Any,
        purpose_revision_id: Any,
        issuer_kind: Any,
        issuer_id: Any,
        runtime: RuntimeLeaseProof,
        valid_from: datetime,
        ttl_seconds: Any | None,
        expires_at: datetime | None,
        renew_after: datetime | None,
        role_lease_id: Any,
        predecessor_lease_id: Any,
        renewal_count: Any,
    ) -> dict[str, Any]:
        world = _identifier(world_id, "world_id")
        assert world is not None
        role = cls._normalise_role_class(role_class)
        holder = cls._normalise_holder_kind(holder_kind)
        lineage = _identifier(lineage_id, "lineage_id", optional=True)
        holder_identifier = _identifier(holder_id, "holder_id")
        assert holder_identifier is not None
        role_scope = cls._normalise_scope(scope)
        _validate_role_binding(role, holder, lineage, role_scope)
        label = _role_label(role_label)
        role_obligation = cls._normalise_obligation(obligation)
        purpose = cls._normalise_purpose_reference(purpose_revision_id)
        issuer = _issuer(
            issuer_kind,
            issuer_id,
            holder_kind=holder,
            holder_id=holder_identifier,
        )
        start, end, renewal = _times(
            role,
            valid_from=_utc(valid_from, "valid_from"),
            ttl_seconds=ttl_seconds,
            expires_at=expires_at,
            renew_after=renew_after,
        )
        predecessor = _identifier(
            predecessor_lease_id, "predecessor_lease_id", optional=True
        )
        count = _nonnegative_count(renewal_count, "renewal_count")
        return {
            "role_lease_id": cls._new_id(role_lease_id, "role_lease_id"),
            "world_id": world,
            "lineage_id": lineage,
            "holder_kind": holder,
            "holder_id": holder_identifier,
            "role_class": role,
            "role_label": label,
            "scope": role_scope,
            "obligation": role_obligation,
            "purpose_revision_id": purpose,
            "issuer_kind": issuer[0],
            "issuer_id": issuer[1],
            "runtime": runtime,
            "valid_from": start,
            "expires_at": end,
            "renew_after": renewal,
            "predecessor_lease_id": predecessor,
            "renewal_count": count,
        }

    def _prepare_candidate(
        self,
        *,
        world_id: Any,
        lineage_id: Any,
        holder_kind: Any,
        holder_id: Any,
        role_class: Any,
        role_label: Any,
        scope: Any,
        obligation: Any,
        purpose_revision_id: Any,
        issuer_kind: Any,
        issuer_id: Any,
        runtime_owner_id: str | None,
        runtime_epoch: int | None,
        runtime: RuntimeLeaseProof | None,
        valid_from: datetime | None,
        ttl_seconds: Any | None,
        expires_at: datetime | None,
        renew_after: datetime | None,
        role_lease_id: Any,
        predecessor_lease_id: Any,
        renewal_count: Any,
        now: datetime | None,
    ) -> dict[str, Any]:
        world = _identifier(world_id, "world_id")
        assert world is not None
        proof = _runtime_proof(
            world_id=world,
            runtime_owner_id=runtime_owner_id,
            runtime_epoch=runtime_epoch,
            runtime=runtime,
        )
        start = self._now(valid_from if valid_from is not None else now)
        return self._candidate(
            world_id=world,
            lineage_id=lineage_id,
            holder_kind=holder_kind,
            holder_id=holder_id,
            role_class=role_class,
            role_label=role_label,
            scope=scope,
            obligation=obligation,
            purpose_revision_id=purpose_revision_id,
            issuer_kind=issuer_kind,
            issuer_id=issuer_id,
            runtime=proof,
            valid_from=start,
            ttl_seconds=ttl_seconds,
            expires_at=expires_at,
            renew_after=renew_after,
            role_lease_id=role_lease_id,
            predecessor_lease_id=predecessor_lease_id,
            renewal_count=renewal_count,
        )

    def request(
        self,
        *,
        world_id: str,
        lineage_id: str | None,
        holder_kind: HolderKind | str,
        holder_id: str,
        role_class: RoleClass | str,
        role_label: str,
        scope: RoleScope,
        issuer_kind: str,
        issuer_id: str,
        runtime_owner_id: str | None = None,
        runtime_epoch: int | None = None,
        runtime: RuntimeLeaseProof | None = None,
        obligation: RoleObligation | None = None,
        purpose_revision_id: str | None = None,
        ttl_seconds: float | int | None = None,
        expires_at: datetime | None = None,
        renew_after: datetime | None = None,
        role_lease_id: str | None = None,
        now: datetime | None = None,
    ) -> RoleLease:
        """Create a durable ``REQUESTED`` lease without granting it yet."""

        current_time = self._now(now)
        candidate = self._prepare_candidate(
            world_id=world_id,
            lineage_id=lineage_id,
            holder_kind=holder_kind,
            holder_id=holder_id,
            role_class=role_class,
            role_label=role_label,
            scope=scope,
            obligation=obligation,
            purpose_revision_id=purpose_revision_id,
            issuer_kind=issuer_kind,
            issuer_id=issuer_id,
            runtime_owner_id=runtime_owner_id,
            runtime_epoch=runtime_epoch,
            runtime=runtime,
            valid_from=current_time,
            ttl_seconds=ttl_seconds,
            expires_at=expires_at,
            renew_after=renew_after,
            role_lease_id=role_lease_id,
            predecessor_lease_id=None,
            renewal_count=0,
            now=current_time,
        )
        with self._write_transaction() as conn:
            self._expire_due_locked(conn, current_time)
            existing = self._row(conn, candidate["role_lease_id"])
            if existing is not None:
                current = self._from_row(existing)
                if self._same_immutable(current, {**candidate, "role_epoch": current.role_epoch}):
                    return current
                raise RoleLeaseConflictError("role_lease_id was reused with a different lease")
            epoch = self._allocate_epoch(
                conn,
                _scope_key(
                    world_id=candidate["world_id"],
                    role_class=candidate["role_class"],
                    lineage_id=candidate["lineage_id"],
                    scope=candidate["scope"],
                ),
            )
            return self._insert(
                conn,
                **candidate,
                role_epoch=epoch,
                status=RoleLeaseStatus.REQUESTED,
                last_evidence_event_id=None,
                now=current_time,
            )

    def grant(
        self,
        role_lease_id: str,
        *,
        runtime_owner_id: str | None = None,
        runtime_epoch: int | None = None,
        runtime: RuntimeLeaseProof | None = None,
        now: datetime | None = None,
    ) -> RoleLease:
        """Activate a requested lease under the same current Runtime proof."""

        lease_id = _identifier(role_lease_id, "role_lease_id")
        assert lease_id is not None
        current_time = self._now(now)
        with self._write_transaction() as conn:
            self._expire_due_locked(conn, current_time)
            row = self._row(conn, lease_id)
            if row is None:
                raise RoleLeaseNotFoundError("role lease does not exist")
            current = self._from_row(row)
            proof = _runtime_proof(
                world_id=current.world_id,
                runtime_owner_id=runtime_owner_id,
                runtime_epoch=runtime_epoch,
                runtime=runtime,
            )
            if current.status is RoleLeaseStatus.ACTIVE:
                if current.runtime_owner_id == proof.owner_id and current.runtime_epoch == proof.epoch:
                    return current
                raise RuntimeLeaseFenceError("active role was granted under another Runtime epoch")
            if current.status is RoleLeaseStatus.EXPIRED:
                raise RoleLeaseExpiredError("requested role expired before grant")
            if current.status is not RoleLeaseStatus.REQUESTED:
                raise RoleLeaseStateError("only a requested role can be granted")
            if current.runtime_owner_id != proof.owner_id or current.runtime_epoch != proof.epoch:
                raise RuntimeLeaseFenceError("Runtime epoch changed before role grant")
            conn.execute(
                "UPDATE role_leases SET status = 'active', updated_at = ? WHERE role_lease_id = ?",
                (_timestamp(current_time), lease_id),
            )
            updated = self._row(conn, lease_id)
            assert updated is not None
            return self._from_row(updated)

    def grant_new(
        self,
        *,
        world_id: str,
        lineage_id: str | None,
        holder_kind: HolderKind | str,
        holder_id: str,
        role_class: RoleClass | str,
        role_label: str,
        scope: RoleScope,
        issuer_kind: str,
        issuer_id: str,
        runtime_owner_id: str | None = None,
        runtime_epoch: int | None = None,
        runtime: RuntimeLeaseProof | None = None,
        obligation: RoleObligation | None = None,
        purpose_revision_id: str | None = None,
        ttl_seconds: float | int | None = None,
        expires_at: datetime | None = None,
        renew_after: datetime | None = None,
        role_lease_id: str | None = None,
        now: datetime | None = None,
    ) -> RoleLease:
        """Create and activate one role atomically under a Runtime proof."""

        current_time = self._now(now)
        candidate = self._prepare_candidate(
            world_id=world_id,
            lineage_id=lineage_id,
            holder_kind=holder_kind,
            holder_id=holder_id,
            role_class=role_class,
            role_label=role_label,
            scope=scope,
            obligation=obligation,
            purpose_revision_id=purpose_revision_id,
            issuer_kind=issuer_kind,
            issuer_id=issuer_id,
            runtime_owner_id=runtime_owner_id,
            runtime_epoch=runtime_epoch,
            runtime=runtime,
            valid_from=current_time,
            ttl_seconds=ttl_seconds,
            expires_at=expires_at,
            renew_after=renew_after,
            role_lease_id=role_lease_id,
            predecessor_lease_id=None,
            renewal_count=0,
            now=current_time,
        )
        with self._write_transaction() as conn:
            self._expire_due_locked(conn, current_time)
            existing = self._row(conn, candidate["role_lease_id"])
            if existing is not None:
                current = self._from_row(existing)
                if current.status is RoleLeaseStatus.ACTIVE and self._same_immutable(
                    current, {**candidate, "role_epoch": current.role_epoch}
                ):
                    return current
                raise RoleLeaseConflictError("role_lease_id was reused with a different lease")
            epoch = self._allocate_epoch(
                conn,
                _scope_key(
                    world_id=candidate["world_id"],
                    role_class=candidate["role_class"],
                    lineage_id=candidate["lineage_id"],
                    scope=candidate["scope"],
                ),
            )
            return self._insert(
                conn,
                **candidate,
                role_epoch=epoch,
                status=RoleLeaseStatus.ACTIVE,
                last_evidence_event_id=None,
                now=current_time,
            )

    def get(self, role_lease_id: str, *, now: datetime | None = None) -> RoleLease | None:
        """Read one lease and durably mark it expired when its deadline passed."""

        lease_id = _identifier(role_lease_id, "role_lease_id")
        assert lease_id is not None
        current_time = self._now(now)
        with self._write_transaction() as conn:
            self._expire_due_locked(conn, current_time)
            row = self._row(conn, lease_id)
            return None if row is None else self._from_row(row)

    def list(
        self,
        *,
        world_id: str,
        lineage_id: str | None = None,
        task_front_id: str | None = None,
        status: RoleLeaseStatus | str | Sequence[RoleLeaseStatus | str] | None = None,
        limit: int = 100,
        now: datetime | None = None,
    ) -> list[RoleLease]:
        """List bounded observations; filters never change authority."""

        world = _identifier(world_id, "world_id")
        assert world is not None
        lineage = _identifier(lineage_id, "lineage_id", optional=True)
        task_front = _identifier(task_front_id, "task_front_id", optional=True)
        if type(limit) is not int or not 1 <= limit <= _MAX_LIST_LIMIT:
            raise RoleLeaseValidationError(f"limit must be in [1, {_MAX_LIST_LIMIT}]")
        statuses: tuple[RoleLeaseStatus, ...]
        if status is None:
            statuses = ()
        elif isinstance(status, (str, RoleLeaseStatus)):
            statuses = (_enum(status, RoleLeaseStatus, "status"),)  # type: ignore[assignment]
        else:
            statuses = tuple(_enum(item, RoleLeaseStatus, "status") for item in status)  # type: ignore[misc]
        current_time = self._now(now)
        with self._write_transaction() as conn:
            self._expire_due_locked(conn, current_time)
            clauses = ["world_id = ?"]
            params: list[Any] = [world]
            if lineage is not None:
                clauses.append("lineage_id = ?")
                params.append(lineage)
            if task_front is not None:
                clauses.append("json_extract(scope_json, '$.task_front_id') = ?")
                params.append(task_front)
            if statuses:
                placeholders = ", ".join("?" for _ in statuses)
                clauses.append(f"status IN ({placeholders})")
                params.extend(item.value for item in statuses)
            rows = conn.execute(
                _ROLE_ROW_SELECT + " WHERE "
                + " AND ".join(clauses)
                + " ORDER BY role_leases.role_epoch, role_leases.created_at LIMIT ?",
                (*params, limit),
            ).fetchall()
            return [self._from_row(row) for row in rows]

    def observe_holder(
        self,
        *,
        world_id: str,
        holder_kind: HolderKind | str,
        holder_id: str,
        limit: int = 100,
        now: datetime | None = None,
    ) -> RoleAccountabilitySnapshot:
        """Read role accountability without expiring or otherwise writing.

        Existing command-oriented readers persist elapsed expiry before they
        return.  An observatory must not do that: page refresh is not a life
        command.  This method computes an effective status in memory and
        reads each role plus its contribution gate inside one SQLite snapshot.
        """

        world = _identifier(world_id, "world_id")
        assert world is not None
        kind = self._normalise_holder_kind(holder_kind)
        holder = _identifier(holder_id, "holder_id")
        assert holder is not None
        if type(limit) is not int or not 1 <= limit <= _MAX_LIST_LIMIT:
            raise RoleLeaseValidationError(f"limit must be in [1, {_MAX_LIST_LIMIT}]")
        current_time = self._now(now)
        with self._read_transaction() as conn:
            rows = conn.execute(
                _ROLE_ROW_SELECT
                + " WHERE role_leases.world_id = ?"
                + " AND role_leases.holder_kind = ?"
                + " AND role_leases.holder_id = ?"
                + " ORDER BY role_leases.created_at DESC,"
                + " role_leases.role_epoch DESC, role_leases.role_lease_id"
                + " LIMIT ?",
                (world, kind.value, holder, limit),
            ).fetchall()
            observations: list[RoleAccountabilityObservation] = []
            for row in rows:
                role = self._from_row(row)
                summary = self._contribution_summary_locked(conn, role)
                contribution_rows = (
                    []
                    if role.accountability_cycle_id is None
                    else conn.execute(
                        "SELECT * FROM role_contributions "
                        "WHERE accountability_cycle_id = ? "
                        "ORDER BY cycle_sequence",
                        (role.accountability_cycle_id,),
                    ).fetchall()
                )
                contributions = [
                    self._contribution_from_row(item)
                    for item in contribution_rows
                ]
                for expected_sequence, contribution in enumerate(
                    contributions,
                    start=1,
                ):
                    self._validate_contribution_binding(conn, role, contribution)
                    if contribution.cycle_sequence != expected_sequence:
                        raise RoleLeaseStorageError(
                            "stored role contribution sequence is corrupt"
                        )
                evidence_classes = tuple(
                    sorted(
                        {item.evidence_class for item in contributions},
                        key=lambda item: item.value,
                    )
                )
                effective_status = role.status
                if (
                    role.status
                    in {
                        RoleLeaseStatus.REQUESTED,
                        RoleLeaseStatus.ACTIVE,
                        RoleLeaseStatus.SUSPENDED,
                    }
                    and role.expires_at <= current_time
                ):
                    effective_status = RoleLeaseStatus.EXPIRED
                observations.append(
                    RoleAccountabilityObservation(
                        role=role,
                        effective_status=effective_status,
                        contribution_summary=summary,
                        contribution_evidence_classes=evidence_classes,
                    )
                )
            return RoleAccountabilitySnapshot(
                world_id=world,
                holder_kind=kind,
                holder_id=holder,
                observed_at=current_time,
                roles=tuple(observations),
            )

    @staticmethod
    def _contribution_from_row(row: sqlite3.Row) -> RoleContribution:
        try:
            output_value = row["output_kind"]
            contribution = RoleContribution(
                contribution_id=str(row["contribution_id"]),
                accountability_cycle_id=str(row["accountability_cycle_id"]),
                role_lease_id=str(row["role_lease_id"]),
                role_epoch=int(row["role_epoch"]),
                cycle_sequence=int(row["cycle_sequence"]),
                world_id=str(row["world_id"]),
                holder_kind=HolderKind(str(row["holder_kind"])),
                holder_id=str(row["holder_id"]),
                contribution_kind=RoleContributionKind(
                    str(row["contribution_kind"])
                ),
                output_kind=(
                    None
                    if output_value is None
                    else RoleOutputKind(str(output_value))
                ),
                evidence_event_id=str(row["evidence_event_id"]),
                source_turn_id=(
                    None
                    if row["source_turn_id"] is None
                    else str(row["source_turn_id"])
                ),
                evidence_class=RoleContributionEvidence(
                    str(row["evidence_class"])
                ),
                created_at=_parse_timestamp(row["created_at"], "created_at"),
            )
            if (
                not contribution.contribution_id
                or not contribution.accountability_cycle_id
                or not contribution.role_lease_id
                or contribution.role_epoch < 1
                or contribution.cycle_sequence < 1
                or not contribution.world_id
                or not contribution.holder_id
                or not contribution.evidence_event_id
            ):
                raise RoleLeaseStorageError(
                    "stored role contribution identity is corrupt"
                )
            if contribution.contribution_kind is RoleContributionKind.DIRECT_OUTPUT:
                expected_evidence = {
                    RoleOutputKind.WORKSPACE_CHECKPOINT: (
                        RoleContributionEvidence.LIVE_WORKSPACE_CHECKPOINTED
                    ),
                    RoleOutputKind.HABITAT_EFFECT: (
                        RoleContributionEvidence.LIVE_HABITAT_EFFECT
                    ),
                }.get(contribution.output_kind)
                if expected_evidence is None or contribution.evidence_class is not expected_evidence:
                    raise RoleLeaseStorageError(
                        "stored direct-output contribution evidence is corrupt"
                    )
            elif (
                contribution.output_kind is not None
                or contribution.evidence_class
                is not RoleContributionEvidence.CONTROL_ONLY
            ):
                raise RoleLeaseStorageError(
                    "stored coordination contribution evidence is corrupt"
                )
            return contribution
        except RoleLeaseError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise RoleLeaseStorageError(
                "stored role contribution row is corrupt"
            ) from exc

    @classmethod
    def _validate_contribution_binding(
        cls,
        conn: sqlite3.Connection,
        role: RoleLease,
        contribution: RoleContribution,
    ) -> None:
        if (
            role.accountability_cycle_id is None
            or contribution.accountability_cycle_id
            != role.accountability_cycle_id
            or contribution.world_id != role.world_id
        ):
            raise RoleLeaseStorageError(
                "stored role contribution binding is corrupt"
            )
        origin_row = cls._row(conn, contribution.role_lease_id)
        if origin_row is None:
            raise RoleLeaseStorageError(
                "stored role contribution origin is missing"
            )
        origin = cls._from_row(origin_row)
        if (
            origin.accountability_cycle_id != contribution.accountability_cycle_id
            or origin.world_id != contribution.world_id
            or origin.role_epoch != contribution.role_epoch
            or origin.holder_kind is not contribution.holder_kind
            or origin.holder_id != contribution.holder_id
        ):
            raise RoleLeaseStorageError(
                "stored role contribution origin binding is corrupt"
            )

    @classmethod
    def _contribution_summary_locked(
        cls,
        conn: sqlite3.Connection,
        role: RoleLease,
    ) -> RoleContributionSummary:
        rows = (
            []
            if role.accountability_cycle_id is None
            else conn.execute(
                "SELECT * FROM role_contributions "
                "WHERE accountability_cycle_id = ? ORDER BY cycle_sequence",
                (role.accountability_cycle_id,),
            ).fetchall()
        )
        contributions = [cls._contribution_from_row(row) for row in rows]
        for expected_sequence, item in enumerate(contributions, start=1):
            cls._validate_contribution_binding(conn, role, item)
            if item.cycle_sequence != expected_sequence:
                raise RoleLeaseStorageError(
                    "stored role contribution sequence is corrupt"
                )
        direct = [
            item
            for item in contributions
            if item.contribution_kind is RoleContributionKind.DIRECT_OUTPUT
        ]
        coordination_count = sum(
            item.contribution_kind is RoleContributionKind.COORDINATION
            for item in contributions
        )
        consecutive_coordination = 0
        for item in reversed(contributions):
            if item.contribution_kind is RoleContributionKind.DIRECT_OUTPUT:
                break
            consecutive_coordination += 1
        obligation = role.obligation
        if obligation is None:
            eligible = True
            reason = "role_has_no_direct_output_obligation"
        elif len(direct) < obligation.minimum_direct_outputs:
            eligible = False
            reason = "role_direct_output_required"
        elif consecutive_coordination > obligation.max_consecutive_coordination:
            eligible = False
            reason = "role_coordination_streak_exceeded"
        else:
            eligible = True
            reason = "role_direct_output_obligation_satisfied"
        return RoleContributionSummary(
            role_lease_id=role.role_lease_id,
            accountability_cycle_id=role.accountability_cycle_id,
            role_epoch=role.role_epoch,
            direct_output_count=len(direct),
            coordination_count=coordination_count,
            consecutive_coordination=consecutive_coordination,
            last_direct_output_event_id=(
                None if not direct else direct[-1].evidence_event_id
            ),
            last_contribution_at=(
                None if not contributions else contributions[-1].created_at
            ),
            renewal_eligible=eligible,
            reason_code=reason,
        )

    def contribution_summary(
        self,
        role_lease_id: str,
        *,
        now: datetime | None = None,
    ) -> RoleContributionSummary:
        """Observe payload-free role progress without creating a stimulus."""

        lease_id = _identifier(role_lease_id, "role_lease_id")
        assert lease_id is not None
        current_time = self._now(now)
        with self._write_transaction() as conn:
            self._expire_due_locked(conn, current_time)
            row = self._row(conn, lease_id)
            if row is None:
                raise RoleLeaseNotFoundError("role lease does not exist")
            return self._contribution_summary_locked(conn, self._from_row(row))

    def list_contributions(
        self,
        role_lease_id: str,
        *,
        limit: int = 100,
    ) -> list[RoleContribution]:
        lease_id = _identifier(role_lease_id, "role_lease_id")
        assert lease_id is not None
        if type(limit) is not int or not 1 <= limit <= _MAX_LIST_LIMIT:
            raise RoleLeaseValidationError(f"limit must be in [1, {_MAX_LIST_LIMIT}]")
        with self._lock:
            self._ensure_open()
            role_row = self._row(self._conn, lease_id)
            if role_row is None:
                raise RoleLeaseNotFoundError("role lease does not exist")
            role = self._from_row(role_row)
            rows = (
                []
                if role.accountability_cycle_id is None
                else self._conn.execute(
                    "SELECT * FROM role_contributions "
                    "WHERE accountability_cycle_id = ? "
                    "ORDER BY cycle_sequence LIMIT ?",
                    (role.accountability_cycle_id, limit),
                ).fetchall()
            )
            contributions = [self._contribution_from_row(row) for row in rows]
            for expected_sequence, item in enumerate(contributions, start=1):
                self._validate_contribution_binding(self._conn, role, item)
                if item.cycle_sequence != expected_sequence:
                    raise RoleLeaseStorageError(
                        "stored role contribution sequence is corrupt"
                    )
            return contributions

    def record_contribution(
        self,
        role_lease_id: str,
        *,
        expected_role_epoch: int,
        holder_kind: HolderKind | str,
        holder_id: str,
        scope: RoleScope,
        contribution_kind: RoleContributionKind | str,
        evidence_event_id: str | None = None,
        evidence_class: RoleContributionEvidence | str | None = None,
        output_kind: RoleOutputKind | str | None = None,
        source_turn_id: str | None = None,
        verified_receipt: VerifiedRoleReceipt | None = None,
        runtime_owner_id: str | None = None,
        runtime_epoch: int | None = None,
        runtime: RuntimeLeaseProof | None = None,
        now: datetime | None = None,
    ) -> RoleContribution:
        """Append one trusted, payload-free activity receipt for a role.

        Waiting, replay and Harness control observations never call this API.
        Coordination may be recorded for anti-drift visibility, but cannot be
        upgraded to direct output by changing a free-form label.
        """

        lease_id = _identifier(role_lease_id, "role_lease_id")
        assert lease_id is not None
        kind = _enum(
            contribution_kind, RoleContributionKind, "contribution_kind"
        )
        current_time = self._now(now)
        with self._write_transaction() as conn:
            current = self._require_current(conn, lease_id, now=current_time)
            proof = _runtime_proof(
                world_id=current.world_id,
                runtime_owner_id=runtime_owner_id,
                runtime_epoch=runtime_epoch,
                runtime=runtime,
            )
            self._require_runtime(current, proof)
            self._require_role_epoch(current, expected_role_epoch)
            self._require_holder(current, holder_kind, holder_id)
            self._require_scope(current, scope)
            self._require_active(current)
            obligation = current.obligation
            if obligation is None:
                raise RoleLeaseValidationError(
                    "role has no direct-output obligation to observe"
                )
            cycle_id = current.accountability_cycle_id
            if cycle_id is None:
                raise RoleLeaseStorageError(
                    "obligated role has no accountability cycle"
                )
            if kind is RoleContributionKind.DIRECT_OUTPUT:
                if any(
                    value is not None
                    for value in (
                        evidence_event_id,
                        evidence_class,
                        output_kind,
                        source_turn_id,
                    )
                ):
                    raise RoleLeaseValidationError(
                        "direct output accepts only a verified production receipt"
                    )
                if not isinstance(verified_receipt, VerifiedRoleReceipt):
                    raise RoleLeaseValidationError(
                        "direct output requires a verified production receipt"
                    )
                verified_receipt.assert_valid()
                if (
                    verified_receipt.world_id != current.world_id
                    or verified_receipt.holder_kind is not current.holder_kind
                    or verified_receipt.holder_id != current.holder_id
                ):
                    raise RoleLeaseValidationError(
                        "verified production receipt belongs to another holder"
                    )
                cycle = conn.execute(
                    "SELECT created_at FROM role_accountability_cycles "
                    "WHERE accountability_cycle_id = ?",
                    (cycle_id,),
                ).fetchone()
                if cycle is None:
                    raise RoleLeaseStorageError(
                        "accountability cycle is missing"
                    )
                cycle_started_at = _parse_timestamp(
                    cycle["created_at"],
                    "accountability_cycle.created_at",
                )
                if (
                    verified_receipt.produced_at < cycle_started_at
                    or verified_receipt.produced_at > current_time
                ):
                    raise RoleLeaseValidationError(
                        "production receipt is outside this accountability cycle"
                    )
                output = verified_receipt.output_kind
                evidence = verified_receipt.evidence_class
                event_id = verified_receipt.evidence_event_id
                turn_id = verified_receipt.source_turn_id
                if output not in obligation.accepted_output_kinds:
                    raise RoleLeaseValidationError(
                        "direct output kind is not accepted by this role"
                    )
                required_evidence = {
                    RoleOutputKind.WORKSPACE_CHECKPOINT: (
                        RoleContributionEvidence.LIVE_WORKSPACE_CHECKPOINTED
                    ),
                    RoleOutputKind.HABITAT_EFFECT: (
                        RoleContributionEvidence.LIVE_HABITAT_EFFECT
                    ),
                }[output]
                if evidence is not required_evidence:
                    raise RoleLeaseValidationError(
                        "direct output evidence does not match its production receipt"
                    )
            else:
                if verified_receipt is not None:
                    raise RoleLeaseValidationError(
                        "coordination cannot consume a production receipt"
                    )
                event_id = _identifier(
                    evidence_event_id,
                    "evidence_event_id",
                )
                turn_id = _identifier(
                    source_turn_id,
                    "source_turn_id",
                    optional=True,
                )
                evidence = _enum(
                    evidence_class,
                    RoleContributionEvidence,
                    "evidence_class",
                )
                output = (
                    None
                    if output_kind is None
                    else _enum(output_kind, RoleOutputKind, "output_kind")
                )
                assert event_id is not None
                if (
                    output is not None
                    or evidence is not RoleContributionEvidence.CONTROL_ONLY
                ):
                    raise RoleLeaseValidationError(
                        "coordination is control-only and cannot carry an output kind"
                    )
            contribution_id = "contribution_" + _digest(
                {
                    "accountability_cycle_id": cycle_id,
                    "evidence_event_id": event_id,
                }
            )[:32]
            existing = conn.execute(
                "SELECT * FROM role_contributions "
                "WHERE accountability_cycle_id = ? "
                "AND evidence_event_id = ?",
                (cycle_id, event_id),
            ).fetchone()
            if existing is not None:
                replay = self._contribution_from_row(existing)
                self._validate_contribution_binding(conn, current, replay)
                if (
                    replay.contribution_id == contribution_id
                    and replay.accountability_cycle_id == cycle_id
                    and replay.world_id == current.world_id
                    and replay.contribution_kind is kind
                    and replay.output_kind is output
                    and replay.source_turn_id == turn_id
                    and replay.evidence_class is evidence
                ):
                    return replay
                raise RoleLeaseConflictError(
                    "role contribution evidence was reused with different meaning"
                )
            try:
                next_sequence = int(
                    conn.execute(
                        "SELECT COALESCE(MAX(cycle_sequence), 0) + 1 "
                        "FROM role_contributions "
                        "WHERE accountability_cycle_id = ?",
                        (cycle_id,),
                    ).fetchone()[0]
                )
                conn.execute(
                    """INSERT INTO role_contributions (
                        contribution_id, accountability_cycle_id, role_lease_id,
                        role_epoch, cycle_sequence, world_id,
                        holder_kind, holder_id, contribution_kind, output_kind,
                        evidence_event_id, source_turn_id, evidence_class,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        contribution_id,
                        cycle_id,
                        current.role_lease_id,
                        current.role_epoch,
                        next_sequence,
                        current.world_id,
                        current.holder_kind.value,
                        current.holder_id,
                        kind.value,
                        None if output is None else output.value,
                        event_id,
                        turn_id,
                        evidence.value,
                        _timestamp(current_time),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise RoleLeaseConflictError(
                    "role contribution identity collides"
                ) from exc
            row = conn.execute(
                "SELECT * FROM role_contributions WHERE contribution_id = ?",
                (contribution_id,),
            ).fetchone()
            if row is None:
                raise RoleLeaseStorageError(
                    "inserted role contribution could not be read back"
                )
            contribution = self._contribution_from_row(row)
            self._validate_contribution_binding(conn, current, contribution)
            return contribution

    def _require_current(
        self,
        conn: sqlite3.Connection,
        role_lease_id: str,
        *,
        now: datetime,
    ) -> RoleLease:
        self._expire_due_locked(conn, now)
        row = self._row(conn, role_lease_id)
        if row is None:
            raise RoleLeaseNotFoundError("role lease does not exist")
        return self._from_row(row)

    @staticmethod
    def _require_runtime(current: RoleLease, proof: RuntimeLeaseProof) -> None:
        if current.world_id != proof.world_id:
            raise RuntimeLeaseFenceError("Runtime proof belongs to another world")
        if current.runtime_owner_id != proof.owner_id or current.runtime_epoch != proof.epoch:
            raise RuntimeLeaseFenceError("Runtime owner/epoch is stale for this role")

    @staticmethod
    def _require_role_epoch(current: RoleLease, expected_role_epoch: int) -> None:
        expected = _epoch(expected_role_epoch, "expected_role_epoch")
        if current.role_epoch != expected:
            raise RoleLeaseConflictError("role lease epoch CAS failed")

    @staticmethod
    def _require_holder(
        current: RoleLease, holder_kind: HolderKind | str, holder_id: str
    ) -> None:
        kind = _enum(holder_kind, HolderKind, "holder_kind")
        identifier = _identifier(holder_id, "holder_id")
        assert identifier is not None
        if current.holder_kind is not kind or current.holder_id != identifier:
            raise RoleLeaseHolderError("caller is not the current role holder")

    @staticmethod
    def _require_active(current: RoleLease) -> None:
        if current.status is RoleLeaseStatus.EXPIRED:
            raise RoleLeaseExpiredError("role lease is expired")
        if current.status is not RoleLeaseStatus.ACTIVE:
            raise RoleLeaseStateError("role lease is not active")

    @staticmethod
    def _require_scope(current: RoleLease, scope: RoleScope | None) -> None:
        if scope is None:
            raise RoleLeaseScopeError("an exact RoleScope is required at authorization")
        if not isinstance(scope, RoleScope) or not current.scope.matches(scope):
            raise RoleLeaseScopeError("action scope does not exactly match role scope")

    def authorize(
        self,
        role_lease_id: str,
        *,
        holder_kind: HolderKind | str,
        holder_id: str,
        expected_role_epoch: int,
        runtime_owner_id: str | None = None,
        runtime_epoch: int | None = None,
        runtime: RuntimeLeaseProof | None = None,
        scope: RoleScope | None = None,
        now: datetime | None = None,
    ) -> HarnessAuthority:
        """Revalidate Runtime and Role fences immediately before adapter entry."""

        lease_id = _identifier(role_lease_id, "role_lease_id")
        assert lease_id is not None
        current_time = self._now(now)
        with self._write_transaction() as conn:
            current = self._require_current(conn, lease_id, now=current_time)
            proof = _runtime_proof(
                world_id=current.world_id,
                runtime_owner_id=runtime_owner_id,
                runtime_epoch=runtime_epoch,
                runtime=runtime,
            )
            self._require_runtime(current, proof)
            self._require_role_epoch(current, expected_role_epoch)
            self._require_holder(current, holder_kind, holder_id)
            self._require_scope(current, scope)
            self._require_active(current)
            if current.expires_at <= current_time:
                raise RoleLeaseExpiredError("role lease expired at authorization boundary")
            return HarnessAuthority(
                world_id=current.world_id,
                runtime_owner_id=proof.owner_id,
                runtime_epoch=proof.epoch,
                role_lease_id=current.role_lease_id,
                role_epoch=current.role_epoch,
                holder_kind=current.holder_kind,
                holder_id=current.holder_id,
                scope=current.scope,
            )

    def renew(
        self,
        role_lease_id: str,
        *,
        expected_role_epoch: int,
        runtime_owner_id: str | None = None,
        runtime_epoch: int | None = None,
        runtime: RuntimeLeaseProof | None = None,
        evidence_event_id: str,
        evidence_class: RoleRenewalEvidence | str,
        issuer_kind: str = "runtime",
        issuer_id: str | None = None,
        ttl_seconds: float | int | None = None,
        expires_at: datetime | None = None,
        renew_after: datetime | None = None,
        now: datetime | None = None,
    ) -> RoleLease:
        """Create a new lease version; never extends the old row in place."""

        lease_id = _identifier(role_lease_id, "role_lease_id")
        assert lease_id is not None
        event_id = _identifier(evidence_event_id, "evidence_event_id")
        assert event_id is not None
        evidence = _enum(evidence_class, RoleRenewalEvidence, "evidence_class")
        current_time = self._now(now)
        with self._write_transaction() as conn:
            current = self._require_current(conn, lease_id, now=current_time)
            proof = _runtime_proof(
                world_id=current.world_id,
                runtime_owner_id=runtime_owner_id,
                runtime_epoch=runtime_epoch,
                runtime=runtime,
            )
            self._require_runtime(current, proof)
            self._require_role_epoch(current, expected_role_epoch)
            self._require_active(current)
            if current.expires_at <= current_time:
                raise RoleLeaseExpiredError("role lease expired before renewal")
            if current_time < current.renew_after:
                raise RoleLeaseStateError("role lease is not inside its renewal window")
            if evidence is RoleRenewalEvidence.CONTROL_ONLY:
                raise RoleLeaseValidationError(
                    "control-only, waiting, replay or approval evidence cannot renew a role"
                )
            if current.obligation is not None:
                contribution = self._contribution_summary_locked(conn, current)
                if evidence is not RoleRenewalEvidence.LIVE_EXTERNAL_RESULT:
                    raise RoleLeaseValidationError(
                        "a direct-output role can renew only from live external-result evidence"
                    )
                if not contribution.renewal_eligible:
                    raise RoleLeaseValidationError(contribution.reason_code)
                if event_id != contribution.last_direct_output_event_id:
                    raise RoleLeaseValidationError(
                        "renewal evidence must identify the latest accepted direct output"
                    )
            if current.renewal_count >= _MAX_RENEWAL_COUNT and evidence not in {
                RoleRenewalEvidence.LIVE_EXTERNAL_RESULT,
                RoleRenewalEvidence.SUBJECT_REFLECTION,
            }:
                raise RoleLeaseValidationError(
                    "continuous renewal limit requires external-result or subject-reflection evidence"
                )
            issuer_identifier = proof.owner_id if issuer_id is None else issuer_id
            new_issuer_kind, new_issuer_id = _issuer(
                issuer_kind,
                issuer_identifier,
                holder_kind=current.holder_kind,
                holder_id=current.holder_id,
            )
            start, end, renewal = _times(
                current.role_class,
                valid_from=current_time,
                ttl_seconds=ttl_seconds,
                expires_at=expires_at,
                renew_after=renew_after,
            )
            scope_key = _scope_key(
                world_id=current.world_id,
                role_class=current.role_class,
                lineage_id=current.lineage_id,
                scope=current.scope,
            )
            new_epoch = self._allocate_epoch(conn, scope_key)
            new_id = uuid.uuid4().hex
            conn.execute(
                "UPDATE role_leases SET status = 'released', released_at = ?, updated_at = ? WHERE role_lease_id = ?",
                (_timestamp(current_time), _timestamp(current_time), lease_id),
            )
            return self._insert(
                conn,
                role_lease_id=new_id,
                world_id=current.world_id,
                lineage_id=current.lineage_id,
                holder_kind=current.holder_kind,
                holder_id=current.holder_id,
                role_class=current.role_class,
                role_label=current.role_label,
                scope=current.scope,
                obligation=current.obligation,
                purpose_revision_id=current.purpose_revision_id,
                issuer_kind=new_issuer_kind,
                issuer_id=new_issuer_id,
                role_epoch=new_epoch,
                runtime=proof,
                valid_from=start,
                expires_at=end,
                renew_after=renewal,
                status=RoleLeaseStatus.ACTIVE,
                predecessor_lease_id=current.role_lease_id,
                renewal_count=current.renewal_count + 1,
                last_evidence_event_id=event_id,
                now=current_time,
            )

    def handoff(
        self,
        role_lease_id: str,
        *,
        expected_role_epoch: int,
        new_holder_kind: HolderKind | str,
        new_holder_id: str,
        new_lineage_id: str | None = None,
        issuer_kind: str = "runtime",
        issuer_id: str | None = None,
        runtime_owner_id: str | None = None,
        runtime_epoch: int | None = None,
        runtime: RuntimeLeaseProof | None = None,
        now: datetime | None = None,
    ) -> RoleLease:
        """Atomically release the old holder and activate a new holder."""

        lease_id = _identifier(role_lease_id, "role_lease_id")
        assert lease_id is not None
        current_time = self._now(now)
        with self._write_transaction() as conn:
            current = self._require_current(conn, lease_id, now=current_time)
            proof = _runtime_proof(
                world_id=current.world_id,
                runtime_owner_id=runtime_owner_id,
                runtime_epoch=runtime_epoch,
                runtime=runtime,
            )
            self._require_runtime(current, proof)
            self._require_role_epoch(current, expected_role_epoch)
            self._require_active(current)
            if current.expires_at <= current_time:
                raise RoleLeaseExpiredError("role lease expired before handoff")
            new_kind = self._normalise_holder_kind(new_holder_kind)
            new_id = _identifier(new_holder_id, "new_holder_id")
            assert new_id is not None
            if new_kind is current.holder_kind and new_id == current.holder_id:
                raise RoleLeaseHolderError("handoff requires a different holder")
            lineage = _identifier(new_lineage_id, "new_lineage_id", optional=True)
            if current.role_class is RoleClass.SUBJECT_ROLE:
                if new_kind is not HolderKind.ENGRAM:
                    raise RoleLeaseValidationError("subject role cannot hand off to a worker/user")
                lineage = current.lineage_id if lineage is None else lineage
                if lineage != current.lineage_id:
                    raise RoleLeaseValidationError(
                        "subject role handoff must preserve the subject lineage"
                    )
            elif new_kind in {HolderKind.WORKER, HolderKind.USER}:
                if lineage is not None:
                    raise RoleLeaseValidationError("worker/user task handoff cannot borrow lineage")
            else:
                lineage = current.lineage_id if lineage is None else lineage
            _validate_role_binding(current.role_class, new_kind, lineage, current.scope)
            issuer_identifier = proof.owner_id if issuer_id is None else issuer_id
            new_issuer_kind, new_issuer_id = _issuer(
                issuer_kind,
                issuer_identifier,
                holder_kind=new_kind,
                holder_id=new_id,
            )
            scope_key = _scope_key(
                world_id=current.world_id,
                role_class=current.role_class,
                lineage_id=lineage,
                scope=current.scope,
            )
            # A subject handoff is expected to preserve the scope lineage.  A
            # task handoff keeps the action scope's epoch even if the holder's
            # optional Engram lineage metadata changes.
            new_epoch = self._allocate_epoch(conn, scope_key)
            conn.execute(
                "UPDATE role_leases SET status = 'released', released_at = ?, updated_at = ? WHERE role_lease_id = ?",
                (_timestamp(current_time), _timestamp(current_time), lease_id),
            )
            start, end, renewal = _times(
                current.role_class,
                valid_from=current_time,
                ttl_seconds=None,
                expires_at=current.expires_at,
                renew_after=None,
            )
            return self._insert(
                conn,
                role_lease_id=uuid.uuid4().hex,
                world_id=current.world_id,
                lineage_id=lineage,
                holder_kind=new_kind,
                holder_id=new_id,
                role_class=current.role_class,
                role_label=current.role_label,
                scope=current.scope,
                obligation=current.obligation,
                purpose_revision_id=current.purpose_revision_id,
                issuer_kind=new_issuer_kind,
                issuer_id=new_issuer_id,
                role_epoch=new_epoch,
                runtime=proof,
                valid_from=start,
                expires_at=end,
                renew_after=renewal,
                status=RoleLeaseStatus.ACTIVE,
                predecessor_lease_id=current.role_lease_id,
                renewal_count=current.renewal_count,
                last_evidence_event_id=None,
                now=current_time,
            )

    def succession(
        self,
        role_lease_id: str,
        *,
        expected_role_epoch: int,
        new_holder_kind: HolderKind | str,
        new_holder_id: str,
        runtime_owner_id: str | None = None,
        runtime_epoch: int | None = None,
        runtime: RuntimeLeaseProof | None = None,
        new_lineage_id: str | None = None,
        issuer_kind: str = "runtime",
        issuer_id: str | None = None,
        purpose_revision_id: str | None = None,
        ttl_seconds: float | int | None = None,
        expires_at: datetime | None = None,
        renew_after: datetime | None = None,
        now: datetime | None = None,
    ) -> RoleLease:
        """Create a new active successor only after the predecessor expired.

        Succession accepts a *new* Runtime proof.  It does not revive or use
        the expired holder's Runtime proof, and the old role epoch can never
        authorize the successor.
        """

        lease_id = _identifier(role_lease_id, "role_lease_id")
        assert lease_id is not None
        current_time = self._now(now)
        with self._write_transaction() as conn:
            current = self._require_current(conn, lease_id, now=current_time)
            if current.status is not RoleLeaseStatus.EXPIRED:
                if current.status is RoleLeaseStatus.ACTIVE and current.expires_at <= current_time:
                    self._expire_due_locked(conn, current_time)
                    current = self._require_current(conn, lease_id, now=current_time)
                else:
                    raise RoleLeaseStateError("succession requires an expired predecessor")
            expected = _epoch(expected_role_epoch, "expected_role_epoch")
            if current.role_epoch != expected:
                raise RoleLeaseConflictError("predecessor role epoch CAS failed")
            proof = _runtime_proof(
                world_id=current.world_id,
                runtime_owner_id=runtime_owner_id,
                runtime_epoch=runtime_epoch,
                runtime=runtime,
            )
            new_kind = self._normalise_holder_kind(new_holder_kind)
            new_id = _identifier(new_holder_id, "new_holder_id")
            assert new_id is not None
            if new_kind is current.holder_kind and new_id == current.holder_id:
                raise RoleLeaseHolderError("succession requires a different holder")
            lineage = _identifier(new_lineage_id, "new_lineage_id", optional=True)
            if current.role_class is RoleClass.SUBJECT_ROLE:
                if new_kind is not HolderKind.ENGRAM:
                    raise RoleLeaseValidationError("subject succession cannot become a worker/user")
                lineage = current.lineage_id if lineage is None else lineage
                if lineage != current.lineage_id:
                    raise RoleLeaseValidationError(
                        "subject succession must preserve the subject lineage"
                    )
            elif new_kind in {HolderKind.WORKER, HolderKind.USER}:
                if lineage is not None:
                    raise RoleLeaseValidationError("worker/user succession cannot borrow lineage")
            else:
                lineage = current.lineage_id if lineage is None else lineage
            _validate_role_binding(current.role_class, new_kind, lineage, current.scope)
            issuer_identifier = proof.owner_id if issuer_id is None else issuer_id
            new_issuer_kind, new_issuer_id = _issuer(
                issuer_kind,
                issuer_identifier,
                holder_kind=new_kind,
                holder_id=new_id,
            )
            purpose = (
                current.purpose_revision_id
                if purpose_revision_id is None
                else self._normalise_purpose_reference(purpose_revision_id)
            )
            start, end, renewal = _times(
                current.role_class,
                valid_from=current_time,
                ttl_seconds=ttl_seconds,
                expires_at=expires_at,
                renew_after=renew_after,
            )
            scope_key = _scope_key(
                world_id=current.world_id,
                role_class=current.role_class,
                lineage_id=lineage,
                scope=current.scope,
            )
            new_epoch = self._allocate_epoch(conn, scope_key)
            return self._insert(
                conn,
                role_lease_id=uuid.uuid4().hex,
                world_id=current.world_id,
                lineage_id=lineage,
                holder_kind=new_kind,
                holder_id=new_id,
                role_class=current.role_class,
                role_label=current.role_label,
                scope=current.scope,
                obligation=current.obligation,
                purpose_revision_id=purpose,
                issuer_kind=new_issuer_kind,
                issuer_id=new_issuer_id,
                role_epoch=new_epoch,
                runtime=proof,
                valid_from=start,
                expires_at=end,
                renew_after=renewal,
                status=RoleLeaseStatus.ACTIVE,
                predecessor_lease_id=current.role_lease_id,
                renewal_count=current.renewal_count,
                last_evidence_event_id=None,
                now=current_time,
            )

    def _transition(
        self,
        role_lease_id: str,
        *,
        target: RoleLeaseStatus,
        expected_role_epoch: int,
        runtime_owner_id: str | None,
        runtime_epoch: int | None,
        runtime: RuntimeLeaseProof | None,
        now: datetime | None,
    ) -> RoleLease:
        lease_id = _identifier(role_lease_id, "role_lease_id")
        assert lease_id is not None
        current_time = self._now(now)
        with self._write_transaction() as conn:
            current = self._require_current(conn, lease_id, now=current_time)
            proof = _runtime_proof(
                world_id=current.world_id,
                runtime_owner_id=runtime_owner_id,
                runtime_epoch=runtime_epoch,
                runtime=runtime,
            )
            self._require_runtime(current, proof)
            self._require_role_epoch(current, expected_role_epoch)
            if current.status is RoleLeaseStatus.EXPIRED:
                raise RoleLeaseExpiredError("role lease is expired")
            if target is RoleLeaseStatus.RELEASED:
                allowed = {RoleLeaseStatus.REQUESTED, RoleLeaseStatus.ACTIVE, RoleLeaseStatus.SUSPENDED}
            elif target is RoleLeaseStatus.REVOKED:
                allowed = {RoleLeaseStatus.REQUESTED, RoleLeaseStatus.ACTIVE, RoleLeaseStatus.SUSPENDED}
            elif target is RoleLeaseStatus.SUSPENDED:
                allowed = {RoleLeaseStatus.ACTIVE}
            elif target is RoleLeaseStatus.ACTIVE:
                allowed = {RoleLeaseStatus.SUSPENDED}
            else:
                allowed = set()
            if current.status not in allowed:
                raise RoleLeaseStateError(
                    f"cannot transition {current.status.value} to {target.value}"
                )
            released_at = _timestamp(current_time) if target in {
                RoleLeaseStatus.RELEASED,
                RoleLeaseStatus.REVOKED,
            } else None
            conn.execute(
                "UPDATE role_leases SET status = ?, released_at = ?, "
                "handoff_suspended = 0, updated_at = ? WHERE role_lease_id = ?",
                (target.value, released_at, _timestamp(current_time), lease_id),
            )
            row = self._row(conn, lease_id)
            assert row is not None
            return self._from_row(row)

    def release(
        self,
        role_lease_id: str,
        *,
        expected_role_epoch: int,
        runtime_owner_id: str | None = None,
        runtime_epoch: int | None = None,
        runtime: RuntimeLeaseProof | None = None,
        now: datetime | None = None,
    ) -> RoleLease:
        return self._transition(
            role_lease_id,
            target=RoleLeaseStatus.RELEASED,
            expected_role_epoch=expected_role_epoch,
            runtime_owner_id=runtime_owner_id,
            runtime_epoch=runtime_epoch,
            runtime=runtime,
            now=now,
        )

    def revoke(
        self,
        role_lease_id: str,
        *,
        expected_role_epoch: int,
        runtime_owner_id: str | None = None,
        runtime_epoch: int | None = None,
        runtime: RuntimeLeaseProof | None = None,
        now: datetime | None = None,
    ) -> RoleLease:
        return self._transition(
            role_lease_id,
            target=RoleLeaseStatus.REVOKED,
            expected_role_epoch=expected_role_epoch,
            runtime_owner_id=runtime_owner_id,
            runtime_epoch=runtime_epoch,
            runtime=runtime,
            now=now,
        )

    def suspend(
        self,
        role_lease_id: str,
        *,
        expected_role_epoch: int,
        runtime_owner_id: str | None = None,
        runtime_epoch: int | None = None,
        runtime: RuntimeLeaseProof | None = None,
        now: datetime | None = None,
    ) -> RoleLease:
        return self._transition(
            role_lease_id,
            target=RoleLeaseStatus.SUSPENDED,
            expected_role_epoch=expected_role_epoch,
            runtime_owner_id=runtime_owner_id,
            runtime_epoch=runtime_epoch,
            runtime=runtime,
            now=now,
        )

    def resume(
        self,
        role_lease_id: str,
        *,
        expected_role_epoch: int,
        runtime_owner_id: str | None = None,
        runtime_epoch: int | None = None,
        runtime: RuntimeLeaseProof | None = None,
        now: datetime | None = None,
    ) -> RoleLease:
        return self._transition(
            role_lease_id,
            target=RoleLeaseStatus.ACTIVE,
            expected_role_epoch=expected_role_epoch,
            runtime_owner_id=runtime_owner_id,
            runtime_epoch=runtime_epoch,
            runtime=runtime,
            now=now,
        )

    def reap_expired(self, *, now: datetime | None = None) -> int:
        """Durably expire all due requested/active/suspended leases."""

        current_time = self._now(now)
        with self._write_transaction() as conn:
            before = conn.total_changes
            self._expire_due_locked(conn, current_time)
            return conn.total_changes - before

    def recover_runtime_takeover(
        self,
        runtime: RuntimeLeaseProof,
        *,
        now: datetime | None = None,
        bootstrap_permit: RuntimeBootstrapPermit | None = None,
    ) -> dict[str, tuple[Any, ...]]:
        """Fence leases from an older Runtime epoch without extending them.

        Temporary task roles are revoked: their workers are process-local and
        must never be respawned by recovery.  Accepted subject roles are
        rebound to the same holder, scope and original expiry under a new
        role epoch.  This is an authority rebind, not a renewal: no deadline
        moves forward and ``renewal_count`` is unchanged.
        """

        if not isinstance(runtime, RuntimeLeaseProof):
            raise RoleLeaseValidationError(
                "recover_runtime_takeover requires a RuntimeLeaseProof"
            )
        current_time = self._now(now)
        if self._publication_permit is not None and bootstrap_permit is None:
            raise RoleLeaseValidationError(
                "runtime-bound takeover recovery requires a bootstrap permit"
            )
        rebound: list[RoleLease] = []
        revoked: list[str] = []
        with self._write_transaction(bootstrap_permit=bootstrap_permit) as conn:
            self._expire_due_locked(conn, current_time)
            rows = conn.execute(
                _ROLE_ROW_SELECT
                + """ WHERE role_leases.world_id = ?
                      AND status IN ('requested', 'active', 'suspended')
                   ORDER BY role_leases.scope_key, role_leases.role_epoch""",
                (runtime.world_id,),
            ).fetchall()
            for row in rows:
                current = self._from_row(row)
                handoff_suspended = bool(row["handoff_suspended"])
                if (
                    current.runtime_owner_id == runtime.owner_id
                    and current.runtime_epoch == runtime.epoch
                ):
                    continue
                if runtime.epoch <= current.runtime_epoch:
                    raise RuntimeLeaseFenceError(
                        "Runtime takeover epoch is not newer than the role authority"
                    )

                terminal_status = (
                    RoleLeaseStatus.RELEASED
                    if current.role_class is RoleClass.SUBJECT_ROLE
                    and current.status in {
                        RoleLeaseStatus.ACTIVE,
                        RoleLeaseStatus.SUSPENDED,
                    }
                    else RoleLeaseStatus.REVOKED
                )
                conn.execute(
                    """UPDATE role_leases
                       SET status = ?, released_at = ?, handoff_suspended = 0,
                           updated_at = ?
                       WHERE role_lease_id = ?""",
                    (
                        terminal_status.value,
                        _timestamp(current_time),
                        _timestamp(current_time),
                        current.role_lease_id,
                    ),
                )
                if terminal_status is RoleLeaseStatus.REVOKED:
                    revoked.append(current.role_lease_id)
                    continue

                remaining = (current.expires_at - current_time).total_seconds()
                if remaining <= 0:
                    revoked.append(current.role_lease_id)
                    continue
                scope_key = _scope_key(
                    world_id=current.world_id,
                    role_class=current.role_class,
                    lineage_id=current.lineage_id,
                    scope=current.scope,
                )
                role_epoch = self._allocate_epoch(conn, scope_key)
                renew_after = current.renew_after
                rebound_status = (
                    RoleLeaseStatus.ACTIVE
                    if handoff_suspended
                    else current.status
                )
                rebound.append(
                    self._insert(
                        conn,
                        role_lease_id=uuid.uuid4().hex,
                        world_id=current.world_id,
                        lineage_id=current.lineage_id,
                        holder_kind=current.holder_kind,
                        holder_id=current.holder_id,
                        role_class=current.role_class,
                        role_label=current.role_label,
                        scope=current.scope,
                        obligation=current.obligation,
                        purpose_revision_id=current.purpose_revision_id,
                        issuer_kind="runtime",
                        issuer_id=runtime.owner_id,
                        role_epoch=role_epoch,
                        runtime=runtime,
                        valid_from=current.valid_from,
                        expires_at=current.expires_at,
                        renew_after=renew_after,
                        status=rebound_status,
                        predecessor_lease_id=current.role_lease_id,
                        renewal_count=current.renewal_count,
                        last_evidence_event_id=current.last_evidence_event_id,
                        now=current_time,
                        accountability_cycle_id=(
                            current.accountability_cycle_id
                        ),
                    )
                )
        return {"rebound": tuple(rebound), "revoked": tuple(revoked)}

    def recover_runtime_shutdown(
        self,
        runtime: RuntimeLeaseProof,
        *,
        recovery_permit: RuntimeRecoveryPermit,
        now: datetime | None = None,
    ) -> dict[str, tuple[str, ...]]:
        """Fence this epoch's roles without erasing persistent subject intent.

        Temporary task roles are revoked.  Subject roles are suspended so a
        successor Runtime may explicitly rebind the same bounded authority;
        no expiry or renewal deadline is extended here.
        """

        from pulse_system.core.runtime.publication import RuntimeRecoveryPermit

        if not isinstance(runtime, RuntimeLeaseProof):
            raise RoleLeaseValidationError(
                "recover_runtime_shutdown requires a RuntimeLeaseProof"
            )
        if not isinstance(recovery_permit, RuntimeRecoveryPermit):
            raise RoleLeaseValidationError(
                "recovery_permit must be a RuntimeRecoveryPermit"
            )
        current_time = self._now(now)
        suspended: list[str] = []
        revoked: list[str] = []
        with self._write_transaction(recovery_permit=recovery_permit) as conn:
            rows = conn.execute(
                _ROLE_ROW_SELECT + " WHERE role_leases.world_id = ? "
                "AND runtime_owner_id = ? AND runtime_epoch = ? "
                "AND status IN ('requested', 'active', 'suspended') "
                "ORDER BY role_leases.scope_key, role_leases.role_epoch",
                (runtime.world_id, runtime.owner_id, runtime.epoch),
            ).fetchall()
            for row in rows:
                lease = self._from_row(row)
                target = (
                    RoleLeaseStatus.SUSPENDED
                    if lease.role_class is RoleClass.SUBJECT_ROLE
                    else RoleLeaseStatus.REVOKED
                )
                updated = conn.execute(
                    "UPDATE role_leases SET status = ?, updated_at = ?, "
                    "released_at = ?, handoff_suspended = ? "
                    "WHERE role_lease_id = ? "
                    "AND status IN ('requested', 'active', 'suspended')",
                    (
                        target.value,
                        _timestamp(current_time),
                        (
                            None
                            if target is RoleLeaseStatus.SUSPENDED
                            else _timestamp(current_time)
                        ),
                        (
                            1
                            if target is RoleLeaseStatus.SUSPENDED
                            and lease.status is RoleLeaseStatus.ACTIVE
                            else int(row["handoff_suspended"])
                        ),
                        lease.role_lease_id,
                    ),
                )
                if updated.rowcount != 1:
                    raise RoleLeaseConflictError(
                        "role lease changed during Runtime shutdown recovery"
                    )
                if target is RoleLeaseStatus.SUSPENDED:
                    suspended.append(lease.role_lease_id)
                else:
                    revoked.append(lease.role_lease_id)
        return {
            "suspended": tuple(suspended),
            "revoked": tuple(revoked),
        }


class RoleLeaseNotFoundError(RoleLeaseError):
    """The requested role lease id is absent."""

    code = "role_lease_not_found"


# The alias makes the domain name explicit for callers that use “ledger” for
# every durable authority component, without creating a second implementation.
RoleLeaseLedger = RoleLeaseStore

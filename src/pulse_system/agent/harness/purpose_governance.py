"""Durable purpose amendments for one continuous subject lineage.

This module owns the three SQLite tables needed for a subject lineage, its
append-only purpose history, and settlement-fenced amendment proposals.  It
can still be used independently for low-level lineage contracts; the proposal
path additionally verifies canonical ``harness_turns`` and ``causal_events``
from the caller-owned database.  The default evidence class remains
``CONTRACT_ONLY`` because this module alone does not establish a provider, Pi,
or live-gate claim.

The database contains bounded identifiers, a canonical purpose text and its
SHA-256 digest, source identifiers, and timestamps.  It has no generic
metadata or execution-payload column: prompts, credentials, and secrets are
outside this domain's input surface.  Purpose text itself is the subject's
purpose, not a hidden harness prompt.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
import unicodedata
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from pulse_system.core.runtime.publication import RuntimePublicationPermit

__all__ = [
    "CONTRACT_ONLY",
    "LIVE",
    "LIVE_GATE_UNVERIFIED",
    "LineageState",
    "PurposeAmendmentKind",
    "PurposeAmendmentProposal",
    "PurposeAmendmentProposalState",
    "PurposeEvidenceClass",
    "PurposeGovernance",
    "PurposeGovernanceError",
    "PurposeLineageConflictError",
    "PurposeLineageNotFoundError",
    "PurposeRecoveryError",
    "PurposeRevision",
    "PurposeRevisionCollisionError",
    "PurposeRevisionConflictError",
    "PurposeRevisionNotFoundError",
    "PurposeRevisionState",
    "PurposeProposalConflictError",
    "PurposeReflectionRequiredError",
    "PurposeSchemaError",
    "PurposeValidationError",
    "SubjectLineage",
]


CONTRACT_ONLY = "CONTRACT_ONLY"
LIVE_GATE_UNVERIFIED = "LIVE_GATE_UNVERIFIED"
LIVE = "LIVE"

MAX_IDENTIFIER_LENGTH = 128
MAX_PURPOSE_CHARS = 4000
MAX_HISTORY_LIMIT = 1000
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_PURPOSE_ROOT_KINDS = frozenset({"stimulus", "spontaneous", "pulse", "propagation"})
_PURPOSE_ROOT_DOMAINS = frozenset({"pulse", "world", "habitat"})
_PURPOSE_ROOT_SOURCES = frozenset(
    {"user", "self", "habitat", "sensory", "propagation"}
)
_PROVENANCE_REQUIRED_SOURCES = frozenset(
    {"user", "habitat", "sensory"}
)


class PurposeEvidenceClass(StrEnum):
    """Evidence labels; this module emits only ``CONTRACT_ONLY``."""

    CONTRACT_ONLY = CONTRACT_ONLY
    LIVE_GATE_UNVERIFIED = LIVE_GATE_UNVERIFIED
    LIVE = LIVE


class LineageState(StrEnum):
    ACTIVE = "active"
    ARCHIVED = "archived"


class PurposeAmendmentKind(StrEnum):
    ESTABLISH = "establish"
    AMEND = "amend"
    WITHDRAW = "withdraw"


class PurposeRevisionState(StrEnum):
    CURRENT = "current"
    SUPERSEDED = "superseded"
    WITHDRAWN = "withdrawn"


class PurposeAmendmentProposalState(StrEnum):
    """Settlement-fenced lifecycle of one subject-authored amendment attempt."""

    PENDING = "pending"
    COMMITTED = "committed"
    REJECTED = "rejected"
    UNCERTAIN = "uncertain"
    CONFLICTED = "conflicted"


class PurposeGovernanceError(RuntimeError):
    """Base error for fail-closed purpose-domain operations."""


class PurposeValidationError(ValueError, PurposeGovernanceError):
    """An identifier, revision, content, or database argument is unsafe."""


class PurposeSchemaError(PurposeGovernanceError):
    """The existing database does not match this module's additive schema."""


class PurposeRecoveryError(PurposeGovernanceError):
    """Durable rows violate lineage invariants and require operator review."""


class PurposeLineageNotFoundError(PurposeGovernanceError):
    """The requested subject lineage does not exist."""


class PurposeRevisionNotFoundError(PurposeGovernanceError):
    """The requested purpose revision does not exist."""


class PurposeLineageConflictError(PurposeGovernanceError):
    """A lineage create or succession CAS lost to another writer."""


class PurposeRevisionConflictError(PurposeGovernanceError):
    """The expected current purpose revision is stale."""

    def __init__(self, *, expected_revision: int | None, current_revision: int | None):
        self.expected_revision = expected_revision
        self.current_revision = current_revision
        super().__init__("purpose current revision CAS conflict")


class PurposeRevisionCollisionError(PurposeGovernanceError):
    """A stable revision id was reused for a different immutable request."""


class PurposeProposalConflictError(PurposeGovernanceError):
    """One Harness turn attempted to stage more than one immutable proposal."""


class PurposeReflectionRequiredError(PurposeGovernanceError):
    """The proposed mutation did not originate in an eligible life turn."""


def _identifier(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise PurposeValidationError(f"{field_name} must be a bounded identifier")
    if (
        not value
        or value != value.strip()
        or len(value) > MAX_IDENTIFIER_LENGTH
        or _IDENTIFIER_RE.fullmatch(value) is None
    ):
        raise PurposeValidationError(
            f"{field_name} must be a bounded identifier without surrounding whitespace"
        )
    return value


def _optional_identifier(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _identifier(value, field_name)


def _revision_number(value: Any, field_name: str = "revision") -> int:
    if type(value) is not int or value < 1:
        raise PurposeValidationError(f"{field_name} must be an integer >= 1")
    return value


def _optional_revision(value: Any) -> int | None:
    if value is None:
        return None
    return _revision_number(value, "expected_revision")


def _content(value: Any, *, allow_withdraw: bool) -> str | None:
    if value is None:
        if allow_withdraw:
            return None
        raise PurposeValidationError("purpose content is required")
    if not isinstance(value, str):
        raise PurposeValidationError("purpose content must be text")
    if "\x00" in value:
        raise PurposeValidationError("purpose content must not contain NUL")
    normalized = unicodedata.normalize("NFC", value).strip()
    if not normalized:
        raise PurposeValidationError("purpose content must not be empty")
    if len(normalized) > MAX_PURPOSE_CHARS:
        raise PurposeValidationError(
            f"purpose content must be at most {MAX_PURPOSE_CHARS} characters"
        )
    return normalized


def _digest(content: str | None) -> str:
    canonical = "" if content is None else content
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise PurposeRecoveryError("purpose timestamp is not valid ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PurposeRecoveryError("purpose timestamp is not timezone-aware")
    return parsed.astimezone(timezone.utc)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


@dataclass(frozen=True, slots=True)
class SubjectLineage:
    """Safe durable projection of one continuous subject identity."""

    lineage_id: str
    world_id: str
    root_engram_id: str
    current_engram_id: str
    current_purpose_revision_id: str | None
    generation: int
    state: LineageState
    created_at: datetime
    updated_at: datetime
    evidence_class: PurposeEvidenceClass = PurposeEvidenceClass.CONTRACT_ONLY

    def to_dict(self) -> dict[str, Any]:
        return {
            "lineage_id": self.lineage_id,
            "world_id": self.world_id,
            "root_engram_id": self.root_engram_id,
            "current_engram_id": self.current_engram_id,
            "current_purpose_revision_id": self.current_purpose_revision_id,
            "generation": self.generation,
            "state": self.state.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "evidence_class": self.evidence_class.value,
        }


@dataclass(frozen=True, slots=True)
class PurposeRevision:
    """Safe projection of an immutable purpose revision."""

    purpose_revision_id: str
    lineage_id: str
    author_engram_id: str
    revision: int
    predecessor_revision_id: str | None
    amendment_kind: PurposeAmendmentKind
    content: str | None
    source_event_id: str
    reflection_event_id: str | None
    content_digest: str
    state: PurposeRevisionState
    created_at: datetime
    superseded_at: datetime | None
    evidence_class: PurposeEvidenceClass = PurposeEvidenceClass.CONTRACT_ONLY

    def to_dict(self) -> dict[str, Any]:
        return {
            "purpose_revision_id": self.purpose_revision_id,
            "lineage_id": self.lineage_id,
            "author_engram_id": self.author_engram_id,
            "revision": self.revision,
            "predecessor_revision_id": self.predecessor_revision_id,
            "amendment_kind": self.amendment_kind.value,
            "content": self.content,
            "source_event_id": self.source_event_id,
            "reflection_event_id": self.reflection_event_id,
            "content_digest": self.content_digest,
            "state": self.state.value,
            "created_at": self.created_at.isoformat(),
            "superseded_at": (
                None if self.superseded_at is None else self.superseded_at.isoformat()
            ),
            "evidence_class": self.evidence_class.value,
        }


@dataclass(frozen=True, slots=True)
class PurposeAmendmentProposal:
    """An immutable proposal whose terminal state is fenced by Harness settlement."""

    proposal_id: str
    lineage_id: str
    author_engram_id: str
    harness_turn_id: str
    tool_call_event_id: str
    tool_call_id: str
    expected_revision: int | None
    amendment_kind: PurposeAmendmentKind
    content: str | None
    content_digest: str
    source_event_id: str
    source_causal_id: str
    source_kind: str
    source_domain: str
    source_flow: str | None
    source_center_id: str | None
    source_provenance_digest: str | None
    state: PurposeAmendmentProposalState
    committed_revision_id: str | None
    result_event_id: str | None
    resolution_code: str | None
    created_at: datetime
    resolved_at: datetime | None
    evidence_class: PurposeEvidenceClass = PurposeEvidenceClass.CONTRACT_ONLY

    def to_dict(self) -> dict[str, Any]:
        return {
            "proposal_id": self.proposal_id,
            "lineage_id": self.lineage_id,
            "author_engram_id": self.author_engram_id,
            "harness_turn_id": self.harness_turn_id,
            "tool_call_event_id": self.tool_call_event_id,
            "tool_call_id": self.tool_call_id,
            "expected_revision": self.expected_revision,
            "amendment_kind": self.amendment_kind.value,
            "content": self.content,
            "content_digest": self.content_digest,
            "source_event_id": self.source_event_id,
            "source_causal_id": self.source_causal_id,
            "source_kind": self.source_kind,
            "source_domain": self.source_domain,
            "source_flow": self.source_flow,
            "source_center_id": self.source_center_id,
            "source_provenance_digest": self.source_provenance_digest,
            "state": self.state.value,
            "committed_revision_id": self.committed_revision_id,
            "result_event_id": self.result_event_id,
            "resolution_code": self.resolution_code,
            "created_at": self.created_at.isoformat(),
            "resolved_at": (
                None if self.resolved_at is None else self.resolved_at.isoformat()
            ),
            "evidence_class": self.evidence_class.value,
        }


_SCHEMA = """
CREATE TABLE IF NOT EXISTS subject_lineages (
    lineage_id TEXT PRIMARY KEY
        CHECK(length(trim(lineage_id)) BETWEEN 1 AND 128),
    world_id TEXT NOT NULL
        CHECK(length(trim(world_id)) BETWEEN 1 AND 128),
    root_engram_id TEXT NOT NULL
        CHECK(length(trim(root_engram_id)) BETWEEN 1 AND 128),
    current_engram_id TEXT NOT NULL
        CHECK(length(trim(current_engram_id)) BETWEEN 1 AND 128),
    current_purpose_revision_id TEXT,
    generation INTEGER NOT NULL
        CHECK(typeof(generation) = 'integer' AND generation >= 0),
    state TEXT NOT NULL CHECK(state IN ('active', 'archived')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(current_purpose_revision_id)
        REFERENCES purpose_revisions(purpose_revision_id)
);

CREATE TABLE IF NOT EXISTS purpose_revisions (
    purpose_revision_id TEXT PRIMARY KEY
        CHECK(length(trim(purpose_revision_id)) BETWEEN 1 AND 128),
    lineage_id TEXT NOT NULL
        REFERENCES subject_lineages(lineage_id),
    author_engram_id TEXT NOT NULL
        CHECK(length(trim(author_engram_id)) BETWEEN 1 AND 128),
    revision INTEGER NOT NULL
        CHECK(typeof(revision) = 'integer' AND revision >= 1),
    predecessor_revision_id TEXT
        REFERENCES purpose_revisions(purpose_revision_id),
    amendment_kind TEXT NOT NULL
        CHECK(amendment_kind IN ('establish', 'amend', 'withdraw')),
    content TEXT,
    source_event_id TEXT NOT NULL
        CHECK(length(trim(source_event_id)) BETWEEN 1 AND 128),
    reflection_event_id TEXT
        CHECK(reflection_event_id IS NULL OR length(trim(reflection_event_id)) BETWEEN 1 AND 128),
    content_digest TEXT NOT NULL CHECK(length(content_digest) = 64),
    state TEXT NOT NULL CHECK(state IN ('current', 'superseded', 'withdrawn')),
    created_at TEXT NOT NULL,
    superseded_at TEXT,
    UNIQUE(lineage_id, revision),
    CHECK(
        (amendment_kind = 'withdraw' AND content IS NULL)
        OR (amendment_kind IN ('establish', 'amend') AND content IS NOT NULL)
    ),
    CHECK(
        (state = 'withdrawn' AND amendment_kind = 'withdraw')
        OR (state <> 'withdrawn')
    )
);

CREATE TABLE IF NOT EXISTS purpose_amendment_proposals (
    proposal_id TEXT PRIMARY KEY
        CHECK(length(trim(proposal_id)) BETWEEN 1 AND 128),
    lineage_id TEXT NOT NULL REFERENCES subject_lineages(lineage_id),
    author_engram_id TEXT NOT NULL
        CHECK(length(trim(author_engram_id)) BETWEEN 1 AND 128),
    harness_turn_id TEXT NOT NULL UNIQUE REFERENCES harness_turns(id),
    tool_call_event_id TEXT NOT NULL REFERENCES causal_events(id),
    tool_call_id TEXT NOT NULL
        CHECK(length(trim(tool_call_id)) BETWEEN 1 AND 128),
    expected_revision INTEGER
        CHECK(expected_revision IS NULL OR (
            typeof(expected_revision) = 'integer' AND expected_revision >= 1
        )),
    amendment_kind TEXT NOT NULL
        CHECK(amendment_kind IN ('establish', 'amend', 'withdraw')),
    content TEXT,
    content_digest TEXT NOT NULL CHECK(length(content_digest) = 64),
    source_event_id TEXT NOT NULL REFERENCES causal_events(id),
    source_causal_id TEXT NOT NULL
        CHECK(length(trim(source_causal_id)) BETWEEN 1 AND 128),
    source_kind TEXT NOT NULL
        CHECK(length(trim(source_kind)) BETWEEN 1 AND 64),
    source_domain TEXT NOT NULL
        CHECK(length(trim(source_domain)) BETWEEN 1 AND 64),
    source_flow TEXT,
    source_center_id TEXT,
    source_provenance_digest TEXT
        CHECK(source_provenance_digest IS NULL OR length(source_provenance_digest) = 64),
    state TEXT NOT NULL
        CHECK(state IN ('pending', 'committed', 'rejected', 'uncertain', 'conflicted')),
    committed_revision_id TEXT REFERENCES purpose_revisions(purpose_revision_id),
    result_event_id TEXT REFERENCES causal_events(id),
    resolution_code TEXT,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    CHECK(
        (amendment_kind = 'withdraw' AND content IS NULL)
        OR (amendment_kind IN ('establish', 'amend') AND content IS NOT NULL)
    ),
    CHECK(
        (state = 'pending' AND committed_revision_id IS NULL
            AND result_event_id IS NULL AND resolution_code IS NULL
            AND resolved_at IS NULL)
        OR (state = 'committed' AND committed_revision_id = proposal_id
            AND result_event_id IS NOT NULL
            AND resolution_code = 'turn_settled' AND resolved_at IS NOT NULL)
        OR (state IN ('rejected', 'uncertain', 'conflicted')
            AND committed_revision_id IS NULL
            AND resolution_code IS NOT NULL AND resolved_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_purpose_current_per_lineage
    ON purpose_revisions(lineage_id) WHERE state = 'current';
CREATE INDEX IF NOT EXISTS idx_purpose_lineage_history
    ON purpose_revisions(lineage_id, revision);
CREATE INDEX IF NOT EXISTS idx_purpose_proposal_lineage_history
    ON purpose_amendment_proposals(lineage_id, created_at, proposal_id);
CREATE INDEX IF NOT EXISTS idx_purpose_proposal_state
    ON purpose_amendment_proposals(state, created_at, proposal_id);

CREATE TRIGGER IF NOT EXISTS purpose_revisions_immutable_fields
BEFORE UPDATE OF purpose_revision_id, lineage_id, author_engram_id, revision,
    predecessor_revision_id, amendment_kind, content, source_event_id,
    reflection_event_id, content_digest, created_at ON purpose_revisions
WHEN OLD.purpose_revision_id IS NOT NEW.purpose_revision_id
    OR OLD.lineage_id IS NOT NEW.lineage_id
    OR OLD.author_engram_id IS NOT NEW.author_engram_id
    OR OLD.revision IS NOT NEW.revision
    OR OLD.predecessor_revision_id IS NOT NEW.predecessor_revision_id
    OR OLD.amendment_kind IS NOT NEW.amendment_kind
    OR OLD.content IS NOT NEW.content
    OR OLD.source_event_id IS NOT NEW.source_event_id
    OR OLD.reflection_event_id IS NOT NEW.reflection_event_id
    OR OLD.content_digest IS NOT NEW.content_digest
    OR OLD.created_at IS NOT NEW.created_at
BEGIN
    SELECT RAISE(ABORT, 'purpose revisions are append-only');
END;

CREATE TRIGGER IF NOT EXISTS purpose_revisions_state_guard
BEFORE UPDATE OF state, superseded_at ON purpose_revisions
WHEN NOT (
    (OLD.state IS NEW.state AND OLD.superseded_at IS NEW.superseded_at)
    OR (
        OLD.state = 'current'
        AND NEW.state = 'superseded'
        AND NEW.superseded_at IS NOT NULL
    )
)
BEGIN
    SELECT RAISE(ABORT, 'purpose revision state transition is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS purpose_revisions_no_delete
BEFORE DELETE ON purpose_revisions
BEGIN
    SELECT RAISE(ABORT, 'purpose revisions are append-only');
END;

CREATE TRIGGER IF NOT EXISTS purpose_proposals_immutable_fields
BEFORE UPDATE OF proposal_id, lineage_id, author_engram_id, harness_turn_id,
    tool_call_event_id, tool_call_id, expected_revision, amendment_kind,
    content, content_digest, source_event_id, source_causal_id, source_kind,
    source_domain, source_flow, source_center_id, source_provenance_digest,
    created_at ON purpose_amendment_proposals
WHEN OLD.proposal_id IS NOT NEW.proposal_id
    OR OLD.lineage_id IS NOT NEW.lineage_id
    OR OLD.author_engram_id IS NOT NEW.author_engram_id
    OR OLD.harness_turn_id IS NOT NEW.harness_turn_id
    OR OLD.tool_call_event_id IS NOT NEW.tool_call_event_id
    OR OLD.tool_call_id IS NOT NEW.tool_call_id
    OR OLD.expected_revision IS NOT NEW.expected_revision
    OR OLD.amendment_kind IS NOT NEW.amendment_kind
    OR OLD.content IS NOT NEW.content
    OR OLD.content_digest IS NOT NEW.content_digest
    OR OLD.source_event_id IS NOT NEW.source_event_id
    OR OLD.source_causal_id IS NOT NEW.source_causal_id
    OR OLD.source_kind IS NOT NEW.source_kind
    OR OLD.source_domain IS NOT NEW.source_domain
    OR OLD.source_flow IS NOT NEW.source_flow
    OR OLD.source_center_id IS NOT NEW.source_center_id
    OR OLD.source_provenance_digest IS NOT NEW.source_provenance_digest
    OR OLD.created_at IS NOT NEW.created_at
BEGIN
    SELECT RAISE(ABORT, 'purpose proposals have immutable request fields');
END;

CREATE TRIGGER IF NOT EXISTS purpose_proposals_state_guard
BEFORE UPDATE OF state, committed_revision_id, result_event_id,
    resolution_code, resolved_at ON purpose_amendment_proposals
WHEN NOT (
    OLD.state = 'pending'
    AND NEW.state IN ('committed', 'rejected', 'uncertain', 'conflicted')
    AND NEW.resolved_at IS NOT NULL
)
BEGIN
    SELECT RAISE(ABORT, 'purpose proposal state transition is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS purpose_proposals_no_delete
BEFORE DELETE ON purpose_amendment_proposals
BEGIN
    SELECT RAISE(ABORT, 'purpose proposals are append-only');
END;

CREATE TRIGGER IF NOT EXISTS subject_lineages_pointer_guard
BEFORE UPDATE OF current_purpose_revision_id ON subject_lineages
WHEN NEW.current_purpose_revision_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM purpose_revisions
        WHERE purpose_revision_id = NEW.current_purpose_revision_id
          AND lineage_id = NEW.lineage_id
          AND state = 'current'
    )
BEGIN
    SELECT RAISE(ABORT, 'lineage current purpose pointer is invalid');
END;
"""


_LINEAGE_COLUMNS = frozenset(
    {
        "lineage_id",
        "world_id",
        "root_engram_id",
        "current_engram_id",
        "current_purpose_revision_id",
        "generation",
        "state",
        "created_at",
        "updated_at",
    }
)
_REVISION_COLUMNS = frozenset(
    {
        "purpose_revision_id",
        "lineage_id",
        "author_engram_id",
        "revision",
        "predecessor_revision_id",
        "amendment_kind",
        "content",
        "source_event_id",
        "reflection_event_id",
        "content_digest",
        "state",
        "created_at",
        "superseded_at",
    }
)
_PROPOSAL_COLUMNS = frozenset(
    {
        "proposal_id",
        "lineage_id",
        "author_engram_id",
        "harness_turn_id",
        "tool_call_event_id",
        "tool_call_id",
        "expected_revision",
        "amendment_kind",
        "content",
        "content_digest",
        "source_event_id",
        "source_causal_id",
        "source_kind",
        "source_domain",
        "source_flow",
        "source_center_id",
        "source_provenance_digest",
        "state",
        "committed_revision_id",
        "result_event_id",
        "resolution_code",
        "created_at",
        "resolved_at",
    }
)


class PurposeGovernance:
    """SQLite-backed purpose and lineage domain with fail-closed boundaries.

    ``database`` may be a filesystem path or a caller-owned
    :class:`sqlite3.Connection`.  A path opens a private connection and is
    safe to use from multiple instances against the same database.  Every
    mutation starts ``BEGIN IMMEDIATE``; the current pointer and the unique
    current-revision index therefore form one durable CAS boundary.

    The instance evidence class is deliberately fixed to ``CONTRACT_ONLY``.
    Runtime integration establishes the subject-turn boundary separately and
    may label its external projection ``LIVE_GATE_UNVERIFIED`` only when the
    durable causal chain has been verified here.
    """

    evidence_class = PurposeEvidenceClass.CONTRACT_ONLY

    def __init__(
        self,
        database: str | Path | sqlite3.Connection,
        *,
        publication_permit: RuntimePublicationPermit | None = None,
    ):
        from pulse_system.core.runtime.publication import RuntimePublicationPermit

        self._lock = threading.RLock()
        self._closed = False
        if publication_permit is not None and not isinstance(
            publication_permit,
            RuntimePublicationPermit,
        ):
            raise TypeError("publication_permit must be a RuntimePublicationPermit or null")
        self._publication_permit = publication_permit
        self._owns_connection = not isinstance(database, sqlite3.Connection)
        if isinstance(database, sqlite3.Connection):
            self._connection = database
        elif isinstance(database, (str, Path)):
            self._connection = sqlite3.connect(
                str(database),
                timeout=5.0,
                check_same_thread=False,
                isolation_level=None,
            )
        else:
            raise TypeError("database must be a path or sqlite3.Connection")

        try:
            guard = (
                nullcontext()
                if self._publication_permit is None
                else self._publication_permit.transaction_guard()
            )
            with guard:
                self._connection.execute("PRAGMA foreign_keys = ON")
                self._connection.execute("PRAGMA busy_timeout = 5000")
                self._connection.execute("PRAGMA journal_mode = WAL")
                self._connection.executescript(_SCHEMA)
            self._verify_schema()
            self._assert_all_integrity()
        except BaseException:
            if self._owns_connection:
                self._connection.close()
            raise

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._owns_connection:
                self._connection.close()

    def __enter__(self) -> "PurposeGovernance":
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise PurposeGovernanceError("purpose governance is closed")

    def _verify_schema(self) -> None:
        for table, expected in (
            ("subject_lineages", _LINEAGE_COLUMNS),
            ("purpose_revisions", _REVISION_COLUMNS),
            ("purpose_amendment_proposals", _PROPOSAL_COLUMNS),
        ):
            row = self._connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table,),
            ).fetchone()
            if row is None:
                raise PurposeSchemaError(f"missing purpose table: {table}")
            actual = {
                str(item[1])
                for item in self._connection.execute(f"PRAGMA table_info({table})")
            }
            missing = expected - actual
            if missing:
                raise PurposeSchemaError(
                    f"incompatible {table} schema; missing columns: {sorted(missing)}"
                )

    def _assert_all_integrity(self) -> None:
        rows = self._connection.execute(
            "SELECT lineage_id FROM subject_lineages ORDER BY lineage_id"
        ).fetchall()
        for (lineage_id,) in rows:
            self._assert_lineage_integrity(str(lineage_id))
        proposal_rows = self._connection.execute(
            """SELECT proposal_id FROM purpose_amendment_proposals
               ORDER BY created_at, proposal_id"""
        ).fetchall()
        for (proposal_id,) in proposal_rows:
            self._assert_proposal_integrity(str(proposal_id))

    def _assert_lineage_integrity(self, lineage_id: str) -> None:
        try:
            lineage_id = _identifier(lineage_id, "lineage_id")
        except PurposeValidationError as exc:
            raise PurposeRecoveryError("subject lineage has an invalid identifier") from exc
        row = self._fetch_lineage_row(lineage_id)
        if row is None:
            raise PurposeLineageNotFoundError("subject lineage does not exist")
        try:
            _identifier(str(row[0]), "lineage_id")
            _identifier(str(row[1]), "world_id")
            _identifier(str(row[2]), "root_engram_id")
            _identifier(str(row[3]), "current_engram_id")
            if row[4] is not None:
                _identifier(str(row[4]), "current_purpose_revision_id")
            generation = int(row[5])
            lineage_state = LineageState(str(row[6]))
            _timestamp(str(row[7]))
            _timestamp(str(row[8]))
        except (TypeError, ValueError) as exc:
            raise PurposeRecoveryError("subject lineage has invalid state") from exc
        if generation < 0:
            raise PurposeRecoveryError("subject lineage has invalid generation")
        if not str(row[1]) or not str(row[2]) or not str(row[3]):
            raise PurposeRecoveryError("subject lineage has an unbounded identity")
        if lineage_state not in {LineageState.ACTIVE, LineageState.ARCHIVED}:
            raise PurposeRecoveryError("subject lineage has invalid lifecycle state")

        revisions = self._connection.execute(
            """SELECT purpose_revision_id, author_engram_id, revision,
                      predecessor_revision_id, amendment_kind, content,
                      source_event_id, reflection_event_id, content_digest,
                      state, superseded_at
               FROM purpose_revisions
               WHERE lineage_id = ? ORDER BY revision""",
            (lineage_id,),
        ).fetchall()
        current_rows = []
        previous_id: str | None = None
        for expected_revision, revision_row in enumerate(revisions, start=1):
            revision_id = str(revision_row[0])
            author_engram_id = str(revision_row[1])
            try:
                revision = int(revision_row[2])
            except (TypeError, ValueError) as exc:
                raise PurposeRecoveryError("purpose revision number is invalid") from exc
            predecessor = None if revision_row[3] is None else str(revision_row[3])
            kind = str(revision_row[4])
            content = None if revision_row[5] is None else str(revision_row[5])
            source_event_id = str(revision_row[6])
            reflection_event_id = (
                None if revision_row[7] is None else str(revision_row[7])
            )
            content_digest = str(revision_row[8])
            state = str(revision_row[9])
            superseded_at = revision_row[10]
            if revision != expected_revision:
                raise PurposeRecoveryError("purpose revision sequence has a gap")
            try:
                _identifier(revision_id, "purpose_revision_id")
                _identifier(str(lineage_id), "lineage_id")
                _identifier(author_engram_id, "author_engram_id")
                _identifier(source_event_id, "source_event_id")
                if revision_row[3] is not None:
                    _identifier(predecessor, "predecessor_revision_id")
                _identifier(str(revision), "revision")
                if reflection_event_id is not None:
                    _identifier(reflection_event_id, "reflection_event_id")
            except PurposeValidationError as exc:
                raise PurposeRecoveryError("purpose revision has an invalid identifier") from exc
            if predecessor != previous_id:
                raise PurposeRecoveryError("purpose revision lineage is broken")
            if not _DIGEST_RE.fullmatch(content_digest):
                raise PurposeRecoveryError("purpose content digest is not SHA-256")
            if content is not None:
                try:
                    if _content(content, allow_withdraw=False) != content:
                        raise PurposeRecoveryError(
                            "purpose content is not canonical UTF-8 text"
                        )
                except PurposeValidationError as exc:
                    raise PurposeRecoveryError("purpose content is invalid") from exc
            if content_digest != _digest(content):
                raise PurposeRecoveryError("purpose content digest does not match")
            if state not in {item.value for item in PurposeRevisionState}:
                raise PurposeRecoveryError("purpose revision has invalid state")
            if state == PurposeRevisionState.SUPERSEDED.value:
                if superseded_at is None:
                    raise PurposeRecoveryError("superseded revision has no timestamp")
                _timestamp(str(superseded_at))
            elif superseded_at is not None:
                raise PurposeRecoveryError("non-superseded revision has a superseded timestamp")
            if kind == PurposeAmendmentKind.WITHDRAW.value:
                if content is not None or state != PurposeRevisionState.WITHDRAWN.value:
                    raise PurposeRecoveryError("withdraw revision has invalid shape")
            elif kind in {
                PurposeAmendmentKind.ESTABLISH.value,
                PurposeAmendmentKind.AMEND.value,
            }:
                if content is None or state == PurposeRevisionState.WITHDRAWN.value:
                    raise PurposeRecoveryError("purpose revision has invalid shape")
            else:
                raise PurposeRecoveryError("purpose revision has invalid amendment kind")
            if state == PurposeRevisionState.CURRENT.value:
                current_rows.append(revision_id)
            previous_id = revision_id

        pointer = None if row[4] is None else str(row[4])
        if pointer is None:
            if current_rows:
                raise PurposeRecoveryError(
                    "lineage has a current revision but no current pointer"
                )
        elif current_rows != [pointer]:
            raise PurposeRecoveryError("lineage current pointer is inconsistent")

    def _assert_proposal_integrity(self, proposal_id: str) -> None:
        row = self._fetch_proposal_row(proposal_id)
        if row is None:
            raise PurposeRecoveryError("purpose proposal disappeared during recovery")
        try:
            proposal = self._proposal_from_row(row)
            for field_name, value in (
                ("proposal_id", proposal.proposal_id),
                ("lineage_id", proposal.lineage_id),
                ("author_engram_id", proposal.author_engram_id),
                ("harness_turn_id", proposal.harness_turn_id),
                ("tool_call_event_id", proposal.tool_call_event_id),
                ("tool_call_id", proposal.tool_call_id),
                ("source_event_id", proposal.source_event_id),
                ("source_causal_id", proposal.source_causal_id),
            ):
                _identifier(value, field_name)
            _optional_revision(proposal.expected_revision)
            _content(
                proposal.content,
                allow_withdraw=(
                    proposal.amendment_kind is PurposeAmendmentKind.WITHDRAW
                ),
            )
            if proposal.content_digest != _digest(proposal.content):
                raise PurposeRecoveryError("purpose proposal content digest diverged")
            if proposal.source_provenance_digest is not None and (
                _DIGEST_RE.fullmatch(proposal.source_provenance_digest) is None
            ):
                raise PurposeRecoveryError(
                    "purpose proposal provenance digest is malformed"
                )
            if proposal.source_center_id is not None:
                _identifier(proposal.source_center_id, "source_center_id")
            if proposal.committed_revision_id is not None:
                _identifier(
                    proposal.committed_revision_id,
                    "committed_revision_id",
                )
            if proposal.result_event_id is not None:
                _identifier(proposal.result_event_id, "result_event_id")
            if proposal.resolution_code is not None:
                _identifier(proposal.resolution_code, "resolution_code")
        except (TypeError, ValueError) as exc:
            if isinstance(exc, PurposeRecoveryError):
                raise
            raise PurposeRecoveryError("purpose proposal has invalid state") from exc

        lineage_row = self._fetch_lineage_row(proposal.lineage_id)
        if lineage_row is None:
            raise PurposeRecoveryError("purpose proposal lineage is missing")
        turn = self._connection.execute(
            """SELECT event_id, engram_id, state, result_event_id
               FROM harness_turns WHERE id = ?""",
            (proposal.harness_turn_id,),
        ).fetchone()
        if (
            turn is None
            or str(turn[0]) != proposal.source_event_id
            or str(turn[1]) != proposal.author_engram_id
        ):
            raise PurposeRecoveryError("purpose proposal turn binding diverged")
        root = self._causal_event_uncommitted(
            self._connection,
            proposal.source_event_id,
        )
        if (
            root["world_id"] != str(lineage_row[1])
            or root["causal_id"] != proposal.source_causal_id
            or root["engram_id"] != proposal.author_engram_id
            or root["kind"] not in _PURPOSE_ROOT_KINDS
            or root["domain"] not in _PURPOSE_ROOT_DOMAINS
            or root["domain"] != proposal.source_domain
            or root["source"] not in _PURPOSE_ROOT_SOURCES
            or root["source"] != proposal.source_kind
            or root["flow"] != proposal.source_flow
            or root["flow"] not in {None, "content"}
            or root["center_id"] != proposal.source_center_id
        ):
            raise PurposeRecoveryError("purpose proposal source projection diverged")
        if proposal.source_center_id is not None:
            center = self._connection.execute(
                "SELECT kind FROM activity_centers WHERE id = ?",
                (proposal.source_center_id,),
            ).fetchone()
            if center is None or str(center[0]) == "task":
                raise PurposeRecoveryError(
                    "purpose proposal source Center is missing or task-scoped"
                )
        try:
            provenance_digest = self._reflection_provenance_digest_uncommitted(
                self._connection,
                root,
            )
        except PurposeReflectionRequiredError as exc:
            raise PurposeRecoveryError(
                "purpose proposal source provenance is no longer valid"
            ) from exc
        if provenance_digest != proposal.source_provenance_digest:
            raise PurposeRecoveryError("purpose proposal provenance diverged")
        self._assert_tool_call_uncommitted(
            self._connection,
            tool_call_event_id=proposal.tool_call_event_id,
            tool_call_id=proposal.tool_call_id,
            root=root,
            author_engram_id=proposal.author_engram_id,
        )

        turn_state = str(turn[2])
        revision_row = self._fetch_revision_row(proposal.proposal_id)
        if proposal.state is PurposeAmendmentProposalState.COMMITTED:
            if turn_state != "settled" or turn[3] is None:
                raise PurposeRecoveryError(
                    "committed purpose proposal lacks a settled turn"
                )
            result_event_id = self._settled_reflection_result_uncommitted(
                self._connection,
                proposal,
                lineage_row,
            )
            if revision_row is None:
                raise PurposeRecoveryError(
                    "committed purpose proposal lacks its revision"
                )
            revision = self._revision_from_row(revision_row)
            if (
                proposal.committed_revision_id != revision.purpose_revision_id
                or proposal.result_event_id != result_event_id
                or revision.lineage_id != proposal.lineage_id
                or revision.author_engram_id != proposal.author_engram_id
                or revision.amendment_kind is not proposal.amendment_kind
                or revision.content != proposal.content
                or revision.content_digest != proposal.content_digest
                or revision.source_event_id != proposal.source_event_id
                or revision.reflection_event_id != result_event_id
            ):
                raise PurposeRecoveryError(
                    "committed purpose proposal and revision diverged"
                )
        elif proposal.state is PurposeAmendmentProposalState.REJECTED:
            if (
                turn_state != "failed"
                or proposal.resolution_code != "harness_turn_failed"
                or proposal.result_event_id is not None
                or revision_row is not None
            ):
                raise PurposeRecoveryError(
                    "rejected purpose proposal has invalid terminal evidence"
                )
        elif proposal.state is PurposeAmendmentProposalState.UNCERTAIN:
            if (
                turn_state != "uncertain"
                or proposal.resolution_code != "harness_turn_uncertain"
                or proposal.result_event_id is not None
                or revision_row is not None
            ):
                raise PurposeRecoveryError(
                    "uncertain purpose proposal has invalid terminal evidence"
                )
        elif proposal.state is PurposeAmendmentProposalState.CONFLICTED:
            result_event_id = self._settled_reflection_result_uncommitted(
                self._connection,
                proposal,
                lineage_row,
            )
            if (
                turn_state != "settled"
                or proposal.resolution_code
                not in {"lineage_holder_changed", "purpose_revision_conflict"}
                or proposal.result_event_id != result_event_id
                or revision_row is not None
            ):
                raise PurposeRecoveryError(
                    "conflicted purpose proposal has invalid terminal evidence"
                )
        elif proposal.state is PurposeAmendmentProposalState.PENDING:
            if revision_row is not None:
                raise PurposeRecoveryError(
                    "pending purpose proposal already has a revision"
                )
        else:
            raise PurposeRecoveryError("purpose proposal has unknown lifecycle state")

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        self._ensure_open()
        with self._lock:
            if self._connection.in_transaction:
                raise PurposeGovernanceError(
                    "purpose mutation cannot run inside an open caller transaction"
                )
            guard = (
                nullcontext()
                if self._publication_permit is None
                else self._publication_permit.transaction_guard()
            )
            with guard:
                try:
                    self._connection.execute("BEGIN IMMEDIATE")
                    yield self._connection
                except BaseException:
                    if self._connection.in_transaction:
                        self._connection.rollback()
                    raise
                else:
                    self._connection.commit()

    def _fetch_lineage_row(self, lineage_id: str):
        return self._connection.execute(
            """SELECT lineage_id, world_id, root_engram_id, current_engram_id,
                      current_purpose_revision_id, generation, state,
                      created_at, updated_at
               FROM subject_lineages WHERE lineage_id = ?""",
            (lineage_id,),
        ).fetchone()

    def _fetch_revision_row(self, revision_id: str):
        return self._connection.execute(
            """SELECT purpose_revision_id, lineage_id, author_engram_id,
                      revision, predecessor_revision_id, amendment_kind,
                      content, source_event_id, reflection_event_id,
                      content_digest, state, created_at, superseded_at
               FROM purpose_revisions WHERE purpose_revision_id = ?""",
            (revision_id,),
        ).fetchone()

    def _fetch_proposal_row(self, proposal_id: str):
        return self._connection.execute(
            """SELECT proposal_id, lineage_id, author_engram_id,
                      harness_turn_id, tool_call_event_id, tool_call_id,
                      expected_revision, amendment_kind, content,
                      content_digest, source_event_id, source_causal_id,
                      source_kind, source_domain, source_flow,
                      source_center_id, source_provenance_digest, state,
                      committed_revision_id, result_event_id,
                      resolution_code, created_at, resolved_at
               FROM purpose_amendment_proposals WHERE proposal_id = ?""",
            (proposal_id,),
        ).fetchone()

    @staticmethod
    def _lineage_from_row(row: tuple[Any, ...]) -> SubjectLineage:
        return SubjectLineage(
            lineage_id=str(row[0]),
            world_id=str(row[1]),
            root_engram_id=str(row[2]),
            current_engram_id=str(row[3]),
            current_purpose_revision_id=(
                None if row[4] is None else str(row[4])
            ),
            generation=int(row[5]),
            state=LineageState(str(row[6])),
            created_at=_timestamp(str(row[7])),
            updated_at=_timestamp(str(row[8])),
        )

    @staticmethod
    def _revision_from_row(row: tuple[Any, ...]) -> PurposeRevision:
        return PurposeRevision(
            purpose_revision_id=str(row[0]),
            lineage_id=str(row[1]),
            author_engram_id=str(row[2]),
            revision=int(row[3]),
            predecessor_revision_id=(None if row[4] is None else str(row[4])),
            amendment_kind=PurposeAmendmentKind(str(row[5])),
            content=None if row[6] is None else str(row[6]),
            source_event_id=str(row[7]),
            reflection_event_id=None if row[8] is None else str(row[8]),
            content_digest=str(row[9]),
            state=PurposeRevisionState(str(row[10])),
            created_at=_timestamp(str(row[11])),
            superseded_at=(None if row[12] is None else _timestamp(str(row[12]))),
        )

    @staticmethod
    def _proposal_from_row(row: tuple[Any, ...]) -> PurposeAmendmentProposal:
        return PurposeAmendmentProposal(
            proposal_id=str(row[0]),
            lineage_id=str(row[1]),
            author_engram_id=str(row[2]),
            harness_turn_id=str(row[3]),
            tool_call_event_id=str(row[4]),
            tool_call_id=str(row[5]),
            expected_revision=None if row[6] is None else int(row[6]),
            amendment_kind=PurposeAmendmentKind(str(row[7])),
            content=None if row[8] is None else str(row[8]),
            content_digest=str(row[9]),
            source_event_id=str(row[10]),
            source_causal_id=str(row[11]),
            source_kind=str(row[12]),
            source_domain=str(row[13]),
            source_flow=None if row[14] is None else str(row[14]),
            source_center_id=None if row[15] is None else str(row[15]),
            source_provenance_digest=(
                None if row[16] is None else str(row[16])
            ),
            state=PurposeAmendmentProposalState(str(row[17])),
            committed_revision_id=None if row[18] is None else str(row[18]),
            result_event_id=None if row[19] is None else str(row[19]),
            resolution_code=None if row[20] is None else str(row[20]),
            created_at=_timestamp(str(row[21])),
            resolved_at=None if row[22] is None else _timestamp(str(row[22])),
        )

    @staticmethod
    def _json_object(raw: Any, field_name: str) -> dict[str, Any]:
        try:
            value = json.loads(str(raw))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PurposeRecoveryError(f"{field_name} is not canonical JSON") from exc
        if not isinstance(value, dict):
            raise PurposeRecoveryError(f"{field_name} is not a JSON object")
        return value

    @classmethod
    def _causal_event_uncommitted(
        cls,
        conn: sqlite3.Connection,
        event_id: str,
    ) -> dict[str, Any]:
        row = conn.execute(
            """SELECT id, world_id, causal_id, parent_event_id, engram_id,
                      center_id, flow, domain, kind, source, status, metadata
               FROM causal_events WHERE id = ?""",
            (event_id,),
        ).fetchone()
        if row is None:
            raise PurposeRecoveryError("purpose provenance references a missing event")
        return {
            "id": str(row[0]),
            "world_id": str(row[1]),
            "causal_id": str(row[2]),
            "parent_event_id": None if row[3] is None else str(row[3]),
            "engram_id": None if row[4] is None else str(row[4]),
            "center_id": None if row[5] is None else str(row[5]),
            "flow": None if row[6] is None else str(row[6]),
            "domain": str(row[7]),
            "kind": str(row[8]),
            "source": str(row[9]),
            "status": str(row[10]),
            "metadata": cls._json_object(row[11], "causal event metadata"),
        }

    @classmethod
    def _eligible_reflection_context_uncommitted(
        cls,
        conn: sqlite3.Connection,
        *,
        lineage_row: tuple[Any, ...],
        author_engram_id: str,
        harness_turn_id: str,
        source_event_id: str,
        expected_turn_state: str,
    ) -> tuple[dict[str, Any], tuple[Any, ...], str | None]:
        turn_row = conn.execute(
            """SELECT event_id, engram_id, state, prompt_accepted,
                      result_event_id
               FROM harness_turns WHERE id = ?""",
            (harness_turn_id,),
        ).fetchone()
        if turn_row is None:
            raise PurposeReflectionRequiredError(
                "purpose proposal requires a durable Harness turn"
            )
        if (
            str(turn_row[0]) != source_event_id
            or str(turn_row[1]) != author_engram_id
            or str(turn_row[2]) != expected_turn_state
        ):
            raise PurposeReflectionRequiredError(
                "purpose proposal is not bound to the expected subject turn"
            )

        root = cls._causal_event_uncommitted(conn, source_event_id)
        expected_root_status = "running" if expected_turn_state == "running" else "settled"
        if (
            root["world_id"] != str(lineage_row[1])
            or root["engram_id"] != author_engram_id
            or root["status"] != expected_root_status
            or root["kind"] not in _PURPOSE_ROOT_KINDS
            or root["domain"] not in _PURPOSE_ROOT_DOMAINS
            or root["source"] not in _PURPOSE_ROOT_SOURCES
            or root["flow"] not in {None, "content"}
        ):
            raise PurposeReflectionRequiredError(
                "purpose proposal requires an eligible life/reflection root"
            )
        center_id = root["center_id"]
        if center_id is not None:
            center = conn.execute(
                "SELECT kind FROM activity_centers WHERE id = ?",
                (center_id,),
            ).fetchone()
            if center is None or str(center[0]) == "task":
                raise PurposeReflectionRequiredError(
                    "task or missing Center cannot authorize purpose mutation"
                )

        provenance_digest = cls._reflection_provenance_digest_uncommitted(
            conn,
            root,
        )
        return root, turn_row, provenance_digest

    @classmethod
    def _reflection_provenance_digest_uncommitted(
        cls,
        conn: sqlite3.Connection,
        root: dict[str, Any],
    ) -> str | None:
        metadata = root["metadata"]
        provenance_digest = metadata.get("stimulus_provenance_digest")
        if provenance_digest is not None and (
            not isinstance(provenance_digest, str)
            or _DIGEST_RE.fullmatch(provenance_digest) is None
        ):
            raise PurposeRecoveryError("stimulus provenance digest is malformed")
        if root["source"] in _PROVENANCE_REQUIRED_SOURCES:
            expected_class = {
                "user": "user_input",
                "habitat": "external_consequence",
                "sensory": "external_consequence",
            }[root["source"]]
            if (
                provenance_digest is None
                or metadata.get("stimulus_class") != expected_class
                or metadata.get("stimulus_evidence_class") != LIVE
            ):
                raise PurposeReflectionRequiredError(
                    "non-self purpose reflection lacks live stimulus provenance"
                )
        elif root["source"] == "propagation":
            return cls._propagation_provenance_digest_uncommitted(conn, root)
        elif provenance_digest is not None and (
            metadata.get("stimulus_class") != "subject_reflection"
            or metadata.get("stimulus_evidence_class") != LIVE
        ):
            raise PurposeReflectionRequiredError(
                "typed self reflection provenance is inconsistent"
            )
        return provenance_digest

    @classmethod
    def _propagation_provenance_digest_uncommitted(
        cls,
        conn: sqlite3.Connection,
        root: dict[str, Any],
    ) -> str:
        """Rebuild canonical settled-turn propagation without trusting a label."""

        if root["kind"] != "propagation" or root["parent_event_id"] is None:
            raise PurposeReflectionRequiredError(
                "purpose reflection requires a canonical propagation child"
            )
        parent = cls._causal_event_uncommitted(conn, root["parent_event_id"])
        if parent["parent_event_id"] is None:
            raise PurposeReflectionRequiredError(
                "purpose reflection propagation lacks a source root"
            )
        source_root = cls._causal_event_uncommitted(
            conn,
            parent["parent_event_id"],
        )
        source_engram_id = root["metadata"].get("source_engram_id")
        depth = root["metadata"].get("depth")
        if (
            parent["world_id"] != root["world_id"]
            or parent["causal_id"] != root["causal_id"]
            or parent["center_id"] != root["center_id"]
            or parent["engram_id"] is None
            or parent["engram_id"] != source_engram_id
            or parent["kind"] != "assistant_result"
            or parent["domain"] != "harness"
            or parent["source"] != "self"
            or parent["status"] != "settled"
            or type(depth) is not int
            or depth < 1
        ):
            raise PurposeReflectionRequiredError(
                "purpose reflection propagation lacks a settled source result"
            )
        if (
            source_root["world_id"] != root["world_id"]
            or source_root["causal_id"] != root["causal_id"]
            or source_root["center_id"] != root["center_id"]
            or source_root["engram_id"] != parent["engram_id"]
            or source_root["status"] != "settled"
        ):
            raise PurposeReflectionRequiredError(
                "purpose reflection propagation source root diverged"
            )
        turns = conn.execute(
            """SELECT id, event_id, engram_id, state, prompt_accepted
               FROM harness_turns WHERE result_event_id = ?""",
            (parent["id"],),
        ).fetchall()
        if len(turns) != 1:
            raise PurposeReflectionRequiredError(
                "purpose reflection propagation lacks one source turn"
            )
        turn = turns[0]
        if (
            str(turn[1]) != parent["parent_event_id"]
            or str(turn[2]) != parent["engram_id"]
            or str(turn[3]) != "settled"
            or turn[4] != 1
        ):
            raise PurposeReflectionRequiredError(
                "purpose reflection propagation source turn is not settled"
            )
        payload = {
            "causal_id": root["causal_id"],
            "depth": depth,
            "source_engram_id": parent["engram_id"],
            "source_result_event_id": parent["id"],
            "source_root_event_id": source_root["id"],
            "source_turn_id": str(turn[0]),
            "target_engram_id": root["engram_id"],
            "target_event_id": root["id"],
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @classmethod
    def _assert_tool_call_uncommitted(
        cls,
        conn: sqlite3.Connection,
        *,
        tool_call_event_id: str,
        tool_call_id: str,
        root: dict[str, Any],
        author_engram_id: str,
    ) -> dict[str, Any]:
        tool_call = cls._causal_event_uncommitted(conn, tool_call_event_id)
        metadata = tool_call["metadata"]
        if (
            tool_call["parent_event_id"] != root["id"]
            or tool_call["causal_id"] != root["causal_id"]
            or tool_call["engram_id"] != author_engram_id
            or tool_call["kind"] != "tool_call"
            or tool_call["domain"] != "harness"
            or tool_call["source"] != "self"
            or tool_call["status"] != "settled"
            or metadata.get("tool_name") != "pulse_life_amend_purpose"
            or metadata.get("tool_call_id") != tool_call_id
        ):
            raise PurposeReflectionRequiredError(
                "purpose proposal tool call is not bound to the subject root"
            )
        return tool_call

    @classmethod
    def _settled_reflection_result_uncommitted(
        cls,
        conn: sqlite3.Connection,
        proposal: PurposeAmendmentProposal,
        lineage_row: tuple[Any, ...],
    ) -> str:
        root, turn_row, _provenance_digest = cls._eligible_reflection_context_uncommitted(
            conn,
            lineage_row=lineage_row,
            author_engram_id=proposal.author_engram_id,
            harness_turn_id=proposal.harness_turn_id,
            source_event_id=proposal.source_event_id,
            expected_turn_state="settled",
        )
        if turn_row[3] != 1 or turn_row[4] is None:
            raise PurposeRecoveryError(
                "settled purpose turn lacks accepted result identity"
            )
        result_event_id = str(turn_row[4])
        result_event = cls._causal_event_uncommitted(conn, result_event_id)
        if (
            result_event["parent_event_id"] != root["id"]
            or result_event["causal_id"] != root["causal_id"]
            or result_event["engram_id"] != proposal.author_engram_id
            or result_event["kind"] != "assistant_result"
            or result_event["domain"] != "harness"
            or result_event["source"] != "self"
            or result_event["status"] != "settled"
        ):
            raise PurposeRecoveryError(
                "purpose proposal is not closed by the turn assistant result"
            )
        cls._assert_tool_call_uncommitted(
            conn,
            tool_call_event_id=proposal.tool_call_event_id,
            tool_call_id=proposal.tool_call_id,
            root=root,
            author_engram_id=proposal.author_engram_id,
        )
        result_rows = conn.execute(
            """SELECT id, parent_event_id, causal_id, engram_id, domain,
                      source, status, metadata
               FROM causal_events
               WHERE parent_event_id = ? AND kind = 'tool_result'""",
            (proposal.tool_call_event_id,),
        ).fetchall()
        if len(result_rows) != 1:
            raise PurposeRecoveryError(
                "purpose proposal requires one canonical tool result"
            )
        tool_result = result_rows[0]
        metadata = cls._json_object(tool_result[7], "purpose tool result metadata")
        result_refs = metadata.get("result_refs")
        if (
            str(tool_result[1]) != proposal.tool_call_event_id
            or str(tool_result[2]) != proposal.source_causal_id
            or str(tool_result[3]) != proposal.author_engram_id
            or str(tool_result[4]) != "harness"
            or str(tool_result[5]) != "self"
            or str(tool_result[6]) != "settled"
            or metadata.get("tool_name") != "pulse_life_amend_purpose"
            or metadata.get("tool_call_id") != proposal.tool_call_id
            or metadata.get("ok") is not True
            or not isinstance(result_refs, dict)
            or result_refs.get("proposal_id") != proposal.proposal_id
        ):
            raise PurposeRecoveryError(
                "purpose tool result does not attest the staged proposal"
            )
        return result_event_id

    def create_lineage(
        self,
        lineage_id: str,
        *,
        world_id: str,
        root_engram_id: str,
        current_engram_id: str | None = None,
    ) -> SubjectLineage:
        """Create one durable lineage identity; never create a duplicate."""

        lineage_id = _identifier(lineage_id, "lineage_id")
        world_id = _identifier(world_id, "world_id")
        root_engram_id = _identifier(root_engram_id, "root_engram_id")
        current_engram_id = _identifier(
            root_engram_id if current_engram_id is None else current_engram_id,
            "current_engram_id",
        )
        now = _now()
        with self._write_transaction() as conn:
            if self._fetch_lineage_row(lineage_id) is not None:
                raise PurposeLineageConflictError("subject lineage already exists")
            conn.execute(
                """INSERT INTO subject_lineages (
                    lineage_id, world_id, root_engram_id, current_engram_id,
                    current_purpose_revision_id, generation, state,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, NULL, 0, 'active', ?, ?)""",
                (
                    lineage_id,
                    world_id,
                    root_engram_id,
                    current_engram_id,
                    now,
                    now,
                ),
            )
            return self._lineage_from_row(self._fetch_lineage_row(lineage_id))

    def get_lineage(self, lineage_id: str) -> SubjectLineage | None:
        lineage_id = _identifier(lineage_id, "lineage_id")
        with self._lock:
            self._ensure_open()
            row = self._fetch_lineage_row(lineage_id)
            if row is None:
                return None
            self._assert_lineage_integrity(lineage_id)
            return self._lineage_from_row(row)

    def require_lineage(self, lineage_id: str) -> SubjectLineage:
        lineage = self.get_lineage(lineage_id)
        if lineage is None:
            raise PurposeLineageNotFoundError("subject lineage does not exist")
        return lineage

    def find_lineage_for_engram(self, engram_id: str) -> SubjectLineage | None:
        """Resolve the one active lineage currently carried by ``engram_id``.

        The lookup is deliberately on ``current_engram_id`` only.  An
        archived predecessor remains part of lineage history, but it must not
        regain subject authority after succession.  Multiple matches are a
        recovery error rather than an arbitrary winner.
        """

        engram_id = _identifier(engram_id, "engram_id")
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """SELECT lineage_id FROM subject_lineages
                   WHERE current_engram_id = ? AND state = 'active'
                   ORDER BY lineage_id LIMIT 2""",
                (engram_id,),
            ).fetchall()
            if not rows:
                return None
            if len(rows) != 1:
                raise PurposeRecoveryError(
                    "one engram is bound to multiple active subject lineages"
                )
            lineage_id = str(rows[0][0])
            self._assert_lineage_integrity(lineage_id)
            row = self._fetch_lineage_row(lineage_id)
            assert row is not None
            return self._lineage_from_row(row)

    def succeed_lineage(
        self,
        lineage_id: str,
        *,
        successor_engram_id: str,
        expected_current_engram_id: str,
        expected_generation: int,
    ) -> SubjectLineage:
        """Advance the same lineage to one successor using a generation CAS."""

        lineage_id = _identifier(lineage_id, "lineage_id")
        successor_engram_id = _identifier(successor_engram_id, "successor_engram_id")
        expected_current_engram_id = _identifier(
            expected_current_engram_id, "expected_current_engram_id"
        )
        if type(expected_generation) is not int or expected_generation < 0:
            raise PurposeValidationError("expected_generation must be an integer >= 0")
        now = _now()
        with self._write_transaction() as conn:
            row = self._fetch_lineage_row(lineage_id)
            if row is None:
                raise PurposeLineageNotFoundError("subject lineage does not exist")
            self._assert_lineage_integrity(lineage_id)
            if str(row[6]) != LineageState.ACTIVE.value:
                raise PurposeLineageConflictError("archived lineage cannot succeed")
            if (
                str(row[3]) == successor_engram_id
                or str(row[3]) != expected_current_engram_id
                or int(row[5]) != expected_generation
            ):
                raise PurposeLineageConflictError("lineage succession CAS conflict")
            changed = conn.execute(
                """UPDATE subject_lineages
                   SET current_engram_id = ?, generation = generation + 1,
                       updated_at = ?
                   WHERE lineage_id = ? AND current_engram_id = ?
                     AND generation = ? AND state = 'active'""",
                (
                    successor_engram_id,
                    now,
                    lineage_id,
                    expected_current_engram_id,
                    expected_generation,
                ),
            )
            if changed.rowcount != 1:
                raise PurposeLineageConflictError("lineage succession CAS conflict")
            return self._lineage_from_row(self._fetch_lineage_row(lineage_id))

    def record_succession(self, *args, **kwargs) -> SubjectLineage:
        """Explicit alias used by succession callers."""

        return self.succeed_lineage(*args, **kwargs)

    def _append_revision_uncommitted(
        self,
        conn: sqlite3.Connection,
        *,
        lineage_id: str,
        purpose_revision_id: str,
        author_engram_id: str,
        expected_revision: int | None,
        canonical_content: str | None,
        amendment_kind: PurposeAmendmentKind,
        source_event_id: str,
        reflection_event_id: str | None,
        content_digest: str,
    ) -> PurposeRevision:
        existing_row = self._fetch_revision_row(purpose_revision_id)
        if existing_row is not None:
            existing = self._revision_from_row(existing_row)
            if (
                existing.lineage_id == lineage_id
                and existing.author_engram_id == author_engram_id
                and existing.amendment_kind is amendment_kind
                and existing.content == canonical_content
                and existing.source_event_id == source_event_id
                and existing.reflection_event_id == reflection_event_id
                and existing.content_digest == content_digest
            ):
                return existing
            raise PurposeRevisionCollisionError(
                "purpose revision id was reused for a different immutable request"
            )

        row = self._fetch_lineage_row(lineage_id)
        if row is None:
            raise PurposeLineageNotFoundError("subject lineage does not exist")
        self._assert_lineage_integrity(lineage_id)
        current_id = None if row[4] is None else str(row[4])
        current_row = (
            None if current_id is None else self._fetch_revision_row(current_id)
        )
        current_revision = None if current_row is None else int(current_row[3])
        if current_id is not None and current_row is None:
            raise PurposeRecoveryError("lineage points to a missing purpose revision")
        if current_row is not None and str(current_row[10]) != "current":
            raise PurposeRecoveryError("lineage points to a non-current purpose revision")
        if expected_revision != current_revision:
            raise PurposeRevisionConflictError(
                expected_revision=expected_revision,
                current_revision=current_revision,
            )
        if str(row[3]) != author_engram_id:
            raise PurposeGovernanceError(
                "purpose author must be the current lineage engram"
            )
        if amendment_kind is PurposeAmendmentKind.WITHDRAW and current_row is None:
            raise PurposeRevisionConflictError(
                expected_revision=expected_revision,
                current_revision=None,
            )
        if amendment_kind is PurposeAmendmentKind.ESTABLISH and current_row is not None:
            raise PurposeRevisionConflictError(
                expected_revision=expected_revision,
                current_revision=current_revision,
            )

        latest_row = conn.execute(
            """SELECT purpose_revision_id, revision
               FROM purpose_revisions WHERE lineage_id = ?
               ORDER BY revision DESC LIMIT 1""",
            (lineage_id,),
        ).fetchone()
        next_revision = 1 if latest_row is None else int(latest_row[1]) + 1
        predecessor_id = (
            current_id
            if current_id is not None
            else (None if latest_row is None else str(latest_row[0]))
        )
        now = _now()
        if current_id is not None:
            changed = conn.execute(
                """UPDATE purpose_revisions
                   SET state = 'superseded', superseded_at = ?
                   WHERE purpose_revision_id = ? AND state = 'current'""",
                (now, current_id),
            )
            if changed.rowcount != 1:
                raise PurposeRevisionConflictError(
                    expected_revision=expected_revision,
                    current_revision=current_revision,
                )

        new_state = (
            PurposeRevisionState.WITHDRAWN.value
            if amendment_kind is PurposeAmendmentKind.WITHDRAW
            else PurposeRevisionState.CURRENT.value
        )
        conn.execute(
            """INSERT INTO purpose_revisions (
                purpose_revision_id, lineage_id, author_engram_id, revision,
                predecessor_revision_id, amendment_kind, content,
                source_event_id, reflection_event_id, content_digest, state,
                created_at, superseded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
            (
                purpose_revision_id,
                lineage_id,
                author_engram_id,
                next_revision,
                predecessor_id,
                amendment_kind.value,
                canonical_content,
                source_event_id,
                reflection_event_id,
                content_digest,
                new_state,
                now,
            ),
        )
        pointer = (
            None
            if amendment_kind is PurposeAmendmentKind.WITHDRAW
            else purpose_revision_id
        )
        changed = conn.execute(
            """UPDATE subject_lineages
               SET current_purpose_revision_id = ?, updated_at = ?
               WHERE lineage_id = ? AND current_purpose_revision_id IS ?""",
            (pointer, now, lineage_id, current_id),
        )
        if changed.rowcount != 1:
            raise PurposeRevisionConflictError(
                expected_revision=expected_revision,
                current_revision=current_revision,
            )
        row = self._fetch_revision_row(purpose_revision_id)
        assert row is not None
        return self._revision_from_row(row)

    def append_revision(
        self,
        lineage_id: str,
        *,
        purpose_revision_id: str,
        author_engram_id: str,
        expected_revision: int | None,
        content: str | None,
        amendment_kind: PurposeAmendmentKind | str,
        source_event_id: str,
        reflection_event_id: str | None = None,
    ) -> PurposeRevision:
        """Append one revision and atomically move the lineage pointer.

        ``purpose_revision_id`` is the caller's stable idempotency key.  A
        retry with the same immutable request replays the stored row; a retry
        with changed content or provenance fails closed.  The method does not
        accept arbitrary metadata, prompts, or secrets.
        """

        lineage_id = _identifier(lineage_id, "lineage_id")
        purpose_revision_id = _identifier(
            purpose_revision_id, "purpose_revision_id"
        )
        author_engram_id = _identifier(author_engram_id, "author_engram_id")
        source_event_id = _identifier(source_event_id, "source_event_id")
        reflection_event_id = _optional_identifier(
            reflection_event_id, "reflection_event_id"
        )
        expected_revision = _optional_revision(expected_revision)
        try:
            amendment_kind = (
                amendment_kind
                if isinstance(amendment_kind, PurposeAmendmentKind)
                else PurposeAmendmentKind(amendment_kind)
            )
        except (TypeError, ValueError) as exc:
            raise PurposeValidationError("unsupported purpose amendment kind") from exc
        canonical_content = _content(
            content, allow_withdraw=amendment_kind is PurposeAmendmentKind.WITHDRAW
        )
        if amendment_kind is PurposeAmendmentKind.WITHDRAW and content is not None:
            raise PurposeValidationError("withdraw revision must not carry content")
        content_digest = _digest(canonical_content)

        with self._write_transaction() as conn:
            return self._append_revision_uncommitted(
                conn,
                lineage_id=lineage_id,
                purpose_revision_id=purpose_revision_id,
                author_engram_id=author_engram_id,
                expected_revision=expected_revision,
                canonical_content=canonical_content,
                amendment_kind=amendment_kind,
                source_event_id=source_event_id,
                reflection_event_id=reflection_event_id,
                content_digest=content_digest,
            )

    def amend_purpose(self, *args, **kwargs) -> PurposeRevision:
        """Subject-facing alias for :meth:`append_revision`."""

        return self.append_revision(*args, **kwargs)

    def stage_amendment(
        self,
        lineage_id: str,
        *,
        proposal_id: str,
        author_engram_id: str,
        harness_turn_id: str,
        tool_call_event_id: str,
        tool_call_id: str,
        expected_revision: int | None,
        content: str | None,
        amendment_kind: PurposeAmendmentKind | str,
        source_event_id: str,
    ) -> PurposeAmendmentProposal:
        """Stage one immutable proposal without changing the current purpose.

        The durable Harness turn and causal tool call, not model arguments,
        establish subject identity and provenance.  Final adoption is owned by
        :meth:`resolve_turn_proposal` after that same turn reaches a terminal
        state.
        """

        lineage_id = _identifier(lineage_id, "lineage_id")
        proposal_id = _identifier(proposal_id, "proposal_id")
        author_engram_id = _identifier(author_engram_id, "author_engram_id")
        harness_turn_id = _identifier(harness_turn_id, "harness_turn_id")
        tool_call_event_id = _identifier(
            tool_call_event_id,
            "tool_call_event_id",
        )
        tool_call_id = _identifier(tool_call_id, "tool_call_id")
        source_event_id = _identifier(source_event_id, "source_event_id")
        expected_revision = _optional_revision(expected_revision)
        try:
            kind = (
                amendment_kind
                if isinstance(amendment_kind, PurposeAmendmentKind)
                else PurposeAmendmentKind(amendment_kind)
            )
        except (TypeError, ValueError) as exc:
            raise PurposeValidationError("unsupported purpose amendment kind") from exc
        canonical_content = _content(
            content,
            allow_withdraw=kind is PurposeAmendmentKind.WITHDRAW,
        )
        if kind is PurposeAmendmentKind.WITHDRAW and content is not None:
            raise PurposeValidationError("withdraw proposal must not carry content")
        content_digest = _digest(canonical_content)

        with self._write_transaction() as conn:
            existing_row = self._fetch_proposal_row(proposal_id)
            if existing_row is not None:
                existing = self._proposal_from_row(existing_row)
                if (
                    existing.lineage_id == lineage_id
                    and existing.author_engram_id == author_engram_id
                    and existing.harness_turn_id == harness_turn_id
                    and existing.tool_call_event_id == tool_call_event_id
                    and existing.tool_call_id == tool_call_id
                    and existing.expected_revision == expected_revision
                    and existing.amendment_kind is kind
                    and existing.content == canonical_content
                    and existing.content_digest == content_digest
                    and existing.source_event_id == source_event_id
                ):
                    return existing
                raise PurposeProposalConflictError(
                    "purpose proposal id was reused for another immutable request"
                )
            turn_collision = conn.execute(
                """SELECT proposal_id FROM purpose_amendment_proposals
                   WHERE harness_turn_id = ?""",
                (harness_turn_id,),
            ).fetchone()
            if turn_collision is not None:
                raise PurposeProposalConflictError(
                    "one Harness turn may stage only one purpose proposal"
                )

            lineage_row = self._fetch_lineage_row(lineage_id)
            if lineage_row is None:
                raise PurposeLineageNotFoundError("subject lineage does not exist")
            self._assert_lineage_integrity(lineage_id)
            if str(lineage_row[3]) != author_engram_id:
                raise PurposeReflectionRequiredError(
                    "purpose author is not the current lineage holder"
                )
            current_id = None if lineage_row[4] is None else str(lineage_row[4])
            current_row = (
                None if current_id is None else self._fetch_revision_row(current_id)
            )
            current_revision = (
                None if current_row is None else int(current_row[3])
            )
            if expected_revision != current_revision:
                raise PurposeRevisionConflictError(
                    expected_revision=expected_revision,
                    current_revision=current_revision,
                )
            if kind is PurposeAmendmentKind.WITHDRAW and current_row is None:
                raise PurposeRevisionConflictError(
                    expected_revision=expected_revision,
                    current_revision=None,
                )
            if kind is PurposeAmendmentKind.ESTABLISH and current_row is not None:
                raise PurposeRevisionConflictError(
                    expected_revision=expected_revision,
                    current_revision=current_revision,
                )

            root, _turn, provenance_digest = self._eligible_reflection_context_uncommitted(
                conn,
                lineage_row=lineage_row,
                author_engram_id=author_engram_id,
                harness_turn_id=harness_turn_id,
                source_event_id=source_event_id,
                expected_turn_state="running",
            )
            self._assert_tool_call_uncommitted(
                conn,
                tool_call_event_id=tool_call_event_id,
                tool_call_id=tool_call_id,
                root=root,
                author_engram_id=author_engram_id,
            )
            now = _now()
            conn.execute(
                """INSERT INTO purpose_amendment_proposals (
                    proposal_id, lineage_id, author_engram_id,
                    harness_turn_id, tool_call_event_id, tool_call_id,
                    expected_revision, amendment_kind, content, content_digest,
                    source_event_id, source_causal_id, source_kind,
                    source_domain, source_flow, source_center_id,
                    source_provenance_digest, state, committed_revision_id,
                    result_event_id, resolution_code, created_at, resolved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                          'pending', NULL, NULL, NULL, ?, NULL)""",
                (
                    proposal_id,
                    lineage_id,
                    author_engram_id,
                    harness_turn_id,
                    tool_call_event_id,
                    tool_call_id,
                    expected_revision,
                    kind.value,
                    canonical_content,
                    content_digest,
                    source_event_id,
                    root["causal_id"],
                    root["source"],
                    root["domain"],
                    root["flow"],
                    root["center_id"],
                    provenance_digest,
                    now,
                ),
            )
            row = self._fetch_proposal_row(proposal_id)
            assert row is not None
            return self._proposal_from_row(row)

    def get_proposal(self, proposal_id: str) -> PurposeAmendmentProposal | None:
        proposal_id = _identifier(proposal_id, "proposal_id")
        with self._lock:
            self._ensure_open()
            row = self._fetch_proposal_row(proposal_id)
            return None if row is None else self._proposal_from_row(row)

    def list_proposals(
        self,
        lineage_id: str,
        *,
        state: PurposeAmendmentProposalState | str | None = None,
        limit: int = MAX_HISTORY_LIMIT,
    ) -> list[PurposeAmendmentProposal]:
        lineage_id = _identifier(lineage_id, "lineage_id")
        if type(limit) is not int or not 1 <= limit <= MAX_HISTORY_LIMIT:
            raise PurposeValidationError(
                f"limit must be an integer in [1, {MAX_HISTORY_LIMIT}]"
            )
        state_value = None
        if state is not None:
            try:
                state_value = (
                    state.value
                    if isinstance(state, PurposeAmendmentProposalState)
                    else PurposeAmendmentProposalState(state).value
                )
            except (TypeError, ValueError) as exc:
                raise PurposeValidationError("unsupported proposal state") from exc
        with self._lock:
            self._ensure_open()
            if self._fetch_lineage_row(lineage_id) is None:
                raise PurposeLineageNotFoundError("subject lineage does not exist")
            clauses = ["lineage_id = ?"]
            params: list[Any] = [lineage_id]
            if state_value is not None:
                clauses.append("state = ?")
                params.append(state_value)
            params.append(limit)
            rows = self._connection.execute(
                """SELECT proposal_id, lineage_id, author_engram_id,
                          harness_turn_id, tool_call_event_id, tool_call_id,
                          expected_revision, amendment_kind, content,
                          content_digest, source_event_id, source_causal_id,
                          source_kind, source_domain, source_flow,
                          source_center_id, source_provenance_digest, state,
                          committed_revision_id, result_event_id,
                          resolution_code, created_at, resolved_at
                   FROM purpose_amendment_proposals WHERE """
                + " AND ".join(clauses)
                + " ORDER BY created_at DESC, proposal_id DESC LIMIT ?",
                params,
            ).fetchall()
            return [self._proposal_from_row(row) for row in rows]

    def _terminalize_proposal_uncommitted(
        self,
        conn: sqlite3.Connection,
        proposal: PurposeAmendmentProposal,
        *,
        state: PurposeAmendmentProposalState,
        resolution_code: str,
        result_event_id: str | None = None,
    ) -> PurposeAmendmentProposal:
        if state not in {
            PurposeAmendmentProposalState.REJECTED,
            PurposeAmendmentProposalState.UNCERTAIN,
            PurposeAmendmentProposalState.CONFLICTED,
        }:
            raise PurposeValidationError("invalid non-commit proposal terminal state")
        resolution_code = _identifier(resolution_code, "resolution_code")
        changed = conn.execute(
            """UPDATE purpose_amendment_proposals
               SET state = ?, result_event_id = ?, resolution_code = ?,
                   resolved_at = ?
               WHERE proposal_id = ? AND state = 'pending'""",
            (
                state.value,
                result_event_id,
                resolution_code,
                _now(),
                proposal.proposal_id,
            ),
        )
        if changed.rowcount != 1:
            row = self._fetch_proposal_row(proposal.proposal_id)
            if row is None:
                raise PurposeRecoveryError("purpose proposal disappeared")
            return self._proposal_from_row(row)
        row = self._fetch_proposal_row(proposal.proposal_id)
        assert row is not None
        return self._proposal_from_row(row)

    def resolve_turn_proposal(
        self,
        harness_turn_id: str,
    ) -> PurposeAmendmentProposal | None:
        """Resolve the proposal for one terminal Harness turn, idempotently."""

        harness_turn_id = _identifier(harness_turn_id, "harness_turn_id")
        with self._write_transaction() as conn:
            row = conn.execute(
                """SELECT proposal_id FROM purpose_amendment_proposals
                   WHERE harness_turn_id = ?""",
                (harness_turn_id,),
            ).fetchone()
            if row is None:
                return None
            proposal_row = self._fetch_proposal_row(str(row[0]))
            assert proposal_row is not None
            proposal = self._proposal_from_row(proposal_row)
            if proposal.state is not PurposeAmendmentProposalState.PENDING:
                return proposal

            turn = conn.execute(
                """SELECT state, result_event_id FROM harness_turns
                   WHERE id = ?""",
                (harness_turn_id,),
            ).fetchone()
            if turn is None:
                raise PurposeRecoveryError("purpose proposal turn is missing")
            turn_state = str(turn[0])
            if turn_state == "running":
                return proposal
            if turn_state == "failed":
                return self._terminalize_proposal_uncommitted(
                    conn,
                    proposal,
                    state=PurposeAmendmentProposalState.REJECTED,
                    resolution_code="harness_turn_failed",
                )
            if turn_state == "uncertain":
                return self._terminalize_proposal_uncommitted(
                    conn,
                    proposal,
                    state=PurposeAmendmentProposalState.UNCERTAIN,
                    resolution_code="harness_turn_uncertain",
                )
            if turn_state != "settled":
                raise PurposeRecoveryError("purpose proposal turn has invalid state")

            lineage_row = self._fetch_lineage_row(proposal.lineage_id)
            if lineage_row is None:
                raise PurposeRecoveryError("purpose proposal lineage is missing")
            result_event_id = self._settled_reflection_result_uncommitted(
                conn,
                proposal,
                lineage_row,
            )
            if str(lineage_row[3]) != proposal.author_engram_id:
                return self._terminalize_proposal_uncommitted(
                    conn,
                    proposal,
                    state=PurposeAmendmentProposalState.CONFLICTED,
                    resolution_code="lineage_holder_changed",
                    result_event_id=result_event_id,
                )
            try:
                revision = self._append_revision_uncommitted(
                    conn,
                    lineage_id=proposal.lineage_id,
                    purpose_revision_id=proposal.proposal_id,
                    author_engram_id=proposal.author_engram_id,
                    expected_revision=proposal.expected_revision,
                    canonical_content=proposal.content,
                    amendment_kind=proposal.amendment_kind,
                    source_event_id=proposal.source_event_id,
                    reflection_event_id=result_event_id,
                    content_digest=proposal.content_digest,
                )
            except PurposeRevisionConflictError:
                return self._terminalize_proposal_uncommitted(
                    conn,
                    proposal,
                    state=PurposeAmendmentProposalState.CONFLICTED,
                    resolution_code="purpose_revision_conflict",
                    result_event_id=result_event_id,
                )
            changed = conn.execute(
                """UPDATE purpose_amendment_proposals
                   SET state = 'committed', committed_revision_id = ?,
                       result_event_id = ?, resolution_code = 'turn_settled',
                       resolved_at = ?
                   WHERE proposal_id = ? AND state = 'pending'""",
                (
                    revision.purpose_revision_id,
                    result_event_id,
                    _now(),
                    proposal.proposal_id,
                ),
            )
            if changed.rowcount != 1:
                raise PurposeRecoveryError(
                    "purpose proposal changed during atomic commit"
                )
            resolved_row = self._fetch_proposal_row(proposal.proposal_id)
            assert resolved_row is not None
            return self._proposal_from_row(resolved_row)

    def reconcile_pending_amendments(self) -> dict[str, int]:
        """Classify or commit every staged proposal from durable turn state."""

        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """SELECT harness_turn_id FROM purpose_amendment_proposals
                   WHERE state = 'pending'
                   ORDER BY created_at, proposal_id"""
            ).fetchall()
        counts = {state.value: 0 for state in PurposeAmendmentProposalState}
        for (turn_id,) in rows:
            proposal = self.resolve_turn_proposal(str(turn_id))
            if proposal is not None:
                counts[proposal.state.value] += 1
        with self._lock:
            self._assert_all_integrity()
        return counts

    def get_revision(self, purpose_revision_id: str) -> PurposeRevision | None:
        purpose_revision_id = _identifier(
            purpose_revision_id, "purpose_revision_id"
        )
        with self._lock:
            self._ensure_open()
            row = self._fetch_revision_row(purpose_revision_id)
            return None if row is None else self._revision_from_row(row)

    def require_revision(self, purpose_revision_id: str) -> PurposeRevision:
        revision = self.get_revision(purpose_revision_id)
        if revision is None:
            raise PurposeRevisionNotFoundError("purpose revision does not exist")
        return revision

    def current_revision(self, lineage_id: str) -> PurposeRevision | None:
        lineage = self.require_lineage(lineage_id)
        if lineage.current_purpose_revision_id is None:
            return None
        return self.require_revision(lineage.current_purpose_revision_id)

    def list_revisions(self, lineage_id: str, *, limit: int = MAX_HISTORY_LIMIT) -> list[PurposeRevision]:
        lineage_id = _identifier(lineage_id, "lineage_id")
        if type(limit) is not int or not 1 <= limit <= MAX_HISTORY_LIMIT:
            raise PurposeValidationError(
                f"limit must be an integer in [1, {MAX_HISTORY_LIMIT}]"
            )
        with self._lock:
            self._ensure_open()
            if self._fetch_lineage_row(lineage_id) is None:
                raise PurposeLineageNotFoundError("subject lineage does not exist")
            self._assert_lineage_integrity(lineage_id)
            rows = self._connection.execute(
                """SELECT purpose_revision_id, lineage_id, author_engram_id,
                          revision, predecessor_revision_id, amendment_kind,
                          content, source_event_id, reflection_event_id,
                          content_digest, state, created_at, superseded_at
                   FROM purpose_revisions WHERE lineage_id = ?
                   ORDER BY revision LIMIT ?""",
                (lineage_id, limit),
            ).fetchall()
            return [self._revision_from_row(row) for row in rows]

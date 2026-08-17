"""Durable task offers and subject-authored consent.

This service is the sole owner of the ``task-offer-consent.v1`` state
machine.  It composes the existing Storage records with CausalLedger's
transaction and uncommitted event primitives so an offer, its deliberation
root, a decision, and an accepted task bundle never become partially visible.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone

from pulse_system.core.causality import CausalLedger
from pulse_system.core.types import (
    ActivityCenter,
    ActivityKind,
    ActivityOrigin,
    CausalEvent,
    CausalEventDomain,
    CausalEventFlow,
    CausalEventKind,
    CausalEventSource,
    CausalEventStatus,
    CenterMembership,
    Engram,
    EngramStatus,
    MembershipRelation,
    TaskFront,
    TaskFrontStatus,
    TaskOffer,
    TaskOfferDecision,
    TaskOfferRevision,
    TaskOfferSnapshot,
    TaskOfferStatus,
    TaskRelationship,
    TaskRelationshipAction,
    TaskRelationshipActorKind,
    TaskRelationshipEvent,
    TaskRelationshipStatus,
    session_name,
)
from pulse_system.core.world import WorldRegistry
from pulse_system.substrate.storage import Storage


_PROTOCOL = "task-offer-consent.v1"
_RESPONSE_TOOL = "pulse_task_offer_respond"
_OFFER_PREAMBLE = (
    "A user has offered you a possible task. This is not yet an active task.\n"
    "Consider the offer against your purpose, ongoing life, commitments, and "
    "capacity. Do not begin the task, change the workspace, delegate it, or "
    "mutate life governance during this deliberation.\n"
    "Before doing any task work, call pulse_task_offer_respond exactly once "
    "with accept, refuse, or request_changes. A request for changes must "
    "include the changes you need.\n\n"
    "Task offer terms:\n"
)


def task_relationship_id_for_offer(offer_id: str) -> str:
    """Return the stable one-to-one identity used by acceptance and recovery."""

    return "taskrel_" + hashlib.sha256(
        f"task-relationship:{offer_id}".encode("utf-8")
    ).hexdigest()[:32]


def task_offer_response_digest(response: str | None) -> str:
    """Return the canonical Pi tool binding for one subject response."""

    binding = "none" if response is None else "text:" + response
    return hashlib.sha256(binding.encode("utf-8")).hexdigest()


def validate_task_offer_decision_origin_uncommitted(
    ledger: CausalLedger,
    conn,
    *,
    world_id: str,
    offer: TaskOffer,
    revision: TaskOfferRevision,
) -> None:
    """Re-prove an accepted revision's root and subject tool-call identity."""

    decision_event_id = revision.decision_event_id
    if (
        offer.world_id != world_id
        or offer.current_revision != revision.revision
        or offer.status is not TaskOfferStatus.ACCEPTED
        or revision.offer_id != offer.id
        or revision.decision is not TaskOfferDecision.ACCEPT
        or decision_event_id is None
    ):
        raise ValueError("accepted task offer head is inconsistent")
    root = ledger._get_event_uncommitted(conn, revision.latest_offer_event_id)
    decision_event = ledger._get_event_uncommitted(conn, decision_event_id)
    if root is None or decision_event is None:
        raise ValueError("accepted task offer origin event is missing")
    if not (
        root.id == revision.latest_offer_event_id
        and root.world_id == world_id
        and root.engram_id == offer.subject_engram_id
        and root.center_id is None
        and root.parent_event_id is None
        and root.flow is CausalEventFlow.CONTENT
        and root.domain is CausalEventDomain.WORLD
        and root.kind is CausalEventKind.STIMULUS
        and root.source is CausalEventSource.USER
        and root.status
        in {
            CausalEventStatus.RUNNING,
            CausalEventStatus.SETTLED,
            CausalEventStatus.UNCERTAIN,
            CausalEventStatus.RECONCILED,
        }
        and root.metadata.get("task_offer_id") == offer.id
        and root.metadata.get("task_offer_revision") == revision.revision
        and root.content == _OFFER_PREAMBLE + revision.content
        and decision_event.id == decision_event_id
        and decision_event.parent_event_id == root.id
        and decision_event.world_id == world_id
        and decision_event.engram_id == offer.subject_engram_id
        and decision_event.causal_id == root.causal_id
        and decision_event.kind is CausalEventKind.TOOL_CALL
        and decision_event.domain is CausalEventDomain.HARNESS
        and decision_event.source is CausalEventSource.SELF
        and decision_event.status is CausalEventStatus.SETTLED
        and decision_event.metadata.get("tool_name") == _RESPONSE_TOOL
        and decision_event.metadata.get("task_offer_decision")
        == TaskOfferDecision.ACCEPT.value
        and decision_event.metadata.get("task_offer_expected_revision")
        == revision.revision
        and decision_event.metadata.get("task_offer_response_digest")
        == task_offer_response_digest(revision.subject_response)
    ):
        raise ValueError("accepted task offer origin is not causally bound")


class TaskOfferError(ValueError):
    """Stable service error consumed by Runtime and Pi tool adapters."""

    def __init__(
        self,
        code: str,
        detail: str,
        remedy: str,
        *,
        status: int = 409,
    ) -> None:
        self.code = code
        self.detail = detail
        self.remedy = remedy
        self.status = status
        super().__init__(f"{code}: {detail}")


@dataclass(frozen=True)
class TaskOfferDecisionReceipt:
    """Stable response view for one decided revision, even after a revise."""

    id: str
    current_revision: int
    status: TaskOfferStatus
    task_front_id: str | None = None


@dataclass(frozen=True)
class TaskOfferOperation(Mapping[str, object]):
    """One committed service operation and its directly affected event."""

    snapshot: TaskOfferSnapshot
    event_id: str | None = None
    task_front_id: str | None = None
    decision: TaskOfferDecision | None = None
    decision_revision: int | None = None
    duplicate: bool = False

    def _mapping(self) -> dict[str, object]:
        result: dict[str, object]
        if self.decision is None:
            result = {
                "snapshot": self.snapshot,
                "task_offer": self.snapshot.offer,
                "decision": None,
                "duplicate": self.duplicate,
            }
        else:
            if self.decision_revision is None:
                raise ValueError("decision operation requires decision_revision")
            status = {
                TaskOfferDecision.ACCEPT: TaskOfferStatus.ACCEPTED,
                TaskOfferDecision.REFUSE: TaskOfferStatus.REFUSED,
                TaskOfferDecision.REQUEST_CHANGES: (
                    TaskOfferStatus.CHANGES_REQUESTED
                ),
            }[self.decision]
            result = {
                "task_offer": TaskOfferDecisionReceipt(
                    id=self.snapshot.offer.id,
                    current_revision=self.decision_revision,
                    status=status,
                    task_front_id=(
                        self.task_front_id
                        if self.decision is TaskOfferDecision.ACCEPT
                        else None
                    ),
                ),
                "decision": self.decision,
                "duplicate": self.duplicate,
            }
        if self.decision is TaskOfferDecision.ACCEPT:
            if self.task_front_id is not None:
                result["task_front_id"] = self.task_front_id
            if self.event_id is not None:
                result["task_event_id"] = self.event_id
        elif self.decision is None and self.event_id is not None:
            # create/revise/remind expose their offer-root event without
            # weakening the response seam's "no task IDs unless accepted".
            result["event_id"] = self.event_id
        return result

    def __getitem__(self, key: str) -> object:
        return self._mapping()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._mapping())

    def __len__(self) -> int:
        return len(self._mapping())


class TaskOfferService:
    """Own the durable TaskOffer aggregate and all of its transitions."""

    def __init__(
        self,
        storage: Storage,
        ledger: CausalLedger,
        world: WorldRegistry,
        *,
        world_id: str,
    ) -> None:
        if ledger.storage is not storage:
            raise ValueError("TaskOfferService requires one shared Storage")
        if getattr(world, "_storage", None) is not storage:
            raise ValueError("TaskOfferService requires one shared WorldRegistry")
        self._storage = storage
        self._ledger = ledger
        self._world = world
        self._world_id = self._require_id(
            world_id,
            "world_id",
            code="invalid_task_offer",
        )

    @property
    def world_id(self) -> str:
        return self._world_id

    def create(
        self,
        subject_engram_id: str,
        content: str,
        title: str | None = None,
        project_id: str | None = None,
        stimulus_provenance_digest: str | None = None,
    ) -> TaskOfferOperation:
        """Atomically persist revision one and its queued deliberation root."""

        subject_engram_id = self._require_id(
            subject_engram_id,
            "subject_engram_id",
            code="invalid_task_offer",
        )
        content = self._normalize_content(content)
        title = self._normalize_title(title, content)
        project_id = self._normalize_optional_id(project_id, "project_id")
        provenance = self._normalize_provenance(stimulus_provenance_digest)
        offer_id = uuid.uuid4().hex
        event_id = self._stable_id(offer_id, "revision:1:offer-root")
        now = self._now()
        offer = TaskOffer(
            id=offer_id,
            world_id=self._world_id,
            subject_engram_id=subject_engram_id,
            status=TaskOfferStatus.PENDING,
            current_revision=1,
            created_at=now,
            updated_at=now,
        )
        revision = TaskOfferRevision(
            offer_id=offer_id,
            revision=1,
            content=content,
            title=title,
            project_id=project_id,
            latest_offer_event_id=event_id,
            created_at=now,
        )
        event = self._offer_event(
            revision,
            subject_engram_id=subject_engram_id,
            event_id=event_id,
            reason_code="task_offer_created",
            provenance_digest=provenance,
            created_at=now,
        )

        with self._ledger._transaction() as conn:
            self._require_active_subject_uncommitted(conn, subject_engram_id)
            self._require_project_uncommitted(conn, project_id)
            self._ledger._ensure_references(conn, subject_engram_id, None)
            self._storage._insert_task_offer(offer)
            self._storage._insert_task_offer_revision(revision)
            self._ledger._insert_event_uncommitted(conn, event)
            snapshot = self._snapshot_uncommitted(conn, offer_id)
        return TaskOfferOperation(snapshot=snapshot, event_id=event.id)

    def list(
        self,
        subject_engram_id: str | None = None,
        status: TaskOfferStatus | str | None = None,
        limit: int = 100,
    ) -> list[TaskOfferSnapshot]:
        """Return a bounded, consistent list ordered by recent mutation."""

        if subject_engram_id is not None:
            subject_engram_id = self._require_id(
                subject_engram_id,
                "subject_engram_id",
                code="invalid_task_offer",
            )
        normalized_status = None
        if status is not None:
            try:
                normalized_status = (
                    status
                    if isinstance(status, TaskOfferStatus)
                    else TaskOfferStatus(status)
                )
            except (TypeError, ValueError) as exc:
                raise self._error(
                    "invalid_task_offer",
                    "status is not a TaskOffer status",
                    "use pending, changes_requested, accepted, refused, or withdrawn",
                    status=400,
                ) from exc
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
            raise self._error(
                "invalid_task_offer",
                "limit must be an integer between 1 and 500",
                "send a bounded positive list limit",
                status=400,
            )
        with self._storage._lock:
            offers = self._storage.list_task_offers(
                world_id=self._world_id,
                subject_engram_id=subject_engram_id,
                include_committed_predecessors=subject_engram_id is not None,
                status=normalized_status,
                limit=limit,
            )
            return [
                self._snapshot_uncommitted(self._storage._conn, offer.id)
                for offer in offers
            ]

    def get(self, offer_id: str) -> TaskOfferSnapshot:
        """Return one aggregate or a stable unknown-offer error."""

        offer_id = self._require_id(
            offer_id,
            "offer_id",
            code="invalid_task_offer",
        )
        with self._storage._lock:
            offer = self._storage._get_task_offer_uncommitted(
                self._storage._conn,
                offer_id,
                world_id=self._world_id,
            )
            if offer is None:
                raise self._unknown_offer(offer_id)
            return self._snapshot_uncommitted(self._storage._conn, offer_id)

    def revise(
        self,
        offer_id: str,
        expected_revision: int,
        content: str,
        title: str | None = None,
        project_id: str | None = None,
        stimulus_provenance_digest: str | None = None,
    ) -> TaskOfferOperation:
        """Replace requested terms with a new immutable revision and root."""

        offer_id = self._require_id(
            offer_id,
            "offer_id",
            code="invalid_task_offer",
        )
        expected_revision = self._normalize_revision(expected_revision)
        content = self._normalize_content(content)
        title = self._normalize_title(title, content)
        project_id = self._normalize_optional_id(project_id, "project_id")
        provenance = self._normalize_provenance(stimulus_provenance_digest)

        with self._ledger._transaction() as conn:
            offer = self._require_offer_uncommitted(conn, offer_id)
            self._assert_revision(offer, expected_revision)
            if offer.status is not TaskOfferStatus.CHANGES_REQUESTED:
                raise self._error(
                    "task_offer_not_changes_requested",
                    f"TaskOffer {offer_id} is {offer.status.value}",
                    "revise only after the subject requests changes",
                )
            self._require_active_subject_uncommitted(
                conn,
                offer.subject_engram_id,
            )
            self._require_project_uncommitted(conn, project_id)
            next_revision = expected_revision + 1
            event_id = self._stable_id(
                offer.id,
                f"revision:{next_revision}:offer-root",
            )
            now = self._now()
            revision = TaskOfferRevision(
                offer_id=offer.id,
                revision=next_revision,
                content=content,
                title=title,
                project_id=project_id,
                latest_offer_event_id=event_id,
                created_at=now,
            )
            event = self._offer_event(
                revision,
                subject_engram_id=offer.subject_engram_id,
                event_id=event_id,
                reason_code="task_offer_revised",
                provenance_digest=provenance,
                created_at=now,
            )
            self._ledger._ensure_references(
                conn,
                offer.subject_engram_id,
                None,
            )
            self._storage._insert_task_offer_revision(revision)
            self._ledger._insert_event_uncommitted(conn, event)
            updated = conn.execute(
                "UPDATE task_offers SET status = 'pending', "
                "current_revision = ?, updated_at = ? "
                "WHERE id = ? AND world_id = ? AND current_revision = ? "
                "AND status = 'changes_requested'",
                (
                    next_revision,
                    self._ts(now),
                    offer.id,
                    self._world_id,
                    expected_revision,
                ),
            )
            if updated.rowcount != 1:
                raise self._revision_conflict(offer.id, expected_revision)
            snapshot = self._snapshot_uncommitted(conn, offer.id)
        return TaskOfferOperation(snapshot=snapshot, event_id=event.id)

    def remind(
        self,
        offer_id: str,
        expected_revision: int,
        stimulus_provenance_digest: str | None = None,
    ) -> TaskOfferOperation:
        """Redeliver a settled undecided revision without changing its terms."""

        offer_id = self._require_id(
            offer_id,
            "offer_id",
            code="invalid_task_offer",
        )
        expected_revision = self._normalize_revision(expected_revision)
        provenance = self._normalize_provenance(stimulus_provenance_digest)
        with self._ledger._transaction() as conn:
            offer = self._require_offer_uncommitted(conn, offer_id)
            self._assert_revision(offer, expected_revision)
            if offer.status is not TaskOfferStatus.PENDING:
                raise self._error(
                    "task_offer_not_pending",
                    f"TaskOffer {offer.id} is {offer.status.value}",
                    "remind only an undecided pending offer",
                )
            revision = self._require_revision_uncommitted(
                conn,
                offer.id,
                expected_revision,
            )
            latest = self._ledger._get_event_uncommitted(
                conn,
                revision.latest_offer_event_id,
            )
            if latest is None:
                raise self._inconsistent(
                    offer.id,
                    "latest deliberation event is missing",
                )
            self._assert_offer_root_identity(offer, revision, latest)
            if latest.status in {
                CausalEventStatus.QUEUED,
                CausalEventStatus.RUNNING,
            }:
                if latest.engram_id == offer.subject_engram_id:
                    snapshot = self._snapshot_uncommitted(conn, offer.id)
                    return TaskOfferOperation(
                        snapshot=snapshot,
                        event_id=latest.id,
                        duplicate=True,
                    )
                if latest.status is CausalEventStatus.RUNNING:
                    raise self._error(
                        "task_offer_decision_in_progress",
                        f"TaskOffer {offer.id} is still running on its predecessor",
                        "let recovery settle the predecessor turn, then remind the successor",
                    )
                cancelled = conn.execute(
                    "UPDATE causal_events SET status = 'cancelled', "
                    "updated_at = ?, settled_at = ? "
                    "WHERE id = ? AND status = 'queued'",
                    (self._ts(self._now()), self._ts(self._now()), latest.id),
                )
                if cancelled.rowcount != 1:
                    raise self._error(
                        "task_offer_decision_in_progress",
                        f"TaskOffer {offer.id} changed during succession recovery",
                        "reload the offer after its predecessor turn settles",
                    )
            self._require_active_subject_uncommitted(
                conn,
                offer.subject_engram_id,
            )
            now = self._now()
            event_id = uuid.uuid4().hex
            event = self._offer_event(
                revision,
                subject_engram_id=offer.subject_engram_id,
                event_id=event_id,
                reason_code="task_offer_reminded",
                provenance_digest=provenance,
                created_at=now,
            )
            self._ledger._ensure_references(
                conn,
                offer.subject_engram_id,
                None,
            )
            self._ledger._insert_event_uncommitted(conn, event)
            updated_revision = conn.execute(
                "UPDATE task_offer_revisions SET latest_offer_event_id = ? "
                "WHERE offer_id = ? AND revision = ? AND decision IS NULL "
                "AND latest_offer_event_id = ?",
                (
                    event.id,
                    offer.id,
                    expected_revision,
                    latest.id,
                ),
            )
            if updated_revision.rowcount != 1:
                raise self._revision_conflict(offer.id, expected_revision)
            updated_offer = conn.execute(
                "UPDATE task_offers SET updated_at = ? WHERE id = ? "
                "AND world_id = ? AND current_revision = ? "
                "AND status = 'pending'",
                (
                    self._ts(now),
                    offer.id,
                    self._world_id,
                    expected_revision,
                ),
            )
            if updated_offer.rowcount != 1:
                raise self._revision_conflict(offer.id, expected_revision)
            snapshot = self._snapshot_uncommitted(conn, offer.id)
        return TaskOfferOperation(snapshot=snapshot, event_id=event.id)

    def withdraw(
        self,
        offer_id: str,
        expected_revision: int,
    ) -> TaskOfferOperation:
        """Withdraw an unresolved offer, cancelling a queued current root."""

        offer_id = self._require_id(
            offer_id,
            "offer_id",
            code="invalid_task_offer",
        )
        expected_revision = self._normalize_revision(expected_revision)
        with self._ledger._transaction() as conn:
            offer = self._require_offer_uncommitted(conn, offer_id)
            self._assert_revision(offer, expected_revision)
            if offer.status not in {
                TaskOfferStatus.PENDING,
                TaskOfferStatus.CHANGES_REQUESTED,
            }:
                raise self._error(
                    "task_offer_not_withdrawable",
                    f"TaskOffer {offer.id} is {offer.status.value}",
                    "withdraw only a pending or changes-requested offer",
                )
            revision = self._require_revision_uncommitted(
                conn,
                offer.id,
                expected_revision,
            )
            latest = self._ledger._get_event_uncommitted(
                conn,
                revision.latest_offer_event_id,
            )
            if latest is None:
                raise self._inconsistent(
                    offer.id,
                    "latest deliberation event is missing",
                )
            self._assert_offer_root_identity(offer, revision, latest)
            if latest.status is CausalEventStatus.RUNNING:
                raise self._error(
                    "task_offer_decision_in_progress",
                    f"TaskOffer {offer.id} is being considered",
                    "wait for the current deliberation turn to settle and retry",
                )
            now = self._now()
            cancelled_event_id = None
            if latest.status is CausalEventStatus.QUEUED:
                cancelled = conn.execute(
                    "UPDATE causal_events SET status = 'cancelled', "
                    "updated_at = ?, settled_at = ? "
                    "WHERE id = ? AND status = 'queued'",
                    (self._ts(now), self._ts(now), latest.id),
                )
                if cancelled.rowcount != 1:
                    raise self._error(
                        "task_offer_decision_in_progress",
                        f"TaskOffer {offer.id} changed before withdrawal",
                        "reload the offer and retry after its turn settles",
                    )
                cancelled_event_id = latest.id
            updated = conn.execute(
                "UPDATE task_offers SET status = 'withdrawn', updated_at = ?, "
                "withdrawn_at = ? WHERE id = ? AND world_id = ? "
                "AND current_revision = ? "
                "AND status IN ('pending', 'changes_requested')",
                (
                    self._ts(now),
                    self._ts(now),
                    offer.id,
                    self._world_id,
                    expected_revision,
                ),
            )
            if updated.rowcount != 1:
                raise self._error(
                    "task_offer_not_withdrawable",
                    f"TaskOffer {offer.id} changed before withdrawal",
                    "reload the offer before retrying",
                )
            snapshot = self._snapshot_uncommitted(conn, offer.id)
        return TaskOfferOperation(
            snapshot=snapshot,
            event_id=cancelled_event_id,
        )

    def respond(
        self,
        *,
        offer_id: str,
        expected_revision: int,
        subject_engram_id: str,
        decision: TaskOfferDecision | str,
        subject_response: str | None = None,
        decision_event_id: str | None = None,
        **compatibility: object,
    ) -> TaskOfferOperation:
        """Commit the current subject's Pi-tool decision exactly once."""

        # An early local draft used ``response`` before the frozen seam
        # named the same value ``subject_response``.  Accept that one keyword
        # during branch integration without advertising a second protocol.
        if compatibility:
            if set(compatibility) != {"response"} or subject_response is not None:
                raise self._error(
                    "invalid_task_offer",
                    "respond received an unknown or duplicate response field",
                    "send subject_response once",
                    status=400,
                )
            subject_response = compatibility["response"]  # type: ignore[assignment]

        offer_id = self._require_id(
            offer_id,
            "offer_id",
            code="task_offer_context_required",
        )
        expected_revision = self._normalize_revision(expected_revision)
        subject_engram_id = self._require_id(
            subject_engram_id,
            "subject_engram_id",
            code="task_offer_subject_mismatch",
        )
        decision_event_id = self._require_id(
            decision_event_id,
            "decision_event_id",
            code="task_offer_context_required",
        )
        try:
            decision = (
                decision
                if isinstance(decision, TaskOfferDecision)
                else TaskOfferDecision(decision)
            )
        except (TypeError, ValueError) as exc:
            raise self._error(
                "invalid_task_offer",
                "decision must be accept, refuse, or request_changes",
                "use one frozen TaskOffer decision",
                status=400,
            ) from exc
        response = self._normalize_response(subject_response, decision)

        with self._ledger._transaction() as conn:
            offer = self._require_offer_uncommitted(conn, offer_id)
            if offer.subject_engram_id != subject_engram_id:
                raise self._error(
                    "task_offer_subject_mismatch",
                    "the current Engram does not own this TaskOffer",
                    "respond only from the offer root delivered to its current subject",
                    status=403,
                )
            revision = self._storage._get_task_offer_revision_uncommitted(
                conn,
                offer.id,
                expected_revision,
            )
            if revision is None:
                raise self._revision_conflict(offer.id, expected_revision)
            if revision.decision is not None:
                if (
                    revision.decision is decision
                    and revision.subject_response == response
                    and revision.decision_event_id == decision_event_id
                ):
                    snapshot = self._snapshot_uncommitted(conn, offer.id)
                    task_event_id = None
                    if decision is TaskOfferDecision.ACCEPT:
                        task_event_id = self._stable_id(
                            offer.id,
                            f"revision:{expected_revision}:accepted-task-root",
                        )
                        if self._ledger._get_event_uncommitted(
                            conn,
                            task_event_id,
                        ) is None:
                            raise self._inconsistent(
                                offer.id,
                                "accepted task event is missing",
                            )
                        if self._storage._get_task_relationship_by_unique_uncommitted(
                            conn,
                            "accepted_offer_id",
                            offer.id,
                            world_id=self._world_id,
                        ) is None:
                            raise self._inconsistent(
                                offer.id,
                                "accepted task relationship is missing",
                            )
                    return TaskOfferOperation(
                        snapshot=snapshot,
                        event_id=task_event_id,
                        task_front_id=offer.task_front_id,
                        decision=decision,
                        decision_revision=expected_revision,
                        duplicate=True,
                    )
                raise self._error(
                    "task_offer_already_resolved",
                    f"TaskOffer revision {expected_revision} already has a decision",
                    "reload the durable subject decision; do not overwrite it",
                )
            self._assert_revision(offer, expected_revision)
            if offer.status is not TaskOfferStatus.PENDING:
                raise self._error(
                    "task_offer_already_resolved",
                    f"TaskOffer {offer.id} is {offer.status.value}",
                    "reload the offer before responding",
                )
            subject = self._require_active_subject_uncommitted(
                conn,
                subject_engram_id,
            )
            self._validate_decision_event_uncommitted(
                conn,
                offer,
                revision,
                decision_event_id,
            )
            now = self._now()
            task_event = None
            task_front_id = None
            next_status = {
                TaskOfferDecision.ACCEPT: TaskOfferStatus.ACCEPTED,
                TaskOfferDecision.REFUSE: TaskOfferStatus.REFUSED,
                TaskOfferDecision.REQUEST_CHANGES: (
                    TaskOfferStatus.CHANGES_REQUESTED
                ),
            }[decision]
            if decision is TaskOfferDecision.ACCEPT:
                self._require_project_uncommitted(conn, revision.project_id)
                task_front, task_event = (
                    self._insert_accepted_task_bundle_uncommitted(
                        conn,
                        offer,
                        revision,
                        subject,
                        decision_event_id=decision_event_id,
                        subject_response=response,
                        now=now,
                    )
                )
                task_front_id = task_front.id
            decided_revision = conn.execute(
                "UPDATE task_offer_revisions SET decision = ?, "
                "subject_response = ?, decision_event_id = ?, decided_at = ? "
                "WHERE offer_id = ? AND revision = ? AND decision IS NULL",
                (
                    decision.value,
                    response,
                    decision_event_id,
                    self._ts(now),
                    offer.id,
                    expected_revision,
                ),
            )
            if decided_revision.rowcount != 1:
                raise self._error(
                    "task_offer_already_resolved",
                    f"TaskOffer revision {expected_revision} changed before decision",
                    "reload the durable winner",
                )
            decided_at = (
                self._ts(now)
                if next_status in {
                    TaskOfferStatus.ACCEPTED,
                    TaskOfferStatus.REFUSED,
                }
                else None
            )
            decided_offer = conn.execute(
                "UPDATE task_offers SET status = ?, task_front_id = ?, "
                "updated_at = ?, decided_at = ? WHERE id = ? "
                "AND world_id = ? AND subject_engram_id = ? "
                "AND current_revision = ? AND status = 'pending'",
                (
                    next_status.value,
                    task_front_id,
                    self._ts(now),
                    decided_at,
                    offer.id,
                    self._world_id,
                    subject_engram_id,
                    expected_revision,
                ),
            )
            if decided_offer.rowcount != 1:
                raise self._error(
                    "task_offer_already_resolved",
                    f"TaskOffer {offer.id} changed before decision",
                    "reload the durable winner",
                )
            snapshot = self._snapshot_uncommitted(conn, offer.id)
        return TaskOfferOperation(
            snapshot=snapshot,
            event_id=task_event.id if task_event is not None else None,
            task_front_id=task_front_id,
            decision=decision,
            decision_revision=expected_revision,
        )

    # ── Transaction-local primitives ──────────────────────────

    def _insert_accepted_task_bundle_uncommitted(
        self,
        conn,
        offer: TaskOffer,
        revision: TaskOfferRevision,
        subject: Engram,
        *,
        decision_event_id: str,
        subject_response: str | None,
        now: datetime,
    ) -> tuple[TaskFront, CausalEvent]:
        center_id = self._stable_id(offer.id, "accepted-task-center")
        front_id = self._stable_id(offer.id, "accepted-task-front")
        event_id = self._stable_id(
            offer.id,
            f"revision:{revision.revision}:accepted-task-root",
        )
        center = ActivityCenter(
            id=center_id,
            kind=ActivityKind.TASK,
            title=revision.title,
            origin=ActivityOrigin.SHARED,
            autonomy=1.0,
            project_id=revision.project_id,
            focal_engram_id=subject.id,
            created_at=now,
            updated_at=now,
        )
        membership = CenterMembership(
            center_id=center.id,
            engram_id=subject.id,
            relation=MembershipRelation.FOCAL,
            created_at=now,
        )
        front = TaskFront(
            id=front_id,
            center_id=center.id,
            focal_engram_id=subject.id,
            title=revision.title,
            status=TaskFrontStatus.OPEN,
            created_at=now,
            updated_at=now,
            last_opened_at=now,
        )
        self._storage._insert_activity_center(center)
        self._storage._insert_center_membership(membership)
        self._storage._insert_task_front(front)
        relationship = TaskRelationship(
            id=task_relationship_id_for_offer(offer.id),
            world_id=self._world_id,
            accepted_offer_id=offer.id,
            task_front_id=front.id,
            center_id=center.id,
            original_subject_engram_id=subject.id,
            current_subject_engram_id=subject.id,
            status=TaskRelationshipStatus.ACTIVE,
            revision=1,
            latest_subject_note=subject_response,
            created_at=now,
            updated_at=now,
        )
        self._storage._insert_task_relationship(relationship)
        self._storage._insert_task_relationship_event(TaskRelationshipEvent(
            relationship_id=relationship.id,
            seq=1,
            action=TaskRelationshipAction.ACCEPTED,
            actor_kind=TaskRelationshipActorKind.SUBJECT,
            actor_id=subject.id,
            before_status=None,
            after_status=TaskRelationshipStatus.ACTIVE,
            content=subject_response,
            source_event_id=decision_event_id,
            created_at=now,
        ))
        self._ledger._ensure_references(
            conn,
            subject.id,
            center.id,
            require_membership=True,
        )
        task_event = CausalEvent(
            id=event_id,
            world_id=self._world_id,
            engram_id=subject.id,
            center_id=center.id,
            flow=CausalEventFlow.CONTENT,
            domain=CausalEventDomain.PULSE,
            kind=CausalEventKind.STIMULUS,
            source=CausalEventSource.USER,
            status=CausalEventStatus.QUEUED,
            content=revision.content,
            metadata={
                "task_offer_id": offer.id,
                "task_offer_revision": revision.revision,
                "decision_event_id": decision_event_id,
                "task_relationship_id": relationship.id,
                "reason_code": "task_offer_accepted",
                "priority": 1.0,
            },
            idempotency_key=self._stable_key(
                offer.id,
                f"revision:{revision.revision}:accepted-task-root",
            ),
            created_at=now,
            updated_at=now,
        )
        self._ledger._insert_event_uncommitted(conn, task_event)
        return front, task_event

    def _offer_event(
        self,
        revision: TaskOfferRevision,
        *,
        subject_engram_id: str,
        event_id: str,
        reason_code: str,
        provenance_digest: str | None,
        created_at: datetime,
    ) -> CausalEvent:
        metadata: dict[str, object] = {
            "task_offer_id": revision.offer_id,
            "task_offer_revision": revision.revision,
            "reason_code": reason_code,
            "priority": 1.0,
            "protocol": _PROTOCOL,
        }
        if provenance_digest is not None:
            metadata["stimulus_provenance_digest"] = provenance_digest
        return CausalEvent(
            id=event_id,
            world_id=self._world_id,
            engram_id=subject_engram_id,
            center_id=None,
            flow=CausalEventFlow.CONTENT,
            domain=CausalEventDomain.WORLD,
            kind=CausalEventKind.STIMULUS,
            source=CausalEventSource.USER,
            status=CausalEventStatus.QUEUED,
            content=_OFFER_PREAMBLE + revision.content,
            metadata=metadata,
            idempotency_key=self._stable_key(
                revision.offer_id,
                f"revision:{revision.revision}:delivery:{event_id}",
            ),
            created_at=created_at,
            updated_at=created_at,
        )

    def _validate_decision_event_uncommitted(
        self,
        conn,
        offer: TaskOffer,
        revision: TaskOfferRevision,
        decision_event_id: str,
    ) -> None:
        decision_event = self._ledger._get_event_uncommitted(
            conn,
            decision_event_id,
        )
        if decision_event is None:
            raise self._error(
                "task_offer_context_required",
                "decision_event_id does not identify a durable Pi tool call",
                "respond from pulse_task_offer_respond in the current offer root",
                status=403,
            )
        root = self._ledger._get_event_uncommitted(
            conn,
            revision.latest_offer_event_id,
        )
        if root is None:
            raise self._inconsistent(offer.id, "current offer root is missing")
        self._assert_offer_root_identity(offer, revision, root)
        if not (
            root.id == decision_event.parent_event_id
            and root.engram_id == offer.subject_engram_id
            and root.status is CausalEventStatus.RUNNING
            and decision_event.world_id == self._world_id
            and decision_event.engram_id == offer.subject_engram_id
            and decision_event.causal_id == root.causal_id
            and decision_event.kind is CausalEventKind.TOOL_CALL
            and decision_event.domain is CausalEventDomain.HARNESS
            and decision_event.source is CausalEventSource.SELF
            and decision_event.status is CausalEventStatus.SETTLED
            and decision_event.metadata.get("tool_name") == _RESPONSE_TOOL
        ):
            raise self._error(
                "task_offer_context_required",
                "the decision tool call is not bound to the current offer root",
                "respond only from the current TaskOffer deliberation turn",
                status=403,
            )

    def _assert_offer_root_identity(
        self,
        offer: TaskOffer,
        revision: TaskOfferRevision,
        root: CausalEvent,
    ) -> None:
        """Fail closed if a revision points at an unrelated causal event."""

        if not (
            root.id == revision.latest_offer_event_id
            and root.world_id == self._world_id
            and root.center_id is None
            and root.domain is CausalEventDomain.WORLD
            and root.kind is CausalEventKind.STIMULUS
            and root.source is CausalEventSource.USER
            and root.metadata.get("task_offer_id") == offer.id
            and root.metadata.get("task_offer_revision") == revision.revision
            and root.content == _OFFER_PREAMBLE + revision.content
        ):
            raise self._inconsistent(
                offer.id,
                "current revision points at an unrelated deliberation event",
            )

    def _snapshot_uncommitted(
        self,
        conn,
        offer_id: str,
    ) -> TaskOfferSnapshot:
        offer = self._storage._get_task_offer_uncommitted(
            conn,
            offer_id,
            world_id=self._world_id,
        )
        if offer is None:
            raise self._unknown_offer(offer_id)
        revisions = self._storage._list_task_offer_revisions_uncommitted(
            conn,
            offer.id,
        )
        current = next(
            (
                revision
                for revision in revisions
                if revision.revision == offer.current_revision
            ),
            None,
        )
        if current is None:
            raise self._inconsistent(offer.id, "current revision is missing")
        return TaskOfferSnapshot(
            offer=offer,
            current_revision=current,
            revisions=tuple(revisions),
        )

    def _require_offer_uncommitted(self, conn, offer_id: str) -> TaskOffer:
        offer = self._storage._get_task_offer_uncommitted(
            conn,
            offer_id,
            world_id=self._world_id,
        )
        if offer is None:
            raise self._unknown_offer(offer_id)
        return offer

    def _require_revision_uncommitted(
        self,
        conn,
        offer_id: str,
        revision: int,
    ) -> TaskOfferRevision:
        value = self._storage._get_task_offer_revision_uncommitted(
            conn,
            offer_id,
            revision,
        )
        if value is None:
            raise self._revision_conflict(offer_id, revision)
        return value

    def _require_active_subject_uncommitted(
        self,
        conn,
        subject_engram_id: str,
    ) -> Engram:
        row = conn.execute(
            "SELECT * FROM engrams WHERE id = ?",
            (subject_engram_id,),
        ).fetchone()
        if row is None:
            raise self._error(
                "unknown_task_offer_subject",
                f"no subject Engram {subject_engram_id}",
                "select an existing active subject",
                status=404,
            )
        subject = self._storage._row_to_engram(row)
        if subject.status is not EngramStatus.ACTIVE:
            raise self._error(
                "task_offer_subject_inactive",
                f"subject Engram {subject_engram_id} is {subject.status.value}",
                "use the current active successor Engram",
            )
        return subject

    def _require_project_uncommitted(
        self,
        conn,
        project_id: str | None,
    ) -> None:
        if project_id is None:
            return
        if conn.execute(
            "SELECT 1 FROM projects WHERE id = ?",
            (project_id,),
        ).fetchone() is None:
            raise self._error(
                "unknown_project",
                f"no project {project_id}",
                "create the project first or omit project_id",
                status=404,
            )

    # ── Validation and stable identities ──────────────────────

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _ts(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat()

    def _assert_revision(self, offer: TaskOffer, expected_revision: int) -> None:
        if offer.current_revision != expected_revision:
            raise self._revision_conflict(offer.id, expected_revision)

    @staticmethod
    def _normalize_content(content: str) -> str:
        if not isinstance(content, str) or not content.strip() or len(content) > 12_000:
            raise TaskOfferError(
                "invalid_task_offer",
                "content must contain 1..12000 characters",
                "send non-empty task terms within the size limit",
                status=400,
            )
        return content

    @staticmethod
    def _normalize_title(title: str | None, content: str) -> str:
        value = (session_name(content) or "New task") if title is None else title
        if not isinstance(value, str):
            raise TaskOfferError(
                "invalid_task_offer",
                "title must be a string or null",
                "send a title containing 1..120 characters",
                status=400,
            )
        value = value.strip()
        if not 1 <= len(value) <= 120:
            raise TaskOfferError(
                "invalid_task_offer",
                "title must contain 1..120 characters after trimming",
                "shorten the task title",
                status=400,
            )
        return value

    @staticmethod
    def _normalize_optional_id(value: str | None, field_name: str) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise TaskOfferError(
                "invalid_task_offer",
                f"{field_name} must be a non-empty string or null",
                f"send a valid {field_name} or omit it",
                status=400,
            )
        return value

    @staticmethod
    def _normalize_provenance(value: str | None) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip() or len(value) > 256:
            raise TaskOfferError(
                "invalid_task_offer",
                "stimulus_provenance_digest must be a non-empty bounded string",
                "pass the digest emitted by the typed stimulus firewall",
                status=400,
            )
        return value

    @staticmethod
    def _normalize_revision(value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise TaskOfferError(
                "invalid_task_offer",
                "expected_revision must be an integer >= 1",
                "reload the offer and send its current revision",
                status=400,
            )
        return value

    @staticmethod
    def _normalize_response(
        response: str | None,
        decision: TaskOfferDecision,
    ) -> str | None:
        if response is not None and not isinstance(response, str):
            raise TaskOfferError(
                "invalid_task_offer",
                "response must be a string or null",
                "send a bounded natural-language response",
                status=400,
            )
        if response is not None and len(response) > 4_000:
            raise TaskOfferError(
                "invalid_task_offer",
                "response must contain at most 4000 characters",
                "shorten the subject response",
                status=400,
            )
        if decision is TaskOfferDecision.REQUEST_CHANGES:
            if response is None or not response.strip():
                raise TaskOfferError(
                    "task_offer_response_required",
                    "request_changes requires the subject's requested changes",
                    "include a non-empty response",
                    status=400,
                )
            return response
        if response is not None and not response.strip():
            return None
        return response

    @staticmethod
    def _require_id(
        value: str | None,
        field_name: str,
        *,
        code: str,
    ) -> str:
        if not isinstance(value, str) or not value.strip():
            raise TaskOfferError(
                code,
                f"{field_name} must be a non-empty string",
                f"supply the current {field_name}",
                status=400 if code == "invalid_task_offer" else 403,
            )
        return value

    def _stable_id(self, offer_id: str, suffix: str) -> str:
        return hashlib.sha256(
            f"{_PROTOCOL}:{self._world_id}:{offer_id}:{suffix}".encode("utf-8")
        ).hexdigest()[:32]

    def _stable_key(self, offer_id: str, suffix: str) -> str:
        return "task-offer:" + hashlib.sha256(
            f"{_PROTOCOL}:{self._world_id}:{offer_id}:{suffix}".encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _error(
        code: str,
        detail: str,
        remedy: str,
        *,
        status: int = 409,
    ) -> TaskOfferError:
        return TaskOfferError(code, detail, remedy, status=status)

    def _unknown_offer(self, offer_id: str) -> TaskOfferError:
        return self._error(
            "unknown_task_offer",
            f"no TaskOffer {offer_id} in world {self._world_id}",
            "list TaskOffers and use one of their ids",
            status=404,
        )

    def _revision_conflict(
        self,
        offer_id: str,
        expected_revision: int,
    ) -> TaskOfferError:
        return self._error(
            "task_offer_revision_conflict",
            f"TaskOffer {offer_id} is no longer at revision {expected_revision}",
            "reload the current revision before retrying",
        )

    def _inconsistent(self, offer_id: str, detail: str) -> TaskOfferError:
        return self._error(
            "task_offer_inconsistent",
            f"TaskOffer {offer_id}: {detail}",
            "stop mutation and repair the durable aggregate",
            status=500,
        )


__all__ = [
    "TaskOfferDecisionReceipt",
    "TaskOfferError",
    "TaskOfferOperation",
    "TaskOfferService",
]

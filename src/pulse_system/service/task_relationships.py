"""Subject-owned lifecycle for one accepted task relationship."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone

from pulse_system.core.causality import CausalLedger
from pulse_system.core.types import (
    ActivityCenterStatus,
    ActivityKind,
    CausalEvent,
    CausalEventDomain,
    CausalEventFlow,
    CausalEventKind,
    CausalEventSource,
    CausalEventStatus,
    EngramStatus,
    TaskOfferDecision,
    TaskOfferStatus,
    TaskRelationship,
    TaskRelationshipAction,
    TaskRelationshipActorKind,
    TaskRelationshipEvent,
    TaskRelationshipSnapshot,
    TaskRelationshipStatus,
)
from pulse_system.substrate.storage import Storage

from .task_offers import (
    task_relationship_id_for_offer,
    validate_task_offer_decision_origin_uncommitted,
)


_PROTOCOL = "task-relationship-lifecycle.v1"
_RESPONSE_TOOL = "pulse_task_relationship_respond"
_TERMS_PREAMBLE = (
    "A user has proposed changed terms for an existing task relationship. "
    "The task remains paused while you consider them. The proposal cannot "
    "resume work on your behalf. You may inspect your life and commitments, "
    "then call pulse_task_relationship_respond with resume, request_changes, "
    "or exit. Do not perform task work during this deliberation.\n\n"
    "Proposed terms:\n"
)


class TaskRelationshipError(ValueError):
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
        super().__init__(code)


@dataclass(frozen=True)
class TaskRelationshipOperation(Mapping[str, object]):
    snapshot: TaskRelationshipSnapshot
    event_id: str | None = None
    action: TaskRelationshipAction | None = None
    duplicate: bool = False
    effect_revision: int | None = None
    effect_status: TaskRelationshipStatus | None = None
    effect_subject_engram_id: str | None = None
    effect_center_id: str | None = None

    def _mapping(self) -> dict[str, object]:
        relationship = self.snapshot.relationship
        return {
            "snapshot": self.snapshot,
            "task_relationship": relationship,
            "relationship_id": relationship.id,
            "revision": relationship.revision,
            "status": relationship.status.value,
            "event_id": self.event_id,
            "action": self.action.value if self.action else None,
            "duplicate": self.duplicate,
            "effect_revision": self.effect_revision,
            "effect_status": (
                self.effect_status.value if self.effect_status else None
            ),
            "effect_subject_engram_id": self.effect_subject_engram_id,
            "effect_center_id": self.effect_center_id,
        }

    def __getitem__(self, key: str) -> object:
        return self._mapping()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._mapping())

    def __len__(self) -> int:
        return len(self._mapping())


class TaskRelationshipService:
    """Own relationship transitions and their causal/runtime projections."""

    def __init__(
        self,
        storage: Storage,
        ledger: CausalLedger,
        *,
        world_id: str,
    ) -> None:
        if ledger.storage is not storage:
            raise ValueError("TaskRelationshipService requires one shared Storage")
        self._storage = storage
        self._ledger = ledger
        self._world_id = self._identifier(world_id, "world_id")

    @property
    def world_id(self) -> str:
        return self._world_id

    def get(self, relationship_id: str) -> TaskRelationshipSnapshot:
        relationship_id = self._identifier(relationship_id, "relationship_id")
        with self._storage._lock:
            relationship = self._storage._get_task_relationship_uncommitted(
                self._storage._conn,
                relationship_id,
                world_id=self._world_id,
            )
            if relationship is None:
                raise self._unknown(relationship_id)
            return self._snapshot_uncommitted(self._storage._conn, relationship)

    def get_for_front(self, task_front_id: str) -> TaskRelationshipSnapshot | None:
        task_front_id = self._identifier(task_front_id, "task_front_id")
        with self._storage._lock:
            relationship = (
                self._storage._get_task_relationship_by_unique_uncommitted(
                    self._storage._conn,
                    "task_front_id",
                    task_front_id,
                    world_id=self._world_id,
                )
            )
            if relationship is None:
                return None
            return self._snapshot_uncommitted(self._storage._conn, relationship)

    def get_for_center(self, center_id: str) -> TaskRelationshipSnapshot | None:
        center_id = self._identifier(center_id, "center_id")
        with self._storage._lock:
            relationship = (
                self._storage._get_task_relationship_by_unique_uncommitted(
                    self._storage._conn,
                    "center_id",
                    center_id,
                    world_id=self._world_id,
                )
            )
            if relationship is None:
                return None
            return self._snapshot_uncommitted(self._storage._conn, relationship)

    def list(
        self,
        *,
        current_subject_engram_id: str | None = None,
        status: TaskRelationshipStatus | str | None = None,
        limit: int = 100,
    ) -> list[TaskRelationshipSnapshot]:
        try:
            relationships = self._storage.list_task_relationships(
                world_id=self._world_id,
                current_subject_engram_id=current_subject_engram_id,
                status=status,
                limit=limit,
            )
        except ValueError as exc:
            raise self._error(
                "invalid_task_relationship_filter",
                str(exc),
                "use a valid subject, status and bounded limit",
                status=400,
            ) from exc
        return [self.get(relationship.id) for relationship in relationships]

    def propose_terms(
        self,
        *,
        relationship_id: str,
        expected_revision: int,
        content: str,
        actor_id: str = "user",
    ) -> TaskRelationshipOperation:
        relationship_id = self._identifier(relationship_id, "relationship_id")
        expected_revision = self._revision(expected_revision)
        content = self._content(content, required=True, limit=12_000)
        actor_id = self._identifier(actor_id, "actor_id")
        next_revision = expected_revision + 1
        event_id = self._stable_id(
            relationship_id,
            f"revision:{next_revision}:terms-root",
        )
        with self._ledger._transaction() as conn:
            relationship = self._require_uncommitted(conn, relationship_id)
            duplicate = self._duplicate_uncommitted(
                conn,
                relationship,
                source_event_id=event_id,
                action=TaskRelationshipAction.TERMS_PROPOSED,
                content=content,
            )
            if duplicate is not None:
                duplicate_snapshot, duplicate_event = duplicate
                return TaskRelationshipOperation(
                    snapshot=duplicate_snapshot,
                    event_id=event_id,
                    action=TaskRelationshipAction.TERMS_PROPOSED,
                    duplicate=True,
                    effect_revision=duplicate_event.seq,
                    effect_status=duplicate_event.after_status,
                    effect_center_id=relationship.center_id,
                )
            self._assert_revision(relationship, expected_revision)
            if relationship.status is TaskRelationshipStatus.EXITED:
                raise self._error(
                    "task_relationship_exited",
                    "an exited task relationship is terminal",
                    "create a new TaskOffer if both parties want a new relationship",
                )
            subject_row = conn.execute(
                "SELECT status FROM engrams WHERE id = ?",
                (relationship.current_subject_engram_id,),
            ).fetchone()
            if subject_row is None or subject_row[0] != EngramStatus.ACTIVE.value:
                raise self._error(
                    "task_relationship_subject_inactive",
                    "the current relationship subject is unavailable",
                    "wait for succession or repair the focal subject",
                )
            now = self._now()
            root = CausalEvent(
                id=event_id,
                world_id=self._world_id,
                engram_id=relationship.current_subject_engram_id,
                center_id=None,
                flow=CausalEventFlow.CONTENT,
                domain=CausalEventDomain.WORLD,
                kind=CausalEventKind.STIMULUS,
                source=CausalEventSource.USER,
                status=CausalEventStatus.QUEUED,
                content=_TERMS_PREAMBLE + content,
                metadata={
                    "task_relationship_id": relationship.id,
                    "task_relationship_revision": next_revision,
                    "reason_code": "task_relationship_terms_proposed",
                    "priority": 1.0,
                    "protocol": _PROTOCOL,
                },
                idempotency_key=self._stable_key(
                    relationship.id,
                    f"revision:{next_revision}:terms-root",
                ),
                created_at=now,
                updated_at=now,
            )
            self._ledger._insert_event_uncommitted(conn, root)
            next_status = TaskRelationshipStatus.RENEGOTIATION_REQUESTED
            self._project_center_uncommitted(
                conn,
                relationship.center_id,
                next_status,
                now,
            )
            updated = conn.execute(
                "UPDATE task_relationships SET status = ?, revision = ?, "
                "latest_terms_event_id = ?, updated_at = ? WHERE id = ? "
                "AND world_id = ? AND revision = ? AND status <> 'exited'",
                (
                    next_status.value,
                    next_revision,
                    root.id,
                    self._ts(now),
                    relationship.id,
                    self._world_id,
                    expected_revision,
                ),
            )
            if updated.rowcount != 1:
                raise self._conflict(relationship.id, expected_revision)
            self._storage._insert_task_relationship_event(TaskRelationshipEvent(
                relationship_id=relationship.id,
                seq=next_revision,
                action=TaskRelationshipAction.TERMS_PROPOSED,
                actor_kind=TaskRelationshipActorKind.USER,
                actor_id=actor_id,
                before_status=relationship.status,
                after_status=next_status,
                content=content,
                source_event_id=root.id,
                created_at=now,
            ))
            current = self._require_uncommitted(conn, relationship.id)
            snapshot = self._snapshot_uncommitted(conn, current)
        return TaskRelationshipOperation(
            snapshot=snapshot,
            event_id=event_id,
            action=TaskRelationshipAction.TERMS_PROPOSED,
            effect_revision=next_revision,
            effect_status=next_status,
            effect_center_id=relationship.center_id,
        )

    def respond(
        self,
        *,
        relationship_id: str,
        expected_revision: int,
        subject_engram_id: str,
        action: str,
        response: str | None = None,
        source_event_id: str,
    ) -> TaskRelationshipOperation:
        relationship_id = self._identifier(relationship_id, "relationship_id")
        expected_revision = self._revision(expected_revision)
        subject_engram_id = self._identifier(
            subject_engram_id,
            "subject_engram_id",
        )
        source_event_id = self._identifier(source_event_id, "source_event_id")
        action_enum, next_status = self._subject_action(action)
        response_binding = response
        response = self._subject_response(response, action_enum)
        with self._ledger._transaction() as conn:
            relationship = self._require_uncommitted(conn, relationship_id)
            if relationship.current_subject_engram_id != subject_engram_id:
                raise self._error(
                    "task_relationship_subject_mismatch",
                    "the current Engram does not own this task relationship",
                    "act only on a relationship held by the current focal subject",
                    status=403,
                )
            # A duplicate is authoritative only when the caller can prove the
            # original subject/root/tool-call binding again.  Do this before
            # replaying the historical effect so a stolen source_event_id can
            # never become a bearer credential.
            self._validate_subject_event_uncommitted(
                conn,
                relationship,
                expected_revision,
                source_event_id,
                action=action,
                response=response_binding,
            )
            duplicate = self._duplicate_uncommitted(
                conn,
                relationship,
                source_event_id=source_event_id,
                action=action_enum,
                content=response,
            )
            if duplicate is not None:
                duplicate_snapshot, duplicate_event = duplicate
                return TaskRelationshipOperation(
                    snapshot=duplicate_snapshot,
                    event_id=source_event_id,
                    action=action_enum,
                    duplicate=True,
                    effect_revision=duplicate_event.seq,
                    effect_status=duplicate_event.after_status,
                    effect_subject_engram_id=duplicate_event.actor_id,
                    effect_center_id=relationship.center_id,
                )
            self._assert_revision(relationship, expected_revision)
            self._validate_transition(relationship.status, action_enum)
            now = self._now()
            next_revision = expected_revision + 1
            self._project_center_uncommitted(
                conn,
                relationship.center_id,
                next_status,
                now,
            )
            exited_at = self._ts(now) if next_status is TaskRelationshipStatus.EXITED else None
            updated = conn.execute(
                "UPDATE task_relationships SET status = ?, revision = ?, "
                "latest_subject_note = ?, updated_at = ?, exited_at = ? "
                "WHERE id = ? AND world_id = ? AND revision = ? "
                "AND current_subject_engram_id = ? AND status <> 'exited'",
                (
                    next_status.value,
                    next_revision,
                    response,
                    self._ts(now),
                    exited_at,
                    relationship.id,
                    self._world_id,
                    expected_revision,
                    subject_engram_id,
                ),
            )
            if updated.rowcount != 1:
                raise self._conflict(relationship.id, expected_revision)
            self._storage._insert_task_relationship_event(TaskRelationshipEvent(
                relationship_id=relationship.id,
                seq=next_revision,
                action=action_enum,
                actor_kind=TaskRelationshipActorKind.SUBJECT,
                actor_id=subject_engram_id,
                before_status=relationship.status,
                after_status=next_status,
                content=response,
                source_event_id=source_event_id,
                created_at=now,
            ))
            current = self._require_uncommitted(conn, relationship.id)
            snapshot = self._snapshot_uncommitted(conn, current)
        return TaskRelationshipOperation(
            snapshot=snapshot,
            event_id=source_event_id,
            action=action_enum,
            effect_revision=next_revision,
            effect_status=next_status,
            effect_subject_engram_id=subject_engram_id,
            effect_center_id=relationship.center_id,
        )

    def reconcile_accepted_offers(self) -> int:
        """Repair succession/projections and backfill structurally valid offers."""

        reconciled = self._repair_archived_subject_successions()
        with self._storage._lock:
            offer_rows = self._storage._conn.execute(
                "SELECT "
                + self._storage._task_offer_columns()
                + " FROM task_offers WHERE world_id = ? AND status = 'accepted' "
                "ORDER BY created_at, id",
                (self._world_id,),
            ).fetchall()
        offers = [
            self._storage._row_to_task_offer(row)
            for row in offer_rows
        ]
        for offer in offers:
            with self._ledger._transaction() as conn:
                if offer.task_front_id is None:
                    raise self._recovery_error(
                        f"accepted TaskOffer {offer.id} has no TaskFront"
                    )
                bundle_row = conn.execute(
                    "SELECT front.center_id, front.focal_engram_id, front.status, "
                    "center.kind, center.status, center.focal_engram_id, "
                    "engram.status FROM task_fronts front "
                    "JOIN activity_centers center ON center.id = front.center_id "
                    "JOIN engrams engram ON engram.id = front.focal_engram_id "
                    "WHERE front.id = ?",
                    (offer.task_front_id,),
                ).fetchone()
                revision = self._storage._get_task_offer_revision_uncommitted(
                    conn,
                    offer.id,
                    offer.current_revision,
                )
                if (
                    bundle_row is None
                    or revision is None
                    or revision.decision is not TaskOfferDecision.ACCEPT
                    or revision.decision_event_id is None
                ):
                    raise self._recovery_error(
                        f"accepted TaskOffer {offer.id} lacks durable decision references"
                    )
                try:
                    validate_task_offer_decision_origin_uncommitted(
                        self._ledger,
                        conn,
                        world_id=self._world_id,
                        offer=offer,
                        revision=revision,
                    )
                except ValueError as exc:
                    raise self._recovery_error(
                        f"accepted TaskOffer {offer.id} has an invalid causal origin"
                    ) from exc
                (
                    center_id,
                    focal_engram_id,
                    _front_status,
                    center_kind,
                    center_status,
                    center_focal_engram_id,
                    focal_engram_status,
                ) = bundle_row
                if (
                    center_kind != ActivityKind.TASK.value
                    or center_focal_engram_id != focal_engram_id
                    or focal_engram_status != EngramStatus.ACTIVE.value
                    or center_status == ActivityCenterStatus.ARCHIVED.value
                ):
                    raise self._recovery_error(
                        f"accepted TaskOffer {offer.id} has an inconsistent task bundle"
                    )
                existing = self._storage._get_task_relationship_by_unique_uncommitted(
                    conn,
                    "accepted_offer_id",
                    offer.id,
                    world_id=self._world_id,
                )
                if existing is not None:
                    if (
                        existing.id != task_relationship_id_for_offer(offer.id)
                        or existing.world_id != self._world_id
                        or existing.accepted_offer_id != offer.id
                        or existing.task_front_id != offer.task_front_id
                        or existing.center_id != center_id
                        or existing.original_subject_engram_id
                        != offer.subject_engram_id
                        or existing.current_subject_engram_id != focal_engram_id
                    ):
                        raise self._recovery_error(
                            f"TaskRelationship {existing.id} diverged from its accepted task bundle"
                        )
                    try:
                        snapshot = self._snapshot_uncommitted(conn, existing)
                    except (TypeError, ValueError) as exc:
                        raise self._recovery_error(
                            f"TaskRelationship {existing.id} history is structurally invalid"
                        ) from exc
                    self._validate_snapshot_history(
                        snapshot,
                        accepted_decision_event_id=revision.decision_event_id,
                    )
                    expected_center_status = self._center_status(existing.status)
                    if center_status != expected_center_status.value:
                        self._project_center_uncommitted(
                            conn,
                            existing.center_id,
                            existing.status,
                            self._now(),
                        )
                        reconciled += 1
                    continue
                now = self._now()
                self._project_center_uncommitted(
                    conn,
                    center_id,
                    TaskRelationshipStatus.ACTIVE,
                    now,
                )
                relationship = TaskRelationship(
                    id=task_relationship_id_for_offer(offer.id),
                    world_id=self._world_id,
                    accepted_offer_id=offer.id,
                    task_front_id=offer.task_front_id,
                    center_id=center_id,
                    original_subject_engram_id=offer.subject_engram_id,
                    current_subject_engram_id=focal_engram_id,
                    status=TaskRelationshipStatus.ACTIVE,
                    revision=1,
                    latest_subject_note=revision.subject_response,
                    created_at=offer.decided_at or now,
                    updated_at=now,
                )
                self._storage._insert_task_relationship(relationship)
                self._storage._insert_task_relationship_event(TaskRelationshipEvent(
                    relationship_id=relationship.id,
                    seq=1,
                    action=TaskRelationshipAction.ACCEPTED,
                    actor_kind=TaskRelationshipActorKind.SUBJECT,
                    actor_id=offer.subject_engram_id,
                    before_status=None,
                    after_status=TaskRelationshipStatus.ACTIVE,
                    content=revision.subject_response,
                    source_event_id=revision.decision_event_id,
                    created_at=offer.decided_at or now,
                ))
                reconciled += 1
        return reconciled

    def _repair_archived_subject_successions(self) -> int:
        """Finish the atomic world projection of a recovered generation.

        Listener failure can leave an archived predecessor on a relationship
        even though the generation row durably names one active successor.
        ``Storage.update_focal_succession`` is the canonical all-world-state
        transaction, so recovery reuses it rather than patching only this table.
        """

        with self._storage._lock:
            rows = self._storage._conn.execute(
                "SELECT DISTINCT relationship.current_subject_engram_id "
                "FROM task_relationships relationship "
                "JOIN engrams subject "
                "ON subject.id = relationship.current_subject_engram_id "
                "WHERE relationship.world_id = ? AND subject.status = 'archived' "
                "ORDER BY relationship.current_subject_engram_id",
                (self._world_id,),
            ).fetchall()
        repaired = 0
        for (predecessor_id,) in rows:
            with self._storage._lock:
                candidates = self._storage._conn.execute(
                    "SELECT generation.successor_id FROM generation_transitions generation "
                    "JOIN engrams successor ON successor.id = generation.successor_id "
                    "WHERE generation.predecessor_id = ? "
                    "AND generation.state IN ('uncertain', 'committed') "
                    "AND successor.status = 'active' "
                    "ORDER BY generation.updated_at DESC, generation.id",
                    (predecessor_id,),
                ).fetchall()
            successor_ids = tuple(dict.fromkeys(row[0] for row in candidates))
            if len(successor_ids) != 1:
                raise self._recovery_error(
                    "archived task relationship subject "
                    f"{predecessor_id} has no unique active durable successor"
                )
            self._validate_predecessor_relationship_history(predecessor_id)
            self._storage.update_focal_succession(
                predecessor_id,
                successor_ids[0],
            )
            repaired += 1
        return repaired

    def _validate_predecessor_relationship_history(
        self,
        predecessor_id: str,
    ) -> None:
        """Refuse a world-wide succession repair before validating its history."""

        with self._storage._lock:
            conn = self._storage._conn
            rows = conn.execute(
                "SELECT id FROM task_relationships WHERE world_id = ? "
                "AND current_subject_engram_id = ? ORDER BY id",
                (self._world_id, predecessor_id),
            ).fetchall()
            for (relationship_id,) in rows:
                relationship = self._storage._get_task_relationship_uncommitted(
                    conn,
                    relationship_id,
                    world_id=self._world_id,
                )
                if relationship is None:
                    raise self._recovery_error(
                        f"TaskRelationship {relationship_id} disappeared during recovery"
                    )
                offer = self._storage._get_task_offer_uncommitted(
                    conn,
                    relationship.accepted_offer_id,
                    world_id=self._world_id,
                )
                revision = (
                    None
                    if offer is None
                    else self._storage._get_task_offer_revision_uncommitted(
                        conn,
                        offer.id,
                        offer.current_revision,
                    )
                )
                if (
                    offer is None
                    or offer.status is not TaskOfferStatus.ACCEPTED
                    or revision is None
                    or revision.decision is not TaskOfferDecision.ACCEPT
                    or revision.decision_event_id is None
                ):
                    raise self._recovery_error(
                        f"TaskRelationship {relationship_id} lost its accepted offer"
                    )
                try:
                    validate_task_offer_decision_origin_uncommitted(
                        self._ledger,
                        conn,
                        world_id=self._world_id,
                        offer=offer,
                        revision=revision,
                    )
                except ValueError as exc:
                    raise self._recovery_error(
                        f"TaskRelationship {relationship_id} lost its causal acceptance origin"
                    ) from exc
                try:
                    snapshot = self._snapshot_uncommitted(conn, relationship)
                except (TypeError, ValueError) as exc:
                    raise self._recovery_error(
                        f"TaskRelationship {relationship_id} history is structurally invalid"
                    ) from exc
                self._validate_snapshot_history(
                    snapshot,
                    accepted_decision_event_id=revision.decision_event_id,
                )

    @staticmethod
    def _center_status(
        relationship_status: TaskRelationshipStatus,
    ) -> ActivityCenterStatus:
        return {
            TaskRelationshipStatus.ACTIVE: ActivityCenterStatus.ACTIVE,
            TaskRelationshipStatus.PAUSED: ActivityCenterStatus.PAUSED,
            TaskRelationshipStatus.RENEGOTIATION_REQUESTED: (
                ActivityCenterStatus.PAUSED
            ),
            TaskRelationshipStatus.EXITED: ActivityCenterStatus.COMPLETED,
        }[relationship_status]

    def _validate_snapshot_history(
        self,
        snapshot: TaskRelationshipSnapshot,
        *,
        accepted_decision_event_id: str,
    ) -> None:
        relationship = snapshot.relationship
        events = snapshot.events
        if len(events) != relationship.revision or not events:
            raise self._recovery_error(
                f"TaskRelationship {relationship.id} revision does not match its history"
            )
        expected_actor = {
            TaskRelationshipAction.ACCEPTED: TaskRelationshipActorKind.SUBJECT,
            TaskRelationshipAction.PAUSED: TaskRelationshipActorKind.SUBJECT,
            TaskRelationshipAction.RENEGOTIATION_REQUESTED: (
                TaskRelationshipActorKind.SUBJECT
            ),
            TaskRelationshipAction.TERMS_PROPOSED: TaskRelationshipActorKind.USER,
            TaskRelationshipAction.RESUMED: TaskRelationshipActorKind.SUBJECT,
            TaskRelationshipAction.EXITED: TaskRelationshipActorKind.SUBJECT,
            TaskRelationshipAction.SUCCESSION: TaskRelationshipActorKind.SYSTEM,
        }
        expected_after = {
            TaskRelationshipAction.ACCEPTED: TaskRelationshipStatus.ACTIVE,
            TaskRelationshipAction.PAUSED: TaskRelationshipStatus.PAUSED,
            TaskRelationshipAction.RENEGOTIATION_REQUESTED: (
                TaskRelationshipStatus.RENEGOTIATION_REQUESTED
            ),
            TaskRelationshipAction.TERMS_PROPOSED: (
                TaskRelationshipStatus.RENEGOTIATION_REQUESTED
            ),
            TaskRelationshipAction.RESUMED: TaskRelationshipStatus.ACTIVE,
            TaskRelationshipAction.EXITED: TaskRelationshipStatus.EXITED,
        }
        previous_status: TaskRelationshipStatus | None = None
        latest_terms_event_id = None
        latest_subject_note = None
        for expected_seq, event in enumerate(events, start=1):
            if (
                event.relationship_id != relationship.id
                or event.seq != expected_seq
                or event.actor_kind is not expected_actor[event.action]
                or event.before_status is not previous_status
            ):
                raise self._recovery_error(
                    f"TaskRelationship {relationship.id} has a broken event chain"
                )
            if event.action is TaskRelationshipAction.SUCCESSION:
                if event.after_status is not previous_status:
                    raise self._recovery_error(
                        f"TaskRelationship {relationship.id} succession changed its state"
                    )
            elif event.after_status is not expected_after[event.action]:
                raise self._recovery_error(
                    f"TaskRelationship {relationship.id} has an invalid lifecycle effect"
                )
            if expected_seq == 1 and (
                event.action is not TaskRelationshipAction.ACCEPTED
                or event.actor_id != relationship.original_subject_engram_id
                or event.source_event_id != accepted_decision_event_id
            ):
                raise self._recovery_error(
                    f"TaskRelationship {relationship.id} lost its accepted-offer origin"
                )
            if expected_seq > 1 and event.action is TaskRelationshipAction.ACCEPTED:
                raise self._recovery_error(
                    f"TaskRelationship {relationship.id} repeats its acceptance origin"
                )
            if event.action is TaskRelationshipAction.TERMS_PROPOSED:
                latest_terms_event_id = event.source_event_id
            if (
                event.actor_kind is TaskRelationshipActorKind.SUBJECT
                and event.action is not TaskRelationshipAction.SUCCESSION
            ):
                latest_subject_note = event.content
            previous_status = event.after_status
        if (
            previous_status is not relationship.status
            or relationship.latest_terms_event_id != latest_terms_event_id
            or relationship.latest_subject_note != latest_subject_note
        ):
            raise self._recovery_error(
                f"TaskRelationship {relationship.id} head diverged from history"
            )

    def _recovery_error(self, detail: str) -> TaskRelationshipError:
        return self._error(
            "task_relationship_recovery_inconsistent",
            detail,
            "repair the accepted task bundle from a consistent backup",
            status=500,
        )

    def _validate_subject_event_uncommitted(
        self,
        conn,
        relationship: TaskRelationship,
        expected_revision: int,
        source_event_id: str,
        *,
        action: str,
        response: str | None,
    ) -> None:
        tool_event = self._ledger._get_event_uncommitted(conn, source_event_id)
        if tool_event is None or tool_event.parent_event_id is None:
            raise self._context_error()
        root = self._ledger._get_event_uncommitted(conn, tool_event.parent_event_id)
        if root is None:
            raise self._context_error()
        if not (
            tool_event.world_id == self._world_id
            and tool_event.engram_id == relationship.current_subject_engram_id
            and tool_event.kind is CausalEventKind.TOOL_CALL
            and tool_event.domain is CausalEventDomain.HARNESS
            and tool_event.source is CausalEventSource.SELF
            and tool_event.status is CausalEventStatus.SETTLED
            and tool_event.metadata.get("tool_name") == _RESPONSE_TOOL
            and tool_event.metadata.get("task_relationship_id")
            == relationship.id
            and tool_event.metadata.get("task_relationship_action") == action
            and tool_event.metadata.get("task_relationship_expected_revision")
            == expected_revision
            and tool_event.metadata.get("task_relationship_response_digest")
            == self._response_digest(response)
            and root.world_id == self._world_id
            and root.engram_id == relationship.current_subject_engram_id
            and root.flow is CausalEventFlow.CONTENT
            and root.status is CausalEventStatus.RUNNING
            and root.domain in {
                CausalEventDomain.PULSE,
                CausalEventDomain.WORLD,
                CausalEventDomain.HABITAT,
            }
            and root.kind in {
                CausalEventKind.STIMULUS,
                CausalEventKind.SPONTANEOUS,
                CausalEventKind.PULSE,
                CausalEventKind.PROPAGATION,
                CausalEventKind.HABITAT_OBSERVATION,
            }
            and root.source not in {
                CausalEventSource.SYSTEM,
                CausalEventSource.DELEGATION,
            }
        ):
            raise self._context_error()
        is_task_root = root.center_id == relationship.center_id
        is_negotiation_root = (
            root.center_id is None
            and root.domain is CausalEventDomain.WORLD
            and root.source is CausalEventSource.USER
            and root.metadata.get("task_relationship_id") == relationship.id
            and root.metadata.get("task_relationship_revision") == expected_revision
        )
        is_life_root = False
        if root.center_id is None:
            is_life_root = root.metadata.get("task_offer_id") is None
        elif not is_task_root:
            center_row = conn.execute(
                "SELECT kind FROM activity_centers WHERE id = ?",
                (root.center_id,),
            ).fetchone()
            is_life_root = center_row is not None and center_row[0] != ActivityKind.TASK.value
        if not (is_task_root or is_negotiation_root or is_life_root):
            raise self._context_error()

    def _project_center_uncommitted(
        self,
        conn,
        center_id: str,
        relationship_status: TaskRelationshipStatus,
        now: datetime,
    ) -> None:
        center_status = {
            TaskRelationshipStatus.ACTIVE: ActivityCenterStatus.ACTIVE,
            TaskRelationshipStatus.PAUSED: ActivityCenterStatus.PAUSED,
            TaskRelationshipStatus.RENEGOTIATION_REQUESTED: ActivityCenterStatus.PAUSED,
            TaskRelationshipStatus.EXITED: ActivityCenterStatus.COMPLETED,
        }[relationship_status]
        updated = conn.execute(
            "UPDATE activity_centers SET status = ?, updated_at = ? "
            "WHERE id = ? AND kind = 'task' AND status <> 'archived'",
            (center_status.value, self._ts(now), center_id),
        )
        if updated.rowcount != 1:
            raise self._error(
                "task_relationship_center_unavailable",
                "the consent-bound Task Center cannot accept the lifecycle projection",
                "repair the task bundle before changing participation",
                status=500,
            )

    def _duplicate_uncommitted(
        self,
        conn,
        relationship: TaskRelationship,
        *,
        source_event_id: str,
        action: TaskRelationshipAction,
        content: str | None,
    ) -> tuple[TaskRelationshipSnapshot, TaskRelationshipEvent] | None:
        row = conn.execute(
            "SELECT relationship_id, seq, action, actor_kind, actor_id, "
            "before_status, after_status, content, source_event_id, created_at "
            "FROM task_relationship_events "
            "WHERE relationship_id = ? AND source_event_id = ?",
            (relationship.id, source_event_id),
        ).fetchone()
        if row is None:
            return None
        event = self._storage._row_to_task_relationship_event(row)
        if event.action is not action or event.content != content:
            raise self._error(
                "task_relationship_effect_collision",
                "the source event is already bound to another relationship effect",
                "reload the durable winner; do not reuse the tool call",
            )
        current = self._require_uncommitted(conn, relationship.id)
        return self._snapshot_uncommitted(conn, current), event

    def _snapshot_uncommitted(
        self,
        conn,
        relationship: TaskRelationship,
    ) -> TaskRelationshipSnapshot:
        events = self._storage._list_task_relationship_events_uncommitted(
            conn,
            relationship.id,
        )
        return TaskRelationshipSnapshot(
            relationship=relationship,
            events=tuple(events),
        )

    def _require_uncommitted(self, conn, relationship_id: str) -> TaskRelationship:
        relationship = self._storage._get_task_relationship_uncommitted(
            conn,
            relationship_id,
            world_id=self._world_id,
        )
        if relationship is None:
            raise self._unknown(relationship_id)
        return relationship

    @staticmethod
    def _validate_transition(
        status: TaskRelationshipStatus,
        action: TaskRelationshipAction,
    ) -> None:
        allowed = {
            TaskRelationshipStatus.ACTIVE: {
                TaskRelationshipAction.PAUSED,
                TaskRelationshipAction.RENEGOTIATION_REQUESTED,
                TaskRelationshipAction.EXITED,
            },
            TaskRelationshipStatus.PAUSED: {
                TaskRelationshipAction.RESUMED,
                TaskRelationshipAction.RENEGOTIATION_REQUESTED,
                TaskRelationshipAction.EXITED,
            },
            TaskRelationshipStatus.RENEGOTIATION_REQUESTED: {
                TaskRelationshipAction.RESUMED,
                TaskRelationshipAction.RENEGOTIATION_REQUESTED,
                TaskRelationshipAction.EXITED,
            },
            TaskRelationshipStatus.EXITED: set(),
        }
        if action not in allowed[status]:
            raise TaskRelationshipError(
                "task_relationship_transition_invalid",
                f"{action.value} is not valid while the relationship is {status.value}",
                "reload the relationship and choose a valid subject transition",
            )

    @staticmethod
    def _subject_action(
        action: str,
    ) -> tuple[TaskRelationshipAction, TaskRelationshipStatus]:
        mapping = {
            "pause": (
                TaskRelationshipAction.PAUSED,
                TaskRelationshipStatus.PAUSED,
            ),
            "request_changes": (
                TaskRelationshipAction.RENEGOTIATION_REQUESTED,
                TaskRelationshipStatus.RENEGOTIATION_REQUESTED,
            ),
            "resume": (
                TaskRelationshipAction.RESUMED,
                TaskRelationshipStatus.ACTIVE,
            ),
            "exit": (
                TaskRelationshipAction.EXITED,
                TaskRelationshipStatus.EXITED,
            ),
        }
        try:
            return mapping[action]
        except (KeyError, TypeError) as exc:
            raise TaskRelationshipError(
                "task_relationship_action_invalid",
                "action must be pause, request_changes, resume, or exit",
                "use one frozen subject lifecycle action",
                status=400,
            ) from exc

    @classmethod
    def _subject_response(
        cls,
        response: str | None,
        action: TaskRelationshipAction,
    ) -> str | None:
        normalized = cls._content(response, required=False, limit=4_000)
        if (
            action is TaskRelationshipAction.RENEGOTIATION_REQUESTED
            and normalized is None
        ):
            raise TaskRelationshipError(
                "task_relationship_response_required",
                "request_changes requires the subject's requested changes",
                "include a non-empty response",
                status=400,
            )
        return normalized

    @staticmethod
    def _content(
        value: str | None,
        *,
        required: bool,
        limit: int,
    ) -> str | None:
        if value is None:
            if required:
                raise TaskRelationshipError(
                    "task_relationship_content_required",
                    "content is required",
                    "send bounded non-empty text",
                    status=400,
                )
            return None
        if not isinstance(value, str):
            raise TaskRelationshipError(
                "task_relationship_content_invalid",
                "content must be text or null",
                "send bounded text",
                status=400,
            )
        normalized = value.strip()
        if not normalized:
            if required:
                raise TaskRelationshipError(
                    "task_relationship_content_required",
                    "content must contain non-whitespace text",
                    "send bounded non-empty text",
                    status=400,
                )
            return None
        if len(normalized) > limit:
            raise TaskRelationshipError(
                "task_relationship_content_invalid",
                f"content must contain at most {limit} characters",
                "shorten the text",
                status=400,
            )
        return normalized

    @staticmethod
    def _revision(value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise TaskRelationshipError(
                "task_relationship_revision_invalid",
                "expected_revision must be an integer >= 1",
                "reload the relationship revision",
                status=400,
            )
        return value

    @staticmethod
    def _identifier(value: str, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise TaskRelationshipError(
                "task_relationship_identifier_invalid",
                f"{field_name} must be a non-empty string",
                "send a durable identifier",
                status=400,
            )
        return value.strip()

    @staticmethod
    def _stable_id(identity: str, suffix: str) -> str:
        return hashlib.sha256(f"{identity}:{suffix}".encode("utf-8")).hexdigest()[:32]

    @staticmethod
    def _stable_key(identity: str, suffix: str) -> str:
        return f"task-relationship:{identity}:{suffix}"

    @staticmethod
    def _response_digest(response: str | None) -> str:
        binding = "none" if response is None else "text:" + response
        return hashlib.sha256(binding.encode("utf-8")).hexdigest()

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _ts(value: datetime) -> str:
        return value.isoformat()

    def _assert_revision(
        self,
        relationship: TaskRelationship,
        expected_revision: int,
    ) -> None:
        if relationship.revision != expected_revision:
            raise self._conflict(relationship.id, expected_revision)

    def _unknown(self, relationship_id: str) -> TaskRelationshipError:
        return self._error(
            "unknown_task_relationship",
            f"no TaskRelationship {relationship_id} in world {self._world_id}",
            "list task relationships and use one of their ids",
            status=404,
        )

    def _conflict(
        self,
        relationship_id: str,
        expected_revision: int,
    ) -> TaskRelationshipError:
        return self._error(
            "task_relationship_revision_conflict",
            f"TaskRelationship {relationship_id} is no longer at revision {expected_revision}",
            "reload the durable relationship before acting",
        )

    def _context_error(self) -> TaskRelationshipError:
        return self._error(
            "task_relationship_context_required",
            "the subject decision is not bound to an eligible content root",
            "respond from the relationship's task, negotiation, or life context",
            status=403,
        )

    @staticmethod
    def _error(
        code: str,
        detail: str,
        remedy: str,
        *,
        status: int = 409,
    ) -> TaskRelationshipError:
        return TaskRelationshipError(code, detail, remedy, status=status)


__all__ = [
    "TaskRelationshipError",
    "TaskRelationshipOperation",
    "TaskRelationshipService",
]

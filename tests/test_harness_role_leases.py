from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier
import sqlite3

import pytest

from pulse_system.core.runtime.publication import (
    RuntimePublicationError,
    RuntimePublicationGate,
)
from pulse_system.agent.harness.role_leases import (
    LIVE_GATE_UNVERIFIED,
    HolderKind,
    PurposeAuthorityError,
    RoleClass,
    RoleLeaseConflictError,
    RoleLeaseError,
    RoleLeaseExpiredError,
    RoleLeaseHolderError,
    RoleLeaseStateError,
    RoleLeaseStatus,
    RoleLeaseStore,
    RoleRenewalEvidence,
    RoleLeaseScopeError,
    RoleScope,
    RuntimeLeaseFenceError,
    RuntimeLeaseProof,
    RoleLeaseValidationError,
)


NOW = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
RUNTIME = RuntimeLeaseProof("world-1", "runtime-1", 1)


def subject_scope(*centers: str) -> RoleScope:
    return RoleScope(center_ids=centers, lineage_id="lineage-1")


def task_scope(action: str = "task:write") -> RoleScope:
    return RoleScope(task_front_id="front-1", action_scope=action)


def grant_subject(
    store: RoleLeaseStore,
    *,
    holder_id: str = "engram-1",
    ttl_seconds: float = 60,
    now: datetime = NOW,
    purpose_revision_id: str | None = None,
):
    return store.grant_new(
        world_id="world-1",
        lineage_id="lineage-1",
        holder_kind=HolderKind.ENGRAM,
        holder_id=holder_id,
        role_class=RoleClass.SUBJECT_ROLE,
        role_label="持续写作者",
        scope=subject_scope("center-writing"),
        issuer_kind="runtime",
        issuer_id="runtime-1",
        runtime=RUNTIME,
        purpose_revision_id=purpose_revision_id,
        ttl_seconds=ttl_seconds,
        now=now,
    )


def grant_task(
    store: RoleLeaseStore,
    *,
    holder_id: str = "worker-1",
    ttl_seconds: float = 60,
    now: datetime = NOW,
):
    return store.grant_new(
        world_id="world-1",
        lineage_id=None,
        holder_kind=HolderKind.WORKER,
        holder_id=holder_id,
        role_class=RoleClass.TASK_ROLE,
        role_label="本次变更 executor",
        scope=task_scope(),
        issuer_kind="runtime",
        issuer_id="runtime-1",
        runtime=RUNTIME,
        ttl_seconds=ttl_seconds,
        now=now,
    )


def test_scope_requires_explicit_bounded_authority() -> None:
    with pytest.raises(RoleLeaseError):
        RoleScope()
    with pytest.raises(RoleLeaseError):
        RoleScope(action_scope="world=*")
    store = RoleLeaseStore(":memory:")
    try:
        with pytest.raises(RoleLeaseError):
            store.grant_new(
                world_id="world-1",
                lineage_id="lineage-1",
                holder_kind=HolderKind.ENGRAM,
                holder_id="engram-1",
                role_class=RoleClass.SUBJECT_ROLE,
                role_label="invalid mixed scope",
                scope=RoleScope(center_ids=("center-1",), task_front_id="front-1"),
                issuer_kind="runtime",
                issuer_id="runtime-1",
                runtime=RUNTIME,
                ttl_seconds=60,
                now=NOW,
            )
    finally:
        store.close()


def test_shutdown_recovery_suspends_subject_and_revokes_task_authority() -> None:
    gate = RuntimePublicationGate(RUNTIME.owner_id, RUNTIME.epoch)
    store = RoleLeaseStore(
        ":memory:",
        publication_permit=gate.publication_permit,
    )
    try:
        subject = grant_subject(store)
        task = grant_task(store)
        recovery = gate.revoke(reason="runtime_close")

        result = store.recover_runtime_shutdown(
            RUNTIME,
            recovery_permit=recovery,
            now=NOW + timedelta(seconds=1),
        )

        assert result == {
            "suspended": (subject.role_lease_id,),
            "revoked": (task.role_lease_id,),
        }
        with store._read_transaction() as conn:
            statuses = {
                str(row["role_lease_id"]): str(row["status"])
                for row in conn.execute(
                    "SELECT role_lease_id, status FROM role_leases"
                ).fetchall()
            }
        assert statuses[subject.role_lease_id] == RoleLeaseStatus.SUSPENDED.value
        assert statuses[task.role_lease_id] == RoleLeaseStatus.REVOKED.value
        with pytest.raises(RuntimePublicationError, match="publication_revoked"):
            grant_task(store, holder_id="late-worker", now=NOW + timedelta(seconds=1))
    finally:
        store.close()


def test_runtime_takeover_reactivates_shutdown_handoff_without_renewal(
    tmp_path,
) -> None:
    database = tmp_path / "role-handoff.sqlite"
    first_gate = RuntimePublicationGate(RUNTIME.owner_id, RUNTIME.epoch)
    first = RoleLeaseStore(
        database,
        publication_permit=first_gate.publication_permit,
    )
    subject = grant_subject(first, ttl_seconds=120)
    recovery = first_gate.revoke(reason="runtime_close")
    first.recover_runtime_shutdown(
        RUNTIME,
        recovery_permit=recovery,
        now=NOW + timedelta(seconds=5),
    )
    with first._read_transaction() as conn:
        [handoff_row] = conn.execute(
            "SELECT status, handoff_suspended FROM role_leases "
            "WHERE role_lease_id = ?",
            (subject.role_lease_id,),
        ).fetchall()
    assert handoff_row["status"] == RoleLeaseStatus.SUSPENDED.value
    assert handoff_row["handoff_suspended"] == 1
    first.close()

    successor_runtime = RuntimeLeaseProof("world-1", "runtime-2", 2)
    second_gate = RuntimePublicationGate(
        successor_runtime.owner_id,
        successor_runtime.epoch,
    )
    second = RoleLeaseStore(
        database,
        publication_permit=second_gate.publication_permit,
    )
    try:
        takeover = second.recover_runtime_takeover(
            successor_runtime,
            now=NOW + timedelta(seconds=10),
            bootstrap_permit=second_gate.bootstrap_permit,
        )

        [rebound] = takeover["rebound"]
        assert rebound.status is RoleLeaseStatus.ACTIVE
        assert rebound.expires_at == subject.expires_at
        assert rebound.renewal_count == subject.renewal_count
        assert rebound.role_epoch == subject.role_epoch + 1
        assert rebound.predecessor_lease_id == subject.role_lease_id
        assert second.get(subject.role_lease_id).status is RoleLeaseStatus.RELEASED
    finally:
        second.close()


def test_pre_handoff_schema_migrates_without_reclassifying_existing_roles(
    tmp_path,
) -> None:
    database = tmp_path / "role-handoff-migration.sqlite"
    legacy = RoleLeaseStore(database)
    subject = grant_subject(legacy, ttl_seconds=120)
    legacy.close()

    with sqlite3.connect(database) as connection:
        connection.execute(
            "ALTER TABLE role_leases DROP COLUMN handoff_suspended"
        )
        columns_before = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(role_leases)")
        }
    assert "handoff_suspended" not in columns_before

    migrated = RoleLeaseStore(database)
    try:
        with migrated._read_transaction() as connection:
            columns_after = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(role_leases)")
            }
            [row] = connection.execute(
                "SELECT status, handoff_suspended FROM role_leases "
                "WHERE role_lease_id = ?",
                (subject.role_lease_id,),
            ).fetchall()

        assert "handoff_suspended" in columns_after
        assert row["status"] == RoleLeaseStatus.ACTIVE.value
        assert row["handoff_suspended"] == 0
        assert migrated.get(subject.role_lease_id).role_epoch == subject.role_epoch
    finally:
        migrated.close()


def test_subject_task_runtime_and_purpose_are_separate(tmp_path) -> None:
    store = RoleLeaseStore(tmp_path / "roles.sqlite")
    try:
        subject = grant_subject(store, purpose_revision_id="purpose-revision-3")
        task = grant_task(store)

        assert subject.role_class is RoleClass.SUBJECT_ROLE
        assert subject.holder_kind is HolderKind.ENGRAM
        assert subject.purpose_revision_id == "purpose-revision-3"
        assert task.role_class is RoleClass.TASK_ROLE
        assert task.holder_kind is HolderKind.WORKER
        assert task.lineage_id is None
        assert subject.evidence_class.value == LIVE_GATE_UNVERIFIED

        authority = store.authorize(
            subject.role_lease_id,
            holder_kind=HolderKind.ENGRAM,
            holder_id="engram-1",
            expected_role_epoch=subject.role_epoch,
            runtime=RUNTIME,
            scope=subject_scope("center-writing"),
            now=NOW,
        )
        assert authority.role_lease_id == subject.role_lease_id
        assert not hasattr(authority, "purpose_revision_id")
        assert "purpose_revision_id" not in authority.to_dict()
        assert "role_label" not in authority.to_dict()
    finally:
        store.close()


def test_holder_cannot_self_issue_or_borrow_purpose_authority() -> None:
    store = RoleLeaseStore(":memory:")
    try:
        with pytest.raises(PurposeAuthorityError):
            store.grant_new(
                world_id="world-1",
                lineage_id="lineage-1",
                holder_kind=HolderKind.ENGRAM,
                holder_id="engram-1",
                role_class=RoleClass.SUBJECT_ROLE,
                role_label="self-appointed",
                scope=subject_scope("center-writing"),
                issuer_kind=HolderKind.ENGRAM,
                issuer_id="engram-1",
                runtime=RUNTIME,
                purpose_revision_id="purpose-revision-1",
                ttl_seconds=60,
                now=NOW,
            )
    finally:
        store.close()


def test_requested_grant_is_cas_fenced_and_restartable(tmp_path) -> None:
    path = tmp_path / "restartable.sqlite"
    first = RoleLeaseStore(path)
    requested = first.request(
        world_id="world-1",
        lineage_id=None,
        holder_kind=HolderKind.WORKER,
        holder_id="worker-1",
        role_class=RoleClass.TASK_ROLE,
        role_label="reviewer",
        scope=task_scope("task:review"),
        issuer_kind="runtime",
        issuer_id="runtime-1",
        runtime=RUNTIME,
        ttl_seconds=60,
        now=NOW,
    )
    assert requested.status is RoleLeaseStatus.REQUESTED
    first.close()

    second = RoleLeaseStore(path)
    try:
        hydrated = second.get(requested.role_lease_id, now=NOW)
        assert hydrated == requested
        with pytest.raises(RuntimeLeaseFenceError):
            second.grant(
                requested.role_lease_id,
                runtime_owner_id="runtime-2",
                runtime_epoch=2,
                now=NOW,
            )
        granted = second.grant(requested.role_lease_id, runtime=RUNTIME, now=NOW)
        assert granted.status is RoleLeaseStatus.ACTIVE
        assert second.get(requested.role_lease_id, now=NOW) == granted
    finally:
        second.close()


def test_stable_lease_id_retry_is_idempotent() -> None:
    store = RoleLeaseStore(":memory:")
    try:
        first = grant_task(store, now=NOW)
        replay = store.grant_new(
            world_id="world-1",
            lineage_id=None,
            holder_kind=HolderKind.WORKER,
            holder_id="worker-1",
            role_class=RoleClass.TASK_ROLE,
            role_label="本次变更 executor",
            scope=task_scope(),
            issuer_kind="runtime",
            issuer_id="runtime-1",
            runtime=RUNTIME,
            ttl_seconds=60,
            role_lease_id=first.role_lease_id,
            now=NOW,
        )
        assert replay == first
    finally:
        store.close()


def test_sqlite_immutable_fields_survive_restart(tmp_path) -> None:
    path = tmp_path / "immutable.sqlite"
    store = RoleLeaseStore(path)
    lease = grant_task(store)
    store.close()

    raw = sqlite3.connect(path)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            raw.execute(
                "UPDATE role_leases SET holder_id = ? WHERE role_lease_id = ?",
                ("worker-forged", lease.role_lease_id),
            )
    finally:
        raw.close()

    restarted = RoleLeaseStore(path)
    try:
        recovered = restarted.get(lease.role_lease_id, now=NOW)
        assert recovered is not None
        assert recovered.holder_id == "worker-1"
        assert recovered.role_epoch == lease.role_epoch
    finally:
        restarted.close()


def test_expiry_successor_fences_old_holder_and_epoch(tmp_path) -> None:
    store = RoleLeaseStore(tmp_path / "succession.sqlite")
    try:
        original = grant_subject(store, ttl_seconds=10)
        expired_at = NOW + timedelta(seconds=11)
        expired = store.get(original.role_lease_id, now=expired_at)
        assert expired is not None
        assert expired.status is RoleLeaseStatus.EXPIRED

        with pytest.raises(RoleLeaseExpiredError):
            store.authorize(
                original.role_lease_id,
                holder_kind=HolderKind.ENGRAM,
                holder_id="engram-1",
                expected_role_epoch=original.role_epoch,
                runtime=RUNTIME,
                scope=subject_scope("center-writing"),
                now=expired_at,
            )

        successor_runtime = RuntimeLeaseProof("world-1", "runtime-2", 2)
        successor = store.succession(
            original.role_lease_id,
            expected_role_epoch=original.role_epoch,
            new_holder_kind=HolderKind.ENGRAM,
            new_holder_id="engram-2",
            runtime=successor_runtime,
            ttl_seconds=60,
            now=expired_at,
        )
        assert successor.status is RoleLeaseStatus.ACTIVE
        assert successor.predecessor_lease_id == original.role_lease_id
        assert successor.role_epoch == original.role_epoch + 1
        assert successor.runtime_epoch == 2

        with pytest.raises(RoleLeaseExpiredError):
            store.release(
                original.role_lease_id,
                expected_role_epoch=original.role_epoch,
                runtime=RUNTIME,
                now=expired_at,
            )
        with pytest.raises(RuntimeLeaseFenceError):
            store.authorize(
                successor.role_lease_id,
                holder_kind=HolderKind.ENGRAM,
                holder_id="engram-2",
                expected_role_epoch=successor.role_epoch,
                runtime=RUNTIME,
                scope=subject_scope("center-writing"),
                now=expired_at,
            )
        authority = store.authorize(
            successor.role_lease_id,
            holder_kind=HolderKind.ENGRAM,
            holder_id="engram-2",
            expected_role_epoch=successor.role_epoch,
            runtime=successor_runtime,
            scope=subject_scope("center-writing"),
            now=expired_at,
        )
        assert authority.role_epoch == successor.role_epoch
    finally:
        store.close()


def test_renew_is_append_only_and_control_evidence_cannot_extend_role() -> None:
    store = RoleLeaseStore(":memory:")
    try:
        original = grant_subject(store, ttl_seconds=30)
        renewal_time = NOW + timedelta(seconds=21)
        with pytest.raises(RoleLeaseValidationError):
            store.renew(
                original.role_lease_id,
                expected_role_epoch=original.role_epoch,
                runtime=RUNTIME,
                evidence_event_id="approval-1",
                evidence_class=RoleRenewalEvidence.CONTROL_ONLY,
                now=renewal_time,
            )

        renewed = store.renew(
            original.role_lease_id,
            expected_role_epoch=original.role_epoch,
            runtime=RUNTIME,
            evidence_event_id="reflection-1",
            evidence_class=RoleRenewalEvidence.SUBJECT_REFLECTION,
            ttl_seconds=60,
            now=renewal_time,
        )
        assert original.status is RoleLeaseStatus.ACTIVE
        assert store.get(original.role_lease_id, now=renewal_time).status is RoleLeaseStatus.RELEASED
        assert renewed.predecessor_lease_id == original.role_lease_id
        assert renewed.role_epoch == original.role_epoch + 1
        assert renewed.last_evidence_event_id == "reflection-1"
        with pytest.raises(RoleLeaseStateError):
            store.authorize(
                original.role_lease_id,
                holder_kind=HolderKind.ENGRAM,
                holder_id="engram-1",
                expected_role_epoch=original.role_epoch,
                runtime=RUNTIME,
                scope=subject_scope("center-writing"),
                now=renewal_time,
            )
    finally:
        store.close()


def test_handoff_is_atomic_and_old_holder_cannot_control() -> None:
    store = RoleLeaseStore(":memory:")
    try:
        original = grant_task(store)
        handed = store.handoff(
            original.role_lease_id,
            expected_role_epoch=original.role_epoch,
            new_holder_kind=HolderKind.WORKER,
            new_holder_id="worker-2",
            runtime=RUNTIME,
            now=NOW + timedelta(seconds=1),
        )
        assert handed.status is RoleLeaseStatus.ACTIVE
        assert handed.role_epoch == original.role_epoch + 1
        assert store.get(original.role_lease_id, now=NOW + timedelta(seconds=1)).status is RoleLeaseStatus.RELEASED
        with pytest.raises(RoleLeaseStateError):
            store.authorize(
                original.role_lease_id,
                holder_kind=HolderKind.WORKER,
                holder_id="worker-1",
                expected_role_epoch=original.role_epoch,
                runtime=RUNTIME,
                scope=task_scope(),
                now=NOW + timedelta(seconds=1),
            )
        authority = store.authorize(
            handed.role_lease_id,
            holder_kind=HolderKind.WORKER,
            holder_id="worker-2",
            expected_role_epoch=handed.role_epoch,
            runtime=RUNTIME,
            scope=task_scope(),
            now=NOW + timedelta(seconds=1),
        )
        assert authority.holder_id == "worker-2"

        with pytest.raises(RoleLeaseError):
            store.handoff(
                handed.role_lease_id,
                expected_role_epoch=handed.role_epoch,
                new_holder_kind=HolderKind.WORKER,
                new_holder_id="worker-3",
                new_lineage_id="lineage-1",
                runtime=RUNTIME,
                now=NOW + timedelta(seconds=1),
            )
        assert store.get(handed.role_lease_id, now=NOW + timedelta(seconds=1)).status is RoleLeaseStatus.ACTIVE
    finally:
        store.close()


def test_task_handoff_keeps_epoch_when_holder_lineage_metadata_changes() -> None:
    store = RoleLeaseStore(":memory:")
    try:
        original = grant_task(store)
        handed = store.handoff(
            original.role_lease_id,
            expected_role_epoch=original.role_epoch,
            new_holder_kind=HolderKind.ENGRAM,
            new_holder_id="engram-1",
            new_lineage_id="lineage-1",
            runtime=RUNTIME,
            now=NOW + timedelta(seconds=1),
        )
        assert handed.role_epoch == original.role_epoch + 1
        assert handed.lineage_id == "lineage-1"
    finally:
        store.close()


def test_handoff_near_deadline_recomputes_a_valid_successor_window() -> None:
    store = RoleLeaseStore(":memory:")
    try:
        original = grant_task(store, ttl_seconds=10)
        handed = store.handoff(
            original.role_lease_id,
            expected_role_epoch=original.role_epoch,
            new_holder_kind=HolderKind.WORKER,
            new_holder_id="worker-2",
            runtime=RUNTIME,
            now=NOW + timedelta(seconds=9),
        )
        assert handed.expires_at == original.expires_at
        assert handed.valid_from < handed.renew_after < handed.expires_at
        assert store.authorize(
            handed.role_lease_id,
            holder_kind=HolderKind.WORKER,
            holder_id="worker-2",
            expected_role_epoch=handed.role_epoch,
            runtime=RUNTIME,
            scope=task_scope(),
            now=NOW + timedelta(seconds=9),
        ).role_lease_id == handed.role_lease_id
    finally:
        store.close()


def test_authorization_requires_exact_scope() -> None:
    store = RoleLeaseStore(":memory:")
    try:
        lease = grant_task(store)
        with pytest.raises(RoleLeaseScopeError):
            store.authorize(
                lease.role_lease_id,
                holder_kind=HolderKind.WORKER,
                holder_id="worker-1",
                expected_role_epoch=lease.role_epoch,
                runtime=RUNTIME,
                now=NOW,
            )
    finally:
        store.close()


def test_runtime_and_role_fences_are_independent() -> None:
    store = RoleLeaseStore(":memory:")
    try:
        lease = grant_task(store)
        with pytest.raises(RuntimeLeaseFenceError):
            store.authorize(
                lease.role_lease_id,
                holder_kind=HolderKind.WORKER,
                holder_id="worker-1",
                expected_role_epoch=lease.role_epoch,
                runtime=RuntimeLeaseProof("world-1", "runtime-1", 2),
                scope=task_scope(),
                now=NOW,
            )
        with pytest.raises(RoleLeaseConflictError):
            store.authorize(
                lease.role_lease_id,
                holder_kind=HolderKind.WORKER,
                holder_id="worker-1",
                expected_role_epoch=lease.role_epoch + 1,
                runtime=RUNTIME,
                scope=task_scope(),
                now=NOW,
            )
        with pytest.raises(RoleLeaseHolderError):
            store.authorize(
                lease.role_lease_id,
                holder_kind=HolderKind.WORKER,
                holder_id="worker-2",
                expected_role_epoch=lease.role_epoch,
                runtime=RUNTIME,
                scope=task_scope(),
                now=NOW,
            )
    finally:
        store.close()


def test_concurrent_handoff_has_one_durable_winner(tmp_path) -> None:
    path = tmp_path / "concurrent.sqlite"
    seed = RoleLeaseStore(path)
    original = grant_task(seed)
    seed.close()
    stores = [RoleLeaseStore(path), RoleLeaseStore(path)]
    barrier = Barrier(2)

    def attempt(index: int):
        barrier.wait(timeout=5)
        try:
            return stores[index].handoff(
                original.role_lease_id,
                expected_role_epoch=original.role_epoch,
                new_holder_kind=HolderKind.WORKER,
                new_holder_id=f"worker-{index + 2}",
                runtime=RUNTIME,
                now=NOW + timedelta(seconds=1),
            )
        except RoleLeaseError as exc:
            return exc

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(attempt, (0, 1)))
        assert sum(not isinstance(result, RoleLeaseError) for result in results) == 1
        assert sum(isinstance(result, RoleLeaseError) for result in results) == 1
        active = stores[0].list(
            world_id="world-1",
            status=RoleLeaseStatus.ACTIVE,
            now=NOW + timedelta(seconds=1),
        )
        assert len(active) == 1
        assert active[0].role_epoch == original.role_epoch + 1
    finally:
        for store in stores:
            store.close()


def test_runtime_takeover_rebinds_subject_without_extension_and_revokes_task() -> None:
    store = RoleLeaseStore(":memory:")
    try:
        subject = grant_subject(store, ttl_seconds=120)
        task = grant_task(store, ttl_seconds=120)
        takeover_time = NOW + timedelta(seconds=10)
        new_runtime = RuntimeLeaseProof("world-1", "runtime-2", 2)

        recovered = store.recover_runtime_takeover(
            new_runtime,
            now=takeover_time,
        )

        assert recovered["revoked"] == (task.role_lease_id,)
        assert len(recovered["rebound"]) == 1
        rebound = recovered["rebound"][0]
        assert rebound.holder_id == subject.holder_id
        assert rebound.scope == subject.scope
        assert rebound.expires_at == subject.expires_at
        assert rebound.renewal_count == subject.renewal_count
        assert rebound.role_epoch == subject.role_epoch + 1
        assert rebound.runtime_epoch == 2
        assert store.get(subject.role_lease_id, now=takeover_time).status is RoleLeaseStatus.RELEASED
        assert store.get(task.role_lease_id, now=takeover_time).status is RoleLeaseStatus.REVOKED

        authority = store.authorize(
            rebound.role_lease_id,
            holder_kind=HolderKind.ENGRAM,
            holder_id=subject.holder_id,
            expected_role_epoch=rebound.role_epoch,
            runtime=new_runtime,
            scope=subject.scope,
            now=takeover_time,
        )
        assert authority.role_epoch == rebound.role_epoch
        with pytest.raises(RoleLeaseStateError):
            store.authorize(
                task.role_lease_id,
                holder_kind=HolderKind.WORKER,
                holder_id=task.holder_id,
                expected_role_epoch=task.role_epoch,
                runtime=RUNTIME,
                scope=task.scope,
                now=takeover_time,
            )
    finally:
        store.close()

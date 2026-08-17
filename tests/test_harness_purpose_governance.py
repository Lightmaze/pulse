from __future__ import annotations

import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from pulse_system.agent.harness.purpose_governance import (
    CONTRACT_ONLY,
    LIVE,
    LIVE_GATE_UNVERIFIED,
    PurposeAmendmentKind,
    PurposeEvidenceClass,
    PurposeGovernance,
    PurposeGovernanceError,
    PurposeLineageConflictError,
    PurposeRevisionCollisionError,
    PurposeRevisionConflictError,
    PurposeRevisionState,
    PurposeValidationError,
)


def _create(governance: PurposeGovernance):
    return governance.create_lineage(
        "lineage-1",
        world_id="world-1",
        root_engram_id="engram-1",
    )


def _establish(governance: PurposeGovernance, *, revision_id: str = "purpose-1"):
    return governance.amend_purpose(
        "lineage-1",
        purpose_revision_id=revision_id,
        author_engram_id="engram-1",
        expected_revision=None,
        content="把真实的生活方向保留下来，持续观察并行动。",
        amendment_kind=PurposeAmendmentKind.ESTABLISH,
        source_event_id="turn-1",
        reflection_event_id="reflection-1",
    )


def test_module_is_contract_only_and_has_no_payload_or_sensitive_columns(tmp_path):
    db_path = tmp_path / "purpose-contract.sqlite"
    governance = PurposeGovernance(db_path)
    try:
        assert governance.evidence_class is PurposeEvidenceClass.CONTRACT_ONLY
        assert governance.evidence_class.value == CONTRACT_ONLY
        assert {LIVE_GATE_UNVERIFIED, LIVE} == {
            PurposeEvidenceClass.LIVE_GATE_UNVERIFIED.value,
            PurposeEvidenceClass.LIVE.value,
        }
        _create(governance)
        revision = _establish(governance)
        columns = {
            str(row[1]).lower()
            for table in ("subject_lineages", "purpose_revisions")
            for row in governance._connection.execute(f"PRAGMA table_info({table})")
        }
        assert "prompt" not in columns
        assert "secret" not in columns
        assert "metadata" not in columns
        assert revision.to_dict()["evidence_class"] == CONTRACT_ONLY
        assert "secret" not in revision.to_dict()
    finally:
        governance.close()


def test_append_only_amendment_has_cas_lineage_and_digest(tmp_path):
    governance = PurposeGovernance(tmp_path / "purpose.sqlite")
    try:
        lineage = _create(governance)
        first = _establish(governance)
        second = governance.amend_purpose(
            "lineage-1",
            purpose_revision_id="purpose-2",
            author_engram_id="engram-1",
            expected_revision=1,
            content="在真实后果中修订方向，而不是把管理状态当成生活。",
            amendment_kind="amend",
            source_event_id="turn-2",
        )
        assert lineage.current_purpose_revision_id is None
        assert first.revision == 1
        superseded = governance.require_revision(first.purpose_revision_id)
        assert superseded.state is PurposeRevisionState.SUPERSEDED
        assert first.content == "把真实的生活方向保留下来，持续观察并行动。"
        assert first.predecessor_revision_id is None
        assert first.content_digest == hashlib.sha256(
            first.content.encode("utf-8")
        ).hexdigest()
        assert second.revision == 2
        assert second.predecessor_revision_id == first.purpose_revision_id
        assert second.state is PurposeRevisionState.CURRENT
        assert governance.current_revision("lineage-1") == second
        assert governance.require_lineage("lineage-1").current_purpose_revision_id == (
            second.purpose_revision_id
        )
        history = governance.list_revisions("lineage-1")
        assert [item.purpose_revision_id for item in history] == [
            "purpose-1",
            "purpose-2",
        ]
    finally:
        governance.close()


def test_withdraw_keeps_history_and_can_reestablish_on_same_lineage(tmp_path):
    db_path = tmp_path / "withdraw.sqlite"
    with PurposeGovernance(db_path) as governance:
        _create(governance)
        _establish(governance)
        withdrawn = governance.amend_purpose(
            "lineage-1",
            purpose_revision_id="purpose-withdraw",
            author_engram_id="engram-1",
            expected_revision=1,
            content=None,
            amendment_kind="withdraw",
            source_event_id="turn-withdraw",
        )
        assert withdrawn.revision == 2
        assert withdrawn.state is PurposeRevisionState.WITHDRAWN
        assert withdrawn.content is None
        assert governance.current_revision("lineage-1") is None

        reestablished = governance.amend_purpose(
            "lineage-1",
            purpose_revision_id="purpose-3",
            author_engram_id="engram-1",
            expected_revision=None,
            content="重新形成方向，但不抹去已经走过的谱系。",
            amendment_kind="establish",
            source_event_id="turn-reestablish",
        )
        assert reestablished.revision == 3
        assert reestablished.predecessor_revision_id == withdrawn.purpose_revision_id
        assert governance.require_lineage("lineage-1").current_purpose_revision_id == (
            reestablished.purpose_revision_id
        )
        assert [r.state for r in governance.list_revisions("lineage-1")] == [
            PurposeRevisionState.SUPERSEDED,
            PurposeRevisionState.WITHDRAWN,
            PurposeRevisionState.CURRENT,
        ]


def test_stable_revision_retry_is_idempotent_but_collision_fails_closed(tmp_path):
    governance = PurposeGovernance(tmp_path / "retry.sqlite")
    try:
        _create(governance)
        first = _establish(governance)
        replay = _establish(governance)
        assert replay == first
        with pytest.raises(PurposeRevisionCollisionError):
            governance.amend_purpose(
                "lineage-1",
                purpose_revision_id="purpose-1",
                author_engram_id="engram-1",
                expected_revision=None,
                content="不同的不可变内容。",
                amendment_kind="establish",
                source_event_id="turn-other",
            )
        assert len(governance.list_revisions("lineage-1")) == 1
    finally:
        governance.close()


def test_concurrent_amendment_has_one_cas_winner(tmp_path):
    db_path = tmp_path / "concurrent.sqlite"
    with PurposeGovernance(db_path) as seed:
        _create(seed)
        _establish(seed)

    barrier = Barrier(2)

    def amend(index: int):
        try:
            with PurposeGovernance(db_path) as governance:
                barrier.wait(timeout=5)
                return governance.amend_purpose(
                    "lineage-1",
                    purpose_revision_id=f"purpose-concurrent-{index}",
                    author_engram_id="engram-1",
                    expected_revision=1,
                    content=f"并发候选 {index}，只有一个可以成为下一版方向。",
                    amendment_kind="amend",
                    source_event_id=f"turn-concurrent-{index}",
                )
        except BaseException as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(amend, (0, 1)))

    successes = [result for result in results if not isinstance(result, BaseException)]
    failures = [result for result in results if isinstance(result, BaseException)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], PurposeRevisionConflictError)
    assert len({result.revision for result in successes}) == 1
    with PurposeGovernance(db_path) as recovered:
        assert len(recovered.list_revisions("lineage-1")) == 2
        assert recovered.current_revision("lineage-1").purpose_revision_id == (
            successes[0].purpose_revision_id
        )


def test_concurrent_amendment_exception_is_explicit():
    governance = PurposeGovernance(":memory:")
    try:
        _create(governance)
        _establish(governance)
        governance.amend_purpose(
            "lineage-1",
            purpose_revision_id="purpose-winner",
            author_engram_id="engram-1",
            expected_revision=1,
            content="winner",
            amendment_kind="amend",
            source_event_id="turn-winner",
        )
        with pytest.raises(PurposeRevisionConflictError) as caught:
            governance.amend_purpose(
                "lineage-1",
                purpose_revision_id="purpose-stale",
                author_engram_id="engram-1",
                expected_revision=1,
                content="stale",
                amendment_kind="amend",
                source_event_id="turn-stale",
            )
        assert caught.value.current_revision == 2
    finally:
        governance.close()


def test_lineage_succession_preserves_history_and_fences_old_author(tmp_path):
    db_path = tmp_path / "succession.sqlite"
    with PurposeGovernance(db_path) as governance:
        _create(governance)
        first = _establish(governance)
        successor = governance.record_succession(
            "lineage-1",
            successor_engram_id="engram-2",
            expected_current_engram_id="engram-1",
            expected_generation=0,
        )
        assert successor.lineage_id == first.lineage_id
        assert successor.root_engram_id == "engram-1"
        assert successor.current_engram_id == "engram-2"
        assert successor.generation == 1
        assert successor.current_purpose_revision_id == first.purpose_revision_id
        assert governance.require_revision(first.purpose_revision_id).author_engram_id == (
            "engram-1"
        )

        with pytest.raises(PurposeGovernanceError):
            governance.amend_purpose(
                "lineage-1",
                purpose_revision_id="purpose-old-author",
                author_engram_id="engram-1",
                expected_revision=1,
                content="旧代不能替新代写入方向。",
                amendment_kind="amend",
                source_event_id="turn-old-author",
            )
        second = governance.amend_purpose(
            "lineage-1",
            purpose_revision_id="purpose-successor",
            author_engram_id="engram-2",
            expected_revision=1,
            content="新一代继承谱系，但由当前主体自己继续修订。",
            amendment_kind="amend",
            source_event_id="turn-successor",
        )
        assert second.predecessor_revision_id == first.purpose_revision_id

    with PurposeGovernance(db_path) as recovered:
        lineage = recovered.require_lineage("lineage-1")
        assert lineage.current_engram_id == "engram-2"
        assert lineage.generation == 1
        assert recovered.current_revision("lineage-1").purpose_revision_id == (
            "purpose-successor"
        )


def test_succession_cas_rejects_second_writer(tmp_path):
    governance = PurposeGovernance(tmp_path / "succession-cas.sqlite")
    try:
        _create(governance)
        governance.succeed_lineage(
            "lineage-1",
            successor_engram_id="engram-2",
            expected_current_engram_id="engram-1",
            expected_generation=0,
        )
        with pytest.raises(PurposeLineageConflictError):
            governance.succeed_lineage(
                "lineage-1",
                successor_engram_id="engram-3",
                expected_current_engram_id="engram-1",
                expected_generation=0,
            )
    finally:
        governance.close()


def test_restart_revalidates_durable_history_and_sqlite_guards_append_only(tmp_path):
    db_path = tmp_path / "restart.sqlite"
    with PurposeGovernance(db_path) as governance:
        _create(governance)
        first = _establish(governance)
        assert governance.current_revision("lineage-1") == first

    with PurposeGovernance(db_path) as recovered:
        assert recovered.require_lineage("lineage-1").current_purpose_revision_id == (
            first.purpose_revision_id
        )
        assert recovered.require_revision(first.purpose_revision_id) == first
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            recovered._connection.execute(
                "UPDATE purpose_revisions SET content = 'tampered' WHERE purpose_revision_id = ?",
                (first.purpose_revision_id,),
            )
            recovered._connection.rollback()
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            recovered._connection.execute(
                "DELETE FROM purpose_revisions WHERE purpose_revision_id = ?",
                (first.purpose_revision_id,),
            )
            recovered._connection.rollback()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("lineage_id", "bad id"),
        ("world_id", ""),
        ("purpose_revision_id", "x" * 129),
        ("source_event_id", "event with space"),
    ],
)
def test_identifiers_are_bounded(field, value, tmp_path):
    governance = PurposeGovernance(tmp_path / f"invalid-{field}.sqlite")
    try:
        if field == "lineage_id":
            with pytest.raises(PurposeValidationError):
                governance.create_lineage(
                    value,
                    world_id="world-1",
                    root_engram_id="engram-1",
                )
        elif field == "world_id":
            with pytest.raises(PurposeValidationError):
                governance.create_lineage(
                    "lineage-1",
                    world_id=value,
                    root_engram_id="engram-1",
                )
        else:
            _create(governance)
            with pytest.raises(PurposeValidationError):
                governance.amend_purpose(
                    "lineage-1",
                    purpose_revision_id=(
                        value if field == "purpose_revision_id" else "purpose-1"
                    ),
                    author_engram_id="engram-1",
                    expected_revision=None,
                    content="valid purpose",
                    amendment_kind="establish",
                    source_event_id=(
                        value if field == "source_event_id" else "turn-1"
                    ),
                )
    finally:
        governance.close()


def test_content_is_bounded_and_digest_is_canonical(tmp_path):
    governance = PurposeGovernance(tmp_path / "content.sqlite")
    try:
        _create(governance)
        with pytest.raises(PurposeValidationError):
            governance.amend_purpose(
                "lineage-1",
                purpose_revision_id="purpose-nul",
                author_engram_id="engram-1",
                expected_revision=None,
                content="bad\x00purpose",
                amendment_kind="establish",
                source_event_id="turn-1",
            )
        with pytest.raises(PurposeValidationError):
            governance.amend_purpose(
                "lineage-1",
                purpose_revision_id="purpose-long",
                author_engram_id="engram-1",
                expected_revision=None,
                content="x" * 4001,
                amendment_kind="establish",
                source_event_id="turn-1",
            )
        revision = governance.amend_purpose(
            "lineage-1",
            purpose_revision_id="purpose-canonical",
            author_engram_id="engram-1",
            expected_revision=None,
            content="  e\u0301  ",
            amendment_kind="establish",
            source_event_id="turn-1",
        )
        assert revision.content == "é"
        assert revision.content_digest == hashlib.sha256("é".encode("utf-8")).hexdigest()
        assert len(revision.content_digest) == 64
    finally:
        governance.close()

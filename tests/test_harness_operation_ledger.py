from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from pulse_system.agent.harness.events import (
    HarnessEventDraft,
    HarnessEventKind,
    HarnessEventPhase,
    HarnessEventSource,
    HarnessEventStatus,
    HarnessEventStore,
)
from pulse_system.agent.harness.operations import (
    HarnessOperationLedger,
    OperationCASMismatchError,
    OperationPhase,
    OperationRecoveryState,
    OperationScopeCollisionError,
    OperationTerminalState,
    OperationTransitionError,
    deterministic_terminal_event_id,
)
from pulse_system.substrate.storage import Storage


def _admit(ledger: HarnessOperationLedger, *, operation_id: str = "op-1", epoch: int = 1):
    return ledger.admit(
        "file.write",
        operation_id,
        world_id="world-1",
        engram_id="engram-1",
        turn_id="turn-1",
        requested_epoch=epoch,
        owner_id="owner-1",
        scope_digest="a" * 64,
        effect_key="file-effect-1",
    )


def test_memory_storage_uses_the_same_durable_ledger() -> None:
    storage = Storage(":memory:")
    try:
        ledger = HarnessOperationLedger(storage)
        operation = _admit(ledger)
        assert operation.phase is OperationPhase.ADMITTED
        assert ledger.get("file.write", "op-1") == operation
        assert ledger.list_recovery() == [operation]
    finally:
        storage.close()


def test_restart_hydrates_and_same_scope_retry_is_idempotent(tmp_path) -> None:
    db_path = tmp_path / "operations.sqlite"
    first_storage = Storage(db_path)
    try:
        first = HarnessOperationLedger(first_storage)
        created = _admit(first)
        replay = _admit(first)
        assert replay == created
    finally:
        first_storage.close()

    second_storage = Storage(db_path)
    try:
        second = HarnessOperationLedger(second_storage)
        hydrated = second.get("file.write", "op-1")
        assert hydrated == created
        assert second.list_recovery() == [created]
    finally:
        second_storage.close()


def test_scope_collision_fails_closed(tmp_path) -> None:
    storage = Storage(tmp_path / "collision.sqlite")
    try:
        ledger = HarnessOperationLedger(storage)
        _admit(ledger)
        with pytest.raises(OperationScopeCollisionError):
            ledger.admit(
                "file.write",
                "op-1",
                world_id="world-1",
                engram_id="engram-1",
                turn_id="turn-1",
                requested_epoch=2,
                owner_id="owner-1",
                scope_digest="b" * 64,
                effect_key="file-effect-2",
            )
        assert ledger.get("file.write", "op-1").requested_epoch == 1
    finally:
        storage.close()


def test_stale_epoch_cannot_cross_boundary(tmp_path) -> None:
    storage = Storage(tmp_path / "epoch.sqlite")
    try:
        ledger = HarnessOperationLedger(storage)
        _admit(ledger, epoch=2)
        with pytest.raises(OperationCASMismatchError):
            ledger.transition(
                "file.write",
                "op-1",
                phase=OperationPhase.STARTING,
                expected_epoch=1,
                owner_id="owner-1",
            )
        with pytest.raises(OperationCASMismatchError):
            ledger.mark_boundary(
                "file.write",
                "op-1",
                expected_epoch=1,
                owner_id="owner-1",
            )
        assert ledger.get("file.write", "op-1").phase is OperationPhase.ADMITTED
    finally:
        storage.close()


def test_failed_not_started_is_valid_before_boundary() -> None:
    storage = Storage(":memory:")
    try:
        ledger = HarnessOperationLedger(storage)
        _admit(ledger)
        ledger.transition(
            "file.write",
            "op-1",
            phase=OperationPhase.APPROVAL_PENDING,
            expected_epoch=1,
            owner_id="owner-1",
        )
        terminal = ledger.claim_terminal(
            "file.write",
            "op-1",
            expected_epoch=1,
            owner_id="owner-1",
            terminal_state=OperationTerminalState.FAILED_NOT_STARTED,
        )
        assert terminal.terminal_state is OperationTerminalState.FAILED_NOT_STARTED
        assert terminal.recovery_state is OperationRecoveryState.REQUIRED
    finally:
        storage.close()


def test_post_boundary_failure_is_forced_to_uncertain() -> None:
    storage = Storage(":memory:")
    try:
        ledger = HarnessOperationLedger(storage)
        _admit(ledger)
        ledger.mark_boundary(
            "file.write", "op-1", expected_epoch=1, owner_id="owner-1"
        )
        with pytest.raises(OperationTransitionError):
            ledger.claim_terminal(
                "file.write",
                "op-1",
                expected_epoch=1,
                owner_id="owner-1",
                terminal_state=OperationTerminalState.FAILED_NOT_STARTED,
            )
        terminal = ledger.claim_terminal(
            "file.write",
            "op-1",
            expected_epoch=1,
            owner_id="owner-1",
            terminal_state=OperationTerminalState.UNCERTAIN,
        )
        assert terminal.terminal_state is OperationTerminalState.UNCERTAIN
    finally:
        storage.close()


def test_concurrent_terminal_claimers_have_one_durable_winner(tmp_path) -> None:
    db_path = tmp_path / "claim.sqlite"
    seed = Storage(db_path)
    seeded = HarnessOperationLedger(seed)
    _admit(seeded)
    seeded.mark_boundary(
        "file.write", "op-1", expected_epoch=1, owner_id="owner-1"
    )
    seed.close()

    storages = [Storage(db_path), Storage(db_path)]
    barrier = Barrier(2)

    def claim(index: int):
        ledger = HarnessOperationLedger(storages[index])
        barrier.wait(timeout=5)
        return ledger.claim_terminal(
            "file.write",
            "op-1",
            expected_epoch=1,
            owner_id="owner-1",
            terminal_state=(
                OperationTerminalState.COMPLETED
                if index == 0
                else OperationTerminalState.UNCERTAIN
            ),
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(claim, (0, 1)))
        assert len({result.terminal_state for result in results}) == 1
        assert len({result.updated_at for result in results}) == 1
        assert results[0].terminal_state in {
            OperationTerminalState.COMPLETED,
            OperationTerminalState.UNCERTAIN,
        }
    finally:
        for storage in storages:
            storage.close()


def test_late_completed_result_cannot_overwrite_uncertain() -> None:
    storage = Storage(":memory:")
    try:
        ledger = HarnessOperationLedger(storage)
        _admit(ledger)
        ledger.mark_boundary(
            "file.write", "op-1", expected_epoch=1, owner_id="owner-1"
        )
        uncertain = ledger.claim_terminal(
            "file.write",
            "op-1",
            expected_epoch=1,
            owner_id="owner-1",
            terminal_state=OperationTerminalState.UNCERTAIN,
        )
        late = ledger.claim_terminal(
            "file.write",
            "op-1",
            expected_epoch=1,
            owner_id="owner-1",
            terminal_state=OperationTerminalState.COMPLETED,
        )
        assert late == uncertain
        assert ledger.get("file.write", "op-1").terminal_state is OperationTerminalState.UNCERTAIN
    finally:
        storage.close()


def test_terminal_event_binding_clears_recovery_required(tmp_path) -> None:
    storage = Storage(tmp_path / "binding.sqlite")
    try:
        ledger = HarnessOperationLedger(storage)
        _admit(ledger)
        ledger.mark_boundary(
            "file.write", "op-1", expected_epoch=1, owner_id="owner-1"
        )
        terminal = ledger.claim_terminal(
            "file.write",
            "op-1",
            expected_epoch=1,
            owner_id="owner-1",
            terminal_state=OperationTerminalState.COMPLETED,
        )
        assert terminal.recovery_state is OperationRecoveryState.REQUIRED
        assert ledger.list_recovery() == [terminal]

        bound = ledger.bind_terminal_event(
            "file.write",
            "op-1",
            terminal_event_id="harness-event-1",
            expected_epoch=1,
            owner_id="owner-1",
        )
        assert bound.terminal_event_id == "harness-event-1"
        assert bound.recovery_state is OperationRecoveryState.CLEARED
        assert ledger.list_recovery() == []
        assert ledger.bind_terminal_event(
            "file.write", "op-1", terminal_event_id="harness-event-1"
        ) == bound
    finally:
        storage.close()


def test_newer_runtime_recovers_pre_boundary_as_cancelled(tmp_path) -> None:
    storage = Storage(tmp_path / "recover-pre.sqlite")
    try:
        ledger = HarnessOperationLedger(storage)
        _admit(ledger, epoch=3)
        recovered = ledger.claim_recovery(
            "file.write",
            "op-1",
            successor_owner_id="owner-2",
            successor_epoch=4,
            expected_prior_owner_id="owner-1",
            expected_prior_epoch=3,
        )
        assert recovered.terminal_state is OperationTerminalState.CANCELLED_NOT_STARTED
        assert recovered.recovery_owner_id == "owner-2"
        assert recovered.recovery_epoch == 4
        assert recovered.recovery_state is OperationRecoveryState.REQUIRED
    finally:
        storage.close()


def test_newer_runtime_recovers_post_boundary_as_uncertain(tmp_path) -> None:
    storage = Storage(tmp_path / "recover-post.sqlite")
    try:
        ledger = HarnessOperationLedger(storage)
        _admit(ledger, epoch=3)
        ledger.mark_boundary(
            "file.write", "op-1", expected_epoch=3, owner_id="owner-1"
        )
        recovered = ledger.claim_recovery(
            "file.write",
            "op-1",
            successor_owner_id="owner-2",
            successor_epoch=4,
            expected_prior_owner_id="owner-1",
            expected_prior_epoch=3,
        )
        assert recovered.terminal_state is OperationTerminalState.UNCERTAIN
        with pytest.raises(OperationCASMismatchError):
            ledger.claim_recovery(
                "file.write",
                "another-op",
                successor_owner_id="owner-1",
                successor_epoch=3,
                expected_prior_owner_id="owner-1",
                expected_prior_epoch=3,
            )
    finally:
        storage.close()


def test_list_for_turn_is_bounded_and_scope_specific() -> None:
    storage = Storage(":memory:")
    try:
        ledger = HarnessOperationLedger(storage)
        _admit(ledger, operation_id="op-1")
        assert ledger.list_for_turn("turn-1") == [ledger.get("file.write", "op-1")]
        assert ledger.list_for_turn("turn-missing") == []
    finally:
        storage.close()


def test_terminal_projection_and_e0_winner_commit_in_one_transaction(tmp_path) -> None:
    storage = Storage(tmp_path / "atomic-terminal.sqlite")
    events = HarnessEventStore(storage)
    ledger = HarnessOperationLedger(storage)
    try:
        operation = _admit(ledger)
        ledger.mark_boundary(
            operation.operation_kind,
            operation.operation_id,
            expected_epoch=1,
            owner_id="owner-1",
        )
        event_id = deterministic_terminal_event_id(
            operation.operation_kind,
            operation.operation_id,
        )
        draft = HarnessEventDraft(
            turn_id="turn-1",
            world_id="world-1",
            engram_id="engram-1",
            kind=HarnessEventKind.TOOL_COMPLETED,
            phase=HarnessEventPhase.TERMINAL,
            source=HarnessEventSource.POLICY,
            status=HarnessEventStatus.COMPLETED,
            payload={"action_request_id": "op-1", "terminal": True},
            event_id=event_id,
        )

        with pytest.raises(OperationCASMismatchError):
            events.append_terminal_operation(
                draft,
                ledger=ledger,
                operation_kind=operation.operation_kind,
                operation_id=operation.operation_id,
                expected_epoch=1,
                owner_id="stale-owner",
                terminal_state=OperationTerminalState.COMPLETED,
            )
        assert events.get(event_id) is None
        assert ledger.get(operation.operation_kind, operation.operation_id).is_terminal is False

        event, winner = events.append_terminal_operation(
            draft,
            ledger=ledger,
            operation_kind=operation.operation_kind,
            operation_id=operation.operation_id,
            expected_epoch=1,
            owner_id="owner-1",
            terminal_state=OperationTerminalState.COMPLETED,
        )
        assert event.event_id == event_id
        assert winner.terminal_event_id == event_id
        assert winner.recovery_state is OperationRecoveryState.CLEARED
        retry_event, retry_winner = events.append_terminal_operation(
            draft,
            ledger=ledger,
            operation_kind=operation.operation_kind,
            operation_id=operation.operation_id,
            expected_epoch=1,
            owner_id="owner-1",
            terminal_state=OperationTerminalState.COMPLETED,
        )
        assert retry_event.event_id == event_id
        assert retry_winner == winner
        assert len(
            [
                item
                for item in events.replay("turn-1", limit=20).events
                if item.event_id == event_id
            ]
        ) == 1
    finally:
        storage.close()

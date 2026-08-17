"""Contract tests for the isolated temporary TaskSubagent protocol."""

from datetime import datetime, timedelta, timezone

import pytest

from pulse_system.agent.harness.task_worker_protocol import (
    ContractOnlyTaskWorkerBackend,
    TaskSubagentActivity,
    TaskSubagentParentContext,
    TaskSubagentSpec,
    TaskSubagentState,
    TaskWorkerCloseSummary,
    TaskWorkerControlResult,
    TaskWorkerEvidence,
    TaskWorkerProcessTreeState,
    TaskWorkerStartResult,
    UnavailableTaskWorkerBackend,
    value_digest,
)


def test_parent_context_and_spec_are_scoped_and_bounded() -> None:
    context = TaskSubagentParentContext(
        world_id="world-1",
        engram_id="engram-1",
        turn_id="turn-1",
        epoch=3,
        capabilities={"task:stop", "read:workspace"},
    )
    spec = TaskSubagentSpec(
        task="inspect the changed files",
        capabilities={"read:workspace"},
        timeout_sec=2,
        idle_timeout_sec=1,
        deadline_at=datetime.now(timezone.utc) + timedelta(seconds=5),
    )

    assert context.capabilities == frozenset({"task:stop", "read:workspace"})
    assert spec.capabilities == frozenset({"read:workspace"})
    assert spec.deadline_at is not None and spec.deadline_at.tzinfo is not None
    assert value_digest(spec.task) != spec.task

    with pytest.raises(ValueError, match="epoch"):
        TaskSubagentParentContext("world", "engram", "turn", -1)
    with pytest.raises(ValueError, match="task must"):
        TaskSubagentSpec(" ")
    with pytest.raises(ValueError, match="timeout_sec"):
        TaskSubagentSpec("task", timeout_sec=0)


def test_activity_wire_shape_does_not_change_worker_identity() -> None:
    activity = TaskSubagentActivity(
        task_id="task_1",
        seq=7,
        kind="worker_completed",
        state=TaskSubagentState.COMPLETED,
        occurred_at=datetime.now(timezone.utc),
        summary="worker completed",
        payload={"result_digest": "abcd"},
        evidence_class=TaskWorkerEvidence.CONTRACT_ONLY,
    )

    wire = activity.to_dict()
    assert wire["kind"] == "subagent_activity"
    assert wire["activity_kind"] == "worker_completed"
    assert wire["state"] == "COMPLETED"
    assert wire["evidence_class"] == "CONTRACT_ONLY"
    assert "prompt" not in wire
    assert "engram_session_id" not in wire


def test_unavailable_backend_is_explicitly_contract_only() -> None:
    backend = UnavailableTaskWorkerBackend()
    context = TaskSubagentParentContext("world", "engram", "turn", 0)
    result = backend.start(
        "task_1",
        TaskSubagentSpec("no hidden mock"),
        context,
        lambda *args, **kwargs: None,
    )

    assert result.state is TaskSubagentState.ERRORED
    assert result.error_code == "worker_backend_unavailable"
    assert result.evidence_class is TaskWorkerEvidence.CONTRACT_ONLY


def test_contract_backend_only_emits_explicit_lifecycle_events() -> None:
    backend = ContractOnlyTaskWorkerBackend()
    emitted: list[tuple[str, dict]] = []

    def emit(kind, summary="", payload=None, **kwargs):
        del summary, kwargs
        emitted.append((kind, dict(payload or {})))

    context = TaskSubagentParentContext("world", "engram", "turn", 0)
    result = backend.start("task_1", TaskSubagentSpec("contract fixture"), context, emit)
    assert result.state is TaskSubagentState.RUNNING
    assert result.evidence_class is TaskWorkerEvidence.CONTRACT_ONLY

    backend.complete("task_1", summary="explicit fixture completion")
    assert result.backend_handle is not None
    assert emitted == [
        ("worker_completed", {"result_digest": value_digest("explicit fixture completion")})
    ]

    control = TaskWorkerControlResult(
        accepted=True,
        terminal=True,
        state=TaskSubagentState.INTERRUPTED,
        detail="barrier observed",
    )
    assert control.terminal is True
    assert control.state is TaskSubagentState.INTERRUPTED


def test_close_summary_is_typed_and_mapping_compatible() -> None:
    summary = TaskWorkerCloseSummary(
        active_before=3,
        unresolved=2,
        owner_joined=False,
        process_tree_state=TaskWorkerProcessTreeState.UNKNOWN,
        cancellation_requested=3,
        terminal_observed=1,
        spawn_operations_settled_uncertain=3,
    )

    assert summary.active_before == 3
    assert summary["workers_observed"] == 3
    assert summary["owner_joined"] is False
    assert summary["process_tree_state"] == "unknown"
    with pytest.raises(ValueError, match="joined owners"):
        TaskWorkerCloseSummary(
            active_before=1,
            unresolved=1,
            owner_joined=True,
            process_tree_state=TaskWorkerProcessTreeState.UNKNOWN,
        )

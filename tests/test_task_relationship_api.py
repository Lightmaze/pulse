"""HTTP and Runtime contracts for subject-owned accepted task relationships."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pulse_system.agent.tools.gateway import ToolInvocationContext
from pulse_system.core.causality import CausalTransitionError
from pulse_system.core.types import (
    ActivityCenterStatus,
    CausalEventKind,
    CausalEventStatus,
    EngramStatus,
)
from pulse_system.interaction.api.routes_write import create_write_router
from pulse_system.service.runtime import (
    RuntimeService,
    RuntimeServiceConfig,
    ServiceError,
)
from pulse_system.service.task_relationships import TaskRelationshipError


def _runtime(tmp_path) -> RuntimeService:
    return RuntimeService(RuntimeServiceConfig(
        db_path=tmp_path / "task-relationship.sqlite",
        metrics_path=tmp_path / "task-relationship.metrics.jsonl",
        workspace=tmp_path,
        mock=True,
        silence_threshold=0.0,
        default_max_wait=0.0,
        base_spontaneous_rate=0.0,
    ))


def _app(runtime: RuntimeService) -> FastAPI:
    app = FastAPI()
    app.state.runtime = runtime
    app.include_router(create_write_router())
    return app


def _accept(
    client: TestClient,
    runtime: RuntimeService,
) -> tuple[str, str, str, str]:
    subject_id = runtime.continuity_engram_id
    offered = client.post("/task-offers", json={
        "subject_engram_id": subject_id,
        "content": "Produce one bounded map while preserving the rest of life.",
        "title": "Bounded map",
    })
    assert offered.status_code == 201, offered.text
    offer = offered.json()
    turn = runtime.causal_ledger.begin_turn(
        offer["event_id"],
        subject_id,
        "consider bounded work",
    )
    accepted = runtime.life_tools.dispatch(
        subject_id,
        "pulse_task_offer_respond",
        {
            "decision": "accept",
            "expected_revision": 1,
            "response": "I accept this bounded scope.",
        },
        ToolInvocationContext("task-relationship-accept"),
    )
    assert accepted["ok"] is True
    runtime.causal_ledger.settle_turn(turn.id, "I accepted the bounded task.")
    front_id = accepted["data"]["task_front_id"]
    snapshot = runtime.task_relationships.get_for_front(front_id)
    assert snapshot is not None
    return (
        subject_id,
        front_id,
        snapshot.relationship.id,
        accepted["data"]["task_event_id"],
    )


def test_terms_pause_work_without_user_force_resume(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    try:
        with TestClient(_app(runtime)) as client:
            _subject_id, front_id, relationship_id, _task_event_id = _accept(
                client,
                runtime,
            )
            initial = client.get(f"/task-fronts/{front_id}")
            assert initial.status_code == 200
            initial_body = initial.json()
            assert initial_body["task_relationship_mode"] == "subject_consent_managed"
            assert initial_body["task_relationship"]["status"] == "active"
            assert initial_body["task_relationship"]["revision"] == 1
            assert initial_body["relationship_events"][0]["action"] == "accepted"

            proposed = client.post(
                f"/task-relationships/{relationship_id}/terms",
                json={
                    "expected_revision": 1,
                    "content": "Please include a second deliverable.",
                },
            )
            assert proposed.status_code == 202, proposed.text
            body = proposed.json()
            assert body["task_relationship"]["status"] == (
                "renegotiation_requested"
            )
            assert body["task_relationship"]["revision"] == 2
            assert body["relationship_events"][-1]["action"] == "terms_proposed"
            assert body["relationship_events"][-1]["actor_kind"] == "user"
            root = runtime.causal_ledger.get_event(body["event_id"])
            assert root is not None
            assert root.center_id is None
            assert root.metadata["task_relationship_id"] == relationship_id
            assert root.metadata["task_relationship_revision"] == 2

            relationship = runtime.task_relationships.get(relationship_id).relationship
            center = runtime.world.get_activity_center(relationship.center_id)
            assert center is not None
            assert center.status is ActivityCenterStatus.PAUSED
            with runtime.storage._lock:
                [task_root_row] = runtime.storage._conn.execute(
                    "SELECT id FROM causal_events WHERE center_id = ? "
                    "AND status = 'queued' ORDER BY seq",
                    (relationship.center_id,),
                ).fetchall()
            runtime.engine.tick()
            assert runtime.causal_ledger.get_event(task_root_row[0]).status is (
                CausalEventStatus.QUEUED
            )
            schedule_row = next(
                row
                for row in runtime.scheduling_snapshot()["centers"]
                if row["center_id"] == relationship.center_id
            )
            assert schedule_row["decision"] == "blocked"
            assert schedule_row["reason"] == "center_inactive"

            refused_message = client.post(
                f"/task-fronts/{front_id}/messages",
                json={"content": "Continue anyway."},
            )
            assert refused_message.status_code == 409
            assert refused_message.json()["error"] == "task_relationship_not_active"

            refused_override = client.patch(
                f"/activity-centers/{relationship.center_id}",
                json={"status": "active"},
            )
            assert refused_override.status_code == 409
            assert refused_override.json()["error"] == (
                "task_relationship_controls_center_status"
            )
            assert client.post(
                f"/task-relationships/{relationship_id}/resume",
                json={"expected_revision": 2},
            ).status_code == 404

            duplicate = client.post(
                f"/task-relationships/{relationship_id}/terms",
                json={
                    "expected_revision": 1,
                    "content": "Please include a second deliverable.",
                },
            )
            assert duplicate.status_code == 202
            assert duplicate.json()["duplicate"] is True
            assert duplicate.json()["event_id"] == body["event_id"]
            assert duplicate.json()["task_relationship"]["revision"] == 2

            collision = client.post(
                f"/task-relationships/{relationship_id}/terms",
                json={
                    "expected_revision": 1,
                    "content": "A conflicting replay.",
                },
            )
            assert collision.status_code == 409
            assert collision.json()["error"] == (
                "task_relationship_effect_collision"
            )
    finally:
        runtime.close()


def test_direct_front_is_explicitly_unmanaged_and_relationship_survives_succession(
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        with TestClient(_app(runtime)) as client:
            direct = client.post(
                "/task-fronts",
                json={"content": "Legacy administrator-created task."},
            )
            assert direct.status_code == 201
            direct_id = direct.json()["task_front"]["id"]
            direct_detail = client.get(f"/task-fronts/{direct_id}").json()
            assert direct_detail["task_relationship_mode"] == (
                "unmanaged_compatibility"
            )
            assert direct_detail["task_relationship"] is None
            assert direct_detail["relationship_events"] == []

            subject_id, front_id, relationship_id, _task_event_id = _accept(
                client,
                runtime,
            )
            successor = runtime.engrams.succession(subject_id)
            assert runtime.storage.get_engram(subject_id).status is EngramStatus.ARCHIVED
            migrated = runtime.task_relationships.get(relationship_id)
            assert migrated.relationship.original_subject_engram_id == subject_id
            assert migrated.relationship.current_subject_engram_id == successor.new_id
            assert migrated.relationship.revision == 2
            assert migrated.events[-1].action.value == "succession"
            assert runtime.world.get_task_front(front_id).focal_engram_id == successor.new_id
    finally:
        runtime.close()


def test_restart_reconciles_only_missing_accepted_relationships(tmp_path) -> None:
    first = _runtime(tmp_path)
    relationship_id = ""
    front_id = ""
    try:
        with TestClient(_app(first)) as client:
            _subject_id, front_id, relationship_id, _task_event_id = _accept(
                client,
                first,
            )
            direct = client.post(
                "/task-fronts",
                json={"content": "Compatibility task without an accepted offer."},
            )
            assert direct.status_code == 201
            direct_id = direct.json()["task_front"]["id"]
    finally:
        first.close()

    database = tmp_path / "task-relationship.sqlite"
    with sqlite3.connect(database) as conn:
        conn.execute(
            "DELETE FROM task_relationship_events WHERE relationship_id = ?",
            (relationship_id,),
        )
        conn.execute(
            "DELETE FROM task_relationships WHERE id = ?",
            (relationship_id,),
        )
        conn.commit()

    second = _runtime(tmp_path)
    try:
        restored = second.task_relationships.get(relationship_id)
        assert restored.relationship.task_front_id == front_id
        assert restored.relationship.status.value == "active"
        assert restored.events[0].action.value == "accepted"
        assert second.task_relationships.get_for_front(direct_id) is None
    finally:
        second.close()


def test_subject_pause_renegotiate_resume_and_exit_vertical_loop(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    try:
        with TestClient(_app(runtime)) as client:
            subject_id, front_id, relationship_id, task_event_id = _accept(
                client,
                runtime,
            )
            task_turn = runtime.causal_ledger.begin_turn(
                task_event_id,
                subject_id,
                "begin only the accepted bounded work",
            )
            paused = runtime.life_tools.dispatch(
                subject_id,
                "pulse_task_relationship_respond",
                {
                    "relationship_id": relationship_id,
                    "expected_revision": 1,
                    "action": "pause",
                    "response": "I need to return to the rest of my life first.",
                },
                ToolInvocationContext("task-relationship-pause"),
            )
            assert paused["ok"] is True, paused
            assert paused["data"]["status"] == "paused"
            assert paused["data"]["task_relationship_revision"] == 2
            runtime.causal_ledger.settle_turn(
                task_turn.id,
                "I paused participation without ending my other life contexts.",
            )

            proposed = client.post(
                f"/task-relationships/{relationship_id}/terms",
                json={
                    "expected_revision": 2,
                    "content": "Keep the deliverable bounded and move the deadline.",
                },
            )
            assert proposed.status_code == 202, proposed.text
            negotiation = proposed.json()
            assert negotiation["task_relationship"]["revision"] == 3
            negotiation_turn = runtime.causal_ledger.begin_turn(
                negotiation["event_id"],
                subject_id,
                "consider changed terms without doing task work",
            )
            blocked_work = runtime.life_tools.dispatch(
                subject_id,
                "bash",
                {"command": "echo must-not-run"},
                ToolInvocationContext("task-relationship-negotiation-bash"),
            )
            assert blocked_work["ok"] is False
            assert blocked_work["error"] == "task_relationship_negotiation_only"
            resumed = runtime.life_tools.dispatch(
                subject_id,
                "pulse_task_relationship_respond",
                {
                    "relationship_id": relationship_id,
                    "expected_revision": 3,
                    "action": "resume",
                    "response": "The bounded revised terms fit now.",
                },
                ToolInvocationContext("task-relationship-resume"),
            )
            assert resumed["ok"] is True, resumed
            assert resumed["data"]["status"] == "active"
            assert resumed["data"]["task_relationship_revision"] == 4
            runtime.causal_ledger.settle_turn(
                negotiation_turn.id,
                "I voluntarily resumed under the revised scope.",
            )

            continued = client.post(
                f"/task-fronts/{front_id}/messages",
                json={"content": "Continue within the revised terms."},
            )
            assert continued.status_code == 202, continued.text
            continued_event_id = continued.json()["event_id"]
            continued_turn = runtime.causal_ledger.begin_turn(
                continued_event_id,
                subject_id,
                "continue within the revised relationship",
            )
            exited = runtime.life_tools.dispatch(
                subject_id,
                "pulse_task_relationship_respond",
                {
                    "relationship_id": relationship_id,
                    "expected_revision": 4,
                    "action": "exit",
                    "response": "I am ending this task relationship now.",
                },
                ToolInvocationContext("task-relationship-exit"),
            )
            assert exited["ok"] is True, exited
            assert exited["data"]["status"] == "exited"
            runtime.causal_ledger.settle_turn(
                continued_turn.id,
                "I exited the task relationship.",
            )

            final = runtime.task_relationships.get(relationship_id)
            assert final.relationship.revision == 5
            assert final.relationship.status.value == "exited"
            assert final.relationship.exited_at is not None
            assert final.events[-1].action.value == "exited"
            center = runtime.world.get_activity_center(
                final.relationship.center_id
            )
            assert center is not None
            assert center.status is ActivityCenterStatus.COMPLETED
            assert client.post(
                f"/task-relationships/{relationship_id}/terms",
                json={"expected_revision": 5, "content": "Please return."},
            ).json()["error"] == "task_relationship_exited"
            assert client.post(
                f"/task-fronts/{front_id}/messages",
                json={"content": "Continue anyway."},
            ).json()["error"] == "task_relationship_not_active"
    finally:
        runtime.close()


def test_subject_tool_retry_replays_committed_effect_after_later_terms(tmp_path) -> None:
    """A crash gap must replay the old effect, not the newer aggregate head."""

    runtime = _runtime(tmp_path)
    try:
        with TestClient(_app(runtime)) as client:
            subject_id, _front_id, relationship_id, task_event_id = _accept(
                client,
                runtime,
            )
            turn = runtime.causal_ledger.begin_turn(
                task_event_id,
                subject_id,
                "pause before another proposal arrives",
            )
            invocation = ToolInvocationContext("task-relationship-crash-gap")
            args = {
                "relationship_id": relationship_id,
                "expected_revision": 1,
                "action": "pause",
                "response": "I need a pause.",
            }
            first = runtime.life_tools.dispatch(
                subject_id,
                "pulse_task_relationship_respond",
                args,
                invocation,
            )
            assert first["ok"] is True, first
            [tool_call] = [
                event
                for event in runtime.causal_ledger.list_events(
                    causal_id=runtime.causal_ledger.get_event(
                        turn.event_id
                    ).causal_id,
                    kind=CausalEventKind.TOOL_CALL,
                    limit=100,
                )
                if event.kind is CausalEventKind.TOOL_CALL
                and event.metadata.get("tool_call_id") == invocation.tool_call_id
            ]
            [tool_result] = [
                event
                for event in runtime.causal_ledger.get_children(tool_call.id)
                if event.kind is CausalEventKind.TOOL_RESULT
            ]
            with runtime.storage._lock:
                runtime.storage._conn.execute(
                    "DELETE FROM causal_events WHERE id = ?",
                    (tool_result.id,),
                )
                runtime.storage._conn.commit()
            runtime.life_tools._tool_result_data.pop(tool_result.id, None)

            later = client.post(
                f"/task-relationships/{relationship_id}/terms",
                json={
                    "expected_revision": 2,
                    "content": "Keep the scope bounded after the pause.",
                },
            )
            assert later.status_code == 202, later.text
            assert later.json()["task_relationship"]["revision"] == 3

            replay = runtime.life_tools.dispatch(
                subject_id,
                "pulse_task_relationship_respond",
                args,
                invocation,
            )
            assert replay["ok"] is True, replay
            assert replay["data"]["task_relationship_revision"] == 2
            assert replay["data"]["status"] == "paused"
            assert runtime.task_relationships.get(
                relationship_id
            ).relationship.revision == 3
    finally:
        runtime.close()


def test_pause_revokes_the_same_task_turn_and_duplicate_is_not_a_bearer_token(
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        with TestClient(_app(runtime)) as client:
            subject_id, _front_id, relationship_id, task_event_id = _accept(
                client,
                runtime,
            )
            turn = runtime.causal_ledger.begin_turn(
                task_event_id,
                subject_id,
                "pause this task now",
            )
            pause = runtime.life_tools.dispatch(
                subject_id,
                "pulse_task_relationship_respond",
                {
                    "relationship_id": relationship_id,
                    "expected_revision": 1,
                    "action": "pause",
                    "response": "I am pausing this work.",
                },
                ToolInvocationContext("same-turn-pause"),
            )
            assert pause["ok"] is True, pause
            assert pause["data"]["execution_revocation"]["turn_id"] == turn.id

            for tool_name, args in (
                ("bash", {"command": "echo must-not-run"}),
                ("pulse_task_spawn", {"task": "continue the paused work"}),
                ("pulse_delegate", {"task": "continue elsewhere"}),
            ):
                blocked = runtime.life_tools.dispatch(
                    subject_id,
                    tool_name,
                    args,
                    ToolInvocationContext(f"blocked-after-pause-{tool_name}"),
                )
                assert blocked["ok"] is False
                assert blocked["error"] == "task_relationship_not_active"

            with pytest.raises(TaskRelationshipError) as stolen:
                runtime.task_relationships.respond(
                    relationship_id=relationship_id,
                    expected_revision=1,
                    subject_engram_id="intruder",
                    action="pause",
                    response="I am pausing this work.",
                    source_event_id=pause["data"]["source_event_id"],
                )
            assert stolen.value.code == "task_relationship_subject_mismatch"
    finally:
        runtime.close()


def test_changed_terms_revoke_an_already_running_task_turn(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    try:
        with TestClient(_app(runtime)) as client:
            subject_id, _front_id, relationship_id, task_event_id = _accept(
                client,
                runtime,
            )
            turn = runtime.causal_ledger.begin_turn(
                task_event_id,
                subject_id,
                "work before terms change",
            )
            proposed = client.post(
                f"/task-relationships/{relationship_id}/terms",
                json={
                    "expected_revision": 1,
                    "content": "Pause and reconsider this changed scope.",
                },
            )
            assert proposed.status_code == 202, proposed.text
            assert proposed.json()["execution_revocation"]["turn_id"] == turn.id
            blocked = runtime.life_tools.dispatch(
                subject_id,
                "bash",
                {"command": "echo must-not-continue"},
                ToolInvocationContext("blocked-after-new-terms"),
            )
            assert blocked["error"] == "task_relationship_not_active"
    finally:
        runtime.close()


def test_turn_claim_rechecks_relationship_after_scheduler_reservation_window(
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        with TestClient(_app(runtime)) as client:
            subject_id, _front_id, relationship_id, task_event_id = _accept(
                client,
                runtime,
            )
            # This queued task root may already have been selected/reserved by
            # the scheduler.  The relationship changes before the worker can
            # atomically claim it as a Harness turn.
            proposed = client.post(
                f"/task-relationships/{relationship_id}/terms",
                json={
                    "expected_revision": 1,
                    "content": "Change the terms before the reserved turn starts.",
                },
            )
            assert proposed.status_code == 202, proposed.text
            with pytest.raises(CausalTransitionError) as denied:
                runtime.causal_ledger.begin_turn(
                    task_event_id,
                    subject_id,
                    "must not start after consent changed",
                )
            assert "task_relationship_not_active" in str(denied.value)
            assert runtime.causal_ledger.get_event(
                task_event_id
            ).status is CausalEventStatus.QUEUED
            assert all(
                turn.event_id != task_event_id
                for turn in runtime.causal_ledger.list_turns()
            )
    finally:
        runtime.close()


def test_changed_terms_do_not_interrupt_an_unrelated_life_turn(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    try:
        with TestClient(_app(runtime)) as client:
            subject_id, _front_id, relationship_id, _task_event_id = _accept(
                client,
                runtime,
            )
            life = runtime.world.create_center_for_existing_engram(
                "hobby",
                "Unrelated music practice",
                subject_id,
            )
            life_event_id = runtime.inject(
                subject_id,
                "Continue listening to the phrase already in progress.",
                center_id=life.center.id,
            )
            life_turn = runtime.causal_ledger.begin_turn(
                life_event_id,
                subject_id,
                "continue unrelated life activity",
            )
            proposed = client.post(
                f"/task-relationships/{relationship_id}/terms",
                json={
                    "expected_revision": 1,
                    "content": "Please reconsider the task scope later.",
                },
            )
            assert proposed.status_code == 202, proposed.text
            revocation = proposed.json()["execution_revocation"]
            assert revocation == {
                "state": "unrelated_running_turn",
                "uncertain": False,
                "turn_id": life_turn.id,
                "target_center_id": runtime.task_relationships.get(
                    relationship_id
                ).relationship.center_id,
                "running_center_id": life.center.id,
            }
            observed = runtime.life_tools.dispatch(
                subject_id,
                "pulse_life_portfolio",
                {},
                ToolInvocationContext("unrelated-life-observation-after-terms"),
            )
            assert observed["ok"] is True
    finally:
        runtime.close()


def test_message_admission_rechecks_relationship_inside_enqueue_transaction(
    tmp_path,
    monkeypatch,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        with TestClient(_app(runtime)) as client:
            _subject_id, front_id, relationship_id, _task_event_id = _accept(
                client,
                runtime,
            )
            original_inject = runtime.inject

            def pause_between_precheck_and_enqueue(*args, **kwargs):
                runtime.task_relationships.propose_terms(
                    relationship_id=relationship_id,
                    expected_revision=1,
                    content="Pause before this raced message is admitted.",
                )
                return original_inject(*args, **kwargs)

            monkeypatch.setattr(runtime, "inject", pause_between_precheck_and_enqueue)
            with pytest.raises(ServiceError) as refused:
                runtime.send_task_front_message(
                    front_id,
                    "THIS_MESSAGE_MUST_NOT_CROSS_THE_PAUSE",
                )
            assert refused.value.error == "task_relationship_not_active"
            with runtime.storage._lock:
                count = runtime.storage._conn.execute(
                    "SELECT COUNT(*) FROM causal_events WHERE content = ?",
                    ("THIS_MESSAGE_MUST_NOT_CROSS_THE_PAUSE",),
                ).fetchone()[0]
            assert count == 0
    finally:
        runtime.close()


def test_restart_repairs_relationship_projection_drift(tmp_path) -> None:
    first = _runtime(tmp_path)
    with TestClient(_app(first)) as client:
        _subject_id, _front_id, relationship_id, _task_event_id = _accept(
            client,
            first,
        )
        center_id = first.task_relationships.get(
            relationship_id
        ).relationship.center_id
    first.close()

    conn = sqlite3.connect(tmp_path / "task-relationship.sqlite")
    try:
        conn.execute(
            "UPDATE activity_centers SET status = 'paused' WHERE id = ?",
            (center_id,),
        )
        conn.commit()
    finally:
        conn.close()

    second = _runtime(tmp_path)
    try:
        relationship = second.task_relationships.get(relationship_id).relationship
        center = second.world.get_activity_center(relationship.center_id)
        assert relationship.status.value == "active"
        assert center is not None
        assert center.status is ActivityCenterStatus.ACTIVE
    finally:
        second.close()


def test_restart_finishes_uncertain_subject_succession_before_reconciliation(
    tmp_path,
) -> None:
    first = _runtime(tmp_path)
    with TestClient(_app(first)) as client:
        predecessor_id, front_id, relationship_id, _task_event_id = _accept(
            client,
            first,
        )
    successor_id = "relationship-successor"
    first.storage.create_engram(engram_id=successor_id)
    now = datetime.now(timezone.utc).isoformat()
    with first.storage._lock:
        first.storage._conn.execute(
            "UPDATE engrams SET status = 'archived' WHERE id = ?",
            (predecessor_id,),
        )
        first.storage._conn.execute(
            "INSERT INTO generation_transitions ("
            "id, causal_id, event_id, predecessor_id, successor_id, state, "
            "summary_turn_id, error_code, created_at, updated_at, settled_at"
            ") VALUES (?, ?, NULL, ?, ?, 'uncertain', NULL, ?, ?, ?, ?)",
            (
                "generation-relationship-recovery",
                "causal-relationship-recovery",
                predecessor_id,
                successor_id,
                "listener_failed",
                now,
                now,
                now,
            ),
        )
        first.storage._conn.commit()
    first.close()

    second = _runtime(tmp_path)
    try:
        assert second.continuity_engram_id == successor_id
        relationship = second.task_relationships.get(relationship_id).relationship
        front = second.world.get_task_front(front_id)
        center = second.world.get_activity_center(relationship.center_id)
        assert relationship.current_subject_engram_id == successor_id
        assert relationship.revision == 2
        assert front is not None and front.focal_engram_id == successor_id
        assert center is not None and center.focal_engram_id == successor_id
        assert second.task_relationships.get(relationship_id).events[-1].action.value == (
            "succession"
        )
    finally:
        second.close()


def test_reconciliation_fails_closed_on_corrupt_task_bundle(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    try:
        with TestClient(_app(runtime)) as client:
            _subject_id, _front_id, relationship_id, _task_event_id = _accept(
                client,
                runtime,
            )
        center_id = runtime.task_relationships.get(
            relationship_id
        ).relationship.center_id
        with runtime.storage._lock:
            runtime.storage._conn.execute(
                "UPDATE activity_centers SET kind = 'hobby' WHERE id = ?",
                (center_id,),
            )
            runtime.storage._conn.commit()
        with pytest.raises(TaskRelationshipError) as inconsistent:
            runtime.task_relationships.reconcile_accepted_offers()
        assert inconsistent.value.code == "task_relationship_recovery_inconsistent"
    finally:
        runtime.close()


def test_reconciliation_fails_closed_on_incomplete_relationship_history(
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        with TestClient(_app(runtime)) as client:
            _subject_id, _front_id, relationship_id, _task_event_id = _accept(
                client,
                runtime,
            )
        with runtime.storage._lock:
            runtime.storage._conn.execute(
                "DELETE FROM task_relationship_events "
                "WHERE relationship_id = ? AND seq = 1",
                (relationship_id,),
            )
            runtime.storage._conn.commit()
        with pytest.raises(TaskRelationshipError) as inconsistent:
            runtime.task_relationships.reconcile_accepted_offers()
        assert inconsistent.value.code == "task_relationship_recovery_inconsistent"
    finally:
        runtime.close()


def test_reconciliation_rejects_unrelated_accepted_decision_event(
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        with TestClient(_app(runtime)) as client:
            _subject_id, front_id, relationship_id, task_event_id = _accept(
                client,
                runtime,
            )
        relationship = runtime.task_relationships.get(
            relationship_id
        ).relationship
        with runtime.storage._lock:
            runtime.storage._conn.execute(
                "DELETE FROM task_relationship_events WHERE relationship_id = ?",
                (relationship_id,),
            )
            runtime.storage._conn.execute(
                "DELETE FROM task_relationships WHERE id = ?",
                (relationship_id,),
            )
            runtime.storage._conn.execute(
                "UPDATE task_offer_revisions SET decision_event_id = ? "
                "WHERE offer_id = ? AND revision = 1",
                (task_event_id, relationship.accepted_offer_id),
            )
            runtime.storage._conn.commit()
        with pytest.raises(TaskRelationshipError) as inconsistent:
            runtime.task_relationships.reconcile_accepted_offers()
        assert inconsistent.value.code == "task_relationship_recovery_inconsistent"
        assert runtime.task_relationships.get_for_front(front_id) is None
    finally:
        runtime.close()

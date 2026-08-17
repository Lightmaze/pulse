"""HTTP contract tests for PulseWorld, TaskFronts, and life centers."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from pulse_system.interaction.api.app import create_app
from pulse_system.interaction.api.routes_write import create_write_router
from pulse_system.service import RuntimeService, RuntimeServiceConfig


def _config(tmp_path, **overrides) -> RuntimeServiceConfig:
    values = {
        "db_path": str(tmp_path / "world.db"),
        "metrics_path": str(tmp_path / "metrics.jsonl"),
        "workspace": tmp_path,
        "mock": True,
        "silence_threshold": 0.0,
        "default_max_wait": 0.0,
        "base_spontaneous_rate": 0.0,
    }
    values.update(overrides)
    return RuntimeServiceConfig(**values)


@pytest.fixture()
def world_client(tmp_path):
    service = RuntimeService(_config(tmp_path))
    app = FastAPI()
    app.state.runtime = service
    app.include_router(create_write_router())
    with TestClient(app) as client:
        client.service = service
        yield client
    service.close()


def test_world_read_exposes_one_identity_and_user_facing_collections(world_client):
    response = world_client.get("/world")
    assert response.status_code == 200
    body = response.json()
    assert body["world_id"] == world_client.service.world_id
    assert body["continuity_engram_id"] == (
        world_client.service.continuity_engram_id
    )
    assert body["task_fronts"] == []
    assert body["activity_centers"] == []
    assert body["world"]["task_fronts"] == 0
    assert body["world"]["life_centers"] == 0
    assert body["shutdown"]["protocol_version"] == "runtime-shutdown.v1"
    assert body["shutdown"]["phase"] == "open"
    assert body["shutdown"]["publication_fence"] == "active"


def test_shutdown_evidence_remains_readable_after_storage_closes(world_client):
    before = world_client.get("/runtime/shutdown")
    assert before.status_code == 200
    assert before.json()["phase"] == "open"

    report = world_client.service.close(timeout=0.5)
    after = world_client.get("/runtime/shutdown")

    assert after.status_code == 200
    assert after.json() == report.to_dict()
    assert after.json()["phase"] == "closed"
    assert after.json()["control_plane_closed"] is True
    assert after.json()["contract_satisfied"] is True


def test_task_front_create_read_message_patch_and_filter_contract(world_client):
    first = "Start a durable task relation"
    before = world_client.service.storage.list_delegations()
    response = world_client.post(
        "/task-fronts",
        json={"content": first, "title": "Durable task"},
    )
    assert response.status_code == 201
    created = response.json()
    assert set(created) == {"task_front", "activity_center", "event_id"}
    front = created["task_front"]
    center = created["activity_center"]
    assert front["center_id"] == center["id"]
    assert front["focal_engram_id"] == center["focal_engram_id"]
    assert world_client.service.storage.list_delegations() == before

    listed = world_client.get("/task-fronts?status=open")
    assert listed.status_code == 200
    assert [row["id"] for row in listed.json()["task_fronts"]] == [front["id"]]

    detail = world_client.get(f"/task-fronts/{front['id']}")
    assert detail.status_code == 200
    assert detail.json()["task_front"]["id"] == front["id"]
    assert detail.json()["messages"] == []

    sent = world_client.post(
        f"/task-fronts/{front['id']}/messages",
        json={"content": "Continue this task"},
    )
    assert sent.status_code == 202
    assert sent.json()["event_id"]

    # The durable consumer executes at most one root per Engram per tick; the
    # second accepted input remains queued rather than being coalesced into an
    # ambiguous multi-root turn.
    world_client.service.engine.tick()
    world_client.service.engine.tick()
    messages = world_client.get(f"/task-fronts/{front['id']}").json()["messages"]
    transcript = "\n".join(
        message["content"]
        for message in messages
        if message["role"] != "assistant"
    )
    # Both durable roots arrive exactly once and without reframing.
    assert transcript.count(first) == 1
    assert transcript.count("Continue this task") == 1

    closed = world_client.patch(
        f"/task-fronts/{front['id']}",
        json={"status": "closed", "title": "Closed view"},
    )
    assert closed.status_code == 200
    assert closed.json()["task_front"]["status"] == "closed"
    refused = world_client.post(
        f"/task-fronts/{front['id']}/messages",
        json={"content": "must reopen first"},
    )
    assert refused.status_code == 409
    assert set(refused.json()) == {"error", "detail", "remedy"}
    assert refused.json()["remedy"]


def test_non_task_center_can_be_created_without_stimulus_and_share_an_engram(
    world_client,
):
    response = world_client.post(
        "/activity-centers",
        json={
            "kind": "hobby",
            "title": "Field recording",
            "description": "Listening for its own sake",
            "origin": "self",
            "autonomy": 0.6,
        },
    )
    assert response.status_code == 201
    created = response.json()
    assert set(created) == {"activity_center", "focal_engram_id"}
    center = created["activity_center"]
    assert center["kind"] == "hobby"
    assert center["origin"] == "self"
    assert world_client.service.engrams.get_session(
        created["focal_engram_id"]
    ) == []

    peer = world_client.service.engrams.create(initial_messages=None)
    member = world_client.post(
        f"/activity-centers/{center['id']}/members",
        json={"engram_id": peer.id, "relation": "shared"},
    )
    assert member.status_code == 201
    assert member.json()["membership"]["engram_id"] == peer.id

    detail = world_client.get(f"/activity-centers/{center['id']}")
    assert detail.status_code == 200
    detail_body = detail.json()
    assert set(detail_body) == {
        "activity_center",
        "members",
        "living_concerns",
        "living_concerns_total",
        "living_concerns_truncated",
        "living_orientations",
        "living_orientations_total",
        "living_orientations_truncated",
        "activity_summary",
        "messages",
        "unattributed_history",
    }
    assert detail_body["living_concerns"] == []
    assert detail_body["living_concerns_total"] == 0
    assert detail_body["living_concerns_truncated"] is False
    assert detail_body["living_orientations"] == []
    assert detail_body["living_orientations_total"] == 0
    assert detail_body["living_orientations_truncated"] is False
    assert set(detail_body["activity_summary"]) == {
        "last_seq",
        "last_event_at",
        "queued",
        "running",
        "uncertain",
        "recent_source",
        "recent_kind",
    }
    assert detail_body["messages"] == []
    assert detail_body["unattributed_history"] == []
    assert {row["engram_id"] for row in detail_body["members"]} == {
        created["focal_engram_id"],
        peer.id,
    }
    filtered = world_client.get("/activity-centers?kind=hobby&status=active")
    assert [row["id"] for row in filtered.json()["activity_centers"]] == [
        center["id"]
    ]

    world_client.patch(
        f"/activity-centers/{center['id']}",
        json={"status": "dormant"},
    )
    stimulus = world_client.post(
        f"/activity-centers/{center['id']}/messages",
        json={"content": "Listen again"},
    )
    assert stimulus.status_code == 202
    assert world_client.get(
        f"/activity-centers/{center['id']}"
    ).json()["activity_center"]["status"] == "active"

    world_client.patch(
        f"/activity-centers/{center['id']}",
        json={"status": "paused"},
    )
    refused = world_client.post(
        f"/activity-centers/{center['id']}/messages",
        json={"content": "pause must hold"},
    )
    assert refused.status_code == 409
    assert refused.json()["error"] == "activity_center_not_writable"


def test_activity_center_detail_exposes_canonical_orientation_history_with_limit_and_privacy(
    tmp_path,
):
    service = RuntimeService(
        _config(tmp_path, living_orientation_history_limit=2)
    )
    app = FastAPI()
    app.state.runtime = service
    app.include_router(create_write_router())
    try:
        with TestClient(app) as client:
            created = client.post(
                "/activity-centers",
                json={
                    "kind": "hobby",
                    "title": "Field listening",
                    "stimulus": "Begin with a quiet listening practice.",
                },
            ).json()
            center_id = created["activity_center"]["id"]
            owner_id = created["focal_engram_id"]

            def event(event_id):
                value = service.causal_ledger.get_event(event_id)
                assert value is not None
                return value

            first_source = event(created["event_id"])
            first = service.world.create_living_orientation(
                center_id,
                owner_id,
                "在田野录音里保持耐心。",
                first_source.causal_id,
                first_source.id,
                orientation_id="orientation-first",
            )
            close_first_id = client.post(
                f"/activity-centers/{center_id}/messages",
                json={"content": "Close the first direction."},
            ).json()["event_id"]
            close_first_source = event(close_first_id)
            service.world.update_living_orientation(
                first.id,
                expected_owner_engram_id=owner_id,
                expected_revision=first.revision,
                content="这段方向已经安静地完成。",
                state="closed",
                causal_id=close_first_source.causal_id,
                source_event_id=close_first_source.id,
            )

            second_source_id = client.post(
                f"/activity-centers/{center_id}/messages",
                json={"content": "Open a second direction."},
            ).json()["event_id"]
            second_source = event(second_source_id)
            second = service.world.create_living_orientation(
                center_id,
                owner_id,
                "在声音之间继续辨认世界。",
                second_source.causal_id,
                second_source.id,
                orientation_id="orientation-second",
            )
            close_second_id = client.post(
                f"/activity-centers/{center_id}/messages",
                json={"content": "Close the second direction."},
            ).json()["event_id"]
            close_second_source = event(close_second_id)
            service.world.update_living_orientation(
                second.id,
                expected_owner_engram_id=owner_id,
                expected_revision=second.revision,
                content="这条方向也已收束。",
                state="closed",
                causal_id=close_second_source.causal_id,
                source_event_id=close_second_source.id,
            )

            current_source_id = client.post(
                f"/activity-centers/{center_id}/messages",
                json={"content": "Rest while staying receptive."},
            ).json()["event_id"]
            current_source = event(current_source_id)
            service.world.create_living_orientation(
                center_id,
                owner_id,
                "让休息也成为继续感知环境的生活。",
                current_source.causal_id,
                current_source.id,
                state="resting",
                orientation_id="orientation-current",
            )

            response = client.get(f"/activity-centers/{center_id}")
            assert response.status_code == 200
            body = response.json()
            assert body["living_orientations_total"] == 3
            assert body["living_orientations_truncated"] is True
            rows = body["living_orientations"]
            assert [row["id"] for row in rows] == [
                "orientation-current",
                "orientation-second",
            ]
            assert rows[0]["state"] == "resting"
            assert rows[0]["content"] == "让休息也成为继续感知环境的生活。"
            assert rows[1]["state"] == "closed"
            assert rows[1]["closed_at"] is not None
            assert set(rows[0]) == {
                "id",
                "center_id",
                "owner_engram_id",
                "content",
                "state",
                "revision",
                "engagement_count",
                "next_eligible_at",
                "last_engagement_event_id",
                "last_engaged_at",
                "created_at",
                "updated_at",
                "closed_at",
            }
            assert "prompt" not in rows[0]
            assert "session_path" not in rows[0]
    finally:
        service.close()


@pytest.mark.parametrize("blocked_status", ["paused", "completed", "archived"])
def test_center_state_blocks_task_front_messages_with_a_remedy(
    world_client,
    blocked_status,
):
    created = world_client.post(
        "/task-fronts",
        json={"content": "stateful task"},
    ).json()
    center_id = created["activity_center"]["id"]
    front_id = created["task_front"]["id"]

    changed = world_client.patch(
        f"/activity-centers/{center_id}",
        json={"status": blocked_status},
    )
    assert changed.status_code == 200
    refused = world_client.post(
        f"/task-fronts/{front_id}/messages",
        json={"content": "do not bypass lifecycle"},
    )
    assert refused.status_code == 409
    assert refused.json()["error"] == "activity_center_not_writable"
    assert refused.json()["remedy"]

    if blocked_status == "archived":
        reopen = world_client.patch(
            f"/activity-centers/{center_id}",
            json={"status": "active"},
        )
        assert reopen.status_code == 409
        assert reopen.json()["error"] == "archived_activity_center"


def test_invalid_world_requests_use_the_flat_refusal_contract(world_client):
    cases = [
        world_client.post("/task-fronts", json={"title": "missing content"}),
        world_client.post(
            "/activity-centers",
            json={"kind": "task", "title": "must use TaskFront"},
        ),
        world_client.post(
            "/activity-centers",
            json={"kind": "hobby", "title": "x", "autonomy": None},
        ),
        world_client.patch("/activity-centers/ghost", json={}),
        world_client.get("/task-fronts?status=imaginary"),
    ]
    assert [response.status_code for response in cases] == [400, 400, 400, 400, 400]
    for response in cases:
        assert set(response.json()) == {"error", "detail", "remedy"}
        assert response.json()["remedy"]


def test_cors_preflight_allows_patch_for_the_browser_workbench(tmp_path):
    service = RuntimeService(_config(tmp_path))
    app = create_app(
        tmp_path / "metrics.jsonl",
        db_path=tmp_path / "world.db",
        runtime=service,
    )
    try:
        with TestClient(app) as client:
            response = client.options(
                "/task-fronts/example",
                headers={
                    "Origin": "http://localhost:5173",
                    "Access-Control-Request-Method": "PATCH",
                },
            )
        assert response.status_code == 200
        assert "PATCH" in response.headers["access-control-allow-methods"]
    finally:
        service.close()


def test_world_route_without_runtime_refuses_instead_of_accepting_dead_work():
    app = FastAPI()
    app.include_router(create_write_router())
    with TestClient(app) as client:
        response = client.get("/world")
    assert response.status_code == 503
    assert response.json()["error"] == "no_runtime"
    assert "app.state.runtime" in response.json()["remedy"]

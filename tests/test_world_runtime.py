"""Cross-layer runtime tests for one PulseWorld with many life fronts."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pulse_system.core.types import (
    ActivityCenterStatus,
    ActivityKind,
    ActivityOrigin,
    Message,
    MessageRole,
    TaskFrontStatus,
)
from pulse_system.service import (
    IDENTITY_COMPONENT,
    WORLD_COMPONENT,
    RuntimeService,
    RuntimeServiceConfig,
    ServiceError,
)
from pulse_system.substrate.storage import Storage


def _config(tmp_path, db=None, **overrides) -> RuntimeServiceConfig:
    values = {
        "db_path": str(db if db is not None else tmp_path / "world.db"),
        "workspace": tmp_path,
        "mock": True,
        "silence_threshold": 0.0,
        "default_max_wait": 0.0,
        "base_spontaneous_rate": 0.0,
    }
    values.update(overrides)
    return RuntimeServiceConfig(**values)


def _messages(service: RuntimeService, engram_id: str) -> list[Message]:
    return service.engrams.get_session(engram_id)


def test_new_world_has_one_durable_identity_and_a_silent_compatibility_anchor(
    tmp_path,
):
    service = RuntimeService(_config(tmp_path))
    try:
        anchor = service.storage.get_engram(service.continuity_engram_id)
        state = service.storage.load_component_state(WORLD_COMPONENT)

        assert service.world_id
        assert service.front_engram_id == service.continuity_engram_id
        assert state == {
            "version": 1,
            "world_id": service.world_id,
            "continuity_engram_id": service.continuity_engram_id,
            "created_at": state["created_at"],
            "legacy_front_migrated": True,
        }
        assert anchor is not None
        assert anchor.metadata.self_excitability == 0.0
        assert _messages(service, anchor.id) == []
        assert service.list_task_fronts() == []
        assert service.list_activity_centers() == []
    finally:
        service.close()


def test_world_and_multiple_fronts_survive_restart_without_multiplying_runtime(
    tmp_path,
):
    db = tmp_path / "durable.db"
    first = RuntimeService(_config(tmp_path, db))
    try:
        world_id = first.world_id
        alpha = first.create_task_front("Build the observatory", title="Alpha")
        beta = first.create_task_front("Map the garden", title="Beta")
        first.engine.tick()

        alpha_front = alpha["task_front"]
        beta_front = beta["task_front"]
        assert alpha_front["focal_engram_id"] != beta_front["focal_engram_id"]
        assert first.engrams._harness is first.harness
        assert first.snapshot()["world"]["task_fronts"] == 2
    finally:
        first.close()

    second = RuntimeService(_config(tmp_path, db))
    try:
        assert second.world_id == world_id
        assert {front["id"] for front in second.list_task_fronts()} == {
            alpha_front["id"],
            beta_front["id"],
        }
        assert second.tick_count == 0
        assert second.snapshot()["world"]["task_fronts"] == 2
    finally:
        second.close()


def test_new_task_is_not_delegation_and_first_content_enters_exactly_once(tmp_path):
    service = RuntimeService(_config(tmp_path))
    text = "This exact first stimulus must travel only once."
    try:
        before = service.storage.list_delegations()
        created = service.create_task_front(text)
        focal = created["task_front"]["focal_engram_id"]

        assert service.storage.list_delegations() == before
        assert created["event_id"]
        assert _messages(service, focal) == []

        service.engine.tick()
        assert sum(message.content == text for message in _messages(service, focal)) == 1
        assert service.storage.list_delegations() == before
    finally:
        service.close()


def test_non_task_life_center_can_exist_quietly_and_modulate_spontaneous_life(
    tmp_path,
):
    service = RuntimeService(_config(tmp_path, base_spontaneous_rate=0.8))
    try:
        created = service.create_activity_center(
            ActivityKind.HOBBY,
            "Night photography",
            description="A life interest, not a work queue.",
            autonomy=0.25,
        )
        center_id = created["activity_center"]["id"]
        focal = created["focal_engram_id"]
        engram = service.storage.get_engram(focal)

        assert "event_id" not in created
        assert service.list_task_fronts() == []
        assert _messages(service, focal) == []

        quarter = service.engine._compute_spontaneous_probability(focal, engram)
        service.update_activity_center(center_id, autonomy=1.0)
        full = service.engine._compute_spontaneous_probability(focal, engram)
        assert quarter == pytest.approx(full * 0.25)
        assert full > 0

        service.update_activity_center(
            center_id,
            status=ActivityCenterStatus.DORMANT,
        )
        assert service.engine._compute_spontaneous_probability(focal, engram) == 0
        service.world.touch_activity_center(center_id)
        assert service.get_activity_center(center_id)["activity_center"]["status"] == "active"
        assert service.engine._compute_spontaneous_probability(focal, engram) == full

        service.update_activity_center(
            center_id,
            status=ActivityCenterStatus.PAUSED,
        )
        assert service.engine._compute_spontaneous_probability(focal, engram) == 0
        assert _messages(service, focal) == []
    finally:
        service.close()


def test_front_and_center_lifecycles_are_independent_and_explicit(tmp_path):
    service = RuntimeService(_config(tmp_path))
    try:
        created = service.create_task_front("Keep this relation alive")
        front_id = created["task_front"]["id"]
        center_id = created["activity_center"]["id"]

        service.update_task_front(front_id, status=TaskFrontStatus.CLOSED)
        assert service.get_activity_center(center_id)["activity_center"]["status"] == "active"
        with pytest.raises(ServiceError, match="task_front_not_open"):
            service.send_task_front_message(front_id, "still here")

        service.update_task_front(front_id, status=TaskFrontStatus.OPEN)
        service.update_activity_center(
            center_id,
            status=ActivityCenterStatus.DORMANT,
        )
        service.send_task_front_message(front_id, "wake on explicit stimulus")
        assert service.get_activity_center(center_id)["activity_center"]["status"] == "active"

        service.update_activity_center(
            center_id,
            status=ActivityCenterStatus.PAUSED,
        )
        with pytest.raises(ServiceError, match="activity_center_not_writable"):
            service.send_task_front_message(front_id, "do not bypass pause")
    finally:
        service.close()


def test_task_focal_succession_keeps_front_and_center_ids(tmp_path):
    service = RuntimeService(_config(tmp_path))
    try:
        created = service.create_task_front("A thought that will change generation")
        front_id = created["task_front"]["id"]
        center_id = created["activity_center"]["id"]
        old_focal = created["task_front"]["focal_engram_id"]
        service.engine.tick()

        successor = service.engrams.succession(old_focal).new_id
        front = service.get_task_front(front_id)["task_front"]
        center = service.get_activity_center(center_id)["activity_center"]

        assert front["id"] == front_id
        assert center["id"] == center_id
        assert front["focal_engram_id"] == successor
        assert center["focal_engram_id"] == successor
        assert service.world.list_memberships(
            center_id=center_id,
            engram_id=old_focal,
        ) == []
        assert service.world.list_memberships(
            center_id=center_id,
            engram_id=successor,
        )[0].relation.value == "focal"
    finally:
        service.close()


def test_pre_world_front_migrates_to_one_visible_task_front_idempotently(tmp_path):
    db = tmp_path / "legacy.db"
    storage = Storage(db)
    legacy_text = "A conversation from before PulseWorld v1"
    legacy = storage.create_engram(
        initial_messages=[Message(MessageRole.USER, legacy_text)],
    )
    storage.save_component_state(IDENTITY_COMPONENT, {
        "front_engram_id": legacy.id,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    storage.close()

    first = RuntimeService(_config(tmp_path, db))
    try:
        world_id = first.world_id
        fronts = first.list_task_fronts()
        centers = first.list_activity_centers(kind=ActivityKind.TASK)
        assert len(fronts) == len(centers) == 1
        assert fronts[0]["focal_engram_id"] == legacy.id
        assert centers[0]["origin"] == ActivityOrigin.SYSTEM.value
        assert any(
            message["content"] == legacy_text
            for message in first.get_task_front(fronts[0]["id"])["messages"]
        )
    finally:
        first.close()

    second = RuntimeService(_config(tmp_path, db))
    try:
        assert second.world_id == world_id
        assert len(second.list_task_fronts()) == 1
        assert len(second.list_activity_centers(kind=ActivityKind.TASK)) == 1
        assert second.storage.load_component_state(WORLD_COMPONENT)[
            "legacy_front_migrated"
        ] is True
    finally:
        second.close()


def test_world_snapshot_and_metrics_keep_natural_language_out_of_sideband(tmp_path):
    service = RuntimeService(_config(tmp_path))
    content = "CONTENT-MUST-NOT-ENTER-SIDEBAND"
    title = "TITLE-MUST-NOT-ENTER-SIDEBAND"
    description = "DESCRIPTION-MUST-NOT-ENTER-SIDEBAND"
    try:
        service.create_task_front(content, title=title)
        service.create_activity_center(
            ActivityKind.LIFE_PROJECT,
            "Private life project",
            description=description,
        )

        snapshot_text = repr(service.snapshot())
        metrics_text = repr(service.metrics.events())
        for private_text in (content, title, description):
            assert private_text not in snapshot_text
            assert private_text not in metrics_text
    finally:
        service.close()

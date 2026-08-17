"""HTTP contract tests for durable Center scheduling observability."""

from __future__ import annotations

from copy import deepcopy

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from pulse_system.agent.harness import HarnessError
from pulse_system.core.claustrum import EngramState
from pulse_system.interaction.api.routes_write import create_write_router
from pulse_system.service import RuntimeService, RuntimeServiceConfig
from pulse_system.service.runtime import ServiceError


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
def scheduling_client(tmp_path):
    service = RuntimeService(_config(tmp_path))
    app = FastAPI()
    app.state.runtime = service
    app.include_router(create_write_router())
    with TestClient(app) as client:
        yield client, service
    service.close()


def test_scheduling_route_exposes_real_empty_runtime_schema(scheduling_client):
    client, service = scheduling_client

    response = client.get("/scheduling")

    assert response.status_code == 200
    body = response.json()
    assert body == service.scheduling_snapshot()
    assert set(body) == {
        "policy_version",
        "lease",
        "capacity",
        "failure_domains",
        "lanes",
        "centers",
        "reservations",
    }
    assert body["policy_version"] == "durable-center-scheduling/v1"
    assert body["lease"]["healthy"] is True
    assert body["lease"]["epoch"] >= 1
    assert set(body["capacity"]) == {
        "budget_per_tick",
        "lane_reservation_per_tick",
        "starvation_boost",
        "starvation_debt_cap",
        "held",
        "background_dispatch",
        "worker_limit",
        "worker_running",
        "worker_available",
        "resident_limit",
        "resident_sessions",
        "starting_sessions",
        "busy_sessions",
        "claustrum_mounted",
        "claustrum_slot_limit",
        "claustrum_slot_used",
        "claustrum_slot_available",
        "claustrum_slot_utilization",
        "claustrum_last_requested",
        "claustrum_last_overflow",
        "succession_worker_limit",
        "succession_workers_running",
        "succession_subjects_pending",
        "succession_subjects_blocked",
    }
    assert body["capacity"]["worker_limit"] == 4
    assert body["capacity"]["worker_running"] == 0
    assert body["capacity"]["resident_limit"] == 0
    assert body["capacity"]["claustrum_mounted"] is False
    assert body["capacity"]["claustrum_slot_limit"] == 0
    assert body["capacity"]["claustrum_slot_utilization"] is None
    assert body["capacity"]["succession_worker_limit"] == 4
    assert body["capacity"]["succession_workers_running"] == 0
    assert body["capacity"]["succession_subjects_pending"] == 0
    assert body["capacity"]["succession_subjects_blocked"] == 0
    assert body["failure_domains"] == {
        "policy_version": "engram-failure-domain.v1",
        "evidence_class": "runtime_memory_projection",
        "limit": 64,
        "total": 0,
        "cooling": 0,
        "degraded": 0,
        "probe_ready": 0,
        "truncated": False,
        "items": [],
    }
    assert [lane["lane"] for lane in body["lanes"]] == ["work", "life"]
    assert body["centers"] == []
    assert body["reservations"] == []


def test_scheduling_capacity_projects_mounted_claustrum_without_side_effects(
    tmp_path,
):
    service = RuntimeService(_config(tmp_path, with_claustrum=True))
    try:
        claustrum = service.claustrum
        assert claustrum is not None
        claustrum.modulate([EngramState(
            engram_id=service.front_engram_id,
            recent_activity=0.0,
            connection_count=0,
            queue_depth=0,
            seconds_since_pulse=0.0,
            cluster_activity=0.0,
        )])
        before = (
            claustrum._observations,
            len(claustrum._buffer),
            id(claustrum._pending),
            tuple(claustrum._slots.live_ids()),
        )

        first = service.scheduling_snapshot()["capacity"]
        second = service.scheduling_snapshot()["capacity"]

        assert first == second
        assert first["claustrum_mounted"] is True
        assert first["claustrum_slot_limit"] == 256
        assert first["claustrum_slot_used"] == 1
        assert first["claustrum_slot_available"] == 255
        assert first["claustrum_slot_utilization"] == pytest.approx(1 / 256)
        assert first["claustrum_last_requested"] == 1
        assert first["claustrum_last_overflow"] == 0
        assert before == (
            claustrum._observations,
            len(claustrum._buffer),
            id(claustrum._pending),
            tuple(claustrum._slots.live_ids()),
        )
        encoded = str(first)
        assert service.front_engram_id not in encoded
    finally:
        service.close()


def test_scheduling_projects_failure_domains_without_mutating_them(tmp_path):
    service = RuntimeService(_config(tmp_path))
    try:
        service.engine._record_failure_domain(
            service.front_engram_id,
            HarnessError(
                "provider_unavailable",
                "private provider detail",
                "private operator remedy",
                phase="provider",
                retryable=True,
                prompt_accepted=False,
            ),
        )
        before = deepcopy(
            service.engine._failure_domains[service.front_engram_id]
        )

        first = service.scheduling_snapshot()["failure_domains"]
        second = service.scheduling_snapshot()["failure_domains"]

        assert first == second
        assert first["degraded"] == 1
        assert first["items"][0]["engram_id"] == service.front_engram_id
        assert first["items"][0]["last_error_code"] == "provider_unavailable"
        assert service.engine._failure_domains[service.front_engram_id] == before
        encoded = str(first)
        assert "private provider detail" not in encoded
        assert "private operator remedy" not in encoded
    finally:
        service.close()


class _SnapshotRuntime:
    def __init__(self, snapshot):
        self.snapshot = deepcopy(snapshot)
        # These values must never be discovered or merged by the route.
        self.prompt = "private prompt"
        self.output = "private model output"
        self.tool_arguments = {"secret": "private tool argument"}

    def scheduling_snapshot(self):
        return deepcopy(self.snapshot)


def _representative_snapshot():
    return {
        "policy_version": "durable-center-scheduling/v1",
        "lease": {
            "scope": "pulse_world",
            "owner_id": "owner-epoch-7",
            "epoch": 7,
            "state": "active",
            "healthy": True,
            "acquired_at": "2026-08-02T10:00:00+00:00",
            "renewed_at": "2026-08-02T10:00:03+00:00",
            "expires_at": "2026-08-02T10:00:33+00:00",
            "released_at": None,
            "lost_reason": None,
        },
        "capacity": {
            "budget_per_tick": 1,
            "lane_reservation_per_tick": 1,
            "starvation_boost": 0.05,
            "starvation_debt_cap": 20,
            "held": 0,
        },
        "failure_domains": {
            "policy_version": "engram-failure-domain.v1",
            "evidence_class": "runtime_memory_projection",
            "limit": 64,
            "total": 1,
            "cooling": 1,
            "degraded": 0,
            "probe_ready": 0,
            "truncated": False,
            "items": [
                {
                    "engram_id": "engram-6",
                    "state": "cooling",
                    "consecutive_failures": 3,
                    "last_failure_at": "2026-08-02T10:00:04+00:00",
                    "retry_at": "2026-08-02T10:00:34+00:00",
                    "last_error_code": "provider_unavailable",
                    "last_error_phase": "provider",
                    "error_retryable": True,
                    "prompt_accepted": False,
                }
            ],
        },
        "lanes": [
            {
                "lane": "work",
                "waiting_centers": 1,
                "max_debt": 4,
                "last_admitted_at": "2026-08-02T09:59:00+00:00",
            },
            {
                "lane": "life",
                "waiting_centers": 1,
                "max_debt": 2,
                "last_admitted_at": None,
            },
        ],
        "centers": [
            {
                "center_id": "task-center",
                "lane": "work",
                "status": "active",
                "decision": "waiting",
                "reason": "budget_deferred",
                "starvation_debt": 4,
                "waiting_since": "2026-08-02T09:59:30+00:00",
                "last_admitted_at": "2026-08-02T09:59:00+00:00",
                "last_decision_at": "2026-08-02T10:00:04+00:00",
                "updated_at": "2026-08-02T10:00:04+00:00",
            },
            {
                "center_id": "life-center",
                "lane": "life",
                "status": "active",
                "decision": "admitted",
                "reason": "lane_reservation",
                "starvation_debt": 0,
                "waiting_since": None,
                "last_admitted_at": "2026-08-02T10:00:04+00:00",
                "last_decision_at": "2026-08-02T10:00:04+00:00",
                "updated_at": "2026-08-02T10:00:04+00:00",
            },
        ],
        "reservations": [
            {
                "id": "reservation-6",
                "world_id": "world-1",
                "event_id": "event-6",
                "engram_id": "engram-6",
                "center_id": "life-center",
                "lane": "life",
                "owner_id": "owner-epoch-6",
                "lease_epoch": 6,
                "state": "abandoned",
                "outcome": "owner_replaced",
                "reason": "fair_share",
                "base_priority": 0.4,
                "effective_score": 0.5,
                "created_at": "2026-08-02T09:58:00+00:00",
                "settled_at": "2026-08-02T10:00:00+00:00",
            }
        ],
    }


def test_scheduling_route_passes_through_waiting_and_abandoned_facts_only():
    snapshot = _representative_snapshot()
    app = FastAPI()
    app.state.runtime = _SnapshotRuntime(snapshot)
    app.include_router(create_write_router())

    with TestClient(app) as client:
        response = client.get("/scheduling")

    assert response.status_code == 200
    assert response.json() == snapshot
    encoded = response.text.lower()
    for forbidden in ("private prompt", "private model output", "private tool argument"):
        assert forbidden not in encoded


def test_scheduling_route_without_runtime_uses_flat_refusal_contract():
    app = FastAPI()
    app.include_router(create_write_router())

    with TestClient(app) as client:
        response = client.get("/scheduling")

    assert response.status_code == 503
    assert response.json()["error"] == "no_runtime"
    assert set(response.json()) == {"error", "detail", "remedy"}


class _UnavailableRuntime:
    def scheduling_snapshot(self):
        raise ServiceError(
            "scheduling_unavailable",
            "the scheduler is not mounted",
            "finish Runtime construction",
            status=503,
        )


def test_scheduling_service_error_uses_existing_refusal_adapter():
    app = FastAPI()
    app.state.runtime = _UnavailableRuntime()
    app.include_router(create_write_router())

    with TestClient(app) as client:
        response = client.get("/scheduling")

    assert response.status_code == 503
    assert response.json() == {
        "error": "scheduling_unavailable",
        "detail": "the scheduler is not mounted",
        "remedy": "finish Runtime construction",
    }

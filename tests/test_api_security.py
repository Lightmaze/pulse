"""Security contract for the process-local Workbench API boundary."""

from __future__ import annotations

import json
import logging

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pulse_system.interaction.api import create_app
from pulse_system.interaction.api.security import (
    ApiSecurityConfigurationError,
    ApiSecurityMiddleware,
    CapabilityProfile,
    LocalApiSecurity,
    is_loopback_host,
    validate_network_bind,
    validate_origin,
)

TOKEN = "test_token_" + "a" * 40
ALLOWED_ORIGIN = "http://127.0.0.1:5173"


@pytest.mark.parametrize(
    "origin",
    [
        "*",
        "null",
        "http://localhost:5173/",
        "http://user@localhost:5173",
        "http://localhost:5173/path",
        "http://localhost:5173?query=yes",
        "http://localhost:5173#fragment",
        "ftp://localhost:5173",
        " http://localhost:5173",
        "",
    ],
)
def test_origin_validation_fails_closed(origin):
    with pytest.raises(ApiSecurityConfigurationError):
        validate_origin(origin)


def test_exact_origin_and_loopback_variants_are_accepted():
    assert validate_origin(ALLOWED_ORIGIN) == ALLOWED_ORIGIN
    assert is_loopback_host("127.0.0.1")
    assert is_loopback_host("127.8.9.10")
    assert is_loopback_host("::1")
    assert is_loopback_host("[::1]")
    assert is_loopback_host("LOCALHOST")
    assert not is_loopback_host("0.0.0.0")


def test_non_loopback_bind_requires_both_opt_ins():
    for allow, explicit in ((False, False), (True, False), (False, True)):
        with pytest.raises(ApiSecurityConfigurationError):
            validate_network_bind(
                "0.0.0.0",
                allow_network_bind=allow,
                explicit_origins=explicit,
            )
    assert validate_network_bind(
        "0.0.0.0",
        allow_network_bind=True,
        explicit_origins=True,
    ) is False


def test_security_value_requires_explicit_origins_for_network_bind():
    with pytest.raises(ApiSecurityConfigurationError):
        LocalApiSecurity(host="0.0.0.0", allow_network_bind=True)
    configured = LocalApiSecurity(
        CapabilityProfile.LAB,
        access_token=TOKEN,
        host="0.0.0.0",
        allow_network_bind=True,
        allowed_origins=("https://workbench.example",),
    )
    assert configured.loopback_only is False


def test_generated_tokens_are_per_start_and_repr_is_redacted():
    first = LocalApiSecurity()
    second = LocalApiSecurity()
    assert first.access_token != second.access_token
    assert len(first.access_token) >= 32
    assert first.access_token not in repr(first)


def _probe_client(security: LocalApiSecurity) -> TestClient:
    app = FastAPI()
    app.add_middleware(ApiSecurityMiddleware, security=security)

    @app.post("/probe")
    def probe():
        return {"reached": True}

    return TestClient(app)


def test_safe_denies_mutation_even_with_the_correct_token():
    client = _probe_client(LocalApiSecurity(access_token=TOKEN))
    response = client.post(
        "/probe", headers={"Authorization": f"Bearer {TOKEN}"}
    )
    assert response.status_code == 403
    assert response.json()["error"] == "profile_write_denied"


@pytest.mark.parametrize(
    "authorization",
    [None, "", "Basic abc", "Bearer", "Bearer wrong_token_" + "x" * 40],
)
def test_workspace_rejects_missing_malformed_or_wrong_token(authorization):
    client = _probe_client(
        LocalApiSecurity(CapabilityProfile.WORKSPACE, access_token=TOKEN)
    )
    headers = {} if authorization is None else {"Authorization": authorization}
    response = client.post("/probe", headers=headers)
    assert response.status_code == 401
    assert response.json()["error"] == "api_token_invalid"
    assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.parametrize("profile", [CapabilityProfile.WORKSPACE, CapabilityProfile.LAB])
def test_write_profile_with_valid_token_reaches_the_existing_route(profile):
    client = _probe_client(LocalApiSecurity(profile, access_token=TOKEN))
    response = client.post(
        "/probe", headers={"Authorization": f"Bearer {TOKEN}"}
    )
    assert response.status_code == 200
    assert response.json() == {"reached": True}


def test_rejection_log_and_response_do_not_disclose_token(caplog):
    client = _probe_client(
        LocalApiSecurity(CapabilityProfile.WORKSPACE, access_token=TOKEN)
    )
    with caplog.at_level(logging.WARNING, logger="pulse_system.api.security"):
        response = client.post(
            "/probe", headers={"Authorization": f"Bearer {TOKEN}wrong"}
        )
    evidence = response.text + caplog.text
    assert TOKEN not in evidence
    assert "api_token_invalid" in evidence


def test_rejection_log_uses_route_template_not_sensitive_path_value(tmp_path, caplog):
    security = LocalApiSecurity(
        CapabilityProfile.WORKSPACE,
        access_token=TOKEN,
    )
    client = TestClient(create_app(tmp_path / "m.jsonl", api_security=security))
    private_event_id = "private-event-identifier"
    with caplog.at_level(logging.WARNING, logger="pulse_system.api.security"):
        response = client.post(
            f"/causal-events/{private_event_id}/reconcile",
            headers={"Authorization": "Bearer " + "x" * 40},
            json={"action": "cancel"},
        )
    assert response.status_code == 401
    assert private_event_id not in caplog.text
    assert "/causal-events/{event_id}/reconcile" in caplog.text


def test_runtime_profile_and_health_are_safe_projections(tmp_path):
    metrics = tmp_path / "private" / "metrics.jsonl"
    security = LocalApiSecurity(
        CapabilityProfile.WORKSPACE,
        access_token=TOKEN,
    )
    client = TestClient(create_app(metrics, api_security=security))

    profile = client.get("/runtime-profile").json()
    assert profile == {
        "schema_version": "pulse-runtime-profile.v1",
        "product_version": "0.2.0-alpha.1",
        "profile": "workspace",
        "write_enabled": True,
        "token_required": True,
        "loopback_only": True,
    }
    health = client.get("/health").json()
    assert health["metrics_available"] is False
    combined = json.dumps({"profile": profile, "health": health})
    assert TOKEN not in combined
    assert str(metrics) not in combined


def test_cors_preflight_allows_only_an_exact_configured_origin(tmp_path):
    client = TestClient(create_app(tmp_path / "m.jsonl"))
    headers = {
        "Origin": ALLOWED_ORIGIN,
        "Access-Control-Request-Method": "POST",
        "Access-Control-Request-Headers": "authorization,content-type",
    }
    allowed = client.options("/status", headers=headers)
    assert allowed.status_code == 200
    assert allowed.headers["access-control-allow-origin"] == ALLOWED_ORIGIN

    denied = client.options(
        "/status",
        headers={**headers, "Origin": "https://untrusted.example"},
    )
    assert denied.status_code == 400
    assert "access-control-allow-origin" not in denied.headers

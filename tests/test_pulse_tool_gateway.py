from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from pulse_system.agent.tools.gateway import PulseToolGateway, ToolInvocationContext


def _post(
    url: str,
    token: str,
    payload: Any,
    *,
    tool_call_id: str | None = "tool-call-1",
) -> tuple[int, dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if tool_call_id is not None:
        headers["X-Pulse-Tool-Call-Id"] = tool_call_id
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urlopen(request, timeout=2) as response:  # noqa: S310 - loopback test
            return int(response.status), json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return int(exc.code), json.loads(exc.read().decode("utf-8"))


def test_loopback_capability_dispatch_and_identity_rejection() -> None:
    calls: list[tuple[str, str, dict[str, Any], ToolInvocationContext]] = []
    gateway = PulseToolGateway(
        dispatcher=lambda engram, tool, args, context: (
            calls.append((engram, tool, args, context))
            or {
                "ok": True,
                "content": "observed",
                "data": {"organ": args["organ"]},
                "event_id": "child-1",
            }
        )
    )
    try:
        address = gateway.start()
        token = gateway.issue("engram-a")

        assert address.host == "127.0.0.1"
        assert address.port > 0
        status, body = _post(
            f"{address.url}/v1/tools/pulse_habitat_observe",
            token,
            {"organ": "window"},
            tool_call_id="pi-call-42",
        )
        assert status == 200
        assert body == {
            "ok": True,
            "content": "observed",
            "data": {"organ": "window"},
            "event_id": "child-1",
        }
        assert calls[0][:3] == (
            "engram-a",
            "pulse_habitat_observe",
            {"organ": "window"},
        )
        assert calls[0][3].tool_call_id == "pi-call-42"

        status, body = _post(
            f"{address.url}/v1/tools/pulse_habitat_observe",
            token,
            {"organ": "window", "engram_id": "forged"},
        )
        assert status == 400
        assert body["error"] == "identity_field_not_allowed"
        assert len(calls) == 1

        status, body = _post(
            f"{address.url}/v1/tools/pulse_habitat_act",
            token,
            {"verb": "act", "payload": {"engram_id": "forged"}},
        )
        assert status == 400
        assert body["error"] == "identity_field_not_allowed"
        assert len(calls) == 1

        status, body = _post(
            f"{address.url}/v1/tools/pulse_habitat_observe",
            "wrong-token",
            {"organ": "window"},
        )
        assert status == 401
        assert body["error"] == "unauthorized"
        assert token not in json.dumps(body)
        assert address.url not in json.dumps(body)

        status, body = _post(
            f"{address.url}/v1/tools/pulse_habitat_observe",
            token,
            {"organ": "window"},
            tool_call_id=None,
        )
        assert status == 400
        assert body["error"] == "tool_call_id_invalid"
    finally:
        gateway.close()
        gateway.close()


def test_schema_size_limit_authorization_and_handler_failure() -> None:
    authorized: list[tuple[str, str, dict[str, Any]]] = []

    def authorize(engram: str, tool: str, ephemeral: dict[str, Any]) -> dict[str, Any]:
        authorized.append((engram, tool, ephemeral))
        return {
            "allow": tool == "read",
            "reason": "test_policy" if tool == "read" else "/private/path",
        }

    gateway = PulseToolGateway(
        dispatcher=lambda *_args: (_ for _ in ()).throw(RuntimeError("private detail")),
        authorizer=authorize,
        max_body_bytes=1024,
    )
    try:
        address = gateway.start()
        token = gateway.issue("engram-b")

        status, body = _post(
            f"{address.url}/v1/tools/pulse_life_create",
            token,
            {"kind": "task", "title": "not allowed by schema"},
        )
        assert status == 400
        assert body["error"] == "tool_schema_kind_invalid"

        status, body = _post(
            f"{address.url}/v1/authorize-tool",
            token,
            {"tool_name": "read", "input": {"path": "README.md"}},
        )
        assert status == 200
        assert body == {"allow": True, "reason": "test_policy"}
        assert authorized == [("engram-b", "read", {"path": "README.md"})]

        status, body = _post(
            f"{address.url}/v1/authorize-tool",
            token,
            {"tool_name": "write", "input": {"path": "README.md"}},
        )
        assert status == 200
        assert body == {"allow": False, "reason": "denied"}

        status, body = _post(
            f"{address.url}/v1/authorize-tool",
            token,
            {"tool_name": "read", "input": {"engram_id": "forged"}},
        )
        assert status == 400
        assert body["error"] == "authorize_schema_invalid"

        status, body = _post(
            f"{address.url}/v1/tools/pulse_life_list",
            token,
            {},
        )
        assert status == 500
        assert body == {"error": "handler_error"}
        assert token not in json.dumps(body)
        assert address.url not in json.dumps(body)

        status, body = _post(
            f"{address.url}/v1/tools/pulse_habitat_act",
            token,
            {"verb": "x", "payload": {"blob": "a" * 2000}},
        )
        assert status == 400
        assert body["error"] == "body_too_large"
    finally:
        gateway.close()


def test_revoke_and_close_invalidate_tokens_and_serve_concurrently() -> None:
    gateway = PulseToolGateway(
        dispatcher=lambda engram, tool, args, _context: {
            "ok": True,
            "content": f"{engram}:{tool}:{args['organ']}",
        }
    )
    try:
        address = gateway.start()
        token_a = gateway.issue("a")
        token_b = gateway.issue("b")
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(
                pool.map(
                    lambda index: _post(
                        f"{address.url}/v1/tools/pulse_habitat_observe",
                        token_a if index % 2 else token_b,
                        {"organ": str(index)},
                    ),
                    range(20),
                )
            )
        assert all(status == 200 and body["ok"] for status, body in results)

        gateway.revoke(token_a)
        status, body = _post(
            f"{address.url}/v1/tools/pulse_habitat_observe",
            token_a,
            {"organ": "revoked"},
        )
        assert status == 401
        assert body["error"] == "unauthorized"

        gateway.revoke(engram_id="b")
        status, body = _post(
            f"{address.url}/v1/tools/pulse_habitat_observe",
            token_b,
            {"organ": "revoked"},
        )
        assert status == 401
        assert body["error"] == "unauthorized"
    finally:
        gateway.close()
    gateway.close()
    gateway.revoke(token_a)
    with pytest.raises(RuntimeError):
        gateway.issue("c")


def test_default_gateway_fails_closed_without_g04_callbacks() -> None:
    gateway = PulseToolGateway()
    try:
        address = gateway.start()
        token = gateway.issue("engram-c")
        status, body = _post(
            f"{address.url}/v1/authorize-tool",
            token,
            {"tool_name": "bash", "input": {"command": "echo safe"}},
        )
        assert status == 200
        assert body == {"allow": False, "reason": "authorization_unavailable"}
    finally:
        gateway.close()


def test_bash_schema_accepts_only_boolean_background_mode() -> None:
    args, error = PulseToolGateway._validate_tool_input(
        "bash",
        {"command": "run a bounded command", "background": True},
    )

    assert error is None
    assert args == {"command": "run a bounded command", "background": True}
    assert PulseToolGateway._validate_tool_input(
        "bash",
        {"command": "run a bounded command", "background": "true"},
    ) == (None, "tool_schema_background_invalid")


def test_living_portfolio_schema_dispatch_and_identity_are_capability_bound() -> None:
    calls: list[tuple[str, str, dict[str, Any], str]] = []
    gateway = PulseToolGateway(
        dispatcher=lambda engram, tool, args, context: (
            calls.append((engram, tool, args, context.tool_call_id))
            or {
                "ok": True,
                "content": "portfolio observed",
                "data": {
                    "portfolio": {
                        "schema_version": "living-portfolio.v1",
                        "subject": {"requested_engram_id": engram},
                    }
                },
                "event_id": "portfolio-result",
            }
        )
    )
    try:
        address = gateway.start()
        token = gateway.issue("portfolio-owner")

        for index, payload in enumerate(({}, {"history_limit": 1}, {"history_limit": 100})):
            status, body = _post(
                f"{address.url}/v1/tools/pulse_life_portfolio",
                token,
                payload,
                tool_call_id=f"portfolio-valid-{index}",
            )
            assert status == 200
            assert body["ok"] is True
            assert body["data"]["portfolio"]["subject"] == {
                "requested_engram_id": "portfolio-owner"
            }

        assert calls == [
            ("portfolio-owner", "pulse_life_portfolio", {}, "portfolio-valid-0"),
            (
                "portfolio-owner",
                "pulse_life_portfolio",
                {"history_limit": 1},
                "portfolio-valid-1",
            ),
            (
                "portfolio-owner",
                "pulse_life_portfolio",
                {"history_limit": 100},
                "portfolio-valid-2",
            ),
        ]

        for index, payload in enumerate(
            (
                {"history_limit": 0},
                {"history_limit": 101},
                {"history_limit": True},
                {"history_limit": 20.0},
                {"history_limit": "20"},
            )
        ):
            status, body = _post(
                f"{address.url}/v1/tools/pulse_life_portfolio",
                token,
                payload,
                tool_call_id=f"portfolio-invalid-{index}",
            )
            assert status == 400
            assert body["error"] == "tool_schema_history_limit_invalid"

        status, body = _post(
            f"{address.url}/v1/tools/pulse_life_portfolio",
            token,
            {"history_limit": 20, "unexpected": True},
            tool_call_id="portfolio-unknown",
        )
        assert status == 400
        assert body["error"] == "tool_schema_unknown_field"

        status, body = _post(
            f"{address.url}/v1/tools/pulse_life_portfolio",
            token,
            {"engram_id": "another-subject"},
            tool_call_id="portfolio-forged",
        )
        assert status == 400
        assert body["error"] == "identity_field_not_allowed"
        assert len(calls) == 3
    finally:
        gateway.close()


def test_temporary_worker_tool_schemas_are_bounded_and_identity_free() -> None:
    calls: list[tuple[str, str, dict[str, Any], str]] = []
    gateway = PulseToolGateway(
        dispatcher=lambda engram, tool, args, context: (
            calls.append((engram, tool, args, context.tool_call_id))
            or {"ok": True, "content": "accepted", "data": {}, "event_id": None}
        )
    )
    try:
        address = gateway.start()
        token = gateway.issue("engram-worker-parent")
        valid = (
            ("pulse_task_spawn", {"task": "inspect the delegated question", "timeout": 30}),
            ("pulse_task_wait", {"task_id": "task_abc123", "after_seq": 0, "timeout": 1}),
            ("pulse_task_steer", {"task_id": "task_abc123", "message": "follow the main path"}),
            ("pulse_task_stop", {"task_id": "task_abc123", "reason": "parent settled"}),
        )
        for index, (tool_name, payload) in enumerate(valid):
            status, body = _post(
                f"{address.url}/v1/tools/{tool_name}",
                token,
                payload,
                tool_call_id=f"worker-tool-{index}",
            )
            assert status == 200
            assert body["ok"] is True

        assert [call[1] for call in calls] == [item[0] for item in valid]
        assert all(call[0] == "engram-worker-parent" for call in calls)

        invalid = (
            ("pulse_task_spawn", {"task": "x", "timeout": 901}),
            ("pulse_task_wait", {"task_id": "engram-forgery"}),
            ("pulse_task_steer", {"task_id": "task_abc123", "message": ""}),
            ("pulse_task_stop", {"task_id": "task_abc123", "engram_id": "forged"}),
        )
        for index, (tool_name, payload) in enumerate(invalid):
            status, body = _post(
                f"{address.url}/v1/tools/{tool_name}",
                token,
                payload,
                tool_call_id=f"worker-invalid-{index}",
            )
            assert status == 400
            assert body["error"].startswith(("tool_schema_", "identity_field_"))
        assert len(calls) == 4
    finally:
        gateway.close()


def test_purpose_and_role_tool_schemas_preserve_cas_and_bounded_scope() -> None:
    calls: list[tuple[str, dict[str, Any]]] = []
    gateway = PulseToolGateway(
        dispatcher=lambda _engram, tool, args, _context: (
            calls.append((tool, args))
            or {"ok": True, "content": "accepted", "data": {}, "event_id": None}
        )
    )
    try:
        address = gateway.start()
        token = gateway.issue("engram-purpose-holder")
        valid = (
            ("pulse_life_purpose", {"history": True, "limit": 20}),
            (
                "pulse_life_amend_purpose",
                {"amendment_kind": "establish", "content": "Live deliberately."},
            ),
            (
                "pulse_life_amend_purpose",
                {
                    "amendment_kind": "amend",
                    "expected_revision": 1,
                    "content": "Live deliberately and remain open.",
                },
            ),
            ("pulse_life_roles", {"active_only": True}),
            (
                "pulse_life_accept_role",
                {
                    "role_label": "patient maker",
                    "center_ids": ["center-a"],
                    "ttl_seconds": 3600,
                    "obligation": {
                        "kind": "direct_output",
                        "minimum_direct_outputs": 1,
                        "max_consecutive_coordination": 3,
                        "accepted_output_kinds": [
                            "workspace_checkpoint",
                            "habitat_effect",
                        ],
                    },
                },
            ),
            (
                "pulse_life_renew_role",
                {
                    "role_lease_id": "role_abc",
                    "expected_role_epoch": 1,
                    "ttl_seconds": 3600,
                },
            ),
            (
                "pulse_life_release_role",
                {"role_lease_id": "role_abc", "expected_role_epoch": 1},
            ),
        )
        for index, (tool_name, payload) in enumerate(valid):
            status, body = _post(
                f"{address.url}/v1/tools/{tool_name}",
                token,
                payload,
                tool_call_id=f"governance-valid-{index}",
            )
            assert status == 200
            assert body["ok"] is True

        invalid = (
            ("pulse_life_amend_purpose", {"amendment_kind": "amend", "content": "no CAS"}),
            (
                "pulse_life_amend_purpose",
                {"amendment_kind": "withdraw", "expected_revision": 2, "content": "hidden"},
            ),
            ("pulse_life_accept_role", {"role_label": "lead", "center_ids": []}),
            (
                "pulse_life_accept_role",
                {"role_label": "lead", "center_ids": ["center-a", "center-a"]},
            ),
            (
                "pulse_life_accept_role",
                {
                    "role_label": "lead",
                    "center_ids": ["center-a"],
                    "obligation": {"minimum_direct_outputs": 0},
                },
            ),
            (
                "pulse_life_renew_role",
                {"role_lease_id": "role_abc", "expected_role_epoch": 0},
            ),
            (
                "pulse_life_release_role",
                {"role_lease_id": "role_abc", "expected_role_epoch": 0},
            ),
        )
        for index, (tool_name, payload) in enumerate(invalid):
            status, body = _post(
                f"{address.url}/v1/tools/{tool_name}",
                token,
                payload,
                tool_call_id=f"governance-invalid-{index}",
            )
            assert status == 400
            assert body["error"].startswith("tool_schema_")
        assert len(calls) == len(valid)
    finally:
        gateway.close()

"""Loopback capability gateway for Pi Pulse tools.

The Gateway is deliberately a narrow transport boundary.  It authenticates
the process capability, validates the frozen request shapes, and delegates
world behavior to callbacks supplied by a later integration layer.  It does
not infer event source, persist tool arguments, or accept an Engram identity
from the request body.
"""

from __future__ import annotations

import hmac
import json
import math
import re
import secrets
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

__all__ = [
    "DispatchCallback",
    "CancelCallback",
    "GatewayAddress",
    "LegacyDispatchCallback",
    "PulseToolGateway",
    "TOOL_CALL_ID_HEADER",
    "ToolInvocationContext",
]

DispatchCallback = Callable[
    [str, str, dict[str, Any], "ToolInvocationContext"], Mapping[str, Any]
]
CancelCallback = Callable[[str, "ToolInvocationContext"], Mapping[str, Any]]
LegacyDispatchCallback = Callable[[str, str, dict[str, Any]], Mapping[str, Any]]
AuthorizeCallback = Callable[[str, str, dict[str, Any]], Mapping[str, Any]]

TOOL_CALL_ID_HEADER = "X-Pulse-Tool-Call-Id"
_MAX_ACTION_TIMEOUT_SECONDS = 300.0
_MAX_TASK_TIMEOUT_SECONDS = 900.0
_MAX_TASK_WAIT_SECONDS = 30.0
_REASON_CODE = re.compile(r"^[a-z][a-z0-9_.-]{0,79}$")
_TOOL_CALL_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_TASK_ID = re.compile(r"^task_[A-Za-z0-9_.:-]{1,91}$")

_TOOL_NAMES = (
    "read",
    "bash",
    "edit",
    "write",
    "pulse_task_offer_respond",
    "pulse_task_relationship_respond",
    "pulse_life_list",
    "pulse_life_portfolio",
    "pulse_life_concerns",
    "pulse_life_hold",
    "pulse_life_orientations",
    "pulse_life_orient",
    "pulse_life_create",
    "pulse_life_update",
    "pulse_life_purpose",
    "pulse_life_amend_purpose",
    "pulse_life_roles",
    "pulse_life_accept_role",
    "pulse_life_renew_role",
    "pulse_life_release_role",
    "pulse_habitat_observe",
    "pulse_habitat_act",
    "pulse_habitat_subscribe",
    "pulse_delegate",
    "pulse_mcp_call",
    "pulse_task_spawn",
    "pulse_task_wait",
    "pulse_task_steer",
    "pulse_task_stop",
)
_TOOL_NAME_SET = frozenset(_TOOL_NAMES)
_ALLOWED_KINDS = frozenset(
    {
        "hobby",
        "life_project",
        "relationship",
        "exploration",
        "practice",
        "expression",
        "rest",
        "other",
    }
)
_ALLOWED_STATUSES = frozenset(
    {"active", "dormant", "paused", "completed", "archived"}
)


@dataclass(frozen=True, slots=True)
class GatewayAddress:
    """The non-secret address exposed to one local Pi process."""

    host: str
    port: int

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"


@dataclass(frozen=True, slots=True)
class ToolInvocationContext:
    """Non-secret Pi invocation identity supplied outside tool arguments."""

    tool_call_id: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.tool_call_id, str)
            or _TOOL_CALL_ID.fullmatch(self.tool_call_id) is None
        ):
            raise ValueError("tool_call_id has an invalid shape")


class PulseToolGateway:
    """A bounded, loopback-only HTTP capability gateway.

    ``dispatcher`` and ``authorizer`` are intentionally optional.  The tool
    protocol owns authorization; Runtime supplies world and causal services.
    Until then, tool calls fail safely and authorization is denied.
    """

    def __init__(
        self,
        *,
        dispatcher: DispatchCallback | None = None,
        legacy_dispatcher: LegacyDispatchCallback | None = None,
        authorizer: AuthorizeCallback | None = None,
        canceller: CancelCallback | None = None,
        max_body_bytes: int = 64 * 1024,
    ) -> None:
        if type(max_body_bytes) is not int or max_body_bytes < 1024:
            raise ValueError("max_body_bytes must be an integer >= 1024")
        if dispatcher is not None and legacy_dispatcher is not None:
            raise ValueError("provide dispatcher or legacy_dispatcher, not both")
        self._dispatcher = dispatcher
        self._legacy_dispatcher = legacy_dispatcher
        self._authorizer = authorizer
        self._canceller = canceller
        self._max_body_bytes = max_body_bytes
        self._lock = threading.RLock()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._address: GatewayAddress | None = None
        self._closed = False
        self._active_handler_threads: set[int] = set()
        self._close_summary: dict[str, Any] | None = None
        self._tokens: dict[str, str] = {}
        self._tokens_by_engram: dict[str, set[str]] = {}

    @property
    def address(self) -> GatewayAddress | None:
        with self._lock:
            return self._address

    @property
    def url(self) -> str | None:
        address = self.address
        return None if address is None else address.url

    def start(self) -> GatewayAddress:
        """Bind ``127.0.0.1:0`` and start the request thread once."""

        with self._lock:
            if self._closed:
                raise RuntimeError("pulse tool gateway is closed")
            if self._server is not None and self._address is not None:
                return self._address

            gateway = self

            class _Handler(BaseHTTPRequestHandler):
                # The response always includes Content-Length and closes the
                # connection, so a request cannot strand a worker on keepalive.
                protocol_version = "HTTP/1.1"

                def do_POST(self) -> None:  # noqa: N802 - stdlib hook name
                    thread_id = threading.get_ident()
                    with gateway._lock:
                        gateway._active_handler_threads.add(thread_id)
                    try:
                        gateway._handle_post(self)
                    finally:
                        with gateway._lock:
                            gateway._active_handler_threads.discard(thread_id)

                def log_message(self, _format: str, *_args: Any) -> None:
                    # Capability, URL and request bodies must never enter
                    # access logs.  Callers receive structured safe errors.
                    return

            server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
            server.daemon_threads = True
            server.block_on_close = False
            host, port = server.server_address[:2]
            address = GatewayAddress(str(host), int(port))
            thread = threading.Thread(
                target=server.serve_forever,
                kwargs={"poll_interval": 0.05},
                name="pulse-tool-gateway",
                daemon=True,
            )
            self._server = server
            self._address = address
            self._thread = thread
            thread.start()
            return address

    def issue(self, engram_id: str) -> str:
        """Issue one 256-bit in-memory capability for one process owner."""

        if not isinstance(engram_id, str) or not engram_id.strip():
            raise ValueError("engram_id must be a non-empty string")
        self.start()
        with self._lock:
            if self._closed:
                raise RuntimeError("pulse tool gateway is closed")
            while True:
                token = secrets.token_urlsafe(32)
                if token not in self._tokens:
                    break
            self._tokens[token] = engram_id
            self._tokens_by_engram.setdefault(engram_id, set()).add(token)
            return token

    def revoke(self, token: str | None = None, *, engram_id: str | None = None) -> None:
        """Idempotently revoke one token or every token for an Engram."""

        if (token is None) == (engram_id is None):
            raise ValueError("provide exactly one token or engram_id")
        with self._lock:
            if token is not None:
                owner = self._tokens.pop(token, None)
                if owner is not None:
                    owned = self._tokens_by_engram.get(owner)
                    if owned is not None:
                        owned.discard(token)
                        if not owned:
                            self._tokens_by_engram.pop(owner, None)
                return

            assert engram_id is not None
            for owned_token in self._tokens_by_engram.pop(engram_id, set()):
                self._tokens.pop(owned_token, None)

    def close(self) -> dict[str, Any]:
        """Stop serving and return payload-free owner-exit evidence."""

        with self._lock:
            if self._closed:
                if self._close_summary is not None:
                    return dict(self._close_summary)
                return {
                    "serve_thread_joined": False,
                    "active_handlers": len(self._active_handler_threads),
                    "owner_joined": False,
                }
            self._closed = True
            self._tokens.clear()
            self._tokens_by_engram.clear()
            server = self._server
            thread = self._thread

        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2.0)
        with self._lock:
            serve_joined = thread is None or not thread.is_alive()
            active_handlers = len(self._active_handler_threads)
            summary = {
                "serve_thread_joined": serve_joined,
                "active_handlers": active_handlers,
                "owner_joined": serve_joined and active_handlers == 0,
            }
            self._close_summary = dict(summary)
            return summary

    def _handle_post(self, handler: BaseHTTPRequestHandler) -> None:
        path = urlsplit(handler.path)
        if path.query or path.fragment:
            self._write_json(handler, HTTPStatus.BAD_REQUEST, {"error": "query_not_allowed"})
            return

        route = self._route(path.path)
        if route is None:
            self._write_json(handler, HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return

        authenticated = self._authenticate(handler.headers.get("Authorization"))
        if authenticated is None:
            self._write_json(handler, HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
            return
        engram_id = authenticated

        tool_call_id = self._read_tool_call_id(handler)
        if tool_call_id is None:
            self._write_json(
                handler,
                HTTPStatus.BAD_REQUEST,
                {"error": "tool_call_id_invalid"},
            )
            return
        invocation = ToolInvocationContext(tool_call_id)

        payload, error = self._read_json(handler)
        if error is not None:
            self._write_json(handler, HTTPStatus.BAD_REQUEST, {"error": error})
            return
        assert payload is not None

        if route[0] == "cancel":
            if payload:
                self._write_json(
                    handler,
                    HTTPStatus.BAD_REQUEST,
                    {"error": "cancel_schema_invalid"},
                )
                return
            result = self._cancel(engram_id, invocation)
            self._write_json(handler, result[0], result[1])
            return

        if route[0] == "tool":
            tool_name = route[1]
            if tool_name not in _TOOL_NAME_SET:
                self._write_json(handler, HTTPStatus.NOT_FOUND, {"error": "unknown_tool"})
                return
            if _contains_identity_field(payload):
                self._write_json(
                    handler,
                    HTTPStatus.BAD_REQUEST,
                    {"error": "identity_field_not_allowed"},
                )
                return
            args, error = self._validate_tool_input(tool_name, payload)
            if error is not None:
                self._write_json(handler, HTTPStatus.BAD_REQUEST, {"error": error})
                return
            assert args is not None
            result = self._dispatch(engram_id, tool_name, args, invocation)
            self._write_json(handler, result[0], result[1])
            return

        if set(payload) != {"tool_name", "input"}:
            self._write_json(
                handler,
                HTTPStatus.BAD_REQUEST,
                {"error": "authorize_schema_invalid"},
            )
            return
        tool_name = payload.get("tool_name")
        ephemeral_input = payload.get("input")
        if (
            not isinstance(tool_name, str)
            or not tool_name.strip()
            or not isinstance(ephemeral_input, dict)
            or _contains_identity_field(ephemeral_input)
        ):
            self._write_json(
                handler,
                HTTPStatus.BAD_REQUEST,
                {"error": "authorize_schema_invalid"},
            )
            return
        result = self._authorize(engram_id, tool_name, dict(ephemeral_input))
        self._write_json(handler, result[0], result[1])

    @staticmethod
    def _route(path: str) -> tuple[str, str] | tuple[str] | None:
        if path == "/v1/tools/cancel":
            return ("cancel",)
        parts = path.split("/")
        if len(parts) == 4 and parts[:3] == ["", "v1", "tools"] and parts[3]:
            return ("tool", parts[3])
        if path == "/v1/authorize-tool":
            return ("authorize",)
        return None

    def _authenticate(self, header: str | None) -> str | None:
        if not isinstance(header, str) or not header.startswith("Bearer "):
            return None
        token = header[7:]
        if not token or token.strip() != token or " " in token:
            return None
        with self._lock:
            if self._closed:
                return None
            # A token map provides the capability boundary.  Compare the
            # actual token in constant time as well, avoiding string leakage
            # if an embedding ever changes the map implementation.
            for candidate, engram_id in self._tokens.items():
                if hmac.compare_digest(candidate, token):
                    return engram_id
        return None

    @staticmethod
    def _read_tool_call_id(handler: BaseHTTPRequestHandler) -> str | None:
        values = handler.headers.get_all(TOOL_CALL_ID_HEADER, [])
        if len(values) != 1:
            return None
        value = values[0]
        if not isinstance(value, str) or _TOOL_CALL_ID.fullmatch(value) is None:
            return None
        return value

    def _read_json(
        self,
        handler: BaseHTTPRequestHandler,
    ) -> tuple[dict[str, Any] | None, str | None]:
        raw_length = handler.headers.get("Content-Length")
        try:
            length = int(raw_length) if raw_length is not None else -1
        except ValueError:
            length = -1
        if length < 0:
            return None, "content_length_required"
        if length > self._max_body_bytes:
            return None, "body_too_large"
        try:
            raw = handler.rfile.read(length)
        except OSError:
            return None, "body_read_failed"
        if len(raw) != length:
            return None, "body_read_failed"
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None, "json_invalid"
        if not isinstance(value, dict):
            return None, "json_object_required"
        return value, None

    def _dispatch(
        self,
        engram_id: str,
        tool_name: str,
        args: dict[str, Any],
        invocation: ToolInvocationContext,
    ) -> tuple[HTTPStatus, dict[str, Any]]:
        with self._lock:
            callback = self._dispatcher
            legacy_callback = self._legacy_dispatcher
            closed = self._closed
        if closed:
            return HTTPStatus.SERVICE_UNAVAILABLE, {"error": "gateway_closed"}
        if callback is None and legacy_callback is None:
            return HTTPStatus.SERVICE_UNAVAILABLE, {"error": "dispatcher_unavailable"}
        try:
            if callback is not None:
                raw = callback(engram_id, tool_name, dict(args), invocation)
            else:
                assert legacy_callback is not None
                raw = legacy_callback(engram_id, tool_name, dict(args))
            return HTTPStatus.OK, self._safe_dispatch_result(raw)
        except Exception:
            return HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "handler_error"}

    def _authorize(
        self,
        engram_id: str,
        tool_name: str,
        ephemeral_input: dict[str, Any],
    ) -> tuple[HTTPStatus, dict[str, Any]]:
        with self._lock:
            callback = self._authorizer
            closed = self._closed
        if closed:
            return HTTPStatus.SERVICE_UNAVAILABLE, {
                "allow": False,
                "reason": "gateway_closed",
            }
        if callback is None:
            return HTTPStatus.OK, {
                "allow": False,
                "reason": "authorization_unavailable",
            }
        try:
            raw = callback(engram_id, tool_name, dict(ephemeral_input))
            return HTTPStatus.OK, self._safe_authorize_result(raw)
        except Exception:
            return HTTPStatus.INTERNAL_SERVER_ERROR, {
                "allow": False,
                "reason": "authorization_failed",
            }

    def _cancel(
        self,
        engram_id: str,
        invocation: ToolInvocationContext,
    ) -> tuple[HTTPStatus, dict[str, Any]]:
        with self._lock:
            callback = self._canceller
            closed = self._closed
        if closed:
            return HTTPStatus.SERVICE_UNAVAILABLE, {
                "ok": False,
                "content": "The Tool Gateway is closed.",
                "data": {},
                "event_id": None,
                "error": "gateway_closed",
            }
        if callback is None:
            return HTTPStatus.SERVICE_UNAVAILABLE, {
                "ok": False,
                "content": "No cancellation broker is attached.",
                "data": {},
                "event_id": None,
                "error": "cancellation_unavailable",
            }
        try:
            raw = callback(engram_id, invocation)
            return HTTPStatus.OK, self._safe_dispatch_result(raw)
        except Exception:
            return HTTPStatus.INTERNAL_SERVER_ERROR, {
                "ok": False,
                "content": "Cancellation failed closed.",
                "data": {},
                "event_id": None,
                "error": "cancellation_failed",
            }

    @staticmethod
    def _safe_dispatch_result(raw: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(raw, Mapping) or type(raw.get("ok")) is not bool:
            return {"error": "handler_contract_invalid"}
        content = raw.get("content", "")
        data = raw.get("data", {})
        event_id = raw.get("event_id")
        if not isinstance(content, str) or len(content) > 1_000_000:
            return {"error": "handler_contract_invalid"}
        if not isinstance(data, dict):
            return {"error": "handler_contract_invalid"}
        if event_id is not None and not isinstance(event_id, str):
            return {"error": "handler_contract_invalid"}
        envelope: dict[str, Any] = {
            "ok": raw["ok"],
            "content": content,
            "data": dict(data),
            "event_id": event_id,
        }
        if raw["ok"] is False:
            # Rejected actions still need to carry their bounded state (for
            # example pending approval or an explicit unsupported adapter) to
            # Pi and the Workbench.  The callback owns only safe structured
            # data; the error code is constrained to the public vocabulary.
            envelope["error"] = _safe_reason_code(
                raw.get("error"),
                "tool_rejected",
            )
        return envelope

    @staticmethod
    def _safe_authorize_result(raw: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(raw, Mapping) or type(raw.get("allow")) is not bool:
            return {"allow": False, "reason": "authorization_contract_invalid"}
        reason = _safe_reason_code(
            raw.get("reason_code", raw.get("reason")),
            "authorization_contract_invalid",
        )
        return {
            "allow": raw["allow"],
            "reason": reason
            if reason != "authorization_contract_invalid"
            else ("allowed" if raw["allow"] else "denied"),
        }

    @staticmethod
    def _validate_tool_input(
        tool_name: str,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str | None]:
        required: dict[str, set[str]] = {
            "read": {"path"},
            "bash": {"command"},
            "edit": {"path", "edits"},
            "write": {"path", "content"},
            "pulse_task_offer_respond": {"decision", "expected_revision"},
            "pulse_task_relationship_respond": {
                "relationship_id",
                "expected_revision",
                "action",
            },
            "pulse_life_list": set(),
            "pulse_life_portfolio": set(),
            "pulse_life_concerns": set(),
            "pulse_life_hold": {"center_id", "content", "disposition"},
            "pulse_life_orientations": set(),
            "pulse_life_orient": {"center_id", "content", "state"},
            "pulse_life_create": {"kind", "title"},
            "pulse_life_update": {"center_id"},
            "pulse_life_purpose": set(),
            "pulse_life_amend_purpose": {"amendment_kind"},
            "pulse_life_roles": set(),
            "pulse_life_accept_role": {"role_label", "center_ids"},
            "pulse_life_renew_role": {"role_lease_id", "expected_role_epoch"},
            "pulse_life_release_role": {"role_lease_id", "expected_role_epoch"},
            "pulse_habitat_observe": {"organ"},
            "pulse_habitat_act": {"verb"},
            "pulse_habitat_subscribe": set(),
            "pulse_delegate": {"task"},
            "pulse_mcp_call": {"server_id", "tool_name", "arguments"},
            "pulse_task_spawn": {"task"},
            "pulse_task_wait": {"task_id"},
            "pulse_task_steer": {"task_id", "message"},
            "pulse_task_stop": {"task_id"},
        }
        optional: dict[str, set[str]] = {
            "read": {"offset", "limit"},
            "bash": {"timeout", "background"},
            "edit": set(),
            "write": set(),
            "pulse_task_offer_respond": {"response"},
            "pulse_task_relationship_respond": {"response"},
            "pulse_life_list": set(),
            "pulse_life_portfolio": {"history_limit"},
            "pulse_life_concerns": {"center_id"},
            "pulse_life_hold": {"concern_id", "revisit_after_seconds"},
            "pulse_life_orientations": {"center_id", "current_only"},
            "pulse_life_orient": {"orientation_id"},
            "pulse_life_create": {"description", "autonomy"},
            "pulse_life_update": {"title", "description", "status", "autonomy"},
            "pulse_life_purpose": {"history", "limit"},
            "pulse_life_amend_purpose": {"expected_revision", "content"},
            "pulse_life_roles": {"active_only"},
            "pulse_life_accept_role": {
                "ttl_seconds",
                "purpose_revision_id",
                "obligation",
            },
            "pulse_life_renew_role": {"ttl_seconds"},
            "pulse_life_release_role": set(),
            "pulse_habitat_observe": {"target"},
            "pulse_habitat_act": {"target", "payload"},
            "pulse_habitat_subscribe": {"channel", "center_id"},
            "pulse_delegate": {"to"},
            "pulse_mcp_call": {"timeout"},
            "pulse_task_spawn": {"timeout", "idle_timeout"},
            "pulse_task_wait": {"after_seq", "timeout"},
            "pulse_task_steer": set(),
            "pulse_task_stop": {"reason"},
        }
        unknown = set(payload).difference(required[tool_name] | optional[tool_name])
        if unknown:
            return None, "tool_schema_unknown_field"
        missing = required[tool_name].difference(payload)
        if missing:
            return None, "tool_schema_required_field"

        args = dict(payload)
        if tool_name == "read":
            path = args["path"]
            if not isinstance(path, str) or not path.strip() or len(path) > 4_096:
                return None, "tool_schema_path_invalid"
            for field_name, minimum, maximum in (("offset", 1, 10_000_000), ("limit", 1, 10_000)):
                if field_name in args:
                    value = args[field_name]
                    if (
                        isinstance(value, bool)
                        or not isinstance(value, int)
                        or value < minimum
                        or value > maximum
                    ):
                        return None, f"tool_schema_{field_name}_invalid"
        elif tool_name == "bash":
            command = args["command"]
            if (
                not isinstance(command, str)
                or not command.strip()
                or len(command) > 32_000
            ):
                return None, "tool_schema_command_invalid"
            if "timeout" in args:
                timeout = args["timeout"]
                if (
                    isinstance(timeout, bool)
                    or not isinstance(timeout, (int, float))
                    or not math.isfinite(float(timeout))
                    or not 0 < float(timeout) <= _MAX_ACTION_TIMEOUT_SECONDS
                ):
                    return None, "tool_schema_timeout_invalid"
            if "background" in args and not isinstance(args["background"], bool):
                return None, "tool_schema_background_invalid"
        elif tool_name == "edit":
            path = args["path"]
            edits = args["edits"]
            if not isinstance(path, str) or not path.strip() or len(path) > 4_096:
                return None, "tool_schema_path_invalid"
            if not isinstance(edits, list) or not edits or len(edits) > 128:
                return None, "tool_schema_edits_invalid"
            for edit in edits:
                if not isinstance(edit, dict) or set(edit) != {"oldText", "newText"}:
                    return None, "tool_schema_edit_entry_invalid"
                if (
                    not isinstance(edit["oldText"], str)
                    or not isinstance(edit["newText"], str)
                    or len(edit["oldText"]) > 1_000_000
                    or len(edit["newText"]) > 1_000_000
                ):
                    return None, "tool_schema_edit_text_invalid"
        elif tool_name == "write":
            path = args["path"]
            content = args["content"]
            if not isinstance(path, str) or not path.strip() or len(path) > 4_096:
                return None, "tool_schema_path_invalid"
            if not isinstance(content, str) or len(content) > 4_000_000:
                return None, "tool_schema_content_invalid"
        elif tool_name == "pulse_task_offer_respond":
            decision = args["decision"]
            expected_revision = args["expected_revision"]
            response = args.get("response")
            if decision not in {"accept", "refuse", "request_changes"}:
                return None, "tool_schema_decision_invalid"
            if type(expected_revision) is not int or expected_revision < 1:
                return None, "tool_schema_expected_revision_invalid"
            if response is not None and (
                not isinstance(response, str) or len(response) > 4000
            ):
                return None, "tool_schema_response_invalid"
            if decision == "request_changes" and (
                not isinstance(response, str) or not response.strip()
            ):
                return None, "task_offer_response_required"
        elif tool_name == "pulse_task_relationship_respond":
            relationship_id = args["relationship_id"]
            expected_revision = args["expected_revision"]
            action = args["action"]
            response = args.get("response")
            if (
                not isinstance(relationship_id, str)
                or not relationship_id.strip()
                or relationship_id.strip() != relationship_id
                or len(relationship_id) > 128
                or "\x00" in relationship_id
            ):
                return None, "tool_schema_relationship_id_invalid"
            if type(expected_revision) is not int or expected_revision < 1:
                return None, "tool_schema_expected_revision_invalid"
            if action not in {"pause", "request_changes", "resume", "exit"}:
                return None, "tool_schema_action_invalid"
            if response is not None and (
                not isinstance(response, str) or len(response) > 4000
            ):
                return None, "tool_schema_response_invalid"
            if action == "request_changes" and (
                not isinstance(response, str) or not response.strip()
            ):
                return None, "task_relationship_response_required"
        elif tool_name == "pulse_mcp_call":
            server_id = args["server_id"]
            remote_tool = args["tool_name"]
            arguments = args["arguments"]
            if (
                not isinstance(server_id, str)
                or not server_id.strip()
                or len(server_id) > 128
            ):
                return None, "tool_schema_server_id_invalid"
            if (
                not isinstance(remote_tool, str)
                or not remote_tool.strip()
                or len(remote_tool) > 128
            ):
                return None, "tool_schema_tool_name_invalid"
            if not isinstance(arguments, dict):
                return None, "tool_schema_arguments_object"
            try:
                argument_bytes = len(
                    json.dumps(
                        arguments,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                )
            except (TypeError, ValueError):
                return None, "tool_schema_arguments_invalid"
            if argument_bytes > 64 * 1024:
                return None, "tool_schema_arguments_too_large"
            if "timeout" in args:
                timeout = args["timeout"]
                if (
                    isinstance(timeout, bool)
                    or not isinstance(timeout, (int, float))
                    or not math.isfinite(float(timeout))
                    or not 0 < float(timeout) <= _MAX_ACTION_TIMEOUT_SECONDS
                ):
                    return None, "tool_schema_timeout_invalid"
        elif tool_name == "pulse_task_spawn":
            task = args["task"]
            if not isinstance(task, str) or not task.strip() or len(task) > 8192:
                return None, "tool_schema_task_invalid"
            for field_name in ("timeout", "idle_timeout"):
                if field_name in args:
                    value = args[field_name]
                    if (
                        isinstance(value, bool)
                        or not isinstance(value, (int, float))
                        or not math.isfinite(float(value))
                        or not 0 < float(value) <= _MAX_TASK_TIMEOUT_SECONDS
                    ):
                        return None, f"tool_schema_{field_name}_invalid"
        elif tool_name == "pulse_task_wait":
            if not isinstance(args["task_id"], str) or _TASK_ID.fullmatch(args["task_id"]) is None:
                return None, "tool_schema_task_id_invalid"
            if "after_seq" in args:
                after_seq = args["after_seq"]
                if isinstance(after_seq, bool) or not isinstance(after_seq, int) or after_seq < 0:
                    return None, "tool_schema_after_seq_invalid"
            if "timeout" in args:
                timeout = args["timeout"]
                if (
                    isinstance(timeout, bool)
                    or not isinstance(timeout, (int, float))
                    or not math.isfinite(float(timeout))
                    or not 0 <= float(timeout) <= _MAX_TASK_WAIT_SECONDS
                ):
                    return None, "tool_schema_timeout_invalid"
        elif tool_name in {"pulse_task_steer", "pulse_task_stop"}:
            if not isinstance(args["task_id"], str) or _TASK_ID.fullmatch(args["task_id"]) is None:
                return None, "tool_schema_task_id_invalid"
            text_field = "message" if tool_name == "pulse_task_steer" else "reason"
            if text_field in args:
                value = args[text_field]
                if not isinstance(value, str) or not value.strip() or len(value) > 8192:
                    return None, f"tool_schema_{text_field}_invalid"
        elif tool_name == "pulse_life_portfolio":
            if "history_limit" in args and (
                type(args["history_limit"]) is not int
                or not 1 <= args["history_limit"] <= 100
            ):
                return None, "tool_schema_history_limit_invalid"
        elif tool_name == "pulse_life_purpose":
            if "history" in args and type(args["history"]) is not bool:
                return None, "tool_schema_history_invalid"
            if "limit" in args and (
                type(args["limit"]) is not int or not 1 <= args["limit"] <= 1000
            ):
                return None, "tool_schema_limit_invalid"
        elif tool_name == "pulse_life_amend_purpose":
            amendment = args["amendment_kind"]
            if amendment not in {"establish", "amend", "withdraw"}:
                return None, "tool_schema_amendment_kind_invalid"
            expected = args.get("expected_revision")
            if expected is not None and (
                type(expected) is not int or expected < 1
            ):
                return None, "tool_schema_expected_revision_invalid"
            if amendment == "establish":
                if expected is not None:
                    return None, "tool_schema_expected_revision_invalid"
                content = args.get("content")
                if not isinstance(content, str) or not content.strip() or len(content) > 4000:
                    return None, "tool_schema_content_invalid"
            elif amendment == "amend":
                content = args.get("content")
                if expected is None:
                    return None, "tool_schema_expected_revision_required"
                if not isinstance(content, str) or not content.strip() or len(content) > 4000:
                    return None, "tool_schema_content_invalid"
            else:
                if expected is None:
                    return None, "tool_schema_expected_revision_required"
                if "content" in args:
                    return None, "tool_schema_withdraw_content_forbidden"
        elif tool_name == "pulse_life_roles":
            if "active_only" in args and type(args["active_only"]) is not bool:
                return None, "tool_schema_active_only_invalid"
        elif tool_name == "pulse_life_accept_role":
            label = args["role_label"]
            centers = args["center_ids"]
            if (
                not isinstance(label, str)
                or not label.strip()
                or len(label.encode("utf-8")) > 256
                or "\x00" in label
                or "\n" in label
                or "\r" in label
            ):
                return None, "tool_schema_role_label_invalid"
            if not isinstance(centers, list) or not 1 <= len(centers) <= 16:
                return None, "tool_schema_center_ids_invalid"
            if any(
                not isinstance(center, str)
                or not center.strip()
                or len(center) > 128
                for center in centers
            ) or len(set(centers)) != len(centers):
                return None, "tool_schema_center_ids_invalid"
            if "ttl_seconds" in args:
                ttl = args["ttl_seconds"]
                if (
                    isinstance(ttl, bool)
                    or not isinstance(ttl, (int, float))
                    or not math.isfinite(float(ttl))
                    or not 0 < float(ttl) <= 90 * 24 * 60 * 60
                ):
                    return None, "tool_schema_ttl_invalid"
            purpose_id = args.get("purpose_revision_id")
            if purpose_id is not None and (
                not isinstance(purpose_id, str)
                or not purpose_id.strip()
                or len(purpose_id) > 128
            ):
                return None, "tool_schema_purpose_revision_id_invalid"
            obligation = args.get("obligation")
            if obligation is not None:
                if not isinstance(obligation, dict):
                    return None, "tool_schema_role_obligation_invalid"
                allowed_obligation = {
                    "kind",
                    "minimum_direct_outputs",
                    "max_consecutive_coordination",
                    "accepted_output_kinds",
                }
                if set(obligation).difference(allowed_obligation):
                    return None, "tool_schema_role_obligation_invalid"
                if obligation.get("kind", "direct_output") != "direct_output":
                    return None, "tool_schema_role_obligation_invalid"
                minimum = obligation.get("minimum_direct_outputs", 1)
                maximum = obligation.get("max_consecutive_coordination", 3)
                outputs = obligation.get(
                    "accepted_output_kinds",
                    ["workspace_checkpoint", "habitat_effect"],
                )
                if type(minimum) is not int or not 1 <= minimum <= 16:
                    return None, "tool_schema_role_obligation_invalid"
                if type(maximum) is not int or not 0 <= maximum <= 64:
                    return None, "tool_schema_role_obligation_invalid"
                if (
                    not isinstance(outputs, list)
                    or not 1 <= len(outputs) <= 2
                    or len(set(outputs)) != len(outputs)
                    or any(
                        item not in {"workspace_checkpoint", "habitat_effect"}
                        for item in outputs
                    )
                ):
                    return None, "tool_schema_role_obligation_invalid"
        elif tool_name in {"pulse_life_renew_role", "pulse_life_release_role"}:
            role_id = args["role_lease_id"]
            role_epoch = args["expected_role_epoch"]
            if not isinstance(role_id, str) or not role_id.strip() or len(role_id) > 128:
                return None, "tool_schema_role_lease_id_invalid"
            if type(role_epoch) is not int or role_epoch < 1:
                return None, "tool_schema_role_epoch_invalid"
            if "ttl_seconds" in args:
                ttl = args["ttl_seconds"]
                if (
                    isinstance(ttl, bool)
                    or not isinstance(ttl, (int, float))
                    or not math.isfinite(float(ttl))
                    or not 0 < float(ttl) <= 90 * 24 * 60 * 60
                ):
                    return None, "tool_schema_ttl_invalid"
        for name in (
            "kind",
            "title",
            "center_id",
            "content",
            "disposition",
            "concern_id",
            "state",
            "orientation_id",
            "organ",
            "verb",
            "task",
            "target",
            "to",
            "channel",
            "server_id",
            "tool_name",
        ):
            if name in args and (
                not isinstance(args[name], str) or not args[name].strip()
            ):
                return None, "tool_schema_string_field"
        if "content" in args and len(args["content"]) > 4000:
            return None, "tool_schema_content_invalid"
        if tool_name == "pulse_life_orientations" and "current_only" in args:
            if type(args["current_only"]) is not bool:
                return None, "tool_schema_current_only_invalid"
        if tool_name == "pulse_life_orient":
            if args["state"] not in {"open", "resting", "closed"}:
                return None, "tool_schema_orientation_state_invalid"
        if tool_name == "pulse_life_hold":
            disposition = args.get("disposition")
            if disposition not in {"quiet", "revisit", "resolved"}:
                return None, "tool_schema_disposition_invalid"
            if disposition == "resolved" and "concern_id" not in args:
                return None, "tool_schema_concern_required"
            has_revisit = "revisit_after_seconds" in args
            if disposition == "revisit":
                if not has_revisit:
                    return None, "tool_schema_revisit_required"
                seconds = args["revisit_after_seconds"]
                if (
                    isinstance(seconds, bool)
                    or not isinstance(seconds, (int, float))
                    or not math.isfinite(float(seconds))
                    or not 0 <= float(seconds) <= 31_536_000
                ):
                    return None, "tool_schema_revisit_invalid"
            elif has_revisit:
                return None, "tool_schema_revisit_not_allowed"
        if tool_name == "pulse_life_create" and args["kind"] not in _ALLOWED_KINDS:
            return None, "tool_schema_kind_invalid"
        if "status" in args and (
            not isinstance(args["status"], str) or args["status"] not in _ALLOWED_STATUSES
        ):
            return None, "tool_schema_status_invalid"
        if "autonomy" in args:
            autonomy = args["autonomy"]
            if (
                isinstance(autonomy, bool)
                or not isinstance(autonomy, (int, float))
                or not math.isfinite(float(autonomy))
                or not 0.0 <= float(autonomy) <= 1.0
            ):
                return None, "tool_schema_autonomy_invalid"
        if "payload" in args and not isinstance(args["payload"], dict):
            return None, "tool_schema_payload_object"
        if _contains_identity_field(args):
            return None, "identity_field_not_allowed"
        return args, None

    @staticmethod
    def _write_json(
        handler: BaseHTTPRequestHandler,
        status: HTTPStatus,
        payload: Mapping[str, Any],
    ) -> None:
        try:
            encoded = json.dumps(
                dict(payload),
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError):
            status = HTTPStatus.INTERNAL_SERVER_ERROR
            encoded = b'{"error":"response_encoding_failed"}'
        handler.send_response(int(status))
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Content-Length", str(len(encoded)))
        handler.send_header("Connection", "close")
        handler.end_headers()
        try:
            handler.wfile.write(encoded)
        except OSError:
            pass
        handler.close_connection = True


def _safe_reason_code(value: Any, fallback: str) -> str:
    if isinstance(value, str) and _REASON_CODE.fullmatch(value):
        return value
    return fallback


def _contains_identity_field(value: Any) -> bool:
    if isinstance(value, dict):
        return any(
            key == "engram_id" or _contains_identity_field(child)
            for key, child in value.items()
        )
    if isinstance(value, list):
        return any(_contains_identity_field(child) for child in value)
    return False

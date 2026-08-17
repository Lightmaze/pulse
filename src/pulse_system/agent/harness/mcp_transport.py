"""Bounded stdio MCP JSON-RPC transport.

This module is an execution adapter for one explicitly configured MCP server.
It deliberately does not discover, enable, or authorize a registry descriptor,
and it does not claim that a normal subprocess is an operating-system
sandbox.  The transport evidence and execution-safety evidence are separate
values so a real protocol exchange cannot be mistaken for a security gate.

The adapter uses the MCP 2025-06-18 lifecycle and the JSON-RPC 2.0 newline
framing used by stdio MCP servers.  Requests are serialized per transport;
request IDs are still checked on every response.  Serialization keeps the
minimal adapter bounded while the reader thread continues to drain stdout and
stderr, preventing a child process from deadlocking on a full pipe.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import queue
import re
import subprocess
import threading
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Literal, TypedDict

from pulse_system.core.runtime.publication import (
    RuntimePublicationError,
    RuntimePublicationPermit,
)

from .process_containment import (
    ContainedProcessOwner,
    PhysicalProcessObservation,
    ProcessTreeEvidence,
    spawn_contained_process,
)

__all__ = [
    "EXECUTION_SAFETY_UNVERIFIED",
    "LIVE_MCP_TRANSPORT",
    "MCPCapabilitySnapshot",
    "MCPEvidence",
    "MCPError",
    "MCPProcessError",
    "MCPProtocolError",
    "MCPRemoteError",
    "MCPStdioTransport",
    "MCPTransportCloseSummary",
    "MCPTimeoutError",
    "MCPCancelledError",
    "MCPToolCollisionError",
    "MCPToolSnapshot",
]


LIVE_MCP_TRANSPORT = "LIVE_MCP_TRANSPORT"
EXECUTION_SAFETY_UNVERIFIED = "EXECUTION_SAFETY_UNVERIFIED"

_DEFAULT_PROTOCOL_VERSION = "2025-06-18"
_DEFAULT_SUPPORTED_PROTOCOL_VERSIONS = (
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
)
_CLIENT_NAME = "pulse-mcp-stdio"
_CLIENT_VERSION = "0.1"
_MAX_ARGS = 64
_MAX_ARG_BYTES = 8 * 1024
_MAX_ENV_KEYS = 64
_MAX_ENV_VALUE_BYTES = 8 * 1024
_MAX_PROTOCOL_VERSION_BYTES = 64
_MAX_TOOL_NAME_BYTES = 128
_MAX_TEXT_BYTES = 512
_MAX_ID_IGNORED = 128
_MAX_PAGINATION_PAGES = 64
_MAX_JSON_DEPTH = 8
_MAX_JSON_ITEMS = 256
_MIN_MESSAGE_BYTES = 128
_MAX_MESSAGE_BYTES = 1024 * 1024
_MIN_TIMEOUT_SECONDS = 0.01
_MAX_TIMEOUT_SECONDS = 300.0
_CANCEL_GRACE_SECONDS = 0.05
_WRITE_QUEUE_CAPACITY = 2
_WRITE_CHUNK_BYTES = 4096
_WRITE_POLL_SECONDS = 0.01
_TOOL_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_PROTOCOL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


class MCPError(RuntimeError):
    """Base error with a stable, non-payload error code."""

    def __init__(self, code: str) -> None:
        if not isinstance(code, str) or not code or len(code) > 96:
            raise ValueError("MCP error code must be bounded and non-empty")
        self.code = code
        super().__init__(code)


class MCPProtocolError(MCPError):
    """The child violated the bounded JSON-RPC/MCP contract."""


class MCPProcessError(MCPError):
    """The child process or its pipes became unavailable."""


class MCPTimeoutError(MCPError):
    """A request exceeded its bounded deadline."""


class MCPCancelledError(MCPError):
    """The caller cancelled a request."""


class MCPToolCollisionError(MCPProtocolError):
    """The child advertised a duplicate or reserved tool name."""


class MCPRemoteError(MCPError):
    """A JSON-RPC error response returned by the MCP server."""

    def __init__(self, rpc_code: int | None) -> None:
        super().__init__("mcp_remote_error")
        self.rpc_code = rpc_code


class _TransportPhase(str, Enum):
    CREATED = "CREATED"
    STARTING = "STARTING"
    READY = "READY"
    BROKEN = "BROKEN"
    CLOSED = "CLOSED"


class MCPTransportCloseSummary(TypedDict):
    """Payload-free physical owner evidence for one stdio transport."""

    active_before: int
    unresolved: int
    transport_owners_unresolved: int
    reader_owners_unresolved: int
    process_roots_observed: int
    process_root_owners_unresolved: int
    owner_joined: bool
    process_tree_state: Literal[
        "not_applicable",
        "empty_verified",
        "root_exit_only",
        "unknown",
    ]


@dataclass(frozen=True, slots=True)
class MCPEvidence:
    """Evidence axes for one transport observation.

    ``transport`` becomes ``LIVE_MCP_TRANSPORT`` only after a real child has
    completed initialize/initialized.  ``execution_safety`` remains
    ``EXECUTION_SAFETY_UNVERIFIED`` because this class uses an ordinary
    subprocess and supplies no OS sandbox proof.
    """

    transport: str = LIVE_MCP_TRANSPORT
    execution_safety: str = EXECUTION_SAFETY_UNVERIFIED

    def __post_init__(self) -> None:
        if self.transport != LIVE_MCP_TRANSPORT:
            raise ValueError("unsupported MCP transport evidence")
        if self.execution_safety != EXECUTION_SAFETY_UNVERIFIED:
            raise ValueError("unsupported MCP execution-safety evidence")

    def to_dict(self) -> dict[str, str]:
        return {
            "transport": self.transport,
            "execution_safety": self.execution_safety,
        }


@dataclass(frozen=True, slots=True)
class MCPToolSnapshot:
    """Bounded, credential-free metadata for one discovered MCP tool."""

    name: str
    description: str
    input_schema: Mapping[str, Any]
    input_schema_digest: str
    title: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "inputSchema": _copy_json(self.input_schema),
            "input_schema_digest": self.input_schema_digest,
        }
        if self.title is not None:
            result["title"] = self.title
        return result


@dataclass(frozen=True, slots=True)
class MCPCapabilitySnapshot:
    """The live server capability snapshot after a successful tools/list."""

    protocol_version: str
    server_name: str
    server_version: str
    server_capabilities: Mapping[str, Any]
    tools: tuple[MCPToolSnapshot, ...]
    evidence: MCPEvidence

    def to_dict(self) -> dict[str, Any]:
        return {
            "protocol_version": self.protocol_version,
            "server_info": {
                "name": self.server_name,
                "version": self.server_version,
            },
            "server_capabilities": _copy_json(self.server_capabilities),
            "tools": [tool.to_dict() for tool in self.tools],
            "evidence": self.evidence.to_dict(),
        }

    @property
    def tool_names(self) -> tuple[str, ...]:
        return tuple(tool.name for tool in self.tools)


@dataclass(frozen=True, slots=True)
class _Inbound:
    message: dict[str, Any] | None = None
    error: MCPError | None = None


@dataclass(slots=True)
class _OutboundWrite:
    """One bounded frame owned by the transport's single writer thread."""

    payload: bytes
    deadline: float
    completed: threading.Event = field(default_factory=threading.Event)
    abandoned: threading.Event = field(default_factory=threading.Event)
    error: MCPError | None = None


def _bounded_timeout(value: Any, *, field_name: str, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a number")
    result = float(value)
    if result < _MIN_TIMEOUT_SECONDS or result > maximum:
        raise ValueError(f"{field_name} is outside the bounded range")
    return result


def _bounded_size(value: Any, *, field_name: str) -> int:
    if type(value) is not int or not _MIN_MESSAGE_BYTES <= value <= _MAX_MESSAGE_BYTES:
        raise ValueError(f"{field_name} is outside the bounded range")
    return value


def _bounded_text(value: Any, *, field_name: str, maximum: int = _MAX_TEXT_BYTES) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise MCPProtocolError(f"{field_name}_invalid")
    if len(value.encode("utf-8", errors="strict")) > maximum:
        raise MCPProtocolError(f"{field_name}_too_large")
    return value


def _safe_protocol_version(value: Any, *, field_name: str = "protocol_version") -> str:
    if not isinstance(value, str) or _PROTOCOL_RE.fullmatch(value) is None:
        raise MCPProtocolError(f"{field_name}_invalid")
    if len(value.encode("ascii", errors="strict")) > _MAX_PROTOCOL_VERSION_BYTES:
        raise MCPProtocolError(f"{field_name}_too_large")
    return value


def _safe_tool_name(value: Any, *, field_name: str = "tool_name") -> str:
    if not isinstance(value, str) or _TOOL_NAME_RE.fullmatch(value) is None:
        raise MCPProtocolError(f"{field_name}_invalid")
    if len(value.encode("ascii", errors="strict")) > _MAX_TOOL_NAME_BYTES:
        raise MCPProtocolError(f"{field_name}_too_large")
    return value


def _safe_env_name(value: Any) -> str:
    if not isinstance(value, str) or _ENV_NAME_RE.fullmatch(value) is None:
        raise ValueError("environment key is invalid")
    return value


def _copy_json(value: Any) -> Any:
    """Return a detached JSON-safe copy of already bounded data."""

    if isinstance(value, dict):
        return {str(key): _copy_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_json(item) for item in value]
    return value


def _bound_json(value: Any, *, depth: int = 0, field_name: str = "payload") -> Any:
    """Validate JSON values without retaining unbounded child payloads."""

    if depth > _MAX_JSON_DEPTH:
        raise MCPProtocolError(f"{field_name}_depth_exceeded")
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        if "\x00" in value or len(value.encode("utf-8")) > _MAX_TEXT_BYTES:
            raise MCPProtocolError(f"{field_name}_text_too_large")
        return value
    if isinstance(value, Mapping):
        if len(value) > _MAX_JSON_ITEMS:
            raise MCPProtocolError(f"{field_name}_object_too_large")
        bounded: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key or len(key) > 128 or "\x00" in key:
                raise MCPProtocolError(f"{field_name}_key_invalid")
            bounded[key] = _bound_json(item, depth=depth + 1, field_name=field_name)
        return bounded
    if isinstance(value, (list, tuple)):
        if len(value) > _MAX_JSON_ITEMS:
            raise MCPProtocolError(f"{field_name}_array_too_large")
        return [
            _bound_json(item, depth=depth + 1, field_name=field_name)
            for item in value
        ]
    raise MCPProtocolError(f"{field_name}_type_invalid")


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _request_id(value: Any) -> bool:
    return (isinstance(value, (str, int)) and not isinstance(value, bool)) or value is None


def _is_cancelled(value: Any) -> bool:
    if value is None:
        return False
    method = getattr(value, "is_set", None)
    if callable(method):
        try:
            return bool(method())
        except Exception:
            return True
    return bool(getattr(value, "cancelled", False) or getattr(value, "aborted", False))


def _absolute_deadline(value: float | None, *, default_timeout: float) -> float:
    if value is None:
        return time.monotonic() + default_timeout
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("deadline must be a finite monotonic timestamp")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("deadline must be a finite monotonic timestamp")
    return result


class MCPStdioTransport:
    """A real, bounded, single-child stdio MCP client.

    ``env`` is never merged with the parent environment.  It is accepted only
    when each key is present in ``env_allowlist``.  The caller must provide an
    absolute existing ``cwd`` and an explicit argv vector; shell execution is
    never used.
    """

    def __init__(
        self,
        *,
        argv: Sequence[str],
        cwd: str | Path,
        env: Mapping[str, str] | None = None,
        env_allowlist: Iterable[str] = (),
        reserved_tool_names: Iterable[str] = (),
        protocol_version: str = _DEFAULT_PROTOCOL_VERSION,
        supported_protocol_versions: Iterable[str] | None = None,
        request_timeout: float = 10.0,
        max_timeout: float = 60.0,
        max_stdout_bytes: int = 256 * 1024,
        max_stderr_bytes: int = 64 * 1024,
        max_message_bytes: int = 64 * 1024,
        max_tools: int = 256,
        max_pagination_pages: int = _MAX_PAGINATION_PAGES,
        shutdown_timeout: float = 2.0,
        publication_permit: RuntimePublicationPermit | None = None,
    ) -> None:
        if (
            publication_permit is not None
            and type(publication_permit) is not RuntimePublicationPermit
        ):
            raise TypeError(
                "publication_permit must be a RuntimePublicationPermit or None"
            )
        if isinstance(argv, (str, bytes, bytearray)) or not isinstance(argv, Sequence):
            raise ValueError("argv must be an explicit sequence")
        if not 1 <= len(argv) <= _MAX_ARGS:
            raise ValueError("argv must contain between 1 and 64 entries")
        normalized_argv: list[str] = []
        for item in argv:
            if not isinstance(item, str) or not item or "\x00" in item:
                raise ValueError("argv entries must be non-empty strings without NUL")
            if len(item.encode("utf-8")) > _MAX_ARG_BYTES:
                raise ValueError("argv entry is too large")
            normalized_argv.append(item)
        self._argv = tuple(normalized_argv)

        candidate_cwd = Path(cwd)
        if not candidate_cwd.is_absolute():
            raise ValueError("cwd must be an absolute path")
        try:
            resolved_cwd = candidate_cwd.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise ValueError("cwd must resolve to an existing directory") from exc
        if not resolved_cwd.is_dir():
            raise ValueError("cwd must be a directory")
        self._cwd = resolved_cwd

        allowlist: dict[str, str] = {}
        for raw_name in env_allowlist:
            name = _safe_env_name(raw_name)
            key = name.upper() if os.name == "nt" else name
            if key in allowlist and allowlist[key] != name:
                raise ValueError("environment allowlist contains a case collision")
            allowlist[key] = name
        if len(allowlist) > _MAX_ENV_KEYS:
            raise ValueError("environment allowlist is too large")
        provided_env: dict[str, str] = {}
        for raw_name, raw_value in (env or {}).items():
            name = _safe_env_name(raw_name)
            key = name.upper() if os.name == "nt" else name
            if key not in allowlist:
                raise ValueError("environment key is not in the explicit allowlist")
            if not isinstance(raw_value, str) or "\x00" in raw_value:
                raise ValueError("environment values must be strings without NUL")
            if len(raw_value.encode("utf-8")) > _MAX_ENV_VALUE_BYTES:
                raise ValueError("environment value is too large")
            provided_env[allowlist[key]] = raw_value
        self._env = dict(provided_env)
        self._env_allowlist = tuple(sorted(allowlist.values()))

        reserved: set[str] = set()
        for raw_name in reserved_tool_names:
            reserved.add(_safe_tool_name(raw_name, field_name="reserved_tool_name"))
        self._reserved_tool_names = frozenset(reserved)

        self._protocol_version = _safe_protocol_version(
            protocol_version, field_name="protocol_version"
        )
        supported = tuple(
            dict.fromkeys(
                _safe_protocol_version(item, field_name="supported_protocol_version")
                for item in (
                    supported_protocol_versions
                    if supported_protocol_versions is not None
                    else _DEFAULT_SUPPORTED_PROTOCOL_VERSIONS
                )
            )
        )
        if self._protocol_version not in supported:
            raise ValueError("protocol_version must be in supported_protocol_versions")
        self._supported_protocol_versions = frozenset(supported)

        self._max_timeout = _bounded_timeout(
            max_timeout, field_name="max_timeout", maximum=_MAX_TIMEOUT_SECONDS
        )
        self._request_timeout = _bounded_timeout(
            request_timeout, field_name="request_timeout", maximum=self._max_timeout
        )
        self._shutdown_timeout = _bounded_timeout(
            shutdown_timeout, field_name="shutdown_timeout", maximum=_MAX_TIMEOUT_SECONDS
        )
        self._max_stdout_bytes = _bounded_size(max_stdout_bytes, field_name="max_stdout_bytes")
        self._max_stderr_bytes = _bounded_size(max_stderr_bytes, field_name="max_stderr_bytes")
        self._max_message_bytes = _bounded_size(
            max_message_bytes, field_name="max_message_bytes"
        )
        if self._max_message_bytes > self._max_stdout_bytes:
            raise ValueError("max_message_bytes cannot exceed max_stdout_bytes")
        if type(max_tools) is not int or not 1 <= max_tools <= _MAX_JSON_ITEMS:
            raise ValueError("max_tools is outside the bounded range")
        if type(max_pagination_pages) is not int or not 1 <= max_pagination_pages <= _MAX_PAGINATION_PAGES:
            raise ValueError("max_pagination_pages is outside the bounded range")
        self._max_tools = max_tools
        self._max_pagination_pages = max_pagination_pages
        self._publication_permit = publication_permit

        self._phase = _TransportPhase.CREATED
        self._process: subprocess.Popen[bytes] | None = None
        self._process_owner: ContainedProcessOwner | None = None
        self._process_was_spawned = False
        self._provisional_owner_token = uuid.uuid4().hex
        self._stdout_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._writer_thread: threading.Thread | None = None
        self._stdin_stream: Any = None
        self._inbound: queue.Queue[_Inbound] = queue.Queue()
        self._outbound: queue.Queue[_OutboundWrite] = queue.Queue(
            maxsize=_WRITE_QUEUE_CAPACITY
        )
        self._state_lock = threading.RLock()
        self._stdin_io_lock = threading.Lock()
        self._request_lock = threading.Lock()
        self._next_request_id = 1
        self._ignored_ids: list[int] = []
        self._fatal_error: MCPError | None = None
        self._stderr_bytes_seen = 0
        self._snapshot: MCPCapabilitySnapshot | None = None
        self._transport_live = False
        self._close_requested = threading.Event()
        self._stdin_close_requested = threading.Event()
        self._owner_threads: dict[int, tuple[threading.Thread, int]] = {}
        self._owner_published_callback: (
            Callable[[MCPStdioTransport, ContainedProcessOwner], None] | None
        ) = None
        self._terminal_callback: (
            Callable[[MCPStdioTransport, str, float | None], None] | None
        ) = None
        self._terminal_notified = False

    @property
    def argv(self) -> tuple[str, ...]:
        return self._argv

    @property
    def cwd(self) -> Path:
        return self._cwd

    @property
    def env_allowlist(self) -> tuple[str, ...]:
        return self._env_allowlist

    @property
    def phase(self) -> str:
        with self._state_lock:
            return self._phase.value

    @property
    def pid(self) -> int | None:
        with self._state_lock:
            return None if self._process is None else self._process.pid

    @property
    def physical_owner(self) -> ContainedProcessOwner | None:
        """Return the exact retained containment owner, never a PID lookup."""

        with self._state_lock:
            return self._process_owner

    @property
    def provisional_owner_token(self) -> str:
        """Identity for a transport that has not published a process owner yet."""

        return self._provisional_owner_token

    @property
    def is_running(self) -> bool:
        with self._state_lock:
            return self._process is not None and self._process.poll() is None

    @property
    def stderr_bytes_seen(self) -> int:
        with self._state_lock:
            return self._stderr_bytes_seen

    @property
    def capability_snapshot(self) -> MCPCapabilitySnapshot | None:
        with self._state_lock:
            return self._snapshot

    @property
    def evidence(self) -> MCPEvidence | None:
        with self._state_lock:
            return MCPEvidence() if self._transport_live else None

    def bind_runtime_owner_callbacks(
        self,
        *,
        owner_published: Callable[
            [MCPStdioTransport, ContainedProcessOwner],
            None,
        ],
        terminal: Callable[[MCPStdioTransport, str, float | None], None],
    ) -> None:
        """Bind the runtime's exact-owner publication and retained-claim seams."""

        if not callable(owner_published) or not callable(terminal):
            raise TypeError("runtime owner callbacks must be callable")
        with self._state_lock:
            if self._phase is not _TransportPhase.CREATED:
                raise MCPProcessError("runtime_callbacks_bind_too_late")
            if (
                self._owner_published_callback is not None
                or self._terminal_callback is not None
            ):
                raise MCPProcessError("runtime_callbacks_already_bound")
            self._owner_published_callback = owner_published
            self._terminal_callback = terminal

    def start(
        self,
        *,
        cancel_event: Any = None,
        deadline: float | None = None,
    ) -> None:
        """Spawn the child and complete initialize/initialized."""

        absolute = _absolute_deadline(
            deadline,
            default_timeout=self._request_timeout,
        )

        self._enter_transport_owner()
        try:
            with self._state_lock:
                if self._phase is _TransportPhase.READY:
                    return
                if self._phase is _TransportPhase.CLOSED:
                    raise MCPProcessError("transport_closed")
                if self._phase is _TransportPhase.BROKEN:
                    raise self._fatal_error or MCPProcessError("transport_broken")
                if self._phase is not _TransportPhase.CREATED:
                    raise MCPProcessError("transport_start_in_progress")
                if self._close_requested.is_set() or _is_cancelled(cancel_event):
                    self._phase = _TransportPhase.CLOSED
                    raise MCPCancelledError("request_cancelled")
                self._phase = _TransportPhase.STARTING
            self._spawn(cancel_event=cancel_event, deadline=absolute)
            result = self._request(
                "initialize",
                {
                    "protocolVersion": self._protocol_version,
                    "capabilities": {},
                    "clientInfo": {
                        "name": _CLIENT_NAME,
                        "version": _CLIENT_VERSION,
                    },
                },
                allow_starting=True,
                cancel_event=cancel_event,
                deadline=absolute,
            )
            self._accept_initialize(result)
            self._send_notification(
                "notifications/initialized",
                {},
                deadline=absolute,
            )
            with self._state_lock:
                if (
                    self._phase is not _TransportPhase.STARTING
                    or self._close_requested.is_set()
                    or _is_cancelled(cancel_event)
                ):
                    raise MCPCancelledError("request_cancelled")
                self._transport_live = True
                self._phase = _TransportPhase.READY
        except MCPError:
            self._fail_closed("initialize_failed", deadline=absolute)
            raise
        except (OSError, ValueError, TypeError) as exc:
            self._fail_closed("initialize_failed", deadline=absolute)
            raise MCPProcessError("initialize_failed") from exc
        finally:
            self._leave_transport_owner()

    def list_tools(self, *, deadline: float | None = None) -> MCPCapabilitySnapshot:
        """Discover all bounded tools and create an immutable capability view."""

        absolute = _absolute_deadline(
            deadline,
            default_timeout=self._request_timeout,
        )

        self._ensure_ready()
        self._require_server_capability("tools")
        tools: list[MCPToolSnapshot] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        for _page in range(self._max_pagination_pages):
            params: dict[str, Any] = {} if cursor is None else {"cursor": cursor}
            result = self._request("tools/list", params, deadline=absolute)
            if not isinstance(result, Mapping):
                self._fail_closed("tools_list_result_invalid", deadline=absolute)
                raise MCPProtocolError("tools_list_result_invalid")
            raw_tools = result.get("tools")
            if not isinstance(raw_tools, list):
                self._fail_closed("tools_list_tools_invalid", deadline=absolute)
                raise MCPProtocolError("tools_list_tools_invalid")
            for raw_tool in raw_tools:
                try:
                    tool = self._parse_tool(raw_tool)
                except MCPError:
                    self._fail_closed("tools_list_tool_invalid", deadline=absolute)
                    raise
                if tool.name in {item.name for item in tools}:
                    self._fail_closed("tool_name_collision", deadline=absolute)
                    raise MCPToolCollisionError("tool_name_collision")
                if tool.name in self._reserved_tool_names:
                    self._fail_closed("tool_name_collision", deadline=absolute)
                    raise MCPToolCollisionError("tool_name_collision")
                tools.append(tool)
                if len(tools) > self._max_tools:
                    self._fail_closed("tools_list_limit_exceeded", deadline=absolute)
                    raise MCPProtocolError("tools_list_limit_exceeded")

            raw_next = result.get("nextCursor")
            if raw_next is None or raw_next == "":
                break
            try:
                next_cursor = _bounded_text(
                    raw_next, field_name="next_cursor", maximum=_MAX_TEXT_BYTES
                )
            except MCPError:
                self._fail_closed("tools_list_cursor_invalid", deadline=absolute)
                raise
            if next_cursor in seen_cursors or next_cursor == cursor:
                self._fail_closed("tools_list_cursor_repeated", deadline=absolute)
                raise MCPProtocolError("tools_list_cursor_repeated")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        else:
            self._fail_closed("tools_list_pagination_limit", deadline=absolute)
            raise MCPProtocolError("tools_list_pagination_limit")

        with self._state_lock:
            if self._phase is not _TransportPhase.READY:
                raise MCPProcessError("transport_not_ready")
            snapshot = MCPCapabilitySnapshot(
                protocol_version=self._server_protocol_version,
                server_name=self._server_name,
                server_version=self._server_version,
                server_capabilities=_copy_json(self._server_capabilities),
                tools=tuple(tools),
                evidence=MCPEvidence(),
            )
            self._snapshot = snapshot
            return snapshot

    def call_tool(
        self,
        name: str,
        arguments: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
        deadline: float | None = None,
        cancel_event: Any = None,
    ) -> dict[str, Any]:
        """Call one previously discovered tool with bounded arguments."""

        absolute = self._request_deadline(timeout=timeout, deadline=deadline)

        self._ensure_ready()
        self._require_server_capability("tools")
        tool_name = _safe_tool_name(name)
        snapshot = self.capability_snapshot
        if snapshot is None or tool_name not in snapshot.tool_names:
            raise MCPProtocolError("tool_not_discovered")
        if arguments is None:
            bounded_arguments: dict[str, Any] = {}
        elif isinstance(arguments, Mapping):
            bounded_arguments = _bound_json(
                arguments, field_name="tool_arguments"
            )
            if not isinstance(bounded_arguments, dict):
                raise MCPProtocolError("tool_arguments_invalid")
        else:
            raise MCPProtocolError("tool_arguments_invalid")
        result = self._request(
            "tools/call",
            {"name": tool_name, "arguments": bounded_arguments},
            deadline=absolute,
            cancel_event=cancel_event,
        )
        if not isinstance(result, Mapping):
            self._fail_closed("tool_result_invalid", deadline=absolute)
            raise MCPProtocolError("tool_result_invalid")
        bounded_result = _bound_json(result, field_name="tool_result")
        if not isinstance(bounded_result, dict):
            self._fail_closed("tool_result_invalid", deadline=absolute)
            raise MCPProtocolError("tool_result_invalid")
        return bounded_result

    def request(
        self,
        method: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
        deadline: float | None = None,
        cancel_event: Any = None,
    ) -> Any:
        """Send a bounded JSON-RPC request after initialization."""

        absolute = self._request_deadline(timeout=timeout, deadline=deadline)

        self._ensure_ready()
        if not isinstance(method, str) or not method or len(method) > 128:
            raise MCPProtocolError("method_invalid")
        bounded_params = {} if params is None else _bound_json(params, field_name="params")
        if not isinstance(bounded_params, dict):
            raise MCPProtocolError("params_invalid")
        return self._request(
            method,
            bounded_params,
            deadline=absolute,
            cancel_event=cancel_event,
        )

    def signal_close(self) -> None:
        """Broadcast cancellation without waiting for any transport owner."""

        self._signal_close("transport_closed")

    def _signal_close(self, error_code: str) -> None:
        """Publish one non-waiting close edge with a stable caller reason."""

        with self._state_lock:
            first_signal = not self._close_requested.is_set()
            self._close_requested.set()
            self._stdin_close_requested.set()
            self._phase = _TransportPhase.CLOSED
        if first_signal:
            self._inbound.put(_Inbound(error=MCPCancelledError(error_code)))
        # Closing the exact retained pipe is the portable stdio shutdown edge.
        # The non-blocking attempt gives a normal POSIX server EOF immediately;
        # if the writer currently owns the pipe, that sole writer observes the
        # close request and performs the same exact close between write chunks.
        self._close_stdin_pipe(deadline=None)

    def close(self, *, deadline: float | None = None) -> MCPTransportCloseSummary:
        """Broadcast, then observe owners under one absolute monotonic deadline.

        A returned ``owner_joined`` value is physical evidence, not an inference
        from having sent cancellation.  ``empty_verified`` can only flow from
        the exact shared containment witness retained by this transport.
        """

        absolute = _absolute_deadline(
            deadline,
            default_timeout=self._shutdown_timeout,
        )
        current = threading.current_thread()
        with self._state_lock:
            owner_threads = self._owner_thread_snapshot_locked()
            reader_threads = self._reader_thread_snapshot_locked()
            process_owner = self._process_owner
            process = None if process_owner is None else process_owner.process
            process_live = process is not None and process.poll() is None
            active_before = (
                sum(thread.is_alive() for thread in owner_threads)
                + sum(thread.is_alive() for thread in reader_threads)
                + int(process_live)
            )

        self.signal_close()

        # Interrupt physical I/O before waiting for any request/writer owner.
        # Windows uses the exact retained Job witness; POSIX closes stdin and
        # remains observation-only because numeric-PID signalling is forbidden.
        physical_observation: PhysicalProcessObservation | None = None
        with self._state_lock:
            process_owner = self._process_owner
        if process_owner is not None:
            try:
                physical_observation = process_owner.terminate_tree(absolute)
            except Exception:
                physical_observation = None
        self._close_stdin_pipe(deadline=absolute)

        # A start owner admitted before close may still be between contained
        # spawn and exact-owner publication.  Its own revoked-start path also
        # terminates through that exact owner; joining remains strictly after
        # the initial physical interrupt.
        for thread in owner_threads:
            if thread is current or not thread.is_alive():
                continue
            remaining = absolute - time.monotonic()
            if remaining <= 0:
                break
            thread.join(timeout=remaining)

        with self._state_lock:
            process_owner = self._process_owner
        if process_owner is not None:
            try:
                physical_observation = process_owner.terminate_tree(absolute)
            except Exception:
                physical_observation = None
        self._close_stdin_pipe(deadline=absolute)

        # A revoked start can publish its exact process owner after the first
        # reader census, so there may be no stdout/stderr thread whose join
        # naturally waits for cooperative EOF shutdown.  Waiting on the
        # retained Popen is identity-safe (it sends no recyclable-PID signal)
        # and gives a normal POSIX child the same bounded convergence window.
        with self._state_lock:
            process_owner = self._process_owner
            process = None if process_owner is None else process_owner.process
        if process is not None and process.poll() is None:
            remaining = absolute - time.monotonic()
            if remaining > 0:
                try:
                    process.wait(timeout=remaining)
                except (OSError, subprocess.TimeoutExpired):
                    pass

        with self._state_lock:
            owner_threads = self._owner_thread_snapshot_locked()
            reader_threads = self._reader_thread_snapshot_locked()
        for thread in reader_threads:
            if thread is current or not thread.is_alive():
                continue
            remaining = absolute - time.monotonic()
            if remaining <= 0:
                break
            thread.join(timeout=remaining)

        with self._state_lock:
            owner_threads = self._owner_thread_snapshot_locked()
            reader_threads = self._reader_thread_snapshot_locked()
            process_owner = self._process_owner
            process = None if process_owner is None else process_owner.process
            process_was_spawned = self._process_was_spawned

        if process_owner is not None:
            try:
                physical_observation = process_owner.observe()
            except Exception:
                physical_observation = None

        transport_unresolved = sum(
            thread is current or thread.is_alive()
            for thread in owner_threads
        )
        reader_unresolved = sum(thread.is_alive() for thread in reader_threads)
        process_unresolved = int(
            process is not None
            and (
                physical_observation is None
                or not physical_observation.root_exited
                or (
                    physical_observation.tree_state
                    is ProcessTreeEvidence.EMPTY_VERIFIED
                    and not physical_observation.resource_converged
                )
            )
        )
        if not process_was_spawned:
            process_tree: Literal[
                "not_applicable",
                "empty_verified",
                "root_exit_only",
                "unknown",
            ] = "not_applicable"
        elif physical_observation is None:
            process_tree = "unknown"
        else:
            process_tree = physical_observation.tree_state.value
        unresolved = transport_unresolved + reader_unresolved + process_unresolved
        return {
            "active_before": active_before,
            "unresolved": unresolved,
            "transport_owners_unresolved": transport_unresolved,
            "reader_owners_unresolved": reader_unresolved,
            "process_roots_observed": int(process_was_spawned),
            "process_root_owners_unresolved": process_unresolved,
            "owner_joined": unresolved == 0,
            "process_tree_state": process_tree,
        }

    def __enter__(self) -> "MCPStdioTransport":
        self.start()
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> None:
        self.close()

    def _spawn(
        self,
        *,
        cancel_event: Any = None,
        deadline: float,
    ) -> None:
        environment = dict(self._env)
        with self._state_lock:
            if self._phase is not _TransportPhase.STARTING or self._close_requested.is_set():
                raise MCPProcessError("transport_closed")
            if _is_cancelled(cancel_event):
                raise MCPCancelledError("request_cancelled")
        guard = (
            nullcontext()
            if self._publication_permit is None
            else self._publication_permit.transaction_guard()
        )
        try:
            with guard:
                with self._state_lock:
                    if (
                        self._phase is not _TransportPhase.STARTING
                        or self._close_requested.is_set()
                    ):
                        raise MCPProcessError("transport_closed")
                    if _is_cancelled(cancel_event):
                        raise MCPCancelledError("request_cancelled")
                process_owner = spawn_contained_process(
                    list(self._argv),
                    cwd=str(self._cwd),
                    env=environment,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    creationflags=(
                        subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
                    ),
                )
                process = process_owner.process
                with self._state_lock:
                    self._process = process
                    self._process_owner = process_owner
                    self._process_was_spawned = True
                    owner_published = self._owner_published_callback
                    cancelled = (
                        self._phase is not _TransportPhase.STARTING
                        or self._close_requested.is_set()
                        or _is_cancelled(cancel_event)
                    )
                if owner_published is not None:
                    try:
                        owner_published(self, process_owner)
                    except Exception as exc:
                        process_owner.terminate_tree(deadline)
                        with self._state_lock:
                            self._phase = _TransportPhase.BROKEN
                            self._fatal_error = MCPProcessError(
                                "owner_publication_failed"
                            )
                        raise MCPProcessError(
                            "owner_publication_failed"
                        ) from exc
                with self._state_lock:
                    cancelled = cancelled or (
                        self._phase is not _TransportPhase.STARTING
                        or self._close_requested.is_set()
                        or _is_cancelled(cancel_event)
                    )
        except RuntimePublicationError as exc:
            with self._state_lock:
                self._phase = _TransportPhase.BROKEN
                self._fatal_error = MCPProcessError(exc.code)
            raise MCPProcessError(exc.code) from None
        except (OSError, ValueError) as exc:
            with self._state_lock:
                self._phase = _TransportPhase.BROKEN
                self._fatal_error = MCPProcessError("spawn_failed")
            raise MCPProcessError("spawn_failed") from exc
        assert process.stdin is not None
        try:
            stdin_stream = self._prepare_stdin_stream(process)
        except (OSError, ValueError, AttributeError) as exc:
            try:
                process_owner.terminate_tree(deadline)
            except Exception:
                pass
            raise MCPProcessError("stdin_owner_setup_failed") from exc
        with self._state_lock:
            self._stdin_stream = stdin_stream
        if cancelled:
            self._stdin_close_requested.set()
            self._close_stdin_pipe(deadline=deadline)
            process_owner.terminate_tree(deadline)
            raise MCPProcessError("transport_closed")
        assert process.stdout is not None and process.stderr is not None
        self._writer_thread = threading.Thread(
            target=self._write_stdin,
            args=(stdin_stream,),
            name="pulse-mcp-writer",
            daemon=True,
        )
        self._stdout_thread = threading.Thread(
            target=self._read_stdout,
            args=(process.stdout,),
            name="pulse-mcp-stdout",
            daemon=True,
        )
        self._stderr_thread = threading.Thread(
            target=self._read_stderr,
            args=(process.stderr,),
            name="pulse-mcp-stderr",
            daemon=True,
        )
        try:
            self._writer_thread.start()
            self._stdout_thread.start()
            self._stderr_thread.start()
        except RuntimeError as exc:
            # The contained process has already been published to Runtime.
            # Convert a partial Thread.start transaction into the canonical
            # MCP error path so start() invokes the terminal callback and the
            # exact transport enters the retained-owner registry before the
            # connect owner unwinds.  Started siblings remain visible to
            # close(); non-started Thread objects are never joined.
            error = MCPProcessError("io_thread_start_failed")
            with self._state_lock:
                self._phase = _TransportPhase.BROKEN
                if self._fatal_error is None:
                    self._fatal_error = error
            raise error from exc

    @staticmethod
    def _prepare_stdin_stream(process: subprocess.Popen[bytes]) -> Any:
        """Adopt the exact stdin pipe as an unbuffered writer-owned stream."""

        stream = process.stdin
        if stream is None:
            raise OSError("stdin_pipe_unavailable")
        detach = getattr(stream, "detach", None)
        if callable(detach):
            stream = detach()
            process.stdin = stream
        if os.name != "nt":
            # A POSIX close cannot safely signal a recyclable PID.  Keeping
            # this exact pipe non-blocking ensures the sole writer can observe
            # deadline/close state and release stdin without a stuck syscall.
            os.set_blocking(stream.fileno(), False)
        return stream

    def _write_stdin(self, stream: Any) -> None:
        """Drain the bounded outbound queue from one persistent owner."""

        try:
            while True:
                if self._stdin_close_requested.is_set():
                    self._close_stdin_pipe(deadline=None)
                    return
                try:
                    outbound = self._outbound.get(timeout=_WRITE_POLL_SECONDS)
                except queue.Empty:
                    continue
                self._write_outbound(stream, outbound)
                outbound.completed.set()
        finally:
            self._close_stdin_pipe(deadline=None)
            while True:
                try:
                    pending = self._outbound.get_nowait()
                except queue.Empty:
                    break
                pending.error = MCPCancelledError("request_cancelled")
                pending.completed.set()

    def _write_outbound(self, stream: Any, outbound: _OutboundWrite) -> None:
        offset = 0
        payload = outbound.payload
        while offset < len(payload):
            if outbound.abandoned.is_set() or time.monotonic() >= outbound.deadline:
                outbound.error = MCPTimeoutError("request_timeout")
                return
            if (
                self._close_requested.is_set()
                or self._stdin_close_requested.is_set()
            ):
                outbound.error = MCPCancelledError("request_cancelled")
                return
            chunk = memoryview(payload)[
                offset : min(len(payload), offset + _WRITE_CHUNK_BYTES)
            ]
            try:
                with self._stdin_io_lock:
                    if (
                        self._close_requested.is_set()
                        or self._stdin_close_requested.is_set()
                    ):
                        outbound.error = MCPCancelledError("request_cancelled")
                        return
                    written = stream.write(chunk)
            except BlockingIOError:
                written = None
            except (BrokenPipeError, OSError, ValueError):
                outbound.error = (
                    MCPCancelledError("request_cancelled")
                    if self._close_requested.is_set()
                    else MCPProcessError("stdin_write_failed")
                )
                return
            if written is None or written == 0:
                self._close_requested.wait(_WRITE_POLL_SECONDS)
                continue
            offset += int(written)

    def _close_stdin_pipe(self, *, deadline: float | None) -> bool:
        """Close this transport's exact stdin object without an unbounded wait."""

        if deadline is None:
            acquired = self._stdin_io_lock.acquire(blocking=False)
        else:
            remaining = max(0.0, deadline - time.monotonic())
            acquired = self._stdin_io_lock.acquire(timeout=remaining)
        if not acquired:
            return False
        try:
            with self._state_lock:
                stream = self._stdin_stream
            if stream is None or bool(getattr(stream, "closed", False)):
                return True
            try:
                stream.close()
            except (BrokenPipeError, OSError, ValueError):
                return bool(getattr(stream, "closed", False))
            return bool(getattr(stream, "closed", True))
        finally:
            self._stdin_io_lock.release()

    def _accept_initialize(self, result: Any) -> None:
        if not isinstance(result, Mapping):
            raise MCPProtocolError("initialize_result_invalid")
        raw_version = result.get("protocolVersion")
        server_protocol_version = _safe_protocol_version(
            raw_version, field_name="server_protocol_version"
        )
        if server_protocol_version not in self._supported_protocol_versions:
            raise MCPProtocolError("protocol_version_unsupported")
        server_info = result.get("serverInfo")
        if not isinstance(server_info, Mapping):
            raise MCPProtocolError("server_info_invalid")
        server_name = _bounded_text(
            server_info.get("name"), field_name="server_name", maximum=_MAX_TEXT_BYTES
        )
        server_version = _bounded_text(
            server_info.get("version"), field_name="server_version", maximum=_MAX_TEXT_BYTES
        )
        raw_capabilities = result.get("capabilities", {})
        if not isinstance(raw_capabilities, Mapping):
            raise MCPProtocolError("server_capabilities_invalid")
        bounded_capabilities = _bound_json(
            raw_capabilities, field_name="server_capabilities"
        )
        if not isinstance(bounded_capabilities, dict):
            raise MCPProtocolError("server_capabilities_invalid")
        with self._state_lock:
            self._server_protocol_version = server_protocol_version
            self._server_name = server_name
            self._server_version = server_version
            self._server_capabilities = bounded_capabilities

    def _parse_tool(self, raw_tool: Any) -> MCPToolSnapshot:
        if not isinstance(raw_tool, Mapping):
            raise MCPProtocolError("tool_descriptor_invalid")
        name = _safe_tool_name(raw_tool.get("name"))
        description_value = raw_tool.get("description", "")
        if not isinstance(description_value, str):
            raise MCPProtocolError("tool_description_invalid")
        description = _bounded_text(
            description_value,
            field_name="tool_description",
            maximum=_MAX_TEXT_BYTES,
        ) if description_value else ""
        title_value = raw_tool.get("title")
        title = None
        if title_value is not None:
            title = _bounded_text(title_value, field_name="tool_title")
        raw_schema = raw_tool.get("inputSchema")
        if not isinstance(raw_schema, Mapping):
            raise MCPProtocolError("tool_input_schema_invalid")
        schema = _bound_json(raw_schema, field_name="tool_input_schema")
        if not isinstance(schema, dict):
            raise MCPProtocolError("tool_input_schema_invalid")
        encoded = json.dumps(
            schema, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        if len(encoded) > self._max_message_bytes:
            raise MCPProtocolError("tool_input_schema_too_large")
        return MCPToolSnapshot(
            name=name,
            title=title,
            description=description,
            input_schema=schema,
            input_schema_digest=hashlib.sha256(encoded).hexdigest(),
        )

    def _enter_transport_owner(self) -> None:
        thread = threading.current_thread()
        key = id(thread)
        with self._state_lock:
            existing = self._owner_threads.get(key)
            count = 0 if existing is None else existing[1]
            self._owner_threads[key] = (thread, count + 1)

    def _leave_transport_owner(self) -> None:
        thread = threading.current_thread()
        key = id(thread)
        with self._state_lock:
            existing = self._owner_threads.get(key)
            if existing is None:
                return
            if existing[1] <= 1:
                self._owner_threads.pop(key, None)
            else:
                self._owner_threads[key] = (thread, existing[1] - 1)

    def _owner_thread_snapshot_locked(self) -> tuple[threading.Thread, ...]:
        threads = list(item[0] for item in self._owner_threads.values())
        writer = self._writer_thread
        if writer is not None and all(thread is not writer for thread in threads):
            threads.append(writer)
        return tuple(threads)

    def _reader_thread_snapshot_locked(self) -> tuple[threading.Thread, ...]:
        return tuple(
            thread
            for thread in (self._stdout_thread, self._stderr_thread)
            if thread is not None
        )

    def _request_cancelled(self, cancel_event: Any) -> bool:
        return self._close_requested.is_set() or _is_cancelled(cancel_event)

    def _request_deadline(
        self,
        *,
        timeout: float | None,
        deadline: float | None,
    ) -> float:
        if timeout is not None and deadline is not None:
            raise ValueError("timeout and deadline are mutually exclusive")
        if deadline is not None:
            return _absolute_deadline(
                deadline,
                default_timeout=self._request_timeout,
            )
        seconds = (
            self._request_timeout
            if timeout is None
            else _bounded_timeout(
                timeout,
                field_name="timeout",
                maximum=self._max_timeout,
            )
        )
        return time.monotonic() + seconds

    def _request(
        self,
        method: str,
        params: Mapping[str, Any],
        *,
        timeout: float | None = None,
        deadline: float | None = None,
        cancel_event: Any = None,
        allow_starting: bool = False,
    ) -> Any:
        self._enter_transport_owner()
        acquired = False
        try:
            absolute = self._request_deadline(
                timeout=timeout,
                deadline=deadline,
            )
            while True:
                if self._request_cancelled(cancel_event):
                    with self._state_lock:
                        fatal = self._fatal_error
                    if fatal is not None:
                        raise fatal
                    raise MCPCancelledError("request_cancelled")
                remaining = absolute - time.monotonic()
                if remaining <= 0:
                    raise MCPTimeoutError("request_timeout")
                if self._request_lock.acquire(timeout=min(remaining, 0.05)):
                    acquired = True
                    break
            with self._state_lock:
                allowed_phases = {
                    _TransportPhase.READY,
                    *({ _TransportPhase.STARTING } if allow_starting else set()),
                }
                if self._phase not in allowed_phases:
                    if self._fatal_error is not None:
                        raise self._fatal_error
                    if self._close_requested.is_set():
                        raise MCPCancelledError("request_cancelled")
                    raise MCPProcessError("transport_not_ready")
                process = self._process
                request_id = self._next_request_id
                self._next_request_id += 1
            if process is None or process.poll() is not None:
                raise MCPProcessError("process_not_running")
            if self._request_cancelled(cancel_event):
                with self._state_lock:
                    fatal = self._fatal_error
                if fatal is not None:
                    raise fatal
                raise MCPCancelledError("request_cancelled")
            message = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": _copy_json(params),
            }
            self._send_message(
                message,
                deadline=absolute,
                cancel_event=cancel_event,
            )
            remaining = absolute - time.monotonic()
            if remaining <= 0:
                raise MCPTimeoutError("request_timeout")
            return self._wait_for_response(
                request_id,
                deadline=absolute,
                cancel_event=cancel_event,
                cancellation_allowed=(method != "initialize"),
            )
        except MCPTimeoutError:
            self._terminalize("request_timeout", deadline=absolute)
            raise
        except MCPCancelledError:
            self._terminalize("client_cancelled", deadline=absolute)
            raise
        finally:
            if acquired:
                self._request_lock.release()
            self._leave_transport_owner()

    def _wait_for_response(
        self,
        request_id: int,
        *,
        deadline: float,
        cancel_event: Any,
        cancellation_allowed: bool,
    ) -> Any:
        cancellation_grace = (
            min(
                _CANCEL_GRACE_SECONDS,
                max(0.0, (deadline - time.monotonic()) / 2.0),
            )
            if cancellation_allowed
            else 0.0
        )
        while True:
            if self._close_requested.is_set():
                with self._state_lock:
                    fatal = self._fatal_error
                if fatal is not None:
                    raise fatal
                if cancellation_allowed:
                    try:
                        self._send_notification(
                            "notifications/cancelled",
                            {"requestId": request_id, "reason": "transport_closing"},
                            deadline=deadline,
                        )
                    except MCPError:
                        pass
                raise MCPCancelledError("request_cancelled")
            if _is_cancelled(cancel_event):
                if cancellation_allowed:
                    self._cancel_and_close(
                        request_id,
                        "client_cancelled",
                        deadline=deadline,
                    )
                else:
                    self._terminalize(
                        "initialize_cancelled",
                        deadline=deadline,
                    )
                raise MCPCancelledError("request_cancelled")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if cancellation_allowed:
                    self._cancel_and_close(
                        request_id,
                        "request_timeout",
                        deadline=deadline,
                    )
                else:
                    self._terminalize(
                        "initialize_timeout",
                        deadline=deadline,
                    )
                raise MCPTimeoutError("request_timeout")
            if cancellation_allowed:
                # The caller's absolute deadline includes cooperative MCP
                # cancellation.  Reserve the existing bounded grace inside
                # that budget instead of adding it after timeout.
                response_remaining = remaining - cancellation_grace
                if response_remaining <= 0:
                    try:
                        inbound = self._inbound.get_nowait()
                    except queue.Empty:
                        self._cancel_and_close(
                            request_id,
                            "request_timeout",
                            deadline=deadline,
                        )
                        raise MCPTimeoutError("request_timeout")
                else:
                    try:
                        inbound = self._inbound.get(
                            timeout=min(response_remaining, 0.05)
                        )
                    except queue.Empty:
                        continue
            else:
                try:
                    inbound = self._inbound.get(timeout=min(remaining, 0.05))
                except queue.Empty:
                    continue
            if inbound.error is not None:
                raise inbound.error
            message = inbound.message
            if not isinstance(message, dict):
                self._fail_closed("inbound_message_invalid", deadline=deadline)
                raise MCPProtocolError("inbound_message_invalid")
            if "id" not in message:
                continue
            response_id = message.get("id")
            if response_id in self._ignored_ids:
                self._ignored_ids.remove(response_id)
                continue
            if response_id != request_id:
                self._fail_closed("unexpected_response_id", deadline=deadline)
                raise MCPProtocolError("unexpected_response_id")
            has_result = "result" in message
            has_error = "error" in message
            if has_result == has_error:
                self._fail_closed(
                    "response_result_error_shape_invalid",
                    deadline=deadline,
                )
                raise MCPProtocolError("response_result_error_shape_invalid")
            if has_error:
                error = message.get("error")
                if not isinstance(error, Mapping):
                    self._fail_closed("remote_error_invalid", deadline=deadline)
                    raise MCPProtocolError("remote_error_invalid")
                raw_code = error.get("code")
                rpc_code = (
                    raw_code
                    if isinstance(raw_code, int)
                    and not isinstance(raw_code, bool)
                    else None
                )
                raise MCPRemoteError(rpc_code)
            return _bound_json(message.get("result"), field_name="response_result")

    def _send_notification(
        self,
        method: str,
        params: Mapping[str, Any],
        *,
        deadline: float | None = None,
    ) -> None:
        message = {
            "jsonrpc": "2.0",
            "method": method,
            "params": _copy_json(params),
        }
        self._send_message(message, deadline=deadline)

    def _require_server_capability(self, name: str) -> None:
        """Refuse an operation the initialized server did not negotiate."""

        with self._state_lock:
            advertised = self._server_capabilities.get(name)
        if not isinstance(advertised, Mapping):
            raise MCPProtocolError(f"server_capability_{name}_unavailable")

    def _send_message(
        self,
        message: Mapping[str, Any],
        *,
        deadline: float | None = None,
        cancel_event: Any = None,
    ) -> None:
        absolute = _absolute_deadline(
            deadline,
            default_timeout=self._request_timeout,
        )
        bounded = _bound_json(message, field_name="outbound_message")
        encoded = (
            json.dumps(
                bounded,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
        if len(encoded) > self._max_message_bytes:
            raise MCPProtocolError("outbound_message_too_large")
        if _is_cancelled(cancel_event) or self._close_requested.is_set():
            raise MCPCancelledError("request_cancelled")
        with self._state_lock:
            process = self._process
            writer = self._writer_thread
            stream = self._stdin_stream
        if (
            process is None
            or process.poll() is not None
            or stream is None
            or bool(getattr(stream, "closed", False))
            or writer is None
            or not writer.is_alive()
        ):
            error = MCPProcessError("process_not_writable")
            self._fail_closed("stdin_write_failed", deadline=absolute)
            raise error

        # Preserve cleanup time *inside* the caller's absolute deadline.  The
        # request thread never performs pipe I/O and can therefore trigger the
        # retained exact-owner close even if the sole writer is blocked in an
        # operating-system write.
        now = time.monotonic()
        interrupt_reserve = min(
            _CANCEL_GRACE_SECONDS,
            max(0.0, (absolute - now) / 2.0),
        )
        write_deadline = absolute - interrupt_reserve
        outbound = _OutboundWrite(payload=encoded, deadline=write_deadline)

        while True:
            if _is_cancelled(cancel_event):
                outbound.abandoned.set()
                raise MCPCancelledError("request_cancelled")
            if self._close_requested.is_set():
                outbound.abandoned.set()
                raise MCPCancelledError("request_cancelled")
            remaining = write_deadline - time.monotonic()
            if remaining <= 0:
                outbound.abandoned.set()
                raise MCPTimeoutError("request_timeout")
            try:
                self._outbound.put(
                    outbound,
                    timeout=min(remaining, _WRITE_POLL_SECONDS),
                )
                break
            except queue.Full:
                continue

        while True:
            remaining = write_deadline - time.monotonic()
            if remaining <= 0:
                outbound.abandoned.set()
                raise MCPTimeoutError("request_timeout")
            if outbound.completed.wait(
                timeout=min(_WRITE_POLL_SECONDS, remaining)
            ):
                break
            if _is_cancelled(cancel_event):
                outbound.abandoned.set()
                raise MCPCancelledError("request_cancelled")
            if self._close_requested.is_set():
                outbound.abandoned.set()
                raise MCPCancelledError("request_cancelled")
            if time.monotonic() >= write_deadline:
                outbound.abandoned.set()
                raise MCPTimeoutError("request_timeout")
        if outbound.error is not None:
            if (
                isinstance(outbound.error, MCPProcessError)
                and not self._close_requested.is_set()
            ):
                self._fail_closed("stdin_write_failed", deadline=absolute)
            raise outbound.error

    def _cancel_and_close(
        self,
        request_id: int,
        reason: str,
        *,
        deadline: float,
    ) -> None:
        self._ignored_ids.append(request_id)
        if len(self._ignored_ids) > _MAX_ID_IGNORED:
            del self._ignored_ids[:-_MAX_ID_IGNORED]
        try:
            self._send_notification(
                "notifications/cancelled",
                {"requestId": request_id, "reason": reason},
                deadline=deadline,
            )
        except MCPError:
            pass
        # Give a cooperative child one bounded scheduling window to observe
        # the cancellation notification before the cleanup path terminates
        # an untrusted or non-cooperative process.
        remaining = max(0.0, deadline - time.monotonic())
        if remaining > 0:
            self._close_requested.wait(
                timeout=min(_CANCEL_GRACE_SECONDS, remaining)
            )
        self._terminalize(reason, deadline=deadline)

    def _read_stdout(self, stream: Any) -> None:
        total = 0
        try:
            while True:
                line = stream.readline(self._max_message_bytes + 1)
                if not line:
                    self._set_fatal("process_stdout_closed", process_error=True)
                    return
                total += len(line)
                if len(line) > self._max_message_bytes or total > self._max_stdout_bytes:
                    self._set_fatal("stdout_limit_exceeded")
                    return
                if not line.endswith(b"\n"):
                    self._set_fatal("stdout_frame_unterminated")
                    return
                try:
                    decoded = line[:-1].rstrip(b"\r").decode("utf-8", errors="strict")
                    message = json.loads(decoded)
                except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
                    self._set_fatal("stdout_protocol_pollution")
                    return
                if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
                    self._set_fatal("jsonrpc_message_invalid")
                    return
                if "id" in message:
                    if not _request_id(message.get("id")):
                        self._set_fatal("jsonrpc_response_id_invalid")
                        return
                    self._inbound.put(_Inbound(message=message))
                    continue
                method = message.get("method")
                if not isinstance(method, str) or not method.startswith("notifications/"):
                    self._set_fatal("jsonrpc_notification_invalid")
                    return
                self._inbound.put(_Inbound(message=message))
        except (OSError, ValueError):
            self._set_fatal("stdout_reader_failed")

    def _read_stderr(self, stream: Any) -> None:
        try:
            while True:
                chunk = stream.read(min(4096, self._max_stderr_bytes + 1))
                if not chunk:
                    return
                with self._state_lock:
                    self._stderr_bytes_seen += len(chunk)
                    total = self._stderr_bytes_seen
                if total > self._max_stderr_bytes:
                    self._set_fatal("stderr_limit_exceeded")
                    return
        except (OSError, ValueError):
            self._set_fatal("stderr_reader_failed")

    def _set_fatal(self, code: str, *, process_error: bool = False) -> None:
        error: MCPError = (
            MCPProcessError(code) if process_error else MCPProtocolError(code)
        )
        with self._state_lock:
            if self._close_requested.is_set():
                return
            if self._fatal_error is None:
                self._fatal_error = error
            self._phase = _TransportPhase.BROKEN
            selected = self._fatal_error
        self._inbound.put(_Inbound(error=selected))
        self._terminalize(code, deadline=None)

    def _fail_closed(self, code: str, *, deadline: float | None = None) -> None:
        with self._state_lock:
            error = MCPProtocolError(code)
            if self._fatal_error is None:
                self._fatal_error = error
            self._phase = _TransportPhase.BROKEN
        self._terminalize(code, deadline=deadline)

    def _terminalize(self, reason: str, *, deadline: float | None) -> None:
        """Publish one terminal edge before any runtime logical detach."""

        with self._state_lock:
            if self._terminal_notified:
                return
            self._terminal_notified = True
            callback = self._terminal_callback
        if callback is not None:
            try:
                callback(self, reason, deadline)
            except Exception:
                # The runtime will re-enter the same idempotent seam from the
                # failing call/connect owner.  A callback failure must not
                # replace the stable MCP protocol error exposed to the caller.
                return
        else:
            self._signal_close("request_cancelled")
            self.close(deadline=deadline)

    def _ensure_ready(self) -> None:
        with self._state_lock:
            if self._phase is _TransportPhase.READY:
                return
            if self._fatal_error is not None:
                raise self._fatal_error
            if self._phase is _TransportPhase.CLOSED:
                raise MCPProcessError("transport_closed")
            raise MCPProcessError("transport_not_ready")

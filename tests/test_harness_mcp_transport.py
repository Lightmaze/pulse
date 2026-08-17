from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path
from textwrap import dedent

import pytest

import pulse_system.agent.harness.mcp_transport as mcp_transport_module
from pulse_system.agent.harness.mcp_transport import (
    EXECUTION_SAFETY_UNVERIFIED,
    LIVE_MCP_TRANSPORT,
    MCPError,
    MCPProtocolError,
    MCPProcessError,
    MCPStdioTransport,
    MCPTimeoutError,
    MCPCancelledError,
    MCPToolCollisionError,
)
from pulse_system.agent.harness.process_containment import ContainedProcessOwner
from pulse_system.core.runtime.publication import RuntimePublicationGate


_CHILD_SOURCE = dedent(
    r'''
    import json
    import os
    import sys
    import threading
    import time

    MODE = sys.argv[1]
    TOOLS = [{
        "name": "echo",
        "description": "return the supplied value",
        "inputSchema": {
            "type": "object",
            "properties": {"value": {"type": "string"}},
            "required": ["value"],
        },
    }]

    def send(message):
        sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
        sys.stdout.flush()

    def error(request_id, code):
        send({"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": "bounded"}})

    cancel_seen = threading.Event()

    def mark(value):
        with open("wire.log", "a", encoding="utf-8") as handle:
            handle.write(value + "\n")

    def wait_for_cancel(request_id):
        if cancel_seen.wait(5):
            mark("cancelled:" + str(request_id))

    if MODE == "oversize-stdout":
        sys.stdout.write("x" * 4096)
        sys.stdout.flush()
        for _line in sys.stdin:
            pass
        raise SystemExit(0)
    if MODE == "oversize-stderr":
        sys.stderr.write("x" * 4096)
        sys.stderr.flush()
        for _line in sys.stdin:
            pass
        raise SystemExit(0)

    for line in sys.stdin:
        request = json.loads(line)
        if "method" not in request:
            continue
        method = request["method"]
        request_id = request.get("id")
        if method == "initialize":
            if MODE == "initialize-timeout":
                continue
            send({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": request["params"]["protocolVersion"],
                    "capabilities": (
                        {} if MODE == "no-tools-capability"
                        else {"tools": {"listChanged": False}}
                    ),
                    "serverInfo": {"name": "temporary-mcp-child", "version": "1.0"},
                },
            })
        elif method == "notifications/initialized":
            mark("initialized")
            continue
        elif method == "tools/list":
            if MODE == "collision":
                send({"jsonrpc": "2.0", "id": request_id, "result": {"tools": [TOOLS[0], TOOLS[0]]}})
            else:
                send({"jsonrpc": "2.0", "id": request_id, "result": {"tools": TOOLS}})
            if MODE == "stop-reading-after-list":
                mark("stdin_reader_stopped")
                while True:
                    time.sleep(1)
        elif method == "tools/call":
            mark("call:" + str(request_id))
            if MODE in {"timeout", "cancel"}:
                threading.Thread(
                    target=wait_for_cancel,
                    args=(request_id,),
                    daemon=True,
                ).start()
            elif MODE == "unexpected-id":
                send({"jsonrpc": "2.0", "id": 9999, "result": {"content": []}})
            else:
                arguments = request["params"].get("arguments", {})
                present = "MCP_TEST_SECRET" in os.environ
                send({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [{"type": "text", "text": arguments.get("value", "")}],
                        "isError": False,
                        "secret_present": present,
                    },
                })
        elif method == "notifications/cancelled":
            mark("cancel_notification")
            cancel_seen.set()
            continue
        elif method == "tools/list_changed":
            continue
        else:
            error(request_id, -32601)
    mark("stdin_eof")
    ''')


def _child(tmp_path: Path, mode: str = "normal") -> tuple[tuple[str, ...], Path]:
    script = tmp_path / "mcp_child.py"
    script.write_text(_CHILD_SOURCE, encoding="utf-8")
    return (sys.executable, "-u", str(script), mode), script


def _transport(
    tmp_path: Path,
    *,
    mode: str = "normal",
    reserved: tuple[str, ...] = (),
    **kwargs,
) -> MCPStdioTransport:
    argv, _script = _child(tmp_path, mode)
    transport = MCPStdioTransport(
        argv=argv,
        cwd=tmp_path.resolve(),
        env={"PYTHONIOENCODING": "utf-8"},
        env_allowlist={"PYTHONIOENCODING"},
        reserved_tool_names=reserved,
        **kwargs,
    )
    return transport


def test_real_stdio_handshake_list_call_snapshot_and_env_isolation(tmp_path: Path) -> None:
    os.environ["MCP_TEST_SECRET"] = "must-not-cross-process-boundary"
    transport = _transport(tmp_path)
    try:
        transport.start()
        snapshot = transport.list_tools()
        result = transport.call_tool("echo", {"value": "hello"})
        wire = (tmp_path / "wire.log").read_text(encoding="utf-8").splitlines()

        assert result["content"][0]["text"] == "hello"
        assert result["secret_present"] is False
        assert snapshot.server_name == "temporary-mcp-child"
        assert snapshot.tool_names == ("echo",)
        assert snapshot.tools[0].input_schema["type"] == "object"
        assert snapshot.evidence.transport == LIVE_MCP_TRANSPORT
        assert snapshot.evidence.execution_safety == EXECUTION_SAFETY_UNVERIFIED
        assert transport.env_allowlist == ("PYTHONIOENCODING",)
        assert "initialized" in wire
        owner = transport.physical_owner
        assert type(owner) is ContainedProcessOwner
        assert owner.process is transport._process
        assert len(owner.owner_token) == 32
    finally:
        summary = transport.close()
        os.environ.pop("MCP_TEST_SECRET", None)
    assert transport.is_running is False
    assert transport.physical_owner is owner
    assert summary["process_tree_state"] == (
        "empty_verified" if os.name == "nt" else "root_exit_only"
    )


def test_all_requests_share_one_writer_owner_and_close_joins_it(
    tmp_path: Path,
) -> None:
    transport = _transport(tmp_path)
    transport.start()
    transport.list_tools()
    writer = transport._writer_thread
    assert writer is not None
    assert writer.is_alive()
    try:
        for index in range(8):
            result = transport.call_tool("echo", {"value": str(index)})
            assert result["content"][0]["text"] == str(index)
            assert transport._writer_thread is writer
    finally:
        summary = transport.close(deadline=time.monotonic() + 1.0)
    writer.join(timeout=0.2)
    assert not writer.is_alive()
    assert summary["transport_owners_unresolved"] == 0


def test_request_id_association_rejects_unexpected_response_id(tmp_path: Path) -> None:
    transport = _transport(tmp_path, mode="unexpected-id")
    try:
        transport.start()
        transport.list_tools()
        with pytest.raises(MCPProtocolError, match="unexpected_response_id"):
            transport.call_tool("echo", {"value": "x"})
        assert transport.is_running is False
    finally:
        transport.close()


def test_timeout_sends_bounded_cancel_and_cleans_real_child(tmp_path: Path) -> None:
    transport = _transport(tmp_path, mode="timeout", request_timeout=0.2, max_timeout=1.0)
    transport.start()
    transport.list_tools()
    started = time.monotonic()
    try:
        with pytest.raises(MCPTimeoutError, match="request_timeout"):
            transport.call_tool("echo", {"value": "wait"}, timeout=0.1)
    finally:
        transport.close()
    wire = (tmp_path / "wire.log").read_text(encoding="utf-8").splitlines()
    assert time.monotonic() - started < 2.0
    assert transport.is_running is False
    assert "cancel_notification" in wire


@pytest.mark.skipif(
    os.name != "nt",
    reason="requires exact Windows Job termination for adversarial child cleanup",
)
def test_blocked_stdin_write_respects_absolute_deadline_and_is_recoverable(
    tmp_path: Path,
) -> None:
    transport = _transport(
        tmp_path,
        mode="stop-reading-after-list",
        request_timeout=1.0,
        max_timeout=2.0,
        max_message_bytes=256 * 1024,
        shutdown_timeout=0.2,
    )
    transport.start()
    transport.list_tools()
    outcome: list[tuple[str, str]] = []

    def invoke() -> None:
        try:
            transport.call_tool(
                "echo",
                {"chunks": ["x" * 512 for _ in range(256)]},
                timeout=0.1,
            )
        except MCPError as exc:
            outcome.append((type(exc).__name__, exc.code))

    caller = threading.Thread(target=invoke, daemon=True)
    started = time.monotonic()
    caller.start()
    caller.join(timeout=0.5)
    returned_within_bound = not caller.is_alive()
    try:
        assert returned_within_bound
        assert outcome == [("MCPTimeoutError", "request_timeout")]
        assert time.monotonic() - started < 0.5
    finally:
        if caller.is_alive():
            owner = transport.physical_owner
            assert type(owner) is ContainedProcessOwner
            owner.terminate_tree(time.monotonic() + 1.0)
        caller.join(timeout=1.0)
        transport.close(deadline=time.monotonic() + 1.0)


def test_signal_close_delivers_stdin_eof_without_process_signal(
    tmp_path: Path,
) -> None:
    transport = _transport(tmp_path)
    transport.start()
    transport.list_tools()
    owner = transport.physical_owner
    assert type(owner) is ContainedProcessOwner
    try:
        transport.signal_close()
        owner.process.wait(timeout=1.0)
        wire = (tmp_path / "wire.log").read_text(encoding="utf-8").splitlines()
        assert "stdin_eof" in wire
    finally:
        summary = transport.close(deadline=time.monotonic() + 1.0)
    assert summary["owner_joined"] is True
    assert summary["process_tree_state"] == (
        "empty_verified" if os.name == "nt" else "root_exit_only"
    )


def test_cancel_event_sends_bounded_cancel_and_cleans_real_child(tmp_path: Path) -> None:
    transport = _transport(tmp_path, mode="cancel", request_timeout=1.0, max_timeout=2.0)
    transport.start()
    transport.list_tools()
    cancelled = threading.Event()

    def cancel() -> None:
        time.sleep(0.08)
        cancelled.set()

    thread = threading.Thread(target=cancel, daemon=True)
    thread.start()
    try:
        with pytest.raises(MCPCancelledError, match="request_cancelled"):
            transport.call_tool("echo", {"value": "wait"}, cancel_event=cancelled)
    finally:
        transport.close()
    thread.join(timeout=1.0)
    wire = (tmp_path / "wire.log").read_text(encoding="utf-8").splitlines()
    assert transport.is_running is False
    assert "cancel_notification" in wire


def test_queued_request_cancellation_never_crosses_the_transport_boundary(
    tmp_path: Path,
) -> None:
    transport = _transport(tmp_path, mode="cancel", request_timeout=2.0, max_timeout=3.0)
    transport.start()
    transport.list_tools()
    first_cancel = threading.Event()
    queued_cancel = threading.Event()
    observed: list[tuple[str, str]] = []

    def invoke(label: str, signal: threading.Event) -> None:
        try:
            transport.call_tool(
                "echo",
                {"value": label},
                timeout=2.0,
                cancel_event=signal,
            )
        except MCPCancelledError as exc:
            observed.append((label, exc.code))

    first = threading.Thread(target=invoke, args=("first", first_cancel), daemon=True)
    first.start()
    wire_path = tmp_path / "wire.log"
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        if wire_path.exists() and any(
            line.startswith("call:")
            for line in wire_path.read_text(encoding="utf-8").splitlines()
        ):
            break
        time.sleep(0.01)

    queued = threading.Thread(target=invoke, args=("queued", queued_cancel), daemon=True)
    queued.start()
    time.sleep(0.05)
    queued_cancel.set()
    queued.join(timeout=1)
    assert not queued.is_alive()
    assert ("queued", "request_cancelled") in observed
    assert len(
        [
            line
            for line in wire_path.read_text(encoding="utf-8").splitlines()
            if line.startswith("call:")
        ]
    ) == 1

    first_cancel.set()
    first.join(timeout=2)
    transport.close()
    assert ("first", "request_cancelled") in observed


def test_initialize_timeout_never_sends_forbidden_cancellation(tmp_path: Path) -> None:
    transport = _transport(
        tmp_path,
        mode="initialize-timeout",
        request_timeout=0.1,
        max_timeout=1.0,
    )
    with pytest.raises(MCPTimeoutError, match="request_timeout"):
        transport.start()
    wire_path = tmp_path / "wire.log"
    wire = wire_path.read_text(encoding="utf-8").splitlines() if wire_path.exists() else []
    assert "cancel_notification" not in wire
    transport.close(deadline=time.monotonic() + 1.0)
    assert transport.is_running is False


def test_tools_require_negotiated_server_capability(tmp_path: Path) -> None:
    transport = _transport(tmp_path, mode="no-tools-capability")
    try:
        transport.start()
        with pytest.raises(MCPProtocolError, match="server_capability_tools_unavailable"):
            transport.list_tools()
    finally:
        transport.close()


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_stream_upper_bound_fails_closed(tmp_path: Path, stream: str) -> None:
    mode = f"oversize-{stream}"
    script = tmp_path / f"oversize_{stream}.py"
    script.write_text(_CHILD_SOURCE, encoding="utf-8")
    transport = MCPStdioTransport(
        argv=(sys.executable, "-u", str(script), mode),
        cwd=tmp_path.resolve(),
        env={},
        env_allowlist=(),
        max_stdout_bytes=1024,
        max_stderr_bytes=1024,
        max_message_bytes=512,
        request_timeout=0.5,
        max_timeout=1.0,
    )
    try:
        with pytest.raises(MCPProtocolError, match="(stdout|stderr)_limit_exceeded"):
            transport.start()
    finally:
        transport.close()
    assert transport.is_running is False


def test_invalid_json_stdout_is_protocol_pollution_and_cleans_child(tmp_path: Path) -> None:
    script = tmp_path / "invalid_json.py"
    script.write_text(
        "import sys\nprint('not-json', flush=True)\nfor _line in sys.stdin: pass\n",
        encoding="utf-8",
    )
    transport = MCPStdioTransport(
        argv=(sys.executable, "-u", str(script)),
        cwd=tmp_path.resolve(),
        env={},
        env_allowlist=(),
        request_timeout=0.5,
        max_timeout=1.0,
    )
    try:
        with pytest.raises(MCPProtocolError, match="stdout_protocol_pollution"):
            transport.start()
    finally:
        transport.close()
    assert transport.is_running is False


def test_tool_name_collision_is_rejected_and_not_exposed(tmp_path: Path) -> None:
    transport = _transport(tmp_path, reserved=("echo",))
    try:
        transport.start()
        with pytest.raises(MCPToolCollisionError, match="tool_name_collision"):
            transport.list_tools()
        assert transport.capability_snapshot is None
        assert transport.is_running is False
    finally:
        transport.close()


def test_explicit_argv_cwd_and_env_allowlist_reject_implicit_inheritance(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="absolute"):
        MCPStdioTransport(argv=(sys.executable,), cwd=".")
    with pytest.raises(ValueError, match="allowlist"):
        MCPStdioTransport(
            argv=(sys.executable,),
            cwd=tmp_path.resolve(),
            env={"MCP_TEST_SECRET": "secret"},
            env_allowlist=(),
        )


def test_runtime_publication_permit_is_strict_and_revoke_wins_before_spawn(
    tmp_path: Path,
    monkeypatch,
) -> None:
    gate = RuntimePublicationGate("runtime-mcp-1", 1)
    with pytest.raises(TypeError, match="RuntimePublicationPermit"):
        _transport(tmp_path, publication_permit=object())
    wrong_gate = RuntimePublicationGate("runtime-mcp-wrong-permits", 1)
    with pytest.raises(TypeError, match="RuntimePublicationPermit"):
        _transport(tmp_path, publication_permit=wrong_gate.bootstrap_permit)
    recovery_permit = wrong_gate.revoke(reason="runtime_close")
    with pytest.raises(TypeError, match="RuntimePublicationPermit"):
        _transport(tmp_path, publication_permit=recovery_permit)

    transport = _transport(
        tmp_path,
        publication_permit=gate.publication_permit,
    )
    real_spawn = mcp_transport_module.spawn_contained_process
    spawn_count = 0

    def counted_spawn(*args, **kwargs):
        nonlocal spawn_count
        spawn_count += 1
        return real_spawn(*args, **kwargs)

    monkeypatch.setattr(
        mcp_transport_module,
        "spawn_contained_process",
        counted_spawn,
    )
    gate.revoke(reason="runtime_close")

    with pytest.raises(MCPProcessError, match="publication_revoked"):
        transport.start()

    summary = transport.close(deadline=time.monotonic() + 0.2)
    assert spawn_count == 0
    assert summary["owner_joined"] is True
    assert summary["process_roots_observed"] == 0
    assert summary["process_tree_state"] == "not_applicable"


def test_pre_revoke_admitted_spawn_is_censused_and_cannot_revive_after_close(
    tmp_path: Path,
    monkeypatch,
) -> None:
    gate = RuntimePublicationGate("runtime-mcp-2", 1)
    transport = _transport(
        tmp_path,
        publication_permit=gate.publication_permit,
    )
    entered_spawn = threading.Event()
    release_spawn = threading.Event()
    real_spawn = mcp_transport_module.spawn_contained_process
    observed: list[str] = []

    def blocked_spawn(*args, **kwargs):
        entered_spawn.set()
        assert release_spawn.wait(2)
        return real_spawn(*args, **kwargs)

    monkeypatch.setattr(
        mcp_transport_module,
        "spawn_contained_process",
        blocked_spawn,
    )

    def start_transport() -> None:
        try:
            transport.start()
        except MCPError as exc:
            observed.append(exc.code)

    owner = threading.Thread(target=start_transport, daemon=True)
    owner.start()
    assert entered_spawn.wait(1)

    revoke_started = time.monotonic()
    gate.revoke(reason="runtime_close")
    assert time.monotonic() - revoke_started < 0.2

    first = transport.close(deadline=time.monotonic() + 0.05)
    assert first["owner_joined"] is False
    assert first["transport_owners_unresolved"] == 1
    assert first["process_roots_observed"] == 0
    assert transport.phase == "CLOSED"

    release_spawn.set()
    owner.join(timeout=2)
    assert not owner.is_alive()
    assert observed == ["transport_closed"]

    settled = transport.close(deadline=time.monotonic() + 3.0)
    assert settled["owner_joined"] is True
    assert settled["process_roots_observed"] == 1
    assert settled["process_root_owners_unresolved"] == 0
    assert settled["process_tree_state"] == (
        "empty_verified" if os.name == "nt" else "root_exit_only"
    )
    assert transport.phase == "CLOSED"
    assert gate.wait_for_publication_drain()["owner_joined"] is True

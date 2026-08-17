from __future__ import annotations

import json
import hashlib
import os
import sys
import threading
import time
from pathlib import Path
from textwrap import dedent

import pytest

import pulse_system.agent.harness.mcp_runtime as mcp_runtime_module
from pulse_system.agent.harness.mcp_runtime import (
    CONTRACT,
    EXECUTION_SAFETY_UNVERIFIED,
    LIVE_GATE_UNVERIFIED,
    MCPApprovalGrant,
    MCPPhysicalOwnerKey,
    MCPApprovalRequiredError,
    MCPApprovalMismatchError,
    MCPCallExecutionError,
    MCPCallScope,
    MCPOperationCollisionError,
    MCPRegistryGate,
    MCPRuntimeConfig,
    MCPRuntimeError,
    MCPRuntimeService,
    MCPServerDescriptor,
    MCPServerNotAllowedError,
    MCPServerState,
    MCPActionBackend,
)
from pulse_system.agent.harness.mcp_transport import (
    MCPError,
    MCPProcessError,
    MCPStdioTransport,
    MCPTransportCloseSummary,
)
from pulse_system.agent.harness.actions import HarnessActionBroker, RoutedActionBackend
from pulse_system.agent.harness.events import HarnessEventStore
from pulse_system.agent.harness.operations import (
    HarnessOperationLedger,
    OperationRecoveryState,
    OperationTerminalState,
)
from pulse_system.agent.harness.security import (
    ApprovalMode,
    ExecutionPolicy,
)
from pulse_system.agent.tools.gateway import ToolInvocationContext
from pulse_system.core.runtime.publication import RuntimePublicationGate
from pulse_system.substrate.storage import Storage


_CHILD_SOURCE = dedent(
    r'''
    import json
    import subprocess
    import sys
    import threading
    import time

    MODE = sys.argv[1]
    calls = 0
    tools = [{
        "name": "echo",
        "description": "return the supplied value",
        "inputSchema": {
            "type": "object",
            "properties": {"value": {"type": "string"}},
        },
    }]

    def send(message):
        sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
        sys.stdout.flush()

    def log(value):
        with open("wire.log", "a", encoding="utf-8") as handle:
            handle.write(value + "\n")

    def wait_for_cancel(request_id):
        time.sleep(10)

    if MODE == "stop-reading-tree-after-list":
        descendant_source = (
            "from pathlib import Path\n"
            "import time\n"
            "with Path('descendant.log').open('a', encoding='utf-8') as handle:\n"
            "    handle.write('started\\n')\n"
            "time.sleep(60)\n"
        )
        subprocess.Popen(
            [sys.executable, "-u", "-c", descendant_source],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        log("tree_root_started")

    for line in sys.stdin:
        request = json.loads(line)
        method = request.get("method")
        request_id = request.get("id")
        if method == "initialize":
            if MODE == "connect-delay":
                log("initialize_entered")
                time.sleep(2)
            send({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": request["params"]["protocolVersion"],
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "runtime-child", "version": "1.0"},
                },
            })
        elif method == "notifications/initialized":
            log("initialized")
        elif method == "tools/list":
            send({"jsonrpc": "2.0", "id": request_id, "result": {"tools": tools}})
            if MODE == "stop-reading-tree-after-list":
                log("stdin_reader_stopped")
                while True:
                    time.sleep(1)
        elif method == "tools/call":
            calls += 1
            log("call:" + str(calls))
            if MODE == "cancel":
                threading.Thread(
                    target=wait_for_cancel,
                    args=(request_id,),
                    daemon=True,
                ).start()
            elif MODE == "unexpected-id":
                send({"jsonrpc": "2.0", "id": 9999, "result": {"content": []}})
            elif MODE == "reader-fatal":
                sys.stdout.write("not-json\n")
                sys.stdout.flush()
                time.sleep(2)
            elif MODE == "stderr-fatal":
                sys.stderr.write("x" * 70000)
                sys.stderr.flush()
                time.sleep(2)
            else:
                value = request["params"].get("arguments", {}).get("value", "")
                send({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [{"type": "text", "text": value}],
                        "isError": False,
                        "secret": "do-not-project",
                    },
                })
        elif method == "notifications/cancelled":
            log("cancelled")
        ''')


def _descriptor(tmp_path: Path, *, mode: str = "normal", server_id: str = "demo") -> MCPServerDescriptor:
    script = tmp_path / f"mcp_child_{server_id}.py"
    script.write_text(_CHILD_SOURCE, encoding="utf-8")
    return MCPServerDescriptor(
        server_id=server_id,
        argv=(sys.executable, "-u", str(script), mode),
        cwd=tmp_path.resolve(),
        env={"PYTHONIOENCODING": "utf-8"},
        env_allowlist=("PYTHONIOENCODING",),
    )


class _ControlledCloseTransport(MCPStdioTransport):
    def __init__(self, tmp_path: Path, *, fail_start: bool = False) -> None:
        super().__init__(
            argv=(sys.executable, "-c", "pass"),
            cwd=tmp_path.resolve(),
        )
        self._fail_start = fail_start
        self._owner_release = threading.Event()
        self._signal_observed = threading.Event()
        self._controlled_owner = threading.Thread(
            target=self._owner_release.wait,
            name="test-mcp-retained-close-owner",
            daemon=True,
        )
        self._controlled_owner.start()

    def start(self, *, cancel_event=None, deadline=None) -> None:
        del cancel_event, deadline
        if self._fail_start:
            raise MCPProcessError("controlled_start_failure")

    def signal_close(self) -> None:
        self._signal_observed.set()

    def close(self, *, deadline: float | None = None) -> MCPTransportCloseSummary:
        self.signal_close()
        unresolved = int(self._controlled_owner.is_alive())
        return {
            "active_before": unresolved,
            "unresolved": unresolved,
            "transport_owners_unresolved": unresolved,
            "reader_owners_unresolved": 0,
            "process_roots_observed": 0,
            "process_root_owners_unresolved": 0,
            "owner_joined": unresolved == 0,
            "process_tree_state": "not_applicable",
        }

    def release_owner(self) -> None:
        self._owner_release.set()
        self._controlled_owner.join(timeout=1.0)
        assert not self._controlled_owner.is_alive()


def _staged_close_summary(
    tree: str,
    *,
    transport_unresolved: int = 0,
    reader_unresolved: int = 0,
    process_owner_unresolved: int = 0,
) -> MCPTransportCloseSummary:
    process_roots = int(tree != "not_applicable")
    unresolved = (
        transport_unresolved
        + reader_unresolved
        + process_owner_unresolved
    )
    return {
        "active_before": max(1, unresolved),
        "unresolved": unresolved,
        "transport_owners_unresolved": transport_unresolved,
        "reader_owners_unresolved": reader_unresolved,
        "process_roots_observed": process_roots,
        "process_root_owners_unresolved": process_owner_unresolved,
        "owner_joined": unresolved == 0,
        "process_tree_state": tree,  # type: ignore[typeddict-item]
    }


class _StagedCloseTransport(MCPStdioTransport):
    def __init__(
        self,
        tmp_path: Path,
        *summaries: MCPTransportCloseSummary,
    ) -> None:
        super().__init__(
            argv=(sys.executable, "-c", "pass"),
            cwd=tmp_path.resolve(),
        )
        if not summaries:
            raise ValueError("at least one staged close summary is required")
        self._summaries = list(summaries)
        self.close_deadlines: list[float] = []
        self.signal_count = 0

    def signal_close(self) -> None:
        self.signal_count += 1

    def close(self, *, deadline: float | None = None) -> MCPTransportCloseSummary:
        assert deadline is not None
        self.close_deadlines.append(deadline)
        if len(self._summaries) > 1:
            return dict(self._summaries.pop(0))  # type: ignore[return-value]
        return dict(self._summaries[0])  # type: ignore[return-value]


def _attach_then_retain_for_test(
    service: MCPRuntimeService,
    transport: MCPStdioTransport,
):
    session = service._sessions["demo"]
    with service._lock:
        session.connect_generation = 1
        session.state = MCPServerState.READY
        session.transport = transport
        key = service._registry_key_for_transport(
            server_id="demo",
            descriptor_digest=session.descriptor.identity.descriptor_digest,
            connect_generation=1,
            transport=transport,
        )
        session.active_owner_key = key
        return service.detach_to_retained_locked(
            session,
            transport,
            key,
            "test_terminal",
            terminal_state=MCPServerState.BROKEN,
        )


def test_artifact_replacement_after_preview_fails_closed(tmp_path: Path):
    descriptor = _descriptor(tmp_path)
    service = MCPRuntimeService([descriptor], allowlisted_server_ids=["demo"])
    backend = MCPActionBackend(service, world_id="world-1")
    request = {
        "server_id": "demo",
        "tool_name": "echo",
        "arguments": {"value": "safe"},
    }
    preview = backend.preview_for("pulse_mcp_call", request)
    script = next(path for path in descriptor.artifact_paths if path.name.startswith("mcp_child_"))
    script.write_text("raise SystemExit('replaced')\n", encoding="utf-8")

    result = backend.execute(
        action_request_id="action-artifact-swap",
        engram_id="engram-1",
        turn_id="turn-1",
        epoch=1,
        tool_name="pulse_mcp_call",
        input_data=request,
        policy_preview=preview,
    )

    assert result["ok"] is False
    assert result["error"] == "descriptor_changed_after_approval"
    assert service.list_server_summaries()[0]["state"] == "DECLARED"
    assert not (tmp_path / "wire.log").exists()


def test_close_server_fences_an_inflight_connect_generation(tmp_path: Path) -> None:
    service = MCPRuntimeService(
        [_descriptor(tmp_path, mode="connect-delay")],
        allowlisted_server_ids=("demo",),
        config=MCPRuntimeConfig(max_connect_wait_sec=3),
    )
    observed: list[str] = []

    def connect() -> None:
        try:
            service.connect("demo")
        except MCPRuntimeError as exc:
            observed.append(exc.code)

    thread = threading.Thread(target=connect, daemon=True)
    thread.start()
    deadline = time.monotonic() + 1
    wire_path = tmp_path / "wire.log"
    while time.monotonic() < deadline:
        if wire_path.exists() and "initialize_entered" in wire_path.read_text(encoding="utf-8"):
            break
        time.sleep(0.01)
    assert wire_path.exists()

    service.close_server("demo")
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert observed
    summary = service.list_server_summaries()[0]
    assert summary["state"] == MCPServerState.CLOSED.value
    assert service.approval_preview("demo", "echo")["process_started"] is False
    with pytest.raises(MCPRuntimeError, match="server_closed"):
        service.connect("demo")
    service.close()


def _scope() -> MCPCallScope:
    return MCPCallScope(world_id="world-1", engram_id="engram-1", turn_id="turn-1", epoch=1)


def _grant(
    service: MCPRuntimeService,
    *,
    operation_id: str,
    value: str,
    scope: MCPCallScope | None = None,
) -> MCPApprovalGrant:
    handle = service.connect("demo")
    scope = _scope() if scope is None else scope
    arguments_digest = hashlib.sha256(
        json.dumps({"value": value}, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return MCPApprovalGrant(
        server_id="demo",
        tool_name="echo",
        operation_id=operation_id,
        scope_digest=scope.digest,
        arguments_digest=arguments_digest,
        capability_digest=handle.capability.capability_digest,
    )


def test_descriptor_identity_and_summary_omit_command_path_and_env_value(tmp_path: Path) -> None:
    descriptor = MCPServerDescriptor(
        server_id="safe-server",
        argv=(sys.executable, "-u", str(tmp_path / "child.py")),
        cwd=tmp_path.resolve(),
        env={"MCP_SECRET": "never-display"},
        env_allowlist=("MCP_SECRET",),
    )
    safe = descriptor.to_safe_dict()
    encoded = json.dumps(safe, ensure_ascii=True)
    assert "never-display" not in encoded
    assert str(tmp_path) not in encoded
    assert "argv" not in safe
    assert descriptor.identity.descriptor_digest
    assert safe["evidence_class"] == CONTRACT
    assert safe["execution_safety"] == EXECUTION_SAFETY_UNVERIFIED


def test_registry_requires_explicit_allowlisted_server_id_and_is_bounded(tmp_path: Path) -> None:
    descriptor = _descriptor(tmp_path)
    service = MCPRuntimeService(
        [descriptor],
        allowlisted_server_ids=(),
        config=MCPRuntimeConfig(max_servers=1, max_active_calls=1, max_retained_calls=1),
    )
    try:
        assert service.list_server_summaries() == ()
        with pytest.raises(MCPServerNotAllowedError, match="server_not_allowlisted"):
            service.connect("demo")
        assert service.safe_summary()["auto_config"] is False
    finally:
        service.close()


def test_real_connect_exposes_capability_digest_but_not_os_sandbox_claim(tmp_path: Path) -> None:
    service = MCPRuntimeService([_descriptor(tmp_path)], allowlisted_server_ids=("demo",))
    try:
        handle = service.connect("demo")
        assert handle.state is MCPServerState.READY
        assert handle.capability.tool_names == ("echo",)
        assert len(handle.capability.capability_digest) == 64
        assert handle.capability.evidence_class == LIVE_GATE_UNVERIFIED
        assert handle.capability.execution_safety == EXECUTION_SAFETY_UNVERIFIED
        summary = service.list_server_summaries()[0]
        encoded = json.dumps(summary, ensure_ascii=True)
        assert "inputSchema" not in encoded
        assert "description" not in encoded
        assert "sandbox" not in encoded.lower()
        assert "owner_token" not in encoded
        assert '"pid"' not in encoded
        assert summary["approval_required"] is True
    finally:
        service.close()


def test_call_requires_exact_approval_and_stable_operation_is_idempotent(tmp_path: Path) -> None:
    service = MCPRuntimeService([_descriptor(tmp_path)], allowlisted_server_ids=("demo",))
    scope = _scope()
    try:
        handle = service.connect("demo")
        with pytest.raises(MCPApprovalRequiredError, match="approval_required"):
            service.call_tool(
                server_id="demo",
                tool_name="echo",
                arguments={"value": "hello"},
                operation_id="op-1",
                scope=scope,
            )
        grant = _grant(service, operation_id="op-1", value="hello")
        result = service.call_tool(
            server_id="demo",
            tool_name="echo",
            arguments={"value": "hello"},
            operation_id="op-1",
            scope=scope,
            approval=grant,
        )
        repeated = service.call_tool(
            server_id="demo",
            tool_name="echo",
            arguments={"value": "hello"},
            operation_id="op-1",
            scope=scope,
            approval=grant,
        )
        assert repeated == result
        assert result.status == "COMPLETED"
        assert result.evidence_class == LIVE_GATE_UNVERIFIED
        assert result.safe_summary["payload"] == "omitted"
        assert result.safe_summary["text_chars"] == 5
        assert "do-not-project" not in json.dumps(result.to_safe_dict(), ensure_ascii=True)
        assert (tmp_path / "wire.log").read_text(encoding="utf-8").splitlines().count("call:1") == 1
    finally:
        service.close()


def test_harness_approval_to_real_mcp_stdio_call_and_e0_terminal(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "mcp-harness.sqlite")
    descriptor = _descriptor(tmp_path)
    service = MCPRuntimeService(
        [descriptor],
        allowlisted_server_ids=("demo",),
    )
    registry_gate = MCPRegistryGate([descriptor], workspace_root=tmp_path)
    event_store = HarnessEventStore(storage)
    ledger = HarnessOperationLedger(storage)
    broker = HarnessActionBroker(
        workspace_root=tmp_path,
        world_id="world-1",
        event_store=event_store,
        epoch_provider=lambda: 1,
        policy=ExecutionPolicy(
            workspace_root=tmp_path,
            capability_allowlist=("mcp.call",),
            approval_mode=ApprovalMode.ALWAYS,
        ),
        backend=RoutedActionBackend(
            {
                "pulse_mcp_call": MCPActionBackend(
                    service,
                    world_id="world-1",
                    registry_gate=registry_gate,
                )
            }
        ),
        operation_ledger=ledger,
        owner_id="runtime-owner-1",
    )
    try:
        pending = broker.dispatch(
            "engram-1",
            "pulse_mcp_call",
            {
                "server_id": "demo",
                "tool_name": "echo",
                "arguments": {"value": "hello-through-mcp"},
            },
            ToolInvocationContext("mcp-call-1"),
            "turn-mcp-1",
        )
        assert pending["error"] == "approval_required"
        approval_event = event_store.get(pending["event_id"])
        assert approval_event is not None
        preview = approval_event.payload["safe_preview"]
        assert preview["server_id"] == "demo"
        assert preview["tool_name"] == "echo"
        assert len(preview["arguments_digest"]) == 64
        assert len(preview["descriptor_digest"]) == 64
        assert "capability_digest" not in preview
        assert preview["capability_state"] == "declared"
        assert preview["registry_descriptor_id"] == "mcp.demo"
        assert preview["registry_status"] == "enabled"
        assert preview["registry_reason"] == "approval_required"
        assert len(preview["registry_provenance_digest"]) == 64
        assert not (tmp_path / "wire.log").exists(), (
            "approval preview must not spawn or initialize the MCP server"
        )

        resolved = broker.resolve_approval(
            pending["data"]["approval_id"],
            {
                "request_id": "resolve-mcp-call-1",
                "expected_turn_id": "turn-mcp-1",
                "expected_epoch": 1,
                "decision": "allow_once",
            },
        )
        assert resolved["accepted"] is True
        delivered = broker.wait_for_action("engram-1", "mcp-call-1", timeout_seconds=2)
        assert delivered["ok"] is True
        assert delivered["content"] == "hello-through-mcp"
        assert delivered["data"]["mcp_result_digest"]
        operation = ledger.get("tool.pulse_mcp_call", "mcp-call-1")
        assert operation is not None
        assert operation.terminal_state is OperationTerminalState.COMPLETED
        assert operation.recovery_state is OperationRecoveryState.CLEARED
        assert (tmp_path / "wire.log").read_text(encoding="utf-8").count("call:1") == 1
    finally:
        service.close()
        storage.close()


def test_registry_reauthorizes_after_approval_before_starting_mcp_process(
    tmp_path: Path,
) -> None:
    descriptor = _descriptor(tmp_path)
    service = MCPRuntimeService([descriptor], allowlisted_server_ids=("demo",))
    registry_gate = MCPRegistryGate([descriptor], workspace_root=tmp_path)
    backend = MCPActionBackend(
        service,
        world_id="world-1",
        registry_gate=registry_gate,
    )
    try:
        input_data = {
            "server_id": "demo",
            "tool_name": "echo",
            "arguments": {"value": "must-not-run"},
        }
        preview = backend.preview_for("pulse_mcp_call", input_data)
        assert not (tmp_path / "wire.log").exists()
        registry_gate.registry.quarantine("mcp.demo", "operator_quarantine")

        result = backend.execute(
            action_request_id="mcp-registry-fenced",
            engram_id="engram-1",
            turn_id="turn-1",
            epoch=1,
            tool_name="pulse_mcp_call",
            input_data=input_data,
            policy_preview=preview,
        )

        assert result["ok"] is False
        assert result["error"] == "registry_descriptor_quarantined"
        assert not (tmp_path / "wire.log").exists()
    finally:
        service.close()


def test_mcp_terminal_loss_never_resends_remote_operation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    storage = Storage(tmp_path / "mcp-recovery.sqlite")
    service = MCPRuntimeService(
        [_descriptor(tmp_path)],
        allowlisted_server_ids=("demo",),
    )
    event_store = HarnessEventStore(storage)
    ledger = HarnessOperationLedger(storage)
    policy = ExecutionPolicy(
        workspace_root=tmp_path,
        capability_allowlist=("mcp.call",),
        approval_mode=ApprovalMode.ALWAYS,
    )
    backend = RoutedActionBackend(
        {"pulse_mcp_call": MCPActionBackend(service, world_id="world-1")}
    )
    broker = HarnessActionBroker(
        workspace_root=tmp_path,
        world_id="world-1",
        event_store=event_store,
        epoch_provider=lambda: 1,
        policy=policy,
        backend=backend,
        operation_ledger=ledger,
        owner_id="runtime-owner-1",
    )
    original_terminal_append = event_store.append_terminal_operation
    try:
        pending = broker.dispatch(
            "engram-1",
            "pulse_mcp_call",
            {
                "server_id": "demo",
                "tool_name": "echo",
                "arguments": {"value": "one-remote-call"},
            },
            ToolInvocationContext("mcp-recovery-1"),
            "turn-mcp-recovery",
        )

        def fail_terminal(*_args, **_kwargs):
            raise OSError("injected MCP atomic terminal loss")

        monkeypatch.setattr(event_store, "append_terminal_operation", fail_terminal)
        resolved = broker.resolve_approval(
            pending["data"]["approval_id"],
            {
                "request_id": "resolve-mcp-recovery-1",
                "expected_turn_id": "turn-mcp-recovery",
                "expected_epoch": 1,
                "decision": "allow_once",
            },
        )
        assert resolved["execution_status"] == "uncertain"
        operation = ledger.get("tool.pulse_mcp_call", "mcp-recovery-1")
        assert operation is not None
        assert operation.terminal_state is OperationTerminalState.UNCERTAIN
        assert operation.recovery_state is OperationRecoveryState.REQUIRED
        assert (tmp_path / "wire.log").read_text(encoding="utf-8").count("call:1") == 1

        monkeypatch.setattr(
            event_store,
            "append_terminal_operation",
            original_terminal_append,
        )
        restarted = HarnessActionBroker(
            workspace_root=tmp_path,
            world_id="world-1",
            event_store=event_store,
            epoch_provider=lambda: 1,
            policy=policy,
            backend=backend,
            operation_ledger=ledger,
            owner_id="runtime-owner-1",
        )
        replay = restarted.dispatch(
            "engram-1",
            "pulse_mcp_call",
            {
                "server_id": "demo",
                "tool_name": "echo",
                "arguments": {"value": "one-remote-call"},
            },
            ToolInvocationContext("mcp-recovery-1"),
            "turn-mcp-recovery",
        )
        assert replay["ok"] is False
        assert replay["error"] == "operation_recovery_required"
        assert (tmp_path / "wire.log").read_text(encoding="utf-8").count("call:1") == 1
    finally:
        service.close()
        storage.close()


def test_approval_scope_argument_and_capability_mismatch_fail_closed(tmp_path: Path) -> None:
    service = MCPRuntimeService([_descriptor(tmp_path)], allowlisted_server_ids=("demo",))
    try:
        service.connect("demo")
        grant = _grant(service, operation_id="op-1", value="hello")
        with pytest.raises(MCPApprovalMismatchError, match="approval_mismatch"):
            service.call_tool(
                server_id="demo",
                tool_name="echo",
                arguments={"value": "different"},
                operation_id="op-1",
                scope=_scope(),
                approval=grant,
            )
    finally:
        service.close()


def test_operation_id_collision_cannot_change_scope_or_payload(tmp_path: Path) -> None:
    service = MCPRuntimeService([_descriptor(tmp_path)], allowlisted_server_ids=("demo",))
    try:
        service.connect("demo")
        grant = _grant(service, operation_id="op-1", value="hello")
        service.call_tool(
            server_id="demo",
            tool_name="echo",
            arguments={"value": "hello"},
            operation_id="op-1",
                scope=_scope(),
                approval=grant,
            )
        changed_scope = MCPCallScope(
            world_id="world-1", engram_id="engram-1", turn_id="turn-2", epoch=1
        )
        changed_scope_grant = _grant(
            service,
            operation_id="op-1",
            value="hello",
            scope=changed_scope,
        )
        with pytest.raises(MCPOperationCollisionError, match="operation_scope_collision"):
            service.call_tool(
                server_id="demo",
                tool_name="echo",
                arguments={"value": "hello"},
                operation_id="op-1",
                scope=changed_scope,
                approval=changed_scope_grant,
            )
    finally:
        service.close()


def test_completed_call_registry_evicts_within_bound(tmp_path: Path) -> None:
    service = MCPRuntimeService(
        [_descriptor(tmp_path)],
        allowlisted_server_ids=("demo",),
        config=MCPRuntimeConfig(max_active_calls=1, max_retained_calls=1),
    )
    try:
        service.connect("demo")
        for operation_id, value in (("op-1", "one"), ("op-2", "two")):
            service.call_tool(
                server_id="demo",
                tool_name="echo",
                arguments={"value": value},
                operation_id=operation_id,
                scope=_scope(),
                approval=_grant(service, operation_id=operation_id, value=value),
            )
        assert service.safe_summary()["registry"]["retained_calls"] == 1
    finally:
        service.close()


def test_cancel_signals_transport_and_close_is_idempotent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = MCPRuntimeService([_descriptor(tmp_path, mode="cancel")], allowlisted_server_ids=("demo",))
    service.connect("demo")
    claims: list[bool] = []
    original_detach = service.detach_to_retained_locked

    def observed_detach(session, transport, key, reason, *, terminal_state):
        claims.append(session.transport is transport)
        return original_detach(
            session,
            transport,
            key,
            reason,
            terminal_state=terminal_state,
        )

    monkeypatch.setattr(service, "detach_to_retained_locked", observed_detach)
    scope = _scope()
    grant = _grant(service, operation_id="op-cancel", value="wait")
    observed: list[str] = []

    def invoke() -> None:
        try:
            service.call_tool(
                server_id="demo",
                tool_name="echo",
                arguments={"value": "wait"},
                operation_id="op-cancel",
                scope=scope,
                approval=grant,
                timeout=2.0,
            )
        except MCPCallExecutionError as exc:
            observed.append(exc.code)

    thread = threading.Thread(target=invoke, daemon=True)
    thread.start()
    deadline = time.monotonic() + 1.0
    while not (tmp_path / "wire.log").exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert service.cancel(server_id="demo", operation_id="op-cancel") is True
    thread.join(timeout=2.0)
    assert observed == ["call_cancelled"]
    assert claims and all(claims)
    service.close()


def test_connect_follower_wait_uses_exact_remaining_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = MCPRuntimeService(
        [_descriptor(tmp_path)],
        allowlisted_server_ids=("demo",),
        config=MCPRuntimeConfig(max_connect_wait_sec=0.015),
    )
    clock = [100.0]
    waits: list[float] = []

    class _DeadlineWaiter:
        def wait(self, timeout: float) -> bool:
            waits.append(timeout)
            clock[0] += timeout
            return False

        def set(self) -> None:
            return None

    monkeypatch.setattr(
        mcp_runtime_module.time,
        "monotonic",
        lambda: clock[0],
    )
    with service._lock:
        service._sessions["demo"].state = MCPServerState.CONNECTING
        service._connect_waiters["demo"] = _DeadlineWaiter()  # type: ignore[assignment]

    try:
        with pytest.raises(MCPRuntimeError) as caught:
            service.connect("demo")

        assert caught.value.code == "connect_wait_timeout"
        assert waits == [pytest.approx(0.015)]
        assert clock[0] == pytest.approx(100.015)
    finally:
        with service._lock:
            service._connect_waiters.clear()
            service._sessions["demo"].state = MCPServerState.FAILED
        service.close(deadline=clock[0] + 0.1)
    service.close()
    assert service.safe_summary()["state"] == "CLOSED"


def test_action_backend_pre_cancel_never_connects_or_spawns(tmp_path: Path) -> None:
    service = MCPRuntimeService(
        [_descriptor(tmp_path)],
        allowlisted_server_ids=("demo",),
    )
    backend = MCPActionBackend(service, world_id="world-1")
    input_data = {
        "server_id": "demo",
        "tool_name": "echo",
        "arguments": {"value": "must-not-run"},
    }
    preview = backend.preview_for("pulse_mcp_call", input_data)
    cancelled = threading.Event()
    cancelled.set()

    result = backend.execute(
        action_request_id="mcp-pre-cancel",
        engram_id="engram-1",
        turn_id="turn-1",
        epoch=1,
        tool_name="pulse_mcp_call",
        input_data=input_data,
        policy_preview=preview,
        signal=cancelled,
    )

    assert result["status"] == "cancelled"
    assert result["execution_status"] == "cancelled"
    assert result["recovery_state"] == "none"
    assert service.list_server_summaries()[0]["state"] == "DECLARED"
    assert service.approval_preview("demo", "echo")["process_started"] is False
    assert not (tmp_path / "wire.log").exists()
    summary = service.close(deadline=time.monotonic() + 0.2)
    assert summary["process_roots_observed"] == 0
    assert summary["owner_joined"] is True


def test_runtime_service_wires_strict_publication_permit_to_spawn(
    tmp_path: Path,
) -> None:
    descriptor = _descriptor(tmp_path)
    with pytest.raises(TypeError, match="RuntimePublicationPermit"):
        MCPRuntimeService(
            [descriptor],
            allowlisted_server_ids=("demo",),
            publication_permit=object(),
        )

    gate = RuntimePublicationGate("runtime-mcp-service", 3)
    service = MCPRuntimeService(
        [descriptor],
        allowlisted_server_ids=("demo",),
        publication_permit=gate.publication_permit,
    )
    gate.revoke(reason="runtime_close")
    with pytest.raises(MCPRuntimeError) as failure:
        service.connect("demo")
    assert failure.value.code == "publication_revoked"
    assert not (tmp_path / "wire.log").exists()
    summary = service.close(deadline=time.monotonic() + 0.2)
    assert summary["process_roots_observed"] == 0
    assert summary["owner_joined"] is True


def test_close_active_call_returns_payload_free_physical_owner_evidence(
    tmp_path: Path,
) -> None:
    service = MCPRuntimeService(
        [_descriptor(tmp_path, mode="cancel")],
        allowlisted_server_ids=("demo",),
    )
    service.connect("demo")
    grant = _grant(service, operation_id="op-hard-close", value="wait")
    observed: list[str] = []

    def invoke() -> None:
        try:
            service.call_tool(
                server_id="demo",
                tool_name="echo",
                arguments={"value": "wait"},
                operation_id="op-hard-close",
                scope=_scope(),
                approval=grant,
                timeout=3.0,
            )
        except MCPCallExecutionError as exc:
            observed.append(exc.code)

    owner = threading.Thread(target=invoke, daemon=True)
    owner.start()
    wire = tmp_path / "wire.log"
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        if wire.exists() and "call:1" in wire.read_text(encoding="utf-8"):
            break
        time.sleep(0.01)
    assert wire.exists()

    summary = service.close(deadline=time.monotonic() + 3.0)
    owner.join(timeout=3.0)

    assert not owner.is_alive()
    assert observed == ["call_cancelled"]
    assert summary["active_calls_before"] == 1
    assert summary["active_calls_unresolved"] == 0
    assert summary["transport_owners_unresolved"] == 0
    assert summary["reader_owners_unresolved"] == 0
    assert summary["process_roots_observed"] == 1
    assert summary["process_root_owners_unresolved"] == 0
    assert summary["owner_joined"] is True
    assert summary["process_tree_state"] == (
        "empty_verified" if os.name == "nt" else "root_exit_only"
    )
    assert "wait" not in json.dumps(summary, sort_keys=True)


def test_connect_exception_retains_unresolved_transport_until_owner_exits(
    tmp_path: Path,
    monkeypatch,
) -> None:
    descriptor = _descriptor(tmp_path)
    transport = _ControlledCloseTransport(tmp_path, fail_start=True)
    monkeypatch.setattr(
        MCPServerDescriptor,
        "build_transport",
        lambda _descriptor, *, publication_permit=None: transport,
    )
    service = MCPRuntimeService(
        [descriptor],
        allowlisted_server_ids=("demo",),
    )
    try:
        with pytest.raises(MCPRuntimeError, match="controlled_start_failure"):
            service.connect("demo")

        assert service.list_server_summaries()[0]["state"] == "FAILED"
        assert service.approval_preview("demo", "echo")["process_started"] is False
        assert (
            service.safe_summary()["registry"]["retained_closing_transports"]
            == 1
        )

        unresolved = service.close(deadline=time.monotonic() + 0.1)
        assert unresolved["transports_observed"] == 1
        assert unresolved["transport_owners_unresolved"] == 1
        assert unresolved["owner_joined"] is False
        assert unresolved["process_tree_state"] == "not_applicable"

        transport.release_owner()
        settled = service.close(deadline=time.monotonic() + 0.1)
        assert settled["transports_observed"] == 1
        assert settled["unresolved"] == 0
        assert settled["owner_joined"] is True
        assert settled["process_tree_state"] == "not_applicable"
        assert (
            service.safe_summary()["registry"]["retained_closing_transports"]
            == 0
        )
        assert service.close(deadline=time.monotonic() + 0.1)[
            "transports_observed"
        ] == 0
    finally:
        transport.release_owner()
        service.close(deadline=time.monotonic() + 0.1)


@pytest.mark.skipif(os.name != "nt", reason="requires Windows Job Objects")
@pytest.mark.parametrize(
    "failed_thread_name, expected_started_threads",
    (
        ("pulse-mcp-writer", 0),
        ("pulse-mcp-stderr", 2),
    ),
    ids=("first-io-owner", "partial-io-owners"),
)
def test_io_thread_start_failure_enters_retained_seam_and_reconnects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed_thread_name: str,
    expected_started_threads: int,
) -> None:
    descriptor = _descriptor(tmp_path)
    service = MCPRuntimeService(
        [descriptor],
        allowlisted_server_ids=("demo",),
    )
    built: list[MCPStdioTransport] = []
    detach_reasons: list[str] = []
    real_build = MCPServerDescriptor.build_transport
    real_detach = service.detach_to_retained_locked

    def captured_build(
        descriptor: MCPServerDescriptor,
        *,
        publication_permit=None,
    ) -> MCPStdioTransport:
        transport = real_build(
            descriptor,
            publication_permit=publication_permit,
        )
        built.append(transport)
        return transport

    def observed_detach(*args, **kwargs):
        detach_reasons.append(str(kwargs.get("reason", "")))
        return real_detach(*args, **kwargs)

    monkeypatch.setattr(MCPServerDescriptor, "build_transport", captured_build)
    monkeypatch.setattr(service, "detach_to_retained_locked", observed_detach)
    native_start = threading.Thread.start
    injected = False

    def fail_selected_start_once(thread: threading.Thread) -> None:
        nonlocal injected
        if not injected and thread.name == failed_thread_name:
            injected = True
            raise RuntimeError("injected_mcp_io_thread_start_failure")
        native_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_selected_start_once)
    try:
        with pytest.raises(MCPRuntimeError, match="io_thread_start_failed"):
            service.connect("demo")

        assert injected is True
        assert len(built) == 1
        failed_transport = built[0]
        io_threads = (
            failed_transport._writer_thread,
            failed_transport._stdout_thread,
            failed_transport._stderr_thread,
        )
        assert sum(
            thread is not None and thread.ident is not None
            for thread in io_threads
        ) == expected_started_threads
        assert failed_transport._process is not None
        assert failed_transport._process.poll() is not None
        assert detach_reasons
        assert service._sessions["demo"].state is MCPServerState.FAILED
        assert service._sessions["demo"].transport is None
        assert "demo" not in service._connect_waiters
        assert (
            service.safe_summary()["registry"]["retained_closing_transports"]
            == 0
        )

        successor = service.connect("demo")

        assert successor.state is MCPServerState.READY
        assert service._sessions["demo"].connect_generation == 2
        assert len(built) == 2
    finally:
        service.close(deadline=time.monotonic() + 2.0)


def test_close_server_retains_detached_transport_until_owner_exits(
    tmp_path: Path,
) -> None:
    service = MCPRuntimeService(
        [_descriptor(tmp_path)],
        allowlisted_server_ids=("demo",),
    )
    transport = _ControlledCloseTransport(tmp_path)
    service._sessions["demo"].transport = transport
    try:
        service.close_server("demo")

        assert service._sessions["demo"].transport is None
        assert (
            service.safe_summary()["registry"]["retained_closing_transports"]
            == 1
        )
        unresolved = service.close(deadline=time.monotonic() + 0.1)
        assert unresolved["transports_observed"] == 1
        assert unresolved["transport_owners_unresolved"] == 1
        assert unresolved["owner_joined"] is False

        transport.release_owner()
        settled = service.close(deadline=time.monotonic() + 0.1)
        assert settled["transports_observed"] == 1
        assert settled["unresolved"] == 0
        assert settled["owner_joined"] is True
        assert (
            service.safe_summary()["registry"]["retained_closing_transports"]
            == 0
        )
    finally:
        transport.release_owner()
        service.close(deadline=time.monotonic() + 0.1)


def test_service_broadcasts_every_transport_before_waiting_on_shared_deadline(
    tmp_path: Path,
) -> None:
    descriptors = (
        _descriptor(tmp_path, server_id="one"),
        _descriptor(tmp_path, server_id="two"),
    )
    service = MCPRuntimeService(
        descriptors,
        allowlisted_server_ids=("one", "two"),
    )
    signalled = {server_id: threading.Event() for server_id in ("one", "two")}
    close_observations: list[bool] = []

    class BlockingTransport(MCPStdioTransport):
        pid = None
        is_running = False

        def __init__(self, server_id: str) -> None:
            super().__init__(
                argv=(sys.executable, "-c", "pass"),
                cwd=tmp_path.resolve(),
            )
            self.server_id = server_id

        def signal_close(self) -> None:
            signalled[self.server_id].set()

        def close(self, *, deadline: float) -> MCPTransportCloseSummary:
            close_observations.append(all(event.is_set() for event in signalled.values()))
            while time.monotonic() < deadline:
                time.sleep(0.005)
            return {
                "active_before": 1,
                "unresolved": 1,
                "transport_owners_unresolved": 1,
                "reader_owners_unresolved": 0,
                "process_roots_observed": 0,
                "process_root_owners_unresolved": 0,
                "owner_joined": False,
                "process_tree_state": "not_applicable",
            }

    for server_id in ("one", "two"):
        service._sessions[server_id].transport = BlockingTransport(server_id)  # type: ignore[assignment]

    started = time.monotonic()
    summary = service.close(deadline=started + 0.06)
    elapsed = time.monotonic() - started

    assert close_observations == [True, True]
    assert elapsed < 0.2
    assert summary["transports_observed"] == 2
    assert summary["transport_owners_unresolved"] == 2
    assert summary["owner_joined"] is False
    assert summary["process_tree_state"] == "not_applicable"


def test_physical_owner_key_binds_generation_identity_and_never_pid() -> None:
    digest = "a" * 64
    first = MCPPhysicalOwnerKey(
        server_id="demo",
        descriptor_digest=digest,
        connect_generation=1,
        owner_token="1" * 32,
    )
    different_owner = MCPPhysicalOwnerKey(
        server_id="demo",
        descriptor_digest=digest,
        connect_generation=1,
        owner_token="2" * 32,
    )
    different_generation = MCPPhysicalOwnerKey(
        server_id="demo",
        descriptor_digest=digest,
        connect_generation=2,
        owner_token="1" * 32,
    )

    assert first != different_owner
    assert first != different_generation
    assert "pid" not in first.__dataclass_fields__
    with pytest.raises(ValueError, match="positive int"):
        MCPPhysicalOwnerKey(
            server_id="demo",
            descriptor_digest=digest,
            connect_generation=0,
            owner_token="1" * 32,
        )


def test_reconcile_keeps_root_only_then_retires_same_record_on_late_empty(
    tmp_path: Path,
) -> None:
    service = MCPRuntimeService(
        [_descriptor(tmp_path)],
        allowlisted_server_ids=("demo",),
    )
    transport = _StagedCloseTransport(
        tmp_path,
        _staged_close_summary("root_exit_only"),
        _staged_close_summary("empty_verified"),
    )
    record = _attach_then_retain_for_test(service, transport)
    try:
        first = service.reconcile_server_close(
            "demo",
            deadline=time.monotonic() + 0.1,
        )
        assert first is not None
        assert first["process_tree_state"] == "root_exit_only"
        assert service.safe_summary()["registry"]["retained_closing_transports"] == 1

        final = service.reconcile_server_close(
            "demo",
            deadline=time.monotonic() + 0.1,
        )
        assert final is not None
        assert final["process_tree_state"] == "empty_verified"
        assert service.safe_summary()["registry"]["retained_closing_transports"] == 0

        # A late result from the exact retired record is fenced and cannot
        # recreate a retained owner or alter the next generation.
        late = service._observe_closing_transport(
            record,
            deadline=time.monotonic() + 0.1,
        )
        assert late.process_tree_state == "empty_verified"
        assert service.safe_summary()["registry"]["retained_closing_transports"] == 0
    finally:
        service.close(deadline=time.monotonic() + 0.1)


def test_empty_tree_does_not_retire_before_witness_resource_converges(
    tmp_path: Path,
) -> None:
    service = MCPRuntimeService(
        [_descriptor(tmp_path)],
        allowlisted_server_ids=("demo",),
    )
    transport = _StagedCloseTransport(
        tmp_path,
        _staged_close_summary(
            "empty_verified",
            process_owner_unresolved=1,
        ),
        _staged_close_summary("empty_verified"),
    )
    _attach_then_retain_for_test(service, transport)
    try:
        witness_pending = service.reconcile_server_close(
            "demo",
            deadline=time.monotonic() + 0.1,
        )
        assert witness_pending is not None
        assert witness_pending["process_tree_state"] == "empty_verified"
        assert witness_pending["process_root_owners_unresolved"] == 1
        assert witness_pending["owner_joined"] is False
        assert service.safe_summary()["registry"]["retained_closing_transports"] == 1

        converged = service.reconcile_server_close(
            "demo",
            deadline=time.monotonic() + 0.1,
        )
        assert converged is not None
        assert converged["process_tree_state"] == "empty_verified"
        assert converged["owner_joined"] is True
        assert service.safe_summary()["registry"]["retained_closing_transports"] == 0
    finally:
        service.close(deadline=time.monotonic() + 0.1)


def test_empty_tree_does_not_retire_until_stdio_reader_joins(tmp_path: Path) -> None:
    service = MCPRuntimeService(
        [_descriptor(tmp_path)],
        allowlisted_server_ids=("demo",),
    )
    transport = _StagedCloseTransport(
        tmp_path,
        _staged_close_summary("empty_verified", reader_unresolved=1),
        _staged_close_summary("empty_verified"),
    )
    _attach_then_retain_for_test(service, transport)
    try:
        reader_pending = service.reconcile_server_close(
            "demo",
            deadline=time.monotonic() + 0.1,
        )
        assert reader_pending is not None
        assert reader_pending["process_tree_state"] == "empty_verified"
        assert reader_pending["reader_owners_unresolved"] == 1
        assert reader_pending["owner_joined"] is False
        assert service.safe_summary()["registry"]["retained_closing_transports"] == 1

        settled = service.reconcile_server_close(
            "demo",
            deadline=time.monotonic() + 0.1,
        )
        assert settled is not None
        assert settled["owner_joined"] is True
        assert service.safe_summary()["registry"]["retained_closing_transports"] == 0
    finally:
        service.close(deadline=time.monotonic() + 0.1)


@pytest.mark.parametrize(
    ("mode", "timeout", "expected_code"),
    (
        ("cancel", 0.05, "request_timeout"),
        ("unexpected-id", 1.0, "unexpected_response_id"),
        ("reader-fatal", 1.0, "stdout_protocol_pollution"),
        ("stderr-fatal", 1.0, "stderr_limit_exceeded"),
    ),
)
def test_terminal_call_paths_claim_attached_transport_before_detach(
    tmp_path: Path,
    monkeypatch,
    mode: str,
    timeout: float,
    expected_code: str,
) -> None:
    service = MCPRuntimeService(
        [_descriptor(tmp_path, mode=mode)],
        allowlisted_server_ids=("demo",),
    )
    service.connect("demo")
    transport = service._sessions["demo"].transport
    assert transport is not None
    grant = _grant(service, operation_id=f"op-{mode}", value="terminal")
    claims: list[tuple[str, bool]] = []
    original = service.detach_to_retained_locked

    def observed_detach(session, candidate, key, reason, *, terminal_state):
        claims.append((reason, session.transport is candidate))
        return original(
            session,
            candidate,
            key,
            reason,
            terminal_state=terminal_state,
        )

    monkeypatch.setattr(service, "detach_to_retained_locked", observed_detach)
    try:
        with pytest.raises(MCPCallExecutionError) as failure:
            service.call_tool(
                server_id="demo",
                tool_name="echo",
                arguments={"value": "terminal"},
                operation_id=f"op-{mode}",
                scope=_scope(),
                approval=grant,
                timeout=timeout,
            )
        assert failure.value.code == expected_code
        assert claims
        assert all(was_attached for _reason, was_attached in claims)
        assert service._sessions["demo"].transport is None
    finally:
        service.close(deadline=time.monotonic() + 0.5)


def test_generic_call_adapter_failure_claims_before_close_signal(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = MCPRuntimeService(
        [_descriptor(tmp_path)],
        allowlisted_server_ids=("demo",),
    )
    service.connect("demo")
    session = service._sessions["demo"]
    transport = session.transport
    assert transport is not None
    grant = _grant(service, operation_id="op-adapter-failure", value="value")
    observed: list[tuple[bool, str]] = []
    original_detach = service.detach_to_retained_locked

    def fail_call(*_args, **_kwargs):
        raise RuntimeError("injected adapter failure")

    def observed_detach(session_arg, candidate, key, reason, *, terminal_state):
        observed.append((session_arg.transport is candidate, candidate.phase))
        return original_detach(
            session_arg,
            candidate,
            key,
            reason,
            terminal_state=terminal_state,
        )

    monkeypatch.setattr(transport, "call_tool", fail_call)
    monkeypatch.setattr(service, "detach_to_retained_locked", observed_detach)
    try:
        with pytest.raises(MCPCallExecutionError) as failure:
            service.call_tool(
                server_id="demo",
                tool_name="echo",
                arguments={"value": "value"},
                operation_id="op-adapter-failure",
                scope=_scope(),
                approval=grant,
                timeout=0.2,
            )
        assert failure.value.code == "call_adapter_failed"
        assert observed == [(True, "READY")]
    finally:
        service.close(deadline=time.monotonic() + 0.5)


def test_timeout_close_reuses_caller_absolute_deadline(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = MCPRuntimeService(
        [_descriptor(tmp_path, mode="cancel")],
        allowlisted_server_ids=("demo",),
    )
    service.connect("demo")
    transport = service._sessions["demo"].transport
    assert transport is not None
    grant = _grant(service, operation_id="op-deadline", value="wait")
    observed_deadlines: list[float] = []
    real_close = transport.close

    def observed_close(*, deadline=None):
        assert deadline is not None
        observed_deadlines.append(deadline)
        return real_close(deadline=deadline)

    monkeypatch.setattr(transport, "close", observed_close)
    started = time.monotonic()
    try:
        with pytest.raises(MCPCallExecutionError) as failure:
            service.call_tool(
                server_id="demo",
                tool_name="echo",
                arguments={"value": "wait"},
                operation_id="op-deadline",
                scope=_scope(),
                approval=grant,
                timeout=0.05,
            )
        elapsed = time.monotonic() - started
        assert failure.value.code == "request_timeout"
        assert observed_deadlines
        assert max(observed_deadlines) <= started + 0.08
        assert elapsed < 0.25
    finally:
        service.close(deadline=time.monotonic() + 0.5)


def test_concurrent_retries_retire_old_record_and_spawn_one_successor(
    tmp_path: Path,
    monkeypatch,
) -> None:
    descriptor = _descriptor(tmp_path)
    service = MCPRuntimeService(
        [descriptor],
        allowlisted_server_ids=("demo",),
    )
    old_transport = _StagedCloseTransport(
        tmp_path,
        _staged_close_summary("root_exit_only"),
        _staged_close_summary("empty_verified"),
    )
    old_record = _attach_then_retain_for_test(service, old_transport)
    first = service.reconcile_server_close(
        "demo",
        deadline=time.monotonic() + 0.1,
    )
    assert first is not None
    assert first["process_tree_state"] == "root_exit_only"

    real_build = MCPServerDescriptor.build_transport
    build_count = 0
    build_lock = threading.Lock()

    def counted_build(self, *, publication_permit=None):
        nonlocal build_count
        with build_lock:
            build_count += 1
        return real_build(self, publication_permit=publication_permit)

    monkeypatch.setattr(MCPServerDescriptor, "build_transport", counted_build)
    barrier = threading.Barrier(3)
    handles = []
    failures: list[Exception] = []

    def retry() -> None:
        barrier.wait()
        try:
            handles.append(service.connect("demo"))
        except Exception as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    owners = [threading.Thread(target=retry, daemon=True) for _ in range(2)]
    for owner in owners:
        owner.start()
    barrier.wait()
    for owner in owners:
        owner.join(timeout=3.0)
    try:
        assert all(not owner.is_alive() for owner in owners)
        assert failures == []
        assert len(handles) == 2
        assert build_count == 1
        successor = service._sessions["demo"].transport
        successor_key = service._sessions["demo"].active_owner_key
        assert successor is not None
        assert isinstance(successor_key, MCPPhysicalOwnerKey)
        assert successor_key.connect_generation == 2

        service._observe_closing_transport(
            old_record,
            deadline=time.monotonic() + 0.1,
        )
        assert service._sessions["demo"].transport is successor
        assert service._sessions["demo"].active_owner_key == successor_key
    finally:
        service.close(deadline=time.monotonic() + 1.0)


@pytest.mark.skipif(
    os.name != "nt",
    reason="requires canonical Windows Job tree evidence and exact termination",
)
def test_real_job_tree_retained_to_empty_then_spawns_one_successor(
    tmp_path: Path,
) -> None:
    descriptor = _descriptor(tmp_path, mode="stop-reading-tree-after-list")
    service = MCPRuntimeService(
        [descriptor],
        allowlisted_server_ids=("demo",),
    )
    service.connect("demo")
    wire_path = tmp_path / "wire.log"
    descendant_path = tmp_path / "descendant.log"
    readiness_deadline = time.monotonic() + 2.0
    while time.monotonic() < readiness_deadline:
        wire = (
            wire_path.read_text(encoding="utf-8").splitlines()
            if wire_path.exists()
            else []
        )
        descendants = (
            descendant_path.read_text(encoding="utf-8").splitlines()
            if descendant_path.exists()
            else []
        )
        if "stdin_reader_stopped" in wire and descendants == ["started"]:
            break
        time.sleep(0.01)
    assert "stdin_reader_stopped" in wire
    assert descendants == ["started"]

    with service._lock:
        session = service._sessions["demo"]
        old_transport = session.transport
        old_key = session.active_owner_key
    assert old_transport is not None
    assert isinstance(old_key, MCPPhysicalOwnerKey)
    call_outcome: list[str] = []

    def blocked_call() -> None:
        try:
            old_transport.call_tool(
                "echo",
                {"chunks": ["x" * 512 for _ in range(100)]},
                timeout=2.0,
            )
        except MCPError as exc:
            call_outcome.append(exc.code)

    call_owner = threading.Thread(target=blocked_call, daemon=True)
    call_owner.start()
    time.sleep(0.05)
    assert call_owner.is_alive()

    # This is the production claim-before-detach seam; the zero-budget first
    # observation deliberately leaves the exact Job owner retained.
    with service._lock:
        old_record = service.detach_to_retained_locked(
            session,
            old_transport,
            old_key,
            "test_real_tree_terminal",
            terminal_state=MCPServerState.BROKEN,
        )
    service._broadcast_closing_transports((old_record,))
    first = service.reconcile_server_close(
        "demo",
        deadline=time.monotonic(),
    )
    assert first is not None
    assert first["owner_joined"] is False
    assert first["process_tree_state"] == "unknown"
    assert service.safe_summary()["registry"]["retained_closing_transports"] == 1
    call_owner.join(timeout=1.0)
    assert not call_owner.is_alive()
    assert call_outcome == ["request_cancelled"]

    barrier = threading.Barrier(3)
    handles = []
    failures: list[Exception] = []

    def reconnect() -> None:
        barrier.wait()
        try:
            handles.append(service.connect("demo"))
        except Exception as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    reconnectors = [
        threading.Thread(target=reconnect, daemon=True) for _ in range(2)
    ]
    for reconnector in reconnectors:
        reconnector.start()
    barrier.wait()
    for reconnector in reconnectors:
        reconnector.join(timeout=5.0)
    try:
        assert all(not reconnector.is_alive() for reconnector in reconnectors)
        assert failures == []
        assert len(handles) == 2
        assert service.safe_summary()["registry"]["retained_closing_transports"] == 0
        assert old_record.last_observation is not None
        assert old_record.last_observation.owner_joined is True
        assert old_record.last_observation.process_tree_state == "empty_verified"
        with service._lock:
            successor = service._sessions["demo"].transport
            successor_key = service._sessions["demo"].active_owner_key
        assert successor is not None
        assert successor is not old_transport
        assert isinstance(successor_key, MCPPhysicalOwnerKey)
        assert successor_key.connect_generation == 2

        successor_tree_deadline = time.monotonic() + 2.0
        while time.monotonic() < successor_tree_deadline:
            wire = wire_path.read_text(encoding="utf-8").splitlines()
            descendants = descendant_path.read_text(encoding="utf-8").splitlines()
            if (
                wire.count("tree_root_started") == 2
                and descendants == ["started", "started"]
            ):
                break
            time.sleep(0.01)
        assert wire.count("tree_root_started") == 2
        assert descendants == ["started", "started"]
    finally:
        service.close(deadline=time.monotonic() + 2.0)


def test_duplicate_server_id_is_rejected_before_any_spawn(tmp_path: Path) -> None:
    first = _descriptor(tmp_path, server_id="duplicate")
    second = _descriptor(tmp_path, server_id="duplicate")
    with pytest.raises(ValueError, match="duplicate MCP server id"):
        MCPRuntimeService(
            (first, second),
            allowlisted_server_ids=("duplicate",),
        )
    assert not (tmp_path / "wire.log").exists()

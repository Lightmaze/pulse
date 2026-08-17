"""World-level contract tests for RuntimeService's persistent Harness."""

from __future__ import annotations

import json
import os
import queue
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from pulse_system.agent.backends.pi import RpcConnectionLost, RpcTimeout
from pulse_system.agent.harness import BINDING_COMPONENT, HarnessError, PiHarnessRuntime
from pulse_system.agent.harness.events import HarnessEventKind, HarnessEventStatus
from pulse_system.agent.harness.operations import (
    OperationRecoveryState,
    OperationTerminalState,
)
from pulse_system.agent.tools.gateway import ToolInvocationContext
from pulse_system.core.causality import CausalLedger
from pulse_system.core.engram import HARNESS_INPUT_COMPONENT
from pulse_system.core.types import Message, MessageRole
from pulse_system.service.runtime import (
    _HarnessControlError,
    _RuntimeHarnessControlGateway,
    RuntimeAssembly,
    RuntimeService,
    RuntimeServiceConfig,
    ServiceError,
)
from pulse_system.service.serve import _parse
from pulse_system.substrate.storage import Storage


class _PersistentPiTransport:
    """Minimal Pi JSONL transport; sessions materialize on the first turn."""

    _EOF = object()

    def __init__(
        self,
        factory: "_PersistentPiFactory",
        label: str,
        session_root: Path,
    ) -> None:
        self.factory = factory
        self.label = label
        self.sent: list[dict] = []
        self.prompts: list[str] = []
        self.switches: list[str] = []
        self.closed = False
        self._out: queue.Queue[str | object] = queue.Queue()
        self._auto_compaction = True
        self._last_text = ""
        self._turn = 0
        self.session_id = f"{label}-fresh"
        self.session_file = os.path.abspath(
            session_root / f"{self.session_id}.jsonl"
        )
        self._session_root = session_root

    def send_line(self, text: str) -> None:
        if self.closed:
            raise RpcConnectionLost("fake Pi transport is closed")
        command = json.loads(text)
        self.sent.append(command)
        kind = command["type"]
        if kind == "get_state":
            self._respond(command, data={
                "sessionId": self.session_id,
                "sessionFile": self.session_file,
                "autoCompactionEnabled": self._auto_compaction,
                "isStreaming": False,
                "pendingMessageCount": 0,
            })
        elif kind == "set_auto_compaction":
            self._auto_compaction = bool(command["enabled"])
            self._respond(command)
        elif kind == "switch_session":
            target = os.path.abspath(command["sessionPath"])
            self.switches.append(target)
            self.session_file = target
            self.session_id = self.factory.session_ids[target]
            self._respond(command, data={"cancelled": False})
        elif kind == "new_session":
            self.session_id = f"{self.label}-child"
            self.session_file = os.path.abspath(
                self._session_root / f"{self.session_id}.jsonl"
            )
            self._respond(command, data={"cancelled": False})
        elif kind == "prompt":
            self._turn += 1
            self.prompts.append(command["message"])
            self._respond(command)
            self._last_text = f"answer-{self.label}-{self._turn}"
            message = {
                "role": "assistant",
                "timestamp": self._turn,
                "stopReason": "stop",
                "content": [{"type": "text", "text": self._last_text}],
                "usage": {
                    "input": 5,
                    "output": 2,
                    "cacheRead": 1,
                    "cacheWrite": 0,
                },
            }
            self._push({"type": "message_end", "message": message})
            path = Path(self.session_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}\n", encoding="utf-8")
            self.factory.session_ids[self.session_file] = self.session_id
            self._push({"type": "agent_settled"})
        elif kind == "get_last_assistant_text":
            self._respond(command, data={"text": self._last_text})
        elif kind in {"abort", "steer"}:
            self._respond(command)
        else:
            self._respond(command, success=False, error=f"unknown command {kind}")

    def read_line(self, timeout: float | None) -> str | None:
        try:
            item = self._out.get(timeout=timeout)
        except queue.Empty:
            raise RpcTimeout from None
        if item is self._EOF:
            return None
        assert isinstance(item, str)
        return item

    def close(self) -> None:
        self.signal_close()

    def signal_close(self) -> bool:
        """Close this in-memory transport without claiming a process exit."""

        if self.closed:
            return False
        self.closed = True
        self._out.put(self._EOF)
        return True

    def wait_closed(self, timeout_sec: float | None = None) -> dict:
        """Return typed owner evidence for this process-free test transport."""

        del timeout_sec
        return {
            "signal_sent": self.closed,
            "process_owners_observed": 0,
            "process_owners_unresolved": 0,
            "reader_owners_observed": 0,
            "reader_owners_unresolved": 0,
            "internal_owner_unresolved": 0,
            "owner_joined": self.closed,
            "process_tree_state": "not_applicable",
            "returncode": 143 if self.closed else None,
            "error_code": None,
        }

    def diagnostics(self) -> dict:
        return {
            "returncode": 143 if self.closed else None,
            "stderr_tail": "",
        }

    def _respond(
        self,
        command: dict,
        *,
        success: bool = True,
        data: dict | None = None,
        error: str | None = None,
    ) -> None:
        payload = {
            "id": command["id"],
            "type": "response",
            "command": command["type"],
            "success": success,
        }
        if data is not None:
            payload["data"] = data
        if error is not None:
            payload["error"] = error
        self._push(payload)

    def _push(self, payload: dict) -> None:
        self._out.put(json.dumps(payload))


class _PersistentPiFactory:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.transports: list[_PersistentPiTransport] = []
        self.session_ids: dict[str, str] = {}

    def __call__(self, _argv: list[str]) -> _PersistentPiTransport:
        return self._open(self.root)

    def open_transport(
        self,
        _argv: list[str],
        *,
        cwd: str | None,
        env: dict[str, str],
    ) -> _PersistentPiTransport:
        del cwd
        configured = env.get("PI_CODING_AGENT_SESSION_DIR")
        session_root = self.root if configured is None else Path(configured)
        return self._open(session_root)

    def _open(self, session_root: Path) -> _PersistentPiTransport:
        transport = _PersistentPiTransport(
            self,
            f"pi{len(self.transports) + 1}",
            session_root,
        )
        self.transports.append(transport)
        return transport


class _PreflightHarness:
    """Lifecycle probe for failures after Harness preflight."""

    def __init__(self) -> None:
        self.preflighted = False
        self.closed = False

    def preflight(self) -> None:
        self.preflighted = True

    def close(self) -> None:
        self.closed = True


def _runtime_factory(transport_factory: _PersistentPiFactory):
    def build(workspace, **kwargs):
        return PiHarnessRuntime(
            workspace,
            transport_factory=transport_factory,
            **kwargs,
        )

    return build


def _pi_config(tmp_path: Path, db: Path | None = None, **overrides):
    values = dict(
        db_path=str(db or (tmp_path / "run.db")),
        workspace=tmp_path,
        mock=False,
        provider="ollama",
        tick_interval=0.01,
        silence_threshold=0.0,
        default_max_wait=0.0,
        harness_turn_timeout_sec=3.0,
    )
    values.update(overrides)
    return RuntimeServiceConfig(**values)


def test_interrupt_fences_actions_before_two_terminal_session_sweeps() -> None:
    order: list[str] = []

    class Broker:
        def cancel_for_turn(self, engram_id, turn_id, *, epoch, reason):
            assert (engram_id, turn_id, epoch, reason) == (
                "engram-1",
                "turn-1",
                7,
                "turn_interrupt",
            )
            order.append("fence_actions")
            return {
                "fenced": True,
                "action_request_ids": ["action-racing-start"],
            }

        def settle_cancellations(self, *, timeout_seconds, action_request_ids):
            assert timeout_seconds == 0.25
            assert action_request_ids == {"action-racing-start"}
            order.append("settle_actions")
            return {"uncertain": 0, "uncertain_action_request_ids": []}

    class Sessions:
        def __init__(self) -> None:
            self.calls = 0

        def stop_turn(self, turn_id, *, engram_id, reason):
            assert turn_id == "turn-1"
            assert engram_id == "engram-1"
            self.calls += 1
            order.append(f"stop_sessions_{self.calls}")
            if self.calls == 1:
                return ()
            assert reason == "turn_interrupt_reconcile"
            return (
                SimpleNamespace(
                    summary=SimpleNamespace(
                        terminal_session_id="session-raced-after-sweep"
                    ),
                    uncertain=False,
                ),
            )

    class Harness:
        def abort(self, engram_id):
            assert engram_id == "engram-1"
            order.append("abort_pi")

    service = SimpleNamespace(
        _world_id="world-1",
        _harness_action_broker=Broker(),
        _harness_terminal_sessions=Sessions(),
        _harness=Harness(),
        _harness_evidence_class=lambda: "LIVE_GATE_UNVERIFIED",
    )
    gateway = _RuntimeHarnessControlGateway(service)
    gateway._scope_turn = lambda turn_id, request: (
        SimpleNamespace(engram_id="engram-1"),
        7,
    )
    event_seq = {"value": 0}

    def append_event(*_args, **_kwargs):
        event_seq["value"] += 1
        return SimpleNamespace(seq=event_seq["value"])

    gateway._append_control_event = append_event

    result = gateway.request_control(
        "interrupt",
        "turn-1",
        {
            "request_id": "interrupt-race-1",
            "expected_epoch": 7,
            "expected_state": "running",
        },
    )

    assert order == [
        "fence_actions",
        "stop_sessions_1",
        "abort_pi",
        "settle_actions",
        "stop_sessions_2",
    ]
    assert result["terminal_sessions"] == {
        "configured": True,
        "stopped": 1,
        "uncertain": False,
        "sweeps": 2,
    }


def test_control_request_id_is_bound_to_complete_scope_and_exact_retry_is_idempotent() -> None:
    calls: list[tuple[str, str]] = []

    class Harness:
        def steer(self, engram_id, message):
            calls.append((engram_id, message))

    service = SimpleNamespace(
        _world_id="world-1",
        _harness_action_broker=None,
        _harness_terminal_sessions=None,
        _harness=Harness(),
        _harness_evidence_class=lambda: "LIVE_GATE_UNVERIFIED",
    )
    gateway = _RuntimeHarnessControlGateway(service)
    turns = {
        "turn-a": SimpleNamespace(engram_id="engram-a"),
        "turn-b": SimpleNamespace(engram_id="engram-b"),
    }
    gateway._scope_turn = lambda turn_id, request: (turns[turn_id], 7)
    event_seq = {"value": 0}

    def append_event(*_args, **_kwargs):
        event_seq["value"] += 1
        return SimpleNamespace(seq=event_seq["value"])

    gateway._append_control_event = append_event
    request = {
        "request_id": "control-shared-id",
        "expected_epoch": 7,
        "expected_state": "running",
        "message": "continue the main work",
    }

    first = gateway.request_control("steer", "turn-a", request)
    replay = gateway.request_control("steer", "turn-a", request)

    assert first["accepted"] is True
    assert replay["idempotent"] is True
    assert calls == [("engram-a", "continue the main work")]
    assert event_seq["value"] == 2

    with pytest.raises(_HarnessControlError) as cross_turn:
        gateway.request_control("steer", "turn-b", request)
    assert cross_turn.value.code == "control_request_scope_conflict"
    assert cross_turn.value.status == 409
    assert calls == [("engram-a", "continue the main work")]

    with pytest.raises(_HarnessControlError) as cross_operation:
        gateway.request_control("interrupt", "turn-a", request)
    assert cross_operation.value.code == "control_request_scope_conflict"
    assert calls == [("engram-a", "continue the main work")]


def test_admission_pending_steer_records_a_durable_failed_terminal() -> None:
    calls: list[str] = []

    class Harness:
        def steer(self, _engram_id, message):
            calls.append(message)
            raise HarnessError(
                "pi_steer_admission_pending",
                "prompt admission is not durable",
                "wait for turn_started with sideband_ready=true",
                phase="sideband",
                retryable=True,
                prompt_accepted=None,
            )

    service = SimpleNamespace(
        _world_id="world-1",
        _harness_action_broker=None,
        _harness_terminal_sessions=None,
        _harness=Harness(),
        _harness_evidence_class=lambda: "FAKE_RPC_CONTRACT",
    )
    gateway = _RuntimeHarnessControlGateway(service)
    gateway._scope_turn = lambda _turn_id, _request: (
        SimpleNamespace(engram_id="engram-a"),
        7,
    )
    events: list[tuple[HarnessEventKind, HarnessEventStatus, dict]] = []

    def append_event(_turn_id, _engram_id, kind, status, payload):
        events.append((kind, status, payload))
        return SimpleNamespace(seq=len(events))

    gateway._append_control_event = append_event
    request = {
        "request_id": "steer-before-sideband-ready",
        "expected_epoch": 7,
        "expected_state": "running",
        "message": "continue after admission",
    }

    with pytest.raises(_HarnessControlError) as rejected:
        gateway.request_control("steer", "turn-a", request)

    assert rejected.value.code == "pi_steer_admission_pending"
    assert rejected.value.uncertain is False
    assert calls == ["continue after admission"]
    assert [(kind, status) for kind, status, _payload in events] == [
        (HarnessEventKind.CONTROL_REQUESTED, HarnessEventStatus.RUNNING),
        (HarnessEventKind.CONTROL_RESOLVED, HarnessEventStatus.FAILED),
    ]
    assert events[-1][2] == {
        "request_id": "steer-before-sideband-ready",
        "operation": "steer",
        "accepted": False,
        "uncertain": False,
        "error_code": "pi_steer_admission_pending",
    }

    replay = gateway.request_control("steer", "turn-a", request)
    assert replay == {
        "request_id": "steer-before-sideband-ready",
        "turn_id": "turn-a",
        "accepted": False,
        "state": "rejected",
        "uncertain": False,
        "error_code": "pi_steer_admission_pending",
        "evidence_class": "FAKE_RPC_CONTRACT",
        "event_seq": 2,
        "idempotent": True,
    }
    assert calls == ["continue after admission"]
    assert len(events) == 2


def test_control_retry_revalidates_turn_before_consulting_idempotency_cache() -> None:
    calls: list[str] = []
    service = SimpleNamespace(
        _world_id="world-1",
        _harness_action_broker=None,
        _harness_terminal_sessions=None,
        _harness=SimpleNamespace(
            steer=lambda _engram_id, message: calls.append(message)
        ),
        _harness_evidence_class=lambda: "LIVE_GATE_UNVERIFIED",
    )
    gateway = _RuntimeHarnessControlGateway(service)
    gateway._scope_turn = lambda _turn_id, _request: (
        SimpleNamespace(engram_id="engram-a"),
        7,
    )
    sequence = iter((1, 2))
    gateway._append_control_event = lambda *_args, **_kwargs: SimpleNamespace(
        seq=next(sequence)
    )
    request = {
        "request_id": "control-revalidate",
        "expected_epoch": 7,
        "expected_state": "running",
        "message": "continue",
    }
    assert gateway.request_control("steer", "turn-a", request)["accepted"] is True

    def stale_scope(_turn_id, _request):
        raise _HarnessControlError(
            "stale_epoch",
            "the lease epoch changed",
            "refresh the turn summary",
            status=409,
        )

    gateway._scope_turn = stale_scope
    with pytest.raises(_HarnessControlError) as stale:
        gateway.request_control("steer", "turn-a", request)
    assert stale.value.code == "stale_epoch"
    assert calls == ["continue"]


def test_concurrent_control_retry_cannot_duplicate_side_effect() -> None:
    entered = threading.Event()
    release = threading.Event()
    calls: list[str] = []

    def steer(_engram_id, message):
        calls.append(message)
        entered.set()
        assert release.wait(timeout=2.0)

    service = SimpleNamespace(
        _world_id="world-1",
        _harness_action_broker=None,
        _harness_terminal_sessions=None,
        _harness=SimpleNamespace(steer=steer),
        _harness_evidence_class=lambda: "LIVE_GATE_UNVERIFIED",
    )
    gateway = _RuntimeHarnessControlGateway(service)
    gateway._scope_turn = lambda _turn_id, _request: (
        SimpleNamespace(engram_id="engram-a"),
        7,
    )
    sequence = iter((1, 2))
    gateway._append_control_event = lambda *_args, **_kwargs: SimpleNamespace(
        seq=next(sequence)
    )
    request = {
        "request_id": "control-concurrent",
        "expected_epoch": 7,
        "expected_state": "running",
        "message": "continue",
    }
    first_result: list[dict] = []
    worker = threading.Thread(
        target=lambda: first_result.append(
            gateway.request_control("steer", "turn-a", request)
        )
    )
    worker.start()
    assert entered.wait(timeout=2.0)

    with pytest.raises(_HarnessControlError) as duplicate:
        gateway.request_control("steer", "turn-a", request)
    assert duplicate.value.code == "control_request_in_flight"

    release.set()
    worker.join(timeout=2.0)
    assert not worker.is_alive()
    assert first_result[0]["accepted"] is True
    assert calls == ["continue"]


def test_foreign_world_turn_cannot_enter_control_or_terminal_side_effects() -> None:
    storage = Storage(":memory:")
    try:
        storage.create_engram(engram_id="engram-current")
        storage.create_engram(engram_id="engram-foreign")
        ledger = CausalLedger(storage)
        current_event = ledger.enqueue(
            world_id="world-current",
            domain="pulse",
            kind="stimulus",
            source="self",
            engram_id="engram-current",
            content="current",
        )
        foreign_event = ledger.enqueue(
            world_id="world-foreign",
            domain="pulse",
            kind="stimulus",
            source="self",
            engram_id="engram-foreign",
            content="foreign",
        )
        ledger.begin_turn(current_event.id, "engram-current", "current")
        foreign_turn = ledger.begin_turn(
            foreign_event.id,
            "engram-foreign",
            "foreign",
        )
        side_effects: list[tuple[str, str]] = []

        service = SimpleNamespace(
            _world_id="world-current",
            _causal_ledger=ledger,
            _lease_keeper=SimpleNamespace(
                assert_owned=lambda: (_ for _ in ()).throw(
                    AssertionError("foreign turn must be rejected before lease use")
                )
            ),
            _harness_action_broker=None,
            _harness_terminal_sessions=SimpleNamespace(
                list_for_turn=lambda *_args, **_kwargs: side_effects.append(
                    ("terminal", "listed")
                )
            ),
            _harness=SimpleNamespace(
                steer=lambda engram_id, message: side_effects.append(
                    (engram_id, message)
                )
            ),
            _harness_evidence_class=lambda: "LIVE_GATE_UNVERIFIED",
        )
        gateway = _RuntimeHarnessControlGateway(service)
        gateway._append_control_event = lambda *_args, **_kwargs: side_effects.append(
            ("control", "event")
        )

        with pytest.raises(_HarnessControlError) as control:
            gateway.request_control(
                "steer",
                foreign_turn.id,
                {
                    "request_id": "foreign-world-control",
                    "expected_epoch": 1,
                    "expected_state": "running",
                    "message": "must not run",
                },
            )
        assert control.value.code == "turn_not_found"

        with pytest.raises(_HarnessControlError) as terminal:
            gateway.list_terminal_sessions(foreign_turn.id)
        assert terminal.value.code == "turn_not_found"
        assert side_effects == []
        assert gateway._results == {}
        assert gateway._request_scopes == {}
        assert gateway._inflight == set()
    finally:
        storage.close()


def test_terminal_session_list_uses_authoritative_turn_engram_scope() -> None:
    calls: list[tuple[str, str, int]] = []

    class Sessions:
        def list_for_turn(self, turn_id, *, engram_id, limit):
            calls.append((turn_id, engram_id, limit))
            return SimpleNamespace(
                to_wire=lambda: {
                    "sessions": [],
                    "count": 0,
                    "evidence_class": "LIVE_GATE_UNVERIFIED",
                }
            )

    service = SimpleNamespace(
        _world_id="world-1",
        _causal_ledger=SimpleNamespace(
            get_turn_for_world=lambda turn_id, world_id: SimpleNamespace(
                id=turn_id,
                engram_id="engram-authoritative",
            )
        ),
        _harness_terminal_sessions=Sessions(),
    )

    result = _RuntimeHarnessControlGateway(service).list_terminal_sessions(
        "turn-1",
        limit=7,
    )

    assert calls == [("turn-1", "engram-authoritative", 7)]
    assert result["count"] == 0


def test_terminal_session_stop_derives_engram_from_parent_turn() -> None:
    calls: list[dict] = []
    summary = SimpleNamespace(
        terminal_session_id="session-1",
        turn_id="turn-1",
        engram_id="engram-authoritative",
    )

    class Sessions:
        def inspect(self, terminal_session_id, **kwargs):
            assert terminal_session_id == "session-1"
            assert kwargs == {
                "expected_engram_id": "engram-authoritative",
                "expected_turn_id": "turn-1",
            }
            return summary

        def stop(self, terminal_session_id, **kwargs):
            calls.append({"terminal_session_id": terminal_session_id, **kwargs})
            return SimpleNamespace(
                to_wire=lambda: {
                    "terminal_session_id": terminal_session_id,
                    "accepted": True,
                }
            )

    service = SimpleNamespace(
        _world_id="world-1",
        _causal_ledger=SimpleNamespace(
            get_turn_for_world=lambda turn_id, world_id: SimpleNamespace(
                id=turn_id,
                engram_id="engram-authoritative",
            )
        ),
        _harness_terminal_sessions=Sessions(),
    )

    result = _RuntimeHarnessControlGateway(service).stop_terminal_session(
        "session-1",
        request={
            "request_id": "stop-1",
            "expected_epoch": 7,
            "expected_turn_id": "turn-1",
            "expected_state": "RUNNING",
        },
    )

    assert result["accepted"] is True
    assert calls == [
        {
            "terminal_session_id": "session-1",
            "request_id": "stop-1",
            "expected_epoch": 7,
            "expected_engram_id": "engram-authoritative",
            "expected_turn_id": "turn-1",
            "expected_state": "RUNNING",
            "reason": "user_stop",
        }
    ]


def test_terminal_session_reads_derive_authoritative_engram_from_selected_turn() -> None:
    calls: list[tuple[str, dict]] = []
    summary = SimpleNamespace(
        to_wire=lambda: {
            "terminal_session_id": "session-1",
            "turn_id": "turn-1",
            "engram_id": "engram-authoritative",
        }
    )

    class Sessions:
        def inspect(self, terminal_session_id, **kwargs):
            calls.append(("inspect", {"id": terminal_session_id, **kwargs}))
            return summary

        def read_output(self, terminal_session_id, **kwargs):
            calls.append(("output", {"id": terminal_session_id, **kwargs}))
            return SimpleNamespace(
                to_wire=lambda: {
                    "terminal_session_id": terminal_session_id,
                    "output": [],
                }
            )

    service = SimpleNamespace(
        _world_id="world-1",
        _causal_ledger=SimpleNamespace(
            get_turn_for_world=lambda turn_id, world_id: SimpleNamespace(
                id=turn_id,
                engram_id="engram-authoritative",
            )
        ),
        _harness_terminal_sessions=Sessions(),
    )
    gateway = _RuntimeHarnessControlGateway(service)

    inspected = gateway.inspect_terminal_session(
        "session-1", expected_turn_id="turn-1"
    )
    output = gateway.read_terminal_session_output(
        "session-1",
        expected_turn_id="turn-1",
        after_seq=3,
        limit=7,
    )

    assert inspected["engram_id"] == "engram-authoritative"
    assert output["terminal_session_id"] == "session-1"
    assert calls == [
        (
            "inspect",
            {
                "id": "session-1",
                "expected_engram_id": "engram-authoritative",
                "expected_turn_id": "turn-1",
            },
        ),
        (
            "output",
            {
                "id": "session-1",
                "expected_engram_id": "engram-authoritative",
                "expected_turn_id": "turn-1",
                "after_seq": 3,
                "limit": 7,
            },
        ),
    ]

    with pytest.raises(_HarnessControlError) as missing_scope:
        gateway.inspect_terminal_session("session-1")
    assert missing_scope.value.code == "invalid_turn_id"


def test_default_runtime_fails_fast_when_pi_is_missing(tmp_path: Path) -> None:
    db = tmp_path / "missing-pi.db"
    config = RuntimeServiceConfig(
        db_path=db,
        workspace=tmp_path,
        pi_executable="definitely-not-a-real-pi-executable",
    )
    with pytest.raises(HarnessError) as caught:
        RuntimeService(config)
    assert caught.value.code == "pi_not_installed"
    assert caught.value.phase == "preflight"

    # Failed construction released the SQLite handle.
    Storage(db).close()


def test_assembly_preserves_missing_pi_error_and_shutdown_evidence(
    tmp_path: Path,
) -> None:
    db = tmp_path / "missing-pi-assembly.db"
    outcome = RuntimeAssembly.open(
        RuntimeServiceConfig(
            db_path=db,
            workspace=tmp_path,
            pi_executable="definitely-not-a-real-pi-executable",
        )
    )

    assert outcome.runtime is None
    assert isinstance(outcome.error, HarnessError)
    original = outcome.error
    with pytest.raises(HarnessError) as reraised:
        outcome.raise_for_error()
    assert reraised.value is original
    report = outcome.shutdown.wait_terminal(timeout=1.0)
    assert report is not None
    assert outcome.shutdown.primary_trigger.value == "startup_failure"
    assert report.protocol_version == "runtime-shutdown.v1"
    assert report.publication_fence.value == "revoked"
    assert report.owner_lease.value == "released"
    assert report.storage_state.value == "closed"


def test_invalid_input_cursor_closes_preflighted_harness_and_storage(
    tmp_path: Path,
) -> None:
    db = tmp_path / "invalid-input-cursor.db"
    seed = Storage(db)
    seed.save_component_state(HARNESS_INPUT_COMPONENT, {
        "version": 999,
        "cursors": {},
    })
    seed.close()
    harnesses: list[_PreflightHarness] = []

    def harness_factory(_workspace, **_kwargs):
        harness = _PreflightHarness()
        harnesses.append(harness)
        return harness

    outcome = RuntimeAssembly.open(
        _pi_config(tmp_path, db),
        harness_factory=harness_factory,
    )

    assert outcome.runtime is None
    assert isinstance(outcome.error, HarnessError)
    original = outcome.error
    with pytest.raises(HarnessError) as caught:
        outcome.raise_for_error()
    assert caught.value is original
    assert caught.value.code == "harness_input_cursor_invalid"
    assert caught.value.phase == "input_cursor"
    assert len(harnesses) == 1
    assert harnesses[0].preflighted is True
    assert harnesses[0].closed is True
    report = outcome.shutdown.wait_terminal(timeout=1.0)
    assert report is not None
    component_names = {item.component for item in report.components}
    assert "harness" in component_names
    assert "tool_gateway" in component_names
    assert "pulse_engine" not in component_names
    assert outcome.shutdown.primary_trigger.value == "startup_failure"
    # Failed construction leaves the SQLite handle independently openable.
    Storage(db).close()


def test_explicit_mock_is_a_visible_harness_choice(tmp_path: Path) -> None:
    service = RuntimeService(RuntimeServiceConfig(
        db_path=tmp_path / "mock.db",
        workspace=tmp_path,
        mock=True,
    ))
    try:
        view = service.snapshot()
        assert view["harness"] == {
            "kind": "mock",
            "live_sessions": 0,
            "bindings": 0,
            "states": {},
        }
        result = service.engrams.pulse(service.front_engram_id)
        assert result.content.startswith("[mock response to:")
        assert service.engrams._harness is service.harness
    finally:
        service.close()


def test_runtime_builds_scoped_task_worker_fleet_from_pi_template(tmp_path: Path) -> None:
    transports = _PersistentPiFactory(tmp_path)
    worker_root = tmp_path.parent / f"{tmp_path.name}-worker-fleet"
    service = RuntimeService(
        _pi_config(
            tmp_path,
            harness_task_worker_enabled=True,
            harness_task_worker_root=worker_root,
            harness_task_worker_capacity=1,
            harness_task_worker_max_per_turn=1,
            harness_task_worker_default_timeout_sec=5,
            harness_task_worker_max_timeout_sec=5,
        ),
        harness_factory=_runtime_factory(transports),
    )
    try:
        assert service._harness_task_worker_bridge is not None
        spawned = service._harness_task_worker_bridge.dispatch(
            service.front_engram_id,
            "pulse_task_spawn",
            {"task": "return one bounded answer", "timeout": 5},
            ToolInvocationContext("runtime-worker-spawn-1"),
            "runtime-worker-turn-1",
        )
        assert spawned["ok"] is True
        task_id = spawned["data"]["task_id"]

        delivered = None
        for index in range(30):
            observed = service._harness_task_worker_bridge.dispatch(
                service.front_engram_id,
                "pulse_task_wait",
                {"task_id": task_id, "timeout": 0.1},
                ToolInvocationContext(f"runtime-worker-wait-{index}"),
                "runtime-worker-turn-1",
            )
            if observed["data"].get("output_delivered") is True:
                delivered = observed
                break
            time.sleep(0.01)

        assert delivered is not None
        assert delivered["content"].startswith("answer-pi")
        assert worker_root.is_dir()
        assert not any(worker_root.iterdir())
    finally:
        service.close()


def test_task_worker_root_must_be_explicit_and_external(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requires harness_task_worker_root"):
        RuntimeServiceConfig(harness_task_worker_enabled=True)

    transports = _PersistentPiFactory(tmp_path)
    with pytest.raises(ValueError, match="external"):
        RuntimeService(
            _pi_config(
                tmp_path,
                harness_task_worker_enabled=True,
                harness_task_worker_root=tmp_path / "inside-workspace",
            ),
            harness_factory=_runtime_factory(transports),
        )


def test_checkpoint_restore_is_e0_fenced_and_same_request_replays(tmp_path: Path) -> None:
    workspace = tmp_path / "restore-workspace"
    workspace.mkdir()
    checkpoints = tmp_path / "restore-checkpoints"
    transports = _PersistentPiFactory(tmp_path)
    service = RuntimeService(
        _pi_config(
            workspace,
            db=tmp_path / "restore-runtime.db",
            workspace=workspace,
            harness_file_mutation_enabled=True,
            harness_checkpoint_root=checkpoints,
        ),
        harness_factory=_runtime_factory(transports),
    )
    lease = service.snapshot()["lease"]
    try:
        backend = service._harness_workspace_backend
        assert backend is not None
        changed = backend.execute(
            action_request_id="seed-checkpoint-1",
            engram_id=service.front_engram_id,
            turn_id="turn-restore-e0",
            epoch=lease["epoch"],
            tool_name="write",
            input_data={"path": "restore.txt", "content": "post-image\n"},
            policy_preview={},
        )
        assert changed["ok"] is True
        checkpoint_id = changed["data"]["checkpoint_id"]
        assert (workspace / "restore.txt").is_file()

        restored = service.harness_control_gateway.restore_checkpoint(
            checkpoint_id,
            "restore-request-1",
            lease["epoch"],
        )
        assert restored["ok"] is True
        assert restored["state"] == "restored"
        assert not (workspace / "restore.txt").exists()
        operation = service._harness_operation_ledger.get(
            "checkpoint.restore",
            "restore-request-1",
        )
        assert operation is not None
        assert operation.terminal_state is OperationTerminalState.COMPLETED
        assert operation.recovery_state is OperationRecoveryState.CLEARED
        before = len(
            service.harness_event_store.replay("turn-restore-e0", limit=50).events
        )

        replay = service.harness_control_gateway.restore_checkpoint(
            checkpoint_id,
            "restore-request-1",
            lease["epoch"],
        )
        assert replay["ok"] is True
        assert replay["idempotent"] is True
        assert len(
            service.harness_event_store.replay("turn-restore-e0", limit=50).events
        ) == before
    finally:
        service.close()


def test_checkpoint_restore_terminal_loss_is_uncertain_and_not_reexecuted(
    tmp_path: Path,
    monkeypatch,
) -> None:
    workspace = tmp_path / "restore-loss-workspace"
    workspace.mkdir()
    checkpoints = tmp_path / "restore-loss-checkpoints"
    transports = _PersistentPiFactory(tmp_path)
    service = RuntimeService(
        _pi_config(
            workspace,
            db=tmp_path / "restore-loss-runtime.db",
            workspace=workspace,
            harness_file_mutation_enabled=True,
            harness_checkpoint_root=checkpoints,
        ),
        harness_factory=_runtime_factory(transports),
    )
    lease = service.snapshot()["lease"]
    backend = service._harness_workspace_backend
    assert backend is not None
    changed = backend.execute(
        action_request_id="seed-checkpoint-loss",
        engram_id=service.front_engram_id,
        turn_id="turn-restore-loss",
        epoch=lease["epoch"],
        tool_name="write",
        input_data={"path": "restore-loss.txt", "content": "post-image\n"},
        policy_preview={},
    )
    checkpoint_id = changed["data"]["checkpoint_id"]
    restore_calls = 0
    original_restore = backend.restore
    original_terminal_append = service.harness_event_store.append_terminal_operation

    def counted_restore(*args, **kwargs):
        nonlocal restore_calls
        restore_calls += 1
        return original_restore(*args, **kwargs)

    def fail_terminal(*_args, **_kwargs):
        raise OSError("injected atomic restore terminal loss")

    monkeypatch.setattr(backend, "restore", counted_restore)
    monkeypatch.setattr(
        service.harness_event_store,
        "append_terminal_operation",
        fail_terminal,
    )
    try:
        result = service.harness_control_gateway.restore_checkpoint(
            checkpoint_id,
            "restore-loss-request-1",
            lease["epoch"],
        )
        assert result["state"] == "uncertain"
        assert result["error"] == "checkpoint_restore_event_persist_failed"
        assert restore_calls == 1
        assert not (workspace / "restore-loss.txt").exists()
        operation = service._harness_operation_ledger.get(
            "checkpoint.restore",
            "restore-loss-request-1",
        )
        assert operation is not None
        assert operation.terminal_state is OperationTerminalState.UNCERTAIN
        assert operation.recovery_state is OperationRecoveryState.REQUIRED

        monkeypatch.setattr(
            service.harness_event_store,
            "append_terminal_operation",
            original_terminal_append,
        )
        with pytest.raises(Exception) as caught:
            service.harness_control_gateway.restore_checkpoint(
                checkpoint_id,
                "restore-loss-request-1",
                lease["epoch"],
            )
        assert getattr(caught.value, "code", None) == "operation_recovery_required"
        assert restore_calls == 1
    finally:
        service.close()


def test_checkpoint_restore_adapter_uncertainty_is_durable_uncertain(
    tmp_path: Path,
    monkeypatch,
) -> None:
    workspace = tmp_path / "restore-uncertain-workspace"
    workspace.mkdir()
    transports = _PersistentPiFactory(tmp_path)
    service = RuntimeService(
        _pi_config(
            workspace,
            db=tmp_path / "restore-uncertain-runtime.db",
            workspace=workspace,
            harness_file_mutation_enabled=True,
            harness_checkpoint_root=tmp_path / "restore-uncertain-checkpoints",
        ),
        harness_factory=_runtime_factory(transports),
    )
    lease = service.snapshot()["lease"]
    try:
        backend = service._harness_workspace_backend
        assert backend is not None
        changed = backend.execute(
            action_request_id="seed-checkpoint-uncertain",
            engram_id=service.front_engram_id,
            turn_id="turn-restore-uncertain",
            epoch=lease["epoch"],
            tool_name="write",
            input_data={"path": "restore-uncertain.txt", "content": "post-image\n"},
            policy_preview={},
        )
        checkpoint_id = changed["data"]["checkpoint_id"]
        monkeypatch.setattr(
            backend,
            "restore",
            lambda *_args, **_kwargs: {
                "ok": False,
                "state": "uncertain",
                "status": "uncertain",
                "error": "restore_cleanup_unconfirmed",
                "applied_paths": ["restore-uncertain.txt"],
                "evidence_class": "LIVE_GATE_UNVERIFIED",
            },
        )

        result = service.harness_control_gateway.restore_checkpoint(
            checkpoint_id,
            "restore-uncertain-request-1",
            lease["epoch"],
        )
        assert result["ok"] is False
        assert result["state"] == "uncertain"
        operation = service._harness_operation_ledger.get(
            "checkpoint.restore",
            "restore-uncertain-request-1",
        )
        assert operation is not None
        assert operation.terminal_state is OperationTerminalState.UNCERTAIN
        assert operation.recovery_state is OperationRecoveryState.CLEARED
        terminal = service.harness_event_store.get(operation.terminal_event_id)
        assert terminal is not None
        assert terminal.status is HarnessEventStatus.UNCERTAIN
    finally:
        service.close()


def test_binding_callback_persists_the_complete_snapshot(tmp_path: Path) -> None:
    transports = _PersistentPiFactory(tmp_path)
    service = RuntimeService(
        _pi_config(tmp_path),
        harness_factory=_runtime_factory(transports),
    )
    try:
        front = service.front_engram_id
        service.engrams.pulse(front)
        stored = service.storage.load_component_state(BINDING_COMPONENT)
        assert stored["version"] == 1
        assert set(stored["sessions"]) == {front}
        assert stored["sessions"][front]["state"] == "materialized"
        assert service.engrams._harness_turn_timeout_sec == 3.0
    finally:
        service.close()


def test_same_database_restores_front_and_switches_same_pi_session(
    tmp_path: Path,
) -> None:
    db = tmp_path / "restart.db"
    transports = _PersistentPiFactory(tmp_path)
    factory = _runtime_factory(transports)

    first = RuntimeService(_pi_config(tmp_path, db), harness_factory=factory)
    front = first.front_engram_id
    first.engrams.pulse(front)
    persisted = first.storage.load_component_state(BINDING_COMPONENT)
    binding = persisted["sessions"][front]
    first.close()
    assert all(transport.closed for transport in transports.transports)

    second = RuntimeService(_pi_config(tmp_path, db), harness_factory=factory)
    try:
        assert second.front_engram_id == front
        assert second.resumed is True
        second.engrams.pulse(front)
        resumed_transport = transports.transports[-1]
        assert resumed_transport.switches == [binding["session_file"]]
        assert resumed_transport.session_id == binding["session_id"]
        assert second.storage.load_component_state(BINDING_COMPONENT) == persisted
    finally:
        second.close()
    assert all(transport.closed for transport in transports.transports)


def test_close_releases_every_live_transport(tmp_path: Path) -> None:
    transports = _PersistentPiFactory(tmp_path)
    service = RuntimeService(
        _pi_config(tmp_path),
        harness_factory=_runtime_factory(transports),
    )
    front = service.front_engram_id
    other = service.engrams.create(initial_messages=[
        Message(role=MessageRole.USER, content="another life line"),
    ])
    service.engrams.pulse(front)
    service.engrams.pulse(other.id)
    assert len(transports.transports) == 2

    service.close()
    service.close()
    assert all(transport.closed for transport in transports.transports)


def test_snapshot_summarizes_pi_without_paths_or_language(tmp_path: Path) -> None:
    transports = _PersistentPiFactory(tmp_path)
    service = RuntimeService(
        _pi_config(tmp_path),
        harness_factory=_runtime_factory(transports),
    )
    try:
        service.engrams.pulse(service.front_engram_id)
        summary = service.snapshot()["harness"]
        assert set(summary) == {
            "kind",
            "live_sessions",
            "bindings",
            "states",
            "mcp_registry",
        }
        assert summary["mcp_registry"] == {
            "attached": False,
            "descriptors": 0,
            "enabled": 0,
            "supported_transports": [],
            "evidence_class": "CONTRACT",
            "live_gate": "LIVE_GATE_UNVERIFIED",
        }
        assert summary["kind"] == "pi"
        assert summary["live_sessions"] == 1
        assert summary["bindings"] == 1
        encoded = json.dumps(summary)
        assert ".jsonl" not in encoded
        for forbidden in ("session_file", "prompt", "content", "output"):
            assert forbidden not in encoded.lower()
    finally:
        service.close()


def test_close_order_is_idempotent(tmp_path: Path, monkeypatch) -> None:
    service = RuntimeService(RuntimeServiceConfig(
        db_path=tmp_path / "order.db",
        workspace=tmp_path,
        mock=True,
    ))
    order: list[str] = []
    original_executor_close = service._executor.shutdown
    original_harness_close = service.harness.close
    original_metrics_flush = service.metrics.flush
    original_storage_close = service.storage.close

    def executor_close(*args, **kwargs):
        order.append("executor")
        return original_executor_close(*args, **kwargs)

    def harness_close():
        order.append("harness")
        return original_harness_close()

    def metrics_flush():
        order.append("metrics")
        return original_metrics_flush()

    def storage_close():
        order.append("storage")
        return original_storage_close()

    monkeypatch.setattr(service._executor, "shutdown", executor_close)
    monkeypatch.setattr(service.harness, "close", harness_close)
    monkeypatch.setattr(service.metrics, "flush", metrics_flush)
    monkeypatch.setattr(service.storage, "close", storage_close)

    service.close()
    service.close()
    assert order.count("executor") == 1
    assert order.count("harness") == 1
    assert order.count("metrics") == 1
    assert order.count("storage") == 1
    assert order.index("executor") < order.index("metrics")
    assert order.index("harness") < order.index("metrics")
    assert order.index("metrics") < order.index("storage")


@pytest.mark.asyncio
async def test_close_refuses_to_race_an_active_tick_loop(tmp_path: Path) -> None:
    service = RuntimeService(RuntimeServiceConfig(
        db_path=tmp_path / "running.db",
        workspace=tmp_path,
        mock=True,
        tick_interval=0.01,
    ))
    await service.start()
    try:
        with pytest.raises(ServiceError) as caught:
            service.close()
        assert caught.value.error == "runtime_running"
        assert caught.value.status == 409
        assert service.running is True
        assert service._closed is False
    finally:
        await service.stop()
        service.close()


def test_cli_defaults_to_pi_and_accepts_explicit_harness_flags() -> None:
    defaults = _parse([])
    assert defaults.mock is False
    assert defaults.no_mock is False
    assert defaults.pi_executable == "pi"
    assert defaults.pi_provider is None
    assert defaults.pi_model is None
    assert defaults.turn_timeout == 600.0
    assert defaults.enable_codex_read_only_sandbox is False
    assert defaults.codex_sandbox_executable is None
    assert defaults.codex_sandbox_live_gate is None
    assert defaults.codex_sandbox_config is None
    assert defaults.harness_command == []

    explicit = _parse([
        "--mock",
        "--pi-executable", "pi-custom",
        "--pi-provider", "openai",
        "--pi-model", "gpt-test",
        "--turn-timeout", "12.5",
        "--enable-codex-read-only-sandbox",
        "--codex-sandbox-executable", "codex-custom.exe",
        "--codex-sandbox-live-gate", "C:/gates/read-only.json",
        "--codex-sandbox-config", "C:/Users/test/.codex/config.toml",
        "--harness-command", "git",
        "--harness-command", "rg",
    ])
    assert explicit.mock is True
    assert explicit.pi_executable == "pi-custom"
    assert explicit.pi_provider == "openai"
    assert explicit.pi_model == "gpt-test"
    assert explicit.turn_timeout == 12.5
    assert explicit.enable_codex_read_only_sandbox is True
    assert explicit.codex_sandbox_executable == "codex-custom.exe"
    assert explicit.codex_sandbox_live_gate == "C:/gates/read-only.json"
    assert explicit.codex_sandbox_config == "C:/Users/test/.codex/config.toml"
    assert explicit.harness_command == ["git", "rg"]

    deprecated = _parse(["--no-mock"])
    assert deprecated.no_mock is True
    assert deprecated.mock is False


def test_cli_help_names_pi_as_default_and_mock_as_explicit(capsys) -> None:
    with pytest.raises(SystemExit) as caught:
        _parse(["--help"])
    assert caught.value.code == 0
    help_text = " ".join(capsys.readouterr().out.split())
    assert "production default is Pi" in help_text
    assert "deprecated no-op" in help_text

"""Tests for the agent-backend abstraction (agent/backends/).

Two properties matter more than everything else here, and they are tested
first and hardest:

1. **With pi absent** — the state of every machine that has not installed a
   Node package, which is most of them — `PiBackend` refuses by name and
   says how to fix it. That is the path the largest number of users hit, so
   it is the one that most needs to be right.
2. **It never answers with something else instead.** No test in this file
   is allowed to see a pi request satisfied by the local backend.

Everything here runs with pi absent and with no API key: `LocalBackend` is
driven by `LLMAdapter(mock=True)`, and every pi path is driven either by an
injected transport or by a fake `pi` on PATH that this file writes itself.
The fake speaks the real protocol from pi's `rpc-types.ts`; it is a stand-in
for the process, not for the wire format.
"""

from __future__ import annotations

import inspect
import json
import os
import sys
import time
from collections import deque

import pytest

from pulse_system.agent.backends import (
    AgentBackend,
    BackendError,
    BackendResult,
    BackendUnavailable,
    LocalBackend,
    PiBackend,
    TaskSpec,
)
from pulse_system.agent.backends import pi as pi_module
from pulse_system.agent.backends.pi import (
    PI_NPM_PACKAGE,
    RpcConnectionLost,
    RpcTimeout,
    SubprocessRpcTransport,
)
from pulse_system.agent.tools import ToolRegistry
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.engram import EngramManager
from pulse_system.core.types import Message, MessageRole
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


# ── Fixtures ─────────────────────────────────────────────────────


@pytest.fixture
def stack(tmp_path):
    """A live engram substrate with no network and no key."""
    store = Storage(":memory:")
    llm = LLMAdapter(mock=True)
    mgr = EngramManager(store, llm, ConnectionNetwork(store, ConnectionConfig()))
    tools = ToolRegistry(mock=True, workspace_root=tmp_path)
    yield mgr, tools
    store.close()


@pytest.fixture
def local(stack):
    mgr, tools = stack
    return LocalBackend(mgr, tools, max_think_iterations=2)


@pytest.fixture
def no_pi(monkeypatch, tmp_path):
    """A PATH with no pi on it — the state of an ordinary machine."""
    empty = tmp_path / "empty-path"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    monkeypatch.delenv("PATHEXT", raising=False)
    return PiBackend()


# ── A fake pi, speaking pi's own RPC protocol ────────────────────

_INHERIT = object()


class ScriptedPi:
    """An injected transport that behaves like `pi --mode rpc`.

    Shapes are taken from `packages/coding-agent/src/modes/rpc/rpc-types.ts`
    and `packages/agent/src/types.ts`: commands carry `type` and an optional
    `id`; replies are `{"type": "response", "command", "success", ...}`;
    everything else on the wire is an event; the turn ends at
    `agent_settled`.
    """

    def __init__(
        self,
        *,
        reply_text: str = "the task is done",
        stop_reason: str = "stop",
        last_assistant_text: object = _INHERIT,
        refuse: dict[str, str] | None = None,
        die_after: str | None = None,
        hang_after: str | None = None,
        returncode: int | None = 0,
        stderr_tail: str = "",
        event_count: int = 1,
    ):
        self.sent: list[dict] = []
        self.read_timeouts: list[float | None] = []
        self.closed = False
        self._out: deque[str] = deque()
        self._reply_text = reply_text
        self._stop_reason = stop_reason
        self._last_text = (
            reply_text if last_assistant_text is _INHERIT else last_assistant_text
        )
        self._refuse = refuse or {}
        self._die_after = die_after
        self._hang_after = hang_after
        self._returncode = returncode
        self._stderr_tail = stderr_tail
        self._event_count = event_count
        self._dead = False
        self._hanging = False

    # -- transport contract --

    def send_line(self, text: str) -> None:
        assert text.endswith("\n"), "JSONL records must be LF-terminated"
        command = json.loads(text)
        self.sent.append(command)
        kind = command.get("type")
        if self._dead:
            return  # a dead process answers nothing
        if kind in self._refuse:
            self._emit_response(command, success=False, error=self._refuse[kind])
        elif kind == "get_state":
            self._emit_response(command, data=self._state())
        elif kind == "prompt":
            self._handle_prompt(command)
        elif kind == "get_last_assistant_text":
            self._emit_response(command, data={"text": self._last_text})
        elif kind == "get_entries":
            self._emit_response(command, data={"entries": [], "leafId": "leaf-1"})
        else:
            self._emit_response(
                command, success=False, error=f"Unknown command: {kind}"
            )
        if kind == self._die_after:
            self._dead = True
        if kind == self._hang_after:
            self._hanging = True

    def read_line(self, timeout: float | None) -> str | None:
        self.read_timeouts.append(timeout)
        if self._out:
            return self._out.popleft()
        if self._dead:
            return None
        if self._hanging:
            # Honour the budget we were given, so the caller's deadline
            # arithmetic is actually exercised rather than short-circuited.
            time.sleep(min(timeout, 0.05) if timeout is not None else 0.05)
        raise RpcTimeout

    def close(self) -> None:
        self.closed = True

    def diagnostics(self) -> dict:
        return {"returncode": self._returncode, "stderr_tail": self._stderr_tail}

    # -- pi's side --

    def _state(self) -> dict:
        return {
            "sessionId": "fake-session-1",
            "sessionFile": "/tmp/fake.jsonl",
            "thinkingLevel": "off",
            "isStreaming": False,
            "isCompacting": False,
            "steeringMode": "all",
            "followUpMode": "all",
            "autoCompactionEnabled": True,
            "messageCount": 0,
            "pendingMessageCount": 0,
        }

    def _handle_prompt(self, command: dict) -> None:
        self._emit_response(command)
        if self._die_after == "prompt":
            self._push({"type": "agent_start"})
            return
        if self._hang_after == "prompt":
            return
        self._push({"type": "agent_start"})
        message = {
            "role": "assistant",
            "stopReason": self._stop_reason,
            "content": [{"type": "text", "text": self._reply_text}],
        }
        if self._stop_reason in ("error", "aborted"):
            message["errorMessage"] = "provider rejected the request"
            message["content"] = []
        for i in range(self._event_count):
            self._push({"type": "message_end", "message": message, "seq": i})
        self._push({"type": "agent_end", "messages": [message]})
        self._push({"type": "agent_settled"})

    def _emit_response(self, command, *, success=True, data=None, error=None):
        payload = {
            "id": command.get("id"),
            "type": "response",
            "command": command.get("type"),
            "success": success,
        }
        if error is not None:
            payload["error"] = error
        elif data is not None:
            payload["data"] = data
        self._push(payload)

    def _push(self, obj: dict) -> None:
        self._out.append(json.dumps(obj))


def scripted(**kwargs):
    """A PiBackend wired to a ScriptedPi, plus the fake for inspection."""
    fake = ScriptedPi(**kwargs)
    backend = PiBackend(transport_factory=lambda argv: fake)
    return backend, fake


# ── 1. pi is absent — the path most users hit ────────────────────


class TestPiAbsent:
    def test_preflight_refuses_by_name(self, no_pi):
        with pytest.raises(BackendUnavailable) as caught:
            no_pi.preflight()
        assert caught.value.code == "pi_not_installed"

    def test_submit_refuses_rather_than_returning(self, no_pi):
        """The absence must not arrive shaped like a result."""
        with pytest.raises(BackendUnavailable):
            no_pi.submit(TaskSpec("anything at all"))

    def test_the_message_names_what_is_missing_and_how_to_get_it(self, no_pi):
        with pytest.raises(BackendUnavailable) as caught:
            no_pi.preflight()
        text = str(caught.value)
        assert "'pi'" in text                       # what is missing
        assert PI_NPM_PACKAGE in text               # how to get it
        assert "npm install -g" in text
        assert "PATH" in text                       # where it looked

    def test_error_carries_the_contract_error_shape(self, no_pi):
        with pytest.raises(BackendUnavailable) as caught:
            no_pi.preflight()
        payload = caught.value.to_dict()
        assert set(payload) == {"error", "detail", "remedy"}
        assert all(isinstance(v, str) and v for v in payload.values())
        assert json.loads(json.dumps(payload))      # serialisable as-is

    def test_it_is_not_a_bare_traceback(self, no_pi):
        """A FileNotFoundError from Popen would be the failure mode here."""
        with pytest.raises(BackendUnavailable) as caught:
            no_pi.submit(TaskSpec("anything"))
        assert not isinstance(caught.value, (OSError, FileNotFoundError))
        assert isinstance(caught.value, BackendError)

    def test_a_custom_executable_name_is_reported_verbatim(self, no_pi):
        backend = PiBackend(executable="pi-nightly")
        with pytest.raises(BackendUnavailable) as caught:
            backend.preflight()
        assert "'pi-nightly'" in str(caught.value)


class TestNoSilentFallback:
    """The rule that gives this package its reason to exist."""

    def test_pi_module_has_no_access_to_the_local_backend(self):
        assert "LocalBackend" not in vars(pi_module)

    def test_pi_source_never_mentions_the_local_backend(self):
        source = inspect.getsource(pi_module)
        # Skip the module docstring, which discusses the rule in prose.
        body = source.split('"""', 2)[-1]
        assert "LocalBackend" not in body
        assert "backends.local" not in body
        assert "from .local" not in body

    def test_absence_never_yields_a_result_object(self, no_pi):
        outcome = None
        try:
            outcome = no_pi.submit(TaskSpec("do the thing"))
        except BackendUnavailable:
            pass
        assert outcome is None, "pi's absence produced a result to read"

    def test_unavailable_is_raised_not_returned_by_design(self):
        assert issubclass(BackendUnavailable, BackendError)
        assert issubclass(BackendError, Exception)


# ── 2. pi present and working ────────────────────────────────────


class TestPiWorking:
    def test_context_aware_transport_receives_process_workspace_and_env(
        self,
        tmp_path,
    ):
        observed = {}
        fake = ScriptedPi()

        class Factory:
            def open_transport(self, argv, *, cwd, env):
                observed.update(argv=argv, cwd=cwd, env=env)
                return fake

        session_root = tmp_path / "sessions"
        backend = PiBackend(
            workdir=tmp_path,
            env={"PI_CODING_AGENT_SESSION_DIR": str(session_root)},
            transport_factory=Factory(),
        )

        result = backend.submit(TaskSpec("task"))

        assert result.ok is True
        assert observed["argv"] == ["pi", "--mode", "rpc"]
        assert observed["cwd"] == str(tmp_path)
        assert observed["env"] == {
            "PI_CODING_AGENT_SESSION_DIR": str(session_root)
        }

    def test_a_completed_turn_returns_its_text(self):
        backend, fake = scripted(reply_text="learned delegation routing selected a target")
        result = backend.submit(TaskSpec("explain the router"))
        assert result.ok is True
        assert result.backend == "pi"
        assert result.output == "learned delegation routing selected a target"
        assert result.error is None

    def test_the_task_crosses_the_wire_verbatim(self):
        backend, fake = scripted()
        backend.submit(TaskSpec("@front do a thing --with dashes"))
        prompts = [c for c in fake.sent if c["type"] == "prompt"]
        assert len(prompts) == 1
        assert prompts[0]["message"] == "@front do a thing --with dashes"

    def test_it_handshakes_before_prompting(self):
        """A reply proves pi is up; pi's own client sleeps 100 ms instead."""
        backend, fake = scripted()
        backend.submit(TaskSpec("task"))
        assert [c["type"] for c in fake.sent][:2] == ["get_state", "prompt"]

    def test_every_command_is_correlated_by_id(self):
        backend, fake = scripted()
        backend.submit(TaskSpec("task"))
        ids = [c["id"] for c in fake.sent]
        assert all(isinstance(i, str) and i for i in ids)
        assert len(set(ids)) == len(ids)

    def test_the_trace_carries_pi_events_and_the_session(self):
        backend, fake = scripted()
        result = backend.submit(TaskSpec("task"))
        kinds = [entry["kind"] for entry in result.trace]
        assert "pi.session" in kinds
        assert "pi.event" in kinds
        session = next(e for e in result.trace if e["kind"] == "pi.session")
        assert session["session_id"] == "fake-session-1"
        types = [e.get("type") for e in result.trace if e["kind"] == "pi.event"]
        assert "agent_settled" in types

    def test_the_transport_is_closed_afterwards(self):
        backend, fake = scripted()
        backend.submit(TaskSpec("task"))
        assert fake.closed is True

    def test_an_ignored_target_is_recorded_not_dropped(self):
        backend, fake = scripted()
        result = backend.submit(TaskSpec("task", target="engram-7"))
        note = next(e for e in result.trace if e["kind"] == "spec.target_ignored")
        assert note["target"] == "engram-7"

    def test_the_trace_is_capped_and_says_so(self):
        backend = PiBackend(
            transport_factory=lambda argv: ScriptedPi(event_count=40),
            max_trace_events=10,
        )
        result = backend.submit(TaskSpec("task"))
        marker = next(
            e for e in result.trace if e["kind"] == "pi.trace_truncated"
        )
        assert marker["dropped_events"] > 0
        assert marker["kept_events"] == 10

    def test_the_argv_is_the_invocation_confirmed_from_pi_source(self, no_pi):
        backend = PiBackend(
            provider="anthropic",
            model="sonnet",
            transport_factory=lambda argv: ScriptedPi(),
        )
        assert backend.argv() == [
            "pi", "--mode", "rpc", "--provider", "anthropic", "--model", "sonnet",
        ]

    def test_the_session_leaf_probe_is_opt_in(self):
        backend, fake = scripted()
        backend.submit(TaskSpec("task"))
        assert "get_entries" not in [c["type"] for c in fake.sent]

        opted_in = PiBackend(
            transport_factory=lambda argv: fake, include_session_leaf=True
        )
        result = opted_in.submit(TaskSpec("task"))
        leaf = next(e for e in result.trace if e["kind"] == "pi.session_leaf")
        assert leaf["leaf_id"] == "leaf-1"


# ── 3. pi present and failing ────────────────────────────────────


class TestPiFailures:
    def test_a_timeout_reports_as_a_timeout(self):
        backend, fake = scripted(hang_after="prompt")
        result = backend.submit(TaskSpec("task", timeout_sec=0.2))
        assert result.ok is False
        assert result.error.code == "pi_timeout"
        assert "0.2" in result.error.detail
        assert result.error.remedy

    def test_the_deadline_is_actually_passed_to_the_transport(self):
        backend, fake = scripted(hang_after="prompt")
        backend.submit(TaskSpec("task", timeout_sec=0.2))
        budgets = [t for t in fake.read_timeouts if t is not None]
        assert budgets, "the transport was given no budget at all"
        assert max(budgets) <= 0.2 + 1e-9

    def test_no_timeout_means_an_unbounded_wait_for_the_turn(self):
        """`timeout_sec=None` is a real choice: wait as long as it takes.

        The handshake keeps its own separate bound, because a pi that never
        answers `get_state` is not working slowly, it is not there.
        """
        backend, fake = scripted()
        backend.submit(TaskSpec("task", timeout_sec=None))
        assert None in fake.read_timeouts
        bounded = {t for t in fake.read_timeouts if t is not None}
        assert bounded in ({30.0}, set())

    def test_a_killed_process_reports_as_killed_not_as_empty(self):
        backend, fake = scripted(die_after="prompt", returncode=-9)
        result = backend.submit(TaskSpec("task"))
        assert result.ok is False
        assert result.error.code == "pi_killed"
        assert "signal 9" in result.error.detail

    def test_pi_own_sigterm_exit_code_reports_as_killed(self):
        """rpc-mode.ts maps SIGTERM to 143 and SIGHUP to 129."""
        backend, fake = scripted(die_after="prompt", returncode=143)
        result = backend.submit(TaskSpec("task"))
        assert result.error.code == "pi_killed"
        assert "143" in result.error.detail

    def test_a_dead_connection_reports_as_lost_not_as_empty(self):
        backend, fake = scripted(
            die_after="prompt", returncode=1, stderr_tail="Error: no API key"
        )
        result = backend.submit(TaskSpec("task"))
        assert result.ok is False
        assert result.error.code == "pi_connection_lost"
        assert "no API key" in result.error.detail

    def test_a_connection_that_never_comes_up_is_not_a_result(self):
        backend, fake = scripted(die_after="get_state", returncode=1)
        result = backend.submit(TaskSpec("task"))
        assert result.ok is False
        assert result.error.code in ("pi_connection_lost", "pi_killed")
        assert result.output == ""

    def test_a_truncated_turn_is_not_a_finished_one(self):
        """agent-loop.ts:383 refuses to let stopReason "length" read as done."""
        backend, fake = scripted(stop_reason="length", reply_text="half an ans")
        result = backend.submit(TaskSpec("task"))
        assert result.ok is False
        assert result.error.code == "pi_truncated"
        # The partial text is kept — it is just not called a completed answer.
        assert result.output == "half an ans"

    def test_an_errored_turn_reports_pi_own_message(self):
        backend, fake = scripted(stop_reason="error", last_assistant_text=None)
        result = backend.submit(TaskSpec("task"))
        assert result.ok is False
        assert result.error.code == "pi_agent_error"
        assert "provider rejected the request" in result.error.detail

    def test_an_empty_answer_is_a_failure_not_a_success(self):
        backend, fake = scripted(reply_text="", last_assistant_text="")
        result = backend.submit(TaskSpec("task"))
        assert result.ok is False
        assert result.error.code == "pi_empty_output"

    def test_a_refused_prompt_is_reported_as_a_refusal(self):
        backend, fake = scripted(refuse={"prompt": "Unknown command: prompt"})
        result = backend.submit(TaskSpec("task"))
        assert result.ok is False
        assert result.error.code == "pi_prompt_refused"
        assert "Unknown command" in result.error.detail

    def test_a_refused_handshake_never_reaches_the_prompt(self):
        backend, fake = scripted(refuse={"get_state": "boom"})
        result = backend.submit(TaskSpec("task"))
        assert result.error.code == "pi_handshake_refused"
        assert "prompt" not in [c["type"] for c in fake.sent]

    def test_every_failure_carries_a_remedy(self):
        cases = [
            scripted(hang_after="prompt"),
            scripted(die_after="prompt", returncode=-9),
            scripted(stop_reason="length"),
            scripted(stop_reason="error"),
            scripted(reply_text="", last_assistant_text=""),
            scripted(refuse={"prompt": "nope"}),
        ]
        for backend, _ in cases:
            result = backend.submit(TaskSpec("task", timeout_sec=0.2))
            assert result.ok is False
            assert result.error.remedy.strip(), result.error.code
            assert result.error.to_dict()["remedy"]


# ── 4. The real subprocess path, with a fake pi on PATH ──────────

#: A fake pi. It speaks the wire protocol from rpc-types.ts for real, over
#: real pipes, so `SubprocessRpcTransport` is exercised end to end — spawn,
#: LF-only framing, correlation, shutdown — without pi being installed.
_FAKE_PI = r'''
import json, sys

def out(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

last = None
for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    cmd = json.loads(raw)
    kind, cid = cmd.get("type"), cmd.get("id")
    if kind == "get_state":
        out({"id": cid, "type": "response", "command": kind, "success": True,
             "data": {"sessionId": "subproc-session", "thinkingLevel": "off",
                      "isStreaming": False, "isCompacting": False,
                      "steeringMode": "all", "followUpMode": "all",
                      "autoCompactionEnabled": True, "messageCount": 0,
                      "pendingMessageCount": 0}})
    elif kind == "prompt":
        last = "handled: " + cmd["message"]
        msg = {"role": "assistant", "stopReason": "stop",
               "content": [{"type": "text", "text": last}]}
        out({"id": cid, "type": "response", "command": kind, "success": True})
        out({"type": "agent_start"})
        out({"type": "message_end", "message": msg})
        out({"type": "agent_end", "messages": [msg]})
        out({"type": "agent_settled"})
    elif kind == "get_last_assistant_text":
        out({"id": cid, "type": "response", "command": kind, "success": True,
             "data": {"text": last}})
    else:
        out({"id": cid, "type": "response", "command": kind, "success": False,
             "error": "Unknown command: %s" % kind})
'''


def _install_fake_pi(tmp_path, body: str = _FAKE_PI):
    """Put an executable named `pi` on PATH and return its directory."""
    bindir = tmp_path / "fakebin"
    bindir.mkdir(exist_ok=True)
    script = bindir / "fake_pi_impl.py"
    script.write_text(body, encoding="utf-8")
    if os.name == "nt":
        launcher = bindir / "pi.bat"
        launcher.write_text(
            f'@echo off\r\n"{sys.executable}" "{script}" %*\r\n', encoding="utf-8"
        )
    else:
        launcher = bindir / "pi"
        launcher.write_text(
            f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n', encoding="utf-8"
        )
        launcher.chmod(0o755)
    return bindir


@pytest.fixture
def fake_pi_on_path(monkeypatch, tmp_path):
    bindir = _install_fake_pi(tmp_path)
    monkeypatch.setenv("PATH", str(bindir) + os.pathsep + os.environ.get("PATH", ""))
    return bindir


class TestRealSubprocess:
    def test_it_finds_and_drives_a_pi_on_path(self, fake_pi_on_path):
        backend = PiBackend()
        backend.preflight()                       # does not raise
        result = backend.submit(TaskSpec("count the engrams", timeout_sec=60))
        assert result.ok is True, result.error and result.error.to_dict()
        assert result.output == "handled: count the engrams"

    def test_the_resolved_path_is_the_one_on_path(self, fake_pi_on_path):
        resolved = PiBackend().resolve_executable()
        assert str(fake_pi_on_path) in resolved

    def test_the_session_id_comes_back_from_the_real_handshake(
        self, fake_pi_on_path
    ):
        result = PiBackend().submit(TaskSpec("task", timeout_sec=60))
        session = next(e for e in result.trace if e["kind"] == "pi.session")
        assert session["session_id"] == "subproc-session"

    def test_framing_is_lf_only_on_the_wire(self, fake_pi_on_path):
        """pi's jsonl.ts is LF-only by design; we must not emit CRLF."""
        sent: list[bytes] = []

        class Recording(SubprocessRpcTransport):
            def send_line(self, text):
                sent.append(text.encode("utf-8"))
                super().send_line(text)

        backend = PiBackend(
            transport_factory=lambda argv: Recording(
                [PiBackend().resolve_executable(), *argv[1:]]
            )
        )
        backend.submit(TaskSpec("task", timeout_sec=60))
        assert sent
        assert all(raw.endswith(b"\n") and b"\r\n" not in raw for raw in sent)

    def test_the_child_does_not_survive_the_call(self, fake_pi_on_path):
        transports = []

        def factory(argv):
            t = SubprocessRpcTransport(argv)
            transports.append(t)
            return t

        real = PiBackend().resolve_executable()
        backend = PiBackend(
            transport_factory=lambda argv: factory([real, *argv[1:]])
        )
        backend.submit(TaskSpec("task", timeout_sec=60))
        assert transports
        assert transports[0].diagnostics()["returncode"] is not None

    def test_a_pi_that_dies_at_once_is_reported_not_hidden(
        self, monkeypatch, tmp_path
    ):
        bindir = _install_fake_pi(
            tmp_path, "import sys\nsys.stderr.write('fatal: no config\\n')\n"
        )
        monkeypatch.setenv("PATH", str(bindir) + os.pathsep + os.environ.get("PATH", ""))
        result = PiBackend().submit(TaskSpec("task", timeout_sec=30))
        assert result.ok is False
        assert result.error.code in ("pi_connection_lost", "pi_killed")
        assert "fatal: no config" in result.error.detail
        assert result.output == ""


# ── 5. The local backend — the offline default ───────────────────


class TestLocalBackend:
    def test_it_runs_with_no_key_and_no_network(self, local):
        result = local.submit(TaskSpec("summarise the delegation tunnel"))
        assert result.ok is True
        assert result.backend == "local"
        assert result.output.strip()
        assert result.error is None

    def test_preflight_never_refuses(self, local):
        assert local.preflight() is None

    def test_a_fresh_engram_gets_the_task_as_its_first_message(self, local, stack):
        mgr, _ = stack
        result = local.submit(TaskSpec("investigate the claustrum"))
        first = result.trace[0]
        assert first["kind"] == "engram.message"
        assert first["index"] == 0
        assert "investigate the claustrum" in first["content"]

    def test_an_existing_target_gains_the_work(self, local, stack):
        mgr, _ = stack
        engram = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="prior experience"),
        ])
        before = len(mgr.get_session(engram.id))

        result = local.submit(TaskSpec("follow up on that", target=engram.id))

        after = mgr.get_session(engram.id)
        assert len(after) > before
        assert result.trace[0]["index"] == before      # trace starts at the fork
        assert "prior experience" not in [e["content"] for e in result.trace]
        injections = [m for m in after if m.role == MessageRole.INJECTION]
        assert any("follow up on that" in m.content for m in injections)
        assert injections[0].source_engram_id == "backend:local"

    def test_a_missing_target_is_a_precondition_failure(self, local):
        with pytest.raises(BackendError) as caught:
            local.submit(TaskSpec("task", target="no-such-engram"))
        assert caught.value.code == "engram_not_found"
        assert caught.value.remedy
        assert not isinstance(caught.value, BackendUnavailable)

    def test_an_empty_think_is_a_failure_not_a_success(
        self, local, monkeypatch
    ):
        class Mute:
            def __init__(self, *a, **k):
                pass

            def think(self):
                return "   "

        monkeypatch.setattr(
            "pulse_system.agent.backends.local.FrontAgent", Mute
        )
        result = local.submit(TaskSpec("task"))
        assert result.ok is False
        assert result.error.code == "local_no_output"
        assert result.error.remedy

    def test_a_refusing_substrate_reports_with_a_remedy(self, local, monkeypatch):
        from pulse_system.substrate.llm.adapter import LLMCallError

        class Refusing:
            def __init__(self, *a, **k):
                pass

            def think(self):
                raise LLMCallError("DEEPSEEK_API_KEY is not set")

        monkeypatch.setattr(
            "pulse_system.agent.backends.local.FrontAgent", Refusing
        )
        result = local.submit(TaskSpec("task"))
        assert result.ok is False
        assert result.error.code == "local_llm_error"
        assert "DEEPSEEK_API_KEY" in result.error.detail
        assert "mock=True" in result.error.remedy

    def test_the_timeout_becomes_the_front_agent_deadline(self, local, monkeypatch):
        seen = {}

        real = pytest.importorskip(
            "pulse_system.agent.front.agent"
        ).FrontAgent

        class Recording(real):
            def __init__(self, engram_id, mgr, tools, config=None):
                seen["deadline"] = config.deadline_sec
                super().__init__(engram_id, mgr, tools, config)

        monkeypatch.setattr(
            "pulse_system.agent.backends.local.FrontAgent", Recording
        )
        local.submit(TaskSpec("task", timeout_sec=12.5))
        assert seen["deadline"] == 12.5


# ── 6. The shared contract ───────────────────────────────────────


class TestContract:
    def test_both_backends_satisfy_the_protocol(self, local, no_pi):
        assert isinstance(local, AgentBackend)
        assert isinstance(no_pi, AgentBackend)

    def test_the_two_backends_are_named_apart(self, local, no_pi):
        assert local.name == "local"
        assert no_pi.name == "pi"

    def test_a_result_is_json_serialisable_for_the_delegate_endpoint(self, local):
        result = local.submit(TaskSpec("task"))
        payload = json.loads(json.dumps(result.to_dict()))
        assert payload["backend"] == "local"
        assert payload["ok"] is True
        assert payload["error"] is None

    def test_a_failed_result_serialises_its_error(self):
        backend, _ = scripted(stop_reason="length")
        payload = json.loads(json.dumps(backend.submit(TaskSpec("t")).to_dict()))
        assert payload["ok"] is False
        assert payload["error"]["error"] == "pi_truncated"
        assert payload["error"]["remedy"]

    def test_the_task_spec_stays_narrow(self):
        fields = set(TaskSpec.__dataclass_fields__)
        assert fields == {"task", "target", "timeout_sec"}

    def test_the_protocol_stays_narrow(self):
        methods = {n for n in dir(AgentBackend) if not n.startswith("_")}
        attributes = set(AgentBackend.__annotations__)
        assert methods == {"preflight", "submit"}
        assert attributes == {"name"}

    def test_a_result_never_claims_success_with_no_output(self):
        """The one invariant the whole package exists to hold."""
        outcomes = [
            scripted()[0].submit(TaskSpec("t")),
            scripted(reply_text="", last_assistant_text="")[0].submit(TaskSpec("t")),
            scripted(stop_reason="length", reply_text="")[0].submit(TaskSpec("t")),
            scripted(die_after="prompt", returncode=-9)[0].submit(TaskSpec("t")),
            scripted(hang_after="prompt")[0].submit(TaskSpec("t", timeout_sec=0.1)),
        ]
        for result in outcomes:
            assert isinstance(result, BackendResult)
            if result.ok:
                assert result.output.strip()
            else:
                assert result.error is not None
                assert result.error.code and result.error.remedy


def test_the_suite_itself_never_needed_pi(no_pi):
    """If pi were installed here, the absence tests would be vacuous."""
    import shutil as _shutil

    assert _shutil.which("pi") is None

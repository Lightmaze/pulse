"""Contract tests for the persistent Pi Harness runtime.

The fake speaks Pi's JSONL RPC shapes.  It deliberately models the upstream
detail that ``new_session`` has an in-memory path/id before the first assistant
message creates the JSONL file.
"""

from __future__ import annotations

import json
import os
import queue
import random
import sys
import threading
import time
from pathlib import Path

import pytest

from pulse_system.agent.backends import PiBackend
from pulse_system.agent.backends.pi import RpcConnectionLost, SubprocessRpcTransport
from pulse_system.agent.harness import (
    BindingState,
    HarnessError,
    PiHarnessRuntime,
    PiProcessContext,
    SessionBinding,
    merge_pi_settings,
)
from pulse_system.agent.harness.base import binding_snapshot, load_binding_state
from pulse_system.agent.harness.pi import _PiContinuityGuard
from pulse_system.agent.harness.rpc import PiRpcChannel
from pulse_system.core.runtime.publication import RuntimePublicationGate


def _transport_close_observation(
    tree: str,
    *,
    process_observed: int,
    process_unresolved: int,
    owner_joined: bool,
    error_code: str | None,
) -> dict:
    return {
        "signal_sent": True,
        "process_owners_observed": process_observed,
        "process_owners_unresolved": process_unresolved,
        "reader_owners_observed": 0,
        "reader_owners_unresolved": 0,
        "internal_owner_unresolved": 0,
        "owner_joined": owner_joined,
        "process_tree_state": tree,
        "returncode": 143,
        "error_code": error_code,
    }


class FakePersistentPi:
    """A blocking, concurrent-safe Pi RPC transport with persistent sessions."""

    _EOF = object()

    def __init__(
        self,
        root: Path,
        label: str,
        *,
        resume_ids: dict[str, str] | None = None,
        hang_prompt: bool = False,
        die_before_prompt_ack: bool = False,
        die_after_prompt_ack: bool = False,
        materialize: bool = True,
        hang_materialize_state: bool = False,
        hang_startup_state: bool = False,
        abort_settles: bool = True,
        delay_prompt_ack: bool = False,
        close_raises: bool = False,
        stop_reason: str = "stop",
        default_resume_id: str | None = None,
        close_order: list[str] | None = None,
        close_process_observed: int = 0,
        close_process_unresolved: int = 0,
        close_reader_unresolved: int = 0,
        close_wait_entered: threading.Event | None = None,
        close_wait_release: threading.Event | None = None,
        close_summary_sequence: tuple[dict, ...] = (),
    ) -> None:
        self.root = root
        self.label = label
        self.sent: list[dict] = []
        self.prompts: list[str] = []
        self.parent_sessions: list[str | None] = []
        self.parent_payloads: list[bytes] = []
        self.closed = False
        self.prompt_seen = threading.Event()
        self.startup_state_seen = threading.Event()
        self._out: queue.Queue[str | object] = queue.Queue()
        self._resume_ids = {
            os.path.abspath(path): session_id
            for path, session_id in (resume_ids or {}).items()
        }
        self._hang_prompt = hang_prompt
        self._die_before_prompt_ack = die_before_prompt_ack
        self._die_after_prompt_ack = die_after_prompt_ack
        self._materialize = materialize
        self._hang_materialize_state = hang_materialize_state
        self._hang_startup_state = hang_startup_state
        self._abort_settles = abort_settles
        self._delay_prompt_ack = delay_prompt_ack
        self._pending_prompt: dict | None = None
        self._close_raises = close_raises
        self._default_resume_id = default_resume_id
        self._close_order = close_order
        self._close_process_observed = close_process_observed
        self._close_process_unresolved = close_process_unresolved
        self._close_reader_unresolved = close_reader_unresolved
        self._close_wait_entered = close_wait_entered
        self._close_wait_release = close_wait_release
        self._close_summary_sequence = tuple(
            dict(summary) for summary in close_summary_sequence
        )
        self.close_signal_calls = 0
        self.close_wait_calls = 0
        self._stop_reason = stop_reason
        self._session_seq = 0
        self._prompt_seq = 0
        self._auto_compaction = True
        self._last_text = ""
        self.session_id = f"{label}-fresh"
        self.session_file = os.path.abspath(root / f"{self.session_id}.jsonl")
        self.switched_payloads: list[bytes] = []

    def send_line(self, text: str) -> None:
        if self.closed:
            raise RpcConnectionLost("fake Pi is closed")
        assert text.endswith("\n")
        command = json.loads(text)
        self.sent.append(command)
        kind = command["type"]

        if kind == "get_state":
            if self._hang_startup_state and self._prompt_seq == 0:
                self.startup_state_seen.set()
                return
            if self._hang_materialize_state and self._prompt_seq:
                return
            self._respond(command, data={
                "sessionId": self.session_id,
                "sessionFile": self.session_file,
                "autoCompactionEnabled": self._auto_compaction,
                "isStreaming": self._hang_prompt,
                "pendingMessageCount": 0,
            })
        elif kind == "set_auto_compaction":
            self._auto_compaction = bool(command["enabled"])
            self._respond(command)
        elif kind == "switch_session":
            target = os.path.abspath(command["sessionPath"])
            self.switched_payloads.append(Path(target).read_bytes())
            self.session_file = target
            # This mirrors upstream's dangerous missing-file behavior.  The
            # Harness must precheck before this command and verify both values.
            self.session_id = self._resume_ids.get(
                target,
                self._default_resume_id or f"silent-new-{self.label}",
            )
            self._respond(command, data={"cancelled": False})
        elif kind == "new_session":
            self._session_seq += 1
            parent = command.get("parentSession")
            self.parent_sessions.append(parent)
            if isinstance(parent, str):
                self.parent_payloads.append(Path(parent).read_bytes())
            self.session_id = f"{self.label}-child-{self._session_seq}"
            self.session_file = os.path.abspath(
                self.root / f"{self.session_id}.jsonl"
            )
            # No file yet: Pi materializes after the first assistant message.
            self._respond(command, data={"cancelled": False})
        elif kind == "prompt":
            self._handle_prompt(command)
        elif kind == "get_last_assistant_text":
            self._respond(command, data={"text": self._last_text})
        elif kind == "steer":
            self._respond(command)
        elif kind == "abort":
            if self._pending_prompt is not None:
                self._respond(self._pending_prompt)
                self._pending_prompt = None
            if self._hang_prompt:
                self._hang_prompt = False
                message = self._assistant("", "aborted", usage=(1, 0, 0, 0), stamp=99)
                self._push({"type": "message_end", "message": message})
                if self._materialize:
                    path = Path(self.session_file)
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("{}\n", encoding="utf-8")
                if self._abort_settles:
                    self._push({"type": "agent_settled"})
            self._respond(command)
        else:
            self._respond(command, success=False, error=f"Unknown command: {kind}")

    def read_line(self, timeout: float | None) -> str | None:
        try:
            item = self._out.get(timeout=timeout)
        except queue.Empty:
            from pulse_system.agent.backends.pi import RpcTimeout

            raise RpcTimeout from None
        if item is self._EOF:
            return None
        assert isinstance(item, str)
        return item

    def signal_close(self) -> bool:
        self.close_signal_calls += 1
        if self._close_order is not None:
            self._close_order.append(f"signal:{self.label}")
        if self.closed:
            return True
        self.closed = True
        self._out.put(self._EOF)
        if self._close_raises:
            raise OSError("fake close failed")
        return True

    def wait_closed(self, timeout_sec: float | None = None) -> dict:
        self.close_wait_calls += 1
        if self._close_wait_entered is not None:
            self._close_wait_entered.set()
        if self._close_wait_release is not None:
            self._close_wait_release.wait(timeout=timeout_sec)
        if self._close_order is not None:
            self._close_order.append(f"wait:{self.label}")
        if self._close_summary_sequence:
            index = min(
                self.close_wait_calls - 1,
                len(self._close_summary_sequence) - 1,
            )
            summary = dict(self._close_summary_sequence[index])
            summary["signal_sent"] = self.closed
            return summary
        return {
            "signal_sent": self.closed,
            "process_owners_observed": self._close_process_observed,
            "process_owners_unresolved": self._close_process_unresolved,
            "reader_owners_observed": self._close_reader_unresolved,
            "reader_owners_unresolved": self._close_reader_unresolved,
            "internal_owner_unresolved": self._close_reader_unresolved,
            "owner_joined": self.closed
            and self._close_process_unresolved == 0
            and self._close_reader_unresolved == 0,
            "process_tree_state": (
                "unknown"
                if self._close_process_unresolved or self._close_reader_unresolved
                else "not_applicable"
            ),
            "returncode": 143 if self.closed else None,
            "error_code": (
                "pi_test_owner_unresolved"
                if self._close_process_unresolved or self._close_reader_unresolved
                else None
            ),
        }

    def close(self) -> dict:
        self.signal_close()
        return self.wait_closed()

    def diagnostics(self) -> dict:
        return {"returncode": None if not self.closed else 143, "stderr_tail": ""}

    def _handle_prompt(self, command: dict) -> None:
        self.prompts.append(command["message"])
        self._prompt_seq += 1
        self.prompt_seen.set()
        if self._die_before_prompt_ack:
            self._out.put(self._EOF)
            return

        if self._delay_prompt_ack:
            self._pending_prompt = command
            return

        self._respond(command)
        if self._die_after_prompt_ack:
            self._out.put(self._EOF)
            return
        if self._hang_prompt:
            return

        tool_message = self._assistant(
            "checking",
            "toolUse",
            usage=(3, 1, 2, 0),
            stamp=self._prompt_seq * 10,
        )
        final_text = f"answer-{self.label}-{self._prompt_seq}"
        final_message = self._assistant(
            final_text,
            self._stop_reason,
            usage=(5, 2, 1, 1),
            stamp=self._prompt_seq * 10 + 1,
        )
        self._last_text = final_text
        self._push({"type": "message_end", "message": tool_message})
        self._push({"type": "tool_execution_start", "toolCallId": "tool-1"})
        self._push({"type": "tool_execution_end", "toolCallId": "tool-1"})
        self._push({"type": "message_end", "message": final_message})
        # Same message on turn_end must not double-count usage.
        self._push({"type": "turn_end", "message": final_message})
        self._push({"type": "agent_end", "messages": [tool_message, final_message]})
        if self._materialize:
            path = Path(self.session_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}\n", encoding="utf-8")
        self._push({"type": "agent_settled"})

    @staticmethod
    def _assistant(
        text: str,
        stop_reason: str,
        *,
        usage: tuple[int, int, int, int],
        stamp: int,
    ) -> dict:
        input_tokens, output_tokens, cache_read, cache_write = usage
        message = {
            "role": "assistant",
            "timestamp": stamp,
            "stopReason": stop_reason,
            "content": [] if not text else [{"type": "text", "text": text}],
            "usage": {
                "input": input_tokens,
                "output": output_tokens,
                "cacheRead": cache_read,
                "cacheWrite": cache_write,
                "totalTokens": input_tokens + output_tokens,
                "cost": {
                    "input": 0,
                    "output": 0,
                    "cacheRead": 0,
                    "cacheWrite": 0,
                    "total": 0,
                },
            },
        }
        if stop_reason == "error":
            message["errorMessage"] = "provider failed"
        return message

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

    def release_prompt_ack(self) -> None:
        """Release one deliberately delayed prompt acknowledgement."""

        command = self._pending_prompt
        if command is None:
            raise AssertionError("no delayed prompt acknowledgement is pending")
        self._pending_prompt = None
        self._respond(command)

    def _push(self, value: dict) -> None:
        self._out.put(json.dumps(value))


class FakeFactory:
    def __init__(self, root: Path, **options) -> None:
        self.root = root
        self.options = options
        self.transports: list[FakePersistentPi] = []
        self.argvs: list[list[str]] = []
        self.settings_seen: list[bool] = []

    def __call__(self, argv: list[str]) -> FakePersistentPi:
        self.argvs.append(list(argv))
        settings = self.root / ".pi" / "settings.json"
        self.settings_seen.append(settings.is_file())
        fake = FakePersistentPi(
            self.root,
            f"pi{len(self.transports) + 1}",
            **self.options,
        )
        self.transports.append(fake)
        return fake


class LegacyCloseOnlyTransport:
    _EOF = object()

    def __init__(self) -> None:
        self.closed = False
        self.diagnostics_calls = 0
        self._out: queue.Queue[object] = queue.Queue()

    def send_line(self, text: str) -> None:
        del text

    def read_line(self, timeout: float | None) -> str | None:
        del timeout
        item = self._out.get()
        return None if item is self._EOF else str(item)

    def close(self) -> None:
        self.closed = True
        self._out.put(self._EOF)

    def diagnostics(self) -> dict:
        self.diagnostics_calls += 1
        return {"returncode": 0 if self.closed else None, "stderr_tail": ""}


def make_runtime(
    tmp_path: Path,
    factory: FakeFactory,
    *,
    publication_gate: RuntimePublicationGate | None = None,
    **kwargs,
) -> PiHarnessRuntime:
    backend = PiBackend(workdir=tmp_path, transport_factory=factory)
    gate = publication_gate or RuntimePublicationGate(
        f"pi-test-{id(factory):x}",
        1,
    )
    kwargs.setdefault("publication_permit", gate.publication_permit)
    kwargs.setdefault("bootstrap_permit", gate.bootstrap_permit)
    return PiHarnessRuntime(tmp_path, backend=backend, **kwargs)


class TestSettingsAndBindings:
    def test_settings_merge_preserves_unknown_fields(self, tmp_path: Path) -> None:
        settings = tmp_path / ".pi" / "settings.json"
        settings.parent.mkdir()
        settings.write_text(
            json.dumps({"theme": "dark", "compaction": {"enabled": True, "reserveTokens": 9}}),
            encoding="utf-8",
        )

        assert merge_pi_settings(tmp_path) == settings
        stored = json.loads(settings.read_text(encoding="utf-8"))
        assert stored == {
            "theme": "dark",
            "compaction": {"enabled": False, "reserveTokens": 9},
        }

    def test_invalid_settings_are_not_overwritten(self, tmp_path: Path) -> None:
        settings = tmp_path / ".pi" / "settings.json"
        settings.parent.mkdir()
        settings.write_text("{broken", encoding="utf-8")

        with pytest.raises(HarnessError) as caught:
            merge_pi_settings(tmp_path)
        assert caught.value.code == "pi_settings_invalid"
        assert settings.read_text(encoding="utf-8") == "{broken"

    def test_duplicate_materialized_session_file_is_rejected(self, tmp_path: Path) -> None:
        path = tmp_path / "one.jsonl"
        a = SessionBinding(
            "a", BindingState.MATERIALIZED,
            session_id="s-a", session_file=str(path), bootstrapped=True,
        )
        b = SessionBinding(
            "b", BindingState.MATERIALIZED,
            session_id="s-b", session_file=str(path), bootstrapped=True,
        )

        with pytest.raises(HarnessError) as caught:
            load_binding_state(binding_snapshot({"a": a, "b": b}))
        assert caught.value.code == "pi_binding_conflict"


class TestPersistentTurns:
    def test_turn_id_observation_seam_and_evidence_class(self, tmp_path: Path) -> None:
        observed: list[tuple[str, str | None, dict]] = []
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(
            tmp_path,
            factory,
            event_callback=lambda engram, turn_id, event: observed.append(
                (engram, turn_id, event)
            ),
        )
        try:
            result = runtime.run_turn("engram-a", "hello", turn_id="turn-1")
        finally:
            runtime.close()

        assert result.evidence_class == "FAKE_RPC_CONTRACT"
        assert observed
        assert all(engram == "engram-a" and turn_id == "turn-1" for engram, turn_id, _ in observed)
        assert observed[0][2]["type"] == "turn_started"
        assert {event["type"] for _, _, event in observed} >= {
            "tool_execution_start",
            "tool_execution_end",
            "turn_terminal",
        }

    def test_two_turns_reuse_one_process_and_aggregate_current_turn_usage(
        self, tmp_path: Path,
    ) -> None:
        snapshots: list[dict] = []
        metrics: list[tuple[str, dict]] = []
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(
            tmp_path,
            factory,
            binding_callback=snapshots.append,
            metrics_callback=lambda event, fields: metrics.append((event, fields)),
        )

        first = runtime.run_turn("engram-a", "hello", bootstrap_text="seed")
        second = runtime.run_turn("engram-a", "again", bootstrap_text="ignored")

        assert len(factory.transports) == 1
        fake = factory.transports[0]
        assert factory.settings_seen == [True]
        assert fake.prompts == ["seed\n\nhello", "again"]
        assert first.session_id == second.session_id
        assert first.session_file == second.session_file
        assert (first.input_tokens, first.output_tokens) == (8, 3)
        assert (first.cached_tokens, first.cache_write_tokens) == (3, 1)
        assert first.tool_calls == 1
        # One logical Harness turn crossed the provider twice: first for the
        # tool request and once more for the post-tool continuation.  The
        # duplicate turn_end projection of the final message is not counted.
        assert first.provider_requests == 2
        assert second.provider_requests == 2
        assert snapshots[-1]["sessions"]["engram-a"]["state"] == "materialized"
        assert snapshots[-1]["sessions"]["engram-a"]["bootstrapped"] is True

        types = [command["type"] for command in fake.sent]
        first_prompt = types.index("prompt")
        assert "set_auto_compaction" in types[:first_prompt]
        assert types[:first_prompt].count("get_state") >= 2
        for _event, fields in metrics:
            assert "prompt" not in fields
            assert "output" not in fields
            assert "session_file" not in fields
        runtime.close()
        assert fake.closed

    def test_two_engrams_never_share_a_process_or_file(self, tmp_path: Path) -> None:
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(tmp_path, factory)

        one = runtime.run_turn("one", "a")
        two = runtime.run_turn("two", "b")

        assert len(factory.transports) == 2
        assert one.session_file != two.session_file
        runtime.close()

    def test_process_contexts_are_isolated(self, tmp_path: Path) -> None:
        factory = FakeFactory(tmp_path)
        created: list[str] = []
        revoked: list[str] = []

        def context_for(engram_id: str) -> PiProcessContext:
            created.append(engram_id)
            return PiProcessContext(
                extra_args=("--custom-flag", "value with spaces"),
                env={
                    "PULSE_TOOL_GATEWAY_URL": "http://127.0.0.1:32123",
                    "PULSE_TOOL_CAPABILITY": f"cap-{engram_id}",
                },
                revoke=lambda engram_id=engram_id: revoked.append(engram_id),
            )

        runtime = make_runtime(
            tmp_path,
            factory,
            session_context_factory=context_for,
        )
        runtime.run_turn("one", "a")
        runtime.run_turn("two", "b")

        assert created == ["one", "two"]
        assert revoked == []
        assert all("--extension" in argv for argv in factory.argvs)
        assert all("value with spaces" in argv for argv in factory.argvs)
        expected_agent_dir = str(
            tmp_path / ".pulse" / "harness" / "pi" / "agent"
        )
        expected_session_dir = str(
            tmp_path / ".pulse" / "harness" / "pi" / "sessions"
        )
        assert all(
            session._backend._env["PI_CODING_AGENT_DIR"] == expected_agent_dir
            and session._backend._env["PI_CODING_AGENT_SESSION_DIR"]
            == expected_session_dir
            for session in runtime._sessions.values()
        )

        runtime.close()
        assert sorted(revoked) == ["one", "two"]

    def test_default_runtime_preserves_legacy_pi_invocation(self, tmp_path: Path) -> None:
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(tmp_path, factory)

        runtime.run_turn("legacy", "hello")

        assert factory.argvs == [["pi", "--mode", "rpc"]]
        session = runtime._sessions["legacy"]
        assert session._backend._extra_args == ()
        assert session._backend._env is not None
        runtime.close()

    def test_succession_closes_predecessor_before_lazy_successor_process(
        self, tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path)
        revoked: list[str] = []

        def context_for(engram_id: str) -> PiProcessContext:
            return PiProcessContext(
                env={
                    "PULSE_TOOL_GATEWAY_URL": "http://127.0.0.1:32123",
                    "PULSE_TOOL_CAPABILITY": f"cap-{engram_id}",
                },
                revoke=lambda engram_id=engram_id: revoked.append(engram_id),
            )

        runtime = make_runtime(
            tmp_path,
            factory,
            session_context_factory=context_for,
        )
        predecessor = runtime.run_turn("old", "live")
        runtime.succeed("old", "new")

        first_fake = factory.transports[0]
        assert first_fake.closed
        assert revoked == ["old"]
        assert "new_session" not in [command["type"] for command in first_fake.sent]

        successor = runtime.run_turn("new", "next")
        assert len(factory.transports) == 2
        second_fake = factory.transports[1]
        assert second_fake.parent_sessions == [predecessor.session_file]
        assert successor.session_file != predecessor.session_file
        assert revoked == ["old"]

        runtime.close()
        assert revoked == ["old", "new"]

    def test_materialized_restart_switches_and_verifies_same_identity(
        self, tmp_path: Path,
    ) -> None:
        first_factory = FakeFactory(tmp_path)
        first_runtime = make_runtime(tmp_path, first_factory)
        first = first_runtime.run_turn("engram", "before")
        persisted = first_runtime.binding_snapshot()
        first_runtime.close()

        second_factory = FakeFactory(
            tmp_path,
            resume_ids={first.session_file: first.session_id},
        )
        second_runtime = make_runtime(
            tmp_path,
            second_factory,
            binding_state=persisted,
        )
        resumed = second_runtime.run_turn("engram", "after")

        fake = second_factory.transports[0]
        switch = next(command for command in fake.sent if command["type"] == "switch_session")
        assert os.path.abspath(switch["sessionPath"]) == first.session_file
        assert resumed.session_id == first.session_id
        assert resumed.session_file == first.session_file
        second_runtime.close()


class TestBoundedResidentFleet:
    def test_twenty_four_bindings_share_four_resident_processes(
        self, tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(tmp_path, factory, max_live_sessions=4)
        results = [
            runtime.run_turn(f"engram-{index:02d}", f"turn {index}")
            for index in range(24)
        ]

        assert len(runtime.binding_snapshot()["sessions"]) == 24
        assert runtime.capacity_snapshot() == {
            "resident_limit": 4,
            "resident_sessions": 4,
            "starting_sessions": 0,
            "busy_sessions": 0,
        }
        assert sum(not transport.closed for transport in factory.transports) == 4

        first = results[0]
        factory.options["resume_ids"] = {first.session_file: first.session_id}
        resumed = runtime.run_turn("engram-00", "after hibernation")

        assert resumed.session_id == first.session_id
        assert resumed.session_file == first.session_file
        assert factory.transports[-1].prompts == ["after hibernation"]
        assert runtime.capacity_snapshot()["resident_sessions"] == 4
        runtime.close()

    def test_idle_lru_process_hibernates_without_losing_binding(
        self, tmp_path: Path,
    ) -> None:
        metrics: list[tuple[str, dict]] = []
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(
            tmp_path,
            factory,
            max_live_sessions=2,
            metrics_callback=lambda event, fields: metrics.append((event, fields)),
        )

        one = runtime.run_turn("one", "first")
        two = runtime.run_turn("two", "second")
        factory.options["resume_ids"] = {
            one.session_file: one.session_id,
            two.session_file: two.session_id,
        }

        runtime.run_turn("three", "third")

        assert runtime.capacity_snapshot() == {
            "resident_limit": 2,
            "resident_sessions": 2,
            "starting_sessions": 0,
            "busy_sessions": 0,
        }
        assert factory.transports[0].closed is True
        assert factory.transports[1].closed is False
        assert runtime.snapshot("one")["session_id"] == one.session_id
        assert runtime.snapshot("one")["state"] == "UNBOUND"
        assert any(
            event == "harness_session_closed"
            and fields.get("reason") == "capacity_hibernate"
            for event, fields in metrics
        )

        resumed = runtime.run_turn("one", "after hibernation")
        assert resumed.session_id == one.session_id
        assert resumed.session_file == one.session_file
        assert len(factory.transports) == 4
        assert runtime.capacity_snapshot()["resident_sessions"] == 2
        runtime.close()

    def test_busy_process_is_never_evicted_and_capacity_wait_has_deadline(
        self, tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path, hang_prompt=True)
        runtime = make_runtime(tmp_path, factory, max_live_sessions=1)
        outcome: list[object] = []

        def run_blocked() -> None:
            try:
                outcome.append(runtime.run_turn("busy", "wait", timeout_sec=5.0))
            except BaseException as exc:  # test captures the aborted turn
                outcome.append(exc)

        thread = threading.Thread(target=run_blocked)
        thread.start()
        while not factory.transports:
            thread.join(timeout=0.01)
        assert factory.transports[0].prompt_seen.wait(timeout=1.0)

        with pytest.raises(HarnessError) as caught:
            runtime.run_turn("waiting", "later", timeout_sec=0.05)
        assert caught.value.code == "pi_capacity_timeout"
        assert caught.value.retryable is True
        assert caught.value.prompt_accepted is False
        assert len(factory.transports) == 1
        assert factory.transports[0].closed is False

        runtime.abort("busy")
        thread.join(timeout=2.0)
        assert not thread.is_alive()
        assert len(outcome) == 1
        runtime.close()

    def test_succession_capacity_wait_uses_its_bounded_deadline(
        self,
        tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(tmp_path, factory, max_live_sessions=1)
        predecessor = runtime.run_turn("old", "seed")
        factory.options["resume_ids"] = {
            predecessor.session_file: predecessor.session_id,
        }
        factory.options["hang_prompt"] = True
        outcome: list[object] = []

        def occupy_only_process() -> None:
            try:
                outcome.append(
                    runtime.run_turn("busy", "wait", timeout_sec=5.0)
                )
            except BaseException as exc:  # test captures the aborted turn
                outcome.append(exc)

        thread = threading.Thread(target=occupy_only_process)
        thread.start()
        while len(factory.transports) < 2:
            thread.join(timeout=0.01)
        assert factory.transports[-1].prompt_seen.wait(timeout=1.0)

        with pytest.raises(HarnessError) as caught:
            runtime.succeed("old", "new", capacity_timeout_sec=0.05)
        assert caught.value.code == "pi_capacity_timeout"
        assert caught.value.phase == "capacity"
        assert caught.value.prompt_accepted is False
        assert "new" not in runtime.binding_snapshot()["sessions"]

        runtime.abort("busy")
        thread.join(timeout=2.0)
        assert not thread.is_alive()
        runtime.close()

    @pytest.mark.parametrize("value", [0, 257, True, 1.5, "8"])
    def test_invalid_resident_limit_is_rejected(
        self, tmp_path: Path, value,
    ) -> None:
        factory = FakeFactory(tmp_path)
        with pytest.raises(ValueError, match="max_live_sessions"):
            make_runtime(tmp_path, factory, max_live_sessions=value)


class TestPendingLineage:
    def test_successor_restart_recreates_lineage_without_hidden_turn(
        self, tmp_path: Path,
    ) -> None:
        first_factory = FakeFactory(tmp_path)
        runtime = make_runtime(tmp_path, first_factory)
        predecessor = runtime.run_turn("old", "live")

        runtime.succeed("old", "new")
        first_fake = first_factory.transports[0]
        assert first_fake.prompts == ["live"]
        pending = runtime.binding_snapshot()
        wire = pending["sessions"]["new"]
        assert wire["state"] == "pending_lineage"
        assert wire["session_id"] is None
        assert wire["session_file"] is None
        assert wire["parent_session_file"] == predecessor.session_file
        assert first_fake.closed
        assert Path(predecessor.session_file).exists()
        assert "new_session" not in [command["type"] for command in first_fake.sent]
        runtime.close()

        second_factory = FakeFactory(tmp_path)
        resumed = make_runtime(tmp_path, second_factory, binding_state=pending)
        result = resumed.run_turn("new", "next", bootstrap_text="summary")

        second_fake = second_factory.transports[0]
        types = [command["type"] for command in second_fake.sent]
        assert types.index("new_session") < types.index("prompt")
        assert "switch_session" not in types
        assert second_fake.parent_sessions == [predecessor.session_file]
        assert second_fake.prompts == ["summary\n\nnext"]
        materialized = resumed.binding_snapshot()["sessions"]["new"]
        assert materialized["state"] == "materialized"
        assert materialized["parent_session_file"] == predecessor.session_file
        assert result.session_file == materialized["session_file"]
        resumed.close()

    def test_missing_materialized_file_fails_before_spawning(self, tmp_path: Path) -> None:
        missing = SessionBinding(
            "e",
            BindingState.MATERIALIZED,
            session_id="lost",
            session_file=str(tmp_path / "missing.jsonl"),
            bootstrapped=True,
        )
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(
            tmp_path,
            factory,
            binding_state=binding_snapshot({"e": missing}),
        )

        with pytest.raises(HarnessError) as caught:
            runtime.run_turn("e", "wake")
        assert caught.value.code == "pi_session_resume_failed"
        assert factory.transports == []

    def test_failed_materialization_breaks_and_closes_live_process(
        self, tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path, materialize=False)
        runtime = make_runtime(tmp_path, factory)

        with pytest.raises(HarnessError) as caught:
            runtime.run_turn("e", "wake")
        assert caught.value.code == "pi_session_materialization_failed"
        assert caught.value.prompt_accepted is True
        assert caught.value.retryable is False
        assert factory.transports[0].closed

    def test_materialization_has_its_own_deadline_and_never_returns_ready(
        self, tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path, hang_materialize_state=True)
        runtime = make_runtime(tmp_path, factory, sideband_timeout_sec=0.05)

        with pytest.raises(HarnessError) as caught:
            runtime.run_turn("e", "wake", timeout_sec=1)

        assert caught.value.code == "pi_timeout"
        assert caught.value.phase == "finalize"
        assert caught.value.prompt_accepted is True
        assert factory.transports[0].closed
        with pytest.raises(HarnessError) as idle:
            runtime.abort("e")
        assert idle.value.code == "pi_session_not_running"


class TestFailureAndSidebandSemantics:
    def test_idle_abort_and_steer_do_not_start_pi(self, tmp_path: Path) -> None:
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(tmp_path, factory)

        with pytest.raises(HarnessError) as abort_error:
            runtime.abort("e")
        with pytest.raises(HarnessError) as steer_error:
            runtime.steer("e", "news")
        assert abort_error.value.code == "pi_session_not_running"
        assert steer_error.value.code == "pi_session_not_running"
        assert factory.transports == []

    def test_steer_and_abort_correlate_while_turn_is_running(self, tmp_path: Path) -> None:
        factory = FakeFactory(tmp_path, hang_prompt=True)
        sideband_ready = threading.Event()

        def project(_engram_id: str, _turn_id: str | None, event: dict) -> None:
            if event.get("type") == "turn_started" and event.get("sideband_ready") is True:
                sideband_ready.set()

        runtime = make_runtime(tmp_path, factory, event_callback=project)
        outcome: list[HarnessError] = []

        def run() -> None:
            try:
                runtime.run_turn("e", "long", timeout_sec=5)
            except HarnessError as exc:
                outcome.append(exc)

        worker = threading.Thread(target=run)
        worker.start()
        while not factory.transports:
            worker.join(timeout=0.01)
        fake = factory.transports[0]
        assert fake.prompt_seen.wait(2)
        assert sideband_ready.wait(2)
        assert runtime._sessions["e"].wait_sideband_ready(2)
        runtime.steer("e", "new stimulus")
        runtime.abort("e")
        worker.join(timeout=2)

        assert not worker.is_alive()
        assert outcome and outcome[0].code == "pi_aborted"
        assert [c["type"] for c in fake.sent].count("steer") == 1
        assert [c["type"] for c in fake.sent].count("abort") == 1
        runtime.close()

    def test_admitting_steer_is_rejected_until_durable_sideband_ready(
        self,
        tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path, hang_prompt=True, delay_prompt_ack=True)
        sideband_ready = threading.Event()
        projected: list[dict] = []

        def project(_engram_id: str, _turn_id: str | None, event: dict) -> None:
            projected.append(dict(event))
            if event.get("type") == "turn_started" and event.get("sideband_ready") is True:
                sideband_ready.set()

        runtime = make_runtime(tmp_path, factory, event_callback=project)
        outcome: list[HarnessError] = []

        def run() -> None:
            try:
                runtime.run_turn(
                    "e",
                    "long",
                    timeout_sec=5,
                    turn_id="turn-admission-steer",
                )
            except HarnessError as exc:
                outcome.append(exc)

        worker = threading.Thread(target=run)
        worker.start()
        while not factory.transports:
            worker.join(timeout=0.01)
        fake = factory.transports[0]
        assert fake.prompt_seen.wait(2)
        assert runtime._sessions["e"].state.value == "ADMITTING"

        with pytest.raises(HarnessError) as pending:
            runtime.steer("e", "must not race prompt admission")
        assert pending.value.code == "pi_steer_admission_pending"
        assert pending.value.retryable is True
        assert pending.value.prompt_accepted is None
        assert [command["type"] for command in fake.sent].count("steer") == 0
        assert sideband_ready.is_set() is False
        assert runtime._sessions["e"].state.value == "ADMITTING"
        assert not any(event.get("type") == "turn_terminal" for event in projected)

        fake.release_prompt_ack()
        assert sideband_ready.wait(2)
        assert runtime._sessions["e"].wait_sideband_ready(2)
        assert runtime._sessions["e"].snapshot()["sideband_ready"] is True
        runtime.steer("e", "now admitted")
        runtime.abort("e")
        worker.join(timeout=2)

        assert not worker.is_alive()
        assert outcome and outcome[0].code == "pi_aborted"
        assert [command["type"] for command in fake.sent].count("steer") == 1
        assert [command["type"] for command in fake.sent].count("abort") == 1
        runtime.close()

    @pytest.mark.parametrize("seed", range(24))
    def test_admission_boundary_randomized_interleaving_is_classified(
        self,
        tmp_path: Path,
        seed: int,
    ) -> None:
        rng = random.Random(seed)
        steer_delay = rng.uniform(0.0, 0.003)
        ack_delay = rng.uniform(0.0, 0.003)
        workspace = tmp_path / f"seed-{seed}"
        workspace.mkdir()
        factory = FakeFactory(
            workspace,
            hang_prompt=True,
            delay_prompt_ack=True,
        )
        runtime = make_runtime(workspace, factory)
        turn_outcome: list[HarnessError] = []
        control_outcome: list[str | HarnessError] = []

        def run() -> None:
            try:
                runtime.run_turn(
                    "e",
                    "long",
                    timeout_sec=5,
                    turn_id=f"turn-random-{seed}",
                )
            except HarnessError as exc:
                turn_outcome.append(exc)

        worker = threading.Thread(target=run)
        worker.start()
        while not factory.transports:
            worker.join(timeout=0.01)
            if not worker.is_alive():
                pytest.fail(f"turn ended before transport startup: {turn_outcome!r}")
        fake = factory.transports[0]
        assert fake.prompt_seen.wait(2)
        assert runtime._sessions["e"].state.value == "ADMITTING"

        start_race = threading.Event()

        def steer() -> None:
            assert start_race.wait(1)
            time.sleep(steer_delay)
            try:
                runtime.steer("e", f"seed {seed}")
                control_outcome.append("accepted")
            except HarnessError as exc:
                control_outcome.append(exc)

        def acknowledge() -> None:
            assert start_race.wait(1)
            time.sleep(ack_delay)
            fake.release_prompt_ack()

        steerer = threading.Thread(target=steer)
        acknowledger = threading.Thread(target=acknowledge)
        steerer.start()
        acknowledger.start()
        start_race.set()
        steerer.join(timeout=2)
        acknowledger.join(timeout=2)

        assert not steerer.is_alive()
        assert not acknowledger.is_alive()
        assert runtime._sessions["e"].wait_sideband_ready(2)
        assert len(control_outcome) == 1
        classified = control_outcome[0]
        if isinstance(classified, HarnessError):
            assert classified.code == "pi_steer_admission_pending"
            assert [command["type"] for command in fake.sent].count("steer") == 0
        else:
            assert classified == "accepted"
            assert [command["type"] for command in fake.sent].count("steer") == 1

        runtime.abort("e")
        worker.join(timeout=2)
        assert not worker.is_alive()
        assert turn_outcome and turn_outcome[0].code == "pi_aborted"
        runtime.close()

    def test_failed_turn_started_projection_never_opens_steer_window(
        self,
        tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path, hang_prompt=True)
        projection_attempted = threading.Event()

        def fail_projection(
            _engram_id: str,
            _turn_id: str | None,
            event: dict,
        ) -> None:
            if event.get("type") == "turn_started":
                projection_attempted.set()
                raise RuntimeError("durable projection unavailable")

        runtime = make_runtime(tmp_path, factory, event_callback=fail_projection)
        turn_outcome: list[HarnessError] = []

        def run() -> None:
            try:
                runtime.run_turn(
                    "e",
                    "long",
                    timeout_sec=5,
                    turn_id="turn-projection-failed",
                )
            except HarnessError as exc:
                turn_outcome.append(exc)

        worker = threading.Thread(target=run)
        worker.start()
        while not factory.transports:
            worker.join(timeout=0.01)
        assert factory.transports[0].prompt_seen.wait(2)
        assert projection_attempted.wait(2)
        assert runtime._sessions["e"].state.value == "RUNNING"
        assert runtime._sessions["e"].wait_sideband_ready(0.02) is False

        with pytest.raises(HarnessError) as pending:
            runtime.steer("e", "must remain fenced")
        assert pending.value.code == "pi_steer_admission_pending"
        assert runtime._sessions["e"].state.value == "RUNNING"

        runtime.abort("e")
        worker.join(timeout=2)
        assert not worker.is_alive()
        assert turn_outcome and turn_outcome[0].code == "pi_aborted"
        runtime.close()

    def test_external_abort_requires_settled_barrier_before_return(
        self, tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path, hang_prompt=True, abort_settles=False)
        runtime = make_runtime(tmp_path, factory, abort_timeout_sec=0.05)
        outcome: list[HarnessError] = []

        def run() -> None:
            try:
                runtime.run_turn("e", "long", timeout_sec=5)
            except HarnessError as exc:
                outcome.append(exc)

        worker = threading.Thread(target=run)
        worker.start()
        while not factory.transports:
            worker.join(timeout=0.01)
        assert factory.transports[0].prompt_seen.wait(2)

        with pytest.raises(HarnessError) as caught:
            runtime.abort("e")
        assert caught.value.code == "pi_abort_settle_timeout"
        assert caught.value.prompt_accepted is True
        worker.join(timeout=2)
        assert not worker.is_alive()
        assert factory.transports[0].closed
        assert outcome and outcome[0].code == "pi_connection_lost"

    def test_external_abort_returns_after_terminal_projection(
        self, tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path, hang_prompt=True)
        terminal_entered = threading.Event()
        release_terminal = threading.Event()

        def project(_engram_id: str, _turn_id: str | None, event: dict) -> None:
            if event.get("type") == "turn_terminal":
                terminal_entered.set()
                assert release_terminal.wait(2)

        runtime = make_runtime(tmp_path, factory, event_callback=project)
        turn_errors: list[HarnessError] = []
        abort_errors: list[HarnessError] = []

        def run() -> None:
            try:
                runtime.run_turn("e", "long", timeout_sec=5, turn_id="turn-terminal-barrier")
            except HarnessError as exc:
                turn_errors.append(exc)

        def abort() -> None:
            try:
                runtime.abort("e")
            except HarnessError as exc:
                abort_errors.append(exc)

        worker = threading.Thread(target=run)
        worker.start()
        while not factory.transports:
            worker.join(timeout=0.01)
        assert factory.transports[0].prompt_seen.wait(2)
        aborter = threading.Thread(target=abort)
        aborter.start()
        assert terminal_entered.wait(2)
        aborter.join(timeout=0.05)
        assert aborter.is_alive()

        release_terminal.set()
        aborter.join(timeout=2)
        worker.join(timeout=2)
        assert not aborter.is_alive()
        assert not worker.is_alive()
        assert abort_errors == []
        assert turn_errors and turn_errors[0].code == "pi_aborted"
        assert runtime._sessions["e"].state.value == "READY"

    def test_external_abort_cancels_prompt_ack_pending_turn(
        self, tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path, hang_prompt=True, delay_prompt_ack=True)
        runtime = make_runtime(tmp_path, factory)
        outcome: list[HarnessError] = []

        def run() -> None:
            try:
                runtime.run_turn("e", "long", timeout_sec=5, turn_id="turn-admitting")
            except HarnessError as exc:
                outcome.append(exc)

        worker = threading.Thread(target=run)
        worker.start()
        while not factory.transports:
            worker.join(timeout=0.01)
        fake = factory.transports[0]
        assert fake.prompt_seen.wait(2)
        assert runtime._sessions["e"].state.value == "ADMITTING"

        runtime.abort("e")
        worker.join(timeout=2)

        assert not worker.is_alive()
        assert outcome and outcome[0].code == "pi_aborted"
        assert [command["type"] for command in fake.sent].count("abort") == 1
        assert runtime._sessions["e"].state.value == "READY"

    @pytest.mark.parametrize(
        ("factory_options", "accepted", "retryable"),
        [
            ({"die_before_prompt_ack": True}, None, False),
            ({"die_after_prompt_ack": True}, True, False),
        ],
    )
    def test_connection_loss_distinguishes_prompt_acceptance(
        self,
        tmp_path: Path,
        factory_options: dict,
        accepted: bool | None,
        retryable: bool,
    ) -> None:
        factory = FakeFactory(tmp_path, **factory_options)
        runtime = make_runtime(tmp_path, factory)

        with pytest.raises(HarnessError) as caught:
            runtime.run_turn("e", "act", timeout_sec=2)
        assert caught.value.prompt_accepted is accepted
        assert caught.value.retryable is retryable

    def test_timeout_abort_requires_agent_settled_before_reuse(
        self, tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path, hang_prompt=True, abort_settles=False)
        runtime = make_runtime(tmp_path, factory, abort_timeout_sec=0.05)

        # The assertion concerns post-acceptance abort settlement, not whether
        # a loaded Windows runner can construct and admit a session in 20 ms.
        with pytest.raises(HarnessError) as caught:
            runtime.run_turn("e", "long", timeout_sec=0.2)

        assert caught.value.code == "pi_timeout"
        assert caught.value.prompt_accepted is True
        assert "settlement could not be confirmed" in caught.value.detail
        assert factory.transports[0].closed

    def test_close_during_succession_cannot_resurrect_session(
        self, tmp_path: Path,
    ) -> None:
        snapshots: list[dict] = []
        runtime_box: dict[str, PiHarnessRuntime] = {}

        def persist(snapshot: dict) -> None:
            snapshots.append(snapshot)
            if "new" in snapshot["sessions"]:
                runtime_box["runtime"].close()

        factory = FakeFactory(tmp_path)
        runtime = make_runtime(tmp_path, factory, binding_callback=persist)
        runtime_box["runtime"] = runtime
        runtime.run_turn("old", "live")

        with pytest.raises(HarnessError) as caught:
            runtime.succeed("old", "new")

        assert caught.value.code == "harness_closed_during_succession"
        assert factory.transports[0].closed
        with pytest.raises(HarnessError) as closed:
            runtime.run_turn("new", "must not run")
        assert closed.value.code == "harness_closed"

    def test_runtime_close_continues_after_one_transport_close_raises(
        self, tmp_path: Path,
    ) -> None:
        metrics: list[tuple[str, dict]] = []
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(
            tmp_path,
            factory,
            metrics_callback=lambda event, fields: metrics.append((event, fields)),
        )
        runtime.run_turn("one", "a")
        runtime.run_turn("two", "b")
        factory.transports[0]._close_raises = True

        runtime.close()

        assert all(transport.closed for transport in factory.transports)
        assert any(event == "harness_session_close_failed" for event, _ in metrics)


class TestPiShutdownAndContinuityP0:
    def test_canonical_prefix_copy_uses_native_long_path_adapter(
        self,
        tmp_path: Path,
    ) -> None:
        gate = RuntimePublicationGate("pi-test-long-path", 1)
        guard = _PiContinuityGuard(
            tmp_path,
            publication_permit=gate.publication_permit,
            bootstrap_permit=gate.bootstrap_permit,
        )
        deep_root = tmp_path / "long-pi-source"
        deep_parent = deep_root
        index = 0
        while len(str(deep_parent / "session.jsonl")) < 280:
            deep_parent = deep_parent / f"segment-{index:02d}-abcdefghijklmnop"
            index += 1
        source = deep_parent / "session.jsonl"
        payload = b'{"type":"message","content":"canonical"}\n'
        os.makedirs(guard._native_io_path(deep_parent), exist_ok=True)
        try:
            with open(guard._native_io_path(source), "wb") as stream:
                stream.write(payload)

            assert guard._native_is_file(source) is True
            copied = guard._copy_canonical_prefix(source, len(payload))
            assert copied.read_bytes() == payload
        finally:
            try:
                os.unlink(guard._native_io_path(source))
            except FileNotFoundError:
                pass
            current = deep_parent
            while current != deep_root.parent:
                os.rmdir(guard._native_io_path(current))
                current = current.parent

    def test_canonical_prefix_target_and_discard_use_native_long_path_adapter(
        self,
        tmp_path: Path,
    ) -> None:
        deep_root = tmp_path / "long-pi-target"
        deep_workspace = deep_root
        index = 0
        target_leaf = Path(".pulse") / "harness" / "pi" / "sessions"
        while len(str(deep_workspace / target_leaf / "pulse-resume.staging")) < 280:
            deep_workspace = (
                deep_workspace / f"segment-{index:02d}-abcdefghijklmnop"
            )
            index += 1
        gate = RuntimePublicationGate("pi-test-long-target", 1)
        guard = _PiContinuityGuard(
            deep_workspace,
            publication_permit=gate.publication_permit,
            bootstrap_permit=gate.bootstrap_permit,
        )
        source = tmp_path / "short-source.jsonl"
        payload = b'{"type":"message","content":"canonical"}\n'
        source.write_bytes(payload)

        try:
            copied = guard._copy_canonical_prefix(source, len(payload))
            with open(guard._native_io_path(copied), "rb") as stream:
                assert stream.read() == payload

            guard._discard_bootstrap_staging(copied)

            assert not os.path.exists(guard._native_io_path(copied))
        finally:
            current = guard._session_root
            while current != deep_root.parent:
                try:
                    os.rmdir(guard._native_io_path(current))
                except FileNotFoundError:
                    pass
                current = current.parent

    def test_continuity_requires_publication_permit_before_transport_start(
        self,
        tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path)
        runtime = PiHarnessRuntime(
            tmp_path,
            backend=PiBackend(workdir=tmp_path, transport_factory=factory),
        )

        with pytest.raises(HarnessError) as preflight:
            runtime.preflight()
        with pytest.raises(HarnessError) as caught:
            runtime.run_turn("engram", "must-not-start")

        assert preflight.value.code == "pi_continuity_publication_permit_required"
        assert caught.value.code == "pi_continuity_publication_permit_required"
        assert factory.transports == []
        runtime.close()

    def test_recovery_permit_seals_writer_after_runtime_publication_revoke(
        self,
        tmp_path: Path,
    ) -> None:
        gate = RuntimePublicationGate("pi-test-recovery-seal", 1)
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(
            tmp_path,
            factory,
            publication_gate=gate,
        )
        runtime.run_turn("engram", "work")
        recovery_permit = gate.revoke(reason="test_shutdown")

        summary = runtime.close(recovery_permit=recovery_permit)

        assert summary["continuity_writers_sealed"] == 1
        assert summary["owner_joined"] is True
        assert summary["unresolved"] == 0

    def test_revoked_publication_without_recovery_permit_keeps_writer_unresolved(
        self,
        tmp_path: Path,
    ) -> None:
        gate = RuntimePublicationGate("pi-test-missing-recovery", 1)
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(
            tmp_path,
            factory,
            publication_gate=gate,
        )
        runtime.run_turn("engram", "work")
        gate.revoke(reason="test_shutdown")

        summary = runtime.close()

        assert summary["continuity_writers_sealed"] == 0
        assert summary["owner_joined"] is False
        assert summary["internal_owner_unresolved"] >= 1
        assert "pi_continuity_writer_unsealed" in summary["error_codes"]

    def test_cached_close_can_recovery_reseal_without_transport_rewait(
        self,
        tmp_path: Path,
    ) -> None:
        gate = RuntimePublicationGate("pi-test-cross-revoke", 1)
        wait_entered = threading.Event()
        wait_release = threading.Event()
        factory = FakeFactory(
            tmp_path,
            close_wait_entered=wait_entered,
            close_wait_release=wait_release,
        )
        runtime = make_runtime(
            tmp_path,
            factory,
            publication_gate=gate,
        )
        runtime.run_turn("engram", "work")
        first_results: list[dict] = []
        second_results: list[dict] = []

        first = threading.Thread(
            target=lambda: first_results.append(runtime.close()),
        )
        first.start()
        assert wait_entered.wait(timeout=1.0)
        recovery_permit = gate.revoke(reason="test_cross_revoke")
        second = threading.Thread(
            target=lambda: second_results.append(
                runtime.close(recovery_permit=recovery_permit)
            ),
        )
        second.start()
        wait_release.set()
        first.join(timeout=2.0)
        second.join(timeout=2.0)

        assert not first.is_alive()
        assert not second.is_alive()
        assert first_results[0]["continuity_writers_sealed"] == 0
        assert first_results[0]["owner_joined"] is False
        assert second_results[0]["continuity_writers_sealed"] == 1
        assert second_results[0]["owner_joined"] is True
        assert factory.transports[0].close_signal_calls == 1
        assert factory.transports[0].close_wait_calls == 1
        assert runtime.close()["continuity_writers_sealed"] == 1
        assert (
            runtime.close(recovery_permit=recovery_permit)[
                "continuity_writers_sealed"
            ]
            == 1
        )
        assert factory.transports[0].close_signal_calls == 1
        assert factory.transports[0].close_wait_calls == 1

    def test_tail_isolation_requires_distinct_bootstrap_permit(
        self,
        tmp_path: Path,
    ) -> None:
        first_factory = FakeFactory(tmp_path)
        first_runtime = make_runtime(tmp_path, first_factory)
        first = first_runtime.run_turn("engram", "before")
        persisted = first_runtime.binding_snapshot()
        first_runtime.close()
        old_path = Path(first.session_file)
        with old_path.open("ab") as stream:
            stream.write(b'{"late":true}\n')

        gate = RuntimePublicationGate("pi-test-bootstrap-role", 1)
        second_factory = FakeFactory(tmp_path)
        second_runtime = make_runtime(
            tmp_path,
            second_factory,
            publication_gate=gate,
            bootstrap_permit=None,
            binding_state=persisted,
        )

        with pytest.raises(HarnessError) as caught:
            second_runtime.run_turn("engram", "must-not-recover")

        assert caught.value.code == "pi_continuity_bootstrap_permit_required"
        assert second_factory.transports == []
        second_runtime.close()

    def test_real_subprocess_transport_reports_root_and_reader_exit(self) -> None:
        transport = SubprocessRpcTransport(
            [
                sys.executable,
                "-u",
                "-c",
                "import sys; sys.stdin.buffer.read()",
            ]
        )

        transport.close()
        summary = transport.wait_closed(timeout_sec=2.0)

        assert summary["process_owners_observed"] == 1
        assert summary["reader_owners_observed"] == 2
        assert summary["reader_owners_unresolved"] == 0
        if os.name == "nt":
            assert summary["process_owners_unresolved"] == 0
            assert summary["owner_joined"] is True
            assert summary["process_tree_state"] == "empty_verified"
        else:
            assert summary["process_owners_unresolved"] == 1
            assert summary["owner_joined"] is False
            assert summary["process_tree_state"] == "root_exit_only"

    @pytest.mark.skipif(os.name != "nt", reason="requires Windows Job Objects")
    def test_direct_transport_constructor_failure_converges_before_raise(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        native_start = threading.Thread.start
        captured: list[SubprocessRpcTransport] = []

        def fail_stderr_reader(thread: threading.Thread) -> None:
            if thread.name.startswith("pi-stderr-reader-"):
                target_owner = getattr(thread._target, "__self__", None)
                assert isinstance(target_owner, SubprocessRpcTransport)
                captured.append(target_owner)
                raise RuntimeError("injected_direct_reader_start_failure")
            native_start(thread)

        monkeypatch.setattr(threading.Thread, "start", fail_stderr_reader)

        with pytest.raises(RpcConnectionLost, match="reader could not start"):
            SubprocessRpcTransport(
                [
                    sys.executable,
                    "-u",
                    "-c",
                    "import sys,time; sys.stdin.buffer.read(); time.sleep(5)",
                ]
            )

        assert len(captured) == 1
        transport = captured[0]
        assert transport._proc.poll() is not None
        assert transport._close_summary is not None
        assert transport._close_summary["process_tree_state"] == "empty_verified"
        assert transport._close_summary["process_owners_unresolved"] == 0
        assert transport._close_summary["reader_owners_observed"] == 1
        assert transport._close_summary["reader_owners_unresolved"] == 0
        assert transport._close_summary["owner_joined"] is True

    @pytest.mark.skipif(os.name != "nt", reason="requires Windows Job Objects")
    def test_reader_activation_holds_close_fence_until_owner_set_is_committed(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        transport = SubprocessRpcTransport(
            [
                sys.executable,
                "-u",
                "-c",
                "import sys,time; sys.stdin.buffer.read(); time.sleep(5)",
            ],
            start_readers=False,
        )
        native_start = threading.Thread.start
        activation_entered = threading.Event()
        release_activation = threading.Event()
        close_attempted = threading.Event()
        close_returned = threading.Event()
        activation_errors: list[BaseException] = []

        def gate_stdout_reader(thread: threading.Thread) -> None:
            if thread.name.startswith("pi-stdout-reader-"):
                activation_entered.set()
                release_activation.wait(timeout=2.0)
            native_start(thread)

        def activate() -> None:
            try:
                transport.start_readers()
            except BaseException as exc:
                activation_errors.append(exc)

        def close_signal() -> None:
            close_attempted.set()
            transport.signal_close()
            close_returned.set()

        monkeypatch.setattr(threading.Thread, "start", gate_stdout_reader)
        starter = threading.Thread(target=activate, name="test-pi-reader-activator")
        closer = threading.Thread(target=close_signal, name="test-pi-close-owner")
        try:
            starter.start()
            assert activation_entered.wait(timeout=1.0)
            closer.start()
            assert close_attempted.wait(timeout=1.0)
            assert close_returned.wait(timeout=0.05) is False

            release_activation.set()
            starter.join(timeout=1.0)
            closer.join(timeout=1.0)

            assert not starter.is_alive()
            assert not closer.is_alive()
            assert activation_errors == []
            assert transport._reader_started == [True, True]
            assert close_returned.is_set()

            summary = transport.wait_closed(timeout_sec=2.0)
            assert summary["reader_owners_observed"] == 2
            assert summary["reader_owners_unresolved"] == 0
            assert summary["process_tree_state"] == "empty_verified"
            assert summary["owner_joined"] is True
        finally:
            release_activation.set()
            transport.wait_closed(timeout_sec=2.0)

    @pytest.mark.skipif(os.name != "nt", reason="requires Windows Job Objects")
    @pytest.mark.parametrize(
        "failed_thread_prefix, expected_transport_readers",
        (
            ("pi-stderr-reader-", 1),
            ("pi-rpc-reader-", 2),
        ),
        ids=("partial-transport-readers", "rpc-reader"),
    )
    def test_pi_reader_start_failure_converges_exact_published_owner(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        failed_thread_prefix: str,
        expected_transport_readers: int,
    ) -> None:
        class _DeferredContainedPiFactory:
            def __init__(self) -> None:
                self.transports: list[SubprocessRpcTransport] = []

            def __call__(self, _argv: list[str]) -> SubprocessRpcTransport:
                transport = SubprocessRpcTransport(
                    [
                        sys.executable,
                        "-u",
                        "-c",
                        "import sys,time; sys.stdin.buffer.read(); time.sleep(5)",
                    ],
                    cwd=str(tmp_path),
                    start_readers=False,
                )
                self.transports.append(transport)
                return transport

        factory = _DeferredContainedPiFactory()
        runtime = make_runtime(tmp_path, factory)  # type: ignore[arg-type]
        claimed_sessions: list[object] = []
        native_claim = runtime._claim_physical_session_locked

        def observed_claim(session) -> None:
            claimed_sessions.append(session)
            native_claim(session)

        monkeypatch.setattr(
            runtime,
            "_claim_physical_session_locked",
            observed_claim,
        )
        native_start = threading.Thread.start
        failed = False

        def fail_selected_reader_once(thread: threading.Thread) -> None:
            nonlocal failed
            if not failed and thread.name.startswith(failed_thread_prefix):
                failed = True
                raise RuntimeError("injected_pi_reader_start_failure")
            native_start(thread)

        monkeypatch.setattr(threading.Thread, "start", fail_selected_reader_once)
        try:
            with pytest.raises(HarnessError) as caught:
                runtime.run_turn("engram", "must-not-run")

            assert caught.value.code == "pi_startup_failed"
            assert failed is True
            assert len(factory.transports) == 1
            transport = factory.transports[0]
            assert sum(transport._reader_started) == expected_transport_readers
            assert transport._proc.poll() is not None
            assert transport._close_summary is not None
            assert transport._close_summary["process_tree_state"] == "empty_verified"
            assert transport._close_summary["process_owners_unresolved"] == 0
            assert transport._close_summary["reader_owners_observed"] == (
                expected_transport_readers
            )
            assert transport._close_summary["reader_owners_unresolved"] == 0
            assert transport._close_summary["owner_joined"] is True
            assert claimed_sessions
            assert runtime._physical_sessions == {}
        finally:
            runtime.close(timeout_sec=2.25)

    def test_close_signal_thread_start_failure_rolls_back_and_retries(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        transport = FakePersistentPi(tmp_path, "close-signal-start-retry")
        channel = PiRpcChannel(transport, id_prefix="close-signal-start-retry")
        native_start = threading.Thread.start
        failures = 0

        def fail_close_signal_once(thread: threading.Thread) -> None:
            nonlocal failures
            if failures == 0 and thread.name.startswith("pi-rpc-close-signal-"):
                failures += 1
                raise RuntimeError("injected_close_signal_start_failure")
            native_start(thread)

        monkeypatch.setattr(threading.Thread, "start", fail_close_signal_once)

        with pytest.raises(RuntimeError, match="injected_close_signal_start_failure"):
            channel.begin_close()

        assert channel._close_signal_thread is None
        assert transport.close_signal_calls == 0

        summary = channel.finish_close(timeout_sec=1.0)

        assert failures == 1
        assert transport.close_signal_calls == 1
        assert summary["signal_sent"] is True
        assert summary["unresolved"] == 0
        assert summary["owner_joined"] is True
        assert summary["process_tree_state"] == "not_applicable"

    def test_close_wait_thread_start_failure_rolls_back_and_retries(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        transport = FakePersistentPi(tmp_path, "close-wait-start-retry")
        channel = PiRpcChannel(transport, id_prefix="close-wait-start-retry")
        native_start = threading.Thread.start
        failures = 0

        def fail_close_wait_once(thread: threading.Thread) -> None:
            nonlocal failures
            if failures == 0 and thread.name.startswith("pi-rpc-close-wait-"):
                failures += 1
                raise RuntimeError("injected_close_wait_start_failure")
            native_start(thread)

        monkeypatch.setattr(threading.Thread, "start", fail_close_wait_once)

        first = channel.finish_close(timeout_sec=0.1)

        assert failures == 1
        assert channel._close_wait_thread is None
        assert transport.close_signal_calls == 1
        assert transport.close_wait_calls == 0
        assert first["owner_joined"] is False
        assert first["unresolved"] >= 1

        second = channel.finish_close(timeout_sec=1.0)

        assert transport.close_signal_calls == 1
        assert transport.close_wait_calls == 1
        assert second["signal_sent"] is True
        assert second["unresolved"] == 0
        assert second["owner_joined"] is True
        assert second["process_tree_state"] == "not_applicable"

    @pytest.mark.skipif(os.name != "nt", reason="requires Windows Job Objects")
    def test_transport_timeout_reobserves_same_owner_to_empty_verified(self) -> None:
        transport = SubprocessRpcTransport(
            [
                sys.executable,
                "-u",
                "-c",
                "import sys,time; sys.stdin.buffer.read(); time.sleep(5)",
            ]
        )
        owner = transport._owner
        owner_token = owner.owner_token
        try:
            first = transport.wait_closed(timeout_sec=0.0)
            first_generation = owner._generation
            second = transport.wait_closed(timeout_sec=2.0)

            assert first["process_owners_observed"] == 1
            assert first["process_owners_unresolved"] == 1
            assert first["owner_joined"] is False
            assert second["process_owners_observed"] == 1
            assert second["process_owners_unresolved"] == 0
            assert second["owner_joined"] is True
            assert second["process_tree_state"] == "empty_verified"
            assert transport._owner is owner
            assert owner.owner_token == owner_token
            assert owner._generation == first_generation + 1
            assert transport._signal_sent is True
            assert transport._tree_termination_attempted is True
        finally:
            transport.wait_closed(timeout_sec=2.0)

    @pytest.mark.skipif(os.name != "nt", reason="requires Windows Job Objects")
    def test_empty_tree_with_unreleased_witness_remains_reobservable(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        transport = SubprocessRpcTransport(
            [sys.executable, "-u", "-c", "raise SystemExit(0)"]
        )
        process = transport._proc
        process.wait(timeout=10.0)
        for reader in transport._readers:
            reader.join(timeout=2.0)
        api = process._pulse_job_api
        native_close_handle = api.close_handle
        release_witness = threading.Event()

        def gated_close_handle(handle: int) -> bool:
            if not release_witness.is_set():
                return False
            return native_close_handle(handle)

        monkeypatch.setattr(api, "close_handle", gated_close_handle)
        first = transport.wait_closed(timeout_sec=0.0)
        release_witness.set()
        second = transport.wait_closed(timeout_sec=0.0)

        assert first["process_tree_state"] == "empty_verified"
        assert first["process_owners_unresolved"] == 1
        assert first["owner_joined"] is False
        assert first["error_code"] == "containment_handle_close_unproven"
        assert second["process_tree_state"] == "empty_verified"
        assert second["process_owners_unresolved"] == 0
        assert second["owner_joined"] is True
        assert second["error_code"] is None
        assert process.containment_released is True

    @pytest.mark.skipif(os.name != "nt", reason="requires Windows Job Objects")
    def test_real_contained_rpc_owner_converges_through_all_pi_layers(
        self,
        tmp_path: Path,
    ) -> None:
        session_file = (tmp_path / "contained-pi.jsonl").resolve()
        script = f"""
import json
import pathlib
import sys
import time

session_file = {str(session_file)!r}
session_id = "contained-pi-session"
auto_compaction = True
last_text = ""

def emit(value):
    sys.stdout.write(json.dumps(value) + "\\n")
    sys.stdout.flush()

def response(command, data=None):
    value = {{
        "id": command["id"],
        "type": "response",
        "command": command["type"],
        "success": True,
    }}
    if data is not None:
        value["data"] = data
    emit(value)

for raw in sys.stdin:
    command = json.loads(raw)
    kind = command["type"]
    if kind == "get_state":
        response(command, {{
            "sessionId": session_id,
            "sessionFile": session_file,
            "autoCompactionEnabled": auto_compaction,
            "isStreaming": False,
            "pendingMessageCount": 0,
        }})
    elif kind == "set_auto_compaction":
        auto_compaction = bool(command["enabled"])
        response(command)
    elif kind == "prompt":
        last_text = "contained-answer"
        pathlib.Path(session_file).write_text("{{}}\\n", encoding="utf-8")
        response(command)
        message = {{
            "role": "assistant",
            "timestamp": 1,
            "stopReason": "stop",
            "content": [{{"type": "text", "text": last_text}}],
            "usage": {{
                "input": 1,
                "output": 1,
                "cacheRead": 0,
                "cacheWrite": 0,
                "totalTokens": 2,
                "cost": {{
                    "input": 0,
                    "output": 0,
                    "cacheRead": 0,
                    "cacheWrite": 0,
                    "total": 0,
                }},
            }},
        }}
        emit({{"type": "message_end", "message": message}})
        emit({{"type": "agent_settled"}})
    elif kind == "get_last_assistant_text":
        response(command, {{"text": last_text}})
    else:
        response(command)

time.sleep(5)
"""

        class _ContainedPiFactory:
            def __init__(self) -> None:
                self.spawn_calls = 0
                self.signal_calls = 0
                self.wait_calls = 0
                self.signal_done = threading.Event()
                self.transports: list[SubprocessRpcTransport] = []

            def __call__(self, _argv: list[str]) -> SubprocessRpcTransport:
                self.spawn_calls += 1
                transport = SubprocessRpcTransport(
                    [sys.executable, "-u", "-c", script],
                    cwd=str(tmp_path),
                )
                native_signal = transport.signal_close
                native_wait = transport.wait_closed

                def counted_signal() -> bool:
                    if not transport._signal_sent:
                        self.signal_calls += 1
                    try:
                        return native_signal()
                    finally:
                        self.signal_done.set()

                def counted_wait(
                    timeout_sec: float | None = None,
                ) -> dict:
                    self.wait_calls += 1
                    return native_wait(timeout_sec=timeout_sec)

                transport.signal_close = counted_signal  # type: ignore[method-assign]
                transport.wait_closed = counted_wait  # type: ignore[method-assign]
                self.transports.append(transport)
                return transport

        factory = _ContainedPiFactory()
        runtime = make_runtime(tmp_path, factory)  # type: ignore[arg-type]
        result = runtime.run_turn("engram", "work")
        session = runtime._sessions["engram"]
        session._begin_close(reason="test_timeout")
        assert factory.signal_done.wait(timeout=1.0)

        first = runtime.close(timeout_sec=0.0)
        second = runtime.close(timeout_sec=2.0)

        assert result.content == "contained-answer"
        assert first["sessions_observed"] == 1
        assert first["process_owners_unresolved"] == 1
        assert first["owner_joined"] is False
        assert second["sessions_observed"] == 1
        assert second["process_owners_observed"] == 1
        assert second["process_owners_unresolved"] == 0
        assert second["unresolved"] == 0
        assert second["owner_joined"] is True
        assert second["process_tree_state"] == "empty_verified"
        assert factory.spawn_calls == 1
        assert factory.signal_calls == 1
        assert factory.wait_calls == 2
        assert len(factory.transports) == 1
        assert runtime._physical_sessions == {}

    def test_channel_starts_a_new_wait_generation_for_nonfinal_evidence(
        self,
        tmp_path: Path,
    ) -> None:
        transport = FakePersistentPi(
            tmp_path,
            "wait-generation",
            close_summary_sequence=(
                _transport_close_observation(
                    "unknown",
                    process_observed=1,
                    process_unresolved=1,
                    owner_joined=False,
                    error_code="pi_test_owner_unresolved",
                ),
                _transport_close_observation(
                    "not_applicable",
                    process_observed=0,
                    process_unresolved=0,
                    owner_joined=True,
                    error_code=None,
                ),
            ),
        )
        channel = PiRpcChannel(transport, id_prefix="wait-generation")

        first = channel.finish_close(timeout_sec=1.0)
        second = channel.finish_close(timeout_sec=1.0)
        third = channel.finish_close(timeout_sec=1.0)

        assert first["owner_joined"] is False
        assert first["process_tree_state"] == "unknown"
        assert second["owner_joined"] is True
        assert second["process_tree_state"] == "not_applicable"
        assert second["process_owners_observed"] == 1
        assert third == second
        assert transport.close_signal_calls == 1
        assert transport.close_wait_calls == 2

    def test_channel_recomputes_total_after_nonfinal_tree_forces_unresolved(
        self,
        tmp_path: Path,
    ) -> None:
        transport = FakePersistentPi(
            tmp_path,
            "nonfinal-tree-count",
            close_summary_sequence=(
                _transport_close_observation(
                    "unknown",
                    process_observed=0,
                    process_unresolved=0,
                    owner_joined=False,
                    error_code="pi_test_nonfinal_tree",
                ),
            ),
        )

        summary = PiRpcChannel(
            transport,
            id_prefix="nonfinal-tree-count",
        ).close()

        assert summary["process_owners_unresolved"] == 1
        assert summary["unresolved"] == (
            summary["process_owners_unresolved"]
            + summary["internal_owner_unresolved"]
        )
        assert summary["unresolved"] >= 1
        assert summary["owner_joined"] is False

    @pytest.mark.parametrize(
        "invalid_summary",
        [
            _transport_close_observation(
                "empty_verified",
                process_observed=1,
                process_unresolved=0,
                owner_joined=True,
                error_code=None,
            ),
            {
                **_transport_close_observation(
                    "not_applicable",
                    process_observed=0,
                    process_unresolved=0,
                    owner_joined=True,
                    error_code=None,
                ),
                "process_owners_observed": True,
            },
        ],
        ids=("fake-empty", "non-exact-count"),
    )
    def test_fake_or_nonexact_transport_summary_fails_closed(
        self,
        tmp_path: Path,
        invalid_summary: dict,
    ) -> None:
        transport = FakePersistentPi(
            tmp_path,
            "invalid-close-summary",
            close_summary_sequence=(invalid_summary,),
        )

        summary = PiRpcChannel(transport, id_prefix="invalid-summary").close()

        assert summary["owner_joined"] is False
        assert summary["unresolved"] >= 1
        assert summary["process_tree_state"] == "unknown"
        assert summary["error_code"] == "pi_transport_owner_contract_missing"

    def test_session_and_fleet_refresh_nonfinal_cache_without_resignal_or_spawn(
        self,
        tmp_path: Path,
    ) -> None:
        factory = FakeFactory(
            tmp_path,
            close_summary_sequence=(
                _transport_close_observation(
                    "unknown",
                    process_observed=1,
                    process_unresolved=1,
                    owner_joined=False,
                    error_code="pi_test_owner_unresolved",
                ),
                _transport_close_observation(
                    "not_applicable",
                    process_observed=0,
                    process_unresolved=0,
                    owner_joined=True,
                    error_code=None,
                ),
            ),
        )
        runtime = make_runtime(tmp_path, factory)
        runtime.run_turn("engram", "work")

        first = runtime.close(timeout_sec=1.0)
        second = runtime.close(timeout_sec=1.0)
        third = runtime.close(timeout_sec=1.0)

        assert first["owner_joined"] is False
        assert first["unresolved"] >= 1
        assert second["owner_joined"] is True
        assert second["unresolved"] == 0
        assert second["process_tree_state"] == "not_applicable"
        assert second["process_owners_observed"] == 1
        assert second["process_owners_unresolved"] == 0
        assert third == second
        assert len(factory.transports) == 1
        assert factory.transports[0].close_signal_calls == 1
        assert factory.transports[0].close_wait_calls == 2
        assert runtime._physical_sessions == {}

    def test_close_session_claims_physical_owner_before_fleet_census(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(tmp_path, factory)
        runtime.run_turn("engram", "work")
        session = runtime._sessions["engram"]
        original_close = session._close
        close_entered = threading.Event()
        release_close = threading.Event()

        def blocked_close(*, reason: str, **kwargs) -> dict:
            close_entered.set()
            release_close.wait(timeout=2.0)
            return original_close(reason=reason, **kwargs)

        monkeypatch.setattr(session, "_close", blocked_close)
        closer = threading.Thread(target=lambda: runtime.close_session("engram"))
        closer.start()
        assert close_entered.wait(timeout=1.0)

        summary = runtime.close(timeout_sec=1.0)
        release_close.set()
        closer.join(timeout=2.0)

        assert not closer.is_alive()
        assert summary["sessions_observed"] == 1
        assert summary["active_before"] == 1
        assert factory.transports[0].close_signal_calls == 1
        assert len(factory.transports) == 1

    def test_capacity_eviction_claims_physical_owner_before_fleet_census(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(tmp_path, factory, max_live_sessions=1)
        runtime.run_turn("one", "first")
        victim = runtime._sessions["one"]
        original_close = victim._close
        close_entered = threading.Event()
        release_close = threading.Event()
        outcome: list[BaseException] = []

        def blocked_close(*, reason: str, **kwargs) -> dict:
            close_entered.set()
            release_close.wait(timeout=2.0)
            return original_close(reason=reason, **kwargs)

        def start_successor() -> None:
            try:
                runtime.run_turn("two", "second")
            except BaseException as exc:
                outcome.append(exc)

        monkeypatch.setattr(victim, "_close", blocked_close)
        starter = threading.Thread(target=start_successor)
        starter.start()
        assert close_entered.wait(timeout=1.0)

        summary = runtime.close(timeout_sec=1.0)
        release_close.set()
        starter.join(timeout=2.0)

        assert not starter.is_alive()
        assert outcome
        assert summary["sessions_observed"] == 1
        assert summary["active_before"] == 1
        assert len(factory.transports) == 1
        assert factory.transports[0].close_signal_calls == 1

    def test_retained_close_session_fences_successor_until_reobserve_final(
        self,
        tmp_path: Path,
    ) -> None:
        factory = FakeFactory(
            tmp_path,
            default_resume_id="pi1-fresh",
            close_summary_sequence=(
                _transport_close_observation(
                    "unknown",
                    process_observed=1,
                    process_unresolved=1,
                    owner_joined=False,
                    error_code="pi_test_owner_unresolved",
                ),
                _transport_close_observation(
                    "unknown",
                    process_observed=1,
                    process_unresolved=1,
                    owner_joined=False,
                    error_code="pi_test_owner_unresolved",
                ),
                _transport_close_observation(
                    "not_applicable",
                    process_observed=0,
                    process_unresolved=0,
                    owner_joined=True,
                    error_code=None,
                ),
            ),
        )
        runtime = make_runtime(tmp_path, factory)
        runtime.run_turn("engram", "first")

        runtime.close_session("engram")
        with pytest.raises(HarnessError) as blocked:
            runtime.run_turn("engram", "must-not-spawn")

        assert blocked.value.code == "pi_physical_owner_unresolved"
        assert len(factory.transports) == 1
        assert len(runtime._physical_sessions) == 1
        assert factory.transports[0].close_signal_calls == 1
        assert factory.transports[0].close_wait_calls == 2

        runtime.close_session("engram")
        runtime.run_turn("engram", "after-convergence")

        assert len(factory.transports) == 2
        assert factory.transports[0].close_signal_calls == 1
        assert factory.transports[0].close_wait_calls == 3
        assert runtime._physical_sessions == {}
        runtime.close(timeout_sec=1.0)
        runtime.close(timeout_sec=1.0)

    def test_nonfinal_succession_fences_both_ids_until_explicit_retry(
        self,
        tmp_path: Path,
    ) -> None:
        unresolved = _transport_close_observation(
            "unknown",
            process_observed=1,
            process_unresolved=1,
            owner_joined=False,
            error_code="pi_test_owner_unresolved",
        )
        factory = FakeFactory(
            tmp_path,
            close_summary_sequence=(
                unresolved,
                unresolved,
                unresolved,
                _transport_close_observation(
                    "not_applicable",
                    process_observed=0,
                    process_unresolved=0,
                    owner_joined=True,
                    error_code=None,
                ),
            ),
        )
        runtime = make_runtime(tmp_path, factory)
        predecessor = runtime.run_turn("old", "seed")

        with pytest.raises(HarnessError) as first_close:
            runtime.succeed("old", "new")

        assert first_close.value.code == "pi_physical_owner_unresolved"
        assert runtime._sessions == {}
        assert len(runtime._physical_sessions) == 1
        retained = next(iter(runtime._physical_sessions.values()))
        assert runtime._physical_session_fences == {
            "old": retained,
            "new": retained,
        }
        close_evidence = retained._cached_close_evidence()
        assert close_evidence is not None
        assert close_evidence["lifecycle_owner_unresolved"] == 0
        assert (
            runtime.binding_snapshot()["sessions"]["new"]["state"]
            == "pending_lineage"
        )

        for engram_id in ("old", "new"):
            with pytest.raises(HarnessError) as fenced:
                runtime.run_turn(engram_id, "must-not-spawn", timeout_sec=1.0)
            assert fenced.value.code == "pi_physical_owner_unresolved"
            assert len(factory.transports) == 1

        runtime.succeed("old", "new", capacity_timeout_sec=1.0)
        successor = runtime.run_turn("new", "after-convergence", timeout_sec=1.0)

        predecessor_transport = factory.transports[0]
        assert successor.content == "answer-pi2-1"
        assert len(factory.transports) == 2
        assert predecessor_transport.close_signal_calls == 1
        assert predecessor_transport.close_wait_calls == 4
        assert runtime._physical_sessions == {}
        assert runtime._physical_session_fences == {}
        assert factory.transports[1].parent_sessions == [
            predecessor.session_file
        ]
        runtime.close(timeout_sec=1.0)

    def test_nonfinal_succession_remains_in_fleet_close_census(
        self,
        tmp_path: Path,
    ) -> None:
        unresolved = _transport_close_observation(
            "unknown",
            process_observed=1,
            process_unresolved=1,
            owner_joined=False,
            error_code="pi_test_owner_unresolved",
        )
        factory = FakeFactory(
            tmp_path,
            close_summary_sequence=(
                unresolved,
                unresolved,
                _transport_close_observation(
                    "not_applicable",
                    process_observed=0,
                    process_unresolved=0,
                    owner_joined=True,
                    error_code=None,
                ),
            ),
        )
        runtime = make_runtime(tmp_path, factory)
        runtime.run_turn("old", "seed")

        with pytest.raises(HarnessError):
            runtime.succeed("old", "new")
        first_fleet = runtime.close(timeout_sec=1.0)

        assert first_fleet["sessions_observed"] == 1
        assert first_fleet["active_before"] == 1
        assert first_fleet["process_owners_unresolved"] == 1
        assert first_fleet["owner_joined"] is False
        assert len(runtime._physical_sessions) == 1
        assert len(factory.transports) == 1
        assert factory.transports[0].close_signal_calls == 1
        assert factory.transports[0].close_wait_calls == 2

        final_fleet = runtime.close(timeout_sec=1.0)
        assert final_fleet["sessions_observed"] == 1
        assert final_fleet["owner_joined"] is True
        assert runtime._physical_sessions == {}
        assert runtime._physical_session_fences == {}

    def test_capacity_reobserve_continuity_lock_obeys_absolute_deadline(
        self,
        tmp_path: Path,
    ) -> None:
        contexts_created: list[str] = []

        def context_for(engram_id: str) -> PiProcessContext:
            contexts_created.append(engram_id)
            return PiProcessContext()

        factory = FakeFactory(
            tmp_path,
            close_summary_sequence=(
                _transport_close_observation(
                    "unknown",
                    process_observed=1,
                    process_unresolved=1,
                    owner_joined=False,
                    error_code="pi_test_owner_unresolved",
                ),
                _transport_close_observation(
                    "not_applicable",
                    process_observed=0,
                    process_unresolved=0,
                    owner_joined=True,
                    error_code=None,
                ),
            ),
        )
        runtime = make_runtime(
            tmp_path,
            factory,
            max_live_sessions=1,
            session_context_factory=context_for,
        )
        runtime.run_turn("one", "first")

        with pytest.raises(HarnessError) as retained:
            runtime.run_turn("two", "evict", timeout_sec=1.0)
        assert retained.value.code == "pi_physical_owner_unresolved"

        victim = next(iter(runtime._physical_sessions.values()))
        lock_entered = threading.Event()
        release_lock = threading.Event()

        def hold_continuity_lock() -> None:
            with victim._continuity._lock:
                lock_entered.set()
                release_lock.wait(timeout=1.0)

        holder = threading.Thread(target=hold_continuity_lock)
        holder.start()
        assert lock_entered.wait(timeout=1.0)
        try:
            started = time.monotonic()
            with pytest.raises(HarnessError) as timed_out:
                runtime.run_turn("two", "bounded", timeout_sec=0.05)
            elapsed = time.monotonic() - started

            assert timed_out.value.code == "pi_capacity_timeout"
            assert elapsed < 0.20
            assert len(factory.transports) == 1
            assert contexts_created == ["one"]
            assert runtime._starting == set()
            assert runtime._starting_sessions == {}
            assert factory.transports[0].close_signal_calls == 1
            assert factory.transports[0].close_wait_calls == 2
        finally:
            release_lock.set()
            holder.join(timeout=1.0)
        assert not holder.is_alive()

        result = runtime.run_turn("two", "retry", timeout_sec=1.0)

        assert result.content == "answer-pi2-1"
        assert len(factory.transports) == 2
        assert contexts_created == ["one", "two"]
        assert factory.transports[0].close_signal_calls == 1
        assert factory.transports[0].close_wait_calls == 2
        assert runtime._physical_sessions == {}
        runtime.close(timeout_sec=1.0)

    def test_capacity_demand_reobserves_retained_eviction_and_starts_successor(
        self,
        tmp_path: Path,
    ) -> None:
        factory = FakeFactory(
            tmp_path,
            close_summary_sequence=(
                _transport_close_observation(
                    "unknown",
                    process_observed=1,
                    process_unresolved=1,
                    owner_joined=False,
                    error_code="pi_test_owner_unresolved",
                ),
                _transport_close_observation(
                    "not_applicable",
                    process_observed=0,
                    process_unresolved=0,
                    owner_joined=True,
                    error_code=None,
                ),
            ),
        )
        runtime = make_runtime(tmp_path, factory, max_live_sessions=1)
        runtime.run_turn("one", "first")

        with pytest.raises(HarnessError) as blocked:
            runtime.run_turn("two", "first-attempt", timeout_sec=1.0)

        victim_transport = factory.transports[0]
        assert blocked.value.code == "pi_physical_owner_unresolved"
        assert len(factory.transports) == 1
        assert len(runtime._physical_sessions) == 1
        assert victim_transport.close_signal_calls == 1
        assert victim_transport.close_wait_calls == 1

        result = runtime.run_turn("two", "second-attempt", timeout_sec=1.0)

        assert result.content == "answer-pi2-1"
        assert len(factory.transports) == 2
        assert victim_transport.close_signal_calls == 1
        assert victim_transport.close_wait_calls == 2
        assert runtime._physical_sessions == {}
        assert set(runtime._sessions) == {"two"}
        runtime.close(timeout_sec=1.0)
        runtime.close(timeout_sec=1.0)

    def test_runtime_broadcasts_every_close_signal_before_any_transport_wait(
        self,
        tmp_path: Path,
    ) -> None:
        order: list[str] = []
        factory = FakeFactory(tmp_path, close_order=order)
        runtime = make_runtime(tmp_path, factory)
        runtime.run_turn("one", "a")
        runtime.run_turn("two", "b")

        summary = runtime.close()

        first_wait = next(index for index, item in enumerate(order) if item.startswith("wait:"))
        assert {item for item in order[:first_wait]} == {
            "signal:pi1",
            "signal:pi2",
        }
        assert summary["sessions_observed"] == 2
        assert summary["signals_dispatched"] == 2
        assert summary["signals_sent"] == 2
        assert summary["unresolved"] == 0
        assert summary["owner_joined"] is True
        assert summary["process_tree_state"] == "not_applicable"

    def test_continuity_lock_cannot_block_fleet_close_signal_broadcast(
        self,
        tmp_path: Path,
    ) -> None:
        order: list[str] = []
        factory = FakeFactory(tmp_path, close_order=order)
        runtime = make_runtime(tmp_path, factory)
        runtime.run_turn("one", "a")
        runtime.run_turn("two", "b")
        first_session = runtime._sessions["one"]
        lock_entered = threading.Event()
        release_lock = threading.Event()

        def hold_continuity_lock() -> None:
            with first_session._continuity._lock:
                lock_entered.set()
                release_lock.wait(timeout=2.0)

        holder = threading.Thread(target=hold_continuity_lock)
        holder.start()
        assert lock_entered.wait(timeout=1.0)
        summaries: list[dict] = []
        closer = threading.Thread(target=lambda: summaries.append(runtime.close()))
        closer.start()
        deadline = threading.Event()
        for _ in range(100):
            if {item for item in order if item.startswith("signal:")} == {
                "signal:pi1",
                "signal:pi2",
            }:
                break
            deadline.wait(0.01)

        assert {item for item in order if item.startswith("signal:")} == {
            "signal:pi1",
            "signal:pi2",
        }
        release_lock.set()
        holder.join(timeout=2.0)
        closer.join(timeout=2.0)
        assert not holder.is_alive()
        assert not closer.is_alive()
        assert summaries[0]["signals_sent"] == 2

    def test_runtime_close_captures_a_resident_session_still_starting(
        self,
        tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path, hang_startup_state=True)
        runtime = make_runtime(
            tmp_path,
            factory,
            handshake_timeout_sec=5.0,
        )
        outcome: list[BaseException] = []

        def start_turn() -> None:
            try:
                runtime.run_turn("starting", "work")
            except BaseException as exc:
                outcome.append(exc)

        worker = threading.Thread(target=start_turn)
        worker.start()
        while not factory.transports:
            worker.join(timeout=0.01)
        assert factory.transports[0].startup_state_seen.wait(timeout=1.0)

        summary = runtime.close(timeout_sec=1.0)
        worker.join(timeout=2.0)

        assert summary["sessions_observed"] == 1
        assert summary["signals_dispatched"] == 1
        assert factory.transports[0].closed is True
        assert not worker.is_alive()
        assert outcome

    def test_typed_summary_preserves_process_reader_and_internal_unresolved(
        self,
        tmp_path: Path,
    ) -> None:
        factory = FakeFactory(
            tmp_path,
            close_process_observed=1,
            close_process_unresolved=1,
            close_reader_unresolved=2,
        )
        runtime = make_runtime(tmp_path, factory)
        runtime.run_turn("engram", "work")

        summary = runtime.close()

        assert summary["process_owners_observed"] == 1
        assert summary["process_owners_unresolved"] == 1
        assert summary["transport_reader_owners_observed"] == 2
        assert summary["transport_reader_owners_unresolved"] == 2
        assert summary["internal_owner_unresolved"] >= 3
        assert summary["unresolved"] >= 4
        assert summary["owner_joined"] is False
        assert summary["process_tree_state"] == "unknown"
        assert "pi_test_owner_unresolved" in summary["error_codes"]

    def test_legacy_transport_close_never_defaults_to_joined(self) -> None:
        transport = LegacyCloseOnlyTransport()
        channel = PiRpcChannel(transport, id_prefix="legacy-close")

        summary = channel.close()

        assert transport.closed is True
        assert summary["owner_joined"] is False
        assert summary["internal_owner_unresolved"] >= 1
        assert summary["error_code"] == "pi_transport_owner_contract_missing"
        assert transport.diagnostics_calls == 0

    def test_late_jsonl_tail_is_quarantined_from_next_canonical_resume(
        self,
        tmp_path: Path,
    ) -> None:
        first_factory = FakeFactory(tmp_path)
        first_runtime = make_runtime(tmp_path, first_factory)
        first = first_runtime.run_turn("engram", "before")
        persisted = first_runtime.binding_snapshot()
        first_summary = first_runtime.close()
        assert first_summary["owner_joined"] is True

        old_path = Path(first.session_file)
        committed_prefix = old_path.read_bytes()
        late_tail = b'{"late":true}\n'
        with old_path.open("ab") as stream:
            stream.write(late_tail)

        second_factory = FakeFactory(
            tmp_path,
            default_resume_id=first.session_id,
        )
        second_runtime = make_runtime(
            tmp_path,
            second_factory,
            binding_state=persisted,
        )
        resumed = second_runtime.run_turn("engram", "after")

        fake = second_factory.transports[0]
        switch = next(
            command for command in fake.sent if command["type"] == "switch_session"
        )
        canonical_path = Path(switch["sessionPath"])
        assert canonical_path != old_path
        assert fake.switched_payloads == [committed_prefix]
        assert late_tail not in fake.switched_payloads[0]
        assert old_path.read_bytes() == committed_prefix + late_tail
        assert resumed.session_file == str(canonical_path.resolve())
        assert (
            second_runtime.binding_snapshot()["sessions"]["engram"]["session_file"]
            == str(canonical_path.resolve())
        )
        quarantine_records = list(
            (tmp_path / ".pulse" / "harness" / "pi" / "continuity" / "quarantine").glob("*.json")
        )
        assert len(quarantine_records) == 1
        quarantine = json.loads(quarantine_records[0].read_text(encoding="utf-8"))
        assert quarantine["quarantined_tail_bytes"] == len(late_tail)
        assert quarantine["canonical_session_file"] == str(canonical_path.resolve())
        second_runtime.close()

    def test_revoke_between_terminal_projection_and_watermark_fails_closed(
        self,
        tmp_path: Path,
    ) -> None:
        runtime_box: dict[str, PiHarnessRuntime] = {}
        gate = RuntimePublicationGate("pi-test-terminal-revoke", 1)

        def close_on_terminal(
            _engram_id: str,
            _turn_id: str | None,
            event: dict,
        ) -> None:
            if event.get("type") == "turn_terminal":
                recovery_permit = gate.revoke(reason="test_terminal")
                runtime_box["runtime"].close(
                    timeout_sec=0.5,
                    recovery_permit=recovery_permit,
                )

        first_factory = FakeFactory(tmp_path)
        first_runtime = make_runtime(
            tmp_path,
            first_factory,
            publication_gate=gate,
            event_callback=close_on_terminal,
        )
        runtime_box["runtime"] = first_runtime

        with pytest.raises(HarnessError) as caught:
            first_runtime.run_turn("engram", "accepted")
        assert caught.value.code == "pi_continuity_revoked"
        assert caught.value.prompt_accepted is True
        persisted = first_runtime.binding_snapshot()

        second_factory = FakeFactory(tmp_path)
        second_runtime = make_runtime(
            tmp_path,
            second_factory,
            binding_state=persisted,
        )
        with pytest.raises(HarnessError) as resume_error:
            second_runtime.run_turn("engram", "must-not-adopt")
        assert resume_error.value.code == "pi_continuity_uncommitted"
        assert second_factory.transports == []
        second_runtime.close()

    def test_late_predecessor_tail_is_quarantined_from_successor_lineage(
        self,
        tmp_path: Path,
    ) -> None:
        factory = FakeFactory(tmp_path)
        runtime = make_runtime(tmp_path, factory)
        predecessor = runtime.run_turn("old", "before")
        runtime.succeed("old", "new")

        old_path = Path(predecessor.session_file)
        committed_prefix = old_path.read_bytes()
        late_tail = b'{"late-lineage":true}\n'
        with old_path.open("ab") as stream:
            stream.write(late_tail)

        runtime.run_turn("new", "after")

        successor_transport = factory.transports[1]
        [safe_parent] = successor_transport.parent_sessions
        assert safe_parent is not None
        assert Path(safe_parent) != old_path
        assert successor_transport.parent_payloads == [committed_prefix]
        assert late_tail not in successor_transport.parent_payloads[0]
        assert old_path.read_bytes() == committed_prefix + late_tail
        materialized = runtime.binding_snapshot()["sessions"]["new"]
        assert materialized["parent_session_file"] == safe_parent
        runtime.close()

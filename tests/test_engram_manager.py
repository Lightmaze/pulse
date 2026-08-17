"""Tests for Engram management."""

from types import SimpleNamespace

import pytest

from pulse_system.agent.harness.base import HarnessError, HarnessTurnResult
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.engram import EngramManager
from pulse_system.core.types import (
    EngramStatus,
    Message,
    MessageRole,
)
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


class _RecordingHarness:
    def __init__(self, tmp_path, outputs=None, *, fail_succeed=False):
        self._tmp_path = tmp_path
        self._outputs = list(outputs or [])
        self._bootstrapped: dict[str, bool] = {}
        self.calls: list[dict] = []
        self.rotations: list[tuple[str, str]] = []
        self.fail_succeed = fail_succeed
        self.on_succeed = None

    def snapshot(self, engram_id):
        if engram_id not in self._bootstrapped:
            raise HarnessError(
                "pi_session_unknown",
                "no binding",
                "run a turn",
                phase="snapshot",
            )
        return {"bootstrapped": self._bootstrapped[engram_id]}

    def run_turn(
        self,
        engram_id,
        prompt,
        *,
        timeout_sec=None,
        bootstrap_text=None,
    ):
        self.calls.append({
            "engram_id": engram_id,
            "prompt": prompt,
            "timeout_sec": timeout_sec,
            "bootstrap_text": bootstrap_text,
        })
        self._bootstrapped[engram_id] = True
        content = (
            self._outputs.pop(0)
            if self._outputs
            else f"final text {len(self.calls)}"
        )
        return HarnessTurnResult(
            engram_id=engram_id,
            session_id=f"session-{engram_id}",
            session_file=str(self._tmp_path / f"{engram_id}.jsonl"),
            content=content,
            stop_reason="stop",
            input_tokens=11,
            output_tokens=7,
            cached_tokens=3,
            cache_write_tokens=2,
            tool_calls=4,
            trace=[{"type": "tool_result", "content": "TRACE-ONLY"}],
        )

    def succeed(self, old_engram_id, new_engram_id):
        self.rotations.append((old_engram_id, new_engram_id))
        if self.on_succeed is not None:
            self.on_succeed(old_engram_id, new_engram_id)
        if self.fail_succeed:
            raise HarnessError(
                "pi_succession_failed",
                "rotation refused",
                "retry after repair",
                phase="succession",
            )
        self._bootstrapped[new_engram_id] = False


@pytest.fixture
def deps():
    store = Storage(":memory:")
    llm = LLMAdapter(mock=True)
    conn_net = ConnectionNetwork(store, ConnectionConfig())
    yield store, llm, conn_net
    store.close()


@pytest.fixture
def mgr(deps):
    store, llm, conn_net = deps
    return EngramManager(store, llm, conn_net)


@pytest.fixture
def store(deps):
    return deps[0]


# ── create ───────────────────────────────────────────────────────


class TestCreate:
    def test_create_basic(self, mgr: EngramManager):
        e = mgr.create()
        assert e.status == EngramStatus.ACTIVE
        assert len(e.id) > 0

    def test_create_with_project(self, mgr: EngramManager):
        e = mgr.create(project_id="proj1")
        assert e.project_id == "proj1"

    def test_create_with_initial_messages(self, mgr: EngramManager):
        msgs = [
            Message(role=MessageRole.USER, content="hello"),
            Message(role=MessageRole.ASSISTANT, content="hi"),
        ]
        e = mgr.create(initial_messages=msgs)
        session = mgr.get_session(e.id)
        assert len(session) == 2
        assert session[0].content == "hello"

    def test_create_sets_token_count(self, mgr: EngramManager, store: Storage):
        msgs = [Message(role=MessageRole.USER, content="a" * 300)]
        e = mgr.create(initial_messages=msgs)
        fetched = store.get_engram(e.id)
        assert fetched.metadata.token_count > 0


# ── pulse ────────────────────────────────────────────────────────


class TestPulse:
    def test_basic_pulse(self, mgr: EngramManager):
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="Tell me about the ocean"),
        ])
        result = mgr.pulse(e.id)
        assert len(result.content) > 0
        assert "[mock response" in result.content
        assert result.input_tokens > 0
        assert result.output_tokens > 0
        assert result.cached_tokens >= 0

    def test_pulse_appends_to_session(self, mgr: EngramManager):
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="hello"),
        ])
        mgr.pulse(e.id)
        session = mgr.get_session(e.id)
        assert len(session) == 2
        assert session[1].role == MessageRole.ASSISTANT

    def test_pulse_with_injection(self, mgr: EngramManager):
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="thinking about cats"),
        ])
        mgr.pulse(e.id, injected_context="cats are wonderful", source_engram_id="other")
        session = mgr.get_session(e.id)
        # original + injection + LLM response
        assert len(session) == 3
        assert session[1].role == MessageRole.INJECTION
        assert session[1].content == "cats are wonderful"
        assert session[1].source_engram_id == "other"
        assert session[2].role == MessageRole.ASSISTANT

    def test_pulse_updates_metadata(self, mgr: EngramManager, store: Storage):
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="hi"),
        ])
        mgr.pulse(e.id)
        fetched = store.get_engram(e.id)
        assert fetched.total_pulses == 1
        assert fetched.last_pulse_at is not None
        assert fetched.metadata.recent_activity > 0
        assert fetched.metadata.token_count > 0

    def test_token_count_is_context_size_not_cumulative(
        self, mgr: EngramManager, store: Storage
    ):
        """token_count is SET to the current context size each pulse.

        Cumulative accounting would grow quadratically; context size grows
        roughly linearly with the number of messages.
        """
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="x" * 300),
        ])
        counts = []
        for _ in range(4):
            result = mgr.pulse(e.id)
            counts.append(store.get_engram(e.id).metadata.token_count)
            # SET semantics: token_count equals this call's input+output
            assert counts[-1] == result.input_tokens + result.output_tokens
        # growth between consecutive pulses stays bounded (one assistant
        # message per pulse), instead of doubling as with accumulation
        deltas = [b - a for a, b in zip(counts, counts[1:])]
        assert all(d < counts[0] for d in deltas)

    def test_pulse_increments_count(self, mgr: EngramManager, store: Storage):
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="hi"),
        ])
        mgr.pulse(e.id)
        mgr.pulse(e.id)
        mgr.pulse(e.id)
        fetched = store.get_engram(e.id)
        assert fetched.total_pulses == 3

    def test_pulse_nonexistent_raises(self, mgr: EngramManager):
        with pytest.raises(ValueError, match="not found"):
            mgr.pulse("nonexistent")

    def test_pulse_archived_raises(self, mgr: EngramManager, store: Storage):
        e = mgr.create()
        store.archive_engram(e.id)
        with pytest.raises(ValueError, match="archived"):
            mgr.pulse(e.id)

    def test_pulse_no_system_prompt(self, mgr: EngramManager, deps):
        """Verify the free-context rule: no system prompt or structured instructions."""
        _, llm, _ = deps
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="hello"),
        ])
        # Monkey-patch to capture messages sent to LLM
        captured = []
        original = llm.complete
        def spy(messages, **kwargs):
            captured.append(messages)
            return original(messages, **kwargs)
        llm.complete = spy

        mgr.pulse(e.id)

        assert len(captured) == 1
        sent = captured[0]
        # No system role message
        roles = [m["role"] for m in sent]
        assert "system" not in roles
        # First message is the raw user content
        assert sent[0]["content"] == "hello"

    def test_injection_mapped_to_user_role(self, mgr: EngramManager, deps):
        """Injection messages should be sent as 'user' role to LLM."""
        _, llm, _ = deps
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="hello"),
        ])
        mgr.append_injection(e.id, "some context", "src1")

        captured = []
        original = llm.complete
        def spy(messages, **kwargs):
            captured.append(messages)
            return original(messages, **kwargs)
        llm.complete = spy

        mgr.pulse(e.id)
        sent = captured[0]
        # injection should be mapped to user role
        assert sent[1]["role"] == "user"
        assert sent[1]["content"] == "some context"

    def test_harness_bootstraps_once_then_sends_only_incremental_text(
        self, deps, tmp_path
    ):
        store, llm, conn_net = deps
        harness = _RecordingHarness(tmp_path, outputs=["first final", "second final"])
        manager = EngramManager(store, llm, conn_net, harness=harness)

        def adapter_must_not_run(*args, **kwargs):
            pytest.fail("LLMAdapter.complete must not run when Harness exists")

        llm.complete = adapter_must_not_run
        engram = manager.create(initial_messages=[
            Message(role=MessageRole.USER, content="one-time seed"),
        ])
        manager.append_injection(
            engram.id,
            "fresh natural text",
            "source-sideband-id",
        )

        first = manager.pulse(
            engram.id,
            runtime_config=SimpleNamespace(deadline_sec=12.5),
        )
        manager.append_injection(engram.id, "next turn only", "another-source")
        second = manager.pulse(engram.id)

        assert first.content == "first final"
        assert second.content == "second final"
        assert harness.calls[0] == {
            "engram_id": engram.id,
            "prompt": "fresh natural text",
            "timeout_sec": 12.5,
            "bootstrap_text": "one-time seed",
        }
        assert harness.calls[1] == {
            "engram_id": engram.id,
            "prompt": "next turn only",
            "timeout_sec": None,
            "bootstrap_text": None,
        }
        assert "source-sideband-id" not in harness.calls[0]["prompt"]
        assert "first final" not in harness.calls[1]["prompt"]

    def test_harness_spontaneous_turn_uses_exact_empty_prompt(
        self, deps, tmp_path
    ):
        store, llm, conn_net = deps
        harness = _RecordingHarness(tmp_path)
        manager = EngramManager(store, llm, conn_net, harness=harness)
        engram = manager.create(initial_messages=[
            Message(role=MessageRole.ASSISTANT, content="bootstrap summary"),
        ])

        manager.pulse(engram.id)

        assert harness.calls[0]["prompt"] == ""
        assert harness.calls[0]["bootstrap_text"] == "bootstrap summary"

    def test_harness_result_maps_usage_metadata_and_final_projection_only(
        self, deps, tmp_path
    ):
        store, llm, conn_net = deps
        harness = _RecordingHarness(tmp_path, outputs=["settled final"])
        manager = EngramManager(store, llm, conn_net, harness=harness)
        engram = manager.create()

        result = manager.pulse(engram.id)

        assert result.content == "settled final"
        assert (
            result.input_tokens,
            result.output_tokens,
            result.cached_tokens,
            result.cache_write_tokens,
            result.tool_calls,
        ) == (11, 7, 3, 2, 4)
        current = store.get_engram(engram.id)
        assert current.total_pulses == 1
        assert current.last_pulse_at is not None
        assert current.metadata.recent_activity == pytest.approx(0.2)
        assert current.metadata.token_count == 18
        session = manager.get_session(engram.id)
        assert [(message.role, message.content) for message in session] == [
            (MessageRole.ASSISTANT, "settled final"),
        ]
        assert all("TRACE-ONLY" not in message.content for message in session)


# ── append_injection ─────────────────────────────────────────────


class TestAppendInjection:
    def test_basic_injection(self, mgr: EngramManager):
        e = mgr.create()
        mgr.append_injection(e.id, "context from neighbor", "neighbor_id")
        session = mgr.get_session(e.id)
        assert len(session) == 1
        assert session[0].role == MessageRole.INJECTION
        assert session[0].source_engram_id == "neighbor_id"

    def test_multiple_injections(self, mgr: EngramManager):
        e = mgr.create()
        mgr.append_injection(e.id, "from A", "a")
        mgr.append_injection(e.id, "from B", "b")
        session = mgr.get_session(e.id)
        assert len(session) == 2
        assert session[0].source_engram_id == "a"
        assert session[1].source_engram_id == "b"


# ── succession ───────────────────────────────────────────────────


class TestSuccession:
    def test_basic_succession(self, mgr: EngramManager, store: Storage):
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="lots of deep thoughts"),
            Message(role=MessageRole.ASSISTANT, content="indeed very deep"),
        ])
        new_id = mgr.succession(e.id).new_id

        # Old engram should be archived
        old = store.get_engram(e.id)
        assert old.status == EngramStatus.ARCHIVED

        # New engram should exist and be active
        new = store.get_engram(new_id)
        assert new is not None
        assert new.status == EngramStatus.ACTIVE

        # New engram should have the summary as initial message
        new_session = mgr.get_session(new_id)
        assert len(new_session) == 1
        assert new_session[0].role == MessageRole.ASSISTANT
        assert "[mock response" in new_session[0].content

    def test_succession_inherits_project(self, mgr: EngramManager, store: Storage):
        e = mgr.create(
            project_id="proj1",
            initial_messages=[
                Message(role=MessageRole.USER, content="project work"),
            ],
        )
        new_id = mgr.succession(e.id).new_id
        new = store.get_engram(new_id)
        assert new.project_id == "proj1"

    def test_succession_transfers_connections(self, mgr: EngramManager, store: Storage):
        e1 = mgr.create()
        e2 = mgr.create()
        e3 = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="connected engram"),
        ])

        # Create connections: e3 → e1, e2 → e3
        store.create_connection(e3.id, e1.id, 0.5)
        store.create_connection(e2.id, e3.id, 0.7)

        new_id = mgr.succession(e3.id).new_id

        # Old connections should be gone
        assert store.get_connection(e3.id, e1.id) is None
        assert store.get_connection(e2.id, e3.id) is None

        # New engram should have the connections
        assert store.get_connection(new_id, e1.id) is not None
        assert store.get_connection(new_id, e1.id).weight == pytest.approx(0.5)
        assert store.get_connection(e2.id, new_id) is not None

    def test_succession_nonexistent_raises(self, mgr: EngramManager):
        with pytest.raises(ValueError, match="not found"):
            mgr.succession("nonexistent")

    def test_succession_archived_raises(self, mgr: EngramManager, store: Storage):
        e = mgr.create()
        store.archive_engram(e.id)
        with pytest.raises(ValueError, match="archived"):
            mgr.succession(e.id)

    def test_harness_succession_bootstraps_successor_from_summary_once(
        self, deps, tmp_path
    ):
        store, llm, conn_net = deps
        harness = _RecordingHarness(
            tmp_path,
            outputs=["lineage summary", "successor final"],
        )
        manager = EngramManager(store, llm, conn_net, harness=harness)

        def adapter_must_not_run(*args, **kwargs):
            pytest.fail("LLMAdapter.complete must not run during Harness succession")

        llm.complete = adapter_must_not_run
        old = manager.create(initial_messages=[
            Message(role=MessageRole.USER, content="predecessor history"),
        ])
        harness.on_succeed = lambda old_id, new_id: (
            old_id == old.id
            and store.get_engram(old_id).status == EngramStatus.ACTIVE
        ) or pytest.fail("predecessor was archived before Harness rotation")

        succession = manager.succession(old.id)
        manager.pulse(succession.new_id)

        assert store.get_engram(old.id).status == EngramStatus.ARCHIVED
        assert harness.rotations == [(old.id, succession.new_id)]
        assert harness.calls[0]["bootstrap_text"] == "predecessor history"
        assert "comprehensive summary" in harness.calls[0]["prompt"]
        assert harness.calls[1]["engram_id"] == succession.new_id
        assert harness.calls[1]["prompt"] == ""
        assert harness.calls[1]["bootstrap_text"] == "lineage summary"
        assert (
            succession.input_tokens,
            succession.output_tokens,
            succession.cached_tokens,
            succession.cache_write_tokens,
        ) == (11, 7, 3, 2)

    def test_harness_prepare_hides_candidate_and_defers_world_mutation(
        self,
        deps,
        tmp_path,
    ):
        store, llm, conn_net = deps
        harness = _RecordingHarness(tmp_path, outputs=["lineage summary"])
        manager = EngramManager(store, llm, conn_net, harness=harness)
        listener_calls: list[tuple[str, str]] = []
        manager.add_succession_listener(
            lambda old_id, new_id: listener_calls.append((old_id, new_id))
        )
        old = manager.create(
            initial_messages=[
                Message(role=MessageRole.USER, content="predecessor history"),
            ]
        )

        preparation = manager.prepare_succession(old.id)

        assert harness.rotations == [(old.id, preparation.successor_id)]
        assert store.get_engram(old.id).status is EngramStatus.ACTIVE
        assert (
            store.get_engram(preparation.successor_id).status
            is EngramStatus.PROVISIONAL
        )
        assert listener_calls == []

        result = manager.commit_succession(preparation)

        assert result.new_id == preparation.successor_id
        assert store.get_engram(old.id).status is EngramStatus.ARCHIVED
        assert store.get_engram(result.new_id).status is EngramStatus.ACTIVE
        assert listener_calls == [(old.id, result.new_id)]


# ── import_conversation ──────────────────────────────────────────


class TestImportConversation:
    def test_basic_import(self, mgr: EngramManager):
        msgs = [
            Message(role=MessageRole.USER, content="imported question"),
            Message(role=MessageRole.ASSISTANT, content="imported answer"),
        ]
        e = mgr.import_conversation(msgs)
        assert e.status == EngramStatus.ACTIVE

        session = mgr.get_session(e.id)
        assert len(session) == 2
        assert session[0].content == "imported question"

    def test_import_with_project(self, mgr: EngramManager):
        msgs = [Message(role=MessageRole.USER, content="hello")]
        e = mgr.import_conversation(msgs, project_id="proj1")
        assert e.project_id == "proj1"

    def test_import_sets_token_count(self, mgr: EngramManager, store: Storage):
        msgs = [Message(role=MessageRole.USER, content="a" * 600)]
        e = mgr.import_conversation(msgs)
        fetched = store.get_engram(e.id)
        assert fetched.metadata.token_count > 0


# ── Archive listeners release the fork MLP slot ───────────────────


class TestArchiveListeners:
    def test_archive_fires_listener_and_releases_slot(self, mgr, store):
        from pulse_system.core.delegation import DelegationRouter

        router = DelegationRouter(store)
        mgr.add_archive_listener(router.mask_engram)

        e = mgr.create()
        router.register_engram(e.id)
        slot = router._slots.slot_of(e.id, create=False)
        assert slot is not None

        assert mgr.archive(e.id) is True
        # slot released → reusable by a fresh engram
        assert router._slots.slot_of(e.id, create=False) is None
        e2 = mgr.create()
        router.register_engram(e2.id)
        assert router._slots.slot_of(e2.id, create=False) == slot

    def test_archive_of_missing_engram_skips_listeners(self, mgr):
        fired = []
        mgr.add_archive_listener(lambda eid: fired.append(eid))
        assert mgr.archive("nonexistent") is False
        assert fired == []

    def test_succession_reassigns_slot_not_masked(self, mgr, store):
        """Succession must inherit the slot (reassign), never release it —
        even with an archive listener attached alongside the succession one."""
        from pulse_system.core.delegation import DelegationRouter

        router = DelegationRouter(store)
        mgr.add_succession_listener(router.reassign_engram)
        mgr.add_archive_listener(router.mask_engram)

        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="carry me forward"),
        ])
        router.register_engram(e.id)
        slot = router._slots.slot_of(e.id, create=False)

        new_id = mgr.succession(e.id).new_id
        # successor holds the same slot; predecessor no longer maps anywhere
        assert router._slots.slot_of(new_id, create=False) == slot
        assert router._slots.slot_of(e.id, create=False) is None

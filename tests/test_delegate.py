"""Tests for the delegation tunnel."""

import pytest

from pulse_system.agent.delegate import Delegator, DelegatorConfig
from pulse_system.agent.front import FrontAgent, FrontAgentConfig
from pulse_system.agent.tools import ToolRegistry
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.engram import EngramManager
from pulse_system.core.runtime.publication import (
    RuntimePublicationPermit,
    RuntimePublicationGate,
)
from pulse_system.core.types import EngramStatus, Message, MessageRole
from pulse_system.education.library import Library
from pulse_system.interaction.metrics import MetricsRecorder
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


def _publication_permit(owner_id: str) -> RuntimePublicationPermit:
    return RuntimePublicationGate(owner_id, 1).publication_permit


@pytest.fixture
def stack(tmp_path):
    store = Storage(":memory:")
    llm = LLMAdapter(mock=True)
    conn_net = ConnectionNetwork(store, ConnectionConfig())
    mgr = EngramManager(store, llm, conn_net)
    tools = ToolRegistry(mock=True, workspace_root=tmp_path)
    library = Library(
        tmp_path / "library",
        publication_authority=_publication_permit(
            "test:delegate-library"
        ),
    )
    metrics = MetricsRecorder()
    delegator = Delegator(
        store, mgr, tools, library=library, metrics=metrics,
        config=DelegatorConfig(max_think_iterations=2),
    )
    yield store, mgr, tools, library, metrics, delegator
    store.close()


def _make(mgr, content="existing engram"):
    return mgr.create(initial_messages=[
        Message(role=MessageRole.USER, content=content),
    ])


# ── Mainline mode ────────────────────────────────────────────────


class TestMainline:
    def test_creates_new_engram_and_returns_result(self, stack):
        store, mgr, _, _, _, delegator = stack
        result = delegator.delegate("front", "research pulse computing")

        assert result.mode == "mainline"
        assert result.content  # think loop produced output
        target = store.get_engram(result.target_id)
        assert target is not None and target.status == EngramStatus.ACTIVE
        session = mgr.get_session(result.target_id)
        assert "research pulse computing" in session[0].content

    def test_existing_target_gains_permanent_experience(self, stack):
        store, mgr, _, _, _, delegator = stack
        e = _make(mgr)
        before = len(mgr.get_session(e.id))

        delegator.delegate("front", "do a follow-up analysis", target_id=e.id)

        session = mgr.get_session(e.id)
        assert len(session) > before  # task + execution appended to mainline
        injected = [m for m in session if m.role == MessageRole.INJECTION]
        assert any("follow-up analysis" in m.content for m in injected)
        assert injected[0].source_engram_id == "delegation:front"

    def test_record_persisted_and_completed(self, stack):
        store, _, _, _, _, delegator = stack
        result = delegator.delegate("front", "task with record")

        record = store.get_delegation(result.record_id)
        assert record["caller_id"] == "front"
        assert record["target_id"] == result.target_id
        assert record["mode"] == "mainline"
        assert record["result_summary"]
        assert record["completed_at"] is not None
        assert record["task_embedding"] is not None  # mock embed available

    def test_contract_framed_into_task(self, stack):
        _, mgr, _, _, _, delegator = stack
        result = delegator.delegate(
            "front", "write a summary", contract="返回三条要点",
        )
        session = mgr.get_session(result.target_id)
        assert "返回三条要点" in session[0].content

    def test_missing_target_raises(self, stack):
        *_, delegator = stack
        with pytest.raises(ValueError, match="not found"):
            delegator.delegate("front", "task", target_id="nonexistent")


# ── Snapshot mode ────────────────────────────────────────────────


class TestSnapshot:
    def test_mainline_untouched_and_summary_returned(self, stack):
        store, mgr, _, _, _, delegator = stack
        e = _make(mgr, "target with history")
        before = [m.content for m in mgr.get_session(e.id)]

        result = delegator.delegate(
            "caller_engram", "borrow this expertise", target_id=e.id,
            mode="snapshot",
        )

        after = [m.content for m in mgr.get_session(e.id)]
        assert after == before          # target mainline never notices
        assert result.content           # compressed diff came back
        assert result.target_id == e.id

    def test_snapshot_archived_after_run(self, stack):
        store, mgr, _, _, _, delegator = stack
        e = _make(mgr)
        delegator.delegate("c", "task", target_id=e.id, mode="snapshot")

        active = store.list_engrams(status=EngramStatus.ACTIVE)
        assert {x.id for x in active} == {e.id}  # fork did not leak

    def test_diff_summary_written_to_library_diary(self, stack):
        _, mgr, _, library, _, delegator = stack
        e = _make(mgr)
        delegator.delegate("caller_x", "snapshot task", target_id=e.id,
                           mode="snapshot")
        diary = library.read_diary(e.id)
        assert "snapshot task" in diary
        assert "delegation:caller_x" in diary

    def test_snapshot_requires_target(self, stack):
        *_, delegator = stack
        with pytest.raises(ValueError, match="requires an existing target"):
            delegator.delegate("c", "task", mode="snapshot")


# ── Outcome & guards ─────────────────────────────────────────────


class TestOutcomeAndGuards:
    def test_record_outcome(self, stack):
        store, _, _, _, _, delegator = stack
        result = delegator.delegate("front", "judged task")
        assert delegator.record_outcome(result.record_id, "adopted")
        assert store.get_delegation(result.record_id)["outcome"] == "adopted"

    def test_invalid_outcome_rejected(self, stack):
        *_, delegator = stack
        result = delegator.delegate("front", "t")
        with pytest.raises(ValueError):
            delegator.record_outcome(result.record_id, "great")

    def test_depth_limit_blocks_recursion(self, stack):
        store, mgr, tools, library, metrics, delegator = stack
        # register the delegate tool so delegated engrams could recurse
        tools.register("delegate", "delegate a task", delegator.as_tool("front"))

        # simulate being already at max depth
        delegator._depth.value = delegator._config.max_depth
        with pytest.raises(RuntimeError, match="depth limit"):
            delegator.delegate("front", "too deep")
        delegator._depth.value = 0

    def test_metrics_event_emitted(self, stack):
        *_, metrics, delegator = stack
        delegator.delegate("front", "measured task")
        [ev] = metrics.events("delegation")
        assert ev["caller"] == "front"
        assert ev["mode"] == "mainline"


# ── Front-stage tool integration ────────────────────────────────────────────────


class TestDelegateTool:
    def test_tool_plain_task_creates_engram(self, stack):
        store, _, _, _, _, delegator = stack
        tool = delegator.as_tool("front-engram")
        result = tool(task="build a report")
        assert result.success
        assert "delegation" in result.content
        assert len(store.list_delegations("front-engram")) == 1

    def test_tool_at_syntax_targets_existing(self, stack):
        store, mgr, _, _, _, delegator = stack
        e = _make(mgr)
        tool = delegator.as_tool("front")
        result = tool(task=f"@{e.id} continue the analysis")
        assert result.success
        [record] = store.list_delegations("front")
        assert record["target_id"] == e.id

    def test_front_agent_pattern_triggers_delegate(self, stack):
        store, mgr, tools, _, _, delegator = stack

        front_engram = _make(mgr, "front consciousness")
        tools.register("delegate", "delegate heavy work",
                       delegator.as_tool(front_engram.id))

        # scripted LLM: first output delegates, second concludes
        calls = {"n": 0}
        orig = mgr.llm.complete

        def scripted(messages, **kw):
            calls["n"] += 1
            if calls["n"] == 1:
                r = orig(messages, **kw)
                r.content = "这件事需要深入执行。委派: 系统性梳理STDP文献"
                return r
            return orig(messages, **kw)

        mgr.llm.complete = scripted

        front = FrontAgent(front_engram.id, mgr, tools,
                           FrontAgentConfig(max_think_iterations=3))
        front.think()

        records = store.list_delegations(front_engram.id)
        assert len(records) == 1
        assert "STDP" in records[0]["task"]

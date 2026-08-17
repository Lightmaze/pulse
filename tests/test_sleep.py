"""Tests for the sleep/dream engine."""

import pytest

from pulse_system.agent.delegate import Delegator, DelegatorConfig
from pulse_system.agent.tools import ToolRegistry
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.delegation import DelegationRouter, RouterConfig
from pulse_system.core.engram import EngramManager
from pulse_system.core.runtime.publication import (
    RuntimePublicationPermit,
    RuntimePublicationGate,
)
from pulse_system.core.sleep import SleepConfig, SleepEngine
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
    library = Library(
        tmp_path / "lib",
        publication_authority=_publication_permit(
            "test:sleep-library"
        ),
    )
    mgr = EngramManager(store, llm, conn_net, library=library)
    tools = ToolRegistry(mock=True, workspace_root=tmp_path)
    metrics = MetricsRecorder()
    router = DelegationRouter(store, config=RouterConfig(max_slots=16))
    delegator = Delegator(
        store, mgr, tools, library=library, metrics=metrics, router=router,
        config=DelegatorConfig(max_think_iterations=1),
    )
    sleep = SleepEngine(
        store, mgr, conn_net, library,
        delegator=delegator, router=router, metrics=metrics,
        config=SleepConfig(cycles=2, rem_reads_per_night=2),
    )
    yield store, mgr, library, sleep, metrics, router
    store.close()


def _make(mgr, content="an engram with history"):
    return mgr.create(initial_messages=[
        Message(role=MessageRole.USER, content=content),
    ])


class TestNREM:
    def test_diary_consolidated_into_wiki(self, stack):
        store, mgr, library, sleep, metrics, _ = stack
        e = _make(mgr)
        library.append_diary(e.id, "白天被借用做了任务A,发现模式X", source="delegation:f")
        library.append_diary(e.id, "旁听了一段关于Y的讨论", source="clone:s1")
        before_len = len(mgr.get_session(e.id))

        report = sleep.run_night()

        assert e.id in report.consolidated
        assert len(library.wiki_entries(e.id)) >= 1
        # main session untouched — dream output goes to wiki only
        assert len(mgr.get_session(e.id)) == before_len

    def test_watermark_prevents_reconsolidation(self, stack):
        store, mgr, library, sleep, _, _ = stack
        e = _make(mgr)
        library.append_diary(e.id, "entry one")

        r1 = sleep.run_night()
        r2 = sleep.run_night()

        assert e.id in r1.consolidated
        assert e.id not in r2.consolidated  # nothing new since watermark

        library.append_diary(e.id, "entry two, after first night")
        r3 = sleep.run_night()
        assert e.id in r3.consolidated

    def test_archived_engram_skipped(self, stack):
        store, mgr, library, sleep, _, _ = stack
        e = _make(mgr)
        library.append_diary(e.id, "entry")
        store.archive_engram(e.id)

        report = sleep.run_night()
        assert e.id not in report.consolidated


class TestREM:
    def test_deep_read_spawns_hub_with_connections(self, stack):
        store, mgr, library, sleep, metrics, _ = stack
        a = _make(mgr, "经历A:" + "关于选择与判断的思考。" * 30)
        b = _make(mgr, "经历B:" + "关于探索与利用的权衡。" * 30)

        report = sleep.run_night()

        assert len(report.deep_reads) >= 1
        assert len(report.hubs_spawned) >= 1
        hub_id = report.hubs_spawned[0]
        hub = store.get_engram(hub_id)
        assert hub is not None and hub.status == EngramStatus.ACTIVE
        # hub wired back to at least one source, both directions
        read = report.deep_reads[0]
        assert store.get_connection(hub_id, read) is not None
        assert store.get_connection(read, hub_id) is not None

    def test_coverage_first_reading(self, stack):
        store, mgr, library, sleep, _, _ = stack
        a = _make(mgr, "old and read")
        b = _make(mgr, "never read")
        sleep._mark_read(a.id)
        store.update_engram_metadata(a.id, recent_activity=1.0)
        store.update_engram_metadata(b.id, recent_activity=0.1)

        # despite a's higher activity, never-read b goes first
        assert sleep._pick_for_reading(exclude=set()) == b.id

    def test_dream_forks_do_not_leak(self, stack):
        store, mgr, library, sleep, _, _ = stack
        e = _make(mgr, "content " * 50)
        library.append_diary(e.id, "one entry")
        active_before = {x.id for x in store.list_engrams(status=EngramStatus.ACTIVE)}

        report = sleep.run_night()

        active_after = {x.id for x in store.list_engrams(status=EngramStatus.ACTIVE)}
        leaked = active_after - active_before - set(report.hubs_spawned)
        assert leaked == set(), f"transient forks leaked: {leaked}"

    def test_virtual_delegation_covers_zero_experience(self, stack):
        store, mgr, library, sleep, _, router = stack
        e = _make(mgr, "never delegated to")

        report = sleep.run_night()

        assert report.virtual_delegations >= 1
        records = store.list_delegations("dream")
        assert any(r["target_id"] == e.id for r in records)


class TestNightStructure:
    def test_report_and_metrics(self, stack):
        store, mgr, library, sleep, metrics, _ = stack
        e = _make(mgr)
        library.append_diary(e.id, "entry")

        report = sleep.run_night()

        assert report.cycles == 2
        [night] = metrics.events("sleep_night")
        assert night["consolidated"] == len(report.consolidated)
        assert night["errors"] == report.errors

    def test_night_without_material_is_calm(self, stack):
        """Empty network → no crashes, empty report."""
        store, mgr, library, sleep, _, _ = stack
        report = sleep.run_night()
        assert report.consolidated == []
        assert report.hubs_spawned == []

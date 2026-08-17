"""Tests for Project cluster management."""

import pytest

from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.engram import EngramManager
from pulse_system.core.types import Message, MessageRole
from pulse_system.education.project import ProjectManager
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


@pytest.fixture
def store():
    s = Storage(":memory:")
    yield s
    s.close()


@pytest.fixture
def mock_llm():
    return LLMAdapter(mock=True)


@pytest.fixture
def conn_net(store):
    return ConnectionNetwork(store, ConnectionConfig())


@pytest.fixture
def mgr(store, mock_llm, conn_net):
    return EngramManager(store, mock_llm, conn_net)


@pytest.fixture
def pm(store):
    return ProjectManager(store)


# ── Create ───────────────────────────────────────────────────────


class TestCreate:
    def test_create_project(self, pm, tmp_path):
        project = pm.create("TestProject", "A test project", str(tmp_path / "ws"))
        assert project.name == "TestProject"
        assert project.description == "A test project"
        assert project.workspace_path == str(tmp_path / "ws")

    def test_create_auto_workspace(self, pm):
        project = pm.create("AutoWS", "auto workspace")
        assert project.workspace_path is not None
        assert "AutoWS" in project.workspace_path

    def test_list_projects(self, pm, tmp_path):
        pm.create("P1", workspace_path=str(tmp_path / "p1"))
        pm.create("P2", workspace_path=str(tmp_path / "p2"))
        projects = pm.list_projects()
        assert len(projects) == 2

    def test_get_project(self, pm, tmp_path):
        p = pm.create("GetMe", workspace_path=str(tmp_path / "ws"))
        found = pm.get_project(p.id)
        assert found is not None
        assert found.name == "GetMe"

    def test_get_nonexistent(self, pm):
        assert pm.get_project("nonexistent") is None


# ── Engram membership ───────────────────────────────────────────


class TestMembership:
    def test_add_engram(self, pm, mgr, store, tmp_path):
        project = pm.create("P", workspace_path=str(tmp_path / "ws"))
        e = mgr.create(initial_messages=[Message(role=MessageRole.USER, content="hi")])

        pm.add_engram(project.id, e.id)
        engrams = pm.get_engrams(project.id)
        assert len(engrams) == 1
        assert engrams[0].id == e.id

    def test_add_to_nonexistent_project(self, pm, mgr):
        e = mgr.create()
        with pytest.raises(ValueError, match="Project"):
            pm.add_engram("fake", e.id)

    def test_add_nonexistent_engram(self, pm, tmp_path):
        p = pm.create("P", workspace_path=str(tmp_path / "ws"))
        with pytest.raises(ValueError, match="Engram"):
            pm.add_engram(p.id, "fake")

    def test_remove_engram(self, pm, mgr, store, tmp_path):
        project = pm.create("P", workspace_path=str(tmp_path / "ws"))
        e = mgr.create()
        pm.add_engram(project.id, e.id)

        pm.remove_engram(project.id, e.id)
        engrams = pm.get_engrams(project.id)
        assert len(engrams) == 0

    def test_remove_wrong_project(self, pm, mgr, tmp_path):
        p1 = pm.create("P1", workspace_path=str(tmp_path / "p1"))
        p2 = pm.create("P2", workspace_path=str(tmp_path / "p2"))
        e = mgr.create()
        pm.add_engram(p1.id, e.id)

        with pytest.raises(ValueError, match="not in project"):
            pm.remove_engram(p2.id, e.id)

    def test_multiple_engrams(self, pm, mgr, tmp_path):
        project = pm.create("P", workspace_path=str(tmp_path / "ws"))
        e1 = mgr.create()
        e2 = mgr.create()
        e3 = mgr.create()

        pm.add_engram(project.id, e1.id)
        pm.add_engram(project.id, e2.id)
        pm.add_engram(project.id, e3.id)

        engrams = pm.get_engrams(project.id)
        assert len(engrams) == 3


# ── Workspace ────────────────────────────────────────────────────


class TestWorkspace:
    def test_get_workspace(self, pm, tmp_path):
        p = pm.create("WS", workspace_path=str(tmp_path / "ws"))
        assert pm.get_workspace(p.id) == str(tmp_path / "ws")

    def test_get_workspace_nonexistent(self, pm):
        assert pm.get_workspace("fake") is None


# ── Boost intra-connections ──────────────────────────────────────


class TestBoost:
    def test_boost_intra_connections(self, pm, mgr, store, tmp_path):
        project = pm.create("P", workspace_path=str(tmp_path / "ws"))
        e1 = mgr.create()
        e2 = mgr.create()
        pm.add_engram(project.id, e1.id)
        pm.add_engram(project.id, e2.id)

        store.create_connection(e1.id, e2.id, 0.3)
        store.create_connection(e2.id, e1.id, 0.2)

        boosted = pm.boost_intra_connections(project.id, 2.0)
        assert boosted == 2

        conn = store.get_connection(e1.id, e2.id)
        assert conn.weight == pytest.approx(0.6)

        conn2 = store.get_connection(e2.id, e1.id)
        assert conn2.weight == pytest.approx(0.4)

    def test_boost_capped_at_1(self, pm, mgr, store, tmp_path):
        project = pm.create("P", workspace_path=str(tmp_path / "ws"))
        e1 = mgr.create()
        e2 = mgr.create()
        pm.add_engram(project.id, e1.id)
        pm.add_engram(project.id, e2.id)

        store.create_connection(e1.id, e2.id, 0.8)
        pm.boost_intra_connections(project.id, 2.0)

        conn = store.get_connection(e1.id, e2.id)
        assert conn.weight == 1.0

    def test_boost_ignores_external_connections(self, pm, mgr, store, tmp_path):
        project = pm.create("P", workspace_path=str(tmp_path / "ws"))
        e1 = mgr.create()
        e2 = mgr.create()
        e_external = mgr.create()
        pm.add_engram(project.id, e1.id)
        pm.add_engram(project.id, e2.id)
        # e_external NOT in project

        store.create_connection(e1.id, e_external.id, 0.3)
        store.create_connection(e1.id, e2.id, 0.3)

        boosted = pm.boost_intra_connections(project.id, 2.0)
        assert boosted == 1  # only the intra-project connection

        ext_conn = store.get_connection(e1.id, e_external.id)
        assert ext_conn.weight == pytest.approx(0.3)  # unchanged

    def test_boost_single_engram_noop(self, pm, mgr, tmp_path):
        project = pm.create("P", workspace_path=str(tmp_path / "ws"))
        e = mgr.create()
        pm.add_engram(project.id, e.id)

        assert pm.boost_intra_connections(project.id, 2.0) == 0

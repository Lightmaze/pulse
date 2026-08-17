"""Tests for index management."""

import pytest

from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.engram import EngramManager
from pulse_system.core.types import MessageRole
from pulse_system.education.index import IndexManager
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
def idx_mgr(mgr, store):
    return IndexManager(mgr, store)


def _make_project(store):
    return store.create_project(name="TestProject", description="test")


# ── Create index ─────────────────────────────────────────────────


class TestCreateIndex:
    def test_create_index(self, idx_mgr, store):
        project = _make_project(store)
        index_id = idx_mgr.create_index(
            project_id=project.id,
            title="Deep Learning Mastery",
            structure="1. Foundations\n2. CNNs\n3. Transformers",
            commitment="Master deep learning from scratch to research-level.",
        )

        assert index_id is not None

        # Index engram should have initial content
        engram = idx_mgr._mgr.get(index_id)
        assert engram is not None
        assert engram.project_id == project.id

        session = idx_mgr._mgr.get_session(index_id)
        assert len(session) == 1
        assert "Deep Learning Mastery" in session[0].content
        assert "Foundations" in session[0].content
        assert "Master deep learning" in session[0].content

    def test_index_linked_to_project(self, idx_mgr, store):
        project = _make_project(store)
        index_id = idx_mgr.create_index(
            project.id, "Title", "Structure", "Commitment"
        )

        updated = store.get_project(project.id)
        assert updated.index_engram_id == index_id


# ── Update progress ──────────────────────────────────────────────


class TestUpdateProgress:
    def test_progress_appended(self, idx_mgr, store):
        project = _make_project(store)
        index_id = idx_mgr.create_index(
            project.id, "Title", "Structure", "Commitment"
        )

        idx_mgr.update_progress(index_id, "Chapter 1: Foundations", "completed")

        session = idx_mgr._mgr.get_session(index_id)
        # initial + progress injection
        assert len(session) == 2
        assert session[1].role == MessageRole.INJECTION
        assert "Foundations" in session[1].content
        assert "completed" in session[1].content

    def test_multiple_progress_updates(self, idx_mgr, store):
        project = _make_project(store)
        index_id = idx_mgr.create_index(
            project.id, "Title", "Structure", "Commitment"
        )

        idx_mgr.update_progress(index_id, "Ch1", "done")
        idx_mgr.update_progress(index_id, "Ch2", "in progress")
        idx_mgr.update_progress(index_id, "Ch3", "not started")

        session = idx_mgr._mgr.get_session(index_id)
        assert len(session) == 4  # initial + 3 progress


# ── Reaffirm ─────────────────────────────────────────────────────


class TestReaffirm:
    def test_reaffirm_pulses(self, idx_mgr, store):
        project = _make_project(store)
        index_id = idx_mgr.create_index(
            project.id, "Title", "Structure", "Commitment"
        )

        output = idx_mgr.reaffirm(index_id)
        assert isinstance(output, str)
        assert len(output) > 0

        # Session should now have initial + LLM response
        session = idx_mgr._mgr.get_session(index_id)
        assert len(session) == 2
        assert session[1].role == MessageRole.ASSISTANT

    def test_reaffirm_after_progress(self, idx_mgr, store):
        project = _make_project(store)
        index_id = idx_mgr.create_index(
            project.id, "Title", "Structure", "Commitment"
        )

        idx_mgr.update_progress(index_id, "Ch1", "done")
        output = idx_mgr.reaffirm(index_id)
        assert len(output) > 0

        session = idx_mgr._mgr.get_session(index_id)
        # initial + progress_injection + LLM_response
        assert len(session) == 3


# ── Get index ────────────────────────────────────────────────────


class TestGetIndex:
    def test_get_index(self, idx_mgr, store):
        project = _make_project(store)
        index_id = idx_mgr.create_index(
            project.id, "Title", "Structure", "Commitment"
        )

        engram = idx_mgr.get_index(project.id)
        assert engram is not None
        assert engram.id == index_id

    def test_get_index_no_project(self, idx_mgr):
        assert idx_mgr.get_index("nonexistent") is None

    def test_get_index_no_index(self, idx_mgr, store):
        project = _make_project(store)
        assert idx_mgr.get_index(project.id) is None

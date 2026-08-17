"""Tests for the storage layer."""

import pytest
from datetime import datetime, timezone

from pulse_system.core.types import (
    Connection,
    ConnectionType,
    Engram,
    EngramStatus,
    Message,
    MessageRole,
)
from pulse_system.substrate.storage import Storage


@pytest.fixture
def store():
    s = Storage(":memory:")
    yield s
    s.close()


# ── Engram CRUD ──────────────────────────────────────────────────


class TestEngramCRUD:
    def test_create_and_get(self, store: Storage):
        e = store.create_engram(engram_id="e1")
        assert e.id == "e1"
        assert e.status == EngramStatus.ACTIVE
        assert e.total_pulses == 0

        fetched = store.get_engram("e1")
        assert fetched is not None
        assert fetched.id == "e1"
        assert fetched.status == EngramStatus.ACTIVE

    def test_create_with_project(self, store: Storage):
        e = store.create_engram(engram_id="e1", project_id="proj1")
        assert e.project_id == "proj1"
        fetched = store.get_engram("e1")
        assert fetched.project_id == "proj1"

    def test_create_with_initial_messages(self, store: Storage):
        msgs = [
            Message(role=MessageRole.USER, content="hello"),
            Message(role=MessageRole.ASSISTANT, content="hi there"),
        ]
        e = store.create_engram(engram_id="e1", initial_messages=msgs)
        session = store.get_session("e1")
        assert len(session) == 2
        assert session[0].role == MessageRole.USER
        assert session[0].content == "hello"
        assert session[1].content == "hi there"
        assert e.name == "hello"
        assert e.name_origin == "auto"
        assert e.nickname is None

    def test_auto_name_can_be_deferred_until_first_effective_content(
        self, store: Storage
    ):
        e = store.create_engram(engram_id="e1", auto_name=False)
        assert e.name is None
        assert store.ensure_auto_name("e1", "  调律与介入必须分开  ")
        assert store.get_engram("e1").name == "调律与介入必须分开"

    def test_user_identity_is_persistent_and_auto_name_cannot_overwrite_it(
        self, store: Storage
    ):
        store.create_engram(engram_id="e1", auto_name=False)
        updated = store.update_engram_identity(
            "e1", {"name": "Runtime rail", "nickname": "northstar"}
        )
        assert updated is not None
        assert updated.name == "Runtime rail"
        assert updated.name_origin == "user"
        assert updated.nickname == "northstar"
        assert not store.ensure_auto_name("e1", "a later user message")
        fetched = store.get_engram("e1")
        assert fetched.name == "Runtime rail"
        assert fetched.nickname == "northstar"

    def test_get_nonexistent(self, store: Storage):
        assert store.get_engram("nope") is None

    def test_list_engrams(self, store: Storage):
        store.create_engram(engram_id="e1", project_id="p1")
        store.create_engram(engram_id="e2", project_id="p1")
        store.create_engram(engram_id="e3", project_id="p2")

        all_engrams = store.list_engrams()
        assert len(all_engrams) == 3

        p1_engrams = store.list_engrams(project_id="p1")
        assert len(p1_engrams) == 2

    def test_archive(self, store: Storage):
        store.create_engram(engram_id="e1")
        assert store.archive_engram("e1")

        e = store.get_engram("e1")
        assert e.status == EngramStatus.ARCHIVED

        active = store.list_engrams(status=EngramStatus.ACTIVE)
        assert len(active) == 0

        archived = store.list_engrams(status=EngramStatus.ARCHIVED)
        assert len(archived) == 1

    def test_archive_nonexistent(self, store: Storage):
        assert not store.archive_engram("nope")

    def test_update_metadata(self, store: Storage):
        store.create_engram(engram_id="e1")
        now = datetime.now(timezone.utc)
        store.update_engram_metadata(
            "e1",
            last_pulse_at=now,
            total_pulses=5,
            recent_activity=0.8,
            token_count=1200,
        )
        e = store.get_engram("e1")
        assert e.total_pulses == 5
        assert e.metadata.recent_activity == 0.8
        assert e.metadata.token_count == 1200
        assert e.last_pulse_at is not None

    def test_auto_generated_id(self, store: Storage):
        e = store.create_engram()
        assert len(e.id) == 16


# ── Session (append-only) ───────────────────────────────────────


class TestSession:
    def test_append_single(self, store: Storage):
        store.create_engram(engram_id="e1")
        store.append_message("e1", Message(role=MessageRole.USER, content="msg1"))
        session = store.get_session("e1")
        assert len(session) == 1
        assert session[0].content == "msg1"

    def test_append_preserves_order(self, store: Storage):
        store.create_engram(engram_id="e1")
        for i in range(5):
            store.append_message(
                "e1", Message(role=MessageRole.USER, content=f"msg{i}")
            )
        session = store.get_session("e1")
        assert [m.content for m in session] == [f"msg{i}" for i in range(5)]

    def test_append_batch(self, store: Storage):
        store.create_engram(engram_id="e1")
        msgs = [
            Message(role=MessageRole.USER, content="a"),
            Message(role=MessageRole.ASSISTANT, content="b"),
        ]
        store.append_messages("e1", msgs)
        session = store.get_session("e1")
        assert len(session) == 2

    def test_injection_message(self, store: Storage):
        store.create_engram(engram_id="e1")
        store.append_message(
            "e1",
            Message(
                role=MessageRole.INJECTION,
                content="injected context",
                source_engram_id="e2",
            ),
        )
        session = store.get_session("e1")
        assert session[0].role == MessageRole.INJECTION
        assert session[0].source_engram_id == "e2"

    def test_session_limit_returns_most_recent(self, store: Storage):
        store.create_engram(engram_id="e1")
        for i in range(10):
            store.append_message(
                "e1", Message(role=MessageRole.USER, content=f"msg{i}")
            )
        session = store.get_session("e1", limit=3)
        assert len(session) == 3
        # most recent 3, still in chronological order
        assert [m.content for m in session] == ["msg7", "msg8", "msg9"]

    def test_message_count(self, store: Storage):
        store.create_engram(engram_id="e1")
        assert store.get_message_count("e1") == 0
        store.append_message("e1", Message(role=MessageRole.USER, content="x"))
        assert store.get_message_count("e1") == 1


# ── Connection CRUD ──────────────────────────────────────────────


class TestConnectionCRUD:
    def _setup_engrams(self, store: Storage):
        store.create_engram(engram_id="a")
        store.create_engram(engram_id="b")
        store.create_engram(engram_id="c")

    def test_create_and_get(self, store: Storage):
        self._setup_engrams(store)
        conn = store.create_connection("a", "b", 0.5)
        assert conn.from_id == "a"
        assert conn.to_id == "b"
        assert conn.weight == 0.5

        fetched = store.get_connection("a", "b")
        assert fetched is not None
        assert fetched.weight == 0.5

    def test_get_nonexistent_connection(self, store: Storage):
        self._setup_engrams(store)
        assert store.get_connection("a", "b") is None

    def test_inhibitory_connection(self, store: Storage):
        self._setup_engrams(store)
        conn = store.create_connection("a", "b", 0.3, ConnectionType.INHIBITORY)
        assert conn.conn_type == ConnectionType.INHIBITORY
        fetched = store.get_connection("a", "b")
        assert fetched.conn_type == ConnectionType.INHIBITORY

    def test_outgoing_edges(self, store: Storage):
        self._setup_engrams(store)
        store.create_connection("a", "b", 0.5)
        store.create_connection("a", "c", 0.2)
        store.create_connection("b", "c", 0.8)

        out_a = store.get_outgoing("a")
        assert len(out_a) == 2

        out_a_strong = store.get_outgoing("a", min_weight=0.3)
        assert len(out_a_strong) == 1
        assert out_a_strong[0].to_id == "b"

    def test_incoming_edges(self, store: Storage):
        self._setup_engrams(store)
        store.create_connection("a", "c", 0.5)
        store.create_connection("b", "c", 0.8)

        inc_c = store.get_incoming("c")
        assert len(inc_c) == 2

    def test_update_weight(self, store: Storage):
        self._setup_engrams(store)
        store.create_connection("a", "b", 0.5)
        store.update_weight("a", "b", 0.9)

        conn = store.get_connection("a", "b")
        assert conn.weight == pytest.approx(0.9)

    def test_update_weight_clamped(self, store: Storage):
        self._setup_engrams(store)
        store.create_connection("a", "b", 0.5)

        store.update_weight("a", "b", 1.5)
        assert store.get_connection("a", "b").weight == pytest.approx(1.0)

        store.update_weight("a", "b", -0.3)
        assert store.get_connection("a", "b").weight == pytest.approx(0.0)

    def test_decay_all(self, store: Storage):
        self._setup_engrams(store)
        store.create_connection("a", "b", 1.0)
        store.create_connection("a", "c", 0.5)

        updated = store.decay_all(0.1)  # multiply by 0.9
        assert updated == 2

        ab = store.get_connection("a", "b")
        assert ab.weight == pytest.approx(0.9)

        ac = store.get_connection("a", "c")
        assert ac.weight == pytest.approx(0.45)

    def test_prune(self, store: Storage):
        self._setup_engrams(store)
        store.create_connection("a", "b", 0.5)
        store.create_connection("a", "c", 0.005)

        pruned = store.prune(0.01)
        assert pruned == 1
        assert store.get_connection("a", "b") is not None
        assert store.get_connection("a", "c") is None

    def test_decay_then_prune(self, store: Storage):
        self._setup_engrams(store)
        store.create_connection("a", "b", 0.02)
        store.create_connection("a", "c", 0.8)

        for _ in range(5):
            store.decay_all(0.1)

        # 0.02 * 0.9^5 ≈ 0.012, 0.8 * 0.9^5 ≈ 0.472
        store.prune(0.05)
        assert store.get_connection("a", "b") is None
        assert store.get_connection("a", "c") is not None

    def test_count_connections(self, store: Storage):
        self._setup_engrams(store)
        store.create_connection("a", "b", 0.5)
        store.create_connection("b", "c", 0.3)
        assert store.count_connections("b") == 2  # one incoming, one outgoing
        assert store.count_connections("a") == 1

    def test_transfer_connections(self, store: Storage):
        self._setup_engrams(store)
        store.create_engram(engram_id="a_new")

        store.create_connection("a", "b", 0.5)
        store.create_connection("c", "a", 0.3)

        store.transfer_connections("a", "a_new")

        assert store.get_connection("a", "b") is None
        assert store.get_connection("a_new", "b") is not None
        assert store.get_connection("a_new", "b").weight == pytest.approx(0.5)

        assert store.get_connection("c", "a") is None
        assert store.get_connection("c", "a_new") is not None


# ── transfer_connections conflict merge ──────────────────────────


class TestTransferConnectionsMerge:
    def test_conflicting_outgoing_merged_max_weight(self, store):
        old = store.create_engram()
        new = store.create_engram()
        peer = store.create_engram()
        store.create_connection(old.id, peer.id, 0.8)
        store.create_connection(new.id, peer.id, 0.3)

        store.transfer_connections(old.id, new.id)  # must not raise

        merged = store.get_connection(new.id, peer.id)
        assert merged is not None
        assert merged.weight == pytest.approx(0.8)
        assert store.get_connection(old.id, peer.id) is None

    def test_conflicting_incoming_merged_max_weight(self, store):
        old = store.create_engram()
        new = store.create_engram()
        peer = store.create_engram()
        store.create_connection(peer.id, old.id, 0.2)
        store.create_connection(peer.id, new.id, 0.9)

        store.transfer_connections(old.id, new.id)

        merged = store.get_connection(peer.id, new.id)
        assert merged is not None
        assert merged.weight == pytest.approx(0.9)
        assert store.get_connection(peer.id, old.id) is None

    def test_old_new_edges_dropped_not_self_looped(self, store):
        old = store.create_engram()
        new = store.create_engram()
        store.create_connection(old.id, new.id, 0.5)
        store.create_connection(new.id, old.id, 0.4)

        store.transfer_connections(old.id, new.id)

        assert store.get_connection(new.id, new.id) is None
        assert store.count_connections(old.id) == 0

    def test_plain_transfer_still_works(self, store):
        old = store.create_engram()
        new = store.create_engram()
        a = store.create_engram()
        b = store.create_engram()
        store.create_connection(old.id, a.id, 0.6)
        store.create_connection(b.id, old.id, 0.7)

        store.transfer_connections(old.id, new.id)

        assert store.get_connection(new.id, a.id).weight == pytest.approx(0.6)
        assert store.get_connection(b.id, new.id).weight == pytest.approx(0.7)


# ── Component state (delegation router/Claustrum modulator weight persistence) ─────────────────


class TestComponentState:
    def test_missing_returns_none(self, store):
        assert store.load_component_state("nope") is None

    def test_roundtrip(self, store):
        state = {"updates_seen": 3, "w": [[0.5, 1.0], [0.0, -2.5]]}
        store.save_component_state("comp", state)
        assert store.load_component_state("comp") == state

    def test_overwrite_keeps_latest(self, store):
        store.save_component_state("comp", {"v": 1})
        store.save_component_state("comp", {"v": 2})
        assert store.load_component_state("comp") == {"v": 2}

    def test_components_are_independent(self, store):
        store.save_component_state("a", {"v": 1})
        store.save_component_state("b", {"v": 2})
        assert store.load_component_state("a") == {"v": 1}
        assert store.load_component_state("b") == {"v": 2}

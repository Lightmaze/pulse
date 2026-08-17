"""Tests for the clone front-stage agent.

Uses mock LLM. Tests clone session lifecycle, activation gating,
steer judgment, silence/exit, and CloneManager.
"""

import pytest

from pulse_system.agent.clone import CloneManager, CloneSession
from pulse_system.agent.clone.agent import (
    ActiveEngram,
    CloneSessionConfig,
    _SILENCE_MARKERS,
)
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.engram import EngramManager
from pulse_system.core.types import Message, MessageRole
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


def _make_clone(store, mock_llm, topic=None, config=None):
    return CloneSession(
        session_id="test-session",
        storage=store,
        llm=mock_llm,
        topic=topic,
        config=config,
    )


# ── Basic session ────────────────────────────────────────────────


class TestCloneSession:
    def test_create_session(self, store, mock_llm):
        clone = _make_clone(store, mock_llm, topic="test topic")
        assert clone.id == "test-session"
        assert clone.topic == "test topic"
        assert len(clone.get_history()) == 0

    def test_process_message_returns_response(self, store, mock_llm):
        clone = _make_clone(store, mock_llm)
        response = clone.process_message("Hello!")
        assert isinstance(response, str)
        assert len(response) > 0

    def test_history_grows(self, store, mock_llm):
        clone = _make_clone(store, mock_llm)
        clone.process_message("First message")
        clone.process_message("Second message")
        history = clone.get_history()
        # Each process_message adds: user msg + clone response (at minimum)
        assert len(history) >= 4

    def test_history_has_correct_roles(self, store, mock_llm):
        clone = _make_clone(store, mock_llm)
        clone.process_message("Hello")
        history = clone.get_history()
        assert history[0].role == "user"
        assert history[0].content == "Hello"
        assert history[-1].role == "clone"


# ── Activation gating ───────────────────────────────────────────


class TestActivationGating:
    def test_keyword_gate_activates_relevant(self, store, mock_llm, mgr):
        # Create an engram about quantum physics
        mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="quantum physics experiment"),
        ])

        clone = _make_clone(store, mock_llm)
        clone.process_message("Tell me about quantum physics")

        # The engram should have been activated (keyword overlap: "quantum", "physics")
        active = clone.get_active_engrams()
        assert len(active) >= 1

    def test_no_activation_on_irrelevant(self, store, mock_llm, mgr):
        # Create an engram about cooking
        mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="cooking recipes pasta sauce"),
        ])

        clone = _make_clone(store, mock_llm)
        clone.process_message("quantum physics experiment")

        active = clone.get_active_engrams()
        assert len(active) == 0

    def test_max_active_engrams_respected(self, store, mock_llm, mgr):
        # Create many engrams with overlapping content
        for i in range(10):
            mgr.create(initial_messages=[
                Message(role=MessageRole.USER, content=f"shared topic discussion {i}"),
            ])

        config = CloneSessionConfig(max_active_engrams=3)
        clone = _make_clone(store, mock_llm, config=config)
        clone.process_message("shared topic discussion")

        assert len(clone.get_active_engrams()) <= 3

    def test_already_active_not_readded(self, store, mock_llm, mgr):
        mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="quantum physics test"),
        ])

        clone = _make_clone(store, mock_llm)
        clone.process_message("tell me quantum physics")
        first_active = clone.get_active_engrams()

        clone.process_message("more quantum physics please")
        second_active = clone.get_active_engrams()

        # Same engram shouldn't be re-added
        assert set(first_active) == set(second_active)


# ── Steer judgment ───────────────────────────────────────────────


class TestSteerJudgment:
    def test_steer_engram_produces_output(self, store, mock_llm, mgr):
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="I know about science."),
        ])
        session = mgr.get_session(e.id)

        clone = _make_clone(store, mock_llm)
        active = ActiveEngram(engram_id=e.id, session_snapshot=session)

        # Mock LLM returns non-silent response
        result = clone._steer_engram(active)
        # Mock LLM echoes input, which doesn't start with silence markers
        assert result is not None

    def test_silence_detection(self, store, mock_llm, mgr):
        """Test that silence markers are recognized."""
        for marker in _SILENCE_MARKERS:
            # Verify marker is a known pattern
            assert marker.startswith("[")


# ── Exit condition ───────────────────────────────────────────────


class TestExitCondition:
    def test_engram_exits_after_consecutive_silences(self, store, conn_net):
        """After N consecutive silences, engram exits and diary is written."""
        # Use a scripted LLM that always returns silence
        llm = _SilentLLM()
        mgr = EngramManager(store, llm, conn_net)

        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="shared keywords here"),
        ])

        # Manually activate the engram in the clone session
        clone = _make_clone(store, llm)
        session = mgr.get_session(e.id)
        clone._active_engrams[e.id] = ActiveEngram(
            engram_id=e.id, session_snapshot=session,
        )

        # Process messages — the engram should stay silent each time
        for _ in range(3):
            clone.process_message("another message")

        # After 3 silences (MAX_SILENCE_BEFORE_EXIT=3), engram should exit
        assert e.id not in clone._active_engrams

    def test_diary_written_on_exit(self, store, conn_net):
        llm = _SilentLLM()
        mgr = EngramManager(store, llm, conn_net)

        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="shared keywords here"),
        ])

        clone = _make_clone(store, llm)
        session = mgr.get_session(e.id)
        clone._active_engrams[e.id] = ActiveEngram(
            engram_id=e.id, session_snapshot=session,
        )

        for _ in range(3):
            clone.process_message("message")

        summary = clone.finalize()
        assert summary["engrams_activated"] >= 1
        assert any(e.id == entry[0] for entry in summary["diary_entries"])

    def test_active_engram_resets_silence_on_contribution(self, store, conn_net):
        """If an engram contributes, its silence counter resets."""
        # Use a scripted LLM: silent, silent, then contributes, then silent...
        llm = _ScriptedSteerLLM([
            "[沉默]", "[沉默]",  # 2 silences
            "I have something to say!",  # contribution → reset
            "[沉默]", "[沉默]",  # 2 more silences (below threshold)
        ])
        mgr = EngramManager(store, llm, conn_net)

        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="shared keywords here"),
        ])

        clone = _make_clone(store, llm)
        session = mgr.get_session(e.id)
        clone._active_engrams[e.id] = ActiveEngram(
            engram_id=e.id, session_snapshot=session,
        )

        for _ in range(5):
            clone.process_message("message")

        # Should still be active: 2 silences → contribution(reset) → 2 silences (< 3)
        assert e.id in clone._active_engrams


# ── Finalize ─────────────────────────────────────────────────────


class TestFinalize:
    def test_finalize_returns_summary(self, store, mock_llm):
        clone = _make_clone(store, mock_llm, topic="test")
        clone.process_message("first")
        clone.process_message("second")

        summary = clone.finalize()
        assert summary["session_id"] == "test-session"
        assert summary["topic"] == "test"
        assert summary["message_count"] >= 4  # 2 user + 2 clone responses


# ── CloneManager ─────────────────────────────────────────────────


class TestCloneManager:
    def test_create_session(self, store, mock_llm):
        manager = CloneManager(store, mock_llm)
        session = manager.create_session(topic="physics")
        assert session.topic == "physics"
        assert manager.get_session(session.id) is session

    def test_list_sessions(self, store, mock_llm):
        manager = CloneManager(store, mock_llm)
        manager.create_session(topic="a")
        manager.create_session(topic="b")
        sessions = manager.list_sessions()
        assert len(sessions) == 2

    def test_get_nonexistent_session(self, store, mock_llm):
        manager = CloneManager(store, mock_llm)
        assert manager.get_session("nonexistent") is None


# ── Engram contributions appear in history ───────────────────────


class TestEngramContributions:
    def test_engram_output_in_history(self, store, mock_llm, mgr):
        # Create an engram that will be activated
        mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="quantum physics experiment"),
        ])

        clone = _make_clone(store, mock_llm)
        clone.process_message("tell me about quantum physics")

        history = clone.get_history()
        engram_msgs = [m for m in history if m.role == "engram"]
        # If engram was activated AND steer returned non-silent, it should appear
        if clone.get_active_engrams():
            assert len(engram_msgs) >= 1


# ── Helper LLMs ──────────────────────────────────────────────────


class _SilentLLM:
    """LLM that always returns silence marker."""

    def __init__(self):
        self.mock = True
        self.stats = type("S", (), {
            "total_calls": 0, "total_input_tokens": 0,
            "total_output_tokens": 0, "cache_hits": 0, "cache_misses": 0,
            "cache_hit_rate": 0.0,
        })()

    def complete(self, messages, **kwargs):
        return type("R", (), {
            "content": "[沉默]",
            "input_tokens": 10, "output_tokens": 5,
        })()

    def embed(self, text):
        return type("R", (), {"vector": [0.0] * 256})()

    def estimate_tokens(self, text):
        return len(text) // 3

    def get_stats(self):
        return self.stats


class _ScriptedSteerLLM:
    """LLM with scripted responses for steer calls.

    First call per process_message is the steer call (from active engram),
    second call is the clone self-response.
    """

    def __init__(self, steer_responses: list[str]):
        self._steer_responses = list(steer_responses)
        self._steer_idx = 0
        self._call_count = 0
        self.mock = True
        self.stats = type("S", (), {
            "total_calls": 0, "total_input_tokens": 0,
            "total_output_tokens": 0, "cache_hits": 0, "cache_misses": 0,
            "cache_hit_rate": 0.0,
        })()

    def complete(self, messages, **kwargs):
        self._call_count += 1
        # Detect steer call by checking if the last message contains the steer prompt
        last_content = messages[-1].get("content", "") if messages else ""
        if "旁听" in last_content or "沉默" in last_content:
            idx = min(self._steer_idx, len(self._steer_responses) - 1)
            resp = self._steer_responses[idx]
            self._steer_idx += 1
        else:
            resp = "[clone response]"

        return type("R", (), {
            "content": resp,
            "input_tokens": 10, "output_tokens": 5,
        })()

    def embed(self, text):
        return type("R", (), {"vector": [0.0] * 256})()

    def estimate_tokens(self, text):
        return len(text) // 3

    def get_stats(self):
        return self.stats


# ── CJK gating ───────────────────────────────────────────────────


class TestCJKGating:
    def test_chinese_message_activates_relevant_engram(self, store, mock_llm, mgr):
        mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="脉冲神经网络与STDP学习机制的研究"),
        ])

        clone = _make_clone(store, mock_llm)
        clone.process_message("请介绍脉冲神经网络")

        assert len(clone.get_active_engrams()) >= 1

    def test_chinese_irrelevant_not_activated(self, store, mock_llm, mgr):
        mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="今天晚饭做红烧肉的菜谱"),
        ])

        clone = _make_clone(store, mock_llm)
        clone.process_message("量子力学的基本原理是什么")

        assert len(clone.get_active_engrams()) == 0

    def test_mixed_language_overlap(self, store, mock_llm, mgr):
        mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="关于 LLM 的 context 工程实践"),
        ])

        clone = _make_clone(store, mock_llm)
        clone.process_message("LLM 的 context 怎么管理")

        assert len(clone.get_active_engrams()) >= 1


class TestTokenize:
    def test_english_words(self):
        from pulse_system.core.types import tokenize

        assert tokenize("Hello World hello") == {"hello", "world"}

    def test_cjk_chars_and_bigrams(self):
        from pulse_system.core.types import tokenize

        tokens = tokenize("脉冲计算")
        assert {"脉", "冲", "计", "算"} <= tokens
        assert {"脉冲", "冲计", "计算"} <= tokens

    def test_empty(self):
        from pulse_system.core.types import tokenize

        assert tokenize("") == set()
        assert tokenize("   ,。!") == set()


# ── Diary write-back (v0.4 / 4.3) ────────────────────────────────


class TestDiaryWriteBack:
    def test_exit_diary_appended_to_engram_session(self, store, mock_llm, mgr):
        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="quantum physics topic"),
        ])
        clone = _make_clone(store, _SilentLLM())
        # activate manually, then force silence-based exit
        session = store.get_session(e.id)
        clone._active_engrams[e.id] = ActiveEngram(
            engram_id=e.id, session_snapshot=list(session),
            consecutive_silences=2,  # next silence triggers exit
        )

        clone.process_message("unrelated message causing silence")

        assert e.id not in clone.get_active_engrams()
        updated = store.get_session(e.id)
        diary_msgs = [m for m in updated if m.role == MessageRole.INJECTION]
        assert len(diary_msgs) == 1
        assert "旁听" in diary_msgs[0].content
        assert diary_msgs[0].source_engram_id == f"clone:{clone.id}"


# ── EmbeddingCache (v0.4 / 4.4) ──────────────────────────────────


class TestEmbeddingCache:
    def test_same_content_embeds_once(self, mock_llm):
        from pulse_system.substrate.llm import EmbeddingCache

        calls = {"n": 0}
        orig = mock_llm.embed

        def counting(text):
            calls["n"] += 1
            return orig(text)

        mock_llm.embed = counting
        cache = EmbeddingCache(mock_llm)

        v1 = cache.get("engram1", "same session text")
        v2 = cache.get("engram1", "same session text")
        assert v1 == v2
        assert calls["n"] == 1
        assert cache.hits == 1

    def test_changed_content_reembeds(self, mock_llm):
        from pulse_system.substrate.llm import EmbeddingCache

        cache = EmbeddingCache(mock_llm)
        v1 = cache.get("engram1", "version one")
        v2 = cache.get("engram1", "version two")
        assert v1 != v2
        assert cache.misses == 2

    def test_lru_bound(self, mock_llm):
        from pulse_system.substrate.llm import EmbeddingCache

        cache = EmbeddingCache(mock_llm, max_entries=10)
        for i in range(25):
            cache.get(f"k{i}", f"text {i}")
        assert len(cache._cache) <= 10

    def test_gate_uses_cache(self, store, mock_llm, mgr):
        """Repeated gating of unchanged sessions must not re-embed them."""
        from pulse_system.substrate.llm import EmbeddingCache

        e = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content="stable session content"),
        ])
        cache = EmbeddingCache(mock_llm)
        clone = CloneSession(
            session_id="s1", storage=store, llm=mock_llm,
            embedding_cache=cache,
        )

        embed_calls = {"n": 0}
        orig = mock_llm.embed

        def counting(text):
            embed_calls["n"] += 1
            return orig(text)

        mock_llm.embed = counting

        candidates = store.list_engrams()
        clone._embedding_gate("first message", candidates)
        after_first = embed_calls["n"]
        clone._active_engrams.clear()
        clone._embedding_gate("second message", candidates)
        after_second = embed_calls["n"]

        # each gate embeds the user message (uncached) but the candidate
        # session is served from cache the second time
        assert after_first == 2   # user msg + candidate
        assert after_second == 3  # only the new user msg

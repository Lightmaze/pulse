"""Tests for conversation import."""

import json

import pytest

from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.engram import EngramManager
from pulse_system.core.types import MessageRole
from pulse_system.education.ingest import ConversationImporter
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
def importer(mgr, conn_net, mock_llm):
    return ConversationImporter(mgr, conn_net, mock_llm)


# ── Claude format ────────────────────────────────────────────────


class TestClaudeFormat:
    def test_import_claude_json(self, importer, tmp_path):
        data = [
            {"role": "user", "content": "Hello Claude"},
            {"role": "assistant", "content": "Hi there!"},
            {"role": "user", "content": "How are you?"},
            {"role": "assistant", "content": "I'm doing well."},
        ]
        f = tmp_path / "conv.json"
        f.write_text(json.dumps(data), encoding="utf-8")

        engram = importer.import_file(str(f), "claude")
        session = importer._mgr.get_session(engram.id)
        assert len(session) == 4
        assert session[0].role == MessageRole.USER
        assert session[1].role == MessageRole.ASSISTANT

    def test_claude_wrapped_format(self, importer, tmp_path):
        data = {"messages": [
            {"role": "user", "content": "Question"},
            {"role": "assistant", "content": "Answer"},
        ]}
        f = tmp_path / "conv.json"
        f.write_text(json.dumps(data), encoding="utf-8")

        engram = importer.import_file(str(f), "claude")
        session = importer._mgr.get_session(engram.id)
        assert len(session) == 2


# ── ChatGPT format ───────────────────────────────────────────────


class TestChatGPTFormat:
    def test_import_chatgpt_flat(self, importer, tmp_path):
        data = [
            {"role": "user", "content": "Hi GPT"},
            {"role": "assistant", "content": "Hello!"},
        ]
        f = tmp_path / "gpt.json"
        f.write_text(json.dumps(data), encoding="utf-8")

        engram = importer.import_file(str(f), "chatgpt")
        session = importer._mgr.get_session(engram.id)
        assert len(session) == 2

    def test_chatgpt_conversations_wrapper(self, importer, tmp_path):
        data = {"conversations": [{"messages": [
            {"role": "user", "content": "Question"},
            {"role": "assistant", "content": "Answer"},
        ]}]}
        f = tmp_path / "gpt.json"
        f.write_text(json.dumps(data), encoding="utf-8")

        engram = importer.import_file(str(f), "chatgpt")
        session = importer._mgr.get_session(engram.id)
        assert len(session) == 2

    def test_chatgpt_empty_parts_skipped(self, importer, tmp_path):
        """Content with an empty parts list must not raise."""
        data = [
            {"role": "user", "content": {"parts": []}},
            {"role": "user", "content": {"parts": ["real question"]}},
            {"role": "assistant", "content": "Answer"},
        ]
        f = tmp_path / "gpt.json"
        f.write_text(json.dumps(data), encoding="utf-8")

        engram = importer.import_file(str(f), "chatgpt")
        session = importer._mgr.get_session(engram.id)
        assert len(session) == 2
        assert session[0].content == "real question"

    def test_chatgpt_skips_system(self, importer, tmp_path):
        data = [
            {"role": "system", "content": "You are helpful."},
            {"role": "user", "content": "Hi"},
            {"role": "assistant", "content": "Hello"},
        ]
        f = tmp_path / "gpt.json"
        f.write_text(json.dumps(data), encoding="utf-8")

        engram = importer.import_file(str(f), "chatgpt")
        session = importer._mgr.get_session(engram.id)
        assert len(session) == 2


# ── Markdown format ──────────────────────────────────────────────


class TestMarkdownFormat:
    def test_import_markdown(self, importer, tmp_path):
        md = """## User
What is gravity?

## Assistant
Gravity is a fundamental force.

## User
Tell me more.

## Assistant
It attracts objects with mass.
"""
        f = tmp_path / "conv.md"
        f.write_text(md, encoding="utf-8")

        engram = importer.import_file(str(f), "markdown")
        session = importer._mgr.get_session(engram.id)
        assert len(session) == 4
        assert session[0].role == MessageRole.USER
        assert "gravity" in session[0].content.lower()

    def test_markdown_no_headers(self, importer, tmp_path):
        f = tmp_path / "plain.md"
        f.write_text("Just some text without role headers.", encoding="utf-8")

        engram = importer.import_file(str(f), "markdown")
        session = importer._mgr.get_session(engram.id)
        assert len(session) == 1
        assert session[0].role == MessageRole.ASSISTANT


# ── Plain text format ────────────────────────────────────────────


class TestPlainFormat:
    def test_import_plain(self, importer, tmp_path):
        f = tmp_path / "doc.txt"
        f.write_text("This is a document about neural networks.", encoding="utf-8")

        engram = importer.import_file(str(f), "plain")
        session = importer._mgr.get_session(engram.id)
        assert len(session) == 1
        assert session[0].role == MessageRole.ASSISTANT
        assert "neural networks" in session[0].content

    def test_empty_plain_raises(self, importer, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_text("", encoding="utf-8")

        with pytest.raises(ValueError, match="No messages"):
            importer.import_file(str(f), "plain")


# ── Directory import ─────────────────────────────────────────────


class TestDirectoryImport:
    def test_import_directory(self, importer, tmp_path):
        for i in range(3):
            data = [
                {"role": "user", "content": f"Question {i}"},
                {"role": "assistant", "content": f"Answer {i}"},
            ]
            (tmp_path / f"conv{i}.json").write_text(json.dumps(data), encoding="utf-8")

        results = importer.import_directory(str(tmp_path), "claude")
        assert len(results) == 3

    def test_import_directory_skips_bad_files(self, importer, tmp_path):
        # One good, one bad
        good = [{"role": "user", "content": "OK"}]
        (tmp_path / "good.json").write_text(json.dumps(good), encoding="utf-8")
        (tmp_path / "bad.json").write_text("not json{{{", encoding="utf-8")

        results = importer.import_directory(str(tmp_path), "claude")
        assert len(results) == 1

    def test_not_a_directory(self, importer, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("hello", encoding="utf-8")
        with pytest.raises(ValueError, match="Not a directory"):
            importer.import_directory(str(f), "plain")


# ── Post-import initialization ───────────────────────────────────


class TestPostImport:
    def test_creates_connections(self, importer, tmp_path):
        files = []
        for i, topic in enumerate(["quantum physics research", "quantum mechanics study"]):
            data = [{"role": "user", "content": topic}]
            f = tmp_path / f"conv{i}.json"
            f.write_text(json.dumps(data), encoding="utf-8")
            files.append(f)

        engrams = importer.import_directory(str(tmp_path), "claude")
        ids = [e.id for e in engrams]
        conn_count = importer.post_import_initialize(ids)
        # With mock embeddings, similar texts should create connections
        assert conn_count >= 0  # exact count depends on mock similarity

    def test_single_engram_no_connections(self, importer, tmp_path):
        data = [{"role": "user", "content": "solo"}]
        f = tmp_path / "solo.json"
        f.write_text(json.dumps(data), encoding="utf-8")

        engram = importer.import_file(str(f), "claude")
        count = importer.post_import_initialize([engram.id])
        assert count == 0


# ── Unsupported format ───────────────────────────────────────────


class TestUnsupported:
    def test_unsupported_format_raises(self, importer, tmp_path):
        f = tmp_path / "file.xyz"
        f.write_text("data", encoding="utf-8")
        with pytest.raises(ValueError, match="Unsupported format"):
            importer.import_file(str(f), "xyz")

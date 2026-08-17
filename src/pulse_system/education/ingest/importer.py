"""Conversation import — bring external dialogues into the engram network.

Supports Claude JSON, ChatGPT JSON, Markdown, and plain text formats.
Each conversation file becomes one engram.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING

from pulse_system.core.types import Connection, Message, MessageRole

if TYPE_CHECKING:
    from pulse_system.core.connection.network import ConnectionNetwork
    from pulse_system.core.engram.manager import EngramManager
    from pulse_system.substrate.llm.adapter import LLMAdapter


_SUPPORTED_FORMATS = {"claude", "chatgpt", "markdown", "plain"}

_MD_ROLE_PATTERN = re.compile(
    r"^##\s*(User|Assistant|user|assistant)\s*$", re.MULTILINE
)


class ConversationImporter:
    """Import external conversations as engrams."""

    def __init__(
        self,
        engram_manager: EngramManager,
        connection_network: ConnectionNetwork,
        llm: LLMAdapter,
    ):
        self._mgr = engram_manager
        self._connections = connection_network
        self._llm = llm

    def import_file(
        self,
        filepath: str | Path,
        format: str,
        project_id: str | None = None,
    ):
        """Import a single conversation file as an engram."""
        if format not in _SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {format}. Use one of {_SUPPORTED_FORMATS}")

        filepath = Path(filepath)
        messages = self._parse_file(filepath, format)
        if not messages:
            raise ValueError(f"No messages parsed from {filepath}")

        return self._mgr.import_conversation(messages, project_id=project_id)

    def import_directory(
        self,
        dirpath: str | Path,
        format: str,
        project_id: str | None = None,
    ):
        """Import all conversation files from a directory."""
        dirpath = Path(dirpath)
        if not dirpath.is_dir():
            raise ValueError(f"Not a directory: {dirpath}")

        ext_map = {
            "claude": ".json",
            "chatgpt": ".json",
            "markdown": ".md",
            "plain": ".txt",
        }
        ext = ext_map.get(format, ".*")

        results = []
        for f in sorted(dirpath.iterdir()):
            if f.is_file() and (ext == ".*" or f.suffix == ext):
                try:
                    engram = self.import_file(f, format, project_id)
                    results.append(engram)
                except (ValueError, json.JSONDecodeError):
                    continue
        return results

    def post_import_initialize(self, engram_ids: list[str]) -> int:
        """Compute embeddings and create initial connections between imported engrams."""
        connections = self._connections.initialize_from_embeddings(engram_ids, self._llm)
        return len(connections)

    # ── Parsers ──────────────────────────────────────────────────

    def _parse_file(self, filepath: Path, format: str) -> list[Message]:
        text = filepath.read_text(encoding="utf-8")
        if format == "claude":
            return self._parse_claude(text)
        elif format == "chatgpt":
            return self._parse_chatgpt(text)
        elif format == "markdown":
            return self._parse_markdown(text)
        elif format == "plain":
            return self._parse_plain(text)
        return []

    @staticmethod
    def _parse_claude(text: str) -> list[Message]:
        """Parse Claude export JSON: array of {role, content}."""
        data = json.loads(text)
        messages_data = data if isinstance(data, list) else data.get("messages", [])
        messages = []
        for item in messages_data:
            role_str = item.get("role", "").lower()
            content = item.get("content", "")
            if not content:
                continue
            if role_str == "user":
                role = MessageRole.USER
            elif role_str in ("assistant", "ai"):
                role = MessageRole.ASSISTANT
            else:
                continue
            messages.append(Message(role=role, content=content))
        return messages

    @staticmethod
    def _parse_chatgpt(text: str) -> list[Message]:
        """Parse ChatGPT export JSON: {conversations: [{messages: [...]}]} or flat messages."""
        data = json.loads(text)

        if isinstance(data, list):
            raw_messages = data
        elif "messages" in data:
            raw_messages = data["messages"]
        elif "conversations" in data:
            convos = data["conversations"]
            if convos:
                raw_messages = convos[0].get("messages", [])
            else:
                return []
        else:
            return []

        messages = []
        for item in raw_messages:
            role_str = str(item.get("role", "")).lower()
            content = item.get("content", "")
            if isinstance(content, dict):
                if "parts" in content:
                    parts = content["parts"]
                    content = parts[0] if parts else ""
                else:
                    content = str(content)
            if not content or not isinstance(content, str):
                continue
            if role_str == "user":
                role = MessageRole.USER
            elif role_str in ("assistant", "ai", "chatgpt"):
                role = MessageRole.ASSISTANT
            elif role_str == "system":
                continue
            else:
                continue
            messages.append(Message(role=role, content=content))
        return messages

    @staticmethod
    def _parse_markdown(text: str) -> list[Message]:
        """Parse markdown with ## User / ## Assistant sections."""
        parts = _MD_ROLE_PATTERN.split(text)
        if len(parts) < 2:
            return [Message(role=MessageRole.ASSISTANT, content=text.strip())] if text.strip() else []

        messages = []
        i = 1
        while i < len(parts) - 1:
            role_str = parts[i].strip().lower()
            content = parts[i + 1].strip()
            if content:
                role = MessageRole.USER if role_str == "user" else MessageRole.ASSISTANT
                messages.append(Message(role=role, content=content))
            i += 2
        return messages

    @staticmethod
    def _parse_plain(text: str) -> list[Message]:
        """Plain text → single assistant message."""
        content = text.strip()
        if not content:
            return []
        return [Message(role=MessageRole.ASSISTANT, content=content)]

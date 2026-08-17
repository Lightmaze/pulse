"""Clone front-stage agent.

A clone session is a conversation that runs separately from engrams.
Active engrams can be gated in as read-only observers — they contribute
when relevant and stay silent when not.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pulse_system.core.types import Message, MessageRole
from pulse_system.substrate.llm.adapter import LLMAdapter
from pulse_system.substrate.storage.store import Storage


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _uuid() -> str:
    return uuid.uuid4().hex[:16]


_STEER_PROMPT = (
    "你正在旁听一段对话。如果你认为自己有相关的见解可以贡献，"
    "请分享。如果与你无关或你没有要补充的，请回复'[沉默]'。"
)

_SILENCE_MARKERS = ("[沉默]", "[无需介入]", "[silence]")

_MAX_SILENCE_BEFORE_EXIT = 3


@dataclass
class CloneMessage:
    role: str  # "user", "clone", "engram"
    content: str
    timestamp: datetime = field(default_factory=_now)
    source_engram_id: str | None = None


@dataclass
class ActiveEngram:
    engram_id: str
    session_snapshot: list[Message]
    consecutive_silences: int = 0


@dataclass
class CloneSessionConfig:
    activation_threshold: float = 0.3
    max_active_engrams: int = 5


class CloneSession:
    """A clone conversation session with engram observer gating."""

    def __init__(
        self,
        session_id: str,
        storage: Storage,
        llm: LLMAdapter,
        *,
        topic: str | None = None,
        config: CloneSessionConfig | None = None,
        embedding_cache: "EmbeddingCache | None" = None,
        library=None,
    ):
        self._id = session_id
        self._storage = storage
        self._llm = llm
        self._topic = topic
        self._config = config or CloneSessionConfig()
        self._embedding_cache = embedding_cache
        self._library = library  # optional Library for diary write-back
        self._history: list[CloneMessage] = []
        self._active_engrams: dict[str, ActiveEngram] = {}
        self._diary_entries: list[tuple[str, str]] = []

    @property
    def id(self) -> str:
        return self._id

    @property
    def topic(self) -> str | None:
        return self._topic

    def process_message(self, user_message: str) -> str:
        """Full clone message processing pipeline."""
        # 1. Append user message
        self._history.append(CloneMessage(role="user", content=user_message))

        # 2. Activation gating
        self._activation_gate(user_message)

        # 3. Steer each active engram
        to_remove = []
        for eid, active in self._active_engrams.items():
            contribution = self._steer_engram(active)
            if contribution is not None:
                self._history.append(CloneMessage(
                    role="engram",
                    content=contribution,
                    source_engram_id=eid,
                ))
                active.consecutive_silences = 0
            else:
                active.consecutive_silences += 1

            # 6. Exit condition check
            if active.consecutive_silences >= _MAX_SILENCE_BEFORE_EXIT:
                self._write_exit_diary(active)
                to_remove.append(eid)

        for eid in to_remove:
            del self._active_engrams[eid]

        # 4. Clone self-response
        response = self._clone_respond()

        # 5. Append response
        self._history.append(CloneMessage(role="clone", content=response))

        return response

    def get_history(self) -> list[CloneMessage]:
        return list(self._history)

    def get_active_engrams(self) -> list[str]:
        return list(self._active_engrams.keys())

    def finalize(self) -> dict:
        """End session. Returns summary of cognitive changes."""
        return {
            "session_id": self._id,
            "topic": self._topic,
            "message_count": len(self._history),
            "engrams_activated": len(self._diary_entries),
            "diary_entries": list(self._diary_entries),
        }

    # ── Activation gating ────────────────────────────────────────

    def _activation_gate(self, user_message: str) -> None:
        """Gate engrams into the conversation based on relevance."""
        if len(self._active_engrams) >= self._config.max_active_engrams:
            return

        from pulse_system.core.types import EngramStatus
        engrams = self._storage.list_engrams(status=EngramStatus.ACTIVE)

        already_active = set(self._active_engrams.keys())
        candidates = [e for e in engrams if e.id not in already_active]

        if not candidates:
            return

        if self._llm.mock:
            self._keyword_gate(user_message, candidates)
        else:
            self._embedding_gate(user_message, candidates)

    def _keyword_gate(self, user_message: str, candidates: list) -> None:
        """Mock-mode gating: activate on token overlap with the engram session.

        Uses the shared tokenizer (words + CJK characters/bigrams) so gating
        works for Chinese input, which whitespace splitting cannot segment.
        """
        from pulse_system.core.types import tokenize

        user_tokens = tokenize(user_message)
        if not user_tokens:
            return

        for engram in candidates:
            if len(self._active_engrams) >= self._config.max_active_engrams:
                break
            session = self._storage.get_session(engram.id)
            session_text = " ".join(m.content for m in session)
            overlap = user_tokens & tokenize(session_text)
            if len(overlap) >= 2 or (len(overlap) >= 1 and len(user_tokens) <= 3):
                snapshot = list(session)
                self._active_engrams[engram.id] = ActiveEngram(
                    engram_id=engram.id,
                    session_snapshot=snapshot,
                )

    def _embedding_gate(self, user_message: str, candidates: list) -> None:
        """Real-mode gating: cosine similarity between user message and engram sessions.

        Candidate embeddings are served from the shared EmbeddingCache when
        available — sessions change slowly, so repeated gating of the same
        engram costs no extra embedding calls until its session changes.
        """
        user_embed = self._llm.embed(user_message).vector

        for engram in candidates:
            if len(self._active_engrams) >= self._config.max_active_engrams:
                break
            session = self._storage.get_session(engram.id)
            if not session:
                continue
            session_text = " ".join(m.content for m in session[-5:])
            if self._embedding_cache is not None:
                engram_embed = self._embedding_cache.get(engram.id, session_text)
            else:
                engram_embed = self._llm.embed(session_text).vector

            sim = _cosine_similarity(user_embed, engram_embed)
            if sim >= self._config.activation_threshold:
                self._active_engrams[engram.id] = ActiveEngram(
                    engram_id=engram.id,
                    session_snapshot=list(session),
                )

    # ── Steer ────────────────────────────────────────────────────

    def _steer_engram(self, active: ActiveEngram) -> str | None:
        """Ask an engram snapshot whether it has something to contribute."""
        messages: list[dict[str, str]] = []

        for m in active.session_snapshot:
            role = m.role.value
            if role == "injection":
                role = "user"
            messages.append({"role": role, "content": m.content})

        recent_history = self._history[-6:]
        history_text = "\n".join(
            f"[{m.role}]: {m.content}" for m in recent_history
        )
        messages.append({
            "role": "user",
            "content": f"{history_text}\n\n{_STEER_PROMPT}",
        })

        result = self._llm.complete(messages)
        output = result.content.strip()

        if not output:
            return None
        for marker in _SILENCE_MARKERS:
            if output.startswith(marker):
                return None

        return output

    # ── Clone response ───────────────────────────────────────────

    def _clone_respond(self) -> str:
        """Generate clone's own response from full conversation history."""
        messages: list[dict[str, str]] = []
        for m in self._history:
            if m.role == "user":
                messages.append({"role": "user", "content": m.content})
            elif m.role == "clone":
                messages.append({"role": "assistant", "content": m.content})
            elif m.role == "engram":
                label = f"(engram {m.source_engram_id})" if m.source_engram_id else ""
                messages.append({"role": "user", "content": f"{label} {m.content}"})

        if not messages:
            messages.append({"role": "user", "content": "(conversation start)"})

        result = self._llm.complete(messages)
        return result.content

    # ── Diary ────────────────────────────────────────────────────

    def _write_exit_diary(self, active: ActiveEngram) -> None:
        """Write an observation diary entry when an engram exits.

        The diary goes to the Engram's Library —
        the main session stays clean, holding only the engram's own
        cognition. Without a library the entry falls back to a session
        INJECTION so the observation is not lost.
        """
        recent = self._history[-5:]
        summary = "; ".join(m.content[:50] for m in recent)
        diary = f"旁听对话后退场。最近话题：{summary}"
        self._diary_entries.append((active.engram_id, diary))
        if self._library is not None:
            self._library.append_diary(
                active.engram_id, diary, source=f"clone:{self._id}"
            )
        else:
            self._storage.append_message(
                active.engram_id,
                Message(
                    role=MessageRole.INJECTION,
                    content=diary,
                    source_engram_id=f"clone:{self._id}",
                ),
            )


class CloneManager:
    """Manages multiple clone sessions (sharing one embedding cache)."""

    def __init__(self, storage: Storage, llm: LLMAdapter, library=None):
        from pulse_system.substrate.llm import EmbeddingCache

        self._storage = storage
        self._llm = llm
        self._library = library
        self._sessions: dict[str, CloneSession] = {}
        self._embedding_cache = EmbeddingCache(llm)

    def create_session(
        self, topic: str | None = None, config: CloneSessionConfig | None = None
    ) -> CloneSession:
        sid = _uuid()
        session = CloneSession(
            session_id=sid,
            storage=self._storage,
            llm=self._llm,
            topic=topic,
            config=config,
            embedding_cache=self._embedding_cache,
            library=self._library,
        )
        self._sessions[sid] = session
        return session

    def get_session(self, session_id: str) -> CloneSession | None:
        return self._sessions.get(session_id)

    def list_sessions(self) -> list[CloneSession]:
        return list(self._sessions.values())


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)

"""Lightweight text tokenization shared across modules.

Whitespace splitting alone cannot segment CJK text (no spaces), which made
keyword-overlap gating silently inert for Chinese input. This tokenizer
combines alphanumeric words with CJK single characters and character
bigrams — no external segmenter required.
"""

from __future__ import annotations

import re

_WORD_RE = re.compile(r"[a-zA-Z0-9]+")
_CJK_RE = re.compile(r"[一-鿿]")
SESSION_NAME_MAX = 60


def tokenize(text: str) -> set[str]:
    """Tokenize text into a set of comparable units.

    - Latin/digit runs are lowercased words.
    - Each CJK character is a token, plus every adjacent-character bigram
      (bigrams approximate Chinese words well enough for overlap gating).
    """
    tokens = set(w.lower() for w in _WORD_RE.findall(text))
    chars = _CJK_RE.findall(text)
    tokens.update(chars)
    tokens.update(a + b for a, b in zip(chars, chars[1:]))
    return tokens


def session_name(text: str, max_chars: int = SESSION_NAME_MAX) -> str | None:
    """Derive a compact session name from content without changing language.

    This is deliberately deterministic for v0.1: the first effective content
    names the session, independent of the GUI locale and without requiring an
    LLM call. User renames are stored separately and are never regenerated.
    """
    collapsed = " ".join(text.split())
    if not collapsed:
        return None
    if len(collapsed) <= max_chars:
        return collapsed
    return collapsed[: max_chars - 1] + "…"

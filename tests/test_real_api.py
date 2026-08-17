"""Real-API integration checks (excluded from the default test run).

Run explicitly with a key present:

    DEEPSEEK_API_KEY=sk-...  uv run pytest tests/test_real_api.py -m real -v
"""

import os

import pytest

from pulse_system.substrate.llm import LLMAdapter

_HAS_KEY = bool(os.environ.get("DEEPSEEK_API_KEY"))

pytestmark = [
    pytest.mark.real,
    pytest.mark.skipif(not _HAS_KEY, reason="DEEPSEEK_API_KEY not set"),
]


def test_complete_reports_usage_and_cache_fields():
    llm = LLMAdapter(provider="deepseek", max_tokens=32)
    messages = [
        {"role": "user", "content": "Reply with the single word: pulse"},
    ]

    r1 = llm.complete(messages)
    assert r1.content.strip()
    assert r1.input_tokens > 0
    assert r1.output_tokens > 0
    assert r1.cached_tokens >= 0
    assert r1.cached_tokens <= r1.input_tokens

    # Extend the same prefix — DeepSeek context caching should report hits
    # once the shared prefix exceeds one 64-token block. Not asserted hard
    # (cache residency is best-effort), but the field must parse.
    messages2 = messages + [
        {"role": "assistant", "content": r1.content},
        {"role": "user", "content": "Now reply with the single word: echo"},
    ]
    r2 = llm.complete(messages2)
    assert r2.cached_tokens >= 0

    stats = llm.get_stats()
    assert stats.total_calls == 2
    assert stats.total_input_tokens == r1.input_tokens + r2.input_tokens

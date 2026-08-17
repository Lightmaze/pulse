from __future__ import annotations

import pytest


@pytest.fixture
def production_harness_test_provider_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> str:
    """Install a deterministic, non-secret credential for offline fixtures."""
    value = "test-only-not-a-provider-secret"
    monkeypatch.setenv("DEEPSEEK_API_KEY", value)
    return value

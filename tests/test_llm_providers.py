"""The provider table: every declared provider must be usable, and must say
the true thing when it is not.

Two failures these tests exist to prevent:

1. A provider is listed but its profile is incomplete or its base_url is a
   placeholder, so the table advertises a substrate that cannot be reached.
2. A provider without a key refuses by naming OPENAI_API_KEY — the OpenAI
   client's default error — which sends someone to set a variable that this
   provider never reads.

Every test here runs offline and with no key in the environment. Where a key
would change the outcome, the variable is deleted explicitly so the result
does not depend on the caller's shell.
"""

from __future__ import annotations

import os

import pytest

from pulse_system.substrate.llm import LLMAdapter, LLMCallError
from pulse_system.substrate.llm.adapter import (
    _DEFAULT_EMBED_MODEL,
    _PROVIDER_PROFILES,
)

ALL_PROVIDERS = sorted(_PROVIDER_PROFILES)
KEYED_PROVIDERS = sorted(n for n, p in _PROVIDER_PROFILES.items() if p["api_key_env"])
KEYLESS_PROVIDERS = sorted(
    n for n, p in _PROVIDER_PROFILES.items() if not p["api_key_env"]
)


@pytest.fixture
def no_keys(monkeypatch: pytest.MonkeyPatch):
    """Strip every provider key the table knows about."""
    for profile in _PROVIDER_PROFILES.values():
        if profile["api_key_env"]:
            monkeypatch.delenv(profile["api_key_env"], raising=False)


# ── The table itself ─────────────────────────────────────────────


class TestTableCompleteness:
    def test_enough_providers_that_a_stranger_has_one(self):
        """Six is the floor: below it, running this project means opening an
        account rather than reusing a key you already hold."""
        assert len(_PROVIDER_PROFILES) >= 6, sorted(_PROVIDER_PROFILES)

    @pytest.mark.parametrize("name", ALL_PROVIDERS)
    def test_profile_carries_every_field(self, name: str):
        profile = _PROVIDER_PROFILES[name]
        missing = {
            "base_url",
            "model",
            "api_key_env",
            "cache_read_discount",
            "embed_model",
        } - set(profile)
        assert not missing, f"{name} lacks {sorted(missing)}"

    @pytest.mark.parametrize("name", ALL_PROVIDERS)
    def test_base_url_is_a_real_endpoint(self, name: str):
        base_url = _PROVIDER_PROFILES[name]["base_url"]
        assert base_url.startswith(("https://", "http://")), base_url
        # Placeholders are the failure mode this catches: a table entry that
        # looks complete and resolves to nothing.
        assert "example.com" not in base_url
        assert "<" not in base_url and "{" not in base_url, (
            f"{name} base_url is a template, not an endpoint: {base_url}"
        )

    @pytest.mark.parametrize("name", ALL_PROVIDERS)
    def test_model_is_named(self, name: str):
        model = _PROVIDER_PROFILES[name]["model"]
        assert isinstance(model, str) and model.strip()

    @pytest.mark.parametrize("name", ALL_PROVIDERS)
    def test_cache_discount_is_a_ratio(self, name: str):
        """A discount is a price ratio in (0, 1]. 1.0 means none was confirmed
        from the provider's own pricing page — the pessimistic direction, which
        makes the budget under-spend rather than over-spend."""
        d = _PROVIDER_PROFILES[name]["cache_read_discount"]
        assert isinstance(d, (int, float)) and not isinstance(d, bool)
        assert 0.0 < d <= 1.0, f"{name}: {d}"

    @pytest.mark.parametrize("name", ALL_PROVIDERS)
    def test_embed_model_is_a_name_or_honestly_absent(self, name: str):
        em = _PROVIDER_PROFILES[name]["embed_model"]
        assert em is None or (isinstance(em, str) and em.strip())

    @pytest.mark.parametrize("name", ALL_PROVIDERS)
    def test_api_key_env_is_always_a_string(self, name: str):
        """Keyless providers use "" and never None. The observatory's
        /substrates endpoint passes this value straight to os.environ.get(),
        which raises TypeError on None — so the no-key marker has to be falsy
        *and* a str. Callers must test it for falsiness, never `is None`."""
        env = _PROVIDER_PROFILES[name]["api_key_env"]
        assert isinstance(env, str), f"{name}: {env!r}"
        assert os.environ.get(env) is None or isinstance(os.environ.get(env), str)

    def test_env_var_names_are_distinct_per_provider(self):
        """Two providers sharing a variable means setting one silently arms the
        other, and the refusal message stops identifying anything."""
        named = [p["api_key_env"] for p in _PROVIDER_PROFILES.values() if p["api_key_env"]]
        assert len(named) == len(set(named)), sorted(named)


# ── Mock construction: every provider runs offline ───────────────


class TestEveryProviderRunsInMock:
    @pytest.mark.parametrize("name", ALL_PROVIDERS)
    def test_constructs_and_completes(self, name: str, no_keys):
        llm = LLMAdapter(mock=True, provider=name)
        result = llm.complete([{"role": "user", "content": "x"}])
        assert result.content
        assert result.input_tokens > 0
        assert llm.provider == name
        assert llm.model == _PROVIDER_PROFILES[name]["model"]

    @pytest.mark.parametrize("name", ALL_PROVIDERS)
    def test_carries_its_own_cache_discount(self, name: str):
        llm = LLMAdapter(mock=True, provider=name)
        assert llm.cache_read_discount == pytest.approx(
            _PROVIDER_PROFILES[name]["cache_read_discount"]
        )

    @pytest.mark.parametrize("name", ALL_PROVIDERS)
    def test_embeds_in_mock(self, name: str):
        llm = LLMAdapter(mock=True, provider=name)
        assert len(llm.embed("hello").vector) == 256

    @pytest.mark.parametrize("name", ALL_PROVIDERS)
    def test_embed_model_resolves_to_something(self, name: str):
        llm = LLMAdapter(mock=True, provider=name)
        expected = _PROVIDER_PROFILES[name]["embed_model"] or _DEFAULT_EMBED_MODEL
        assert llm.embed_model == expected

    def test_gemini_gets_its_own_embedding_model(self):
        """A provider with a verified embeddings endpoint must not inherit
        OpenAI's model name, which its own base_url does not serve."""
        assert LLMAdapter(mock=True, provider="gemini").embed_model != _DEFAULT_EMBED_MODEL

    def test_explicit_embed_model_still_wins(self):
        llm = LLMAdapter(mock=True, provider="gemini", embed_model="custom-embed")
        assert llm.embed_model == "custom-embed"


# ── Refusal names the variable that would actually work ──────────


class TestRefusalNamesTheRightVariable:
    @pytest.mark.parametrize("name", KEYED_PROVIDERS)
    def test_refuses_without_a_key(self, name: str, no_keys):
        with pytest.raises(LLMCallError):
            LLMAdapter(provider=name)

    @pytest.mark.parametrize("name", KEYED_PROVIDERS)
    def test_refusal_names_this_providers_variable(self, name: str, no_keys):
        env = _PROVIDER_PROFILES[name]["api_key_env"]
        with pytest.raises(LLMCallError) as exc:
            LLMAdapter(provider=name)
        assert env in str(exc.value), (
            f"{name} refused without naming {env}; the message was: {exc.value}"
        )

    @pytest.mark.parametrize("name", KEYED_PROVIDERS)
    def test_refusal_does_not_misname_openai(self, name: str, no_keys):
        """The OpenAI client's own error names OPENAI_API_KEY for every
        provider. That message sent people to set the wrong variable, watch it
        fail identically, and conclude the project was broken."""
        if name == "openai":
            pytest.skip("OPENAI_API_KEY is the right answer for openai")
        with pytest.raises(LLMCallError) as exc:
            LLMAdapter(provider=name)
        assert "OPENAI_API_KEY" not in str(exc.value)

    @pytest.mark.parametrize("name", KEYED_PROVIDERS)
    def test_refusal_states_the_offline_way_out(self, name: str, no_keys):
        """A refusal must say what would work — this project's standing rule."""
        with pytest.raises(LLMCallError) as exc:
            LLMAdapter(provider=name)
        assert "mock=True" in str(exc.value)

    @pytest.mark.parametrize("name", KEYED_PROVIDERS)
    def test_the_named_variable_is_the_one_actually_read(
        self, name: str, no_keys, monkeypatch: pytest.MonkeyPatch
    ):
        """Naming a variable that the adapter does not read would be a more
        convincing lie than naming none. Set exactly it and the client builds —
        no network is touched by construction."""
        monkeypatch.setenv(_PROVIDER_PROFILES[name]["api_key_env"], "sk-test-not-real")
        llm = LLMAdapter(provider=name)
        assert llm._client is not None

    @pytest.mark.parametrize("name", KEYED_PROVIDERS)
    def test_explicit_key_bypasses_the_environment(self, name: str, no_keys):
        assert LLMAdapter(provider=name, api_key="sk-test-not-real")._client is not None

    def test_mock_mode_never_refuses(self, no_keys):
        """Mock mode must be reachable with no key for every provider, or the
        offline path is a claim rather than a fact."""
        for name in ALL_PROVIDERS:
            LLMAdapter(mock=True, provider=name)


# ── The keyless local provider ───────────────────────────────────


class TestKeylessLocalProvider:
    def test_the_table_offers_a_provider_needing_no_account(self):
        """Someone with no API key at all must still have a way to run this
        against a real model, not only against the mock."""
        assert KEYLESS_PROVIDERS, sorted(_PROVIDER_PROFILES)

    @pytest.mark.parametrize("name", KEYLESS_PROVIDERS)
    def test_builds_instead_of_refusing(self, name: str, no_keys):
        llm = LLMAdapter(provider=name)
        assert llm._client is not None
        assert llm.provider == name

    @pytest.mark.parametrize("name", KEYLESS_PROVIDERS)
    def test_points_at_a_local_server(self, name: str):
        base_url = _PROVIDER_PROFILES[name]["base_url"]
        assert "localhost" in base_url or "127.0.0.1" in base_url, base_url

    @pytest.mark.parametrize("name", KEYLESS_PROVIDERS)
    def test_still_honours_an_explicit_key(self, name: str, no_keys):
        """Some people put a proxy in front of their local server."""
        llm = LLMAdapter(provider=name, api_key="sk-proxy-token")
        assert llm._client is not None


# ── Unknown providers ────────────────────────────────────────────


class TestUnknownProvider:
    def test_unknown_provider_lists_the_real_ones(self):
        with pytest.raises(ValueError) as exc:
            LLMAdapter(mock=True, provider="not-a-provider")
        message = str(exc.value)
        for name in ALL_PROVIDERS:
            assert name in message

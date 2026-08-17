"""Tests for v0.3 real tool implementations and the CLI entry point."""

import subprocess
import sys

import pytest

from pulse_system.agent.tools import html_to_text, make_tavily_search
from pulse_system.agent.tools import real as real_tools
from pulse_system.agent.tools.real import real_web_fetch


class TestHtmlToText:
    def test_strips_tags_and_scripts(self):
        html = (
            "<html><head><style>body{color:red}</style>"
            "<script>alert('x')</script></head>"
            "<body><h1>Title</h1><p>Hello <b>world</b>.</p></body></html>"
        )
        text = html_to_text(html)
        assert "Title" in text
        assert "Hello world" in text
        assert "alert" not in text
        assert "color:red" not in text
        assert "<" not in text

    def test_entities_decoded(self):
        assert html_to_text("<p>a &amp; b &lt;c&gt;</p>") == "a & b <c>"

    def test_block_tags_become_newlines(self):
        text = html_to_text("<p>one</p><p>two</p>")
        assert "one" in text and "two" in text
        assert text.index("one") < text.index("two")


class _FakeResponse:
    def __init__(
        self, *, text="", json_data=None, content_type="text/html",
        status_code=200, location=None,
    ):
        self.text = text
        self._json = json_data
        self.status_code = status_code
        self.headers = {"content-type": content_type}
        if location is not None:
            self.headers["location"] = location

    def raise_for_status(self):
        pass

    def json(self):
        return self._json


@pytest.fixture
def public_dns(monkeypatch):
    """Resolve every host to a public IP so web_fetch's SSRF guard passes
    without touching the network (SSRF tests override this per-case)."""
    monkeypatch.setattr(real_tools, "_resolve_host", lambda host: ["93.184.216.34"])


class TestRealWebFetch:
    def test_fetch_html(self, monkeypatch, public_dns):
        import httpx

        monkeypatch.setattr(
            httpx, "get",
            lambda url, **kw: _FakeResponse(text="<p>page body</p>"),
        )
        result = real_web_fetch("https://example.com")
        assert result.success
        assert "page body" in result.content

    def test_fetch_failure_returns_error_result(self, monkeypatch, public_dns):
        import httpx

        def boom(url, **kw):
            raise httpx.ConnectError("no route")

        monkeypatch.setattr(httpx, "get", boom)
        result = real_web_fetch("https://unreachable.example")
        assert not result.success
        assert "fetch failed" in result.error

    def test_scheme_added_when_missing(self, monkeypatch, public_dns):
        import httpx

        seen = {}

        def record(url, **kw):
            seen["url"] = url
            return _FakeResponse(text="<p>ok</p>")

        monkeypatch.setattr(httpx, "get", record)
        real_web_fetch("example.com/page")
        assert seen["url"].startswith("https://")


class TestWebFetchSSRF:
    """web_fetch must refuse internal targets (no network in these tests —
    IP literals resolve locally; hostnames use monkeypatched DNS)."""

    def _no_request(self, monkeypatch):
        import httpx

        def fail(url, **kw):
            raise AssertionError(f"httpx.get must not run for blocked {url}")

        monkeypatch.setattr(httpx, "get", fail)

    @pytest.mark.parametrize("target", [
        "http://127.0.0.1/",           # loopback
        "http://169.254.169.254/latest/meta-data/",  # cloud metadata
        "http://[::1]/",               # loopback v6
        "http://10.0.0.5/admin",       # RFC1918 private
        "http://192.168.1.1/",         # RFC1918 private
        "http://0.0.0.0/",             # unspecified
    ])
    def test_internal_ip_literals_blocked(self, monkeypatch, target):
        # No DNS override: these are literals and must be rejected pre-request.
        self._no_request(monkeypatch)
        result = real_web_fetch(target)
        assert not result.success
        assert "blocked" in result.error

    def test_hostname_resolving_internal_blocked(self, monkeypatch):
        self._no_request(monkeypatch)
        monkeypatch.setattr(
            real_tools, "_resolve_host", lambda host: ["10.1.2.3"]
        )
        result = real_web_fetch("http://intranet.local/")
        assert not result.success
        assert "blocked" in result.error

    def test_redirect_to_internal_blocked(self, monkeypatch):
        import httpx

        # First hop is public and 302s to the metadata IP; the second hop's
        # host must be re-validated and rejected.
        monkeypatch.setattr(
            real_tools, "_resolve_host",
            lambda host: ["93.184.216.34"] if host == "safe.example"
            else ["169.254.169.254"],
        )

        def get(url, **kw):
            assert "169.254" not in url, "must not request the internal hop"
            return _FakeResponse(
                status_code=302, location="http://169.254.169.254/latest/",
            )

        monkeypatch.setattr(httpx, "get", get)
        result = real_web_fetch("https://safe.example/")
        assert not result.success
        assert "blocked" in result.error

    def test_redirect_disabled_flag_passed(self, monkeypatch, public_dns):
        import httpx

        seen = {}

        def get(url, **kw):
            seen["follow"] = kw.get("follow_redirects")
            return _FakeResponse(text="<p>ok</p>")

        monkeypatch.setattr(httpx, "get", get)
        real_web_fetch("https://safe.example/")
        assert seen["follow"] is False

    def test_redirect_loop_capped(self, monkeypatch, public_dns):
        import httpx

        # Always redirects to a public host → should stop after the cap.
        monkeypatch.setattr(
            httpx, "get",
            lambda url, **kw: _FakeResponse(
                status_code=302, location="https://safe.example/next",
            ),
        )
        result = real_web_fetch("https://safe.example/")
        assert not result.success
        assert "too many redirects" in result.error


class TestTavilySearch:
    def test_results_formatted(self, monkeypatch):
        import httpx

        payload = {"results": [
            {"title": "One", "url": "https://a", "content": "first snippet"},
            {"title": "Two", "url": "https://b", "content": "second snippet"},
        ]}
        monkeypatch.setattr(
            httpx, "post", lambda url, **kw: _FakeResponse(json_data=payload)
        )
        search = make_tavily_search("key")
        result = search(query="pulse computing")
        assert result.success
        assert "One" in result.content and "https://a" in result.content
        assert "second snippet" in result.content

    def test_empty_results(self, monkeypatch):
        import httpx

        monkeypatch.setattr(
            httpx, "post", lambda url, **kw: _FakeResponse(json_data={"results": []})
        )
        search = make_tavily_search("key")
        result = search(query="nothing")
        assert result.success
        assert "No results" in result.content

    def test_failure_returns_error_result(self, monkeypatch):
        import httpx

        def boom(url, **kw):
            raise httpx.HTTPError("500")

        monkeypatch.setattr(httpx, "post", boom)
        search = make_tavily_search("key")
        result = search(query="x")
        assert not result.success


class TestCLI:
    def test_mock_repl_end_to_end(self, tmp_path):
        proc = subprocess.run(
            [sys.executable, "-m", "pulse_system", "--mock"],
            input="/status\nhello\n/quit\n",
            capture_output=True, text=True, timeout=90,
            encoding="utf-8",
            cwd=tmp_path,
        )
        assert proc.returncode == 0, proc.stderr
        assert "engine running" in proc.stdout
        assert "front>" in proc.stdout
        assert "engine stopped" in proc.stdout
        assert (tmp_path / ".pulse" / "library").is_dir()

    def test_unknown_command_is_handled(self, tmp_path):
        proc = subprocess.run(
            [sys.executable, "-m", "pulse_system", "--mock"],
            input="/bogus\n/quit\n",
            capture_output=True, text=True, timeout=90,
            encoding="utf-8",
            cwd=tmp_path,
        )
        assert proc.returncode == 0, proc.stderr
        assert "unknown command" in proc.stdout
        assert (tmp_path / ".pulse" / "library").is_dir()

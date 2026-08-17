"""Real implementations for network tools (v0.3 / 3.5).

These are not auto-registered. Callers opt in explicitly:

    registry.register("web_fetch", "Read the content of a web page", real_web_fetch)
    registry.register("web_search", "Search the web", make_tavily_search(api_key))

Results are returned as natural-language text (the free-context rule); failures come back
as ToolResult(success=False) so the front agent can react instead of crashing.
"""

from __future__ import annotations

import ipaddress
import logging
import re
import socket
from urllib.parse import urljoin, urlsplit

from .registry import ToolFunction, ToolResult

_logger = logging.getLogger("pulse_system.tools")

_MAX_FETCH_CHARS = 8_000
_MAX_REDIRECTS = 5
_TAVILY_ENDPOINT = "https://api.tavily.com/search"
_REDIRECT_CODES = (301, 302, 303, 307, 308)

_SCRIPT_STYLE_RE = re.compile(
    r"<(script|style|noscript)\b[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL
)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t\r\f\v]+")
_BLANK_LINES_RE = re.compile(r"\n\s*\n+")
_BLOCK_TAG_RE = re.compile(
    r"</?(p|div|br|li|ul|ol|h[1-6]|tr|table|section|article|header|footer)\b[^>]*>",
    re.IGNORECASE,
)


def html_to_text(html: str) -> str:
    """Crude but dependency-free HTML → text extraction."""
    text = _SCRIPT_STYLE_RE.sub(" ", html)
    text = _BLOCK_TAG_RE.sub("\n", text)
    text = _TAG_RE.sub(" ", text)
    # minimal entity handling for readability
    for entity, char in (
        ("&nbsp;", " "), ("&amp;", "&"), ("&lt;", "<"),
        ("&gt;", ">"), ("&quot;", '"'), ("&#39;", "'"),
    ):
        text = text.replace(entity, char)
    text = _WS_RE.sub(" ", text)
    text = _BLANK_LINES_RE.sub("\n\n", text)
    return text.strip()


# ── SSRF guard ───────────────────────────────────────────────────
#
# web_fetch is reachable through the propagation → front-agent-regex chain
# (web-input safety rule), so a fetched URL is attacker-influenceable. Every host — the initial
# one and every redirect hop — is DNS-resolved and refused if any resolved
# address is non-public: loopback, RFC1918 private, link-local (which covers
# 169.254.169.254 cloud metadata), ULA/loopback v6, or unspecified.


def _resolve_host(host: str) -> list[str]:
    """Resolve a host to its IP strings (own function so tests can seam it).

    IP literals resolve without a network round-trip; real hostnames hit DNS.
    """
    infos = socket.getaddrinfo(host, None)
    # sockaddr[0] is the address; strip any IPv6 zone id (fe80::1%eth0).
    return [info[4][0].split("%")[0] for info in infos]


def _blocked_reason(host: str) -> str | None:
    """Return a rejection reason if `host` resolves to any non-public address."""
    try:
        addrs = _resolve_host(host)
    except OSError as e:
        return f"DNS resolution failed for {host!r}: {e}"
    for addr in addrs:
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            continue
        # Unwrap ::ffff:10.0.0.1 style mappings before classifying.
        mapped = getattr(ip, "ipv4_mapped", None)
        if mapped is not None:
            ip = mapped
        if (
            ip.is_private or ip.is_loopback or ip.is_link_local
            or ip.is_unspecified or ip.is_reserved or ip.is_multicast
        ):
            return f"{host!r} resolves to non-public address {ip}"
    return None


def real_web_fetch(url: str) -> ToolResult:
    """Fetch a web page and return its readable text (truncated).

    Redirects are followed manually (follow_redirects disabled) so the SSRF
    guard re-runs on every hop — an external URL must not be able to bounce
    the request onto an internal address.
    """
    import httpx

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    current = url
    try:
        for _ in range(_MAX_REDIRECTS + 1):
            split = urlsplit(current)
            if split.scheme not in ("http", "https"):
                return ToolResult(
                    success=False, content="",
                    error=f"fetch failed: unsupported scheme {split.scheme!r}",
                )
            host = split.hostname
            if not host:
                return ToolResult(
                    success=False, content="",
                    error=f"fetch failed: no host in {current!r}",
                )
            reason = _blocked_reason(host)
            if reason is not None:
                return ToolResult(
                    success=False, content="", error=f"fetch blocked: {reason}"
                )
            resp = httpx.get(
                current,
                timeout=15.0,
                follow_redirects=False,
                headers={"User-Agent": "pulse-system/0.3"},
            )
            if resp.status_code in _REDIRECT_CODES and "location" in resp.headers:
                current = urljoin(current, resp.headers["location"])
                continue
            resp.raise_for_status()
            break
        else:
            return ToolResult(
                success=False, content="",
                error=f"fetch failed: too many redirects (>{_MAX_REDIRECTS})",
            )
    except Exception as e:
        _logger.warning("web_fetch failed for %s: %s", url, e)
        return ToolResult(success=False, content="", error=f"fetch failed: {e}")

    content_type = resp.headers.get("content-type", "")
    body = resp.text
    text = html_to_text(body) if "html" in content_type else body.strip()
    if len(text) > _MAX_FETCH_CHARS:
        text = text[:_MAX_FETCH_CHARS] + "\n[... truncated]"
    return ToolResult(success=True, content=f"Content from {url}:\n{text}")


def make_tavily_search(api_key: str, *, max_results: int = 5) -> ToolFunction:
    """Build a web_search tool backed by the Tavily search API."""
    import httpx

    def web_search(query: str) -> ToolResult:
        try:
            resp = httpx.post(
                _TAVILY_ENDPOINT,
                json={
                    "api_key": api_key,
                    "query": query,
                    "max_results": max_results,
                },
                timeout=15.0,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            _logger.warning("web_search failed for %r: %s", query, e)
            return ToolResult(success=False, content="", error=f"search failed: {e}")

        results = data.get("results", [])
        if not results:
            return ToolResult(success=True, content=f"No results for \"{query}\".")
        lines = [f"Search results for \"{query}\":"]
        for i, r in enumerate(results, 1):
            title = r.get("title", "(untitled)")
            url = r.get("url", "")
            snippet = (r.get("content") or "").strip()[:300]
            lines.append(f"{i}. {title} — {url}\n   {snippet}")
        return ToolResult(success=True, content="\n".join(lines))

    return web_search

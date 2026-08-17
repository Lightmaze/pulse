"""Agent tool registry and built-in tools.

Provides a unified tool-call interface for engrams to interact with the world.
Tool results are returned as natural-language text (the free-context rule).

Two safety properties:
- Non-mock mode registers NO fake network/code tools. web_search / web_fetch /
  code_execute only exist in mock mode, or when the caller injects real
  implementations via register(). This prevents the LLM from reasoning over
  fabricated data in production.
- File tools are sandboxed to a workspace root. Tool calls are triggered by
  pattern-matching LLM output, and that output can be influenced by content
  propagated from other engrams — unrestricted file access would be a prompt
  injection → arbitrary-file-write channel.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from pulse_system.core.runtime.publication import (
    RuntimePublicationPermit,
)


@dataclass(frozen=True)
class ToolResult:
    success: bool
    content: str
    error: str | None = None


ToolFunction = Callable[..., ToolResult]


@dataclass(frozen=True)
class ToolEntry:
    name: str
    description: str
    fn: ToolFunction
    publication_guarded: bool


class ToolRegistry:
    """Manages available tools and provides a unified execute interface."""

    def __init__(
        self,
        *,
        mock: bool = False,
        workspace_root: str | Path | None = None,
        protected_roots: list[str | Path] | None = None,
        builtins: bool = True,
        allow: list[str] | None = None,
        publication_permit: RuntimePublicationPermit | None = None,
    ):
        if (
            publication_permit is not None
            and type(publication_permit) is not RuntimePublicationPermit
        ):
            raise TypeError(
                "publication_permit must be a RuntimePublicationPermit or None"
            )
        self._tools: dict[str, ToolEntry] = {}
        self._mock = mock
        self._publication_permit = publication_permit
        self._root = Path(workspace_root or Path.cwd()).resolve()
        # Subtrees inside the workspace that write-path tools may not touch,
        # even though reads are allowed (e.g. .pulse/, holding the library and
        # db). Blocks a "propagation → file_write → self-propagating skill"
        # poisoning channel while leaving discover_skills/file_read working.
        self._protected: list[Path] = [
            Path(p).resolve() for p in (protected_roots or [])
        ]
        # toolset restriction: builtins/allow restrict the built-in toolset. Container-style
        # tasks act through an injected exec tool; a host-side file_write
        # outranks code_execute in the front agent's patterns and silently
        # steals the turn (the file lands on the host, not in the container).
        # `builtins=False` skips all builtins; `allow=[...]` whitelists by
        # name. Neither gates caller-side register(), which stays explicit.
        self._allow: set[str] | None = None if allow is None else set(allow)
        if builtins:
            self._register_builtins()

    @property
    def workspace_root(self) -> Path:
        return self._root

    def register(self, name: str, description: str, fn: ToolFunction) -> None:
        """Register one synchronous custom tool.

        A Runtime-bound registry treats every caller-supplied callback as an
        ordinary external-effect owner.  The callback must therefore return
        only after all work it owns has settled; detached/background owners
        belong on the Harness action or task-worker surfaces, which expose an
        explicit shutdown census.
        """

        self._tools[name] = ToolEntry(
            name=name,
            description=description,
            fn=fn,
            publication_guarded=True,
        )

    def execute(self, tool_name: str, **kwargs) -> ToolResult:
        entry = self._tools.get(tool_name)
        if entry is None:
            return ToolResult(
                success=False,
                content="",
                error=f"Unknown tool: {tool_name}",
            )
        try:
            permit = self._publication_permit
            if entry.publication_guarded and permit is not None:
                # The guard is the linearization point against Runtime
                # publication revocation.  It also keeps an admitted custom
                # callback in the shutdown owner census until the callback
                # returns.  There is deliberately no unguarded fallback after
                # revocation.
                with permit.transaction_guard():
                    return entry.fn(**kwargs)
            return entry.fn(**kwargs)
        except Exception as e:
            return ToolResult(success=False, content="", error=str(e))

    def get_available_tools(self) -> list[tuple[str, str]]:
        return [(e.name, e.description) for e in self._tools.values()]

    def describe_tools_natural(self) -> str:
        """Return a natural-language description of all tools for LLM context."""
        if not self._tools:
            return "No tools are currently available."
        lines = ["Available capabilities:"]
        for entry in self._tools.values():
            lines.append(f"- {entry.name}: {entry.description}")
        return "\n".join(lines)

    def has_tool(self, name: str) -> bool:
        return name in self._tools

    # ── Built-in tools ───────────────────────────────────────────

    def _register_builtins(self) -> None:
        if self._mock:
            self._register_builtin("web_search", "Search the web for information", _mock_web_search)
            self._register_builtin("web_fetch", "Read the content of a web page", _mock_web_fetch)
            self._register_builtin("code_execute", "Execute code and return the result", _mock_code_execute)
        # Non-mock: no fake network/code tools. Real implementations must be
        # injected by the caller via register().

        self._register_builtin("file_read", "Read the contents of a file in the workspace", self._file_read)
        self._register_builtin("file_write", "Write content to a file in the workspace", self._file_write)
        self._register_builtin("file_list", "List files and directories in the workspace", self._file_list)

    def _register_builtin(self, name: str, description: str, fn: ToolFunction) -> None:
        if self._allow is None or name in self._allow:
            # Built-ins have individually audited effect semantics.  In
            # particular file_write owns its transaction guard around the
            # physical replace; read/list and explicit mock tools publish no
            # external effect.  Public register() never receives this bypass.
            self._tools[name] = ToolEntry(
                name=name,
                description=description,
                fn=fn,
                publication_guarded=False,
            )

    # ── Workspace sandbox ────────────────────────────────────────

    def _resolve(self, path: str, *, for_write: bool = False) -> Path:
        """Resolve a tool-supplied path and confine it to the workspace root.

        `for_write` additionally refuses paths inside a protected root. Both
        checks compare already-resolved paths, so relative/absolute/.. variants
        collapse to the same target and cannot slip past the guard.
        """
        p = Path(path)
        resolved = (p if p.is_absolute() else self._root / p).resolve()
        if not resolved.is_relative_to(self._root):
            raise PermissionError(
                f"Path escapes workspace ({self._root}): {path}"
            )
        if for_write:
            for root in self._protected:
                if resolved == root or resolved.is_relative_to(root):
                    raise PermissionError(
                        f"Path is write-protected ({root}): {path}"
                    )
        return resolved

    # ── File tools (sandboxed) ───────────────────────────────────

    def _file_read(self, path: str) -> ToolResult:
        try:
            resolved = self._resolve(path)
            content = resolved.read_text(encoding="utf-8")
            return ToolResult(success=True, content=content)
        except Exception as e:
            return ToolResult(success=False, content="", error=str(e))

    def _file_write(self, path: str, content: str) -> ToolResult:
        try:
            resolved = self._resolve(path, for_write=True)

            def commit() -> None:
                resolved.parent.mkdir(parents=True, exist_ok=True)
                temporary: str | None = None
                try:
                    with tempfile.NamedTemporaryFile(
                        mode="w",
                        encoding="utf-8",
                        newline="",
                        prefix=f".{resolved.name}.",
                        suffix=".tmp",
                        dir=resolved.parent,
                        delete=False,
                    ) as stream:
                        temporary = stream.name
                        stream.write(content)
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary, resolved)
                    temporary = None
                finally:
                    if temporary is not None:
                        try:
                            os.unlink(temporary)
                        except OSError:
                            pass

            permit = self._publication_permit
            if permit is None:
                commit()
            else:
                with permit.transaction_guard():
                    commit()
            return ToolResult(
                success=True, content=f"Written {len(content)} bytes to {resolved}"
            )
        except Exception as e:
            return ToolResult(success=False, content="", error=str(e))

    def _file_list(self, directory: str) -> ToolResult:
        try:
            resolved = self._resolve(directory)
            entries = os.listdir(resolved)
            if not entries:
                return ToolResult(success=True, content=f"{resolved} is empty.")
            listing = "\n".join(f"  {e}" for e in sorted(entries))
            return ToolResult(success=True, content=f"Contents of {resolved}:\n{listing}")
        except Exception as e:
            return ToolResult(success=False, content="", error=str(e))


# ── Mock implementations ─────────────────────────────────────────


def _mock_web_search(query: str) -> ToolResult:
    return ToolResult(
        success=True,
        content=(
            f"Search results for \"{query}\":\n"
            f"1. Wikipedia article about {query} — a comprehensive overview.\n"
            f"2. Recent news: {query} has been a trending topic this week.\n"
            f"3. Academic paper: \"Understanding {query}\" (2024)."
        ),
    )


def _mock_web_fetch(url: str) -> ToolResult:
    return ToolResult(
        success=True,
        content=(
            f"Content from {url}:\n"
            f"This is the main content of the page. "
            f"It contains information relevant to the topic at hand."
        ),
    )


def _mock_code_execute(code: str, language: str = "python") -> ToolResult:
    return ToolResult(
        success=True,
        content=f"[{language}] Code executed successfully. Output: (mock result for executed code)",
    )

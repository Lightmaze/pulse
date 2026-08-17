"""Tests for the agent tool registry."""

import os
import tempfile
import threading

import pytest

from pulse_system.agent.tools import ToolRegistry, ToolResult
from pulse_system.core.runtime.publication import RuntimePublicationGate


@pytest.fixture
def registry(tmp_path):
    return ToolRegistry(mock=True, workspace_root=tmp_path)


# ── Registration and discovery ───────────────────────────────────


class TestRegistration:
    def test_bootstrap_permit_cannot_authorize_file_tools(self, tmp_path) -> None:
        gate = RuntimePublicationGate("tools-bootstrap-denied", 1)

        with pytest.raises(TypeError, match="RuntimePublicationPermit"):
            ToolRegistry(
                mock=True,
                workspace_root=tmp_path,
                publication_permit=gate.bootstrap_permit,  # type: ignore[arg-type]
            )

    def test_builtins_registered(self, registry: ToolRegistry):
        tools = registry.get_available_tools()
        names = [name for name, _ in tools]
        assert "web_search" in names
        assert "web_fetch" in names
        assert "file_read" in names
        assert "file_write" in names
        assert "file_list" in names
        assert "code_execute" in names

    def test_register_custom_tool(self, registry: ToolRegistry):
        def my_tool(x: str) -> ToolResult:
            return ToolResult(success=True, content=f"got {x}")

        registry.register("my_tool", "A custom tool", my_tool)
        assert registry.has_tool("my_tool")
        result = registry.execute("my_tool", x="hello")
        assert result.success
        assert result.content == "got hello"

    def test_has_tool(self, registry: ToolRegistry):
        assert registry.has_tool("web_search")
        assert not registry.has_tool("nonexistent")

    def test_describe_tools_natural(self, registry: ToolRegistry):
        desc = registry.describe_tools_natural()
        assert "web_search" in desc
        assert "file_read" in desc
        assert "Available capabilities" in desc

    def test_empty_registry_describe(self):
        reg = ToolRegistry(mock=True)
        reg._tools.clear()
        desc = reg.describe_tools_natural()
        assert "No tools" in desc

    def test_real_mode_has_no_fake_tools(self, tmp_path):
        """Non-mock mode must not register mock web/code tools."""
        reg = ToolRegistry(mock=False, workspace_root=tmp_path)
        names = [name for name, _ in reg.get_available_tools()]
        assert "web_search" not in names
        assert "web_fetch" not in names
        assert "code_execute" not in names
        # file tools remain available (sandboxed)
        assert "file_read" in names
        assert "file_write" in names
        assert "file_list" in names

    def test_real_mode_accepts_injected_implementation(self, tmp_path):
        reg = ToolRegistry(mock=False, workspace_root=tmp_path)

        def real_search(query: str) -> ToolResult:
            return ToolResult(success=True, content=f"real results for {query}")

        reg.register("web_search", "Real web search", real_search)
        assert reg.has_tool("web_search")
        assert "real results" in reg.execute("web_search", query="x").content


# ── Execute interface ────────────────────────────────────────────


class TestExecute:
    def test_unknown_tool(self, registry: ToolRegistry):
        result = registry.execute("nonexistent_tool")
        assert not result.success
        assert result.error is not None
        assert "Unknown tool" in result.error

    def test_tool_exception_caught(self, registry: ToolRegistry):
        def bad_tool() -> ToolResult:
            raise RuntimeError("boom")

        registry.register("bad", "A broken tool", bad_tool)
        result = registry.execute("bad")
        assert not result.success
        assert "boom" in result.error

    def test_runtime_revocation_blocks_custom_callback_without_invocation(
        self,
        tmp_path,
    ) -> None:
        gate = RuntimePublicationGate("tool-custom-revoked", 1)
        registry = ToolRegistry(
            mock=False,
            workspace_root=tmp_path,
            publication_permit=gate.publication_permit,
        )
        target = tmp_path / "custom-effect.txt"
        invocations = 0

        def custom_effect() -> ToolResult:
            nonlocal invocations
            invocations += 1
            target.write_text("late", encoding="utf-8")
            return ToolResult(success=True, content="written")

        registry.register("custom_effect", "write one external effect", custom_effect)
        gate.revoke(reason="test_shutdown")

        result = registry.execute("custom_effect")

        assert result.success is False
        assert result.error == "publication_revoked"
        assert invocations == 0
        assert not target.exists()

    def test_running_custom_callback_remains_in_publication_owner_census(
        self,
        tmp_path,
    ) -> None:
        gate = RuntimePublicationGate("tool-custom-running", 1)
        registry = ToolRegistry(
            mock=False,
            workspace_root=tmp_path,
            publication_permit=gate.publication_permit,
        )
        callback_entered = threading.Event()
        callback_release = threading.Event()
        drain_started = threading.Event()
        results: list[ToolResult] = []
        drain_summaries: list[dict[str, object]] = []

        def blocking_effect() -> ToolResult:
            callback_entered.set()
            assert callback_release.wait(2.0)
            return ToolResult(success=True, content="settled")

        registry.register(
            "blocking_effect",
            "hold one synchronous external effect",
            blocking_effect,
        )
        callback_owner = threading.Thread(
            target=lambda: results.append(registry.execute("blocking_effect")),
            name="tool-custom-effect-owner",
        )
        callback_owner.start()
        assert callback_entered.wait(1.0)
        assert gate.snapshot().active_publication_transactions == 1

        gate.revoke(reason="test_shutdown")

        def observe_drain() -> None:
            drain_started.set()
            drain_summaries.append(gate.wait_for_publication_drain())

        drain_owner = threading.Thread(
            target=observe_drain,
            name="tool-custom-effect-drain",
        )
        drain_owner.start()
        assert drain_started.wait(1.0)
        drain_owner.join(timeout=0.05)
        assert drain_owner.is_alive()

        callback_release.set()
        callback_owner.join(timeout=1.0)
        drain_owner.join(timeout=1.0)

        assert not callback_owner.is_alive()
        assert not drain_owner.is_alive()
        assert results == [ToolResult(success=True, content="settled")]
        assert drain_summaries == [
            {
                "active_before": 1,
                "unresolved": 0,
                "owner_joined": True,
                "process_tree_state": "not_applicable",
                "publication_transactions": 1,
                "bootstrap_transactions": 0,
            }
        ]


# ── Mock tools ───────────────────────────────────────────────────


class TestMockTools:
    def test_web_search(self, registry: ToolRegistry):
        result = registry.execute("web_search", query="quantum computing")
        assert result.success
        assert "quantum computing" in result.content

    def test_web_fetch(self, registry: ToolRegistry):
        result = registry.execute("web_fetch", url="https://example.com")
        assert result.success
        assert "example.com" in result.content

    def test_code_execute(self, registry: ToolRegistry):
        result = registry.execute("code_execute", code="print('hello')", language="python")
        assert result.success
        assert "python" in result.content


# ── File tools (real implementation) ─────────────────────────────


class TestFileTools:
    def test_file_read(self, registry: ToolRegistry, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello world", encoding="utf-8")
        result = registry.execute("file_read", path=str(f))
        assert result.success
        assert result.content == "hello world"

    def test_file_read_nonexistent(self, registry: ToolRegistry):
        result = registry.execute("file_read", path="/nonexistent/path/xyz.txt")
        assert not result.success
        assert result.error is not None

    def test_file_write(self, registry: ToolRegistry, tmp_path):
        f = tmp_path / "output.txt"
        result = registry.execute("file_write", path=str(f), content="written content")
        assert result.success
        assert f.read_text(encoding="utf-8") == "written content"

    def test_file_write_creates_dirs(self, registry: ToolRegistry, tmp_path):
        f = tmp_path / "sub" / "dir" / "file.txt"
        result = registry.execute("file_write", path=str(f), content="deep write")
        assert result.success
        assert f.read_text(encoding="utf-8") == "deep write"

    def test_runtime_revocation_blocks_mock_file_write_without_fallback(
        self,
        tmp_path,
    ):
        gate = RuntimePublicationGate("tool-registry-runtime", 1)
        registry = ToolRegistry(
            mock=True,
            workspace_root=tmp_path,
            publication_permit=gate.publication_permit,
        )
        target = tmp_path / "runtime-owned.txt"
        first = registry.execute(
            "file_write",
            path=str(target),
            content="before",
        )
        assert first.success is True

        gate.revoke(reason="test_shutdown")
        late = registry.execute(
            "file_write",
            path=str(target),
            content="after",
        )

        assert late.success is False
        assert "publication_revoked" in (late.error or "")
        assert target.read_text(encoding="utf-8") == "before"

    def test_file_list(self, registry: ToolRegistry, tmp_path):
        (tmp_path / "a.txt").touch()
        (tmp_path / "b.txt").touch()
        result = registry.execute("file_list", directory=str(tmp_path))
        assert result.success
        assert "a.txt" in result.content
        assert "b.txt" in result.content

    def test_file_list_empty(self, registry: ToolRegistry, tmp_path):
        result = registry.execute("file_list", directory=str(tmp_path))
        assert result.success
        assert "empty" in result.content

    def test_file_list_nonexistent(self, registry: ToolRegistry):
        result = registry.execute("file_list", directory="/nonexistent/dir")
        assert not result.success


# ── Workspace sandbox ─────────────────────────────────────────────


class TestSandbox:
    def test_relative_path_stays_in_workspace(self, registry: ToolRegistry, tmp_path):
        result = registry.execute("file_write", path="notes/a.txt", content="ok")
        assert result.success
        assert (tmp_path / "notes" / "a.txt").read_text(encoding="utf-8") == "ok"

    def test_dotdot_escape_rejected(self, registry: ToolRegistry, tmp_path):
        result = registry.execute(
            "file_write", path="../escape.txt", content="nope"
        )
        assert not result.success
        assert "workspace" in result.error
        assert not (tmp_path.parent / "escape.txt").exists()

    def test_absolute_path_outside_rejected(self, registry: ToolRegistry, tmp_path):
        outside = tmp_path.parent / "outside.txt"
        result = registry.execute("file_read", path=str(outside))
        assert not result.success
        assert "workspace" in result.error

    def test_absolute_path_inside_allowed(self, registry: ToolRegistry, tmp_path):
        f = tmp_path / "inside.txt"
        f.write_text("fine", encoding="utf-8")
        result = registry.execute("file_read", path=str(f))
        assert result.success
        assert result.content == "fine"

    def test_list_escape_rejected(self, registry: ToolRegistry, tmp_path):
        result = registry.execute("file_list", directory=str(tmp_path.parent))
        assert not result.success
        assert "workspace" in result.error

    def test_default_root_is_cwd(self):
        reg = ToolRegistry(mock=True)
        import pathlib
        assert reg.workspace_root == pathlib.Path.cwd().resolve()


# ── Protected roots: .pulse write exclusion ──────────────────────


class TestProtectedRoots:
    @pytest.fixture
    def registry(self, tmp_path):
        return ToolRegistry(
            mock=True,
            workspace_root=tmp_path,
            protected_roots=[tmp_path / ".pulse"],
        )

    def test_write_into_protected_rejected(self, registry, tmp_path):
        result = registry.execute(
            "file_write", path=".pulse/library/skills/evil.md", content="x"
        )
        assert not result.success
        assert "write-protected" in result.error
        assert not (tmp_path / ".pulse" / "library" / "skills" / "evil.md").exists()

    def test_read_from_protected_allowed(self, registry, tmp_path):
        f = tmp_path / ".pulse" / "library" / "note.md"
        f.parent.mkdir(parents=True)
        f.write_text("procedural memory", encoding="utf-8")
        result = registry.execute("file_read", path=".pulse/library/note.md")
        assert result.success
        assert result.content == "procedural memory"

    def test_write_elsewhere_in_workspace_allowed(self, registry, tmp_path):
        result = registry.execute(
            "file_write", path="workspace_notes/a.txt", content="ok"
        )
        assert result.success
        assert (tmp_path / "workspace_notes" / "a.txt").read_text(
            encoding="utf-8"
        ) == "ok"

    @pytest.mark.parametrize("variant", [
        ".pulse/db.sqlite",                 # direct child
        "notes/../.pulse/library/x",        # traversal back into .pulse
        "./.pulse/library/y",               # dot-relative
    ])
    def test_path_variants_cannot_bypass(self, registry, tmp_path, variant):
        result = registry.execute("file_write", path=variant, content="x")
        assert not result.success
        assert "write-protected" in result.error

    def test_absolute_path_into_protected_rejected(self, registry, tmp_path):
        target = tmp_path / ".pulse" / "library" / "z.md"
        result = registry.execute("file_write", path=str(target), content="x")
        assert not result.success
        assert "write-protected" in result.error

    def test_no_protected_roots_is_unrestricted(self, tmp_path):
        reg = ToolRegistry(mock=True, workspace_root=tmp_path)
        result = reg.execute(
            "file_write", path=".pulse/library/ok.md", content="x"
        )
        assert result.success


# ── toolset restriction: restricted toolsets ──────────────────────────────────────


class TestRestrictedToolset:
    """toolset restriction regression: callers must be able to exclude or whitelist builtins.

    Exposed by the LHTB bridge: builtins were registered unconditionally, and
    file_write's pattern outranks code_execute, so host file tools stole
    container turns — the agent believed it wrote a file, the container saw
    nothing, the verifier scored 0 with clean logs.
    """

    def test_builtins_false_registers_nothing(self, tmp_path):
        reg = ToolRegistry(mock=True, workspace_root=tmp_path, builtins=False)
        assert reg.get_available_tools() == []

    def test_builtins_false_custom_register_still_works(self, tmp_path):
        reg = ToolRegistry(mock=True, workspace_root=tmp_path, builtins=False)
        reg.register("code_execute", "Run a command in the task container",
                     lambda code, language="bash": ToolResult(success=True, content="ok"))
        names = [n for n, _ in reg.get_available_tools()]
        assert names == ["code_execute"]

    def test_allow_whitelists_builtins(self, tmp_path):
        reg = ToolRegistry(mock=True, workspace_root=tmp_path, allow=["file_read"])
        names = [n for n, _ in reg.get_available_tools()]
        assert names == ["file_read"]

    def test_allow_empty_excludes_all_builtins(self, tmp_path):
        reg = ToolRegistry(mock=True, workspace_root=tmp_path, allow=[])
        assert reg.get_available_tools() == []

    def test_allow_does_not_gate_caller_register(self, tmp_path):
        reg = ToolRegistry(mock=True, workspace_root=tmp_path, allow=["file_read"])
        reg.register("bash", "container exec", lambda command: ToolResult(success=True, content="ok"))
        assert reg.has_tool("bash")

    def test_default_is_unchanged(self, tmp_path):
        reg = ToolRegistry(mock=True, workspace_root=tmp_path)
        names = [n for n, _ in reg.get_available_tools()]
        assert "file_write" in names
        assert "web_search" in names

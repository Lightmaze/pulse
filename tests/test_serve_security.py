"""CLI-side fail-closed checks for API Profile and network binding."""

from __future__ import annotations

import pytest

from pulse_system.interaction.api.security import (
    ApiSecurityConfigurationError,
    CapabilityProfile,
)
from pulse_system.service.serve import _build_api_security, _parse, main


def test_cli_defaults_to_loopback_safe_profile():
    security = _build_api_security(_parse([]))
    assert security.profile is CapabilityProfile.SAFE
    assert security.loopback_only is True
    assert security.write_enabled is False


@pytest.mark.parametrize(
    "argv",
    [
        ["--host", "0.0.0.0"],
        ["--host", "0.0.0.0", "--allow-network-bind"],
        ["--host", "0.0.0.0", "--origin", "https://workbench.example"],
    ],
)
def test_non_loopback_cli_needs_both_network_opt_ins(argv):
    with pytest.raises(ApiSecurityConfigurationError):
        _build_api_security(_parse(argv))


def test_non_loopback_cli_accepts_double_opt_in():
    security = _build_api_security(
        _parse(
            [
                "--host",
                "0.0.0.0",
                "--allow-network-bind",
                "--origin",
                "https://workbench.example",
            ]
        )
    )
    assert security.loopback_only is False
    assert security.allowed_origins == ("https://workbench.example",)


@pytest.mark.parametrize(
    "argv",
    [
        ["--enable-codex-read-only-sandbox"],
        ["--codex-sandbox-executable", "codex"],
        ["--harness-command", "git"],
        ["--enable-harness-pipe-sessions"],
    ],
)
def test_safe_profile_rejects_tool_capability_configuration(argv):
    with pytest.raises(ApiSecurityConfigurationError):
        _build_api_security(_parse(argv))


def test_workspace_rejects_pipe_while_lab_accepts_the_profile_ceiling():
    with pytest.raises(ApiSecurityConfigurationError):
        _build_api_security(
            _parse(["--profile", "workspace", "--enable-harness-pipe-sessions"])
        )
    security = _build_api_security(
        _parse(["--profile", "lab", "--enable-harness-pipe-sessions"])
    )
    assert security.profile is CapabilityProfile.LAB


def test_invalid_security_fails_before_database_directory_creation(tmp_path, capsys):
    db = tmp_path / "must-not-exist" / "run.db"
    result = main(["--mock", "--host", "0.0.0.0", "--db", str(db)])
    assert result == 2
    assert not db.parent.exists()
    captured = capsys.readouterr()
    assert "security configuration failed" in captured.err

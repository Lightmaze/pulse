"""Public release, version, and toolchain contracts."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

import yaml

from pulse_system.interaction.api.app import create_app
from pulse_system.interaction.api.security import LocalApiSecurity
from pulse_system.version import (
    PUBLIC_VERSION,
    PYTHON_DISTRIBUTION_VERSION,
    __version__,
)


ROOT = Path(__file__).resolve().parents[1]
PUBLIC = "0.2.0-alpha.1"
PYTHON = "0.2.0a1"


def _toml(path: str) -> dict:
    return tomllib.loads((ROOT / path).read_text(encoding="utf-8"))


def _json(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def test_python_web_api_and_changelog_versions_are_consistent(tmp_path):
    project = _toml("pyproject.toml")
    lock = _toml("uv.lock")
    web = _json("web/package.json")
    web_lock = _json("web/package-lock.json")

    assert project["project"]["version"] == PYTHON
    [root_package] = [
        package for package in lock["package"] if package["name"] == "pulse-system"
    ]
    assert root_package["version"] == PYTHON
    assert PYTHON_DISTRIBUTION_VERSION == __version__ == PYTHON
    assert PUBLIC_VERSION == PUBLIC
    assert web["version"] == web_lock["version"] == PUBLIC
    assert web_lock["packages"][""]["version"] == PUBLIC
    assert f"## [{PUBLIC}]" in (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")

    security = LocalApiSecurity(access_token="a" * 32)
    assert security.public_projection()["product_version"] == PUBLIC
    app = create_app(tmp_path / "metrics.jsonl", api_security=security)
    assert app.version == PUBLIC


def test_release_toolchain_is_exactly_pinned():
    project = _toml("pyproject.toml")
    web = _json("web/package.json")
    web_lock = _json("web/package-lock.json")

    assert (ROOT / ".python-version").read_text(encoding="utf-8").strip() == (
        "3.12.13"
    )
    assert project["tool"]["uv"]["required-version"] == "==0.11.1"
    assert (ROOT / ".node-version").read_text(encoding="utf-8").strip() == (
        "24.15.0"
    )
    assert (ROOT / ".nvmrc").read_text(encoding="utf-8").strip() == "24.15.0"
    assert web["packageManager"] == "npm@11.12.1"
    assert web["engines"] == {"node": "24.15.0", "npm": "11.12.1"}
    assert web["devEngines"] == {
        "runtime": {
            "name": "node",
            "version": "24.15.0",
            "onFail": "error",
        },
        "packageManager": {
            "name": "npm",
            "version": "11.12.1",
            "onFail": "error",
        },
    }
    assert web_lock["packages"][""]["engines"] == web["engines"]
    assert web["scripts"]["test"].startswith("node --test ")


def test_ci_runs_no_key_python_web_and_cold_artifact_contracts_on_both_hosts():
    path = ROOT / ".github" / "workflows" / "ci.yml"
    text = path.read_text(encoding="utf-8")
    workflow = yaml.safe_load(text)
    [job] = workflow["jobs"].values()

    assert job["strategy"]["matrix"]["os"] == [
        "ubuntu-latest",
        "windows-latest",
    ]
    for name in ("DEEPSEEK_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
        assert workflow["env"][name] == ""
    required = (
        "actions/checkout@v7.0.0",
        "astral-sh/setup-uv@v9.0.0",
        'version: "0.11.1"',
        'python-version: "3.12.13"',
        "actions/setup-node@v6.4.0",
        'node-version: "24.15.0"',
        "npm install --global npm@11.12.1",
        "uv run --no-sync pytest",
        "npm ci",
        "npm test",
        "npm run build",
        "runner.temp }}/pulse-web-dist",
        "uv build --out-dir",
        "release_artifact_check.py",
        "--cold-install",
    )
    for fragment in required:
        assert fragment in text

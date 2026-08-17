from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.skipif(
    not (os.environ.get("PULSE_PI_NODE") or shutil.which("node")),
    reason="Node runtime is not configured for the Pi extension probe",
)
def test_mutable_extension_timeout_sends_cancel_before_abort() -> None:
    node = os.environ.get("PULSE_PI_NODE") or shutil.which("node")
    assert node is not None
    repo_root = Path(__file__).resolve().parents[1]
    probe = repo_root / "tests" / "assets" / "pulse_extension_timeout_probe.mjs"
    completed = subprocess.run(
        [node, "--experimental-strip-types", str(probe)],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "pulse_extension_timeout_ok" in completed.stdout

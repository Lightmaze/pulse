"""Inspect and cold-install release artifacts without importing the source tree."""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path


PYTHON_VERSION = "0.2.0a1"
MIGRATIONS = (
    "pulse_system/substrate/storage/migrations/0001_initial.sql",
    "pulse_system/substrate/storage/migrations/0002_task_relationship.sql",
    "pulse_system/substrate/storage/migrations/0003_role_lease.sql",
    "pulse_system/substrate/storage/migrations/0004_role_direct_output.sql",
    "pulse_system/substrate/storage/migrations/0005_dendritic_convergence.sql",
    "pulse_system/substrate/storage/migrations/0006_dendritic_window_evidence.sql",
)
PROVIDER_ENV = (
    "DEEPSEEK_API_KEY",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
)
PUBLIC_PACKAGE_ROOTS = frozenset(
    {
        "agent",
        "core",
        "education",
        "habitat",
        "interaction",
        "service",
        "substrate",
        "weights",
    }
)
PUBLIC_PACKAGE_FILES = frozenset({"__init__.py", "__main__.py", "cli.py", "version.py"})


def _members(path: Path) -> tuple[str, ...]:
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            return tuple(name.replace("\\", "/") for name in archive.namelist())
    with tarfile.open(path, mode="r:gz") as archive:
        return tuple(member.name.replace("\\", "/") for member in archive.getmembers())


def _verify_members(path: Path, members: tuple[str, ...]) -> None:
    for migration in MIGRATIONS:
        if not any(name.endswith(migration) for name in members):
            raise RuntimeError(f"{path.name} is missing packaged migration {migration}")
    forbidden_suffixes = (".db", ".db-wal", ".db-shm", ".jsonl")
    for name in members:
        lowered = name.casefold()
        parts = tuple(part for part in lowered.split("/") if part)
        if lowered.endswith(forbidden_suffixes):
            raise RuntimeError(f"{path.name} contains runtime data: {name}")
        if any(part in {".env", ".fgrun", "artifacts"} for part in parts):
            raise RuntimeError(f"{path.name} contains a runtime path: {name}")
        if any(part in {"auth.json", "live-gate.json"} for part in parts):
            raise RuntimeError(f"{path.name} contains a credential/live artifact: {name}")
        if "pulse_system" not in parts:
            continue
        package_parts = parts[parts.index("pulse_system") + 1 :]
        if not package_parts:
            continue
        if len(package_parts) == 1 and package_parts[0] in PUBLIC_PACKAGE_FILES:
            continue
        if package_parts[0] not in PUBLIC_PACKAGE_ROOTS:
            raise RuntimeError(f"{path.name} contains an unexpected package path: {name}")


def verify_artifacts(directory: Path) -> tuple[Path, Path]:
    wheel = tuple(sorted(directory.glob("*.whl")))
    sdist = tuple(sorted(directory.glob("*.tar.gz")))
    if len(wheel) != 1 or len(sdist) != 1:
        raise RuntimeError(
            f"expected one wheel and one sdist, found {len(wheel)} and {len(sdist)}"
        )
    expected_stem = f"pulse_system-{PYTHON_VERSION}"
    if not wheel[0].name.startswith(expected_stem + "-"):
        raise RuntimeError(f"unexpected wheel version: {wheel[0].name}")
    if sdist[0].name != expected_stem + ".tar.gz":
        raise RuntimeError(f"unexpected sdist version: {sdist[0].name}")
    for artifact in (wheel[0], sdist[0]):
        _verify_members(artifact, _members(artifact))
        digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
        print(f"SHA256 {artifact.name} {digest}")
    return wheel[0], sdist[0]


def _venv_python(directory: Path) -> Path:
    if os.name == "nt":
        return directory / "Scripts" / "python.exe"
    return directory / "bin" / "python"


def _venv_pulse(directory: Path) -> Path:
    if os.name == "nt":
        return directory / "Scripts" / "pulse.exe"
    return directory / "bin" / "pulse"


def cold_install(artifact: Path, *, temp_root: Path | None = None) -> None:
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv is required for the cold-install check")
    environment = os.environ.copy()
    for name in PROVIDER_ENV:
        environment.pop(name, None)
    with tempfile.TemporaryDirectory(
        prefix=f"pulse-{artifact.suffix.lstrip('.')}-",
        dir=temp_root,
    ) as raw_directory:
        venv = Path(raw_directory) / "venv"
        subprocess.run(
            [uv, "venv", "--python", "3.12.13", str(venv)],
            check=True,
            env=environment,
        )
        python = _venv_python(venv)
        subprocess.run(
            [uv, "pip", "install", "--python", str(python), str(artifact)],
            check=True,
            env=environment,
        )
        subprocess.run(
            [
                str(python),
                "-c",
                "import pulse_system; "
                "assert pulse_system.__version__ == '0.2.0a1'; "
                "print(pulse_system.__version__)",
            ],
            check=True,
            cwd=venv,
            env=environment,
        )
        subprocess.run(
            [str(_venv_pulse(venv)), "--help"],
            check=True,
            cwd=venv,
            env=environment,
            stdout=subprocess.DEVNULL,
        )
        print(f"COLD_INSTALL_OK {artifact.name}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--cold-install", action="store_true")
    parser.add_argument("--temp-root", type=Path, default=None)
    args = parser.parse_args(argv)
    directory = args.directory.resolve()
    if not directory.is_dir():
        parser.error(f"artifact directory does not exist: {directory}")
    temp_root = None if args.temp_root is None else args.temp_root.resolve()
    if temp_root is not None:
        temp_root.mkdir(parents=True, exist_ok=True)
    artifacts = verify_artifacts(directory)
    if args.cold_install:
        for artifact in artifacts:
            cold_install(artifact, temp_root=temp_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

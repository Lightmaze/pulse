"""Every distributed Python script must parse.

The check deliberately compiles source without importing it, so release
utilities remain testable without network access or provider credentials.
An existing but empty scripts directory is a failure rather than a vacuous
green result.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
_SCRIPTS = sorted(_SCRIPTS_DIR.glob("*.py"))

# Files that are deliberately not runnable scripts. Naming them here is the
# point: a truncated file cannot join this list by accident, the way it could
# by simply being short.
_KNOWN_STUBS: frozenset[str] = frozenset()


def test_the_guard_has_something_to_guard() -> None:
    """Without this, an empty scripts/ makes the whole file vacuously green."""
    if not _SCRIPTS_DIR.is_dir():
        pytest.skip(
            "scripts/ is not present; source checkouts and sdists include the "
            "public release scripts."
        )
    assert _SCRIPTS, (
        f"{_SCRIPTS_DIR} exists but holds no .py files. Either the scripts were "
        "removed, or this guard is now watching an empty room and reporting green."
    )


@pytest.mark.parametrize("path", _SCRIPTS, ids=lambda p: p.name)
def test_script_parses(path: Path) -> None:
    src = path.read_text(encoding="utf-8")
    if path.name in _KNOWN_STUBS:
        pytest.skip(f"{path.name} is a declared stub, not a runnable script")
    try:
        ast.parse(src, filename=str(path))
    except SyntaxError as exc:
        pytest.fail(f"{path.name}:{exc.lineno} does not parse: {exc.msg}")

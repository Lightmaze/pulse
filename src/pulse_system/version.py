"""Release identifiers shared by package metadata and public projections."""

from __future__ import annotations

from typing import Final

PYTHON_DISTRIBUTION_VERSION: Final[str] = "0.2.0a1"
PUBLIC_VERSION: Final[str] = "0.2.0-alpha.1"

# Conventional runtime attribute used by import/cold-install checks.
__version__: Final[str] = PYTHON_DISTRIBUTION_VERSION

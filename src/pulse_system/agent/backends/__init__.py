"""Agent backends — where a delegated task actually runs.

See `base.py` for the contract and the one rule this package enforces: a
backend that cannot run says so by name and stops, rather than quietly
letting some other executor answer in its place.

    from pulse_system.agent.backends import LocalBackend, PiBackend, TaskSpec

    backend = LocalBackend(engram_manager, tools)      # the default
    result = backend.submit(TaskSpec("summarise the delegation router"))
"""

from .base import (
    AgentBackend,
    BackendError,
    BackendResult,
    BackendUnavailable,
    TaskSpec,
)
from .local import LocalBackend
from .pi import PI_INSTALL_HINT, PI_NPM_PACKAGE, PiBackend

__all__ = [
    "PI_INSTALL_HINT",
    "PI_NPM_PACKAGE",
    "AgentBackend",
    "BackendError",
    "BackendResult",
    "BackendUnavailable",
    "LocalBackend",
    "PiBackend",
    "TaskSpec",
]

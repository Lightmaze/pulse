"""Agent backends — the thing that actually executes a delegated task.

The tunnel stream (`agent/delegate/`) routes a task *to an engram* and
`core/delegation/` learns *which* engram. Neither answers the question
underneath both: **what runs the work.** The runtime API's
`POST /delegate {"backend": "pi"|"local"|null}` had nowhere to send a
non-local task. This package is that answer, made explicit and plural.

Two implementations ship:

- `LocalBackend` (`local.py`) — in-process, on the front-stage agent path.
  Runs under `LLMAdapter(mock=True)` with no API key, so the whole system
  stays demonstrable offline. **This is the default.**
- `PiBackend` (`pi.py`) — drives the `pi` coding-agent CLI as a subprocess.

── The rule this package exists to enforce ──────────────────────────

A backend that cannot run says so, by name, and stops. It never quietly
substitutes a different executor for the one that was asked for. "I could
not reach pi" is not a result and must never arrive shaped like one — that
substitution is precisely the class of lie this repository has a written
rule against, and the reason `pi.py` imports nothing from `local.py`.

Two failure shapes, split on one question — *did anything run?*

- **Preconditions raise.** The executor is absent, the target engram does
  not exist: nothing ran, there is no trace, and there is nothing partial
  worth returning. `BackendUnavailable` / `BackendError` are raised.
- **Execution outcomes return.** The task really started and then timed
  out, was killed, or errored: `BackendResult(ok=False, error=...)`, with
  whatever partial output and trace survived. Those are worth keeping.

No path in this package returns `ok=True` alongside output the backend did
not actually receive from its executor — an empty result is reported as a
failure, not as success with nothing in it.

Every error states *what failed* and *how the caller could make it work*. A
refusal without a remedy is half a refusal.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "AgentBackend",
    "BackendError",
    "BackendResult",
    "BackendUnavailable",
    "TaskSpec",
]


class BackendError(Exception):
    """A refusal that names the failure *and* the way out (contract §6).

    `to_dict()` is the wire shape the delegate endpoint returns verbatim:
    `{"error": ..., "detail": ..., "remedy": ...}`.
    """

    def __init__(self, code: str, detail: str, remedy: str):
        self.code = code
        self.detail = detail
        self.remedy = remedy
        super().__init__(f"{code}: {detail}\n  remedy: {remedy}")

    def to_dict(self) -> dict[str, str]:
        return {"error": self.code, "detail": self.detail, "remedy": self.remedy}


class BackendUnavailable(BackendError):
    """The requested executor is not present.

    Always raised, never returned, and never swapped for another backend.
    A caller that wants a fallback must choose it in the open.
    """


@dataclass(frozen=True)
class TaskSpec:
    """One unit of delegated work.

    Deliberately three fields. A wide task object here would be wrong in a
    week: each backend would grow a private corner of it, and the corners
    would rot unread. Everything backend-specific (workspace root, model,
    iteration budget) is configured on the backend instance instead, where
    it belongs — it describes the deployment, not the task.

    - `task`     the instruction, verbatim. Never re-framed by a backend.
    - `target`   an engram id for in-process backends. `None` = fresh
                 engram. Backends with no engram record it as ignored in
                 their trace rather than dropping it silently.
    - `timeout_sec`  wall-clock budget. `None` = no limit (a real choice,
                 not a default; unbounded work is how a run hangs forever).
    """

    task: str
    target: str | None = None
    timeout_sec: float | None = 300.0


@dataclass
class BackendResult:
    """What came back, plus as much of the trace as the backend can give.

    `trace` is intentionally untyped-per-entry: `LocalBackend` returns
    engram session messages, `PiBackend` returns pi's JSON event lines.
    Forcing those into one schema would discard the detail that makes each
    worth reading. Every entry does carry a `"kind"` key so a consumer can
    tell them apart without guessing.
    """

    backend: str
    ok: bool
    output: str
    trace: list[dict[str, Any]] = field(default_factory=list)
    error: BackendError | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "ok": self.ok,
            "output": self.output,
            "trace": self.trace,
            "error": self.error.to_dict() if self.error is not None else None,
        }


@runtime_checkable
class AgentBackend(Protocol):
    """Submit a task, get a result and a trace. That is the whole contract.

    `preflight()` exists so a caller — the delegate endpoint, a status
    page — can ask "could you run this?" without running anything. It
    raises `BackendUnavailable` when the answer is no, and returns None
    otherwise. `submit()` calls it first, so it is never a way to skip
    the check, only a way to ask early.
    """

    name: str

    def preflight(self) -> None:
        """Raise `BackendUnavailable` if this backend cannot execute."""
        ...

    def submit(self, spec: TaskSpec) -> BackendResult:
        """Run `spec` to completion (or to its timeout) and report."""
        ...

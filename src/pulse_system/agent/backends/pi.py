"""PiBackend — delegation executed by pi (badlogic/pi-mono) over its RPC mode.

pi is a TypeScript agent harness (MIT, Node >= 22.19, npm package
`@earendil-works/pi-coding-agent`, bin `pi`). It offers four embedding
modes; this driver uses **RPC**, because RPC is a *defined wire contract*
that pi ships and tests against, whereas a print-mode command line would be
a shape we inferred from a flag table. When the other side publishes a
protocol, drive the protocol.

── Confirmed against the pi source ──────────────────────────────────

Read from a local clone of https://github.com/badlogic/pi-mono at commit
3da591a, `packages/coding-agent` version 0.80.10. Line references are to
that tree; the same files on the default branch:

  https://github.com/badlogic/pi-mono/blob/main/packages/coding-agent/src/modes/rpc/rpc-types.ts
  https://github.com/badlogic/pi-mono/blob/main/packages/coding-agent/src/modes/rpc/rpc-mode.ts
  https://github.com/badlogic/pi-mono/blob/main/packages/coding-agent/src/modes/rpc/rpc-client.ts
  https://github.com/badlogic/pi-mono/blob/main/packages/coding-agent/src/modes/rpc/jsonl.ts
  https://github.com/badlogic/pi-mono/blob/main/packages/agent/src/agent-loop.ts
  https://github.com/badlogic/pi-mono/blob/main/packages/ai/src/types.ts

- **Invocation.** `pi --mode rpc` (plus optional `--provider <name>` and
  `--model <pattern>`). `--mode` accepts exactly `text|json|rpc`
  (`src/cli/args.ts:78`); pi's own client spawns `node <cli> --mode rpc`
  and appends provider/model the same way (`rpc-client.ts` `start()`).
- **Framing.** Strict JSONL, **LF-only**, one JSON object per line;
  a trailing `\r` is stripped; U+2028/U+2029 are *not* record separators
  (`jsonl.ts`, which deliberately avoids Node's readline for this reason).
  Commands go to stdin, responses and events come back on stdout
  (`rpc-mode.ts` header).
- **Correlation.** A command may carry `id`; the reply is
  `{"id": ..., "type": "response", "command": <type>, "success": true, "data"?: ...}`
  or `{"id": ..., "type": "response", "command": ..., "success": false,
  "error": "<message>"}` (`rpc-types.ts`, `rpc-mode.ts` `success()`/`error()`).
  Any line that is not a correlated response is an event — that is exactly
  how pi's own client demultiplexes (`rpc-client.ts` `handleLine()`), and
  it is why responses and events may interleave freely.
- **Completion.** A turn is done at `{"type": "agent_settled"}`
  (`core/agent-session.ts:143` and `:563-564`). pi's own `waitForIdle()`
  waits on precisely this event and nothing else (`rpc-client.ts:447`).
- **Commands used here**, all members of the `RpcCommand` union:
  `get_state`, `prompt`, `get_last_assistant_text`, `get_entries`.
  An unrecognised command returns `success: false` with
  `"Unknown command: <type>"` (`rpc-mode.ts:689-691`) — so a pi too old for
  one of these refuses in a readable way instead of hanging.
- **No startup handshake line.** pi emits nothing to announce readiness;
  its own client simply sleeps 100 ms (`rpc-client.ts` `start()`). Sleeping
  is not evidence, so this driver instead does a `get_state` round trip:
  a reply proves the process is up *and* yields `sessionId`.
- **Shutdown.** RPC mode installs a SIGTERM handler (plus SIGHUP off
  Windows) that exits **143** / **129** respectively (`rpc-mode.ts`
  `registerSignalHandlers`). pi's own client stops the child with SIGTERM
  and escalates to SIGKILL after 1 s (`rpc-client.ts` `stop()`); this
  driver mirrors that, including the 1 s grace.
- **Stop reasons.** `"stop" | "length" | "toolUse" | "error" | "aborted"`
  (`packages/ai/src/types.ts:380`).
- **Truncation is failure, not completion.** When `stopReason === "length"`
  pi's loop calls `failToolCallsFromTruncatedMessage` (`agent-loop.ts:383`)
  so a cut-off turn's tool calls read as failed rather than as done. This
  repository enforces the same rule in its LLM adapter, arrived at
  independently. This driver honours it: a `"length"` finish is reported
  `ok=False` with code `pi_truncated`, never as a completed answer.

── Marked UNVERIFIED (labelled, not guessed) ────────────────────────

Each of these is handled defensively; none is asserted as fact.

1. **Whether `pi --mode rpc` exits on stdin EOF.** Not confirmed — pi's own
   client never closes stdin, it signals instead. This driver therefore
   does not rely on EOF: it closes stdin *and* terminates, then kills.
   To check: `src/modes/rpc/rpc-mode.ts` `shutdown()` and the `onEnd`
   handler in `src/modes/rpc/jsonl.ts`.
2. **How a missing/invalid provider API key surfaces.** Unconfirmed
   whether it arrives as `response success:false`, as an assistant message
   with `stopReason:"error"`, or only on stderr. Both structured paths are
   handled and stderr is captured either way. To check:
   `src/core/agent-session.ts` prompt error handling and
   `packages/ai/src/` provider error mapping.
3. **Exit codes for startup failures** (bad flag, unusable config). Several
   `process.exit(1)` sites exist in `src/main.ts`, but the full mapping was
   not enumerated; this driver reports whatever code it observes rather
   than interpreting it. To check: `src/main.ts`.
4. **`get_entries`' `since` parameter semantics and payload size.** The
   field exists in `RpcCommand` but its filtering behaviour was not
   confirmed, so the session-tree probe is opt-in
   (`include_session_leaf=True`) and never runs by default. To check:
   `src/modes/rpc/rpc-mode.ts` `get_entries` case.

── What this module will not do ─────────────────────────────────────

If pi is not installed, this raises `BackendUnavailable` naming the missing
executable and the command that installs it. It does not fall back to
`LocalBackend`; it does not import `LocalBackend`; a test asserts that this
module's source never mentions it. An RPC connection that never comes up,
or that dies mid-task, is reported as exactly that — never as an empty
success.
"""

from __future__ import annotations

import json
import os
import queue
import shutil
import subprocess
import threading
import time
from collections import deque
from collections.abc import Mapping, Sequence
from typing import Any, Literal, Protocol, TYPE_CHECKING, TypedDict

from pulse_system.agent.backends.base import (
    BackendError,
    BackendResult,
    BackendUnavailable,
    TaskSpec,
)

if TYPE_CHECKING:
    from pulse_system.agent.harness.process_containment import ContainedProcessOwner


_PI_PROCESS_BASE_ENV_KEYS = (
    "COMSPEC",
    "LANG",
    "LC_ALL",
    "NODE_EXTRA_CA_CERTS",
    "PATH",
    "PATHEXT",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "SystemRoot",
    "TEMP",
    "TMP",
    "TMPDIR",
    "WINDIR",
)
_PI_PROVIDER_ENV_KEYS: dict[str, tuple[str, ...]] = {
    "ai-gateway": ("AI_GATEWAY_API_KEY",),
    "amazon-bedrock": (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_BEARER_TOKEN_BEDROCK",
        "AWS_REGION",
    ),
    "anthropic": ("ANTHROPIC_OAUTH_TOKEN", "ANTHROPIC_API_KEY"),
    "ant-ling": ("ANT_LING_API_KEY",),
    "azure-openai-responses": (
        "AZURE_OPENAI_API_KEY",
        "AZURE_OPENAI_BASE_URL",
        "AZURE_OPENAI_RESOURCE_NAME",
        "AZURE_OPENAI_API_VERSION",
        "AZURE_OPENAI_DEPLOYMENT_NAME_MAP",
    ),
    "cerebras": ("CEREBRAS_API_KEY",),
    "cloudflare-ai-gateway": (
        "CLOUDFLARE_API_KEY",
        "CLOUDFLARE_ACCOUNT_ID",
        "CLOUDFLARE_GATEWAY_ID",
    ),
    "cloudflare-workers-ai": (
        "CLOUDFLARE_API_KEY",
        "CLOUDFLARE_ACCOUNT_ID",
    ),
    "deepseek": ("DEEPSEEK_API_KEY",),
    "fireworks": ("FIREWORKS_API_KEY",),
    "google": ("GEMINI_API_KEY",),
    "google-vertex": ("GOOGLE_CLOUD_API_KEY",),
    "groq": ("GROQ_API_KEY",),
    "huggingface": ("HF_TOKEN",),
    "kimi-coding": ("KIMI_API_KEY",),
    "minimax": ("MINIMAX_API_KEY",),
    "minimax-cn": ("MINIMAX_CN_API_KEY",),
    "mistral": ("MISTRAL_API_KEY",),
    "moonshotai": ("MOONSHOT_API_KEY",),
    "moonshotai-cn": ("MOONSHOT_API_KEY",),
    "nvidia": ("NVIDIA_API_KEY",),
    "opencode": ("OPENCODE_API_KEY",),
    "opencode-go": ("OPENCODE_API_KEY",),
    "openai": ("OPENAI_API_KEY",),
    "openrouter": ("OPENROUTER_API_KEY",),
    "together": ("TOGETHER_API_KEY",),
    "xai": ("XAI_API_KEY",),
    "xiaomi": ("XIAOMI_API_KEY",),
    "xiaomi-token-plan-ams": ("XIAOMI_TOKEN_PLAN_AMS_API_KEY",),
    "xiaomi-token-plan-cn": ("XIAOMI_TOKEN_PLAN_CN_API_KEY",),
    "xiaomi-token-plan-sgp": ("XIAOMI_TOKEN_PLAN_SGP_API_KEY",),
    "zai": ("ZAI_API_KEY",),
    "zai-coding-cn": ("ZAI_CODING_CN_API_KEY",),
}


def _minimal_pi_environment(provider: str | None) -> dict[str, str]:
    """Build the default child environment without ambient credentials.

    Explicit ``env=`` remains an exact caller-owned override.  The default
    production path carries only process launch essentials and credentials
    documented for the selected provider; unrelated API keys, auth/session
    roots and Node injection flags never cross into Pi.
    """

    source = os.environ
    names = list(_PI_PROCESS_BASE_ENV_KEYS)
    normalized_provider = provider.strip().casefold() if isinstance(provider, str) else ""
    names.extend(_PI_PROVIDER_ENV_KEYS.get(normalized_provider, ()))
    return {
        name: source[name]
        for name in names
        if isinstance(source.get(name), str) and source[name]
    }

__all__ = [
    "PI_INSTALL_HINT",
    "PI_NPM_PACKAGE",
    "PiBackend",
    "PiTransportCloseSummary",
    "RpcConnectionLost",
    "RpcTimeout",
    "RpcTransport",
    "SubprocessRpcTransport",
]

#: The npm package that provides the `pi` executable, and how to get it.
#: Confirmed from the upstream package manifest: the package provides the
#: `pi` executable and requires Node >= 22.19.
PI_NPM_PACKAGE = "@earendil-works/pi-coding-agent"
PI_INSTALL_HINT = (
    f"install pi with: npm install -g --ignore-scripts {PI_NPM_PACKAGE} "
    "(requires Node >= 22.19); then confirm with `pi --version`. "
    "If it is installed somewhere off PATH, pass "
    "PiBackend(executable='/absolute/path/to/pi'). Docs: https://pi.dev/docs/latest"
)

#: Confirmed: src/cli/args.ts:78 accepts --mode text|json|rpc.
PI_RPC_ARGS = ("--mode", "rpc")

#: Confirmed: rpc-client.ts stop() — SIGTERM, then SIGKILL after 1 s.
_KILL_GRACE_SEC = 1.0

#: Confirmed: rpc-mode.ts registerSignalHandlers — SIGTERM exits 143,
#: SIGHUP exits 129. Any negative code is a POSIX signal death.
_PI_SIGNAL_EXIT_CODES = (129, 143)


class PiTransportCloseSummary(TypedDict):
    """Content-free evidence produced by one Pi transport shutdown.

    ``empty_verified`` is emitted only from the exact shared contained-process
    owner.  A root return code or joined reader thread is never promoted into
    descendant-tree proof.
    """

    signal_sent: bool
    process_owners_observed: int
    process_owners_unresolved: int
    reader_owners_observed: int
    reader_owners_unresolved: int
    internal_owner_unresolved: int
    owner_joined: bool
    process_tree_state: Literal[
        "not_applicable",
        "empty_verified",
        "root_exit_only",
        "unknown",
    ]
    returncode: int | None
    error_code: str | None


class RpcTimeout(Exception):
    """A transport read ran out of time. Part of the transport contract."""


class RpcConnectionLost(Exception):
    """pi's stdout reached EOF, or its stdin died — the process is gone."""


class RpcTransport(Protocol):
    """The seam between this driver and a live pi process.

    Exists so the protocol logic can be tested without pi installed, and so
    an embedder can route RPC over something other than a local subprocess.
    Injecting a transport replaces the executor; it never replaces pi with a
    different *kind* of agent, which is the substitution this package
    forbids.

    Both exceptions in the contract are public (`RpcTimeout`,
    `RpcConnectionLost`) precisely so an outside implementation can satisfy
    it without reaching into private names.
    """

    def send_line(self, text: str) -> None:
        """Write one LF-terminated JSONL record to pi's stdin."""
        ...

    def read_line(self, timeout: float | None) -> str | None:
        """One line from pi's stdout, without its trailing newline.

        Returns `None` at EOF. Raises `RpcTimeout` if `timeout` elapses
        first. `timeout=None` means wait indefinitely.
        """
        ...

    def close(self) -> PiTransportCloseSummary | None:
        """Shut pi down and, when supported, return typed owner evidence."""
        ...

    def diagnostics(self) -> dict[str, Any]:
        """`{"returncode": int|None, "stderr_tail": str}` for error detail."""
        ...


class SubprocessRpcTransport:
    """A real `pi --mode rpc` child process.

    The pipes are **binary**, not text mode, on purpose. pi's framing is
    LF-only by explicit design (`jsonl.ts`), and Python's text-mode writer
    would translate every `\\n` into `\\r\\n` on Windows. pi tolerates that
    — its reader strips a trailing `\\r` — but relying on the other side's
    forgiveness to paper over framing we control is the wrong way round.
    Encoding and line splitting are done here, exactly once.

    stdout is drained by a reader thread into a queue, because neither
    `select()` on pipes nor non-blocking reads are portable to Windows, and
    this repository is developed there. stderr is drained separately into a
    bounded deque so a failing pi's diagnostics survive without unbounded
    memory.
    """

    _EOF = object()

    def __init__(
        self,
        argv: list[str],
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        stderr_lines: int = 200,
        start_readers: bool = True,
    ):
        # Import lazily: importing a harness submodule while this backend
        # module itself is initialising would execute harness.__init__, which
        # imports PiBackend.  Construction happens after module initialisation
        # and therefore has no package-cycle ambiguity.
        from pulse_system.agent.harness.process_containment import (
            ContainedProcessOwner,
            spawn_contained_process,
        )

        owner = spawn_contained_process(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=cwd,
            env=env,
        )
        if type(owner) is not ContainedProcessOwner:
            raise TypeError("Pi contained spawn returned a non-canonical owner")
        self._owner: ContainedProcessOwner = owner
        self._proc = owner.process
        self._lines: queue.Queue[Any] = queue.Queue()
        self._stderr: deque[str] = deque(maxlen=stderr_lines)
        self._closed = False
        self._close_lock = threading.RLock()
        self._wait_lock = threading.Lock()
        self._reader_start_lock = threading.Lock()
        self._signal_sent = False
        self._signal_error_code: str | None = None
        self._tree_termination_attempted = False
        self._close_summary: PiTransportCloseSummary | None = None
        self._readers = [
            threading.Thread(
                target=self._pump_stdout,
                name=f"pi-stdout-reader-{self._proc.pid}",
                daemon=True,
            ),
            threading.Thread(
                target=self._pump_stderr,
                name=f"pi-stderr-reader-{self._proc.pid}",
                daemon=True,
            ),
        ]
        self._reader_started = [False for _reader in self._readers]
        if start_readers:
            try:
                self.start_readers()
            except RpcConnectionLost:
                # Direct users historically receive an active transport from
                # the constructor.  If that compatibility path cannot start
                # every reader, synchronously converge the exact contained
                # owner before the object becomes unreachable.  The
                # persistent Harness uses deferred activation instead, so its
                # session owns this transport before any Thread.start edge.
                self.signal_close()
                self.wait_closed(timeout_sec=2.25)
                raise

    def start_readers(self) -> None:
        """Start both pipe pumps transactionally from an already-owned object.

        The process owner exists before these threads.  Callers that need a
        retained-owner guarantee therefore construct with
        ``start_readers=False``, publish the transport into their owner cell,
        and call this method only afterwards.  A partial start remains fully
        represented by ``_reader_started`` and can be closed/reobserved.
        """

        with self._reader_start_lock:
            with self._close_lock:
                if self._closed:
                    raise RpcConnectionLost(
                        "pi transport closed before reader activation"
                    )
                # Hold the close fence through every Thread.start commit.
                # signal_close therefore observes either the complete set or
                # the exact partial set; it can never retire the process and
                # then allow a late reader to become an uncounted owner.
                for index, reader in enumerate(self._readers):
                    if self._reader_started[index]:
                        continue
                    try:
                        reader.start()
                    except RuntimeError as exc:
                        self._signal_error_code = "pi_reader_start_failed"
                        raise RpcConnectionLost(
                            f"Pi transport reader could not start: {exc}"
                        ) from exc
                    self._reader_started[index] = True

    @staticmethod
    def _decode(raw: bytes) -> str:
        # Binary iteration splits on b"\n" only — strict JSONL framing.
        # A trailing "\r" is stripped for the same reason pi's reader does.
        return raw.decode("utf-8", errors="replace").rstrip("\n").rstrip("\r")

    def _pump_stdout(self) -> None:
        try:
            for raw in self._proc.stdout:  # type: ignore[union-attr]
                self._lines.put(self._decode(raw))
        except (OSError, ValueError):
            pass
        finally:
            self._lines.put(self._EOF)

    def _pump_stderr(self) -> None:
        try:
            for raw in self._proc.stderr:  # type: ignore[union-attr]
                self._stderr.append(self._decode(raw) + "\n")
        except (OSError, ValueError):
            pass

    def send_line(self, text: str) -> None:
        stdin = self._proc.stdin
        if stdin is None:
            raise RpcConnectionLost("pi stdin is not open")
        if not text.endswith("\n"):
            text += "\n"
        try:
            stdin.write(text.encode("utf-8"))
            stdin.flush()
        except (OSError, ValueError) as exc:
            # pi died between our liveness check and this write.
            raise RpcConnectionLost(f"pi stdin is not writable: {exc}") from exc

    def read_line(self, timeout: float | None) -> str | None:
        try:
            item = self._lines.get(timeout=timeout)
        except queue.Empty:
            raise RpcTimeout from None
        if item is self._EOF:
            return None
        return item

    def signal_close(self) -> bool:
        """Close stdin once without waiting for process or reader exit.

        Fleet shutdown calls this phase for every resident session before it
        waits on any process.  Tree termination is performed once by the first
        bounded ``wait_closed`` generation, after every fleet signal has been
        dispatched.
        """

        with self._close_lock:
            if self._signal_sent:
                return True
            self._signal_sent = True
            self._closed = True
            proc = self._proc
            try:
                if proc.stdin is not None:
                    proc.stdin.close()
            except (OSError, ValueError):
                self._signal_error_code = "pi_stdin_close_failed"
            return True

    def wait_closed(self, timeout_sec: float | None = None) -> PiTransportCloseSummary:
        """Bound the root wait and reader joins, returning explicit evidence."""

        if timeout_sec is not None and timeout_sec < 0:
            raise ValueError("timeout_sec must be non-negative or None")
        self.signal_close()
        with self._wait_lock:
            with self._close_lock:
                if self._summary_is_final(self._close_summary):
                    assert self._close_summary is not None
                    return dict(self._close_summary)

            budget = 2.0 * _KILL_GRACE_SEC if timeout_sec is None else timeout_sec
            deadline = time.monotonic() + budget
            proc = self._proc

            with self._close_lock:
                terminate_tree = (
                    not self._tree_termination_attempted
                    and deadline > time.monotonic()
                )
                if terminate_tree:
                    # A retry may observe the same owner, but must not signal
                    # it a second time.  Claim before entering native code so
                    # even an exception cannot create a duplicate signal.
                    self._tree_termination_attempted = True

            observation = None
            observation_error: str | None = None
            try:
                if terminate_tree:
                    observation = self._owner.terminate_tree(deadline)
                else:
                    observation = self._owner.observe()
            except Exception as exc:
                observation_error = (
                    f"pi_process_owner_observe_{type(exc).__name__}"
                )

            root_exited = bool(
                observation is not None
                and getattr(observation, "root_observed", False) is True
                and getattr(observation, "root_exited", False) is True
            )

            # Closing the read streams wakes pumps after a confirmed root
            # exit. Their liveness remains visible when the shared deadline
            # cannot join them.
            if root_exited:
                for stream in (proc.stdout, proc.stderr):
                    try:
                        if stream is not None:
                            stream.close()
                    except (OSError, ValueError):
                        pass
            for index, reader in enumerate(self._readers):
                if not self._reader_started[index]:
                    continue
                remaining = max(0.0, deadline - time.monotonic())
                if remaining <= 0:
                    break
                reader.join(timeout=remaining)

            tree = "unknown"
            resource_converged = False
            owner_error: str | None = None
            if observation is not None:
                try:
                    from pulse_system.agent.harness.process_containment import (
                        PhysicalProcessObservation,
                    )

                    if type(observation) is PhysicalProcessObservation:
                        tree = observation.tree_state.value
                        resource_converged = observation.resource_converged
                        if type(resource_converged) is not bool:
                            resource_converged = False
                            observation_error = (
                                "pi_process_owner_contract_invalid"
                            )
                        owner_error = observation.error_code
                    else:
                        observation_error = "pi_process_owner_contract_missing"
                except (AttributeError, ValueError):
                    observation_error = "pi_process_owner_contract_invalid"

            returncode = proc.poll()
            # Tree proof and retained-witness release are independent.  In
            # particular, EMPTY_VERIFIED with an unreleased Job handle must
            # stay unresolved and eligible for another same-owner observe.
            process_unresolved = int(not resource_converged)
            reader_observed = sum(self._reader_started)
            reader_unresolved = sum(
                reader.is_alive()
                for index, reader in enumerate(self._readers)
                if self._reader_started[index]
            )
            with self._close_lock:
                error_code = self._signal_error_code
            error_code = error_code or observation_error or owner_error
            if tree == "empty_verified" and process_unresolved:
                error_code = (
                    error_code or "pi_process_witness_release_unproven"
                )
            elif tree == "root_exit_only":
                error_code = error_code or "pi_process_tree_exit_unproven"
            elif process_unresolved:
                error_code = error_code or "pi_process_exit_unproven"
            elif reader_unresolved:
                error_code = error_code or "pi_transport_reader_exit_unproven"
            owner_joined = (
                process_unresolved == 0
                and reader_unresolved == 0
                and tree in {"not_applicable", "empty_verified"}
            )
            if owner_joined:
                # The activation exception is already carried by the caller;
                # final close evidence describes the retained owner's current
                # physical state and therefore has no lingering close error.
                error_code = None
            summary: PiTransportCloseSummary = {
                "signal_sent": self._signal_sent,
                "process_owners_observed": 1,
                "process_owners_unresolved": process_unresolved,
                "reader_owners_observed": reader_observed,
                "reader_owners_unresolved": reader_unresolved,
                "internal_owner_unresolved": reader_unresolved,
                "owner_joined": owner_joined,
                "process_tree_state": tree,
                "returncode": returncode if type(returncode) is int else None,
                "error_code": error_code,
            }
            with self._close_lock:
                self._close_summary = self._merge_close_summary(
                    self._close_summary,
                    summary,
                )
                return dict(self._close_summary)

    def close(self) -> PiTransportCloseSummary:
        self.signal_close()
        return self.wait_closed()

    def diagnostics(self) -> dict[str, Any]:
        return {
            "returncode": self._proc.poll(),
            "stderr_tail": "".join(self._stderr).strip(),
        }

    @staticmethod
    def _summary_is_final(summary: PiTransportCloseSummary | None) -> bool:
        return bool(
            summary is not None
            and summary["owner_joined"]
            and summary["process_owners_unresolved"] == 0
            and summary["reader_owners_unresolved"] == 0
            and summary["internal_owner_unresolved"] == 0
            and summary["process_tree_state"]
            in {"not_applicable", "empty_verified"}
        )

    @classmethod
    def _merge_close_summary(
        cls,
        current: PiTransportCloseSummary | None,
        candidate: PiTransportCloseSummary,
    ) -> PiTransportCloseSummary:
        if current is None:
            return candidate
        tree_rank = {
            "unknown": 0,
            "root_exit_only": 1,
            "not_applicable": 2,
            "empty_verified": 3,
        }
        tree = max(
            (current["process_tree_state"], candidate["process_tree_state"]),
            key=tree_rank.__getitem__,
        )
        process_unresolved = min(
            current["process_owners_unresolved"],
            candidate["process_owners_unresolved"],
        )
        reader_unresolved = min(
            current["reader_owners_unresolved"],
            candidate["reader_owners_unresolved"],
        )
        internal_unresolved = min(
            current["internal_owner_unresolved"],
            candidate["internal_owner_unresolved"],
        )
        owner_joined = (
            process_unresolved == 0
            and reader_unresolved == 0
            and internal_unresolved == 0
            and tree in {"not_applicable", "empty_verified"}
        )
        return {
            "signal_sent": current["signal_sent"] or candidate["signal_sent"],
            "process_owners_observed": max(
                current["process_owners_observed"],
                candidate["process_owners_observed"],
            ),
            "process_owners_unresolved": process_unresolved,
            "reader_owners_observed": max(
                current["reader_owners_observed"],
                candidate["reader_owners_observed"],
            ),
            "reader_owners_unresolved": reader_unresolved,
            "internal_owner_unresolved": internal_unresolved,
            "owner_joined": owner_joined,
            "process_tree_state": tree,
            "returncode": (
                candidate["returncode"]
                if candidate["returncode"] is not None
                else current["returncode"]
            ),
            "error_code": None if owner_joined else candidate["error_code"],
        }


class PiBackend:
    """Runs a delegated task inside a `pi --mode rpc` process.

    `transport_factory` is the test/embedding seam: it receives the argv
    this driver would have spawned and returns an `RpcTransport`. When it is
    supplied, PATH resolution is skipped — the caller has taken
    responsibility for producing a live pi.  A context-aware factory object
    may additionally implement ``open_transport(argv, *, cwd, env)``.  That
    form preserves per-process workspace and environment isolation (notably
    Pi's agent/session roots) without breaking historical argv-only callables.
    """

    name = "pi"

    def __init__(
        self,
        executable: str = "pi",
        *,
        workdir: str | os.PathLike[str] | None = None,
        provider: str | None = None,
        model: str | None = None,
        env: dict[str, str] | None = None,
        handshake_timeout_sec: float = 30.0,
        max_trace_events: int = 500,
        include_session_leaf: bool = False,
        transport_factory: Any = None,
        extra_args: Sequence[str] = (),
        launcher_args: Sequence[str] = (),
    ):
        if not isinstance(extra_args, (tuple, list)):
            raise TypeError("extra_args must be a tuple or list of argv strings")
        if any(not isinstance(arg, str) or not arg for arg in extra_args):
            raise ValueError("extra_args must contain non-empty strings")
        if not isinstance(launcher_args, (tuple, list)):
            raise TypeError("launcher_args must be a tuple or list of argv strings")
        if any(not isinstance(arg, str) or not arg for arg in launcher_args):
            raise ValueError("launcher_args must contain non-empty strings")
        if env is not None:
            if not isinstance(env, Mapping):
                raise TypeError("env must be a string mapping or None")
            if any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in env.items()
            ):
                raise ValueError("env keys and values must be strings")
        self._executable = executable
        self._workdir = os.fspath(workdir) if workdir is not None else None
        self._provider = provider
        self._model = model
        self._env = (
            dict(env)
            if env is not None
            else _minimal_pi_environment(provider)
        )
        self._handshake_timeout = handshake_timeout_sec
        self._max_trace = max_trace_events
        self._include_leaf = include_session_leaf
        self._transport_factory = transport_factory
        self._extra_args = tuple(extra_args)
        self._launcher_args = tuple(launcher_args)
        self._request_seq = 0

    # ── Availability ─────────────────────────────────────────────

    def resolve_executable(self) -> str:
        """Absolute path to pi, or raise `BackendUnavailable` naming it."""
        if self._transport_factory is not None:
            return self._executable
        found = shutil.which(self._executable)
        if found is None:
            raise BackendUnavailable(
                "pi_not_installed",
                f"no executable named {self._executable!r} was found on PATH, "
                f"so the pi backend has nothing to talk to",
                PI_INSTALL_HINT,
            )
        return found

    def preflight(self) -> None:
        self.resolve_executable()

    def argv(self) -> list[str]:
        """The exact command line this backend spawns."""
        argv = [self.resolve_executable(), *self._launcher_args, *PI_RPC_ARGS]
        if self._provider:
            argv += ["--provider", self._provider]
        if self._model:
            argv += ["--model", self._model]
        argv += list(self._extra_args)
        return argv

    def for_session(
        self,
        *,
        extra_args: Sequence[str] = (),
        env: Mapping[str, str] | None = None,
    ) -> "PiBackend":
        """Return an immutable per-process derivative of this backend.

        The runtime owns one configured backend template and derives this
        object for each live Pi process.  The legacy ``transport_factory``
        still receives only argv; session env is applied only to the real
        subprocess transport, so existing fake transports remain source
        compatible.

        ``env`` is an overlay.  With no base env, an explicit copy of the
        parent environment is made to match Pi's Node RPC client, which
        merges ``process.env`` with its per-client overrides.  With an
        explicit base env, the historical exact-dictionary behavior is
        preserved and the overlay is applied on top.
        """

        if not isinstance(extra_args, (tuple, list)):
            raise TypeError("extra_args must be a tuple or list of argv strings")
        if any(not isinstance(arg, str) or not arg for arg in extra_args):
            raise ValueError("extra_args must contain non-empty strings")
        if env is not None:
            if not isinstance(env, Mapping):
                raise TypeError("env must be a string mapping or None")
            if any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in env.items()
            ):
                raise ValueError("env keys and values must be strings")

        merged_env = dict(self._env)
        if env is not None:
            merged_env.update(env)

        return PiBackend(
            self._executable,
            workdir=self._workdir,
            provider=self._provider,
            model=self._model,
            env=merged_env,
            handshake_timeout_sec=self._handshake_timeout,
            max_trace_events=self._max_trace,
            include_session_leaf=self._include_leaf,
            transport_factory=self._transport_factory,
            extra_args=(*self._extra_args, *tuple(extra_args)),
            launcher_args=self._launcher_args,
        )

    # ── Execution ────────────────────────────────────────────────

    def submit(self, spec: TaskSpec) -> BackendResult:
        transport = self.open_transport(defer_reader_start=True)
        try:
            start_readers = getattr(transport, "start_readers", None)
            if callable(start_readers):
                start_readers()
            session = _Session(self, transport, spec)
            return session.run()
        finally:
            transport.close()

    def open_transport(self, *, defer_reader_start: bool = False) -> RpcTransport:
        """Open one configured pi RPC transport.

        The persistent Harness runtime uses this public seam to share the
        exact executable resolution, argv, cwd, environment, and injected
        transport behavior of the one-shot backend without depending on its
        private ``_open`` helper.  Opening a transport never substitutes a
        different executor when pi is unavailable.
        """
        argv = self.argv()  # raises BackendUnavailable when pi is absent
        try:
            return self._open(argv, defer_reader_start=defer_reader_start)
        except OSError as exc:
            # which() found it, exec did not: wrong architecture, lost
            # permission bit, Node missing. Nothing ran, so this raises.
            raise BackendUnavailable(
                "pi_not_executable",
                f"found {argv[0]!r} but could not start it: "
                f"{type(exc).__name__}: {exc}",
                PI_INSTALL_HINT,
            ) from exc

    def _open(
        self,
        argv: list[str],
        *,
        defer_reader_start: bool = False,
    ) -> RpcTransport:
        if self._transport_factory is not None:
            contextual_open = getattr(
                self._transport_factory,
                "open_transport",
                None,
            )
            if callable(contextual_open):
                return contextual_open(
                    list(argv),
                    cwd=self._workdir,
                    env=dict(self._env),
                )
            return self._transport_factory(argv)
        return SubprocessRpcTransport(
            argv,
            cwd=self._workdir,
            env=self._env,
            start_readers=not defer_reader_start,
        )

    def _next_id(self) -> str:
        self._request_seq += 1
        return f"pulse-{self._request_seq}"


class _Session:
    """One prompt's worth of conversation with a live pi process."""

    def __init__(self, backend: PiBackend, transport: RpcTransport, spec: TaskSpec):
        self._b = backend
        self._t = transport
        self._spec = spec
        self._trace: list[dict[str, Any]] = []
        self._dropped = 0
        self._last_assistant: dict[str, Any] | None = None
        self._deadline = (
            None if spec.timeout_sec is None else time.monotonic() + spec.timeout_sec
        )

    # ── Public ───────────────────────────────────────────────────

    def run(self) -> BackendResult:
        if self._spec.target is not None:
            # pi has no engrams. Say so in the trace rather than dropping it.
            self._note(
                "spec.target_ignored",
                target=self._spec.target,
                reason="pi runs its own session tree; it has no engram to target",
            )
        try:
            return self._converse()
        except RpcTimeout:
            return self._fail(
                "pi_timeout",
                f"pi did not finish within {self._spec.timeout_sec} s; "
                f"the process was terminated{self._where()}",
                "raise TaskSpec.timeout_sec, split the task, or set "
                "timeout_sec=None to wait indefinitely",
                partial=True,
            )
        except RpcConnectionLost as exc:
            return self._lost(str(exc))

    # ── Conversation ─────────────────────────────────────────────

    def _converse(self) -> BackendResult:
        # 1. Liveness. A reply to get_state proves pi is up; a sleep would
        #    prove nothing. Its own client sleeps 100 ms instead.
        state = self._request(
            {"type": "get_state"}, timeout=self._b._handshake_timeout
        )
        if not state.get("success"):
            return self._refused("pi_handshake_refused", state, "get_state")
        session_state = state.get("data") or {}
        self._note(
            "pi.session",
            session_id=session_state.get("sessionId"),
            session_file=session_state.get("sessionFile"),
            model=(session_state.get("model") or {}).get("id"),
        )

        # 2. The task, verbatim. No framing, no system-prompt smuggling.
        started = self._request({"type": "prompt", "message": self._spec.task})
        if not started.get("success"):
            return self._refused("pi_prompt_refused", started, "prompt")

        # 3. Wait for the turn to settle. Events stream into the trace.
        self._await_settled()

        # 4. The authoritative final text, from pi rather than reassembled
        #    by us. Falls back to text harvested from pi's own events — the
        #    same executor either way.
        text, text_note = self._final_text()
        if self._b._include_leaf:
            self._probe_session_leaf()

        return self._verdict(text, text_note)

    def _await_settled(self) -> None:
        while True:
            obj = self._read()
            if obj is None:
                raise RpcConnectionLost("pi closed stdout before the turn settled")
            self._absorb(obj)
            if obj.get("type") == "agent_settled":
                return

    def _final_text(self) -> tuple[str, str | None]:
        reply = self._request({"type": "get_last_assistant_text"})
        if reply.get("success"):
            text = (reply.get("data") or {}).get("text")
            if isinstance(text, str) and text.strip():
                return text, None
            return "", "pi reported no final assistant text"
        harvested = self._harvest_text()
        note = f"get_last_assistant_text failed: {reply.get('error')!r}"
        return harvested, note

    def _probe_session_leaf(self) -> None:
        """Record pi's session leaf — its analogue of a succession point.

        pi's session is a JSONL DAG (`harness/session/jsonl-repo.ts`) where
        `getBranch() == getPathToRoot(leafId)`. The leaf is the handle a
        later call would fork from, so it is worth carrying back. Opt-in:
        see UNVERIFIED note 4 about payload size.
        """
        reply = self._request({"type": "get_entries"})
        if reply.get("success"):
            data = reply.get("data") or {}
            self._note(
                "pi.session_leaf",
                leaf_id=data.get("leafId"),
                entry_count=len(data.get("entries") or []),
            )
        else:
            self._note("pi.session_leaf", leaf_id=None, error=reply.get("error"))

    # ── Verdict ──────────────────────────────────────────────────

    def _verdict(self, text: str, text_note: str | None) -> BackendResult:
        stop = (self._last_assistant or {}).get("stopReason")

        if stop == "length":
            # agent-loop.ts:383 refuses to let a truncated turn read as
            # finished; so do we.
            return self._fail(
                "pi_truncated",
                "pi's final turn stopped at the context limit "
                '(stopReason "length"), so it is cut off, not finished'
                + (f"; {len(text)} chars were produced" if text else ""),
                "shorten the task, raise pi's model context window "
                "(--model), or let pi compact and retry",
                output=text,
            )

        if stop in ("error", "aborted"):
            message = (self._last_assistant or {}).get("errorMessage") or stop
            return self._fail(
                "pi_agent_error",
                f'pi finished with stopReason "{stop}": {message}'
                + self._stderr_suffix(),
                "check pi's provider credentials and model with "
                "`pi --mode rpc` run by hand; see UNVERIFIED note 2 in "
                "this module about how key errors surface",
                output=text,
            )

        if not text.strip():
            # An empty answer is a failure to report, not a success to pass
            # on. This is the exact shape the package exists to prevent.
            return self._fail(
                "pi_empty_output",
                "pi settled without producing any assistant text"
                + (f" ({text_note})" if text_note else "")
                + self._stderr_suffix(),
                "run the same prompt under `pi --mode rpc` by hand to see "
                "what it emitted; check stderr for provider errors",
            )

        if text_note:
            self._note("pi.note", detail=text_note)
        return BackendResult(
            backend=self._b.name, ok=True, output=text, trace=self._finish_trace()
        )

    def _refused(self, code: str, reply: dict, command: str) -> BackendResult:
        return self._fail(
            code,
            f"pi refused the {command!r} command: {reply.get('error')!r}"
            + self._stderr_suffix(),
            "check that the installed pi is recent enough to support "
            f"{command!r} (`pi --version`); an unknown command reports as "
            '"Unknown command: <type>"',
        )

    def _lost(self, why: str) -> BackendResult:
        diag = self._t.diagnostics()
        rc = diag.get("returncode")
        killed = isinstance(rc, int) and (rc < 0 or rc in _PI_SIGNAL_EXIT_CODES)
        if killed:
            how = (
                f"killed by signal {-rc}"
                if isinstance(rc, int) and rc < 0
                else f"shut down on a signal (exit {rc}; pi maps SIGTERM->143, "
                f"SIGHUP->129)"
            )
            return self._fail(
                "pi_killed",
                f"the pi process was {how} before the task finished"
                + self._where()
                + self._stderr_suffix(),
                "check for an OOM killer, a CI job timeout, or a parent "
                "process shutting the tree down; then re-run",
                partial=True,
            )
        return self._fail(
            "pi_connection_lost",
            f"the pi RPC connection ended before the task finished "
            f"({why}; exit code {rc})" + self._where() + self._stderr_suffix(),
            "run the same command by hand to see pi's startup output: "
            f"`{' '.join(self._b.argv())}`",
            partial=True,
        )

    # ── Plumbing ─────────────────────────────────────────────────

    def _request(self, body: dict, *, timeout: float | None = None) -> dict:
        """Send a command and read until its correlated response arrives.

        Events seen while waiting are absorbed into the trace, exactly as
        pi's own client does — responses and events interleave by design.
        """
        req_id = self._b._next_id()
        payload = dict(body, id=req_id)
        self._t.send_line(json.dumps(payload, ensure_ascii=False) + "\n")
        while True:
            obj = self._read(timeout)
            if obj is None:
                raise RpcConnectionLost(
                    f"pi closed stdout while awaiting a response to "
                    f"{body['type']!r}"
                )
            if (
                obj.get("type") == "response"
                and obj.get("id") == req_id
            ):
                return obj
            self._absorb(obj)

    def _read(self, timeout: float | None = None) -> dict | None:
        """One decoded JSONL object, or None at EOF."""
        while True:
            line = self._t.read_line(self._budget(timeout))
            if line is None:
                return None
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                # pi's own client ignores non-JSON lines; we keep a bounded
                # record so unparsable output is visible, not invisible.
                self._note("pi.unparsable_line", text=line[:400])
                continue
            if isinstance(obj, dict):
                return obj
            self._note("pi.unexpected_json", text=line[:400])

    def _budget(self, timeout: float | None) -> float | None:
        """Whichever runs out first: this step's timeout or the deadline."""
        candidates = [t for t in (timeout, self._remaining()) if t is not None]
        if not candidates:
            return None
        budget = min(candidates)
        if budget <= 0:
            raise RpcTimeout
        return budget

    def _remaining(self) -> float | None:
        if self._deadline is None:
            return None
        return self._deadline - time.monotonic()

    def _absorb(self, obj: dict) -> None:
        """Record an event, and remember the newest assistant message."""
        for message in _assistant_messages(obj):
            self._last_assistant = message
        if len(self._trace) < self._b._max_trace:
            self._trace.append(dict(obj, kind="pi.event"))
        else:
            self._dropped += 1

    def _note(self, kind: str, **fields: Any) -> None:
        self._trace.append(dict(fields, kind=kind))

    def _finish_trace(self) -> list[dict[str, Any]]:
        if self._dropped:
            self._trace.append({
                "kind": "pi.trace_truncated",
                "dropped_events": self._dropped,
                "kept_events": self._b._max_trace,
                "detail": "raise PiBackend(max_trace_events=...) to keep more",
            })
            self._dropped = 0
        return self._trace

    def _harvest_text(self) -> str:
        """Assistant text reassembled from pi's own streamed events."""
        if self._last_assistant is None:
            return ""
        return _text_of(self._last_assistant)

    def _where(self) -> str:
        return f" (after {len(self._trace)} trace entries)"

    def _stderr_suffix(self) -> str:
        tail = (self._t.diagnostics() or {}).get("stderr_tail") or ""
        if not tail:
            return ""
        return f"; pi stderr: {tail[-600:]}"

    def _fail(
        self,
        code: str,
        detail: str,
        remedy: str,
        *,
        output: str = "",
        partial: bool = False,
    ) -> BackendResult:
        if partial and not output:
            output = self._harvest_text()
        return BackendResult(
            backend=self._b.name,
            ok=False,
            output=output,
            trace=self._finish_trace(),
            error=BackendError(code, detail, remedy),
        )


# ── Event shape helpers (pure, so they are testable on their own) ──


def _assistant_messages(event: dict) -> list[dict]:
    """Every assistant message carried by one pi event, oldest first.

    Confirmed from the upstream JSON event contract:
    `message_start`/`message_update`/`message_end` carry `message`;
    `turn_end` carries `message`; `agent_end` carries `messages`.
    """
    found: list[dict] = []
    single = event.get("message")
    if isinstance(single, dict) and single.get("role") == "assistant":
        found.append(single)
    many = event.get("messages")
    if isinstance(many, list):
        found.extend(
            m for m in many if isinstance(m, dict) and m.get("role") == "assistant"
        )
    return found


def _text_of(message: dict) -> str:
    """Concatenated text blocks of an AssistantMessage.

    Confirmed: `content: (TextContent | ThinkingContent | ToolCall)[]`
    (packages/ai/src/types.ts:390). Thinking blocks and tool calls are not
    answer text and are deliberately excluded.
    """
    content = message.get("content")
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts = [
        block.get("text", "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text"
    ]
    return "\n".join(p for p in parts if p).strip()

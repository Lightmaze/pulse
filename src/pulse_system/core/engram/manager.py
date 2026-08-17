"""Engram management.

Higher-level engram operations on top of Storage + LLM:
- create, pulse, append_injection, succession, import_conversation
"""

from __future__ import annotations

import inspect
import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from pulse_system.agent.harness.base import HarnessError
from pulse_system.core.causality import CausalTransitionError
from pulse_system.core.causality.ledger import EngramPulseActivity, RuntimeFence
from pulse_system.core.connection.network import ConnectionNetwork
from pulse_system.core.types import (
    CausalEventDomain,
    CausalEventKind,
    CausalEventSource,
    CausalEventStatus,
    Engram,
    EngramStatus,
    GenerationTransitionState,
    Message,
    MessageRole,
    RuntimeLeaseLostError,
)
from pulse_system.substrate.llm.adapter import LLMAdapter, estimate_tokens
from pulse_system.substrate.storage.store import Storage


_logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pulse_system.agent.harness.base import HarnessRuntime
    from pulse_system.core.causality import CausalLedger


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class PulseResult:
    """Outcome of a single pulse, including per-call token usage.

    `tool_calls` is 0 on the ordinary path and names how many acts a
    tool-capable firing performed (see pulse(tools=...)). Token counts on that
    path are summed over every completion the firing made.
    """

    content: str
    input_tokens: int
    output_tokens: int
    cached_tokens: int
    cache_write_tokens: int = 0
    tool_calls: int = 0
    provider_requests: int = 0
    # Durable causal context.  The fields are optional so the legacy LLM and
    # non-ledger Harness adapters retain their historical constructor shape.
    event_id: str | None = None
    causal_id: str | None = None
    turn_id: str | None = None
    result_event_id: str | None = None


@dataclass
class SuccessionResult:
    """Outcome of a succession: the successor id plus the summary call's own
    token usage. The engine charges the budget from these per-call figures
    rather than a global stats delta, which would attribute concurrent
    front/clone/tick adapter traffic to the succession (succession continuity)."""

    new_id: str
    input_tokens: int
    output_tokens: int
    cached_tokens: int
    cache_write_tokens: int = 0
    generation_id: str | None = None
    causal_id: str | None = None
    summary_turn_id: str | None = None
    handed_off_event_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class SuccessionPreparation:
    """A settled summary and externally rotated, not-yet-live successor.

    No world mutation is represented here.  The Engine may compute this value
    in a bounded worker and later give it to the single coordinator thread for
    :meth:`EngramManager.commit_succession`.
    """

    predecessor_id: str
    successor_id: str
    input_tokens: int
    output_tokens: int
    cached_tokens: int
    cache_write_tokens: int = 0
    generation_id: str | None = None
    causal_id: str | None = None
    summary_turn_id: str | None = None

    @property
    def usage(self) -> "HarnessUsage":
        return HarnessUsage(
            input_tokens=self.input_tokens,
            output_tokens=self.output_tokens,
            cached_tokens=self.cached_tokens,
            cache_write_tokens=self.cache_write_tokens,
        )


@dataclass(frozen=True)
class HarnessUsage:
    """Usage already spent by a settled Harness turn."""

    input_tokens: int
    output_tokens: int
    cached_tokens: int
    cache_write_tokens: int = 0


class SuccessionHarnessError(HarnessError):
    """A rotate failure after its summary turn has already spent usage."""

    def __init__(self, cause: HarnessError, usage: HarnessUsage) -> None:
        super().__init__(
            cause.code,
            cause.detail,
            cause.remedy,
            phase=cause.phase,
            retryable=cause.retryable,
            prompt_accepted=cause.prompt_accepted,
            partial_output=cause.partial_output,
            trace=cause.trace,
        )
        self.usage = usage
        self._usage_claim_lock = threading.Lock()
        self._usage_claimed = False

    def claim_usage(self) -> HarnessUsage | None:
        """Return the spent usage to exactly one accounting consumer."""

        with self._usage_claim_lock:
            if self._usage_claimed:
                return None
            self._usage_claimed = True
            return self.usage


class SuccessionPreparationError(RuntimeError):
    """A post-summary failure whose already-spent usage must be charged."""

    def __init__(self, cause: Exception, usage: HarnessUsage) -> None:
        self.cause = cause
        self.usage = usage
        self._usage_claim_lock = threading.Lock()
        self._usage_claimed = False
        super().__init__(str(cause))

    def claim_usage(self) -> HarnessUsage | None:
        """Return the spent usage to exactly one accounting consumer."""

        with self._usage_claim_lock:
            if self._usage_claimed:
                return None
            self._usage_claimed = True
            return self.usage


_SUCCESSION_PROMPT = (
    "Please provide a comprehensive summary of everything discussed so far "
    "in this conversation. Capture the key ideas, conclusions, open questions, "
    "and any important context. This summary will serve as the foundation "
    "for continuing this line of thinking in a new session."
)

HARNESS_INPUT_COMPONENT = "harness.pulse.inputs.v1"
_HARNESS_INPUT_VERSION = 1


class EngramManager:
    """High-level engram lifecycle operations."""

    def __init__(
        self,
        storage: Storage,
        llm: LLMAdapter,
        connection_network: ConnectionNetwork,
        library=None,
        substrates=None,
        *,
        harness: "HarnessRuntime | None" = None,
        harness_turn_timeout_sec: float | None = None,
        causal_ledger: "CausalLedger | None" = None,
        causal_world_id: str | None = None,
    ):
        self._storage = storage
        self._llm = llm
        self._connections = connection_network
        # Optional Library: procedural memory that survives succession.
        self._library = library
        # Optional substrate registry SubstrateRegistry: per-engram compute binding. The
        # plain `llm` remains the default substrate and the network-level
        # service (embeddings) either way.
        self._substrates = substrates
        # The persistent Harness is the production cognition-and-action path.
        # None intentionally preserves the legacy/unit-test LLM path.
        self._harness = harness
        self._harness_turn_timeout_sec = harness_turn_timeout_sec
        # Optional durable execution ledger.  The Runtime composition root must
        # call recover_inflight() before constructing this manager; this class
        # deliberately does not recover or mutate scheduler state on its own.
        self._causal_ledger = causal_ledger
        self._causal_world_id = causal_world_id
        self._harness_bootstrapped: set[str] = set()
        self._harness_calls_guard = threading.Lock()
        self._harness_calls: dict[str, threading.RLock] = {}
        self._harness_cursor_lock = threading.RLock()
        self._harness_input_cursors = (
            self._load_harness_input_cursors() if harness is not None else {}
        )
        # Succession listeners (extension point): sideband components that
        # keep per-engram state (delegation router/Claustrum modulator slot maps, engine depth/inhibition)
        # subscribe here so successors inherit it — no hard dependencies.
        self._succession_listeners: list = []
        # Archive listeners: sideband components free the archived engram's
        # slot here (mask_engram), so temporary forks (snapshot/dream/sleep)
        # don't leak MLP slots. Distinct from succession — a successor inherits
        # its slot via reassign, so that path must not fire these.
        self._archive_listeners: list = []
        # Terminal-turn listeners observe an already durable Harness state.
        # They may reconcile settlement-fenced sideband domains, but they can
        # never change the turn outcome or make an accepted prompt retryable.
        self._turn_terminal_listeners: list = []

    def add_succession_listener(self, callback) -> None:
        """callback(old_id: str, new_id: str) — called after succession."""
        self._succession_listeners.append(callback)

    def add_archive_listener(self, callback) -> None:
        """callback(engram_id: str) — called after a successful archive()."""
        self._archive_listeners.append(callback)

    def add_turn_terminal_listener(self, callback) -> None:
        """Register ``callback(turn_id)`` after durable turn terminalization."""

        if not callable(callback):
            raise TypeError("turn terminal listener must be callable")
        self._turn_terminal_listeners.append(callback)

    def _notify_turn_terminal(self, turn_id: str) -> None:
        for callback in tuple(self._turn_terminal_listeners):
            try:
                callback(turn_id)
            except Exception:  # noqa: BLE001 - the turn is already terminal
                _logger.exception(
                    "turn terminal listener failed after durable settlement: %s",
                    turn_id,
                )

    def archive(self, engram_id: str) -> bool:
        """Archive an engram and notify archive listeners on success.

        Fork-and-discard paths (delegator snapshot, sleep dream/fork) route
        their archive here so listeners can release the slot the fork
        transiently occupied. Succession does NOT use this — see succession().
        """
        archived = self._storage.archive_engram(engram_id)
        if archived:
            for callback in self._archive_listeners:
                callback(engram_id)
        return archived

    @property
    def llm(self) -> LLMAdapter:
        return self._llm

    @property
    def causal_ledger(self) -> "CausalLedger | None":
        """The optional durable execution ledger used by Runtime."""

        return self._causal_ledger

    def set_causal_world_id(self, world_id: str) -> None:
        """Bind direct Manager succession to Runtime's one PulseWorld."""

        if not isinstance(world_id, str) or not world_id.strip():
            raise ValueError("causal world_id must be a non-empty string")
        self._causal_world_id = world_id

    @staticmethod
    def generation_candidate_id(generation_id: str) -> str:
        """Stable provisional successor id used to isolate crash orphans."""

        return uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"pulse-system:generation-successor:{generation_id}",
        ).hex

    def _adapter_for(self, engram: Engram):
        """Resolve the substrate for an engram (substrate registry); default without one."""
        if self._substrates is None or engram.substrate_binding is None:
            return self._llm
        return self._substrates.get(engram.substrate_binding)

    def bind_substrate(self, engram_id: str, binding: str | None) -> None:
        """Bind an engram to a named substrate (None reverts to default)."""
        if (
            binding is not None
            and self._substrates is not None
            and not self._substrates.has(binding)
        ):
            raise ValueError(f"unknown substrate binding: {binding}")
        if not self._storage.set_substrate_binding(engram_id, binding):
            raise ValueError(f"Engram {engram_id} not found")

    def combined_llm_stats(self):
        """Usage summed across all substrates (single-adapter = its stats)."""
        if self._substrates is not None:
            return self._substrates.combined_stats()
        return self._llm.get_stats()

    def create(
        self,
        project_id: str | None = None,
        initial_messages: list[Message] | None = None,
        *,
        auto_name: bool = True,
    ) -> Engram:
        engram = self._storage.create_engram(
            project_id=project_id,
            initial_messages=initial_messages,
            auto_name=auto_name,
        )
        if initial_messages:
            token_count = sum(
                estimate_tokens(m.content) for m in initial_messages
            )
            self._storage.update_engram_metadata(
                engram.id, token_count=token_count
            )
        return engram

    def pulse(
        self,
        engram_id: str,
        injected_context: str | None = None,
        source_engram_id: str | None = None,
        *,
        tools=None,
        runtime_config=None,
        pulse_event: Any = None,
        causal_retry_allowed: bool = False,
        runtime_fence: RuntimeFence | None = None,
    ) -> PulseResult:
        """Execute one pulse and append its final natural-text projection.

        With a persistent Harness, only messages after the last assistant
        projection become this turn's prompt. Older SQLite history is offered
        once as bootstrap text and is never the continuing transcript source.
        The Harness owns its complete model/tool loop, so the legacy adapter
        and optional Python tool loop are bypassed on that path.

        The messages sent to the LLM are the raw session history — no system
        prompt and no structured instructions — this remains the
        explicit ``harness=None`` compatibility path.

        Returns a PulseResult carrying the output content and this call's
        token usage (for incremental budget accounting).

        `tools` (a ToolRegistry) opts this firing into the engram runtime:
        the independent-Engram rule says every engram is a cognitive subject and the interaction
        spec says an engram in solitude has full action authority, so the
        ability to act belongs to firing rather than to one privileged agent.
        With a registry the firing runs pulse → act → append result as natural
        text → pulse again (see core/engram/runtime.py); the LLM still receives
        only the raw session, and results still arrive as plain language.

        The parameter is opt-in and off by default; omitting it preserves the
        raw-session execution path.
        """
        if pulse_event is not None and self._harness is None:
            raise ValueError(
                "durable PulseEvent execution requires a persistent Harness"
            )
        if runtime_fence is not None and pulse_event is None:
            raise ValueError("runtime_fence requires a durable PulseEvent")
        if self._harness is not None:
            self._assert_generation_pulse_admissible(engram_id)
            with self._harness_call_lock(engram_id):
                # The first check deliberately happens before waiting so a
                # known blocked lineage fails quickly.  The second is the
                # authoritative check: succession may have begun while this
                # pulse waited for the per-Engram Harness lock.
                self._assert_generation_pulse_admissible(engram_id)
                if pulse_event is not None:
                    return self._pulse_with_causal_event(
                        engram_id,
                        pulse_event=pulse_event,
                        injected_context=injected_context,
                        runtime_config=runtime_config,
                        causal_retry_allowed=causal_retry_allowed,
                        runtime_fence=runtime_fence,
                    )
                return self._pulse_with_harness(
                    engram_id,
                    injected_context=injected_context,
                    source_engram_id=source_engram_id,
                    runtime_config=runtime_config,
                )

        if tools is not None:
            from pulse_system.core.engram.runtime import EngramRuntime

            outcome = EngramRuntime(self, tools, runtime_config).run(
                engram_id,
                injected_context=injected_context,
                source_engram_id=source_engram_id,
            )
            return PulseResult(
                content=outcome.content,
                input_tokens=outcome.input_tokens,
                output_tokens=outcome.output_tokens,
                cached_tokens=outcome.cached_tokens,
                cache_write_tokens=outcome.cache_write_tokens,
                tool_calls=len(outcome.tool_calls),
            )

        engram = self._storage.get_engram(engram_id)
        if engram is None:
            raise ValueError(f"Engram {engram_id} not found")
        if engram.status != EngramStatus.ACTIVE:
            raise ValueError(f"Engram {engram_id} is archived")

        # Append injected context if provided
        if injected_context:
            self._storage.append_message(
                engram_id,
                Message(
                    role=MessageRole.INJECTION,
                    content=injected_context,
                    source_engram_id=source_engram_id,
                ),
            )

        # Build messages from session — raw pass-through (the free-context rule)
        session = self._storage.get_session(engram_id)
        messages = self._session_to_llm_messages(session)

        # Call the engram's bound substrate (substrate registry; default without a binding)
        result = self._adapter_for(engram).complete(messages)

        # Append LLM output back to session
        self._storage.append_message(
            engram_id,
            Message(role=MessageRole.ASSISTANT, content=result.content),
        )

        # Update metadata. token_count is the *current context size*: the
        # last call's input tokens already cover the whole session history,
        # so SET (not accumulate) — this is what succession thresholds on.
        now = _now()
        current = self._storage.get_engram(engram_id)
        new_pulses = (current.total_pulses if current else 0) + 1
        context_tokens = result.input_tokens + result.output_tokens
        activity = min(
            1.0,
            (current.metadata.recent_activity if current else 0) + 0.2,
        )
        self._storage.update_engram_metadata(
            engram_id,
            last_pulse_at=now,
            total_pulses=new_pulses,
            recent_activity=activity,
            token_count=context_tokens,
        )

        return PulseResult(
            content=result.content,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            cached_tokens=getattr(result, "cached_tokens", 0),
            cache_write_tokens=getattr(result, "cache_write_tokens", 0),
        )

    def _pulse_with_causal_event(
        self,
        engram_id: str,
        *,
        pulse_event: Any,
        injected_context: str | None,
        runtime_config,
        causal_retry_allowed: bool,
        runtime_fence: RuntimeFence | None = None,
    ) -> PulseResult:
        """Execute one ledger-backed Harness turn.

        The durable event store owns the input projection and transaction boundaries.  This
        method therefore never appends an injection or assistant message
        itself: ``begin_turn`` creates/reuses the input projection and
        ``settle_turn`` atomically commits the assistant, cursor, result
        child, and terminal states.  The exact ``cursor_before:cursor_after``
        range selected by the store is the only text sent to Pi.
        """

        if self._causal_ledger is None:
            raise ValueError(
                "a causal ledger is required for a durable PulseEvent"
            )
        assert self._harness is not None

        engram = self._storage.get_engram(engram_id)
        if engram is None:
            raise ValueError(f"Engram {engram_id} not found")
        if engram.status != EngramStatus.ACTIVE:
            raise ValueError(f"Engram {engram_id} is archived")

        event_id = getattr(pulse_event, "event_id", None)
        if event_id is None:
            event_id = getattr(pulse_event, "id", None)
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("durable PulseEvent requires event_id")

        event = self._causal_ledger.get_event(event_id)
        if event is None:
            raise ValueError(f"causal event {event_id} not found")
        if event.engram_id is not None and event.engram_id != engram_id:
            raise ValueError(
                f"causal event {event_id} belongs to {event.engram_id}, "
                f"not {engram_id}"
            )
        if injected_context is not None and injected_context != event.content:
            raise ValueError(
                "durable event content is the sole input projection; "
                "injected_context differs"
            )

        source_engram_id = getattr(pulse_event, "source_engram_id", None)
        if source_engram_id is None:
            candidate = event.metadata.get("source_engram_id")
            if isinstance(candidate, str) and candidate:
                source_engram_id = candidate
        input_message = None
        if event.content is not None:
            input_message = Message(
                role=MessageRole.INJECTION,
                content=event.content,
                source_engram_id=source_engram_id,
            )

        # This is the only pre-Pi durable write.  It claims the queued event,
        # persists/reuses its exact input projection, and records both message
        # boundaries before the Harness call begins.
        turn = self._causal_ledger.begin_turn(
            event.id,
            engram_id,
            input_message,
            runtime_fence=runtime_fence,
        )
        session = self._storage.get_session(engram_id)
        if not 0 <= turn.cursor_before <= turn.cursor_after <= len(session):
            raise RuntimeError(
                "durable Harness turn boundaries do not match the message index"
            )
        prompt = self._join_natural_messages(
            session[turn.cursor_before:turn.cursor_after]
        )

        timeout_sec = self._harness_turn_timeout_sec
        if runtime_config is not None:
            timeout_sec = getattr(runtime_config, "deadline_sec", timeout_sec)

        try:
            # Do not pass ledger metadata, event ids, or a duplicate bootstrap
            # copy into the prompt.  The selected message range is the prompt.
            harness_result = self._invoke_harness_turn(
                engram_id,
                prompt,
                timeout_sec=timeout_sec,
                bootstrap_text=None,
                turn_id=turn.id,
            )
        except HarnessError as exc:
            # Only an explicitly rejected prompt may requeue this same event.
            # Accepted and unknown outcomes become uncertain and advance the
            # anti-replay cursor inside the ledger transaction.
            self._causal_ledger.fail_turn(
                turn.id,
                acceptance=exc.prompt_accepted,
                code=exc.code,
                phase=exc.phase,
                retry_allowed=(
                    causal_retry_allowed
                    and exc.retryable
                    and exc.prompt_accepted is False
                ),
                runtime_fence=runtime_fence,
            )
            self._notify_turn_terminal(turn.id)
            if exc.prompt_accepted is not False:
                self._remember_harness_input_cursor(
                    engram_id,
                    turn.cursor_after,
                )
            raise

        try:
            settled_turn, result_event = self._causal_ledger.settle_turn(
                turn.id,
                harness_result,
                usage={
                    "input_count": harness_result.input_tokens,
                    "output_count": harness_result.output_tokens,
                    "cached_count": harness_result.cached_tokens,
                    "cache_write_count": harness_result.cache_write_tokens,
                },
                metadata={
                    "tool_calls": harness_result.tool_calls,
                    "provider_requests": harness_result.provider_requests,
                },
                runtime_fence=runtime_fence,
                engram_activity=EngramPulseActivity(
                    last_pulse_at=_now(),
                    token_count=(
                        harness_result.input_tokens
                        + harness_result.output_tokens
                    ),
                ),
            )
        except RuntimeLeaseLostError:
            # This worker no longer owns the world.  In particular it must not
            # convert the still-running turn to failed/uncertain; takeover
            # recovery is the only authority allowed to classify it.
            raise
        except Exception as exc:
            # Pi has already returned a successful result, so a settlement
            # fault must never fall through as an auto-retryable pulse.  The ledger
            # can terminalize the still-running pair as uncertain; if that
            # best-effort write also fails, startup recovery owns the row.
            try:
                self._causal_ledger.fail_turn(
                    turn.id,
                    acceptance=None,
                    code="settle_failed",
                    phase="settle",
                    retry_allowed=False,
                    runtime_fence=runtime_fence,
                )
                self._notify_turn_terminal(turn.id)
            except Exception:
                raise
            self._remember_harness_input_cursor(
                engram_id,
                turn.cursor_after,
            )
            raise HarnessError(
                "causal_settle_failed",
                "the successful Harness result could not be durably settled",
                "reconcile the uncertain causal event before creating a new child",
                phase="settle",
                retryable=False,
                prompt_accepted=None,
            ) from exc
        self._harness_bootstrapped.add(engram_id)
        self._notify_turn_terminal(settled_turn.id)
        # Keep the compatibility cache coherent for callers that inspect it;
        # the ledger remains the durable cursor owner and no second commit is
        # performed here.
        self._remember_harness_input_cursor(
            engram_id,
            settled_turn.cursor_after,
        )

        return PulseResult(
            content=harness_result.content,
            input_tokens=harness_result.input_tokens,
            output_tokens=harness_result.output_tokens,
            cached_tokens=harness_result.cached_tokens,
            cache_write_tokens=harness_result.cache_write_tokens,
            tool_calls=harness_result.tool_calls,
            provider_requests=harness_result.provider_requests,
            event_id=event.id,
            causal_id=event.causal_id,
            turn_id=settled_turn.id,
            result_event_id=result_event.id,
        )

    def _pulse_with_harness(
        self,
        engram_id: str,
        *,
        injected_context: str | None,
        source_engram_id: str | None,
        runtime_config,
    ) -> PulseResult:
        """Run exactly one settled Harness turn and project only its final text."""

        engram = self._storage.get_engram(engram_id)
        if engram is None:
            raise ValueError(f"Engram {engram_id} not found")
        if engram.status != EngramStatus.ACTIVE:
            raise ValueError(f"Engram {engram_id} is archived")

        if injected_context:
            self._storage.append_message(
                engram_id,
                Message(
                    role=MessageRole.INJECTION,
                    content=injected_context,
                    source_engram_id=source_engram_id,
                ),
            )

        session = self._storage.get_session(engram_id)
        bootstrapped = self._harness_is_bootstrapped(engram_id, session)
        submitted_count = len(session)
        prompt, bootstrap_text = self._harness_turn_inputs(
            session,
            bootstrapped=bootstrapped,
            consumed_count=self._harness_input_cursor(engram_id, submitted_count),
        )
        timeout_sec = self._harness_turn_timeout_sec
        if runtime_config is not None:
            timeout_sec = getattr(runtime_config, "deadline_sec", timeout_sec)

        assert self._harness is not None
        turn = self._run_harness_turn(
            engram_id,
            prompt,
            timeout_sec=timeout_sec,
            bootstrap_text=bootstrap_text,
            submitted_count=submitted_count,
        )
        self._harness_bootstrapped.add(engram_id)

        # SQLite remains an observation/message index. Pi trace and tool
        # details deliberately stay in the Harness result and never enter it.
        self._storage.append_message(
            engram_id,
            Message(role=MessageRole.ASSISTANT, content=turn.content),
        )
        self._commit_harness_input_cursor(engram_id, submitted_count + 1)

        now = _now()
        current = self._storage.get_engram(engram_id)
        new_pulses = (current.total_pulses if current else 0) + 1
        activity = min(
            1.0,
            (current.metadata.recent_activity if current else 0) + 0.2,
        )
        self._storage.update_engram_metadata(
            engram_id,
            last_pulse_at=now,
            total_pulses=new_pulses,
            recent_activity=activity,
            token_count=turn.input_tokens + turn.output_tokens,
        )

        return PulseResult(
            content=turn.content,
            input_tokens=turn.input_tokens,
            output_tokens=turn.output_tokens,
            cached_tokens=turn.cached_tokens,
            cache_write_tokens=turn.cache_write_tokens,
            tool_calls=turn.tool_calls,
        )

    def append_injection(
        self,
        engram_id: str,
        content: str,
        source_id: str,
    ) -> None:
        """Append an injection message from another engram."""
        self._storage.append_message(
            engram_id,
            Message(
                role=MessageRole.INJECTION,
                content=content,
                source_engram_id=source_id,
            ),
        )

    def succession(
        self,
        engram_id: str,
        *,
        parent_event_id: str | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> SuccessionResult:
        """Execute engram succession (generational turnover).

        1. Ask LLM to summarize current session
        2. Create new engram with summary as initial message
        3. Transfer connections from old to new
        4. Archive old engram
        5. Return a SuccessionResult (successor id + this call's token usage)
        """
        engram = self._storage.get_engram(engram_id)
        if engram is None:
            raise ValueError(f"Engram {engram_id} not found")
        if engram.status != EngramStatus.ACTIVE:
            raise ValueError(f"Engram {engram_id} is archived")

        if self._harness is not None:
            with self._harness_call_lock(engram_id):
                current = self._storage.get_engram(engram_id)
                if current is None:
                    raise ValueError(f"Engram {engram_id} not found")
                if current.status != EngramStatus.ACTIVE:
                    raise ValueError(f"Engram {engram_id} is archived")
                return self._succession_with_harness(
                    current,
                    parent_event_id=parent_event_id,
                    runtime_fence=runtime_fence,
                )

        if runtime_fence is not None:
            raise ValueError("runtime_fence requires a durable Harness succession")

        # Build summary request from current session
        session = self._storage.get_session(engram_id)
        messages = self._session_to_llm_messages(session)
        messages.append({"role": "user", "content": _SUCCESSION_PROMPT})

        # Summarize on the engram's own substrate (substrate registry)
        result = self._adapter_for(engram).complete(messages)
        summary = result.content

        # Create new engram with summary as seed
        new_engram = self._storage.create_engram(
            project_id=engram.project_id,
            initial_messages=[
                Message(role=MessageRole.ASSISTANT, content=summary),
            ],
            name=engram.name,
            name_origin=engram.name_origin,
            nickname=engram.nickname,
        )
        token_count = estimate_tokens(summary)
        self._storage.update_engram_metadata(
            new_engram.id, token_count=token_count
        )

        # Transfer connections
        self._connections.transfer_connections(engram_id, new_engram.id)

        # Procedural memory survives the generation change (Library):
        # episodic detail dies with the old session, the library carries over.
        if self._library is not None:
            self._library.transfer(engram_id, new_engram.id)

        # Archive old. Direct storage call (not self.archive) so the archive
        # listeners do NOT fire — the successor inherits the slot via the
        # succession listeners below, and a mask here would release it.
        self._storage.archive_engram(engram_id)

        for callback in self._succession_listeners:
            callback(engram_id, new_engram.id)

        return SuccessionResult(
            new_id=new_engram.id,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            cached_tokens=getattr(result, "cached_tokens", 0),
            cache_write_tokens=getattr(result, "cache_write_tokens", 0),
        )

    def prepare_succession(
        self,
        engram_id: str,
        *,
        parent_event_id: str | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> SuccessionPreparation:
        """Run the blocking summary/rotation stage without mutating the world."""

        if self._harness is None:
            raise ValueError("asynchronous succession preparation requires a Harness")
        if runtime_fence is not None and self._causal_ledger is None:
            raise ValueError("runtime_fence requires a durable causal ledger")
        with self._harness_call_lock(engram_id):
            current = self._storage.get_engram(engram_id)
            if current is None:
                raise ValueError(f"Engram {engram_id} not found")
            if current.status != EngramStatus.ACTIVE:
                raise ValueError(f"Engram {engram_id} is archived")
            return self._prepare_succession_with_harness(
                current,
                parent_event_id=parent_event_id,
                runtime_fence=runtime_fence,
            )

    def commit_succession(
        self,
        preparation: SuccessionPreparation,
        *,
        runtime_fence: RuntimeFence | None = None,
    ) -> SuccessionResult:
        """Apply one prepared lineage change on the caller's coordinator thread."""

        if not isinstance(preparation, SuccessionPreparation):
            raise TypeError("preparation must be a SuccessionPreparation")
        if self._harness is None:
            raise ValueError("prepared succession commit requires a Harness")
        if runtime_fence is not None and self._causal_ledger is None:
            raise ValueError("runtime_fence requires a durable causal ledger")
        with self._harness_call_lock(preparation.predecessor_id):
            return self._commit_succession_with_harness(
                preparation,
                runtime_fence=runtime_fence,
            )

    def _succession_with_harness(
        self,
        engram: Engram,
        *,
        parent_event_id: str | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> SuccessionResult:
        """Synchronous compatibility composition of prepare and commit."""

        preparation = self._prepare_succession_with_harness(
            engram,
            parent_event_id=parent_event_id,
            runtime_fence=runtime_fence,
        )
        try:
            return self._commit_succession_with_harness(
                preparation,
                runtime_fence=runtime_fence,
            )
        except (SuccessionHarnessError, SuccessionPreparationError):
            raise
        except HarnessError as exc:
            # The summary has already settled even though coordinator-side
            # publication failed. Preserve that fact for the synchronous
            # Engine path just as the asynchronous reaper preserves its
            # preparation before committing it.
            raise SuccessionHarnessError(exc, preparation.usage) from exc
        except Exception as exc:
            raise SuccessionPreparationError(exc, preparation.usage) from exc

    def _prepare_succession_with_harness(
        self,
        engram: Engram,
        *,
        parent_event_id: str | None = None,
        runtime_fence: RuntimeFence | None = None,
    ) -> SuccessionPreparation:
        """Summarize and rotate Pi without changing living world ownership.

        The legacy Harness adapter keeps its historical direct call shape.
        The production Runtime supplies a ``CausalLedger``; in that path the
        summary is itself a durable child turn and every non-repeatable
        generation stage is written before the next external action.
        """

        assert self._harness is not None
        generation = None
        summary_turn_id: str | None = None
        turn_usage: HarnessUsage | None = None

        if self._causal_ledger is not None:
            generation = self._begin_durable_generation(
                engram.id,
                parent_event_id=parent_event_id,
                runtime_fence=runtime_fence,
            )
            try:
                self._causal_ledger.transition_generation(
                    generation.id,
                    GenerationTransitionState.SUMMARIZING,
                    runtime_fence=runtime_fence,
                )
                summary_event = self._causal_ledger.enqueue(
                    world_id=self._causal_ledger.get_event(
                        generation.event_id
                    ).world_id,
                    flow=None,
                    domain=CausalEventDomain.GENERATION,
                    kind=CausalEventKind.SPONTANEOUS,
                    source=CausalEventSource.SELF,
                    content=_SUCCESSION_PROMPT,
                    parent_event_id=generation.event_id,
                    engram_id=engram.id,
                    metadata={
                        "generation_id": generation.id,
                        "generation_stage": "summary",
                    },
                    idempotency_key=f"generation-summary:{generation.id}",
                    runtime_fence=runtime_fence,
                )
                summary_result = self._pulse_with_causal_event(
                    engram.id,
                    pulse_event=summary_event,
                    injected_context=None,
                    runtime_config=None,
                    causal_retry_allowed=False,
                    runtime_fence=runtime_fence,
                )
                summary = summary_result.content
                summary_turn_id = summary_result.turn_id
                # Usage becomes a durable accounting fact as soon as the
                # summary turn settles.  Every later transition/persistence
                # failure must carry it back to the coordinator.
                turn_usage = HarnessUsage(
                    input_tokens=summary_result.input_tokens,
                    output_tokens=summary_result.output_tokens,
                    cached_tokens=summary_result.cached_tokens,
                    cache_write_tokens=summary_result.cache_write_tokens,
                )
                self._causal_ledger.transition_generation(
                    generation.id,
                    GenerationTransitionState.ROTATING,
                    summary_turn_id=summary_turn_id,
                    runtime_fence=runtime_fence,
                )
            except HarnessError as exc:
                self._mark_generation_failure(
                    generation,
                    exc,
                    runtime_fence=runtime_fence,
                )
                if turn_usage is not None:
                    raise SuccessionHarnessError(exc, turn_usage) from exc
                raise
            except Exception as exc:
                self._mark_generation_failure(
                    generation,
                    exc,
                    runtime_fence=runtime_fence,
                )
                if turn_usage is not None:
                    raise SuccessionPreparationError(exc, turn_usage) from exc
                raise
        else:
            session = self._storage.get_session(engram.id)
            bootstrapped = self._harness_is_bootstrapped(engram.id, session)
            submitted_count = len(session)
            pending, bootstrap_text = self._harness_turn_inputs(
                session,
                bootstrapped=bootstrapped,
                consumed_count=self._harness_input_cursor(
                    engram.id,
                    submitted_count,
                ),
            )
            summary_prompt = self._join_natural_text_parts(
                [pending, _SUCCESSION_PROMPT]
            )
            turn = self._run_harness_turn(
                engram.id,
                summary_prompt,
                timeout_sec=self._harness_turn_timeout_sec,
                bootstrap_text=bootstrap_text,
                submitted_count=submitted_count,
            )
            summary = turn.content
            turn_usage = HarnessUsage(
                input_tokens=turn.input_tokens,
                output_tokens=turn.output_tokens,
                cached_tokens=turn.cached_tokens,
                cache_write_tokens=turn.cache_write_tokens,
            )
            self._harness_bootstrapped.add(engram.id)
            # The summary command consumed every indexed message even though
            # its output is a lineage seed rather than another predecessor
            # projection.
            try:
                self._commit_harness_input_cursor(engram.id, submitted_count)
            except HarnessError as exc:
                raise SuccessionHarnessError(exc, turn_usage) from exc
            except Exception as exc:
                raise SuccessionPreparationError(exc, turn_usage) from exc

        assert turn_usage is not None

        # A successor identity must exist before Pi can rotate ownership, but
        # it must never be born ACTIVE.  Identity, summary seed, token metadata
        # and PROVISIONAL visibility therefore share one fenced transaction.
        try:
            new_engram = self._storage.create_provisional_engram(
                engram_id=(
                    self.generation_candidate_id(generation.id)
                    if generation is not None
                    else None
                ),
                project_id=engram.project_id,
                initial_messages=[
                    Message(role=MessageRole.ASSISTANT, content=summary),
                ],
                token_count=estimate_tokens(summary),
                name=engram.name,
                name_origin=engram.name_origin,
                nickname=engram.nickname,
                runtime_owner_id=(
                    runtime_fence.owner_id if runtime_fence is not None else None
                ),
                runtime_lease_epoch=(
                    runtime_fence.epoch if runtime_fence is not None else None
                ),
            )
        except Exception as exc:
            self._mark_generation_failure(
                generation,
                exc,
                runtime_fence=runtime_fence,
            )
            raise SuccessionPreparationError(exc, turn_usage) from exc

        if generation is not None:
            # The candidate now exists durably.  Persist its identity while the
            # transition is still ROTATING, before invoking Pi's external
            # new-session/close operation.
            try:
                generation = self._causal_ledger.transition_generation(
                    generation.id,
                    GenerationTransitionState.ROTATING,
                    successor_id=new_engram.id,
                    summary_turn_id=summary_turn_id,
                    runtime_fence=runtime_fence,
                )
            except Exception as exc:
                self._mark_generation_failure(
                    generation,
                    exc,
                    runtime_fence=runtime_fence,
                )
                raise SuccessionPreparationError(exc, turn_usage) from exc

        try:
            self._invoke_harness_succeed(engram.id, new_engram.id)
        except HarnessError as exc:
            cleanup_error: Exception | None = None
            if generation is None or exc.prompt_accepted is False:
                # Pi explicitly refused; the candidate was not accepted and
                # can be safely hidden. Accepted/unknown outcomes retain the
                # candidate for explicit reconciliation instead.
                try:
                    self._storage.archive_engram(new_engram.id)
                except Exception as archive_exc:
                    cleanup_error = archive_exc
                if generation is not None:
                    self._mark_generation_failure(
                        generation,
                        exc,
                        runtime_fence=runtime_fence,
                    )
            elif generation is not None:
                self._mark_generation_failure(
                    generation,
                    exc,
                    runtime_fence=runtime_fence,
                )
            if cleanup_error is not None:
                raise SuccessionPreparationError(
                    cleanup_error,
                    turn_usage,
                ) from exc
            raise SuccessionHarnessError(exc, turn_usage) from exc
        except Exception as exc:
            if generation is None:
                try:
                    self._storage.archive_engram(new_engram.id)
                except Exception as archive_exc:
                    exc = archive_exc
            else:
                # A non-Harness exception after the rotation boundary is an
                # unknown external outcome. Do not guess by archiving or
                # replaying the lineage.
                self._mark_generation_failure(
                    generation,
                    exc,
                    runtime_fence=runtime_fence,
                    force_uncertain=True,
                )
            raise SuccessionPreparationError(exc, turn_usage) from exc

        self._harness_bootstrapped.discard(engram.id)
        self._harness_bootstrapped.discard(new_engram.id)

        return SuccessionPreparation(
            predecessor_id=engram.id,
            successor_id=new_engram.id,
            input_tokens=turn_usage.input_tokens,
            output_tokens=turn_usage.output_tokens,
            cached_tokens=turn_usage.cached_tokens,
            cache_write_tokens=turn_usage.cache_write_tokens,
            generation_id=generation.id if generation is not None else None,
            causal_id=generation.causal_id if generation is not None else None,
            summary_turn_id=summary_turn_id,
        )

    def _commit_succession_with_harness(
        self,
        preparation: SuccessionPreparation,
        *,
        runtime_fence: RuntimeFence | None,
    ) -> SuccessionResult:
        """Commit living ownership after a worker has finished Pi rotation."""

        generation = None
        if preparation.generation_id is not None:
            if self._causal_ledger is None:
                raise CausalTransitionError(
                    "durable succession preparation requires its causal ledger"
                )
            generation = self._causal_ledger.get_generation(
                preparation.generation_id
            )
            if generation is None:
                raise CausalTransitionError(
                    f"generation {preparation.generation_id} is missing"
                )
            if (
                generation.predecessor_id != preparation.predecessor_id
                or generation.successor_id != preparation.successor_id
                or generation.summary_turn_id != preparation.summary_turn_id
            ):
                raise CausalTransitionError(
                    "succession preparation no longer matches its generation"
                )
            if generation.state is GenerationTransitionState.COMMITTED:
                return self._succession_result(preparation)
            if generation.state is not GenerationTransitionState.ROTATING:
                raise CausalTransitionError(
                    f"generation {generation.id} is {generation.state.value}, "
                    "expected rotating"
                )
            generation = self._causal_ledger.assert_generation_runtime_fence(
                generation.id,
                runtime_fence,
            )
            if generation.state is not GenerationTransitionState.ROTATING:
                raise CausalTransitionError(
                    f"generation {generation.id} is {generation.state.value}, "
                    "expected rotating"
                )

        predecessor = self._storage.get_engram(preparation.predecessor_id)
        successor = self._storage.get_engram(preparation.successor_id)
        if predecessor is None or predecessor.status is not EngramStatus.ACTIVE:
            raise CausalTransitionError(
                "succession predecessor is missing or no longer active"
            )
        if successor is None or successor.status is not EngramStatus.PROVISIONAL:
            raise CausalTransitionError(
                "succession candidate is missing or no longer provisional"
            )

        handed_off_event_ids: tuple[str, ...] = ()
        publication_started = False
        try:
            if generation is not None:
                observed_generation = (
                    self._causal_ledger.assert_generation_runtime_fence(
                        generation.id,
                        runtime_fence,
                    )
                )
                if (
                    observed_generation.state
                    is not GenerationTransitionState.ROTATING
                ):
                    raise CausalTransitionError(
                        f"generation {generation.id} is "
                        f"{observed_generation.state.value}, expected rotating"
                    )

            # These adapters span SQLite tables, files and in-memory state.
            # They cannot share the core publication transaction, so crossing
            # the first boundary makes any later failure UNCERTAIN.  Do not
            # hold Storage's global lock while an adapter or listener runs;
            # that would stall lease heartbeat and unrelated subjects.
            publication_started = True
            self._connections.transfer_connections(
                preparation.predecessor_id,
                preparation.successor_id,
            )
            if self._library is not None:
                self._library.transfer(
                    preparation.predecessor_id,
                    preparation.successor_id,
                )
            if generation is not None:
                observed_generation = (
                    self._causal_ledger.assert_generation_runtime_fence(
                        generation.id,
                        runtime_fence,
                    )
                )
                if (
                    observed_generation.state
                    is not GenerationTransitionState.ROTATING
                ):
                    raise CausalTransitionError(
                        f"generation {generation.id} is "
                        f"{observed_generation.state.value}, expected rotating"
                    )
                for callback in self._succession_listeners:
                    callback(
                        preparation.predecessor_id,
                        preparation.successor_id,
                    )
                generation, handed_off_event_ids = (
                    self._causal_ledger.commit_succession_publication(
                        generation.id,
                        preparation.predecessor_id,
                        preparation.successor_id,
                        summary_turn_id=preparation.summary_turn_id,
                        runtime_fence=runtime_fence,
                    )
                )
            else:
                # Compatibility Harnesses without the durable causal ledger
                # retain their historical single-Storage status transition.
                self._storage.commit_engram_succession_status(
                    preparation.predecessor_id,
                    preparation.successor_id,
                )
                for callback in self._succession_listeners:
                    callback(
                        preparation.predecessor_id,
                        preparation.successor_id,
                    )
        except RuntimeLeaseLostError:
            # Only takeover recovery may classify work after this epoch loses
            # authority.  In particular, do not let the old owner write an
            # ``uncertain`` winner through a second unfenced path.
            raise
        except Exception as exc:
            if generation is not None:
                # Cross-substrate publication may already be visible even if
                # the atomic core commit rolled back.  Never downgrade that
                # outcome to FAILED merely because a callback reports an
                # explicit refusal-shaped exception.
                self._mark_generation_failure(
                    generation,
                    exc,
                    runtime_fence=runtime_fence,
                    force_uncertain=publication_started,
                )
            raise

        return self._succession_result(
            preparation,
            handed_off_event_ids=handed_off_event_ids,
        )

    @staticmethod
    def _succession_result(
        preparation: SuccessionPreparation,
        *,
        handed_off_event_ids: tuple[str, ...] = (),
    ) -> SuccessionResult:
        return SuccessionResult(
            new_id=preparation.successor_id,
            input_tokens=preparation.input_tokens,
            output_tokens=preparation.output_tokens,
            cached_tokens=preparation.cached_tokens,
            cache_write_tokens=preparation.cache_write_tokens,
            generation_id=preparation.generation_id,
            causal_id=preparation.causal_id,
            summary_turn_id=preparation.summary_turn_id,
            handed_off_event_ids=handed_off_event_ids,
        )

    def _begin_durable_generation(
        self,
        predecessor_id: str,
        *,
        parent_event_id: str | None,
        runtime_fence: RuntimeFence | None = None,
    ):
        """Create the durable transition before requesting a summary."""

        assert self._causal_ledger is not None
        parent = None
        if parent_event_id is not None:
            parent = self._causal_ledger.get_event(parent_event_id)
            if parent is None:
                raise HarnessError(
                    "generation_parent_missing",
                    "the succession parent causal event is missing",
                    "reconcile the causal chain before retrying succession",
                    phase="generation",
                    retryable=False,
                    prompt_accepted=None,
                )
            if parent.engram_id not in {None, predecessor_id}:
                raise HarnessError(
                    "generation_parent_mismatch",
                    "the succession parent belongs to another Engram",
                    "use the settled result event of the predecessor",
                    phase="generation",
                    retryable=False,
                    prompt_accepted=None,
                )
        else:
            # Direct API callers do not have the Engine's current result event.
            # Reuse the latest terminal event for the predecessor when one is
            # available; this keeps the world/causal chain stable without
            # inventing a second PulseWorld.
            events = self._causal_ledger.list_events(
                engram_id=predecessor_id,
                limit=500,
            )
            parent = next(
                (
                    event
                    for event in reversed(events)
                    if event.kind is not CausalEventKind.GENERATION_TRANSITION
                    and event.status
                    in {
                        CausalEventStatus.SETTLED,
                        CausalEventStatus.FAILED,
                        CausalEventStatus.UNCERTAIN,
                        CausalEventStatus.RECONCILED,
                    }
                ),
                None,
            )

        try:
            if (
                self._causal_world_id is not None
                and parent is not None
                and parent.world_id != self._causal_world_id
            ):
                raise HarnessError(
                    "generation_world_mismatch",
                    "the succession parent belongs to another PulseWorld",
                    "use the current Runtime causal event as the parent",
                    phase="generation",
                    retryable=False,
                    prompt_accepted=None,
                )
            return self._causal_ledger.begin_generation(
                predecessor_id,
                parent_event_id=parent.id if parent is not None else None,
                world_id=(
                    parent.world_id
                    if parent is not None
                    else self._causal_world_id or "default"
                ),
                runtime_fence=runtime_fence,
            )
        except CausalTransitionError as exc:
            raise HarnessError(
                "generation_blocked",
                "the predecessor already has an unfinished or uncertain generation",
                "reconcile the existing GenerationTransition before retrying",
                phase="generation",
                retryable=False,
                prompt_accepted=None,
            ) from exc

    def _mark_generation_failure(
        self,
        generation,
        error: BaseException,
        *,
        runtime_fence: RuntimeFence | None = None,
        force_uncertain: bool = False,
    ) -> None:
        """Best-effort terminalize a failed stage without inventing success."""

        if self._causal_ledger is None or generation is None:
            return
        if isinstance(error, RuntimeLeaseLostError):
            return
        acceptance = getattr(error, "prompt_accepted", None)
        state = (
            GenerationTransitionState.UNCERTAIN
            if force_uncertain or acceptance is not False
            else GenerationTransitionState.FAILED
        )
        try:
            self._causal_ledger.transition_generation(
                generation.id,
                state,
                successor_id=generation.successor_id,
                summary_turn_id=generation.summary_turn_id,
                error_code=getattr(error, "code", None)
                or type(error).__name__.casefold(),
                runtime_fence=runtime_fence,
            )
        except Exception:
            # The original failure is more useful to the caller.  Startup
            # recovery will still classify a non-terminal transition as
            # uncertain when the durable write can be made later.
            return

    def import_conversation(
        self,
        messages: list[Message],
        project_id: str | None = None,
    ) -> Engram:
        """Import an external conversation as a new engram."""
        engram = self._storage.create_engram(
            project_id=project_id,
            initial_messages=messages,
        )
        token_count = sum(estimate_tokens(m.content) for m in messages)
        self._storage.update_engram_metadata(
            engram.id, token_count=token_count
        )
        return engram

    def get(self, engram_id: str) -> Engram | None:
        return self._storage.get_engram(engram_id)

    def get_session(self, engram_id: str) -> list[Message]:
        return self._storage.get_session(engram_id)

    # ── Internal ─────────────────────────────────────────────────

    def _harness_call_lock(self, engram_id: str) -> threading.RLock:
        """Serialize prompt selection through projection for one Engram."""

        with self._harness_calls_guard:
            return self._harness_calls.setdefault(engram_id, threading.RLock())

    def _assert_generation_pulse_admissible(self, engram_id: str) -> None:
        if (
            self._causal_ledger is None
            or engram_id
            not in self._causal_ledger.generation_blocked_predecessors()
        ):
            return
        raise HarnessError(
            "generation_in_progress",
            "the Engram lineage has an unfinished or uncertain succession",
            "finish or reconcile the existing GenerationTransition before pulsing",
            phase="generation",
            retryable=False,
            prompt_accepted=None,
        )

    def _load_harness_input_cursors(self) -> dict[str, int]:
        raw = self._storage.load_component_state(HARNESS_INPUT_COMPONENT)
        if raw is None:
            return {}
        if not isinstance(raw, dict) or raw.get("version") != _HARNESS_INPUT_VERSION:
            raise HarnessError(
                "harness_input_cursor_invalid",
                "the persisted Harness input cursor has an unsupported shape or version",
                f"repair or explicitly migrate {HARNESS_INPUT_COMPONENT}",
                phase="input_cursor",
            )
        values = raw.get("cursors")
        if not isinstance(values, dict):
            raise HarnessError(
                "harness_input_cursor_invalid",
                "the persisted Harness input cursor has no cursors object",
                f"repair {HARNESS_INPUT_COMPONENT}",
                phase="input_cursor",
            )
        cursors: dict[str, int] = {}
        for engram_id, count in values.items():
            if (
                not isinstance(engram_id, str)
                or not engram_id
                or type(count) is not int
                or count < 0
            ):
                raise HarnessError(
                    "harness_input_cursor_invalid",
                    "the persisted Harness input cursor contains an invalid Engram/count entry",
                    f"repair {HARNESS_INPUT_COMPONENT}",
                    phase="input_cursor",
                )
            cursors[engram_id] = count
        return cursors

    def _harness_input_cursor(
        self,
        engram_id: str,
        message_count: int,
    ) -> int | None:
        with self._harness_cursor_lock:
            cursor = self._harness_input_cursors.get(engram_id)
        if cursor is not None and cursor > message_count:
            raise HarnessError(
                "harness_input_cursor_invalid",
                f"Harness input cursor {cursor} exceeds the {message_count} indexed messages for Engram {engram_id!r}",
                f"repair {HARNESS_INPUT_COMPONENT} before another pulse",
                phase="input_cursor",
            )
        return cursor

    def _commit_harness_input_cursor(self, engram_id: str, count: int) -> None:
        with self._harness_cursor_lock:
            candidate = dict(self._harness_input_cursors)
            candidate[engram_id] = count
            self._storage.save_component_state(
                HARNESS_INPUT_COMPONENT,
                {
                    "version": _HARNESS_INPUT_VERSION,
                    "cursors": {
                        key: candidate[key]
                        for key in sorted(candidate)
                    },
                },
            )
            self._harness_input_cursors = candidate

    def _remember_harness_input_cursor(self, engram_id: str, count: int) -> None:
        """Update the in-memory compatibility view without a durable write."""

        with self._harness_cursor_lock:
            self._harness_input_cursors[engram_id] = count

    def _run_harness_turn(
        self,
        engram_id: str,
        prompt: str,
        *,
        timeout_sec: float | None,
        bootstrap_text: str | None,
        submitted_count: int,
        turn_id: str | None = None,
    ):
        """Send once; persist an anti-replay cursor after observed sent/unknown input."""

        assert self._harness is not None
        try:
            return self._invoke_harness_turn(
                engram_id,
                prompt,
                timeout_sec=timeout_sec,
                bootstrap_text=bootstrap_text,
                turn_id=turn_id,
            )
        except HarnessError as exc:
            if exc.prompt_accepted is not False:
                try:
                    self._commit_harness_input_cursor(engram_id, submitted_count)
                except Exception as persist_exc:
                    raise HarnessError(
                        "harness_input_cursor_persist_failed",
                        "a sent or ambiguously accepted Harness input could not be marked as submitted",
                        "stop automatic pulses and reconcile the Pi session plus SQLite input cursor before continuing",
                        phase="input_cursor",
                        retryable=False,
                        prompt_accepted=exc.prompt_accepted,
                        partial_output=exc.partial_output,
                        trace=exc.trace,
                    ) from persist_exc
            raise

    @staticmethod
    def _supports_turn_id(run_turn: Any) -> bool:
        """Keep old test/embedding Harnesses source-compatible.

        The production Pi runtime accepts ``turn_id``.  A number of existing
        narrow fakes intentionally model the pre-v1 protocol, so the manager
        checks the callable signature before passing the new optional keyword
        rather than catching a TypeError after a possibly side-effecting call.
        """

        try:
            parameters = inspect.signature(run_turn).parameters.values()
        except (TypeError, ValueError):
            return True
        return any(
            parameter.name == "turn_id"
            or parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters
        )

    def _invoke_harness_turn(
        self,
        engram_id: str,
        prompt: str,
        *,
        timeout_sec: float | None,
        bootstrap_text: str | None,
        turn_id: str | None = None,
    ):
        assert self._harness is not None
        kwargs: dict[str, Any] = {
            "timeout_sec": timeout_sec,
            "bootstrap_text": bootstrap_text,
        }
        if turn_id is not None and self._supports_turn_id(self._harness.run_turn):
            kwargs["turn_id"] = turn_id
        return self._harness.run_turn(engram_id, prompt, **kwargs)

    @staticmethod
    def _supports_succession_capacity_timeout(succeed: Any) -> bool:
        try:
            parameters = inspect.signature(succeed).parameters.values()
        except (TypeError, ValueError):
            return True
        return any(
            parameter.name == "capacity_timeout_sec"
            or parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters
        )

    def _invoke_harness_succeed(
        self,
        old_engram_id: str,
        new_engram_id: str,
    ) -> None:
        """Bound Pi resident-capacity admission without overclaiming rotation."""

        assert self._harness is not None
        succeed = self._harness.succeed
        if self._supports_succession_capacity_timeout(succeed):
            succeed(
                old_engram_id,
                new_engram_id,
                capacity_timeout_sec=self._harness_turn_timeout_sec,
            )
            return
        succeed(old_engram_id, new_engram_id)

    def _harness_is_bootstrapped(
        self,
        engram_id: str,
        session: list[Message],
    ) -> bool:
        """Resolve persisted Harness bootstrap state without starting Pi."""

        if engram_id in self._harness_bootstrapped:
            return True
        if not session:
            return False

        assert self._harness is not None
        try:
            snapshot = self._harness.snapshot(engram_id)
        except HarnessError as exc:
            if exc.code == "pi_session_unknown":
                return False
            raise
        if snapshot.get("bootstrapped") is True:
            self._harness_bootstrapped.add(engram_id)
            return True
        return False

    @classmethod
    def _harness_turn_inputs(
        cls,
        session: list[Message],
        *,
        bootstrapped: bool,
        consumed_count: int | None = None,
    ) -> tuple[str, str | None]:
        """Split the message index into one-time seed and incremental text."""

        if consumed_count is not None:
            # A submission cursor is stronger than an assistant boundary: it
            # also covers an accepted/unknown turn that produced no projection.
            return cls._join_natural_messages(session[consumed_count:]), None

        last_assistant = next(
            (
                index
                for index in range(len(session) - 1, -1, -1)
                if session[index].role == MessageRole.ASSISTANT
            ),
            -1,
        )
        first_injection = next(
            (
                index
                for index, message in enumerate(session)
                if message.role == MessageRole.INJECTION
            ),
            len(session),
        )
        if bootstrapped:
            pending = (
                session[last_assistant + 1:]
                if last_assistant >= 0
                else session[first_injection:]
            )
            return cls._join_natural_messages(pending), None

        if last_assistant >= 0:
            seed = session[:last_assistant + 1]
            pending = session[last_assistant + 1:]
        else:
            seed = session[:first_injection]
            pending = session[first_injection:]

        bootstrap_text = cls._join_natural_messages(seed)
        return (
            cls._join_natural_messages(pending),
            bootstrap_text if bootstrap_text != "" else None,
        )

    @staticmethod
    def _join_natural_messages(messages: list[Message]) -> str:
        return EngramManager._join_natural_text_parts(
            [message.content for message in messages]
        )

    @staticmethod
    def _join_natural_text_parts(parts: list[str]) -> str:
        return "\n\n".join(part for part in parts if part != "")

    @staticmethod
    def _session_to_llm_messages(
        session: list[Message],
    ) -> list[dict[str, str]]:
        """Convert session Messages to LLM-compatible dicts.

        Injection messages are passed as 'user' role to the LLM since most
        APIs don't have a native 'injection' role. The content is the raw
        natural text — no structural framing added (the free-context rule).
        """
        msgs: list[dict[str, str]] = []
        for m in session:
            role = m.role.value
            if role == "injection":
                role = "user"
            msgs.append({"role": role, "content": m.content})
        return msgs

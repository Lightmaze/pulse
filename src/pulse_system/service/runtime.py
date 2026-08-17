"""The long-running service: one object that holds the whole organism.

This module holds the system together past the end of a command and provides
the lifecycle shared by the CLI, API and tests.

Three properties make it a runtime rather than a wiring diagram:

1. **It keeps running with nobody watching.** A background tick loop drives
   the engine on its own schedule. The loop runs the blocking tick in a worker
   thread (`asyncio.to_thread`), so an API event loop sharing the process stays
   responsive; a tick that raises is logged and the next one still happens — a
   runtime that dies of one bad tick is not a runtime.

2. **The world survives the process.** One durable ``world_id`` and a
   compatibility continuity Engram are written to ``component_state`` and
   resumed on the next start. TaskFronts and life ActivityCenters live inside
   that one world; changing the selected Front never creates another runtime.
   Succession re-points every durable world reference, so a generational
   turnover is continuation rather than a new identity.

3. **Commanded and observed are kept apart.** A tuning knob does not take
   effect when it is turned; it takes effect at the next tick boundary. Holding
   only one number would make the panel assert an effect during the interval
   before it lands. `commanded`, `observed` and `applied_at_tick` are three
   separate facts and are reported as three.

**the free-context rule, the line this module may not cross.** Sideband data never enters an
engram's LLM context. Tuning writes *rhythm* parameters — spontaneous rate,
dendrite wait, propagation threshold, inhibition gate — into config objects the
engine reads. It never composes text. The only content path is `inject()`, and
it durably enqueues a causal event that the Engine reconstructs into its
dendrite scheduling cache: no system prompt, no framing, no "the user wants…"
preamble. The `source` label on an injection is recorded in the metrics
sideband and is never shown to the model.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections.abc import Coroutine, Mapping
from typing import Any, Callable

from pulse_system.agent.harness import (
    BINDING_COMPONENT,
    HarnessError,
    HarnessRuntime,
    HarnessTurnResult,
    PiHarnessRuntime,
)
from pulse_system.agent.harness.actions import (
    HarnessActionBroker,
    HarnessActionError,
    RoutedActionBackend,
    TerminalSessionActionBackend,
)
from pulse_system.agent.harness.sandbox import (
    CodexCliPipeProcessBackend,
    CodexCliSandboxBackend,
    PipeLifecycleGate,
    SandboxLiveGate,
    SandboxPreflight,
)
from pulse_system.agent.harness.terminal import TerminalManager
from pulse_system.agent.harness.terminal_sessions import (
    TerminalSessionConflictError,
    TerminalSessionError,
    TerminalSessionLeaseError,
    TerminalSessionNotFoundError,
    TerminalSessionService,
    TerminalSessionStore,
)
from pulse_system.agent.harness.security import (
    ApprovalMode,
    CommandScope,
    ExecutionPolicy,
    FilesystemAccess,
    NetworkAccess,
)
from pulse_system.agent.harness.events import (
    HarnessEvent,
    HarnessEventDraft,
    HarnessEventKind,
    HarnessEventPhase,
    HarnessEventProjector,
    HarnessEventSource,
    HarnessEventStatus,
    HarnessEventStore,
)
from pulse_system.agent.harness.operations import (
    HarnessOperationError,
    HarnessOperationLedger,
    OperationPhase,
    OperationRecoveryState,
    OperationTerminalState,
    deterministic_terminal_event_id,
)
from pulse_system.agent.harness.mcp_runtime import (
    MCPActionBackend,
    MCPRegistryGate,
    MCPRuntimeService,
    MCPServerDescriptor,
)
from pulse_system.agent.harness.pi_task_worker import PiTaskWorkerBackend
from pulse_system.agent.harness.purpose_governance import (
    PurposeGovernance,
    PurposeGovernanceError,
    PurposeLineageConflictError,
)
from pulse_system.agent.harness.role_leases import (
    HolderKind,
    RoleClass,
    RoleLeaseStatus,
    RoleLeaseStore,
    RuntimeLeaseProof as RoleRuntimeLeaseProof,
)
from pulse_system.agent.harness.stimulus_firewall import (
    ControlLedger,
    DecisionRoute,
    EvidenceClass as StimulusEvidenceClass,
    ProvenanceMode,
    StimulusClass,
    StimulusEnvelope,
    StimulusFirewall,
    digest_payload,
)
from pulse_system.agent.harness.task_subagents import (
    TaskSubagentConfig,
    TaskSubagentService,
)
from pulse_system.agent.harness.task_worker_runtime import TaskWorkerToolBridge
from pulse_system.agent.delegate import Delegator, DelegatorConfig
from pulse_system.agent.tools import ToolRegistry
from pulse_system.agent.tools.gateway import PulseToolGateway, ToolInvocationContext
from pulse_system.core.claustrum import ClaustrumModulator
from pulse_system.core.causality import (
    CausalAdmissionConflictError,
    CausalLedger,
    CausalTransitionError,
    RuntimeFence,
)
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.learning_policy import (
    OnlineLearningAudit,
    OnlineLearningPolicy,
)
from pulse_system.core.delegation import (
    DURABLE_DELEGATION_MODE,
    DelegationRouter,
    DelegationTunnelError,
    DurableDelegationTunnel,
)
from pulse_system.core.dendrite import DendriteConfig, DendriteProcessor
from pulse_system.core.engram import EngramManager
from pulse_system.core.habitat import ManagedHabitat
from pulse_system.core.pulse import PulseEngine, PulseEngineConfig
from pulse_system.core.pulse.engine import SpontaneousDispatch
from pulse_system.core.runtime import (
    CenterSchedulingConfig,
    DurableCenterScheduler,
    RuntimeConfig,
    RuntimeBootstrapPermit,
    RuntimeLeaseKeeper,
    RuntimeManager,
    RuntimePublicationError,
    RuntimePublicationGate,
    RuntimePublicationPermit,
    RuntimeRetainedOwnerProbe,
    RuntimeRetainedOwnerProbeRegistry,
    RuntimeRecoveryPermit,
    RuntimeShutdownClaim,
    RuntimeShutdownController,
    RuntimeShutdownObserver,
    RuntimeShutdownReport,
    RuntimeShutdownTrigger,
    ShutdownCancelState,
    ShutdownComponentReport,
    ShutdownDeadline,
    ShutdownDurableRecoveryState,
    ShutdownEffectState,
    ShutdownOwnerLeaseState,
    ShutdownOwnerState,
    ShutdownPhase,
    ShutdownProcessTreeState,
    ShutdownPublicationFenceState,
    ShutdownReportBuilder,
    ShutdownStorageState,
)
from pulse_system.core.runtime.shutdown import component_report
from pulse_system.core.sensory import SensoryCortex
from pulse_system.core.types import (
    ActivityCenter,
    ActivityCenterStatus,
    ActivityKind,
    ActivityOrigin,
    CenterMembership,
    CausalEvent,
    CausalEventDomain,
    CausalEventFlow,
    CausalEventKind,
    CausalEventSource,
    CausalEventStatus,
    Engram,
    EngramStatus,
    LivingConcern,
    LivingOrientation,
    MembershipRelation,
    Message,
    MessageRole,
    RuntimeLeaseConflictError,
    RuntimeLeaseError,
    TaskFront,
    TaskFrontStatus,
    TaskOffer,
    TaskOfferRevision,
    TaskOfferSnapshot,
    TaskRelationship,
    TaskRelationshipEvent,
    TaskRelationshipSnapshot,
    TaskRelationshipStatus,
    session_name,
)
from pulse_system.core.world import WorldRegistry
from pulse_system.education.library import Library
from pulse_system.interaction.metrics import MetricsRecorder
from pulse_system.substrate.llm import LLMAdapter, LLMCallError, SubstrateRegistry
from pulse_system.substrate.storage import Storage

from .life_tools import LifeToolService
from .living_portfolio import (
    LivingPortfolioProjector,
    LivingPortfolioRecoveryError,
    LivingPortfolioValidationError,
)
from .role_accountability import (
    DEFAULT_ROLE_LIMIT,
    RoleAccountabilityProjector,
    RoleAccountabilityRecoveryError,
    RoleAccountabilityValidationError,
)
from .task_offers import (
    TaskOfferError,
    TaskOfferOperation,
    TaskOfferService,
)
from .task_relationships import (
    TaskRelationshipError,
    TaskRelationshipOperation,
    TaskRelationshipService,
)

_logger = logging.getLogger("pulse_system.service")

#: `component_state` key under which the runtime's identity is persisted.
IDENTITY_COMPONENT = "runtime_service"

#: One durable world identity per SQLite substrate.
WORLD_COMPONENT = "pulse.world.v1"

#: The four rhythm knobs of contract §2.2. Content is deliberately absent.
TUNING_KNOBS = ("activity", "wait", "propagation_threshold", "gate")

#: Accepted ranges, reported verbatim in the remedy of a rejected command.
_KNOB_RANGE: dict[str, tuple[float, float]] = {
    "activity": (0.0, 1.0),
    "wait": (0.0, 3600.0),
    "propagation_threshold": (0.0, 1.0),
    "gate": (0.0, 100.0),
}

_KNOB_UNIT = {
    "activity": "base spontaneous-activation rate per check",
    "wait": "dendrite max-wait seconds",
    "propagation_threshold": "minimum edge weight that propagates",
    "gate": "inhibition→propagation gate strength (0 = off)",
}

#: Deprecated compatibility export.  New worlds deliberately inject no role
#: text; user-visible work begins by creating a TaskFront with natural input.
FRONT_SEED = ""

_LOCAL_BACKEND = "local"
_HARNESS_KIND_PI = "pi"
_HARNESS_KIND_MOCK = "mock"
_SHUTDOWN_RECOVERY_DOMAINS = (
    "causal",
    "center_reservations",
    "external_effects",
    "harness_operations",
    "pi_continuity",
    "role_leases",
    "task_workers",
    "terminal_sessions",
)
_CENTER_CONCERN_LIMIT = 200

_SAFE_HARNESS_METRIC_FIELDS = frozenset({
    "engram",
    "resumed",
    "session_id",
    "correlation_id",
    "bootstrap",
    "input_tokens",
    "output_tokens",
    "cached_tokens",
    "cache_write_tokens",
    "tool_calls",
    "code",
    "phase",
    "retryable",
    "prompt_accepted",
    "reason",
    "error_type",
})

HarnessFactory = Callable[..., HarnessRuntime]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso() -> str:
    return _now().isoformat()


def _stable_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            default=lambda item: {"type": type(item).__name__},
        ).encode("utf-8")
    ).hexdigest()


class ServiceError(Exception):
    """A refusal that carries its own remedy (contract §6).

    A refusal without a way forward is half a refusal. Every failure this
    service raises names what could not be done *and* what would make it work,
    and the write routes serialize it to exactly the documented shape.
    """

    def __init__(self, error: str, detail: str, remedy: str, *, status: int = 400):
        super().__init__(f"{error}: {detail}")
        self.error = error
        self.detail = detail
        self.remedy = remedy
        self.status = status

    def payload(self) -> dict[str, str]:
        return {"error": self.error, "detail": self.detail, "remedy": self.remedy}


@dataclass(frozen=True, slots=True)
class RuntimeAssemblyOutcome:
    """Construction result that preserves both the error and shutdown observer."""

    runtime: RuntimeService | None
    error: BaseException | None
    shutdown: RuntimeShutdownObserver

    def __post_init__(self) -> None:
        if (self.runtime is None) == (self.error is None):
            raise ValueError("exactly one of runtime or error must be present")
        if not isinstance(self.shutdown, RuntimeShutdownObserver):
            raise ValueError("shutdown must be a RuntimeShutdownObserver")

    def raise_for_error(self) -> RuntimeService:
        """Re-raise the original object without wrapping or changing its type."""

        if self.error is not None:
            raise self.error
        runtime = self.runtime
        if runtime is None:  # pragma: no cover - dataclass invariant
            raise RuntimeError("Runtime assembly has neither runtime nor error")
        return runtime


@dataclass
class RuntimeServiceConfig:
    """Everything one PulseWorld needs, defaulting to the real Pi Harness."""

    # Persistence. ":memory:" is legal but forfeits property 2 — identity
    # cannot outlive a process that never wrote it anywhere.
    db_path: str | Path = ":memory:"
    workspace: str | Path | None = None
    metrics_path: str | Path | None = None

    # Harness and retained embedding/delegation substrate. ``mock=True`` is an
    # explicit legacy/test Harness; production never falls back to it.
    mock: bool = False
    provider: str = "deepseek"
    model: str | None = None
    max_tokens: int = 2048
    pi_executable: str = "pi"
    pi_provider: str | None = None
    pi_model: str | None = None
    # The command adapter never auto-enables from PATH or from a callable ``--help``.  An
    # operator must name the executable, opt in, and provide the matching
    # adversarial live-gate artifact and effective config before a backend
    # enters the broker.
    codex_sandbox_enabled: bool = False
    codex_sandbox_executable: str | Path | None = None
    # Workspace-write remains disabled until an OS/profile gate proves that
    # `.pulse` is protected (or a verified adapter applies disposable staging).
    codex_sandbox_permission_profile: str = ":read-only"
    codex_sandbox_live_gate: str | Path | None = None
    codex_sandbox_config: str | Path | None = None
    harness_command_allowlist: tuple[str, ...] = ()
    # Durable background commands are a separate capability from foreground
    # sandbox execution.  They require an independently bound owner-death
    # lifecycle artifact and never auto-enable from the foreground gate.
    harness_pipe_sessions_enabled: bool = False
    harness_pipe_lifecycle_gate: str | Path | None = None
    harness_pipe_session_capacity: int = 8
    # File mutation is independent of the command sandbox.  It is
    # disabled unless the operator explicitly supplies an external,
    # protected checkpoint root; every edit/write must checkpoint before the
    # first workspace byte changes.
    harness_file_mutation_enabled: bool = False
    harness_checkpoint_root: str | Path | None = None
    # MCP is explicit-only: descriptors may contain credential values, so
    # Runtime never discovers ambient config or enables a server by presence.
    harness_mcp_enabled: bool = False
    harness_mcp_descriptors: tuple[Any, ...] = ()
    harness_mcp_allowlisted_server_ids: tuple[str, ...] = ()
    # Temporary workers are a separate, explicitly enabled compute fleet.
    # Their scratch root must be absolute and external to the organism's
    # workspace so a child cannot become an Engram or inherit `.pulse` state.
    harness_task_worker_enabled: bool = False
    harness_task_worker_root: str | Path | None = None
    harness_task_worker_capacity: int = 2
    harness_task_worker_max_per_turn: int = 4
    harness_task_worker_default_timeout_sec: float = 300.0
    harness_task_worker_max_timeout_sec: float = 900.0
    harness_turn_timeout_sec: float | None = 600.0
    harness_handshake_timeout_sec: float = 30.0
    harness_sideband_timeout_sec: float = 30.0
    harness_abort_timeout_sec: float = 5.0
    # Durable Engram identities may greatly outnumber live subprocesses. The
    # coordinator bounds simultaneous turns separately from Pi's idle/LRU
    # resident process cache.
    pulse_worker_capacity: int = 4
    pi_resident_session_limit: int = 8

    # Rhythm.
    tick_interval: float = 0.1
    budget_per_tick: int = 5
    hourly_token_budget: int = 100_000
    silence_threshold: float = 5.0
    default_max_wait: float = 30.0
    propagation_threshold: float = 0.3
    base_spontaneous_rate: float = 0.02
    inhibition_propagation_gate: float = 0.0
    topology_interval_ticks: int | None = None
    # Compact connection viability is production observability: every 100
    # default 100 ms ticks is a ~10 s snapshot. It is read-only, content-free,
    # and may be disabled by setting None.
    connectivity_interval_ticks: int | None = 100
    living_concern_reentry_budget_per_tick: int = 2
    living_concern_reentry_priority: float = 0.85
    living_orientation_refractory_sec: float = 300.0
    living_orientation_priority: float = 0.3
    living_orientation_history_limit: int = 20
    runtime_lease_ttl_sec: float = 30.0
    runtime_lease_renew_interval_sec: float = 5.0
    # One absolute budget covers freeze, settlement, publication revocation,
    # owner-lease cleanup and every local execution domain.
    runtime_shutdown_timeout_sec: float = 10.0
    center_lane_reservation_per_tick: int = 1
    center_starvation_boost: float = 0.05
    center_starvation_debt_cap: int = 20
    center_reservation_history_limit: int = 20

    # Sideband streams, both opt-in: attached, they modulate rhythm and
    # routing; detached, the engine preserves its unmodulated behavior.
    with_claustrum: bool = False
    with_router: bool = False
    # Immutable mutation policy. All inference components remain assembled;
    # production defaults preserve every existing online update.
    online_learning_policy: OnlineLearningPolicy = field(
        default_factory=OnlineLearningPolicy
    )

    # Deprecated compatibility option. New continuity anchors are deliberately
    # silent (0.0); TaskFront focal Engrams use ordinary Engram defaults.
    front_self_excitability: float = 0.5
    metrics_flush_every: int = 1
    metrics_max_bytes: int = 128 * 1024 * 1024
    metrics_archive_count: int = 1
    metrics_replay_bytes: int = 4 * 1024 * 1024

    def __post_init__(self) -> None:
        if not isinstance(self.online_learning_policy, OnlineLearningPolicy):
            raise ValueError(
                "online_learning_policy must be an OnlineLearningPolicy"
            )
        if type(self.codex_sandbox_enabled) is not bool:
            raise ValueError("codex_sandbox_enabled must be a bool")
        if type(self.harness_pipe_sessions_enabled) is not bool:
            raise ValueError("harness_pipe_sessions_enabled must be a bool")
        if type(self.harness_file_mutation_enabled) is not bool:
            raise ValueError("harness_file_mutation_enabled must be a bool")
        if type(self.harness_mcp_enabled) is not bool:
            raise ValueError("harness_mcp_enabled must be a bool")
        if type(self.harness_task_worker_enabled) is not bool:
            raise ValueError("harness_task_worker_enabled must be a bool")
        if not isinstance(self.harness_mcp_descriptors, (tuple, list)):
            raise ValueError("harness_mcp_descriptors must be a sequence")
        if any(
            not isinstance(item, MCPServerDescriptor)
            for item in self.harness_mcp_descriptors
        ):
            raise ValueError(
                "harness_mcp_descriptors must contain MCPServerDescriptor values"
            )
        self.harness_mcp_descriptors = tuple(self.harness_mcp_descriptors)
        if not isinstance(self.harness_mcp_allowlisted_server_ids, (tuple, list)):
            raise ValueError(
                "harness_mcp_allowlisted_server_ids must be a sequence"
            )
        if any(
            not isinstance(item, str) or not item.strip() or len(item) > 128
            for item in self.harness_mcp_allowlisted_server_ids
        ):
            raise ValueError("MCP allowlisted server ids must be bounded strings")
        self.harness_mcp_allowlisted_server_ids = tuple(
            item.strip() for item in self.harness_mcp_allowlisted_server_ids
        )
        if self.harness_mcp_enabled:
            if not self.harness_mcp_descriptors:
                raise ValueError("harness_mcp_enabled requires explicit descriptors")
            if not self.harness_mcp_allowlisted_server_ids:
                raise ValueError("harness_mcp_enabled requires an explicit allowlist")
        elif self.harness_mcp_descriptors or self.harness_mcp_allowlisted_server_ids:
            raise ValueError(
                "MCP descriptors and allowlist require harness_mcp_enabled"
            )
        if self.harness_task_worker_root is not None:
            if not isinstance(self.harness_task_worker_root, (str, Path)) or not str(
                self.harness_task_worker_root
            ).strip():
                raise ValueError("harness_task_worker_root must be a non-empty path")
            worker_root = Path(self.harness_task_worker_root).expanduser()
            if not worker_root.is_absolute():
                raise ValueError("harness_task_worker_root must be absolute")
        if self.harness_task_worker_enabled and self.harness_task_worker_root is None:
            raise ValueError(
                "harness_task_worker_enabled requires harness_task_worker_root"
            )
        if not self.harness_task_worker_enabled and self.harness_task_worker_root is not None:
            raise ValueError(
                "harness_task_worker_root requires harness_task_worker_enabled"
            )
        for field_name, upper in (
            ("harness_task_worker_capacity", 64),
            ("harness_task_worker_max_per_turn", 64),
        ):
            value = getattr(self, field_name)
            if type(value) is not int or not 1 <= value <= upper:
                raise ValueError(f"{field_name} must be an integer between 1 and {upper}")
        for field_name in (
            "harness_task_worker_default_timeout_sec",
            "harness_task_worker_max_timeout_sec",
        ):
            value = getattr(self, field_name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or not 1.0 <= float(value) <= 3600.0
            ):
                raise ValueError(f"{field_name} must be between 1 and 3600 seconds")
            setattr(self, field_name, float(value))
        if (
            self.harness_task_worker_default_timeout_sec
            > self.harness_task_worker_max_timeout_sec
        ):
            raise ValueError(
                "harness_task_worker_default_timeout_sec cannot exceed the maximum"
            )
        if self.harness_checkpoint_root is not None:
            if not isinstance(self.harness_checkpoint_root, (str, Path)) or not str(
                self.harness_checkpoint_root
            ).strip():
                raise ValueError("harness_checkpoint_root must be a non-empty path")
            checkpoint_root = Path(self.harness_checkpoint_root).expanduser()
            if not checkpoint_root.is_absolute():
                raise ValueError("harness_checkpoint_root must be absolute")
        if self.harness_file_mutation_enabled and self.harness_checkpoint_root is None:
            raise ValueError(
                "harness_file_mutation_enabled requires harness_checkpoint_root"
            )
        if self.codex_sandbox_executable is not None:
            if not isinstance(self.codex_sandbox_executable, (str, Path)):
                raise ValueError("codex_sandbox_executable must be a path or command name")
            if not str(self.codex_sandbox_executable).strip():
                raise ValueError("codex_sandbox_executable must not be empty")
        if self.codex_sandbox_permission_profile not in {":workspace", ":read-only"}:
            raise ValueError(
                "codex_sandbox_permission_profile must be :workspace or :read-only"
            )
        for field_name in ("codex_sandbox_live_gate", "codex_sandbox_config"):
            candidate = getattr(self, field_name)
            if candidate is not None:
                if not isinstance(candidate, (str, Path)) or not str(candidate).strip():
                    raise ValueError(f"{field_name} must be a non-empty path")
        if self.harness_pipe_lifecycle_gate is not None and (
            not isinstance(self.harness_pipe_lifecycle_gate, (str, Path))
            or not str(self.harness_pipe_lifecycle_gate).strip()
        ):
            raise ValueError("harness_pipe_lifecycle_gate must be a non-empty path")
        if (
            type(self.harness_pipe_session_capacity) is not int
            or not 1 <= self.harness_pipe_session_capacity <= 64
        ):
            raise ValueError(
                "harness_pipe_session_capacity must be an integer between 1 and 64"
            )
        if self.codex_sandbox_live_gate is not None and self.codex_sandbox_config is None:
            raise ValueError(
                "codex_sandbox_config is required with codex_sandbox_live_gate"
            )
        allowlist = self.harness_command_allowlist
        if not isinstance(allowlist, (tuple, list)):
            raise ValueError("harness_command_allowlist must be a sequence")
        if any(
            not isinstance(item, str) or not item.strip() or len(item.strip()) > 128
            for item in allowlist
        ):
            raise ValueError("harness_command_allowlist entries must be bounded strings")
        self.harness_command_allowlist = tuple(item.strip() for item in allowlist)
        if self.codex_sandbox_enabled:
            if self.codex_sandbox_permission_profile != ":read-only":
                raise ValueError(
                    "the production Codex sandbox gate currently supports only :read-only"
                )
            missing = [
                name
                for name in (
                    "codex_sandbox_executable",
                    "codex_sandbox_live_gate",
                    "codex_sandbox_config",
                )
                if getattr(self, name) is None
            ]
            if missing:
                raise ValueError(
                    "codex_sandbox_enabled requires " + ", ".join(missing)
                )
            if not self.harness_command_allowlist:
                raise ValueError(
                    "codex_sandbox_enabled requires at least one harness command allowlist entry"
                )
        if self.harness_pipe_sessions_enabled:
            if self.mock:
                raise ValueError(
                    "harness_pipe_sessions_enabled is unavailable in mock Runtime"
                )
            if not self.codex_sandbox_enabled:
                raise ValueError(
                    "harness_pipe_sessions_enabled requires codex_sandbox_enabled"
                )
            if self.harness_pipe_lifecycle_gate is None:
                raise ValueError(
                    "harness_pipe_sessions_enabled requires harness_pipe_lifecycle_gate"
                )
        elif self.harness_pipe_lifecycle_gate is not None:
            raise ValueError(
                "harness_pipe_lifecycle_gate requires harness_pipe_sessions_enabled"
            )
        workers = self.pulse_worker_capacity
        if type(workers) is not int or not 1 <= workers <= 64:
            raise ValueError(
                "pulse_worker_capacity must be an integer between 1 and 64"
            )
        residents = self.pi_resident_session_limit
        if type(residents) is not int or not 1 <= residents <= 256:
            raise ValueError(
                "pi_resident_session_limit must be an integer between 1 and 256"
            )
        if residents < workers:
            raise ValueError(
                "pi_resident_session_limit must be at least pulse_worker_capacity"
            )
        connectivity_interval = self.connectivity_interval_ticks
        if (
            connectivity_interval is not None
            and (
                type(connectivity_interval) is not int
                or connectivity_interval <= 0
            )
        ):
            raise ValueError(
                "connectivity_interval_ticks must be a positive integer or None"
            )
        if (
            type(self.metrics_max_bytes) is not int
            or self.metrics_max_bytes < 1024 * 1024
        ):
            raise ValueError("metrics_max_bytes must be an integer >= 1 MiB")
        if (
            type(self.metrics_archive_count) is not int
            or not 0 <= self.metrics_archive_count <= 16
        ):
            raise ValueError(
                "metrics_archive_count must be an integer between 0 and 16"
            )
        if (
            type(self.metrics_replay_bytes) is not int
            or not 64 * 1024 <= self.metrics_replay_bytes <= 32 * 1024 * 1024
        ):
            raise ValueError(
                "metrics_replay_bytes must be an integer between 64 KiB and 32 MiB"
            )
        ttl = self.runtime_lease_ttl_sec
        if (
            isinstance(ttl, bool)
            or not isinstance(ttl, (int, float))
            or not math.isfinite(float(ttl))
            or not 5.0 <= float(ttl) <= 3600.0
        ):
            raise ValueError(
                "runtime_lease_ttl_sec must be a finite number between 5 and 3600"
            )
        interval = self.runtime_lease_renew_interval_sec
        if (
            isinstance(interval, bool)
            or not isinstance(interval, (int, float))
            or not math.isfinite(float(interval))
            or not 1.0 <= float(interval) <= float(ttl) / 2.0
        ):
            raise ValueError(
                "runtime_lease_renew_interval_sec must be a finite number "
                "between 1 and half the lease TTL"
            )
        self.runtime_lease_ttl_sec = float(ttl)
        self.runtime_lease_renew_interval_sec = float(interval)
        shutdown_timeout = self.runtime_shutdown_timeout_sec
        if (
            isinstance(shutdown_timeout, bool)
            or not isinstance(shutdown_timeout, (int, float))
            or not math.isfinite(float(shutdown_timeout))
            or not 0.05 <= float(shutdown_timeout) <= 300.0
        ):
            raise ValueError(
                "runtime_shutdown_timeout_sec must be a finite number "
                "between 0.05 and 300"
            )
        self.runtime_shutdown_timeout_sec = float(shutdown_timeout)
        lane_reserve = self.center_lane_reservation_per_tick
        if type(lane_reserve) is not int or not 0 <= lane_reserve <= 32:
            raise ValueError(
                "center_lane_reservation_per_tick must be an integer between 0 and 32"
            )
        starvation_boost = self.center_starvation_boost
        if (
            isinstance(starvation_boost, bool)
            or not isinstance(starvation_boost, (int, float))
            or not math.isfinite(float(starvation_boost))
            or not 0.0 <= float(starvation_boost) <= 1.0
        ):
            raise ValueError(
                "center_starvation_boost must be a finite number between 0 and 1"
            )
        self.center_starvation_boost = float(starvation_boost)
        debt_cap = self.center_starvation_debt_cap
        if type(debt_cap) is not int or not 1 <= debt_cap <= 10_000:
            raise ValueError(
                "center_starvation_debt_cap must be an integer between 1 and 10000"
            )
        history_limit = self.center_reservation_history_limit
        if type(history_limit) is not int or not 1 <= history_limit <= 200:
            raise ValueError(
                "center_reservation_history_limit must be an integer between 1 and 200"
            )
        budget = self.living_concern_reentry_budget_per_tick
        if type(budget) is not int or not 0 <= budget <= 64:
            raise ValueError(
                "living_concern_reentry_budget_per_tick must be an integer "
                "between 0 and 64"
            )
        priority = self.living_concern_reentry_priority
        if (
            isinstance(priority, bool)
            or not isinstance(priority, (int, float))
            or not math.isfinite(float(priority))
            or not 0.0 <= float(priority) <= 1.0
        ):
            raise ValueError(
                "living_concern_reentry_priority must be a finite number "
                "between 0 and 1"
            )
        self.living_concern_reentry_priority = float(priority)
        refractory = self.living_orientation_refractory_sec
        if (
            isinstance(refractory, bool)
            or not isinstance(refractory, (int, float))
            or not math.isfinite(float(refractory))
            or not 0.0 <= float(refractory) <= 86_400.0
        ):
            raise ValueError(
                "living_orientation_refractory_sec must be a finite number "
                "between 0 and 86400"
            )
        self.living_orientation_refractory_sec = float(refractory)
        orientation_priority = self.living_orientation_priority
        if (
            isinstance(orientation_priority, bool)
            or not isinstance(orientation_priority, (int, float))
            or not math.isfinite(float(orientation_priority))
            or not 0.0 <= float(orientation_priority) <= 1.0
        ):
            raise ValueError(
                "living_orientation_priority must be a finite number "
                "between 0 and 1"
            )
        self.living_orientation_priority = float(orientation_priority)
        orientation_history_limit = self.living_orientation_history_limit
        if (
            type(orientation_history_limit) is not int
            or not 1 <= orientation_history_limit <= 200
        ):
            raise ValueError(
                "living_orientation_history_limit must be an integer "
                "between 1 and 200"
            )


@dataclass
class TuningView:
    """Three separate facts about one knob set, never collapsed into one."""

    commanded: dict[str, float | None]
    observed: dict[str, float]
    applied_at_tick: int | None

    def as_dict(self) -> dict:
        return {
            "commanded": dict(self.commanded),
            "observed": dict(self.observed),
            "applied_at_tick": self.applied_at_tick,
        }


@dataclass
class _Delegation:
    """One delegation as the runtime sees it, from request to result."""

    id: str
    task: str
    to: str | None
    backend: str | None
    caller_id: str
    created_at: str
    status: str = "queued"
    completed_at: str | None = None
    target_id: str | None = None
    targets: list[str] = field(default_factory=list)
    record_ids: list[str] = field(default_factory=list)
    mode: str | None = None
    result: str | None = None
    route: dict | None = None
    error: dict | None = None


class _MockHarnessRuntime:
    """Explicit offline Harness used only when ``mock=True`` is requested.

    It preserves the Harness boundary for tests and demos: EngramManager never
    silently falls back to its legacy direct-LLM pulse path.  It deliberately
    has no durable Pi bindings or live process count.
    """

    def __init__(self, workspace: Path, llm: LLMAdapter) -> None:
        self._workspace = workspace.resolve()
        self._llm = llm
        self._bootstrapped: set[str] = set()
        self._closed = False
        self._lock = threading.RLock()

    def preflight(self) -> None:
        with self._lock:
            if self._closed:
                raise self._error("harness_closed", "the mock Harness is closed")

    def run_turn(
        self,
        engram_id: str,
        prompt: str,
        *,
        timeout_sec: float | None = None,
        bootstrap_text: str | None = None,
        turn_id: str | None = None,
    ) -> HarnessTurnResult:
        del timeout_sec
        del turn_id
        if not isinstance(engram_id, str) or not engram_id.strip():
            raise self._error("harness_input_invalid", "engram_id must be non-empty")
        if not isinstance(prompt, str) or (
            bootstrap_text is not None and not isinstance(bootstrap_text, str)
        ):
            raise self._error("harness_input_invalid", "Harness input must be text")
        with self._lock:
            if self._closed:
                raise self._error("harness_closed", "the mock Harness is closed")
            first = engram_id not in self._bootstrapped
            parts = []
            if first and bootstrap_text:
                parts.append(bootstrap_text)
            if prompt:
                parts.append(prompt)
            submitted = "\n\n".join(parts)
            result = self._llm.complete([{"role": "user", "content": submitted}])
            self._bootstrapped.add(engram_id)
        return HarnessTurnResult(
            engram_id=engram_id,
            session_id=f"mock:{engram_id}",
            session_file=str(
                self._workspace / ".pulse" / "mock-harness" / f"{engram_id}.jsonl"
            ),
            content=result.content,
            stop_reason="stop",
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            cached_tokens=getattr(result, "cached_tokens", 0),
            cache_write_tokens=getattr(result, "cache_write_tokens", 0),
            evidence_class="EXPLICIT_MOCK",
        )

    def snapshot(self, engram_id: str) -> dict[str, Any]:
        with self._lock:
            if engram_id not in self._bootstrapped:
                raise HarnessError(
                    "pi_session_unknown",
                    f"Engram {engram_id!r} has no mock Harness session",
                    "run a turn before requesting its Harness snapshot",
                    phase="snapshot",
                )
            return {
                "engram_id": engram_id,
                "state": "READY",
                "bootstrapped": True,
            }

    def abort(self, engram_id: str) -> None:
        raise self._error(
            "mock_turn_not_running",
            f"Engram {engram_id!r} has no asynchronous mock turn to abort",
        )

    def steer(self, engram_id: str, content: str) -> None:
        del content
        raise self._error(
            "mock_turn_not_running",
            f"Engram {engram_id!r} has no asynchronous mock turn to steer",
        )

    def succeed(
        self,
        old_engram_id: str,
        new_engram_id: str,
        *,
        capacity_timeout_sec: float | None = None,
    ) -> None:
        del capacity_timeout_sec
        with self._lock:
            if self._closed:
                raise self._error("harness_closed", "the mock Harness is closed")
            self._bootstrapped.discard(old_engram_id)
            self._bootstrapped.discard(new_engram_id)

    def close_session(self, engram_id: str) -> None:
        with self._lock:
            self._bootstrapped.discard(engram_id)


    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._bootstrapped.clear()

    @staticmethod
    def _error(code: str, detail: str) -> HarnessError:
        return HarnessError(
            code,
            detail,
            "construct a new explicit mock RuntimeService",
            phase="mock",
        )


@dataclass(slots=True)
class _RuntimePhysicalConvergenceSource:
    """One exact adapter retained after the public shutdown deadline."""

    component: str
    source: PiHarnessRuntime | MCPRuntimeService
    probe: RuntimeRetainedOwnerProbe
    recovery_permit: RuntimeRecoveryPermit | None
    started_at: datetime
    started_monotonic: float
    semantic_report: tuple[Any, ...]
    next_observation_monotonic: float
    backoff_seconds: float
    observation_count: int = 0
    observation_failure_logged: bool = False
    observation: _RuntimePhysicalObservation | None = None


@dataclass(slots=True)
class _RuntimePhysicalObservation:
    """One isolated adapter call owned by the convergence coordinator.

    Python cannot forcibly stop a thread that ignores its deadline.  Keeping
    at most one call in flight per exact source bounds that failure to two
    daemon threads while the coordinator remains able to observe the other
    source and publish its proof.
    """

    deadline_monotonic: float
    done: threading.Event = field(default_factory=threading.Event)
    state: dict[str, Any] = field(default_factory=dict)
    thread: threading.Thread | None = None
    timeout_recorded: bool = False


class _RuntimePhysicalConvergenceCoordinator:
    """One typed producer for late Pi/MCP physical-owner evidence.

    The coordinator never mutates the public shutdown builder.  It repeatedly
    asks only the two exact production adapters for a bounded close
    observation and publishes canonical snapshots to the existing passive
    probe registry.  The Storage finalizer therefore remains snapshot-only.
    """

    _MAX_SOURCES = 2
    _MIN_BACKOFF_SECONDS = 0.025
    _MAX_BACKOFF_SECONDS = 0.5
    _OBSERVATION_BUDGET_SECONDS = 0.1

    def __init__(
        self,
        registry: RuntimeRetainedOwnerProbeRegistry,
        *,
        translator: Callable[[str, Any], dict[str, Any]],
    ) -> None:
        if type(registry) is not RuntimeRetainedOwnerProbeRegistry:
            raise TypeError(
                "registry must be a RuntimeRetainedOwnerProbeRegistry"
            )
        if not callable(translator):
            raise TypeError("translator must be callable")
        self._registry = registry
        self._translator = translator
        self._condition = threading.Condition(threading.RLock())
        self._sources: dict[str, _RuntimePhysicalConvergenceSource] = {}
        self._registered_sources: dict[
            str,
            PiHarnessRuntime | MCPRuntimeService,
        ] = {}
        self._probes: dict[str, RuntimeRetainedOwnerProbe] = {}
        self._worker: threading.Thread | None = None
        self._worker_started = False
        self._worker_start_count = 0
        self._observation_count = 0
        self._observation_failures = 0
        self._registrations_sealed = False
        self._idle = threading.Event()
        self._idle.set()

    def prepare_pi(
        self,
        source: PiHarnessRuntime,
        initial_report: ShutdownComponentReport,
        *,
        recovery_permit: RuntimeRecoveryPermit | None = None,
    ) -> RuntimeRetainedOwnerProbe:
        """Register Pi's passive probe before public retained publication."""

        if type(source) is not PiHarnessRuntime:
            raise TypeError("Pi convergence source must be exact PiHarnessRuntime")
        if (
            recovery_permit is not None
            and type(recovery_permit) is not RuntimeRecoveryPermit
        ):
            raise TypeError(
                "Pi convergence preparation permit must be exact"
            )
        return self._prepare(
            component="harness",
            source=source,
            initial_report=initial_report,
            recovery_permit=recovery_permit,
        ).probe

    def prepare_mcp(
        self,
        source: MCPRuntimeService,
        initial_report: ShutdownComponentReport,
    ) -> RuntimeRetainedOwnerProbe:
        """Register MCP's passive probe before public retained publication."""

        if type(source) is not MCPRuntimeService:
            raise TypeError(
                "MCP convergence source must be exact MCPRuntimeService"
            )
        return self._prepare(
            component="mcp_runtime",
            source=source,
            initial_report=initial_report,
            recovery_permit=None,
        ).probe

    def register_pi(
        self,
        source: PiHarnessRuntime,
        initial_report: ShutdownComponentReport,
        *,
        recovery_permit: RuntimeRecoveryPermit,
    ) -> RuntimeRetainedOwnerProbe:
        if type(source) is not PiHarnessRuntime:
            raise TypeError("Pi convergence source must be exact PiHarnessRuntime")
        if type(recovery_permit) is not RuntimeRecoveryPermit:
            raise TypeError(
                "Pi convergence requires an exact RuntimeRecoveryPermit"
            )
        record = self._prepare(
            component="harness",
            source=source,
            initial_report=initial_report,
            recovery_permit=recovery_permit,
        )
        self._activate(record)
        return record.probe

    def register_mcp(
        self,
        source: MCPRuntimeService,
        initial_report: ShutdownComponentReport,
    ) -> RuntimeRetainedOwnerProbe:
        if type(source) is not MCPRuntimeService:
            raise TypeError(
                "MCP convergence source must be exact MCPRuntimeService"
            )
        record = self._prepare(
            component="mcp_runtime",
            source=source,
            initial_report=initial_report,
            recovery_permit=None,
        )
        self._activate(record)
        return record.probe

    def _prepare(
        self,
        *,
        component: str,
        source: PiHarnessRuntime | MCPRuntimeService,
        initial_report: ShutdownComponentReport,
        recovery_permit: RuntimeRecoveryPermit | None,
    ) -> _RuntimePhysicalConvergenceSource:
        if type(initial_report) is not ShutdownComponentReport:
            raise TypeError("initial_report must be a ShutdownComponentReport")
        if initial_report.component != component:
            raise ValueError("initial_report component does not match source")
        if initial_report.physical_exit_proven:
            raise ValueError("a physically final source must not be retained")

        with self._condition:
            existing_source = self._registered_sources.get(component)
            if existing_source is not None:
                if existing_source is not source:
                    raise RuntimeError(
                        f"physical convergence source conflict: {component}"
                    )
                existing_record = self._sources.get(component)
                if existing_record is not None:
                    return existing_record
                probe = self._probes[component]
                return _RuntimePhysicalConvergenceSource(
                    component=component,
                    source=source,
                    probe=probe,
                    recovery_permit=recovery_permit,
                    started_at=probe.snapshot().started_at,
                    started_monotonic=max(
                        0.0,
                        time.monotonic() - probe.snapshot().elapsed_seconds,
                    ),
                    semantic_report=self._semantic_report(probe.snapshot()),
                    next_observation_monotonic=time.monotonic(),
                    backoff_seconds=self._MIN_BACKOFF_SECONDS,
                )
            if self._registrations_sealed:
                raise RuntimeError(
                    "physical convergence source registrations are sealed"
                )
            if len(self._registered_sources) >= self._MAX_SOURCES:
                raise RuntimeError("physical convergence source capacity exceeded")

            # Register the passive cell before making the source visible to
            # the worker.  A finalizer can never observe retained state without
            # first having the corresponding wakeable probe in its census.
            probe = RuntimeRetainedOwnerProbe(component, initial_report)
            self._registry.register(probe)
            source_record = _RuntimePhysicalConvergenceSource(
                component=component,
                source=source,
                probe=probe,
                recovery_permit=recovery_permit,
                started_at=initial_report.started_at,
                started_monotonic=max(
                    0.0,
                    time.monotonic() - initial_report.elapsed_seconds,
                ),
                semantic_report=self._semantic_report(initial_report),
                next_observation_monotonic=time.monotonic(),
                backoff_seconds=self._MIN_BACKOFF_SECONDS,
            )
            self._registered_sources[component] = source
            self._probes[component] = probe
            return source_record

    def _activate(self, source_record: _RuntimePhysicalConvergenceSource) -> None:
        with self._condition:
            existing_source = self._registered_sources.get(
                source_record.component
            )
            if existing_source is not source_record.source:
                raise RuntimeError(
                    f"physical convergence source is not registered: "
                    f"{source_record.component}"
                )
            existing_record = self._sources.get(source_record.component)
            if existing_record is not None:
                if existing_record.source is not source_record.source:
                    raise RuntimeError(
                        f"physical convergence source conflict: "
                        f"{source_record.component}"
                    )
                return
            if source_record.probe.snapshot().physical_exit_proven:
                return
            self._sources[source_record.component] = source_record
            self._idle.clear()
            if not self._worker_started:
                worker = threading.Thread(
                    target=self._run,
                    name="runtime-physical-convergence",
                    daemon=True,
                )
                self._worker = worker
                try:
                    worker.start()
                except Exception:
                    # Thread.start is the ownership commit point. Preserve the
                    # passive probe/source identity, but roll back active and
                    # lifecycle state so an exact same-source retry can create
                    # the sole worker instead of being silently orphaned.
                    self._worker = None
                    self._sources.pop(source_record.component, None)
                    if not self._sources:
                        self._idle.set()
                    raise
                self._worker_started = True
                self._worker_start_count += 1
            elif self._worker is None or not self._worker.is_alive():
                raise RuntimeError(
                    "physical convergence worker retired before source final"
                )
            self._condition.notify_all()

    def reconcile_pi(
        self,
        source: PiHarnessRuntime,
        report: ShutdownComponentReport,
        *,
        recovery_permit: RuntimeRecoveryPermit,
    ) -> RuntimeRetainedOwnerProbe:
        if type(source) is not PiHarnessRuntime:
            raise TypeError("Pi convergence source must be exact PiHarnessRuntime")
        if type(recovery_permit) is not RuntimeRecoveryPermit:
            raise TypeError(
                "Pi convergence requires an exact RuntimeRecoveryPermit"
            )
        record = self._prepare(
            component="harness",
            source=source,
            initial_report=(
                report
                if not report.physical_exit_proven
                else RuntimeService._initial_retained_component_report(
                    "harness"
                )
            ),
            recovery_permit=recovery_permit,
        )
        self._reconcile(record, report)
        return record.probe

    def reconcile_mcp(
        self,
        source: MCPRuntimeService,
        report: ShutdownComponentReport,
    ) -> RuntimeRetainedOwnerProbe:
        if type(source) is not MCPRuntimeService:
            raise TypeError(
                "MCP convergence source must be exact MCPRuntimeService"
            )
        record = self._prepare(
            component="mcp_runtime",
            source=source,
            initial_report=(
                report
                if not report.physical_exit_proven
                else RuntimeService._initial_retained_component_report(
                    "mcp_runtime"
                )
            ),
            recovery_permit=None,
        )
        self._reconcile(record, report)
        return record.probe

    def _reconcile(
        self,
        source_record: _RuntimePhysicalConvergenceSource,
        report: ShutdownComponentReport,
    ) -> None:
        if type(report) is not ShutdownComponentReport:
            raise TypeError("report must be a ShutdownComponentReport")
        if report.component != source_record.component:
            raise ValueError("report component does not match source")
        with self._condition:
            current = source_record.probe.snapshot()
            if current.physical_exit_proven:
                # First physical proof is terminal private evidence. A later
                # stale retained observation can neither reactivate the source
                # nor regress the canonical probe.
                return
            if self._semantic_report(report) != self._semantic_report(current):
                self._registry.publish(source_record.probe, report)
                source_record.semantic_report = self._semantic_report(report)
            if report.physical_exit_proven:
                self._sources.pop(source_record.component, None)
                if not self._sources:
                    self._idle.set()
                self._condition.notify_all()
                return
        self._activate(source_record)

    def seal_registrations(self) -> None:
        """Declare the Runtime's exact source census complete.

        The sole worker may retire only after this boundary and after every
        active source has physical proof.  Until then it stays dormant, so a
        second source can join after the first source converges without a new
        worker generation or an unobserved retained owner.
        """

        with self._condition:
            self._registrations_sealed = True
            self._condition.notify_all()

    def _run(self) -> None:
        while True:
            with self._condition:
                if not self._sources:
                    self._idle.set()
                    if self._registrations_sealed:
                        return
                    self._condition.wait()
                    continue
                now = time.monotonic()
                wait_until: list[float] = []
                for source in tuple(self._sources.values()):
                    observation = source.observation
                    if observation is not None:
                        if observation.done.is_set():
                            self._complete_observation(source, observation)
                            if self._sources.get(source.component) is source:
                                wait_until.append(
                                    source.next_observation_monotonic
                                )
                            continue
                        if (
                            now >= observation.deadline_monotonic
                            and not observation.timeout_recorded
                        ):
                            observation.timeout_recorded = True
                            self._observation_failures += 1
                            if not source.observation_failure_logged:
                                _logger.warning(
                                    "Bounded physical convergence observation "
                                    "timed out for %s",
                                    source.component,
                                )
                                source.observation_failure_logged = True
                        if not observation.timeout_recorded:
                            wait_until.append(observation.deadline_monotonic)
                        continue
                    if source.next_observation_monotonic <= now:
                        self._start_observation(source)
                        if source.observation is None:
                            # Thread.start failed and was synchronously folded
                            # into the ordinary failure/backoff state. Include
                            # that new due time in this wait generation.
                            wait_until.append(
                                source.next_observation_monotonic
                            )
                    else:
                        wait_until.append(source.next_observation_monotonic)
                if not self._sources:
                    continue
                if wait_until:
                    timeout = max(0.0, min(wait_until) - time.monotonic())
                    self._condition.wait(timeout=timeout)
                else:
                    # Every active source has one timed-out call in flight.
                    # Its completion notifies this condition; no replacement
                    # call is spawned, so a hostile adapter cannot grow an
                    # unbounded thread fleet.
                    self._condition.wait()

    def _start_observation(
        self,
        source: _RuntimePhysicalConvergenceSource,
    ) -> None:
        deadline = time.monotonic() + self._OBSERVATION_BUDGET_SECONDS
        observation = _RuntimePhysicalObservation(deadline)
        source.observation = observation
        source.observation_count += 1
        self._observation_count += 1

        def observe() -> None:
            try:
                observation.state["report"] = self._call_source(
                    source,
                    deadline=deadline,
                )
            except Exception:  # noqa: BLE001 - source remains retained
                observation.state["failed"] = True
            finally:
                with self._condition:
                    observation.done.set()
                    self._condition.notify_all()

        owner = threading.Thread(
            target=observe,
            name=f"runtime-convergence-observe-{source.component}",
            daemon=True,
        )
        observation.thread = owner
        try:
            owner.start()
        except Exception:  # noqa: BLE001 - retain source and retry with backoff
            observation.state["failed"] = True
            observation.done.set()
            # We still own the coordinator condition here. Consume the failed
            # attempt synchronously; notifying before the outer worker waits
            # would be a lost wakeup and strand a completed observation.
            self._complete_observation(source, observation)

    def _complete_observation(
        self,
        source: _RuntimePhysicalConvergenceSource,
        observation: _RuntimePhysicalObservation,
    ) -> None:
        if source.observation is not observation:
            return
        source.observation = None
        report = observation.state.get("report")
        if type(report) is ShutdownComponentReport:
            semantic = self._semantic_report(report)
            if semantic != source.semantic_report:
                try:
                    self._registry.publish(source.probe, report)
                except RuntimeError:
                    if not source.probe.snapshot().physical_exit_proven:
                        _logger.exception(
                            "Physical convergence publish failed for %s",
                            source.component,
                        )
                else:
                    source.semantic_report = semantic
                    source.backoff_seconds = self._MIN_BACKOFF_SECONDS
                    source.observation_failure_logged = False
            if source.probe.snapshot().physical_exit_proven:
                self._sources.pop(source.component, None)
                if not self._sources:
                    self._idle.set()
                self._condition.notify_all()
                return
        elif not observation.timeout_recorded:
            self._observation_failures += 1
            if not source.observation_failure_logged:
                _logger.warning(
                    "Bounded physical convergence observation failed for %s",
                    source.component,
                )
                source.observation_failure_logged = True
        source.next_observation_monotonic = (
            time.monotonic() + source.backoff_seconds
        )
        source.backoff_seconds = min(
            self._MAX_BACKOFF_SECONDS,
            source.backoff_seconds * 2.0,
        )

    def _call_source(
        self,
        source: _RuntimePhysicalConvergenceSource,
        *,
        deadline: float,
    ) -> ShutdownComponentReport | None:
        if type(source.source) is PiHarnessRuntime:
            permit = source.recovery_permit
            if type(permit) is not RuntimeRecoveryPermit:
                raise RuntimeError("Pi recovery authority is unavailable")
            result = source.source.close(
                timeout_sec=max(0.0, deadline - time.monotonic()),
                recovery_permit=permit,
            )
        elif type(source.source) is MCPRuntimeService:
            result = source.source.close(deadline=deadline)
        else:  # pragma: no cover - registration is exact-type fenced
            raise TypeError("unsupported physical convergence source")
        evidence = self._translator(source.component, result)
        if not evidence:
            raise RuntimeError("physical convergence evidence is unavailable")
        return component_report(
            source.component,
            effect=evidence.get(
                "effect",
                ShutdownEffectState.UNCERTAIN,
            ),
            owner=evidence.get("owner", ShutdownOwnerState.ESCAPED),
            process_tree=evidence.get(
                "process_tree",
                ShutdownProcessTreeState.UNKNOWN,
            ),
            cancel=evidence.get(
                "cancel",
                ShutdownCancelState.SIGNALLED,
            ),
            started_at=source.started_at,
            started_monotonic=source.started_monotonic,
            active_before=max(0, int(evidence.get("active_before", 1))),
            unresolved=max(0, int(evidence.get("unresolved", 1))),
            error_code=evidence.get("error_code"),
        )

    @staticmethod
    def _semantic_report(report: ShutdownComponentReport) -> tuple[Any, ...]:
        return (
            report.effect,
            report.owner,
            report.process_tree,
            report.cancel,
            report.active_before,
            report.unresolved,
            report.error_code,
        )

    def wait_until_idle(self, timeout: float | None = None) -> bool:
        """Wait only for test/diagnostic ownership; never drives adapters."""

        return self._idle.wait(timeout=timeout)

    def safe_snapshot(self) -> dict[str, Any]:
        with self._condition:
            worker = self._worker
            return {
                "protocol_version": "runtime-retained-convergence.v1",
                "worker_started": self._worker_started,
                "worker_alive": bool(worker is not None and worker.is_alive()),
                "worker_start_count": self._worker_start_count,
                "registrations_sealed": self._registrations_sealed,
                "registered_sources": len(self._registered_sources),
                "active_sources": len(self._sources),
                "components": sorted(self._sources),
                "observation_count": self._observation_count,
                "observation_failures": self._observation_failures,
                "inflight_observations": sum(
                    source.observation is not None
                    for source in self._sources.values()
                ),
            }


class _RuntimeShutdownCoordinator:
    """Bounded Runtime close protocol shared only by RuntimeService."""

    def _close_harness_component(
        self,
        harness: HarnessRuntime,
        *,
        deadline: ShutdownDeadline,
        after_publication_revoke: bool,
    ) -> Any:
        """Close Pi with its typed lifecycle authority, other Harnesses plainly.

        The generic Harness protocol intentionally remains small.  Pi owns an
        additional durable JSONL continuity boundary, so only the concrete Pi
        implementation receives Runtime's recovery capability.  Test/custom
        Harnesses cannot gain that authority by accepting similarly named
        keyword arguments.
        """

        if type(harness) is PiHarnessRuntime:
            recovery_permit = None
            if after_publication_revoke:
                candidate = getattr(self, "_recovery_permit", None)
                if type(candidate) is RuntimeRecoveryPermit:
                    recovery_permit = candidate
            return harness.close(
                timeout_sec=deadline.remaining_seconds(),
                recovery_permit=recovery_permit,
            )
        return harness.close()

    def _shutdown_expected_components(
        self,
        *,
        harness_override: HarnessRuntime | None = None,
    ) -> tuple[str, ...]:
        """Freeze only the execution domains actually attached to this Runtime.

        A fully constructed Runtime retains the original complete evidence
        surface.  A constructor failure must not manufacture escaped evidence
        for components that never existed.
        """

        names: list[str] = []
        if getattr(self, "_gateway", None) is not None:
            names.append("tool_gateway")
        if getattr(self, "_executor", None) is not None:
            names.append("delegation_workers")
        if getattr(self, "_engine", None) is not None:
            names.extend(
                ("pulse_engine", "pulse_workers", "succession_workers")
            )
        if hasattr(self, "_recovery_owner_tasks"):
            names.append("recovery_owners")
        if getattr(self, "_publication_gate", None) is not None:
            names.extend(("publication_transactions", "publication_watchdog"))
        if getattr(self, "_causal_ledger", None) is not None:
            names.extend(
                (
                    "causal_recovery",
                    *(
                        f"recovery_{domain}"
                        for domain in _SHUTDOWN_RECOVERY_DOMAINS
                    ),
                )
            )
        if getattr(self, "_engine", None) is not None:
            names.append("runtime_tick")
        if harness_override is not None or getattr(self, "_harness", None) is not None:
            names.append("harness")
        if getattr(self, "_harness_action_broker", None) is not None:
            names.append("harness_actions")
        if getattr(self, "_life_tools", None) is not None:
            names.append("life_tools")
        if getattr(self, "_harness_terminal_sessions", None) is not None:
            names.append("terminal_sessions")
        if (
            getattr(self, "_harness_task_worker_bridge", None) is not None
            or getattr(self, "_harness_task_worker_backend", None) is not None
        ):
            names.append("task_workers")
        if getattr(self, "_harness_mcp_service", None) is not None:
            names.append("mcp_runtime")
        if getattr(self, "_harness_action_executor", None) is not None:
            names.append("action_workers")
        if getattr(self, "_harness_sandbox_backend", None) is not None:
            names.append("sandbox_processes")
        if getattr(self, "_lease_keeper", None) is not None:
            names.append("owner_lease")
        return tuple(names)

    @staticmethod
    def _start_shutdown_call(
        component: str,
        function: Callable[[], Any],
    ) -> dict[str, Any]:
        """Start one potentially uncooperative closer outside the caller."""

        done = threading.Event()
        state: dict[str, Any] = {}

        def run() -> None:
            try:
                state["result"] = function()
            except BaseException as exc:  # noqa: BLE001 - evidence, not propagation
                state["error_type"] = type(exc).__name__.casefold()
            finally:
                done.set()

        thread = threading.Thread(
            target=run,
            name=f"shutdown-{component}",
            daemon=True,
        )
        thread.start()
        return {
            "component": component,
            "done": done,
            "state": state,
            "thread": thread,
            "started_at": _now(),
            "started_monotonic": time.monotonic(),
        }

    def _start_registered_recovery_owner(
        self,
        component: str,
        function: Callable[[], Any],
    ) -> dict[str, Any]:
        """Start and retain a recovery owner until final Runtime close census."""

        task = self._start_shutdown_call(component, function)
        with self._recovery_owner_lock:
            self._recovery_owner_tasks.append(task)
        return task

    def _snapshot_recovery_owners(self) -> tuple[Mapping[str, Any], ...]:
        with self._recovery_owner_lock:
            return tuple(self._recovery_owner_tasks)

    @staticmethod
    def _join_recovery_owners(
        tasks: tuple[Mapping[str, Any], ...],
    ) -> dict[str, Any]:
        """Join every previously launched recovery thread without losing one."""

        for task in tasks:
            task["done"].wait()
        unresolved = 0
        failed = 0
        for task in tasks:
            thread = task["thread"]
            if thread is not threading.current_thread():
                thread.join()
            unresolved += int(thread.is_alive() or not task["done"].is_set())
            failed += int("error_type" in task["state"])
        return {
            "active_before": len(tasks),
            "unresolved": unresolved,
            "owner_joined": unresolved == 0,
            "process_tree_state": "not_applicable",
            "failed_recoveries": failed,
        }

    @staticmethod
    def _observe_shutdown_call(
        task: Mapping[str, Any],
        deadline: ShutdownDeadline,
        *,
        process_tree: ShutdownProcessTreeState = (
            ShutdownProcessTreeState.NOT_APPLICABLE
        ),
        cancel: ShutdownCancelState = ShutdownCancelState.SIGNALLED,
    ) -> ShutdownComponentReport:
        done = task["done"]
        done.wait(timeout=deadline.remaining_seconds())
        state = task["state"]
        thread = task["thread"]
        if done.is_set() and thread is not threading.current_thread():
            thread.join(timeout=deadline.remaining_seconds())
        # A retained owner publishes its terminal snapshot from the wrapper's
        # ``finally`` block, immediately before the thread returns.  At that
        # boundary the current thread is still technically alive, but no
        # adapter code remains to run and the done latch is already set.
        joined = done.is_set() and (
            thread is threading.current_thread() or not thread.is_alive()
        )
        failed = "error_type" in state
        component = str(task["component"])
        if not joined:
            effect = ShutdownEffectState.UNCERTAIN
            owner = ShutdownOwnerState.ESCAPED
            unresolved = 1
            error_code = "shutdown_owner_unresolved"
        elif failed:
            effect = ShutdownEffectState.UNCERTAIN
            owner = ShutdownOwnerState.JOINED
            unresolved = 0
            error_code = "shutdown_component_failed"
        else:
            if (
                component == "harness"
                and task.get("result_authority") != "pi_harness_v1"
            ):
                # An opaque/custom Harness may return a Mapping that blocks,
                # lies about child owners, or claims an empty process tree.
                # Runtime trusts only the exact built-in Pi implementation;
                # for every other adapter the wrapper-thread observation is
                # the entire evidence surface.
                override = {}
            else:
                override = _RuntimeShutdownCoordinator._shutdown_result_evidence(
                    component,
                    state.get("result"),
                )
            if not override and process_tree is ShutdownProcessTreeState.UNKNOWN:
                # A wrapper returning proves only that wrapper's thread ended.
                # It is not evidence that its internal readers, sessions or
                # child process tree exited.
                effect = ShutdownEffectState.UNCERTAIN
                owner = ShutdownOwnerState.UNKNOWN
                unresolved = 1
                error_code = "shutdown_component_exit_unproven"
            else:
                effect = override.get("effect", ShutdownEffectState.SETTLED)
                owner = override.get("owner", ShutdownOwnerState.JOINED)
                process_tree = override.get("process_tree", process_tree)
                cancel = override.get("cancel", cancel)
                unresolved = int(override.get("unresolved", 0))
                error_code = override.get("error_code")
        active_before = 1
        if joined and not failed:
            active_before = int(override.get("active_before", active_before))
        return component_report(
            component,
            effect=effect,
            owner=owner,
            process_tree=process_tree,
            cancel=cancel,
            started_at=task["started_at"],
            started_monotonic=task["started_monotonic"],
            active_before=active_before,
            unresolved=unresolved,
            error_code=error_code,
        )

    @staticmethod
    def _shutdown_result_evidence(
        component: str,
        result: Any,
    ) -> dict[str, Any]:
        """Translate explicit component outcomes without inspecting payloads."""

        if component == "life_tools" and isinstance(result, Mapping):
            active = max(0, int(result.get("active_before", 0)))
            unresolved = max(0, int(result.get("unresolved", 0)))
            cancelled = max(0, int(result.get("cancelled", 0)))
            joined = result.get("owner_joined") is True
            if unresolved or not joined:
                return {
                    "effect": ShutdownEffectState.UNCERTAIN,
                    "owner": ShutdownOwnerState.ESCAPED,
                    "process_tree": ShutdownProcessTreeState.NOT_APPLICABLE,
                    "cancel": (
                        ShutdownCancelState.SIGNALLED
                        if cancelled
                        else ShutdownCancelState.UNKNOWN
                    ),
                    "active_before": active,
                    "unresolved": max(1, unresolved),
                    "error_code": "life_worker_exit_unproven",
                }
            return {
                "effect": (
                    ShutdownEffectState.NOT_STARTED
                    if active == 0
                    else ShutdownEffectState.SETTLED
                ),
                "owner": ShutdownOwnerState.JOINED,
                "process_tree": ShutdownProcessTreeState.NOT_APPLICABLE,
                "cancel": (
                    ShutdownCancelState.SIGNALLED
                    if cancelled
                    else ShutdownCancelState.NOT_NEEDED
                ),
                "active_before": active,
                "unresolved": 0,
            }

        if component == "tool_gateway" and isinstance(result, Mapping):
            handlers = max(0, int(result.get("active_handlers", 0)))
            joined = result.get("owner_joined") is True
            return {
                "effect": (
                    ShutdownEffectState.SETTLED
                    if joined
                    else ShutdownEffectState.UNCERTAIN
                ),
                "owner": (
                    ShutdownOwnerState.JOINED
                    if joined
                    else ShutdownOwnerState.ESCAPED
                ),
                "process_tree": ShutdownProcessTreeState.NOT_APPLICABLE,
                "cancel": ShutdownCancelState.SIGNALLED,
                "active_before": 1,
                "unresolved": 0 if joined else max(1, handlers),
                "error_code": None if joined else "gateway_owner_exit_unproven",
            }

        if component in {"publication_transactions", "publication_watchdog"} and isinstance(
            result,
            Mapping,
        ):
            active = max(0, int(result.get("active_before", 0)))
            unresolved = max(0, int(result.get("unresolved", 0)))
            joined = result.get("owner_joined") is True and unresolved == 0
            return {
                "effect": (
                    ShutdownEffectState.NOT_STARTED
                    if active == 0
                    else (
                        ShutdownEffectState.SETTLED
                        if joined
                        else ShutdownEffectState.UNCERTAIN
                    )
                ),
                "owner": (
                    ShutdownOwnerState.JOINED
                    if joined
                    else ShutdownOwnerState.ESCAPED
                ),
                "process_tree": ShutdownProcessTreeState.NOT_APPLICABLE,
                "cancel": ShutdownCancelState.NOT_NEEDED,
                "active_before": active,
                "unresolved": 0 if joined else max(1, unresolved),
                "error_code": (
                    None
                    if joined
                    else (
                        "publication_watchdog_owner_unresolved"
                        if component == "publication_watchdog"
                        else "publication_transaction_owner_unresolved"
                    )
                ),
            }

        if component == "recovery_owners" and isinstance(result, Mapping):
            active = max(0, int(result.get("active_before", 0)))
            unresolved = max(0, int(result.get("unresolved", 0)))
            joined = result.get("owner_joined") is True and unresolved == 0
            return {
                # This component proves only physical recovery-thread exit.
                # Durable success/failure is reported by causal_recovery and
                # its per-domain children, never inferred from thread return.
                "effect": (
                    ShutdownEffectState.NOT_STARTED
                    if active == 0
                    else (
                        ShutdownEffectState.SETTLED
                        if joined
                        else ShutdownEffectState.UNCERTAIN
                    )
                ),
                "owner": (
                    ShutdownOwnerState.JOINED
                    if joined
                    else ShutdownOwnerState.ESCAPED
                ),
                "process_tree": ShutdownProcessTreeState.NOT_APPLICABLE,
                "cancel": ShutdownCancelState.NOT_NEEDED,
                "active_before": active,
                "unresolved": 0 if joined else max(1, unresolved),
                "error_code": (
                    None if joined else "recovery_owner_exit_unproven"
                ),
            }

        if component == "harness" and type(result) is dict:
            active = max(
                0,
                int(result.get("active_before", result.get("sessions_observed", 0))),
            )
            sessions = max(0, int(result.get("sessions_observed", active)))
            sealed = max(0, int(result.get("continuity_writers_sealed", 0)))
            unresolved = max(0, int(result.get("unresolved", 0)))
            unresolved += max(0, sessions - sealed)
            joined = result.get("owner_joined") is True and unresolved == 0
            tree = {
                "not_applicable": ShutdownProcessTreeState.NOT_APPLICABLE,
                "empty_verified": ShutdownProcessTreeState.EMPTY_VERIFIED,
                "root_exit_only": ShutdownProcessTreeState.ROOT_EXIT_ONLY,
                "unknown": ShutdownProcessTreeState.UNKNOWN,
            }.get(
                result.get("process_tree_state"),
                ShutdownProcessTreeState.UNKNOWN,
            )
            return {
                "effect": (
                    ShutdownEffectState.NOT_STARTED
                    if active == 0
                    else (
                        ShutdownEffectState.SETTLED
                        if joined
                        else ShutdownEffectState.UNCERTAIN
                    )
                ),
                "owner": (
                    ShutdownOwnerState.JOINED
                    if joined
                    else ShutdownOwnerState.ESCAPED
                ),
                "process_tree": tree,
                "cancel": (
                    ShutdownCancelState.SIGNALLED
                    if result.get("cancel_signalled") is True or active
                    else ShutdownCancelState.NOT_NEEDED
                ),
                "active_before": active,
                "unresolved": 0 if joined else max(1, unresolved),
                "error_code": (
                    None
                    if joined and tree is not ShutdownProcessTreeState.UNKNOWN
                    else "pi_harness_exit_unproven"
                ),
            }

        if component == "terminal_sessions":
            if hasattr(result, "to_wire"):
                result = result.to_wire()
            if not isinstance(result, Mapping):
                return {}
            active = max(0, int(result.get("active_before", 0)))
            unresolved = max(0, int(result.get("unresolved", 0)))
            joined = result.get("owner_joined") is True and unresolved == 0
            tree = {
                "not_applicable": ShutdownProcessTreeState.NOT_APPLICABLE,
                "empty_verified": ShutdownProcessTreeState.EMPTY_VERIFIED,
                "root_exit_only": ShutdownProcessTreeState.ROOT_EXIT_ONLY,
                "unknown": ShutdownProcessTreeState.UNKNOWN,
            }.get(
                result.get("process_tree_state"),
                ShutdownProcessTreeState.UNKNOWN,
            )
            return {
                "effect": (
                    ShutdownEffectState.NOT_STARTED
                    if active == 0
                    else (
                        ShutdownEffectState.UNCERTAIN
                        if not joined
                        else ShutdownEffectState.SETTLED
                    )
                ),
                "owner": (
                    ShutdownOwnerState.JOINED
                    if joined
                    else ShutdownOwnerState.ESCAPED
                ),
                "process_tree": tree,
                "cancel": (
                    ShutdownCancelState.NOT_NEEDED
                    if active == 0
                    else ShutdownCancelState.SIGNALLED
                ),
                "active_before": active,
                "unresolved": 0 if joined else max(1, unresolved),
                "error_code": (
                    None
                    if joined and tree is not ShutdownProcessTreeState.UNKNOWN
                    else "terminal_exit_unproven"
                ),
            }

        if component == "harness_actions" and isinstance(result, Mapping):
            active = len(result.get("action_request_ids", ()))
            uncertain = max(0, int(result.get("uncertain", 0)))
            return {
                "effect": (
                    ShutdownEffectState.NOT_STARTED
                    if active == 0
                    else (
                        ShutdownEffectState.UNCERTAIN
                        if uncertain
                        else ShutdownEffectState.SETTLED
                    )
                ),
                "owner": ShutdownOwnerState.JOINED,
                "process_tree": (
                    ShutdownProcessTreeState.NOT_APPLICABLE
                    if active == 0
                    else ShutdownProcessTreeState.UNKNOWN
                ),
                "cancel": (
                    ShutdownCancelState.NOT_NEEDED
                    if active == 0
                    else ShutdownCancelState.SIGNALLED
                ),
                "active_before": active,
                "unresolved": uncertain,
                "error_code": (
                    "sandbox_cancellation_uncertain" if uncertain else None
                ),
            }

        if component == "mcp_runtime" and isinstance(result, Mapping):
            active = max(0, int(result.get("active_before", 0)))
            unresolved = max(0, int(result.get("unresolved", 0)))
            transports = max(0, int(result.get("transports_observed", 0)))
            joined = result.get("owner_joined") is True and unresolved == 0
            tree = {
                "not_applicable": ShutdownProcessTreeState.NOT_APPLICABLE,
                "empty_verified": ShutdownProcessTreeState.EMPTY_VERIFIED,
                "root_exit_only": ShutdownProcessTreeState.ROOT_EXIT_ONLY,
                "unknown": ShutdownProcessTreeState.UNKNOWN,
            }.get(
                result.get("process_tree_state"),
                (
                    ShutdownProcessTreeState.NOT_APPLICABLE
                    if transports == 0
                    else ShutdownProcessTreeState.UNKNOWN
                ),
            )
            return {
                "effect": (
                    ShutdownEffectState.NOT_STARTED
                    if active == 0 and transports == 0
                    else (
                        ShutdownEffectState.SETTLED
                        if joined
                        else ShutdownEffectState.UNCERTAIN
                    )
                ),
                "owner": (
                    ShutdownOwnerState.JOINED
                    if joined
                    else ShutdownOwnerState.ESCAPED
                ),
                "process_tree": tree,
                "cancel": (
                    ShutdownCancelState.NOT_NEEDED
                    if active == 0 and transports == 0
                    else ShutdownCancelState.SIGNALLED
                ),
                "active_before": active,
                "unresolved": 0 if joined else max(1, unresolved),
                "error_code": (
                    "mcp_owner_exit_unproven"
                    if not joined or tree is ShutdownProcessTreeState.UNKNOWN
                    else (
                        "mcp_descendant_exit_unproven"
                        if tree is ShutdownProcessTreeState.ROOT_EXIT_ONLY
                        else None
                    )
                ),
            }

        if component == "task_workers" and isinstance(result, Mapping):
            active = max(
                0,
                int(result.get("active_before", result.get("workers_observed", 0))),
            )
            unresolved = max(0, int(result.get("unresolved", 0)))
            uncertain = max(
                0,
                int(result.get("spawn_operations_settled_uncertain", 0)),
            )
            joined = result.get("owner_joined") is True and unresolved == 0
            tree = {
                "not_applicable": ShutdownProcessTreeState.NOT_APPLICABLE,
                "empty_verified": ShutdownProcessTreeState.EMPTY_VERIFIED,
                "root_exit_only": ShutdownProcessTreeState.ROOT_EXIT_ONLY,
                "unknown": ShutdownProcessTreeState.UNKNOWN,
            }.get(
                result.get("process_tree_state"),
                (
                    ShutdownProcessTreeState.NOT_APPLICABLE
                    if active == 0
                    else ShutdownProcessTreeState.UNKNOWN
                ),
            )
            return {
                "effect": (
                    ShutdownEffectState.NOT_STARTED
                    if active == 0
                    else (
                        ShutdownEffectState.UNCERTAIN
                        if uncertain or not joined
                        else ShutdownEffectState.SETTLED
                    )
                ),
                "owner": (
                    ShutdownOwnerState.JOINED
                    if joined
                    else ShutdownOwnerState.ESCAPED
                ),
                "process_tree": tree,
                "cancel": (
                    ShutdownCancelState.NOT_NEEDED
                    if active == 0
                    else ShutdownCancelState.SIGNALLED
                ),
                "active_before": active,
                "unresolved": 0 if joined else max(1, unresolved, active),
                "error_code": (
                    None
                    if joined and tree is not ShutdownProcessTreeState.UNKNOWN
                    else "task_worker_owner_exit_unproven"
                ),
            }

        if component == "causal_recovery" and isinstance(result, Mapping):
            harness = result.get("harness_operations", {})
            unresolved = max(
                0,
                int(
                    result.get(
                        "unresolved_total",
                        (
                            harness.get("unresolved", 0)
                            if isinstance(harness, Mapping)
                            else 1
                        ),
                    )
                ),
            )
            domains = result.get("domains", {})
            recovered = 0
            if isinstance(domains, Mapping):
                recovered = sum(
                    max(0, int(item.get("recovered", 0)))
                    for item in domains.values()
                    if isinstance(item, Mapping)
                )
            return {
                "effect": (
                    ShutdownEffectState.SETTLED
                    if unresolved == 0
                    else ShutdownEffectState.UNCERTAIN
                ),
                "owner": ShutdownOwnerState.JOINED,
                "process_tree": ShutdownProcessTreeState.NOT_APPLICABLE,
                "cancel": ShutdownCancelState.NOT_NEEDED,
                "active_before": recovered,
                "unresolved": unresolved,
                "error_code": (
                    None
                    if unresolved == 0
                    else "durable_domain_recovery_incomplete"
                ),
            }

        if component == "sandbox_processes" and isinstance(result, Mapping):
            active = max(0, int(result.get("active_before", 0)))
            observed = max(0, int(result.get("observed_processes", 0)))
            unresolved = max(0, int(result.get("unresolved", 0)))
            raw_tree = result.get("process_tree_state")
            tree = {
                "not_applicable": ShutdownProcessTreeState.NOT_APPLICABLE,
                "root_exit_only": ShutdownProcessTreeState.ROOT_EXIT_ONLY,
                "unknown": ShutdownProcessTreeState.UNKNOWN,
            }.get(raw_tree, ShutdownProcessTreeState.UNKNOWN)
            joined = result.get("owner_joined") is True and unresolved == 0
            return {
                "effect": (
                    ShutdownEffectState.NOT_STARTED
                    if observed == 0
                    else (
                        ShutdownEffectState.SETTLED
                        if joined and tree is ShutdownProcessTreeState.ROOT_EXIT_ONLY
                        else ShutdownEffectState.UNCERTAIN
                    )
                ),
                "owner": (
                    ShutdownOwnerState.JOINED
                    if joined
                    else ShutdownOwnerState.ESCAPED
                ),
                "process_tree": tree,
                "cancel": (
                    ShutdownCancelState.SIGNALLED
                    if active
                    else ShutdownCancelState.NOT_NEEDED
                ),
                "active_before": active,
                "unresolved": 0 if joined else max(1, unresolved),
                "error_code": (
                    None
                    if joined and tree is not ShutdownProcessTreeState.UNKNOWN
                    else "sandbox_process_tree_unresolved"
                ),
            }
        return {}

    @staticmethod
    def _shutdown_recovery_domain_reports(
        task: Mapping[str, Any],
    ) -> tuple[ShutdownComponentReport, ...]:
        """Project each durable recovery domain into canonical evidence."""

        result = task.get("state", {}).get("result")
        domains = result.get("domains", {}) if isinstance(result, Mapping) else {}
        reports: list[ShutdownComponentReport] = []
        for name in _SHUTDOWN_RECOVERY_DOMAINS:
            raw = domains.get(name, {}) if isinstance(domains, Mapping) else {}
            unresolved = (
                max(0, int(raw.get("unresolved", 0)))
                if isinstance(raw, Mapping) and name in domains
                else 1
            )
            recovered = (
                max(0, int(raw.get("recovered", 0)))
                if isinstance(raw, Mapping)
                else 0
            )
            attempted = (
                max(0, int(raw.get("attempted", recovered)))
                if isinstance(raw, Mapping)
                else recovered
            )
            reports.append(
                component_report(
                    f"recovery_{name}",
                    effect=(
                        ShutdownEffectState.SETTLED
                        if unresolved == 0
                        else ShutdownEffectState.UNCERTAIN
                    ),
                    owner=ShutdownOwnerState.JOINED,
                    process_tree=ShutdownProcessTreeState.NOT_APPLICABLE,
                    cancel=ShutdownCancelState.NOT_NEEDED,
                    started_at=task["started_at"],
                    started_monotonic=task["started_monotonic"],
                    active_before=max(recovered, attempted),
                    unresolved=unresolved,
                    error_code=(
                        None
                        if unresolved == 0
                        else "durable_domain_recovery_incomplete"
                    ),
                )
            )
        return tuple(reports)

    @staticmethod
    def _task_worker_close_evidence(*results: Any) -> dict[str, Any]:
        """Combine bridge/backend owner evidence without assuming aliasing."""

        rows: list[Mapping[str, Any]] = []
        for result in results:
            if result is None:
                continue
            if hasattr(result, "to_dict"):
                result = result.to_dict()
            if isinstance(result, Mapping):
                rows.append(result)
        if not rows:
            return {
                "active_before": 0,
                "workers_observed": 0,
                "unresolved": 0,
                "owner_joined": True,
                "process_tree_state": "not_applicable",
                "spawn_operations_settled_uncertain": 0,
            }
        tree_rank = {
            "not_applicable": 0,
            "empty_verified": 1,
            "root_exit_only": 2,
            "unknown": 3,
        }
        tree = max(
            (
                str(row.get("process_tree_state", "unknown"))
                for row in rows
            ),
            key=lambda value: tree_rank.get(value, 3),
        )
        active = max(
            max(
                0,
                int(row.get("active_before", row.get("workers_observed", 0))),
            )
            for row in rows
        )
        return {
            "active_before": active,
            "workers_observed": active,
            "unresolved": sum(
                max(0, int(row.get("unresolved", 0))) for row in rows
            ),
            "owner_joined": all(row.get("owner_joined") is True for row in rows),
            "process_tree_state": tree,
            "cancellation_requested": sum(
                max(0, int(row.get("cancellation_requested", 0)))
                for row in rows
            ),
            "terminal_observed": sum(
                max(0, int(row.get("terminal_observed", 0)))
                for row in rows
            ),
            "spawn_operations_settled_uncertain": sum(
                max(
                    0,
                    int(row.get("spawn_operations_settled_uncertain", 0)),
                )
                for row in rows
            ),
        }

    def _recover_shutdown_durable_state(self) -> dict[str, Any]:
        """Serialize every Runtime-owned shutdown recovery pass."""

        with self._recovery_run_lock:
            return self._recover_shutdown_durable_state_owned()

    def _recover_shutdown_durable_state_owned(self) -> dict[str, Any]:
        """Use the recovery-only capability after publication revocation."""

        fence = self._current_recovery_fence()
        recovery_permit = fence.permit
        if not isinstance(recovery_permit, RuntimeRecoveryPermit):
            raise RuntimeError("shutdown recovery permit is unavailable")
        shutdown_builder = self._shutdown_builder
        recovery_deadline = (
            shutdown_builder.deadline.deadline_monotonic
            if shutdown_builder is not None
            else time.monotonic()
            + self._config.runtime_shutdown_timeout_sec
        )
        domains: dict[str, dict[str, Any]] = {}
        causal = None
        try:
            causal = self._causal_ledger.recover_inflight(runtime_fence=fence)
            domains["causal"] = {
                "recovered": (
                    len(causal.turn_ids)
                    + len(causal.event_ids)
                    + len(causal.effect_ids)
                    + len(causal.generation_ids)
                ),
                "unresolved": 0,
            }
        except Exception as exc:  # noqa: BLE001 - domain evidence continues
            _logger.exception("Shutdown causal recovery failed")
            domains["causal"] = {
                "recovered": 0,
                "unresolved": 1,
                "error_code": type(exc).__name__.casefold(),
            }

        try:
            reservations = self._storage.recover_owned_center_reservations_for_shutdown(
                fence.owner_id,
                fence.epoch,
                now=_now(),
                recovery_permit=recovery_permit,
            )
            domains["center_reservations"] = {
                "recovered": len(reservations),
                "unresolved": 0,
            }
        except Exception as exc:  # noqa: BLE001
            _logger.exception("Shutdown center reservation recovery failed")
            domains["center_reservations"] = {
                "recovered": 0,
                "unresolved": 1,
                "error_code": type(exc).__name__.casefold(),
            }

        role_store = self._role_lease_store
        if role_store is None:
            domains["role_leases"] = {"recovered": 0, "unresolved": 0}
        else:
            try:
                role_result = role_store.recover_runtime_shutdown(
                    RoleRuntimeLeaseProof(
                        world_id=self._world_id,
                        owner_id=fence.owner_id,
                        epoch=fence.epoch,
                    ),
                    recovery_permit=recovery_permit,
                )
                domains["role_leases"] = {
                    "recovered": (
                        len(role_result["suspended"])
                        + len(role_result["revoked"])
                    ),
                    "unresolved": 0,
                }
            except Exception as exc:  # noqa: BLE001
                _logger.exception("Shutdown role lease recovery failed")
                domains["role_leases"] = {
                    "recovered": 0,
                    "unresolved": 1,
                    "error_code": type(exc).__name__.casefold(),
                }

        external_attempted = 0
        external_recovered = 0
        external_unresolved = 0
        external_components: dict[str, dict[str, int]] = {}
        for component_name, adapter in (
            ("habitat", getattr(self, "_habitat", None)),
            ("library", getattr(self, "_library", None)),
            (
                "workspace",
                getattr(self, "_harness_workspace_backend", None),
            ),
        ):
            if adapter is None:
                continue
            try:
                snapshot = adapter.recovery_snapshot()
                if not isinstance(snapshot, Mapping):
                    raise TypeError("external recovery snapshot must be a mapping")
                attempted_count = max(0, int(snapshot.get("attempted", 0)))
                unresolved_count = max(0, int(snapshot.get("unresolved", 0)))
                recovered_count = max(0, attempted_count - unresolved_count)
                external_components[component_name] = {
                    "attempted": attempted_count,
                    "recovered": recovered_count,
                    "unresolved": unresolved_count,
                }
                external_attempted += attempted_count
                external_recovered += recovered_count
                external_unresolved += unresolved_count
            except Exception as exc:  # noqa: BLE001 - aggregate honest evidence
                _logger.exception(
                    "Shutdown external-effect snapshot failed for %s",
                    component_name,
                )
                external_components[component_name] = {
                    "attempted": 0,
                    "recovered": 0,
                    "unresolved": 1,
                }
                external_unresolved += 1
        domains["external_effects"] = {
            "attempted": external_attempted,
            "recovered": external_recovered,
            "unresolved": external_unresolved,
            "components": external_components,
        }

        terminal_recovery_summary: Mapping[str, Any] | None = None
        terminal_sessions = self._harness_terminal_sessions
        if terminal_sessions is not None:
            try:
                terminal_result = terminal_sessions.close(
                    deadline=recovery_deadline,
                    recovery_permit=recovery_permit,
                )
                if hasattr(terminal_result, "to_wire"):
                    terminal_result = terminal_result.to_wire()
                if isinstance(terminal_result, Mapping):
                    terminal_recovery_summary = terminal_result
            except Exception:  # noqa: BLE001 - store scan below remains evidence
                _logger.exception("Shutdown terminal-session recovery failed")

        worker_recovery_summary: Mapping[str, Any] | None = None
        worker_bridge = self._harness_task_worker_bridge
        if worker_bridge is not None:
            try:
                worker_result = worker_bridge.close(
                    reason="runtime_shutdown",
                    deadline=recovery_deadline,
                    recovery_permit=recovery_permit,
                )
                if hasattr(worker_result, "to_dict"):
                    worker_result = worker_result.to_dict()
                if isinstance(worker_result, Mapping):
                    worker_recovery_summary = worker_result
            except Exception:  # noqa: BLE001 - ledger recovery below remains evidence
                _logger.exception("Shutdown task-worker recovery failed")

        ledger = self._harness_operation_ledger
        store = self._harness_event_store
        attempted = 0
        recovered = 0
        worker_attempted = 0
        worker_recovered = 0
        remaining_current: list[Any] = []
        if ledger is not None and store is not None:
            operations = ledger.list_recovery(limit=500)
            for operation in operations:
                if (
                    operation.owner_id != fence.owner_id
                    or operation.requested_epoch != fence.epoch
                ):
                    continue
                attempted += 1
                is_worker_operation = operation.operation_kind.startswith("worker.")
                if is_worker_operation:
                    worker_attempted += 1
                terminal_state = operation.terminal_state
                if terminal_state is None:
                    terminal_state = (
                        OperationTerminalState.CANCELLED_NOT_STARTED
                        if operation.phase
                        in {
                            OperationPhase.INTENT,
                            OperationPhase.ADMITTED,
                            OperationPhase.APPROVAL_PENDING,
                            OperationPhase.STARTING,
                        }
                        else OperationTerminalState.UNCERTAIN
                    )
                uncertain = terminal_state is OperationTerminalState.UNCERTAIN
                try:
                    _event, winner = store.recover_terminal_operation(
                        HarnessEventDraft(
                            turn_id=operation.turn_id,
                            world_id=operation.world_id,
                            engram_id=operation.engram_id,
                            kind=HarnessEventKind.TOOL_COMPLETED,
                            phase=HarnessEventPhase.TERMINAL,
                            source=HarnessEventSource.PULSE_CONTROL,
                            status=(
                                HarnessEventStatus.UNCERTAIN
                                if uncertain
                                else HarnessEventStatus.CANCELLED
                            ),
                            payload={
                                "action_request_id": operation.operation_id,
                                "tool_name": operation.operation_kind[:64],
                                "epoch": operation.requested_epoch,
                                "execution_status": (
                                    "uncertain" if uncertain else "cancelled"
                                ),
                                "recovery_state": "recovered",
                                "error_code": (
                                    "runtime_shutdown_after_adapter_boundary"
                                    if uncertain
                                    else "runtime_shutdown_before_adapter_boundary"
                                ),
                                "evidence_class": "LIVE_GATE_UNVERIFIED",
                            },
                            event_id=deterministic_terminal_event_id(
                                operation.operation_kind,
                                operation.operation_id,
                            ),
                        ),
                        ledger=ledger,
                        recovery_permit=recovery_permit,
                        operation_kind=operation.operation_kind,
                        operation_id=operation.operation_id,
                        expected_epoch=operation.requested_epoch,
                        owner_id=operation.owner_id,
                        terminal_state=terminal_state,
                    )
                    if (
                        winner.is_terminal
                        and winner.terminal_event_id is not None
                        and winner.recovery_state is OperationRecoveryState.CLEARED
                    ):
                        recovered += 1
                        if is_worker_operation:
                            worker_recovered += 1
                except Exception:  # noqa: BLE001 - counted as unresolved evidence
                    _logger.exception(
                        "Shutdown Harness operation recovery failed for %s/%s",
                        operation.operation_kind,
                        operation.operation_id,
                    )
            remaining_current = [
                operation
                for operation in ledger.list_recovery(limit=500)
                if operation.owner_id == fence.owner_id
                and operation.requested_epoch == fence.epoch
            ]
        domains["harness_operations"] = {
            "attempted": attempted,
            "recovered": recovered,
            "unresolved": len(remaining_current),
            "truncated": len(remaining_current) == 500,
        }
        remaining_workers = [
            operation
            for operation in remaining_current
            if operation.operation_kind.startswith("worker.")
        ]
        worker_summary_attempted = 0
        worker_summary_recovered = 0
        worker_summary_unresolved = 0
        if worker_recovery_summary is not None:
            worker_summary_attempted = max(
                0,
                int(worker_recovery_summary.get("active_before", 0)),
            )
            worker_summary_unresolved = max(
                0,
                int(worker_recovery_summary.get("unresolved", 0)),
            )
            worker_summary_recovered = max(
                0,
                int(worker_recovery_summary.get("terminal_observed", 0)),
            )
        domains["task_workers"] = {
            "attempted": max(worker_attempted, worker_summary_attempted),
            "recovered": max(worker_recovered, worker_summary_recovered),
            "unresolved": max(
                len(remaining_workers),
                worker_summary_unresolved,
            ),
            "truncated": len(remaining_workers) == 500,
        }

        terminal_store = self._harness_terminal_store
        terminal_scan_unresolved = 0
        terminal_scan_truncated = False
        if terminal_store is not None:
            active = terminal_store.active_for_scope(world_id=self._world_id)
            pending = terminal_store.pending_projection_ids(
                world_id=self._world_id,
                owner_id=fence.owner_id,
                epoch=fence.epoch,
                limit=500,
            )
            terminal_scan_unresolved = len(active) + len(pending)
            terminal_scan_truncated = len(pending) == 500
        if terminal_recovery_summary is not None:
            terminal_attempted = max(
                0,
                int(terminal_recovery_summary.get("active_before", 0)),
            )
            terminal_unresolved = max(
                0,
                int(terminal_recovery_summary.get("unresolved", 0)),
            )
            domains["terminal_sessions"] = {
                "attempted": terminal_attempted,
                "recovered": max(
                    0,
                    int(terminal_recovery_summary.get("terminal_observed", 0)),
                ),
                "unresolved": max(
                    terminal_unresolved,
                    terminal_scan_unresolved,
                ),
                "truncated": terminal_scan_truncated,
            }
        elif terminal_store is None and terminal_sessions is None:
            domains["terminal_sessions"] = {"recovered": 0, "unresolved": 0}
        elif terminal_store is None:
            domains["terminal_sessions"] = {
                "recovered": 0,
                "unresolved": 1,
                "error_code": "terminal_recovery_summary_unavailable",
            }
        else:
            domains["terminal_sessions"] = {
                "recovered": 0,
                "unresolved": terminal_scan_unresolved,
                "truncated": terminal_scan_truncated,
            }

        harness = getattr(self, "_harness", None)
        if (
            getattr(self, "_harness_kind", None) != _HARNESS_KIND_PI
            or type(harness) is not PiHarnessRuntime
        ):
            domains["pi_continuity"] = {
                "attempted": 0,
                "recovered": 0,
                "unresolved": 0,
            }
        else:
            try:
                continuity = harness.close(
                    timeout_sec=self._config.runtime_shutdown_timeout_sec,
                    recovery_permit=recovery_permit,
                )
                sessions = max(
                    0,
                    int(
                        continuity.get(
                            "sessions_observed",
                            continuity.get("active_before", 0),
                        )
                    ),
                )
                sealed = max(
                    0,
                    int(continuity.get("continuity_writers_sealed", 0)),
                )
                domains["pi_continuity"] = {
                    "attempted": sessions,
                    "recovered": min(sessions, sealed),
                    "unresolved": max(0, sessions - sealed),
                }
            except Exception as exc:  # noqa: BLE001
                _logger.exception("Shutdown Pi continuity recovery failed")
                domains["pi_continuity"] = {
                    "attempted": 0,
                    "recovered": 0,
                    "unresolved": 1,
                    "error_code": type(exc).__name__.casefold(),
                }

        unresolved_total = sum(
            max(0, int(domain.get("unresolved", 0)))
            for domain in domains.values()
        )
        return {
            "causal": causal,
            "domains": domains,
            "harness_operations": domains["harness_operations"],
            "unresolved_total": unresolved_total,
        }

    def _join_tick_owner(self) -> None:
        with self._tick_lock:
            return

    def _close_shared_state(self) -> None:
        """Close local stores after every possible shared-Storage owner exits."""

        errors = 0
        role_store = self._role_lease_store
        if role_store is not None:
            try:
                role_store.close()
            except Exception:  # noqa: BLE001
                errors += 1
        purpose = self._purpose_governance
        if purpose is not None:
            try:
                purpose.close()
            except Exception:  # noqa: BLE001
                errors += 1
        metrics = self._metrics
        if metrics is not None:
            try:
                metrics.flush()
            except Exception:  # noqa: BLE001
                errors += 1
        try:
            self._storage.close()
        except Exception:  # noqa: BLE001
            errors += 1
        if errors:
            raise RuntimeError("shared_state_close_failed")

    @staticmethod
    def _initial_retained_component_report(
        component: str,
    ) -> ShutdownComponentReport:
        observed_at = _now()
        return component_report(
            component,
            effect=ShutdownEffectState.UNCERTAIN,
            owner=ShutdownOwnerState.ESCAPED,
            process_tree=ShutdownProcessTreeState.UNKNOWN,
            cancel=ShutdownCancelState.SIGNALLED,
            started_at=observed_at,
            started_monotonic=time.monotonic(),
            active_before=1,
            unresolved=1,
            error_code=(
                "pi_harness_exit_unproven"
                if component == "harness"
                else "mcp_owner_exit_unproven"
            ),
        )

    def _register_runtime_physical_sources(
        self,
        *,
        harness: HarnessRuntime | None,
        mcp_runtime: MCPRuntimeService | None,
        reports: Mapping[str, ShutdownComponentReport],
    ) -> None:
        """Reconcile exact adapters with first-winner shutdown evidence."""

        if type(harness) is PiHarnessRuntime:
            harness_report = reports.get("harness")
            if type(harness_report) is not ShutdownComponentReport:
                harness_report = self._initial_retained_component_report(
                    "harness"
                )
            permit = self._recovery_permit
            if type(permit) is not RuntimeRecoveryPermit:
                raise RuntimeError(
                    "Pi convergence cannot start without recovery authority"
                )
            self._physical_convergence.reconcile_pi(
                harness,
                harness_report,
                recovery_permit=permit,
            )

        if type(mcp_runtime) is MCPRuntimeService:
            mcp_report = reports.get("mcp_runtime")
            if type(mcp_report) is not ShutdownComponentReport:
                mcp_report = self._initial_retained_component_report(
                    "mcp_runtime"
                )
            self._physical_convergence.reconcile_mcp(
                mcp_runtime,
                mcp_report,
            )

    def _prepare_runtime_physical_sources(
        self,
        *,
        harness: HarnessRuntime | None,
        mcp_runtime: MCPRuntimeService | None,
    ) -> None:
        """Install passive probes before a deadline may publish retained state.

        Preparation does not call either adapter.  The ordinary shutdown
        flight remains the first close owner; its first-winner report later
        activates private convergence only when physical proof is missing.
        """

        if type(harness) is PiHarnessRuntime:
            permit = self._recovery_permit
            self._physical_convergence.prepare_pi(
                harness,
                self._initial_retained_component_report("harness"),
                recovery_permit=(
                    permit
                    if type(permit) is RuntimeRecoveryPermit
                    else None
                ),
            )
        if type(mcp_runtime) is MCPRuntimeService:
            self._physical_convergence.prepare_mcp(
                mcp_runtime,
                self._initial_retained_component_report("mcp_runtime"),
            )

    def _start_storage_finalizer(
        self,
        task_specs: tuple[
            tuple[Mapping[str, Any], ShutdownProcessTreeState], ...
        ],
        expected_components: tuple[str, ...],
        preobserved_reports: tuple[ShutdownComponentReport, ...] = (),
    ) -> None:
        with self._close_lock:
            if self._shutdown_storage_finalizer_started:
                return
            self._shutdown_storage_finalizer_started = True

        def finalize() -> None:
            for task, _tree_state in task_specs:
                task["done"].wait()
            engine = getattr(self, "_engine", None)
            engine_drained = (
                True if engine is None else engine.wait_for_shutdown_drain()
            )
            reports: dict[str, ShutdownComponentReport] = {
                report.component: report
                for report in preobserved_reports
                if type(report) is ShutdownComponentReport
            }
            for task, tree_state in task_specs:
                report = self._observe_shutdown_call(
                    task,
                    ShutdownDeadline.after(0.05),
                    process_tree=tree_state,
                )
                reports[report.component] = report
                if report.component == "pulse_engine":
                    result = task["state"].get("result")
                    if isinstance(result, tuple):
                        for child in result:
                            if type(child) is ShutdownComponentReport:
                                if (
                                    engine_drained
                                    and child.component
                                    in {"pulse_workers", "succession_workers"}
                                ):
                                    # The public deadline report remains an
                                    # immutable record of the earlier escape.
                                    # This private finalizer observation is a
                                    # later physical-owner proof only; durable
                                    # effects were already classified by the
                                    # recovery_* component reports below.
                                    observed_at = _now()
                                    child = component_report(
                                        child.component,
                                        effect=(
                                            ShutdownEffectState.NOT_STARTED
                                            if child.active_before == 0
                                            else ShutdownEffectState.SETTLED
                                        ),
                                        owner=ShutdownOwnerState.JOINED,
                                        process_tree=(
                                            ShutdownProcessTreeState.NOT_APPLICABLE
                                        ),
                                        cancel=child.cancel,
                                        started_at=observed_at,
                                        started_monotonic=time.monotonic(),
                                        active_before=child.active_before,
                                        unresolved=0,
                                    )
                                reports[child.component] = child
                elif report.component == "causal_recovery":
                    for child in self._shutdown_recovery_domain_reports(task):
                        reports[child.component] = child
            expected = set(expected_components)
            non_lease_expected = expected - {"owner_lease"}
            generation, probes = self._retained_owner_probes.snapshot()
            while True:
                for probe in probes:
                    try:
                        if type(probe) is not RuntimeRetainedOwnerProbe:
                            raise TypeError(
                                "retained-owner probe is not canonical"
                            )
                        report = probe.snapshot()
                        if type(report) is not ShutdownComponentReport:
                            raise TypeError(
                                "retained-owner snapshot is not canonical"
                            )
                        if report.component != probe.component:
                            raise ValueError(
                                "retained-owner snapshot component mismatch"
                            )
                    except Exception:  # noqa: BLE001 - keep exact owner retained
                        _logger.exception(
                            "Retained-owner probe %s failed",
                            probe.component,
                        )
                        continue
                    reports[report.component] = report
                required_physical = non_lease_expected | {
                    probe.component for probe in probes
                }
                if required_physical.issubset(reports) and all(
                    reports[name].physical_exit_proven
                    for name in required_physical
                ):
                    if self._retained_owner_probes.seal_if_unchanged(
                        generation
                    ):
                        break
                    generation, probes = self._retained_owner_probes.snapshot()
                    continue
                _logger.warning(
                    "Deferred Runtime shared-state close retained Storage: "
                    "physical exit is not proven for every component"
                )
                # One Runtime-owned finalizer sleeps on a condition generation.
                # Producers register/publish typed snapshots; observation
                # executes no adapter callback, needs no per-adapter reaper,
                # and performs no polling.
                generation = self._retained_owner_probes.wait_for_change(
                    generation
                )
                generation, probes = self._retained_owner_probes.snapshot()
            if "owner_lease" in expected:
                keeper = getattr(self, "_lease_keeper", None)
                if keeper is None:
                    _logger.warning(
                        "Deferred Runtime close retained Storage: "
                        "owner lease keeper is unavailable"
                    )
                    return
                authority_lost = keeper.health().lost_reason is not None
                try:
                    released = keeper.close(release=not authority_lost)
                except Exception:  # noqa: BLE001
                    _logger.exception(
                        "Deferred Runtime owner lease release failed"
                    )
                    return
                if not authority_lost and released.state.value != "released":
                    _logger.warning(
                        "Deferred Runtime close retained Storage: "
                        "owner lease release is unproven"
                    )
                    return
            try:
                self._close_shared_state()
            except Exception:  # noqa: BLE001 - process exit remains fallback
                _logger.exception("Deferred Runtime shared-state close failed")

        owner = threading.Thread(
            target=finalize,
            name="runtime-storage-finalizer",
            daemon=True,
        )
        with self._close_lock:
            self._shutdown_storage_finalizer_thread = owner
        owner.start()

    def _finish_shutdown_report(
        self,
        builder: ShutdownReportBuilder,
        *,
        deadline_terminalizer: bool = False,
    ) -> RuntimeShutdownReport:
        """Freeze one terminal report even if the coordinator itself faults."""

        with self._close_lock:
            terminal = self._shutdown_controller.terminal
            if terminal is not None:
                self._shutdown_report = terminal
                self._shutdown_done.set()
                return terminal
            if builder.snapshot()["publication_fence"] != "revoked":
                trigger = (
                    self._shutdown_controller.primary_trigger
                    or RuntimeShutdownTrigger.CLOSE
                )
                try:
                    self._revoke_publication(
                        reason=self._shutdown_reason(trigger)
                    )
                    builder.set_publication_fence(
                        ShutdownPublicationFenceState.REVOKED
                    )
                except Exception:  # noqa: BLE001
                    builder.set_publication_fence(
                        ShutdownPublicationFenceState.FAILED
                    )
            snapshot = builder.snapshot()
            if snapshot["durable_recovery"] == "not_attempted":
                builder.set_durable_recovery(
                    ShutdownDurableRecoveryState.TIMED_OUT
                    if builder.deadline.expired
                    else ShutdownDurableRecoveryState.FAILED
                )
            if snapshot["owner_lease"] == "not_attempted":
                keeper = getattr(self, "_lease_keeper", None)
                if self._runtime_lease_lost.is_set() or keeper is None:
                    builder.set_owner_lease(ShutdownOwnerLeaseState.LOST)
                else:
                    builder.set_owner_lease(
                        ShutdownOwnerLeaseState.RELEASE_PENDING
                    )
            if snapshot["storage_state"] == "open":
                builder.set_storage_state(
                    ShutdownStorageState.RETAINED_FOR_ESCAPED_WORKERS
                )
            if builder.phase.value not in {"cleaning", "fenced"}:
                try:
                    builder.advance(ShutdownPhase.CLEANING)
                except ValueError:
                    pass
            owner_claim = self._shutdown_owner_claim
            if owner_claim is None:  # pragma: no cover - construction invariant
                raise RuntimeError("shutdown owner claim is unavailable")
        # Publishing is intentionally outside _close_lock. A coordinator that
        # is descheduled at this exact point must not block the pre-authorized
        # deadline terminalizer from materializing the same builder.
        try:
            report = (
                self._shutdown_controller.finish_on_deadline(owner_claim)
                if deadline_terminalizer
                else self._shutdown_controller.finish(owner_claim)
            )
        except RuntimeError as exc:
            if deadline_terminalizer or "deadline terminalizer" not in str(exc):
                raise
            report = self._shutdown_observer.wait_terminal(timeout=0.1)
            if report is None:
                raise
        with self._close_lock:
            self._shutdown_report = report
            self._shutdown_done.set()
        return report

    def _start_shutdown_deadline_terminalizer(
        self,
        claim: RuntimeShutdownClaim,
        builder: ShutdownReportBuilder,
    ) -> None:
        """Arm one pre-authorized deadline publisher; callers never gain it."""

        def terminalize() -> None:
            while self._shutdown_controller.terminal is None:
                remaining = builder.deadline.remaining_seconds()
                if remaining <= 0:
                    break
                report = self._shutdown_observer.wait_terminal(
                    timeout=remaining
                )
                if report is not None:
                    return
            try:
                self._finish_shutdown_report(
                    builder,
                    deadline_terminalizer=True,
                )
            except Exception:  # noqa: BLE001 - owner may have won concurrently
                if self._shutdown_controller.terminal is None:
                    _logger.exception(
                        "Runtime shutdown deadline terminalizer failed"
                    )

        terminalizer = threading.Thread(
            target=terminalize,
            name="runtime-shutdown-terminalizer",
            daemon=True,
        )
        self._shutdown_controller.bind_deadline_terminalizer(
            claim,
            terminalizer,
        )
        with self._close_lock:
            self._shutdown_terminalizer_thread = terminalizer
        terminalizer.start()

    def shutdown_snapshot(self) -> dict[str, Any]:
        """Return lifecycle evidence without touching durable Storage.

        This accessor deliberately depends only on in-memory immutable state,
        so it remains safe after Storage has closed or while an escaped owner
        keeps Storage retained for a deferred finalizer.
        """

        return self._shutdown_observer.snapshot()

    @staticmethod
    def _shutdown_reason(trigger: RuntimeShutdownTrigger) -> str:
        return {
            RuntimeShutdownTrigger.CLOSE: "runtime_close",
            RuntimeShutdownTrigger.STARTUP_FAILURE: "runtime_startup_failed",
            RuntimeShutdownTrigger.LEASE_LOST: "runtime_lease_lost",
        }[trigger]

    def close(self, timeout: float | None = None) -> RuntimeShutdownReport:
        """Close through the one canonical Runtime lifecycle flight."""

        result = self._request_runtime_shutdown(
            RuntimeShutdownTrigger.CLOSE,
            timeout=timeout,
            wait=True,
        )
        if not isinstance(result, RuntimeShutdownReport):  # pragma: no cover
            raise RuntimeError("synchronous Runtime close did not return a report")
        return result

    def _request_runtime_shutdown(
        self,
        trigger: RuntimeShutdownTrigger,
        *,
        timeout: float | None = None,
        wait: bool,
        harness_override: HarnessRuntime | None = None,
    ) -> RuntimeShutdownReport | RuntimeShutdownObserver:
        """Start or join the single shutdown flight for any Runtime trigger."""

        timeout_seconds = (
            self._config.runtime_shutdown_timeout_sec
            if timeout is None
            else timeout
        )
        candidate_deadline = ShutdownDeadline.after(timeout_seconds)
        expected_components = self._shutdown_expected_components(
            harness_override=harness_override,
        )
        construction_pending = (
            trigger is RuntimeShutdownTrigger.LEASE_LOST
            and not self._runtime_construction_done.is_set()
        )
        if construction_pending:
            expected_components = tuple(
                dict.fromkeys((*expected_components, "runtime_construction"))
            )

        with self._close_lock:
            terminal = self._shutdown_controller.terminal
            if terminal is not None:
                self._shutdown_report = terminal
                return terminal
            if (
                trigger is RuntimeShutdownTrigger.CLOSE
                and self._shutdown_controller.primary_trigger is None
                and self.running
            ):
                raise ServiceError(
                    "runtime_running",
                    "synchronous close cannot safely own an active async tick",
                    "await service.stop() before calling service.close()",
                    status=409,
                )
            claim = self._shutdown_controller.begin(
                trigger,
                candidate_deadline,
                expected_components,
            )
            builder = claim.builder if claim.is_owner else self._shutdown_builder
            if builder is None:  # pragma: no cover - service lock invariant
                raise RuntimeError(
                    "shutdown controller did not retain its owner builder"
                )
            deadline = builder.deadline
            self._shutdown_builder = builder
            if claim.is_owner:
                self._shutdown_owner_claim = claim
            self._signal_tick_stop()
            self._quiescing = True
            self._closed = True
            gate = self._publication_gate
            if claim.is_owner and gate is not None:
                gate.arm_deadline(deadline)
            if builder.snapshot()["publication_fence"] == "not_attempted":
                builder.set_publication_fence(
                    ShutdownPublicationFenceState.ACTIVE
                )

        if claim.is_owner:
            # This passive registration must precede the terminalizer.  A
            # zero-length shutdown budget may otherwise publish a retained
            # Pi/MCP component before its private evidence producer exists.
            self._prepare_runtime_physical_sources(
                harness=(
                    getattr(self, "_harness", None)
                    if harness_override is None
                    else harness_override
                ),
                mcp_runtime=getattr(self, "_harness_mcp_service", None),
            )

        if trigger in {
            RuntimeShutdownTrigger.STARTUP_FAILURE,
            RuntimeShutdownTrigger.LEASE_LOST,
        }:
            try:
                self._revoke_publication(reason=self._shutdown_reason(trigger))
                builder.set_publication_fence(
                    ShutdownPublicationFenceState.REVOKED
                )
            except Exception:  # noqa: BLE001 - containment must continue
                _logger.exception(
                    "Runtime publication revocation for %s failed",
                    trigger.value,
                )
                builder.set_publication_fence(
                    ShutdownPublicationFenceState.FAILED
                )

        if claim.is_owner:
            self._start_shutdown_deadline_terminalizer(claim, builder)
            if not wait:
                def run() -> None:
                    self._shutdown_controller.bind_owner(claim)
                    effective_components = expected_components
                    initial_reports: tuple[ShutdownComponentReport, ...] = ()
                    if construction_pending:
                        # Do not spend the shutdown budget waiting for a
                        # constructor blocked in factory or preflight. Classify
                        # its owner now and cancel already attached resources.
                        construction_report = (
                            self._observe_runtime_construction_owner()
                        )
                        initial_reports = (construction_report,)
                        attached = self._shutdown_expected_components()
                        builder.expect_components(attached)
                        effective_components = tuple(
                            dict.fromkeys((*expected_components, *attached))
                        )
                    self._run_shutdown_coordinator(
                        claim,
                        expected_components=effective_components,
                        harness_override=harness_override,
                        initial_reports=initial_reports,
                    )

                owner = threading.Thread(
                    target=run,
                    name="runtime-shutdown-coordinator",
                    daemon=True,
                )
                with self._close_lock:
                    self._shutdown_coordinator_thread = owner
                owner.start()
                return self._shutdown_observer
            self._shutdown_controller.bind_owner(claim)
            return self._run_shutdown_coordinator(
                claim,
                expected_components=expected_components,
                harness_override=harness_override,
            )

        if not wait:
            return self._shutdown_observer
        report = self._shutdown_observer.wait_terminal(
            timeout=deadline.remaining_seconds()
        )
        if report is None:
            # The shutdown deadline itself is never extended. This tiny join
            # window only lets the already-bound coordinator publish the
            # report it materialized at the deadline scheduling boundary.
            report = self._shutdown_observer.wait_terminal(timeout=0.05)
        if report is not None:
            with self._close_lock:
                self._shutdown_report = report
            return report
        raise RuntimeError("shutdown coordinator did not publish terminal state")

    def _run_shutdown_coordinator(
        self,
        claim: RuntimeShutdownClaim,
        *,
        expected_components: tuple[str, ...],
        harness_override: HarnessRuntime | None = None,
        initial_reports: tuple[ShutdownComponentReport, ...] = (),
    ) -> RuntimeShutdownReport:
        builder = claim.builder
        if builder is None:  # pragma: no cover - only owner calls this method
            raise RuntimeError("shutdown owner claim has no builder")
        deadline = builder.deadline
        trigger = claim.primary_trigger

        all_tasks: list[Mapping[str, Any]] = []
        task_specs: list[
            tuple[Mapping[str, Any], ShutdownProcessTreeState]
        ] = []
        observed_reports: dict[str, ShutdownComponentReport] = {}
        durable_component_reports: list[ShutdownComponentReport] = []
        recovery_state = ShutdownDurableRecoveryState.NOT_ATTEMPTED
        active_harness = (
            getattr(self, "_harness", None)
            if harness_override is None
            else harness_override
        )
        mcp_runtime = getattr(self, "_harness_mcp_service", None)

        def record(report: ShutdownComponentReport) -> ShutdownComponentReport:
            winner = builder.record_component(report)
            observed_reports[winner.component] = winner
            return winner

        for initial_report in initial_reports:
            record(initial_report)

        def launch(
            component: str,
            function: Callable[[], Any],
            process_tree: ShutdownProcessTreeState = (
                ShutdownProcessTreeState.NOT_APPLICABLE
            ),
            *,
            result_authority: str | None = None,
        ) -> Mapping[str, Any]:
            task = self._start_shutdown_call(component, function)
            if result_authority is not None:
                task["result_authority"] = result_authority
            all_tasks.append(task)
            task_specs.append((task, process_tree))
            return task

        try:
            builder.advance(ShutdownPhase.SETTLING)
            settle_remaining = deadline.remaining_seconds()
            if settle_remaining > 0.06:
                settle_deadline = ShutdownDeadline.after(
                    max(
                        0.05,
                        min(
                            1.0,
                            deadline.timeout_seconds * 0.2,
                            settle_remaining * 0.5,
                        ),
                    )
                )
            else:
                settle_deadline = deadline

            # Every ordinary execution owner receives its stop signal before
            # the coordinator waits for any one of them.  Recovery and lease
            # release remain ordered because they require revocation and the
            # still-owned epoch respectively.
            prior_recovery_owners = self._snapshot_recovery_owners()
            recovery_owners_task = launch(
                "recovery_owners",
                lambda: self._join_recovery_owners(prior_recovery_owners),
            )
            gateway = getattr(self, "_gateway", None)
            gateway_task = (
                None
                if gateway is None
                else launch("tool_gateway", gateway.close)
            )
            executor = getattr(self, "_executor", None)
            delegation_task = (
                None
                if executor is None
                else launch(
                    "delegation_workers",
                    lambda: executor.shutdown(
                        wait=True,
                        cancel_futures=True,
                    ),
                )
            )

            broker_task = None
            broker = getattr(self, "_harness_action_broker", None)
            if broker is not None:
                def cancel_actions() -> Any:
                    result = broker.cancel_all(
                        reason=self._shutdown_reason(trigger)
                    )
                    result.update(
                        broker.settle_cancellations(
                            timeout_seconds=settle_deadline.remaining_seconds()
                        )
                    )
                    return result

                broker_task = launch("harness_actions", cancel_actions)

            engine = getattr(self, "_engine", None)
            engine_task = None
            runtime_tick_task = None
            if engine is not None:
                engine_task = launch(
                    "pulse_engine",
                    lambda: engine.close(
                        deadline=settle_deadline,
                        abort=(
                            None
                            if active_harness is None
                            else active_harness.abort
                        ),
                    ),
                )
                runtime_tick_task = launch(
                    "runtime_tick",
                    self._join_tick_owner,
                )
            harness_task = None
            if active_harness is not None:
                harness_task = launch(
                    "harness",
                    lambda: self._close_harness_component(
                        active_harness,
                        deadline=deadline,
                        after_publication_revoke=(
                            trigger is not RuntimeShutdownTrigger.CLOSE
                        ),
                    ),
                    (
                        ShutdownProcessTreeState.NOT_APPLICABLE
                        if getattr(self, "_harness_kind", None)
                        == _HARNESS_KIND_MOCK
                        else ShutdownProcessTreeState.UNKNOWN
                    ),
                    result_authority=(
                        "pi_harness_v1"
                        if type(active_harness) is PiHarnessRuntime
                        else None
                    ),
                )

            life_task = None
            life_tools = getattr(self, "_life_tools", None)
            if life_tools is not None:
                def close_life_tools() -> dict[str, Any]:
                    summary = life_tools.close(
                        timeout=deadline.remaining_seconds()
                    )
                    if summary.get("owner_joined") is not True:
                        life_tools.wait_for_shutdown_drain()
                        summary = {
                            **summary,
                            "owner_joined": True,
                            "unresolved": 0,
                        }
                    return summary

                life_task = launch("life_tools", close_life_tools)

            terminal_task = None
            terminal_sessions = getattr(
                self,
                "_harness_terminal_sessions",
                None,
            )
            recovery_permit = (
                self._recovery_permit
                if (
                    trigger is RuntimeShutdownTrigger.STARTUP_FAILURE
                    and type(self._recovery_permit) is RuntimeRecoveryPermit
                )
                else None
            )
            if terminal_sessions is not None:
                terminal_task = launch(
                    "terminal_sessions",
                    lambda: terminal_sessions.close(
                        deadline=deadline.deadline_monotonic,
                        recovery_permit=recovery_permit,
                    ),
                    ShutdownProcessTreeState.UNKNOWN,
                )

            worker_task = None
            worker_bridge = getattr(
                self,
                "_harness_task_worker_bridge",
                None,
            )
            worker_backend = getattr(
                self,
                "_harness_task_worker_backend",
                None,
            )
            if worker_bridge is not None or worker_backend is not None:
                def close_task_worker_fleet() -> dict[str, Any]:
                    bridge_result = None
                    backend_result = None
                    try:
                        if worker_bridge is not None:
                            bridge_result = worker_bridge.close(
                                deadline=deadline.deadline_monotonic,
                                recovery_permit=recovery_permit,
                            )
                    finally:
                        if worker_backend is not None:
                            backend_result = worker_backend.shutdown(
                                deadline=deadline.deadline_monotonic,
                            )
                    return self._task_worker_close_evidence(
                        bridge_result,
                        backend_result,
                    )

                worker_task = launch(
                    "task_workers",
                    close_task_worker_fleet,
                    ShutdownProcessTreeState.UNKNOWN,
                )

            mcp_task = None
            if mcp_runtime is not None:
                mcp_task = launch(
                    "mcp_runtime",
                    lambda: mcp_runtime.close(
                        deadline=deadline.deadline_monotonic,
                    ),
                    ShutdownProcessTreeState.UNKNOWN,
                )

            action_task = None
            action_executor = getattr(
                self,
                "_harness_action_executor",
                None,
            )
            if action_executor is not None:
                action_task = launch(
                    "action_workers",
                    lambda: action_executor.shutdown(
                        wait=True,
                        cancel_futures=True,
                    ),
                )

            sandbox_task = None
            sandbox_backend = getattr(
                self,
                "_harness_sandbox_backend",
                None,
            )
            if sandbox_backend is not None:
                sandbox_task = launch(
                    "sandbox_processes",
                    lambda: sandbox_backend.shutdown_evidence(
                        timeout=deadline.remaining_seconds()
                    ),
                    ShutdownProcessTreeState.UNKNOWN,
                )

            if engine_task is not None:
                engine_wrapper_report = record(
                    self._observe_shutdown_call(engine_task, settle_deadline)
                )
                if (
                    engine_task["done"].is_set()
                    and "error_type" not in engine_task["state"]
                ):
                    engine_result = engine_task["state"].get("result")
                    if isinstance(engine_result, tuple):
                        for report in engine_result:
                            if type(report) is ShutdownComponentReport:
                                record(report)
                if engine_wrapper_report.owner is ShutdownOwnerState.ESCAPED:
                    durable_component_reports.append(engine_wrapper_report)

            if broker_task is not None:
                broker_report = record(
                    self._observe_shutdown_call(
                        broker_task,
                        settle_deadline,
                    )
                )
                durable_component_reports.append(broker_report)

            # All other closers were already broadcast.  Give the whole set
            # the same settle window before revocation so terminal/session
            # ledgers may publish their ordinary final outcome.  This loop
            # does not extend the deadline and does not record premature
            # escaped evidence; the canonical observation happens below.
            for task, _tree_state in task_specs:
                if task is engine_task or task is broker_task:
                    continue
                task["done"].wait(timeout=settle_deadline.remaining_seconds())

            builder.advance(ShutdownPhase.FENCING)
            publication_gate = getattr(self, "_publication_gate", None)
            if publication_gate is None:
                # Construction may fail after lease acquisition but before a
                # publication generation exists. The fence is honestly failed,
                # while lease/Storage cleanup must still continue.
                builder.set_publication_fence(
                    ShutdownPublicationFenceState.FAILED
                )
            else:
                try:
                    self._revoke_publication(reason=self._shutdown_reason(trigger))
                    builder.set_publication_fence(
                        ShutdownPublicationFenceState.REVOKED
                    )
                except Exception:  # noqa: BLE001 - cleanup remains mandatory
                    _logger.exception("Runtime shutdown publication revoke failed")
                    builder.set_publication_fence(
                        ShutdownPublicationFenceState.FAILED
                    )
            builder.advance(ShutdownPhase.FENCED)

            publication_task = None
            publication_watchdog_task = None
            if publication_gate is not None:
                publication_task = self._start_shutdown_call(
                    "publication_transactions",
                    publication_gate.wait_for_publication_drain,
                )
                all_tasks.append(publication_task)
                task_specs.append(
                    (
                        publication_task,
                        ShutdownProcessTreeState.NOT_APPLICABLE,
                    )
                )
                publication_watchdog_task = self._start_shutdown_call(
                    "publication_watchdog",
                    publication_gate.wait_for_watchdog_exit,
                )
                all_tasks.append(publication_watchdog_task)
                task_specs.append(
                    (
                        publication_watchdog_task,
                        ShutdownProcessTreeState.NOT_APPLICABLE,
                    )
                )

            recovery_task = None

            def recovery_authority_lost_now() -> bool:
                return (
                    self._runtime_lease_lost.is_set()
                    or RuntimeShutdownTrigger.LEASE_LOST
                    in self._shutdown_controller.seen_triggers
                )

            def record_recovery_authority_lost() -> None:
                observed_at = _now()
                observed_monotonic = time.monotonic()
                recovery_component_names = {
                    "causal_recovery",
                    *(
                        f"recovery_{domain}"
                        for domain in _SHUTDOWN_RECOVERY_DOMAINS
                    ),
                }
                for component in expected_components:
                    if component not in recovery_component_names:
                        continue
                    record(
                        component_report(
                            component,
                            effect=ShutdownEffectState.NOT_STARTED,
                            owner=ShutdownOwnerState.JOINED,
                            process_tree=(
                                ShutdownProcessTreeState.NOT_APPLICABLE
                            ),
                            cancel=ShutdownCancelState.NOT_NEEDED,
                            started_at=observed_at,
                            started_monotonic=observed_monotonic,
                            unresolved=0,
                            error_code="recovery_authority_lost",
                        )
                    )

            recovery_authority_lost = recovery_authority_lost_now()
            has_recovery_domain = (
                getattr(self, "_causal_ledger", None) is not None
            )
            if has_recovery_domain and not recovery_authority_lost:
                def guarded_recovery() -> Mapping[str, Any]:
                    # The worker rechecks after executor admission. This closes
                    # the check→queue window where a lease-loss callback can
                    # win before the recovery body starts.
                    if recovery_authority_lost_now():
                        return {"authority_lost": True}
                    result = self._recover_shutdown_durable_state()
                    if recovery_authority_lost_now():
                        return {"authority_lost": True}
                    return result

                recovery_task = self._start_shutdown_call(
                    "causal_recovery",
                    guarded_recovery,
                )
                all_tasks.append(recovery_task)
                task_specs.append(
                    (recovery_task, ShutdownProcessTreeState.NOT_APPLICABLE)
                )
                recovery_observation = self._observe_shutdown_call(
                    recovery_task,
                    deadline,
                    cancel=ShutdownCancelState.NOT_NEEDED,
                )
                if not recovery_task["done"].is_set():
                    record(recovery_observation)
                    recovery_state = ShutdownDurableRecoveryState.TIMED_OUT
                elif "error_type" in recovery_task["state"]:
                    record(recovery_observation)
                    recovery_state = ShutdownDurableRecoveryState.FAILED
                else:
                    recovery_result = recovery_task["state"]["result"]
                    recovery_authority_lost = (
                        recovery_authority_lost_now()
                        or recovery_result.get("authority_lost") is True
                    )
                    if recovery_authority_lost:
                        record_recovery_authority_lost()
                        recovery_state = ShutdownDurableRecoveryState.FAILED
                    else:
                        record(recovery_observation)
                        for domain_report in (
                            self._shutdown_recovery_domain_reports(recovery_task)
                        ):
                            record(domain_report)
                        if recovery_result.get("causal") is not None:
                            self._recovery = recovery_result["causal"]
                        recovery_state = (
                            ShutdownDurableRecoveryState.COMPLETED
                            if recovery_result.get("unresolved_total", 1) == 0
                            else ShutdownDurableRecoveryState.FAILED
                        )
            elif has_recovery_domain:
                record_recovery_authority_lost()
                recovery_state = ShutdownDurableRecoveryState.FAILED
            else:
                recovery_state = ShutdownDurableRecoveryState.NOT_NEEDED
            builder.set_durable_recovery(recovery_state)

            builder.advance(ShutdownPhase.CLEANING)
            early_components = {"pulse_engine", "harness_actions"}
            durable_names = {
                "life_tools",
                "terminal_sessions",
                "task_workers",
                "mcp_runtime",
                "action_workers",
                "sandbox_processes",
                "publication_transactions",
            }
            for task, tree_state in task_specs:
                component = str(task["component"])
                if component in early_components:
                    continue
                report = record(
                    self._observe_shutdown_call(
                        task,
                        deadline,
                        process_tree=tree_state,
                    )
                )
                if component in durable_names:
                    durable_component_reports.append(report)

            # Public first-winner component reports are now frozen in the
            # builder. Register any exact unresolved Pi/MCP source before the
            # deferred finalizer can take or seal its probe census. Late proof
            # is private resource-release evidence only.
            self._register_runtime_physical_sources(
                harness=active_harness,
                mcp_runtime=mcp_runtime,
                reports=observed_reports,
            )
            if self._runtime_construction_done.is_set():
                self._physical_convergence.seal_registrations()

            durable_timed_out = any(
                report.owner is ShutdownOwnerState.ESCAPED
                for report in durable_component_reports
            )
            durable_failed = any(
                report.error_code == "shutdown_component_failed"
                for report in durable_component_reports
            )
            if durable_timed_out:
                recovery_state = ShutdownDurableRecoveryState.TIMED_OUT
            elif (
                durable_failed
                and recovery_state is not ShutdownDurableRecoveryState.TIMED_OUT
            ):
                recovery_state = ShutdownDurableRecoveryState.FAILED
            builder.set_durable_recovery(recovery_state)

            recovery_authority_lost = recovery_authority_lost_now()
            keeper = getattr(self, "_lease_keeper", None)
            if keeper is not None:
                expected_before_release = set(expected_components) - {
                    "owner_lease"
                }
                engine_drained = (
                    engine is None
                    or engine.wait_for_shutdown_drain(timeout=0.0)
                )
                safe_to_release = (
                    not recovery_authority_lost
                    and not deadline.expired
                    and all(task["done"].is_set() for task in all_tasks)
                    and engine_drained
                    and expected_before_release.issubset(observed_reports)
                    and all(
                        observed_reports[name].physical_exit_proven
                        for name in expected_before_release
                    )
                )
                if recovery_authority_lost:
                    owner_task = self._start_shutdown_call(
                        "owner_lease",
                        lambda: keeper.close(release=False),
                    )
                    all_tasks.append(owner_task)
                    task_specs.append(
                        (owner_task, ShutdownProcessTreeState.NOT_APPLICABLE)
                    )
                    record(
                        self._observe_shutdown_call(
                            owner_task,
                            deadline,
                            cancel=ShutdownCancelState.NOT_NEEDED,
                        )
                    )
                    builder.set_owner_lease(ShutdownOwnerLeaseState.LOST)
                elif safe_to_release:
                    owner_task = self._start_shutdown_call(
                        "owner_lease",
                        lambda: keeper.close(release=True),
                    )
                    all_tasks.append(owner_task)
                    task_specs.append(
                        (owner_task, ShutdownProcessTreeState.NOT_APPLICABLE)
                    )
                    owner_report = record(
                        self._observe_shutdown_call(
                            owner_task,
                            deadline,
                            cancel=ShutdownCancelState.NOT_NEEDED,
                        )
                    )
                    if not owner_task["done"].is_set():
                        builder.set_owner_lease(
                            ShutdownOwnerLeaseState.RELEASE_PENDING
                        )
                    elif "error_type" in owner_task["state"]:
                        health = keeper.health()
                        builder.set_owner_lease(
                            ShutdownOwnerLeaseState.LOST
                            if not health.healthy
                            else ShutdownOwnerLeaseState.FAILED
                        )
                    else:
                        lease = owner_task["state"]["result"]
                        builder.set_owner_lease(
                            ShutdownOwnerLeaseState.RELEASED
                            if lease.state.value == "released"
                            else ShutdownOwnerLeaseState.LOST
                        )
                else:
                    # Keep a healthy startup owner renewing while any physical
                    # participant remains unresolved. Letting the lease expire
                    # would admit a successor alongside the old owner. The one
                    # deferred finalizer releases this exact epoch later.
                    health = keeper.health()
                    observed_at = _now()
                    observed_monotonic = time.monotonic()
                    record(
                        component_report(
                            "owner_lease",
                            effect=ShutdownEffectState.UNCERTAIN,
                            owner=ShutdownOwnerState.ESCAPED,
                            process_tree=ShutdownProcessTreeState.NOT_APPLICABLE,
                            cancel=ShutdownCancelState.NOT_NEEDED,
                            started_at=observed_at,
                            started_monotonic=observed_monotonic,
                            active_before=1,
                            unresolved=1,
                            error_code=(
                                "owner_lease_retained_for_escaped_work"
                                if health.healthy
                                else "owner_lease_lost"
                            ),
                        )
                    )
                    builder.set_owner_lease(
                        ShutdownOwnerLeaseState.RELEASE_PENDING
                        if health.healthy
                        else ShutdownOwnerLeaseState.LOST
                    )
            else:
                builder.set_owner_lease(ShutdownOwnerLeaseState.LOST)

            shared_owners_done = (
                all(task["done"].is_set() for task in all_tasks)
                and (
                    engine is None
                    or engine.wait_for_shutdown_drain(timeout=0.0)
                )
                and set(expected_components).issubset(observed_reports)
                and all(
                    observed_reports[name].physical_exit_proven
                    for name in expected_components
                )
            )
            if shared_owners_done and not deadline.expired:
                storage_task = self._start_shutdown_call(
                    "shared_storage",
                    self._close_shared_state,
                )
                all_tasks.append(storage_task)
                storage_report = record(
                    self._observe_shutdown_call(
                        storage_task,
                        deadline,
                        cancel=ShutdownCancelState.NOT_NEEDED,
                    )
                )
                if not storage_task["done"].is_set():
                    builder.set_storage_state(
                        ShutdownStorageState.CLOSE_PENDING
                    )
                elif "error_type" in storage_task["state"]:
                    builder.set_storage_state(ShutdownStorageState.FAILED)
                else:
                    builder.set_storage_state(ShutdownStorageState.CLOSED)
            else:
                builder.set_storage_state(
                    ShutdownStorageState.RETAINED_FOR_ESCAPED_WORKERS
                )
                self._start_storage_finalizer(
                    tuple(task_specs),
                    expected_components,
                    tuple(observed_reports.values()),
                )
        except Exception:  # noqa: BLE001 - finish an honest terminal report
            _logger.exception("Runtime shutdown coordinator failed")
            if type(self._recovery_permit) is not RuntimeRecoveryPermit:
                try:
                    self._revoke_publication(
                        reason=self._shutdown_reason(trigger)
                    )
                    builder.set_publication_fence(
                        ShutdownPublicationFenceState.REVOKED
                    )
                except Exception:  # noqa: BLE001 - fail closed below
                    _logger.exception(
                        "Runtime shutdown exception-path revocation failed"
                    )
                    builder.set_publication_fence(
                        ShutdownPublicationFenceState.FAILED
                    )
            if builder.phase.value in {"open", "freezing", "settling"}:
                builder.set_durable_recovery(
                    ShutdownDurableRecoveryState.FAILED
                )
            builder.set_storage_state(
                ShutdownStorageState.RETAINED_FOR_ESCAPED_WORKERS
            )
            try:
                self._register_runtime_physical_sources(
                    harness=active_harness,
                    mcp_runtime=mcp_runtime,
                    reports=observed_reports,
                )
                if self._runtime_construction_done.is_set():
                    self._physical_convergence.seal_registrations()
            except Exception:  # noqa: BLE001 - retain on invariant failure
                _logger.exception(
                    "Runtime physical convergence registration failed"
                )
            self._start_storage_finalizer(
                tuple(task_specs),
                expected_components,
                tuple(observed_reports.values()),
            )
        return self._finish_shutdown_report(builder)

class _HarnessControlError(RuntimeError):
    """Structured sideband refusal for the Harness control gateway."""

    def __init__(
        self,
        code: str,
        detail: str,
        remedy: str,
        *,
        status: int,
        uncertain: bool = False,
    ):
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail
        self.remedy = remedy
        self.status = status
        self.uncertain = uncertain


@dataclass(frozen=True, slots=True)
class _HarnessControlScope:
    world_id: str
    engram_id: str
    turn_id: str
    epoch: int
    operation: str
    request_id: str


class _RuntimeHarnessControlGateway:
    """Fence-aware interrupt/steer adapter for the live RuntimeService.

    The adapter is deliberately narrower than the runtime itself.  It maps a
    durable turn to its owning Engram, checks the current lease epoch and
    terminal state, then sends only Pi's existing sideband command.  It never
    replays a prompt and it never writes control metadata into Engram content.
    """

    _MAX_IDEMPOTENCY = 1024

    def __init__(self, service: "RuntimeService") -> None:
        self._service = service
        self._lock = threading.RLock()
        self._results: dict[_HarnessControlScope, dict[str, Any]] = {}
        self._request_scopes: dict[str, _HarnessControlScope] = {}
        self._inflight: set[_HarnessControlScope] = set()

    def request_control(
        self,
        operation: str,
        turn_id: str,
        request: Mapping[str, Any],
        **_: Any,
    ) -> dict[str, Any]:
        if operation not in {"interrupt", "steer"}:
            raise _HarnessControlError(
                "unsupported_control",
                f"unsupported Harness control operation {operation!r}",
                "use interrupt or steer",
                status=400,
            )
        request_id = self._require_request_id(request)
        turn, epoch = self._scope_turn(turn_id, request)
        scope = _HarnessControlScope(
            world_id=self._service._world_id,
            engram_id=turn.engram_id,
            turn_id=turn_id,
            epoch=epoch,
            operation=operation,
            request_id=request_id,
        )
        prior = self._reserve_control(scope)
        if prior is not None:
            return prior
        try:
            result = self._execute_control(
                operation,
                turn_id,
                request,
                request_id=request_id,
                turn=turn,
                epoch=epoch,
            )
        except _HarnessControlError as exc:
            # CONTROL_REQUESTED is already durable at this point. Every
            # rejected or uncertain attempt therefore receives an explicit
            # terminal partner; otherwise replay would show a permanently
            # running control action after this caller returned.
            result = self._record_control_failure(scope, exc)
            self._remember(scope, result)
            raise
        except BaseException:
            self._release_control(scope)
            raise
        self._remember(scope, result)
        return result

    def _execute_control(
        self,
        operation: str,
        turn_id: str,
        request: Mapping[str, Any],
        *,
        request_id: str,
        turn: Any,
        epoch: int,
    ) -> dict[str, Any]:
        engram_id = turn.engram_id
        self._append_control_event(
            turn_id,
            engram_id,
            HarnessEventKind.CONTROL_REQUESTED,
            HarnessEventStatus.RUNNING,
            {
                "request_id": request_id,
                "operation": operation,
                "expected_state": request.get("expected_state"),
                "expected_epoch": request.get("expected_epoch"),
            },
        )
        try:
            if operation == "interrupt":
                broker = self._service._harness_action_broker
                cancellation = (
                    None
                    if broker is None
                    else broker.cancel_for_turn(
                        engram_id,
                        turn_id,
                        epoch=epoch,
                        reason="turn_interrupt",
                    )
                )
                # The admission fence above is installed before scanning
                # sessions or aborting Pi.  A tool call already crossing the
                # boundary shares the cancelled action token; a later call is
                # rejected by the turn fence.
                terminal_sessions = self._service._harness_terminal_sessions
                stopped_sessions: list[Any] = []
                session_uncertain = False
                if terminal_sessions is not None:
                    try:
                        stopped_sessions.extend(
                            terminal_sessions.stop_turn(
                                turn_id,
                                engram_id=engram_id,
                                reason="turn_interrupt",
                            )
                        )
                    except TerminalSessionError:
                        session_uncertain = True
                worker_bridge = getattr(
                    self._service,
                    "_harness_task_worker_bridge",
                    None,
                )
                if worker_bridge is None:
                    worker_cancellation = {
                        "configured": False,
                        "workers_observed": 0,
                        "stop_accepted": 0,
                        "uncertain": False,
                        "task_ids": [],
                    }
                else:
                    try:
                        worker_cancellation = worker_bridge.stop_turn(
                            engram_id,
                            turn_id,
                            reason="turn authority revoked",
                        )
                    except Exception:
                        worker_cancellation = {
                            "configured": True,
                            "workers_observed": 0,
                            "stop_accepted": 0,
                            "uncertain": True,
                            "task_ids": [],
                        }
                self._service._harness.abort(engram_id)
                if broker is not None and cancellation is not None:
                    action_ids = set(
                        cancellation.get("action_request_ids", [])
                    )
                    settlement = broker.settle_cancellations(
                        timeout_seconds=0.25,
                        action_request_ids=action_ids,
                    )
                    cancellation.update(settlement)
                if terminal_sessions is not None:
                    # A second sweep closes the interval in which a process
                    # was spawned while the first durable session scan was in
                    # progress.  Stop is idempotent for terminal winners.
                    try:
                        stopped_sessions.extend(
                            terminal_sessions.stop_turn(
                                turn_id,
                                engram_id=engram_id,
                                reason="turn_interrupt_reconcile",
                            )
                        )
                    except TerminalSessionError:
                        session_uncertain = True
                    stopped_ids = {
                        result.summary.terminal_session_id
                        for result in stopped_sessions
                    }
                    session_cancellation = {
                        "configured": True,
                        "stopped": len(stopped_ids),
                        "uncertain": session_uncertain or any(
                            result.uncertain for result in stopped_sessions
                        ),
                        "sweeps": 2,
                    }
                else:
                    session_cancellation = {
                        "configured": False,
                        "stopped": 0,
                        "uncertain": False,
                        "sweeps": 0,
                    }
            else:
                cancellation = None
                session_cancellation = None
                worker_cancellation = None
                message = request.get("message")
                if not isinstance(message, str) or not message.strip():
                    raise _HarnessControlError(
                        "invalid_message",
                        "steer requires a natural-language message",
                        "send a non-empty message without structured control metadata",
                        status=400,
                    )
                self._service._harness.steer(engram_id, message)
        except _HarnessControlError:
            raise
        except HarnessError as exc:
            raise _HarnessControlError(
                exc.code,
                exc.detail,
                exc.remedy,
                status=409 if exc.prompt_accepted is not False else 503,
                uncertain=(
                    operation == "interrupt"
                    or exc.code in {"pi_steer_timeout", "pi_connection_lost"}
                ),
            ) from exc
        except Exception as exc:  # noqa: BLE001 - adapter boundary
            raise _HarnessControlError(
                "control_unavailable",
                f"the live Harness rejected {operation}: {type(exc).__name__}",
                "inspect the Harness state and retry only after it is RUNNING",
                status=503,
                # An opaque adapter exception cannot prove whether its
                # side-effect boundary was crossed.
                uncertain=True,
            ) from exc

        result = {
            "request_id": request_id,
            "turn_id": turn_id,
            "accepted": True,
            "state": "accepted",
            "uncertain": operation == "interrupt",
            "evidence_class": self._service._harness_evidence_class(),
        }
        if cancellation is not None:
            result["cancelled_actions"] = cancellation
        if session_cancellation is not None:
            result["terminal_sessions"] = session_cancellation
        if worker_cancellation is not None:
            result["task_workers"] = worker_cancellation
        try:
            event = self._append_control_event(
                turn_id,
                engram_id,
                HarnessEventKind.CONTROL_RESOLVED,
                HarnessEventStatus.UNCERTAIN if result["uncertain"] else HarnessEventStatus.COMPLETED,
                {
                    "request_id": request_id,
                    "operation": operation,
                    "accepted": True,
                    "uncertain": result["uncertain"],
                    **(
                        {}
                        if session_cancellation is None
                        else {
                            "terminal_sessions_stopped": session_cancellation[
                                "stopped"
                            ],
                            "terminal_sessions_uncertain": session_cancellation[
                                "uncertain"
                            ],
                        }
                    ),
                    **(
                        {}
                        if worker_cancellation is None
                        else {
                            "task_workers_stopped": worker_cancellation[
                                "stop_accepted"
                            ],
                            "task_workers_uncertain": worker_cancellation[
                                "uncertain"
                            ],
                        }
                    ),
                },
            )
            result["event_seq"] = event.seq
        except Exception:  # noqa: BLE001 - side effect already happened
            result.update(
                state="uncertain",
                uncertain=True,
                error_code="control_event_persist_failed",
                event_seq=None,
            )
        return result

    def _record_control_failure(
        self,
        scope: _HarnessControlScope,
        error: _HarnessControlError,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "request_id": scope.request_id,
            "turn_id": scope.turn_id,
            "accepted": False,
            "state": "uncertain" if error.uncertain else "rejected",
            "uncertain": error.uncertain,
            "error_code": error.code,
            "evidence_class": self._service._harness_evidence_class(),
        }
        try:
            event = self._append_control_event(
                scope.turn_id,
                scope.engram_id,
                HarnessEventKind.CONTROL_RESOLVED,
                (
                    HarnessEventStatus.UNCERTAIN
                    if error.uncertain
                    else HarnessEventStatus.FAILED
                ),
                {
                    "request_id": scope.request_id,
                    "operation": scope.operation,
                    "accepted": False,
                    "uncertain": error.uncertain,
                    "error_code": error.code,
                },
            )
            result["event_seq"] = event.seq
        except Exception:  # noqa: BLE001 - requested event may already exist
            result.update(
                state="uncertain",
                uncertain=True,
                error_code="control_failure_event_persist_failed",
                cause_error_code=error.code,
                event_seq=None,
            )
        return result

    def resolve_approval(
        self,
        approval_id: str,
        request: Mapping[str, Any],
        **_: Any,
    ) -> dict[str, Any]:
        broker = self._service._harness_action_broker
        if broker is None:
            raise _HarnessControlError(
                "harness_approval_unavailable",
                "the Runtime has no policy-aware Harness action broker",
                "attach the action broker before accepting an approval decision",
                status=503,
            )
        try:
            return broker.resolve_approval(approval_id, request)
        except HarnessActionError as exc:
            raise _HarnessControlError(
                exc.code,
                exc.detail,
                exc.remedy,
                status=exc.status,
            ) from exc

    def _terminal_session_service(self) -> TerminalSessionService:
        service = self._service._harness_terminal_sessions
        if service is None:
            raise _HarnessControlError(
                "terminal_sessions_unavailable",
                "the Runtime has no verified durable PIPE session service",
                "enable the explicit sandbox and owner-death lifecycle gates",
                status=503,
            )
        return service

    @staticmethod
    def _raise_terminal_session_error(exc: TerminalSessionError) -> None:
        if isinstance(exc, TerminalSessionNotFoundError):
            raise _HarnessControlError(
                "terminal_session_not_found",
                "the terminal session is unknown or outside bounded retention",
                "reload the terminal session list for this turn",
                status=404,
            ) from exc
        if isinstance(
            exc,
            (TerminalSessionLeaseError, TerminalSessionConflictError),
        ):
            raise _HarnessControlError(
                "terminal_session_scope_conflict",
                "the Runtime lease, epoch, or terminal session state changed",
                "reload the session and do not retry a side effect automatically",
                status=409,
            ) from exc
        raise _HarnessControlError(
            "terminal_sessions_unavailable",
            "the durable terminal session service rejected the request",
            "inspect Runtime health and retry only after the service is available",
            status=503,
        ) from exc

    def list_terminal_sessions(
        self,
        turn_id: str,
        limit: int = 16,
        **_: Any,
    ) -> dict[str, Any]:
        turn = self._turn_in_current_world(turn_id)
        if turn is None:
            raise _HarnessControlError(
                "turn_not_found",
                f"Harness turn {turn_id!r} was not found",
                "refresh the Workbench turn list",
                status=404,
            )
        try:
            return self._terminal_session_service().list_for_turn(
                turn_id,
                engram_id=turn.engram_id,
                limit=limit,
            ).to_wire()
        except TerminalSessionError as exc:
            self._raise_terminal_session_error(exc)

    def inspect_terminal_session(
        self,
        terminal_session_id: str,
        expected_turn_id: str | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        turn = self._authoritative_terminal_turn(expected_turn_id)
        try:
            return self._terminal_session_service().inspect(
                terminal_session_id,
                expected_engram_id=turn.engram_id,
                expected_turn_id=turn.id,
            ).to_wire()
        except TerminalSessionError as exc:
            self._raise_terminal_session_error(exc)

    def read_terminal_session_output(
        self,
        terminal_session_id: str,
        expected_turn_id: str | None = None,
        after_seq: int = 0,
        limit: int = 200,
        **_: Any,
    ) -> dict[str, Any]:
        turn = self._authoritative_terminal_turn(expected_turn_id)
        try:
            return self._terminal_session_service().read_output(
                terminal_session_id,
                expected_engram_id=turn.engram_id,
                expected_turn_id=turn.id,
                after_seq=after_seq,
                limit=limit,
            ).to_wire()
        except TerminalSessionError as exc:
            self._raise_terminal_session_error(exc)

    def _authoritative_terminal_turn(self, expected_turn_id: Any) -> Any:
        if not isinstance(expected_turn_id, str) or not expected_turn_id.strip():
            raise _HarnessControlError(
                "invalid_turn_id",
                "terminal session reads require the owning turn id",
                "use the turn_id returned by the scoped terminal session list",
                status=400,
            )
        turn = self._turn_in_current_world(expected_turn_id.strip())
        if turn is None:
            raise _HarnessControlError(
                "turn_not_found",
                f"Harness turn {expected_turn_id!r} was not found",
                "refresh the Workbench turn list",
                status=404,
            )
        return turn

    def _turn_in_current_world(self, turn_id: str) -> Any | None:
        return self._service._causal_ledger.get_turn_for_world(
            turn_id,
            self._service._world_id,
        )

    def stop_terminal_session(
        self,
        terminal_session_id: str,
        request: Mapping[str, Any] | None = None,
        request_id: str | None = None,
        expected_epoch: int | None = None,
        expected_turn_id: str | None = None,
        expected_state: str | None = None,
        reason: str | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        payload = dict(request) if isinstance(request, Mapping) else {}
        request_id = payload.get("request_id", request_id)
        expected_epoch = payload.get("expected_epoch", expected_epoch)
        expected_turn_id = payload.get("expected_turn_id", expected_turn_id)
        expected_state = payload.get("expected_state", expected_state)
        reason = payload.get("reason", reason)
        if not isinstance(request_id, str) or not request_id:
            raise _HarnessControlError(
                "invalid_request_id",
                "terminal stop requires a stable request_id",
                "reuse the request id generated for this stop attempt",
                status=400,
            )
        if (
            isinstance(expected_epoch, bool)
            or not isinstance(expected_epoch, int)
            or expected_epoch < 1
        ):
            raise _HarnessControlError(
                "invalid_epoch",
                "terminal stop requires the session epoch",
                "use the epoch returned by terminal session inspection",
                status=400,
            )
        if not isinstance(expected_turn_id, str) or not expected_turn_id:
            raise _HarnessControlError(
                "invalid_turn_id",
                "terminal stop requires the owning turn id",
                "use the turn_id returned by terminal session inspection",
                status=400,
            )
        try:
            terminal_sessions = self._terminal_session_service()
            turn = self._authoritative_terminal_turn(expected_turn_id)
            terminal_sessions.inspect(
                terminal_session_id,
                expected_engram_id=turn.engram_id,
                expected_turn_id=turn.id,
            )
            return terminal_sessions.stop(
                terminal_session_id,
                request_id=request_id,
                expected_epoch=expected_epoch,
                expected_engram_id=turn.engram_id,
                expected_turn_id=expected_turn_id,
                expected_state=expected_state,
                reason=reason or "user_stop",
            ).to_wire()
        except TerminalSessionError as exc:
            self._raise_terminal_session_error(exc)

    def list_checkpoints(self, turn_id: str | None = None, **_: Any) -> list[dict[str, Any]]:
        backend = self._service._harness_workspace_backend
        if backend is None:
            raise _HarnessControlError(
                "checkpoint_unavailable",
                "the Runtime has no checkpoint-first workspace adapter",
                "enable harness_file_mutation_enabled with an external checkpoint root",
                status=503,
            )
        try:
            values = backend.list_checkpoints()
        except Exception as exc:
            raise _HarnessControlError(
                "checkpoint_unavailable",
                f"checkpoint manifests are unreadable: {type(exc).__name__}",
                "repair the protected checkpoint root before restoring",
                status=503,
            ) from exc
        output: list[dict[str, Any]] = []
        for value in values:
            mapping = value if isinstance(value, Mapping) else {}
            if turn_id is not None and mapping.get("turn_id") != turn_id:
                continue
            output.append(dict(mapping))
        return output

    def restore_checkpoint(
        self,
        checkpoint_id: str,
        request_id: str,
        expected_epoch: int,
        changed_paths: tuple[str, ...] = (),
        **_: Any,
    ) -> dict[str, Any]:
        if not isinstance(checkpoint_id, str) or not checkpoint_id.strip():
            raise _HarnessControlError(
                "invalid_checkpoint_id",
                "checkpoint_id must be non-empty",
                "use an id returned by the checkpoint reader",
                status=400,
            )
        if not isinstance(request_id, str) or not request_id.strip():
            raise _HarnessControlError(
                "invalid_request_id",
                "request_id must be non-empty",
                "send a stable id for restore retry",
                status=400,
            )
        checkpoint_id = checkpoint_id.strip()
        request_id = request_id.strip()
        try:
            lease = self._service._lease_keeper.assert_owned()
        except RuntimeLeaseError as exc:
            raise _HarnessControlError(
                "runtime_lease_lost",
                "the Runtime no longer owns the PulseWorld",
                "retry only after the active Runtime exposes its current epoch",
                status=409,
            ) from exc
        if expected_epoch != lease.epoch:
            raise _HarnessControlError(
                "stale_epoch",
                "the restore request belongs to an old Runtime epoch",
                "refresh checkpoint state and resend with the current epoch",
                status=409,
            )
        backend = self._service._harness_workspace_backend
        if backend is None:
            raise _HarnessControlError(
                "checkpoint_unavailable",
                "the Runtime has no checkpoint restore adapter",
                "enable the checkpoint-first workspace adapter",
                status=503,
            )
        try:
            records = backend.list_checkpoints()
        except Exception as exc:
            raise _HarnessControlError(
                "checkpoint_unavailable",
                f"checkpoint manifests are unreadable: {type(exc).__name__}",
                "repair the protected checkpoint root before restoring",
                status=503,
            ) from exc
        checkpoint = next(
            (
                dict(value)
                for value in records
                if isinstance(value, Mapping)
                and value.get("checkpoint_id") == checkpoint_id
            ),
            None,
        )
        if checkpoint is None:
            raise _HarnessControlError(
                "checkpoint_not_found",
                "the requested checkpoint is not present in the protected manifest",
                "refresh the checkpoint list before retrying",
                status=404,
            )
        turn_id = checkpoint.get("turn_id")
        engram_id = checkpoint.get("engram_id")
        if not isinstance(turn_id, str) or not isinstance(engram_id, str):
            raise _HarnessControlError(
                "checkpoint_scope_invalid",
                "the checkpoint has no durable parent turn scope",
                "quarantine or repair the manifest before restoring",
                status=503,
            )
        ledger = self._service._harness_operation_ledger
        store = self._service._harness_event_store
        if ledger is None or store is None:
            raise _HarnessControlError(
                "operation_ledger_unavailable",
                "the restore recovery ledger is unavailable",
                "restart the owning Runtime before restoring",
                status=503,
            )
        try:
            operation = ledger.admit(
                "checkpoint.restore",
                request_id,
                world_id=self._service._world_id,
                engram_id=engram_id,
                turn_id=turn_id,
                requested_epoch=lease.epoch,
                owner_id=lease.owner_id,
                scope_digest=_stable_digest(
                    {
                        "world_id": self._service._world_id,
                        "engram_id": engram_id,
                        "turn_id": turn_id,
                        "epoch": lease.epoch,
                        "request_id": request_id,
                    }
                ),
                effect_key=_stable_digest(
                    {
                        "checkpoint_id": checkpoint_id,
                        "changed_paths": list(changed_paths),
                    }
                ),
            )
        except HarnessOperationError as exc:
            raise _HarnessControlError(
                "checkpoint_restore_scope_collision",
                "the restore request id is already bound to another scope or effect",
                "use the original request exactly or submit a new request id",
                status=409,
            ) from exc
        if operation.is_terminal:
            return self._replay_checkpoint_restore(operation, store)
        if operation.phase is not OperationPhase.ADMITTED:
            raise _HarnessControlError(
                "operation_recovery_required",
                "the restore operation is nonterminal from an earlier attempt",
                "allow Runtime recovery to classify it before retrying",
                status=409,
            )
        try:
            requested = self._append_control_event(
                turn_id,
                engram_id,
                HarnessEventKind.FILE_CHANGE,
                HarnessEventStatus.RUNNING,
                {
                    "request_id": request_id,
                    "action_request_id": request_id,
                    "operation": "checkpoint.restore",
                    "checkpoint_id": checkpoint_id,
                    "state": "starting",
                    "changed_paths": list(changed_paths),
                },
            )
        except Exception:
            requested = None
        if requested is None:
            return self._checkpoint_restore_not_started(
                ledger,
                operation,
                turn_id,
                engram_id,
                checkpoint_id,
                request_id,
                "checkpoint_restore_start_event_failed",
            )
        try:
            operation = ledger.transition(
                operation.operation_kind,
                operation.operation_id,
                phase=OperationPhase.STARTING,
                expected_epoch=operation.requested_epoch,
                owner_id=operation.owner_id,
            )
            operation = ledger.mark_boundary(
                operation.operation_kind,
                operation.operation_id,
                expected_epoch=operation.requested_epoch,
                owner_id=operation.owner_id,
            )
        except HarnessOperationError:
            return self._checkpoint_restore_not_started(
                ledger,
                operation,
                turn_id,
                engram_id,
                checkpoint_id,
                request_id,
                "checkpoint_restore_boundary_failed",
            )
        try:
            raw = backend.restore(
                checkpoint_id,
                expected_epoch=expected_epoch,
                changed_paths=changed_paths,
            )
        except Exception as exc:
            return self._checkpoint_restore_uncertain(
                ledger,
                operation,
                turn_id,
                engram_id,
                checkpoint_id,
                request_id,
                "checkpoint_restore_adapter_exception",
            )
        if not isinstance(raw, Mapping):
            return self._checkpoint_restore_uncertain(
                ledger,
                operation,
                turn_id,
                engram_id,
                checkpoint_id,
                request_id,
                "checkpoint_result_invalid",
            )
        result = dict(raw)
        state = str(result.get("state", result.get("status", "unknown")))
        ok = result.get("ok") is True
        uncertain = state.casefold() == "uncertain"
        result.setdefault("state", state)
        result.setdefault("status", state)
        event = self._append_checkpoint_terminal(
            ledger,
            operation,
            turn_id,
            engram_id,
            (
                HarnessEventStatus.UNCERTAIN
                if uncertain
                else HarnessEventStatus.COMPLETED
                if ok
                else HarnessEventStatus.FAILED
            ),
            {
                "request_id": request_id,
                "action_request_id": request_id,
                "operation": "checkpoint.restore",
                "checkpoint_id": checkpoint_id,
                "changed_paths": list(result.get("applied_paths", ())),
                "state": state,
                "ok": ok,
                "error_code": result.get("error", result.get("error_code")),
                "terminal": True,
                "evidence_class": result.get(
                    "evidence_class", "LIVE_GATE_UNVERIFIED"
                ),
            },
            terminal_state=(
                OperationTerminalState.UNCERTAIN
                if uncertain
                else OperationTerminalState.COMPLETED
            ),
        )
        if event is None:
            return self._checkpoint_restore_uncertain(
                ledger,
                operation,
                turn_id,
                engram_id,
                checkpoint_id,
                request_id,
                "checkpoint_restore_event_persist_failed",
                append_event=False,
            )
        result.update(
            action_request_id=request_id,
            event_seq=event.seq,
            event_id=event.event_id,
        )
        return result

    def _checkpoint_restore_not_started(
        self,
        ledger: HarnessOperationLedger,
        operation: Any,
        turn_id: str,
        engram_id: str,
        checkpoint_id: str,
        request_id: str,
        code: str,
    ) -> dict[str, Any]:
        event = self._append_checkpoint_terminal(
            ledger,
            operation,
            turn_id,
            engram_id,
            HarnessEventStatus.FAILED,
            {
                "request_id": request_id,
                "action_request_id": request_id,
                "operation": "checkpoint.restore",
                "checkpoint_id": checkpoint_id,
                "state": "failed_not_started",
                "ok": False,
                "error_code": code,
                "terminal": True,
            },
            terminal_state=OperationTerminalState.FAILED_NOT_STARTED,
        )
        return {
            "ok": False,
            "checkpoint_id": checkpoint_id,
            "action_request_id": request_id,
            "state": "failed_not_started",
            "status": "failed_not_started",
            "error": code,
            "event_id": None if event is None else event.event_id,
            "event_seq": None if event is None else event.seq,
        }

    def _checkpoint_restore_uncertain(
        self,
        ledger: HarnessOperationLedger,
        operation: Any,
        turn_id: str,
        engram_id: str,
        checkpoint_id: str,
        request_id: str,
        code: str,
        *,
        append_event: bool = True,
    ) -> dict[str, Any]:
        event = None
        if append_event:
            event = self._append_checkpoint_terminal(
                ledger,
                operation,
                turn_id,
                engram_id,
                HarnessEventStatus.UNCERTAIN,
                {
                    "request_id": request_id,
                    "action_request_id": request_id,
                    "operation": "checkpoint.restore",
                    "checkpoint_id": checkpoint_id,
                    "state": "uncertain",
                    "ok": False,
                    "error_code": code,
                    "terminal": True,
                },
                terminal_state=OperationTerminalState.UNCERTAIN,
            )
        else:
            try:
                ledger.claim_terminal(
                    operation.operation_kind,
                    operation.operation_id,
                    expected_epoch=operation.requested_epoch,
                    owner_id=operation.owner_id,
                    terminal_state=OperationTerminalState.UNCERTAIN,
                    terminal_event_id=None,
                )
            except HarnessOperationError:
                pass
        return {
            "ok": False,
            "checkpoint_id": checkpoint_id,
            "action_request_id": request_id,
            "state": "uncertain",
            "status": "uncertain",
            "uncertain": True,
            "recovery_state": "uncertain",
            "error": code,
            "error_code": code,
            "event_id": None if event is None else event.event_id,
            "event_seq": None if event is None else event.seq,
        }

    def _append_checkpoint_terminal(
        self,
        ledger: HarnessOperationLedger,
        operation: Any,
        turn_id: str,
        engram_id: str,
        status: HarnessEventStatus,
        payload: Mapping[str, Any],
        *,
        terminal_state: OperationTerminalState,
    ):
        store = self._service._harness_event_store
        terminal_append = getattr(store, "append_terminal_operation", None)
        if not callable(terminal_append):
            return None
        draft = HarnessEventDraft(
            turn_id=turn_id,
            world_id=self._service._world_id,
            engram_id=engram_id,
            kind=HarnessEventKind.FILE_CHANGE,
            phase=HarnessEventPhase.TERMINAL,
            source=HarnessEventSource.PULSE_CONTROL,
            status=status,
            payload=dict(payload),
            event_id=deterministic_terminal_event_id(
                operation.operation_kind,
                operation.operation_id,
            ),
        )
        try:
            event, winner = terminal_append(
                draft,
                ledger=ledger,
                operation_kind=operation.operation_kind,
                operation_id=operation.operation_id,
                expected_epoch=operation.requested_epoch,
                owner_id=operation.owner_id,
                terminal_state=terminal_state,
            )
            return event if winner.terminal_event_id == event.event_id else None
        except Exception:
            try:
                current = ledger.get(
                    operation.operation_kind,
                    operation.operation_id,
                )
                post_boundary = current is not None and current.phase in {
                    OperationPhase.BOUNDARY_ENTERED,
                    OperationPhase.ADAPTER_RETURNED,
                    OperationPhase.TERMINALIZING,
                }
                ledger.claim_terminal(
                    operation.operation_kind,
                    operation.operation_id,
                    expected_epoch=operation.requested_epoch,
                    owner_id=operation.owner_id,
                    terminal_state=(
                        OperationTerminalState.UNCERTAIN
                        if post_boundary
                        else terminal_state
                    ),
                    terminal_event_id=None,
                )
            except HarnessOperationError:
                pass
            return None

    def _replay_checkpoint_restore(
        self,
        operation: Any,
        store: HarnessEventStore,
    ) -> dict[str, Any]:
        if (
            operation.recovery_state is not OperationRecoveryState.CLEARED
            or not operation.terminal_event_id
        ):
            raise _HarnessControlError(
                "operation_recovery_required",
                "the restore terminal has no complete durable event binding",
                "reconcile the checkpoint manifest before retrying",
                status=409,
            )
        event = store.get(operation.terminal_event_id)
        if event is None:
            raise _HarnessControlError(
                "operation_recovery_required",
                "the restore terminal event is no longer retained",
                "reconcile the checkpoint manifest before retrying",
                status=409,
            )
        payload = event.payload if isinstance(event.payload, Mapping) else {}
        ok = payload.get("ok") is True
        state = str(payload.get("state", "uncertain"))
        result = {
            "ok": ok,
            "checkpoint_id": payload.get("checkpoint_id"),
            "action_request_id": operation.operation_id,
            "turn_id": operation.turn_id,
            "engram_id": operation.engram_id,
            "state": state,
            "status": state,
            "applied_paths": list(payload.get("changed_paths", ())),
            "evidence_class": payload.get(
                "evidence_class", "LIVE_GATE_UNVERIFIED"
            ),
            "event_id": event.event_id,
            "event_seq": event.seq,
            "idempotent": True,
        }
        error = payload.get("error_code")
        if not ok:
            result["error"] = error if isinstance(error, str) else "checkpoint_restore_failed"
        return result

    def _scope_turn(self, turn_id: str, request: Mapping[str, Any]):
        ledger = self._service._causal_ledger
        turn = ledger.get_turn_for_world(turn_id, self._service._world_id)
        if turn is None:
            raise _HarnessControlError(
                "turn_not_found",
                f"Harness turn {turn_id!r} was not found",
                "refresh the Workbench turn list",
                status=404,
            )
        try:
            lease = self._service._lease_keeper.assert_owned()
        except RuntimeLeaseError as exc:
            raise _HarnessControlError(
                "runtime_lease_lost",
                "the Runtime no longer owns the PulseWorld",
                "reconnect after a new Runtime has acquired the durable lease",
                status=409,
            ) from exc
        expected_epoch = request.get("expected_epoch")
        if expected_epoch != lease.epoch:
            raise _HarnessControlError(
                "stale_epoch",
                "the control request was created for an old world lease epoch",
                "read the current turn summary and resend with its epoch",
                status=409,
            )
        if str(turn.state.value) not in {"running", "uncertain"}:
            raise _HarnessControlError(
                "terminal_turn",
                "the Harness turn is already terminal",
                "read the terminal event and start a new turn",
                status=410,
            )
        expected_state = request.get("expected_state")
        if expected_state is not None and str(turn.state.value) != str(expected_state):
            raise _HarnessControlError(
                "stale_turn",
                "the Harness turn state changed before the control request arrived",
                "refresh the turn summary and retry with its current state",
                status=409,
            )
        return turn, lease.epoch

    @staticmethod
    def _require_request_id(request: Mapping[str, Any]) -> str:
        value = request.get("request_id")
        if not isinstance(value, str) or not value.strip() or len(value) > 256:
            raise _HarnessControlError(
                "invalid_request_id",
                "request_id must be a bounded non-empty string",
                "send a stable request_id so a retry cannot duplicate a side effect",
                status=400,
            )
        return value.strip()

    def _reserve_control(
        self,
        scope: _HarnessControlScope,
    ) -> dict[str, Any] | None:
        with self._lock:
            prior_scope = self._request_scopes.get(scope.request_id)
            if prior_scope is not None and prior_scope != scope:
                raise _HarnessControlError(
                    "control_request_scope_conflict",
                    "request_id is already bound to another World, Engram, turn, epoch or operation",
                    "generate a new request_id for the new control scope",
                    status=409,
                )
            prior = self._results.get(scope)
            if prior is not None:
                return dict(prior, idempotent=True)
            if scope in self._inflight:
                raise _HarnessControlError(
                    "control_request_in_flight",
                    "the same control request is still being resolved",
                    "wait for the first request to settle before retrying",
                    status=409,
                )
            self._request_scopes[scope.request_id] = scope
            self._inflight.add(scope)
        return None

    def _release_control(self, scope: _HarnessControlScope) -> None:
        with self._lock:
            self._inflight.discard(scope)
            if (
                scope not in self._results
                and self._request_scopes.get(scope.request_id) == scope
            ):
                self._request_scopes.pop(scope.request_id, None)

    def _remember(
        self,
        scope: _HarnessControlScope,
        result: dict[str, Any],
    ) -> None:
        with self._lock:
            self._request_scopes[scope.request_id] = scope
            self._results[scope] = dict(result)
            self._inflight.discard(scope)
            while len(self._results) > self._MAX_IDEMPOTENCY:
                evicted = next(iter(self._results))
                self._results.pop(evicted)
                if self._request_scopes.get(evicted.request_id) == evicted:
                    self._request_scopes.pop(evicted.request_id, None)

    def _append_control_event(
        self,
        turn_id: str,
        engram_id: str,
        kind: HarnessEventKind,
        status: HarnessEventStatus,
        payload: dict[str, Any],
    ):
        event_store = self._service._harness_event_store
        if event_store is None:
            raise _HarnessControlError(
                "harness_event_store_unavailable",
                "the Harness event projection is not attached",
                "attach the durable event store before accepting control",
                status=503,
            )
        return event_store.append(
            HarnessEventDraft(
                turn_id=turn_id,
                world_id=self._service._world_id,
                engram_id=engram_id,
                kind=kind,
                phase=HarnessEventPhase.CONTROL,
                source=HarnessEventSource.PULSE_CONTROL,
                status=status,
                payload=payload,
            )
        )


class _BoundedActionExecutor:
    """Runtime-owned bounded queue for approved Harness adapter work."""

    def __init__(self, *, max_workers: int, max_pending: int) -> None:
        if max_workers < 1 or max_pending < 0:
            raise ValueError("Harness executor capacity must be positive")
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="pulse-harness-action",
        )
        self._slots = threading.BoundedSemaphore(max_workers + max_pending)

    def submit(self, function: Callable[..., Any], *args: Any, **kwargs: Any) -> Future:
        if not self._slots.acquire(blocking=False):
            raise RuntimeError("harness_execution_capacity_exhausted")
        try:
            return self._executor.submit(self._run, function, args, kwargs)
        except BaseException:
            self._slots.release()
            raise

    def _run(
        self,
        function: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        try:
            return function(*args, **kwargs)
        finally:
            self._slots.release()

    def shutdown(self, *, wait: bool, cancel_futures: bool) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)


class RuntimeService(_RuntimeShutdownCoordinator):
    """One object owning the whole organism, alive between requests.

    Construct it, `await start()`, and it keeps thinking. `await stop()` ends
    the loop without losing the thought; a later construction against the same
    database resumes it.
    """

    def __init__(
        self,
        config: RuntimeServiceConfig | None = None,
        *,
        substrates: SubstrateRegistry | None = None,
        harness_factory: HarnessFactory | None = None,
        shutdown_controller: RuntimeShutdownController | None = None,
    ):
        self._config = RuntimeServiceConfig() if config is None else config
        c = self._config

        workspace = (Path(c.workspace) if c.workspace else Path.cwd()).resolve()
        self._workspace = workspace
        self._substrates = substrates
        self._close_lock = threading.Lock()
        self._tick_lock = threading.Lock()
        self._recovery_run_lock = threading.Lock()
        self._recovery_owner_lock = threading.RLock()
        self._recovery_owner_tasks: list[dict[str, Any]] = []
        self._publication_gate: RuntimePublicationGate | None = None
        self._publication_permit: RuntimePublicationPermit | None = None
        self._bootstrap_permit: RuntimeBootstrapPermit | None = None
        self._recovery_permit: RuntimeRecoveryPermit | None = None
        if shutdown_controller is not None and not isinstance(
            shutdown_controller,
            RuntimeShutdownController,
        ):
            raise ValueError(
                "shutdown_controller must be a RuntimeShutdownController"
            )
        self._shutdown_controller = (
            RuntimeShutdownController()
            if shutdown_controller is None
            else shutdown_controller
        )
        self._shutdown_observer = self._shutdown_controller.observer
        self._shutdown_builder: ShutdownReportBuilder | None = None
        self._shutdown_report: RuntimeShutdownReport | None = None
        self._shutdown_owner_claim: RuntimeShutdownClaim | None = None
        self._shutdown_done = threading.Event()
        self._shutdown_coordinator_thread: threading.Thread | None = None
        self._shutdown_terminalizer_thread: threading.Thread | None = None
        self._shutdown_storage_finalizer_started = False
        self._shutdown_storage_finalizer_thread: threading.Thread | None = None
        self._runtime_construction_done = threading.Event()
        self._runtime_lease_lost = threading.Event()
        self._retained_owner_probes = RuntimeRetainedOwnerProbeRegistry()
        self._physical_convergence = _RuntimePhysicalConvergenceCoordinator(
            self._retained_owner_probes,
            translator=self._shutdown_result_evidence,
        )
        self._closed = False
        self._quiescing = False
        self._stop = asyncio.Event()
        self._tick_loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task | None = None
        self._lease_keeper: RuntimeLeaseKeeper | None = None
        self._causal_ledger: CausalLedger | None = None
        self._metrics: MetricsRecorder | None = None
        self._gateway: PulseToolGateway | None = None
        self._harness: HarnessRuntime | None = None
        # Set after the additive Harness event projection is constructed.
        # Pi can be built with this callback before the store is ready; the
        # callback is a no-op until the owned Runtime has installed it.
        self._harness_event_store: HarnessEventStore | None = None
        self._harness_event_projector: HarnessEventProjector | None = None
        self._harness_operation_ledger: HarnessOperationLedger | None = None
        self._recovered_harness_operations: tuple[dict[str, Any], ...] = ()
        self._harness_control_gateway: _RuntimeHarnessControlGateway | None = None
        self._harness_action_broker: HarnessActionBroker | None = None
        self._harness_action_executor: _BoundedActionExecutor | None = None
        self._harness_sandbox_backend: CodexCliSandboxBackend | None = None
        self._harness_sandbox_preflight: SandboxPreflight | None = None
        self._harness_pipe_backend: CodexCliPipeProcessBackend | None = None
        self._harness_terminal_manager: TerminalManager | None = None
        self._harness_terminal_store: TerminalSessionStore | None = None
        self._harness_terminal_sessions: TerminalSessionService | None = None
        self._harness_workspace_backend: Any = None
        self._harness_mcp_service: MCPRuntimeService | None = None
        self._harness_mcp_backend: MCPActionBackend | None = None
        self._harness_mcp_registry_gate: MCPRegistryGate | None = None
        self._harness_task_worker_backend: PiTaskWorkerBackend | None = None
        self._harness_task_subagents: TaskSubagentService | None = None
        self._harness_task_worker_bridge: TaskWorkerToolBridge | None = None
        self._task_offers: TaskOfferService | None = None
        self._task_relationships: TaskRelationshipService | None = None
        self._life_tools: LifeToolService | None = None
        self._purpose_governance: PurposeGovernance | None = None
        self._purpose_recovery: dict[str, int] = {}
        self._living_portfolio_projector: LivingPortfolioProjector | None = None
        self._role_accountability_projector: RoleAccountabilityProjector | None = None
        self._role_lease_store: RoleLeaseStore | None = None
        self._stimulus_control_ledger: ControlLedger | None = None
        self._stimulus_firewall: StimulusFirewall | None = None
        self._stimulus_observer_health = "initializing"
        self._stimulus_observer_failures = 0
        self._stimulus_observer_last_error: str | None = None
        self._stimulus_control_audit: dict[str, Any] = {
            "retained_records": 0,
            "total_seen": 0,
            "oldest_sequence": None,
            "latest_sequence": None,
            "truncated": False,
            "payload_free": True,
            "replay_can_enqueue": False,
        }
        self._scheduler: DurableCenterScheduler | None = None
        self._recovered_center_reservations = ()
        self._runtime_construction_probe = RuntimeRetainedOwnerProbe(
            "runtime_construction",
            self._observe_runtime_construction_owner(),
        )
        self._retained_owner_probes.register(self._runtime_construction_probe)

        self._storage = Storage(c.db_path)
        try:
            self._harness_operation_ledger = HarnessOperationLedger(self._storage)
        except Exception:
            self._storage.close()
            self._closed = True
            raise
        try:
            self._lease_keeper = RuntimeLeaseKeeper(
                self._storage,
                ttl_sec=c.runtime_lease_ttl_sec,
                renew_interval_sec=c.runtime_lease_renew_interval_sec,
                on_lost=self._on_runtime_lease_lost,
            )
        except RuntimeLeaseConflictError as exc:
            self._storage.close()
            self._closed = True
            lease = exc.lease
            detail = (
                "another Runtime owns this durable database"
                if lease is None
                else "another Runtime owns this durable database at "
                f"epoch {lease.epoch} until {lease.expires_at.isoformat()}"
            )
            raise ServiceError(
                "runtime_lease_conflict",
                detail,
                "stop the current Runtime cleanly, or retry after its lease expires",
                status=409,
            ) from exc
        except Exception:
            # Acquisition and heartbeat startup happen before the ordinary
            # constructor cleanup boundary. Never leak the first SQLite
            # connection when that earliest ownership boundary itself fails.
            self._storage.close()
            self._closed = True
            raise
        self._shutdown_controller.mark_runtime_authority_acquired()
        try:
            owner_lease = self._lease_keeper.assert_owned()
            self._publication_gate = RuntimePublicationGate(
                owner_lease.owner_id,
                owner_lease.epoch,
            )
            self._publication_permit = self._publication_gate.publication_permit
            self._bootstrap_permit = self._publication_gate.bootstrap_permit
            self._storage.bind_runtime_publication_permit(self._publication_permit)
        except Exception:
            self._cleanup_startup_failure(None)
            raise
        # Recovery is the first operation after opening the durable substrate.
        # Ownership is the only earlier mutation boundary: a rejected second
        # Runtime must never recover the first owner's live work.
        self._recovered_generation_summary_ids: tuple[str, ...] = ()
        self._recovered_generation_orphan_ids: tuple[str, ...] = ()
        self._recovered_continuity_successor_id: str | None = None
        try:
            self._causal_ledger = CausalLedger(
                self._storage,
                default_runtime_fence=RuntimeFence(
                    owner_id=owner_lease.owner_id,
                    epoch=owner_lease.epoch,
                    permit=self._publication_permit,
                ),
            )
            self._recovery = self._causal_ledger.recover_inflight(
                runtime_fence=self._current_bootstrap_fence()
            )
            (
                self._recovered_generation_summary_ids,
                self._recovered_generation_orphan_ids,
            ) = self._isolate_recovered_generation_state()
            self._recovered_continuity_successor_id = (
                self._repair_recovered_continuity_identity()
            )
        except Exception:
            self._cleanup_startup_failure(None)
            raise
        try:
            self._metrics = MetricsRecorder(
                c.metrics_path,
                flush_every=c.metrics_flush_every,
                max_bytes=c.metrics_max_bytes,
                archive_count=c.metrics_archive_count,
                publication_permit=self._publication_permit,
            )
            lease_health = self._lease_keeper.health()
            self._metrics.record(
                "runtime_lease_acquired",
                owner=lease_health.lease.owner_id,
                epoch=lease_health.lease.epoch,
                expires_at=lease_health.lease.expires_at.isoformat(),
                takeover=lease_health.lease.epoch > 1,
            )
            recovered_reservations = self._storage.recover_held_center_reservations(
                lease_health.lease.owner_id,
                lease_health.lease.epoch,
                now=_now(),
                bootstrap_permit=self._bootstrap_permit,
            )
            self._recovered_center_reservations = tuple(recovered_reservations)
            if recovered_reservations:
                outcomes: dict[str, int] = {}
                old_epochs: set[int] = set()
                for reservation in recovered_reservations:
                    outcome = (
                        "unknown"
                        if reservation.outcome is None
                        else reservation.outcome.value
                    )
                    outcomes[outcome] = outcomes.get(outcome, 0) + 1
                    old_epochs.add(reservation.lease_epoch)
                self._metrics.record(
                    "center_reservation_recovered",
                    count=len(recovered_reservations),
                    old_epochs=sorted(old_epochs),
                    outcomes=outcomes,
                )
            if (
                self._recovered_generation_summary_ids
                or self._recovered_generation_orphan_ids
            ):
                self._metrics.record(
                    "generation_recovery_isolation",
                    summaries=len(self._recovered_generation_summary_ids),
                    orphans=len(self._recovered_generation_orphan_ids),
                )
            self._gateway = PulseToolGateway(
                dispatcher=self._dispatch_gateway_tool,
                authorizer=self._authorize_builtin_tool,
                canceller=self._cancel_gateway_action,
            )
            action_workers = max(1, min(4, int(c.pulse_worker_capacity)))
            self._harness_action_executor = _BoundedActionExecutor(
                max_workers=action_workers,
                max_pending=action_workers * 4,
            )
        except Exception:
            self._cleanup_startup_failure(None)
            raise
        harness: HarnessRuntime | None = None
        try:
            if c.mock:
                self._llm = LLMAdapter(
                    provider=c.provider,
                    model=c.model,
                    max_tokens=c.max_tokens,
                    mock=True,
                )
                harness = _MockHarnessRuntime(workspace, self._llm)
                self._harness = harness
                self._harness_kind = _HARNESS_KIND_MOCK
            else:
                factory = (
                    PiHarnessRuntime
                    if harness_factory is None
                    else harness_factory
                )
                harness = factory(
                    workspace,
                    binding_state=self._storage.load_component_state(
                        BINDING_COMPONENT
                    ),
                    binding_callback=self._persist_harness_bindings,
                    metrics_callback=self._record_harness_metric,
                    event_callback=self._record_harness_event,
                    executable=c.pi_executable,
                    provider=c.pi_provider or c.provider,
                    model=c.pi_model if c.pi_model is not None else c.model,
                    handshake_timeout_sec=c.harness_handshake_timeout_sec,
                    sideband_timeout_sec=c.harness_sideband_timeout_sec,
                    abort_timeout_sec=c.harness_abort_timeout_sec,
                    max_live_sessions=c.pi_resident_session_limit,
                    tool_gateway=self._gateway,
                    publication_permit=self._publication_permit,
                    bootstrap_permit=self._bootstrap_permit,
                )
                # Preflight may block while ownership is lost. Attach the
                # exact Harness first so the canonical census can observe it.
                self._harness = harness
                if self._shutdown_controller.primary_trigger is not None:
                    # The public deadline may have terminalized while factory
                    # itself was blocked. Never preflight or admit this late
                    # object; close it under the one retained-owner registry.
                    self._register_late_harness_shutdown(harness)
                    if self._runtime_lease_lost.is_set():
                        raise self._runtime_lease_service_error()
                    raise ServiceError(
                        "runtime_quiescing",
                        "Harness factory returned after Runtime shutdown began",
                        "construct a new Runtime after the previous owner converges",
                        status=409,
                    )
                # Production readiness is a constructor invariant.  A missing
                # Pi executable cannot be deferred to a background pulse.
                harness.preflight()
                self._harness_kind = _HARNESS_KIND_PI
                self._llm = LLMAdapter(
                    provider=c.provider,
                    model=c.model,
                    max_tokens=c.max_tokens,
                    mock=False,
                )
            self._harness = harness
        except Exception:
            self._cleanup_startup_failure(harness)
            raise

        try:
            self._require_runtime_owner()
            self._initialize_owned_runtime(c, workspace, substrates)
        except Exception:
            # Binding preflight is not the last fallible startup boundary.
            # Keep ownership through every durable world mutation, then tear
            # down all constructed resources if any later component rejects
            # its persisted state or configuration.
            self._cleanup_startup_failure(self._harness)
            raise
        self._mark_runtime_construction_done()
        if (
            self._shutdown_controller.primary_trigger
            is RuntimeShutdownTrigger.LEASE_LOST
        ):
            self._cleanup_startup_failure(self._harness)
            raise self._runtime_lease_service_error()

    def _initialize_owned_runtime(
        self,
        c: RuntimeServiceConfig,
        workspace: Path,
        substrates: SubstrateRegistry | None,
    ) -> None:
        """Finish construction while the caller's lease cleanup guard is live."""

        self._online_learning_audit = OnlineLearningAudit()
        self._connections = ConnectionNetwork(
            self._storage,
            ConnectionConfig(),
            learning_policy=c.online_learning_policy,
            learning_audit=self._online_learning_audit,
        )
        self._library = Library(
            workspace / ".pulse" / "library",
            publication_authority=self._publication_permit,
        )
        self._mgr = EngramManager(
            self._storage,
            self._llm,
            self._connections,
            library=self._library,
            substrates=substrates,
            harness=self._harness,
            harness_turn_timeout_sec=c.harness_turn_timeout_sec,
            causal_ledger=self._causal_ledger,
        )
        self._world = WorldRegistry(self._storage)
        self._dendrite = DendriteProcessor(self._mgr, DendriteConfig(
            silence_threshold=c.silence_threshold,
            default_max_wait=c.default_max_wait,
        ))
        self._runtime = RuntimeManager(RuntimeConfig(
            budget_per_tick=c.budget_per_tick,
            hourly_token_budget=c.hourly_token_budget,
            daily_token_budget=c.hourly_token_budget * 12,
            cache_read_discount=self._llm.cache_read_discount,
        ))

        # Sideband learners keep per-engram slot maps; the manager's listeners
        # are what keep those maps coherent across succession and archival.
        self._router: DelegationRouter | None = None
        if c.with_router:
            self._router = DelegationRouter(
                self._storage,
                metrics=self._metrics,
                learning_policy=c.online_learning_policy,
                learning_audit=self._online_learning_audit,
            )
            self._mgr.add_succession_listener(self._router.reassign_engram)
            self._mgr.add_archive_listener(self._router.mask_engram)

        self._claustrum: ClaustrumModulator | None = None
        if c.with_claustrum:
            self._claustrum = ClaustrumModulator(
                self._storage,
                metrics=self._metrics,
                learning_policy=c.online_learning_policy,
                learning_audit=self._online_learning_audit,
            )
            self._mgr.add_succession_listener(self._claustrum.reassign_engram)
            self._mgr.add_archive_listener(self._claustrum.mask_engram)

        # .pulse holds the library and the database; no write-path tool may
        # reach into it, or a poisoned engram could persist a skill that other
        # engrams read back.
        self._tools = ToolRegistry(
            mock=c.mock,
            workspace_root=workspace,
            protected_roots=[workspace / ".pulse"],
            publication_permit=self._publication_permit,
        )
        # The Python FrontAgent/regex loop is retained only for explicit mock
        # compatibility. Production delegation is a queued causal root that
        # the ordinary scheduler executes through the target's persistent Pi
        # HarnessSession.
        self._delegator: Delegator | None = None
        if c.mock:
            self._delegator = Delegator(
                self._storage,
                self._mgr,
                self._tools,
                library=self._library,
                metrics=self._metrics,
                router=self._router,
                config=DelegatorConfig(max_think_iterations=5),
            )

        # ── One durable PulseWorld (contract §5) ────────────────
        # ``front_engram_id`` remains a compatibility anchor for old callers
        # and delegation tools.  Product work begins at a TaskFront.
        self._front_id, self._resumed = self._resume_or_create_front()
        (
            self._world_id,
            self._world_created_at,
            self._legacy_front_migrated,
        ) = self._resume_or_create_world()
        self._purpose_governance = PurposeGovernance(
            c.db_path,
            publication_permit=self._publication_permit,
        )
        self._purpose_recovery = (
            self._purpose_governance.reconcile_pending_amendments()
        )
        if any(self._purpose_recovery.values()):
            self._metrics.record(
                "purpose_amendment_recovered",
                **self._purpose_recovery,
            )
        self._living_portfolio_projector = LivingPortfolioProjector(
            self._world,
            self._purpose_governance,
            self._world_id,
        )
        self._role_lease_store = RoleLeaseStore(
            c.db_path,
            publication_permit=self._publication_permit,
        )
        self._role_accountability_projector = RoleAccountabilityProjector(
            self._role_lease_store,
            self._world_id,
        )
        role_owner_lease = self._lease_keeper.assert_owned()
        role_recovery = self._role_lease_store.recover_runtime_takeover(
            RoleRuntimeLeaseProof(
                world_id=self._world_id,
                owner_id=role_owner_lease.owner_id,
                epoch=role_owner_lease.epoch,
            ),
            bootstrap_permit=self._bootstrap_permit,
        )
        if role_recovery["rebound"] or role_recovery["revoked"]:
            self._metrics.record(
                "role_lease_runtime_recovered",
                rebound=len(role_recovery["rebound"]),
                revoked=len(role_recovery["revoked"]),
                epoch=role_owner_lease.epoch,
            )
        self._stimulus_control_ledger = ControlLedger(max_records=4096)
        self._stimulus_firewall = StimulusFirewall(
            ledger=self._stimulus_control_ledger,
            max_seen_decisions=4096,
        )
        self._task_offers = TaskOfferService(
            self._storage,
            self._causal_ledger,
            self._world,
            world_id=self._world_id,
        )
        self._task_relationships = TaskRelationshipService(
            self._storage,
            self._causal_ledger,
            world_id=self._world_id,
        )
        reconciled_relationships = (
            self._task_relationships.reconcile_accepted_offers()
        )
        if reconciled_relationships:
            self._metrics.record(
                "task_relationships_reconciled",
                count=reconciled_relationships,
                world=self._world_id,
            )
        try:
            self._stimulus_control_audit = (
                self._storage.harness_control_audit_snapshot(self._world_id)
            )
            self._stimulus_observer_health = "healthy"
        except Exception as exc:  # noqa: BLE001 - expose degraded audit state
            self._stimulus_observer_health = "degraded"
            self._stimulus_observer_failures = 1
            self._stimulus_observer_last_error = type(exc).__name__
            self._metrics.record(
                "harness_control_audit_failed",
                error_type=type(exc).__name__,
            )
        self._harness_event_store = HarnessEventStore(
            self._storage,
            observer=self._observe_harness_event,
        )
        self._harness_event_projector = HarnessEventProjector(
            self._harness_event_store,
            world_id=self._world_id,
        )
        self._recovered_harness_operations = self._recover_harness_operations()
        action_backend = None
        if not c.mock:
            if c.codex_sandbox_executable is None:
                self._harness_sandbox_preflight = SandboxPreflight(
                    available=False,
                    executable=None,
                    version=None,
                    command_surface=(),
                    error_code="codex_sandbox_executable_required",
                    detail="an explicit Codex CLI path is required; PATH discovery never enables the command adapter",
                )
            else:
                try:
                    live_gate = None
                    live_gate_error = None
                    if c.codex_sandbox_live_gate is not None:
                        try:
                            live_gate = SandboxLiveGate.load(c.codex_sandbox_live_gate)
                        except (OSError, ValueError):
                            live_gate_error = "sandbox_live_gate_artifact_invalid"
                    candidate_backend = CodexCliSandboxBackend(
                        workspace_root=workspace,
                        executable=c.codex_sandbox_executable,
                        permission_profile=c.codex_sandbox_permission_profile,
                        live_gate=live_gate,
                        codex_config=c.codex_sandbox_config,
                    )
                    preflight = candidate_backend.preflight()
                    if live_gate_error is not None:
                        preflight = SandboxPreflight(
                            available=preflight.available,
                            executable=preflight.executable,
                            version=preflight.version,
                            command_surface=preflight.command_surface,
                            error_code=live_gate_error,
                            detail=(
                                "the configured live-gate artifact is invalid; no process adapter was enabled"
                            ),
                            sandbox_implementation="unknown",
                        )
                    self._harness_sandbox_preflight = preflight
                    # Do not enable `:workspace` bash yet.  Codex's profile
                    # alone is not evidence that `.pulse` is protected; until
                    # that adversarial gate or verified staging adapter exists,
                    # only an explicitly verified read-only profile may enter
                    # the action broker.
                    if (
                        preflight.live_gate_verified
                        and c.codex_sandbox_permission_profile == ":read-only"
                        and c.codex_sandbox_enabled
                    ):
                        action_backend = candidate_backend
                except Exception:
                    # A sandbox discovery problem is a visible unsupported adapter
                    # state, not a reason to stop the already valid PulseWorld.
                    self._harness_sandbox_preflight = SandboxPreflight(
                        available=False,
                        executable=str(c.codex_sandbox_executable),
                        version=None,
                        command_surface=(),
                        error_code="codex_sandbox_preflight_error",
                        detail="Codex sandbox preflight raised; no process adapter was enabled",
                    )
        self._harness_sandbox_backend = action_backend
        if not c.mock and c.harness_pipe_sessions_enabled:
            if action_backend is None:
                reason = (
                    "sandbox_backend_unavailable"
                    if self._harness_sandbox_preflight is None
                    else (
                        self._harness_sandbox_preflight.error_code
                        or "sandbox_live_gate_required"
                    )
                )
                raise ValueError(
                    "harness PIPE sessions require the verified read-only "
                    f"sandbox backend: {reason}"
                )
            try:
                lifecycle_gate = PipeLifecycleGate.load(
                    Path(c.harness_pipe_lifecycle_gate)
                )
            except (OSError, ValueError) as exc:
                raise ValueError(
                    "harness_pipe_lifecycle_gate is invalid"
                ) from exc
            pipe_backend = CodexCliPipeProcessBackend(
                action_backend,
                lifecycle_gate=lifecycle_gate,
                max_processes=c.harness_pipe_session_capacity,
            )
            if not pipe_backend.supports_execution:
                raise ValueError(
                    "harness PIPE session backend is not safely callable: "
                    f"{pipe_backend.availability_error or 'background_lifecycle_unverified'}"
                )
            owner_lease = self._lease_keeper.assert_owned()
            terminal_manager = TerminalManager(
                workspace,
                backend=pipe_backend,
                output_redactor=action_backend.redact_pipe_output,
                max_processes=c.harness_pipe_session_capacity,
            )
            terminal_store = TerminalSessionStore(
                self._storage,
                self._harness_event_store,
            )
            terminal_sessions = TerminalSessionService(
                manager=terminal_manager,
                store=terminal_store,
                world_id=self._world_id,
                owner_id=owner_lease.owner_id,
                epoch=owner_lease.epoch,
            )
            self._harness_pipe_backend = pipe_backend
            self._harness_terminal_manager = terminal_manager
            self._harness_terminal_store = terminal_store
            self._harness_terminal_sessions = terminal_sessions
            action_backend = TerminalSessionActionBackend(
                action_backend,
                terminal_sessions,
                background_backend=pipe_backend,
            )
        workspace_backend = None
        if not c.mock and c.harness_file_mutation_enabled:
            # Import only for an explicit operator opt-in so a contract-only
            # installation cannot accidentally grow a writable path.
            from pulse_system.agent.harness.workspace_mutation import (
                CheckpointedWorkspaceBackend,
            )

            workspace_backend = CheckpointedWorkspaceBackend(
                workspace_root=workspace,
                checkpoint_root=Path(c.harness_checkpoint_root),
                world_id=self._world_id,
                publication_authority=self._publication_permit,
            )
            preflight = workspace_backend.preflight()
            if not isinstance(preflight, Mapping) or preflight.get("available") is not True:
                reason = (
                    preflight.get("error_code", "workspace_checkpoint_preflight_failed")
                    if isinstance(preflight, Mapping)
                    else "workspace_checkpoint_preflight_failed"
                )
                raise ValueError(
                    f"harness checkpoint backend is not safely callable: {reason}"
                )
        self._harness_workspace_backend = workspace_backend
        mcp_backend = None
        if not c.mock and c.harness_mcp_enabled:
            allowed_server_ids = frozenset(c.harness_mcp_allowlisted_server_ids)
            allowed_descriptors = tuple(
                descriptor
                for descriptor in c.harness_mcp_descriptors
                if descriptor.server_id in allowed_server_ids
            )
            self._harness_mcp_registry_gate = MCPRegistryGate(
                allowed_descriptors,
                workspace_root=workspace,
            )
            self._harness_mcp_service = MCPRuntimeService(
                c.harness_mcp_descriptors,
                allowlisted_server_ids=c.harness_mcp_allowlisted_server_ids,
                publication_permit=self._publication_permit,
            )
            mcp_backend = MCPActionBackend(
                self._harness_mcp_service,
                world_id=self._world_id,
                registry_gate=self._harness_mcp_registry_gate,
            )
            self._harness_mcp_backend = mcp_backend
        action_routes: dict[str, Any] = {}
        if action_backend is not None:
            action_routes["bash"] = action_backend
        if workspace_backend is not None:
            action_routes["edit"] = workspace_backend
            action_routes["write"] = workspace_backend
        if mcp_backend is not None:
            action_routes["pulse_mcp_call"] = mcp_backend
        broker_backend = RoutedActionBackend(action_routes) if action_routes else None
        action_policy = None
        if not c.mock:
            action_policy = ExecutionPolicy(
                workspace_root=workspace,
                filesystem=(
                    FilesystemAccess.WORKSPACE_WRITE
                    if workspace_backend is not None
                    else (
                        FilesystemAccess.READ_ONLY
                        if c.codex_sandbox_permission_profile == ":read-only"
                        else FilesystemAccess.WORKSPACE_WRITE
                    )
                ),
                network=NetworkAccess.DENY,
                command=CommandScope.WORKSPACE,
                command_allowlist=c.harness_command_allowlist,
                approval_mode=ApprovalMode.ALWAYS,
                capability_allowlist=(
                    ("mcp.call",) if mcp_backend is not None else ()
                ),
                protected_roots=(Path(workspace) / ".pulse",),
            )
        action_owner_lease = self._lease_keeper.assert_owned()
        self._harness_action_broker = HarnessActionBroker(
            workspace_root=workspace,
            world_id=self._world_id,
            event_store=self._harness_event_store,
            epoch_provider=self._harness_action_epoch,
            command_allowlist=c.harness_command_allowlist,
            policy=action_policy,
            backend=broker_backend,
            execution_executor=self._harness_action_executor,
            operation_ledger=self._harness_operation_ledger,
            owner_id=action_owner_lease.owner_id,
        )
        self._harness_control_gateway = _RuntimeHarnessControlGateway(self)
        if not c.mock and c.harness_task_worker_enabled:
            worker_root = Path(c.harness_task_worker_root).expanduser().resolve()
            if (
                worker_root == workspace
                or worker_root in workspace.parents
                or workspace in worker_root.parents
            ):
                raise ValueError(
                    "harness_task_worker_root must be external to the Runtime workspace"
                )
            backend_template = getattr(self._harness, "backend_template", None)
            if backend_template is None:
                raise ValueError(
                    "harness task workers require the production Pi Harness backend"
                )
            self._harness_task_worker_backend = PiTaskWorkerBackend(
                backend_template,
                worker_root=worker_root,
                max_workers=c.harness_task_worker_capacity,
                max_pending=c.harness_task_worker_capacity,
                default_timeout_sec=c.harness_task_worker_default_timeout_sec,
                max_timeout_sec=c.harness_task_worker_max_timeout_sec,
                handshake_timeout_sec=c.harness_handshake_timeout_sec,
                sideband_timeout_sec=c.harness_sideband_timeout_sec,
                abort_timeout_sec=c.harness_abort_timeout_sec,
                publication_permit=self._publication_permit,
            )
            self._harness_task_subagents = TaskSubagentService(
                self._harness_task_worker_backend,
                config=TaskSubagentConfig(
                    max_workers=c.harness_task_worker_capacity,
                    max_workers_per_turn=c.harness_task_worker_max_per_turn,
                    default_timeout_sec=c.harness_task_worker_default_timeout_sec,
                    max_timeout_sec=c.harness_task_worker_max_timeout_sec,
                ),
            )
            self._harness_task_worker_bridge = TaskWorkerToolBridge(
                self._harness_task_subagents,
                operation_ledger=self._harness_operation_ledger,
                event_store=self._harness_event_store,
                world_id=self._world_id,
                owner_id=action_owner_lease.owner_id,
                epoch_provider=self._harness_action_epoch,
                role_store=self._role_lease_store,
            )
        # The Manager is constructed before the persisted world identity is
        # loaded for historical bootstrap reasons. Bind it before any direct
        # succession API can run so a no-parent call cannot fall back to a
        # second implicit ``default`` world.
        self._mgr.set_causal_world_id(self._world_id)
        self._mgr.add_succession_listener(self._world.handle_succession)
        # World identity is the relationship/task execution authority.  Move
        # it first so a later independent governance-store failure cannot leave
        # an archived predecessor owning a live task relationship.
        self._mgr.add_succession_listener(self._advance_subject_governance)
        self._mgr.add_succession_listener(self._follow_succession)
        if self._delegator is not None:
            self._tools.register(
                "delegate",
                "Delegate heavy execution to another engram "
                "(@<id> targets an existing one)",
                self._delegator.as_tool(self._front_id),
            )
            self._tools.register(
                "discover_skills",
                "List skills available to this engram",
                self._library.discover_tool(self._front_id),
            )

        self._delegation_tunnel = DurableDelegationTunnel(
            self._storage,
            self._causal_ledger,
            world_id=self._world_id,
            router=self._router,
            runtime_fence_provider=lambda: self._current_runtime_fence(
                require_accepting=False
            ),
        )
        self._delegation_scan_seq = 0
        self._pending_delegation_requests: set[str] = set()

        # ── Habitat, Sensory and first-class life tools ─────────
        # These are one-world services.  A TaskFront never receives its own
        # runtime, Habitat root or Gateway capability.
        self._habitat = ManagedHabitat(
            workspace,
            self._world_id,
            publication_authority=self._publication_permit,
        )
        self._sensory = SensoryCortex(
            self._dendrite,
            default_wait=max(0.0, c.default_max_wait),
        )
        self._mgr.add_succession_listener(self._sensory.reassign_engram)
        self._life_tools = LifeToolService(
            self._storage,
            self._causal_ledger,
            self._world,
            self._habitat,
            world_id=self._world_id,
            workspace=workspace,
            delegation_tunnel=self._delegation_tunnel,
            delegator=self._delegator,
            metrics=self._metrics,
            action_gateway=self._harness_action_broker.dispatch,
            action_waiter=self._harness_action_broker.wait_for_action,
            task_worker_gateway=(
                None
                if self._harness_task_worker_bridge is None
                else self._harness_task_worker_bridge.dispatch
            ),
            purpose_governance=self._purpose_governance,
            lineage_resolver=self._ensure_subject_lineage,
            role_store=self._role_lease_store,
            runtime_lease_provider=self._role_runtime_proof,
            workspace_receipt_resolver=(
                None
                if workspace_backend is None
                else workspace_backend.verify_role_receipt
            ),
            task_offer_service=self._task_offers,
            task_relationship_service=self._task_relationships,
            task_relationship_revoker=self._revoke_task_relationship_execution,
            runtime_fence_provider=lambda: self._current_runtime_fence(
                require_accepting=False
            ),
        )
        self._mgr.add_turn_terminal_listener(
            self._life_tools.on_harness_turn_terminal
        )

        # ── Durable Pulse engine ────────────────────────────────
        # Runtime polls Sensory before this tick so the engine never consumes
        # a non-durable channel directly.  The engine itself remains the
        # frozen durable causal consumer.
        keeper = self._lease_keeper
        if keeper is None:
            raise RuntimeError("Runtime lease keeper disappeared during startup")
        owner_lease = keeper.assert_owned()
        self._scheduler = DurableCenterScheduler(
            self._storage,
            world_id=self._world_id,
            owner_id=owner_lease.owner_id,
            lease_epoch=owner_lease.epoch,
            config=CenterSchedulingConfig(
                lane_reservation_per_tick=c.center_lane_reservation_per_tick,
                starvation_boost=c.center_starvation_boost,
                starvation_debt_cap=c.center_starvation_debt_cap,
                reservation_history_limit=c.center_reservation_history_limit,
            ),
        )
        self._engine = PulseEngine(
            storage=self._storage,
            engram_manager=self._mgr,
            connection_network=self._connections,
            dendrite=self._dendrite,
            runtime=self._runtime,
            metrics=self._metrics,
            claustrum=self._claustrum,
            sensory=None,
            causal_ledger=self._causal_ledger,
            world_id=self._world_id,
            spontaneous_factor=self._world.spontaneous_factor,
            spontaneous_center=self._spontaneous_center_for_engram,
            spontaneous_emitter=self._spontaneous_dispatch_for_engram,
            scheduler=self._scheduler,
            runtime_fence=self._current_runtime_fence(),
            config=PulseEngineConfig(
                tick_interval=c.tick_interval,
                budget_per_tick=c.budget_per_tick,
                propagation_threshold=c.propagation_threshold,
                base_spontaneous_rate=c.base_spontaneous_rate,
                inhibition_propagation_gate=c.inhibition_propagation_gate,
                topology_interval_ticks=c.topology_interval_ticks,
                connectivity_interval_ticks=c.connectivity_interval_ticks,
                max_parallel_pulses=c.pulse_worker_capacity,
                max_parallel_successions=max(
                    1,
                    min(4, c.pulse_worker_capacity),
                ),
                # Explicit mock is a compatibility/test Harness and keeps the
                # historical immediate tick return. Production Pi worlds use
                # the non-blocking coordinator path.
                background_dispatch=not c.mock,
            ),
        )

        # ── Tuning (contract §2.2) ───────────────────────────────
        self._tuning_lock = threading.Lock()
        self._defaults: dict[str, float] = {
            k: self._read_knob(k) for k in TUNING_KNOBS
        }
        self._commanded: dict[str, float | None] = {k: None for k in TUNING_KNOBS}
        self._pending: dict[str, float] = {}
        self._applied_at_tick: int | None = None
        self._will_apply_from_tick: int | None = None

        # ── Delegation registry (contract §2.3) ──────────────────
        self._deleg_lock = threading.Lock()
        self._delegations: dict[str, _Delegation] = {}
        self._futures: dict[str, Future] = {}
        self._executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="pulse-delegate"
        )
        self._reconcile_durable_delegations()

        self._metrics.record(
            "runtime_start",
            world=self._world_id,
            front=self._front_id,
            resumed=self._resumed,
            claustrum=self._claustrum is not None,
            router=self._router is not None,
            harness=self._harness_kind,
        )

    def _isolate_recovered_generation_state(
        self,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Terminate queued summary roots and hide unlinked crash candidates.

        The causal scheduler intentionally exposes no generic ``queued -> cancelled`` mutation.
        A queued generation summary can therefore be isolated through its
        existing turn lifecycle: claim the durable root without contacting Pi,
        then explicitly fail that turn.  This creates a visible failed fact,
        never a replay.  A deterministic candidate id lets startup archive a
        successor created before the generation row could persist its id.
        """

        summary_ids: list[str] = []
        orphan_ids: list[str] = []
        runtime_fence = self._current_runtime_fence(require_accepting=False)
        uncertain = self._causal_ledger.list_generations(state="uncertain")
        for generation in uncertain:
            queued = self._causal_ledger.list_events(
                parent_event_id=generation.event_id,
                status=CausalEventStatus.QUEUED,
                limit=500,
            ) if generation.event_id is not None else []
            for event in queued:
                if (
                    event.metadata.get("generation_id") != generation.id
                    or event.metadata.get("generation_stage") != "summary"
                    or event.engram_id is None
                ):
                    continue
                turn = self._causal_ledger.begin_turn(
                    event.id,
                    event.engram_id,
                    None,
                    runtime_fence=runtime_fence,
                )
                self._causal_ledger.fail_turn(
                    turn.id,
                    acceptance=False,
                    code="generation_recovered",
                    phase="generation_recovery",
                    retry_allowed=False,
                    runtime_fence=runtime_fence,
                )
                summary_ids.append(event.id)

            if generation.successor_id is None:
                candidate_id = EngramManager.generation_candidate_id(
                    generation.id
                )
                candidate = self._storage.get_engram(candidate_id)
                if candidate is not None and candidate.status in {
                    EngramStatus.ACTIVE,
                    EngramStatus.PROVISIONAL,
                }:
                    # No durable generation row points at this candidate. It
                    # is an orphan from the create→persist crash window, not a
                    # recoverable successor. Archive it so it cannot become a
                    # second active identity on restart.
                    self._storage.archive_engram(candidate_id)
                    orphan_ids.append(candidate_id)

        return tuple(summary_ids), tuple(orphan_ids)

    def _repair_recovered_continuity_identity(self) -> str | None:
        """Keep a durable named successor instead of creating a third identity."""

        state = self._storage.load_component_state(IDENTITY_COMPONENT) or {}
        predecessor_id = state.get("front_engram_id")
        if not isinstance(predecessor_id, str) or not predecessor_id.strip():
            return None
        predecessor = self._storage.get_engram(predecessor_id)
        if predecessor is None or predecessor.status is not EngramStatus.ARCHIVED:
            return None
        with self._storage._lock:
            rows = self._storage._conn.execute(
                "SELECT generation.successor_id FROM generation_transitions generation "
                "JOIN engrams successor ON successor.id = generation.successor_id "
                "WHERE generation.predecessor_id = ? "
                "AND generation.state IN ('uncertain', 'committed') "
                "AND successor.status = 'active' "
                "ORDER BY generation.updated_at DESC, generation.id",
                (predecessor_id,),
            ).fetchall()
        successor_ids = tuple(dict.fromkeys(row[0] for row in rows))
        if not successor_ids:
            return None
        if len(successor_ids) != 1:
            raise ServiceError(
                "generation_recovery_ambiguous",
                f"archived continuity Engram {predecessor_id} has multiple active successors",
                "repair generation history before starting the Runtime",
                status=500,
            )
        successor_id = successor_ids[0]
        self._persist_identity(successor_id)
        return successor_id

    # ── Accessors ────────────────────────────────────────────────

    def _signal_tick_stop(self) -> None:
        """Set the asyncio stop event without touching its loop cross-thread."""

        stop = self._stop
        loop = self._tick_loop
        if loop is not None and loop.is_running():
            try:
                running = asyncio.get_running_loop()
            except RuntimeError:
                running = None
            if running is not loop:
                try:
                    loop.call_soon_threadsafe(stop.set)
                except RuntimeError:
                    # A closed loop has no tick owner left to wake. Shutdown
                    # coordination must continue instead of losing the flight.
                    pass
                return
        try:
            stop.set()
        except RuntimeError:
            # The only remaining failure mode is an Event bound to a loop that
            # closed concurrently. The lifecycle fence is independent of the
            # tick wakeup and remains mandatory.
            pass

    def _observe_runtime_construction_owner(self) -> ShutdownComponentReport:
        done = self._runtime_construction_done.is_set()
        observed_at = _now()
        return component_report(
            "runtime_construction",
            effect=(
                ShutdownEffectState.SETTLED
                if done
                else ShutdownEffectState.UNCERTAIN
            ),
            owner=(
                ShutdownOwnerState.JOINED
                if done
                else ShutdownOwnerState.ESCAPED
            ),
            process_tree=ShutdownProcessTreeState.NOT_APPLICABLE,
            cancel=(
                ShutdownCancelState.NOT_NEEDED
                if done
                else ShutdownCancelState.SIGNALLED
            ),
            started_at=observed_at,
            started_monotonic=time.monotonic(),
            active_before=1,
            unresolved=0 if done else 1,
            error_code=None if done else "runtime_construction_inflight",
        )

    def _mark_runtime_construction_done(self) -> None:
        if self._runtime_construction_done.is_set():
            return
        self._runtime_construction_done.set()
        shutdown_trigger = self._shutdown_controller.primary_trigger
        if shutdown_trigger is not None:
            # A source may have attached after a lease-loss census was taken.
            # Reconcile it before publishing construction-final: that probe is
            # the finalizer's wake edge, so publishing it first would allow a
            # registry-seal race that permanently excludes the late source.
            if type(self._recovery_permit) is not RuntimeRecoveryPermit:
                self._revoke_publication(
                    reason=self._shutdown_reason(shutdown_trigger)
                )
            self._register_runtime_physical_sources(
                harness=getattr(self, "_harness", None),
                mcp_runtime=getattr(self, "_harness_mcp_service", None),
                reports={},
            )
        self._retained_owner_probes.publish(
            self._runtime_construction_probe,
            self._observe_runtime_construction_owner(),
        )
        if shutdown_trigger is not None:
            self._physical_convergence.seal_registrations()

    def _register_late_harness_shutdown(
        self,
        harness: HarnessRuntime,
    ) -> None:
        """Hand a factory result that arrived after terminal to the finalizer."""

        if type(harness) is PiHarnessRuntime:
            permit = self._recovery_permit
            if type(permit) is not RuntimeRecoveryPermit:
                raise RuntimeError(
                    "late Pi Harness lacks Runtime recovery authority"
                )
            self._physical_convergence.register_pi(
                harness,
                self._initial_retained_component_report("harness"),
                recovery_permit=permit,
            )
            return

        done = threading.Event()
        state: dict[str, Any] = {}
        task: dict[str, Any] = {
            "component": "harness",
            "done": done,
            "state": state,
            "started_at": _now(),
            "started_monotonic": time.monotonic(),
        }
        if type(harness) is PiHarnessRuntime:
            task["result_authority"] = "pi_harness_v1"

        observed_at = _now()
        probe = RuntimeRetainedOwnerProbe(
            "harness",
            component_report(
                "harness",
                effect=ShutdownEffectState.UNCERTAIN,
                owner=ShutdownOwnerState.ESCAPED,
                process_tree=ShutdownProcessTreeState.UNKNOWN,
                cancel=ShutdownCancelState.SIGNALLED,
                started_at=observed_at,
                started_monotonic=time.monotonic(),
                active_before=1,
                unresolved=1,
                error_code="shutdown_owner_unresolved",
            ),
        )

        def close_late_harness() -> None:
            try:
                state["result"] = self._close_harness_component(
                    harness,
                    deadline=ShutdownDeadline.after(
                        self._config.runtime_shutdown_timeout_sec
                    ),
                    after_publication_revoke=True,
                )
            except BaseException as exc:  # noqa: BLE001 - retained evidence
                state["error_type"] = type(exc).__name__.casefold()
            finally:
                done.set()
                try:
                    report = self._observe_shutdown_call(
                        task,
                        ShutdownDeadline.after(0.05),
                        process_tree=ShutdownProcessTreeState.UNKNOWN,
                    )
                    self._retained_owner_probes.publish(probe, report)
                except RuntimeError:
                    # A sealed registry means the finalizer already observed
                    # this task as physically complete in the same generation.
                    pass

        owner = threading.Thread(
            target=close_late_harness,
            name="shutdown-late-harness",
            daemon=True,
        )
        task["thread"] = owner
        self._retained_owner_probes.register(probe)
        owner.start()

    def _cleanup_startup_failure(
        self,
        harness: HarnessRuntime | None,
    ) -> None:
        """Route partial construction through the canonical shutdown flight."""

        if harness is not None and self._harness is None:
            # Register before publishing construction completion. A concurrent
            # lease-loss coordinator refreshes census only after this event.
            self._harness = harness
        self._mark_runtime_construction_done()
        try:
            self._request_runtime_shutdown(
                RuntimeShutdownTrigger.STARTUP_FAILURE,
                wait=True,
                harness_override=harness,
            )
        except Exception:  # noqa: BLE001 - preserve the constructor failure
            _logger.exception(
                "Canonical Runtime cleanup after startup failure failed"
            )
    def _on_runtime_lease_lost(self, error: RuntimeLeaseError) -> None:
        """Fail closed and wake the canonical coordinator without waiting."""

        self._runtime_lease_lost.set()
        self._quiescing = True
        self._signal_tick_stop()
        try:
            self._request_runtime_shutdown(
                RuntimeShutdownTrigger.LEASE_LOST,
                wait=False,
            )
        except Exception:  # noqa: BLE001 - callback must still return promptly
            _logger.exception(
                "Runtime lease-loss shutdown flight could not be started"
            )
            try:
                self._revoke_publication(reason="runtime_lease_lost")
            except Exception:  # noqa: BLE001 - final fail-closed attempt
                _logger.exception(
                    "Runtime publication revocation after lease loss failed"
                )
        _ = error
    def _runtime_lease_service_error(
        self,
        error: RuntimeLeaseError | None = None,
    ) -> ServiceError:
        keeper = self._lease_keeper
        health = keeper.health() if keeper is not None else None
        reason = (
            error.reason
            if error is not None
            else (
                health.lost_reason
                if health is not None and health.lost_reason is not None
                else "ownership_unavailable"
            )
        )
        return ServiceError(
            "runtime_lease_lost",
            f"this Runtime no longer owns the durable database ({reason})",
            "construct a new RuntimeService after the active owner lease is available",
            status=409,
        )

    def _require_runtime_owner(self) -> None:
        """Fence a new mutation before it touches durable organism state."""

        keeper = self._lease_keeper
        if keeper is None:
            raise self._runtime_lease_service_error()
        health = keeper.health()
        if not health.healthy:
            raise self._runtime_lease_service_error()
        if self._closed or self._quiescing:
            raise ServiceError(
                "runtime_quiescing",
                "this runtime is not accepting new work",
                "construct a new RuntimeService against the durable database",
                status=409,
            )
        try:
            self._require_publication_permit()
        except RuntimePublicationError as exc:
            self._quiescing = True
            raise ServiceError(
                "runtime_quiescing",
                "this runtime publication lifecycle has been revoked",
                "construct a new RuntimeService against the durable database",
                status=409,
            ) from exc
        try:
            keeper.assert_owned()
        except RuntimeLeaseError as exc:
            self._on_runtime_lease_lost(exc)
            raise self._runtime_lease_service_error(exc) from exc

    def _current_runtime_fence(
        self,
        *,
        require_accepting: bool = True,
    ) -> RuntimeFence:
        """Snapshot a proven lease epoch for one transaction-local fence.

        Shutdown recovery is allowed while this Runtime is quiescing, so it
        bypasses only the accepting-work check.  ``assert_owned`` still makes
        takeover or expiry fail closed before the fenced transaction begins.
        """

        if require_accepting:
            self._require_runtime_owner()
        keeper = self._lease_keeper
        if keeper is None:
            raise RuntimeError("Runtime lease keeper is unavailable")
        lease = keeper.assert_owned()
        permit = self._publication_permit
        if permit is None:
            raise RuntimeError("Runtime publication permit is unavailable")
        permit.assert_publication()
        return RuntimeFence(
            owner_id=lease.owner_id,
            epoch=lease.epoch,
            permit=permit,
        )

    def _require_publication_permit(self) -> None:
        permit = self._publication_permit
        if permit is None:
            raise RuntimeError("Runtime publication permit is unavailable")
        permit.assert_publication()

    def _revoke_publication(self, *, reason: str) -> RuntimeRecoveryPermit:
        gate = self._publication_gate
        if gate is None:
            raise RuntimeError("Runtime publication gate is unavailable")
        permit = gate.revoke(reason=reason)
        self._recovery_permit = permit
        return permit

    def _current_bootstrap_fence(self) -> RuntimeFence:
        """Return the takeover-only authority used before normal startup."""

        permit = self._bootstrap_permit
        if permit is None:
            raise RuntimeError("Runtime bootstrap permit is unavailable")
        keeper = self._lease_keeper
        if keeper is None:
            raise RuntimeError("Runtime lease keeper is unavailable")
        lease = keeper.assert_owned()
        permit.assert_bootstrap()
        return RuntimeFence(
            owner_id=lease.owner_id,
            epoch=lease.epoch,
            permit=permit,
        )

    def _current_recovery_fence(self) -> RuntimeFence:
        permit = self._recovery_permit
        if permit is None:
            permit = self._revoke_publication(reason="runtime_recovery")
        keeper = self._lease_keeper
        if keeper is None:
            raise RuntimeError("Runtime lease keeper is unavailable")
        lease = keeper.assert_owned()
        permit.assert_recovery()
        return RuntimeFence(
            owner_id=lease.owner_id,
            epoch=lease.epoch,
            permit=permit,
        )

    def _runtime_lease_view(self) -> dict[str, Any]:
        keeper = self._lease_keeper
        if keeper is None:
            return {
                "scope": "pulse_world",
                "owner_id": None,
                "epoch": None,
                "state": "unavailable",
                "healthy": False,
                "acquired_at": None,
                "renewed_at": None,
                "expires_at": None,
                "released_at": None,
                "lost_reason": "ownership_unavailable",
            }
        health = keeper.health()
        lease = health.lease
        return {
            "scope": lease.scope,
            "owner_id": lease.owner_id,
            "epoch": lease.epoch,
            "state": lease.state.value,
            "healthy": health.healthy,
            "acquired_at": lease.acquired_at.isoformat(),
            "renewed_at": lease.renewed_at.isoformat(),
            "expires_at": lease.expires_at.isoformat(),
            "released_at": (
                None if lease.released_at is None else lease.released_at.isoformat()
            ),
            "lost_reason": health.lost_reason,
        }

    def scheduling_snapshot(self) -> dict[str, Any]:
        """Canonical read model for Runtime ownership and Center admission."""

        scheduler = self._scheduler
        if scheduler is None:
            raise ServiceError(
                "scheduling_unavailable",
                "the durable Center scheduler is not mounted",
                "finish Runtime construction before reading scheduling state",
                status=503,
            )
        state = scheduler.scheduling_snapshot(
            history_limit=self._config.center_reservation_history_limit,
        )
        capacity = dict(state["capacity"])
        capacity["budget_per_tick"] = self._config.budget_per_tick
        capacity.update(self._engine.capacity_snapshot())
        harness_capacity = getattr(self._harness, "capacity_snapshot", None)
        if callable(harness_capacity):
            capacity.update(harness_capacity())
        else:
            capacity.update({
                "resident_limit": 0,
                "resident_sessions": 0,
                "starting_sessions": 0,
                "busy_sessions": 0,
            })
        if self._claustrum is None:
            capacity.update({
                "claustrum_mounted": False,
                "claustrum_slot_limit": 0,
                "claustrum_slot_used": 0,
                "claustrum_slot_available": 0,
                "claustrum_slot_utilization": None,
                "claustrum_last_requested": 0,
                "claustrum_last_overflow": 0,
            })
        else:
            capacity["claustrum_mounted"] = True
            capacity.update(self._claustrum.capacity_snapshot())
        return {
            "policy_version": state["policy_version"],
            "lease": self._runtime_lease_view(),
            "capacity": capacity,
            "failure_domains": self._engine.failure_domain_snapshot(),
            "lanes": list(state["lanes"]),
            "centers": list(state["centers"]),
            "reservations": list(state["reservations"]),
        }

    @property
    def config(self) -> RuntimeServiceConfig:
        return self._config

    @property
    def storage(self) -> Storage:
        return self._storage

    @property
    def causal_ledger(self) -> CausalLedger:
        return self._causal_ledger

    @property
    def gateway(self) -> PulseToolGateway:
        return self._gateway

    @property
    def habitat(self) -> ManagedHabitat:
        return self._habitat

    @property
    def sensory(self) -> SensoryCortex:
        return self._sensory

    @property
    def life_tools(self) -> LifeToolService:
        assert self._life_tools is not None
        return self._life_tools

    @property
    def role_leases(self) -> RoleLeaseStore:
        """Runtime-owned bounded role authority, exposed for read projections."""

        if self._role_lease_store is None:
            raise RuntimeError("role lease store is unavailable")
        return self._role_lease_store

    @property
    def task_offers(self) -> TaskOfferService:
        """The Runtime-owned TaskOfferService.

        Runtime owns this instance and injects the same object into Pi tools;
        API and subject decisions therefore cannot drift into two state
        machines.
        """

        if self._task_offers is None:
            raise RuntimeError("task offer service is unavailable")
        return self._task_offers

    @property
    def task_relationships(self) -> TaskRelationshipService:
        if self._task_relationships is None:
            raise RuntimeError("task relationship service is unavailable")
        return self._task_relationships

    @property
    def llm(self) -> LLMAdapter:
        return self._llm

    @property
    def harness(self) -> HarnessRuntime:
        return self._harness

    @property
    def engrams(self) -> EngramManager:
        return self._mgr

    @property
    def connections(self) -> ConnectionNetwork:
        return self._connections

    @property
    def online_learning_audit(self) -> dict[str, Any]:
        return self._online_learning_audit.snapshot(
            self._config.online_learning_policy
        )

    @property
    def engine(self) -> PulseEngine:
        return self._engine

    @property
    def dendrite(self) -> DendriteProcessor:
        return self._dendrite

    @property
    def resources(self) -> RuntimeManager:
        return self._runtime

    @property
    def claustrum(self) -> ClaustrumModulator | None:
        return self._claustrum

    @property
    def router(self) -> DelegationRouter | None:
        return self._router

    @property
    def delegator(self) -> Delegator | None:
        return self._delegator

    @property
    def delegation_tunnel(self) -> DurableDelegationTunnel:
        return self._delegation_tunnel

    @property
    def metrics(self) -> MetricsRecorder:
        return self._metrics

    @property
    def library(self) -> Library:
        return self._library

    @property
    def tools(self) -> ToolRegistry:
        return self._tools

    @property
    def harness_event_store(self) -> HarnessEventStore | None:
        """Redacted, bounded Harness replay projection for the observatory."""

        return self._harness_event_store

    @property
    def harness_control_gateway(self) -> _RuntimeHarnessControlGateway | None:
        """Epoch-fenced sideband interrupt/steer gateway for Workbench."""

        return self._harness_control_gateway

    @property
    def harness_action_broker(self) -> HarnessActionBroker | None:
        """Policy/approval seam for mutable Pi proxy tools."""

        return self._harness_action_broker

    @property
    def harness_sandbox_preflight(self) -> SandboxPreflight | None:
        """Callable-CLI evidence; availability is not an OS live claim."""

        return self._harness_sandbox_preflight

    @property
    def harness_terminal_sessions(self) -> TerminalSessionService | None:
        """Durable PIPE session service, present only behind both live gates."""

        return self._harness_terminal_sessions

    def _harness_evidence_class(self) -> str:
        if self._harness_kind == _HARNESS_KIND_MOCK:
            return "EXPLICIT_MOCK"
        return str(getattr(self._harness, "evidence_class", "LIVE_GATE_UNVERIFIED"))

    @property
    def world(self) -> WorldRegistry:
        """Durable fronts and life centers in this process-wide PulseWorld."""
        return self._world

    @property
    def world_id(self) -> str:
        return self._world_id

    @property
    def continuity_engram_id(self) -> str:
        return self._front_id

    @property
    def front_engram_id(self) -> str:
        """Deprecated alias for :attr:`continuity_engram_id`."""
        return self._front_id

    @property
    def resumed(self) -> bool:
        """True when this process picked up an existing front engram."""
        return self._resumed

    @property
    def tick_count(self) -> int:
        return self._engine.tick_count

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    # ── Gateway composition boundary ───────────────────────────

    def _dispatch_gateway_tool(
        self,
        engram_id: str,
        tool_name: str,
        args: dict[str, Any],
        invocation: ToolInvocationContext,
    ) -> dict[str, Any]:
        if self._closed or self._quiescing or self._life_tools is None:
            return {
                "ok": False,
                "content": "",
                "data": {},
                "event_id": None,
                "error": "runtime_quiescing",
            }
        return self._life_tools.dispatch(
            engram_id,
            tool_name,
            args,
            invocation,
        )

    def _authorize_builtin_tool(
        self,
        engram_id: str,
        tool_name: str,
        input_data: dict[str, Any],
    ) -> dict[str, Any]:
        if self._closed or self._quiescing or self._life_tools is None:
            return {"allow": False, "reason_code": "runtime_quiescing"}
        return self._life_tools.authorize(engram_id, tool_name, input_data)

    def _cancel_gateway_action(
        self,
        engram_id: str,
        invocation: ToolInvocationContext,
    ) -> dict[str, Any]:
        broker = self._harness_action_broker
        if broker is None:
            return {
                "ok": False,
                "content": "The Harness action broker is unavailable.",
                "data": {},
                "event_id": None,
                "error": "action_gateway_unavailable",
            }
        return broker.cancel_action(
            engram_id,
            invocation.tool_call_id,
            reason="pi_tool_abort",
        )

    def _revoke_task_relationship_execution(
        self,
        *,
        engram_id: str,
        relationship_id: str,
        relationship_revision: int,
        action: str,
        source_event_id: str,
    ) -> dict[str, Any]:
        """Fence and interrupt the turn whose subject withdrew task authority."""

        del source_event_id
        turn = self._causal_ledger.get_running_turn(engram_id)
        if turn is None:
            return {
                "state": "no_running_turn",
                "uncertain": False,
                "turn_id": None,
            }
        try:
            relationship = self.task_relationships.get(
                relationship_id
            ).relationship
        except TaskRelationshipError:
            relationship = None
        root = self._causal_ledger.get_event(turn.event_id)
        if relationship is not None and root is not None:
            root_metadata = (
                root.metadata if isinstance(root.metadata, Mapping) else {}
            )
            targets_relationship = (
                root.center_id == relationship.center_id
                or (
                    root.center_id is None
                    and root_metadata.get("task_relationship_id")
                    == relationship.id
                )
            )
            if not targets_relationship:
                return {
                    "state": "unrelated_running_turn",
                    "uncertain": False,
                    "turn_id": turn.id,
                    "target_center_id": relationship.center_id,
                    "running_center_id": root.center_id,
                }
        gateway = self._harness_control_gateway
        keeper = self._lease_keeper
        if gateway is None or keeper is None:
            return {
                "state": "uncertain",
                "uncertain": True,
                "turn_id": turn.id,
                "error_code": "task_relationship_revocation_unavailable",
            }
        try:
            epoch = keeper.assert_owned().epoch
            result = gateway.request_control(
                "interrupt",
                turn.id,
                {
                    "request_id": (
                        "task-relationship-revoke:"
                        f"{relationship_id}:{relationship_revision}:{action}"
                    ),
                    "expected_epoch": epoch,
                    "expected_state": turn.state.value,
                },
            )
        except _HarnessControlError as exc:
            return {
                "state": "uncertain",
                "uncertain": True,
                "turn_id": turn.id,
                "error_code": exc.code,
            }
        return {
            "state": result.get("state", "accepted"),
            "uncertain": bool(result.get("uncertain", True)),
            "turn_id": turn.id,
            "cancelled_actions": result.get("cancelled_actions"),
            "terminal_sessions": result.get("terminal_sessions"),
            "task_workers": result.get("task_workers"),
        }

    def _harness_action_epoch(self) -> int:
        """Return the lease epoch only after proving current ownership."""

        self._require_runtime_owner()
        keeper = self._lease_keeper
        if keeper is None:
            raise RuntimeError("runtime lease keeper unavailable")
        return keeper.assert_owned().epoch

    def _recover_harness_operations(self) -> tuple[dict[str, Any], ...]:
        """Terminalize orphaned adapter operations before dispatch opens.

        Recovery never replays an adapter.  A newer Runtime can prove that a
        pre-boundary row did not start and cancels it; any row that may have
        crossed the boundary is permanently ``UNCERTAIN`` until an operator
        reconciles its external effect.  A canonical terminal event is then
        appended and bound back to the operation ledger.  Append failure
        leaves ``recovery_state=required`` for the next successor.
        """

        ledger = self._harness_operation_ledger
        store = self._harness_event_store
        keeper = self._lease_keeper
        if ledger is None or store is None or keeper is None:
            return ()
        lease = keeper.assert_owned()
        recovered: list[dict[str, Any]] = []
        operations = ledger.list_recovery(limit=500)
        for operation in operations:
            current = operation
            try:
                if not operation.is_terminal:
                    if (
                        operation.owner_id == lease.owner_id
                        or operation.requested_epoch >= lease.epoch
                    ):
                        # This cannot be safely declared orphaned by the same
                        # or an older owner.  Keep it fenced and visible.
                        recovered.append(
                            {
                                "operation_kind": operation.operation_kind,
                                "operation_id": operation.operation_id,
                                "terminal_state": None,
                                "recovery_state": "required",
                                "error_code": "operation_recovery_cas_blocked",
                            }
                        )
                        continue
                    current = ledger.claim_recovery(
                        operation.operation_kind,
                        operation.operation_id,
                        successor_owner_id=lease.owner_id,
                        successor_epoch=lease.epoch,
                        expected_prior_owner_id=operation.owner_id,
                        expected_prior_epoch=operation.requested_epoch,
                    )
                if current.terminal_event_id is None:
                    uncertain = (
                        current.terminal_state is OperationTerminalState.UNCERTAIN
                    )
                    status = (
                        HarnessEventStatus.UNCERTAIN
                        if uncertain
                        else HarnessEventStatus.CANCELLED
                    )
                    tool_name = (
                        current.operation_kind.removeprefix("tool.")[:64]
                        if current.operation_kind.startswith("tool.")
                        else current.operation_kind[:64]
                    )
                    event = store.append(
                        HarnessEventDraft(
                            turn_id=current.turn_id,
                            world_id=current.world_id,
                            engram_id=current.engram_id,
                            kind=HarnessEventKind.TOOL_COMPLETED,
                            phase=HarnessEventPhase.TERMINAL,
                            source=HarnessEventSource.PULSE_CONTROL,
                            status=status,
                            payload={
                                "action_request_id": current.operation_id,
                                "tool_name": tool_name,
                                "epoch": current.requested_epoch,
                                "recovered_by_epoch": lease.epoch,
                                "execution_status": (
                                    "uncertain" if uncertain else "cancelled"
                                ),
                                "recovery_state": "recovered",
                                "error_code": (
                                    "runtime_restart_after_adapter_boundary"
                                    if uncertain
                                    else "runtime_restart_before_adapter_boundary"
                                ),
                                "evidence_class": "LIVE_GATE_UNVERIFIED",
                            },
                        )
                    )
                    current = ledger.bind_terminal_event(
                        current.operation_kind,
                        current.operation_id,
                        terminal_event_id=event.event_id,
                    )
                recovered.append(
                    {
                        "operation_kind": current.operation_kind,
                        "operation_id": current.operation_id,
                        "terminal_state": (
                            None
                            if current.terminal_state is None
                            else current.terminal_state.value
                        ),
                        "recovery_state": current.recovery_state.value,
                        "terminal_event_id": current.terminal_event_id,
                    }
                )
            except Exception:  # noqa: BLE001
                recovered.append(
                    {
                        "operation_kind": operation.operation_kind,
                        "operation_id": operation.operation_id,
                        "terminal_state": None,
                        "recovery_state": "required",
                        "error_code": "operation_recovery_failed",
                    }
                )
                _logger.exception(
                    "Harness operation recovery failed for %s/%s",
                    operation.operation_kind,
                    operation.operation_id,
                )
        return tuple(recovered)

    # ── Harness sideband and persistence ───────────────────────

    def _persist_harness_bindings(self, snapshot: dict[str, Any]) -> None:
        """Atomically replace the complete v1 binding component."""
        fence = self._current_runtime_fence()
        self._storage.save_component_state_fenced(
            BINDING_COMPONENT,
            snapshot,
            runtime_owner_id=fence.owner_id,
            runtime_lease_epoch=fence.epoch,
            now=_now(),
        )

    def _record_harness_metric(
        self,
        event_type: str,
        fields: dict[str, Any],
    ) -> None:
        """Forward only the frozen sideband vocabulary into MetricsRecorder."""
        safe = {
            key: value
            for key, value in fields.items()
            if key in _SAFE_HARNESS_METRIC_FIELDS
        }
        self._metrics.record(event_type, **safe)

    def _observe_harness_event(self, event: HarnessEvent) -> None:
        """Classify every Harness projection as control-only at append time.

        This callback has no reference to ``CausalLedger.enqueue``.  Even a
        LIVE approval, worker result, replay marker, or manager message can
        therefore be observed and audited without becoming content,
        learning evidence, spontaneous activity, or role-renewal evidence.
        """

        try:
            firewall = self._stimulus_firewall
            if firewall is None:
                raise RuntimeError("stimulus firewall is unavailable")
            lineage_id = None
            governance = self._purpose_governance
            if governance is not None:
                lineage = governance.find_lineage_for_engram(event.engram_id)
                lineage_id = None if lineage is None else lineage.lineage_id
            envelope = StimulusEnvelope.control_event(
                f"harness:{event.event_id}",
                event_id=event.event_id,
                source_id="harness-event-store",
                stimulus_class=StimulusClass.CONTROL_OBSERVATION,
                mode=ProvenanceMode.CONTROL_OBSERVATION,
                evidence_class=StimulusEvidenceClass.LIVE,
                target_lineage_id=lineage_id,
            )
            decision = firewall.evaluate(envelope)
            if decision.route is not DecisionRoute.CONTROL_LEDGER:
                raise RuntimeError("Harness control observation crossed into the life queue")
            self._persist_stimulus_control_decision(decision)
            self._stimulus_observer_health = (
                "healthy"
                if self._stimulus_observer_failures == 0
                else "recovered"
            )
            self._stimulus_observer_last_error = None
        except Exception as exc:  # noqa: BLE001 - effect evidence must stay readable
            # Harness events may represent an effect which has already crossed
            # an external boundary.  Observation failure therefore cannot
            # roll the event back or request a replay.  It is made explicit in
            # runtime health instead of being silently swallowed.
            self._stimulus_observer_health = "degraded"
            self._stimulus_observer_failures += 1
            self._stimulus_observer_last_error = type(exc).__name__
            self._record_harness_metric(
                "harness_control_audit_failed",
                {"error_type": type(exc).__name__},
            )

    def _persist_stimulus_control_decision(self, decision: Any) -> None:
        """Persist one rejected/control-only firewall decision without payload."""

        if decision.route is not DecisionRoute.CONTROL_LEDGER:
            raise RuntimeError("only control-ledger decisions use the control audit")
        self._stimulus_control_audit = self._storage.append_harness_control_observation(
            world_id=self._world_id,
            record_id=decision.decision_digest,
            stimulus_id=decision.stimulus_id,
            stimulus_class=decision.stimulus_class.value,
            declared_class=decision.declared_class.value,
            evidence_class=decision.evidence_class.value,
            route=decision.route.value,
            reason_code=decision.reason_code,
            provenance_digest=decision.provenance_digest,
            external_effect_id=decision.external_effect_id,
            max_records=4096,
        )

    def _record_harness_event(
        self,
        engram_id: str,
        turn_id: str | None,
        event: dict[str, Any],
    ) -> None:
        """Forward Pi observations through the redacting event projection.

        The event store owns schema, sequence allocation and redaction.  This
        boundary intentionally does not log or persist the raw mapping when
        the projection is unavailable, so a startup race cannot turn a
        provider payload into an unbounded side channel.
        """

        if turn_id is None:
            return
        projector = self._harness_event_projector
        if projector is None:
            return
        append = getattr(projector, "append_observation", None)
        if not callable(append):
            return
        try:
            append(engram_id, turn_id, event)
        except Exception as exc:  # noqa: BLE001 - observation is non-critical
            self._record_harness_metric(
                "harness_event_store_failed",
                {"error_type": type(exc).__name__},
            )

    def _harness_summary(self) -> dict[str, Any]:
        if self._harness_kind == _HARNESS_KIND_MOCK:
            return {
                "kind": _HARNESS_KIND_MOCK,
                "live_sessions": 0,
                "bindings": 0,
                "states": {},
            }

        binding_snapshot = self._harness.binding_snapshot()  # type: ignore[attr-defined]
        sessions = binding_snapshot.get("sessions", {})
        states: dict[str, int] = {}
        live_sessions = 0
        for engram_id in sorted(sessions):
            try:
                state = str(self._harness.snapshot(engram_id).get("state", "UNKNOWN"))
            except HarnessError:
                state = "UNKNOWN"
            states[state] = states.get(state, 0) + 1
            if state not in {"UNBOUND", "CLOSED", "UNKNOWN"}:
                live_sessions += 1
        return {
            "kind": _HARNESS_KIND_PI,
            "live_sessions": live_sessions,
            "bindings": len(sessions),
            "states": states,
            "mcp_registry": (
                {
                    "attached": False,
                    "descriptors": 0,
                    "enabled": 0,
                    "supported_transports": [],
                    "evidence_class": "CONTRACT",
                    "live_gate": "LIVE_GATE_UNVERIFIED",
                }
                if self._harness_mcp_registry_gate is None
                else dict(self._harness_mcp_registry_gate.summary())
            ),
        }

    # ── World identity and legacy continuity (contract §5) ─────

    def _resume_or_create_front(self) -> tuple[str, bool]:
        """Resume the stored front engram, or create one and store it.

        A stored id whose engram is gone or archived is not resurrected — it is
        replaced and the replacement is stored. Silently pulsing an archived
        session would be worse than starting over.
        """
        state = self._storage.load_component_state(IDENTITY_COMPONENT) or {}
        stored = state.get("front_engram_id")
        if stored:
            engram = self._storage.get_engram(stored)
            if engram is not None and engram.status == EngramStatus.ACTIVE:
                _logger.info("resumed front engram %s", stored)
                return stored, True
            _logger.warning(
                "stored front engram %s is %s; starting a new one",
                stored, "missing" if engram is None else engram.status.value,
            )

        engram = self._mgr.create(initial_messages=None, auto_name=False)
        self._storage.update_engram_metadata(
            engram.id, self_excitability=0.0
        )
        self._persist_identity(engram.id)
        _logger.info("created front engram %s", engram.id)
        return engram.id, False

    def _resume_or_create_world(self) -> tuple[str, str, bool]:
        """Resume the one PulseWorld and migrate the historical single Front.

        A pre-v1 database has ``runtime_service.front_engram_id`` but no world
        component.  That old conversation becomes a visible system-origin
        TaskFront exactly once.  A fresh v1 database keeps its continuity
        anchor invisible and empty.
        """

        state = self._storage.load_component_state(WORLD_COMPONENT)
        if state is not None:
            if not isinstance(state, dict) or state.get("version") != 1:
                raise ServiceError(
                    "invalid_world_state",
                    f"{WORLD_COMPONENT} is not a supported v1 component",
                    "restore a valid component-state backup or migrate it to v1",
                    status=500,
                )
            world_id = state.get("world_id")
            created_at = state.get("created_at")
            if not isinstance(world_id, str) or not world_id.strip():
                raise ServiceError(
                    "invalid_world_state",
                    "the persisted world_id is missing",
                    "restore a valid component-state backup",
                    status=500,
                )
            if not isinstance(created_at, str) or not created_at.strip():
                raise ServiceError(
                    "invalid_world_state",
                    "the persisted PulseWorld creation time is missing",
                    "restore a valid component-state backup",
                    status=500,
                )
            migrated = bool(state.get("legacy_front_migrated", False))
            if not migrated and self._resumed:
                self._migrate_legacy_front(world_id)
                migrated = True
            elif migrated and self._resumed:
                # Databases migrated by an earlier release already have the
                # legacy TaskFront but not its Center-scoped causal projection.
                # Repair only an existing legacy Front: a fresh v1 world's
                # intentionally silent compatibility anchor must stay hidden.
                legacy_fronts = self._world.list_task_fronts(
                    focal_engram_id=self._front_id
                )
                if legacy_fronts:
                    front = legacy_fronts[0]
                    center = self._world.get_activity_center(front.center_id)
                    if center is None:
                        raise ServiceError(
                            "legacy_front_broken",
                            f"legacy TaskFront {front.id} has no ActivityCenter",
                            "restore the legacy Front and Center from one backup",
                            status=500,
                        )
                    self._project_legacy_front_messages(world_id, front, center)
            self._persist_world(
                world_id,
                created_at,
                legacy_front_migrated=migrated,
            )
            self._metrics.record(
                "world_ready",
                world=world_id,
                continuity_engram=self._front_id,
                resumed=True,
                legacy_front_migrated=migrated,
            )
            return world_id, created_at, migrated

        world_id = uuid.uuid4().hex
        created_at = _iso()
        # ``resumed`` here means the identity pre-dated this world component.
        # That is the only case in which there is a historical Front to expose.
        migrated = not self._resumed
        if self._resumed:
            self._migrate_legacy_front(world_id)
            migrated = True
        self._persist_world(
            world_id,
            created_at,
            legacy_front_migrated=migrated,
        )
        self._metrics.record(
            "world_ready",
            world=world_id,
            continuity_engram=self._front_id,
            resumed=False,
            legacy_front_migrated=migrated,
        )
        return world_id, created_at, migrated

    def _migrate_legacy_front(self, world_id: str) -> None:
        """Make a pre-PulseWorld conversation visible without duplicating it."""

        existing_fronts = self._world.list_task_fronts(
            focal_engram_id=self._front_id
        )
        if existing_fronts:
            front = existing_fronts[0]
            center = self._world.get_activity_center(front.center_id)
            if center is None:
                raise ServiceError(
                    "legacy_front_broken",
                    f"legacy TaskFront {front.id} has no ActivityCenter",
                    "restore the legacy Front and Center from one backup",
                    status=500,
                )
            self._project_legacy_front_messages(world_id, front, center)
            return

        engram = self._storage.get_engram(self._front_id)
        if engram is None:
            raise ServiceError(
                "legacy_front_missing",
                f"continuity Engram {self._front_id} disappeared during migration",
                "restore the Engram before starting this runtime",
                status=500,
            )
        title = engram.name or "Legacy front"
        centers = [
            center
            for center in self._world.list_activity_centers(
                kind=ActivityKind.TASK,
                engram_id=self._front_id,
            )
            if center.focal_engram_id == self._front_id
        ]
        if centers:
            center = centers[0]
        else:
            center = self._world.create_center_for_existing_engram(
                ActivityKind.TASK,
                title,
                self._front_id,
                origin=ActivityOrigin.SYSTEM,
                center_id=uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"pulse-world:legacy-center:{self._front_id}",
                ).hex[:16],
            ).center
        front = self._world.create_task_front_for_center(
            center.id,
            self._front_id,
            title,
            front_id=uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"pulse-world:legacy-front:{self._front_id}",
            ).hex[:16],
        )
        self._project_legacy_front_messages(world_id, front, center)

    def _project_legacy_front_messages(
        self,
        world_id: str,
        front: TaskFront,
        center: ActivityCenter,
    ) -> None:
        """Import the explicitly migrated legacy conversation as causal facts.

        The pre-PulseWorld continuity Engram was itself the sole Front, so its
        existing message index has an explicit task attribution.  Importing it
        once at the migration boundary preserves that contract without making
        TaskFront GET fall back to a full identity-session read.  These rows
        are already-settled history and can never become scheduler inputs.
        """

        messages = self._storage.get_session(front.focal_engram_id)
        if not messages:
            return
        inserted = 0
        root_event_id: str | None = None
        ledger = self._causal_ledger
        with ledger._transaction() as conn:
            for index, message in enumerate(messages):
                event_id = uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"pulse-world:legacy-message:{front.id}:{index}",
                ).hex
                is_assistant = message.role is MessageRole.ASSISTANT
                is_legacy_injection = message.role is MessageRole.INJECTION
                parent_event_id = root_event_id if is_assistant else None
                causal_id = parent_event_id or event_id
                kind = (
                    CausalEventKind.ASSISTANT_RESULT
                    if is_assistant
                    else CausalEventKind.STIMULUS
                )
                source = (
                    CausalEventSource.SELF
                    if is_assistant
                    else (
                        CausalEventSource.SYSTEM
                        if is_legacy_injection
                        else CausalEventSource.USER
                    )
                )
                metadata: dict[str, Any] = {
                    "reason_code": "legacy_front_migration",
                    "message_index": index,
                    "legacy_message_role": message.role.value,
                }
                if message.source_engram_id is not None:
                    metadata["source_engram_id"] = message.source_engram_id
                event = CausalEvent(
                    id=event_id,
                    world_id=world_id,
                    causal_id=causal_id,
                    parent_event_id=parent_event_id,
                    engram_id=front.focal_engram_id,
                    center_id=center.id,
                    flow=None if is_assistant else CausalEventFlow.CONTENT,
                    domain=(
                        CausalEventDomain.HARNESS
                        if is_assistant
                        else CausalEventDomain.PULSE
                    ),
                    kind=kind,
                    source=source,
                    status=CausalEventStatus.SETTLED,
                    content=message.content,
                    metadata=metadata,
                    idempotency_key=f"legacy-front-message:{front.id}:{index}",
                    attempts=0,
                    created_at=message.timestamp,
                    updated_at=message.timestamp,
                    started_at=message.timestamp,
                    settled_at=message.timestamp,
                )
                existing = ledger._get_event_uncommitted(conn, event_id)
                if existing is None:
                    ledger._ensure_references(
                        conn,
                        front.focal_engram_id,
                        center.id,
                    )
                    ledger._insert_event_uncommitted(conn, event)
                    inserted += 1
                else:
                    # Releases before causal-flow-boundary.v1 projected a
                    # legacy MessageRole.INJECTION as an orphan propagation.
                    # It has no durable parent to name, so new imports use an
                    # explicit SYSTEM stimulus root.  Existing rows remain
                    # immutable audit evidence and are accepted here only in
                    # that exact historical shape; amplification reports the
                    # missing-parent violation instead of silently repairing it.
                    historical_orphan_propagation = (
                        is_legacy_injection
                        and existing.kind is CausalEventKind.PROPAGATION
                        and existing.source is CausalEventSource.PROPAGATION
                        and existing.flow is CausalEventFlow.CONTENT
                        and existing.parent_event_id is None
                        and existing.metadata.get("reason_code")
                        == "legacy_front_migration"
                    )
                    canonical_projection = (
                        existing.kind is event.kind
                        and existing.source is event.source
                        and existing.flow is event.flow
                    )
                    if (
                        existing.world_id != event.world_id
                        or existing.engram_id != event.engram_id
                        or existing.center_id != event.center_id
                        or existing.parent_event_id != event.parent_event_id
                        or not (
                            canonical_projection
                            or historical_orphan_propagation
                        )
                        or existing.status is not CausalEventStatus.SETTLED
                        or existing.content != event.content
                    ):
                        raise ServiceError(
                            "legacy_front_projection_conflict",
                            "legacy TaskFront history conflicts with its causal projection",
                            "restore one consistent pre-PulseWorld database backup",
                            status=500,
                        )
                if not is_assistant:
                    root_event_id = event_id
                elif root_event_id is None:
                    root_event_id = event_id
        if inserted:
            self._metrics.record(
                "legacy_front_messages_projected",
                world=world_id,
                front=front.id,
                center=center.id,
                count=inserted,
            )

    def _persist_world(
        self,
        world_id: str,
        created_at: str,
        *,
        legacy_front_migrated: bool,
    ) -> None:
        self._storage.save_component_state(WORLD_COMPONENT, {
            "version": 1,
            "world_id": world_id,
            "continuity_engram_id": self._front_id,
            "created_at": created_at,
            "legacy_front_migrated": legacy_front_migrated,
        })

    def _persist_identity(self, engram_id: str) -> None:
        self._storage.save_component_state(IDENTITY_COMPONENT, {
            "front_engram_id": engram_id,
            "updated_at": _iso(),
        })

    def _role_runtime_proof(self) -> RoleRuntimeLeaseProof:
        self._require_runtime_owner()
        keeper = self._lease_keeper
        if keeper is None:
            raise RuntimeError("Runtime lease keeper is unavailable")
        lease = keeper.assert_owned()
        return RoleRuntimeLeaseProof(
            world_id=self._world_id,
            owner_id=lease.owner_id,
            epoch=lease.epoch,
        )

    def _ensure_subject_lineage(self, engram_id: str) -> str:
        """Return one durable lineage id for the active Engram."""

        governance = self._purpose_governance
        if governance is None:
            raise RuntimeError("purpose governance is unavailable")
        existing = governance.find_lineage_for_engram(engram_id)
        if existing is not None:
            if existing.world_id != self._world_id:
                raise RuntimeError("subject lineage belongs to another world")
            return existing.lineage_id
        lineage_id = "lineage_" + uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"pulse-subject-lineage:{self._world_id}:{engram_id}",
        ).hex
        try:
            lineage = governance.create_lineage(
                lineage_id,
                world_id=self._world_id,
                root_engram_id=engram_id,
                current_engram_id=engram_id,
            )
        except PurposeLineageConflictError:
            lineage = governance.require_lineage(lineage_id)
            if lineage.current_engram_id != engram_id:
                raise RuntimeError("subject lineage creation collided with another holder")
        return lineage.lineage_id

    def _task_offer_ingress_provenance(
        self,
        engram_id: str,
        content: str,
        *,
        operation: str,
        offer_id: str | None = None,
        revision: int | None = None,
    ) -> str:
        """Authorize proposed terms as live user content without enqueueing it.

        TaskOfferService owns the transaction which stores the offer and its
        deliberation root.  Runtime still owns the typed ingress boundary, so
        it evaluates the exact user terms first and passes only immutable
        provenance into that transaction.  This method never creates a
        CausalEvent on its own.
        """

        lineage_id = self._ensure_subject_lineage(engram_id)
        identity = _stable_digest(
            {
                "world_id": self._world_id,
                "engram_id": engram_id,
                "operation": operation,
                "offer_id": offer_id,
                "revision": revision,
                "ingress_nonce": uuid.uuid4().hex,
            }
        )[:32]
        envelope = StimulusEnvelope.user_input(
            f"task_offer_{identity}",
            user_id="local-user",
            event_id=f"task_offer_ingress_{identity}",
            target_lineage_id=lineage_id,
            content=content,
            evidence_class=StimulusEvidenceClass.LIVE,
            flow=CausalEventFlow.CONTENT.value,
        )
        firewall = self._stimulus_firewall
        if firewall is None:
            raise ServiceError(
                "stimulus_firewall_unavailable",
                "the life/control boundary is unavailable",
                "restart the owning Runtime before proposing a task",
                status=503,
            )
        decision = firewall.evaluate(envelope)
        if not decision.life_queue_eligible:
            try:
                self._persist_stimulus_control_decision(decision)
            except Exception as exc:
                raise ServiceError(
                    "stimulus_audit_unavailable",
                    "the rejected task offer could not be durably audited",
                    "repair the payload-free control audit before retrying",
                    status=503,
                ) from exc
            raise ServiceError(
                "stimulus_not_eligible",
                "the proposed task terms were routed to control only "
                f"({decision.reason_code})",
                "supply live user-authored task terms",
                status=403,
            )
        return envelope.provenance.digest

    def _advance_subject_governance(self, old_id: str, new_id: str) -> None:
        """Move purpose identity and bounded subject roles across succession."""

        governance = self._purpose_governance
        role_store = self._role_lease_store
        if governance is None or role_store is None:
            raise RuntimeError("subject governance is unavailable during succession")
        lineage = governance.find_lineage_for_engram(old_id)
        if lineage is None:
            return
        advanced = governance.succeed_lineage(
            lineage.lineage_id,
            successor_engram_id=new_id,
            expected_current_engram_id=old_id,
            expected_generation=lineage.generation,
        )
        proof = self._role_runtime_proof()
        roles = role_store.list(
            world_id=self._world_id,
            lineage_id=advanced.lineage_id,
            status=(RoleLeaseStatus.ACTIVE, RoleLeaseStatus.SUSPENDED),
            limit=100,
        )
        for role in roles:
            if role.role_class is not RoleClass.SUBJECT_ROLE:
                continue
            if role.holder_kind is not HolderKind.ENGRAM or role.holder_id != old_id:
                raise RuntimeError("subject role holder diverged from lineage succession")
            if role.status is RoleLeaseStatus.ACTIVE:
                role_store.handoff(
                    role.role_lease_id,
                    expected_role_epoch=role.role_epoch,
                    new_holder_kind=HolderKind.ENGRAM,
                    new_holder_id=new_id,
                    new_lineage_id=advanced.lineage_id,
                    runtime=proof,
                )
            else:
                # Suspension is not evidence for automatically resuming a
                # role in the successor. Release its scope; the subject may
                # explicitly accept a new bounded role later.
                role_store.revoke(
                    role.role_lease_id,
                    expected_role_epoch=role.role_epoch,
                    runtime=proof,
                )

    def _follow_succession(self, old_id: str, new_id: str) -> None:
        """A generational turnover of the front engram is continuation.

        Without this the persisted id would point at an archived predecessor
        and the next start would open a brand-new session — the thought would
        die of the very mechanism designed to keep it alive.
        """
        if old_id != self._front_id:
            return
        self._front_id = new_id
        self._persist_identity(new_id)
        self._persist_world(
            self._world_id,
            self._world_created_at,
            legacy_front_migrated=self._legacy_front_migrated,
        )
        self._metrics.record("front_succession", old=old_id, new=new_id)
        _logger.info("front engram succeeded: %s -> %s", old_id, new_id)

    # ── TaskFronts and life ActivityCenters ─────────────────────

    @staticmethod
    def _activity_center_view(center: ActivityCenter) -> dict[str, Any]:
        return {
            "id": center.id,
            "kind": center.kind.value,
            "title": center.title,
            "description": center.description,
            "status": center.status.value,
            "origin": center.origin.value,
            "autonomy": center.autonomy,
            "project_id": center.project_id,
            "focal_engram_id": center.focal_engram_id,
            "created_at": center.created_at.isoformat(),
            "updated_at": center.updated_at.isoformat(),
            "last_active_at": (
                center.last_active_at.isoformat()
                if center.last_active_at is not None
                else None
            ),
        }

    @staticmethod
    def _task_front_view(front: TaskFront) -> dict[str, Any]:
        return {
            "id": front.id,
            "center_id": front.center_id,
            "focal_engram_id": front.focal_engram_id,
            "title": front.title,
            "status": front.status.value,
            "created_at": front.created_at.isoformat(),
            "updated_at": front.updated_at.isoformat(),
            "last_opened_at": front.last_opened_at.isoformat(),
        }

    @staticmethod
    def _task_offer_view(offer: TaskOffer) -> dict[str, Any]:
        return {
            "id": offer.id,
            "world_id": offer.world_id,
            "subject_engram_id": offer.subject_engram_id,
            "status": offer.status.value,
            "current_revision": offer.current_revision,
            "task_front_id": offer.task_front_id,
            "created_at": offer.created_at.isoformat(),
            "updated_at": offer.updated_at.isoformat(),
            "decided_at": (
                None if offer.decided_at is None else offer.decided_at.isoformat()
            ),
            "withdrawn_at": (
                None
                if offer.withdrawn_at is None
                else offer.withdrawn_at.isoformat()
            ),
        }

    @staticmethod
    def _task_offer_revision_view(
        revision: TaskOfferRevision,
    ) -> dict[str, Any]:
        return {
            "offer_id": revision.offer_id,
            "revision": revision.revision,
            "content": revision.content,
            "title": revision.title,
            "project_id": revision.project_id,
            "latest_offer_event_id": revision.latest_offer_event_id,
            "decision": (
                None if revision.decision is None else revision.decision.value
            ),
            "subject_response": revision.subject_response,
            "decision_event_id": revision.decision_event_id,
            "created_at": revision.created_at.isoformat(),
            "decided_at": (
                None
                if revision.decided_at is None
                else revision.decided_at.isoformat()
            ),
        }

    @classmethod
    def _task_offer_snapshot_view(
        cls,
        snapshot: TaskOfferSnapshot,
    ) -> dict[str, Any]:
        return {
            "task_offer": cls._task_offer_view(snapshot.offer),
            "current_revision": cls._task_offer_revision_view(
                snapshot.current_revision
            ),
            "revisions": [
                cls._task_offer_revision_view(revision)
                for revision in snapshot.revisions
            ],
        }

    @staticmethod
    def _task_relationship_view(
        relationship: TaskRelationship,
    ) -> dict[str, Any]:
        return {
            "id": relationship.id,
            "world_id": relationship.world_id,
            "accepted_offer_id": relationship.accepted_offer_id,
            "task_front_id": relationship.task_front_id,
            "center_id": relationship.center_id,
            "original_subject_engram_id": (
                relationship.original_subject_engram_id
            ),
            "current_subject_engram_id": relationship.current_subject_engram_id,
            "status": relationship.status.value,
            "revision": relationship.revision,
            "latest_terms_event_id": relationship.latest_terms_event_id,
            "latest_subject_note": relationship.latest_subject_note,
            "created_at": relationship.created_at.isoformat(),
            "updated_at": relationship.updated_at.isoformat(),
            "exited_at": (
                None
                if relationship.exited_at is None
                else relationship.exited_at.isoformat()
            ),
        }

    @staticmethod
    def _task_relationship_event_view(
        event: TaskRelationshipEvent,
    ) -> dict[str, Any]:
        return {
            "relationship_id": event.relationship_id,
            "seq": event.seq,
            "action": event.action.value,
            "actor_kind": event.actor_kind.value,
            "actor_id": event.actor_id,
            "before_status": (
                None if event.before_status is None else event.before_status.value
            ),
            "after_status": event.after_status.value,
            "content": event.content,
            "source_event_id": event.source_event_id,
            "created_at": event.created_at.isoformat(),
        }

    @classmethod
    def _task_relationship_snapshot_view(
        cls,
        snapshot: TaskRelationshipSnapshot,
    ) -> dict[str, Any]:
        return {
            "task_relationship": cls._task_relationship_view(
                snapshot.relationship
            ),
            "relationship_events": [
                cls._task_relationship_event_view(event)
                for event in snapshot.events
            ],
        }

    @staticmethod
    def _membership_view(membership: CenterMembership) -> dict[str, Any]:
        return {
            "center_id": membership.center_id,
            "engram_id": membership.engram_id,
            "relation": membership.relation.value,
            "created_at": membership.created_at.isoformat(),
        }

    @staticmethod
    def _engram_view(engram: Engram) -> dict[str, Any]:
        return {
            "id": engram.id,
            "project_id": engram.project_id,
            "status": engram.status.value,
            "created_at": engram.created_at.isoformat(),
            "last_pulse_at": (
                engram.last_pulse_at.isoformat()
                if engram.last_pulse_at is not None
                else None
            ),
            "total_pulses": engram.total_pulses,
            "name": engram.name,
            "name_origin": engram.name_origin,
            "nickname": engram.nickname,
            "substrate_binding": engram.substrate_binding,
        }

    @staticmethod
    def _message_view(message: Message) -> dict[str, Any]:
        return {
            "role": message.role.value,
            "content": message.content,
            "timestamp": message.timestamp.isoformat(),
            "source_engram_id": message.source_engram_id,
        }

    @staticmethod
    def _living_concern_view(concern: LivingConcern) -> dict[str, Any]:
        return {
            "id": concern.id,
            "center_id": concern.center_id,
            "owner_engram_id": concern.owner_engram_id,
            "content": concern.content,
            "disposition": concern.disposition.value,
            "revisit_at": (
                concern.revisit_at.isoformat() if concern.revisit_at else None
            ),
            "causal_id": concern.causal_id,
            "source_event_id": concern.source_event_id,
            "revision": concern.revision,
            "last_reentry_event_id": concern.last_reentry_event_id,
            "created_at": concern.created_at.isoformat(),
            "updated_at": concern.updated_at.isoformat(),
            "resolved_at": (
                concern.resolved_at.isoformat() if concern.resolved_at else None
            ),
        }

    @staticmethod
    def _living_orientation_view(
        orientation: LivingOrientation,
    ) -> dict[str, Any]:
        """Return the safe, canonical read projection for one orientation.

        Center detail is the only public surface allowed to carry the
        subject-authored orientation content.  Keep this projection explicit
        so capability/session internals cannot leak into the Workbench.
        """

        return {
            "id": orientation.id,
            "center_id": orientation.center_id,
            "owner_engram_id": orientation.owner_engram_id,
            "content": orientation.content,
            "state": orientation.state.value,
            "revision": orientation.revision,
            "engagement_count": orientation.engagement_count,
            "next_eligible_at": (
                orientation.next_eligible_at.isoformat()
                if orientation.next_eligible_at is not None
                else None
            ),
            "last_engagement_event_id": orientation.last_engagement_event_id,
            "last_engaged_at": (
                orientation.last_engaged_at.isoformat()
                if orientation.last_engaged_at is not None
                else None
            ),
            "created_at": orientation.created_at.isoformat(),
            "updated_at": orientation.updated_at.isoformat(),
            "closed_at": (
                orientation.closed_at.isoformat()
                if orientation.closed_at is not None
                else None
            ),
        }

    def _center_activity_summary(self, center_id: str) -> dict[str, Any]:
        with self._storage._lock:
            counts = self._storage._conn.execute(
                "SELECT MAX(seq), "
                "SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status = 'uncertain' THEN 1 ELSE 0 END) "
                "FROM causal_events WHERE world_id = ? AND center_id = ?",
                (self._world_id, center_id),
            ).fetchone()
            latest = self._storage._conn.execute(
                "SELECT created_at, source, kind FROM causal_events "
                "WHERE world_id = ? AND center_id = ? "
                "ORDER BY seq DESC LIMIT 1",
                (self._world_id, center_id),
            ).fetchone()
        return {
            "last_seq": counts[0] if counts is not None else None,
            "last_event_at": latest[0] if latest is not None else None,
            "queued": int(counts[1] or 0) if counts is not None else 0,
            "running": int(counts[2] or 0) if counts is not None else 0,
            "uncertain": int(counts[3] or 0) if counts is not None else 0,
            "recent_source": latest[1] if latest is not None else None,
            "recent_kind": latest[2] if latest is not None else None,
        }

    def _causal_message_projection(
        self,
        *,
        center_id: str | None = None,
        engram_id: str | None = None,
        unattributed: bool = False,
        session_admitted: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses = [
            "world_id = ?",
            "content IS NOT NULL",
            "kind IN ('stimulus', 'spontaneous', 'propagation', "
            "'assistant_result')",
        ]
        params: list[Any] = [self._world_id]
        if center_id is not None:
            clauses.append("center_id = ?")
            params.append(center_id)
        elif unattributed:
            clauses.append("center_id IS NULL")
        if engram_id is not None:
            # A TaskFront keeps its Center id across succession.  Anchor the
            # projection at the current holder while retaining causal content
            # from committed predecessors in the same durable lineage.
            clauses.append(
                "engram_id IN ("
                "WITH RECURSIVE lineage_engram(id) AS ("
                "SELECT ? UNION "
                "SELECT generation.predecessor_id "
                "FROM generation_transitions generation "
                "JOIN lineage_engram current "
                "ON generation.successor_id = current.id "
                "WHERE generation.state = 'committed'"
                ") SELECT id FROM lineage_engram)"
            )
            params.append(engram_id)
        if session_admitted:
            # A fresh root is visible on the causal timeline immediately, but
            # is not conversation until begin_turn has projected its message.
            # Explicit-refusal retries are queued again after that projection,
            # so the durable turn evidence keeps them visible exactly once.
            clauses.append(
                "(causal_events.status <> 'queued' OR EXISTS ("
                "SELECT 1 FROM harness_turns admitted_turn "
                "WHERE admitted_turn.event_id = causal_events.id "
                "AND admitted_turn.input_message_id IS NOT NULL))"
            )
        params.append(limit)
        with self._storage._lock:
            rows = self._storage._conn.execute(
                "SELECT seq, id, causal_id, parent_event_id, engram_id, "
                "center_id, kind, source, status, content, metadata, "
                "created_at FROM causal_events WHERE "
                + " AND ".join(clauses)
                + " ORDER BY seq DESC LIMIT ?",
                params,
            ).fetchall()
        projected: list[dict[str, Any]] = []
        for row in reversed(rows):
            metadata = json.loads(row[10])
            source_engram_id = metadata.get("source_engram_id")
            if not isinstance(source_engram_id, str) or not source_engram_id:
                source_engram_id = None
            projected.append({
                "seq": row[0],
                "event_id": row[1],
                "causal_id": row[2],
                "parent_event_id": row[3],
                "engram_id": row[4],
                "center_id": row[5],
                "role": "assistant" if row[6] == "assistant_result" else "user",
                "kind": row[6],
                "source": row[7],
                "status": row[8],
                "content": row[9],
                "metadata": metadata,
                "timestamp": row[11],
                "source_engram_id": source_engram_id,
            })
        return projected

    def _ensure_project(self, project_id: str | None) -> None:
        if project_id is None:
            return
        if not isinstance(project_id, str) or not project_id.strip():
            raise ServiceError(
                "invalid_project",
                "project_id must be a non-empty string or null",
                "send an existing project id, or omit project_id",
                status=400,
            )
        if self._storage.get_project(project_id) is None:
            raise ServiceError(
                "unknown_project",
                f"no project {project_id} in this PulseWorld",
                "create the Project first, or omit project_id",
                status=404,
            )

    @staticmethod
    def _world_input_error(error: ValueError, noun: str) -> ServiceError:
        return ServiceError(
            f"invalid_{noun}",
            str(error),
            f"correct the {noun.replace('_', ' ')} fields and try again",
            status=400,
        )

    @staticmethod
    def _task_offer_service_error(error: TaskOfferError) -> ServiceError:
        return ServiceError(
            error.code,
            error.detail,
            error.remedy,
            status=error.status,
        )

    @staticmethod
    def _task_relationship_service_error(
        error: TaskRelationshipError,
    ) -> ServiceError:
        return ServiceError(
            error.code,
            error.detail,
            error.remedy,
            status=error.status,
        )

    @classmethod
    def _task_relationship_operation_view(
        cls,
        operation: TaskRelationshipOperation,
    ) -> dict[str, Any]:
        payload = cls._task_relationship_snapshot_view(operation.snapshot)
        if operation.event_id is not None:
            payload["event_id"] = operation.event_id
        payload["duplicate"] = operation.duplicate
        return payload

    def list_task_relationships(
        self,
        *,
        subject_engram_id: str | None = None,
        status: TaskRelationshipStatus | str | None = None,
    ) -> list[dict[str, Any]]:
        try:
            snapshots = self.task_relationships.list(
                current_subject_engram_id=subject_engram_id,
                status=status,
                limit=100,
            )
        except TaskRelationshipError as exc:
            raise self._task_relationship_service_error(exc) from exc
        return [
            self._task_relationship_snapshot_view(snapshot)
            for snapshot in snapshots
        ]

    def get_task_relationship(
        self,
        relationship_id: str,
    ) -> dict[str, Any]:
        try:
            snapshot = self.task_relationships.get(relationship_id)
        except TaskRelationshipError as exc:
            raise self._task_relationship_service_error(exc) from exc
        return self._task_relationship_snapshot_view(snapshot)

    def propose_task_relationship_terms(
        self,
        relationship_id: str,
        *,
        expected_revision: int,
        content: str,
    ) -> dict[str, Any]:
        self._require_runtime_owner()
        try:
            operation = self.task_relationships.propose_terms(
                relationship_id=relationship_id,
                expected_revision=expected_revision,
                content=content,
            )
        except TaskRelationshipError as exc:
            raise self._task_relationship_service_error(exc) from exc
        self._metrics.record(
            "task_relationship_terms_proposed",
            world=self._world_id,
            relationship=relationship_id,
            revision=operation.snapshot.relationship.revision,
        )
        relationship = operation.snapshot.relationship
        revocation = self._revoke_task_relationship_execution(
            engram_id=relationship.current_subject_engram_id,
            relationship_id=relationship.id,
            relationship_revision=(
                operation.effect_revision or relationship.revision
            ),
            action="terms_proposed",
            source_event_id=operation.event_id or relationship.id,
        )
        payload = self._task_relationship_operation_view(operation)
        payload["execution_revocation"] = revocation
        return payload

    @classmethod
    def _task_offer_summary_view(
        cls,
        snapshot: TaskOfferSnapshot,
    ) -> dict[str, Any]:
        return {
            "task_offer": cls._task_offer_view(snapshot.offer),
            "current_revision": cls._task_offer_revision_view(
                snapshot.current_revision
            ),
        }

    @classmethod
    def _task_offer_operation_view(
        cls,
        operation: TaskOfferOperation,
    ) -> dict[str, Any]:
        payload = cls._task_offer_summary_view(operation.snapshot)
        if operation.event_id is not None:
            payload["event_id"] = operation.event_id
        return payload

    def create_task_offer(
        self,
        subject_engram_id: str,
        content: str,
        *,
        title: str | None = None,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        """Offer work to one continuing subject without creating a task yet."""

        with self._close_lock:
            self._require_runtime_owner()
            if (
                not isinstance(subject_engram_id, str)
                or not subject_engram_id.strip()
            ):
                raise ServiceError(
                    "invalid_task_offer",
                    "subject_engram_id must be a non-empty string",
                    "select one active subject Engram",
                    status=400,
                )
            subject = self._storage.get_engram(subject_engram_id)
            if subject is None:
                raise ServiceError(
                    "unknown_task_offer_subject",
                    f"no subject Engram {subject_engram_id} in this PulseWorld",
                    "select an existing active subject",
                    status=404,
                )
            if subject.status is not EngramStatus.ACTIVE:
                raise ServiceError(
                    "task_offer_subject_inactive",
                    f"subject Engram {subject_engram_id} is {subject.status.value}",
                    "use the current active successor Engram",
                    status=409,
                )
            self._ensure_project(project_id)
            if not isinstance(content, str) or not content.strip():
                raise ServiceError(
                    "invalid_task_offer",
                    "content must contain proposed task terms",
                    "send non-empty natural-language task terms",
                    status=400,
                )
            provenance = self._task_offer_ingress_provenance(
                subject.id,
                content,
                operation="create",
                revision=1,
            )
            service = self.task_offers
            try:
                operation = service.create(
                    subject.id,
                    content,
                    title,
                    project_id,
                    provenance,
                )
            except TaskOfferError as exc:
                raise self._task_offer_service_error(exc) from exc
        self._metrics.record(
            "task_offer_created",
            world=self._world_id,
            offer=operation.snapshot.offer.id,
            revision=operation.snapshot.offer.current_revision,
            subject=operation.snapshot.offer.subject_engram_id,
            event_id=operation.event_id,
        )
        return self._task_offer_operation_view(operation)

    def list_task_offers(
        self,
        *,
        subject_engram_id: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        try:
            snapshots = self.task_offers.list(
                subject_engram_id,
                status,
                50,
            )
        except TaskOfferError as exc:
            raise self._task_offer_service_error(exc) from exc
        return [self._task_offer_summary_view(snapshot) for snapshot in snapshots]

    def get_task_offer(self, offer_id: str) -> dict[str, Any]:
        try:
            snapshot = self.task_offers.get(offer_id)
        except TaskOfferError as exc:
            raise self._task_offer_service_error(exc) from exc
        return self._task_offer_snapshot_view(snapshot)

    def revise_task_offer(
        self,
        offer_id: str,
        *,
        expected_revision: int,
        content: str,
        title: str | None = None,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        with self._close_lock:
            self._require_runtime_owner()
            if (
                isinstance(expected_revision, bool)
                or not isinstance(expected_revision, int)
                or expected_revision < 1
            ):
                raise ServiceError(
                    "invalid_task_offer",
                    "expected_revision must be an integer >= 1",
                    "reload the offer and send its current revision",
                    status=400,
                )
            if not isinstance(content, str) or not content.strip():
                raise ServiceError(
                    "invalid_task_offer",
                    "content must contain revised task terms",
                    "send non-empty natural-language task terms",
                    status=400,
                )
            self._ensure_project(project_id)
            try:
                current = self.task_offers.get(offer_id)
            except TaskOfferError as exc:
                raise self._task_offer_service_error(exc) from exc
            provenance = self._task_offer_ingress_provenance(
                current.offer.subject_engram_id,
                content,
                operation="revise",
                offer_id=current.offer.id,
                revision=expected_revision + 1,
            )
            try:
                operation = self.task_offers.revise(
                    offer_id,
                    expected_revision,
                    content,
                    title,
                    project_id,
                    provenance,
                )
            except TaskOfferError as exc:
                raise self._task_offer_service_error(exc) from exc
        self._metrics.record(
            "task_offer_revised",
            world=self._world_id,
            offer=operation.snapshot.offer.id,
            revision=operation.snapshot.offer.current_revision,
            subject=operation.snapshot.offer.subject_engram_id,
            event_id=operation.event_id,
        )
        return self._task_offer_operation_view(operation)

    def remind_task_offer(
        self,
        offer_id: str,
        *,
        expected_revision: int,
    ) -> dict[str, Any]:
        with self._close_lock:
            self._require_runtime_owner()
            try:
                current = self.task_offers.get(offer_id)
                provenance = self._task_offer_ingress_provenance(
                    current.offer.subject_engram_id,
                    current.current_revision.content,
                    operation="remind",
                    offer_id=current.offer.id,
                    revision=expected_revision,
                )
                operation = self.task_offers.remind(
                    offer_id,
                    expected_revision,
                    provenance,
                )
            except TaskOfferError as exc:
                raise self._task_offer_service_error(exc) from exc
        self._metrics.record(
            "task_offer_reminded",
            world=self._world_id,
            offer=operation.snapshot.offer.id,
            revision=operation.snapshot.offer.current_revision,
            subject=operation.snapshot.offer.subject_engram_id,
            event_id=operation.event_id,
            duplicate=operation.duplicate,
        )
        return self._task_offer_operation_view(operation)

    def withdraw_task_offer(
        self,
        offer_id: str,
        *,
        expected_revision: int,
    ) -> dict[str, Any]:
        with self._close_lock:
            self._require_runtime_owner()
            try:
                operation = self.task_offers.withdraw(
                    offer_id,
                    expected_revision,
                )
            except TaskOfferError as exc:
                raise self._task_offer_service_error(exc) from exc
        self._metrics.record(
            "task_offer_withdrawn",
            world=self._world_id,
            offer=operation.snapshot.offer.id,
            revision=operation.snapshot.offer.current_revision,
            subject=operation.snapshot.offer.subject_engram_id,
            cancelled_event_id=operation.event_id,
        )
        return self._task_offer_operation_view(operation)

    def create_task_front(
        self,
        content: str,
        *,
        title: str | None = None,
        project_id: str | None = None,
        subject_engram_id: str | None = None,
    ) -> dict[str, Any]:
        """Create one task view inside this world, then queue its first text.

        This is deliberately unrelated to delegation: it creates no worker
        record and invokes no Delegator.  The focal Engram receives ``content``
        once through the ordinary dendrite stream.
        """

        self._require_runtime_owner()
        if not isinstance(content, str) or not content.strip():
            raise ServiceError(
                "empty_content",
                "a TaskFront needs a first natural-language message",
                'send {"content": "<text>"}',
                status=400,
            )
        self._ensure_project(project_id)
        resolved_title = title if title is not None else session_name(content)
        if resolved_title is None:
            resolved_title = "New task"
        subject_id = None
        if subject_engram_id is not None:
            if (
                not isinstance(subject_engram_id, str)
                or not subject_engram_id.strip()
            ):
                raise ServiceError(
                    "invalid_task_subject",
                    "subject_engram_id must be a non-empty string or null",
                    "send an active Engram id, null, or omit subject_engram_id",
                    status=400,
                )
            subject_id = subject_engram_id
            subject = self._storage.get_engram(subject_id)
            if subject is None:
                raise ServiceError(
                    "unknown_task_subject",
                    f"no subject Engram {subject_id} in this PulseWorld",
                    "list current Engrams and use one of their ids",
                    status=404,
                )
            if subject.status is not EngramStatus.ACTIVE:
                raise ServiceError(
                    "task_subject_inactive",
                    f"subject Engram {subject_id} is {subject.status.value}",
                    "use the current active successor Engram id",
                    status=409,
                )
        try:
            if subject_id is None:
                bundle = self._world.create_task_front(
                    resolved_title,
                    project_id=project_id,
                    origin=ActivityOrigin.USER,
                )
            else:
                bundle = self._world.create_task_bundle_for_existing_engram(
                    resolved_title,
                    subject_id,
                    project_id=project_id,
                    origin=ActivityOrigin.USER,
                )
        except ValueError as exc:
            if subject_id is not None:
                # The storage operation revalidates inside its transaction.  Map a
                # concurrent lifecycle change to the frozen public refusal.
                subject = self._storage.get_engram(subject_id)
                if subject is None:
                    raise ServiceError(
                        "unknown_task_subject",
                        f"no subject Engram {subject_id} in this PulseWorld",
                        "list current Engrams and use one of their ids",
                        status=404,
                    ) from exc
                if subject.status is not EngramStatus.ACTIVE:
                    raise ServiceError(
                        "task_subject_inactive",
                        f"subject Engram {subject_id} is {subject.status.value}",
                        "use the current active successor Engram id",
                        status=409,
                    ) from exc
            raise self._world_input_error(exc, "task_front") from exc

        event_id = self.inject(
            bundle.focal_engram.id,
            content,
            source="user",
            center_id=bundle.center.id,
        )
        center = self._world.touch_activity_center(bundle.center.id) or bundle.center
        front = self._world.touch_task_front(bundle.front.id) or bundle.front
        self._metrics.record(
            "task_front_created",
            world=self._world_id,
            front=front.id,
            center=center.id,
            focal_engram=front.focal_engram_id,
            event_id=event_id,
            subject_mode="new" if subject_id is None else "existing",
        )
        return {
            "task_front": self._task_front_view(front),
            "activity_center": self._activity_center_view(center),
            "event_id": event_id,
        }

    def list_task_fronts(
        self,
        *,
        status: TaskFrontStatus | str | None = None,
    ) -> list[dict[str, Any]]:
        try:
            fronts = self._world.list_task_fronts(status=status)
        except ValueError as exc:
            raise self._world_input_error(exc, "task_front_filter") from exc
        return [self._task_front_view(front) for front in fronts]

    def get_task_front(self, front_id: str) -> dict[str, Any]:
        front = self._world.get_task_front(front_id)
        if front is None:
            raise ServiceError(
                "unknown_task_front",
                f"no TaskFront {front_id} in this PulseWorld",
                "list TaskFronts and use one of their ids",
                status=404,
            )
        center = self._world.get_activity_center(front.center_id)
        engram = self._storage.get_engram(front.focal_engram_id)
        memberships = self._world.list_memberships(
            center_id=front.center_id,
            engram_id=front.focal_engram_id,
        )
        if (
            center is None
            or engram is None
            or center.kind is not ActivityKind.TASK
            or center.focal_engram_id != front.focal_engram_id
            or not any(
                membership.relation is MembershipRelation.FOCAL
                for membership in memberships
            )
        ):
            raise ServiceError(
                "broken_task_front",
                f"TaskFront {front_id} has a missing durable reference",
                "repair the database from a consistent backup",
                status=500,
            )
        try:
            relationship = self.task_relationships.get_for_front(front.id)
        except TaskRelationshipError as exc:
            raise self._task_relationship_service_error(exc) from exc
        relationship_payload = (
            None
            if relationship is None
            else self._task_relationship_snapshot_view(relationship)
        )
        return {
            "task_front": self._task_front_view(front),
            "activity_center": self._activity_center_view(center),
            "focal_engram": self._engram_view(engram),
            "message_scope": "center",
            "messages": self._causal_message_projection(
                center_id=front.center_id,
                engram_id=front.focal_engram_id,
                session_admitted=True,
            ),
            "unattributed_history": self._causal_message_projection(
                engram_id=front.focal_engram_id,
                unattributed=True,
                session_admitted=True,
                limit=50,
            ),
            "task_relationship_mode": (
                "unmanaged_compatibility"
                if relationship_payload is None
                else "subject_consent_managed"
            ),
            "task_relationship": (
                None
                if relationship_payload is None
                else relationship_payload["task_relationship"]
            ),
            "relationship_events": (
                []
                if relationship_payload is None
                else relationship_payload["relationship_events"]
            ),
        }

    def send_task_front_message(self, front_id: str, content: str) -> str:
        self._require_runtime_owner()
        front = self._world.get_task_front(front_id)
        if front is None:
            raise ServiceError(
                "unknown_task_front",
                f"no TaskFront {front_id} in this PulseWorld",
                "list TaskFronts and use one of their ids",
                status=404,
            )
        if front.status is not TaskFrontStatus.OPEN:
            raise ServiceError(
                "task_front_not_open",
                f"TaskFront {front_id} is {front.status.value}",
                "reopen the Front explicitly before sending another message",
                status=409,
            )
        relationship = self.task_relationships.get_for_front(front.id)
        if (
            relationship is not None
            and relationship.relationship.status
            is not TaskRelationshipStatus.ACTIVE
        ):
            raise ServiceError(
                "task_relationship_not_active",
                f"TaskRelationship {relationship.relationship.id} is "
                f"{relationship.relationship.status.value}",
                "use the renegotiation surface or wait for the subject to resume",
                status=409,
            )
        center = self._world.get_activity_center(front.center_id)
        if center is None:
            raise ServiceError(
                "broken_task_front",
                f"TaskFront {front_id} has no ActivityCenter",
                "repair the database from a consistent backup",
                status=500,
            )
        if center.status not in {
            ActivityCenterStatus.ACTIVE,
            ActivityCenterStatus.DORMANT,
        }:
            raise ServiceError(
                "activity_center_not_writable",
                f"ActivityCenter {center.id} is {center.status.value}",
                "set its status to active before sending another message",
                status=409,
            )
        event_id = self.inject(
            front.focal_engram_id,
            content,
            source="user",
            center_id=center.id,
            _admission_guard=self._task_message_admission_guard(
                center_id=center.id,
                engram_id=front.focal_engram_id,
                front_id=front.id,
            ),
        )
        self._world.touch_activity_center(center.id)
        self._world.touch_task_front(front.id)
        return event_id

    def update_task_front(
        self,
        front_id: str,
        *,
        title: str | None = None,
        status: TaskFrontStatus | str | None = None,
    ) -> dict[str, Any]:
        self._require_runtime_owner()
        current = self._world.get_task_front(front_id)
        if current is None:
            raise ServiceError(
                "unknown_task_front",
                f"no TaskFront {front_id} in this PulseWorld",
                "list TaskFronts and use one of their ids",
                status=404,
            )
        if current.status is TaskFrontStatus.ARCHIVED and (
            status is not None and status != TaskFrontStatus.ARCHIVED
            and status != TaskFrontStatus.ARCHIVED.value
        ):
            raise ServiceError(
                "archived_task_front",
                f"TaskFront {front_id} is archived and cannot be reopened",
                "create a new TaskFront while keeping this one as history",
                status=409,
            )
        try:
            updated = self._world.update_task_front(
                front_id,
                title=title,
                status=status,
            )
        except ValueError as exc:
            raise self._world_input_error(exc, "task_front") from exc
        assert updated is not None
        self._metrics.record(
            "task_front_updated",
            world=self._world_id,
            front=front_id,
            status=updated.status.value,
        )
        return self._task_front_view(updated)

    def create_activity_center(
        self,
        kind: ActivityKind | str,
        title: str,
        *,
        description: str = "",
        origin: ActivityOrigin | str = ActivityOrigin.USER,
        autonomy: float = 1.0,
        project_id: str | None = None,
        stimulus: str | None = None,
    ) -> dict[str, Any]:
        """Create a non-task life domain; initial stimulus is optional."""

        self._require_runtime_owner()
        if stimulus is not None and (
            not isinstance(stimulus, str) or not stimulus.strip()
        ):
            raise ServiceError(
                "empty_stimulus",
                "stimulus must contain natural language when supplied",
                "omit stimulus for a quiet Center, or send non-empty text",
                status=400,
            )
        self._ensure_project(project_id)
        try:
            bundle = self._world.create_activity_center(
                kind,
                title,
                description,
                project_id,
                origin=origin,
                autonomy=autonomy,
            )
        except ValueError as exc:
            raise self._world_input_error(exc, "activity_center") from exc

        event_id = None
        center = bundle.center
        if stimulus is not None:
            event_id = self.inject(
                bundle.focal_engram.id,
                stimulus,
                source="user",
                center_id=center.id,
            )
            center = self._world.touch_activity_center(center.id) or center
        self._metrics.record(
            "activity_center_created",
            world=self._world_id,
            center=center.id,
            kind=center.kind.value,
            origin=center.origin.value,
            focal_engram=bundle.focal_engram.id,
            event_id=event_id,
        )
        result = {
            "activity_center": self._activity_center_view(center),
            "focal_engram_id": bundle.focal_engram.id,
        }
        if event_id is not None:
            result["event_id"] = event_id
        return result

    def list_activity_centers(
        self,
        *,
        kind: ActivityKind | str | None = None,
        status: ActivityCenterStatus | str | None = None,
    ) -> list[dict[str, Any]]:
        try:
            centers = self._world.list_activity_centers(kind=kind, status=status)
        except ValueError as exc:
            raise self._world_input_error(exc, "activity_center_filter") from exc
        return [self._activity_center_view(center) for center in centers]

    def get_living_portfolio(
        self,
        engram_id: str,
        purpose_history_limit: int = 20,
    ) -> dict[str, Any]:
        """Return one subject-scale, read-only living Portfolio projection."""

        engram = self._storage.get_engram(engram_id)
        if engram is None:
            raise ServiceError(
                "unknown_engram",
                f"no Engram {engram_id} in this PulseWorld",
                "list active Engrams and use one of their ids",
                status=404,
            )
        if engram.status is not EngramStatus.ACTIVE:
            raise ServiceError(
                "living_portfolio_holder_inactive",
                f"Engram {engram_id} is {engram.status.value} and cannot hold "
                "the current living Portfolio",
                "use the current successor Engram id",
                status=409,
            )
        projector = self._living_portfolio_projector
        if projector is None:
            raise ServiceError(
                "living_portfolio_unavailable",
                "the living Portfolio projector is unavailable",
                "restart the Runtime after restoring purpose governance",
                status=503,
            )
        try:
            portfolio = projector.project(
                engram_id,
                history_limit=purpose_history_limit,
            )
        except LivingPortfolioValidationError as exc:
            raise ServiceError(
                "living_portfolio_history_limit_invalid",
                str(exc),
                "send purpose_history_limit=1..100, or omit it to use 20",
                status=400,
            ) from exc
        except LivingPortfolioRecoveryError as exc:
            raise ServiceError(
                "living_portfolio_unavailable",
                "the living Portfolio could not be reconstructed from its "
                "durable purpose, lineage, and Center state",
                "repair the durable subject state, then retry the read",
                status=503,
            ) from exc

        current_purpose = portfolio["purpose"]["current"]
        self._metrics.record(
            "living_portfolio_observed",
            world=self._world_id,
            engram=engram_id,
            lineage_state=portfolio["subject"]["lineage_state"],
            item_count=portfolio["item_count"],
            current_purpose_revision_id=(
                None
                if current_purpose is None
                else current_purpose["purpose_revision_id"]
            ),
        )
        return portfolio

    def get_purpose_amendment_attempts(
        self,
        engram_id: str,
        *,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Observe settlement-fenced purpose attempts without mutating lineage."""

        if type(limit) is not int or not 1 <= limit <= 100:
            raise ServiceError(
                "purpose_amendment_limit_invalid",
                "purpose amendment limit must be an integer in [1, 100]",
                "send limit=1..100, or omit it to use 20",
                status=400,
            )
        engram = self._storage.get_engram(engram_id)
        if engram is None:
            raise ServiceError(
                "unknown_engram",
                f"no Engram {engram_id} in this PulseWorld",
                "list active Engrams and use one of their ids",
                status=404,
            )
        if engram.status is not EngramStatus.ACTIVE:
            raise ServiceError(
                "purpose_amendment_holder_inactive",
                f"Engram {engram_id} is not the active subject holder",
                "use the current successor Engram id",
                status=409,
            )
        governance = self._purpose_governance
        if governance is None:
            raise ServiceError(
                "purpose_governance_unavailable",
                "purpose amendment governance is unavailable",
                "restart the owning Runtime",
                status=503,
            )
        try:
            lineage = governance.find_lineage_for_engram(engram_id)
            attempts = (
                []
                if lineage is None
                else governance.list_proposals(lineage.lineage_id, limit=limit)
            )
            current = (
                None
                if lineage is None
                else governance.current_revision(lineage.lineage_id)
            )
        except PurposeGovernanceError as exc:
            raise ServiceError(
                "purpose_governance_unavailable",
                "purpose amendment state could not be reconstructed",
                "repair the durable purpose lineage before retrying",
                status=503,
            ) from exc
        settlement = (
            {"health": "unavailable", "last_error_type": None}
            if self._life_tools is None
            else self._life_tools.purpose_settlement_status()
        )
        return {
            "schema_version": "purpose-amendments.v1",
            "world_id": self._world_id,
            "subject": {
                "requested_engram_id": engram_id,
                "lineage_id": None if lineage is None else lineage.lineage_id,
                "current_engram_id": (
                    engram_id if lineage is None else lineage.current_engram_id
                ),
                "generation": 0 if lineage is None else lineage.generation,
            },
            "current_purpose_revision_id": (
                None if current is None else current.purpose_revision_id
            ),
            "attempts": [attempt.to_dict() for attempt in attempts],
            "attempt_count": len(attempts),
            "settlement": {
                **settlement,
                "startup_recovery": dict(self._purpose_recovery),
            },
            "evidence_class": "LIVE_GATE_UNVERIFIED",
        }

    def get_role_accountability(
        self,
        engram_id: str,
        *,
        limit: int = DEFAULT_ROLE_LIMIT,
    ) -> dict[str, Any]:
        """Return one holder-bounded role snapshot without life-side writes."""

        engram = self._storage.get_engram(engram_id)
        if engram is None:
            raise ServiceError(
                "unknown_engram",
                f"no Engram {engram_id} in this PulseWorld",
                "list active Engrams and use one of their ids",
                status=404,
            )
        if engram.status is not EngramStatus.ACTIVE:
            raise ServiceError(
                "role_accountability_holder_inactive",
                f"Engram {engram_id} is {engram.status.value} and is not a "
                "current role holder",
                "use the current successor Engram id",
                status=409,
            )
        projector = self._role_accountability_projector
        if projector is None:
            raise ServiceError(
                "role_accountability_unavailable",
                "the role accountability projector is unavailable",
                "restart the Runtime after restoring the role store",
                status=503,
            )
        try:
            return projector.project(engram_id, limit=limit)
        except RoleAccountabilityValidationError as exc:
            raise ServiceError(
                "role_accountability_limit_invalid",
                str(exc),
                "send limit=1..64, or omit it to use 32",
                status=400,
            ) from exc
        except RoleAccountabilityRecoveryError as exc:
            raise ServiceError(
                "role_accountability_unavailable",
                "role accountability could not be reconstructed from its "
                "durable lease, cycle, obligation, and contribution state",
                "repair the durable role state, then retry the read",
                status=503,
            ) from exc

    def get_activity_center(self, center_id: str) -> dict[str, Any]:
        center = self._world.get_activity_center(center_id)
        if center is None:
            raise ServiceError(
                "unknown_activity_center",
                f"no ActivityCenter {center_id} in this PulseWorld",
                "list ActivityCenters and use one of their ids",
                status=404,
            )
        concerns = self._world.list_living_concerns(center_id=center_id)
        visible_concerns = concerns[:_CENTER_CONCERN_LIMIT]
        orientations = self._world.list_living_orientations(center_id=center_id)
        orientation_limit = self._config.living_orientation_history_limit
        visible_orientations = orientations[:orientation_limit]
        return {
            "activity_center": self._activity_center_view(center),
            "members": [
                self._membership_view(membership)
                for membership in self._world.list_memberships(center_id=center_id)
            ],
            "living_concerns": [
                self._living_concern_view(concern)
                for concern in visible_concerns
            ],
            "living_concerns_total": len(concerns),
            "living_concerns_truncated": len(concerns) > len(visible_concerns),
            "living_orientations": [
                self._living_orientation_view(orientation)
                for orientation in visible_orientations
            ],
            "living_orientations_total": len(orientations),
            "living_orientations_truncated": (
                len(orientations) > len(visible_orientations)
            ),
            "activity_summary": self._center_activity_summary(center_id),
            "messages": self._causal_message_projection(center_id=center_id),
            "unattributed_history": (
                self._causal_message_projection(
                    engram_id=center.focal_engram_id,
                    unattributed=True,
                    limit=50,
                )
                if center.focal_engram_id is not None
                else []
            ),
        }

    def send_activity_center_message(self, center_id: str, content: str) -> str:
        """Deliver an explicit stimulus through a life Center's focal Engram."""

        self._require_runtime_owner()
        center = self._world.get_activity_center(center_id)
        if center is None:
            raise ServiceError(
                "unknown_activity_center",
                f"no ActivityCenter {center_id} in this PulseWorld",
                "list ActivityCenters and use one of their ids",
                status=404,
            )
        relationship = self.task_relationships.get_for_center(center_id)
        if (
            relationship is not None
            and relationship.relationship.status
            is not TaskRelationshipStatus.ACTIVE
        ):
            raise ServiceError(
                "task_relationship_not_active",
                f"TaskRelationship {relationship.relationship.id} is "
                f"{relationship.relationship.status.value}",
                "use the renegotiation surface or wait for the subject to resume",
                status=409,
            )
        if center.status not in {
            ActivityCenterStatus.ACTIVE,
            ActivityCenterStatus.DORMANT,
        }:
            raise ServiceError(
                "activity_center_not_writable",
                f"ActivityCenter {center_id} is {center.status.value}",
                "set its status to active before sending another stimulus",
                status=409,
            )
        if center.focal_engram_id is None:
            raise ServiceError(
                "activity_center_has_no_focus",
                f"ActivityCenter {center_id} has no focal Engram",
                "assign a focal membership before sending a stimulus",
                status=409,
            )
        event_id = self.inject(
            center.focal_engram_id,
            content,
            source="user",
            center_id=center.id,
            _admission_guard=self._task_message_admission_guard(
                center_id=center.id,
                engram_id=center.focal_engram_id,
            ),
        )
        self._world.touch_activity_center(center_id)
        return event_id

    def _task_message_admission_guard(
        self,
        *,
        center_id: str,
        engram_id: str,
        front_id: str | None = None,
    ) -> Callable[[Any], None]:
        """Linearize message admission with relationship/Center transitions."""

        def guard(conn: Any) -> None:
            if front_id is not None:
                front_row = conn.execute(
                    "SELECT center_id, focal_engram_id, status FROM task_fronts "
                    "WHERE id = ?",
                    (front_id,),
                ).fetchone()
                if front_row is None:
                    raise ServiceError(
                        "unknown_task_front",
                        f"no TaskFront {front_id} in this PulseWorld",
                        "reload the TaskFront before sending another message",
                        status=404,
                    )
                if (
                    front_row[0] != center_id
                    or front_row[1] != engram_id
                    or front_row[2] != TaskFrontStatus.OPEN.value
                ):
                    raise ServiceError(
                        "task_front_not_open",
                        f"TaskFront {front_id} changed before message admission",
                        "reload the TaskFront before sending another message",
                        status=409,
                    )
            center_row = conn.execute(
                "SELECT kind, status, focal_engram_id FROM activity_centers "
                "WHERE id = ?",
                (center_id,),
            ).fetchone()
            if center_row is None:
                raise ServiceError(
                    "unknown_activity_center",
                    f"no ActivityCenter {center_id} in this PulseWorld",
                    "reload the Center before sending another message",
                    status=404,
                )
            relationship_row = conn.execute(
                "SELECT id, status, current_subject_engram_id "
                "FROM task_relationships WHERE world_id = ? AND center_id = ?",
                (self._world_id, center_id),
            ).fetchone()
            if relationship_row is not None and (
                relationship_row[1] != TaskRelationshipStatus.ACTIVE.value
                or relationship_row[2] != engram_id
            ):
                raise ServiceError(
                    "task_relationship_not_active",
                    f"TaskRelationship {relationship_row[0]} changed before message admission",
                    "reload the relationship or wait for the subject to resume",
                    status=409,
                )
            if (
                center_row[2] != engram_id
                or center_row[1]
                not in {
                    ActivityCenterStatus.ACTIVE.value,
                    ActivityCenterStatus.DORMANT.value,
                }
            ):
                raise ServiceError(
                    "activity_center_not_writable",
                    f"ActivityCenter {center_id} changed before message admission",
                    "reload the Center before sending another stimulus",
                    status=409,
                )

        return guard

    def update_activity_center(
        self,
        center_id: str,
        *,
        title: str | None = None,
        description: str | None = None,
        status: ActivityCenterStatus | str | None = None,
        autonomy: float | None = None,
    ) -> dict[str, Any]:
        self._require_runtime_owner()
        current = self._world.get_activity_center(center_id)
        if current is None:
            raise ServiceError(
                "unknown_activity_center",
                f"no ActivityCenter {center_id} in this PulseWorld",
                "list ActivityCenters and use one of their ids",
                status=404,
            )
        if current.status is ActivityCenterStatus.ARCHIVED and (
            status is not None and status != ActivityCenterStatus.ARCHIVED
            and status != ActivityCenterStatus.ARCHIVED.value
        ):
            raise ServiceError(
                "archived_activity_center",
                f"ActivityCenter {center_id} is archived and cannot reactivate",
                "create a new Center while keeping this one as history",
                status=409,
            )
        relationship = self.task_relationships.get_for_center(center_id)
        if relationship is not None and status is not None:
            raise ServiceError(
                "task_relationship_controls_center_status",
                f"ActivityCenter {center_id} belongs to subject-managed "
                f"TaskRelationship {relationship.relationship.id}",
                "change participation through the subject relationship lifecycle",
                status=409,
            )
        try:
            updated = self._world.update_activity_center(
                center_id,
                title=title,
                description=description,
                status=status,
                autonomy=autonomy,
            )
        except ValueError as exc:
            raise self._world_input_error(exc, "activity_center") from exc
        assert updated is not None
        self._metrics.record(
            "activity_center_updated",
            world=self._world_id,
            center=center_id,
            kind=updated.kind.value,
            status=updated.status.value,
            origin=updated.origin.value,
        )
        return self._activity_center_view(updated)

    def add_center_membership(
        self,
        center_id: str,
        engram_id: str,
        relation: MembershipRelation | str = MembershipRelation.PARTICIPANT,
    ) -> dict[str, Any]:
        self._require_runtime_owner()
        try:
            membership = self._world.add_membership(
                center_id,
                engram_id,
                relation,
            )
        except ValueError as exc:
            detail = str(exc)
            status = 404 if "not found" in detail else 400
            raise ServiceError(
                "invalid_center_membership",
                detail,
                "use an existing Center and Engram with a valid relation",
                status=status,
            ) from exc
        self._metrics.record(
            "center_membership_added",
            world=self._world_id,
            center=center_id,
            engram=engram_id,
            relation=membership.relation.value,
        )
        return self._membership_view(membership)

    # ── Content stream (contract §2.1) ───────────────────────────

    def inject(
        self,
        engram_id: str,
        content: str,
        *,
        source: str | CausalEventSource = CausalEventSource.USER,
        priority: float = 1.0,
        causal_id: str | None = None,
        parent_event_id: str | None = None,
        flow: CausalEventFlow | str | None = CausalEventFlow.CONTENT,
        kind: CausalEventKind | str = CausalEventKind.STIMULUS,
        center_id: str | None = None,
        idempotency_key: str | None = None,
        stimulus_envelope: StimulusEnvelope | None = None,
        _admission_guard: Callable[[Any], None] | None = None,
    ) -> str:
        """Durably enqueue one piece of content and return its event id.

        The causal ledger is the only input truth.  PulseEngine reconstructs
        its scheduling cache from this row on the next tick; no API-thread
        inbox can disappear between the return and that tick.
        """
        self._require_runtime_owner()
        if not isinstance(content, str) or not content.strip():
            raise ServiceError(
                "empty_content",
                "an injection with no content has nothing to deliver",
                'POST {"content": "<text>", "source": "user"}',
                status=400,
            )
        try:
            source_value = (
                source
                if isinstance(source, CausalEventSource)
                else CausalEventSource(source)
            )
        except (TypeError, ValueError, CausalTransitionError) as exc:
            raise ServiceError(
                "invalid_source",
                "source is not a supported causal event source",
                "use user, self, habitat, sensory, propagation, delegation, or system",
                status=400,
            ) from exc
        try:
            priority_value = float(priority)
        except (TypeError, ValueError) as exc:
            raise ServiceError(
                "invalid_priority",
                "priority must be a finite number",
                "send a finite numeric priority",
                status=400,
            ) from exc
        if not math.isfinite(priority_value):
            raise ServiceError(
                "invalid_priority",
                "priority must be a finite number",
                "send a finite numeric priority",
                status=400,
            )

        try:
            # Owner validation, durable enqueue and the derived local writes
            # share the close lock. A concurrent close therefore happens
            # wholly before this boundary (the input is refused) or after the
            # accepted event has a complete local response projection; it
            # cannot close SQLite between validation and commit.
            with self._close_lock:
                self._require_runtime_owner()
                engram = self._storage.get_engram(engram_id)
                if engram is None:
                    raise ServiceError(
                        "unknown_engram",
                        f"no engram {engram_id} in this runtime",
                        f"GET /engrams for live ids, or address the front engram "
                        f"{self._front_id}",
                        status=404,
                    )
                if engram.status != EngramStatus.ACTIVE:
                    raise ServiceError(
                        "archived_engram",
                        f"engram {engram_id} is archived and no longer pulses",
                        f"inject into its live successor, or into the front engram "
                        f"{self._front_id}",
                        status=409,
                    )
                lineage_id = self._ensure_subject_lineage(engram_id)
                try:
                    flow_value = (
                        CausalEventFlow.CONTENT
                        if flow is None
                        else (
                            flow
                            if isinstance(flow, CausalEventFlow)
                            else CausalEventFlow(flow)
                        )
                    )
                except (TypeError, ValueError) as exc:
                    raise ServiceError(
                        "invalid_flow",
                        "flow is not one of the three organism information flows",
                        "use content, spectrum, or tunnel",
                        status=400,
                    ) from exc
                if flow_value is not CausalEventFlow.CONTENT:
                    raise ServiceError(
                        "invalid_content_ingress_flow",
                        "ordinary natural-language ingress is restricted to the content flow",
                        "use content here; use the Claustrum numeric sideband for spectrum "
                        "or DurableDelegationTunnel for tunnel delivery",
                        status=400,
                    )
                envelope = stimulus_envelope
                if source_value is CausalEventSource.USER:
                    if envelope is None:
                        identity_seed = (
                            idempotency_key
                            if isinstance(idempotency_key, str) and idempotency_key
                            else uuid.uuid4().hex
                        )
                        identity = _stable_digest(
                            {
                                "world_id": self._world_id,
                                "engram_id": engram_id,
                                "identity_seed": identity_seed,
                                "center_id": center_id,
                            }
                        )[:32]
                        envelope = StimulusEnvelope.user_input(
                            f"stimulus_{identity}",
                            user_id="local-user",
                            event_id=f"ingress_{identity}",
                            target_lineage_id=lineage_id,
                            content=content,
                            evidence_class=StimulusEvidenceClass.LIVE,
                            flow=flow_value.value,
                        )
                    expected_class = StimulusClass.USER_INPUT
                else:
                    expected_class = {
                        CausalEventSource.SELF: StimulusClass.SUBJECT_REFLECTION,
                        CausalEventSource.HABITAT: StimulusClass.EXTERNAL_CONSEQUENCE,
                        CausalEventSource.SENSORY: StimulusClass.EXTERNAL_CONSEQUENCE,
                        CausalEventSource.PROPAGATION: StimulusClass.CONTENT_PROPAGATION,
                        CausalEventSource.DELEGATION: StimulusClass.CONTENT_PROPAGATION,
                    }.get(source_value)
                    if envelope is None or expected_class is None:
                        raise ServiceError(
                            "stimulus_provenance_required",
                            "non-user content cannot enter life through an untyped source label",
                            "submit a typed live StimulusEnvelope from the owning adapter",
                            status=403,
                        )
                if not isinstance(envelope, StimulusEnvelope):
                    raise ServiceError(
                        "stimulus_envelope_invalid",
                        "the life ingress did not receive a typed stimulus envelope",
                        "use a StimulusEnvelope constructor with immutable provenance",
                        status=400,
                    )
                if (
                    envelope.stimulus_class is not expected_class
                    or envelope.target_lineage_id != lineage_id
                    or envelope.content_digest != digest_payload(content)
                    or envelope.flow != flow_value.value
                ):
                    raise ServiceError(
                        "stimulus_envelope_mismatch",
                        "the stimulus envelope does not match the content, lineage, source, or flow",
                        "rebuild the envelope from the exact live source event",
                        status=409,
                    )
                effect_idempotency_key = idempotency_key
                if envelope.external_effect_id is not None:
                    effect_digest = _stable_digest(
                        {
                            "world_id": self._world_id,
                            "external_effect_id": envelope.external_effect_id,
                        }
                    )
                    effect_idempotency_key = f"external-effect:{effect_digest}"
                    existing_effect = self._causal_ledger.find_causal_event_by_idempotency(
                        self._world_id,
                        effect_idempotency_key,
                    )
                    if existing_effect is not None:
                        identity_matches = (
                            existing_effect.engram_id == engram_id
                            and existing_effect.source is source_value
                            and existing_effect.flow is flow_value
                            and existing_effect.content == content
                            and existing_effect.metadata.get("external_effect_digest")
                            == effect_digest
                            and existing_effect.metadata.get("stimulus_provenance_digest")
                            == envelope.provenance.digest
                        )
                        if not identity_matches:
                            raise ServiceError(
                                "external_effect_collision",
                                "the external effect id is already bound to another stimulus identity",
                                "reuse the original envelope or allocate a new external effect id",
                                status=409,
                            )
                        return existing_effect.id
                firewall = self._stimulus_firewall
                if firewall is None:
                    raise ServiceError(
                        "stimulus_firewall_unavailable",
                        "the life/control boundary is unavailable",
                        "restart the owning Runtime before accepting input",
                        status=503,
                    )
                decision = firewall.evaluate(envelope)
                if not decision.life_queue_eligible:
                    try:
                        self._persist_stimulus_control_decision(decision)
                    except Exception as exc:
                        raise ServiceError(
                            "stimulus_audit_unavailable",
                            "the rejected stimulus could not be durably audited",
                            "repair the payload-free control audit before retrying",
                            status=503,
                        ) from exc
                    raise ServiceError(
                        "stimulus_not_eligible",
                        f"the typed stimulus was routed to control only ({decision.reason_code})",
                        "supply live non-control provenance or leave it in the control ledger",
                        status=403,
                    )
                def _live_subject_admission_guard(conn) -> None:
                    row = conn.execute(
                        "SELECT status FROM engrams WHERE id = ?",
                        (engram_id,),
                    ).fetchone()
                    if row is None or row[0] != EngramStatus.ACTIVE.value:
                        raise CausalAdmissionConflictError(
                            "the target Engram changed before event admission"
                        )
                    if _admission_guard is not None:
                        _admission_guard(conn)

                event = self._causal_ledger.enqueue(
                    world_id=self._world_id,
                    flow=flow_value,
                    domain=CausalEventDomain.PULSE,
                    kind=kind,
                    source=source_value,
                    content=content,
                    causal_id=causal_id,
                    parent_event_id=parent_event_id,
                    engram_id=engram_id,
                    center_id=center_id,
                    metadata={
                        "priority": priority_value,
                        "stimulus_class": envelope.stimulus_class.value,
                        "stimulus_evidence_class": envelope.evidence_class.value,
                        "stimulus_provenance_digest": envelope.provenance.digest,
                        **(
                            {
                                "external_effect_digest": effect_digest,
                            }
                            if envelope.external_effect_id is not None
                            else {}
                        ),
                    },
                    idempotency_key=effect_idempotency_key,
                    admission_guard=_live_subject_admission_guard,
                    runtime_fence=self._current_runtime_fence(),
                )
                # Naming is a derived convenience, not an input truth. Do it
                # only after the accepted stimulus is durable so a rejected
                # event cannot leave identity state behind.
                if source_value is CausalEventSource.USER:
                    self._storage.ensure_auto_name(engram_id, content)
                self._metrics.record(
                    "inject",
                    event_id=event.id,
                    engram=engram_id,
                    source=source_value.value,
                    chars=len(content),
                )
        except CausalAdmissionConflictError as exc:
            raise ServiceError(
                "causal_state_conflict",
                "the target lineage or causal state changed during admission",
                "refresh the active subject and causal parent, then submit against the current state",
                status=409,
            ) from exc
        except (KeyError, TypeError, ValueError, CausalTransitionError) as exc:
            raise ServiceError(
                "invalid_causal_event",
                "the durable input event did not satisfy the causal contract",
                "send a valid source, kind, flow, and existing parent/center",
                status=400,
            ) from exc
        return event.id

    def update_identity(
        self,
        engram_id: str,
        updates: dict[str, str | None],
    ) -> dict[str, str | None]:
        """Apply user-authored display identity without changing the signature."""
        self._require_runtime_owner()
        unknown = sorted(set(updates) - {"name", "nickname"})
        if unknown:
            raise ServiceError(
                "unknown_identity_key",
                f"{unknown} is not an identity field",
                "send name and/or nickname",
                status=400,
            )
        if not updates:
            raise ServiceError(
                "empty_identity",
                "the request names no identity field to change",
                'send {"name": "<session name>"} and/or '
                '{"nickname": "<user-defined nickname>"}',
                status=400,
            )

        normalized: dict[str, str | None] = {}
        if "name" in updates:
            raw_name = updates["name"]
            if not isinstance(raw_name, str) or not raw_name.strip():
                raise ServiceError(
                    "invalid_name",
                    "name must be a non-empty string",
                    'send {"name": "<session name>"}',
                    status=400,
                )
            name = " ".join(raw_name.split())
            if len(name) > 80:
                raise ServiceError(
                    "name_too_long",
                    f"name is {len(name)} characters; the limit is 80",
                    "shorten the session name to 80 characters or fewer",
                    status=400,
                )
            normalized["name"] = name

        if "nickname" in updates:
            raw_nickname = updates["nickname"]
            if raw_nickname is not None and not isinstance(raw_nickname, str):
                raise ServiceError(
                    "invalid_nickname",
                    "nickname must be a string or null",
                    'send {"nickname": "<user-defined nickname>"} or null',
                    status=400,
                )
            nickname = raw_nickname.strip() if isinstance(raw_nickname, str) else None
            if nickname == "":
                nickname = None
            if nickname is not None and len(nickname) > 80:
                raise ServiceError(
                    "nickname_too_long",
                    f"nickname is {len(nickname)} characters; the limit is 80",
                    "shorten the nickname to 80 characters or fewer",
                    status=400,
                )
            normalized["nickname"] = nickname

        updated = self._storage.update_engram_identity(engram_id, normalized)
        if updated is None:
            raise ServiceError(
                "unknown_engram",
                f"no engram {engram_id} in this runtime",
                "GET /engrams for known signatures",
                status=404,
            )
        self._metrics.record(
            "identity_updated",
            engram=engram_id,
            fields=sorted(normalized),
        )
        return {
            "signature": updated.id,
            "name": updated.name,
            "name_origin": updated.name_origin,
            "nickname": updated.nickname,
        }

    # ── Tuning / claustrum rhythm stream (contract §2.2) ─────────

    def tuning(self) -> TuningView:
        """The three facts: what was asked, what is running, when it landed."""
        with self._tuning_lock:
            return TuningView(
                commanded=dict(self._commanded),
                observed={k: self._read_knob(k) for k in TUNING_KNOBS},
                applied_at_tick=self._applied_at_tick,
            )

    def command_tuning(
        self,
        values: dict[str, float | None],
    ) -> tuple[dict[str, float | None], int]:
        """Stage a tuning command. Returns (commanded, will_apply_from_tick).

        A key present with `null` releases that knob back to the claustrum's
        own control (the runtime default is restored so the modulator's factor
        acts on an untouched base). A key that is absent is left alone —
        takeover and autonomy are per-item, not one global switch.

        Nothing changes here. The values are staged and applied at the next
        tick boundary; until then `observed` still reports the old numbers,
        which is the truth.
        """
        self._require_runtime_owner()
        unknown = sorted(set(values) - set(TUNING_KNOBS))
        if unknown:
            raise ServiceError(
                "unknown_tuning_key",
                f"{unknown} is not a rhythm parameter",
                "tuning acts on rhythm only — use "
                f"{list(TUNING_KNOBS)}; to influence *content*, "
                "POST /engrams/{id}/inject instead",
                status=400,
            )

        staged: dict[str, float] = {}
        for name, raw in values.items():
            if raw is None:
                staged[name] = self._defaults[name]
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                raise ServiceError(
                    "invalid_tuning_value",
                    f"{name}={raw!r} is not a number",
                    f"send a number in [{_KNOB_RANGE[name][0]}, "
                    f"{_KNOB_RANGE[name][1]}] ({_KNOB_UNIT[name]}), or null "
                    "to hand the knob back to the claustrum",
                    status=400,
                ) from None
            low, high = _KNOB_RANGE[name]
            if not (low <= value <= high) or value != value:
                raise ServiceError(
                    "tuning_out_of_range",
                    f"{name}={value} is outside [{low}, {high}] "
                    f"({_KNOB_UNIT[name]})",
                    f"send a value in [{low}, {high}], or null to hand the "
                    "knob back to the claustrum",
                    status=400,
                )
            staged[name] = value

        with self._tuning_lock:
            for name, value in staged.items():
                self._commanded[name] = None if values[name] is None else value
                self._pending[name] = value
            self._will_apply_from_tick = self._engine.tick_count + 1
            commanded = dict(self._commanded)
            will_apply = self._will_apply_from_tick
        self._metrics.record(
            "tuning_commanded",
            commanded={k: v for k, v in commanded.items() if v is not None},
            will_apply_from_tick=will_apply,
        )
        return commanded, will_apply

    def apply_pending_tuning(self) -> dict[str, float]:
        """Move staged values into the live rhythm. Called before each tick.

        Public because a caller driving ticks by hand needs the same boundary
        the loop uses; returns what was applied (empty when nothing was).
        """
        with self._tuning_lock:
            if not self._pending:
                return {}
            pending, self._pending = self._pending, {}
            tick = self._engine.tick_count + 1
            for name, value in pending.items():
                self._write_knob(name, value)
            self._applied_at_tick = tick
            commanded = dict(self._commanded)
        self._metrics.record(
            "tuning_applied",
            tick=tick,
            applied=pending,
            commanded={k: v for k, v in commanded.items() if v is not None},
        )
        return pending

    def _read_knob(self, name: str) -> float:
        """The value the engine is actually running with right now.

        With a claustrum attached these are the *bases* its per-engram factors
        multiply — the runtime-wide number, which is what a knob commands.
        """
        engine = self._engine.config
        if name == "activity":
            return float(engine.base_spontaneous_rate)
        if name == "wait":
            return float(self._dendrite.config.default_max_wait)
        if name == "propagation_threshold":
            return float(engine.propagation_threshold)
        if name == "gate":
            return float(engine.inhibition_propagation_gate)
        raise KeyError(name)

    def _write_knob(self, name: str, value: float) -> None:
        engine = self._engine.config
        if name == "activity":
            engine.base_spontaneous_rate = value
        elif name == "wait":
            self._dendrite.config.default_max_wait = value
        elif name == "propagation_threshold":
            engine.propagation_threshold = value
        elif name == "gate":
            engine.inhibition_propagation_gate = value
        else:
            raise KeyError(name)

    def record_tuning(self) -> dict | None:
        """Publish what the Tuning stream actually did this tick.

        The read side needs the *effective* per-engram gate, not the run's base
        gate. While the claustrum's fourth head is off (the default) the two are
        identical, so nothing is wrong — and the moment modulation is switched
        on, a panel still drawing the base would show a number that looks live
        and is stale. That is §2.2's failure mode wearing a different field
        name, so the factors are measured and emitted rather than left to be
        reconstructed downstream.

        Inhibition rides along for the same reason: it was being inferred from
        `propagate.inhibited` plus edge weight plus an assumed decay. That
        inference is a reasonable stopgap and a poor source of truth; this is
        the level the engine is actually holding, decayed to now.

        The event is emitted only when at least one engram is off-neutral, so a
        claustrum-free run preserves the unmodulated event stream.
        **Absent from the event means neutral**: factor 1.0, inhibition 0,
        effective gate == base gate.

        This reads two private engine attributes. It is a named seam, not an
        oversight: the engine is the right place to record its own modulation.
        An unrecorded value that the UI silently guesses at is the worse of the two
        problems. `getattr` defaults mean a later engine refactor degrades this
        to "no event", never to a crash.
        """
        engine = self._engine
        gate_mods: dict[str, float] = getattr(engine, "_gate_mods", {}) or {}
        activity_mods: dict[str, float] = getattr(engine, "_activity_mods", {}) or {}
        prop_mods: dict[str, float] = getattr(engine, "_propagation_mods", {}) or {}
        wait_mods: dict[str, float] = getattr(self._dendrite, "_wait_modifiers", {}) or {}
        raw_inhibition: dict[str, tuple[float, datetime]] = (
            getattr(engine, "_inhibition", {}) or {}
        )

        base_gate = float(engine.config.inhibition_propagation_gate)
        tau = float(engine.config.inhibition_tau)
        now = _now()

        levels: dict[str, float] = {}
        for eid, entry in list(raw_inhibition.items()):
            try:
                level, updated_at = entry
            except (TypeError, ValueError):
                continue
            elapsed = max(0.0, (now - updated_at).total_seconds())
            decayed = level * math.exp(-elapsed / tau) if tau > 0 else 0.0
            if decayed >= 1e-4:
                levels[eid] = decayed

        ids = set(gate_mods) | set(activity_mods) | set(prop_mods) \
            | set(wait_mods) | set(levels)
        rows = []
        for eid in sorted(ids):
            gate_factor = float(gate_mods.get(eid, 1.0))
            activity = float(activity_mods.get(eid, 1.0))
            propagation = float(prop_mods.get(eid, 1.0))
            wait = float(wait_mods.get(eid, 1.0))
            inhibition = levels.get(eid, 0.0)
            if (
                gate_factor == 1.0 and activity == 1.0
                and propagation == 1.0 and wait == 1.0 and inhibition == 0.0
            ):
                continue
            rows.append({
                "id": eid,
                "activity": round(activity, 4),
                "wait": round(wait, 4),
                "propagation": round(propagation, 4),
                "gate": round(gate_factor, 4),
                "effective_gate": round(base_gate * gate_factor, 6),
                "inhibition": round(inhibition, 4),
            })
        if not rows:
            return None

        payload = {
            "tick": engine.tick_count,
            "base_gate": base_gate,
            "inhibition_tau": tau,
            "engrams": rows,
        }
        self._metrics.record("tuning", **payload)
        return payload

    # ── Tunnel stream (contract §2.3) ────────────────────────────

    def delegate(
        self,
        task: str,
        *,
        to: str | None = None,
        backend: str | None = None,
        caller_id: str | None = None,
        center_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> str:
        """Durably queue a same-Center delegation for the ordinary scheduler."""
        self._require_runtime_owner()
        if not task or not task.strip():
            raise ServiceError(
                "empty_task",
                "a delegation with no task has nothing to delegate",
                'POST {"task": "<what to do>", "to": null, "backend": null}',
                status=400,
            )
        if self._config.mock and caller_id is None and center_id is None:
            return self._delegate_legacy_mock(task, to=to, backend=backend)
        if backend not in (None, "pi"):
            raise ServiceError(
                "unsupported_delegation_backend",
                "native durable delegation always uses the target Engram's Pi session",
                'omit backend or send "backend": "pi"',
                status=400,
            )
        caller_id, center_id = self._resolve_delegation_context(
            caller_id,
            center_id,
        )
        stable_key = idempotency_key or uuid.uuid4().hex
        try:
            admission = self._delegation_tunnel.enqueue(
                caller_id=caller_id,
                center_id=center_id,
                task=task.strip(),
                target_id=to,
                idempotency_key=stable_key,
            )
        except (DelegationTunnelError, KeyError, ValueError) as exc:
            raise ServiceError(
                "delegation_rejected",
                str(exc),
                "choose two active members of the same ActivityCenter, or add "
                "a participant before delegating",
                status=409,
            ) from exc
        self._pending_delegation_requests.add(admission.request_event.id)
        self._metrics.record(
            "delegation_requested",
            id=admission.record_id,
            event_id=admission.request_event.id,
            caller=caller_id,
            target=admission.target_id,
            center=center_id,
            mode=DURABLE_DELEGATION_MODE,
            recovered=admission.recovered,
        )
        return admission.record_id

    def _resolve_delegation_context(
        self,
        caller_id: str | None,
        center_id: str | None,
    ) -> tuple[str, str]:
        """Resolve an explicit subject relation, with one-front convenience."""

        if (caller_id is None) != (center_id is None):
            raise ServiceError(
                "delegation_context_incomplete",
                "caller_id and center_id must be supplied together",
                "send both fields from the active TaskFront",
                status=400,
            )
        if caller_id is not None and center_id is not None:
            return caller_id, center_id
        fronts = self._world.list_task_fronts(status=TaskFrontStatus.OPEN)
        if len(fronts) != 1:
            raise ServiceError(
                "delegation_context_required",
                "delegation is ambiguous without one selected TaskFront",
                "send caller_id and center_id from the TaskFront initiating it",
                status=400,
            )
        return fronts[0].focal_engram_id, fronts[0].center_id

    def _delegate_legacy_mock(
        self,
        task: str,
        *,
        to: str | None,
        backend: str | None,
    ) -> str:
        """Keep the old one-shot contract behind explicit ``mock=True`` only."""

        if to is not None:
            target = self._storage.get_engram(to)
            if target is None:
                raise ServiceError(
                    "unknown_engram",
                    f"no engram {to} to delegate to",
                    "GET /engrams for live ids, or send \"to\": null to let "
                    "the router choose",
                    status=404,
                )
            if target.status is not EngramStatus.ACTIVE:
                raise ServiceError(
                    "archived_engram",
                    f"engram {to} is archived and cannot take work",
                    "delegate to a live engram",
                    status=409,
                )
        self._check_backend(backend, to)
        rec = _Delegation(
            id=uuid.uuid4().hex,
            task=task,
            to=to,
            backend=backend,
            caller_id=self._front_id,
            created_at=_iso(),
        )
        with self._deleg_lock:
            if self._closed:
                raise ServiceError(
                    "runtime_closed",
                    "this runtime no longer accepts delegations",
                    "construct a new RuntimeService against the same db_path",
                    status=409,
                )
            self._delegations[rec.id] = rec
            self._futures[rec.id] = self._executor.submit(
                self._run_delegation,
                rec,
            )
        self._metrics.record(
            "delegation_requested",
            id=rec.id,
            to=to,
            backend=backend,
            mode="legacy_mock",
        )
        return rec.id

    def _check_backend(self, backend: str | None, to: str | None) -> None:
        """Backends are substrate bindings; only registered ones can run work."""
        if backend is None or backend == _LOCAL_BACKEND:
            return
        available = self._substrates.names() if self._substrates else []
        if backend not in available:
            raise ServiceError(
                "no_backend",
                f"delegation backend {backend!r} is not registered in this "
                "runtime",
                f"start the runtime with a substrate named {backend!r} "
                "(RuntimeService(substrates=SubstrateRegistry(...)) with "
                f"register({backend!r}, adapter)), or send "
                '"backend": "local" / null to run on the default substrate',
                status=503,
            )
        if to is None:
            raise ServiceError(
                "backend_needs_target",
                f"backend {backend!r} binds a substrate to an engram, and the "
                "router's target is not known until the delegation runs",
                f'send "to": "<engram_id>" together with '
                f'"backend": {backend!r}, or "backend": null to let the '
                "router choose freely",
                status=400,
            )

    def _run_delegation(self, rec: _Delegation) -> None:
        rec.status = "running"
        try:
            if rec.backend not in (None, _LOCAL_BACKEND) and rec.to is not None:
                self._mgr.bind_substrate(rec.to, rec.backend)
            if rec.to is not None:
                results = [self._delegator.delegate(
                    rec.caller_id, rec.task, target_id=rec.to
                )]
                rec.route = {"decided_by": "caller", "chosen": rec.to}
            else:
                rec.route = self._route_preview(rec.task)
                results = self._delegator.delegate_routed(rec.caller_id, rec.task)
            rec.record_ids = [r.record_id for r in results]
            rec.targets = [r.target_id for r in results]
            rec.target_id = results[0].target_id
            rec.mode = results[0].mode
            rec.result = results[0].content
            rec.status = "done"
            if rec.route is not None:
                rec.route["chosen"] = rec.target_id
        except (ValueError, RuntimeError, LLMCallError) as exc:
            rec.status = "failed"
            rec.error = {
                "error": "delegation_failed",
                "detail": f"{type(exc).__name__}: {exc}",
                "remedy": (
                    "check the target engram is live and the substrate has a "
                    "usable key (GET /substrates), then POST /delegate again"
                ),
            }
            _logger.warning("delegation %s failed: %s", rec.id, exc)
        except Exception as exc:  # noqa: BLE001 — a worker must not die silently
            rec.status = "failed"
            rec.error = {
                "error": "delegation_failed",
                "detail": f"{type(exc).__name__}: {exc}",
                "remedy": "see the runtime log for the traceback, then retry",
            }
            _logger.exception("delegation %s crashed", rec.id)
        finally:
            rec.completed_at = _iso()
            self._metrics.record(
                "delegation_done",
                id=rec.id,
                status=rec.status,
                target=rec.target_id,
            )

    def _route_preview(self, task: str) -> dict | None:
        """The router's reasoning, in the shape the right column must show.

        Scores come from the router's own public ranking, so this reports the
        decision surface rather than a second implementation of it.
        """
        if self._router is None:
            return {
                "decided_by": "fallback",
                "detail": "no router attached; the task opens a fresh engram",
            }
        candidates = [
            e.id for e in self._storage.list_engrams(status=EngramStatus.ACTIVE)
            if e.id != self._front_id
        ]
        if not candidates:
            return {
                "decided_by": "fallback",
                "detail": "no live candidate engrams; the task opens a fresh one",
            }
        try:
            embedding = self._llm.embed(task).vector
        except (NotImplementedError, LLMCallError):
            embedding = None
        scores = self._router.rank(self._front_id, embedding, candidates)
        return {
            "decided_by": "router",
            "temperature": round(self._router.temperature(), 4),
            "canary_threshold": round(self._router.canary_threshold(), 4),
            "scores": {k: round(v, 6) for k, v in scores.items()},
        }

    def await_delegation(self, delegation_id: str, timeout: float = 30.0) -> dict:
        """Block until a delegation settles. For scripts and tests, not routes."""
        with self._deleg_lock:
            future = self._futures.get(delegation_id)
        if future is not None:
            future.result(timeout=timeout)
        durable = self._storage.get_delegation(delegation_id)
        if durable is not None:
            if durable["completed_at"] is None and not self.running:
                raise ServiceError(
                    "runtime_not_running",
                    "cannot await an unsettled durable delegation while the runtime is stopped",
                    "await service.start() before awaiting the delegation",
                    status=409,
                )
            deadline = time.monotonic() + timeout
            while durable["completed_at"] is None and time.monotonic() < deadline:
                time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))
                durable = self._storage.get_delegation(delegation_id)
                if durable is None:
                    break
            if durable is None:
                raise ServiceError(
                    "unknown_delegation",
                    f"no delegation {delegation_id}",
                    "GET /delegations to list durable records",
                    status=404,
                )
            return self._durable_delegation_view(durable)
        with self._deleg_lock:
            rec = self._delegations.get(delegation_id)
        if rec is None:
            raise ServiceError(
                "unknown_delegation",
                f"no delegation {delegation_id}",
                "GET /delegations to list the ones this runtime knows",
                status=404,
            )
        return self._delegation_view(rec)

    def delegations(self, limit: int = 50) -> list[dict]:
        """Delegation records, newest first — API-issued and engram-issued.

        Records created by an engram calling the `delegate` tool are just as
        real as ones the human asked for, so both appear; the API-issued ones
        additionally carry the router's decision.
        """
        with self._deleg_lock:
            records = list(self._delegations.values())
        views = [self._delegation_view(r) for r in records]
        covered = {rid for r in records for rid in r.record_ids}
        for row in self._storage.list_delegations():
            if row["id"] in covered:
                continue
            views.append(self._durable_delegation_view(row))
        views.sort(key=lambda v: v["created_at"], reverse=True)
        return views[: max(0, limit)]

    @staticmethod
    def _durable_delegation_view(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": row["id"],
            "task": row["task"],
            "status": "done" if row["completed_at"] else "running",
            "caller_id": row["caller_id"],
            "target_id": row["target_id"],
            "targets": [row["target_id"]],
            "mode": row["mode"],
            "backend": "pi" if row["mode"] == DURABLE_DELEGATION_MODE else None,
            "created_at": row["created_at"],
            "completed_at": row["completed_at"],
            "record_ids": [row["id"]],
            "result": row["result_summary"],
            "outcome": row["outcome"],
            "route": {"decided_by": "durable_tunnel"},
            "error": None,
        }

    def _delegation_view(self, rec: _Delegation) -> dict:
        outcomes = []
        for rid in rec.record_ids:
            row = self._storage.get_delegation(rid)
            if row is not None and row["outcome"]:
                outcomes.append(row["outcome"])
        return {
            "id": rec.id,
            "task": rec.task,
            "status": rec.status,
            "caller_id": rec.caller_id,
            "target_id": rec.target_id,
            "targets": list(rec.targets),
            "mode": rec.mode,
            "backend": rec.backend,
            "created_at": rec.created_at,
            "completed_at": rec.completed_at,
            "record_ids": list(rec.record_ids),
            "result": rec.result,
            "outcome": outcomes[0] if outcomes else None,
            "route": rec.route,
            "error": rec.error,
        }

    def record_delegation_outcome(
        self,
        record_id: str,
        outcome: str,
    ) -> dict[str, Any]:
        """Persist the sole explicit learning signal for tunnel routing."""

        self._require_runtime_owner()
        try:
            update = self._delegation_tunnel.record_outcome(record_id, outcome)
        except (DelegationTunnelError, KeyError, ValueError) as exc:
            raise ServiceError(
                "delegation_outcome_rejected",
                str(exc),
                "submit adopted, revised, or discarded for a completed durable "
                "delegation record",
                status=409,
            ) from exc
        self._metrics.record(
            "delegation_outcome",
            record_id=update.record_id,
            outcome=update.outcome,
            changed=update.changed,
            learning_updates=update.learning_updates,
        )
        return {
            "record_id": update.record_id,
            "outcome": update.outcome,
            "changed": update.changed,
            "learning_updates": update.learning_updates,
        }

    def _reconcile_durable_delegations(self) -> None:
        """Discover new requests and repair terminal tunnel projections."""

        while True:
            events = self._causal_ledger.list_events(
                after_seq=self._delegation_scan_seq,
                limit=200,
                world_id=self._world_id,
                kind=CausalEventKind.DELEGATION_REQUEST,
            )
            if not events:
                break
            for event in events:
                if event.seq is not None:
                    self._delegation_scan_seq = max(
                        self._delegation_scan_seq,
                        event.seq,
                    )
                if event.metadata.get("delegation_mode") == DURABLE_DELEGATION_MODE:
                    self._pending_delegation_requests.add(event.id)
            if len(events) < 200:
                break

        for request_event_id in tuple(self._pending_delegation_requests):
            try:
                reconciliation = self._delegation_tunnel.reconcile(
                    request_event_id
                )
            except Exception as exc:  # noqa: BLE001 - keep pending for repair
                _logger.exception(
                    "durable delegation reconciliation failed for %s",
                    request_event_id,
                )
                self._metrics.record(
                    "delegation_reconcile_failed",
                    event_id=request_event_id,
                    error=type(exc).__name__,
                )
                continue
            if reconciliation.state != "pending":
                self._pending_delegation_requests.discard(request_event_id)
                self._metrics.record(
                    "delegation_reconciled",
                    event_id=request_event_id,
                    record_id=reconciliation.record_id,
                    state=reconciliation.state,
                    delivery_event_id=(
                        None
                        if reconciliation.delivery_event is None
                        else reconciliation.delivery_event.id
                    ),
                )

    # ── The loop ─────────────────────────────────────────────────

    def _spontaneous_center_for_engram(self, engram_id: str) -> str | None:
        """Attribute generic fallback only to an un-oriented active Center."""

        _centers, unoriented = self._active_unoriented_centers(engram_id)
        return unoriented[0].id if len(unoriented) == 1 else None

    def _active_unoriented_centers(
        self,
        engram_id: str,
    ) -> tuple[list[ActivityCenter], list[ActivityCenter]]:
        """Return active Centers and those without a current orientation."""

        centers = [
            center
            for center in self._world.list_activity_centers(
                status=ActivityCenterStatus.ACTIVE,
                engram_id=engram_id,
            )
            if center.kind is not ActivityKind.TASK
        ]
        current = self._world.list_living_orientations(
            owner_engram_id=engram_id,
            current_only=True,
        )
        oriented_center_ids = {orientation.center_id for orientation in current}
        unoriented = [
            center for center in centers
            if center.id not in oriented_center_ids
        ]
        return centers, unoriented

    def _spontaneous_dispatch_for_engram(
        self,
        engram_id: str,
    ) -> SpontaneousDispatch:
        """Project one already-fired impulse into the living orientation path.

        The Engine invokes this callback while holding the Storage lock.  The
        re-entrant lock here also makes direct unit invocation safe and keeps
        selection, idempotent enqueue, and engagement CAS one critical section.
        """

        with self._storage._lock:
            orientation = self._world.select_living_orientation(
                engram_id,
                _now(),
            )
            if orientation is None:
                centers, unoriented = self._active_unoriented_centers(engram_id)
                if unoriented:
                    return SpontaneousDispatch.FALLBACK
                if not centers:
                    all_centers = self._world.list_activity_centers(
                        engram_id=engram_id,
                    )
                    if not all_centers:
                        return SpontaneousDispatch.FALLBACK
                self._metrics.record(
                    "living_orientation_spontaneous_suppressed",
                    world=self._world_id,
                    engram=engram_id,
                    reason_code="all_active_centers_unavailable",
                    active_centers=len(centers),
                    oriented_centers=len(centers) - len(unoriented),
                )
                return SpontaneousDispatch.SUPPRESSED

            if self._causal_ledger is None:
                raise RuntimeError(
                    "LivingOrientation spontaneous emission requires a causal ledger"
                )
            engagement_sequence = orientation.engagement_count + 1
            idempotency_key = (
                f"living-orientation:{orientation.id}:"
                f"revision:{orientation.revision}:"
                f"engagement:{engagement_sequence}"
            )
            event = self._causal_ledger.enqueue(
                world_id=self._world_id,
                flow=None,
                domain=CausalEventDomain.PULSE,
                kind=CausalEventKind.SPONTANEOUS,
                source=CausalEventSource.SELF,
                content=orientation.content,
                causal_id=orientation.causal_id,
                parent_event_id=orientation.source_event_id,
                engram_id=orientation.owner_engram_id,
                center_id=orientation.center_id,
                metadata={
                    "reason_code": "living_orientation_engagement",
                    "orientation_id": orientation.id,
                    "orientation_revision": orientation.revision,
                    "engagement_sequence": engagement_sequence,
                    "priority": self._config.living_orientation_priority,
                },
                idempotency_key=idempotency_key,
                runtime_fence=self._current_runtime_fence(),
            )
            if event.status is not CausalEventStatus.QUEUED:
                self._metrics.record(
                    "living_orientation_spontaneous_suppressed",
                    world=self._world_id,
                    engram=engram_id,
                    orientation=orientation.id,
                    reason_code="engagement_event_not_queued",
                    status=event.status.value,
                )
                return SpontaneousDispatch.SUPPRESSED
            engaged_at = _now()
            next_eligible_at = engaged_at + timedelta(
                seconds=self._config.living_orientation_refractory_sec
            )
            marked = self._world.mark_living_orientation_engaged(
                orientation.id,
                orientation.revision,
                orientation.engagement_count,
                event.id,
                next_eligible_at,
            )
            self._metrics.record(
                "living_orientation_engagement_enqueued",
                world=self._world_id,
                event_id=event.id,
                causal=event.causal_id,
                center=event.center_id,
                engram=event.engram_id,
                orientation=marked.id,
                revision=marked.revision,
                engagement=marked.engagement_count,
                status=event.status.value,
            )
            return SpontaneousDispatch.EMITTED

    def _repair_living_orientation_engagements(self) -> int:
        """Project only already-durable next-key events after a crash."""

        with self._storage._lock:
            if self._causal_ledger is None:
                return 0
            repaired = 0
            orientations = self._world.list_living_orientations(
                current_only=True,
            )
            for orientation in orientations:
                if orientation.state.value != "open":
                    continue
                engagement_sequence = orientation.engagement_count + 1
                idempotency_key = (
                    f"living-orientation:{orientation.id}:"
                    f"revision:{orientation.revision}:"
                    f"engagement:{engagement_sequence}"
                )
                event = self._causal_ledger.find_causal_event_by_idempotency(
                    self._world_id,
                    idempotency_key,
                )
                if (
                    event is None
                    or event.status is not CausalEventStatus.QUEUED
                ):
                    continue
                repaired_orientation = self._world.mark_living_orientation_engaged(
                    orientation.id,
                    orientation.revision,
                    orientation.engagement_count,
                    event.id,
                    event.created_at + timedelta(
                        seconds=self._config.living_orientation_refractory_sec
                    ),
                )
                repaired += 1
                self._metrics.record(
                    "living_orientation_engagement_repaired",
                    world=self._world_id,
                    event_id=event.id,
                    orientation=repaired_orientation.id,
                    revision=repaired_orientation.revision,
                    engagement=repaired_orientation.engagement_count,
                    status=event.status.value,
                )
            return repaired

    def _enqueue_due_living_concerns(self) -> int:
        """Admit each due authored revision once, then make it quiet."""

        with self._storage._lock:
            return self._enqueue_due_living_concerns_locked()

    def _enqueue_due_living_concerns_locked(self) -> int:
        """Linearize Center state, authored revision, enqueue, and CAS mark."""

        budget = self._config.living_concern_reentry_budget_per_tick
        if budget == 0:
            return 0
        due = self._world.list_due_living_concerns(
            _now(),
            min(500, budget + 1),
        )
        admitted = 0
        for concern in due[:budget]:
            event = self._causal_ledger.enqueue(
                world_id=self._world_id,
                flow=None,
                domain=CausalEventDomain.PULSE,
                kind=CausalEventKind.SPONTANEOUS,
                source=CausalEventSource.SELF,
                content=concern.content,
                causal_id=concern.causal_id,
                parent_event_id=concern.source_event_id,
                engram_id=concern.owner_engram_id,
                center_id=concern.center_id,
                metadata={
                    "reason_code": "living_concern_reentry",
                    "concern_id": concern.id,
                    "revision": concern.revision,
                    "priority": (
                        self._config.living_concern_reentry_priority
                    ),
                },
                idempotency_key=(
                    f"living-concern:{concern.id}:revision:{concern.revision}"
                ),
                runtime_fence=self._current_runtime_fence(),
            )
            self._world.mark_living_concern_reentered(
                concern.id,
                concern.revision,
                event.id,
            )
            admitted += 1
            self._metrics.record(
                "living_concern_reentry_enqueued",
                world=self._world_id,
                event=event.id,
                causal=event.causal_id,
                center=concern.center_id,
                engram=concern.owner_engram_id,
                concern=concern.id,
                revision=concern.revision,
            )
        if len(due) > budget:
            self._metrics.record(
                "living_concern_reentry_deferred",
                due_count=len(due),
                admitted_count=admitted,
            )
        return admitted

    def _poll_habitat(self) -> int:
        """Commit unbidden Habitat changes before the engine consumes them."""

        with self._storage._lock:
            return self._poll_habitat_locked()

    def _poll_habitat_locked(self) -> int:
        """Linearize Center availability with each Habitat cursor advance."""

        changes = self._habitat.poll_changes()
        if not changes:
            return 0
        subscriptions = self._storage.list_habitat_subscriptions(
            world_id=self._world_id,
            status="active",
        )
        enqueued = 0
        for change in changes:
            response = change.response
            if not response.unbidden:
                continue
            matches = [
                subscription
                for subscription in subscriptions
                if subscription.channel in {"all", response.channel}
            ]
            if not matches:
                continue

            pending = [
                subscription
                for subscription in matches
                if subscription.last_fingerprint != change.fingerprint
            ]
            all_processed = True
            eligible: dict[tuple[str, str | None], list[Any]] = {}
            for subscription in pending:
                owner = self._storage.get_engram(subscription.engram_id)
                if owner is None or owner.status is not EngramStatus.ACTIVE:
                    # Archived identities cannot hold the Habitat source open
                    # forever. Succession normally moves live subscriptions.
                    self._storage.update_habitat_fingerprint(
                        subscription.id,
                        change.fingerprint,
                    )
                    continue
                if subscription.center_id is not None:
                    center = self._world.get_activity_center(
                        subscription.center_id
                    )
                    if (
                        center is not None
                        and center.status is ActivityCenterStatus.ARCHIVED
                    ):
                        # Archived is terminal and cannot later consume the
                        # held observation. Retire only this binding so it
                        # cannot pin the world-level Habitat cursor forever.
                        self._storage.deactivate_habitat_subscription(
                            subscription.id
                        )
                        continue
                    if (
                        center is None
                        or center.status is not ActivityCenterStatus.ACTIVE
                    ):
                        # Preserve both cursors. Restoring the Center will
                        # expose this exact same world change on a later tick.
                        all_processed = False
                        continue
                eligible.setdefault(
                    (subscription.engram_id, subscription.center_id), []
                ).append(subscription)

            for (engram_id, center_id), group in sorted(
                eligible.items(),
                key=lambda item: (item[0][0], item[0][1] or ""),
            ):
                idempotency_key = (
                    f"habitat:{self._world_id}:{engram_id}:"
                    f"{change.fingerprint}"
                    if center_id is None
                    else f"habitat:{self._world_id}:{engram_id}:"
                    f"{center_id}:{change.fingerprint}"
                )
                event = self._causal_ledger.enqueue(
                    world_id=self._world_id,
                    flow=CausalEventFlow.CONTENT,
                    domain=CausalEventDomain.HABITAT,
                    kind=CausalEventKind.STIMULUS,
                    source=CausalEventSource.HABITAT,
                    content=response.detail,
                    engram_id=engram_id,
                    center_id=center_id,
                    metadata={
                        "channel": response.channel,
                        "fingerprint": change.fingerprint,
                    },
                    idempotency_key=idempotency_key,
                    runtime_fence=self._current_runtime_fence(),
                )
                enqueued += 1
                for subscription in group:
                    self._storage.update_habitat_fingerprint(
                        subscription.id,
                        change.fingerprint,
                    )
                self._metrics.record(
                    "habitat_stimulus_enqueued",
                    event_id=event.id,
                    engram=engram_id,
                    center=center_id,
                    channel=response.channel,
                )

            # ManagedHabitat owns one world-level source cursor. It can advance
            # only when every matching Center cursor has advanced. A paused
            # Center therefore does not lose the observation merely because a
            # diffuse or unrelated active subscription consumed it first.
            if all_processed:
                self._habitat.acknowledge(change)
        return enqueued

    def _poll_sensory(self) -> int:
        """Turn sensory source fingerprints into durable content events."""

        items = self._sensory.poll_durable()
        enqueued = 0
        for engram_id, content, priority, fingerprint, channel in items:
            engram = self._storage.get_engram(engram_id)
            if engram is None or engram.status is not EngramStatus.ACTIVE:
                # An archived binding cannot be allowed to keep a source
                # cursor hot forever. Succession listeners move live bindings.
                SensoryCortex.acknowledge(channel, fingerprint)
                continue
            event = self._causal_ledger.enqueue(
                world_id=self._world_id,
                flow=CausalEventFlow.CONTENT,
                domain=CausalEventDomain.PULSE,
                kind=CausalEventKind.STIMULUS,
                source=CausalEventSource.SENSORY,
                content=content,
                engram_id=engram_id,
                metadata={
                    "channel": type(channel).__name__,
                    "fingerprint": fingerprint,
                    "priority": priority,
                },
                idempotency_key=f"sensory:{engram_id}:{fingerprint}",
                runtime_fence=self._current_runtime_fence(),
            )
            SensoryCortex.acknowledge(channel, fingerprint)
            enqueued += 1
            self._metrics.record(
                "sensory_stimulus_enqueued",
                event_id=event.id,
                engram=engram_id,
                channel=type(channel).__name__,
            )
        return enqueued

    def tick_once(self) -> list[tuple[str, str]]:
        """Run one Runtime-owned durable heartbeat after :meth:`start`."""

        self._require_runtime_owner()
        if not self.running:
            raise ServiceError(
                "runtime_not_running",
                "RuntimeService.tick_once requires an active lifecycle",
                "await service.start() before driving a Runtime tick",
                status=409,
            )
        with self._tick_lock:
            self._require_runtime_owner()
            self._poll_habitat()
            self._poll_sensory()
            self._enqueue_due_living_concerns()
            self.apply_pending_tuning()
            # Repair the durable projection before Engine can make another
            # spontaneous decision.  This operation only marks an event
            # already found by its stable next-engagement key; it never
            # creates an event, changes its status, or replays uncertainty.
            self._repair_living_orientation_engagements()
            try:
                result = self._engine.tick()
            except RuntimeLeaseError as exc:
                # The owner can change in the narrow interval after the
                # foreground fence and before the atomic reservation/settle
                # transaction. Convert that race into the same fail-closed
                # Runtime state as a heartbeat-detected loss.
                self._on_runtime_lease_lost(exc)
                raise self._runtime_lease_service_error(exc) from exc
            self._reconcile_durable_delegations()
            self.record_tuning()
            return result

    async def start(self) -> None:
        """Start ticking in the background. Idempotent."""
        if self._closed:
            raise ServiceError(
                "runtime_closed",
                "this runtime has been closed and cannot be restarted",
                "construct a new RuntimeService against the same db_path — "
                "the front engram resumes from there",
                status=409,
            )
        if self._quiescing:
            keeper = self._lease_keeper
            if keeper is None or not keeper.health().healthy:
                raise self._runtime_lease_service_error()
            raise ServiceError(
                "runtime_quiescing",
                "the previous lifecycle stop timed out and was recovered as uncertain",
                "construct a new RuntimeService against the same durable database",
                status=409,
            )
        self._require_runtime_owner()
        self._quiescing = False
        if self.running:
            return
        self._tick_loop = asyncio.get_running_loop()
        self._stop = asyncio.Event()
        self._task = asyncio.create_task(self._loop(), name="pulse-tick-loop")
        # Let the loop reach its first await so `running` is true on return.
        await asyncio.sleep(0)

    def stop(self, timeout: float = 30.0) -> Coroutine[Any, Any, None]:
        """Fence new work now and return the awaitable tick-loop shutdown."""

        # ``async def`` bodies do not run until their coroutine is scheduled.
        # Establish the publication fence in the ordinary call itself so a
        # loaded event loop cannot admit work between stop() and its first
        # scheduling turn.
        if self._task is not None:
            self._quiescing = True
        return self._stop_tick_loop(timeout)

    async def _stop_tick_loop(self, timeout: float) -> None:
        """Stop ticking. The thought is already on disk; only the loop ends."""
        if self._task is None:
            return
        stopped_cleanly = False
        self._signal_tick_stop()
        try:
            await asyncio.wait_for(asyncio.shield(self._task), timeout=timeout)
            stopped_cleanly = True
        except asyncio.TimeoutError:
            _logger.warning("tick loop did not stop within %.1fs; cancelling", timeout)
            # Cancelling asyncio.to_thread cannot cancel the worker that may
            # already be inside Pi.  Mark its durable running turn/effect and
            # generation uncertain before releasing the lifecycle task; the
            # later close boundary waits for the worker to leave the tick lock.
            try:
                self._revoke_publication(reason="runtime_stop_timeout")
            except Exception:  # noqa: BLE001 - recovery remains best effort
                _logger.exception(
                    "Runtime publication revocation after stop timeout failed"
                )
            try:
                recovery_owner = self._start_registered_recovery_owner(
                    "stop_recovery",
                    self._recover_shutdown_durable_state,
                )
                recovery_deadline = (
                    asyncio.get_running_loop().time()
                    + self._config.runtime_shutdown_timeout_sec
                )
                while not recovery_owner["done"].is_set():
                    remaining = (
                        recovery_deadline - asyncio.get_running_loop().time()
                    )
                    if remaining <= 0:
                        raise asyncio.TimeoutError
                    await asyncio.sleep(min(0.05, remaining))
                recovery_thread = recovery_owner["thread"]
                if not recovery_thread.is_alive():
                    recovery_thread.join(timeout=0.0)
                recovery_state = recovery_owner["state"]
                if "error_type" in recovery_state:
                    raise RuntimeError(
                        "runtime_stop_recovery_failed:"
                        + str(recovery_state["error_type"])
                    )
                recovery_result = recovery_state["result"]
                if recovery_result.get("causal") is not None:
                    self._recovery = recovery_result["causal"]
                self._metrics.record(
                    "runtime_stop_recovery",
                    unresolved=recovery_result.get("unresolved_total", 1),
                    domains=sorted(recovery_result.get("domains", {})),
                )
            except Exception:  # noqa: BLE001
                _logger.exception("durable recovery after stop timeout failed")
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        finally:
            self._task = None
            # A normal stop is restartable.  Timeout recovery remains sticky:
            # the worker behind asyncio.to_thread may still own physical work,
            # so only a fresh RuntimeService may publish the next generation.
            if stopped_cleanly:
                self._quiescing = False
        self._metrics.record("runtime_stop", ticks=self._engine.tick_count)

    async def _loop(self) -> None:
        """The heartbeat.

        The tick itself is blocking (LLM calls, SQLite) so it runs in a worker
        thread and the event loop stays free for the API. Ticks remain strictly
        serial. A tick that raises is recorded and the next one still runs —
        the runtime outlives its own bad moments.
        """
        interval = self._config.tick_interval
        while not self._stop.is_set():
            try:
                await asyncio.to_thread(self.tick_once)
            except asyncio.CancelledError:
                raise
            except ServiceError as exc:
                if exc.error == "runtime_lease_lost":
                    _logger.error("tick loop stopped after Runtime lease loss")
                    self._metrics.record(
                        "tick_error",
                        error="ServiceError: runtime_lease_lost",
                    )
                    return
                _logger.exception("tick refused")
                self._metrics.record(
                    "tick_error", error=f"ServiceError: {exc.error}"
                )
            except Exception as exc:  # noqa: BLE001
                _logger.exception("tick failed")
                self._metrics.record(
                    "tick_error", error=f"{type(exc).__name__}: {exc}"
                )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    # ── Snapshot ─────────────────────────────────────────────────

    def _causal_state_counts(self) -> dict[str, int]:
        counts = {status.value: 0 for status in CausalEventStatus}
        after_seq = 0
        while True:
            events = self._causal_ledger.list_events(
                after_seq=after_seq,
                limit=500,
                world_id=self._world_id,
            )
            if not events:
                break
            for event in events:
                counts[event.status.value] = counts.get(event.status.value, 0) + 1
            last_seq = events[-1].seq
            if last_seq is None or last_seq <= after_seq or len(events) < 500:
                break
            after_seq = last_seq
        return counts

    def snapshot(self) -> dict:
        """A runtime status view for whoever mounts the read side."""
        active = self._storage.list_engrams(status=EngramStatus.ACTIVE)
        fronts = self._world.list_task_fronts()
        centers = self._world.list_activity_centers()
        subscriptions = self._storage.list_habitat_subscriptions(
            world_id=self._world_id,
            status="active",
        )
        return {
            "world_id": self._world_id,
            "continuity_engram_id": self._front_id,
            "world": {
                "id": self._world_id,
                "created_at": self._world_created_at,
                "continuity_engram_id": self._front_id,
                "legacy_front_migrated": self._legacy_front_migrated,
                "task_fronts": len(fronts),
                "activity_centers": len(centers),
                "life_centers": sum(
                    center.kind is not ActivityKind.TASK for center in centers
                ),
            },
            # One-release compatibility field. It is not a TaskFront id.
            "front_engram_id": self._front_id,
            "resumed": self._resumed,
            "running": self.running,
            "lease": self._runtime_lease_view(),
            "tick": self._engine.tick_count,
            "engrams_active": len(active),
            "claustrum": self._claustrum is not None,
            "router": self._router is not None,
            "mock": self._config.mock,
            "harness": self._harness_summary(),
            "stimulus_firewall": {
                "attached": self._stimulus_firewall is not None,
                "control_records_current_runtime": (
                    0
                    if self._stimulus_control_ledger is None
                    else len(self._stimulus_control_ledger)
                ),
                "observer_health": self._stimulus_observer_health,
                "observer_failures": self._stimulus_observer_failures,
                "observer_last_error": self._stimulus_observer_last_error,
                "durable_audit": dict(self._stimulus_control_audit),
                "control_replay_can_enqueue": False,
            },
            "purpose_settlement": {
                **(
                    {"health": "unavailable", "last_error_type": None}
                    if self._life_tools is None
                    else self._life_tools.purpose_settlement_status()
                ),
                "startup_recovery": dict(self._purpose_recovery),
            },
            "causal": self._causal_state_counts(),
            "recovery": {
                "turns": len(self._recovery.turn_ids),
                "events": len(self._recovery.event_ids),
                "effects": len(self._recovery.effect_ids),
                "generations": len(self._recovery.generation_ids),
                "isolated_generation_summaries": len(
                    self._recovered_generation_summary_ids
                ),
                "archived_generation_orphans": len(
                    self._recovered_generation_orphan_ids
                ),
            },
            "habitat": {
                "subscriptions_active": len(subscriptions),
                "sensory_bindings": len(self._sensory.bound_engrams()),
            },
            "tuning": self.tuning().as_dict(),
            "resources": self._runtime.snapshot(),
        }


class RuntimeAssembly:
    """Composition-root entry that keeps startup shutdown evidence observable."""

    @staticmethod
    def _finish_local_claim(
        controller: RuntimeShutdownController,
        claim: RuntimeShutdownClaim,
    ) -> RuntimeShutdownReport:
        """Finish a non-blocking assembly-only flight at its exact cutoff."""

        try:
            return controller.finish(claim)
        except RuntimeError as exc:
            if "deadline terminalizer" not in str(exc):
                raise
            return controller.finish_on_deadline(claim)

    @classmethod
    def _finish_unowned_failure(
        cls,
        controller: RuntimeShutdownController,
        *,
        timeout: float,
    ) -> None:
        """Terminalize failures that happened before Runtime acquired authority."""

        claim = controller.begin(
            RuntimeShutdownTrigger.STARTUP_FAILURE,
            ShutdownDeadline.after(timeout),
            ("runtime_assembly",),
        )
        if not claim.is_owner or claim.builder is None:  # pragma: no cover
            return
        local_owner = threading.current_thread()
        controller.bind_owner(claim, local_owner)
        controller.bind_deadline_terminalizer(claim, local_owner)
        observed_at = _now()
        claim.builder.record_component(
            component_report(
                "runtime_assembly",
                effect=ShutdownEffectState.NOT_STARTED,
                owner=ShutdownOwnerState.JOINED,
                process_tree=ShutdownProcessTreeState.NOT_APPLICABLE,
                cancel=ShutdownCancelState.NOT_NEEDED,
                started_at=observed_at,
                started_monotonic=time.monotonic(),
                unresolved=0,
                error_code="runtime_assembly_not_owned",
            )
        )
        claim.builder.set_durable_recovery(
            ShutdownDurableRecoveryState.NOT_NEEDED
        )
        claim.builder.set_publication_fence(
            ShutdownPublicationFenceState.FAILED
        )
        claim.builder.set_storage_state(ShutdownStorageState.CLOSED)
        cls._finish_local_claim(controller, claim)

    @classmethod
    def _finish_retained_authority_failure(
        cls,
        controller: RuntimeShutdownController,
        *,
        timeout: float,
    ) -> None:
        """Fail honestly if acquired Runtime authority escaped constructor cleanup."""

        claim = controller.begin(
            RuntimeShutdownTrigger.STARTUP_FAILURE,
            ShutdownDeadline.after(timeout),
            ("runtime_assembly",),
        )
        if not claim.is_owner or claim.builder is None:  # pragma: no cover
            return
        local_owner = threading.current_thread()
        controller.bind_owner(claim, local_owner)
        controller.bind_deadline_terminalizer(claim, local_owner)
        observed_at = _now()
        claim.builder.record_component(
            component_report(
                "runtime_assembly",
                effect=ShutdownEffectState.UNCERTAIN,
                owner=ShutdownOwnerState.ESCAPED,
                process_tree=ShutdownProcessTreeState.NOT_APPLICABLE,
                cancel=ShutdownCancelState.SIGNALLED,
                started_at=observed_at,
                started_monotonic=time.monotonic(),
                active_before=1,
                unresolved=1,
                error_code="runtime_assembly_authority_retained",
            )
        )
        claim.builder.set_durable_recovery(ShutdownDurableRecoveryState.FAILED)
        claim.builder.set_publication_fence(
            ShutdownPublicationFenceState.FAILED
        )
        claim.builder.set_owner_lease(ShutdownOwnerLeaseState.RELEASE_PENDING)
        claim.builder.set_storage_state(
            ShutdownStorageState.RETAINED_FOR_ESCAPED_WORKERS
        )
        cls._finish_local_claim(controller, claim)

    @classmethod
    def open(
        cls,
        config: RuntimeServiceConfig | None = None,
        *,
        substrates: SubstrateRegistry | None = None,
        harness_factory: HarnessFactory | None = None,
    ) -> RuntimeAssemblyOutcome:
        controller = RuntimeShutdownController()
        runtime_config = RuntimeServiceConfig() if config is None else config
        try:
            runtime = RuntimeService(
                runtime_config,
                substrates=substrates,
                harness_factory=harness_factory,
                shutdown_controller=controller,
            )
        except Exception as error:  # noqa: BLE001 - preserve exact startup error
            if controller.primary_trigger is None:
                if controller.runtime_authority_acquired:
                    cls._finish_retained_authority_failure(
                        controller,
                        timeout=runtime_config.runtime_shutdown_timeout_sec,
                    )
                else:
                    cls._finish_unowned_failure(
                        controller,
                        timeout=runtime_config.runtime_shutdown_timeout_sec,
                    )
            return RuntimeAssemblyOutcome(
                runtime=None,
                error=error,
                shutdown=controller.observer,
            )
        return RuntimeAssemblyOutcome(
            runtime=runtime,
            error=None,
            shutdown=controller.observer,
        )

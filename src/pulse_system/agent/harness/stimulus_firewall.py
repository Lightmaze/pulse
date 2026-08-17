"""Pure domain firewall for separating life stimuli from control traffic.

This module intentionally has no scheduler, Runtime, persistence, or Harness
imports.  It makes one domain decision only: whether a typed, bounded,
provenance-carrying event may be offered to the life lane.  A caller still has
to perform the actual queueing, learning, and activation work elsewhere.

The firewall does not trust a free-form ``reason_code`` or a caller supplied
label.  The declared :class:`StimulusClass` must agree with the structured
provenance facts, and any control-plane step contaminates the whole chain.
Unknown, incomplete, replayed, and insufficiently evidenced events fail
closed.  Decisions and control records are immutable; the control ledger is
append-only and bounded.

Evidence is deliberately explicit.  This contract has no live adapter of its
own, so its implementation evidence is always ``CONTRACT_ONLY``.  An input
event may carry ``LIVE_GATE_UNVERIFIED`` or ``LIVE`` evidence, but the gate
only admits life stimuli with an explicit ``LIVE`` chain.  No value is
promoted by this module.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from enum import StrEnum
from threading import RLock
from typing import Any, ClassVar

__all__ = [
    "CONTRACT_ONLY",
    "LIVE",
    "LIVE_GATE_UNVERIFIED",
    "MAX_PROVENANCE_DEPTH",
    "ControlLedger",
    "ControlLedgerCapacityError",
    "ControlRecord",
    "DecisionRoute",
    "EvidenceClass",
    "ProvenanceChain",
    "ProvenanceMode",
    "ProvenanceSource",
    "ProvenanceStep",
    "StimulusClass",
    "StimulusDecision",
    "StimulusEnvelope",
    "StimulusFirewall",
    "digest_payload",
]


MAX_PROVENANCE_DEPTH = 8
_MAX_ID_LENGTH = 256
_MAX_CONTENT_DIGEST_LENGTH = 256
_MAX_CAPABILITY_LENGTH = 128
_ALLOWED_FLOWS = frozenset({"content", "spectrum", "tunnel"})
_REFLECTION_CAPABILITIES = frozenset(
    {
        "pulse_life_amend_purpose",
        "pulse_life_hold",
        "pulse_life_orient",
        "pulse_life_project",
        "subject_project",
    }
)


class EvidenceClass(StrEnum):
    """Evidence supplied by the producer of an event.

    The firewall never infers a stronger class from an output, a test pass, or
    a label.  ``LIVE`` means the caller has already completed its own live
    gate; it is not a claim made by this module.
    """

    CONTRACT_ONLY = "CONTRACT_ONLY"
    LIVE_GATE_UNVERIFIED = "LIVE_GATE_UNVERIFIED"
    LIVE = "LIVE"


CONTRACT_ONLY = EvidenceClass.CONTRACT_ONLY.value
LIVE_GATE_UNVERIFIED = EvidenceClass.LIVE_GATE_UNVERIFIED.value
LIVE = EvidenceClass.LIVE.value

_EVIDENCE_RANK = {
    EvidenceClass.CONTRACT_ONLY: 0,
    EvidenceClass.LIVE_GATE_UNVERIFIED: 1,
    EvidenceClass.LIVE: 2,
}


class StimulusClass(StrEnum):
    """The only source classes understood by the life/control boundary."""

    USER_INPUT = "user_input"
    EXTERNAL_CONSEQUENCE = "external_consequence"
    CONTENT_PROPAGATION = "content_propagation"
    SUBJECT_REFLECTION = "subject_reflection"
    CONTROL_OBSERVATION = "control_observation"
    MANAGEMENT = "management"
    WAITING = "waiting"
    VERIFICATION = "verification"
    REPLAY_OR_PROJECTION = "replay_or_projection"
    UNKNOWN = "unknown"


_CONTROL_CLASSES = frozenset(
    {
        StimulusClass.CONTROL_OBSERVATION,
        StimulusClass.MANAGEMENT,
        StimulusClass.WAITING,
        StimulusClass.VERIFICATION,
        StimulusClass.REPLAY_OR_PROJECTION,
    }
)
_LIFE_CLASSES = frozenset(
    {
        StimulusClass.USER_INPUT,
        StimulusClass.EXTERNAL_CONSEQUENCE,
        StimulusClass.CONTENT_PROPAGATION,
        StimulusClass.SUBJECT_REFLECTION,
    }
)


class DecisionRoute(StrEnum):
    """The domain route a decision exposes to an outer orchestrator."""

    LIFE_QUEUE = "life_queue"
    CONTROL_LEDGER = "control_ledger"


class ProvenanceSource(StrEnum):
    """Typed origins; arbitrary metadata strings are not accepted."""

    USER = "user"
    HABITAT = "habitat"
    EXTERNAL_ADAPTER = "external_adapter"
    HUMAN_RESPONSE = "human_response"
    SUBJECT_TURN = "subject_turn"
    CONTROL_COORDINATOR = "control_coordinator"
    MANAGER = "manager"
    TEST_RUNNER = "test_runner"
    REPLAY = "replay"
    UNKNOWN = "unknown"


class ProvenanceMode(StrEnum):
    """How a source event is being presented at the current chain step."""

    DIRECT = "direct"
    CONTENT_PROPAGATION = "content_propagation"
    SUBJECT_REFLECTION = "subject_reflection"
    CONTROL_OBSERVATION = "control_observation"
    MANAGEMENT = "management"
    WAITING = "waiting"
    VERIFICATION = "verification"
    REPLAY_OR_PROJECTION = "replay_or_projection"
    UNKNOWN = "unknown"


_CONTROL_MODES = frozenset(
    {
        ProvenanceMode.CONTROL_OBSERVATION,
        ProvenanceMode.MANAGEMENT,
        ProvenanceMode.WAITING,
        ProvenanceMode.VERIFICATION,
        ProvenanceMode.REPLAY_OR_PROJECTION,
    }
)
_CONTROL_SOURCES = frozenset(
    {
        ProvenanceSource.CONTROL_COORDINATOR,
        ProvenanceSource.MANAGER,
        ProvenanceSource.TEST_RUNNER,
        ProvenanceSource.REPLAY,
    }
)
_EXTERNAL_SOURCES = frozenset(
    {
        ProvenanceSource.HABITAT,
        ProvenanceSource.EXTERNAL_ADAPTER,
        ProvenanceSource.HUMAN_RESPONSE,
    }
)


def _require_enum(value: Any, expected: type[StrEnum], field_name: str) -> None:
    if not isinstance(value, expected):
        raise TypeError(f"{field_name} must be {expected.__name__}, not a free-form label")


def _require_id(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    value = value.strip()
    if len(value) > _MAX_ID_LENGTH or "\x00" in value:
        raise ValueError(f"{field_name} is invalid or too long")
    return value


def _optional_id(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_id(value, field_name)


def _optional_text(value: str | None, field_name: str, *, limit: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be omitted or a non-empty string")
    value = value.strip()
    if len(value) > limit or "\x00" in value:
        raise ValueError(f"{field_name} is invalid or too long")
    return value


def _require_bool(value: bool, field_name: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{field_name} must be a bool")
    return value


def digest_payload(content: str) -> str:
    """Return a non-reversible digest suitable for a stimulus envelope."""

    if not isinstance(content, str):
        raise TypeError("content must be a string")
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _payload_digest(content: str | None, content_digest: str | None) -> str | None:
    if content is not None and content_digest is not None:
        raise ValueError("provide content or content_digest, not both")
    if content is not None:
        return digest_payload(content)
    return _optional_text(content_digest, "content_digest", limit=_MAX_CONTENT_DIGEST_LENGTH)


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ProvenanceStep:
    """One immutable, typed link in a bounded causal chain.

    ``confirmed`` is meaningful only for an external consequence.  For a
    subject reflection, ``settled`` and ``explicit_adoption`` jointly prove
    that the subject authored and adopted the reflection in a settled turn.
    A control continuation can therefore not be relabelled as a reflection by
    copying assistant text.
    """

    source_kind: ProvenanceSource
    mode: ProvenanceMode
    event_id: str
    source_id: str
    evidence_class: EvidenceClass = EvidenceClass.CONTRACT_ONLY
    predecessor_event_id: str | None = None
    source_turn_id: str | None = None
    source_lineage_id: str | None = None
    settled: bool = False
    explicit_adoption: bool = False
    capability: str | None = None
    external_effect_id: str | None = None
    confirmed: bool = False
    control_only: bool = False

    def __post_init__(self) -> None:
        _require_enum(self.source_kind, ProvenanceSource, "source_kind")
        _require_enum(self.mode, ProvenanceMode, "mode")
        _require_enum(self.evidence_class, EvidenceClass, "evidence_class")
        object.__setattr__(self, "event_id", _require_id(self.event_id, "event_id"))
        object.__setattr__(self, "source_id", _require_id(self.source_id, "source_id"))
        object.__setattr__(
            self,
            "predecessor_event_id",
            _optional_id(self.predecessor_event_id, "predecessor_event_id"),
        )
        object.__setattr__(self, "source_turn_id", _optional_id(self.source_turn_id, "source_turn_id"))
        object.__setattr__(
            self,
            "source_lineage_id",
            _optional_id(self.source_lineage_id, "source_lineage_id"),
        )
        object.__setattr__(self, "capability", _optional_text(self.capability, "capability", limit=_MAX_CAPABILITY_LENGTH))
        object.__setattr__(
            self,
            "external_effect_id",
            _optional_id(self.external_effect_id, "external_effect_id"),
        )
        for field_name in ("settled", "explicit_adoption", "confirmed", "control_only"):
            _require_bool(getattr(self, field_name), field_name)

    @classmethod
    def user_input(
        cls,
        *,
        event_id: str,
        user_id: str,
        evidence_class: EvidenceClass = EvidenceClass.CONTRACT_ONLY,
        predecessor_event_id: str | None = None,
    ) -> "ProvenanceStep":
        return cls(
            source_kind=ProvenanceSource.USER,
            mode=ProvenanceMode.DIRECT,
            event_id=event_id,
            source_id=user_id,
            evidence_class=evidence_class,
            predecessor_event_id=predecessor_event_id,
        )

    @classmethod
    def external_consequence(
        cls,
        *,
        event_id: str,
        adapter_id: str,
        external_effect_id: str,
        evidence_class: EvidenceClass = EvidenceClass.CONTRACT_ONLY,
        source_kind: ProvenanceSource = ProvenanceSource.EXTERNAL_ADAPTER,
        predecessor_event_id: str | None = None,
    ) -> "ProvenanceStep":
        if source_kind not in _EXTERNAL_SOURCES:
            raise ValueError("external consequence must come from a typed external source")
        return cls(
            source_kind=source_kind,
            mode=ProvenanceMode.DIRECT,
            event_id=event_id,
            source_id=adapter_id,
            evidence_class=evidence_class,
            predecessor_event_id=predecessor_event_id,
            external_effect_id=external_effect_id,
            confirmed=True,
        )

    @classmethod
    def content_propagation(
        cls,
        *,
        event_id: str,
        source_id: str,
        source_lineage_id: str,
        source_turn_id: str,
        evidence_class: EvidenceClass = EvidenceClass.CONTRACT_ONLY,
        settled: bool = True,
        predecessor_event_id: str | None = None,
    ) -> "ProvenanceStep":
        return cls(
            source_kind=ProvenanceSource.SUBJECT_TURN,
            mode=ProvenanceMode.CONTENT_PROPAGATION,
            event_id=event_id,
            source_id=source_id,
            evidence_class=evidence_class,
            predecessor_event_id=predecessor_event_id,
            source_turn_id=source_turn_id,
            source_lineage_id=source_lineage_id,
            settled=settled,
        )

    @classmethod
    def subject_reflection(
        cls,
        *,
        event_id: str,
        source_id: str,
        source_lineage_id: str,
        source_turn_id: str,
        capability: str,
        evidence_class: EvidenceClass = EvidenceClass.CONTRACT_ONLY,
        settled: bool = True,
        explicit_adoption: bool = True,
        predecessor_event_id: str | None = None,
    ) -> "ProvenanceStep":
        return cls(
            source_kind=ProvenanceSource.SUBJECT_TURN,
            mode=ProvenanceMode.SUBJECT_REFLECTION,
            event_id=event_id,
            source_id=source_id,
            evidence_class=evidence_class,
            predecessor_event_id=predecessor_event_id,
            source_turn_id=source_turn_id,
            source_lineage_id=source_lineage_id,
            settled=settled,
            explicit_adoption=explicit_adoption,
            capability=capability,
        )

    @classmethod
    def control_event(
        cls,
        *,
        event_id: str,
        source_id: str,
        mode: ProvenanceMode,
        evidence_class: EvidenceClass = EvidenceClass.CONTRACT_ONLY,
        predecessor_event_id: str | None = None,
    ) -> "ProvenanceStep":
        if mode not in _CONTROL_MODES:
            raise ValueError("control_event requires a control-plane mode")
        source_kind = {
            ProvenanceMode.MANAGEMENT: ProvenanceSource.MANAGER,
            ProvenanceMode.VERIFICATION: ProvenanceSource.TEST_RUNNER,
            ProvenanceMode.REPLAY_OR_PROJECTION: ProvenanceSource.REPLAY,
        }.get(mode, ProvenanceSource.CONTROL_COORDINATOR)
        return cls(
            source_kind=source_kind,
            mode=mode,
            event_id=event_id,
            source_id=source_id,
            evidence_class=evidence_class,
            predecessor_event_id=predecessor_event_id,
            control_only=True,
        )

    @classmethod
    def management(cls, **kwargs: Any) -> "ProvenanceStep":
        return cls.control_event(mode=ProvenanceMode.MANAGEMENT, **kwargs)

    @classmethod
    def waiting(cls, **kwargs: Any) -> "ProvenanceStep":
        return cls.control_event(mode=ProvenanceMode.WAITING, **kwargs)

    @classmethod
    def verification(cls, **kwargs: Any) -> "ProvenanceStep":
        return cls.control_event(mode=ProvenanceMode.VERIFICATION, **kwargs)

    @classmethod
    def replay_or_projection(cls, **kwargs: Any) -> "ProvenanceStep":
        return cls.control_event(mode=ProvenanceMode.REPLAY_OR_PROJECTION, **kwargs)

    @property
    def semantic_class(self) -> StimulusClass:
        """Derive a class from typed facts, never from arbitrary metadata."""

        if self.source_kind is ProvenanceSource.USER and self.mode is ProvenanceMode.DIRECT:
            return StimulusClass.USER_INPUT
        if self.source_kind in _EXTERNAL_SOURCES and self.mode is ProvenanceMode.DIRECT:
            return StimulusClass.EXTERNAL_CONSEQUENCE
        if self.source_kind is ProvenanceSource.SUBJECT_TURN:
            if self.mode is ProvenanceMode.CONTENT_PROPAGATION:
                return StimulusClass.CONTENT_PROPAGATION
            if self.mode is ProvenanceMode.SUBJECT_REFLECTION:
                return StimulusClass.SUBJECT_REFLECTION
        if self.mode is ProvenanceMode.CONTROL_OBSERVATION:
            return StimulusClass.CONTROL_OBSERVATION
        if self.mode is ProvenanceMode.MANAGEMENT or self.source_kind is ProvenanceSource.MANAGER:
            return StimulusClass.MANAGEMENT
        if self.mode is ProvenanceMode.WAITING:
            return StimulusClass.WAITING
        if self.mode is ProvenanceMode.VERIFICATION or self.source_kind is ProvenanceSource.TEST_RUNNER:
            return StimulusClass.VERIFICATION
        if self.mode is ProvenanceMode.REPLAY_OR_PROJECTION or self.source_kind is ProvenanceSource.REPLAY:
            return StimulusClass.REPLAY_OR_PROJECTION
        return StimulusClass.UNKNOWN

    @property
    def is_control_plane(self) -> bool:
        return self.control_only or self.source_kind in _CONTROL_SOURCES or self.mode in _CONTROL_MODES


@dataclass(frozen=True, slots=True)
class ProvenanceChain:
    """A finite, connected, immutable provenance chain."""

    steps: tuple[ProvenanceStep, ...]
    MAX_DEPTH: ClassVar[int] = MAX_PROVENANCE_DEPTH

    def __post_init__(self) -> None:
        if isinstance(self.steps, (str, bytes)):
            raise TypeError("steps must be an iterable of ProvenanceStep")
        steps = tuple(self.steps)
        if not steps:
            raise ValueError("provenance chain cannot be empty")
        if len(steps) > self.MAX_DEPTH:
            raise ValueError(f"provenance chain exceeds bounded depth {self.MAX_DEPTH}")
        for step in steps:
            if not isinstance(step, ProvenanceStep):
                raise TypeError("provenance chain contains a non-typed step")
        if steps[0].predecessor_event_id is not None:
            raise ValueError("the root provenance step cannot have a predecessor")
        event_ids = {steps[0].event_id}
        for previous, current in zip(steps[:-1], steps[1:], strict=True):
            if current.predecessor_event_id != previous.event_id:
                raise ValueError("provenance chain is disconnected")
            if current.event_id in event_ids:
                raise ValueError("provenance chain contains an event cycle")
            event_ids.add(current.event_id)
        object.__setattr__(self, "steps", steps)

    @classmethod
    def single(cls, step: ProvenanceStep) -> "ProvenanceChain":
        return cls((step,))

    @property
    def root(self) -> ProvenanceStep:
        return self.steps[0]

    @property
    def tip(self) -> ProvenanceStep:
        return self.steps[-1]

    @property
    def depth(self) -> int:
        return len(self.steps)

    @property
    def minimum_evidence(self) -> EvidenceClass:
        return min(self.steps, key=lambda step: _EVIDENCE_RANK[step.evidence_class]).evidence_class

    @property
    def contains_control_plane(self) -> bool:
        return any(step.is_control_plane for step in self.steps)

    def append(self, step: ProvenanceStep) -> "ProvenanceChain":
        if not isinstance(step, ProvenanceStep):
            raise TypeError("step must be a ProvenanceStep")
        predecessor = self.tip.event_id
        if step.predecessor_event_id not in (None, predecessor):
            raise ValueError("appended provenance step has the wrong predecessor")
        if step.predecessor_event_id is None:
            step = replace(step, predecessor_event_id=predecessor)
        return type(self)(self.steps + (step,))

    @property
    def digest(self) -> str:
        return _canonical_digest(
            [
                {
                    "source_kind": step.source_kind.value,
                    "mode": step.mode.value,
                    "event_id": step.event_id,
                    "source_id": step.source_id,
                    "evidence_class": step.evidence_class.value,
                    "predecessor_event_id": step.predecessor_event_id,
                    "source_turn_id": step.source_turn_id,
                    "source_lineage_id": step.source_lineage_id,
                    "settled": step.settled,
                    "explicit_adoption": step.explicit_adoption,
                    "capability": step.capability,
                    "external_effect_id": step.external_effect_id,
                    "confirmed": step.confirmed,
                    "control_only": step.control_only,
                }
                for step in self.steps
            ]
        )


@dataclass(frozen=True, slots=True)
class StimulusEnvelope:
    """A typed candidate entering the firewall.

    The envelope carries only a content digest, never a prompt or arbitrary
    metadata.  A caller may construct one directly for adversarial testing,
    but all source facts still have to be represented by typed provenance.
    """

    stimulus_id: str
    stimulus_class: StimulusClass
    provenance: ProvenanceChain
    target_lineage_id: str | None = None
    content_digest: str | None = None
    evidence_class: EvidenceClass | None = None
    external_effect_id: str | None = None
    flow: str | None = None

    def __post_init__(self) -> None:
        _require_enum(self.stimulus_class, StimulusClass, "stimulus_class")
        if not isinstance(self.provenance, ProvenanceChain):
            raise TypeError("provenance must be a ProvenanceChain")
        object.__setattr__(self, "stimulus_id", _require_id(self.stimulus_id, "stimulus_id"))
        object.__setattr__(self, "target_lineage_id", _optional_id(self.target_lineage_id, "target_lineage_id"))
        object.__setattr__(
            self,
            "content_digest",
            _optional_text(self.content_digest, "content_digest", limit=_MAX_CONTENT_DIGEST_LENGTH),
        )
        if self.evidence_class is None:
            object.__setattr__(self, "evidence_class", self.provenance.minimum_evidence)
        else:
            _require_enum(self.evidence_class, EvidenceClass, "evidence_class")
            if _EVIDENCE_RANK[self.evidence_class] > _EVIDENCE_RANK[self.provenance.minimum_evidence]:
                raise ValueError("envelope evidence cannot upgrade a weaker provenance step")
        object.__setattr__(
            self,
            "external_effect_id",
            _optional_id(self.external_effect_id, "external_effect_id"),
        )
        if self.external_effect_id is not None:
            effect_ids = {
                step.external_effect_id
                for step in self.provenance.steps
                if step.external_effect_id is not None
            }
            if self.external_effect_id not in effect_ids:
                raise ValueError("external_effect_id is not present in provenance")
        if self.flow is not None:
            if not isinstance(self.flow, str) or self.flow not in _ALLOWED_FLOWS:
                raise ValueError("flow must be one of content, spectrum, or tunnel")

    @classmethod
    def user_input(
        cls,
        stimulus_id: str,
        *,
        user_id: str,
        event_id: str,
        target_lineage_id: str,
        content: str | None = None,
        content_digest: str | None = None,
        evidence_class: EvidenceClass = EvidenceClass.CONTRACT_ONLY,
        flow: str = "content",
    ) -> "StimulusEnvelope":
        return cls(
            stimulus_id=stimulus_id,
            stimulus_class=StimulusClass.USER_INPUT,
            provenance=ProvenanceChain.single(
                ProvenanceStep.user_input(
                    event_id=event_id,
                    user_id=user_id,
                    evidence_class=evidence_class,
                )
            ),
            target_lineage_id=target_lineage_id,
            content_digest=_payload_digest(content, content_digest),
            evidence_class=evidence_class,
            flow=flow,
        )

    @classmethod
    def external_consequence(
        cls,
        stimulus_id: str,
        *,
        adapter_id: str,
        event_id: str,
        external_effect_id: str,
        target_lineage_id: str,
        content: str | None = None,
        content_digest: str | None = None,
        evidence_class: EvidenceClass = EvidenceClass.CONTRACT_ONLY,
        source_kind: ProvenanceSource = ProvenanceSource.EXTERNAL_ADAPTER,
        flow: str = "content",
    ) -> "StimulusEnvelope":
        return cls(
            stimulus_id=stimulus_id,
            stimulus_class=StimulusClass.EXTERNAL_CONSEQUENCE,
            provenance=ProvenanceChain.single(
                ProvenanceStep.external_consequence(
                    event_id=event_id,
                    adapter_id=adapter_id,
                    external_effect_id=external_effect_id,
                    evidence_class=evidence_class,
                    source_kind=source_kind,
                )
            ),
            target_lineage_id=target_lineage_id,
            content_digest=_payload_digest(content, content_digest),
            evidence_class=evidence_class,
            external_effect_id=external_effect_id,
            flow=flow,
        )

    @classmethod
    def content_propagation(
        cls,
        stimulus_id: str,
        *,
        source_id: str,
        source_lineage_id: str,
        source_turn_id: str,
        target_lineage_id: str,
        content: str | None = None,
        content_digest: str | None = None,
        evidence_class: EvidenceClass = EvidenceClass.CONTRACT_ONLY,
        settled: bool = True,
        flow: str = "content",
    ) -> "StimulusEnvelope":
        return cls(
            stimulus_id=stimulus_id,
            stimulus_class=StimulusClass.CONTENT_PROPAGATION,
            provenance=ProvenanceChain.single(
                ProvenanceStep.content_propagation(
                    event_id=source_turn_id,
                    source_id=source_id,
                    source_lineage_id=source_lineage_id,
                    source_turn_id=source_turn_id,
                    evidence_class=evidence_class,
                    settled=settled,
                )
            ),
            target_lineage_id=target_lineage_id,
            content_digest=_payload_digest(content, content_digest),
            evidence_class=evidence_class,
            flow=flow,
        )

    @classmethod
    def subject_reflection(
        cls,
        stimulus_id: str,
        *,
        lineage_id: str,
        source_turn_id: str,
        capability: str,
        target_lineage_id: str | None = None,
        content: str | None = None,
        content_digest: str | None = None,
        evidence_class: EvidenceClass = EvidenceClass.CONTRACT_ONLY,
        settled: bool = True,
        explicit_adoption: bool = True,
        flow: str = "content",
    ) -> "StimulusEnvelope":
        if target_lineage_id is None:
            target_lineage_id = lineage_id
        return cls(
            stimulus_id=stimulus_id,
            stimulus_class=StimulusClass.SUBJECT_REFLECTION,
            provenance=ProvenanceChain.single(
                ProvenanceStep.subject_reflection(
                    event_id=source_turn_id,
                    source_id=lineage_id,
                    source_lineage_id=lineage_id,
                    source_turn_id=source_turn_id,
                    capability=capability,
                    evidence_class=evidence_class,
                    settled=settled,
                    explicit_adoption=explicit_adoption,
                )
            ),
            target_lineage_id=target_lineage_id,
            content_digest=_payload_digest(content, content_digest),
            evidence_class=evidence_class,
            flow=flow,
        )

    @classmethod
    def control_event(
        cls,
        stimulus_id: str,
        *,
        event_id: str,
        source_id: str,
        stimulus_class: StimulusClass,
        mode: ProvenanceMode,
        evidence_class: EvidenceClass = EvidenceClass.CONTRACT_ONLY,
        target_lineage_id: str | None = None,
    ) -> "StimulusEnvelope":
        if stimulus_class not in _CONTROL_CLASSES:
            raise ValueError("control_event requires a control StimulusClass")
        return cls(
            stimulus_id=stimulus_id,
            stimulus_class=stimulus_class,
            provenance=ProvenanceChain.single(
                ProvenanceStep.control_event(
                    event_id=event_id,
                    source_id=source_id,
                    mode=mode,
                    evidence_class=evidence_class,
                )
            ),
            target_lineage_id=target_lineage_id,
            evidence_class=evidence_class,
        )

    @property
    def identity_digest(self) -> str:
        return _canonical_digest(
            {
                "stimulus_id": self.stimulus_id,
                "stimulus_class": self.stimulus_class.value,
                "provenance_digest": self.provenance.digest,
                "target_lineage_id": self.target_lineage_id,
                "content_digest": self.content_digest,
                "evidence_class": self.evidence_class.value,
                "external_effect_id": self.external_effect_id,
                "flow": self.flow,
            }
        )


@dataclass(frozen=True, slots=True)
class StimulusDecision:
    """Immutable outcome of one firewall evaluation."""

    stimulus_id: str
    stimulus_class: StimulusClass
    declared_class: StimulusClass
    evidence_class: EvidenceClass
    route: DecisionRoute
    reason_code: str
    provenance_digest: str
    life_queue_eligible: bool
    learning_eligible: bool
    spontaneous_activation_eligible: bool
    external_effect_id: str | None = None
    control_record_id: str | None = None

    def __post_init__(self) -> None:
        _require_enum(self.stimulus_class, StimulusClass, "stimulus_class")
        _require_enum(self.declared_class, StimulusClass, "declared_class")
        _require_enum(self.evidence_class, EvidenceClass, "evidence_class")
        _require_enum(self.route, DecisionRoute, "route")
        _require_id(self.stimulus_id, "stimulus_id")
        _require_id(self.reason_code, "reason_code")
        _require_id(self.provenance_digest, "provenance_digest")
        _optional_id(self.external_effect_id, "external_effect_id")
        _optional_id(self.control_record_id, "control_record_id")
        for field_name in (
            "life_queue_eligible",
            "learning_eligible",
            "spontaneous_activation_eligible",
        ):
            _require_bool(getattr(self, field_name), field_name)
        if self.life_queue_eligible:
            if self.route is not DecisionRoute.LIFE_QUEUE:
                raise ValueError("an eligible decision must route to the life queue")
            if not (
                self.learning_eligible and self.spontaneous_activation_eligible
            ):
                raise ValueError("life eligibility must carry all three life permissions")
            if self.stimulus_class not in _LIFE_CLASSES or self.evidence_class is not EvidenceClass.LIVE:
                raise ValueError("only live, eligible life classes may enter the life queue")
        else:
            if self.route is DecisionRoute.LIFE_QUEUE:
                raise ValueError("a denied decision cannot route to the life queue")
            if self.learning_eligible or self.spontaneous_activation_eligible:
                raise ValueError("denied decisions cannot update learning or activation")

    @property
    def accepted(self) -> bool:
        return self.life_queue_eligible

    @property
    def control_only(self) -> bool:
        return not self.life_queue_eligible

    @property
    def authoritative_class(self) -> StimulusClass:
        return self.stimulus_class

    @property
    def decision_digest(self) -> str:
        return _canonical_digest(
            {
                "stimulus_id": self.stimulus_id,
                "stimulus_class": self.stimulus_class.value,
                "declared_class": self.declared_class.value,
                "evidence_class": self.evidence_class.value,
                "route": self.route.value,
                "reason_code": self.reason_code,
                "provenance_digest": self.provenance_digest,
                "life_queue_eligible": self.life_queue_eligible,
                "learning_eligible": self.learning_eligible,
                "spontaneous_activation_eligible": self.spontaneous_activation_eligible,
                "external_effect_id": self.external_effect_id,
            }
        )


@dataclass(frozen=True, slots=True)
class ControlRecord:
    """Payload-free immutable audit entry for a non-life decision."""

    sequence: int
    record_id: str
    stimulus_id: str
    stimulus_class: StimulusClass
    declared_class: StimulusClass
    evidence_class: EvidenceClass
    route: DecisionRoute
    reason_code: str
    provenance_digest: str
    external_effect_id: str | None


class ControlLedgerCapacityError(RuntimeError):
    """The bounded control ledger is full; callers must fail closed."""


class ControlLedger:
    """An append-only, bounded, replay-safe control decision ledger.

    It deliberately stores no content, prompt, output, or arbitrary metadata.
    ``replay`` returns observations only; there is no method which turns a
    record back into a life stimulus.
    """

    def __init__(self, *, max_records: int = 4096) -> None:
        if type(max_records) is not int or max_records <= 0:
            raise ValueError("max_records must be a positive int")
        self._max_records = max_records
        self._records: list[ControlRecord] = []
        self._by_id: dict[str, ControlRecord] = {}
        self._lock = RLock()

    @property
    def max_records(self) -> int:
        return self._max_records

    def append(self, decision: StimulusDecision) -> ControlRecord:
        if not isinstance(decision, StimulusDecision):
            raise TypeError("ControlLedger accepts only StimulusDecision")
        if decision.life_queue_eligible:
            raise ValueError("a life-eligible decision cannot be written to ControlLedger")
        with self._lock:
            existing = self._by_id.get(decision.decision_digest)
            if existing is not None:
                return existing
            if len(self._records) >= self._max_records:
                raise ControlLedgerCapacityError("control ledger capacity exhausted")
            record = ControlRecord(
                sequence=len(self._records) + 1,
                record_id=decision.decision_digest,
                stimulus_id=decision.stimulus_id,
                stimulus_class=decision.stimulus_class,
                declared_class=decision.declared_class,
                evidence_class=decision.evidence_class,
                route=decision.route,
                reason_code=decision.reason_code,
                provenance_digest=decision.provenance_digest,
                external_effect_id=decision.external_effect_id,
            )
            self._records.append(record)
            self._by_id[record.record_id] = record
            return record

    def snapshot(self) -> tuple[ControlRecord, ...]:
        with self._lock:
            return tuple(self._records)

    def replay(self) -> tuple[ControlRecord, ...]:
        """Return control observations without invoking any life path."""

        return self.snapshot()

    def get(self, record_id: str) -> ControlRecord | None:
        record_id = _require_id(record_id, "record_id")
        with self._lock:
            return self._by_id.get(record_id)

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)


class StimulusFirewall:
    """Pure, fail-closed admission policy for typed stimulus envelopes."""

    implementation_evidence: ClassVar[EvidenceClass] = EvidenceClass.CONTRACT_ONLY

    def __init__(
        self,
        *,
        ledger: ControlLedger | None = None,
        max_seen_decisions: int = 4096,
        max_provenance_depth: int = MAX_PROVENANCE_DEPTH,
    ) -> None:
        if type(max_seen_decisions) is not int or max_seen_decisions <= 0:
            raise ValueError("max_seen_decisions must be a positive int")
        if type(max_provenance_depth) is not int or not 1 <= max_provenance_depth <= MAX_PROVENANCE_DEPTH:
            raise ValueError(f"max_provenance_depth must be between 1 and {MAX_PROVENANCE_DEPTH}")
        if ledger is not None and not isinstance(ledger, ControlLedger):
            raise TypeError("ledger must be a ControlLedger")
        self.ledger = ledger if ledger is not None else ControlLedger(max_records=max_seen_decisions)
        self.max_seen_decisions = max_seen_decisions
        self.max_provenance_depth = max_provenance_depth
        self._decisions: dict[str, tuple[str, StimulusDecision]] = {}
        self._external_effects: dict[str, str] = {}
        self._lock = RLock()

    def evaluate(self, envelope: StimulusEnvelope) -> StimulusDecision:
        """Evaluate one envelope; never enqueue, learn, pulse, or activate."""

        if not isinstance(envelope, StimulusEnvelope):
            raise TypeError("StimulusFirewall accepts only a typed StimulusEnvelope")
        with self._lock:
            cached = self._decisions.get(envelope.stimulus_id)
            if cached is not None:
                identity_digest, decision = cached
                if identity_digest == envelope.identity_digest:
                    return decision
                return self._deny(
                    envelope,
                    effective_class=StimulusClass.UNKNOWN,
                    reason_code="stimulus_id_collision",
                    remember=False,
                )
            if len(self._decisions) >= self.max_seen_decisions:
                return self._deny(
                    envelope,
                    effective_class=StimulusClass.UNKNOWN,
                    reason_code="firewall_capacity_exhausted",
                    remember=False,
                )
            if envelope.provenance.depth > self.max_provenance_depth:
                return self._deny(
                    envelope,
                    effective_class=StimulusClass.UNKNOWN,
                    reason_code="provenance_depth_exceeded",
                )

            effective_class = self._authoritative_class(envelope.provenance)
            if envelope.stimulus_class is not effective_class:
                return self._deny(
                    envelope,
                    effective_class=effective_class,
                    reason_code="stimulus_class_spoofed",
                )
            if envelope.provenance.contains_control_plane:
                return self._deny(
                    envelope,
                    effective_class=effective_class,
                    reason_code="control_provenance_contamination",
                )

            reason = self._eligible_reason(envelope, effective_class)
            if reason is not None:
                return self._deny(envelope, effective_class=effective_class, reason_code=reason)

            effect_id = envelope.external_effect_id
            if effect_id is not None and effect_id in self._external_effects:
                return self._deny(
                    envelope,
                    effective_class=effective_class,
                    reason_code="external_effect_replayed",
                )
            decision = StimulusDecision(
                stimulus_id=envelope.stimulus_id,
                stimulus_class=effective_class,
                declared_class=envelope.stimulus_class,
                evidence_class=envelope.evidence_class,
                route=DecisionRoute.LIFE_QUEUE,
                reason_code="eligible",
                provenance_digest=envelope.provenance.digest,
                life_queue_eligible=True,
                learning_eligible=True,
                spontaneous_activation_eligible=True,
                external_effect_id=effect_id,
            )
            self._remember(envelope, decision)
            if effect_id is not None:
                self._external_effects[effect_id] = envelope.stimulus_id
            return decision

    decide = evaluate

    def _remember(self, envelope: StimulusEnvelope, decision: StimulusDecision) -> None:
        self._decisions[envelope.stimulus_id] = (envelope.identity_digest, decision)

    def _deny(
        self,
        envelope: StimulusEnvelope,
        *,
        effective_class: StimulusClass,
        reason_code: str,
        remember: bool = True,
    ) -> StimulusDecision:
        decision = StimulusDecision(
            stimulus_id=envelope.stimulus_id,
            stimulus_class=effective_class,
            declared_class=envelope.stimulus_class,
            evidence_class=envelope.evidence_class,
            route=DecisionRoute.CONTROL_LEDGER,
            reason_code=reason_code,
            provenance_digest=envelope.provenance.digest,
            life_queue_eligible=False,
            learning_eligible=False,
            spontaneous_activation_eligible=False,
            external_effect_id=envelope.external_effect_id,
        )
        try:
            record = self.ledger.append(decision)
            decision = replace(decision, control_record_id=record.record_id)
        except ControlLedgerCapacityError:
            # A full audit surface cannot become permission to enter life.
            decision = replace(decision, reason_code="control_ledger_capacity")
        if remember:
            self._remember(envelope, decision)
        return decision

    @staticmethod
    def _authoritative_class(chain: ProvenanceChain) -> StimulusClass:
        control_steps = [step for step in chain.steps if step.is_control_plane]
        if control_steps:
            effective = control_steps[-1].semantic_class
            return effective if effective in _CONTROL_CLASSES else StimulusClass.CONTROL_OBSERVATION
        return chain.tip.semantic_class

    @staticmethod
    def _eligible_reason(envelope: StimulusEnvelope, stimulus_class: StimulusClass) -> str | None:
        if stimulus_class not in _LIFE_CLASSES:
            return "unknown_stimulus_class"
        if envelope.evidence_class is not EvidenceClass.LIVE:
            return "evidence_not_live"
        if envelope.provenance.minimum_evidence is not EvidenceClass.LIVE:
            return "provenance_evidence_not_live"
        if envelope.target_lineage_id is None:
            return "missing_target_lineage"
        if envelope.content_digest is None:
            return "missing_content_digest"

        tip = envelope.provenance.tip
        if stimulus_class is StimulusClass.USER_INPUT:
            if tip.source_kind is not ProvenanceSource.USER or tip.mode is not ProvenanceMode.DIRECT:
                return "user_input_provenance_invalid"
            return None

        if stimulus_class is StimulusClass.EXTERNAL_CONSEQUENCE:
            if tip.source_kind not in _EXTERNAL_SOURCES or tip.mode is not ProvenanceMode.DIRECT:
                return "external_source_invalid"
            if not tip.confirmed or tip.external_effect_id is None:
                return "external_effect_not_confirmed"
            if envelope.external_effect_id != tip.external_effect_id:
                return "external_effect_identity_mismatch"
            effect_ids = [
                step.external_effect_id
                for step in envelope.provenance.steps
                if step.external_effect_id is not None
            ]
            if len(effect_ids) != len(set(effect_ids)):
                return "external_effect_chain_ambiguous"
            return None

        if stimulus_class is StimulusClass.CONTENT_PROPAGATION:
            if tip.source_kind is not ProvenanceSource.SUBJECT_TURN:
                return "content_source_invalid"
            if tip.mode is not ProvenanceMode.CONTENT_PROPAGATION:
                return "content_mode_invalid"
            if not tip.settled or tip.source_turn_id is None or tip.source_lineage_id is None:
                return "content_source_not_settled"
            return None

        if stimulus_class is StimulusClass.SUBJECT_REFLECTION:
            if tip.source_kind is not ProvenanceSource.SUBJECT_TURN:
                return "reflection_source_invalid"
            if tip.mode is not ProvenanceMode.SUBJECT_REFLECTION:
                return "reflection_mode_invalid"
            if not tip.settled or not tip.explicit_adoption:
                return "reflection_not_explicitly_adopted"
            if tip.source_turn_id is None or tip.source_lineage_id != envelope.target_lineage_id:
                return "reflection_lineage_invalid"
            if tip.capability not in _REFLECTION_CAPABILITIES:
                return "reflection_capability_invalid"
            return None

        return "unknown_stimulus_class"

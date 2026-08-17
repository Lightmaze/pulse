"""Machine-enforced boundaries for the organism's three information flows.

``flow=None`` is deliberately retained for same-subject/internal causal facts.
It is not a fourth cross-subject flow. CONTENT carries natural-language
semantics across ordinary connections, SPECTRUM is a non-executable numeric
sideband, and TUNNEL is the addressed delegation path.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from pulse_system.core.types import (
    CausalEventDomain,
    CausalEventFlow,
    CausalEventKind,
    CausalEventSource,
    CausalEventStatus,
)


class CausalFlowInvariantError(ValueError):
    """Raised before an event can cross a frozen information-flow boundary."""


class CausalFlowRecord(Protocol):
    flow: CausalEventFlow | None
    domain: CausalEventDomain
    kind: CausalEventKind
    source: CausalEventSource
    status: CausalEventStatus
    content: str | None
    metadata: Mapping[str, Any]
    parent_event_id: str | None
    engram_id: str | None


NON_TURN_ROOT_KINDS = frozenset(
    {
        CausalEventKind.TOOL_CALL,
        CausalEventKind.TOOL_RESULT,
        CausalEventKind.HABITAT_ACTION,
        CausalEventKind.HABITAT_CONSEQUENCE,
        CausalEventKind.GENERATION_TRANSITION,
        CausalEventKind.ASSISTANT_RESULT,
    }
)
NON_TURN_ROOT_KIND_VALUES = frozenset(kind.value for kind in NON_TURN_ROOT_KINDS)

_SPECTRUM_KINDS = frozenset(
    {CausalEventKind.PULSE, CausalEventKind.SYSTEM}
)
_SPECTRUM_DOMAINS = frozenset(
    {CausalEventDomain.PULSE, CausalEventDomain.SYSTEM}
)
_SPECTRUM_SOURCES = frozenset(
    {CausalEventSource.SELF, CausalEventSource.SYSTEM}
)
_SPECTRUM_NONTERMINAL = frozenset(
    {CausalEventStatus.QUEUED, CausalEventStatus.RUNNING}
)
_TUNNEL_KINDS = frozenset(
    {
        CausalEventKind.DELEGATION_REQUEST,
        CausalEventKind.DELEGATION_RESULT,
        CausalEventKind.SYSTEM,
    }
)
_DELEGATION_KINDS = frozenset(
    {CausalEventKind.DELEGATION_REQUEST, CausalEventKind.DELEGATION_RESULT}
)
_SYMBOL = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}\Z")


def _nonempty_content(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _spectrum_metadata_is_symbolic(value: Any) -> bool:
    """Reject prose smuggled through a nominally content-free sideband."""

    if isinstance(value, Mapping):
        return all(
            isinstance(key, str)
            and _SYMBOL.fullmatch(key) is not None
            and _spectrum_metadata_is_symbolic(child)
            for key, child in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        return all(_spectrum_metadata_is_symbolic(child) for child in value)
    if value is None or isinstance(value, (bool, int)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    return isinstance(value, str) and _SYMBOL.fullmatch(value) is not None


def causal_flow_violation_codes(event: CausalFlowRecord) -> tuple[str, ...]:
    """Return stable codes for every violated durable flow invariant."""

    violations: list[str] = []
    flow = event.flow

    if event.status is CausalEventStatus.QUEUED and event.kind in NON_TURN_ROOT_KINDS:
        violations.append("non_turn_event_cannot_queue")

    if flow is CausalEventFlow.CONTENT:
        if not _nonempty_content(event.content):
            violations.append("content_flow_requires_natural_content")
        if event.kind in _DELEGATION_KINDS:
            violations.append("delegation_kind_requires_tunnel")

    if flow is CausalEventFlow.SPECTRUM:
        if event.content is not None:
            violations.append("spectrum_content_forbidden")
        if event.status in _SPECTRUM_NONTERMINAL:
            violations.append("spectrum_cannot_execute")
        if event.kind not in _SPECTRUM_KINDS:
            violations.append("spectrum_kind_invalid")
        if event.domain not in _SPECTRUM_DOMAINS:
            violations.append("spectrum_domain_invalid")
        if event.source not in _SPECTRUM_SOURCES:
            violations.append("spectrum_source_invalid")
        if not _spectrum_metadata_is_symbolic(event.metadata):
            violations.append("spectrum_metadata_not_numeric_or_symbolic")

    if flow is CausalEventFlow.TUNNEL:
        if not _nonempty_content(event.content):
            violations.append("tunnel_requires_natural_content")
        if event.source is not CausalEventSource.DELEGATION:
            violations.append("tunnel_requires_delegation_source")
        if event.domain is not CausalEventDomain.SYSTEM:
            violations.append("tunnel_requires_system_domain")
        if event.kind not in _TUNNEL_KINDS:
            violations.append("tunnel_kind_invalid")

    if event.kind is CausalEventKind.PROPAGATION:
        if flow is not CausalEventFlow.CONTENT:
            violations.append("propagation_requires_content_flow")
        if event.source is not CausalEventSource.PROPAGATION:
            violations.append("propagation_requires_propagation_source")
        if event.parent_event_id is None:
            violations.append("propagation_requires_parent")
        if event.engram_id is None:
            violations.append("propagation_requires_target_engram")
        if not _nonempty_content(event.content):
            violations.append("propagation_requires_natural_content")
    elif event.source is CausalEventSource.PROPAGATION:
        violations.append("propagation_source_requires_propagation_kind")

    if event.kind in _DELEGATION_KINDS:
        if flow is not CausalEventFlow.TUNNEL:
            violations.append("delegation_kind_requires_tunnel")
        if event.source is not CausalEventSource.DELEGATION:
            violations.append("delegation_kind_requires_delegation_source")
    elif event.source is CausalEventSource.DELEGATION:
        if not (
            flow is CausalEventFlow.TUNNEL
            and event.kind is CausalEventKind.SYSTEM
        ):
            violations.append("delegation_source_requires_tunnel_kind")

    return tuple(dict.fromkeys(violations))


def causal_turn_violation_codes(event: CausalFlowRecord) -> tuple[str, ...]:
    """Return flow plus claimability violations for one prospective turn."""

    violations = list(causal_flow_violation_codes(event))
    if event.kind in NON_TURN_ROOT_KINDS:
        violations.append("event_kind_is_not_turn_root")
    if event.engram_id is None:
        violations.append("turn_requires_target_engram")
    if event.flow is CausalEventFlow.SPECTRUM:
        violations.append("spectrum_cannot_enter_harness")
    return tuple(dict.fromkeys(violations))


def assert_causal_flow(event: CausalFlowRecord) -> None:
    violations = causal_flow_violation_codes(event)
    if violations:
        raise CausalFlowInvariantError(
            "causal flow invariant violated: " + ", ".join(violations)
        )


def assert_causal_turn(event: CausalFlowRecord) -> None:
    violations = causal_turn_violation_codes(event)
    if violations:
        raise CausalFlowInvariantError(
            "causal turn flow invariant violated: " + ", ".join(violations)
        )


def may_emit_content_propagation(event: CausalFlowRecord) -> bool:
    """Return whether a settled turn may create new CONTENT deliveries."""

    return event.flow in {None, CausalEventFlow.CONTENT}


__all__ = [
    "CausalFlowInvariantError",
    "NON_TURN_ROOT_KINDS",
    "NON_TURN_ROOT_KIND_VALUES",
    "assert_causal_flow",
    "assert_causal_turn",
    "causal_flow_violation_codes",
    "causal_turn_violation_codes",
    "may_emit_content_propagation",
]

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from pulse_system.agent.harness.stimulus_firewall import (
    CONTRACT_ONLY,
    LIVE,
    LIVE_GATE_UNVERIFIED,
    ControlLedger,
    DecisionRoute,
    EvidenceClass,
    ProvenanceChain,
    ProvenanceMode,
    ProvenanceSource,
    ProvenanceStep,
    StimulusClass,
    StimulusEnvelope,
    StimulusFirewall,
)


def _live_external(stimulus_id: str = "stimulus-external", effect_id: str = "effect-1") -> StimulusEnvelope:
    return StimulusEnvelope.external_consequence(
        stimulus_id,
        adapter_id="habitat-file",
        event_id=f"event-{stimulus_id}",
        external_effect_id=effect_id,
        target_lineage_id="lineage-1",
        content="a confirmed external change",
        evidence_class=EvidenceClass.LIVE,
    )


def _live_project(stimulus_id: str = "stimulus-project") -> StimulusEnvelope:
    return StimulusEnvelope.subject_reflection(
        stimulus_id,
        lineage_id="lineage-1",
        source_turn_id=f"turn-{stimulus_id}",
        capability="subject_project",
        content="I will continue my own long-running project.",
        evidence_class=EvidenceClass.LIVE,
    )


def test_stimulus_classes_and_decisions_are_immutable() -> None:
    firewall = StimulusFirewall()
    decision = firewall.evaluate(_live_external())

    assert decision.accepted is True
    assert decision.stimulus_class is StimulusClass.EXTERNAL_CONSEQUENCE
    assert decision.evidence_class.value == LIVE
    with pytest.raises(FrozenInstanceError):
        decision.reason_code = "management"  # type: ignore[misc]

    envelope = _live_external()
    with pytest.raises(FrozenInstanceError):
        envelope.stimulus_class = StimulusClass.MANAGEMENT  # type: ignore[misc]


@pytest.mark.parametrize("evidence", [EvidenceClass.CONTRACT_ONLY, EvidenceClass.LIVE_GATE_UNVERIFIED])
def test_unverified_external_evidence_never_becomes_life_stimulus(evidence: EvidenceClass) -> None:
    envelope = StimulusEnvelope.external_consequence(
        "unverified-external",
        adapter_id="mcp-adapter",
        event_id="external-event",
        external_effect_id="effect-unverified",
        target_lineage_id="lineage-1",
        content="possibly changed",
        evidence_class=evidence,
    )
    decision = StimulusFirewall().evaluate(envelope)

    assert decision.route is DecisionRoute.CONTROL_LEDGER
    assert decision.life_queue_eligible is False
    assert decision.learning_eligible is False
    assert decision.spontaneous_activation_eligible is False
    assert decision.reason_code in {"evidence_not_live", "provenance_evidence_not_live"}
    assert decision.evidence_class.value == evidence.value


def test_real_external_consequence_and_subject_project_can_enter_life_lane() -> None:
    firewall = StimulusFirewall()

    external = firewall.evaluate(_live_external())
    project = firewall.evaluate(_live_project())

    for decision in (external, project):
        assert decision.route is DecisionRoute.LIFE_QUEUE
        assert decision.life_queue_eligible is True
        assert decision.learning_eligible is True
        assert decision.spontaneous_activation_eligible is True
    assert project.stimulus_class is StimulusClass.SUBJECT_REFLECTION


def test_live_user_input_and_settled_content_propagation_are_eligible() -> None:
    firewall = StimulusFirewall()
    user = StimulusEnvelope.user_input(
        "user-stimulus",
        user_id="user-1",
        event_id="ui-1",
        target_lineage_id="lineage-1",
        content="a real user message",
        evidence_class=EvidenceClass.LIVE,
    )
    content = StimulusEnvelope.content_propagation(
        "content-stimulus",
        source_id="engram-2",
        source_lineage_id="lineage-2",
        source_turn_id="turn-2",
        target_lineage_id="lineage-1",
        content="settled subject content",
        evidence_class=EvidenceClass.LIVE,
    )

    assert firewall.evaluate(user).accepted is True
    assert firewall.evaluate(content).accepted is True


@pytest.mark.parametrize(
    ("stimulus_class", "mode"),
    [
        (StimulusClass.CONTROL_OBSERVATION, ProvenanceMode.CONTROL_OBSERVATION),
        (StimulusClass.MANAGEMENT, ProvenanceMode.MANAGEMENT),
        (StimulusClass.WAITING, ProvenanceMode.WAITING),
        (StimulusClass.VERIFICATION, ProvenanceMode.VERIFICATION),
        (StimulusClass.REPLAY_OR_PROJECTION, ProvenanceMode.REPLAY_OR_PROJECTION),
    ],
)
def test_control_classes_can_only_be_observed_in_control_ledger(
    stimulus_class: StimulusClass,
    mode: ProvenanceMode,
) -> None:
    ledger = ControlLedger()
    firewall = StimulusFirewall(ledger=ledger)
    envelope = StimulusEnvelope.control_event(
        f"control-{mode.value}",
        event_id=f"control-event-{mode.value}",
        source_id="coordinator",
        stimulus_class=stimulus_class,
        mode=mode,
        evidence_class=EvidenceClass.LIVE_GATE_UNVERIFIED,
    )

    decision = firewall.evaluate(envelope)

    assert decision.route is DecisionRoute.CONTROL_LEDGER
    assert decision.control_only is True
    assert not decision.life_queue_eligible
    assert not decision.learning_eligible
    assert not decision.spontaneous_activation_eligible
    assert decision.control_record_id is not None
    assert len(ledger) == 1
    assert ledger.replay()[0].reason_code == "control_provenance_contamination"


def test_external_label_cannot_override_management_provenance() -> None:
    envelope = StimulusEnvelope(
        stimulus_id="spoofed-external",
        stimulus_class=StimulusClass.EXTERNAL_CONSEQUENCE,
        provenance=ProvenanceChain.single(
            ProvenanceStep.management(
                event_id="manager-event",
                source_id="lead-coordinator",
                evidence_class=EvidenceClass.LIVE,
            )
        ),
        target_lineage_id="lineage-1",
        content_digest="content-digest",
        evidence_class=EvidenceClass.LIVE,
    )

    decision = StimulusFirewall().evaluate(envelope)

    assert decision.stimulus_class is StimulusClass.MANAGEMENT
    assert decision.declared_class is StimulusClass.EXTERNAL_CONSEQUENCE
    assert decision.reason_code == "stimulus_class_spoofed"
    assert decision.life_queue_eligible is False


def test_control_step_contaminates_an_otherwise_external_chain() -> None:
    root = ProvenanceStep.external_consequence(
        event_id="external-root",
        adapter_id="adapter",
        external_effect_id="effect-chain",
        evidence_class=EvidenceClass.LIVE,
    )
    chain = ProvenanceChain.single(root).append(
        ProvenanceStep.waiting(
            event_id="waiting-projection",
            source_id="coordinator",
            evidence_class=EvidenceClass.LIVE,
        )
    )
    envelope = StimulusEnvelope(
        stimulus_id="contaminated-chain",
        stimulus_class=StimulusClass.EXTERNAL_CONSEQUENCE,
        provenance=chain,
        target_lineage_id="lineage-1",
        content_digest="content-digest",
        evidence_class=EvidenceClass.LIVE,
        external_effect_id="effect-chain",
    )

    decision = StimulusFirewall().evaluate(envelope)

    assert decision.reason_code == "stimulus_class_spoofed"
    assert decision.stimulus_class is StimulusClass.WAITING
    assert not decision.life_queue_eligible


@pytest.mark.parametrize(
    "step",
    [
        ProvenanceStep.subject_reflection(
            event_id="unsettled-turn",
            source_id="lineage-1",
            source_lineage_id="lineage-1",
            source_turn_id="turn-unsettled",
            capability="subject_project",
            evidence_class=EvidenceClass.LIVE,
            settled=False,
        ),
        ProvenanceStep.subject_reflection(
            event_id="unadopted-turn",
            source_id="lineage-1",
            source_lineage_id="lineage-1",
            source_turn_id="turn-unadopted",
            capability="subject_project",
            evidence_class=EvidenceClass.LIVE,
            explicit_adoption=False,
        ),
    ],
)
def test_reflection_requires_a_settled_explicit_subject_turn(step: ProvenanceStep) -> None:
    envelope = StimulusEnvelope(
        stimulus_id=step.event_id,
        stimulus_class=StimulusClass.SUBJECT_REFLECTION,
        provenance=ProvenanceChain.single(step),
        target_lineage_id="lineage-1",
        content_digest="content-digest",
        evidence_class=EvidenceClass.LIVE,
    )

    decision = StimulusFirewall().evaluate(envelope)

    assert decision.reason_code in {
        "reflection_not_explicitly_adopted",
        "reflection_lineage_invalid",
    }
    assert decision.control_only


def test_bounded_connected_provenance_rejects_cycles_and_long_chains() -> None:
    first = ProvenanceStep.user_input(
        event_id="event-0",
        user_id="user-1",
        evidence_class=EvidenceClass.LIVE,
    )
    chain = ProvenanceChain.single(first)
    for index in range(1, 8):
        chain = chain.append(
            ProvenanceStep.content_propagation(
                event_id=f"event-{index}",
                source_id=f"engram-{index}",
                source_lineage_id="lineage-1",
                source_turn_id=f"turn-{index}",
                evidence_class=EvidenceClass.LIVE,
            )
        )
    assert chain.depth == 8
    with pytest.raises(ValueError, match="bounded depth"):
        chain.append(
            ProvenanceStep.content_propagation(
                event_id="event-too-deep",
                source_id="engram-too-deep",
                source_lineage_id="lineage-1",
                source_turn_id="turn-too-deep",
                evidence_class=EvidenceClass.LIVE,
            )
        )

    with pytest.raises(ValueError, match="disconnected"):
        ProvenanceChain(
            (
                first,
                ProvenanceStep.content_propagation(
                    event_id="event-2",
                    source_id="engram-2",
                    source_lineage_id="lineage-1",
                    source_turn_id="turn-2",
                    evidence_class=EvidenceClass.LIVE,
                    predecessor_event_id="not-event-0",
                ),
            )
        )


def test_external_effect_is_idempotent_and_stable_on_same_retry() -> None:
    firewall = StimulusFirewall()
    first_envelope = _live_external("first", "effect-once")
    retry = _live_external("first", "effect-once")
    duplicate_effect = _live_external("second", "effect-once")

    first = firewall.evaluate(first_envelope)
    same = firewall.evaluate(retry)
    duplicate = firewall.evaluate(duplicate_effect)

    assert same == first
    assert duplicate.reason_code == "external_effect_replayed"
    assert duplicate.control_only
    assert len(firewall.ledger) == 1


def test_collision_and_capacity_rejections_do_not_overwrite_bounded_state() -> None:
    firewall = StimulusFirewall(max_seen_decisions=1, ledger=ControlLedger(max_records=8))
    first = _live_external("stable-id", "stable-effect")
    collision = StimulusEnvelope.external_consequence(
        "stable-id",
        adapter_id="different-adapter",
        event_id="different-event",
        external_effect_id="different-effect",
        target_lineage_id="lineage-1",
        content="different payload",
        evidence_class=EvidenceClass.LIVE,
    )
    other = _live_project("over-capacity")

    accepted = firewall.evaluate(first)
    assert firewall.evaluate(collision).reason_code == "stimulus_id_collision"
    assert firewall.evaluate(first) == accepted

    capacity = firewall.evaluate(other)
    assert capacity.reason_code == "firewall_capacity_exhausted"
    assert firewall.evaluate(first) == accepted


def test_replay_is_observation_only_and_never_reinvokes_gate() -> None:
    ledger = ControlLedger()
    firewall = StimulusFirewall(ledger=ledger)
    denied = firewall.evaluate(
        StimulusEnvelope.control_event(
            "replay-control",
            event_id="replay-event",
            source_id="workbench",
            stimulus_class=StimulusClass.REPLAY_OR_PROJECTION,
            mode=ProvenanceMode.REPLAY_OR_PROJECTION,
        )
    )

    replayed = ledger.replay()

    assert denied.control_record_id == replayed[0].record_id
    assert all(record.route is DecisionRoute.CONTROL_LEDGER for record in replayed)
    assert not hasattr(ledger, "enqueue")
    assert len(ledger) == 1


def test_unknown_or_free_form_labels_fail_closed() -> None:
    with pytest.raises(TypeError, match="free-form label"):
        ProvenanceStep(
            source_kind="external_adapter",  # type: ignore[arg-type]
            mode=ProvenanceMode.DIRECT,
            event_id="event",
            source_id="adapter",
        )
    with pytest.raises(TypeError, match="free-form label"):
        StimulusEnvelope(
            stimulus_id="unknown-class",
            stimulus_class="external_consequence",  # type: ignore[arg-type]
            provenance=ProvenanceChain.single(
                ProvenanceStep(
                    source_kind=ProvenanceSource.UNKNOWN,
                    mode=ProvenanceMode.UNKNOWN,
                    event_id="event-unknown",
                    source_id="unknown-source",
                )
            ),
        )


def test_ledger_is_bounded_and_does_not_store_payload() -> None:
    ledger = ControlLedger(max_records=1)
    firewall = StimulusFirewall(ledger=ledger)
    first = StimulusEnvelope.control_event(
        "control-one",
        event_id="control-one-event",
        source_id="manager",
        stimulus_class=StimulusClass.MANAGEMENT,
        mode=ProvenanceMode.MANAGEMENT,
    )
    second = StimulusEnvelope.control_event(
        "control-two",
        event_id="control-two-event",
        source_id="manager",
        stimulus_class=StimulusClass.MANAGEMENT,
        mode=ProvenanceMode.MANAGEMENT,
    )

    firewall.evaluate(first)
    capacity_decision = firewall.evaluate(second)

    assert len(ledger) == 1
    assert capacity_decision.reason_code == "control_ledger_capacity"
    assert not hasattr(ledger.replay()[0], "content")
    assert not hasattr(ledger.replay()[0], "payload")


def test_firewall_implementation_evidence_stays_contract_only() -> None:
    assert StimulusFirewall.implementation_evidence is EvidenceClass.CONTRACT_ONLY
    assert CONTRACT_ONLY == "CONTRACT_ONLY"
    assert LIVE_GATE_UNVERIFIED == "LIVE_GATE_UNVERIFIED"
    assert LIVE == "LIVE"

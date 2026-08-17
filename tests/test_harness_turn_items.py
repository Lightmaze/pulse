"""Focused contract tests for the independent turn-item projection."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pulse_system.agent.harness.events import (
    HarnessEvent,
    HarnessEventKind,
    HarnessEventPhase,
    HarnessEventSource,
    HarnessEventStatus,
)
from pulse_system.agent.harness.turn_items import (
    CONTRACT,
    LIVE,
    LIVE_GATE_UNVERIFIED,
    TurnItemIndex,
    TurnItemProjector,
    project_turn_items,
)


def _event(
    event_id: str,
    seq: int,
    kind: str,
    *,
    payload: dict[str, object],
    status: str = "running",
    phase: str | None = None,
    source: str = "pi_rpc",
    turn_id: str = "turn-1",
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "turn_id": turn_id,
        "world_id": "world-1",
        "engram_id": "engram-1",
        "seq": seq,
        "kind": kind,
        "phase": phase or ("terminal" if kind.endswith("completed") else "stream"),
        "source": source,
        "status": status,
        "occurred_at": f"2026-08-04T16:00:{seq:02d}+00:00",
        "payload_json": payload,
    }


def test_pi_broker_terminal_layers_share_one_stable_item_and_keep_history() -> None:
    index = TurnItemProjector()
    updates = index.ingest_many(
        [
            _event(
                "pi-start",
                1,
                "tool_started",
                payload={
                    "toolCallId": "call-7",
                    "tool_name": "write",
                    "evidence_class": "LIVE_PI_PROVIDER",
                },
                phase="stream",
            ),
            _event(
                "broker-approval",
                2,
                "approval_requested",
                payload={
                    "data": {
                        "action_request_id": "call-7",
                        "evidence_class": "CONTRACT_ONLY",
                    }
                },
                phase="approval",
                source="policy",
            ),
            _event(
                "broker-resolved",
                3,
                "approval_resolved",
                payload={
                    "action_request_id": "call-7",
                    "evidence_class": "CONTRACT_ONLY",
                },
                phase="approval",
                source="pulse_control",
                status="completed",
            ),
            _event(
                "terminal-write",
                4,
                "tool_completed",
                payload={
                    "action_request_id": "call-7",
                    "tool_name": "write",
                    "execution_status": "completed",
                    "evidence_class": "LIVE_WORKSPACE_CHECKPOINTED",
                },
                phase="terminal",
                source="terminal",
                status="completed",
            ),
        ],
        sort_by_sequence=True,
    )

    replay = index.replay("turn-1")
    assert [update.accepted for update in updates] == [True, True, True, True]
    assert len(replay.items) == 1
    item = replay.items[0]
    assert item.item_id == updates[0].item_id == updates[-1].item_id
    assert item.tool_call_id == "call-7"
    assert item.terminal is True
    assert item.state == "completed"
    assert item.phase == "terminal"
    assert item.phase_history == ("stream", "approval", "terminal")
    assert [entry.source for entry in item.history] == [
        "pi_rpc",
        "policy",
        "pulse_control",
        "terminal",
    ]
    assert item.evidence_classes == (
        "LIVE_PI_PROVIDER",
        "CONTRACT_ONLY",
        "LIVE_WORKSPACE_CHECKPOINTED",
    )
    assert item.evidence_class == CONTRACT
    assert item.evidence_level == CONTRACT


def test_same_event_is_idempotent_but_changed_event_and_seq_are_conflicts() -> None:
    index = TurnItemIndex()
    first = _event(
        "same-event",
        1,
        "tool_started",
        payload={"toolCallId": "call-1", "tool_name": "edit"},
    )
    assert index.ingest(first).accepted is True
    duplicate = index.ingest(dict(first))
    assert duplicate.accepted is True and duplicate.duplicate is True
    changed = dict(first)
    changed["payload_json"] = {"toolCallId": "call-1", "tool_name": "write"}
    conflict = index.ingest(changed)
    assert conflict.accepted is False
    assert {item.code for item in conflict.conflicts} == {"event_id_conflict"}

    different_id_same_seq = _event(
        "other-event",
        1,
        "tool_started",
        payload={"toolCallId": "call-1", "tool_name": "edit"},
    )
    sequence_conflict = index.ingest(different_id_same_seq)
    assert sequence_conflict.accepted is False
    assert {item.code for item in sequence_conflict.conflicts} == {"sequence_conflict"}
    assert len(index.replay("turn-1").items[0].history) == 1


def test_sequence_gap_is_reported_and_filled_sequence_removes_inferred_gap() -> None:
    index = TurnItemIndex()
    index.ingest(
        _event(
            "start",
            1,
            "tool_started",
            payload={"toolCallId": "call-gap"},
        )
    )
    index.ingest(
        _event(
            "terminal",
            3,
            "tool_completed",
            payload={"toolCallId": "call-gap"},
            status="completed",
            phase="terminal",
        )
    )
    replay = index.replay("turn-1")
    assert [(gap.from_seq, gap.to_seq) for gap in replay.gaps] == [(2, 2)]
    assert replay.items[0].has_gap is True
    index.ingest(
        _event(
            "progress",
            2,
            "tool_progress",
            payload={"toolCallId": "call-gap"},
        )
    )
    assert index.replay("turn-1").gaps == ()


def test_explicit_page_gap_and_bounded_history_are_visible_without_raw_payload() -> None:
    index = TurnItemIndex(max_history_per_item=2, max_replay_items=1)
    events = [
        _event(
            f"event-{seq}",
            seq,
            "tool_progress" if seq < 4 else "tool_completed",
            payload={"toolCallId": "call-bounded", "secret": "do-not-project"},
            status="completed" if seq == 4 else "running",
            phase="terminal" if seq == 4 else "stream",
        )
        for seq in range(1, 5)
    ]
    index.ingest_page(
        {
            "turn_id": "turn-1",
            "events": events,
            "gaps": [
                {
                    "from_seq": 8,
                    "to_seq": 9,
                    "reason": "pruned_or_missing",
                }
            ],
        }
    )
    item = index.replay("turn-1").items[0]
    assert item.history_total == 4
    assert len(item.history) == 2
    assert item.history_truncated is True
    assert all("secret" not in entry.to_dict() for entry in item.history)
    assert index.replay("turn-1").truncated is True
    assert [(gap.from_seq, gap.to_seq) for gap in index.replay("turn-1").gaps] == [
        (8, 9)
    ]


def test_late_event_is_retained_as_history_but_cannot_revive_terminal_item() -> None:
    index = TurnItemIndex()
    index.ingest(
        _event(
            "start",
            1,
            "tool_started",
            payload={"toolCallId": "call-late"},
        )
    )
    index.ingest(
        _event(
            "winner",
            2,
            "tool_completed",
            payload={"toolCallId": "call-late", "evidence_class": LIVE},
            status="completed",
            phase="terminal",
        )
    )
    late = index.ingest(
        _event(
            "late-progress",
            3,
            "tool_progress",
            payload={"toolCallId": "call-late"},
            phase="stream",
        )
    )
    late_terminal = index.ingest(
        _event(
            "late-failure",
            4,
            "tool_completed",
            payload={"toolCallId": "call-late"},
            status="failed",
            phase="terminal",
        )
    )
    item = index.replay("turn-1").items[0]
    assert late.accepted is True and late.late is True
    assert late_terminal.accepted is True and late_terminal.late is True
    assert item.state == "completed"
    assert item.terminal_event_id == "winner"
    assert item.phase == "terminal"
    assert item.late_event_count == 2
    assert [entry.late for entry in item.history[-2:]] == [True, True]
    assert "late_event_after_terminal" in {conflict.code for conflict in item.conflicts}


def test_evidence_is_conservative_and_harness_event_objects_are_supported() -> None:
    index = TurnItemIndex()
    event = HarnessEvent(
        event_id="object-event",
        turn_id="turn-object",
        world_id="world-1",
        engram_id="engram-1",
        seq=1,
        parent_event_id=None,
        kind=HarnessEventKind.TOOL_STARTED,
        phase=HarnessEventPhase.STREAM,
        source=HarnessEventSource.PI_RPC,
        status=HarnessEventStatus.RUNNING,
        occurred_at=datetime.now(timezone.utc),
        payload_json={"toolCallId": "object-call"},
        payload_bytes=32,
        payload_digest="digest",
        redacted=False,
        truncated=False,
    )
    index.ingest(event)
    item = index.replay("turn-object").items[0]
    assert item.evidence_class == LIVE_GATE_UNVERIFIED
    assert item.evidence_level == LIVE_GATE_UNVERIFIED
    assert item.history[0].evidence_class is None


def test_project_function_exposes_future_workbench_wire_shape() -> None:
    replay = project_turn_items(
        [
            _event(
                "a",
                1,
                "tool_started",
                payload={"toolCallId": "call-wire"},
            )
        ],
        turn_id="turn-1",
    )
    wire = replay.to_wire()
    assert wire["protocol_version"] == "harness.turn-items.v1"
    assert wire["bounded"] is True
    assert wire["items"][0]["item_id"] == replay.items[0].item_id
    assert "payload_json" not in wire["items"][0]


def test_unknown_turn_is_bounded_and_capacity_rejects_without_silent_drop() -> None:
    index = TurnItemIndex(max_items_per_turn=1)
    unknown = index.replay("never-seen")
    assert unknown.turn_known is False
    index.ingest(
        _event(
            "one",
            1,
            "tool_started",
            payload={"toolCallId": "one"},
        )
    )
    rejected = index.ingest(
        _event(
            "two",
            2,
            "tool_started",
            payload={"toolCallId": "two"},
        )
    )
    assert rejected.accepted is False
    assert rejected.conflicts[0].code == "item_capacity_exhausted"
    assert len(index.replay("turn-1").items) == 1

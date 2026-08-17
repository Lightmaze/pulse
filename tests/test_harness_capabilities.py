from __future__ import annotations

import pytest

from pulse_system.agent.harness.capabilities import (
    CapabilityContext,
    CapabilityError,
    CapabilitySet,
)


def test_intersection_never_widens_filesystem_network_or_scope() -> None:
    declared = CapabilitySet(
        tool_names={"read", "write"},
        filesystem_roots={"workspace/src"},
        network_domains={"api.example.com"},
        process_classes={"shell"},
        secrets_access={"read"},
        world_ids={"world-a"},
        engram_ids={"engram-a"},
        turn_ids={"turn-a"},
        mcp_transports={"stdio"},
    )
    policy = CapabilitySet(
        tool_names={"read"},
        filesystem_roots={"workspace"},
        network_domains={"example.com"},
        process_classes={"shell", "python"},
        secrets_access={"read", "write"},
        world_ids={"world-a", "world-b"},
        engram_ids={"engram-a"},
        turn_ids={"turn-a", "turn-b"},
        mcp_transports={"stdio", "sse"},
    )

    effective = declared.intersect(policy)

    assert effective.to_dict() == {
        "tool_names": ["read"],
        "filesystem_roots": ["workspace/src"],
        "network_domains": ["api.example.com"],
        "process_classes": ["shell"],
        "secrets_access": ["read"],
        "world_ids": ["world-a"],
        "engram_ids": ["engram-a"],
        "turn_ids": ["turn-a"],
        "mcp_transports": ["stdio"],
    }
    assert effective.contains(
        CapabilitySet(
            tool_names={"read"},
            filesystem_roots={"workspace/src"},
            network_domains={"api.example.com"},
        )
    )
    assert not effective.contains(CapabilitySet(tool_names={"write"}))
    assert not effective.allows_scope(
        world_id="world-b", engram_id="engram-a", turn_id="turn-a"
    )


def test_nested_domain_and_root_intersection_uses_narrower_scope() -> None:
    left = CapabilitySet(
        filesystem_roots={"."},
        network_domains={"*.example.com"},
    )
    right = CapabilitySet(
        filesystem_roots={"workspace/docs"},
        network_domains={"api.example.com"},
    )

    effective = left.intersection(right)

    assert effective.filesystem_roots == frozenset({"workspace/docs"})
    assert effective.network_domains == frozenset({"api.example.com"})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("filesystem_roots", {"C:\\secrets"}),
        ("filesystem_roots", {"../outside"}),
        ("network_domains", {"https://example.com"}),
        ("network_domains", {"example.com/private"}),
        ("secrets_access", {"read", "raw_token_value"}),
    ],
)
def test_capability_values_fail_closed(field: str, value: set[str]) -> None:
    with pytest.raises(CapabilityError):
        CapabilitySet(**{field: value})


def test_context_parsing_is_identity_bound_and_does_not_carry_payload() -> None:
    context = CapabilityContext.from_mapping(
        {
            "world_id": "world-a",
            "engram_id": "engram-a",
            "turn_id": "turn-a",
            "policy": {
                "tool_names": ["read"],
                "world_ids": ["world-a"],
                "engram_ids": ["engram-a"],
                "turn_ids": ["turn-a"],
            },
            "approval_granted": True,
            "raw_tool_payload": {"password": "must-not-be-copied"},
        }
    )

    assert context.approval_granted is True
    assert context.policy.tool_names == frozenset({"read"})
    assert not hasattr(context, "raw_tool_payload")


def test_empty_capability_set_is_deny_all() -> None:
    empty = CapabilitySet.empty()
    assert empty.is_empty()
    assert not empty.contains(CapabilitySet(tool_names={"read"}))
    assert not empty.allows_scope(world_id="world-a", engram_id="engram-a")

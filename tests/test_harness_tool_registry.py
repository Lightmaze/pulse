from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pulse_system.agent.harness.capabilities import CapabilityContext, CapabilitySet
from pulse_system.agent.harness.tool_registry import (
    DescriptorKind,
    ExtensionDescriptor,
    HookDescriptor,
    MCPDescriptor,
    PluginDescriptor,
    RegistryStatus,
    SignatureState,
    SkillDescriptor,
    ToolRegistry,
)


def _verified(_raw: bytes, metadata: dict[str, str]) -> bool:
    """Local contract verifier; no package or network operation is involved."""

    return metadata.get("state") == "verified"


def _manifest(
    *,
    descriptor_id: str = "demo.tool",
    kind: str = "plugin",
    version: str = "1.2.3",
    transport: str | None = None,
    signature_state: str = "verified",
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "id": descriptor_id,
        "name": "Demo descriptor",
        "kind": kind,
        "version": version,
        "description": "safe descriptor metadata",
        "entrypoint": "never-returned-to-caller",
        "prompt": "never injected into an Engram prompt",
        "tool_payload": {"password": "never stored"},
        "signature": {"state": signature_state, "key_id": "test-key"},
        "declared_capabilities": {
            "tool_names": ["read", "write"],
            "filesystem_roots": ["workspace/src"],
            "network_domains": ["api.example.com"],
            "process_classes": ["shell"],
            "secrets_access": ["metadata"],
            "world_ids": ["world-a"],
            "engram_ids": ["engram-a"],
            "turn_ids": ["turn-a"],
            "mcp_transports": ["stdio"],
        },
        "lifecycle": "session",
        "execution_location": "local_process",
        "approval_required": True,
    }
    if transport is not None:
        value["transport"] = transport
    return value


def _context(*, approval: bool = True, tool_names: set[str] | None = None) -> CapabilityContext:
    return CapabilityContext(
        world_id="world-a",
        engram_id="engram-a",
        turn_id="turn-a",
        policy=CapabilitySet(
            tool_names=tool_names or {"read"},
            filesystem_roots={"workspace"},
            network_domains={"example.com"},
            process_classes={"shell"},
            secrets_access={"metadata"},
            world_ids={"world-a"},
            engram_ids={"engram-a"},
            turn_ids={"turn-a"},
            mcp_transports={"stdio"},
        ),
        approval_granted=approval,
    )


def test_inline_discovery_records_provenance_and_safe_descriptor() -> None:
    registry = ToolRegistry(signature_verifier=_verified)

    descriptors = registry.discover(_manifest())

    assert len(descriptors) == 1
    descriptor = descriptors[0]
    assert isinstance(descriptor, PluginDescriptor)
    assert descriptor.provenance.signature_state is SignatureState.VERIFIED
    assert len(descriptor.provenance.digest) == 64
    assert descriptor.status is RegistryStatus.DISABLED
    safe = descriptor.to_dict()
    rendered = json.dumps(safe, ensure_ascii=False, sort_keys=True)
    assert "never-returned-to-caller" not in rendered
    assert "never injected" not in rendered
    assert "never stored" not in rendered
    assert "password" not in rendered
    assert safe["provenance"]["relative_path"] is None
    assert safe["provenance"]["signature_state"] == "verified"


def test_configure_and_resolve_are_intersection_only_and_contract_labeled() -> None:
    registry = ToolRegistry(signature_verifier=_verified)
    registry.discover(_manifest())

    finding = registry.configure(
        "demo.tool",
        True,
        version_pin="1.2.3",
        scope={
            "tool_names": ["read"],
            "filesystem_roots": ["workspace/src"],
            "network_domains": ["api.example.com"],
            "world_ids": ["world-a"],
            "engram_ids": ["engram-a"],
            "turn_ids": ["turn-a"],
        },
    )

    assert finding.status is RegistryStatus.ENABLED
    decision = registry.resolve("demo.tool", _context())
    assert decision.allowed is True
    assert decision.reason == "capability_allowed_contract_only"
    assert decision.evidence_class.value == "CONTRACT_ONLY"
    assert decision.live_gate.value == "LIVE_GATE_UNVERIFIED"
    assert decision.effective_capabilities.tool_names == frozenset({"read"})
    assert decision.effective_capabilities.filesystem_roots == frozenset({"workspace/src"})
    assert decision.requires_approval is True

    denied = registry.resolve("demo.tool", _context(tool_names={"write"}))
    assert denied.allowed is False
    assert denied.reason == "capability_intersection_empty"


def test_approval_is_not_bypassed_by_registry() -> None:
    registry = ToolRegistry(signature_verifier=_verified)
    registry.discover(_manifest())
    registry.configure("demo.tool", True, version_pin="1.2.3")

    decision = registry.resolve("demo.tool", _context(approval=False))

    assert decision.allowed is False
    assert decision.reason == "approval_required"
    assert decision.requires_approval is True


def test_unverified_manifest_is_quarantined_and_cannot_be_enabled() -> None:
    registry = ToolRegistry()
    registry.discover(_manifest(signature_state="verified"))

    descriptor = registry.descriptors()[0]
    assert descriptor.provenance.signature_state is SignatureState.UNVERIFIED
    assert descriptor.status is RegistryStatus.QUARANTINED
    finding = registry.configure(descriptor.id, True, version_pin=descriptor.version)
    assert finding.status is RegistryStatus.QUARANTINED
    assert finding.reason == "signature_unverified"
    decision = registry.resolve(descriptor.id, _context())
    assert decision.allowed is False
    assert decision.reason == "descriptor_quarantined"


def test_mcp_transport_is_explicitly_unsupported_without_adapter() -> None:
    registry = ToolRegistry(signature_verifier=_verified)
    registry.discover(_manifest(kind="mcp", descriptor_id="mcp.demo", transport="sse"))

    descriptor = registry.descriptors()[0]
    assert isinstance(descriptor, MCPDescriptor)
    assert descriptor.status is RegistryStatus.UNSUPPORTED
    assert descriptor.reason == "mcp_transport_unsupported"
    finding = registry.configure(descriptor.id, True, version_pin=descriptor.version)
    assert finding.status is RegistryStatus.UNSUPPORTED
    decision = registry.resolve(descriptor.id, _context())
    assert decision.allowed is False
    assert decision.reason == "mcp_transport_unsupported"
    assert decision.transport_status == "unsupported"
    assert decision.live_gate.value == "LIVE_GATE_UNVERIFIED"


def test_mcp_transport_support_is_still_not_a_connection_or_execution() -> None:
    registry = ToolRegistry(
        signature_verifier=_verified,
        supported_mcp_transports={"stdio"},
    )
    registry.discover(_manifest(kind="mcp", descriptor_id="mcp.demo", transport="stdio"))
    finding = registry.configure("mcp.demo", True, version_pin="1.2.3")
    assert finding.status is RegistryStatus.ENABLED
    decision = registry.resolve("mcp.demo", _context())
    assert decision.allowed is True
    assert decision.transport_status == "contract_only"
    assert decision.live_gate.value == "LIVE_GATE_UNVERIFIED"


def test_version_pin_is_required_for_multiple_versions_and_qualified_ids_work() -> None:
    registry = ToolRegistry(signature_verifier=_verified)
    registry.discover(_manifest(version="1.0.0"))
    registry.discover(_manifest(version="2.0.0"))

    rejected = registry.configure("demo.tool", True)
    assert rejected.status is RegistryStatus.INCOMPATIBLE
    assert rejected.reason == "version_pin_required"
    accepted = registry.configure("demo.tool@2.0.0", True)
    assert accepted.status is RegistryStatus.ENABLED
    assert accepted.descriptor is not None
    assert accepted.descriptor.version == "2.0.0"


def test_local_discovery_is_allowlisted_and_bounded(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    outside = tmp_path / "outside"
    allowed.mkdir()
    outside.mkdir()
    manifest = _manifest(descriptor_id="local.demo", kind="extension")
    (allowed / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    (outside / "manifest.json").write_text(
        json.dumps(_manifest(descriptor_id="outside.demo")), encoding="utf-8"
    )

    registry = ToolRegistry(
        workspace_root=tmp_path,
        allowed_source_roots=[allowed],
        signature_verifier=_verified,
    )
    discovered = registry.discover()
    assert [item.id for item in discovered] == ["local.demo"]
    assert isinstance(discovered[0], ExtensionDescriptor)
    assert discovered[0].provenance.relative_path == "allowed/manifest.json"
    assert discovered[0].provenance.source_kind == "local"

    assert registry.discover(outside) == []
    assert any(item.reason == "source_path_not_allowlisted" for item in registry.findings())


def test_all_ecosystem_kinds_have_lifecycle_and_safe_provenance() -> None:
    registry = ToolRegistry(signature_verifier=_verified)
    manifests = [
        _manifest(descriptor_id="skill.demo", kind="skill"),
        _manifest(descriptor_id="extension.demo", kind="extension"),
        _manifest(descriptor_id="hook.demo", kind="hook"),
        _manifest(descriptor_id="tool.demo", kind="tool"),
    ]
    descriptors = registry.discover({"manifests": manifests})

    by_id = {item.id: item for item in descriptors}
    assert isinstance(by_id["skill.demo"], SkillDescriptor)
    assert isinstance(by_id["extension.demo"], ExtensionDescriptor)
    assert isinstance(by_id["hook.demo"], HookDescriptor)
    assert by_id["skill.demo"].lifecycle == "session"
    assert by_id["hook.demo"].lifecycle == "session"
    assert all(item.kind in set(DescriptorKind) for item in descriptors)


def test_quarantine_reason_is_bounded_and_not_a_payload_channel() -> None:
    registry = ToolRegistry(signature_verifier=_verified)
    registry.discover(_manifest(descriptor_id="hook.demo", kind="hook"))
    secret_reason = "manual review password=super-secret-value"

    finding = registry.quarantine("hook.demo", secret_reason)
    rendered = json.dumps(finding.to_dict(), ensure_ascii=False)
    assert finding.status is RegistryStatus.QUARANTINED
    assert "super-secret-value" not in rendered
    assert "password" not in rendered


def test_inline_manifest_and_declared_digest_are_bounded_and_fail_closed() -> None:
    registry = ToolRegistry(signature_verifier=_verified, max_manifest_bytes=1024)
    oversized = _manifest(descriptor_id="oversized.demo")
    oversized["description"] = "x" * 2048
    assert registry.discover(oversized) == []
    assert any(item.reason == "manifest_too_large" for item in registry.findings())

    digest_registry = ToolRegistry(signature_verifier=_verified)
    bad_digest = _manifest(descriptor_id="digest.demo")
    bad_digest["digest"] = "0" * 64
    descriptors = digest_registry.discover(bad_digest)
    assert len(descriptors) == 1
    assert descriptors[0].status is RegistryStatus.QUARANTINED
    assert descriptors[0].reason == "digest_mismatch"

from __future__ import annotations

from dataclasses import dataclass

from pulse_system.agent.harness.checkpoints import (
    BackendCheckpointResult,
    BackendDropResult,
    BackendRestoreResult,
    CheckpointScope,
    CheckpointState,
    CheckpointStore,
    RestoreState,
)
from pulse_system.agent.harness.terminal import (
    CONTRACT_ONLY,
)
from pulse_system.agent.harness.security import PolicyDecision


def allow_policy(_request):
    return PolicyDecision(
        allow=True,
        requires_approval=False,
        policy_id="checkpoint-test-policy",
        reason_code="approved_contract_fixture",
        evidence_class=CONTRACT_ONLY,
    )


@dataclass
class ContractCheckpointBackend:
    supports_checkpoints: bool = True
    evidence_class: str = CONTRACT_ONLY
    raise_on_create: bool = False

    def __post_init__(self):
        self.create_calls = 0
        self.restore_calls = 0
        self.drop_calls = 0

    def create(self, scope, *, checkpoint_id):
        self.create_calls += 1
        if self.raise_on_create:
            raise RuntimeError("backend unavailable")
        return BackendCheckpointResult(
            state=CheckpointState.CREATED,
            backend_ref=f"fixture:{checkpoint_id}",
        )

    def restore(self, reference, scope, *, changed_paths):
        self.restore_calls += 1
        return BackendRestoreResult(
            state=RestoreState.RESTORED,
            applied_paths=changed_paths,
        )

    def drop(self, reference, scope):
        self.drop_calls += 1
        return BackendDropResult(state=CheckpointState.DROPPED)


def make_scope(tmp_path, *, epoch=7, changed_paths=("src/file.txt",)):
    return CheckpointScope(
        turn_id="turn-checkpoint",
        world_id="world-1",
        engram_id="engram-1",
        epoch=epoch,
        workspace_root=tmp_path,
        changed_paths=changed_paths,
        label="before-change",
    )


def test_default_checkpoint_backend_is_explicitly_unsupported(tmp_path):
    store = CheckpointStore()
    reference = store.create(make_scope(tmp_path), policy_context=allow_policy)

    assert reference.state is CheckpointState.UNSUPPORTED
    assert reference.evidence_class == "CONTRACT_ONLY"
    assert reference.backend_ref is None
    assert reference.to_wire().get("workspace_root") is None

    restored = store.restore(
        reference.id,
        workspace_root=tmp_path,
        expected_epoch=7,
        policy_context=allow_policy,
    )
    assert restored.state is RestoreState.UNSUPPORTED
    assert restored.error_code == "unsupported_execution"


def test_policy_denial_does_not_call_checkpoint_backend(tmp_path):
    backend = ContractCheckpointBackend()
    store = CheckpointStore(backend=backend)
    reference = store.create(
        make_scope(tmp_path),
        policy_context=PolicyDecision(
            allow=False,
            requires_approval=True,
            policy_id="deny",
            reason_code="approval_required",
        ),
    )

    assert reference.state is CheckpointState.DECLINED
    assert reference.reason == "approval_required"
    assert backend.create_calls == 0


def test_create_restore_and_drop_check_workspace_epoch_and_paths(tmp_path):
    backend = ContractCheckpointBackend()
    store = CheckpointStore(backend=backend)
    reference = store.create(make_scope(tmp_path), policy_context=allow_policy)

    assert reference.state is CheckpointState.CREATED
    assert reference.evidence_class == CONTRACT_ONLY
    assert reference.changed_paths == ("src/file.txt",)
    assert "workspace_root" not in reference.to_wire()

    stale = store.restore(
        reference.id,
        workspace_root=tmp_path,
        expected_epoch=8,
        policy_context=allow_policy,
    )
    assert stale.state is RestoreState.STALE
    assert backend.restore_calls == 0

    outside = store.restore(
        reference.id,
        workspace_root=tmp_path,
        expected_epoch=7,
        changed_paths=("src/other.txt",),
        policy_context=allow_policy,
    )
    assert outside.state is RestoreState.DECLINED
    assert outside.error_code == "changed_path_outside_scope"
    assert backend.restore_calls == 0

    restored = store.restore(
        reference.id,
        workspace_root=tmp_path,
        expected_epoch=7,
        changed_paths=("src/file.txt",),
        policy_context=allow_policy,
    )
    assert restored.state is RestoreState.RESTORED
    assert restored.applied_paths == ("src/file.txt",)
    assert backend.restore_calls == 1

    dropped = store.drop(
        reference.id,
        workspace_root=tmp_path,
        expected_epoch=7,
        policy_context=allow_policy,
    )
    assert dropped.state is CheckpointState.DROPPED
    assert backend.drop_calls == 1


def test_workspace_mismatch_and_unknown_reference_are_safe(tmp_path):
    other = tmp_path / "other"
    other.mkdir()
    backend = ContractCheckpointBackend()
    store = CheckpointStore(backend=backend)
    reference = store.create(make_scope(tmp_path), policy_context=allow_policy)

    mismatch = store.restore(
        reference.id,
        workspace_root=other,
        expected_epoch=7,
        policy_context=allow_policy,
    )
    missing = store.restore(
        "missing-checkpoint",
        workspace_root=tmp_path,
        expected_epoch=7,
        policy_context=allow_policy,
    )

    assert mismatch.state is RestoreState.DECLINED
    assert mismatch.error_code == "workspace_mismatch"
    assert missing.state is RestoreState.NOT_FOUND
    assert backend.restore_calls == 0


def test_backend_creation_failure_is_uncertain_not_created(tmp_path):
    backend = ContractCheckpointBackend(raise_on_create=True)
    store = CheckpointStore(backend=backend)

    reference = store.create(make_scope(tmp_path), policy_context=allow_policy)

    assert reference.state is CheckpointState.UNCERTAIN
    assert reference.reason
    assert reference.backend_ref is None


def test_reference_retention_is_bounded(tmp_path):
    backend = ContractCheckpointBackend()
    store = CheckpointStore(backend=backend, max_retained=2)

    first = store.create(make_scope(tmp_path, changed_paths=("a.txt",)), policy_context=allow_policy)
    second = store.create(make_scope(tmp_path, changed_paths=("b.txt",)), policy_context=allow_policy)
    third = store.create(make_scope(tmp_path, changed_paths=("c.txt",)), policy_context=allow_policy)

    retained = {reference.id for reference in store.list()}
    assert len(retained) == 2
    assert third.id in retained
    assert first.id not in retained
    assert second.id in retained

from __future__ import annotations

from pathlib import Path

from pulse_system.agent.harness.changes import (
    ChangeApplyResult,
    ChangeApplyState,
    ChangeKind,
    ChangeSet,
    ChangeStatus,
    normalize_relative_path,
)
from pulse_system.agent.harness.terminal import (
    CONTRACT_ONLY,
)
from pulse_system.agent.harness.security import PolicyDecision


def allow_policy(_request):
    return PolicyDecision(
        allow=True,
        requires_approval=False,
        policy_id="change-test-policy",
        reason_code="approved_contract_fixture",
        evidence_class=CONTRACT_ONLY,
    )


class ContractApplier:
    supports_execution = True
    evidence_class = CONTRACT_ONLY

    def __init__(self):
        self.calls = 0

    def apply(self, change_set):
        self.calls += 1
        return ChangeApplyResult(
            change_set_id=change_set.id,
            state=ChangeApplyState.APPLIED,
            applied_paths=tuple(
                change.path
                for change in change_set.changes
                if change.kind is not ChangeKind.DELETE
            ),
            evidence_class=CONTRACT_ONLY,
        )


def test_from_patch_parses_structured_add_update_delete_and_move(tmp_path: Path):
    add = ChangeSet.from_patch(
        "--- /dev/null\n+++ b/src/new.txt\n@@ -0,0 +1 @@\n+new\n",
        turn_id="turn-add",
        redactor=lambda value: value,
    )
    update = ChangeSet.from_patch(
        "--- a/src/file.txt\n+++ b/src/file.txt\n@@ -1 +1 @@\n-old\n+new\n",
        turn_id="turn-update",
        redactor=lambda value: value,
    )
    delete = ChangeSet.from_patch(
        "--- a/src/old.txt\n+++ /dev/null\n@@ -1 +0,0 @@\n-old\n",
        turn_id="turn-delete",
        redactor=lambda value: value,
    )
    move = ChangeSet.from_patch(
        "diff --git a/src/old.txt b/src/new.txt\n"
        "similarity index 100%\n"
        "rename from src/old.txt\n"
        "rename to src/new.txt\n",
        turn_id="turn-move",
        redactor=lambda value: value,
    )

    assert add.status is ChangeStatus.PROPOSED
    assert add.applied is False
    assert add.changes[0].kind is ChangeKind.ADD
    assert add.changes[0].path == "src/new.txt"
    assert update.changes[0].kind is ChangeKind.UPDATE
    assert delete.changes[0].kind is ChangeKind.DELETE
    assert move.changes[0].kind is ChangeKind.MOVE
    assert move.changes[0].path == "src/old.txt"
    assert move.changes[0].move_path == "src/new.txt"
    assert add.to_wire()["diff_preview"].startswith("---")
    assert not (tmp_path / "src" / "new.txt").exists()


def test_absolute_traversal_and_oversized_patches_are_declined_without_preview():
    traversal = ChangeSet.from_patch(
        "--- a/../secret.txt\n+++ b/../secret.txt\n",
        turn_id="turn-invalid",
        redactor=lambda value: value,
    )
    absolute = ChangeSet.from_patch(
        "--- C:/secret.txt\n+++ C:/secret.txt\n",
        turn_id="turn-absolute",
        redactor=lambda value: value,
    )
    oversized = ChangeSet.from_patch(
        "--- a/src/file.txt\n+++ b/src/file.txt\n",
        turn_id="turn-large",
        max_patch_bytes=8,
        redactor=lambda value: value,
    )

    for change_set in (traversal, absolute, oversized):
        assert change_set.status is ChangeStatus.DECLINED
        assert change_set.changes == ()
        assert change_set.applied is False
        assert change_set.reject_reason
        assert change_set.diff_preview == "[REDACTION_REQUIRED]"


def test_default_apply_is_unsupported_and_does_not_turn_diff_into_success(tmp_path):
    change_set = ChangeSet.from_patch(
        "--- /dev/null\n+++ b/new.txt\n@@ -0,0 +1 @@\n+secret\n",
        turn_id="turn-contract",
        redactor=lambda value: value,
    )

    result = change_set.apply(policy_context=allow_policy)

    assert result.state is ChangeApplyState.UNSUPPORTED
    assert result.error_code == "unsupported_execution"
    assert result.evidence_class == "CONTRACT_ONLY"
    assert change_set.status is ChangeStatus.PROPOSED
    assert change_set.applied is False
    assert not (tmp_path / "new.txt").exists()


def test_explicit_contract_applier_returns_separate_application_evidence():
    change_set = ChangeSet.from_patch(
        "--- /dev/null\n+++ b/new.txt\n@@ -0,0 +1 @@\n+new\n",
        turn_id="turn-applied-contract",
        redactor=lambda value: value,
    )
    applier = ContractApplier()

    result = change_set.apply(policy_context=allow_policy, applier=applier)

    assert result.state is ChangeApplyState.APPLIED
    assert result.evidence_class == CONTRACT_ONLY
    assert result.applied_paths == ("new.txt",)
    assert applier.calls == 1
    assert change_set.status is ChangeStatus.PROPOSED
    assert change_set.applied is False


def test_redaction_failure_never_returns_the_untrusted_patch_preview():
    secret = "token=super-secret-value"
    change_set = ChangeSet.from_patch(
        f"--- a/src/file.txt\n+++ b/src/file.txt\n@@ -1 +1 @@\n-{secret}\n",
        turn_id="turn-redaction",
        redactor=lambda _value: (_ for _ in ()).throw(RuntimeError("redactor down")),
    )

    assert change_set.diff_preview == "[REDACTION_FAILED]"
    assert change_set.redacted is True
    assert secret not in change_set.to_wire()["diff_preview"]


def test_path_normalization_is_strict_and_workspace_relative():
    assert normalize_relative_path(r"src\folder\file.txt") == "src/folder/file.txt"

    for unsafe in ("/tmp/file", r"C:\file", "../file", "src/../file", "src//file"):
        try:
            normalize_relative_path(unsafe)
        except ValueError:
            pass
        else:
            raise AssertionError(f"unsafe path accepted: {unsafe}")

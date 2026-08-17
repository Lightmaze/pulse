from __future__ import annotations

import inspect
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from pulse_system.agent.harness import process_containment as containment_module
from pulse_system.agent.harness.process_containment import (
    PHYSICAL_PROCESS_OWNER_PROTOCOL,
    ContainedProcessOwner,
    PhysicalProcessObservation,
    ProcessContainmentKind,
    ProcessTreeEvidence,
    spawn_contained_process,
)


def _wait_for_file(path: Path, *, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not path.is_file():
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out waiting for {path.name}")
        time.sleep(0.02)


def test_physical_observation_is_exact_and_consistent() -> None:
    with pytest.raises(TypeError, match="retained owner"):
        PhysicalProcessObservation(
            protocol_version=PHYSICAL_PROCESS_OWNER_PROTOCOL,
            owner_token="0" * 32,
            containment=ProcessContainmentKind.WINDOWS_JOB,
            root_observed=True,
            root_exited=True,
            tree_state=ProcessTreeEvidence.EMPTY_VERIFIED,
            observation_generation=1,
            witness_released=True,
        )
    assert "pid" not in PhysicalProcessObservation.__dataclass_fields__
    assert "argv" not in PhysicalProcessObservation.__dataclass_fields__
    with pytest.raises(TypeError, match="final canonical type"):

        class _DerivedObservation(PhysicalProcessObservation):
            pass

    with pytest.raises(TypeError, match="final canonical type"):

        class _DerivedOwner(ContainedProcessOwner):
            pass


def test_windows_breakaway_is_rejected_before_spawn() -> None:
    if os.name != "nt":
        pytest.skip("requires Windows creation flags")
    with pytest.raises(ValueError, match="BREAKAWAY"):
        spawn_contained_process(
            [sys.executable, "-c", "raise SystemExit(0)"],
            creationflags=0x01000000,
        )


@pytest.mark.skipif(os.name != "nt", reason="requires Windows Job Objects")
def test_shared_launcher_observes_natural_job_empty(tmp_path: Path) -> None:
    owner = spawn_contained_process(
        [sys.executable, "-c", "raise SystemExit(7)"],
        cwd=tmp_path,
        env=os.environ,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    process = owner.process
    process.wait(timeout=10)

    first = owner.observe()
    second = owner.observe()

    assert process.returncode == 7
    assert owner.containment is ProcessContainmentKind.WINDOWS_JOB
    assert first.owner_token == second.owner_token == owner.owner_token
    assert first.tree_state is ProcessTreeEvidence.EMPTY_VERIFIED
    assert second.tree_state is ProcessTreeEvidence.EMPTY_VERIFIED
    assert first.witness_released is True
    assert first.resource_converged is True
    assert second.observation_generation == first.observation_generation + 1
    assert process.containment_assigned_before_resume is True
    assert process.close_containment() is True


@pytest.mark.skipif(os.name != "nt", reason="requires Windows Job Objects")
def test_shared_owner_terminates_root_and_descendant_tree(tmp_path: Path) -> None:
    tree_path = tmp_path / "physical-owner-tree.json"
    child_code = "import time; time.sleep(60)"
    root_code = (
        "import json, os, pathlib, subprocess, sys, time; "
        f"child=subprocess.Popen([sys.executable, '-c', {child_code!r}]); "
        f"pathlib.Path({str(tree_path)!r}).write_text(json.dumps({{'root':os.getpid(),'child':child.pid}}), encoding='utf-8'); "
        "time.sleep(60)"
    )
    owner = spawn_contained_process(
        [sys.executable, "-c", root_code],
        cwd=tmp_path,
        env=os.environ,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_file(tree_path)
        tree = json.loads(tree_path.read_text(encoding="utf-8"))
        members = set(owner.process.contained_process_ids())
        assert int(tree["root"]) in members
        assert int(tree["child"]) in members

        observed = owner.terminate_tree(time.monotonic() + 10.0)

        assert observed.root_exited is True
        assert observed.tree_state is ProcessTreeEvidence.EMPTY_VERIFIED
        assert observed.witness_released is True
        assert observed.resource_converged is True
        assert observed.error_code is None
        assert owner.process.poll() is not None
        assert owner.process.contained_process_ids() == ()
    finally:
        owner.terminate_tree(time.monotonic() + 2.0)


def test_job_api_implementation_is_unique() -> None:
    source_root = Path(containment_module.__file__).resolve().parent
    matches: list[Path] = []
    for path in source_root.glob("*.py"):
        if "AssignProcessToJobObject" in path.read_text(encoding="utf-8"):
            matches.append(path)
    assert matches == [Path(containment_module.__file__).resolve()]


def test_observation_proof_cannot_regress(monkeypatch: pytest.MonkeyPatch) -> None:
    if os.name != "nt":
        pytest.skip("requires canonical Windows contained process type")
    process = containment_module._WindowsContainedPopen.__new__(
        containment_module._WindowsContainedPopen
    )
    process.returncode = 0
    process._pulse_job_lock = threading.RLock()
    process._pulse_job_handle = 1
    states = iter((False, None))
    monkeypatch.setattr(process, "poll", lambda: 0)
    monkeypatch.setattr(process, "containment_state", lambda: next(states))
    monkeypatch.setattr(process, "close_containment", lambda: False)
    owner = ContainedProcessOwner(process, ProcessContainmentKind.WINDOWS_JOB)

    first = owner.observe()
    second = owner.observe()

    assert first.tree_state is ProcessTreeEvidence.ROOT_EXIT_ONLY
    assert second.tree_state is ProcessTreeEvidence.ROOT_EXIT_ONLY
    assert second.root_exited is True


@pytest.mark.skipif(os.name != "nt", reason="requires canonical Windows Popen type")
def test_empty_tree_proof_survives_transient_witness_close_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = containment_module._WindowsContainedPopen.__new__(
        containment_module._WindowsContainedPopen
    )
    process.returncode = 0
    close_results = iter((False, True))
    monkeypatch.setattr(process, "poll", lambda: 0)
    monkeypatch.setattr(process, "containment_state", lambda: True)
    monkeypatch.setattr(process, "close_containment", lambda: next(close_results))
    owner = ContainedProcessOwner(process, ProcessContainmentKind.WINDOWS_JOB)

    first = owner.observe()
    second = owner.observe()

    assert first.tree_state is ProcessTreeEvidence.EMPTY_VERIFIED
    assert first.physical_exit_proven is True
    assert first.witness_released is False
    assert first.resource_converged is False
    assert first.error_code == "containment_handle_close_unproven"
    assert second.tree_state is ProcessTreeEvidence.EMPTY_VERIFIED
    assert second.witness_released is True
    assert second.resource_converged is True
    assert second.error_code is None


@pytest.mark.skipif(os.name != "nt", reason="requires Windows Job Objects")
def test_owner_terminates_descendant_after_root_already_exited(tmp_path: Path) -> None:
    child_path = tmp_path / "root-exited-child.json"
    child_code = "import time; time.sleep(60)"
    root_code = (
        "import json, pathlib, subprocess, sys; "
        f"child=subprocess.Popen([sys.executable, '-c', {child_code!r}]); "
        f"pathlib.Path({str(child_path)!r}).write_text(json.dumps({{'child':child.pid}}), encoding='utf-8')"
    )
    owner = spawn_contained_process(
        [sys.executable, "-c", root_code],
        cwd=tmp_path,
        env=os.environ,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_file(child_path)
        owner.process.wait(timeout=10)
        child_pid = int(json.loads(child_path.read_text(encoding="utf-8"))["child"])
        assert child_pid in set(owner.process.contained_process_ids())

        before = owner.observe()
        after = owner.terminate_tree(time.monotonic() + 10.0)

        assert before.tree_state is ProcessTreeEvidence.ROOT_EXIT_ONLY
        assert before.witness_released is False
        assert after.tree_state is ProcessTreeEvidence.EMPTY_VERIFIED
        assert after.witness_released is True
        assert after.resource_converged is True
    finally:
        owner.terminate_tree(time.monotonic() + 2.0)


def test_emergency_rollback_registry_retains_until_exact_handles_close() -> None:
    class _RollbackApi:
        def __init__(self) -> None:
            self.waits = 0
            self.close_attempts: list[int] = []
            self.closed: set[int] = set()
            self.job_close_attempts = 0

        def terminate_job(self, job_handle: int, exit_code: int = 1) -> bool:
            assert (job_handle, exit_code) == (11, 1)
            return True

        def wait_job_empty(self, job_handle: int, timeout_seconds: float) -> bool:
            assert job_handle == 11
            assert timeout_seconds == 0.1
            # Real Job accounting can remain non-empty while external process
            # handles are retained.  The registry must transfer exact identity
            # to the Job and close root/thread handles before this query.
            assert {22, 33}.issubset(self.closed)
            self.waits += 1
            return self.waits >= 2

        def close_handle(self, handle: int) -> bool:
            self.close_attempts.append(handle)
            if handle == 11:
                self.job_close_attempts += 1
                if self.job_close_attempts < 2:
                    return False
            self.closed.add(handle)
            return True

    slots = threading.BoundedSemaphore(1)
    assert slots.acquire(blocking=False) is True
    registry = containment_module._SuspendedProcessRollbackRegistry(slots)
    api = _RollbackApi()

    registry.retain(
        api=api,
        assigned=True,
        job_handle=11,
        process_handle=22,
        thread_handle=33,
    )

    assert registry.wait_empty(2.0) is True
    assert api.waits >= 2
    assert api.close_attempts == [33, 22, 11, 11]
    assert slots.acquire(blocking=False) is True
    slots.release()


@pytest.mark.skipif(os.name != "nt", reason="requires Windows Popen execution seam")
def test_unproven_spawn_rollback_transfers_reserved_slot_and_exact_handles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api_instances: list[object] = []

    class _Slot:
        def __init__(self) -> None:
            self.acquired = 0
            self.released = 0

        def acquire(self, *, blocking: bool) -> bool:
            assert blocking is False
            self.acquired += 1
            return True

        def release(self) -> None:
            self.released += 1

    class _UnprovenApi:
        def __init__(self) -> None:
            self.closed: list[int] = []
            api_instances.append(self)

        def create_kill_on_close_job(self) -> int:
            return 11

        def assign_process(self, job_handle: int, process_handle: int) -> None:
            assert (job_handle, process_handle) == (11, 22)
            raise OSError("assign_failed")

        def terminate_process(self, process_handle: int, exit_code: int = 1) -> bool:
            assert (process_handle, exit_code) == (22, 1)
            return True

        def wait_handle(self, process_handle: int, timeout_seconds: float) -> bool:
            assert (process_handle, timeout_seconds) == (22, 2.0)
            return False

        def close_handle(self, handle: int) -> bool:
            self.closed.append(handle)
            return True

    class _RetainedRegistry:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def retain(self, **kwargs: object) -> str:
            self.calls.append(dict(kwargs))
            return "f" * 32

    slot = _Slot()
    registry = _RetainedRegistry()

    monkeypatch.setattr(containment_module, "_WINDOWS_SPAWN_ROLLBACK_SLOTS", slot)
    monkeypatch.setattr(containment_module, "_WindowsJobApi", _UnprovenApi)
    monkeypatch.setattr(containment_module, "_SUSPENDED_PROCESS_ROLLBACKS", registry)
    monkeypatch.setattr(
        subprocess._winapi,  # type: ignore[attr-defined]
        "CreateProcess",
        lambda *_args: (22, 33, 44, 55),
    )

    with pytest.raises(RuntimeError, match="rollback_unproven"):
        containment_module._WindowsContainedPopen(
            [sys.executable, "-c", "raise SystemExit(0)"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    assert slot.acquired == 1
    assert slot.released == 0
    assert len(registry.calls) == 1
    call = registry.calls[0]
    assert call["assigned"] is False
    assert call["job_handle"] == 11
    assert call["process_handle"] == 22
    assert call["thread_handle"] == 33
    assert len(api_instances) == 1
    assert isinstance(api_instances[0], _UnprovenApi)
    assert api_instances[0].closed == []


def test_posix_owner_has_no_numeric_pid_or_pgid_signal_fallback() -> None:
    source = inspect.getsource(ContainedProcessOwner.terminate_tree)
    assert "killpg" not in source
    assert "process.terminate()" not in source
    assert "process.kill()" not in source


@pytest.mark.skipif(os.name == "nt", reason="requires a POSIX host")
def test_posix_terminate_tree_observes_without_unsafe_late_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = spawn_contained_process(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    process = owner.process
    terminate = process.terminate
    kill = process.kill
    monkeypatch.setattr(
        process,
        "terminate",
        lambda: (_ for _ in ()).throw(AssertionError("unsafe numeric PID signal")),
    )
    monkeypatch.setattr(
        process,
        "kill",
        lambda: (_ for _ in ()).throw(AssertionError("unsafe numeric PID signal")),
    )
    try:
        observed = owner.terminate_tree(time.monotonic() + 0.1)
        assert observed.tree_state is ProcessTreeEvidence.UNKNOWN
        assert observed.root_exited is False
        assert process.poll() is None
    finally:
        terminate()
        try:
            process.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            kill()
            process.wait(timeout=2.0)

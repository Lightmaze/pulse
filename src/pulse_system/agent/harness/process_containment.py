"""Race-free process ownership and descendant-tree evidence.

Windows children are created suspended, assigned to a Pulse-owned Job Object,
and resumed only after the boundary exists.  Other platforms isolate the root
in a new session, but this shared owner remains observation-only: CPython's
``Popen`` signals still target a recyclable numeric PID.  Those hosts therefore
receive neither a late OS signal nor whole-tree proof.  A retained Windows Job
handle remains an exact kernel witness for the original owner.

The public observation is payload-free.  It never contains a PID, command,
path, environment value, process output, or native handle.
"""

from __future__ import annotations

import math
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Sequence

__all__ = [
    "ContainedProcessOwner",
    "PhysicalProcessObservation",
    "ProcessContainmentKind",
    "ProcessTreeEvidence",
    "spawn_contained_process",
]


PHYSICAL_PROCESS_OWNER_PROTOCOL = "physical-process-owner.v1"

_WINDOWS_CREATE_SUSPENDED = 0x00000004
_WINDOWS_CREATE_BREAKAWAY_FROM_JOB = 0x01000000
_WINDOWS_JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION = 1
_WINDOWS_JOB_OBJECT_BASIC_PROCESS_ID_LIST = 3
_WINDOWS_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
_WINDOWS_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
_WINDOWS_WAIT_OBJECT_0 = 0
_WINDOWS_WAIT_TIMEOUT = 258
_WINDOWS_SPAWN_ROLLBACK_CAPACITY = 32
_OWNER_TOKEN_RE = re.compile(r"^[0-9a-f]{32}$")
_OBSERVATION_AUTHORITY = object()
_WINDOWS_SPAWN_ROLLBACK_SLOTS = threading.BoundedSemaphore(
    _WINDOWS_SPAWN_ROLLBACK_CAPACITY
)


class ProcessContainmentKind(str, Enum):
    WINDOWS_JOB = "windows_job"
    POSIX_PROCESS_GROUP = "posix_process_group"
    UNAVAILABLE = "unavailable"


class ProcessTreeEvidence(str, Enum):
    NOT_APPLICABLE = "not_applicable"
    EMPTY_VERIFIED = "empty_verified"
    ROOT_EXIT_ONLY = "root_exit_only"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True, init=False)
class PhysicalProcessObservation:
    """Exact, content-free observation for one retained process owner."""

    protocol_version: str
    owner_token: str
    containment: ProcessContainmentKind
    root_observed: bool
    root_exited: bool
    tree_state: ProcessTreeEvidence
    observation_generation: int
    witness_released: bool
    error_code: str | None = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        del kwargs
        raise TypeError("PhysicalProcessObservation is a final canonical type")

    def __init__(
        self,
        *,
        protocol_version: str,
        owner_token: str,
        containment: ProcessContainmentKind,
        root_observed: bool,
        root_exited: bool,
        tree_state: ProcessTreeEvidence,
        observation_generation: int,
        witness_released: bool,
        error_code: str | None = None,
        _authority: object | None = None,
    ) -> None:
        if _authority is not _OBSERVATION_AUTHORITY:
            raise TypeError(
                "PhysicalProcessObservation can only be created by its retained owner"
            )
        for name, value in (
            ("protocol_version", protocol_version),
            ("owner_token", owner_token),
            ("containment", containment),
            ("root_observed", root_observed),
            ("root_exited", root_exited),
            ("tree_state", tree_state),
            ("observation_generation", observation_generation),
            ("witness_released", witness_released),
            ("error_code", error_code),
        ):
            object.__setattr__(self, name, value)
        self.__post_init__()

    def __post_init__(self) -> None:
        if type(self.protocol_version) is not str or (
            self.protocol_version != PHYSICAL_PROCESS_OWNER_PROTOCOL
        ):
            raise ValueError("unsupported physical process owner protocol")
        if type(self.owner_token) is not str or _OWNER_TOKEN_RE.fullmatch(
            self.owner_token
        ) is None:
            raise ValueError("owner_token must be a 32-character lowercase hex id")
        if type(self.containment) is not ProcessContainmentKind:
            raise ValueError("containment must be a ProcessContainmentKind")
        if type(self.root_observed) is not bool or type(self.root_exited) is not bool:
            raise ValueError("root observation fields must be exact bool values")
        if type(self.tree_state) is not ProcessTreeEvidence:
            raise ValueError("tree_state must be a ProcessTreeEvidence")
        if (
            type(self.observation_generation) is not int
            or self.observation_generation < 0
        ):
            raise ValueError("observation_generation must be a non-negative int")
        if type(self.witness_released) is not bool:
            raise ValueError("witness_released must be an exact bool")
        if self.error_code is not None and (
            type(self.error_code) is not str
            or re.fullmatch(r"[a-z0-9_]{1,96}", self.error_code) is None
        ):
            raise ValueError("error_code must be a bounded token or None")
        if self.root_exited and not self.root_observed:
            raise ValueError("an unobserved root cannot be reported exited")
        if self.tree_state is ProcessTreeEvidence.EMPTY_VERIFIED:
            if (
                not self.root_observed
                or not self.root_exited
                or self.containment is not ProcessContainmentKind.WINDOWS_JOB
            ):
                raise ValueError(
                    "empty_verified requires an exited root in Windows Job containment"
                )
        if (
            self.containment is ProcessContainmentKind.WINDOWS_JOB
            and self.root_observed
            and self.witness_released
            and self.tree_state is not ProcessTreeEvidence.EMPTY_VERIFIED
        ):
            raise ValueError(
                "a released Windows Job witness requires retained empty-tree proof"
            )
        if self.tree_state is ProcessTreeEvidence.ROOT_EXIT_ONLY and (
            not self.root_observed or not self.root_exited
        ):
            raise ValueError("root_exit_only requires an observed exited root")
        if self.tree_state is ProcessTreeEvidence.NOT_APPLICABLE and (
            self.root_observed or self.root_exited
        ):
            raise ValueError("not_applicable cannot describe a spawned root")

    @property
    def physical_exit_proven(self) -> bool:
        return self.tree_state in {
            ProcessTreeEvidence.NOT_APPLICABLE,
            ProcessTreeEvidence.EMPTY_VERIFIED,
        }

    @property
    def resource_converged(self) -> bool:
        return self.physical_exit_proven and self.witness_released


def _physical_process_observation(
    *,
    owner_token: str,
    containment: ProcessContainmentKind,
    root_observed: bool,
    root_exited: bool,
    tree_state: ProcessTreeEvidence,
    observation_generation: int,
    witness_released: bool,
    error_code: str | None = None,
) -> PhysicalProcessObservation:
    return PhysicalProcessObservation(
        _authority=_OBSERVATION_AUTHORITY,
        protocol_version=PHYSICAL_PROCESS_OWNER_PROTOCOL,
        owner_token=owner_token,
        containment=containment,
        root_observed=root_observed,
        root_exited=root_exited,
        tree_state=tree_state,
        observation_generation=observation_generation,
        witness_released=witness_released,
        error_code=error_code,
    )


class _WindowsJobApi:
    """Fail-fast Win32 seam for one Pulse-owned outer Job Object."""

    def __init__(self) -> None:
        if os.name != "nt":
            raise OSError("windows_job_object_unavailable")
        try:
            import ctypes
            from ctypes import wintypes
        except (ImportError, AttributeError) as exc:  # pragma: no cover - Windows only
            raise OSError("windows_job_api_unavailable") from exc
        if not hasattr(ctypes, "WinDLL"):
            raise OSError("windows_job_api_unavailable")

        class _IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class _BasicLimitInformation(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class _ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", _BasicLimitInformation),
                ("IoInfo", _IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        class _BasicAccountingInformation(ctypes.Structure):
            _fields_ = [
                ("TotalUserTime", ctypes.c_longlong),
                ("TotalKernelTime", ctypes.c_longlong),
                ("ThisPeriodTotalUserTime", ctypes.c_longlong),
                ("ThisPeriodKernelTime", ctypes.c_longlong),
                ("TotalPageFaultCount", wintypes.DWORD),
                ("TotalProcesses", wintypes.DWORD),
                ("ActiveProcesses", wintypes.DWORD),
                ("TotalTerminatedProcesses", wintypes.DWORD),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.argtypes = (wintypes.LPVOID, wintypes.LPCWSTR)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = (
            wintypes.HANDLE,
            ctypes.c_int,
            wintypes.LPVOID,
            wintypes.DWORD,
        )
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = (
            wintypes.HANDLE,
            wintypes.HANDLE,
        )
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.QueryInformationJobObject.argtypes = (
            wintypes.HANDLE,
            ctypes.c_int,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.LPVOID,
        )
        kernel32.QueryInformationJobObject.restype = wintypes.BOOL
        kernel32.ResumeThread.argtypes = (wintypes.HANDLE,)
        kernel32.ResumeThread.restype = wintypes.DWORD
        kernel32.TerminateJobObject.argtypes = (wintypes.HANDLE, wintypes.UINT)
        kernel32.TerminateJobObject.restype = wintypes.BOOL
        kernel32.TerminateProcess.argtypes = (wintypes.HANDLE, wintypes.UINT)
        kernel32.TerminateProcess.restype = wintypes.BOOL
        kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        self._ctypes = ctypes
        self._kernel32 = kernel32
        self._extended_limit_type = _ExtendedLimitInformation
        self._basic_accounting_type = _BasicAccountingInformation

    def create_kill_on_close_job(self) -> int:
        raw = self._kernel32.CreateJobObjectW(None, None)
        handle = self._handle_value(raw)
        if handle == 0:
            raise self._last_error("CreateJobObjectW")
        limits = self._extended_limit_type()
        limits.BasicLimitInformation.LimitFlags = (
            _WINDOWS_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        )
        configured = self._kernel32.SetInformationJobObject(
            handle,
            _WINDOWS_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
            self._ctypes.byref(limits),
            self._ctypes.sizeof(limits),
        )
        if not configured:
            error = self._last_error("SetInformationJobObject")
            self.close_handle(handle)
            raise error
        return handle

    def assign_process(self, job_handle: int, process_handle: int) -> None:
        if not self._kernel32.AssignProcessToJobObject(job_handle, process_handle):
            raise self._last_error("AssignProcessToJobObject")

    def resume_primary_thread(self, thread_handle: int) -> int:
        previous = int(self._kernel32.ResumeThread(thread_handle))
        if previous == 0xFFFFFFFF:
            raise self._last_error("ResumeThread")
        return previous

    def terminate_job(self, job_handle: int, exit_code: int = 1) -> bool:
        return bool(self._kernel32.TerminateJobObject(job_handle, exit_code))

    def terminate_process(self, process_handle: int, exit_code: int = 1) -> bool:
        return bool(self._kernel32.TerminateProcess(process_handle, exit_code))

    def active_processes(self, job_handle: int) -> int:
        accounting = self._basic_accounting_type()
        returned_length = self._ctypes.c_uint32()
        if not self._kernel32.QueryInformationJobObject(
            job_handle,
            _WINDOWS_JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION,
            self._ctypes.byref(accounting),
            self._ctypes.sizeof(accounting),
            self._ctypes.byref(returned_length),
        ):
            raise self._last_error("QueryInformationJobObject")
        return int(accounting.ActiveProcesses)

    def process_ids(
        self,
        job_handle: int,
        *,
        capacity: int = 256,
    ) -> tuple[int, ...]:
        if type(capacity) is not int or not 1 <= capacity <= 4096:
            raise ValueError("job process-id capacity is invalid")

        class _BasicProcessIdList(self._ctypes.Structure):
            _fields_ = [
                ("NumberOfAssignedProcesses", self._ctypes.c_uint32),
                ("NumberOfProcessIdsInList", self._ctypes.c_uint32),
                ("ProcessIdList", self._ctypes.c_size_t * capacity),
            ]

        values = _BasicProcessIdList()
        returned_length = self._ctypes.c_uint32()
        if not self._kernel32.QueryInformationJobObject(
            job_handle,
            _WINDOWS_JOB_OBJECT_BASIC_PROCESS_ID_LIST,
            self._ctypes.byref(values),
            self._ctypes.sizeof(values),
            self._ctypes.byref(returned_length),
        ):
            raise self._last_error("QueryInformationJobObject")
        assigned = int(values.NumberOfAssignedProcesses)
        listed = int(values.NumberOfProcessIdsInList)
        if assigned != listed or listed > capacity:
            raise OSError("job_process_id_list_incomplete")
        process_ids = tuple(
            int(values.ProcessIdList[index]) for index in range(listed)
        )
        if any(process_id <= 0 for process_id in process_ids) or len(
            set(process_ids)
        ) != len(process_ids):
            raise OSError("job_process_id_list_invalid")
        return tuple(sorted(process_ids))

    def wait_job_empty(self, job_handle: int, timeout_seconds: float) -> bool:
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        sleep_seconds = 0.01
        while True:
            if self.active_processes(job_handle) == 0:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(remaining, sleep_seconds))
            sleep_seconds = min(0.25, sleep_seconds * 2.0)

    def wait_handle(self, handle: int, timeout_seconds: float) -> bool:
        timeout_ms = max(0, min(0xFFFFFFFE, int(timeout_seconds * 1000)))
        result = int(self._kernel32.WaitForSingleObject(handle, timeout_ms))
        if result == _WINDOWS_WAIT_OBJECT_0:
            return True
        if result == _WINDOWS_WAIT_TIMEOUT:
            return False
        raise self._last_error("WaitForSingleObject")

    def close_handle(self, handle: int) -> bool:
        return bool(self._kernel32.CloseHandle(handle))

    def _last_error(self, operation: str) -> OSError:
        code = int(self._ctypes.get_last_error())
        return OSError(code, f"{operation} failed")

    @staticmethod
    def _handle_value(handle: Any) -> int:
        if isinstance(handle, int):
            return handle
        return int(getattr(handle, "value", 0) or 0)


class _SuspendedProcessRollbackUnproven(RuntimeError):
    """The child never entered user code, but termination is not yet proven."""

    def __init__(self, *, assigned: bool) -> None:
        super().__init__("suspended_process_rollback_unproven")
        self.assigned = assigned


@dataclass(slots=True)
class _SuspendedProcessRollbackRecord:
    token: str
    api: _WindowsJobApi
    assigned: bool
    job_handle: int
    process_handle: int
    thread_handle: int
    termination_proven: bool = False


class _SuspendedProcessRollbackRegistry:
    """Bounded exact-handle owner for rare pre-admission rollback failures.

    A spawn reserves capacity before ``CreateProcess``.  If synchronous
    rollback cannot prove exit, the raw handles and that reservation move here.
    One daemon repeatedly converges the exact handles and exits when the
    registry becomes empty; it never identifies a process by PID.
    """

    def __init__(self, slots: threading.BoundedSemaphore) -> None:
        self._slots = slots
        self._lock = threading.RLock()
        self._records: dict[str, _SuspendedProcessRollbackRecord] = {}
        self._worker: threading.Thread | None = None
        self._wake = threading.Event()

    def retain(
        self,
        *,
        api: _WindowsJobApi,
        assigned: bool,
        job_handle: int,
        process_handle: int,
        thread_handle: int,
    ) -> str:
        if any(
            type(value) is not int or value <= 0
            for value in (job_handle, process_handle, thread_handle)
        ):
            raise ValueError("rollback handles must be positive exact integers")
        token = uuid.uuid4().hex
        record = _SuspendedProcessRollbackRecord(
            token=token,
            api=api,
            assigned=bool(assigned),
            job_handle=job_handle,
            process_handle=process_handle,
            thread_handle=thread_handle,
        )
        with self._lock:
            self._records[token] = record
            worker = self._worker
            if worker is None or not worker.is_alive():
                worker = threading.Thread(
                    target=self._run,
                    name="pulse-suspended-rollback",
                    daemon=True,
                )
                self._worker = worker
                try:
                    worker.start()
                except RuntimeError:
                    # The exact handles remain owned by this bounded registry.
                    # A later retain/wait observation may start a successor;
                    # propagating here would let the Popen frame close handles
                    # that have already crossed the ownership boundary.
                    self._worker = None
            self._wake.set()
        return token

    def retained_count(self) -> int:
        with self._lock:
            return len(self._records)

    def wait_empty(self, timeout_seconds: float) -> bool:
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        while True:
            self._ensure_worker()
            if self.retained_count() == 0:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            self._wake.wait(min(0.02, remaining))

    def _ensure_worker(self) -> None:
        with self._lock:
            if not self._records:
                return
            worker = self._worker
            if worker is not None and worker.is_alive():
                return
            worker = threading.Thread(
                target=self._run,
                name="pulse-suspended-rollback",
                daemon=True,
            )
            self._worker = worker
            try:
                worker.start()
            except RuntimeError:
                self._worker = None

    def _run(self) -> None:
        while True:
            with self._lock:
                records = tuple(self._records.values())
                if not records:
                    self._worker = None
                    self._wake.set()
                    return
                self._wake.clear()
            for record in records:
                try:
                    converged = self._converge_once(record)
                except BaseException:
                    converged = False
                if not converged:
                    continue
                removed = False
                with self._lock:
                    if self._records.get(record.token) is record:
                        del self._records[record.token]
                        removed = True
                        self._wake.set()
                if removed:
                    self._slots.release()
            self._wake.wait(0.05)

    @staticmethod
    def _converge_once(record: _SuspendedProcessRollbackRecord) -> bool:
        api = record.api
        if record.assigned:
            # Assignment transferred exact identity to the Job.  Native Job
            # accounting may retain ActiveProcesses until external process
            # handles are released, so those handles must be closed *before*
            # waiting for an empty census.  The Job handle remains the exact
            # kill/reobserve witness throughout this transition.
            try:
                api.terminate_job(record.job_handle, exit_code=1)
            except OSError:
                pass
            _SuspendedProcessRollbackRegistry._close_handle_field(
                record, "thread_handle"
            )
            _SuspendedProcessRollbackRegistry._close_handle_field(
                record, "process_handle"
            )
            if record.thread_handle or record.process_handle:
                return False
            if not record.termination_proven:
                try:
                    record.termination_proven = api.wait_job_empty(
                        record.job_handle, 0.1
                    )
                except OSError:
                    record.termination_proven = False
            if record.termination_proven:
                _SuspendedProcessRollbackRegistry._close_handle_field(
                    record, "job_handle"
                )
        else:
            # No Job assignment occurred, so the process handle itself is the
            # only exact identity.  The unrelated Job and primary thread
            # handles can be released immediately; the process handle stays
            # retained until WaitForSingleObject proves termination.
            _SuspendedProcessRollbackRegistry._close_handle_field(
                record, "thread_handle"
            )
            _SuspendedProcessRollbackRegistry._close_handle_field(
                record, "job_handle"
            )
            if not record.termination_proven:
                try:
                    api.terminate_process(record.process_handle, exit_code=1)
                except OSError:
                    pass
                try:
                    record.termination_proven = api.wait_handle(
                        record.process_handle, 0.1
                    )
                except OSError:
                    record.termination_proven = False
            if record.termination_proven:
                _SuspendedProcessRollbackRegistry._close_handle_field(
                    record, "process_handle"
                )
        return not any(
            (record.thread_handle, record.process_handle, record.job_handle)
        )

    @staticmethod
    def _close_handle_field(
        record: _SuspendedProcessRollbackRecord,
        field_name: str,
    ) -> bool:
        handle = int(getattr(record, field_name))
        if not handle:
            return True
        try:
            closed = record.api.close_handle(handle)
        except OSError:
            closed = False
        if closed:
            setattr(record, field_name, 0)
        return bool(closed)


_SUSPENDED_PROCESS_ROLLBACKS = _SuspendedProcessRollbackRegistry(
    _WINDOWS_SPAWN_ROLLBACK_SLOTS
)


def _establish_suspended_job_boundary(
    api: _WindowsJobApi,
    *,
    job_handle: int,
    process_handle: int,
    thread_handle: int,
) -> None:
    """Attach before resume, rolling back without running user code on failure."""

    assigned = False
    try:
        api.assign_process(job_handle, process_handle)
        assigned = True
        previous_suspend_count = api.resume_primary_thread(thread_handle)
        if previous_suspend_count != 1:
            raise OSError("primary_thread_suspend_count_unexpected")
    except BaseException as boundary_error:
        rollback_proven = False
        if assigned:
            try:
                api.terminate_job(job_handle, exit_code=1)
            except OSError:
                pass
            try:
                rollback_proven = api.wait_job_empty(job_handle, 2.0)
            except OSError:
                rollback_proven = False
        else:
            try:
                api.terminate_process(process_handle, exit_code=1)
            except OSError:
                pass
            try:
                rollback_proven = api.wait_handle(process_handle, 2.0)
            except OSError:
                rollback_proven = False
        if not rollback_proven:
            raise _SuspendedProcessRollbackUnproven(
                assigned=assigned
            ) from boundary_error
        raise


class _WindowsContainedPopen(subprocess.Popen[Any]):
    """``Popen`` with a retained primary thread and race-free outer Job."""

    def __init__(
        self,
        *args: Any,
        _job_api: _WindowsJobApi | None = None,
        **kwargs: Any,
    ) -> None:
        if os.name != "nt":
            raise OSError("windows_job_object_unavailable")
        self._pulse_job_lock = threading.RLock()
        self._pulse_job_handle = 0
        self._pulse_job_assigned = False
        self._pulse_primary_thread_resumed = False
        self._pulse_spawn_slot_held = False
        if not _WINDOWS_SPAWN_ROLLBACK_SLOTS.acquire(blocking=False):
            raise OSError("windows_contained_spawn_capacity_unavailable")
        self._pulse_spawn_slot_held = True
        try:
            self._pulse_job_api = _job_api or _WindowsJobApi()
            self._pulse_job_handle = self._pulse_job_api.create_kill_on_close_job()
            try:
                super().__init__(*args, **kwargs)
            except BaseException:
                self._close_job_unchecked()
                raise
        except BaseException:
            raise
        finally:
            self._release_spawn_rollback_slot()

    @property
    def containment_assigned_before_resume(self) -> bool:
        return self._pulse_job_assigned and self._pulse_primary_thread_resumed

    def contained_process_ids(self) -> tuple[int, ...]:
        with self._pulse_job_lock:
            job_handle = self._pulse_job_handle
        if not job_handle:
            return ()
        return self._pulse_job_api.process_ids(job_handle)

    def containment_state(self) -> bool | None:
        self._release_root_handle_after_exit()
        with self._pulse_job_lock:
            job_handle = self._pulse_job_handle
        if not job_handle:
            return self.poll() is not None
        try:
            return self._pulse_job_api.active_processes(job_handle) == 0
        except OSError:
            return None

    @property
    def containment_released(self) -> bool:
        with self._pulse_job_lock:
            return not bool(self._pulse_job_handle)

    def terminate_contained_tree(self, timeout_seconds: float = 3.0) -> bool:
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not 0 < float(timeout_seconds) <= 30
        ):
            return False
        with self._pulse_job_lock:
            job_handle = self._pulse_job_handle
            if not job_handle:
                return self.poll() is not None
            deadline = time.monotonic() + float(timeout_seconds)
            terminated = self._pulse_job_api.terminate_job(job_handle, exit_code=1)
            try:
                self.wait(timeout=max(0.001, deadline - time.monotonic()))
            except (OSError, subprocess.TimeoutExpired):
                return False
            self._release_root_handle_after_exit()
            try:
                settled = self._pulse_job_api.wait_job_empty(
                    job_handle,
                    max(0.0, deadline - time.monotonic()),
                )
            except OSError:
                settled = False
            if not settled:
                return False
            closed = self._pulse_job_api.close_handle(job_handle)
            if closed:
                self._pulse_job_handle = 0
        if not (terminated or settled):
            return False
        return bool(closed and self.poll() is not None)

    def close_containment(self) -> bool:
        self._release_root_handle_after_exit()
        with self._pulse_job_lock:
            job_handle = self._pulse_job_handle
            if not job_handle:
                return True
            try:
                if self._pulse_job_api.active_processes(job_handle) != 0:
                    return False
            except OSError:
                return False
            closed = self._pulse_job_api.close_handle(job_handle)
            if closed:
                self._pulse_job_handle = 0
            return bool(closed)

    def _release_root_handle_after_exit(self) -> None:
        if self.returncode is None:
            return
        handle = getattr(self, "_handle", None)
        if handle is None:
            return
        try:
            handle.Close()
        except (OSError, ValueError):
            return
        self._handle = None

    def _close_job_unchecked(self) -> None:
        lock = getattr(self, "_pulse_job_lock", None)
        if lock is None:
            return
        with lock:
            handle = getattr(self, "_pulse_job_handle", 0)
            if not handle:
                return
            try:
                closed = self._pulse_job_api.close_handle(handle)
            except BaseException:
                closed = False
            if closed:
                self._pulse_job_handle = 0

    def _release_spawn_rollback_slot(self) -> None:
        if not getattr(self, "_pulse_spawn_slot_held", False):
            return
        self._pulse_spawn_slot_held = False
        _WINDOWS_SPAWN_ROLLBACK_SLOTS.release()

    def __del__(self) -> None:  # pragma: no cover - deterministic paths close explicitly
        try:
            self._close_job_unchecked()
        finally:
            try:
                super().__del__()
            except BaseException:
                pass

    def _execute_child(
        self,
        args: Any,
        executable: Any,
        preexec_fn: Any,
        close_fds: bool,
        pass_fds: Any,
        cwd: Any,
        env: Any,
        startupinfo: Any,
        creationflags: int,
        shell: bool,
        p2cread: Any,
        p2cwrite: Any,
        c2pread: Any,
        c2pwrite: Any,
        errread: Any,
        errwrite: Any,
        unused_restore_signals: Any,
        unused_gid: Any,
        unused_gids: Any,
        unused_uid: Any,
        unused_umask: Any,
        unused_start_new_session: Any,
        unused_process_group: Any,
    ) -> None:
        del (
            preexec_fn,
            unused_restore_signals,
            unused_gid,
            unused_gids,
            unused_uid,
            unused_umask,
            unused_start_new_session,
            unused_process_group,
        )
        if pass_fds:
            raise ValueError("pass_fds is unsupported by contained Windows spawn")
        if shell:
            raise ValueError("shell=True is forbidden by contained Windows spawn")
        if creationflags & _WINDOWS_CREATE_BREAKAWAY_FROM_JOB:
            raise ValueError("CREATE_BREAKAWAY_FROM_JOB is forbidden")

        if isinstance(args, str):
            command_line = args
        elif isinstance(args, bytes):
            command_line = subprocess.list2cmdline([args])
        elif isinstance(args, os.PathLike):
            command_line = subprocess.list2cmdline([args])
        else:
            command_line = subprocess.list2cmdline(args)
        executable_value = None if executable is None else os.fsdecode(executable)
        startup = subprocess.STARTUPINFO() if startupinfo is None else startupinfo.copy()
        use_std_handles = -1 not in (p2cread, c2pwrite, errwrite)
        winapi = subprocess._winapi  # type: ignore[attr-defined]
        if use_std_handles:
            startup.dwFlags |= winapi.STARTF_USESTDHANDLES
            startup.hStdInput = p2cread
            startup.hStdOutput = c2pwrite
            startup.hStdError = errwrite

        attribute_list = startup.lpAttributeList
        have_handle_list = bool(
            attribute_list
            and "handle_list" in attribute_list
            and attribute_list["handle_list"]
        )
        if have_handle_list or (use_std_handles and close_fds):
            if attribute_list is None:
                attribute_list = startup.lpAttributeList = {}
            handle_list = attribute_list["handle_list"] = list(
                attribute_list.get("handle_list", [])
            )
            if use_std_handles:
                handle_list += [int(p2cread), int(c2pwrite), int(errwrite)]
            handle_list[:] = self._filter_handle_list(handle_list)
            if handle_list:
                close_fds = False

        cwd_value = None if cwd is None else os.fsdecode(cwd)
        sys.audit("subprocess.Popen", executable_value, command_line, cwd_value, env)
        process_handle = 0
        thread_handle = 0
        try:
            process_handle, thread_handle, pid, _tid = winapi.CreateProcess(
                executable_value,
                command_line,
                None,
                None,
                int(not close_fds),
                creationflags | _WINDOWS_CREATE_SUSPENDED,
                env,
                cwd_value,
                startup,
            )
            try:
                _establish_suspended_job_boundary(
                    self._pulse_job_api,
                    job_handle=self._pulse_job_handle,
                    process_handle=process_handle,
                    thread_handle=thread_handle,
                )
            except _SuspendedProcessRollbackUnproven as rollback_error:
                if (
                    type(self._pulse_job_api) is _WindowsJobApi
                    and self._pulse_spawn_slot_held
                ):
                    _SUSPENDED_PROCESS_ROLLBACKS.retain(
                        api=self._pulse_job_api,
                        assigned=rollback_error.assigned,
                        job_handle=self._pulse_job_handle,
                        process_handle=process_handle,
                        thread_handle=thread_handle,
                    )
                    with self._pulse_job_lock:
                        self._pulse_job_handle = 0
                    self._pulse_spawn_slot_held = False
                    process_handle = 0
                    thread_handle = 0
                raise
            self._pulse_job_assigned = True
            self._pulse_primary_thread_resumed = True
            self._handle = subprocess.Handle(process_handle)
            process_handle = 0
            self.pid = pid
            self._child_created = True
        finally:
            self._close_pipe_fds(
                p2cread,
                p2cwrite,
                c2pread,
                c2pwrite,
                errread,
                errwrite,
            )
            if thread_handle:
                winapi.CloseHandle(thread_handle)
            if process_handle:
                winapi.CloseHandle(process_handle)


class ContainedProcessOwner:
    """Final identity cell that retains a process and its containment witness."""

    __slots__ = (
        "_containment",
        "_generation",
        "_last_observation",
        "_lock",
        "_owner_token",
        "_process",
    )

    def __init_subclass__(cls, **kwargs: Any) -> None:
        del kwargs
        raise TypeError("ContainedProcessOwner is a final canonical type")

    def __init__(
        self,
        process: subprocess.Popen[Any],
        containment: ProcessContainmentKind,
    ) -> None:
        if not isinstance(process, subprocess.Popen):
            raise TypeError("process must be a subprocess.Popen")
        if type(containment) is not ProcessContainmentKind:
            raise TypeError("containment must be a ProcessContainmentKind")
        if containment is ProcessContainmentKind.WINDOWS_JOB and type(
            process
        ) is not _WindowsContainedPopen:
            raise TypeError("windows_job containment requires the canonical Popen")
        self._process = process
        self._containment = containment
        self._owner_token = uuid.uuid4().hex
        self._generation = 0
        self._last_observation: PhysicalProcessObservation | None = None
        self._lock = threading.RLock()

    @property
    def process(self) -> subprocess.Popen[Any]:
        return self._process

    @property
    def owner_token(self) -> str:
        return self._owner_token

    @property
    def containment(self) -> ProcessContainmentKind:
        return self._containment

    def observe(self) -> PhysicalProcessObservation:
        with self._lock:
            process = self._process
            root_exited = process.poll() is not None
            error_code: str | None = None
            witness_released = True
            if self._containment is ProcessContainmentKind.WINDOWS_JOB:
                assert type(process) is _WindowsContainedPopen
                state = process.containment_state()
                if state is True:
                    tree_state = ProcessTreeEvidence.EMPTY_VERIFIED
                    witness_released = process.close_containment()
                    if not witness_released:
                        error_code = "containment_handle_close_unproven"
                elif state is False and root_exited:
                    tree_state = ProcessTreeEvidence.ROOT_EXIT_ONLY
                    witness_released = process.containment_released
                else:
                    tree_state = ProcessTreeEvidence.UNKNOWN
                    witness_released = process.containment_released
                    if state is None:
                        error_code = "containment_observation_unavailable"
            elif root_exited:
                tree_state = ProcessTreeEvidence.ROOT_EXIT_ONLY
            else:
                tree_state = ProcessTreeEvidence.UNKNOWN
            previous = self._last_observation
            if previous is not None:
                root_exited = previous.root_exited or root_exited
                if previous.tree_state is ProcessTreeEvidence.EMPTY_VERIFIED:
                    tree_state = ProcessTreeEvidence.EMPTY_VERIFIED
                elif (
                    previous.tree_state is ProcessTreeEvidence.ROOT_EXIT_ONLY
                    and tree_state is ProcessTreeEvidence.UNKNOWN
                ):
                    tree_state = ProcessTreeEvidence.ROOT_EXIT_ONLY
                witness_released = (
                    previous.witness_released or witness_released
                )
            if (
                tree_state is ProcessTreeEvidence.EMPTY_VERIFIED
                and witness_released
            ):
                error_code = None
            self._generation += 1
            observation = _physical_process_observation(
                owner_token=self._owner_token,
                containment=self._containment,
                root_observed=True,
                root_exited=root_exited,
                tree_state=tree_state,
                observation_generation=self._generation,
                witness_released=witness_released,
                error_code=error_code,
            )
            self._last_observation = observation
            return observation

    def close_containment_if_empty(self) -> bool:
        """Release only an exact witness whose process-tree census is empty."""

        with self._lock:
            if self._containment is not ProcessContainmentKind.WINDOWS_JOB:
                return True
            process = self._process
            assert type(process) is _WindowsContainedPopen
            if process.containment_state() is not True:
                return False
            return process.close_containment()

    def terminate_tree(self, deadline_monotonic: float) -> PhysicalProcessObservation:
        if isinstance(deadline_monotonic, bool) or not isinstance(
            deadline_monotonic, (int, float)
        ):
            raise TypeError("deadline_monotonic must be a finite number")
        deadline = float(deadline_monotonic)
        if not math.isfinite(deadline):
            raise ValueError("deadline_monotonic must be finite")
        with self._lock:
            process = self._process
            remaining = max(0.0, deadline - time.monotonic())
            if self._containment is ProcessContainmentKind.WINDOWS_JOB:
                if remaining > 0:
                    assert type(process) is _WindowsContainedPopen
                    process.terminate_contained_tree(min(30.0, remaining))
            # On POSIX, CPython's retained Popen.terminate()/kill() still send
            # to the recyclable numeric PID.  Without a pidfd (or equivalent
            # spawn-time kernel identity) a late signal can hit an unrelated
            # successor, so this protocol deliberately observes only.  Pi/MCP
            # first use their own pipe/protocol cancellation; a non-cooperative
            # POSIX root remains honestly unresolved until a future kernel-
            # fenced containment adapter is available.
        return self.observe()


def spawn_contained_process(
    argv: Sequence[str],
    *,
    cwd: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
    stdin: Any = subprocess.PIPE,
    stdout: Any = subprocess.PIPE,
    stderr: Any = subprocess.PIPE,
    creationflags: int = 0,
) -> ContainedProcessOwner:
    """Create one shell-free child under the strongest truthful host boundary."""

    if isinstance(argv, (str, bytes, bytearray)):
        raise TypeError("argv must be a sequence of strings")
    command = list(argv)
    if not command or any(type(item) is not str or not item for item in command):
        raise ValueError("argv must contain non-empty exact strings")
    if type(creationflags) is not int or creationflags < 0:
        raise ValueError("creationflags must be a non-negative int")
    if os.name == "nt":
        if creationflags & _WINDOWS_CREATE_BREAKAWAY_FROM_JOB:
            raise ValueError("CREATE_BREAKAWAY_FROM_JOB is forbidden")
        process = _WindowsContainedPopen(
            command,
            cwd=None if cwd is None else os.fspath(cwd),
            env=None if env is None else dict(env),
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            shell=False,
            close_fds=True,
            creationflags=(
                creationflags | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            ),
        )
        return ContainedProcessOwner(process, ProcessContainmentKind.WINDOWS_JOB)
    process = subprocess.Popen(  # noqa: S603 - argv is explicit, shell is forbidden
        command,
        cwd=None if cwd is None else os.fspath(cwd),
        env=None if env is None else dict(env),
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
        shell=False,
        close_fds=True,
        start_new_session=True,
    )
    return ContainedProcessOwner(process, ProcessContainmentKind.POSIX_PROCESS_GROUP)


# Private compatibility aliases used by sandbox and its established tests.
WindowsJobApi = _WindowsJobApi
WindowsContainedPopen = _WindowsContainedPopen
establish_suspended_job_boundary = _establish_suspended_job_boundary

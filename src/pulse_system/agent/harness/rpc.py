"""Concurrent-safe JSONL RPC demultiplexing for a live Pi process."""

from __future__ import annotations

import json
import queue
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, TypedDict

from pulse_system.agent.backends.pi import (
    PiTransportCloseSummary,
    RpcConnectionLost,
    RpcTimeout,
    RpcTransport,
    SubprocessRpcTransport,
)

__all__ = ["PiRpcChannel", "PiRpcCloseSummary", "RpcProtocolError"]


class RpcProtocolError(Exception):
    """Pi emitted a correlated response that violates its public RPC shape."""


class PiRpcCloseSummary(TypedDict):
    """Lossless, content-free close evidence for one RPC channel."""

    signal_dispatched: bool
    signal_sent: bool
    process_owners_observed: int
    process_owners_unresolved: int
    channel_reader_owners_observed: int
    channel_reader_owners_unresolved: int
    transport_reader_owners_observed: int
    transport_reader_owners_unresolved: int
    close_worker_owners_observed: int
    close_worker_owners_unresolved: int
    internal_owner_unresolved: int
    unresolved: int
    owner_joined: bool
    process_tree_state: Literal[
        "not_applicable",
        "empty_verified",
        "root_exit_only",
        "unknown",
    ]
    error_code: str | None


_TRANSPORT_CLOSE_KEYS = frozenset(
    {
        "signal_sent",
        "process_owners_observed",
        "process_owners_unresolved",
        "reader_owners_observed",
        "reader_owners_unresolved",
        "internal_owner_unresolved",
        "owner_joined",
        "process_tree_state",
        "returncode",
        "error_code",
    }
)
_PHYSICAL_FINAL_TREE_STATES = frozenset({"not_applicable", "empty_verified"})
_TREE_RANK = {
    "unknown": 0,
    "root_exit_only": 1,
    "not_applicable": 2,
    "empty_verified": 2,
}


@dataclass(frozen=True, slots=True)
class _Failure:
    error: Exception


class PiRpcChannel:
    """Own the single stdout reader and correlate concurrent RPC requests.

    A turn waits on the ordered event queue while ``abort`` and ``steer`` can
    issue requests from other threads.  Only this class reads stdout, so an
    event or response can never be consumed by the wrong caller.
    """

    def __init__(
        self,
        transport: RpcTransport,
        *,
        id_prefix: str = "pulse",
        autostart: bool = True,
    ):
        self._transport = transport
        self._id_prefix = id_prefix
        self._lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._pending: dict[str, queue.Queue[dict[str, Any] | _Failure]] = {}
        self._events: queue.Queue[dict[str, Any] | _Failure] = queue.Queue()
        self._sequence = 0
        self._closed = False
        self._terminal_error: Exception | None = None
        self._close_signal_done = threading.Event()
        self._close_wait_done = threading.Event()
        self._close_signal_thread: threading.Thread | None = None
        self._close_wait_thread: threading.Thread | None = None
        self._transport_close_summary: Mapping[str, Any] | None = None
        self._close_signal_error_code: str | None = None
        self._close_wait_error_code: str | None = None
        self._close_wait_generation = 0
        self._close_summary: PiRpcCloseSummary | None = None
        self._reader = threading.Thread(
            target=self._read_forever,
            name=f"pi-rpc-reader-{id_prefix}",
            daemon=True,
        )
        self._reader_started = False
        if autostart:
            try:
                self.start_reader()
            except RpcConnectionLost:
                # Preserve the historical constructor contract without ever
                # abandoning its exact transport.  The persistent Pi session
                # uses autostart=False and publishes this channel before the
                # activation edge; direct callers get bounded cleanup here.
                try:
                    self.finish_close(timeout_sec=2.25)
                except Exception:
                    pass
                raise

    def start_reader(self) -> None:
        """Start the sole RPC reader after the channel has an owning cell."""

        with self._lock:
            if self._reader_started:
                return
            if self._closed:
                raise RpcConnectionLost(
                    "Pi RPC channel closed before reader activation"
                )
            try:
                self._reader.start()
            except RuntimeError as exc:
                error = RpcConnectionLost(
                    f"Pi RPC reader could not start: {exc}"
                )
                self._terminal_error = error
                raise error from exc
            self._reader_started = True

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def diagnostics(self) -> dict[str, Any]:
        try:
            return dict(self._transport.diagnostics() or {})
        except Exception as exc:  # diagnostics must not conceal the real failure
            return {"returncode": None, "stderr_tail": f"diagnostics failed: {exc}"}

    def request(
        self,
        body: dict[str, Any],
        *,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> dict[str, Any]:
        """Send a command and wait only for its same-ID response."""

        command = body.get("type")
        if not isinstance(command, str) or not command:
            raise ValueError("RPC request type must be a non-empty string")

        waiter: queue.Queue[dict[str, Any] | _Failure] = queue.Queue(maxsize=1)
        with self._lock:
            if self._closed or self._terminal_error is not None:
                error = self._terminal_error or RpcConnectionLost("Pi RPC channel is closed")
                raise RpcConnectionLost(str(error)) from error
            self._sequence += 1
            request_id = f"{self._id_prefix}-{self._sequence}"
            self._pending[request_id] = waiter

        payload = dict(body, id=request_id)
        try:
            encoded = json.dumps(payload, ensure_ascii=False) + "\n"
            with self._write_lock:
                self._transport.send_line(encoded)
        except RpcConnectionLost as exc:
            with self._lock:
                self._pending.pop(request_id, None)
            self._signal_failure(exc)
            raise
        except (OSError, ValueError) as exc:
            lost = RpcConnectionLost(f"Pi RPC command {command!r} could not be sent: {exc}")
            with self._lock:
                self._pending.pop(request_id, None)
            self._signal_failure(lost)
            raise lost from exc

        try:
            budget = self._budget(timeout=timeout, deadline=deadline)
            item = waiter.get(timeout=budget)
        except queue.Empty:
            with self._lock:
                self._pending.pop(request_id, None)
            raise RpcTimeout from None
        finally:
            with self._lock:
                self._pending.pop(request_id, None)

        if isinstance(item, _Failure):
            raise item.error
        if item.get("type") != "response":
            raise RpcProtocolError(
                f"Pi response to {command!r} has type {item.get('type')!r}"
            )
        if item.get("id") != request_id:
            raise RpcProtocolError(
                f"Pi response to {command!r} has mismatched id {item.get('id')!r}"
            )
        response_command = item.get("command")
        if response_command != command:
            raise RpcProtocolError(
                f"Pi response id {request_id!r} names command {response_command!r}, "
                f"expected {command!r}"
            )
        if type(item.get("success")) is not bool:
            raise RpcProtocolError(
                f"Pi response to {command!r} has no boolean success field"
            )
        return item

    def read_event(
        self,
        *,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> dict[str, Any]:
        """Read the next non-response object in original stdout order."""

        try:
            item = self._events.get(timeout=self._budget(timeout=timeout, deadline=deadline))
        except queue.Empty:
            raise RpcTimeout from None
        if isinstance(item, _Failure):
            raise item.error
        return item

    def drain_events(self) -> list[dict[str, Any]]:
        """Discard and return events left by a completed lifecycle command."""

        drained: list[dict[str, Any]] = []
        while True:
            try:
                item = self._events.get_nowait()
            except queue.Empty:
                break
            if isinstance(item, _Failure):
                self._events.put(item)
                break
            drained.append(item)
        return drained

    def begin_close(self) -> None:
        """Dispatch transport shutdown without waiting for transport exit."""

        with self._lock:
            if self._close_signal_thread is not None:
                return
            self._closed = True
            signal_thread = threading.Thread(
                target=self._dispatch_transport_close_signal,
                name=f"pi-rpc-close-signal-{self._id_prefix}",
                daemon=True,
            )
            self._close_signal_thread = signal_thread
            self._signal_failure(RpcConnectionLost("Pi RPC channel was closed"))
            try:
                signal_thread.start()
            except RuntimeError as exc:
                # Thread.start is the commit edge.  Roll the reservation back
                # so the same exact channel can retry signal dispatch on the
                # next close observation instead of becoming permanently
                # poisoned by a non-started Thread object.
                if self._close_signal_thread is signal_thread:
                    self._close_signal_thread = None
                self._close_signal_error_code = (
                    f"pi_close_signal_start_{type(exc).__name__}"
                )
                raise

    def finish_close(
        self,
        *,
        deadline: float | None = None,
        timeout_sec: float | None = None,
    ) -> PiRpcCloseSummary:
        """Wait only within one shared deadline and report every owner."""

        if deadline is not None and timeout_sec is not None:
            raise ValueError("pass deadline or timeout_sec, not both")
        if timeout_sec is not None and timeout_sec < 0:
            raise ValueError("timeout_sec must be non-negative or None")
        if deadline is None:
            deadline = time.monotonic() + (
                2.25 if timeout_sec is None else timeout_sec
            )
        try:
            self.begin_close()
        except RuntimeError:
            # Preserve the exact transport as unresolved.  A later close call
            # re-enters begin_close because the failed reservation rolled
            # back; no owner or retry capability is lost.
            pass
        with self._lock:
            if self._summary_is_final(self._close_summary):
                assert self._close_summary is not None
                return dict(self._close_summary)
            signal_thread = self._close_signal_thread

        self._join_until(signal_thread, deadline)
        with self._lock:
            wait_generation_before = self._close_wait_generation
        if self._close_signal_done.is_set():
            try:
                self._start_transport_wait(deadline)
            except RuntimeError:
                pass
        with self._lock:
            wait_thread = self._close_wait_thread
            started_wait_generation = (
                self._close_wait_generation > wait_generation_before
            )
        self._join_until(wait_thread, deadline)
        if (
            not started_wait_generation
            and wait_thread is not None
            and not wait_thread.is_alive()
            and time.monotonic() < deadline
        ):
            # This call inherited an older live generation.  Once that worker
            # ends non-final, the same bounded call may spend its remaining
            # budget on exactly one fresh observation generation.
            try:
                self._start_transport_wait(deadline)
            except RuntimeError:
                pass
            with self._lock:
                refreshed_wait_thread = self._close_wait_thread
            if refreshed_wait_thread is not wait_thread:
                wait_thread = refreshed_wait_thread
                self._join_until(wait_thread, deadline)
        if self._reader_started and self._reader is not threading.current_thread():
            self._join_until(self._reader, deadline)

        with self._lock:
            raw = dict(self._transport_close_summary or {})
            close_error = (
                self._close_signal_error_code or self._close_wait_error_code
            )
            signal_thread = self._close_signal_thread
            wait_thread = self._close_wait_thread

        typed_transport = self._validated_transport_summary(raw)
        if typed_transport is None:
            # A legacy transport may have returned cleanly, but it did not
            # prove whether it owns pump threads or an embedded process.  Its
            # successful return must therefore never become JOINED by default.
            # Do not call arbitrary diagnostics here: that compatibility hook
            # has no bounded-wait contract and must not defeat close deadlines.
            typed_transport = {
                "signal_sent": self._close_signal_done.is_set(),
                "process_owners_observed": 1,
                "process_owners_unresolved": 1,
                "reader_owners_observed": 0,
                "reader_owners_unresolved": 0,
                "internal_owner_unresolved": 1,
                "owner_joined": False,
                "process_tree_state": "unknown",
                "error_code": "pi_transport_owner_contract_missing",
            }
        raw = typed_transport

        signal_unresolved = int(
            signal_thread is not None and signal_thread.is_alive()
        )
        wait_unresolved = int(wait_thread is not None and wait_thread.is_alive())
        channel_reader_observed = int(self._reader_started)
        channel_reader_unresolved = int(
            self._reader_started and self._reader.is_alive()
        )
        transport_reader_unresolved = max(
            0, int(raw.get("reader_owners_unresolved", 0))
        )
        process_unresolved = max(
            0, int(raw.get("process_owners_unresolved", 0))
        )
        transport_internal_unresolved = max(
            transport_reader_unresolved,
            int(raw.get("internal_owner_unresolved", 0)),
        )
        close_worker_unresolved = signal_unresolved + wait_unresolved
        internal_unresolved = (
            channel_reader_unresolved
            + transport_internal_unresolved
            + close_worker_unresolved
        )
        tree = raw.get("process_tree_state")
        if tree not in {
            "not_applicable",
            "empty_verified",
            "root_exit_only",
            "unknown",
        }:
            tree = "unknown"
        if tree not in _PHYSICAL_FINAL_TREE_STATES:
            # A non-final tree is itself one unresolved physical owner even
            # when a compatibility transport reports its root count as zero.
            process_unresolved = max(1, process_unresolved)
        unresolved = process_unresolved + internal_unresolved
        error_code = close_error or raw.get("error_code")
        if unresolved and not error_code:
            error_code = "pi_rpc_owner_exit_unproven"
        summary: PiRpcCloseSummary = {
            "signal_dispatched": signal_thread is not None,
            "signal_sent": bool(raw.get("signal_sent")),
            "process_owners_observed": max(
                process_unresolved,
                int(raw.get("process_owners_observed", 0)),
            ),
            "process_owners_unresolved": process_unresolved,
            "channel_reader_owners_observed": channel_reader_observed,
            "channel_reader_owners_unresolved": channel_reader_unresolved,
            "transport_reader_owners_observed": max(
                0, int(raw.get("reader_owners_observed", 0))
            ),
            "transport_reader_owners_unresolved": transport_reader_unresolved,
            "close_worker_owners_observed": int(signal_thread is not None)
            + int(wait_thread is not None),
            "close_worker_owners_unresolved": close_worker_unresolved,
            "internal_owner_unresolved": internal_unresolved,
            "unresolved": unresolved,
            "owner_joined": bool(raw.get("owner_joined"))
            and unresolved == 0
            and tree in _PHYSICAL_FINAL_TREE_STATES,
            "process_tree_state": tree,
            "error_code": error_code,
        }
        with self._lock:
            self._close_summary = self._merge_close_summary(
                self._close_summary,
                summary,
            )
            return dict(self._close_summary)

    def await_close_signal(self, *, deadline: float) -> bool:
        """Wait for the non-blocking signal phase, never for owner exit."""

        try:
            self.begin_close()
        except RuntimeError:
            return False
        remaining = max(0.0, deadline - time.monotonic())
        return self._close_signal_done.wait(timeout=remaining)

    def close(self) -> PiRpcCloseSummary:
        return self.finish_close()

    def _dispatch_transport_close_signal(self) -> None:
        try:
            signal = getattr(self._transport, "signal_close", None)
            if callable(signal):
                signal()
            else:
                result = self._transport.close()
                if isinstance(result, Mapping):
                    with self._lock:
                        self._transport_close_summary = dict(result)
            with self._lock:
                self._close_signal_error_code = None
        except Exception as exc:  # close evidence must survive adapter failure
            with self._lock:
                self._close_signal_error_code = (
                    f"pi_transport_close_{type(exc).__name__}"
                )
        finally:
            self._close_signal_done.set()

    def _start_transport_wait(self, deadline: float) -> None:
        wait = getattr(self._transport, "wait_closed", None)
        if not callable(wait):
            self._close_wait_done.set()
            return
        with self._lock:
            current = self._close_wait_thread
            if current is not None and current.is_alive():
                return
            if self._transport_summary_is_final_locked():
                return
            remaining = max(0.0, deadline - time.monotonic())
            self._close_wait_generation += 1
            generation = self._close_wait_generation
            self._close_wait_done.clear()
            wait_thread = threading.Thread(
                target=self._wait_transport_closed,
                args=(wait, remaining),
                name=f"pi-rpc-close-wait-{self._id_prefix}-{generation}",
                daemon=True,
            )
            self._close_wait_thread = wait_thread
            try:
                wait_thread.start()
            except RuntimeError as exc:
                if self._close_wait_thread is wait_thread:
                    self._close_wait_thread = None
                self._close_wait_error_code = (
                    f"pi_close_wait_start_{type(exc).__name__}"
                )
                raise

    def _wait_transport_closed(self, wait: Any, timeout_sec: float) -> None:
        try:
            result = wait(timeout_sec=timeout_sec)
            if isinstance(result, Mapping):
                with self._lock:
                    self._transport_close_summary = dict(result)
                    self._close_wait_error_code = None
        except Exception as exc:  # preserve typed uncertainty for Runtime
            with self._lock:
                self._close_wait_error_code = (
                    f"pi_transport_wait_{type(exc).__name__}"
                )
        finally:
            self._close_wait_done.set()

    def _validated_transport_summary(
        self,
        raw: Mapping[str, Any],
    ) -> PiTransportCloseSummary | None:
        """Accept exact typed evidence and fence fake ``empty_verified``."""

        if type(raw) is not dict or frozenset(raw) != _TRANSPORT_CLOSE_KEYS:
            return None
        bool_fields = ("signal_sent", "owner_joined")
        if any(type(raw.get(name)) is not bool for name in bool_fields):
            return None
        count_fields = (
            "process_owners_observed",
            "process_owners_unresolved",
            "reader_owners_observed",
            "reader_owners_unresolved",
            "internal_owner_unresolved",
        )
        if any(
            type(raw.get(name)) is not int or int(raw[name]) < 0
            for name in count_fields
        ):
            return None
        if (
            raw["process_owners_unresolved"] > raw["process_owners_observed"]
            or raw["reader_owners_unresolved"] > raw["reader_owners_observed"]
            or raw["internal_owner_unresolved"]
            < raw["reader_owners_unresolved"]
        ):
            return None
        tree = raw.get("process_tree_state")
        if type(tree) is not str or tree not in _TREE_RANK:
            return None
        if (
            tree == "empty_verified"
            and type(self._transport) is not SubprocessRpcTransport
        ):
            return None
        returncode = raw.get("returncode")
        if returncode is not None and type(returncode) is not int:
            return None
        error_code = raw.get("error_code")
        if error_code is not None and (
            type(error_code) is not str or not 1 <= len(error_code) <= 160
        ):
            return None
        physical_final = tree in _PHYSICAL_FINAL_TREE_STATES
        expected_joined = bool(
            raw["process_owners_unresolved"] == 0
            and raw["reader_owners_unresolved"] == 0
            and raw["internal_owner_unresolved"] == 0
            and physical_final
        )
        if raw["owner_joined"] is not expected_joined:
            return None
        return dict(raw)  # type: ignore[return-value]

    def _transport_summary_is_final_locked(self) -> bool:
        raw = dict(self._transport_close_summary or {})
        validated = self._validated_transport_summary(raw)
        return bool(
            validated is not None
            and validated["owner_joined"]
            and validated["process_tree_state"] in _PHYSICAL_FINAL_TREE_STATES
        )

    @staticmethod
    def _summary_is_final(summary: PiRpcCloseSummary | None) -> bool:
        return bool(
            summary is not None
            and summary["owner_joined"]
            and summary["unresolved"] == 0
            and summary["process_tree_state"] in _PHYSICAL_FINAL_TREE_STATES
        )

    @classmethod
    def _merge_close_summary(
        cls,
        current: PiRpcCloseSummary | None,
        candidate: PiRpcCloseSummary,
    ) -> PiRpcCloseSummary:
        if current is None:
            return candidate
        current_tree = current["process_tree_state"]
        candidate_tree = candidate["process_tree_state"]
        tree = (
            current_tree
            if _TREE_RANK[current_tree] >= _TREE_RANK[candidate_tree]
            else candidate_tree
        )

        process_unresolved = min(
            current["process_owners_unresolved"],
            candidate["process_owners_unresolved"],
        )
        channel_reader_unresolved = min(
            current["channel_reader_owners_unresolved"],
            candidate["channel_reader_owners_unresolved"],
        )
        transport_reader_unresolved = min(
            current["transport_reader_owners_unresolved"],
            candidate["transport_reader_owners_unresolved"],
        )
        close_worker_unresolved = min(
            current["close_worker_owners_unresolved"],
            candidate["close_worker_owners_unresolved"],
        )
        internal_unresolved = max(
            channel_reader_unresolved
            + transport_reader_unresolved
            + close_worker_unresolved,
            min(
                current["internal_owner_unresolved"],
                candidate["internal_owner_unresolved"],
            ),
        )
        unresolved = process_unresolved + internal_unresolved
        owner_joined = (
            unresolved == 0 and tree in _PHYSICAL_FINAL_TREE_STATES
        )
        return {
            "signal_dispatched": (
                current["signal_dispatched"] or candidate["signal_dispatched"]
            ),
            "signal_sent": current["signal_sent"] or candidate["signal_sent"],
            "process_owners_observed": max(
                current["process_owners_observed"],
                candidate["process_owners_observed"],
            ),
            "process_owners_unresolved": process_unresolved,
            "channel_reader_owners_observed": max(
                current["channel_reader_owners_observed"],
                candidate["channel_reader_owners_observed"],
            ),
            "channel_reader_owners_unresolved": channel_reader_unresolved,
            "transport_reader_owners_observed": max(
                current["transport_reader_owners_observed"],
                candidate["transport_reader_owners_observed"],
            ),
            "transport_reader_owners_unresolved": transport_reader_unresolved,
            "close_worker_owners_observed": max(
                current["close_worker_owners_observed"],
                candidate["close_worker_owners_observed"],
            ),
            "close_worker_owners_unresolved": close_worker_unresolved,
            "internal_owner_unresolved": internal_unresolved,
            "unresolved": unresolved,
            "owner_joined": owner_joined,
            "process_tree_state": tree,
            "error_code": None if owner_joined else candidate["error_code"],
        }

    @staticmethod
    def _join_until(thread: threading.Thread | None, deadline: float) -> None:
        if thread is None or thread is threading.current_thread():
            return
        if thread.ident is None and not thread.is_alive():
            return
        remaining = max(0.0, deadline - time.monotonic())
        if remaining > 0:
            thread.join(timeout=remaining)

    @staticmethod
    def _budget(*, timeout: float | None, deadline: float | None) -> float | None:
        candidates: list[float] = []
        if timeout is not None:
            candidates.append(timeout)
        if deadline is not None:
            candidates.append(deadline - time.monotonic())
        if not candidates:
            return None
        budget = min(candidates)
        if budget <= 0:
            raise RpcTimeout
        return budget

    def _read_forever(self) -> None:
        while True:
            with self._lock:
                if self._closed:
                    return
            try:
                line = self._transport.read_line(None)
            except RpcTimeout:
                continue
            except RpcConnectionLost as exc:
                self._signal_failure(exc)
                return
            except (OSError, ValueError) as exc:
                self._signal_failure(RpcConnectionLost(f"Pi RPC read failed: {exc}"))
                return

            if line is None:
                self._signal_failure(RpcConnectionLost("Pi closed its RPC stdout"))
                return
            stripped = line.strip()
            if not stripped:
                continue
            try:
                value = json.loads(stripped)
            except ValueError:
                self._events.put({
                    "type": "pi_unparsable_line",
                    "text": stripped[:400],
                })
                continue
            if not isinstance(value, dict):
                self._events.put({
                    "type": "pi_unexpected_json",
                    "value_type": type(value).__name__,
                })
                continue

            request_id = value.get("id")
            waiter = None
            if value.get("type") == "response" and isinstance(request_id, str):
                with self._lock:
                    waiter = self._pending.get(request_id)
            if waiter is not None:
                waiter.put(value)
            else:
                self._events.put(value)

    def _signal_failure(self, error: Exception) -> None:
        with self._lock:
            if self._terminal_error is None:
                self._terminal_error = error
            failure = _Failure(self._terminal_error)
            waiters = list(self._pending.values())
            self._pending.clear()
        for waiter in waiters:
            try:
                waiter.put_nowait(failure)
            except queue.Full:
                pass
        self._events.put(failure)

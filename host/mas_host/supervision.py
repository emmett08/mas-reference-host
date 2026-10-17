"""Killable Linux adapter execution with parent-owned protocol operations.

Adapters are trusted code, and a fork is not a security sandbox: inherited
memory includes host state. Only bounded integer JSON crosses the local IPC
channel. The single-threaded parent retains admission and checkpoint authority.
Terminating local execution cannot establish cancellation of a remote effect.
"""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
import json
import math
import os
import select
import signal
import socket
import sys
import threading
import time
from typing import Callable

from .codec import integer_json as _json
from .contracts import (Adapter, AdapterError, AdapterInterrupted, AdapterTimeout,
                        HostContext, Invocation, KnownInvocationFailure, Outcome, RecoveryRequired,
                        RunnerError, TaskRequest, invocation_failure_diagnostic,
                        validate_invocation_diagnostic)
from .process_group import ProcessGroup
from .provider_outcomes import ProviderOutcomeFailure, validate_provider_outcome, validate_completed_output


MAX_FRAME = 65_536


def _invalid() -> None:
    raise AdapterError("invalid adapter IPC frame")


def _decode(data: bytes) -> dict:
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                _invalid()
            value[key] = item
        return value

    def reject(_value):
        _invalid()

    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=pairs,
                           parse_float=reject, parse_constant=reject)
        if type(value) is not dict:
            _invalid()
        _json(value, MAX_FRAME - 1)
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise AdapterError("invalid adapter IPC frame") from None


def _fields(frame: dict, names: set[str]) -> None:
    if set(frame) != names:
        _invalid()


def _raise_reply_error(frame: dict) -> None:
    """Only public error categories cross IPC; exception text never does."""
    if "error" not in frame:
        return
    if frame["error"] == "known_invocation_failure":
        _fields(frame, {"error", "outcome", "call_id"} | ({"completed_output"} if "completed_output" in frame else set()))
        if type(frame["call_id"]) is not str or not 0 < len(frame["call_id"]) <= 256:
            _invalid()
        try:
            outcome = validate_provider_outcome(frame["outcome"])
            output = validate_completed_output(outcome, frame.get("completed_output"))
        except (ValueError, RunnerError):
            _invalid()
        raise KnownInvocationFailure(outcome, frame["call_id"], completed_output=output)
    _fields(frame, {"error"})
    if frame["error"] == "adapter_error":
        raise AdapterError("delegated adapter failed; inspect run status")
    if frame["error"] == "runner_error":
        raise RunnerError("host operation rejected; inspect run status")
    _invalid()


def _operation_reply(channel, operation):
    """Return recoverable failures to the adapter; uncertainty remains fatal."""
    try:
        return True, operation()
    except (RecoveryRequired, AdapterInterrupted, AdapterTimeout):
        raise
    except KnownInvocationFailure as error:
        channel.send({"error": "known_invocation_failure", "outcome": error.outcome,
                      "call_id": error.call_id, **({"completed_output": error.completed_output}
                          if error.completed_output is not None else {})})
    except AdapterError:
        channel.send({"error": "adapter_error"})
    except RunnerError:
        channel.send({"error": "runner_error"})
    return False, None


class _Channel:
    def __init__(self, sock: socket.socket, deadline: float):
        self.sock = sock
        self.deadline = deadline
        self.buffer = bytearray()

    def remaining(self) -> float:
        value = self.deadline - time.monotonic()
        if value <= 0:
            raise AdapterTimeout("adapter deadline exceeded; outcome may be unknown")
        return value

    def send(self, value: dict) -> None:
        source = (_json(value, MAX_FRAME - 1) + "\n").encode("utf-8")
        try:
            self.sock.settimeout(self.remaining())
            self.sock.sendall(source)
        except (OSError, TimeoutError):
            raise AdapterError("adapter IPC interrupted; outcome may be unknown") from None

    def receive(self, tick: Callable[[], None] | None = None,
                poll_interval: float = 0.05) -> dict:
        while True:
            remaining = self.remaining()
            newline = self.buffer.find(b"\n")
            if newline >= 0:
                source = bytes(self.buffer[:newline])
                del self.buffer[:newline + 1]
                return _decode(source)
            if len(self.buffer) >= MAX_FRAME:
                _invalid()
            if tick is not None:
                tick()
                remaining = self.remaining()
            try:
                readable, _, _ = select.select([self.sock], [], [],
                                                min(remaining, poll_interval))
                if not readable:
                    continue
                data = self.sock.recv(min(4096, MAX_FRAME - len(self.buffer)))
            except (OSError, TimeoutError):
                raise AdapterError("adapter IPC interrupted; outcome may be unknown") from None
            if not data:
                self.remaining()
                raise AdapterError("adapter exited without a result; outcome may be unknown")
            self.buffer.extend(data)


class _ContextProxy:
    """A child-side context; callables remain in the child, never in IPC."""

    def __init__(self, channel: _Channel, actor: str):
        self._channel = channel
        self._actor = actor
        self._thread = threading.get_ident()
        self._invoking = False
        self._invocation_id: str | None = None

    @property
    def actor(self) -> str:
        return self._actor

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, self._channel.deadline - time.monotonic())

    @property
    def invocation_id(self) -> str | None:
        """Durable attempt identifier; diagnostic correlation, not idempotency."""
        return self._invocation_id

    def _check(self) -> None:
        if self._invoking or threading.get_ident() != self._thread:
            raise RunnerError("adapter context is not active")
        self._channel.remaining()

    def request(self, recipient: str, operation: str, input: dict) -> Outcome:
        self._check()
        self._channel.send({"op": "request", "recipient": recipient,
                            "operation": operation, "input": input})
        reply = self._channel.receive()
        _raise_reply_error(reply)
        _fields(reply, {"output", "status"})
        return Outcome(reply["output"], reply["status"])

    def assessment(self, operation: str, data: dict) -> dict:
        self._check()
        self._channel.send({"op": "assessment", "operation": operation, "data": data})
        reply = self._channel.receive()
        _raise_reply_error(reply)
        _fields(reply, {"assessment"})
        if type(reply["assessment"]) is not dict:
            _invalid()
        return reply["assessment"]

    def invoke(self, call, *, input: dict | None = None,
               reservation_microusd: int | None = None) -> dict:
        retry_of = None
        while True:
            try:
                return self._invoke_once(call, input=input, reservation_microusd=reservation_microusd,
                                         retry_of=retry_of)
            except KnownInvocationFailure as error:
                self._check()
                self._channel.send({"op": "retry", "call_id": error.call_id})
                reply = self._channel.receive()
                _raise_reply_error(reply)
                _fields(reply, {"retry"})
                plan = reply["retry"]
                if plan is None:
                    raise
                if (type(plan) is not dict or type(plan.get("eligible_at_ms")) is not int
                        or plan.get("previous_call_id") != error.call_id):
                    _invalid()
                while time.time() * 1000 < plan["eligible_at_ms"]:
                    time.sleep(min(0.05, self._channel.remaining(),
                                   max(0, plan["eligible_at_ms"] / 1000 - time.time())))
                retry_of = error.call_id

    def _invoke_once(self, call, *, input, reservation_microusd, retry_of=None):
        self._check()
        self._channel.send({"op": "prepare", "input": input,
                            "reservation_microusd": reservation_microusd,
                            **({"retry_of": retry_of} if retry_of is not None else {})})
        reply = self._channel.receive()
        _raise_reply_error(reply)
        _fields(reply, {"execute", "output", "call_id"})
        if reply["execute"] is False:
            if type(reply["output"]) is not dict:
                _invalid()
            return reply["output"]
        if (reply["execute"] is not True or reply["output"] is not None
                or type(reply["call_id"]) is not str or not reply["call_id"]):
            _invalid()
        try:
            self._invoking = True
            self._invocation_id = reply["call_id"]
            invocation = call()
            if (not isinstance(invocation, Invocation)
                    or type(invocation.output) is not dict
                    or type(invocation.evidence) is not dict):
                raise AdapterError("adapter invocation returned an invalid result")
            self._channel.send({"op": "complete", "output": invocation.output,
                                "evidence": invocation.evidence})
            result = self._channel.receive()
            _fields(result, {"output"})
            if type(result["output"]) is not dict:
                _invalid()
            return result["output"]
        except ProviderOutcomeFailure as error:
            # The parent validates and checkpoints this observation before the
            # adapter may catch it and continue with application-owned handling.
            checked = validate_provider_outcome(error.outcome)
            self._channel.send({"op": "known_failure", "outcome": checked,
                                **({"completed_output": error.completed_output}
                                   if error.completed_output is not None else {})})
            acknowledgement = self._channel.receive()
            _fields(acknowledgement, {"known_failure"})
            if acknowledgement["known_failure"] is not True:
                _invalid()
            raise KnownInvocationFailure(checked, reply["call_id"], completed_output=error.completed_output) from None
        except Exception as error:
            # No exception text, keys, or provider response bodies become
            # diagnostics. If this frame cannot be sent the parent still holds
            # the outstanding ticket and records the same uncertainty.
            try:
                diagnostic = invocation_failure_diagnostic(error)
                self._channel.send({"op": "failed", **(
                    {"diagnostic": diagnostic} if diagnostic is not None else {})})
            except Exception:
                pass
            raise AdapterError("adapter invocation failed; outcome may be unknown") from None
        finally:
            self._invoking = False
            self._invocation_id = None


def _child(channel: _Channel, adapter, actor: str, request) -> None:
    try:
        outcome = adapter.handle(_ContextProxy(channel, actor), request)
        if (not isinstance(outcome, Outcome) or type(outcome.output) is not dict
                or outcome.status not in ("completed", "unresolved")):
            raise AdapterError("adapter returned an invalid outcome")
        channel.send({"op": "result", "output": outcome.output, "status": outcome.status})
    except BaseException:
        try:
            channel.send({"op": "error"})
        except BaseException:
            pass
    finally:
        channel.sock.close()
        # Do not flush or run copied parent atexit handlers after fork.
        os._exit(0)


class AdapterProtocolSession:
    """Parent-owned worker protocol, shared by forked and registered workers.

    The caller owns worker lifetime and must stop/reap it before finish_pending.
    No signal handler or process creation occurs here. Legacy fork supervision
    supplies an admission guard for its main-thread shutdown signal handling.
    """

    def __init__(self, context: HostContext, *, admission_guard=nullcontext):
        self.context = context
        self.ticket = None
        self.diagnostic = None
        self._admission_guard = admission_guard

    def serve(self, channel, *, tick=None, poll_interval=0.05) -> Outcome:
        try:
            while True:
                frame = channel.receive(tick, poll_interval)
                operation = frame.get("op")
                if operation == "complete":
                    _fields(frame, {"op", "output", "evidence"})
                    if self.ticket is None or type(frame["output"]) is not dict or type(frame["evidence"]) is not dict:
                        _invalid()
                    output = self.context.complete_invocation(self.ticket, Invocation(frame["output"], frame["evidence"]))
                    self.ticket = None
                    channel.send({"output": output})
                elif operation == "known_failure":
                    _fields(frame, {"op", "outcome"} | ({"completed_output"} if "completed_output" in frame else set()))
                    if self.ticket is None:
                        _invalid()
                    try:
                        checked = validate_provider_outcome(frame["outcome"])
                        output = validate_completed_output(checked, frame.get("completed_output"))
                    except (ValueError, RunnerError):
                        _invalid()
                    if output is None:
                        self.context.reject_invocation(self.ticket, checked)
                    else:
                        self.context.reject_invocation(self.ticket, checked, completed_output=output)
                    self.ticket = None
                    channel.send({"known_failure": True})
                elif operation == "failed":
                    _fields(frame, {"op", "diagnostic"} if "diagnostic" in frame else {"op"})
                    if self.ticket is None:
                        _invalid()
                    if "diagnostic" in frame:
                        try:
                            self.diagnostic = validate_invocation_diagnostic(frame["diagnostic"])
                        except ValueError:
                            _invalid()
                    raise AdapterError("adapter invocation failed; outcome may be unknown")
                elif self.ticket is not None:
                    _invalid()
                elif operation == "retry":
                    _fields(frame, {"op", "call_id"})
                    if type(frame["call_id"]) is not str or not 0 < len(frame["call_id"]) <= 256:
                        _invalid()
                    accepted, plan = _operation_reply(channel, lambda: self.context.retry_invocation(frame["call_id"]))
                    if accepted:
                        channel.send({"retry": plan})
                elif operation == "prepare":
                    _fields(frame, {"op", "input", "reservation_microusd"}
                            | ({"retry_of"} if "retry_of" in frame else set()))
                    amount = frame["reservation_microusd"]
                    if (frame["input"] is not None and type(frame["input"]) is not dict
                            or amount is not None and (type(amount) is not int or amount < 0)
                            or "retry_of" in frame and (type(frame["retry_of"]) is not str
                                                       or not 0 < len(frame["retry_of"]) <= 256)):
                        _invalid()
                    # Install ownership of the ticket before handling shutdown.
                    # Admission may have committed executing intent when it returns.
                    with self._admission_guard():
                        accepted, prepared = _operation_reply(channel, lambda: self.context.prepare_invocation(
                            input=frame["input"], reservation_microusd=frame["reservation_microusd"],
                            **({"retry_of": frame["retry_of"]} if "retry_of" in frame else {})))
                        if accepted and prepared.execute:
                            self.ticket = prepared
                    if not accepted:
                        continue
                    if prepared.execute:
                        channel.send({"execute": True, "output": None, "call_id": prepared.call_id})
                    else:
                        channel.send({"execute": False, "output": prepared.cached_output,
                                      "call_id": prepared.call_id})
                elif operation == "request":
                    _fields(frame, {"op", "recipient", "operation", "input"})
                    if (type(frame["recipient"]) is not str or type(frame["operation"]) is not str
                            or type(frame["input"]) is not dict):
                        _invalid()
                    accepted, outcome = _operation_reply(channel, lambda: self.context.request(
                        frame["recipient"], frame["operation"], frame["input"]))
                    if not accepted:
                        continue
                    channel.send({"output": outcome.output, "status": outcome.status})
                elif operation == "assessment":
                    _fields(frame, {"op", "operation", "data"})
                    if type(frame["operation"]) is not str or type(frame["data"]) is not dict:
                        _invalid()
                    accepted, result = _operation_reply(channel, lambda: self.context.assessment(
                        frame["operation"], frame["data"]))
                    if accepted:
                        channel.send({"assessment": result})
                elif operation == "result":
                    _fields(frame, {"op", "output", "status"})
                    if type(frame["output"]) is not dict or frame["status"] not in ("completed", "unresolved"):
                        _invalid()
                    return Outcome(frame["output"], frame["status"])
                elif operation == "error":
                    _fields(frame, {"op"})
                    raise AdapterError("adapter execution failed; inspect run status")
                else:
                    _invalid()
        except Exception:
            if self.ticket is not None:
                raise RecoveryRequired("adapter invocation interrupted; inspect recovery journal") from None
            raise

    def finish_pending(self) -> None:
        """After worker teardown, preserve any unacknowledged effect as unknown."""
        if self.ticket is not None:
            try:
                if self.diagnostic is None:
                    self.context.fail_invocation(self.ticket)
                else:
                    self.context.fail_invocation(self.ticket, diagnostic=self.diagnostic)
            except Exception:
                pass  # A durable executing record still requires reconciliation.


@contextmanager
def _admission_signals_blocked():
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGINT})
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)



def execute(adapter, context, request, *, deadline: float,
            tick: Callable[[], None] | None = None, poll_interval: float = 0.05) -> Outcome:
    """Supervise one handler until an absolute monotonic run deadline.

    ``context`` stays in the parent. Its request/prepare/complete/fail methods
    are authoritative. Recoverable operation errors return a sanitised public
    category to the adapter; uncertain effects and journal failures abort.
    Nested execution must receive the same or an earlier absolute deadline.
    The caller must bound administrative commands and its reconciliation tick.
    SIGTERM/SIGINT stop local work and leave pending effects uncertain.
    """
    if (not sys.platform.startswith("linux") or threading.active_count() != 1
            or threading.current_thread() is not threading.main_thread()):
        raise RunnerError("supervised adapters require a single-threaded Linux host")
    if (type(deadline) not in (int, float) or not math.isfinite(deadline)
            or type(poll_interval) not in (int, float)
            or not math.isfinite(poll_interval) or not 0 < poll_interval <= 1):
        raise RunnerError("invalid adapter supervision deadline or poll interval")
    if deadline <= time.monotonic():
        raise AdapterTimeout("adapter deadline exceeded; outcome may be unknown")
    parent_sock, child_sock = socket.socketpair()
    channel = _Channel(parent_sock, deadline)
    group = None
    session = AdapterProtocolSession(context, admission_guard=_admission_signals_blocked)
    previous = {}

    def interrupt(_number, _frame):
        raise AdapterInterrupted("host shutdown interrupted adapter; outcome may be unknown")

    try:
        for number in (signal.SIGTERM, signal.SIGINT):
            previous[number] = signal.signal(number, interrupt)
        # Do not let shutdown land between forking the guardian and installing
        # its owner object: the finally block must always be able to close it.
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGINT})
        try:
            group = ProcessGroup(child_sock.fileno(),
                                 lambda: _child(_Channel(child_sock, deadline), adapter,
                                                context.actor, request), deadline,
                                 getattr(context, "ownership_descriptors", ()))
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        child_sock.close()
        return session.serve(channel, tick=tick, poll_interval=poll_interval)
    finally:
        # Repeated shutdown signals must not interrupt child cleanup. Restore
        # the caller's handlers afterwards, including nested supervision.
        for number in previous:
            signal.signal(number, signal.SIG_IGN)
        try:
            if group is not None:
                group.close()
            session.finish_pending()
        finally:
            parent_sock.close()
            child_sock.close()
            for number, handler in previous.items():
                signal.signal(number, handler)


class InlineExecutor:
    """In-process strategy for deterministic testing; no execution isolation."""

    def execute(self, adapter: Adapter, context: HostContext, request: TaskRequest, *,
                deadline: float, tick: Callable[[], None] | None = None) -> Outcome:
        return adapter.handle(context, request)


class LinuxProcessExecutor:
    """Trusted adapters in a killable group owned by an independent guardian."""

    def execute(self, adapter: Adapter, context: HostContext, request: TaskRequest, *,
                deadline: float, tick: Callable[[], None] | None = None) -> Outcome:
        return execute(adapter, context, request, deadline=deadline, tick=tick)

"""Real process lifetime and bounded IPC tests; no provider API is called."""

import ctypes
import gc
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
import warnings
import weakref

from mas_host.runner import AdapterError, Invocation, Outcome, RecoveryRequired, RunnerError, TaskRequest
from mas_host.contracts import AdapterContext
from mas_host.journal import JournalError, RunLock
from mas_host.supervision import (AdapterInterrupted, AdapterTimeout,
                                  InlineExecutor, LinuxProcessExecutor, _decode, execute)


REQUEST = TaskRequest("a", "b", "test", {}, "task", "message")


class FunctionAdapter:
    def __init__(self, function):
        self.function = function

    def handle(self, context, request):
        return self.function(context, request)


class ParentContext:
    actor = "b"

    def __init__(self, cached=None, request=None):
        self.events = []
        self.cached = cached
        self.delegate = request

    def prepare_invocation(self, **kwargs):
        self.events.append(("prepare", os.getpid(), kwargs))
        return SimpleNamespace(call_id="call-1", execute=self.cached is None,
                               cached_output=self.cached)

    def complete_invocation(self, ticket, invocation):
        self.events.append(("complete", os.getpid(), ticket.call_id, invocation.evidence))
        return invocation.output

    def fail_invocation(self, ticket):
        self.events.append(("unknown", os.getpid(), ticket.call_id))

    def request(self, recipient, operation, input):
        self.events.append(("request", os.getpid(), recipient, operation, input))
        if self.delegate:
            return self.delegate(recipient, operation, input)
        return Outcome({"delegated": input})


@unittest.skipUnless(sys.platform.startswith("linux"), "Linux process supervision")
class SupervisionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.context = ParentContext()
        self.original_descriptors = set(os.listdir("/proc/self/fd"))

    def run_adapter(self, function, timeout=2, context=None, tick=None):
        return execute(FunctionAdapter(function), context or self.context, REQUEST,
                       deadline=time.monotonic() + timeout, tick=tick)

    def record_and_hang(self, path):
        def callback(*_args):
            path.write_text(str(os.getpid()))
            while True:
                time.sleep(0.01)
        return callback

    def assert_reaped(self, path):
        child_pid = int(path.read_text())
        with self.assertRaises(ChildProcessError):
            os.waitpid(child_pid, os.WNOHANG)
        with self.assertRaises(ProcessLookupError):
            os.kill(child_pid, 0)

    def stopped(self, pid):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        stat = Path(f"/proc/{pid}/stat")
        try:
            return stat.read_text().split(") ", 1)[1].startswith("Z")
        except (FileNotFoundError, ProcessLookupError):
            return True

    def test_invocation_is_admitted_and_completed_by_parent(self):
        def adapter(context, _request):
            result = context.invoke(
                lambda: Invocation({"child_pid": os.getpid(), "id": context.invocation_id},
                                   {"source": "stub"}),
                input={"model": "stub"}, reservation_microusd=100)
            self.assertIsNone(context.invocation_id)
            return Outcome(result)

        result = self.run_adapter(adapter)
        self.assertNotEqual(result.output["child_pid"], os.getpid())
        self.assertEqual(result.output["id"], "call-1")
        self.assertEqual([event[0] for event in self.context.events], ["prepare", "complete"])
        self.assertTrue(all(event[1] == os.getpid() for event in self.context.events))
        self.assertEqual(self.context.events[0][2],
                         {"input": {"model": "stub"}, "reservation_microusd": 100})
        self.assertEqual(set(os.listdir("/proc/self/fd")), self.original_descriptors)

    def test_cached_invocation_never_calls_provider(self):
        path = Path(self.temp.name) / "must-not-run"

        def adapter(context, _request):
            def provider():
                path.write_text("effect")
                return Invocation({}, {})
            return Outcome(context.invoke(provider))

        context = ParentContext(cached={"saved": 3})
        self.assertEqual(self.run_adapter(adapter, context=context).output, {"saved": 3})
        self.assertFalse(path.exists())
        self.assertEqual([event[0] for event in context.events], ["prepare"])

    def test_inherited_finaliser_cannot_close_reused_worker_descriptor(self):
        """Raw fork cleanup must not leave cyclic host owners able to close new FDs."""
        was_enabled = gc.isenabled()
        frozen_before = gc.get_freeze_count()
        gc.disable()
        try:
            stream = open(Path(self.temp.name) / "host-file", "wb")
            inherited_descriptor = stream.fileno()
            cycle = [stream]
            cycle.append(cycle)
            del stream, cycle  # Only cyclic GC can now close the inherited file.
            output = Path(self.temp.name) / "worker-file"

            def adapter(_context, _request):
                descriptor = os.open(output, os.O_CREAT | os.O_WRONLY, 0o600)
                if descriptor != inherited_descriptor:
                    os.dup2(descriptor, inherited_descriptor)
                    os.close(descriptor)
                try:
                    # Explicit collection also defeats merely disabling GC.
                    gc.collect()
                    os.write(inherited_descriptor, b"worker owns this descriptor")
                    class NewCycle:
                        pass
                    value = NewCycle()
                    value.cycle = value
                    reference = weakref.ref(value)
                    del value
                    gc.collect()
                    return Outcome({"new_objects_collected": reference() is None})
                finally:
                    os.close(inherited_descriptor)

            outcome = self.run_adapter(adapter)
            self.assertTrue(outcome.output["new_objects_collected"])
            self.assertEqual(output.read_bytes(), b"worker owns this descriptor")
            self.assertEqual(gc.get_freeze_count(), frozen_before)
            self.assertFalse(gc.isenabled())
        finally:
            # This deliberate orphan fixture belongs to the parent; finalise it
            # without reporting the intentional unclosed-file test as a leak.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", ResourceWarning)
                gc.collect()
            if was_enabled:
                gc.enable()

    def test_zero_cost_mock_reservation_still_uses_parent_admission(self):
        result = self.run_adapter(lambda context, _request: Outcome(context.invoke(
            lambda: Invocation({"mock": True}, {"network": False}),
            input={"provider": "mock"}, reservation_microusd=0)))
        self.assertEqual(result.output, {"mock": True})
        self.assertEqual(self.context.events[0][2]["reservation_microusd"], 0)
        self.assertEqual([event[0] for event in self.context.events], ["prepare", "complete"])

    def test_hung_handler_is_killed_and_reaped_without_invocation(self):
        path = Path(self.temp.name) / "pid"
        ticks = []
        start = time.monotonic()
        with self.assertRaises(AdapterTimeout):
            self.run_adapter(self.record_and_hang(path), timeout=0.2,
                             tick=lambda: ticks.append(time.monotonic()))
        self.assertLess(time.monotonic() - start, 1)
        self.assertGreaterEqual(len(ticks), 2)
        self.assertEqual(self.context.events, [])
        self.assert_reaped(path)

    def test_hung_invocation_retains_unknown_and_is_not_replayed(self):
        path = Path(self.temp.name) / "pid"
        with self.assertRaises(RecoveryRequired):
            self.run_adapter(lambda context, _request:
                             Outcome(context.invoke(self.record_and_hang(path))), timeout=0.2)
        self.assertEqual([event[0] for event in self.context.events], ["prepare", "unknown"])
        self.assert_reaped(path)

    def test_child_death_after_admission_retains_unknown(self):
        def provider():
            os._exit(7)

        with self.assertRaises(AdapterError):
            self.run_adapter(lambda context, _request: Outcome(context.invoke(provider)))
        self.assertEqual([event[0] for event in self.context.events], ["prepare", "unknown"])

    def test_parent_reconciliation_error_still_kills_child(self):
        path = Path(self.temp.name) / "pid"

        def tick():
            if path.exists():
                raise RunnerError("reconciliation failed")

        with self.assertRaisesRegex(RunnerError, "reconciliation failed"):
            self.run_adapter(self.record_and_hang(path), tick=tick)
        self.assert_reaped(path)

    def test_nested_delegation_inherits_absolute_deadline(self):
        deadline = time.monotonic() + 0.25
        path = Path(self.temp.name) / "nested-pid"

        def delegate(*_args):
            return execute(FunctionAdapter(self.record_and_hang(path)), ParentContext(),
                           REQUEST, deadline=deadline)

        context = ParentContext(request=delegate)
        with self.assertRaises(AdapterTimeout):
            execute(FunctionAdapter(lambda ctx, _request: ctx.request("c", "check", {})),
                    context, REQUEST, deadline=deadline)
        self.assertLess(time.monotonic() - deadline, 0.5)
        self.assert_reaped(path)
        self.assertEqual(set(os.listdir("/proc/self/fd")), self.original_descriptors)

    def test_sigterm_cleans_child_and_restores_host_handler(self):
        path = Path(self.temp.name) / "pid"
        previous = signal.getsignal(signal.SIGTERM)
        host_pid = os.getpid()

        def provider():
            path.write_text(str(os.getpid()))
            os.kill(host_pid, signal.SIGTERM)
            while True:
                time.sleep(0.01)

        with self.assertRaises(RecoveryRequired):
            self.run_adapter(lambda context, _request: Outcome(context.invoke(provider)))
        self.assertIs(signal.getsignal(signal.SIGTERM), previous)
        self.assertEqual([event[0] for event in self.context.events], ["prepare", "unknown"])
        self.assert_reaped(path)

    def test_shutdown_during_admission_keeps_ticket_recoverable(self):
        effect = Path(self.temp.name) / "effect"

        class InterruptAfterAdmission(ParentContext):
            def prepare_invocation(self, **kwargs):
                ticket = super().prepare_invocation(**kwargs)
                os.kill(os.getpid(), signal.SIGTERM)
                return ticket

        context = InterruptAfterAdmission()

        def provider():
            effect.write_text("must not dispatch")
            return Invocation({}, {})

        with self.assertRaises(RecoveryRequired):
            self.run_adapter(lambda ctx, _request: Outcome(ctx.invoke(provider)), context=context)
        self.assertFalse(effect.exists())
        self.assertEqual([event[0] for event in context.events], ["prepare", "unknown"])

    def test_sigkill_owner_terminates_direct_adapter_child(self):
        path = Path(self.temp.name) / "pid"
        owner = os.fork()
        if owner == 0:
            try:
                self.run_adapter(self.record_and_hang(path), timeout=10)
            finally:
                os._exit(0)
        try:
            until = time.monotonic() + 3
            while not path.exists() and time.monotonic() < until:
                time.sleep(0.01)
            self.assertTrue(path.exists())
            child = int(path.read_text())
            os.kill(owner, signal.SIGKILL)
            os.waitpid(owner, 0)
            owner = None
            # The container's init owns this orphan; it may remain a zombie
            # until init reaps it. Either absent or zombie means no execution.
            while time.monotonic() < until:
                if self.stopped(child):
                    break
                time.sleep(0.01)
            else:
                self.fail("adapter remained alive after its host died")
        finally:
            if owner is not None:
                os.kill(owner, signal.SIGKILL)
                os.waitpid(owner, 0)

    def test_background_descendant_in_same_process_group_is_terminated(self):
        path = Path(self.temp.name) / "descendant"

        def adapter(_context, _request):
            descendant = os.fork()
            if descendant == 0:
                while True:
                    time.sleep(0.01)
            path.write_text(str(descendant))
            return Outcome({})

        self.run_adapter(adapter)
        child = int(path.read_text())
        until = time.monotonic() + 1
        while time.monotonic() < until:
            if self.stopped(child):
                break
            time.sleep(0.01)
        else:
            self.fail("adapter left background work alive")

    def test_owner_sigkill_keeps_lock_until_guardian_reaps_descendants(self):
        """No escaping/daemonising: a normal fork must not outlive run ownership."""
        directory = Path(self.temp.name)
        marker = directory / "group.json"
        heartbeat = directory / "heartbeat"
        libc = ctypes.CDLL(None, use_errno=True)
        original = ctypes.c_int()
        self.assertEqual(libc.prctl(37, ctypes.byref(original), 0, 0, 0), 0)
        self.assertEqual(libc.prctl(36, 1, 0, 0, 0), 0)
        owner = None
        guardian = None
        group = None

        def handler(_context, _request):
            descendant = os.fork()
            if descendant == 0:
                while True:
                    heartbeat.write_text(str(time.monotonic()))
                    time.sleep(0.01)
            marker.write_text(json.dumps({"worker": os.getpid(), "descendant": descendant,
                                          "guardian": os.getppid()}))
            while True:
                time.sleep(0.01)

        try:
            owner = os.fork()
            if owner == 0:
                try:
                    lock = RunLock(directory)
                    context = ParentContext()
                    context.ownership_descriptors = (lock.fileno(),)
                    self.run_adapter(handler, timeout=10, context=context)
                finally:
                    os._exit(0)
            until = time.monotonic() + 3
            while time.monotonic() < until:
                try:
                    group = json.loads(marker.read_text())
                    if heartbeat.exists():
                        break
                except (FileNotFoundError, json.JSONDecodeError):
                    pass
                time.sleep(0.01)
            self.assertIsNotNone(group)
            guardian = group["guardian"]
            self.assertEqual(os.getpgid(group["descendant"]), group["worker"])
            # Pause cleanup deliberately: ownership must remain unavailable
            # throughout this interval, not just until the original host dies.
            os.kill(guardian, signal.SIGSTOP)
            os.kill(owner, signal.SIGKILL)
            os.waitpid(owner, 0)
            owner = None
            with self.assertRaises(JournalError):
                RunLock(directory)
            os.kill(guardian, signal.SIGCONT)
            until = time.monotonic() + 3
            while time.monotonic() < until:
                pid, _status = os.waitpid(guardian, os.WNOHANG)
                if pid:
                    guardian = None
                    break
                time.sleep(0.01)
            self.assertIsNone(guardian, "guardian did not finish cleanup")
            for pid in (group["worker"], group["descendant"]):
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
            with RunLock(directory):
                pass
        finally:
            if owner is not None:
                os.kill(owner, signal.SIGKILL)
                os.waitpid(owner, 0)
            if guardian is not None:
                os.kill(guardian, signal.SIGCONT)
                os.kill(guardian, signal.SIGTERM)
                os.waitpid(guardian, 0)
            if group:
                try:
                    os.killpg(group["worker"], signal.SIGKILL)
                except ProcessLookupError:
                    pass
            libc.prctl(36, original.value, 0, 0, 0)

    def test_guardian_stops_group_while_host_reconciliation_is_blocked(self):
        path = Path(self.temp.name) / "pid"
        observed = []

        def tick():
            if not path.exists():
                return
            time.sleep(0.2)
            try:
                os.kill(int(path.read_text()), 0)
                observed.append("alive")
            except ProcessLookupError:
                observed.append("stopped")

        with self.assertRaises(AdapterTimeout):
            self.run_adapter(self.record_and_hang(path), timeout=0.1, tick=tick)
        self.assertEqual(observed, ["stopped"])

    def test_adapter_context_contract_is_shared_by_both_execution_strategies(self):
        from mas_host.runner import Context
        direct = Context(SimpleNamespace(_deadline=time.monotonic() + 3), "b", "message")
        self.assertIsInstance(direct, AdapterContext)

        def adapter(context, _request):
            return Outcome({"contract": isinstance(context, AdapterContext),
                            "actor": context.actor})

        inline = InlineExecutor().execute(FunctionAdapter(adapter), direct, REQUEST,
                                          deadline=time.monotonic() + 2)
        isolated = self.run_adapter(adapter)
        self.assertEqual(inline, isolated)
        self.assertEqual(isolated.output, {"contract": True, "actor": "b"})

    def test_recoverable_delegation_errors_are_catchable_in_both_strategies(self):
        for error_type in (AdapterError, RunnerError):
            with self.subTest(error=error_type.__name__):
                def reject(*_args):
                    raise error_type("secret-provider-detail")

                def adapter(context, _request):
                    try:
                        context.request("c", "check", {})
                    except error_type as error:
                        return Outcome({"caught": type(error).__name__})
                    self.fail("delegated error was not raised")

                for executor in (InlineExecutor(), LinuxProcessExecutor()):
                    result = executor.execute(FunctionAdapter(adapter), ParentContext(request=reject),
                                              REQUEST, deadline=time.monotonic() + 2)
                    self.assertEqual(result.output, {"caught": error_type.__name__})

    def test_recoverable_parent_error_text_never_crosses_ipc(self):
        def reject(*_args):
            raise AdapterError("secret-provider-detail")

        def adapter(context, _request):
            try:
                context.request("c", "check", {})
            except AdapterError as error:
                return Outcome({"detail": str(error)})

        result = self.run_adapter(adapter, context=ParentContext(request=reject))
        self.assertNotIn("secret-provider-detail", result.output["detail"])

    def test_uncertain_delegation_cannot_be_caught_as_a_recoverable_failure(self):
        def unknown(*_args):
            raise RecoveryRequired("uncertain effect")

        def adapter(context, _request):
            try:
                context.request("c", "check", {})
            except AdapterError:
                return Outcome({"should_not_complete": True})

        with self.assertRaises(RecoveryRequired):
            self.run_adapter(adapter, context=ParentContext(request=unknown))

    def test_cancellation_during_delegation_aborts_instead_of_returning_a_catchable_error(self):
        effect = Path(self.temp.name) / "continued-after-shutdown"

        def interrupted(*_args):
            raise AdapterInterrupted("host shutdown")

        def adapter(context, _request):
            try:
                context.request("c", "check", {})
            except AdapterError:
                effect.write_text("must not continue")
            return Outcome({})

        with self.assertRaises(AdapterInterrupted):
            self.run_adapter(adapter, context=ParentContext(request=interrupted))
        self.assertFalse(effect.exists())

    def test_invalid_and_oversize_provider_outputs_preserve_unknown(self):
        for output in ({"floating": 0.5}, {"large": "x" * 65_536}):
            context = ParentContext()
            with self.assertRaises(AdapterError):
                self.run_adapter(lambda ctx, _request:
                                 Outcome(ctx.invoke(lambda: Invocation(output, {}))), context=context)
            self.assertEqual([event[0] for event in context.events], ["prepare", "unknown"])

    def test_ipc_parser_rejects_duplicates_floats_and_extra_commands(self):
        for source in (b'{"op":"a","op":"b"}', b'{"n":0.2}', b'{"n":NaN}', b'[]'):
            with self.assertRaises(AdapterError):
                _decode(source)

        def adapter(context, _request):
            context._channel.send({"op": "request", "recipient": "c", "operation": "x",
                                   "input": {}, "actor": "someone-else"})
            return Outcome({})

        with self.assertRaises(AdapterError):
            self.run_adapter(adapter)
        self.assertEqual(self.context.events, [])

    def test_exception_text_is_not_transported(self):
        def adapter(_context, _request):
            raise ValueError("secret-provider-key")

        with self.assertRaises(AdapterError) as caught:
            self.run_adapter(adapter)
        self.assertNotIn("secret-provider-key", str(caught.exception))

    def test_expired_deadline_never_dispatches_handler(self):
        path = Path(self.temp.name) / "effect"
        with self.assertRaises(AdapterTimeout):
            execute(FunctionAdapter(lambda *_args: path.write_text("effect")), self.context,
                    REQUEST, deadline=time.monotonic() - 1)
        self.assertFalse(path.exists())


if __name__ == "__main__":
    unittest.main()

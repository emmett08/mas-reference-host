"""Barrier workloads proving real assignment overlap without timing inference."""
import json
import os
from pathlib import Path
import time

from mas_host import Invocation, Outcome
from mas_host.task_service import Workflow


def _gate_shutdown_observation():
    """Put only the active assignment inside telemetry when shutdown arrives."""
    gate_path = os.environ.get("MAS_TEST_TELEMETRY_GATE")
    if not gate_path:
        return
    from mas_host import telemetry
    if getattr(telemetry._open, "_dispatcher_shutdown_gate", False):
        return
    gate = json.loads(Path(gate_path).read_text())
    original = telemetry._open
    entered = False

    def gated_open(*args, **kwargs):
        nonlocal entered
        if not entered:
            try:
                owner = json.loads(Path(gate["started"]).read_text())["service"]
            except (FileNotFoundError, ValueError):
                owner = None
            if owner == os.getpid():
                # The adapter has started, so the assignment owner's real
                # supervisor has installed its interruption handler. Keep this
                # one observation open until the dispatcher sends SIGTERM.
                entered = True
                Path(gate["entered"]).touch()
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    time.sleep(0.01)
        return original(*args, **kwargs)

    gated_open._dispatcher_shutdown_gate = True
    telemetry._open = gated_open


class Barrier:
    def __init__(self):
        # Capture the assignment owner before the adapter guardian forks. Avoid
        # reading /proc numeric paths across nested PID namespace mappings.
        self.service_pid = os.getpid()

    def handle(self, context, request):
        def call():
            guardian = os.getppid()
            Path(request.input["started"]).write_text(json.dumps(
                {"worker": os.getpid(), "guardian": guardian, "service": self.service_pid}))
            limit = time.monotonic() + 15
            while not Path(request.input["release"]).exists() and time.monotonic() < limit:
                time.sleep(0.01)
            return Invocation({"value": request.input["value"]}, {"provider": "barrier-fixture"})
        return Outcome(context.invoke(call, input=request.input, reservation_microusd=0))


def workflows():
    _gate_shutdown_observation()
    topology = {"agents": ["client", "worker"], "edges": [["client", "worker"], ["worker", "client"]],
                "application": "agent-task/1", "mailbox_capacity": 10,
                "max_messages": 20, "max_bytes": 100_000, "max_calls": 4, "wall_seconds": 20}
    return {"barrier": Workflow("barrier/1", topology,
        {"client": Barrier(), "worker": Barrier()}, "client", "worker", "barrier")}

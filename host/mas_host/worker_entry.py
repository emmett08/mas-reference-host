"""Bounded entry protocol for explicitly registered executable workers.

Custom executables call run_worker with an application-owned dictionary of
factories. Neither import names nor executable paths come from task input.
"""
from __future__ import annotations
import os
import socket
import time

from .contracts import AdapterError, Outcome, TaskRequest
from .supervision import _Channel, _ContextProxy, _fields


def run_worker(factories) -> int:
    channel = None
    try:
        descriptor = int(os.environ.pop("MAS_WORKER_CHANNEL_FD"))
        if descriptor <= 2:
            return 2
        sock = socket.socket(fileno=descriptor)
        channel = _Channel(sock, time.monotonic() + 10)
        frame = channel.receive()
        _fields(frame, {"version", "factory", "actor", "configuration", "request", "remaining_ms"})
        if (frame["version"] != "mas-worker/1" or type(frame["factory"]) is not str
                or frame["factory"] not in factories or type(frame["configuration"]) is not dict
                or type(frame["remaining_ms"]) is not int or not 1 <= frame["remaining_ms"] <= 86_400_000):
            raise AdapterError("invalid executable worker request")
        request = frame["request"]
        _fields(request, {"sender", "recipient", "operation", "input", "task_id", "message_id"})
        if (type(frame["actor"]) is not str or frame["actor"] != request["recipient"]
                or type(request["input"]) is not dict
                or any(type(value) is not str for key, value in request.items() if key != "input")):
            raise AdapterError("invalid executable worker identity")
        channel.deadline = time.monotonic() + frame["remaining_ms"] / 1000
        adapter = factories[frame["factory"]](frame["configuration"])
        result = adapter.handle(_ContextProxy(channel, frame["actor"]), TaskRequest(**request))
        if not isinstance(result, Outcome) or type(result.output) is not dict or result.status not in ("completed", "unresolved"):
            raise AdapterError("invalid executable worker result")
        channel.send({"op": "result", "output": result.output, "status": result.status})
        return 0
    except BaseException:
        if channel is not None:
            try:
                channel.send({"op": "error"})
            except BaseException:
                pass
        return 2
    finally:
        if channel is not None:
            channel.sock.close()


class _Echo:
    def __init__(self, configuration):
        if configuration:
            raise AdapterError("echo worker has no configuration")

    def handle(self, _context, request):
        return Outcome(request.input)


def _structured(configuration):
    from .agent_config import load_agent_configs, create_agent_adapters
    from .operation_contracts import OperationContract
    _fields(configuration, {"actor", "agent", "contract", "mock_output"})
    definition = configuration["contract"]
    _fields(definition, {"name", "version", "input_schema", "output_schema", "allowed_callers"})
    contract = OperationContract(**definition)
    actor = configuration["actor"]
    configs = load_agent_configs({actor: configuration["agent"]}, contracts={contract.operation: contract})
    fixtures = {} if configuration["mock_output"] is None else {actor: configuration["mock_output"]}
    return create_agent_adapters(configs, registered_actors=[actor], mock_outputs=fixtures)[actor]


def main():
    return run_worker({"echo": _Echo, "structured": _structured})


if __name__ == "__main__":
    raise SystemExit(main())

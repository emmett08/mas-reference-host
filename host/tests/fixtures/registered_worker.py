"""Trusted, explicitly registered executables for supervisor failure traces."""
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time

from mas_host import Invocation, Outcome
from mas_host.worker_entry import run_worker


class Fixture:
    def __init__(self, configuration):
        self.configuration = configuration

    def handle(self, context, request):
        mode = self.configuration["mode"]
        if mode in {"not_sent", "completed_invalid", "completed_valid"}:
            from mas_host.provider_outcomes import completed_unusable, not_sent
            def fail():
                if self.configuration.get("record_file"):
                    with Path(self.configuration["record_file"]).open("a") as stream:
                        stream.write("attempt\n")
                if mode == "not_sent":
                    raise not_sent("custom", "operation_busy")
                if mode == "completed_valid":
                    return Invocation({"answer": "PRIVATE completed output"}, {"response_id": "fixture-completed",
                                                                             "usage": {"total_tokens": 7}})
                raise completed_unusable("custom", "invalid_output", {"response_id": "fixture-completed",
                                                                       "usage": {"total_tokens": 7}})
            return Outcome(context.invoke(fail, input=request.input, reservation_microusd=0))
        if mode == "invoke":
            return Outcome(context.invoke(lambda: Invocation(request.input,
                {"worker": "registered-fixture", "call_id": context.invocation_id}),
                input=request.input, reservation_microusd=0))
        if mode == "delegate":
            return context.request(self.configuration["recipient"], "echo", request.input)
        if mode == "inspect":
            descriptors = {}
            for descriptor in os.listdir("/proc/self/fd"):
                try:
                    descriptors[descriptor] = os.readlink("/proc/self/fd/" + descriptor)
                except OSError:
                    pass
            return Outcome({"environment": sorted(os.environ), "descriptors": descriptors,
                "memory": resource.getrlimit(resource.RLIMIT_AS)[0],
                "cpu": resource.getrlimit(resource.RLIMIT_CPU)[0],
                "file": resource.getrlimit(resource.RLIMIT_FSIZE)[0]})
        if mode in ("spawn", "stall"):
            def call():
                child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"],
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                Path(self.configuration["record_file"]).write_text(json.dumps({"worker": os.getpid(),
                    "guardian": os.getppid(), "child": child.pid}))
                if mode == "stall":
                    time.sleep(120)
                return Invocation({"spawned": True}, {"provider": "fixture"})
            return Outcome(context.invoke(call, reservation_microusd=0))
        if mode == "filelimit":
            def write():
                Path(self.configuration["output_file"]).write_bytes(b"x" * 8192)
                return Invocation({"written": True}, {})
            return Outcome(context.invoke(write, reservation_microusd=0))
        raise ValueError("unknown fixture mode")


if __name__ == "__main__":
    raise SystemExit(run_worker({"fixture": Fixture}))

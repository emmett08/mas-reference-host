#!/usr/bin/env python3
"""Bounded local status-path comparison; reports measurements, asserts no speedup.

Run after make build.
The workload uses no provider, network, persistent evidence or credentials.
"""
import argparse
import hashlib
import json
from pathlib import Path
import resource
import statistics
import sys
import tempfile
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "host"))
from mas_host import Outcome, Runner
from mas_host.protocol_client import PersistentProtocolClient, ProtocolClient
import subprocess


class Echo:
    def handle(self, _context, request):
        return Outcome(request.input)


def measure(strategy, samples):
    # Include construction and teardown in the CPU comparison. In particular,
    # RUSAGE_CHILDREN cannot see the persistent gateway until it has been reaped.
    # Status latency remains a separate, steady-state observation below.
    lifecycle_started = time.perf_counter_ns()
    lifecycle_self = resource.getrusage(resource.RUSAGE_SELF)
    lifecycle_children = resource.getrusage(resource.RUSAGE_CHILDREN)
    binaries = ROOT / "rust-protocol" / "target" / "release"
    client = strategy(str(binaries / "mas"), str(binaries / "mas-gateway"))
    spec = {"agents": ["a", "b"], "edges": [["a", "b"], ["b", "a"]],
            "application": "agent-task/1", "mailbox_capacity": 10,
            "max_messages": 100, "max_bytes": 100_000, "max_calls": 10, "wall_seconds": 120}
    with tempfile.TemporaryDirectory() as directory:
        try:
            with Runner(spec, {"a": Echo(), "b": Echo()}, Path(directory),
                        protocol_client=client, supervised=False) as runner:
                runner.request("a", "b", "echo", {"value": 1})
                launches = []
                original = subprocess.Popen

                def start(*args, **kwargs):
                    launches.append(True)
                    return original(*args, **kwargs)

                cpu_before = resource.getrusage(resource.RUSAGE_SELF)
                elapsed = []
                with patch("mas_host.protocol_client.subprocess.Popen", side_effect=start):
                    for _ in range(samples):
                        started = time.perf_counter_ns()
                        snapshot = runner.status()
                        elapsed.append((time.perf_counter_ns() - started) / 1e6)
                cpu_after = resource.getrusage(resource.RUSAGE_SELF)
                result = {"strategy": strategy.__name__, "samples": samples,
                        "status_process_launches": len(launches),
                        "median_ms": statistics.median(elapsed),
                        "p95_ms": sorted(elapsed)[int((samples - 1) * 0.95)],
                        "total_ms": sum(elapsed), "response_bytes": len(json.dumps(snapshot)),
                        "status_host_cpu_seconds": (cpu_after.ru_utime + cpu_after.ru_stime
                                             - cpu_before.ru_utime - cpu_before.ru_stime),
                        "latency_scope": "status calls after the same completed two-actor exchange"}
        finally:
            if hasattr(client, "close"):
                client.close()
    # Both strategies have now released their runner and reaped all children.
    # This is comparable whole-lifecycle CPU, not a claim about status-only CPU.
    final_self = resource.getrusage(resource.RUSAGE_SELF)
    final_children = resource.getrusage(resource.RUSAGE_CHILDREN)
    own = (final_self.ru_utime + final_self.ru_stime
           - lifecycle_self.ru_utime - lifecycle_self.ru_stime)
    children = (final_children.ru_utime + final_children.ru_stime
                - lifecycle_children.ru_utime - lifecycle_children.ru_stime)
    result["lifecycle"] = {"host_cpu_seconds": max(0.0, own),
        "child_cpu_seconds": max(0.0, children),
        "total_cpu_seconds": max(0.0, own + children),
        "wall_ms": (time.perf_counter_ns() - lifecycle_started) / 1e6,
        "scope": "construction, one exchange, status queries, close and child reaping",
        "children_reaped": True}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=100)
    args = parser.parse_args()
    if not 5 <= args.samples <= 1000:
        parser.error("samples must be from 5 to 1000")
    binaries = ROOT / "rust-protocol" / "target" / "release"
    identities = {name: hashlib.sha256((binaries / name).read_bytes()).hexdigest()
                  for name in ("mas", "mas-gateway")}
    print(json.dumps({"format": "mas-ipc-benchmark/2", "qualification": False,
        "binary_sha256": identities,
        "workload": "completed-run status; one two-actor echo exchange",
        "limitations": ["single bounded fixture; no provider throughput or production SLO claim",
                        "CPU includes setup/teardown; status latency does not"],
        "results": [measure(strategy, args.samples) for strategy in (ProtocolClient, PersistentProtocolClient)]}, indent=2))


if __name__ == "__main__":
    main()

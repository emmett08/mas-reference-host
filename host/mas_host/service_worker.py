"""Bounded main-process dispatcher for independently owned service workers.

Only the dispatcher writes readiness. Each child loads trusted deployment
configuration and executes one durable assignment; normal service/run/capacity
locks remain authoritative. A slot retained by an orphaned child prevents a
replacement dispatcher from exceeding the deployment's process bound.
"""
from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from .codec import canonical_json
from .service_contracts import ServiceError


MAX_RESULT_BYTES = 2048


def configuration_identity(configuration):
    return hashlib.sha256(canonical_json(configuration).encode()).hexdigest()


@dataclass
class _Child:
    process: subprocess.Popen
    task_id: str
    slot: int
    response: int


class ServiceDispatcher:
    def __init__(self, service, config_path: Path, config: dict, *, processes=None,
                 interval=0.25, drain_seconds=30):
        maximum = service.capacity.policy.max_workers
        processes = maximum if processes is None else processes
        if (type(processes) is not int or not 1 <= processes <= maximum
                or type(interval) not in (int, float) or not math.isfinite(interval) or not 0.01 <= interval <= 30
                or type(drain_seconds) not in (int, float) or not math.isfinite(drain_seconds)
                or not 0 <= drain_seconds <= 3600):
            raise ServiceError("invalid_dispatcher_policy")
        self.service = service
        self.path = Path(config_path).absolute()
        self.config_identity = configuration_identity(config)
        self.maximum, self.processes = maximum, processes
        self.interval, self.drain_seconds = interval, drain_seconds
        self.children = {}
        self.deferred = {}
        if hasattr(service, "authority"):
            from .shared_control import SharedWorkerControl
            self.control = SharedWorkerControl(service.authority)
        else:
            from .worker_control import WorkerControl
            self.control = WorkerControl(service.operations)

    def _slot(self):
        # The monitor lock permits one active dispatcher. Scan the entire frozen
        # deployment bound so earlier orphaned children count after restart or
        # when this operator selects a smaller process limit.
        selected, busy = None, 0
        try:
            for index in range(self.maximum):
                fd = os.open(self.service.state_dir / f".dispatcher-slot-{index}.lock",
                             os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    busy += 1
                    os.close(fd)
                    continue
                if selected is None:
                    selected = fd
                else:
                    os.close(fd)
            if busy >= self.processes:
                if selected is not None:
                    os.close(selected)
                return None
            return selected
        except BaseException:
            if selected is not None:
                os.close(selected)
            raise

    def _launch(self, task_id):
        with self.control.admission() as allowed:
            if not allowed:
                return False
            return self._launch_admitted(task_id)

    def _launch_admitted(self, task_id):
        slot = self._slot()
        if slot is None:
            return False
        read_end, write_end = os.pipe()
        try:
            command = [sys.executable, "-m", "mas_host.service_worker", "--config", str(self.path),
                       "--state-dir", str(self.service.state_dir), "--task-id", task_id,
                       "--configuration-identity", self.config_identity,
                       "--slot-fd", str(slot), "--result-fd", str(write_end)]
            process = subprocess.Popen(command, stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                pass_fds=(slot, write_end), close_fds=True, start_new_session=True)
            self.children[task_id] = _Child(process, task_id, slot, read_end)
        except BaseException:
            os.close(slot)
            os.close(read_end)
            raise
        finally:
            os.close(write_end)
        return True

    def _reap(self):
        completed = []
        for task_id, child in list(self.children.items()):
            if child.process.poll() is None:
                continue
            try:
                os.set_blocking(child.response, False)
                body = os.read(child.response, MAX_RESULT_BYTES + 1)
                if len(body) > MAX_RESULT_BYTES:
                    raise ValueError
                result = json.loads(body)
                if (type(result) is not dict or set(result) != {"task_id", "state", "error"}
                        or result["task_id"] != task_id
                        or result["state"] is not None and type(result["state"]) is not str
                        or result["error"] is not None and type(result["error"]) is not str):
                    raise ValueError
            except (OSError, ValueError, TypeError):
                result = {"task_id": task_id, "state": None, "error": "worker_interrupted"}
            finally:
                os.close(child.response)
                # Closing without LOCK_UN leaves any inherited descriptions
                # authoritative until their process exits.
                os.close(child.slot)
                del self.children[task_id]
            completed.append(result)
            if result["state"] in (None, "blocked", "queued", "running") or result["error"]:
                self.deferred[task_id] = time.monotonic() + max(1, self.interval)
        return completed

    def run(self, monitor, *, stopping, once=False, on_result=None):
        """Poll durable scheduling intents; drain on shutdown within a bound."""
        stop_at = None
        dispatched = False
        terminated = False
        try:
            while True:
                now = time.monotonic()
                for result in self._reap():
                    if on_result is not None:
                        on_result(result)
                monitor.polls += 1
                stopping_now = stopping()
                if stopping_now and stop_at is None:
                    stop_at = now + self.drain_seconds
                if stop_at is not None and now >= stop_at and not terminated:
                    for child in self.children.values():
                        if child.process.poll() is None:
                            child.process.terminate()
                    terminated = True
                    stop_at = now + 5
                if terminated and now >= stop_at:
                    for child in self.children.values():
                        if child.process.poll() is None:
                            child.process.kill()
                    stop_at = now + 5
                draining = self.control.status()['draining']
                if not stopping_now and not draining and not (once and dispatched):
                    monitor.write("polling")
                    rows = self.service.pending()
                    pending_ids = {row["task_id"] for row in rows}
                    self.deferred = {key: until for key, until in self.deferred.items()
                                     if key in pending_ids and until > now}
                    # Capacity's durable principal rotation chooses the first
                    # owner. Interleave their FIFO queues inside this batch so
                    # one principal cannot fill every orchestration slot before
                    # another eligible principal gets an assignment process.
                    queues = {}
                    for row in rows:
                        queues.setdefault(row['principal'], deque()).append(row)
                    interleaved = []
                    while any(queues.values()):
                        for queue in queues.values():
                            if queue:
                                interleaved.append(queue.popleft())
                    for row in interleaved:
                        task_id = row["task_id"]
                        if task_id in self.children or task_id in self.deferred:
                            continue
                        if len(self.children) >= self.processes or not self._launch(task_id):
                            break
                        dispatched = True
                        if once:
                            break
                monitor.write("draining" if draining else "executing" if self.children else "idle",
                              allowance=max(30, self.interval + 5))
                if (stopping_now or once) and not self.children:
                    return
                time.sleep(self.interval)
        finally:
            # Exceptions cannot orphan new scheduling work. Existing worker
            # guardians retain run/capacity locks during their own cleanup.
            for child in self.children.values():
                if child.process.poll() is None:
                    child.process.terminate()
            for child in list(self.children.values()):
                try:
                    child.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.process.kill()
                    child.process.wait(timeout=5)
            self._reap()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Private trusted assignment worker")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--configuration-identity", required=True)
    parser.add_argument("--slot-fd", type=int, required=True)
    parser.add_argument("--result-fd", type=int, required=True)
    args = parser.parse_args(argv)
    result = {"task_id": args.task_id, "state": None, "error": None}
    try:
        for fd in (args.slot_fd, args.result_fd):
            if fd <= 2:
                raise ServiceError("invalid_worker_descriptor")
            os.fstat(fd)
            os.set_inheritable(fd, False)
        if len(args.task_id) > 256:
            raise ServiceError("invalid_task_id")
        from .bridge_config import build_service, load_config
        configuration = load_config(args.config)
        if configuration_identity(configuration) != args.configuration_identity:
            raise ServiceError("worker_configuration_changed")
        service = build_service(configuration, args.state_dir)
        current = service.run_once(args.task_id)
        if current is not None:
            result["state"] = current["state"]
    except Exception as error:
        result["error"] = error.code if isinstance(error, ServiceError) else "worker_failed"
    finally:
        try:
            source = json.dumps(result, separators=(",", ":")).encode()
            if len(source) <= MAX_RESULT_BYTES:
                os.write(args.result_fd, source)
        finally:
            os.close(args.result_fd)
            os.close(args.slot_fd)
    return 0 if result["error"] is None else 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Bounded subprocess transport for the authoritative Rust protocol tools."""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import select
import shutil
import subprocess
import threading
import time
from typing import Protocol

from .contracts import RunnerError


class ProtocolTransport(Protocol):
    """Small transport port; Rust command semantics remain authoritative."""

    mas: str
    gateway: str

    def command(self, args: list[str], source: str | None = None, *,
                timeout: float = 10, empty_response: bool = False): ...

    def validate_gateway(self, database, configuration, *, timeout: float = 10) -> None: ...


class ProtocolClient:
    """Own command execution, credential-free environments and safe diagnostics."""

    def __init__(self, mas_bin: str = "mas", gateway_bin: str = "mas-gateway"):
        self.mas = shutil.which(str(mas_bin))
        self.gateway = shutil.which(str(gateway_bin))
        if not self.mas or not self.gateway:
            raise RunnerError("mas and mas-gateway binaries must be installed")

    def command(self, args: list[str], source: str | None = None, *,
                timeout: float = 10, empty_response: bool = False):
        try:
            result = subprocess.run(args, input=source, encoding="utf-8",
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=timeout, check=False,
                                    env={"PATH": os.environ.get("PATH", os.defpath)})
            if empty_response:
                if result.returncode or result.stdout:
                    raise RunnerError("protocol operation rejected; inspect run status")
                return None
            response = json.loads(result.stdout)
            if result.returncode or response.get("ok") is not True:
                raise RunnerError("protocol operation rejected; inspect run status")
            return response["result"]
        except (OSError, subprocess.TimeoutExpired, UnicodeError, ValueError, KeyError, AttributeError):
            # Never attach subprocess output, provider credentials or request bodies.
            raise RunnerError("protocol command unavailable or interrupted") from None

    def validate_gateway(self, database, configuration, *, timeout: float = 10) -> None:
        # Opening an empty broker session validates/fixes its policy fingerprint,
        # without consuming a request attempt or authorising any external effect.
        self.command([self.gateway, "serve", str(database), str(configuration)], "",
                     timeout=timeout, empty_response=True)


class PersistentProtocolClient(ProtocolClient):
    """One trusted Rust child per run; bounded synchronous IPC with no replay.

    Bootstrap commands retain the command transport until ``validate_gateway``
    binds one immutable run. Thereafter status, codec and capability operations
    reuse the child. A lost reply invalidates that session and is surfaced to
    the caller; only the caller's operation-specific recovery may retry it.
    No helper threads are created, so Linux adapter supervision can still fork.
    """

    VERSION = 1
    MAX_HEADER_BYTES = 1024
    MAX_REQUEST_BYTES = 65_792
    MAX_RESPONSE_BYTES = 16 * 1024 * 1024

    def __init__(self, mas_bin: str = "mas", gateway_bin: str = "mas-gateway"):
        super().__init__(mas_bin, gateway_bin)
        self._owner_pid = os.getpid()
        self._binding = None
        self._process = None
        self._sequence = 0
        self._buffer = bytearray()
        self._lock = threading.Lock()
        self._closed = False

    def _assert_owner(self):
        if os.getpid() != self._owner_pid or self._closed:
            raise RunnerError("protocol transport is closed or belongs to another process")

    @staticmethod
    def _remaining(deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError
        return remaining

    def _write(self, data, deadline):
        offset = 0
        descriptor = self._process.stdin.fileno()
        while offset < len(data):
            if not select.select([], [descriptor], [], self._remaining(deadline))[1]:
                raise TimeoutError
            try:
                count = os.write(descriptor, data[offset:])
            except BlockingIOError:
                continue
            if not count:
                raise OSError
            offset += count

    def _read(self, count, deadline):
        while len(self._buffer) < count:
            self._receive(deadline, min(65_536, count - len(self._buffer)))
        result = bytes(self._buffer[:count])
        del self._buffer[:count]
        return result

    def _receive(self, deadline, limit):
        descriptor = self._process.stdout.fileno()
        if not select.select([descriptor], [], [], self._remaining(deadline))[0]:
            raise TimeoutError
        try:
            chunk = os.read(descriptor, limit)
        except BlockingIOError:
            return
        if not chunk:
            raise OSError
        self._buffer.extend(chunk)

    @staticmethod
    def _json(source):
        def unique_pairs(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError
                result[key] = value
            return result
        return json.loads(source, object_pairs_hook=unique_pairs,
                          parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()))

    def _header(self, deadline):
        while b"\n" not in self._buffer:
            if len(self._buffer) >= self.MAX_HEADER_BYTES:
                raise ValueError
            self._receive(deadline, self.MAX_HEADER_BYTES - len(self._buffer))
        index = self._buffer.index(b"\n")
        if index + 1 > self.MAX_HEADER_BYTES:
            raise ValueError
        source = bytes(self._buffer[:index])
        del self._buffer[:index + 1]
        header = self._json(source)
        if type(header) is not dict:
            raise ValueError
        return header

    def _start(self, deadline):
        if self._process is not None:
            if self._process.poll() is None:
                return
            self._terminate()
        self._process = subprocess.Popen([self.gateway, "host", *self._binding],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env={"PATH": os.environ.get("PATH", os.defpath)}, close_fds=True, bufsize=0)
        os.set_blocking(self._process.stdin.fileno(), False)
        os.set_blocking(self._process.stdout.fileno(), False)
        hello = self._header(deadline)
        expected = {"version": self.VERSION, "max_header_bytes": self.MAX_HEADER_BYTES,
                    "max_request_bytes": self.MAX_REQUEST_BYTES,
                    "max_response_bytes": self.MAX_RESPONSE_BYTES}
        if hello != expected or any(type(value) is not int for value in hello.values()):
            raise ValueError
        self._sequence = 0

    def _terminate(self):
        self._assert_process_owner()
        process, self._process = self._process, None
        self._buffer.clear()
        if process is None:
            return
        if process.poll() is None:
            process.kill()
        process.wait()
        for stream in (process.stdin, process.stdout):
            if stream is not None:
                stream.close()

    def _exchange(self, operation, source, deadline):
        self._start(deadline)
        body = source.encode("utf-8")
        if len(body) > self.MAX_REQUEST_BYTES:
            raise ValueError
        self._sequence += 1
        request = {"version": self.VERSION, "request_id": self._sequence,
                   "operation": operation, "body_bytes": len(body),
                   "deadline_ms": int((time.time() + self._remaining(deadline)) * 1000)}
        framing = json.dumps(request, separators=(",", ":")).encode("ascii") + b"\n"
        self._write(framing + body, deadline)
        header = self._header(deadline)
        if (set(header) != {"version", "request_id", "body_bytes", "close"}
                or type(header["version"]) is not int or header["version"] != self.VERSION
                or type(header["request_id"]) is not int or header["request_id"] != self._sequence
                or type(header["body_bytes"]) is not int
                or not 0 <= header["body_bytes"] <= self.MAX_RESPONSE_BYTES
                or type(header["close"]) is not bool):
            raise ValueError
        response = self._json(self._read(header["body_bytes"], deadline))
        if type(response) is not dict or type(response.get("ok")) is not bool:
            raise ValueError
        if header["close"]:
            self._terminate()
        if response["ok"] is not True:
            raise RunnerError("protocol operation rejected; inspect run status")
        return response["result"]

    def _bounded(self, operation, source, timeout):
        self._assert_owner()
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 120:
            raise RunnerError("protocol timeout must be positive and at most 120 seconds")
        deadline = time.monotonic() + timeout
        if not self._lock.acquire(timeout=timeout):
            raise RunnerError("protocol transport is busy")
        try:
            self._assert_owner()
            return self._exchange(operation, source, deadline)
        except (OSError, TimeoutError, UnicodeError, ValueError, KeyError, AttributeError):
            self._terminate()
            raise RunnerError("protocol command unavailable or interrupted") from None
        except BaseException:
            # Interrupts can arrive after dispatch. Discard unread correlation
            # state without manufacturing a successful or retryable outcome.
            self._terminate()
            raise
        finally:
            self._lock.release()

    def validate_gateway(self, database, configuration, *, timeout: float = 10) -> None:
        self._assert_owner()
        try:
            with Path(configuration).open("rb") as stream:
                config = self._json(stream.read(65_537))
            runs = {item["run_id"] for item in config["credentials"]}
            if len(runs) != 1:
                raise ValueError
            binding = (str(database), str(configuration), runs.pop())
            if self._binding is not None and self._binding != binding:
                raise ValueError
        except (OSError, UnicodeError, ValueError, TypeError, KeyError):
            raise RunnerError("persistent protocol transport requires one immutable run") from None
        self._binding = binding
        # A status request confirms handshake and the run, consuming no agent
        # admission. Rust alone validates credential policy and file permissions.
        self._bounded("status", "", timeout)

    def command(self, args: list[str], source: str | None = None, *,
                timeout: float = 10, empty_response: bool = False):
        self._assert_owner()
        if self._binding is not None and not empty_response:
            database, configuration, run_id = self._binding
            if args == [self.gateway, "serve", database, configuration] and source is not None:
                return self._bounded("gateway", source.removesuffix("\n"), timeout)
            if args == [self.mas, "encode", "-"] and source is not None:
                return self._bounded("encode", source, timeout)
            for operation in ("status", "reconcile"):
                if args == [self.mas, operation, database, run_id] and source is None:
                    return self._bounded(operation, "", timeout)
        return super().command(args, source, timeout=timeout, empty_response=empty_response)

    def close(self):
        self._assert_process_owner()
        with self._lock:
            if self._closed:
                return
            try:
                if self._process is not None:
                    try:
                        self._exchange("close", "", time.monotonic() + 1)
                    except (RunnerError, OSError, TimeoutError, UnicodeError, ValueError, KeyError, AttributeError):
                        pass
            finally:
                self._terminate()
                self._closed = True

    def _assert_process_owner(self):
        if os.getpid() != self._owner_pid:
            raise RunnerError("protocol transport belongs to another process")

    def __enter__(self):
        self._assert_owner()
        return self

    def __exit__(self, *_exc):
        self.close()

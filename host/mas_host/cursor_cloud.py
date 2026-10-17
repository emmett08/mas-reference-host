"""Small, non-retrying client for Cursor Cloud Agents REST API v1.

The cloud API is an agent/job API, never an OpenAI-compatible inference API.
Only explicit repository jobs are supported; credentials never enter job records.
"""
from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
import http.client
import json
import re
import ssl
import signal
import threading
import time
from typing import Protocol
from urllib.parse import urlsplit


class CloudError(RuntimeError):
    """Fixed diagnostics suitable for a shared operator console."""


STATUSES = frozenset({"CREATING", "RUNNING", "FINISHED", "ERROR", "CANCELLED", "EXPIRED"})
TERMINAL = frozenset({"FINISHED", "ERROR", "CANCELLED", "EXPIRED"})
MAX_RESPONSE = 1024 * 1024


@contextmanager
def _wall_timeout(seconds: int):
    """Bound the entire synchronous operation, including slow response headers.

    Socket timeouts alone only bound inactivity. The supported Linux host runs
    adapters and CLI actions on the main thread, where a POSIX interval timer
    can interrupt a blocking request without leaving a worker thread behind.
    An earlier caller timer is never postponed and is restored on exit.
    """
    if threading.current_thread() is not threading.main_thread() or not hasattr(signal, "setitimer"):
        raise CloudError("bounded cloud HTTP requires a POSIX main thread")
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_delay, previous_interval = signal.getitimer(signal.ITIMER_REAL)
    started = time.monotonic()
    def expired(_number, _frame):
        raise CloudError("Cursor Cloud request timed out; external outcome may be unknown")
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, min(seconds, previous_delay) if previous_delay else seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_delay:
            signal.setitimer(signal.ITIMER_REAL,
                             max(0.000001, previous_delay - (time.monotonic() - started)), previous_interval)


def identifier(value: str, prefix: str) -> str:
    if type(value) is not str or not re.fullmatch(prefix + r"[A-Za-z0-9_-]{1,128}", value):
        raise CloudError("invalid remote job identifier")
    return value


def _text(value, maximum: int, field: str) -> str:
    try:
        valid = type(value) is str and 0 < len(value.encode("utf-8")) <= maximum and "\x00" not in value
    except UnicodeError:
        valid = False
    if not valid:
        raise CloudError("invalid cloud job " + field)
    return value


@dataclass(frozen=True)
class CloudJobSpec:
    repository: str
    ref: str
    model: str
    prompt: str
    reservation_microusd: int = 100_000
    mode: str = "plan"
    auto_create_pr: bool = False

    def __post_init__(self):
        _text(self.repository, 2048, "repository")
        try:
            parsed = urlsplit(self.repository)
            port = parsed.port
        except ValueError:
            raise CloudError("cloud repository must be an HTTPS URL without credentials") from None
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
                or parsed.query or parsed.fragment or port not in (None, 443)
                or not parsed.path.strip("/") or any(c.isspace() for c in self.repository)):
            raise CloudError("cloud repository must be an HTTPS URL without credentials")
        _text(self.ref, 256, "ref")
        if any(c.isspace() for c in self.ref) or self.ref.startswith("-"):
            raise CloudError("invalid cloud job ref")
        if type(self.model) is not str or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}", self.model):
            raise CloudError("invalid cloud model identifier")
        _text(self.prompt, 32_768, "prompt")
        if (type(self.reservation_microusd) is not int
                or not 1 <= self.reservation_microusd <= 100_000_000):
            raise CloudError("cloud job requires a positive bounded spending reservation")
        if type(self.mode) is not str or self.mode not in ("plan", "agent") or type(self.auto_create_pr) is not bool:
            raise CloudError("invalid cloud execution settings")

    def descriptor(self) -> dict:
        return {"provider": "cursor-cloud", "api": "v1", "contract": "repository-job/1",
                "repository": self.repository, "ref": self.ref, "model": self.model,
                "mode": self.mode, "auto_create_pr": self.auto_create_pr,
                "work_on_current_branch": False, "reservation_microusd": self.reservation_microusd}

    def payload(self, agent_id: str) -> dict:
        return {"agentId": identifier(agent_id, "bc-"), "prompt": {"text": self.prompt},
                "model": {"id": self.model},
                "repos": [{"url": self.repository, "startingRef": self.ref}],
                "mode": self.mode, "autoCreatePR": self.auto_create_pr,
                "workOnCurrentBranch": False}


class CloudClient(Protocol):
    def create(self, payload: dict) -> dict: ...
    def agent(self, agent_id: str) -> dict: ...
    def run(self, agent_id: str, run_id: str) -> dict: ...
    def cancel(self, agent_id: str, run_id: str) -> dict: ...


class CursorCloudClient:
    """Bearer authentication to one fixed HTTPS origin, no redirects or retries."""
    def __init__(self, api_key: str, *, timeout_seconds: int = 30, connection_factory=None):
        if (type(api_key) is not str or not 1 <= len(api_key) <= 4096
                or any(c.isspace() or ord(c) < 33 or ord(c) > 126 for c in api_key)):
            raise CloudError("Cursor Cloud requires a valid API key")
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
            raise CloudError("cloud HTTP timeout must be 1 to 60 seconds")
        self._api_key = api_key
        self.timeout_seconds = timeout_seconds
        self._connection_factory = connection_factory or http.client.HTTPSConnection

    def _request(self, method: str, path: str, payload: dict | None = None) -> dict:
        with _wall_timeout(self.timeout_seconds):
            return self._request_bounded(method, path, payload)

    def _request_bounded(self, method: str, path: str, payload: dict | None) -> dict:
        connection = None
        deadline = time.monotonic() + self.timeout_seconds
        try:
            connection = self._connection_factory("api.cursor.com", timeout=self.timeout_seconds,
                                                  context=ssl.create_default_context())
            body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode() if payload is not None else None
            connection.request(method, "/v1" + path, body=body,
                               headers={"Authorization": "Bearer " + self._api_key,
                                        "Content-Type": "application/json", "Accept": "application/json"})
            response = connection.getresponse()
            if response.status < 200 or response.status >= 300:
                # A 404, 409 or timeout is not proof that a create had no effect.
                raise CloudError("Cursor Cloud request failed; inspect durable job state")
            chunks, size = [], 0
            while True:
                if time.monotonic() >= deadline:
                    raise CloudError("Cursor Cloud request timed out; external outcome may be unknown")
                chunk = response.read1(min(65_536, MAX_RESPONSE + 1 - size))
                size += len(chunk)
                if size > MAX_RESPONSE:
                    raise CloudError("Cursor Cloud response exceeds input limit")
                if not chunk:
                    break
                chunks.append(chunk)
            value = json.loads(b"".join(chunks), object_pairs_hook=_unique,
                               parse_constant=lambda _: _invalid())
            if type(value) is not dict:
                raise CloudError("invalid Cursor Cloud response")
            return value
        except CloudError:
            raise
        except Exception:
            raise CloudError("Cursor Cloud request failed; external outcome may be unknown") from None
        finally:
            if connection is not None:
                connection.close()

    def create(self, payload: dict) -> dict:
        return self._request("POST", "/agents", payload)

    def agent(self, agent_id: str) -> dict:
        return self._request("GET", "/agents/" + identifier(agent_id, "bc-"))

    def run(self, agent_id: str, run_id: str) -> dict:
        return self._request("GET", "/agents/" + identifier(agent_id, "bc-")
                             + "/runs/" + identifier(run_id, "run-"))

    def cancel(self, agent_id: str, run_id: str) -> dict:
        return self._request("POST", "/agents/" + identifier(agent_id, "bc-")
                             + "/runs/" + identifier(run_id, "run-") + "/cancel")


def _invalid():
    raise CloudError("invalid Cursor Cloud response")


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            _invalid()
        result[key] = value
    return result


def run_snapshot(value: dict, agent_id: str, run_id: str | None = None) -> dict:
    """Keep only bounded public identifiers and untrusted result text."""
    if type(value) is not dict or value.get("agentId") != agent_id:
        raise CloudError("Cursor run does not match the durable agent identity")
    actual_run = identifier(value.get("id"), "run-")
    if run_id is not None and actual_run != run_id:
        raise CloudError("Cursor run does not match the durable run identity")
    status = value.get("status")
    if type(status) is not str or status not in STATUSES:
        raise CloudError("unsupported Cursor run status; local state retained")
    result = {"agent_id": agent_id, "run_id": actual_run, "remote_status": status}
    if "result" in value:
        try:
            valid = type(value["result"]) is str and len(value["result"].encode("utf-8")) <= 65_536
        except UnicodeError:
            valid = False
        if not valid:
            raise CloudError("Cursor result exceeds supported text limit")
        result["result"] = value["result"]
    if "durationMs" in value:
        duration = value["durationMs"]
        if type(duration) is not int or not 0 <= duration <= 2**63 - 1:
            raise CloudError("invalid Cursor run duration")
        result["duration_ms"] = duration
    return result

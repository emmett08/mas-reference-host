"""Protocol-neutral durable task service ports.

The authenticated principal is supplied by the boundary, never by a task body.
Accepted work and a completed application result have different states.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable


class ServiceError(RuntimeError):
    """Fixed diagnostic suitable for an untrusted caller."""

    def __init__(self, code: str, message: str | None = None):
        self.code = code
        super().__init__(message or code.replace("_", " "))


@runtime_checkable
class TaskLifecycle(Protocol):
    """Submission and lifecycle operations used by orchestration connectors."""
    def submit(self, principal: str, request_key: str, workflow: str, input: dict, *, metadata: dict | None = None) -> dict: ...
    def inspect(self, principal: str, task_id: str) -> dict: ...
    def result(self, principal: str, task_id: str) -> dict: ...
    def cancel(self, principal: str, task_id: str) -> dict: ...
    def update(self, principal: str, task_id: str, request_key: str, input: dict, *, metadata: dict | None = None) -> dict: ...


@runtime_checkable
class TaskService(TaskLifecycle, Protocol):
    """Public authenticated task service; it grants no worker execution access."""
    def events(self, principal: str, task_id: str, after: int = 0) -> list[dict]: ...
    def events_page(self, principal: str, task_id: str, after: int = 0, limit: int = 100) -> dict: ...
    def list_tasks(self, principal: str, limit: int = 100, offset: int = 0) -> list[dict]: ...
    def list_task_page(self, principal: str, *, page_size: int = 100, after: tuple[int, str] | None = None, state: str | None = None, metadata: dict | None = None, updated_after: int | None = None) -> dict: ...


@runtime_checkable
class TaskWorker(Protocol):
    """Trusted local execution capability, excluded from network task clients."""
    def run_once(self) -> dict | None: ...

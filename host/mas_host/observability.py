"""Bounded operational records; request data and credentials are never logged."""
from __future__ import annotations

import json
import sys
import threading
import time

from .service_contracts import ServiceError


def validate_event_page(value, *, after: int, limit: int):
    """Reject malformed/non-advancing pages before a client loops over them."""
    if (type(value) is not dict or set(value) != {"events", "next_after", "has_more"}
            or type(value["events"]) is not list or len(value["events"]) > limit
            or type(value["next_after"]) is not int or type(value["has_more"]) is not bool):
        raise ServiceError("invalid_remote_response")
    previous = after
    for event in value["events"]:
        if (type(event) is not dict or type(event.get("id")) is not int
                or not previous < event["id"] <= 2**63 - 1):
            raise ServiceError("invalid_remote_response")
        previous = event["id"]
    if value["next_after"] != previous or value["has_more"] and not value["events"]:
        raise ServiceError("invalid_remote_response")
    return value


def event_page_arguments(after, limit):
    if type(after) is not int or not 0 <= after <= 2**63 - 1:
        raise ServiceError("invalid_cursor")
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ServiceError("invalid_pagination")


class RequestLog:
    """Write one bounded JSON object per request to a supplied text stream.

    Field selection occurs here rather than serialising arbitrary exception or
    HTTP objects. A broken logging destination cannot change task admission.
    """
    def __init__(self, stream=None):
        self.stream = stream if stream is not None else sys.stderr
        self.lock = threading.Lock()

    def __call__(self, record):
        fields = ("request_id", "operation", "status", "error_code", "duration_ms")
        value = {name: record[name] for name in fields}
        value.update(schema="mas-request-log/1", event="request_finished", timestamp_ms=int(time.time() * 1000))
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        if len(encoded) > 1024:
            return
        try:
            with self.lock:
                self.stream.write(encoded + "\n")
                self.stream.flush()
        except (OSError, ValueError):
            pass

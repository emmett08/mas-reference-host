"""Versioned retry and timing policies; authority remains with the host stores."""
from __future__ import annotations

from dataclasses import dataclass

from .contracts import RunnerError


_RETRY_CATEGORIES = frozenset({"credentials_unavailable", "runtime_unavailable",
                             "client_initialisation_failed", "invalid_request", "operation_busy"})


@dataclass(frozen=True)
class RetryPolicy:
    """Explicitly bounded retries of proved pre-dispatch failures only.

    Attempts retain separate allocations. Completed invalid output, unknown
    effects and generic HTTP errors are never eligible under this policy.
    """

    max_attempts: int = 1
    categories: tuple[str, ...] = ()
    backoff_ms: int = 100
    max_backoff_ms: int = 1000

    def __post_init__(self):
        if (type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 8
                or type(self.backoff_ms) is not int or not 0 <= self.backoff_ms <= 60_000
                or type(self.max_backoff_ms) is not int or not self.backoff_ms <= self.max_backoff_ms <= 60_000
                or not isinstance(self.categories, (tuple, list))
                or any(type(item) is not str or item not in _RETRY_CATEGORIES for item in self.categories)
                or len(set(self.categories)) != len(self.categories)
                or self.max_attempts > 1 and not self.categories):
            raise RunnerError("invalid retry policy")
        object.__setattr__(self, "categories", tuple(sorted(self.categories)))

    @property
    def descriptor(self) -> dict:
        return {"format": "mas-retry-policy/1", "max_attempts": self.max_attempts,
                "categories": list(self.categories), "backoff_ms": self.backoff_ms,
                "max_backoff_ms": self.max_backoff_ms}

    def eligible(self, outcome: dict, attempt: int) -> bool:
        from .provider_outcomes import validate_provider_outcome
        checked = validate_provider_outcome(outcome)
        return (checked["certainty"] == "not_sent" and checked["category"] in self.categories
                and type(attempt) is int and 1 <= attempt < self.max_attempts)

    def delay_ms(self, attempt: int) -> int:
        if type(attempt) is not int or not 1 <= attempt <= self.max_attempts:
            raise RunnerError("invalid retry attempt")
        return min(self.max_backoff_ms, self.backoff_ms * 2 ** (attempt - 1))


def retry_policy(value: RetryPolicy | dict | None) -> RetryPolicy:
    if value is None:
        return RetryPolicy()
    if isinstance(value, RetryPolicy):
        return value
    if type(value) is not dict:
        raise RunnerError("invalid retry policy")
    fields = dict(value)
    if "format" in fields and fields.pop("format") != "mas-retry-policy/1":
        raise RunnerError("unsupported retry policy")
    try:
        return RetryPolicy(**fields)
    except TypeError:
        raise RunnerError("invalid retry policy") from None


@dataclass(frozen=True)
class TimingPolicy:
    """Limits measured independently from an immutable submission timestamp."""

    business_seconds: int
    active_seconds: int
    queue_seconds: int
    pause_seconds: int

    def __post_init__(self):
        if any(type(value) is not int or not 1 <= value <= 315_360_000 for value in (
                self.business_seconds, self.active_seconds, self.queue_seconds, self.pause_seconds)):
            raise RunnerError("invalid timing policy")

    @property
    def descriptor(self) -> dict:
        return {"format": "mas-timing-policy/1", "business_seconds": self.business_seconds,
                "active_seconds": self.active_seconds, "queue_seconds": self.queue_seconds,
                "pause_seconds": self.pause_seconds}

    def start(self, now_ms: int) -> dict:
        _timestamp(now_ms)
        return {"format": "mas-execution-clock/2", "policy": self.descriptor, "created_ms": now_ms,
                "observed_ms": now_ms, "phase": "queued", "phase_started_ms": now_ms,
                "active_ms": 0, "active_branches": 0, "worker_ms": 0,
                "worker_time_complete": True, "extensions": {}, "deadline_revision": 0}


def timing_policy(value: TimingPolicy | dict | None) -> TimingPolicy | None:
    if value is None or isinstance(value, TimingPolicy):
        return value
    if type(value) is not dict:
        raise RunnerError("invalid timing policy")
    fields = dict(value)
    if "format" in fields and fields.pop("format") != "mas-timing-policy/1":
        raise RunnerError("unsupported timing policy")
    try:
        return TimingPolicy(**fields)
    except TypeError:
        raise RunnerError("invalid timing policy") from None


def _timestamp(value):
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise RunnerError("invalid execution clock timestamp")


def clock_status(clock: dict, now_ms: int) -> dict:
    """Charge unobserved running time conservatively, including across restart."""
    _timestamp(now_ms)
    required = {"format", "policy", "created_ms", "observed_ms", "phase", "phase_started_ms",
                "active_ms", "active_branches"}
    if type(clock) is not dict:
        raise RunnerError("invalid execution clock")
    version = clock.get("format")
    if version == "mas-execution-clock/2":
        required |= {"worker_ms", "worker_time_complete", "extensions", "deadline_revision"}
    if set(clock) != required or version not in {"mas-execution-clock/1", "mas-execution-clock/2"}:
        raise RunnerError("invalid execution clock")
    policy = timing_policy(clock["policy"])
    if policy is None:
        raise RunnerError("invalid execution clock")
    if version == "mas-execution-clock/2":
        _timestamp(clock["active_ms"])
        _timestamp(clock["worker_ms"])
        _timestamp(clock["deadline_revision"])
        if (type(clock["worker_time_complete"]) is not bool or clock["worker_ms"] < clock["active_ms"]
                or type(clock["extensions"]) is not dict
                or not set(clock["extensions"]) <= {"business_seconds", "queue_seconds", "pause_seconds"}):
            raise RunnerError("invalid execution clock accounting")
        for key, value in clock["extensions"].items():
            if type(value) is not int or not getattr(policy, key) <= value <= 315_360_000:
                raise RunnerError("invalid execution clock extension")
        policy = timing_policy(policy.descriptor | clock["extensions"])
    if policy is None or clock["phase"] not in {"queued", "running", "paused", "terminal"}:
        raise RunnerError("invalid execution clock")
    for field in ("created_ms", "observed_ms", "phase_started_ms", "active_ms", "active_branches"):
        _timestamp(clock[field])
    if (not clock["created_ms"] <= clock["phase_started_ms"] <= clock["observed_ms"] <= now_ms
            or clock["active_branches"] > 1024
            or (clock["phase"] == "running") != (clock["active_branches"] > 0)):
        raise RunnerError("execution clock regression or invalid phase")
    # Historical terminal outcomes retain their final clock observations; a
    # later inspection must not turn a completed task into a deadline failure.
    if clock["phase"] == "terminal":
        now_ms = clock["observed_ms"]
    interval = now_ms - clock["observed_ms"] if clock["phase"] == "running" else 0
    active = clock["active_ms"] + interval
    # A v1 record lacks historical branch multiplicities. Its active elapsed
    # time is a lower bound, never fabricated as exact worker consumption.
    complete = clock.get("worker_time_complete", clock["active_ms"] == 0)
    worker = clock.get("worker_ms", clock["active_ms"]) + interval * clock["active_branches"]
    expired = []
    if now_ms - clock["created_ms"] >= policy.business_seconds * 1000:
        expired.append("business_deadline")
    if active >= policy.active_seconds * 1000:
        expired.append("active_budget")
    if clock["phase"] == "queued" and now_ms - clock["phase_started_ms"] >= policy.queue_seconds * 1000:
        expired.append("queue_deadline")
    if clock["phase"] == "paused" and now_ms - clock["phase_started_ms"] >= policy.pause_seconds * 1000:
        expired.append("pause_deadline")
    return {"expired": expired, "active_ms": active,
            "worker_ms": worker if complete else None, "worker_ms_lower_bound": worker,
            "worker_time_complete": complete, "deadline_revision": clock.get("deadline_revision", 0),
            "remaining_active_ms": max(0, policy.active_seconds * 1000 - active),
            "business_expires_ms": clock["created_ms"] + policy.business_seconds * 1000}


def transition_clock(clock: dict, now_ms: int, phase: str, *, active_branches: int = 0) -> dict:
    """Store owner commits the returned clock with its lifecycle transition.

    Pause/terminal transitions are permitted only after local workers have
    stopped. Running branches count elapsed time once, not worker-seconds.
    """
    status = clock_status(clock, now_ms)
    if phase not in {"queued", "running", "paused", "terminal"}:
        raise RunnerError("invalid execution clock phase")
    if clock["phase"] == "terminal" and phase != "terminal":
        raise RunnerError("terminal execution clock cannot resume")
    if status["expired"] and phase in {"queued", "running"}:
        raise RunnerError("execution clock expired")
    updated = {**clock, "format": "mas-execution-clock/2",
               "worker_ms": status["worker_ms_lower_bound"],
               "worker_time_complete": status["worker_time_complete"],
               "extensions": clock.get("extensions", {}),
               "deadline_revision": clock.get("deadline_revision", 0),
               "observed_ms": now_ms, "active_ms": status["active_ms"], "phase": phase,
               "phase_started_ms": clock["phase_started_ms"] if phase == clock["phase"] else now_ms,
               "active_branches": active_branches}
    clock_status(updated, now_ms)
    return updated

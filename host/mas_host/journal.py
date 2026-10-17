"""Host workflow checkpoints; Rust remains the task and transport authority.

The journal records intent before effects and results before protocol completion.
It never interprets a missing reply as proof that an external effect did not occur.
"""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid

from .schema import JOURNAL, SchemaError, connect


class JournalError(RuntimeError):
    pass


class RunLock:
    """Exclusive process lifetime lock shared by runners and operator mutations."""
    def __init__(self, run_dir: Path):
        descriptor = os.open(Path(run_dir) / ".host.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        self._file = os.fdopen(descriptor, "a+", encoding="utf-8")
        try:
            fcntl.flock(self._file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._file.close()
            raise JournalError("run is already in use") from None

    def fileno(self) -> int:
        """Descriptor retained by the execution guardian until child cleanup."""
        return self._file.fileno()

    def close(self):
        if not self._file.closed:
            fcntl.flock(self._file, fcntl.LOCK_UN)
            self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


class Journal:
    def __init__(self, run_dir: Path, metadata: dict | None = None):
        path = Path(run_dir) / "host.sqlite"
        if metadata is not None and set(metadata) != {"run_id", "workflow_version", "topology", "max_depth"}:
            raise JournalError("incomplete or unsupported recovery journal metadata")
        if metadata is None and not path.is_file():
            raise JournalError("run has no recovery journal")
        def validate_metadata(db):
            values = {row[0]: row[1] for row in db.execute("SELECT key,value FROM metadata")}
            if not values and metadata is not None:
                return  # Known bootstrap can finish a legacy partial initialisation.
            required = {"run_id", "workflow_version", "topology", "max_depth"}
            if set(values) != required:
                raise JournalError("incomplete or unsupported recovery journal metadata")
            try:
                decoded = {key: json.loads(value) for key, value in values.items()}
            except (TypeError, ValueError):
                raise JournalError("invalid recovery journal metadata") from None
            if metadata is not None and decoded != metadata:
                raise JournalError("bootstrap metadata differs from recovery journal")

        def initialise(db):
            for key, value in metadata.items():
                db.execute("INSERT INTO metadata VALUES(?,?)", (key, canonical(value)))

        try:
            self.db = connect(path, JOURNAL,
                              initialise=initialise if metadata is not None else None,
                              validate_content=validate_metadata)
            if metadata is not None:
                with self.db:
                    if not self.db.execute("SELECT 1 FROM metadata LIMIT 1").fetchone():
                        initialise(self.db)
                validate_metadata(self.db)
        except SchemaError as error:
            raise JournalError(str(error)) from None

    def metadata(self):
        return {row["key"]: json.loads(row["value"]) for row in self.db.execute("SELECT * FROM metadata")}

    def step(self, key: str, kind: str, inputs: dict, initial: dict):
        with self.db:
            row = self.db.execute("SELECT * FROM steps WHERE key=?", (key,)).fetchone()
            if row is not None:
                if row["kind"] != kind or row["input"] != canonical(inputs):
                    raise JournalError("workflow replay differs from durable checkpoint")
                return json.loads(row["data"])
            self.db.execute("INSERT INTO steps VALUES(?,?,?,?)", (key, kind, canonical(inputs), canonical(initial)))
            return initial

    def set_step(self, key, data):
        with self.db:
            self.db.execute("UPDATE steps SET data=? WHERE key=?", (canonical(data), key))

    def get_step(self, key):
        row = self.db.execute("SELECT data FROM steps WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def queue_event(self, event: dict):
        """Journal outbox identity shares the existing, versioned step repository."""
        key, encoded = "event:" + event["message_id"], canonical(event)
        with self.db:
            prior = self.db.execute("SELECT kind,data FROM steps WHERE key=?", (key,)).fetchone()
            if prior is not None:
                if prior["kind"] not in {"event_pending", "event_published"} or prior["data"] != encoded:
                    raise JournalError("protocol event identity conflict")
                return
            self.db.execute("INSERT INTO steps VALUES(?,'event_pending','{}',?)", (key, encoded))

    def pending_events(self):
        return [json.loads(row[0]) for row in self.db.execute(
            "SELECT data FROM steps WHERE kind='event_pending' ORDER BY rowid")]

    def publish_event(self, message_id):
        with self.db:
            self.db.execute("UPDATE steps SET kind='event_published' WHERE key=? AND kind='event_pending'",
                            ("event:" + message_id,))

    def recorded_envelopes(self):
        for row in self.db.execute("SELECT data FROM steps WHERE kind='request' ORDER BY rowid"):
            data = json.loads(row[0])
            for key in ("request", "terminal"):
                if data.get(key) is not None:
                    yield data[key]

    def child_count(self, request_id):
        prefix = request_id + ":"
        return self.db.execute("SELECT COUNT(*) FROM steps WHERE substr(key,1,?)=?", (len(prefix), prefix)).fetchone()[0]

    def invocation(self, call_id):
        row = self.db.execute("SELECT * FROM invocations WHERE call_id=?", (call_id,)).fetchone()
        if row is None:
            return None
        value = dict(row)
        for key in ("output", "evidence"):
            if value[key] is not None:
                value[key] = json.loads(value[key])
        return value

    def create_invocation(self, step_key, actor, reservation_microusd):
        call_id = "call-" + uuid.uuid4().hex
        with self.db:
            self.db.execute("INSERT INTO invocations VALUES(?,?,?,'prepared',NULL,NULL,?)",
                            (call_id, step_key, actor, reservation_microusd))
            self.db.execute("UPDATE steps SET data=? WHERE key=?", (canonical({"call_id": call_id}), step_key))
        return self.invocation(call_id)

    def update_invocation(self, call_id, state, *, output=None, evidence=None):
        with self.db:
            self.db.execute("UPDATE invocations SET state=?,output=?,evidence=? WHERE call_id=?",
                            (state, canonical(output) if output is not None else None,
                             canonical(evidence) if evidence is not None else None, call_id))

    def record_known_failure(self, call_id: str, outcome: dict, *, completed_output: dict | None = None) -> dict:
        """Checkpoint a trusted terminal observation without releasing allocation.

        Version 2 readers recognise failed_known; old writers reject the schema
        rather than interpreting an unfamiliar state as permission to dispatch.
        """
        from .provider_outcomes import validate_provider_outcome, validate_completed_output
        checked = validate_provider_outcome(outcome)
        output = validate_completed_output(checked, completed_output)
        evidence = {"outcome": "not_executed" if checked["certainty"] == "not_sent" else "completed",
                    "provider_outcome": checked}
        with self.db:
            changed = self.db.execute(
                "UPDATE invocations SET state='failed_known',output=?,evidence=? "
                "WHERE call_id=? AND state='executing'",
                (canonical(output) if output is not None else None, canonical(evidence), call_id))
            if changed.rowcount != 1:
                raise JournalError("invocation is not awaiting a provider outcome")
        return self.invocation(call_id)

    def schedule_retry(self, call_id: str, policy, *, next_step_key: str, now_ms: int) -> dict | None:
        """Persist one bounded decision; restart never resets backoff or attempts."""
        with self.db:
            call = self.invocation(call_id)
            if call is None or call["state"] != "failed_known":
                return None
            row = self.db.execute("SELECT input,data FROM steps WHERE key=?", (call["step_key"],)).fetchone()
            inputs, data = json.loads(row[0]), json.loads(row[1])
            if inputs.get("retry_policy") != policy.descriptor:
                return None
            attempt = inputs.get("attempt", {}).get("number", 1)
            if not policy.eligible(call["evidence"]["provider_outcome"], attempt):
                return None
            if "retry_decision" in data:
                plan = data["retry_decision"]
                if plan["policy"] != policy.descriptor or plan["step_key"] != next_step_key:
                    raise JournalError("retry policy or step differs from durable decision")
                return plan
            retry_after = call["evidence"]["provider_outcome"].get("retry_after_seconds", 0)
            plan = {"previous_call_id": call_id,
                    "root_call_id": inputs.get("attempt", {}).get("root_call_id", call_id),
                    "number": attempt + 1,
                    "eligible_at_ms": now_ms + max(policy.delay_ms(attempt), retry_after * 1000),
                    "step_key": next_step_key, "policy": policy.descriptor}
            if retry_after:
                plan["retry_after_seconds"] = retry_after
            data["retry_decision"] = plan
            self.db.execute("UPDATE steps SET data=? WHERE key=?", (canonical(data), call["step_key"]))
            return plan

    def retry_inputs(self, call_id: str, *, actor: str, step_key: str, inputs: dict,
                     policy, now_ms: int) -> dict:
        """Check a scheduled attempt against original input and parent authority."""
        call = self.invocation(call_id)
        if call is None or call["actor"] != actor or call["state"] != "failed_known":
            raise JournalError("retry does not name a known failed invocation")
        row = self.db.execute("SELECT input,data FROM steps WHERE key=?", (call["step_key"],)).fetchone()
        previous, data = json.loads(row[0]), json.loads(row[1])
        plan = data.get("retry_decision")
        if (type(plan) is not dict or plan["previous_call_id"] != call_id or plan["step_key"] != step_key
                or plan["policy"] != policy.descriptor or now_ms < plan["eligible_at_ms"]
                or not policy.eligible(call["evidence"]["provider_outcome"], plan["number"] - 1)
                or any(canonical(previous[name]) != canonical(inputs[name])
                       for name in ("input", "reservation_microusd"))):
            raise JournalError("retry is not eligible or its binding changed")
        return {"number": plan["number"], "root_call_id": plan["root_call_id"], "previous_call_id": call_id}

    def close(self):
        self.db.close()


def inspect_run(run_dir: Path) -> dict:
    """Read checkpoint metadata and invocation audit without returning task inputs."""
    path = Path(run_dir).resolve() / "host.sqlite"
    if not path.is_file():
        raise JournalError("run has no recovery journal")
    try:
        connection = connect(path, JOURNAL, readonly=True)
    except SchemaError as error:
        raise JournalError(str(error)) from None
    from contextlib import closing
    with closing(connection) as db:
        metadata = {row["key"]: json.loads(row["value"]) for row in db.execute("SELECT * FROM metadata")}
        calls = []
        for row in db.execute("SELECT * FROM invocations ORDER BY rowid"):
            item = dict(row)
            for key in ("output", "evidence"):
                if item[key] is not None:
                    item[key] = json.loads(item[key])
            step = db.execute("SELECT input,data FROM steps WHERE key=?", (item["step_key"],)).fetchone()
            if step is not None:
                inputs, data = json.loads(step[0]), json.loads(step[1])
                if "attempt" in inputs:
                    item["attempt"] = inputs["attempt"]
                if "retry_decision" in data:
                    item["retry_decision"] = data["retry_decision"]
            calls.append(item)
        resolutions = [dict(row) for row in db.execute("SELECT * FROM resolutions ORDER BY id")]
        for item in resolutions:
            item["evidence"] = json.loads(item["evidence"])
        return {"run_id": metadata["run_id"], "workflow_version": metadata["workflow_version"],
                "invocations": calls, "resolutions": resolutions,
                "steps": db.execute("SELECT COUNT(*) FROM steps WHERE kind NOT IN ('event_pending','event_published')").fetchone()[0],
                "recovery_required": any(item["state"] in ("executing", "unknown") for item in calls)}


def resolve_invocation(run_dir: Path, call_id: str, *, decision: str, evidence: dict,
                       output: dict | None = None) -> dict:
    """Record a trusted operator decision. This never contacts/repeats a provider.

    completed requires independently established output; confirmed_no_effect is
    explicit permission to try this invocation again, with a fresh metered call.
    Neither decision clears or edits the original Rust call record.
    """
    from .runner import _json
    if decision not in ("completed", "confirmed_no_effect") or type(evidence) is not dict or not evidence:
        raise JournalError("resolution requires a supported decision and nonempty evidence")
    if decision == "completed" and type(output) is not dict:
        raise JournalError("completed resolution requires object output")
    if decision == "confirmed_no_effect" and output is not None:
        raise JournalError("confirmed_no_effect cannot supply output")
    _json({"evidence": evidence, "output": output})
    with RunLock(run_dir):
        journal = Journal(run_dir)
        try:
            call = journal.invocation(call_id)
            if call is None or call["state"] not in ("executing", "unknown"):
                raise JournalError("only an uncertain invocation can be resolved")
            with journal.db:
                journal.db.execute("INSERT INTO resolutions(call_id,decision,evidence,created) VALUES(?,?,?,?)",
                                   (call_id, decision, canonical(evidence), int(time.time())))
                journal.db.execute("UPDATE invocations SET state=?,output=?,evidence=? WHERE call_id=?",
                                   ("resolved" if decision == "completed" else "retry_allowed",
                                    canonical(output) if output is not None else None, canonical(evidence), call_id))
        finally:
            journal.close()
    return inspect_run(run_dir)

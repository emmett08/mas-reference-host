# Provider outcomes, bounded attempts and recovery

The generic structured OpenAI and Cursor providers distinguish an observed
completion from an uncertain external effect. Construction, descriptors and
management inspection remain credential-free. Their explicit `readiness()`
methods check local credentials/runtime readiness without submitting inference.
Legacy invoice descriptors and stored uncertain records retain their existing
compatibility behaviour; new Cursor cleanup failures retain observed completion.

| Observation | Durable invocation | Permitted next action |
| --- | --- | --- |
| Credentials, runtime or configuration failure proved before dispatch | `failed_known`, certainty `not_sent` | Handle the error, submit new work, or use an explicitly configured bounded retry policy |
| Completed refusal, invalid structured output or forbidden tool call | `failed_known`, certainty `completed` | Inspect the retained completion; a correction is new admitted work |
| Valid completed result followed by failed local cleanup | `failed_known`, certainty `completed`, output status `valid` | Complete local cleanup before using the retained output; never repeat the provider call |
| Lost response, timeout, incomplete response or generic HTTP error | `unknown` | Gather evidence and make an explicit operator decision |
| Valid completed result | `completed` | Replay the checkpoint without invoking the provider |

Every admitted attempt keeps its call and spending allocation. A known failure
does not refund it. Typed outcome records contain only bounded categories,
correlation identifiers, request digests and token counts. Raw exception text,
provider response bodies and credentials are excluded. Generic HTTP status and
`Retry-After` observations do not establish that a request had no effect.

A cleanup failure does not erase an observed valid response or report normal
success. Its bounded output is retained separately in the private invocation
journal and worker receipt, never inside the public outcome diagnostic. Custom
outputs above 32 KiB are omitted while completion certainty remains retained.
Replay raises the same known cleanup failure without invoking the provider.

The host journal is version 2. Its layout is unchanged, but version 1 writers
must reject it because they do not understand the new terminal state. Existing
unknown records migrate without reclassification. Inline, legacy forked,
registered executable and remote registered-worker execution preserve these
observations across their parent-owned admission boundaries.

## Explicit retry policy

Configure policies in a trusted workflow factory. Omitting them preserves the
existing workflow descriptor and disables automatic attempts beyond the first.

```python
from dataclasses import replace
from mas_host.execution_policy import RetryPolicy, TimingPolicy
from mas_host.circuit_breaker import CircuitPolicy

workflow = replace(
    workflow,
    retry_policy=RetryPolicy(
        max_attempts=3,
        categories=("operation_busy",),
        backoff_ms=100,
        max_backoff_ms=1000,
    ),
    timing_policy=TimingPolicy(
        business_seconds=7 * 86400,
        active_seconds=3600,
        queue_seconds=900,
        pause_seconds=3 * 86400,
    ),
    circuit_breakers={
        "model_actor": CircuitPolicy(
            resource_key="provider:production-openai-v1",
            failure_threshold=5,
            open_seconds=30,
            half_open_seconds=60,
        ),
    },
)
```

Actor names must exist in the workflow topology. Retry eligibility is restricted
to explicitly listed, proved pre-dispatch categories. The maximum is eight
attempts. Completed invalid output, refusal and unknown effects never qualify.
Each retry has a distinct call ID and allocation, with durable ancestry,
eligibility time and policy in the journal. Restart does not reset backoff or
the attempt count. A trusted `not_sent(..., retry_after_seconds=...)` observation
can lengthen the delay; it cannot extend the execution deadline. Outcome records
themselves say `automatic_retry: false`: the observation alone grants no retry
authority; the separate frozen host policy decides eligibility.

Timing policy separates the immutable business deadline, cumulative elapsed
active time, queue wait and per-pause response expiry. Active parallel branches
charge elapsed workflow time once. Unobserved running time is charged
conservatively after a crash. Computation pauses only after local workers stop.
The terminal clock retains its final observation when inspected later. Existing
instances without a clock keep their original fixed-deadline semantics.

## Shared provider circuits

Circuits are opt-in and keyed by an administrator-selected provider resource
identity. Workflows using the same key share `provider-circuits.sqlite`; a
different policy under that key is rejected. Use a new explicit resource key to
introduce a new policy. A circuit check precedes fresh call admission and never
authorises retry of an earlier call.

Only trusted finite transport, timeout, HTTP 429 and HTTP 5xx diagnostics count
as infrastructure failures. Unrelated application exceptions and cancellation
do not count. A completed provider response, including invalid output, is a
successful infrastructure observation. An open circuit suppresses fresh
dispatch. After its interval, one durable half-open probe may run. An abandoned
probe reopens the interval without repeating its invocation. Generation checks
prevent a late result from closing a newer open circuit. Existing uncertain
effects and their allocations remain retained throughout.

The circuit store participates in deployment backup and audit-only restore.
`CircuitBreaker.status()` reports its resource, failures, interval and current
probe. This local controller store does not claim consensus or availability
across independent controller databases.

## Operator recovery

`RecoveryService` takes an existing durable service and an explicit mapping of
authenticated operators to task owners they may inspect and resolve. Task-owner
permission alone does not grant resolution authority. Its inbox covers retained
revisions, uncertain journal calls and local remote-task/Cursor-job receipts.
It separates cancellation intent, closed future dispatch, observed local stop,
remote cancellation acknowledgement and remaining effect reconciliation.
An idle process or elapsed lease does not prove remote cancellation.

```python
from mas_host.recovery import RecoveryService

recovery = RecoveryService(service, operators={"operator-id": {"task-owner"}})
inbox = recovery.inbox("operator-id", "task-owner", task_id)
recovery.resolve(
    "operator-id", "task-owner", task_id, call_id,
    request_key="incident-123-decision-1",
    decision="completed",
    evidence={
        "source": "provider_receipt",
        "reference": "receipt-123",
        "observed_at_ms": 1792000000000,
    },
    output={"answer": 42},
)
# Separately request normal scheduling after all effects are reconciled.
service.recover("task-owner", task_id)
```

The alternative decision is `confirmed_no_effect`, with no output. It requires
independently established evidence and permits a fresh metered attempt when
normal recovery runs. Repeating the same resolution request is idempotent;
changing its decision, evidence or output conflicts. The audit retains operator,
owner, task, request key and observation. Live run ownership and audit-only
deployment state prevent mutation. Resolution itself never invokes a provider
or resumes task execution.

Deployments may register `ReconciliationQuery(name, version, callback,
permits_no_effect=False)` contracts. Queries return a bounded exact-call
observation and never resolve automatically. A missing search result is not an
accepted certainty. The registry is a trusted extension point; no generic live
provider query is implied. Existing remote-task reconciliation uses admitted
exact peer handles and explicit operator evidence. The local admin commands
`recovery-inbox` and `recovery-resolve` expose the service through configured
workflow grants; see `python -m mas_host.admin --help`.

## Progress delivery

`Runner(event_sink=...)` publishes accepted protocol identities during execution.
Durable services use it to publish principal-scoped events before a leaf returns.
The payload contains run, task, message, sender, recipient and kind identifiers,
with no prompt or provider output. Service event stores deduplicate by run and
message identity. A journal outbox records pending publications and acknowledgements;
startup repairs the acceptance/publication gap from authoritative Rust messages
and retained host envelopes. A failed sink or lost acknowledgement does not
repeat an external effect. Polling event pages resumes from a durable cursor.

## Pinned orchestration peers

`OrchestratorConnector` can take `PeerContract(deployment_id,
{"workflow_name": workflow_identity})`. It checks the task-service contract and
capabilities, then passes expected deployment and executable identities through
the authenticated TaskService HTTP client. Submission and continuation validate
those identities inside their existing write transactions. Replaying an old
key may return its original pinned receipt after the default registration
changes; fresh work cannot silently select the changed implementation.

Temporal activities and LangGraph nodes retain logical operation keys across
outer retries. The HTTP client does not automatically repeat POSTs. A caller
that loses its first accepted receipt may repeat the same key and input to
recover the original task. Unknown, human-input, authentication and cancellation
observations stay explicit. Pinning currently applies to the TaskService HTTP
and Python connector surface; MCP/A2A peers retain their own protocol-specific
capability contracts.

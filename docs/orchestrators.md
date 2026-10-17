# Temporal and LangGraph bridges

The connectors submit work through the same authenticated durable task service
as MCP and A2A. The outer orchestrator owns business sequencing. MAS retains
request identity, route permissions, call admission, spending reservations and
recovery. A returned submission receipt is not a completed business result.

For administrator-managed MAS peers, use the [versioned federation registry](managed-federation.md).
It binds local caller grants to remote authenticated identities, executable pins,
capabilities and artifact scopes across the HTTP, MCP and A2A lifecycle profiles.

| Profile | Inbound | Outbound |
|---|---|---|
| Temporal Python SDK 1.34.0 | Seven registered activities over `TaskService` | Durable, exact-run workflow submission, observation, cancellation and reconciliation |
| LangGraph Python 1.2.15 | A real `StateGraph` node supporting checkpointers and interrupt/resume | Use the Temporal workflow adapter or a configured remote-agent bridge for external execution |

The SDKs are optional and imported only when their framework entry points are
constructed. The host still runs without either installed. Their Python
requirement is 3.10 or newer. The reproducible lock targets CPython 3.14 on
glibc Linux amd64 and arm64; it also contains the OpenAI pins so an orchestrator
worker can coexist with that provider. It is not an Alpine SDK image profile.

```bash
python3.14 -m venv .orchestrator-venv
.orchestrator-venv/bin/python -m pip install --require-hashes -r host/requirements-orchestrators.txt
.orchestrator-venv/bin/python -m pip install --require-hashes -r host/requirements-build.txt
.orchestrator-venv/bin/python -m pip install --no-deps --no-build-isolation ./host
```

For other supported Python versions, resolve `./host[temporal,langgraph]` in an
isolated environment and review that environment's dependency lock. The committed
CPython 3.14 wheel hashes are specific to their ABI.

## Temporal calls MAS

Start the MAS service using the authenticated deployment configuration described
in [the bridge guide](bridges.md). Its scheduler must run independently of the
Temporal activity worker. The following worker registers the activities; the
caller supplies its own workflow definitions in its existing Temporal workers.

```bash
.orchestrator-venv/bin/python -m mas_host.bridges.temporal_worker \
  --temporal-target temporal.example.org:7233 \
  --namespace engineering \
  --task-queue mas-operations \
  --mas-endpoint https://mas.example.org \
  --token-file /run/secrets/mas_service_token \
  --principal temporal-worker
```

TLS is enabled for Temporal and required for the MAS HTTP service. The explicit
`--insecure-temporal` and `--allow-loopback-http` switches support development;
the latter accepts numeric loopback addresses only. A Temporal API key can be
supplied separately with `--temporal-api-key-file`.

| Activity | Input | Returned value |
|---|---|---|
| `mas.submit` | `workflow`, `input`; optional `request_key` | Durable receipt |
| `mas.execute` | Submission fields; optional `timeout_seconds`, `poll_seconds` | Latest receipt after bounded polling |
| `mas.inspect` | `task_id` | Current receipt, including errors and uncertainty |
| `mas.wait` | `task_id`; optional polling settings | Latest receipt after bounded polling |
| `mas.result` | `task_id` | Completed application payload; other states raise `MAS.result_unavailable` |
| `mas.cancel` | `task_id` | Receipt with cancellation intent and observed state |
| `mas.update` | `task_id`, `input`; optional `request_key` | Receipt for an explicitly supported continuation |

For example, workflow code can schedule an activity with a heartbeat timeout:

```python
from datetime import timedelta
from temporalio import workflow

receipt = await workflow.execute_activity(
    "mas.execute",
    {"workflow": "invoice-check", "input": {"invoice_id": "invoice-17"},
     "timeout_seconds": 30, "poll_seconds": 1},
    task_queue="mas-operations",
    start_to_close_timeout=timedelta(seconds=50),
    heartbeat_timeout=timedelta(seconds=20),
    result_type=dict,
)
```

Branch on `receipt["state"]`. Completed, failed, cancelled, input-required,
authorisation-required and unknown states remain distinct. Polling can return
queued or running when its budget expires. Schedule `mas.wait` again for the same
task; do not create a new submission key to escape an uncertain result.

Default submission and update keys combine the Temporal namespace, workflow run
ID and activity ID. Retry attempt numbers are excluded. Standalone activities
use their activity-run ID. To preserve one operation across workflow
continue-as-new, supply an explicit stable `request_key`; change it for each new
logical operation. Reusing a key with changed content raises a non-retryable
conflict. `mas.execute` and `mas.wait` heartbeat and cooperate with cancellation
without asynchronous exceptions interrupting the recording of cancellation
intent. Transport calls have a separate hard deadline. A polling budget may
include one already-started transport call; size the activity and heartbeat
timeouts above that call's deadline.

## LangGraph calls MAS

The node takes a fixed principal through `OrchestratorConnector`; graph input
cannot choose another caller. Each state requires `mas_operation_id`, stable
across retries and changed for each new logical operation in a graph loop.

```python
from typing import TypedDict
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command
from mas_host.bridges.langgraph import make_langgraph_node
from mas_host.bridges.orchestrator import OrchestratorConnector
from mas_host.bridges.service_client import TaskServiceClient

class State(TypedDict):
    input: dict
    mas_operation_id: str
    mas_receipt: dict

service = TaskServiceClient("https://mas.example.org", principal="graph-worker",
                           token_file="/run/secrets/mas_service_token")
builder = StateGraph(State)
builder.add_node("check", make_langgraph_node(
    OrchestratorConnector(service, "graph-worker"), "invoice-check", "check"))
builder.add_edge(START, "check")
builder.add_edge("check", END)
graph = builder.compile(checkpointer=InMemorySaver())
config = {"configurable": {"thread_id": "invoice-batch-17"}}
result = graph.invoke({"input": {"invoice_id": "invoice-17"},
                       "mas_operation_id": "check-1"}, config)
# If result contains __interrupt__, the same thread can resume with:
result = graph.invoke(Command(resume={"action": "poll"}), config)
```

`InMemorySaver` above demonstrates the API. Use a durable checkpointer for
process-restart recovery. An interrupt advertises only the available actions:
poll, cancellation where supported, and additional input where the workflow
explicitly permits it. An input continuation uses
`{"action":"input","request_key":"correction-1","input":{...}}`.
The separate update key prevents repeated graph execution from applying the
same correction twice. A resumed node returns `mas_receipt`; the graph owner
decides whether to wait again, route to review or finish. No automatic retry or
provider substitution occurs after an unknown outcome.

## MAS calls a Temporal workflow

Compose `TemporalWorkflowClient`, `RemoteTaskStore` and `RemoteTaskService`
inside `Workflow.adapter_factory`. The scheduler supplies `ExecutionBinding`;
never derive its principal, namespace or run directory from request input.
For a registration whose actor is named `remote`:

```python
from mas_host.bridges.remote import RemoteTaskService, RemoteTaskStore
from mas_host.bridges.temporal import (
    TemporalTarget, TemporalWorkflowClient, TemporalWorkflowAdapter,
)

target = TemporalTarget(
    endpoint="temporal.example.org:7233", namespace="engineering",
    workflow="InvoiceWorkflow", task_queue="invoice-workers",
    deployment_id="engineering-mas-production", tls=True,
)

def adapter_factory(binding):
    remote = RemoteTaskService(
        RemoteTaskStore(binding.state_dir), TemporalWorkflowClient(target),
        namespace=binding.remote_namespace("remote"),
    )
    # prototypes is the trusted actor map used by your Workflow registration.
    return {**prototypes, "remote": TemporalWorkflowAdapter(
        remote, binding.run_dir, reservation_microusd=1000)}
```

Set that registration's root recipient to `remote`, its operation to `workflow`,
and its `adapter_factory` to the function above. The actor map must exactly match
the declared topology. Bump the registration's workflow version whenever target,
adapter code, admission or result policy changes. The remote record namespace
and Temporal workflow ID are both scoped to the trusted owner, so identical job
keys from different principals cannot collide.

Submit inputs use `{"action":"submit","request_key":"invoice-17","input":{...}}`.
Later operations use the same remote key and a fresh outer service submission
key: `{"action":"poll","request_key":"invoice-17"}` or
`{"action":"cancel","request_key":"invoice-17","cancel_key":"stop-1"}`.
Each operation obtains a fresh admitted invocation. A successful MAS submission
contains a remote job receipt; its `state` describes the remote workflow.

The client disables duplicate workflow IDs, records intent before dispatch and
stores the exact workflow/run ID pair. A cancellation acknowledgement preserves
the observed running state until the server reports cancellation. Lost initial
replies remain unknown and cannot trigger another start. For reconciliation,
`client.lookup(adapter.submission_payload(key, original_input))` performs a
read-only lookup and verifies the workflow type and original input binding in
the Temporal memo. Recovery uses the server's first-run identity and verifies
that exact run, including after continue-as-new; a successor's inherited memo
does not make it the admitted execution. Missing original-run history leaves
recovery unresolved. Obtain that read under a fresh admitted invocation when
automating reconciliation. An authorised local operator can then invoke
`RemoteTaskService.attach` with the original trusted namespace, the exact
`remote_id`, nonempty operator evidence and a fresh admitted context.
Attachment is excluded from the public `workflow` dispatcher: a submitter's
claim to be an operator grants no reconciliation authority. The adapter's direct
`attach` operation is reserved for trusted administrative registration or local
administrative code. Attachment performs an admitted observation of that run.
A missing workflow is not proof that the original
submission had no effect.

This outbound profile accepts bounded integer-JSON dictionaries as inputs and
results. It covers one Temporal run: continue-as-new is reported as unknown,
never silently followed to a new execution. Signals and updates are not
advertised as supported continuation capabilities. Define a separate explicit
profile for workflows that need those semantics. Configure Temporal retention
to exceed the MAS reconciliation horizon: after remote history expiry, retain
the local uncertain record until independent evidence resolves it.

## Verification and dependency maintenance

```bash
PYTHONPATH=host:host/tests .orchestrator-venv/bin/python -m unittest \
  test_orchestrator test_orchestrator_sdks test_orchestrator_service -v
MAS_TEMPORAL_INTEROP=1 PYTHONPATH=host:host/tests \
  .orchestrator-venv/bin/python -m unittest test_orchestrator_temporal_server -v
python3.14 scripts/release-locks.py --only bridges --only orchestrators --keep-versions
python3.14 scripts/release-locks.py --check
```

The real-server test uses Temporal CLI 1.9.1 / Server 1.32.0 and no paid provider.
Set `MAS_TEMPORAL_SERVER` to an already provisioned CLI binary to avoid downloading
it. It tests actual workflow completion, cancellation, a discarded accepted
submission reply, memo-checked recovery and duplicate rejection. It also covers
lost-reply recovery after continue-as-new and verifies that cancellation
of the recovered original does not affect its successor. Other tests
execute the installed Temporal activity environment, a real LangGraph
checkpoint/interrupt/resume graph, actual durable MAS service calls and the
Rust gateway. The inbound real-server case runs a Temporal activity in its
thread executor, calls the authenticated HTTP service and executes MAS through
a separate exclusive worker process. Discarding the first accepted submission
reply forces a real activity retry; both attempts retain one request identity,
one durable task and one completed result. The optional profiles have their own
wheel hashes and CI checks.

Primary API references: [Temporal Python SDK](https://github.com/temporalio/sdk-python),
[Temporal activity failure detection](https://docs.temporal.io/encyclopedia/detecting-activity-failures),
[LangGraph interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts),
and [LangGraph durable execution](https://docs.langchain.com/oss/python/langgraph/durable-execution).

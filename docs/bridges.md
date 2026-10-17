# Durable MCP, A2A and orchestrator bridges

The bridges expose one authenticated durable task service over the existing
Rust admission boundary. MCP and A2A use their own wire formats; Temporal and
LangGraph use the same submission/status contract. OpenAI, Cursor and deterministic
adapters remain selectable inside the workflow independently of its bridge.

## Supported profiles

| Boundary | Implemented behaviour |
| --- | --- |
| MCP 2026-07-28 | Inbound stdio and Streamable HTTP; discovery, tools, exact schemas and negotiated Tasks polling, continuation and cancellation. Outbound HTTP JSON/SSE responses and stdio tool invocation. |
| A2A 1.0, schema release 1.0.1 | Agent Card and JSON-RPC SendMessage, GetTask, ListTasks and CancelTask; task/context identity, artifacts, interrupted states, bounded blocking and polling. Outbound delegation and admitted lifecycle operations. |
| Temporal SDK 1.34.0 | Activities calling MAS; durable outbound workflow starts, exact workflow/run observation, cancellation and memo-checked reconciliation. |
| LangGraph 1.2.15 | Nodes calling MAS with stable thread/node identities, checkpoint interruption and explicit continuation. |
| Task service HTTP | Authenticated submit, inspect, result, cancel, update, events, offset lists and filtered keyset pages for applications and orchestrator workers. |

Streaming subscriptions, push notifications, arbitrary artifact downloads and
older protocol versions are not advertised. MCP's current profile has no
initialisation handshake or protocol session. Durable events are polled using
an application cursor. LangGraph hosted thread/run management is outside this
connector's profile; outbound workflow execution is implemented for Temporal.

See [MCP details](mcp-bridge.md), [A2A details](a2a-bridge.md) and
[orchestrator setup](orchestrators.md) for wire and library examples.

## Start an offline deployment

Install the host as described in [the host guide](host-runner.md). The normal
and Cursor images include the JSON Schema dependencies required by MCP.
Native installations can use `host/requirements-bridges.txt` with the documented
CPython 3.14 Linux wheel profile, or install the pinned `bridges` extra using a
compatible Python environment. Temporal/LangGraph have a separate optional
lock, `host/requirements-orchestrators.txt`, for Linux glibc amd64/arm64.

Create a private bearer token and a file containing its digest:

```sh
mkdir -p secrets
chmod 700 secrets
python -m mas_host bridge init-auth \
  --principal example-client \
  --token-file secrets/bridge.token \
  --verifier-file secrets/bridge-credentials.json
export MAS_BRIDGE_CREDENTIALS="$(cat secrets/bridge-credentials.json)"
docker compose -f compose.bridges.yaml up --build -d
unset MAS_BRIDGE_CREDENTIALS
```

The bearer token is never printed. Keep `secrets/bridge.token` private. The
server receives the digest file through a private environment-backed Compose
secret. The worker needs no inbound credential. Both processes share the state
volume; only the worker executes adapters. The published port is bound to the
host loopback address. For subsequent Compose operations, set the digest-file
environment variable again before invoking Compose.

The example grants only `invoice_mesh` to `example-client`. Select another
topology before starting the deployment:

```sh
export MAS_BRIDGE_CONFIG=/app/examples/bridges/star.json
# Or: /app/examples/bridges/tree.json
```

Use `invoice_star` or `invoice_tree` respectively when submitting. These are
different routed workflows, with deterministic checking of the same invoice.
The tree is a bidirectional chain of ingress, planner, reviewer and checker;
the star's hub owns checking and requests proposals from its leaves.

Submit through the stable Python client:

```python
from pathlib import Path
from mas_host.bridges.service_client import TaskServiceClient

service = TaskServiceClient(
    "http://127.0.0.1:8765", principal="example-client",
    token_file=Path("secrets/bridge.token"), allow_loopback_http=True)
receipt = service.submit("example-client", "invoice-2026-001", "invoice_mesh", {
    "invoice": [{"item": "Component", "quantity": 2, "unit_cents": 1500}]
})
print(receipt["task_id"], receipt["state"])
# Reuse the same request key and content after a lost submission reply.
current = service.inspect("example-client", receipt["task_id"])
if current["state"] == "completed":
    print(service.result("example-client", receipt["task_id"]))
```

The client never sends its principal as caller-controlled authority. The server
derives that identity from the token and checks its workflow grants. A changed
payload under an existing request key fails with `request_conflict`.

`submit` and `update` accept optional integer-JSON `metadata`, which participates
in request-key identity. Submission metadata remains on the task receipt;
continuation metadata binds that individual continuation request. The HTTP
client implements the public `TaskService` contract, including
`list_task_page(page_size=..., after=..., state=..., metadata=...,
updated_after=...)`. Its `next` cursor is passed back as `after` until it is
`None`; metadata filters support up to 64 string-valued predicates.
Trusted local worker execution is a separate `TaskWorker` capability and has
no HTTP operation. Orchestration connectors require only `TaskLifecycle`.

The task service caps each application input, metadata object and result at
65,536 canonical UTF-8 JSON bytes, within a separate 1 MiB HTTP envelope.
For the Rust executor, application data must also fit its complete 65,536-byte
wire message and escaped 65,536-byte gateway frame. Identity, operation and
correlation fields consume part of those bounds; characters requiring JSON
escaping consume additional space. The service checks the actual request
encoding before accepting a submission or continuation, without creating a
task or charging a run. Oversized frames return `input_too_large`. Metadata
is application history and is not included in the Rust request. An oversized
adapter result fails the task while retaining any already recorded invocation
output for inspection; it does not turn that known output into an unknown effect.
Task and event lists return a prefix bounded to 512 KiB even when the requested
item count is larger. For offset lists advance by the number actually returned;
for events advance `after` to the final event ID. Continue until an empty list,
since a short list can reflect the byte bound. Filtered pages provide an
explicit `next` cursor for both item-count and byte truncation. MCP retains its
separate 65,536-byte whole-message profile and returns a protocol error if a
response cannot fit; these task-service envelope limits do not enlarge it.

MCP clients use `http://127.0.0.1:8765/mcp`; A2A's card is at
`http://127.0.0.1:8765/.well-known/agent-card.json`. The checked examples use
bounded integer JSON. Decimal values must be represented by an application-defined
decimal string or rational record; the bridge never silently rounds them.

## Mix Cursor and OpenAI

Supply provider secrets as in [providers](providers.md), set an account-supported
`MAS_PLANNER_MODEL`, and start:

```sh
docker compose -f compose.bridges.yaml -f compose.bridges.mixed.yaml up --build -d
```

The override selects a Cursor planner and OpenAI reviewer, grants all three
invoice workflows and uses the repaired glibc image. Both server and worker
validate the same frozen provider configuration; only admitted worker execution
makes provider calls. A provider/model change requires a new workflow version
and registration name, or a separate deployment. Existing durable identities
cannot be silently replaced. Live provider acceptance still requires credentials.

## Native execution and custom workflows

The executable entry points are:

```sh
python -m mas_host bridge serve --config deployment.json --state-dir .mas-state
python -m mas_host bridge worker --config deployment.json --state-dir .mas-state
python -m mas_host bridge worker --once --config deployment.json --state-dir .mas-state
python -m mas_host bridge stdio --principal alice --config deployment.json --state-dir .mas-state
```

Run the worker separately from the HTTP/stdio process. Add
`--allow-loopback-http` for the explicit local development HTTP profile; HTTPS
is required for non-loopback public endpoints. A stdio server uses the trusted
OS launcher's configured principal and never accepts a principal from messages.

Configuration is bounded JSON, version `1`, with `grants` mapping authenticated
principals to workflow names. The built-in `mode` is `mock`, `openai`, `cursor`
or `mixed`. A local trusted `factory: "my_application.workflows:build"` instead
loads a function returning a mapping of names to `Workflow` values. Remotely
supplied data cannot select Python modules, adapters, endpoints or credentials.

Each registration fixes version, topology, actor membership, operation and
limits. `Workflow.adapter_factory` optionally constructs adapters from the
trusted `ExecutionBinding`, after the service assigns the run directory.
Outbound adapters use `binding.run_dir` and
`binding.remote_namespace(actor)`; this separates identical caller keys across
principals, workflow identities and actors. See the protocol-specific factory
examples. A factory must return exactly the registered actor set and its
behaviour must be covered by the explicit workflow version.

## Authentication and deployment boundary

The static token profile is for explicitly configured local/service clients.
An Internet-facing MCP resource uses the OAuth profile with the organisation's
authorisation server. Configure `auth.type: "oauth"` with `issuer`, `resource`,
`introspection_url`, `client_id` and `client_secret_file`. The `resource` must
identify this protected MCP resource. Introspection uses a pinned HTTPS endpoint,
separate credentials, active-token/audience/issuer checks and the scopes
`mas:submit`, `mas:read`, `mas:cancel` and `mas:update`. Workflow grants provide
the second authorisation check. Protected-resource discovery and challenges
point clients to the configured authorisation server. MAS does not issue OAuth
tokens or implement an identity provider.

Terminate public TLS in the organisation's reverse proxy and restrict access
to the backend. Set `public_url`, `allowed_hosts` and `allowed_origins` to the
actual deployment; forwarded identity headers are never trusted. The server
library also accepts an `ssl_context` for native TLS. Authentication secrets
must be owned by the runtime UID, regular files, mode `0400` or `0600`, with no
symlink. The public Agent Card contains static capability information only.

The server bounds concurrent I/O handlers, request bytes, headers and read
deadlines. The worker command runs a dispatcher with bounded independent
assignment processes. Configure `scheduler.max_workers` and
`scheduler.max_per_principal` in the deployment configuration (defaults: four
and two); workflow and resource limits share the same durable capacity store.
Use `bridge worker --processes N` to lower the assignment-process bound and
`--drain-seconds N` to set the graceful shutdown interval (default: 30 seconds).
Principals are interleaved while each principal's pending assignments keep
their creation order. Composition leaves and ordinary tasks consume the same
execution capacity.

Each assignment retains its own task lock and process-slot lock. Surviving
assignment processes continue to count against the bound if the dispatcher
dies and restarts. Only the dispatcher publishes worker readiness. Shutdown
stops new assignments, drains active work, then terminates overdue workers;
their supervisors retain run and capacity ownership until descendant cleanup
finishes. Interrupted external effects retain their uncertainty. `--once`
attempts one assignment and exits.

This profile remains one Linux host with SQLite storage and local file locks.
Multiple network callers do not change frozen MAS routing permissions; these
processes do not provide controller high availability.

## Recovery and cancellation

A submission and every continuation revision have one durable run identity and
one root request. Restart replays that request through the existing journal.
External dispatch still requires a host invocation and deployment reservation.
Every remote poll, cancellation and continuation uses a fresh admitted call;
there is no background thread holding an expired adapter context.

`queued`, `running`, `input_required`, `auth_required`, `completed`, `failed`,
`cancelled` and `unknown` are distinct service states. `completed` denotes the
registered operation's result. A remote-job submission result can contain an
active remote receipt; its remote completion is a separate observation.
Unresolved application results default to failure; interrupted states require
an explicit workflow classifier and continuation contract.

Cancelling queued work or an inactive local continuation closes that local
task. Cancelling running work records intent and uses supervised cleanup;
uncertain effects remain unknown. Local cancellation never establishes that a
remote job stopped. Remote adapters preserve cancellation intent until an
identified peer observation confirms its state. The first terminal observation
is immutable; stale polls cannot reopen it.

A lost first remote receipt cannot trigger another submission. Explicit,
evidence-backed attachment/reconciliation uses the peer's exact identity.
Uncertain cancellation or update effects also need evidence and a fresh
observation before later work proceeds. These administrative decisions are
trusted input, not proof that a provider performed the claimed effect.

Read [operations](operations.md) for backup, retention, audit-only restores and
host invocation resolution. All new stores participate in those controls.
Unknown and active remote effects retain the host records they need even when
the local submission has completed. Durable event reads use an inclusive
history store and an exclusive `after` cursor; replaying a read is harmless.

## Design and verification

The service is a Facade with separate public lifecycle, public query and trusted
worker capabilities. Protocol clients adapt their wire semantics; SDK connectors own
framework integration. Version-aware repositories own persistence, explicit
state transitions own uncertainty, and execution factories bind authenticated
service identity to the existing adapter Strategy. Rust's atomic task/message
transactions and protocol version remain unchanged.

The simpler alternative—independent HTTP wrappers around `Runner.request`—
cannot preserve ordinal replay or deduplicate a lost submission response.
A distributed scheduler replacement would add a new execution model. The shared
service addresses the required failure boundary with a bounded single-host
queue and short transactions. Tests cover interface substitution, actual Rust
execution, protocol schemas, HTTP/stdio exchanges, cancellation, process death,
cross-principal isolation, exact remote identities and restored stores. SDK tests
exercise real LangGraph checkpoints and a pinned real Temporal server. The
[finite models](https://github.com/emmett08/mas/blob/main/formal/bridge-notes.md) establish their stated bounded
properties, not unrestricted implementation or provider correctness.

Outbound task I/O uses a per-task process lock while the remote intent and
receipt use short SQLite transactions. A slow peer therefore leaves inspection
and unrelated task lifecycle operations available. Concurrent operations on the
same remote task receive `operation_busy` before dispatch; administrative
resolution cannot race a live response. Process death releases ownership but
leaves its committed unknown intent, so a later caller cannot silently submit
again. Receipt persistence checks the original invocation, binding and peer
identity before accepting a late completion. A shared deployment lock remains
held across the call so maintenance cannot take a partial snapshot.

See [HTTP transport policy](http-transport-policy.md) for explicit corporate
proxy/private-CA configuration, and [artefact exchange](artifacts.md) for
configured authenticated chunk transfer and typed scientific array payloads.

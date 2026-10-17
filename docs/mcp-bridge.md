# MCP bridge

The bridge implements MCP **2026-07-28** and its **2026-07-28 Tasks extension**.
It uses the durable task service for ingress and the shared admitted remote-task
service for egress. The Rust task/message transaction boundary is unchanged.

## Compatibility profile

| Area | Implemented behaviour |
|---|---|
| Discovery | `server/discover`, `tools/list`, `ping`; private, immediately stale discovery cache |
| Inbound tools | `mas.submit`, `mas.status`, `mas.result`, `mas.cancel`, `mas.update`, `mas.events`, `mas.list` |
| Asynchronous work | Per-request Tasks negotiation; `tasks/get`, `tasks/update`, `tasks/cancel` |
| Transport | Stdio; Streamable HTTP POST. Outbound HTTP accepts JSON and bounded request-scoped SSE responses. |
| Version/capabilities | Required on every request, with required matching HTTP routing headers |
| Continuation | Explicit form/URL elicitation carried by Tasks; all outstanding answers must be supplied together |
| Data | Exact bounded integer JSON, including nested values; fractional values are rejected, never rounded |
| Schemas | JSON Schema 2020-12 using pinned `jsonschema`; local references only |
| Authentication | HTTP transport receives a separately acquired bearer token; the server authenticates each request before selecting a principal. Stdio principal is trusted local configuration. |

Legacy `initialize`/sessions, GET/SSE sessions, `Last-Event-ID`, sampling, roots,
prompts, resource retrieval and notification subscriptions are not advertised.
Durable event retrieval uses `mas.events`. The client preserves returned content
and resource links but never fetches their URLs. The profile deliberately refuses
non-task multi-round-trip tool continuations: an ambiguous initial result stays
subject to reconciliation instead of triggering an automatic second tool call.

The default wire ceiling is 65,536 UTF-8 bytes. The MAS codec additionally bounds
nesting and integer size. A remote schema must fit the frozen configuration
identity; external schema references are rejected before invocation. Larger
results need an application-defined, bounded artifact reference whose retrieval
has its own authorisation and validation. Result text is duplicated for small
structured results only; larger results retain their exact `structuredContent`.

Install the locked bridge dependencies used by CI/container builds:

```sh
python -m pip install --require-hashes -r host/requirements-bridges.txt
python -m pip install --no-deps ./host
```

## Exposing MAS

The HTTP server, exclusive worker and stdio entry point use the same configuration
and persistent deployment directory:

```sh
python -m mas_host bridge serve --config /absolute/bridge.json --state-dir /absolute/mas-state
python -m mas_host bridge worker --config /absolute/bridge.json --state-dir /absolute/mas-state
python -m mas_host bridge stdio --config /absolute/bridge.json --state-dir /absolute/mas-state --principal alice
```

The worker is a separate process. A network handler commits a submission and
returns its durable receipt; it does not synchronously execute the Runner.
For local development only, use the server's explicit `--allow-loopback-http`
option with a literal loopback `public_url`. Production terminates TLS at the
configured boundary. See [bridge configuration](bridges.md) for authentication,
allowed hosts/origins, protected-resource metadata and Compose deployment.

An initial tool request has this shape:

```json
{
  "jsonrpc": "2.0",
  "id": "request-1",
  "method": "tools/call",
  "params": {
    "_meta": {
      "io.modelcontextprotocol/protocolVersion": "2026-07-28",
      "io.modelcontextprotocol/clientCapabilities": {
        "extensions": {"io.modelcontextprotocol/tasks": {}}
      }
    },
    "name": "mas.submit",
    "arguments": {
      "request_key": "batch-42",
      "workflow": "echo",
      "input": {"value": 42}
    }
  }
}
```

On HTTP, send `Content-Type: application/json`,
`Accept: application/json, text/event-stream`,
`MCP-Protocol-Version: 2026-07-28`, `Mcp-Method: tools/call` and
`Mcp-Name: mas.submit`, plus the caller's bearer credential. Never put that
credential in arguments or metadata.

`mas.submit` requires the Tasks capability before it creates any work. Preserve
`request_key` and the same canonical input when recovering a lost reply. A
changed input under the same key conflicts. The response's `taskId` is usable
immediately; poll it through `tasks/get`, with `Mcp-Name` equal to that task ID.
Every poll repeats the required metadata. A guessed task ID never grants access.

A task in `completed` may hold a tool result with `isError: true`. In this bridge,
MAS business failure is represented that way; it is not relabelled as a JSON-RPC
failure. An unknown external outcome remains `working` with
`_meta["org.mas/state"] == "unknown"`. A cancellation acknowledgement contains
only `resultType: "complete"`; subsequently observed work can still complete.

A workflow can return `result.input_requests` when its classifier reports
`input_required`. Entries must be explicit `elicitation/create` requests.
Clients must declare the relevant elicitation capability on each request.
Exposed request IDs include the durable revision (`r0:answer`, `r1:answer`) so a
delayed answer cannot satisfy a later request. `tasks/update` validates accepted
form content against its requested schema and passes
`{"input_responses": {original_key: response}}` to the trusted continuation policy.
Unknown/already-satisfied keys are ignored. Partial sets of outstanding answers
are rejected without advancing the workflow.

## Calling a remote MCP tool

Freeze the endpoint, tool definition, schema, spending allowance and adapter
implementation version in trusted deployment configuration. Remote discovery
must not choose routes, alter topology or add capabilities. `MCPClient.discover()`
and `list_tools()` support explicit operator discovery; they perform network I/O
and must be admitted if executed inside an MAS workflow.

The following factory binds each remote adapter to the actual run directory and
to the authenticated owner, workflow version and actor. It never derives its
namespace from input supplied by a caller or remote peer.

```python
from pathlib import Path

from mas_host import Outcome
from mas_host.bridges.authentication import read_token_file
from mas_host.bridges.mcp_client import MCPClient, MCPRemoteClient, MCPToolAdapter
from mas_host.bridges.mcp_transport import StreamableHTTPTransport
from mas_host.bridges.remote import RemoteTaskService, RemoteTaskStore
from mas_host.task_service import DurableTaskService, Workflow

class Echo:
    def handle(self, context, request):
        return Outcome(request.input)

TOOL = {
    "name": "multiply",
    "inputSchema": {
        "type": "object",
        "properties": {"n": {"type": "integer"}},
        "required": ["n"],
        "additionalProperties": False,
    },
}
TOPOLOGY = {
    "agents": ["caller", "remote"],
    "edges": [["caller", "remote"], ["remote", "caller"]],
    "application": "agent-task/1",
    "mailbox_capacity": 10,
    "max_messages": 100,
    "max_bytes": 100_000,
    "max_calls": 8,
    "wall_seconds": 120,
}

def adapters(binding):
    transport = StreamableHTTPTransport(
        "https://mcp.example.org/mcp",
        token=read_token_file(Path("/run/secrets/remote_mcp_token")),
        timeout_seconds=30,
    )
    remote = MCPRemoteClient(MCPClient(transport, tasks=True), TOOL)
    jobs = RemoteTaskService(
        RemoteTaskStore(binding.state_dir), remote,
        namespace=binding.remote_namespace("remote"),
    )
    return {
        "caller": Echo(),
        "remote": MCPToolAdapter(jobs, binding.run_dir,
                                 reservation_microusd=100_000),
    }

workflow = Workflow(
    "remote-multiply/1", TOPOLOGY,
    {"caller": Echo(), "remote": Echo()},
    "caller", "remote", "mcp.execute",
    adapter_factory=adapters,
)
service = DurableTaskService(
    Path("/absolute/mas-state"), {"remote-multiply": workflow},
    {"alice": {"remote-multiply"}},
)
submission = service.submit(
    "alice", "operation-42-submit", "remote-multiply",
    {"operation": "call", "key": "remote-job-42", "arguments": {"n": 7}},
)
service.run_once()
receipt = service.result("alice", submission["task_id"])
```

The `example.org` endpoint/tool are illustrative deployment configuration, not a
hosted service supplied by MAS. Keep the token in a private regular file with
mode `0600`; supply it as a container secret. The token never enters the frozen
provider descriptor, journals or request payloads. Upstream OAuth acquisition
and refresh belong to the organisation's identity integration; the bridge does
not follow authentication URLs advertised by an untrusted peer.

The completed *MAS operation* contains a remote-job receipt. Its `state` can
still be `running`; completion of submission is not completion of remote work.
Use the same workflow/owner/job key and a fresh outer request key to observe it:

```python
poll = service.submit(
    "alice", "operation-42-poll-1", "remote-multiply",
    {"operation": "poll", "key": "remote-job-42"},
)
service.run_once()
observation = service.result("alice", poll["task_id"])
```

For task observations, `observation["result"]["mcp_task"]` retains the peer's
operational metadata, including any `pollIntervalMs`; a completed observation's
`tool_result` retains the exact tool result and its `isError` flag. An immediate
tool response is stored directly as the receipt's `result`. The receipt's outer
`state` reports the mapped business outcome in both cases.

The four public dispatch actions have these exact inputs:

| `operation` | Additional fields |
|---|---|
| `call` | `key`, `arguments` |
| `poll` | `key` |
| `cancel` | `key`, `request_key` |
| `update` | `key`, `request_key`, `input_responses` |

Each dispatch enters `context.invoke(...)` and verifies the executing invocation
and deployment admission before sending. Every lifecycle observation receives
its own admission; no background process keeps an expired adapter context.
Reuse the inner `request_key` for uncertain cancellation/update recovery. Honour
the peer's suggested polling interval when scheduling observations.

For a trusted local MCP server, replace the transport with
`StdioTransport(["/absolute/python", "/absolute/server.py"], cwd=Path("/private/work"))`.
Pass a narrowly selected `environment` and, separately, `secret_environment` if
needed. Ambient host credentials are not inherited. Stdio servers must stay in
the MAS guardian's process group; they are trusted adapters, not isolated
untrusted executables. Use supervised execution for descendant cleanup after
host death.

## Uncertain effects and operator reconciliation

A lost initial reply leaves the remote job and host invocation unknown. Reopening
the stores, retrying the operation or changing an outer request key does not send
the original remote effect again. MCP supplies no universal initial-call
idempotency guarantee, so the bridge does not infer one from a JSON-RPC ID.

When independent evidence identifies the exact remote task, a trusted operator
can use the explicit `mcp.attach` adapter operation with
`{"key": "remote-job-42", "remote_id": "the-exact-task-id", "evidence": {...}}`.
It must execute in a separately admitted Runner with the original trusted
namespace and the new run's directory. It polls that exact remote ID before
attaching. The public `mcp.execute` dispatcher deliberately rejects `attach`.
Attaching a receipt does not itself resolve the original host invocation.

Use `RemoteTaskService.status(key)` to obtain the verified logical-key receipt.
After reviewing its evidence, record that exact output through the existing
operator command:

```sh
python -m mas_host admin --state-dir /absolute/mas-state resolve \
  /absolute/mas-state/ORIGINAL_RUN CALL_ID --decision completed \
  --evidence /private/reconciliation-evidence.json \
  --output /private/verified-receipt.json
```

Then call `DurableTaskService.recover(principal, task_id)` for the interrupted
outer service task. Recovery reuses its original run and invocation. For a lost
update/cancel reply, `RemoteTaskService.resolve_operation(...)` accepts an explicit
evidenced `applied` or `not_applied` decision; it never sends another request.
The corresponding host invocation still requires its own supported resolution.
A bare timeout, a missing response or a remote `404` is not evidence of no effect.

## Verification and sources

`test_mcp_bridge.py`, `test_mcp_client.py` and `test_mcp_review.py` cover durable
identity and principal ownership, admission-before-dispatch, actual Rust Runner
replay, lost HTTP receipts, cancellation races, exact schema handling, real
stdio/HTTP/SSE, thread-compatible deadlines and continuation key collisions.
Inbound discovery/tool responses are also checked against the pinned upstream
JSON Schema. The fixture records its source and SHA-256 alongside the upstream
licence notice. No live external MCP service is contacted by these tests.

- [MCP base protocol](https://modelcontextprotocol.io/specification/2026-07-28/basic)
- [Versioning](https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning)
- [Streamable HTTP](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)
- [Tools and schemas](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)
- [Tasks extension](https://tasks.extensions.modelcontextprotocol.io/specification/2026-07-28/tasks)

# A2A 1.0 bridge

This is the transitional Python-host bridge for the **Nehemiah protocol**
(existing MAS wire and package identifiers are unchanged). New host development
belongs in [mas-host](https://github.com/emmett08/mas-host). The Python host will
be removed only after its required integrations have migrated; this guide does
not imply feature parity between the two hosts.

The bridge implements the A2A **1.0 JSON-RPC polling binding**, checked against
the v1.0.1 specification and its published generated schema. It uses
`SendMessage`, `GetTask`, `ListTasks` and `CancelTask`, `ROLE_USER`/`ROLE_AGENT`,
and `TASK_STATE_*` values. The older 0.3 `message/send` binding is not enabled.
The Agent Card is served at `/.well-known/agent-card.json`. Every RPC request
uses `A2A-Version: 1.0`; caller authentication and operation scopes are enforced
by the HTTP boundary before a bridge method runs.

The card advertises JSON input/output, bearer authentication, one JSON-RPC
interface, and **no streaming or push notifications**. An authenticated extended
card is available when the task service implements discovery. It uses
the normative `securityRequirements` field; some prose examples in the A2A
website still show the older `security` field. The frozen upstream schema tests
reject that older field.

The public card describes the service without listing private registrations.
The extended card derives standard `AgentSkill` entries from the caller's
available registrations; complete pinned contracts remain in the service
extension. A skill description grants no permission and does not attest output
quality. The existing service rechecks grants during invocation.

`urn:mas:bounded-json:1` describes the single JSON data part, exact-integer
numeric profile, request bound and reconciliation rule. The service and lifecycle
extensions retain their existing URIs. Clients select supported versions with
`A2A-Extensions`; unknown versions fail before task dispatch. Successful HTTP
responses acknowledge the selected extensions. Managed MAS peers send their
pinned service and bounded-JSON profiles. Optional profiles preserve existing calls
without this header. An embedding application may pass `required_extensions`
to `A2ABridge`; those extensions are marked required in both cards and missing
support produces A2A error `-32008`. Public card retrieval remains available
before negotiation. Do not make an optional profile required in an existing
deployment without migrating its callers.

## Inbound use

Send a single `application/json` data part. Its object selects an authorised
registered workflow and supplies that workflow's input. The input retains the
host's integer-only JSON, depth and byte limits. Fractional quantities need an
application-declared rational or decimal-string representation. URL, binary and
text input parts are not accepted by this application profile. No artifact URL
is fetched automatically.

```json
{
  "jsonrpc": "2.0",
  "id": "request-1",
  "method": "SendMessage",
  "params": {
    "message": {
      "messageId": "document-2026-10-14-1",
      "contextId": "document-review-session",
      "role": "ROLE_USER",
      "parts": [{
        "mediaType": "application/json",
        "data": {"workflow": "invoice", "input": {"invoice": [{"quantity": 2, "unit_price": 17}]}}
      }]
    },
    "configuration": {"returnImmediately": true}
  }
}
```

Use the registered workflow's actual input schema; the example above illustrates
wire structure. A successful asynchronous response has `result.task`, containing
a server-assigned `id`, durable `contextId` and current status. `GetTask` returns
the Task directly. Completed application output appears in a JSON artifact.

```json
{"jsonrpc":"2.0","id":"poll-1","method":"GetTask","params":{"id":"task-returned-by-server","historyLength":0}}
```

`messageId` is durably deduplicated within the authenticated principal. Repeating
the same message obtains the same task; changing its input, workflow, context or
message metadata under that identity is rejected. The principal is supplied by
authentication, never by a message body. Supplied context identifiers are kept as
conversation groupings; task ownership is checked independently. Omitting a
context produces a stable server context. History is omitted in this bounded
profile; A2A permits a server to return fewer history entries than requested.
Wire message, context, task and artifact identifiers are opaque nonempty UTF-8
strings bounded to 1024 bytes; slashes, Unicode and JSON-escaped characters are
preserved. Internal logical keys and admitted call IDs retain their separate
restricted local format. Long or otherwise opaque inbound message IDs receive
stable hashed local request keys, while existing local-format message keys keep
their prior identity.

`returnImmediately` defaults to false in A2A. The handler then waits, within its
configured response deadline, for terminal or interrupted state while a separate
worker executes the task. It never executes the Runner in a request thread.
A durable unknown outcome remains unresolved during this wait; it is available
through polling but is not returned as a successful blocking result.
A response deadline leaves the task and message identity intact. Repeat that
same message, or poll a previously obtained task ID. An HTTP timeout does not
cancel the task or authorise another submission identity.

`ListTasks` implements principal-scoped keyset pagination ordered by latest
status timestamp and task ID, with context, state and timestamp filters. It
returns `tasks`, `nextPageToken`, `pageSize` and `totalSize`; artifacts are omitted
unless `includeArtifacts` is true. A page token is bound to its original filters
and principal. It is a cursor, not an access credential.

To continue an interrupted task, send a fresh `messageId` with its `taskId`, the
same context if supplied, and new workflow input. The workflow must declare its
continuation function. A terminal task cannot be restarted by continuation.

`CancelTask` requests cancellation. It returns a cancelled Task only when the
local task is actually cancelled. If execution is still active, the response is
`TaskNotCancelableError` (-32002) and cancellation intent remains durable; poll
for the outcome. Cancelling the local task does not assert that a separate
remote job has stopped.

## Outbound use and trusted scope

`A2AClient` implements protocol conversion. `RemoteTaskService` owns the durable
remote lifecycle. `A2AAdapter` invokes that service through the existing
`AdapterContext.invoke` admission boundary. Bind it with a workflow adapter
factory so the host directory and namespace come from the scheduler:

```python
from pathlib import Path
from mas_host import Outcome
from mas_host.bridges.a2a import A2AAdapter, A2AClient
from mas_host.bridges.authentication import read_token_file
from mas_host.bridges.remote import RemoteTaskService, RemoteTaskStore
from mas_host.bridges.remote_http import BoundedJSONHTTP

class Entry:
    def handle(self, context, request):
        return Outcome(request.input)

class Route:
    def handle(self, context, request):
        # This registered workflow explicitly grants these operations.
        operation = request.input["operation"]
        if operation not in {"a2a.submit", "a2a.get", "a2a.cancel", "a2a.update"}:
            raise ValueError("unsupported operation")
        return context.request("remote", operation, request.input["input"])

def adapter_factory(binding):
    transport = BoundedJSONHTTP(
        "https://trusted-peer.example/a2a",
        credential=read_token_file(Path("/run/secrets/a2a_token")),
        timeout_seconds=20,
    )
    client = A2AClient(transport, client_identity="organisation-mas-service")
    service = RemoteTaskService(
        RemoteTaskStore(binding.state_dir), client,
        namespace=binding.remote_namespace("remote"),
    )
    return {
        "entry": Entry(), "route": Route(),
        "remote": A2AAdapter(service, host_run_dir=binding.run_dir,
                             reservation_microusd=1000),
    }
```

Register a `Workflow` with actors `entry`, `route`, `remote`, permitted edges
`entry -> route -> remote`, sender `entry`, recipient `route`, operation
`dispatch`, an explicit version, and that `adapter_factory`. The initial
`adapters` mapping supplies the same actor set with valid adapter objects; the
factory supplies the execution-bound instances. Changing peer configuration,
client identity or adapter behaviour requires a new workflow version.

The namespace is derived from the authenticated principal, registered workflow
identity and remote actor. It is stable across fresh host runs and distinct
between principals. It must never come from application input. Remote logical
keys can therefore be reused by different callers without revealing or
controlling each other's tasks.

The adapter applies the `mas-a2a-v1` wire identity policy before persisting a
submission or continuation. Outgoing message IDs are derived from the trusted
namespace, local task key, operation, original message ID and, for continuation,
the durable operation request key. Two owners sharing one peer credential therefore
send distinct identities even when their caller IDs and application input match.
Exact retries and reopened adapters preserve the same binding. Explicit initial
context IDs are local conversation aliases: the adapter scopes them by namespace,
so an alias groups tasks within one owner but cannot select another owner's
upstream conversation.

A supplied continuation context must either equal the peer identity saved in its
owned receipt or be the original local alias that maps to it; any other context
is rejected. An omitted context stays omitted and the exact saved task ID selects
the task. This preserves the operation binding if a reply first establishes a
previously absent context. Polling and cancellation also use the exact saved
peer task ID.
Supplying a task ID on an initial submission is rejected, and a continuation's
task ID must match the saved task. The bounded adapter rejects `referenceTaskIds`
because caller-supplied upstream task references would confer authority through
the shared peer credential. The lower-level `A2AClient` is a protocol mapper;
trusted callers using it directly must establish their own ownership policy.

This changes the persisted client profile to `bounded-json-polling/2`. Receipts
created under `/1` are not silently migrated: the descriptor check rejects their
polls, cancellations and continuations, and their submission bindings cannot be
reused with the new wire payload. No rejected operation dispatches to the peer.
Deploy this pre-merge incompatible profile with an explicit workflow/configuration
version change. Existing `/1` receipts require operator investigation and evidence
of the exact task ownership and outstanding effects before any migration or
reconciliation. A new namespace or key is not permission to resubmit old work;
historical peer IDs may already be shared across owners. Do not relabel their
descriptors or blindly retry them under `/2`.

The adapter operations accept these inputs:

| Operation | Input |
|---|---|
| `a2a.submit` | `{"key":"local-job-key","message":{...A2A ROLE_USER message...}}` |
| `a2a.get` | `{"key":"local-job-key"}` |
| `a2a.cancel` | `{"key":"local-job-key","request_key":"cancel-1"}` |
| `a2a.update` | `{"key":"local-job-key","request_key":"turn-2","message":{...}}` |

The return value is a durable remote receipt. Its state describes the remote
work. A returned receipt does not make the surrounding local submission task a
completed remote business result. Applications must explicitly classify or
continue their own workflow accordingly. A direct A2A Message is represented as
a completed result without a remote task ID.
Task responses may omit the optional context ID. Once a context is observed,
later omissions preserve it and conflicting supplied contexts are rejected.

Every submission, observation, cancellation and continuation receives a fresh
MAS call admission. The configured reservation applies to each invocation and
is an admission allowance, not a remote provider charge guarantee. The client
uses one configured endpoint, no redirects and no automatic retries. Its HTTP
worker has a wall-clock deadline, can run from a caller thread and is killed and
reaped on timeout. HTTPS verifies the peer certificate. Plain HTTP is permitted
only for explicitly enabled literal loopback test endpoints.

## Lost receipts and operator recovery

`remote-tasks.sqlite` records namespace, peer descriptor, immutable submission
input, admitted host run/call, remote task/context IDs, observations,
cancellation intent and operation history. Credentials never enter descriptors
or these records. The database rejects unsupported schemas before writing;
restored deployments remain audit-only. Run directories are stored relative to
the deployment. Backup and retention include remote state and hold unresolved
runs.

An initial submission intent is committed as unknown before transport dispatch.
A lost response, malformed receipt or process death cannot cause automatic
resubmission. Repeating the logical key returns the recorded receipt or an
unknown-outcome error. A2A itself does not universally require message-ID
deduplication, so a peer cannot be assumed to recover the first task identity.

After independently establishing the exact remote task ID, an operator can use
`service.attach(key, remote_id, evidence={...}, context=fresh_context,
host_run_dir=current_run_dir, reservation_microusd=...)`. This is a separately
admitted exact-task observation; it records the operator evidence and checks
the returned identity. The original host invocation may still need the
existing host-journal resolution procedure before that host workflow resumes.

A lost update or cancellation reply retains its operation intent and is never
resent automatically. A new cancellation is rejected while a mutating operation
remains unresolved; its intent cannot overwrite an unknown continuation.
Resolve that operation with evidence before requesting cancellation. Polling may learn task state but cannot prove whether a
specific continuation was applied. After obtaining independent evidence, use:

```python
service.resolve_operation(
    "local-job-key", "update:turn-2",
    evidence={"peer_audit_receipt": "reviewed-evidence-id"},
    decision="applied",  # or "not_applied", supported by the evidence
)
```

This trusted operator operation sends no remote request. An applied continuation
requires a fresh admitted observation before another continuation. A
not-applied decision permits a new operation key; the old key remains resolved
and cannot resend the old effect. Cancellation intent blocks fresh
continuations. Remote identity or descriptor changes are rejected.

`service.inspect(key)` exposes the owner-scoped operation keys and audit entries.
A lost read-only poll or attachment observation can be closed with
`resolve_operation(..., decision="observation_unavailable", evidence={...})`.
This records the missing observation without changing task state, clearing an
unresolved submission, or issuing another network request.

## Evidence and limits

The offline fixture `host/tests/fixtures/a2a-v1.0.1.schema.json` is the unmodified
upstream generated bundle. Its adjacent provenance record fixes source URL,
retrieval date and SHA-256; `A2A-LICENSE.txt` retains the Apache-2.0 licence.
Tests validate cards, send responses, Tasks, Messages and listing responses
against that bundle, reject older incompatible wire shapes, and exchange real
HTTP messages with the local bridge. Other tests cover principal isolation,
continuations, default blocking, cancellation races, lost replies, changed
identities and storage-version rejection. These are bounded implementation and
schema checks, not certification against every third-party A2A agent.

References: [A2A v1.0.1 specification](https://a2a-protocol.org/v1.0.1/specification/),
[normative proto](https://github.com/a2aproject/A2A/blob/v1.0.1/specification/a2a.proto),
[official generated schema](https://a2a-protocol.org/v1.0.1/spec/a2a.json).

### MAS lifecycle inspection extension

Authenticated `GetExtendedAgentCard` advertises
`urn:mas:lifecycle-inspection:1`. Its explicitly namespaced JSON-RPC methods are
`mas/InspectTask` (`task_id`, `path`, optional `revision`), `mas/InspectGraph`
(`task_id`, optional `section`, `offset`, `limit`), `mas/EventsPage` (`task_id`,
optional `after`, `limit`), `mas/ListTaskPage` (the unified page/filter fields),
and `mas/RecoverTask` (`task_id`). These are MAS extensions, not standard A2A
methods. Reads require `mas:read`; recovery requires `mas:update` and retains the
service's evidence and ownership checks. Ordinary A2A send/get/list/cancel
messages keep their existing contract. Graph pages contain structure only;
authorised leaf inspection retrieves application output separately.

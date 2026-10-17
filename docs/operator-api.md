# Authenticated operator API, CLI and console

The same `OperatorService` implements operator actions for HTTP, the portable
Python client, the CLI and the browser console. Task owner access and operator
access are independent. Every request requires the `mas:admin` token scope and
an explicit operator grant in trusted deployment configuration:

```json
{
  "operators": {
    "support": {"owners": ["alice"], "deployment_admin": false},
    "deployment-admin": {"owners": ["alice", "bob"], "deployment_admin": true, "manage_peers": false}
  }
}
```

Add this object to the normal bridge configuration. Owners are exact identities;
wildcards are rejected. Deployment administrators still need an explicit owner
grant to inspect or resolve that owner's tasks. Ordinary task grants are checked
again by the underlying task service. Possessing `mas:admin` without a configured
operator grant confers no access. Peer writes additionally require `manage_peers: true`; deployment administration alone does not permit them. Operator identity always comes from the verified
token; request bodies cannot choose it, local paths or factories.

Create a private administrator token and verifier with:

```sh
python -m mas_host bridge init-auth --principal deployment-admin \
  --scope mas:admin --token-file ./operator.token --verifier-file ./operator-verifier.json
```

Merge the resulting verifier record into the service's trusted credential
configuration. Keep the token private. The normal bridge server installs the
operator port when `operators` is configured; no separate server is needed.

## Operations

Send integer JSON to `POST /service/operator/OPERATION`. Successful responses
have the same `{"result": ...}` envelope as the task API. Unknown fields fail
validation. Listing responses include bounded pages and continuation offsets.

| Operations | Required grant | Effect |
| --- | --- | --- |
| `capabilities` | Any operator | Show exact owner grants and available operations. |
| `registrations`, `tasks`, `task` | Named owner | Discover registrations, find tasks and inspect their current state. |
| `recovery_inbox`, `recovery_query` | Named owner | Inspect uncertain invocations or use a separately registered read-only evidence query. |
| `recovery_resolve` | Named owner | Record a supported completion or confirmed-no-effect decision, with evidence and an idempotency key. |
| `request_epoch_status`, `request_epoch_retire` | Named owner | Inspect or permanently close a drained explicit request epoch; optionally prune eligible settled receipts. |
| `deadline_extend` | Named owner | Extend an open business, queue or pause clock using a revision and attributed reason. |
| `budget`, `budget_amend` | Deployment administrator | Read or increase lifetime limits without reducing recorded allocations. |
| `periods`, `period_configure` | Deployment administrator | Inspect or install fixed allocation windows; preserve prior reservations. |
| `charge_record`, `charge_reconcile` | Deployment administrator | Record attributed invoice observations and compare them with an admitted invocation's reservation. |
| `storage`, `archive_preview` | Deployment administrator | Inspect storage/admission and preview task, workflow or run retention eligibility. |
| `archive` | Deployment administrator and explicit owner grant | Archive one eligible task or composition under an immutable request key and reason. |
| `artifact_namespaces`, `artifact_retire` | Deployment administrator | Inspect namespace closure or permanently retire a drained namespace without deleting bytes. |
| `artifact_links`, `artifact_bind`, `artifact_release` | Deployment administrator and explicit owner grant | Inspect consumer edges, audit a legacy task binding or release an exact link. |
| `command_capacity`, `command_capacity_amend` | Deployment administrator | Inspect shared command receipt capacity or apply an audited grow-only amendment. |
| `definition`, `definition_retire` | Deployment administrator | Inspect exact definition availability or retire unused historical identities. |
| `peer_status` | Deployment administrator or peer manager | Inspect bounded registered peer versions. |
| `peer_register`, `peer_revoke` | Explicit `manage_peers: true` | Register pinned peer bindings or revoke a version; credentials stay outside request bodies. |
| `worker_status`, `worker_drain` | Deployment administrator | Inspect or change persistent local dispatcher drain state. |

Recovery resolutions do not resume execution. The task owner still calls normal
`recover` after resolution. Deadline extensions preserve task and invocation
identity and cannot extend active provider execution. Charge observations never
refund allocations or claim the provider invoice is complete. Retention previews
never delete files. Request epoch retirement records the authenticated operator and rejects unfinished or uncertain work. Definition retirement rejects current aliases and references
from unfinished work. Definition availability reports retained source and current
registrations; dispatch re-verifies implementation bytes.

The recovery inbox paginates uncertain entries and remote observations separately
using the same `offset` and `limit`. Its cancellation fields distinguish requested
cancellation, stopped future dispatch, local process stop, remote cancellation
and unresolved effects. Evidence queries are available only when trusted
application construction supplies named `ReconciliationQuery` registrations.

## CLI and Python client

Every operation is available without the controller's filesystem. For example:

```sh
python -m mas_host bridge operator capabilities \
  --endpoint https://mas.example --operator deployment-admin --token-file ./operator.token

python -m mas_host bridge operator recovery_inbox \
  --endpoint https://mas.example --operator deployment-admin --token-file ./operator.token \
  --input-file ./inbox-request.json
```

`inbox-request.json` contains logical identities only:

```json
{"principal":"alice","task_id":"TASK_ID","offset":0,"limit":20}
```

The Python client uses the identical route and contract:

```python
from pathlib import Path
from mas_host.bridges.service_client import TaskServiceClient

client = TaskServiceClient("https://mas.example", token_file=Path("operator.token"),
                           principal="deployment-admin")
status = client.operator("deployment-admin", "worker_status", {})
receipt = client.operator("deployment-admin", "worker_drain", {
    "request_key": "maintenance-2026-10-14", "reason": "Deploy verified worker version",
    "expected_revision": status["revision"], "draining": True,
})
```

On Windows, or when using a credential manager, pass a trusted callable as
`token_provider` instead of `token_file`. The client calls and validates it on
every request, allowing credential rotation. The two credential sources are
mutually exclusive; provider failures expose only a fixed error code.

Drain uses a durable revision and request identity. Repeating the same request
returns its original receipt; changing its payload under the same key fails. An
exclusive control barrier waits for any already-started process launch before
committing the drain receipt. Subsequent local dispatcher launches stop until a
revision-checked resume. Already granted effects continue and retain their existing
identity, allocation and evidence obligations. The dispatcher remains healthy
with phase `draining`, while admission reports closed. Restart preserves drain;
audit-only restores permit inspection and reject changes.

## Browser console

Open `/console`, select **Operator**, and connect with the administrative token.
The console displays authorised owners and provides task search, recovery forms,
deadline and budget amendments, allocation periods, charge observations, storage
and retention previews, definition inspection and worker drain/resume controls.
Use the exact revision returned by the corresponding status or amendment.

All data uses authenticated same-origin API calls. Tokens remain only in page
memory; disconnect, page close or reload clears them. Operator inputs, evidence,
outputs and displayed state are cleared on disconnect. The console renders data
as text, rejects unsafe browser integers and keeps its restrictive content
security policy. The Python client supports larger integer values.

Graph inspection is available to task owners as `POST /service/graph`,
`TaskServiceClient.inspect_graph`, `mas-client ENDPOINT graph`, MCP `mas.graph`,
and the A2A lifecycle-inspection extension. Operators use the owner-scoped
`graph` operation. Select `nodes`, `attempts` or `scopes`, with an offset and a
limit of 1–100; `next_offset` advances through bounded structural pages. The
console provides the same graph and metadata/state search controls. Graph
responses exclude application inputs and outputs; leaf inspection retains its
separate owner-checked result contract. MCP `mas.page` exposes the unified
search/cursor operation.

Shared-profile ingress readiness reads the configured PostgreSQL authority
without provisioning, renewing a lease, loading registration factories or
opening local SQLite. Use `bridge worker-health --config CONFIG --state-dir
CACHE` for shared workers. Local monitor files attest only to that process's
liveness; current native epoch/lease and admission state remain authoritative.

### Authorised history archival

`archive` takes `kind` (`tasks`, `workflows`, or shared standalone `runs`), `principal`, `task_id`,
`request_key`, `reason`, and `older_than_seconds`. The server injects the
operator identity. Preview is advisory; application rechecks terminal state,
all execution attempts, effect uncertainty, retention links and unacknowledged
event deliveries while holding the authoritative transaction or maintenance
lock. Old human-pause revisions can be archived only after a later revision
finishes and their effect references are settled.

Local archival retains a recoverable administrative intent before changing the
owning repository. Shared archival atomically compacts task/composition detail,
run journals, protocol message/result bodies, worker result/evidence bodies,
and corresponding native command receipts. It preserves original identifiers,
digests, request mappings, charges, clock/amendment records and compact attempt
references. Archived command replies explicitly return `command_result_archived`;
they never rerun the old command. Exact worker receipt replay retains its prior
acknowledgement without reopening artefact retention. A new receipt for an
archived worker job is rejected. Pending webhook bytes block archival until
acknowledgement, and expired event cursors require resynchronisation.

Historical allocation is unchanged. Archival frees detailed-history capacity;
it does not refund lifetime or period consumption. Legacy installations with
unmapped native command receipts reject payload archival until their provenance
can be established, instead of guessing which active work owns those receipts.

For shared standalone run archival, `task_id` carries the original run ID.
The authority verifies its principal, age, uncertain effects, artefact links
and that no unarchived task or composition still owns the run. Native Cursor
Cloud job runners use this path after confirmed terminal observations. A
pending historical poll acknowledgement does not prevent archival after an
independent correlated terminal job observation; uncertain live jobs do.

Artefact administration is available through the same operator HTTP, portable
CLI and console routes. `artifact_links` takes `principal`, `namespace`,
`artifact_principal` and pagination. `artifact_bind` additionally takes the
selected `task_id`, `link_id`, `request_key` and `reason`; the service checks the
task grant and constructs its target. `artifact_release` uses the same fields
without `task_id`. `artifact_retire` takes `namespace`, `request_key` and `reason`.
All mutations inject the authenticated operator; actor mappings and immutable
link identities are rechecked in the native transaction.

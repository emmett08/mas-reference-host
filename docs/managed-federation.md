# Managed federation

`PeerRegistry` stores administrator-approved peer versions in the deployment's
`peer-registry.sqlite`. `ManagedPeerConnector` resolves a version once and binds
every operation to that immutable identity. Local callers cannot supply an
endpoint, bearer token, deployment identity or workflow digest through task input.
Peer records contain no credentials. Trusted startup supplies a private token
file and the CA/proxy policy whose public descriptor is pinned in the registration.
Managed operations freeze the credential
across discovery and their subsequent effect, then read the file again for the
next operation. Rotation cannot change the authenticated principal between
negotiation and submission.

Three lifecycle profiles are implemented:

| Profile | Transport | Required negotiation |
| --- | --- | --- |
| `mas-http/1` | MAS `/service` JSON operations | `mas-task-service/1` discovery |
| `mas-mcp/1` | MCP `2026-07-28` at `/mcp` | Tasks and `org.mas/task-service/1` extensions, frozen MAS tool schemas |
| `mas-a2a/1` | A2A `1.0` JSON-RPC at `/a2a` | Authenticated extended AgentCard with `urn:mas:task-service:1` extension |

These profiles support MAS-to-MAS federation. Generic third-party MCP tools and
A2A agents continue to use the existing adapters and their explicit capability
contracts. Discovery never installs code, imports schemas, follows advertised
URLs, rewrites endpoints or obtains credentials. Unsupported profiles fail
before submission. HTTPS is required except explicit numeric loopback HTTP for
tests. Redirects and automatic effect retries remain disabled.

## Register and connect

Obtain the remote deployment ID, authenticated principal and workflow identity
from an authorised discovery request, then review those values before recording
them. The registration's `callers` are local authenticated identities; its
`outbound_principal` is the independently granted remote identity. Use distinct
credentials for each peer and grant that identity only the remote workflows and
artifact scopes it needs.

```python
from mas_host.peer_registry import PeerRegistration, PeerRegistry
from mas_host.bridges.managed_peer import ManagedPeerConnector
from mas_host.bridges.orchestrator import stable_request_key

registry = PeerRegistry(local_service.operations)
registration = PeerRegistration(
    name="analysis-host", version="1", profile="mas-mcp/1",
    endpoint="https://analysis.example/mcp",
    outbound_principal="analysis-client", deployment_id=reviewed_deployment_id,
    workflows={"analysis": reviewed_workflow_sha256},
    callers=["orchestrator"],
    capabilities=["submit", "inspect", "result", "cancel", "update"],
    artifact_scopes=["analysis-results"],
)
registry.register(operator="owner", request_key="install-analysis-1",
    reason="Approve reviewed analysis workflow", expected_revision=0,
    registration=registration)
peer = ManagedPeerConnector(registry, "analysis-host", "orchestrator",
    token_file="/run/secrets/analysis-token", version="1",
    identity=registration.identity)
receipt = peer.submit(stable_request_key(execution_id, "analyse"),
                      "analysis", {"input": 42})
```

The registry's Python administration API is trusted local code. Expose it only
through the operator service's peer administration grant, never as a workflow
tool. Each change requires a reason, unique stable request key and expected
registry revision. Repeating the same change returns its receipt; changing the
same key conflicts. A version cannot be edited or revived after revocation.
Register a new version to change endpoints, grants, credentials' principal,
capabilities, executable pins, transport policy or timeout. Existing connectors retain the old version
until explicitly replaced. Backups preserve the registry and tombstones; audit
restores permit reads and reject changes.

For a configured HTTP policy, pass `transport_policy=policy.descriptor()` and
an integer `timeout_seconds` from 1 to 60 to `PeerRegistration`; pass the actual
`HTTPTransportPolicy` and the same timeout to `ManagedPeerConnector`. The policy
descriptor includes proxy origin, origin allowlist, additional-CA digest and
whether proxy authentication is required. Credential values and private file
paths are excluded, so rotation preserves the binding. Lifecycle and artefact
clients both check their effective settings before credentials or network use,
including after discovery. A changed policy or timeout requires a new registered
peer version. Historical registrations with neither optional field keep their
exact identity and mean direct/system-trust transport with a 30-second timeout;
an explicit default `HTTPTransportPolicy()` has the same transport meaning.

Every use checks local caller grants and revocation, then confirms the remote
authenticated principal, deployment, capabilities and workflow identities.
Submit and continuation carry expected deployment and workflow identities to
the remote atomic admission operation. A stale discovery result cannot admit a
changed workflow. A revocation blocks future authorisation checks; an operation
already sent may finish and requires ordinary remote reconciliation.

`peer.artifacts()` returns the bounded verified transfer client. Only declared
artifact scopes and sessions opened by that client are accepted. Every chunk
rechecks registry revocation. The remote service still enforces its principal
mapping and artifact grants; the registration does not grant remote access.
The transfer route uses the configured peer's same HTTPS origin. Transfer status
after losing the local session is an operator recovery operation, not permission
derived from possessing a transfer token.

## Deployment recipes

For independent hosts, register each remote service in the orchestrator's local
registry. Keep the request key derived from the outer execution ID and logical
step, including after a lost receipt. All three profiles recover that same
durable remote task when the exact request is repeated. Retry policy belongs to
the outer orchestrator; a timeout does not cancel the task. Retain the observed
task ID and use inspect/result/cancel/update for its lifecycle.

For a native worker pool, register the controller endpoint as the peer. Worker
selection, claim epochs, spool replay and output publication remain the remote
controller's responsibility. Do not register individual workers as interchangeable
task-service endpoints or share local SQLite state among controllers.

For Temporal, LangGraph or a custom durable orchestrator, inject the managed
connector into the existing framework adapter. Persist the peer version and
identity with the workflow configuration. Activity retries reuse the logical
request key; workflow upgrades explicitly select a new peer version. Polling
deadlines and cancellation intent keep their existing distinct meanings.

Each deployment independently admits calls and owns its budget. Federation does
not create a global budget, transfer accounting authority, grant remote effects
or provide high availability. Shared-controller authority is a separate profile.

## Acceptance evidence

`test_managed_peers` exercises real loopback HTTP, MCP and A2A bridges backed by
the durable task service and Rust runner. It covers a lost first submission
receipt with one remote task, idempotent continuation, retrieval and cancellation,
principal mismatch, deployment/workflow drift, unsupported negotiation, caller
grants, immutable versions, revision conflicts, revocation, restored audit reads
and multi-chunk artifact access with revocation between chunks. Corporate TLS,
identity-provider policy and cross-host deployment qualification remain deployment
acceptance work; these tests make no external provider calls.

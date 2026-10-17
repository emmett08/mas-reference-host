# Host architecture and behavioural contracts

The host schedules trusted adapters over the existing Rust admission boundary.
Its abstractions preserve actor identity, ordered workflow replay, call
reservation before dispatch, original deadlines and uncertainty after an
unconfirmed external effect. Rust remains the authority for wire validation,
message/task transactions and protocol admission. Python records workflow
checkpoints and deployment allocations.

## Design choice

Three options were considered for the readiness repairs:

| Option | Consequence |
| --- | --- |
| Patch the existing methods in place | Small textual changes, but bootstrap, subprocess transport and execution errors remain coupled and difficult to fault-test independently. |
| Extract the observed ownership and interface boundaries | Keeps the public runner façade and persistence semantics while making bootstrap, execution strategies and schema admission independently testable. Selected. |
| Replace the host with a new service framework or distributed scheduler | Introduces transport, deployment and migration obligations that do not resolve the observed single-host defects. |

The selected changes address specific failures. A file split or interface alone
does not establish SOLID compliance. The relevant acceptance criteria are
substitution across execution strategies, exclusive ownership during cleanup,
durable identity across bootstrap crashes and rejection of incompatible storage
before writes.

## Responsibilities

| Component | Owned responsibility |
| --- | --- |
| `contracts.py` | Adapter-facing values, typed public failures and narrow context/execution protocols. |
| `codec.py` | Shared Rust actor-identity grammar, bounded integer JSON and canonical JSON identity primitives. |
| `Runner` | Ordered request orchestration and checkpoint replay through a stable façade. |
| `RunSession` | Exclusive run lifetime and staged, idempotent bootstrap. |
| `ProtocolClient` | Bounded Rust subprocess execution, credential-free child environments and sanitised diagnostics. |
| Execution strategies | Inline or supervised execution under the same adapter-facing contract. |
| Process-group guardian | Retain ownership until local adapter work and ordinary descendants have terminated. |
| Journal and deployment repositories | Own their distinct schema identities, validated migrations and durable records. |
| Reconciler monitor | Report a fresh, successful scan by a live watcher independently of admission headroom. |
| Provider binding | Require explicit effect identity and spending allocation before admission. |
| `agent_config.py` | Register arbitrary model actors with application instructions, operation contracts and stable provider identities. |
| `provider_config.py` | Retain the invoice example's planner/reviewer configuration and credential validation. |
| `provider_credentials.py` | Resolve bounded provider-specific secrets independently of application adapters. |
| Generic OpenAI and Cursor providers | Convert application-defined structured requests to bounded SDK calls and validate the declared output schema. |
| `CursorTextSession` and the OpenAI client boundary | Own provider transport policy and client lifecycle independently of application prompts. |
| Invoice provider adapters | Apply the demonstration's invoice prompt and total-validation contract through the shared provider transport. |
| `CursorCloudClient` | Bounded, non-retrying HTTP operations against the fixed Cursor Cloud API origin. |
| `JobStore` and `DurableJobService` | Persist remote-job intent, correlate exact identities, serialise observations and retain reconciliation evidence. |
| `CursorCloudJobAdapter` | Admit a repository-job submission through the existing runner and return its durable receipt. |
| `DurableTaskService` and `TaskStore` | Bind principal-scoped request keys to immutable submissions, serialise execution revisions and commit task events with state changes. |
| `service_mutation.py` | Share maintenance locking and read-only/audit-only rejection between task and composition services; new-input admission remains distinct from cancellation and recovery writes. |
| `RunnerExecutor` and `ExecutionBinding` | Assign one run/root request to each revision and construct adapters from trusted principal, workflow and run identity. |
| HTTP/stdio boundary and token verifiers | Bound incoming requests, authenticate callers and enforce scopes before invoking the task service. |
| MCP and A2A adapters | Convert the declared protocol profile into task-service operations or admitted remote-client operations. |
| `RemoteTaskService` and `RemoteTaskStore` | Persist namespaced remote intent and exact peer identities, distinguish observations from uncertain effects, and retain resolution evidence. |
| Temporal and LangGraph connectors | Derive stable request identities from framework execution, preserve retry ownership and translate interruption/cancellation semantics. |

The façade, Strategy and Adapter patterns correspond to existing variations.
The bootstrap state machine records intent, deployment admission, Rust state,
journal readiness and gateway readiness. These stages span separate durable
stores; the design uses idempotent recovery rather than asserting a transaction
across all stores. Rust task transitions and message insertion retain their
single atomic transaction.

## Interface preservation

Both execution strategies provide only the adapter contract: actor identity,
invocation identity, remaining time, delegation and reserved invocation.
Recoverable public errors cross supervised IPC using fixed typed responses;
private exception strings and provider payloads do not become diagnostics.
An uncertain effect remains unresolved even when adapter code catches its
exception. The host cannot manufacture external cancellation or no-effect
evidence.

The new provider contract deliberately removes implicit defaults for custom
providers. Implementations must declare their descriptor and allocation.
Existing built-in descriptors retain their identity; custom providers missing
the newly mandatory members must be updated before they can run. A descriptor
is the adapter author's compatibility declaration, not proof of provider
determinism or truthful billing.

## Provider substitution and durable jobs

The provider extension compares three designs:

| Option | Consequence |
| --- | --- |
| Treat a Cursor token as an OpenAI credential | Unsupported: the documented APIs have different authentication and execution contracts. |
| Make every provider implement an asynchronous job service | Adds job lifecycle obligations to synchronous inference and obscures the existing result contract. |
| Use interchangeable local result providers and a distinct durable-job adapter | Preserves the existing workflow and gives remote jobs explicit submission, observation and cancellation semantics. Selected. |

`TotalProvider` is a Strategy interface: the planner and reviewer depend on
`total`, a descriptor and an explicit reservation. A Factory selects mock,
OpenAI or Cursor independently for each role. The SDK adapters share validation
of invoice inputs and exact integer-JSON outputs, while retaining provider-owned
authentication and response conversion. Existing OpenAI and mock descriptors
remain unchanged so their completed workflows retain recovery compatibility.

`CursorTextSession` creates a fresh private directory, disables tools, MCP,
subagents and ambient settings, and attempts to close its owned agent and bridge
on normal return or exception. Its bridge
is launched only after durable invocation admission, within the supervised
process group. These controls constrain the SDK configuration; they do not
isolate hostile code from the operating system. SDK-managed internal model
retries remain distinct from the host's one admitted submission.

The canonical `cursor_provider.CursorLocalProvider` implements generic structured
inference; `InvoiceCursorProvider` owns the invoice-specific prompt and validation.
The old `cursor_local.CursorLocalProvider` import is a compatibility alias for the
invoice adapter until a major API release. Generic inference uses the public
session operation, and both OpenAI adapters use the neutral client boundary.
Immutable provider descriptors are available before credential resolution or
optional SDK initialisation. The generic registration path defers these live
dependencies to admitted invocation execution, keeping management commands usable
without credentials. Existing descriptors remain stable across this refactor.

The Cloud adapter implements the existing `Adapter` contract, not `TotalProvider`.
Its output says that a repository job was submitted. `JobStore` persists the
frozen specification, client-assigned agent ID and launch intent before dispatch.
The service records the exact returned run ID and polls only that identity.
Ambiguous dispatch retains `launch_unknown`; it never enables an automatic
second POST. Cancellation intent and remote cancellation are separate records.
The first correlated terminal observation remains immutable.

This division applies single responsibility to configuration, SDK conversion,
HTTP transport and persistence; interface segregation keeps repository lifecycle
methods out of synchronous providers. Injected clients permit contract tests
without credentials. Rust envelopes, route permissions and task transitions are
unchanged. The cloud job's completion has its own lifetime, so a completed MAS
submission task cannot imply that repository work finished or passed review.

The forked guardian and worker freeze inherited garbage-collected objects
before closing copied host descriptors. This prevents an inherited file or
SQLite finaliser from later closing a descriptor number reused by the worker.
New worker objects remain collectable; the parent collector is unchanged.
A regression forces that descriptor reuse and explicit collection.

### Proxy transport and failure observations

Cursor proxy support is an explicit transport policy behind the existing
provider interface. The selected design uses a small host-owned bootstrap and
the SDK's public configuration API. Editing minified SDK transports would add
an unverified maintenance dependency; enabling arbitrary inherited Node hooks
would defeat the restricted launcher. Leaving the launcher unchanged cannot
support the observed proxy-only environment. The new helper owns command,
proxy-exclusion and bootstrap identity construction; the provider continues to
own the agent lifecycle and result contract.

The original `sdk-default` descriptor is unchanged. Opted-in
`environment-proxy-http1` descriptors include the policy revision and bootstrap
hash, so a changed transport cannot silently resume an existing invocation.
Authenticated local callbacks are excluded from proxy routing in the child;
the parent's configuration and certificate validation are preserved. The SDK
may still select HTTP/2 under server policy, which requires live validation.

Invocation diagnostics now preserve the adapter-reported local failure stage
and whether cleanup also failed. The exact finite schema is checked before IPC
transmission and again before durable recording. Older frames without these
fields remain valid. Unknown outcomes, reservations, admissions and recovery
decisions are unchanged: even `stage=launch` supplies no proof that a remote
effect did not occur. A cleanup failure cannot replace the original failure
stage. A cleanup-only failure still prevents a successful invocation record.

This change adds observation detail to the existing unknown transition; it
does not add a protocol state or strengthen a formal claim. Local proxy, TLS,
loopback-credential, cleanup and supervised-journal tests check implementation
properties outside the finite Promela models. Successful live runs and replay
are separate acceptance evidence for the selected environment and models.

## Durable service and protocol boundaries

The bridges add a service above `Runner`; network request handlers never drive
its ordinal replay directly. Submission commits the authenticated principal,
request key, canonical content digest, workflow identity, first execution
revision and initial event together. Repeating the key returns the same receipt;
changing its content is a conflict. A separate exclusive worker selects a
revision and executes its deterministic run/root request. The guardian retains
worker ownership alongside run ownership until descendants have stopped, so a
replacement worker cannot race cleanup after host death.

`TaskService` is the shared application Facade. MCP 2026-07-28, A2A 1.0 and the
HTTP service adapter perform their own wire conversion and capability checks.
MCP's Tasks completion may contain an error tool result; A2A's task and context
identities remain distinct. Neither mapping substitutes transport acceptance
for business success. The HTTP boundary authenticates scoped credentials, then
applies workflow grants; a supplied principal, task ID or Agent Card is never
authority. Stdio uses the trusted launcher's configured principal. Supported
profiles and their deployment settings are in [the bridge guide](bridges.md).

The optional execution factory receives an immutable `ExecutionBinding` after
the run directory has been assigned. Its remote namespace includes authenticated
principal, workflow identity and actor. This permits one registered workflow
to handle several callers without allowing identical caller keys to identify
each other's remote work. Endpoints, credentials, actor membership and adapters
come from trusted configuration; received messages cannot select them.

Remote protocol clients implement `RemoteClient`, while `RemoteTaskService`
owns persistence and admission. Submission intent precedes the external call;
response persistence follows it in a separate transaction. A lost first receipt
therefore remains unknown instead of triggering another submission. Polling,
continuation and cancellation each use an admitted invocation. Unknown lifecycle
effects retain their operation identities until an explicit evidenced decision;
administrative resolution does not itself contact the peer. Protocol-specific
clients retain their own state mappings and exact remote identities.

Service cancellation closes queued work or a paused local continuation. Running
work records cancellation intent and uses supervised cleanup; uncertainty about
external effects survives. A local cancellation cannot establish remote
cancellation. Likewise, revoking a caller's grant prevents further dispatch
without relabelling an interrupted effect as a confirmed failure.

Temporal activities and LangGraph nodes submit and inspect the same service
receipts. An outer retry repeats the original logical request identity. Temporal
outbound starts use a separate remote adapter with exact workflow/run identity
and memo-checked reconciliation. LangGraph integration covers nodes, checkpoints
and continuation; it does not implement a hosted LangGraph job service. The
outer framework owns business sequencing, while MAS owns admission and recovery
of each submitted operation.

These boundaries apply Ports and Adapters, a service Facade, version-aware
Repositories and explicit lifecycle state machines. Execution and provider
variation use Strategy; factories bind trusted deployment configuration. Task
events are durable cursor reads, not a push outbox with delivery acknowledgements.
All stores join backup, schema rejection, audit-only restoration and retention;
unresolved work protects every run record it needs. The architecture remains a
single Linux host with SQLite, rather than a distributed scheduler.

## Verification and limits

### Generic composition above the serial runner

`WorkflowEngine` adds durable sequence, conditional, loop, parallel and
subworkflow execution above existing `Workflow` registrations. `PlanEvaluator`
owns control-flow decisions; `WorkflowStore` owns versioned composition records;
`SchedulerOwnership` owns Linux worker lifetime and inherited locks. Every leaf
uses `RunnerExecutor`, the existing Rust gateway and its own immutable run
identity. `Runner` retains its serial context and ordinal replay semantics.

The public composition journal operations own node checkpoints, scope records,
leaf assignments and instance transitions. The engine retains registration,
authorisation, scheduling and effect-reconciliation policy. Initial execution
and continuation use the same persisted-assignment construction, with the
revision and effective deadline supplied explicitly. State changes and their
events remain within the same transaction.

Workflow inspection has two deliberate views. The graph reports control and
attempt states without leaf payloads. An explicit task-detail request returns
one retained leaf revision's output and error after the same ownership, grant
and binding checks. This lets an authorised caller read a paused application's
question without returning every branch's data in each status response. Detail
inspection reads existing composition records and changes no execution state;
it needs no separate output store or persistence migration.

Every read-only composition transaction establishes a SQLite snapshot. Graph
queries select their public fields directly, avoiding historical input/output
decoding when those payloads are omitted from the response.

An in-place threaded runner was rejected because its thread-bound contexts,
active request stack and ordered checkpoints are part of its recovery contract.
Independent synchronous wrappers were also insufficient: they do not retain
branch identities, joins, ancestor budgets or unknown outcomes. Process-owned
leaf runs preserve those contracts while admitting genuine concurrent work.
The cost is a separate Rust run and supervised process hierarchy for each leaf;
this remains a bounded single-host implementation.

Every plan binds the transitive registered workflow identities. Optional
`Workflow.contract_descriptor` covers operation, provider and acceptance
descriptors; omission preserves old workflow fingerprints. Topology compilation
produces the existing directed ACL rather than introducing new wire types.
Structured OpenAI/Cursor interfaces coexist with the invoice `TotalProvider`;
the latter's descriptors and recovery compatibility are retained.

The parent deadline is fixed at submission. Subworkflow entry freezes a deadline
no later than its parent's; root and ancestor call/spending allowances apply
atomically before deployment admission. A denied downstream call conservatively
retains its composition allocation. Each nested concurrency cap applies in
addition to the root cap. The static expanded-node bound and runtime root-node
cap limit control-flow work. Model-produced results cannot change these limits.

Scheduler and deployment locks survive in workers and execution guardians.
Closing an owner never explicitly unlocks another surviving owner's descriptor.
A lifetime pipe stops child admission after scheduler death; original deadlines
still bound execution. Unknown effects block further dispatch until the existing
evidence-backed invocation resolution allows recovery of the original run.
After loss of both scheduler and worker, restart checks retained run journals
before reclassifying stale running attempts. Cancellation or expiry cannot
publish a terminal composition while an admitted effect remains unresolved.
The task service and composition engine share the same read-only bootstrap
evidence check. Missing published journals or admitted run directories remain
uncertain; validated pre-journal bootstrap state can establish safe cancellation.
Parallel joins settle all dispatched work and require every branch to complete
before declaring success. Acceptance of the returned result remains separate.

Composition databases and run-local artefacts participate in quiescent backups.
Restores remain audit-only. Composition history conservatively retains every
referenced run; deleting historical composition records is not an automatic
retention feature in this version. Artefact consumer links independently retain
live or unresolved references. See [composition](composable-workflows.md) and
[artefact ownership](artifacts.md) for the explicit operational contracts.

The new model in [workflow composition](https://github.com/emmett08/mas/blob/main/formal/workflow-composition-notes.md)
covers bounded admission, crash identity, terminal safety and joins. It is an
abstract state-machine check, not a refinement proof of the Python scheduler.

Regressions exercise actual process death during bootstrap, a killed host with
ordinary adapter descendants, ownership retention during cleanup, incompatible
database versions, failed reconciliation, persistent live-acceptance state and
context error substitution. Container acceptance runs the installed package and
Rust binaries on native amd64 and arm64. OpenAI HTTP contract tests use the
real pinned SDK with a fake transport; Cursor contract tests use injected
clients and separately exercise the installed bridge. Live local-provider
acceptance is a two-submission check requiring credentials and persistent state.
The SDK may perform internal requests that the host does not count separately.

The finite Promela models preserve their documented safety and progress scope.
They do not model Python schemas, Linux process cleanup, actual storage flushes
or provider behaviour. The additional Cursor job model checks finite launch,
correlation, terminal-state and cancellation safety; it has no remote progress
or monetary-cap claim. Implementation regressions supply evidence for these
additional boundaries; no refinement proof is claimed.

The [bridge models](https://github.com/emmett08/mas/blob/main/formal/bridge-notes.md) add request-key conflicts,
admission-before-effect, lost receipts, restart, cancellation intent, stale
observations and durable event reads. Their finite bounds do not establish
remote availability, cryptographic security or unbounded event delivery.
Implementation checks separately exercise actual HTTP/stdio framing, principal
isolation, protocol schemas, interrupted service execution and maintenance of
both new repositories. Optional SDK checks execute real LangGraph checkpoints
and a pinned local Temporal server; they do not establish acceptance against an
organisation's identity provider, remote agents or production orchestrator.

The project retains Python's declared supported syntax, existing unittest/Cargo
tests, locked dependencies and SPIN verification. Rust 1.99.0 is the current
pinned toolchain. The runtime repair introduces no new formatter, linter or framework.
Private orchestration helpers remain with their owning workflow when separating
them would split a transaction or replay invariant; bootstrap and command
execution were extracted because they have independent failure and lifecycle
contracts.

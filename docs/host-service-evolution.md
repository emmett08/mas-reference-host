# Versioned tasks, composed services and local scheduling

The host keeps Python as the service and adapter integration layer. Rust remains
the protocol and task-admission authority. A deployment can expose independently
submitted workflows and composed plans through one authenticated service without
changing existing task, request, run or continuation identities.

## Registrations and deployment changes

`Workflow.identity` binds its declared descriptor to a measured implementation
manifest. Registration hashes the complete source packages and assets containing
its adapters, classifiers, continuations and factories, together with runtime
imports of those entry modules and their package dependencies. Registered Python
functions and reachable helpers are compared against code compiled from those
source bytes; a file digest alone cannot hide stale loaded code. The manifest
also records the Python implementation/version and optimisation mode. Absolute
installation paths are excluded, so relocating identical packages preserves
identity. Source files must be available; synthetic or unavailable entrypoint
code fails closed.

The optional `implementation_digest` is now an expected SHA-256 value which must
match the measured manifest, rather than a deployment assertion. Trusted factory
code can include additional callable package roots through
`implementation_sources=(trusted_helper, ...)` for dynamically selected
application integrations. This is a code registration API, never a remote import
name. The bridge includes its configured factory in new registrations; retained
versions must supply their original binding, including original factory sources.
Mutable application settings still belong in the explicit contract descriptor;
source integrity does not make arbitrary Python code a sandbox.

`definition-registry.sqlite` version 2 stores the reproducible manifests alongside
immutable descriptors and current aliases. Registration and actual execution
check source availability and integrity. File hashes are cached only while
device, inode, size, modification time and change time remain identical; writes
during hashing fail. Historical receipt inspection does not traverse package
files. New submissions select the current alias. Duplicate requests,
continuations and recovery retain their original identities.

Migration preserves every version-1 descriptor, asserted digest and identity
exactly. An unverified legacy record remains `not_recorded` or
`deployment_asserted_digest`; migration cannot certify it using currently loaded
code. New registrations have distinct verified identities. Keep immutable
versioned application packages/runtime installations to drain previous work;
replacing their bytes makes those definitions unavailable for execution while
retaining authorised historical inspection and lifecycle administration.

Pass old executable versions as `workflow_versions={"name": (old_workflow, ...)}`
when constructing `DurableTaskService`, `WorkflowEngine` or `UnifiedTaskService`.
Declarative plan sources are retained and can be reconstructed; Python callbacks
and adapter implementations must remain supplied by trusted deployment code.
Legacy records whose descriptors were never recorded remain explicitly unknown.

Missing executable code blocks unstarted work without blocking other queue
entries. A possibly started effect becomes `unknown`, requiring reconciliation.
Historical inspection and cancellation depend on retained owner/grant records,
not on the current executable map. Restoring the exact definition lets blocked
work use its original assignment. It never automatically retries unknown work.
Retiring a current alias or a version referenced by live work is rejected.

## Exposing a composed service

A trusted bridge factory can still return the original workflow mapping. To
expose plans, it returns:

```python
return {
    "workflows": {"extract": extraction_workflow},
    "plans": {"review": review_plan},
    "workflow_versions": {"extract": (previous_extraction_workflow,)},
}
```

Bridge configuration grants contain both allowed logical names. A name cannot
identify both a leaf workflow and a plan. Leaf submissions retain `task-…` IDs;
plans retain `flow-…` IDs. Durable submission records are the scheduling intent,
so there is no separate enqueue write that can be lost after acknowledgement.
The unified facade arbitrates submission keys across both repositories.

New unified submissions use the explicit `mas-unified/1` request namespace.
Existing arbitrary legacy keys retain their original namespace and logical IDs;
a default retry first returns an existing versioned key, then an existing legacy
key. The underlying stores use separate namespace and epoch columns, so a string
which happens to resemble a prefix cannot alias another namespace.

Supply `request_epoch="release-42"` on HTTP/client/MCP submissions or in the A2A
JSON data part to use an independently scoped epoch. The local workflow CLI uses
`--request-epoch release-42`. Continuations inherit their original task's scope.
The same key in another epoch denotes separate work. Reusing a key inside one
epoch preserves the usual payload-conflict checks and cross-plan/leaf arbitration.

An authorised owner or operator can retire a drained epoch with
`retire_request_epoch(owner, epoch, reason="…", prune=True)`. Queued, paused,
running and uncertain work prevents retirement, including unresolved child
executions beneath a terminal parent. The durable retirement intent immediately
rejects every subsequent submit or continuation into that epoch, including
previously accepted keys. Each owning repository then records its retirement;
restart or later admission repairs interrupted owner acknowledgements. A direct
leaf or composition API uses the same guard, so it cannot bypass an incomplete
retirement. The result records the work owner, authorising principal and reason.

Optional pruning removes deduplication rows for archived leaf tasks and archived
composition continuations only after closure. Compact historical task/plan
receipts and lifetime accounting remain available. Legacy arbitrary keys and the
default epoch are retained indefinitely. Historical inspection and cancellation
continue by original task ID after retirement.

Plan discovery exposes bounded integer-JSON input/output contracts. A plan may
declare `input_schema` and `output_schema`; registration checks their local JSON
Schema syntax, admission validates new input, and completion validates the final
output while retaining the executed leaf evidence. Omitting them retains the
existing object contract.

The shared operations are submit, inspect, result, cancel, update, event pages,
listing, discovery and recovery. A paused composed receipt contains
`result.pending_tasks` with explicit node paths and revisions. Continue one
selected node with:

```json
{"path":"root/s1","input":{"approved":true}}
```

Continuation request metadata is bound to that continuation key separately from
the submission metadata. Replaying the same payload and metadata returns the
original receipt, including after archival; changing either conflicts. This
preserves A2A message identity without rewriting the original task context.

Use the task-detail operation to inspect a selected node/revision. Event pages
return `events`, `next_after` and `has_more`. Listing selects bounded identifiers
in SQL before loading results. Keyset cursors are observed positions, not frozen
snapshots. Archived results remain listed with `archived: true` and
`result_available: false`; event cursors older than compacted history produce
`event_cursor_expired`.

Accepted protocol envelopes publish progress while execution is still running.
Events contain bounded sender/recipient, message, protocol-task, run and revision
identifiers, without input or output bodies. Public stores deduplicate the run
and message identity; the Runner's durable publication outbox retries an
unacknowledged event after interruption. Replaying a run or performing the final
flush does not create another copy of the same progress event. Consumers resume
from the public event cursor.

Discovery is scoped to the authenticated principal and identifies the deployment
and registration digest. Clients can pin `expected_deployment_id` and
`expected_identity` on submission or continuation. Those checks occur in the
admission transaction, including request replay. They cannot silently select a
new default registration after discovery.

## Bounded local execution

The bridge worker dispatches independent assignment processes. It stays single
threaded and leaves Rust-backed execution and adapter supervision to each child.
Per-task and per-plan lifetime locks prevent simultaneous local ownership.
Inherited dispatcher slots continue counting orphaned assignment processes until
they exit. Shutdown stops new scheduling, drains for the configured interval,
then terminates remaining child processes; adapter guardians retain their own
ownership locks until supervised cleanup finishes.

`scheduler-capacity.sqlite` supplies a shared limit for standalone and composed
leaf execution. Example bridge configuration:

```json
"scheduler": {
  "max_workers": 4,
  "max_per_principal": 2,
  "workflows": {"extract": 2},
  "resources": {"provider-account-main": 1}
}
```

A workflow opts into resource gates through
`resource_keys=("provider-account-main",)`. Limits are persisted, and incompatible
configuration fails rather than silently replacing deployment policy. The
dispatcher interleaves principals and retains FIFO order within each principal.
Composed plans also keep their existing local parallelism and scope bounds.

Slot rows describe admissions; inherited `flock` descriptors establish their
lifetime. A timestamp or presumed dead PID cannot free an occupied slot. This is
a **single Linux machine** mechanism, not a claim that SQLite or its lock files
can safely be shared across machines. Distributed invocation workers use their
separate controller/fencing mechanism; they do not become independent workflow
schedulers or budget authorities.

## Active time and human pauses

An optional `TimingPolicy` on a workflow or top-level plan separates four limits:

```python
TimingPolicy(business_seconds=172800, active_seconds=600,
             queue_seconds=3600, pause_seconds=86400)
```

The durable clock preserves submission time and accumulated active milliseconds
across explicit continuation revisions. Active time runs once while any local
branch is active, rather than once per branch. Queue and human-pause durations
are measured separately. Business time always bounds the overall process.
Restart conservatively charges an unobserved running interval; it does not reset
the budget. Policy expiry closes future dispatch, exposes the specific reason
and retains unknown external effects for reconciliation.

Omitting the policy preserves legacy wall-deadline behaviour. A subworkflow plan
can supply its own `timing_policy`; its durable scope clock then counts only
active descendant branches and preserves queue/pause allowances across resumes.
An expired scope closes composition admission, drains active workers and records
the scope path and expiry reasons, retaining any unknown effects. Existing nested
scope `wall_seconds` values remain absolute bounds when that subplan has no
explicit policy. A top-level policy does not remove those nested limits.
Per-run Rust topology deadlines also remain enforced.

History retention releases retained-detail capacity, not cumulative deployment
spending. Request and continuation tombstones preserve acknowledgement identity.
Budget amendments are separate audited management operations.

Clock format `mas-execution-clock/2` also accumulates `worker_ms`: each running
interval contributes its duration multiplied by the observed running branch
count. This is a resource measure independent of the `active_ms` budget. Pauses
charge neither measure; an unobserved running interval is conservatively charged
through the next observation. Existing format-1 clocks retain their original
identity, timestamps and active allowance. Their historical worker multiplicities
cannot be reconstructed: after migration, `worker_ms` is null,
`worker_ms_lower_bound` records the known lower bound, and
`worker_time_complete` is false whenever prior active time was present.

`DeadlineService` grants deadline administration separately from task ownership.
An authenticated operator may increase absolute `business_seconds`,
`queue_seconds` or `pause_seconds` allowances on open queued, blocked or
human-paused work. The operation preserves the pinned definition, original
phase timestamps, accumulated computation and all effect identities. It never
changes `active_seconds`, resumes execution, revives expired/uncertain work or
migrates a legacy fixed deadline. Nested scope extensions are explicit; every
ancestor limit remains enforced.

The owning task/composition store commits the clock, `deadline_extended` event
and amendment receipt together. Each request key has one immutable receipt;
changed content conflicts, and `expected_revision` rejects concurrent lost
updates. Receipts survive event pruning and lifecycle archival. Task and
composition schemas migrate from versions 1/2 to version 3 without rewriting
submission or invocation identities. Older executables reject that schema;
roll forward or restore an appropriate quiescent backup rather than running an
older binary against it.

The local administration command uses the local operator trust boundary:

```sh
python -m mas_host admin --state-dir .mas-state extend-deadline \
  --config deployment.json --operator release-manager --owner alice task-EXAMPLE \
  --request-key review-extension-1 --reason 'Review window agreed' \
  --expected-revision 0 --limits extension.json
```

For example, `extension.json` may contain `{"pause_seconds": 172800}`. Supply
`--path` for an exact nested composition scope. Network boundaries must derive
operator identity and independent administrative grants from authenticated
configuration, never from request body assertions.


## Sequential progress and storage observations

Composition schema 4 introduced `control_progress`; subsequent schema versions
retain it. Sequence, reduction, loop and while controls commit a cursor and the
last completed child's output. Reduction checkpoints also preserve accumulated
expanded-input bytes. The store verifies the referenced child completion before
advancing the cursor. A crash before the checkpoint replays retained evidence;
a crash afterwards resumes at the next child with the original assignment IDs.
An unfinished or uncertain child cannot enter the completed prefix. Existing
queued attempt records retain the ready leaf assignments across restart.

Unchanged node/attempt observations use read-only transactions. Repeated node
states produce neither a write nor an extra event. Linux schedulers wake on
worker `pidfd` readiness where supported, retaining a 20 ms timer for cancellation
and expiry checks. Unsupported kernels retain the bounded timer path.

Storage accounting maintains a process-local file-size index. Every admission
and health decision fully enumerates directories and re-reads file sizes/types;
external SDK file growth therefore remains observable without trusting host
write hooks. Full checks reject symlinks and special files and tolerate vanished
SQLite journals. The index is discarded safely after loss, fork or excess cache
size. An explicit `estimate()` method can reuse directory inventories with
periodic reconciliation, but is excluded from admission: coarse filesystem
timestamps cannot establish that no new directory entries exist. Admission
thresholds remain distinct from physical filesystem quotas.

Run `python scripts/benchmark-workflow-overhead.py` for the bounded comparison.
The sequential case measures repeated observation behind 100 completed steps;
the storage case measures 1,000 files with external writes between observations.
The benchmark marks provisional inventory estimates as ineligible for admission.
Results describe local control overhead and do not measure provider latency or
establish a production throughput objective.

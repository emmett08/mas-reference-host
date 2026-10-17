# Shared task service

`create_shared_service(authority, cache_dir, workflows, grants, plans=..., allocation_limits=...)`
constructs the public leaf/composition facade over the native PostgreSQL host
repository. The authority must already identify an explicitly provisioned
controller epoch. Construction installs additive host tables and starts lease
renewal; it never initialises a deployment or chooses a takeover.

Task receipts, revisions, queue claims, definition metadata, source package
manifests, request namespace retirement and events are native shared records.
Each mutation checks the controller epoch and commits its receipt in the same
transaction. A worker result additionally requires the retained assignment
owner. Replacement controllers replay the original run and root request; local
cache loss does not create another task, provider call or quota allocation.
An uncertain provider invocation retains its native admission until an audited
resolution permits explicit recovery.

Source manifests are uploaded as immutable bounded chunks and verified against
their complete SHA-256 before definition registration. Historical registrations
retain their exact identity. Inspection requires only retained metadata;
execution also verifies the registered loaded package. A missing historical
implementation blocks its existing assignment. Queue polling skips unavailable
blocked definitions and rotates bounded pages so unrelated work can proceed.
Restoring the exact source resumes the original assignment. A terminal parent
with an uncertain child still retains its definitions.

Completed `result` requests return the application output. Archival retains the
task identity, ownership, definition identity, allocation and result digest,
marks the result unavailable, and releases retained task capacity. Event cursors
older than the retained floor fail explicitly. Exact request-key retries return
the archived metadata. An explicitly retired versioned epoch rejects every
later submission and continuation; `prune=True` removes only request markers
for archived tasks and compositions while retaining the epoch retirement.
Legacy arbitrary-key namespaces remain retained.

The bridge configuration may select `shared` with exactly these fields:

```json
{
  "binary": "/opt/mas/bin/mas-shared",
  "dsn_file": "/run/mas/postgres-dsn",
  "namespace": "mas_production",
  "deployment": "production",
  "controller": "controller-a",
  "epoch": 1,
  "secret_key_file": "/run/mas/gateway-key",
  "secret_reference": "gateway-key/1",
  "allocation_limits": {
    "deployment": {"max_runs": 100000, "max_calls": 1000000, "max_microusd": 1000000000},
    "principal/alice": {"max_runs": 10000, "max_calls": 100000, "max_microusd": 100000000}
  }
}
```

Credential files must be owned regular files with no group/other permissions.
The secret key contains at least 32 bytes. DSNs and key bytes are not stored in
shared task metadata. Every granted principal requires an explicit allocation
scope. The selected binary supplies the verified database transport profile.

Shared artefact configuration requires S3 content and uses PostgreSQL metadata,
publication receipts and access grants. Transfer staging files and transfer IDs
are bounded, expiring caches attached to one controller. Losing that cache
invalidates the transfer session; it does not lose a published reference.
Upload retries with the same verified content recover the original immutable
reference. Download chunks and cached-session status recheck shared grants;
revocation on one controller applies to cached downloads on another. Published
content is never assembled from an incomplete transfer.

`host/tests/test_shared_service.py` exercises real PostgreSQL through the Rust
private command binary. These tests require `MAS_TEST_POSTGRES_DSN` and
`MAS_SHARED_BIN`; a skipped fixture is not evidence of backend qualification.

The same PostgreSQL transaction checks deployment and principal outstanding
submission limits across leaf tasks and compositions. Pauses and uncertain
children retain pending capacity. Optional principal `max_tasks` bounds retained
logical submissions; immutable request replays do not consume another slot.
Principal call and reservation limits narrow the explicitly configured native
allocation scope. Frozen policy changes are rejected rather than replacing
existing allocation counters.

The `scheduler` policy applies native shared slots to every started leaf run,
including composition branches. Global, principal, workflow and resource caps
are checked together; successful admission advances round-robin principal
ordering. A fenced controller's assignment retains its slot until the original
assignment is recovered or authoritatively classified. Capacity is not freed
by cache loss or elapsed worker silence alone.

# Shared artefact authority

`SharedArtifactStore` implements the principal-bound repository contract using
PostgreSQL metadata and the explicit `S3ArtifactContent` strategy. Controller
directories contain no authoritative index, grants, reservation or retention
links. The shared service migration installs these tables with the other host
repositories; a schema version mismatch fails closed.

The namespace freezes its content endpoint, bucket, prefix, transport policy,
storage identity and quota policy. Credential values and paths are excluded.
Its object key is derived from retained deployment/namespace identity and the
content hash. The producer, exact input digests and initial readers contribute
to the same reference identity as the local repository.

Publication has explicit ordering:

1. An epoch-validated transaction reserves object count and bytes, including
   reservations whose prior upload result is unknown.
2. The trusted content port conditionally writes the immutable object and
   verifies its complete length and SHA-256 through GET. No database lock is
   held during network I/O.
3. A second epoch-validated transaction publishes the manifest. A lost result
   or replacement controller repeats verification against the original key.

A controller fenced during step 2 can leave an orphaned object and charged
reservation, but cannot publish metadata. Concurrent reservations obey the
namespace's aggregate bounds. A digest never grants read authority. Reads
verify a private snapshot and recheck grants before returning it; revocation
does not recall bytes already supplied to an authorised consumer.

Retention links have immutable owner and reference sets. `ensure_link` is for
worker receipt replay: it cannot reactivate an already released link. Explicit
`link` transitions retain the local repository contract. A producer can revoke
an original reader permanently; duplicate publication cannot undo that change.
Unknown upload reservations remain charged. There is no automatic deletion or
quota refund based on elapsed time.

```python
from mas_host.shared_artifacts import SharedArtifactStore

repository = SharedArtifactStore(
    authority, namespace='results', principal=authenticated_principal,
    content_backend=configured_s3_backend, policy=configured_artifact_policy,
)
reference = repository.put_bytes(report, readers=('approved-consumer',))
```

Only the trusted service constructs this binding. Network payloads cannot choose
the database connection, content backend or principal. Backup/replacement must
retain both the shared database and all referenced object versions. Restoring
metadata alone never proves content availability or permission to reactivate
admission. The real-backend suite exercises publication loss, concurrent quotas,
fencing during upload, replacement-cache reads, revocation, tampering and replay
retention. Its S3 transport fault double is separate from the real S3 acceptance
in `scripts/check-object-storage.py`.

## Lifecycle retention and namespace retirement

Shared links can carry one immutable lifecycle target: `run`, `task` or
`workflow`, its exact ID and the authenticated service owner. Bind a trusted
repository before context selection, workspace preparation or materialisation:

```python
consumer = repository.with_retention_target(
    kind='task', id=task_id, principal=authenticated_task_owner,
)
consumer.link('context-selection', [reference])
```

The bound repository derives a stable link ID from the caller's original link
ID and the complete target. Reusing the same context in another task creates a
separate consumer edge; it does not move the first edge. `retention_link_id`
returns this ID for inspection. All callers of the repository's normal `link`
method, including context, workspace and peer-materialisation helpers, inherit
this binding. Shared worker output receipts automatically bind their unresolved
links to the original admitted run.

Native transactions verify the target's retained owner and reject new links to
archived targets. An artefact principal differing from the service owner must
match a mapping in the frozen transfer-scope configuration for that namespace;
A backing namespace and actor must map to exactly one public owner across all
configured scopes; ambiguous legacy configurations deny access until repaired.
Knowing a task ID or content digest supplies no permission. Trusted callers must
carry the authenticated owner, not substitute the artefact actor name.

Archival checks the parent target and all original descendant runs. Unrelated
typed links no longer block the deployment. Old or deliberately unbound links
remain conservative until explicitly mapped or released. Administrators with
an explicit grant for the owner can inspect links, audit a legacy binding to an
owned task, or release a completed consumer. Legacy binding asserts the complete
owning lifecycle and cannot silently replace an existing target.

The operator `artifact_retire` command permanently closes a namespace after
its consumer links are released and its live transfer sessions have drained.
The exact request key, operator and reason survive retries. Namespace status
reports `closed`; verification and already-authorised reads continue. This is
logical retirement: object bytes, manifests, digests, reservations and backup
requirements remain intact. It does not garbage-collect content or refund
charged storage. New publication and active links fail after retirement.

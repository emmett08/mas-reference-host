# Shared worker and event authority

`SharedWorkerController` implements the existing worker RPC contract with
PostgreSQL placement, boot generations, claims, receipt history and operator
reconciliation. It uses the same `SharedAuthority` as the host run journal.
Local directories contain no worker-controller authority in this profile.

```python
from mas_host.shared_control import SharedWorkerController

controller = SharedWorkerController(
    authority,
    {"worker": {"trust_zone": "trusted", "capacity": 2,
                "capabilities": {"analyse": implementation_sha256}}},
    max_outstanding=100, per_principal=10,
)
```

The RPC server accepts this controller through its existing `worker_controller`
argument. The worker CLI also accepts an optional trusted `authority_factory`
registration beside `worker_grants`. The factory returns the provisioned shared
authority; the CLI renews its bound controller lease until shutdown. Network
requests cannot select this factory, PostgreSQL credentials or controller epochs.
The explicit shared-schema migration must precede service startup.

Submission and dispatch recheck the original executing host invocation, exact
input binding, principal and nonrefundable reservation in the same transaction as
the worker transition. Claims and dispatch are disabled by controller drain.
A repeated dispatch command never returns fresh execution permission, including
when the first command committed and its response was lost. Claim expiry before
dispatch permits reassignment; expiry or reboot after dispatch preserves an
unknown obligation and continues consuming worker capacity. Late receipts remain
inspectable evidence without replacing the current assignment's outcome.

Receipt acknowledgement follows durable metadata, original claim validation and
full output verification. Artefact output requires `SharedArtifactStore` bound to
the controller's exact `SharedAuthority`, with the explicit S3 content backend.
A local artefact store cannot satisfy a shared-worker completion contract.
Immutable unresolved links precede the worker receipt transaction. Failure or
fencing between those steps leaves conservative retention, and acknowledgement
replay preserves any already-released exact retention link. The shared repository
rechecks claim authority and host invocation state before accepting an outcome.
Operator reconciliation requires an existing native host-resolution record;
it does not dispatch, refund or infer that an external effect stopped.

`SharedWorkerControl` records revision-checked, attributed drain/resume commands.
Its transaction also changes the global controller drain gate, so task, run and
remote-worker admissions observe the same setting. Already granted effects may
continue and their receipts remain admissible while draining.

`EventDelivery` automatically selects `SharedEventDelivery` for services exposing
a shared authority. Its PostgreSQL outbox retains subscription ownership, the
exact pending page, attempt ownership, retry time and acknowledgement cursor.
Concurrent delivery attempts serialize through a durable attempt lease. Sending
uses no open database transaction. A lost acknowledgement repeats the same
immutable delivery ID and bytes; only the matching attempt can atomically advance
the cursor and remove the page. Stale controllers cannot acknowledge or schedule
a new attempt. Previously admitted sends may reach a receiver after fencing, so
receivers must deduplicate delivery IDs. Each source read and send checks current
task-owner visibility; expired source cursors require explicit resynchronisation.

`test_shared_control.py` supplies real PostgreSQL integration tests for these
boundaries, including shared S3 metadata and corrupt output bytes. The backend CI
runs them with `MAS_TEST_POSTGRES_DSN` and the feature-enabled `MAS_SHARED_BIN`.
A local run without that fixture reports skipped backend tests; it is not evidence
of successful PostgreSQL failover or object-store replication qualification.

The authenticated operator API selects the same shared recovery, deadline, peer,
accounting and control repositories. Lifetime budget amendments increase only
limits, retain original startup baselines and preserve all charges. Shared storage
reports PostgreSQL relation sizes and reserved or unverified object bytes; it does
not report a controller cache as the deployment's durable storage. Retention
previews use shared task, invocation and remote-obligation records. Outstanding
artefact links conservatively block eligibility until released. These previews
are read-only and do not alter receipts or allocations.

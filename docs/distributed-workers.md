# One controller, registered remote workers

The remote worker profile distributes leaf invocation execution while keeping
Rust admission, call allocation and host journals at one controller deployment.
It uses `RemoteWorkerAdapter` inside the ordinary Runner invocation path. Workers
consume an already issued call identity; they cannot submit work as a user,
choose a workflow, delegate host tasks or allocate another call.

This is an initial Linux worker profile with a SQLite controller. It does not
provide multiple active controllers, database replication, failover election or
an OS sandbox for hostile applications. Registered processes are trusted code;
the Rust guardian bounds their lifetime and resource use. Hostile code needs a
separate deployment isolation boundary. See [executable workers](executable-workers.md).

## Placement and authority

Each worker authenticates as its own principal with only `mas:worker`. A separate
controller grant binds that principal to operation names, exact implementation
SHA-256 identities, a trust zone and capacity. Neither token possession alone nor
a worker-supplied capability advertisement creates a grant. A worker registers
its runtime, architecture, boot identity and configured capabilities. Registration
negotiates `mas-worker/2`, supported output profiles and Python 3.10–3.14 (an
explicit `python-MAJOR.MINOR.PATCH` identifier). Missing or unknown protocol,
runtime or output versions fail before a claim or dispatch. The worker verifies
the controller's negotiation response before accepting work. Persisted registrations
without this negotiation must re-register with a fresh boot identity. Placement
requires an exact implementation, output profile and trust-zone match.

The capability identity includes the registered executable descriptor, local
execution deadline and allowed inner reservation. An inner provider invocation
uses the original controller call identity and cannot reserve more than the
controller already charged. The worker cannot retry a provider invocation. Known
`not_sent` and completed-but-invalid observations cross IPC, the receipt spool and
the controller without being converted to unknown; controller retry policy remains
the sole authority for a subsequent charged invocation.

Global and per-user outstanding caps include pending, claimed, dispatched and
unknown jobs. Unknown jobs also retain that worker principal's advertised capacity,
including after a new boot; restarting the worker does not free an uncertain effect. Unknown effects hold capacity until a trusted host-journal resolution is
explicitly imported by the local operator reconciliation command below. Saturation before enqueue records a
known `not_sent` failure; its existing call allocation remains charged.

Capacity, lease duration and grants are pinned in controller metadata. A second
process using different policy is rejected. There is currently no policy
amendment command: plan capability changes as a coordinated deployment upgrade,
and preserve existing state for audit. Drain stops new claims without cancelling
already granted effects.

## Durable boundaries

| Event | Durable behaviour |
| --- | --- |
| Claim expires before dispatch | Another worker may receive a newer fence. The old fence cannot dispatch. |
| Dispatch response is lost | The worker does not execute without a positive response. It retains an unknown observation; it never repeats dispatch to obtain a new grant. |
| Lease expires after dispatch | The job becomes unknown and is never assigned again. Heartbeats demonstrate contact, not that effects have stopped. |
| Worker restarts | A new boot fences its older claims. An old boot cannot register again. Interrupted dispatching/executing records become unknown receipts. |
| Receipt acknowledgement is lost | The worker resends the same durable receipt ID. It does not repeat application execution. |
| Receipt arrives after fencing | The controller retains the authenticated evidence but cannot use it to overwrite the current assignment or task. |
| Controller is restored for audit | Worker authority remains closed. Restore does not permit new dispatch. |

The pinned invocation and receipt explicitly select an output contract:

| Contract | Completion requirement |
| --- | --- |
| `inline-json/1` | Commit bounded inline JSON and evidence in the controller journal. Reference-shaped values remain opaque application data. |
| `durable-artifacts/1` | Commit the receipt only after every declared named output has a valid immutable reference, complete durable bytes matching its SHA-256 and size, a read grant for both the authenticated worker and submitting principal, and a durable retention link. |

Both profiles bound successful output and evidence individually to 65,536 bytes,
with a separate whole-receipt bound. An artefact operation pins its required output
names in the original invocation binding and registered executable capability
identity. Neither the receipt nor the worker can add, omit or downgrade required
outputs. The explicit output envelope is `mas-worker-artifact-output/1`, containing
`result` (an integer-JSON object) and `artifacts` (the declared name-to-reference
map). Ordinary reference-shaped JSON never enables this contract.

The controller's trusted `artifact_repository(namespace, principal)` resolver
selects a configured durable repository, with an immutable `artifact_store_id`
in controller policy. It never follows worker-supplied URLs or file paths. The
resolver must return the requested authenticated principal unchanged. Configure
the same grant and storage authority for publication and completion verification.
Local `ArtifactStore` and its optional object backend use the same interface.
The artefact profile is unavailable until the resolver is configured.

Workers publish objects through the explicit authenticated artefact upload/commit
API before returning their output. A worker needs only narrowly scoped publication
credentials. Controller storage credentials remain at the controller. Failed
verification withholds acknowledgement and leaves the original receipt in the
worker spool. Reconnection resends that receipt, including its original call,
boot, fence, output references and receipt ID; it never repeats the effect. Late
verified receipts are retained for reconciliation and cannot restore expired
execution authority. An object disappearing after an acknowledgement is lost also
blocks repeat acknowledgement until the original bytes are recovered.

An unresolved retention link precedes the receipt commit; a crash between these
writes leaves conservative retention. Successful and stale results retain their
links until explicit application retention policy releases them after consumption
or reconciliation. Losing the worker spool before acknowledgement still leaves
the effect unknown. A storage outage or a hash/grant mismatch never authorises a
replacement call.

The controller commits receipt evidence before acknowledging storage. Workers
keep their private spool until that acknowledgement. Failed uploads stop new
work rather than discarding evidence. A spool defaults to a 16 MiB main SQLite
database and 100 unacknowledged attempts; each complete receipt is bounded below
128 KiB. SQLite's rollback journal needs additional temporary disk space.
The worker enforces the database page ceiling on each write connection, reserves
space for assignment/receipt row updates before claiming, and allocates its
receipt storage before requesting dispatch. Acknowledged rows release reusable
pages; small configured limits can close admission even when the database is
empty. The Rust guardian retains the worker ownership lock until its descendants
are reaped, including after worker-parent death. Oversized
or invalid application output produces a bounded unknown observation. Controller
retention protects live/unknown jobs and non-authoritative receipts. These records
are deliberately not automatically erased when a lease expires.

## Run the local example

Use the repository's pinned Rust and Python prerequisites. Build the release
binaries and install the host package, or set `PYTHONPATH` as below. Start in the
repository directory:

```sh
export PYTHONPATH="$PWD/host:$PWD/examples/host"
export MAS_BIN="$PWD/rust-protocol/target/release/mas"
export MAS_GATEWAY_BIN="$PWD/rust-protocol/target/release/mas-gateway"
python examples/host/remote_echo.py
```

The last command prints the echo capability digest. Compute it on each worker:
registered executable paths, versions and pinned files are part of its identity.
Use reproducible deployment paths and dependencies if several workers should
advertise the same capability. The example pins its own file and the worker
entry module; it does not claim to pin every transitive Python dependency.

Create private token files and a private verifier file using the authentication
setup described in [bridges](bridges.md). Give each worker a distinct principal
and token; the verifier entry's scopes must be `["mas:worker"]`. Keep the raw
bearer token only on its worker. Put the SHA-256 digest in the controller verifier.
A controller configuration is:

```json
{
  "version": 1,
  "auth": {"type": "static", "file": "/private/worker-verifier.json"},
  "public_url": "http://127.0.0.1:8766",
  "max_outstanding": 100,
  "per_principal": 10,
  "lease_ms": 30000,
  "worker_grants": {
    "worker-one": {
      "capabilities": {"echo": "REPLACE_WITH_PRINTED_64_HEX_DIGEST"},
      "trust_zone": "local-demo",
      "capacity": 1
    }
  }
}
```

The controller state directory must be the same deployment directory used by the
Runner executing `RemoteWorkerAdapter`. The controller endpoint alone never
admits a new call.

```sh
python -m mas_host workers controller --config controller.json \
  --state-dir ./controller-state --allow-loopback-http
```

A worker configuration is:

```json
{
  "version": 1,
  "endpoint": "http://127.0.0.1:8766",
  "token_file": "/private/worker-one.token",
  "factory": "remote_echo:operations",
  "trust_zone": "local-demo"
}
```

The factory is trusted operator configuration and returns registered leaf
operations. The registered operation name becomes the worker request operation,
including a contract version such as `solve/1`. The leaf context uses actor
`worker` and caller `controller`; configure structured-provider contracts for
those identities. The authenticated user principal remains in the controller
invocation binding, not a worker-selected caller field. Task data cannot supply import names or executable paths. Start each
worker with its own spool directory and token:

```sh
python -m mas_host workers run --config worker-one.json \
  --state-dir ./worker-one-state --allow-loopback-http
```

For different machines use HTTPS with the existing bounded HTTP transport policy
and an HTTPS reverse proxy at the controller; loopback HTTP is solely for this
example. See [HTTP transport policy](http-transport-policy.md) for private CA and
mTLS configuration. Preserve the worker spool across restarts. Do not share one
spool between worker identities or use multiple live processes for one token.

After an unclean machine reset, a writable repository owner lets SQLite recover
its hot rollback journal before checking the recovered schema and receipt data.
This recovery restores committed storage; it does not replay an application
effect. Read-only audit opens never perform this recovery and may report that
writable recovery is required. Keep database sidecars with their database;
deleting a journal to force an inspection can destroy the retained evidence.
Python repositories and native Rust SQLite owners use `synchronous=EXTRA`, which
also syncs removal of a DELETE-mode rollback journal. SQLite's `FULL` mode alone
can lose a committed transaction across an immediate OS/power failure. The VM
gate uses explicit QEMU `cache=writeback` with disk flushes enabled and requires
the exact original receipt after a hard reset; it does not sync the guest on
behalf of the application before injecting that failure.
These durability settings follow the [SQLite synchronization contract](https://www.sqlite.org/pragma.html#pragma_synchronous)
and [QEMU disk-cache contract](https://www.qemu.org/docs/master/system/qemu-manpage.html).
The report includes the actual QMP-reported cache policy and fails if flushes are disabled.

In the controller's application factory, construct the controller with the same
pinned grants and policy, then install a `RemoteWorkerAdapter` for the appropriate
actor. Bind its user principal from the authenticated task admission context:

```python
from mas_host.distributed import RemoteWorkerAdapter, WorkerController

controller = WorkerController(deployment_dir, worker_grants,
    max_outstanding=100, per_principal=10, lease_ms=30000)
adapter = RemoteWorkerAdapter(controller, principal=authenticated_principal,
    operation="echo", implementation=echo_digest, trust_zone="local-demo",
    reservation_microusd=0)
# Supply adapter in the ordinary Runner adapter map for the leaf actor.
```

For durable output, declare the same output names on the adapter and registered
operation. The leaf application returns the explicit envelope after publication:

```python
from mas_host.artifacts import ArtifactStore
from mas_host.distributed import ARTIFACT_OUTPUT_PROFILE, artifact_output
from mas_host.distributed_worker import WorkerOperation

# Trusted controller configuration; storage_root is not worker input.
def repositories(namespace, principal):
    return ArtifactStore(storage_root, namespace=namespace, principal=principal)

controller = WorkerController(deployment_dir, worker_grants,
    artifact_repository=repositories, artifact_store_id="reports-store/1")
adapter = RemoteWorkerAdapter(controller, principal=authenticated_principal,
    operation="report/1", implementation=report_digest, trust_zone="reports",
    output_profile=ARTIFACT_OUTPUT_PROFILE, required_outputs=("report",))
operation = WorkerOperation.registered("report/1", report_worker_spec,
    output_profile=ARTIFACT_OUTPUT_PROFILE, required_outputs=("report",))
# Inside the registered leaf, after authenticated upload/commit:
output = artifact_output({"status": "ready"}, {"report": committed_ref.to_dict()})
```

For CLI deployment, add `"artifact_factory": "deployment:repositories_factory"`
and `"artifact_store_id": "reports-store/1"` to controller configuration. The
trusted zero-argument factory returns the two-argument repository resolver above.
Use the identical resolver and store identity in the application controller.
Include object storage and manifests in backup and retention procedures. The
worker JSON configuration still contains no unrestricted controller credentials.

The endpoint exposes only authenticated `POST /workers/register`, `claim`,
`dispatch`, `heartbeat`, `receipt` and `drain`. `WorkerClient.call` implements that
bounded RPC surface without automatic effect retries. Application/operator code
can inspect scoped jobs and retained receipts through `WorkerController.inspect`
and `evidence`. A worker token cannot adjudicate evidence or release capacity. First use the
existing operator recovery service or `mas_host admin resolve` to record an
evidence-backed host-journal resolution. Then release the exact remote obligation:

```sh
python -m mas_host workers reconcile --config controller.json \
  --state-dir ./controller-state --operator operator-name \
  --run-id ORIGINAL_RUN_ID --call-id ORIGINAL_CALL_ID
```

This local command requires the controller's private state and pinned configuration.
It accepts no new outcome. It verifies the original allocation, run identity, exact
invocation binding and an existing `resolved` or `retry_allowed` journal decision,
then records an immutable resolution reference/digest and marks only that job
`reconciled`. It releases its outstanding capacity without refunding the allocation
or resuming execution. A later worker receipt remains retained, non-authoritative
evidence and cannot reactivate the job. Normal service recovery is a separate
explicit action; confirmed no-effect permits a new charged call only through the
existing host recovery policy.

## Evidence and release limits

`test_distributed_workers.py` exercises real Runner allocations, authenticated
HTTP, lease expiry, stale boot/fence rejection, receipt replay after a lost
acknowledgement, queue quotas, restore admission closure, and two independent
worker processes executing through the Rust guardian. The provider outcome test
runs known failures through the same registered worker IPC, HTTP, spool and
controller boundaries. `test_distributed_artifacts.py` additionally checks
version rejection, output-profile placement, missing bytes, corruption, grant
failure, named-output completeness, replay, stale receipts and spool recovery.
These tests use local processes. The separate
`scripts/check-multimachine.py` qualification gate boots two Ubuntu QEMU virtual
machines with independent kernels and writable disks, plus a controller process
on the host. It does not count containers or sibling processes as machines. Its
`multimachine.yml` CI job runs on relevant pull requests, including stacked PRs,
and fails if the required runtime, pinned image or any assertion is unavailable.

The guest image has a dated URL and SHA-256 in
`scripts/qualification/ubuntu-cloud-image.json`. QEMU uses TCG, so KVM access,
host mounts, elevated networking and paid machine credentials are unnecessary.
Each guest receives a different worker credential and spool; its application
executes through the real Rust guardian. Host-pinned SSH keys and private reverse
tunnels carry authenticated worker RPC; no guest receives a controller storage
credential. QEMU user networking is restricted to the explicit SSH forwarding.

The gate checks both workers execute on distinct kernel boot identities,
controller RPC process restart with retained authority, a real virtual-NIC
disconnect beyond the lease, rejected delayed heartbeats, lost receipt ACK and
exact receipt replay after an actual guest hard reset, deliberate spool deletion after effect observation, and
unavailable artifact bytes before acknowledgement. It checks original invocation
IDs, one application effect, retained unknown state, retained receipt identities,
outstanding queue limits and nonrefundable call/spending allocations. The artifact
loss case begins with a controller-published, principal-granted object and checks
the completion contract; it is not a qualification of external object-store
replication or the worker publication API.

To reproduce, install `qemu-system-x86`, `cloud-image-utils` and `openssh-client`,
build the release Rust binaries, download the exact image in the lock, then run:

```sh
python scripts/check-multimachine.py --image /absolute/path/ubuntu-24.04.img \
  --state-dir /absolute/path/new-private-machine-evidence
```

The state directory must not already exist. The report starts with `passed:false`
and changes only after every assertion passes. Retain `qualification.json`, the
guest serial logs and controller logs; SSH keys, tokens, VM disks and private
runtime archives are deliberately excluded from CI uploads. An unavailable or
interrupted VM gate is not a passing skip. This source change does not by itself
constitute an accepted machine qualification: retain the actual CI/local report.
Physical-host power loss, independent-storage durability and
certificate rotation remain separate deployment drills. This profile establishes
neither controller high availability nor arbitrary-provider exactly-once effects.

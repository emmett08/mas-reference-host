# Deployment acceptance

Acceptance is evidence about a particular release, host and workload. The
automated checks below exercise the Rust binaries, durable SQLite state and
supervised Python adapters. Mock runs make no provider requests. SPIN's bounded
protocol results remain separate from these implementation checks.

The [fixed reference-load qualification](reference-load.md) adds a minimum
60-second paced workload, at least 100 completions, p99 measurements, process
and RSS observations, retained state growth and deterministic capacity/fairness
checks. It uses private temporary state and no paid provider. Its ceilings are
fixture regression limits, not production SLOs.

## Fault and recovery checks

Build the release binaries and install the host, then run the deployment tests:

```sh
cargo build --locked --release --manifest-path .protocol/rust-protocol/Cargo.toml --target-dir "$PWD/rust-protocol/target" --bin mas --bin mas-gateway
python -m pip install ./host
export MAS_BIN="$PWD/rust-protocol/target/release/mas"
export MAS_GATEWAY_BIN="$PWD/rust-protocol/target/release/mas-gateway"
python -m unittest discover -s host/tests -p test_deployment.py -v
```

Native amd64 and arm64 Linux CI jobs execute these checks, both with the native
Rust binaries and inside the installed Alpine container. The tagged release
workflow repeats the complete host suite against each final architecture image
and checks its actual dynamic musl/SQLite linkage. These are required gates;
support for a particular release requires the jobs to have passed for that
release's commit.

The optional Cursor local image has a separate Wolfi/glibc runtime because the
SDK's native bridge does not support the Alpine/musl image. Its architecture
checks must execute the installed bridge and host on both amd64 and arm64;
building a wheel or importing the SDK alone cannot establish bridge execution.
Before bridge execution, runtime preflight verifies the Node 24.21.0 executable
against the architecture-specific SHA-256 lock, the reviewed native `Headers`
source repair and removal of the obsolete dependency trees. Native installations
must run `python scripts/install-cursor-runtime.py` after installing or
reinstalling the Cursor SDK; the image build performs this step automatically.
Unrecognised or partially repaired files fail preflight. The Wolfi package set
is SHA-256-locked and its signatures are verified during offline installation.
Container checks also inspect shared-library resolution and execute the
installed bridge; image security scans remain a separate gate.
The Cloud adapter uses standard-library HTTPS and runs in the ordinary image.

| Injected condition | Required observation |
|---|---|
| `SIGKILL` of the host after recording provider output | Its direct supervised child terminates; explicit resumption returns the recorded output; the effect file contains one effect. |
| `SIGKILL` during a provider callback | Explicit resumption refuses to repeat the unknown invocation; the effect file still contains one effect. |
| Rust call completion commits but its response is lost | The journal retains the known output; resumption reconciles it with Rust without invoking the callback again. |
| Child-only file growth limit interrupts a real Rust database write | Message admission rolls back, SQLite integrity remains valid, and a subsequent bounded message succeeds. |
| SQLite returns `SQLITE_FULL` while writing the host checkpoint | No provider callback, call reservation or Rust message is admitted. SQLite integrity remains valid. |
| Back up and restore a completed run | Run identity, deployment identity and cumulative charges survive; replay returns the recorded output. |
| Independent host processes compete for one deployment call budget | Admission never exceeds the shared budget; the exercise reports rejected work. |
| Live mode has no usable credential | It returns `blocked_missing_credential` before creating or charging a deployment. |

Storage faults are bounded to a temporary database or child process. The tests
never fill the host disk. Process termination tests establish behaviour at the
observed checkpoint boundaries; they do not simulate physical power loss or
claim that an external provider cancelled work. A restored snapshot is for
inspection and completed-result replay; it cannot grant fresh effect admission.

## Durable bridge acceptance

The [bridge deployment](bridges.md) has separate HTTP/stdio and worker processes.
Acceptance must exercise them together with persistent state, not just call a
protocol converter with an in-memory service double. The host suite includes
real Rust execution and an HTTP process test showing that MCP, A2A and the task
service observe the same durable receipts. The three supplied workflow
topologies each run their deterministic checker. Provider selection remains an
independent dimension; these checks use mocks unless live execution is expressly
enabled through the existing provider acceptance path.

Install `host/requirements-bridges.txt` alongside the locked host dependencies
before running the complete `host/tests` suite. The current lock targets the
documented CPython 3.14 Linux wheel profile. MCP and A2A schema tests require
this dependency set; a skipped schema check does not establish conformance.
The native architecture matrix tests the installed standard and Cursor images
and validates all three bridge Compose configurations and their private
authentication mount.

Use `compose.bridges.production.yaml` for a continuously served deployment.
Its bridge, worker and independent reconciler share the same `bridge-state`
volume. Follow [bridge supervision and recovery](operations.md#supervise-a-bridge-deployment)
for startup, local readiness, alerts and quiescent backup/cutover. Health means
the relevant local process is operational within its declared timing bound;
it does not establish a successful external provider or authentication service.

| Injected bridge condition | Required observation |
| --- | --- |
| Submission reply lost or outer orchestrator activity retried | The same principal/key/content returns the original receipt and run; changed content conflicts. |
| Another authenticated caller supplies a known task or remote key | Principal grants and trusted remote namespace prevent access or reuse. |
| Worker dies before its service result commit | Recovery reuses the original execution revision and Rust journal; known effects are replayed from recorded results. |
| Worker dies during an external effect | The task remains unknown and cannot authorise another submission without evidenced resolution. |
| Permission revoked after interrupted dispatch | No further dispatch occurs; possible earlier effects remain unknown. |
| Cancellation of queued or paused local work | Local continuation closes; a separately tracked active remote receipt remains unchanged and retained. |
| Cancellation races remote completion or loses its response | Intent, uncertain operation and observed terminal state remain distinct; stale polls cannot reopen a terminal receipt. |
| Backup, audit-only restore or retention reaches bridge state | Both bridge repositories survive consistently; restored copies cannot dispatch; unresolved tasks, revisions and operations retain their host evidence. |
| Newer repository schema or invalid persisted state | Maintenance reports failure without overwriting the unsupported database. |
| Ingress is absent or its local task repository is unreadable/unsupported | Local bridge readiness fails without creating state or consuming an admission. |
| Idle worker is killed, stopped or replaced | Worker readiness fails even if its last successful status file remains. |
| Worker executes longer than an idle freshness interval | Readiness reports the bounded `executing` phase; it fails once the workflow wall limit plus operational cleanup allowance is exceeded. |
| Second CLI worker starts against an existing scheduling loop | It exits without replacing the first worker's monitor or scheduling ownership. |
| Continuous Compose services restart while receipts are retained | Ingress returns the original authenticated receipt; the worker and reconciler regain readiness against the same volume. |

Every remote submission, poll, cancellation and continuation must use the
admitted adapter contract. Inspection of local state and an evidenced operator
resolution are separate from a fresh peer observation. Reconciliation health
explicitly reports whether a remote refresh occurred; the local watcher does
not claim that it contacted a peer.

The optional orchestrator gate has its own environment:

```sh
python -m pip install --require-hashes -r host/requirements-build.txt -r host/requirements-orchestrators.txt
python -m pip install --no-deps --no-build-isolation ./host
MAS_TEMPORAL_INTEROP=1 python -m unittest discover -s host/tests -p 'test_orchestrator*.py' -v
```

This exercises the pinned Temporal and LangGraph SDKs, real checkpoint
continuation, and a local Temporal CLI 1.9.1 server. The Temporal test covers
start, terminal observation, cancellation and recovery after discarding an
accepted start reply. Set `MAS_TEMPORAL_SERVER` to an existing CLI binary to
avoid the SDK's pinned download. A default host run may skip optional SDK or
real-server checks; only their explicitly executed gate establishes that
interoperability evidence.

Before deploying a network bridge, also test the organisation's configured
OAuth issuer/audience/scopes, reverse proxy and permitted origins, plus the
actual remote peer's advertised profile. The local static-token examples,
schema fixtures and development orchestrator do not establish those deployment
facts. Streaming subscriptions, push notifications, arbitrary artifact fetching
and hosted LangGraph job management remain outside the advertised profiles.
The single-host worker and SQLite stores provide no high-availability guarantee.

Run the focused local readiness regressions independently with:

```sh
python -m unittest discover -s host/tests -p test_bridge_health.py -v
```

For target-host acceptance, retain the exact image identities, Compose project
and merged configuration, configured immutable policy, authenticated submission
and completion records, ingress-restart result, observed health transitions,
latency distribution and restoration evidence. Test with the intended request
sizes and queue concurrency. Set a maximum queued age and reconciliation delay,
then demonstrate that monitoring detects a stopped worker and failed scan.
Repeat the backup/restore and replacement cutover procedure using separate
storage, proving that the restored copy cannot admit fresh work. A passing mock
deployment leaves the actual provider, OAuth, TLS and remote-peer checks pending.

## Repeatable load and bounded soak

Run twelve independent three-agent mesh jobs, with at most three active hosts:

```sh
python -m mas_host.acceptance --runs 12 --concurrency 3 \
  --state-dir .mas-acceptance --report evidence/deployment-acceptance.json
```

For a longer exercise, bound both admission time and job count:

```sh
python -m mas_host.acceptance --runs 500 --concurrency 4 --soak-seconds 300 \
  --state-dir .mas-acceptance-soak --report evidence/deployment-soak.json
```

The command stops submitting work at the first applicable limit and drains
already admitted jobs under their run deadlines. An existing deployment retains
its cumulative limits across repeated executions. Use `admin health` to inspect
those limits before a soak; budget exhaustion is a reported admission failure.
In mock mode, omitting `--state-dir` uses a temporary deployment and removes its
private state after the public report has been produced.

Reports use schema `mas-deployment-acceptance/1` and contain:

- Workload, run count, concurrency, elapsed time and success/error counts.
- Integer millisecond latency summaries, including nearest-rank p50 and p95.
- Python, SQLite and platform versions, CPU count and SHA-256 identities of the Rust binaries.
- Deployment policy and cumulative run, call and micro-US-dollar reservations before and after execution.
- Individual run identifiers, outcomes and observed unknown-call counts.

The workload is deterministic integer invoice checking with simulated inference.
Measured latencies include process creation and durable protocol operations.
Use a representative application and target deployment to establish an
application-specific latency or throughput objective. A small passing run
establishes execution of the declared checks, not production capacity.

The checked-in [smoke report](https://github.com/emmett08/mas/blob/main/evidence/deployment-acceptance.json) and
[bounded soak report](https://github.com/emmett08/mas/blob/main/evidence/deployment-soak.json) record the executed
workloads. The [initial soak](https://github.com/emmett08/mas/blob/main/evidence/deployment-soak-initial.json) exposed
a race when SQLite removed a rollback journal during a state-size scan. The
scanner now takes one metadata snapshot per entry and tolerates an entry that
has disappeared; symlinks and special files still fail validation. A targeted
regression and the repeated soak cover that repair. Keep this failed evidence
distinct from the subsequent passing report.

## Explicit live-provider smoke test

Install the locked provider dependencies using the release instructions. Supply
`OPENAI_API_KEY_FILE` as a private, readable file, and select models available to
your account. The following command explicitly permits one mesh run with at most
two OpenAI submissions:

```sh
python -m mas_host.acceptance --live --runs 1 --concurrency 1 \
  --provider-mode openai \
  --state-dir .mas-live-acceptance --report evidence/deployment-live.json
```

For Cursor, install `host/requirements-cursor.txt` on the supported glibc
runtime, supply `CURSOR_API_KEY_FILE` and explicitly set both Cursor model names:

```sh
python -m mas_host.acceptance --live --runs 1 --concurrency 1 \
  --provider-mode cursor \
  --state-dir .mas-cursor-acceptance --report evidence/deployment-cursor-live.json
```

For a Cursor planner and OpenAI reviewer, supply both key files and models, then:

```sh
MAS_PLANNER_PROVIDER=cursor MAS_REVIEWER_PROVIDER=openai \
python -m mas_host.acceptance --live --runs 1 --concurrency 1 \
  --provider-mode mixed \
  --state-dir .mas-mixed-acceptance --report evidence/deployment-mixed-live.json
```

Per-role provider environment settings override live mode defaults. Inspect
them before each exercise; the report records the actual selected providers.
Live acceptance requires both roles to use live providers. Mock-only execution
remains the default when `--live` is absent.

Live mode rejects concurrency, multiple runs and soak settings. The host records
an expenditure reservation before each provider callback; the default OpenAI
adapter reserves 100,000 micro-US-dollars per call. These are conservative local
allocations, not a measurement of the provider's invoice. Provider account
controls and currently applicable pricing determine the actual financial limit.
`MAS_CALL_RESERVATION_MICROUSD` configures the positive allocation per call;
`MAS_PLANNER_RESERVATION_MICROUSD` and `MAS_REVIEWER_RESERVATION_MICROUSD` may
override it. A Cursor submission may cause SDK-managed internal model retries.
The two-submission limit therefore does not establish two underlying requests
or a hard spending ceiling. Reports mark internal provider requests as unmeasured.

Live mode also requires an explicit `--state-dir` before providers are created
or any work is admitted. Use persistent private storage and retain it after both
successful and unsuccessful exercises. In particular, an unknown outcome needs
its original journal and spending reservations for reconciliation; the command
never removes the supplied state directory. Do not run live acceptance on an
ephemeral CI runner unless a separate durable recovery mechanism retains this
private state. Ordinary CI artefacts must not contain journals, capabilities or
credentials.

An unavailable credential, provider refusal, deadline, malformed output or
incorrect model answer prevents a passing report. A lost response can leave an
unknown outcome requiring inspection and reconciliation. Live acceptance is
complete only when a report records an actual successful provider execution;
mock tests and credential-mount checks do not supply that evidence.

## Durable Cloud job acceptance

The [durable-job commands](providers.md#durable-cursor-cloud-jobs) provide a
separate acceptance path. The local-provider mesh exercise does not cover
remote repository work. For a repository and prompt you have authorised:

1. Submit one job with a stable local ID and retain its state directory.
2. Check that the receipt identifies the saved agent and exact run.
3. Reconcile that run until a terminal status is observed, then review the
   returned result and repository changes independently.
4. For a cancellation exercise, use a separately authorised job and confirm a
   remote terminal observation after requesting cancellation. An acknowledgement
   alone cannot pass the cancellation check.

Offline fault tests cover lost create responses, process restart, refusal to
relaunch an uncertain job, mismatched run identities, conflicting terminal
observations and cancellation uncertainty. The finite
[Cursor job model](https://github.com/emmett08/mas/blob/main/formal/cursor-jobs-notes.md) checks the corresponding
safety properties and defect mutations. Neither supplies live account or
repository acceptance. A cloud job can outlive local expiry; its retained
receipt and reconciliation state remain necessary after the submit command ends.

Before unattended deployment, retain the passing fault report, a load report
from the target host, the bounded live report for the configured provider and
models, and the release's dependency/security check results. Re-run the affected
checks when execution, persistence, adapters or deployment settings change.

# Operating a bounded single-host deployment

The supported operational boundary is one private state directory on a local filesystem, shared by trusted hosts and a Rust/SQLite broker. The host journal records workflow checkpoints; the deployment ledger admits work against lifetime quotas. Linux file locks exclude concurrent execution of the same run and coordinate backups and retention. Container CPU, memory and PID limits bound the supplied Compose services. Adapters remain trusted application code.

## Start and inspect

Run the existing demonstration command, then inspect the shared deployment:

```sh
docker compose up --build --abort-on-container-exit --exit-code-from mesh
docker compose run --rm mesh admin --state-dir /state health
docker compose run --rm mesh admin --state-dir /state runs
```

The `runs` command supplies retained directory names. Prefix one with `/state/` for inspection:

```sh
docker compose run --rm mesh admin inspect /state/RUN_DIRECTORY
```

The same commands work outside containers as `python -m mas_host admin ...` with `mas` and `mas-gateway` on `PATH`. `MAS_BIN` selects the Rust administrative binary. `MAS_STATE_DIR` supplies the default state directory. State and backup directories must have mode `0700`; copied files have mode `0600`. Reports and backups may contain application data and agent capabilities. Keep them private.

## Supervise a bridge deployment

Create the private credentials using [the bridge setup](bridges.md), then use
the bridge-specific production overlay. Initialise any custom deployment policy
in `bridge-state` before this first start; otherwise the documented default
lifetime limits become durable and require explicit audited amendments:

```sh
export MAS_BRIDGE_CREDENTIALS="$(cat secrets/bridge-credentials.json)"
docker compose -f compose.bridges.yaml -f compose.bridges.production.yaml up --build -d --wait
docker compose -f compose.bridges.yaml -f compose.bridges.production.yaml ps
```

The overlay supervises `bridge`, `worker` and `bridge-reconciler` with
`restart: unless-stopped`, bounded resources and separate health checks. All
three use **`bridge-state`**. The `compose.production.yaml` overlay belongs to
the batch demonstration and watches `mesh-state`; it does not reconcile bridge
work. The bridge remains published only at host loopback. Configure the real
reverse proxy, TLS, OAuth and permitted origins separately before admitting
external traffic. Retain the same Compose project name and volume when
restarting or upgrading; changing the project name creates a different
deployment and lifetime budget.

For the mixed provider profile, supply its provider secrets and model settings,
set `MAS_BRIDGE_CONFIG=/app/examples/bridges/mixed.json`, and put
`-f compose.bridges.mixed.yaml` between the base and production files. Keep
that exact configuration on subsequent Compose commands. Provider/model
changes still require a new workflow name/version or separately budgeted
deployment. The reconciler uses the ordinary image and needs no provider or
inbound authentication secret. Retain the credential environment variable for
Compose operations; unset it when finished. Application and provider secrets
remain private runtime-owned files.

Read the three health dimensions inside their own containers:

```sh
docker compose -f compose.bridges.yaml -f compose.bridges.production.yaml exec bridge \
  python -m mas_host bridge health --config /app/examples/bridges/mesh.json --state-dir /state
docker compose -f compose.bridges.yaml -f compose.bridges.production.yaml exec worker \
  python -m mas_host bridge worker-health --state-dir /state --max-age-seconds 30
docker compose -f compose.bridges.yaml -f compose.bridges.production.yaml exec bridge-reconciler \
  python -m mas_host admin --state-dir /state reconciler-health --max-age-seconds 30
```

Use the selected configuration path in the first command. Ingress readiness
checks the local HTTP accept loop's public Agent Card and validates the private
ledger and task repository without admission or migration. It neither exposes
task state through an unauthenticated endpoint nor duplicates a client bearer
secret into the server. It does not test the organisation's OAuth service,
public TLS path or provider availability; exercise those through the authenticated
deployment acceptance checks.

Worker readiness requires the owning process's Linux boot identity and start
ticks, the original state directory, an exclusively owned scheduling loop and
recent polling. While work executes, it reports `phase: executing` until the
registered `wall_seconds` plus 90 seconds of bootstrap/cleanup allowance have
elapsed. That allowance controls alerting only: it cannot extend the protocol
deadline or establish remote progress. There is no heartbeat thread running
beside the worker's process forks. A stopped, killed, stale or replaced worker
fails readiness; a second continuous or `--once` CLI worker cannot take over a
live loop. Run each health command in its service's PID namespace. A read-only
deployment report remains available even when its expenditure or disk limits
close admission.

Configure the host's monitoring system to collect these statuses and the
private `admin health` report. At minimum, act on:

| Observation | Operator response |
| --- | --- |
| Any container repeatedly restarts or becomes unhealthy | Inspect its bounded logs and durable state; check CPU, memory, PID and storage limits. Docker restarts exited containers, not unhealthy running processes. |
| Reconciler's successful scan age exceeds 30 seconds | Investigate failed or slow scans and retained run count; adjust the threshold only against the required expiry-reconciliation latency. |
| Worker poll is stale or its executing allowance expires | Inspect the run and supervisor before restarting it. Preserve unknown effects for explicit reconciliation. |
| `admission_open` is false, or disk/budget headroom approaches its limit | Close new submissions and inspect the lifetime budget or storage threshold; archive eligible history or apply an approved budget amendment. Restarting does not replenish quotas. |
| Queued-task age exceeds the workload's accepted latency | Investigate the single worker's capacity and current task; increasing HTTP replicas does not increase scheduling capacity. |
| Local `unknown`, remote unknown operations or pending cancellation persist | Reconcile with the actual peer using admitted lifecycle operations and retained evidence. A successful local scan is not a fresh peer observation. |

Set numerical queue-latency and capacity thresholds from the target workload's
acceptance run. The supplied mock invoice results do not define an SLO for
another application. Preserve and use the accepted immutable image identities
for upgrades; local build tags alone do not identify a release.

## Set a deployment policy

Before the first run, initialise a policy with a JSON file:

```json
{
  "max_runs": 1000,
  "max_calls": 10000,
  "max_reserved_microusd": 100000000,
  "min_free_bytes": 67108864,
  "max_state_bytes": 1073741824,
  "per_call_reservation_microusd": 100000
}
```

```sh
python -m mas_host admin --state-dir /absolute/private/state init --policy policy.json
```

These are also the default limits. Each new run and each provider invocation must be admitted by the same SQLite deployment ledger. Concurrent local hosts share those transactions. Charges survive process restart and retention deletion. Reusing an existing identity is idempotent; changing its expenditure allocation is rejected. Mock/local calls may reserve zero expenditure while still consuming a call admission.

Lifetime allowances can be increased explicitly after draining active runners.
Write a private JSON file containing absolute new limits, for example
`{"max_runs":2000,"max_calls":20000,"max_reserved_microusd":200000000}`, then run:

```sh
python -m mas_host admin --state-dir /absolute/private/state budget-history
python -m mas_host admin --state-dir /absolute/private/state amend-budget \
  --request-key capacity-2026-10 --principal operator-alice \
  --reason 'Approved next workload allocation' --expected-revision 0 --limits new-limits.json
```

The local operator identity is an attribution supplied by this trusted CLI,
not remote authentication. The revision comparison prevents competing changes;
the request key makes a repeated identical amendment return its original receipt.
Changing a keyed amendment conflicts. Amendments may increase only lifetime run,
call and reserved-expenditure limits. They cannot decrease a limit, erase a
charge, change storage policy or reactivate an audit-only restore. Long-lived
ledger handles read the current policy before new admission.

`max_reserved_microusd` measures **reserved allocation**, in millionths of a US dollar. A default invocation allocates 100,000 microUSD ($0.10). This is an operator allowance, not a verified price estimate or the provider's invoice. An unknown outcome retains its full allocation. Configure suitable per-call reservations and a provider-account spending limit before a live run. The runtime cannot prevent spending through an unrelated client, external account or another state directory.

Changing an environment variable or restarting a container cannot replenish the ledger. An exhausted deployment stops admitting work until an explicit budget amendment increases the relevant allowance. Storage thresholds remain fixed. A new deployment creates a separate lifetime budget, so create one only after reconciling the old deployment and approving its replacement budget. Removing a ledger while retaining run databases fails closed. Deleting the entire deployment is an administrative destruction of its audit and cannot be detected by an independent replacement directory.

Disk thresholds are checked before admissions. They close admission when free space falls below `min_free_bytes` or retained state reaches `max_state_bytes`. Writes already admitted can cross the threshold; use a filesystem quota for a strict storage ceiling. Watch `free_bytes`, `state_bytes`, usage and admission state through the read-only `health` command. It never creates a deployment or migrates its database. Backups occupy separate storage and need their own retention policy.

Both durable services check these thresholds before persisting a new submission
or continuation. An identical retained request can still return its receipt;
cancellation and evidenced recovery remain available above the threshold.
Deployment-backed artefact stores also check before publishing new content or
reference metadata. These recovery paths still require physical space for their
records. A read-only ledger handle or restored audit-only deployment rejects
service mutations independently of available space.

## Reconcile expiry independently

A host crash must not leave task expiry dependent on the next adapter call. The watcher calls Rust reconciliation independently:

```sh
python -m mas_host admin --state-dir /absolute/private/state reconcile
python -m mas_host admin --state-dir /absolute/private/state watch --interval 5
```

A single run can be reconciled with `reconcile --run-dir /absolute/private/state/RUN_DIRECTORY`. The watcher prints one JSON record per scan, including disk/quota health, handles SIGINT/SIGTERM, and bounds each Rust command to ten seconds. `--iterations 3` performs a finite acceptance check and returns a nonzero exit code if any scan fails. Only one watcher may own a state directory at a time.

A separate readiness command checks that the continuous watcher has recently completed a successful scan and that the same process remains alive:

```sh
python -m mas_host admin --state-dir /absolute/private/state reconciler-health --max-age-seconds 30
```

Readiness fails for missing, failed, stale or stopped watchers. The status records the Linux boot identity, process start ticks and the state directory identity, so a reused PID or copied heartbeat cannot establish readiness. It uses monotonic elapsed time. Admission quotas remain a separate health dimension: an exhausted deployment can still have a healthy reconciler. A finite watcher stops after its checks and therefore is not subsequently ready as a continuous service. A scan of many slow run databases can exceed the freshness threshold; choose that threshold according to the required reconciliation latency. Reconciliation closes expired protocol tasks; it does not establish that a remote provider stopped or that an external effect was absent.

The production Compose overlay supplies a separate reconciler service. Its process can continue reconciling retained runs after a host exits. Its Docker health check uses `reconciler-health`, with a 30-second freshness threshold, rather than treating a running process as proof of successful reconciliation. Docker marks a failed service unhealthy; configure operational alerting for that status (the restart policy alone does not restart unhealthy but running processes). Stop this service as well as hosts for predictable operator maintenance; snapshot locking rejects a busy deployment rather than copying inconsistent databases.

Health reports include local `task_service` state counts and `remote_tasks` state, pending-action and unknown-operation counts. These are persisted observations, not fresh provider status checks. Reconciliation validates the bridge repositories and reports `bridge_observations.remote_refreshed: false`; an unsupported schema or unreadable repository makes the scan fail. A remote refresh must use the admitted bridge lifecycle operation so polling cannot bypass deployment accounting.

The `composition` health entry reports workflow instance, node and attempt states
and retained allocations. Reconciliation reports `composition_observations` and
validates the composition schema without creating missing state. An unsupported
schema, unreadable database or invalid durable state makes the scan fail. The
watcher records a degraded scan and a bounded diagnostic when health inspection
fails, so an unusable workflow repository cannot establish readiness.
Counts include retained historical revisions. They report persisted observations,
not worker liveness or progress; reservations are not actual provider expenditure.

Service cancellation closes queued work or a paused local continuation. Cancelling a running task records intent for its worker; cancelling an unknown task retains that uncertainty. A local `cancelled` service receipt does not confirm that a separate remote task stopped. Remote cancellation uses its own admitted lifecycle operation, and its receipt continues to protect the associated evidence until reconciled. Revoking a caller's permission prevents further dispatch but preserves an interrupted execution as unknown when earlier effects may have escaped.

## Recover an interrupted workflow

Inspect the run before replaying it. A `prepared` invocation has no recorded provider entry; `executing` or `unknown` requires reconciliation with the external system. A completed checkpoint supplies its saved result on replay. Workflow version and call order must match the frozen run.

For a custom host, reopen its original run with the same trusted adapters and workflow version:

```python
with Runner.resume(run_directory, adapters, workflow_version="invoice-v1") as runner:
    result = runner.request("checker", "planner", "invoice.plan", original_input)
```

Preserve the original top-level input. Changing it is rejected. Resume preserves the original protocol deadline and budgets; it cannot reopen an expired task. See [host-runner.md](host-runner.md) for the adapter contract and execution deadlines.

When external evidence establishes a result, record it explicitly:

```sh
python -m mas_host admin resolve /absolute/private/state/RUN_DIRECTORY CALL_ID \
  --decision completed --evidence provider-reconciliation.json --output verified-output.json
```

The evidence file must be a nonempty JSON object documenting the basis for the decision; the output file must contain the verified output object. Keep keys, credentials and sensitive raw exception text out of evidence. The command requires the run to be stopped and exclusively locked.

If the external system establishes that no effect occurred, the operator may instead authorise a new, independently charged attempt:

```sh
python -m mas_host admin resolve /absolute/private/state/RUN_DIRECTORY CALL_ID \
  --decision confirmed_no_effect --evidence provider-reconciliation.json
```

The decision is retained in an append-only resolution history. Original Rust invocation evidence remains unchanged. A timeout alone does not establish `confirmed_no_effect`. A provider response ID can help with reconciliation, but provider-specific retention, retrieval and side-effect semantics determine what it proves. The included OpenAI adapter disables automatic SDK retries; the operator must establish the outcome of uncertain requests before any fresh invocation.

The service recovery inbox addresses work by its public task or composition ID:

```sh
python -m mas_host admin --state-dir /absolute/private/state recovery-inbox \
  --config bridge.json --operator operator-alice --owner alice TASK_ID
python -m mas_host admin --state-dir /absolute/private/state recovery-resolve \
  --config bridge.json --operator operator-alice --owner alice TASK_ID CALL_ID \
  --request-key reviewed-receipt-1 --decision completed \
  --evidence observed-receipt.json --output verified-output.json
```

The evidence object has exactly `source`, `reference` and `observed_at_ms`;
`source` is `provider_receipt`, `provider_query` or `operator_attestation`.
The inbox distinguishes cancellation intent, stopped local execution, remote
cancellation observations and unresolved effects. It rechecks the configured
owner grants. These trusted local commands attribute an operator decision;
they do not authenticate an operator supplied by a network caller. Resolution
does not resume execution. Use the service's authorised `recover` operation
after reviewing the resulting state. Request-key replay returns the same
decision, while changed evidence or output conflicts.

## Back up and restore

Stop active runners. Backups take exclusive deployment and run locks, then use SQLite's backup API for each database, copy private configuration, and produce a SHA-256 manifest. A busy run causes the operation to fail. Use an absent or empty destination outside the state tree:

```sh
python -m mas_host admin --state-dir /absolute/private/state backup /absolute/private/backup-2026-10-13
python -m mas_host admin --state-dir /absolute/private/restored restore /absolute/private/backup-2026-10-13
```

Restoration verifies the complete file set, content hashes and SQLite integrity before writing into an empty destination. Symlinks and unsafe paths are rejected. Hashes detect corruption; authenticate the backup's origin separately. The snapshot preserves the deployment identity, quotas, private capabilities, workflow checkpoints and unresolved calls. Ephemeral reconciler status is excluded.

**Restored deployments are audit-only.** Their persistent ledger blocks every new call admission and every new run. Inspection, expiry reconciliation and replay of already completed root results remain available. This prevents an old snapshot from regaining budget consumed after the backup or becoming a second active copy of the same spending authority. This release deliberately has no command to reactivate a restored spending ledger. Reconcile external effects and account usage, stop the original deployment, and initialise a separately budgeted deployment for any authorised new work. Unknown work is never automatically replayed from a backup.

The original deployment remains unchanged by backup or restore. Treat backups as private operational evidence and periodically run the restoration acceptance check on separate storage.

Snapshots include `task-service.sqlite` and `remote-tasks.sqlite` when present: submission identities, continuation revisions, event cursors' source records, remote receipts and unresolved lifecycle operations survive together. Service scheduling and remote lifecycle operations share the deployment lock, so an active operation prevents a partial snapshot. New bridge run references are relative to their deployment. Restored references resolve only within the copied tree, and the audit-only marker prevents remote submission, polling and cancellation. Earlier Cursor records retain their absolute evidence paths; retention protects the corresponding copied run without opening or modifying the source path.

For bridge maintenance, stop new ingress, then drain or explicitly cancel
queued work and inspect all running or unknown tasks. Stop `worker` and
`bridge-reconciler` before taking the snapshot. Mount separate backup storage,
owned by runtime UID 10001 with mode `0700`, at `/backups` in a one-off bridge
container. With `MAS_BRIDGE_BACKUPS` set to that absolute host directory:

```sh
docker compose -f compose.bridges.yaml -f compose.bridges.production.yaml stop bridge worker bridge-reconciler
docker compose -f compose.bridges.yaml -f compose.bridges.production.yaml run --rm --no-deps \
  -v "$MAS_BRIDGE_BACKUPS:/backups" bridge admin --state-dir /state backup /backups/quiescent-snapshot
```

Use a new empty snapshot destination on each backup. This command captures
the bridge volume; using the batch `mesh` service would capture different
state. The two ephemeral readiness records are excluded. Restore into separate
private storage and verify identities, charges, result replay and unresolved
receipts before treating a backup as usable. The original deployment remains
stopped during a replacement cutover.

A replacement that must accept fresh work needs a newly initialised deployment
with an explicitly approved lifetime budget and frozen workflow versions.
Inspect the restored audit copy, reconcile all possible external effects and
record which unexecuted inputs are authorised for new submission. Give those
submissions new request identities in the new deployment; neither copying an
old spending ledger nor replaying an unknown effect is a recovery procedure.
Complete authenticated readiness and a representative bounded workload before
switching traffic. Keep the old deployment stopped and its receipts available
for reconciliation. No command in this release reactivates restored spending
authority or provides automatic failover.

## Repository formats and upgrades

The host-side repositories have independent application identities and versions:

| Repository | Application identity | Current version | Compatible previous format |
| --- | --- | --- | --- |
| Deployment ledger | `MASD` (`0x4d415344`) | 4 | Identified versions 2–3 and unidentified version 1 |
| Workflow journal | `MASJ` (`0x4d41534a`) | 2 | Identified version 1 and unidentified version 0 |
| Cursor cloud jobs | `MASC` (`0x4d415343`) | 1 | None |
| Durable task service | `MAST` (`0x4d415354`) | 2 | Version 1 |
| Remote bridge tasks | `MASR` (`0x4d415352`) | 1 | None |
| Workflow composition | `MASW` (`0x4d415357`) | 2 | Version 1 |
| Definition registry | `MASV` (`0x4d415356`) | 1 | Optional; old records retain their original identity |
| Local scheduler capacity | `MASQ` (`0x4d415351`) | 1 | Optional |
| Remote worker controller | `MASX` (`0x4d415358`) | 1 | Optional |

A writable open validates the exact declared tables, constraints and indices, and the saved metadata, before transactionally migrating a recognised previous format. Migration preserves deployment identity, policy, charges, checkpoints and resolutions. Unknown application identities, versions, extra objects or changed layouts are rejected before database mutation. Read-only health and inspection recognise supported previous formats without migrating them. Neither operation creates missing state. The Rust protocol repository retains its own format and migration rules.

Take a quiescent backup before an upgrade. A newer host-side format must never be opened for writing by an older release. Follow [the release procedure](releases.md); an audit-only restore cannot regain spending authority.

## Retain evidence and remove completed runs

Retention is explicit and defaults to a preview:

```sh
python -m mas_host admin --state-dir /absolute/private/state prune --older-than-seconds 2592000
python -m mas_host admin --state-dir /absolute/private/state prune --older-than-seconds 2592000 --apply
```

A run is eligible only when its retention age since the latest state-file change is reached, no host holds its lock, every protocol task is terminal, and the host journal establishes the outcome of every invocation. Unknown or prepared invocations block deletion. Every execution revision belonging to a queued, running, input-required, auth-required or unknown service task is retained. Remote nonterminal tasks, pending lifecycle actions and unknown operations also protect their host evidence, even if another observation reports the remote task terminal. Deletion records a permanent deployment-ledger tombstone before removing files. Lifetime run, call and expenditure charges are never refunded. An interrupted deletion may leave files with a recorded tombstone; retain that ledger entry and remove remaining files only after operator inspection.

Detailed task and composition history can be archived separately before pruning
their unreferenced run files. Both commands preview by default; repeat with
`--apply` after reviewing the candidates:

```sh
python -m mas_host admin --state-dir /absolute/private/state archive-tasks --older-than-seconds 2592000
python -m mas_host admin --state-dir /absolute/private/state archive-workflows --older-than-seconds 2592000
python -m mas_host admin --state-dir /absolute/private/state compact-history
```

Archival checks every execution reference, unsettled provider/remote work and
retained artefact links under exclusive deployment ownership. A terminal parent
alone is insufficient. The compact receipt retains principal, request identity,
definition identity, metadata, final state and result digest. Legacy request
keys remain indefinitely; identical resubmissions return that original archived
receipt and conflicting input is rejected. No new work starts from an archived
receipt. Result payloads and detailed events become unavailable: callers receive
`result_unavailable` or `event_cursor_expired` and can inspect the retained
receipt. New tasks no longer count archived payload records against detailed
history capacity. Lifetime allocations remain charged.

SQLite reuses freed pages internally; `compact-history` reclaims physical file
space after archival. It requires stopped runners and temporary free space for
SQLite's replacement file. It changes no logical records. Active remote worker
jobs and unreviewed late worker receipts also protect run evidence. Audit-only
restores permit previews, but reject archival, compaction and budget amendments.

These controls support a bounded single-host service. They do not provide multi-host consensus, hostile-code isolation, automatic provider refund accounting, or a highly available distributed database. The acceptance suite exercises process interruption, quotas, disk thresholds, backup/restore, sustained local load and explicit live-provider acceptance separately.


## Principal queue and allocation quotas

Trusted bridge configuration may set independent limits for each principal:

```json
"principal_quotas": {
  "team-a": {"max_pending": 100, "max_calls": 5000,
             "max_reserved_microusd": 50000000},
  "team-b": {"max_pending": 20, "max_calls": 1000,
             "max_reserved_microusd": 10000000}
}
```

`max_pending` bounds outstanding logical submissions across the leaf and
composition repositories together. Running, blocked, paused and unknown work
retain capacity. A terminal composition with a running or unknown child retains
capacity until that child is resolved. A continuation belongs to its existing
logical submission. Completing or safely cancelling a submission releases this
queue allowance; archival is unnecessary. Replaying an accepted request still
returns its original receipt when the limit is full. Another principal's
allowance remains independent, subject to deployment-wide bounds.

`max_calls` and `max_reserved_microusd` are lifetime allocation ceilings, separate
from concurrency slots and actual provider invoices. The deployment ledger
checks the principal and deployment ceilings in one call-admission transaction.
Its immutable run-to-principal binding comes from the trusted service assignment;
application input cannot choose another owner. Composition ancestors retain their
own admission controls. A denied downstream admission never refunds a retained
ancestor reservation. Failure, retries, process death, archive and restart do not
refund accepted call allocations. Each separately admitted retry consumes another
call and reservation.

The policy is stored in deployment ledger version 4. Unchanged configuration and
omitted configuration use the installed limits. Initial installation requires a
quiescent deployment; changing or clearing installed limits is rejected. The
existing deployment budget-amendment API changes only deployment ceilings and
cannot silently increase principal ceilings. Principals without configured limits
remain subject to the deployment and workflow limits.

Migration preserves existing call rows and IDs. Historical calls whose owner is
unknown are conservatively counted against every configured principal; this does
not duplicate the deployment's total consumption. A trusted resumed service
assignment can attribute its original run to its retained principal. Unowned
legacy runs cannot receive fresh call admissions while principal quotas are
configured. Purged call evidence and attribution remain in the ledger and backup;
a restored deployment remains audit-only.

Trusted operator code can inspect `ledger.principal_policy(principal)` and
`ledger.principal_usage(principal)`. Usage explicitly includes
`unattributed_calls` and `unattributed_reserved_microusd`.
`principal_quotas.pending_count(ledger, principal)` reports outstanding logical
submissions. These identities are never emitted as metric labels.


## Period allocations and provider charge observations

`AccountingAdministration(ledger)` installs audited fixed UTC allocation
windows for the deployment or a named principal. `PeriodPolicy` declares
`period_seconds`, `start_epoch`, `max_calls` and `max_reserved_microusd`.
Installation backfills retained calls, including conservative attribution of
legacy calls whose owner is unknown. It requires quiescent maintenance ownership.
Period policies are immutable; repeated identical administration keys replay.
All applicable period and lifetime limits commit with the original call
allocation. Failed checks roll back together. A repeated call identity never
spends in another period. A backwards clock cannot reopen a previous period.

Provider invoices can be reconciled through attributed `record_charge`
observations: provider, unique charge ID, original run/call ID, integer
micro-USD charge, observation time and an evidence reference. Changed content
under a retained charge identity conflicts. `reconcile` reports observed charges
against the original reservation and flags excess. A partial invoice is never
reported complete. Lower observed charges do not refund allocation, and higher
charges do not erase an already admitted effect. Each invocation retains at most
1,024 charge observations; new observations also require storage admission.
The ledger v4-to-v5 migration adds these tables without rewriting old identities
or lifetime charges. Audit restores remain read-only.

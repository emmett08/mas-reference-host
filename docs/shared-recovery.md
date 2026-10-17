# Shared PostgreSQL recovery

The shared profile uses explicit schema migrations: protocol 1, leaf authority
2, controller leases 3, service repositories 4, period accounting 5, recovery
guards 6, Cursor Cloud jobs 7, command capacity 8, lifecycle retention 9, native
payload archival 10, artifact transfer sessions 11 and typed artifact retention 12. Existing versioned state is upgraded; it is never reinitialised or
silently replaced. The optional private command transport accepts up to 1 MiB
of strict integer JSON. Native protocol messages retain their 65,536-byte bound.

## Declared reference qualification targets

Before running the recovery tests, the reference targets are:

- zero loss of acknowledged run/message/call/request identities, cumulative
  allocations and uncertain effects;
- same-database controller recovery gate within 60 seconds, excluding independent
  reconciliation of unresolved provider outcomes;
- a separate populated controlled-cutover rehearsal within 60 seconds, measured
  from source sealing through dump, restore, external object restoration,
  verification, activation, explicit admission reopening and a fresh task's
  successful completion;
- a restored archive remains closed to all service admission until its exact
  final source state, shared objects and external fencing proof are verified.

These are executable fixture acceptance gates, not a production availability
SLO. PostgreSQL HA/WAL retention, storage replication and fencing integrations
must supply deployment-specific evidence. A database lease cannot fence another
independently writable database, and a provider may finish an earlier admitted
effect after controller ownership changes.

## Controller handover

`SharedAuthority.start_renewal()` starts a private renewal subprocess, preserving
the single-threaded supervisor/fork boundary. Its configuration travels over
private stdin; a lifetime pipe and parent PID check stop it when its controller
exits. Renewal failure does not elect a new controller or reuse a new epoch.
Normal operations validate both epoch and lease in the same transaction.
Native replies distinguish `stale_protocol_authority` and
`controller_lease_expired` from unavailable or malformed transport replies.
The Python port preserves these fixed codes in `SharedAuthorityRejected`, a
`RunnerError` subclass; it exposes no database or credential details.

A live controller may explicitly `handover(next_controller)`. After its lease
expires, a successor with the exact prior binding may `takeover(next_controller)`.
The compare-and-set changes epoch once. Every old handle is fenced, including
read access. Existing run, request and invocation IDs remain unchanged. Pending
unknown effects stay charged and require their original evidence/recovery path.

## Final archives and isolated audit restores

`SharedBackupManager.archive()` is a final-source operation, intended for a
controlled relocation/recovery cutover. It permanently seals the source before
running native `pg_dump --format=custom`. It does not take a live rolling backup
and then claim that later acknowledgements were captured. The seal is signed
with an externally retained recovery key, separate from the controller key.
It contains source database identity, epoch, schema layout, every authoritative
table's row count/digest, sequence values and the shared object inventory.

Sealing is serialised with all source mutations. Afterwards, service calls,
renewals, handover and takeover are rejected on that source. A failed dump leaves
it sealed. Retrying the same seal identity can create another archive; it cannot
reopen admission. The archive manifest additionally signs the archive byte
length and SHA-256. PostgreSQL client DSNs are parsed by libpq. Credentials stay
out of command arguments, output and diagnostics. Remote client connections
require certificate/hostname verified TLS.

`restore_audit()` requires an empty, isolated target database and rejects the
source database as a target. It verifies the signed archive before invoking
native `pg_restore --single-transaction --exit-on-error`; it never uses `--clean`
or overwrites an existing schema. The restored source is already sealed. The
audit transaction verifies its complete retained state against the signed final
seal before marking it `audit`. Normal service operations remain forbidden.
A read-only inspection transaction permits inventory inspection even when the
copied controller lease has expired. No expiry reconciliation runs on this path.

An interrupted restore or failed audit remains isolated and cannot admit work.
Old/unsealed snapshots are not activation candidates. Crash/PITR archives lacking
a verified final acknowledged high-water mark remain audit-only; this tool does
not manufacture that evidence or auto-promote a replica.

## Controlled activation

`verify_objects(reader)` reads and hashes every retained verified object through
the deployment's trusted content port. Missing bytes, changed digests/sizes or a
changed inventory stop recovery. Object reservations and unresolved retention
links stay in the verified database state; they are never refunded on restore.
The database archive contains object identities and obligations, not object
bytes. Operators must retain and restore the content independently under the
original backend and store identities before verification. The recovery manager
does not copy object backups or authorise rebinding to a different backend.

Activation also requires an **externally signed fencing attestation**, using the
retained independent recovery key reference. There is no `force` flag or boolean
approval. The attestation uses `mas-fencing-proof/1` and binds:

- source and target PostgreSQL system/database identities;
- final seal digest and archive SHA-256;
- complete retained state digest and verified object count/digest;
- operator, external fencing evidence reference, next controller and an expiry
  no more than one hour ahead.

The fencing integration is responsible for independently establishing that the
old database cannot resume as writable authority. The recovery client does not
issue its own fencing proof. The native activation transaction checks the proof,
original recovery-key fingerprint, target, exact restored state and expiry. It
increments the original epoch and records the proof. The successor starts
**drained**, inspects unresolved work and explicitly changes drain through the
operator API. No provider calls execute as part of restoration or activation.

## Operator command

Use a trusted Python factory returning `(SharedBackupManager, object_reader)`.
The factory resolves database credentials, the external recovery key and the
original content backend from deployment configuration; none are CLI arguments.
The source factory points at the original authority. The restore factory points
at a separate empty database with the original schema/binding.

```sh
python -m mas_host.shared_backup_cli --factory deployment:source_recovery \
  archive /secure/backup-42 --operator operator-1 --request-key cutover-42 \
  --evidence /secure/source-seal-evidence.json
python -m mas_host.shared_backup_cli --factory deployment:restore_recovery \
  restore-audit /secure/backup-42
python -m mas_host.shared_backup_cli --factory deployment:restore_recovery inspect
python -m mas_host.shared_backup_cli --factory deployment:restore_recovery verify-objects
python -m mas_host.shared_backup_cli --factory deployment:restore_recovery \
  activate --fencing-proof /secure/external-fencing-proof.json
```

Native PostgreSQL client tools and libpq are required. The operator role requires
backup/restore privileges and PostgreSQL system-identity inspection. Application
roles and network policy must prevent unauthorised DDL or direct edits to the
guard, epochs and retained allocation ledgers.

The PostgreSQL CI gate runs actual archive creation and restoration into a new
database, verifies the complete table/sequence digest, rejects archive tampering,
rejects unsigned/changed fencing proofs, checks closed audit admission, hashes
object bytes through the content port and retains an uncertain charged call
through activation. The object transport fault fixture is separate from the
real S3 and multi-machine deployment qualifications.

`test_shared_recovery_qualification.py` also populates completed, queued and
human-paused leaf tasks; a paused composition; an unresolved peer submission;
an unresolved Cursor launch; worker claims and a non-authoritative late receipt;
provider circuit state; acknowledged upload chunks; and a pending webhook page.
It checks that the required domain tables contain records before sealing, compares
every retained table and sequence after restore, and reads each domain through
the successor's native repositories. Original task, invocation, remote, worker,
transfer and event identities survive. Before activation, the rehearsal empties
the original object store and confirms that verification fails against a new
empty content service. An explicit external-operator stage restores previously
retained backup bytes into that service under the original pinned identities;
the recovery manager then verifies them. This stage uses the bounded S3 transport
double and the content publication port, not direct insertion into its object map.

Allocation totals remain unchanged through restoration, and the activated
successor initially remains drained. After inspecting the restored state, the
operator explicitly reopens admission. The public task service then completes a
fresh task with one bounded invocation and a one-microusd reservation. The test
checks that deployment and principal run, call and reservation totals each rise
by exactly one, including after request replay. Prior uncertain peer, cloud and
worker effects remain unresolved; the old invocation remains charged; the prior
provider doubles receive no additional calls; and the fresh effect runs once.

The rehearsal emits `mas-controlled-recovery-qualification/1` JSON in the backend
test log, including stage durations, restored object count, fresh completed task
count and the total measured interval. Its total includes external object restore
and explicit reopening through successful execution; the older archive test's
activation-only timer continues to check the narrower activation operation. The source is available
for an intentional seal, the database is local, external fencing is a controlled
fixture attestation, object backup bytes were retained before the cutover, and
object transport uses a bounded double. These explicit conditions make the
result reproducible without claiming production RPO/RTO.

Crash/PITR promotion still requires a deployment-specific proof that every
acknowledged admission and effect record survived, plus independently established
fencing of any old writable authority. A successful controlled-cutover rehearsal
does not provide that proof for an unexpectedly lost database. Without it, the
restored copy remains audit-only; operators must not reset allocations or create
replacement invocation identities to reopen admission.

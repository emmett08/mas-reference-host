# Period accounting in shared deployments

`SharedAccountingAdministration` exposes the same period and provider-charge
operations as the local accounting administration. `OperatorService` selects it
when the task service uses a shared authority. Operator grants and request-key
identity continue to apply; model input cannot configure a period or supply a
provider charge observation.

The `accounting.configure_period` command installs one immutable policy for a
deployment or `principal:<name>` scope. A policy defines fixed UTC windows by
`period_seconds` and `start_epoch`, a call ceiling and a reservation ceiling in
integer micro-USD. It is not a rolling quota. Reusing its exact request key and
content returns the original receipt; changing a retained policy conflicts.

Every new native `admit_invocation` records its permanent allocation and database
admission timestamp, even before any period policy exists. Within that same
transaction, the shared host checks and charges lifetime scopes, every matching
period and the native call reservation. Any rejection rolls all of them back.
The principal comes from the frozen host-run record. No controller-supplied clock
or principal can select a different period or owner.

Period indices use mathematical floor division, including timestamps before a
future anchor. Each policy retains a monotonically increasing high-water index.
If the database clock moves back into an earlier bucket, new admission fails
until the deployment clock is reconciled. A later period permits new allocations
under that period's limits; it never changes lifetime usage or returns previous
reservations.

Installing the version-five accounting repository backfills historical admitted
calls by joining native calls to host invocations. Earlier formats did not retain
their admission timestamps. Those records explicitly retain `admitted_at: null`
and `timestamp_basis: legacy_timestamp_unknown`; migration does not invent a
date. When a period policy is installed, those unknown historical allocations
are conservatively charged to its current bucket and marked
`legacy_current_period`. The `periods` result reports their call count and amount
separately as `legacy_unknown_calls` and `legacy_unknown_reserved_microusd`.
Previously timestamped calls are placed in their actual buckets. Installing a
policy may reveal usage already exceeding its ceiling; future calls are then
rejected without altering the historical allocation.

Provider charges are separate attributed observations. Each `(provider,
charge_id)` binds a known admitted run/call, integer amount, observation time,
evidence identifier and operator identity. Replays are idempotent and conflicting
observations fail. Future-dated observations and references to unadmitted calls
are rejected. Reconciliation reports observed totals against the permanent
reservation, and explicitly reports `invoice_complete: false` and
`allocation_changed: false`. A lower bill, cancellation, failure or lost receipt
does not replenish authority.

`test_shared_accounting.py` covers atomic deployment/principal period rejection,
concurrent admission, timestamped backfill, immutable policies, negative period
indices and charge observations without refund against actual PostgreSQL.
`rust-protocol/tests/shared_accounting.rs` tests a real historical migration and
simulated database-clock rollback, asserting that native and lifetime charges
also roll back. The backend workflow supplies the real database; skipped local
tests are explicitly not acceptance evidence.

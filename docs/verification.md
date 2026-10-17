# Reference-host verification

The execution records below were imported from `emmett08/mas` at commit
`395c407111ea6c37eaa203f60d3cc162b7c9c56b`. They describe those historical
revisions. Current qualification is recorded by this repository's PR checks;
new builds identify host and protocol commits separately.

Run `make protocol` before the reproduction commands. The external verifier
writes to `.protocol/formal/evidence`; protocol examples can be run with
`make -C .protocol examples`.

## Current audit follow-up — 15 October 2026

The requirements audit starts from merged PR #45, commit
`030f9f75396ad88d6289b01ba9bd47d8ab90ee66`, tree
`687c19a280fbcdbb23816a830aa704a1929c18ed`. Its seven hosted workflow
runs passed on the identical PR tree. This follow-up adds transport phase
budgets, runtime provenance, generated contract checks, mixed-load qualification,
populated shared recovery and bounded authority/recovery models. The canonical
[requirement matrix](host-evolution-plan.md) distinguishes implementation from
outstanding acceptance. Earlier revision-specific narratives remain in Git;
[the earlier source receipt](verification/host-evolution-source.json) is historical,
not qualification of this follow-up.

### Execution record

Final combined-head results are recorded in the pull request checks. The local
verification environment uses Linux x86_64, Python 3.12.14 with jsonschema,
Rust/Cargo 1.99.0, SPIN 6.5.1 and GCC 13.3.0. Hosted profiles install their pinned
SDKs and SPIN package separately. Missing optional SDK/backend tests are skipped
explicitly; a skip is not passing interoperability evidence.

The native suite passed **72 tests**, with 13 actual-PostgreSQL cases and three
SPIN-only unit entry points reserved for their separate fixture/verifier gates.
The standalone scripts suite passed without an installed host package, including
the build-time provenance writer; seven provenance reader tests also passed against a fresh
installed wheel in a tests-only directory. Focused checks passed for phase deadlines,
managed peers, provenance portability, generated codec/allocation contracts and
strict native rejection decoding. The generated differential methods also
passed through the installed public `mas encode/parse` fallback.

The first integrated local host run executed 1,268 tests in 274.3 seconds with
127 explicit profile/dependency skips and three errors: two managed-peer
continuation subcases and dispatcher shutdown. It overlapped source changes and
build/load work and is excluded from passing evidence. The two managed-peer errors coincide exactly with a source-file edit between
subcases and the implementation identity correctly failed closed. Both affected
tests and the complete dispatcher class passed on stable source. A second full
local run ended without a unittest summary; it is incomplete, despite the outer
command's zero exit status. The complete recovery class passed separately.
Exact-source hosted results, not either local run, must qualify the full suite.

A later installed amd64 Cursor run reproduced the dispatcher shutdown timeout.
Deterministic signal injection then demonstrated that best-effort telemetry
could swallow the supervisor's `AdapterInterrupted` exception during a status
poll. Observation handlers now preserve shutdown control flow, including the
runtime provenance readers; their data and filesystem failures remain
non-authoritative. The signal regressions failed before the repair and passed
afterwards. The original five-second dispatcher shutdown assertion is retained;
the final combined-head hosted checks must qualify the repair.

An independent-VM partition run timed out waiting for its synchronous management
SSH response on the same NIC that the fixture disconnected. That run is failed/inconclusive:
it supplies no completion evidence for the partition case. The fixture now
launches one detached guest invocation with a 60-second monotonic deadline and
persists its actual exit and result before fresh SSH reads them after reconnect.
The nonce, guest boot and launch identity must match; absent, failed, malformed
or late completion fails qualification. The lease, receipt, single-effect and
quota assertions are unchanged, and failure paths retain diagnostic snapshots.
Standalone subprocess tests exercise completion and rejection boundaries; the
real independent-kernel gate remains necessary on the submitted source.

### Formal verification and correspondence

The [hosted Protocol checks run](https://github.com/emmett08/mas/actions/runs/38696365514)
passed **158 cases and 188 checks** on the submitted model sources: 102 complete
zero-error checks, 70 intended-defect counterexamples, two liveness-assumption
counterexamples and 14 reachability witnesses. SPIN 6.5.2 completed verification
in approximately 287 seconds. The downloaded evidence ZIP matched GitHub's
SHA-256 (`b5ab81b4e86f77c13a7753c48e10fc53f11ac86a460ddcc0abf65f54f4ddb7b4`);
all 17 source hashes, 947 log references and 86 replay-trail hashes matched.
This intact hosted evidence supersedes the local report with one unexplained
empty raw log; that individual positive profile also passed an independent rerun.

The new authority/recovery coverage includes 22 named defect mutations, eight
reachability witnesses and seven selected traces with 59 observations, in
addition to the existing transport, lifecycle, host recovery and composition
checks. Reviewers checked the assertion oracles and repaired lease-expiry,
stale-controller reachability, contested child-quota and nonempty-history
witness defects. The workflow report binds these results to its model/runner
hashes; full implementation and installed-image checks remain separate gates.

The complete finite-state searches establish only the modelled properties at
the documented bounds. They do not establish unbounded correctness, a refinement
proof of every Rust/Python execution, external-provider exactly-once effects,
cryptographic security or deployment liveness. A mutation passes its negative
control only when the intended named assertion fails and its trail replays;
resource exhaustion and unrelated failures cannot satisfy that gate. Witnesses
show reachability and are counted separately from positive safety checks.

The shared authority model separates controller epoch, worker boot, assignment
fence, lease expiry, durable admission and dispatch grant. It checks two
controllers, two workers and two logical calls, with unit charges across
bounded deployment/principal/provider/ancestor scopes. The recovery model covers
one source, one snapshot, one audit restore and one replacement, preserving two
logical identities and their grants and charges. Atomic committed commands,
trusted external fencing evidence and the storage contracts remain assumptions.
See [the formal model contract](https://github.com/emmett08/mas/blob/main/formal/README.md),
[host evolution](https://github.com/emmett08/mas/blob/main/formal/host-evolution-notes.md) and [shared authority/recovery](https://github.com/emmett08/mas/blob/main/formal/shared-authority-notes.md)
for scenario bounds, assertions, mutations and implementation mappings.

Selected shared traces are executed against both Promela transitions and actual
native PostgreSQL authority. The native harness compares state, accounting,
evidence, acceptance and the exact intended rejection code; transport failures
cannot count as semantic rejections. This is selected trace correspondence,
not exhaustive implementation refinement. Generated tests independently encode
integer JSON, compare Python/Rust actor and payload boundaries, and exercise
allocation conservation/replay/restart and gateway admission. Their fixed seeds
and bounded input counts are reproducible test evidence, not universal proofs.

### Capacity, CPU and recovery evidence

The full mixed development fixture passed on local revision `b1cd942` (the same tree as published `0693eb5`): 1,024 cancelled historical
task receipts, 32 completed historical executions, 64 mixed tasks across two
principals and 16 continuations. All 23 checks passed, including four concurrent
callbacks, two per principal, queue rejection/reuse, progress during pauses,
exactly 96 calls and 112 runs, zero spending and no surviving descendants.
Preparation took 35.8 seconds and the mixed phase 24.7 seconds; observed completion
p99 was 6.29 seconds, with a peak of 15 descendants and 306 MB summed RSS.
This development report has `qualification: false`: the copied native binaries
were not rebuilt in that isolated checkout. The later timing-boundary repair
passed the full-shape smoke; exact-source CI runs both full profiles. Neither
fixture establishes production capacity, paid-provider latency or long endurance.
See [the fixed workload and ceilings](reference-load.md).

The separate retention regression creates 1,000 terminal fixture instances with
actual admitted run/call allocations, archives them, reopens the owners, and
completes a fresh public workflow. It verifies retained request keys and unchanged
historical charges. It does not claim 1,000 actual provider executions or 1,000
populated run directories. Its purpose is the original instance-retention cliff.

The [IPC benchmark](verification/host-ipc-benchmark.json) uses format
`mas-ipc-benchmark/2`. Status latency measures 100 queries after a completed echo
exchange; CPU measures construction, exchange, status, close and child reaping
for each strategy. Binary hashes identify the measured executables. Fixed order,
cache state and instrumentation affect this single fixture; it is not a
production SLO or independently authenticated build-provenance record.

The populated [shared recovery rehearsal](shared-recovery.md) covers sealed
archive, database restore, external object restoration, activation, explicit
admission reopening and a fresh task completed through the public service,
with completed/queued/paused work, unknown effects, stale receipts, allocations,
transfers, artefacts, grants and event outboxes. Its timing includes the whole
controlled cutover. It does not establish recovery after unexpected source loss,
PITR coverage, production RPO/RTO or independent physical fencing correctness.
The object transport is a bounded S3 double; the archive does not contain
object bytes. Actual PostgreSQL execution belongs to the corresponding hosted
gate, and real S3 publication/fencing has its separate fixture.

### Outstanding acceptance

- W12 incremental authoritative storage accounting remains unimplemented. Under
  arbitrary SDK filesystem writes, an asynchronously updated cache cannot safely
  bound current usage. Full scans remain; they are not atomic filesystem snapshots
  or enforced filesystem quotas. A mediated-write or quota-enforced profile needs
  its own explicit contract before replacing them.
- Live OpenAI, Cursor, mixed/Cloud and external-peer profiles require their
  authorised credentials, budgets and environments. No paid calls ran here.
- Deployment OAuth/proxy/private-CA, production networking, sustained workloads,
  replication/fencing and recovery objectives require deployment evidence.
- Installed amd64/arm64 images, portable clients, PostgreSQL/S3, real Temporal,
  containment, VM faults and security scans must pass on the submitted source.
- Content-bound runtime provenance inherits installation trust. Release signing,
  an accepted release tag and published-image qualification remain release gates.

## Reproduction and evidence

Build the actual Rust boundary before running the host suite:

```sh
cargo test --locked --manifest-path .protocol/rust-protocol/Cargo.toml --target-dir "$PWD/rust-protocol/target"
cargo build --release --locked --manifest-path .protocol/rust-protocol/Cargo.toml --target-dir "$PWD/rust-protocol/target"
export PYTHONPATH="$PWD/host"
export MAS_BIN="$PWD/rust-protocol/target/release/mas"
export MAS_GATEWAY_BIN="$PWD/rust-protocol/target/release/mas-gateway"
export MAS_CODEC_BIN="$PWD/rust-protocol/target/release/mas-protocol-codec"
python -m unittest discover -s host/tests -v
python -m unittest discover -s scripts/tests -v
./rust-protocol/target/release/verify
make -C .protocol examples
```

Use the hash-locked supported Python dependencies in the corresponding CI
workflow. Optional provider/orchestrator dependencies and the real Temporal
server are separate profiles; follow [provider installation](providers.md).
The actual shared backend requires the `postgres-repository` feature,
`MAS_TEST_POSTGRES_DSN`, the built `mas-shared` binary and native archive clients;
[its workflow](../.github/workflows/postgres.yml) runs native conformance,
Python shared tests and combined PostgreSQL/RustFS boundaries.

The formal verifier requires Linux, SPIN, a C compiler, `sh` and `sha256sum`.
It emits `formal/evidence/summary.json` plus raw search logs, models and replayable
trails. The Protocol checks workflow retains that directory as an artefact.
Reports bind the runner, corpus and models by hash and include tool versions,
commands, bounds and observed outcomes. Review those identities against the
submitted tree; an earlier report cannot qualify later code. No approximate
bitstate/hash-compact search substitutes for a complete positive check.

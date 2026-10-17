**MAS host: complete implementation and delivery plan**

Prepared 14 October 2026. Repository: https://github.com/emmett08/mas. Baseline: `ccc7bb85cede76398aee19b24b9281f2d2959046`, which includes PR #15. Status: PR #45 is merged; the post-merge requirements audit is being addressed on `feat/host-verification-completion`. Release acceptance remains open. This document is the canonical plan for this review and should be updated in place as work progresses.

**Implementation record, 15 October 2026.** The source changes implement the
bounded profiles below. The original W00–W17 requirements remain the acceptance
contract; a partial profile does not close its whole work package or milestone.
The consolidated implementation in [PR #45](https://github.com/emmett08/mas/pull/45)
was merged as `030f9f75396ad88d6289b01ba9bd47d8ab90ee66`. This follow-up
implements the subsequent audit repairs and adds qualification evidence. Earlier
PRs remain in Git history. A source implementation, a passing bounded fixture
and production acceptance are distinct claims; none implies a release.

| Package | Implemented source profile | Qualification and deployment acceptance |
|---|---|---|
| W00 | Pinned baseline, real Rust fixtures, paced leaf and saturated mixed two-principal load gates, SDK/orchestrator and formal evidence. | The mixed fixture covers four active callbacks, full-queue rejection, short/long/paused work and retained history; reports identify exact tested source. Production workload/SLOs, live provider profiles and availability objectives require deployment acceptance. |
| W01 | Durable immutable definitions/current aliases, loaded implementation-package digest verification, authorised history and retirement, in local and shared repositories. | Deployment signing-key operations and application-specific acceptance. |
| W02 | Typed no-send/completed-invalid/refused/completed-valid-cleanup outcomes; bounded diagnostics and private completed output; original unknown records remain unknown. | Live credentialled provider acceptance and provider-specific proof of non-execution beyond the declared adapters. |
| W03 | Unified leaf/composition lifecycle, versioned request-key epochs and retirement, discovery schemas, durable scheduling intent, native shared queues and historical routing. | Combined-head interface and PostgreSQL qualification. |
| W04 | Atomic assignment claims; shared global/principal/workflow/resource slots; eligible-principal round-robin; bounded queues and principal call/spend quotas; drain. | Exact-source mixed-load and sustained production fairness qualification. Priority and weight remain explicit future policy extension points under the specified initial round-robin policy. |
| W05 | Per-peer and per-Cursor-job operation ownership outside database locks during network waits; exact binding validation; explicit HTTPS CONNECT proxy, CA and origin policy; opt-in cumulative DNS/connect/TLS/header/body phase deadlines inside the disposable transport process. | Deployment-specific OAuth/corporate proxy/private-CA qualification. The default transport identity is unchanged; phase budgets opt into a new identity. |
| W06 | Reference-aware archive/prune, permanent tombstones, audited lifetime amendments, immutable UTC period budgets, retained attributed provider charge observations; local and shared accounting. | The archival regression removes 1,000 terminal fixture instances with actual admitted run/call allocations, retains old keys and charges, then completes a new public workflow. It is not 1,000 provider executions. Deployment migration/rollback and real invoice acceptance remain separate. |
| W07 | Business, queue, active and human-pause clocks; audited authorised deadline extensions; summed worker time; native shared admission checks. | Combined-head parallel timing and restart qualification. |
| W08 | Durable bounded retries/backoff, shared circuits, exact Cursor/peer observations, query extension contract, operator recovery inbox/resolution, and separate cancellation observations. | Additional built-in provider-specific network queries and credentialled recovery qualification. Unknown effects never become retryable merely through a timeout. |
| W09 | Durable progress, subscriptions/webhook outbox, safe logs, operator API/CLI/console, read-only source/release/package/binary provenance and retained worker/provider version diagnostics, fixed-label metrics and bounded correlated latency spans. Native Linux/macOS/Windows clients passed on the earlier integrated head. | Final-head API/UI parity, portability and redaction checks; deployment collector operations. Installed provenance is content binding under installation trust, not independent release authentication. |
| W10 | Trusted versioned provider registry, bounded map/reduce/while/switch/transforms, schemas, all/quorum/first-accepted joins and separately admitted recovery/compensation. | Application-specific acceptance and production workload qualification; compensation is never an assertion of automatic undo. |
| W11 | Authorised chunked transfer with shared acknowledged chunks/session state, verified float64 encodings, S3-compatible content storage, native PostgreSQL artefact metadata, frozen backend identity and fenced publication. Real RustFS backend acceptance passed. | Combined-head PostgreSQL/object-store failure qualification and deployment lifecycle policy acceptance. |
| W12 | Persistent Rust IPC, service-selected immutable registered workers, native process guardians/pidfd wakeups, persisted workflow progress/frontier and full storage traversal; IPC benchmark compares CPU after both strategies close and reap children. | **Incremental authoritative storage accounting remains unimplemented.** Arbitrary SDK writes cannot be safely covered by a cached upper bound; full scans remain necessary and are not an atomic filesystem snapshot. Production performance targets and native macOS/Windows workers require separate qualification. |
| W13 | Durable-service workspace dispatch with immutable tool/policy/output bindings, trusted workspaces and explicit isolated Linux containers with CPU/memory/process/network limits, immutable inputs and replayed output receipts. Real Docker qualification passed on the earlier integrated head. | Final-head containment checks and deployment-specific container image acceptance. |
| W14 | Authenticated fenced remote workers, bounded durable spool, exact retained protocol/artefact receipts, revocation-aware transfer and shared native worker authority; independent VM fault gate implemented. | All five two-guest fault cases passed at 71e4d87, including the SQLite durability and workspace repairs. Physical-host and production-network acceptance remain separate. |
| W15 | Managed immutable HTTP/MCP/A2A peers, pinned capability negotiation, operation-scoped credential snapshots, lost-receipt identity reuse and shared remote-intent/circuit authority. Three-profile local race tests passed. | Combined-head shared backend, deployment-specific OAuth/proxy/private-CA and cross-organisation acceptance. |
| W16 | Native PostgreSQL authority for protocol, journals, queues, composition, workers, Cursor jobs, transfer sessions, definitions, accounting and events; S3 artefact bytes; controller epoch fencing and leases; guarded sealed backup/restore with independently evidenced activation; populated rehearsal retains tasks, compositions, unknown effects, quotas, transfers, artefacts, events and grants. | Exact-source PostgreSQL execution of populated cutover and authority traces; production replication/fencing and measured recovery objectives. Controlled sealed-source recovery does not establish unforeseen-loss/PITR coverage. No production activation is claimed. |
| W17 | Source, real Rust/Temporal/CLI, native client, real Docker and S3 acceptance; installed amd64/arm64, PostgreSQL, security, formal and independent-VM CI gates; generated cross-language codec/accounting checks and bounded authority/quota/recovery models with mutation controls. | Final combined-head checks and fault gates; live paid profiles, sustained workload/SLOs, release provenance and accepted release. |

See [host-service-evolution.md](host-service-evolution.md),
[provider-outcomes-recovery.md](provider-outcomes-recovery.md),
[host-observability.md](host-observability.md),
[trusted-workspaces.md](trusted-workspaces.md) and
[distributed-workers.md](distributed-workers.md) for supported configuration,
semantics and explicit limits. The remaining items above are not reclassified as
completed simply because their extension points exist.

**Review and integration.** Qualify the combined head of the follow-up PR.
Earlier passing checks do not qualify later source. The qualification column
identifies outstanding implementation and deployment requirements;
revision-specific results are recorded in [verification.md](verification.md)
and the PR checks. Model verification covers the documented finite abstractions;
selected native traces and generated tests provide implementation evidence, not
a proof that all Rust/Python executions refine those models.

**The intended outcome is a reusable MAS execution service that users can submit work to, observe, extend, upgrade and recover without manipulating internal databases.** Delivery proceeds from a dependable single-host service to distributed execution and then high availability. The existing Rust protocol authority, Python adapter ecosystem and conservative treatment of uncertain external effects remain the foundation.

All three GitHub Actions workflows for the baseline—Protocol checks, Host and Compose checks, and Dependency and image security—have passed. That establishes those executed checks, not representative production acceptance. The preceding review also passed 18 targeted Python tests and reproduced capacity and upgrade failures using temporary state. Live providers, deployment-specific authentication and production workloads require separate acceptance.

**Scope includes the full set of recommendations from the review.** It covers unified service access, background scheduling, concurrency and fairness, immutable workflow versions, provider outcomes and recovery, retention, budget administration, human pauses, observability, corporate transports, extensible providers, richer composition, remote artefacts, workspace/tool execution, selective Rust implementation, distributed workers, federation, disaster recovery and release qualification. Application-specific research, coding and infrastructure workflows consume these facilities through adapters; their domain logic does not move into the host.

The working assumptions are Linux execution workers, trusted application registrations, compatibility with existing public interfaces, support for the current amd64 and arm64 image targets, and portable clients for macOS, Windows and Linux. Native macOS/Windows supervision is a separately qualified profile. Untrusted code execution requires an explicit containment profile; ordinary trusted adapters do not become sandboxed merely because they run in another process.

**The release sequence is functional, rather than tied to an invented completion date.** Engineering estimates follow the baseline measurements and deployment profile in W00. Every work package below has dependencies and an exit condition. The original plan called for focused PRs. The consolidated delivery was merged in PR #45; preserve focused commits and review evidence in its audit follow-up.

| Milestone | Outcome | Required packages | Claim permitted after its gate |
|---|---|---|---|
| M1 | Dependable local service | W00–W09 | Supported bounded single-host service for the explicitly tested workload/provider profiles. |
| M2 | Extensible applications and efficient runtime | W10–W13, on M1 | Richer bounded workflows, remote artefacts, explicit tool profiles and measured runtime improvements. |
| M3 | Distributed execution | W14, W15, on M2 | Remote workers and federated peers under the documented authority and failure model; controller host remains an availability dependency. |
| M4 | High availability and disaster recovery | W16, on M3 | Fenced controller replacement and recovery against measured availability/recovery objectives. |
| Every milestone | Independently reviewable release evidence | W17 applied to the milestone's scope | Only capabilities actually exercised and passed are advertised. |

**Preserved contracts are the acceptance basis for every change.**

1. The authenticated boundary supplies principal identity. A payload, content hash, workflow name or remote Agent Card never grants authority.
2. Every submission pins an immutable definition and canonical input identity. Retries of the same request key return the same logical receipt; changed content conflicts.
3. Admission and allocation precede external dispatch. Recovery never silently creates a new logical effect identity.
4. Completed invocation evidence is replayed. An executing or unknown effect is reconciled before any retry whose safety depends on its absence.
5. Lease expiry, process death, local cancellation and a provider timeout do not establish that an external effect stopped.
6. Output validity, effect certainty, cancellation observations and accounting observations remain separate. An invalid answer can still be a completed and chargeable provider call.
7. Every ancestor, deployment, principal and provider limit applies where configured. Archival, restart and failover never replenish historical allocation by deleting evidence.
8. Durable state changes and the events needed to recover them have an atomic owner. Cross-store transitions use explicit idempotent intent/receipt recovery; they are never described as one transaction when they are not.
9. Control messages remain bounded. Large outputs travel through authorised artefact references with integrity checks.
10. Rust remains authoritative for protocol admission and message/task transitions. A Python or network adapter cannot bypass that boundary.
11. An unavailable old implementation blocks that instance's execution, while authorised historical inspection and lifecycle administration remain available.
12. Unknown schema, protocol or worker versions fail closed. Compatibility is tested rather than inferred from a shared method name.

**The target architecture separates responsibilities by their authority.** These are responsibilities to implement, not a requirement to add a separate microservice for each row.

| Responsibility | Owner and boundary |
|---|---|
| Authentication and authorisation | HTTP/MCP/A2A boundaries and configured identity provider; principal ownership enforced again by the application service. |
| Submission and lifecycle | One service contract for leaf workflows and composed plans; CLI and connectors are clients of that contract. |
| Definition resolution | Immutable registry of plans, topologies, operation contracts, provider policies and implementation/package identities. Aliases select versions only at new submission. |
| Scheduling | One logical assignment authority; claims ready work, enforces concurrency/fairness and drains workers. Composition supplies ready leaf work through this authority. |
| Persistence | Public stores own cohesive transitions, claims, checkpoints and events. Avoid a generic CRUD layer that splits invariants between callers. |
| Effect admission | Existing Rust protocol authority plus durable host allocation policy. A failed downstream admission cannot authorise a provider call. |
| Execution | Supervised workers execute pinned registrations through a bounded worker protocol. Provider SDKs initially remain in Python. |
| Reconciliation | Capability-specific provider/peer observations and evidence-backed operator decisions; no generic blind replay. |
| Artefacts | Authorised metadata, verified content, provenance and retention; local backend first, object storage for remote consumers. |
| Operations | Shared CLI/API/console lifecycle, structured diagnostics, metrics, events and recovery procedures. |

**Every durable store needs an explicit distributed owner.** For M3, the controller owns scheduling, admission, host journals and the per-run Rust databases. Remote workers hold replaceable inputs/workspaces and a bounded durable spool of unacknowledged effect observations. That spool transports evidence; it grants no admission authority. Losing an unacknowledged observation leaves the associated effect unknown. Workers invoke the authoritative gateway over authenticated, bounded RPC. M3 can distribute execution while retaining a single-controller availability limitation. M4 replaces controller-local authoritative records with a shared transactional backend and object storage, including Rust protocol state—not merely the Python queue.

| Existing state | M1/M2 owner | M3 owner | M4 requirement |
|---|---|---|---|
| Task service and composition records | Versioned local stores | Authoritative controller | Shared transactional lifecycle, queue and definition records. |
| Deployment and ancestor allocations | Durable local ledger | Controller admission authority | Transactional shared ledger with authority epochs and unique allocation identities. |
| Rust mailboxes, task state, capabilities and admissions | Per-run Rust/SQLite authority | Controller-side Rust gateway | Shared durable protocol repository or equivalently fenced durable authority; stale writers rejected. |
| Runner checkpoints and bootstrap intent | Host journal/bootstrap stores | Controller journal service | Shared checkpoints and bootstrap transitions with stable original IDs. |
| Unacknowledged worker observations | Local supervisor/evidence transport | Bounded durable worker spool until controller acknowledgement | Same evidence transport rule; loss produces uncertainty, never permission to rerun. |
| Remote tasks and Cursor jobs | Local durable repositories | Controller reconciliation service | Shared intent, peer identity, observations and unresolved-operation records. |
| Artefact metadata/content | Local integrity-checked repository | Authenticated transfer/object backend | Durable metadata, verified objects, retention and backup consistency. |
| Credentials and keys | Execution-time secret resolution | Secrets scoped to worker/trust zone | Managed key rotation, revocation and recovery; secrets excluded from public descriptors. |

**W00 — Establish the reference profiles, regression cases and identity contract.** Size: small. Dependencies: none.

- Capture the reviewed revision, public APIs, schema versions, descriptor identities and installed-image versions. Read applicable repository instructions again before implementation.
- Add focused regressions for the reproduced history-capacity cliff, unavailable-version queue blockage, historical inspection/cancellation rejection, global remote-call locking and pre-dispatch provider failures.
- Specify IDs for definitions, logical submissions, continuation revisions, assignments, invocations, provider effects, event cursors and allocation authority. Preserve existing `task-*`/`flow-*` identities and legacy request-key namespaces.
- Establish the operational workload and hardware profile. Measure admission, dispatch, status, cancellation, SQLite writes, directory scans, process launches, provider wait, storage growth and recovery time separately.
- Pin a reference test matrix: one leaf, a parallel plan, a long sequential plan, two principals, paused human input, invalid provider output, uncertain remote submission, large artefact and an old-version queued job.
- Exit: baseline fixtures run against the real Rust boundary; invariants and exact supported profiles are documented; performance and recovery objectives have explicit workload/hardware/bound assumptions. A mock-only baseline cannot satisfy live-provider acceptance.

**W01 — Make definitions immutable and upgrades operationally safe.** Size: medium. Dependencies: W00. Primary areas: `task_service.py`, `workflow_engine.py`, `workflow_plan.py`, stores and registry loading.

- Persist version-addressed registration manifests, including plan, transitive workflow identities, topology, operation/acceptance descriptors and implementation/package digest. A version string alone is insufficient to identify reproducible code.
- Add current-version aliases for new submissions and a compatibility-aware execution resolver. Maintain old implementations until their live work drains; retirement becomes an explicit operation.
- Read historical receipts, results, graph details and cancellation authority from retained metadata without importing the old adapter. Missing executable definitions produce an individually blocked state.
- Preserve the exact original identity during migration. If old code cannot be identified or recovered, mark execution unavailable; do not relabel current code as the old implementation.
- Exit: queue v1, install v2, submit v2 and remove the v1 implementation. Both remain inspectable; v2 executes; v1 is blocked or drained according to its retained implementation. The dispatcher continues past it. Historical owner/grant checks remain enforced.

**W02 — Introduce truthful provider outcomes and safe diagnostics.** Size: medium. Dependencies: W00. Primary areas: `contracts.py`, structured providers, `openai_client.py`, `cursor_session.py`, `runner.py`, journal and supervisor IPC.

- Represent proven pre-dispatch failure, confirmed provider rejection where its contract establishes non-execution, completed valid output, completed invalid/refused output, and unknown effect. Define which trusted component can supply evidence for each state.
- Keep certainty and output validity separate. Retain provider completion/usage evidence when schema validation fails. Any repair call is a new admitted attempt.
- Add credential, SDK/runtime and schema readiness checks with no inference. Management operations remain usable without credentials. Readiness is advisory; dispatch still checks the actual execution conditions.
- Preserve sanitised stage, category, provider request ID, retry-after and permitted recovery action. Raw prompts, secrets and arbitrary exception bodies stay out of general logs; sensitive diagnostic artefacts use explicit grants.
- Exit: missing credentials/SDK fail before network dispatch without creating false uncertainty; malformed returned output records completed execution; ambiguous transmission remains unknown; diagnostics pass redaction tests and old unknown records remain unknown.

**W03 — Unify leaf and composition access and persist scheduling intent.** Size: large. Dependencies: W01 and the lifecycle contract from W02. Primary areas: `service_contracts.py`, task/composition services, `bridge_config.py`, CLI, HTTP/MCP/A2A and orchestrator adapters.

- Provide one application contract for discovery, submit, inspect, result, leaf detail, list/search/page, events, continue, cancel and recover. Discovery returns only authorised registrations, schemas and supported capabilities.
- Retain separate execution implementations for a leaf and a composed plan behind the service. Preserve existing leaf endpoints/imports through compatibility adapters; new unified request keys use an explicitly versioned namespace.
- Commit scheduling intent with submission/continuation in the owning store. If state remains in separate databases, use a transactional outbox in each owner and stable assignment IDs; receiving/delivering the outbox twice must be harmless. Do not add an untracked second write.
- Add a persistent dispatcher lifecycle. A client disconnect does not stop accepted work, and a restart resumes committed scheduling intent.
- Exit: the same branching/parallel/loop/subworkflow plan works through CLI, HTTP, MCP and A2A. Paused leaf detail and continuation work remotely. All interfaces preserve authorisation, idempotency, uncertainty and old leaf behaviour. Crashes between acceptance, enqueue and acknowledgement lose no accepted submission.

**W04 — Add bounded concurrent scheduling and fair tenancy.** Size: large. Dependencies: W03. Primary areas: `task_service.py`, `workflow_scheduler.py`, `workflow_engine.py`, `bridge_health.py`, allocation policy and execution ownership.

- Replace the deployment-wide execution lock with atomic per-assignment claims and supervised process ownership. Keep local guardian ownership until predecessor work is stopped; a bookkeeping timeout alone cannot release execution authority.
- Route composition-ready leaves and independent jobs through one dispatch policy. Enforce deployment, principal, workflow/ancestor and provider limits; avoid independent schedulers each assuming all capacity is theirs.
- Implement a specified fair policy—initially round-robin across eligible principals with FIFO within a principal—and explicit priority/weight extension points. Fairness applies when resources are available and tasks are eligible; it is not an unconditional liveness promise.
- Bound queue depth, process count and per-principal call/allocation usage. Human pauses release worker slots, while uncertain effects retain the accounting and reconciliation controls they require.
- Support worker registration, health, drain and graceful shutdown on one machine. Authenticated ingress remains separate from the process pool.
- Exit: two independent jobs overlap; contending workers cannot concurrently own one assignment or duplicate an admitted effect. Recovery may replay the same assigned run under its original identities and retained evidence. Aggregate caps hold across simultaneous compositions; one principal cannot monopolise reserved capacity; cancellation closes new admission; scheduler/worker death preserves original effect identity. Test both two-worker interleavings and representative configured capacity.

**W05 — Shorten remote ownership and support explicit corporate transport.** Size: medium. Dependencies: W02 and W03; can run alongside W04. Primary areas: `bridges/remote.py`, HTTP transports, provider transport and credentials.

- Persist remote intent, release the database/global lock, execute under per-operation ownership, then commit the correlated receipt. A read-only snapshot must not wait for an unrelated network request. Stale completions are rejected by operation identity/epoch.
- Persist remote agent/run/task identifiers as soon as reliably observed. Keep exact peer bindings; a lost first receipt stays unknown unless provider-specific evidence resolves it.
- Introduce explicit trusted proxy, private-CA, endpoint allowlist, DNS/TLS/connect/body timeout, redirect and local callback bypass policies. No arbitrary interpreter hooks or ambient startup settings enter workers.
- Keep secret resolution separate from public identity. Secret rotation preserves a binding; changed endpoint or effectful transport behaviour requires a versioned policy identity.
- Exit: a stalled peer does not block unrelated inspect/cancel/submit; crash between intent and receipt retains uncertainty; direct/proxy/private-CA paths and failure cases pass; credentials do not leak into proxy-excluded control channels or diagnostics.

**W06 — Finish history retention, deduplication and budget administration.** Size: large. Dependencies: W01, W03 and W04. Primary areas: workflow/task stores, `operations.py`, artefacts and administrative APIs.

- Separate active-work limits, detailed-history quotas, deduplication retention and immutable accounting totals. Remove the lifetime instance-count cliff through supported archival.
- Implement retention preview, archive and prune over the full reference graph: compositions, continuations, run journals, remote jobs, evidence, artefact links and unknown effects. A terminal parent alone is insufficient evidence that every child reference can be deleted.
- Preserve compact receipts/tombstones containing principal/key namespace, input/definition digest, original IDs, final state or archive reference, and accounting references. Retain legacy arbitrary-key tombstones indefinitely. New namespaces may use explicit request epochs: retain an epoch retirement record and reject every later submission into that epoch before deleting its per-key tombstones. Specify the deduplication horizon without promising that forgotten arbitrary keys can still be recognised.
- Add audited policy amendments or allocation grants with an authorising principal, reason and effective epoch. Maintain immutable historical consumption. Period limits, concurrent reservations and cumulative totals are distinct quantities.
- Report configured allowance, reserved allocation, provider-reported tokens/cost and reconciled usage separately. Unknown reservations are not released merely because a process died.
- Exit: under an explicitly authorised lifetime run/call allowance sufficient for the workload, new submissions continue beyond the old 1,000 historical instances after eligible archival. The separate baseline lifetime run allowance must not be mistaken for the history-capacity defect. Old keys remain idempotent/conflicting as specified; unknown effects stay retained; archive/restart never increases authorised allowance or refunds consumption; concurrent amendments cannot over-admit. Backup includes tombstones and policy history.

**W07 — Separate waiting, computation and business deadlines.** Size: medium. Dependencies: W03 and W04. Primary areas: workflow plan, task/composition stores, scheduler and continuation contracts.

- Add a versioned timing policy: absolute business deadline, runnable queue-wait bound, cumulative active execution budget and per-pause response expiry. Define pause eligibility and authoritative clock sources.
- Count active elapsed workflow time over intervals with running branches; expose summed worker time as a separate resource measure. Persist transitions and conservatively account for unobserved running time after a crash.
- Stop computation accounting only after workers have stopped. Unknown external effects keep their uncertainty and business expiry even while no local worker runs.
- Support explicitly authorised deadline extension with an audit event. Existing v1 instances retain their original fixed-deadline semantics unless an explicit compatible migration is performed.
- Exit: a one-hour computation budget can wait several days for human input within its business deadline; restart never resets elapsed allowance; multiple parallel/paused branches follow the declared clock rules; expiry/cancellation blocks new work without claiming remote cancellation.

**W08 — Implement bounded retry, cancellation and reconciliation workflows.** Size: large. Dependencies: W02, W03, W05 and W07; allocation rules from W06.

- Add persistent retry eligibility, attempt count, backoff, retry-after, deadline and circuit-breaker state. Each admitted attempt has its own durable allocation; documented provider idempotency keys are distinct from tracing identifiers.
- Retry only when the effect classification and provider contract justify it. A generic HTTP status or a search returning no result is not universal proof of no execution.
- Show cancellation requested, future dispatch stopped, local workers stopped, remote cancellation acknowledged and final effect reconciliation as separate observations. Handle completion racing cancellation without erasing a known result.
- Add provider/peer-specific queries and a recovery inbox with admissible decisions, required evidence and audited actor identity. Correction of invalid output is explicit new work.
- Default an unknown ancestor effect to blocked dependent dispatch. Any permitted continuation of independent branches must be an explicit policy justified by dependency and budget isolation.
- Exit: inject failure before send, during transmission, after acceptance, after response and before durable commit. No ambiguous effect is automatically redispatched; restart preserves retry/backoff state; local termination never becomes false remote cancellation; provider-specific recovery succeeds against its declared evidence contract.

**W09 — Deliver observable lifecycle and usable operations.** Size: large. Dependencies: W03 for initial events/UI; final acceptance W04–W08. Primary areas: CLI, service events, bridge server, health/monitoring and a small authenticated console.

- Publish progress events durably during execution, not only on return. Provide resumable subscriptions and optional webhooks through a transactional outbox with acknowledgement/retry; consumers deduplicate event IDs. Polling remains supported.
- Define event retention and cursor expiry. An expired cursor produces an explicit resynchronisation response. Event authorisation follows task ownership and grants.
- Provide equivalent CLI/API/console operations for registration discovery, submission, list/search, queue age, graph/leaf detail, human input, cancellation, recovery, budgets, storage, definition availability and worker drain.
- Add safe structured logs and metrics/traces correlated by request, task, run, assignment and invocation. Include queue/dispatch/provider latency, rejection reasons, retries, unknown-effect age, reconciliation freshness, storage growth and allocation headroom. Bound metric label cardinality.
- Add a doctor/preflight and effective-configuration report, including release/source/schema/worker/provider versions. Provide a portable client for macOS/Windows/Linux; Linux remains the execution target.
- Exit: an authorised user can find and complete a paused workflow without knowing a database path; an operator can explain waiting/failure without SQLite access; reconnect preserves retained progress; diagnostics and UI display the same states; secret/payload leakage tests pass. M1 release requires the local subset of W17.

**W10 — Extend bounded composition and provider registration.** Size: large. Dependencies: W01–W04, W07–W08. Primary areas: `agent_config.py`, operation contracts, workflow plan/evaluator and acceptance policies.

- Replace hard-coded provider selection with a trusted registry of stable IDs, implementation versions and capability descriptors. Configuration selects authorised registrations; it cannot import arbitrary modules or choose arbitrary executable paths.
- Declare structured-output dialect support, tools, sessions/context, cancellation, reconciliation, streaming, token controls, endpoints and accounting. Reject unsupported settings before effect dispatch; preserve existing descriptors and aliases.
- Add typed input/output selection and mapping, bounded map/fan-out over an input collection, explicit failure branches, configurable all/quorum/first-accepted joins, and registered compensating actions. Define sibling cancellation/uncertainty semantics for early joins.
- Keep plan expansion, dynamic item count, aggregate memory, call and concurrency bounds explicit. Use a declarative selector language with no arbitrary expression evaluation.
- Keep application acceptance distinct from task completion. Include acceptance policy identity and evidence references in final receipts when configured; independent validation remains application-specific.
- Exit: add a fourth configured provider without editing core dispatch; compose unrelated workflows through mappings and bounded fan-out; quorum/early-result plans retain unresolved sibling effects; compensation is an admitted action, not a promise to undo arbitrary effects; tests cover missing fields, invalid schemas and expansion limits.

**W11 — Provide remote artefacts and explicit scientific data encodings.** Size: large. Dependencies: W03, W06 and W09. Primary areas: `artifacts.py`, authenticated service, retention and storage backends.

- Preserve the existing repository contract and local backend; add object storage and authorised upload/download with bounded streaming. Verify complete hashes before publishing usable references. Incomplete uploads remain bounded and collectable.
- Retain digest, size, media type, producer, input provenance, grants and retention links. A digest or storage URL is never read authority. Support revocation policy for future access and state its limits for already downloaded bytes.
- Add resumable transfers where needed, transfer expiry, content quotas and crash cleanup. Shared storage credentials stay inside the trusted service/worker profile.
- Define versioned decimal/rational/string or typed-array artefacts for scientific data. Keep the integer-JSON control protocol unchanged unless a separately versioned protocol change is justified and verified.
- Exit: another host consumes an authorised artefact without the producer's filesystem; cross-principal and tampered transfers fail; interrupted upload never publishes partial content; archive/restore preserves references; large reports, code and data avoid oversized control frames.

**W12 — Reduce overhead with persistent Rust IPC and explicit worker supervision.** Size: large, split into transport and supervision PRs. Dependencies: W00, W02, W04 and W09. Primary areas: `ProtocolTransport`, Rust gateway, `supervision.py`, `process_group.py` and execution strategies.

- Profile the complete path first: 50 ms status polling, repeated Rust command launch, completed-prefix traversal, SQLite schema/write checks and full directory walks. Retain the benchmark harness with bounded raw results; do not store repetitive generated evidence permanently.
- Implement a persistent per-run Rust gateway behind the existing transport port. Use framed bounded messages, request correlation, protocol negotiation, deadlines and controlled back-pressure. Separate cheap local cancellation checks from durable expiry reconciliation.
- Retain command-per-process transport temporarily as a compatibility strategy. Compare both implementations on the same recorded conformance/failure traces. Retrying a lost IPC reply is allowed only where the operation's identity semantics make it safe.
- Persist the ready frontier, wake on events/timers, use read-only paths for unchanged state and account for storage incrementally with periodic reconciliation. Optimisations must preserve recovery invariants.
- Replace implicit Python fork inheritance with registered worker executables and a bounded protocol. Move process ownership/deadlines/termination into Rust in a second step; Python SDK workers remain supported. No pickle or request-selected executable imports.
- Exit: old/new transport and execution strategies agree on observable contract traces; teardown leaves no owned descendants; malformed/truncated IPC cannot cause admission; a threaded client can use the standalone service; measured CPU/process/latency improvements meet W00 objectives. Native OS profiles are advertised only after their own supervision tests.

**W13 — Add explicit workspace, tool and durable-context profiles.** Size: large. Dependencies: W02, W08, W10–W12.

- Retain text-only inference as one profile. Add read-only repository analysis, isolated code execution and explicitly authorised external-effect tools as separate profiles with declared capabilities.
- Pin immutable source/input snapshots; create bounded writable workspaces per attempt; expose only approved mounts, tools, environment and network access. Enforce OS/container resource controls for the declared threat model.
- Route every external-effect tool invocation through host allocation, certainty and recovery handling. Model-produced text cannot grant new permissions or expand budgets.
- Store context/session references, exact input snapshots, truncation/summarisation policy and token budgets. Version context compaction; recovery must not silently supply different input under an old invocation identity.
- Exit: a coding workflow reads an approved snapshot, invokes permitted tests, returns patch/test artefacts and resumes without repeating completed effects. Traversal, undeclared access and cross-principal artefact reads fail. Containment tests match the advertised trust profile. M2 release requires its W17 gates.

**W14 — Distribute execution to authenticated remote workers.** Size: extra large. Dependencies: M1, W10–W13. This package does not claim controller high availability.

- Introduce host identity, boot epoch, worker capabilities, architecture/runtime versions, trust zone, resource capacity and drain state. Use authenticated channels and revocable credentials; remote workers do not receive the controller's unrestricted secrets.
- Extend the explicit worker protocol to remote transport. The controller retains authoritative Rust protocol state, host checkpoints, invocation records and allocations. Workers retain workspace/cache state and a bounded durable spool of unacknowledged observations. The spool is retained until authoritative acknowledgement and cannot grant execution or spending authority.
- Add placement by authorised capabilities and capacity; atomic assignment claims, leases and monotonically increasing fencing tokens; bounded heartbeats, stale-state-write rejection and reconnect handling. A fenced worker cannot commit authoritative task state, but its authenticated receipt for the original invocation can enter an append-only reconciliation evidence path. The current authority validates that evidence before changing task/effect state.
- Validate authority immediately before granting external dispatch. A disconnected/stale worker receives no new admissions. An already granted/sent operation can remain uncertain; lease expiry cannot make it safe to repeat.
- Require durable controller-side evidence and all required output artefacts in verified durable storage before acknowledging worker completion. Where the worker has a result the controller lacks, reconnect resubmits its spooled observation under the original identity rather than rerunning the provider. Expired ownership routes it through reconciliation evidence, preserving useful observations without restoring stale authority. If the unacknowledged spool is lost, the effect remains unknown and cannot automatically rerun.
- Exit: run across at least two worker machines; test partition, worker death, controller restart, delayed heartbeats/replies, duplicate delivery, mixed versions and lost artefacts. Stable identities, quota ceilings and uncertainty survive. Document the remaining controller failure dependency explicitly.

**W15 — Complete federation and orchestrator ownership.** Size: medium/large. Dependencies: W03, W05, W08, W10 and W11; can proceed alongside W14.

- Add trusted peer registration, negotiated supported profiles, explicit outbound identities, capability checks and remote artefact transfer. Peer budgets stay independent unless a separate shared allocation authority is implemented.
- Preserve the exact local/remote task mapping through lost receipts, continuation, cancellation and remote status conflicts. Peer result completion remains subject to local acceptance rules.
- Harden Temporal and LangGraph integrations against nested retries/cancellation. Temporal owns business sequencing in that deployment mode; MAS owns each admitted operation. An outer retry reuses the original MAS logical request key.
- Provide deployment recipes for independent local hosts, federation, the native worker pool and an organisation already using Temporal. These are explicit choices; avoid two schedulers independently owning one logical effect.
- Exit: lost first peer receipt does not create a second task; peer impersonation/credential forwarding fails; peer outages do not block unrelated local work; orchestrator retries and cancellation preserve MAS identity and uncertainty. M3 release requires W14/W15 scope-specific W17 evidence.

**W16 — Add shared durable authority, high availability and controlled disaster recovery.** Size: extra large, multiple sequential PRs. Dependencies: M3 and accepted availability/recovery objectives from W00.

- Introduce transaction-oriented repositories for the complete state inventory above. Retain SQLite for local mode; implement PostgreSQL as the shared transactional backend and object storage for artefacts. The schema/transaction design must cover Rust protocol records and host journals as well as scheduling.
- Define atomic claim/fence/allocation transitions and unique command identities. If one action spans stores, use durable intent/receipt recovery with proven ordering; never permit stale authority to commit a new admission.
- Add controller epochs, leadership/ownership renewal, state-version compatibility and graceful leader drain. Followers may serve only reads consistent with the documented freshness/authorisation contract.
- Provide consistent backups, point-in-time recovery where supported, artefact integrity verification and a replacement activation procedure. Fence the original authority before permitting new work.
- An old snapshot may omit later charges. Activation requires complete recoverable admission history or a conservatively reconciled replacement allocation. Without that evidence, the restore remains audit-only. There is no unconditional reactivate switch.
- Exercise failover and disaster recovery with measured RPO/RTO, distinguishing local service recovery from unresolved external effects. Automatic promotion with open admission requires zero loss of acknowledged identity, admission and effect records. If the recovery profile can lose those records, promotion keeps admission closed until conservative reconciliation establishes usable authority and allocations.
- Exit: stale controllers/workers cannot obtain fresh admissions/reservations or overwrite current state; one current authority issues new allocations. Earlier granted calls and SDK retries remain charged as outstanding work and may still cause external effects after fencing. Original invocation IDs and unknown effects survive under the accepted replication profile, or admission remains closed pending reconciliation. Crash/partition/restore tests cover every durable store and measured objectives pass. M4 requires W17 release qualification.

**W17 — Verify and release each supported capability profile.** Size: continuous, bounded to each changed contract. Dependencies: the packages advertised by that release.

- Run the current Rust, Python, installed-container, schema, bridge interoperability, release-helper, dependency and image checks. Add contract tests for every storage/transport/execution strategy actually supported.
- Extend existing SPIN/Promela models for concurrent claims, hierarchical quotas, version availability, archive/dedup, retry certainty, cancellation races, distributed fencing and recovery authority. State bounds and fairness assumptions explicitly; retain mutation counterexamples that demonstrate each intended defect is detected.
- Link selected model transitions to implementation traces and fault-injection tests. Do not describe finite model checking as a proof of the whole Python/Rust/Linux/provider implementation.
- Add property tests for identity, canonicalisation, quota arithmetic and state invariants. Use multi-process and multi-machine tests where those are the actual failure boundaries.
- Test the published installed images on amd64 and arm64, not only source checkouts. Report consistent package, image, source, schema and protocol versions.
- Run credentialled acceptance for the selected OpenAI, Cursor, mixed, Cloud and peer profiles; corporate OAuth/TLS/proxy/private-CA paths where advertised. Paid calls run only in an already authorised environment and budget; missing access is an explicit unpassed gate.
- Execute migration, drain, backup, restore and rollback/forward-repair rehearsals. Capture representative load and a bounded soak with evidence for memory/process/storage growth, fairness and reconciliation latency.
- Keep one current requirement/evidence matrix in existing documentation. Update tests/docs in place, use CI artefacts for bulky logs, and rely on Git history for superseded versions. Release claims identify passed profiles and known limits.
- Produce and retain the accepted versioned packages/images, source bindings, checksums, SBOM/provenance and migration instructions. Publish through the authorised owner release workflow, then verify fresh installation and operation from those exact retained artefacts. Passing source tests without an installable accepted release does not complete a milestone.

**The dependency graph determines implementation order.** W17 accompanies every package; arrows below express prerequisites rather than staffing requirements.

| Package | Prerequisites | Main delivery gate |
|---|---|---|
| W00 | None | Baseline, identity and workload contract. |
| W01 | W00 | Versions coexist; unavailable old work cannot block the queue. |
| W02 | W00 | Failure certainty and output validity are distinguishable. |
| W03 | W01, W02 lifecycle contract | One authenticated lifecycle; durable scheduling intent. |
| W04 | W03 | Safe concurrency and fairness. |
| W05 | W02, W03 | Nonblocking peer operations and explicit transport policy. |
| W06 | W01, W03, W04 | Archival preserves deduplication and accounting. |
| W07 | W03, W04 | Durable queue/compute/human/business clocks. |
| W08 | W02, W03, W05, W06, W07 | Bounded recovery with no blind ambiguous retry. |
| W09 | W03 initially; W04–W08 for M1 | Actionable user/operator lifecycle. |
| W10 | W01–W04, W07, W08 | Extensible providers and bounded composition. |
| W11 | W03, W06, W09 | Authorised verified remote artefacts. |
| W12 | W00, W02, W04, W09 | Equivalent behaviour and measured overhead reduction. |
| W13 | W02, W08, W10, W11, W12 | Explicit workspace/tool/context contracts. |
| W14 | M1, W10–W13 | Remote execution under one durable authority. |
| W15 | W03, W05, W08, W10, W11 | Reliable peer and orchestrator ownership. |
| W16 | M3 | Shared authority, failover and controlled recovery. |

The first parallel lanes are W01/versioning and W02/provider outcomes after W00. After W03, scheduling and remote transport can proceed independently; observability can start against the service contract. W06 and W07 can proceed in parallel after W04. Artefacts, provider/composition extensions and the measured Rust transport work can proceed on stable interfaces after M1. Avoid simultaneous edits to shared store schemas without a single migration owner.

**Verification has explicit scenario coverage.**

| Scenario | Required observation |
|---|---|
| Same key, same content; same key, changed content | Original receipt; deterministic conflict. |
| v1 queued while v2 is deployed; v1 code removed | v2 progresses; v1 remains inspectable and individually blocked/drainable. |
| Historical instances exceed the old cap | New admission depends on active/storage policy, with retained dedup/accounting. |
| Two principals and several compositions contend | All aggregate and ancestor limits hold; fair eligible progress is measured. |
| Missing credentials, refusal, invalid output, ambiguous timeout | Distinct certainty/validity/diagnostic states with correct accounting. |
| Cancellation races provider completion | Known result/effect evidence survives; remote cancellation is not fabricated. |
| Crash at each intent/send/receipt/commit boundary | Original identity retained; ambiguous effects not blindly repeated. |
| Human pause spans restart and several days | Computation allowance preserved; response/business expiry enforced. |
| Slow peer and simultaneous local inspection | Unrelated management remains available. |
| Event disconnect, duplicate delivery and expired cursor | Resumption/dedup or explicit resynchronisation. |
| Corrupt, incomplete or unauthorised artefact | No published partial content or unauthorised bytes. |
| Worker/controller partition and stale rejoin | Stale admission/commit rejected; outstanding effects reconciled. |
| Old backup after later spending | Spent authority never reappears; uncertain restoration remains audit-only. |
| Real installed provider/bridge profiles | Exact model/runtime/network/account profile recorded and accepted. |

**Operational targets must be measurable before a release is called ready.** W00 sets numerical objectives for a declared reference machine and representative workflow rather than presenting arbitrary latency numbers as established capability. Record p50/p95/p99 admission and status latency, queue age per principal, dispatch throughput, control-plane overhead per active leaf, cancellation-intent latency, local cleanup latency, event lag, unresolved-effect age, memory/PID growth, storage growth, reconciliation cadence and recovery RPO/RTO. Provider time is reported separately from host overhead. Fairness and latency gates require sufficient repeated samples and stated load; an average over a mock run is insufficient.

For capacity qualification, test at the configured concurrency ceiling and beyond it, at both small and retained-history-heavy state sizes, with simultaneous short/long/paused jobs. Verify graceful back-pressure rather than relying on favourable throughput alone. The regression threshold and performance objective are fixed before optimisation measurements; any revision records its reason and does not silently redefine success.

**Migration proceeds through controlled versions.**

1. Inventory and freeze the current deployment's IDs, definitions, grants, request namespaces, schema versions, allocations, unresolved effects and artefact links. Take an authenticated, verified backup.
2. Install readers/validators for the new representation before enabling new writers. Dry-run migration under exclusive maintenance ownership on an isolated audit copy; perform no external effects there.
3. Backfill only facts supported by retained evidence. Missing implementation packages remain unavailable; unclassified legacy effects remain unknown; allocation totals remain exact.
4. Drain old executors or retain explicitly compatible workers. Atomically cut over the authoritative writer/dispatcher; old and new incompatible writers must never operate concurrently.
5. Check stable IDs, request mappings, principal grants, journal/remote references, accounting totals and artefact hashes before reopening admission.
6. Snapshot rollback requires no committed application mutation since that snapshot, or a verified lossless reverse migration/current-state-compatible binary. Submissions, cancellation, continuation, grant revocation, resolution and budget amendments all count as mutations, even when no provider call occurred. Otherwise prefer forward repair. Restoring a stale backup remains subject to authority fencing, preservation of acknowledged decisions and accounting reconciliation.
7. Remove compatibility code only after the documented support window and migration evidence permit it. Keep one current schema/API guide; Git retains superseded implementation history.

**Risks and decision rules remain visible.**

| Risk | Control and decision rule |
|---|---|
| Scope expands faster than operability | Ship M1 before distributed execution; keep review-sized PRs with demonstrated user behaviour. |
| New parallelism weakens exclusive effect ownership | Per-assignment ownership, global quota tests, crash schedules and explicit uncertain states precede performance tuning. |
| Archival breaks replay or restores budget | Compact identity/accounting records and reference-aware retention are prerequisites for deletion. |
| Registry migration fabricates old code identity | Block unavailable versions; never substitute current implementation under an old digest. |
| Rust port duplicates semantics or slows delivery | Migrate behind existing ports, compare traces and measure benefit; retain Python provider integration. |
| Shared queue hides worker-local authoritative state | Complete the state-ownership inventory before M3; M4 covers Rust and host stores together. |
| Failover repeats an escaped effect | Fencing blocks future authority, while reconciliation governs already escaped effects. |
| Restored snapshot omits spending | Require complete recoverable history or conservatively reissued authority; retain audit-only otherwise. |
| SDK/API/network profiles drift | Pin and report versions; run live acceptance for advertised profiles; classify unsupported capabilities before dispatch. |
| Diagnostics expose application data | Sanitised default fields, explicit access-controlled diagnostics and redaction tests. |
| Application code is mistaken for sandboxed code | Explicit trust/containment profiles and platform-specific verification. |

**Alternatives are retained where they reduce implementation cost.** An organisation with existing Temporal operations may use Temporal for cross-host business sequencing and keep MAS instances bounded and independently deployed. That path still needs M1 lifecycle/recovery fixes and tested identity ownership; it does not turn local Rust/journal state into highly available state. Independent tenant deployments are a simpler isolation/capacity option when shared budgets and transparent failover are unnecessary. A full Rust rewrite remains a separate future decision requiring a measured maintenance or performance advantage; it is not required to deliver this plan.

**Resources and execution ownership.** Assign one technical owner for identity/state migrations; separate owners can implement scheduler, providers, Rust runtime and operations against agreed contracts. A reviewer other than the implementer checks each changed effect/admission/recovery boundary. Required environments are Linux amd64/arm64 runners, an isolated multi-machine test network for M3, shared transactional/object storage for M4, and appropriately authorised live provider/identity/network test accounts. Keep estimates in small/medium/large/extra-large bands until W00 measurements and staffing are known. Do not promise dates or a production SLO from this planning document alone.

**Immediate execution order is W00, then W01 and W02 in parallel, followed by W03.** These changes remove the most disruptive current behaviours and define the contracts needed by every later phase. Implementation should continue through the selected milestone without repeatedly requesting confirmation for already authorised reversible work; release, paid external operations and authority-changing production actions follow the session's actual authorisation and deployment policy.

**Plan completion means coverage, while release completion means evidence.** This plan covers the review's identified gaps and their dependencies. A milestone is complete only when its implementations, migrations, documentation, supported-platform tests and acceptance evidence pass. No skipped live/deployment gate is relabelled as success.

**Source anchors.** The following sources identify the reviewed behaviour; all repository links are pinned to the reviewed commit.

- [Host architecture](https://github.com/emmett08/mas/blob/ccc7bb85/docs/host-architecture.md).
- [Composable workflows](https://github.com/emmett08/mas/blob/ccc7bb85/docs/composable-workflows.md).
- [Bridge service construction](https://github.com/emmett08/mas/blob/ccc7bb85/host/mas_host/bridge_config.py).
- [Task service registration, capacity and serial worker](https://github.com/emmett08/mas/blob/ccc7bb85/host/mas_host/task_service.py).
- [Composition binding and lifecycle](https://github.com/emmett08/mas/blob/ccc7bb85/host/mas_host/workflow_engine.py).
- [Composition history and capacity](https://github.com/emmett08/mas/blob/ccc7bb85/host/mas_host/workflow_store.py).
- [Invocation certainty and replay](https://github.com/emmett08/mas/blob/ccc7bb85/host/mas_host/runner.py).
- [Provider request and output contract](https://github.com/emmett08/mas/blob/ccc7bb85/host/mas_host/structured_provider.py).
- [Remote ownership and network dispatch](https://github.com/emmett08/mas/blob/ccc7bb85/host/mas_host/bridges/remote.py).
- [Process supervision](https://github.com/emmett08/mas/blob/ccc7bb85/host/mas_host/supervision.py).
- [Artefact storage](https://github.com/emmett08/mas/blob/ccc7bb85/docs/artifacts.md).
- [Operations, budgets and audit-only restore](https://github.com/emmett08/mas/blob/ccc7bb85/docs/operations.md).
- [Verification scope](https://github.com/emmett08/mas/blob/ccc7bb85/docs/verification.md).
- [Protocol checks](https://github.com/emmett08/mas/actions/runs/38623875881), [Host and Compose checks](https://github.com/emmett08/mas/actions/runs/38623875763), [Dependency and image security](https://github.com/emmett08/mas/actions/runs/38623875833): successful for the baseline when this plan was prepared.

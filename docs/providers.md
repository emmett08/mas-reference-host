# OpenAI, restricted Cursor and durable Cursor jobs

The host supports schema-validated inference through Cursor or OpenAI and
durable repository jobs through Cursor Cloud. Applications supply their own
instructions, input and output contracts. The local providers can classify text,
review configuration, propose a plan or produce another bounded JSON result.
A Cursor Cloud agent performs repository work whose lifetime is independent of
the local host. A Cursor API key is not an OpenAI API credential.

| Implementation | Contract | Completion means |
| --- | --- | --- |
| `cursor_provider.CursorLocalProvider` | `StructuredProvider` | A restricted SDK run returned an object satisfying the application's output schema. |
| `OpenAIStructuredProvider` | `StructuredProvider` | A Responses API call returned an object satisfying the application's output schema. |
| `InvoiceCursorProvider` / `OpenAIProvider` | Invoice example's `TotalProvider` | A model call returned a valid invoice total. |
| `CursorCloudJobAdapter` | `Adapter` for `cloud.submit` | A durable receipt identifies an accepted remote agent/run. |

Providers can be selected independently of the graph. The same local provider
classes can serve roles in star, mesh or tree workflows. Changing a topology
does not create additional provider capabilities or a distributed transport.

## Reusable model adapters

Use `StructuredAgentAdapter` with a versioned `OperationContract` to invoke a
model through the host's existing admission, journal and recovery boundaries.
The contract supplies allowed callers and input/output schemas; instructions
belong to the application. For example, an application can declare a
`classify-change/1` operation with a string `description` input and an enumerated
`category` output, without adding a provider method or editing the host.
See [arbitrary model roles and operation contracts](composable-workflows.md#operation-contracts-and-arbitrary-model-roles)
for registration and execution.

The canonical Cursor import is
`from mas_host.cursor_provider import CursorLocalProvider`. Its `infer` method
accepts a `StructuredRequest`; `CursorTextSession.execute_prompt` owns the shared
restricted SDK lifecycle. Invoice instructions and total validation live in
`invoice_cursor.py`. Generic providers do not depend on that application module.

For compatibility, `structured_cursor.CursorStructuredProvider` names the generic
provider, and the old `cursor_local.CursorLocalProvider` import remains an alias
for `InvoiceCursorProvider`. Migrate invoice callers to
`mas_host.invoice_cursor.InvoiceCursorProvider` and new applications to
`mas_host.cursor_provider.CursorLocalProvider`; the old invoice import is retained
until a major API release. Existing invoice workflow descriptors and replay
identities remain unchanged.

Generic registry construction validates public configuration without reading
credentials, importing optional model SDKs or starting clients. Inspection,
cancellation and recovery therefore remain available when live credentials or
the Cursor runtime are unavailable. Credential resolution and runtime checks
occur when an admitted invocation executes. An injected client remains owned
by its caller; a client created for an invocation is closed by that invocation.

## Credentials and model selection

Use a Cursor user or service-account API key. Team Admin keys are not supported
by the SDK. The key must have access to the selected model and, for Cloud jobs,
the repository and cloud-agent service. Select model identifiers from your
account's current catalogue; MAS does not assume that a model available through
OpenAI is also available through Cursor. The local adapter rejects `auto-smart`
because it has no configured Router optimisation policy.

Each provider reads its own `*_API_KEY_FILE`, or its `*_API_KEY` environment
variable when no file is configured. An unreadable or invalid secret file fails
without falling back to the environment. Credentials are excluded from provider
descriptors, protocol messages and reports. Keep secrets out of `.env`, source
control, prompts and job specifications.

For Bash, prompt without echoing a token or placing it in shell history:

```bash
read -rsp 'Cursor API key: ' CURSOR_API_KEY
export CURSOR_API_KEY
```

For mixed execution, also supply OpenAI's credential:

```bash
read -rsp 'OpenAI API key: ' OPENAI_API_KEY
export OPENAI_API_KEY
```

## Compose: two Cursor roles or a mixed mesh

Use Docker Compose 2.24.4 or newer for the Cursor overlays, which replace the
default temporary-filesystem allocation with a larger mount. Copy `.env.example`
to `.env` and set the model identifiers. For two Cursor
roles, both `MAS_PLANNER_MODEL` and `MAS_REVIEWER_MODEL` are required:

```sh
docker compose -f compose.yaml -f compose.cursor.yaml run --build --rm mesh
```

For a Cursor planner and OpenAI reviewer, set `MAS_PLANNER_MODEL` to a Cursor
model. `MAS_REVIEWER_MODEL` defaults to `gpt-4o-mini`; change it to an OpenAI model
that supports Responses API structured outputs if needed:

```sh
docker compose -f compose.yaml -f compose.mixed.yaml run --build --rm mesh
```

The mixed override mounts two separate secret files. It does not insert their
contents into the container environment. Both overrides retain the named state
volume and non-root execution. After the run, remove the shell variables:

```sh
unset CURSOR_API_KEY OPENAI_API_KEY
```

The optional `Dockerfile.cursor` uses a pinned Wolfi/glibc runtime for Linux
amd64 and arm64. Its complete Python runtime package set is SHA-256-locked and
verified against Wolfi's trusted package-signing keys during offline
installation. Debian is used only to build Python wheels. The SDK's native
bridge has no supported musl wheel, so this image is separate from the default
Alpine image. It contains both provider SDKs and is built locally by these
Compose commands. The image replaces the SDK's bundled Node executable with
SHA-256-locked Node 24.21.0 and applies the reviewed bridge dependency repair
described below. The SDK has separate licence terms; see
[third-party notices](../THIRD_PARTY_LICENSE.txt).

## Native and library use

The host's supervised execution requires Linux. For the locked Cursor runtime,
use CPython 3.14 on amd64 or arm64 with glibc, install the Rust binaries, then
run these commands from the repository checkout:

```sh
python -m pip install --require-hashes -r host/requirements-build.txt -r host/requirements-cursor.txt
python -m pip install --no-deps --no-build-isolation ./host
python scripts/install-cursor-runtime.py
export PATH="$PWD/rust-protocol/target/release:$PATH"
```

Run the installer with the same interpreter and environment that contains
`cursor-sdk==1.0.37`. It downloads the official Node 24.21.0 archive for the
current supported architecture, verifies the locked archive and executable
SHA-256 values, and replaces the SDK bridge's Node executable in place. It also
replaces the bridge dependency's obsolete Headers polyfill with Node's native
`Headers`, checks the exact source hashes, removes the unused `undici` and
`@fastify/busboy` package trees, and updates the installed wheel's `RECORD` to
describe the changes. This removes the dependency code as well as its metadata.

Repeat the installer after reinstalling the SDK. Local-provider preflight checks
the Node executable identity, repaired source files and dependency removals
before starting the bridge. The Compose build performs these steps automatically.
An interrupted dependency deletion or an unrecognised source change fails
integrity checks; reinstall `cursor-sdk==1.0.37` from the locked dependencies,
then rerun the installer. Do not delete more files to bypass that check.

For an offline installation, prepare the verified files on a connected machine
of the same architecture, then copy the directory to the deployment host:

```sh
python scripts/install-cursor-runtime.py --out /absolute/cursor-node-files
python scripts/install-cursor-runtime.py --source /absolute/cursor-node-files
```

The first command prepares the Node executable and licence; the second installs
them and applies the same bridge repair to the current environment. Both use
the repository's versioned runtime lock, so copying files does not bypass
executable verification. Add `--arch amd64` or `--arch arm64`
to `--out` when preparing files for a different target architecture. Installation
requires Linux with the matching architecture.

The combined lock contains both SDKs. OpenAI-only execution can continue using
`host/requirements-openai.txt` and the existing Alpine image. Cursor Cloud HTTP
jobs use the standard library and do not require the local SDK.

Configure a native mixed run using private key files and explicit role settings:

```sh
export CURSOR_API_KEY_FILE=/absolute/private/cursor-key
export OPENAI_API_KEY_FILE=/absolute/private/openai-key
export MAS_PLANNER_PROVIDER=cursor
export MAS_PLANNER_MODEL=YOUR_CURSOR_MODEL
export MAS_REVIEWER_PROVIDER=openai
export MAS_REVIEWER_MODEL=gpt-4o-mini
python -m mas_host --mode mixed --state-dir .mas-state
```

Replace `YOUR_CURSOR_MODEL` before running. `--mode cursor` defaults both roles
to Cursor; `--mode openai` defaults both to OpenAI. Explicit per-role provider
settings override those live defaults. `--mode mixed` requires both providers
to be specified. `--mode mock` always stays offline.

`MAS_PLANNER_RESERVATION_MICROUSD` and `MAS_REVIEWER_RESERVATION_MICROUSD`
override `MAS_CALL_RESERVATION_MICROUSD` for the corresponding role. Live
reservations must be positive; mock reservations are zero. The defaults are
100,000 micro-US-dollars per invocation. A reservation is a local admission
allowance, not a provider invoice or a monetary ceiling enforced by the provider.

Invoice example callers can inject an `InvoiceCursorProvider` or `OpenAIProvider` into the
existing `PlannerAdapter`, `ReviewerAdapter` or `run_demo` parameters. The
factory in `provider_config.py` is the shared construction path for the CLI and
live acceptance. Provider selection requires no Rust wire-format change.

## Cursor through an environment proxy

The default `MAS_CURSOR_TRANSPORT=sdk-default` preserves the original SDK
transport and workflow identity. On a host whose outbound access requires an
HTTP proxy, explicitly select:

```sh
export MAS_CURSOR_TRANSPORT=environment-proxy-http1
```

This setting applies only to selected Cursor local providers. Library callers
use `CursorLocalProvider(..., transport_policy="environment-proxy-http1")` from
`mas_host.cursor_provider`, or `InvoiceCursorProvider` for the invoice example.
MAS launches its pinned Node runtime with `--use-env-proxy` and configures the
same SDK instance used by the bridge with
`configureCursorSdk({local: {useHttp1ForAgent: true}})` before bridge startup.
The bootstrap is shipped in the host package; its hash and transport policy
form part of the invocation's recovery identity. Unknown values fail before
admission. Arbitrary Node flags or startup hooks are never inherited.

Supply the existing `HTTP_PROXY`/`HTTPS_PROXY` and trusted CA configuration to
the runtime. Lowercase proxy variables retain Node's precedence. The child
preserves the effective `no_proxy`/`NO_PROXY` exclusions and adds only local
callback hosts, so authenticated bridge callbacks stay on loopback. Python's
bridge client continues to use `trust_env=False`. The parent environment and
TLS certificate verification remain unchanged. Compose overlays pass the
transport selection; add proxy variables and any CA mount through an
operator-controlled runtime override. Keep proxy credentials out of tracked
configuration and reports.

The SDK's server-supplied HTTP/2 policy can override its local HTTP/1.1
preference. This mode therefore does not guarantee compatibility with every
proxy or account policy. Verify the actual generation path with a fresh bounded
acceptance run; catalogue access and local agent creation alone establish only
prerequisite connectivity. Do not change the transport of an unresolved run
and resubmit it. Preserve the original state and admit a separate test run.

## Restricted local execution and recovery

The pinned local adapter creates one fresh private workspace per invocation,
sets `tools=[]`, disallows MCP and task delegation, supplies empty MCP/subagent
maps and loads no ambient setting sources. It rejects observed tool calls and
validates bounded, exact integer-JSON output. The checker independently computes
the result. Model-generated commands are never executed by this workflow.

The adapter launches the SDK's bundled bridge after durable MAS admission and
attempts to close its owned agent and client on normal return or exception.
The bridge runs in the supervised process group with a private home and a
restricted environment. Forced process termination cannot run Python cleanup;
the private temporary directory may remain until the host or container clears
it. Process-group termination is a separate guardian responsibility. These
controls form an SDK execution policy for trusted code, not an OS sandbox.

Bridge RPC retries are disabled. The pinned SDK may still retry underlying
model requests, so one MAS invocation counts one agent-run submission rather
than one guaranteed model request. Killing the local process or requesting
cancellation cannot establish that remote execution or billing stopped.

Resume with the original provider, model and effect-relevant settings. They form
part of workflow identity. The host reuses recorded successful invocation
outputs and blocks uncertain effects. It never switches providers automatically
after a timeout. See [host recovery](host-runner.md#persistence-and-explicit-recovery).

Cursor invocation failures retain a bounded diagnostic stage in the durable
call evidence. Only fixed provider/stage labels and a cleanup-failure flag cross
the supervised boundary; SDK messages, headers, response bodies and credentials
are excluded. When cleanup also fails, the original failure stage is retained.
These diagnostics locate the local failure boundary, not whether a remote
effect occurred. The outcome remains unknown until supported recovery evidence
resolves it; diagnostics never authorise an automatic retry.

## Durable Cursor Cloud jobs

Copy [the example specification](../examples/host/cursor-cloud-job.json) to a
private `cursor-job.json`, replacing the repository, ref and model with values
available to your Cursor account:

```json
{
  "repository": "https://github.com/YOUR_ORGANISATION/YOUR_REPOSITORY",
  "ref": "main",
  "model": "YOUR_CURSOR_MODEL",
  "prompt": "Inspect the repository and propose a test plan. Do not change files.",
  "reservation_microusd": 100000,
  "mode": "plan",
  "auto_create_pr": false
}
```

Save it as `cursor-job.json`. Prefer an immutable commit ref when the job must
refer to a reproducible source version. `mode` may be `plan` or `agent`; plan
mode expresses agent behaviour, not an OS permission boundary. The adapter
always sends `workOnCurrentBranch=false`; automatic PR creation requires an
explicit `auto_create_pr=true`. Review the prompt and repository permissions
before submitting: Cloud agents can perform repository actions under the
selected account's authority.

The standalone Cloud Compose service uses the ordinary Alpine image and needs
only the Cursor secret. With `CURSOR_API_KEY` supplied in the shell, run:

```sh
docker compose -f compose.cursor-cloud.yaml run --build --rm \
  -v "$PWD/cursor-job.json:/job.json:ro" jobs submit review-001 --spec /job.json
docker compose -f compose.cursor-cloud.yaml run --rm jobs status review-001
docker compose -f compose.cursor-cloud.yaml run --rm jobs reconcile review-001
docker compose -f compose.cursor-cloud.yaml run --rm jobs status review-001 --result
```

The specification must be readable by container UID 10001. The named
`cursor-jobs-state` volume retains receipts and journals across invocations;
`docker compose -f compose.cursor-cloud.yaml down --volumes` deletes it. Do not
remove the volume while work is active or its outcome is unresolved. Running
the service without a command prints help and submits no job.

For native execution, use one stable local job ID for the intended operation:

```sh
python -m mas_host jobs --state-dir .mas-state submit review-001 --spec cursor-job.json
python -m mas_host jobs --state-dir .mas-state status review-001
python -m mas_host jobs --state-dir .mas-state reconcile review-001
python -m mas_host jobs --state-dir .mas-state status review-001 --result
```

`status` reads local state without a credential or network request. `reconcile`
polls the exact saved agent/run and updates the durable observation. Repeat it
when needed; the CLI does not run a background poller. `--result` includes
untrusted remote text for review. A remote `FINISHED` status records service
completion; it is not an independent correctness check or authorisation to
merge the resulting work.

The local job store persists specification and client-assigned agent identity
before dispatch. The normal runner reserves the submission before the HTTP
POST. A repeated job ID with a changed specification is rejected. A lost launch
response leaves `launch_unknown`; repeating submit never issues another POST.
Use a new job ID only for an intentionally separate, separately charged job.

To request cancellation, then observe its outcome:

```sh
python -m mas_host jobs --state-dir .mas-state cancel review-001
python -m mas_host jobs --state-dir .mas-state reconcile review-001
```

A cancellation acknowledgement means only that the request was accepted. The
job remains nonterminal until a correlated remote observation establishes a
terminal state. Local expiry and local process termination do not cancel the
remote job. After an uncertain cancellation request, first reconcile; an explicit
`cancel review-001 --retry` requests cancellation of the same nonterminal run again.
It never launches another job. Retain the private state directory until remote
and local outcomes have been reconciled.

Deployment health reports the last locally recorded job states; it does not
poll Cursor. Retention preserves the originating MAS run while its job is active
or unresolved. Backups include the cloud-job database. Restores remain audit
copies: local status inspection is allowed, while submissions, polling updates,
attachments and cancellation are blocked to prevent stale state authorising
remote operations.

For a lost launch response, obtain the exact remote run ID from independent
Cursor records. Record the source of that evidence in a private JSON object,
then attach and, if needed, resolve the uncertain MAS submission:

```sh
python -m mas_host jobs --state-dir .mas-state reconcile review-001 \
  --run-id run-EXACT_ID --evidence reconciliation.json --resolve-host
python -m mas_host jobs --state-dir .mas-state status review-001 --audit
```

Attachment verifies the frozen remote agent settings and exact run identity;
it never substitutes the agent's latest run. A missing lookup is not proof
that dispatch had no effect. `--resolve-host` records the submission receipt
as the invocation output. When the run identity is already saved,
`reconcile review-001 --resolve-host --evidence reconciliation.json` resolves
only the local receipt and needs no credential or network request. It does not
complete the remote repository task or reverse a local deadline. Resume the
original MAS run only after reconciliation:

```sh
python -m mas_host jobs --state-dir .mas-state submit review-001 \
  --spec cursor-job.json --resume /absolute/original/run-directory
```

### Shared PostgreSQL job authority

`SharedJobStore(authority, cache_dir, principal=...)` and
`SharedDurableJobService` use the same native PostgreSQL namespace and controller
epoch as the shared runner. They retain the original job ID, specification,
client-assigned agent ID, exact remote run ID, submission allocation, cancellation
intent and reconciliation audit. The local `JobStore` remains the default.
Provision the shared authority and its deployment/principal allocation scopes
before constructing this port; it never initialises over an existing database.

A trusted deployment factory returns the bound store:

```python
def cloud_jobs():
    from mas_host.shared_jobs import SharedJobStore
    authority = configured_shared_authority()  # Resolve private credentials here.
    return SharedJobStore(authority, '/var/cache/mas/jobs', principal='operator-1')
```

Use the same factory for submission and inspection:

```sh
python -m mas_host jobs --factory deployment:cloud_jobs submit review-001 --spec cursor-job.json
python -m mas_host jobs --factory deployment:cloud_jobs status review-001 --audit
python -m mas_host jobs --factory deployment:cloud_jobs reconcile review-001
```

The CLI uses shared native admission for this store. Job IDs are scoped to the
factory's trusted principal. Status reads PostgreSQL without contacting Cursor;
the cache has no authoritative SQLite database. In shared receipts,
`host_run_dir` carries the original native run ID, which can be supplied to
`--resume` from a replacement controller without the old cache directory.

An epoch-checked transaction records `launch_unknown` before creating a cloud
agent. It verifies the original native invocation, frozen principal, exact
reservation and current run owner. A replayed dispatch grant cannot make another
request. Attachment still requires an evidenced exact run and verifies the
repository/ref against the frozen agent settings. Host resolution settles only
the original submission receipt; it never refunds allocation or completes the
remote repository work.

Shared observations carry retained operation IDs and revisions. A late poll
cannot overwrite a newer or terminal snapshot; its evidence is retained. A
cancellation acknowledgement remains distinct from terminal confirmation, and
repeating an uncertain cancellation requires `--retry`. Controller replacement
does not replay an old mutation. The repository bounds retained jobs at 100,000
per database namespace and operations at 10,000 per job; exhaustion rejects new
operations. Guarded [shared recovery](shared-recovery.md) includes these tables
and their original allocations in its sealed inventory.

`test_shared_jobs.py` exercises actual PostgreSQL admission, lost create/begin
receipts, controller replacement, evidenced attachment, principal isolation,
late observations and explicit cancellation retry with controlled provider
responses. It makes no live Cursor requests. The PostgreSQL workflow supplies
the real database; a skipped local run is not backend acceptance evidence.

## Offline failure and recovery walkthrough

Suppose a repository job is accepted remotely, but its launch response is lost.
MAS must retain the uncertain submission through restart, avoid a second launch
and attach only the exact remote run supported by evidence. The existing
[failure-injection tests](../host/tests/test_cursor_jobs.py) make these steps
executable without a Cursor account or any repository changes.

From the repository root on Linux with Python 3.10 or newer, run:

```sh
PYTHONPATH="$PWD/host:$PWD/host/tests" python3 -m unittest -v \
  test_cursor_jobs.DurableCloudTests.test_intent_before_network_and_stable_receipt \
  test_cursor_jobs.DurableCloudTests.test_lost_create_reply_stays_unknown_on_restart_and_is_not_retried \
  test_cursor_jobs.DurableCloudTests.test_404_or_failed_attachment_never_authorises_create \
  test_cursor_jobs.DurableCloudTests.test_audited_attachment_uses_explicit_run_not_latest \
  test_cursor_jobs.DurableCloudTests.test_saved_submission_can_resolve_host_offline_without_api_key \
  test_cursor_jobs.DurableCloudTests.test_terminal_snapshot_is_immutable_and_poll_is_local
```

These six independent scenarios call the production durable-job service and use
real temporary SQLite stores with fixture admission records. The injected cloud
client performs no network requests. No Rust build or optional Python package is
needed for this command. Each test has fresh state; the sequence describes the
recovery procedure rather than sharing one job between tests.

| Step | What the executable scenario checks |
| --- | --- |
| Record intent before dispatch | The durable state already contains `launch_unknown`, the agent identity and admitted call identity when submission begins. Repeating a successful submission returns its saved receipt. |
| Lose the launch response and reopen state | A new service/store instance still observes `launch_unknown`; another submission attempt is rejected without another create call. |
| Fail to establish the remote outcome | A failed attachment leaves the job uncertain and does not authorise another launch. The test injects a lookup failure; it does not call the live HTTP API. |
| Attach an evidenced run | Attachment validates the supplied run and records operator evidence. An unrelated `latestRunId` is ignored. |
| Resolve the local invocation | A saved receipt and reconciliation evidence resolve the host invocation without credentials or another provider call. This resolves submission, not the remote task. |
| Observe completion | A terminal snapshot is retained unchanged; later polling uses the local record. Remote result text remains untrusted. |

Success prints six `ok` results and a final `OK`. Failures exit non-zero. The
temporary databases are removed by the tests; no credentials, paid calls or
live-provider acceptance are involved.

For the complete host path, build the pinned Rust release binaries using the
[build instructions](../README.md#build-and-run), then run:

```sh
cargo build --release --locked --manifest-path .protocol/rust-protocol/Cargo.toml --target-dir "$PWD/rust-protocol/target"
PYTHONPATH="$PWD/host:$PWD/host/tests" python3 -m unittest -v \
  test_cursor_jobs.CloudRunnerIntegrationTests.test_process_death_after_remote_acceptance_reconciles_without_redispatch
```

This single scenario uses the Rust gateway, deployment admission and supervised
adapter execution. A fixture writes a simulated remote acceptance record and
then exits abruptly. The test reopens durable state, checks that resubmission
is blocked, attaches the evidenced run, resolves the invocation and resumes the
original host run. It verifies that only one simulated launch and one call
allocation exist. Remote acceptance is simulated; the local process death and
recovery are real.

For an actual job, use the [submission and reconciliation commands above](#durable-cursor-cloud-jobs).
Retain the original job ID and state directory, obtain the exact remote run ID
from independent provider records, attach it with evidence, resolve the local
submission and resume its original run. If the run identity cannot be established,
keep the outcome uncertain. An absent lookup result is insufficient evidence
for a new submission. Review the terminal job's output before accepting changes.

## Verification and operating limits

Offline tests exercise provider contracts, output rejection, bridge cleanup,
frozen bindings and durable-job failure/recovery paths using injected clients.
The [Cursor Promela model](https://github.com/emmett08/mas/blob/main/formal/cursor-jobs-notes.md) separates launch,
receipt, polling and cancellation, with deliberate mutations that must fail.
These checks do not execute a real provider or prove the external service.

Run the [live acceptance commands](deployment-acceptance.md#explicit-live-provider-smoke-test)
with your own credentials and persistent state for the selected local-provider
combination. Cloud acceptance additionally requires a deliberately authorised
repository job, a correlated terminal observation and review of its result.
Missing credentials, account permissions or native runtime support remain failed
or blocked acceptance, not passing evidence.

Official API references consulted on 14 October 2026:

- [Cursor Python SDK](https://cursor.com/docs/sdk/python).
- [Cursor Cloud Agents API](https://cursor.com/docs/cloud-agent/api/endpoints).
- [OpenAI Responses API](https://developers.openai.com/api/reference/python/resources/responses).

Actual billing, service availability, model correctness and repository side
effects remain external obligations. Bounded formal checks do not establish
arbitrary-scale progress or exactly-once remote effects.

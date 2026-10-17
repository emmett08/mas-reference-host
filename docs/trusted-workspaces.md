# Trusted workspaces, tools and context snapshots

`mas_host.workspaces` integrates approved input artefacts and explicitly
registered tools with the Rust executable-worker supervisor. The default
profiles are **trusted-code-execution** and **trusted-repository-analysis**.
Existing restricted text-only inference remains available separately.

These profiles run trusted application code as the host's operating-system
user. They are **not sandboxes**. They do not confine filesystem syscalls or
network access, and cannot safely execute arbitrary model-generated code.
`isolated-untrusted-code` and other unqualified profile names are rejected.

An optional **isolated-container-execution** strategy is implemented below. It
requires a separately qualified Linux Docker deployment. The implementation
environment had no Docker daemon, so its genuine containment gate remains
unpassed there; deterministic policy tests are not evidence of OS isolation.

## Authority and resource boundaries

| Boundary | Current enforcement |
|---|---|
| Tool selection | Trusted startup registers `WorkspaceTool` objects. `WorkspaceTools.resolve()` checks the immutable policy allow-list. Request data cannot select a command, import or executable. |
| External effects | A tool declaring external effects requires explicit `allow_external_effects=True`. Each complete tool process is admitted through `context.invoke`; child requests cannot create nested calls or delegation inside that invocation. |
| Inputs | A principal-bound `ArtifactStore` authenticates every reference before materialisation. Exact immutable manifests, contents and file sets are checked before and after execution. |
| Workspace ownership | Principal, logical run and attempt determine a hashed private directory. Host file operations use directory descriptors and reject traversal, symlinks, hardlinked files and non-regular objects. |
| Read-only snapshots | Input files are created with mode `0400`. Trusted workers must respect this contract; the shared OS user can bypass permissions. Mutation is detected and publication fails. |
| Process resources | The Rust supervisor enforces the registered memory, CPU and per-file limits and reaps owned descendants. |
| Aggregate storage | Input file/byte limits are enforced before materialisation. Total workspace file/byte limits and output limits are checked after execution and before publication. These are not hard filesystem quotas during execution. |
| Environment/network | WorkerSpec passes only its explicit environment. Network restriction remains a trusted-tool contract, not an OS containment claim. |
| Cleanup | An explicit resolution artefact and an exclusive workspace lease are required. The Rust guardian retains that lease until descendants are reaped. |

A principal supplied to `WorkspaceManager` must come from the host's
authenticated session. Workspace references are identifiers, not bearer
capabilities. All managers and registrations are trusted host configuration.

## Prepare and execute an approved tool

An application worker follows `mas_host.worker_entry.run_worker`, with a fixed
factory allow-list. Its factory receives trusted configuration and uses the
workspace's `inputs`, `work` and `outputs` directories. The executable-worker
protocol is required; an arbitrary CLI cannot be substituted directly.

```python
from hashlib import sha256
from pathlib import Path
import sys

from mas_host.artifacts import ArtifactStore
from mas_host.executable_worker import RegisteredProcessExecutor, WorkerLimits, WorkerSpec
from mas_host.workspaces import (
    WorkspaceAdapter, WorkspaceExecutor, WorkspaceInput, WorkspaceManager,
    WorkspaceOutput, WorkspacePolicy, WorkspaceTool, WorkspaceTools,
)

store = ArtifactStore(Path("/srv/mas/artefacts"), namespace="project", principal=principal)
manager = WorkspaceManager(Path("/srv/mas/workspaces"), principal=principal, artifacts=store)
policy = WorkspacePolicy("approved-tests", "1", ("test-runner",))

script = Path("/srv/mas/application/test_worker.py")
worker = WorkerSpec(
    "test-runner", "1", (sys.executable, str(script)),
    factory="approved-tests", configuration={}, cwd=script.parent,
    environment={"PYTHONPATH": "/srv/mas/application:/srv/mas/host",
                 "PYTHONDONTWRITEBYTECODE": "1"},
    limits=WorkerLimits(memory_bytes=512 * 1024 * 1024, cpu_seconds=120),
    pinned_files={str(script): sha256(script.read_bytes()).hexdigest()},
)
tools = WorkspaceTools((WorkspaceTool("test-runner", worker, reservation_microusd=1000),))
reference = manager.prepare(
    logical_run_id, attempt_id, policy,
    (WorkspaceInput("source.py", approved_source_reference),),
)
adapter = WorkspaceAdapter(
    manager, reference, policy, tools.resolve("test-runner", policy),
    (WorkspaceOutput("test-report.json", "application/json"),
     WorkspaceOutput("changes.patch", "text/plain")),
)
executor = WorkspaceExecutor(RegisteredProcessExecutor("/srv/mas/bin/mas-gateway"))
```

For a direct `Runner`, register `adapter` and pass `executor`. Durable local and
shared services select `WorkspaceExecutor` automatically for `WorkspaceAdapter`
registrations, including the existing container profile. Other actors can keep
their own execution profiles and use the same parent-owned context protocol.

A `Workflow` registration freezes the complete workspace tool descriptor,
policy and output manifest. Its trusted `adapter_factory` uses the assigned
`ExecutionBinding` principal, run and revision to prepare each attempt's
workspace. The returned adapter must preserve those approved capabilities;
changing a tool command, worker configuration, policy or output contract changes
workflow identity and cannot be substituted during recovery. The manager's
cache directory and each assigned `WorkspaceRef` are excluded from the static
workflow binding. The exact workspace, input and context identities are instead
recorded in that run's admitted invocation.

The workspace is bound to one attempt and one admitted tool execution. Use a new
attempt identity for a distinct execution; completed calls resume through the
existing journal checkpoints. A replacement shared controller can materialise
the original immutable inputs in a new cache and replay the recorded output
without rerunning the tool or publishing another artefact. Reusing a
non-pristine workspace cannot authorise another execution.

`WorkerSpec.with_workspace()` preserves the original environment and pinned
dependencies while binding the private working directory. The executable,
explicit script/dependency pins and each materialised input are checked before
dispatch. Interpreted code and dependencies must be included in the deployment's
pins or immutable image identity; pinning an interpreter alone does not freeze
all imported packages.

The worker's trusted configuration receives a `workspace` object containing its
identity and fixed relative directory names. Input directories must stay
unchanged; put caches, temporary files and other writable state in `work`.
Publishable results must be written beneath `outputs` at the registered output
paths. Task input does not extend that path allow-list.

The adapter returns `result`, immutable `artifacts`, and the workspace identity.
Each published artefact includes source/context SHA-256 provenance. Its admitted
invocation evidence also records worker and tool identity. Publication remains
subject to the artefact store's own quotas. A crash during multi-output
publication may retain an immutable partial set; the invocation remains unknown
until reconciled, and automatic execution is not repeated.

## Exact durable context

`mas_host.context_snapshots` selects an ordered prefix of complete UTF-8 text
artefacts under item, byte and token limits:

```python
from mas_host.context_snapshots import ContextPolicy, TokenCounter, build_context

counter = TokenCounter("approved-model-tokeniser", "pinned-v1", count_model_tokens)
context = build_context(
    store, ordered_context_references,
    policy=ContextPolicy(max_items=12, max_bytes=24000, max_tokens=6000),
    tokeniser=counter,
)
reference = manager.prepare(logical_run_id, attempt_id, policy, inputs, context=context)
```

The token-counting callable is explicitly supplied by trusted startup code. It
must match the selected model and be versioned when its behaviour changes. Its
budget covers selected context text, including separators; reserve additional
space for instructions, tool schemas and generated output separately.

Selection stops at the first item that exceeds a bound. It neither silently
summarises nor partially truncates a document. The published `mas-context/1`
snapshot contains exact text, selected references, omitted reference identities,
the omission reason, measured byte/token counts and the selection/tokeniser
versions. Recovery reads that snapshot, preserving the original input. Changing
the selection policy or tokeniser identity produces a different artefact.

Context references and workspace inputs/outputs receive live retention links.
Release a shared context's retention link only when its other consumers have
also finished; workspace cleanup does not infer that relationship.

## Resolution and retention

After the application has reconciled every associated effect, retain a
resolution receipt as an artefact and call:

```python
manager.release(reference, resolution=resolution_reference)
manager.cleanup(reference)
```

The application owns the interpretation and authorisation of that receipt.
Worker exit, apparent success and elapsed time alone do not prove that a remote
effect resolved. Release closes the workspace's artefact-retention links;
cleanup takes an exclusive lease before removing local files. A small retired
identity record prevents the same principal/run/attempt from being recreated
after deletion. Cleanup never follows links to their targets.

Current tests cover admitted execution, durable replay without another process,
content and script drift, authorised context reads, principal separation,
path traversal, symlinks and hardlinks, mutation of source snapshots, allow-list
and effect-policy rejection, and retention/quiescence rules. A deployment still
needs its W17 live-provider, capacity and recovery qualification. The optional
container profile additionally requires the real-daemon gate below.


## Optional isolated container execution

`ContainerSpec` selects a retained OCI image by `sha256:` image ID or by
`repository@sha256:` digest and one registered absolute command. The image must
contain the fixed entrypoint in `Dockerfile.workspace`. Build that image, retain
its identity, then use it in a trusted registration:

```python
from mas_host.container_execution import ContainerSpec, DockerRuntime

policy = WorkspacePolicy("isolated-tests", "1", ("tests",),
                         profile="isolated-container-execution")
worker = ContainerSpec("tests", "1", retained_image_digest,
    ("/usr/local/bin/python", "-I", "/workspace/inputs/tests.py"),
    memory_bytes=256 * 1024 * 1024, cpu_seconds=60, cpus=1, pids=64)
tool = WorkspaceTool("tests", worker, reservation_microusd=1000)
reference = manager.prepare(logical_run_id, attempt_id, policy,
    (WorkspaceInput("tests.py", approved_tests_reference),), context=context)
adapter = WorkspaceAdapter(manager, reference, policy, tool,
    (WorkspaceOutput("stdout.txt", "text/plain"),
     WorkspaceOutput("stderr.txt", "text/plain"),
     WorkspaceOutput("changes.patch", "text/plain")))
executor = WorkspaceExecutor(RegisteredProcessExecutor("/srv/mas/bin/mas-gateway"),
    container_runtime=DockerRuntime("/usr/bin/docker", socket_path="/var/run/docker.sock"))
```

The application executes as UID/GID 65532 with no supplementary groups or
capabilities. A trusted root process *inside the container* retains SETUID and
SETGID to create that application process, CHOWN to transfer work/output directory
ownership, and DAC_READ_SEARCH to read declared private outputs. It enforces an independent alarm
that the application cannot signal or change. The container has a read-only
image, private namespaces, no host mounts, no external network, no privileged
mode, no-new-privileges and Docker's seccomp policy. Memory/swap, CPU rate/time,
PID count, per-file size and bounded tmpfs bytes/inodes constrain resource use.
The tmpfs is `noexec`: interpreted source and image-installed executables are
supported; newly compiled native binaries in the workspace are not executable.
The runtime rejects missing cgroup-v2 resource controls and image-declared
volumes, and never pulls a missing image automatically.

Only authorised input/context bytes enter the container through a bounded binary
stream. They become root-owned read-only files under `/workspace/inputs` and
`/workspace/context.json`; request data is `/workspace/request.json`. The
application writes beneath `/workspace/work` and `/workspace/outputs`. Standard
output and error are captured as `outputs/stdout.txt` and `outputs/stderr.txt`.
Declared regular files return through a byte-bounded, hash-checked stream and
then use the existing artefact publication and provenance rules. Paths, symlinks,
hardlinks, special files, changed digests and oversized/truncated transfers are
rejected before usable output publication. No host credentials, daemon socket
or ambient Docker context enter the application container.

Each container execution remains one `context.invoke` allocation. The host
records its deterministic container name before creating it, retains the daemon's
exact container ID when received, and confirms removal before recording completed
output. A lost start receipt, deadline or failed cleanup leaves the invocation
unknown; a new host process cannot silently start that workspace again. The
in-image initial-input timeout and execution alarm also bound containers whose
attaching host dies. Operators reconcile the retained name against the same
configured daemon before releasing the workspace. The Docker daemon and Linux
kernel remain trusted dependencies; this profile makes no VM-level or
kernel-exploit resistance claim.

External-effect tools remain separate explicitly authorised, metered tool
registrations. The isolated profile rejects `allow_external_effects=True` and
provides no option to enable container networking.

Run the real containment gate against the exact retained image:

```sh
docker build -f Dockerfile.workspace -t mas-workspace:qualification .
image=$(docker image inspect --format '{{.Id}}' mas-workspace:qualification)
python scripts/check-container-workspace.py --image "$image" \
  --mas rust-protocol/target/release/mas \
  --gateway rust-protocol/target/release/mas-gateway
```

The gate uses real Rust admission and checks application identity/capabilities,
protected supervision, immutable inputs, denied host/socket/network access,
permitted tests, mode0600 output files within mode0700 directories, verified output
artefacts, replay and cleanup. The `workspace-containment.yml` CI workflow runs
this gate on relevant pushes and pull requests, including stacked branches,
against the immutable ID of the image it built. Missing Docker
fails the gate instead of producing a passing skip. Unit tests separately cover
policy mismatch, request-selected options, transfers, principal grants and lost
receipt recovery. Qualify daemon/kernel/runtime versions, timeout/host-death
faults and configured resource ceilings before advertising this profile.

The runtime controls follow the official [Docker container run reference](https://docs.docker.com/reference/cli/docker/container/run/),
[container security model](https://docs.docker.com/engine/security/) and
[seccomp guidance](https://docs.docker.com/engine/security/seccomp/).

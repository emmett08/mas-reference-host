# Composable topologies and durable workflows

MAS can combine named topologies, run independent workflow branches in bounded
parallel processes, and resume an interrupted composition using its original
identities. A new application supplies adapters, operation contracts and plan
configuration. It does not need new Rust message types or domain-specific core
changes.

The boundaries are explicit:

| Layer | Responsibility |
|---|---|
| Topology | Which actors may exchange messages within a Rust run. |
| Operation contract | Allowed callers and versioned input/output schemas. |
| Registered `Workflow` | Trusted adapters, entry operation, topology and implementation version. |
| `WorkflowPlan` | Sequence, conditional choice, parallel joins, bounded loops and reusable plans. |
| Acceptance policy | Whether a particular output satisfies a declared criterion for its inputs. |
| Artefact store | Bounded, integrity-checked file references and explicit recipient grants. |

The Rust codec, capability gateway, mailbox and task profile remain the
communication boundary. Python compiles topology configuration and schedules
registered workflows. Each leaf workflow owns a separate Rust run; its adapters
exchange requests and replies through `Runner`. Parent/child scheduling records
are local control state, not a new wire protocol.

## Run two offline applications

Run these commands from the repository root on Linux. They require no model SDK,
API key or network service. The examples use deterministic trusted adapters.

```sh
cargo build --release --locked --manifest-path .protocol/rust-protocol/Cargo.toml --target-dir "$PWD/rust-protocol/target"
export PYTHONPATH="$PWD/host${PYTHONPATH:+:$PYTHONPATH}"
export PATH="$PWD/rust-protocol/target/release:$PATH"

python -m mas_host workflow validate \
  --factory mas_host.workflow_examples:example_registry

python -m mas_host workflow run \
  --factory mas_host.workflow_examples:example_registry \
  --state-dir "$PWD/state/composable" --principal demo \
  --plan text-pipeline --request-key text-1 \
  --input examples/workflows/text-input.json

python -m mas_host workflow run \
  --factory mas_host.workflow_examples:example_registry \
  --state-dir "$PWD/state/composable" --principal demo \
  --plan arithmetic-evidence --request-key arithmetic-1 \
  --input examples/workflows/arithmetic-input.json
```

`text-pipeline` normalises whitespace, chooses whether to uppercase the text, then
computes a summary and reversal in parallel. Its result is:

```json
{"branches":{"reverse":{"text":"DLROW OLLEH"},"summary":{"characters":11,"text":"HELLO WORLD","words":2}}}
```

`arithmetic-evidence` invokes a reusable bounded-increment plan, then squares and
doubles its final value in parallel. The supplied input advances from zero to
three and returns square nine and double six. The loop permits at most five
increments; an unreachable target fails with `iteration_limit` in the loop node.

Both applications use the same composed star, mesh and tree configuration. Their
checker re-evaluates the deterministic function; this demonstrates an explicit
checking route, not independent mathematical assurance or model accuracy. The
arithmetic `checked` field is demonstration output, not a protocol guarantee.

The executable factory is
[`workflow_examples.py`](../host/mas_host/workflow_examples.py). Readable JSON
copies of its plans and topology, plus input files, are in
[`examples/workflows`](../examples/workflows). Tests check that these copies match
the executable definitions. Editing a copied plan alone does not alter the
example factory; a deployment factory can load its own JSON into `WorkflowPlan`.

## Topology composition

`compile_topology(spec)` in `mas_host.topology` returns the existing Rust
configuration with explicit sorted `agents` and directed `edges`. For example:

```json
{
  "format": "mas-topology/1",
  "groups": {
    "coordinate": {"kind": "star", "agents": ["hub", "worker"], "hub": "hub"},
    "review": {"kind": "mesh", "agents": ["a", "b", "c"]},
    "validate": {
      "kind": "tree", "agents": ["root", "left", "right"],
      "parents": {"left": "root", "right": "root"}
    }
  },
  "links": [
    ["coordinate/hub", "review/a"], ["review/a", "coordinate/hub"],
    ["coordinate/hub", "validate/root"], ["validate/root", "coordinate/hub"]
  ],
  "requests": [["coordinate/hub", "review/a"], ["coordinate/hub", "validate/root"]],
  "application": "agent-task/1",
  "max_calls": 20,
  "wall_seconds": 120
}
```

Groups qualify local actors as `group/actor`. `/` is reserved between these
components; the complete identity must satisfy Rust's 128-character ASCII limit.
The complete graph contains at most 32 actors. Group definitions can be reused
under different group names; their actors remain distinct.

| Group kind | Required fields beyond `kind` and `agents` | Edges generated |
|---|---|---|
| `star` | `hub` | Hub to every other member and the reverse routes. |
| `mesh` | None | Every distinct ordered pair inside that group. |
| `tree` | `parents`, mapping each non-root child to its parent | Parent/child pairs in both directions; exactly one root and no cycles. |
| `custom` | `edges`, an explicit list of local directed pairs | Exactly the declared pairs. |

`links` grant directed routes between groups. A `requests` declaration checks
that both the forward route and its return route already exist; it never adds a
missing permission. Duplicate, unknown, self and malformed routes are rejected.
The compiler does not infer links between groups or permit dynamic membership.

Flat configuration remains accepted by the compiler; its optional `name` and
`description` metadata are omitted from the effective Rust configuration, as in
the existing administrative CLI. Omitted flat `edges`
preserves the Rust Store's legacy all-registered-routes meaning by expanding it
explicitly. Explicit `null` is rejected. The generic Store permits a null
application profile, while the host's `mas init` path requires `agent-task/1`;
set that profile for executable host workflows. Existing run metadata is not
rewritten through the compiler during recovery.

`freeze_topology(spec)` retains an immutable effective configuration. Its
`configuration` property returns a fresh copy; `fingerprint` is SHA-256 over the
canonical effective configuration. The fingerprint includes budgets and routes,
and ignores declaration order. `inspect_topology(spec)` reports these values.

```sh
python -m mas_host workflow validate --topology examples/workflows/topology.json
python -m mas_host workflow graph --topology examples/workflows/topology.json
```

The second command prints Mermaid source for permitted directions. It describes
communication authority, not execution order.

## Plan semantics and bounds

A `WorkflowPlan` has exactly `format`, `name`, `version`, `limits` and `root`.
The format is `mas-workflow/1`. A plan contains bounded integer JSON; it cannot
import modules, evaluate expressions or execute shell text.

| Node | Data flow and completion |
|---|---|
| `{"kind":"task","workflow":"registered-leaf"}` | Executes the named registered `Workflow` with the current input object. |
| `{"kind":"sequence","steps":[...]}` | Passes each completed output object to the next step. |
| `{"kind":"parallel","branches":{"name":node,...}}` | Sends the same input to each branch; waits for all completed results and returns `{"branches":{"name":output,...}}`. |
| `{"kind":"choose","condition":...,"then":node,"else":node}` | Selects one branch using the current input. |
| `{"kind":"loop","body":node,"until":...,"max_iterations":5}` | Runs the body at least once; tests each output and passes it to the next iteration until matched. Exhaustion fails. |
| `{"kind":"subworkflow","workflow":"registered-plan"}` | Expands the referenced plan within the parent's durable instance, retaining child bounds as well as ancestor bounds. Recursive references are rejected. |

A condition is `{"path":["result",0,"accepted"],"equals":true}`. Strings select
object fields and integers select array positions. An empty path selects the
whole value. Missing or incompatible paths do not match. Equality is exact JSON
equality: a boolean does not equal an integer. There is no implicit conversion,
Python expression or substitution language. Add an explicit adapter for a data
transformation between steps.

Every plan declares these limits:

```json
{"max_steps":64,"max_parallel":2,"wall_seconds":180,"max_calls":20,"max_reserved_microusd":0}
```

`max_steps` bounds logical control nodes as well as leaf tasks, including the
worst-case loop and subworkflow expansion. `max_parallel` bounds concurrently
active leaf workers per instance. `wall_seconds` becomes an absolute deadline at
submission and does not restart on recovery. `max_calls` and
`max_reserved_microusd` bound admitted calls and conservative spending
reservations across the whole composition, above existing deployment and Rust
run limits. They do not estimate or refund actual external bills. Each
subworkflow invocation also has its own aggregate call, reservation and parallel
limits, charged alongside every ancestor. Its deadline is frozen at first entry
as the earlier of the parent's original deadline and first-entry time plus the
child's `wall_seconds`. Recovery cannot extend either deadline. The child's
worst-case step expansion is validated statically; the root's step limit is also
enforced against durable runtime nodes.

The engine uses separate Linux processes for independent leaves. Each leaf
retains the existing serial, nested `Runner` semantics within its Rust run.
The scheduler requires a single-threaded Linux process; do not construct it
inside a threaded web server. Each instance has one scheduling owner, with
durable identities based on logical paths rather than completion order. Several
instances require deployment-level capacity management in addition to their
individual `max_parallel` bounds.

## Submission, inspection and recovery

`WorkflowEngine(state_dir, plans, workflows, grants, ...)` accepts explicit
trusted registrations. The CLI loads these through `--factory module:callable`,
whose return value has exactly `plans`, `workflows` and `grants`. This factory is
trusted executable deployment code. Neither application input nor plan JSON can
select a factory, authenticated identity or Python import.

Use `submit` with the same arguments as a new `run` to persist work without
executing it. Reusing a principal's request key with identical input and binding
returns the existing instance; changing those values is a conflict. Subsequent
commands address the returned `instance_id`:

```sh
python -m mas_host workflow status \
  --factory mas_host.workflow_examples:example_registry \
  --state-dir "$PWD/state/composable" --principal demo --instance-id INSTANCE_ID

python -m mas_host workflow run \
  --factory mas_host.workflow_examples:example_registry \
  --state-dir "$PWD/state/composable" --principal demo --instance-id INSTANCE_ID
```

`status` returns the plan binding, deadline, state, output, allocation use,
logical node paths and leaf attempt states from one read snapshot. Graph queries
do not load omitted input or historical output payloads. `cancel` records
cancellation and stops further admission. Cancellation does not undo external effects. Unknown
effects take precedence over an apparently cancelled or failed result.
Once workers have drained, cancellation closes queued and paused latest leaf
revisions and their visible nodes. Earlier continuation revisions, completed
branch results and uncertain effects retain their evidence. A queued sibling
cannot start after cancellation, including when another branch remains unknown.

An `input_required` or `auth_required` leaf can be continued with `continue`,
`--path` from status, a new `--request-key` and an `--input` file. The registered
workflow must provide its continuation function; the new revision preserves
the original composition deadline and allocation. Continuation input does not
grant a principal new authority.

Use `task` to inspect one leaf's application output, including a question or
required-field description returned when it paused:

```sh
python -m mas_host workflow task \
  --factory my_application:registry \
  --state-dir "$PWD/state/composable" --principal owner \
  --instance-id INSTANCE_ID --path root/s1
```

The corresponding API is `engine.inspect_task(principal, instance_id, path,
revision=None)`. Omitting `--revision` selects the latest revision; supplying a
revision from 0 through 31 selects retained history. The returned revision identifies
the exact output being inspected. A historical pause does not imply that the
current workflow is still waiting for that input. Control nodes have no leaf
detail record; select a task path from `status`.

Leaf inspection checks instance ownership, the principal's current plan grant
and the registered workflow binding. It reads existing records without running
an adapter or admitting work, including in a read-only or restored audit-only
deployment. The graph view continues to omit leaf payloads. The detail response
exposes the selected leaf's output and error, but no raw input, provider
configuration or credentials held by the host. Application output can itself
contain private data; the host does not infer which application fields are
sensitive, so handle the returned output accordingly.

The response is bounded to 136 KiB of UTF-8, including the CLI envelope and
newline. Output and error each retain the existing 64 KiB integer-JSON limit
and nesting allowance; inspection framing does not reduce that allowance.

New submissions and continuations obey the deployment storage thresholds before
persisting input. Identical request replay, cancellation and evidenced recovery
remain available above those thresholds; a read-only or restored audit-only
deployment still rejects service mutations.

`recover` requeues uncertain work for the original assigned run after any
required invocation reconciliation; it does not create a replacement effect
identity. Run the instance again after recovery. An unresolved invocation still
blocks execution until the existing [operations procedure](operations.md)
records an evidenced decision. Changing an adapter, operation contract, provider
binding or plan requires an appropriate version/descriptor change; an existing
instance rejects a changed binding.

If both the scheduler and its worker die before recording an outcome, restart
inspects the assigned run's journal before applying cancellation or expiry.
Unresolved effects keep the composition `unknown` until evidenced resolution;
the original cancellation intent and deadline remain effective after recovery.

The loop recovery integration tests interrupt a later iteration inside a reusable
subworkflow alongside a completed parallel branch. They exercise scheduler
termination, uncertain effects, active cancellation, iteration-limit exhaustion
and recovery after a lost completion record. Independent effect receipts and
retained journal/protocol records check that recovery preserves completed work,
run identities, deadlines and shared allocations.

CLI JSON `ok:true` and exit zero mean the requested command succeeded. Inspect
`result.state` to distinguish completed work from a durable pause or failure.
The local `--principal` is asserted by the trusted launcher. It is not a network
authentication mechanism. Existing [MCP and A2A bridges](bridges.md) remain
separate authenticated boundaries.

## Operation contracts and arbitrary model roles

Install `host[workflows]` for the pinned JSON Schema dependency. A versioned
`OperationContract` declares input/output schemas and permitted callers; an
`OperationRegistry` is an actor adapter dispatching explicitly registered
handlers. Schema validation uses bounded integer JSON and local schema
references. No remote schema retrieval occurs.

```python
from mas_host.operation_contracts import OperationContract, OperationRegistry, RegisteredOperation

contract = OperationContract(
    "transform", "1",
    {"type": "object", "properties": {"text": {"type": "string"}},
     "required": ["text"], "additionalProperties": False},
    {"type": "object", "properties": {"text": {"type": "string"}},
     "required": ["text"], "additionalProperties": False},
    ["coordinate/hub"],
)
# YourAdapter implements the existing handle(context, request) interface.
registry = OperationRegistry("review/worker", [RegisteredOperation(contract, YourAdapter(), "1")])
```

The wire operation is `transform/1`. Route permission and operation permission
are both required. Attach `registry.descriptor` to the enclosing `Workflow`'s
`contract_descriptor` so a changed schema or caller list invalidates recovery
under an old binding.

`mas_host.agent_config` provides `AgentConfig`, `load_agent_configs` and
`create_agent_adapters` for arbitrary actor names and roles. Each actor declares
its provider, model, instructions, operation contract and call reservation.
Supported built-ins are explicit offline fixtures, OpenAI and restricted Cursor;
`StructuredProvider` is the extension interface. Example construction:

```python
from mas_host.agent_config import AgentConfig, create_agent_adapters

config = AgentConfig("review/worker", "mock", "fixture-v1", 0,
                     "Transform the supplied text.", contract, role="reviewer")
adapters = create_agent_adapters(
    {"review/worker": config},
    registered_actors={"coordinate/hub", "review/worker"},
    mock_outputs={"review/worker": {"text": "HELLO"}},
)
```

Register the caller's adapter separately. Add each structured adapter's
`descriptor()` to `Workflow.contract_descriptor`; it includes the instruction,
contract, model and fixture binding. Live providers use the existing credential
and reservation paths described in [providers](providers.md). The invoice
provider API and its planner/reviewer demonstration remain compatible.

Generic registration and descriptor inspection require no live credentials or
optional provider SDK. Credentials and runtime dependencies are resolved only
when the admitted model invocation executes. This also lets `status`, `task`,
`cancel` and `recover` load the same registered binding without provider secrets.

The runnable structured example combines both schemas with operation dispatch,
two simultaneous model actors, exact acceptance checks and admitted publication
of integrity-checked artefacts:

```bash
python -m mas_host workflow run \
  --factory mas_host.structured_examples:example_registry \
  --state-dir .mas-structured-demo --principal demo \
  --plan structured-evidence --request-key structured-1 \
  --input examples/workflows/structured-input.json
```

This defaults to explicit offline fixtures for `{"text":"hello","value":7}`.
Both results are accepted for that input. Other inputs can complete with a
**rejected** decision because the fixtures stay fixed; completion records that
the assessment finished. Each branch returns its result, acceptance decision
and an artefact reference. The branch's `run_directory` in `status` locates its
`artifacts` directory. Read access belongs to the recorded producer and the
authenticated principal mapped to `owner-<SHA256(principal UTF-8 bytes)>`.

To configure providers, copy
[`structured-agents.json`](../examples/workflows/structured-agents.json) and set
`MAS_STRUCTURED_CONFIG_FILE` to that copy. Select `openai` or `cursor`, an explicit
supported model and a positive `reservation_microusd` for each live actor; supply
the matching credential through the existing provider settings. Construction
does not submit inference; running the configured workflow makes the admitted
calls. Cursor uses the existing verified restricted runtime. OpenAI alone accepts
`max_output_tokens` (default 4096, maximum 16384); Cursor alone accepts
`cursor_transport`. The structured provider contract accepts bounded integer JSON
and object output schemas, with a 32,768-byte result limit. OpenAI additionally
requires schemas supported by its strict structured-output API.

## Acceptance and artefacts

Acceptance is a separate, versioned decision about a particular input/output
pair. It does not change the meaning of protocol delivery or task completion.

```python
from mas_host.acceptance_policies import AssessmentSubject, DeterministicPolicy

policy = DeterministicPolicy("double-check", "1", lambda source, result: result["n"] == 2 * source["n"])
decision = policy.evaluate(AssessmentSubject({"n": 3}, {"n": 6}))
assert decision.status == "accepted"
```

The decision binds both data identities and the policy version. Designated
reviewer and aggregation policies also return accepted, rejected or unresolved
decisions. The host must authenticate reviewer identities; a claim inside
model output cannot grant authority. Agreement is evidence under the configured
aggregation rule, not an independent correctness guarantee. Include policy
descriptors in the workflow binding when they govern its result.

For large inputs or results, use `ArtifactStore` rather than placing file bytes
inside protocol messages. References bind namespace, producer, input hashes,
media type, size, content hash and explicit readers. Link references before
handoff so live or unresolved work prevents pruning. See the complete
[artefact API, access and retention contract](artifacts.md).

## Candidate-bound agreement

An existing quorum join counts branch outputs satisfying its versioned
`acceptance.condition`. Opt in to candidate agreement when the reviewers must
judge the same candidate against the same context:

```json
{
  "mode": "quorum",
  "minimum": 2,
  "acceptance": {
    "id": "incident-review", "version": "2",
    "condition": {"path": ["accepted"], "equals": true}
  },
  "agreement": {
    "version": "mas-candidate-quorum/1",
    "candidate": {"path": ["candidate"], "type": "object"},
    "context": {"path": ["review_context"], "type": "object"}
  }
}
```

The expected candidate is an exact `{"id":"…","sha256":"…"}` record
reference. The context is a nonempty object containing the rubric, criteria,
evidence references and other versions required by the application. Both are
resolved from the parallel node's original input before branch allocation.
Each completed branch must return `accepted:true`, that exact `candidate` and
that exact `context`, in addition to satisfying the acceptance condition.
Missing, foreign, stale or malformed values contribute no vote. JSON booleans
and integers remain distinct.

Agreement branches must be direct registered tasks with distinct recipient
actors. Two workflow names targeting the same actor cannot contribute two
votes. A configuration binding identifies each trusted workflow and target;
workers cannot claim another voter identity in result JSON. Accepted winners
use the existing immutable join decision and cancellation/drain mechanism.
The result's `join.agreement` contains the resolved candidate, context,
minimum and `binding_sha256`; `join.selected` identifies the retained branches.
The journal also retains nonwinning task results. An old join without
`agreement` keeps its original source identity and predicate-count semantics.

Agreement is an application decision rule. Evaluator independence, factual
correctness and permission to execute remediation require their own contracts.

## Durable assessment and bounded revision in the Python host

Register `Runner(assessment_policies={actor: policy})` or the equivalent optional
`Workflow.assessment_policies` in trusted application code. The registration
uses the local SQLite journal. Shared PostgreSQL runners, task services and
composition services reject this profile before task/call admission; they never
silently omit its assessment policy. The registration contains:

```python
policy = {
    "format": "mas-assessment-policy/1",
    "evaluator": {"id": "reviewer", "version": "checker-1", "kind": "deterministic"},
    "rubric": rubric_record,
    "candidate": {"path": ["candidate"], "type": "object"},
    "evidence": {"path": ["evidence"], "type": "array"},
    "max_revisions": 1,
    "feedback_visibility": "full",
    "max_feedback": 4,
    "max_feedback_bytes": 12000,
}
```

The selectors nominate the complete candidate and evidence records from the
original request input. Before dispatching evaluator code, the parent
validates those records through the Rust `mas assessment -` command and retains
the frozen invocation. Evaluation authority comes from registration, including
the full evaluator version and rubric. It never comes from a worker's result.
An existing run with admitted requests cannot gain assessment authority during
recovery, and changing retained registration rejects recovery.

A registered evaluator must return `Outcome({"assessment": exact_reference})`
or include that `assessment` reference alongside its other output fields. The
parent resolves the retained Assessment and revalidates its complete frozen
context before permitting the terminal Result. Skipping assessment, fabricating
a reference or supplying a contradictory `accepted`/`passes` value fails the
task. The host derives `accepted`, `passes`, candidate, rubric and evidence
references, and marks the resulting output `profile:"mas-assessment-result/1"`.
This is a host receipt, distinct from standard `assessment.v1` carriage that
also requires the envelope's message and sender identities to match the record.
The final reference, task status and receipt are immutable in the run journal;
restart cannot replace a retained evaluator error with another final judgement.
A completed assessment cannot be returned with an unresolved outer task.

By default the receipt's `context` contains the exact rubric and evidence
references. An optional registered `context` selector can nominate a nonempty
object from the original input when an application also needs experiment or
criterion versions for agreement. The parent freezes that value under
`context.application`, alongside the authoritative `context.rubric` and
`context.evidence`, and rejects a worker-supplied replacement. Identical
application labels therefore cannot hide different rubrics or evidence when
forming candidate agreement.

Adapters call `context.assessment(operation, data)` through the same parent-owned
IPC boundary as other host operations:

| Operation | Data | Retained result |
|---|---|---|
| `candidate` | Original nominated candidate record | Exact reference; identical insertion is idempotent. |
| `evidence` | One originally nominated evidence record | Exact reference; worker-selected replacements are rejected. |
| `assess` | `assessment.v1` Assessment record | Contextually validated reference and `passes` judgement. |
| `reserve_revision` | `{"request_key":"revision-1","parent": exact_reference}` | Frozen revision input containing original input, parent, selected feedback, policy, original `deadline_ms` and revision ordinal. |
| `complete_revision` | `{"request_key":"revision-1","candidate": revised_record}` | Exact revised reference bound to that reservation's parent and feedback. |

The host seeds the original candidate/evidence automatically. It accepts
assessments only for that candidate or a completed reserved revision in the
same lineage. Rust validates rubric coverage, evaluator authority, every exact
candidate/rubric/evidence reference, error status and revision feedback links.
SQLite `BEGIN IMMEDIATE` protects validation-dependent reads, immutable history
insertion and revision allocation within the existing run journal. A conflicting
record under an existing identity is rejected; distinct assessment identities
preserve conflicting findings. Completed, inconclusive and evaluator-error
records use the same durable replay path.

`feedback_visibility:"withheld"` supplies no assessment records to revision.
`"full"` supplies all retained assessments for the selected parent within the
declared count and byte limits; exceeding a limit rejects the reservation rather
than silently dropping a finding. Identical reservation replay returns the exact
original context even when later assessments exist. A revised candidate must
reference exactly the disclosed feedback. Withheld-feedback resampling therefore
has no feedback links. The count limit cannot be reset by restarting the host.

Use `context.invoke(generate, input=frozen_revision_input, ...)` for the actual
generation effect, then `complete_revision` for its candidate. The existing
prepared/executing/unknown invocation journal, inherited budgets and original
run deadline still control execution. A lost provider response requires explicit
reconciliation; a revision reservation does not authorise another dispatch.
Feedback changes neither task routes nor call limits nor remediation permission.
Disclosure concerns host-assembled inputs under the existing trusted-worker
boundary; it does not isolate shared files or provider memory.

## Compatibility and evidence

These features leave the Rust wire format unchanged. Topology composition is
opt-in at configuration construction; old flat registrations and the existing
serial runner keep their interfaces. A new composition SQLite store records
plan instances and logical branch identities. Existing run journals remain
authoritative for their admitted calls and uncertain effects.

The offline tests cover topology admission against the real Rust CLI, malformed
routes and trees, exact plan conditions, bounded expansion, unrelated executable
applications, recovery and concurrency boundaries. Run the focused checks with:

```sh
python -m unittest discover -s host/tests -p 'test_topology.py' -v
python -m unittest discover -s host/tests -p 'test_workflow*.py' -v
python -m unittest discover -s host/tests -p 'test_operation_contracts.py' -v
python -m unittest discover -s host/tests -p 'test_acceptance_policies.py' -v
```

Verification establishes the properties exercised by those checks. It does not
establish application correctness, provider availability or a guarantee that
an interrupted external action did not occur. Existing formal protocol models
remain relevant to the unchanged Rust layer; composition has separate bounded
scheduling and recovery obligations.

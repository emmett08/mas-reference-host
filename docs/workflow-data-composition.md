# Data mapping and bounded collection workflows

Workflow plans support `transform`, `map`, `reduce`, `switch`, `while` and `recover` alongside
the existing tasks, sequences, parallel branches, choices, do-until loops and
subworkflows. Their configuration is part of the immutable source and transitive
binding identities. Existing plan sources retain their identities.

These nodes use declarative data selection. They cannot evaluate expressions,
import code or dispatch an unregistered workflow. Every potentially selected
branch is validated when the registry is bound.

## Typed selection and transformation

A selector is either a literal or an explicitly typed path:

```json
{"path": ["branches", "review", "accepted"], "type": "boolean"}
```

String path components select object properties and non-negative integer
components select array indices. Supported types are `object`, `array`, `string`,
`integer`, `boolean` and `null`. Booleans and integers remain distinct. A missing
path fails with `workflow_selection_missing`; a present value of another type
fails with `workflow_selection_type_mismatch`. An empty path selects the entire
input. `{"literal": {"approved": true}}` supplies a constant object.

A `transform` node produces an object from selected fields:

```json
{
  "kind": "transform",
  "fields": {
    "document": {"path": ["branches", "draft", "document"], "type": "object"},
    "accepted": {"path": ["branches", "review", "accepted"], "type": "boolean"},
    "version": {"literal": "reviewed/1"}
  }
}
```

An optional `schema` property declares the output's object JSON schema. It uses
the host's local Draft 2020-12 validation, with external references disabled.
Mapping, schema or size failure stops that branch before its next task is
allocated. Place transformations before or after tasks in a sequence to map
unrelated application contracts explicitly.

## Bounded fan-out

`map` applies a registered body to each item in an array:

```json
{
  "kind": "map",
  "items": {"path": ["documents"], "type": "array"},
  "max_items": 20,
  "max_input_bytes": 262144,
  "body": {
    "kind": "sequence",
    "steps": [
      {
        "kind": "transform",
        "fields": {
          "document": {"path": ["item"], "type": "object"},
          "policy": {"path": ["input", "policy"], "type": "object"}
        }
      },
      {"kind": "task", "workflow": "review-document"}
    ]
  }
}
```

Each body receives `{"input": original_input, "item": item, "index": index}`.
The example projects this envelope into the registered task's own contract.
Children have durable paths `m0`, `m1`, and so on. Results are returned as
`{"items": [result0, result1, ...]}` in input order, independently of completion
order. An empty collection completes with an empty `items` array.

The engine preflights both item count and the sum of encoded body-input sizes
before allocating any child. `max_items` is between 1 and 1,000;
`max_input_bytes` is between 1 and 1,048,576. Each individual body input and the
combined output also obey the existing 65,536-byte integer-JSON limit. Include
the repeated original input when choosing the expansion budget. Large shared
documents should be represented by artefact references.

Map bodies use the existing scheduler's `max_parallel`, scope limits, call
limits and reservation accounting. They do not create a separate unbounded
worker pool. Unknown effects take precedence over collection failure. The
evaluator continues observing every sibling if the combined output becomes too
large, retaining their individual results and any unknown effects.

## Sequential reduction

`reduce` folds the items into an object accumulator:

```json
{
  "kind": "reduce",
  "items": {"path": ["items"], "type": "array"},
  "initial": {"literal": {"approved": [], "rejected": []}},
  "max_items": 20,
  "max_input_bytes": 262144,
  "body": {"kind": "task", "workflow": "merge-review"}
}
```

The body receives the map envelope plus `accumulator`. Its completed output is
the next accumulator. The next iteration is allocated only after the previous
iteration completes. Empty input returns the initial object. Reduction returns
the final accumulator directly.

Reduction accounts for each actual body-input size, including the evolving
accumulator. A later budget violation fails before allocating that iteration;
previous completed effects remain retained. Recovery reconstructs the consumed
budget from those retained outputs, so restarting cannot renew the allowance.
Durable paths are `r0`, `r1`, and so on.

## Multiway routing and precondition loops

`switch` selects one named branch by exact JSON equality:

```json
{
  "kind": "switch",
  "select": {"path": ["category"], "type": "string"},
  "cases": [
    {"name": "research", "equals": "research",
     "body": {"kind": "task", "workflow": "research-review"}},
    {"name": "engineering", "equals": "engineering",
     "body": {"kind": "task", "workflow": "engineering-review"}}
  ],
  "default": {"kind": "task", "workflow": "general-review"}
}
```

Up to 32 cases are allowed. Duplicate case names or equal match values are
rejected. Unmatched values use `default`; missing or incorrectly typed values
fail selection. Only the selected branch receives an attempt. Its named path
remains stable on replay.

`while` checks its condition before each iteration:

```json
{
  "kind": "while",
  "condition": {"path": ["requires_revision"], "equals": true},
  "max_iterations": 5,
  "body": {"kind": "task", "workflow": "revise-and-check"}
}
```

A false initial condition completes without running the body. Each completed
body output becomes the next input. A condition that remains true after the
last allowed iteration fails with `iteration_limit`; it never creates another
attempt. The existing `loop` node retains its do-until behaviour and executes
the body at least once.

## Expansion and retained effects

Static `max_steps` includes each control node. Collection bodies are charged
their declared maximum item count, loops their maximum iterations, and routing
the most expensive branch. Transitive subworkflow expansion is included. Actual
execution may consume fewer steps but cannot exceed the bound.

Recovery bodies, optional compensation and failure handlers are all charged in
the maximum step count. Early joins charge every declared branch, including
branches that may be cancelled before dispatch. The existing call, reservation,
parallelism and elapsed-time budgets also cover recovery actions.

## Accepted-result joins

Omitting `join` preserves the existing `parallel` contract: every branch must
complete, and the result is `{"branches": {...}}`. An explicit join supplies a
versioned acceptance policy:

```json
{
  "kind": "parallel",
  "branches": {
    "review-a": {"kind": "task", "workflow": "review-a"},
    "review-b": {"kind": "task", "workflow": "review-b"},
    "review-c": {"kind": "task", "workflow": "review-c"}
  },
  "join": {
    "mode": "quorum",
    "minimum": 2,
    "acceptance": {
      "id": "accepted-review",
      "version": "1",
      "condition": {"path": ["accepted"], "equals": true}
    }
  }
}
```

`all` requires every completed branch output to match the condition. `quorum`
requires `minimum` matching outputs, between one and the declared branch count.
`first-accepted` requires one matching output. Only `quorum` accepts `minimum`.
Explicit joins allow at most 32 branches, bounding their durable sibling-path
disposition record even at the maximum nesting depth.
The acceptance identifier, version and exact condition are all included in the
immutable plan binding. Conditions are the same pure, exact JSON comparisons
used by routing; they do not import a predicate or grant execution authority.

Selection uses completed outputs observed by the scheduler, with branch-name
order breaking ties within one observation. It does not promise the earliest
physical completion across processes. The host durably records selected names
before cancelling obsolete scopes. Queued obsolete work is closed without
dispatch; running work receives cancellation and must drain. No obsolete
control node expands new children after the decision. A restarted scheduler
uses the retained selection even if another output has since arrived.
A candidate awaiting human input or authentication does not block other eligible
candidates; the join pauses only when no candidate can currently advance.

Completion waits for all already-dispatched obsolete work to drain. A stopped
effect with an unknown external outcome keeps the whole join `unknown`, even
when the acceptance threshold was satisfied. Resolve its retained invocation
journal before using `recover`; superseded resolved work is then closed without
redispatch. Cancellation is not evidence that an external effect did not occur.
If the threshold becomes unreachable, the host uses the same stop-and-drain
process before returning `workflow_acceptance_rejected`.

An explicit join returns selected outputs and its policy receipt:

```json
{
  "branches": {"review-a": {"accepted": true}, "review-c": {"accepted": true}},
  "join": {
    "mode": "quorum",
    "acceptance": {"id": "accepted-review", "version": "1",
                   "condition": {"path": ["accepted"], "equals": true}},
    "selected": ["review-a", "review-c"]
  }
}
```

Individual branch results remain inspectable even when excluded. The combined
receipt obeys the 65,536-byte integer-JSON limit; exceeding it fails the join
without repeating completed child effects.

## Known-failure handling and compensation

`recover` selects a declared failure handler only after a known failed body:

```json
{
  "kind": "recover",
  "body": {"kind": "task", "workflow": "reserve-stock"},
  "compensation": {"kind": "task", "workflow": "request-stock-release"},
  "on_failure": {"kind": "task", "workflow": "record-order-failure"}
}
```

`compensation` is optional and must be one registered task. The body's successful
output passes through directly. `unknown`, `input_required`, `auth_required`,
`expired` and `cancelled` do not automatically enter recovery. When the body
fails, a durable failure disposition first closes and drains the whole body
subtree. An unknown sibling blocks both compensation and the failure handler
until reconciliation establishes its outcome.

The compensation receives `{"input": original_input, "failure": {"state":
"failed", "path": "root/body"}}`, using the body's actual durable path. The
handler receives that envelope and, when configured, a `compensation` object
containing the registered workflow name and its completed output. The handler
can be any bounded plan node, with transformations available to match task
input contracts. Each envelope must fit the ordinary integer-JSON size bound
before its next child is allocated.

Compensation is a new admitted workflow action with its own durable invocation,
result, failure and uncertainty. It does not undo arbitrary prior effects or
imply transactional rollback. If compensation fails or pauses, the handler does
not run. Completed compensation is retained and is not repeated during replay.
The recovery result is `{"result": handler_output, "recovery": {"failure":
failure_descriptor, "compensation": compensation_receipt_or_null}}`; a failure
that was handled therefore remains visible in the result and node history.

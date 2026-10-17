"""Plan decision rules, independent of process scheduling and SQLite storage.

The evaluator selects paths and joins retained results through durable journal
operations. It never dispatches adapters, owns processes or grants admission.
"""
from __future__ import annotations

from collections.abc import Mapping
import time
from typing import Protocol

from .codec import JSON_BYTES, canonical_json, integer_json
from .contracts import RunnerError
from .service_contracts import ServiceError
from .workflow_plan import (WorkflowPlan, evaluate_condition,
                            select_value, transform_value)


FINISHED_STATES = frozenset({'completed', 'failed', 'cancelled', 'expired'})
PAUSED_STATES = frozenset({'unknown', 'input_required', 'auth_required'})


class PlanJournal(Protocol):
    """Durable operations required by the plan decision algorithm."""
    def node(self, instance: str, path: str, kind: str, input: dict, limits: dict) -> dict: ...
    def attempt(self, instance: str, path: str, workflow: str, input: dict,
                 deadline: float) -> dict | None: ...
    def finish_node(self, instance: str, path: str, state: str,
                     output: dict | None = None, error: dict | None = None) -> tuple[str, dict | None]: ...
    def scope(self, instance: str, path: str, limits: dict, deadline: float, *,
              timing_policy: dict | None = None) -> float: ...
    def join_decision(self, instance: str, path: str) -> dict | None: ...
    def decide_join(self, instance: str, path: str, winners: list[str],
                    obsolete: list[str]) -> dict: ...
    def join_drain_state(self, instance: str, path: str) -> str: ...


class PlanEvaluator:
    """Advance registered control nodes and collect ready leaf assignments."""
    def __init__(self, plans: Mapping[str, WorkflowPlan], journal: PlanJournal):
        self.plans = plans
        self.journal = journal

    def _progress(self, instance, path, kind, initial):
        # The optional capability keeps third-party decision-only journals
        # compatible; durable hosts implement it through their public store.
        read = getattr(self.journal, 'progress', None)
        saved = read(instance, path, kind) if callable(read) else None
        return saved or {'next_index': 0, 'output': initial, 'consumed': 0}

    def _checkpoint(self, instance, path, kind, index, output, consumed=0):
        save = getattr(self.journal, 'checkpoint', None)
        if callable(save):
            save(instance, path, kind, index, output, consumed)

    @staticmethod
    def _collection_items(node, input):
        items = select_value(node['items'], input)
        if len(items) > node['max_items']:
            raise ServiceError('workflow_collection_item_limit')
        return items

    @staticmethod
    def _collection_input(input, item, index, consumed, limit, **extra):
        supplied = {'input': input, 'item': item, 'index': index, **extra}
        try:
            consumed += len(integer_json(supplied).encode('utf-8'))
        except RunnerError:
            raise ServiceError('workflow_collection_input_limit') from None
        if consumed > limit:
            raise ServiceError('workflow_collection_input_limit')
        return supplied, consumed

    def _map(self, instance, path, node, input, limits, deadline, ready):
        # Preflight all expanded inputs before allocating even the first child.
        # The list itself is bounded by the declared count and aggregate bytes.
        consumed, inputs = 0, []
        try:
            for index, item in enumerate(self._collection_items(node, input)):
                supplied, consumed = self._collection_input(
                    input, item, index, consumed, node['max_input_bytes'])
                inputs.append(supplied)
        except ServiceError as error:
            return self.journal.finish_node(instance, path, 'failed', error={'code': error.code})
        states, outputs, output_bytes, oversized = set(), [], len('{"items":[]}'), False
        for index, supplied in enumerate(inputs):
            state, output = self.advance(instance, f'{path}/m{index}', node['body'], supplied,
                                         limits, deadline, ready)
            states.add(state)
            if state == 'completed' and not oversized:
                try:
                    output_bytes += len(integer_json(output).encode('utf-8')) + bool(outputs)
                    oversized = output_bytes > JSON_BYTES
                except RunnerError:
                    oversized = True
                if not oversized:
                    outputs.append(output)
            # Continue observing every sibling when the combined result is too
            # large; an output-size failure must never conceal an unknown effect.
        if not states or states == {'completed'}:
            if oversized:
                return self.journal.finish_node(instance, path, 'failed',
                                                error={'code': 'composed_output_not_encodable'})
            return self.journal.finish_node(instance, path, 'completed',
                                           {'items': outputs})
        state = next((state for state in ('unknown', 'input_required', 'auth_required',
                      'failed', 'expired', 'cancelled') if state in states), 'waiting')
        return self.journal.finish_node(instance, path, state)

    def _reduce(self, instance, path, node, input, limits, deadline, ready):
        try:
            items = self._collection_items(node, input)
            accumulator = select_value(node['initial'], input)
        except ServiceError as error:
            return self.journal.finish_node(instance, path, 'failed', error={'code': error.code})
        saved = self._progress(instance, path, 'reduce', accumulator)
        consumed, accumulator = saved['consumed'], saved['output']
        for index in range(saved['next_index'], len(items)):
            item = items[index]
            # The prefix checkpoint preserves the same cumulative byte budget.
            try:
                supplied, consumed = self._collection_input(input, item, index, consumed,
                    node['max_input_bytes'], accumulator=accumulator)
            except ServiceError as error:
                return self.journal.finish_node(instance, path, 'failed', error={'code': error.code})
            state, accumulator = self.advance(instance, f'{path}/r{index}', node['body'],
                                               supplied, limits, deadline, ready)
            if state != 'completed':
                return self.journal.finish_node(instance, path, state)
            self._checkpoint(instance, path, 'reduce', index + 1, accumulator, consumed)
        return self.journal.finish_node(instance, path, 'completed', accumulator)

    @staticmethod
    def _discard_ready(ready, prefixes):
        ready[:] = [attempt for attempt in ready if not any(
            attempt['path'] == prefix or attempt['path'].startswith(prefix + '/')
            for prefix in prefixes)]

    def _finish_bounded(self, instance, path, output):
        try:
            integer_json(output)
        except RunnerError:
            return self.journal.finish_node(instance, path, 'failed',
                                            error={'code': 'composed_output_not_encodable'})
        return self.journal.finish_node(instance, path, 'completed', output)

    def _parallel_join(self, instance, path, node, input, limits, deadline, ready):
        policy, branches = node['join'], node['branches']
        minimum = (len(branches) if policy['mode'] == 'all' else
                   policy['minimum'] if policy['mode'] == 'quorum' else 1)
        agreement = None
        if 'agreement' in policy:
            from .workflow_agreement import agreement_binding, agrees
            try:
                agreement = agreement_binding(policy['agreement'], input, minimum)
            except ServiceError as error:
                return self.journal.finish_node(instance, path, 'failed', error={'code': error.code})
        decision = self.journal.join_decision(instance, path)
        if decision is None:
            accepted, states = [], []
            for name, child in sorted(branches.items()):
                state, output = self.advance(instance, f'{path}/p-{name}', child,
                                             input, limits, deadline, ready)
                states.append(state)
                if (state == 'completed' and evaluate_condition(policy['acceptance']['condition'], output)
                        and (agreement is None or agrees(output, agreement))):
                    accepted.append(name)
                    if len(accepted) == minimum:
                        break
            if len(accepted) == minimum:
                obsolete = [f'{path}/p-{name}' for name in sorted(branches) if name not in accepted]
                decision = self.journal.decide_join(instance, path, accepted, obsolete)
            elif len(accepted) + sum(state not in FINISHED_STATES for state in states) < minimum:
                # The predicate cannot now reach its threshold. Stop and drain
                # every outstanding branch before exposing a known failure.
                decision = self.journal.decide_join(instance, path, [],
                    [f'{path}/p-{name}' for name in sorted(branches)])
            else:
                # A paused candidate does not prevent another eligible branch
                # from satisfying the threshold. Uncertain effects still close
                # global dispatch until they are reconciled.
                state = ('unknown' if 'unknown' in states else 'waiting'
                         if any(value in {'queued', 'waiting', 'running'} for value in states)
                         else next((value for value in ('input_required', 'auth_required')
                                    if value in states), 'waiting'))
                return self.journal.finish_node(instance, path, state)
        self._discard_ready(ready, decision['obsolete'])
        drain = self.journal.join_drain_state(instance, path)
        if drain != 'drained':
            return self.journal.finish_node(instance, path, drain)
        if not decision['winners']:
            return self.journal.finish_node(instance, path, 'failed',
                                            error={'code': 'workflow_acceptance_rejected'})
        output = {'branches': {}, 'join': {'mode': policy['mode'],
                  'acceptance': policy['acceptance'], 'selected': decision['winners']}}
        if agreement is not None:
            output['join']['agreement'] = agreement
        # Read only retained winners after the immutable decision. Obsolete
        # controls must never expand a new child during replay or recovery.
        for name in decision['winners']:
            state, value = self.advance(instance, f'{path}/p-{name}', branches[name],
                                        input, limits, deadline, ready)
            if state != 'completed':
                raise ServiceError('workflow_join_winner_unavailable')
            output['branches'][name] = value
            try:
                integer_json(output)
            except RunnerError:
                return self.journal.finish_node(instance, path, 'failed',
                                                error={'code': 'composed_output_not_encodable'})
        return self._finish_bounded(instance, path, output)

    def _recover(self, instance, path, node, input, limits, deadline, ready):
        decision_path, body_path = f'{path}/failure', f'{path}/body'
        decision = self.journal.join_decision(instance, decision_path)
        if decision is None:
            state, output = self.advance(instance, body_path, node['body'], input,
                                         limits, deadline, ready)
            if state != 'failed':
                return self.journal.finish_node(instance, path, state, output)
            decision = self.journal.decide_join(instance, decision_path, [], [body_path])
        self._discard_ready(ready, decision['obsolete'])
        drain = self.journal.join_drain_state(instance, decision_path)
        if drain != 'drained':
            return self.journal.finish_node(instance, path, drain)
        failure = {'state': 'failed', 'path': body_path}
        supplied = {'input': input, 'failure': failure}
        compensation = None
        try:
            integer_json(supplied)
        except RunnerError:
            return self.journal.finish_node(instance, path, 'failed',
                                            error={'code': 'workflow_recovery_input_limit'})
        if 'compensation' in node:
            state, output = self.advance(instance, f'{path}/compensate', node['compensation'],
                                         supplied, limits, deadline, ready)
            if state != 'completed':
                return self.journal.finish_node(instance, path, state)
            compensation = {'workflow': node['compensation']['workflow'], 'output': output}
            supplied['compensation'] = compensation
            try:
                integer_json(supplied)
            except RunnerError:
                return self.journal.finish_node(instance, path, 'failed',
                                                error={'code': 'workflow_recovery_input_limit'})
        state, output = self.advance(instance, f'{path}/on_failure', node['on_failure'],
                                     supplied, limits, deadline, ready)
        if state != 'completed':
            return self.journal.finish_node(instance, path, state)
        return self._finish_bounded(instance, path, {'result': output,
            'recovery': {'failure': failure, 'compensation': compensation}})

    def advance(self, instance, path, node, input, limits, deadline, ready):
        kind = node['kind']
        saved = self.journal.node(instance, path, kind, input, limits)
        if saved['state'] == 'completed' or kind == 'task' and saved['state'] in FINISHED_STATES:
            return saved['state'], saved['output']
        if kind == 'task':
            attempt = self.journal.attempt(instance, path, node['workflow'], input, deadline)
            if attempt is None:
                return self.journal.finish_node(instance, path, 'expired')
            state = attempt['state']
            if state == 'queued':
                ready.append(attempt)
            if state in FINISHED_STATES | PAUSED_STATES:
                return self.journal.finish_node(instance, path, state, attempt['output'], attempt['error'])
            return state, None
        if kind == 'transform':
            try:
                output = transform_value(node, input)
            except ServiceError as error:
                return self.journal.finish_node(instance, path, 'failed', error={'code': error.code})
            return self.journal.finish_node(instance, path, 'completed', output)
        if kind == 'map':
            return self._map(instance, path, node, input, limits, deadline, ready)
        if kind == 'reduce':
            return self._reduce(instance, path, node, input, limits, deadline, ready)
        if kind == 'recover':
            return self._recover(instance, path, node, input, limits, deadline, ready)
        if kind == 'sequence':
            saved = self._progress(instance, path, kind, input)
            output = saved['output']
            for index in range(saved['next_index'], len(node['steps'])):
                state, output = self.advance(instance, f'{path}/s{index}', node['steps'][index], output, limits, deadline, ready)
                if state != 'completed':
                    return self.journal.finish_node(instance, path, state)
                self._checkpoint(instance, path, kind, index + 1, output)
            return self.journal.finish_node(instance, path, 'completed', output)
        if kind == 'parallel':
            if 'join' in node:
                return self._parallel_join(instance, path, node, input, limits, deadline, ready)
            results = {name: self.advance(instance, f'{path}/p-{name}', child, input, limits, deadline, ready)
                       for name, child in sorted(node['branches'].items())}
            states = [value[0] for value in results.values()]
            if all(state == 'completed' for state in states):
                return self.journal.finish_node(instance, path, 'completed', {'branches': {name: value[1] for name, value in results.items()}})
            # Every active branch is drained before finalisation. Unknown takes
            # precedence over failure/cancellation and blocks fresh dispatch.
            state = next((s for s in ('unknown', 'input_required', 'auth_required', 'failed', 'expired', 'cancelled') if s in states), 'waiting')
            return self.journal.finish_node(instance, path, state)
        if kind == 'choose':
            branch = 'then' if evaluate_condition(node['condition'], input) else 'else'
            state, output = self.advance(instance, f'{path}/{branch}', node[branch], input, limits, deadline, ready)
        elif kind == 'switch':
            try:
                selected = canonical_json(select_value(node['select'], input))
            except ServiceError as error:
                return self.journal.finish_node(instance, path, 'failed', error={'code': error.code})
            branch, child = 'default', node['default']
            for case in node['cases']:
                if canonical_json(case['equals']) == selected:
                    branch, child = f'case-{case["name"]}', case['body']
                    break
            state, output = self.advance(instance, f'{path}/{branch}', child, input, limits, deadline, ready)
        elif kind == 'while':
            saved = self._progress(instance, path, kind, input)
            output = saved['output']
            for index in range(saved['next_index'], node['max_iterations']):
                if not evaluate_condition(node['condition'], output):
                    return self.journal.finish_node(instance, path, 'completed', output)
                state, output = self.advance(instance, f'{path}/i{index}', node['body'],
                                               output, limits, deadline, ready)
                if state != 'completed':
                    break
                self._checkpoint(instance, path, kind, index + 1, output)
            else:
                if evaluate_condition(node['condition'], output):
                    return self.journal.finish_node(instance, path, 'failed',
                                                     error={'code': 'iteration_limit'})
                return self.journal.finish_node(instance, path, 'completed', output)
        elif kind == 'loop':
            saved = self._progress(instance, path, kind, input)
            output = saved['output']
            if saved['next_index'] and evaluate_condition(node['until'], output):
                return self.journal.finish_node(instance, path, 'completed', output)
            for index in range(saved['next_index'], node['max_iterations']):
                state, output = self.advance(instance, f'{path}/i{index}', node['body'], output, limits, deadline, ready)
                if state != 'completed':
                    break
                self._checkpoint(instance, path, kind, index + 1, output)
                if evaluate_condition(node['until'], output):
                    break
            else:
                return self.journal.finish_node(instance, path, 'failed', error={'code': 'iteration_limit'})
        elif kind == 'subworkflow':
            child = self.plans[node['workflow']].spec
            from .operation_contracts import ContractError, JsonObjectSchema
            if 'input_schema' in child:
                try:
                    JsonObjectSchema(child['input_schema']).validate(input)
                except ContractError:
                    return self.journal.finish_node(instance, path, 'failed', error={'code': 'workflow_input_schema_mismatch'})
            child_path = f'{path}/sub-{node["workflow"]}'
            timing = {'timing_policy': child['timing_policy']} if 'timing_policy' in child else {}
            child_deadline = self.journal.scope(instance, child_path, child['limits'], deadline, **timing)
            if child_deadline <= time.time():
                return self.journal.finish_node(instance, path, 'expired')
            state, output = self.advance(instance, child_path, child['root'], input, limits, child_deadline, ready)
            if state == 'completed' and 'output_schema' in child:
                try:
                    JsonObjectSchema(child['output_schema']).validate(output)
                except ContractError:
                    return self.journal.finish_node(instance, path, 'failed', error={'code': 'workflow_output_schema_mismatch'})
        else:
            raise ServiceError('invalid_node')
        return self.journal.finish_node(instance, path, state, output)

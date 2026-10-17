"""Owned task details expose durable pauses without writing or broadening scope."""
from contextlib import closing, redirect_stderr, redirect_stdout
from dataclasses import replace
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from mas_host.codec import JSON_BYTES, integer_json
from mas_host.contracts import Outcome
from mas_host.operations import DeploymentLedger, backup_state, restore_state
from mas_host.service_contracts import ServiceError
from mas_host.workflow_cli import main
from mas_host.workflow_engine import WorkflowEngine
from mas_host.workflow_inspection import TASK_INSPECTION_BYTES, TASK_INSPECTION_FIELDS, encode_task_inspection
from mas_host.workflow_plan import WorkflowPlan
from test_runner import FunctionAdapter
from test_workflow_engine_contracts import specification, workflow
import test_runner as runner_tests


def inspection_registry():
    def respond(_context, request):
        if request.input.get('raise_error'):
            raise RuntimeError('PRIVATE-EXCEPTION-TOKEN')
        return Outcome({'state': request.input.get('next_state', 'input_required'),
                        'question': 'Supply the next decision.', 'answer': request.input.get('answer')})
    adapter = FunctionAdapter(respond)
    leaf = workflow(adapters={'a': adapter, 'b': adapter},
                    classify=lambda outcome: outcome.output['state'],
                    continue_input=lambda previous, supplied: supplied,
                    contract_descriptor={'provider_configuration': 'PRIVATE-CONFIG-TOKEN'})
    plan = specification()
    plan['root'] = {'kind': 'sequence', 'steps': [plan['root']]}
    return {'plans': {'example': WorkflowPlan(plan)}, 'workflows': {'echo': leaf},
            'grants': {'alice': {'example'}, 'bob': {'example'}}}


def snapshot(directory):
    return {str(path.relative_to(directory)): (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
            for path in directory.rglob('*') if path.is_file()}


class WorkflowInspectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.state = self.directory / 'state'
        self.registry = inspection_registry()
        self.options = {'mas_bin': runner_tests.RunnerIntegrationTests.mas,
                        'gateway_bin': runner_tests.RunnerIntegrationTests.gateway}
        self.engine = WorkflowEngine(self.state, **self.registry, **self.options)

    def run_task(self, **input):
        instance = self.engine.submit('alice', 'request', 'example', input)['instance_id']
        return instance, self.engine.run('alice', instance)

    def detail(self, instance, revision=None):
        return self.engine.inspect_task('alice', instance, 'root/s0', revision)

    def test_pure_pauses_expose_output_without_graph_expansion_or_input_disclosure(self):
        instance, graph = self.run_task(private_input='PRIVATE-INPUT-TOKEN')
        self.assertEqual(graph['state'], 'input_required')
        self.assertIsNone(graph['output'])
        before = snapshot(self.state)
        detail = self.detail(instance)
        self.assertEqual(set(detail), TASK_INSPECTION_FIELDS)
        self.assertEqual(detail['output']['question'], 'Supply the next decision.')
        self.assertEqual(detail['state'], 'input_required')
        self.assertEqual(detail['revision'], 0)
        self.assertEqual(detail['workflow'], 'echo')
        self.assertEqual(detail['identity'], self.registry['workflows']['echo'].identity)
        self.assertEqual(detail['deadline'], graph['deadline'])
        self.assertEqual(self.engine.inspect('alice', instance), graph)
        self.assertEqual(snapshot(self.state), before)
        self.assertNotIn('PRIVATE-', json.dumps(detail))

    def test_latest_and_historical_revisions_keep_each_pause_and_result(self):
        instance, _ = self.run_task()
        first = self.detail(instance)
        self.engine.continue_task('alice', instance, 'root/s0', 'authorise', {'next_state': 'auth_required'})
        queued = self.detail(instance)
        self.assertEqual((queued['revision'], queued['state'], queued['output']), (1, 'queued', None))
        self.engine.run('alice', instance)
        second = self.detail(instance)
        self.assertEqual((second['revision'], second['state']), (1, 'auth_required'))
        self.engine.continue_task('alice', instance, 'root/s0', 'complete', {'next_state': 'completed', 'answer': 7})
        self.engine.run('alice', instance)
        latest = self.detail(instance)
        self.assertEqual((latest['revision'], latest['state'], latest['output']['answer']), (2, 'completed', 7))
        self.assertEqual(self.detail(instance, 0)['output'], first['output'])
        self.assertEqual(self.detail(instance, 1)['output'], second['output'])
        self.assertEqual(len({first['run_directory'], second['run_directory'], latest['run_directory']}), 3)

    def test_ownership_grants_and_binding_drift_are_rechecked(self):
        instance, _ = self.run_task()
        with self.assertRaises(ServiceError) as caught:
            self.engine.inspect_task('bob', instance, 'root/s0')
        self.assertEqual(caught.exception.code, 'workflow_not_found')
        self.engine.grants['alice'] = frozenset()
        with self.assertRaises(ServiceError) as caught:
            self.detail(instance)
        self.assertEqual(caught.exception.code, 'forbidden')
        self.engine.grants['alice'] = frozenset({'example'})
        retained = self.detail(instance)
        self.engine.workflows['echo'] = replace(self.engine.workflows['echo'], version='changed')
        self.assertEqual(self.detail(instance), retained,
                         'retained authorised history does not require executable adapter availability')

    def test_missing_controls_unexecuted_tasks_and_revisions_have_fixed_errors(self):
        instance = self.engine.submit('alice', 'request', 'example', {})['instance_id']
        with self.assertRaises(ServiceError) as caught:
            self.detail(instance)
        self.assertEqual(caught.exception.code, 'workflow_task_not_found')
        self.engine.run('alice', instance)
        for path, revision in [('root', None), ('root/s9', None), ('root/s0', 1)]:
            with self.subTest(path=path, revision=revision), self.assertRaises(ServiceError) as caught:
                self.engine.inspect_task('alice', instance, path, revision)
            self.assertEqual(caught.exception.code, 'workflow_task_not_found')

    def test_invalid_selectors_are_fixed_errors_and_never_filesystem_paths(self):
        instance, _ = self.run_task()
        before = snapshot(self.directory)
        for path in (None, True, 3, [], '', '../outside', '/tmp/outside', 'root/..', 'root//s0',
                     'root/é', 'root\x00', 'root/' + 'a' * 4096):
            with self.subTest(path=path), self.assertRaises(ServiceError) as caught:
                self.engine.inspect_task('alice', instance, path)
            self.assertEqual(caught.exception.code, 'invalid_task_path')
        for revision in (True, False, -1, 32, '0', 0.0, []):
            with self.subTest(revision=revision), self.assertRaises(ServiceError) as caught:
                self.detail(instance, revision)
            self.assertEqual(caught.exception.code, 'invalid_task_revision')
        self.assertEqual(snapshot(self.directory), before)

    def test_exception_details_remain_sanitised(self):
        instance, result = self.run_task(raise_error=True)
        self.assertEqual(result['state'], 'failed')
        detail = self.detail(instance)
        self.assertEqual(detail['error'], {'code': 'workflow_failed'})
        self.assertNotIn('PRIVATE-EXCEPTION-TOKEN', encode_task_inspection(detail))

    def test_readonly_ledger_and_audit_restore_have_no_file_or_event_mutation(self):
        instance, _ = self.run_task()
        bundle, restored = self.directory / 'backup', self.directory / 'restored'
        backup_state(self.state, bundle)
        restore_state(bundle, restored)
        for state in (self.state, restored):
            with self.subTest(state=state):
                before = snapshot(state)
                engine = WorkflowEngine(state, **self.registry, **self.options,
                                        operations=DeploymentLedger(state, readonly=True))
                self.assertTrue(engine.store.readonly)
                self.assertEqual(engine.inspect_task('alice', instance, 'root/s0')['state'], 'input_required')
                self.assertEqual(snapshot(state), before)

    def test_parent_and_leaf_are_read_from_one_sqlite_snapshot(self):
        instance, _ = self.run_task()
        with closing(sqlite3.connect(self.state / 'composition.sqlite')) as db, db:
            db.execute('PRAGMA journal_mode=WAL')
        owned = self.engine._owned
        def change_after_authorisation(db, principal, identity):
            item = owned(db, principal, identity)
            self.assertTrue(db.in_transaction)
            with closing(sqlite3.connect(self.state / 'composition.sqlite')) as writer, writer:
                writer.execute("UPDATE instances SET state='auth_required' WHERE id=?", (instance,))
                writer.execute("UPDATE attempts SET state='auth_required',output=? WHERE instance=?",
                               (json.dumps({'changed': True}), instance))
            return item
        with patch.object(self.engine, '_owned', side_effect=change_after_authorisation):
            detail = self.detail(instance)
        self.assertEqual((detail['instance_state'], detail['state']), ('input_required', 'input_required'))
        self.assertIn('question', detail['output'])
        self.assertEqual(self.detail(instance)['output'], {'changed': True})

    def test_graph_does_not_mix_parent_pause_with_new_continuation(self):
        instance, before = self.run_task()
        with closing(sqlite3.connect(self.state / 'composition.sqlite')) as db, db:
            db.execute('PRAGMA journal_mode=WAL')
        writer = WorkflowEngine(self.state, **self.registry, **self.options)
        owned = self.engine._owned

        def continue_after_authorisation(db, principal, identity):
            item = owned(db, principal, identity)
            writer.continue_task('alice', instance, 'root/s0', 'concurrent-decision',
                                 {'next_state': 'completed', 'answer': 7})
            return item

        with patch.object(self.engine, '_owned', side_effect=continue_after_authorisation):
            observed = self.engine.inspect('alice', instance)
        self.assertEqual(observed, before)
        latest = self.engine.inspect('alice', instance)
        self.assertEqual(latest['state'], 'queued')
        self.assertEqual([(item['revision'], item['state']) for item in latest['attempts']],
                         [(0, 'input_required'), (1, 'queued')])

    def test_graph_can_inspect_state_without_decoding_private_payloads(self):
        instance, before = self.run_task()
        # Damage only payloads excluded from the graph. State/ownership and
        # history remain inspectable without loading or decoding those bytes.
        with self.engine.store.transaction() as db:
            db.execute('UPDATE instances SET input=? WHERE id=?', ('invalid JSON', instance))
            for table in ('nodes', 'attempts'):
                db.execute(f'UPDATE {table} SET input=?,output=? WHERE instance=?',
                           ('invalid JSON', 'invalid JSON', instance))
        self.assertEqual(self.engine.inspect('alice', instance), before)

    def test_full_size_payloads_and_max_depth_survive_inspection_framing(self):
        instance, _ = self.run_task()
        output = {'payload': 'é' * ((JSON_BYTES - len('{"payload":""}')) // 2)}
        error = {'code': 'x' * (JSON_BYTES - len('{"code":""}'))}
        self.assertEqual(len(integer_json(output).encode()), JSON_BYTES)
        self.assertEqual(len(integer_json(error).encode()), JSON_BYTES)
        with self.engine.store.transaction() as db:
            db.execute('UPDATE attempts SET output=?,error=? WHERE instance=?',
                       (integer_json(output), integer_json(error), instance))
        detail = self.detail(instance)
        source = encode_task_inspection(detail)
        self.assertLessEqual(len(source.encode('utf-8')), TASK_INSPECTION_BYTES)
        self.assertEqual(json.loads(source)['result']['output'], output)
        nested = 0
        for _ in range(32):
            nested = {'n': nested}
        integer_json(nested)
        detail['output'] = nested
        self.assertEqual(json.loads(encode_task_inspection(detail))['result']['output'], nested)
        detail['output'] = {'payload': 'x' * JSON_BYTES}
        with self.assertRaises(ServiceError) as caught:
            encode_task_inspection(detail)
        self.assertEqual(caught.exception.code, 'task_inspection_too_large')

    def test_real_cli_returns_current_and_historical_detail_without_writes(self):
        instance, _ = self.run_task()
        self.engine.continue_task('alice', instance, 'root/s0', 'next', {'next_state': 'auth_required'})
        self.engine.run('alice', instance)
        # Historical application data may fill both independent payload bounds.
        # The actual CLI must retain UTF-8 instead of expanding non-ASCII text.
        output = {'payload': 'é' * ((JSON_BYTES - len('{"payload":""}')) // 2)}
        error = {'code': 'x' * (JSON_BYTES - len('{"code":""}'))}
        with self.engine.store.transaction() as db:
            db.execute('UPDATE attempts SET output=?,error=? WHERE instance=? AND revision=0',
                       (integer_json(output), integer_json(error), instance))
        repo = Path(__file__).resolve().parents[2]
        environment = dict(os.environ, PYTHONPATH=os.pathsep.join((str(repo / 'host'), str(repo / 'host/tests'))))
        command = [sys.executable, '-m', 'mas_host', 'workflow', 'task',
                   '--factory', 'test_workflow_inspection:inspection_registry', '--state-dir', str(self.state),
                   '--principal', 'alice', '--instance-id', instance, '--path', 'root/s0']
        before = snapshot(self.state)
        for extra, revision in (([], 1), (['--revision', '0'], 0)):
            result = subprocess.run(command + extra, env=environment, cwd=self.directory,
                                    text=True, capture_output=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertLessEqual(len(result.stdout.encode('utf-8')), TASK_INSPECTION_BYTES)
            self.assertEqual(json.loads(result.stdout)['result'], self.detail(instance, revision))
        self.assertEqual(snapshot(self.state), before)

    def test_cli_does_not_initialise_missing_state_and_rejects_selector_before_factory(self):
        missing = self.directory / 'missing'
        common = ['task', '--factory', 'test_workflow_inspection:inspection_registry', '--state-dir', str(missing),
                  '--principal', 'alice', '--instance-id', 'flow-' + '0' * 32]
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(main(common + ['--path', 'root/s0']), 1)
        self.assertFalse(missing.exists())
        with patch('mas_host.workflow_cli.load_registry') as load:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                self.assertEqual(main(common + ['--path', 'root/s0', '--revision', '-1']), 1)
            load.assert_not_called()


if __name__ == '__main__':
    unittest.main()

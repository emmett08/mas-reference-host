"""Actual PostgreSQL authorised archival closes runs and removes receipt copies."""
import copy
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import uuid

from mas_host import Invocation, Outcome
from mas_host.service_contracts import ServiceError
from mas_host.shared_authority import SharedAuthority
from mas_host.shared_backup import postgres_environment
from mas_host.shared_service import create_shared_service
from mas_host.task_service import Workflow


class ArchiveEcho:
    def handle(self, _context, request):
        return Outcome(request.input)


class ArchiveEffect:
    def handle(self, context, request):
        return Outcome(context.invoke(lambda: Invocation(request.input, {'outcome': 'completed'}),
                                      input=request.input, reservation_microusd=7))


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedArchivalTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.authority = SharedAuthority(
            binary=os.environ.get('MAS_SHARED_BIN') or str(Path(__file__).resolve().parents[2] / 'rust-protocol/target/debug/mas-shared'),
            dsn=os.environ['MAS_TEST_POSTGRES_DSN'], namespace='mas_archive_' + uuid.uuid4().hex,
            deployment='deployment', controller='controller', epoch=1,
            secret_key=b'archive-fixture-key/' * 2, secret_reference='key/1')
        self.authority.initialise()
        self.addCleanup(self.authority.close)
        self.workflow = Workflow('archival/1', {'agents': ['a', 'b'], 'application': 'agent-task/1',
            'max_calls': 5, 'wall_seconds': 300}, {'a': ArchiveEcho(), 'b': ArchiveEffect()}, 'a', 'b', 'work')
        self.service = create_shared_service(self.authority, self.temporary.name,
            {'work': self.workflow}, {'alice': {'work'}}, supervised=False,
            allocation_limits={key: {'max_runs': 10, 'max_calls': 10, 'max_microusd': 100}
                               for key in ('deployment', 'principal/alice')})

    def sql(self, source):
        result = subprocess.run([shutil.which('psql'), '--no-psqlrc', '--no-password', '--tuples-only',
            '--no-align', '--set=ON_ERROR_STOP=1'], input='SET search_path TO ' + self.authority.namespace + ',pg_catalog;\n' + source,
            text=True, env=postgres_environment(self.authority), stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, timeout=30)
        self.assertEqual(result.returncode, 0, 'native fixture inspection failed')
        return result.stdout.strip().splitlines()[-1]

    def test_lost_historical_reply_becomes_tombstone_without_regrant_or_refund(self):
        marker = 'payload_' + uuid.uuid4().hex
        original = self.authority.command
        commands = []
        def capture(operation, parameters, *, command_id=None):
            command_id = command_id or 'captured-' + uuid.uuid4().hex
            commands.append((operation, copy.deepcopy(parameters), command_id))
            return original(operation, parameters, command_id=command_id)
        with patch.object(self.authority, 'command', side_effect=capture):
            task = self.service.submit('alice', 'request-one', 'work', {'secret': marker})['task_id']
            self.service.run_once()
        self.assertEqual(self.service.result('alice', task), {'secret': marker})
        runs = self.service.recovery_runs('alice', task)
        self.assertEqual(len(runs), 1)
        run = runs[0]['run_id']
        before = self.authority.query('scope', {'scope': 'deployment'})
        archived = original('operator.archive', {'kind': 'tasks', 'task_id': task, 'principal': 'alice',
            'operator': 'owner', 'reason': 'Remove completed fixture payload', 'request_key': 'archive-one',
            'older_than_seconds': 0}, command_id='archive-command')['receipt']
        self.assertTrue(archived['archived'])
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'}), before)

        self.assertEqual((before['calls'], before['microusd']), (1, 7))
        tombstone = self.authority.query('run_archive', {'run': run})
        self.assertEqual(tombstone['format'], 'mas-archived-run/1')
        self.assertFalse(tombstone['result_available'])
        for operation, parameters, identity in commands:
            if operation in ('task.submit', 'task.complete', 'step', 'admit_invocation'):
                with self.assertRaises(ServiceError) as caught:
                    original(operation, parameters, command_id=identity)
                self.assertEqual(caught.exception.code, 'command_result_archived')
                receipt = self.authority.query('command_receipt', {'command_id': identity})
                self.assertEqual(receipt['format'], 'mas-archived-command/1')
                self.assertEqual(len(receipt['receipt_digest']), 64)
                self.assertNotIn(marker, str(receipt))
        with self.assertRaises(ServiceError) as caught:
            original('claim_run', {'run': run, 'owner': 'new-owner'})
        self.assertEqual(caught.exception.code, 'run_archived')
        with self.assertRaises(ServiceError):
            self.service.result('alice', task)
        # Inspect native storage, not just the redacted public result.
        for table in ('host_commands', 'host_steps', 'host_invocations', 'host_resolutions',
                      'messages', 'calls', 'service_tasks', 'service_executions'):
            self.assertEqual(self.sql("SELECT EXISTS(SELECT 1 FROM " + table + " t WHERE row_to_json(t)::text LIKE '%" + marker + "%');"), 'f', table)
        self.assertEqual(self.sql("SELECT count(*) FROM calls WHERE run='" + run + "';"), '1')
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'}), before)

    def test_unmapped_legacy_receipt_rolls_back_all_payload_compaction(self):
        marker = 'legacy_' + uuid.uuid4().hex
        task = self.service.submit('alice', 'legacy-task', 'work', {'secret': marker})['task_id']
        self.service.run_once()
        run = self.service.recovery_runs('alice', task)[0]['run_id']
        # This is the exact retained receipt shape after a pre-10 migration:
        # request/receipt exist, while no trustworthy operation/target was kept.
        self.sql("INSERT INTO host_commands(id,digest,receipt) VALUES('legacy-command','" + 'a' * 64 +
                 "','{\"kind\":\"invoke\",\"input\":{\"secret\":\"" + marker + "\"}}');\n"
                 "UPDATE command_capacity SET normal_used=normal_used+1 WHERE singleton;\nSELECT 1;")
        before = self.authority.query('scope', {'scope': 'deployment'})
        with self.assertRaises(ServiceError) as caught:
            self.authority.command('operator.archive', {'kind': 'tasks', 'task_id': task, 'principal': 'alice',
                'operator': 'owner', 'reason': 'Fixture legacy refusal', 'request_key': 'archive-legacy',
                'older_than_seconds': 0})
        self.assertEqual(caught.exception.code, 'legacy_archive_mapping_required')
        self.assertEqual(self.service.result('alice', task), {'secret': marker})
        self.assertIsNone(self.authority.query('run_archive', {'run': run}))
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'}), before)

    def test_terminal_worker_payload_and_receipt_copies_are_compacted(self):
        from mas_host.distributed import remote_binding, WORKER_PROTOCOL
        from mas_host.shared_control import SharedWorkerController
        marker = 'worker_' + uuid.uuid4().hex
        task = self.service.submit('alice', 'worker-task', 'work', {'secret': marker})['task_id']
        claim = self.authority.command('task.claim', {'owner': 'task-owner', 'task': task, 'workflows': ['work']})['receipt']
        runner = self.authority.runner(self.workflow.topology, self.workflow.adapters,
            Path(self.temporary.name) / 'worker-cache', principal='alice', new_run_id=claim['run_id'],
            workflow_version=self.workflow.version, supervised=False)
        digest = self.workflow.verified_implementation.digest
        controller = SharedWorkerController(self.authority,
            {'worker': {'trust_zone': 'trusted', 'capacity': 1, 'capabilities': {'work': digest}}})
        binding = remote_binding('alice', 'work', digest, 'trusted', {'secret': marker}, 7)
        runner._journal.step('remote-effect', 'invoke', {'input': binding}, {})
        invocation = runner._journal.create_invocation('remote-effect', 'b', 7)
        call = invocation['call_id']
        runner._session.admit_invocation(call)
        controller.submit_admitted(call, binding)
        controller.register('worker', boot='boot', capabilities={'work': digest}, architecture='x86_64',
            runtime='python-3.14.0', trust_zone='trusted', protocol=WORKER_PROTOCOL)
        assignment = controller.claim('worker', boot='boot')['assignment']
        self.assertTrue(controller.dispatch('worker', **{key: assignment[key] for key in ('boot', 'call_id', 'fence')})['execute'])
        receipt = {'id': 'worker-result', 'call_id': call, 'boot': assignment['boot'], 'fence': assignment['fence'],
            'status': 'completed', 'output': {'secret': marker}, 'evidence': {}, 'output_profile': 'inline-json/1'}
        acknowledgement = controller.receipt('worker', receipt=receipt)
        self.authority.command('observe_invocation', {'run': runner.run_id, 'owner': runner._session.owner,
            'call_id': call, 'state': 'completed', 'output': receipt['output'], 'evidence': {'receipt': receipt['id']}})
        runner.close()
        self.authority.command('task.complete', {'principal': 'alice', 'task': task, 'owner': 'task-owner',
            'state': 'completed', 'result': receipt['output'], 'error': None})
        before = self.authority.query('scope', {'scope': 'deployment'})
        self.authority.command('operator.archive', {'kind': 'tasks', 'task_id': task, 'principal': 'alice',
            'operator': 'owner', 'reason': 'Remove completed worker payload', 'request_key': 'archive-worker',
            'older_than_seconds': 0})
        self.assertEqual(controller.receipt('worker', receipt=receipt), acknowledgement)
        with self.assertRaises(ServiceError) as changed:
            controller.receipt('worker', receipt={**receipt, 'output': {'changed': True}})
        self.assertEqual(changed.exception.code, 'receipt_conflict')
        with self.assertRaises(ServiceError) as novel:
            controller.receipt('worker', receipt={**receipt, 'id': 'new-receipt'})
        self.assertEqual(novel.exception.code, 'worker_job_archived')
        for table in ('worker_jobs', 'worker_receipts', 'host_commands', 'host_steps', 'host_invocations'):
            self.assertEqual(self.sql("SELECT EXISTS(SELECT 1 FROM " + table + " t WHERE row_to_json(t)::text LIKE '%" + marker + "%');"), 'f', table)
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'}), before)

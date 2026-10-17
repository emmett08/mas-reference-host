"""Actual PostgreSQL registered-worker dispatch through the unified factory."""
import os
from pathlib import Path
import tempfile
import unittest
import uuid

from test_service_executable_workers import ROOT, FIXTURE, worker_spec, workflow, plan


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class RegisteredWorkerSharedTests(unittest.TestCase):
    def test_shared_factory_routes_leaf_and_composed_registered_workers(self):
        from mas_host.shared_authority import SharedAuthority
        from mas_host.shared_service import create_shared_service
        with tempfile.TemporaryDirectory() as directory:
            authority = SharedAuthority(binary=os.environ.get('MAS_SHARED_BIN') or str(ROOT / 'rust-protocol/target/debug/mas-shared'),
                dsn=os.environ['MAS_TEST_POSTGRES_DSN'], namespace='mas_workers_' + uuid.uuid4().hex,
                deployment='deployment', controller='controller', epoch=1,
                secret_key=b'worker-test-secret' * 3, secret_reference='key/1')
            authority.initialise()
            self.addCleanup(authority.close)
            script = Path(directory) / 'worker.py'
            script.write_bytes(FIXTURE.read_bytes())
            selected = workflow(worker_spec(script=script))
            service = create_shared_service(authority, Path(directory), {'work': selected},
                {'alice': {'work', 'sequence'}}, plans={'sequence': plan()},
                gateway_bin=os.environ.get('MAS_GATEWAY_BIN') or str(ROOT / 'rust-protocol/target/debug/mas-gateway'),
                allocation_limits={name: {'max_runs': 20, 'max_calls': 20, 'max_microusd': 100}
                                   for name in ('deployment', 'principal/alice')})
            for name in ('work', 'sequence'):
                with self.subTest(name=name):
                    queued = service.submit('alice', name, name, {'value': 19})
                    done = service.run_once(queued['task_id'])
                    self.assertEqual(done['state'], 'completed', done)
                    self.assertEqual(service.result('alice', done['task_id']), {'value': 19})
                    runs = service.recovery_runs('alice', done['task_id'])
                    self.assertTrue(runs)
                    for run in runs:
                        calls = authority.query('invocations', {'run': run['run_id']})
                        self.assertEqual(len(calls), 1)
                        self.assertEqual(calls[0]['state'], 'completed')
            queued = service.submit('alice', 'drift', 'work', {'value': 23})
            script.write_text("raise AssertionError('changed worker must not execute')\n")
            blocked = service.run_once(queued['task_id'])
            self.assertEqual(blocked['state'], 'blocked', blocked)
            self.assertEqual(blocked['workflow_identity'], selected.identity)
            self.assertEqual(service.submit('alice', 'drift', 'work', {'value': 23})['task_id'], queued['task_id'])

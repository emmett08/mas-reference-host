"""Read-only diagnostics cannot execute factories, expose identities or mutate state."""
from contextlib import closing, redirect_stdout
from dataclasses import asdict
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import shutil
import tempfile
import unittest
from unittest.mock import patch

from mas_host.admin import main
from mas_host.circuit_breaker import CIRCUITS
from mas_host.diagnostics import doctor, effective_configuration, operational_metrics, prometheus_metrics
from mas_host.distributed import REMOTE
from mas_host.operations import DeploymentLedger, DeploymentPolicy
from mas_host.schema import LEDGER_V2, connect
from mas_host.task_store import SERVICE, SERVICE_V1
from mas_host.workflow_store import COMPOSITION_V1


class DiagnosticsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / 'state'
        self.config = self.root / 'config.json'
        self.secret = 'private-principal-and-credential'

    def write_config(self, **changes):
        credentials = self.root / 'credentials.json'
        credentials.write_text(json.dumps({'version': 1, 'credentials': [{
            'sha256': hashlib.sha256(self.secret.encode()).hexdigest(),
            'principal': self.secret, 'scopes': ['mas:submit']}]}))
        credentials.chmod(0o600)
        source = {'version': 1, 'factory': 'untrusted_module:factory',
                  'grants': {self.secret: ['private-workflow']},
                  'auth': {'type': 'static', 'file': str(credentials)}}
        source.update(changes)
        self.config.write_text(json.dumps(source))
        return source

    def snapshot(self):
        return {str(path.relative_to(self.root)): (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mode)
                for path in self.root.rglob('*') if path.is_file()}

    def database(self, name, schema):
        self.state.mkdir(mode=0o700, exist_ok=True)
        db = connect(self.state / name, schema, initialise=lambda _db: None)
        self.addCleanup(db.close)
        return db

    def test_effective_config_defaults_validate_auth_without_loading_factory_or_network(self):
        source = self.write_config(scheduler={'max_workers': 7, 'resources': {'secret-provider': 3}})
        before = self.snapshot()
        with patch('mas_host.bridges.authentication.StaticTokenVerifier.verify', side_effect=AssertionError('token verification forbidden')), \
             patch('mas_host.bridge_config.importlib.import_module', side_effect=AssertionError('factory import forbidden')):
            result = effective_configuration(self.config)
        self.assertTrue(result['ok'], result)
        summary = result['configuration']
        self.assertEqual(summary['limits'], {'max_pending': 1000, 'max_tasks': 10000, 'max_instances': 1000})
        self.assertEqual(summary['scheduler']['max_workers'], 7)
        self.assertEqual(summary['scheduler']['resource_limit_count'], 1)
        self.assertEqual(summary['principal_count'], 1)
        self.assertFalse(summary['factory_loaded'])
        text = json.dumps(result)
        for secret in (self.secret, source['factory'], 'secret-provider', 'private-workflow', str(self.root)):
            self.assertNotIn(secret, text)
        self.assertEqual(self.snapshot(), before)

    def test_oauth_credential_readability_checked_without_introspection(self):
        secret = self.root / 'client-secret'
        secret.write_text(self.secret)
        secret.chmod(0o600)
        self.write_config(auth={'type': 'oauth', 'issuer': 'https://issuer.example',
            'resource': 'https://mas.example', 'introspection_url': 'https://issuer.example/token',
            'client_id': self.secret, 'client_secret_file': str(secret)})
        with patch('mas_host.bridges.authentication.OAuthIntrospectionVerifier.verify', side_effect=AssertionError()):
            self.assertTrue(effective_configuration(self.config)['ok'])
        secret.chmod(0o644)
        self.assertEqual(effective_configuration(self.config), {'ok': False, 'error': 'diagnostic_configuration_invalid'})

    def test_malformed_declarations_fail_without_echoing_values(self):
        for changes in ({'factory': None}, {'auth': None}, {'scheduler': {'max_workers': True}},
                        {'max_instances': 100001}, {'allowed_hosts': self.secret},
                        {'artifacts': {'scopes': {}}}, {'public_url': 'https://user:password@example.test'}):
            with self.subTest(changes=changes):
                self.write_config(**changes)
                result = effective_configuration(self.config)
                self.assertEqual(result, {'ok': False, 'error': 'diagnostic_configuration_invalid'})

    def test_absent_state_metrics_are_zero_without_creating_directory(self):
        before = self.snapshot()
        result = operational_metrics(self.state)
        self.assertTrue(result['ok'], result)
        self.assertFalse(self.state.exists())
        self.assertFalse(result['budget']['admission_open'])
        self.assertEqual(sum(result['counts']['tasks'].values()), 0)
        self.assertTrue(all(not store['present'] for store in result['stores'].values()))
        self.assertEqual(self.snapshot(), before)

    def test_readonly_legacy_ledger_service_composition_do_not_migrate(self):
        self.state.mkdir(mode=0o700)
        for name, schema in [('deployment.sqlite', LEDGER_V2), ('task-service.sqlite', SERVICE_V1),
                             ('composition.sqlite', COMPOSITION_V1)]:
            with closing(sqlite3.connect(self.state / name)) as db:
                for statement in schema.statements:
                    db.execute(statement)
                db.execute(f'PRAGMA application_id={schema.application_id}')
                db.execute(f'PRAGMA user_version={schema.version}')
                if name == 'deployment.sqlite':
                    db.executemany('INSERT INTO metadata VALUES(?,?)', [
                        ('policy', json.dumps(asdict(DeploymentPolicy()))),
                        ('deployment_id', self.secret), ('created_at', '123'), ('admission_state', 'active')])
                db.commit()
        before = self.snapshot()
        result = operational_metrics(self.state)
        self.assertTrue(result['ok'], result)
        for name in ('deployment.sqlite', 'task-service.sqlite', 'composition.sqlite'):
            self.assertTrue(result['stores'][name]['migration_needed'])
        self.assertNotIn(self.secret, json.dumps(result))
        self.assertEqual(self.snapshot(), before)

    def test_metrics_aggregate_age_budget_circuits_and_workers_without_private_labels(self):
        DeploymentLedger(self.state, DeploymentPolicy(min_free_bytes=1))
        db = self.database('task-service.sqlite', SERVICE)
        for index, state, stamp in ((1, 'queued', 10000), (2, 'unknown', 30000)):
            db.execute('INSERT INTO tasks(id,principal,request_key,workflow,workflow_identity,state,input,metadata,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
                (str(index), self.secret, self.secret, self.secret, 'identity', state,
                 json.dumps({'secret': self.secret}), '{}', stamp, stamp))
        db.commit()
        circuits = self.database('provider-circuits.sqlite', CIRCUITS)
        circuits.execute('INSERT INTO circuits VALUES(?,?,5,200000,1,NULL,NULL)', (self.secret, '{}'))
        circuits.commit()
        workers = self.database('remote-workers.sqlite', REMOTE)
        workers.execute('INSERT INTO workers VALUES(?,?,1,?,1,1000)', (self.secret, self.secret, '{}'))
        workers.execute("INSERT INTO jobs(call_id,run_id,principal,binding,state,created) VALUES(?,?,?,?,?,?)",
                        (self.secret, self.secret, self.secret, '{}', 'pending', 20000))
        workers.commit()
        before = self.snapshot()
        result = operational_metrics(self.state, now_ms=100000)
        self.assertTrue(result['ok'], result)
        self.assertEqual(result['ages_seconds']['tasks'], {'queued': 90, 'unknown': 70})
        self.assertEqual(result['ages_seconds']['remote_jobs']['queued'], 80)
        self.assertEqual(result['circuits']['open'], 1)
        self.assertEqual(result['workers'], {'registered': 1, 'draining': 1})
        self.assertTrue(result['budget']['admission_open'])
        rendered = prometheus_metrics(result)
        self.assertNotIn(self.secret, rendered + json.dumps(result))
        self.assertLess(len(rendered.splitlines()), 150)
        self.assertIn('mas_records{source="tasks",state="unknown"} 1', rendered)
        self.assertEqual(self.snapshot(), before)

    def test_corrupt_unknown_schema_and_unknown_state_fail_closed_and_never_mutate(self):
        db = self.database('task-service.sqlite', SERVICE)
        for statement in ('PRAGMA user_version=99', 'PRAGMA application_id=42',
                          'CREATE TABLE injected(secret TEXT)'):
            with self.subTest(statement=statement):
                db.execute(statement)
                db.commit()
                before = self.snapshot()
                result = operational_metrics(self.state)
                self.assertEqual(result, {'ok': False, 'error': 'diagnostic_state_unavailable'})
                self.assertEqual(prometheus_metrics(result).splitlines()[-1], 'mas_diagnostics_ok 0')
                self.assertEqual(self.snapshot(), before)
        db.close()
        (self.state / 'task-service.sqlite').unlink()
        (self.state / 'composition.sqlite').write_bytes(b'not a database secret')
        before = self.snapshot()
        self.assertFalse(operational_metrics(self.state)['ok'])
        self.assertEqual(self.snapshot(), before)

    def test_untrusted_database_state_cannot_expand_metric_cardinality(self):
        db = self.database('task-service.sqlite', SERVICE)
        db.execute('INSERT INTO tasks(id,principal,request_key,workflow,workflow_identity,state,input,metadata,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
                   ('one', 'owner', 'key', 'flow', 'identity', self.secret, '{}', '{}', 0, 0))
        db.commit()
        result = operational_metrics(self.state)
        self.assertFalse(result['ok'])
        self.assertNotIn(self.secret, prometheus_metrics(result))

    def test_wal_modes_fail_before_sqlite_open_without_creating_or_modifying_sidecars(self):
        for mode in ('existing-shm', 'missing-shm', 'checkpointed-no-sidecars'):
            with self.subTest(mode=mode):
                source = self.root / ('source-' + mode)
                source.mkdir(mode=0o700)
                db = connect(source / 'task-service.sqlite', SERVICE, initialise=lambda _db: None)
                self.addCleanup(db.close)
                db.execute('PRAGMA journal_mode=WAL')
                db.execute("INSERT INTO metadata VALUES('private',?)", (self.secret,))
                db.commit()
                if mode == 'checkpointed-no-sidecars':
                    db.close()
                destination = self.root / ('state-' + mode)
                destination.mkdir(mode=0o700)
                for path in source.iterdir():
                    if mode != 'missing-shm' or not path.name.endswith('-shm'):
                        shutil.copyfile(path, destination / path.name)
                self.assertEqual((destination / 'task-service.sqlite').read_bytes()[18:20], b'\x02\x02')
                before = self.snapshot()
                with patch('mas_host.diagnostics.connect', side_effect=AssertionError('WAL must not be opened')) as opened:
                    report = operational_metrics(destination)
                opened.assert_not_called()
                self.assertEqual(report, {'ok': False, 'error': 'diagnostic_state_unavailable'})
                self.assertEqual(self.snapshot(), before)

    def test_prometheus_rejects_supplied_labels_and_metric_names_without_partial_output(self):
        original = operational_metrics(self.state)
        for family, entry in (('counts', 'tasks'), ('ages_seconds', 'tasks'),
                              ('budget', None), ('stores', None)):
            with self.subTest(family=family):
                report = json.loads(json.dumps(original))
                target = report[family][entry] if entry else report[family]
                target['private"}\ninjected_metric 999'] = 1
                text = prometheus_metrics(report)
                self.assertEqual(text, prometheus_metrics({'ok': False}))
                self.assertNotIn('private', text)
                self.assertNotIn('mas_records', text)

    def test_binary_hash_bound_applies_when_file_grows_after_initial_stat(self):
        from mas_host.diagnostics import _binary
        executable = self.root / 'growing-binary'
        executable.write_bytes(b'abcd')
        executable.chmod(0o700)
        digest = hashlib.sha256
        def grow_then_hash():
            with executable.open('ab') as stream:
                stream.write(b'efghijkl')
            return digest()
        with patch('mas_host.diagnostics._BINARY_MAX_BYTES', 8), \
                patch('mas_host.diagnostics.hashlib.sha256', side_effect=grow_then_hash):
            observed = _binary(str(executable))
        self.assertEqual(observed['version_status'], 'unavailable')
        self.assertFalse(observed['executable'])
        self.assertIsNone(observed['sha256'])

    def test_doctor_hashes_binaries_without_executing_any_code(self):
        self.write_config()
        executable = self.root / 'untrusted-binary'
        marker = self.root / 'would-execute'
        executable.write_text('#!/bin/sh\ntouch ' + str(marker) + '\n')
        executable.chmod(0o700)
        before = self.snapshot()
        report = doctor(self.state, self.config, environ={'MAS_BIN': str(executable), 'MAS_GATEWAY_BIN': str(executable)})
        self.assertEqual(report['ok'], os.access(executable, os.X_OK), report)
        self.assertTrue(report['advisory'])
        self.assertEqual(report['runtime']['binaries']['mas']['sha256'], hashlib.sha256(executable.read_bytes()).hexdigest())
        self.assertEqual(report['runtime']['binaries']['mas']['version_status'], 'not_executed')
        self.assertFalse(marker.exists())
        self.assertFalse(self.state.exists())
        self.assertEqual(self.snapshot(), before)
        self.assertNotIn(self.secret, json.dumps(report))

    def test_cli_json_prometheus_effective_config_and_preflight(self):
        self.write_config()
        for args in (['metrics'], ['metrics', '--format', 'prometheus'], ['effective-config', '--config', str(self.config)],
                     ['preflight', '--config', str(self.config)]):
            with self.subTest(args=args), redirect_stdout(io.StringIO()) as output:
                code = main(['--state-dir', str(self.state), *args])
            self.assertEqual(code, 0, output.getvalue())
            self.assertNotIn(self.secret, output.getvalue())
            if 'prometheus' in args:
                self.assertIn('mas_diagnostics_ok 1', output.getvalue())
            else:
                self.assertTrue(json.loads(output.getvalue())['ok'])
        self.assertFalse(self.state.exists())


if __name__ == '__main__':
    unittest.main()

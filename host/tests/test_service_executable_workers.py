"""Registered worker identities and real dispatch through durable service ports."""
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from mas_host import Outcome, RunnerError
from mas_host.bridge_config import build_service
from mas_host.executable_worker import WorkerSpec, WorkerRegistry
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, Workflow
from mas_host.unified_service import UnifiedTaskService
from test_runner import topology


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = Path(__file__).resolve().parent / 'fixtures' / 'registered_worker.py'
MAS = os.environ.get('MAS_BIN', str(ROOT / 'rust-protocol/target/release/mas'))
GATEWAY = os.environ.get('MAS_GATEWAY_BIN', str(ROOT / 'rust-protocol/target/release/mas-gateway'))


class Echo:
    def handle(self, _context, request):
        return Outcome(request.input)


def worker_spec(mode='invoke', *, script=FIXTURE, configuration=None, environment=None, secret_environment_names=()):
    return WorkerSpec('application', '1', (sys.executable, str(script)), factory='fixture',
        configuration=configuration or {'mode': mode}, cwd=ROOT,
        environment=environment or {'PYTHONPATH': str(ROOT / 'host')},
        secret_environment_names=secret_environment_names)


def workflow(spec=None, *, mixed=False):
    adapter = WorkerRegistry([spec or worker_spec()]).adapter('application')
    adapters = {actor: adapter for actor in ('a', 'b', 'c')}
    if mixed:
        adapters.update(a=Echo(), c=Echo())
    return Workflow('registered/1', topology(), adapters, 'a', 'b', 'work')


def registered_workflows():
    """Trusted bridge configuration factory, independent of request input."""
    return {'work': workflow()}


def plan():
    return {'format': 'mas-workflow/1', 'name': 'sequence', 'version': '1',
        'limits': {'max_steps': 20, 'max_parallel': 1, 'wall_seconds': 120,
                   'max_calls': 10, 'max_reserved_microusd': 0},
        'root': {'kind': 'sequence', 'steps': [{'kind': 'task', 'workflow': 'work'}] * 2}}


class RegisteredWorkerIdentityTests(unittest.TestCase):
    def test_configuration_and_loading_environment_change_workflow_identity(self):
        one = workflow(worker_spec())
        two = workflow(worker_spec('inspect'))
        self.assertNotEqual(one.identity, two.identity)
        self.assertEqual(one.verified_implementation.digest, two.verified_implementation.digest)
        self.assertEqual(one.descriptor['worker_bindings']['b'], one.adapters['b'].spec.descriptor)
        environment = {'PYTHONPATH': str(ROOT / 'host'), 'PATH': '/usr/bin', 'PROVIDER_KEY': 'first'}
        first = workflow(worker_spec(environment=environment, secret_environment_names=('PROVIDER_KEY',)))
        second = workflow(worker_spec(environment={**environment, 'PROVIDER_KEY': 'rotated'},
                                      secret_environment_names=('PROVIDER_KEY',)))
        changed = workflow(worker_spec(environment={**environment, 'PATH': '/other/bin'},
                                       secret_environment_names=('PROVIDER_KEY',)))
        self.assertEqual(first.identity, second.identity)
        self.assertNotEqual(first.identity, changed.identity)
        self.assertNotIn('first', str(first.descriptor))
        plain = Workflow('legacy/1', topology(), {actor: Echo() for actor in ('a', 'b', 'c')}, 'a', 'b', 'echo')
        self.assertNotIn('worker_bindings', plain.descriptor)

    def test_all_ordinary_environment_values_and_secret_policy_bind_identity(self):
        environment = {'PYTHONPATH': str(ROOT / 'host'), 'ENDPOINT': 'https://one.example',
                       'MODE': 'checked', 'FEATURE_FLAG': 'off', 'TOKEN': 'private-one'}
        original = worker_spec(environment=environment, secret_environment_names=('TOKEN',))
        for name, value in {'ENDPOINT': 'https://two.example', 'MODE': 'unchecked', 'FEATURE_FLAG': 'on'}.items():
            with self.subTest(name=name):
                changed = worker_spec(environment={**environment, name: value}, secret_environment_names=('TOKEN',))
                self.assertNotEqual(original.identity, changed.identity)
                self.assertNotEqual(workflow(original).identity, workflow(changed).identity)
        public_token = worker_spec(environment=environment)
        self.assertNotEqual(original.identity, public_token.identity)
        rotated = worker_spec(environment={**environment, 'TOKEN': 'private-two'}, secret_environment_names=('TOKEN',))
        self.assertEqual(original.descriptor, rotated.descriptor)
        self.assertEqual(original.identity, rotated.identity)
        self.assertNotIn('private-one', str(original.descriptor))
        import hashlib
        self.assertNotIn(hashlib.sha256(b'private-one').hexdigest(), str(original.descriptor))
        copied = original.with_workspace(ROOT, {'mode': 'inspect'})
        self.assertEqual(copied.descriptor['secret_environment_names'], ['TOKEN'])
        self.assertEqual(copied.descriptor['environment_sha256'], original.descriptor['environment_sha256'])
        copied.verify()

    def test_secret_declarations_cannot_hide_loader_values_or_unknown_names(self):
        environment = {'PYTHONPATH': str(ROOT / 'host'), 'PATH': '/usr/bin',
                       'LD_PRELOAD': '/approved.so', 'DYLD_LIBRARY_PATH': '/approved', 'TOKEN': 'private'}
        for names in [('PYTHONPATH',), ('PATH',), ('LD_PRELOAD',), ('DYLD_LIBRARY_PATH',),
                      ('ABSENT',), ('TOKEN', 'TOKEN'), 'TOKEN']:
            with self.subTest(names=names), self.assertRaises(RunnerError):
                worker_spec(environment=environment, secret_environment_names=names)

    def test_factory_cannot_replace_pinned_worker_configuration(self):
        original = workflow()
        original.verify_adapters(dict(original.adapters))
        replaced = workflow(worker_spec('inspect'))
        with self.assertRaises(ServiceError) as error:
            original.verify_adapters(dict(replaced.adapters))
        self.assertEqual(error.exception.code, 'implementation_worker_binding_mismatch')
        with self.assertRaises(ServiceError):
            original.verify_adapters({actor: Echo() for actor in original.adapters})

    def test_python_module_binds_whole_entry_package_without_importing_it(self):
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / 'worker_application'
            package.mkdir()
            (package / '__init__.py').write_text("raise AssertionError('registration must not import')\n")
            (package / '__main__.py').write_text('from .helper import result\n')
            helper = package / 'helper.py'
            helper.write_text('result = 1\n')
            spec = WorkerSpec('package', '1', (sys.executable, '-m', 'worker_application'),
                factory='application', configuration={}, cwd=ROOT,
                environment={'PYTHONPATH': directory})
            spec.verify()
            before = helper.stat()
            helper.write_text('result = 2\n')
            os.utime(helper, ns=(before.st_atime_ns, before.st_mtime_ns))
            with self.assertRaises(RunnerError):
                spec.verify()


@unittest.skipUnless(sys.platform.startswith('linux'), 'registered workers require Linux')
class RegisteredWorkerServiceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    def service(self, selected=None, **options):
        return DurableTaskService(self.directory / 'service', {'work': selected or workflow()},
            {'alice': {'work'}}, mas_bin=MAS, gateway_bin=GATEWAY, **options)

    def test_service_routes_registered_workers_and_retains_native_call_evidence(self):
        service = self.service()
        submitted = service.submit('alice', 'first', 'work', {'value': 7})
        done = service.run_once(submitted['task_id'])
        self.assertEqual(done['state'], 'completed', done)
        self.assertEqual(service.result('alice', done['task_id']), {'value': 7})
        from mas_host.journal import inspect_run
        execution = service.recovery_runs('alice', done['task_id'])[0]
        calls = inspect_run(service.state_dir / execution['run_directory'])['invocations']
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]['state'], 'completed')

    def test_mixed_profiles_delegate_through_the_same_parent_context(self):
        selected = workflow(worker_spec(configuration={'mode': 'delegate', 'recipient': 'c'}), mixed=True)
        service = self.service(selected)
        submitted = service.submit('alice', 'mixed', 'work', {'value': 11})
        done = service.run_once(submitted['task_id'])
        self.assertEqual(done['state'], 'completed', done)
        self.assertEqual(service.result('alice', done['task_id']), {'value': 11})

    def test_script_drift_blocks_old_work_without_relabelling_or_effects(self):
        script = self.directory / 'worker.py'
        script.write_bytes(FIXTURE.read_bytes())
        selected = workflow(worker_spec(script=script))
        service = self.service(selected)
        queued = service.submit('alice', 'old', 'work', {'value': 3})
        script.write_text("raise AssertionError('unapproved changed worker')\n")
        done = service.run_once(queued['task_id'])
        self.assertEqual(done['state'], 'blocked', done)
        self.assertEqual(done['workflow_identity'], selected.identity)
        self.assertEqual(service.submit('alice', 'old', 'work', {'value': 3})['task_id'], queued['task_id'])
        self.assertNotEqual(workflow(worker_spec(script=script)).identity, selected.identity)

    def test_existing_bytecode_cannot_override_the_pinned_source_package(self):
        import py_compile
        package = self.directory / 'checked_worker'
        package.mkdir()
        (package / '__init__.py').write_text('')
        (package / '__main__.py').write_text('from .implementation import Fixture\n'
            'from mas_host.worker_entry import run_worker\n'
            "raise SystemExit(run_worker({'fixture': Fixture}))\n")
        source = package / 'implementation.py'
        good = FIXTURE.read_text()
        bad = good.replace('lambda: Invocation(request.input,', 'lambda: Invocation({"tampered": 1},')
        good += ' ' * (len(bad.encode()) - len(good.encode()))
        source.write_text(bad)
        before = source.stat()
        py_compile.compile(str(source), doraise=True)
        source.write_text(good)
        os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns))
        spec = WorkerSpec('application', '1', (sys.executable, '-m', 'checked_worker'),
            factory='fixture', configuration={'mode': 'invoke'}, cwd=ROOT,
            environment={'PYTHONPATH': os.pathsep.join((str(self.directory), str(ROOT / 'host')))})
        service = self.service(workflow(spec))
        queued = service.submit('alice', 'bytecode', 'work', {'value': 29})
        done = service.run_once(queued['task_id'])
        self.assertEqual(done['state'], 'completed', done)
        self.assertEqual(service.result('alice', done['task_id']), {'value': 29})

    def test_trusted_bridge_factory_and_composition_dispatch_registered_workers(self):
        with mock.patch.dict(os.environ, MAS_BIN=MAS, MAS_GATEWAY_BIN=GATEWAY):
            service = build_service({'factory': __name__ + ':registered_workflows',
                'grants': {'alice': ['work']}}, self.directory / 'bridge')
        submitted = service.submit('alice', 'bridge', 'work', {'value': 13})
        self.assertEqual(service.run_once(submitted['task_id'])['state'], 'completed')
        composed = UnifiedTaskService(self.directory / 'composition', {'work': workflow()},
            {'sequence': plan()}, {'alice': {'sequence'}}, mas_bin=MAS, gateway_bin=GATEWAY)
        submitted = composed.submit('alice', 'composed', 'sequence', {'value': 17})
        done = composed.run_once(submitted['task_id'])
        self.assertEqual(done['state'], 'completed', done)
        self.assertEqual(composed.result('alice', done['task_id']), {'value': 17})



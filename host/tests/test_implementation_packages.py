"""Package bytes, loaded callbacks and historical identities stay separate."""
import importlib.util
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

from mas_host.definitions import DEFINITIONS_V1, DefinitionRegistry, canonical
from mas_host.implementation import VerifiedImplementation
from mas_host.schema import connect
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, Workflow
from test_definition_versions import Executor


SOURCE = '''def helper():
    return 1

class Adapter:
    def handle(self, context, request):
        return helper()
'''


class ImplementationPackageTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.name = 'mas_verified_fixture'
        self.addCleanup(lambda: sys.modules.pop(self.name, None))

    def load(self, root, source=SOURCE):
        package = root / self.name
        package.mkdir(parents=True, exist_ok=True)
        path = package / '__init__.py'
        path.write_text(source)
        spec = importlib.util.spec_from_file_location(self.name, path,
                                                   submodule_search_locations=[str(package)])
        module = importlib.util.module_from_spec(spec)
        sys.modules[self.name] = module
        # compile actual bytes; avoid timestamp-based pyc reuse in this fixture.
        exec(compile(source, str(path), 'exec'), module.__dict__)
        return module, path

    def workflow(self, module):
        return Workflow('stable-label', {'agents': ['a', 'b'], 'application': 'agent-task/1'},
                        {'a': module.Adapter(), 'b': module.Adapter()}, 'a', 'b', 'work')

    def test_standard_dataclass_repr_does_not_require_synthetic_source(self):
        module, _ = self.load(self.root, SOURCE + """
from dataclasses import dataclass
@dataclass
class Options:
    label: str
class Configured:
    def __init__(self):
        self.options = Options('fixed')
    def handle(self, context, request):
        return repr(self.options)
""")
        measured = VerifiedImplementation.capture((module.Configured(),))
        measured.verify((module.Configured(),))

    def test_factory_constructor_classes_are_pinned_before_first_binding(self):
        module, _ = self.load(self.root, SOURCE + """
class Constructed:
    def handle(self, context, request):
        return helper()
def factory(binding):
    return {'a': Constructed(), 'b': Constructed()}
""")
        workflow = Workflow('factory/1', {'agents': ['a', 'b'], 'application': 'agent-task/1'},
            {'a': module.Adapter(), 'b': module.Adapter()}, 'a', 'b', 'work', adapter_factory=module.factory)
        names = {item['name'] for item in workflow.verified_implementation.manifest['entrypoints']}
        self.assertIn('Constructed.handle', names)
        workflow.verified_implementation.verify_bindings(tuple(module.factory(None).values()))

    def test_dynamic_application_method_cannot_hide_behind_another_adapter(self):
        module, _ = self.load(self.root)
        namespace = {'__name__': module.__name__}
        exec("def handle(self, context, request): return 42", namespace)
        dynamic = type('Dynamic', (), {'__module__': module.__name__, 'handle': namespace['handle']})
        with self.assertRaisesRegex(ServiceError, 'source unavailable'):
            VerifiedImplementation.capture((module.Adapter(), dynamic()))

    def test_relocation_preserves_reproducible_identity(self):
        first, _ = self.load(self.root / 'first')
        original = self.workflow(first)
        second, _ = self.load(self.root / 'second')
        relocated = self.workflow(second)
        self.assertEqual(original.identity, relocated.identity)
        self.assertNotIn(str(self.root), original.verified_implementation.encoded_manifest)

    def test_false_digest_and_stale_loaded_helper_are_rejected(self):
        module, path = self.load(self.root)
        original = self.workflow(module)
        path.write_text(SOURCE.replace('return 1', 'return 2'))
        with self.assertRaisesRegex(ServiceError, 'loaded code mismatch'):
            self.workflow(module)
        with self.assertRaises(ServiceError):
            original.verify_implementation()

    def test_changed_package_assets_change_identity_with_same_version_label(self):
        module, path = self.load(self.root)
        asset = path.parent / 'policy.json'
        asset.write_text('{"level":1}')
        original = self.workflow(module)
        stamp = asset.stat()
        asset.write_text('{"level":2}')
        os.utime(asset, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
        changed = self.workflow(module)
        self.assertNotEqual(original.identity, changed.identity)
        with self.assertRaisesRegex(ServiceError, 'implementation changed'):
            original.verify_implementation()

    def test_loaded_helper_rebinding_cannot_hide_behind_package_digest(self):
        module, _ = self.load(self.root, SOURCE + '\ndef other():\n    return 9\n')
        original = self.workflow(module)
        module.helper = module.other
        changed = self.workflow(module)
        self.assertNotEqual(original.identity, changed.identity)
        with self.assertRaises(ServiceError):
            original.verify_implementation()

    def test_missing_package_blocks_only_its_execution_and_keeps_history(self):
        module, path = self.load(self.root / 'packages')
        workflow = self.workflow(module)
        executor = Executor()
        service = DurableTaskService(self.root / 'state', {'leaf': workflow},
                                     {'owner': {'leaf'}}, executor=executor)
        receipt = service.submit('owner', 'old', 'leaf', {})
        path.unlink()
        self.assertEqual(service.inspect('owner', receipt['task_id'])['state'], 'queued')
        self.assertEqual(service.run_once()['state'], 'blocked')
        self.assertEqual(service.cancel('owner', receipt['task_id'])['state'], 'cancelled')
        self.assertEqual(executor.calls, [])

    def test_transitive_imported_helper_package_is_pinned(self):
        dependency = 'mas_verified_dependency'
        self.addCleanup(lambda: sys.modules.pop(dependency, None))
        dep_path = self.root / (dependency + '.py')
        dep_path.write_text('def convert():\n    return 7\n')
        spec = importlib.util.spec_from_file_location(dependency, dep_path)
        dep = importlib.util.module_from_spec(spec)
        sys.modules[dependency] = dep
        spec.loader.exec_module(dep)
        module, _ = self.load(self.root, 'from ' + dependency + ' import convert\n' +
                              SOURCE.replace('return helper()', 'return convert()'))
        original = self.workflow(module)
        self.assertIn(dependency + '/' + dependency + '.py', original.verified_implementation.manifest['files'])
        dep_path.write_text('def convert():\n    return 8\n')
        with self.assertRaises(ServiceError):
            original.verify_implementation()

    def test_factory_adapters_require_previously_pinned_packages(self):
        module, _ = self.load(self.root)
        original = self.workflow(module)
        original.verified_implementation.verify_bindings((module.Adapter(),))
        other_name = 'mas_unpinned_factory'
        self.addCleanup(lambda: sys.modules.pop(other_name, None))
        path = self.root / (other_name + '.py')
        path.write_text(SOURCE)
        spec = importlib.util.spec_from_file_location(other_name, path)
        other = importlib.util.module_from_spec(spec)
        sys.modules[other_name] = other
        spec.loader.exec_module(other)
        with self.assertRaisesRegex(ServiceError, 'unpinned factory adapter'):
            original.verified_implementation.verify_bindings((other.Adapter(),))

    def test_registry_v1_migration_preserves_assertions_and_identity(self):
        state = self.root / 'state'
        state.mkdir()
        path = state / 'definition-registry.sqlite'
        with connect(path, DEFINITIONS_V1, initialise=lambda _: None) as db:
            db.execute('INSERT INTO definitions VALUES(?,?,?,?,?,?,?,?)',
                       ('workflow', 'old', 'b' * 64, '1', None, 'a' * 64, 0, 0))
            db.execute('INSERT INTO aliases VALUES(?,?,?)', ('workflow', 'old', 'b' * 64))
        readonly = DefinitionRegistry(state, readonly=True)
        self.assertEqual(readonly.inspect('workflow', 'old', 'b' * 64)['implementation_provenance'],
                         'deployment_asserted_digest')
        migrated = DefinitionRegistry(state)
        self.assertEqual(migrated.current('workflow', 'old'), 'b' * 64)
        old = migrated.inspect('workflow', 'old', 'b' * 64)
        self.assertIsNone(old['implementation_manifest'])
        self.assertEqual(old['implementation_provenance'], 'deployment_asserted_digest')
        with sqlite3.connect(path) as db:
            self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0], 2)


if __name__ == '__main__':
    unittest.main()

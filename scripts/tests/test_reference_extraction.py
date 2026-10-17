"""The extracted distribution works without a protocol source checkout."""
import importlib.metadata
import importlib.resources
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class ReferenceExtractionTests(unittest.TestCase):
    def test_installed_package_retains_entry_points_and_runtime_assets(self):
        distribution = importlib.metadata.distribution('mas-host')
        commands = {entry.name: entry.value for entry in distribution.entry_points}
        self.assertEqual(commands['mas-host'], 'mas_host.__main__:main')
        self.assertEqual(commands['mas-client'], 'mas_host.client_cli:main')
        package = importlib.resources.files('mas_host')
        for name in ('cursor_runtime.json', 'cursor_bridge_patch.json'):
            self.assertIsInstance(json.loads(package.joinpath(name).read_text()), dict)
        self.assertIn('configureCursorSdk', package.joinpath('cursor_transport_bootstrap.mjs').read_text())

    def test_installed_cli_help_works_outside_the_checkout_without_protocol_tools(self):
        environment = {key: value for key, value in os.environ.items()
                       if key not in ('PYTHONPATH', 'MAS_BIN', 'MAS_GATEWAY_BIN')}
        environment['PATH'] = ''
        with tempfile.TemporaryDirectory() as directory:
            for module in ('mas_host', 'mas_host.client_cli'):
                with self.subTest(module=module):
                    result = subprocess.run([sys.executable, '-I', '-m', module, '--help'],
                                            cwd=directory, env=environment,
                                            capture_output=True, text=True, timeout=30)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn('usage:', result.stdout)

    def test_protocol_pin_and_imported_reference_fixtures_are_present(self):
        self.assertRegex((ROOT / 'protocol-revision.txt').read_text(), r'^[0-9a-f]{40}\n$')
        for relative in ('examples/host/mesh.json', 'examples/workflows/structured-agents.json',
                         'examples/bridges/mixed.json', 'formal/authority-traces.json'):
            with self.subTest(path=relative):
                self.assertIsInstance(json.loads((ROOT / relative).read_text()), dict)
        self.assertFalse((ROOT / 'rust-protocol' / 'src').exists())
        self.assertFalse((ROOT / 'rust-protocol' / 'Cargo.toml').exists())


if __name__ == '__main__':
    unittest.main()

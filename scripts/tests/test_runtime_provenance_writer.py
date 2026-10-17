"""Build-time provenance uses repository inputs absent from installed runtimes."""
import hashlib
import importlib.util
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


REPOSITORY = Path(__file__).resolve().parents[2]


class ProvenanceWriterTests(unittest.TestCase):
    def test_build_writer_binds_generated_package_and_does_not_run_binary(self):
        # The candidate scripts suite also runs before mas_host is installed.
        # Load only the explicitly selected helper, as the build writer does.
        spec = importlib.util.spec_from_file_location('runtime_provenance_writer_test',
            REPOSITORY / 'host' / 'mas_host' / 'runtime_provenance.py')
        provenance = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(provenance)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            host = root / 'host'
            host.mkdir()
            shutil.copytree(REPOSITORY / 'host' / 'mas_host', host / 'mas_host',
                            ignore=shutil.ignore_patterns('__pycache__'))
            shutil.copyfile(REPOSITORY / 'host' / 'pyproject.toml', host / 'pyproject.toml')
            binary_dir = root / 'bin'
            binary_dir.mkdir()
            manifest = root / 'Cargo.toml'
            manifest.write_text('[package]\nversion = "0.1.0"\n')
            binaries = {name: {'sha256': hashlib.sha256(name.encode()).hexdigest()}
                        for name in ('mas', 'mas-gateway')}
            for name in binaries:
                # These are data files, not executable programs.
                (binary_dir / name).write_bytes(name.encode())
            subprocess.run([sys.executable, str(REPOSITORY / 'scripts' / 'write-runtime-provenance.py'),
                '--host', str(host), '--rust-manifest', str(manifest),
                '--binaries', str(binary_dir), '--source-commit', 'a' * 40, '--release-tag', 'v0.3.0'],
                check=True)
            result = provenance.build_record(host / 'mas_host', binaries)
            self.assertEqual(result['status'], 'content_bound')
            self.assertEqual(result['release_tag'], 'v0.3.0')
            self.assertEqual(result['source_commit'], 'a' * 40)
            self.assertTrue(all(result['binary_matches'].values()))


if __name__ == '__main__':
    unittest.main()

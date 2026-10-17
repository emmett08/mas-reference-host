"""Every extracted dependency profile must be audited completely."""
from contextlib import chdir, redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import re
import runpy
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]


class ReleaseAuditTests(unittest.TestCase):
    def audit(self, omit_object_storage=False):
        audited = set()

        def run(command):
            lock = Path(command[command.index('-r') + 1])
            audited.add(lock.name)
            dependencies = [{'name': name, 'version': version, 'vulns': []}
                            for name, version in re.findall(
                                r'^([a-z0-9-]+)==([^\s]+)', lock.read_text(), re.MULTILINE)]
            if omit_object_storage and lock.name == 'requirements-object-storage.txt':
                dependencies.pop()
            Path(command[command.index('--output') + 1]).write_text(
                json.dumps({'dependencies': dependencies}))
            return SimpleNamespace(returncode=0)

        with tempfile.TemporaryDirectory() as directory, chdir(directory), \
                patch.object(subprocess, 'run', side_effect=run), \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as result:
                runpy.run_path(str(ROOT / 'scripts/release-audit.py'), run_name='__main__')
        return result.exception.code, audited

    def test_all_committed_profiles_including_object_storage_are_audited(self):
        code, audited = self.audit()
        self.assertEqual(code, 0)
        self.assertEqual(audited, {path.name for path in (ROOT / 'host').glob('requirements-*.txt')})

    def test_incomplete_object_storage_report_fails_closed(self):
        code, _ = self.audit(omit_object_storage=True)
        self.assertEqual(code, 1)


if __name__ == '__main__':
    unittest.main()

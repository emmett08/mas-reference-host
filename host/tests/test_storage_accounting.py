"""Storage inventory reuse never reuses file sizes for an admission decision."""
import os
from pathlib import Path
import tempfile
import unittest

from mas_host.operations import DeploymentLedger, DeploymentPolicy, OperationsError
from mas_host.storage_accounting import StateSizeIndex, StorageAccountingError, state_size


class StorageAccountingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / 'nested').mkdir()
        self.file = self.root / 'nested' / 'sdk-output'
        self.file.write_bytes(b'abc')
        self.index = StateSizeIndex(self.root)

    def test_unchanged_inventory_reuses_names_but_growth_is_observed_immediately(self):
        self.assertEqual(self.index.estimate(), 3)
        self.assertEqual(self.index.last_observation['directory_listings'], 2)
        self.assertEqual(self.index.estimate(), 3)
        self.assertEqual(self.index.last_observation['directory_listings'], 0)
        self.file.write_bytes(b'abcdefghij')  # Directory mtime does not change.
        self.assertEqual(self.index.estimate(), 10)
        self.assertEqual(self.index.last_observation['directory_listings'], 0)
        self.assertEqual(self.index.last_observation['stat_calls'], 3)
        self.file.write_bytes(b'a')
        self.assertEqual(self.index.estimate(), 1)

    def test_add_remove_rename_and_directory_replacement_match_full_scan(self):
        self.index.estimate()
        for action in (
            lambda: (self.root / 'new').mkdir(),
            lambda: (self.root / 'new' / 'data').write_bytes(b'new'),
            lambda: self.file.rename(self.root / 'new' / 'moved'),
            lambda: (self.root / 'nested').rmdir(),
            lambda: (self.root / 'new' / 'data').unlink(),
        ):
            action()
            self.assertEqual(self.index.estimate(), state_size(self.root))

    def test_periodic_reconciliation_and_memory_fallback_keep_exact_counts(self):
        index = StateSizeIndex(self.root, reconcile_checks=1)
        self.assertEqual(index.estimate(), 3)
        index.estimate()
        self.assertFalse(index.last_observation['reconciled'])
        index.estimate()
        self.assertTrue(index.last_observation['reconciled'])
        bounded = StateSizeIndex(self.root, max_entries=1)
        for _ in range(3):
            self.assertEqual(bounded.estimate(), 3)
            self.assertFalse(bounded.last_observation['inventory_cached'])

    def test_cached_known_file_replaced_by_symlink_or_special_file_rejects(self):
        self.index.estimate()
        self.file.unlink()
        self.file.symlink_to(self.root / 'elsewhere')
        with self.assertRaisesRegex(StorageAccountingError, 'symlinks'):
            self.index.estimate()
        self.file.unlink()
        os.mkfifo(self.file)
        with self.assertRaisesRegex(StorageAccountingError, 'regular files only'):
            self.index.estimate()

    def test_restoring_directory_mtime_does_not_hide_new_entries(self):
        self.index.estimate()
        directory = self.root / 'nested'
        before = directory.stat()
        (directory / 'new').write_bytes(b'new bytes')
        os.utime(directory, ns=(before.st_atime_ns, before.st_mtime_ns))
        self.assertEqual(self.index.estimate(), 12)  # ctime invalidates inventory.

    def test_authoritative_measurement_enumerates_every_directory(self):
        self.index.estimate()
        self.index.estimate()
        self.assertEqual(self.index.last_observation['directory_listings'], 0)
        self.assertEqual(self.index.measure(), 3)
        self.assertEqual(self.index.last_observation['directory_listings'], 2)
        (self.root / 'nested' / 'new-sdk-output').write_bytes(b'new')
        self.assertEqual(self.index.measure(), 6)
        self.assertTrue(self.index.last_observation['reconciled'])

    def test_external_growth_closes_ledger_admission_after_cached_check(self):
        state = self.root / 'deployment'
        ledger = DeploymentLedger(state, DeploymentPolicy(max_state_bytes=200_000, min_free_bytes=1))
        output = state / 'sdk-output'
        output.write_bytes(b'initial')
        ledger.check_storage_admission()
        output.write_bytes(b'x' * 200_000)
        with self.assertRaisesRegex(OperationsError, 'state size limit'):
            ledger.check_storage_admission()

"""Incremental directory inventory with fresh file sizes at every admission.

Every authoritative measurement fully enumerates the tree and reads current
file sizes/types. A separate provisional estimate reuses directory inventories
with periodic reconciliation; coarse filesystem timestamps make that estimate
unsuitable for permitting admission. Neither path claims a filesystem quota.
"""
from __future__ import annotations

import os
from pathlib import Path
import stat
import threading
import time


class StorageAccountingError(RuntimeError):
    pass


class StateSizeIndex:
    """Process-local inventory; loss merely requires a complete reconciliation."""
    def __init__(self, root, *, reconcile_seconds=30, reconcile_checks=64, max_entries=100_000):
        self.root = os.fspath(Path(root).absolute())
        self.reconcile_seconds = reconcile_seconds
        self.reconcile_checks = reconcile_checks
        self.max_entries = max_entries
        self._pid = os.getpid()
        self._lock = threading.RLock()
        self._directories = {}
        self._sizes = {}
        self._total = 0
        self._checks = 0
        self._reconciled_at = 0.0
        self.last_observation = {}

    def measure(self):
        """Exact current tree walk; required before allowing any new admission."""
        return self._observe(True)

    def estimate(self):
        """Provisional observation only; never sufficient to permit admission."""
        return self._observe(False)

    def _observe(self, force):
        if self._pid != os.getpid():
            # A forked worker inherits no claim that another process's lock or
            # previously observed directory inventory remains valid.
            self._pid = os.getpid()
            self._lock = threading.RLock()
            force = True
        with self._lock:
            return self._measure(force)

    def _measure(self, force):
        started = time.monotonic()
        reconcile = (force or not self._directories or self._checks >= self.reconcile_checks
                     or started - self._reconciled_at >= self.reconcile_seconds)
        pending, directories, sizes = [self.root], {}, {}
        listings = stats = 0
        total = self._total
        while pending:
            path = pending.pop()
            try:
                details = os.stat(path, follow_symlinks=False)
                stats += 1
            except FileNotFoundError:
                # Transient WAL/journal files can disappear during any scan.
                continue
            mode = details.st_mode
            if stat.S_ISLNK(mode):
                raise StorageAccountingError('state tree must not contain symlinks')
            if stat.S_ISREG(mode):
                sizes[path] = details.st_size
                total += details.st_size - self._sizes.get(path, 0)
                continue
            if not stat.S_ISDIR(mode):
                raise StorageAccountingError('state tree must contain regular files only')
            identity = (details.st_dev, details.st_ino, details.st_mtime_ns, details.st_ctime_ns)
            cached = None if reconcile else self._directories.get(path)
            if cached is not None and cached[0] == identity:
                names = cached[1]
            else:
                try:
                    with os.scandir(path) as entries:
                        names = tuple(entry.name for entry in entries)
                    listings += 1
                except FileNotFoundError:
                    continue
            directories[path] = (identity, names)
            pending.extend(os.path.join(path, name) for name in names)
        total -= sum(size for path, size in self._sizes.items() if path not in sizes)
        # Bound cache memory independently of the deployment's byte threshold.
        # Overly many tiny files fall back to exact reconciliation every check.
        cacheable = len(directories) + len(sizes) <= self.max_entries
        self._directories = directories if cacheable else {}
        self._sizes = sizes if cacheable else {}
        self._total = total if cacheable else 0
        if reconcile:
            self._reconciled_at, self._checks = started, 0
        else:
            self._checks += 1
        self.last_observation = {'bytes': total, 'files': len(sizes), 'directories': len(directories),
                                 'directory_listings': listings, 'stat_calls': stats,
                                 'reconciled': reconcile, 'inventory_cached': cacheable}
        return total


def state_size(path):
    """Uncached full reconciliation for maintenance and independent checking."""
    return StateSizeIndex(path).measure()

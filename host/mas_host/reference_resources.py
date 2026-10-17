"""Linux resource observations for the private reference-load qualification.

Samples are observations, not resource admission. RSS is a sum of resident pages
(shared pages can be counted repeatedly); short-lived peaks can occur between
samples. Process start ticks prevent PID reuse from being mistaken for a leak.
"""
from pathlib import Path
import os
import stat


def process_stat(text):
    left, right = text.index('('), text.rindex(')')
    fields = text[right + 2:].split()
    return {'pid': int(text[:left]), 'ppid': int(fields[1]),
            'start_ticks': int(fields[19]), 'rss_pages': max(0, int(fields[21])),
            'threads': int(fields[17])}


class ProcessTreeSampler:
    def __init__(self, proc=Path('/proc')):
        self.proc = Path(proc)
        # /proc may be mounted in an ancestor PID namespace. Do not interpolate
        # os.getpid(): /proc/self/stat identifies the matching visible root PID.
        root = process_stat((self.proc / 'self/stat').read_text())
        self.root = (root['pid'], root['start_ticks'])
        self.seen = set()
        self.page_size = os.sysconf('SC_PAGE_SIZE')

    def sample(self):
        records = {}
        for path in self.proc.iterdir():
            if not path.name.isdigit():
                continue
            try:
                item = process_stat((path / 'stat').read_text())
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                continue
            records[item['pid']] = item
        root = records.get(self.root[0])
        if root is None or root['start_ticks'] != self.root[1]:
            raise RuntimeError('reference observer process identity unavailable')
        descendants, frontier = set(), {self.root[0]}
        while frontier:
            children = {pid for pid, row in records.items() if row['ppid'] in frontier}
            frontier = children - descendants - {self.root[0]}
            descendants.update(frontier)
        identities = {(pid, records[pid]['start_ticks']) for pid in descendants}
        # Previously observed owned descendants remain monitored after orphaning.
        retained = {(pid, ticks) for pid, ticks in self.seen
                    if pid in records and records[pid]['start_ticks'] == ticks}
        alive = identities | retained
        self.seen |= identities
        return {'observer_rss_bytes': root['rss_pages'] * self.page_size,
                'descendant_rss_bytes': sum(records[pid]['rss_pages'] for pid, _ in alive) * self.page_size,
                'descendant_processes': len(alive),
                'descendant_threads': sum(records[pid]['threads'] for pid, _ in alive),
                'observed_orphans': len(retained - identities),
                'distinct_descendants_observed': len(self.seen)}


def storage_sample(root):
    apparent = allocated = files = 0
    pending = [Path(root)]
    while pending:
        for path in pending.pop().iterdir():
            try:
                item = path.lstat()
            except FileNotFoundError:
                continue  # SQLite may have removed a transient journal.
            if stat.S_ISDIR(item.st_mode):
                pending.append(path)
            elif stat.S_ISREG(item.st_mode):
                apparent += item.st_size
                allocated += item.st_blocks * 512
                files += 1
            else:
                raise RuntimeError('unsafe file in reference state')
    return {'apparent_bytes': apparent, 'allocated_bytes': allocated, 'files': files}


def distribution(values):
    ordered = sorted(values)
    def rank(percent):
        return ordered[min(len(ordered) - 1, (len(ordered) * percent + 99) // 100 - 1)] if ordered else None
    return {'samples': len(ordered), 'minimum': ordered[0] if ordered else None,
            'p50': rank(50), 'p95': rank(95), 'p99': rank(99),
            'maximum': ordered[-1] if ordered else None,
            'quantile_method': 'nearest_rank'}

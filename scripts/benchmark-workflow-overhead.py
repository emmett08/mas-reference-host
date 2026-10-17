#!/usr/bin/env python3
"""Bounded completed-prefix and storage-inventory comparison without providers."""
import argparse
import json
import os
import platform
import stat
from pathlib import Path
import statistics
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'host'))
from mas_host.storage_accounting import StateSizeIndex
from mas_host.workflow_evaluator import PlanEvaluator
from mas_host.workflow_store import WorkflowStore


class Journal:
    def __init__(self, store, enabled):
        self.store, self.visits = store, 0
        self.progress = store.progress if enabled else None
        self.checkpoint = store.checkpoint if enabled else None

    def node(self, *args):
        self.visits += 1
        return self.store.node(*args)

    def attempt(self, instance, path, workflow, input, deadline):
        return self.store.attempt(instance, path, workflow, input, deadline,
                                  identity='fixture/1', wall_seconds=300)

    def finish_node(self, *args, **kwargs):
        return self.store.finish_node(*args, **kwargs)


def summary(samples):
    return {'median_ms': statistics.median(samples), 'p95_ms': sorted(samples)[int(.95 * (len(samples)-1))],
            'total_ms': sum(samples), 'samples': len(samples)}


def workflow(prefix, samples, enabled):
    with tempfile.TemporaryDirectory() as folder:
        store = WorkflowStore(Path(folder))
        with store.transaction() as db:
            instance = store.create_instance(db, 'owner', 'request', 'benchmark', 'fixture/1', {},
                                             wall_seconds=300, maximum=1)
        journal = Journal(store, enabled)
        node = {'kind': 'sequence', 'steps': [{'kind': 'task', 'workflow': 'leaf'}] * (prefix + 1)}
        limits = {'max_steps': prefix + 2}
        evaluator = PlanEvaluator({}, journal)
        def advance():
            ready = []
            evaluator.advance(instance, 'root', node, {}, limits, time.time() + 300, ready)
            return ready
        for _ in range(prefix):
            ready = advance()
            store.save_attempt(ready[0], 'completed', {})
        advance()
        journal.visits = 0
        timings = []
        for _ in range(samples):
            started = time.perf_counter_ns()
            advance()
            timings.append((time.perf_counter_ns() - started) / 1e6)
        return {'strategy': 'durable_prefix' if enabled else 'completed_node_replay',
                'completed_prefix': prefix, 'node_reads': journal.visits, **summary(timings)}


def legacy_size(root):
    total = 0
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in [*directories, *files]:
            info = (Path(directory) / name).lstat()
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
    return total


def storage(files, samples, strategy):
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        for index in range(files):
            directory = root / str(index // 10)
            directory.mkdir(exist_ok=True)
            (directory / str(index)).write_bytes(b'x' * 64)
        index = StateSizeIndex(root, reconcile_checks=samples + 1, reconcile_seconds=300)
        index.measure()
        timings, listings, stats = [], 0, 0
        for sample in range(samples):
            # File growth is external to the index and must be visible at once.
            (root / '0' / '0').write_bytes(b'x' * (64 + sample))
            started = time.perf_counter_ns()
            actual = (legacy_size(root) if strategy == 'legacy_walk' else
                      index.measure() if strategy == 'exact_index' else index.estimate())
            timings.append((time.perf_counter_ns() - started) / 1e6)
            assert actual == files * 64 + sample
            listings += (files + 9) // 10 + 1 if strategy == 'legacy_walk' else index.last_observation['directory_listings']
            stats += files + (files + 9) // 10 if strategy == 'legacy_walk' else index.last_observation['stat_calls']
        return {'strategy': strategy, 'admission_eligible': strategy != 'inventory_estimate',
                'files': files, 'directory_listings': listings, 'fresh_stat_calls': stats, **summary(timings)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--samples', type=int, default=30)
    parser.add_argument('--prefix', type=int, default=100)
    parser.add_argument('--files', type=int, default=1000)
    parser.add_argument('--section', choices=('all', 'workflow', 'storage'), default='all')
    args = parser.parse_args()
    if not (5 <= args.samples <= 1000 and 1 <= args.prefix <= 1000 and 10 <= args.files <= 10000):
        parser.error('samples 5..1000, prefix 1..1000 and files 10..10000 required')
    print(json.dumps({'format': 'mas-workflow-overhead-benchmark/1',
        'runtime': {'python': platform.python_version(), 'platform': platform.platform(), 'logical_cpus': os.cpu_count()},
        'workload': 'Retained sequential prefix; existing-file growth under unchanged directory inventories',
        'workflow': [workflow(args.prefix, args.samples, enabled) for enabled in (False, True)] if args.section != 'storage' else [],
        'storage': [storage(args.files, args.samples, strategy) for strategy in ('legacy_walk', 'exact_index', 'inventory_estimate')] if args.section != 'workflow' else []}, indent=2))


if __name__ == '__main__':
    main()

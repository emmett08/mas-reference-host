"""Bounded, no-paid-provider reference exposure; not a production SLO."""
from dataclasses import asdict, dataclass
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import time

from .bridge_config import build_service
from .bridge_health import worker_health
from .reference_resources import ProcessTreeSampler, distribution, storage_sample
from .scheduler_capacity import SchedulerCapacity, SchedulerPolicy


@dataclass(frozen=True)
class Profile:
    name: str = 'local-leaf-two-principals/1'
    minimum_load_seconds: int = 60
    maximum_load_seconds: int = 180
    minimum_completed: int = 100
    maximum_submissions: int = 256
    submissions_per_second: int = 2
    outstanding_per_principal: int = 8
    workers: int = 4
    workers_per_principal: int = 2
    sample_interval_ms: int = 100
    maximum_drain_seconds: int = 60
    maximum_cleanup_seconds: int = 30
    maximum_descendant_processes: int = 64
    maximum_descendant_rss_bytes: int = 2 * 1024**3
    maximum_observer_growth_bytes: int = 64 * 1024**2
    maximum_state_base_bytes: int = 16 * 1024**2
    maximum_state_bytes_per_run: int = 2 * 1024**2
    maximum_control_p99_seconds: int = 2
    maximum_completion_p99_seconds: int = 60


SMOKE = Profile(name='local-leaf-smoke/1', minimum_load_seconds=5, maximum_load_seconds=30,
                minimum_completed=4, maximum_submissions=16)


def capacity_checks(root, profile):
    """Exercise the real lock-backed admission owner with deterministic inputs."""
    root.mkdir(mode=0o700)
    capacity = SchedulerCapacity(root, SchedulerPolicy(profile.workers, profile.workers_per_principal))
    leases = []
    checks = {}
    try:
        for principal in ('alice', 'bob'):
            for index in range(2):
                leases.append(capacity.acquire(f'{principal}-{index}', principal, 'reference'))
        checks['four_slots_admitted'] = all(lease is not None for lease in leases)
        denied = capacity.acquire('overflow', 'carol', 'reference')
        checks['global_capacity_denied'] = denied is None
        if denied is not None:
            denied.close()
        leases[-1].close()
        denied = capacity.acquire('alice-overflow', 'alice', 'reference')
        checks['principal_capacity_denied'] = denied is None
        if denied is not None:
            denied.close()
        rows = [{'principal': owner, 'number': number} for owner in ('alice', 'bob') for number in range(4)]
        ordered = capacity.fair_order(rows)
        checks['round_robin_fifo'] = (all(ordered[i]['principal'] != ordered[i + 1]['principal']
                                         for i in range(len(ordered) - 1))
            and all([row['number'] for row in ordered if row['principal'] == owner] == list(range(4))
                    for owner in ('alice', 'bob')))
    finally:
        for lease in leases:
            if lease is not None:
                lease.close()
    resumed = capacity.acquire('released', 'alice', 'reference')
    checks['released_capacity_reusable'] = resumed is not None
    if resumed is not None:
        resumed.close()
    return checks


def _hash(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def provenance(repository, expected_commit=None, smoke=False):
    def git(*args):
        return subprocess.check_output(['git', '-C', str(repository), *args], text=True).strip()
    revision = git('rev-parse', 'HEAD')
    clean = not git('status', '--porcelain')
    if expected_commit is not None and revision != expected_commit:
        raise ValueError('reference source revision differs from expected commit')
    if not smoke and not clean:
        raise ValueError('qualification requires a clean checkout; use --smoke for development')
    if not smoke and Path(__file__).resolve().parents[2] != Path(repository).resolve():
        raise ValueError('qualification must import the measured checkout')
    binaries = {}
    for variable, fallback in (('MAS_BIN', 'mas'), ('MAS_GATEWAY_BIN', 'mas-gateway')):
        binary = shutil.which(os.environ.get(variable, fallback))
        if binary is None:
            raise ValueError(f'{variable} binary unavailable')
        binaries[variable] = {'sha256': _hash(binary), 'bytes': Path(binary).stat().st_size}
    return {'source_commit': revision, 'source_clean': clean,
            'expected_commit_checked': expected_commit is not None, 'binaries': binaries,
            'python': sys.version, 'platform': platform.platform(),
            'cpu_affinity_count': len(os.sched_getaffinity(0)), 'cpu_count': os.cpu_count(),
            'page_size': os.sysconf('SC_PAGE_SIZE'),
            'visible_system_memory_bytes': os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGE_SIZE')}


@contextmanager
def _fixture():
    previous = os.environ.get('MAS_REFERENCE_EVIDENCE')
    try:
        with tempfile.TemporaryDirectory(prefix='mas-reference-') as directory:
            root = Path(directory)
            os.environ['MAS_REFERENCE_EVIDENCE'] = str(root / 'effects.jsonl')
            yield root
    finally:
        if previous is None:
            os.environ.pop('MAS_REFERENCE_EVIDENCE', None)
        else:
            os.environ['MAS_REFERENCE_EVIDENCE'] = previous


def trace_metrics(path, jobs, profile):
    records = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
    starts, ends, active = {}, {}, {}
    peak = 0
    principal_peak = {'alice': 0, 'bob': 0}
    valid = True
    for record in sorted(records, key=lambda row: row['time_ns']):
        key, principal = record['key'], record['principal']
        if key not in jobs or jobs[key]['principal'] != principal:
            valid = False
            continue
        if record['kind'] == 'start':
            valid &= key not in starts
            starts[key] = record['time_ns'] / 1e9
            active[key] = principal
        elif record['kind'] == 'end':
            valid &= key in active and key not in ends
            ends[key] = record['time_ns'] / 1e9
            active.pop(key, None)
        else:
            valid = False
        peak = max(peak, len(active))
        for owner in principal_peak:
            principal_peak[owner] = max(principal_peak[owner], sum(p == owner for p in active.values()))
    valid &= not active and set(starts) == set(ends) == set(jobs)
    wait, before, overhead = [], [], []
    for key in starts.keys() & ends.keys():
        duration = ends[key] - starts[key]
        wait.append(duration)
        before.append(starts[key] - jobs[key]['submitted'])
        if 'completed' in jobs[key]:
            overhead.append(jobs[key]['completed'] - jobs[key]['submitted'] - duration)
    return {'valid_exact_once_pairs': bool(valid), 'peak_active_callbacks': peak,
            'peak_active_callbacks_by_principal': principal_peak,
            'capacity_observation_within_policy': peak <= profile.workers and all(
                value <= profile.workers_per_principal for value in principal_peak.values()),
            'simulated_provider_wait_seconds': distribution(wait),
            'submission_to_callback_start_seconds': distribution(before),
            'submission_to_callback_start_seconds_by_principal': {owner: distribution([
                starts[key] - jobs[key]['submitted'] for key in starts if jobs[key]['principal'] == owner])
                for owner in principal_peak},
            'completion_minus_callback_wait_seconds': distribution(overhead)}


def run_reference(repository, *, smoke=False, expected_commit=None):
    profile = SMOKE if smoke else Profile()
    report = {'format': 'mas-reference-load/1', 'qualification': not smoke,
              'profile': asdict(profile), 'provenance': provenance(repository, expected_commit, smoke),
              'assumptions': ['Local Linux single-host authority with durable SQLite and real Rust binaries.',
                  'Two principals submit equal single-call leaves at a combined rate at most two per second.',
                  'Each callback waits 100 ms locally and reserves zero micro-US dollars; no network provider.',
                  'Cold task execution is included; dispatcher startup is reported separately.',
                  'Process/RSS samples can miss peaks between 100 ms observations.',
                  'RSS sums shared pages repeatedly; the observer and its descendants are reported separately.',
                  'State retains completed runs; bounded linear growth is expected and is not a memory leak.',
                  'Fixed regression ceilings qualify this fixture only, not production latency or capacity.',
                  'Separate gates cover fault recovery, parallel compositions, transport and HA.']}
    print(json.dumps({'reference_profile_before_measurement': report}, sort_keys=True), file=sys.stderr, flush=True)
    with _fixture() as root:
        report['deterministic_capacity_checks'] = capacity_checks(root / 'capacity-check', profile)
        state, evidence = root / 'state', root / 'effects.jsonl'
        config = {'version': 1, 'factory': 'mas_host.reference_workload:workflows',
                  'grants': {'alice': ['reference'], 'bob': ['reference']},
                  'scheduler': {'max_workers': profile.workers, 'max_per_principal': profile.workers_per_principal},
                  'max_pending': 32, 'max_tasks': 1000}
        config_path = root / 'config.json'
        config_path.write_text(json.dumps(config))
        service = build_service(config, state)
        initial_health = service.operations.health()
        # No policy amendment or budget increase: these are fresh, default limits.
        if profile.maximum_submissions > min(initial_health['policy']['max_runs'], initial_health['policy']['max_calls']):
            raise ValueError('reference run exceeds the frozen deployment limits')
        report['deployment_policy'] = initial_health['policy']
        report['initial_filesystem_available_bytes'] = initial_health['free_bytes']
        sampler = ProcessTreeSampler()
        baseline = {**sampler.sample(), **storage_sample(state)}
        samples, jobs, outstanding, admissions, inspections = [], {}, {}, [], []
        failures = []
        completed_by_owner = {'alice': 0, 'bob': 0}
        begun = time.monotonic()
        last_storage, last_storage_at = storage_sample(state), begun
        worker_log = (root / 'worker.log').open('wb')
        worker = subprocess.Popen([sys.executable, '-m', 'mas_host', 'bridge', 'worker',
            '--config', str(config_path), '--state-dir', str(state), '--interval', '0.02',
            '--drain-seconds', '5'], stdout=subprocess.DEVNULL, stderr=worker_log)
        ready_deadline = begun + 30
        load_started = next_admission = time.monotonic()
        load_finished = None
        next_owner = 0
        try:
            while True:
                now = time.monotonic()
                if worker.poll() is not None:
                    failures.append('dispatcher_exited')
                    break
                try:
                    ready = worker_health(state)['worker_ready']
                except (OSError, ValueError, RuntimeError):
                    ready = False
                if ready:
                    break
                if now >= ready_deadline:
                    failures.append('dispatcher_startup_timeout')
                    break
                time.sleep(0.05)
            load_started = next_admission = time.monotonic()
            report['dispatcher_startup_seconds'] = load_started - begun
            while not failures:
                now = time.monotonic()
                elapsed = now - load_started
                if worker.poll() is not None:
                    failures.append('dispatcher_exited')
                    break
                for key, item in tuple(outstanding.items()):
                    call = time.monotonic()
                    receipt = service.inspect(item['principal'], item['task_id'])
                    inspections.append(time.monotonic() - call)
                    if receipt['state'] == 'completed':
                        if receipt['result'] != {'key': key}:
                            failures.append('unexpected_result')
                        item['completed'] = time.monotonic()
                        completed_by_owner[item['principal']] += 1
                        del outstanding[key]
                    elif receipt['state'] not in {'queued', 'running'}:
                        failures.append('unexpected_task_state_' + receipt['state'])
                if load_finished is None:
                    if elapsed >= profile.minimum_load_seconds and sum(completed_by_owner.values()) >= profile.minimum_completed:
                        load_finished = time.monotonic()
                    elif elapsed >= profile.maximum_load_seconds:
                        failures.append('maximum_load_window_reached')
                    elif len(jobs) >= profile.maximum_submissions:
                        # Do not idle to manufacture a minimum soak duration.
                        failures.append('maximum_submissions_reached_before_exposure')
                    elif now >= next_admission:
                        for offset in range(2):
                            owner_index = (next_owner + offset) % 2
                            owner = ('alice', 'bob')[owner_index]
                            if sum(item['principal'] == owner for item in outstanding.values()) >= profile.outstanding_per_principal:
                                continue
                            key = f'reference-{len(jobs):04d}'
                            submitted = time.monotonic()
                            receipt = service.submit(owner, key, 'reference', {'principal': owner, 'key': key})
                            admissions.append(time.monotonic() - submitted)
                            item = {'principal': owner, 'task_id': receipt['task_id'], 'submitted': submitted}
                            jobs[key] = outstanding[key] = item
                            next_owner = (owner_index + 1) % 2
                            next_admission = time.monotonic() + 1 / profile.submissions_per_second
                            break
                elif not outstanding:
                    break
                elif now - load_finished > profile.maximum_drain_seconds:
                    failures.append('drain_timeout')
                if now - last_storage_at >= 1:
                    last_storage, last_storage_at = storage_sample(state), now
                sample = {'elapsed_seconds': now - load_started, **sampler.sample(), **last_storage}
                samples.append(sample)
                if sample['descendant_processes'] > profile.maximum_descendant_processes:
                    failures.append('process_ceiling_exceeded')
                if sample['descendant_rss_bytes'] > profile.maximum_descendant_rss_bytes:
                    failures.append('rss_ceiling_exceeded')
                time.sleep(profile.sample_interval_ms / 1000)
        finally:
            finished = time.monotonic()
            cleanup_deadline = finished + profile.maximum_cleanup_seconds
            worker.terminate() if worker.poll() is None else None
            try:
                worker.wait(timeout=profile.maximum_cleanup_seconds)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=5)
                failures.append('dispatcher_cleanup_timeout')
            worker_log.close()
            while True:
                final_process = sampler.sample()
                if not final_process['descendant_processes'] or time.monotonic() >= cleanup_deadline:
                    break
                time.sleep(0.1)
            cleanup_seconds = time.monotonic() - finished
        final_storage = storage_sample(state)
        final_health = service.operations.health()
        completion = distribution([item['completed'] - item['submitted'] for item in jobs.values() if 'completed' in item])
        admission, inspection = distribution(admissions), distribution(inspections)
        trace = trace_metrics(evidence, jobs, profile)
        final_provenance = provenance(repository, smoke=True)
        report['final_provenance'] = final_provenance
        resource_peaks = {key: max([baseline[key], final_process.get(key, 0)] + [row[key] for row in samples])
                          for key in baseline if key in final_process}
        growth = {key: final_storage[key] - baseline[key] for key in final_storage}
        checks = {**report['deterministic_capacity_checks'],
            'source_and_binaries_stable': all(final_provenance[key] == report['provenance'][key]
                                             for key in ('source_commit', 'source_clean', 'binaries')),
            'minimum_active_load_duration': (load_finished or finished) - load_started >= profile.minimum_load_seconds,
            'minimum_completed_samples': sum(completed_by_owner.values()) >= profile.minimum_completed,
            'all_submissions_completed': len(jobs) == sum(completed_by_owner.values()),
            'both_principals_progressed': min(completed_by_owner.values()) >= profile.minimum_completed // 4,
            'effect_trace_exact_once': trace['valid_exact_once_pairs'],
            'observed_callback_capacity': trace['capacity_observation_within_policy'],
            'process_peak_within_limit': resource_peaks['descendant_processes'] <= profile.maximum_descendant_processes,
            'rss_peak_within_limit': resource_peaks['descendant_rss_bytes'] <= profile.maximum_descendant_rss_bytes,
            'observer_growth_within_limit': final_process['observer_rss_bytes'] - baseline['observer_rss_bytes'] <= profile.maximum_observer_growth_bytes,
            'all_observed_descendants_reaped': final_process['descendant_processes'] == 0,
            'cleanup_within_fixture_bound': cleanup_seconds <= profile.maximum_cleanup_seconds + 5,
            'retained_state_growth_within_limit': growth['apparent_bytes'] <= profile.maximum_state_base_bytes + len(jobs) * profile.maximum_state_bytes_per_run,
            'state_below_deployment_ceiling': final_storage['apparent_bytes'] < initial_health['policy']['max_state_bytes'],
            'no_paid_reservations': final_health['usage']['reserved_microusd'] == 0,
            'one_admission_and_call_per_submission': final_health['usage']['runs'] == final_health['usage']['calls'] == len(jobs),
            'control_p99_within_fixture_ceiling': all((row['p99'] or float('inf')) <= profile.maximum_control_p99_seconds for row in (admission, inspection)),
            'completion_p99_within_fixture_ceiling': (completion['p99'] or float('inf')) <= profile.maximum_completion_p99_seconds}
        report.update(passed=not failures and all(checks.values()), failures=failures, checks=checks,
            submitted=len(jobs), completed_by_principal=completed_by_owner,
            active_load_seconds=(load_finished or finished) - load_started,
            elapsed_to_drain_seconds=finished - load_started, cleanup_seconds=cleanup_seconds,
            completed_per_second=sum(completed_by_owner.values()) / max(finished - load_started, 0.001),
            latency_seconds={'submit': admission, 'inspect': inspection, 'submission_to_completion_observation': completion},
            callbacks=trace, resource_baseline=baseline, resource_peaks=resource_peaks,
            final_processes=final_process, retained_state=final_storage, retained_state_growth=growth,
            deployment_usage=final_health['usage'], samples=samples)
    return report

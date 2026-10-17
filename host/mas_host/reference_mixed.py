"""Bounded contention/history qualification using the real local host lifecycle.

All limits below are fixed before measurement. The callback is deliberately
local; this is implementation evidence, not a network-provider performance SLO.
"""
from dataclasses import asdict, dataclass
import json
import subprocess
import sys
import time

from .bridge_config import build_service
from .bridge_health import worker_health
from .reference_load import _fixture, provenance, trace_metrics
from .reference_resources import ProcessTreeSampler, distribution, storage_sample
from .service_contracts import ServiceError


@dataclass(frozen=True)
class MixedProfile:
    name: str = 'local-mixed-retained-history/1'
    retained_cancelled: int = 1024
    retained_completed: int = 32
    mixed_rounds: int = 4
    tasks_per_round: int = 16
    workers: int = 4
    workers_per_principal: int = 2
    max_pending: int = 16
    max_tasks: int = 2048
    paused_hold_seconds: float = 3
    maximum_saturation_seconds: int = 30
    maximum_phase_seconds: int = 180
    maximum_cleanup_seconds: int = 30
    sample_interval_ms: int = 100
    maximum_control_p99_seconds: int = 2
    maximum_completion_p99_seconds: int = 60
    maximum_descendant_processes: int = 64
    maximum_descendant_rss_bytes: int = 2 * 1024**3
    maximum_observer_growth_bytes: int = 64 * 1024**2
    maximum_state_bytes: int = 512 * 1024**2


MIXED_SMOKE = MixedProfile(name='local-mixed-smoke/1', retained_cancelled=16,
                           retained_completed=4, mixed_rounds=2)


def _records(path):
    # Writers take a lock; readers must do so too, avoiding a torn last record.
    import fcntl
    if not path.exists():
        return []
    with path.open() as stream:
        fcntl.flock(stream, fcntl.LOCK_SH)
        return [json.loads(line) for line in stream]


def latency_groups(jobs):
    """Keep principal and workload-class distributions separately inspectable."""
    result = {}
    for field in ('principal', 'class'):
        result[field] = {value: distribution([item['completed'] - item['submitted']
            for item in jobs.values() if item.get(field) == value and 'completed' in item])
            for value in sorted({item[field] for item in jobs.values()})}
    return result


def progress_during_pause(records, jobs):
    """Callback timestamps exclude delayed observer polls as false progress."""
    return any(row['kind'] == 'end' and row['key'] != key
               and item['paused_at'] < row['time_ns'] / 1e9 < item['resumed_at']
               for key, item in jobs.items() if 'paused_at' in item and 'resumed_at' in item
               for row in records)


class MixedExposure:
    """Observer only: admission, continuation and execution use public owners."""
    def __init__(self, service, root, profile, sampler):
        self.service, self.root, self.profile, self.sampler = service, root, profile, sampler
        self.jobs, self.samples, self.admissions, self.inspections = {}, [], [], []
        self.started = time.monotonic()
        self.last_storage = storage_sample(service.state_dir)
        self.last_storage_at = self.started
        self.worker = None

    def sample(self):
        stamp = time.monotonic()
        if stamp - self.last_storage_at >= 1:
            self.last_storage, self.last_storage_at = storage_sample(self.service.state_dir), stamp
        row = {'elapsed_seconds': stamp - self.started, **self.sampler.sample(), **self.last_storage}
        self.samples.append(row)
        if (row['descendant_processes'] > self.profile.maximum_descendant_processes
                or row['descendant_rss_bytes'] > self.profile.maximum_descendant_rss_bytes):
            raise RuntimeError('resource_ceiling_exceeded')

    def submit(self, principal, kind, phase):
        key = f'mixed-{len(self.jobs):04d}'
        started = time.monotonic()
        receipt = self.service.submit(principal, key, 'mixed',
                                      {'principal': principal, 'key': key, 'class': kind})
        self.admissions.append(time.monotonic() - started)
        self.jobs[key] = {'principal': principal, 'class': kind, 'phase': phase,
                          'task_id': receipt['task_id'], 'submitted': started}
        return key

    def poll(self):
        if self.worker is not None and self.worker.poll() is not None:
            raise RuntimeError('dispatcher_exited')
        for key, item in self.jobs.items():
            if 'completed' in item:
                continue
            started = time.monotonic()
            receipt = self.service.inspect(item['principal'], item['task_id'])
            self.inspections.append(time.monotonic() - started)
            if receipt['state'] == 'completed':
                if receipt['result'] != {'key': key}:
                    raise RuntimeError('unexpected_result')
                item['completed'] = time.monotonic()
            elif receipt['state'] == 'input_required':
                if item['class'] != 'paused':
                    raise RuntimeError('unexpected_pause')
                item.setdefault('paused_at', time.monotonic())
                if time.monotonic() - item['paused_at'] >= self.profile.paused_hold_seconds:
                    resume_requested = time.monotonic()
                    self.service.update(item['principal'], item['task_id'], key + '-continue', {'resumed': True})
                    # End the observed pause before update can commit or dispatch.
                    item['resumed_at'] = resume_requested
            elif receipt['state'] not in {'queued', 'running'}:
                raise RuntimeError('unexpected_task_state_' + receipt['state'])
        self.sample()

    def wait(self, predicate, *, timeout=None):
        deadline = time.monotonic() + (timeout or self.profile.maximum_phase_seconds)
        while not predicate():
            if time.monotonic() >= deadline:
                raise RuntimeError('phase_timeout')
            self.poll()
            time.sleep(self.profile.sample_interval_ms / 1000)

    def drain(self):
        self.wait(lambda: all('completed' in item for item in self.jobs.values()))


def _seed_cancelled(service, profile, exposure):
    """Retain genuine public-API receipts, requests, executions and events."""
    started = time.monotonic()
    for index in range(profile.retained_cancelled):
        principal = ('alice', 'bob')[index % 2]
        receipt = service.submit(principal, f'history-{index:04d}', 'mixed',
                                 {'principal': principal, 'key': f'history-{index:04d}', 'class': 'history'})
        if service.cancel(principal, receipt['task_id'])['state'] != 'cancelled':
            raise RuntimeError('history_cancel_failed')
        if index % 16 == 0:
            exposure.sample()
        if time.monotonic() - started > profile.maximum_phase_seconds:
            raise RuntimeError('history_seed_timeout')
    return time.monotonic() - started


def _history_counts(service):
    with service.store.transaction(readonly=True) as db:
        return {row['state']: row['count'] for row in db.execute(
            'SELECT state,count(*) AS count FROM tasks GROUP BY state')}


def run_mixed_reference(repository, *, smoke=False, expected_commit=None):
    profile = MIXED_SMOKE if smoke else MixedProfile()
    report = {'format': 'mas-reference-mixed/1', 'qualification': not smoke,
              'profile': asdict(profile), 'provenance': provenance(repository, expected_commit, smoke),
              'assumptions': [
                  'Local Linux, durable SQLite, real Rust binaries and supervised dispatcher.',
                  'No paid calls: local callbacks wait 50 ms (short), 2 s (long), or on an observer-released saturation barrier.',
                  'Paused work enters input_required and is continued through the public service after at least 3 s.',
                  'Retained history includes 1024 cancelled task receipts and 32 completed real executions in the full profile.',
                  'Cancelled receipts exercise task/request/event history, not retained run-directory capacity.',
                  'Four rounds of 16 tasks have an equal mixture per principal; the first holds all four slots before overload.',
                  'History preparation and dispatcher startup are separately reported; samples include both.',
                  'Both principals must finish every round; this establishes bounded progress, not general scheduler fairness.',
                  'Resource sampling can miss between-sample peaks; summed RSS double-counts shared pages.',
                  'The companion leaf profile supplies minimum-duration exposure; this profile supplies contention and lifecycle diversity.',
                  'Regression ceilings qualify this local fixture only, not production capacity or external-provider latency.']}
    print(json.dumps({'reference_profile_before_measurement': report}, sort_keys=True), file=sys.stderr, flush=True)
    with _fixture() as root:
        state = root / 'state'
        config = {'version': 1, 'factory': 'mas_host.reference_workload:mixed_workflows',
                  'grants': {'alice': ['mixed'], 'bob': ['mixed']},
                  'scheduler': {'max_workers': profile.workers, 'max_per_principal': profile.workers_per_principal},
                  'max_pending': profile.max_pending, 'max_tasks': profile.max_tasks}
        config_path = root / 'config.json'
        config_path.write_text(json.dumps(config))
        service = build_service(config, state)
        policy = service.operations.health()['policy']
        # Continuations consume fresh runs; the callback-free initial pause does not consume a call.
        planned_runs = profile.retained_completed + profile.mixed_rounds * 20
        if planned_runs > policy['max_runs'] or planned_runs > policy['max_calls']:
            raise ValueError('mixed reference exceeds the frozen default deployment limits')
        report['deployment_policy'] = policy
        sampler = ProcessTreeSampler()
        baseline = {**sampler.sample(), **storage_sample(state)}
        exposure = MixedExposure(service, root, profile, sampler)
        worker = None
        failures, checks, rounds = [], {}, []
        worker_log = (root / 'worker.log').open('wb')
        try:
            report['history_seed_seconds'] = _seed_cancelled(service, profile, exposure)
            started = time.monotonic()
            worker = subprocess.Popen([sys.executable, '-m', 'mas_host', 'bridge', 'worker',
                '--config', str(config_path), '--state-dir', str(state), '--interval', '0.02',
                '--drain-seconds', '5'], stdout=subprocess.DEVNULL, stderr=worker_log)
            exposure.worker = worker
            def ready():
                try:
                    return worker_health(state)['worker_ready']
                except (OSError, ValueError, RuntimeError):
                    return False
            exposure.wait(ready, timeout=30)
            report['dispatcher_startup_seconds'] = time.monotonic() - started
            completed_history_started = time.monotonic()
            for offset in range(0, profile.retained_completed, profile.max_pending):
                for index in range(offset, min(offset + profile.max_pending, profile.retained_completed)):
                    exposure.submit(('alice', 'bob')[index % 2], 'history', 'history')
                exposure.drain()
            report['completed_history_seconds'] = time.monotonic() - completed_history_started
            report['history_before_mixed_load'] = _history_counts(service)
            mixed_started = time.monotonic()
            for round_index in range(profile.mixed_rounds):
                round_started = time.monotonic()
                before = set(exposure.jobs)
                if round_index == 0:
                    for index in range(4):
                        exposure.submit(('alice', 'bob')[index % 2], 'gate', 'mixed')
                    exposure.wait(lambda: len([row for row in _records(root / 'effects.jsonl')
                        if row.get('class') == 'gate' and row['kind'] == 'start']) == profile.workers,
                        timeout=profile.maximum_saturation_seconds)
                    checks['four_callbacks_overlap_before_release'] = not any(
                        row.get('class') == 'gate' and row['kind'] == 'end' for row in _records(root / 'effects.jsonl'))
                    kinds = ['paused', 'paused', 'short', 'short', 'long', 'long'] * 2
                else:
                    kinds = ['paused', 'paused', 'short', 'short', 'long', 'long', 'short', 'short'] * 2
                for index, kind in enumerate(kinds):
                    exposure.submit(('alice', 'bob')[index % 2], kind, 'mixed')
                if round_index == 0:
                    try:
                        service.submit('alice', 'overflow', 'mixed',
                                       {'principal': 'alice', 'key': 'overflow', 'class': 'short'})
                    except ServiceError as error:
                        checks['saturated_queue_rejected'] = error.code == 'queue_full'
                    else:
                        checks['saturated_queue_rejected'] = False
                    (root / 'release-gate').touch()
                exposure.drain()
                completed = [exposure.jobs[key] for key in set(exposure.jobs) - before]
                rounds.append({'round': round_index, 'elapsed_seconds': time.monotonic() - round_started,
                    'completed_by_principal': {owner: sum(item['principal'] == owner for item in completed)
                                               for owner in ('alice', 'bob')}})
            report['mixed_elapsed_seconds'] = time.monotonic() - mixed_started
        except (RuntimeError, ServiceError, OSError, ValueError) as error:
            failures.append(type(error).__name__ + ':' + (str(error) if isinstance(error, RuntimeError) else 'fixture_failed'))
        finally:
            finished = time.monotonic()
            if worker is not None:
                if worker.poll() is None:
                    worker.terminate()
                try:
                    worker.wait(timeout=profile.maximum_cleanup_seconds)
                except subprocess.TimeoutExpired:
                    worker.kill()
                    worker.wait(timeout=5)
                    failures.append('dispatcher_cleanup_timeout')
            worker_log.close()
            deadline = finished + profile.maximum_cleanup_seconds
            while True:
                final_processes = sampler.sample()
                if not final_processes['descendant_processes'] or time.monotonic() >= deadline:
                    break
                time.sleep(0.1)
            cleanup_seconds = time.monotonic() - finished
        if not failures and len(rounds) == profile.mixed_rounds and not final_processes['descendant_processes']:
            # Reuse the rejected key only after the dispatcher has stopped.
            # Cancelling while it runs would race admission and charge another run.
            try:
                receipt = service.submit('alice', 'overflow', 'mixed',
                                         {'principal': 'alice', 'key': 'overflow', 'class': 'short'})
                checks['denied_request_key_reusable_after_drain'] = receipt['state'] == 'queued'
                checks['reused_probe_cancelled_without_execution'] = service.cancel('alice', receipt['task_id'])['state'] == 'cancelled'
            except (RuntimeError, OSError, ValueError):
                failures.append('post_cleanup_reuse_probe_failed')
        final_storage = storage_sample(state)
        mixed = {key: item for key, item in exposure.jobs.items() if item['phase'] == 'mixed'}
        completed_by_owner = {owner: sum(item['principal'] == owner and 'completed' in item for item in mixed.values())
                              for owner in ('alice', 'bob')}
        trace = trace_metrics(root / 'effects.jsonl', exposure.jobs, profile)
        paused = [item for item in mixed.values() if item['class'] == 'paused']
        admission, inspection = distribution(exposure.admissions), distribution(exposure.inspections)
        completion = distribution([item['completed'] - item['submitted'] for item in mixed.values() if 'completed' in item])
        final_provenance = provenance(repository, smoke=True)
        peaks = {key: max([baseline[key], final_processes[key]] + [row[key] for row in exposure.samples])
                 for key in final_processes}
        usage = service.operations.health()['usage']
        checks.update({
            'source_and_binaries_stable': all(report['provenance'][key] == final_provenance[key]
                                             for key in ('source_commit', 'source_clean', 'binaries')),
            'required_history_present_before_load': report.get('history_before_mixed_load') == {
                'cancelled': profile.retained_cancelled, 'completed': profile.retained_completed},
            'all_rounds_completed': len(rounds) == profile.mixed_rounds,
            'both_principals_complete_every_round': len(rounds) == profile.mixed_rounds and all(
                row['completed_by_principal'] == {'alice': 8, 'bob': 8} for row in rounds),
            'all_mixed_tasks_completed': sum(completed_by_owner.values()) == profile.mixed_rounds * profile.tasks_per_round,
            'paused_tasks_continued_after_hold': len(paused) == profile.mixed_rounds * 4 and all(
                item.get('resumed_at', 0) - item.get('paused_at', float('inf')) >= profile.paused_hold_seconds
                and 'completed' in item for item in paused),
            'other_work_progressed_while_tasks_paused': progress_during_pause(_records(root / 'effects.jsonl'), mixed),
            'effect_trace_exact_once': trace['valid_exact_once_pairs'],
            'actual_capacity_reached_and_obeyed': trace['capacity_observation_within_policy'] and
                trace['peak_active_callbacks'] == profile.workers and all(
                    value == profile.workers_per_principal for value in trace['peak_active_callbacks_by_principal'].values()),
            'no_paid_reservations': usage['reserved_microusd'] == 0,
            'one_call_per_task_and_separate_pause_run': usage['calls'] == len(exposure.jobs) and
                usage['runs'] == len(exposure.jobs) + len(paused),
            'process_peak_within_limit': peaks['descendant_processes'] <= profile.maximum_descendant_processes,
            'rss_peak_within_limit': peaks['descendant_rss_bytes'] <= profile.maximum_descendant_rss_bytes,
            'observer_growth_within_limit': final_processes['observer_rss_bytes'] - baseline['observer_rss_bytes'] <= profile.maximum_observer_growth_bytes,
            'all_observed_descendants_reaped': final_processes['descendant_processes'] == 0,
            'cleanup_within_fixture_bound': cleanup_seconds <= profile.maximum_cleanup_seconds + 5,
            'retained_state_within_limit': final_storage['apparent_bytes'] <= min(profile.maximum_state_bytes, policy['max_state_bytes']),
            'control_p99_within_fixture_ceiling': all((item['p99'] or float('inf')) <= profile.maximum_control_p99_seconds
                                                      for item in (admission, inspection)),
            'completion_p99_within_fixture_ceiling': (completion['p99'] or float('inf')) <= profile.maximum_completion_p99_seconds})
        report.update(passed=not failures and all(checks.values()), failures=failures, checks=checks,
            final_provenance=final_provenance, submitted=len(mixed), completed_by_principal=completed_by_owner,
            rounds=rounds, callbacks=trace, resource_baseline=baseline, resource_peaks=peaks,
            final_processes=final_processes, retained_state=final_storage,
            retained_state_growth={key: final_storage[key] - baseline[key] for key in final_storage},
            retained_tasks=_history_counts(service), deployment_usage=usage, cleanup_seconds=cleanup_seconds,
            latency_seconds={'submit': admission, 'inspect': inspection, 'submission_to_completion_observation': completion,
                             'completion_by': latency_groups(mixed)}, samples=exposure.samples)
    return report

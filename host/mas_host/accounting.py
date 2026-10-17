"""Period allocation limits and attributed provider charge observations.

Limits constrain reservation at admission, not provider billing time. Provider
charges are observations: they never refund or replace historical allocations.
Every mutation is owned by the deployment ledger transaction.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import re
import time

from .operations import OperationsError, _db, _file_lock



def _identity(value):
    if type(value) is not str or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:/@-]{0,199}', value) is None:
        raise OperationsError('invalid accounting identity')
    return value


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)


def _scope(value):
    if value != 'deployment':
        if type(value) is not str or not value.startswith('principal:'):
            raise OperationsError('invalid allocation period scope')
        _identity(value.removeprefix('principal:'))
    return value


@dataclass(frozen=True)
class PeriodPolicy:
    """UTC fixed windows, anchored at start_epoch; no rolling-window claim."""
    period_seconds: int
    max_calls: int
    max_reserved_microusd: int
    start_epoch: int = 0

    def __post_init__(self):
        if (any(type(value) is not int for value in asdict(self).values())
                or not 1 <= self.period_seconds <= 366 * 86400
                or not 1 <= self.max_calls <= 2**63 - 1
                or not 0 <= self.max_reserved_microusd <= 2**63 - 1
                or not 0 <= self.start_epoch < 2**63):
            raise OperationsError('invalid allocation period policy')

    def bucket(self, timestamp):
        return (timestamp - self.start_epoch) // self.period_seconds


def reserve_periods(db, run_id, call_id, principal, allocation, timestamp):
    """Called inside the same transaction as the unique lifetime allocation."""
    for row in db.execute('SELECT * FROM period_policies').fetchall():
        scope = row['scope']
        if scope != 'deployment' and principal is not None and scope != 'principal:' + principal:
            continue
        policy = PeriodPolicy(**json.loads(row['policy']))
        bucket = policy.bucket(timestamp)
        # Wall-clock rollback must not reopen an earlier period or spend against
        # a newly emptied bucket. Require reconciliation of the deployment clock.
        if bucket < row['high_water']:
            raise OperationsError('allocation clock moved backwards')
        used = db.execute('SELECT count(*),coalesce(sum(reserved_microusd),0) FROM period_allocations WHERE scope=? AND period=?',
                          (scope, bucket)).fetchone()
        if used[0] >= policy.max_calls or allocation > policy.max_reserved_microusd - used[1]:
            raise OperationsError('allocation period limit exhausted')
        db.execute('INSERT INTO period_allocations VALUES(?,?,?,?,?)', (scope, bucket, run_id, call_id, allocation))
        db.execute('UPDATE period_policies SET high_water=? WHERE scope=?', (bucket, scope))


class AccountingAdministration:
    """Trusted operator administration; callers separately enforce actor grants."""
    def __init__(self, ledger):
        self.ledger = ledger

    def configure_period(self, scope, policy, *, request_key, actor, reason):
        _scope(scope)
        _identity(request_key)
        _identity(actor)
        if not isinstance(policy, PeriodPolicy) or type(reason) is not str or not 1 <= len(reason) <= 500:
            raise OperationsError('invalid allocation period amendment')
        if self.ledger.readonly:
            raise OperationsError('read-only deployment cannot configure accounting')
        encoded = _canonical(asdict(policy))
        with _file_lock(self.ledger.state_dir / '.deployment.lock', True, nonblocking=True), _db(self.ledger.path) as db:
            db.execute('BEGIN IMMEDIATE')
            if not self.ledger._admission_enabled(db):
                raise OperationsError('audit-only deployment cannot configure accounting')
            old = db.execute('SELECT * FROM period_policies WHERE scope=? OR request_key=?', (scope, request_key)).fetchall()
            if old:
                if len(old) != 1 or any(old[0][key] != value for key, value in {
                        'scope': scope, 'policy': encoded, 'request_key': request_key, 'actor': actor, 'reason': reason}.items()):
                    raise OperationsError('allocation period policy conflict')
                return self._policy_receipt(old[0])
            timestamp = int(time.time())
            high_water = policy.bucket(timestamp)
            if db.execute('SELECT count(*) FROM period_policies').fetchone()[0] >= 10000:
                raise OperationsError('allocation period policy limit reached')
            self.ledger.check_storage_admission()
            db.execute('INSERT INTO period_policies VALUES(?,?,?,?,?,?,?)',
                       (scope, encoded, high_water, request_key, actor, reason, timestamp))
            # Preserve every earlier reservation. Unknown legacy ownership is
            # charged conservatively to any configured principal scope.
            rows = db.execute('SELECT c.*,p.principal FROM calls c LEFT JOIN run_principals p USING(run_id)')
            for row in rows:
                if scope != 'deployment' and row['principal'] is not None and scope != 'principal:' + row['principal']:
                    continue
                bucket = policy.bucket(row['admitted_at'])
                high_water = max(high_water, bucket)
                db.execute('INSERT INTO period_allocations VALUES(?,?,?,?,?)',
                           (scope, bucket, row['run_id'], row['call_id'], row['reserved_microusd']))
            db.execute('UPDATE period_policies SET high_water=? WHERE scope=?', (high_water, scope))
            return self._policy_receipt(db.execute('SELECT * FROM period_policies WHERE scope=?', (scope,)).fetchone())

    @staticmethod
    def _policy_receipt(row):
        return {**dict(row), 'policy': json.loads(row['policy'])}

    def periods(self):
        with _db(self.ledger.path, readonly=True) as db:
            result = []
            for row in db.execute('SELECT * FROM period_policies ORDER BY scope').fetchall():
                policy = PeriodPolicy(**json.loads(row['policy']))
                period = max(row['high_water'], policy.bucket(int(time.time())))
                used = db.execute('SELECT count(*),coalesce(sum(reserved_microusd),0) FROM period_allocations WHERE scope=? AND period=?',
                                  (row['scope'], period)).fetchone()
                result.append({**self._policy_receipt(row), 'period': period, 'calls': used[0], 'reserved_microusd': used[1]})
            return result

    def record_charge(self, *, provider, charge_id, run_id, call_id, actual_microusd,
                      observed_at, evidence, actor):
        for value in (provider, charge_id, run_id, call_id, evidence, actor):
            _identity(value)
        if (type(actual_microusd) is not int or not 0 <= actual_microusd <= 2**63 - 1
                or type(observed_at) is not int or not 0 <= observed_at <= int(time.time())):
            raise OperationsError('invalid provider charge observation')
        command = {'provider': provider, 'charge_id': charge_id, 'run_id': run_id, 'call_id': call_id,
                   'actual_microusd': actual_microusd, 'observed_at': observed_at, 'evidence': evidence, 'actor': actor}
        digest = hashlib.sha256(_canonical(command).encode()).hexdigest()
        if self.ledger.readonly:
            raise OperationsError('read-only deployment cannot record charges')
        with _file_lock(self.ledger.state_dir / '.deployment.lock', False), _db(self.ledger.path) as db:
            db.execute('BEGIN IMMEDIATE')
            if not self.ledger._admission_enabled(db):
                raise OperationsError('audit-only deployment cannot record charges')
            prior = db.execute('SELECT digest FROM provider_charges WHERE provider=? AND charge_id=?', (provider, charge_id)).fetchone()
            if prior is not None:
                if prior['digest'] != digest:
                    raise OperationsError('provider charge observation conflict')
            else:
                if not db.execute('SELECT 1 FROM calls WHERE run_id=? AND call_id=?', (run_id, call_id)).fetchone():
                    raise OperationsError('provider charge requires an admitted invocation')
                if db.execute('SELECT count(*) FROM provider_charges WHERE run_id=? AND call_id=?', (run_id, call_id)).fetchone()[0] >= 1024:
                    raise OperationsError('provider charge observation limit reached')
                self.ledger.check_storage_admission()
                db.execute('INSERT INTO provider_charges VALUES(?,?,?,?,?,?,?,?,?)', (*command.values(), digest))
        return {**command, 'allocation_changed': False}

    def reconcile(self, run_id, call_id):
        with _db(self.ledger.path, readonly=True) as db:
            call = db.execute('SELECT reserved_microusd FROM calls WHERE run_id=? AND call_id=?', (run_id, call_id)).fetchone()
            if call is None:
                raise OperationsError('admitted invocation not found')
            observations = [dict(row) for row in db.execute('SELECT provider,charge_id,actual_microusd,observed_at,evidence,actor FROM provider_charges WHERE run_id=? AND call_id=? ORDER BY provider,charge_id', (run_id, call_id))]
        observed = sum(row['actual_microusd'] for row in observations)
        return {'run_id': run_id, 'call_id': call_id, 'reserved_microusd': call['reserved_microusd'],
                'observed_charge_microusd': observed, 'observations': observations,
                'status': 'unobserved' if not observations else ('exceeds_reservation' if observed > call['reserved_microusd'] else 'within_reservation'),
                'invoice_complete': False, 'allocation_changed': False}

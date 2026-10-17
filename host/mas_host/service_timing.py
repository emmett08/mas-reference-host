"""Persist lifecycle clocks with their owning task or composition transaction."""
import time

from .codec import canonical_json
from .execution_policy import clock_status, transition_clock
from .service_contracts import ServiceError


def milliseconds():
    return int(time.time() * 1000)


def status(item, timestamp=None):
    clock = item.get('clock')
    return clock_status(clock, milliseconds() if timestamp is None else timestamp) if clock else None


def require_open(item):
    observed = status(item)
    if observed and observed['expired']:
        raise ServiceError(observed['expired'][0])


def transition(db, table, item, phase, *, branches=0, timestamp=None):
    if table not in {'tasks', 'instances'}:
        raise ValueError('invalid clock owner')
    if not item.get('clock'):
        return
    timestamp = milliseconds() if timestamp is None else timestamp
    clock = transition_clock(item['clock'], timestamp, phase, active_branches=branches)
    db.execute('UPDATE ' + table + ' SET clock=? WHERE id=?', (canonical_json(clock), item['id']))
    item['clock'] = clock


def execution_deadline(item):
    observed = status(item)
    if observed is None:
        return None
    remaining = min(observed['remaining_active_ms'], observed['business_expires_ms'] - milliseconds())
    return time.monotonic() + max(0, remaining) / 1000


def receipt(item):
    observed = status(item)
    return {'timing': {**observed, 'phase': item['clock']['phase']}} if observed else {}

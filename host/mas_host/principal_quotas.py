"""Principal admission policies; payloads cannot select allocation ownership."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import hashlib

from .operations import OperationsError, _db, _file_lock
from .service_contracts import ServiceError


@dataclass(frozen=True)
class PrincipalQuota:
    """Outstanding logical submissions and immutable lifetime allocations."""

    max_pending: int = 1000
    max_calls: int = 10000
    max_reserved_microusd: int = 100_000_000

    def __post_init__(self):
        if any(type(value) is not int or not 1 <= value <= 2**63 - 1
               for value in asdict(self).values()):
            raise OperationsError('invalid principal quota')


def principal_identity(value):
    if (type(value) is not str or not value or len(value) > 256
            or any(ord(character) < 32 for character in value)):
        raise OperationsError('invalid allocation principal')
    return value


def quota_configuration(value):
    if type(value) is not dict or len(value) > 10000:
        raise OperationsError('invalid principal quota configuration')
    result = {}
    for principal, policy in value.items():
        principal_identity(principal)
        try:
            result[principal] = asdict(policy if isinstance(policy, PrincipalQuota) else PrincipalQuota(**policy))
        except (TypeError, ValueError):
            raise OperationsError('invalid principal quota configuration') from None
    return result


class PrincipalAllocation:
    """Bind trusted service ownership before any run or provider admission.

    Composition allocation wraps the same deployment ledger, so its ancestor
    controls remain in force. Call ownership comes from the retained run binding.
    """

    def __init__(self, ledger, principal):
        self.ledger, self.principal = ledger, principal_identity(principal)

    def __getattr__(self, name):
        return getattr(self.ledger, name)

    def admit_run(self, run_id, run_dir):
        return self.ledger.admit_run(run_id, run_dir, principal=self.principal)


def pending_count(operations, principal):
    """Count both repositories without modifying either or retaining a cache.

    Pauses and uncertainty retain outstanding capacity. A terminal composition
    with an unresolved child cannot free it by concealing that child state.
    The short principal submission lock serialises only new admissions; lifecycle
    completion can decrease this snapshot concurrently without over-admission.
    """
    count = 0
    for filename, query in (
        ('task-service.sqlite', "SELECT count(*) FROM tasks t WHERE principal=? AND "
         "(state NOT IN ('completed','failed','cancelled') OR EXISTS "
         "(SELECT 1 FROM executions e WHERE e.task_id=t.id AND e.state IN ('running','unknown')))"),
        ('composition.sqlite', "SELECT count(*) FROM instances i WHERE principal=? AND "
         "(state NOT IN ('completed','failed','cancelled','expired') OR EXISTS "
         "(SELECT 1 FROM attempts a WHERE a.instance=i.id AND a.state IN ('running','unknown')))"),
    ):
        path = operations.state_dir / filename
        if path.exists() or path.is_symlink():
            with _db(path, readonly=True) as db:
                count += db.execute(query, (principal,)).fetchone()[0]
    return count


@contextmanager
def submission_mutation(operations, store, principal):
    """Hold queue ownership through the owning submission transaction commit."""
    from .service_mutation import service_mutation
    principal_identity(principal)
    digest = hashlib.sha256(principal.encode()).hexdigest()
    with _file_lock(operations.state_dir / ('.principal-queue-' + digest + '.lock'), True):
        with service_mutation(operations, store) as db:
            yield db


def check_submission_quota(operations, principal):
    policy = operations.principal_policy(principal)
    if policy is not None and pending_count(operations, principal) >= policy['max_pending']:
        raise ServiceError('principal_queue_limit')

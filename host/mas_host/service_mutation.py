"""Shared maintenance and audit-only boundary for durable service mutations.

This boundary permits recording cancellation and recovery when storage admission
has closed. Callers check ``check_storage_admission`` after recognising duplicate
requests and before persisting genuinely new submissions or continuations.
"""
from contextlib import contextmanager

from .operations import _file_lock
from .service_contracts import ServiceError


@contextmanager
def service_mutation(operations, store):
    """Serialise one repository mutation against deployment maintenance."""
    with _file_lock(operations.state_dir / '.deployment.lock', False):
        if operations.readonly or not operations.admission_enabled:
            raise ServiceError('audit_only')
        with store.transaction() as db:
            yield db

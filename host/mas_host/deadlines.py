"""Separately authorised deadline administration for leaf and composed work."""
from __future__ import annotations

from .deadline_amendments import request
from .service_contracts import ServiceError
from .service_timing import milliseconds


class DeadlineService:
    """Configured operator grants are independent of ordinary submission rights.

    Operator identity comes from the authenticated boundary. ``operators`` maps
    those identities to task owners whose clocks they may extend. The service
    rechecks ordinary owner visibility and grants inside the store transaction.
    """
    def __init__(self, service, *, operators):
        self.service = service
        self.operators = {operator: frozenset(owners) for operator, owners in operators.items()}

    def extend(self, operator, principal, task_id, *, request_key, reason,
               expected_revision, limits, path=None):
        if principal not in self.operators.get(operator, ()):
            raise ServiceError('forbidden')
        command = request(operator=operator, principal=principal, request_key=request_key,
                          reason=reason, expected_revision=expected_revision, limits=limits, path=path)
        composition = type(task_id) is str and task_id.startswith('flow-')
        service = self.service
        if hasattr(service, 'leaves'):
            service = service.compositions if composition else service.leaves
        if not composition and path is not None:
            raise ServiceError('deadline_scope_not_supported')
        # Transactional lifecycle state checks reject running or uncertain work;
        # no extension resumes execution or changes an effect identity.
        with service._mutation() as db:
            owner = service._owned(db, principal, task_id)
            return service.store.extend_deadline(db, owner, command, milliseconds())

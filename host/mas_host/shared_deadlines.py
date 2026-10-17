"""Operator-authorised deadline amendments in the shared lifecycle owner."""
from .deadline_amendments import request
from .service_contracts import ServiceError


class SharedDeadlineService:
    def __init__(self, service, *, operators):
        self.service = service
        self.operators = {operator: frozenset(owners) for operator, owners in operators.items()}

    def extend(self, operator, principal, task_id, *, request_key, reason,
               expected_revision, limits, path=None):
        if principal not in self.operators.get(operator, ()):
            raise ServiceError('forbidden')
        command = request(operator=operator, principal=principal, request_key=request_key,
            reason=reason, expected_revision=expected_revision, limits=limits, path=path)
        self.service.inspect(principal, task_id)
        composition = task_id.startswith('flow-')
        if not composition and path is not None:
            raise ServiceError('deadline_scope_not_supported')
        kind = 'composition' if composition else 'task'
        return self.service.authority.command(kind + '.extend_deadline', {
            'principal': principal, 'instance' if composition else 'task': task_id,
            'command': command})['receipt']

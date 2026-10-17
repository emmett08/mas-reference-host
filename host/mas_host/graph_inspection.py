"""Bounded graph structure; full application values remain in leaf inspection."""
from .codec import integer_json
from .service_contracts import ServiceError

SECTIONS = {
    'nodes': ('path,kind,state', 'path'),
    'attempts': ('path,revision,workflow,identity,run_directory AS run_id,state', 'path,revision'),
    'scopes': ('path,deadline', 'path'),
}


def selector(section, offset, limit):
    if (type(section) is not str or section not in SECTIONS
            or type(offset) is not int or not 0 <= offset < 2**63
            or type(limit) is not int or not 1 <= limit <= 100):
        raise ServiceError('invalid_graph_selector')


def inspect_graph(service, principal, task_id, *, section='nodes', offset=0, limit=20):
    selector(section, offset, limit)
    if type(task_id) is not str or not task_id.startswith('flow-'):
        raise ServiceError('unsupported_capability')
    if hasattr(service, 'authority'):
        if principal not in service.grants:
            raise ServiceError('forbidden')
        value = service.authority.query('operator.graph', {'principal': principal, 'task_id': task_id,
            'section': section, 'offset': offset, 'limit': limit})
        if value['workflow'] not in service.grants[principal]:
            raise ServiceError('forbidden')
        return value
    owner = service.compositions
    with owner.store.transaction(readonly=True) as db:
        item = owner._owned(db, principal, task_id)
        fields, order = SECTIONS[section]
        total = db.execute(f'SELECT count(*) FROM {section} WHERE instance=?', (task_id,)).fetchone()[0]
        rows = db.execute(f'SELECT {fields} FROM {section} WHERE instance=? ORDER BY {order} LIMIT ? OFFSET ?',
                          (task_id, limit, offset))
        selected, size = [], 1024
        for row in rows:
            value = dict(row)
            amount = len(integer_json(value).encode('utf-8'))
            if selected and size + amount > 24000:
                break
            selected.append(value)
            size += amount
    return {'task_id': task_id, 'archived': bool(item.get('archived')), 'workflow': item['plan'], 'state': item['state'], 'section': section,
            'items': selected, 'total': total,
            'next_offset': offset + len(selected) if offset + len(selected) < total else None}

"""Bounded selectors and encoding for one durable workflow task revision."""
from __future__ import annotations

import json
import re

from .codec import JSON_BYTES, JsonSizeError, integer_json
from .contracts import RunnerError
from .service_contracts import ServiceError


TASK_INSPECTION_BYTES = 2 * JSON_BYTES + 8192
TASK_INSPECTION_FIELDS = frozenset({
    'instance_id', 'plan', 'instance_state', 'deadline', 'path', 'revision',
    'workflow', 'identity', 'run_directory', 'state', 'output', 'error',
})


def validate_task_selector(path, revision=None):
    """Paths are opaque database selectors, never filesystem paths."""
    if (type(path) is not str or len(path) > 4096
            or re.fullmatch(r'root(?:/[A-Za-z0-9][A-Za-z0-9_.-]*)*', path) is None):
        raise ServiceError('invalid_task_path')
    if revision is not None and (type(revision) is not int or not 0 <= revision < 32):
        raise ServiceError('invalid_task_revision')


def encode_task_inspection(detail):
    """Encode the exact CLI envelope, including its newline, within one bound.

    Application output and error retain their own integer-JSON depth allowance;
    adding inspection framing must not reject a valid deeply nested leaf result.
    This boundary selects fields and bounds data; it does not redact application
    output that the authorised owner deliberately retrieves.
    """
    if type(detail) is not dict or set(detail) != TASK_INSPECTION_FIELDS:
        raise ServiceError('invalid_task_inspection')
    try:
        metadata = {key: value for key, value in detail.items() if key not in {'output', 'error'}}
        integer_json(metadata, 8192 - 64)
        for key in ('output', 'error'):
            if detail[key] is not None and type(detail[key]) is not dict:
                raise ServiceError('invalid_task_inspection')
            integer_json(detail[key])
        source = json.dumps({'ok': True, 'result': detail}, ensure_ascii=False,
                            allow_nan=False, separators=(',', ':')) + '\n'
        if len(source.encode('utf-8')) > TASK_INSPECTION_BYTES:
            raise JsonSizeError('task inspection exceeds response bound')
        return source
    except JsonSizeError:
        raise ServiceError('task_inspection_too_large') from None
    except (RunnerError, ValueError, TypeError, UnicodeError, RecursionError):
        raise ServiceError('invalid_task_inspection') from None

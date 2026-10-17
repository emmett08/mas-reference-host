"""Versioned scientific arrays in hash-verified artefacts, never wire JSON floats."""
from __future__ import annotations

from contextlib import contextmanager
import math
import struct
from tempfile import TemporaryFile

from .artifacts import ArtifactError, ArtifactRef
from .codec import integer_json

MEDIA_TYPE = 'application/vnd.mas.ndarray'
_MAGIC = b'MASARRAY1\n'
_MAX_HEADER = 4096


def _shape(shape):
    if (type(shape) not in (list, tuple) or not 1 <= len(shape) <= 8
            or any(type(size) is not int or not 0 <= size <= 2**31 - 1 for size in shape)):
        raise ArtifactError('invalid numeric array shape')
    count = math.prod(shape)
    if count > 2**53 - 1:
        raise ArtifactError('invalid numeric array shape')
    return list(shape), count


def put_float64(repository, values, *, shape, readers=(), input_sha256s=()):
    """Store finite IEEE 754 binary64 values and integer shape in one hashed object.

    Values are encoded little-endian without decimal string conversion. Units,
    coordinate systems and statistical interpretations remain application data.
    """
    dimensions, count = _shape(shape)
    header = integer_json({'format': 'mas-ndarray/1', 'dtype': 'float64',
                           'byte_order': 'little', 'shape': dimensions, 'finite': True}).encode()
    size = len(_MAGIC) + 4 + len(header) + count * 8
    if size > repository.policy.max_artifact_bytes:
        raise ArtifactError('artefact exceeds per-object quota')
    with TemporaryFile(mode='w+b') as stream:
        stream.write(_MAGIC + struct.pack('<I', len(header)) + header)
        seen = 0
        for value in values:
            if seen >= count or type(value) not in (int, float):
                raise ArtifactError('invalid numeric array values')
            try:
                encoded = struct.pack('<d', value)
            except (OverflowError, struct.error):
                raise ArtifactError('invalid numeric array values') from None
            if not math.isfinite(struct.unpack('<d', encoded)[0]):
                raise ArtifactError('numeric array requires finite values')
            stream.write(encoded)
            seen += 1
        if seen != count:
            raise ArtifactError('numeric array shape does not match values')
        stream.seek(0)
        return repository.put_stream(stream, media_type=MEDIA_TYPE,
                                     readers=readers, input_sha256s=input_sha256s)


@contextmanager
def open_float64(repository, reference):
    """Yield (shape, iterator) only after repository verification; consume in context."""
    ref = reference if isinstance(reference, ArtifactRef) else ArtifactRef.from_dict(reference)
    if ref.media_type != MEDIA_TYPE:
        raise ArtifactError('unsupported numeric array encoding')
    with repository.open(ref) as stream:
        if stream.read(len(_MAGIC)) != _MAGIC:
            raise ArtifactError('unsupported numeric array encoding')
        length_bytes = stream.read(4)
        if len(length_bytes) != 4:
            raise ArtifactError('invalid numeric array header')
        length = struct.unpack('<I', length_bytes)[0]
        if not 1 <= length <= _MAX_HEADER:
            raise ArtifactError('invalid numeric array header')
        raw = stream.read(length)
        try:
            from .artifacts import _load_json
            header = _load_json(raw)
            if (type(header) is not dict or set(header) != {'format', 'dtype', 'byte_order', 'shape', 'finite'}
                    or header['format'] != 'mas-ndarray/1' or header['dtype'] != 'float64'
                    or header['byte_order'] != 'little' or header['finite'] is not True):
                raise ValueError()
            shape, count = _shape(header['shape'])
        except (ValueError, TypeError, KeyError):
            raise ArtifactError('invalid numeric array header') from None
        if ref.size != len(_MAGIC) + 4 + length + count * 8:
            raise ArtifactError('numeric array shape does not match values')
        def values():
            for _ in range(count):
                data = stream.read(8)
                if len(data) != 8:
                    raise ArtifactError('numeric array content is truncated')
                value = struct.unpack('<d', data)[0]
                if not math.isfinite(value):
                    raise ArtifactError('numeric array requires finite values')
                yield value
        yield tuple(shape), values()

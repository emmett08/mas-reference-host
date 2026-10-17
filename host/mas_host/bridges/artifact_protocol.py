"""Platform-neutral bounded artefact transfer messages."""

CHUNK_BYTES = 32768
OPERATIONS = {
    'upload_begin': ({'scope', 'sha256', 'size', 'media_type'}, {'input_sha256s', 'readers'}),
    'upload_chunk': ({'transfer_id', 'offset', 'data'}, set()),
    'upload_commit': ({'transfer_id'}, set()),
    'download_begin': ({'scope', 'reference'}, set()),
    'download_chunk': ({'transfer_id', 'offset'}, set()),
    'close': ({'transfer_id'}, set()),
    'status': ({'transfer_id'}, set()),
    'revoke': ({'scope', 'reference', 'readers'}, set()),
}
WRITE_OPERATIONS = frozenset({'upload_begin', 'upload_chunk', 'upload_commit', 'revoke'})

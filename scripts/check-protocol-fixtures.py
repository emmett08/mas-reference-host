#!/usr/bin/env python3
"""Require reference-host conformance corpora to match the selected protocol."""
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def check(source=ROOT / '.protocol'):
    traces = source / 'formal/task-service-traces.json'
    if not traces.exists():
        # Compatibility with the immutable pre-extraction protocol revision.
        traces = source / 'host/tests/fixtures/task-service-traces.json'
    pairs = [(ROOT / 'host/tests/fixtures/task-service-traces.json', traces),
             (ROOT / 'formal/authority-traces.json', source / 'formal/authority-traces.json')]
    for reference, protocol in pairs:
        if reference.read_bytes() != protocol.read_bytes():
            raise SystemExit(f'Conformance corpus differs from the selected protocol: {reference.name}')
    print('Both conformance corpora match the selected external protocol.')


if __name__ == '__main__':
    check()

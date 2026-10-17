"""Portable authenticated remote client; never imports execution facilities."""
from __future__ import annotations

import argparse
import getpass
import json
from pathlib import Path
import sys
import warnings

from .bridges.authentication import validate_token
from .bridges.service_client import TaskServiceClient, decode_integer_json
from .service_contracts import ServiceError


METHODS = {
    'discover': 'discover', 'submit': 'submit', 'inspect': 'inspect',
    'result': 'result', 'cancel': 'cancel', 'update': 'update',
    'recover': 'recover', 'events': 'events_page', 'wait': 'events_wait',
    'task': 'inspect_task', 'graph': 'inspect_graph', 'list': 'list_task_page',
    'operator': 'operator', 'artifact': 'artifact',
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('endpoint')
    parser.add_argument('operation', choices=sorted(METHODS))
    parser.add_argument('--principal', required=True)
    parser.add_argument('--data', metavar='JSON_FILE', help='bounded integer-JSON arguments; - reads stdin')
    credentials = parser.add_mutually_exclusive_group()
    credentials.add_argument('--token-file', help='owned private POSIX credential file')
    credentials.add_argument('--token-stdin', action='store_true', help='read one bearer token line from stdin')
    parser.add_argument('--timeout', type=int, default=30)
    parser.add_argument('--allow-loopback-http', action='store_true')
    args = parser.parse_args(argv)
    if args.token_stdin and args.data == '-':
        parser.error('--token-stdin and --data - cannot share stdin')
    try:
        payload = {}
        if args.data:
            if args.data == '-':
                raw = sys.stdin.buffer.read(1_048_577)
            else:
                with Path(args.data).open('rb') as stream:
                    raw = stream.read(1_048_577)
            payload = decode_integer_json(raw, max_bytes=1_048_576)
            if type(payload) is not dict:
                raise ServiceError('invalid_arguments')
        options = {}
        if args.token_file:
            options['token_file'] = args.token_file
        else:
            if args.token_stdin:
                token = sys.stdin.readline(8194)
                if token.endswith('\n'):
                    token = token[:-1]
            else:
                # Refuse getpass's echoed fallback: callers can explicitly opt
                # into a private pipe using --token-stdin on every platform.
                with warnings.catch_warnings():
                    warnings.simplefilter('error', getpass.GetPassWarning)
                    token = getpass.getpass('MAS bearer token: ')
            token = validate_token(token)
            options['token_provider'] = lambda: token
        client = TaskServiceClient(args.endpoint, principal=args.principal,
            timeout_seconds=args.timeout, allow_loopback_http=args.allow_loopback_http, **options)
        method = getattr(client, METHODS[args.operation])
        try:
            result = method(args.principal, **payload)
        except TypeError:
            raise ServiceError('invalid_arguments') from None
        sys.stdout.write(json.dumps(result, ensure_ascii=True, sort_keys=True) + '\n')
        return 0
    except ServiceError as error:
        sys.stderr.write(json.dumps({'error': error.code}) + '\n')
        return 2
    except (OSError, ValueError, EOFError, getpass.GetPassWarning):
        sys.stderr.write('{"error":"client_input_unavailable"}\n')
        return 2


if __name__ == '__main__':
    raise SystemExit(main())

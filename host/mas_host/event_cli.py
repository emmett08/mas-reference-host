"""Trusted local administration of bounded webhook event subscriptions."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .bridge_config import build_service, load_config
from .bridges.http_policy import HTTPTransportPolicy
from .event_delivery import EventDelivery, WebhookTarget
from .service_contracts import ServiceError


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--state-dir', type=Path, required=True)
    parser.add_argument('--targets', type=Path, required=True,
                        help='trusted JSON target registrations; secret values stay in private files')
    parser.add_argument('--principal', required=True, help='trusted local task-owner identity')
    parser.add_argument('--subscription', required=True)
    commands = parser.add_subparsers(dest='command', required=True)
    subscribe = commands.add_parser('subscribe')
    subscribe.add_argument('--task-id', required=True)
    subscribe.add_argument('--target', required=True)
    subscribe.add_argument('--after', type=int, default=0)
    commands.add_parser('inspect')
    commands.add_parser('stop')
    commands.add_parser('deliver', help='deliver one page; repeat from a supervised timer')
    args = parser.parse_args(argv)
    try:
        from .bridges.service_client import decode_integer_json
        with args.targets.open('rb') as stream:
            value = decode_integer_json(stream.read(65537), max_bytes=65536)
        if type(value) is not dict or set(value) != {'format', 'targets'} or value['format'] != 'mas-webhooks/1':
            raise ServiceError('invalid_webhook_configuration')
        if type(value['targets']) is not dict or len(value['targets']) > 256:
            raise ServiceError('invalid_webhook_configuration')
        targets = {}
        for name, item in value['targets'].items():
            if type(item) is not dict or not {'endpoint', 'token_file'} <= set(item) or set(item) - {
                    'endpoint', 'token_file', 'allow_loopback_http', 'timeout_seconds', 'transport'}:
                raise ServiceError('invalid_webhook_configuration')
            options = {key: val for key, val in item.items() if key != 'transport'}
            if 'transport' in item:
                options['transport_policy'] = HTTPTransportPolicy.from_config(item['transport'])
            targets[name] = WebhookTarget(name, **options)
        service = build_service(load_config(args.config), args.state_dir)
        delivery = EventDelivery(args.state_dir, service, targets)
        if args.command == 'subscribe':
            result = delivery.subscribe(args.principal, args.subscription, args.task_id, args.target, after=args.after)
        else:
            operation = delivery.deliver_once if args.command == 'deliver' else getattr(delivery, args.command)
            result = operation(args.principal, args.subscription)
        print(json.dumps({'ok': True, 'result': result}))
        return 0
    except (ServiceError, OSError, ValueError, TypeError) as error:
        print(json.dumps({'ok': False, 'error': {'code': error.code if isinstance(error, ServiceError) else 'event_delivery_failed'}}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())

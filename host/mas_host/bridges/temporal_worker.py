"""Run the optional Temporal activity worker against an authenticated MAS service."""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor

from .authentication import read_token_file
from .orchestrator import OrchestratorConnector
from .service_client import TaskServiceClient
from .temporal import temporal_activities


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--temporal-target", required=True, help="configured Temporal host:port")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--task-queue", required=True)
    parser.add_argument("--temporal-api-key-file")
    parser.add_argument("--insecure-temporal", action="store_true", help="disable TLS for a local development server")
    parser.add_argument("--mas-endpoint", required=True)
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--principal", required=True, help="local identity matching the configured bearer principal")
    parser.add_argument("--allow-loopback-http", action="store_true")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args(argv)
    if not 1 <= args.workers <= 32:
        parser.error("workers must be between 1 and 32")

    async def run():
        from temporalio.client import Client
        from temporalio.worker import Worker
        service = TaskServiceClient(args.mas_endpoint, token_file=args.token_file,
            principal=args.principal, allow_loopback_http=args.allow_loopback_http)
        connector = OrchestratorConnector(service, args.principal)
        client = await Client.connect(args.temporal_target, namespace=args.namespace,
            tls=not args.insecure_temporal,
            api_key=read_token_file(args.temporal_api_key_file) if args.temporal_api_key_file else None)
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            await Worker(client, task_queue=args.task_queue, activities=temporal_activities(connector),
                         activity_executor=executor, max_concurrent_activities=args.workers).run()

    asyncio.run(run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

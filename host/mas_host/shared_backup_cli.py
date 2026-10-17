"""Trusted operator command entry point for guarded PostgreSQL recovery."""
import argparse
import importlib
import json
from pathlib import Path
import sys

from .shared_backup import SharedBackupManager


def main(argv=None):
    parser = argparse.ArgumentParser(prog='python -m mas_host.shared_backup_cli')
    parser.add_argument('--factory', required=True, help='trusted module:function returning (manager, object_reader)')
    sub = parser.add_subparsers(dest='command', required=True)
    archive = sub.add_parser('archive')
    archive.add_argument('directory', type=Path)
    archive.add_argument('--operator', required=True)
    archive.add_argument('--request-key', required=True)
    archive.add_argument('--evidence', type=Path, required=True)
    restore = sub.add_parser('restore-audit')
    restore.add_argument('directory', type=Path)
    sub.add_parser('inspect')
    sub.add_parser('verify-objects')
    activate = sub.add_parser('activate')
    activate.add_argument('--fencing-proof', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        module, name = args.factory.split(':', 1)
        manager, reader = getattr(importlib.import_module(module), name)()
        if not isinstance(manager, SharedBackupManager) or not callable(reader):
            raise ValueError
        if args.command == 'archive':
            result = manager.archive(args.directory, operator=args.operator,
                request_key=args.request_key, evidence=json.loads(args.evidence.read_text()))
        elif args.command == 'restore-audit':
            result = manager.restore_audit(args.directory)
        elif args.command == 'verify-objects':
            result = manager.verify_objects(reader)
        elif args.command == 'activate':
            result = manager.activate(fencing_proof=json.loads(args.fencing_proof.read_text()), read_object=reader)
        else:
            result = manager.inspect()
        print(json.dumps({'ok': True, 'result': result}, separators=(',', ':')))
        return 0
    except Exception:
        print(json.dumps({'ok': False, 'error': {'code': 'shared_recovery_rejected'}}))
        return 1


if __name__ == '__main__':
    sys.exit(main())

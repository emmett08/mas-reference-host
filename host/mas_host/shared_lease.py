"""Private controller lease guardian. Never import runtime/provider configuration."""
import json
import os
import select
import subprocess
import sys
import time


def main():
    lifetime, parent = int(sys.argv[1]), int(sys.argv[2])
    data = sys.stdin.buffer.read(65_537)
    if len(data) > 65_536:
        return 1
    config = json.loads(data)
    interval = config['lease_seconds'] / 3
    next_renewal = time.monotonic() + interval
    env = {'PATH': os.environ.get('PATH', os.defpath), 'MAS_POSTGRES_DSN': config['dsn']}
    if config['ca_file']:
        env['MAS_POSTGRES_CA_FILE'] = config['ca_file']
    command = json.dumps({'version': 1, 'schema': config['schema'], 'binding': config['binding'],
        'operation': 'renew', 'lease_seconds': config['lease_seconds']}, separators=(',', ':'))
    while os.getppid() == parent:
        # The PID check also covers forked workers inheriting the write end.
        ready, _, _ = select.select([lifetime], [], [], min(1, max(0, next_renewal - time.monotonic())))
        if ready and not os.read(lifetime, 1):
            return 0
        if time.monotonic() < next_renewal:
            continue
        result = subprocess.run([config['binary']], input=command, text=True, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=10, check=False)
        reply = json.loads(result.stdout)
        if result.returncode or reply.get('version') != 1 or reply.get('ok') is not True:
            return 1
        next_renewal = time.monotonic() + interval
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception:
        sys.exit(1)

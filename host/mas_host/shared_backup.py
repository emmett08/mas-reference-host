"""Sealed PostgreSQL archive recovery with independent fencing attestation.

The controller has no recovery signing key. An operator supplies the externally
held key to this tool. This module never signs an activation/fencing proof; that
proof must come from the configured external fencing authority.
"""
from __future__ import annotations

import ctypes
from contextlib import contextmanager
import ctypes.util
import hashlib
import hmac
import ipaddress
import json
import os
from pathlib import Path
import shutil
import subprocess
import stat
import tempfile

from .codec import integer_json
from .contracts import RunnerError
from .journal import canonical


class SharedBackupError(RunnerError):
    pass


class _Option(ctypes.Structure):
    _fields_ = [(name, ctypes.c_char_p) for name in ('keyword', 'envvar', 'compiled', 'val', 'label', 'dispchar')] + [('dispsize', ctypes.c_int)]


def _libpq_parameters(dsn):
    """Use the actual libpq parser; never split a DSN or put secrets in argv."""
    name = ctypes.util.find_library('pq') or 'libpq.so.5'
    try:
        lib = ctypes.CDLL(name)
    except OSError:
        raise SharedBackupError('PostgreSQL client library is required') from None
    lib.PQconninfoParse.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p)]
    lib.PQconninfoParse.restype = ctypes.POINTER(_Option)
    lib.PQconninfoFree.argtypes = [ctypes.POINTER(_Option)]
    lib.PQfreemem.argtypes = [ctypes.c_void_p]
    error = ctypes.c_char_p()
    pointer = lib.PQconninfoParse(dsn.encode(), ctypes.byref(error))
    if not pointer:
        if error: lib.PQfreemem(error)
        raise SharedBackupError('invalid PostgreSQL recovery connection')
    try:
        result, index = {}, 0
        while pointer[index].keyword:
            item = pointer[index]
            if item.val is not None:
                if item.envvar is None:
                    raise SharedBackupError('PostgreSQL recovery connection option is not supported')
                result[item.envvar.decode()] = item.val.decode()
            index += 1
        return result
    finally:
        lib.PQconninfoFree(pointer)


def _remote_host(value):
    if not value or value.startswith('/') or value == 'localhost':
        return False
    try:
        return not ipaddress.ip_address(value).is_loopback
    except ValueError:
        return True


def postgres_environment(authority):
    env = {'PATH': os.environ.get('PATH', os.defpath), 'LC_ALL': 'C',
           **_libpq_parameters(authority._dsn)}
    if any(_remote_host(part) for name in ('PGHOST', 'PGHOSTADDR') for part in env.get(name, '').split(',')):
        if env.get('PGSSLMODE') == 'disable':
            raise SharedBackupError('remote PostgreSQL recovery requires verified TLS')
        env['PGSSLMODE'] = 'verify-full'
    elif env.get('PGSSLMODE') == 'require':
        env['PGSSLMODE'] = 'verify-full'
    if authority._postgres_ca_file:
        env['PGSSLROOTCERT'] = authority._postgres_ca_file
    return env


def _signature(payload, key):
    return hmac.new(key, canonical(payload).encode(), hashlib.sha256).hexdigest()


def _file_digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


class SharedBackupManager:
    def __init__(self, authority, *, recovery_key, key_reference, pg_dump='pg_dump', pg_restore='pg_restore'):
        from .codec import is_actor_identity
        if type(recovery_key) is not bytes or len(recovery_key) < 32 or not is_actor_identity(key_reference):
            raise SharedBackupError('invalid independent recovery authority')
        self.authority, self._key, self.key_reference = authority, recovery_key, key_reference
        self.pg_dump, self.pg_restore = shutil.which(pg_dump), shutil.which(pg_restore)
        if not self.pg_dump or not self.pg_restore:
            raise SharedBackupError('native PostgreSQL archive tools are required')

    def _request(self, operation, *, timeout=300, **fields):
        source = integer_json({'version': 1, 'schema': self.authority.namespace,
            'binding': self.authority._binding, 'operation': operation, **fields}, 1_048_576)
        env = {'PATH': os.environ.get('PATH', os.defpath), 'MAS_POSTGRES_DSN': self.authority._dsn,
               'MAS_RECOVERY_KEY_HEX': self._key.hex()}
        if self.authority._postgres_ca_file:
            env['MAS_POSTGRES_CA_FILE'] = self.authority._postgres_ca_file
        try:
            result = subprocess.run([self.authority._binary], input=source, text=True,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env, timeout=timeout, check=False)
            response = json.loads(result.stdout)
            if result.returncode or response.get('version') != 1 or response.get('ok') is not True:
                raise ValueError
            return response['result']
        except (OSError, ValueError, KeyError, subprocess.TimeoutExpired):
            raise SharedBackupError('shared recovery rejected; retain the original source and evidence') from None

    def inspect(self):
        return self._request('recovery_inspect')

    def archive(self, directory, *, operator, request_key, evidence):
        """Seal the source permanently, then create its final exact native archive.

        Failure leaves the source sealed. Repeating with the same source request
        can produce another archive without reopening any admissions.
        """
        path = Path(directory)
        path.mkdir(mode=0o700, parents=False, exist_ok=False)
        seal = self._request('recovery_seal', parameters={'operator': operator,
            'request_key': request_key, 'key_reference': self.key_reference, 'evidence': evidence})
        self.authority.close()
        target = path / 'authority.pgdump'
        env = postgres_environment(self.authority)
        try:
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, 'wb') as output:
                result = subprocess.run([self.pg_dump, '--format=custom', '--no-owner', '--no-acl',
                    '--schema', self.authority.namespace, '--strict-names'], env=env,
                    stdout=output, stderr=subprocess.DEVNULL, timeout=300, check=False)
                output.flush()
                os.fsync(output.fileno())
            if result.returncode:
                raise SharedBackupError('native PostgreSQL archive failed; source remains sealed')
        except (OSError, subprocess.TimeoutExpired):
            raise SharedBackupError('native PostgreSQL archive failed; source remains sealed') from None
        payload = {'format': 'mas-shared-backup/1', 'archive_sha256': _file_digest(target),
            'archive_bytes': target.stat().st_size, 'seal': seal, 'key_reference': self.key_reference}
        manifest = {'payload': payload, 'signature': _signature(payload, self._key)}
        with (path / 'manifest.json').open('x', encoding='utf-8') as output:
            os.chmod(path / 'manifest.json', 0o600)
            output.write(canonical(manifest) + '\n')
            output.flush()
            os.fsync(output.fileno())
        return manifest

    @staticmethod
    def _private_regular(descriptor):
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or info.st_uid != os.geteuid() or info.st_mode & 0o077):
            raise SharedBackupError('recovery files must be private owned regular files')
        return info

    @contextmanager
    def _verified_archive(self, directory):
        """Hash into a private staging descriptor; pg_restore never reopens a path."""
        directory_fd = archive_fd = manifest_fd = None
        try:
            directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            info = os.fstat(directory_fd)
            if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise SharedBackupError('recovery directory must be private and owned')
            manifest_fd = os.open('manifest.json', os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
            info = self._private_regular(manifest_fd)
            if info.st_size > 16 * 1024 * 1024:
                raise ValueError
            with os.fdopen(manifest_fd, 'rb') as source:
                manifest_fd = None
                data = source.read(16 * 1024 * 1024 + 1)
            if len(data) > 16 * 1024 * 1024:
                raise ValueError
            manifest = json.loads(data)
            payload = manifest['payload']
            if (payload['format'] != 'mas-shared-backup/1' or payload['key_reference'] != self.key_reference
                    or not hmac.compare_digest(manifest['signature'], _signature(payload, self._key))
                    or payload['seal']['payload']['namespace'] != self.authority.namespace
                    or type(payload['archive_bytes']) is not int or payload['archive_bytes'] < 0):
                raise ValueError
            archive_fd = os.open('authority.pgdump', os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
            if self._private_regular(archive_fd).st_size != payload['archive_bytes']:
                raise ValueError
            with tempfile.TemporaryFile(mode='w+b') as staged:
                digest, total = hashlib.sha256(), 0
                with os.fdopen(archive_fd, 'rb') as source:
                    archive_fd = None
                    for chunk in iter(lambda: source.read(1024 * 1024), b''):
                        total += len(chunk)
                        if total > payload['archive_bytes']:
                            raise ValueError
                        digest.update(chunk)
                        staged.write(chunk)
                if total != payload['archive_bytes'] or digest.hexdigest() != payload['archive_sha256']:
                    raise ValueError
                staged.flush()
                staged.seek(0)
                yield payload, staged
        except (OSError, ValueError, TypeError, KeyError):
            raise SharedBackupError('backup manifest or archive verification failed') from None
        finally:
            for descriptor in (manifest_fd, archive_fd, directory_fd):
                if descriptor is not None:
                    os.close(descriptor)

    def restore_audit(self, directory):
        """Restore verified staging bytes into an empty isolated database only."""
        with self._verified_archive(directory) as (payload, staged):
            target = self._request('recovery_target')
            if target == payload['seal']['payload']['source']:
                raise SharedBackupError('the original authority cannot be a restore target')
            env = postgres_environment(self.authority)
            database = env.get('PGDATABASE')
            if not database or '=' in database or database.startswith(('postgres://', 'postgresql://')):
                raise SharedBackupError('restore requires an explicit database name')
            try:
                result = subprocess.run([self.pg_restore, '--exit-on-error', '--single-transaction',
                    '--no-owner', '--no-acl', '--dbname', database, '--'], env=env,
                    stdin=staged, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    timeout=300, check=False)
                if result.returncode:
                    raise SharedBackupError('native restore failed; target must remain isolated')
            except (OSError, subprocess.TimeoutExpired):
                raise SharedBackupError('native restore failed; target must remain isolated') from None
            return self._request('recovery_audit', parameters={'seal': payload['seal'],
                'archive_sha256': payload['archive_sha256']})

    def verify_objects(self, read_object):
        """Read and hash every retained verified object through its trusted port.

        The reader receives namespace, digest and expected size and must return
        bytes or an iterable of byte chunks. Missing/corrupt objects fail closed.
        """
        report = self.inspect()
        if report['mode'] != 'audit':
            raise SharedBackupError('object recovery verification requires an audit clone')
        digest, count, after = hashlib.sha256(), 0, ('', '')
        while True:
            rows = self._request('recovery_objects', after_namespace=after[0], after_digest=after[1])
            for record in rows:
                chunks = read_object(record['namespace'], record['digest'], record['size'])
                if type(chunks) is bytes:
                    chunks = (chunks,)
                actual, size = hashlib.sha256(), 0
                for chunk in chunks:
                    if type(chunk) is not bytes:
                        raise SharedBackupError('invalid shared object stream')
                    size += len(chunk)
                    if size > record['size']:
                        raise SharedBackupError('shared object differs from retained allocation')
                    actual.update(chunk)
                if size != record['size'] or actual.hexdigest() != record['digest']:
                    raise SharedBackupError('shared object differs from retained allocation')
                encoded = canonical(record).encode()
                digest.update(len(encoded).to_bytes(8, 'big'))
                digest.update(encoded)
                count += 1
                after = (record['namespace'], record['digest'])
            if len(rows) < 1000:
                break
        state = report['state']
        if digest.hexdigest() != state['object_digest'] or count != state['object_count']:
            raise SharedBackupError('shared object inventory changed during verification')
        return {'object_digest': digest.hexdigest(), 'object_count': count,
                'state_digest': state['state_digest'], 'target': report['database'],
                'archive_sha256': report['restore']['archive_sha256'],
                'seal_digest': report['restore']['seal_digest']}

    def activate(self, *, fencing_proof, read_object):
        """Verify objects again, then consume an externally signed fencing proof.

        The result remains drained. The successor explicitly renews the returned
        epoch, reconciles unresolved effects, then changes drain through audit.
        """
        objects = self.verify_objects(read_object)
        payload = fencing_proof.get('payload', {})
        if any(payload.get(key) != objects[key] for key in objects) or payload.get('verified_object_digest') != objects['object_digest']:
            raise SharedBackupError('fencing attestation differs from verified shared object state')
        return self._request('recovery_activate', parameters={'proof': fencing_proof})

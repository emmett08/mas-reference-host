"""Seeded cross-language and accounting properties, without test dependencies.

Failures identify the seed and case (or the complete operation prefix), so the
same input can be replayed directly. The JSON oracle spells out the encoding
contract instead of calling the production serializer. Accounting expectations
are derived from the accepted event history rather than SQL or cached counters.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

from mas_host.accounting import AccountingAdministration, PeriodPolicy
from mas_host.codec import canonical_json, integer_json, is_actor_identity, json_identity
from mas_host.contracts import RunnerError
from mas_host.operations import DeploymentLedger, DeploymentPolicy, OperationsError
from mas_host.principal_quotas import PrincipalQuota


SEEDS = (0, 17, 0x5A17, 0xC0DEC)
TEXT = ('', 'a', '雪', '🧠', '\u007f', '\u0080', '\u2028', '\U0010ffff',
        'é', 'e\u0301', '\x00\b\t\n\f\r', '\\"/', '𐀀', '\ue000')


def oracle_json(value):
    """Small independent encoder for the generated integer-JSON domain."""
    if value is None:
        return 'null'
    if type(value) is bool:
        return 'true' if value else 'false'
    if type(value) is int:
        return str(value)
    if type(value) is str:
        escapes = {'"': '\\"', '\\': '\\\\', '\b': '\\b', '\f': '\\f',
                   '\n': '\\n', '\r': '\\r', '\t': '\\t'}
        return '"' + ''.join(escapes.get(c, '\\u%04x' % ord(c) if ord(c) < 32 else c)
                             for c in value) + '"'
    if type(value) is list:
        return '[' + ','.join(map(oracle_json, value)) + ']'
    return '{' + ','.join(oracle_json(k) + ':' + oracle_json(value[k])
                          for k in sorted(value)) + '}'


def generated_value(rng, depth=0):
    kind = rng.randrange(4 if depth == 4 else 6)
    if kind == 0:
        return rng.choice((None, True, False))
    if kind == 1:
        return rng.choice((-1, 1)) * rng.getrandbits(rng.choice((0, 1, 63, 64, 127, 512, 2048)))
    if kind in (2, 3):
        return ''.join(rng.choices(TEXT, k=rng.randrange(5)))
    if kind == 4:
        return [generated_value(rng, depth + 1) for _ in range(rng.randrange(5))]
    keys = rng.sample(TEXT, rng.randrange(5))
    return {key: generated_value(rng, depth + 1) for key in keys}


def reordered(value):
    if type(value) is dict:
        return {key: reordered(item) for key, item in reversed(list(value.items()))}
    if type(value) is list:
        return list(map(reordered, value))
    return value


def envelope(payload, sender='a'):
    return dict(message_id='m', run_id='r', sender=sender, recipient='b',
                version='1', kind='Notice', payload=payload)


class GeneratedCanonicalContracts(unittest.TestCase):
    def test_seeded_encoding_identity_and_utf8_byte_boundary(self):
        for seed in SEEDS:
            rng = random.Random(seed)
            for case in range(32):
                value = generated_value(rng)
                expected = oracle_json(value)
                with self.subTest(seed=seed, case=case, value=value):
                    self.assertEqual(canonical_json(value), expected)
                    self.assertEqual(json.loads(integer_json(value)), value)
                    self.assertEqual(json_identity(value), hashlib.sha256(expected.encode()).hexdigest())
                    self.assertEqual(json_identity(value), json_identity(reordered(value)))
                    size = len(expected.encode('utf-8'))
                    self.assertEqual(canonical_json(value, size), expected)
                    with self.assertRaises(RunnerError):
                        canonical_json(value, size - 1)

    def test_integer_and_type_boundaries_do_not_coerce(self):
        for digits in (1, 19, 20, 38, 1023, 1024, 1025):
            for sign in (-1, 1):
                value = {'integer': sign * (10**digits - 1)}
                with self.subTest(digits=digits, sign=sign):
                    if digits <= 1024:
                        self.assertEqual(canonical_json(value), oracle_json(value))
                    else:
                        with self.assertRaises(RunnerError):
                            canonical_json(value)
        class SubInt(int):
            pass
        class SubString(str):
            pass
        for invalid in (0.0, -0.0, float('nan'), float('inf'), (), set(), b'x',
                        SubInt(1), SubString('x'), {1: 'x'}, {'x': '\ud800'}, {'\udfff': 1}):
            for wrap in (lambda v: v, lambda v: {'nested': [v]}):
                with self.subTest(invalid=repr(invalid), wrap=wrap):
                    with self.assertRaises(RunnerError):
                        canonical_json(wrap(invalid))

    def test_semantic_changes_do_not_reuse_descriptor_identity(self):
        # The contract preserves Unicode code points, JSON types and array order.
        for left, right in ((1, True), (0, False), ('é', 'e\u0301'),
                            ([1, 2], [2, 1]), ({'x': None}, {}), ('1', 1)):
            with self.subTest(left=left, right=right):
                self.assertNotEqual(json_identity(left), json_identity(right))

    def test_explicit_protocol_depth_preserves_legacy_storage_identity(self):
        for leaf in ([], {}, 0):
            value = leaf
            for _ in range(32):
                value = [value]
            # Existing stored descriptors can end in an empty depth-32 container.
            # Their canonical bytes and identity must remain readable unchanged.
            expected = oracle_json(value)
            self.assertEqual(canonical_json(value), expected)
            self.assertEqual(json_identity(value), hashlib.sha256(expected.encode()).hexdigest())
            if type(leaf) is int:
                self.assertEqual(integer_json(value, max_depth=32), expected)
            else:
                with self.assertRaises(RunnerError):
                    integer_json(value, max_depth=32)
            self.assertEqual(integer_json(value, max_depth=33), expected)
        for invalid in (0, -1, 34, True, 32.0):
            with self.subTest(max_depth=invalid), self.assertRaises(RunnerError):
                integer_json({}, max_depth=invalid)


class GeneratedRustCorrespondence(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        release = Path(__file__).resolve().parents[2] / 'rust-protocol/target/release'
        cls.binary = (os.environ.get('MAS_CODEC_BIN') or shutil.which('mas-protocol-codec')
                      or str(release / 'mas-protocol-codec'))
        cls.public_cli = False
        if not Path(cls.binary).is_file():
            # Installed images expose the same Rust parser through mas; they
            # need not install the development-only standalone codec wrapper.
            cls.binary = os.environ.get('MAS_BIN') or shutil.which('mas') or str(release / 'mas')
            cls.public_cli = True
        if not Path(cls.binary).is_file():
            raise unittest.SkipTest('Rust codec and mas unavailable; build release or set MAS_BIN')
        # Contain a systematically hanging executable across all generated
        # subtests, instead of multiplying the per-process timeout 306 times.
        cls.deadline = time.monotonic() + 60

    def codec(self, operation, source):
        remaining = self.deadline - time.monotonic()
        self.assertGreater(remaining, 0, 'generated codec execution budget exhausted')
        command = [self.binary, operation] + (['-'] if self.public_cli else [])
        result = subprocess.run(command, input=source, text=True,
                                capture_output=True, timeout=min(10, remaining))
        self.assertIn(result.returncode, (0, 2), result.stderr)
        value = json.loads(result.stdout)
        self.assertEqual(result.returncode == 0, value['ok'])
        if self.public_cli and value['ok']:
            return {'ok': True, **({'text': value['result']['text']} if operation == 'encode'
                                  else {'message': value['result']})}
        return value

    def test_seeded_python_rust_canonical_payload_and_identity(self):
        for seed in SEEDS:
            rng = random.Random(seed)
            for case in range(24):
                payload = {'case': generated_value(rng), '雪': generated_value(rng)}
                expected = oracle_json(payload)
                with self.subTest(seed=seed, case=case, payload=payload):
                    encoded = self.codec('encode', json.dumps(envelope(payload), ensure_ascii=True))
                    self.assertTrue(encoded['ok'], encoded)
                    self.assertEqual(encoded['text'],
                        'message "m" run "r" from "a" to "b" version "1" kind "Notice" payload ' + expected + ';')
                    parsed = self.codec('parse', encoded['text'])
                    self.assertEqual(parsed['message'], envelope(payload))
                    self.assertEqual(json_identity(parsed['message']['payload']),
                                     hashlib.sha256(expected.encode()).hexdigest())

    def test_generated_actor_grammar_agrees_at_actual_codec_boundary(self):
        rng = random.Random(0xAC70)
        alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.:/-'
        values = ['', None, True, 0, [], 'a' * 127, 'a' * 128, 'a' * 129]
        for _ in range(40):
            candidate = rng.choice(alphabet[:62]) + ''.join(rng.choices(alphabet, k=rng.randrange(128)))
            values.extend((candidate, candidate + rng.choice(('é', '\x00', '\n', '\\', ' ', '@'))))
        for case, value in enumerate(values):
            with self.subTest(case=case, actor=value):
                result = self.codec('encode', json.dumps(envelope({}, value)))
                self.assertEqual(result['ok'], is_actor_identity(value), result)

    def test_integer_and_container_depth_boundaries_agree(self):
        for digits in (1023, 1024, 1025):
            for sign in (-1, 1):
                value = {'x': sign * (10**digits - 1)}
                with self.subTest(digits=digits, sign=sign):
                    result = self.codec('encode', json.dumps(envelope(value)))
                    self.assertEqual(result['ok'], digits <= 1024, result)
        for containers in (31, 32, 33):
            value = []
            for _ in range(containers - 2):
                value = [value]
            payload = {'x': value}  # Include the payload object in the depth.
            with self.subTest(containers=containers):
                result = self.codec('encode', json.dumps(envelope(payload)))
                self.assertEqual(result['ok'], containers <= 32, result)
                if containers <= 32:
                    integer_json(payload, max_depth=32)
                else:
                    with self.assertRaises(RunnerError):
                        integer_json(payload, max_depth=32)

    def test_runner_encode_envelope_keeps_the_full_rust_payload_depth(self):
        from mas_host.runner import _encode_wire

        def command(arguments, source):
            self.assertEqual(arguments, ['codec', 'encode', '-'])
            result = self.codec('encode', source)
            self.assertTrue(result['ok'], result)
            return result

        for leaf in ([], {}, 0):
            payload = leaf
            for _ in range(31 if type(leaf) is int else 30):
                payload = [payload]
            payload = {'x': payload}
            with self.subTest(leaf=leaf):
                source = _encode_wire(command, 'codec', 'r', 'a', 'b', 'Notice', payload, 'm')
                self.assertEqual(self.codec('parse', source)['message']['payload'], payload)
                with self.assertRaises(RunnerError):
                    _encode_wire(command, 'codec', 'r', 'a', 'b', 'Notice', {'extra': payload}, 'm')

    def test_adversarial_lexemes_never_enter_canonical_identity(self):
        invalid = ('{"x":1,"\\u0078":2}', '{"x":1.0}', '{"x":1e0}',
                   '{"x":01}', '{"x":+1}', '{"x":NaN}', '{"x":true,}',
                   '{"x":"\\ud800"}', '{"x":"\\udfff"}', '{"x":"\\u000g"}')
        prefix = 'message "m" run "r" from "a" to "b" version "1" kind "Notice" payload '
        for payload in invalid:
            with self.subTest(payload=payload):
                self.assertFalse(self.codec('parse', prefix + payload + ';')['ok'])
        result = self.codec('parse', prefix + '{"x":-0};')
        self.assertEqual(result['message']['payload'], {'x': 0})


class GeneratedAllocationContracts(unittest.TestCase):
    def test_seeded_all_scope_conservation_replay_conflict_and_restart(self):
        for seed in SEEDS:
            with self.subTest(seed=seed), tempfile.TemporaryDirectory() as directory:
                rng = random.Random(seed)
                root = Path(directory) / 'state'
                ledger = DeploymentLedger(root, DeploymentPolicy(max_calls=20, max_reserved_microusd=96,
                    per_call_reservation_microusd=1, min_free_bytes=1), principal_quotas={
                    owner: PrincipalQuota(max_calls=12, max_reserved_microusd=64) for owner in ('alice', 'bob')})
                for owner in ('alice', 'bob'):
                    ledger.admit_run(owner, root / owner, principal=owner)
                with patch('time.time', return_value=120):
                    admin = AccountingAdministration(ledger)
                    for scope in ('deployment', 'principal:alice', 'principal:bob'):
                        admin.configure_period(scope, PeriodPolicy(60, 6 if scope == 'deployment' else 3, 30),
                            request_key=scope, actor='operator', reason='generated allocation contract')
                accepted, trace = {}, []
                high_water = dict.fromkeys(('deployment', 'alice', 'bob'), 2)
                for step in range(80):
                    owner, call = rng.choice(('alice', 'bob')), 'call-' + str(rng.randrange(16))
                    amount = rng.choice((0, 1, 5, 10, 29, 30, 31, 64, 96, -1, True, 1.5))
                    if accepted and (step % 7 == 0 or step % 11 == 0):
                        owner, call = rng.choice(list(accepted))
                        amount = accepted[owner, call][0] + (step % 7 != 0)
                    timestamp = 120 + (step // 20) * 60 + rng.choice((0, 59, 0, -61))
                    bucket = timestamp // 60
                    key = owner, call
                    prior = accepted.get(key)
                    rows = list(accepted.values())
                    own = [v for (p, _), v in accepted.items() if p == owner]
                    period = [v for v in rows if v[1] == bucket]
                    own_period = [v for v in own if v[1] == bucket]
                    valid = type(amount) is int and 0 <= amount <= 96
                    duplicate = valid and prior is not None and prior[0] == amount
                    fresh = valid and prior is None and len(rows) < 20 and sum(v[0] for v in rows) + amount <= 96
                    fresh = fresh and len(own) < 12 and sum(v[0] for v in own) + amount <= 64
                    fresh = fresh and bucket >= max(high_water['deployment'], high_water[owner])
                    fresh = fresh and len(period) < 6 and len(own_period) < 3
                    fresh = fresh and sum(v[0] for v in period) + amount <= 30
                    fresh = fresh and sum(v[0] for v in own_period) + amount <= 30
                    trace.append((owner, call, amount, timestamp))
                    with self.subTest(seed=seed, step=step, prefix=trace.copy()), patch('time.time', return_value=timestamp):
                        if fresh or duplicate:
                            self.assertEqual(ledger.admit_call(owner, call, amount), bool(fresh))
                            if fresh:
                                accepted[key] = amount, bucket
                                high_water['deployment'] = high_water[owner] = bucket
                        else:
                            with self.assertRaises(OperationsError):
                                ledger.admit_call(owner, call, amount)
                        usage = ledger.health()['usage']
                        self.assertEqual(usage, {'runs': 2, 'calls': len(accepted),
                            'reserved_microusd': sum(v[0] for v in accepted.values())})
                        for principal in ('alice', 'bob'):
                            amounts = [v[0] for (p, _), v in accepted.items() if p == principal]
                            self.assertEqual(ledger.principal_usage(principal),
                                             {'calls': len(amounts), 'reserved_microusd': sum(amounts),
                                              'unattributed_calls': 0, 'unattributed_reserved_microusd': 0})
                        for observed in AccountingAdministration(ledger).periods():
                            selected = [v[0] for (p, _), v in accepted.items()
                                        if v[1] == observed['period'] and observed['scope'] in ('deployment', 'principal:' + p)]
                            self.assertEqual((observed['calls'], observed['reserved_microusd']),
                                             (len(selected), sum(selected)))
                    if step % 9 == 0:
                        ledger = DeploymentLedger(root)

    def test_integer_ceiling_and_charge_observations_never_refund_reservations(self):
        maximum = 2**63 - 1
        for seed in SEEDS:
            with self.subTest(seed=seed), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / 'state'
                ledger = DeploymentLedger(root, DeploymentPolicy(max_calls=3, max_reserved_microusd=maximum,
                    per_call_reservation_microusd=1, min_free_bytes=1))
                ledger.admit_run('r', root / 'r')
                first = random.Random(seed).randrange(1, maximum)
                ledger.admit_call('r', 'a', first)
                ledger.admit_call('r', 'b', maximum - first)
                with self.assertRaises(OperationsError):
                    ledger.admit_call('r', 'over-budget', 1)
                with self.assertRaises(OperationsError):
                    ledger.admit_call('r', 'a', first - 1)
                ledger.admit_call('r', 'zero', 0)
                for amount in (0, 1, maximum, maximum + 1, -1, True):
                    with self.assertRaises(OperationsError):
                        ledger.admit_call('r', 'extra', amount)
                admin = AccountingAdministration(ledger)
                for index, amount in enumerate((0, 1, maximum)):
                    command = dict(provider='provider', charge_id='charge-' + str(index), run_id='r',
                        call_id='a', actual_microusd=amount, observed_at=0, evidence='invoice', actor='operator')
                    self.assertEqual(admin.record_charge(**command), admin.record_charge(**command))
                    with self.assertRaises(OperationsError):
                        admin.record_charge(**{**command, 'evidence': 'changed'})
                self.assertEqual(admin.reconcile('r', 'a')['observed_charge_microusd'], maximum + 1)
                self.assertEqual(DeploymentLedger(root).health()['usage']['reserved_microusd'], maximum)


if __name__ == '__main__':
    unittest.main()

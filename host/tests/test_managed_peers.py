"""Managed federation over real local HTTP, MCP and A2A task services."""
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from mas_host.operations import DeploymentLedger, backup_state, restore_state
from mas_host.peer_registry import PeerRegistration, PeerRegistry
from mas_host.service_contracts import ServiceError
from mas_host.bridges.a2a import A2ABridge
from mas_host.bridges.mcp import MCPBridge
from mas_host.bridges.managed_peer import ManagedPeerConnector
from mas_host.bridges.http_policy import HTTPTransportPolicy
from mas_host.bridges.orchestrator import stable_request_key
from test_bridge_server import ServerFixture, TOKEN, OTHER
import test_task_service


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.operations = DeploymentLedger(self.root / 'state')
        self.registry = PeerRegistry(self.operations)

    def registration(self, **options):
        return PeerRegistration(**({'name': 'worker-host', 'version': '1', 'profile': 'mas-http/1',
            'endpoint': 'https://worker.example', 'outbound_principal': 'alice',
            'deployment_id': 'remote-deployment', 'workflows': {'echo': 'a' * 64}, 'callers': ['orchestrator']} | options))

    def register(self, registration=None, *, key='install', revision=0):
        return self.registry.register(operator='owner', request_key=key, reason='Test registration',
            expected_revision=revision, registration=registration or self.registration())

    def test_version_and_request_identity_revocation_and_caller_authorisation(self):
        first = self.register()
        self.assertEqual(first, self.register())
        with self.assertRaisesRegex(ServiceError, 'peer change conflict'):
            self.register(self.registration(endpoint='https://another.example'))
        with self.assertRaisesRegex(ServiceError, 'peer version immutable'):
            self.register(self.registration(endpoint='https://another.example'), key='replace', revision=1)
        with self.assertRaisesRegex(ServiceError, 'peer forbidden'):
            self.registry.resolve('worker-host', 'outsider')
        original = self.registry.resolve('worker-host', 'orchestrator')
        self.register(self.registration(version='2'), key='install2', revision=1)
        self.assertEqual(self.registry.resolve('worker-host', 'orchestrator').descriptor['version'], '2')
        self.assertEqual(self.registry.resolve('worker-host', 'orchestrator', version='1').identity, original.identity)
        self.registry.revoke(operator='owner', request_key='revoke', reason='Credential withdrawn',
            expected_revision=2, name='worker-host', version='1')
        with self.assertRaisesRegex(ServiceError, 'peer unavailable'):
            self.registry.resolve('worker-host', 'orchestrator', version='1')
        with self.assertRaisesRegex(ServiceError, 'peer version immutable'):
            self.register(key='revive', revision=3)
        self.assertEqual(self.registry.status()['revision'], 3)

    def test_validation_profile_endpoint_and_untrusted_data(self):
        for options in ({'profile': 'mas-mcp/2'}, {'profile': []}, {'capabilities': [[]]},
                        {'endpoint': 'https://user:secret@worker.example'}, {'callers': []},
                        {'profile': 'mas-mcp/1', 'endpoint': 'https://worker.example/service'},
                        {'allow_loopback_http': True, 'endpoint': 'http://worker.example'}):
            with self.subTest(options=options), self.assertRaises(ServiceError):
                self.registration(**options)
        value = self.registration().descriptor
        with self.assertRaises(ServiceError):
            PeerRegistration.from_dict(value | {'token': 'must-not-be-persisted'})

    def test_backup_readonly_registry_and_revision_compare_and_swap(self):
        self.register()
        with self.assertRaisesRegex(ServiceError, 'peer revision conflict'):
            self.register(self.registration(version='2'), key='stale')
        backup_state(self.operations.state_dir, self.root / 'backup')
        restore_state(self.root / 'backup', self.root / 'restored')
        restored = PeerRegistry(DeploymentLedger(self.root / 'restored', readonly=True))
        self.assertEqual(restored.status(), self.registry.status())
        with self.assertRaisesRegex(ServiceError, 'audit only'):
            restored.revoke(operator='owner', request_key='no', reason='Read only', expected_revision=1,
                            name='worker-host', version='1')

    def test_transport_policy_and_timeout_are_immutable_public_peer_bindings(self):
        original = self.registration()
        self.assertNotIn('transport_policy', original.descriptor)
        self.assertNotIn('timeout_seconds', original.descriptor)
        self.assertEqual(PeerRegistration.from_dict(original.descriptor)._encoded, original._encoded)
        self.register(original)
        default = ManagedPeerConnector(self.registry, 'worker-host', 'orchestrator', token_file='/unused')
        explicit = ManagedPeerConnector(self.registry, 'worker-host', 'orchestrator', token_file='/unused',
                                        transport_policy=HTTPTransportPolicy())
        self.assertEqual(default.descriptor(), explicit.descriptor())
        policy = HTTPTransportPolicy(proxy_url='http://proxy.example:8080',
            proxy_authorization_file='/private/old-credential', allowed_origins=('https://worker.example',))
        changed = self.registration(transport_policy=policy.descriptor(), timeout_seconds=17)
        self.assertNotEqual(changed.identity, original.identity)
        with self.assertRaisesRegex(ServiceError, 'peer version immutable'):
            self.register(changed, key='changed-transport', revision=1)
        pinned = self.registration(version='2', transport_policy=policy.descriptor(), timeout_seconds=17)
        self.register(pinned, key='transport-v2', revision=1)
        rotated = replace(policy, proxy_authorization_file='/private/rotated-credential')
        peer = ManagedPeerConnector(self.registry, 'worker-host', 'orchestrator', token_file='/private/token',
            transport_policy=rotated, timeout_seconds=17)
        self.assertEqual(peer.registration.identity, pinned.identity)
        self.assertEqual(peer.descriptor()['transport_policy'], policy.descriptor())
        self.assertEqual(peer.descriptor()['timeout_seconds'], 17)
        self.assertNotIn('/private/', str(pinned.descriptor) + str(peer.descriptor()))
        self.assertEqual(PeerRegistration.from_dict(pinned.descriptor)._encoded, pinned._encoded)
        # Old aliases/versions keep their original direct transport binding.
        self.assertEqual(self.registry.resolve('worker-host', 'orchestrator', version='1').identity, original.identity)
        for options in ({'transport_policy': None, 'timeout_seconds': 17},
                        {'transport_policy': policy, 'timeout_seconds': 30},
                        {'transport_policy': replace(policy, proxy_url='http://other.example:8080'), 'timeout_seconds': 17},
                        {'transport_policy': replace(policy, allowed_origins=()), 'timeout_seconds': 17}):
            with self.subTest(options=options), mock.patch(
                    'mas_host.bridges.managed_peer.read_token_file') as secret:
                with self.assertRaisesRegex(ServiceError, 'peer binding changed'):
                    ManagedPeerConnector(self.registry, 'worker-host', 'orchestrator', token_file='/unused', **options)
                secret.assert_not_called()

    def test_transport_descriptor_rejects_secrets_and_invalid_timeout(self):
        policy = HTTPTransportPolicy().descriptor()
        for extra in ({'proxy_authorization_file': '/secret'}, {'additional_ca_pem': 'secret'},
                      {'additional_ca_sha256': 'bad'}, {'loopback_proxy_bypass': False},
                      {'proxy_authentication': True}):
            with self.subTest(extra=extra), self.assertRaises(ServiceError):
                self.registration(transport_policy=policy | extra)
        for timeout in (0, 61, True, 1.5):
            with self.subTest(timeout=timeout), self.assertRaises(ServiceError):
                self.registration(timeout_seconds=timeout)
        with self.assertRaises(ServiceError):
            PeerRegistration.from_dict(self.registration().descriptor | {'timeout_seconds': 30})
        for origins in (['https://worker.example'], ['https://worker.example:0443'],
                        ['https://b.example:443', 'https://a.example:443'],
                        ['https://a.example:443', 'https://a.example:443']):
            with self.subTest(origins=origins), self.assertRaises(ServiceError):
                self.registration(transport_policy=policy | {'allowed_origins': origins})
        ca_one = self.registration(transport_policy=policy | {'additional_ca_sha256': 'a' * 64})
        ca_two = self.registration(transport_policy=policy | {'additional_ca_sha256': 'b' * 64})
        self.assertNotEqual(ca_one.identity, ca_two.identity)

    def test_phase_timeout_changes_require_a_new_peer_binding(self):
        budgets = dict.fromkeys(('dns', 'connect', 'tls', 'headers', 'body'), 1000)
        policy = HTTPTransportPolicy(phase_timeouts_ms=budgets)
        registration = self.registration(transport_policy=policy.descriptor())
        self.register(registration)
        peer = ManagedPeerConnector(self.registry, 'worker-host', 'orchestrator',
                                    token_file='/private/token', transport_policy=policy)
        self.assertEqual(peer.registration.identity, registration.identity)
        self.assertEqual(PeerRegistration.from_dict(registration.descriptor).identity, registration.identity)
        changed = HTTPTransportPolicy(phase_timeouts_ms={**budgets, 'body': 2000})
        self.assertNotEqual(self.registration(transport_policy=changed.descriptor()).identity,
                            registration.identity)
        with self.assertRaisesRegex(ServiceError, 'peer version immutable'):
            self.register(self.registration(transport_policy=changed.descriptor()), key='timeout-change', revision=1)
        peer.service.transport_policy = changed
        with mock.patch('mas_host.bridges.managed_peer.read_token_file') as secret:
            with self.assertRaisesRegex(ServiceError, 'peer binding changed'):
                peer.negotiate()
            secret.assert_not_called()

    def test_mutated_lifecycle_or_artifact_transport_fails_before_credentials(self):
        self.register()
        for target in ('service', '_artifact_service'):
            for field, value in (('transport_policy', HTTPTransportPolicy(proxy_url='http://changed.example')),
                                 ('timeout_seconds', 12), ('endpoint', 'https://changed.example'),
                                 ('principal', 'outsider'), ('allow_loopback_http', True)):
                peer = ManagedPeerConnector(self.registry, 'worker-host', 'orchestrator', token_file='/unused')
                setattr(getattr(peer, target), field, value)
                with self.subTest(target=target, field=field), mock.patch(
                        'mas_host.bridges.managed_peer.read_token_file') as secret:
                    with self.assertRaisesRegex(ServiceError, 'peer binding changed'):
                        peer.negotiate()
                    with self.assertRaisesRegex(ServiceError, 'peer binding changed'):
                        peer.artifacts()
                    secret.assert_not_called()


class ManagedPeerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_task_service.TaskServiceTests.setUpClass()
        cls.mas, cls.gateway = test_task_service.TaskServiceTests.mas, test_task_service.TaskServiceTests.gateway

    def setUp(self):
        test_task_service.TaskServiceTests.setUp(self)
        self.token_file = self.root / 'token'
        self.token_file.write_text(TOKEN)
        self.token_file.chmod(0o600)
        self.registry = PeerRegistry(DeploymentLedger(self.root / 'local-state'))

    def service(self, **options):
        return test_task_service.TaskServiceTests.service(self, **options)

    @contextmanager
    def peer(self, profile, *, capabilities=None, artifact_scopes=(), artifact_transfer=None):
        remote = self.service()
        with ServerFixture(remote, mcp_bridge=MCPBridge(remote),
                a2a_bridge=A2ABridge(remote, endpoint='http://127.0.0.1/a2a', allow_http=True),
                artifact_transfer=artifact_transfer) as server:
            server.server.a2a_bridge.endpoint = server.endpoint + '/a2a'
            endpoint = server.endpoint + {'mas-http/1': '', 'mas-mcp/1': '/mcp', 'mas-a2a/1': '/a2a'}[profile]
            registration = PeerRegistration(name='worker', version='1', profile=profile, endpoint=endpoint,
                outbound_principal='alice', deployment_id=remote.operations.deployment_id,
                workflows={'echo': self.workflow.identity}, callers=['local-user'],
                capabilities=capabilities or ['submit', 'inspect', 'result', 'cancel', 'update'],
                artifact_scopes=artifact_scopes, allow_loopback_http=True)
            self.registry.register(operator='owner', request_key='install', reason='Loopback acceptance',
                expected_revision=0, registration=registration)
            yield ManagedPeerConnector(self.registry, 'worker', 'local-user', token_file=self.token_file), remote

    def test_each_profile_recovers_lost_first_receipt_without_duplicate_work(self):
        for profile in ('mas-http/1', 'mas-mcp/1', 'mas-a2a/1'):
            with self.subTest(profile=profile):
                # Independent local/remote state for every protocol profile.
                self.state = self.root / profile.replace('/', '-')
                self.registry = PeerRegistry(DeploymentLedger(self.root / ('local-' + profile.replace('/', '-'))))
                with self.peer(profile) as (peer, remote):
                    key = stable_request_key('outer-workflow-1', 'step-a')
                    real_submit = peer.service.submit
                    def lose_reply(*args, **kwargs):
                        real_submit(*args, **kwargs)
                        raise ServiceError('transport_unknown')
                    with mock.patch.object(peer.service, 'submit', side_effect=lose_reply):
                        with self.assertRaises(ServiceError):
                            peer.submit(key, 'echo', {'answer': 42})
                    task = peer.submit(key, 'echo', {'answer': 42})
                    self.assertEqual(len(remote.list_tasks('alice')), 1)
                    self.assertEqual(peer.inspect(task['task_id'])['workflow_identity'], self.workflow.identity)
                    remote.run_once()
                    self.assertEqual(peer.result(task['task_id']), {'answer': 42})
                    self.assertEqual(peer.cancel(task['task_id'])['state'], 'completed')

    def test_wrong_authenticated_principal_and_revocation_fail_before_submission(self):
        with self.peer('mas-http/1') as (peer, remote):
            self.token_file.write_text(OTHER)
            with self.assertRaisesRegex(ServiceError, 'peer identity mismatch'):
                peer.submit('wrong-token', 'echo', {})
            self.assertEqual(remote.list_tasks('bob'), [])
            self.token_file.write_text(TOKEN)
            task = peer.submit('good-token', 'echo', {})
            self.registry.revoke(operator='owner', request_key='revoke', reason='Access ended',
                expected_revision=1, name='worker', version='1')
            for operation in (lambda: peer.negotiate(), lambda: peer.inspect(task['task_id']),
                              lambda: peer.cancel(task['task_id']), lambda: peer.submit('new', 'echo', {})):
                with self.assertRaisesRegex(ServiceError, 'peer unavailable'):
                    operation()
            self.assertFalse(remote.inspect('alice', task['task_id'])['cancel_requested'])

    def test_credential_rotation_cannot_change_principal_between_discovery_and_submit(self):
        for profile in ('mas-http/1', 'mas-mcp/1', 'mas-a2a/1'):
            with self.subTest(profile=profile):
                self.state = self.root / profile.replace('/', '-')
                self.registry = PeerRegistry(DeploymentLedger(self.root / ('local-' + profile.replace('/', '-'))))
                self.token_file.write_text(TOKEN)
                with self.peer(profile) as (peer, remote):
                    original = remote.discover
                    def rotate_after_discovery(principal):
                        result = original(principal)
                        self.token_file.write_text(OTHER)
                        return result
                    with mock.patch.object(remote, 'discover', side_effect=rotate_after_discovery):
                        task = peer.submit('rotation', 'echo', {})
                    self.assertEqual(remote.inspect('alice', task['task_id'])['task_id'], task['task_id'])
                    self.assertEqual(remote.list_tasks('bob'), [])
                    with self.assertRaisesRegex(ServiceError, 'peer identity mismatch'):
                        peer.submit('next-operation', 'echo', {})
                    self.assertEqual(remote.list_tasks('bob'), [])

    def test_workflow_and_deployment_drift_block_mutation(self):
        with self.peer('mas-mcp/1') as (peer, remote):
            original = remote.discover
            for drift in ('deployment', 'workflow'):
                def changed(principal):
                    result = original(principal)
                    if drift == 'deployment':
                        result['deployment_id'] = 'different-deployment'
                    else:
                        result['registrations'][0]['identity'] = '0' * 64
                    return result
                with mock.patch.object(remote, 'discover', side_effect=changed):
                    with self.assertRaisesRegex(ServiceError, 'peer contract mismatch'):
                        peer.submit('drift', 'echo', {})
            self.assertEqual(remote.list_tasks('alice'), [])

    def test_transport_drift_after_discovery_blocks_effect_across_profiles(self):
        for profile in ('mas-http/1', 'mas-mcp/1', 'mas-a2a/1'):
            with self.subTest(profile=profile):
                self.state = self.root / profile.replace('/', '-')
                self.registry = PeerRegistry(DeploymentLedger(self.root / ('local-' + profile.replace('/', '-'))))
                with self.peer(profile) as (peer, remote):
                    original = remote.discover
                    def changed(principal):
                        result = original(principal)
                        peer.service.transport_policy = HTTPTransportPolicy(proxy_url='http://changed.example')
                        return result
                    with mock.patch.object(remote, 'discover', side_effect=changed):
                        with self.assertRaisesRegex(ServiceError, 'peer binding changed'):
                            peer.submit('transport-drift', 'echo', {})
                    self.assertEqual(remote.list_tasks('alice'), [])

    def test_continuation_reuses_logical_key_across_profiles(self):
        self.workflow = replace(self.workflow,
            classify=lambda outcome: 'completed' if 'answer' in outcome.output else 'input_required',
            continue_input=lambda previous, supplied: previous | supplied)
        for profile in ('mas-http/1', 'mas-mcp/1', 'mas-a2a/1'):
            with self.subTest(profile=profile):
                self.state = self.root / profile.replace('/', '-')
                self.registry = PeerRegistry(DeploymentLedger(self.root / ('local-' + profile.replace('/', '-'))))
                with self.peer(profile) as (peer, remote):
                    task = peer.submit('question', 'echo', {'question': 'amount?'})
                    self.assertEqual(remote.run_once()['state'], 'input_required')
                    first = peer.update(task['task_id'], 'answer', {'answer': 8})
                    self.assertEqual(peer.update(task['task_id'], 'answer', {'answer': 8}), first)
                    self.assertEqual(remote.inspect('alice', task['task_id'])['revision'], 1)
                    remote.run_once()
                    self.assertEqual(peer.result(task['task_id']), {'question': 'amount?', 'answer': 8})

    def test_capability_and_unknown_profile_negotiation_fail_closed(self):
        with self.peer('mas-mcp/1', capabilities=['inspect']) as (peer, remote):
            with self.assertRaisesRegex(ServiceError, 'peer capability forbidden'):
                peer.submit('forbidden', 'echo', {})
            from mas_host.bridges.mcp import MCPBridge, TASKS
            with mock.patch.object(MCPBridge, 'discover', return_value={
                    'resultType': 'complete', 'supportedVersions': ['2026-07-28'],
                    'capabilities': {'extensions': {TASKS: {}}}}):
                with self.assertRaisesRegex(ServiceError, 'peer profile mismatch'):
                    peer.negotiate()
            self.assertEqual(remote.list_tasks('alice'), [])

    def test_artifact_scope_binding_and_revocation_check_each_chunk(self):
        from mas_host.artifacts import ArtifactStore
        from mas_host.bridges.artifact_transfer import ArtifactScope, ArtifactTransferService
        repository = ArtifactStore(self.root / 'remote-artifacts', namespace='outputs', principal='writer')
        ref = repository.put_bytes(b'large-result' * 10000)
        service = ArtifactTransferService(self.root / 'transfers', {'results': ArtifactScope(repository.root,
            'outputs', {'alice': 'writer'})})
        target = ArtifactStore(self.root / 'local-artifacts', namespace='inputs', principal='consumer')
        with self.peer('mas-a2a/1', artifact_scopes=['results'], artifact_transfer=service) as (peer, _):
            client = peer.artifacts()
            with self.assertRaisesRegex(ServiceError, 'peer artifact forbidden'):
                client.materialize('secret', ref, target)
            result = client.materialize('results', ref, target)
            self.assertEqual(target.read_bytes(result['reference']), repository.read_bytes(ref))
            first = client.exchange('download_begin', {'scope': 'results', 'reference': ref.to_dict()})
            self.registry.revoke(operator='owner', request_key='revoke', reason='Access ended',
                expected_revision=1, name='worker', version='1')
            with self.assertRaisesRegex(ServiceError, 'peer unavailable'):
                client.exchange('download_chunk', {'transfer_id': first['transfer_id'], 'offset': 0})


if __name__ == '__main__':
    unittest.main()

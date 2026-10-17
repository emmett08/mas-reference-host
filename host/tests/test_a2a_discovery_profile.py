"""Discovery is a filtered description; negotiated profiles never grant authority."""
import unittest
from unittest.mock import patch

from mas_host.bridges.a2a import A2ABridge, BOUNDED_PROFILE, SERVICE_PROFILE
from mas_host.bridges.peer_clients import A2ATaskServiceClient
from mas_host.service_contracts import ServiceError
from test_a2a_bridge import validate
from test_bridge_server import ServerFixture, TOKEN, OTHER


class DiscoveryService:
    def __init__(self):
        self.reads = []

    def discover(self, principal):
        if principal not in {'alice', 'bob'}:
            raise ServiceError('unauthorised')
        self.reads.append(principal)
        return {'format': 'mas-task-service/1', 'principal': principal,
                'deployment_id': 'test-deployment', 'capabilities': ['inspect'],
                'registrations': [{'name': principal + '-review', 'kind': 'workflow',
                                   'available': True, 'identity': 'pinned', 'contract': None}]}


def discovery_request():
    return {'jsonrpc': '2.0', 'id': 1, 'method': 'GetExtendedAgentCard', 'params': {}}


class DiscoveryProfileTests(unittest.TestCase):
    def test_extended_skills_are_authorised_and_public_card_has_no_registrations(self):
        service = DiscoveryService()
        bridge = A2ABridge(service)
        public = bridge.agent_card()
        validate('Agent Card', public)
        self.assertEqual([s['id'] for s in public['skills']], ['bounded-workflow'])
        self.assertEqual(service.reads, [])
        for principal in ('alice', 'bob'):
            card = bridge.extended_agent_card(principal)
            validate('Agent Card', card)
            self.assertEqual([s['id'] for s in card['skills']],
                             ['bounded-workflow', 'registration:' + principal + '-review'])
        self.assertNotIn('alice-review', str(bridge.extended_agent_card('bob')))

    def test_required_and_unknown_profiles_fail_before_discovery(self):
        service = DiscoveryService()
        bridge = A2ABridge(service, required_extensions=(BOUNDED_PROFILE,))
        request = discovery_request()
        base = {'A2A-Version': '1.0'}
        self.assertEqual(bridge.handle('alice', request, base)['error']['code'], -32008)
        self.assertEqual(bridge.handle('alice', request, base | {
            'A2A-Extensions': BOUNDED_PROFILE + ',urn:mas:bounded-json:2'})['error']['code'], -32004)
        self.assertEqual(service.reads, [])
        response = bridge.handle('alice', request, base | {'a2a-extensions': BOUNDED_PROFILE})
        validate('Agent Card', response['result'])
        declared = {x['uri']: x for x in response['result']['capabilities']['extensions']}
        self.assertTrue(declared[BOUNDED_PROFILE]['required'])
        self.assertEqual(service.reads, ['alice'])

    def test_optional_profile_preserves_legacy_calls_and_rejects_malformed_selection(self):
        bridge = A2ABridge(DiscoveryService())
        self.assertIn('result', bridge.handle('alice', discovery_request()))
        for value in (' ', BOUNDED_PROFILE + ',', BOUNDED_PROFILE + ',' + BOUNDED_PROFILE):
            response = bridge.handle('alice', discovery_request(), {'A2A-Extensions': value})
            self.assertEqual(response['error']['code'], -32602)
        with self.assertRaises(ServiceError):
            A2ABridge(DiscoveryService(), required_extensions=('urn:unknown:1',))

    def test_http_preserves_negotiation_and_caller_filter(self):
        service = DiscoveryService()
        bridge = A2ABridge(service, required_extensions=(SERVICE_PROFILE,))
        with ServerFixture(service, a2a_bridge=bridge, request_log=lambda _: None) as server:
            base = {'A2A-Version': '1.0'}
            result = server.request('/a2a', discovery_request(), headers=base)[2]
            self.assertEqual(result['error']['code'], -32008)
            selected = base | {'A2A-Extensions': SERVICE_PROFILE}
            for token, name in ((TOKEN, 'alice'), (OTHER, 'bob')):
                _, headers, result = server.request('/a2a', discovery_request(), token=token, headers=selected)
                self.assertEqual(headers['A2A-Extensions'], SERVICE_PROFILE)
                self.assertEqual(result['result']['skills'][1]['id'], 'registration:' + name + '-review')

    def test_managed_client_sends_its_pinned_profile(self):
        service = DiscoveryService()
        bridge = A2ABridge(service, endpoint='https://peer.example/a2a',
                          required_extensions=(SERVICE_PROFILE, BOUNDED_PROFILE))
        import json
        calls = []

        def exchange(endpoint, **options):
            calls.append(options['headers'])
            request = json.loads(options['body'])
            return 200, {'content-type': 'application/json'}, json.dumps(
                bridge.handle('alice', request, options['headers'])).encode()

        client = A2ATaskServiceClient('https://peer.example/a2a', principal='alice', token_file=None,
                                      token_provider=lambda: TOKEN)
        with patch('mas_host.bridges.peer_clients.http_exchange', side_effect=exchange):
            self.assertEqual(client.discover('alice')['principal'], 'alice')
        self.assertEqual(set(calls[0]['A2A-Extensions'].split(',')), {SERVICE_PROFILE, BOUNDED_PROFILE})


if __name__ == '__main__':
    unittest.main()

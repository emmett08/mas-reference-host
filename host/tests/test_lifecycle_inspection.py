"""Unified graph and A2A extensions preserve ownership, bounds and scopes."""
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest

from mas_host.bridges.a2a import A2ABridge, LIFECYCLE_PROFILE
from mas_host.bridges.mcp import MCPBridge, metadata
from mas_host.bridges.service_client import TaskServiceClient
from mas_host.service_contracts import ServiceError
from mas_host.shared_service import SharedTaskFacade
from mas_host.unified_service import UnifiedTaskService
from test_bridge_server import ServerFixture, READ, OTHER, TOKEN
from test_definition_versions import Executor
from test_workflow_engine_contracts import workflow, specification


class LifecycleInspectionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.service = UnifiedTaskService(self.root, {'echo': workflow()}, {'example': specification()},
            {'alice': {'echo', 'example'}, 'bob': {'echo'}}, executor=Executor())
        self.task = self.service.submit('alice', 'graph', 'example', {})['task_id']
        with self.service.compositions.store.transaction() as db:
            for index in range(3):
                db.execute("INSERT INTO nodes(instance,path,kind,input,state,output) VALUES(?,?,?,'{}','completed',?)",
                           (self.task, 'root/n' + str(index), 'task', '{"private":"leaf-output"}'))

    def test_graph_pages_are_structural_bounded_and_owner_authorised(self):
        page = self.service.inspect_graph('alice', self.task, limit=2)
        self.assertEqual(page['total'], 3)
        self.assertEqual(page['next_offset'], 2)
        self.assertEqual(set(page['items'][0]), {'path', 'kind', 'state'})
        self.assertEqual(self.service.inspect_graph('alice', self.task, offset=2)['items'][0]['path'], 'root/n2')
        with self.assertRaises(ServiceError):
            self.service.inspect_graph('bob', self.task)
        for options in ({'section': 'input'}, {'limit': True}, {'offset': -1}):
            with self.assertRaises(ServiceError):
                self.service.inspect_graph('alice', self.task, **options)

    def test_http_mcp_a2a_share_graph_and_search_results_and_recovery_scope(self):
        a2a, mcp = A2ABridge(self.service), MCPBridge(self.service)
        expected = self.service.inspect_graph('alice', self.task)
        card = a2a.extended_agent_card('alice')
        self.assertIn(LIFECYCLE_PROFILE, [value['uri'] for value in card['capabilities']['extensions']])
        with ServerFixture(self.service, a2a_bridge=a2a, mcp_bridge=mcp, request_log=lambda _: None) as server:
            client = TaskServiceClient(server.endpoint, principal='alice', token_provider=lambda: TOKEN,
                                       allow_loopback_http=True)
            self.assertEqual(client.inspect_graph('alice', self.task), expected)
            request = {'jsonrpc': '2.0', 'id': 1, 'method': 'mas/InspectGraph', 'params': {'task_id': self.task}}
            status, _, response = server.request('/a2a', request, token=READ, headers={'A2A-Version': '1.0'})
            self.assertEqual(status, 200)
            self.assertEqual(response['result'], expected)
            self.assertIn('error', server.request('/a2a', request, token=OTHER, headers={'A2A-Version': '1.0'})[2])
            recover = {**request, 'method': 'mas/RecoverTask'}
            self.assertEqual(server.request('/a2a', recover, token=READ, headers={'A2A-Version': '1.0'})[0], 403)
        response = mcp.handle('alice', {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call',
            'params': {'name': 'mas.graph', 'arguments': {'task_id': self.task}, '_meta': metadata(tasks=True)}})
        self.assertEqual(response['result']['structuredContent'], expected)
        page = a2a.handle('alice', {'jsonrpc': '2.0', 'id': 3, 'method': 'mas/ListTaskPage',
            'params': {'state': 'queued', 'page_size': 1}})['result']
        self.assertEqual(page['tasks'][0]['task_id'], self.task)

    def test_shared_graph_never_uses_controller_cache_as_authority(self):
        expected = {'workflow': 'example', 'items': []}
        class Authority:
            def query(self, operation, parameters):
                self.called = operation, parameters
                return expected
        authority = Authority()
        facade = object.__new__(SharedTaskFacade)
        facade.authority = authority
        facade.grants = {'alice': {'example'}}
        facade.state_dir = self.root / 'absent'
        self.assertEqual(facade.inspect_graph('alice', 'flow-native'), expected)
        self.assertEqual(authority.called[0], 'operator.graph')
        self.assertEqual(authority.called[1]['principal'], 'alice')
        self.assertFalse(facade.state_dir.exists())


if __name__ == '__main__':
    unittest.main()

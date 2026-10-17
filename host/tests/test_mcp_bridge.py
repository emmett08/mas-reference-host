"""MCP 2026-07-28 wire contracts against durable SQLite service and Rust Runner."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from mas_host import Outcome
from mas_host.bridges.mcp import (MCPBridge, MCPError, PREFIX, TASKS, VERSION, bounded,
    decode_header, encode_header, decode_message, http_status, metadata, routing_headers, task_result)
from mas_host.task_service import DurableTaskService, ExecutionResult, Workflow
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests


def request(method, params=None, *, tasks=True, elicitation=False):
    return {'jsonrpc':'2.0','id':1,'method':method,
            'params':{**(params or {}),'_meta':metadata(tasks=tasks,elicitation=elicitation)}}


class MCPBridgeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas, cls.gateway = runner_tests.RunnerIntegrationTests.mas, runner_tests.RunnerIntegrationTests.gateway

    def setUp(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.state = Path(temp.name)/'state'
        self.workflow = Workflow('echo/1',topology(),
            {actor:FunctionAdapter(lambda _c,r:Outcome(r.input)) for actor in ('a','b','c')},'a','b','echo')
        self.service = self.make_service()
        self.bridge = MCPBridge(self.service)

    def make_service(self, **kwargs):
        return DurableTaskService(self.state,{'echo':self.workflow},{'alice':{'echo'},'bob':{'echo'}},
                                  mas_bin=self.mas,gateway_bin=self.gateway,supervised=False,**kwargs)

    def call(self,name,args,principal='alice',**kwargs):
        return self.bridge.handle(principal,request('tools/call',{'name':name,'arguments':args},**kwargs))

    def submit(self,key='key',value=None):
        response = self.call('mas.submit',{'request_key':key,'workflow':'echo','input':value or {'n':7}})
        self.assertNotIn('error',response,response)
        return response['result']

    def test_discovery_and_list_match_official_json_schema(self):
        try:
            import jsonschema
        except ImportError:
            self.skipTest('install bridges extra for official JSON Schema conformance checks')
        fixture = Path(__file__).parent/'fixtures/mcp-2026-07-28-schema.json'
        schema=json.loads(fixture.read_text())
        provenance=json.loads(fixture.with_suffix('.provenance.json').read_text())
        self.assertEqual(hashlib.sha256(fixture.read_bytes()).hexdigest(),provenance['sha256'])
        for method,definition in [('server/discover','DiscoverResult'),('tools/list','ListToolsResult')]:
            response=self.bridge.handle('alice',request(method))
            jsonschema.Draft202012Validator({**schema,'$ref':'#/$defs/'+definition}).validate(response['result'])
            jsonschema.Draft202012Validator({**schema,'$ref':'#/$defs/JSONRPCResultResponse'}).validate(response)
        tool_response=self.call('mas.list',{})['result']
        jsonschema.Draft202012Validator({**schema,'$ref':'#/$defs/CallToolResult'}).validate(tool_response)

    def test_no_handshake_metadata_is_required_per_request(self):
        discovery=self.bridge.handle('alice',request('server/discover'))
        self.assertEqual(discovery['result']['supportedVersions'],[VERSION])
        missing={'jsonrpc':'2.0','id':2,'method':'tools/list','params':{}}
        self.assertEqual(self.bridge.handle('alice',missing)['error']['code'],-32602)
        self.assertEqual(self.bridge.handle('alice',request('initialize'))['error']['code'],-32601)
        old=request('tools/list');old['params']['_meta'][PREFIX+'protocolVersion']='2025-11-25'
        response=self.bridge.handle('alice',old)
        self.assertEqual(response['error']['code'],-32022)
        self.assertEqual(response['error']['data']['supported'],[VERSION])

    def test_capability_rejection_precedes_submission(self):
        response=self.call('mas.submit',{'request_key':'no-cap','workflow':'echo','input':{}},tasks=False)
        self.assertEqual(response['error']['code'],-32021)
        self.assertEqual(self.service.list_tasks('alice'),[])
        self.assertEqual(self.service.operations.health()['usage']['runs'],0)

    def test_durable_submit_replay_conflict_and_principal_isolation(self):
        first=self.submit()
        self.assertEqual(first['resultType'],'task')
        self.assertEqual(first['status'],'working')
        self.assertEqual(self.submit()['taskId'],first['taskId'])
        self.bridge=MCPBridge(self.make_service())
        self.assertEqual(self.submit()['taskId'],first['taskId'])
        conflict=self.call('mas.submit',{'request_key':'key','workflow':'echo','input':{'n':9}})
        self.assertEqual(conflict['error']['code'],1001)
        for method in ('tasks/get','tasks/cancel'):
            response=self.bridge.handle('bob',request(method,{'taskId':first['taskId']}))
            self.assertEqual(response['error']['code'],-32602)

    def test_real_runner_result_poll_and_events_survive_reopen(self):
        receipt=self.submit();self.service.run_once()
        self.bridge=MCPBridge(self.make_service())
        result=self.bridge.handle('alice',request('tasks/get',{'taskId':receipt['taskId']}))['result']
        self.assertEqual(result['status'],'completed')
        self.assertFalse(result['result']['isError'])
        self.assertEqual(result['result']['structuredContent']['result'],{'n':7})
        self.assertRegex(result['createdAt'],r'^20\d\d-')
        events=self.call('mas.events',{'task_id':receipt['taskId']})['result']['structuredContent']['events']
        self.assertGreater(len(events),1)
        self.assertEqual(self.call('mas.events',{'task_id':receipt['taskId'],'after':events[-1]['id']})['result']['structuredContent']['events'],[])

    def test_task_cancel_acknowledges_intent_and_has_no_nested_task(self):
        receipt=self.submit()
        response=self.bridge.handle('alice',request('tasks/cancel',{'taskId':receipt['taskId']}))
        self.assertEqual(response['result'],{'resultType':'complete'})
        self.assertEqual(self.bridge.handle('alice',request('tasks/get',{'taskId':receipt['taskId']}))['result']['status'],'cancelled')
        self.assertIsNone(self.service.run_once())

    def test_application_failure_is_completed_error_tool_result(self):
        class Fails:
            def execute(self,*a,**k):return ExecutionResult('failed',{'reason':'rejected'},{'code':'business'})
        self.service=self.make_service(executor=Fails());self.bridge=MCPBridge(self.service)
        receipt=self.submit();self.service.run_once()
        result=self.bridge.handle('alice',request('tasks/get',{'taskId':receipt['taskId']}))['result']
        self.assertEqual(result['status'],'completed')
        self.assertTrue(result['result']['isError'])
        self.assertEqual(result['_meta']['org.mas/state'],'failed')

    def test_unknown_outcome_remains_nonterminal_and_no_automatic_retry(self):
        class Unknown:
            def execute(self,*a,**k):return ExecutionResult('unknown',error={'code':'reconciliation_required'})
        self.service=self.make_service(executor=Unknown());self.bridge=MCPBridge(self.service)
        receipt=self.submit();self.service.run_once()
        result=self.bridge.handle('alice',request('tasks/get',{'taskId':receipt['taskId']}))['result']
        self.assertEqual(result['status'],'working')
        self.assertEqual(result['_meta']['org.mas/state'],'unknown')
        self.assertIsNone(self.service.run_once())

    def test_input_requests_require_caps_updates_are_idempotent(self):
        inputs={'answer-0':{'method':'elicitation/create','params':{'mode':'form','message':'Enter value',
                'requestedSchema':{'type':'object','properties':{'n':{'type':'integer'}},'required':['n']}}}}
        class Interrupted:
            def execute(self,*a,**k):return ExecutionResult('input_required',{'input_requests':inputs})
        self.workflow=replace(self.workflow,continue_input=lambda previous,value:value)
        self.state=self.state.parent/'continuation'
        self.service=self.make_service(executor=Interrupted());self.bridge=MCPBridge(self.service)
        receipt=self.submit();self.service.run_once()
        query={'taskId':receipt['taskId']}
        self.assertEqual(self.bridge.handle('alice',request('tasks/get',query))['error']['code'],-32021)
        self.assertEqual(self.bridge.handle('alice',request('tasks/get',query,elicitation=True))['result']['inputRequests'],{'r0:answer-0':inputs['answer-0']})
        update={**query,'inputResponses':{'r0:answer-0':{'action':'accept','content':{'n':8}}}}
        self.assertEqual(self.bridge.handle('alice',request('tasks/update',update,elicitation=True))['result'],{'resultType':'complete'})
        self.assertEqual(self.bridge.handle('alice',request('tasks/update',update,elicitation=True))['result'],{'resultType':'complete'})
        self.assertEqual(self.service.inspect('alice',receipt['taskId'])['revision'],1)

    def test_routing_headers_validate_before_side_effect_and_opaque_values_roundtrip(self):
        message=request('tools/call',{'name':'mas.submit','arguments':{'request_key':'x','workflow':'echo','input':{}}})
        headers=routing_headers(message);headers['Mcp-Name']='mas.cancel'
        response=self.bridge.handle('alice',message,headers)
        self.assertEqual(response['error']['code'],-32020);self.assertEqual(http_status(response),400)
        self.assertEqual(self.service.list_tasks('alice'),[])
        for value in ('Hello, 世界',' padded ','line1\nline2','=?base64?literal?=','plain'):
            self.assertEqual(decode_header(encode_header(value)),value)
        with self.assertRaises(MCPError):decode_header('=?base64?%%%?=')

    def test_bounded_json_rejects_decimal_duplicate_keys_and_non_object(self):
        for source in ('{"a":1.2}','{"a":1,"a":2}','[]','{"a":NaN}'):
            with self.assertRaises(MCPError):decode_message(source)
        response=self.call('mas.submit',{'request_key':'x','workflow':'echo','input':{'n':1.5}})
        self.assertEqual(response['error']['code'],-32602)
        self.assertEqual(self.service.list_tasks('alice'),[])
        for value in ({1:'x'}, {'x':'y'*70000}, {'n':True}):
            if value=={'n':True}:self.assertEqual(bounded(value),value)
            else:
                with self.assertRaises(MCPError):bounded(value)

    def test_notification_never_invokes_durable_task_method(self):
        message=request('tools/call',{'name':'mas.submit','arguments':{'request_key':'x','workflow':'echo','input':{}}})
        del message['id']
        self.assertIsNone(self.bridge.handle('alice',message))
        self.assertEqual(self.service.list_tasks('alice'),[])


if __name__=='__main__':unittest.main()

"""Real HTTP, SSE and stdio MCP exchanges with bounded fault handling."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from mas_host import Outcome, Runner, RecoveryRequired
from mas_host.bridges.mcp import MCPError, PREFIX, VERSION, decode_header, metadata, tool_result, validate_request
from mas_host.bridges.mcp_client import MCPClient, MCPRemoteClient, MCPToolAdapter, parameter_headers, validate_schema
from mas_host.bridges.mcp_transport import StreamableHTTPTransport, StdioTransport, _response_from_sse
from mas_host.bridges.remote import RemoteTaskService, RemoteTaskStore
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, Workflow
from test_runner import FunctionAdapter, topology
import test_runner as runner_tests

TOOL={'name':'multiply','inputSchema':{'type':'object','properties':{'n':{'type':'integer','x-mcp-header':'Number'}},'required':['n'],'additionalProperties':False}}


class Peer:
    def __init__(self,mode='immediate'):
        self.mode=mode;self.requests=[];self.status='working';self.fail=False;self.entered=threading.Event()
        self.task_id='task-remote';self.input_requests=None
        parent=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_POST(self):
                message=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                parent.requests.append((message,dict(self.headers)));parent.entered.set()
                try:validate_request(message,dict(self.headers))
                except MCPError as error:
                    self.send_response(400);self.end_headers();return
                method=message['method']
                if parent.mode=='lost' and method=='tools/call':
                    self.close_connection=True;self.connection.shutdown(socket.SHUT_RDWR);return
                if parent.mode=='slow':
                    time.sleep(2);self.close_connection=True;return
                if parent.mode=='redirect':
                    self.send_response(307);self.send_header('Location','http://127.0.0.1:1/leak');self.end_headers();return
                if method=='server/discover':
                    result={'resultType':'complete','supportedVersions':[VERSION],'capabilities':{},'cacheScope':'private','ttlMs':0}
                elif method=='tools/list':
                    result={'resultType':'complete','tools':[TOOL]}
                elif method in ('tasks/cancel','tasks/update'):
                    result={'resultType':'complete'}
                elif method=='tasks/get' or parent.mode=='task':
                    result={'resultType':'task' if method=='tools/call' else 'complete','taskId':parent.task_id,
                        'status':parent.status,'createdAt':'2026-10-14T10:00:00Z','lastUpdatedAt':'2026-10-14T10:00:00Z','ttlMs':None}
                    if parent.status=='completed':result['result']=tool_result({'n':10},failed=parent.fail)
                    if parent.status=='input_required':result['inputRequests']=parent.input_requests
                else:
                    result=tool_result({'n':message['params']['arguments']['n']*2},failed=parent.fail)
                    if parent.mode=='decimal':result['structuredContent']['n']=.5
                response={'jsonrpc':'2.0','id':message['id'],'result':result}
                body=json.dumps(response).encode()
                content='application/json'
                if parent.mode=='sse':
                    body=b': heartbeat\n\nevent: message\ndata: {"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":1}}\n\ndata: '+body+b'\n\n'
                    content='text/event-stream'
                self.send_response(200);self.send_header('Content-Type',content)
                self.send_header('Content-Length',str(len(body)));self.end_headers()
                try:self.wfile.write(body)
                except BrokenPipeError:pass
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.endpoint='http://127.0.0.1:%d/mcp'%self.server.server_port

    def client(self,**kwargs):
        return MCPClient(StreamableHTTPTransport(self.endpoint,token='test-private-token',allow_loopback_http=True,**kwargs))

    def close(self):
        self.server.shutdown();self.server.server_close();self.thread.join()


class MCPClientTests(unittest.TestCase):
    def peer(self,mode='immediate'):
        peer=Peer(mode);self.addCleanup(peer.close);return peer

    def test_real_http_metadata_routing_credential_and_business_error(self):
        peer=self.peer();peer.fail=True
        client=peer.client();remote=MCPRemoteClient(client,TOOL)
        snapshot=remote.submit({'arguments':{'n':5}})
        self.assertEqual(snapshot.state,'failed');self.assertTrue(snapshot.result['isError'])
        message,headers=peer.requests[0]
        self.assertEqual(headers['Mcp-Method'],'tools/call');self.assertEqual(headers['Mcp-Name'],'multiply')
        self.assertEqual(headers['Mcp-Param-Number'],'5')
        self.assertEqual(headers['Authorization'],'Bearer test-private-token')
        self.assertEqual(message['params']['_meta'][PREFIX+'protocolVersion'],VERSION)
        self.assertNotIn('test-private-token',json.dumps(remote.descriptor()))

    def test_sse_comments_notifications_and_complete_response(self):
        peer=self.peer('sse');result=peer.client().call_tool(TOOL,{'n':6})
        self.assertEqual(result['structuredContent'],{'n':12})
        with self.assertRaises(MCPError):_response_from_sse(b'data: {"jsonrpc":"2.0","method":"notifications/progress"}\n\n','1')
        with self.assertRaises(MCPError):_response_from_sse(b'data: {"jsonrpc":"2.0","id":"other","result":{}}\n\n','1')

    def test_current_tasks_poll_and_cancel_ack_are_distinct(self):
        peer=self.peer('task');remote=MCPRemoteClient(peer.client(),TOOL)
        receipt=remote.submit({'arguments':{'n':4}})
        self.assertEqual(receipt.state,'running')
        cancelled=remote.cancel(receipt.remote_id)
        self.assertEqual(cancelled.state,'running');self.assertTrue(cancelled.result['cancel_acknowledged'])
        peer.status='completed';peer.fail=True
        final=remote.poll(receipt.remote_id)
        self.assertEqual(final.state,'failed');self.assertEqual(final.result['mcp_status'],'completed')
        self.assertEqual(peer.requests[-1][1]['Mcp-Name'],'task-remote')

    def test_redirect_and_noninteger_result_are_rejected_without_retry(self):
        for mode in ('redirect','decimal','lost'):
            with self.subTest(mode=mode):
                peer=self.peer(mode)
                with self.assertRaises(MCPError):peer.client().call_tool(TOOL,{'n':5})
                self.assertEqual(len(peer.requests),1)

    def test_wall_deadline_works_on_nonmain_thread(self):
        peer=self.peer('slow');outcome=[]
        def run():
            try:peer.client(timeout_seconds=1).call_tool(TOOL,{'n':5})
            except MCPError:outcome.append('bounded')
        start=time.monotonic();worker=threading.Thread(target=run);worker.start();worker.join(2)
        self.assertFalse(worker.is_alive());self.assertEqual(outcome,['bounded'])
        self.assertLess(time.monotonic()-start,1.8)

    def test_header_annotations_and_schema_dialect_fail_closed(self):
        schema={'type':'object','properties':{'p':{'type':'object','properties':{'s':{'type':'string','x-mcp-header':'Unicode'}}}}}
        self.assertEqual(parameter_headers(schema,{'p':{'s':'世界'}}),{'Mcp-Param-Unicode':'=?base64?5LiW55WM?='})
        invalid=[{'oneOf':[TOOL['inputSchema']]},
                 {'type':'object','properties':{'x':{'type':'number','x-mcp-header':'x'}}},
                 {'type':'object','properties':{'x':{'type':'string','x-mcp-header':'x'},'y':{'type':'string','x-mcp-header':'X'}}}]
        for value in invalid:
            with self.assertRaises(MCPError):parameter_headers(value,{})
        with self.assertRaises(MCPError):validate_schema({'$schema':'http://json-schema.org/draft-07/schema','type':'object'})
        with self.assertRaises(MCPError):validate_schema({'$ref':'https://untrusted.invalid/schema'})
        with self.assertRaises(MCPError):validate_schema(TOOL['inputSchema'],{'n':'5'})

    def test_real_stdio_has_no_initialise_or_ambient_secret(self):
        with tempfile.TemporaryDirectory() as temporary:
            script=Path(temporary)/'peer.py'
            script.write_text('''import sys,json,os
for line in sys.stdin:
 m=json.loads(line)
 result={'resultType':'complete','content':[{'type':'text','text':m['method']}],'structuredContent':{'ambient':'AMBIENT_SECRET' in os.environ},'isError':False}
 print(json.dumps({'jsonrpc':'2.0','id':m['id'],'result':result}),flush=True)
''')
            with patch.dict(os.environ,{'AMBIENT_SECRET':'must-not-propagate'}):
                with StdioTransport([sys.executable,str(script)],cwd=Path(temporary)) as transport:
                    result=MCPClient(transport).call_tool(TOOL,{'n':1})
                    self.assertEqual(result['content'][0]['text'],'tools/call')
                    self.assertEqual(result['structuredContent'],{'ambient':False})
                    process=transport._process
                self.assertIsNotNone(process.poll())

    def test_stdio_timeout_is_bounded_and_child_reaped(self):
        with tempfile.TemporaryDirectory() as temporary:
            script=Path(temporary)/'peer.py';script.write_text('import time\ntime.sleep(10)\n')
            transport=StdioTransport([sys.executable,str(script)],cwd=Path(temporary),timeout_seconds=1)
            start=time.monotonic()
            with self.assertRaises(MCPError):MCPClient(transport).call_tool(TOOL,{'n':1})
            self.assertIsNone(transport._process);self.assertLess(time.monotonic()-start,2)


class MCPDurableRunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.mas,cls.gateway=runner_tests.RunnerIntegrationTests.mas,runner_tests.RunnerIntegrationTests.gateway

    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);self.state=Path(temp.name)
        self.peer=Peer('task');self.addCleanup(self.peer.close)
        self.remote=MCPRemoteClient(self.peer.client(),TOOL)
        self.service=RemoteTaskService(RemoteTaskStore(self.state),self.remote,namespace="alice:workflow:b")
        self.adapter=MCPToolAdapter(self.service,self.state/'pending',reservation_microusd=5)
        self.adapters={'a':FunctionAdapter(lambda c,r:Outcome(r.input)),'b':self.adapter}
        self.runner=Runner(topology(('a','b')),self.adapters,self.state,mas_bin=self.mas,gateway_bin=self.gateway,supervised=False,workflow_version='mcp-tests/1')
        self.adapter.host_run_dir=self.runner.run_dir
        self.addCleanup(self.runner.close)

    def test_real_runner_admits_before_dispatch_and_replays_after_restart(self):
        outcome=self.runner.request('a','b','mcp.call',{'key':'job1','arguments':{'n':3}})
        self.assertEqual(outcome.output['state'],'running')
        run_dir=self.runner.run_dir;self.runner.close()
        self.service=RemoteTaskService(RemoteTaskStore(self.state),self.remote,namespace="alice:workflow:b")
        self.adapter.service=self.service
        with Runner.resume(run_dir,self.adapters,mas_bin=self.mas,gateway_bin=self.gateway,supervised=False,workflow_version='mcp-tests/1') as resumed:
            replay=resumed.request('a','b','mcp.call',{'key':'job1','arguments':{'n':3}})
            self.assertEqual(replay.output,outcome.output)
            self.peer.status='completed'
            polled=resumed.request('a','b','mcp.poll',{'key':'job1'})
            self.assertEqual(polled.output['state'],'completed')
        self.assertEqual([item[0]['method'] for item in self.peer.requests],['tools/call','tasks/get'])

    def test_lost_receipt_persists_unknown_and_reopen_never_resubmits(self):
        self.peer.mode='lost'
        with self.assertRaises(RecoveryRequired):
            self.runner.request('a','b','mcp.call',{'key':'lost1','arguments':{'n':3}})
        run_dir=self.runner.run_dir;self.runner.close()
        self.assertEqual(self.service.status('lost1')['state'],'unknown')
        with Runner.resume(run_dir,self.adapters,mas_bin=self.mas,gateway_bin=self.gateway,supervised=False,workflow_version='mcp-tests/1') as resumed:
            with self.assertRaises(RecoveryRequired):resumed.request('a','b','mcp.call',{'key':'lost1','arguments':{'n':3}})
        self.assertEqual(len(self.peer.requests),1)

    def test_cancel_acknowledgement_does_not_claim_stopped(self):
        self.runner.request('a','b','mcp.call',{'key':'job1','arguments':{'n':3}})
        outcome=self.runner.request('a','b','mcp.cancel',{'key':'job1','request_key':'cancel1'})
        self.assertTrue(outcome.output['cancel_requested']);self.assertEqual(outcome.output['state'],'running')
        self.peer.status='completed'
        outcome=self.runner.request('a','b','mcp.poll',{'key':'job1'})
        self.assertEqual(outcome.output['state'],'completed')

    def test_opaque_peer_identity_survives_store_and_lifecycle_header_encoding(self):
        prefix='tasks/世界\n\0/'
        opaque=prefix+'x'*(1024-len(prefix.encode('utf-8')))
        input_id='consent/é\n\0'
        self.peer.task_id=opaque;self.peer.status='input_required'
        self.peer.input_requests={input_id:{'method':'elicitation/create','params':{
            'mode':'form','message':'Continue?',
            'requestedSchema':{'type':'object','properties':{'yes':{'type':'boolean'}}}}}}
        self.remote.client.elicitation=True
        self.service=RemoteTaskService(RemoteTaskStore(self.state),self.remote,namespace='alice:workflow:b')
        self.adapter.service=self.service
        submitted=self.runner.request('a','b','mcp.call',{'key':'opaque','arguments':{'n':3}})
        self.assertEqual(submitted.output['remote_id'],opaque)
        self.assertIn(input_id,submitted.output['result']['input_requests'])
        self.service=RemoteTaskService(RemoteTaskStore(self.state),self.remote,namespace='alice:workflow:b')
        self.adapter.service=self.service
        self.assertEqual(self.service.status('opaque')['remote_id'],opaque)
        self.runner.request('a','b','mcp.update',{'key':'opaque','request_key':'answer',
            'input_responses':{input_id:{'action':'accept','content':{'yes':True}}}})
        self.peer.status='working'
        self.runner.request('a','b','mcp.poll',{'key':'opaque'})
        self.runner.request('a','b','mcp.cancel',{'key':'opaque','request_key':'stop'})
        lifecycle=self.peer.requests[1:]
        self.assertEqual([message['method'] for message,_ in lifecycle],
                         ['tasks/update','tasks/get','tasks/cancel'])
        for message,headers in lifecycle:
            self.assertEqual(message['params']['taskId'],opaque)
            self.assertEqual(decode_header(headers['Mcp-Name']),opaque)
            self.assertTrue(headers['Mcp-Name'].startswith('=?base64?'))
            self.assertTrue(all(32<=ord(c)<=126 for c in headers['Mcp-Name']))
        self.assertIn(input_id,lifecycle[0][0]['params']['inputResponses'])
        self.assertEqual(self.service.status('opaque')['remote_id'],opaque)
        self.peer.task_id=opaque+'x'
        with self.assertRaises(ServiceError):
            self.remote.submit({'arguments':{'n':3}}).validate()

    def test_one_workflow_factory_keeps_owner_namespace_across_call_and_poll(self):
        def factory(binding):
            jobs=RemoteTaskService(RemoteTaskStore(binding.state_dir),self.remote,
                                   namespace=binding.remote_namespace('b'))
            return {'a':FunctionAdapter(lambda c,r:Outcome(r.input)),
                    'b':MCPToolAdapter(jobs,binding.run_dir,reservation_microusd=5)}
        placeholder={actor:FunctionAdapter(lambda c,r:Outcome(r.input)) for actor in ('a','b')}
        workflow=Workflow('remote/1',topology(('a','b')),placeholder,'a','b','mcp.execute',adapter_factory=factory)
        service=DurableTaskService(self.state,{'remote':workflow},{'alice':{'remote'},'bob':{'remote'}},
                                  mas_bin=self.mas,gateway_bin=self.gateway,supervised=False)
        submitted=service.submit('alice','submit-1','remote',{'operation':'call','key':'same','arguments':{'n':3}})
        self.assertEqual(service.run_once()['state'],'completed')
        receipt=service.result('alice',submitted['task_id'])
        self.assertEqual(receipt['state'],'running')
        self.peer.status='completed'
        polled=service.submit('alice','poll-1','remote',{'operation':'poll','key':'same'})
        service.run_once()
        self.assertEqual(service.result('alice',polled['task_id'])['state'],'completed')
        self.assertEqual([r[0]['method'] for r in self.peer.requests],['tools/call','tasks/get'])
        # The public dispatcher cannot invoke the trusted operator attachment.
        rejected=service.submit('alice','attach-1','remote',{'operation':'attach','key':'same',
                    'remote_id':'task-remote','evidence':{'claimed':True}})
        self.assertEqual(service.run_once()['state'],'failed')
        self.assertEqual(len(self.peer.requests),2)


if __name__=='__main__':unittest.main()

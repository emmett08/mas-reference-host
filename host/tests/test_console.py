"""Static console never bypasses authenticated lifecycle boundaries."""
import base64
import hashlib
import http.client
import json
import shutil
import subprocess
import threading
import unittest

from mas_host.bridges.console import SCRIPT, STYLE, console_response
from test_bridge_server import JoinedBridgeHTTPServer, MemoryService, TOKEN, verifier


class ConsoleTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node.js required for console JavaScript regression')
    def test_javascript_session_reset_and_integer_precision(self):
        # Execute the shipped script with a small DOM/fetch harness, including a
        # transport which delivers a stale response even after abort().
        harness = r'''
const assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
const elements=new Map(),get=id=>{if(!elements.has(id))elements.set(id,{value:'',textContent:'',children:[],replaceChildren(){this.children=[];},append(x){this.children.push(x);},addEventListener(){}});return elements.get(id);};
const pending=[];const context=vm.createContext({document:{getElementById:get,createElement:()=>({addEventListener(){}})},window:{addEventListener(){}},AbortController,crypto:require('node:crypto').webcrypto,fetch:(url,options)=>new Promise(resolve=>pending.push({url,options,resolve}))});
vm.runInContext(fs.readFileSync(0,'utf8'),context);
(async()=>{
 assert.throws(()=>vm.runInContext(`parse('{"count":9007199254740993}')`,context),/safe integers/);
 assert.throws(()=>vm.runInContext(`parse('{"nested":[1.2]}')`,context),/safe integers/);
 assert.equal(vm.runInContext(`parse('{"count":9007199254740991}').count`,context),9007199254740991);
 get('leaf-detail').textContent='private previous result';get('input').value='{"secret":1}';get('reply').value='{"private":2}';get('path').value='private/path';get('workflows').append({value:'private-workflow'});
 const old=vm.runInContext(`token='old-token';action(async()=>{show(await api('inspect',{task_id:'old-task'}));})`,context);
 assert.equal(pending.length,1);get('disconnect').onclick();
 assert.equal(pending[0].options.signal.aborted,true);
 assert.equal(get('leaf-detail').textContent,'');assert.equal(get('input').value,'{}');assert.equal(get('reply').value,'{}');assert.equal(get('path').value,'');assert.equal(get('workflows').children.length,0);
 vm.runInContext(`token='new-token';status('New connection.');`,context);
 pending[0].resolve({ok:true,text:async()=>'{"result":{"task_id":"old-task","result":"private"}}'});await old;
 assert.equal(get('detail').textContent,'');assert.equal(get('task').value,'');assert.equal(get('status').textContent,'New connection.');
 get('task').value='one';const first=vm.runInContext('progress()',context),second=vm.runInContext('progress()',context);
 for(const item of pending.slice(1,3))item.resolve({ok:true,text:async()=>'{"result":{"events":[{"id":1}],"next_after":1,"has_more":false}}'});
 await Promise.all([first,second]);assert.equal(get('events').textContent,'{"id":1}');
 const switched=vm.runInContext('progress()',context);get('task').value='two';
 pending[3].resolve({ok:true,text:async()=>'{"result":{"events":[{"id":2}],"next_after":2,"has_more":false}}'});await switched;
 assert.equal(vm.runInContext('cursor',context),1);assert.equal(get('events').textContent,'{"id":1}');
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
        result = subprocess.run(['node', '-e', harness], input=SCRIPT, text=True,
                                capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_csp_hashes_match_fixed_script_and_style_without_credential_storage(self):
        body, headers = console_response()
        for content in (SCRIPT, STYLE):
            digest = base64.b64encode(hashlib.sha256(content.encode()).digest()).decode()
            self.assertIn('sha256-' + digest, headers['Content-Security-Policy'])
        self.assertIn("connect-src 'self'", headers['Content-Security-Policy'])
        for forbidden in ('localStorage', 'sessionStorage', 'innerHTML', 'eval(', 'https://'):
            self.assertNotIn(forbidden, body.decode())
        self.assertLess(len(body), 65536)

    def test_console_shell_is_public_but_data_and_origin_stay_authorised(self):
        service = MemoryService()
        server = JoinedBridgeHTTPServer(('127.0.0.1', 0), service=service, verifier=verifier(),
            public_url='http://127.0.0.1', allowed_hosts=['127.0.0.1'], allow_loopback_http=True,
            allowed_origins=['http://127.0.0.1'], request_log=lambda _: None)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        def exchange(method, path, body=None, headers=None):
            client = http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=2)
            try:
                client.request(method, path, body=body, headers={'Host': '127.0.0.1', **(headers or {})})
                response = client.getresponse()
                return response.status, dict(response.getheaders()), response.read()
            finally:
                client.close()
        code, headers, body = exchange('GET', '/console')
        self.assertEqual(code, 200)
        self.assertTrue(headers['Content-Type'].startswith('text/html'))
        self.assertEqual(headers['Cache-Control'], 'no-store')
        self.assertIn("frame-ancestors 'none'", headers['Content-Security-Policy'])
        self.assertNotIn(TOKEN.encode(), body)
        self.assertEqual(service.calls, [])
        payload = json.dumps({'request_key': 'one', 'workflow': 'echo', 'input': {}})
        code, _, _ = exchange('POST', '/service/submit', payload, {'Content-Type': 'application/json'})
        self.assertEqual(code, 401)
        code, _, _ = exchange('POST', '/service/submit', payload, {'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + TOKEN, 'Origin': 'http://untrusted.example'})
        self.assertEqual(code, 400)
        self.assertEqual(service.calls, [])
        code, _, _ = exchange('POST', '/service/submit', payload, {'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + TOKEN, 'Origin': 'http://127.0.0.1'})
        self.assertEqual(code, 200)
        self.assertEqual(service.calls, [('alice', 'one')])


if __name__ == '__main__':
    unittest.main()

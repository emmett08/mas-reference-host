"""The console observes telemetry through the explicit administrator boundary."""
from pathlib import Path
import shutil
import subprocess
import tempfile
from types import SimpleNamespace
import unittest

from mas_host.bridges.console import SCRIPT, console_response
from mas_host.operator_service import OperatorService
from mas_host.service_contracts import ServiceError
from mas_host.telemetry import record


class ConsoleTelemetryTests(unittest.TestCase):
    def test_shared_observations_use_the_controller_cache_and_require_admin(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            record(root, 'provider', 1200, run_id='private-run')
            service = SimpleNamespace(authority=SimpleNamespace(observation_dir=root),
                state_dir=root / 'not-the-observation-directory',
                recovery_service=lambda **_: None, deadline_service=lambda **_: None)
            operator = OperatorService(service, operators={
                'admin': {'owners': [], 'deployment_admin': True},
                'reviewer': {'owners': ['alice'], 'deployment_admin': False}})
            with self.assertRaises(ServiceError) as denied:
                operator.dispatch('reviewer', 'telemetry', {})
            self.assertEqual(denied.exception.code, 'forbidden')
            with self.assertRaises(ServiceError) as injected:
                operator.dispatch('admin', 'telemetry', {'root': '/other'})
            self.assertEqual(injected.exception.code, 'invalid_operator_request')
            result = operator.dispatch('admin', 'telemetry', {})
            self.assertTrue(result['ok'])
            self.assertEqual(result['records'], 1)
            self.assertEqual(result['recent_spans'][0]['run_id'], 'private-run')

    @unittest.skipUnless(shutil.which('node'), 'Node.js required for console JavaScript regression')
    def test_button_uses_authenticated_read_and_disconnect_clears_observations(self):
        self.assertIn(b'id="op-telemetry"', console_response()[0])
        harness = r'''
const assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
const elements=new Map(),get=id=>{if(!elements.has(id))elements.set(id,{value:'',textContent:'',children:[],replaceChildren(){this.children=[];},append(x){this.children.push(x);},addEventListener(){}});return elements.get(id);};
const calls=[],pending=[];
const context=vm.createContext({document:{getElementById:get,createElement:()=>({addEventListener(){}})},window:{addEventListener(){}},AbortController,crypto:require('node:crypto').webcrypto,fetch:(url,options)=>new Promise(resolve=>{calls.push({url,options});pending.push(resolve);})});
vm.runInContext(fs.readFileSync(0,'utf8'),context);
(async()=>{
 await get('op-telemetry').onclick();assert.equal(calls.length,0);
 vm.runInContext("token='admin-token'",context);
 const request=get('op-telemetry').onclick();
 assert.equal(calls[0].url,'/service/operator/telemetry');
 assert.equal(calls[0].options.headers.Authorization,'Bearer admin-token');
 assert.equal(calls[0].options.body,'{}');
 assert.equal(calls[0].options.credentials,'omit');
 assert.equal(calls[0].options.redirect,'error');
 pending[0]({ok:true,text:async()=>JSON.stringify({result:{format:'mas-telemetry/1',records:1,recent_spans:[{run_id:'private-run'}]}})});
 await request;assert.match(get('op-result').textContent,/private-run/);
 get('disconnect').onclick();assert.equal(get('op-result').textContent,'');
 assert.equal(vm.runInContext('token',context),'');
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
        result = subprocess.run(['node', '-e', harness], input=SCRIPT, text=True,
                                capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()

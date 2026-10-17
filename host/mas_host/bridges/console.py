"""Static same-origin console; all application data uses authenticated APIs."""
import base64
import hashlib


STYLE = """
:root{font:16px system-ui;color:#18212b;background:#f5f7fa}*{box-sizing:border-box}
body{max-width:1100px;margin:auto;padding:24px}h1{margin-bottom:4px}p{line-height:1.5}
section{background:white;border:1px solid #d7dfe8;border-radius:10px;padding:18px;margin:16px 0}
label{display:block;margin:12px 0 5px;font-weight:600}input,textarea,select{width:100%;padding:9px;border:1px solid #9ba9b8;border-radius:4px;font:inherit}
textarea{font-family:monospace;min-height:85px}button{padding:9px 15px;margin:10px 6px 0 0;border:1px solid #314e71;border-radius:4px;background:#173f6b;color:white;font:inherit;cursor:pointer}
button:disabled{opacity:.5;cursor:default}button.secondary{color:#173f6b;background:white}
pre{white-space:pre-wrap;overflow-wrap:anywhere;max-height:420px;overflow:auto;background:#f5f7fa;padding:12px;font-size:13px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}.hint{color:#4b5d70;font-size:14px}
#status{white-space:pre-wrap;min-height:24px}#tasks button{display:block;width:100%;text-align:left;background:white;color:#173f6b;border-color:#c3ccd8}
@media(max-width:720px){body{padding:12px}.grid{grid-template-columns:1fr}}
"""

SCRIPT = """
'use strict';
const $=id=>document.getElementById(id); let token='',cursor=0,eventTask='',events=[],nextPage=null,selection=null,generation=0;
const requests=new Set();
const status=text=>{$('status').textContent=text;};
function parse(text){return JSON.parse(text,(_key,value)=>{if(typeof value==='number'&&!Number.isSafeInteger(value))throw Error('The browser requires safe integers. Use the Python client for larger integers or artefacts for scientific data.');return value;});}
function object(id){const value=parse($(id).value);if(!value||Array.isArray(value)||typeof value!=='object')throw Error('Input must be a JSON object.');return value;}
function reset(){generation++;for(const request of requests)request.abort();requests.clear();token='';selection=null;events=[];cursor=0;eventTask='';nextPage=null;
 for(const id of ['tasks','workflows','op-owner'])$(id).replaceChildren();for(const id of ['detail','leaf-detail','graph-detail','events','definitions','op-result','op-permissions'])$(id).textContent='';
 for(const id of ['token','task','path','key','reply-key','updated-after','op-call','op-key','op-reason','op-name','op-identity','op-revision','op-epoch','op-peer-name','op-peer-version','op-query-name','op-query-version','op-artifact-namespace','op-artifact-principal','op-artifact-link'])$(id).value='';for(const id of ['input','reply','search-metadata','op-evidence','op-output','op-limits','op-period','op-charge','op-peer'])$(id).value='{}';$('more').disabled=true;$('op-revision').value='0';$('op-offset').value='0';$('graph-offset').value='0';
}
async function api(operation,input){
 if(!token)throw Error('Connect with your service token first.');
 const current=generation,controller=new AbortController();requests.add(controller);
 try{const response=await fetch('/service/'+operation,{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},body:JSON.stringify(input),credentials:'omit',cache:'no-store',redirect:'error',signal:controller.signal});
 const text=await response.text();if(current!==generation)throw Error('Session changed.');const body=parse(text);if(!response.ok)throw Error(body.error?.code||'Request failed');return body.result;
 }finally{requests.delete(controller);}
}
async function action(work){const current=generation;try{status('Working…');await work();if(current===generation)status('Ready.');}catch(error){if(current===generation)status(String(error.message||'Operation failed'));}}
function show(value){selection=value;$('task').value=value.task_id;$('detail').textContent=JSON.stringify(value,null,2);}
const isOperator=()=>$('connection-role').value==='operator';
async function refresh(){const value=isOperator()?await operatorCall('task',ownerTask()):await api('inspect',{task_id:$('task').value});show(value);}
async function list(more=false){
 const query={page_size:30};if(more&&nextPage)query.after=nextPage;if($('state').value)query.state=$('state').value;
 if($('search-metadata').value)query.metadata=object('search-metadata');if($('updated-after').value)query.updated_after=parse($('updated-after').value);
 const page=isOperator()?await operatorCall('tasks',{...query,principal:$('op-owner').value}):await api('page',query);if(!more)$('tasks').replaceChildren();
 for(const task of page.tasks){const button=document.createElement('button');button.textContent=task.workflow+' · '+task.state+' · '+task.task_id;button.addEventListener('click',()=>action(async()=>{show(task);await refresh();}));$('tasks').append(button);}
 nextPage=page.next;$('more').disabled=!nextPage;
}
async function discover(){const data=isOperator()?await operatorCall('registrations',{principal:$('op-owner').value}):await api('discover',{});$('workflows').replaceChildren();
 for(const item of data.registrations){const option=document.createElement('option');option.value=item.name;option.textContent=item.name+(item.available?'':' (unavailable)');option.disabled=!item.available;$('workflows').append(option);}
 $('definitions').textContent=JSON.stringify(data,null,2);
}
async function progress(){const task=$('task').value;if(eventTask!==task){cursor=0;events=[];eventTask=task;}
 const after=cursor,page=isOperator()?await operatorCall('events',{...ownerTask(),after,limit:100}):await api('events_page',{task_id:task,after,limit:100});if(task!==$('task').value||eventTask!==task||cursor!==after)return;cursor=page.next_after;events.push(...page.events);events=events.slice(-500);$('events').textContent=events.map(x=>JSON.stringify(x)).join('\\n');
}
$('connect').onclick=()=>{const supplied=$('token').value.trim();reset();token=supplied;return action(async()=>{if($('connection-role').value==='operator'){await operatorCapabilities();}else{await discover();await list();}});};
$('disconnect').onclick=()=>{reset();status('Disconnected. Token and displayed data cleared.');};
$('refresh-list').onclick=()=>action(()=>list());$('more').onclick=()=>action(()=>list(true));
$('submit').onclick=()=>action(async()=>{show(await api('submit',{workflow:$('workflows').value,request_key:$('key').value,input:object('input')}));await list();});
$('inspect').onclick=()=>action(refresh);
$('cancel').onclick=()=>action(async()=>{show(await api('cancel',{task_id:$('task').value}));});
$('recover').onclick=()=>action(async()=>{show(await api('recover',{task_id:$('task').value}));});
$('continue').onclick=()=>action(async()=>{let input=object('reply');if(selection?.kind==='plan')input={path:$('path').value,input};show(await api('update',{task_id:$('task').value,request_key:$('reply-key').value,input}));});
$('leaf').onclick=()=>action(async()=>{$('leaf-detail').textContent=JSON.stringify((isOperator()?await operatorCall('task',{...ownerTask(),path:$('path').value}):await api('task',{task_id:$('task').value,path:$('path').value})),null,2);});
$('graph').onclick=()=>action(async()=>{const query={task_id:$('task').value,section:$('graph-section').value,offset:parse($('graph-offset').value),limit:20};const value=isOperator()?await operatorCall('graph',{...query,principal:$('op-owner').value}):await api('graph',query);$('graph-detail').textContent=JSON.stringify(value,null,2);if(value.next_offset!==null)$('graph-offset').value=String(value.next_offset);});
$('progress').onclick=()=>action(progress);
$('new-key').onclick=()=>{$('key').value=crypto.randomUUID();};
async function operatorCall(operation,input={}){const result=await api('operator/'+operation,input);$('op-result').textContent=JSON.stringify(result,null,2);if(Number.isSafeInteger(result.revision))$('op-revision').value=String(result.revision);return result;}
const ownerTask=()=>({principal:$('op-owner').value,task_id:$('task').value});
const amendment=()=>({request_key:$('op-key').value,reason:$('op-reason').value,expected_revision:parse($('op-revision').value)});
const definition=()=>({kind:$('op-kind').value,name:$('op-name').value,identity:$('op-identity').value});
async function operatorCapabilities(){const value=await operatorCall('capabilities');$('op-permissions').textContent=JSON.stringify(value,null,2);$('op-owner').replaceChildren();for(const owner of value.owners){const option=document.createElement('option');option.value=owner;option.textContent=owner;$('op-owner').append(option);}}
$('op-connect').onclick=()=>action(operatorCapabilities);
$('op-new-key').onclick=()=>{$('op-key').value=crypto.randomUUID();};
$('op-tasks').onclick=()=>action(()=>list());
$('op-registrations').onclick=()=>action(discover);
const operatorPage=()=>({offset:parse($('op-offset').value),limit:20});
$('op-epoch-status').onclick=()=>action(()=>operatorCall('request_epoch_status',{principal:$('op-owner').value,epoch:$('op-epoch').value}));
$('op-epoch-retire').onclick=()=>action(()=>operatorCall('request_epoch_retire',{principal:$('op-owner').value,epoch:$('op-epoch').value,reason:$('op-reason').value,prune:$('op-prune').value==='true'}));
$('op-inbox').onclick=()=>action(()=>operatorCall('recovery_inbox',{...ownerTask(),...operatorPage()}));
$('op-query').onclick=()=>action(()=>operatorCall('recovery_query',{...ownerTask(),call_id:$('op-call').value,query_name:$('op-query-name').value,query_version:$('op-query-version').value}));
$('op-resolve').onclick=()=>action(async()=>{const payload={...ownerTask(),call_id:$('op-call').value,request_key:$('op-key').value,decision:$('op-decision').value,evidence:object('op-evidence')};if(payload.decision==='completed')payload.output=object('op-output');await operatorCall('recovery_resolve',payload);});
$('op-deadline').onclick=()=>action(()=>operatorCall('deadline_extend',{...ownerTask(),...amendment(),limits:object('op-limits'),...($('path').value?{path:$('path').value}:{})}));
$('op-budget').onclick=()=>action(()=>operatorCall('budget',operatorPage()));
$('op-budget-amend').onclick=()=>action(()=>operatorCall('budget_amend',{...amendment(),limits:object('op-limits')}));
$('op-periods').onclick=()=>action(()=>operatorCall('periods',operatorPage()));
$('op-period-configure').onclick=()=>action(()=>operatorCall('period_configure',{...object('op-period'),request_key:$('op-key').value,reason:$('op-reason').value}));
$('op-charge-record').onclick=()=>action(()=>operatorCall('charge_record',object('op-charge')));
$('op-charge-reconcile').onclick=()=>action(()=>{const charge=object('op-charge');return operatorCall('charge_reconcile',{run_id:charge.run_id,call_id:charge.call_id,...operatorPage()});});
$('op-storage').onclick=()=>action(()=>operatorCall('storage'));
$('op-telemetry').onclick=()=>action(()=>operatorCall('telemetry'));
$('op-peer-status').onclick=()=>action(()=>operatorCall('peer_status',operatorPage()));
$('op-peer-register').onclick=()=>action(()=>operatorCall('peer_register',{...amendment(),registration:object('op-peer')}));
$('op-peer-revoke').onclick=()=>action(()=>operatorCall('peer_revoke',{...amendment(),name:$('op-peer-name').value,version:$('op-peer-version').value}));
$('op-workers').onclick=()=>action(()=>operatorCall('worker_status'));
$('op-drain').onclick=()=>action(()=>operatorCall('worker_drain',{...amendment(),draining:$('op-draining').value==='true'}));
$('op-archive').onclick=()=>action(()=>operatorCall('archive_preview',{kind:$('op-archive-kind').value,older_than_seconds:parse($('op-age').value),...operatorPage()}));
$('op-archive-apply').onclick=()=>action(()=>operatorCall('archive',{...ownerTask(),kind:$('op-archive-kind').value,older_than_seconds:parse($('op-age').value),request_key:$('op-key').value,reason:$('op-reason').value}));
function artifactOwner(){return {principal:$('op-owner').value,namespace:$('op-artifact-namespace').value,artifact_principal:$('op-artifact-principal').value};}
function artifactChange(){return {...artifactOwner(),link_id:$('op-artifact-link').value,request_key:$('op-key').value,reason:$('op-reason').value};}
$('op-artifact-namespaces').onclick=()=>action(()=>operatorCall('artifact_namespaces',operatorPage()));
$('op-artifact-links').onclick=()=>action(()=>operatorCall('artifact_links',{...artifactOwner(),...operatorPage()}));
$('op-artifact-bind').onclick=()=>action(()=>operatorCall('artifact_bind',{...artifactChange(),task_id:$('task').value}));
$('op-artifact-release').onclick=()=>action(()=>operatorCall('artifact_release',artifactChange()));
$('op-artifact-retire').onclick=()=>action(()=>operatorCall('artifact_retire',{namespace:$('op-artifact-namespace').value,request_key:$('op-key').value,reason:$('op-reason').value}));
$('op-command-capacity').onclick=()=>action(()=>operatorCall('command_capacity'));
$('op-command-amend').onclick=()=>action(()=>operatorCall('command_capacity_amend',{...object('op-limits'),request_key:$('op-key').value,expected_revision:parse($('op-revision').value),evidence:object('op-evidence')}));
$('op-definition').onclick=()=>action(()=>operatorCall('definition',definition()));
$('op-retire').onclick=()=>action(()=>operatorCall('definition_retire',definition()));
window.addEventListener('pagehide',reset);
status('Connect to inspect your authorised tasks.');
"""

BODY = """<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MAS operations</title><style>STYLE</style><body>
<h1>MAS operations</h1><p class="hint">Submit work, follow progress and complete human input. The service enforces your permissions on every action.</p>
<section><label for="connection-role">Access</label><select id="connection-role"><option value="task">Task owner</option><option value="operator">Operator</option></select><label for="token">Service token</label><input id="token" type="password" autocomplete="off" spellcheck="false">
<button id="connect">Connect</button><button id="disconnect" class="secondary">Disconnect</button>
<p class="hint">The token stays in this page's memory. Refreshing or closing the page clears it.</p><p id="status" role="status" aria-live="polite"></p></section>
<div class="grid"><section><h2>Your tasks</h2><label for="state">State</label><select id="state"><option value="">All states</option><option>queued</option><option>running</option><option>input_required</option><option>auth_required</option><option>blocked</option><option>unknown</option><option>completed</option><option>failed</option><option>cancelled</option></select>
<label for="search-metadata">Metadata search JSON</label><textarea id="search-metadata" maxlength="8000">{}</textarea><label for="updated-after">Updated after UTC epoch milliseconds (optional)</label><input id="updated-after" inputmode="numeric">
<button id="refresh-list">Refresh list</button><div id="tasks"></div><button id="more" disabled>Load more</button></section>
<section><h2>Submit work</h2><label for="workflows">Registration</label><select id="workflows"></select>
<label for="key">Request key</label><input id="key" maxlength="200"><button id="new-key" class="secondary">New key</button>
<p class="hint">Keep the same key when checking an interrupted submission. A new key creates new work.</p>
<label for="input">Input JSON</label><textarea id="input" maxlength="64000">{}</textarea><button id="submit">Submit</button>
<details><summary>Authorised registrations</summary><pre id="definitions"></pre></details></section></div>
<section><h2>Task detail</h2><label for="task">Task ID</label><input id="task" maxlength="200"><button id="inspect">Inspect</button><button id="cancel" class="secondary">Request cancellation</button><button id="recover" class="secondary">Recover after resolution</button>
<pre id="detail"></pre><p class="hint">Unknown effects require evidence-backed resolution. Cancellation intent does not prove a remote effect stopped.</p>
<label for="path">Composed leaf path</label><input id="path" maxlength="500" placeholder="root/0"><button id="leaf" class="secondary">Inspect leaf</button><pre id="leaf-detail"></pre>
<label for="graph-section">Graph structure</label><select id="graph-section"><option value="nodes">Nodes</option><option value="attempts">Execution attempts</option><option value="scopes">Nested scopes</option></select><label for="graph-offset">Graph offset</label><input id="graph-offset" inputmode="numeric" value="0"><button id="graph" class="secondary">Read graph page</button><pre id="graph-detail"></pre>
<label for="reply-key">Continuation request key</label><input id="reply-key" maxlength="200"><label for="reply">Human input JSON</label><textarea id="reply" maxlength="64000">{}</textarea><button id="continue">Continue paused work</button></section>
<section><h2>Progress</h2><button id="progress">Read next events</button><p class="hint">Retained events resume from the last received ID. Archived history may require inspecting the compact receipt.</p><pre id="events"></pre></section>
<section><h2>Operator controls</h2><p class="hint">Operator access requires a separate administrative token scope and explicit grants. Task ownership alone grants no administrative access.</p>
<button id="op-connect">Load operator permissions</button><pre id="op-permissions"></pre>
<label for="op-owner">Authorised task owner</label><select id="op-owner"></select><button id="op-tasks">Find owner tasks</button><button id="op-registrations">Discover owner registrations</button><button id="op-inbox">Recovery inbox for selected task</button>
<label for="op-offset">Administrative page offset</label><input id="op-offset" inputmode="numeric" value="0"><p class="hint">Lists show up to 20 records. Use the returned next_offset for another page. Task lists use Load more above.</p>
<label for="op-call">Invocation ID from recovery inbox</label><input id="op-call" maxlength="200">
<label for="op-key">Operator request key</label><input id="op-key" maxlength="200"><button id="op-new-key" class="secondary">New request key</button>
<label for="op-reason">Reason</label><input id="op-reason" maxlength="2048">
<label for="op-revision">Expected budget, deadline or worker-control revision</label><input id="op-revision" inputmode="numeric" value="0">
<details><summary>Resolve an uncertain effect</summary><p class="hint">Record an evidence-supported outcome. Resolution does not resume the task or refund its allocation.</p>
<label for="op-query-name">Registered evidence query name</label><input id="op-query-name" maxlength="200"><label for="op-query-version">Exact query version</label><input id="op-query-version" maxlength="200"><button id="op-query">Query reconciliation evidence</button>
<label for="op-decision">Decision</label><select id="op-decision"><option value="completed">Completed</option><option value="confirmed_no_effect">Confirmed no effect</option></select>
<label for="op-evidence">Evidence JSON: source, reference, observed_at_ms</label><textarea id="op-evidence" maxlength="32000">{}</textarea>
<label for="op-output">Verified output JSON for completion</label><textarea id="op-output" maxlength="32000">{}</textarea><button id="op-resolve">Record resolution</button></details>
<details><summary>Deadline and lifetime budget amendments</summary><label for="op-limits">Absolute limits JSON</label><textarea id="op-limits" maxlength="8000">{}</textarea>
<p class="hint">Deadlines accept business_seconds, queue_seconds or pause_seconds. Budgets accept max_runs, max_calls or max_reserved_microusd. Existing use stays charged.</p>
<button id="op-deadline">Extend selected task deadline</button><button id="op-budget">Read lifetime budget and revision</button><button id="op-budget-amend">Increase lifetime budget</button></details>
<details><summary>Allocation periods and provider charges</summary><button id="op-periods">Read allocation periods</button>
<label for="op-period">Period policy JSON: scope and policy</label><textarea id="op-period" maxlength="8000">{}</textarea>
<p class="hint">Scope is deployment or principal:NAME. Policy contains period_seconds, max_calls, max_reserved_microusd and optionally start_epoch. Existing reservations remain charged.</p><button id="op-period-configure">Configure fixed allocation period</button>
<label for="op-charge">Charge JSON: provider, charge_id, run_id, call_id, actual_microusd, observed_at, evidence</label><textarea id="op-charge" maxlength="8000">{}</textarea>
<p class="hint">observed_at is UTC epoch seconds; evidence is an exact source reference. Observations do not refund allocations or establish invoice completeness.</p><button id="op-charge-record">Record provider charge observation</button><button id="op-charge-reconcile">Compare charges with reservation</button></details>
<details><summary>Latency observations</summary><button id="op-telemetry">Read latency histograms and recent spans</button><p class="hint">Deployment administrators can inspect this controller's retained observations. These samples can be dropped and are separate from the durable effect and billing records.</p></details>
<details><summary>Storage and local workers</summary><button id="op-storage">Storage and admission status</button><button id="op-workers">Worker drain status</button>
<label for="op-draining">Local dispatcher state</label><select id="op-draining"><option value="true">Drain: stop new assignments</option><option value="false">Resume new assignments</option></select><button id="op-drain">Apply worker state</button>
<p class="hint">Drain preserves running effects and receipts. It does not establish remote cancellation.</p>
<label for="op-archive-kind">Retention preview</label><select id="op-archive-kind"><option value="tasks">Tasks</option><option value="workflows">Composed workflows</option><option value="runs">Run files</option></select>
<label for="op-age">Minimum age in seconds</label><input id="op-age" inputmode="numeric" value="86400"><button id="op-archive">Preview retention eligibility</button><button id="op-archive-apply">Archive selected lifecycle</button><p class="hint">Archival requires both deployment administration and a grant for the selected owner. It removes eligible detailed history while retaining identities and charged allocations. Run archival here requires the shared profile and an unreferenced standalone run; local run pruning uses the maintenance CLI.</p></details>
<details><summary>Shared artefact retention</summary><button id="op-artifact-namespaces">Inspect namespaces</button>
<label for="op-artifact-namespace">Namespace</label><input id="op-artifact-namespace" maxlength="200"><label for="op-artifact-principal">Configured artefact principal for selected owner</label><input id="op-artifact-principal" maxlength="200"><button id="op-artifact-links">Inspect retention links</button>
<label for="op-artifact-link">Exact retention link ID</label><input id="op-artifact-link" maxlength="200"><button id="op-artifact-bind">Bind legacy link to selected task</button><button id="op-artifact-release">Release selected link</button><button id="op-artifact-retire">Retire namespace</button><p class="hint">Use the request key and reason above. Binding asserts the complete owning lifecycle for an old link. Release requires evidence that this consumer no longer needs the reference. Namespace retirement permanently closes publication after all links are released. Content and charged reservations remain retained.</p></details>
<details><summary>Shared command receipt capacity</summary><button id="op-command-capacity">Inspect shared command capacity</button><button id="op-command-amend">Increase shared command capacity</button><p class="hint">Use the amendment fields above, limits JSON with normal_limit and recovery_limit, and evidence JSON. Existing command identities remain retained; this operation is available on the shared profile.</p></details>
<details><summary>Federation peer administration</summary><button id="op-peer-status">Inspect peer registry</button><label for="op-peer">Versioned peer registration JSON</label><textarea id="op-peer" maxlength="32000">{}</textarea><button id="op-peer-register">Register exact peer version</button><label for="op-peer-name">Peer name to revoke</label><input id="op-peer-name" maxlength="200"><label for="op-peer-version">Peer version to revoke</label><input id="op-peer-version" maxlength="200"><button id="op-peer-revoke">Revoke peer version</button><p class="hint">Peer changes require a separate manage_peers grant. Registration pins endpoint, deployment, workflow identities, callers and capabilities. Credentials stay in trusted startup configuration. Revocation stops future authorisation and does not cancel earlier effects.</p></details>
<details><summary>Request epoch retention</summary><label for="op-epoch">Explicit client request epoch</label><input id="op-epoch" maxlength="200"><label for="op-prune">Settled receipt retention</label><select id="op-prune"><option value="false">Retain receipts</option><option value="true">Prune eligible receipts after epoch retirement</option></select><button id="op-epoch-status">Inspect epoch</button><button id="op-epoch-retire">Retire drained epoch</button><p class="hint">Retirement permanently closes this epoch to new or delayed requests. The service rejects epochs with unfinished or uncertain work.</p></details>
<details><summary>Definition availability</summary><label for="op-kind">Definition kind</label><select id="op-kind"><option value="workflow">Workflow</option><option value="plan">Plan</option></select>
<label for="op-name">Registration name</label><input id="op-name" maxlength="200"><label for="op-identity">Exact definition identity</label><input id="op-identity" maxlength="200">
<button id="op-definition">Inspect availability</button><button id="op-retire">Retire unused historical definition</button><p class="hint">The service rejects retirement of current definitions or definitions referenced by unfinished work.</p></details>
<pre id="op-result"></pre></section>
<script>SCRIPT</script></body></html>"""


def console_response():
    digest = lambda value: base64.b64encode(hashlib.sha256(value.encode()).digest()).decode()
    content = BODY.replace('STYLE', STYLE).replace('SCRIPT', SCRIPT).encode()
    return content, {
        'Content-Security-Policy': "default-src 'none'; connect-src 'self'; script-src 'sha256-" + digest(SCRIPT)
            + "'; style-src 'sha256-" + digest(STYLE) + "'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
        'Referrer-Policy': 'no-referrer',
        'Permissions-Policy': 'camera=(), microphone=(), geolocation=()',
    }

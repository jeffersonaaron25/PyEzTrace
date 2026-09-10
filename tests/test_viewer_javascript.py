"""Execute live polling state transitions in Node with a minimal DOM adapter."""
import shutil
import subprocess
from pathlib import Path

import pytest

from pyeztrace.viewer import TraceViewerServer


def test_polling_races_and_unavailable_metrics(tmp_path):
    node = shutil.which('node')
    if not node:
        pytest.skip('Node is required for embedded JavaScript regression checks')
    bundle = TraceViewerServer(Path('/tmp/unused'))._js_bundle()
    start = bundle.rindex('  loadState();')
    bundle = bundle[:start] + r'''
  (async()=>{
    render = ()=>{}; renderStatusBar = ()=>{}; renderEmptyState = ()=>{};
    rebuildCallToRunMap = ()=>{}; renderFnTypeOptions = ()=>{};
    scheduleRefresh = ()=>{};
    tree = [{call_id:'kept'}]; total = 1;
    let resolve;
    fetchJson = ()=>new Promise(r=>{ resolve = r; });
    logs = [{id:0}]; logsFetchCounter = 1;
    const poll = fetchTree();
    autoRefreshEl.handlers.change({target:{checked:false}});
    resolve({roots:[{call_id:'unexpected'}], total_nodes:1, source:{generation:'g',calls_started:2}});
    await poll;
    assert(tree[0].call_id === 'kept', 'an in-flight poll changed the paused view');
    sourceState = {generation:'g',calls_started:2};
    snapshotGeneration = 'g'; snapshotCalls = 2; snapshotAt = 100;
    pausedGeneration = 'g'; pausedBaselineNodes = 2;
    fetchJson = async()=>({source:{generation:'g',calls_started:7},total_nodes:1});
    await pollStatusOnly();
    assert(liveNodeCount === 7 && tree[0].call_id === 'kept', 'status probe changed view or counted retained nodes');
    assert(elapsedReferenceTime() === 100, 'paused elapsed timer advanced');
    let statusCalls = 0; let treeCalls = 0;
    pollStatusOnly = ()=>{ statusCalls++; }; fetchTree = ()=>{ treeCalls++; };
    retryNowEl.handlers.click();
    assert(statusCalls === 1 && treeCalls === 0, 'retry resumed a paused view');
    sourceState = {status:'ok', generation:'g'};
    tree = [{call_id:'running', function:'work', status:'running', start_time:1, children:[]}];
    metrics = []; total = 1;
    const overview = buildOverviewPanel();
    assert(overview.includes('No recorded memory samples'), 'missing memory appears as zero');
    assert(!overview.includes('likely interrupted'), 'long running call is labeled interrupted');

    // A running-call timer must never run backwards. It used to keep counting
    // from the live clock for the whole staleness grace period and then snap
    // back to the last successful read.
    autoRefreshEnabled = true;
    consecutiveFailures = 0;
    connState = 'ok';
    sourceState = {status:'ok', generation:'g'};
    const startedAt = Date.now()/1000 - 40;
    lastSuccessAt = Date.now()/1000;
    snapshotAt = Date.now()/1000;
    const node = {call_id:'r', function:'work', status:'running', duration:null, start_time:startedAt};
    const parseSecs = (html)=>parseFloat(html.replace(/<[^>]*>/g,'').replace(/[^0-9.]/g,''));
    const liveSecs = parseSecs(elapsedHtml(node));
    assert(elapsedIsLive(), 'timer not live while polls are succeeding');

    // Simulate the server going away: snapshotAt stays put, the clock moves on.
    snapshotAt = Date.now()/1000 - 5;
    consecutiveFailures = 1;
    assert(!elapsedIsLive(), 'timer still live after a failed poll');
    const frozenSecs = parseSecs(elapsedHtml(node));
    assert(frozenSecs <= liveSecs + 0.5, 'frozen timer exceeded the last successful read');
    const frozenHtml = elapsedHtml(node);
    assert(frozenHtml.includes('frozen'), 'frozen timer is not marked as frozen');
    assert(/last successful read/.test(frozenHtml), 'frozen timer does not explain itself');

    // Held steady across ticks rather than drifting.
    const again = parseSecs(elapsedHtml(node));
    assert(again === frozenSecs, 'frozen timer drifted between reads');

    // Recovering restores a live, forward-moving timer.
    consecutiveFailures = 0;
    lastSuccessAt = Date.now()/1000;
    snapshotAt = Date.now()/1000;
    assert(elapsedIsLive(), 'timer did not resume after recovery');
    assert(parseSecs(elapsedHtml(node)) >= frozenSecs, 'timer went backwards after recovery');

    console.log('PASS: pause race, status count, frozen timer, retry, unavailable metrics, monotonic elapsed');
  })().catch(err=>{ console.error(err); process.exitCode=1; });
})();
'''
    stub = r'''
const assert = (condition, message)=>{ if(!condition) throw new Error(message); };
const elements = new Map();
const makeElement = ()=>({handlers:{}, value:'', checked:true, textContent:'', innerHTML:'',
  style:{}, dataset:{}, classList:{add(){},remove(){},toggle(){}},
  addEventListener(name, fn){this.handlers[name]=fn;}, querySelector(){return null;},
  querySelectorAll(){return [];}, getBoundingClientRect(){return {width:1000};}});
global.document = {hidden:false, getElementById(id){
  if(!elements.has(id)) elements.set(id,makeElement()); return elements.get(id);
}, addEventListener(){}, querySelectorAll(){return [];}};
global.window = {addEventListener(){}, innerWidth:1200};
global.localStorage = {getItem(){return null;},setItem(){}};
global.navigator = {};
'''
    script = tmp_path / 'viewer-state.js'
    script.write_text(stub + bundle)
    result = subprocess.run([node, str(script)], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr

import json
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from pathlib import Path
from typing import Dict, Any, List, Optional
import time


class _TraceTreeBuilder:
    def __init__(self, log_file: Path) -> None:
        self.log_file = log_file

    def _read_lines(self) -> List[str]:
        if not self.log_file.exists():
            return []
        try:
            with self.log_file.open('r', encoding='utf-8', errors='ignore') as f:
                return f.readlines()
        except Exception:
            return []

    def _parse_json_lines(self, lines: List[str]) -> List[Dict[str, Any]]:
        entries = []
        for line in lines:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                # Minimal validation
                if isinstance(obj, dict) and 'timestamp' in obj and 'level' in obj:
                    entries.append(obj)
            except Exception:
                # Ignore non-JSON lines
                continue
        return entries

    def _to_epoch(self, timestamp_str: str) -> float:
        try:
            # Format: YYYY-MM-DDTHH:MM:SS
            # Parse conservatively to avoid extra deps
            struct_time = time.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
            return time.mktime(struct_time)
        except Exception:
            return time.time()

    def build_tree(self) -> Dict[str, Any]:
        lines = self._read_lines()
        entries = self._parse_json_lines(lines)
        nodes: Dict[str, Dict[str, Any]] = {}
        metrics_entries: List[Dict[str, Any]] = []
        roots: List[str] = []

        for e in entries:
            data = e.get('data') or {}
            call_id = data.get('call_id')
            parent_id = data.get('parent_id')
            event = data.get('event')  # 'start' | 'end' | 'error' | None
            function = e.get('function') or data.get('function')
            fn_type = e.get('fn_type') or data.get('fn_type')
            status = data.get('status')

            if event == 'metrics_summary':
                metrics_entries.append({
                    'timestamp': e.get('timestamp'),
                    'status': status or e.get('level'),
                    'metrics': data.get('metrics', []),
                    'total_functions': data.get('total_functions'),
                    'total_calls': data.get('total_calls'),
                    'generated_at': data.get('generated_at') or self._to_epoch(e.get('timestamp', ''))
                })
                continue

            if not call_id:
                # Not a structured trace entry; skip from tree but include as loose log?
                continue

            if call_id not in nodes:
                nodes[call_id] = {
                    'call_id': call_id,
                    'parent_id': parent_id,
                    'function': function,
                    'fn_type': fn_type,
                    'start_time': None,
                    'end_time': None,
                    'duration': None,
                    'cpu_time': None,
                    'mem_peak_kb': None,
                    'mem_delta_kb': None,
                    'args_preview': None,
                    'kwargs_preview': None,
                    'result_preview': None,
                    'status': status,
                    'level': e.get('level'),
                    'project': e.get('project'),
                    'children': []  # child call_ids
                }

            node = nodes[call_id]

            # Link parent-child
            if parent_id and parent_id in nodes:
                parent = nodes[parent_id]
                if call_id not in parent['children']:
                    parent['children'].append(call_id)

            # Identify roots later after all nodes present

            # Timestamps and metrics
            if event == 'start':
                node['start_time'] = data.get('time_epoch') or self._to_epoch(e.get('timestamp', ''))
                node['args_preview'] = data.get('args_preview')
                node['kwargs_preview'] = data.get('kwargs_preview')
                node['status'] = status or 'running'
            elif event == 'end':
                node['end_time'] = data.get('time_epoch') or self._to_epoch(e.get('timestamp', ''))
                node['duration'] = e.get('duration')
                node['cpu_time'] = data.get('cpu_time')
                node['mem_peak_kb'] = data.get('mem_peak_kb')
                node['mem_delta_kb'] = data.get('mem_delta_kb')
                node['result_preview'] = data.get('result_preview')
                node['status'] = status or 'success'
            elif event == 'error':
                # Mark node with error info
                node['error'] = e.get('message')
                node['status'] = status or 'error'
                node['end_time'] = data.get('time_epoch') or self._to_epoch(e.get('timestamp', ''))

        # Determine roots
        seen_as_child = set()
        for n in nodes.values():
            for c in n['children']:
                seen_as_child.add(c)
        roots = [cid for cid, n in nodes.items() if not n.get('parent_id') or cid not in seen_as_child]

        # Convert to nested structure
        def materialize(cid: str) -> Dict[str, Any]:
            n = nodes[cid]
            return {
                **{k: v for k, v in n.items() if k != 'children'},
                'children': [materialize(child) for child in n['children']]
            }

        tree = [materialize(cid) for cid in roots]
        return {
            'generated_at': time.time(),
            'log_file': str(self.log_file),
            'roots': tree,
            'total_nodes': len(nodes),
            'metrics': metrics_entries
        }


class TraceViewerServer:
    def __init__(self, log_file: Path, host: str = '127.0.0.1', port: int = 8765) -> None:
        self.log_file = log_file
        self.host = host
        self.port = port
        self._builder = _TraceTreeBuilder(log_file)
        self._httpd: Optional[ThreadingHTTPServer] = None

    def _handler_factory(self):
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def _send(self, code: int, body: bytes, ctype: str = 'application/json'):
                self.send_response(code)
                self.send_header('Content-Type', ctype)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):  # noqa: N802 (keep stdlib name)
                parsed = urlparse(self.path)
                if parsed.path == '/':
                    self._send(200, outer._html_page().encode('utf-8'), 'text/html; charset=utf-8')
                elif parsed.path == '/app.js':
                    self._send(200, outer._js_bundle().encode('utf-8'), 'application/javascript')
                elif parsed.path == '/api/tree':
                    data = outer._builder.build_tree()
                    self._send(200, json.dumps(data).encode('utf-8'), 'application/json')
                elif parsed.path == '/api/entries':
                    # raw entries for debugging
                    lines = outer._builder._read_lines()
                    entries = outer._builder._parse_json_lines(lines)
                    self._send(200, json.dumps(entries[-1000:]).encode('utf-8'), 'application/json')
                else:
                    self._send(404, b'Not Found', 'text/plain')

            def log_message(self, format, *args):  # Silence default logging
                return

        return Handler

    def _html_page(self) -> str:
        return (
            """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>PyEzTrace Viewer</title>
  <style>
    :root {
      color-scheme: light dark;
      --bg: #0b1220;
      --surface: #0f172a;
      --surface-soft: #111827;
      --border: #1f2937;
      --text: #e5e7eb;
      --muted: #9ca3af;
      --accent: #38bdf8;
      --success: #22c55e;
      --error: #ef4444;
    }
    * { box-sizing: border-box; }
    body { font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 0; background: var(--bg); color: var(--text); }
    header { position: sticky; top: 0; z-index: 2; background: linear-gradient(180deg, rgba(15,23,42,0.95), rgba(15,23,42,0.85)); color: var(--text); padding: 12px 16px; display: flex; align-items: center; gap: 12px; border-bottom: 1px solid var(--border); backdrop-filter: blur(10px); }
    header input { padding: 10px 12px; width: 320px; border-radius: 8px; border: 1px solid var(--border); background: var(--surface-soft); color: var(--text); outline: none; transition: border-color 120ms ease; }
    header input:focus { border-color: var(--accent); box-shadow: 0 0 0 2px rgba(56,189,248,0.25); }
    header .meta { margin-left: auto; font-size: 12px; color: var(--muted); display: flex; gap: 12px; align-items: center; }
    main { padding: 16px; max-width: 1200px; margin: 0 auto; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin: 16px 0; }
    .card { border: 1px solid var(--border); background: var(--surface); border-radius: 10px; padding: 12px; box-shadow: 0 10px 30px rgba(0,0,0,0.25); }
    .node { border: 1px solid var(--border); border-radius: 10px; margin: 10px 0; padding: 10px 12px; background: var(--surface); box-shadow: inset 0 1px 0 rgba(255,255,255,0.02); }
    .node.error { border-color: rgba(239,68,68,0.6); background: rgba(239,68,68,0.05); }
    .title { display: flex; align-items: center; gap: 10px; cursor: pointer; }
    .fn { font-weight: 700; letter-spacing: -0.01em; }
    .pill { font-size: 11px; padding: 4px 8px; border-radius: 999px; background: rgba(56,189,248,0.15); color: #38bdf8; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 700; border: 1px solid rgba(56,189,248,0.35); }
    .pill.error { background: rgba(239,68,68,0.15); color: #fca5a5; border-color: rgba(239,68,68,0.4); }
    .metrics { font-size: 12px; color: var(--muted); display: flex; gap: 10px; flex-wrap: wrap; }
    .kv { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; background: var(--surface-soft); padding: 6px 8px; border-radius: 6px; margin: 4px 0; border: 1px solid var(--border); }
    .children { margin-left: 16px; border-left: 2px dashed var(--border); padding-left: 10px; }
    .muted { color: var(--muted); font-size: 12px; }
    .toolbar { display: flex; align-items: center; gap: 8px; }
    .btn { background: var(--surface-soft); color: var(--text); border: 1px solid var(--border); padding: 9px 11px; border-radius: 8px; cursor: pointer; transition: transform 120ms ease, border-color 120ms ease; }
    .btn.primary { background: linear-gradient(135deg, #38bdf8, #0ea5e9); color: #0b1220; border: none; font-weight: 700; }
    .btn:active { transform: translateY(1px); }
    .badges { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .badge { background: rgba(56,189,248,0.12); color: #7dd3fc; border: 1px solid rgba(56,189,248,0.25); padding: 4px 8px; border-radius: 8px; font-size: 12px; }
    .badge.error { background: rgba(239,68,68,0.12); color: #fecdd3; border-color: rgba(239,68,68,0.3); }
    .section-title { display: flex; align-items: center; justify-content: space-between; margin-top: 8px; margin-bottom: 6px; color: var(--muted); font-size: 13px; text-transform: uppercase; letter-spacing: 0.08em; }
    .toggle { display: flex; align-items: center; gap: 6px; color: var(--muted); font-size: 12px; cursor: pointer; }
    .chip-group { display: flex; gap: 6px; }
    .chip { padding: 6px 10px; border-radius: 999px; border: 1px solid var(--border); background: var(--surface); color: var(--muted); cursor: pointer; font-size: 12px; }
    .chip.active { border-color: var(--accent); color: var(--text); box-shadow: 0 0 0 2px rgba(56,189,248,0.2); }
    .flex { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    .hidden { display: none; }
    .grow { flex: 1; }
    .timestamp { font-size: 12px; color: var(--muted); }
    .collapsible-section { margin-bottom: 16px; }
    .collapsible-header { display: flex; align-items: center; justify-content: space-between; cursor: pointer; padding: 12px; background: var(--surface-soft); border: 1px solid var(--border); border-radius: 8px; transition: background 120ms ease; }
    .collapsible-header:hover { background: var(--surface); }
    .collapsible-content { margin-top: 8px; }
    .collapsible-content.hidden { display: none; }
    .chevron { transition: transform 200ms ease; display: inline-block; }
    .chevron.expanded { transform: rotate(90deg); }
    .metrics-table { width: 100%; border-collapse: collapse; margin-top: 8px; }
    .metrics-table th { text-align: left; padding: 10px 12px; background: var(--surface-soft); border-bottom: 2px solid var(--border); color: var(--text); font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; }
    .metrics-table td { padding: 10px 12px; border-bottom: 1px solid var(--border); color: var(--text); font-size: 13px; }
    .metrics-table tr:hover { background: var(--surface-soft); }
    .metrics-table tr:last-child td { border-bottom: none; }
    .metrics-table .function-name { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-weight: 600; color: var(--accent); }
    .metrics-table .number { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; text-align: right; }
    .metrics-table .bad { color: var(--error); }
    .metrics-table .good { color: var(--success); }
    .metrics-summary { display: flex; gap: 12px; margin-bottom: 12px; flex-wrap: wrap; }
    .metrics-summary-item { padding: 8px 12px; background: var(--surface-soft); border: 1px solid var(--border); border-radius: 6px; font-size: 12px; }
    .metrics-summary-item strong { color: var(--accent); margin-right: 6px; }
  </style>
  <script defer src="/app.js"></script>
  <script>
    window.__PYEZTRACE_VIEWER_CONFIG__ = {};
  </script>
</head>
<body>
  <header>
    <div class="toolbar">
      <strong>PyEzTrace Viewer</strong>
      <input id="search" placeholder="Search functions, errors, IDs..." />
      <div class="chip-group" id="status-filter">
        <button class="chip active" data-filter="all">All</button>
        <button class="chip" data-filter="errors">Errors only</button>
        <button class="chip" data-filter="completed">Completed</button>
      </div>
      <button class="btn primary" id="refresh">Refresh</button>
    </div>
    <div class="meta" id="meta"></div>
  </header>
  <main>
    <div class="grid" id="overview"></div>
    <div id="root"></div>
  </main>
</body>
</html>
            """
        ).strip()

    def _js_bundle(self) -> str:
        return (
            """


(function(){
  const rootEl = document.getElementById('root');
  const searchEl = document.getElementById('search');
  const metaEl = document.getElementById('meta');
  const refreshBtn = document.getElementById('refresh');
  const overviewEl = document.getElementById('overview');
  const statusFilterGroup = document.getElementById('status-filter');
  const autoRefreshToggle = document.getElementById('auto-refresh');
  const expandAllBtn = document.getElementById('expand-all');
  const collapseAllBtn = document.getElementById('collapse-all');

  let tree = [];
  let total = 0;
  let metrics = [];
  let generatedAt = null;
  let statusFilter = 'all';
  const expanded = new Set();
  let metricsExpanded = false; // Collapsed by default
  let refreshTimer = null;

  async function fetchTree(){
    const res = await fetch('/api/tree');
    const data = await res.json();
    tree = data.roots || [];
    total = data.total_nodes || 0;
    metrics = data.metrics || [];
    generatedAt = data.generated_at || null;
    metaEl.textContent = `${generatedAt ? new Date(generatedAt*1000).toLocaleString() : ''} • ${data.log_file} • ${total} nodes`;
    render();
  }

  function fmt(n){ return n==null ? '-' : (typeof n==='number' ? n.toFixed(6) : String(n)); }

  function fmtDuration(ms){
    if(ms==null) return '-';
    if(ms >= 1) return `${ms.toFixed(3)}s`;
    return `${(ms*1000).toFixed(1)}ms`;
  }

  function fmtTime(epoch){
    if(!epoch) return '-';
    const d = new Date(epoch*1000);
    return `${d.toLocaleTimeString()} (${d.toLocaleDateString()})`;
  }

  function matchFilter(node, q){
    const hay = [node.function||'', node.error||'', node.call_id||'', node.parent_id||'', node.status||''].join(' ').toLowerCase();
    return hay.includes(q);
  }

  function passesStatus(node){
    if(statusFilter === 'all') return true;
    if(statusFilter === 'errors') return !!node.error || node.status === 'error';
    if(statusFilter === 'completed') return node.status === 'success';
    return true;
  }

  function shouldDisplay(node, q){
    const selfMatch = matchFilter(node, q) && passesStatus(node);
    const childMatch = (node.children||[]).some(c=>shouldDisplay(c, q));
    return selfMatch || childMatch;
  }

  function renderNode(node, q, depth){
    const visible = shouldDisplay(node, q);
    if(!visible) return '';

    const metrics = [
      `time: ${fmtDuration(node.duration)}`,
      `cpu: ${fmt(node.cpu_time)}s`,
      `memΔ: ${node.mem_delta_kb==null?'-':node.mem_delta_kb+' KB'}`,
      `peak: ${node.mem_peak_kb==null?'-':node.mem_peak_kb+' KB'}`
    ].join(' • ');

    const args = node.args_preview!=null ? JSON.stringify(node.args_preview) : '-';
    const kwargs = node.kwargs_preview!=null ? JSON.stringify(node.kwargs_preview) : '-';
    const result = node.result_preview!=null ? JSON.stringify(node.result_preview) : '-';
    const hasErr = !!node.error;
    const isExpanded = expanded.has(node.call_id) || depth < 1 || hasErr;
    const badges = [
      node.fn_type ? `<span class="pill">${node.fn_type}</span>` : '',
      hasErr ? `<span class="pill error">error</span>` : '',
      node.status ? `<span class="badge ${hasErr?'error':''}">${node.status}</span>` : ''
    ].join('');

    return `
      <div class="node ${hasErr?'error':''}">
        <div class="title" data-cid="${node.call_id}" onclick="window.__toggleNode('${node.call_id}')">
          ${badges}
          <span class="fn">${node.function||node.call_id}</span>
          <span class="metrics">${metrics}</span>
          <span class="grow"></span>
          <span class="timestamp">${fmtTime(node.start_time)}</span>
          <span class="pill">${isExpanded ? 'Collapse' : 'Expand'}</span>
        </div>
        <div class="details ${isExpanded?'':'hidden'}" data-details="${node.call_id}">
          <div class="kv"><strong>call_id:</strong> ${node.call_id} ${node.parent_id?`<span class="muted">parent:</span> ${node.parent_id}`:''}</div>
          <div class="kv"><strong>status:</strong> ${node.status||'-'} • <strong>started:</strong> ${fmtTime(node.start_time)} • <strong>ended:</strong> ${fmtTime(node.end_time)}</div>
          <div class="kv"><strong>args:</strong> ${args}</div>
          <div class="kv"><strong>kwargs:</strong> ${kwargs}</div>
          <div class="kv"><strong>result:</strong> ${result}</div>
          ${hasErr?`<div class="kv"><strong>error:</strong> ${node.error}</div>`:''}
          ${node.children && node.children.length ? `<div class="children">${node.children.map(n=>renderNode(n,q, depth+1)).join('')}</div>` : ''}
        </div>
      </div>
    `;
  }

  function render(){
    const q = (searchEl.value||'').toLowerCase().trim();
    const html = tree.map(n=>renderNode(n, q, 0)).join('');
    const traceHeader = `<div class="section-title">
      <span>Trace calls</span>
      <div class="flex">
        <label class="toggle"><input type="checkbox" id="auto-refresh" checked /> Auto refresh</label>
        <button class="btn" id="expand-all">Expand all</button>
        <button class="btn" id="collapse-all">Collapse all</button>
      </div>
    </div>`;
    const traceHtml = traceHeader + (html || '<div class="muted">No trace nodes found. Ensure EZTRACE_LOG_FORMAT=json.</div>');

    const overviewCards = [];
    overviewCards.push(`<div class="card"><div class="section-title"><span>Last updated</span></div><div class="fn">${generatedAt ? new Date(generatedAt*1000).toLocaleString() : '-'}</div><div class="muted">Live reload every 2.5s</div></div>`);
    overviewCards.push(`<div class="card"><div class="section-title"><span>Nodes</span></div><div class="fn">${total}</div><div class="muted">Trace entries parsed</div></div>`);
    const errorCount = countMatches(tree, n => !!n.error || n.status === 'error');
    overviewCards.push(`<div class="card"><div class="section-title"><span>Errors</span></div><div class="fn" style="color:#fca5a5;">${errorCount}</div><div class="muted">Across all calls</div></div>`);
    overviewCards.push(`<div class="card"><div class="section-title"><span>Metrics snapshots</span></div><div class="fn">${metrics.length}</div><div class="muted">performance summaries</div></div>`);
    overviewEl.innerHTML = overviewCards.join('');

    // Build metrics HTML (collapsible, at the top)
    const metricsHtml = metrics.length ? `
      <div class="collapsible-section">
        <div class="collapsible-header" onclick="window.__toggleMetrics()">
          <div class="section-title" style="margin:0;">
            <span>Performance metrics</span>
            <span class="muted" style="font-size:11px; margin-left:8px;">(${metrics.length} snapshot${metrics.length !== 1 ? 's' : ''})</span>
          </div>
          <span class="chevron ${metricsExpanded ? 'expanded' : ''}" style="color:var(--muted);">▶</span>
        </div>
        <div class="collapsible-content ${metricsExpanded ? '' : 'hidden'}">
          ${metrics.map((m, idx)=>`
            <div class="card" style="margin-bottom: ${idx < metrics.length - 1 ? '20px' : '0'};">
              <div class="flex" style="justify-content: space-between; margin-bottom: 12px; padding-bottom: 12px; border-bottom: 1px solid var(--border);">
                <div class="badges">
                  <span class="badge">${m.status||'-'}</span>
                  <span class="badge">${m.timestamp||'-'}</span>
                </div>
                <div class="muted" style="font-size: 11px;">Generated ${m.generated_at ? new Date(m.generated_at*1000).toLocaleString() : '-'}</div>
              </div>
              <div class="metrics-summary">
                <div class="metrics-summary-item">
                  <strong>Functions:</strong>${m.total_functions||0}
                </div>
                <div class="metrics-summary-item">
                  <strong>Total calls:</strong>${m.total_calls||0}
                </div>
                <div class="metrics-summary-item">
                  <strong>Metrics:</strong>${(m.metrics||[]).length}
                </div>
              </div>
              ${(m.metrics||[]).length > 0 ? `
                <table class="metrics-table">
                  <thead>
                    <tr>
                      <th>Function</th>
                      <th class="number">Calls</th>
                      <th class="number">Total Time</th>
                      <th class="number">Avg Time</th>
                      <th class="number">Time/Call</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${(m.metrics||[]).map(row=>{
                      const avgMs = (row.avg_seconds || 0) * 1000;
                      const timePerCall = row.calls > 0 ? (row.total_seconds / row.calls * 1000) : 0;
                      const avgClass = avgMs > 100 ? 'bad' : avgMs > 10 ? '' : 'good';
                      return `
                        <tr>
                          <td class="function-name">${row.function||'-'}</td>
                          <td class="number">${row.calls||0}</td>
                          <td class="number">${(row.total_seconds||0).toFixed(6)}s</td>
                          <td class="number ${avgClass}">${(row.avg_seconds||0).toFixed(6)}s</td>
                          <td class="number">${timePerCall.toFixed(3)}ms</td>
                        </tr>
                      `;
                    }).join('')}
                  </tbody>
                </table>
              ` : '<div class="muted" style="padding:16px 0; text-align:center;">No metrics data available</div>'}
            </div>
          `).join('')}
        </div>
      </div>
    ` : '';

    rootEl.innerHTML = metricsHtml + traceHtml;
  }

  searchEl.addEventListener('input', render);
  refreshBtn.addEventListener('click', fetchTree);

  window.__toggleNode = function(cid){
    if(expanded.has(cid)) expanded.delete(cid); else expanded.add(cid);
    render();
  }

  window.__toggleMetrics = function(){
    metricsExpanded = !metricsExpanded;
    render();
  }

  function setStatusFilter(val){
    statusFilter = val;
    [...statusFilterGroup.querySelectorAll('.chip')].forEach(btn=>{
      btn.classList.toggle('active', btn.dataset.filter === val);
    });
    render();
  }
  statusFilterGroup.addEventListener('click', (e)=>{
    if(e.target && e.target.dataset && e.target.dataset.filter){
      setStatusFilter(e.target.dataset.filter);
    }
  });

  function countMatches(nodes, predicate){
    let count = 0;
    nodes.forEach(n=>{
      if(predicate(n)) count++;
      if(n.children) count += countMatches(n.children, predicate);
    });
    return count;
  }

  expandAllBtn.addEventListener('click', ()=>{
    const collect = (nodes)=>{ nodes.forEach(n=>{ expanded.add(n.call_id); if(n.children) collect(n.children); }); };
    collect(tree); render();
  });

  collapseAllBtn.addEventListener('click', ()=>{ expanded.clear(); render(); });

  autoRefreshToggle.addEventListener('change', ()=>{
    if(autoRefreshToggle.checked){ scheduleRefresh(); }
    else { clearInterval(refreshTimer); refreshTimer = null; }
  });

  function scheduleRefresh(){
    if(refreshTimer) clearInterval(refreshTimer);
    refreshTimer = setInterval(()=>{
      if(autoRefreshToggle.checked) fetchTree();
    }, 2500);
  }

  fetchTree();
  scheduleRefresh();
})();
            """
        ).strip()

    def serve_forever(self) -> None:
        self._httpd = ThreadingHTTPServer((self.host, self.port), self._handler_factory())
        print(f"PyEzTrace Viewer serving on http://{self.host}:{self.port} (reading {self.log_file})")
        try:
            self._httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            self._httpd.server_close()


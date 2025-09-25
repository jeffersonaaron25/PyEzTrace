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
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 0; }
    header { background: #111827; color: #f9fafb; padding: 12px 16px; display: flex; align-items: center; gap: 12px; }
    header input { padding: 8px 10px; width: 320px; border-radius: 6px; border: 1px solid #374151; background: #111827; color: #e5e7eb; }
    header .meta { margin-left: auto; font-size: 12px; color: #9ca3af; }
    main { padding: 16px; }
    .node { border: 1px solid #e5e7eb; border-radius: 8px; margin: 6px 0; padding: 8px 10px; }
    .node.error { border-color: #ef4444; background: #fef2f2; }
    .title { display: flex; align-items: center; gap: 8px; cursor: pointer; }
    .fn { font-weight: 600; }
    .pill { font-size: 11px; padding: 2px 6px; border-radius: 999px; background: #eef2ff; color: #3730a3; }
    .metrics { font-size: 12px; color: #374151; display: flex; gap: 10px; flex-wrap: wrap; }
    .kv { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; background: #f3f4f6; padding: 6px; border-radius: 6px; margin: 4px 0; }
    .children { margin-left: 16px; border-left: 2px dashed #e5e7eb; padding-left: 10px; }
    .muted { color: #6b7280; font-size: 12px; }
    .toolbar { display: flex; align-items: center; gap: 8px; }
    .btn { background: #111827; color: white; border: none; padding: 8px 10px; border-radius: 6px; cursor: pointer; }
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
      <input id="search" placeholder="Filter by function or error..." />
      <button class="btn" id="refresh">Refresh</button>
    </div>
    <div class="meta" id="meta"></div>
  </header>
  <main>
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

  let tree = [];
  let total = 0;
  let metrics = [];

  async function fetchTree(){
    const res = await fetch('/api/tree');
    const data = await res.json();
    tree = data.roots || [];
    total = data.total_nodes || 0;
    metrics = data.metrics || [];
    metaEl.textContent = `${new Date(data.generated_at*1000).toLocaleString()} • ${data.log_file} • ${total} nodes`;
    render();
  }

  function fmt(n){ return n==null ? '-' : (typeof n==='number' ? n.toFixed(6) : String(n)); }

  function matchFilter(node, q){
    const hay = [node.function||'', node.error||''].join(' ').toLowerCase();
    return hay.includes(q);
  }

  function renderNode(node, q){
    const visible = !q || matchFilter(node, q) || (node.children||[]).some(c=>matchFilter(c,q));
    if(!visible) return '';

    const metrics = [
      `time: ${fmt(node.duration)}s`,
      `cpu: ${fmt(node.cpu_time)}s`,
      `memΔ: ${node.mem_delta_kb==null?'-':node.mem_delta_kb+' KB'}`,
      `peak: ${node.mem_peak_kb==null?'-':node.mem_peak_kb+' KB'}`
    ].join(' • ');

    const args = node.args_preview!=null ? JSON.stringify(node.args_preview) : '-';
    const kwargs = node.kwargs_preview!=null ? JSON.stringify(node.kwargs_preview) : '-';
    const result = node.result_preview!=null ? JSON.stringify(node.result_preview) : '-';
    const hasErr = !!node.error;

    return `
      <div class="node ${hasErr?'error':''}">
        <div class="title" onclick="this.nextElementSibling.classList.toggle('hidden')">
          <span class="pill">${node.fn_type||''}</span>
          <span class="fn">${node.function||node.call_id}</span>
          <span class="metrics">${metrics}</span>
          ${hasErr?`<span class="pill" style="background:#fee2e2;color:#991b1b">error</span>`:''}
        </div>
        <div class="details">
          <div class="kv"><strong>call_id:</strong> ${node.call_id} ${node.parent_id?`<span class="muted">parent:</span> ${node.parent_id}`:''}</div>
          <div class="kv"><strong>args:</strong> ${args}</div>
          <div class="kv"><strong>kwargs:</strong> ${kwargs}</div>
          <div class="kv"><strong>result:</strong> ${result}</div>
          ${hasErr?`<div class="kv"><strong>error:</strong> ${node.error}</div>`:''}
          ${node.children && node.children.length ? `<div class="children">${node.children.map(n=>renderNode(n,q)).join('')}</div>` : ''}
        </div>
      </div>
    `;
  }

  function render(){
    const q = (searchEl.value||'').toLowerCase().trim();
    const html = tree.map(n=>renderNode(n, q)).join('');
    const metricsHtml = metrics.length ? `
      <div style="margin:12px 0;padding:10px;border:1px solid #e5e7eb;border-radius:8px;background:#f9fafb;">
        <div style="font-weight:600;margin-bottom:8px;">Performance Metrics</div>
        ${metrics.map(m=>`
          <div class="kv">
            <div><strong>Status:</strong> ${m.status||'-'} | <strong>Timestamp:</strong> ${m.timestamp||'-'}</div>
            <div><strong>Total functions:</strong> ${m.total_functions||0} • <strong>Total calls:</strong> ${m.total_calls||0}</div>
            <div style="margin-top:4px;">
              ${(m.metrics||[]).map(row=>`
                <div>- ${row.function}: ${row.calls} calls • total ${row.total_seconds}s • avg ${row.avg_seconds}s</div>
              `).join('') || '<div>- no data</div>'}
            </div>
          </div>
        `).join('')}
      </div>
    ` : '';
    rootEl.innerHTML = html || '<div class="muted">No trace nodes found. Ensure EZTRACE_LOG_FORMAT=json.</div>';
    if(metricsHtml) rootEl.innerHTML = metricsHtml + rootEl.innerHTML;
  }

  searchEl.addEventListener('input', render);
  refreshBtn.addEventListener('click', fetchTree);

  fetchTree();
  setInterval(fetchTree, 2500);
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


import json
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from pathlib import Path
from typing import Dict, Any, List, Optional
import time


class _TraceTreeBuilder:
    def __init__(self, log_file: Path) -> None:
        # Normalize to an absolute, user-expanded path so `~` and relative paths work
        # even when the viewer is started from a different working directory.
        try:
            self.log_file = log_file.expanduser().resolve(strict=False)
        except Exception:
            self.log_file = Path(str(log_file)).expanduser()

    def _metrics_file(self) -> Path:
        return Path(str(self.log_file) + ".metrics")

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

    def _read_metrics_sidecar(self) -> List[Dict[str, Any]]:
        metrics_file = self._metrics_file()
        if not metrics_file.exists():
            return []
        try:
            lines = metrics_file.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            return []

        metrics_entries: List[Dict[str, Any]] = []
        for line in lines:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if isinstance(obj, dict) and obj.get("event") == "metrics_summary":
                    metrics_entries.append(obj)
            except Exception:
                continue
        return metrics_entries

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
        metrics_entries_from_log: List[Dict[str, Any]] = []
        roots: List[str] = []

        def ensure_node(cid: str, parent_id: Optional[str] = None) -> Dict[str, Any]:
            if cid not in nodes:
                nodes[cid] = {
                    'call_id': cid,
                    'parent_id': parent_id,
                    'function': None,
                    'fn_type': None,
                    'start_time': None,
                    'end_time': None,
                    'duration': None,
                    'cpu_time': None,
                    'mem_peak_kb': None,
                    'mem_rss_kb': None,
                    'mem_delta_kb': None,
                    'mem_mode': None,
                    'args_preview': None,
                    'kwargs_preview': None,
                    'result_preview': None,
                    'status': None,
                    'level': None,
                    'project': None,
                    'children': []
                }
            node = nodes[cid]
            if parent_id and node.get('parent_id') is None:
                node['parent_id'] = parent_id
            return node

        for e in entries:
            data = e.get('data') or {}
            call_id = data.get('call_id')
            parent_id = data.get('parent_id')
            event = data.get('event')  # 'start' | 'end' | 'error' | None
            function = e.get('function') or data.get('function')
            fn_type = e.get('fn_type') or data.get('fn_type')
            status = data.get('status')

            if event == 'metrics_summary':
                metrics_entries_from_log.append({
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

            node = ensure_node(call_id, parent_id)
            node.update({
                'function': node.get('function') or function,
                'fn_type': node.get('fn_type') or fn_type,
                'status': status if status is not None else node.get('status'),
                'level': node.get('level') or e.get('level'),
                'project': node.get('project') or e.get('project'),
            })

            if parent_id:
                parent = ensure_node(parent_id)
                if call_id not in parent['children']:
                    parent['children'].append(call_id)

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
                node['mem_rss_kb'] = data.get('mem_rss_kb') or data.get('mem_peak_kb')
                node['mem_peak_kb'] = data.get('mem_peak_kb')
                node['mem_delta_kb'] = data.get('mem_delta_kb')
                node['mem_mode'] = data.get('mem_mode') or node.get('mem_mode')
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

        sidecar_metrics = self._read_metrics_sidecar()
        metrics_entries: List[Dict[str, Any]] = []
        if sidecar_metrics:
            # Prefer sidecar snapshots; they are derived UI caches and avoid polluting trace logs.
            metrics_entries = sidecar_metrics
        else:
            metrics_entries = metrics_entries_from_log

        return {
            'generated_at': time.time(),
            'log_file': str(self.log_file),
            'roots': tree,
            'total_nodes': len(nodes),
            'metrics': metrics_entries
        }


class TraceViewerServer:
    def __init__(self, log_file: Path, host: str = '127.0.0.1', port: int = 8765) -> None:
        try:
            self.log_file = log_file.expanduser().resolve(strict=False)
        except Exception:
            self.log_file = Path(str(log_file)).expanduser()
        self.host = host
        self.port = port
        self._builder = _TraceTreeBuilder(self.log_file)
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
    header { position: sticky; top: 0; z-index: 4; background: linear-gradient(180deg, rgba(15,23,42,0.96), rgba(15,23,42,0.9)); color: var(--text); padding: 10px 16px 12px; border-bottom: 1px solid var(--border); backdrop-filter: blur(10px); }
    header input { padding: 9px 11px; width: 100%; border-radius: 8px; border: 1px solid var(--border); background: var(--surface-soft); color: var(--text); outline: none; transition: border-color 120ms ease; min-width: 0; }
    header input:focus { border-color: var(--accent); box-shadow: 0 0 0 2px rgba(56,189,248,0.25); }
    header .meta { font-size: 12px; color: var(--muted); display: flex; gap: 8px; align-items: center; justify-content: flex-end; min-width: 0; text-align: right; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .header-top { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 4px; }
    .header-right { display: flex; align-items: center; gap: 10px; min-width: 0; }
    .title-wrap { display: flex; flex-direction: column; min-width: 0; }
    .title-wrap strong { font-size: 16px; letter-spacing: -0.01em; }
    .title-sub { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; }
    main { padding: 16px; max-width: 1400px; margin: 0 auto; }
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
    .toolbar { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .btn { background: var(--surface-soft); color: var(--text); border: 1px solid var(--border); padding: 9px 11px; border-radius: 8px; cursor: pointer; transition: transform 120ms ease, border-color 120ms ease; }
    .btn.primary { background: linear-gradient(135deg, #38bdf8, #0ea5e9); color: #0b1220; border: none; font-weight: 700; }
    .btn.small { padding: 6px 9px; font-size: 11px; }
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
    .select { height: 34px; padding: 6px 10px; border-radius: 8px; border: 1px solid var(--border); background: var(--surface-soft); color: var(--text); font-size: 12px; }
    .input-sm { width: 110px; height: 34px; padding: 6px 10px; border-radius: 8px; border: 1px solid var(--border); background: var(--surface-soft); color: var(--text); font-size: 12px; }
    .filter-label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; }
    .toggle input { accent-color: var(--accent); }
    .pill.success { background: rgba(34,197,94,0.15); color: #86efac; border-color: rgba(34,197,94,0.4); }
    .flame-card { border: 1px solid var(--border); background: var(--surface); border-radius: 10px; padding: 12px; box-shadow: 0 10px 30px rgba(0,0,0,0.25); }
    .flame-legend { display: flex; gap: 14px; flex-wrap: wrap; margin-bottom: 10px; font-size: 12px; color: var(--muted); }
    .flame-container { position: relative; width: 100%; min-height: 120px; background: var(--surface-soft); border: 1px solid var(--border); border-radius: 10px; overflow: hidden; }
    .flame-depth-line { position: absolute; left: 0; right: 0; border-top: 1px dashed rgba(156,163,175,0.25); pointer-events: none; }
    .flame-depth-label { position: absolute; left: 6px; font-size: 10px; color: var(--muted); background: rgba(17,24,39,0.8); padding: 1px 4px; border-radius: 4px; pointer-events: none; }
    .flame-bar { position: absolute; height: 22px; border-radius: 6px; padding: 2px 6px; font-size: 11px; line-height: 18px; color: #0b1220; background: linear-gradient(135deg, rgba(56,189,248,0.9), rgba(14,165,233,0.9)); box-shadow: 0 2px 6px rgba(0,0,0,0.2); overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }
    .flame-bar.error { background: linear-gradient(135deg, rgba(239,68,68,0.9), rgba(244,114,182,0.9)); }
    .flame-scale { display: flex; justify-content: space-between; color: var(--muted); font-size: 11px; margin-top: 8px; }
    .issue-table { width: 100%; border-collapse: collapse; }
    .issue-table th, .issue-table td { padding: 10px 12px; border-bottom: 1px solid var(--border); font-size: 12px; text-align: left; }
    .issue-table th { text-transform: uppercase; letter-spacing: 0.06em; color: var(--muted); }
    .split-layout { display: grid; grid-template-columns: minmax(260px, 0.9fr) minmax(320px, 1.1fr) minmax(320px, 1.2fr); gap: 14px; align-items: stretch; }
    .panel { border: 1px solid var(--border); background: var(--surface); border-radius: 12px; padding: 12px; box-shadow: 0 12px 30px rgba(0,0,0,0.25); display: flex; flex-direction: column; min-height: 0; height: clamp(380px, 60vh, 620px); overflow: hidden; }
    .trace-tree { max-height: none; overflow: auto; padding-right: 4px; min-height: 0; flex: 1; }
    .trace-row { padding: 8px 10px; border-radius: 8px; border: 1px solid transparent; display: flex; align-items: center; gap: 8px; cursor: pointer; }
    .trace-row:hover { background: var(--surface-soft); border-color: var(--border); }
    .trace-row.selected { border-color: rgba(56,189,248,0.6); box-shadow: 0 0 0 2px rgba(56,189,248,0.2); }
    .trace-row.error { border-color: rgba(239,68,68,0.6); background: rgba(239,68,68,0.08); }
    .trace-indent { width: 14px; height: 1px; }
    .trace-depth { min-width: 34px; text-align: center; font-size: 10px; color: var(--muted); border: 1px solid var(--border); border-radius: 999px; padding: 2px 6px; }
    .trace-main { display: flex; align-items: center; gap: 8px; min-width: 0; flex: 1; }
    .trace-fn { white-space: nowrap; text-overflow: ellipsis; overflow: hidden; font-weight: 600; }
    .trace-id { font-size: 11px; color: var(--muted); font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
    .trace-meta { font-size: 11px; color: var(--muted); }
    .run-list { display: flex; flex-direction: column; gap: 6px; min-height: 0; flex: 1; margin-bottom: 0; }
    .run-toolbar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 8px; }
    .run-search { min-width: 0; flex: 1; min-height: 34px; padding: 8px 10px; border: 1px solid var(--border); border-radius: 8px; background: var(--surface-soft); color: var(--text); }
    .virtual-viewport { position: relative; overflow: auto; max-height: none; min-height: 0; flex: 1; border: 1px solid var(--border); border-radius: 8px; background: rgba(17,24,39,0.45); }
    .virtual-spacer { width: 1px; opacity: 0; }
    .virtual-layer { position: absolute; left: 0; right: 0; top: 0; }
    .run-group { height: 34px; display: flex; align-items: center; padding: 0 10px; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; border-bottom: 1px dashed var(--border); }
    .run-item { padding: 8px 10px; border-radius: 8px; border: 1px solid var(--border); background: var(--surface-soft); cursor: pointer; font-size: 12px; display: flex; gap: 8px; align-items: center; }
    .run-item.compact { min-height: 34px; margin: 2px 6px; }
    .run-item.comfy { min-height: 50px; margin: 4px 6px; }
    .run-item.active { border-color: rgba(56,189,248,0.6); box-shadow: 0 0 0 2px rgba(56,189,248,0.2); color: var(--text); }
    .run-item .pill { font-size: 10px; padding: 2px 6px; }
    .selection-strip { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 8px; border: 1px solid var(--border); border-radius: 8px; padding: 8px; background: rgba(17,24,39,0.4); }
    .selection-strip .btn { padding: 6px 8px; font-size: 11px; }
    .selection-path { font-size: 12px; color: var(--muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 100%; }
    .trace-controls { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 8px; }
    .tab-row { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 8px; align-items: center; }
    .tab-spacer { flex: 1; }
    .tab-secondary { display: flex; gap: 8px; align-items: center; margin-left: auto; }
    .tab-btn { border: 1px solid var(--border); background: var(--surface-soft); color: var(--muted); border-radius: 999px; padding: 6px 12px; font-size: 12px; cursor: pointer; }
    .tab-btn.active { color: var(--text); border-color: var(--accent); box-shadow: 0 0 0 2px rgba(56,189,248,0.2); }
    .insight-panel { border: 1px solid var(--border); border-radius: 10px; background: var(--surface); padding: 12px; margin-bottom: 14px; }
    .trace-settings { position: sticky; top: 80px; z-index: 3; margin-bottom: 25px; }
    .trace-settings .trace-controls { margin-bottom: 0; }
    .insight-panel.traces-panel { height: clamp(320px, 56vh, 620px); max-height: 62vh; min-height: 320px; display: flex; flex-direction: column; min-height: 0; overflow: hidden; }
    .flame-scroll { min-height: 0; height: 100%; flex: 1; overflow: auto; overscroll-behavior: contain; padding-right: 4px; }
    .insight-panel.metrics-panel { height: clamp(320px, 56vh, 620px); max-height: 62vh; min-height: 320px; display: flex; flex-direction: column; min-height: 0; overflow: hidden; }
    .metrics-scroll { min-height: 0; height: 100%; flex: 1; overflow: auto; overscroll-behavior: contain; padding-right: 4px; }
    .panel-title { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 8px; }
    .hidden-panel { display: none; }
    .overview-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin-bottom: 12px; }
    .overview-card { border: 1px solid var(--border); border-radius: 8px; background: var(--surface-soft); padding: 10px; }
    .overview-label { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 4px; }
    .overview-value { font-size: 20px; font-weight: 700; letter-spacing: -0.02em; }
    .overview-sub { color: var(--muted); font-size: 12px; margin-top: 2px; }
    .overview-columns { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; align-items: start; }
    .overview-block { border: 1px solid var(--border); border-radius: 8px; background: var(--surface-soft); padding: 10px; min-height: 0; width: 100%; max-width: 100%; overflow: hidden; height: 404px; min-height: 404px; }
    .overview-block .metrics-table { margin-top: 0; }
    .overview-scroll { max-height: 330px; min-height: 330px; overflow: auto; padding-right: 4px; }
    .overview-trend { font-size: 11px; margin-top: 4px; }
    .overview-trend.up { color: #86efac; }
    .overview-trend.down { color: #fca5a5; }
    .overview-trend.flat { color: var(--muted); }
    .info-wrap { position: relative; display: inline-flex; align-items: center; margin-left: 6px; vertical-align: middle; }
    .info-icon { width: 15px; height: 15px; border-radius: 999px; border: 1px solid var(--border); color: var(--muted); display: inline-flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 700; cursor: help; background: rgba(17,24,39,0.7); text-transform: none; }
    .tooltip { position: absolute; left: 50%; transform: translateX(-50%); bottom: calc(100% + 8px); min-width: 220px; max-width: 360px; background: #0b1220; border: 1px solid var(--border); border-radius: 8px; color: var(--text); font-size: 11px; line-height: 1.35; padding: 8px 10px; box-shadow: 0 10px 30px rgba(0,0,0,0.35); opacity: 0; pointer-events: none; transition: opacity 120ms ease; text-transform: none; letter-spacing: normal; z-index: 50; }
    .info-wrap:hover .tooltip, .info-wrap:focus-within .tooltip { opacity: 1; }
    .detail-block { margin-bottom: 10px; }
    .detail-title { font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); margin-bottom: 6px; }
    .detail-title.error { color: #fca5a5; }
    .detail-error { border: 1px solid rgba(239,68,68,0.55); background: rgba(239,68,68,0.12); border-radius: 8px; padding: 10px; margin-top: 8px; }
    .detail-error .detail-error-title { display: flex; align-items: center; gap: 6px; color: #fecaca; font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 6px; font-weight: 700; }
    .kv.error-kv { border-color: rgba(239,68,68,0.6); background: rgba(239,68,68,0.14); color: #ffe4e6; white-space: pre-wrap; word-break: break-word; }
    #trace-details { min-height: 0; flex: 1; overflow: auto; padding-right: 4px; }
    @media (max-width: 1080px) {
      header .meta { justify-content: flex-start; text-align: left; }
      .split-layout { grid-template-columns: 1fr; }
      .overview-columns { grid-template-columns: 1fr; }
      .header-right { width: 100%; justify-content: space-between; }
    }
  </style>
  <script defer src="/app.js"></script>
  <script>
    window.__PYEZTRACE_VIEWER_CONFIG__ = {};
  </script>
</head>
<body>
  <header>
    <div class="header-top">
      <div class="title-wrap">
        <strong>PyEzTrace Viewer</strong>
        <span class="title-sub">Trace Explorer</span>
      </div>
      <div class="header-right">
        <div class="meta" id="meta"></div>
        <button class="btn primary" id="refresh">Refresh</button>
      </div>
    </div>
  </header>
  <main>
    <div class="grid hidden-panel" id="overview"></div>
    <div id="trace-settings" class="insight-panel trace-settings">
      <div class="panel-title">Trace filters</div>
      <div class="trace-controls">
        <input id="search" class="run-search" placeholder="Search functions, errors, IDs..." />
        <div class="chip-group" id="status-filter">
          <button class="chip active" data-filter="all">All</button>
          <button class="chip" data-filter="errors">Errors only</button>
          <button class="chip" data-filter="completed">Completed</button>
        </div>
        <label class="filter-label">Min duration</label>
        <input id="min-duration" class="input-sm" type="number" min="0" step="1" placeholder="ms" />
        <label class="filter-label">Type</label>
        <select id="fn-type" class="select">
          <option value="all">All types</option>
        </select>
        <label class="filter-label">Sort</label>
        <select id="sort-mode" class="select">
          <option value="start">Start time</option>
          <option value="duration">Duration</option>
          <option value="cpu">CPU</option>
          <option value="mem">Memory</option>
          <option value="name">Name</option>
        </select>
        <label class="toggle"><input type="checkbox" id="toggle-payloads" checked /> Payloads</label>
      </div>
    </div>
    <div id="root"></div>
    <div class="split-layout" id="split-layout">
      <div class="panel">
        <div class="section-title"><span>Trace runs</span></div>
        <div class="run-toolbar">
          <input id="run-search" class="run-search" placeholder="Search runs..." />
          <select id="run-group" class="select">
            <option value="none">No grouping</option>
            <option value="function">Group by function</option>
            <option value="status">Group by status</option>
            <option value="time">Group by minute</option>
          </select>
          <label class="toggle"><input id="run-compact" type="checkbox" checked /> Compact</label>
        </div>
        <div class="run-list" id="run-list"></div>
      </div>
      <div class="panel">
        <div class="section-title"><span>Trace hierarchy</span></div>
        <div id="selection-strip" class="selection-strip"></div>
        <div id="trace-controls" class="trace-controls">
          <label class="toggle"><input type="checkbox" id="auto-refresh" checked /> Auto refresh</label>
          <label class="filter-label">Focus</label>
          <select id="focus-mode" class="select">
            <option value="all">All</option>
            <option value="errors">Errors</option>
            <option value="slow">Slow calls</option>
            <option value="path">Selected path</option>
          </select>
          <label class="filter-label">Depth</label>
          <input id="depth-limit" class="input-sm" type="number" min="0" step="1" value="99" />
          <button class="btn small" id="expand-depth">+Depth</button>
          <button class="btn small" id="collapse-all">Collapse</button>
          <button class="btn small" id="copy-filtered">Copy filtered</button>
        </div>
        <div class="trace-tree" id="trace-tree"></div>
      </div>
      <div class="panel">
        <div class="section-title"><span>Trace details</span></div>
        <div id="trace-details"></div>
      </div>
    </div>
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
  const traceSettingsEl = document.getElementById('trace-settings');
  const searchEl = document.getElementById('search');
  const metaEl = document.getElementById('meta');
  const refreshBtn = document.getElementById('refresh');
  const overviewEl = document.getElementById('overview');
  const splitLayoutEl = document.getElementById('split-layout');
  const runListEl = document.getElementById('run-list');
  const runSearchEl = document.getElementById('run-search');
  const runGroupEl = document.getElementById('run-group');
  const runCompactEl = document.getElementById('run-compact');
  const traceTreeEl = document.getElementById('trace-tree');
  const traceDetailsEl = document.getElementById('trace-details');
  const selectionStripEl = document.getElementById('selection-strip');
  const minDurationEl = document.getElementById('min-duration');
  const fnTypeEl = document.getElementById('fn-type');
  const sortModeEl = document.getElementById('sort-mode');
  const togglePayloadsEl = document.getElementById('toggle-payloads');
  const statusFilterGroup = document.getElementById('status-filter');
  const autoRefreshEl = document.getElementById('auto-refresh');
  const focusModeEl = document.getElementById('focus-mode');
  const depthLimitEl = document.getElementById('depth-limit');
  const expandDepthEl = document.getElementById('expand-depth');
  const collapseAllEl = document.getElementById('collapse-all');
  const copyFilteredEl = document.getElementById('copy-filtered');

  let tree = [];
  let total = 0;
  let metrics = [];
  let generatedAt = null;
  let statusFilter = 'all';
  let minDurationMs = 0;
  let fnTypeFilter = 'all';
  let sortMode = 'start';
  let showPayloads = true;
  let metricsTab = 'latest';
  let insightTab = 'overview';
  let refreshTimer = null;
  let autoRefreshEnabled = true;
  let selectedRunId = null;
  let selectedCallId = null;
  let runQuery = '';
  let runGroupBy = 'none';
  let runCompact = true;
  let focusMode = 'all';
  let depthLimit = 99;
  let slowThresholdMs = 10;
  let visibleTraceNodes = [];
  let traceMap = new Map();
  let runScrollTop = 0;
  let selectionHistory = [];
  let historyIndex = -1;

  const STATE_KEY = 'pyeztrace_viewer_ui_v1';

  function saveState(){
    try {
      localStorage.setItem(STATE_KEY, JSON.stringify({
        statusFilter, minDurationMs, fnTypeFilter, sortMode, showPayloads,
        metricsTab, insightTab, autoRefreshEnabled, runQuery, runGroupBy,
        runCompact, focusMode, depthLimit, selectedRunId
      }));
    } catch (_e) {}
  }

  function loadState(){
    try {
      const raw = localStorage.getItem(STATE_KEY);
      if(!raw) return;
      const s = JSON.parse(raw);
      statusFilter = s.statusFilter || statusFilter;
      minDurationMs = Number(s.minDurationMs || minDurationMs);
      fnTypeFilter = s.fnTypeFilter || fnTypeFilter;
      sortMode = s.sortMode || sortMode;
      showPayloads = s.showPayloads !== false;
      metricsTab = s.metricsTab || metricsTab;
      insightTab = s.insightTab || 'overview';
      autoRefreshEnabled = s.autoRefreshEnabled !== false;
      runQuery = s.runQuery || '';
      runGroupBy = s.runGroupBy || runGroupBy;
      runCompact = s.runCompact !== false;
      focusMode = s.focusMode || focusMode;
      depthLimit = Number(s.depthLimit || depthLimit);
      selectedRunId = s.selectedRunId || null;
    } catch (_e) {}
  }

  function syncControlState(){
    minDurationEl.value = minDurationMs || '';
    if([...fnTypeEl.options].some(o=>o.value === fnTypeFilter)){
      fnTypeEl.value = fnTypeFilter;
    } else {
      fnTypeFilter = 'all';
      fnTypeEl.value = 'all';
    }
    sortModeEl.value = sortMode;
    togglePayloadsEl.checked = showPayloads;
    runSearchEl.value = runQuery;
    runGroupEl.value = runGroupBy;
    runCompactEl.checked = runCompact;
    autoRefreshEl.checked = autoRefreshEnabled;
    focusModeEl.value = focusMode;
    depthLimitEl.value = depthLimit;
    [...statusFilterGroup.querySelectorAll('.chip')].forEach(btn=>{
      btn.classList.toggle('active', btn.dataset.filter === statusFilter);
    });
  }

  function fmt(n){ return n==null ? '-' : (typeof n==='number' ? n.toFixed(6) : String(n)); }
  function cleanFnName(name){
    if(!name) return '-';
    return String(name).replace(/\.<locals>\./g, '.').replace(/<locals>/g, '');
  }
  function fmtDuration(sec){
    if(sec==null) return '-';
    if(sec >= 1) return `${sec.toFixed(3)}s`;
    return `${(sec*1000).toFixed(1)}ms`;
  }
  function fmtTime(epoch){
    if(!epoch) return '-';
    const d = new Date(epoch*1000);
    return `${d.toLocaleTimeString()} (${d.toLocaleDateString()})`;
  }
  function escapeHtml(value){
    return String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }
  function escapeAttr(value){
    return String(value).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }
  function infoTip(text){
    return `<span class="info-wrap"><span class="info-icon" tabindex="0" aria-label="Metric info">i</span><span class="tooltip">${escapeHtml(text)}</span></span>`;
  }
  function safeEnd(node){
    if(node.end_time) return node.end_time;
    if(node.start_time && node.duration != null) return node.start_time + node.duration;
    return node.start_time || null;
  }

  function flattenNodes(nodes, depth=0, parentId=null, acc=[]){
    nodes.forEach(n=>{
      acc.push({ ...n, depth, parent_id: parentId });
      if(n.children && n.children.length) flattenNodes(n.children, depth+1, n.call_id, acc);
    });
    return acc;
  }

  function getRunNode(runId){
    return tree.find(n=>n.call_id === runId) || null;
  }

  function currentTree(){
    if(selectedRunId){
      const match = getRunNode(selectedRunId);
      return match ? [match] : tree;
    }
    return tree;
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
  function passesExtra(node){
    if(fnTypeFilter !== 'all' && node.fn_type !== fnTypeFilter) return false;
    if(minDurationMs > 0){
      if(node.duration == null) return false;
      if(node.duration * 1000 < minDurationMs) return false;
    }
    return true;
  }
  function matchesNode(node, q){
    return matchFilter(node, q) && passesStatus(node) && passesExtra(node);
  }
  function shouldDisplay(node, q){
    const selfMatch = matchesNode(node, q);
    const childMatch = (node.children||[]).some(c=>shouldDisplay(c, q));
    return selfMatch || childMatch;
  }

  function summarizeNodes(nodes, q){
    let count = 0;
    let totalMs = 0;
    let maxMs = 0;
    let errors = 0;
    const visit = (n)=>{
      if(matchesNode(n, q)){
        count++;
        if(n.duration != null){
          const ms = n.duration * 1000;
          totalMs += ms;
          maxMs = Math.max(maxMs, ms);
        }
        if(n.error || n.status === 'error') errors++;
      }
      (n.children||[]).forEach(visit);
    };
    nodes.forEach(visit);
    const avgMs = count > 0 ? (totalMs / count) : 0;
    return { count, totalMs, maxMs, avgMs, errors };
  }

  function renderFnTypeOptions(){
    const types = Array.from(
      new Set(
        flattenNodes(tree)
          .map(n=>n.fn_type)
          .filter(Boolean)
      )
    ).sort();
    fnTypeEl.innerHTML =
      `<option value="all">All types</option>` +
      types.map(t=>`<option value="${escapeAttr(t)}">${escapeHtml(t)}</option>`).join('');
    if(fnTypeFilter !== 'all' && !types.includes(fnTypeFilter)){
      fnTypeFilter = 'all';
    }
    fnTypeEl.value = fnTypeFilter;
  }

  function normalizeMetricsList(mList){
    const out = [];
    (mList||[]).forEach(row=>{
      if(!row) return;
      out.push({
        function: row.function || '-',
        calls: row.calls || 0,
        total_seconds: row.total_seconds || 0,
        avg_seconds: row.avg_seconds || 0
      });
    });
    return out;
  }

  function buildDeltaSeries(snaps){
    const series = new Map();
    if(!snaps || snaps.length < 2) return series;
    const toMap = (snap)=>{
      const map = new Map();
      normalizeMetricsList(snap.metrics).forEach(r=> map.set(r.function, r));
      return map;
    };
    for(let i=1;i<snaps.length;i++){
      const prev = toMap(snaps[i-1]);
      const cur = toMap(snaps[i]);
      const fns = new Set([...prev.keys(), ...cur.keys()]);
      fns.forEach(fn=>{
        const p = prev.get(fn) || {calls:0,total_seconds:0};
        const c = cur.get(fn) || {calls:0,total_seconds:0};
        const dcalls = (c.calls||0) - (p.calls||0);
        const dtotal = (c.total_seconds||0) - (p.total_seconds||0);
        if(dcalls <= 0 || dtotal < 0) return;
        if(!series.has(fn)) series.set(fn, { fn, deltas: [] });
        series.get(fn).deltas.push(dtotal);
      });
    }
    return series;
  }

  function sparkline(values){
    const vals = (values||[]).map(v=>Number(v)||0);
    if(!vals.length) return '<span class="muted">-</span>';
    const max = Math.max(...vals, 1e-9);
    const bars = vals.map(v=>{
      const h = Math.max(2, Math.round((v / max) * 18));
      return `<span style="display:inline-block;width:4px;height:${h}px;background:rgba(56,189,248,0.65);border-radius:2px;"></span>`;
    }).join('');
    return `<span style="display:inline-flex;align-items:flex-end;gap:2px;height:20px;">${bars}</span>`;
  }

  function buildMetricsPanel(){
    const latestMetrics = metrics && metrics.length ? metrics[metrics.length - 1] : null;
    if(!latestMetrics){
      return `<div class="insight-panel metrics-panel"><div class="panel-title">Performance metrics</div><div class="metrics-scroll"><div class="muted">No metrics snapshots available.</div></div></div>`;
    }
    if(metricsTab === 'latest'){
      return `
        <div class="insight-panel metrics-panel">
          <div class="panel-title">Performance metrics (latest)</div>
          <div class="metrics-scroll">
            <div class="metrics-summary">
              <div class="metrics-summary-item"><strong>Functions:</strong>${latestMetrics.total_functions||0}</div>
              <div class="metrics-summary-item"><strong>Total calls:</strong>${latestMetrics.total_calls||0}</div>
              <div class="metrics-summary-item"><strong>Generated:</strong>${latestMetrics.generated_at ? new Date(latestMetrics.generated_at*1000).toLocaleTimeString() : '-'}</div>
            </div>
            <table class="metrics-table">
              <thead><tr><th>Function</th><th class="number">Calls</th><th class="number">Total</th><th class="number">Avg</th></tr></thead>
              <tbody>
                ${normalizeMetricsList(latestMetrics.metrics).slice(0,40).map(row=>`
                  <tr>
                    <td class="function-name">${row.function}</td>
                    <td class="number">${row.calls}</td>
                    <td class="number">${row.total_seconds.toFixed(6)}s</td>
                    <td class="number">${row.avg_seconds.toFixed(6)}s</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        </div>
      `;
    }
    const series = buildDeltaSeries(metrics);
    const latestList = normalizeMetricsList(latestMetrics.metrics).sort((a,b)=> (b.total_seconds||0)-(a.total_seconds||0)).slice(0,20);
    return `
      <div class="insight-panel metrics-panel">
        <div class="panel-title">Performance metrics (time series)</div>
        <div class="metrics-scroll">
          <table class="metrics-table">
            <thead><tr><th>Function</th><th class="number">Total</th><th class="number">Calls</th><th>Trend</th></tr></thead>
            <tbody>
              ${latestList.map(r=>`
                <tr>
                  <td class="function-name">${r.function}</td>
                  <td class="number">${r.total_seconds.toFixed(6)}s</td>
                  <td class="number">${r.calls}</td>
                  <td>${sparkline((series.get(r.function)||{}).deltas||[])}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;
  }

  function buildFlameGraph(nodes, q){
    const filtered = flattenNodes(nodes).filter(n=>matchesNode(n, q));
    if(filtered.length === 0){
      return `<div class="insight-panel"><div class="panel-title">Flame graph</div><div class="muted">No trace data for current filters.</div></div>`;
    }
    const starts = filtered.map(n=>n.start_time).filter(Boolean);
    const ends = filtered.map(n=>safeEnd(n)).filter(Boolean);
    const minStart = Math.min(...starts);
    const maxEnd = Math.max(...ends);
    const span = Math.max(maxEnd - minStart, 0.000001);
    const maxDepth = Math.max(...filtered.map(n=>n.depth || 0), 0);
    const rowHeight = 26;
    const height = (maxDepth + 1) * rowHeight + 8;
    const depthGuides = Array.from({length: maxDepth + 1}, (_v, d)=>{
      const y = d * rowHeight + 16;
      return `<div class="flame-depth-line" style="top:${y}px;"></div><div class="flame-depth-label" style="top:${y-9}px;">d${d}</div>`;
    }).join('');
    const bars = filtered.map(n=>{
      const start = n.start_time || minStart;
      const end = safeEnd(n) || start;
      const left = ((start - minStart) / span) * 100;
      const width = Math.max(((end - start) / span) * 100, 0.5);
      const top = (n.depth || 0) * rowHeight + 6;
      const label = `${n.function || n.call_id} (${fmtDuration(n.duration)})`;
      const isError = n.error || n.status === 'error';
      const text = width > 9 ? cleanFnName(n.function || n.call_id) : '';
      return `<div class="flame-bar ${isError ? 'error' : ''}" style="left:${left}%;width:${width}%;top:${top}px;" title="${label}">${text}</div>`;
    }).join('');
    return `
      <div class="insight-panel traces-panel">
        <div class="panel-title">Flame graph</div>
        <div class="flame-scroll">
          <div class="flame-legend">
            <span><strong>X axis:</strong> wall-clock timeline</span>
            <span><strong>Bar width:</strong> call duration</span>
            <span><strong>Y axis:</strong> call depth (d0 root)</span>
          </div>
          <div class="flame-container" style="height:${height}px;">${depthGuides}${bars}</div>
          <div class="flame-scale">
            <span>${new Date(minStart*1000).toLocaleTimeString()}</span>
            <span>${span.toFixed(3)}s span</span>
            <span>${new Date(maxEnd*1000).toLocaleTimeString()}</span>
          </div>
        </div>
      </div>
    `;
  }

  function buildIssuesPanel(nodes, q){
    const issues = flattenNodes(nodes).filter(n=>matchesNode(n, q) && (n.error || n.status === 'error'));
    return `
      <div class="insight-panel">
        <div class="panel-title">Issue debugger (${issues.length})</div>
        ${issues.length ? `
          <table class="issue-table">
            <thead><tr><th>Function</th><th>Error</th><th>Call ID</th><th></th></tr></thead>
            <tbody>
              ${issues.slice(0,60).map(n=>`
                <tr>
                  <td>${escapeHtml(cleanFnName(n.function || '-'))}</td>
                  <td>${escapeHtml(n.error || '-')}</td>
                  <td class="muted">${n.call_id || '-'}</td>
                  <td><button class="btn small" data-action="copy-text" data-copy="${escapeAttr(encodeURIComponent(n.call_id || ''))}">Copy</button></td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        ` : '<div class="muted">No errors for current filters.</div>'}
      </div>
    `;
  }

  function percentile(values, p){
    if(!values.length) return null;
    const sorted = [...values].sort((a,b)=>a-b);
    const idx = Math.max(0, Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1));
    return sorted[idx];
  }

  function buildOverviewPanel(){
    const allNodes = flattenNodes(tree);
    const durationsMs = allNodes.filter(n=>n.duration != null).map(n=>n.duration * 1000);
    const totalCalls = allNodes.length;
    const totalRuns = tree.length;
    const errorNodes = allNodes.filter(n=>n.error || n.status === 'error');
    const successNodes = allNodes.filter(n=>n.status === 'success');
    const errorRate = totalCalls ? ((errorNodes.length / totalCalls) * 100) : 0;
    const p50 = percentile(durationsMs, 50);
    const p95 = percentile(durationsMs, 95);
    const p99 = percentile(durationsMs, 99);
    const latestMetrics = metrics.length ? metrics[metrics.length - 1] : null;
    const missingEnd = allNodes.filter(n=>n.start_time && !n.end_time).length;

    const fnMap = new Map();
    let cpuTotal = 0;
    let memDeltaNet = 0;
    let memDeltaPositive = 0;
    let memDeltaNegative = 0;
    let memDeltaMax = 0;
    const memModes = new Set();
    allNodes.forEach(n=>{
      const key = cleanFnName(n.function || n.call_id || 'unknown');
      if(!fnMap.has(key)){
        fnMap.set(key, { fn: key, calls: 0, totalMs: 0, errors: 0, maxMs: 0, cpuS: 0, memDeltaKb: 0 });
      }
      const row = fnMap.get(key);
      row.calls += 1;
      if(n.duration != null){
        const ms = n.duration * 1000;
        row.totalMs += ms;
        row.maxMs = Math.max(row.maxMs, ms);
      }
      if(n.cpu_time != null){
        row.cpuS += Number(n.cpu_time) || 0;
        cpuTotal += Number(n.cpu_time) || 0;
      }
      if(n.mem_delta_kb != null){
        const md = Number(n.mem_delta_kb) || 0;
        row.memDeltaKb += md;
        memDeltaNet += md;
        if(md >= 0){
          memDeltaPositive += md;
        } else {
          memDeltaNegative += md;
        }
        memDeltaMax = Math.max(memDeltaMax, md);
      }
      if(n.mem_mode){
        memModes.add(String(n.mem_mode));
      }
      if(n.error || n.status === 'error') row.errors += 1;
    });
    const hotspots = [...fnMap.values()]
      .sort((a,b)=> b.totalMs - a.totalMs)
      .slice(0, 12);
    const cpuHotspots = [...fnMap.values()].sort((a,b)=> b.cpuS - a.cpuS).slice(0, 10);
    const memHotspots = [...fnMap.values()].sort((a,b)=> b.memDeltaKb - a.memDeltaKb).slice(0, 10);

    const errMap = new Map();
    errorNodes.forEach(n=>{
      const sig = String(n.error || 'error').split('\\n')[0].slice(0, 140);
      if(!errMap.has(sig)) errMap.set(sig, { sig, count: 0, fn: cleanFnName(n.function || '-') });
      errMap.get(sig).count += 1;
    });
    const errorSigs = [...errMap.values()].sort((a,b)=> b.count - a.count).slice(0, 12);

    const generated = generatedAt ? new Date(generatedAt*1000).toLocaleString() : '-';
    const metricsGenerated = latestMetrics && latestMetrics.generated_at ? new Date(latestMetrics.generated_at*1000).toLocaleTimeString() : '-';
    const prevMetrics = metrics.length > 1 ? metrics[metrics.length - 2] : null;
    const memModeLabel = memModes.has('peak_rusage') ? 'Peak RSS fallback' : (memModes.has('current_rss') ? 'Current RSS' : 'Unknown');
    const memTipText = memModes.has('peak_rusage')
      ? 'Memory values are using peak RSS fallback on this platform/runtime. Deltas can overstate real-time memory movement and are best used as coarse signals.'
      : 'Memory values are based on current RSS snapshots. Compare SUM+, SUM-, and NET to distinguish bursty churn from sustained growth.';

    const startTimes = allNodes.map(n=>n.start_time).filter(Boolean);
    const endTimes = allNodes.map(n=>safeEnd(n)).filter(Boolean);
    const spanSec = (startTimes.length && endTimes.length) ? Math.max(0, Math.max(...endTimes) - Math.min(...startTimes)) : 0;
    const callsPerMin = spanSec > 0 ? (totalCalls / (spanSec / 60)) : 0;

    const refTs = startTimes.length ? Math.max(...startTimes) : (generatedAt || 0);
    const RECENT_WINDOW = 300; // 5 min
    const recentNodes = allNodes.filter(n => (n.start_time || 0) >= (refTs - RECENT_WINDOW));
    const previousNodes = allNodes.filter(n => (n.start_time || 0) < (refTs - RECENT_WINDOW) && (n.start_time || 0) >= (refTs - RECENT_WINDOW * 2));

    const windowStats = (nodes) => {
      const d = nodes.filter(n=>n.duration != null).map(n=>n.duration * 1000);
      const errs = nodes.filter(n=>n.error || n.status === 'error').length;
      const cpu = nodes.reduce((a,n)=>a + (Number(n.cpu_time)||0), 0);
      return {
        calls: nodes.length,
        errors: errs,
        errorRate: nodes.length ? (errs / nodes.length * 100) : 0,
        avgMs: d.length ? d.reduce((a,b)=>a+b,0) / d.length : 0,
        p95: percentile(d, 95) || 0,
        cpu
      };
    };
    const recent = windowStats(recentNodes);
    const previous = windowStats(previousNodes);

    const trend = (cur, prev, higherIsBetter=true) => {
      if(!prev) return { txt: 'n/a', cls: 'flat' };
      const pct = prev === 0 ? null : ((cur - prev) / prev) * 100;
      if(pct == null || !isFinite(pct)) return { txt: 'n/a', cls: 'flat' };
      const sign = pct > 0 ? '+' : '';
      const good = higherIsBetter ? pct >= 0 : pct <= 0;
      return { txt: `${sign}${pct.toFixed(1)}% vs prev`, cls: good ? 'up' : 'down' };
    };

    const recentSlow = [...recentNodes]
      .filter(n=>n.duration != null)
      .sort((a,b)=> (b.duration||0) - (a.duration||0))
      .slice(0, 10);

    return `
      <div class="insight-panel">
        <div class="panel-title">Overview dashboard</div>
        <div class="overview-grid">
          <div class="overview-card"><div class="overview-label">Last updated ${infoTip('Timestamp of the latest parsed trace data. Use this to confirm the dashboard reflects current logs.')}</div><div class="overview-value">${generated}</div><div class="overview-sub">Live trace snapshot</div></div>
          <div class="overview-card"><div class="overview-label">Trace runs ${infoTip('Number of top-level trace roots. Useful for estimating how many independent workflows were captured.')}</div><div class="overview-value">${totalRuns}</div><div class="overview-sub">Top-level root traces</div></div>
          <div class="overview-card"><div class="overview-label">Total calls ${infoTip('Count of all parsed trace nodes (root + nested). Higher values indicate deeper or busier execution paths.')}</div><div class="overview-value">${totalCalls}</div><div class="overview-sub">All nodes parsed</div></div>
          <div class="overview-card"><div class="overview-label">Success rate ${infoTip('Share of calls with successful completion status. Track this over time for service stability.')}</div><div class="overview-value">${totalCalls ? ((successNodes.length/totalCalls)*100).toFixed(1) : '0.0'}%</div><div class="overview-sub">${successNodes.length} successful calls</div></div>
          <div class="overview-card"><div class="overview-label">Error rate ${infoTip('Share of calls marked as errors. Rising error rate can indicate regressions or environmental failures.')}</div><div class="overview-value" style="color:#fca5a5;">${errorRate.toFixed(1)}%</div><div class="overview-sub">${errorNodes.length} error calls</div></div>
          <div class="overview-card"><div class="overview-label">Latency p95 / p99 ${infoTip('Tail latency percentiles. p95 and p99 are strong indicators of user-facing slowdowns and outliers.')}</div><div class="overview-value">${p95==null?'-':p95.toFixed(1)} / ${p99==null?'-':p99.toFixed(1)} ms</div><div class="overview-sub">p50 ${p50==null?'-':p50.toFixed(1)} ms</div></div>
          <div class="overview-card"><div class="overview-label">Trace health ${infoTip('Calls that started but have no end timestamp. Persistent growth may indicate interrupted execution or incomplete logging.')}</div><div class="overview-value">${missingEnd}</div><div class="overview-sub">Calls missing end timestamp</div></div>
          <div class="overview-card"><div class="overview-label">Calls / min ${infoTip('Throughput estimate over the observed trace span. Useful for capacity monitoring and traffic comparisons.')}</div><div class="overview-value">${callsPerMin ? callsPerMin.toFixed(1) : '-'}</div><div class="overview-sub">Across ${(spanSec/60).toFixed(1)} min window</div></div>
          <div class="overview-card"><div class="overview-label">CPU total ${infoTip('Sum of recorded CPU time across calls. Helpful for spotting compute-intensive workloads.')}</div><div class="overview-value">${cpuTotal.toFixed(3)}s</div><div class="overview-sub">Tracked across all calls</div></div>
          <div class="overview-card"><div class="overview-label">Mem delta SUM+ / SUM- / NET ${infoTip(memTipText)}</div><div class="overview-value">${memDeltaPositive.toFixed(0)} / ${Math.abs(memDeltaNegative).toFixed(0)} / ${memDeltaNet.toFixed(0)} KB</div><div class="overview-sub">MAX +${memDeltaMax.toFixed(0)} KB • ${memModeLabel}</div></div>
          <div class="overview-card"><div class="overview-label">Metrics snapshots ${infoTip('Number of metrics snapshots available. More snapshots improve trend confidence and historical visibility.')}</div><div class="overview-value">${metrics.length}</div><div class="overview-sub">Latest at ${metricsGenerated}</div></div>
          <div class="overview-card">
            <div class="overview-label">Recent 5m calls ${infoTip('Calls started in the most recent 5-minute window relative to the latest trace timestamp in this file. Compare with previous window to gauge momentum.')}</div>
            <div class="overview-value">${recent.calls}</div>
            <div class="overview-sub">Previous window ${previous.calls}</div>
            <div class="overview-trend ${trend(recent.calls, previous.calls, true).cls}">${trend(recent.calls, previous.calls, true).txt}</div>
          </div>
          <div class="overview-card">
            <div class="overview-label">Recent 5m p95 ${infoTip('95th percentile latency in the latest 5-minute window. Lower is generally better for responsiveness.')}</div>
            <div class="overview-value">${recent.p95 ? recent.p95.toFixed(1) : '-'} ms</div>
            <div class="overview-sub">Previous ${previous.p95 ? previous.p95.toFixed(1) : '-'} ms</div>
            <div class="overview-trend ${trend(recent.p95, previous.p95, false).cls}">${trend(recent.p95, previous.p95, false).txt}</div>
          </div>
          <div class="overview-card">
            <div class="overview-label">Recent 5m error rate ${infoTip('Error percentage in the latest 5-minute window. A sustained rise is a key maintenance and reliability signal.')}</div>
            <div class="overview-value" style="color:#fca5a5;">${recent.errorRate.toFixed(1)}%</div>
            <div class="overview-sub">Previous ${previous.errorRate.toFixed(1)}%</div>
            <div class="overview-trend ${trend(recent.errorRate, previous.errorRate, false).cls}">${trend(recent.errorRate, previous.errorRate, false).txt}</div>
          </div>
        </div>
        <div class="overview-columns">
          <div class="overview-block">
            <div class="panel-title">Hotspots by total time</div>
            <div class="overview-scroll">
              <table class="metrics-table">
                <thead><tr><th>Function</th><th class="number">Calls</th><th class="number">Total</th><th class="number">Max</th><th class="number">Errors</th></tr></thead>
                <tbody>
                  ${hotspots.map(r=>`
                    <tr>
                      <td class="function-name">${escapeHtml(r.fn)}</td>
                      <td class="number">${r.calls}</td>
                      <td class="number">${r.totalMs.toFixed(1)}ms</td>
                      <td class="number">${r.maxMs.toFixed(1)}ms</td>
                      <td class="number">${r.errors}</td>
                    </tr>
                  `).join('') || `<tr><td class="muted" colspan="5">No hotspot data</td></tr>`}
                </tbody>
              </table>
            </div>
          </div>
          <div class="overview-block">
            <div class="panel-title">Top error signatures</div>
            <div class="overview-scroll">
              <table class="metrics-table">
                <thead><tr><th>Signature</th><th>Function</th><th class="number">Count</th></tr></thead>
                <tbody>
                  ${errorSigs.map(r=>`
                    <tr>
                      <td>${escapeHtml(r.sig)}</td>
                      <td class="function-name">${escapeHtml(r.fn)}</td>
                      <td class="number">${r.count}</td>
                    </tr>
                  `).join('') || `<tr><td class="muted" colspan="3">No errors detected</td></tr>`}
                </tbody>
              </table>
            </div>
          </div>
          <div class="overview-block">
            <div class="panel-title">CPU hotspots</div>
            <div class="overview-scroll">
              <table class="metrics-table">
                <thead><tr><th>Function</th><th class="number">CPU</th><th class="number">Calls</th></tr></thead>
                <tbody>
                  ${cpuHotspots.map(r=>`
                    <tr>
                      <td class="function-name">${escapeHtml(r.fn)}</td>
                      <td class="number">${r.cpuS.toFixed(4)}s</td>
                      <td class="number">${r.calls}</td>
                    </tr>
                  `).join('') || `<tr><td class="muted" colspan="3">No CPU data</td></tr>`}
                </tbody>
              </table>
            </div>
          </div>
          <div class="overview-block">
            <div class="panel-title">Recent slow calls (5m)</div>
            <div class="overview-scroll">
              <table class="metrics-table">
                <thead><tr><th>Function</th><th class="number">Duration</th><th class="number">Start</th></tr></thead>
                <tbody>
                  ${recentSlow.map(n=>`
                    <tr>
                      <td class="function-name">${escapeHtml(cleanFnName(n.function || n.call_id || '-'))}</td>
                      <td class="number">${(n.duration * 1000).toFixed(1)}ms</td>
                      <td class="number">${n.start_time ? new Date(n.start_time*1000).toLocaleTimeString() : '-'}</td>
                    </tr>
                  `).join('') || `<tr><td class="muted" colspan="3">No recent calls</td></tr>`}
                </tbody>
              </table>
            </div>
          </div>
          <div class="overview-block">
            <div class="panel-title">Memory delta hotspots</div>
            <div class="overview-scroll">
              <table class="metrics-table">
                <thead><tr><th>Function</th><th class="number">MemΔ</th><th class="number">Calls</th></tr></thead>
                <tbody>
                  ${memHotspots.map(r=>`
                    <tr>
                      <td class="function-name">${escapeHtml(r.fn)}</td>
                      <td class="number">${r.memDeltaKb.toFixed(0)} KB</td>
                      <td class="number">${r.calls}</td>
                    </tr>
                  `).join('') || `<tr><td class="muted" colspan="3">No memory delta data</td></tr>`}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    `;
  }

  function groupedRunItems(rawRuns){
    const query = (runQuery || '').toLowerCase().trim();
    let runs = rawRuns.filter(r=>{
      if(!query) return true;
      const hay = `${r.function||''} ${r.id||''} ${r.status||''}`.toLowerCase();
      return hay.includes(query);
    });
    runs = runs.sort((a,b)=>(b.start_time||0)-(a.start_time||0));
    if(runGroupBy === 'none') return runs.map(r=>({kind:'run', run:r}));
    const keyFor = (r)=>{
      if(runGroupBy === 'function') return r.function || 'unknown';
      if(runGroupBy === 'status') return r.status || 'unknown';
      const dt = r.start_time ? new Date(r.start_time*1000) : null;
      return dt ? dt.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'}) : 'unknown';
    };
    const groups = new Map();
    runs.forEach(r=>{
      const k = keyFor(r);
      if(!groups.has(k)) groups.set(k, []);
      groups.get(k).push(r);
    });
    const out = [];
    [...groups.entries()].forEach(([label, items])=>{
      out.push({kind:'group', label, count: items.length});
      items.forEach(run=> out.push({kind:'run', run}));
    });
    return out;
  }

  function ensureRunVirtualDom(){
    if(document.getElementById('run-viewport')) return;
    runListEl.innerHTML = `
      <div id="run-viewport" class="virtual-viewport">
        <div id="run-spacer" class="virtual-spacer"></div>
        <div id="run-layer" class="virtual-layer"></div>
      </div>
    `;
    const viewport = document.getElementById('run-viewport');
    viewport.addEventListener('scroll', ()=>{
      runScrollTop = viewport.scrollTop || 0;
      renderRuns();
    });
  }

  function renderRuns(){
    ensureRunVirtualDom();
    const rawRuns = tree.map((n, idx)=>({
      id: n.call_id || `run-${idx}`,
      function: n.function || 'root',
      status: n.status || '-',
      error: n.error || null,
      start_time: n.start_time,
      duration: n.duration
    }));
    const items = groupedRunItems(rawRuns);
    const viewport = document.getElementById('run-viewport');
    const spacer = document.getElementById('run-spacer');
    const layer = document.getElementById('run-layer');
    const rowH = runCompact ? 38 : 56;
    const totalH = items.length * rowH;
    spacer.style.height = `${totalH}px`;
    const viewH = viewport.clientHeight || 620;
    const maxScroll = Math.max(0, totalH - viewH);
    if((runScrollTop || 0) > maxScroll){
      runScrollTop = maxScroll;
      viewport.scrollTop = maxScroll;
    }
    const start = Math.max(0, Math.floor((runScrollTop || viewport.scrollTop || 0) / rowH) - 4);
    const end = Math.min(items.length, start + Math.ceil(viewH / rowH) + 8);
    const slice = items.slice(start, end);
    layer.style.transform = `translateY(${start * rowH}px)`;
    layer.innerHTML = slice.map(item=>{
      if(item.kind === 'group'){
        return `<div class="run-group" style="height:${rowH}px;">${escapeHtml(item.label)} (${item.count})</div>`;
      }
      const run = item.run;
      const isActive = run.id === selectedRunId;
      const time = run.start_time ? new Date(run.start_time*1000).toLocaleTimeString() : '-';
      const errorBadge = run.error || run.status === 'error' ? '<span class="pill error">error</span>' : '';
      return `
        <div class="run-item ${isActive ? 'active' : ''} ${runCompact ? 'compact' : 'comfy'}" data-action="select-run" data-run-id="${escapeAttr(run.id)}" style="height:${rowH-6}px;">
          ${errorBadge}
          <div class="grow">
            <div>${escapeHtml(cleanFnName(run.function))}</div>
            ${runCompact ? '' : `<div class="muted">${escapeHtml(run.id)}</div>`}
          </div>
          <div class="muted">${time}</div>
        </div>
      `;
    }).join('');
    if(!selectedRunId && rawRuns.length) selectedRunId = rawRuns[0].id;
  }

  function getPathSet(parentMap, targetId){
    const s = new Set();
    let cur = targetId;
    while(cur && parentMap.has(cur)){
      s.add(cur);
      cur = parentMap.get(cur);
    }
    if(cur) s.add(cur);
    return s;
  }

  function renderSelectionStrip(){
    if(!selectionStripEl) return;
    const node = traceMap.get(selectedCallId);
    if(!node){
      selectionStripEl.innerHTML = '<span class="selection-path">No node selected</span>';
      return;
    }
    const path = [];
    let cur = node;
    while(cur){
      path.push(cleanFnName(cur.function || cur.call_id || '?'));
      cur = traceMap.get(cur.parent_id);
    }
    path.reverse();
    const canBack = historyIndex > 0;
    const canForward = historyIndex >= 0 && historyIndex < selectionHistory.length - 1;
    selectionStripEl.innerHTML = `
      <button class="btn small" data-action="history-nav" data-delta="-1" ${canBack ? '' : 'disabled'}>Back</button>
      <button class="btn small" data-action="history-nav" data-delta="1" ${canForward ? '' : 'disabled'}>Forward</button>
      <button class="btn small" data-action="jump-parent">Parent</button>
      <button class="btn small" data-action="copy-selected-id">Copy ID</button>
      <span class="selection-path">${escapeHtml(path.join('  >  '))}</span>
    `;
  }

  function renderTraceTree(activeTree, q){
    const flat = flattenNodes(activeTree);
    traceMap = new Map(flat.map(n=>[n.call_id, n]));
    if(!selectedCallId && flat.length) selectedCallId = flat[0].call_id || null;
    if(selectedCallId && !traceMap.has(selectedCallId) && flat.length) selectedCallId = flat[0].call_id;
    const parentMap = new Map(flat.map(n=>[n.call_id, n.parent_id]));
    const pathSet = focusMode === 'path' ? getPathSet(parentMap, selectedCallId) : new Set();
    const visible = flat.filter(n=>{
      if((n.depth||0) > depthLimit) return false;
      if(!shouldDisplay(n, q)) return false;
      if(focusMode === 'errors' && !(n.error || n.status === 'error')) return false;
      if(focusMode === 'slow' && !((n.duration||0) * 1000 >= slowThresholdMs)) return false;
      if(focusMode === 'path' && !pathSet.has(n.call_id)) return false;
      return true;
    });
    visibleTraceNodes = visible;
    traceTreeEl.innerHTML = visible.map(n=>{
      const depth = n.depth || 0;
      const depthPad = 10 + (depth * 14);
      const isSelected = n.call_id === selectedCallId;
      const hasError = n.error || n.status === 'error';
      const duration = n.duration != null ? fmtDuration(n.duration) : '-';
      const shortId = (n.call_id || '-').slice(0, 8);
      const start = n.start_time ? new Date(n.start_time*1000).toLocaleTimeString() : '-';
      return `
        <div class="trace-row ${isSelected ? 'selected' : ''} ${hasError ? 'error' : ''}" data-action="select-call" data-call-id="${escapeAttr(n.call_id || '')}" style="padding-left:${depthPad}px;" title="call_id=${escapeAttr(n.call_id || '')} parent_id=${escapeAttr(n.parent_id || '-')}">
          <span class="trace-depth">d${depth}</span>
          <span class="trace-main">
            <span class="trace-fn">${escapeHtml(cleanFnName(n.function || n.call_id))}</span>
            <span class="trace-id">${shortId}</span>
          </span>
          <span class="trace-meta">${duration}</span>
          <span class="trace-meta">${start}</span>
          ${hasError ? '<span class="pill error">error</span>' : ''}
        </div>
      `;
    }).join('') || '<div class="muted">No trace nodes found for current filters.</div>';
    renderSelectionStrip();
  }

  function renderTraceDetails(activeTree){
    const flat = flattenNodes(activeTree);
    const node = flat.find(n=>n.call_id === selectedCallId) || flat[0];
    if(!node){
      traceDetailsEl.innerHTML = '<div class="muted">Select a trace to see details.</div>';
      return;
    }
    const args = node.args_preview!=null ? JSON.stringify(node.args_preview, null, 2) : '-';
    const kwargs = node.kwargs_preview!=null ? JSON.stringify(node.kwargs_preview, null, 2) : '-';
    const result = node.result_preview!=null ? JSON.stringify(node.result_preview, null, 2) : '-';
    const hasError = !!(node.error || node.status === 'error');
    const error = node.error ? `
      <div class="detail-error">
        <div class="detail-error-title">Error detected</div>
        <div class="kv error-kv">${escapeHtml(node.error)}</div>
      </div>
    ` : '';
    traceDetailsEl.innerHTML = `
      <div class="detail-block">
        <div class="detail-title ${hasError ? 'error' : ''}">Overview</div>
        <div class="kv"><strong>Function:</strong> ${escapeHtml(cleanFnName(node.function || '-'))}</div>
        <div class="kv ${hasError ? 'error-kv' : ''}"><strong>Status:</strong> ${escapeHtml(node.status || '-')}</div>
        <div class="kv"><strong>Call ID:</strong> ${escapeHtml(node.call_id || '-')}</div>
        <div class="kv"><strong>Parent ID:</strong> ${escapeHtml(node.parent_id || '-')}</div>
        <div class="kv"><strong>Start:</strong> ${fmtTime(node.start_time)} • <strong>End:</strong> ${fmtTime(node.end_time)}</div>
        <div class="kv"><strong>Duration:</strong> ${fmtDuration(node.duration)} • <strong>CPU:</strong> ${fmt(node.cpu_time)}s • <strong>MemΔ:</strong> ${node.mem_delta_kb ?? '-'} • <strong>Mem mode:</strong> ${escapeHtml(node.mem_mode || '-')}</div>
        ${error}
      </div>
      <div class="detail-block"><div class="detail-title">Args</div><div class="kv">${escapeHtml(args)}</div></div>
      <div class="detail-block"><div class="detail-title">Kwargs</div><div class="kv">${escapeHtml(kwargs)}</div></div>
      <div class="detail-block"><div class="detail-title">Result</div><div class="kv">${escapeHtml(result)}</div></div>
      <div class="detail-block"><button class="btn small" data-action="copy-text" data-copy="${escapeAttr(encodeURIComponent(JSON.stringify(node, null, 2)))}">Copy JSON</button></div>
    `;
  }

  function pushHistory(runId, callId){
    const last = selectionHistory[historyIndex];
    if(last && last.runId === runId && last.callId === callId) return;
    selectionHistory = selectionHistory.slice(0, historyIndex + 1);
    selectionHistory.push({runId, callId});
    historyIndex = selectionHistory.length - 1;
    if(selectionHistory.length > 300){
      selectionHistory.shift();
      historyIndex = selectionHistory.length - 1;
    }
  }

  function applyHistory(delta){
    const next = historyIndex + delta;
    if(next < 0 || next >= selectionHistory.length) return;
    historyIndex = next;
    const item = selectionHistory[historyIndex];
    selectedRunId = item.runId;
    selectedCallId = item.callId;
    render();
  }

  function getFilteredNodes(q){
    return flattenNodes(currentTree()).filter(n=>matchesNode(n, q));
  }

  function render(){
    syncControlState();
    const q = (searchEl.value||'').toLowerCase().trim();
    const activeTree = currentTree();
    overviewEl.innerHTML = '';

    const overviewPanel = buildOverviewPanel();
    const metricsPanel = buildMetricsPanel();
    const flamePanel = buildFlameGraph(activeTree, q);
    const issuesPanel = buildIssuesPanel(activeTree, q);

    rootEl.innerHTML = `
      <div class="tab-row">
        <button class="tab-btn ${insightTab==='overview' ? 'active' : ''}" data-action="select-insight-tab" data-tab="overview">Overview</button>
        <button class="tab-btn ${insightTab==='flame' ? 'active' : ''}" data-action="select-insight-tab" data-tab="flame">Traces</button>
        <button class="tab-btn ${insightTab==='issues' ? 'active' : ''}" data-action="select-insight-tab" data-tab="issues">Issues</button>
        <button class="tab-btn ${insightTab==='metrics' ? 'active' : ''}" data-action="select-insight-tab" data-tab="metrics">Metrics</button>
        <span class="tab-spacer"></span>
        ${insightTab==='metrics' ? `
          <div class="tab-secondary">
            <button class="tab-btn ${metricsTab==='latest' ? 'active' : ''}" data-action="metrics-tab" data-tab="latest">Latest</button>
            <button class="tab-btn ${metricsTab==='timeseries' ? 'active' : ''}" data-action="metrics-tab" data-tab="timeseries">Time series</button>
          </div>
        ` : ''}
      </div>
      <div class="${insightTab==='overview' ? '' : 'hidden-panel'}">${overviewPanel}</div>
      <div id="traces-tab-pane" class="${insightTab==='flame' ? '' : 'hidden-panel'}">
        <div id="trace-settings-slot"></div>
        ${flamePanel}
      </div>
      <div class="${insightTab==='issues' ? '' : 'hidden-panel'}">${issuesPanel}</div>
      <div class="${insightTab==='metrics' ? '' : 'hidden-panel'}">${metricsPanel}</div>
    `;

    const traceSettingsSlot = document.getElementById('trace-settings-slot');
    if(traceSettingsEl && traceSettingsSlot && traceSettingsEl.parentElement !== traceSettingsSlot){
      traceSettingsSlot.appendChild(traceSettingsEl);
    }
    if(traceSettingsEl){
      traceSettingsEl.classList.remove('hidden-panel');
    }

    if(splitLayoutEl){
      splitLayoutEl.classList.toggle('hidden-panel', insightTab !== 'flame');
    }

    renderRuns();
    renderTraceTree(activeTree, q);
    renderTraceDetails(activeTree);
    saveState();
  }

  async function fetchTree(){
    const res = await fetch('/api/tree');
    const data = await res.json();
    tree = data.roots || [];
    total = data.total_nodes || 0;
    metrics = data.metrics || [];
    generatedAt = data.generated_at || null;
    renderFnTypeOptions();
    metaEl.textContent = `${generatedAt ? new Date(generatedAt*1000).toLocaleString() : ''} • ${data.log_file} • ${total} nodes`;
    if(!selectedRunId && tree.length) selectedRunId = tree[0].call_id || null;
    const runStillExists = selectedRunId ? !!getRunNode(selectedRunId) : false;
    if(!runStillExists && tree.length){
      selectedRunId = tree[0].call_id || null;
      selectedCallId = null;
    }
    render();
  }

  window.__copyText = function(text){
    if(!navigator.clipboard) return;
    navigator.clipboard.writeText(text);
  };

  window.__selectRun = function(runId){
    selectedRunId = runId;
    const activeFlat = flattenNodes(currentTree());
    selectedCallId = activeFlat.length ? activeFlat[0].call_id : null;
    pushHistory(selectedRunId, selectedCallId);
    render();
  };

  window.__selectCall = function(callId){
    selectedCallId = callId;
    pushHistory(selectedRunId, selectedCallId);
    render();
  };

  document.addEventListener('click', (e)=>{
    const el = e.target && e.target.closest ? e.target.closest('[data-action]') : null;
    if(!el) return;
    const action = el.getAttribute('data-action');
    if(action === 'select-run'){
      window.__selectRun(el.getAttribute('data-run-id') || null);
      return;
    }
    if(action === 'select-call'){
      window.__selectCall(el.getAttribute('data-call-id') || null);
      return;
    }
    if(action === 'copy-text'){
      const encoded = el.getAttribute('data-copy') || '';
      try { window.__copyText(decodeURIComponent(encoded)); } catch (_err) { window.__copyText(encoded); }
      return;
    }
    if(action === 'select-insight-tab'){
      insightTab = el.getAttribute('data-tab') || 'overview';
      render();
      return;
    }
    if(action === 'metrics-tab'){
      metricsTab = el.getAttribute('data-tab') || 'latest';
      render();
      return;
    }
    if(action === 'history-nav'){
      applyHistory(Number(el.getAttribute('data-delta') || 0));
      return;
    }
    if(action === 'jump-parent'){
      const cur = traceMap.get(selectedCallId);
      if(cur && cur.parent_id){
        selectedCallId = cur.parent_id;
        pushHistory(selectedRunId, selectedCallId);
        render();
      }
      return;
    }
    if(action === 'copy-selected-id'){
      if(selectedCallId) window.__copyText(selectedCallId);
      return;
    }
  });

  function setStatusFilter(val){
    statusFilter = val;
    render();
  }

  statusFilterGroup.addEventListener('click', (e)=>{
    if(e.target && e.target.dataset && e.target.dataset.filter){
      setStatusFilter(e.target.dataset.filter);
    }
  });

  searchEl.addEventListener('input', render);
  refreshBtn.addEventListener('click', fetchTree);
  minDurationEl.addEventListener('input', (e)=>{ minDurationMs = Number(e.target.value || 0); render(); });
  fnTypeEl.addEventListener('change', (e)=>{ fnTypeFilter = e.target.value || 'all'; render(); });
  sortModeEl.addEventListener('change', (e)=>{ sortMode = e.target.value || 'start'; render(); });
  togglePayloadsEl.addEventListener('change', (e)=>{ showPayloads = !!e.target.checked; render(); });
  runSearchEl.addEventListener('input', (e)=>{ runQuery = e.target.value || ''; renderRuns(); saveState(); });
  runGroupEl.addEventListener('change', (e)=>{ runGroupBy = e.target.value || 'none'; renderRuns(); saveState(); });
  runCompactEl.addEventListener('change', (e)=>{ runCompact = !!e.target.checked; renderRuns(); saveState(); });
  autoRefreshEl.addEventListener('change', (e)=>{
    autoRefreshEnabled = !!e.target.checked;
    if(autoRefreshEnabled) scheduleRefresh(true); else if(refreshTimer) clearInterval(refreshTimer);
    saveState();
  });
  focusModeEl.addEventListener('change', (e)=>{ focusMode = e.target.value || 'all'; render(); });
  depthLimitEl.addEventListener('input', (e)=>{ depthLimit = Math.max(0, Number(e.target.value || 0)); render(); });
  expandDepthEl.addEventListener('click', ()=>{ depthLimit = Math.min(999, depthLimit + 1); depthLimitEl.value = depthLimit; render(); });
  collapseAllEl.addEventListener('click', ()=>{ depthLimit = 1; depthLimitEl.value = depthLimit; render(); });
  copyFilteredEl.addEventListener('click', ()=>{
    const q = (searchEl.value||'').toLowerCase().trim();
    window.__copyText(JSON.stringify(getFilteredNodes(q), null, 2));
  });

  document.addEventListener('keydown', (e)=>{
    const t = e.target;
    if(t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.tagName === 'SELECT')) return;
    if(!visibleTraceNodes.length) return;
    const idx = visibleTraceNodes.findIndex(n=>n.call_id === selectedCallId);
    if(e.key === 'j' || e.key === 'ArrowDown'){
      const next = visibleTraceNodes[Math.min(visibleTraceNodes.length - 1, Math.max(0, idx + 1))];
      if(next){ selectedCallId = next.call_id; pushHistory(selectedRunId, selectedCallId); render(); e.preventDefault(); }
    } else if(e.key === 'k' || e.key === 'ArrowUp'){
      const prev = visibleTraceNodes[Math.max(0, idx - 1)];
      if(prev){ selectedCallId = prev.call_id; pushHistory(selectedRunId, selectedCallId); render(); e.preventDefault(); }
    } else if(e.key === 'h' || e.key === 'ArrowLeft'){
      const cur = traceMap.get(selectedCallId);
      if(cur && cur.parent_id){ selectedCallId = cur.parent_id; pushHistory(selectedRunId, selectedCallId); render(); e.preventDefault(); }
    } else if(e.key === 'l' || e.key === 'ArrowRight'){
      const child = visibleTraceNodes.find(n=>n.parent_id === selectedCallId);
      if(child){ selectedCallId = child.call_id; pushHistory(selectedRunId, selectedCallId); render(); e.preventDefault(); }
    }
  });

  function scheduleRefresh(immediate=false){
    if(refreshTimer) clearInterval(refreshTimer);
    if(!autoRefreshEnabled) return;
    refreshTimer = setInterval(()=>{ if(autoRefreshEnabled) fetchTree(); }, 2500);
    if(immediate) fetchTree();
  }

  window.addEventListener('resize', ()=> renderRuns());

  loadState();
  syncControlState();
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

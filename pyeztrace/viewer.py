import hashlib
import json
import math
import os
import shlex
import stat
import sys
import threading
from collections import Counter
from datetime import datetime
from functools import wraps
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from pathlib import Path
from typing import Dict, Any, List, Optional
import time


def _synchronized(method):
    """Keep a response and its source metadata on the same file snapshot."""
    @wraps(method)
    def locked(self, *args, **kwargs):
        with self._entries_lock:
            return method(self, *args, **kwargs)
    return locked


def _finite_number(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return float(value) if math.isfinite(value) else None
        except (OverflowError, ValueError):
            pass
    return None


class _TraceTreeBuilder:
    def __init__(self, log_file: Path) -> None:
        # Normalize to an absolute, user-expanded path so `~` and relative paths work
        # even when the viewer is started from a different working directory.
        try:
            self.log_file = log_file.expanduser().resolve(strict=False)
        except Exception:
            self.log_file = Path(str(log_file)).expanduser()
        self._entries_lock = threading.RLock()
        self._cached_entries: List[Dict[str, Any]] = []
        self._cached_offset = 0
        self._cached_inode: Optional[tuple[int, int]] = None
        self._cached_remainder = b""
        self._pruned_count = 0
        self._version = 0
        # Source-state tracking. These let the UI distinguish "no file yet",
        # "file exists but is empty", "file has lines but none are trace records"
        # and "records exist but filters hide them" instead of showing zeroes.
        self._unparsed_lines = 0
        self._newest_event_at: Optional[float] = None
        self._generation = "empty"
        self._generation_head = None
        self._calls_started = 0
        self._node_references = Counter()
        self._read_error = None
        self._file_exists = False
        self._file_size = 0
        self._file_mtime = None
        self._count_is_estimate = False
        self._tree_cache = None
        self._tree_cache_version = -1
        self._tree_cache_metrics_mtime = None
        self._tree_cache_metrics_size = None
        self._logs_cache = None
        self._logs_cache_version = -1
        self._logs_cache_limit = -1
        self._logs_cache_preview = -1

    def _stat_inode(self) -> Optional[tuple[int, int]]:
        try:
            st = self.log_file.stat()
            return (int(st.st_dev), int(st.st_ino))
        except Exception:
            return None

    def _metrics_file(self) -> Path:
        return Path(str(self.log_file) + ".metrics")

    def _metrics_stat(self) -> tuple[float, int]:
        metrics_file = self._metrics_file()
        try:
            st = metrics_file.stat()
            return (st.st_mtime, st.st_size)
        except Exception:
            return (0.0, 0)

    def _reset_source(self):
        self._cached_entries = []
        self._cached_offset = 0
        self._cached_inode = None
        self._cached_remainder = b""
        self._pruned_count = 0
        self._unparsed_lines = 0
        self._newest_event_at = None
        # New content: recompute the identity on the next read.
        self._generation = "empty"
        self._generation_head = None
        self._calls_started = 0
        self._node_references.clear()
        self._count_is_estimate = False
        self._version += 1

    _HEAD_SCAN_BYTES = 65536

    def _read_head_key(self, handle) -> Optional[str]:
        """Hash the log's first COMPLETE line, or None if there isn't one yet.

        This is the log's content fingerprint. It is read on every poll, not
        cached once, because a same-size in-place rewrite changes neither the
        inode nor the file size and would otherwise go undetected - leaving the
        viewer reading new content from a stale offset.

        Only the first line is hashed: a fixed byte window would keep changing
        while a small file is still being appended to, and a partially written
        first line would hash differently once completed.
        """
        position = handle.tell()
        try:
            handle.seek(0)
            head = handle.read(self._HEAD_SCAN_BYTES)
        except OSError:
            return None
        finally:
            try:
                handle.seek(position)
            except OSError:
                pass
        newline = head.find(b"\n")
        if newline >= 0:
            head = head[:newline + 1]
        elif len(head) < self._HEAD_SCAN_BYTES:
            # Nothing written yet, or the first line is still incomplete.
            return None
        return hashlib.sha1(head).hexdigest()[:16]

    @_synchronized
    def _read_entries_cached(self) -> List[Dict[str, Any]]:
        self._read_error = None
        try:
            with self.log_file.open("rb") as f:
                st = os.fstat(f.fileno())
                if not stat.S_ISREG(st.st_mode):
                    raise OSError("Trace source must be a regular file")
                inode = (st.st_dev, st.st_ino)
                self._file_exists = True
                self._file_size = st.st_size
                self._file_mtime = st.st_mtime
                head_key = self._read_head_key(f)
                # Three ways the stream we were following can be gone: a new
                # file, a shrunken file, or the same-sized file rewritten in
                # place (which changes neither inode nor size).
                if (
                    (self._cached_inode is not None and inode != self._cached_inode)
                    or st.st_size < self._cached_offset
                    or (
                        self._generation_head is not None
                        and head_key is not None
                        and head_key != self._generation_head
                    )
                ):
                    self._reset_source()
                self._cached_inode = inode
                # Identify the log *stream*, not this process. Restarting the
                # viewer against an unchanged file must keep the same generation,
                # or the browser mistakes a restart for the log being replaced
                # and discards the user's selection.
                self._generation_head = head_key
                self._generation = "empty" if head_key is None else f"{inode[0]}-{inode[1]}-{head_key}"
                if self._cached_offset == 0 and st.st_size > 50 * 1024 * 1024:
                    f.seek(st.st_size - 50 * 1024 * 1024)
                    f.readline()
                    self._pruned_count = f.tell() // 500
                    self._count_is_estimate = True
                else:
                    f.seek(self._cached_offset)
                chunk = f.read()
                self._cached_offset = f.tell()
        except FileNotFoundError:
            if self._cached_inode is not None:
                self._reset_source()
            self._file_exists = False
            self._file_size = 0
            self._file_mtime = None
            return []
        except OSError as exc:
            self._read_error = str(exc)
            return list(self._cached_entries)
        if not chunk:
            return list(self._cached_entries)
        lines = (self._cached_remainder + chunk).split(b"\n")
        self._cached_remainder = lines.pop()
        parsed, skipped = self._parse_json_lines(lines)
        self._unparsed_lines += skipped
        for entry in parsed:
            data = entry.get("data") or {}
            if data.get("event") == "start" and data.get("call_id"):
                self._calls_started += 1
            for key in ("call_id", "parent_id"):
                if data.get(key):
                    self._node_references[data[key]] += 1
            epoch = self._event_epoch(entry)
            if epoch is not None and (self._newest_event_at is None or epoch > self._newest_event_at):
                self._newest_event_at = epoch
        if parsed:
            self._cached_entries.extend(parsed)
            self._version += 1
            prune_amount = max(0, len(self._cached_entries) - 100000)
            for entry in self._cached_entries[:prune_amount]:
                for key in ("call_id", "parent_id"):
                    value = (entry.get("data") or {}).get(key)
                    if value:
                        self._node_references[value] -= 1
                        if not self._node_references[value]:
                            del self._node_references[value]
            self._pruned_count += prune_amount
            self._cached_entries = self._cached_entries[prune_amount:]
        return list(self._cached_entries)

    def _parse_json_lines(self, lines: List[str]) -> tuple[List[Dict[str, Any]], int]:
        """Parse JSONL trace records.

        Returns the parsed entries plus the number of non-empty lines that were
        skipped. The skipped count is what lets the viewer say "this file is not
        JSON trace output" instead of silently rendering an empty dashboard.
        """
        entries = []
        skipped = 0
        for line in lines:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s, parse_constant=lambda _: None,
                                 parse_float=lambda value: _finite_number(float(value)))
                # Minimal validation
                if (isinstance(obj, dict) and isinstance(obj.get('timestamp'), str)
                        and isinstance(obj.get('level'), str)
                        and all(isinstance(obj.get(k), (str, type(None)))
                                for k in ('function', 'project', 'message', 'fn_type'))
                        and (obj.get('data') is None or isinstance(obj.get('data'), dict))
                        and all(isinstance((obj.get('data') or {}).get(k), (str, type(None)))
                                for k in ('call_id', 'parent_id', 'event', 'status'))):
                    entries.append(obj)
                else:
                    skipped += 1
            except Exception:
                # Ignore non-JSON lines, but remember that we saw them.
                skipped += 1
        return entries, skipped

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
                obj = json.loads(s, parse_constant=lambda _: None,
                                 parse_float=lambda value: _finite_number(float(value)))
                if (isinstance(obj, dict) and obj.get("event") == "metrics_summary"
                        and isinstance(obj.get("metrics", []), list)
                        and all(isinstance(item, dict) for item in obj.get("metrics", []))):
                    metrics_entries.append(obj)
            except Exception:
                continue
        return metrics_entries

    def _event_epoch(self, entry: Dict[str, Any]) -> Optional[float]:
        """Best-effort event time for an entry, or None if it cannot be determined.

        Unlike _to_epoch this never falls back to "now": a record with an
        unreadable timestamp must not make the newest trace look current.
        """
        data = entry.get("data")
        if isinstance(data, dict):
            epoch = _finite_number(data.get("time_epoch"))
            if epoch is not None:
                return epoch
        return self._to_epoch(entry.get("timestamp", ""))

    @_synchronized
    def source_state(self, refresh=True) -> Dict[str, Any]:
        if refresh:
            self._read_entries_cached()
        count = len(self._cached_entries) + self._pruned_count
        status = "ok"
        if self._read_error:
            status = "read_error"
        elif not self._file_exists:
            status = "missing_file"
        elif not self._file_size:
            status = "empty_file"
        elif not count:
            status = "unparsable" if self._unparsed_lines else "no_records"
        return {
            "status": status, "log_file": str(self.log_file),
            "file_exists": self._file_exists, "file_size": self._file_size,
            "file_mtime": self._file_mtime, "record_count": count,
            "unparsed_lines": self._unparsed_lines,
            "newest_event_at": self._newest_event_at,
            "read_error": self._read_error, "generation": self._generation,
            "calls_started": self._calls_started,
            "count_is_estimate": self._count_is_estimate,
            "total_nodes": len(self._node_references),
        }

    def _to_epoch(self, timestamp_str: str) -> Optional[float]:
        try:
            return _finite_number(datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")).timestamp())
        except (TypeError, ValueError, OverflowError, AttributeError, OSError):
            return None

    def _safe_json_dumps(self, value: Any) -> str:
        try:
            return json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            return json.dumps(str(value), ensure_ascii=False)

    def _build_log_record(
        self,
        entry: Dict[str, Any],
        entry_idx: int,
        payload_preview_chars: int = 1200,
    ) -> Dict[str, Any]:
        cached = entry.get("_cached_record")
        if isinstance(cached, dict) and cached.get("_preview_limit") == payload_preview_chars:
            record = cached.copy()
            record["id"] = entry_idx
            return record

        data = entry.get("data")
        if not isinstance(data, dict):
            data = {}
        call_id = data.get("call_id")
        parent_id = data.get("parent_id")
        event = data.get("event")
        payload_json = self._safe_json_dumps(data)
        payload_truncated = len(payload_json) > payload_preview_chars
        payload_preview = payload_json[:payload_preview_chars]
        if payload_truncated:
            payload_preview += "…"

        ts_epoch = self._event_epoch(entry)

        record = {
            "id": entry_idx,
            "timestamp": entry.get("timestamp"),
            "timestamp_epoch": ts_epoch,
            "level": entry.get("level"),
            "project": entry.get("project"),
            "fn_type": entry.get("fn_type"),
            "function": entry.get("function"),
            "message": entry.get("message"),
            "call_id": call_id,
            "parent_id": parent_id,
            "event": event,
            "status": data.get("status"),
            "linked_to_trace": bool(call_id),
            "is_trace_event": event in {"start", "end", "error"},
            "payload_preview": payload_preview,
            "payload_size": len(payload_json),
            "payload_truncated": payload_truncated,
            "payload_keys": sorted([str(k) for k in data.keys()])[:40],
        }

        cache_copy = record.copy()
        cache_copy["_preview_limit"] = payload_preview_chars
        entry["_cached_record"] = cache_copy

        return record

    @_synchronized
    def build_logs(self, limit: int = 2000, payload_preview_chars: int = 1200) -> Dict[str, Any]:
        entries = self._read_entries_cached()
        with self._entries_lock:
            if (self._logs_cache is not None
                    and self._logs_cache_version == self._version
                    and self._logs_cache_limit == limit
                    and self._logs_cache_preview == payload_preview_chars):
                res = self._logs_cache.copy()
                res["generated_at"] = time.time()
                return res

        total_entries = len(entries) + self._pruned_count
        if limit > 0 and len(entries) > limit:
            entries_window = entries[-limit:]
            start_logical_idx = total_entries - limit
        else:
            entries_window = entries
            start_logical_idx = self._pruned_count

        records = [
            self._build_log_record(entry, start_logical_idx + i, payload_preview_chars=payload_preview_chars)
            for i, entry in enumerate(entries_window)
        ]

        res_to_cache = {
            "generated_at": time.time(),
            "log_file": str(self.log_file),
            "total_entries": total_entries,
            "generation": self._generation,
            "logs": records,
        }

        with self._entries_lock:
            self._logs_cache = res_to_cache
            self._logs_cache_version = self._version
            self._logs_cache_limit = limit
            self._logs_cache_preview = payload_preview_chars

        return res_to_cache.copy()

    @_synchronized
    def get_log_payload(self, entry_idx: int, generation: Optional[str] = None) -> Optional[Dict[str, Any]]:
        entries = self._read_entries_cached()
        if generation is not None and generation != self._generation:
            return None
        local_idx = entry_idx - self._pruned_count
        if local_idx < 0 or local_idx >= len(entries):
            return None
        entry = entries[local_idx]
        data = entry.get("data")
        if not isinstance(data, dict):
            data = {}
        return {
            "id": entry_idx,
            "entry": entry,
            "payload": data,
            "payload_json": self._safe_json_dumps(data),
            "payload_size": len(self._safe_json_dumps(data)),
        }

    @_synchronized
    def build_tree(self) -> Dict[str, Any]:
        entries = self._read_entries_cached()
        mtime, size = self._metrics_stat()
        with self._entries_lock:
            if (self._tree_cache is not None
                    and self._tree_cache_version == self._version
                    and self._tree_cache_metrics_mtime == mtime
                    and self._tree_cache_metrics_size == size):
                res = self._tree_cache.copy()
                res['generated_at'] = time.time()
                return res

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
                    'metrics': [item for item in data.get('metrics', []) if isinstance(item, dict)]
                               if isinstance(data.get('metrics', []), list) else [],
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
                node['start_time'] = self._event_epoch(e)
                node['args_preview'] = data.get('args_preview')
                node['kwargs_preview'] = data.get('kwargs_preview')
                node['status'] = status or 'running'
            elif event == 'end':
                node['end_time'] = self._event_epoch(e)
                node['duration'] = _finite_number(e.get('duration'))
                node['cpu_time'] = _finite_number(data.get('cpu_time'))
                node['mem_rss_kb'] = _finite_number(data.get('mem_rss_kb', data.get('mem_peak_kb')))
                node['mem_peak_kb'] = _finite_number(data.get('mem_peak_kb'))
                node['mem_delta_kb'] = _finite_number(data.get('mem_delta_kb'))
                node['mem_mode'] = data.get('mem_mode') or node.get('mem_mode')
                node['result_preview'] = data.get('result_preview')
                node['status'] = status or 'success'
            elif event == 'error':
                # Mark node with error info
                node['error'] = e.get('message')
                node['status'] = status or 'error'
                node['end_time'] = self._event_epoch(e)

        # Determine roots
        seen_as_child = set()
        for n in nodes.values():
            for c in n['children']:
                seen_as_child.add(c)
        roots = [cid for cid, n in nodes.items() if not n.get('parent_id') or cid not in seen_as_child]

        # Convert to nested structure with cycle detection to prevent RecursionError on corrupted logs
        visited = set()
        materialized_cids = set()
        depth_limited = False
        def materialize(cid: str) -> Dict[str, Any]:
            nonlocal depth_limited
            if len(visited) >= 150:
                depth_limited = True
            materialized_cids.add(cid)
            if cid in visited or len(visited) >= 150:
                n = nodes[cid]
                return {
                    **{k: v for k, v in n.items() if k != 'children'},
                    'children': [],
                    'error': 'Circular reference detected' if cid in visited else None,
                    'display_warning': 'Maximum display depth reached' if cid not in visited else None
                }
            visited.add(cid)
            try:
                n = nodes[cid]
                return {
                    **{k: v for k, v in n.items() if k != 'children'},
                    'children': [materialize(child) for child in n['children']]
                }
            finally:
                visited.remove(cid)

        tree = [materialize(cid) for cid in roots]

        # Any remaining nodes that were not materialized form unreached cycles; materialize them as roots
        for cid in list(nodes.keys()):
            if cid not in materialized_cids:
                tree.append(materialize(cid))

        sidecar_metrics = self._read_metrics_sidecar()
        metrics_entries: List[Dict[str, Any]] = []
        if sidecar_metrics:
            # Prefer sidecar snapshots; they are derived UI caches and avoid polluting trace logs.
            metrics_entries = sidecar_metrics
        else:
            metrics_entries = metrics_entries_from_log

        res_to_cache = {
            'generated_at': time.time(),
            'log_file': str(self.log_file),
            'roots': tree,
            'total_nodes': len(nodes),
            'display_depth_limited': depth_limited,
            'metrics': metrics_entries
        }

        with self._entries_lock:
            self._tree_cache = res_to_cache
            self._tree_cache_version = self._version
            self._tree_cache_metrics_mtime = mtime
            self._tree_cache_metrics_size = size

        return res_to_cache.copy()


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
                query = parse_qs(parsed.query)
                if parsed.path == '/':
                    self._send(200, outer._html_page().encode('utf-8'), 'text/html; charset=utf-8')
                elif parsed.path == '/app.js':
                    self._send(200, outer._js_bundle().encode('utf-8'), 'application/javascript')
                elif parsed.path == '/api/status':
                    # Cheap liveness + freshness probe. Used while the view is
                    # paused so connection state stays truthful and we can count
                    # what arrived without re-rendering the tree.
                    source = outer._builder.source_state()
                    data = {
                        'generated_at': time.time(),
                        'total_nodes': source['total_nodes'],
                        'source': source,
                    }
                    self._send(200, json.dumps(data).encode('utf-8'), 'application/json')
                elif parsed.path == '/api/tree':
                    with outer._builder._entries_lock:
                        data = outer._builder.build_tree()
                        data['source'] = outer._builder.source_state(refresh=False)
                    # `generated_at` is poll time; `source.newest_event_at` is data age.
                    # They are reported separately so the UI never presents one as the other.
                    self._send(200, json.dumps(data).encode('utf-8'), 'application/json')
                elif parsed.path == '/api/logs':
                    try:
                        limit = int((query.get('limit') or ['2000'])[0])
                    except Exception:
                        limit = 2000
                    try:
                        preview = int((query.get('preview') or ['1200'])[0])
                    except Exception:
                        preview = 1200
                    limit = max(100, min(limit, 10000))
                    preview = max(100, min(preview, 50000))
                    data = outer._builder.build_logs(limit=limit, payload_preview_chars=preview)
                    self._send(200, json.dumps(data).encode('utf-8'), 'application/json')
                elif parsed.path == '/api/logs/payload':
                    try:
                        entry_id = int((query.get('id') or ['-1'])[0])
                    except Exception:
                        entry_id = -1
                    payload = outer._builder.get_log_payload(entry_id, (query.get('generation') or [None])[0])
                    if payload is None:
                        self._send(404, b'Not Found', 'text/plain')
                    else:
                        self._send(200, json.dumps(payload).encode('utf-8'), 'application/json')
                elif parsed.path == '/api/entries':
                    # raw entries for debugging
                    entries = outer._builder._read_entries_cached()
                    clean_entries = [{k: v for k, v in e.items() if not k.startswith("_")} for e in entries[-1000:]]
                    self._send(200, json.dumps(clean_entries).encode('utf-8'), 'application/json')
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
    .text-action { cursor: pointer; }
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
    .logs-wrap { display: grid; grid-template-columns: minmax(380px, 1fr) minmax(380px, 1fr); gap: 12px; }
    .logs-controls { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }
    .logs-list-wrap { height: 70vh; max-height: 70vh; min-height: 320px; border: 1px solid var(--border); border-radius: 8px; background: rgba(17,24,39,0.45); overflow: hidden; }
    .logs-list-wrap .virtual-viewport { height: 100%; border: none; background: transparent; }
    .logs-detail-col { padding: 10px; max-height: 70vh; min-height: 320px; border: 1px solid var(--border); border-radius: 8px; background: rgba(17,24,39,0.45); overflow-y: scroll; overflow-x: hidden; }
    .log-row { padding: 8px 10px; border-bottom: 1px solid var(--border); cursor: pointer; }
    .log-row:hover { background: var(--surface-soft); }
    .log-row.active { background: rgba(56,189,248,0.14); box-shadow: inset 0 0 0 1px rgba(56,189,248,0.4); }
    .log-row-title { display: flex; gap: 8px; align-items: center; margin-bottom: 4px; }
    .log-row-msg { font-size: 12px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .log-row-meta { font-size: 11px; color: var(--muted); display: flex; gap: 8px; flex-wrap: wrap; }
    .code-block { border: 1px solid var(--border); border-radius: 8px; background: rgba(17,24,39,0.7); padding: 10px; margin-top: 8px; max-height: 48vh; overflow: auto; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; white-space: pre-wrap; word-break: break-word; }
    .log-detail-header { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 8px; }
    .pretty-card { border: 1px solid var(--border); border-radius: 10px; background: rgba(17,24,39,0.68); padding: 10px; margin-top: 8px; }
    .pretty-grid { display: grid; grid-template-columns: minmax(120px, 180px) 1fr; gap: 8px 10px; }
    .pretty-key { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; }
    .pretty-value { color: var(--text); font-size: 12px; line-height: 1.35; min-width: 0; overflow-wrap: anywhere; }
    .pretty-mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
    .pretty-badge-row { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 8px; }
    .pretty-json { margin-top: 8px; border: 1px solid var(--border); border-radius: 8px; background: rgba(2,6,23,0.65); padding: 10px; max-height: 34vh; overflow: auto; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; white-space: pre-wrap; word-break: break-word; }
    .payload-tree-controls { display: flex; gap: 8px; margin-top: 8px; }
    .payload-tree { margin-top: 8px; border: 1px solid var(--border); border-radius: 8px; background: rgba(2,6,23,0.65); padding: 10px; max-height: 34vh; overflow: auto; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; }
    .payload-tree-node > summary { cursor: pointer; user-select: none; list-style: none; display: flex; gap: 8px; align-items: center; padding: 2px 0; }
    .payload-tree-node > summary::-webkit-details-marker { display: none; }
    .payload-tree-node > summary::before { content: '▸'; color: var(--muted); width: 10px; display: inline-block; }
    .payload-tree-node[open] > summary::before { content: '▾'; }
    .payload-tree-children { margin-left: 14px; padding-left: 10px; border-left: 1px dashed var(--border); }
    .payload-tree-leaf { display: flex; gap: 6px; padding: 2px 0; align-items: baseline; min-width: 0; }
    .payload-tree-key { color: #93c5fd; word-break: break-word; }
    .payload-tree-meta { color: var(--muted); font-size: 11px; }
    .payload-tree-sep { color: var(--muted); }
    .payload-tree-value { min-width: 0; white-space: pre-wrap; word-break: break-word; }
    .payload-tree-value.string { color: #86efac; }
    .payload-tree-value.number { color: #fcd34d; }
    .payload-tree-value.boolean { color: #67e8f9; }
    .payload-tree-value.null { color: #c4b5fd; }
    .payload-tree-empty { color: var(--muted); padding: 2px 0; }
    .status-bar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; font-size: 12px; color: var(--muted); padding-top: 8px; margin-top: 6px; border-top: 1px solid var(--border); }
    .status-item { display: inline-flex; align-items: center; gap: 5px; white-space: nowrap; }
    .status-item strong { color: var(--text); font-weight: 600; }
    .status-sep { width: 1px; height: 14px; background: var(--border); }
    .conn-state { display: inline-flex; align-items: center; gap: 6px; font-weight: 700; letter-spacing: 0.02em; white-space: nowrap; }
    .conn-dot { width: 8px; height: 8px; border-radius: 999px; background: var(--muted); flex: none; }
    .conn-state.ok { color: #86efac; }
    .conn-state.ok .conn-dot { background: #22c55e; box-shadow: 0 0 0 3px rgba(34,197,94,0.18); }
    .conn-state.warn { color: #fcd34d; }
    .conn-state.warn .conn-dot { background: #f59e0b; box-shadow: 0 0 0 3px rgba(245,158,11,0.18); animation: conn-pulse 1.1s ease-in-out infinite; }
    .conn-state.bad { color: #fca5a5; }
    .conn-state.bad .conn-dot { background: #ef4444; box-shadow: 0 0 0 3px rgba(239,68,68,0.18); }
    @keyframes conn-pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.35; } }
    @media (prefers-reduced-motion: reduce) { .conn-state.warn .conn-dot { animation: none; } .running-dot { animation: none; } }
    .stale-note { color: #fcd34d; }
    .empty-state { border: 1px solid var(--border); border-radius: 12px; background: var(--surface); padding: 22px; margin-bottom: 14px; }
    .empty-state h2 { margin: 0 0 6px; font-size: 16px; color: var(--text); }
    .empty-state p { margin: 0 0 10px; color: var(--muted); font-size: 13px; line-height: 1.5; max-width: 70ch; }
    .empty-state code { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; background: rgba(2,6,23,0.6); border: 1px solid var(--border); border-radius: 6px; padding: 1px 5px; overflow-wrap: anywhere; }
    .empty-state pre { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; background: rgba(2,6,23,0.6); border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; overflow-x: auto; color: var(--text); margin: 0 0 10px; }
    .empty-state .empty-kicker { font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--accent); font-weight: 700; margin-bottom: 8px; }
    .empty-state ol { margin: 0; padding-left: 20px; color: var(--muted); font-size: 13px; line-height: 1.7; }
    .running-dot { width: 7px; height: 7px; border-radius: 999px; background: #38bdf8; display: inline-block; flex: none; animation: conn-pulse 1.2s ease-in-out infinite; }
    .pill.running { background: rgba(56,189,248,0.15); color: #7dd3fc; border-color: rgba(56,189,248,0.4); display: inline-flex; align-items: center; gap: 5px; }
    .elapsed { color: #7dd3fc; font-variant-numeric: tabular-nums; }
    /* A frozen timer is not counting. Dim it and mark it so a static number is
       not mistaken for a live one. */
    .elapsed.frozen { color: var(--muted); }
    .elapsed.frozen::after { content: ' \\23F8'; font-size: 0.85em; opacity: 0.8; }
    @media (max-width: 1080px) {
      header .meta { justify-content: flex-start; text-align: left; }
      .split-layout { grid-template-columns: 1fr; }
      .overview-columns { grid-template-columns: 1fr; }
      .logs-wrap { grid-template-columns: 1fr; }
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
    <div class="status-bar" id="status-bar">
      <span class="conn-state" id="conn-state" role="status" aria-live="polite">
        <span class="conn-dot" aria-hidden="true"></span><span id="conn-label">Connecting</span>
      </span>
      <span class="status-sep" aria-hidden="true"></span>
      <span class="status-item">Last checked <strong id="last-checked" title="When the viewer last reached the server">never</strong></span>
      <span class="status-item">Newest trace <strong id="newest-trace" title="Time of the most recent trace record in the log file">none yet</strong></span>
      <label class="toggle"><input type="checkbox" id="auto-refresh" checked /> Auto refresh</label>
      <span class="status-item hidden-panel" id="paused-counter"></span>
      <button class="btn small hidden-panel" id="retry-now">Retry now</button>
    </div>
  </header>
  <main>
    <div class="empty-state hidden-panel" id="empty-state"></div>
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
            r"""


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
  const emptyStateEl = document.getElementById('empty-state');
  const connStateEl = document.getElementById('conn-state');
  const connLabelEl = document.getElementById('conn-label');
  const lastCheckedEl = document.getElementById('last-checked');
  const newestTraceEl = document.getElementById('newest-trace');
  const pausedCounterEl = document.getElementById('paused-counter');
  const retryNowEl = document.getElementById('retry-now');

  // Connection + freshness state. `lastSuccessAt` is when we last reached the
  // server; `sourceState.newest_event_at` is how old the newest trace is.
  // These are deliberately never collapsed into one "last updated" value.
  let connState = 'connecting';
  let lastSuccessAt = null;
  let consecutiveFailures = 0;
  let lastFetchError = null;
  let sourceState = null;
  let pausedBaselineNodes = null;
  let pausedGeneration = null;
  let snapshotGeneration = null;
  let snapshotCalls = 0;
  let snapshotAt = null;
  let viewRevision = 0;
  let statusInFlight = false;
  let logsGeneration = null;
  let liveNodeCount = 0;
  let statusTicker = null;

  let tree = [];
  let logs = [];
  let logsVersion = 0;
  let total = 0;
  let metrics = [];
  let generatedAt = null;
  let logsGeneratedAt = null;
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
  let callToRunMap = new Map();
  let runScrollTop = 0;
  let selectionHistory = [];
  let historyIndex = -1;
  let logQuery = '';
  let logLevelFilter = 'all';
  let logLinkFilter = 'all';
  let logViewMode = 'console';
  let payloadMode = 'pretty';
  let selectedLogId = null;
  let logScrollTop = 0;
  let logDetailScrollTop = 0;
  let pendingLogAnchorId = null;
  let pendingLogAnchorOffset = 0;
  let visibleLogs = [];
  let payloadTreeOpenStateByKey = new Map();
  let panelScrollTopByKey = new Map();
  let filteredLogsCacheKey = '';
  let filteredLogsCache = [];
  let logSearchDebounce = null;
  let logsFetchCounter = 0;
  let fetchTreeInFlight = false;
  const fullPayloadCache = new Map();

  const STATE_KEY = 'pyeztrace_viewer_ui_v1';

  function saveState(){
    try {
      localStorage.setItem(STATE_KEY, JSON.stringify({
        statusFilter, minDurationMs, fnTypeFilter, sortMode, showPayloads,
        metricsTab, insightTab, autoRefreshEnabled, runQuery, runGroupBy,
        runCompact, focusMode, depthLimit, selectedRunId, logQuery,
        logLevelFilter, logLinkFilter, logViewMode, payloadMode
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
      logQuery = s.logQuery || '';
      logLevelFilter = s.logLevelFilter || logLevelFilter;
      logLinkFilter = s.logLinkFilter || logLinkFilter;
      logViewMode = s.logViewMode || logViewMode;
      payloadMode = s.payloadMode || payloadMode;
      if(payloadMode !== 'raw' && payloadMode !== 'pretty'){
        payloadMode = 'pretty';
      }
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
  function fmtClock(epoch){
    if(!epoch) return null;
    return new Date(epoch*1000).toLocaleTimeString();
  }
  function fmtAgo(epoch){
    if(!epoch) return null;
    const secs = Math.max(0, (Date.now()/1000) - epoch);
    if(secs < 5) return 'just now';
    if(secs < 60) return `${Math.floor(secs)}s ago`;
    if(secs < 3600) return `${Math.floor(secs/60)}m ago`;
    if(secs < 86400) return `${Math.floor(secs/3600)}h ${Math.floor((secs%3600)/60)}m ago`;
    return `${Math.floor(secs/86400)}d ago`;
  }
  function fmtElapsed(secs){
    if(secs == null || !isFinite(secs) || secs < 0) return '-';
    if(secs < 60) return `${secs.toFixed(1)}s`;
    if(secs < 3600) return `${Math.floor(secs/60)}m ${String(Math.floor(secs%60)).padStart(2,'0')}s`;
    return `${Math.floor(secs/3600)}h ${String(Math.floor((secs%3600)/60)).padStart(2,'0')}m`;
  }
  function isRunning(node){
    return node && node.status === 'running' && node.duration == null;
  }
  // A running-call timer may only advance while we are actually reading the
  // log. It freezes on the FIRST failed poll rather than after the staleness
  // grace period: counting past the last successful read and then snapping back
  // to it made the timer visibly run backwards.
  function elapsedIsLive(){
    return autoRefreshEnabled
      && consecutiveFailures === 0
      && effectiveConnState() === 'ok'
      && !(sourceState && sourceState.read_error);
  }
  function elapsedReferenceTime(){
    return elapsedIsLive() ? Date.now()/1000 : (snapshotAt || Date.now()/1000);
  }
  function applyElapsed(el, start){
    const live = elapsedIsLive();
    el.textContent = fmtElapsed(elapsedReferenceTime() - start);
    el.classList.toggle('frozen', !live);
    el.title = live
      ? 'Elapsed time for a call that is still running'
      : 'Frozen: elapsed time as of the last successful read';
  }
  function elapsedHtml(node){
    if(!isRunning(node) || !node.start_time) return '';
    const live = elapsedIsLive();
    const secs = elapsedReferenceTime() - node.start_time;
    return `<span class="elapsed${live ? '' : ' frozen'}" data-start="${node.start_time}" title="${
      live ? 'Elapsed time for a call that is still running' : 'Frozen: elapsed time as of the last successful read'
    }">${escapeHtml(fmtElapsed(secs))}</span>`;
  }
  function runningPill(){
    return '<span class="pill running"><span class="running-dot" aria-hidden="true"></span>running</span>';
  }

  function setConnState(state, err){
    connState = state;
    lastFetchError = err || null;
    renderStatusBar();
  }

  // Polling is suspended while the tab is in the background (and browsers
  // throttle timers there anyway). Reporting "Connected" during that window
  // would be the same lie as showing stale data with a fresh timestamp, so the
  // displayed state is derived from when we actually last succeeded.
  const STALE_AFTER_SEC = 10;
  function effectiveConnState(){
    if(connState !== 'ok') return connState;
    const age = lastSuccessAt ? ((Date.now()/1000) - lastSuccessAt) : Infinity;
    if(document.hidden && age > STALE_AFTER_SEC) return 'background';
    if(age > STALE_AFTER_SEC) return 'stale';
    return 'ok';
  }

  function renderStatusBar(){
    if(!connStateEl) return;
    const labels = {
      connecting: 'Connecting',
      ok: 'Connected',
      background: 'Paused - tab in background',
      stale: 'Not checking',
      retrying: 'Reconnecting',
      down: 'Disconnected'
    };
    const effective = effectiveConnState();
    const tone = { connecting: 'warn', ok: 'ok', background: 'warn', stale: 'warn', retrying: 'warn', down: 'bad' }[effective] || 'warn';
    connStateEl.classList.remove('ok', 'warn', 'bad');
    connStateEl.classList.add(tone);
    let label = labels[effective] || 'Unknown';
    if(effective === 'retrying' && consecutiveFailures > 1){
      label += ` (${consecutiveFailures} failed checks)`;
    }
    // This is an aria-live region and renderStatusBar runs every second. Only
    // write when the text actually changes, so screen readers announce state
    // transitions rather than re-announcing the same state repeatedly.
    if(connLabelEl.textContent !== label) connLabelEl.textContent = label;
    connStateEl.title = lastFetchError
      ? `Last error: ${lastFetchError}`
      : (effective === 'background' ? 'Polling resumes as soon as this tab is visible again.' : '');

    // Last checked = reachability. Never shown as data freshness.
    if(lastSuccessAt){
      const ago = fmtAgo(lastSuccessAt);
      lastCheckedEl.textContent = effective === 'ok' ? ago : `${ago} (stale)`;
      lastCheckedEl.title = `Last successful read at ${fmtClock(lastSuccessAt)}`;
      lastCheckedEl.classList.toggle('stale-note', effective !== 'ok');
    } else {
      lastCheckedEl.textContent = 'never';
      lastCheckedEl.classList.toggle('stale-note', effective === 'down');
    }

    // Newest trace = data age, straight from the log records themselves.
    const newest = sourceState && sourceState.newest_event_at;
    if(newest){
      newestTraceEl.textContent = fmtAgo(newest);
      newestTraceEl.title = `Most recent trace record at ${fmtClock(newest)}`;
    } else {
      newestTraceEl.textContent = 'none yet';
      newestTraceEl.title = 'No trace records have been read from the log file yet';
    }

    if(sourceState && sourceState.read_error){
      connLabelEl.textContent = 'Source unreadable - snapshot stale';
      connStateEl.classList.remove('ok'); connStateEl.classList.add('warn');
      connStateEl.title = sourceState.read_error;
    }
    if(retryNowEl){
      retryNowEl.classList.toggle('hidden-panel', effective === 'ok' || effective === 'connecting' || effective === 'background');
    }
    renderPausedCounter();
  }

  function renderPausedCounter(){
    if(!pausedCounterEl) return;
    if(autoRefreshEnabled || pausedBaselineNodes == null){
      pausedCounterEl.classList.add('hidden-panel');
      pausedCounterEl.textContent = '';
      return;
    }
    if(sourceState && pausedGeneration && sourceState.generation !== pausedGeneration){
      pausedCounterEl.classList.remove('hidden-panel');
      pausedCounterEl.textContent = 'Paused - log source replaced or truncated; resume to load it';
      return;
    }
    const delta = Math.max(0, liveNodeCount - pausedBaselineNodes);
    pausedCounterEl.classList.remove('hidden-panel');
    pausedCounterEl.innerHTML = delta > 0
      ? `<strong>Paused</strong> - ${delta} new call${delta === 1 ? '' : 's'} not shown`
      : '<strong>Paused</strong> - capture continues';
  }

  // Updates relative times and running-call timers without re-rendering the
  // tree, so selection, scroll position and focus survive.
  function tickLiveUi(){
    renderStatusBar();
    document.querySelectorAll('.elapsed[data-start]').forEach(el=>{
      const start = parseFloat(el.dataset.start);
      if(isFinite(start)) applyElapsed(el, start);
    });
  }

  function emptyStateHtml(){
    const st = sourceState;
    if(!st) return null;
    const file = escapeHtml(st.log_file || '');
    const quotedFile = "'" + (st.log_file || 'logs/app.log').replaceAll("'", "'\\''") + "'";
    const cmd = 'export EZTRACE_DISABLE_FILE_LOGGING=0 EZTRACE_FILE_LOG_FORMAT=json EZTRACE_LOG_FILE=' + quotedFile;
    if(connState === 'down' && !st.record_count){
      return `
        <div class="empty-kicker">Viewer disconnected</div>
        <h2>Cannot reach the viewer server</h2>
        <p>The page is still open but the server at this address is not responding${lastFetchError ? ` (${escapeHtml(lastFetchError)})` : ''}. Any results below were read before the connection dropped.</p>
        <p>Restart it with:</p>
        <pre>pyeztrace serve ${escapeHtml(quotedFile)} --open</pre>`;
    }
    if(st.status === 'read_error'){
      return `<h2>Cannot read the trace file</h2><p>${escapeHtml(st.read_error || 'Read failed')}. Last available data is preserved.</p>`;
    }
    if(st.status === 'missing_file'){
      return `
        <div class="empty-kicker">Waiting for the log file</div>
        <h2>No log file yet</h2>
        <p>The viewer is watching <code>${file}</code>. It does not exist yet, which is normal if the traced application has not started. The dashboard appears automatically once records arrive - no need to restart the viewer.</p>
        <ol>
          <li>Enable JSON file logging for the app at this path:<pre>${escapeHtml(cmd)}</pre></li>
          <li>Run the traced application as usual.</li>
        </ol>`;
    }
    if(st.status === 'empty_file' || st.status === 'no_records'){
      return `
        <div class="empty-kicker">Waiting for traces</div>
        <h2>Log file found, no trace records yet</h2>
        <p><code>${file}</code> exists but has no trace records so far. The viewer is polling and will render as soon as the first traced call starts.</p>
        <p>If the app is definitely running, confirm it is writing JSON to this exact path and that at least one function is decorated for tracing.</p>`;
    }
    if(st.status === 'unparsable'){
      return `
        <div class="empty-kicker">Wrong log format</div>
        <h2>This file is not JSON trace output</h2>
        <p>Read <strong>${st.unparsed_lines}</strong> line${st.unparsed_lines === 1 ? '' : 's'} from <code>${file}</code>, but none are JSON trace records. This usually means the application is writing the human-readable log format.</p>
        <p>Set the format before starting the traced application, then restart it:</p>
        <pre>export EZTRACE_FILE_LOG_FORMAT=json</pre>
        <p>The viewer picks up the change on its own once valid records are written.</p>`;
    }
    return null;
  }

  function filtersHideEverythingHtml(visibleCount){
    if(visibleCount > 0) return null;
    const count = insightTab === 'logs' ? logs.length : total;
    if(!sourceState || sourceState.status !== 'ok' || !count) return null;
    return `
      <div class="empty-kicker">Nothing matches</div>
      <h2>${count} ${insightTab === 'logs' ? 'log records' : 'calls'} captured, but filters hide them all</h2>
      <p>Trace data is arriving normally. ${insightTab === 'logs' ? 'The current log search, level or link filters exclude every record.' : 'The current search, status, duration or type filters exclude every call.'}</p>
      <p><button class="btn small" id="clear-all-filters">Clear all filters</button></p>`;
  }

  function renderEmptyState(visibleCount){
    if(!emptyStateEl) return false;
    const html = emptyStateHtml() || filtersHideEverythingHtml(visibleCount);
    if(!html){
      emptyStateEl.classList.add('hidden-panel');
      emptyStateEl.innerHTML = '';
      return false;
    }
    emptyStateEl.innerHTML = html;
    emptyStateEl.classList.remove('hidden-panel');
    const clearBtn = document.getElementById('clear-all-filters');
    if(clearBtn) clearBtn.addEventListener('click', clearAllFilters);
    return true;
  }

  function clearAllFilters(){
    searchEl.value = '';
    statusFilter = 'all';
    minDurationMs = 0;
    fnTypeFilter = 'all';
    focusMode = 'all';
    runQuery = ''; runSearchEl.value = '';
    logQuery = ''; logLevelFilter = 'all'; logLinkFilter = 'all';
    depthLimit = 99; depthLimitEl.value = 99;
    if(minDurationEl) minDurationEl.value = '';
    if(fnTypeEl) fnTypeEl.value = 'all';
    if(focusModeEl) focusModeEl.value = 'all';
    saveState();
    render();
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

  function rebuildCallToRunMap(){
    const out = new Map();
    tree.forEach(root=>{
      const runId = root.call_id || null;
      flattenNodes([root]).forEach(n=>{
        if(n.call_id) out.set(n.call_id, runId);
      });
    });
    callToRunMap = out;
  }

  function filteredLogs(){
    const cacheKey = `${logsVersion}|${logQuery}|${logLevelFilter}|${logLinkFilter}`;
    if(cacheKey === filteredLogsCacheKey){
      return filteredLogsCache;
    }
    const q = (logQuery || '').toLowerCase().trim();
    const out = logs.filter(l=>{
      if(logLevelFilter !== 'all' && String(l.level || '').toUpperCase() !== logLevelFilter) return false;
      if(logLinkFilter === 'linked' && !l.linked_to_trace) return false;
      if(logLinkFilter === 'unlinked' && l.linked_to_trace) return false;
      if(!q) return true;
      const hay = [
        l.message || '',
        l.function || '',
        l.fn_type || '',
        l.level || '',
        l.call_id || '',
        l.parent_id || '',
        l.event || '',
        l.payload_preview || ''
      ].join(' ').toLowerCase();
      return hay.includes(q);
    }).sort((a,b)=> (b.timestamp_epoch || 0) - (a.timestamp_epoch || 0));
    filteredLogsCacheKey = cacheKey;
    filteredLogsCache = out;
    return out;
  }

  function getLogsScrollEl(){
    const viewport = document.getElementById('logs-viewport');
    const wrapper = document.getElementById('logs-list-wrap');
    if(wrapper && wrapper.scrollTop > 0 && (!viewport || viewport.scrollTop === 0)){
      return wrapper;
    }
    return viewport || wrapper || null;
  }

  function getLogsDetailEl(){
    return document.getElementById('logs-detail-col');
  }

  function captureScrollGroup(baseKey, selector){
    const nodes = document.querySelectorAll(selector);
    nodes.forEach((node, idx)=>{
      panelScrollTopByKey.set(`${baseKey}:${idx}`, node.scrollTop || 0);
    });
  }

  function restoreScrollGroup(baseKey, selector){
    const nodes = document.querySelectorAll(selector);
    nodes.forEach((node, idx)=>{
      const key = `${baseKey}:${idx}`;
      if(!panelScrollTopByKey.has(key)) return;
      const wanted = Number(panelScrollTopByKey.get(key) || 0);
      const maxTop = Math.max(0, node.scrollHeight - node.clientHeight);
      node.scrollTop = Math.min(wanted, maxTop);
    });
  }

  function captureUiScrollState(){
    const listEl = getLogsScrollEl();
    if(listEl){
      logScrollTop = listEl.scrollTop || 0;
    }
    const detailEl = getLogsDetailEl();
    if(detailEl){
      logDetailScrollTop = detailEl.scrollTop || 0;
    }
    captureScrollGroup('trace-tree', '#trace-tree');
    captureScrollGroup('trace-details', '#trace-details');
    captureScrollGroup('run-viewport', '#run-viewport');
    captureScrollGroup('flame-scroll', '.flame-scroll');
    captureScrollGroup('metrics-scroll', '.metrics-scroll');
    captureScrollGroup('overview-scroll', '.overview-scroll');
    captureScrollGroup('code-block', '.code-block');
    captureScrollGroup('payload-tree', '.payload-tree');
  }

  function restoreUiScrollState(){
    const listEl = getLogsScrollEl();
    if(listEl){
      const maxTop = Math.max(0, listEl.scrollHeight - listEl.clientHeight);
      listEl.scrollTop = Math.min(logScrollTop || 0, maxTop);
    }
    const detailEl = getLogsDetailEl();
    if(detailEl){
      const maxTop = Math.max(0, detailEl.scrollHeight - detailEl.clientHeight);
      detailEl.scrollTop = Math.min(logDetailScrollTop || 0, maxTop);
    }
    restoreScrollGroup('trace-tree', '#trace-tree');
    restoreScrollGroup('trace-details', '#trace-details');
    restoreScrollGroup('run-viewport', '#run-viewport');
    restoreScrollGroup('flame-scroll', '.flame-scroll');
    restoreScrollGroup('metrics-scroll', '.metrics-scroll');
    restoreScrollGroup('overview-scroll', '.overview-scroll');
    restoreScrollGroup('code-block', '.code-block');
    restoreScrollGroup('payload-tree', '.payload-tree');
  }

  function captureLogListAnchor(){
    const viewport = getLogsScrollEl();
    if(!viewport || !visibleLogs.length){
      pendingLogAnchorId = null;
      pendingLogAnchorOffset = 0;
      return;
    }
    const rowH = 72;
    const top = viewport.scrollTop || logScrollTop || 0;
    const topIndex = Math.max(0, Math.min(visibleLogs.length - 1, Math.floor(top / rowH)));
    const topLog = visibleLogs[topIndex];
    if(!topLog){
      pendingLogAnchorId = null;
      pendingLogAnchorOffset = 0;
      return;
    }
    pendingLogAnchorId = String(topLog.id);
    pendingLogAnchorOffset = top - (topIndex * rowH);
  }

  function getSelectedVisibleLog(){
    if(!visibleLogs.length) return null;
    return visibleLogs.find(l=>String(l.id) === String(selectedLogId)) || visibleLogs[0] || null;
  }

  function renderLogsRows(){
    const viewport = document.getElementById('logs-viewport');
    const spacer = document.getElementById('logs-spacer');
    const layer = document.getElementById('logs-layer');
    if(!viewport || !spacer || !layer) return;
    const rowH = 72;
    const totalH = visibleLogs.length * rowH;
    spacer.style.height = `${totalH}px`;
    const viewH = viewport.clientHeight || 560;
    if(pendingLogAnchorId !== null){
      const anchorIndex = visibleLogs.findIndex(l=>String(l.id) === String(pendingLogAnchorId));
      if(anchorIndex >= 0){
        logScrollTop = (anchorIndex * rowH) + (pendingLogAnchorOffset || 0);
      }
      pendingLogAnchorId = null;
      pendingLogAnchorOffset = 0;
    }
    const maxScroll = Math.max(0, totalH - viewH);
    if((logScrollTop || 0) > maxScroll){
      logScrollTop = maxScroll;
    }
    const scrollEl = getLogsScrollEl();
    if(scrollEl && scrollEl.scrollTop !== (logScrollTop || 0)){
      scrollEl.scrollTop = logScrollTop || 0;
    }
    const top = logScrollTop || (scrollEl ? scrollEl.scrollTop : 0) || 0;
    const start = Math.max(0, Math.floor(top / rowH) - 4);
    const end = Math.min(visibleLogs.length, start + Math.ceil(viewH / rowH) + 8);
    const slice = visibleLogs.slice(start, end);
    layer.style.transform = `translateY(${start * rowH}px)`;
    layer.innerHTML = slice.map(l=>`
      <div class="log-row ${String(selectedLogId)===String(l.id) ? 'active' : ''}" data-action="select-log" data-log-id="${escapeAttr(String(l.id))}" style="height:${rowH-6}px;">
        <div class="log-row-title">
          <span class="pill ${String(l.level||'').toUpperCase()==='ERROR' ? 'error' : 'success'}">${escapeHtml(String(l.level || '-').toUpperCase())}</span>
          ${l.linked_to_trace ? '<span class="pill">trace</span>' : '<span class="pill">orphan</span>'}
          ${l.payload_truncated ? '<span class="pill">truncated</span>' : ''}
          <span class="muted">${escapeHtml(l.timestamp || '-')}</span>
        </div>
        <div class="log-row-msg">${escapeHtml(l.message || '(no message)')}</div>
        <div class="log-row-meta">
          <span>${escapeHtml(cleanFnName(l.function || '-'))}</span>
          <span>call=${escapeHtml((l.call_id || '-').slice(0, 12))}</span>
          <span>event=${escapeHtml(l.event || '-')}</span>
        </div>
      </div>
    `).join('') || '<div class="log-row"><span class="muted">No logs for current filters.</span></div>';
  }

  function logConsoleLine(log){
    const ts = log.timestamp || '-';
    const level = (log.level || '-').toUpperCase();
    const fn = cleanFnName(log.function || '-');
    const msg = log.message || '';
    const cid = log.call_id ? ` call_id=${log.call_id}` : '';
    return `${ts} ${level} ${fn} ${msg}${cid}`;
  }

  function parsedPayload(log){
    const loaded = fullPayloadCache.get(String(log.id));
    if(loaded && loaded.payload && typeof loaded.payload === 'object'){
      return loaded.payload;
    }
    try {
      const raw = log.payload_preview || '{}';
      const parsed = JSON.parse(raw);
      if(parsed && typeof parsed === 'object') return parsed;
    } catch (_e) {}
    return null;
  }

  function payloadTreeDomId(log){
    return `payload-tree-${String(log.id || 'x').replace(/[^a-zA-Z0-9_-]/g, '_')}`;
  }

  function payloadTreeStateKey(log){
    if(!log) return '';
    return [
      String(log.id || ''),
      String(log.timestamp_epoch || log.timestamp || ''),
      String(log.call_id || ''),
      String(log.message || ''),
    ].join('|');
  }

  function payloadTreeChildPath(basePath, childKey){
    return `${basePath}/${encodeURIComponent(String(childKey))}`;
  }

  function isPayloadContainer(value){
    return Array.isArray(value) || (!!value && typeof value === 'object');
  }

  function renderPayloadScalar(value){
    if(value === null){
      return '<span class="payload-tree-value null">null</span>';
    }
    const t = typeof value;
    if(t === 'string'){
      return `<span class="payload-tree-value string">"${escapeHtml(value)}"</span>`;
    }
    if(t === 'number'){
      return `<span class="payload-tree-value number">${escapeHtml(String(value))}</span>`;
    }
    if(t === 'boolean'){
      return `<span class="payload-tree-value boolean">${escapeHtml(String(value))}</span>`;
    }
    return `<span class="payload-tree-value">${escapeHtml(String(value))}</span>`;
  }

  function renderPayloadTreeNode(key, value, depth=0, nodePath='/', openSet=null){
    const keyHtml = escapeHtml(String(key));
    if(!isPayloadContainer(value)){
      return `
        <div class="payload-tree-leaf">
          <span class="payload-tree-key">${keyHtml}</span>
          <span class="payload-tree-sep">:</span>
          ${renderPayloadScalar(value)}
        </div>
      `;
    }

    const entries = Array.isArray(value)
      ? value.map((item, idx)=>([idx, item]))
      : Object.entries(value || {});
    const isOpen = openSet ? openSet.has(nodePath) : depth <= 1;
    const openAttr = isOpen ? 'open' : '';
    const shape = Array.isArray(value)
      ? `list[${entries.length}]`
      : `dict{${entries.length}}`;
    const children = entries.length
      ? entries.map(([childKey, childValue])=>renderPayloadTreeNode(
          childKey,
          childValue,
          depth + 1,
          payloadTreeChildPath(nodePath, childKey),
          openSet
        )).join('')
      : '<div class="payload-tree-empty">empty</div>';

    return `
      <details class="payload-tree-node" data-node-path="${escapeAttr(nodePath)}" ${openAttr}>
        <summary>
          <span class="payload-tree-key">${keyHtml}</span>
          <span class="payload-tree-meta">${shape}</span>
        </summary>
        <div class="payload-tree-children">${children}</div>
      </details>
    `;
  }

  function collectPayloadTreeOpenSet(treeEl){
    const openSet = new Set();
    if(!treeEl) return openSet;
    treeEl.querySelectorAll('details.payload-tree-node[data-node-path]').forEach(node=>{
      if(node.open){
        openSet.add(node.getAttribute('data-node-path') || '/');
      }
    });
    return openSet;
  }

  function persistPayloadTreeStateForSelected(){
    const selected = getSelectedVisibleLog();
    if(!selected) return;
    const treeEl = document.getElementById(payloadTreeDomId(selected));
    if(!treeEl) return;
    payloadTreeOpenStateByKey.set(payloadTreeStateKey(selected), collectPayloadTreeOpenSet(treeEl));
  }

  function snapshotPayloadTreeState(){
    persistPayloadTreeStateForSelected();
  }

  function setPayloadTreeExpansion(treeId, expanded){
    if(!treeId) return;
    const treeEl = document.getElementById(treeId);
    if(!treeEl) return;
    treeEl.querySelectorAll('details.payload-tree-node').forEach(el=>{
      el.open = !!expanded;
    });
    persistPayloadTreeStateForSelected();
  }

  function compactLogObject(log){
    return {
      id: log.id,
      timestamp: log.timestamp,
      level: log.level,
      project: log.project,
      fn_type: log.fn_type,
      function: log.function,
      message: log.message,
      call_id: log.call_id,
      parent_id: log.parent_id,
      event: log.event,
      status: log.status,
      payload_preview: log.payload_preview
    };
  }

  function renderFormattedLogView(log){
    const obj = compactLogObject(log);
    return `
      <div class="pretty-card">
        <div class="pretty-badge-row">
          <span class="pill ${String(log.level||'').toUpperCase()==='ERROR' ? 'error' : 'success'}">${escapeHtml(String(log.level || '-').toUpperCase())}</span>
          ${log.linked_to_trace ? '<span class="pill">trace-linked</span>' : '<span class="pill">unlinked</span>'}
          ${log.event ? `<span class="pill">${escapeHtml(log.event)}</span>` : ''}
          ${log.payload_truncated ? '<span class="pill">payload preview</span>' : '<span class="pill">full payload</span>'}
        </div>
        <div class="pretty-grid">
          <div class="pretty-key">Timestamp</div><div class="pretty-value">${escapeHtml(log.timestamp || '-')}</div>
          <div class="pretty-key">Function</div><div class="pretty-value">${escapeHtml(cleanFnName(log.function || '-'))}</div>
          <div class="pretty-key">Message</div><div class="pretty-value">${escapeHtml(log.message || '-')}</div>
          <div class="pretty-key">Call ID</div><div class="pretty-value pretty-mono">${escapeHtml(log.call_id || '-')}</div>
          <div class="pretty-key">Parent ID</div><div class="pretty-value pretty-mono">${escapeHtml(log.parent_id || '-')}</div>
          <div class="pretty-key">Status</div><div class="pretty-value">${escapeHtml(log.status || '-')}</div>
        </div>
      </div>
    `;
  }

  function renderFormattedPayloadView(log){
    const payload = parsedPayload(log);
    if(!payload){
      return `<div class="pretty-card"><div class="muted">Payload unavailable for formatted view.</div></div>`;
    }
    const treeId = payloadTreeDomId(log);
    const rootKey = 'root';
    const openSet = payloadTreeOpenStateByKey.get(payloadTreeStateKey(log)) || null;
    const treeHtml = renderPayloadTreeNode(rootKey, payload, 0, '/', openSet);
    const topLevelCount = Array.isArray(payload)
      ? payload.length
      : Object.keys(payload || {}).length;
    return `
      <div class="pretty-card">
        <div class="pretty-badge-row">
          <span class="pill">${topLevelCount} top-level</span>
          ${log.payload_truncated ? '<span class="pill">preview data</span>' : '<span class="pill">full data</span>'}
        </div>
        <div class="payload-tree-controls">
          <button class="btn small" data-action="payload-expand-all" data-tree-id="${escapeAttr(treeId)}">Expand all</button>
          <button class="btn small" data-action="payload-collapse-all" data-tree-id="${escapeAttr(treeId)}">Collapse all</button>
        </div>
        <div id="${escapeAttr(treeId)}" class="payload-tree">
          ${treeHtml}
        </div>
      </div>
    `;
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
            <thead><tr><th>Function</th><th>Error</th><th>Call ID</th><th>Actions</th></tr></thead>
            <tbody>
              ${issues.slice(0,60).map(n=>{
                  const callId = n.call_id || '';
                  const hasTraceTarget = !!(callId && callToRunMap.has(callId));
                  const callIdCell = callId
                    ? (hasTraceTarget
                        ? `<button class="btn small" data-action="go-trace-from-log" data-call-id="${escapeAttr(callId)}">${escapeHtml(callId)}</button>`
                        : `<span class="muted">${escapeHtml(callId)}</span>`)
                    : '-';
                  return `
                <tr>
                  <td>${escapeHtml(cleanFnName(n.function || '-'))}</td>
                  <td>${escapeHtml(n.error || '-')}</td>
                  <td>${callIdCell}</td>
                  <td>
                    ${hasTraceTarget ? `<button class="btn small primary" data-action="go-trace-from-log" data-call-id="${escapeAttr(callId)}">Open trace</button>` : `<span class="muted">No trace</span>`}
                    <button class="btn small" data-action="copy-text" data-copy="${escapeAttr(encodeURIComponent(callId))}">Copy</button>
                  </td>
                </tr>
                  `;
                }).join('')}
            </tbody>
          </table>
        ` : '<div class="muted">No errors for current filters.</div>'}
      </div>
    `;
  }

  function formatPayload(log){
    const loaded = fullPayloadCache.get(String(log.id));
    const payload = loaded ? loaded.payload : null;
    if(payloadMode === 'raw'){
      if(loaded) return loaded.payload_json || JSON.stringify(payload);
      return log.payload_preview || '{}';
    }
    if(loaded) return JSON.stringify(payload, null, 2);
    try {
      return JSON.stringify(JSON.parse(log.payload_preview || '{}'), null, 2);
    } catch (_e) {
      return log.payload_preview || '{}';
    }
  }

  function logPrimaryView(log){
    if(logViewMode === 'console'){
      return logConsoleLine(log);
    }
    if(logViewMode === 'json'){
      return JSON.stringify(compactLogObject(log));
    }
    return JSON.stringify(compactLogObject(log), null, 2);
  }

  function buildLogsPanel(){
    const visible = filteredLogs();
    visibleLogs = visible;
    const levelSet = new Set(['all']);
    logs.forEach(l=> {
      const level = String(l.level || '').toUpperCase().trim();
      if(level) levelSet.add(level);
    });
    const levels = [...levelSet].filter(Boolean);
    if(!levels.includes(logLevelFilter)){
      logLevelFilter = 'all';
    }
    if(!selectedLogId && visible.length){
      selectedLogId = String(visible[0].id);
    }
    if(selectedLogId && !visible.some(l=>String(l.id) === String(selectedLogId))){
      selectedLogId = visible.length ? String(visible[0].id) : null;
    }
    const selected = getSelectedVisibleLog();
    const hasTraceTarget = selected && selected.call_id && callToRunMap.has(selected.call_id);
    const payloadText = selected ? formatPayload(selected) : '';
    const loadedPayload = selected ? fullPayloadCache.get(String(selected.id)) : null;
    const payloadState = selected ? (selected.payload_truncated && !loadedPayload ? 'preview' : 'full') : 'preview';
    const logViewBody = selected
      ? (logViewMode === 'pretty'
        ? renderFormattedLogView(selected)
        : `<div class="code-block">${escapeHtml(logPrimaryView(selected))}</div>`)
      : '<div class="muted">Select a log to inspect details.</div>';
    const payloadViewBody = selected
      ? (payloadMode === 'pretty'
        ? renderFormattedPayloadView(selected)
        : `<div class="code-block">${escapeHtml(payloadText)}</div>`)
      : '<div class="muted">Select a log to inspect payload.</div>';
    return `
      <div id="logs-panel-shell" class="insight-panel">
        <div id="logs-panel-title" class="panel-title">Logs explorer (${visible.length}/${logs.length})</div>
        <div id="logs-panel-controls" class="logs-controls">
          <input id="log-search" class="run-search" placeholder="Search logs, payloads, IDs..." value="${escapeAttr(logQuery)}" />
          <select id="log-level" class="select">
            ${levels.map(l=>`<option value="${escapeAttr(l)}" ${logLevelFilter===l ? 'selected' : ''}>${escapeHtml(l === 'all' ? 'All levels' : l)}</option>`).join('')}
          </select>
          <select id="log-link-filter" class="select">
            <option value="all" ${logLinkFilter==='all' ? 'selected' : ''}>All logs</option>
            <option value="linked" ${logLinkFilter==='linked' ? 'selected' : ''}>Trace-linked</option>
            <option value="unlinked" ${logLinkFilter==='unlinked' ? 'selected' : ''}>Unlinked</option>
          </select>
          <select id="log-view-mode" class="select">
            <option value="console" ${logViewMode==='console' ? 'selected' : ''}>Console style</option>
            <option value="json" ${logViewMode==='json' ? 'selected' : ''}>JSON compact</option>
            <option value="pretty" ${logViewMode==='pretty' ? 'selected' : ''}>JSON formatted</option>
          </select>
          <select id="payload-mode" class="select">
            <option value="raw" ${payloadMode==='raw' ? 'selected' : ''}>Payload raw</option>
            <option value="pretty" ${payloadMode==='pretty' ? 'selected' : ''}>Payload formatted</option>
          </select>
        </div>
        <div id="logs-panel-body" class="logs-wrap">
          <div>
            <div id="logs-list-wrap" class="logs-list-wrap">
              <div id="logs-viewport" class="virtual-viewport">
                <div id="logs-spacer" class="virtual-spacer"></div>
                <div id="logs-layer" class="virtual-layer"></div>
              </div>
            </div>
          </div>
          <div id="logs-detail-col" class="logs-detail-col">
            ${selected ? `
              <div class="log-detail-header">
                <button class="btn small" data-action="copy-selected-log">Copy selected</button>
                ${hasTraceTarget ? `<button class="btn small primary" data-action="go-trace-from-log" data-call-id="${escapeAttr(selected.call_id || '')}">Go to trace</button>` : ''}
                ${selected.call_id ? `<button class="btn small" data-action="copy-selected-log-call-id">Copy call_id</button>` : ''}
                ${selected.payload_truncated && !loadedPayload ? `<button class="btn small" data-action="load-log-payload" data-log-id="${escapeAttr(String(selected.id))}">Load full payload</button>` : ''}
                <span class="muted">payload ${payloadState} (${selected.payload_size} chars)</span>
              </div>
              <div class="detail-title">Log view</div>
              ${logViewBody}
              <div class="detail-title" style="margin-top:8px;">Payload</div>
              ${payloadViewBody}
            ` : '<div class="muted">Select a log to inspect details.</div>'}
          </div>
        </div>
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
    const runningNodes = allNodes.filter(isRunning);
    // Rates are computed over *finished* calls only. A call still in flight has
    // not succeeded or failed, and counting it as either understates both.
    const settledCalls = successNodes.length + errorNodes.length;
    const errorRate = settledCalls ? ((errorNodes.length / settledCalls) * 100) : 0;
    const successRate = settledCalls ? ((successNodes.length / settledCalls) * 100) : 0;
    const oldestRunningStart = runningNodes.reduce((acc, n)=> (n.start_time && (acc == null || n.start_time < acc)) ? n.start_time : acc, null);
    const p50 = percentile(durationsMs, 50);
    const p95 = percentile(durationsMs, 95);
    const p99 = percentile(durationsMs, 99);
    const latestMetrics = metrics.length ? metrics[metrics.length - 1] : null;
    // A long-open call is an observation, not proof of interruption.
    const STALLED_AFTER_SEC = 300;
    const nowSec = Date.now()/1000;
    const missingEnd = allNodes.filter(n=>n.start_time && !n.end_time && (nowSec - n.start_time) > STALLED_AFTER_SEC).length;

    const fnMap = new Map();
    const functionTraceTarget = new Map();
    let cpuSamples = 0;
    let memSamples = 0;
    let cpuTotal = 0;
    let memDeltaNet = 0;
    let memDeltaPositive = 0;
    let memDeltaNegative = 0;
    let memDeltaMax = 0;
    const memModes = new Set();
    allNodes.forEach(n=>{
      const key = cleanFnName(n.function || n.call_id || 'unknown');
      if(!fnMap.has(key)){
        fnMap.set(key, { fn: key, calls: 0, completed: 0, running: 0, totalMs: 0, errors: 0, maxMs: 0, cpuS: 0, memDeltaKb: 0, cpuSamples: 0, memSamples: 0 });
      }
      const row = fnMap.get(key);
      row.calls += 1;
      if(isRunning(n)) row.running += 1;
      if(n.duration != null) row.completed += 1;
      if(n.duration != null){
        const ms = n.duration * 1000;
        row.totalMs += ms;
        row.maxMs = Math.max(row.maxMs, ms);
      }
      if(n.cpu_time != null){
        cpuSamples += 1; row.cpuSamples += 1;
        row.cpuS += Number(n.cpu_time) || 0;
        cpuTotal += Number(n.cpu_time) || 0;
      }
      if(n.mem_delta_kb != null){
        memSamples += 1; row.memSamples += 1;
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
      if(n.call_id){
        const prev = functionTraceTarget.get(key);
        const duration = Number(n.duration) || 0;
        if(!prev || duration > prev.duration){
          functionTraceTarget.set(key, { call_id: n.call_id, duration });
        }
      }
    });
    const hotspots = [...fnMap.values()]
      .sort((a,b)=> b.totalMs - a.totalMs)
      .slice(0, 12);
    const cpuHotspots = [...fnMap.values()].filter(r=>r.cpuSamples).sort((a,b)=> b.cpuS - a.cpuS).slice(0, 10);
    const memHotspots = [...fnMap.values()].filter(r=>r.memSamples).sort((a,b)=> b.memDeltaKb - a.memDeltaKb).slice(0, 10);

    const errMap = new Map();
    errorNodes.forEach(n=>{
      const sig = String(n.error || 'error').split('\\n')[0].slice(0, 140);
      if(!errMap.has(sig)) errMap.set(sig, { sig, count: 0, fn: cleanFnName(n.function || '-'), call_id: n.call_id || null });
      const row = errMap.get(sig);
      row.count += 1;
      if(!row.call_id && n.call_id) row.call_id = n.call_id;
    });
    const errorSigs = [...errMap.values()].sort((a,b)=> b.count - a.count).slice(0, 12);

    const newestEventAt = sourceState && sourceState.newest_event_at;
    const newestTraceLabel = newestEventAt ? fmtAgo(newestEventAt) : 'none yet';
    const newestTraceSub = newestEventAt ? `At ${fmtClock(newestEventAt)}` : 'No trace records read yet';
    const lastCheckedLabel = lastSuccessAt ? fmtAgo(lastSuccessAt) : 'never';
    const effectiveConn = effectiveConnState();
    const lastCheckedSub = !lastSuccessAt
      ? 'Not connected yet'
      : ({
          ok: `Server reachable at ${fmtClock(lastSuccessAt)}`,
          background: 'Paused while this tab is in the background',
          stale: 'Not currently polling',
          retrying: 'Retrying the connection',
          down: 'Disconnected - this view is frozen'
        }[effectiveConn] || 'Unknown');
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
        errorRate: nodes.filter(n=>n.status === 'success' || n.status === 'error' || n.error).length ? (errs / nodes.filter(n=>n.status === 'success' || n.status === 'error' || n.error).length * 100) : 0,
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
          <div class="overview-card"><div class="overview-label">Newest trace ${infoTip('Age of the most recent trace record in the log file. This is data freshness: if it stops advancing, the traced application has stopped emitting traces.')}</div><div class="overview-value">${escapeHtml(newestTraceLabel)}</div><div class="overview-sub">${escapeHtml(newestTraceSub)}</div></div>
          <div class="overview-card"><div class="overview-label">Last checked ${infoTip('When the viewer last reached the server. This is connection health, not data freshness - it advances on every successful poll even when no new traces have arrived.')}</div><div class="overview-value">${escapeHtml(lastCheckedLabel)}</div><div class="overview-sub">${escapeHtml(lastCheckedSub)}</div></div>
          <div class="overview-card"><div class="overview-label">Trace runs ${infoTip('Number of top-level trace roots. Useful for estimating how many independent workflows were captured.')}</div><div class="overview-value">${totalRuns}</div><div class="overview-sub">Top-level root traces</div></div>
          <div class="overview-card"><div class="overview-label">Total calls ${infoTip('Count of all parsed trace nodes (root + nested). Higher values indicate deeper or busier execution paths.')}</div><div class="overview-value">${totalCalls}</div><div class="overview-sub">All nodes parsed</div></div>
          <div class="overview-card"><div class="overview-label">Running now ${infoTip('Calls that have started and not yet reported an end. These are in flight, not failures - their duration and resource metrics are recorded on completion.')}</div><div class="overview-value">${runningNodes.length ? `${runningNodes.length} <span class="running-dot" aria-hidden="true"></span>` : '0'}</div><div class="overview-sub">${oldestRunningStart ? `Oldest running for <span class="elapsed" data-start="${oldestRunningStart}">${escapeHtml(fmtElapsed((Date.now()/1000) - oldestRunningStart))}</span>` : 'No calls in flight'}</div></div>
          <div class="overview-card"><div class="overview-label">Success rate ${infoTip('Share of FINISHED calls that completed successfully. Calls still running are excluded so in-flight work is never counted as a failure.')}</div><div class="overview-value">${settledCalls ? successRate.toFixed(1) + '%' : '-'}</div><div class="overview-sub">${successNodes.length} of ${settledCalls} finished call${settledCalls === 1 ? '' : 's'}</div></div>
          <div class="overview-card"><div class="overview-label">Error rate ${infoTip('Share of FINISHED calls that ended in an error. Calls still running are excluded from the denominator.')}</div><div class="overview-value" style="color:#fca5a5;">${settledCalls ? errorRate.toFixed(1) + '%' : '-'}</div><div class="overview-sub">${errorNodes.length} of ${settledCalls} finished call${settledCalls === 1 ? '' : 's'}</div></div>
          <div class="overview-card"><div class="overview-label">Latency p95 / p99 ${infoTip('Tail latency percentiles. p95 and p99 are strong indicators of user-facing slowdowns and outliers.')}</div><div class="overview-value">${p95==null?'-':p95.toFixed(1)} / ${p99==null?'-':p99.toFixed(1)} ms</div><div class="overview-sub">p50 ${p50==null?'-':p50.toFixed(1)} ms</div></div>
          <div class="overview-card"><div class="overview-label">Trace health ${infoTip('Calls open for more than 5 minutes with no end record; they may still be running. All open calls are counted under "Running now" instead, so live traffic is not reported as a problem.')}</div><div class="overview-value">${missingEnd}</div><div class="overview-sub">${missingEnd ? 'Open over 5 min - may still be running' : 'No calls open over 5 min'}</div></div>
          <div class="overview-card"><div class="overview-label">Calls / min ${infoTip('Throughput estimate over the observed trace span. Useful for capacity monitoring and traffic comparisons.')}</div><div class="overview-value">${callsPerMin ? callsPerMin.toFixed(1) : '-'}</div><div class="overview-sub">Across ${(spanSec/60).toFixed(1)} min window</div></div>
          <div class="overview-card"><div class="overview-label">CPU total ${infoTip('Sum of recorded CPU time across calls. Helpful for spotting compute-intensive workloads.')}</div><div class="overview-value">${cpuSamples ? cpuTotal.toFixed(3) + 's' : '-'}</div><div class="overview-sub">Recorded for ${cpuSamples} call${cpuSamples === 1 ? '' : 's'}</div></div>
          <div class="overview-card"><div class="overview-label">Mem delta SUM+ / SUM- / NET ${infoTip(memTipText)}</div><div class="overview-value">${memSamples ? `${memDeltaPositive.toFixed(0)} / ${Math.abs(memDeltaNegative).toFixed(0)} / ${memDeltaNet.toFixed(0)} KB` : '-'}</div><div class="overview-sub">${memSamples ? `MAX +${memDeltaMax.toFixed(0)} KB • ${memModeLabel}` : 'No recorded memory samples'}</div></div>
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
                  ${hotspots.map(r=>{
                    const target = functionTraceTarget.get(r.fn);
                    const callId = target && target.call_id ? target.call_id : '';
                    const hasTraceTarget = !!(callId && callToRunMap.has(callId));
                    const fnText = hasTraceTarget
                      ? `<span class="function-name text-action" data-action="go-trace-from-log" data-call-id="${escapeAttr(callId)}">${escapeHtml(r.fn)}</span>`
                      : `<span class="function-name">${escapeHtml(r.fn)}</span>`;
                    return `
                    <tr>
                      <td>${fnText}${r.running ? ` <span class="pill running"><span class="running-dot" aria-hidden="true"></span>${r.running}</span>` : ''}</td>
                      <td class="number">${r.calls}</td>
                      <td class="number">${r.completed ? r.totalMs.toFixed(1) + 'ms' : '-'}</td>
                      <td class="number">${r.completed ? r.maxMs.toFixed(1) + 'ms' : '-'}</td>
                      <td class="number">${r.errors}</td>
                    </tr>
                  `;
                  }).join('') || `<tr><td class="muted" colspan="5">No hotspot data</td></tr>`}
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
                  ${errorSigs.map(r=>{
                    const callId = r.call_id || '';
                    const hasTraceTarget = !!(callId && callToRunMap.has(callId));
                    const sigText = hasTraceTarget
                      ? `<span class="text-action" data-action="go-trace-from-log" data-call-id="${escapeAttr(callId)}">${escapeHtml(r.sig)}</span>`
                      : escapeHtml(r.sig);
                    const fnText = hasTraceTarget
                      ? `<span class="function-name text-action" data-action="go-trace-from-log" data-call-id="${escapeAttr(callId)}">${escapeHtml(r.fn)}</span>`
                      : `<span class="function-name">${escapeHtml(r.fn)}</span>`;
                    return `
                    <tr>
                      <td>${sigText}</td>
                      <td>${fnText}</td>
                      <td class="number">${r.count}</td>
                    </tr>
                  `;
                  }).join('') || `<tr><td class="muted" colspan="3">No errors detected</td></tr>`}
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
                  ${cpuHotspots.map(r=>{
                    const target = functionTraceTarget.get(r.fn);
                    const callId = target && target.call_id ? target.call_id : '';
                    const hasTraceTarget = !!(callId && callToRunMap.has(callId));
                    const fnText = hasTraceTarget
                      ? `<span class="function-name text-action" data-action="go-trace-from-log" data-call-id="${escapeAttr(callId)}">${escapeHtml(r.fn)}</span>`
                      : `<span class="function-name">${escapeHtml(r.fn)}</span>`;
                    return `
                    <tr>
                      <td>${fnText}</td>
                      <td class="number">${r.cpuS.toFixed(4)}s</td>
                      <td class="number">${r.calls}</td>
                    </tr>
                  `;
                  }).join('') || `<tr><td class="muted" colspan="3">No CPU data</td></tr>`}
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
                  ${recentSlow.map(n=>{
                    const callId = n.call_id || '';
                    const hasTraceTarget = !!(callId && callToRunMap.has(callId));
                    const fnText = escapeHtml(cleanFnName(n.function || n.call_id || '-'));
                    const linkedFnText = hasTraceTarget
                      ? `<span class="function-name text-action" data-action="go-trace-from-log" data-call-id="${escapeAttr(callId)}">${fnText}</span>`
                      : `<span class="function-name">${fnText}</span>`;
                    return `
                    <tr>
                      <td>${linkedFnText}</td>
                      <td class="number">${(n.duration * 1000).toFixed(1)}ms</td>
                      <td class="number">${n.start_time ? new Date(n.start_time*1000).toLocaleTimeString() : '-'}</td>
                    </tr>
                  `;
                  }).join('') || `<tr><td class="muted" colspan="3">No recent calls</td></tr>`}
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
                  ${memHotspots.map(r=>{
                    const target = functionTraceTarget.get(r.fn);
                    const callId = target && target.call_id ? target.call_id : '';
                    const hasTraceTarget = !!(callId && callToRunMap.has(callId));
                    const fnText = hasTraceTarget
                      ? `<span class="function-name text-action" data-action="go-trace-from-log" data-call-id="${escapeAttr(callId)}">${escapeHtml(r.fn)}</span>`
                      : `<span class="function-name">${escapeHtml(r.fn)}</span>`;
                    return `
                    <tr>
                      <td>${fnText}</td>
                      <td class="number">${r.memDeltaKb.toFixed(0)} KB</td>
                      <td class="number">${r.calls}</td>
                    </tr>
                  `;
                  }).join('') || `<tr><td class="muted" colspan="3">No memory delta data</td></tr>`}
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
      const runIsRunning = isRunning(run);
      return `
        <div class="run-item ${isActive ? 'active' : ''} ${runCompact ? 'compact' : 'comfy'}" data-action="select-run" data-run-id="${escapeAttr(run.id)}" style="height:${rowH-6}px;">
          ${errorBadge}
          ${runIsRunning ? runningPill() : ''}
          <div class="grow">
            <div>${escapeHtml(cleanFnName(run.function))}</div>
            ${runCompact ? '' : `<div class="muted">${escapeHtml(run.id)}</div>`}
          </div>
          <div class="muted">${runIsRunning ? (elapsedHtml(run) || time) : time}</div>
        </div>
      `;
    }).join('') || (rawRuns.length ? '<div class="muted">No runs match the run search. Clear the run search to see all runs.</div>' : '');
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
      const running = isRunning(n);
      // A running call has no duration yet - show live elapsed time instead of a
      // dash, and never imply the call took 0ms.
      const duration = running ? (elapsedHtml(n) || 'running') : (n.duration != null ? fmtDuration(n.duration) : '-');
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
          ${running ? runningPill() : ''}
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
    const running = isRunning(node);
    const relatedLogs = logs.filter(l=>l.call_id && l.call_id === node.call_id).slice(0, 12);
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
        <div class="kv ${hasError ? 'error-kv' : ''}"><strong>Status:</strong> ${escapeHtml(node.status || '-')} ${running ? runningPill() : ''}</div>
        <div class="kv"><strong>Call ID:</strong> ${escapeHtml(node.call_id || '-')}</div>
        <div class="kv"><strong>Parent ID:</strong> ${escapeHtml(node.parent_id || '-')}</div>
        <div class="kv"><strong>Start:</strong> ${fmtTime(node.start_time)} • <strong>End:</strong> ${running ? 'in progress' : fmtTime(node.end_time)}</div>
        ${running
          ? `<div class="kv"><strong>Elapsed:</strong> ${elapsedHtml(node) || '-'} • <strong>CPU:</strong> pending • <strong>Mem&#916;:</strong> pending • <strong>Mem mode:</strong> ${escapeHtml(node.mem_mode || 'pending')}</div>
             <div class="kv muted">Resource metrics are recorded when the call completes. Blank values here are not zero.</div>`
          : `<div class="kv"><strong>Duration:</strong> ${fmtDuration(node.duration)} • <strong>CPU:</strong> ${node.cpu_time == null ? '-' : fmt(node.cpu_time) + 's'} • <strong>Mem&#916;:</strong> ${node.mem_delta_kb ?? '-'} • <strong>Mem mode:</strong> ${escapeHtml(node.mem_mode || '-')}</div>`}
        ${error}
      </div>
      <div class="detail-block"><div class="detail-title">Args</div><div class="kv">${escapeHtml(args)}</div></div>
      <div class="detail-block"><div class="detail-title">Kwargs</div><div class="kv">${escapeHtml(kwargs)}</div></div>
      <div class="detail-block"><div class="detail-title">Result</div><div class="kv">${escapeHtml(result)}</div></div>
      <div class="detail-block">
        <div class="detail-title">Linked logs (${relatedLogs.length})</div>
        ${relatedLogs.length ? relatedLogs.map(l=>`
          <div class="kv">
            <strong>${escapeHtml(String(l.level || '-').toUpperCase())}</strong>
            <span class="muted">${escapeHtml(l.timestamp || '-')}</span>
            <div>${escapeHtml(l.message || '')}</div>
            <div class="flex" style="margin-top:6px;">
              <button class="btn small" data-action="select-log" data-log-id="${escapeAttr(String(l.id))}">Open log</button>
              <button class="btn small" data-action="copy-text" data-copy="${escapeAttr(encodeURIComponent(String(l.id)))}">Copy log id</button>
            </div>
          </div>
        `).join('') : '<div class="muted">No logs linked to this call ID.</div>'}
        <div class="flex" style="margin-top:6px;">
          <button class="btn small" data-action="open-logs-tab">Open logs tab</button>
        </div>
      </div>
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
    captureUiScrollState();
    syncControlState();
    const q = (searchEl.value||'').toLowerCase().trim();
    const activeTree = currentTree();
    overviewEl.innerHTML = '';

    // A dashboard of zeroes explains nothing. When there is no data to show,
    // say what the viewer is waiting for and suppress the empty panels.
    const visibleCount = insightTab === 'logs' ? filteredLogs().length : flattenNodes(tree).filter(n=>matchesNode(n, q)).length;
    const showingEmptyState = renderEmptyState(visibleCount);
    const noDataAtAll = !total;
    if(rootEl) rootEl.classList.toggle('hidden-panel', showingEmptyState && noDataAtAll);
    if(traceSettingsEl) traceSettingsEl.classList.toggle('hidden-panel', showingEmptyState && noDataAtAll);
    if(splitLayoutEl && showingEmptyState && noDataAtAll) splitLayoutEl.classList.add('hidden-panel');
    if(showingEmptyState && noDataAtAll){
      renderStatusBar();
      saveState();
      return;
    }

    const overviewPanel = insightTab === 'overview' ? buildOverviewPanel() : '';
    const metricsPanel = insightTab === 'metrics' ? buildMetricsPanel() : '';
    const flamePanel = insightTab === 'flame' ? buildFlameGraph(activeTree, q) : '';
    const issuesPanel = insightTab === 'issues' ? buildIssuesPanel(activeTree, q) : '';
    const logsPanel = insightTab === 'logs' ? buildLogsPanel() : '';

    rootEl.innerHTML = `
      <div class="tab-row">
        <button class="tab-btn ${insightTab==='overview' ? 'active' : ''}" data-action="select-insight-tab" data-tab="overview">Overview</button>
        <button class="tab-btn ${insightTab==='flame' ? 'active' : ''}" data-action="select-insight-tab" data-tab="flame">Traces</button>
        <button class="tab-btn ${insightTab==='issues' ? 'active' : ''}" data-action="select-insight-tab" data-tab="issues">Issues</button>
        <button class="tab-btn ${insightTab==='metrics' ? 'active' : ''}" data-action="select-insight-tab" data-tab="metrics">Metrics</button>
        <button class="tab-btn ${insightTab==='logs' ? 'active' : ''}" data-action="select-insight-tab" data-tab="logs">Logs</button>
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
      <div class="${insightTab==='logs' ? '' : 'hidden-panel'}">${logsPanel}</div>
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

    if(insightTab === 'flame'){
      renderRuns();
      renderTraceTree(activeTree, q);
      renderTraceDetails(activeTree);
    }
    if(insightTab === 'logs'){
      bindLogsControls();
      renderLogsRows();
    }
    restoreUiScrollState();
    renderStatusBar();
    saveState();
  }

  function renderLogsOnly(){
    renderEmptyState(filteredLogs().length);
    const shell = document.getElementById('logs-panel-shell');
    if(!shell) return;
    snapshotPayloadTreeState();
    captureLogListAnchor();
    captureUiScrollState();
    const temp = document.createElement('div');
    temp.innerHTML = buildLogsPanel();
    const nextTitle = temp.querySelector('#logs-panel-title');
    const nextBody = temp.querySelector('#logs-panel-body');
    const curTitle = document.getElementById('logs-panel-title');
    const curBody = document.getElementById('logs-panel-body');
    if(nextTitle && curTitle){
      curTitle.textContent = nextTitle.textContent || curTitle.textContent;
    }
    if(nextBody && curBody){
      curBody.innerHTML = nextBody.innerHTML;
      bindLogsControls();
      renderLogsRows();
      restoreUiScrollState();
    }
    saveState();
  }

  async function fetchJson(url){
    const controller = new AbortController();
    const timeout = setTimeout(()=>controller.abort(), 8000);
    try {
      const res = await fetch(url, { cache: 'no-store', signal: controller.signal });
      if(!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } finally { clearTimeout(timeout); }
  }

  async function fetchTree(forceLogs=false){
    if(fetchTreeInFlight) return;
    fetchTreeInFlight = true;
    const revision = viewRevision;
    try {
    const shouldFetchLogs = forceLogs || (insightTab === 'logs') || logs.length === 0 || (logsFetchCounter % 3 === 0);
    logsFetchCounter += 1;
    const [data, logsData] = await Promise.all([
      fetchJson('/api/tree'),
      shouldFetchLogs ? fetchJson('/api/logs?limit=2500&preview=1800') : Promise.resolve(null)
    ]);
    if(revision !== viewRevision) return;
    consecutiveFailures = 0;
    lastSuccessAt = Date.now()/1000;
    sourceState = data.source || sourceState;
    setConnState('ok', null);
    if(sourceState.read_error) { renderEmptyState(1); return; }
    if(snapshotGeneration && snapshotGeneration !== sourceState.generation){
      logs = []; fullPayloadCache.clear(); selectedLogId = null;
      selectedCallId = null; selectedRunId = null;
    }
    snapshotGeneration = sourceState.generation;
    snapshotCalls = sourceState.calls_started || 0;
    snapshotAt = Date.now()/1000;
    tree = data.roots || [];
    if(logsData && logsData.generation === snapshotGeneration){
      logsGeneration = logsData.generation;
      logs = logsData.logs || [];
      fullPayloadCache.clear();
      logsGeneratedAt = logsData.generated_at || null;
      logsVersion += 1;
    }
    total = data.total_nodes || 0;
    liveNodeCount = snapshotCalls;
    // Restored a paused session: anchor the "new calls" count to what is shown.
    if(!autoRefreshEnabled){
      pausedBaselineNodes = snapshotCalls;
      pausedGeneration = snapshotGeneration;
    }
    metrics = data.metrics || [];
    generatedAt = data.generated_at || null;
    rebuildCallToRunMap();
    renderFnTypeOptions();
    // Times live in the status bar, where "last checked" and "newest trace" are
    // reported separately. Keep this line to identifying facts only.
    metaEl.textContent = `${data.log_file} • ${total} nodes • ${logs.length} logs${data.display_depth_limited ? ' • deep hierarchy split into sections at 150 levels' : ''}${sourceState.count_is_estimate ? ' • earlier record count is estimated' : ''}`;
    metaEl.title = data.log_file || '';
    if(!selectedRunId && tree.length) selectedRunId = tree[0].call_id || null;
    const runStillExists = selectedRunId ? !!getRunNode(selectedRunId) : false;
    if(!runStillExists && tree.length){
      selectedRunId = tree[0].call_id || null;
      selectedCallId = null;
    }
    if(insightTab === 'logs' && document.getElementById('logs-panel-shell')){
      renderLogsOnly();
      renderEmptyState(filteredLogs().length);
    } else {
      render();
    }
    } catch (err) {
      // Never leave a stale view silently claiming to be current. Keep the last
      // good snapshot on screen, but say plainly that it is no longer live.
      consecutiveFailures += 1;
      const message = (err && err.message) ? err.message : String(err);
      setConnState(consecutiveFailures >= 3 ? 'down' : 'retrying', message);
      if(consecutiveFailures >= 3) renderEmptyState(1);
    } finally {
      fetchTreeInFlight = false;
      if(revision !== viewRevision && autoRefreshEnabled) fetchTree();
    }
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

  window.__selectLog = function(logId){
    selectedLogId = String(logId);
    insightTab = 'logs';
    render();
  };

  async function loadLogPayload(logId){
    const key = String(logId);
    if(fullPayloadCache.has(key)) return;
    const generation = logsGeneration;
    let data;
    try { data = await fetchJson(`/api/logs/payload?id=${encodeURIComponent(key)}&generation=${encodeURIComponent(generation)}`); } catch (err) {
      const detail = document.getElementById('logs-panel-title');
      if(detail) detail.textContent = 'Payload unavailable; refresh after log rotation or reconnect.';
      return;
    }
    if(generation !== logsGeneration) return;
    fullPayloadCache.set(key, data);
    render();
  }

  function goToTraceFromCallId(callId){
    if(!callId) return;
    const runId = callToRunMap.get(callId);
    if(!runId) return;
    selectedRunId = runId;
    selectedCallId = callId;
    insightTab = 'flame';
    pushHistory(selectedRunId, selectedCallId);
    render();
  }

  function bindLogsControls(){
    const logSearchEl = document.getElementById('log-search');
    const logLevelEl = document.getElementById('log-level');
    const logLinkEl = document.getElementById('log-link-filter');
    const logViewModeEl = document.getElementById('log-view-mode');
    const payloadModeEl = document.getElementById('payload-mode');
    const logsViewportEl = document.getElementById('logs-viewport');
    const logsListWrapEl = document.getElementById('logs-list-wrap');
    const logsDetailEl = document.getElementById('logs-detail-col');
    if(logSearchEl) logSearchEl.oninput = (e)=>{
      logQuery = e.target.value || '';
      if(logSearchDebounce) clearTimeout(logSearchDebounce);
      logSearchDebounce = setTimeout(()=>{ renderLogsOnly(); }, 140);
    };
    if(logLevelEl) logLevelEl.onchange = (e)=>{ logLevelFilter = e.target.value || 'all'; renderLogsOnly(); };
    if(logLinkEl) logLinkEl.onchange = (e)=>{ logLinkFilter = e.target.value || 'all'; renderLogsOnly(); };
    if(logViewModeEl) logViewModeEl.onchange = (e)=>{ logViewMode = e.target.value || 'console'; renderLogsOnly(); };
    if(payloadModeEl) payloadModeEl.onchange = (e)=>{ payloadMode = e.target.value || 'pretty'; renderLogsOnly(); };
    if(logsViewportEl && !logsViewportEl.dataset.bound){
      logsViewportEl.dataset.bound = '1';
      logsViewportEl.addEventListener('scroll', ()=>{
        logScrollTop = logsViewportEl.scrollTop || 0;
        renderLogsRows();
      });
    }
    if(logsListWrapEl && !logsListWrapEl.dataset.bound){
      logsListWrapEl.dataset.bound = '1';
      logsListWrapEl.addEventListener('scroll', ()=>{
        logScrollTop = logsListWrapEl.scrollTop || 0;
        renderLogsRows();
      });
    }
    if(logsDetailEl && !logsDetailEl.dataset.bound){
      logsDetailEl.dataset.bound = '1';
      logsDetailEl.addEventListener('scroll', ()=>{
        logDetailScrollTop = logsDetailEl.scrollTop || 0;
      });
    }
  }

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
    if(action === 'select-log'){
      window.__selectLog(el.getAttribute('data-log-id') || null);
      return;
    }
    if(action === 'open-logs-tab'){
      insightTab = 'logs';
      render();
      return;
    }
    if(action === 'go-trace-from-log'){
      goToTraceFromCallId(el.getAttribute('data-call-id') || null);
      return;
    }
    if(action === 'load-log-payload'){
      loadLogPayload(el.getAttribute('data-log-id') || null);
      return;
    }
    if(action === 'copy-selected-log'){
      const selected = getSelectedVisibleLog();
      if(selected){
        window.__copyText(logPrimaryView(selected));
      }
      return;
    }
    if(action === 'copy-selected-log-call-id'){
      const selected = getSelectedVisibleLog();
      if(selected && selected.call_id){
        window.__copyText(String(selected.call_id));
      }
      return;
    }
    if(action === 'payload-expand-all'){
      setPayloadTreeExpansion(el.getAttribute('data-tree-id') || '', true);
      return;
    }
    if(action === 'payload-collapse-all'){
      setPayloadTreeExpansion(el.getAttribute('data-tree-id') || '', false);
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

  document.addEventListener('toggle', (e)=>{
    const el = e.target;
    if(!el || !el.matches || !el.matches('details.payload-tree-node')){
      return;
    }
    persistPayloadTreeStateForSelected();
  }, true);

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
  refreshBtn.addEventListener('click', ()=>fetchTree(true));
  minDurationEl.addEventListener('input', (e)=>{ minDurationMs = Number(e.target.value || 0); render(); });
  fnTypeEl.addEventListener('change', (e)=>{ fnTypeFilter = e.target.value || 'all'; render(); });
  sortModeEl.addEventListener('change', (e)=>{ sortMode = e.target.value || 'start'; render(); });
  togglePayloadsEl.addEventListener('change', (e)=>{ showPayloads = !!e.target.checked; render(); });
  runSearchEl.addEventListener('input', (e)=>{ runQuery = e.target.value || ''; renderRuns(); saveState(); });
  runGroupEl.addEventListener('change', (e)=>{ runGroupBy = e.target.value || 'none'; renderRuns(); saveState(); });
  runCompactEl.addEventListener('change', (e)=>{ runCompact = !!e.target.checked; renderRuns(); saveState(); });
  autoRefreshEl.addEventListener('change', (e)=>{
    viewRevision += 1;
    autoRefreshEnabled = !!e.target.checked;
    if(autoRefreshEnabled){
      // Resuming: drop the baseline and catch up immediately.
      pausedBaselineNodes = null;
      scheduleRefresh(true);
    } else {
      // Pausing freezes the view only. The server keeps reading the log and the
      // traced application keeps writing to it; we remember where we paused so
      // we can report how much arrived in the meantime.
      pausedBaselineNodes = snapshotCalls;
      pausedGeneration = snapshotGeneration;
      snapshotAt = Date.now()/1000;
      liveNodeCount = snapshotCalls;
    }
    renderStatusBar();
    saveState();
  });
  if(retryNowEl){
    retryNowEl.addEventListener('click', ()=>{
      consecutiveFailures = 0;
      setConnState('retrying', lastFetchError);
      if(autoRefreshEnabled) fetchTree(); else pollStatusOnly();
    });
  }
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

  // Polled while the view is paused: keeps connection state honest and counts
  // calls arriving in the background, without disturbing the frozen view.
  async function pollStatusOnly(){
    if(statusInFlight) return;
    statusInFlight = true;
    const revision = viewRevision;
    try {
      const data = await fetchJson('/api/status');
      if(revision !== viewRevision) return;
      consecutiveFailures = 0;
      lastSuccessAt = Date.now()/1000;
      sourceState = data.source || sourceState;
      liveNodeCount = sourceState.calls_started || 0;
      setConnState('ok', null);
    } catch (err) {
      consecutiveFailures += 1;
      const message = (err && err.message) ? err.message : String(err);
      setConnState(consecutiveFailures >= 3 ? 'down' : 'retrying', message);
    } finally { statusInFlight = false; }
  }

  function scheduleRefresh(immediate=false){
    if(refreshTimer) clearInterval(refreshTimer);
    refreshTimer = setInterval(()=>{
      if(document.hidden) return;
      // Paused stops rendering, not monitoring.
      if(autoRefreshEnabled) fetchTree(); else pollStatusOnly();
    }, 2500);
    if(immediate) fetchTree(true);
  }

  window.addEventListener('resize', ()=>{
    renderRuns();
    if(insightTab === 'logs'){
      renderLogsRows();
    }
  });

  // Re-poll as soon as the tab is visible again so a backgrounded viewer does
  // not sit on stale data while claiming to be connected.
  document.addEventListener('visibilitychange', ()=>{
    if(document.hidden) return;
    if(autoRefreshEnabled) fetchTree(); else pollStatusOnly();
  });

  loadState();
  syncControlState();
  renderStatusBar();
  fetchTree();
  scheduleRefresh();
  // Relative times and running-call elapsed counters advance every second
  // independently of network polling.
  statusTicker = setInterval(tickLiveUi, 1000);
})();
            """
        ).strip()

    @property
    def url(self) -> str:
        # 0.0.0.0 is a bind address, not something a browser can open.
        host = '127.0.0.1' if self.host in ('0.0.0.0', '::', '') else self.host
        if ':' in host and not host.startswith('['):
            host = f'[{host}]'
        return f'http://{host}:{self.port}'

    def _open_browser(self) -> None:
        """Open the viewer in a browser once the server is accepting connections."""
        import webbrowser
        try:
            webbrowser.open_new_tab(self.url)
        except Exception as exc:  # pragma: no cover - platform dependent
            print(f"Could not open a browser automatically ({exc}). Open {self.url} manually.", file=sys.stderr)

    def serve_forever(self, open_browser: bool = False) -> None:
        try:
            self._httpd = ThreadingHTTPServer((self.host, self.port), self._handler_factory())
            self.port = self._httpd.server_address[1]
        except OSError as exc:
            import errno
            if exc.errno in (errno.EADDRINUSE, errno.EACCES):
                if exc.errno == errno.EADDRINUSE:
                    print(
                        f"Error: port {self.port} on {self.host} is already in use.\n"
                        f"       Another viewer may already be running at {self.url}.\n"
                        f"       Use a different port:  pyeztrace serve {shlex.quote(str(self.log_file))} --port {self.port + 1 if self.port < 65535 else 8765}",
                        file=sys.stderr
                    )
                else:
                    print(
                        f"Error: not allowed to bind {self.host}:{self.port}.\n"
                        f"       Ports below 1024 usually require elevated privileges; "
                        f"try --port 8765.", file=sys.stderr
                    )
                raise SystemExit(1)
            raise

        source = self._builder.source_state()
        print(f"PyEzTrace Viewer serving on {self.url} (reading {self.log_file})", flush=True)
        if source['status'] == 'missing_file':
            print(f"  Waiting for {self.log_file} to appear - the viewer will pick it up automatically.")
        elif source['status'] in ('empty_file', 'no_records'):
            print("  Log file is empty so far. Waiting for the first trace record.")
        elif source['status'] == 'unparsable':
            print(
                f"  Warning: {source['unparsed_lines']} line(s) read, none are JSON trace records.\n"
                f"           Set EZTRACE_FILE_LOG_FORMAT=json in the traced app and restart it."
            )
        print("  Press Ctrl+C to stop.", flush=True)

        if open_browser:
            threading.Timer(0.3, self._open_browser).start()

        try:
            self._httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nViewer stopped.")
        finally:
            self._httpd.server_close()

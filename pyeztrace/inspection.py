"""Bounded, snapshot-based inspection shared by machine-facing consumers."""
import base64
import hashlib
import json
import os
import stat
from pathlib import Path

MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_OUTPUT_BYTES = 256 * 1024


class InspectionError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def encoded(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')


class Snapshot:
    def __init__(self, path):
        fd = os.open(Path(path), os.O_RDONLY | getattr(os, 'O_NONBLOCK', 0))
        with os.fdopen(fd, 'rb') as source:
            if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                raise InspectionError('invalid_source', 'Source must be a regular file')
            raw = source.read(MAX_FILE_BYTES + 1)
        if len(raw) > MAX_FILE_BYTES:
            raise InspectionError('source_too_large', 'Source exceeds 64 MiB; select a smaller log segment')
        self.fingerprint = hashlib.sha256(raw).hexdigest()
        self.source = {'sha256': self.fingerprint, 'bytes': len(raw)}
        self.warnings = []
        self.rows = []
        self.calls = {}
        self.runs = {}
        malformed = 0
        legacy_run = 'legacy'
        for number, line in enumerate(raw.splitlines(), 1):
            if number > 100000:
                raise InspectionError("too_many_records", "Source exceeds 100,000 records")
            if len(line) > 1024 * 1024:
                raise InspectionError("record_too_large", "A record exceeds 1 MiB")
            try:
                row = json.loads(line, parse_constant=lambda v: None)
                if not isinstance(row, dict) or not isinstance(row.get('data', {}), dict):
                    raise ValueError()
            except (ValueError, UnicodeError, RecursionError):
                malformed += 1
                continue
            version = row.get('schema_version')
            if version is not None and (type(version) is not int or version != 1):
                raise InspectionError('unsupported_schema', f'Unsupported record schema on line {number}')
            data = row.get('data', {})
            run = row.get('run_id') if version == 1 else legacy_run
            if not isinstance(run, str) or not run:
                malformed += 1
                continue
            row['_run'] = run
            row['_line'] = number
            self.rows.append(row)
            summary = self.runs.setdefault(run, {'run_id': run, 'name': None,
                                                'status': 'unknown', 'calls': 0})
            event = data.get('event')
            if event == 'run_start':
                summary.update(name=row.get('function'), status='incomplete')
            elif event in ('run_end', 'run_error'):
                summary['status'] = 'error' if event == 'run_error' else 'success'
            cid = data.get('call_id')
            if not isinstance(cid, str) or not cid:
                continue
            key = (run, cid)
            if key not in self.calls:
                self.calls[key] = {'call_id': cid, 'run_id': run, 'parent_id': None,
                                   'function': None, 'kind': data.get('kind', 'call'),
                                   'status': 'unknown', 'duration': None, 'records': []}
                summary['calls'] += 1
            call = self.calls[key]
            parent = data.get('parent_id')
            if isinstance(parent, str) and parent:
                call['parent_id'] = parent
            call['function'] = call['function'] or row.get('function')
            if event == 'start':
                call['status'] = 'incomplete'
            elif event in ('end', 'error'):
                call['status'] = data.get('status', 'error' if event == 'error' else 'success')
                call['duration'] = row.get('duration')
            call['records'].append(row)
        if malformed:
            self.warnings.append({'code': 'malformed_records', 'count': malformed})

    def find(self, cid, run=None):
        matches = [c for (rid, identity), c in self.calls.items()
                   if identity == cid and (run is None or rid == run)]
        if not matches:
            raise InspectionError('not_found', 'Call was not found')
        if len(matches) != 1:
            raise InspectionError('ambiguous_call', 'Call ID occurs in multiple runs; specify --run')
        return matches[0]

    @staticmethod
    def summary(call):
        return {k: v for k, v in call.items() if k != 'records'}

    def ancestors(self, call):
        path, seen = [], set()
        while call:
            if call['call_id'] in seen:
                raise InspectionError('cyclic_parent', 'Parent chain contains a cycle')
            seen.add(call['call_id'])
            path.append(self.summary(call))
            parent = call['parent_id']
            call = self.calls.get((call['run_id'], parent))
            if parent and call is None:
                self.warnings.append({'code': 'missing_parent', 'call_id': parent})
        return list(reversed(path))

    def page(self, values, query, limit, cursor):
        offset = 0
        if cursor:
            if len(cursor) > 4096:
                raise InspectionError("invalid_cursor", "Cursor exceeds size limit")
            try:
                token = json.loads(base64.urlsafe_b64decode(cursor.encode()))
                if token['source'] != self.fingerprint or token['query'] != query:
                    raise InspectionError('stale_cursor', 'Source or query changed; restart pagination')
                offset = token['offset']
                if type(offset) is not int or not 0 <= offset <= len(values):
                    raise ValueError()
            except InspectionError:
                raise
            except (ValueError, KeyError, TypeError, UnicodeError):
                raise InspectionError('invalid_cursor', 'Invalid pagination cursor') from None
        end = min(offset + limit, len(values))
        next_cursor = None
        if end < len(values):
            next_cursor = base64.urlsafe_b64encode(encoded(
                {'source': self.fingerprint, 'query': query, 'offset': end})).decode()
        return values[offset:end], next_cursor

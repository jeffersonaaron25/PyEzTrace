"""Versioned JSON CLI without changing the legacy print/serve contracts."""
import argparse
import json
import sys

from pyeztrace.inspection import InspectionError, Snapshot, encoded, MAX_OUTPUT_BYTES

COMMANDS = {'runs', 'tree', 'call', 'path', 'errors', 'payload'}


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise InspectionError('invalid_arguments', message)


def main(argv):
    command = argv[0]
    try:
        parser = Parser(prog=f'pyeztrace {command}', allow_abbrev=False)
        parser.add_argument('log_file')
        if command in ('call', 'path', 'payload'):
            parser.add_argument('call_id')
        parser.add_argument('--run')
        parser.add_argument('--format', choices=['json', 'text'], default='json')
        if command in ('runs', 'tree', 'errors'):
            parser.add_argument('--limit', type=int, default=100)
            parser.add_argument('--cursor')
        if command == 'payload':
            parser.add_argument('--max-bytes', type=int, default=8192)
        args = parser.parse_args(argv[1:])
        if hasattr(args, 'limit') and not 1 <= args.limit <= 1000:
            raise InspectionError('invalid_arguments', '--limit must be between 1 and 1000')
        if hasattr(args, 'max_bytes') and not 1 <= args.max_bytes <= 65536:
            raise InspectionError('invalid_arguments', '--max-bytes must be between 1 and 65536')
        snapshot = Snapshot(args.log_file)
        cursor = None
        if command in ('call', 'path', 'payload'):
            call = snapshot.find(args.call_id, args.run)
            if command == 'call':
                value = snapshot.summary(call)
            elif command == 'path':
                value = snapshot.ancestors(call)
            else:
                payload = [{'line': r['_line'], 'message': r.get('message'), 'data': r.get('data')}
                           for r in call['records']]
                raw = encoded(payload)
                value = {'call_id': call['call_id'], 'run_id': call['run_id'],
                         'bytes': len(raw), 'truncated': len(raw) > args.max_bytes,
                         'encoding': 'utf-8', 'content': raw[:args.max_bytes].decode('utf-8', errors='ignore')}
        else:
            if command == 'runs':
                values = [r for r in snapshot.runs.values() if args.run is None or r['run_id'] == args.run]
            else:
                values = [snapshot.summary(c) for c in snapshot.calls.values()
                          if (args.run is None or c['run_id'] == args.run)
                          and (command != 'errors' or c['status'] == 'error')]
            query = {'command': command, 'run': args.run}
            value, cursor = snapshot.page(values, query, args.limit, args.cursor)
        result = {'schema_version': 1, 'command': command, 'data': value,
                  'source': snapshot.source, 'warnings': snapshot.warnings, 'next_cursor': cursor}
        raw = encoded(result)
        if len(raw) > MAX_OUTPUT_BYTES:
            raise InspectionError('output_too_large', 'Output exceeds 256 KiB; lower --limit or use payload')
        output = raw.decode() if args.format == 'json' else json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False)
        if len(output.encode('utf-8')) > MAX_OUTPUT_BYTES:
            raise InspectionError('output_too_large', 'Output exceeds 256 KiB; lower --limit')
        sys.stdout.write(output + '\n')
        return 0
    except BrokenPipeError:
        return 0
    except (OSError, InspectionError, RecursionError, UnicodeError) as exc:
        code = getattr(exc, 'code', 'io_error' if isinstance(exc, OSError) else 'invalid_record')
        sys.stderr.write(json.dumps({'schema_version': 1, 'error': {'code': code, 'message': str(exc)}}) + '\n')
        return 2 if code == 'invalid_arguments' else 1

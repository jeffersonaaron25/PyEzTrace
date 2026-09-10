#!/usr/bin/env python3
"""Command-line interface for PyEzTrace log analysis and viewer."""

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import re
import os
import sys

from pyeztrace._version import get_version


class LogAnalyzer:
    def __init__(self, log_file: Path):
        self.log_file = log_file
        self._ansi_pattern = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
        
    def parse_logs(self, filter_level: Optional[str] = None, 
                   since: Optional[datetime] = None,
                   until: Optional[datetime] = None,
                   context: Optional[dict] = None) -> List[dict]:
        """Parse and filter log entries."""
        entries = []
        
        with open(self.log_file, 'r') as f:
            for line in f:
                try:
                    entry = self._parse_line(line.strip())
                    if self._should_include(entry, filter_level, since, until, context):
                        entries.append(entry)
                except (ValueError, TypeError, KeyError):
                    continue  # Skip invalid lines
                    
        return entries
    
    def read_formatted_lines(self, filter_level: Optional[str] = None,
                            since: Optional[datetime] = None,
                            until: Optional[datetime] = None,
                            context: Optional[dict] = None) -> List[str]:
        """Read original formatted lines from log file, preserving ANSI codes and tree structure."""
        formatted_lines = []
        
        with open(self.log_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                original_line = line.rstrip('\n\r')
                if not original_line.strip():
                    continue
                    
                try:
                    # Parse to check filters (strip ANSI for parsing)
                    entry = self._parse_line(self._strip_ansi_codes(original_line))
                    if self._should_include(entry, filter_level, since, until, context):
                        formatted_lines.append(original_line)
                except:
                    # If parsing fails, include the line anyway (might be a non-standard format)
                    if not any((filter_level, since, until, context)):
                        formatted_lines.append(original_line)
                    
        return formatted_lines
    
    def format_json_entry(self, entry: dict, call_hierarchy: Optional[dict] = None) -> str:
        """Reconstruct formatted output from JSON log entry."""
        # Import here to avoid circular dependency
        from pyeztrace.custom_logging import Logging
        
        timestamp = entry.get('timestamp', '')
        level = entry.get('level', 'INFO')
        project = entry.get('project', '')
        function = entry.get('function', '')
        message = entry.get('message', '')
        duration = entry.get('duration')
        data = entry.get('data', {})
        fn_type = entry.get('fn_type', '')
        event = data.get('event', '')
        
        # Check if this is a metrics entry
        is_metrics = (event == 'metrics_summary' or fn_type == 'metrics') and data.get('metrics')
        
        if is_metrics:
            # Format metrics entry with metrics table
            color = Logging.COLOR_CODES.get(level, '')
            reset = Logging.COLOR_CODES['RESET']
            
            # Header line
            msg = f"{color}{timestamp} - {level} - [{project}] {message}{reset}\n"
            
            # Metrics summary
            total_functions = data.get('total_functions', 0)
            total_calls = data.get('total_calls', 0)
            metrics_list = data.get('metrics', [])
            
            if metrics_list:
                msg += f"  Functions: {total_functions}, Total calls: {total_calls}\n"
                msg += f"  {'Function':<40} {'Calls':>8} {'Total Time':>12} {'Avg Time':>12} {'Time/Call':>12}\n"
                msg += f"  {'-' * 40} {'-' * 8} {'-' * 12} {'-' * 12} {'-' * 12}\n"
                
                for metric in metrics_list:
                    func_name = metric.get('function', '')
                    calls = metric.get('calls', 0)
                    total_seconds = metric.get('total_seconds', 0.0)
                    avg_seconds = metric.get('avg_seconds', 0.0)
                    time_per_call = (total_seconds / calls * 1000) if calls > 0 else 0.0
                    
                    # Truncate function name if too long
                    if len(func_name) > 38:
                        func_name = func_name[:35] + "..."
                    
                    msg += f"  {func_name:<40} {calls:>8} {total_seconds:>12.6f}s {avg_seconds:>12.6f}s {time_per_call:>12.3f}ms\n"
            
            return msg.rstrip()
        
        # Try to determine level_indent from data or call hierarchy
        level_indent = 0
        call_id = data.get('call_id')
        
        if call_hierarchy and call_id:
            # Calculate depth from call hierarchy
            depth = 0
            current_id = call_id
            visited = set()
            while current_id and current_id in call_hierarchy and current_id not in visited:
                visited.add(current_id)
                depth += 1
                current_id = call_hierarchy[current_id]
            level_indent = depth
        else:
            # Try to get from data fields (might be stored as 'depth' or 'level_indent')
            level_indent = data.get('depth', data.get('level_indent', 0))
            if isinstance(level_indent, str):
                try:
                    level_indent = int(level_indent)
                except:
                    level_indent = 0
            elif not isinstance(level_indent, int):
                level_indent = 0
        
        # Build tree structure
        if level_indent == 0:
            tree = ""
        elif level_indent == 1:
            tree = "├──"
        else:
            tree = "│    " * (level_indent - 1) + "├───"
        
        # Get color codes
        color = Logging.COLOR_CODES.get(level, '')
        reset = Logging.COLOR_CODES['RESET']
        
        # Format message
        msg = f"{color}{timestamp} - {level} - [{project}] {tree} {function} {message}{reset}"
        if duration is not None:
            msg += f" (took {duration:.5f} seconds)"
        
        return msg
    
    def build_call_hierarchy(self, entries: List[dict]) -> dict:
        """Build a call hierarchy map from entries: {call_id: parent_id}."""
        hierarchy = {}
        for entry in entries:
            data = entry.get('data', {})
            call_id = data.get('call_id')
            parent_id = data.get('parent_id')
            if call_id and parent_id:
                hierarchy[call_id] = parent_id
        return hierarchy
    
    def analyze_performance(self, function_name: Optional[str] = None, **filters) -> dict:
        """Analyze performance metrics from logs."""
        metrics = {}
        entries = self.parse_logs(**filters)
        
        for entry in entries:
            if not isinstance(entry.get('duration'), (int, float)) or isinstance(entry.get('duration'), bool) or not math.isfinite(entry['duration']):
                continue
                
            func = entry.get('function', 'unknown')
            if function_name and func != function_name:
                continue
                
            if func not in metrics:
                metrics[func] = {
                    'count': 0,
                    'total_time': 0,
                    'min_time': float('inf'),
                    'max_time': 0,
                }
                
            m = metrics[func]
            duration = float(entry['duration'])
            m['count'] += 1
            m['total_time'] += duration
            m['min_time'] = min(m['min_time'], duration)
            m['max_time'] = max(m['max_time'], duration)
            
        # Calculate averages
        for m in metrics.values():
            m['avg_time'] = m['total_time'] / m['count']
            
        return metrics
    
    def find_errors(self, since: Optional[datetime] = None) -> List[dict]:
        """Find error entries in logs."""
        return self.parse_logs(filter_level="ERROR", since=since)
    
    def _parse_line(self, line: str) -> dict:
        """Parse a single log line."""
        line = self._strip_ansi_codes(line)
        if not line:
            raise ValueError("Empty log line")

        try:
            # Try JSON format first
            entry = json.loads(line, parse_constant=lambda _: None,
                               parse_float=lambda value: float(value) if math.isfinite(float(value)) else None)
            if not isinstance(entry, dict) or not isinstance(entry.get('data', {}), dict):
                raise ValueError('Invalid log record')
            return entry
        except:
            # Fall back to parsing other formats
            return self._parse_plain_format(line)

    def _parse_plain_format(self, line: str) -> dict:
        """Parse plain text format."""
        pattern = (
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\s*-\s*'
            r'(\w+)\s*-\s*'
            r'(?:\[([^\]]+)\]\s*)?'
            r'(.*)'
        )
        match = re.match(pattern, line)
        if not match:
            raise ValueError("Invalid log format")

        timestamp, level, project, rest = match.groups()
        return {
            'timestamp': timestamp,
            'level': level,
            'project': project,
            'message': rest.strip()
        }

    def _strip_ansi_codes(self, line: str) -> str:
        """Remove ANSI color codes from a log line."""
        return self._ansi_pattern.sub('', line).strip()
    
    def _should_include(self, entry: dict, 
                       filter_level: Optional[str] = None,
                       since: Optional[datetime] = None,
                       until: Optional[datetime] = None,
                       context: Optional[dict] = None) -> bool:
        """Check if log entry matches filters."""
        if filter_level and entry.get('level') != filter_level:
            return False
            
        timestamp = datetime.fromisoformat(entry['timestamp'].replace('Z', '+00:00'))
        if since and timestamp.astimezone() < since.astimezone():
            return False
        if until and timestamp.astimezone() > until.astimezone():
            return False
            
        if context:
            entry_context = entry.get('data', {})
            return all(entry_context.get(k) == v for k, v in context.items())
            
        return True

def _port(value):
    try:
        port = int(value)
        if 0 <= port <= 65535:
            return port
    except (TypeError, ValueError):
        pass
    raise argparse.ArgumentTypeError("port must be between 0 and 65535")


def main():
    """Main entry point for the pyeztrace CLI command."""
    parser = argparse.ArgumentParser(
        description="PyEzTrace Log Analyzer and Viewer",
        prog="pyeztrace", allow_abbrev=False
    )
    
    # Add version argument (must be before subparsers)
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {get_version()}'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Analyze / print subcommand (default)
    parser_print = subparsers.add_parser('print', help='Print or analyze logs', allow_abbrev=False)
    parser_print.add_argument('log_file', type=Path, help="Path to log file")
    parser_print.add_argument('--level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                      help="Filter by log level")
    parser_print.add_argument('--since', type=str, help="Show logs since (YYYY-MM-DD[THH:MM:SS])")
    parser_print.add_argument('--until', type=str, help="Show logs until (YYYY-MM-DD[THH:MM:SS])")
    parser_print.add_argument('--context', type=str, help="Filter by context (key=value[,key=value])")
    output_mode = parser_print.add_mutually_exclusive_group()
    output_mode.add_argument('--analyze', action='store_true', help="Show performance metrics")
    parser_print.add_argument('--function', type=str, help="Analyze specific function")
    output_mode.add_argument('--errors', action='store_true', help="Show only errors")
    parser_print.add_argument('--format', choices=['text', 'json'], default='text',
                      help="Output format")
    parser_print.add_argument('--color', choices=['auto', 'always', 'never'], default='auto',
                              help='ANSI color policy (auto honors NO_COLOR and redirected output)')
    parser_print.set_defaults(func=_cmd_print)

    # Serve subcommand
    parser_serve = subparsers.add_parser(
        'serve', help='Run interactive viewer server', allow_abbrev=False,
        description='Watch JSON file logs; a missing file is picked up when it appears.',
        epilog='Enable file output in the traced app with EZTRACE_DISABLE_FILE_LOGGING=0 '
               'and EZTRACE_FILE_LOG_FORMAT=json. Set EZTRACE_LOG_FILE to the watched path.')
    parser_serve.add_argument('log_file', type=Path, help='Path to JSON-formatted log file')
    parser_serve.add_argument('--host', type=str, default=os.environ.get('EZTRACE_VIEW_HOST', '127.0.0.1'))
    parser_serve.add_argument('--port', type=_port, default=os.environ.get('EZTRACE_VIEW_PORT', '8765'))
    parser_serve.add_argument('--open', dest='open_browser', action='store_true',
                              help='Open the viewer in your default browser once the server is up')
    parser_serve.set_defaults(func=_cmd_serve)

    args = parser.parse_args()

    # If no command provided, show help (backward compatibility removed due to subparser conflicts)
    if not getattr(args, 'command', None):
        parser.print_help()
        return

    # Use the function-based approach for subcommands
    if hasattr(args, 'func'):
        try:
            return args.func(args)
        except BrokenPipeError:
            return 0
        except OSError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        except ValueError as exc:
            parser.error(str(exc))
    else:
        parser.print_help()


def _cmd_print(args):
    # Parse datetime arguments
    since = datetime.fromisoformat(args.since.replace('Z', '+00:00')) if getattr(args, 'since', None) else None
    until = datetime.fromisoformat(args.until.replace('Z', '+00:00')) if getattr(args, 'until', None) else None

    # Parse context filters
    context = {}
    if getattr(args, 'context', None):
        for pair in args.context.split(','):
            if '=' not in pair or not pair.split('=', 1)[0].strip():
                raise ValueError('--context requires key=value[,key=value]')
            key, value = pair.split('=', 1)
            context[key.strip()] = value.strip()

    log_file = getattr(args, 'log_file', None)
    if log_file is None:
        print("Error: No log file specified", file=sys.stderr)
        return 1
        
    if since and until and since.astimezone() > until.astimezone():
        raise ValueError('--since must not be later than --until')
    if getattr(args, 'function', None) and not getattr(args, 'analyze', False):
        raise ValueError('--function requires --analyze')
    analyzer = LogAnalyzer(log_file.expanduser())
    log_file = analyzer.log_file
    filters = dict(filter_level=getattr(args, 'level', None), since=since, until=until, context=context)
    color = getattr(args, 'color', 'auto')
    use_color = color == 'always' or (color == 'auto' and sys.stdout.isatty() and 'NO_COLOR' not in os.environ)

    if getattr(args, 'analyze', False):
        metrics = analyzer.analyze_performance(getattr(args, 'function', None), **filters)
        if getattr(args, 'format', 'text') == 'json':
            print(json.dumps(metrics, indent=2))
        else:
            for func, m in metrics.items():
                print(f"\nFunction: {func}")
                print(f"  Calls:     {m['count']}")
                print(f"  Total:     {m['total_time']:.3f}s")
                print(f"  Average:   {m['avg_time']:.3f}s")
                print(f"  Min:       {m['min_time']:.3f}s")
                print(f"  Max:       {m['max_time']:.3f}s")
        return

    if getattr(args, 'errors', False):
        errors = [entry for entry in analyzer.parse_logs(**filters) if entry.get('level') == 'ERROR']
        if getattr(args, 'format', 'text') == 'json':
            print(json.dumps(errors, indent=2))
        else:
            for error in errors:
                print(f"\n{error['timestamp']} - {error['message']}")
                if 'data' in error:
                    print(f"Context: {json.dumps(error['data'], indent=2)}")
        return

    # Check if log file contains JSON format
    is_json_format = False
    try:
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            first_line = f.readline().strip()
            if first_line:
                try:
                    json.loads(first_line)
                    is_json_format = True
                except:
                    pass
    except:
        pass
    
    if getattr(args, 'format', 'text') == 'json':
        entries = analyzer.parse_logs(getattr(args, 'level', None), since, until, context)
        print(json.dumps(entries, indent=2))
    elif is_json_format:
        # For JSON format logs, reconstruct formatted output
        entries = analyzer.parse_logs(getattr(args, 'level', None), since, until, context)
        # Build call hierarchy for depth calculation
        call_hierarchy = analyzer.build_call_hierarchy(entries)
        for entry in entries:
            formatted = analyzer.format_json_entry(entry, call_hierarchy)
            print(formatted if use_color else analyzer._strip_ansi_codes(formatted))
    else:
        # For color/plain format logs, preserve original formatting
        formatted_lines = analyzer.read_formatted_lines(
            getattr(args, 'level', None), since, until, context
        )
        for line in formatted_lines:
            print(line if use_color else analyzer._strip_ansi_codes(line))


def _cmd_serve(args):
    # Get log_file from args
    log_file = getattr(args, 'log_file', None)
    if log_file is None:
        print("Error: No log file specified", file=sys.stderr)
        return 1

    from pyeztrace.viewer import TraceViewerServer
    server = TraceViewerServer(log_file, host=args.host, port=args.port)
    # The viewer tolerates a missing or not-yet-written log file: it reports what
    # it is waiting for, here and in the browser, and starts rendering as soon as
    # trace records appear.
    server.serve_forever(open_browser=getattr(args, 'open_browser', False))

if __name__ == '__main__':
    sys.exit(main())

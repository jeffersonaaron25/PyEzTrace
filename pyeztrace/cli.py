#!/usr/bin/env python3
"""Command-line interface for PyEzTrace log analysis and viewer."""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import re
import os
import sys

def _get_version():
    """Get the package version dynamically."""
    try:
        # Python 3.8+ standard library approach
        from importlib.metadata import version
        return version('pyeztrace')
    except ImportError:
        # Fallback for Python < 3.8
        try:
            import pkg_resources
            return pkg_resources.get_distribution('pyeztrace').version
        except Exception:
            import tomllib
            try:
                with open('pyproject.toml', 'rb') as f:
                    data = tomllib.load(f)
                    return data['project']['version']
            except Exception:
                return 'unknown'

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
                except:
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
            while current_id and current_id in call_hierarchy:
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
    
    def analyze_performance(self, function_name: Optional[str] = None) -> dict:
        """Analyze performance metrics from logs."""
        metrics = {}
        entries = self.parse_logs()
        
        for entry in entries:
            if 'duration' not in entry:
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
            return json.loads(line)
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
            
        timestamp = datetime.fromisoformat(entry['timestamp'])
        if since and timestamp < since:
            return False
        if until and timestamp > until:
            return False
            
        if context:
            entry_context = entry.get('data', {})
            return all(entry_context.get(k) == v for k, v in context.items())
            
        return True

def main():
    """Main entry point for the pyeztrace CLI command."""
    parser = argparse.ArgumentParser(
        description="PyEzTrace Log Analyzer and Viewer",
        prog="pyeztrace"
    )
    
    # Add version argument (must be before subparsers)
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {_get_version()}'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Analyze / print subcommand (default)
    parser_print = subparsers.add_parser('print', help='Print or analyze logs')
    parser_print.add_argument('log_file', type=Path, help="Path to log file")
    parser_print.add_argument('--level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                      help="Filter by log level")
    parser_print.add_argument('--since', type=str, help="Show logs since (YYYY-MM-DD[THH:MM:SS])")
    parser_print.add_argument('--until', type=str, help="Show logs until (YYYY-MM-DD[THH:MM:SS])")
    parser_print.add_argument('--context', type=str, help="Filter by context (key=value[,key=value])")
    parser_print.add_argument('--analyze', action='store_true', help="Show performance metrics")
    parser_print.add_argument('--function', type=str, help="Analyze specific function")
    parser_print.add_argument('--errors', action='store_true', help="Show only errors")
    parser_print.add_argument('--format', choices=['text', 'json'], default='text',
                      help="Output format")
    parser_print.set_defaults(func=_cmd_print)

    # Serve subcommand
    parser_serve = subparsers.add_parser('serve', help='Run interactive viewer server')
    parser_serve.add_argument('log_file', type=Path, help='Path to JSON-formatted log file')
    parser_serve.add_argument('--host', type=str, default=os.environ.get('EZTRACE_VIEW_HOST', '127.0.0.1'))
    parser_serve.add_argument('--port', type=int, default=int(os.environ.get('EZTRACE_VIEW_PORT', '8765')))
    parser_serve.set_defaults(func=_cmd_serve)

    # Backward compatible arguments (no subcommand -> treat as print)
    parser.add_argument('--level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help=argparse.SUPPRESS)
    parser.add_argument('--since', type=str, help=argparse.SUPPRESS)
    parser.add_argument('--until', type=str, help=argparse.SUPPRESS)
    parser.add_argument('--context', type=str, help=argparse.SUPPRESS)
    parser.add_argument('--analyze', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--function', type=str, help=argparse.SUPPRESS)
    parser.add_argument('--errors', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--format', choices=['text', 'json'], default='text', help=argparse.SUPPRESS)
    # Note: log_file argument removed from main parser to avoid conflicts with subparsers
    
    args = parser.parse_args()

    # If no command provided, show help (backward compatibility removed due to subparser conflicts)
    if not getattr(args, 'command', None):
        parser.print_help()
        return

    # Use the function-based approach for subcommands
    if hasattr(args, 'func'):
        return args.func(args)
    else:
        parser.print_help()


def _cmd_print(args):
    # Parse datetime arguments
    since = datetime.fromisoformat(args.since) if getattr(args, 'since', None) else None
    until = datetime.fromisoformat(args.until) if getattr(args, 'until', None) else None

    # Parse context filters
    context = {}
    if getattr(args, 'context', None):
        for pair in args.context.split(','):
            key, value = pair.split('=')
            context[key.strip()] = value.strip()

    log_file = getattr(args, 'log_file', None)
    if log_file is None:
        print("Error: No log file specified")
        return 1
        
    analyzer = LogAnalyzer(log_file)

    if getattr(args, 'analyze', False):
        metrics = analyzer.analyze_performance(getattr(args, 'function', None))
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
        errors = analyzer.find_errors(since)
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
            print(formatted)
    else:
        # For color/plain format logs, preserve original formatting
        formatted_lines = analyzer.read_formatted_lines(
            getattr(args, 'level', None), since, until, context
        )
        for line in formatted_lines:
            print(line)


def _cmd_serve(args):
    # Ensure JSON file logging is used
    print("Note: The viewer expects the log file in JSON format. Set EZTRACE_FILE_LOG_FORMAT=json (or EZTRACE_LOG_FORMAT=json) before running your app.")
    
    # Get log_file from args
    log_file = getattr(args, 'log_file', None)
    if log_file is None:
        print("Error: No log file specified")
        return 1
        
    from pyeztrace.viewer import TraceViewerServer
    server = TraceViewerServer(log_file, host=args.host, port=args.port)
    server.serve_forever()

if __name__ == '__main__':
    main()

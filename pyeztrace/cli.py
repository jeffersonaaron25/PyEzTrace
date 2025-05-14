#!/usr/bin/env python3
"""Command-line interface for PyEzTrace log analysis."""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import re

class LogAnalyzer:
    def __init__(self, log_file: Path):
        self.log_file = log_file
        
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
        try:
            # Try JSON format first
            return json.loads(line)
        except:
            # Fall back to parsing other formats
            return self._parse_plain_format(line)
    
    def _parse_plain_format(self, line: str) -> dict:
        """Parse plain text format."""
        pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}) - (\w+) - \[([^\]]+)\](.*)'
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
    parser = argparse.ArgumentParser(description="PyEzTrace Log Analyzer")
    parser.add_argument('log_file', type=Path, help="Path to log file")
    parser.add_argument('--level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                      help="Filter by log level")
    parser.add_argument('--since', type=str, help="Show logs since (YYYY-MM-DD[THH:MM:SS])")
    parser.add_argument('--until', type=str, help="Show logs until (YYYY-MM-DD[THH:MM:SS])")
    parser.add_argument('--context', type=str, help="Filter by context (key=value[,key=value])")
    parser.add_argument('--analyze', action='store_true', help="Show performance metrics")
    parser.add_argument('--function', type=str, help="Analyze specific function")
    parser.add_argument('--errors', action='store_true', help="Show only errors")
    parser.add_argument('--format', choices=['text', 'json'], default='text',
                      help="Output format")
    
    args = parser.parse_args()
    
    # Parse datetime arguments
    since = datetime.fromisoformat(args.since) if args.since else None
    until = datetime.fromisoformat(args.until) if args.until else None
    
    # Parse context filters
    context = {}
    if args.context:
        for pair in args.context.split(','):
            key, value = pair.split('=')
            context[key.strip()] = value.strip()
    
    analyzer = LogAnalyzer(args.log_file)
    
    if args.analyze:
        metrics = analyzer.analyze_performance(args.function)
        if args.format == 'json':
            print(json.dumps(metrics, indent=2))
        else:
            for func, m in metrics.items():
                print(f"\nFunction: {func}")
                print(f"  Calls:     {m['count']}")
                print(f"  Total:     {m['total_time']:.3f}s")
                print(f"  Average:   {m['avg_time']:.3f}s")
                print(f"  Min:       {m['min_time']:.3f}s")
                print(f"  Max:       {m['max_time']:.3f}s")
    
    elif args.errors:
        errors = analyzer.find_errors(since)
        if args.format == 'json':
            print(json.dumps(errors, indent=2))
        else:
            for error in errors:
                print(f"\n{error['timestamp']} - {error['message']}")
                if 'data' in error:
                    print(f"Context: {json.dumps(error['data'], indent=2)}")
    
    else:
        entries = analyzer.parse_logs(args.level, since, until, context)
        if args.format == 'json':
            print(json.dumps(entries, indent=2))
        else:
            for entry in entries:
                print(f"{entry['timestamp']} - {entry['level']} - {entry['message']}")

if __name__ == '__main__':
    main()

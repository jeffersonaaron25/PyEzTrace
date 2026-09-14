"""Verify installed PyEzTrace through a real, local-only OTLP Collector.

Requires pyeztrace[otel] and a Collector binary with OTLP receiver/file exporter.
The script starts and stops its own collector; results remain in a temporary
folder for inspection. No external telemetry destination is configured.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time


APP = '''
import asyncio
from pyeztrace import trace, get_otel_status
from pyeztrace.setup import Setup
from pyeztrace.custom_logging import Logging
Setup.initialize('collector-smoke', disable_file_logging=True)
@trace()
async def worker(number):
    await asyncio.sleep(0.01)
    return number * 2
@trace()
async def failing():
    raise ValueError('synthetic failure')
@trace()
async def main():
    assert await asyncio.gather(worker(1), worker(2)) == [2, 4]
    try:
        await failing()
    except ValueError:
        pass
asyncio.run(main())
Logging.flush_logs()
status = get_otel_status()
assert status['enabled'] and status['initialized'], status
assert status['error'] is None, status
print(status)
# Normal interpreter shutdown flushes the SDK's batch span processor.
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--collector', required=True, type=Path)
    args = parser.parse_args()
    collector = args.collector.resolve(strict=True)
    root = Path(tempfile.mkdtemp(prefix='pyeztrace-otel-smoke-'))
    traces = root / 'spans.jsonl'
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    config = {
        'receivers': {'otlp': {'protocols': {'http': {'endpoint': f'127.0.0.1:{port}'}}}},
        'exporters': {'file': {'path': str(traces)}},
        'service': {
            'telemetry': {'metrics': {'level': 'none'}},
            'pipelines': {'traces': {'receivers': ['otlp'], 'exporters': ['file']}},
        },
    }
    config_path = root / 'collector.yaml'
    config_path.write_text(json.dumps(config, indent=2))
    app = root / 'app.py'
    app.write_text(APP)
    env = {'PATH': os.environ.get('PATH', ''), 'HOME': str(root),
           'NO_PROXY': '127.0.0.1,localhost'}
    with (root / 'collector.log').open('w') as output:
        process = subprocess.Popen([str(collector), '--config', str(config_path)],
                                   env=env, stdout=output, stderr=output)
        try:
            deadline = time.monotonic() + 15
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f'Collector stopped; see {root / "collector.log"}')
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('Collector did not become ready')
                    time.sleep(0.05)
            result = subprocess.run(
                [sys.executable, str(app)], cwd=root, capture_output=True, text=True,
                timeout=30, env={**env, 'EZTRACE_OTEL_ENABLED': 'true',
                                'EZTRACE_OTEL_EXPORTER': 'otlp',
                                'EZTRACE_SERVICE_NAME': 'collector-smoke',
                                'EZTRACE_OTLP_ENDPOINT': f'http://127.0.0.1:{port}/v1/traces'})
            (root / 'application.log').write_text(result.stdout + result.stderr)
            assert result.returncode == 0, result.stderr
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    batches = [json.loads(line) for line in traces.read_text().splitlines()]
    resources = [r for batch in batches for r in batch['resourceSpans']]
    spans = [s for resource in resources for scope in resource['scopeSpans']
             for s in scope['spans']]
    assert len(spans) == 4, spans
    parent = next(s for s in spans if s['name'] == 'main')
    children = [s for s in spans if s['name'] != 'main']
    assert sorted(s['name'] for s in children) == ['failing', 'worker', 'worker']
    assert all(s['traceId'] == parent['traceId'] and
               s['parentSpanId'] == parent['spanId'] for s in children)
    failure = next(s for s in children if s['name'] == 'failing')
    assert failure['status']['code'] == 2
    assert any(event['name'] == 'exception' for event in failure['events'])
    assert all(int(s['endTimeUnixNano']) >= int(s['startTimeUnixNano']) for s in spans)
    for resource in resources:
        attributes = {a['key']: a['value'] for a in resource['resource']['attributes']}
        assert attributes['service.name']['stringValue'] == 'collector-smoke'
    print('PASS: 4 spans received by the real Collector, correct parent/child IDs,')
    print('      async work, exception event/error status, service name, and shutdown export.')
    print(f'Evidence: {root}')


if __name__ == '__main__':
    main()

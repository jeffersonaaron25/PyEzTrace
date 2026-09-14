"""Public diagnostics and local viewer startup contracts."""
import json
import os
import subprocess
import sys
import threading
import urllib.request


def test_public_status_does_not_load_optional_sdk_or_initialize():
    code = '''
import json, sys
from pyeztrace import get_otel_status
status = get_otel_status()
assert not status['initialized'] and not status['enabled']
assert not any(k == 'opentelemetry' or k.startswith('opentelemetry.') for k in sys.modules)
print(json.dumps(status))
'''
    result = subprocess.run([sys.executable, '-c', code], capture_output=True,
                            text=True, timeout=15,
                            env={**os.environ, 'EZTRACE_OTEL_ENABLED': 'true'})
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['otel_enabled_env'] == 'true'


def test_viewer_serves_without_reverse_dns(monkeypatch, tmp_path):
    from pyeztrace.viewer import TraceViewerServer, _ViewerHTTPServer

    def unavailable(*args):
        raise AssertionError('viewer must not require reverse DNS')

    monkeypatch.setattr('socket.getfqdn', unavailable)
    viewer = TraceViewerServer(tmp_path / 'missing.log', port=0)
    server = _ViewerHTTPServer(('127.0.0.1', 0), viewer._handler_factory())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with urllib.request.urlopen(
            f'http://127.0.0.1:{server.server_port}/api/status', timeout=5
        ) as response:
            assert json.load(response)['source']['status'] == 'missing_file'
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)

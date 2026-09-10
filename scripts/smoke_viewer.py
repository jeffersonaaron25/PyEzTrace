"""Installed-package smoke: real tracing, CLI, HTTP, partial writes and rotation.

Run with the Python interpreter whose installed pyeztrace package you want to test.
Only standard-library dependencies are required. Temporary files are removed.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import urllib.error
import urllib.request

from pyeztrace.viewer import TraceViewerServer

APP = '''
import asyncio
from pyeztrace import trace
from pyeztrace.setup import Setup
from pyeztrace.custom_logging import Logging
Setup.initialize("SMOKE", disable_file_logging=False, file_format="json")
@trace()
async def child(number):
    await asyncio.sleep(0.01)
    if number == 2:
        raise ValueError("expected smoke error")
    return {"value": number, "unicode": "café"}
@trace()
async def main():
    with Logging().with_context(tenant="smoke"):
        await asyncio.gather(*(child(n) for n in range(4)), return_exceptions=True)
asyncio.run(main())
'''


def main():
    with tempfile.TemporaryDirectory(prefix='pyeztrace smoke ') as directory:
        root = Path(directory)
        log = root / 'trace with spaces.log'
        viewer = TraceViewerServer(log, port=0)
        # Bind synchronously so readiness is observed, not guessed with a sleep.
        from http.server import ThreadingHTTPServer
        viewer._httpd = ThreadingHTTPServer((viewer.host, 0), viewer._handler_factory())
        viewer.port = viewer._httpd.server_address[1]
        thread = threading.Thread(target=viewer._httpd.serve_forever, daemon=True)
        thread.start()
        def get(route):
            with urllib.request.urlopen(viewer.url + route, timeout=5) as response:
                raw = response.read()
                return json.loads(raw) if route.startswith('/api/') else raw
        def cli(*args):
            result = subprocess.run([sys.executable, '-m', 'pyeztrace.cli', *map(str, args)],
                                    cwd=root, capture_output=True, text=True, timeout=20)
            assert result.returncode == 0, result.stderr
            return result
        try:
            assert get('/api/status')['source']['status'] == 'missing_file'
            assert b'PyEzTrace' in get('/') and b'fetchTree' in get('/app.js')
            app = root / 'app.py'
            app.write_text(APP)
            env = {**os.environ, 'EZTRACE_LOG_FILE': str(log),
                   'EZTRACE_DISABLE_FILE_LOGGING': '0', 'EZTRACE_FILE_LOG_FORMAT': 'json'}
            subprocess.run([sys.executable, str(app)], cwd=root, env=env, check=True,
                           capture_output=True, timeout=20)
            tree = get('/api/tree')
            assert tree['total_nodes'] == 5, tree
            assert tree['source']['calls_started'] == 5
            logs = get('/api/logs')
            assert len(logs['logs']) >= 10
            row = logs['logs'][0]
            route = f"/api/logs/payload?id={row['id']}&generation={logs['generation']}"
            assert get(route)['payload']['call_id']
            assert json.loads(cli('print', log, '--errors', '--format', 'json').stdout)
            assert json.loads(cli('print', log, '--analyze', '--format', 'json').stdout)
            assert '\x1b' not in cli('print', log).stdout
            replacement = root / 'replacement'
            replacement.write_text('')
            replacement.replace(log)
            assert get('/api/tree')['source']['status'] == 'empty_file'
            try:
                get(route)
                raise AssertionError('old payload identity was reused')
            except urllib.error.HTTPError as exc:
                assert exc.code == 404
            print('PASS: real async application → installed CLI → HTTP viewer → rotation')
        finally:
            viewer._httpd.shutdown()
            viewer._httpd.server_close()
            thread.join(5)


if __name__ == '__main__':
    main()

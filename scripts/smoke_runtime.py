"""Run real-process logging lifecycle checks with the selected Python install.

Uses temporary files only. Normal exit, an unhandled error, and a handled SIGINT
must flush every accepted log record. No shutdown guarantee is made for SIGKILL
or os._exit(), which bypass Python cleanup.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile


APPLICATION = '''
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import signal
import sys
import time
from pyeztrace.setup import Setup
from pyeztrace.custom_logging import Logging, LogContext
from pyeztrace.tracer import trace

async def configure():
    await Setup.async_initialize("runtime-smoke", disable_file_logging=False,
        file_format="json", console_format="plain", log_file=sys.argv[1],
        buffer_enabled=True, buffer_flush_interval=60)
asyncio.run(configure())
log = Logging()
shared = LogContext(component="workers")
def worker(index):
    with shared:
        for item in range(20):
            log.log_info("record", record_id=f"{index}:{item}")
with ThreadPoolExecutor(max_workers=4) as executor:
    list(executor.map(worker, range(4)))
@trace()
async def cancellable():
    with log.with_context(task="cancelled"):
        raise asyncio.CancelledError()
async def cancellation():
    try:
        await cancellable()
    except asyncio.CancelledError:
        assert LogContext.get_current_context() == {}
asyncio.run(cancellation())
mode = sys.argv[2]
if mode == "error":
    raise RuntimeError("expected smoke failure")
if mode == "interrupt":
    signal.signal(signal.SIGINT, signal.default_int_handler)
    print("READY", flush=True)
    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        log.log_info("interrupt handled")
'''


def main():
    with tempfile.TemporaryDirectory(prefix='pyeztrace-runtime-') as directory:
        root = Path(directory)
        app = root / 'app.py'
        app.write_text(APPLICATION)
        for mode in ('normal', 'error', 'interrupt'):
            if mode == 'interrupt' and os.name == 'nt':
                continue
            log = root / f'{mode}.jsonl'
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(('EZTRACE_', 'OTEL_'))}
            process = subprocess.Popen([sys.executable, str(app), str(log), mode],
                                       cwd=root, env=env, text=True,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                if mode == 'interrupt':
                    # A readiness line avoids timing races when sending SIGINT.
                    import threading
                    ready = threading.Event()
                    def watch():
                        for line in process.stdout:
                            if line.strip() == 'READY':
                                ready.set()
                                return
                    reader = threading.Thread(target=watch, daemon=True)
                    reader.start()
                    assert ready.wait(15), 'child did not become ready'
                    reader.join(1)
                    process.send_signal(signal.SIGINT)
                stdout, stderr = process.communicate(timeout=20)
                assert process.returncode == (1 if mode == 'error' else 0), stderr
                rows = [json.loads(line) for line in log.read_text().splitlines()]
                identifiers = [row.get('data', {}).get('record_id') for row in rows
                               if row.get('message') == 'record']
                assert len(identifiers) == len(set(identifiers)) == 80, (mode, len(identifiers))
                assert set(identifiers) == {f'{i}:{j}' for i in range(4) for j in range(20)}
                if mode == 'interrupt':
                    assert any(row.get('message') == 'interrupt handled' for row in rows)
                print(f'PASS: {mode} exit flushed 80 records without loss or duplicates')
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()


if __name__ == '__main__':
    main()

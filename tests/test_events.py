import asyncio
import json
import os
import subprocess
import sys

from pyeztrace.events import Run, current_run_id


def test_explicit_runs_restore_and_isolate(monkeypatch):
    from pyeztrace.custom_logging import Logging
    monkeypatch.setattr(Logging, 'log_info', lambda *a, **k: None)
    previous = current_run_id()
    with Run('outer') as outer:
        assert current_run_id() == outer.id
        with Run('inner') as inner:
            assert current_run_id() == inner.id
        assert current_run_id() == outer.id
    assert current_run_id() == previous


async def test_concurrent_runs(monkeypatch):
    from pyeztrace.custom_logging import Logging
    monkeypatch.setattr(Logging, 'log_info', lambda *a, **k: None)
    async def work():
        with Run() as run:
            await asyncio.sleep(0)
            assert current_run_id() == run.id
            return run.id
    runs = await asyncio.gather(work(), work())
    assert len(set(runs)) == 2


def test_json_envelope_and_old_viewer(tmp_path):
    code = '''
from pyeztrace import Run, trace
from pyeztrace.setup import Setup
from pyeztrace.custom_logging import Logging
Setup.initialize('test',disable_file_logging=False,log_file='log.jsonl')
Logging()
@trace()
def call(): return 1
with Run('example'): call()
Logging.flush_logs()
'''
    result = subprocess.run([sys.executable, '-c', code], cwd=tmp_path,
                            env={**os.environ, 'PYTHONPATH': os.getcwd()},
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    rows = [json.loads(x) for x in (tmp_path / 'logs/log.jsonl').read_text().splitlines()]
    assert len({r['run_id'] for r in rows}) == 1
    assert len({r['event_id'] for r in rows}) == len(rows)
    assert all(r['schema_version'] == 1 for r in rows)
    from pyeztrace.viewer import _TraceTreeBuilder
    assert _TraceTreeBuilder(tmp_path / 'logs/log.jsonl').build_tree()['roots']


def test_failure_investigation_matches_viewer(tmp_path):
    code = '''
from types import SimpleNamespace
from pyeztrace import Run, trace
from pyeztrace.setup import Setup
from pyeztrace.custom_logging import Logging
from pyeztrace.integrations.sdk import trace_openai
Setup.initialize('test',disable_file_logging=False,log_file='log.jsonl')
Logging()
def fail(**kwargs): raise ValueError('private error')
client=trace_openai(SimpleNamespace(responses=SimpleNamespace(create=fail)))
@trace()
def work(): client.responses.create(model='synthetic', input='Bearer secret-value')
try:
    with Run('failure'): work()
except ValueError: pass
Logging.flush_logs()
'''
    result = subprocess.run([sys.executable, '-c', code], cwd=tmp_path,
                            env={**os.environ, 'PYTHONPATH': os.getcwd()},
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    path = tmp_path / 'logs/log.jsonl'
    from pyeztrace.inspection import Snapshot
    from pyeztrace.viewer import _TraceTreeBuilder
    snapshot = Snapshot(path)
    llm = next(c for c in snapshot.calls.values() if c['kind'] == 'llm')
    assert llm['status'] == 'error' and llm['parent_id']
    assert len(snapshot.ancestors(llm)) == 2
    roots = _TraceTreeBuilder(path).build_tree()['roots']
    viewed = roots[0]['children'][0]
    assert viewed['call_id'] == llm['call_id'] and viewed['run_id'] == llm['run_id']
    assert viewed['status'] == 'error' and viewed['llm']['provider'] == 'openai'
    assert 'secret-value' not in json.dumps(llm['records'])
    cli = subprocess.run([sys.executable, '-m', 'pyeztrace.cli', 'errors', str(path)],
                         capture_output=True, text=True, timeout=15)
    assert cli.returncode == 0, cli.stderr
    assert llm['call_id'] in {c['call_id'] for c in json.loads(cli.stdout)['data']}

import json
import subprocess
import sys

import pytest

from pyeztrace.inspection import InspectionError, Snapshot


def invoke(*args):
    return subprocess.run([sys.executable, '-m', 'pyeztrace.cli', *map(str, args)],
                          capture_output=True, text=True, timeout=15)


@pytest.fixture
def log(tmp_path):
    path = tmp_path / 'trace.jsonl'
    rows = [
        {'function': 'root', 'data': {'event': 'start', 'call_id': 'a'}},
        {'function': 'child', 'data': {'event': 'start', 'call_id': 'b', 'parent_id': 'a'}},
        {'function': 'child', 'duration': 0.2, 'data': {'event': 'error', 'call_id': 'b', 'parent_id': 'a', 'status': 'error', 'result_preview': '☃' * 100}},
        {'function': 'root', 'duration': 0.3, 'data': {'event': 'end', 'call_id': 'a', 'status': 'success'}},
    ]
    path.write_text('\n'.join(map(json.dumps, rows)))
    return path


def test_failure_investigation_and_bounded_payload(log):
    result = invoke('errors', log)
    assert result.returncode == 0, result.stderr
    failure = json.loads(result.stdout)['data'][0]
    assert failure['call_id'] == 'b'
    assert 'result_preview' not in result.stdout
    result = invoke('path', log, 'b')
    assert [c['call_id'] for c in json.loads(result.stdout)['data']] == ['a', 'b']
    payload = json.loads(invoke('payload', log, 'b', '--max-bytes', 100).stdout)['data']
    assert payload['truncated'] and len(payload['content'].encode()) <= 100
    assert json.loads(invoke('runs', log).stdout)['data'][0]['status'] == 'unknown'


def test_cursor_binds_content_and_query(log):
    first = json.loads(invoke('tree', log, '--limit', 1).stdout)
    cursor = first['next_cursor']
    second = json.loads(invoke('tree', log, '--limit', 1, '--cursor', cursor).stdout)
    assert second['data'][0]['call_id'] == 'b' and second['next_cursor'] is None
    assert json.loads(invoke('errors', log, '--cursor', cursor).stderr)['error']['code'] == 'stale_cursor'
    log.write_text(log.read_text() + '\n{}')
    result = invoke('tree', log, '--cursor', cursor)
    assert result.returncode == 1 and not result.stdout
    assert json.loads(result.stderr)['error']['code'] == 'stale_cursor'


@pytest.mark.parametrize('arguments', [('--limit', '0'), ('--limit', 'abc'), ('--unknown',)])
def test_structured_argument_errors(log, arguments):
    result = invoke('tree', log, *arguments)
    assert result.returncode == 2 and not result.stdout
    assert json.loads(result.stderr)['error']['code'] == 'invalid_arguments'


def test_corrupt_and_future_records(log):
    with log.open('a') as stream:
        stream.write('\nnot json\n[]\n')
    assert Snapshot(log).warnings[0]['count'] == 2
    with log.open('a') as stream:
        stream.write('{"schema_version":2,"data":{}}\n')
    with pytest.raises(InspectionError, match='Unsupported'):
        Snapshot(log)


def test_missing_ambiguous_and_cyclic_calls(tmp_path):
    path = tmp_path / 'trace'
    rows = [{'schema_version': 1, 'run_id': r, 'data': {'call_id': 'a', 'parent_id': 'a'}} for r in ('one', 'two')]
    path.write_text('\n'.join(map(json.dumps, rows)))
    snapshot = Snapshot(path)
    with pytest.raises(InspectionError, match='multiple'):
        snapshot.find('a')
    with pytest.raises(InspectionError, match='cycle'):
        snapshot.ancestors(snapshot.find('a', 'one'))
    result = invoke('call', path, 'missing')
    assert result.returncode == 1 and json.loads(result.stderr)['error']['code'] == 'not_found'


def test_payload_does_not_follow_file_references(tmp_path):
    secret = tmp_path / 'secret'
    secret.write_text('MUST NOT READ THIS')
    path = tmp_path / 'log'
    path.write_text(json.dumps({'data': {'call_id': 'a', 'payload_file': str(secret)}}))
    assert 'MUST NOT READ THIS' not in invoke('payload', path, 'a').stdout


def test_fifo_is_rejected_without_waiting(tmp_path):
    import os
    from pyeztrace.inspection import Snapshot, InspectionError
    if not hasattr(os, 'mkfifo'):
        pytest.skip('POSIX FIFO test')
    path = tmp_path / 'pipe'
    os.mkfifo(path)
    with pytest.raises(InspectionError, match='regular file'):
        Snapshot(path)


def test_oversized_cursor_is_rejected(tmp_path):
    from pyeztrace.inspection import Snapshot, InspectionError
    path = tmp_path / 'empty.log'
    path.write_text('')
    with pytest.raises(InspectionError, match='size limit'):
        Snapshot(path).page([], {}, 10, 'x' * 4097)

"""Exercise the public command, streams, filters, and exit statuses."""
import json
import os
import subprocess
import sys

import pytest


def cli(*args, env=None):
    return subprocess.run([sys.executable, '-m', 'pyeztrace.cli', *map(str, args)],
                          capture_output=True, text=True, env=env)


@pytest.fixture
def records(tmp_path):
    path = tmp_path / 'trace with spaces.log'
    rows = [dict(timestamp=f'2026-09-{day:02d}T12:00:00+00:00', level=level,
                 function='work', message='message', duration=1,
                 data={'tenant': tenant})
            for day, level, tenant in [(1, 'ERROR', 'a=b'), (2, 'ERROR', 'a=b'),
                                       (3, 'ERROR', 'other'), (4, 'INFO', 'a=b')]]
    path.write_text('\n'.join(map(json.dumps, rows)) + '\n')
    return path


@pytest.mark.parametrize('mode', [[], ['--analyze'], ['--errors']])
def test_filters_apply_to_every_mode(records, mode):
    result = cli('print', records, *mode, '--since', '2026-09-02T00:00:00Z',
                 '--until', '2026-09-03T23:59:59Z', '--context', 'tenant=a=b',
                 '--level', 'ERROR', '--format', 'json')
    assert result.returncode == 0, result.stderr
    assert result.stderr == ''
    value = json.loads(result.stdout)
    assert value['work']['count'] == 1 if mode == ['--analyze'] else len(value) == 1


@pytest.mark.parametrize('args', [
    ['--since', 'yesterday'], ['--context', 'bad'], ['--context', '=value'],
    ['--analyze', '--errors'], ['--function', 'work'],
    ['--since', '2026-10-01', '--until', '2026-09-01'], ['--form', 'json'],
])
def test_invalid_arguments_are_usage_errors(records, args):
    result = cli('print', records, *args)
    assert result.returncode == 2
    assert not result.stdout
    assert 'Traceback' not in result.stderr


@pytest.mark.parametrize('port', ['invalid', '-1', '65536'])
def test_invalid_environment_port_is_usage_error(records, port):
    result = cli('serve', records, env={**os.environ, 'EZTRACE_VIEW_PORT': port})
    assert result.returncode == 2
    assert 'port must be' in result.stderr
    assert not result.stdout


def test_missing_file_fails_with_clean_streams(tmp_path):
    result = cli('print', tmp_path / 'missing', '--format', 'json')
    assert result.returncode == 1
    assert not result.stdout
    assert 'Error:' in result.stderr and 'Traceback' not in result.stderr


def test_color_and_empty_results(records):
    assert '\x1b' not in cli('print', records).stdout
    assert '\x1b' in cli('print', records, '--color', 'always').stdout
    result = cli('print', records, '--level', 'DEBUG', '--format', 'json')
    assert result.returncode == 0 and json.loads(result.stdout) == []


def test_help_version():
    for args in [('--help',), ('--version',), ('print', '--help'), ('serve', '--help')]:
        result = cli(*args)
        assert result.returncode == 0 and result.stdout and not result.stderr


def test_open_uses_bound_url_in_a_real_subprocess(tmp_path):
    import shlex
    import signal
    import time
    import urllib.request

    if sys.platform == 'win32':
        pytest.skip('POSIX BROWSER command override smoke test')
    receipt = tmp_path / 'opened-url.txt'
    recorder = tmp_path / 'record browser.py'
    recorder.write_text('import pathlib, sys\npathlib.Path(sys.argv[1]).write_text(sys.argv[2])\n')
    browser_command = ' '.join(map(shlex.quote, [sys.executable, str(recorder), str(receipt)])) + ' %s'
    process = subprocess.Popen(
        [sys.executable, '-m', 'pyeztrace.cli', 'serve', str(tmp_path / 'missing.log'),
         '--port', '0', '--open'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, env={**os.environ, 'BROWSER': browser_command})
    try:
        deadline = time.monotonic() + 8
        while not receipt.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert receipt.exists(), 'browser command did not receive a URL'
        url = receipt.read_text()
        assert url.startswith('http://127.0.0.1:') and not url.endswith(':0')
        with urllib.request.urlopen(url + '/api/status', timeout=5) as response:
            assert json.load(response)['source']['status'] == 'missing_file'
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        stdout, stderr = process.communicate(timeout=8)
    assert process.returncode == 0, stderr
    assert 'Viewer stopped.' in stdout

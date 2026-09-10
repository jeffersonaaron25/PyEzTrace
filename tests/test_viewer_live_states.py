"""Acceptance coverage for the viewer's live states.

Covers the six states a user can land in: waiting for a file, waiting for
records, unreadable records, active traces, completed traces, and a rotated
log file. The recurring theme is truthfulness: the viewer must never present
poll time as data freshness, and must never present a running call as a
zero-duration one.
"""
import json
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from pyeztrace.viewer import TraceViewerServer, _TraceTreeBuilder


def _record(call_id, event, function="handler", epoch=None, duration=None, **data):
    epoch = time.time() if epoch is None else epoch
    payload = {"event": event, "call_id": call_id, "parent_id": None, "time_epoch": epoch}
    payload.update(data)
    entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(epoch)),
        "level": "INFO",
        "project": "TESTAPP",
        "fn_type": "parent",
        "function": function,
        "message": "called..." if event == "start" else "Ok.",
        "data": payload,
    }
    if duration is not None:
        entry["duration"] = duration
    return json.dumps(entry)


def _append(path: Path, *lines: str) -> None:
    with path.open("a", encoding="utf-8") as fh:
        for line in lines:
            fh.write(line + "\n")


# --------------------------------------------------------------------------
# Source states
# --------------------------------------------------------------------------

def test_missing_file_is_reported_as_waiting(tmp_path):
    builder = _TraceTreeBuilder(tmp_path / "nope.log")
    state = builder.source_state()
    assert state["status"] == "missing_file"
    assert state["file_exists"] is False
    assert state["record_count"] == 0
    assert state["newest_event_at"] is None


def test_empty_file_is_distinct_from_missing_file(tmp_path):
    log = tmp_path / "app.log"
    log.write_text("", encoding="utf-8")
    state = _TraceTreeBuilder(log).source_state()
    assert state["status"] == "empty_file"
    assert state["file_exists"] is True


def test_non_json_log_is_reported_as_unparsable(tmp_path):
    """The silent-zeroes case: a human-readable log must be diagnosed, not ignored."""
    log = tmp_path / "app.log"
    log.write_text(
        "2026-09-09T10:00:01 - INFO - [APP] fetch_user started\n"
        "2026-09-09T10:00:01 - INFO - [APP] fetch_user completed in 0.15s\n",
        encoding="utf-8",
    )
    state = _TraceTreeBuilder(log).source_state()
    assert state["status"] == "unparsable"
    assert state["record_count"] == 0
    assert state["unparsed_lines"] == 2


def test_json_that_is_not_a_trace_record_counts_as_unparsable(tmp_path):
    log = tmp_path / "app.log"
    log.write_text('{"hello": "world"}\n[1,2,3]\n', encoding="utf-8")
    state = _TraceTreeBuilder(log).source_state()
    assert state["status"] == "unparsable"
    assert state["unparsed_lines"] == 2


def test_valid_records_report_ok_and_newest_event_time(tmp_path):
    log = tmp_path / "app.log"
    epoch = time.time() - 30
    _append(log, _record("c1", "start", epoch=epoch))
    state = _TraceTreeBuilder(log).source_state()
    assert state["status"] == "ok"
    assert state["record_count"] == 1
    assert state["newest_event_at"] == pytest.approx(epoch, abs=1.0)


def test_newest_event_at_never_falls_back_to_now(tmp_path):
    """An unreadable timestamp must not make stale data look current."""
    log = tmp_path / "app.log"
    log.write_text(
        json.dumps({"timestamp": "not-a-timestamp", "level": "INFO", "data": {}}) + "\n",
        encoding="utf-8",
    )
    builder = _TraceTreeBuilder(log)
    state = builder.source_state()
    assert state["record_count"] == 1
    assert state["newest_event_at"] is None


# --------------------------------------------------------------------------
# Freshness vs reachability
# --------------------------------------------------------------------------

def test_generated_at_advances_but_newest_event_at_does_not(tmp_path):
    """The core defect: a cached tree used to get a fresh timestamp stamped on it.

    generated_at is poll time and may advance on every request. newest_event_at
    is data age and must only move when new records arrive.
    """
    log = tmp_path / "app.log"
    epoch = time.time() - 60
    _append(log, _record("c1", "start", epoch=epoch))
    builder = _TraceTreeBuilder(log)

    first_tree = builder.build_tree()
    first_state = builder.source_state()
    time.sleep(0.05)
    second_tree = builder.build_tree()
    second_state = builder.source_state()

    assert second_tree["generated_at"] > first_tree["generated_at"]
    assert second_state["newest_event_at"] == first_state["newest_event_at"]


def test_newest_event_at_advances_when_records_arrive(tmp_path):
    log = tmp_path / "app.log"
    old = time.time() - 120
    _append(log, _record("c1", "start", epoch=old))
    builder = _TraceTreeBuilder(log)
    before = builder.source_state()["newest_event_at"]

    _append(log, _record("c2", "start", epoch=time.time()))
    after = builder.source_state()["newest_event_at"]
    assert after > before


# --------------------------------------------------------------------------
# Running vs completed calls
# --------------------------------------------------------------------------

def test_started_call_is_running_with_no_duration(tmp_path):
    """A call in flight must be marked running and must NOT report a duration."""
    log = tmp_path / "app.log"
    start = time.time()
    _append(log, _record("c1", "start", function="slow_task", epoch=start))
    tree = _TraceTreeBuilder(log).build_tree()

    node = tree["roots"][0]
    assert node["status"] == "running"
    assert node["duration"] is None
    assert node["end_time"] is None
    assert node["start_time"] == pytest.approx(start, abs=1.0)
    # Resource metrics stay absent rather than defaulting to zero.
    assert node["cpu_time"] is None
    assert node["mem_delta_kb"] is None


def test_running_call_transitions_to_success_in_place(tmp_path):
    log = tmp_path / "app.log"
    start = time.time()
    _append(log, _record("c1", "start", function="slow_task", epoch=start))
    builder = _TraceTreeBuilder(log)
    assert builder.build_tree()["roots"][0]["status"] == "running"

    _append(log, _record(
        "c1", "end", function="slow_task", epoch=start + 9.0, duration=9.0,
        cpu_time=0.5, mem_delta_kb=128, mem_mode="peak_rusage", result_preview="done",
    ))
    node = builder.build_tree()["roots"][0]
    assert node["status"] == "success"
    assert node["duration"] == pytest.approx(9.0)
    assert node["cpu_time"] == pytest.approx(0.5)
    assert node["call_id"] == "c1"


def test_error_call_is_marked_error(tmp_path):
    log = tmp_path / "app.log"
    start = time.time()
    _append(log, _record("c1", "start", function="charge", epoch=start))
    entry = json.loads(_record("c1", "error", function="charge", epoch=start + 0.2))
    entry["level"] = "ERROR"
    entry["message"] = "payment declined"
    _append(log, json.dumps(entry))

    node = _TraceTreeBuilder(log).build_tree()["roots"][0]
    assert node["status"] == "error"
    assert "payment declined" in (node.get("error") or "")


# --------------------------------------------------------------------------
# Rotation
# --------------------------------------------------------------------------

def test_rotation_resets_counts_and_freshness(tmp_path):
    log = tmp_path / "app.log"
    _append(log, _record("c1", "start", epoch=time.time()))
    builder = _TraceTreeBuilder(log)
    assert builder.source_state()["record_count"] == 1

    # Simulate truncation in place (log rotation with copytruncate).
    log.write_text("", encoding="utf-8")
    state = builder.source_state()
    assert state["record_count"] == 0
    assert state["newest_event_at"] is None
    assert state["status"] == "empty_file"


def test_deleting_the_file_returns_to_waiting_state(tmp_path):
    log = tmp_path / "app.log"
    _append(log, _record("c1", "start", epoch=time.time()))
    builder = _TraceTreeBuilder(log)
    assert builder.source_state()["status"] == "ok"

    log.unlink()
    state = builder.source_state()
    assert state["status"] == "missing_file"
    assert state["record_count"] == 0
    assert state["newest_event_at"] is None


# --------------------------------------------------------------------------
# HTTP surface
# --------------------------------------------------------------------------

@pytest.fixture
def running_server(tmp_path):
    log = tmp_path / "app.log"
    server = TraceViewerServer(log, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    for _ in range(100):
        if server._httpd is not None:
            break
        time.sleep(0.02)
    assert server._httpd is not None
    port = server._httpd.server_address[1]
    yield f"http://127.0.0.1:{port}", log
    server._httpd.shutdown()
    thread.join(timeout=5)


def _get(url):
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def test_status_endpoint_reports_source_state(running_server):
    base, log = running_server
    data = _get(f"{base}/api/status")
    assert data["source"]["status"] == "missing_file"
    assert data["total_nodes"] == 0

    _append(log, _record("c1", "start", epoch=time.time()))
    data = _get(f"{base}/api/status")
    assert data["source"]["status"] == "ok"
    assert data["total_nodes"] == 1


def test_tree_endpoint_carries_source_state(running_server):
    base, log = running_server
    _append(log, _record("c1", "start", epoch=time.time()))
    data = _get(f"{base}/api/tree")
    assert "source" in data
    assert data["source"]["status"] == "ok"
    assert data["source"]["newest_event_at"] is not None
    # Both times are present and are different things.
    assert "generated_at" in data
    assert data["generated_at"] >= data["source"]["newest_event_at"]


def test_server_starts_against_a_missing_log_file(running_server):
    """The viewer can be started before the traced app exists."""
    base, log = running_server
    assert not log.exists()
    data = _get(f"{base}/api/tree")
    assert data["roots"] == []
    assert data["source"]["status"] == "missing_file"


def test_page_and_bundle_are_served(running_server):
    base, _ = running_server
    with urllib.request.urlopen(f"{base}/", timeout=5) as resp:
        html = resp.read().decode("utf-8")
    assert 'id="empty-state"' in html
    assert 'id="conn-state"' in html
    assert 'id="last-checked"' in html
    assert 'id="newest-trace"' in html
    with urllib.request.urlopen(f"{base}/app.js", timeout=5) as resp:
        js = resp.read().decode("utf-8")
    assert "effectiveConnState" in js
    assert "renderEmptyState" in js


# --------------------------------------------------------------------------
# CLI behaviour
# --------------------------------------------------------------------------

def test_url_uses_loopback_for_wildcard_bind(tmp_path):
    assert TraceViewerServer(tmp_path / "a.log", host="0.0.0.0", port=9).url == "http://127.0.0.1:9"
    assert TraceViewerServer(tmp_path / "a.log", host="127.0.0.1", port=9).url == "http://127.0.0.1:9"


def test_open_browser_flag_opens_the_served_url(tmp_path, monkeypatch):
    opened = []
    import webbrowser
    monkeypatch.setattr(webbrowser, "open_new_tab", lambda url: opened.append(url))
    server = TraceViewerServer(tmp_path / "a.log", host="127.0.0.1", port=8765)
    server._open_browser()
    assert opened == ["http://127.0.0.1:8765"]


def test_port_in_use_exits_with_a_message(tmp_path, capsys):
    log = tmp_path / "app.log"
    holder = TraceViewerServer(log, host="127.0.0.1", port=0)
    thread = threading.Thread(target=holder.serve_forever, daemon=True)
    thread.start()
    for _ in range(100):
        if holder._httpd is not None:
            break
        time.sleep(0.02)
    port = holder._httpd.server_address[1]
    try:
        clash = TraceViewerServer(log, host="127.0.0.1", port=port)
        with pytest.raises(SystemExit) as exc:
            clash.serve_forever()
        assert exc.value.code == 1
        out = capsys.readouterr().err
        assert "already in use" in out
        assert f"--port {port + 1}" in out
    finally:
        holder._httpd.shutdown()
        thread.join(timeout=5)


def test_serve_parser_accepts_open_flag():
    import sys
    from unittest import mock
    from pyeztrace import cli

    called = {}

    def fake_serve(args):
        called["open_browser"] = args.open_browser
        called["port"] = args.port

    with mock.patch.object(cli, "_cmd_serve", fake_serve), \
         mock.patch.object(sys, "argv", ["pyeztrace", "serve", "x.log", "--open", "--port", "9999"]):
        cli.main()
    assert called["open_browser"] is True
    assert called["port"] == 9999


def test_partial_utf8_survives_poll_and_delete(tmp_path):
    path = tmp_path / 'unicode.log'
    builder = _TraceTreeBuilder(path)
    raw = json.dumps({'timestamp': '2026-09-01T00:00:00Z', 'level': 'INFO',
                      'message': 'café'}, ensure_ascii=False).encode()
    cut = raw.index('é'.encode()) + 1
    path.write_bytes(raw[:cut])
    assert builder.build_logs()['logs'] == []
    with path.open('ab') as f:
        f.write(raw[cut:] + b'\n')
    assert builder.build_logs()['logs'][0]['message'] == 'café'
    path.unlink()
    assert builder.source_state()['status'] == 'missing_file'
    path.write_text(_record('new', 'start') + '\n')
    assert builder.build_tree()['total_nodes'] == 1


def test_rotation_payload_generation_and_start_counter(tmp_path):
    path = tmp_path / 'log'
    path.write_text(_record('old', 'start') + '\n')
    builder = _TraceTreeBuilder(path)
    generation = builder.build_logs()['generation']
    _append(path, _record('old', 'end', duration=1))
    assert builder.source_state()['calls_started'] == 1
    replacement = tmp_path / 'replacement'
    replacement.write_text(_record('new', 'start') + '\n')
    replacement.replace(path)
    assert builder.get_log_payload(0, generation) is None
    assert builder.source_state()['generation'] != generation


def test_unreadable_source_preserves_records(tmp_path, monkeypatch):
    path = tmp_path / 'log'
    path.write_text(_record('old', 'start') + '\n')
    builder = _TraceTreeBuilder(path)
    builder.build_tree()
    def denied(*args, **kwargs):
        raise PermissionError('permission denied')
    monkeypatch.setattr(Path, 'open', denied)
    state = builder.source_state()
    assert state['status'] == 'read_error'
    assert state['record_count'] == 1
    assert builder.build_tree()['total_nodes'] == 1


@pytest.mark.parametrize('epoch', [float('nan'), float('inf'), True, {}, 'bad'])
def test_invalid_epochs_never_become_now(tmp_path, epoch):
    builder = _TraceTreeBuilder(tmp_path / 'log')
    assert builder._event_epoch({'timestamp': 'invalid', 'data': {'time_epoch': epoch}}) is None
    assert builder._event_epoch({'timestamp': '1970-01-01T01:00:00+01:00', 'data': {'time_epoch': epoch}}) == 0


def test_malformed_records_and_deep_tree(tmp_path):
    path = tmp_path / 'log'
    bad = [{'timestamp': '2026-09-01', 'level': 'INFO', 'data': value}
           for value in [[], 'text', {'call_id': []}, {'event': {}}]]
    path.write_text('\n'.join(map(json.dumps, bad)) + '\n')
    builder = _TraceTreeBuilder(path)
    assert builder.build_tree()['total_nodes'] == 0
    assert builder.source_state()['unparsed_lines'] == 4
    _append(path, *[_record(str(i), 'start', parent_id=str(i-1) if i else None)
                   for i in range(1200)])
    result = builder.build_tree()
    assert result['total_nodes'] == 1200
    assert 'Maximum display depth reached' in json.dumps(result)


def test_status_does_not_materialize_tree(running_server, monkeypatch):
    base, log = running_server
    def forbidden(self):
        raise AssertionError('status must not build a tree')
    monkeypatch.setattr(_TraceTreeBuilder, 'build_tree', forbidden)
    with urllib.request.urlopen(base + '/api/status') as response:
        assert json.load(response)['source']['calls_started'] == 0


# --------------------------------------------------------------------------
# Log-stream identity (generation)
# --------------------------------------------------------------------------
# The browser treats a changed generation as "this log was replaced, the
# snapshot is no longer comparable" and discards the selection. So the
# generation must identify the log stream, not the viewer process: restarting
# the viewer against an unchanged file has to keep the same value.

def test_generation_is_a_sentinel_before_any_record(tmp_path):
    log = tmp_path / "app.log"
    assert _TraceTreeBuilder(log).source_state()["generation"] == "empty"
    log.write_text("", encoding="utf-8")
    assert _TraceTreeBuilder(log).source_state()["generation"] == "empty"


def test_generation_waits_for_a_complete_first_line(tmp_path):
    """A half-written first record must not produce a generation it will lose."""
    log = tmp_path / "app.log"
    full = _record("c1", "start", epoch=time.time())
    log.write_text(full[:40], encoding="utf-8")
    assert _TraceTreeBuilder(log).source_state()["generation"] == "empty"

    log.write_text(full + "\n", encoding="utf-8")
    assert _TraceTreeBuilder(log).source_state()["generation"] != "empty"


def test_generation_survives_a_viewer_restart(tmp_path):
    """Restarting `pyeztrace serve` must not look like the log was replaced."""
    log = tmp_path / "app.log"
    _append(log, _record("c1", "start", epoch=time.time()))
    first = _TraceTreeBuilder(log).source_state()["generation"]
    second = _TraceTreeBuilder(log).source_state()["generation"]
    assert first == second != "empty"


def test_generation_is_stable_while_the_log_grows(tmp_path):
    log = tmp_path / "app.log"
    _append(log, _record("c1", "start", epoch=time.time()))
    builder = _TraceTreeBuilder(log)
    baseline = builder.source_state()["generation"]

    for index in range(5):
        _append(log, _record(f"c{index + 2}", "start", epoch=time.time()))
        assert builder.source_state()["generation"] == baseline
    # And to a viewer that starts later against the same file.
    assert _TraceTreeBuilder(log).source_state()["generation"] == baseline


def test_generation_changes_on_copytruncate(tmp_path):
    """Same inode, replaced content: the snapshot is genuinely stale."""
    log = tmp_path / "app.log"
    _append(log, _record("c1", "start", epoch=time.time()))
    builder = _TraceTreeBuilder(log)
    before = builder.source_state()["generation"]

    log.write_text(_record("other", "start", epoch=time.time()) + "\n", encoding="utf-8")
    after = builder.source_state()["generation"]
    assert after != before
    assert after != "empty"


def test_generation_changes_when_the_file_is_rotated_away(tmp_path):
    log = tmp_path / "app.log"
    _append(log, _record("c1", "start", epoch=time.time()))
    builder = _TraceTreeBuilder(log)
    before = builder.source_state()["generation"]

    log.rename(tmp_path / "app.log.1")
    _append(log, _record("fresh", "start", epoch=time.time()))
    assert builder.source_state()["generation"] != before


def test_same_size_in_place_rewrite_is_detected(tmp_path):
    """A rewrite that changes neither inode nor size must still be caught.

    Otherwise the viewer keeps its old byte offset and reads the new content
    from the middle of a record, while reporting the snapshot as unchanged.
    """
    log = tmp_path / "app.log"
    first = _record("AAAA", "start", epoch=1789000000.0)
    second = _record("BBBB", "start", epoch=1789000000.0)
    assert len(first) == len(second), "fixture must produce identical lengths"

    log.write_text(first + "\n", encoding="utf-8")
    builder = _TraceTreeBuilder(log)
    before = builder.source_state()
    assert [n["call_id"] for n in builder.build_tree()["roots"]] == ["AAAA"]

    log.write_text(second + "\n", encoding="utf-8")
    after = builder.source_state()
    assert after["generation"] != before["generation"]
    assert after["record_count"] == 1
    assert [n["call_id"] for n in builder.build_tree()["roots"]] == ["BBBB"]

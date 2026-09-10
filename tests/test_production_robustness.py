import os
import asyncio
import json
import logging
import queue
from pathlib import Path
import pytest
from unittest.mock import MagicMock

from pyeztrace.custom_logging import Logging, BufferedHandler
from pyeztrace.setup import Setup
from pyeztrace.config import config
from pyeztrace.tracer import (
    child_trace_decorator,
    trace,
    trace_children_in_module,
    tracing_active,
)
from pyeztrace.viewer import _TraceTreeBuilder


@pytest.fixture(autouse=True)
def reset_setup():
    Setup.reset()


@pytest.fixture
def tracing_capture():
    Setup.enable_testing_mode()
    Setup.initialize("GENERATOR_TEST")
    try:
        yield
    finally:
        config.disable_resource_metrics = False
        Setup.disable_testing_mode()


def test_monkey_patch_transactional_rollback():
    """Verify that if patching fails halfway through, all patched attributes are rolled back."""
    class DummyTarget:
        def good_method(self):
            return "good"

        def bad_method(self):
            return "bad"

    # Store originals
    orig_good = DummyTarget.good_method
    orig_bad = DummyTarget.bad_method

    # Create a decorator that will raise an error when wrapping 'bad_method'
    call_count = 0
    def failing_decorator(func):
        nonlocal call_count
        call_count += 1
        if func.__name__ == "bad_method":
            raise RuntimeError("Inject failure during patching")
        # Just return a dummy wrapped function
        def wrapped(*args, **kwargs):
            return func(*args, **kwargs)
        wrapped.__name__ = func.__name__
        return wrapped

    # Use trace_children_in_module
    patcher = trace_children_in_module(DummyTarget, failing_decorator)

    with pytest.raises(RuntimeError, match="Inject failure during patching"):
        patcher.__enter__()

    # Verify rollback: both methods should be restored to their original functions
    assert DummyTarget.good_method is orig_good
    assert DummyTarget.bad_method is orig_bad


def test_buffered_handler_close_flushes_and_closes_target():
    """Verify BufferedHandler close() flushes buffered records and closes its target handler."""
    target = logging.Handler()
    target.emit = MagicMock()
    target.close = MagicMock()

    handler = BufferedHandler(target, buffer_size=10, flush_interval=60.0)

    # Log a record
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="buffered message",
        args=(),
        exc_info=None,
    )
    handler.emit(record)

    # Verify not yet emitted to target
    target.emit.assert_not_called()

    # Close handler
    handler.close()

    # Verify target was called to emit and close
    target.emit.assert_called_once_with(record)
    target.close.assert_called_once()


def test_viewer_memory_pruning(tmp_path):
    """Verify that the viewer builder limits the cached entries to prevent memory growth and translates IDs correctly."""
    log_file = tmp_path / "test_pruning.log"
    
    # Write 100,005 valid JSON lines
    lines = []
    for i in range(100005):
        lines.append(json.dumps({
            "timestamp": "2026-06-27T12:00:00",
            "level": "INFO",
            "project": "TEST",
            "function": "dummy",
            "message": f"msg {i}",
            "data": {
                "call_id": f"cid-{i}",
                "event": "start",
                "time_epoch": 1782561600.0 + i
            }
        }) + "\n")
    
    log_file.write_text("".join(lines), encoding="utf-8")

    builder = _TraceTreeBuilder(log_file)
    
    # Read entries
    entries = builder._read_entries_cached()
    
    # Cache should be capped at 100,000
    assert len(entries) == 100000
    assert builder._pruned_count == 5

    # Check build_logs
    res = builder.build_logs(limit=10)
    assert res["total_entries"] == 100005
    assert len(res["logs"]) == 10
    
    # Verify that the ID of the last entry is 100004
    assert res["logs"][-1]["id"] == 100004
    assert res["logs"][-1]["message"] == "msg 100004"

    # Verify get_log_payload mapping
    # logical ID 5 maps to local index 0 in builder._cached_entries (msg 5)
    payload_5 = builder.get_log_payload(5)
    assert payload_5 is not None
    assert payload_5["payload"]["call_id"] == "cid-5"

    # logical ID 4 should return None because it has been pruned
    payload_4 = builder.get_log_payload(4)
    assert payload_4 is None


def test_viewer_cycle_detection(tmp_path):
    """Verify that build_tree detects circular references in logs and does not stack overflow."""
    log_file = tmp_path / "test_cycle.log"
    
    # Create two entries with mutually circular parent_id/call_id
    entries = [
        json.dumps({
            "timestamp": "2026-06-27T12:00:00",
            "level": "INFO",
            "data": {
                "call_id": "A",
                "parent_id": "B",
                "event": "start",
                "function": "funcA"
            }
        }) + "\n",
        json.dumps({
            "timestamp": "2026-06-27T12:00:00",
            "level": "INFO",
            "data": {
                "call_id": "B",
                "parent_id": "A",
                "event": "start",
                "function": "funcB"
            }
        }) + "\n"
    ]
    log_file.write_text("".join(entries), encoding="utf-8")

    builder = _TraceTreeBuilder(log_file)
    
    # Verify build_tree handles the cycle gracefully
    tree_res = builder.build_tree()
    assert tree_res is not None
    
    # The roots list should contain the nodes, and one of them should have the error message
    roots = tree_res["roots"]
    assert len(roots) > 0
    
    # Let's check that one of the materialized nodes reports circular reference error
    found_error = False
    def check_node(node):
        nonlocal found_error
        if node.get("error") == "Circular reference detected":
            found_error = True
        for child in node.get("children", []):
            check_node(child)

    for r in roots:
        check_node(r)

    assert found_error, "Expected circular reference detection error in materialized tree"


def test_viewer_caching_optimizations(tmp_path):
    """Verify that build_tree and build_logs cache results and avoid re-serialization/re-materialization."""
    log_file = tmp_path / "test_caching.log"
    log_file.write_text(json.dumps({
        "timestamp": "2026-06-27T12:00:00",
        "level": "INFO",
        "data": {
            "call_id": "A",
            "event": "start",
            "function": "funcA"
        }
    }) + "\n", encoding="utf-8")

    builder = _TraceTreeBuilder(log_file)

    # Initial build
    res1 = builder.build_tree()
    assert builder._tree_cache is not None
    assert builder._tree_cache_version == builder._version

    # Modify the cache manually to verify it is returned
    builder._tree_cache = {"mocked_tree": True}
    res2 = builder.build_tree()
    assert res2.get("mocked_tree") is True

    # Same for build_logs
    res_logs1 = builder.build_logs(limit=10)
    assert builder._logs_cache is not None
    assert builder._logs_cache_version == builder._version

    builder._logs_cache = {"mocked_logs": True}
    res_logs2 = builder.build_logs(limit=10)
    assert res_logs2.get("mocked_logs") is True


def test_buffered_handler_background_flush():
    """Verify that BufferedHandler automatically flushes after flush_interval via the background thread."""
    import time
    
    target = logging.Handler()
    target.emit = MagicMock()
    
    # 0.1s flush interval
    handler = BufferedHandler(target, buffer_size=10, flush_interval=0.1)
    
    try:
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="background flush message",
            args=(),
            exc_info=None,
        )
        handler.emit(record)
        
        # Verify not yet emitted immediately
        target.emit.assert_not_called()
        
        # Wait slightly longer than the flush_interval
        time.sleep(0.25)
        
        # Verify background thread has automatically flushed it
        target.emit.assert_called_once_with(record)
    finally:
        handler.close()


def test_disable_resource_metrics():
    """Verify that disable_resource_metrics avoids cpu_time and memory snapshot collection."""
    from pyeztrace.config import config
    from pyeztrace.tracer import trace
    
    # Enable testing mode and initialize Setup
    Setup.enable_testing_mode()
    Setup.initialize("TEST_PROJECT")
    
    try:
        # 1. With resource metrics enabled (default)
        config.disable_resource_metrics = False
        
        @trace()
        def tracked_func():
            return "tracked"
            
        tracked_func()
        
        logs = Setup.get_captured_logs()
        assert len(logs) >= 2
        # Check start and end logs
        end_log = logs[-1]
        
        # Verify cpu_time and memory keys exist
        assert "cpu_time" in end_log["kwargs"]
        assert "mem_peak_kb" in end_log["kwargs"]
        assert end_log["kwargs"]["mem_mode"] != "disabled"
        
        # 2. With resource metrics disabled
        config.disable_resource_metrics = True
        Setup.clear_captured_logs()
        
        @trace()
        def untracked_func():
            return "untracked"
            
        untracked_func()
        
        logs_disabled = Setup.get_captured_logs()
        assert len(logs_disabled) >= 2
        end_log_disabled = logs_disabled[-1]
        
        # Verify cpu_time and memory are disabled / None
        assert end_log_disabled["kwargs"].get("cpu_time") is None
        assert end_log_disabled["kwargs"].get("mem_peak_kb") is None
        assert end_log_disabled["kwargs"].get("mem_rss_kb") is None
        assert end_log_disabled["kwargs"].get("mem_delta_kb") is None
        assert end_log_disabled["kwargs"].get("mem_mode") == "disabled"
        
    finally:
        Setup.disable_testing_mode()
        config.disable_resource_metrics = False


def test_sync_generator_preserves_protocol_and_cleans_up(tracing_capture):
    @trace()
    def values():
        received = yield "ready"
        return received

    generator = values()
    assert Setup.get_level() == 0
    assert next(generator) == "ready"
    assert Setup.get_level() == 1

    with pytest.raises(StopIteration) as stopped:
        generator.send("complete")

    assert stopped.value.value == "complete"
    assert Setup.get_level() == 0
    events = [entry["kwargs"].get("event") for entry in Setup.get_captured_logs()]
    assert events == ["start", "end"]


def test_sync_generator_supports_disabled_resource_metrics(tracing_capture):
    config.disable_resource_metrics = True

    @trace()
    def values():
        yield 1
        yield 2

    assert list(values()) == [1, 2]
    end_log = Setup.get_captured_logs()[-1]
    assert end_log["kwargs"]["event"] == "end"
    assert end_log["kwargs"]["cpu_time"] is None
    assert end_log["kwargs"]["mem_mode"] == "disabled"


async def test_async_generator_supports_disabled_resource_metrics(tracing_capture):
    config.disable_resource_metrics = True

    @trace()
    async def values():
        yield 1
        await asyncio.sleep(0)
        yield 2

    assert [item async for item in values()] == [1, 2]
    assert Setup.get_level() == 0
    end_log = Setup.get_captured_logs()[-1]
    assert end_log["kwargs"]["event"] == "end"
    assert end_log["kwargs"]["cpu_time"] is None


def test_generator_early_close_restores_context_without_success_log(tracing_capture):
    @trace()
    def values():
        yield 1
        yield 2

    generator = values()
    assert next(generator) == 1
    assert Setup.get_level() == 1
    generator.close()

    assert Setup.get_level() == 0
    events = [entry["kwargs"].get("event") for entry in Setup.get_captured_logs()]
    assert events == ["start"]


def test_generator_exception_is_preserved_and_logged(tracing_capture):
    @trace()
    def values():
        yield 1
        raise ValueError("generator-failed")

    generator = values()
    assert next(generator) == 1
    with pytest.raises(ValueError, match="generator-failed"):
        next(generator)

    assert Setup.get_level() == 0
    error_logs = [
        entry for entry in Setup.get_captured_logs()
        if entry["kwargs"].get("event") == "error"
    ]
    assert len(error_logs) == 1


def test_child_generator_is_lazy_and_does_not_leak_context(tracing_capture):
    def values():
        yield 1

    active_token = tracing_active.set(True)
    try:
        wrapped = child_trace_decorator(values)
        generator = wrapped()
        assert Setup.get_level() == 0
        generator.close()
        assert Setup.get_level() == 0

        assert list(wrapped()) == [1]
        assert Setup.get_level() == 0
    finally:
        tracing_active.reset(active_token)


def test_non_finite_sampling_values_are_rejected():
    with pytest.raises(ValueError, match="sample_rate"):
        trace(sample_rate=float("nan"))

    with pytest.raises(ValueError, match="adaptive_slow_threshold"):
        trace(adaptive_slow_threshold=float("inf"))

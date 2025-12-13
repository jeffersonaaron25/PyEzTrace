import json
import logging
import random
import time
import io
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest

from pyeztrace.config import config
from pyeztrace.custom_logging import Logging, BufferedHandler
from pyeztrace.setup import Setup
from pyeztrace.tracer import trace

@pytest.fixture(autouse=True)
def reset_setup():
    Setup.reset()


@pytest.fixture
def reset_logging_state():
    logger = logging.getLogger("pyeztrace")
    for handler in logger.handlers[:]:
        if hasattr(handler, "close"):
            handler.close()
        logger.removeHandler(handler)
    Logging._configured = False
    Logging._base_format = None
    Logging._console_format = None
    Logging._file_format = None
    Logging._file_logging_enabled = False
    yield
    for handler in logger.handlers[:]:
        if hasattr(handler, "close"):
            handler.close()
        logger.removeHandler(handler)
    Logging._configured = False
    Logging._base_format = None
    Logging._console_format = None
    Logging._file_format = None
    Logging._file_logging_enabled = False


@pytest.fixture
def restore_log_config():
    original_enabled = config.buffer_enabled
    original_flush_interval = config.buffer_flush_interval
    yield
    config.buffer_enabled = original_enabled
    config.buffer_flush_interval = original_flush_interval


@pytest.fixture
def restore_config():
    original = {
        "log_dir": config.log_dir,
        "log_file": config.log_file,
        "disable_file_logging": config.disable_file_logging,
        "_explicit": getattr(config, "_explicit", {}).copy(),
        "format": config.format,
        "console_format": config.console_format,
        "file_format": config.file_format,
    }
    yield
    config.log_dir = original["log_dir"]
    config.log_file = original["log_file"]
    config.disable_file_logging = original["disable_file_logging"]
    config.format = original["format"]
    config.console_format = original["console_format"]
    config.file_format = original["file_format"]
    if hasattr(config, "_explicit"):
        config._explicit = original["_explicit"]


def test_buffering_respects_environment(monkeypatch, restore_log_config, reset_logging_state):
    monkeypatch.setenv("EZTRACE_BUFFER_ENABLED", "true")
    monkeypatch.setenv("EZTRACE_BUFFER_FLUSH_INTERVAL", "2.5")

    Setup.initialize("EZTRACE_BUFFERING_ENV", show_metrics=False)
    Logging(log_format="plain")

    logger = logging.getLogger("pyeztrace")
    buffered_handlers = [h for h in logger.handlers if isinstance(h, BufferedHandler)]

    assert len(buffered_handlers) == 2
    assert {h.flush_interval for h in buffered_handlers} == {2.5}


def test_buffering_respects_config(monkeypatch, restore_log_config, reset_logging_state):
    monkeypatch.delenv("EZTRACE_BUFFER_ENABLED", raising=False)
    monkeypatch.delenv("EZTRACE_BUFFER_FLUSH_INTERVAL", raising=False)

    config.buffer_enabled = True
    config.buffer_flush_interval = 0.2

    Setup.initialize("EZTRACE_BUFFERING_CONFIG", show_metrics=False)
    Logging(log_format="plain")

    logger = logging.getLogger("pyeztrace")
    buffered_handlers = [h for h in logger.handlers if isinstance(h, BufferedHandler)]

    assert len(buffered_handlers) == 2
    assert {h.flush_interval for h in buffered_handlers} == {0.2}

def test_log_info_and_error(monkeypatch):
    Setup.initialize("EZTRACER_LOG", show_metrics=False)
    log = Logging(log_format="plain")
    # Should not raise
    log.log_info("info message", function="test_func")
    log.log_error("error message", function="test_func")


def test_disable_file_logging_via_config(tmp_path, reset_logging_state, restore_config):
    config.log_dir = str(tmp_path / "logs")
    config.log_file = "no-file.log"
    config.disable_file_logging = True

    Setup.initialize("NO_FILE", show_metrics=False)
    log = Logging(log_format="plain")
    log.log_info("info message", function="test_func")
    Logging.flush_logs()

    assert not config.get_log_path().exists()


def test_disable_file_logging_via_initializer(tmp_path, reset_logging_state, restore_config):
    config.log_dir = str(tmp_path / "logs")
    config.log_file = "no-file.log"
    config.disable_file_logging = False

    Setup.initialize("NO_FILE", show_metrics=False, disable_file_logging=True)
    log = Logging(log_format="plain")
    log.log_info("info message", function="test_func")
    Logging.flush_logs()

    assert not config.get_log_path().exists()


def test_file_logging_enabled_by_default(tmp_path, reset_logging_state, restore_config):
    config.log_dir = str(tmp_path / "logs")
    config.log_file = "file.log"
    config.disable_file_logging = False

    Setup.initialize("FILE_OK", show_metrics=False)
    log = Logging(log_format="plain")
    log.log_info("info message", function="test_func")
    Logging.flush_logs()

    log_path = config.get_log_path()
    assert log_path.exists()
    assert log_path.read_text().strip()

def test_default_console_color_file_json(tmp_path, reset_logging_state, restore_config, monkeypatch):
    config.log_dir = str(tmp_path / "logs")
    config.log_file = "default.log"
    config.disable_file_logging = False

    Setup.initialize("DEFAULT_FORMATS", show_metrics=False)

    buf = io.StringIO()
    monkeypatch.setattr(sys, "__stdout__", buf, raising=False)

    log = Logging()
    log.log_info("hello", function="test_func")
    Logging.flush_logs()

    console_out = buf.getvalue()
    assert "\x1b[" in console_out  # ANSI color escape
    assert '"timestamp"' not in console_out  # file JSON should not leak to console

    log_path = config.get_log_path()
    lines = [l for l in log_path.read_text().splitlines() if l.strip()]
    assert lines
    assert "\x1b[" not in lines[-1]
    payload = json.loads(lines[-1])
    assert payload["message"] == "hello"

def test_log_format_json(monkeypatch):
    Setup.initialize("EZTRACER_LOG2", show_metrics=False)
    log = Logging(log_format="json")
    msg = log._format_message("INFO", "msg", function="f", duration=1.23)
    import json
    data = json.loads(msg)
    assert data["function"] == "f"
    assert data["duration"] == 1.23

def simulate_complex_operation(log, depth=0, max_depth=3):
    """Simulate a complex operation with nested calls and random delays"""
    if depth >= max_depth:
        return
    
    operations = ['database_query', 'api_call', 'file_operation', 'cache_lookup']
    operation = random.choice(operations)
    duration = random.uniform(0.1, 0.5)
    
    log.log_info(f"Starting {operation}", 
                 function=f"level_{depth}_{operation}",
                 duration=duration,
                 depth=depth,
                 operation_type=operation)
    
    time.sleep(duration)  # Simulate work
    
    if random.random() < 0.2:  # 20% chance of error
        try:
            raise Exception(f"Simulated error in {operation}")
        except Exception as e:
            log.log_error(str(e), 
                         function=f"level_{depth}_{operation}",
                         error_type="SimulatedError",
                         depth=depth)
    
    # Recursive call to simulate nested operations
    simulate_complex_operation(log, depth + 1, max_depth)
    
    log.log_info(f"Completed {operation}",
                 function=f"level_{depth}_{operation}",
                 duration=duration,
                 depth=depth,
                 operation_type=operation)

def test_concurrent_logging():
    """Test logging from multiple concurrent threads"""
    Setup.initialize("EZTRACER_CONCURRENT", show_metrics=True)
    log = Logging(log_format="json")
    
    def worker(worker_id):
        for _ in range(5):  # Each worker does 5 operations
            simulate_complex_operation(log)
            time.sleep(random.uniform(0.1, 0.3))
    
    # Create 5 concurrent workers
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker, i) for i in range(5)]
        # Wait for all workers to complete
        [f.result() for f in futures]

def test_high_volume_logging():
    """Test handling of high volume of log messages"""
    Setup.initialize("EZTRACER_VOLUME", show_metrics=True)
    log = Logging(log_format="json")
    
    start_time = time.time()
    message_count = 1000
    
    for i in range(message_count):
        log.log_info(
            f"High volume message {i}",
            function="volume_test",
            iteration=i,
            timestamp=time.time(),
            custom_field=f"value_{i % 10}"
        )
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Basic performance assertion - should handle 1000 messages reasonably quickly
    assert duration < 2.0, f"Logging {message_count} messages took too long: {duration:.2f} seconds"

def test_complex_json_formatting():
    """Test logging with complex nested JSON structures"""
    Setup.initialize("EZTRACER_COMPLEX", show_metrics=True)
    log = Logging(log_format="json")
    
    complex_data = {
        "user": {
            "id": 123,
            "preferences": {
                "theme": "dark",
                "notifications": ["email", "push"],
                "filters": [{"name": "priority", "value": "high"}]
            }
        },
        "session": {
            "id": "abc123",
            "start_time": time.time(),
            "metadata": {
                "source": "web",
                "browser": "chrome",
                "platform": "macos"
            }
        }
    }
    
    msg = log._format_message(
        "INFO",
        "Complex data structure logging",
        function="complex_json_test",
        data=complex_data
    )
    
    # Verify the complex data structure was properly serialized
    parsed = json.loads(msg)
    assert parsed["data"]["data"]["user"]["id"] == 123
    assert isinstance(parsed["data"]["data"]["session"]["start_time"], (int, float))
    assert parsed["data"]["data"]["user"]["preferences"]["theme"] == "dark"

def test_error_handling_and_recovery():
    """Test logging system's error handling and recovery capabilities"""
    Setup.initialize("EZTRACER_ERROR", show_metrics=True)
    log = Logging(log_format="json")
    
    def simulate_problematic_logging():
        # Test logging with various problematic inputs
        problematic_inputs = [
            None,  # None message
            "".join(chr(i) for i in range(128, 256)),  # Invalid Unicode
            "A" * 1000000,  # Very large message
            {"circular": None}  # Potentially problematic structure
        ]
        
        for input_data in problematic_inputs:
            try:
                log.log_info(str(input_data), function="error_test", problematic_data=input_data)
            except Exception as e:
                # Log should handle or properly report the error
                log.log_error(f"Logging error: {str(e)}", 
                            function="error_test",
                            error_type=type(e).__name__)
    
    simulate_problematic_logging()
    
    # Verify logging still works after error conditions
    log.log_info("Recovery test", function="error_test")

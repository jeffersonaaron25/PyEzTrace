import io
import logging
import sys

import pytest

from pyeztrace.config import config
from pyeztrace.custom_logging import Logging
from pyeztrace.setup import Setup


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
def disable_file_logging():
    original_disable = config.disable_file_logging
    original_log_dir = config.log_dir
    original_log_file = config.log_file
    config.disable_file_logging = True
    config.log_dir = "."
    config.log_file = "test.log"
    yield
    config.disable_file_logging = original_disable
    config.log_dir = original_log_dir
    config.log_file = original_log_file


def test_print_falls_back_before_setup(capsys):
    from pyeztrace import printing

    printing.print("hello", end="")
    captured = capsys.readouterr()
    assert captured.out == "hello"


def test_print_logs_when_initialized(capsys, reset_logging_state, disable_file_logging, monkeypatch):
    buf = io.StringIO()
    monkeypatch.setattr(sys, "__stdout__", buf, raising=False)

    Setup.initialize("PRINT_LOG", show_metrics=False)
    # Configure logger once to avoid color codes in assertions.
    Logging(log_format="plain", disable_file_logging=True)

    from pyeztrace import print as ez_print

    ez_print("hello from print", level="WARNING")
    Logging.flush_logs()

    captured = buf.getvalue()
    assert "WARNING" in captured
    assert "hello from print" in captured

import json
from pathlib import Path

import pytest

from pyeztrace.cli import LogAnalyzer


@pytest.fixture()
def tmp_log(tmp_path: Path) -> Path:
    return tmp_path / "app.log"


def test_parse_plain_line_without_project(tmp_log: Path):
    tmp_log.write_text("2025-11-20T17:51:59 - INFO - called...\n")

    analyzer = LogAnalyzer(tmp_log)
    entries = analyzer.parse_logs()

    assert entries == [
        {
            "timestamp": "2025-11-20T17:51:59",
            "level": "INFO",
            "project": None,
            "message": "called...",
        }
    ]


def test_parse_colorized_plain_line(tmp_log: Path):
    color_prefix = "\x1b[32m"
    reset = "\x1b[0m"
    colored_line = f"{color_prefix}2025-11-20T17:51:59 - INFO - [MYAPP] called...{reset}\n"
    tmp_log.write_text(colored_line)

    analyzer = LogAnalyzer(tmp_log)
    entries = analyzer.parse_logs()

    assert entries == [
        {
            "timestamp": "2025-11-20T17:51:59",
            "level": "INFO",
            "project": "MYAPP",
            "message": "called...",
        }
    ]


def test_json_lines_are_preserved(tmp_log: Path):
    payload = {
        "timestamp": "2025-11-20T17:51:59",
        "level": "INFO",
        "message": "called...",
    }
    tmp_log.write_text(json.dumps(payload) + "\n")

    analyzer = LogAnalyzer(tmp_log)
    entries = analyzer.parse_logs()

    assert entries == [payload]

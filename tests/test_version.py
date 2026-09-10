"""The package version has exactly one source: pyproject.toml.

These tests exist because the OTEL bridge silently reported a stale version for
a whole release: it hardcoded the string, so a version bump left instrumentation
metadata pointing at the previous release.
"""
import re
import subprocess
import sys
from pathlib import Path

import pytest

from pyeztrace._version import PACKAGE_NAME, UNKNOWN_VERSION, get_version

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = REPO_ROOT / "pyproject.toml"


def _declared_version() -> str:
    """Read the version straight out of pyproject.toml.

    Parsed with a regex rather than tomllib so the test also runs on the
    project's minimum supported Python (3.9), which has no tomllib.
    """
    text = PYPROJECT.read_text(encoding="utf-8")
    match = re.search(r'(?m)^version\s*=\s*["\']([^"\']+)["\']', text)
    assert match, "could not find a version in pyproject.toml"
    return match.group(1)


def test_version_resolves_to_something_real():
    resolved = get_version()
    assert resolved
    assert resolved != UNKNOWN_VERSION


def test_resolved_version_matches_pyproject():
    assert get_version() == _declared_version()


def test_get_version_is_cached():
    """Span serialization asks per exported span; metadata lookup hits the disk."""
    get_version()
    before = get_version.cache_info()
    get_version()
    after = get_version.cache_info()
    assert after.hits == before.hits + 1
    assert after.misses == before.misses


def test_no_module_hardcodes_the_version():
    """The guard that the original defect needed.

    Any module embedding the current version literal will keep reporting it
    after the next bump. pyproject.toml is the only place it may appear.
    """
    version = _declared_version()
    offenders = []
    for path in sorted((REPO_ROOT / PACKAGE_NAME).rglob("*.py")):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if f'"{version}"' in line or f"'{version}'" in line:
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}: {line.strip()}")
    assert not offenders, (
        "version literal is hardcoded; import pyeztrace._version.get_version instead:\n"
        + "\n".join(offenders)
    )


def test_otel_instrumentation_version_is_not_hardcoded():
    """Specifically pin the two sites that carried the stale value."""
    pytest.importorskip("opentelemetry", reason="OTEL bridge requires optional dependencies")
    source = (REPO_ROOT / PACKAGE_NAME / "otel.py").read_text(encoding="utf-8")
    assert '"version": get_version()' in source
    assert '"library.version": get_version()' in source


def test_cli_version_flag_reports_the_resolved_version():
    result = subprocess.run(
        [sys.executable, "-m", "pyeztrace.cli", "--version"],
        capture_output=True, text=True, cwd=str(REPO_ROOT), timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert get_version() in (result.stdout + result.stderr)

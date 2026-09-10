"""Single source for the package version.

`pyproject.toml` declares the version; no other module should hardcode it.

Resolution is cached: span serialization asks for the version once per exported
span, and `importlib.metadata.version()` touches the filesystem on every call.
"""
from functools import lru_cache
from pathlib import Path
from typing import Optional

PACKAGE_NAME = "pyeztrace"
UNKNOWN_VERSION = "unknown"


@lru_cache(maxsize=1)
def get_version() -> str:
    """Return the installed package version, or "unknown" if undeterminable."""
    try:
        from importlib.metadata import version

        return version(PACKAGE_NAME)
    except Exception:
        pass

    try:
        import pkg_resources  # type: ignore[import-untyped]

        return pkg_resources.get_distribution(PACKAGE_NAME).version
    except Exception:
        pass

    return _version_from_pyproject() or UNKNOWN_VERSION


def _version_from_pyproject() -> Optional[str]:
    """Last resort for a source checkout that was never installed.

    Looks next to the package rather than in the process working directory:
    a CWD-relative lookup only worked when the caller happened to be run from
    the repository root.
    """
    try:
        import tomllib  # Python 3.11+; no third-party fallback by design
    except ImportError:
        return None

    package_dir = Path(__file__).resolve().parent
    for candidate in (package_dir / "pyproject.toml", package_dir.parent / "pyproject.toml"):
        if not candidate.is_file():
            continue
        try:
            with candidate.open("rb") as handle:
                data = tomllib.load(handle)
        except (OSError, ValueError):
            continue
        project = data.get("project")
        if not isinstance(project, dict):
            continue
        # Only trust a pyproject that actually describes this package, in case
        # the package is vendored inside an unrelated project tree.
        if project.get("name") != PACKAGE_NAME:
            continue
        found = project.get("version")
        if isinstance(found, str) and found:
            return found
    return None

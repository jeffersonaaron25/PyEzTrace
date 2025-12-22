"""Convenience helpers for replacing ``print`` with EzTrace logging."""

from __future__ import annotations

import asyncio
import builtins
import sys
from typing import Any, Callable, Dict

from pyeztrace.custom_logging import Logging
from pyeztrace.setup import Setup


_LEVEL_TO_LOG_FN: Dict[str, Callable[[str], None]] = {
    "DEBUG": Logging.log_debug,
    "INFO": Logging.log_info,
    "WARNING": Logging.log_warning,
    "ERROR": Logging.log_error,
}


def _log_message(level: str, message: str) -> None:
    """Log a message at the requested level using :class:`Logging`.

    Falls back to ``INFO`` when an unknown level is provided.
    """

    log_fn = _LEVEL_TO_LOG_FN.get(level.upper(), Logging.log_info)
    log_fn(message)


def _ensure_logging_initialized() -> None:
    """Initialize logging if it hasn't been configured yet."""

    if not Logging._configured:  # type: ignore[attr-defined]
        Logging()


def print(
    *objects: Any,
    sep: str = " ",
    end: str = "\n",
    file=None,
    flush: bool = False,
    level: str = "INFO",
) -> None:
    """Drop-in replacement for :func:`print` that logs via EzTrace.

    This helper allows codebases full of ``print`` statements to be redirected to
    the EzTrace logger with minimal changes. When EzTrace is not initialized yet
    (or when writing to a custom ``file``), it falls back to Python's built-in
    :func:`print` to preserve expected behavior.

    In async contexts, the actual log call is scheduled on the default executor
    to avoid blocking the event loop, while synchronous callers log inline.
    """

    message = sep.join(str(obj) for obj in objects)
    if end and end != "\n":
        message += end

    # Preserve built-in behavior when directing output to a custom file-like object.
    if file not in (None, sys.stdout, sys.stderr):
        builtins.print(*objects, sep=sep, end=end, file=file, flush=flush)
        return

    # If EzTrace hasn't been set up yet, act like the built-in print.
    if not Setup.is_setup_done():
        builtins.print(*objects, sep=sep, end=end, file=file, flush=flush)
        return

    try:
        _ensure_logging_initialized()
    except Exception:
        builtins.print(*objects, sep=sep, end=end, file=file, flush=flush)
        return

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop (or synchronous code); log directly.
        _log_message(level, message)
        return

    def _suppress_exception(fut: "asyncio.Future[None]") -> None:
        try:
            fut.exception()
        except Exception:
            # Last-resort fallback to ensure logs aren't silently lost.
            builtins.print(*objects, sep=sep, end=end, file=file, flush=flush)

    future = loop.run_in_executor(None, _log_message, level, message)
    future.add_done_callback(_suppress_exception)


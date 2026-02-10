import threading
import asyncio
import contextvars
from typing import Optional
from pyeztrace import exceptions
from pyeztrace.config import config


class Setup:
    """
    A class to manage the setup state of the application (thread-safe and asyncio-safe).
    """
    __project = None
    __setup_done = False
    __thread_level = threading.local()
    __async_level = contextvars.ContextVar("async_level", default=0)
    __show_metrics = False
    __disable_file_logging = None
    __lock = threading.Lock()
    __async_lock = asyncio.Lock()
    __metrics_registered = False
    __testing_mode = False

    @classmethod
    def _in_async_task(cls) -> bool:
        """Return True when called from within an active asyncio Task."""
        try:
            return asyncio.current_task() is not None
        except RuntimeError:
            return False

    # Methods for testing
    @classmethod
    def enable_testing_mode(cls):
        """
        Enable testing mode. In testing mode:
        - Logging doesn't write to files
        - Log messages are captured for inspection
        - No side effects to real application monitoring
        """
        with cls.__lock:
            cls.__testing_mode = True
            cls._captured_logs = []

    @classmethod
    def disable_testing_mode(cls):
        """Disable testing mode."""
        with cls.__lock:
            cls.__testing_mode = False
            if hasattr(cls, '_captured_logs'):
                delattr(cls, '_captured_logs')

    @classmethod
    def is_testing_mode(cls):
        """Check if testing mode is enabled."""
        with cls.__lock:
            return cls.__testing_mode

    @classmethod
    def get_captured_logs(cls):
        """Get logs captured in testing mode."""
        with cls.__lock:
            if not cls.__testing_mode:
                raise exceptions.SetupError("Not in testing mode. No logs captured.")
            return cls._captured_logs.copy() if hasattr(cls, '_captured_logs') else []

    @classmethod
    def capture_log(cls, log_entry):
        """Capture a log entry in testing mode."""
        with cls.__lock:
            if cls.__testing_mode and hasattr(cls, '_captured_logs'):
                cls._captured_logs.append(log_entry)

    @classmethod
    def clear_captured_logs(cls):
        """Clear captured logs in testing mode."""
        with cls.__lock:
            if cls.__testing_mode and hasattr(cls, '_captured_logs'):
                cls._captured_logs.clear()

    # Synchronous methods (thread-safe)
    @classmethod
    def _apply_runtime_config_overrides(
        cls,
        *,
        log_format: Optional[str] = None,
        console_format: Optional[str] = None,
        file_format: Optional[str] = None,
        log_level: Optional[str] = None,
        log_file: Optional[str] = None,
        log_dir: Optional[str] = None,
        max_size: Optional[int] = None,
        backup_count: Optional[int] = None,
        buffer_enabled: Optional[bool] = None,
        buffer_flush_interval: Optional[float] = None,
    ) -> None:
        """Apply explicit config overrides before logger initialization."""
        if log_format is not None:
            config.format = log_format
        if console_format is not None:
            config.console_format = console_format
        if file_format is not None:
            config.file_format = file_format
        if log_level is not None:
            config.log_level = log_level
        if log_file is not None:
            config.log_file = log_file
        if log_dir is not None:
            config.log_dir = log_dir
        if max_size is not None:
            config.max_size = max_size
        if backup_count is not None:
            config.backup_count = backup_count
        if buffer_enabled is not None:
            config.buffer_enabled = buffer_enabled
        if buffer_flush_interval is not None:
            config.buffer_flush_interval = buffer_flush_interval

    @classmethod
    def initialize(
        cls,
        project="eztracer",
        show_metrics=False,
        disable_file_logging=None,
        *,
        log_format: Optional[str] = None,
        console_format: Optional[str] = None,
        file_format: Optional[str] = None,
        log_level: Optional[str] = None,
        log_file: Optional[str] = None,
        log_dir: Optional[str] = None,
        max_size: Optional[int] = None,
        backup_count: Optional[int] = None,
        buffer_enabled: Optional[bool] = None,
        buffer_flush_interval: Optional[float] = None,
    ):
        with cls.__lock:
            if cls.__setup_done:
                raise exceptions.SetupAlreadyDoneError("Setup is already done.")
            cls.__setup_done = True
            cls.__thread_level.value = 0
            cls.__project = project.upper()
            cls.__show_metrics = show_metrics
            cls._apply_runtime_config_overrides(
                log_format=log_format,
                console_format=console_format,
                file_format=file_format,
                log_level=log_level,
                log_file=log_file,
                log_dir=log_dir,
                max_size=max_size,
                backup_count=backup_count,
                buffer_enabled=buffer_enabled,
                buffer_flush_interval=buffer_flush_interval,
            )
            if disable_file_logging is None:
                cls.__disable_file_logging = config.disable_file_logging
            else:
                cls.__disable_file_logging = disable_file_logging
            if show_metrics:
                cls._register_metrics_handler()

    @classmethod
    def _register_metrics_handler(cls):
        """Register the atexit handler for metrics if not already registered"""
        if not cls.__metrics_registered:
            from pyeztrace.custom_logging import Logging
            import atexit
            atexit.register(Logging.log_final_metrics_summary)
            cls.__metrics_registered = True

    @classmethod
    def is_setup_done(cls):
        with cls.__lock:
            return cls.__setup_done

    @classmethod
    def set_setup_done(cls):
        with cls.__lock:
            cls.__setup_done = True

    @classmethod
    def increment_level(cls):
        with cls.__lock:
            if cls._in_async_task():
                current = cls.__async_level.get()
                cls.__async_level.set(current + 1)
            else:
                if not hasattr(cls.__thread_level, "value"):
                    cls.__thread_level.value = 0
                cls.__thread_level.value += 1

    @classmethod
    def decrement_level(cls):
        with cls.__lock:
            if cls._in_async_task():
                current = cls.__async_level.get()
                cls.__async_level.set(current - 1)
            else:
                if not hasattr(cls.__thread_level, "value"):
                    cls.__thread_level.value = 0
                cls.__thread_level.value -= 1

    @classmethod
    def get_level(cls):
        with cls.__lock:
            if cls._in_async_task():
                return cls.__async_level.get()
            return getattr(cls.__thread_level, "value", 0)

    @classmethod
    def get_project(cls):
        with cls.__lock:
            return cls.__project

    # Async methods (asyncio-safe)
    @classmethod
    async def async_initialize(cls, project="eztracer"):
        async with cls.__async_lock:
            with cls.__lock:
                if cls.__setup_done:
                    raise exceptions.SetupAlreadyDoneError("Setup is already done.")
                cls.__setup_done = True
                cls.__async_level.set(0)
                cls.__project = project.upper()

    @classmethod
    async def async_is_setup_done(cls):
        async with cls.__async_lock:
            with cls.__lock:
                return cls.__setup_done

    @classmethod
    async def async_set_setup_done(cls):
        async with cls.__async_lock:
            with cls.__lock:
                cls.__setup_done = True

    @classmethod
    async def async_increment_level(cls):
        async with cls.__async_lock:
            with cls.__lock:
                current = cls.__async_level.get()
                cls.__async_level.set(current + 1)

    @classmethod
    async def async_decrement_level(cls):
        async with cls.__async_lock:
            with cls.__lock:
                current = cls.__async_level.get()
                cls.__async_level.set(current - 1)

    @classmethod
    async def async_get_level(cls):
        async with cls.__async_lock:
            with cls.__lock:
                return cls.__async_level.get()

    @classmethod
    async def async_get_project(cls):
        async with cls.__async_lock:
            with cls.__lock:
                return cls.__project
        
    @classmethod
    def set_show_metrics(cls, show_metrics: bool):
        """
        Set whether to show metrics or not.
        """
        with cls.__lock:
            cls.__show_metrics = show_metrics
            if show_metrics:
                cls._register_metrics_handler()

    @classmethod
    def get_show_metrics(cls) -> bool:
        """
        Get whether to show metrics or not.
        """
        with cls.__lock:
            return cls.__show_metrics

    @classmethod
    def reset(cls):
        """Reset all class variables to their initial state. Used primarily for testing."""
        with cls.__lock:
            cls.__project = None
            cls.__setup_done = False
            cls.__thread_level.value = 0
            cls.__show_metrics = False
            cls.__disable_file_logging = None
        cls.__async_level.set(0)

    @classmethod
    def set_project(cls, project: str) -> None:
        """Change the project name after initialization.
        
        Args:
            project: The new project name
        """
        with cls.__lock:
            if not cls.__setup_done:
                raise exceptions.SetupNotDoneError("Setup must be done before setting project name.")
            cls.__project = project.upper()

    @classmethod
    def get_disable_file_logging(cls) -> bool:
        with cls.__lock:
            if cls.__disable_file_logging is None:
                return config.disable_file_logging
            return cls.__disable_file_logging

    @classmethod
    def set_disable_file_logging(cls, disable: bool) -> None:
        with cls.__lock:
            cls.__disable_file_logging = disable

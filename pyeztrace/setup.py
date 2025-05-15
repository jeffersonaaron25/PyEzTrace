import threading
import asyncio
import contextvars
from pyeztrace import exceptions


class Setup:
    """
    A class to manage the setup state of the application (thread-safe and asyncio-safe).
    """
    __project = None
    __setup_done = False
    __thread_level = threading.local()
    __async_level = contextvars.ContextVar("async_level", default=0)
    __show_metrics = False
    __lock = threading.Lock()
    __async_lock = asyncio.Lock()
    __metrics_registered = False


    # Synchronous methods (thread-safe)
    @classmethod
    def initialize(cls, project="eztracer", show_metrics=False):
        with cls.__lock:
            if cls.__setup_done:
                raise exceptions.SetupAlreadyDoneError("Setup is already done.")
            cls.__setup_done = True
            cls.__thread_level.value = 0
            cls.__project = project.upper()
            cls.__show_metrics = show_metrics
            if show_metrics:
                cls._register_metrics_handler()

    @classmethod
    def _register_metrics_handler(cls):
        """Register the atexit handler for metrics if not already registered"""
        if not cls.__metrics_registered:
            from pyeztrace.custom_logging import Logging
            import atexit
            atexit.register(Logging.log_metrics_summary)
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
            if not hasattr(cls.__thread_level, "value"):
                cls.__thread_level.value = 0
            cls.__thread_level.value += 1

    @classmethod
    def decrement_level(cls):
        with cls.__lock:
            if not hasattr(cls.__thread_level, "value"):
                cls.__thread_level.value = 0
            cls.__thread_level.value -= 1

    @classmethod
    def get_level(cls):
        with cls.__lock:
            return getattr(cls.__thread_level, "value", 0)

    @classmethod
    def get_project(cls):
        with cls.__lock:
            return cls.__project

    # Async methods (asyncio-safe)
    @classmethod
    async def async_initialize(cls, project="eztracer"):
        async with cls.__async_lock:
            if cls.__setup_done:
                raise exceptions.SetupAlreadyDoneError("Setup is already done.")
            cls.__setup_done = True
            cls.__async_level.set(0)
            cls.__project = project.upper()

    @classmethod
    async def async_is_setup_done(cls):
        async with cls.__async_lock:
            return cls.__setup_done

    @classmethod
    async def async_set_setup_done(cls):
        async with cls.__async_lock:
            cls.__setup_done = True

    @classmethod
    async def async_increment_level(cls):
        async with cls.__async_lock:
            current = cls.__async_level.get()
            cls.__async_level.set(current + 1)

    @classmethod
    async def async_decrement_level(cls):
        async with cls.__async_lock:
            current = cls.__async_level.get()
            cls.__async_level.set(current - 1)

    @classmethod
    async def async_get_level(cls):
        async with cls.__async_lock:
            return cls.__async_level.get()

    @classmethod
    async def async_get_project(cls):
        async with cls.__async_lock:
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

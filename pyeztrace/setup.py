import threading
import asyncio
from pyeztrace import exceptions


class Setup:
    """
    A class to manage the setup state of the application (thread-safe and asyncio-safe).
    """
    __project = None
    __setup_done = False
    __level = 0
    __show_metrics = False
    __lock = threading.Lock()
    __async_lock = asyncio.Lock()


    # Synchronous methods (thread-safe)
    @classmethod
    def initialize(cls, project="eztracer", show_metrics=False):
        with cls.__lock:
            if cls.__setup_done:
                raise exceptions.SetupAlreadyDoneError("Setup is already done.")
            cls.__setup_done = True
            cls.__level = 0
            cls.__project = project.upper()
            cls.__show_metrics = show_metrics

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
            cls.__level += 1

    @classmethod
    def decrement_level(cls):
        with cls.__lock:
            cls.__level -= 1

    @classmethod
    def get_level(cls):
        with cls.__lock:
            return cls.__level

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
            cls.__level = 0
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
            cls.__level += 1

    @classmethod
    async def async_decrement_level(cls):
        async with cls.__async_lock:
            cls.__level -= 1

    @classmethod
    async def async_get_level(cls):
        async with cls.__async_lock:
            return cls.__level

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
            cls.__level = 0
            cls.__show_metrics = False

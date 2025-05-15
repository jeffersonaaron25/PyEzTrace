
import time
import contextvars
import types
import inspect
import sys
import threading
import atexit
import fnmatch
from typing import Any, Callable, Optional, Sequence, Union, Dict, List
from pyeztrace.setup import Setup
from pyeztrace.custom_logging import Logging

def ensure_initialized():
    """Ensure EzTrace is initialized with sensible defaults if not already done."""
    if not Setup.is_setup_done():
        project_name = sys.argv[0].split('/')[-1].replace('.py', '') if sys.argv else "EzTrace"
        Setup.initialize(project_name)
        Setup.set_setup_done()
    return Logging()

logging = ensure_initialized()

def _safe_to_wrap(obj):
    # Only wrap python-level functions / coroutines / builtins, skip everything else
    return (
        inspect.isfunction(obj)
        or inspect.iscoroutinefunction(obj)
        or (inspect.isbuiltin(obj) and getattr(obj, "__module__", "").startswith("builtins"))
    )

# ContextVar to indicate tracing is active
tracing_active = contextvars.ContextVar("tracing_active", default=False)

class trace_children_in_module:
    """
    Context manager to monkey-patch all functions in a module (or class) with a child-tracing decorator.
    Robust for concurrent tracing: uses per-thread and per-coroutine reference counting and locking.
    Only active when tracing_active is True.
    """
    _thread_local = threading.local()
    _coroutine_local = contextvars.ContextVar("trace_patch_ref", default=None)

    def __init__(self, module_or_class: Any, child_decorator: Callable[[Callable[..., Any]], Callable[..., Any]]) -> None:
        self.module_or_class = module_or_class
        self.child_decorator = child_decorator
        self.originals: Dict[str, Callable[..., Any]] = {}
        self._is_thread = threading.current_thread() is not None

    def _get_ref_counter(self) -> dict:
        # Prefer coroutine-local if inside a coroutine, else thread-local
        try:
            # If running in an event loop, use contextvar
            import asyncio
            if asyncio.get_event_loop().is_running():
                ref = trace_children_in_module._coroutine_local.get()
                if ref is None:
                    ref = {}
                    trace_children_in_module._coroutine_local.set(ref)
                return ref
        except Exception:
            pass
        # Fallback to thread-local
        if not hasattr(trace_children_in_module._thread_local, "ref"):
            trace_children_in_module._thread_local.ref = {}
        return trace_children_in_module._thread_local.ref

    def __enter__(self) -> None:
        ref_counter = self._get_ref_counter()
        key = id(self.module_or_class)
        if key not in ref_counter:
            # First entry for this context: patch
            ref_counter[key] = 1
            if isinstance(self.module_or_class, types.ModuleType):
                items = self.module_or_class.__dict__.items()
            else:
                items = self.module_or_class.__dict__.items()
            for name, obj in items:
                if not _safe_to_wrap(obj):          # skip early
                    continue
                if callable(obj) and not (name.startswith("__") and not name in ["__call__", "__init__"]):
                    self.originals[name] = obj
                    setattr(self.module_or_class, name, self.child_decorator(obj))
        else:
            # Nested/concurrent: just increment
            ref_counter[key] += 1

    async def __aenter__(self) -> 'trace_children_in_module':
        self.__enter__()
        return self

    async def __aexit__(self) -> None:
        self.__exit__()

    def __exit__(self) -> None:
        ref_counter = self._get_ref_counter()
        key = id(self.module_or_class)
        if key in ref_counter:
            ref_counter[key] -= 1
            if ref_counter[key] == 0:
                # Last exit for this context: restore
                for name, obj in self.originals.items():
                    setattr(self.module_or_class, name, obj)
                del ref_counter[key]

def child_trace_decorator(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator for child functions: only logs if tracing_active is True.
    """
    import functools
    if inspect.iscoroutinefunction(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if tracing_active.get():
                Setup.increment_level()
                logging.log_info(f"called...", type="child", function=func.__qualname__)
                start = time.time()
                try:
                    result = await func(*args, **kwargs)
                    end = time.time()
                    duration = end - start
                    logging.log_info(f"Ok.", type="child", function=func.__qualname__, duration=duration)
                    logging.record_metric(f"{func.__qualname__}", duration)
                    return result
                except Exception as e:
                    logging.log_error(f"Error: {str(e)}", type="child", function=func.__qualname__)
                    logging.raise_exception_to_log(e, str(e), stack=False)
                    raise
                finally:
                    Setup.decrement_level()
            else:
                return func(*args, **kwargs)
        return wrapper
    else:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if tracing_active.get():
                Setup.increment_level()
                logging.log_info(f"called...", type="child", function=func.__qualname__)
                start = time.time()
                try:
                    result = func(*args, **kwargs)
                    end = time.time()
                    duration = end - start
                    logging.log_info(f"Ok.", type="child", function=func.__qualname__, duration=duration)
                    logging.record_metric(f"{func.__qualname__}", duration)
                    return result
                except Exception as e:
                    logging.log_error(f"Error: {str(e)}", type="child", function=func.__qualname__)
                    logging.raise_exception_to_log(e, str(e), stack=False)
                    raise
                finally:
                    Setup.decrement_level()
            else:
                return func(*args, **kwargs)
        return wrapper

def trace(
    message: Optional[str] = None,
    stack: bool = False,
    modules_or_classes: Optional[Union[Any, Sequence[Any]]] = None,
    include: Optional[Sequence[str]] = None,
    exclude: Optional[Sequence[str]] = None
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator for parent function. Enables tracing for all child functions in the given modules or classes.
    If modules_or_classes is None, it will automatically patch the module where the parent function is defined.
    Accepts a single module/class or a list of modules/classes for cross-module tracing.
    Handles both sync and async parent functions.
    Supports selective tracing via include/exclude patterns (function names).
    """
    def _should_trace(func_name: str) -> bool:
        if include:
            if not any(fnmatch.fnmatch(func_name, pat) for pat in include):
                return False
        if exclude:
            if any(fnmatch.fnmatch(func_name, pat) for pat in exclude):
                return False
        return True

    def make_child_decorator(orig_decorator):
        def selective_decorator(func):
            if _safe_to_wrap(func) and hasattr(func, "__name__") and _should_trace(func.__name__):
                return orig_decorator(func)
            return func
        return selective_decorator

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        def _get_targets() -> List[Any]:
            # Accepts a single module/class or a list/tuple
            targets = []
            if modules_or_classes is None:
                mod = sys.modules.get(func.__module__)
                if mod is not None:
                    targets.append(mod)
            elif isinstance(modules_or_classes, (list, tuple, set)):
                targets.extend(modules_or_classes)
            else:
                targets.append(modules_or_classes)
            return targets

        import functools
        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                token = tracing_active.set(True)
                Setup.increment_level()
                logging.log_info(f"called...", type="parent", function=func.__qualname__)
                start = time.time()
                try:
                    targets = _get_targets()
                    if targets:
                        managers = [trace_children_in_module(t, make_child_decorator(child_trace_decorator)) for t in targets]
                        for m in managers:
                            await m.__aenter__()
                        try:
                            result = await func(*args, **kwargs)
                        finally:
                            for m in reversed(managers):
                                await m.__aexit__()
                    else:
                        result = await func(*args, **kwargs)
                    end = time.time()
                    duration = end - start
                    logging.log_info(f"Ok.", type="parent", function=func.__qualname__, duration=duration)
                    logging.record_metric(f"{func.__qualname__}", duration)
                    return result
                except Exception as e:
                    logging.log_error(f"Error: {str(e)}", type="parent", function=func.__qualname__)
                    error_message = f"{message} -> {str(e)}" if message else str(e)
                    logging.raise_exception_to_log(e, error_message, stack)
                    raise
                finally:
                    Setup.decrement_level()
                    tracing_active.reset(token)
            return async_wrapper
        else:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                token = tracing_active.set(True)
                Setup.increment_level()
                logging.log_info(f"called...", type="parent", function=func.__name__)
                start = time.time()
                try:
                    targets = _get_targets()
                    if targets:
                        managers = [trace_children_in_module(t, make_child_decorator(child_trace_decorator)) for t in targets]
                        for m in managers:
                            m.__enter__()
                        try:
                            result = func(*args, **kwargs)
                        finally:
                            for m in reversed(managers):
                                m.__exit__()
                    else:
                        result = func(*args, **kwargs)
                    end = time.time()
                    duration = end - start
                    logging.log_info(f"Ok.", type="parent", function=f"{func.__qualname__}", duration=duration)
                    logging.record_metric(f"{func.__qualname__}", duration)
                    return result
                except Exception as e:
                    logging.log_error(f"Error: {str(e)}", type="parent", function=f"{func.__qualname__}")
                    error_message = f"{message} -> {str(e)}" if message else str(e)
                    logging.raise_exception_to_log(e, error_message, stack)
                    raise
                finally:
                    Setup.decrement_level()
                    tracing_active.reset(token)
            return wrapper
    return decorator
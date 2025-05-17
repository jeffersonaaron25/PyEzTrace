import time
import contextvars
import types
import inspect
import sys
import threading
import fnmatch
from typing import Any, Callable, Optional, Sequence, Union, Dict, List
from pyeztrace.setup import Setup
from pyeztrace.custom_logging import Logging

def ensure_initialized():
    """Ensure EzTrace is initialized with sensible defaults if not already done."""
    try:
        if not Setup.is_setup_done():
            try:
                # Try to get main script name
                project_name = sys.argv[0].split('/')[-1].replace('.py', '') if sys.argv else None
                
                # If running in interactive mode or argv[0] is empty, try module name
                if not project_name or project_name == '' or project_name == '-c':
                    # Try to get calling module name
                    frame = inspect.currentframe()
                    if frame:
                        try:
                            frame = frame.f_back
                            if frame and frame.f_globals:
                                module_name = frame.f_globals.get('__name__')
                                if module_name and module_name != '__main__':
                                    project_name = module_name.split('.')[0]
                        finally:
                            del frame
                
                # Fallback 
                if not project_name or project_name == '':
                    project_name = "EzTrace"
                    
                Setup.initialize(project_name)
                Setup.set_setup_done()
            except Exception as e:
                # Last resort fallback
                Setup.initialize("EzTrace")
                Setup.set_setup_done()
                print(f"Warning: Error during tracer initialization: {str(e)}")
    except Exception as e:
        # Catastrophic failure - print and continue
        print(f"Critical error in EzTrace initialization: {str(e)}")
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

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.__exit__(None, None, None)

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
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
    
    # Skip decoration entirely if we know we're not in a tracing context
    # This avoids the additional function call overhead
    if not tracing_active.get():
        return func
        
    if inspect.iscoroutinefunction(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if not tracing_active.get():
                return await func(*args, **kwargs)
                
            Setup.increment_level()
            logging.log_info(f"called...", fn_type="child", function=func.__qualname__)
            start = time.time()
            try:
                result = await func(*args, **kwargs)
                end = time.time()
                duration = end - start
                logging.log_info(f"Ok.", fn_type="child", function=func.__qualname__, duration=duration)
                logging.record_metric(func.__qualname__, duration)
                return result
            except Exception as e:
                logging.log_error(f"Error: {str(e)}", fn_type="child", function=func.__qualname__)
                logging.raise_exception_to_log(e, str(e), stack=False)
                raise
            finally:
                Setup.decrement_level()
        return wrapper
    else:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not tracing_active.get():
                return func(*args, **kwargs)
                
            Setup.increment_level()
            logging.log_info(f"called...", fn_type="child", function=func.__qualname__)
            start = time.time()
            try:
                result = func(*args, **kwargs)
                end = time.time()
                duration = end - start
                logging.log_info(f"Ok.", fn_type="child", function=func.__qualname__, duration=duration)
                logging.record_metric(func.__qualname__, duration)
                return result
            except Exception as e:
                logging.log_error(f"Error: {str(e)}", fn_type="child", function=func.__qualname__)
                logging.raise_exception_to_log(e, str(e), stack=False)
                raise
            finally:
                Setup.decrement_level()
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
                try:
                    # Initialize if not already done
                    ensure_initialized()
                    token = tracing_active.set(True)
                    Setup.increment_level()
                    logging.log_info(f"called...", fn_type="parent", function=func.__qualname__)
                    start = time.time()
                    
                    targets = _get_targets()
                    managers = []
                    try:
                        if targets:
                            managers = [trace_children_in_module(t, make_child_decorator(child_trace_decorator)) for t in targets]
                            for m in managers:
                                await m.__aenter__()
                        result = await func(*args, **kwargs)
                        end = time.time()
                        duration = end - start
                        logging.log_info(f"Ok.", fn_type="parent", function=func.__qualname__, duration=duration)
                        logging.record_metric(func.__qualname__, duration)
                        return result
                    except Exception as e:
                        logging.log_error(f"Error: {str(e)}", fn_type="parent", function=func.__qualname__)
                        error_message = f"{message} -> {str(e)}" if message else str(e)
                        logging.raise_exception_to_log(e, error_message, stack)
                        raise
                    finally:
                        # Clean up context managers even if an exception occurs
                        for m in reversed(managers):
                            try:
                                await m.__aexit__(None, None, None)
                            except Exception:
                                pass  # Prevent cleanup exceptions from masking the original error
                except Exception as e:
                    # Fallback for catastrophic failures in the tracing setup
                    print(f"TRACE ERROR: {func.__qualname__} - {str(e)}")
                    import traceback
                    traceback.print_exc()
                    return await func(*args, **kwargs)
                finally:
                    try:
                        Setup.decrement_level()
                        tracing_active.reset(token)
                    except Exception:
                        pass  # Last-resort exception handling
            return async_wrapper
        else:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    # Initialize if not already done
                    ensure_initialized()
                    token = tracing_active.set(True)
                    Setup.increment_level()
                    logging.log_info(f"called...", fn_type="parent", function=func.__name__)
                    start = time.time()
                    
                    targets = _get_targets()
                    managers = []
                    try:
                        if targets:
                            managers = [trace_children_in_module(t, make_child_decorator(child_trace_decorator)) for t in targets]
                            for m in managers:
                                m.__enter__()
                        result = func(*args, **kwargs)
                        end = time.time()
                        duration = end - start
                        logging.log_info(f"Ok.", fn_type="parent", function=func.__qualname__, duration=duration)
                        logging.record_metric(func.__qualname__, duration)
                        return result
                    except Exception as e:
                        logging.log_error(f"Error: {str(e)}", function=func.__qualname__)
                        error_message = f"{message} -> {str(e)}" if message else str(e)
                        logging.raise_exception_to_log(e, error_message, stack)
                        raise
                    finally:
                        # Clean up context managers even if an exception occurs
                        for m in reversed(managers):
                            try:
                                m.__exit__(None, None, None)
                            except Exception:
                                pass  # Prevent cleanup exceptions from masking the original error
                except Exception as e:
                    # Fallback for catastrophic failures in the tracing setup
                    print(f"TRACE ERROR: {func.__qualname__} - {str(e)}")
                    import traceback
                    traceback.print_exc()
                    return func(*args, **kwargs)
                finally:
                    try:
                        Setup.decrement_level()
                        tracing_active.reset(token)
                    except Exception:
                        pass  # Last-resort exception handling
            return wrapper
    return decorator
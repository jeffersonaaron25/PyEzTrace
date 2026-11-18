try:
    from builtins import BaseExceptionGroup
except ImportError:  # Python 3.9–3.10
    BaseExceptionGroup = ()


def protect_exception_details(exc):
    """Keep the original exception while marking its details private to tracing."""
    if isinstance(exc, BaseException):
        BaseException.__setattr__(exc, "_pyeztrace_private_details", True)


def has_private_exception_details(exc):
    pending = [exc]
    seen = set()
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if getattr(current, "_pyeztrace_private_details", False):
            return True
        pending.extend((current.__cause__, current.__context__))
        if isinstance(current, BaseExceptionGroup):
            pending.extend(current.exceptions)
    return False


class SetupNotDoneError(Exception):
    """
    Exception raised when setup is not done.
    """
    pass

class SetupAlreadyDoneError(Exception):
    """
    Exception raised when setup is already done.
    """
    pass

class SetupError(Exception):
    """
    Exception raised for general setup errors.
    """
    pass

class TracingError(Exception):
    """
    Exception raised for errors in the tracing system.
    """
    pass

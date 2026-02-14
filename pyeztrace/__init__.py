"""PyEzTrace package exports."""

__all__ = ["Logging", "trace", "set_global_redaction", "print"]


def __getattr__(name):
    if name == "Logging":
        from .custom_logging import Logging
        return Logging
    if name == "trace":
        from .tracer import trace
        return trace
    if name == "set_global_redaction":
        from .tracer import set_global_redaction
        return set_global_redaction
    if name == "print":
        from .printing import print as pyeztrace_print  # noqa: A001
        return pyeztrace_print
    raise AttributeError(f"module 'pyeztrace' has no attribute {name!r}")
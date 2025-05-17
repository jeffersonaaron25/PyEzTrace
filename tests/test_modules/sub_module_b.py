"""Sub-module B for testing recursive tracing."""

def module_b_function():
    """Function in sub-module B that should be discovered and traced recursively."""
    return 20

def module_b_helper():
    """Helper function in sub-module B that may be called internally."""
    return 15

# Import something from an external library to test that it doesn't cause issues
import os

def get_env():
    """Function that uses an external library."""
    return os.environ.get("TEST_VAR", "default") 
"""Sub-module A for testing recursive tracing."""

def module_a_function():
    """Function in sub-module A that should be discovered and traced recursively."""
    return 10

def module_a_helper():
    """Helper function in sub-module A that may be called internally."""
    return 5

# Import nested module to test deeper recursion
from tests.test_modules.nested import nested_function

def call_nested():
    """Function that calls a function from a nested module."""
    return nested_function() 
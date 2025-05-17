"""Main module for testing recursive tracing."""

def main_function():
    """Main function that will be directly traced."""
    result = submodule_function()
    return result * 10

def submodule_function():
    """Function in main module that will be traced because it's called by main_function."""
    return 5

# Import sub-modules that should be discovered by recursive tracing
from tests.test_modules.sub_module_a import module_a_function
from tests.test_modules.sub_module_b import module_b_function

def call_imported_modules():
    """Function that calls imported module functions."""
    a_result = module_a_function()
    b_result = module_b_function()
    return a_result + b_result 
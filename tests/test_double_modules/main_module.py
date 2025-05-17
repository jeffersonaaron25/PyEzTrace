
from pyeztrace.tracer import trace
from tests.test_double_modules.helper_module import HelperClass, helper_function, another_helper

# Will be traced directly and via recursive tracing
def main_function(value):
    helper = HelperClass(value)
    result = helper.get_value() + helper_function(value)
    return result

# Will be traced via two different recursive traces
def shared_function(value):
    return another_helper(value) + 5

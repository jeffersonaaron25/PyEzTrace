import pytest
import sys
import os
import importlib
from typing import List

from pyeztrace.setup import Setup
from pyeztrace.tracer import trace, tracing_active, _TRACED_ATTRIBUTE

# Setup path for import
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from tests.test_double_modules.main_module import main_function, shared_function
from tests.test_double_modules.helper_module import HelperClass

@pytest.fixture(autouse=True)
def reset_setup():
    """Reset the Setup state before each test."""
    Setup.reset()
    if tracing_active.get() is not False:
        tracing_active.set(False)
    yield
    Setup.reset()
    if tracing_active.get() is not False:
        tracing_active.set(False)

@pytest.fixture
def setup_testing_mode():
    """Setup in testing mode to capture logs."""
    Setup.initialize("TEST_DOUBLE_TRACE", show_metrics=True)
    Setup.enable_testing_mode()
    yield
    Setup.disable_testing_mode()
    Setup.reset()

def test_double_decorated_function(setup_testing_mode):
    """Test that a function decorated with trace and also traced via recursive tracing is only traced once."""
    
    # Apply trace directly to our functions to ensure they are decorated
    original_main_function = main_function
    
    @trace()
    def direct_trace_main(value):
        return original_main_function(value)
    
    # Also add a recursive tracer
    @trace(recursive_depth=1, module_pattern="tests.test_double_modules.*")
    def recursive_caller():
        return direct_trace_main(5)
    
    # Call both functions
    result = recursive_caller()
    
    # Check result
    assert result == 20  # 5 + helper_function(5) = 5 + 15 = 20
    
    # Get logs
    logs = Setup.get_captured_logs()
    
    # Print all logs for debugging
    print("\nAll logs captured:")
    for i, log in enumerate(logs):
        print(f"Log {i+1}: {log}")
    
    # Check that the right methods were called
    method_calls = {}
    for log in logs:
        fn = log.get("function", "")
        msg = log.get("message", "")
        
        # Only process log entries with function names and 'called' message
        if fn and "called" in msg:
            if fn not in method_calls:
                method_calls[fn] = 0
            method_calls[fn] += 1
    
    # Print all method calls
    print("\nMethod calls recorded:")
    for method, count in method_calls.items():
        print(f"{method}: {count} calls")
    
    # Check that the local direct_trace_main is called
    direct_trace_found = False
    for method in method_calls:
        if 'direct_trace_main' in method:
            direct_trace_found = True
            assert method_calls[method] == 1, f"Expected direct_trace_main to be called once, got {method_calls[method]}"
    
    assert direct_trace_found, "direct_trace_main was not found in the logs"
    
    # Verify that HelperClass.get_value is called, which is used by main_function
    helper_method_called = False
    for method in method_calls:
        if 'HelperClass.get_value' in method:
            helper_method_called = True
            
    assert helper_method_called, "HelperClass.get_value (used by main_function) should be called"

def test_method_directly_traced(setup_testing_mode):
    """Test that methods with direct trace decorators work correctly."""
    
    # Create an instance and call the traced method
    helper = HelperClass(10)
    result = helper.double_value()
    
    # Check result
    assert result == 20
    
    # Check logs
    logs = Setup.get_captured_logs()
    
    # Look for double_value logs
    double_value_logs = [
        log for log in logs 
        if log.get("function") and "double_value" in log.get("function")
    ]
    
    # We should see logs for double_value
    assert len(double_value_logs) >= 1

def test_method_traced_in_execution_path(setup_testing_mode):
    """Test that repeated calls to the same method in one execution path are traced correctly."""
    
    # Define a function that calls the same method twice
    @trace()
    def call_method_twice():
        helper = HelperClass(5)
        first_call = helper.get_value()  # First call to get_value
        second_call = helper.get_value() # Second call to get_value
        return first_call + second_call
    
    # Call the function
    result = call_method_twice()
    assert result == 10  # 5 + 5
    
    # Get logs
    logs = Setup.get_captured_logs()
    
    # Count how many times get_value was called
    get_value_call_logs = [
        log for log in logs 
        if log.get("function") and "get_value" in log.get("function") and "called" in log.get("message", "")
    ]
    
    # Print all get_value logs for debugging
    print(f"Number of get_value calls: {len(get_value_call_logs)}")
    for i, log in enumerate(get_value_call_logs):
        print(f"Call {i+1}: {log}")
    
    # Should see two calls to get_value since they're separate invocations
    assert len(get_value_call_logs) == 2

def test_multiple_recursive_traces(setup_testing_mode):
    """Test that a function traced by multiple recursive tracers is only traced once per call."""
    
    # Import and apply a direct trace
    @trace()
    def shared_func_wrapper(value):
        return shared_function(value)
    
    # Two functions that both use the traced function
    @trace(recursive_depth=1, module_pattern="tests.test_double_modules.*")
    def first_caller():
        return shared_func_wrapper(7)
    
    @trace(recursive_depth=1, module_pattern="tests.test_double_modules.*")
    def second_caller():
        return shared_func_wrapper(8)
    
    # Call both functions
    first_result = first_caller()
    second_result = second_caller()
    
    # Check results are correct
    assert first_result == 19  # another_helper(7) + 5 = 14 + 5 = 19
    assert second_result == 21  # another_helper(8) + 5 = 16 + 5 = 21
    
    # Get logs
    logs = Setup.get_captured_logs()
    
    # Print all logs for debugging
    print("\nAll logs captured:")
    for i, log in enumerate(logs):
        print(f"Log {i+1}: {log}")
    
    # Check that the right methods were called
    method_calls = {}
    for log in logs:
        fn = log.get("function", "")
        msg = log.get("message", "")
        
        # Only process log entries with function names and 'called' message
        if fn and "called" in msg:
            if fn not in method_calls:
                method_calls[fn] = 0
            method_calls[fn] += 1
    
    # Print all method calls
    print("\nMethod calls recorded:")
    for method, count in method_calls.items():
        print(f"{method}: {count} calls")
    
    # Find the first and second caller functions by partial name match
    first_caller_found = False
    second_caller_found = False
    for method in method_calls:
        if 'first_caller' in method:
            first_caller_found = True
            assert method_calls[method] == 1, f"Expected first_caller to be called once, got {method_calls[method]}"
        elif 'second_caller' in method:
            second_caller_found = True
            assert method_calls[method] == 1, f"Expected second_caller to be called once, got {method_calls[method]}"
    
    assert first_caller_found, "first_caller was not found in the logs"
    assert second_caller_found, "second_caller was not found in the logs"
    
    # Find the shared_func_wrapper by partial name match
    shared_wrapper_found = False
    shared_wrapper_calls = 0
    for method in method_calls:
        if 'shared_func_wrapper' in method:
            shared_wrapper_found = True
            shared_wrapper_calls = method_calls[method]
    
    # Check that shared_func_wrapper is called twice (once for each parent)
    assert shared_wrapper_found, "shared_func_wrapper was not found in the logs"
    assert shared_wrapper_calls == 2, f"Expected shared_func_wrapper to be called twice, got {shared_wrapper_calls}"
    
    # Verify shared_function was called in the logs
    assert "shared_function" in str(logs), "shared_function should have been called" 
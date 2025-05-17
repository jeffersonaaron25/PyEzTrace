import asyncio
import time
import pytest
import contextlib

from pyeztrace.setup import Setup
from pyeztrace.tracer import trace, tracing_active
from pyeztrace.custom_logging import Logging, LogContext

async def test_simple_async_task():
    """Test tracing when a traced function creates an async task of another traced function."""
    # Make sure to reset setup first
    Setup.reset()
    Setup.initialize("ASYNC_TASK_TEST", show_metrics=False)
    # Limit the log depth to avoid recursion errors
    # Setup.enable_testing_mode()
    
    # Define traced functions
    @trace()
    async def parent_function():
        print("Parent function running")
        Logging.log_info("Parent function running", function="parent_function")
        
        # Create a task of the child function
        task = asyncio.create_task(child_function("task1"))
        
        # Do some work
        await asyncio.sleep(0.05)
        print("Parent function still running after creating task")
        
        # Wait for the task to complete
        await task
        print("Parent function completed after waiting for task")
        
        return "parent done"
    
    @trace()
    async def child_function(task_id):
        # This should be traced independently
        print(f"Child function running: {task_id}")
        Logging.log_info(f"Child function running: {task_id}", function="child_function")
        await asyncio.sleep(0.1)
        print(f"Child function completed: {task_id}")
        return f"child {task_id} done"
    
    # Run the parent function
    result = await parent_function()
    assert result == "parent done"
    
    Setup.reset()

async def test_nested_async_tasks():
    """Test tracing with nested async tasks to see how context propagates."""
    # Make sure to reset setup first
    Setup.reset()
    Setup.initialize("NESTED_ASYNC_TEST", show_metrics=False)
    # Limit the log depth to avoid recursion errors
    # Setup.enable_testing_mode()
    
    # Define traced functions with 3 levels of nesting
    @trace()
    async def level1_function():
        print("Level 1 function running")
        Logging.log_info("Level 1 running", function="level1")
        
        # Create a task for level 2
        task = asyncio.create_task(level2_function("task-L2"))
        
        # Do some work
        await asyncio.sleep(0.03)
        
        # Wait for the task to complete
        await task
        print("Level 1 function completed")
        return "level1 done"
    
    @trace()
    async def level2_function(task_id):
        print(f"Level 2 function running: {task_id}")
        Logging.log_info(f"Level 2 running: {task_id}", function="level2")
        
        # Create a task for level 3
        task = asyncio.create_task(level3_function("task-L3"))
        
        # Do some work
        await asyncio.sleep(0.03)
        
        # Wait for the task to complete
        await task
        print(f"Level 2 function completed: {task_id}")
        return "level2 done"
    
    @trace()
    async def level3_function(task_id):
        print(f"Level 3 function running: {task_id}")
        Logging.log_info(f"Level 3 running: {task_id}", function="level3")
        
        # Do some work
        await asyncio.sleep(0.05)
        print(f"Level 3 function completed: {task_id}")
        return "level3 done"
    
    # Run the top-level function
    result = await level1_function()
    assert result == "level1 done"
    
    Setup.reset()

async def test_non_awaited_tasks():
    """Test tracing behavior when tasks are created but not immediately awaited."""
    # Make sure to reset setup first
    Setup.reset()
    # Initialize with show_metrics=False to see the hierarchy in logs
    Setup.initialize("FIRE_AND_FORGET", show_metrics=False)
    # Limit the log depth to avoid recursion errors
    # Do NOT enable testing mode so we can see the actual log output
    
    # Define traced functions
    @trace()
    async def parent_function():
        print("🚀 Parent function starting")
        Logging.log_info("Parent function starting", function="parent_function")
        
        # Create multiple tasks without awaiting them immediately
        task1 = asyncio.create_task(child_function("task1", 0.1))
        task2 = asyncio.create_task(child_function("task2", 0.05))
        task3 = asyncio.create_task(child_function("task3", 0.15))
        
        # Do some work in the parent without waiting
        await asyncio.sleep(0.08)
        Logging.log_info("Parent function still working", function="parent_function")
        
        # Now wait for some tasks, but not all
        await task2  # Wait for the middle task only
        
        # Do more work
        Logging.log_info("Parent continued after task2", function="parent_function")
        await asyncio.sleep(0.05)
        
        # Don't wait for the other tasks to complete before returning
        print("🏁 Parent function finished (tasks 1 and 3 may still be running)")
        return "parent done"
    
    @trace()
    async def child_function(task_id, sleep_time):
        # This should be traced independently
        print(f"👶 Child function {task_id} starting (sleep: {sleep_time}s)")
        Logging.log_info(f"Child {task_id} starting", function=f"child_{task_id}")
        
        # Simulate work
        await asyncio.sleep(sleep_time)
        
        print(f"✅ Child function {task_id} completed")
        Logging.log_info(f"Child {task_id} completed", function=f"child_{task_id}")
        return f"child {task_id} done"
    
    try:
        # Run the parent function, but make sure we wait for pending tasks
        # to avoid task was destroyed but it is pending warnings
        parent_task = asyncio.create_task(parent_function())
        
        # Wait for the parent function to complete
        await parent_task
        
        # Give some time for any remaining tasks to complete
        await asyncio.sleep(0.2)
        
        # Print a message about completion
        print("\n🔍 Test completed - check the logs above to see the trace hierarchy")
    finally:
        
        # Clean up
        Setup.reset()

async def main():
    """Run all tests."""
    try:
        print("=== Testing Simple Async Task ===")
        await test_simple_async_task()
        
        print("\n=== Testing Nested Async Tasks ===")
        await test_nested_async_tasks()
        
        print("\n=== Testing Non-Awaited Async Tasks ===")
        await test_non_awaited_tasks()
    except Exception as e:
        print(f"Error running tests: {e}")
        
if __name__ == "__main__":
    asyncio.run(main()) 
from pyeztrace.setup import Setup
Setup.initialize('EzTracer', show_metrics=True)

from pyeztrace.tracer import trace
from pyeztrace.custom_logging import Logging

logger = Logging()


@trace()
def test_example():
    with logger.with_context(key="test_example"):
        logger.log_info("This is a test function.")
        # Simulate some work
        with logger.with_context(action="test_example_work"):
            for i in range(5):
                logger.log_info(f"Working... {i}")
                # Simulate a delay
                import time
                time.sleep(0.5)
    logger.log_info("Test function completed.")
    # Simulate some more work
    for i in range(5):
        logger.log_info(f"Working... {i}")
        # Simulate a delay
        import time
        time.sleep(0.5)
    logger.log_info("Test function completed again.")
    test_example2()
    test_example3()

@trace()
def test_example2():
    logger.log_info("This is a test function 2.")
    # Simulate some work
    for i in range(5):
        logger.log_info(f"Working... {i}")
        # Simulate a delay
        import time
        time.sleep(0.5)
    logger.log_info("Test function 2 completed.")

@trace()
def test_example3():
    logger.log_info("This is a test function 3.")
    # Simulate some work
    for i in range(5):
        logger.log_info(f"Working... {i}")
        # Simulate a delay
        import time
        time.sleep(0.5)
    logger.log_info("Test function 3 completed.")
    test_example2()

if __name__ == "__main__":
    test_example()
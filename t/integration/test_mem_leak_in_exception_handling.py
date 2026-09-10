"""
Integration tests for memory leak issue #8882.

These tests reproduce memory leak scenarios that occur when Celery tasks
raise unhandled exceptions, causing ExceptionInfo objects to not be
properly garbage collected.
"""

import gc
import logging
import os
import tracemalloc

from celery import Celery

logger = logging.getLogger(__name__)


class MemoryLeakUnhandledExceptionsTest:
    """Test class for memory leak scenarios with unhandled exceptions."""

    def __init__(self):
        self.app = Celery('test_memory_leak')
        self.app.conf.update(
            broker_url='memory://',
            result_backend='cache+memory://',
            task_always_eager=True,
            task_eager_propagates=True,
            task_store_eager_result=True,
        )
        self.setup_tasks()

    def setup_tasks(self):
        """Setup test tasks."""

        @self.app.task
        def task_success():
            """Task that completes successfully - baseline for memory comparison."""
            return "success"

        @self.app.task
        def task_unhandled_exception():
            """Task that raises an unhandled RuntimeError exception."""
            raise RuntimeError("Unhandled exception for memory leak test")

        @self.app.task(bind=True, max_retries=3)
        def task_retry_then_fail(self):
            """Task that retries multiple times and eventually fails with unhandled exception."""
            if self.request.retries < self.max_retries:
                raise self.retry(countdown=0.001)
            raise RuntimeError("Final retry failure - unhandled exception")

        @self.app.task
        def task_nested_exception_stack():
            """Task that raises exception through deeply nested function calls."""
            def deep_level_5():
                local_data = {"level": 5, "data": list(range(100))}  # noqa: F841
                raise ValueError("Deep nested exception at level 5")

            def deep_level_4():
                local_data = {"level": 4, "nested": {"data": list(range(50))}}  # noqa: F841
                deep_level_5()

            def deep_level_3():
                local_data = [1, 2, 3, {"nested": True}]  # noqa: F841
                deep_level_4()

            def deep_level_2():
                deep_level_3()

            def deep_level_1():
                deep_level_2()

            deep_level_1()

        self.task_success = task_success
        self.task_unhandled_exception = task_unhandled_exception
        self.task_retry_then_fail = task_retry_then_fail
        self.task_nested_exception_stack = task_nested_exception_stack


def get_memory_usage():
    """
    Get current memory usage in bytes.

    Returns RSS (total process memory) if psutil is available,
    otherwise returns Python heap allocations via tracemalloc.
    Note: These measurements are not directly comparable.
    """
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return process.memory_info().rss
    except ImportError:
        # Fallback to tracemalloc if psutil not available
        current, peak = tracemalloc.get_traced_memory()
        return current


def test_mem_leak_unhandled_exceptions():
    """Test that reproduces the memory leak when tasks raise unhandled exceptions."""

    # Setup
    test_instance = MemoryLeakUnhandledExceptionsTest()

    # Enable memory tracing
    tracemalloc.start()

    # Warm up - run some successful tasks first
    for _ in range(50):
        try:
            test_instance.task_success.apply()
        except Exception:
            pass

    # Force garbage collection and get baseline memory
    gc.collect()
    baseline_memory = get_memory_usage()

    # Run many failing tasks - this should demonstrate the leak
    exception_count = 0
    for _ in range(500):  # Reduced from 1000 to make test faster
        try:
            test_instance.task_unhandled_exception.apply()
        except Exception:
            exception_count += 1

    # Force garbage collection
    gc.collect()
    after_exceptions_memory = get_memory_usage()

    # Run successful tasks again to ensure the leak is from exceptions
    for _ in range(50):
        try:
            test_instance.task_success.apply()
        except Exception:
            pass

    gc.collect()
    final_memory = get_memory_usage()

    # Calculate memory increase
    memory_increase = after_exceptions_memory - baseline_memory

    # Stop tracing
    tracemalloc.stop()

    # Log memory statistics for debugging
    logger.debug("--- Memory Statistics ---")  # Separator for better readability
    logger.debug(f"Baseline memory: {baseline_memory / 1024 / 1024:.2f} MB")
    logger.debug(f"After exceptions: {after_exceptions_memory / 1024 / 1024:.2f} MB")
    logger.debug(f"Final memory: {final_memory / 1024 / 1024:.2f} MB")
    logger.debug(f"Memory increase: {memory_increase / 1024 / 1024:.2f} MB")
    logger.debug(f"Exceptions processed: {exception_count}")

    # The test should demonstrate a significant memory increase
    # This threshold may need adjustment based on the system
    memory_increase_mb = memory_increase / 1024 / 1024

    # Verify the memory leak is fixed - memory increase should be minimal
    # Before fix: >70MB for 1000 tasks (~70KB/task)
    # After fix: <5MB for 500 tasks (<10KB/task)
    threshold_percent = float(os.getenv("MEMORY_LEAK_THRESHOLD_PERCENT", 10))  # Default: 10% increase
    memory_threshold_mb = baseline_memory / 1024 / 1024 * (threshold_percent / 100)
    assert memory_increase_mb < memory_threshold_mb, (
        f"Memory leak still exists! Expected <{memory_threshold_mb:.2f}MB increase "
        f"(based on {threshold_percent}% of baseline), "
        f"but got {memory_increase_mb:.2f}MB. "
        f"This indicates the memory leak fix is not working properly."
    )


def test_mem_leak_retry_failures():
    """Test memory leak with task retry and eventual failure scenarios."""

    test_instance = MemoryLeakUnhandledExceptionsTest()

    # Enable memory tracing
    tracemalloc.start()

    # Get baseline
    gc.collect()
    baseline_memory = get_memory_usage()

    # Run tasks that retry and eventually fail
    for _ in range(100):  # Fewer iterations since retries are expensive
        try:
            test_instance.task_retry_then_fail.apply()
        except Exception:
            pass

    gc.collect()
    after_retries_memory = get_memory_usage()

    # Stop tracing
    tracemalloc.stop()

    # Calculate memory increase
    memory_increase = after_retries_memory - baseline_memory
    memory_increase_mb = memory_increase / 1024 / 1024

    logger.debug("")  # New line for better readability
    logger.debug(f"Baseline memory: {baseline_memory / 1024 / 1024:.2f} MB")
    logger.debug(f"After retries: {after_retries_memory / 1024 / 1024:.2f} MB")
    logger.debug(f"Memory increase: {memory_increase_mb:.2f} MB")

    # Retries should not show significant memory increase if fix is working
    assert memory_increase_mb < 3, (
        f"Memory leak in retry scenarios! Expected <3MB increase for 100 retry tasks, "
        f"but got {memory_increase_mb:.2f}MB"
    )


def test_mem_leak_nested_exception_stacks():
    """Test memory leak with deeply nested exception stacks and local variables."""

    test_instance = MemoryLeakUnhandledExceptionsTest()

    # Enable memory tracing
    tracemalloc.start()

    # Get baseline
    gc.collect()
    baseline_memory = get_memory_usage()

    # Run tasks with complex exception stacks
    for _ in range(200):
        try:
            test_instance.task_nested_exception_stack.apply()
        except Exception:
            pass

    gc.collect()
    after_complex_memory = get_memory_usage()

    # Stop tracing
    tracemalloc.stop()

    # Calculate memory increase
    memory_increase = after_complex_memory - baseline_memory
    memory_increase_mb = memory_increase / 1024 / 1024

    logger.debug("Memory usage results:")
    logger.debug(f"Baseline memory: {baseline_memory / 1024 / 1024:.2f} MB")
    logger.debug(f"After complex exceptions: {after_complex_memory / 1024 / 1024:.2f} MB")
    logger.debug(f"Memory increase: {memory_increase_mb:.2f} MB")

    # Complex exceptions should not show significant memory increase if fix is working
    assert memory_increase_mb < 4, (
        f"Memory leak in nested exception scenarios! Expected <4MB increase for 200 nested tasks, "
        f"but got {memory_increase_mb:.2f}MB"
    )


if __name__ == "__main__":
    # Allow running these tests standalone for debugging
    print("Running memory leak integration tests...")
    test_mem_leak_unhandled_exceptions()
    test_mem_leak_retry_failures()
    test_mem_leak_nested_exception_stacks()
    print("Memory leak integration tests completed")

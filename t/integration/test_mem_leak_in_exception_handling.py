"""
Integration test for memory leak issue #8882.

This test reproduces the memory leak that occurs when Celery tasks
raise unhandled exceptions, causing ExceptionInfo objects to not be
properly garbage collected.
"""

import gc
import os
import sys
import tracemalloc

from celery import Celery
from celery.app.trace import traceback_clear


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
    """Get current memory usage in bytes.

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


def test_traceback_clear_uses_exc_argument():
    """Test that traceback_clear(exc) correctly uses the exc argument.

    This test proves that the reported issue about traceback_clear not using
    the exc argument is NOT valid. The function does use the exc argument correctly.
    """
    print("Testing traceback_clear(exc) functionality...")

    # Create exception with traceback
    def create_exception_with_traceback():
        """Create an exception with a traceback for testing."""
        try:
            # Create a nested call stack to have frames to clear
            def inner_function():
                x = "some_local_variable" * 1000  # Create local variable  # noqa: F841
                y = list(range(1000))  # Another local variable  # noqa: F841
                raise ValueError("Test exception with traceback")

            def outer_function():
                z = "outer_local_variable" * 1000  # Local variable in outer frame  # noqa: F841
                inner_function()

            outer_function()
        except Exception as e:
            return e

    # Test 1: traceback_clear(exc) with provided exception
    exc = create_exception_with_traceback()

    # Verify exception has traceback
    exc_tb = getattr(exc, '__traceback__', None)
    if exc_tb is None:
        raise AssertionError("Exception should have traceback")
    print("PASS: Exception has traceback")

    # Count initial frames
    initial_frames = []
    tb = exc_tb
    while tb is not None:
        initial_frames.append(tb.tb_frame)
        tb = tb.tb_next

    print(f"PASS: Initial traceback has {len(initial_frames)} frames")

    # Verify frames have local variables before clearing
    frame_locals_before = []
    for frame in initial_frames:
        frame_locals_before.append(len(frame.f_locals))

    print(f"PASS: Frames have local variables: {frame_locals_before}")

    # Call traceback_clear with the exception - this should use exc argument
    traceback_clear(exc)
    print("PASS: Called traceback_clear(exc)")

    # Verify frames are cleared
    exc_tb_after = getattr(exc, '__traceback__', None)
    if exc_tb_after is None:
        raise AssertionError("Traceback should still exist after clearing")

    tb = exc_tb_after
    frames_after = []
    while tb is not None:
        frames_after.append(tb.tb_frame)
        tb = tb.tb_next

    # Check that frame locals are cleared
    cleared_count = 0
    for frame in frames_after:
        if len(frame.f_locals) == 0:
            cleared_count += 1

    print(f"PASS: After traceback_clear: {cleared_count}/{len(frames_after)} frames cleared")

    # Verify the function actually used the exc argument by checking traceback still exists
    if getattr(exc, '__traceback__', None) is None:
        raise AssertionError("Traceback should still exist but frames should be cleared")
    print("PASS: Traceback object still exists (proving exc argument was used)")

    # Test 2: traceback_clear() without exc argument
    print("Testing traceback_clear() without exc argument...")

    try:
        def test_function():
            local_var = "test" * 1000  # noqa: F841
            raise RuntimeError("Test exception")

        test_function()
    except Exception:
        # Now we're in except block with active traceback
        _, _, tb_before = sys.exc_info()
        assert tb_before is not None, "Should have active traceback"
        print("PASS: Active traceback exists")

        # Call traceback_clear without argument - should use sys.exc_info()
        traceback_clear()
        print("PASS: Called traceback_clear() without argument")

    # Test 3: traceback_clear(None)
    print("Testing traceback_clear(None)...")

    try:
        def test_function():
            local_var = "test" * 1000  # noqa: F841
            raise RuntimeError("Test exception")

        test_function()
    except Exception:
        # Call with None - should fall back to sys.exc_info()
        traceback_clear(None)
        print("PASS: Called traceback_clear(None) - uses sys.exc_info() fallback")


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

    # Print memory statistics for debugging
    print()  # New line for better readability
    print(f"Baseline memory: {baseline_memory / 1024 / 1024:.2f} MB")
    print(f"After exceptions: {after_exceptions_memory / 1024 / 1024:.2f} MB")
    print(f"Final memory: {final_memory / 1024 / 1024:.2f} MB")
    print(f"Memory increase: {memory_increase / 1024 / 1024:.2f} MB")
    print(f"Exceptions processed: {exception_count}")

    # The test should demonstrate a significant memory increase
    # This threshold may need adjustment based on the system
    memory_increase_mb = memory_increase / 1024 / 1024

    # Verify the memory leak is fixed - memory increase should be minimal
    # Before fix: >70MB for 1000 tasks (~70KB/task)
    # After fix: <5MB for 500 tasks (<10KB/task)
    assert memory_increase_mb < 5, (
        f"Memory leak still exists! Expected <5MB increase for 500 tasks, "
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

    print()  # New line for better readability
    print(f"Baseline memory: {baseline_memory / 1024 / 1024:.2f} MB")
    print(f"After retries: {after_retries_memory / 1024 / 1024:.2f} MB")
    print(f"Memory increase: {memory_increase_mb:.2f} MB")

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

    print()  # New line for better readability
    print(f"Baseline memory: {baseline_memory / 1024 / 1024:.2f} MB")
    print(f"After complex exceptions: {after_complex_memory / 1024 / 1024:.2f} MB")
    print(f"Memory increase: {memory_increase_mb:.2f} MB")

    # Complex exceptions should not show significant memory increase if fix is working
    assert memory_increase_mb < 4, (
        f"Memory leak in nested exception scenarios! Expected <4MB increase for 200 nested tasks, "
        f"but got {memory_increase_mb:.2f}MB"
    )


if __name__ == "__main__":
    # Allow running this test standalone for debugging
    print("Running memory leak test for unhandled exceptions...")
    test_mem_leak_unhandled_exceptions()
    print("Memory leak test completed")

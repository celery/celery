"""
Simple test to reproduce JSON serialization memory leak (issue #9475).

In the Celery repository, create this file in the `t/integration` directory.

`t/integration/test_mem_leak_json_serialization.py`

This test creates nested chain/group structures and measures memory usage
during JSON serialization vs pickle serialization.
"""

import gc
import os
import time
import tracemalloc

from celery import Celery, chain, group

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


def get_memory_usage():
    """
    Get current memory usage in bytes.

    Returns RSS (total process memory) if psutil is available,
    otherwise returns Python heap allocations via tracemalloc.
    """
    if HAS_PSUTIL:
        try:
            process = psutil.Process(os.getpid())
            return process.memory_info().rss
        except Exception:
            pass

    # Fallback to tracemalloc if psutil not available or fails
    try:
        current, peak = tracemalloc.get_traced_memory()
        return current
    except RuntimeError:
        # tracemalloc not started
        return 0


# Test configuration from environment variables
DEFAULT_TIMEOUT = int(os.getenv('TEST_TIMEOUT_SECONDS', '240'))
DEFAULT_MEMORY_THRESHOLD = int(os.getenv('TEST_MEMORY_THRESHOLD_MB', '1000'))


def get_test_config():
    """Get test configuration from environment variables."""
    return {
        'timeout_seconds': int(os.getenv('TEST_TIMEOUT_SECONDS', str(DEFAULT_TIMEOUT))),
        'memory_threshold_mb': int(os.getenv('TEST_MEMORY_THRESHOLD_MB', str(DEFAULT_MEMORY_THRESHOLD))),
        'levels': int(os.getenv('LEVELS', '5')),
        'nodes_per_level': int(os.getenv('NODES_PER_LEVEL', '5')),
        'json_memory_threshold_mb': float(os.getenv('JSON_MEMORY_THRESHOLD_MB', '100')),
    }


def test_json_serialization_memory_leak():
    """
    Test that JSON serialization doesn't cause excessive memory usage.

    This test reproduces issue #9475 by creating nested chain/group structures
    and comparing memory usage between JSON and pickle serialization.
    """
    config = get_test_config()

    # Memory threshold in MB - if JSON uses more than this compared to pickle,
    # we consider it a memory leak
    MEMORY_THRESHOLD_MB = config['json_memory_threshold_mb']

    # Test parameters - can be overridden via environment
    # levels size impact more on memory than nodes_per_level
    levels = config['levels']
    nodes_per_level = config['nodes_per_level']

    print(f"Testing with {levels} levels, {nodes_per_level} nodes per level")
    print(f"Memory threshold: {MEMORY_THRESHOLD_MB} MB")

    # Test with both serializers
    results = {}

    for serializer in ['pickle', 'json']:
        start = time.time()
        print(f"\nTesting {serializer} serializer...")

        # Create Celery app with specific serializer
        app = Celery(f'test_{serializer}')
        app.conf.update(
            broker_url='memory://',
            result_backend='cache+memory://',
            task_always_eager=False,
            task_serializer=serializer,
            result_serializer=serializer,
            accept_content=[serializer],
        )

        @app.task(bind=True)
        def job1(self, job_callee, *args, **kwargs):
            """Task from the original issue #9475."""
            print("------------------\nI AM: ", self.request.id)
            job_callee_original = job_callee

            # Count the depth of nested lists
            depth = 0
            is_a_list = isinstance(job_callee_original, list)
            job_callee_original_cp = job_callee_original

            if (
                is_a_list and
                len(job_callee_original) > 0 and
                not isinstance(job_callee_original[0], list)
            ):
                print("MOST INNER LIST: ", job_callee_original)

            while is_a_list and len(job_callee_original_cp) > 0:
                depth += 1
                job_callee_inner = job_callee_original_cp[0] if is_a_list else job_callee_original_cp
                is_a_list = isinstance(job_callee_inner, list)
                job_callee_original_cp = job_callee_inner

            print(f"Nested list depth: {depth}")
            return job_callee_original

        # Start memory tracking
        tracemalloc.start()

        # Get baseline memory
        gc.collect()
        baseline_memory = get_memory_usage()

        # Create the problematic structure from issue #9475
        per_level_groups = []
        for idx in range(levels):
            task_name = f"Node_{idx + 1}"
            group_tasks = [job1.s(idx).set(task_id=f"{task_name}_node_{jdx + 1}") for jdx in range(nodes_per_level)]
            per_level_groups.append(group(group_tasks))

        # Memory after structure creation
        structure_memory = get_memory_usage()

        # This triggers the memory leak with JSON serialization
        chain_sig = chain(per_level_groups)
        chain_sig.apply_async()

        # Measure final memory
        final_memory = get_memory_usage()

        # Stop tracking
        tracemalloc.stop()

        # Calculate memory usage
        structure_increase = (structure_memory - baseline_memory) / 1024 / 1024
        serialization_increase = (final_memory - structure_memory) / 1024 / 1024
        total_increase = (final_memory - baseline_memory) / 1024 / 1024

        results[serializer] = {
            'structure_mb': structure_increase,
            'serialization_mb': serialization_increase,
            'total_mb': total_increase,
            'total_time': time.time() - start,
        }

        print(f"  Structure creation: {structure_increase:.2f} MB")
        print(f"  Serialization: {serialization_increase:.2f} MB")
        print(f"  Total increase: {total_increase:.2f} MB")

    # Compare results
    pickle_total = results['pickle']['total_mb']
    pickle_time = results['pickle']['total_time']
    json_time = results['json']['total_time']
    json_total = results['json']['total_mb']

    print("\nComparison:")
    print(f"  Pickle total: {pickle_total:.2f} MB")
    print(f"  JSON total: {json_total:.2f} MB")

    if pickle_total > 0:
        ratio = json_total / pickle_total
        print(f"  JSON uses {ratio:.1f}x more memory than pickle")

    memory_difference = json_total - pickle_total
    print(f"  Memory difference: {memory_difference:.2f} MB")

    print(f"  Pickle time: {pickle_time:.2f} seconds")
    print(f"  JSON time: {json_time:.2f} seconds")

    # Check for memory leak
    if memory_difference > MEMORY_THRESHOLD_MB:
        raise AssertionError(
            f"JSON serialization memory leak detected! "
            f"JSON used {memory_difference:.2f} MB more than pickle "
            f"(threshold: {MEMORY_THRESHOLD_MB} MB). "
            f"This indicates the memory leak from issue #9475."
        )

    print(f"\n✓ Test passed - memory difference ({memory_difference:.2f} MB) is below threshold")


if __name__ == "__main__":
    print("Running JSON serialization memory leak tests...")
    test_json_serialization_memory_leak()
    print("All tests completed successfully!")

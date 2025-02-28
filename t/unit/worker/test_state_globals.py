"""Unit tests for Celery worker state global variables.

The worker state module (celery.worker.state) contains several global variables
that are critical for tracking task state and worker operations:

- reserved_requests: WeakSet of tasks that have been reserved but not yet executed
- active_requests: WeakSet of tasks currently being executed
- successful_requests: LimitedSet of task IDs that completed successfully
- revoked: LimitedSet of task IDs that have been revoked

These globals are fundamental to Celery's operation, affecting:
- Task state management
- Worker memory management (through WeakSets)
- Task result tracking
- Task revocation handling

These tests ensure that:
1. The globals maintain correct state throughout task lifecycle
2. Memory is properly managed (no leaks in WeakSets)
3. Set size limits are respected (LimitedSets)
4. Task state transitions work correctly
"""

import gc
import weakref
from time import sleep

import pytest

from celery.utils.collections import LimitedSet
from celery.worker import state
from celery.worker.request import Request


class MockRequest:
    """Mock Request for testing.
    
    Simulates the minimal interface needed from celery.worker.request.Request
    for testing worker state management.

    Attributes:
        id (str): Task ID used for tracking and set membership
        name (str): Task name used by the worker for statistics
    """
    def __init__(self, id):
        self.id = id
        self.name = f"task-{id}"


@pytest.fixture(autouse=True)
def _reset_state():
    """Reset worker state before and after each test.
    
    This fixture ensures each test starts with a clean state and
    cleans up after itself, preventing test interference.
    
    The cleanup includes:
    - Resetting all global sets
    - Running garbage collection to clean up weak references
    """
    state.reset_state()
    yield
    state.reset_state()
    gc.collect()


def test_weakset_behavior():
    """Test proper memory management of WeakSet globals.
    
    The reserved_requests and active_requests sets are WeakSets,
    meaning they should automatically remove their entries when
    the referenced objects are garbage collected.
    
    This test verifies that:
    1. Items can be added to the WeakSet
    2. Items are accessible while referenced
    3. Items are automatically removed when dereferenced
    """
    # Create a reference and add to set
    ref = MockRequest("test-1")
    state.reserved_requests.add(ref)
    assert len(state.reserved_requests) == 1

    # Verify item exists
    items = list(state.reserved_requests)
    assert len(items) == 1
    assert items[0].id == "test-1"

    # Remove reference and verify cleanup
    items = None  # Remove reference from items list
    ref = None  # Remove direct reference
    gc.collect()  # Force garbage collection
    assert len(state.reserved_requests) == 0


def test_limitedset_behavior():
    """Test LimitedSet globals behavior and constraints.
    
    The successful_requests and revoked sets are LimitedSets,
    which have a maximum size and can expire entries.
    
    This test verifies that:
    1. Sets have correct type and configuration
    2. Basic add/contains operations work
    3. Size limits are enforced
    """
    # Test successful_requests
    assert isinstance(state.successful_requests, LimitedSet)
    assert state.successful_requests.maxlen == state.SUCCESSFUL_MAX
    assert state.successful_requests.expires == state.SUCCESSFUL_EXPIRES

    # Test basic add/contains
    state.successful_requests.add("test-1")
    assert "test-1" in state.successful_requests

    # Test maxlen
    for i in range(state.SUCCESSFUL_MAX + 10):
        state.successful_requests.add(f"test-{i}")
    assert len(state.successful_requests) <= state.SUCCESSFUL_MAX


def test_successful_requests_specific():
    """Test specific behavior of successful_requests set.
    
    The successful_requests set tracks task IDs that have completed
    successfully. This is used for duplicate task filtering and
    result backend optimization.
    
    This test verifies that:
    1. Task IDs are properly added
    2. Membership testing works correctly
    3. Non-existent tasks are properly reported as not present
    """
    # Test that it properly tracks successful task IDs
    state.successful_requests.add("task-1")
    state.successful_requests.add("task-2")
    
    assert "task-1" in state.successful_requests
    assert "task-2" in state.successful_requests
    assert "task-3" not in state.successful_requests


def test_revoked_specific():
    """Test specific behavior of revoked set.
    
    The revoked set tracks task IDs that have been explicitly
    revoked. This is critical for task cancellation and
    duplicate task prevention.
    
    This test verifies that:
    1. Task IDs are properly marked as revoked
    2. Revocation status is correctly reported
    3. Non-revoked tasks are properly reported as not revoked
    """
    # Test revocation tracking
    state.revoked.add("task-1")
    state.revoked.add("task-2")
    
    assert "task-1" in state.revoked
    assert "task-2" in state.revoked
    assert "task-3" not in state.revoked


def test_request_state_lifecycle():
    """Test complete task request lifecycle through state transitions.
    
    Tasks move through several states during their lifecycle:
    1. Reserved (pending execution)
    2. Active (currently executing)
    3. Complete (successful or failed)
    
    This test verifies that:
    1. Tasks are properly tracked in each state
    2. State transitions are handled correctly
    3. Completed tasks are removed from tracking sets
    4. Successful tasks are recorded in successful_requests
    """
    request = MockRequest("lifecycle-1")
    
    # Test reservation
    state.task_reserved(request)
    assert request in state.reserved_requests
    assert request.id in state.requests
    
    # Test acceptance
    state.task_accepted(request)
    assert request in state.active_requests
    assert request.id in state.requests
    
    # Test completion (success)
    state.task_ready(request, successful=True)
    assert request.id in state.successful_requests
    assert request not in state.active_requests
    assert request not in state.reserved_requests
    assert request.id not in state.requests

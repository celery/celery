import gc
import weakref
from time import sleep

import pytest

from celery.utils.collections import LimitedSet
from celery.worker import state
from celery.worker.request import Request


class MockRequest:
    """Mock Request for testing."""
    def __init__(self, id):
        self.id = id


def test_weakset_behavior():
    """Test WeakSet globals behavior."""
    # Reset state before test
    state.reset_state()

    # Create a reference and add to set
    ref = MockRequest("test-1")
    state.reserved_requests.add(ref)
    assert len(state.reserved_requests) == 1

    # Verify item exists
    items = list(state.reserved_requests)
    assert len(items) == 1
    assert items[0].id == "test-1"

    # Remove reference and verify cleanup
    del ref
    gc.collect()  # Force garbage collection
    assert len(state.reserved_requests) == 0

    # Cleanup after test
    state.reset_state()
    gc.collect()


def test_limitedset_behavior():
    """Test LimitedSet globals behavior."""
    # Reset state before test
    state.reset_state()

    # Test successful_requests
    assert isinstance(state.successful_requests, LimitedSet)
    assert state.successful_requests.maxlen == state.SUCCESSFUL_MAX
    assert state.successful_requests.expires == state.SUCCESSFUL_EXPIRES

    # Test basic add/contains
    state.successful_requests.add("test-1")
    assert "test-1" in state.successful_requests

    # Test expiration
    state.successful_requests.add("test-expire", expires=0.1)
    assert "test-expire" in state.successful_requests
    sleep(0.2)
    assert "test-expire" not in state.successful_requests

    # Test maxlen
    for i in range(state.SUCCESSFUL_MAX + 10):
        state.successful_requests.add(f"test-{i}")
    assert len(state.successful_requests) <= state.SUCCESSFUL_MAX

    # Cleanup after test
    state.reset_state()


def test_successful_requests_specific():
    """Test specific behavior of successful_requests."""
    # Reset state before test
    state.reset_state()

    # Test that it properly tracks successful task IDs
    state.successful_requests.add("task-1")
    state.successful_requests.add("task-2")
    
    assert "task-1" in state.successful_requests
    assert "task-2" in state.successful_requests
    assert "task-3" not in state.successful_requests

    # Cleanup after test
    state.reset_state()


def test_revoked_specific():
    """Test specific behavior of revoked."""
    # Reset state before test
    state.reset_state()

    # Test revocation tracking
    state.revoked.add("task-1")
    state.revoked.add("task-2")
    
    assert "task-1" in state.revoked
    assert "task-2" in state.revoked
    assert "task-3" not in state.revoked

    # Cleanup after test
    state.reset_state()


def test_request_state_lifecycle():
    """Test the complete lifecycle of a request through different states."""
    # Reset state before test
    state.reset_state()

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

    # Cleanup after test
    state.reset_state()
    gc.collect()

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


class TestStateGlobals:

    @pytest.fixture(autouse=True)
    def setup_state(self):
        # Reset state before each test
        state.reset_state()
        yield
        # Cleanup after each test
        state.reset_state()
        gc.collect()  # Help with cleanup of weak references

    @pytest.mark.parametrize("global_set,factory", [
        (state.reserved_requests, MockRequest),
        (state.active_requests, MockRequest),
    ])
    def test_weakset_behavior(self, global_set, factory):
        """Test WeakSet globals behavior."""
        # Create a reference and add to set
        ref = factory("test-1")
        global_set.add(ref)
        assert len(global_set) == 1

        # Verify item exists
        items = list(global_set)
        assert len(items) == 1
        assert items[0].id == "test-1"

        # Remove reference and verify cleanup
        del ref
        gc.collect()  # Force garbage collection
        assert len(global_set) == 0

    @pytest.mark.parametrize("global_set,maxlen,expires", [
        (state.successful_requests, state.SUCCESSFUL_MAX, state.SUCCESSFUL_EXPIRES),
        (state.revoked, state.REVOKES_MAX, state.REVOKE_EXPIRES),
    ])
    def test_limitedset_behavior(self, global_set, maxlen, expires):
        """Test LimitedSet globals behavior."""
        assert isinstance(global_set, LimitedSet)
        assert global_set.maxlen == maxlen
        assert global_set.expires == expires

        # Test basic add/contains
        global_set.add("test-1")
        assert "test-1" in global_set

        # Test expiration
        if expires:
            global_set.add("test-expire", expires=0.1)
            assert "test-expire" in global_set
            sleep(0.2)
            assert "test-expire" not in global_set

        # Test maxlen
        for i in range(maxlen + 10):
            global_set.add(f"test-{i}")
        assert len(global_set) <= maxlen

    def test_successful_requests_specific(self):
        """Test specific behavior of successful_requests."""
        # Test that it properly tracks successful task IDs
        state.successful_requests.add("task-1")
        state.successful_requests.add("task-2")
        
        assert "task-1" in state.successful_requests
        assert "task-2" in state.successful_requests
        assert "task-3" not in state.successful_requests

    def test_revoked_specific(self):
        """Test specific behavior of revoked."""
        # Test revocation tracking
        state.revoked.add("task-1")
        state.revoked.add("task-2")
        
        assert "task-1" in state.revoked
        assert "task-2" in state.revoked
        assert "task-3" not in state.revoked

    def test_request_state_lifecycle(self):
        """Test the complete lifecycle of a request through different states."""
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

"""Unit tests for Celery worker state module.

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
import os
import pickle
import sys
from importlib import import_module
from time import time
from unittest.mock import Mock, patch

import pytest

from celery import uuid
from celery.exceptions import WorkerShutdown, WorkerTerminate
from celery.platforms import EX_OK
from celery.utils.collections import LimitedSet
from celery.worker import state


class SimpleReq:
    """Request class for testing.
    
    Simulates the minimal interface needed from celery.worker.request.Request
    for testing worker state management.

    Attributes:
        id (str): Task ID used for tracking and set membership
        name (str): Task name used by the worker for statistics
    """
    def __init__(self, name):
        self.id = uuid()
        self.name = name


@pytest.fixture
def reset_state():
    yield
    state.active_requests.clear()
    state.revoked.clear()
    state.revoked_stamps.clear()
    state.total_count.clear()
    state.reset_state()
    gc.collect()


class MockShelve(dict):
    filename = None
    in_sync = False
    closed = False

    def open(self, filename, **kwargs):
        self.filename = filename
        return self

    def sync(self):
        self.in_sync = True

    def close(self):
        self.closed = True


class MyPersistent(state.Persistent):
    storage = MockShelve()


class test_maybe_shutdown:

    def teardown_method(self):
        state.should_stop = None
        state.should_terminate = None

    def test_should_stop(self):
        state.should_stop = True
        with pytest.raises(WorkerShutdown):
            state.maybe_shutdown()
        state.should_stop = 0
        with pytest.raises(WorkerShutdown):
            state.maybe_shutdown()
        state.should_stop = False
        try:
            state.maybe_shutdown()
        except SystemExit:
            raise RuntimeError('should not have exited')
        state.should_stop = None
        try:
            state.maybe_shutdown()
        except SystemExit:
            raise RuntimeError('should not have exited')

        state.should_stop = 0
        try:
            state.maybe_shutdown()
        except SystemExit as exc:
            assert exc.code == 0
        else:
            raise RuntimeError('should have exited')

        state.should_stop = 303
        try:
            state.maybe_shutdown()
        except SystemExit as exc:
            assert exc.code == 303
        else:
            raise RuntimeError('should have exited')

    @pytest.mark.parametrize('should_stop', (None, False, True, EX_OK))
    def test_should_terminate(self, should_stop):
        state.should_stop = should_stop
        state.should_terminate = True
        with pytest.raises(WorkerTerminate):
            state.maybe_shutdown()


@pytest.mark.usefixtures('reset_state')
class test_Persistent:

    @pytest.fixture
    def p(self):
        return MyPersistent(state, filename='celery-state')

    def test_close_twice(self, p):
        p._is_open = False
        p.close()

    def test_constructor(self, p):
        assert p.db == {}
        assert p.db.filename == p.filename

    def test_save(self, p):
        p.db['foo'] = 'bar'
        p.save()
        assert p.db.in_sync
        assert p.db.closed

    def add_revoked(self, p, *ids):
        for id in ids:
            p.db.setdefault('revoked', LimitedSet()).add(id)

    def test_merge(self, p, data=['foo', 'bar', 'baz']):
        state.revoked.update(data)
        p.merge()
        for item in data:
            assert item in state.revoked

    def test_merge_dict(self, p):
        p.clock = Mock()
        p.clock.adjust.return_value = 626
        d = {'revoked': {'abc': time()}, 'clock': 313}
        p._merge_with(d)
        p.clock.adjust.assert_called_with(313)
        assert d['clock'] == 626
        assert 'abc' in state.revoked

    def test_sync_clock_and_purge(self, p):
        passthrough = Mock()
        passthrough.side_effect = lambda x: x
        with patch('celery.worker.state.revoked') as revoked:
            d = {'clock': 0}
            p.clock = Mock()
            p.clock.forward.return_value = 627
            p._dumps = passthrough
            p.compress = passthrough
            p._sync_with(d)
            revoked.purge.assert_called_with()
            assert d['clock'] == 627
            assert 'revoked' not in d
            assert d['zrevoked'] is revoked

    def test_sync(self, p,
                  data1=['foo', 'bar', 'baz'], data2=['baz', 'ini', 'koz']):
        self.add_revoked(p, *data1)
        for item in data2:
            state.revoked.add(item)
        p.sync()

        assert p.db['zrevoked']
        pickled = p.decompress(p.db['zrevoked'])
        assert pickled
        saved = pickle.loads(pickled)
        for item in data2:
            assert item in saved


@pytest.mark.usefixtures('reset_state')
class test_state:

    def test_accepted(self, requests=[SimpleReq('foo'),
                                      SimpleReq('bar'),
                                      SimpleReq('baz'),
                                      SimpleReq('baz')]):
        for request in requests:
            state.task_accepted(request)
        for req in requests:
            assert req in state.active_requests
        assert state.total_count['foo'] == 1
        assert state.total_count['bar'] == 1
        assert state.total_count['baz'] == 2

    def test_ready(self, requests=[SimpleReq('foo'),
                                   SimpleReq('bar')]):
        for request in requests:
            state.task_accepted(request)
        assert len(state.active_requests) == 2
        for request in requests:
            state.task_ready(request)
        assert len(state.active_requests) == 0

    def test_weakset_behavior(self):
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
        ref = SimpleReq("test-1")
        state.reserved_requests.add(ref)
        assert len(state.reserved_requests) == 1

        # Verify item exists
        items = list(state.reserved_requests)
        assert len(items) == 1
        assert items[0].name == "test-1"

        # Remove reference and verify cleanup
        items = None  # Remove reference from items list
        ref = None  # Remove direct reference
        gc.collect()  # Force garbage collection
        assert len(state.reserved_requests) == 0

    def test_limitedset_behavior(self):
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

    def test_successful_requests_specific(self):
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

    def test_revoked_specific(self):
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

    def test_request_state_lifecycle(self):
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
        request = SimpleReq("lifecycle-1")
        
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


class test_state_configuration():

    @staticmethod
    def import_state():
        with patch.dict(sys.modules):
            del sys.modules['celery.worker.state']
            return import_module('celery.worker.state')

    @patch.dict(os.environ, {
        'CELERY_WORKER_REVOKES_MAX': '50001',
        'CELERY_WORKER_SUCCESSFUL_MAX': '1001',
        'CELERY_WORKER_REVOKE_EXPIRES': '10801',
        'CELERY_WORKER_SUCCESSFUL_EXPIRES': '10801',
    })
    def test_custom_configuration(self):
        state = self.import_state()
        assert state.REVOKES_MAX == 50001
        assert state.SUCCESSFUL_MAX == 1001
        assert state.REVOKE_EXPIRES == 10801
        assert state.SUCCESSFUL_EXPIRES == 10801

    def test_default_configuration(self):
        state = self.import_state()
        assert state.REVOKES_MAX == 50000
        assert state.SUCCESSFUL_MAX == 1000
        assert state.REVOKE_EXPIRES == 10800
        assert state.SUCCESSFUL_EXPIRES == 10800

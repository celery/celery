import pickle
from time import time

import pytest
from case import Mock, patch

from celery import uuid
from celery.exceptions import WorkerShutdown, WorkerTerminate
from celery.platforms import EX_OK
from celery.utils.collections import LimitedSet
from celery.worker import state


@pytest.fixture
def reset_state():
    yield
    state.active_requests.clear()
    state.revoked.clear()
    state.total_count.clear()


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

    def teardown(self):
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


class SimpleReq:

    def __init__(self, name):
        self.id = uuid()
        self.name = name


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

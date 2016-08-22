from __future__ import absolute_import, unicode_literals

import pytest

from kombu.pidbox import Mailbox
from vine.utils import wraps

from celery import uuid
from celery.app import control
from celery.exceptions import DuplicateNodenameWarning


class MockMailbox(Mailbox):
    sent = []

    def _publish(self, command, *args, **kwargs):
        self.__class__.sent.append(command)

    def close(self):
        pass

    def _collect(self, *args, **kwargs):
        pass


class Control(control.Control):
    Mailbox = MockMailbox


def with_mock_broadcast(fun):

    @wraps(fun)
    def _resets(*args, **kwargs):
        MockMailbox.sent = []
        try:
            return fun(*args, **kwargs)
        finally:
            MockMailbox.sent = []
    return _resets


class test_flatten_reply:

    def test_flatten_reply(self):
        reply = [
            {'foo@example.com': {'hello': 10}},
            {'foo@example.com': {'hello': 20}},
            {'bar@example.com': {'hello': 30}}
        ]
        with pytest.warns(DuplicateNodenameWarning) as w:
            nodes = control.flatten_reply(reply)

        assert 'Received multiple replies from node name: {0}.'.format(
            next(iter(reply[0]))) in str(w[0].message.args[0])
        assert 'foo@example.com' in nodes
        assert 'bar@example.com' in nodes


class test_inspect:

    def setup(self):
        self.c = Control(app=self.app)
        self.prev, self.app.control = self.app.control, self.c
        self.i = self.c.inspect()

    def test_prepare_reply(self):
        reply = self.i._prepare([
            {'w1': {'ok': 1}},
            {'w2': {'ok': 1}},
        ])
        assert reply == {
            'w1': {'ok': 1},
            'w2': {'ok': 1},
        }

        i = self.c.inspect(destination='w1')
        assert i._prepare([{'w1': {'ok': 1}}]) == {'ok': 1}

    @with_mock_broadcast
    def test_active(self):
        self.i.active()
        assert 'active' in MockMailbox.sent

    @with_mock_broadcast
    def test_clock(self):
        self.i.clock()
        assert 'clock' in MockMailbox.sent

    @with_mock_broadcast
    def test_conf(self):
        self.i.conf()
        assert 'conf' in MockMailbox.sent

    @with_mock_broadcast
    def test_hello(self):
        self.i.hello('george@vandelay.com')
        assert 'hello' in MockMailbox.sent

    @with_mock_broadcast
    def test_memsample(self):
        self.i.memsample()
        assert 'memsample' in MockMailbox.sent

    @with_mock_broadcast
    def test_memdump(self):
        self.i.memdump()
        assert 'memdump' in MockMailbox.sent

    @with_mock_broadcast
    def test_objgraph(self):
        self.i.objgraph()
        assert 'objgraph' in MockMailbox.sent

    @with_mock_broadcast
    def test_scheduled(self):
        self.i.scheduled()
        assert 'scheduled' in MockMailbox.sent

    @with_mock_broadcast
    def test_reserved(self):
        self.i.reserved()
        assert 'reserved' in MockMailbox.sent

    @with_mock_broadcast
    def test_stats(self):
        self.i.stats()
        assert 'stats' in MockMailbox.sent

    @with_mock_broadcast
    def test_revoked(self):
        self.i.revoked()
        assert 'revoked' in MockMailbox.sent

    @with_mock_broadcast
    def test_tasks(self):
        self.i.registered()
        assert 'registered' in MockMailbox.sent

    @with_mock_broadcast
    def test_ping(self):
        self.i.ping()
        assert 'ping' in MockMailbox.sent

    @with_mock_broadcast
    def test_active_queues(self):
        self.i.active_queues()
        assert 'active_queues' in MockMailbox.sent

    @with_mock_broadcast
    def test_report(self):
        self.i.report()
        assert 'report' in MockMailbox.sent


class test_Broadcast:

    def setup(self):
        self.control = Control(app=self.app)
        self.app.control = self.control

        @self.app.task(shared=False)
        def mytask():
            pass
        self.mytask = mytask

    def test_purge(self):
        self.control.purge()

    @with_mock_broadcast
    def test_broadcast(self):
        self.control.broadcast('foobarbaz', arguments=[])
        assert 'foobarbaz' in MockMailbox.sent

    @with_mock_broadcast
    def test_broadcast_limit(self):
        self.control.broadcast(
            'foobarbaz1', arguments=[], limit=None, destination=[1, 2, 3],
        )
        assert 'foobarbaz1' in MockMailbox.sent

    @with_mock_broadcast
    def test_broadcast_validate(self):
        with pytest.raises(ValueError):
            self.control.broadcast('foobarbaz2',
                                   destination='foo')

    @with_mock_broadcast
    def test_rate_limit(self):
        self.control.rate_limit(self.mytask.name, '100/m')
        assert 'rate_limit' in MockMailbox.sent

    @with_mock_broadcast
    def test_time_limit(self):
        self.control.time_limit(self.mytask.name, soft=10, hard=20)
        assert 'time_limit' in MockMailbox.sent

    @with_mock_broadcast
    def test_add_consumer(self):
        self.control.add_consumer('foo')
        assert 'add_consumer' in MockMailbox.sent

    @with_mock_broadcast
    def test_cancel_consumer(self):
        self.control.cancel_consumer('foo')
        assert 'cancel_consumer' in MockMailbox.sent

    @with_mock_broadcast
    def test_enable_events(self):
        self.control.enable_events()
        assert 'enable_events' in MockMailbox.sent

    @with_mock_broadcast
    def test_disable_events(self):
        self.control.disable_events()
        assert 'disable_events' in MockMailbox.sent

    @with_mock_broadcast
    def test_revoke(self):
        self.control.revoke('foozbaaz')
        assert 'revoke' in MockMailbox.sent

    @with_mock_broadcast
    def test_ping(self):
        self.control.ping()
        assert 'ping' in MockMailbox.sent

    @with_mock_broadcast
    def test_election(self):
        self.control.election('some_id', 'topic', 'action')
        assert 'election' in MockMailbox.sent

    @with_mock_broadcast
    def test_pool_grow(self):
        self.control.pool_grow(2)
        assert 'pool_grow' in MockMailbox.sent

    @with_mock_broadcast
    def test_pool_shrink(self):
        self.control.pool_shrink(2)
        assert 'pool_shrink' in MockMailbox.sent

    @with_mock_broadcast
    def test_revoke_from_result(self):
        self.app.AsyncResult('foozbazzbar').revoke()
        assert 'revoke' in MockMailbox.sent

    @with_mock_broadcast
    def test_revoke_from_resultset(self):
        r = self.app.GroupResult(uuid(),
                                 [self.app.AsyncResult(x)
                                  for x in [uuid() for i in range(10)]])
        r.revoke()
        assert 'revoke' in MockMailbox.sent

from __future__ import absolute_import, unicode_literals

import pytest
from case import Mock

from celery import uuid
from celery.app import control
from celery.exceptions import DuplicateNodenameWarning
from celery.five import items
from celery.utils.collections import LimitedSet


def _info_for_commandclass(type_):
    from celery.worker.control import Panel
    return [
        (name, info)
        for name, info in items(Panel.meta)
        if info.type == type_
    ]


def test_client_implements_all_commands(app):
    commands = _info_for_commandclass('control')
    assert commands
    for name, info in commands:
        assert getattr(app.control, name)


def test_inspect_implements_all_commands(app):
    inspect = app.control.inspect()
    commands = _info_for_commandclass('inspect')
    assert commands
    for name, info in commands:
        if info.type == 'inspect':
            assert getattr(inspect, name)


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
        self.app.control.broadcast = Mock(name='broadcast')
        self.app.control.broadcast.return_value = {}
        self.inspect = self.app.control.inspect()

    def test_prepare_reply(self):
        reply = self.inspect._prepare([
            {'w1': {'ok': 1}},
            {'w2': {'ok': 1}},
        ])
        assert reply == {
            'w1': {'ok': 1},
            'w2': {'ok': 1},
        }

        i = self.app.control.inspect(destination='w1')
        assert i._prepare([{'w1': {'ok': 1}}]) == {'ok': 1}

    def assert_broadcast_called(self, command,
                                destination=None,
                                callback=None,
                                connection=None,
                                limit=None,
                                timeout=None,
                                reply=True,
                                pattern=None,
                                matcher=None,
                                **arguments):
        self.app.control.broadcast.assert_called_with(
            command,
            arguments=arguments,
            destination=destination or self.inspect.destination,
            pattern=pattern or self.inspect.pattern,
            matcher=matcher or self.inspect.destination,
            callback=callback or self.inspect.callback,
            connection=connection or self.inspect.connection,
            limit=limit if limit is not None else self.inspect.limit,
            timeout=timeout if timeout is not None else self.inspect.timeout,
            reply=reply,
        )

    def test_active(self):
        self.inspect.active()
        self.assert_broadcast_called('active')

    def test_clock(self):
        self.inspect.clock()
        self.assert_broadcast_called('clock')

    def test_conf(self):
        self.inspect.conf()
        self.assert_broadcast_called('conf', with_defaults=False)

    def test_conf__with_defaults(self):
        self.inspect.conf(with_defaults=True)
        self.assert_broadcast_called('conf', with_defaults=True)

    def test_hello(self):
        self.inspect.hello('george@vandelay.com')
        self.assert_broadcast_called(
            'hello', from_node='george@vandelay.com', revoked=None)

    def test_hello__with_revoked(self):
        revoked = LimitedSet(100)
        for i in range(100):
            revoked.add('id{0}'.format(i))
        self.inspect.hello('george@vandelay.com', revoked=revoked._data)
        self.assert_broadcast_called(
            'hello', from_node='george@vandelay.com', revoked=revoked._data)

    def test_memsample(self):
        self.inspect.memsample()
        self.assert_broadcast_called('memsample')

    def test_memdump(self):
        self.inspect.memdump()
        self.assert_broadcast_called('memdump', samples=10)

    def test_memdump__samples_specified(self):
        self.inspect.memdump(samples=303)
        self.assert_broadcast_called('memdump', samples=303)

    def test_objgraph(self):
        self.inspect.objgraph()
        self.assert_broadcast_called(
            'objgraph', num=200, type='Request', max_depth=10)

    def test_scheduled(self):
        self.inspect.scheduled()
        self.assert_broadcast_called('scheduled')

    def test_reserved(self):
        self.inspect.reserved()
        self.assert_broadcast_called('reserved')

    def test_stats(self):
        self.inspect.stats()
        self.assert_broadcast_called('stats')

    def test_revoked(self):
        self.inspect.revoked()
        self.assert_broadcast_called('revoked')

    def test_registered(self):
        self.inspect.registered()
        self.assert_broadcast_called('registered', taskinfoitems=())

    def test_registered__taskinfoitems(self):
        self.inspect.registered('rate_limit', 'time_limit')
        self.assert_broadcast_called(
            'registered',
            taskinfoitems=('rate_limit', 'time_limit'),
        )

    def test_ping(self):
        self.inspect.ping()
        self.assert_broadcast_called('ping')

    def test_ping_matcher_pattern(self):
        orig_inspect = self.inspect
        self.inspect = self.app.control.inspect(pattern=".*", matcher="pcre")
        self.inspect.ping()
        try:
            self.assert_broadcast_called('ping', pattern=".*", matcher="pcre")
        except AssertionError as e:
            self.inspect = orig_inspect
            raise e

    def test_active_queues(self):
        self.inspect.active_queues()
        self.assert_broadcast_called('active_queues')

    def test_query_task(self):
        self.inspect.query_task('foo', 'bar')
        self.assert_broadcast_called('query_task', ids=('foo', 'bar'))

    def test_query_task__compat_single_list_argument(self):
        self.inspect.query_task(['foo', 'bar'])
        self.assert_broadcast_called('query_task', ids=['foo', 'bar'])

    def test_query_task__scalar(self):
        self.inspect.query_task('foo')
        self.assert_broadcast_called('query_task', ids=('foo',))

    def test_report(self):
        self.inspect.report()
        self.assert_broadcast_called('report')


class test_Control_broadcast:

    def setup(self):
        self.app.control.mailbox = Mock(name='mailbox')

    def test_broadcast(self):
        self.app.control.broadcast('foobarbaz', arguments={'foo': 2})
        self.app.control.mailbox.assert_called()
        self.app.control.mailbox()._broadcast.assert_called_with(
            'foobarbaz', {'foo': 2}, None, False, 1.0, None, None,
            channel=None,
        )

    def test_broadcast_limit(self):
        self.app.control.broadcast(
            'foobarbaz1', arguments=None, limit=None, destination=[1, 2, 3],
        )
        self.app.control.mailbox.assert_called()
        self.app.control.mailbox()._broadcast.assert_called_with(
            'foobarbaz1', {}, [1, 2, 3], False, 1.0, None, None,
            channel=None,
        )


class test_Control:

    def setup(self):
        self.app.control.broadcast = Mock(name='broadcast')
        self.app.control.broadcast.return_value = {}

        @self.app.task(shared=False)
        def mytask():
            pass
        self.mytask = mytask

    def assert_control_called_with_args(self, name, destination=None,
                                        _options=None, **args):
        self.app.control.broadcast.assert_called_with(
            name, destination=destination, arguments=args, **_options or {})

    def test_purge(self):
        self.app.amqp.TaskConsumer = Mock(name='TaskConsumer')
        self.app.control.purge()
        self.app.amqp.TaskConsumer().purge.assert_called_with()

    def test_rate_limit(self):
        self.app.control.rate_limit(self.mytask.name, '100/m')
        self.assert_control_called_with_args(
            'rate_limit',
            destination=None,
            task_name=self.mytask.name,
            rate_limit='100/m',
        )

    def test_rate_limit__with_destination(self):
        self.app.control.rate_limit(
            self.mytask.name, '100/m', 'a@w.com', limit=100)
        self.assert_control_called_with_args(
            'rate_limit',
            destination='a@w.com',
            task_name=self.mytask.name,
            rate_limit='100/m',
            _options={'limit': 100},
        )

    def test_time_limit(self):
        self.app.control.time_limit(self.mytask.name, soft=10, hard=20)
        self.assert_control_called_with_args(
            'time_limit',
            destination=None,
            task_name=self.mytask.name,
            soft=10,
            hard=20,
        )

    def test_time_limit__with_destination(self):
        self.app.control.time_limit(
            self.mytask.name, soft=10, hard=20,
            destination='a@q.com', limit=99,
        )
        self.assert_control_called_with_args(
            'time_limit',
            destination='a@q.com',
            task_name=self.mytask.name,
            soft=10,
            hard=20,
            _options={'limit': 99},
        )

    def test_add_consumer(self):
        self.app.control.add_consumer('foo')
        self.assert_control_called_with_args(
            'add_consumer',
            destination=None,
            queue='foo',
            exchange=None,
            exchange_type='direct',
            routing_key=None,
        )

    def test_add_consumer__with_options_and_dest(self):
        self.app.control.add_consumer(
            'foo', 'ex', 'topic', 'rkey', destination='a@q.com', limit=78)
        self.assert_control_called_with_args(
            'add_consumer',
            destination='a@q.com',
            queue='foo',
            exchange='ex',
            exchange_type='topic',
            routing_key='rkey',
            _options={'limit': 78},
        )

    def test_cancel_consumer(self):
        self.app.control.cancel_consumer('foo')
        self.assert_control_called_with_args(
            'cancel_consumer',
            destination=None,
            queue='foo',
        )

    def test_cancel_consumer__with_destination(self):
        self.app.control.cancel_consumer(
            'foo', destination='w1@q.com', limit=3)
        self.assert_control_called_with_args(
            'cancel_consumer',
            destination='w1@q.com',
            queue='foo',
            _options={'limit': 3},
        )

    def test_shutdown(self):
        self.app.control.shutdown()
        self.assert_control_called_with_args('shutdown', destination=None)

    def test_shutdown__with_destination(self):
        self.app.control.shutdown(destination='a@q.com', limit=3)
        self.assert_control_called_with_args(
            'shutdown', destination='a@q.com', _options={'limit': 3})

    def test_heartbeat(self):
        self.app.control.heartbeat()
        self.assert_control_called_with_args('heartbeat', destination=None)

    def test_heartbeat__with_destination(self):
        self.app.control.heartbeat(destination='a@q.com', limit=3)
        self.assert_control_called_with_args(
            'heartbeat', destination='a@q.com', _options={'limit': 3})

    def test_pool_restart(self):
        self.app.control.pool_restart()
        self.assert_control_called_with_args(
            'pool_restart',
            destination=None,
            modules=None,
            reload=False,
            reloader=None)

    def test_terminate(self):
        self.app.control.revoke = Mock(name='revoke')
        self.app.control.terminate('124')
        self.app.control.revoke.assert_called_with(
            '124', destination=None,
            terminate=True,
            signal=control.TERM_SIGNAME,
        )

    def test_enable_events(self):
        self.app.control.enable_events()
        self.assert_control_called_with_args('enable_events', destination=None)

    def test_enable_events_with_destination(self):
        self.app.control.enable_events(destination='a@q.com', limit=3)
        self.assert_control_called_with_args(
            'enable_events', destination='a@q.com', _options={'limit': 3})

    def test_disable_events(self):
        self.app.control.disable_events()
        self.assert_control_called_with_args(
            'disable_events', destination=None)

    def test_disable_events_with_destination(self):
        self.app.control.disable_events(destination='a@q.com', limit=3)
        self.assert_control_called_with_args(
            'disable_events', destination='a@q.com', _options={'limit': 3})

    def test_ping(self):
        self.app.control.ping()
        self.assert_control_called_with_args(
            'ping', destination=None,
            _options={'timeout': 1.0, 'reply': True})

    def test_ping_with_destination(self):
        self.app.control.ping(destination='a@q.com', limit=3)
        self.assert_control_called_with_args(
            'ping',
            destination='a@q.com',
            _options={
                'limit': 3,
                'timeout': 1.0,
                'reply': True,
            })

    def test_revoke(self):
        self.app.control.revoke('foozbaaz')
        self.assert_control_called_with_args(
            'revoke',
            destination=None,
            task_id='foozbaaz',
            signal=control.TERM_SIGNAME,
            terminate=False,
        )

    def test_revoke__with_options(self):
        self.app.control.revoke(
            'foozbaaz',
            destination='a@q.com',
            terminate=True,
            signal='KILL',
            limit=404,
        )
        self.assert_control_called_with_args(
            'revoke',
            destination='a@q.com',
            task_id='foozbaaz',
            signal='KILL',
            terminate=True,
            _options={'limit': 404},
        )

    def test_election(self):
        self.app.control.election('some_id', 'topic', 'action')
        self.assert_control_called_with_args(
            'election',
            destination=None,
            topic='topic',
            action='action',
            id='some_id',
            _options={'connection': None},
        )

    def test_autoscale(self):
        self.app.control.autoscale(300, 10)
        self.assert_control_called_with_args(
            'autoscale', max=300, min=10, destination=None)

    def test_autoscale__with_options(self):
        self.app.control.autoscale(300, 10, destination='a@q.com', limit=39)
        self.assert_control_called_with_args(
            'autoscale', max=300, min=10,
            destination='a@q.com',
            _options={'limit': 39}
        )

    def test_pool_grow(self):
        self.app.control.pool_grow(2)
        self.assert_control_called_with_args(
            'pool_grow', n=2, destination=None)

    def test_pool_grow__with_options(self):
        self.app.control.pool_grow(2, destination='a@q.com', limit=39)
        self.assert_control_called_with_args(
            'pool_grow', n=2,
            destination='a@q.com',
            _options={'limit': 39}
        )

    def test_pool_shrink(self):
        self.app.control.pool_shrink(2)
        self.assert_control_called_with_args(
            'pool_shrink', n=2, destination=None)

    def test_pool_shrink__with_options(self):
        self.app.control.pool_shrink(2, destination='a@q.com', limit=39)
        self.assert_control_called_with_args(
            'pool_shrink', n=2,
            destination='a@q.com',
            _options={'limit': 39}
        )

    def test_revoke_from_result(self):
        self.app.control.revoke = Mock(name='revoke')
        self.app.AsyncResult('foozbazzbar').revoke()
        self.app.control.revoke.assert_called_with(
            'foozbazzbar',
            connection=None, reply=False, signal=None,
            terminate=False, timeout=None)

    def test_revoke_from_resultset(self):
        self.app.control.revoke = Mock(name='revoke')
        uuids = [uuid() for _ in range(10)]
        r = self.app.GroupResult(
            uuid(), [self.app.AsyncResult(x) for x in uuids])
        r.revoke()
        self.app.control.revoke.assert_called_with(
            uuids,
            connection=None, reply=False, signal=None,
            terminate=False, timeout=None)

    def test_after_fork_clears_mailbox_pool(self):
        amqp = Mock(name='amqp')
        self.app.amqp = amqp
        closed_pool = Mock(name='closed pool')
        amqp.producer_pool = closed_pool
        assert closed_pool is self.app.control.mailbox.producer_pool
        self.app.control._after_fork()
        new_pool = Mock(name='new pool')
        amqp.producer_pool = new_pool
        assert new_pool is self.app.control.mailbox.producer_pool

    def test_control_exchange__default(self):
        c = control.Control(self.app)
        assert c.mailbox.namespace == 'celery'

    def test_control_exchange__setting(self):
        self.app.conf.control_exchange = 'test_exchange'
        c = control.Control(self.app)
        assert c.mailbox.namespace == 'test_exchange'

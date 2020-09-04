import socket
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from queue import Queue as FastQueue
from unittest.mock import Mock, call, patch

import pytest
from kombu import pidbox
from kombu.utils.uuid import uuid

from celery.utils.collections import AttributeDict
from celery.utils.timer2 import Timer
from celery.worker import WorkController as _WC  # noqa
from celery.worker import consumer, control
from celery.worker import state as worker_state
from celery.worker.pidbox import Pidbox, gPidbox
from celery.worker.request import Request
from celery.worker.state import revoked

hostname = socket.gethostname()


class WorkController:
    autoscaler = None

    def stats(self):
        return {'total': worker_state.total_count}


class Consumer(consumer.Consumer):

    def __init__(self, app):
        self.app = app
        self.buffer = FastQueue()
        self.timer = Timer()
        self.event_dispatcher = Mock()
        self.controller = WorkController()
        self.task_consumer = Mock()
        self.prefetch_multiplier = 1
        self.initial_prefetch_count = 1

        from celery.concurrency.base import BasePool
        self.pool = BasePool(10)
        self.task_buckets = defaultdict(lambda: None)
        self.hub = None

    def call_soon(self, p, *args, **kwargs):
        return p(*args, **kwargs)


class test_Pidbox:

    def test_shutdown(self):
        with patch('celery.worker.pidbox.ignore_errors') as eig:
            parent = Mock()
            pbox = Pidbox(parent)
            pbox._close_channel = Mock()
            assert pbox.c is parent
            pconsumer = pbox.consumer = Mock()
            cancel = pconsumer.cancel
            pbox.shutdown(parent)
            eig.assert_called_with(parent, cancel)
            pbox._close_channel.assert_called_with(parent)


class test_Pidbox_green:

    def test_stop(self):
        parent = Mock()
        g = gPidbox(parent)
        stopped = g._node_stopped = Mock()
        shutdown = g._node_shutdown = Mock()
        close_chan = g._close_channel = Mock()

        g.stop(parent)
        shutdown.set.assert_called_with()
        stopped.wait.assert_called_with()
        close_chan.assert_called_with(parent)
        assert g._node_stopped is None
        assert g._node_shutdown is None

        close_chan.reset()
        g.stop(parent)
        close_chan.assert_called_with(parent)

    def test_resets(self):
        parent = Mock()
        g = gPidbox(parent)
        g._resets = 100
        g.reset()
        assert g._resets == 101

    def test_loop(self):
        parent = Mock()
        conn = self.app.connection_for_read()
        parent.connection_for_read.return_value = conn
        drain = conn.drain_events = Mock()
        g = gPidbox(parent)
        parent.connection = Mock()
        do_reset = g._do_reset = Mock()

        call_count = [0]

        def se(*args, **kwargs):
            if call_count[0] > 2:
                g._node_shutdown.set()
            g.reset()
            call_count[0] += 1
        drain.side_effect = se
        g.loop(parent)

        assert do_reset.call_count == 4


class test_ControlPanel:

    def setup(self):
        self.panel = self.create_panel(consumer=Consumer(self.app))

        @self.app.task(name='c.unittest.mytask', rate_limit=200, shared=False)
        def mytask():
            pass
        self.mytask = mytask

    def create_state(self, **kwargs):
        kwargs.setdefault('app', self.app)
        kwargs.setdefault('hostname', hostname)
        kwargs.setdefault('tset', set)
        return AttributeDict(kwargs)

    def create_panel(self, **kwargs):
        return self.app.control.mailbox.Node(
            hostname=hostname,
            state=self.create_state(**kwargs),
            handlers=control.Panel.data,
        )

    def test_enable_events(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        evd = consumer.event_dispatcher
        evd.groups = set()
        panel.handle('enable_events')
        assert not evd.groups
        evd.groups = {'worker'}
        panel.handle('enable_events')
        assert 'task' in evd.groups
        evd.groups = {'task'}
        assert 'already enabled' in panel.handle('enable_events')['ok']

    def test_disable_events(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        evd = consumer.event_dispatcher
        evd.enabled = True
        evd.groups = {'task'}
        panel.handle('disable_events')
        assert 'task' not in evd.groups
        assert 'already disabled' in panel.handle('disable_events')['ok']

    def test_clock(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        panel.state.app.clock.value = 313
        x = panel.handle('clock')
        assert x['clock'] == 313

    def test_hello(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        panel.state.app.clock.value = 313
        panel.state.hostname = 'elaine@vandelay.com'
        worker_state.revoked.add('revoked1')
        try:
            assert panel.handle('hello', {
                'from_node': 'elaine@vandelay.com',
            }) is None
            x = panel.handle('hello', {
                'from_node': 'george@vandelay.com',
            })
            assert x['clock'] == 314  # incremented
            x = panel.handle('hello', {
                'from_node': 'george@vandelay.com',
                'revoked': {'1234', '4567', '891'}
            })
            assert 'revoked1' in x['revoked']
            assert '1234' in x['revoked']
            assert '4567' in x['revoked']
            assert '891' in x['revoked']
            assert x['clock'] == 315  # incremented
        finally:
            worker_state.revoked.discard('revoked1')

    def test_conf(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        panel.app = self.app
        panel.app.finalize()
        self.app.conf.some_key6 = 'hello world'
        x = panel.handle('dump_conf')
        assert 'some_key6' in x

    def test_election(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        consumer.gossip = Mock()
        panel.handle(
            'election', {'id': 'id', 'topic': 'topic', 'action': 'action'},
        )
        consumer.gossip.election.assert_called_with('id', 'topic', 'action')

    def test_election__no_gossip(self):
        consumer = Mock(name='consumer')
        consumer.gossip = None
        panel = self.create_panel(consumer=consumer)
        panel.handle(
            'election', {'id': 'id', 'topic': 'topic', 'action': 'action'},
        )

    def test_heartbeat(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        event_dispatcher = consumer.event_dispatcher
        event_dispatcher.enabled = True
        panel.handle('heartbeat')
        assert ('worker-heartbeat',) in event_dispatcher.send.call_args

    def test_time_limit(self):
        panel = self.create_panel(consumer=Mock())
        r = panel.handle('time_limit', arguments={
            'task_name': self.mytask.name, 'hard': 30, 'soft': 10})
        assert self.mytask.time_limit == 30
        assert self.mytask.soft_time_limit == 10
        assert 'ok' in r
        r = panel.handle('time_limit', arguments={
            'task_name': self.mytask.name, 'hard': None, 'soft': None})
        assert self.mytask.time_limit is None
        assert self.mytask.soft_time_limit is None
        assert 'ok' in r

        r = panel.handle('time_limit', arguments={
            'task_name': '248e8afya9s8dh921eh928', 'hard': 30})
        assert 'error' in r

    def test_active_queues(self):
        import kombu

        x = kombu.Consumer(self.app.connection_for_read(),
                           [kombu.Queue('foo', kombu.Exchange('foo'), 'foo'),
                            kombu.Queue('bar', kombu.Exchange('bar'), 'bar')],
                           auto_declare=False)
        consumer = Mock()
        consumer.task_consumer = x
        panel = self.create_panel(consumer=consumer)
        r = panel.handle('active_queues')
        assert list(sorted(q['name'] for q in r)) == ['bar', 'foo']

    def test_active_queues__empty(self):
        consumer = Mock(name='consumer')
        panel = self.create_panel(consumer=consumer)
        consumer.task_consumer = None
        assert not panel.handle('active_queues')

    def test_dump_tasks(self):
        info = '\n'.join(self.panel.handle('dump_tasks'))
        assert 'mytask' in info
        assert 'rate_limit=200' in info

    def test_dump_tasks2(self):
        prev, control.DEFAULT_TASK_INFO_ITEMS = (
            control.DEFAULT_TASK_INFO_ITEMS, [])
        try:
            info = '\n'.join(self.panel.handle('dump_tasks'))
            assert 'mytask' in info
            assert 'rate_limit=200' not in info
        finally:
            control.DEFAULT_TASK_INFO_ITEMS = prev

    def test_stats(self):
        prev_count, worker_state.total_count = worker_state.total_count, 100
        try:
            assert self.panel.handle('stats')['total'] == 100
        finally:
            worker_state.total_count = prev_count

    def test_report(self):
        self.panel.handle('report')

    def test_active(self):
        r = Request(
            self.TaskMessage(self.mytask.name, 'do re mi'),
            app=self.app,
        )
        worker_state.active_requests.add(r)
        try:
            assert self.panel.handle('dump_active')
        finally:
            worker_state.active_requests.discard(r)

    def test_pool_grow(self):

        class MockPool:

            def __init__(self, size=1):
                self.size = size

            def grow(self, n=1):
                self.size += n

            def shrink(self, n=1):
                self.size -= n

            @property
            def num_processes(self):
                return self.size

        consumer = Consumer(self.app)
        consumer.prefetch_multiplier = 8
        consumer.qos = Mock(name='qos')
        consumer.pool = MockPool(1)
        panel = self.create_panel(consumer=consumer)

        panel.handle('pool_grow')
        assert consumer.pool.size == 2
        consumer.qos.increment_eventually.assert_called_with(8)
        assert consumer.initial_prefetch_count == 16
        panel.handle('pool_shrink')
        assert consumer.pool.size == 1
        consumer.qos.decrement_eventually.assert_called_with(8)
        assert consumer.initial_prefetch_count == 8

        panel.state.consumer = Mock()
        panel.state.consumer.controller = Mock()
        r = panel.handle('pool_grow')
        assert 'error' in r
        r = panel.handle('pool_shrink')
        assert 'error' in r

    def test_add__cancel_consumer(self):

        class MockConsumer:
            queues = []
            canceled = []
            consuming = False
            hub = Mock(name='hub')

            def add_queue(self, queue):
                self.queues.append(queue.name)

            def consume(self):
                self.consuming = True

            def cancel_by_queue(self, queue):
                self.canceled.append(queue)

            def consuming_from(self, queue):
                return queue in self.queues

        consumer = Consumer(self.app)
        consumer.task_consumer = MockConsumer()
        panel = self.create_panel(consumer=consumer)

        panel.handle('add_consumer', {'queue': 'MyQueue'})
        assert 'MyQueue' in consumer.task_consumer.queues
        assert consumer.task_consumer.consuming
        panel.handle('add_consumer', {'queue': 'MyQueue'})
        panel.handle('cancel_consumer', {'queue': 'MyQueue'})
        assert 'MyQueue' in consumer.task_consumer.canceled

    def test_revoked(self):
        worker_state.revoked.clear()
        worker_state.revoked.add('a1')
        worker_state.revoked.add('a2')

        try:
            assert sorted(self.panel.handle('dump_revoked')) == ['a1', 'a2']
        finally:
            worker_state.revoked.clear()

    def test_dump_schedule(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        assert not panel.handle('dump_schedule')
        r = Request(
            self.TaskMessage(self.mytask.name, 'CAFEBABE'),
            app=self.app,
        )
        consumer.timer.schedule.enter_at(
            consumer.timer.Entry(lambda x: x, (r,)),
            datetime.now() + timedelta(seconds=10))
        consumer.timer.schedule.enter_at(
            consumer.timer.Entry(lambda x: x, (object(),)),
            datetime.now() + timedelta(seconds=10))
        assert panel.handle('dump_schedule')

    def test_dump_reserved(self):
        consumer = Consumer(self.app)
        req = Request(
            self.TaskMessage(self.mytask.name, args=(2, 2)), app=self.app,
        )  # ^ need to keep reference for reserved_tasks WeakSet.
        worker_state.task_reserved(req)
        try:
            panel = self.create_panel(consumer=consumer)
            response = panel.handle('dump_reserved', {'safe': True})
            assert response[0]['name'] == self.mytask.name
            assert response[0]['hostname'] == socket.gethostname()
            worker_state.reserved_requests.clear()
            assert not panel.handle('dump_reserved')
        finally:
            worker_state.reserved_requests.clear()

    def test_rate_limit_invalid_rate_limit_string(self):
        e = self.panel.handle('rate_limit', arguments={
            'task_name': 'tasks.add', 'rate_limit': 'x1240301#%!'})
        assert 'Invalid rate limit string' in e.get('error')

    def test_rate_limit(self):

        class xConsumer:
            reset = False

            def reset_rate_limits(self):
                self.reset = True

        consumer = xConsumer()
        panel = self.create_panel(app=self.app, consumer=consumer)

        task = self.app.tasks[self.mytask.name]
        panel.handle('rate_limit', arguments={'task_name': task.name,
                                              'rate_limit': '100/m'})
        assert task.rate_limit == '100/m'
        assert consumer.reset
        consumer.reset = False
        panel.handle('rate_limit', arguments={
            'task_name': task.name,
            'rate_limit': 0,
        })
        assert task.rate_limit == 0
        assert consumer.reset

    def test_rate_limit_nonexistant_task(self):
        self.panel.handle('rate_limit', arguments={
            'task_name': 'xxxx.does.not.exist',
            'rate_limit': '1000/s'})

    def test_unexposed_command(self):
        with pytest.raises(KeyError):
            self.panel.handle('foo', arguments={})

    def test_revoke_with_name(self):
        tid = uuid()
        m = {
            'method': 'revoke',
            'destination': hostname,
            'arguments': {
                'task_id': tid,
                'task_name': self.mytask.name,
            },
        }
        self.panel.handle_message(m, None)
        assert tid in revoked

    def test_revoke_with_name_not_in_registry(self):
        tid = uuid()
        m = {
            'method': 'revoke',
            'destination': hostname,
            'arguments': {
                'task_id': tid,
                'task_name': 'xxxxxxxxx33333333388888',
            },
        }
        self.panel.handle_message(m, None)
        assert tid in revoked

    def test_revoke(self):
        tid = uuid()
        m = {
            'method': 'revoke',
            'destination': hostname,
            'arguments': {
                'task_id': tid,
            },
        }
        self.panel.handle_message(m, None)
        assert tid in revoked

        m = {
            'method': 'revoke',
            'destination': 'does.not.exist',
            'arguments': {
                'task_id': tid + 'xxx',
            },
        }
        self.panel.handle_message(m, None)
        assert tid + 'xxx' not in revoked

    def test_revoke_terminate(self):
        request = Mock()
        request.id = tid = uuid()
        state = self.create_state()
        state.consumer = Mock()
        worker_state.task_reserved(request)
        try:
            r = control.revoke(state, tid, terminate=True)
            assert tid in revoked
            assert request.terminate.call_count
            assert 'terminate:' in r['ok']
            # unknown task id only revokes
            r = control.revoke(state, uuid(), terminate=True)
            assert 'tasks unknown' in r['ok']
        finally:
            worker_state.task_ready(request)

    def test_autoscale(self):
        self.panel.state.consumer = Mock()
        self.panel.state.consumer.controller = Mock()
        sc = self.panel.state.consumer.controller.autoscaler = Mock()
        sc.update.return_value = 10, 2
        m = {'method': 'autoscale',
             'destination': hostname,
             'arguments': {'max': '10', 'min': '2'}}
        r = self.panel.handle_message(m, None)
        assert 'ok' in r

        self.panel.state.consumer.controller.autoscaler = None
        r = self.panel.handle_message(m, None)
        assert 'error' in r

    def test_ping(self):
        m = {'method': 'ping',
             'destination': hostname}
        r = self.panel.handle_message(m, None)
        assert r == {'ok': 'pong'}

    def test_shutdown(self):
        m = {'method': 'shutdown',
             'destination': hostname}
        with pytest.raises(SystemExit):
            self.panel.handle_message(m, None)

    def test_panel_reply(self):

        replies = []

        class _Node(pidbox.Node):

            def reply(self, data, exchange, routing_key, **kwargs):
                replies.append(data)

        panel = _Node(
            hostname=hostname,
            state=self.create_state(consumer=Consumer(self.app)),
            handlers=control.Panel.data,
            mailbox=self.app.control.mailbox,
        )
        r = panel.dispatch('ping', reply_to={
            'exchange': 'x',
            'routing_key': 'x',
        })
        assert r == {'ok': 'pong'}
        assert replies[0] == {panel.hostname: {'ok': 'pong'}}

    def test_pool_restart(self):
        consumer = Consumer(self.app)
        consumer.controller = _WC(app=self.app)
        consumer.controller.consumer = consumer
        consumer.controller.pool.restart = Mock()
        consumer.reset_rate_limits = Mock(name='reset_rate_limits()')
        consumer.update_strategies = Mock(name='update_strategies()')
        consumer.event_dispatcher = Mock(name='evd')
        panel = self.create_panel(consumer=consumer)
        assert panel.state.consumer.controller.consumer is consumer
        panel.app = self.app
        _import = panel.app.loader.import_from_cwd = Mock()
        _reload = Mock()

        with pytest.raises(ValueError):
            panel.handle('pool_restart', {'reloader': _reload})

        self.app.conf.worker_pool_restarts = True
        panel.handle('pool_restart', {'reloader': _reload})
        consumer.controller.pool.restart.assert_called()
        consumer.reset_rate_limits.assert_called_with()
        consumer.update_strategies.assert_called_with()
        _reload.assert_not_called()
        _import.assert_not_called()
        consumer.controller.pool.restart.side_effect = NotImplementedError()
        panel.handle('pool_restart', {'reloader': _reload})
        consumer.controller.consumer = None
        panel.handle('pool_restart', {'reloader': _reload})

    @patch('celery.worker.worker.logger.debug')
    def test_pool_restart_import_modules(self, _debug):
        consumer = Consumer(self.app)
        consumer.controller = _WC(app=self.app)
        consumer.controller.consumer = consumer
        consumer.controller.pool.restart = Mock()
        consumer.reset_rate_limits = Mock(name='reset_rate_limits()')
        consumer.update_strategies = Mock(name='update_strategies()')
        panel = self.create_panel(consumer=consumer)
        panel.app = self.app
        assert panel.state.consumer.controller.consumer is consumer
        _import = consumer.controller.app.loader.import_from_cwd = Mock()
        _reload = Mock()

        self.app.conf.worker_pool_restarts = True
        with patch('sys.modules'):
            panel.handle('pool_restart', {
                'modules': ['foo', 'bar'],
                'reloader': _reload,
            })
        consumer.controller.pool.restart.assert_called()
        consumer.reset_rate_limits.assert_called_with()
        consumer.update_strategies.assert_called_with()
        _reload.assert_not_called()
        _import.assert_has_calls([call('bar'), call('foo')], any_order=True)
        assert _import.call_count == 2

    def test_pool_restart_reload_modules(self):
        consumer = Consumer(self.app)
        consumer.controller = _WC(app=self.app)
        consumer.controller.consumer = consumer
        consumer.controller.pool.restart = Mock()
        consumer.reset_rate_limits = Mock(name='reset_rate_limits()')
        consumer.update_strategies = Mock(name='update_strategies()')
        panel = self.create_panel(consumer=consumer)
        panel.app = self.app
        _import = panel.app.loader.import_from_cwd = Mock()
        _reload = Mock()

        self.app.conf.worker_pool_restarts = True
        with patch.dict(sys.modules, {'foo': None}):
            panel.handle('pool_restart', {
                'modules': ['foo'],
                'reload': False,
                'reloader': _reload,
            })

            consumer.controller.pool.restart.assert_called()
            _reload.assert_not_called()
            _import.assert_not_called()

            _import.reset_mock()
            _reload.reset_mock()
            consumer.controller.pool.restart.reset_mock()

            panel.handle('pool_restart', {
                'modules': ['foo'],
                'reload': True,
                'reloader': _reload,
            })
            consumer.controller.pool.restart.assert_called()
            _reload.assert_called()
            _import.assert_not_called()

    def test_query_task(self):
        consumer = Consumer(self.app)
        consumer.controller = _WC(app=self.app)
        consumer.controller.consumer = consumer
        panel = self.create_panel(consumer=consumer)
        panel.app = self.app
        req1 = Request(
            self.TaskMessage(self.mytask.name, args=(2, 2)),
            app=self.app,
        )
        worker_state.task_reserved(req1)
        try:
            assert not panel.handle('query_task', {'ids': {'1daa'}})
            ret = panel.handle('query_task', {'ids': {req1.id}})
            assert req1.id in ret
            assert ret[req1.id][0] == 'reserved'
            worker_state.active_requests.add(req1)
            try:
                ret = panel.handle('query_task', {'ids': {req1.id}})
                assert ret[req1.id][0] == 'active'
            finally:
                worker_state.active_requests.clear()
            ret = panel.handle('query_task', {'ids': {req1.id}})
            assert ret[req1.id][0] == 'reserved'
        finally:
            worker_state.reserved_requests.clear()

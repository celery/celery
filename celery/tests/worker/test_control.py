from __future__ import absolute_import, unicode_literals

import sys
import socket

from collections import defaultdict
from datetime import datetime, timedelta

from kombu import pidbox
from kombu.utils import uuid

from celery.five import Queue as FastQueue
from celery.utils.timer2 import Timer
from celery.worker import WorkController as _WC
from celery.worker import consumer
from celery.worker import control
from celery.worker import state as worker_state
from celery.worker.request import Request
from celery.worker.state import revoked
from celery.worker.pidbox import Pidbox, gPidbox
from celery.utils.collections import AttributeDict

from celery.tests.case import AppCase, Mock, TaskMessage, call, patch

hostname = socket.gethostname()


class WorkController(object):

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


class test_Pidbox(AppCase):

    def test_shutdown(self):
        with patch('celery.worker.pidbox.ignore_errors') as eig:
            parent = Mock()
            pbox = Pidbox(parent)
            pbox._close_channel = Mock()
            self.assertIs(pbox.c, parent)
            pconsumer = pbox.consumer = Mock()
            cancel = pconsumer.cancel
            pbox.shutdown(parent)
            eig.assert_called_with(parent, cancel)
            pbox._close_channel.assert_called_with(parent)


class test_Pidbox_green(AppCase):

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
        self.assertIsNone(g._node_stopped)
        self.assertIsNone(g._node_shutdown)

        close_chan.reset()
        g.stop(parent)
        close_chan.assert_called_with(parent)

    def test_resets(self):
        parent = Mock()
        g = gPidbox(parent)
        g._resets = 100
        g.reset()
        self.assertEqual(g._resets, 101)

    def test_loop(self):
        parent = Mock()
        conn = parent.connect.return_value = self.app.connection_for_read()
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

        self.assertEqual(do_reset.call_count, 4)


class test_ControlPanel(AppCase):

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
        return self.app.control.mailbox.Node(hostname=hostname,
                                             state=self.create_state(**kwargs),
                                             handlers=control.Panel.data)

    def test_enable_events(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        evd = consumer.event_dispatcher
        evd.groups = set()
        panel.handle('enable_events')
        self.assertFalse(evd.groups)
        evd.groups = {'worker'}
        panel.handle('enable_events')
        self.assertIn('task', evd.groups)
        evd.groups = {'task'}
        self.assertIn('already enabled', panel.handle('enable_events')['ok'])

    def test_disable_events(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        evd = consumer.event_dispatcher
        evd.enabled = True
        evd.groups = {'task'}
        panel.handle('disable_events')
        self.assertNotIn('task', evd.groups)
        self.assertIn('already disabled', panel.handle('disable_events')['ok'])

    def test_clock(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        panel.state.app.clock.value = 313
        x = panel.handle('clock')
        self.assertEqual(x['clock'], 313)

    def test_hello(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        panel.state.app.clock.value = 313
        panel.state.hostname = 'elaine@vandelay.com'
        worker_state.revoked.add('revoked1')
        try:
            self.assertIsNone(panel.handle('hello', {
                'from_node': 'elaine@vandelay.com',
            }))
            x = panel.handle('hello', {
                'from_node': 'george@vandelay.com',
            })
            self.assertEqual(x['clock'], 314)  # incremented
            x = panel.handle('hello', {
                'from_node': 'george@vandelay.com',
                'revoked': {'1234', '4567', '891'}
            })
            self.assertIn('revoked1', x['revoked'])
            self.assertIn('1234', x['revoked'])
            self.assertIn('4567', x['revoked'])
            self.assertIn('891', x['revoked'])
            self.assertEqual(x['clock'], 315)  # incremented
        finally:
            worker_state.revoked.discard('revoked1')

    def test_conf(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        panel.app = self.app
        panel.app.finalize()
        self.app.conf.some_key6 = 'hello world'
        x = panel.handle('dump_conf')
        self.assertIn('some_key6', x)

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
        consumer.event_dispatcher.enabled = True
        panel.handle('heartbeat')
        self.assertIn(('worker-heartbeat',),
                      consumer.event_dispatcher.send.call_args)

    def test_time_limit(self):
        panel = self.create_panel(consumer=Mock())
        r = panel.handle('time_limit', arguments=dict(
            task_name=self.mytask.name, hard=30, soft=10))
        self.assertEqual(
            (self.mytask.time_limit, self.mytask.soft_time_limit),
            (30, 10),
        )
        self.assertIn('ok', r)
        r = panel.handle('time_limit', arguments=dict(
            task_name=self.mytask.name, hard=None, soft=None))
        self.assertEqual(
            (self.mytask.time_limit, self.mytask.soft_time_limit),
            (None, None),
        )
        self.assertIn('ok', r)

        r = panel.handle('time_limit', arguments=dict(
            task_name='248e8afya9s8dh921eh928', hard=30))
        self.assertIn('error', r)

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
        self.assertListEqual(list(sorted(q['name'] for q in r)),
                             ['bar', 'foo'])

    def test_active_queues__empty(self):
        consumer = Mock(name='consumer')
        panel = self.create_panel(consumer=consumer)
        consumer.task_consumer = None
        self.assertFalse(panel.handle('active_queues'))

    def test_dump_tasks(self):
        info = '\n'.join(self.panel.handle('dump_tasks'))
        self.assertIn('mytask', info)
        self.assertIn('rate_limit=200', info)

    def test_dump_tasks2(self):
        prev, control.DEFAULT_TASK_INFO_ITEMS = (
            control.DEFAULT_TASK_INFO_ITEMS, [])
        try:
            info = '\n'.join(self.panel.handle('dump_tasks'))
            self.assertIn('mytask', info)
            self.assertNotIn('rate_limit=200', info)
        finally:
            control.DEFAULT_TASK_INFO_ITEMS = prev

    def test_stats(self):
        prev_count, worker_state.total_count = worker_state.total_count, 100
        try:
            self.assertDictContainsSubset({'total': 100},
                                          self.panel.handle('stats'))
        finally:
            worker_state.total_count = prev_count

    def test_report(self):
        self.panel.handle('report')

    def test_active(self):
        r = Request(TaskMessage(self.mytask.name, 'do re mi'), app=self.app)
        worker_state.active_requests.add(r)
        try:
            self.assertTrue(self.panel.handle('dump_active'))
        finally:
            worker_state.active_requests.discard(r)

    def test_pool_grow(self):

        class MockPool(object):

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
        self.assertEqual(consumer.pool.size, 2)
        consumer.qos.increment_eventually.assert_called_with(8)
        self.assertEqual(consumer.initial_prefetch_count, 16)
        panel.handle('pool_shrink')
        self.assertEqual(consumer.pool.size, 1)
        consumer.qos.decrement_eventually.assert_called_with(8)
        self.assertEqual(consumer.initial_prefetch_count, 8)

        panel.state.consumer = Mock()
        panel.state.consumer.controller = Mock()

    def test_add__cancel_consumer(self):

        class MockConsumer(object):
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
        self.assertIn('MyQueue', consumer.task_consumer.queues)
        self.assertTrue(consumer.task_consumer.consuming)
        panel.handle('add_consumer', {'queue': 'MyQueue'})
        panel.handle('cancel_consumer', {'queue': 'MyQueue'})
        self.assertIn('MyQueue', consumer.task_consumer.canceled)

    def test_revoked(self):
        worker_state.revoked.clear()
        worker_state.revoked.add('a1')
        worker_state.revoked.add('a2')

        try:
            self.assertEqual(sorted(self.panel.handle('dump_revoked')),
                             ['a1', 'a2'])
        finally:
            worker_state.revoked.clear()

    def test_dump_schedule(self):
        consumer = Consumer(self.app)
        panel = self.create_panel(consumer=consumer)
        self.assertFalse(panel.handle('dump_schedule'))
        r = Request(TaskMessage(self.mytask.name, 'CAFEBABE'), app=self.app)
        consumer.timer.schedule.enter_at(
            consumer.timer.Entry(lambda x: x, (r,)),
            datetime.now() + timedelta(seconds=10))
        consumer.timer.schedule.enter_at(
            consumer.timer.Entry(lambda x: x, (object(),)),
            datetime.now() + timedelta(seconds=10))
        self.assertTrue(panel.handle('dump_schedule'))

    def test_dump_reserved(self):
        consumer = Consumer(self.app)
        req = Request(
            TaskMessage(self.mytask.name, args=(2, 2)), app=self.app,
        )  # ^ need to keep reference for reserved_tasks WeakSet.
        worker_state.task_reserved(req)
        try:
            panel = self.create_panel(consumer=consumer)
            response = panel.handle('dump_reserved', {'safe': True})
            self.assertDictContainsSubset(
                {'name': self.mytask.name,
                 'hostname': socket.gethostname()},
                response[0],
            )
            worker_state.reserved_requests.clear()
            self.assertFalse(panel.handle('dump_reserved'))
        finally:
            worker_state.reserved_requests.clear()

    def test_rate_limit_invalid_rate_limit_string(self):
        e = self.panel.handle('rate_limit', arguments=dict(
            task_name='tasks.add', rate_limit='x1240301#%!'))
        self.assertIn('Invalid rate limit string', e.get('error'))

    def test_rate_limit(self):

        class xConsumer(object):
            reset = False

            def reset_rate_limits(self):
                self.reset = True

        consumer = xConsumer()
        panel = self.create_panel(app=self.app, consumer=consumer)

        task = self.app.tasks[self.mytask.name]
        panel.handle('rate_limit', arguments=dict(task_name=task.name,
                                                  rate_limit='100/m'))
        self.assertEqual(task.rate_limit, '100/m')
        self.assertTrue(consumer.reset)
        consumer.reset = False
        panel.handle('rate_limit', arguments=dict(task_name=task.name,
                                                  rate_limit=0))
        self.assertEqual(task.rate_limit, 0)
        self.assertTrue(consumer.reset)

    def test_rate_limit_nonexistant_task(self):
        self.panel.handle('rate_limit', arguments={
            'task_name': 'xxxx.does.not.exist',
            'rate_limit': '1000/s'})

    def test_unexposed_command(self):
        with self.assertRaises(KeyError):
            self.panel.handle('foo', arguments={})

    def test_revoke_with_name(self):
        tid = uuid()
        m = {'method': 'revoke',
             'destination': hostname,
             'arguments': {'task_id': tid,
                           'task_name': self.mytask.name}}
        self.panel.handle_message(m, None)
        self.assertIn(tid, revoked)

    def test_revoke_with_name_not_in_registry(self):
        tid = uuid()
        m = {'method': 'revoke',
             'destination': hostname,
             'arguments': {'task_id': tid,
                           'task_name': 'xxxxxxxxx33333333388888'}}
        self.panel.handle_message(m, None)
        self.assertIn(tid, revoked)

    def test_revoke(self):
        tid = uuid()
        m = {'method': 'revoke',
             'destination': hostname,
             'arguments': {'task_id': tid}}
        self.panel.handle_message(m, None)
        self.assertIn(tid, revoked)

        m = {'method': 'revoke',
             'destination': 'does.not.exist',
             'arguments': {'task_id': tid + 'xxx'}}
        self.panel.handle_message(m, None)
        self.assertNotIn(tid + 'xxx', revoked)

    def test_revoke_terminate(self):
        request = Mock()
        request.id = tid = uuid()
        state = self.create_state()
        state.consumer = Mock()
        worker_state.task_reserved(request)
        try:
            r = control.revoke(state, tid, terminate=True)
            self.assertIn(tid, revoked)
            self.assertTrue(request.terminate.call_count)
            self.assertIn('terminate:', r['ok'])
            # unknown task id only revokes
            r = control.revoke(state, uuid(), terminate=True)
            self.assertIn('tasks unknown', r['ok'])
        finally:
            worker_state.task_ready(request)

    def test_ping(self):
        m = {'method': 'ping',
             'destination': hostname}
        r = self.panel.handle_message(m, None)
        self.assertEqual(r, {'ok': 'pong'})

    def test_shutdown(self):
        m = {'method': 'shutdown',
             'destination': hostname}
        with self.assertRaises(SystemExit):
            self.panel.handle_message(m, None)

    def test_panel_reply(self):

        replies = []

        class _Node(pidbox.Node):

            def reply(self, data, exchange, routing_key, **kwargs):
                replies.append(data)

        panel = _Node(hostname=hostname,
                      state=self.create_state(consumer=Consumer(self.app)),
                      handlers=control.Panel.data,
                      mailbox=self.app.control.mailbox)
        r = panel.dispatch('ping', reply_to={'exchange': 'x',
                                             'routing_key': 'x'})
        self.assertEqual(r, {'ok': 'pong'})
        self.assertDictEqual(replies[0], {panel.hostname: {'ok': 'pong'}})

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

        with self.assertRaises(ValueError):
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

    @patch('celery.worker.logger.debug')
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
            panel.handle('pool_restart', {'modules': ['foo', 'bar'],
                                          'reloader': _reload})

        consumer.controller.pool.restart.assert_called()
        consumer.reset_rate_limits.assert_called_with()
        consumer.update_strategies.assert_called_with()
        _reload.assert_not_called()
        _import.assert_has_calls([call('bar'), call('foo')], any_order=True)
        self.assertEqual(_import.call_count, 2)

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
            panel.handle('pool_restart', {'modules': ['foo'],
                                          'reload': False,
                                          'reloader': _reload})

            consumer.controller.pool.restart.assert_called()
            _reload.assert_not_called()
            _import.assert_not_called()

            _import.reset_mock()
            _reload.reset_mock()
            consumer.controller.pool.restart.reset_mock()

            panel.handle('pool_restart', {'modules': ['foo'],
                                          'reload': True,
                                          'reloader': _reload})

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
            TaskMessage(self.mytask.name, args=(2, 2)),
            app=self.app,
        )
        worker_state.task_reserved(req1)
        try:
            self.assertFalse(panel.handle('query_task', {'ids': {'1daa'}}))
            ret = panel.handle('query_task', {'ids': {req1.id}})
            self.assertIn(req1.id, ret)
            self.assertEqual(ret[req1.id][0], 'reserved')
            worker_state.active_requests.add(req1)
            try:
                ret = panel.handle('query_task', {'ids': {req1.id}})
                self.assertEqual(ret[req1.id][0], 'active')
            finally:
                worker_state.active_requests.clear()
            ret = panel.handle('query_task', {'ids': {req1.id}})
            self.assertEqual(ret[req1.id][0], 'reserved')
        finally:
            worker_state.reserved_requests.clear()

from __future__ import absolute_import
from __future__ import with_statement

import socket
import sys

from collections import deque
from datetime import datetime, timedelta
from Queue import Empty

from billiard.exceptions import WorkerLostError
from kombu import Connection
from kombu.exceptions import StdChannelError
from kombu.transport.base import Message
from mock import Mock, patch
from nose import SkipTest

from celery import current_app
from celery.app.defaults import DEFAULTS
from celery.concurrency.base import BasePool
from celery.datastructures import AttributeDict
from celery.exceptions import SystemTerminate
from celery.task import task as task_dec
from celery.task import periodic_task as periodic_task_dec
from celery.utils import uuid
from celery.worker import WorkController, Queues, Timers, EvLoop, Pool
from celery.worker.buckets import FastQueue, AsyncTaskBucket
from celery.worker.job import Request
from celery.worker.consumer import BlockingConsumer
from celery.worker.consumer import QoS, RUN, PREFETCH_COUNT_MAX, CLOSE
from celery.utils.serialization import pickle
from celery.utils.timer2 import Timer
from celery.utils.threads import Event

from celery.tests.utils import AppCase, Case


class PlaceHolder(object):
        pass


class MyKombuConsumer(BlockingConsumer):
    broadcast_consumer = Mock()
    task_consumer = Mock()

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('pool', BasePool(2))
        super(MyKombuConsumer, self).__init__(*args, **kwargs)

    def restart_heartbeat(self):
        self.heart = None


class MockNode(object):
    commands = []

    def handle_message(self, body, message):
        self.commands.append(body.pop('command', None))


class MockEventDispatcher(object):
    sent = []
    closed = False
    flushed = False
    _outbound_buffer = []

    def send(self, event, *args, **kwargs):
        self.sent.append(event)

    def close(self):
        self.closed = True

    def flush(self):
        self.flushed = True


class MockHeart(object):
    closed = False

    def stop(self):
        self.closed = True


@task_dec()
def foo_task(x, y, z, **kwargs):
    return x * y * z


@periodic_task_dec(run_every=60)
def foo_periodic_task():
    return 'foo'


def create_message(channel, **data):
    data.setdefault('id', uuid())
    channel.no_ack_consumers = set()
    return Message(channel, body=pickle.dumps(dict(**data)),
                   content_type='application/x-python-serialize',
                   content_encoding='binary',
                   delivery_info={'consumer_tag': 'mock'})


class test_QoS(Case):

    class _QoS(QoS):
        def __init__(self, value):
            self.value = value
            QoS.__init__(self, None, value)

        def set(self, value):
            return value

    def test_qos_increment_decrement(self):
        qos = self._QoS(10)
        self.assertEqual(qos.increment_eventually(), 11)
        self.assertEqual(qos.increment_eventually(3), 14)
        self.assertEqual(qos.increment_eventually(-30), 14)
        self.assertEqual(qos.decrement_eventually(7), 7)
        self.assertEqual(qos.decrement_eventually(), 6)

    def test_qos_disabled_increment_decrement(self):
        qos = self._QoS(0)
        self.assertEqual(qos.increment_eventually(), 0)
        self.assertEqual(qos.increment_eventually(3), 0)
        self.assertEqual(qos.increment_eventually(-30), 0)
        self.assertEqual(qos.decrement_eventually(7), 0)
        self.assertEqual(qos.decrement_eventually(), 0)
        self.assertEqual(qos.decrement_eventually(10), 0)

    def test_qos_thread_safe(self):
        qos = self._QoS(10)

        def add():
            for i in xrange(1000):
                qos.increment_eventually()

        def sub():
            for i in xrange(1000):
                qos.decrement_eventually()

        def threaded(funs):
            from threading import Thread
            threads = [Thread(target=fun) for fun in funs]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        threaded([add, add])
        self.assertEqual(qos.value, 2010)

        qos.value = 1000
        threaded([add, sub])  # n = 2
        self.assertEqual(qos.value, 1000)

    def test_exceeds_short(self):
        qos = QoS(Mock(), PREFETCH_COUNT_MAX - 1)
        qos.update()
        self.assertEqual(qos.value, PREFETCH_COUNT_MAX - 1)
        qos.increment_eventually()
        self.assertEqual(qos.value, PREFETCH_COUNT_MAX)
        qos.increment_eventually()
        self.assertEqual(qos.value, PREFETCH_COUNT_MAX + 1)
        qos.decrement_eventually()
        self.assertEqual(qos.value, PREFETCH_COUNT_MAX)
        qos.decrement_eventually()
        self.assertEqual(qos.value, PREFETCH_COUNT_MAX - 1)

    def test_consumer_increment_decrement(self):
        consumer = Mock()
        qos = QoS(consumer, 10)
        qos.update()
        self.assertEqual(qos.value, 10)
        consumer.qos.assert_called_with(prefetch_count=10)
        qos.decrement_eventually()
        qos.update()
        self.assertEqual(qos.value, 9)
        consumer.qos.assert_called_with(prefetch_count=9)
        qos.decrement_eventually()
        self.assertEqual(qos.value, 8)
        consumer.qos.assert_called_with(prefetch_count=9)
        self.assertIn({'prefetch_count': 9}, consumer.qos.call_args)

        # Does not decrement 0 value
        qos.value = 0
        qos.decrement_eventually()
        self.assertEqual(qos.value, 0)
        qos.increment_eventually()
        self.assertEqual(qos.value, 0)

    def test_consumer_decrement_eventually(self):
        consumer = Mock()
        qos = QoS(consumer, 10)
        qos.decrement_eventually()
        self.assertEqual(qos.value, 9)
        qos.value = 0
        qos.decrement_eventually()
        self.assertEqual(qos.value, 0)

    def test_set(self):
        consumer = Mock()
        qos = QoS(consumer, 10)
        qos.set(12)
        self.assertEqual(qos.prev, 12)
        qos.set(qos.prev)


class test_Consumer(Case):

    def setUp(self):
        self.ready_queue = FastQueue()
        self.timer = Timer()

    def tearDown(self):
        self.timer.stop()

    def test_info(self):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l.qos = QoS(l.task_consumer, 10)
        info = l.info
        self.assertEqual(info['prefetch_count'], 10)
        self.assertFalse(info['broker'])

        l.connection = current_app.connection()
        info = l.info
        self.assertTrue(info['broker'])

    def test_start_when_closed(self):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l._state = CLOSE
        l.start()

    def test_connection(self):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)

        l.reset_connection()
        self.assertIsInstance(l.connection, Connection)

        l._state = RUN
        l.event_dispatcher = None
        l.stop_consumers(close_connection=False)
        self.assertTrue(l.connection)

        l._state = RUN
        l.stop_consumers()
        self.assertIsNone(l.connection)
        self.assertIsNone(l.task_consumer)

        l.reset_connection()
        self.assertIsInstance(l.connection, Connection)
        l.stop_consumers()

        l.stop()
        l.close_connection()
        self.assertIsNone(l.connection)
        self.assertIsNone(l.task_consumer)

    def test_close_connection(self):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l._state = RUN
        l.close_connection()

        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        eventer = l.event_dispatcher = Mock()
        eventer.enabled = True
        heart = l.heart = MockHeart()
        l._state = RUN
        l.stop_consumers()
        self.assertTrue(eventer.close.call_count)
        self.assertTrue(heart.closed)

    @patch('celery.worker.consumer.warn')
    def test_receive_message_unknown(self, warn):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        backend = Mock()
        m = create_message(backend, unknown={'baz': '!!!'})
        l.event_dispatcher = Mock()
        l.pidbox_node = MockNode()

        l.receive_message(m.decode(), m)
        self.assertTrue(warn.call_count)

    @patch('celery.worker.consumer.to_timestamp')
    def test_receive_message_eta_OverflowError(self, to_timestamp):
        to_timestamp.side_effect = OverflowError()
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        m = create_message(Mock(), task=foo_task.name,
                           args=('2, 2'),
                           kwargs={},
                           eta=datetime.now().isoformat())
        l.event_dispatcher = Mock()
        l.pidbox_node = MockNode()
        l.update_strategies()
        l.qos = Mock()

        l.receive_message(m.decode(), m)
        self.assertTrue(to_timestamp.called)
        self.assertTrue(m.acknowledged)

    @patch('celery.worker.consumer.error')
    def test_receive_message_InvalidTaskError(self, error):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        m = create_message(Mock(), task=foo_task.name,
                           args=(1, 2), kwargs='foobarbaz', id=1)
        l.update_strategies()
        l.event_dispatcher = Mock()
        l.pidbox_node = MockNode()

        l.receive_message(m.decode(), m)
        self.assertIn('Received invalid task message', error.call_args[0][0])

    @patch('celery.worker.consumer.crit')
    def test_on_decode_error(self, crit):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)

        class MockMessage(Mock):
            content_type = 'application/x-msgpack'
            content_encoding = 'binary'
            body = 'foobarbaz'

        message = MockMessage()
        l.on_decode_error(message, KeyError('foo'))
        self.assertTrue(message.ack.call_count)
        self.assertIn("Can't decode message body", crit.call_args[0][0])

    def test_receieve_message(self):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        m = create_message(Mock(), task=foo_task.name,
                           args=[2, 4, 8], kwargs={})
        l.update_strategies()

        l.event_dispatcher = Mock()
        l.receive_message(m.decode(), m)

        in_bucket = self.ready_queue.get_nowait()
        self.assertIsInstance(in_bucket, Request)
        self.assertEqual(in_bucket.name, foo_task.name)
        self.assertEqual(in_bucket.execute(), 2 * 4 * 8)
        self.assertTrue(self.timer.empty())

    def test_start_connection_error(self):

        class MockConsumer(BlockingConsumer):
            iterations = 0

            def consume_messages(self):
                if not self.iterations:
                    self.iterations = 1
                    raise KeyError('foo')
                raise SyntaxError('bar')

        l = MockConsumer(self.ready_queue, timer=self.timer,
                         send_events=False, pool=BasePool())
        l.connection_errors = (KeyError, )
        with self.assertRaises(SyntaxError):
            l.start()
        l.heart.stop()
        l.timer.stop()

    def test_start_channel_error(self):
        # Regression test for AMQPChannelExceptions that can occur within the
        # consumer. (i.e. 404 errors)

        class MockConsumer(BlockingConsumer):
            iterations = 0

            def consume_messages(self):
                if not self.iterations:
                    self.iterations = 1
                    raise KeyError('foo')
                raise SyntaxError('bar')

        l = MockConsumer(self.ready_queue, timer=self.timer,
                         send_events=False, pool=BasePool())

        l.channel_errors = (KeyError, )
        self.assertRaises(SyntaxError, l.start)
        l.heart.stop()
        l.timer.stop()

    def test_consume_messages_ignores_socket_timeout(self):

        class Connection(current_app.connection().__class__):
            obj = None

            def drain_events(self, **kwargs):
                self.obj.connection = None
                raise socket.timeout(10)

        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l.connection = Connection()
        l.task_consumer = Mock()
        l.connection.obj = l
        l.qos = QoS(l.task_consumer, 10)
        l.consume_messages()

    def test_consume_messages_when_socket_error(self):

        class Connection(current_app.connection().__class__):
            obj = None

            def drain_events(self, **kwargs):
                self.obj.connection = None
                raise socket.error('foo')

        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l._state = RUN
        c = l.connection = Connection()
        l.connection.obj = l
        l.task_consumer = Mock()
        l.qos = QoS(l.task_consumer, 10)
        with self.assertRaises(socket.error):
            l.consume_messages()

        l._state = CLOSE
        l.connection = c
        l.consume_messages()

    def test_consume_messages(self):

        class Connection(current_app.connection().__class__):
            obj = None

            def drain_events(self, **kwargs):
                self.obj.connection = None

        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l.connection = Connection()
        l.connection.obj = l
        l.task_consumer = Mock()
        l.qos = QoS(l.task_consumer, 10)

        l.consume_messages()
        l.consume_messages()
        self.assertTrue(l.task_consumer.consume.call_count)
        l.task_consumer.qos.assert_called_with(prefetch_count=10)
        l.task_consumer.qos = Mock()
        self.assertEqual(l.qos.value, 10)
        l.qos.decrement_eventually()
        self.assertEqual(l.qos.value, 9)
        l.qos.update()
        self.assertEqual(l.qos.value, 9)
        l.task_consumer.qos.assert_called_with(prefetch_count=9)

    def test_maybe_conn_error(self):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l.connection_errors = (KeyError, )
        l.channel_errors = (SyntaxError, )
        l.maybe_conn_error(Mock(side_effect=AttributeError('foo')))
        l.maybe_conn_error(Mock(side_effect=KeyError('foo')))
        l.maybe_conn_error(Mock(side_effect=SyntaxError('foo')))
        with self.assertRaises(IndexError):
            l.maybe_conn_error(Mock(side_effect=IndexError('foo')))

    def test_apply_eta_task(self):
        from celery.worker import state
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l.qos = QoS(None, 10)

        task = object()
        qos = l.qos.value
        l.apply_eta_task(task)
        self.assertIn(task, state.reserved_requests)
        self.assertEqual(l.qos.value, qos - 1)
        self.assertIs(self.ready_queue.get_nowait(), task)

    def test_receieve_message_eta_isoformat(self):
        if sys.version_info < (2, 6):
            raise SkipTest('test broken on Python 2.5')
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        m = create_message(Mock(), task=foo_task.name,
                           eta=datetime.now().isoformat(),
                           args=[2, 4, 8], kwargs={})

        l.task_consumer = Mock()
        l.qos = QoS(l.task_consumer, l.initial_prefetch_count)
        current_pcount = l.qos.value
        l.event_dispatcher = Mock()
        l.enabled = False
        l.update_strategies()
        l.receive_message(m.decode(), m)
        l.timer.stop()
        l.timer.join(1)

        items = [entry[2] for entry in self.timer.queue]
        found = 0
        for item in items:
            if item.args[0].name == foo_task.name:
                found = True
        self.assertTrue(found)
        self.assertGreater(l.qos.value, current_pcount)
        l.timer.stop()

    def test_on_control(self):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l.pidbox_node = Mock()
        l.reset_pidbox_node = Mock()

        l.on_control('foo', 'bar')
        l.pidbox_node.handle_message.assert_called_with('foo', 'bar')

        l.pidbox_node = Mock()
        l.pidbox_node.handle_message.side_effect = KeyError('foo')
        l.on_control('foo', 'bar')
        l.pidbox_node.handle_message.assert_called_with('foo', 'bar')

        l.pidbox_node = Mock()
        l.pidbox_node.handle_message.side_effect = ValueError('foo')
        l.on_control('foo', 'bar')
        l.pidbox_node.handle_message.assert_called_with('foo', 'bar')
        l.reset_pidbox_node.assert_called_with()

    def test_revoke(self):
        ready_queue = FastQueue()
        l = MyKombuConsumer(ready_queue, timer=self.timer)
        backend = Mock()
        id = uuid()
        t = create_message(backend, task=foo_task.name, args=[2, 4, 8],
                           kwargs={}, id=id)
        from celery.worker.state import revoked
        revoked.add(id)

        l.receive_message(t.decode(), t)
        self.assertTrue(ready_queue.empty())

    def test_receieve_message_not_registered(self):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        backend = Mock()
        m = create_message(backend, task='x.X.31x', args=[2, 4, 8], kwargs={})

        l.event_dispatcher = Mock()
        self.assertFalse(l.receive_message(m.decode(), m))
        with self.assertRaises(Empty):
            self.ready_queue.get_nowait()
        self.assertTrue(self.timer.empty())

    @patch('celery.worker.consumer.warn')
    @patch('celery.worker.consumer.logger')
    def test_receieve_message_ack_raises(self, logger, warn):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        backend = Mock()
        m = create_message(backend, args=[2, 4, 8], kwargs={})

        l.event_dispatcher = Mock()
        l.connection_errors = (socket.error, )
        m.reject = Mock()
        m.reject.side_effect = socket.error('foo')
        self.assertFalse(l.receive_message(m.decode(), m))
        self.assertTrue(warn.call_count)
        with self.assertRaises(Empty):
            self.ready_queue.get_nowait()
        self.assertTrue(self.timer.empty())
        m.reject.assert_called_with()
        self.assertTrue(logger.critical.call_count)

    def test_receieve_message_eta(self):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l.event_dispatcher = Mock()
        l.event_dispatcher._outbound_buffer = deque()
        backend = Mock()
        m = create_message(
            backend, task=foo_task.name,
            args=[2, 4, 8], kwargs={},
            eta=(datetime.now() + timedelta(days=1)).isoformat(),
        )

        l.reset_connection()
        p = l.app.conf.BROKER_CONNECTION_RETRY
        l.app.conf.BROKER_CONNECTION_RETRY = False
        try:
            l.reset_connection()
        finally:
            l.app.conf.BROKER_CONNECTION_RETRY = p
        l.stop_consumers()
        l.event_dispatcher = Mock()
        l.receive_message(m.decode(), m)
        l.timer.stop()
        in_hold = l.timer.queue[0]
        self.assertEqual(len(in_hold), 3)
        eta, priority, entry = in_hold
        task = entry.args[0]
        self.assertIsInstance(task, Request)
        self.assertEqual(task.name, foo_task.name)
        self.assertEqual(task.execute(), 2 * 4 * 8)
        with self.assertRaises(Empty):
            self.ready_queue.get_nowait()

    def test_reset_pidbox_node(self):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l.pidbox_node = Mock()
        chan = l.pidbox_node.channel = Mock()
        l.connection = Mock()
        chan.close.side_effect = socket.error('foo')
        l.connection_errors = (socket.error, )
        l.reset_pidbox_node()
        chan.close.assert_called_with()

    def test_reset_pidbox_node_green(self):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l.pool = Mock()
        l.pool.is_green = True
        l.reset_pidbox_node()
        l.pool.spawn_n.assert_called_with(l._green_pidbox_node)

    def test__green_pidbox_node(self):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l.pidbox_node = Mock()

        class BConsumer(Mock):

            def __enter__(self):
                self.consume()
                return self

            def __exit__(self, *exc_info):
                self.cancel()

        l.pidbox_node.listen = BConsumer()
        connections = []

        class Connection(object):
            calls = 0

            def __init__(self, obj):
                connections.append(self)
                self.obj = obj
                self.default_channel = self.channel()
                self.closed = False

            def __enter__(self):
                return self

            def __exit__(self, *exc_info):
                self.close()

            def channel(self):
                return Mock()

            def as_uri(self):
                return 'dummy://'

            def drain_events(self, **kwargs):
                if not self.calls:
                    self.calls += 1
                    raise socket.timeout()
                self.obj.connection = None
                self.obj._pidbox_node_shutdown.set()

            def close(self):
                self.closed = True

        l.connection = Mock()
        l._open_connection = lambda: Connection(obj=l)
        l._green_pidbox_node()

        l.pidbox_node.listen.assert_called_with(callback=l.on_control)
        self.assertTrue(l.broadcast_consumer)
        l.broadcast_consumer.consume.assert_called_with()

        self.assertIsNone(l.connection)
        self.assertTrue(connections[0].closed)

    @patch('kombu.connection.Connection._establish_connection')
    @patch('kombu.utils.sleep')
    def test_open_connection_errback(self, sleep, connect):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        from kombu.transport.memory import Transport
        Transport.connection_errors = (StdChannelError, )

        def effect():
            if connect.call_count > 1:
                return
            raise StdChannelError()
        connect.side_effect = effect
        l._open_connection()
        connect.assert_called_with()

    def test_stop_pidbox_node(self):
        l = MyKombuConsumer(self.ready_queue, timer=self.timer)
        l._pidbox_node_stopped = Event()
        l._pidbox_node_shutdown = Event()
        l._pidbox_node_stopped.set()
        l.stop_pidbox_node()

    def test_start__consume_messages(self):

        class _QoS(object):
            prev = 3
            value = 4

            def update(self):
                self.prev = self.value

        class _Consumer(MyKombuConsumer):
            iterations = 0

            def reset_connection(self):
                if self.iterations >= 1:
                    raise KeyError('foo')

        init_callback = Mock()
        l = _Consumer(self.ready_queue, timer=self.timer,
                      init_callback=init_callback)
        l.task_consumer = Mock()
        l.broadcast_consumer = Mock()
        l.qos = _QoS()
        l.connection = Connection()
        l.iterations = 0

        def raises_KeyError(limit=None):
            l.iterations += 1
            if l.qos.prev != l.qos.value:
                l.qos.update()
            if l.iterations >= 2:
                raise KeyError('foo')

        l.consume_messages = raises_KeyError
        with self.assertRaises(KeyError):
            l.start()
        self.assertTrue(init_callback.call_count)
        self.assertEqual(l.iterations, 1)
        self.assertEqual(l.qos.prev, l.qos.value)

        init_callback.reset_mock()
        l = _Consumer(self.ready_queue, timer=self.timer,
                      send_events=False, init_callback=init_callback)
        l.qos = _QoS()
        l.task_consumer = Mock()
        l.broadcast_consumer = Mock()
        l.connection = Connection()
        l.consume_messages = Mock(side_effect=socket.error('foo'))
        with self.assertRaises(socket.error):
            l.start()
        self.assertTrue(init_callback.call_count)
        self.assertTrue(l.consume_messages.call_count)

    def test_reset_connection_with_no_node(self):
        l = BlockingConsumer(self.ready_queue, timer=self.timer)
        self.assertEqual(None, l.pool)
        l.reset_connection()

    def test_on_task_revoked(self):
        l = BlockingConsumer(self.ready_queue, timer=self.timer)
        task = Mock()
        task.revoked.return_value = True
        l.on_task(task)

    def test_on_task_no_events(self):
        l = BlockingConsumer(self.ready_queue, timer=self.timer)
        task = Mock()
        task.revoked.return_value = False
        l.event_dispatcher = Mock()
        l.event_dispatcher.enabled = False
        task.eta = None
        l._does_info = False
        l.on_task(task)


class test_WorkController(AppCase):

    def setup(self):
        self.worker = self.create_worker()
        from celery import worker
        self._logger = worker.logger
        self.logger = worker.logger = Mock()

    def teardown(self):
        from celery import worker
        worker.logger = self._logger

    def create_worker(self, **kw):
        worker = self.app.WorkController(concurrency=1, loglevel=0, **kw)
        worker._shutdown_complete.set()
        return worker

    @patch('celery.platforms.create_pidlock')
    def test_use_pidfile(self, create_pidlock):
        create_pidlock.return_value = Mock()
        worker = self.create_worker(pidfile='pidfilelockfilepid')
        worker.components = []
        worker.start()
        self.assertTrue(create_pidlock.called)
        worker.stop()
        self.assertTrue(worker.pidlock.release.called)

    @patch('celery.platforms.signals')
    @patch('celery.platforms.set_mp_process_title')
    def test_process_initializer(self, set_mp_process_title, _signals):
        from celery import Celery
        from celery import signals
        from celery._state import _tls
        from celery.concurrency.processes import process_initializer
        from celery.concurrency.processes import (WORKER_SIGRESET,
                                                  WORKER_SIGIGNORE)

        def on_worker_process_init(**kwargs):
            on_worker_process_init.called = True
        on_worker_process_init.called = False
        signals.worker_process_init.connect(on_worker_process_init)

        loader = Mock()
        loader.override_backends = {}
        app = Celery(loader=loader, set_as_current=False)
        app.loader = loader
        app.conf = AttributeDict(DEFAULTS)
        process_initializer(app, 'awesome.worker.com')
        _signals.ignore.assert_any_call(*WORKER_SIGIGNORE)
        _signals.reset.assert_any_call(*WORKER_SIGRESET)
        self.assertTrue(app.loader.init_worker.call_count)
        self.assertTrue(on_worker_process_init.called)
        self.assertIs(_tls.current_app, app)
        set_mp_process_title.assert_called_with(
            'celeryd', hostname='awesome.worker.com',
        )

    def test_with_rate_limits_disabled(self):
        worker = WorkController(concurrency=1, loglevel=0,
                                disable_rate_limits=True)
        self.assertTrue(hasattr(worker.ready_queue, 'put'))

    def test_attrs(self):
        worker = self.worker
        self.assertIsInstance(worker.timer, Timer)
        self.assertTrue(worker.timer)
        self.assertTrue(worker.pool)
        self.assertTrue(worker.consumer)
        self.assertTrue(worker.mediator)
        self.assertTrue(worker.components)

    def test_with_embedded_celerybeat(self):
        worker = WorkController(concurrency=1, loglevel=0, beat=True)
        self.assertTrue(worker.beat)
        self.assertIn(worker.beat, worker.components)

    def test_with_autoscaler(self):
        worker = self.create_worker(
            autoscale=[10, 3], send_events=False,
            timer_cls='celery.utils.timer2.Timer',
        )
        self.assertTrue(worker.autoscaler)

    def test_dont_stop_or_terminate(self):
        worker = WorkController(concurrency=1, loglevel=0)
        worker.stop()
        self.assertNotEqual(worker._state, worker.CLOSE)
        worker.terminate()
        self.assertNotEqual(worker._state, worker.CLOSE)

        sigsafe, worker.pool.signal_safe = worker.pool.signal_safe, False
        try:
            worker._state = worker.RUN
            worker.stop(in_sighandler=True)
            self.assertNotEqual(worker._state, worker.CLOSE)
            worker.terminate(in_sighandler=True)
            self.assertNotEqual(worker._state, worker.CLOSE)
        finally:
            worker.pool.signal_safe = sigsafe

    def test_on_timer_error(self):
        worker = WorkController(concurrency=1, loglevel=0)

        try:
            raise KeyError('foo')
        except KeyError, exc:
            Timers(worker).on_timer_error(exc)
            msg, args = self.logger.error.call_args[0]
            self.assertIn('KeyError', msg % args)

    def test_on_timer_tick(self):
        worker = WorkController(concurrency=1, loglevel=10)

        Timers(worker).on_timer_tick(30.0)
        xargs = self.logger.debug.call_args[0]
        fmt, arg = xargs[0], xargs[1]
        self.assertEqual(30.0, arg)
        self.assertIn('Next eta %s secs', fmt)

    def test_process_task(self):
        worker = self.worker
        worker.pool = Mock()
        backend = Mock()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = Request.from_message(m, m.decode())
        worker.process_task(task)
        self.assertEqual(worker.pool.apply_async.call_count, 1)
        worker.pool.stop()

    def test_process_task_raise_base(self):
        worker = self.worker
        worker.pool = Mock()
        worker.pool.apply_async.side_effect = KeyboardInterrupt('Ctrl+C')
        backend = Mock()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = Request.from_message(m, m.decode())
        worker.components = []
        worker._state = worker.RUN
        with self.assertRaises(KeyboardInterrupt):
            worker.process_task(task)
        self.assertEqual(worker._state, worker.TERMINATE)

    def test_process_task_raise_SystemTerminate(self):
        worker = self.worker
        worker.pool = Mock()
        worker.pool.apply_async.side_effect = SystemTerminate()
        backend = Mock()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = Request.from_message(m, m.decode())
        worker.components = []
        worker._state = worker.RUN
        with self.assertRaises(SystemExit):
            worker.process_task(task)
        self.assertEqual(worker._state, worker.TERMINATE)

    def test_process_task_raise_regular(self):
        worker = self.worker
        worker.pool = Mock()
        worker.pool.apply_async.side_effect = KeyError('some exception')
        backend = Mock()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = Request.from_message(m, m.decode())
        worker.process_task(task)
        worker.pool.stop()

    def test_start_catches_base_exceptions(self):
        worker1 = self.create_worker()
        stc = Mock()
        stc.start.side_effect = SystemTerminate()
        worker1.components = [stc]
        worker1.start()
        self.assertTrue(stc.terminate.call_count)

        worker2 = self.create_worker()
        sec = Mock()
        sec.start.side_effect = SystemExit()
        sec.terminate = None
        worker2.components = [sec]
        worker2.start()
        self.assertTrue(sec.stop.call_count)

    def test_state_db(self):
        from celery.worker import state
        Persistent = state.Persistent

        state.Persistent = Mock()
        try:
            worker = self.create_worker(state_db='statefilename')
            self.assertTrue(worker._persistence)
        finally:
            state.Persistent = Persistent

    def test_disable_rate_limits_solo(self):
        worker = self.create_worker(disable_rate_limits=True,
                                    pool_cls='solo')
        self.assertIsInstance(worker.ready_queue, FastQueue)
        self.assertIsNone(worker.mediator)
        self.assertEqual(worker.ready_queue.put, worker.process_task)

    def test_enable_rate_limits_eventloop(self):
        try:
            worker = self.create_worker(disable_rate_limits=False,
                                        use_eventloop=True,
                                        pool_cls='processes')
        except ImportError:
            raise SkipTest('multiprocessing not supported')
        self.assertIsInstance(worker.ready_queue, AsyncTaskBucket)
        # XXX disabled until 3.1
        #self.assertFalse(worker.mediator)
        #self.assertNotEqual(worker.ready_queue.put, worker.process_task)

    def test_disable_rate_limits_processes(self):
        raise SkipTest('disabled until v3.1')
        try:
            worker = self.create_worker(disable_rate_limits=True,
                                        use_eventloop=False,
                                        pool_cls='processes')
        except ImportError:
            raise SkipTest('multiprocessing not supported')
        self.assertIsInstance(worker.ready_queue, FastQueue)
        self.assertFalse(worker.mediator)
        self.assertEqual(worker.ready_queue.put, worker.process_task)

    def test_process_task_sem(self):
        worker = self.worker
        worker.semaphore = Mock()
        worker._quick_acquire = worker.semaphore.acquire

        req = Mock()
        worker.process_task_sem(req)
        worker.semaphore.acquire.assert_called_with(worker.process_task, req)

    def test_signal_consumer_close(self):
        worker = self.worker
        worker.consumer = Mock()

        worker.signal_consumer_close()
        worker.consumer.close.assert_called_with()

        worker.consumer.close.side_effect = AttributeError()
        worker.signal_consumer_close()

    def test_start__stop(self):
        worker = self.worker
        worker._shutdown_complete.set()
        worker.components = [Mock(), Mock(), Mock(), Mock()]

        worker.start()
        for w in worker.components:
            self.assertTrue(w.start.call_count)
        worker.stop()
        for component in worker.components:
            self.assertTrue(w.stop.call_count)

        # Doesn't close pool if no pool.
        worker.start()
        worker.pool = None
        worker.stop()

        # test that stop of None is not attempted
        worker.components[-1] = None
        worker.start()
        worker.stop()

    def test_component_raises(self):
        worker = self.worker
        comp = Mock()
        worker.components = [comp]
        comp.start.side_effect = TypeError()
        worker.stop = Mock()
        worker.start()
        worker.stop.assert_called_with()

    def test_state(self):
        self.assertTrue(self.worker.state)

    def test_start__terminate(self):
        worker = self.worker
        worker._shutdown_complete.set()
        worker.components = [Mock(), Mock(), Mock(), Mock(), Mock()]
        for component in worker.components[:3]:
            component.terminate = None

        worker.start()
        for w in worker.components[:3]:
            self.assertTrue(w.start.call_count)
        self.assertTrue(worker._running, len(worker.components))
        self.assertEqual(worker._state, RUN)
        worker.terminate()
        for component in worker.components[:3]:
            self.assertTrue(component.stop.call_count)
        self.assertTrue(worker.components[4].terminate.call_count)

    def test_Queues_pool_not_rlimit_safe(self):
        w = Mock()
        w.pool_cls.rlimit_safe = False
        Queues(w).create(w)
        self.assertTrue(w.disable_rate_limits)

    def test_Queues_pool_no_sem(self):
        raise SkipTest('disabled until v3.1')
        w = Mock()
        w.pool_cls.uses_semaphore = False
        Queues(w).create(w)
        self.assertIs(w.ready_queue.put, w.process_task)

    def test_EvLoop_crate(self):
        w = Mock()
        x = EvLoop(w)
        hub = x.create(w)
        self.assertTrue(w.timer.max_interval)
        self.assertIs(w.hub, hub)

    def test_Pool_crate_threaded(self):
        w = Mock()
        w._conninfo.connection_errors = w._conninfo.channel_errors = ()
        w.pool_cls = Mock()
        w.use_eventloop = False
        pool = Pool(w)
        pool.create(w)

    def test_Pool_create(self):
        from celery.worker.hub import BoundedSemaphore
        w = Mock()
        w._conninfo.connection_errors = w._conninfo.channel_errors = ()
        w.hub = Mock()
        w.hub.on_init = []
        w.pool_cls = Mock()
        P = w.pool_cls.return_value = Mock()
        P._cache = {}
        P.timers = {Mock(): 30}
        w.use_eventloop = True
        w.consumer.restart_count = -1
        pool = Pool(w)
        pool.create(w)
        self.assertIsInstance(w.semaphore, BoundedSemaphore)
        self.assertTrue(w.hub.on_init)

        hub = Mock()
        w.hub.on_init[0](hub)

        cbs = w.pool.init_callbacks.call_args[1]
        w = Mock()
        cbs['on_process_up'](w)
        hub.add_reader.assert_called_with(w.sentinel, P.maintain_pool)

        cbs['on_process_down'](w)
        hub.remove.assert_called_with(w.sentinel)

        w.pool._tref_for_id = {}

        result = Mock()

        cbs['on_timeout_cancel'](result)
        cbs['on_timeout_cancel'](result)  # no more tref

        with self.assertRaises(WorkerLostError):
            P.did_start_ok.return_value = False
            w.consumer.restart_count = 0
            pool.on_poll_init(P, w, hub)

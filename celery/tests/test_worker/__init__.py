from __future__ import absolute_import
from __future__ import with_statement

import socket
import sys

from collections import deque
from datetime import datetime, timedelta
from Queue import Empty

from kombu.transport.base import Message
from kombu.connection import BrokerConnection
from mock import Mock, patch

from celery import current_app
from celery.concurrency.base import BasePool
from celery.exceptions import SystemTerminate
from celery.task import task as task_dec
from celery.task import periodic_task as periodic_task_dec
from celery.utils import uuid
from celery.worker import WorkController
from celery.worker.buckets import FastQueue
from celery.worker.job import TaskRequest
from celery.worker.consumer import Consumer as MainConsumer
from celery.worker.consumer import QoS, RUN, PREFETCH_COUNT_MAX, CLOSE
from celery.utils.serialization import pickle
from celery.utils.timer2 import Timer

from celery.tests.compat import catch_warnings
from celery.tests.utils import unittest
from celery.tests.utils import AppCase


class PlaceHolder(object):
        pass


class MyKombuConsumer(MainConsumer):
    broadcast_consumer = Mock()
    task_consumer = Mock()

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("pool", BasePool(2))
        super(MyKombuConsumer, self).__init__(*args, **kwargs)

    def restart_heartbeat(self):
        self.heart = None


class MockNode(object):
    commands = []

    def handle_message(self, body, message):
        self.commands.append(body.pop("command", None))


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
    return "foo"


def create_message(channel, **data):
    data.setdefault("id", uuid())
    channel.no_ack_consumers = set()
    return Message(channel, body=pickle.dumps(dict(**data)),
                   content_type="application/x-python-serialize",
                   content_encoding="binary",
                   delivery_info={"consumer_tag": "mock"})


class test_QoS(unittest.TestCase):

    class _QoS(QoS):
        def __init__(self, value):
            self.value = value
            QoS.__init__(self, None, value, None)

        def set(self, value):
            return value

    def test_qos_increment_decrement(self):
        qos = self._QoS(10)
        self.assertEqual(qos.increment(), 11)
        self.assertEqual(qos.increment(3), 14)
        self.assertEqual(qos.increment(-30), 14)
        self.assertEqual(qos.decrement(7), 7)
        self.assertEqual(qos.decrement(), 6)
        with self.assertRaises(AssertionError):
            qos.decrement(10)

    def test_qos_disabled_increment_decrement(self):
        qos = self._QoS(0)
        self.assertEqual(qos.increment(), 0)
        self.assertEqual(qos.increment(3), 0)
        self.assertEqual(qos.increment(-30), 0)
        self.assertEqual(qos.decrement(7), 0)
        self.assertEqual(qos.decrement(), 0)
        self.assertEqual(qos.decrement(10), 0)

    def test_qos_thread_safe(self):
        qos = self._QoS(10)

        def add():
            for i in xrange(1000):
                qos.increment()

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
        qos = QoS(Mock(), PREFETCH_COUNT_MAX - 1,
                current_app.log.get_default_logger())
        qos.update()
        self.assertEqual(qos.value, PREFETCH_COUNT_MAX - 1)
        qos.increment()
        self.assertEqual(qos.value, PREFETCH_COUNT_MAX)
        qos.increment()
        self.assertEqual(qos.value, PREFETCH_COUNT_MAX + 1)
        qos.decrement()
        self.assertEqual(qos.value, PREFETCH_COUNT_MAX)
        qos.decrement()
        self.assertEqual(qos.value, PREFETCH_COUNT_MAX - 1)

    def test_consumer_increment_decrement(self):
        consumer = Mock()
        qos = QoS(consumer, 10, current_app.log.get_default_logger())
        qos.update()
        self.assertEqual(qos.value, 10)
        self.assertIn({"prefetch_count": 10}, consumer.qos.call_args)
        qos.decrement()
        self.assertEqual(qos.value, 9)
        self.assertIn({"prefetch_count": 9}, consumer.qos.call_args)
        qos.decrement_eventually()
        self.assertEqual(qos.value, 8)
        self.assertIn({"prefetch_count": 9}, consumer.qos.call_args)

        # Does not decrement 0 value
        qos.value = 0
        qos.decrement()
        self.assertEqual(qos.value, 0)
        qos.increment()
        self.assertEqual(qos.value, 0)

    def test_consumer_decrement_eventually(self):
        consumer = Mock()
        qos = QoS(consumer, 10, current_app.log.get_default_logger())
        qos.decrement_eventually()
        self.assertEqual(qos.value, 9)
        qos.value = 0
        qos.decrement_eventually()
        self.assertEqual(qos.value, 0)

    def test_set(self):
        consumer = Mock()
        qos = QoS(consumer, 10, current_app.log.get_default_logger())
        qos.set(12)
        self.assertEqual(qos.prev, 12)
        qos.set(qos.prev)


class test_Consumer(unittest.TestCase):

    def setUp(self):
        self.ready_queue = FastQueue()
        self.eta_schedule = Timer()
        self.logger = current_app.log.get_default_logger()
        self.logger.setLevel(0)

    def tearDown(self):
        self.eta_schedule.stop()

    def test_info(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        l.qos = QoS(l.task_consumer, 10, l.logger)
        info = l.info
        self.assertEqual(info["prefetch_count"], 10)
        self.assertFalse(info["broker"])

        l.connection = current_app.broker_connection()
        info = l.info
        self.assertTrue(info["broker"])

    def test_start_when_closed(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                            send_events=False)
        l._state = CLOSE
        l.start()

    def test_connection(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)

        l.reset_connection()
        self.assertIsInstance(l.connection, BrokerConnection)

        l._state = RUN
        l.event_dispatcher = None
        l.stop_consumers(close_connection=False)
        self.assertTrue(l.connection)

        l._state = RUN
        l.stop_consumers()
        self.assertIsNone(l.connection)
        self.assertIsNone(l.task_consumer)

        l.reset_connection()
        self.assertIsInstance(l.connection, BrokerConnection)
        l.stop_consumers()

        l.stop()
        l.close_connection()
        self.assertIsNone(l.connection)
        self.assertIsNone(l.task_consumer)

    def test_close_connection(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        l._state = RUN
        l.close_connection()

        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        eventer = l.event_dispatcher = Mock()
        eventer.enabled = True
        heart = l.heart = MockHeart()
        l._state = RUN
        l.stop_consumers()
        self.assertTrue(eventer.close.call_count)
        self.assertTrue(heart.closed)

    def test_receive_message_unknown(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        backend = Mock()
        m = create_message(backend, unknown={"baz": "!!!"})
        l.event_dispatcher = Mock()
        l.pidbox_node = MockNode()

        with catch_warnings(record=True) as log:
            l.receive_message(m.decode(), m)
            self.assertTrue(log)
            self.assertIn("unknown message", log[0].message.args[0])

    @patch("celery.utils.timer2.to_timestamp")
    def test_receive_message_eta_OverflowError(self, to_timestamp):
        to_timestamp.side_effect = OverflowError()
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                             send_events=False)
        m = create_message(Mock(), task=foo_task.name,
                                   args=("2, 2"),
                                   kwargs={},
                                   eta=datetime.now().isoformat())
        l.event_dispatcher = Mock()
        l.pidbox_node = MockNode()

        l.receive_message(m.decode(), m)
        self.assertTrue(m.acknowledged)
        self.assertTrue(to_timestamp.call_count)

    def test_receive_message_InvalidTaskError(self):
        logger = Mock()
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, logger,
                           send_events=False)
        m = create_message(Mock(), task=foo_task.name,
                           args=(1, 2), kwargs="foobarbaz", id=1)
        l.event_dispatcher = Mock()
        l.pidbox_node = MockNode()

        l.receive_message(m.decode(), m)
        self.assertIn("Received invalid task message",
                      logger.error.call_args[0][0])

    def test_on_decode_error(self):
        logger = Mock()
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, logger,
                           send_events=False)

        class MockMessage(Mock):
            content_type = "application/x-msgpack"
            content_encoding = "binary"
            body = "foobarbaz"

        message = MockMessage()
        l.on_decode_error(message, KeyError("foo"))
        self.assertTrue(message.ack.call_count)
        self.assertIn("Can't decode message body",
                      logger.critical.call_args[0][0])

    def test_receieve_message(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        m = create_message(Mock(), task=foo_task.name,
                           args=[2, 4, 8], kwargs={})

        l.event_dispatcher = Mock()
        l.receive_message(m.decode(), m)

        in_bucket = self.ready_queue.get_nowait()
        self.assertIsInstance(in_bucket, TaskRequest)
        self.assertEqual(in_bucket.task_name, foo_task.name)
        self.assertEqual(in_bucket.execute(), 2 * 4 * 8)
        self.assertTrue(self.eta_schedule.empty())

    def test_start_connection_error(self):

        class MockConsumer(MainConsumer):
            iterations = 0

            def consume_messages(self):
                if not self.iterations:
                    self.iterations = 1
                    raise KeyError("foo")
                raise SyntaxError("bar")

        l = MockConsumer(self.ready_queue, self.eta_schedule, self.logger,
                             send_events=False, pool=BasePool())
        l.connection_errors = (KeyError, )
        with self.assertRaises(SyntaxError):
            l.start()
        l.heart.stop()
        l.priority_timer.stop()

    def test_consume_messages_ignores_socket_timeout(self):

        class Connection(current_app.broker_connection().__class__):
            obj = None

            def drain_events(self, **kwargs):
                self.obj.connection = None
                raise socket.timeout(10)

        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                            send_events=False)
        l.connection = Connection()
        l.task_consumer = Mock()
        l.connection.obj = l
        l.qos = QoS(l.task_consumer, 10, l.logger)
        l.consume_messages()

    def test_consume_messages_when_socket_error(self):

        class Connection(current_app.broker_connection().__class__):
            obj = None

            def drain_events(self, **kwargs):
                self.obj.connection = None
                raise socket.error("foo")

        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                            send_events=False)
        l._state = RUN
        c = l.connection = Connection()
        l.connection.obj = l
        l.task_consumer = Mock()
        l.qos = QoS(l.task_consumer, 10, l.logger)
        with self.assertRaises(socket.error):
            l.consume_messages()

        l._state = CLOSE
        l.connection = c
        l.consume_messages()

    def test_consume_messages(self):

        class Connection(current_app.broker_connection().__class__):
            obj = None

            def drain_events(self, **kwargs):
                self.obj.connection = None

        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                             send_events=False)
        l.connection = Connection()
        l.connection.obj = l
        l.task_consumer = Mock()
        l.qos = QoS(l.task_consumer, 10, l.logger)

        l.consume_messages()
        l.consume_messages()
        self.assertTrue(l.task_consumer.consume.call_count)
        l.task_consumer.qos.assert_called_with(prefetch_count=10)
        l.qos.decrement()
        l.consume_messages()
        l.task_consumer.qos.assert_called_with(prefetch_count=9)

    def test_maybe_conn_error(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                             send_events=False)
        l.connection_errors = (KeyError, )
        l.channel_errors = (SyntaxError, )
        l.maybe_conn_error(Mock(side_effect=AttributeError("foo")))
        l.maybe_conn_error(Mock(side_effect=KeyError("foo")))
        l.maybe_conn_error(Mock(side_effect=SyntaxError("foo")))
        with self.assertRaises(IndexError):
            l.maybe_conn_error(Mock(side_effect=IndexError("foo")))

    def test_apply_eta_task(self):
        from celery.worker import state
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                             send_events=False)
        l.qos = QoS(None, 10, l.logger)

        task = object()
        qos = l.qos.value
        l.apply_eta_task(task)
        self.assertIn(task, state.reserved_requests)
        self.assertEqual(l.qos.value, qos - 1)
        self.assertIs(self.ready_queue.get_nowait(), task)

    def test_receieve_message_eta_isoformat(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                             send_events=False)
        m = create_message(Mock(), task=foo_task.name,
                           eta=datetime.now().isoformat(),
                           args=[2, 4, 8], kwargs={})

        l.task_consumer = Mock()
        l.qos = QoS(l.task_consumer, l.initial_prefetch_count, l.logger)
        l.event_dispatcher = Mock()
        l.enabled = False
        l.receive_message(m.decode(), m)
        l.eta_schedule.stop()

        items = [entry[2] for entry in self.eta_schedule.queue]
        found = 0
        for item in items:
            if item.args[0].task_name == foo_task.name:
                found = True
        self.assertTrue(found)
        self.assertTrue(l.task_consumer.qos.call_count)
        l.eta_schedule.stop()

    def test_on_control(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                             send_events=False)
        l.pidbox_node = Mock()
        l.reset_pidbox_node = Mock()

        l.on_control("foo", "bar")
        l.pidbox_node.handle_message.assert_called_with("foo", "bar")

        l.pidbox_node = Mock()
        l.pidbox_node.handle_message.side_effect = KeyError("foo")
        l.on_control("foo", "bar")
        l.pidbox_node.handle_message.assert_called_with("foo", "bar")

        l.pidbox_node = Mock()
        l.pidbox_node.handle_message.side_effect = ValueError("foo")
        l.on_control("foo", "bar")
        l.pidbox_node.handle_message.assert_called_with("foo", "bar")
        l.reset_pidbox_node.assert_called_with()

    def test_revoke(self):
        ready_queue = FastQueue()
        l = MyKombuConsumer(ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        backend = Mock()
        id = uuid()
        t = create_message(backend, task=foo_task.name, args=[2, 4, 8],
                           kwargs={}, id=id)
        from celery.worker.state import revoked
        revoked.add(id)

        l.receive_message(t.decode(), t)
        self.assertTrue(ready_queue.empty())

    def test_receieve_message_not_registered(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                          send_events=False)
        backend = Mock()
        m = create_message(backend, task="x.X.31x", args=[2, 4, 8], kwargs={})

        l.event_dispatcher = Mock()
        self.assertFalse(l.receive_message(m.decode(), m))
        with self.assertRaises(Empty):
            self.ready_queue.get_nowait()
        self.assertTrue(self.eta_schedule.empty())

    def test_receieve_message_ack_raises(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                          send_events=False)
        backend = Mock()
        m = create_message(backend, args=[2, 4, 8], kwargs={})

        l.event_dispatcher = Mock()
        l.connection_errors = (socket.error, )
        l.logger = Mock()
        m.ack = Mock()
        m.ack.side_effect = socket.error("foo")
        with catch_warnings(record=True) as log:
            self.assertFalse(l.receive_message(m.decode(), m))
            self.assertTrue(log)
            self.assertIn("unknown message", log[0].message.args[0])
        with self.assertRaises(Empty):
            self.ready_queue.get_nowait()
        self.assertTrue(self.eta_schedule.empty())
        m.ack.assert_called_with()
        self.assertTrue(l.logger.critical.call_count)

    def test_receieve_message_eta(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                          send_events=False)
        l.event_dispatcher = Mock()
        l.event_dispatcher._outbound_buffer = deque()
        backend = Mock()
        m = create_message(backend, task=foo_task.name,
                           args=[2, 4, 8], kwargs={},
                           eta=(datetime.now() +
                               timedelta(days=1)).isoformat())

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
        l.eta_schedule.stop()
        in_hold = self.eta_schedule.queue[0]
        self.assertEqual(len(in_hold), 3)
        eta, priority, entry = in_hold
        task = entry.args[0]
        self.assertIsInstance(task, TaskRequest)
        self.assertEqual(task.task_name, foo_task.name)
        self.assertEqual(task.execute(), 2 * 4 * 8)
        with self.assertRaises(Empty):
            self.ready_queue.get_nowait()

    def test_reset_pidbox_node(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                          send_events=False)
        l.pidbox_node = Mock()
        chan = l.pidbox_node.channel = Mock()
        l.connection = Mock()
        chan.close.side_effect = socket.error("foo")
        l.connection_errors = (socket.error, )
        l.reset_pidbox_node()
        chan.close.assert_called_with()

    def test_reset_pidbox_node_green(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                          send_events=False)
        l.pool = Mock()
        l.pool.is_green = True
        l.reset_pidbox_node()
        l.pool.spawn_n.assert_called_with(l._green_pidbox_node)

    def test__green_pidbox_node(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                          send_events=False)
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

            def drain_events(self, **kwargs):
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
                    raise KeyError("foo")

        init_callback = Mock()
        l = _Consumer(self.ready_queue, self.eta_schedule, self.logger,
                      send_events=False, init_callback=init_callback)
        l.task_consumer = Mock()
        l.broadcast_consumer = Mock()
        l.qos = _QoS()
        l.connection = BrokerConnection()
        l.iterations = 0

        def raises_KeyError(limit=None):
            l.iterations += 1
            if l.qos.prev != l.qos.value:
                l.qos.update()
            if l.iterations >= 2:
                raise KeyError("foo")

        l.consume_messages = raises_KeyError
        with self.assertRaises(KeyError):
            l.start()
        self.assertTrue(init_callback.call_count)
        self.assertEqual(l.iterations, 1)
        self.assertEqual(l.qos.prev, l.qos.value)

        init_callback.reset_mock()
        l = _Consumer(self.ready_queue, self.eta_schedule, self.logger,
                      send_events=False, init_callback=init_callback)
        l.qos = _QoS()
        l.task_consumer = Mock()
        l.broadcast_consumer = Mock()
        l.connection = BrokerConnection()
        l.consume_messages = Mock(side_effect=socket.error("foo"))
        with self.assertRaises(socket.error):
            l.start()
        self.assertTrue(init_callback.call_count)
        self.assertTrue(l.consume_messages.call_count)

    def test_reset_connection_with_no_node(self):

        l = MainConsumer(self.ready_queue, self.eta_schedule, self.logger)
        self.assertEqual(None, l.pool)
        l.reset_connection()


class test_WorkController(AppCase):

    def setup(self):
        self.worker = self.create_worker()

    def create_worker(self, **kw):
        worker = WorkController(concurrency=1, loglevel=0, **kw)
        worker._shutdown_complete.set()
        worker.logger = Mock()
        return worker

    @patch("celery.platforms.signals")
    @patch("celery.platforms.set_mp_process_title")
    def test_process_initializer(self, set_mp_process_title, _signals):
        from celery import Celery
        from celery import signals
        from celery.app import _tls
        from celery.worker import process_initializer
        from celery.worker import WORKER_SIGRESET, WORKER_SIGIGNORE

        def on_worker_process_init(**kwargs):
            on_worker_process_init.called = True
        on_worker_process_init.called = False
        signals.worker_process_init.connect(on_worker_process_init)

        app = Celery(loader=Mock(), set_as_current=False)
        process_initializer(app, "awesome.worker.com")
        self.assertIn((tuple(WORKER_SIGIGNORE), {}),
                      _signals.ignore.call_args_list)
        self.assertIn((tuple(WORKER_SIGRESET), {}),
                      _signals.reset.call_args_list)
        self.assertTrue(app.loader.init_worker.call_count)
        self.assertTrue(on_worker_process_init.called)
        self.assertIs(_tls.current_app, app)
        set_mp_process_title.assert_called_with("celeryd",
                        hostname="awesome.worker.com")

    def test_with_rate_limits_disabled(self):
        worker = WorkController(concurrency=1, loglevel=0,
                                disable_rate_limits=True)
        self.assertTrue(hasattr(worker.ready_queue, "put"))

    def test_attrs(self):
        worker = self.worker
        self.assertIsInstance(worker.scheduler, Timer)
        self.assertTrue(worker.scheduler)
        self.assertTrue(worker.pool)
        self.assertTrue(worker.consumer)
        self.assertTrue(worker.mediator)
        self.assertTrue(worker.components)

    def test_with_embedded_celerybeat(self):
        worker = WorkController(concurrency=1, loglevel=0,
                                embed_clockservice=True)
        self.assertTrue(worker.beat)
        self.assertIn(worker.beat, worker.components)

    def test_with_autoscaler(self):
        worker = self.create_worker(autoscale=[10, 3], send_events=False,
                                eta_scheduler_cls="celery.utils.timer2.Timer")
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
        worker.logger = Mock()

        try:
            raise KeyError("foo")
        except KeyError:
            exc_info = sys.exc_info()

        worker.on_timer_error(exc_info)
        msg, args = worker.logger.error.call_args[0]
        self.assertIn("KeyError", msg % args)

    def test_on_timer_tick(self):
        worker = WorkController(concurrency=1, loglevel=10)
        worker.logger = Mock()
        worker.timer_debug = worker.logger.debug

        worker.on_timer_tick(30.0)
        logged = worker.logger.debug.call_args[0][0]
        self.assertIn("30.0", logged)

    def test_process_task(self):
        worker = self.worker
        worker.pool = Mock()
        backend = Mock()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = TaskRequest.from_message(m, m.decode())
        worker.process_task(task)
        self.assertEqual(worker.pool.apply_async.call_count, 1)
        worker.pool.stop()

    def test_process_task_raise_base(self):
        worker = self.worker
        worker.pool = Mock()
        worker.pool.apply_async.side_effect = KeyboardInterrupt("Ctrl+C")
        backend = Mock()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = TaskRequest.from_message(m, m.decode())
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
        task = TaskRequest.from_message(m, m.decode())
        worker.components = []
        worker._state = worker.RUN
        with self.assertRaises(SystemExit):
            worker.process_task(task)
        self.assertEqual(worker._state, worker.TERMINATE)

    def test_process_task_raise_regular(self):
        worker = self.worker
        worker.pool = Mock()
        worker.pool.apply_async.side_effect = KeyError("some exception")
        backend = Mock()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = TaskRequest.from_message(m, m.decode())
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
            worker = self.create_worker(db="statefilename")
            self.assertTrue(worker._persistence)
        finally:
            state.Persistent = Persistent

    def test_disable_rate_limits(self):
        from celery.worker.buckets import FastQueue
        worker = self.create_worker(disable_rate_limits=True)
        self.assertIsInstance(worker.ready_queue, FastQueue)
        self.assertIsNone(worker.mediator)
        self.assertEqual(worker.ready_queue.put, worker.process_task)

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

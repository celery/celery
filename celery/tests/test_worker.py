import socket
import unittest2 as unittest

from datetime import datetime, timedelta
from Queue import Empty

from kombu.transport.base import Message
from kombu.connection import BrokerConnection
from celery.utils.timer2 import Timer

from celery.app import app_or_default
from celery.concurrency.base import BasePool
from celery.decorators import task as task_dec
from celery.decorators import periodic_task as periodic_task_dec
from celery.serialization import pickle
from celery.utils import gen_unique_id
from celery.worker import WorkController
from celery.worker.buckets import FastQueue
from celery.worker.job import TaskRequest
from celery.worker.consumer import Consumer as MainConsumer
from celery.worker.consumer import QoS, RUN

from celery.tests.compat import catch_warnings
from celery.tests.utils import execute_context


class MockConsumer(object):

    class Channel(object):

        def close(self):
            pass

    def register_callback(self, cb):
        pass

    def consume(self):
        pass

    @property
    def channel(self):
        return self.Channel()


class PlaceHolder(object):
        pass


class MyKombuConsumer(MainConsumer):
    broadcast_consumer = MockConsumer()
    task_consumer = MockConsumer()

    def restart_heartbeat(self):
        self.heart = None


class MockNode(object):
    commands = []

    def handle_message(self, message_data, message):
        self.commands.append(message.pop("command", None))


class MockEventDispatcher(object):
    sent = []
    closed = False
    flushed = False

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


class MockLogger(object):

    def __init__(self):
        self.logged = []

    def critical(self, msg, *args, **kwargs):
        self.logged.append(msg)

    def info(self, msg, *args, **kwargs):
        self.logged.append(msg)

    def error(self, msg, *args, **kwargs):
        self.logged.append(msg)

    def debug(self, msg, *args, **kwargs):
        self.logged.append(msg)


class MockBackend(object):
    _acked = False

    def basic_ack(self, delivery_tag):
        self._acked = True


class MockPool(BasePool):
    _terminated = False
    _stopped = False

    def __init__(self, *args, **kwargs):
        self.raise_regular = kwargs.get("raise_regular", False)
        self.raise_base = kwargs.get("raise_base", False)

    def apply_async(self, *args, **kwargs):
        if self.raise_regular:
            raise KeyError("some exception")
        if self.raise_base:
            raise KeyboardInterrupt("Ctrl+c")

    def start(self):
        pass

    def stop(self):
        self._stopped = True
        return True

    def terminate(self):
        self._terminated = True
        self.stop()


class MockController(object):

    def __init__(self, w, *args, **kwargs):
        self._w = w
        self._stopped = False

    def start(self):
        self._w["started"] = True
        self._stopped = False

    def stop(self):
        self._stopped = True


def create_message(backend, **data):
    data.setdefault("id", gen_unique_id())
    return Message(backend, body=pickle.dumps(dict(**data)),
                   content_type="application/x-python-serialize",
                   content_encoding="binary")


class test_QoS(unittest.TestCase):

    class MockConsumer(object):
        prefetch_count = 0

        def qos(self, prefetch_size=0, prefetch_count=0, apply_global=False):
            self.prefetch_count = prefetch_count

    def test_decrement(self):
        consumer = self.MockConsumer()
        qos = QoS(consumer, 10, app_or_default().log.get_default_logger())
        qos.update()
        self.assertEqual(int(qos.value), 10)
        self.assertEqual(consumer.prefetch_count, 10)
        qos.decrement()
        self.assertEqual(int(qos.value), 9)
        self.assertEqual(consumer.prefetch_count, 9)
        qos.decrement_eventually()
        self.assertEqual(int(qos.value), 8)
        self.assertEqual(consumer.prefetch_count, 9)


class test_Consumer(unittest.TestCase):

    def setUp(self):
        self.ready_queue = FastQueue()
        self.eta_schedule = Timer()
        self.logger = app_or_default().log.get_default_logger()
        self.logger.setLevel(0)

    def tearDown(self):
        self.eta_schedule.stop()

    def test_connection(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)

        l.reset_connection()
        self.assertIsInstance(l.connection, BrokerConnection)

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
        eventer = l.event_dispatcher = MockEventDispatcher()
        heart = l.heart = MockHeart()
        l._state = RUN
        l.stop_consumers()
        self.assertTrue(eventer.closed)
        self.assertTrue(heart.closed)

    def test_receive_message_unknown(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        backend = MockBackend()
        m = create_message(backend, unknown={"baz": "!!!"})
        l.event_dispatcher = MockEventDispatcher()
        l.pidbox_node = MockNode()

        def with_catch_warnings(log):
            l.receive_message(m.decode(), m)
            self.assertTrue(log)
            self.assertIn("unknown message", log[0].message.args[0])

        context = catch_warnings(record=True)
        execute_context(context, with_catch_warnings)

    def test_receive_message_eta_OverflowError(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                             send_events=False)
        backend = MockBackend()
        called = [False]

        def to_timestamp(d):
            called[0] = True
            raise OverflowError()

        m = create_message(backend, task=foo_task.name,
                                    args=("2, 2"),
                                    kwargs={},
                                    eta=datetime.now().isoformat())
        l.event_dispatcher = MockEventDispatcher()
        l.pidbox_node = MockNode()

        from celery.worker import consumer
        prev, consumer.to_timestamp = consumer.to_timestamp, to_timestamp
        try:
            l.receive_message(m.decode(), m)
            self.assertTrue(m.acknowledged)
            self.assertTrue(called[0])
        finally:
            consumer.to_timestamp = prev

    def test_receive_message_InvalidTaskError(self):
        logger = MockLogger()
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, logger,
                           send_events=False)
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name,
            args=(1, 2), kwargs="foobarbaz", id=1)
        l.event_dispatcher = MockEventDispatcher()
        l.pidbox_node = MockNode()

        l.receive_message(m.decode(), m)
        self.assertIn("Invalid task ignored", logger.logged[0])

    def test_on_decode_error(self):
        logger = MockLogger()
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, logger,
                           send_events=False)

        class MockMessage(object):
            content_type = "application/x-msgpack"
            content_encoding = "binary"
            body = "foobarbaz"
            acked = False

            def ack(self):
                self.acked = True

        message = MockMessage()
        l.on_decode_error(message, KeyError("foo"))
        self.assertTrue(message.acked)
        self.assertIn("Message decoding error", logger.logged[0])

    def test_receieve_message(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name,
                           args=[2, 4, 8], kwargs={})

        l.event_dispatcher = MockEventDispatcher()
        l.receive_message(m.decode(), m)

        in_bucket = self.ready_queue.get_nowait()
        self.assertIsInstance(in_bucket, TaskRequest)
        self.assertEqual(in_bucket.task_name, foo_task.name)
        self.assertEqual(in_bucket.execute(), 2 * 4 * 8)
        self.assertTrue(self.eta_schedule.empty())

    def test_receieve_message_eta_isoformat(self):

        class MockConsumer(object):
            prefetch_count_incremented = False

            def qos(self, **kwargs):
                self.prefetch_count_incremented = True

        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                             send_events=False)
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name,
                           eta=datetime.now().isoformat(),
                           args=[2, 4, 8], kwargs={})

        l.task_consumer = MockConsumer()
        l.qos = QoS(l.task_consumer, l.initial_prefetch_count, l.logger)
        l.event_dispatcher = MockEventDispatcher()
        l.receive_message(m.decode(), m)
        l.eta_schedule.stop()

        items = [entry[2] for entry in self.eta_schedule.queue]
        found = 0
        for item in items:
            if item.args[0].task_name == foo_task.name:
                found = True
        self.assertTrue(found)
        self.assertTrue(l.task_consumer.prefetch_count_incremented)
        l.eta_schedule.stop()

    def test_revoke(self):
        ready_queue = FastQueue()
        l = MyKombuConsumer(ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        backend = MockBackend()
        id = gen_unique_id()
        t = create_message(backend, task=foo_task.name, args=[2, 4, 8],
                           kwargs={}, id=id)
        from celery.worker.state import revoked
        revoked.add(id)

        l.receive_message(t.decode(), t)
        self.assertTrue(ready_queue.empty())

    def test_receieve_message_not_registered(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                          send_events=False)
        backend = MockBackend()
        m = create_message(backend, task="x.X.31x", args=[2, 4, 8], kwargs={})

        l.event_dispatcher = MockEventDispatcher()
        self.assertFalse(l.receive_message(m.decode(), m))
        self.assertRaises(Empty, self.ready_queue.get_nowait)
        self.assertTrue(self.eta_schedule.empty())

    def test_receieve_message_eta(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                          send_events=False)
        dispatcher = l.event_dispatcher = MockEventDispatcher()
        backend = MockBackend()
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
        self.assertTrue(dispatcher.flushed)
        l.event_dispatcher = MockEventDispatcher()
        l.receive_message(m.decode(), m)
        l.eta_schedule.stop()
        in_hold = self.eta_schedule.queue[0]
        self.assertEqual(len(in_hold), 3)
        eta, priority, entry = in_hold
        task = entry.args[0]
        self.assertIsInstance(task, TaskRequest)
        self.assertEqual(task.task_name, foo_task.name)
        self.assertEqual(task.execute(), 2 * 4 * 8)
        self.assertRaises(Empty, self.ready_queue.get_nowait)

    def test_start__consume_messages(self):

        class _QoS(object):
            prev = 3
            next = 4

            def update(self):
                self.prev = self.next

        class _Consumer(MyKombuConsumer):
            iterations = 0
            wait_method = None

            def reset_connection(self):
                if self.iterations >= 1:
                    raise KeyError("foo")

        called_back = [False]

        def init_callback(consumer):
            called_back[0] = True

        l = _Consumer(self.ready_queue, self.eta_schedule, self.logger,
                      send_events=False, init_callback=init_callback)
        l.task_consumer = MockConsumer()
        l.broadcast_consumer = MockConsumer()
        l.qos = _QoS()
        l.connection = BrokerConnection()
        l.iterations = 0

        def raises_KeyError(limit=None):
            l.iterations += 1
            if l.qos.prev != l.qos.next:
                l.qos.update()
            if l.iterations >= 2:
                raise KeyError("foo")

        l.consume_messages = raises_KeyError
        self.assertRaises(KeyError, l.start)
        self.assertTrue(called_back[0])
        self.assertEqual(l.iterations, 1)
        self.assertEqual(l.qos.prev, l.qos.next)

        l = _Consumer(self.ready_queue, self.eta_schedule, self.logger,
                      send_events=False, init_callback=init_callback)
        l.qos = _QoS()
        l.task_consumer = MockConsumer()
        l.broadcast_consumer = MockConsumer()
        l.connection = BrokerConnection()

        def raises_socket_error(limit=None):
            l.iterations = 1
            raise socket.error("foo")

        l.consume_messages = raises_socket_error
        self.assertRaises(socket.error, l.start)
        self.assertTrue(called_back[0])
        self.assertEqual(l.iterations, 1)


class test_WorkController(unittest.TestCase):

    def setUp(self):
        self.worker = WorkController(concurrency=1, loglevel=0)
        self.worker.logger = MockLogger()

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

    def test_process_task(self):
        worker = self.worker
        worker.pool = MockPool()
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = TaskRequest.from_message(m, m.decode())
        worker.process_task(task)
        worker.pool.stop()

    def test_process_task_raise_base(self):
        worker = self.worker
        worker.pool = MockPool(raise_base=True)
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = TaskRequest.from_message(m, m.decode())
        worker.components = []
        worker._state = worker.RUN
        self.assertRaises(KeyboardInterrupt, worker.process_task, task)
        self.assertEqual(worker._state, worker.TERMINATE)

    def test_process_task_raise_regular(self):
        worker = self.worker
        worker.pool = MockPool(raise_regular=True)
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = TaskRequest.from_message(m, m.decode())
        worker.process_task(task)
        worker.pool.stop()

    def test_start__stop(self):
        worker = self.worker
        w1 = {"started": False}
        w2 = {"started": False}
        w3 = {"started": False}
        w4 = {"started": False}
        worker.components = [MockController(w1), MockController(w2),
                             MockController(w3), MockController(w4)]

        worker.start()
        for w in (w1, w2, w3, w4):
            self.assertTrue(w["started"])
        self.assertTrue(worker._running, len(worker.components))
        worker.stop()
        for component in worker.components:
            self.assertTrue(component._stopped)

    def test_start__terminate(self):
        worker = self.worker
        w1 = {"started": False}
        w2 = {"started": False}
        w3 = {"started": False}
        w4 = {"started": False}
        worker.components = [MockController(w1), MockController(w2),
                             MockController(w3), MockController(w4),
                             MockPool()]

        worker.start()
        for w in (w1, w2, w3, w4):
            self.assertTrue(w["started"])
        self.assertTrue(worker._running, len(worker.components))
        self.assertEqual(worker._state, RUN)
        worker.terminate()
        for component in worker.components:
            self.assertTrue(component._stopped)
        self.assertTrue(worker.components[4]._terminated)

import socket
import unittest2 as unittest

from datetime import datetime, timedelta
from multiprocessing import get_logger
from Queue import Empty

from carrot.backends.base import BaseMessage
from carrot.connection import BrokerConnection
from celery.utils.timer2 import Timer

from celery.decorators import task as task_dec
from celery.decorators import periodic_task as periodic_task_dec
from celery.serialization import pickle
from celery.utils import gen_unique_id
from celery.worker import WorkController
from celery.worker.buckets import FastQueue
from celery.worker.job import TaskRequest
from celery.worker import listener
from celery.worker.listener import CarrotListener, QoS, RUN

from celery.tests.compat import catch_warnings
from celery.tests.utils import execute_context


class PlaceHolder(object):
        pass


class MyCarrotListener(CarrotListener):

    def restart_heartbeat(self):
        self.heart = None


class MockControlDispatch(object):
    commands = []

    def dispatch_from_message(self, message):
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

    def ack(self, delivery_tag):
        self._acked = True


class MockPool(object):
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
    return BaseMessage(backend, body=pickle.dumps(dict(**data)),
                       content_type="application/x-python-serialize",
                       content_encoding="binary")


class test_QoS(unittest.TestCase):

    class MockConsumer(object):
        prefetch_count = 0

        def qos(self, prefetch_size=0, prefetch_count=0, apply_global=False):
            self.prefetch_count = prefetch_count

    def test_decrement(self):
        consumer = self.MockConsumer()
        qos = QoS(consumer, 10, get_logger())
        qos.update()
        self.assertEqual(int(qos.value), 10)
        self.assertEqual(consumer.prefetch_count, 10)
        qos.decrement()
        self.assertEqual(int(qos.value), 9)
        self.assertEqual(consumer.prefetch_count, 9)
        qos.decrement_eventually()
        self.assertEqual(int(qos.value), 8)
        self.assertEqual(consumer.prefetch_count, 9)


class test_CarrotListener(unittest.TestCase):

    def setUp(self):
        self.ready_queue = FastQueue()
        self.eta_schedule = Timer()
        self.logger = get_logger()
        self.logger.setLevel(0)

    def tearDown(self):
        self.eta_schedule.stop()

    def test_mainloop(self):
        l = MyCarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)

        class MockConnection(object):

            def drain_events(self):
                return "draining"

        l.connection = MockConnection()
        l.connection.connection = MockConnection()

        it = l._mainloop()
        self.assertTrue(it.next(), "draining")
        records = {}

        def create_recorder(key):
            def _recorder(*args, **kwargs):
                records[key] = True
            return _recorder

        l.task_consumer = PlaceHolder()
        l.task_consumer.iterconsume = create_recorder("consume_tasks")
        l.broadcast_consumer = PlaceHolder()
        l.broadcast_consumer.register_callback = create_recorder(
                                                    "broadcast_callback")
        l.broadcast_consumer.iterconsume = create_recorder(
                                             "consume_broadcast")
        l.task_consumer.add_consumer = create_recorder("consumer_add")

        records.clear()
        self.assertEqual(l._detect_wait_method(), l._mainloop)
        for record in ("broadcast_callback", "consume_broadcast",
                "consume_tasks"):
            self.assertTrue(records.get(record))

        records.clear()
        l.connection.connection = PlaceHolder()
        self.assertIs(l._detect_wait_method(), l.task_consumer.iterconsume)
        self.assertTrue(records.get("consumer_add"))

    def test_connection(self):
        l = MyCarrotListener(self.ready_queue, self.eta_schedule, self.logger,
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

    def test_receive_message_control_command(self):
        l = MyCarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        backend = MockBackend()
        m = create_message(backend, control={"command": "shutdown"})
        l.event_dispatcher = MockEventDispatcher()
        l.control_dispatch = MockControlDispatch()
        l.receive_message(m.decode(), m)
        self.assertIn("shutdown", l.control_dispatch.commands)

    def test_close_connection(self):
        l = MyCarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        l._state = RUN
        l.close_connection()

        l = MyCarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        eventer = l.event_dispatcher = MockEventDispatcher()
        heart = l.heart = MockHeart()
        l._state = RUN
        l.stop_consumers()
        self.assertTrue(eventer.closed)
        self.assertTrue(heart.closed)

    def test_receive_message_unknown(self):
        l = MyCarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        backend = MockBackend()
        m = create_message(backend, unknown={"baz": "!!!"})
        l.event_dispatcher = MockEventDispatcher()
        l.control_dispatch = MockControlDispatch()

        def with_catch_warnings(log):
            l.receive_message(m.decode(), m)
            self.assertTrue(log)
            self.assertIn("unknown message", log[0].message.args[0])

        context = catch_warnings(record=True)
        execute_context(context, with_catch_warnings)

    def test_receive_message_eta_OverflowError(self):
        l = MyCarrotListener(self.ready_queue, self.eta_schedule, self.logger,
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
        l.control_dispatch = MockControlDispatch()

        prev, listener.to_timestamp = listener.to_timestamp, to_timestamp
        try:
            l.receive_message(m.decode(), m)
            self.assertTrue(m.acknowledged)
            self.assertTrue(called[0])
        finally:
            listener.to_timestamp = prev

    def test_receive_message_InvalidTaskError(self):
        logger = MockLogger()
        l = MyCarrotListener(self.ready_queue, self.eta_schedule, logger,
                           send_events=False)
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name,
            args=(1, 2), kwargs="foobarbaz", id=1)
        l.event_dispatcher = MockEventDispatcher()
        l.control_dispatch = MockControlDispatch()

        l.receive_message(m.decode(), m)
        self.assertIn("Invalid task ignored", logger.logged[0])

    def test_on_decode_error(self):
        logger = MockLogger()
        l = MyCarrotListener(self.ready_queue, self.eta_schedule, logger,
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
        l = MyCarrotListener(self.ready_queue, self.eta_schedule, self.logger,
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

        l = MyCarrotListener(self.ready_queue, self.eta_schedule, self.logger,
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
        l = MyCarrotListener(ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        backend = MockBackend()
        id = gen_unique_id()
        c = create_message(backend, control={"command": "revoke",
                                             "task_id": id})
        t = create_message(backend, task=foo_task.name, args=[2, 4, 8],
                           kwargs={}, id=id)
        l.event_dispatcher = MockEventDispatcher()
        l.receive_message(c.decode(), c)
        from celery.worker.state import revoked
        self.assertIn(id, revoked)

        l.receive_message(t.decode(), t)
        self.assertTrue(ready_queue.empty())

    def test_receieve_message_not_registered(self):
        l = MyCarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                          send_events=False)
        backend = MockBackend()
        m = create_message(backend, task="x.X.31x", args=[2, 4, 8], kwargs={})

        l.event_dispatcher = MockEventDispatcher()
        self.assertFalse(l.receive_message(m.decode(), m))
        self.assertRaises(Empty, self.ready_queue.get_nowait)
        self.assertTrue(self.eta_schedule.empty())

    def test_receieve_message_eta(self):
        l = MyCarrotListener(self.ready_queue, self.eta_schedule, self.logger,
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

        class _Listener(MyCarrotListener):
            iterations = 0
            wait_method = None

            def reset_connection(self):
                if self.iterations >= 1:
                    raise KeyError("foo")

            def _detect_wait_method(self):
                return self.wait_method

        called_back = [False]

        def init_callback(listener):
            called_back[0] = True

        l = _Listener(self.ready_queue, self.eta_schedule, self.logger,
                      send_events=False, init_callback=init_callback)
        l.qos = _QoS()

        def raises_KeyError(limit=None):
            yield True
            l.iterations = 1
            raise KeyError("foo")

        l.wait_method = raises_KeyError
        self.assertRaises(KeyError, l.start)
        self.assertTrue(called_back[0])
        self.assertEqual(l.iterations, 1)
        self.assertEqual(l.qos.prev, l.qos.next)

        l = _Listener(self.ready_queue, self.eta_schedule, self.logger,
                      send_events=False, init_callback=init_callback)
        l.qos = _QoS()

        def raises_socket_error(limit=None):
            yield True
            l.iterations = 1
            raise socket.error("foo")

        l.wait_method = raises_socket_error
        self.assertRaises(KeyError, l.start)
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
        self.assertTrue(worker.listener)
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
        worker.process_task(task)
        worker.pool.stop()

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

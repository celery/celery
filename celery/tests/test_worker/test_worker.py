import socket
import sys

from datetime import datetime, timedelta
from Queue import Empty

from kombu.transport.base import Message
from kombu.connection import BrokerConnection
from celery.utils.timer2 import Timer

from celery import current_app
from celery.concurrency.base import BasePool
from celery.exceptions import SystemTerminate
from celery.task import task as task_dec
from celery.task import periodic_task as periodic_task_dec
from celery.utils import timer2
from celery.utils import gen_unique_id
from celery.worker import WorkController
from celery.worker.buckets import FastQueue
from celery.worker.job import TaskRequest
from celery.worker.consumer import Consumer as MainConsumer
from celery.worker.consumer import QoS, RUN, PREFETCH_COUNT_MAX
from celery.utils.serialization import pickle

from celery.tests.compat import catch_warnings
from celery.tests.utils import unittest
from celery.tests.utils import AppCase, execute_context, skip


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
    no_ack_consumers = set()

    def basic_ack(self, delivery_tag):
        self._acked = True


class MockPool(BasePool):
    _terminated = False
    _stopped = False

    def __init__(self, *args, **kwargs):
        self.raise_regular = kwargs.get("raise_regular", False)
        self.raise_base = kwargs.get("raise_base", False)
        self.raise_SystemTerminate = kwargs.get("raise_SystemTerminate",
                                                False)

    def apply_async(self, *args, **kwargs):
        if self.raise_regular:
            raise KeyError("some exception")
        if self.raise_base:
            raise KeyboardInterrupt("Ctrl+c")
        if self.raise_SystemTerminate:
            raise SystemTerminate()

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
        self.assertRaises(AssertionError, qos.decrement, 10)

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

    class MockConsumer(object):
        prefetch_count = 0

        def qos(self, prefetch_size=0, prefetch_count=0, apply_global=False):
            self.prefetch_count = prefetch_count

    def test_exceeds_short(self):
        consumer = self.MockConsumer()
        qos = QoS(consumer, PREFETCH_COUNT_MAX - 1,
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
        consumer = self.MockConsumer()
        qos = QoS(consumer, 10, current_app.log.get_default_logger())
        qos.update()
        self.assertEqual(qos.value, 10)
        self.assertEqual(consumer.prefetch_count, 10)
        qos.decrement()
        self.assertEqual(qos.value, 9)
        self.assertEqual(consumer.prefetch_count, 9)
        qos.decrement_eventually()
        self.assertEqual(qos.value, 8)
        self.assertEqual(consumer.prefetch_count, 9)

        # Does not decrement 0 value
        qos.value = 0
        qos.decrement()
        self.assertEqual(qos.value, 0)
        qos.increment()
        self.assertEqual(qos.value, 0)


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

    def test_connection(self):
        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)

        l.reset_connection()
        self.assertIsInstance(l.connection, BrokerConnection)

        l._state = RUN
        l.event_dispatcher = None
        l.stop_consumers(close=False)
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

        prev, timer2.to_timestamp = timer2.to_timestamp, to_timestamp
        try:
            l.receive_message(m.decode(), m)
            self.assertTrue(m.acknowledged)
            self.assertTrue(called[0])
        finally:
            timer2.to_timestamp = prev

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
        self.assertIn("Can't decode message body", logger.logged[0])

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
        self.assertRaises(SyntaxError, l.start)
        l.heart.stop()

    def test_consume_messages(self):

        class Connection(current_app.broker_connection().__class__):
            obj = None

            def drain_events(self, **kwargs):
                self.obj.connection = None

        class Consumer(object):
            consuming = False
            prefetch_count = 0

            def consume(self):
                self.consuming = True

            def qos(self, prefetch_size=0, prefetch_count=0,
                            apply_global=False):
                self.prefetch_count = prefetch_count

        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                             send_events=False)
        l.connection = Connection()
        l.connection.obj = l
        l.task_consumer = Consumer()
        l.qos = QoS(l.task_consumer, 10, l.logger)

        l.consume_messages()
        l.consume_messages()
        self.assertTrue(l.task_consumer.consuming)
        self.assertEqual(l.task_consumer.prefetch_count, 10)

        l.qos.decrement()
        l.consume_messages()
        self.assertEqual(l.task_consumer.prefetch_count, 9)

    def test_maybe_conn_error(self):

        def raises(error):

            def fun():
                raise error

            return fun

        l = MyKombuConsumer(self.ready_queue, self.eta_schedule, self.logger,
                             send_events=False)
        l.connection_errors = (KeyError, )
        l.channel_errors = (SyntaxError, )
        l.maybe_conn_error(raises(AttributeError("foo")))
        l.maybe_conn_error(raises(KeyError("foo")))
        l.maybe_conn_error(raises(SyntaxError("foo")))
        self.assertRaises(IndexError, l.maybe_conn_error,
                raises(IndexError("foo")))

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
        l.event_dispatcher = MockEventDispatcher()
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
            value = 4

            def update(self):
                self.prev = self.value

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
            if l.qos.prev != l.qos.value:
                l.qos.update()
            if l.iterations >= 2:
                raise KeyError("foo")

        l.consume_messages = raises_KeyError
        self.assertRaises(KeyError, l.start)
        self.assertTrue(called_back[0])
        self.assertEqual(l.iterations, 1)
        self.assertEqual(l.qos.prev, l.qos.value)

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


class test_WorkController(AppCase):

    def setup(self):
        self.worker = self.create_worker()

    def create_worker(self, **kw):
        worker = WorkController(concurrency=1, loglevel=0, **kw)
        worker.logger = MockLogger()
        return worker

    def test_process_initializer(self):
        from celery import Celery
        from celery import platforms
        from celery import signals
        from celery.app import _tls
        from celery.worker import process_initializer
        from celery.worker import WORKER_SIGRESET, WORKER_SIGIGNORE

        ignored_signals = []
        reset_signals = []
        worker_init = [False]
        default_app = current_app
        app = Celery(loader="default", set_as_current=False)

        class Loader(object):

            def init_worker(self):
                worker_init[0] = True
        app.loader = Loader()

        def on_worker_process_init(**kwargs):
            on_worker_process_init.called = True
        on_worker_process_init.called = False
        signals.worker_process_init.connect(on_worker_process_init)

        def set_mp_process_title(title, hostname=None):
            set_mp_process_title.called = (title, hostname)
        set_mp_process_title.called = ()

        pignore_signal = platforms.ignore_signal
        preset_signal = platforms.reset_signal
        psetproctitle = platforms.set_mp_process_title
        platforms.ignore_signal = lambda sig: ignored_signals.append(sig)
        platforms.reset_signal = lambda sig: reset_signals.append(sig)
        platforms.set_mp_process_title = set_mp_process_title
        try:
            process_initializer(app, "awesome.worker.com")
            self.assertItemsEqual(ignored_signals, WORKER_SIGIGNORE)
            self.assertItemsEqual(reset_signals, WORKER_SIGRESET)
            self.assertTrue(worker_init[0])
            self.assertTrue(on_worker_process_init.called)
            self.assertIs(_tls.current_app, app)
            self.assertTupleEqual(set_mp_process_title.called,
                                  ("celeryd", "awesome.worker.com"))
        finally:
            platforms.ignore_signal = pignore_signal
            platforms.reset_signal = preset_signal
            platforms.set_mp_process_title = psetproctitle
            default_app.set_current()

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
        worker.logger = MockLogger()

        try:
            raise KeyError("foo")
        except KeyError:
            exc_info = sys.exc_info()

        worker.on_timer_error(exc_info)
        logged = worker.logger.logged[0]
        self.assertIn("KeyError", logged)

    def test_on_timer_tick(self):
        worker = WorkController(concurrency=1, loglevel=10)
        worker.logger = MockLogger()
        worker.timer_debug = worker.logger.debug

        worker.on_timer_tick(30.0)
        logged = worker.logger.logged[0]
        self.assertIn("30.0", logged)

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

    def test_process_task_raise_SystemTerminate(self):
        worker = self.worker
        worker.pool = MockPool(raise_SystemTerminate=True)
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = TaskRequest.from_message(m, m.decode())
        worker.components = []
        worker._state = worker.RUN
        self.assertRaises(SystemExit, worker.process_task, task)
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

    def test_start_catches_base_exceptions(self):

        class Component(object):
            stopped = False
            terminated = False

            def __init__(self, exc):
                self.exc = exc

            def start(self):
                raise self.exc

            def terminate(self):
                self.terminated = True

            def stop(self):
                self.stopped = True

        worker1 = self.create_worker()
        worker1.components = [Component(SystemTerminate())]
        self.assertRaises(SystemExit, worker1.start)
        self.assertTrue(worker1.components[0].terminated)

        worker2 = self.create_worker()
        worker2.components = [Component(SystemExit())]
        self.assertRaises(SystemExit, worker2.start)
        self.assertTrue(worker2.components[0].stopped)

    def test_state_db(self):
        from celery.worker import state
        Persistent = state.Persistent

        class MockPersistent(Persistent):

            def _load(self):
                return {}

        state.Persistent = MockPersistent
        try:
            worker = self.create_worker(db="statefilename")
            self.assertTrue(worker._finalize_db)
            worker._finalize_db.cancel()
        finally:
            state.Persistent = Persistent

    @skip("Issue #264")
    def test_disable_rate_limits(self):
        from celery.worker.buckets import FastQueue
        worker = self.create_worker(disable_rate_limits=True)
        self.assertIsInstance(worker.ready_queue, FastQueue)
        self.assertIsNone(worker.mediator)
        self.assertEqual(worker.ready_queue.put, worker.process_task)

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

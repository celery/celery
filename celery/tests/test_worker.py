import unittest
from Queue import Queue, Empty
from datetime import datetime, timedelta
from multiprocessing import get_logger

from carrot.connection import BrokerConnection
from carrot.backends.base import BaseMessage
from billiard.serialization import pickle

from celery import conf
from celery.utils import gen_unique_id, noop
from celery.worker import WorkController
from celery.worker.listener import CarrotListener, RUN, CLOSE
from celery.worker.job import TaskWrapper
from celery.worker.scheduler import Scheduler
from celery.decorators import task as task_dec
from celery.decorators import periodic_task as periodic_task_dec


class PlaceHolder(object):
        pass


class MockControlDispatch(object):
    commands = []

    def dispatch_from_message(self, message):
        self.commands.append(message.pop("command", None))


class MockEventDispatcher(object):
    sent = []
    closed = False

    def send(self, event, *args, **kwargs):
        self.sent.append(event)

    def close(self):
        self.closed = True


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

    def critical(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass


class MockBackend(object):
    _acked = False

    def ack(self, delivery_tag):
        self._acked = True


class MockPool(object):

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
        pass
        return True


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


class TestCarrotListener(unittest.TestCase):

    def setUp(self):
        self.ready_queue = Queue()
        self.eta_schedule = Scheduler(self.ready_queue)
        self.logger = get_logger()
        self.logger.setLevel(0)

    def test_mainloop(self):
        l = CarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)

        class MockConnection(object):

            def drain_events(self):
                return "draining"

        l.connection = PlaceHolder()
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
        self.assertEquals(l._detect_wait_method(), l._mainloop)
        self.assertTrue(records.get("broadcast_callback"))
        self.assertTrue(records.get("consume_broadcast"))
        self.assertTrue(records.get("consume_tasks"))

        records.clear()
        l.connection.connection = PlaceHolder()
        self.assertTrue(l._detect_wait_method() is l.task_consumer.iterconsume)
        self.assertTrue(records.get("consumer_add"))

    def test_connection(self):
        l = CarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)

        l.reset_connection()
        self.assertTrue(isinstance(l.connection, BrokerConnection))

        l.close_connection()
        self.assertTrue(l.connection is None)
        self.assertTrue(l.task_consumer is None)

        l.reset_connection()
        self.assertTrue(isinstance(l.connection, BrokerConnection))

        l.stop()
        self.assertTrue(l.connection is None)
        self.assertTrue(l.task_consumer is None)

    def test_receive_message_control_command(self):
        l = CarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        backend = MockBackend()
        m = create_message(backend, control={"command": "shutdown"})
        l.event_dispatcher = MockEventDispatcher()
        l.control_dispatch = MockControlDispatch()
        l.receive_message(m.decode(), m)
        self.assertTrue("shutdown" in l.control_dispatch.commands)

    def test_close_connection(self):
        l = CarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        l._state = RUN
        l.close_connection()

        l = CarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        eventer = l.event_dispatcher = MockEventDispatcher()
        heart = l.heart = MockHeart()
        l._state = RUN
        l.close_connection()
        self.assertTrue(eventer.closed)
        self.assertTrue(heart.closed)

    def test_receive_message_unknown(self):
        l = CarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        backend = MockBackend()
        m = create_message(backend, unknown={"baz": "!!!"})
        l.event_dispatcher = MockEventDispatcher()
        l.control_dispatch = MockControlDispatch()
        import warnings
        with warnings.catch_warnings(record=True) as log:
                l.receive_message(m.decode(), m)
                self.assertTrue(log)
                self.assertTrue("unknown message" in log[0].message.args[0])

    def test_receieve_message(self):
        l = CarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name,
                           args=[2, 4, 8], kwargs={})

        l.event_dispatcher = MockEventDispatcher()
        l.receive_message(m.decode(), m)

        in_bucket = self.ready_queue.get_nowait()
        self.assertTrue(isinstance(in_bucket, TaskWrapper))
        self.assertEquals(in_bucket.task_name, foo_task.name)
        self.assertEquals(in_bucket.execute(), 2 * 4 * 8)
        self.assertTrue(self.eta_schedule.empty())

    def test_receieve_message_eta_isoformat(self):
        l = CarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name,
                           eta=datetime.now().isoformat(),
                           args=[2, 4, 8], kwargs={})

        l.event_dispatcher = MockEventDispatcher()
        l.receive_message(m.decode(), m)

        items = [entry[2] for entry in self.eta_schedule.queue]
        found = 0
        for item in items:
            if item.task_name == foo_task.name:
                found = True
        self.assertTrue(found)

    def test_revoke(self):
        ready_queue = Queue()
        l = CarrotListener(ready_queue, self.eta_schedule, self.logger,
                           send_events=False)
        backend = MockBackend()
        id = gen_unique_id()
        c = create_message(backend, control={"command": "revoke",
                                             "task_id": id})
        t = create_message(backend, task=foo_task.name, args=[2, 4, 8],
                           kwargs={}, id=id)
        l.event_dispatcher = MockEventDispatcher()
        l.receive_message(c.decode(), c)
        from celery.worker.revoke import revoked
        self.assertTrue(id in revoked)

        l.receive_message(t.decode(), t)
        self.assertTrue(ready_queue.empty())

    def test_receieve_message_not_registered(self):
        l = CarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                          send_events=False)
        backend = MockBackend()
        m = create_message(backend, task="x.X.31x", args=[2, 4, 8], kwargs={})

        l.event_dispatcher = MockEventDispatcher()
        self.assertFalse(l.receive_message(m.decode(), m))
        self.assertRaises(Empty, self.ready_queue.get_nowait)
        self.assertTrue(self.eta_schedule.empty())

    def test_receieve_message_eta(self):
        l = CarrotListener(self.ready_queue, self.eta_schedule, self.logger,
                          send_events=False)
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name,
                           args=[2, 4, 8], kwargs={},
                           eta=(datetime.now() +
                               timedelta(days=1)).isoformat())

        l.reset_connection()
        p, conf.BROKER_CONNECTION_RETRY = conf.BROKER_CONNECTION_RETRY, False
        try:
            l.reset_connection()
        finally:
            conf.BROKER_CONNECTION_RETRY = p
        l.receive_message(m.decode(), m)

        in_hold = self.eta_schedule.queue[0]
        self.assertEquals(len(in_hold), 4)
        eta, priority, task, on_accept = in_hold
        self.assertTrue(isinstance(task, TaskWrapper))
        self.assertTrue(callable(on_accept))
        self.assertEquals(task.task_name, foo_task.name)
        self.assertEquals(task.execute(), 2 * 4 * 8)
        self.assertRaises(Empty, self.ready_queue.get_nowait)


class TestWorkController(unittest.TestCase):

    def setUp(self):
        self.worker = WorkController(concurrency=1, loglevel=0)
        self.worker.logger = MockLogger()

    def test_attrs(self):
        worker = self.worker
        self.assertTrue(isinstance(worker.eta_schedule, Scheduler))
        self.assertTrue(worker.scheduler)
        self.assertTrue(worker.pool)
        self.assertTrue(worker.listener)
        self.assertTrue(worker.mediator)
        self.assertTrue(worker.components)

    def test_process_task(self):
        worker = self.worker
        worker.pool = MockPool()
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = TaskWrapper.from_message(m, m.decode())
        worker.process_task(task)
        worker.pool.stop()

    def test_process_task_raise_base(self):
        worker = self.worker
        worker.pool = MockPool(raise_base=True)
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = TaskWrapper.from_message(m, m.decode())
        worker.process_task(task)
        worker.pool.stop()

    def test_process_task_raise_regular(self):
        worker = self.worker
        worker.pool = MockPool(raise_regular=True)
        backend = MockBackend()
        m = create_message(backend, task=foo_task.name, args=[4, 8, 10],
                           kwargs={})
        task = TaskWrapper.from_message(m, m.decode())
        worker.process_task(task)
        worker.pool.stop()

    def test_start_stop(self):
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
        for component in worker.components:
            self.assertTrue(component._stopped)

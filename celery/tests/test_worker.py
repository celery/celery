import unittest
from Queue import Queue, Empty
from datetime import datetime, timedelta
from multiprocessing import get_logger

from carrot.connection import BrokerConnection
from carrot.backends.base import BaseMessage
from billiard.serialization import pickle

from celery.utils import gen_unique_id
from celery.worker import CarrotListener, WorkController
from celery.worker.job import TaskWrapper
from celery.worker.scheduler import Scheduler
from celery.decorators import task as task_dec
from celery.decorators import periodic_task as periodic_task_dec


class MockEventDispatcher(object):

    def send(self, *args, **kwargs):
        pass

    def close(self):
        pass


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
        self.worker = WorkController(concurrency=1,
                                     loglevel=0,
                                     is_detached=False)
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

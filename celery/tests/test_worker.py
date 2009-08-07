import unittest
from Queue import Queue, Empty
from carrot.connection import AMQPConnection
from celery.messaging import TaskConsumer
from celery.worker.job import TaskWrapper
from celery.worker import AMQPListener, WorkController
from multiprocessing import get_logger
from carrot.backends.base import BaseMessage
from celery import registry
from celery.serialization import pickle
from celery.utils import gen_unique_id
from datetime import datetime, timedelta


def foo_task(x, y, z, **kwargs):
    return x * y * z
registry.tasks.register(foo_task, name="c.u.foo")


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
    data["id"] = gen_unique_id()
    return BaseMessage(backend, body=pickle.dumps(dict(**data)),
                       content_type="application/x-python-serialize",
                       content_encoding="binary")


class TestAMQPListener(unittest.TestCase):

    def setUp(self):
        self.bucket_queue = Queue()
        self.hold_queue = Queue()
        self.logger = get_logger()
        self.logger.setLevel(0)

    def test_connection(self):
        l = AMQPListener(self.bucket_queue, self.hold_queue, self.logger)

        c = l.reset_connection()
        self.assertTrue(c is l.task_consumer)
        self.assertTrue(isinstance(l.amqp_connection, AMQPConnection))

        l.close_connection()
        self.assertTrue(l.amqp_connection is None)
        self.assertTrue(l.task_consumer is None)

        c = l.reset_connection()
        self.assertTrue(c is l.task_consumer)
        self.assertTrue(isinstance(l.amqp_connection, AMQPConnection))

        l.stop()
        self.assertTrue(l.amqp_connection is None)
        self.assertTrue(l.task_consumer is None)

    def test_receieve_message(self):
        l = AMQPListener(self.bucket_queue, self.hold_queue, self.logger)
        backend = MockBackend()
        m = create_message(backend, task="c.u.foo", args=[2, 4, 8], kwargs={})

        l.receive_message(m.decode(), m)

        in_bucket = self.bucket_queue.get_nowait()
        self.assertTrue(isinstance(in_bucket, TaskWrapper))
        self.assertEquals(in_bucket.task_name, "c.u.foo")
        self.assertEquals(in_bucket.execute(), 2 * 4 * 8)
        self.assertRaises(Empty, self.hold_queue.get_nowait)

    def test_receieve_message_not_registered(self):
        l = AMQPListener(self.bucket_queue, self.hold_queue, self.logger)
        backend = MockBackend()
        m = create_message(backend, task="x.X.31x", args=[2, 4, 8], kwargs={})

        self.assertFalse(l.receive_message(m.decode(), m))
        self.assertRaises(Empty, self.bucket_queue.get_nowait)
        self.assertRaises(Empty, self.hold_queue.get_nowait)

    def test_receieve_message_eta(self):
        l = AMQPListener(self.bucket_queue, self.hold_queue, self.logger)
        backend = MockBackend()
        m = create_message(backend, task="c.u.foo", args=[2, 4, 8], kwargs={},
                           eta=datetime.now() + timedelta(days=1))

        l.receive_message(m.decode(), m)

        in_hold = self.hold_queue.get_nowait()
        self.assertEquals(len(in_hold), 2)
        task, eta = in_hold
        self.assertTrue(isinstance(task, TaskWrapper))
        self.assertTrue(isinstance(eta, datetime))
        self.assertEquals(task.task_name, "c.u.foo")
        self.assertEquals(task.execute(), 2 * 4 * 8)
        self.assertRaises(Empty, self.bucket_queue.get_nowait)


class TestWorkController(unittest.TestCase):

    def setUp(self):
        self.worker = WorkController(concurrency=1, loglevel=0,
                                     is_detached=False)
        self.worker.logger = MockLogger()

    def test_attrs(self):
        worker = self.worker
        self.assertTrue(isinstance(worker.bucket_queue, Queue))
        self.assertTrue(isinstance(worker.hold_queue, Queue))
        self.assertTrue(worker.periodic_work_controller)
        self.assertTrue(worker.pool)
        self.assertTrue(worker.amqp_listener)
        self.assertTrue(worker.mediator)
        self.assertTrue(worker.components)

    def test_safe_process_task(self):
        worker = self.worker
        worker.pool = MockPool()
        backend = MockBackend()
        m = create_message(backend, task="c.u.foo", args=[4, 8, 10],
                           kwargs={})
        task = TaskWrapper.from_message(m, m.decode())
        worker.safe_process_task(task)
        worker.pool.stop()

    def test_safe_process_task_raise_base(self):
        worker = self.worker
        worker.pool = MockPool(raise_base=True)
        backend = MockBackend()
        m = create_message(backend, task="c.u.foo", args=[4, 8, 10],
                           kwargs={})
        task = TaskWrapper.from_message(m, m.decode())
        worker.safe_process_task(task)
        worker.pool.stop()

    def test_safe_process_task_raise_regular(self):
        worker = self.worker
        worker.pool = MockPool(raise_regular=True)
        backend = MockBackend()
        m = create_message(backend, task="c.u.foo", args=[4, 8, 10],
                           kwargs={})
        task = TaskWrapper.from_message(m, m.decode())
        worker.safe_process_task(task)
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

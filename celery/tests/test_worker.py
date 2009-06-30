import unittest
from Queue import Queue, Empty
from carrot.connection import AMQPConnection
from celery.messaging import TaskConsumer
from celery.worker.job import TaskWrapper
from celery.worker import AMQPListener, WorkController
from multiprocessing import get_logger
from carrot.backends.base import BaseMessage
from celery import registry
from celery.utils import pickle, gen_unique_id
from datetime import datetime, timedelta


def foo_task(x, y, z, **kwargs):
    return x * y * z
registry.tasks.register(foo_task, name="c.u.foo")


class MockBackend(object):
    _acked = False

    def ack(self, delivery_tag):
        self._acked = True


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
        self.assertTrue(isinstance(c, TaskConsumer))
        self.assertTrue(c is l.task_consumer)
        self.assertTrue(isinstance(l.amqp_connection, AMQPConnection))

        l.close_connection()
        self.assertTrue(l.amqp_connection is None)
        self.assertTrue(l.task_consumer is None)

        c = l.reset_connection()
        self.assertTrue(isinstance(c, TaskConsumer))
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

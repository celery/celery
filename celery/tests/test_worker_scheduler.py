import unittest
from Queue import Queue, Empty
from datetime import datetime, timedelta

from celery.worker.scheduler import Scheduler


class MockItem(object):

    def __init__(self, value):
        self.task_id = value


class TestScheduler(unittest.TestCase):

    def test_sched_and_run_now(self):
        ready_queue = Queue()
        sched = Scheduler(ready_queue)
        now = datetime.now()

        callback_called = [False]
        def callback():
            callback_called[0] = True

        sched.enter(MockItem("foo"), eta=now, callback=callback)

        remaining = iter(sched).next()
        self.assertEquals(remaining, 0)
        self.assertTrue(callback_called[0])
        self.assertEquals(ready_queue.get_nowait().task_id, "foo")

    def test_sched_run_later(self):
        ready_queue = Queue()
        sched = Scheduler(ready_queue)
        now = datetime.now()

        callback_called = [False]
        def callback():
            callback_called[0] = True

        eta = now + timedelta(seconds=10)
        sched.enter(MockItem("foo"), eta=eta, callback=callback)

        remaining = iter(sched).next()
        self.assertTrue(remaining > 7)
        self.assertFalse(callback_called[0])
        self.assertRaises(Empty, ready_queue.get_nowait)

    def test_empty_queue_yields_None(self):
        ready_queue = Queue()
        sched = Scheduler(ready_queue)

        self.assertTrue(iter(sched).next() is None)

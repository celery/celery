from __future__ import generators

import unittest2 as unittest
from Queue import Queue, Empty
from datetime import datetime, timedelta

from celery.worker.scheduler import Scheduler


class MockItem(object):
    is_revoked = False

    def __init__(self, value):
        self.task_id = value

    def revoked(self):
        return self.is_revoked


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
        self.assertEqual(remaining, 0)
        self.assertTrue(callback_called[0])
        self.assertEqual(ready_queue.get_nowait().task_id, "foo")

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
        self.assertTrue(remaining > 7 or remaining == sched.max_interval)
        self.assertFalse(callback_called[0])
        self.assertRaises(Empty, ready_queue.get_nowait)

    def test_empty_queue_yields_None(self):
        ready_queue = Queue()
        sched = Scheduler(ready_queue)
        self.assertIsNone(iter(sched).next())

from __future__ import generators

import unittest2 as unittest

from datetime import datetime, timedelta
from Queue import Queue, Empty

from celery.worker.scheduler import Scheduler


class MockItem(object):

    def __init__(self, value, revoked=False):
        self.task_id = value
        self.is_revoked = revoked

    def revoked(self):
        return self.is_revoked


class test_Scheduler(unittest.TestCase):

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

    def test_sched_revoked(self):
        ready_queue = Queue()
        sched = Scheduler(ready_queue)
        now = datetime.now()

        callback_called = [False]
        def callback():
            callback_called[0] = True

        sched.enter(MockItem("foo", revoked=True), eta=now, callback=callback)
        iter(sched).next()
        self.assertFalse(callback_called[0])
        self.assertRaises(Empty, ready_queue.get_nowait)
        self.assertFalse(sched.queue)
        sched.clear()

    def test_sched_clear(self):
        ready_queue = Queue()
        sched = Scheduler(ready_queue)
        sched.enter(MockItem("foo"), eta=datetime.now(), callback=None)
        self.assertFalse(sched.empty())
        sched.clear()
        self.assertTrue(sched.empty())

    def test_sched_info(self):
        ready_queue = Queue()
        sched = Scheduler(ready_queue)
        item = MockItem("foo")
        sched.enter(item, eta=10, callback=None)
        self.assertDictEqual({"eta": 10, "priority": 0,
                              "item": item}, sched.info().next())

    def test_sched_queue(self):
        ready_queue = Queue()
        sched = Scheduler(ready_queue)
        sched.enter(MockItem("foo"), eta=datetime.now(), callback=None)
        self.assertTrue(sched.queue)

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

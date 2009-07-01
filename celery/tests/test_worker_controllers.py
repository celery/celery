import unittest
import time
import multiprocessing
from Queue import Queue, Empty
from datetime import datetime, timedelta

from celery.worker.controllers import Mediator, PeriodicWorkController
from celery.worker.controllers import InfinityThread


class MyInfinityThread(InfinityThread):

    def on_iteration(self):
        import time
        time.sleep(1)


class TestInfinityThread(unittest.TestCase):

    def test_on_iteration(self):
        self.assertRaises(NotImplementedError, InfinityThread().on_iteration)

    def test_run(self):
        t = MyInfinityThread()
        t._shutdown.set()
        t.run()
        self.assertTrue(t._stopped.isSet())

    def test_start_stop(self):
        t = MyInfinityThread()
        t.start()
        self.assertFalse(t._shutdown.isSet())
        self.assertFalse(t._stopped.isSet())
        t.stop()
        self.assertTrue(t._shutdown.isSet())
        self.assertTrue(t._stopped.isSet())


class TestMediator(unittest.TestCase):

    def test_mediator_start__stop(self):
        bucket_queue = Queue()
        m = Mediator(bucket_queue, lambda t: t)
        m.start()
        self.assertFalse(m._shutdown.isSet())
        self.assertFalse(m._stopped.isSet())
        m.stop()
        m.join()
        self.assertTrue(m._shutdown.isSet())
        self.assertTrue(m._stopped.isSet())

    def test_mediator_on_iteration(self):
        bucket_queue = Queue()
        got = {}

        def mycallback(value):
            got["value"] = value

        m = Mediator(bucket_queue, mycallback)
        bucket_queue.put("George Constanza")

        m.on_iteration()

        self.assertEquals(got["value"], "George Constanza")


class TestPeriodicWorkController(unittest.TestCase):

    def test_process_hold_queue(self):
        bucket_queue = Queue()
        hold_queue = Queue()
        m = PeriodicWorkController(bucket_queue, hold_queue)

        m.process_hold_queue()

        hold_queue.put(("task1", datetime.now() - timedelta(days=1)))

        m.process_hold_queue()
        self.assertRaises(Empty, hold_queue.get_nowait)
        self.assertEquals(bucket_queue.get_nowait(), "task1")
        tomorrow = datetime.now() + timedelta(days=1)
        hold_queue.put(("task2", tomorrow))
        m.process_hold_queue()
        self.assertRaises(Empty, bucket_queue.get_nowait)
        self.assertEquals(hold_queue.get_nowait(), ("task2", tomorrow))

    def test_run_periodic_tasks(self):
        bucket_queue = Queue()
        hold_queue = Queue()
        m = PeriodicWorkController(bucket_queue, hold_queue)
        m.run_periodic_tasks()

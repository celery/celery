import unittest
import time
import multiprocessing
from Queue import Queue, Empty
from datetime import datetime, timedelta

from celery.worker.controllers import Mediator
from celery.worker.controllers import BackgroundThread


class MockTask(object):
    task_id = 1234
    task_name = "mocktask"

    def __init__(self, value, **kwargs):
        self.value = value


class MyBackgroundThread(BackgroundThread):

    def on_iteration(self):
        import time
        time.sleep(1)


class TestBackgroundThread(unittest.TestCase):

    def test_on_iteration(self):
        self.assertRaises(NotImplementedError,
                BackgroundThread().on_iteration)

    def test_run(self):
        t = MyBackgroundThread()
        t._shutdown.set()
        t.run()
        self.assertTrue(t._stopped.isSet())

    def test_start_stop(self):
        t = MyBackgroundThread()
        t.start()
        self.assertFalse(t._shutdown.isSet())
        self.assertFalse(t._stopped.isSet())
        t.stop()
        self.assertTrue(t._shutdown.isSet())
        self.assertTrue(t._stopped.isSet())


class TestMediator(unittest.TestCase):

    def test_mediator_start__stop(self):
        ready_queue = Queue()
        m = Mediator(ready_queue, lambda t: t)
        m.start()
        self.assertFalse(m._shutdown.isSet())
        self.assertFalse(m._stopped.isSet())
        m.stop()
        m.join()
        self.assertTrue(m._shutdown.isSet())
        self.assertTrue(m._stopped.isSet())

    def test_mediator_on_iteration(self):
        ready_queue = Queue()
        got = {}

        def mycallback(value):
            got["value"] = value.value

        m = Mediator(ready_queue, mycallback)
        ready_queue.put(MockTask("George Constanza"))

        m.on_iteration()

        self.assertEquals(got["value"], "George Constanza")

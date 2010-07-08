import time
import unittest2 as unittest
from Queue import Queue

from celery.utils import gen_unique_id
from celery.worker.controllers import Mediator
from celery.worker.controllers import BackgroundThread, ScheduleController
from celery.worker.state import revoked as revoked_tasks


class MockTask(object):
    task_id = 1234
    task_name = "mocktask"
    acked = False

    def __init__(self, value, **kwargs):
        self.value = value

    def on_ack(self):
        self.acked = True

    def revoked(self):
        if self.task_id in revoked_tasks:
            self.on_ack()
            return True
        return False


class MyBackgroundThread(BackgroundThread):

    def on_iteration(self):
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
        ready_queue.put(MockTask("George Costanza"))

        m.on_iteration()

        self.assertEqual(got["value"], "George Costanza")

    def test_mediator_on_iteration_revoked(self):
        ready_queue = Queue()
        got = {}

        def mycallback(value):
            got["value"] = value.value

        m = Mediator(ready_queue, mycallback)
        t = MockTask("Jerry Seinfeld")
        t.task_id = gen_unique_id()
        revoked_tasks.add(t.task_id)
        ready_queue.put(t)

        m.on_iteration()

        self.assertNotIn("value", got)
        self.assertTrue(t.acked)


class TestScheduleController(unittest.TestCase):

    def test_on_iteration(self):
        times = range(10) + [None]
        c = ScheduleController(times)

        import time
        slept = [None]

        def _sleep(count):
            slept[0] = count

        old_sleep = time.sleep
        time.sleep = _sleep
        try:
            for i in times:
                c.on_iteration()
                res = i or 1
                self.assertEqual(slept[0], res)
        finally:
            time.sleep = old_sleep

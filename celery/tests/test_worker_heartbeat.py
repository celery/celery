from celery.worker.heartbeat import Heart

from celery.tests.utils import unittest, sleepdeprived


class MockDispatcher(object):
    heart = None
    next_iter = 0

    def __init__(self):
        self.sent = []

    def send(self, msg):
        self.sent.append(msg)
        if self.heart:
            if self.next_iter > 10:
                self.heart._shutdown.set()
            self.next_iter += 1


class MockDispatcherRaising(object):

    def send(self, msg):
        if msg == "worker-offline":
            raise Exception("foo")


class MockHeart(Heart):
    _alive = True
    _joined = False

    def isAlive(self):
        return self._alive

    def join(self, timeout=None):
        self._joined = True


class TestHeart(unittest.TestCase):

    def test_stop(self):
        eventer = MockDispatcher()
        h = MockHeart(eventer, interval=1)
        h._state = "RUN"
        h.stop()
        self.assertTrue(h._joined)

        h2 = MockHeart(eventer, interval=1)
        h2._alive = False
        h2._state = "RUN"
        h2.stop()
        self.assertFalse(h2._joined)

    def test_time_raises_TypeError(self):
        from celery.worker import heartbeat

        def raises_TypeError(exc):
            raise TypeError("x")

        prev_time, heartbeat.time = heartbeat.time, raises_TypeError
        try:
            eventer = MockDispatcher()
            heart = Heart(eventer, interval=0.1)
            heart.run()
            self.assertIn("worker-online", eventer.sent)
            self.assertNotIn("worker-heartbeat", eventer.sent)

        finally:
            heartbeat.time = prev_time

    @sleepdeprived
    def test_run_manages_cycle(self):
        eventer = MockDispatcher()
        heart = Heart(eventer, interval=0.1)
        eventer.heart = heart
        heart.run()
        self.assertEqual(heart._state, "RUN")
        self.assertTrue(heart._shutdown.isSet())
        heart._shutdown.clear()
        heart._stopped.clear()
        eventer.next_iter = 0
        heart.run()

    def test_run(self):
        eventer = MockDispatcher()

        heart = Heart(eventer, interval=1)
        heart._shutdown.set()
        heart.run()
        self.assertEqual(heart._state, "RUN")
        self.assertIn("worker-online", eventer.sent)
        self.assertIn("worker-heartbeat", eventer.sent)
        self.assertIn("worker-offline", eventer.sent)

        heart.stop()
        heart.stop()
        self.assertEqual(heart._state, "CLOSE")

        heart = Heart(eventer, interval=0.00001)
        heart._shutdown.set()
        for i in range(10):
            heart.run()

    def test_run_exception(self):
        eventer = MockDispatcherRaising()
        heart = Heart(eventer, interval=1)
        heart._shutdown.set()
        self.assertRaises(Exception, heart.run)

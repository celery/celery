from celery.tests.utils import unittest

from celery.worker.heartbeat import Heart


class MockDispatcher(object):
    shutdown_event = None
    next_iter = False

    def __init__(self):
        self.sent = []

    def send(self, msg):
        self.sent.append(msg)
        if self.shutdown_event:
            if self.next_iter:
                self.shutdown_event.set()
            self.next_iter = True


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

    def test_run_manages_cycle(self):
        eventer = MockDispatcher()
        heart = Heart(eventer, interval=1)
        eventer.shutdown_event = heart._shutdown
        heart.run()
        self.assertEqual(heart._state, "RUN")
        self.assertTrue(heart._shutdown.isSet())
        self.assertTrue(heart._stopped.isSet())

    def test_run(self):
        eventer = MockDispatcher()

        heart = Heart(eventer, interval=1)
        heart._shutdown.set()
        heart.run()
        self.assertEqual(heart._state, "RUN")
        self.assertIn("worker-online", eventer.sent)
        self.assertIn("worker-heartbeat", eventer.sent)
        self.assertIn("worker-offline", eventer.sent)

        self.assertTrue(heart._stopped.isSet())

        heart.stop()
        heart.stop()
        self.assertEqual(heart._state, "CLOSE")

        heart = Heart(eventer, interval=0.00001)
        heart._shutdown.set()
        for i in range(10):
            heart.run()

    def test_run_stopped_is_set_even_if_send_breaks(self):
        eventer = MockDispatcherRaising()
        heart = Heart(eventer, interval=1)
        heart._shutdown.set()
        self.assertRaises(Exception, heart.run)
        self.assertTrue(heart._stopped.isSet())

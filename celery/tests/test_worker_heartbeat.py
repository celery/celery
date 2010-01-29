import unittest

from celery.worker.heartbeat import Heart


class MockDispatcher(object):

    def __init__(self):
        self.sent = []

    def send(self, msg):
        self.sent.append(msg)


class MockDispatcherRaising(object):

    def send(self, msg):
        if msg == "worker-offline":
            raise Exception("foo")


class TestHeart(unittest.TestCase):

    def test_run(self):
        eventer = MockDispatcher()

        heart = Heart(eventer, interval=1)
        heart._shutdown.set()
        heart.run()
        self.assertTrue(heart._state == "RUN")
        self.assertTrue("worker-online" in eventer.sent)
        self.assertTrue("worker-heartbeat" in eventer.sent)
        self.assertTrue("worker-offline" in eventer.sent)

        self.assertTrue(heart._stopped.isSet())

        heart.stop()
        heart.stop()
        self.assertTrue(heart._state == "CLOSE")

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

import logging

from time import time

from celery.concurrency.base import BasePool
from celery.worker import state
from celery.worker import autoscale

from celery.tests.utils import unittest, sleepdeprived

logger = logging.getLogger("celery.tests.autoscale")


class Object(object):
    pass


class MockPool(BasePool):
    shrink_raises_exception = False

    def __init__(self, *args, **kwargs):
        super(MockPool, self).__init__(*args, **kwargs)
        self._pool = Object()
        self._pool._processes = self.limit

    def grow(self, n=1):
        self._pool._processes += n

    def shrink(self, n=1):
        if self.shrink_raises_exception:
            raise KeyError("foo")
        self._pool._processes -= n

    @property
    def current(self):
        return self._pool._processes


class test_Autoscaler(unittest.TestCase):

    def setUp(self):
        self.pool = MockPool(3)

    def test_stop(self):

        class Scaler(autoscale.Autoscaler):
            alive = True
            joined = False

            def isAlive(self):
                return self.alive

            def join(self, timeout=None):
                self.joined = True

        x = Scaler(self.pool, 10, 3, logger=logger)
        x._stopped.set()
        x.stop()
        self.assertTrue(x.joined)
        x.joined = False
        x.alive = False
        x.stop()
        self.assertFalse(x.joined)

    @sleepdeprived(autoscale)
    def test_scale(self):
        x = autoscale.Autoscaler(self.pool, 10, 3, logger=logger)
        x.scale()
        self.assertEqual(x.pool.current, 3)
        for i in range(20):
            state.reserved_requests.add(i)
        x.scale()
        x.scale()
        self.assertEqual(x.pool.current, 10)
        state.reserved_requests.clear()
        x.scale()
        self.assertEqual(x.pool.current, 10)
        x._last_action = time() - 10000
        x.scale()
        self.assertEqual(x.pool.current, 3)

    def test_run(self):

        class Scaler(autoscale.Autoscaler):
            scale_called = False

            def scale(self):
                self.scale_called = True
                self._shutdown.set()

        x = Scaler(self.pool, 10, 3, logger=logger)
        x.run()
        self.assertTrue(x._shutdown.isSet())
        self.assertTrue(x._stopped.isSet())
        self.assertTrue(x.scale_called)

    def test_shrink_raises_exception(self):
        x = autoscale.Autoscaler(self.pool, 10, 3, logger=logger)
        x.scale_up(3)
        x._last_action = time() - 10000
        x.pool.shrink_raises_exception = True
        x.scale_down(1)

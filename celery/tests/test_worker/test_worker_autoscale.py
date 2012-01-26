from __future__ import absolute_import

import logging
import sys

from time import time

from mock import Mock, patch

from celery.concurrency.base import BasePool
from celery.worker import state
from celery.worker import autoscale
from celery.tests.utils import Case, sleepdeprived

logger = logging.getLogger("celery.tests.autoscale")


class Object(object):
    pass


class MockPool(BasePool):
    shrink_raises_exception = False
    shrink_raises_ValueError = False

    def __init__(self, *args, **kwargs):
        super(MockPool, self).__init__(*args, **kwargs)
        self._pool = Object()
        self._pool._processes = self.limit

    def grow(self, n=1):
        self._pool._processes += n

    def shrink(self, n=1):
        if self.shrink_raises_exception:
            raise KeyError("foo")
        if self.shrink_raises_ValueError:
            raise ValueError("foo")
        self._pool._processes -= n

    @property
    def num_processes(self):
        return self._pool._processes


class test_Autoscaler(Case):

    def setUp(self):
        self.pool = MockPool(3)

    def test_stop(self):

        class Scaler(autoscale.Autoscaler):
            alive = True
            joined = False

            def is_alive(self):
                return self.alive

            def join(self, timeout=None):
                self.joined = True

        x = Scaler(self.pool, 10, 3, logger=logger)
        x._is_stopped.set()
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
        self.assertEqual(x.pool.num_processes, 3)
        for i in range(20):
            state.reserved_requests.add(i)
        x.scale()
        x.scale()
        self.assertEqual(x.pool.num_processes, 10)
        state.reserved_requests.clear()
        x.scale()
        self.assertEqual(x.pool.num_processes, 10)
        x._last_action = time() - 10000
        x.scale()
        self.assertEqual(x.pool.num_processes, 3)

    def test_run(self):

        class Scaler(autoscale.Autoscaler):
            scale_called = False

            def body(self):
                self.scale_called = True
                self._is_shutdown.set()

        x = Scaler(self.pool, 10, 3, logger=logger)
        x.run()
        self.assertTrue(x._is_shutdown.isSet())
        self.assertTrue(x._is_stopped.isSet())
        self.assertTrue(x.scale_called)

    def test_shrink_raises_exception(self):
        x = autoscale.Autoscaler(self.pool, 10, 3, logger=logger)
        x.scale_up(3)
        x._last_action = time() - 10000
        x.pool.shrink_raises_exception = True
        x.scale_down(1)

    def test_shrink_raises_ValueError(self):
        x = autoscale.Autoscaler(self.pool, 10, 3, logger=logger)
        x.logger = Mock()
        x.scale_up(3)
        x._last_action = time() - 10000
        x.pool.shrink_raises_ValueError = True
        x.scale_down(1)
        self.assertTrue(x.logger.debug.call_count)

    def test_update_and_force(self):
        x = autoscale.Autoscaler(self.pool, 10, 3, logger=logger)
        self.assertEqual(x.processes, 3)
        x.force_scale_up(5)
        self.assertEqual(x.processes, 8)
        x.update(5, None)
        self.assertEqual(x.processes, 5)
        x.force_scale_down(3)
        self.assertEqual(x.processes, 2)
        x.update(3, None)
        self.assertEqual(x.processes, 3)

    def test_info(self):
        x = autoscale.Autoscaler(self.pool, 10, 3, logger=logger)
        info = x.info()
        self.assertEqual(info['max'], 10)
        self.assertEqual(info['min'], 3)
        self.assertEqual(info['current'], 3)

    @patch("os._exit")
    def test_thread_crash(self, _exit):

        class _Autoscaler(autoscale.Autoscaler):

            def body(self):
                self._is_shutdown.set()
                raise OSError("foo")
        x = _Autoscaler(self.pool, 10, 3, logger=logger)

        stderr = Mock()
        p, sys.stderr = sys.stderr, stderr
        try:
            x.run()
        finally:
            sys.stderr = p
        _exit.assert_called_with(1)
        self.assertTrue(stderr.write.call_count)

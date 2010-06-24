import sys
import unittest2 as unittest

from celery.concurrency import processes as mp
from celery.datastructures import ExceptionInfo


def to_excinfo(exc):
    try:
        raise exc
    except:
        return ExceptionInfo(sys.exc_info())


class MockPool(object):
    started = False
    closed = False
    joined = False
    terminated = False
    _state = None

    def __init__(self, *args, **kwargs):
        self.started = True
        self._state = mp.RUN

    def close(self):
        self.closed = True
        self._state = "CLOSE"

    def join(self):
        self.joined = True

    def terminate(self):
        self.terminated = True

    def apply_async(self, *args, **kwargs):
        pass


class TaskPool(mp.TaskPool):
    Pool = MockPool


class test_TaskPool(unittest.TestCase):

    def test_start(self):
        pool = TaskPool(10)
        pool.start()
        self.assertTrue(pool._pool.started)

        _pool = pool._pool
        pool.stop()
        self.assertTrue(_pool.closed)
        self.assertTrue(_pool.joined)
        pool.stop()

        pool.start()
        _pool = pool._pool
        pool.terminate()
        pool.terminate()
        self.assertTrue(_pool.terminated)

    def test_on_ready_exception(self):

        scratch = [None]
        def errback(retval):
            scratch[0] = retval

        pool = TaskPool(10)
        exc = to_excinfo(KeyError("foo"))
        pool.on_ready([], [errback], exc)
        self.assertEqual(exc, scratch[0])

    def test_on_ready_value(self):

        scratch = [None]
        def callback(retval):
            scratch[0] = retval

        pool = TaskPool(10)
        retval = "the quick brown fox"
        pool.on_ready([callback], [], retval)
        self.assertEqual(retval, scratch[0])

    def test_on_ready_exit_exception(self):
        pool = TaskPool(10)
        exc = to_excinfo(SystemExit("foo"))
        self.assertRaises(SystemExit, pool.on_ready, [], [], exc)

    def test_apply_async(self):
        pool = TaskPool(10)
        pool.start()
        pool.apply_async(lambda x: x, (2, ), {})



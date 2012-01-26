from __future__ import absolute_import
from __future__ import with_statement

import signal
import sys
import time

from itertools import cycle

from mock import Mock, patch
from nose import SkipTest

try:
    from celery.concurrency import processes as mp
    from celery.concurrency.processes.pool import safe_apply_callback
except ImportError:

    class _mp(object):
        RUN = 0x1

        class TaskPool(object):
            _pool = Mock()

            def __init__(self, *args, **kwargs):
                pass

            def start(self):
                pass

            def stop(self):
                pass

            def apply_async(self, *args, **kwargs):
                pass
    mp = _mp()  # noqa
    safe_apply_callback = None  # noqa

from celery.datastructures import ExceptionInfo
from celery.utils import noop
from celery.tests.utils import Case


class Object(object):   # for writeable attributes.

    def __init__(self, **kwargs):
        [setattr(self, k, v) for k, v in kwargs.items()]


def to_excinfo(exc):
    try:
        raise exc
    except:
        return ExceptionInfo(sys.exc_info())


class MockResult(object):

    def __init__(self, value, pid):
        self.value = value
        self.pid = pid

    def worker_pids(self):
        return [self.pid]

    def get(self):
        return self.value


class MockPool(object):
    started = False
    closed = False
    joined = False
    terminated = False
    _state = None

    def __init__(self, *args, **kwargs):
        self.started = True
        self._state = mp.RUN
        self.processes = kwargs.get("processes")
        self._pool = [Object(pid=i) for i in range(self.processes)]
        self._current_proc = cycle(xrange(self.processes)).next

    def close(self):
        self.closed = True
        self._state = "CLOSE"

    def join(self):
        self.joined = True

    def terminate(self):
        self.terminated = True

    def grow(self, n=1):
        self.processes += n

    def shrink(self, n=1):
        self.processes -= n

    def apply_async(self, *args, **kwargs):
        pass


class ExeMockPool(MockPool):

    def apply_async(self, target, args=(), kwargs={}, callback=noop):
        from threading import Timer
        res = target(*args, **kwargs)
        Timer(0.1, callback, (res, )).start()
        return MockResult(res, self._current_proc())


class TaskPool(mp.TaskPool):
    Pool = MockPool


class ExeMockTaskPool(mp.TaskPool):
    Pool = ExeMockPool


class test_TaskPool(Case):

    def setUp(self):
        try:
            import multiprocessing  # noqa
        except ImportError:
            raise SkipTest("multiprocessing not supported")

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

    def test_safe_apply_callback(self):
        if safe_apply_callback is None:
            raise SkipTest("multiprocessig not supported")
        _good_called = [0]
        _evil_called = [0]

        def good(x):
            _good_called[0] = 1
            return x

        def evil(x):
            _evil_called[0] = 1
            raise KeyError(x)

        self.assertIsNone(safe_apply_callback(good, 10))
        self.assertIsNone(safe_apply_callback(evil, 10))
        self.assertTrue(_good_called[0])
        self.assertTrue(_evil_called[0])

    def test_apply_async(self):
        pool = TaskPool(10)
        pool.start()
        pool.apply_async(lambda x: x, (2, ), {})

    def test_terminate_job(self):

        @patch("celery.concurrency.processes._kill")
        def _do_test(_kill):
            pool = TaskPool(10)
            pool.terminate_job(1341)
            _kill.assert_called_with(1341, signal.SIGTERM)

        _do_test()

    def test_grow_shrink(self):
        pool = TaskPool(10)
        pool.start()
        self.assertEqual(pool._pool.processes, 10)
        pool.grow()
        self.assertEqual(pool._pool.processes, 11)
        pool.shrink(2)
        self.assertEqual(pool._pool.processes, 9)

    def test_info(self):
        pool = TaskPool(10)
        procs = [Object(pid=i) for i in range(pool.limit)]
        pool._pool = Object(_pool=procs,
                            _maxtasksperchild=None,
                            timeout=10,
                            soft_timeout=5)
        info = pool.info
        self.assertEqual(info["max-concurrency"], pool.limit)
        self.assertIsNone(info["max-tasks-per-child"])
        self.assertEqual(info["timeouts"], (5, 10))

    def test_restart(self):
        raise SkipTest("functional test")

        def get_pids(pool):
            return set([p.pid for p in pool._pool._pool])

        tp = self.TaskPool(5)
        time.sleep(0.5)
        tp.start()
        pids = get_pids(tp)
        tp.restart()
        time.sleep(0.5)
        self.assertEqual(pids, get_pids(tp))

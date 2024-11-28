"""Gevent execution pool."""
import functools
import types
from time import monotonic

from kombu.asynchronous import timer as _timer

from . import base

try:
    from gevent import Timeout
except ImportError:
    Timeout = None

__all__ = ('TaskPool',)

# pylint: disable=redefined-outer-name
# We cache globals and attribute lookups, so disable this warning.


def apply_target(target, args=(), kwargs=None, callback=None,
                 accept_callback=None, getpid=None, **_):
    kwargs = {} if not kwargs else kwargs
    return base.apply_target(target, args, kwargs, callback, accept_callback,
                             pid=getpid(), **_)


def apply_timeout(target, args=(), kwargs=None, callback=None,
                  accept_callback=None, getpid=None, timeout=None,
                  timeout_callback=None, Timeout=Timeout,
                  apply_target=base.apply_target, **rest):
    kwargs = {} if not kwargs else kwargs
    try:
        with Timeout(timeout):
            return apply_target(target, args, kwargs, callback,
                                accept_callback, getpid(),
                                propagate=(Timeout,), **rest)
    except Timeout:
        return timeout_callback(False, timeout)


class Timer(_timer.Timer):

    def __init__(self, *args, **kwargs):
        from gevent import Greenlet, GreenletExit

        class _Greenlet(Greenlet):
            cancel = Greenlet.kill

        self._Greenlet = _Greenlet
        self._GreenletExit = GreenletExit
        super().__init__(*args, **kwargs)
        self._queue = set()

    def _enter(self, eta, priority, entry, **kwargs):
        secs = max(eta - monotonic(), 0)
        g = self._Greenlet.spawn_later(secs, entry)
        self._queue.add(g)
        g.link(self._entry_exit)
        g.entry = entry
        g.eta = eta
        g.priority = priority
        g.canceled = False
        return g

    def _entry_exit(self, g):
        try:
            g.kill()
        finally:
            self._queue.discard(g)

    def clear(self):
        queue = self._queue
        while queue:
            try:
                queue.pop().kill()
            except KeyError:
                pass

    @property
    def queue(self):
        return self._queue


class TaskPool(base.BasePool):
    """GEvent Pool."""

    Timer = Timer

    signal_safe = False
    is_green = True
    task_join_will_block = False
    _pool = None
    _pool_map = None
    _quick_put = None

    def __init__(self, *args, **kwargs):
        from gevent import getcurrent, spawn_raw
        from gevent.pool import Pool
        self.Pool = Pool
        self.getcurrent = getcurrent
        self.getpid = lambda: id(getcurrent())
        self.spawn_n = spawn_raw
        self.timeout = kwargs.get('timeout')
        super().__init__(*args, **kwargs)

    def on_start(self):
        self._pool = self.Pool(self.limit)
        self._pool_map = {}
        self._quick_put = self._pool.spawn

    def on_stop(self):
        if self._pool is not None:
            self._pool.join()

    def on_apply(self, target, args=None, kwargs=None, callback=None,
                 accept_callback=None, timeout=None,
                 timeout_callback=None, apply_target=apply_target, **_):
        timeout = self.timeout if timeout is None else timeout
        target = self._make_killable_target(target)
        greenlet = self._quick_put(apply_timeout if timeout else apply_target,
                                   target, args, kwargs, callback, accept_callback,
                                   self.getpid, timeout=timeout, timeout_callback=timeout_callback)
        self._add_to_pool_map(id(greenlet), greenlet)
        greenlet.terminate = types.MethodType(_terminate, greenlet)
        return greenlet

    def grow(self, n=1):
        self._pool._semaphore.counter += n
        self._pool.size += n

    def shrink(self, n=1):
        self._pool._semaphore.counter -= n
        self._pool.size -= n

    def terminate_job(self, pid, signal=None):
        import gevent

        if pid in self._pool_map:
            greenlet = self._pool_map[pid]
            gevent.kill(greenlet)

    @property
    def num_processes(self):
        return len(self._pool)

    @staticmethod
    def _make_killable_target(target):
        def killable_target(*args, **kwargs):
            from greenlet import GreenletExit
            try:
                return target(*args, **kwargs)
            except GreenletExit:
                return (False, None, None)

        return killable_target

    def _add_to_pool_map(self, pid, greenlet):
        self._pool_map[pid] = greenlet
        greenlet.link(
            functools.partial(self._cleanup_after_job_finish, pid=pid, pool_map=self._pool_map),
        )

    @staticmethod
    def _cleanup_after_job_finish(greenlet, pool_map, pid):
        del pool_map[pid]


def _terminate(self, signal):
    # Done in `TaskPool.terminate_job`
    pass

"""Gevent execution pool."""
import functools
import types
from time import monotonic
from weakref import ref

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
        from gevent import Greenlet, GreenletExit, getcurrent
        from greenlet import greenlet

        timer_ref = ref(self)

        class _Greenlet(Greenlet):
            def cancel(self, *args, **kwargs):
                timer = timer_ref()
                if timer is not None:
                    return timer._cancel(self, *args, **kwargs)
                # A completed one-shot handle must not keep its timer alive.
                self.canceled = True
                entry = getattr(self, 'entry', None)
                if entry is not None:
                    entry.cancel()
                return Greenlet.kill(self, *args, **kwargs)

        self._Greenlet = _Greenlet
        self._GreenletExit = GreenletExit
        self._getcurrent = getcurrent
        self._kill = Greenlet.kill
        # Unlike Greenlet.started/bool(Greenlet), the base implementation is
        # false for a spawn_later callback which has not actually run yet.
        self._is_active = greenlet.__bool__
        super().__init__(*args, **kwargs)
        self._queue = set()

    def _cancel(self, g, *args, **kwargs):
        g.canceled = True
        entry = getattr(g, 'entry', None)
        current = self._getcurrent()
        if entry is not None:
            entry.cancel()
            for queued in tuple(self._queue):
                if (
                    queued is not g and
                    queued is not current and
                    getattr(queued, 'entry', None) is entry and
                    not self._is_active(queued)
                ):
                    # Cancel future iterations without interrupting a later
                    # iteration already running, or yielding to pending ones.
                    queued.kill(block=False)
                    self._queue.discard(queued)
        if g is not current:
            # Preserve kill's arguments/semantics for the explicitly supplied
            # handle, including when that handle is currently suspended.
            return self._kill(g, *args, **kwargs)

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
        # Mark every Entry before any callback can exit and reschedule itself.
        for g in tuple(queue):
            g.canceled = True
            entry = getattr(g, 'entry', None)
            if entry is not None:
                entry.cancel()
        current = self._getcurrent()
        while queue:
            try:
                g = queue.pop()
                # clear() keeps its existing force-stop behavior, but must not
                # kill its own caller or wait for callbacks to accept the kill.
                if g is not current:
                    g.kill(block=False)
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
        self.limit += n
        self._pool.size = self.limit
        # Pool.spawn blocks on the semaphore when the pool is full.
        # Release slots so autoscale wakes any task submissions waiting
        # for capacity instead of waiting for a running greenlet to finish.
        for _ in range(n):
            self._pool._semaphore.release()

    def shrink(self, n=1):
        self._pool._semaphore.counter -= n
        self.limit -= n
        self._pool.size = self.limit

    def terminate_job(self, pid, signal=None):
        import gevent

        if pid in self._pool_map:
            greenlet = self._pool_map[pid]
            gevent.kill(greenlet)

    @property
    def num_processes(self):
        return self.limit

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

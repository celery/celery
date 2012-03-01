# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os
if not os.environ.get("GEVENT_NOPATCH"):
    from gevent import monkey
    monkey.patch_all()

import sys

from time import time

from ..utils import timer2

from .base import apply_target, BasePool


class Schedule(timer2.Schedule):

    def __init__(self, *args, **kwargs):
        from gevent.greenlet import Greenlet, GreenletExit

        class _Greenlet(Greenlet):

            def cancel(self):
                self.kill()

        self._Greenlet = _Greenlet
        self._GreenletExit = GreenletExit
        super(Schedule, self).__init__(*args, **kwargs)
        self._queue = set()

    def enter(self, entry, eta=None, priority=0):
        try:
            eta = timer2.to_timestamp(eta)
        except OverflowError:
            if not self.handle_error(sys.exc_info()):
                raise

        now = time()
        if eta is None:
            eta = now
        secs = max(eta - now, 0)

        g = self._Greenlet.spawn_later(secs, entry)
        self._queue.add(g)
        g.link(self._entry_exit)
        g.entry = entry
        g.eta = eta
        g.priority = priority
        g.cancelled = False

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
        return [(g.eta, g.priority, g.entry) for g in self._queue]


class Timer(timer2.Timer):
    Schedule = Schedule

    def ensure_started(self):
        pass

    def stop(self):
        self.schedule.clear()

    def start(self):
        pass


class TaskPool(BasePool):
    Timer = Timer

    signal_safe = False
    rlimit_safe = False
    is_green = True

    def __init__(self, *args, **kwargs):
        from gevent import spawn_raw
        from gevent.pool import Pool
        self.Pool = Pool
        self.spawn_n = spawn_raw
        super(TaskPool, self).__init__(*args, **kwargs)

    def on_start(self):
        self._pool = self.Pool(self.limit)

    def on_stop(self):
        if self._pool is not None:
            self._pool.join()

    def on_apply(self, target, args=None, kwargs=None, callback=None,
            accept_callback=None, **_):
        return self._pool.spawn(apply_target, target, args, kwargs,
                                callback, accept_callback)

    def grow(self, n=1):
        self._pool._semaphore.counter += n
        self._pool.size += n

    def shrink(self, n=1):
        self._pool._semaphore.counter -= n
        self._pool.size -= n

    @property
    def num_processes(self):
        return len(self._pool)

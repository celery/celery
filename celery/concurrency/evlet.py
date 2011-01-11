import os
import sys

from time import time

import eventlet
import eventlet.debug

if not os.environ.get("EVENTLET_NOPATCH"):
    eventlet.monkey_patch()
    eventlet.debug.hub_prevent_multiple_readers(False)

from eventlet import GreenPool
from eventlet.greenthread import getcurrent, spawn, spawn_after_local
from greenlet import GreenletExit

from celery.concurrency import base
from celery.utils import timer2


def apply_target(target, args=(), kwargs={}, callback=None,
                 accept_callback=None):
    return base.apply_target(target, args, kwargs, callback, accept_callback,
                             pid=getcurrent())


class Schedule(timer2.Schedule):

    def __init__(self, *args, **kwargs):
        super(Schedule, self).__init__(*args, **kwargs)
        self._queue = set()

    def enter(self, entry, eta=None, priority=0):
        try:
            timer2.to_timestamp(eta)
        except OverflowError:
            if not self.handle_error(sys.exc_info()):
                raise

        now = time()
        if eta is None:
            eta = now
        secs = eta - now

        g = spawn_after_local(secs, entry)
        self._queue.add(g)
        g.link(self._entry_exit, entry)
        g.entry = entry
        g.eta = eta
        g.priority = priority
        g.cancelled = False

        return g

    def _entry_exit(self, g, entry):
        try:
            try:
                g.wait()
            except GreenletExit:
                entry.cancel()
                g.cancelled = True
        finally:
            self._queue.discard(g)

    def clear(self):
        queue = self._queue
        while queue:
            try:
                queue.pop().cancel()
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


class TaskPool(base.BasePool):
    Pool = GreenPool
    Timer = Timer

    signal_safe = False

    def on_start(self):
        self._pool = self.Pool(self.limit)

    def on_stop(self):
        if self._pool is not None:
            self._pool.waitall()

    def on_apply(self, target, args=None, kwargs=None, callback=None,
            accept_callback=None, **_):
        self._pool.spawn(apply_target, target, args, kwargs,
                         callback, accept_callback)

    def blocking(self, fun, *args, **kwargs):
        return spawn(fun, *args, **kwargs).wait()

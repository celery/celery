# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os
if not os.environ.get("EVENTLET_NOPATCH"):
    import eventlet
    import eventlet.debug
    eventlet.monkey_patch()
    eventlet.debug.hub_prevent_multiple_readers(False)

import sys

from time import time

from .. import signals
from ..utils import timer2

from . import base


def apply_target(target, args=(), kwargs={}, callback=None,
                 accept_callback=None, getpid=None):
    return base.apply_target(target, args, kwargs, callback, accept_callback,
                             pid=getpid())


class Schedule(timer2.Schedule):

    def __init__(self, *args, **kwargs):
        from eventlet.greenthread import spawn_after
        from greenlet import GreenletExit
        super(Schedule, self).__init__(*args, **kwargs)

        self.GreenletExit = GreenletExit
        self._spawn_after = spawn_after
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

        g = self._spawn_after(secs, entry)
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
            except self.GreenletExit:
                entry.cancel()
                g.cancelled = True
        finally:
            self._queue.discard(g)

    def clear(self):
        queue = self._queue
        while queue:
            try:
                queue.pop().cancel()
            except (KeyError, self.GreenletExit):
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

    def cancel(self, tref):
        try:
            tref.cancel()
        except self.schedule.GreenletExit:
            pass

    def start(self):
        pass


class TaskPool(base.BasePool):
    Timer = Timer

    rlimit_safe = False
    signal_safe = False
    is_green = True

    def __init__(self, *args, **kwargs):
        from eventlet import greenthread
        from eventlet.greenpool import GreenPool
        self.Pool = GreenPool
        self.getcurrent = greenthread.getcurrent
        self.getpid = lambda: id(greenthread.getcurrent())
        self.spawn_n = greenthread.spawn_n

        super(TaskPool, self).__init__(*args, **kwargs)

    def on_start(self):
        self._pool = self.Pool(self.limit)
        signals.eventlet_pool_started.send(sender=self)

    def on_stop(self):
        signals.eventlet_pool_preshutdown.send(sender=self)
        if self._pool is not None:
            self._pool.waitall()
        signals.eventlet_pool_postshutdown.send(sender=self)

    def on_apply(self, target, args=None, kwargs=None, callback=None,
            accept_callback=None, **_):
        signals.eventlet_pool_apply.send(sender=self,
                target=target, args=args, kwargs=kwargs)
        self._pool.spawn_n(apply_target, target, args, kwargs,
                           callback, accept_callback,
                           self.getpid)

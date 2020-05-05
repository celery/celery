# -*- coding: utf-8 -*-
"""Eventlet execution pool."""
from __future__ import absolute_import, unicode_literals

import sys

from kombu.asynchronous import timer as _timer  # noqa
from kombu.five import monotonic

from celery import signals  # noqa

from . import base  # noqa

__all__ = ('TaskPool',)

W_RACE = """\
Celery module with %s imported before eventlet patched\
"""
RACE_MODS = ('billiard.', 'celery.', 'kombu.')


#: Warn if we couldn't patch early enough,
#: and thread/socket depending celery modules have already been loaded.
for mod in (mod for mod in sys.modules if mod.startswith(RACE_MODS)):
    for side in ('thread', 'threading', 'socket'):  # pragma: no cover
        if getattr(mod, side, None):
            import warnings
            warnings.warn(RuntimeWarning(W_RACE % side))


def apply_target(target, args=(), kwargs=None, callback=None,
                 accept_callback=None, getpid=None):
    kwargs = {} if not kwargs else kwargs
    return base.apply_target(target, args, kwargs, callback, accept_callback,
                             pid=getpid())


class Timer(_timer.Timer):
    """Eventlet Timer."""

    def __init__(self, *args, **kwargs):
        from eventlet.greenthread import spawn_after
        from greenlet import GreenletExit
        super(Timer, self).__init__(*args, **kwargs)

        self.GreenletExit = GreenletExit
        self._spawn_after = spawn_after
        self._queue = set()

    def _enter(self, eta, priority, entry, **kwargs):
        secs = max(eta - monotonic(), 0)
        g = self._spawn_after(secs, entry)
        self._queue.add(g)
        g.link(self._entry_exit, entry)
        g.entry = entry
        g.eta = eta
        g.priority = priority
        g.canceled = False
        return g

    def _entry_exit(self, g, entry):
        try:
            try:
                g.wait()
            except self.GreenletExit:
                entry.cancel()
                g.canceled = True
        finally:
            self._queue.discard(g)

    def clear(self):
        queue = self._queue
        while queue:
            try:
                queue.pop().cancel()
            except (KeyError, self.GreenletExit):
                pass

    def cancel(self, tref):
        try:
            tref.cancel()
        except self.GreenletExit:
            pass

    @property
    def queue(self):
        return self._queue


class TaskPool(base.BasePool):
    """Eventlet Task Pool."""

    Timer = Timer

    signal_safe = False
    is_green = True
    task_join_will_block = False
    _pool = None
    _quick_put = None

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
        self._quick_put = self._pool.spawn_n
        self._quick_apply_sig = signals.eventlet_pool_apply.send

    def on_stop(self):
        signals.eventlet_pool_preshutdown.send(sender=self)
        if self._pool is not None:
            self._pool.waitall()
        signals.eventlet_pool_postshutdown.send(sender=self)

    def on_apply(self, target, args=None, kwargs=None, callback=None,
                 accept_callback=None, **_):
        self._quick_apply_sig(
            sender=self, target=target, args=args, kwargs=kwargs,
        )
        self._quick_put(apply_target, target, args, kwargs,
                        callback, accept_callback,
                        self.getpid)

    def grow(self, n=1):
        limit = self.limit + n
        self._pool.resize(limit)
        self.limit = limit

    def shrink(self, n=1):
        limit = self.limit - n
        self._pool.resize(limit)
        self.limit = limit

    def _get_info(self):
        info = super(TaskPool, self)._get_info()
        info.update({
            'max-concurrency': self.limit,
            'free-threads': self._pool.free(),
            'running-threads': self._pool.running(),
        })
        return info
